package sampling

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// AnalyzeSample get the sample for the place using, it is for one table
func AnalyzeSample(ctx sessionctx.Context, histColl *statistics.HistColl, columnID int64, isIndex bool, sampleSize uint64, fulltable bool) error {

	taskCh := make(chan *analyzeSampleTask)
	resultCh := make(chan analyzeSampleResult)

	buildAnalyzeSampleTask(ctx, histColl, columnID, isIndex, sampleSize, fulltable, taskCh)

	// 采样的并发度写死为 1
	concurrency := 1
	for i := 0; i < concurrency; i++ {
		go analyzeSampleWorker(taskCh, resultCh, i == 0)
	}
	close(taskCh)

	// 接收结果
	panicCnt := 0
	for panicCnt < concurrency {
		result, ok := <-resultCh
		if !ok {
			break
		}
		if result.Err != nil {
			if result.Err == errAnalyzeSamplingWorkerPanic {
				panicCnt++
			}
			continue
		}

		// 处理结果, 直接写到 histColl 中
		if result.IsIndex == 1 {
			histColl.Indices[result.Sample[0].SID].SampleC = result.Sample[0]
		} else {
			for _, samplec := range result.Sample {
				histColl.Columns[columnID].SampleC = samplec
			}
		}
	}
	return nil
}

var errAnalyzeSamplingWorkerPanic = errors.New("analyze worker panic")

type analyzeSampleTask struct {
	sampleExec *AnalyzeSampleExec
	//job        *statistics.AnalyzeJob
}

// AnalyzeSampleExec is a executor used to sampling
type AnalyzeSampleExec struct {
	ctx             sessionctx.Context
	physicalTableID int64
	pkInfo          *model.ColumnInfo
	colsInfo        []*model.ColumnInfo
	idxsInfo        []*model.IndexInfo
	concurrency     int
	tblInfo         *model.TableInfo
	cache           *tikv.RegionCache
	wg              *sync.WaitGroup
	sampLocs        chan *tikv.KeyLocation
	rowCount        uint64
	sampCursor      int32
	sampTasks       []*AnalyzeFastTask
	scanTasks       []*tikv.KeyLocation
	collectors      []*statistics.SampleCollector
	randSeed        int64
	//job             *AnalyzeJob
	sampleSize uint64
}

// AnalyzeFastTask is the task use to sample from kv
type AnalyzeFastTask struct {
	Location    *tikv.KeyLocation
	SampSize    uint64
	BeginOffset uint64
	EndOffset   uint64
}

type analyzeSampleResult struct {
	PhysicalTableID int64
	Sample          []*statistics.SampleC
	//Count           int64
	IsIndex int
	Err     error
	//job     *AnalyzeJob
}

func buildAnalyzeSampleTask(ctx sessionctx.Context, histColl *statistics.HistColl, columnID int64, isIndex bool, sampleSize uint64, fulltable bool, taskCh chan *analyzeSampleTask) {
	statsHandle := domain.GetDomain(ctx).StatsHandle()
	physicalID := histColl.PhysicalID
	table := statsHandle.GetTableByPID(GetInfoSchema(ctx), physicalID)
	tableInfo := table.Meta()
	cloumsInfos := tableInfo.Columns
	indexInfos := tableInfo.Indices
	pkInfo := tableInfo.GetPkColInfo()
	concurrency := 1

	if fulltable {
		// TODO
		return
	}

	var sampleExec AnalyzeSampleExec
	sampleExec.ctx = ctx
	sampleExec.physicalTableID = physicalID
	sampleExec.tblInfo = tableInfo
	sampleExec.concurrency = concurrency
	sampleExec.wg = &sync.WaitGroup{}
	sampleExec.sampleSize = sampleSize
	if isIndex {
		// 只做单个对 idx 的任务
		sampleExec.idxsInfo = []*model.IndexInfo{indexInfos[columnID]}
		taskCh <- &analyzeSampleTask{
			sampleExec: &sampleExec,
		}
	} else {
		// 做单个对表上所有列的任务
		sampleExec.colsInfo = cloumsInfos
		sampleExec.pkInfo = pkInfo
		taskCh <- &analyzeSampleTask{
			sampleExec: &sampleExec,
		}
	}
}

// analyzeWorker 负责处理 TaskCh 中的所有 analyzeTask
func analyzeSampleWorker(taskCh <-chan *analyzeSampleTask, resultCh chan<- analyzeSampleResult, isCloseChanThread bool) {
	for {
		task, ok := <-taskCh
		if !ok {
			break
		}
		for _, result := range analyzeSampleExec(task.sampleExec) {
			resultCh <- result
		}
	}
}

// -----
// 处理单个表的采样，返回所有列上的统计信息
// 由于对于一个表的采样可能是idx采样和colm的采样，所以可能需要返回 2 个 analyzeResult
func analyzeSampleExec(exec *AnalyzeSampleExec) []analyzeSampleResult {
	sample, err := exec.buildSample()
	if err != nil {
		return []analyzeSampleResult{{Err: err}}
	}
	var results []analyzeSampleResult
	hasPKInfo := 0
	if exec.pkInfo != nil {
		hasPKInfo = 1
	}
	// 处理存在索引的情况
	if len(exec.idxsInfo) > 0 {
		for i := hasPKInfo + len(exec.idxsInfo); i < len(sample); i++ {
			idxResult := analyzeSampleResult{
				PhysicalTableID: exec.physicalTableID,
				Sample:          []*statistics.SampleC{sample[i]},
				IsIndex:         1,
				//job:             exec.job,
			}
			results = append(results, idxResult)
		}
	}

	// 处理列上的采集
	if len(exec.colsInfo) > 0 {
		colResult := analyzeSampleResult{
			PhysicalTableID: exec.physicalTableID,
			Sample:          sample[:hasPKInfo+len(exec.idxsInfo)],
			//job:             exec.job,
		}
		results = append(results, colResult)
	}
	return results
}

// 构建采样，需要将 AnalyzeResult 中 Sample 传进去
func (e *AnalyzeSampleExec) buildSample() (sample []*statistics.SampleC, err error) {
	// 测试需要所以默认值为 1
	// if RandSeed == 1 {
	// 	e.randSeed = time.Now().UnixNano()
	// } else {
	// 	e.randSeed = RandSeed
	// }
	e.randSeed = 1
	rander := rand.New(rand.NewSource(e.randSeed))

	needRebuild, maxBuildTimes := true, 5
	regionErrorCounter := 0
	for counter := maxBuildTimes; needRebuild && counter > 0; counter-- {
		regionErrorCounter++
		needRebuild, err = e.buildSampTask()
		if err != nil {
			return nil, err
		}
	}

	if needRebuild {
		errMsg := "build Sample analyze task failed, exceed maxBuildTimes: %v"
		return nil, errors.Errorf(errMsg, maxBuildTimes)
	}

	//defer e.job.Update(int64(e.rowCount))

	// 如果表的总行数小于样本量的2倍,直接全表扫描
	if e.rowCount < e.sampleSize*2 {
		for _, task := range e.sampTasks {
			e.scanTasks = append(e.scanTasks, task.Location)
		}
		e.sampTasks = e.sampTasks[:0]
		e.rowCount = 0
		return e.runTasks()
	}

	// 生成样本长度个随机数
	randPos := make([]uint64, 0, e.sampleSize+1)
	for i := 0; i < int(e.sampleSize); i++ {
		randPos = append(randPos, uint64(rander.Int63n(int64(e.rowCount))))
	}
	sort.Slice(randPos, func(i, j int) bool { return randPos[i] < randPos[j] })

	for _, task := range e.sampTasks {
		begin := sort.Search(len(randPos), func(i int) bool { return randPos[i] >= task.BeginOffset })
		end := sort.Search(len(randPos), func(i int) bool { return randPos[i] >= task.EndOffset })
		task.SampSize = uint64(end - begin)
	}
	return e.runTasks()
}

func (e *AnalyzeSampleExec) runTasks() ([]*statistics.SampleC, error) {
	errs := make([]error, e.concurrency)
	hasPKInfo := 0
	if e.pkInfo != nil {
		hasPKInfo = 1
	}

	length := len(e.colsInfo) + hasPKInfo + len(e.idxsInfo)
	e.collectors = make([]*statistics.SampleCollector, length)
	for i := range e.collectors {
		e.collectors[i] = &statistics.SampleCollector{
			MaxSampleSize: int64(e.sampleSize),
			Samples:       make([]*statistics.SampleItem, e.sampleSize),
		}
	}

	e.wg.Add(e.concurrency)
	bo := tikv.NewBackoffer(context.Background(), 500)
	for i := 0; i < e.concurrency; i++ {
		go e.handleSampTasks(bo, i, &errs[i])
	}
	e.wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	_, err := e.handleScanTasks(bo)
	if err != nil {
		return nil, err
	}

	stats := domain.GetDomain(e.ctx).StatsHandle()
	rowCount := int64(e.rowCount)
	if stats.Lease() > 0 {
		rowCount = mathutil.MinInt64(stats.GetTableStats(e.tblInfo).Count, rowCount)
	}

	//fmt
	fmt.Println("sampleSize = ", e.sampleSize)

	// 生成采样结果
	samples := make([]*statistics.SampleC, length)
	for i := 0; i < length; i++ {
		// 生成收集器属性
		collector := e.collectors[i]
		collector.Samples = collector.Samples[:e.sampCursor]
		sort.Slice(collector.Samples, func(i, j int) bool { return collector.Samples[i].RowID < collector.Samples[j].RowID })
		collector.CalcTotalSize()

		rowCount = mathutil.MaxInt64(rowCount, int64(len(collector.Samples)))
		collector.TotalSize *= rowCount / int64(len(collector.Samples))

		if i < hasPKInfo {
			samples[i], err = e.buildICSample(e.pkInfo.ID, e.collectors[i])
		} else if i < hasPKInfo+len(e.colsInfo) {
			samples[i], err = e.buildICSample(e.colsInfo[i-hasPKInfo].ID, e.collectors[i])
		} else {
			samples[i], err = e.buildICSample(e.idxsInfo[i-hasPKInfo-len(e.colsInfo)].ID, e.collectors[i])
		}
		if err != nil {
			return nil, err
		}
	}
	return samples, nil
}

func (e *AnalyzeSampleExec) buildICSample(colID int64, collector *statistics.SampleCollector) (*statistics.SampleC, error) {
	sc := e.ctx.GetSessionVars().StmtCtx
	sampleItems := collector.Samples
	err := statistics.SortSampleItems(sc, sampleItems)
	if err != nil {
		return nil, err
	}
	var sample *statistics.SampleC
	for i := 0; i < len(sampleItems); i++ {
		sample.SampleColumn.AppendBytes(sampleItems[i].Value.GetBytes())
	}
	sample.SID = colID
	return sample, nil
}

//--from executor--//

// buildSampTask returns two variables, the first bool is whether the task meets region error
// and need to rebuild.
func (e *AnalyzeSampleExec) buildSampTask() (needRebuild bool, err error) {
	// Do get regions row count.
	bo := tikv.NewBackoffer(context.Background(), 500)
	needRebuildForRoutine := make([]bool, e.concurrency)
	errs := make([]error, e.concurrency)
	sampTasksForRoutine := make([][]*AnalyzeFastTask, e.concurrency)
	e.sampLocs = make(chan *tikv.KeyLocation, e.concurrency)
	e.wg.Add(e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		go e.getSampRegionsRowCount(bo, &needRebuildForRoutine[i], &errs[i], &sampTasksForRoutine[i])
	}

	defer func() {
		close(e.sampLocs)
		e.wg.Wait()
		if err != nil {
			return
		}
		for i := 0; i < e.concurrency; i++ {
			if errs[i] != nil {
				err = errs[i]
			}
			needRebuild = needRebuild || needRebuildForRoutine[i]
			e.sampTasks = append(e.sampTasks, sampTasksForRoutine[i]...)
		}
	}()

	store, _ := e.ctx.GetStore().(tikv.Storage)
	e.cache = store.GetRegionCache()
	startKey, endKey := tablecodec.GetTableHandleKeyRange(e.physicalTableID)
	targetKey, err := e.getNextSampleKey(bo, startKey)
	if err != nil {
		return false, err
	}
	e.rowCount = 0
	for _, task := range e.sampTasks {
		cnt := task.EndOffset - task.BeginOffset
		task.BeginOffset = e.rowCount
		task.EndOffset = e.rowCount + cnt
		e.rowCount += cnt
	}
	accessRegionsCounter := 0
	for {
		// Search for the region which contains the targetKey.
		loc, err := e.cache.LocateKey(bo, targetKey)
		if err != nil {
			return false, err
		}
		if bytes.Compare(endKey, loc.StartKey) < 0 {
			break
		}
		accessRegionsCounter++

		// Set the next search key.
		targetKey = loc.EndKey

		// If the KV pairs in the region all belonging to the table, add it to the sample task.
		if bytes.Compare(startKey, loc.StartKey) <= 0 && len(loc.EndKey) != 0 && bytes.Compare(loc.EndKey, endKey) <= 0 {
			e.sampLocs <- loc
			continue
		}

		e.scanTasks = append(e.scanTasks, loc)
		if bytes.Compare(loc.StartKey, startKey) < 0 {
			loc.StartKey = startKey
		}
		if bytes.Compare(endKey, loc.EndKey) < 0 || len(loc.EndKey) == 0 {
			loc.EndKey = endKey
			break
		}
	}

	return false, nil
}

func (e *AnalyzeSampleExec) getNextSampleKey(bo *tikv.Backoffer, startKey kv.Key) (kv.Key, error) {
	if len(e.sampTasks) == 0 {
		e.scanTasks = e.scanTasks[:0]
		return startKey, nil
	}
	sort.Slice(e.sampTasks, func(i, j int) bool {
		return bytes.Compare(e.sampTasks[i].Location.StartKey, e.sampTasks[j].Location.StartKey) < 0
	})
	// The sample task should be consecutive with scan task.
	if len(e.scanTasks) > 0 && bytes.Equal(e.scanTasks[0].StartKey, startKey) && !bytes.Equal(e.scanTasks[0].EndKey, e.sampTasks[0].Location.StartKey) {
		e.scanTasks = e.scanTasks[:0]
		e.sampTasks = e.sampTasks[:0]
		return startKey, nil
	}
	prefixLen := 0
	for ; prefixLen < len(e.sampTasks)-1; prefixLen++ {
		if !bytes.Equal(e.sampTasks[prefixLen].Location.EndKey, e.sampTasks[prefixLen+1].Location.StartKey) {
			break
		}
	}
	// Find the last one that could align with region bound.
	for ; prefixLen >= 0; prefixLen-- {
		loc, err := e.cache.LocateKey(bo, e.sampTasks[prefixLen].Location.EndKey)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(loc.StartKey, e.sampTasks[prefixLen].Location.EndKey) {
			startKey = loc.StartKey
			break
		}
	}
	e.sampTasks = e.sampTasks[:prefixLen+1]
	for i := len(e.scanTasks) - 1; i >= 0; i-- {
		if bytes.Compare(startKey, e.scanTasks[i].EndKey) < 0 {
			e.scanTasks = e.scanTasks[:i]
		}
	}
	return startKey, nil
}

func (e *AnalyzeSampleExec) getSampRegionsRowCount(bo *tikv.Backoffer, needRebuild *bool, err *error, sampTasks *[]*AnalyzeFastTask) {
	defer func() {
		if *needRebuild == true {
			for ok := true; ok; _, ok = <-e.sampLocs {
				// Do nothing, just clear the channel.
			}
		}
		e.wg.Done()
	}()
	client := e.ctx.GetStore().(tikv.Storage).GetTiKVClient()
	for {
		loc, ok := <-e.sampLocs
		if !ok {
			return
		}
		req := tikvrpc.NewRequest(tikvrpc.CmdDebugGetRegionProperties, &debugpb.GetRegionPropertiesRequest{
			RegionId: loc.Region.GetID(),
		})
		var resp *tikvrpc.Response
		var rpcCtx *tikv.RPCContext
		// we always use the first follower when follower read is enabled
		rpcCtx, *err = e.cache.GetTiKVRPCContext(bo, loc.Region, e.ctx.GetSessionVars().GetReplicaRead(), 0)
		if *err != nil {
			return
		}

		ctx := context.Background()
		resp, *err = client.SendRequest(ctx, rpcCtx.Addr, req, tikv.ReadTimeoutMedium)
		if *err != nil {
			return
		}
		if resp.Resp == nil || len(resp.Resp.(*debugpb.GetRegionPropertiesResponse).Props) == 0 {
			*needRebuild = true
			return
		}
		for _, prop := range resp.Resp.(*debugpb.GetRegionPropertiesResponse).Props {
			if prop.Name == "mvcc.num_rows" {
				var cnt uint64
				cnt, *err = strconv.ParseUint(prop.Value, 10, 64)
				if *err != nil {
					return
				}
				newCount := atomic.AddUint64(&e.rowCount, cnt)
				task := &AnalyzeFastTask{
					Location:    loc,
					BeginOffset: newCount - cnt,
					EndOffset:   newCount,
				}
				*sampTasks = append(*sampTasks, task)
				break
			}
		}
	}
}

func (e *AnalyzeSampleExec) handleSampTasks(bo *tikv.Backoffer, workID int, err *error) {
	defer e.wg.Done()
	var snapshot kv.Snapshot
	snapshot, *err = e.ctx.GetStore().(tikv.Storage).GetSnapshot(kv.MaxVersion)
	if *err != nil {
		return
	}
	if e.ctx.GetSessionVars().GetReplicaRead().IsFollowerRead() {
		snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
	}
	rander := rand.New(rand.NewSource(e.randSeed + int64(workID)))

	for i := workID; i < len(e.sampTasks); i += e.concurrency {
		task := e.sampTasks[i]
		if task.SampSize == 0 {
			continue
		}

		var tableID, minRowID, maxRowID int64
		startKey, endKey := task.Location.StartKey, task.Location.EndKey
		tableID, minRowID, *err = tablecodec.DecodeRecordKey(startKey)
		if *err != nil {
			return
		}
		_, maxRowID, *err = tablecodec.DecodeRecordKey(endKey)
		if *err != nil {
			return
		}

		keys := make([]kv.Key, 0, task.SampSize)
		for i := 0; i < int(task.SampSize); i++ {
			randKey := rander.Int63n(maxRowID-minRowID) + minRowID
			keys = append(keys, tablecodec.EncodeRowKeyWithHandle(tableID, randKey))
		}

		kvMap := make(map[string][]byte, len(keys))
		for _, key := range keys {
			var iter kv.Iterator
			iter, *err = snapshot.Iter(key, endKey)
			if *err != nil {
				return
			}
			if iter.Valid() {
				kvMap[string(iter.Key())] = iter.Value()
			}
		}

		*err = e.handleBatchSeekResponse(kvMap)
		if *err != nil {
			return
		}
	}
}

func (e *AnalyzeSampleExec) handleScanTasks(bo *tikv.Backoffer) (keysSize int, err error) {
	snapshot, err := e.ctx.GetStore().(tikv.Storage).GetSnapshot(kv.MaxVersion)
	if err != nil {
		return 0, err
	}
	if e.ctx.GetSessionVars().GetReplicaRead().IsFollowerRead() {
		snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
	}
	for _, t := range e.scanTasks {
		iter, err := snapshot.Iter(t.StartKey, t.EndKey)
		if err != nil {
			return keysSize, err
		}
		size, err := e.handleScanIter(iter)
		keysSize += size
		if err != nil {
			return keysSize, err
		}
	}
	return keysSize, nil
}

func (e *AnalyzeSampleExec) handleBatchSeekResponse(kvMap map[string][]byte) (err error) {
	length := int32(len(kvMap))
	newCursor := atomic.AddInt32(&e.sampCursor, length)
	hasPKInfo := 0
	if e.pkInfo != nil {
		hasPKInfo = 1
	}
	samplePos := newCursor - length
	for sKey, sValue := range kvMap {
		err = e.updateCollectorSamples(sValue, kv.Key(sKey), samplePos, hasPKInfo)
		if err != nil {
			return err
		}
		samplePos++
	}
	return nil
}

func (e *AnalyzeSampleExec) handleScanIter(iter kv.Iterator) (scanKeysSize int, err error) {
	hasPKInfo := 0
	if e.pkInfo != nil {
		hasPKInfo = 1
	}
	rander := rand.New(rand.NewSource(e.randSeed + int64(e.rowCount)))
	sampleSize := int64(e.sampleSize)
	for ; iter.Valid() && err == nil; err = iter.Next() {
		// reservoir sampling
		e.rowCount++
		scanKeysSize++
		randNum := rander.Int63n(int64(e.rowCount))
		if randNum > sampleSize && e.sampCursor == int32(sampleSize) {
			continue
		}

		p := rander.Int31n(int32(sampleSize))
		if e.sampCursor < int32(sampleSize) {
			p = e.sampCursor
			e.sampCursor++
		}

		err = e.updateCollectorSamples(iter.Value(), iter.Key(), p, hasPKInfo)
		if err != nil {
			return
		}
	}
	return
}

func (e *AnalyzeSampleExec) updateCollectorSamples(sValue []byte, sKey kv.Key, samplePos int32, hasPKInfo int) (err error) {
	// Decode the cols value in order.
	var values map[int64]types.Datum
	values, err = e.decodeValues(sValue)
	if err != nil {
		return err
	}
	var rowID int64
	rowID, err = tablecodec.DecodeRowKey(sKey)
	if err != nil {
		return err
	}
	// Update the primary key collector.
	if hasPKInfo > 0 {
		v, ok := values[e.pkInfo.ID]
		if !ok {
			var key int64
			_, key, err = tablecodec.DecodeRecordKey(sKey)
			if err != nil {
				return err
			}
			v = types.NewIntDatum(key)
		}
		if mysql.HasUnsignedFlag(e.pkInfo.Flag) {
			v.SetUint64(uint64(v.GetInt64()))
		}
		if e.collectors[0].Samples[samplePos] == nil {
			e.collectors[0].Samples[samplePos] = &statistics.SampleItem{}
		}
		e.collectors[0].Samples[samplePos].RowID = rowID
		e.collectors[0].Samples[samplePos].Value = v
	}
	// Update the columns' collectors.
	for j, colInfo := range e.colsInfo {
		v, err := e.getValueByInfo(colInfo, values)
		if err != nil {
			return err
		}
		if e.collectors[hasPKInfo+j].Samples[samplePos] == nil {
			e.collectors[hasPKInfo+j].Samples[samplePos] = &statistics.SampleItem{}
		}
		e.collectors[hasPKInfo+j].Samples[samplePos].RowID = rowID
		e.collectors[hasPKInfo+j].Samples[samplePos].Value = v
	}
	// Update the indexes' collectors.
	for j, idxInfo := range e.idxsInfo {
		idxVals := make([]types.Datum, 0, len(idxInfo.Columns))
		for _, idxCol := range idxInfo.Columns {
			for _, colInfo := range e.colsInfo {
				if colInfo.Name == idxCol.Name {
					v, err := e.getValueByInfo(colInfo, values)
					if err != nil {
						return err
					}
					idxVals = append(idxVals, v)
					break
				}
			}
		}
		var bytes []byte
		bytes, err = codec.EncodeKey(e.ctx.GetSessionVars().StmtCtx, bytes, idxVals...)
		if err != nil {
			return err
		}
		if e.collectors[len(e.colsInfo)+hasPKInfo+j].Samples[samplePos] == nil {
			e.collectors[len(e.colsInfo)+hasPKInfo+j].Samples[samplePos] = &statistics.SampleItem{}
		}
		e.collectors[len(e.colsInfo)+hasPKInfo+j].Samples[samplePos].RowID = rowID
		e.collectors[len(e.colsInfo)+hasPKInfo+j].Samples[samplePos].Value = types.NewBytesDatum(bytes)
	}
	return nil
}

func (e *AnalyzeSampleExec) decodeValues(sValue []byte) (values map[int64]types.Datum, err error) {
	colID2FieldTypes := make(map[int64]*types.FieldType, len(e.colsInfo))
	if e.pkInfo != nil {
		colID2FieldTypes[e.pkInfo.ID] = &e.pkInfo.FieldType
	}
	for _, col := range e.colsInfo {
		colID2FieldTypes[col.ID] = &col.FieldType
	}
	return tablecodec.DecodeRow(sValue, colID2FieldTypes, e.ctx.GetSessionVars().Location())
}

func (e *AnalyzeSampleExec) getValueByInfo(colInfo *model.ColumnInfo, values map[int64]types.Datum) (types.Datum, error) {
	val, ok := values[colInfo.ID]
	if !ok {
		return table.GetColOriginDefaultValue(e.ctx, colInfo)
	}
	return val, nil
}

// GetInfoSchema is a func copy from executer/compiler
func GetInfoSchema(ctx sessionctx.Context) infoschema.InfoSchema {
	sessVar := ctx.GetSessionVars()
	var is infoschema.InfoSchema
	if snap := sessVar.SnapshotInfoschema; snap != nil {
		is = snap.(infoschema.InfoSchema)
		logutil.BgLogger().Info("use snapshot schema", zap.Uint64("conn", sessVar.ConnectionID), zap.Int64("schemaVersion", is.SchemaMetaVersion()))
	} else {
		is = sessVar.TxnCtx.InfoSchema.(infoschema.InfoSchema)
	}
	return is
}
