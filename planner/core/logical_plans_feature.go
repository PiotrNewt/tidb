// code by piotr newt for experiment

package core

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/pingcap/tidb/sessionctx/variable"
	mlpb "github.com/piotrnewt/sdo/src/go-mlpb"
	"google.golang.org/grpc"
)

func requestMLServer(feature *Feature, sql string, isDone bool, latency int64, sv *variable.SessionVars, stepIdx int, flag uint64) int {
	if !isDone && feature == nil {
		return 0
	}

	// ip and part should can be configured
	connect, err := grpc.Dial("127.0.0.1:56066", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer connect.Close()

	var plan string
	if feature != nil {
		plan = feature.plan
		// fmt.Println(plan)
	}

	client := mlpb.NewAutoLogicalRulesApplyClient(connect)
	response, err := client.GetNextApplyIdxRequest(
		context.Background(),
		&mlpb.NextApplyIdxRequest{
			Sql:            sql,
			Latency:        latency,
			ParserLatency:  int64(sv.DurationParse),
			CompileLatency: int64(sv.DurationCompile),
			Done:           isDone,
			Plan:           plan,
			Flag:           flag,
			StepIdx:        int64(stepIdx),
		})

	res := int(response.GetRuleIdx())
	// fmt.Println("ruleIdx: ", res)
	return res
}

func sendFinalPlan(logic LogicalPlan, stepIdx int, flag uint64) {
	final := getFeatureOfLogicalPlan(logic)
	_ = requestMLServer(final, getSQLByPlan(logic), true, 0.0, logic.SCtx().GetSessionVars(), stepIdx, flag)
}

func randReword() int64 {
	rand.Seed(time.Now().Unix())
	return rand.Int63()
}

// DoneThisQuery send the time as reward and clean the state info
func DoneThisQuery(latency time.Duration, sql string, sv *variable.SessionVars) {
	if isSysQuery(sv.StmtCtx.Tables) {
		return
	}
	_ = requestMLServer(nil, sql, true, int64(latency), sv, -1, 0)
	// fmt.Println("ml sql done")
}

// randString generate a random string with lence
func randString(n int) string {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func getSQLByPlan(logic LogicalPlan) string {
	if logic == nil {
		return ""
	}

	sctx := logic.SCtx()
	sessionVars := sctx.GetSessionVars()
	return sessionVars.StmtCtx.OriginalSQL
}

// Feature represents logical plan feature.
type Feature struct {
	plan string
}

// getFeatureOfLogicalPlan scan the logical paln from top node to the buttom.
// it will return fearture of logical plan.
func getFeatureOfLogicalPlan(logic LogicalPlan) *Feature {
	return &Feature{
		plan: serialize(&logic),
	}
}

/*
              GetFeature()
                   ▲
                   │
                   │               FeatureEmbedding()
                   │    ┌─────┐            ▲
      ┌────────┐   │    │*****│            │
      │        │───○───▶│*****│ ────┐      │
      └────┬───┘        └─────┘     │      │
           │                        │      │
      ┌────┴───┐        ┌─────┐     │     ┌○┐   ┌───────────┐
      │        │───────▶│*****│     └────▶│ │   │░░░░░░░░░░░│
      └┬──────┬┘        │*****│──────────▶│ │   │░░░░░░░░░░░│
       │      │         └─────┘     ┌────▶│ ├──▶│░░░░░░░░░░░│
       │      │                     │┌───▶│ │   │░░░░░░░░░░░│
┌──────┴─┐  ┌─┴──────┐   ┌─────┐    ││ │  └─┘   │░░░░░░░░░░░│
│        │  │        │──▶│*****│    ││ │        └───────────┘
└────────┘  └────┬───┘   │*****│────┘│ │
                 │       └─────┘     │ │
            ┌────┴───┐    ┌─────┐    │ │
            │        │───▶│*****│    │ │
            └────────┘    │*****│────┘ │
                          └─────┘      │
                                       ▼
                                ┌──────────────┐
                                │ to ml_server │
                                └──────────────┘
*/

// there are 3 ways to encode plan tree
/*
(1)	Seq2Seq: planner/core/stringer.go/ToString + a seq2seq model + https://github.com/zyguan/sql-spider
(2) Tree LSTM: https://github.com/dmlc/dgl/tree/master/examples/pytorch/tree_lstm
(3) Tree CNN: https://github.com/RyanMarcus/TreeConvolution
*/

// 1. we collect the info which will be modified by rules.
/*
(1) gcSubstituter: expression(column+1) --> expression(indexed virtual column)
(2) columnPruner: expression(column) --> nil
(3) buildKeySolver: we do not to move this rule, because it is base step before another.
(4) decorrelateSolver: apply --> join (apply just can be implemented as next-loop join)
(5) aggregationEliminator: aggregation --> projection, it may rewrite expression (if agg group by unique key)
(6) projectionEliminator: expression(column) --> another expression(column)
(7) maxMinEliminator: select max(id) from t --> select max(id) from (subquery)
(8) ppdSolver: Predicate Push Down, expression --> expression (tree structure)
(9) outerJoinEliminator: outer join --> nil (tree structure)
(10) partitionProcessor: rewrites the ast(plan) for table partition. (tree structure)
(11) aggregationPushDownSolver: aggregation push down (tree structure)
(12) pushDownTopNOptimizer: topN push down (tree structure)
(13) joinReOrderSolver: (tree structure)
(14) columnPruner: expression(column) --> nil
(--) conclusion: maybe we just need the tree structure information, and it can be represented by a sequence.
*/

// serialize serialize a plan tree to string, which will easy to deserialize in ml_server.
func serialize(root *LogicalPlan) string {
	// var getTp func(*LogicalPlan) string
	getNodeTp := func(node *LogicalPlan) string {
		var tp string
		switch (*node).(type) {
		case *LogicalJoin:
			tp = "1"
		case *LogicalAggregation:
			tp = "2"
		case *LogicalProjection:
			tp = "3"
		case *LogicalSelection:
			tp = "4"
		case *LogicalApply:
			tp = "5"
		case *LogicalMaxOneRow:
			tp = "6"
		case *LogicalTableDual:
			tp = "7"
		case *DataSource:
			tp = "8"
		case *TiKVSingleGather:
			tp = "9"
		case *LogicalTableScan:
			tp = "10"
		case *LogicalIndexScan:
			tp = "11"
		case *LogicalUnionAll:
			tp = "12"
		case *LogicalSort:
			tp = "13"
		case *LogicalLock:
			tp = "14"
		case *LogicalLimit:
			tp = "15"
		case *LogicalWindow:
			tp = "16"
		case *LogicalShow:
			tp = "17"
		case *logicalSchemaProducer:
			tp = "18"
		case *LogicalTopN:
			tp = "19"
		case *LogicalUnionScan:
			tp = "20"
		default:
			// baselogicalplan
			return "0"
		}
		return tp
	}

	if root == nil {
		return ""
	}
	queue := []*LogicalPlan{root}
	res := getNodeTp(root) + "_"
	for len(queue) != 0 {
		node := *queue[0]
		queue = queue[1:]
		children := node.Children()
		// left
		if len(children) > 0 && children[0] != nil {
			res += getNodeTp(&children[0]) + "_"
			queue = append(queue, &children[0])
		} else {
			res += "#_"
		}
		// right
		if len(children) > 1 && children[1] != nil {
			res += getNodeTp(&children[1]) + "_"
			queue = append(queue, &children[1])
		} else {
			res += "#_"
		}
	}
	return res
}

// GetFeature implements the Feature GetFeature interface.
// baseLogicalPlan implements it for some easy plans.
func (logic *baseLogicalPlan) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *LogicalJoin) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *LogicalAggregation) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *LogicalProjection) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *LogicalSelection) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *LogicalApply) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *LogicalMaxOneRow) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *LogicalTableDual) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *DataSource) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *TiKVSingleGather) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *LogicalTableScan) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic LogicalIndexScan) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *LogicalUnionAll) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *LogicalSort) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *LogicalLock) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *LogicalLimit) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *LogicalWindow) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *LogicalShow) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *logicalSchemaProducer) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *LogicalTopN) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (logic *LogicalUnionScan) GetFeature() string {
	// TODO
	return ""
}
