package statistics_test

import (
	"sync"

	. "github.com/pingcap/check"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/testkit"
)

type testSuite1 struct {
	store kv.Storage
	dom   *domain.Domain
	cli   *checkRequestClient
}

type checkRequestClient struct {
	tikv.Client
	priority       pb.CommandPri
	lowPriorityCnt uint32
	mu             struct {
		sync.RWMutex
		checkFlags uint32
		syncLog    bool
	}
}

// Sql correctness test
func (s *testSuite1) TestSQLWithSampling(c *C) {
	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(cluster)
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(cluster),
	)
	c.Assert(err, IsNil)
	// var dom *domain.Domain
	// session.DisableStats4Test()
	// session.SetSchemaLease(0)
	// dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c char(10), index index_b(b))")
}

// 对执行计划的改变
