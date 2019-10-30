package statistics_test

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

// Single column correctness test
func (s *testStatsSuite) TestSamplingSingleCol(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t1")
	testKit.MustExec("create table t1(a int)")
	for i := 0; i < 100; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t1 values (%d)", i))
	}

	for j := 0; j <= 10; j++ {
		testKit.MustExec(fmt.Sprintf("set @@session.tidb_optimizer_dynamic_sampling = %d;", j))
		testKit.MustQuery("select count(*) from t1 where a > 0").Check(testkit.Rows("99"))
	}
}

// Multi-column correctness test
func (s *testStatsSuite) TestSamplingMultCol(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t1")
	testKit.MustExec("create table t1(a int, b int)")
	for i := 0; i < 100; i++ {
		if i < 50 {
			testKit.MustExec(fmt.Sprintf("insert into t1 values (%d, %d)", i, i))
		} else {
			testKit.MustExec(fmt.Sprintf("insert into t1 values (%d, %d)", i, i-1))
		}
	}

	for j := 0; j <= 10; j++ {
		testKit.MustExec(fmt.Sprintf("set @@session.tidb_optimizer_dynamic_sampling = %d;", j))
		testKit.MustQuery("select count(*) from t1 where a = b").Check(testkit.Rows("50"))
	}
}
