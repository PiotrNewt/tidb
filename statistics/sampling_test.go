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
func (s *testStatsSuite) TestSamplingMultiCol(c *C) {
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

// Multi-table correctness test
func (s *testStatsSuite) TestSamplingMultiTbl(c *C) {
	defer cleanEnv(c, s.store, s.do)
	testKit := testkit.NewTestKit(c, s.store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t1")
	testKit.MustExec("create table t1(a int, b int)")
	testKit.MustExec("drop table if exists t2")
	testKit.MustExec("create table t2(c int, d int)")
	for i := 0; i < 100; i++ {
		if i < 50 {
			testKit.MustExec(fmt.Sprintf("insert into t1 values (%d, %d)", i, i))
			testKit.MustExec(fmt.Sprintf("insert into t2 values (%d, %d)", i, i+1))
		} else {
			testKit.MustExec(fmt.Sprintf("insert into t1 values (%d, %d)", i, i-1))
			testKit.MustExec(fmt.Sprintf("insert into t2 values (%d, %d)", i, i))
		}
	}

	for j := 0; j <= 10; j++ {
		testKit.MustExec(fmt.Sprintf("set @@session.tidb_optimizer_dynamic_sampling = %d;", j))
		testKit.MustQuery("select count(*) from t1,t2 where t1.a = t2.c").Check(testkit.Rows("100"))
		testKit.MustQuery("select count(*) from t1,t2 where t1.a = t2.c and t2.d < 50").Check(testkit.Rows("49"))
		testKit.MustQuery("select count(*) from t1,t2 where t1.a = 50 and t2.d = 50").Check(testkit.Rows("2"))
		testKit.MustQuery("select count(*) from t1 join t2 on t1.a < 10 and t2.d < 10").Check(testkit.Rows("90"))
		testKit.MustQuery("select count(*) from t1 where a in (select c from t2 where d >= 80)").Check(testkit.Rows("20"))
	}
}
