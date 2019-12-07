// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"os"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite1) TestIndexAdvise(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	_, err := tk.Exec("index advise infile '/tmp/nonexistence.sql'")
	c.Assert(err.Error(), Equals, "Index Advise: don't support load data without local field")
	_, err = tk.Exec("index advise local infile ''")
	c.Assert(err.Error(), Equals, "Index Advise: infile path is empty")

	path := "/tmp/inde_advise.sql"
	fp, err := os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)
	defer func() {
		err = fp.Close()
		c.Assert(err, IsNil)
		err = os.Remove(path)
		c.Assert(err, IsNil)
	}()
	_, err = fp.WriteString("\n" +
		"select * from t;\n" +
		"\n" +
		"select * from t where a > 1;\n" +
		"select a from t where a > 1 and a < 100;\n" +
		"\n" +
		"\n" +
		"select a,b from t1,t2 where t1.a = t2.b;\n" +
		"\n")
	c.Assert(err, IsNil)

	tk.MustExec("index advise local infile '/tmp/inde_advise.sql'")
	ctx := tk.Se.(sessionctx.Context)
	_, ok := ctx.Value(executor.IndexAdviseKey).(*executor.IndexAdviseInfo)
	c.Assert(ok, IsTrue)
	defer ctx.SetValue(executor.IndexAdviseKey, nil)
}
