// Copyright 2015 PingCAP, Inc.
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

package core

import (
	"math"
	"math/rand"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"go.uber.org/atomic"
)

// OptimizeAstNode optimizes the query to a physical plan directly.
var OptimizeAstNode func(ctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (Plan, error)

// AllowCartesianProduct means whether tidb allows cartesian join without equal conditions.
var AllowCartesianProduct = atomic.NewBool(true)

const (
	flagPrunColumns uint64 = 1 << iota
	flagBuildKeyInfo
	flagDecorrelate
	flagEliminateAgg
	flagEliminateProjection
	flagMaxMinEliminate
	flagPredicatePushDown
	flagEliminateOuterJoin
	flagPartitionProcessor
	flagPushDownAgg
	flagPushDownTopN
	flagJoinReOrder
)

var optRuleList = []logicalOptRule{
	&columnPruner{},
	&buildKeySolver{},
	&decorrelateSolver{},
	&aggregationEliminator{},
	&projectionEliminater{},
	&maxMinEliminator{},
	&ppdSolver{},
	&outerJoinEliminator{},
	&partitionProcessor{},
	&aggregationPushDownSolver{},
	&pushDownTopNOptimizer{},
	&joinReOrderSolver{},
}

// logicalOptRule means a logical optimizing rule, which contains decorrelate, ppd, column pruning, etc.
type logicalOptRule interface {
	optimize(LogicalPlan) (LogicalPlan, error)
}

// BuildLogicalPlan used to build logical plan from ast.Node.
func BuildLogicalPlan(ctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (Plan, error) {
	ctx.GetSessionVars().PlanID = 0
	ctx.GetSessionVars().PlanColumnID = 0
	builder := &PlanBuilder{
		ctx:       ctx,
		is:        is,
		colMapper: make(map[*ast.ColumnNameExpr]int),
	}
	p, err := builder.Build(node)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// CheckPrivilege checks the privilege for a user.
func CheckPrivilege(activeRoles []*auth.RoleIdentity, pm privilege.Manager, vs []visitInfo) error {
	for _, v := range vs {
		if !pm.RequestVerification(activeRoles, v.db, v.table, v.column, v.privilege) {
			if v.err == nil {
				return ErrPrivilegeCheckFail
			}
			return v.err
		}
	}
	return nil
}

// DoOptimize optimizes a logical plan to a physical plan.
func DoOptimize(flag uint64, logic LogicalPlan) (PhysicalPlan, error) {
	logic, err := logicalOptimize(flag, logic)
	if err != nil {
		return nil, err
	}
	if !AllowCartesianProduct.Load() && existsCartesianProduct(logic) {
		return nil, errors.Trace(ErrCartesianProductUnsupported)
	}
	physical, err := physicalOptimize(logic)
	if err != nil {
		return nil, err
	}
	finalPlan := postOptimize(physical)
	return finalPlan, nil
}

func postOptimize(plan PhysicalPlan) PhysicalPlan {
	plan = eliminatePhysicalProjection(plan)
	plan = injectExtraProjection(plan)
	return plan
}

// func logicalOptimize(flag uint64, logic LogicalPlan) (LogicalPlan, error) {
// 	var err error
// 	for i, rule := range optRuleList {
// 		// The order of flags is same as the order of optRule in the list.
// 		// We use a bitmask to record which opt rules should be used. If the i-th bit is 1, it means we should
// 		// apply i-th optimizing rule.
// 		if flag&(1<<uint(i)) == 0 {
// 			continue
// 		}
// 		logic, err = rule.optimize(logic)
// 		if err != nil {
// 			return nil, err
// 		}
// 	}
// 	return logic, err
// }

func randInt(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return min + rand.Intn(max-min)
}

func logicalOptimize(flag uint64, logic LogicalPlan) (LogicalPlan, error) {
	// get the useable logical rule by flag
	rules := make([]logicalOptRule,0,)
	for i, rule := range optRuleList  {
		if flag&(1<<uint(i)) == 0 {
			continue
		}
		//number of rules will not over 10, so there will not be a bottleneck
		rules = append(rules, rule)
	}

	var err error
	count := len(rules)
	for count > 0 {
		idx := randInt(0, count-1)
		rule := rules[idx]

		logic, err = rule.optimize(logic)
		if err != nil {
			return nil, err
		}

		// reset the optRuleList
		rules[idx], rules[count-1] = rules[count-1], rules[idx]
		count--
		rules = rules[:count-1]
	}
	return logic, err
}

func physicalOptimize(logic LogicalPlan) (PhysicalPlan, error) {
	if _, err := logic.recursiveDeriveStats(); err != nil {
		return nil, err
	}

	logic.preparePossibleProperties()

	prop := &property.PhysicalProperty{
		TaskTp:      property.RootTaskType,
		ExpectedCnt: math.MaxFloat64,
	}

	t, err := logic.findBestTask(prop)
	if err != nil {
		return nil, err
	}
	if t.invalid() {
		return nil, ErrInternal.GenWithStackByArgs("Can't find a proper physical plan for this query")
	}

	err = t.plan().ResolveIndices()
	return t.plan(), err
}

func existsCartesianProduct(p LogicalPlan) bool {
	if join, ok := p.(*LogicalJoin); ok && len(join.EqualConditions) == 0 {
		return join.JoinType == InnerJoin || join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin
	}
	for _, child := range p.Children() {
		if existsCartesianProduct(child) {
			return true
		}
	}
	return false
}

func init() {
	expression.EvalAstExpr = evalAstExpr
}
