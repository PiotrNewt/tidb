// code by piotr newt for experiment

package core

import (
	"github.com/pingcap/tidb/sessionctx"
)

// GetFeatureOFLogicalPlan scan the logical plan from top node to the buttom.
// it will return info of all nodes.
func GetFeatureOFLogicalPlan(logic LogicalPlan) string {
	planStr := logic.TP()
	children := logic.Children()
	if len(children) == 0 {
		return planStr
	}

	for _, child := range children {
		planStr += " / " + GetFeatureOFLogicalPlan(child)
	}
	return planStr
}

// TODO: we need more infomation of logical plan.
// logical plan which will be optimize by rule we need get more info.
// other logical plan just need type.
// we don't need consider hint information.

// RequestMessage represent message from tidb to rl-server. (just for experiment)
type RequestMessage struct {
	SQL            string
	LogicalPlanSeq string // LogicalPlanSeq will like "{s[s(e<a>)]}"
}

func getFeature(ctx sessionctx.Context, logic LogicalPlan) *RequestMessage {
	sctx := logic.SCtx()
	sessionVars := sctx.GetSessionVars()
	sql := sessionVars.StmtCtx.OriginalSQL
	rm := &RequestMessage{
		SQL: sql,
	}
	return logic.Traversal(rm)
}

// Feature interface use to extract feature of logical plan.
type Feature interface {
	// Traversal use to traversal logical plan
	Traversal()
}

// Traversal implements the Feature Traversal interface.
// func (join *LogicalJoin) Traversal() {}

// Traversal implements the Feature Traversal interface.
func (agg *LogicalAggregation) Traversal(r *RequestMessage) *RequestMessage {
	collectSeq := agg.TP()
	aggfuncStr := ""
	if len(agg.AggFuncs) != 0 {
		for _, f := range agg.AggFuncs {
			// "(" + + ")"
			aggfuncStr += "(aggfunc "
			aggfuncStr += "<name-" + f.baseFuncDesc.Name + ">"
			aggfuncStr += "<mode-" + f.Mode + ">"
			aggfuncStr += ")"
		}
	}

	groupByItemStr := ""
	if len(agg.GroupByItems) != 0 {
		for _, e := range agg.GroupByItems {
			groupByItemStr += "(groupByItems "
			groupByItemStr += "<info-" + e.ExplainInfo() + ">"
			co := "0"
			if e.IsCorrelated {
				co = "1"
			}
			groupByItrmStr += "<correlated-" + co + ">"
		}
	}

	groupByColStr := ""
	if len(agg.groupByCols) != 0 {
		for _, c := range agg.groupByCols {
			groupByColStr += "(groupByCols "
			groupByColStr += "<id-" + c.UniqueID + ">"
			groupByColStr += "<name-" + c.OrigName + ">"
			b := "0"
			if c.InOperand {
				b = "1"
			}
			groupByColStr += "<isInOperand" + b + ">"
			groupByColStr += "(groupByCols "
		}
	}

	collectSeq += "[" + aggfuncStr + groupByItem + groupByColStr
	children := agg.Children()
	if len(children) == 0 {
		r.LogicalPlanSeq += collectSeq + "]"
		return r
	}

	for child := range chilren {
		r.LogicalPlanSeq += chilren.Traversal(r)
	}
	r.LogicalPlanSeq += chilren.Traversal(r) + "]"
	return r
}

/*
// Traversal implements the Feature Traversal interface.
func (proj *LogicalProjection) Traversal() {}

// Traversal implements the Feature Traversal interface.
func (sele *LogicalSelection) Traversal() {}

// Traversal implements the Feature Traversal interface.
func (apply *LogicalApply) Traversal() {}

// Traversal implements the Feature Traversal interface.
func (m *LogicalMaxOneRow) Traversal() {}

// Traversal implements the Feature Traversal interface.
func (td *LogicalTableDual) Traversal() {}

// Traversal implements the Feature Traversal interface.
func (d *DataSource) Traversal() {}

// Traversal implements the Feature Traversal interface.
func (g *TiKVSingleGather) Traversal() {}

// Traversal implements the Feature Traversal interface.
func (tabScan *LogicalTableScan) Traversal() {}

// Traversal implements the Feature Traversal interface.
func (idxScan LogicalIndexScan) Traversal() {}

// Traversal implements the Feature Traversal interface.
func (u *LogicalUnionAll) Traversal() {}

// Traversal implements the Feature Traversal interface.
func (sort *LogicalSort) Traversal() {}

// Traversal implements the Feature Traversal interface.
func (lock *LogicalLock) Traversal() {}

// Traversal implements the Feature Traversal interface.
func (limit *LogicalLimit) Traversal() {}

// Traversal implements the Feature Traversal interface.
func (win *LogicalWindow) Traversal() {}
*/
