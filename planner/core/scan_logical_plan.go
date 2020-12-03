// code by piotr newt for experiment

package core

import (
	"strconv"

	"github.com/pingcap/tidb/expression"
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

func getFeature(logic LogicalPlan) *RequestMessage {
	sctx := logic.SCtx()
	sessionVars := sctx.GetSessionVars()
	sql := sessionVars.StmtCtx.OriginalSQL
	seq := logic.Traversal()
	rm := &RequestMessage{
		SQL:            sql,
		LogicalPlanSeq: seq,
	}
	return rm
}

// Feature interface use to extract feature of logical plan.
type Feature interface {
	// Traversal use to traversal logical plan
	Traversal() string
}

func bool2str(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func getExpressionStr(e expression.Expression) string {
	s := "(exprs "
	s += "<info-" + e.ExplainInfo() + ">"
	s += "<correlated-" + bool2str(e.IsCorrelated()) + ">)"
	return s
}

func getExpressionArrayStr(es []expression.Expression) string {
	if len(es) == 0 {
		return ""
	}

	s := ""
	for _, e := range es {
		s += getExpressionStr(e)
	}
	return s
}

func getExpressionColsStr(col *expression.Column) string {
	s := "(col"
	s += "<id-" + strconv.FormatInt(col.UniqueID, 10) + ">"
	s += "<name-" + col.OrigName + ">"
	s += "<isInOperand" + bool2str(col.InOperand) + ">)"
	return s
}

func getExpressionColsArrayStr(cols []*expression.Column) string {
	if len(cols) == 0 {
		return ""
	}

	s := ""
	for _, c := range cols {
		s += getExpressionColsStr(c)
	}
	return s
}

func getChildrenSeq(l LogicalPlan) string {
	children := l.Children()
	seq := ""
	if len(children) == 0 {
		return seq
	}
	for _, child := range children {
		seq += child.Traversal()
	}
	return seq
}

// Traversal implements the Feature Traversal interface.
func (p *baseLogicalPlan) Traversal() string {
	return ""
}

// Traversal implements the Feature Traversal interface.
func (join *LogicalJoin) Traversal() string {
	collectSeq := join.TP()
	joinInfoStr := "(info "
	joinInfoStr += "<jointype-" + strconv.Itoa(int(join.JoinType)) + ">"
	joinInfoStr += "<reorderd-" + bool2str(join.reordered) + ">"
	joinInfoStr += "<cartesian-" + bool2str(join.cartesianJoin) + ">"
	joinInfoStr += "<straightJoin-" + bool2str(join.StraightJoin) + ">"
	joinInfoStr += "<preferJoinType-" + strconv.Itoa(int(join.preferJoinType)) + ">)"

	conditionStr := ""
	if join.EqualConditions != nil && len(join.EqualConditions) != 0 {
		for _, c := range join.EqualConditions {
			coditionStr := "(scalarFunction "
			coditionStr += "<" + c.FuncName.O + c.FuncName.L + ">"
			coditionStr += ")"
		}
	}

	if join.LeftConditions != nil {
		conditionStr += getExpressionArrayStr(join.LeftConditions)
	}
	if join.RightConditions != nil {
		conditionStr += getExpressionArrayStr(join.RightConditions)
	}
	if join.OtherConditions != nil {
		conditionStr += getExpressionArrayStr(join.OtherConditions)
	}

	columnsStr := ""
	if join.leftProperties != nil {
		for _, cols := range join.leftProperties {
			columnsStr += getExpressionColsArrayStr(cols)
		}
	}
	if join.rightProperties != nil {
		for _, cols := range join.rightProperties {
			columnsStr += getExpressionColsArrayStr(cols)
		}
	}
	collectSeq += "[" + joinInfoStr + conditionStr + columnsStr + getChildrenSeq(join) + "]"
	return collectSeq
}

// Traversal implements the Feature Traversal interface.
func (agg *LogicalAggregation) Traversal() string {
	collectSeq := agg.TP()
	aggfuncStr := ""
	if agg.AggFuncs != nil && len(agg.AggFuncs) != 0 {
		for _, f := range agg.AggFuncs {
			// "(" + + ")"
			aggfuncStr += "(aggfunc "
			aggfuncStr += "<name-" + f.Name + ">"
			aggfuncStr += "<mode-" + strconv.Itoa(int(f.Mode)) + ">"
			aggfuncStr += ")"
		}
	}

	groupByItemStr := ""
	if len(agg.GroupByItems) != 0 {
		for _, e := range agg.GroupByItems {
			groupByItemStr += getExpressionStr(e)
		}
	}

	groupByColStr := ""
	if len(agg.groupByCols) != 0 {
		groupByColStr += getExpressionColsArrayStr(agg.groupByCols)
	}

	collectSeq += "[" + aggfuncStr + groupByItemStr + groupByColStr + getChildrenSeq(agg) + "]"
	return collectSeq
}

// Traversal implements the Feature Traversal interface.
func (proj *LogicalProjection) Traversal() string {
	// TODO
	return ""
}

// Traversal implements the Feature Traversal interface.
func (sele *LogicalSelection) Traversal() string {
	// TODO
	return ""
}

// Traversal implements the Feature Traversal interface.
func (apply *LogicalApply) Traversal() string {
	// TODO
	return ""
}

// Traversal implements the Feature Traversal interface.
func (m *LogicalMaxOneRow) Traversal() string {
	// TODO
	return ""
}

// Traversal implements the Feature Traversal interface.
func (td *LogicalTableDual) Traversal() string {
	// TODO
	return ""
}

// Traversal implements the Feature Traversal interface.
func (d *DataSource) Traversal() string {
	// TODO
	return ""
}

// Traversal implements the Feature Traversal interface.
func (g *TiKVSingleGather) Traversal() string {
	// TODO
	return ""
}

// Traversal implements the Feature Traversal interface.
func (tabScan *LogicalTableScan) Traversal() string {
	// TODO
	return ""
}

// Traversal implements the Feature Traversal interface.
func (idxScan LogicalIndexScan) Traversal() string {
	// TODO
	return ""
}

// Traversal implements the Feature Traversal interface.
func (u *LogicalUnionAll) Traversal() string {
	// TODO
	return ""
}

// Traversal implements the Feature Traversal interface.
func (sort *LogicalSort) Traversal() string {
	// TODO
	return ""
}

// Traversal implements the Feature Traversal interface.
func (lock *LogicalLock) Traversal() string {
	// TODO
	return ""
}

// Traversal implements the Feature Traversal interface.
func (limit *LogicalLimit) Traversal() string {
	// TODO
	return ""
}

// Traversal implements the Feature Traversal interface.
func (win *LogicalWindow) Traversal() string {
	// TODO
	return ""
}

// Traversal implements the Feature Traversal interface.
func (l *LogicalUnionScan) Traversal() string {
	//TODO
	return ""
}

// Traversal implements the Feature Traversal interface.
func (topn *LogicalTopN) Traversal() string {
	// TODO
	return ""
}

// Traversal implements the Feature Traversal interface.
func (s *logicalSchemaProducer) Traversal() string {
	return ""
}
