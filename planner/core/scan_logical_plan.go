// code by piotr newt for experiment

package core

import (
	"encoding/json"
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
	seq := logic.GetFeature()
	rm := &RequestMessage{
		SQL:            sql,
		LogicalPlanSeq: seq,
	}
	return rm
}

func getFeatureAsJSON(logic LogicalPlan) []byte {
	b, err := json.Marshal(logic)
	if err != nil {
		return []byte{}
	}
	return b
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
		seq += child.GetFeature()
	}
	return seq
}

// GetFeature implements the Feature GetFeature interface.
func (p *baseLogicalPlan) GetFeature() string {
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (join *LogicalJoin) GetFeature() string {
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

// GetFeature implements the Feature GetFeature interface.
func (agg *LogicalAggregation) GetFeature() string {
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

// GetFeature implements the Feature GetFeature interface.
func (proj *LogicalProjection) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (sele *LogicalSelection) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (apply *LogicalApply) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (m *LogicalMaxOneRow) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (td *LogicalTableDual) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (d *DataSource) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (g *TiKVSingleGather) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (tabScan *LogicalTableScan) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (idxScan LogicalIndexScan) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (u *LogicalUnionAll) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (sort *LogicalSort) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (lock *LogicalLock) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (limit *LogicalLimit) GetFeature() string {
	// TODO
	return ""
}

// GetFeature implements the Feature GetFeature interface.
func (win *LogicalWindow) GetFeature() string {
	// TODO
	return ""
}
