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

package executor

import (
	"context"
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
)

// IndexAdviseExec represents a special executor which used to advise index.
type IndexAdviseExec struct {
	IsLocal     bool
	Path        string
	MaxMinutes  uint64
	MaxIndexNum *ast.MaxIndexNumClause
	LinesInfo   *ast.LinesClause
	Ctx         sessionctx.Context
	StmtNodes   [][]ast.StmtNode
	Result      *IndexAdvice
	// TODO: Some member variables required during execution can be defined here.
}

// BuildIndexAdviseExec is a special executor builder for index advise.
func BuildIndexAdviseExec(ctx sessionctx.Context, node *ast.IndexAdviseStmt) error {
	// TODO: Do some judgment when building the exec with AST node
	e := &IndexAdviseExec{
		IsLocal:     node.IsLocal,
		Path:        node.Path,
		MaxMinutes:  node.MaxMinutes,
		MaxIndexNum: node.MaxIndexNum,
		LinesInfo:   node.LinesInfo,
		Ctx:         ctx,
	}
	if !e.IsLocal {
		return errors.New("Index Advise: don't support load data without local field")
	}
	if e.Path == "" {
		return errors.New("Index Advise: infile path is empty")
	}
	if val := ctx.Value(IndexAdviseKey); val != nil {
		return errors.New("Index Advise: there is already an IndexAdviseExec running in the session")
	}
	ctx.SetValue(IndexAdviseKey, e)
	return nil
}

func (e *IndexAdviseExec) getSqls(data []byte) []string {
	str := *(*string)(unsafe.Pointer(&data))
	// TODO: Do some work by LineInfo.
	strs := strings.Split(str, ";")
	return strs
}

func (e *IndexAdviseExec) getStmtNodes(data []byte) error {
	sqls := e.getSqls(data)
	e.StmtNodes = make([][]ast.StmtNode, len(sqls))
	for i, sql := range sqls {
		sqlParser := parser.New()
		stmtNodes, _, err := sqlParser.Parse(sql, "", "")
		if err != nil {
			return err
		}
		e.StmtNodes[i] = stmtNodes
	}
	return nil
}

// GetIndexAdvice gets the index advise by workload file.
func (e *IndexAdviseExec) GetIndexAdvice(ctx context.Context, data []byte) error {
	if err := e.getStmtNodes(data); err != nil {
		return err
	}
	// TODO: Finish the index advise process
	return nil
}

// IndexAdvice represents the index advice.
type IndexAdvice struct {
	// TODO: Define index advice data structure.
	// TODO: Implements the ResultSet interface.
}

// IndexAdviseKeyType is a dummy type to avoid naming collision in context.
type IndexAdviseKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k IndexAdviseKeyType) String() string {
	return "index_advise_var"
}

// IndexAdviseKey is a variable key for index advise.
const IndexAdviseKey IndexAdviseKeyType = 0
