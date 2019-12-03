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
	"github.com/pingcap/tidb/util/chunk"
)

// IndexAdviseExec represents a index advise executor.
type IndexAdviseExec struct {
	baseExecutor

	IsLocal         bool
	indexAdviseInfo *IndexAdviseInfo
}

// Next implements the Executor Next interface.
func (e *IndexAdviseExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if !e.IsLocal {
		return errors.New("Index Advise: don't support load data without local field")
	}

	if val := e.ctx.Value(IndexAdviseKey); val != nil {
		e.ctx.SetValue(IndexAdviseKey, nil)
		return errors.New("Index Advise: there is already an IndexAdviseExec running in the session")
	}
	e.ctx.SetValue(IndexAdviseKey, e.indexAdviseInfo)
	return nil
}

// Close implements the Executor Close interface.
func (e *IndexAdviseExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (e *IndexAdviseExec) Open(ctx context.Context) error {
	return nil
}

// IndexAdviseInfo saves the information of index advise operation.
type IndexAdviseInfo struct {
	Path        string
	MaxMinutes  uint64
	MaxIndexNum *ast.MaxIndexNumClause
	LinesInfo   *ast.LinesClause
	Ctx         sessionctx.Context
	StmtNodes   [][]ast.StmtNode
	Result      *IndexAdvice
}

// BuildIndexAdviseExec is a special executor builder for index advise.
/*
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
*/

func (e *IndexAdviseInfo) getSqls(data []byte) []string {
	str := *(*string)(unsafe.Pointer(&data))
	// TODO: Do some work by LineInfo.
	strs := strings.Split(str, ";")
	return strs
}

func (e *IndexAdviseInfo) getStmtNodes(data []byte) error {
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

// GetIndexAdvice gets the index advice by workload file.
func (e *IndexAdviseInfo) GetIndexAdvice(ctx context.Context, data []byte) error {
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
