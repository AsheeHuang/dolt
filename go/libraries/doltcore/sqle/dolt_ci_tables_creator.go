// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqle

import (
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas"
)

var ExpectedDoltCITables = []doltdb.TableName{
	doltdb.TableName{Name: doltdb.WorkflowsTableName},
	doltdb.TableName{Name: doltdb.WorkflowEventsTableName},
}

type DoltCITablesCreator interface {
	// HasTables is used to check whether the database
	// already contains dolt ci tables. If any expected tables are missing,
	// an error is returned
	HasTables(ctx *sql.Context) (bool, error)

	// CreateTables creates all tables required for dolt ci
	CreateTables(ctx *sql.Context) error
}

type doltCITablesCreator struct {
	ctx              *sql.Context
	db               Database
	commiterName     string
	commiterEmail    string
	workflowsTC      DoltCITableCreator
	workflowEventsTC DoltCITableCreator
}

var _ DoltCITablesCreator = &doltCITablesCreator{}

func NewDoltCITablesCreator(ctx *sql.Context, db Database, committerName, commiterEmail string) *doltCITablesCreator {
	return &doltCITablesCreator{
		ctx:              ctx,
		db:               db,
		commiterName:     committerName,
		commiterEmail:    commiterEmail,
		workflowsTC:      NewDoltCIWorkflowsTableCreator(),
		workflowEventsTC: NewDoltCIWorkflowEventsTableCreator(),
	}
}

func (d *doltCITablesCreator) createTables(ctx *sql.Context) error {
	// TOD0: maybe CreateTable(...) should take in old RootVal and return new RootVal?
	err := d.workflowsTC.CreateTable(ctx)
	if err != nil {
		return err
	}
	return d.workflowEventsTC.CreateTable(ctx)
}

func (d *doltCITablesCreator) HasTables(ctx *sql.Context) (bool, error) {
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)
	ws, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return false, err
	}

	root := ws.WorkingRoot()

	exists := 0
	var hasSome bool
	var hasAll bool
	for _, tableName := range ExpectedDoltCITables {
		found, err := root.HasTable(ctx, tableName)
		if err != nil {
			return false, err
		}
		if found {
			exists++
		}
	}

	hasSome = exists > 0 && exists < len(ExpectedDoltCITables)
	hasAll = exists == len(ExpectedDoltCITables)
	if !hasSome && !hasAll {
		return false, nil
	}
	if hasSome && !hasAll {
		return true, fmt.Errorf("found some but not all of required dolt ci tables")
	}
	return true, nil
}

func (d *doltCITablesCreator) CreateTables(ctx *sql.Context) error {
	if err := dsess.CheckAccessForDb(d.ctx, d.db, branch_control.Permissions_Write); err != nil {
		return err
	}

	err := d.createTables(ctx)
	if err != nil {
		return err
	}

	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)

	ddb, exists := dSess.GetDoltDB(ctx, dbName)
	if !exists {
		return fmt.Errorf("database not found in database %s", dbName)
	}

	wsMeta := &datas.WorkingSetMeta{
		Name:      d.commiterName,
		Email:     d.commiterEmail,
		Timestamp: uint64(time.Now().Unix()),
	}

	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return fmt.Errorf("roots not found in database %s", dbName)
	}

	roots, err = actions.StageTables(ctx, roots, ExpectedDoltCITables, true)
	if err != nil {
		return err
	}

	ws, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return err
	}

	ws = ws.WithWorkingRoot(roots.Working)
	ws = ws.WithStagedRoot(roots.Staged)

	wsHash, err := ws.HashOf()
	if err != nil {
		return err
	}

	wRef := ws.Ref()
	pRef, err := wRef.ToHeadRef()
	if err != nil {
		return err
	}

	parent, err := ddb.ResolveCommitRef(ctx, pRef)
	if err != nil {
		return err
	}

	parents := []*doltdb.Commit{parent}

	meta, err := datas.NewCommitMeta(d.commiterName, d.commiterEmail, "Successfully created Dolt CI tables")
	if err != nil {
		return err
	}

	pcm, err := ddb.NewPendingCommit(ctx, roots, parents, meta)
	if err != nil {
		return err
	}

	_, err = ddb.CommitWithWorkingSet(ctx, pRef, wRef, pcm, ws, wsHash, wsMeta, nil)
	return err
}
