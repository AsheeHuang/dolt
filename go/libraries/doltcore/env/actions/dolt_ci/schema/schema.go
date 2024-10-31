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

package schema

import (
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/dolt_ci"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas"
)

func HasDoltCITables(ctx *sql.Context) (bool, error) {
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
	for _, tableName := range dolt_ci.ExpectedDoltCITablesOrdered {
		found, err := root.HasTable(ctx, tableName)
		if err != nil {
			return false, err
		}
		if found {
			exists++
		}
	}

	hasSome = exists > 0 && exists < len(dolt_ci.ExpectedDoltCITablesOrdered)
	hasAll = exists == len(dolt_ci.ExpectedDoltCITablesOrdered)
	if !hasSome && !hasAll {
		return false, nil
	}
	if hasSome && !hasAll {
		return true, fmt.Errorf("found some but not all of required dolt ci tables")
	}
	return true, nil
}

func CreateDoltCITables(ctx *sql.Context, db sqle.Database, commiterName, commiterEmail string) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}

	err := createDoltCITables(ctx)
	if err != nil {
		return err
	}

	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)

	ddb, exists := dSess.GetDoltDB(ctx, dbName)
	if !exists {
		return fmt.Errorf("database not found in database %s", dbName)
	}

	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return fmt.Errorf("roots not found in database %s", dbName)
	}

	roots, err = actions.StageTables(ctx, roots, dolt_ci.ExpectedDoltCITablesOrdered, true)
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

	meta, err := datas.NewCommitMeta(commiterName, commiterEmail, "Successfully created Dolt CI tables")
	if err != nil {
		return err
	}

	pcm, err := ddb.NewPendingCommit(ctx, roots, parents, meta)
	if err != nil {
		return err
	}

	wsMeta := &datas.WorkingSetMeta{
		Name:      commiterName,
		Email:     commiterEmail,
		Timestamp: uint64(time.Now().Unix()),
	}
	_, err = ddb.CommitWithWorkingSet(ctx, pRef, wRef, pcm, ws, wsHash, wsMeta, nil)
	return err
}

func createDoltCITables(ctx *sql.Context) error {
	// creation order matters here
	// for foreign key creation
	err := createWorkflowsTable(ctx)
	if err != nil {
		return err
	}
	err = createWorkflowEventsTable(ctx)
	if err != nil {
		return err
	}
	err = createWorkflowEventTriggersTable(ctx)
	if err != nil {
		return err
	}
	err = createWorkflowEventTriggerBranchesTable(ctx)
	if err != nil {
		return err
	}
	err = createWorkflowEventTriggerActivitiesTable(ctx)
	if err != nil {
		return err
	}
	err = createWorkflowJobsTable(ctx)
	if err != nil {
		return err
	}
	err = createWorkflowStepsTable(ctx)
	if err != nil {
		return err
	}
	err = createWorkflowSavedQueryStepsTable(ctx)
	if err != nil {
		return err
	}
	return createWorkflowSavedQueryStepExpectedRowColumnResultsTable(ctx)
}
