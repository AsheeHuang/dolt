// Copyright 2025 Dolthub, Inc.
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

package dtables

import (
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
)

// ConflictRowData holds the conflict row values and metadata
type ConflictRowData struct {
	K, BV, OV, TV val.Tuple
	H             hash.Hash
	ID            string
}

// ConflictRowIterator represents the common interface for conflict row iterators
type ConflictRowIterator interface {
	GetTableName() doltdb.TableName
	GetValueReadWriter() types.ValueReadWriter
	GetNodeStore() tree.NodeStore
	GetOurRows() prolly.Map
	GetOurSchema() schema.Schema
	GetKeyDescriptor() val.TupleDesc
	GetBaseValueDescriptor() val.TupleDesc
	GetOursValueDescriptor() val.TupleDesc
	GetTheirsValueDescriptor() val.TupleDesc
	GetOffsets() (b, o, t, n int)
	IsKeyless() bool
	LoadTableMaps(ctx *sql.Context, baseHash, theirHash hash.Hash, baseRows, theirRows *prolly.Map, baseHashRef, theirHashRef *hash.Hash) error
}

// PutConflictRowVals populates the conflict row values for non-keyless tables
func PutConflictRowVals(ctx *sql.Context, itr ConflictRowIterator, c ConflictRowData, r sql.Row, baseRows prolly.Map) error {
	kd := itr.GetKeyDescriptor()
	baseVD := itr.GetBaseValueDescriptor()
	oursVD := itr.GetOursValueDescriptor()
	theirsVD := itr.GetTheirsValueDescriptor()
	b, o, t, _ := itr.GetOffsets()

	if c.BV != nil {
		for i := 0; i < baseVD.Count(); i++ {
			f, err := tree.GetField(ctx, baseVD, i, c.BV, baseRows.NodeStore())
			if err != nil {
				return err
			}
			r[b+kd.Count()+i] = f
		}
	}

	if c.OV != nil {
		for i := 0; i < oursVD.Count(); i++ {
			f, err := tree.GetField(ctx, oursVD, i, c.OV, baseRows.NodeStore())
			if err != nil {
				return err
			}
			r[o+kd.Count()+i] = f
		}
	}
	r[o+kd.Count()+oursVD.Count()] = GetConflictDiffType(c.BV, c.OV)

	if c.TV != nil {
		for i := 0; i < theirsVD.Count(); i++ {
			f, err := tree.GetField(ctx, theirsVD, i, c.TV, baseRows.NodeStore())
			if err != nil {
				return err
			}
			r[t+kd.Count()+i] = f
		}
	}
	r[t+kd.Count()+theirsVD.Count()] = GetConflictDiffType(c.BV, c.TV)
	r[t+kd.Count()+theirsVD.Count()+1] = c.ID

	return nil
}

// PutKeylessConflictRowVals populates the conflict row values for keyless tables
func PutKeylessConflictRowVals(ctx *sql.Context, itr ConflictRowIterator, c ConflictRowData, r sql.Row, baseRows prolly.Map) error {
	ns := baseRows.NodeStore()
	baseVD := itr.GetBaseValueDescriptor()
	oursVD := itr.GetOursValueDescriptor()
	theirsVD := itr.GetTheirsValueDescriptor()
	b, o, t, n := itr.GetOffsets()

	if c.BV != nil {
		// Cardinality
		card, err := tree.GetField(ctx, baseVD, 0, c.BV, ns)
		if err != nil {
			return err
		}
		r[n-3] = card

		for i := 0; i < baseVD.Count()-1; i++ {
			f, err := tree.GetField(ctx, baseVD, i+1, c.BV, ns)
			if err != nil {
				return err
			}
			r[b+i] = f
		}
	} else {
		r[n-3] = uint64(0)
	}

	if c.OV != nil {
		card, err := tree.GetField(ctx, oursVD, 0, c.OV, ns)
		if err != nil {
			return err
		}
		r[n-2] = card

		for i := 0; i < oursVD.Count()-1; i++ {
			f, err := tree.GetField(ctx, oursVD, i+1, c.OV, ns)
			if err != nil {
				return err
			}
			r[o+i] = f
		}
	} else {
		r[n-2] = uint64(0)
	}

	r[o+oursVD.Count()-1] = GetConflictDiffType(c.BV, c.OV)

	if c.TV != nil {
		card, err := tree.GetField(ctx, theirsVD, 0, c.TV, ns)
		if err != nil {
			return err
		}
		r[n-1] = card

		for i := 0; i < theirsVD.Count()-1; i++ {
			f, err := tree.GetField(ctx, theirsVD, i+1, c.TV, ns)
			if err != nil {
				return err
			}
			r[t+i] = f
		}
	} else {
		r[n-1] = uint64(0)
	}

	offset := t + theirsVD.Count() - 1
	r[offset] = GetConflictDiffType(c.BV, c.TV)
	r[n-4] = c.ID

	return nil
}

// LoadTableMapsFromHashes loads the maps specified by the hashes if they are different from
// the currently loaded maps. |baseHash| and |theirHash| are table hashes.
func LoadTableMapsFromHashes(ctx *sql.Context, tblName doltdb.TableName, vrw types.ValueReadWriter, ns tree.NodeStore, ourSch schema.Schema, baseHash, theirHash hash.Hash, baseRows, theirRows *prolly.Map, baseHashRef, theirHashRef *hash.Hash) error {
	if baseHashRef.Compare(baseHash) != 0 {
		rv, err := doltdb.LoadRootValueFromRootIshAddr(ctx, vrw, ns, baseHash)
		if err != nil {
			return err
		}
		baseTbl, ok, err := rv.GetTable(ctx, tblName)
		if err != nil {
			return err
		}

		var idx durable.Index
		if !ok {
			idx, err = durable.NewEmptyPrimaryIndex(ctx, vrw, ns, ourSch)
		} else {
			idx, err = baseTbl.GetRowData(ctx)
		}

		if err != nil {
			return err
		}

		*baseRows, err = durable.ProllyMapFromIndex(idx)
		if err != nil {
			return err
		}

		*baseHashRef = baseHash
	}

	if theirHashRef.Compare(theirHash) != 0 {
		rv, err := doltdb.LoadRootValueFromRootIshAddr(ctx, vrw, ns, theirHash)
		if err != nil {
			return err
		}

		theirTbl, ok, err := rv.GetTable(ctx, tblName)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("failed to find table %s in right root value", tblName)
		}

		idx, err := theirTbl.GetRowData(ctx)
		if err != nil {
			return err
		}
		*theirRows, err = durable.ProllyMapFromIndex(idx)
		if err != nil {
			return err
		}
		*theirHashRef = theirHash
	}

	return nil
}

// GetConflictDiffType returns the diff type for a conflict based on base and other values
func GetConflictDiffType(base val.Tuple, other val.Tuple) string {
	if base == nil {
		return merge.ConflictDiffTypeAdded
	} else if other == nil {
		return merge.ConflictDiffTypeRemoved
	}

	// There has to be some edit, otherwise it wouldn't be a conflict...
	return merge.ConflictDiffTypeModified
}