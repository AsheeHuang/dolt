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

package binlogreplication

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"
)

// DoltBinlogPrimaryController implements the binlogreplication.BinlogPrimaryController
// interface from GMS and is the main extension point where Dolt plugs in to GMS and
// interprets commands and statements related to serving binlog events.
type DoltBinlogPrimaryController struct {
	streamerManager *binlogStreamerManager
	BinlogProducer  *binlogProducer
}

var _ binlogreplication.BinlogPrimaryController = (*DoltBinlogPrimaryController)(nil)

// NewDoltBinlogPrimaryController creates a new DoltBinlogPrimaryController instance.
func NewDoltBinlogPrimaryController() *DoltBinlogPrimaryController {
	controller := DoltBinlogPrimaryController{
		streamerManager: newBinlogStreamerManager(),
	}
	return &controller
}

func (d *DoltBinlogPrimaryController) StreamerManager() *binlogStreamerManager {
	return d.streamerManager
}

// RegisterReplica implements the BinlogPrimaryController interface.
//
// NOTE: This method is invoked from a replica sending a command before the replica requests to start streaming the
// binlog events. We don't currently record the information on registered replicas, but we will eventually need it
// to implement the ListReplicas method below. For now, this method is still useful to throw errors back to the
// replica if bin logging isn't enabled, since errors returned from the BinlogDumpGtid method seem to be dropped
// by the replica, instead of being displayed as an error.
func (d *DoltBinlogPrimaryController) RegisterReplica(ctx *sql.Context, c *mysql.Conn, replicaHost string, replicaPort uint16) error {
	if d.BinlogProducer == nil {
		return fmt.Errorf("no binlog currently being recorded; make sure the server is started with @@log_bin enabled")
	}

	return nil
}

// BinlogDumpGtid implements the BinlogPrimaryController interface.
func (d *DoltBinlogPrimaryController) BinlogDumpGtid(ctx *sql.Context, conn *mysql.Conn, replicaExecutedGtids mysql.GTIDSet) error {
	if d.BinlogProducer == nil {
		// TODO: Add a test for this, now that we have errors being
		//       reported in replica status correctly
		return mysql.NewSQLError(mysql.ERMasterFatalReadingBinlog, "HY000",
			"no binlog currently being recorded; make sure the server is started with @@log_bin enabled")

	}

	primaryExecutedGtids := d.BinlogProducer.gtidPosition.GTIDSet
	// TODO: This is awkward... should controller have a direct link to log manager?
	//       Or should BinlogProducer have a direct link to the log manager?
	missingGtids := d.BinlogProducer.streamerManager.logManager.calculateMissingGtids(replicaExecutedGtids, primaryExecutedGtids)
	if !missingGtids.Equal(mysql.Mysql56GTIDSet{}) {
		// We must send back error code 1236 (ER_MASTER_FATAL_ERROR_READING_BINLOG) to the replica to signal an error,
		// otherwise the replica won't expose the error in replica status and will just keep trying to reconnect and
		// only log the error to MySQL's error log.
		return mysql.NewSQLError(mysql.ERMasterFatalReadingBinlog, "HY000",
			"Cannot replicate because the source purged required binary logs. Replicate the missing transactions from elsewhere, or provision a new replica from backup. Consider increasing the source's binary log expiration period. The GTID set sent by the replica is '%s', and the missing transactions are '%s'.",
			replicaExecutedGtids.String(), missingGtids.String())
	}

	err := d.streamerManager.StartStream(ctx, conn, replicaExecutedGtids, d.BinlogProducer.binlogFormat, d.BinlogProducer.binlogEventMeta)
	if err != nil {
		logrus.Warnf("exiting binlog streamer due to error: %s", err.Error())
	} else {
		logrus.Trace("exiting binlog streamer cleanly")
	}

	return err
}

// ListReplicas implements the BinlogPrimaryController interface.
func (d *DoltBinlogPrimaryController) ListReplicas(ctx *sql.Context) error {
	return fmt.Errorf("ListReplicas not implemented in Dolt yet")
}

// ListBinaryLogs implements the BinlogPrimaryController interface.
func (d *DoltBinlogPrimaryController) ListBinaryLogs(_ *sql.Context) ([]binlogreplication.BinaryLogFileMetadata, error) {
	logManager := d.streamerManager.logManager
	logFiles, err := logManager.logFilesOnDiskForBranch(BinlogBranch)
	if err != nil {
		return nil, err
	}

	logFileMetadata := make([]binlogreplication.BinaryLogFileMetadata, len(logFiles))
	for i, logFile := range logFiles {
		fileStats, err := os.Stat(filepath.Join(logManager.binlogDirectory, logFile))
		if err != nil {
			return nil, err
		}
		logFileMetadata[i] = binlogreplication.BinaryLogFileMetadata{
			Name: logFile,
			Size: uint64(fileStats.Size()),
		}
	}
	return logFileMetadata, nil
}

// GetBinaryLogStatus implements the BinlogPrimaryController interface.
func (d *DoltBinlogPrimaryController) GetBinaryLogStatus(ctx *sql.Context) ([]binlogreplication.BinaryLogStatus, error) {
	return []binlogreplication.BinaryLogStatus{{
		File:          d.streamerManager.logManager.currentBinlogFileName,
		Position:      uint(d.streamerManager.logManager.currentPosition),
		ExecutedGtids: d.BinlogProducer.currentGtidPosition(),
	}}, nil
}
