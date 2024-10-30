#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

skip_remote_engine() {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
     skip "session ctx in shell is not the same as session in server"
    fi
}

@test "ci: init should create dolt ci workflow tables" {
    skip_remote_engine

    run dolt ci init
    [ "$status" -eq 0 ]

    run dolt log -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully created Dolt CI tables" ]] || false

    run dolt sql -q "select * from dolt_ci_workflows;"
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from dolt_ci_workflow_events;"
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from dolt_ci_workflow_event_triggers;"
    [ "$status" -eq 0 ]
}

@test "ci: should not allow users to alter the rows or schema of dolt ci workflow tables directly" {
    skip_remote_engine

    run dolt ci init
    [ "$status" -eq 0 ]

    run dolt sql -q "show create table dolt_ci_workflows;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "name" ]] || false

    run dolt sql -q "insert into dolt_ci_workflows (name, created_at, updated_at) values ('workflow_1', current_timestamp, current_timestamp);"
    [ "$status" -eq 1 ]

    run dolt sql -q "alter table dolt_ci_workflows add column test_col int;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table dolt_ci_workflows cannot be altered" ]] || false

    run dolt sql -q "alter table dolt_ci_workflows drop column name;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table dolt_ci_workflows cannot be altered" ]] || false
}
