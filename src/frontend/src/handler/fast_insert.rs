// Copyright 2024 RisingWave Labs
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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Format;
use risingwave_batch::worker_manager::worker_node_manager::WorkerNodeSelector;
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{FunctionId, Schema};
use risingwave_common::session_config::QueryMode;
use risingwave_common::types::{DataType, Datum};
use risingwave_sqlparser::ast::{SetExpr, Statement};

use super::extended_handle::{PortalResult, PrepareStatement, PreparedResult};
use super::{create_mv, declare_cursor, PgResponseStream, RwPgResponse};
use crate::binder::{Binder, BoundCreateView, BoundStatement};
use crate::catalog::TableId;
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::flush::do_flush;
use crate::handler::privilege::resolve_privileges;
use crate::handler::query::{gen_batch_plan_by_statement, gen_batch_plan_fragmenter};
use crate::handler::util::{to_pg_field, DataChunkToRowSetAdapter};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::Explain;
use crate::optimizer::{
    ExecutionModeDecider, OptimizerContext, OptimizerContextRef, ReadStorageTableVisitor,
    RelationCollectorVisitor, SysTableVisitor,
};
use crate::planner::Planner;
use crate::scheduler::plan_fragmenter::Query;
use crate::scheduler::{
    BatchPlanFragmenter, DistributedQueryStream, ExecutionContext, ExecutionContextRef,
    LocalQueryExecution, LocalQueryStream,
};
use crate::session::SessionImpl;
use crate::PlanRef;

pub async fn gen_fast_insert_execution(
    handler_args: HandlerArgs,
    stmt: Statement,
    formats: Vec<Format>,
) -> Result<RwPgResponse> {
    assert!(matches!(stmt, Statement::Insert { .. }));
    let session = handler_args.session.clone();

    let plan_fragmenter_result = {
        let context = OptimizerContext::from_handler_args(handler_args);
        let plan_result = gen_batch_plan_by_statement(&session, context.into(), stmt)?;
        gen_batch_plan_fragmenter(&session, plan_result)?
    };

    let BatchPlanFragmenterResult {
        plan_fragmenter,
        _query_mode,
        schema,
        stmt_type,
        read_storage_tables,
    } = plan_fragmenter_result;
    assert!(matches!(stmt_type, StatementType::INSERT));

    // Acquire the write guard for DML statements.
    session.txn_write_guard()?;

    let query = plan_fragmenter.generate_complete_query().await?;
    tracing::trace!("Generated query after plan fragmenter: {:?}", &query);

    let pg_descs = schema
        .fields()
        .iter()
        .map(to_pg_field)
        .collect::<Vec<PgFieldDescriptor>>();
    let column_types = schema.fields().iter().map(|f| f.data_type()).collect_vec();

    let stream = PgResponseStream::LocalQuery(DataChunkToRowSetAdapter::new(
        local_execute(
            session.clone(),
            query,
            can_timeout_cancel,
            &read_storage_tables,
        )
        .await?,
        column_types,
        formats,
        session.clone(),
    ));

    let (row_stream, pg_descs) =
        create_stream(session.clone(), plan_fragmenter_result, formats).await?;
    todo!()
}

pub async fn local_execute(
    session: Arc<SessionImpl>,
    query: Query,
    can_timeout_cancel: bool,
    read_storage_tables: &HashSet<TableId>,
) -> Result<LocalQueryStream> {
    let timeout = None;
    let front_env = session.env();

    let snapshot = session.pinned_snapshot();

    // TODO: Passing sql here
    let execution = LocalQueryExecution::new(
        query,
        front_env.clone(),
        "",
        snapshot.support_barrier_read(),
        snapshot.batch_query_epoch(read_storage_tables)?,
        session,
        timeout,
    );

    Ok(execution.stream_rows())
}
