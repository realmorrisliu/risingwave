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

//! Local execution for batch query.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::anyhow;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_batch::error::BatchError;
use risingwave_batch::executor::ExecutorBuilder;
use risingwave_batch::task::{ShutdownToken, TaskId};
use risingwave_batch::worker_manager::worker_node_manager::WorkerNodeSelector;
use risingwave_common::array::DataChunk;
use risingwave_common::bail;
use risingwave_common::error::BoxedError;
use risingwave_common::hash::WorkerSlotMapping;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::tracing::{InstrumentStream, TracingContext};
use risingwave_pb::batch_plan::exchange_info::DistributionMode;
use risingwave_pb::batch_plan::exchange_source::LocalExecutePlan::Plan;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{
    ExchangeInfo, ExchangeSource, LocalExecutePlan, PbTaskId, PlanFragment, PlanNode as PbPlanNode,
    TaskOutputId,
};
use risingwave_pb::common::{BatchQueryEpoch, WorkerNode};
use tracing::debug;

use super::plan_fragmenter::{PartitionInfo, QueryStage, QueryStageRef};
use crate::catalog::{FragmentId, TableId};
use crate::error::RwError;
use crate::optimizer::plan_node::PlanNodeType;
use crate::scheduler::plan_fragmenter::{ExecutionPlanNode, Query, StageId};
use crate::scheduler::task_context::FrontendBatchTaskContext;
use crate::scheduler::{SchedulerError, SchedulerResult};
use crate::session::{FrontendEnv, SessionImpl};

pub struct FastInsertExecution {
    query: Query,
    front_env: FrontendEnv,
    batch_query_epoch: BatchQueryEpoch,
    session: Arc<SessionImpl>,
    worker_node_manager: WorkerNodeSelector,
}

impl FastInsertExecution {
    pub fn new(
        query: Query,
        front_env: FrontendEnv,
        support_barrier_read: bool,
        batch_query_epoch: BatchQueryEpoch,
        session: Arc<SessionImpl>,
    ) -> Self {
        let worker_node_manager =
            WorkerNodeSelector::new(front_env.worker_node_manager_ref(), support_barrier_read);

        Self {
            query,
            front_env,
            batch_query_epoch,
            session,
            worker_node_manager,
        }
    }

    fn shutdown_rx(&self) -> ShutdownToken {
        self.session.reset_cancel_query_flag()
    }

    #[try_stream(ok = DataChunk, error = RwError)]
    pub async fn run_inner(self) {
        let context = FrontendBatchTaskContext::new(self.session.clone());

        let task_id = TaskId {
            query_id: self.query.query_id.id.clone(),
            stage_id: 0,
            task_id: 0,
        };

        let plan_fragment = self.create_plan_fragment()?;
        let plan_node = plan_fragment.root.unwrap();

        println!("WKXLOG plan_node: {:?}", plan_node);

        let executor = ExecutorBuilder::new(
            &plan_node,
            &task_id,
            context,
            self.batch_query_epoch,
            self.shutdown_rx().clone(),
        );

        let executor = executor.build().await?;

        println!("WKXLOG executor: {:?}", executor);

        #[for_await]
        for chunk in executor.execute() {
            yield chunk?;
        }
    }

    pub fn epoch(&self) -> BatchQueryEpoch {
        self.batch_query_epoch
    }

    pub fn gen_plan(&self) -> SchedulerResult<PlanFragment> {
        let plan_fragment = self.create_plan_fragment()?;
        // let plan_node = plan_fragment.root.unwrap();
        Ok(plan_fragment)
    }

    /// Convert query to plan fragment.
    ///
    /// We can convert a query to plan fragment since in local execution mode, there are at most
    /// two layers, e.g. root stage and its optional input stage. If it does have input stage, it
    /// will be embedded in exchange source, so we can always convert a query into a plan fragment.
    ///
    /// We remark that the boundary to determine which part should be executed on the frontend and
    /// which part should be executed on the backend is `the first exchange operator` when looking
    /// from the the root of the plan to the leaves. The first exchange operator contains
    /// the pushed-down plan fragment.
    fn create_plan_fragment(&self) -> SchedulerResult<PlanFragment> {
        let next_executor_id = Arc::new(AtomicU32::new(0));
        let root_stage_id = self.query.root_stage_id();
        let root_stage = self.query.stage_graph.stages.get(&root_stage_id).unwrap();
        assert_eq!(root_stage.parallelism.unwrap(), 1);
        let second_stage_id = self.query.stage_graph.get_child_stages(&root_stage_id);
        let plan_node_prost = match second_stage_id {
            None => {
                debug!("Local execution mode converts a plan with a single stage");
                self.convert_plan_node(&root_stage.root, &mut None, None, next_executor_id)?
            }
            Some(second_stage_ids) => {
                debug!("Local execution mode converts a plan with two stages");
                if second_stage_ids.is_empty() {
                    // This branch is defensive programming. The semantics should be the same as
                    // `None`.
                    self.convert_plan_node(&root_stage.root, &mut None, None, next_executor_id)?
                } else {
                    let mut second_stages = HashMap::new();
                    for second_stage_id in second_stage_ids {
                        let second_stage =
                            self.query.stage_graph.stages.get(second_stage_id).unwrap();
                        second_stages.insert(*second_stage_id, second_stage.clone());
                    }
                    let mut stage_id_to_plan = Some(second_stages);
                    let res = self.convert_plan_node(
                        &root_stage.root,
                        &mut stage_id_to_plan,
                        None,
                        next_executor_id,
                    )?;
                    assert!(
                        stage_id_to_plan.as_ref().unwrap().is_empty(),
                        "We expect that all the child stage plan fragments have been used"
                    );
                    res
                }
            }
        };

        Ok(PlanFragment {
            root: Some(plan_node_prost),
            // Intentionally leave this as `None` as this is the last stage for the frontend
            // to really get the output of computation, which is single distribution
            // but we do not need to explicitly specify this.
            exchange_info: None,
        })
    }

    fn convert_plan_node(
        &self,
        execution_plan_node: &ExecutionPlanNode,
        second_stages: &mut Option<HashMap<StageId, QueryStageRef>>,
        partition: Option<PartitionInfo>,
        next_executor_id: Arc<AtomicU32>,
    ) -> SchedulerResult<PbPlanNode> {
        let identity = format!(
            "{:?}-{}",
            execution_plan_node.plan_node_type,
            next_executor_id.fetch_add(1, Ordering::Relaxed)
        );
        match execution_plan_node.plan_node_type {
            PlanNodeType::BatchExchange => {
                let exchange_source_stage_id = execution_plan_node
                    .source_stage_id
                    .expect("We expect stage id for Exchange Operator");
                let Some(second_stages) = second_stages.as_mut() else {
                    bail!("Unexpected exchange detected. We are either converting a single stage plan or converting the second stage of the plan.")
                };
                let second_stage = second_stages.remove(&exchange_source_stage_id).expect(
                    "We expect child stage fragment for Exchange Operator running in the frontend",
                );
                let mut node_body = execution_plan_node.node.clone();
                let sources = match &mut node_body {
                    NodeBody::Exchange(exchange_node) => &mut exchange_node.sources,
                    NodeBody::MergeSortExchange(merge_sort_exchange_node) => {
                        &mut merge_sort_exchange_node
                            .exchange
                            .as_mut()
                            .expect("MergeSortExchangeNode must have a exchange node")
                            .sources
                    }
                    _ => unreachable!(),
                };
                assert!(sources.is_empty());

                let tracing_context = TracingContext::from_current_span().to_protobuf();

                if let Some(table_scan_info) = second_stage.table_scan_info.clone()
                    && let Some(vnode_bitmaps) = table_scan_info.partitions()
                {
                    // Similar to the distributed case (StageRunner::schedule_tasks).
                    // Set `vnode_ranges` of the scan node in `local_execute_plan` of each
                    // `exchange_source`.
                    let (worker_ids, vnode_bitmaps): (Vec<_>, Vec<_>) =
                        vnode_bitmaps.clone().into_iter().unzip();
                    let workers = self
                        .worker_node_manager
                        .manager
                        .get_workers_by_worker_slot_ids(&worker_ids)?;
                    for (idx, (worker_node, partition)) in
                        (workers.into_iter().zip_eq_fast(vnode_bitmaps.into_iter())).enumerate()
                    {
                        let second_stage_plan_node = self.convert_plan_node(
                            &second_stage.root,
                            &mut None,
                            Some(PartitionInfo::Table(partition)),
                            next_executor_id.clone(),
                        )?;
                        let second_stage_plan_fragment = PlanFragment {
                            root: Some(second_stage_plan_node),
                            exchange_info: Some(ExchangeInfo {
                                mode: DistributionMode::Single as i32,
                                ..Default::default()
                            }),
                        };
                        let local_execute_plan = LocalExecutePlan {
                            plan: Some(second_stage_plan_fragment),
                            epoch: Some(self.batch_query_epoch),
                            tracing_context: tracing_context.clone(),
                        };
                        let exchange_source = ExchangeSource {
                            task_output_id: Some(TaskOutputId {
                                task_id: Some(PbTaskId {
                                    task_id: idx as u64,
                                    stage_id: exchange_source_stage_id,
                                    query_id: self.query.query_id.id.clone(),
                                }),
                                output_id: 0,
                            }),
                            host: Some(worker_node.host.as_ref().unwrap().clone()),
                            local_execute_plan: Some(Plan(local_execute_plan)),
                        };
                        sources.push(exchange_source);
                    }
                } else {
                    let second_stage_plan_node = self.convert_plan_node(
                        &second_stage.root,
                        &mut None,
                        None,
                        next_executor_id,
                    )?;
                    let second_stage_plan_fragment = PlanFragment {
                        root: Some(second_stage_plan_node),
                        exchange_info: Some(ExchangeInfo {
                            mode: DistributionMode::Single as i32,
                            ..Default::default()
                        }),
                    };

                    let local_execute_plan = LocalExecutePlan {
                        plan: Some(second_stage_plan_fragment),
                        epoch: Some(self.batch_query_epoch),
                        tracing_context,
                    };

                    let workers = self.choose_worker(&second_stage)?;
                    *sources = workers
                        .iter()
                        .enumerate()
                        .map(|(idx, worker_node)| {
                            let exchange_source = ExchangeSource {
                                task_output_id: Some(TaskOutputId {
                                    task_id: Some(PbTaskId {
                                        task_id: idx as u64,
                                        stage_id: exchange_source_stage_id,
                                        query_id: self.query.query_id.id.clone(),
                                    }),
                                    output_id: 0,
                                }),
                                host: Some(worker_node.host.as_ref().unwrap().clone()),
                                local_execute_plan: Some(Plan(local_execute_plan.clone())),
                            };
                            exchange_source
                        })
                        .collect();
                }

                Ok(PbPlanNode {
                    // Since all the rest plan is embedded into the exchange node,
                    // there is no children any more.
                    children: vec![],
                    identity,
                    node_body: Some(node_body),
                })
            }
            _ => {
                let children = execution_plan_node
                    .children
                    .iter()
                    .map(|e| {
                        self.convert_plan_node(
                            e,
                            second_stages,
                            partition.clone(),
                            next_executor_id.clone(),
                        )
                    })
                    .collect::<SchedulerResult<Vec<PbPlanNode>>>()?;

                Ok(PbPlanNode {
                    children,
                    identity,
                    node_body: Some(execution_plan_node.node.clone()),
                })
            }
        }
    }

    #[inline(always)]
    fn get_table_dml_vnode_mapping(
        &self,
        table_id: &TableId,
    ) -> SchedulerResult<WorkerSlotMapping> {
        let guard = self.front_env.catalog_reader().read_guard();

        let table = guard
            .get_any_table_by_id(table_id)
            .map_err(|e| SchedulerError::Internal(anyhow!(e)))?;

        let fragment_id = match table.dml_fragment_id.as_ref() {
            Some(dml_fragment_id) => dml_fragment_id,
            // Backward compatibility for those table without `dml_fragment_id`.
            None => &table.fragment_id,
        };

        self.worker_node_manager
            .manager
            .get_streaming_fragment_mapping(fragment_id)
            .map_err(|e| e.into())
    }

    fn choose_worker(&self, stage: &Arc<QueryStage>) -> SchedulerResult<Vec<WorkerNode>> {
        if let Some(table_id) = stage.dml_table_id.as_ref() {
            // dml should use streaming vnode mapping
            let vnode_mapping = self.get_table_dml_vnode_mapping(table_id)?;
            let worker_node = {
                let worker_ids = vnode_mapping.iter_unique().collect_vec();
                let candidates = self
                    .worker_node_manager
                    .manager
                    .get_workers_by_worker_slot_ids(&worker_ids)?;
                if candidates.is_empty() {
                    return Err(BatchError::EmptyWorkerNodes.into());
                }
                candidates[stage.session_id.0 as usize % candidates.len()].clone()
            };
            Ok(vec![worker_node])
        } else {
            let mut workers = Vec::with_capacity(stage.parallelism.unwrap() as usize);
            for _ in 0..stage.parallelism.unwrap() {
                workers.push(self.worker_node_manager.next_random_worker()?);
            }
            Ok(workers)
        }
    }
}

pub async fn run_inner_call(
    plan_node: &PlanFragment,
    epoch: BatchQueryEpoch,
    context: FrontendBatchTaskContext,
) -> Result<(), BoxedError> {
    let mut stream = Box::pin(run_inner(plan_node.clone(), epoch, context))
        .map(|r| r.map_err(|e| Box::new(e) as BoxedError));
    while let Some(r) = stream.next().await {
        let _ = r?;
    }
    Ok(())
}

#[try_stream(ok = DataChunk, error = RwError)]
async fn run_inner(plan: PlanFragment, epoch: BatchQueryEpoch, context: FrontendBatchTaskContext) {
    let task_id = TaskId {
        query_id: String::from("WXKLOG"),
        stage_id: 0,
        task_id: 0,
    };
    let plan_node = plan.root.unwrap();
    let executor = ExecutorBuilder::new(
        &plan_node,
        &task_id,
        context,
        epoch,
        // self.shutdown_rx().clone(),
        ShutdownToken::empty(),
    );

    let executor = executor.build().await?;

    println!("WKXLOG executor: {:?}", executor);

    #[for_await]
    for chunk in executor.execute() {
        yield chunk?;
    }
    // while let Some(result) = executor.execute().next().await {
    //     result?;
    // }
    // Ok(())
}

pub async fn run_inner_call_2(
    plan_node: &PlanFragment,
    epoch: BatchQueryEpoch,
    context: FrontendBatchTaskContext,
    session: Arc<SessionImpl>,
) -> Result<(), BoxedError> {
    // let compute_runtime = self.front_env.compute_runtime();

    let catalog_reader = session.env().catalog_reader().clone();
    let user_info_reader = session.env().user_info_reader().clone();
    let auth_context = session.auth_context().clone();
    let db_name = session.database().to_string();
    let search_path = session.config().search_path();
    let time_zone = session.config().timezone();
    let strict_mode = session.config().batch_expr_strict_mode();
    let meta_client = session.env().meta_client_ref();

    let exec = async move { run_inner_call(plan_node, epoch, context).await };

    use risingwave_expr::expr_context::*;

    use crate::expr::function_impl::context::{
        AUTH_CONTEXT, CATALOG_READER, DB_NAME, META_CLIENT, SEARCH_PATH, USER_INFO_READER,
    };

    // box is necessary, otherwise the size of `exec` will double each time it is nested.
    let exec = async move { CATALOG_READER::scope(catalog_reader, exec).await }.boxed();
    let exec = async move { USER_INFO_READER::scope(user_info_reader, exec).await }.boxed();
    let exec = async move { DB_NAME::scope(db_name, exec).await }.boxed();
    let exec = async move { SEARCH_PATH::scope(search_path, exec).await }.boxed();
    let exec = async move { AUTH_CONTEXT::scope(auth_context, exec).await }.boxed();
    let exec = async move { TIME_ZONE::scope(time_zone, exec).await }.boxed();
    let exec = async move { STRICT_MODE::scope(strict_mode, exec).await }.boxed();
    let exec = async move { META_CLIENT::scope(meta_client, exec).await }.boxed();
    exec.await
}
