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

use std::cmp::min;
use std::collections::VecDeque;
use std::future::{pending, ready, Future};
use std::mem::take;
use std::sync::Arc;

use anyhow::anyhow;
use futures::future::{try_join_all, Either};
use futures::{pin_mut, Stream, TryFutureExt, TryStreamExt};
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::metrics::LabelGuardedIntCounter;
use risingwave_common::row::OwnedRow;
use risingwave_common::util::epoch::EpochPair;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::ChangeLogRow;
use risingwave_storage::StateStore;
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::backfill::snapshot_backfill::state::{
    BackfillState, EpochBackfillProgress, VnodeBackfillProgress,
};
use crate::executor::backfill::snapshot_backfill::vnode_stream::VnodeStream;
use crate::executor::backfill::utils::{create_builder, mapping_message};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::prelude::{try_stream, StateTable, StreamExt};
use crate::executor::{
    expect_first_barrier, ActorContextRef, Barrier, BoxedMessageStream, DispatcherBarrier,
    DispatcherMessage, Execute, MergeExecutorInput, Message, StreamExecutorError,
    StreamExecutorResult,
};
use crate::task::CreateMviewProgressReporter;

pub struct SnapshotBackfillExecutor<S: StateStore> {
    /// Upstream table
    upstream_table: StorageTable<S>,
    pk_in_output_indices: Vec<usize>,

    /// Backfill progress table
    progress_state_table: StateTable<S>,

    /// Upstream with the same schema with the upstream table.
    upstream: MergeExecutorInput,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    output_indices: Vec<usize>,

    progress: CreateMviewProgressReporter,

    chunk_size: usize,
    rate_limit: Option<usize>,

    barrier_rx: UnboundedReceiver<Barrier>,

    actor_ctx: ActorContextRef,
    metrics: Arc<StreamingMetrics>,

    snapshot_epoch: Option<u64>,
}

impl<S: StateStore> SnapshotBackfillExecutor<S> {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        upstream_table: StorageTable<S>,
        progress_state_table: StateTable<S>,
        upstream: MergeExecutorInput,
        output_indices: Vec<usize>,
        actor_ctx: ActorContextRef,
        progress: CreateMviewProgressReporter,
        chunk_size: usize,
        rate_limit: Option<usize>,
        barrier_rx: UnboundedReceiver<Barrier>,
        metrics: Arc<StreamingMetrics>,
        snapshot_epoch: Option<u64>,
    ) -> StreamExecutorResult<Self> {
        assert_eq!(&upstream.info.schema, upstream_table.schema());
        let Some(pk_in_output_indices) = upstream_table.pk_in_output_indices() else {
            panic!(
                "storage table should include all pk columns in output: pk_indices: {:?}, output_indices: {:?}, schema: {:?}",
                upstream_table.pk_indices(),
                upstream_table.output_indices(),
                upstream_table.schema()
            )
        };
        if let Some(rate_limit) = rate_limit {
            debug!(
                rate_limit,
                "create snapshot backfill executor with rate limit"
            );
        }
        VnodeBackfillProgress::validate_progress_table_schema(
            progress_state_table.get_data_types(),
            upstream_table.pk_serializer().get_data_types(),
        )?;
        Ok(Self {
            upstream_table,
            pk_in_output_indices,
            progress_state_table,
            upstream,
            output_indices,
            progress,
            chunk_size,
            rate_limit,
            barrier_rx,
            actor_ctx,
            metrics,
            snapshot_epoch,
        })
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let table_id = self.upstream_table.table_id();
        debug!("snapshot backfill executor start");
        let first_upstream_barrier = expect_first_barrier(&mut self.upstream).await?;
        debug!(epoch = ?first_upstream_barrier.epoch, "get first upstream barrier");
        let first_recv_barrier = receive_next_barrier(&mut self.barrier_rx).await?;
        debug!(epoch = ?first_recv_barrier.epoch, "get first inject barrier");
        let should_backfill = if let Some(snapshot_epoch) = self.snapshot_epoch {
            if first_upstream_barrier.epoch != first_recv_barrier.epoch {
                assert_eq!(snapshot_epoch, first_upstream_barrier.epoch.prev);
                true
            } else {
                false
            }
        } else {
            // when snapshot epoch is not set, the StreamNode must be created previously and has finished the backfill
            if cfg!(debug_assertions) {
                panic!(
                    "snapshot epoch not set. first_upstream_epoch: {:?}, first_recv_epoch: {:?}",
                    first_upstream_barrier.epoch, first_recv_barrier.epoch
                );
            } else {
                warn!(first_upstream_epoch = ?first_upstream_barrier.epoch, first_recv_epoch=?first_recv_barrier.epoch, "snapshot epoch not set");
                assert_eq!(first_upstream_barrier.epoch, first_recv_barrier.epoch);
                false
            }
        };
        let first_recv_barrier_epoch = first_recv_barrier.epoch;
        yield Message::Barrier(first_recv_barrier);
        self.progress_state_table
            .init_epoch(first_recv_barrier_epoch)
            .await?;

        let (mut barrier_epoch, mut need_report_finish, mut backfill_state) = {
            if should_backfill {
                let mut backfill_state =
                    BackfillState::new([], self.upstream_table.pk_serializer().clone());
                assert!(backfill_state.latest_progress().next().is_none());
                let table_id_str = format!("{}", self.upstream_table.table_id().table_id);
                let actor_id_str = format!("{}", self.actor_ctx.id);

                let consume_upstream_row_count = self
                    .metrics
                    .snapshot_backfill_consume_row_count
                    .with_guarded_label_values(&[&table_id_str, &actor_id_str, "consume_upstream"]);

                let mut upstream_buffer =
                    UpstreamBuffer::new(&mut self.upstream, consume_upstream_row_count);

                let first_barrier_epoch = first_upstream_barrier.epoch;

                // Phase 1: consume upstream snapshot
                {
                    {
                        let consuming_snapshot_row_count = self
                            .metrics
                            .snapshot_backfill_consume_row_count
                            .with_guarded_label_values(&[
                                &table_id_str,
                                &actor_id_str,
                                "consuming_snapshot",
                            ]);
                        let snapshot_stream = make_consume_snapshot_stream(
                            &self.upstream_table,
                            &self.pk_in_output_indices,
                            first_barrier_epoch.prev,
                            self.chunk_size,
                            self.rate_limit,
                            &mut self.barrier_rx,
                            &mut self.progress,
                            &mut backfill_state,
                            &mut self.progress_state_table,
                            first_recv_barrier_epoch,
                        );

                        pin_mut!(snapshot_stream);

                        while let Some(message) = upstream_buffer
                            .run_future(snapshot_stream.try_next())
                            .await?
                        {
                            if let Message::Chunk(chunk) = &message {
                                consuming_snapshot_row_count.inc_by(chunk.cardinality() as _);
                            }
                            yield message;
                        }
                    }

                    let recv_barrier = self.barrier_rx.recv().await.expect("should exist");
                    assert_eq!(first_upstream_barrier.epoch, recv_barrier.epoch);
                    self.progress_state_table.commit(recv_barrier.epoch).await?;
                    yield Message::Barrier(recv_barrier);
                }

                let mut upstream_buffer = upstream_buffer.start_consuming_log_store();

                let mut barrier_epoch = first_barrier_epoch;

                let initial_pending_barrier = upstream_buffer.barrier_count();
                info!(
                    ?barrier_epoch,
                    table_id = self.upstream_table.table_id().table_id,
                    initial_pending_barrier,
                    "start consuming log store"
                );

                let consuming_log_store_row_count = self
                    .metrics
                    .snapshot_backfill_consume_row_count
                    .with_guarded_label_values(&[
                        &table_id_str,
                        &actor_id_str,
                        "consuming_log_store",
                    ]);

                // Phase 2: consume upstream log store
                while let Some(upstream_barriers) =
                    upstream_buffer.next_checkpoint_barrier().await?
                {
                    for upstream_barrier in upstream_barriers {
                        let barrier = receive_next_barrier(&mut self.barrier_rx).await?;
                        assert_eq!(upstream_barrier.epoch, barrier.epoch);
                        assert_eq!(barrier_epoch.curr, barrier.epoch.prev);
                        barrier_epoch = barrier.epoch;
                        debug!(?barrier_epoch, kind = ?barrier.kind, "before consume change log");
                        // use `upstream_buffer.run_future` to poll upstream concurrently so that we won't have back-pressure
                        // on the upstream. Otherwise, in `batch_iter_log_with_pk_bounds`, we may wait upstream epoch to be committed,
                        // and the back-pressure may cause the upstream unable to consume the barrier and then cause deadlock.
                        let stream = upstream_buffer
                            .run_future(make_log_stream(
                                &self.upstream_table,
                                barrier_epoch.prev,
                                None,
                                self.chunk_size,
                            ))
                            .await?;
                        pin_mut!(stream);
                        while let Some(chunk) =
                            upstream_buffer.run_future(stream.try_next()).await?
                        {
                            debug!(
                                ?barrier_epoch,
                                size = chunk.cardinality(),
                                "consume change log yield chunk",
                            );
                            consuming_log_store_row_count.inc_by(chunk.cardinality() as _);
                            yield Message::Chunk(chunk);
                        }

                        debug!(?barrier_epoch, "after consume change log");

                        self.progress.update_create_mview_log_store_progress(
                            barrier.epoch,
                            upstream_buffer.barrier_count(),
                        );

                        backfill_state.finish_epoch(
                            self.upstream_table.vnodes().iter_vnodes(),
                            barrier.epoch.prev,
                        );
                        for (_vnode, old_row, new_row) in backfill_state.uncommitted_state() {
                            let record = if let Some(old_row) = old_row {
                                Record::Update { old_row, new_row }
                            } else {
                                Record::Insert { new_row }
                            };
                            self.progress_state_table.write_record(record);
                        }
                        self.progress_state_table.commit(barrier.epoch).await?;
                        backfill_state.mark_committed();
                        let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.actor_ctx.id);
                        yield Message::Barrier(barrier);
                        if update_vnode_bitmap.is_some() {
                            return Err(anyhow!(
                                "should not update vnode bitmap during consuming log store"
                            )
                            .into());
                        }
                    }
                }

                info!(
                    ?barrier_epoch,
                    table_id = self.upstream_table.table_id().table_id,
                    "finish consuming log store"
                );
                (barrier_epoch, true, backfill_state)
            } else {
                let pk_serializer = self.upstream_table.pk_serializer();
                let progress = try_join_all(self.progress_state_table.vnodes().iter_vnodes().map(
                    |vnode| {
                        self.progress_state_table
                            .get_row([vnode.to_datum()])
                            .map_ok(move |progress_row| {
                                let progress = VnodeBackfillProgress::from_row(
                                    &progress_row.expect(
                                        "should exist because all vnode have same epoch progress",
                                    ),
                                    pk_serializer,
                                );
                                let expected_progress = VnodeBackfillProgress {
                                    epoch: first_upstream_barrier.epoch.prev,
                                    progress: EpochBackfillProgress::Consumed,
                                };
                                assert_eq!(progress, expected_progress, "vnode: {:?}", vnode);
                                (vnode, progress)
                            })
                    },
                ))
                .await?;
                let backfill_state =
                    BackfillState::new(progress, self.upstream_table.pk_serializer().clone());
                info!(
                    table_id = self.upstream_table.table_id().table_id,
                    "skip backfill"
                );
                assert_eq!(first_upstream_barrier.epoch, first_recv_barrier_epoch);
                (first_upstream_barrier.epoch, false, backfill_state)
            }
        };
        let mut upstream = self.upstream.into_executor(self.barrier_rx).execute();
        // Phase 3: consume upstream
        while let Some(msg) = upstream.try_next().await? {
            match msg {
                Message::Barrier(barrier) => {
                    assert_eq!(barrier.epoch.prev, barrier_epoch.curr);
                    backfill_state.finish_epoch(
                        self.upstream_table.vnodes().iter_vnodes(),
                        barrier.epoch.prev,
                    );
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.actor_ctx.id);
                    let is_scale =
                        update_vnode_bitmap.is_some() || barrier.is_stop(self.actor_ctx.id);
                    if is_scale {
                        use itertools::Itertools;
                        info!(?table_id, vnodes = ?self.upstream_table.vnodes().iter_vnodes().collect_vec(), prev_epoch = barrier.epoch.prev, "write finish epoch");
                    }
                    barrier_epoch = barrier.epoch;
                    if need_report_finish {
                        need_report_finish = false;
                        self.progress.finish_consuming_log_store(barrier_epoch);
                    }
                    for (vnode, old_row, new_row) in backfill_state.uncommitted_state() {
                        let record = if let Some(old_row) = old_row {
                            Record::Update { old_row, new_row }
                        } else {
                            Record::Insert { new_row }
                        };
                        if is_scale {
                            info!(
                                ?table_id,
                                ?vnode,
                                ?record,
                                "write record before update vnode bitmap"
                            );
                        }
                        self.progress_state_table.write_record(record);
                    }
                    self.progress_state_table.commit(barrier.epoch).await?;
                    backfill_state.mark_committed();
                    if let Some(new_vnode_bitmap) = update_vnode_bitmap {
                        yield Message::Barrier(barrier);
                        self.progress_state_table
                            .try_wait_committed_epoch(barrier_epoch.prev)
                            .await?;
                        let _prev_vnode_bitmap = self
                            .upstream_table
                            .update_vnode_bitmap(new_vnode_bitmap.clone());
                        let (_, _) = self
                            .progress_state_table
                            .update_vnode_bitmap(new_vnode_bitmap);
                        let pk_serializer = self.upstream_table.pk_serializer();
                        // try_join_all((0..256).map(
                        //     |vnode| {
                        //         let vnode = VirtualNode::from_index(vnode);
                        //         // TODO: remove the clone
                        //         let pk_serializer = pk_serializer.clone();
                        //         self.progress_state_table
                        //             .get_row([vnode.to_datum()])
                        //             .map_ok(move |progress_row| {
                        //                 let progress = VnodeBackfillProgress::from_row(
                        //                     &progress_row.expect("should exist because all vnode have same epoch progress"),
                        //                     &pk_serializer,
                        //                 );
                        //                 let expected_progress = VnodeBackfillProgress {
                        //                     epoch: barrier_epoch.prev,
                        //                     progress: EpochBackfillProgress::Consumed,
                        //                 };
                        //                 assert_eq!(progress, expected_progress, "{:?}", vnode);
                        //             })
                        //     },
                        // ))
                        //     .await?;
                        let progress =
                            try_join_all(self.progress_state_table.vnodes().iter_vnodes().map(
                                |vnode| {
                                    // TODO: remove the clone
                                    let pk_serializer = pk_serializer.clone();
                                    self.progress_state_table
                                        .get_row([vnode.to_datum()])
                                        .map_ok(move |progress_row| {
                                            let progress = VnodeBackfillProgress::from_row(
                                                &progress_row.expect("should exist because all vnode have same epoch progress"),
                                                &pk_serializer,
                                            );
                                            let expected_progress = VnodeBackfillProgress {
                                                epoch: barrier_epoch.prev,
                                                progress: EpochBackfillProgress::Consumed,
                                            };
                                            assert_eq!(progress, expected_progress, "{:?}", vnode);
                                            (vnode, progress)
                                        })
                                },
                            ))
                            .await?;
                        backfill_state.update_vnode_bitmap(progress);
                    } else {
                        yield Message::Barrier(barrier);
                    }
                }
                msg => {
                    yield msg;
                }
            }
        }
    }
}

impl<S: StateStore> Execute for SnapshotBackfillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        let output_indices = self.output_indices.clone();
        self.execute_inner()
            .filter_map(move |result| {
                ready({
                    match result {
                        Ok(message) => mapping_message(message, &output_indices).map(Ok),
                        Err(e) => Some(Err(e)),
                    }
                })
            })
            .boxed()
    }
}

struct ConsumingSnapshot;
struct ConsumingLogStore;

struct UpstreamBuffer<'a, S> {
    upstream: &'a mut MergeExecutorInput,
    max_pending_checkpoint_barrier_num: usize,
    pending_non_checkpoint_barriers: Vec<DispatcherBarrier>,
    /// Barriers received from upstream but not yet received the barrier from local barrier worker.
    ///
    /// In the outer `VecDeque`, newer barriers at the front.
    /// In the inner `Vec`, newer barrier at the back, with the last barrier as checkpoint barrier,
    /// and others as non-checkpoint barrier
    upstream_pending_barriers: VecDeque<Vec<DispatcherBarrier>>,
    /// Whether we have started polling any upstream data before the next barrier.
    /// When `true`, we should continue polling until the next barrier, because
    /// some data in this epoch have been discarded and data in this epoch
    /// must be read from log store
    is_polling_epoch_data: bool,
    consume_upstream_row_count: LabelGuardedIntCounter<3>,
    _phase: S,
}

impl<'a> UpstreamBuffer<'a, ConsumingSnapshot> {
    fn new(
        upstream: &'a mut MergeExecutorInput,
        consume_upstream_row_count: LabelGuardedIntCounter<3>,
    ) -> Self {
        Self {
            upstream,
            is_polling_epoch_data: false,
            consume_upstream_row_count,
            pending_non_checkpoint_barriers: vec![],
            upstream_pending_barriers: Default::default(),
            // no limit on the number of pending barrier in the beginning
            max_pending_checkpoint_barrier_num: usize::MAX,
            _phase: ConsumingSnapshot {},
        }
    }

    fn start_consuming_log_store(self) -> UpstreamBuffer<'a, ConsumingLogStore> {
        let max_pending_barrier_num = self.barrier_count();
        UpstreamBuffer {
            upstream: self.upstream,
            pending_non_checkpoint_barriers: self.pending_non_checkpoint_barriers,
            upstream_pending_barriers: self.upstream_pending_barriers,
            max_pending_checkpoint_barrier_num: max_pending_barrier_num,
            is_polling_epoch_data: self.is_polling_epoch_data,
            consume_upstream_row_count: self.consume_upstream_row_count,
            _phase: ConsumingLogStore {},
        }
    }
}

impl<'a, S> UpstreamBuffer<'a, S> {
    async fn concurrently_consume_upstream(&mut self) -> StreamExecutorError {
        {
            loop {
                if let Err(e) = try {
                    if self.upstream_pending_barriers.len()
                        >= self.max_pending_checkpoint_barrier_num
                    {
                        // pause the future to block consuming upstream
                        return pending().await;
                    }
                    let barrier = self.consume_until_next_checkpoint_barrier().await?;
                    self.upstream_pending_barriers.push_front(barrier);
                } {
                    break e;
                }
            }
        }
    }

    /// Consume the upstream until seeing the next barrier.
    /// `pending_barriers` must be non-empty after this method returns.
    async fn consume_until_next_checkpoint_barrier(
        &mut self,
    ) -> StreamExecutorResult<Vec<DispatcherBarrier>> {
        loop {
            let msg: DispatcherMessage = self
                .upstream
                .try_next()
                .await?
                .ok_or_else(|| anyhow!("end of upstream"))?;
            match msg {
                DispatcherMessage::Chunk(chunk) => {
                    self.is_polling_epoch_data = true;
                    self.consume_upstream_row_count
                        .inc_by(chunk.cardinality() as _);
                }
                DispatcherMessage::Barrier(barrier) => {
                    let is_checkpoint = barrier.kind.is_checkpoint();
                    self.pending_non_checkpoint_barriers.push(barrier);
                    if is_checkpoint {
                        self.is_polling_epoch_data = false;
                        break Ok(take(&mut self.pending_non_checkpoint_barriers));
                    } else {
                        self.is_polling_epoch_data = true;
                    }
                }
                DispatcherMessage::Watermark(_) => {
                    self.is_polling_epoch_data = true;
                }
            }
        }
    }
}

impl<'a> UpstreamBuffer<'a, ConsumingLogStore> {
    async fn next_checkpoint_barrier(
        &mut self,
    ) -> StreamExecutorResult<Option<Vec<DispatcherBarrier>>> {
        Ok(
            if let Some(barriers) = self.upstream_pending_barriers.pop_back() {
                // sub(1) to ensure that the lag is monotonically decreasing.
                self.max_pending_checkpoint_barrier_num = min(
                    self.upstream_pending_barriers.len(),
                    self.max_pending_checkpoint_barrier_num.saturating_sub(1),
                );
                Some(barriers)
            } else {
                self.max_pending_checkpoint_barrier_num = 0;
                if self.is_polling_epoch_data {
                    let barriers = self.consume_until_next_checkpoint_barrier().await?;
                    Some(barriers)
                } else {
                    None
                }
            },
        )
    }
}

impl<'a, S> UpstreamBuffer<'a, S> {
    /// Run a future while concurrently polling the upstream so that the upstream
    /// won't be back-pressured.
    async fn run_future<T, E: Into<StreamExecutorError>>(
        &mut self,
        future: impl Future<Output = Result<T, E>>,
    ) -> StreamExecutorResult<T> {
        select! {
            biased;
            e = self.concurrently_consume_upstream() => {
                Err(e)
            }
            // this arm won't be starved, because the first arm is always pending unless returning with error
            result = future => {
                result.map_err(Into::into)
            }
        }
    }

    fn barrier_count(&self) -> usize {
        self.upstream_pending_barriers.len()
    }
}

async fn receive_next_barrier(
    barrier_rx: &mut UnboundedReceiver<Barrier>,
) -> StreamExecutorResult<Barrier> {
    Ok(barrier_rx
        .recv()
        .await
        .ok_or_else(|| anyhow!("end of barrier receiver"))?)
}

async fn make_log_stream(
    upstream_table: &StorageTable<impl StateStore>,
    prev_epoch: u64,
    start_pk: Option<OwnedRow>,
    chunk_size: usize,
) -> StreamExecutorResult<VnodeStream<impl super::vnode_stream::BackfillRowStream>> {
    let data_types = upstream_table.schema().data_types();
    let start_pk = start_pk.as_ref();
    let vnode_streams = try_join_all(upstream_table.vnodes().iter_vnodes().map(move |vnode| {
        upstream_table
            .batch_iter_vnode_log(
                prev_epoch,
                HummockReadEpoch::Committed(prev_epoch),
                start_pk,
                vnode,
            )
            .map_ok(move |stream| {
                let stream = stream.map(|result| {
                    Ok(match result? {
                        ChangeLogRow::Insert(row) => ((Op::Insert, row), None),
                        ChangeLogRow::Update {
                            new_value,
                            old_value,
                        } => (
                            (Op::UpdateDelete, old_value),
                            Some((Op::UpdateInsert, new_value)),
                        ),
                        ChangeLogRow::Delete(row) => ((Op::Delete, row), None),
                    })
                });
                (vnode, stream)
            })
    }))
    .await?;
    let builder = create_builder(None, chunk_size, data_types.clone());
    Ok(VnodeStream::new(vnode_streams, builder))
}

async fn make_snapshot_stream(
    upstream_table: &StorageTable<impl StateStore>,
    snapshot_epoch: u64,
    start_pk: Option<OwnedRow>,
    rate_limit: Option<usize>,
    chunk_size: usize,
) -> StreamExecutorResult<VnodeStream<impl super::vnode_stream::BackfillRowStream>> {
    let data_types = upstream_table.schema().data_types();
    let start_pk = start_pk.as_ref();
    let vnode_streams = try_join_all(upstream_table.vnodes().iter_vnodes().map(move |vnode| {
        upstream_table
            .batch_iter_vnode(
                HummockReadEpoch::Committed(snapshot_epoch),
                start_pk,
                vnode,
                PrefetchOptions::prefetch_for_large_range_scan(),
            )
            .map_ok(move |stream| {
                let stream = stream.map(|result| Ok(((Op::Insert, result?), None)));
                (vnode, stream)
            })
    }))
    .await?;
    let builder = create_builder(rate_limit, chunk_size, data_types.clone());
    Ok(VnodeStream::new(vnode_streams, builder))
}

#[expect(clippy::too_many_arguments)]
#[try_stream(ok = Message, error = StreamExecutorError)]
async fn make_consume_snapshot_stream<'a, S: StateStore>(
    upstream_table: &'a StorageTable<S>,
    pk_in_output_indices: &'a [usize],
    snapshot_epoch: u64,
    chunk_size: usize,
    rate_limit: Option<usize>,
    barrier_rx: &'a mut UnboundedReceiver<Barrier>,
    progress: &'a mut CreateMviewProgressReporter,
    backfill_state: &'a mut BackfillState,
    progress_state_table: &'a mut StateTable<S>,
    first_recv_barrier_epoch: EpochPair,
) {
    let mut barrier_epoch = first_recv_barrier_epoch;

    info!(
        table_id = upstream_table.table_id().table_id,
        ?barrier_epoch,
        "start consuming snapshot"
    );

    // start consume upstream snapshot
    let mut snapshot_stream =
        make_snapshot_stream(upstream_table, snapshot_epoch, None, rate_limit, chunk_size).await?;

    async fn select_barrier_and_snapshot_stream(
        barrier_rx: &mut UnboundedReceiver<Barrier>,
        snapshot_stream: &mut (impl Stream<Item = StreamExecutorResult<StreamChunk>> + Unpin),
        throttle_snapshot_stream: bool,
    ) -> StreamExecutorResult<Either<Barrier, Option<StreamChunk>>> {
        select!(
            result = receive_next_barrier(barrier_rx) => {
                Ok(Either::Left(result?))
            },
            result = snapshot_stream.try_next(), if !throttle_snapshot_stream => {
                Ok(Either::Right(result?))
            }
        )
    }

    let mut count = 0;
    let mut epoch_row_count = 0;
    loop {
        let throttle_snapshot_stream = if let Some(rate_limit) = rate_limit {
            epoch_row_count > rate_limit
        } else {
            false
        };
        match select_barrier_and_snapshot_stream(
            barrier_rx,
            &mut snapshot_stream,
            throttle_snapshot_stream,
        )
        .await?
        {
            Either::Left(barrier) => {
                assert_eq!(barrier.epoch.prev, barrier_epoch.curr);
                barrier_epoch = barrier.epoch;
                if barrier_epoch.curr >= snapshot_epoch {
                    return Err(anyhow!("should not receive barrier with epoch {barrier_epoch:?} later than snapshot epoch {snapshot_epoch}").into());
                }
                if let Some(chunk) = snapshot_stream.consume_builder() {
                    count += chunk.cardinality();
                    epoch_row_count += chunk.cardinality();
                    yield Message::Chunk(chunk);
                }
                snapshot_stream
                    .for_vnode_pk_progress(pk_in_output_indices, |vnode, pk_progress| {
                        if let Some(pk) = pk_progress {
                            backfill_state.update_epoch_progress(vnode, snapshot_epoch, pk);
                        } else {
                            backfill_state.finish_epoch([vnode], snapshot_epoch);
                        }
                    })
                    .await?;
                for (_vnode, old_row, new_row) in backfill_state.uncommitted_state() {
                    let record = if let Some(old_row) = old_row {
                        Record::Update { old_row, new_row }
                    } else {
                        Record::Insert { new_row }
                    };
                    progress_state_table.write_record(record);
                }
                progress_state_table.commit(barrier.epoch).await?;
                backfill_state.mark_committed();
                debug!(?barrier_epoch, count, epoch_row_count, "update progress");
                progress.update(barrier_epoch, barrier_epoch.prev, count as _);
                epoch_row_count = 0;

                yield Message::Barrier(barrier);
            }
            Either::Right(Some(chunk)) => {
                count += chunk.cardinality();
                epoch_row_count += chunk.cardinality();
                yield Message::Chunk(chunk);
            }
            Either::Right(None) => {
                break;
            }
        }
    }

    // finish consuming upstream snapshot, report finish
    let barrier_to_report_finish = receive_next_barrier(barrier_rx).await?;
    assert_eq!(barrier_to_report_finish.epoch.prev, barrier_epoch.curr);
    barrier_epoch = barrier_to_report_finish.epoch;
    info!(?barrier_epoch, count, "report finish");
    snapshot_stream
        .for_vnode_pk_progress(pk_in_output_indices, |vnode, pk_progress| {
            assert_eq!(pk_progress, None);
            backfill_state.finish_epoch([vnode], snapshot_epoch);
        })
        .await?;
    for (_vnode, old_row, new_row) in backfill_state.uncommitted_state() {
        let record = if let Some(old_row) = old_row {
            Record::Update { old_row, new_row }
        } else {
            Record::Insert { new_row }
        };
        progress_state_table.write_record(record);
    }
    progress_state_table.commit(barrier_epoch).await?;
    backfill_state.mark_committed();
    progress.finish(barrier_epoch, count as _);
    yield Message::Barrier(barrier_to_report_finish);

    // keep receiving remaining barriers until receiving a barrier with epoch as snapshot_epoch
    loop {
        let barrier = receive_next_barrier(barrier_rx).await?;
        assert_eq!(barrier.epoch.prev, barrier_epoch.curr);
        barrier_epoch = barrier.epoch;
        progress_state_table.commit(barrier.epoch).await?;
        yield Message::Barrier(barrier);
        if barrier_epoch.curr == snapshot_epoch {
            break;
        }
    }
    info!(?barrier_epoch, "finish consuming snapshot");
}
