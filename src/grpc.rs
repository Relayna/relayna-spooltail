//! gRPC service over the same store and subscription machinery as the HTTP API.
use crate::{
    Broker, CancellationTopics, Problem, WorkLease, feed, publish, request_cancellation, store,
};
use anyhow::{Context, Result};
use prost::Message;
use serde_json::Value;
use std::{path::Path, pin::Pin};
use tokio::net::TcpListener;
use tokio_stream::{Stream, StreamExt};
use tonic::{
    Request, Response, Status,
    metadata::MetadataValue,
    transport::{
        Identity, Server, ServerTlsConfig,
        server::{Router, TcpIncoming},
    },
};

mod replay;

mod wire {
    tonic::include_proto!("spooltail.v1");
}
use crate::stream_members::{StreamAccess, StreamMembership, StreamOwnership};
use crate::subscriptions::{
    DEFAULT_STREAM_BATCH_RECORDS, MAX_STREAM_BATCH_BYTES, validate_batch_count,
};
use wire::spooltail_server::{Spooltail, SpooltailServer};
use wire::{
    AuditHistoryPage, AuditHistoryRequest, CancellationRequest, ClaimRequest, ClaimResponse,
    CommitRequest, CompleteRequest, Delivery, Empty, MoveTaskRequest, NextResponse, Offset, Page,
    Partition, Partitions, Publish, Record, SettleRequest, SettlementAction, StatsResponse,
    StreamBatch, StreamBatchCommit, StreamBatchRequest, StreamGroup, Task, TaskId,
    TopicReplayRequest,
};

struct Api(Broker);

impl Api {
    async fn replay_records(
        &self,
        q: TopicReplayRequest,
    ) -> Result<(i64, Vec<store::Record>), Status> {
        self.0
            .run(move |s| {
                s.read_after_batch(
                    &q.topic,
                    q.after,
                    q.limit.unwrap_or(DEFAULT_STREAM_BATCH_RECORDS),
                )
            })
            .await
            .map_err(status)
    }
}

fn status(error: anyhow::Error) -> Status {
    match error.downcast_ref::<Problem>() {
        Some(Problem::Capacity | Problem::ServiceQuota) => {
            Status::resource_exhausted(error.to_string())
        }
        Some(Problem::Missing) => Status::not_found(error.to_string()),
        Some(Problem::Invalid(_)) => Status::invalid_argument(error.to_string()),
        Some(Problem::Conflict(_)) => Status::failed_precondition(error.to_string()),
        Some(Problem::Gap { earliest }) => {
            let mut status = Status::failed_precondition(error.to_string());
            if let Some(id) = earliest {
                status.metadata_mut().insert_bin(
                    "spooltail-earliest-event-id-bin",
                    MetadataValue::from_bytes(id.as_bytes()),
                );
            }
            status
        }
        None => {
            eprintln!("broker error: {error:#}");
            Status::internal("broker storage failure")
        }
    }
}

fn encode(record: store::Record) -> Result<Record> {
    Ok(Record {
        offset: record.offset,
        partition: record.partition,
        event_json: serde_json::to_vec(&record.event)?,
    })
}

fn encode_batch(after: i64, records: Vec<store::Record>) -> Result<Response<StreamBatch>, Status> {
    let records = records
        .into_iter()
        .map(encode)
        .collect::<Result<_>>()
        .map_err(status)?;
    let mut page = StreamBatch { after, records };
    while page.encoded_len() > MAX_STREAM_BATCH_BYTES {
        page.records.pop();
    }
    Ok(Response::new(page))
}

fn decode(bytes: &[u8]) -> Result<Value> {
    serde_json::from_slice(bytes)
        .map_err(|error| Problem::Invalid(format!("invalid event JSON: {error}")).into())
}

fn stream_access(partition: Option<u32>, ownership: Option<wire::StreamOwnership>) -> StreamAccess {
    StreamAccess {
        partition,
        ownership: ownership.map(|owner| StreamOwnership {
            member_epoch: owner.member_epoch,
            assignment_epoch: owner.assignment_epoch,
        }),
    }
}

fn encode_membership(member: StreamMembership) -> wire::StreamMembership {
    wire::StreamMembership {
        member_epoch: member.member_epoch,
        lease_until_ms: member.lease_until_ms,
        assignments: member
            .assignments
            .into_iter()
            .map(|assignment| wire::StreamAssignment {
                partition: assignment.partition,
                assignment_epoch: assignment.assignment_epoch,
            })
            .collect(),
    }
}

#[tonic::async_trait]
impl Spooltail for Api {
    async fn ping(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }

    async fn latest(&self, request: Request<Task>) -> Result<Response<Record>, Status> {
        let q = request.into_inner();
        let record = self
            .0
            .run(move |s| s.latest(&q.topic, &q.task_id))
            .await
            .map_err(status)?;
        Ok(Response::new(encode(record).map_err(status)?))
    }

    async fn history(&self, request: Request<Task>) -> Result<Response<Page>, Status> {
        let q = request.into_inner();
        let records = self
            .0
            .history_page(move |s| {
                let position = s.resolve(&q.topic, &q.task_id, q.after.as_deref())?;
                s.page(&q.topic, &q.task_id, position)
            })
            .await
            .map_err(status)?;
        let records = records
            .into_iter()
            .map(encode)
            .collect::<Result<_>>()
            .map_err(status)?;
        Ok(Response::new(Page { records }))
    }

    async fn audit_history(
        &self,
        request: Request<AuditHistoryRequest>,
    ) -> Result<Response<AuditHistoryPage>, Status> {
        let q = request.into_inner();
        let limit = q.limit.unwrap_or(DEFAULT_STREAM_BATCH_RECORDS);
        validate_batch_count(limit).map_err(status)?;
        let (through, records) = self
            .0
            .run(move |s| {
                s.audit_history(&q.topic, &q.task_id, q.after.as_deref(), q.through, limit)
            })
            .await
            .map_err(status)?;
        let records = records
            .into_iter()
            .map(encode)
            .collect::<Result<_>>()
            .map_err(status)?;
        Ok(Response::new(AuditHistoryPage { records, through }))
    }

    async fn append(&self, request: Request<Publish>) -> Result<Response<Offset>, Status> {
        let q = request.into_inner();
        let event = decode(&q.event_json).map_err(status)?;
        let position = publish(self.0.clone(), q.topic, event, None, q.owner_service)
            .await
            .map_err(status)?;
        Ok(Response::new(Offset {
            offset: position.offset,
            partition: position.partition,
        }))
    }

    async fn claim(
        &self,
        request: Request<ClaimRequest>,
    ) -> Result<Response<ClaimResponse>, Status> {
        let q = request.into_inner();
        let claim = self
            .0
            .run(move |s| s.claim(&q.topic, &q.group, q.lease_ms, q.partition))
            .await
            .map_err(status)?;
        let Some(claim) = claim else {
            return Ok(Response::new(ClaimResponse::default()));
        };
        Ok(Response::new(ClaimResponse {
            record: Some(encode(claim.record).map_err(status)?),
            generation: claim.generation,
            lease_until_ms: claim.lease_until_ms,
            partition: claim.partition,
        }))
    }

    async fn get_work_stats(
        &self,
        request: Request<wire::WorkStatsRequest>,
    ) -> Result<Response<wire::WorkStats>, Status> {
        let q = request.into_inner();
        let stats = self
            .0
            .run(move |s| s.work_stats(&q.topic, &q.group, q.partition, q.owner_service.as_deref()))
            .await
            .map_err(status)?;
        Ok(Response::new(wire::WorkStats {
            queue_depth: stats.queue_depth,
            ready: stats.ready,
            leased: stats.leased,
            delayed: stats.delayed,
            dead_lettered: stats.dead_lettered,
            expired_leases: stats.expired_leases,
            oldest_ready_age_ms: stats.oldest_ready_age_ms,
            expired_lease_redeliveries_total: stats.expired_lease_redeliveries_total,
            retries_total: stats.retries_total,
            retries_per_minute: stats.retries_per_minute,
        }))
    }

    async fn complete(
        &self,
        request: Request<CompleteRequest>,
    ) -> Result<Response<Offset>, Status> {
        let q = request.into_inner();
        let terminal = q
            .status
            .ok_or_else(|| Status::invalid_argument("missing terminal event"))?;
        let lease = WorkLease {
            topic: q.topic,
            group: q.group,
            offset: q.offset,
            generation: q.generation,
        };
        let event = decode(&terminal.event_json).map_err(status)?;
        let position = publish(self.0.clone(), terminal.topic, event, Some(lease), None)
            .await
            .map_err(status)?;
        Ok(Response::new(Offset {
            offset: position.offset,
            partition: position.partition,
        }))
    }

    async fn request_cancellation(
        &self,
        request: Request<CancellationRequest>,
    ) -> Result<Response<Offset>, Status> {
        let q = request.into_inner();
        let topics = CancellationTopics {
            status_topic: q.status_topic,
            cancel_topic: q.cancel_topic,
        };
        let position = request_cancellation(self.0.clone(), q.task_topic, q.task_id, topics)
            .await
            .map_err(status)?;
        Ok(Response::new(Offset {
            offset: position.offset,
            partition: position.partition,
        }))
    }

    async fn settle(&self, request: Request<SettleRequest>) -> Result<Response<Empty>, Status> {
        let q = request.into_inner();
        let action = match SettlementAction::try_from(q.action) {
            Ok(SettlementAction::Ack) => "ack",
            Ok(SettlementAction::Renew) => "renew",
            Ok(SettlementAction::Retry) => "retry",
            Ok(SettlementAction::DeadLetter) => "dead-letter",
            _ => return Err(Status::invalid_argument("expected a settlement action")),
        };
        self.0
            .run(move |s| {
                s.settle(
                    &q.topic,
                    &q.group,
                    q.offset,
                    q.generation,
                    action,
                    q.delay_ms,
                )
            })
            .await
            .map_err(status)?;
        Ok(Response::new(Empty {}))
    }

    async fn list_dead_letters(
        &self,
        request: Request<wire::DeadLetterListRequest>,
    ) -> Result<Response<wire::DeadLetterPage>, Status> {
        let q = request.into_inner();
        let entries = self
            .0
            .run(move |s| {
                s.list_dead_letters(
                    &q.topic,
                    &q.group,
                    q.after.unwrap_or_default(),
                    q.limit.unwrap_or(DEFAULT_STREAM_BATCH_RECORDS),
                )
            })
            .await
            .map_err(status)?;
        let entries = entries
            .into_iter()
            .map(|entry| wire::DeadLetterSummary {
                offset: entry.offset,
                task_id: entry.task_id,
                event_id: entry.event_id,
                generation: entry.generation,
                partition: entry.partition,
            })
            .collect();
        Ok(Response::new(wire::DeadLetterPage { entries }))
    }

    async fn get_dead_letter(
        &self,
        request: Request<wire::DeadLetterRequest>,
    ) -> Result<Response<wire::DeadLetter>, Status> {
        let q = request.into_inner();
        let entry = self
            .0
            .run(move |s| s.dead_letter(&q.topic, &q.group, q.offset))
            .await
            .map_err(status)?;
        Ok(Response::new(wire::DeadLetter {
            record: Some(encode(entry.record).map_err(status)?),
            generation: entry.generation,
            partition: entry.partition,
        }))
    }

    async fn redrive_dead_letter(
        &self,
        request: Request<wire::DeadLetterRedriveRequest>,
    ) -> Result<Response<Empty>, Status> {
        let q = request.into_inner();
        self.0
            .run(move |s| s.redrive_dead_letter(&q.topic, &q.group, q.offset, q.generation))
            .await
            .map_err(status)?;
        Ok(Response::new(Empty {}))
    }

    async fn next(&self, request: Request<StreamGroup>) -> Result<Response<NextResponse>, Status> {
        let q = request.into_inner();
        let record = self
            .0
            .run(move |s| s.group_next(&q.topic, &q.group, stream_access(q.partition, q.ownership)))
            .await
            .map_err(status)?;
        let record = record.map(encode).transpose().map_err(status)?;
        Ok(Response::new(NextResponse { record }))
    }

    async fn join_stream_group(
        &self,
        request: Request<wire::StreamJoinRequest>,
    ) -> Result<Response<wire::StreamMembership>, Status> {
        let q = request.into_inner();
        let member = self
            .0
            .run(move |s| s.join_stream_group(&q.topic, &q.group, &q.member_id, q.lease_ms))
            .await
            .map_err(status)?;
        Ok(Response::new(encode_membership(member)))
    }

    async fn heartbeat_stream_group(
        &self,
        request: Request<wire::StreamMemberRequest>,
    ) -> Result<Response<wire::StreamMembership>, Status> {
        let q = request.into_inner();
        let member = self
            .0
            .run(move |s| s.heartbeat_stream_group(&q.topic, &q.group, q.member_epoch))
            .await
            .map_err(status)?;
        Ok(Response::new(encode_membership(member)))
    }

    async fn leave_stream_group(
        &self,
        request: Request<wire::StreamMemberRequest>,
    ) -> Result<Response<Empty>, Status> {
        let q = request.into_inner();
        self.0
            .run(move |s| s.leave_stream_group(&q.topic, &q.group, q.member_epoch))
            .await
            .map_err(status)?;
        Ok(Response::new(Empty {}))
    }

    async fn get_stream_lag(
        &self,
        request: Request<StreamGroup>,
    ) -> Result<Response<wire::StreamLag>, Status> {
        let q = request.into_inner();
        let lag = self
            .0
            .run(move |s| s.stream_lag(&q.topic, &q.group, q.partition))
            .await
            .map_err(status)?;
        Ok(Response::new(wire::StreamLag {
            checkpoint: lag.checkpoint,
            latest_offset: lag.latest_offset,
            lag: lag.lag,
            retention_gap: lag.retention_gap,
        }))
    }

    async fn commit(&self, request: Request<CommitRequest>) -> Result<Response<Empty>, Status> {
        let q = request.into_inner();
        self.0
            .run(move |s| {
                s.commit(
                    &q.topic,
                    &q.group,
                    q.offset,
                    stream_access(q.partition, q.ownership),
                )
            })
            .await
            .map_err(status)?;
        Ok(Response::new(Empty {}))
    }

    async fn next_batch(
        &self,
        request: Request<StreamBatchRequest>,
    ) -> Result<Response<StreamBatch>, Status> {
        let q = request.into_inner();
        let (after, records) = self
            .0
            .run(move |s| {
                s.next_batch(
                    &q.topic,
                    &q.group,
                    stream_access(q.partition, q.ownership),
                    q.limit.unwrap_or(DEFAULT_STREAM_BATCH_RECORDS),
                )
            })
            .await
            .map_err(status)?;
        encode_batch(after, records)
    }

    async fn commit_batch(
        &self,
        request: Request<StreamBatchCommit>,
    ) -> Result<Response<Empty>, Status> {
        let q = request.into_inner();
        self.0
            .run(move |s| {
                s.commit_batch(
                    &q.topic,
                    &q.group,
                    stream_access(q.partition, q.ownership),
                    q.after,
                    q.through,
                    q.count,
                )
            })
            .await
            .map_err(status)?;
        Ok(Response::new(Empty {}))
    }

    async fn read_after_batch(
        &self,
        request: Request<TopicReplayRequest>,
    ) -> Result<Response<StreamBatch>, Status> {
        let (after, records) = self.replay_records(request.into_inner()).await?;
        encode_batch(after, records)
    }

    async fn read_replay_page(
        &self,
        request: Request<TopicReplayRequest>,
    ) -> Result<Response<wire::ReplayPage>, Status> {
        let (after, records) = self.replay_records(request.into_inner()).await?;
        Ok(Response::new(
            replay::events(after, records).map_err(status)?,
        ))
    }

    async fn read_numeric_page(
        &self,
        request: Request<wire::NumericRequest>,
    ) -> Result<Response<wire::NumericPage>, Status> {
        let q = request.into_inner();
        let replay = q
            .replay
            .ok_or_else(|| Status::invalid_argument("missing replay request"))?;
        let (after, records) = self.replay_records(replay).await?;
        Ok(Response::new(
            replay::numbers(after, records, &q.array_field, &q.value_field).map_err(status)?,
        ))
    }

    async fn list_partitions(&self, _: Request<Empty>) -> Result<Response<Partitions>, Status> {
        let ids = self
            .0
            .run(|s| Ok(s.partition_ids()))
            .await
            .map_err(status)?;
        Ok(Response::new(Partitions { ids }))
    }

    async fn ensure_partition(
        &self,
        request: Request<Partition>,
    ) -> Result<Response<Partition>, Status> {
        let partition = request.into_inner();
        let id = partition.id;
        self.0
            .run(move |s| s.ensure_partition(id))
            .await
            .map_err(status)?;
        Ok(Response::new(partition))
    }

    async fn task_partition(
        &self,
        request: Request<TaskId>,
    ) -> Result<Response<Partition>, Status> {
        let task = request.into_inner().task_id;
        let id = self
            .0
            .run(move |s| s.task_partition(&task))
            .await
            .map_err(status)?;
        Ok(Response::new(Partition { id }))
    }

    async fn move_task(
        &self,
        request: Request<MoveTaskRequest>,
    ) -> Result<Response<Partition>, Status> {
        let q = request.into_inner();
        let from = q
            .from_partition
            .ok_or_else(|| Status::invalid_argument("missing from_partition"))?;
        let to = q
            .to_partition
            .ok_or_else(|| Status::invalid_argument("missing to_partition"))?;
        self.0
            .move_task(q.task_id, from, to)
            .await
            .map_err(status)?;
        Ok(Response::new(Partition { id: to }))
    }

    async fn stats(&self, _: Request<Empty>) -> Result<Response<StatsResponse>, Status> {
        let stats = self.0.stats().await.map_err(status)?;
        Ok(Response::new(StatsResponse {
            payload_reads: stats.payload_reads,
            recovered_frames: stats.recovered_frames,
            free_bytes: stats.free_bytes,
            disk_reserve_bytes: stats.disk_reserve_bytes,
            raw_journal_bytes: stats.raw_journal_bytes,
            packed_journal_bytes: stats.packed_journal_bytes,
            archive_pending_records: stats.archive_pending_records,
            retired_segments: stats.retired_segments,
            packing_space_pauses: stats.packing_space_pauses,
        }))
    }

    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<Delivery, Status>> + Send>>;

    async fn subscribe(
        &self,
        request: Request<Task>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let q = request.into_inner();
        let events = feed(self.0.clone(), q.topic, q.task_id, q.after).map(|item| {
            Ok(Delivery {
                kind: item.kind.into(),
                event_id: item.id.clone(),
                data_json: item.data.get().as_bytes().to_vec(),
            })
        });
        Ok(Response::new(Box::pin(events)))
    }
}

fn service(broker: Broker) -> SpooltailServer<Api> {
    SpooltailServer::new(Api(broker))
        .max_decoding_message_size(2 * 1024 * 1024)
        // The store allows up to 64 frames of 1 MiB per history page.
        .max_encoding_message_size(70 * 1024 * 1024)
}

fn tls_config(cert: &Path, key: &Path) -> Result<ServerTlsConfig> {
    let cert = std::fs::read(cert).context("read gRPC TLS certificate")?;
    let key = std::fs::read(key).context("read gRPC TLS private key")?;
    Ok(ServerTlsConfig::new().identity(Identity::from_pem(cert, key)))
}

pub struct Listener {
    listener: TcpListener,
    server: Router,
    tls: bool,
}

pub const SETTINGS: [&str; 4] = [
    "SPOOLTAIL_GRPC_LISTEN",
    "SPOOLTAIL_GRPC_TLS_CERT",
    "SPOOLTAIL_GRPC_TLS_KEY",
    "SPOOLTAIL_GRPC_TLS",
];

impl Listener {
    pub async fn bind_from_env(broker: Broker) -> Result<Option<Self>> {
        let [addr, cert, key, mode] = SETTINGS.map(std::env::var_os);
        if addr.is_none() && cert.is_none() && key.is_none() && mode.is_none() {
            return Ok(None);
        }
        let tls = match mode.as_deref() {
            None => true,
            Some(value) if value == "on" => true,
            Some(value) if value == "off" => false,
            _ => anyhow::bail!("SPOOLTAIL_GRPC_TLS must be on or off"),
        };
        let addr: std::net::SocketAddr = addr
            .context("set SPOOLTAIL_GRPC_LISTEN to enable gRPC")?
            .to_str()
            .context("gRPC listen address must be UTF-8")?
            .parse()
            .context("parse SPOOLTAIL_GRPC_LISTEN as an IP address and port")?;
        let mut builder = Server::builder();
        match (tls, cert, key) {
            (true, Some(cert), Some(key)) => {
                builder = builder
                    .tls_config(tls_config(Path::new(&cert), Path::new(&key))?)
                    .context("configure gRPC TLS")?;
            }
            (true, _, _) => {
                anyhow::bail!(
                    "set SPOOLTAIL_GRPC_TLS_CERT and SPOOLTAIL_GRPC_TLS_KEY together for gRPC TLS"
                );
            }
            (false, None, None) => {}
            (false, _, _) => {
                anyhow::bail!(
                    "unset SPOOLTAIL_GRPC_TLS_CERT and SPOOLTAIL_GRPC_TLS_KEY for plaintext gRPC"
                );
            }
        }
        let server = builder.add_service(service(broker));
        let listener = TcpListener::bind(addr)
            .await
            .context("bind gRPC TCP listener")?;
        Ok(Some(Self {
            listener,
            server,
            tls,
        }))
    }

    pub async fn serve(self) -> Result<()> {
        let scheme = if self.tls { "https" } else { "http" };
        println!(
            "gRPC listening on {scheme}://{}",
            self.listener.local_addr()?
        );
        self.server
            .serve_with_incoming(TcpIncoming::from(self.listener).with_nodelay(Some(true)))
            .await
            .context("serve gRPC")
    }
}

#[cfg(feature = "bench-transports")]
pub async fn serve_benchmark(broker: Broker, addr: &str) -> Result<()> {
    let mut builder = Server::builder();
    #[cfg(feature = "bench-tls")]
    if let Some((cert, key)) = crate::bench_tls::identity_files()? {
        builder = builder.tls_config(tls_config(&cert, &key)?)?;
    }
    let server = builder.add_service(service(broker));
    #[cfg(unix)]
    if let Some(path) = addr.strip_prefix("unix:") {
        let listener = tokio::net::UnixListener::bind(path).context("bind gRPC Unix socket")?;
        println!("Listening {addr}");
        server
            .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
            .await?;
        return Ok(());
    }
    let addr = addr.parse().context("parse gRPC listen address")?;
    println!("Listening {addr}");
    server.serve(addr).await?;
    Ok(())
}
