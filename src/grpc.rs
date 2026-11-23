//! gRPC service over the same store and subscription machinery as the HTTP API.
use crate::{Broker, Problem, WorkLease, feed, publish, store};
use anyhow::{Context, Result};
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

mod wire {
    tonic::include_proto!("spooltail.v1");
}
use wire::spooltail_server::{Spooltail, SpooltailServer};
use wire::{
    ClaimRequest, ClaimResponse, CommitRequest, CompleteRequest, Delivery, Empty, MoveTaskRequest,
    NextResponse, Offset, Page, Partition, Partitions, Publish, Record, SettleRequest,
    SettlementAction, StatsResponse, StreamGroup, Task, TaskId,
};

struct Api(Broker);

fn status(error: anyhow::Error) -> Status {
    match error.downcast_ref::<Problem>() {
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

fn decode(bytes: &[u8]) -> Result<Value> {
    serde_json::from_slice(bytes)
        .map_err(|error| Problem::Invalid(format!("invalid event JSON: {error}")).into())
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
            .run(move |s| {
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

    async fn next(&self, request: Request<StreamGroup>) -> Result<Response<NextResponse>, Status> {
        let q = request.into_inner();
        let record = self
            .0
            .run(move |s| s.group_next(&q.topic, &q.group, q.partition))
            .await
            .map_err(status)?;
        let record = record.map(encode).transpose().map_err(status)?;
        Ok(Response::new(NextResponse { record }))
    }

    async fn commit(&self, request: Request<CommitRequest>) -> Result<Response<Empty>, Status> {
        let q = request.into_inner();
        self.0
            .run(move |s| s.commit(&q.topic, &q.group, q.offset, q.partition))
            .await
            .map_err(status)?;
        Ok(Response::new(Empty {}))
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
        let stats = self
            .0
            .run(|s| {
                Ok(StatsResponse {
                    payload_reads: s.payload_reads.get(),
                    recovered_frames: s.recovered_frames.try_into()?,
                })
            })
            .await
            .map_err(status)?;
        Ok(Response::new(stats))
    }

    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<Delivery, Status>> + Send>>;

    async fn subscribe(
        &self,
        request: Request<Task>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let q = request.into_inner();
        let events = feed(self.0.clone(), q.topic, q.task_id, q.after).map(|item| {
            let data_json = serde_json::to_vec(&item.data).map_err(|error| status(error.into()))?;
            Ok(Delivery {
                kind: item.kind.into(),
                event_id: item.id,
                data_json,
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
