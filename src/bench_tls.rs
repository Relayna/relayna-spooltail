use anyhow::{Context, Result};
use axum::Router;
use axum_server::{
    accept::NoDelayAcceptor,
    tls_rustls::{RustlsAcceptor, RustlsConfig},
};
use std::path::PathBuf;

pub fn identity_files() -> Result<Option<(PathBuf, PathBuf)>> {
    let cert = std::env::var_os("SPOOLTAIL_BENCH_CERT");
    let key = std::env::var_os("SPOOLTAIL_BENCH_KEY");
    match (cert, key) {
        (Some(cert), Some(key)) => Ok(Some((cert.into(), key.into()))),
        (None, None) => Ok(None),
        _ => anyhow::bail!("set both SPOOLTAIL_BENCH_CERT and SPOOLTAIL_BENCH_KEY for TLS"),
    }
}

pub async fn serve_http(app: Router, addr: &str, (cert, key): (PathBuf, PathBuf)) -> Result<()> {
    let addr = addr
        .parse::<std::net::SocketAddr>()
        .context("parse HTTPS benchmark listen address")?;
    let tls = RustlsConfig::from_pem_file(cert, key)
        .await
        .context("load HTTPS benchmark identity")?;
    println!("Listening https://{addr}");
    let acceptor = RustlsAcceptor::new(tls).acceptor(NoDelayAcceptor::new());
    axum_server::bind(addr)
        .acceptor(acceptor)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}
