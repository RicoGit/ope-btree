use common::noop_hasher::NoOpHasher;
use env_logger::Env;
use std::error::Error;
use tonic::transport::Server;
use lib::rpc::db_rpc_client::{DbRpcClient};
use tonic::Request;
use tokio_stream::wrappers::ReceiverStream;

mod lib;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + 'static>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    // simple client just create Db and get and put few values (for debug purpose)

    let addr = "http://[::1]:7777";
    let mut client = DbRpcClient::connect(addr).await?;
    
    log::info!("Connected to {}", addr);

    // create channel for putting out to get
    let(client_replies_in, client_replies_out) = tokio::sync::mpsc::channel(1);

    // execute get for getting server requests stream
    let server_requests = client.get(Request::new(ReceiverStream::new(client_replies_out))).await?;

    // iterate over server stream and put to channel client replies

    let get_k1_resp = lib::ClientSearchCb::new(
        server_requests.into_inner(),
        client_replies_in
    ).get_round_trip::<String, String>("k1".to_string())?;

    log::info!("get k1 response: {:?}", get_k1_resp);

    Ok(())
}
