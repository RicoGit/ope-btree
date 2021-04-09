//! OpeDb client ans server test via Grpc transport with no encryption and with noop hasher.
//! Complex test for index and database.

use bytes::Bytes;
use client::ope_btree::test::NoOpCrypt;
use client::ope_btree::OpeBTreeClient;
use client::ope_db::OpeDatabaseClient;
use client_grpc::grpc::rpc::db_rpc_client::DbRpcClient;
use client_grpc::grpc::GrpcDbRpc;

use common::noop_hasher::NoOpHasher;
use common::Hash;
use futures::FutureExt;

use log::LevelFilter;

use server::ope_btree::OpeBTreeConf;
use server::ope_db::DatasetChanged;

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::atomic::AtomicUsize;

use tokio::sync::mpsc::{channel, Receiver, Sender};

use tonic::transport::Server;

// todo move common code to common module

#[tokio::test]
async fn get_from_empty_db_test() {
    // get from empty db

    let (tx, _rx) = channel::<DatasetChanged>(1);
    let mut session = TestSession::create(tx).await;

    let result = session.client.get(k(1)).await;
    assert_eq!(result.unwrap(), None);
    session.close();
}

// todo move to common
struct TestSession {
    client: OpeDatabaseClient<NoOpCrypt, NoOpCrypt, NoOpHasher, GrpcDbRpc>,
    server: tokio::sync::oneshot::Sender<()>,
}

impl TestSession {
    pub async fn create(sender: Sender<DatasetChanged>) -> Self {
        // OS will choose free port
        let port = portpicker::pick_unused_port().expect("No ports free");
        let address = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);

        log::info!("Address: {:?}", address);

        let server = run_server(address, sender).await;
        let client = connect_client(address).await;

        TestSession { client, server }
    }

    pub fn close(self) {
        self.server.send(()).unwrap();
    }
}

#[tokio::test]
async fn put_to_empty_db_test() {
    // put to empty db

    let (tx, _rx) = channel::<DatasetChanged>(1);
    let mut session = TestSession::create(tx).await;

    let result = session.client.put(k(1), v(1)).await;
    assert_eq!(result.unwrap(), None);
    session.close()
}

#[tokio::test]
async fn put_one_and_get_it_back_test() {
    // put one and get it back
    init_logger();

    let (tx, mut rx) = channel::<DatasetChanged>(1);
    let mut session = TestSession::create(tx).await;

    let result = session.client.put(k(1), v(1)).await;
    assert_eq!(result.unwrap(), None);

    let result = session.client.get(k(1)).await;
    assert_eq!(result.unwrap(), Some(v(1)));

    assert_eq!(
        rx.recv().await.unwrap(),
        DatasetChanged::new(h("[[k1][v1]]"), 1, Bytes::new())
    );
    session.close()
}

#[tokio::test]
async fn update_single_item_test() {
    // put one and get it back, update it and get back again
    init_logger();

    let (tx, mut rx) = channel::<DatasetChanged>(1);
    let mut session = TestSession::create(tx).await;

    assert_eq!(session.client.put(k(1), v(1)).await.unwrap(), None);
    assert_eq!(session.client.get(k(1)).await.unwrap(), Some(v(1)));
    assert_eq!(rx.recv().await.unwrap(), dc("[[k1][v1]]", 1));

    assert_eq!(session.client.put(k(1), v(42)).await.unwrap(), Some(v(1)));
    assert_eq!(session.client.get(k(1)).await.unwrap(), Some(v(42)));
    assert_eq!(rx.recv().await.unwrap(), dc("[[k1][v42]]", 2));
    session.close()
}

#[tokio::test]
async fn one_depth_tree_test() {
    // put 4 and get them back (tree depth 1)
    init_logger();

    let (tx, rx) = channel::<DatasetChanged>(1);
    let mut session = TestSession::create(tx).await;

    let changes = put(4, &mut session.client, rx).await;
    assert_eq!(changes, dc("[[k1][v1]][[k2][v2]][[k3][v3]][[k4][v4]]", 4));

    get(4, &mut session.client).await;
    session.close()
}

#[tokio::test]
async fn two_depth_tree_test() {
    // put 10 and get them back (tree depth 2)
    init_logger();

    let (tx, rx) = channel::<DatasetChanged>(1);
    let mut session = TestSession::create(tx).await;

    let n = 10;
    let changes = put(n, &mut session.client, rx).await;
    // k1-k2-k6 - root
    // k1-k10-k2 | k3-k4 | k5-k6 | k7-k8-k9 - leaves
    assert_eq!(changes, dc("[k2][k4][k6][[[k1][v1]][[k10][v10]][[k2][v2]]][[[k3][v3]][[k4][v4]]][[[k5][v5]][[k6][v6]]][[[k7][v7]][[k8][v8]][[k9][v9]]]", n));

    get(n, &mut session.client).await;
    session.close()
}

#[tokio::test]
async fn three_depth_tree_test() {
    // put 20 and get them back (tree depth 3)
    init_logger();

    let (tx, rx) = channel::<DatasetChanged>(1);
    let mut session = TestSession::create(tx).await;

    let n = 20;
    let changes = put(n, &mut session.client, rx).await;
    assert_eq!(changes.new_version, n);

    get(n, &mut session.client).await;
    session.close()
}

#[tokio::test]
async fn five_depth_tree_test() {
    // put 200 and get them back (tree depth 5)
    init_logger();

    let (tx, rx) = channel::<DatasetChanged>(1);
    let mut session = TestSession::create(tx).await;

    let n = 200;
    let changes = put(n, &mut session.client, rx).await;
    assert_eq!(changes.new_version, n);

    get(n, &mut session.client).await;
    session.close()
}

#[tokio::test]
async fn five_depth_tree_reverse_test() {
    // put 200 in reverse order and get them back (tree depth 5)
    init_logger();

    let (tx, rx) = channel::<DatasetChanged>(1);
    let mut session = TestSession::create(tx).await;

    let n = 200;
    let changes = reverse_put(n, &mut session.client, rx).await;
    assert_eq!(changes.new_version, n);

    reverse_get(n, &mut session.client).await;
    session.close()
}

async fn run_server(
    address: SocketAddrV4,
    sender: Sender<DatasetChanged>,
) -> tokio::sync::oneshot::Sender<()> {
    let (stop_server_trigger, shutdown_signal) = tokio::sync::oneshot::channel();

    let conf = OpeBTreeConf {
        arity: 4,
        alpha: 0.25,
    };
    let db = server_grpc::grpc::new_in_memory_db::<NoOpHasher>(conf, sender).await;

    // let addr = address.parse().unwrap();

    log::info!("Start listening {}", address);

    tokio::spawn(async move {
        Server::builder()
            .add_service(server_grpc::grpc::rpc::db_rpc_server::DbRpcServer::new(db))
            .serve_with_shutdown(SocketAddr::V4(address), shutdown_signal.map(|_| ()))
            .await
            .unwrap()
    });

    stop_server_trigger
}

async fn connect_client(
    address: SocketAddrV4,
) -> OpeDatabaseClient<NoOpCrypt, NoOpCrypt, NoOpHasher, GrpcDbRpc> {
    loop {
        if let Ok(client) = DbRpcClient::connect(format!("http://{}", address)).await {
            // if let Ok(client) = DbRpcClient::connect(format!("http://{}", address.to_string())).await {
            let rpc = GrpcDbRpc::new(client);
            log::info!("Connected to {}", address);

            // Creates client for String Key and Val, dummy Hasher and without Encryption
            let index =
                OpeBTreeClient::<NoOpCrypt, NoOpHasher>::new(Hash::empty(), NoOpCrypt {}, ());

            let db_client = OpeDatabaseClient::<NoOpCrypt, NoOpCrypt, NoOpHasher, GrpcDbRpc>::new(
                index,
                NoOpCrypt {},
                rpc,
                AtomicUsize::new(0),
                Bytes::from("dataset_id"),
            );
            break db_client;
        } else {
            // waiting server
        }
    }
}

fn init_logger() {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(LevelFilter::Debug)
        .try_init();
}

fn k(idx: usize) -> String {
    format!("k{}", idx)
}

fn v(idx: usize) -> String {
    format!("v{}", idx)
}

fn h(str: &str) -> Hash {
    Hash::build::<NoOpHasher, _>(str.as_bytes())
}

fn dc(hash: &str, version: usize) -> DatasetChanged {
    DatasetChanged::new(h(hash), version, Bytes::new())
}

async fn get(
    n: usize,
    client: &mut OpeDatabaseClient<NoOpCrypt, NoOpCrypt, NoOpHasher, GrpcDbRpc>,
) {
    for idx in 1..n + 1 {
        let res = client.get(k(idx)).await;
        assert_eq!(res.unwrap(), Some(v(idx)));
    }
}

/// Puts n items and returns last MerkelRoot
async fn put(
    n: usize,
    client: &mut OpeDatabaseClient<NoOpCrypt, NoOpCrypt, NoOpHasher, GrpcDbRpc>,
    mut rx: Receiver<DatasetChanged>,
) -> DatasetChanged {
    let mut m_root = None;
    for idx in 1..n + 1 {
        let res = client.put(k(idx), v(idx)).await;
        assert_eq!(res.unwrap(), None);
        m_root = rx.recv().await;
        assert!(m_root.is_some());
    }
    m_root.unwrap()
}

async fn reverse_get(
    n: usize,
    client: &mut OpeDatabaseClient<NoOpCrypt, NoOpCrypt, NoOpHasher, GrpcDbRpc>,
) {
    for idx in (1..n + 1).rev() {
        let res = client.get(k(idx)).await;
        assert_eq!(res.unwrap(), Some(v(idx)));
    }
}

/// Puts n items and returns last MerkelRoot
async fn reverse_put(
    n: usize,
    client: &mut OpeDatabaseClient<NoOpCrypt, NoOpCrypt, NoOpHasher, GrpcDbRpc>,
    mut rx: Receiver<DatasetChanged>,
) -> DatasetChanged {
    let mut m_root = None;
    for idx in (1..n + 1).rev() {
        let res = client.put(k(idx), v(idx)).await;
        assert_eq!(res.unwrap(), None);
        m_root = rx.recv().await;
        assert!(m_root.is_some());
    }
    m_root.unwrap()
}
