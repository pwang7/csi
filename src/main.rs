//! K8S CSI `gRPC` service

#![deny(
    // The following are allowed by default lints according to
    // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html
    anonymous_parameters,
    bare_trait_objects,
    // box_pointers, // futures involve boxed pointers
    // elided_lifetimes_in_paths, // allow anonymous lifetime
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs, // TODO: add documents
    single_use_lifetimes, // TODO: fix lifetime names only used once
    trivial_casts, // TODO: remove trivial casts in code
    trivial_numeric_casts,
    // unreachable_pub, allow clippy::redundant_pub_crate lint instead
    unsafe_code,
    unstable_features,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    // unused_results, // TODO: fix unused results
    variant_size_differences,

    // Treat warnings as errors
    warnings, // treat all wanings as errors

    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo
)]
#![allow(
    // Some explicitly allowed Clippy lints, must have clear reason to allow
    clippy::implicit_return, // actually omitting the return keyword is idiomatic Rust code
    clippy::module_name_repetitions, // repeation of module name in a struct name is not big deal
    clippy::multiple_crate_versions, // multi-version dependency crates is not able to fix
    clippy::panic, // allow debug_assert, panic in production code
)]

// Ignore format and lint to generated code
#[rustfmt::skip]
#[allow(
    variant_size_differences,
    unreachable_pub,
    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo
)]
mod csi;
// Ignore format and lint to generated code
#[rustfmt::skip]
#[allow(
    unreachable_pub,
    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo
)]
mod csi_grpc;
#[rustfmt::skip]
#[allow(
    unreachable_pub,
    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo
)]
mod datenlord_worker;
#[rustfmt::skip]
#[allow(
    unreachable_pub,
    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo
)]
mod datenlord_worker_grpc;

mod controller;
mod identity;
mod meta_data;
mod node;
mod worker;

use controller::ControllerImpl;
use identity::IdentityImpl;
use meta_data::{
    util::{self, RunAsRole},
    MetaData,
};
use node::NodeImpl;
use worker::WorkerImpl;

use anyhow::Context;
use clap::{App, Arg, ArgMatches};
use grpcio::{Environment, Server};
use log::{debug, info};
use std::sync::Arc;

/// Build CSI and worker service
fn build_grpc_servers(
    end_point: String,
    worker_port: u16,
    node_id: String,
    driver_name: String,
    data_dir: String,
    run_as: RunAsRole,
    etcd_client: etcd_rs::Client,
) -> anyhow::Result<(Server, Server)> {
    remove_socket_file(&end_point);
    remove_socket_file(util::LOCAL_WORKER_SOCKET);

    let (worker_bind_address, worker_bind_port) = if let RunAsRole::Controller = run_as {
        // In this case, run controller only, so worker server won't be public,
        // bind worker service at a socket file and port as 0
        (util::LOCAL_WORKER_SOCKET, 0) // Non-public worker service
    } else {
        ("0.0.0.0", worker_port) // Public worker service
    };

    let ephemeral = false; // TODO: read from command line argument
    let md = MetaData::new(
        node_id,
        data_dir,
        worker_port,
        ephemeral,
        util::MAX_VOLUMES_PER_NODE,
        util::MAX_VOLUME_STORAGE_CAPACITY,
        run_as,
        etcd_client,
    )?;
    let meta_data = Arc::new(md);
    /*
       let (controller_meta_data, node_worker_meta_data) = if let RunAsRole::Both = run_as {
           let meta_data2 = MetaData::new(
               util::CSI_PLUGIN_NAME.to_owned(),
               node_id,
               util::CSI_PLUGIN_VERSION.to_owned(),
               data_dir,
               worker_port,
               ephemeral,
               util::MAX_VOLUME_STORAGE_CAPACITY,
               run_as,
           );
           // If run both controller and node together, then their meta data should be different
           (Arc::new(meta_data1), Arc::new(meta_data2))
       } else {
           // If run controller and node seperately,
           // actually only one role is in effect in a process,
           // so shared meta data between controller and node in a process should be fine
           let arc_meta_data = Arc::new(meta_data1);
           (
               Arc::<MetaData>::clone(&arc_meta_data),
               Arc::<MetaData>::clone(&arc_meta_data),
           )
       };
    */
    let identity_service = csi_grpc::create_identity(IdentityImpl::new(
        driver_name,
        util::CSI_PLUGIN_VERSION.to_owned(),
    ));
    let controller_service =
        csi_grpc::create_controller(ControllerImpl::new(Arc::<MetaData>::clone(&meta_data)));
    let node_service = csi_grpc::create_node(NodeImpl::new(Arc::<MetaData>::clone(&meta_data)));

    // let (mem_size, overflow) = 1024_usize.overflowing_mul(1024);
    // debug_assert!(!overflow, "computing memory size overflowed");
    // let quota = ResourceQuota::new(Some("DatenLordWokerQuota")).resize_memory(mem_size);
    // let ch_builder = ChannelBuilder::new(Arc::<Environment>::clone(&env)).set_resource_quota(quota);
    let csi_server = grpcio::ServerBuilder::new(Arc::new(Environment::new(1)))
        .register_service(identity_service)
        .register_service(controller_service)
        .register_service(node_service)
        .bind(end_point, 0) // Port is not need when bind to socket file
        // .channel_args(ch_builder.build_args())
        .build()
        .context("failed to build CSI gRPC server")?;

    // let env = Arc::new(Environment::new(1));
    // let (mem_size2, overflow2) = 1024_usize.overflowing_mul(1024);
    // debug_assert!(!overflow2, "computing memory size overflowed");
    // let quota = ResourceQuota::new(Some("WorkerQuota")).resize_memory(mem_size2);
    // let ch_builder = ChannelBuilder::new(Arc::<Environment>::clone(&env)).set_resource_quota(quota);
    let worker_service = datenlord_worker_grpc::create_worker(WorkerImpl::new(meta_data));
    let worker_server = grpcio::ServerBuilder::new(Arc::new(Environment::new(1)))
        .register_service(worker_service)
        .bind(worker_bind_address, worker_bind_port)
        // .channel_args(ch_builder.build_args())
        .build()
        .context("failed to build DatenLord worker server")?;

    Ok((csi_server, worker_server))
}

/// Remove existing socket file before run CSI `gRPC` server
fn remove_socket_file(sock: &str) {
    if std::path::Path::new(sock).exists() {
        if let Err(e) = std::fs::remove_file(sock) {
            panic!(
                "failed to remove existing socket file {}, the error is: {}",
                sock, e,
            );
        }
    }
}

/// Helper function to run server
fn run_server_helper(srv: &mut Server) {
    srv.start();
    for (host, port) in srv.bind_addrs() {
        info!("gRPC server listening on {}:{}", host, port);
    }
}

/// Run server synchronuously
fn run_sync_servers(mut csi_server: Server, mut worker_server: Server) {
    run_server_helper(&mut csi_server);
    run_server_helper(&mut worker_server);

    loop {
        std::thread::park();
    }
}

/// Run server asynchronuously
fn run_async_servers(csi_server: Server, worker_server: Server) {
    /// The future to run `gRPC` server
    async fn run_server(mut csi_server: Server, mut worker_server: Server) {
        run_server_helper(&mut csi_server);
        run_server_helper(&mut worker_server);
        let f = futures::future::pending::<()>();
        f.await;
    }
    smol::run(async move {
        run_server(csi_server, worker_server).await;
    });
}

/// Build etcd client
fn build_etcd_client(etcd_address_vec: Vec<String>) -> anyhow::Result<etcd_rs::Client> {
    let etcd_client = smol::run(async move {
        etcd_rs::Client::connect(etcd_rs::ClientConfig {
            endpoints: etcd_address_vec.clone(),
            auth: None,
        })
        .await
        .map_err(|e| {
            anyhow::anyhow!(format!(
                "failed to build etcd client to {:?}, the error is: {}",
                etcd_address_vec, e,
            ))
        })
    })?;
    Ok(etcd_client)
}

/// Argument name of end point
const END_POINT_ARG_NAME: &str = "endpoint";
/// Argument name of worker port
const WORKER_PORT_ARG_NAME: &str = "workerport";
/// Argument name of node ID
const NODE_ID_ARG_NAME: &str = "nodeid";
/// Argument name of driver name
const DRIVER_NAME_ARG_NAME: &str = "drivername";
/// Argument name of data directory
const DATA_DIR_ARG_NAME: &str = "datadir";
/// Argument name of run as role
const RUN_AS_ARG_NAME: &str = "runas";
/// Argument name of etcd addresses
const ETCD_ADDRESS_ARG_NAME: &str = "etcd";

/// Parse command line arguments
fn parse_args() -> ArgMatches<'static> {
    App::new("DatenLord")
        .about("Cloud Native Storage")
        .arg(
            Arg::with_name(END_POINT_ARG_NAME)
                .short("s")
                .long(END_POINT_ARG_NAME)
                .value_name("SOCKET_FILE")
                .takes_value(true)
                .required(true)
                .help(
                    "Set the socket end point of CSI service, \
                        required argument, no default value",
                ),
        )
        .arg(
            Arg::with_name(WORKER_PORT_ARG_NAME)
                .short("p")
                .long(WORKER_PORT_ARG_NAME)
                .value_name("PORT")
                .takes_value(true)
                .help(
                    "Set the port of worker service port, \
                        default value 50051",
                ),
        )
        .arg(
            Arg::with_name(NODE_ID_ARG_NAME)
                .short("n")
                .long(NODE_ID_ARG_NAME)
                .value_name("NODE ID")
                .takes_value(true)
                .required(true)
                .help(
                    "Set the name/address of the node, \
                        should be a real host name or network address, \
                        required argument, no default value",
                ),
        )
        .arg(
            Arg::with_name(DRIVER_NAME_ARG_NAME)
                .short("d")
                .long(DRIVER_NAME_ARG_NAME)
                .value_name("DRIVER NAME")
                .takes_value(true)
                .help(&format!(
                    "Set the CSI driver name, default as {}",
                    util::CSI_PLUGIN_NAME,
                )),
        )
        .arg(
            Arg::with_name(DATA_DIR_ARG_NAME)
                .long(DATA_DIR_ARG_NAME)
                .value_name("DATA DIR")
                .takes_value(true)
                .help(&format!(
                    "Set data directory, default as {}",
                    util::DATA_DIR,
                )),
        )
        .arg(
            Arg::with_name(RUN_AS_ARG_NAME)
                .short("r")
                .long(RUN_AS_ARG_NAME)
                .value_name("ROLE NAME")
                .takes_value(true)
                .help(
                    "Set the runtime service, \
                        set as controller, node or both, \
                        default as node",
                ),
        )
        .arg(
            Arg::with_name(ETCD_ADDRESS_ARG_NAME)
                .short("e")
                .long(ETCD_ADDRESS_ARG_NAME)
                .value_name("ETCD IP:PORT,ETCD IP:PORT")
                .takes_value(true)
                .required(true)
                .help(
                    "Set the etcd addresses of format http://ip:port, \
                        if multiple etcd addresses use comma to seperate, \
                        required argument, no default value",
                ),
        )
        .get_matches()
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let matches = parse_args();
    let end_point = match matches.value_of(END_POINT_ARG_NAME) {
        Some(s) => {
            let sock = s.to_owned();
            if !sock.starts_with("unix:///") {
                panic!(
                    "invalid socket end point: {}, should start with unix:///",
                    sock
                );
            }
            sock
        }
        None => util::END_POINT.to_owned(),
    };
    let worker_port = match matches.value_of(WORKER_PORT_ARG_NAME) {
        Some(p) => match p.parse::<u16>() {
            Ok(port) => port,
            Err(e) => panic!("failed to parse port, the error is: {}", e),
        },
        None => util::DEFAULT_PORT,
    };
    let node_id = match matches.value_of(NODE_ID_ARG_NAME) {
        Some(n) => n.to_owned(),
        None => util::DEFAULT_NODE_NAME.to_owned(),
    };
    let driver_name = match matches.value_of(DRIVER_NAME_ARG_NAME) {
        Some(d) => d.to_owned(),
        None => util::CSI_PLUGIN_NAME.to_owned(),
    };
    let data_dir = match matches.value_of(DATA_DIR_ARG_NAME) {
        Some(d) => d.to_owned(),
        None => util::DATA_DIR.to_owned(),
    };
    let run_as = match matches.value_of(RUN_AS_ARG_NAME) {
        Some(r) => match r {
            "both" => RunAsRole::Both,
            "controller" => RunAsRole::Controller,
            "node" => RunAsRole::Node,
            _ => panic!(
                "invalid {} argument {}, must be one of both, controller, worker",
                RUN_AS_ARG_NAME, r,
            ),
        },
        None => RunAsRole::Node,
    };
    let etcd_address_vec = match matches.value_of(ETCD_ADDRESS_ARG_NAME) {
        Some(a) => a.split(',').map(std::borrow::ToOwned::to_owned).collect(),
        None => Vec::new(),
    };
    debug!(
        "{}={}, {}={}, {}={}, {}={}, {}={}, {}={:?}, {}={:?}",
        END_POINT_ARG_NAME,
        end_point,
        WORKER_PORT_ARG_NAME,
        worker_port,
        NODE_ID_ARG_NAME,
        node_id,
        DRIVER_NAME_ARG_NAME,
        driver_name,
        DATA_DIR_ARG_NAME,
        data_dir,
        RUN_AS_ARG_NAME,
        run_as,
        ETCD_ADDRESS_ARG_NAME,
        etcd_address_vec,
    );

    let etcd_client = build_etcd_client(etcd_address_vec)?;
    let (csi_server, worker_server) = build_grpc_servers(
        end_point,
        worker_port,
        node_id,
        driver_name,
        data_dir,
        run_as,
        etcd_client,
    )?;
    let async_server = false;
    if async_server {
        run_async_servers(csi_server, worker_server);
    } else {
        run_sync_servers(csi_server, worker_server);
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::meta_data::util;
    use super::*;
    use csi::*;
    use csi_grpc::{ControllerClient, IdentityClient, NodeClient};
    use grpcio::{ChannelBuilder, EnvBuilder};

    use protobuf::RepeatedField;
    use std::fs::{self, File};
    use std::io::prelude::*;
    use std::path::{Path, PathBuf};
    use std::sync::Once;
    use std::thread;
    use utilities::{Cast, OverflowArithmetic};

    const NODE_PUBLISH_VOLUME_TARGET_PATH: &str = "/tmp/target_volume_path";
    const NODE_PUBLISH_VOLUME_TARGET_PATH_1: &str = "/tmp/target_volume_path_1";
    const NODE_PUBLISH_VOLUME_TARGET_PATH_2: &str = "/tmp/target_volume_path_2";
    const NODE_PUBLISH_VOLUME_ID: &str = "46ebd0ee-0e6d-43c9-b90d-ccc35a913f3e";
    const ETCD_ENV_VAR_KEY: &str = "ETCD_END_POINT";
    const WORKER_PORT_ENV_VAR_KEY: &str = "WORKER_PORT";
    const DEFAULT_ETCD_ENDPOINT_FOR_TEST: &str = "http://127.0.0.1:2379";
    static GRPC_SERVER: Once = Once::new();

    #[test]
    fn test_all() -> anyhow::Result<()> {
        // TODO: run test case in parallel
        // Because they all depend on etcd, so cannot run in parallel now
        test_meta_data().context("test meta data failed")?;
        test_identity_server().context("test identity server failed")?;
        test_controller_server().context("test controller server failed")?;
        test_node_server().context("test node server failed")?;
        Ok(())
    }

    fn test_meta_data() -> anyhow::Result<()> {
        let etcd_address_vec = get_etcd_address_vec();
        let etcd_client = build_etcd_client(etcd_address_vec)?;
        clear_test_data(&etcd_client)?;

        let worker_port = util::DEFAULT_PORT;
        let node_id = util::DEFAULT_NODE_NAME;
        let data_dir = util::DATA_DIR;
        let run_as = RunAsRole::Both;
        let ephemeral = false;
        let meta_data = MetaData::new(
            node_id.to_owned(),
            data_dir.to_owned(),
            worker_port,
            ephemeral,
            util::MAX_VOLUMES_PER_NODE,
            util::MAX_VOLUME_STORAGE_CAPACITY,
            run_as,
            etcd_client,
        )?;

        let vol_id = "the-fake-ephemeral-volume-id-for-meta-data-test";
        let mut volume = meta_data::DatenLordVolume::build_ephemeral_volume(
            vol_id,
            "ephemeral-volume", // vol_name
            util::DEFAULT_NODE_NAME,
            meta_data.get_volume_path(NODE_PUBLISH_VOLUME_ID).as_path(), // vol_path
        )?;
        let add_vol_res = meta_data.add_volume_meta_data(vol_id.to_owned(), &volume);
        assert!(
            add_vol_res.is_ok(),
            "failed to add new volume meta data to etcd"
        );
        let get_vol_res = meta_data.get_volume_by_name(&volume.vol_name)?;
        assert_eq!(
            get_vol_res.vol_name, volume.vol_name,
            "volume name not match"
        );

        let new_size_bytes = 2.overflow_mul(util::EPHEMERAL_VOLUME_STORAGE_CAPACITY);
        let exp_vol_res = meta_data.expand(&mut volume, new_size_bytes)?;
        assert_eq!(
            exp_vol_res,
            util::EPHEMERAL_VOLUME_STORAGE_CAPACITY,
            "the old size before expand not match"
        );

        let expanded_vol = meta_data.get_volume_by_id(vol_id)?;
        assert_eq!(
            expanded_vol.size_bytes, new_size_bytes,
            "the expanded volume size not match"
        );

        let selected_node = meta_data.select_node(None)?;
        assert_eq!(
            selected_node.node_id,
            util::DEFAULT_NODE_NAME,
            "selected node ID not match"
        );

        let snap_id = "the-fake-snapshot-id-for-meta-data-test";
        let snapshot = meta_data::DatenLordSnapshot::new(
            "test-snapshot-name".to_owned(), //snap_name,
            snap_id.to_owned(),              //snap_id,
            vol_id.to_owned(),
            node_id.to_owned(),
            meta_data.get_snapshot_path(snap_id),
            std::time::SystemTime::now(),
            0,    // size_bytes,
            true, // ready_to_use,
        );
        let add_snap_res = meta_data.add_snapshot_meta_data(snap_id.to_owned(), &snapshot);
        assert!(
            add_snap_res.is_ok(),
            "failed to add new snapshot meta data to etcd"
        );
        let get_snap_by_name_res = meta_data.get_snapshot_by_name(&snapshot.snap_name)?;
        assert_eq!(
            get_snap_by_name_res.snap_name, snapshot.snap_name,
            "snapshot name not match"
        );

        let get_snap_by_src_vol_id_res =
            meta_data.get_snapshot_by_src_volume_id(&snapshot.vol_id)?;
        assert_eq!(
            get_snap_by_src_vol_id_res.vol_id, snapshot.vol_id,
            "snapshot source volume ID not match"
        );

        let del_vol_res = meta_data.delete_volume_meta_data(vol_id)?;
        assert_eq!(del_vol_res.vol_id, vol_id, "deleted volume ID not match");
        let del_snap_res = meta_data.delete_snapshot_meta_data(snap_id)?;
        assert_eq!(
            del_snap_res.snap_id, snap_id,
            "deleted snapshot ID not match"
        );
        Ok(())
    }

    fn get_volume_path(vol_id: &str) -> PathBuf {
        Path::new(util::DATA_DIR).join(vol_id)
    }

    fn get_worker_port() -> u16 {
        match std::env::var(WORKER_PORT_ENV_VAR_KEY) {
            Ok(val) => {
                debug!("{}={}", WORKER_PORT_ENV_VAR_KEY, val);
                match val.parse::<u16>() {
                    Ok(port) => port,
                    Err(e) => panic!(
                        "failed to parse worker port={} to u16, \
                            the error is: {}",
                        val, e,
                    ),
                }
            }
            Err(_) => util::DEFAULT_PORT,
        }
    }

    fn get_etcd_address_vec() -> Vec<String> {
        match std::env::var(ETCD_ENV_VAR_KEY) {
            Ok(val) => {
                debug!("{}={}", ETCD_ENV_VAR_KEY, val);
                vec![val]
            }
            Err(_) => vec![DEFAULT_ETCD_ENDPOINT_FOR_TEST.to_owned()],
        }
    }

    fn clear_test_data(etcd_client: &etcd_rs::Client) -> anyhow::Result<()> {
        let dir_path = Path::new(util::DATA_DIR);
        if dir_path.exists() {
            fs::remove_dir_all(dir_path)?;
        }
        let node_volume_publish_path = Path::new(NODE_PUBLISH_VOLUME_TARGET_PATH);
        if node_volume_publish_path.exists() {
            let umount_res = util::umount_volume_bind_path(NODE_PUBLISH_VOLUME_TARGET_PATH);
            debug!(
                "un-mount {} result: {:?}",
                NODE_PUBLISH_VOLUME_TARGET_PATH, umount_res
            );
            fs::remove_dir_all(NODE_PUBLISH_VOLUME_TARGET_PATH)?;
        }

        let req = etcd_rs::DeleteRequest::new(etcd_rs::KeyRange::all());
        let _ = smol::run(async move {
            etcd_client.kv().delete(req).await.map_err(|e| {
                anyhow::anyhow!("failed to clear all data from etcd, the error is: {}", e,)
            })
        })?;

        Ok(())
    }

    fn run_server() -> anyhow::Result<()> {
        let end_point = util::END_POINT.to_owned();
        let worker_port = get_worker_port();
        let node_id = util::DEFAULT_NODE_NAME.to_owned();
        let driver_name = util::CSI_PLUGIN_NAME.to_owned();
        let data_dir = util::DATA_DIR.to_owned();
        let run_as = RunAsRole::Both;
        let etcd_address_vec = get_etcd_address_vec();
        let etcd_client = build_etcd_client(etcd_address_vec)?;

        let async_server = false;
        GRPC_SERVER.call_once(move || {
            let clear_res = clear_test_data(&etcd_client);
            assert!(
                clear_res.is_ok(),
                "failed to clear test data, the error is: {}",
                clear_res.unwrap_err(),
            );

            let (csi_server, worker_server) = match build_grpc_servers(
                end_point,
                worker_port,
                node_id,
                driver_name,
                data_dir,
                run_as,
                etcd_client,
            ) {
                Ok((s1, s2)) => (s1, s2),
                Err(e) => panic!(
                    "failed to build CSI server and worker server, \
                        the error is : {}",
                    e,
                ),
            };
            // Keep running the task in the background
            let _th = thread::spawn(move || {
                if async_server {
                    run_async_servers(csi_server, worker_server);
                } else {
                    run_sync_servers(csi_server, worker_server);
                }
            });
        });

        Ok(())
    }

    fn build_identity_client() -> anyhow::Result<IdentityClient> {
        run_server()?;
        let env = Arc::new(EnvBuilder::new().build());
        let ch = ChannelBuilder::new(env).connect(util::END_POINT);
        let identity_client = IdentityClient::new(ch);
        Ok(identity_client)
    }

    fn test_identity_server() -> anyhow::Result<()> {
        let client = build_identity_client()?;

        // Test get info
        let info_resp = client
            .get_plugin_info(&GetPluginInfoRequest::new())
            .context("failed to get GetPluginInfoResponse")?;
        assert_eq!(
            info_resp.name,
            util::CSI_PLUGIN_NAME,
            "GetPluginInfoResponse has incorrect name",
        );
        assert_eq!(
            info_resp.vendor_version,
            util::CSI_PLUGIN_VERSION,
            "GetPluginInfoResponse has incorrect version",
        );

        // Test get capabilities
        let cap_resp = client
            .get_plugin_capabilities(&GetPluginCapabilitiesRequest::new())
            .context("failed to get GetPluginCapabilitiesResponse")?;
        let caps = cap_resp.get_capabilities();
        let cap_vec = caps
            .iter()
            .map(|cap| cap.get_service().get_field_type())
            .collect::<Vec<_>>();
        assert_eq!(
            cap_vec,
            vec![
                PluginCapability_Service_Type::CONTROLLER_SERVICE,
                PluginCapability_Service_Type::VOLUME_ACCESSIBILITY_CONSTRAINTS,
            ],
            "get_plugin_capabilities returned capabilities not as expected"
        );

        // Test probe
        let prob_resp = client
            .probe(&ProbeRequest::new())
            .context("failed to get ProbeResponse")?;
        debug_assert!(
            prob_resp.get_ready().value,
            "ProbeResponse showed server not ready",
        );

        Ok(())
    }

    fn build_controller_client() -> anyhow::Result<ControllerClient> {
        run_server()?;
        let env = Arc::new(EnvBuilder::new().build());
        let ch = ChannelBuilder::new(env).connect(util::END_POINT);
        let controller_client = ControllerClient::new(ch);
        Ok(controller_client)
    }

    fn create_volume(
        client: &ControllerClient,
        req: &CreateVolumeRequest,
    ) -> anyhow::Result<CreateVolumeResponse> {
        let resp = client.create_volume(req)?;
        Ok(resp)
    }

    fn delete_volume(
        client: &ControllerClient,
        req: &DeleteVolumeRequest,
    ) -> anyhow::Result<DeleteVolumeResponse> {
        let resp = client.delete_volume(req)?;
        Ok(resp)
    }

    fn create_snapshot(
        client: &ControllerClient,
        req: &CreateSnapshotRequest,
    ) -> anyhow::Result<CreateSnapshotResponse> {
        let resp = client.create_snapshot(req)?;
        Ok(resp)
    }

    fn list_volumes(
        client: &ControllerClient,
        req: &ListVolumesRequest,
    ) -> anyhow::Result<ListVolumesResponse> {
        let resp = client.list_volumes(req)?;
        Ok(resp)
    }

    fn delete_snapshot(
        client: &ControllerClient,
        req: &DeleteSnapshotRequest,
    ) -> anyhow::Result<DeleteSnapshotResponse> {
        let resp = client.delete_snapshot(req)?;
        Ok(resp)
    }

    fn list_snapshots(
        client: &ControllerClient,
        req: &ListSnapshotsRequest,
    ) -> anyhow::Result<ListSnapshotsResponse> {
        let resp = client.list_snapshots(req)?;
        Ok(resp)
    }

    fn controller_expand_volume(
        client: &ControllerClient,
        req: &ControllerExpandVolumeRequest,
    ) -> anyhow::Result<ControllerExpandVolumeResponse> {
        let resp = client.controller_expand_volume(req)?;
        Ok(resp)
    }

    fn write_file_in_volume(
        vol_id: &str,
        vol_file_name: &str,
        vol_file_content: &str,
    ) -> anyhow::Result<()> {
        // Write some date to volume directory
        let vol_path = get_volume_path(vol_id);
        let mut vol_data_file = File::create(vol_path.join(vol_file_name))?;
        vol_data_file.write_all(vol_file_content.as_bytes())?;
        Ok(())
    }

    fn verify_volume_file_content(
        vol_id: &str,
        vol_file_name: &str,
        expected_content: &str,
    ) -> anyhow::Result<()> {
        let vol_file_path = get_volume_path(vol_id).join(vol_file_name);
        let buffer = fs::read_to_string(&vol_file_path).context(format!(
            "failed to read the file name={:?} of volume ID={}",
            vol_file_path, vol_id
        ))?;
        assert!(
            !buffer.is_empty(),
            "failed to read content from duplicated volume file"
        );
        assert_eq!(
            buffer, expected_content,
            "verify volume content failed, read content: {}, expected content: {}",
            buffer, expected_content,
        );
        Ok(())
    }

    fn test_controller_server() -> anyhow::Result<()> {
        let controller_client = build_controller_client()?;

        test_controller_create_volume_from_volume(&controller_client)?;
        test_controller_create_volume_from_snapshot(&controller_client)?;
        test_controller_create_delete_idempotency(&controller_client)?;
        test_controller_expand_volume(&controller_client)?;
        test_controller_create_and_list_snapshot(&controller_client)?;
        test_list_volumes(&controller_client)?;
        Ok(())
    }

    fn test_list_volumes(client: &ControllerClient) -> anyhow::Result<()> {
        let vol_names = (1..5)
            .map(|idx| format!("tmp_volume_name_{}", idx))
            .collect::<Vec<_>>();

        // Test create new volume
        let mut vc = VolumeCapability::new();
        vc.set_mount(VolumeCapability_MountVolume::new());
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);

        let mut creat_vol_req = CreateVolumeRequest::new();
        creat_vol_req.set_volume_capabilities(RepeatedField::from_vec(vec![vc]));

        let mut volumes = Vec::new();
        for vol_name in &vol_names {
            creat_vol_req.set_name((*vol_name).to_owned());
            let creat_resp = create_volume(client, &creat_vol_req)
                .context("failed to get CreateVolumeResponse")?;
            let vol = creat_resp.get_volume();
            volumes.push(vol.get_volume_id().to_owned());
        }

        // List all volumes
        let mut list_vol_req = ListVolumesRequest::new();
        let list_vol_resp1 = list_volumes(client, &list_vol_req)
            .context("failed to get ListVolumesResponse of all volumes")?;

        let list_vols1 = list_vol_resp1.get_entries();
        let all_vol_vec = list_vols1
            .iter()
            .map(|vol| vol.get_volume().get_volume_id())
            .collect::<Vec<_>>();
        let mut vol_vec1 = all_vol_vec.clone();
        vol_vec1.sort();
        let mut expect_vol_vec1 = volumes.clone();
        expect_vol_vec1.sort();
        assert_eq!(vol_vec1, expect_vol_vec1, "list volume result not match");
        assert_eq!(
            list_vol_resp1.get_next_token(),
            expect_vol_vec1.len().to_string(),
            "next volume index not match",
        );

        // List volume from starting position as 1 and max entries as 1
        let starting_pos: usize = 1;
        let max_entries = 2;
        list_vol_req.set_starting_token(starting_pos.to_string());
        list_vol_req.set_max_entries(max_entries);
        let list_vol_resp2 = list_volumes(client, &list_vol_req)
            .context("failed to get ListVolumesResponse of two volumes")?;

        let list_vols2 = list_vol_resp2.get_entries();
        let mut vol_vec2 = list_vols2
            .iter()
            .map(|vol| vol.get_volume().get_volume_id())
            .collect::<Vec<_>>();
        vol_vec2.sort();
        let end_pos = starting_pos.overflow_add(max_entries.cast::<usize>());
        let mut expect_vol_vec2 = all_vol_vec
            .get(starting_pos..end_pos)
            .map_or(Vec::new(), std::borrow::ToOwned::to_owned);
        expect_vol_vec2.sort();
        assert_eq!(vol_vec2, expect_vol_vec2, "list volume result not match");
        let next_starting_pos = starting_pos.overflow_add(max_entries.cast::<usize>());
        assert_eq!(
            list_vol_resp2.get_next_token(),
            next_starting_pos.to_string(),
            "next volume index not match",
        );

        // Delete remaining volumes
        let mut del_vol_req = DeleteVolumeRequest::new();
        for vol_id in volumes {
            del_vol_req.set_volume_id(vol_id.to_owned());
            let _del_vol_resp3 = delete_volume(client, &del_vol_req).context(format!(
                "failed to get DeleteVolumeResponse when delete volume ID={}",
                vol_id,
            ))?;
        }
        Ok(())
    }

    fn test_controller_create_volume_from_volume(client: &ControllerClient) -> anyhow::Result<()> {
        let vol_name = "tmp_volume";
        let dup_vol_name = "dup_volume";
        let vol_file_name = "volume.dat";
        let vol_file_content = "TMP_VOLUME_DATA";

        // Test create new volume
        let mut vc = VolumeCapability::new();
        vc.set_mount(VolumeCapability_MountVolume::new());
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);

        let mut creat_vol_req = CreateVolumeRequest::new();
        creat_vol_req.set_name(vol_name.to_owned());
        creat_vol_req.set_volume_capabilities(RepeatedField::from_vec(vec![vc]));

        let creat_resp1 = create_volume(client, &creat_vol_req)
            .context("failed to get CreateVolumeResponse when create first volume")?;
        let volume1 = creat_resp1.get_volume();

        // Write some date to volume directory
        write_file_in_volume(volume1.get_volume_id(), vol_file_name, vol_file_content)?;

        // Test create volume from existing volume
        creat_vol_req
            .mut_volume_content_source()
            .mut_volume()
            .set_volume_id(volume1.get_volume_id().to_owned());
        creat_vol_req.set_name(dup_vol_name.to_owned());

        let creat_resp2 = create_volume(client, &creat_vol_req)
            .context("failed to get CreateVolumeResponse when create from first volume")?;
        let volume2 = creat_resp2.get_volume();

        assert!(
            volume2.has_content_source(),
            "duplicated volume should have content source",
        );
        assert_eq!(
            volume2.get_content_source().get_volume().get_volume_id(),
            volume1.get_volume_id(),
            "the parent volume ID of duplicated volume not match the source volume ID",
        );

        // Verify volume data of duplicated volume
        verify_volume_file_content(volume2.get_volume_id(), vol_file_name, vol_file_content)
            .context(format!(
                "failed to verify the content of file name={} of volume ID={}",
                vol_file_name,
                volume2.get_volume_id(),
            ))?;

        // List to verify two volumes
        let list_vol_req = ListVolumesRequest::new();
        let list_vol_resp1 = list_volumes(client, &list_vol_req)
            .context("failed to get ListVolumesResponse of two volumes")?;

        let vols1 = list_vol_resp1.get_entries();
        let mut vol_vec1 = vols1
            .iter()
            .map(|vol| vol.get_volume().get_volume_id())
            .collect::<Vec<_>>();
        vol_vec1.sort();
        let mut expect_vol_vec1 = vec![volume1.get_volume_id(), volume2.get_volume_id()];
        expect_vol_vec1.sort();
        assert_eq!(vol_vec1, expect_vol_vec1, "list volume result not match",);
        assert_eq!(
            list_vol_resp1.get_next_token(),
            "2",
            "next volume index not match",
        );

        // Delete second volume
        let mut del_vol_req = DeleteVolumeRequest::new();
        del_vol_req.set_volume_id(volume2.get_volume_id().to_owned());
        let _del_resp1 = delete_volume(client, &del_vol_req)
            .context("failed to get DeleteVolumeResponse when delete second volume")?;

        // List the first volume only
        let list_vol_req = ListVolumesRequest::new();
        let list_vol_resp1 = list_volumes(client, &list_vol_req)
            .context("failed to get ListVolumesResponse of first volume")?;

        let vols2 = list_vol_resp1.get_entries();
        let vol_vec2 = vols2
            .iter()
            .map(|vol| vol.get_volume().get_volume_id())
            .collect::<Vec<_>>();
        let expect_vol_vec2 = vec![volume1.get_volume_id()];
        assert_eq!(vol_vec2, expect_vol_vec2, "list volume result not match",);
        assert_eq!(
            list_vol_resp1.get_next_token(),
            "1",
            "next volume index not match",
        );

        // Delete first volume
        del_vol_req.set_volume_id(volume1.get_volume_id().to_owned());
        let _del_vol_resp3 = delete_volume(client, &del_vol_req)
            .context("failed to get DeleteVolumeResponse when delete first volume")?;

        Ok(())
    }

    fn test_controller_create_volume_from_snapshot(
        client: &ControllerClient,
    ) -> anyhow::Result<()> {
        let vol_name = "tmp_volume";
        let dup_vol_name = "dup_volume";
        let vol_file_name = "volume.dat";
        let vol_file_content = "TMP_VOLUME_DATA";
        let snap_name = "tmp_snapshot";

        // Test create new volume
        let mut vc = VolumeCapability::new();
        vc.set_mount(VolumeCapability_MountVolume::new());
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);

        let mut creat_vol_req = CreateVolumeRequest::new();
        creat_vol_req.set_name(vol_name.to_owned());
        creat_vol_req.set_volume_capabilities(RepeatedField::from_vec(vec![vc]));

        let creat_resp1 = create_volume(client, &creat_vol_req)
            .context("failed to get CreateVolumeResponse when create source volume")?;
        let volume1 = creat_resp1.get_volume();

        // Write some date to volume directory
        write_file_in_volume(volume1.get_volume_id(), vol_file_name, vol_file_content)?;

        // Test create snapshot
        let mut creat_snap_req = CreateSnapshotRequest::new();
        creat_snap_req.set_source_volume_id(volume1.get_volume_id().to_owned());
        creat_snap_req.set_name(snap_name.to_owned());

        let creat_snap_resp = create_snapshot(client, &creat_snap_req)
            .context("failed to get CreateSnapshotResponse")?;

        let snapshot = creat_snap_resp.get_snapshot();
        assert_eq!(
            snapshot.get_ready_to_use(),
            true,
            "snapshot should be ready to use",
        );
        assert_eq!(
            snapshot.get_source_volume_id(),
            volume1.get_volume_id(),
            "snapshot source volume ID not match",
        );

        // Test create volume from snapshot
        creat_vol_req
            .mut_volume_content_source()
            .mut_snapshot()
            .set_snapshot_id(snapshot.get_snapshot_id().to_owned());
        creat_vol_req.set_name(dup_vol_name.to_owned());

        let creat_resp3 = create_volume(client, &creat_vol_req)
            .context("failed to get CreateVolumeResponse when create volume from snapshot")?;
        let volume3 = creat_resp3.get_volume();

        assert!(
            volume3.has_content_source(),
            "duplicated volume should have content source",
        );
        assert_eq!(
            volume3
                .get_content_source()
                .get_snapshot()
                .get_snapshot_id(),
            snapshot.get_snapshot_id(),
            "the parent volume ID of duplicated volume not match the source snapshot ID",
        );

        // Verify volume data of duplicated volume
        verify_volume_file_content(volume3.get_volume_id(), vol_file_name, vol_file_content)
            .context(format!(
                "failed to verify the content of file name={} of volume ID={}",
                vol_file_name,
                volume3.get_volume_id(),
            ))?;

        // Delete snapshot
        let mut del_snap_req = DeleteSnapshotRequest::new();
        del_snap_req.set_snapshot_id(snapshot.get_snapshot_id().to_owned());

        let _del_snap_resp1 = delete_snapshot(client, &del_snap_req)
            .context("failed to get DeleteSnapshotResponse")?;

        // Delete duplicated volume
        let mut del_vol_req = DeleteVolumeRequest::new();
        del_vol_req.set_volume_id(volume3.get_volume_id().to_owned());
        let _del_vol_resp2 = delete_volume(client, &del_vol_req)
            .context("failed to get DeleteVolumeResponse when delete twice")?;

        // Delete source volume
        del_vol_req.set_volume_id(volume1.get_volume_id().to_owned());
        let _del_vol_resp3 =
            delete_volume(client, &del_vol_req).context("failed to get DeleteVolumeResponse")?;

        Ok(())
    }

    fn test_controller_create_delete_idempotency(client: &ControllerClient) -> anyhow::Result<()> {
        let vol_name = "test_volume";
        let snap_name = "test_snapshot";

        // Test create new volume
        let mut vc = VolumeCapability::new();
        vc.set_mount(VolumeCapability_MountVolume::new());
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);
        let mut creat_vol_req = CreateVolumeRequest::new();
        creat_vol_req.set_name(vol_name.to_owned());
        creat_vol_req.set_volume_capabilities(RepeatedField::from_vec(vec![vc]));

        let creat_resp1 =
            create_volume(client, &creat_vol_req).context("failed to get CreateVolumeResponse")?;
        let volume = creat_resp1.get_volume();

        // Idempotency test for create volume
        let creat_resp2 = create_volume(client, &creat_vol_req)
            .context("failed to get CreateVolumeResponse when create twice")?;
        let same_volume = creat_resp2.get_volume();
        assert_eq!(
            volume.get_volume_id(),
            same_volume.get_volume_id(),
            "volume ID should match",
        );

        // Test create snapshot
        let mut creat_snap_req = CreateSnapshotRequest::new();
        creat_snap_req.set_source_volume_id(volume.get_volume_id().to_owned());
        creat_snap_req.set_name(snap_name.to_owned());

        let creat_snap_resp1 = create_snapshot(client, &creat_snap_req)
            .context("failed to get CreateSnapshotResponse")?;

        let snapshot1 = creat_snap_resp1.get_snapshot();
        assert_eq!(
            snapshot1.get_ready_to_use(),
            true,
            "snapshot should be ready to use",
        );
        assert_eq!(
            snapshot1.get_source_volume_id(),
            volume.get_volume_id(),
            "snapshot source volume ID not match",
        );

        // Idempotency test for create snapshot
        let creat_snap_resp2 = create_snapshot(client, &creat_snap_req)
            .context("failed to get CreateSnapshotResponse when create twice")?;

        let snapshot2 = creat_snap_resp2.get_snapshot();
        assert_eq!(
            snapshot2.get_ready_to_use(),
            true,
            "snapshot should be ready to use",
        );
        assert_eq!(
            snapshot2.get_source_volume_id(),
            volume.get_volume_id(),
            "snapshot source volume ID not match",
        );
        assert_eq!(
            snapshot1.get_snapshot_id(),
            snapshot2.get_snapshot_id(),
            "snapshot ID not match",
        );

        // Test delete snapshot
        let mut del_snap_req = DeleteSnapshotRequest::new();
        del_snap_req.set_snapshot_id(snapshot1.get_snapshot_id().to_owned());

        let _del_snap_resp1 = delete_snapshot(client, &del_snap_req)
            .context("failed to get DeleteSnapshotResponse")?;

        // Idempotency test for delete snapshot
        let _del_snap_resp2 = delete_snapshot(client, &del_snap_req)
            .context("failed to get DeleteSnapshotResponse when delete twice")?;

        // Test delete volume
        let mut del_vol_req = DeleteVolumeRequest::new();
        del_vol_req.set_volume_id(volume.get_volume_id().to_owned());
        let _del_resp1 =
            delete_volume(client, &del_vol_req).context("failed to get DeleteVolumeResponse")?;

        // Idempotency test for delete volume
        let _del_resp2 = delete_volume(client, &del_vol_req)
            .context("failed to get DeleteVolumeResponse when delete twice")?;

        Ok(())
    }

    fn test_controller_expand_volume(client: &ControllerClient) -> anyhow::Result<()> {
        let vol_name = "test_volume";

        // Test create new volume
        let mut vc = VolumeCapability::new();
        vc.set_mount(VolumeCapability_MountVolume::new());
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);
        let mut creat_vol_req = CreateVolumeRequest::new();
        creat_vol_req.set_name(vol_name.to_owned());
        creat_vol_req.set_volume_capabilities(RepeatedField::from_vec(vec![vc]));

        let creat_resp1 =
            create_volume(client, &creat_vol_req).context("failed to get CreateVolumeResponse")?;
        let volume = creat_resp1.get_volume();

        // Test expand volume
        let mut exp_req = ControllerExpandVolumeRequest::new();
        exp_req.set_volume_id(volume.get_volume_id().to_owned());
        exp_req
            .mut_capacity_range()
            .set_required_bytes(util::MAX_VOLUME_STORAGE_CAPACITY);
        let exp_resp = controller_expand_volume(client, &exp_req)
            .context("failed to get ControllerExpandVolumeResponse")?;
        assert_eq!(
            exp_resp.get_capacity_bytes(),
            util::MAX_VOLUME_STORAGE_CAPACITY,
            "volume capacity not match after expend",
        );
        assert_eq!(
            exp_resp.get_node_expansion_required(),
            true,
            "CO should call node expand volume after controller expand volume",
        );

        // Test delete volume
        let mut del_vol_req = DeleteVolumeRequest::new();
        del_vol_req.set_volume_id(volume.get_volume_id().to_owned());
        let _del_resp1 =
            delete_volume(client, &del_vol_req).context("failed to get DeleteVolumeResponse")?;

        Ok(())
    }

    fn test_controller_create_and_list_snapshot(client: &ControllerClient) -> anyhow::Result<()> {
        let vol_name = "test_volume";
        let snap_name = "test_snapshot";

        // Test create new volume
        let mut vc = VolumeCapability::new();
        vc.set_mount(VolumeCapability_MountVolume::new());
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);
        let mut creat_vol_req = CreateVolumeRequest::new();
        creat_vol_req.set_name(vol_name.to_owned());
        creat_vol_req.set_volume_capabilities(RepeatedField::from_vec(vec![vc]));

        let creat_resp1 =
            create_volume(client, &creat_vol_req).context("failed to get CreateVolumeResponse")?;
        let volume = creat_resp1.get_volume();

        // Test create snapshot
        let mut creat_snap_req = CreateSnapshotRequest::new();
        creat_snap_req.set_source_volume_id(volume.get_volume_id().to_owned());
        creat_snap_req.set_name(snap_name.to_owned());

        let creat_snap_resp1 = create_snapshot(client, &creat_snap_req)
            .context("failed to get CreateSnapshotResponse")?;

        let snapshot1 = creat_snap_resp1.get_snapshot();
        assert_eq!(
            snapshot1.get_ready_to_use(),
            true,
            "snapshot should be ready to use",
        );
        assert_eq!(
            snapshot1.get_source_volume_id(),
            volume.get_volume_id(),
            "snapshot source volume ID not match",
        );

        // Test for create snapshot failure with name match but src volume ID not match
        creat_snap_req.set_source_volume_id("some_illegle_volume_id".to_owned());
        let creat_snap_resp3 = create_snapshot(client, &creat_snap_req);
        assert!(
            creat_snap_resp3.is_err(),
            "create snapshot should fail when name match but src volume ID not match",
        );

        // Test list snapshot
        let mut list_snap_req = ListSnapshotsRequest::new();
        let list_snap_resp1 = list_snapshots(client, &list_snap_req)
            .context("failed to get ListSnapshotsResponse")?;

        let snaps1 = list_snap_resp1.get_entries();
        let snap_vec1 = snaps1
            .iter()
            .map(|snap| snap.get_snapshot().get_snapshot_id())
            .collect::<Vec<_>>();
        assert_eq!(
            snap_vec1,
            vec![snapshot1.get_snapshot_id()],
            "list snapshot result not match",
        );
        assert_eq!(
            list_snap_resp1.get_next_token(),
            "1",
            "next snapshot index not match",
        );

        // Test list snapshot by src volume ID
        list_snap_req.set_source_volume_id(volume.get_volume_id().to_owned());
        let list_snap_resp2 = list_snapshots(client, &list_snap_req)
            .context("failed to get ListSnapshotsResponse")?;

        let snaps2 = list_snap_resp2.get_entries();
        let snap_vec2 = snaps2
            .iter()
            .map(|snap| snap.get_snapshot().get_snapshot_id())
            .collect::<Vec<_>>();
        assert_eq!(
            snap_vec2,
            vec![snapshot1.get_snapshot_id()],
            "list snapshot result not match",
        );

        // Test list snapshot by snapshot ID
        list_snap_req.clear_source_volume_id();
        list_snap_req.set_snapshot_id(snapshot1.get_snapshot_id().to_owned());
        let list_snap_resp3 = list_snapshots(client, &list_snap_req)
            .context("failed to get ListSnapshotsResponse")?;

        let snaps3 = list_snap_resp3.get_entries();
        let snap_vec3 = snaps3
            .iter()
            .map(|snap| snap.get_snapshot().get_snapshot_id())
            .collect::<Vec<_>>();
        assert_eq!(
            snap_vec3,
            vec![snapshot1.get_snapshot_id()],
            "list snapshot result not match",
        );

        // Test delete snapshot
        let mut del_snap_req = DeleteSnapshotRequest::new();
        del_snap_req.set_snapshot_id(snapshot1.get_snapshot_id().to_owned());

        let _del_snap_resp1 = delete_snapshot(client, &del_snap_req)
            .context("failed to get DeleteSnapshotResponse")?;

        // Test delete volume
        let mut del_vol_req = DeleteVolumeRequest::new();
        del_vol_req.set_volume_id(volume.get_volume_id().to_owned());
        let _del_resp1 =
            delete_volume(client, &del_vol_req).context("failed to get DeleteVolumeResponse")?;

        Ok(())
    }

    fn build_node_client() -> anyhow::Result<NodeClient> {
        run_server()?;
        let env = Arc::new(EnvBuilder::new().build());
        let ch = ChannelBuilder::new(env).connect(util::END_POINT);
        let node_client = NodeClient::new(ch);
        Ok(node_client)
    }

    fn test_node_server() -> anyhow::Result<()> {
        let node_client = build_node_client()?;

        test_node_server_publish_unpublish(&node_client)
            .context("failed to test node publish unpublish")?;
        test_node_server_remount_publish(&node_client).context("failed to test node remount")?;
        test_node_server_multiple_publish(&node_client)
            .context("failed to test node multi-mount")?;
        Ok(())
    }

    fn test_node_server_publish_unpublish(client: &NodeClient) -> anyhow::Result<()> {
        // Test node get capabilities
        let cap_req = NodeGetCapabilitiesRequest::new();
        let cap_resp = client
            .node_get_capabilities(&cap_req)
            .context("failed to get NodeGetCapabilitiesResponse")?;
        let caps = cap_resp.get_capabilities();
        let cap_vec = caps
            .iter()
            .map(|cap| cap.get_rpc().get_field_type())
            .collect::<Vec<_>>();
        assert_eq!(
            cap_vec,
            vec![NodeServiceCapability_RPC_Type::EXPAND_VOLUME,],
            "node_get_capabilities returns unexpected capabilities",
        );

        // Test node get info
        let info_req = NodeGetInfoRequest::new();
        let info_resp = client
            .node_get_info(&info_req)
            .context("failed to get NodeGetInfoResponse")?;
        assert_eq!(
            info_resp.get_node_id(),
            util::DEFAULT_NODE_NAME,
            "node name not match",
        );
        assert_eq!(
            info_resp.get_max_volumes_per_node(),
            util::MAX_VOLUMES_PER_NODE,
            "max volumes per node not match",
        );
        let topology = info_resp.get_accessible_topology();
        assert_eq!(
            topology.get_segments().get(util::TOPOLOGY_KEY_NODE),
            Some(&util::DEFAULT_NODE_NAME.to_owned()), // Expect &String not &str
            "topology not match",
        );

        // Test publish volume
        let target_path = NODE_PUBLISH_VOLUME_TARGET_PATH;
        let vol_id = NODE_PUBLISH_VOLUME_ID;
        let mut mount_option = VolumeCapability_MountVolume::new();
        mount_option.set_fs_type("fuse".to_owned());
        mount_option.set_mount_flags(protobuf::RepeatedField::from_vec(vec![
            "nosuid".to_owned(),
            "nodev".to_owned(),
        ]));
        let mut vc = VolumeCapability::new();
        vc.set_mount(mount_option);
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);
        let mut pub_req = NodePublishVolumeRequest::new();
        pub_req.set_volume_id(vol_id.to_owned());
        pub_req.set_volume_capability(vc);
        pub_req.set_target_path(target_path.to_owned());
        pub_req.set_readonly(false);
        pub_req
            .mut_volume_context()
            .insert(util::EPHEMERAL_KEY_CONTEXT.to_owned(), "true".to_owned());

        let _pub_resp1 = client
            .node_publish_volume(&pub_req)
            .context("failed to get NodePublishVolumeResponse")?;

        // Test expand volume
        let mut exp_req = NodeExpandVolumeRequest::new();
        exp_req.set_volume_id(vol_id.to_owned());
        exp_req.set_volume_path(target_path.to_owned());
        exp_req
            .mut_capacity_range()
            .set_required_bytes(util::MAX_VOLUME_STORAGE_CAPACITY);
        let exp_resp = client
            .node_expand_volume(&exp_req)
            .context("failed to get NodeExpandVolumeResponse")?;
        assert_eq!(
            exp_resp.get_capacity_bytes(),
            util::MAX_VOLUME_STORAGE_CAPACITY,
            "volume capacity not match after expend",
        );

        // Idempotency test for publish volume
        let _pub_resp2 = client
            .node_publish_volume(&pub_req)
            .context("failed to get NodePublishVolumeResponse")?;

        // Test unpublish volume
        let mut unpub_req = NodeUnpublishVolumeRequest::new();
        unpub_req.set_volume_id(vol_id.to_owned());
        unpub_req.set_target_path(target_path.to_owned());

        let _unpub_resp1 = client
            .node_unpublish_volume(&unpub_req)
            .context("failed to get NodeUnpublishVolumeResponse")?;

        Ok(())
    }

    fn test_node_server_remount_publish(client: &NodeClient) -> anyhow::Result<()> {
        // First publish volume
        let target_path = NODE_PUBLISH_VOLUME_TARGET_PATH;
        let vol_id = NODE_PUBLISH_VOLUME_ID;
        let mut mount_option = VolumeCapability_MountVolume::new();
        mount_option.set_fs_type("fuse".to_owned());
        mount_option.set_mount_flags(protobuf::RepeatedField::from_vec(vec![
            "nosuid".to_owned(),
            "nodev".to_owned(),
        ]));
        let mut vc = VolumeCapability::new();
        vc.set_mount(mount_option);
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);
        let mut pub_req = NodePublishVolumeRequest::new();
        pub_req.set_volume_id(vol_id.to_owned());
        pub_req.set_volume_capability(vc);
        pub_req.set_target_path(target_path.to_owned());
        pub_req.set_readonly(false);
        pub_req
            .mut_volume_context()
            .insert(util::EPHEMERAL_KEY_CONTEXT.to_owned(), "true".to_owned());

        let _pub_resp1 = client
            .node_publish_volume(&pub_req)
            .context("failed to get first NodePublishVolumeResponse when test remount")?;

        // Second publish volume
        pub_req.set_readonly(true);

        let _pub_resp2 = client
            .node_publish_volume(&pub_req)
            .context("failed to get second NodePublishVolumeResponse when test remount")?;

        // Test unpublish volume
        let mut unpub_req = NodeUnpublishVolumeRequest::new();
        unpub_req.set_volume_id(vol_id.to_owned());
        unpub_req.set_target_path(target_path.to_owned());

        let _unpub_resp = client
            .node_unpublish_volume(&unpub_req)
            .context("failed to get NodeUnpublishVolumeResponse")?;

        // Verify second unpublish volume result should fail
        let failed_unpub_resp1 = client.node_unpublish_volume(&unpub_req);
        assert!(failed_unpub_resp1.is_err(), "unpublish again should fail");

        Ok(())
    }

    fn test_node_server_multiple_publish(client: &NodeClient) -> anyhow::Result<()> {
        // First publish volume
        let target_path1 = NODE_PUBLISH_VOLUME_TARGET_PATH_1;
        let vol_id = NODE_PUBLISH_VOLUME_ID;
        let mut mount_option = VolumeCapability_MountVolume::new();
        mount_option.set_fs_type("fuse".to_owned());
        mount_option.set_mount_flags(protobuf::RepeatedField::from_vec(vec![
            "nosuid".to_owned(),
            "nodev".to_owned(),
        ]));
        let mut vc = VolumeCapability::new();
        vc.set_mount(mount_option);
        vc.mut_access_mode()
            .set_mode(VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER);
        let mut pub_req = NodePublishVolumeRequest::new();
        pub_req.set_volume_id(vol_id.to_owned());
        pub_req.set_volume_capability(vc);
        pub_req.set_target_path(target_path1.to_owned());
        pub_req.set_readonly(false);
        pub_req
            .mut_volume_context()
            .insert(util::EPHEMERAL_KEY_CONTEXT.to_owned(), "true".to_owned());

        let _pub_resp1 = client
            .node_publish_volume(&pub_req)
            .context("failed to get first NodePublishVolumeResponse")?;

        // Second publish volume
        let target_path2 = NODE_PUBLISH_VOLUME_TARGET_PATH_2;
        pub_req.set_target_path(target_path2.to_owned());
        let _pub_resp2 = client
            .node_publish_volume(&pub_req)
            .context("failed to get second NodePublishVolumeResponse")?;

        // First unpublish volume
        let mut unpub_req = NodeUnpublishVolumeRequest::new();
        unpub_req.set_volume_id(vol_id.to_owned());
        unpub_req.set_target_path(target_path1.to_owned());

        let _unpub_resp1 = client
            .node_unpublish_volume(&unpub_req)
            .context("failed to get first NodeUnpublishVolumeResponse")?;

        // Second unpublish volume
        unpub_req.set_target_path(target_path2.to_owned());
        let _unpub_resp2 = client
            .node_unpublish_volume(&unpub_req)
            .context("failed to get first NodeUnpublishVolumeResponse")?;
        Ok(())
    }
}
