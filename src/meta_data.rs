//! The utilities of meta data management

use anyhow::{anyhow, Context};
// use etcd_rs::*;
use futures::prelude::*;
use grpcio::*;
use log::{debug, error, info, warn};
use nix::mount::{self, MntFlags, MsFlags};
use nix::unistd;
use protobuf::RepeatedField;
use rand::Rng;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::convert::From;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use walkdir::WalkDir;

use super::cast::Cast;
use super::csi::*;
use super::datenlord_worker_grpc::*;
use util::RunAsRole;

/// The path to bind mount helper command
const BIND_MOUNTER: &str = "target/debug/bind_mounter";

/// Utility functions and const variables
pub mod util {
    use super::super::cast::Cast;
    use super::*;

    /// The CSI plugin name
    pub const CSI_PLUGIN_NAME: &str = "io.datenlord.csi.plugin";
    // TODO: should use DatenLord version instead
    /// The CSI plugin version
    pub const CSI_PLUGIN_VERSION: &str = "0.1.0";
    /// Directory where data for volumes and snapshots are persisted.
    /// This can be ephemeral within the container or persisted if
    /// backed by a Pod volume.
    pub const DATA_DIR: &str = "/tmp/csi-data-dir";
    /// The socket file to communicate with K8S CSI sidecars
    pub const END_POINT: &str = "unix:///tmp/csi.sock";
    /// The port of worker node
    pub const DEFAULT_PORT: u16 = 50051;
    /// Max storage capacity per volume,
    /// Default 20GB required by csi-sanity check
    pub const MAX_VOLUME_STORAGE_CAPACITY: i64 = 20 * 1024 * 1024 * 1024;
    /// Default ephemeral volume storage capacity
    pub const EPHEMERAL_VOLUME_STORAGE_CAPACITY: i64 = MAX_VOLUME_STORAGE_CAPACITY / 10;
    /// Extension with which snapshot files will be saved.
    pub const SNAPSHOT_EXT: &str = ".snap";
    /// The key to the topology hashmap
    pub const TOPOLOGY_KEY_NODE: &str = "topology.csi.datenlord.io/node";
    /// The key of ephemeral in volume context
    pub const EPHEMERAL_KEY_CONTEXT: &str = "csi.storage.k8s.io/ephemeral";
    /// Default max volume per node, should read from input argument
    pub const MAX_VOLUMES_PER_NODE: i64 = 256;
    /// Default node name, should read from input argument
    pub const DEFAULT_NODE_NAME: &str = "localhost"; // TODO: to remove
    /// The socket file to be binded by worker service
    pub const LOCAL_WORKER_SOCKET: &str = "unix:///tmp/worker.sock";

    /// The runtime role of CSI plugin
    #[derive(Clone, Copy, Debug)]
    pub enum RunAsRole {
        /// Run both controller and node service
        Both,
        /// Run controller service only
        Controller,
        /// Run node service only
        Node,
    }

    /// Convert `SystemTime` to proto timestamp
    pub fn generate_proto_timestamp(
        st: &std::time::SystemTime,
    ) -> anyhow::Result<protobuf::well_known_types::Timestamp> {
        let d = st
            .duration_since(std::time::UNIX_EPOCH)
            .context("failed to get duration since unix epoch")?;
        let mut ts = protobuf::well_known_types::Timestamp::new();
        ts.set_seconds(d.as_secs().cast());
        ts.set_nanos(d.subsec_nanos().cast());

        Ok(ts)
    }

    // /// Convert proto timestamp to `SystemTime`
    // pub fn generate_system_time(
    //     proto_ts: &protobuf::well_known_types::Timestamp,
    // ) -> anyhow::Result<std::time::SystemTime> {
    //     let drtn = std::time::Duration::new(proto_ts.seconds.cast(), proto_ts.nanos.cast());
    //     let add_res = std::time::UNIX_EPOCH.checked_add(drtn);
    //     if let Some(ts) = add_res {
    //         Ok(ts)
    //     } else {
    //         Err(anyhow::anyhow!(
    //             "failed to convert proto timestamp to SystemTime"
    //         ))
    //     }
    // }

    /// Build list snapshot response
    pub fn add_snapshot_to_list_response(
        snap: &DatenLordSnapshot,
    ) -> anyhow::Result<ListSnapshotsResponse> {
        let mut entry = ListSnapshotsResponse_Entry::new();
        entry.mut_snapshot().set_size_bytes(snap.size_bytes);
        entry.mut_snapshot().set_snapshot_id(snap.snap_id.clone());
        entry
            .mut_snapshot()
            .set_source_volume_id(snap.vol_id.clone());
        entry.mut_snapshot().set_creation_time(
            generate_proto_timestamp(&snap.creation_time)
                .context("failed to convert to proto timestamp")?,
        );
        entry.mut_snapshot().set_ready_to_use(snap.ready_to_use);

        let mut resp = ListSnapshotsResponse::new();
        resp.set_entries(RepeatedField::from_vec(vec![entry]));
        Ok(resp)
    }

    /// Copy a directory recursively
    pub fn copy_directory_recursively(
        from: impl AsRef<Path>,
        to: impl AsRef<Path>,
        follow_symlink: bool,
    ) -> anyhow::Result<usize> {
        let from_path = from.as_ref();
        let to_path = to.as_ref();
        let mut num_copied: usize = 0;
        for entry in WalkDir::new(from_path).follow_links(follow_symlink) {
            let entry = entry?;
            let entry_path = entry.path();
            let stripped_path = entry_path.strip_prefix(from_path)?;
            let target_path = to_path.join(stripped_path);
            if entry_path.is_dir() {
                if !target_path.exists() {
                    fs::create_dir(&target_path)?;
                }
            } else if entry_path.is_file() {
                fs::copy(entry_path, &target_path)?;
                let add_res = num_copied.overflowing_add(1);
                debug_assert!(!add_res.1, "num_copied={} add 1 overflowed", num_copied);
                num_copied = add_res.0;
            } else {
                info!("skip non-file and non-dir path: {}", entry_path.display());
            }
        }

        Ok(num_copied)
    }

    /// Build `CreateVolumeResponse`
    pub fn build_create_volume_response(
        req: &CreateVolumeRequest,
        vol_id: &str,
        node_id: &str,
    ) -> CreateVolumeResponse {
        let mut topology = Topology::new();
        topology
            .mut_segments()
            .insert(TOPOLOGY_KEY_NODE.to_owned(), node_id.to_owned());
        let mut v = Volume::new();
        v.set_volume_id(vol_id.to_owned());
        v.set_capacity_bytes(req.get_capacity_range().get_required_bytes());
        v.set_volume_context(req.get_parameters().clone());
        v.set_content_source(req.get_volume_content_source().clone());
        v.set_accessible_topology(RepeatedField::from_vec(vec![topology]));
        let mut r = CreateVolumeResponse::new();
        r.set_volume(v);
        r
    }

    /// Build `CreateSnapshotResponse`
    pub fn build_create_snapshot_response(
        req: &CreateSnapshotRequest,
        snap_id: &str,
        ts: &std::time::SystemTime,
        size_bytes: i64,
    ) -> anyhow::Result<CreateSnapshotResponse> {
        let ts_res = generate_proto_timestamp(ts);
        let proto_ts_now = match ts_res {
            Ok(ts) => ts,
            Err(e) => {
                return Err(anyhow::anyhow!(format!(
                    "failed to generate proto timestamp \
                            when creating snapshot from volume ID={}, \
                            the error is: {}",
                    req.get_source_volume_id(),
                    e,
                )));
            }
        };
        let mut s = Snapshot::new();
        s.set_snapshot_id(snap_id.to_owned());
        s.set_source_volume_id(req.get_source_volume_id().to_owned());
        s.set_creation_time(proto_ts_now);
        s.set_size_bytes(size_bytes);
        s.set_ready_to_use(true);
        let mut r = CreateSnapshotResponse::new();
        r.set_snapshot(s);
        Ok(r)
    }

    /// Send success `gRPC` response
    pub fn success<R>(ctx: &RpcContext, sink: UnarySink<R>, r: R) {
        let f = sink
            .success(r)
            .map_err(move |e| error!("failed to send response, the error is: {:?}", e))
            .map(|_| ());
        ctx.spawn(f)
    }

    /// Send failure `gRPC` response
    pub fn fail<R>(ctx: &RpcContext, sink: UnarySink<R>, rsc: RpcStatusCode, msg: String) {
        let details = if msg.is_empty() { None } else { Some(msg) };
        let rs = RpcStatus::new(rsc, details);
        let f = sink
            .fail(rs)
            .map_err(move |e| error!("failed to send response, the error is: {:?}", e))
            .map(|_| ());
        ctx.spawn(f)
    }

    /// Decode from bytes
    pub fn decode_from_bytes<T: DeserializeOwned>(bytes: &[u8]) -> anyhow::Result<T> {
        let decoded_value = bincode::deserialize(bytes).map_err(|e| {
            anyhow::anyhow!(
                "failed to decode bytes to {}, the error is: {}",
                std::any::type_name::<T>(),
                e,
            )
        })?;
        Ok(decoded_value)
    }

    /// Un-mount target path, if fail try force un-mount again
    pub fn umount_volume_bind_path(target_dir: &str) -> anyhow::Result<()> {
        if unistd::geteuid().is_root() {
            let umount_res = mount::umount(Path::new(target_dir));
            if let Err(umount_e) = umount_res {
                let umount_force_res = mount::umount2(Path::new(target_dir), MntFlags::MNT_FORCE);
                if let Err(umount_force_e) = umount_force_res {
                    return Err(anyhow::anyhow!(
                        "failed to un-mount the target path={:?}, \
                            the un-mount error is: {:?} and the force un-mount error is: {}",
                        Path::new(target_dir),
                        umount_e,
                        umount_force_e,
                    ));
                }
            }
        } else {
            let umount_handle = Command::new(BIND_MOUNTER)
                .arg("-u")
                .arg(&target_dir)
                .output()
                .context("bind_mounter command failed to start")?;
            if !umount_handle.status.success() {
                let stderr = String::from_utf8_lossy(&umount_handle.stderr);
                debug!("bind_mounter failed to umount, the error is: {}", &stderr);
                return Err(anyhow!(
                    "bind_mounter failed to umount {:?}, the error is: {}",
                    Path::new(target_dir),
                    stderr,
                ));
            }
        }
        // csi-sanity requires plugin to remove the target mount directory
        fs::remove_dir_all(target_dir)
            .context(format!("failed to remove mount target path={}", target_dir))?;

        Ok(())
    }
}

/// `DatenLord` node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatenLordNode {
    /// Node ID
    pub node_id: String,
    /// The port of worker service
    pub worker_port: u16,
    /// Node available space for storage
    pub max_available_space_bytes: i64,
    // /// Node IP
    // pub ip_address: String, // TODO: add node IP address
}

impl DatenLordNode {
    /// Create `DatenLordNode`
    const fn new(node_id: String, worker_port: u16, max_available_space_bytes: i64) -> Self {
        Self {
            node_id,
            worker_port,
            max_available_space_bytes,
        }
    }
}

/// The data structure to store volume and snapshot meta data
pub struct MetaData {
    /// Volume and snapshot data directory
    data_dir: String,
    /// The plugin will save data persistently or not
    ephemeral: bool,
    /// Max volumes per node
    max_volumes_per_node: i64,
    /// The meta data service is for controller or node
    run_as: RunAsRole,
    /// The list of etcd address and port
    etcd_client: etcd_rs::Client,
    /// The meta data about this node
    node: DatenLordNode,
    // /// All volumes by ID
    // volume_meta_data: RwLock<HashMap<String, Arc<DatenLordVolume>>>,
    // /// All snapshots by ID
    // snapshot_meta_data: RwLock<HashMap<String, Arc<DatenLordSnapshot>>>,
}

/// The etcd key prefix to controller ID
const CONTROLLER_PREFIX: &str = "controller";
/// The etcd key prefix to node ID
const NODE_PREFIX: &str = "node";
/// The etcd key prefix to node ID and snapshot ID
const NODE_SNAPSHOT_PREFIX: &str = "node_snapshot_id";
/// The etcd key prefix to node ID and volume ID
const NODE_VOLUME_PREFIX: &str = "node_volume_id";
/// The etcd key prefix to snapshot ID
const SNAPSHOT_ID_PREFIX: &str = "snapshot_id";
/// The etcd key prefix to snapshot name
const SNAPSHOT_NAME_PREFIX: &str = "snapshot_name";
/// The etcd key prefix to snapshot ID and source volume ID
const SNAPSHOT_SOURCE_ID_PREFIX: &str = "snapshot_source_id";
/// The etcd key prefix to volume ID
const VOLUME_ID_PREFIX: &str = "volume_id";
/// The etcd key prefix to volume name
const VOLUME_NAME_PREFIX: &str = "volume_name";
/// The etcd key prefix to volume bind mount path
const VOLUME_BIND_MOUNT_PATH_PREFIX: &str = "volume_bind_mount_path";

impl MetaData {
    /// Create `MetaData`
    pub fn new(
        node_id: String,
        data_dir: String,
        worker_port: u16,
        ephemeral: bool,
        max_volumes_per_node: i64,
        max_available_space_bytes: i64,
        run_as: RunAsRole,
        etcd_client: etcd_rs::Client,
    ) -> anyhow::Result<Self> {
        let node = DatenLordNode::new(node_id, worker_port, max_available_space_bytes);
        let md = Self {
            data_dir,
            ephemeral,
            max_volumes_per_node,
            run_as,
            etcd_client,
            node,
            // volume_meta_data: RwLock::new(HashMap::new()),
            // snapshot_meta_data: RwLock::new(HashMap::new()),
        };
        match md.run_as {
            RunAsRole::Both => {
                md.register_to_etcd(CONTROLLER_PREFIX)?;
                md.register_to_etcd(NODE_PREFIX)?;
            }
            RunAsRole::Controller => md.register_to_etcd(CONTROLLER_PREFIX)?,
            RunAsRole::Node => md.register_to_etcd(NODE_PREFIX)?,
        }

        Ok(md)
    }

    /// Register this worker to etcd
    fn register_to_etcd(&self, prefix: &str) -> anyhow::Result<()> {
        let key = format!("{}/{}", prefix, self.get_node_id());
        let pre_value = smol::run(async { self.write_to_etcd(&key, &self.node).await })?;
        debug_assert!(
            pre_value.is_none(),
            "duplicated node registration in etcd, the node ID={}",
            self.get_node_id(),
        );
        Ok(())
    }

    /// Build `gRPC` client to `DatenLord` worker
    pub fn build_worker_client(&self, node_id: &str) -> WorkerClient {
        // let env = Arc::new(EnvBuilder::new().build());
        let env = Arc::new(Environment::new(1));
        let ch =
            ChannelBuilder::new(env).connect(&format!("{}:{}", node_id, self.get_worker_port()));
        let client = WorkerClient::new(ch);
        debug!(
            "build worker client to {}:{}",
            node_id,
            self.get_worker_port(),
        );
        client
    }

    /// Select a node to create volume or snapshot
    pub fn select_node(&self, tplg: Option<&TopologyRequirement>) -> anyhow::Result<DatenLordNode> {
        // TODO: select node by topology requirment, available space, etc
        if let Some(topology) = tplg {
            debug!(
                "required topologies: {:?} and preferred topologies: {:?}",
                topology.get_requisite(),
                topology.get_preferred(),
            );
        }
        // List key-value pairs with prefix
        let node_list: Vec<DatenLordNode> =
            smol::run(async { self.get_list_from_etcd(&format!("{}/", NODE_PREFIX)).await })?;
        debug_assert!(
            !node_list.is_empty(),
            "failed to retrieve node list from etcd"
        );
        let mut rng = rand::thread_rng();

        // Random select a node
        // TODO: select node based on node available space,
        // volume space requirment, and topology requirment
        let idx = rng.gen_range(0, node_list.len());
        if let Some(node) = node_list.get(idx) {
            Ok(node.clone())
        } else {
            panic!(
                "failed to get the {}-th node from returned node list, list={:?}",
                idx, node_list,
            );
        }
    }

    /// Get volume absolute path by ID
    pub fn get_volume_path(&self, vol_id: &str) -> PathBuf {
        Path::new(&self.data_dir).join(vol_id)
    }

    /// Get snapshot path by ID
    pub fn get_snapshot_path(&self, snap_id: &str) -> PathBuf {
        let mut str_snap_id = snap_id.to_owned();
        str_snap_id.push_str(util::SNAPSHOT_EXT);
        Path::new(&self.data_dir).join(str_snap_id)
    }

    /// Get the node ID
    pub fn get_node_id(&self) -> &str {
        &self.node.node_id
    }

    /// Get the node worker service port
    pub const fn get_worker_port(&self) -> u16 {
        self.node.worker_port
    }

    /// Is volume data ephemeral or not
    pub const fn is_ephemeral(&self) -> bool {
        self.ephemeral
    }

    /// Get max volumes per node
    pub const fn get_max_volumes_per_node(&self) -> i64 {
        self.max_volumes_per_node
    }

    /// Get zero or one key-value pair from etcd
    async fn get_at_most_one_value_from_etcd<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> anyhow::Result<T> {
        let mut value_list = self.get_list_from_etcd(key).await?;
        debug_assert!(
            value_list.len() <= 1,
            "failed to get zero or one key={} from etcd, but get {} values",
            key,
            value_list.len(),
        );
        value_list
            .pop()
            .ok_or_else(|| anyhow::anyhow!("failed to get one key-value pair where key={}", key))
    }

    /// Get key-value list from etcd
    async fn get_list_from_etcd<T: DeserializeOwned>(
        &self,
        prefix: &str,
    ) -> anyhow::Result<Vec<T>> {
        let req = etcd_rs::RangeRequest::new(etcd_rs::KeyRange::prefix(prefix));
        let mut resp = self.etcd_client.kv().range(req).await.map_err(|e| {
            anyhow::anyhow!("failed to get RangeResponse from etcd, the error is: {}", e)
        })?;
        let mut result_vec = Vec::with_capacity(resp.count());
        for kv in resp.take_kvs() {
            let decoded_value: T = util::decode_from_bytes(kv.value())?;
            result_vec.push(decoded_value);
        }
        Ok(result_vec)
    }

    /// Update a existing key value pair to etcd
    async fn update_to_etcd<T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
        value: &T,
    ) -> anyhow::Result<T> {
        let write_res = self.write_to_etcd(key, value).await?;
        if let Some(pre_value) = write_res {
            Ok(pre_value)
        } else {
            panic!("failed to replace previous value, return nothing",);
        }
    }

    /// Write a key value pair to etcd
    async fn write_to_etcd<T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
        value: &T,
    ) -> anyhow::Result<Option<T>> {
        let bin_value = bincode::serialize(value).map_err(|e| {
            anyhow::anyhow!(
                "failed to encode {:?} to binary, the error is: {}",
                value,
                e,
            )
        })?;
        let mut req = etcd_rs::PutRequest::new(key, bin_value);
        req.set_prev_kv(true); // Return previous value
        let mut resp = self.etcd_client.kv().put(req).await.map_err(|e| {
            anyhow::anyhow!("failed to get PutResponse from etcd, the error is: {}", e)
        })?;
        if let Some(pre_kv) = resp.take_prev_kv() {
            let decoded_value: T = util::decode_from_bytes(pre_kv.value())?;
            Ok(Some(decoded_value))
        } else {
            Ok(None)
        }
    }

    /// Delete an existing key value pair from etcd
    async fn delete_one_value_from_etcd<T: DeserializeOwned + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
    ) -> anyhow::Result<T> {
        let delete_res = self.delete_from_etcd(key).await?;
        if let Some(pre_value) = delete_res {
            Ok(pre_value)
        } else {
            panic!("failed to delete exactly one value from etced, delete nothing")
        }
    }

    /// Delete a key value pair or nothing from etcd
    async fn delete_from_etcd<T: DeserializeOwned + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
    ) -> anyhow::Result<Option<T>> {
        let mut req = etcd_rs::DeleteRequest::new(etcd_rs::KeyRange::key(key));
        req.set_prev_kv(true);
        let mut resp = self.etcd_client.kv().delete(req).await.map_err(|e| {
            anyhow::anyhow!(
                "failed to get DeleteResponse from etcd, the error is: {}",
                e,
            )
        })?;

        if resp.has_prev_kvs() {
            let deleted_value_list = resp.take_prev_kvs();
            debug_assert_eq!(
                deleted_value_list.len(),
                1,
                "delete {} key value pairs for a single key, impossible case",
                deleted_value_list.len(),
            );
            let deleted_kv = if let Some(kv) = deleted_value_list.get(0) {
                kv
            } else {
                panic!("failed to get the exactly one deleted key value pair")
            };
            let decoded_value: T = util::decode_from_bytes(deleted_kv.value())?;
            Ok(Some(decoded_value))
        } else {
            Ok(None)
        }
    }

    /// Get snapshot by ID
    pub fn get_snapshot_by_id(&self, snap_id: &str) -> anyhow::Result<DatenLordSnapshot> {
        // match self.snapshot_meta_data.read().unwrap().get(snap_id) {
        //     Some(snap) => Some(Arc::<DatenLordSnapshot>::clone(snap)),
        //     None => None,
        // }
        smol::run(async move {
            self.get_at_most_one_value_from_etcd(&format!("{}/{}", SNAPSHOT_ID_PREFIX, snap_id))
                .await
        })
    }

    /// Find snapshot by name
    pub fn get_snapshot_by_name(&self, snap_name: &str) -> anyhow::Result<DatenLordSnapshot> {
        // match self
        //     .snapshot_meta_data
        //     .read()
        //     .unwrap()
        //     .iter()
        //     .find(|(_key, snapshot)| snapshot.snap_name == snap_name)
        // {
        //     Some(pair) => Some(Arc::<DatenLordSnapshot>::clone(pair.1)),
        //     None => None,
        // }
        let snap_name_key = format!("{}/{}", SNAPSHOT_NAME_PREFIX, snap_name);
        let snap_id: String =
            smol::run(async move { self.get_at_most_one_value_from_etcd(&snap_name_key).await })?;
        debug!(
            "get_snapshot_by_name found snap ID={} by name={}",
            snap_id, snap_name,
        );
        self.get_snapshot_by_id(&snap_id)
    }

    /// Find snapshot by source volume ID, each source volume ID has one snapshot at most
    pub fn get_snapshot_by_src_volume_id(
        &self,
        src_volume_id: &str,
    ) -> anyhow::Result<DatenLordSnapshot> {
        // match self
        //     .snapshot_meta_data
        //     .read()
        //     .unwrap()
        //     .iter()
        //     .find(|(_key, val)| val.vol_id == src_volume_id)
        // {
        //     Some(pair) => Some(Arc::<DatenLordSnapshot>::clone(pair.1)),
        //     None => None,
        // }
        let src_vol_id_key = format!("{}/{}", SNAPSHOT_SOURCE_ID_PREFIX, src_volume_id);
        let snap_id: String =
            smol::run(async move { self.get_at_most_one_value_from_etcd(&src_vol_id_key).await })?;
        debug!(
            "get_snapshot_by_src_volume_id found snap ID={} by source volume ID={}",
            snap_id, src_volume_id,
        );
        self.get_snapshot_by_id(&snap_id)
    }

    // /// Get the number of snapshots
    // pub fn get_num_of_volumes(&self) -> usize {
    //     // self.volume_meta_data.read().unwrap().len()
    //     let list_res: anyhow::Result<Vec<DatenLordVolume>> =
    //         smol::run(async move { self.get_list_from_etcd(&format!("{}/", VOLUME_ID_PREFIX)).await });
    //     match list_res {
    //         Ok(v) => v.len(),
    //         Err(e) => panic!("failed to get volumes, the error is: {}", e),
    //     }
    // }

    // /// Get the number of snapshots
    // pub fn get_num_of_snapshots(&self) -> usize {
    //     // self.snapshot_meta_data.read().unwrap().len()
    //     let list_res: anyhow::Result<Vec<DatenLordSnapshot>> =
    //         smol::run(async move { self.get_list_from_etcd(&format!("{}/", SNAPSHOT_ID_PREFIX)).await });
    //     match list_res {
    //         Ok(v) => v.len(),
    //         Err(e) => panic!("failed to get volumes, the error is: {}", e),
    //     }
    // }

    /// The helper function to list elements
    fn list_helper<E, T, F>(
        collection: impl Into<Vec<E>>,
        starting_token: &str,
        max_entries: i32,
        f: F,
    ) -> (RpcStatusCode, String, Vec<T>, usize)
    where
        F: Fn(&E) -> Option<T>,
    {
        let vector = collection.into();
        let total_num = vector.len();
        let starting_pos = if starting_token.is_empty() {
            0
        } else if let Ok(i) = starting_token.parse::<usize>() {
            i
        } else {
            return (
                RpcStatusCode::ABORTED,
                format!("invalid starting position {}", starting_token),
                Vec::new(), // Empty result list
                0,          // next_pos
            );
        };
        if starting_pos > 0 && starting_pos >= total_num {
            return (
                RpcStatusCode::ABORTED,
                format!(
                    "invalid starting token={}, larger than or equal to the list size={} of volumes",
                    starting_token,
                    total_num,
                ),
                Vec::new(), // Empty result list
                0, // next_pos
            );
        }
        let (remaining, ofr) = total_num.overflowing_sub(starting_pos);
        debug_assert!(
            !ofr,
            "total_num={} subtract num_to_list={} overflowed",
            total_num, starting_pos,
        );

        let num_to_list = if max_entries > 0 {
            if remaining < max_entries.cast() {
                remaining
            } else {
                max_entries.cast()
            }
        } else {
            remaining
        };
        let (next_pos, ofnp) = starting_pos.overflowing_add(num_to_list);
        debug_assert!(
            !ofnp,
            "sarting_pos={} add num_to_list={} overflowed",
            starting_pos, num_to_list,
        );

        let result_vec = vector
            .iter()
            .enumerate()
            .filter_map(|(idx, elem)| {
                if idx < starting_pos {
                    return None;
                }
                let (end_idx_not_list, ofen) = starting_pos.overflowing_add(num_to_list);
                debug_assert_eq!(
                    ofen, false,
                    "starting_pos={} add num_to_list={} overflowed",
                    starting_pos, num_to_list,
                );
                if idx >= end_idx_not_list {
                    return None;
                }
                f(elem)
            })
            .collect::<Vec<_>>();
        (RpcStatusCode::OK, "".to_owned(), result_vec, next_pos)
    }

    /// List volumes
    pub fn list_volumes(
        &self,
        starting_token: &str,
        max_entries: i32,
    ) -> anyhow::Result<(RpcStatusCode, String, Vec<ListVolumesResponse_Entry>, usize)> {
        let vol_list: Vec<DatenLordVolume> = smol::run(async move {
            self.get_list_from_etcd(&format!("{}/", VOLUME_ID_PREFIX))
                .await
        })?;
        // self.volume_meta_data
        //     .read()
        //     .unwrap()
        let result_list = Self::list_helper(vol_list, starting_token, max_entries, |vol| {
            let mut entry = ListVolumesResponse_Entry::new();
            entry.mut_volume().set_capacity_bytes(vol.get_size());
            entry.mut_volume().set_volume_id(vol.vol_id.clone());
            entry.mut_volume().set_content_source(VolumeContentSource {
                field_type: if let Some(vcs) = &vol.content_source {
                    Some(vcs.clone().into())
                } else {
                    None
                },
                ..VolumeContentSource::default()
            });

            Some(entry)
        });
        Ok(result_list)
    }

    /// List snapshots except those creation time failed to convert to proto timestamp
    pub fn list_snapshots(
        &self,
        starting_token: &str,
        max_entries: i32,
    ) -> anyhow::Result<(
        RpcStatusCode,
        String,
        Vec<ListSnapshotsResponse_Entry>,
        usize,
    )> {
        let snap_list: Vec<DatenLordSnapshot> = smol::run(async move {
            self.get_list_from_etcd(&format!("{}/", SNAPSHOT_ID_PREFIX))
                .await
        })?;
        // self
        //     .snapshot_meta_data
        //     .read()
        //     .unwrap()
        let result_list = Self::list_helper(snap_list, starting_token, max_entries, |snap| {
            let mut entry = ListSnapshotsResponse_Entry::new();
            entry.mut_snapshot().set_size_bytes(snap.size_bytes);
            entry.mut_snapshot().set_snapshot_id(snap.snap_id.clone());
            entry
                .mut_snapshot()
                .set_source_volume_id(snap.vol_id.clone());
            entry.mut_snapshot().set_creation_time(
                match util::generate_proto_timestamp(&snap.creation_time) {
                    Ok(ts) => ts,
                    Err(e) => panic!("failed to generate proto timestamp, the error is: {}", e),
                },
            );
            entry.mut_snapshot().set_ready_to_use(snap.ready_to_use);

            Some(entry)
        });
        Ok(result_list)
    }

    /// Find volume by ID
    pub fn find_volume_by_id(&self, vol_id: &str) -> bool {
        // self.volume_meta_data.read().unwrap().contains_key(vol_id)
        self.get_volume_by_id(vol_id).is_ok()
    }

    /// Get volume by ID
    pub fn get_volume_by_id(&self, vol_id: &str) -> anyhow::Result<DatenLordVolume> {
        // match self.volume_meta_data.read().unwrap().get(vol_id) {
        //     Some(vol) => Some(Arc::<DatenLordVolume>::clone(vol)),
        //     None => None,
        // }
        smol::run(async move {
            self.get_at_most_one_value_from_etcd(&format!("{}/{}", VOLUME_ID_PREFIX, vol_id))
                .await
        })
    }

    /// Get volume by name
    pub fn get_volume_by_name(&self, vol_name: &str) -> anyhow::Result<DatenLordVolume> {
        // match self
        //     .volume_meta_data
        //     .read()
        //     .unwrap()
        //     .iter()
        //     .find(|(_key, val)| val.vol_name == vol_name)
        // {
        //     Some(pair) => Some(Arc::<DatenLordVolume>::clone(pair.1)),
        //     None => None,
        // }
        let vol_name_key = format!("{}/{}", VOLUME_NAME_PREFIX, vol_name);
        let vol_id: String =
            smol::run(async move { self.get_at_most_one_value_from_etcd(&vol_name_key).await })?;
        debug!(
            "get_volume_by_name found volume ID={} and name={}",
            vol_id, vol_name
        );
        self.get_volume_by_id(&vol_id)
    }

    /// Add new snapshot meta data
    pub fn add_snapshot_meta_data(
        &self,
        snap_id: String,
        snapshot: &DatenLordSnapshot,
    ) -> anyhow::Result<()> {
        info!("adding the meta data of snapshot ID={}", snap_id);

        // self.snapshot_meta_data
        //     .write()
        //     .unwrap()
        //     .insert(snap_id, Arc::new(snapshot))

        // TODO: use etcd transancation?
        let snap_id_key = format!("{}/{}", SNAPSHOT_ID_PREFIX, snap_id);
        let snap_id_pre_value =
            smol::run(async { self.write_to_etcd(&snap_id_key, snapshot).await })?;
        debug_assert!(
            snap_id_pre_value.is_none(),
            "failed to add new key={}, it replaced previous value {:?}",
            snap_id_key,
            snap_id_pre_value,
        );

        let snap_name_key = format!("{}/{}", SNAPSHOT_NAME_PREFIX, snapshot.snap_name);
        let snap_name_pre_value =
            smol::run(async { self.write_to_etcd(&snap_name_key, &snap_id).await })?;
        debug_assert!(
            snap_name_pre_value.is_none(),
            "failed to add new key={}, it replaced previous value {:?}",
            snap_name_key,
            snap_name_pre_value,
        );

        let snap_source_id_key = format!("{}/{}", SNAPSHOT_SOURCE_ID_PREFIX, snapshot.vol_id);
        let snap_source_pre_value =
            smol::run(async { self.write_to_etcd(&snap_source_id_key, &snap_id).await })?;
        debug_assert!(
            snap_source_pre_value.is_none(),
            "failed to add new key={}, it replaced previous value {:?}",
            snap_source_id_key,
            snap_source_pre_value,
        );

        let node_snap_key = format!("{}/{}/{}", NODE_SNAPSHOT_PREFIX, snapshot.node_id, snap_id);
        let node_snap_pre_value = smol::run(async {
            self.write_to_etcd(&node_snap_key, &snapshot.ready_to_use)
                .await
        })?;
        debug_assert!(
            node_snap_pre_value.is_none(),
            "failed to add new key={}, it replaced previous value {:?}",
            node_snap_key,
            node_snap_pre_value,
        );

        Ok(())
    }

    /// Delete the snapshot meta data
    pub fn delete_snapshot_meta_data(&self, snap_id: &str) -> anyhow::Result<DatenLordSnapshot> {
        info!("deleting the meta data of snapshot ID={}", snap_id);

        // let res = self.snapshot_meta_data.write().unwrap().remove(snap_id);
        // match res {
        //     Some(snap) => Some(snap),
        //     // For idempotency, repeated deletion return no error
        //     None => {
        //         error!("failed to find snapshot ID={} to delete", snap_id);
        //         None
        //     }
        // }

        // TODO: use etcd transancation?
        let snap_id_key = format!("{}/{}", SNAPSHOT_ID_PREFIX, snap_id);
        let snap_id_pre_value: DatenLordSnapshot =
            smol::run(async { self.delete_one_value_from_etcd(&snap_id_key).await })?;
        debug_assert_eq!(
            snap_id_pre_value.snap_id, snap_id,
            "deleted key={} value not match",
            snap_id_key,
        );

        let snap_name_key = format!("{}/{}", SNAPSHOT_NAME_PREFIX, snap_id_pre_value.snap_name);
        let snap_name_pre_value: String =
            smol::run(async { self.delete_one_value_from_etcd(&snap_name_key).await })?;
        debug_assert_eq!(
            snap_name_pre_value, snap_id_pre_value.snap_id,
            "deleted key={} value not match",
            snap_name_key,
        );

        let snap_source_id_key =
            format!("{}/{}", SNAPSHOT_SOURCE_ID_PREFIX, snap_id_pre_value.vol_id);
        let snap_source_pre_value: String =
            smol::run(async { self.delete_one_value_from_etcd(&snap_source_id_key).await })?;
        debug_assert_eq!(
            snap_source_pre_value, snap_id_pre_value.snap_id,
            "deleted key={} value not match",
            snap_source_id_key,
        );

        let node_snap_key = format!(
            "{}/{}/{}",
            NODE_SNAPSHOT_PREFIX, snap_id_pre_value.node_id, snap_id
        );
        let node_snap_pre_value: bool =
            smol::run(async { self.delete_one_value_from_etcd(&node_snap_key).await })?;
        debug_assert_eq!(
            node_snap_pre_value, snap_id_pre_value.ready_to_use,
            "deleted key={} value not match",
            node_snap_key,
        );

        Ok(snap_id_pre_value)
    }

    /// Update the existing volume meta data
    pub fn update_volume_meta_data(
        &self,
        vol_id: String,
        volume: &DatenLordVolume,
    ) -> anyhow::Result<DatenLordVolume> {
        info!("updating the meta data of volume ID={}", vol_id);

        let vol_id_key = format!("{}/{}", VOLUME_ID_PREFIX, vol_id);
        let vol_id_pre_value: DatenLordVolume =
            smol::run(async { self.update_to_etcd(&vol_id_key, volume).await })?;
        debug_assert_eq!(
            vol_id_pre_value.vol_id, vol_id,
            "replaced key={} value not match",
            vol_id_key,
        );

        let vol_name_key = format!("{}/{}", VOLUME_NAME_PREFIX, volume.vol_name);
        let vol_name_pre_value: String =
            smol::run(async { self.update_to_etcd(&vol_name_key, &vol_id).await })?;
        debug_assert_eq!(
            vol_name_pre_value, vol_id,
            "replaced key={} value not match",
            vol_name_key,
        );

        // Volume ephemeral field cannot be changed
        let node_vol_key = format!("{}/{}/{}", NODE_VOLUME_PREFIX, volume.node_id, vol_id);
        let node_vol_pre_value: bool =
            smol::run(async { self.update_to_etcd(&node_vol_key, &volume.ephemeral).await })?;
        debug_assert_eq!(
            node_vol_pre_value, vol_id_pre_value.ephemeral,
            "replaced key={} value not match",
            node_vol_key,
        );

        Ok(vol_id_pre_value)
    }

    /// Add new volume meta data
    pub fn add_volume_meta_data(
        &self,
        vol_id: String,
        volume: &DatenLordVolume,
    ) -> anyhow::Result<()> {
        info!("adding the meta data of volume ID={}", vol_id);

        // self.volume_meta_data
        //     .write()
        //     .unwrap()
        //     .insert(vol_id, Arc::new(volume))

        let vol_id_key = format!("{}/{}", VOLUME_ID_PREFIX, vol_id);
        let vol_id_pre_value = smol::run(async { self.write_to_etcd(&vol_id_key, volume).await })?;
        debug_assert!(
            vol_id_pre_value.is_none(),
            "failed to add new key={}, it replaced previous value {:?}",
            vol_id_key,
            vol_id_pre_value,
        );

        let vol_name_key = format!("{}/{}", VOLUME_NAME_PREFIX, volume.vol_name);
        let vol_name_pre_value =
            smol::run(async { self.write_to_etcd(&vol_name_key, &vol_id).await })?;
        debug_assert!(
            vol_name_pre_value.is_none(),
            "failed to add new key={}, it replaced previous value {:?}",
            vol_name_key,
            vol_name_pre_value,
        );

        let node_vol_key = format!("{}/{}/{}", NODE_VOLUME_PREFIX, volume.node_id, vol_id);
        let node_vol_pre_value =
            smol::run(async { self.write_to_etcd(&node_vol_key, &volume.ephemeral).await })?;
        debug_assert!(
            node_vol_pre_value.is_none(),
            "failed to add new key={}, it replaced previous value {:?}",
            node_vol_key,
            node_vol_pre_value,
        );

        Ok(())
    }

    /// Delete the volume meta data
    pub fn delete_volume_meta_data(&self, vol_id: &str) -> anyhow::Result<DatenLordVolume> {
        info!("deleting volume ID={}", vol_id);

        // let res = self.volume_meta_data.write().unwrap().remove(vol_id);
        // if let Some(vol) = res {
        //     Some(vol)
        // } else {
        //     // For idempotency, repeated deletion return no error
        //     error!("failed to find volume ID={} to delete", vol_id);
        //     None
        // }

        // TODO: use etcd transancation?
        let vol_id_key = format!("{}/{}", VOLUME_ID_PREFIX, vol_id);
        let vol_id_pre_value: DatenLordVolume =
            smol::run(async { self.delete_one_value_from_etcd(&vol_id_key).await })?;
        debug_assert_eq!(
            vol_id_pre_value.vol_id, vol_id,
            "deleted key={} value not match",
            vol_id_key,
        );

        let vol_name_key = format!("{}/{}", VOLUME_NAME_PREFIX, vol_id_pre_value.vol_name);
        let vol_name_pre_value: String =
            smol::run(async { self.delete_one_value_from_etcd(&vol_name_key).await })?;
        debug_assert_eq!(
            vol_name_pre_value, vol_id_pre_value.vol_id,
            "deleted key={} value not match",
            vol_name_key,
        );

        let node_vol_key = format!(
            "{}/{}/{}",
            NODE_VOLUME_PREFIX, vol_id_pre_value.node_id, vol_id
        );
        let node_vol_pre_value: bool =
            smol::run(async { self.delete_one_value_from_etcd(&node_vol_key).await })?;
        debug_assert_eq!(
            node_vol_pre_value, vol_id_pre_value.ephemeral,
            "deleted key={} value not match",
            node_vol_key,
        );

        // if unistd::geteuid().is_root() {
        let get_mount_path_res = self.get_volume_bind_mount_path(vol_id);
        if let Ok(pre_mount_path) = get_mount_path_res {
            let umount_res = util::umount_volume_bind_path(&pre_mount_path);
            if let Err(e) = umount_res {
                panic!(
                    "failed to un-mount volume ID={} bind path={}, \
                            the error is: {}",
                    vol_id, pre_mount_path, e,
                );
            }
            let deleted_path = self.delete_volume_bind_mount_path(vol_id)?;
            debug_assert_eq!(
                pre_mount_path, deleted_path,
                "the volume bind mount path and \
                        the deleted path not match when delete volume meta data",
            );
        }
        // }
        Ok(vol_id_pre_value)
    }

    /// Populate the given destPath with data from the snapshot ID
    pub fn copy_volume_from_snapshot(
        &self,
        dst_volume_size: i64,
        src_snapshot_id: &str,
        dst_volume_id: &str,
    ) -> (RpcStatusCode, String) {
        let dst_path = self.get_volume_path(dst_volume_id);

        match self.get_snapshot_by_id(src_snapshot_id) {
            Err(e) => {
                return (
                    RpcStatusCode::NOT_FOUND,
                    format!(
                        "failed to find source snapshot ID={}, the error is: {}",
                        src_snapshot_id, e,
                    ),
                );
            }
            Ok(src_snapshot) => {
                assert_eq!(
                    src_snapshot.node_id,
                    self.get_node_id(),
                    "snapshot ID={} is on node ID={} not on local node ID={}",
                    src_snapshot_id,
                    src_snapshot.node_id,
                    self.get_node_id(),
                );
                if !src_snapshot.ready_to_use {
                    return (
                        RpcStatusCode::INTERNAL,
                        format!(
                            "source snapshot ID={} and name={} is not yet ready to use",
                            src_snapshot.snap_id, src_snapshot.snap_name,
                        ),
                    );
                }
                if src_snapshot.size_bytes > dst_volume_size {
                    return (
                        RpcStatusCode::INVALID_ARGUMENT,
                        format!(
                            "source snapshot ID={} and name={} has size={} \
                                greater than requested volume size={}",
                            src_snapshot.snap_id,
                            src_snapshot.snap_name,
                            src_snapshot.size_bytes,
                            dst_volume_size,
                        ),
                    );
                }

                debug_assert!(
                    dst_path.is_dir(),
                    "the volume of monnt access type should have a directory path: {:?}",
                    dst_path,
                );
                let open_res = File::open(&src_snapshot.snap_path);
                let tar_gz = match open_res {
                    Ok(tg) => tg,
                    Err(e) => {
                        return (
                            RpcStatusCode::INTERNAL,
                            format!(
                                "failed to open source snapshot {:?}, the error is: {}",
                                src_snapshot.snap_path, e,
                            ),
                        );
                    }
                };
                let tar_file = flate2::read::GzDecoder::new(tar_gz);
                let mut archive = tar::Archive::new(tar_file);
                let unpack_res = archive.unpack(&dst_path);
                if let Err(e) = unpack_res {
                    return (
                        RpcStatusCode::INTERNAL,
                        format!(
                            "failed to decompress snapshot to {:?}, the error is: {}",
                            dst_path, e,
                        ),
                    );
                }
            }
        }

        (RpcStatusCode::OK, "".to_owned())
    }

    /// Populate the given destPath with data from the `src_volume_id`
    pub fn copy_volume_from_volume(
        &self,
        dst_volume_size: i64,
        src_volume_id: &str,
        dst_volume_id: &str,
    ) -> (RpcStatusCode, String) {
        let dst_path = self.get_volume_path(dst_volume_id);

        match self.get_volume_by_id(src_volume_id) {
            Err(e) => {
                return (
                    RpcStatusCode::NOT_FOUND,
                    format!(
                        "source volume ID={} does not exist, make sure \
                            source/destination in the same storage class, \
                            the error is: {}",
                        src_volume_id, e,
                    ),
                );
            }
            Ok(src_volume) => {
                assert_eq!(
                    src_volume.node_id,
                    self.get_node_id(),
                    "snapshot ID={} is on node ID={} not on local node ID={}",
                    src_volume_id,
                    src_volume.node_id,
                    self.get_node_id(),
                );
                if src_volume.get_size() > dst_volume_size {
                    return (
                        RpcStatusCode::INVALID_ARGUMENT,
                        format!(
                            "source volume ID={} and name={} has size={} \
                                greater than requested volume size={}",
                            src_volume.vol_id,
                            src_volume.vol_name,
                            src_volume.get_size(),
                            dst_volume_size,
                        ),
                    );
                }

                let copy_res = util::copy_directory_recursively(
                    &src_volume.vol_path,
                    &dst_path,
                    false, // follow symlink or not
                );
                match copy_res {
                    Ok(copy_size) => {
                        info!(
                            "successfully copied {} files from {:?} to {:?}",
                            copy_size, src_volume.vol_path, dst_path,
                        );
                    }
                    Err(e) => {
                        return (
                            RpcStatusCode::INTERNAL,
                            format!(
                                "failed to pre-populate data from source mount volume {} and name={}, \
                                    the error is: {}", 
                                src_volume.vol_id,
                                src_volume.vol_name,
                                e,
                            ),
                        );
                    }
                }
            }
        }

        (RpcStatusCode::OK, "".to_owned())
    }

    /// Delete volume bind path from etcd
    pub fn delete_volume_bind_mount_path(&self, vol_id: &str) -> anyhow::Result<String> {
        let volume_mount_path_key = format!(
            "{}/{}/{}",
            VOLUME_BIND_MOUNT_PATH_PREFIX,
            self.get_node_id(),
            vol_id,
        );
        let target_path: String = smol::run(async {
            self.delete_one_value_from_etcd(&volume_mount_path_key)
                .await
        })?;

        Ok(target_path)
    }

    /// Get volume bind mount path from etcd
    pub fn get_volume_bind_mount_path(&self, vol_id: &str) -> anyhow::Result<String> {
        let volume_mount_path_key = format!(
            "{}/{}/{}",
            VOLUME_BIND_MOUNT_PATH_PREFIX,
            self.get_node_id(),
            vol_id,
        );
        smol::run(async {
            self.get_at_most_one_value_from_etcd(&volume_mount_path_key)
                .await
        })
    }

    /// Write volume bind mount path to etcd
    fn save_volume_bind_mount_path(&self, vol_id: &str, target_path: &str) {
        let volume_mount_path_key = format!(
            "{}/{}/{}",
            VOLUME_BIND_MOUNT_PATH_PREFIX,
            self.get_node_id(),
            vol_id,
        );
        let target_path_str = target_path.to_owned();
        let volume_mount_path_pre_value = smol::run(async {
            self.write_to_etcd(&volume_mount_path_key, &target_path_str)
                .await
        });
        match volume_mount_path_pre_value {
            Ok(pre_value) => {
                if let Some(pre_mount_path) = pre_value {
                    if pre_mount_path == target_path {
                        warn!(
                            "the volume has been mount to {}, \
                                and this time mount to the same path again {}",
                            pre_mount_path, target_path,
                        );
                    } else {
                        panic!(
                            "the volume has been mount to {}, \
                                and this time re-mount to {}",
                            pre_mount_path, target_path,
                        );
                    }
                }
            }
            Err(e) => panic!(
                "failed to write the mount path={} of volume ID={} to etcd, \
                    the error is: {}",
                target_path, vol_id, e,
            ),
        }
    }

    /// Bind mount volume directory to target path if root
    pub fn bind_mount(
        &self,
        target_path: &str,
        fs_type: &str,
        read_only: bool,
        vol_id: &str,
        mount_options: &str,
        ephemeral: bool,
    ) -> (RpcStatusCode, String) {
        let vol_path = self.get_volume_path(vol_id);
        // Bind mount from target_path to vol_path if run as root
        let target_dir = Path::new(target_path);
        if target_dir.exists() {
            debug!("found target bind mount directory={:?}", target_dir);
        } else {
            let create_res = fs::create_dir_all(target_dir);
            if let Err(e) = create_res {
                return (
                    RpcStatusCode::INTERNAL,
                    format!(
                        "failed to create target bind mount directory={:?}, the error is: {}",
                        target_dir, e,
                    ),
                );
            }
        };

        let mut mnt_flags = MsFlags::MS_BIND;
        if read_only {
            mnt_flags |= MsFlags::MS_RDONLY;
        }
        let get_res = self.get_volume_bind_mount_path(vol_id);
        let remount = match get_res {
            Ok(pre_target_path) => {
                debug_assert_eq!(
                    target_path, &pre_target_path,
                    "only one bind mount path allowed, \
                        the target bind mount path in etcd \
                        not match the one given to re-mount",
                );
                true
            }
            Err(_) => false,
        };
        if remount {
            warn!(
                "re-mount volume ID={} to target path={}",
                vol_id, target_path,
            );
            mnt_flags |= MsFlags::MS_REMOUNT;
        }
        let mount_res = if unistd::geteuid().is_root() {
            mount::mount::<Path, Path, OsStr, OsStr>(
                Some(&vol_path),
                target_dir,
                if fs_type.is_empty() {
                    None
                } else {
                    Some(OsStr::new(fs_type))
                },
                mnt_flags,
                if mount_options.is_empty() {
                    None
                } else {
                    Some(OsStr::new(&mount_options))
                },
            )
            .context(format!(
                "failed to direct mount {:?} to {:?}",
                vol_path, target_dir
            ))
        } else {
            let mut mount_cmd = Command::new(BIND_MOUNTER);
            mount_cmd
                .arg("-f")
                .arg(&vol_path)
                .arg("-t")
                .arg(&target_dir);
            if read_only {
                mount_cmd.arg("-r");
            }
            if remount {
                mount_cmd.arg("-m");
            }
            if !fs_type.is_empty() {
                mount_cmd.arg("-s").arg(&fs_type);
            }
            if !mount_options.is_empty() {
                mount_cmd.arg("-o").arg(&mount_options);
            }
            let mount_handle = match mount_cmd.output() {
                Ok(h) => h,
                Err(e) => {
                    return (
                        RpcStatusCode::INTERNAL,
                        format!("bind_mounter command failed to start, the error is: {}", e),
                    )
                }
            };
            if mount_handle.status.success() {
                Ok(())
            } else {
                let stderr = String::from_utf8_lossy(&mount_handle.stderr);
                debug!("bind_mounter failed to mount, the error is: {}", &stderr);
                Err(anyhow!(
                    "bind_mounter failed to mount {:?} to {:?}, the error is: {}",
                    vol_path,
                    target_dir,
                    stderr,
                ))
            }
        };
        if let Err(bind_err) = mount_res {
            if ephemeral {
                match self.delete_volume_meta_data(vol_id) {
                    Ok(_) => debug!(
                        "successfully deleted ephemeral volume ID={}, when bind mount failed",
                        vol_id
                    ),
                    Err(e) => error!(
                        "failed to delete ephemeral volume ID={}, \
                            when bind mount failed, the error is: {}",
                        vol_id, e,
                    ),
                }
            }
            return (
                RpcStatusCode::INTERNAL,
                format!(
                    "failed to bind mount from {} to {:?}, the bind error is: {}",
                    target_path, vol_path, bind_err,
                ),
            );
        } else {
            info!(
                "successfully bind mounted volume path={:?} to target path={:?}",
                vol_path, target_dir
            );

            self.save_volume_bind_mount_path(vol_id, target_path);
        }

        (RpcStatusCode::OK, "".to_owned())
    }

    // /// or create target path as symbolic link to volume directory if non-root
    // pub fn symbolic_link(
    //     &self,
    //     target_path: &str,
    //     vol_id: &str,
    //     ephemeral: bool,
    // ) -> (RpcStatusCode, String) {
    //     let vol_path = self.get_volume_path(vol_id);
    //     // Build symlink from target_path to vol_path if run as non-root
    //     let sym_link = std::path::Path::new(target_path);
    //     if sym_link.exists() {
    //         let read_link_res = fs::read_link(&sym_link);
    //         match read_link_res {
    //             Err(e) => {
    //                 panic!(
    //                     "failed to read the volume target path={} as a symlink, the error is: {}",
    //                     target_path, e,
    //                 );
    //             }
    //             Ok(old_link_path) => {
    //                 let str_uuid_res = if old_link_path.is_dir() {
    //                     // Find base directory name (volume ID part) of a volume directory path
    //                     let last_res = old_link_path.file_name();
    //                     match last_res {
    //                         None => panic!(
    //                             "failed to get the volume ID from the existing volume path={:?}",
    //                             old_link_path,
    //                         ),
    //                         Some(base_dir_name) => {
    //                             String::from_utf8(base_dir_name.as_bytes().to_owned())
    //                         }
    //                     }
    //                 } else {
    //                     panic!(
    //                         "volume path should be a directory, but {} is not directory",
    //                         old_link_path.display(),
    //                     );
    //                 };
    //                 match str_uuid_res {
    //                     Ok(uuid_str) => {
    //                         let old_vol_id = match uuid::Uuid::parse_str(&uuid_str) {
    //                             Ok(old_uuid) => old_uuid.to_string(),
    //                             Err(e) => panic!(
    //                                 "failed to convert string={} to uuid, the error is: {}",
    //                                 uuid_str, e,
    //                             ),
    //                         };
    //                         // For idempotency, in case call this function repeatedly
    //                         if old_vol_id != vol_id {
    //                             // TODO: should delete old volume here?
    //                             match self.delete_volume_meta_data(&old_vol_id) {
    //                                 Ok(_) => debug!("successfully deleted old volume ID={}", old_vol_id),
    //                                 Err(e) => error!("failed to delete old volume ID={}, the error is: {}", old_vol_id, e,),
    //                             }
    //                         }
    //                     }
    //                     Err(e) => panic!(
    //                         "failed to parse the volume ID from invalid volume path={:?}, the error is: {}",
    //                         old_link_path, e,
    //                     ),
    //                 }
    //             }
    //         }
    //         let remove_res = fs::remove_file(&sym_link);
    //         debug_assert!(
    //             remove_res.is_ok(),
    //             "failed to remove existing target path={:?}",
    //             sym_link
    //         );
    //     }

    //     let link_res = unistd::symlinkat(&vol_path, None, sym_link);
    //     if let Err(e) = link_res {
    //         if ephemeral {
    //             match self.delete_volume_meta_data(vol_id) {
    //                 Ok(_) => debug!(
    //                     "successfully deleted ephemeral volume ID={}, when make symlink failed",
    //                     vol_id
    //                 ),
    //                 Err(e) => error!(
    //                     "failed to delete ephemeral volume ID={}, \
    //                                 when make symlink failed, the error is: {}",
    //                     vol_id, e,
    //                 ),
    //             }
    //         }
    //         return (
    //             RpcStatusCode::INTERNAL,
    //             format!(
    //                 "failed to create symlink from {} to {:?}, the error is: {}",
    //                 target_path, vol_path, e,
    //             ),
    //         );
    //     }

    //     (RpcStatusCode::OK, "".to_owned())
    // }

    /// Build snapshot from source volume
    pub fn build_snapshot_from_volume(
        &self,
        src_vol_id: &str,
        snap_id: &str,
        snap_name: &str,
    ) -> anyhow::Result<DatenLordSnapshot> {
        match self.get_volume_by_id(src_vol_id) {
            Ok(src_vol) => {
                assert_eq!(
                    src_vol.node_id,
                    self.get_node_id(),
                    "volume ID={} is on node ID={} not on local node ID={}",
                    src_vol_id,
                    src_vol.node_id,
                    self.get_node_id(),
                );

                let vol_path = &src_vol.vol_path;
                let snap_path = self.get_snapshot_path(snap_id);

                let open_res = File::create(&snap_path);
                let tar_gz = match open_res {
                    Ok(tg) => tg,
                    Err(e) => {
                        return Err(anyhow::anyhow!(format!(
                            "failed to create snapshot file {:?}, the error is: {}",
                            snap_path, e,
                        ),));
                    }
                };
                let gz_file = flate2::write::GzEncoder::new(tar_gz, flate2::Compression::default());
                let mut tar_file = tar::Builder::new(gz_file);
                let tar_res = tar_file.append_dir_all("./", &vol_path);
                if let Err(e) = tar_res {
                    let remove_res = fs::remove_file(&snap_path);
                    if let Err(e) = remove_res {
                        error!(
                            "failed to remove bad snapshot file {:?}, the error is: {}",
                            snap_path, e,
                        );
                    }
                    return Err(anyhow::anyhow!(format!(
                        "failed to generate snapshot for volume ID={} and name={}, the error is: {}",
                        src_vol.vol_id, src_vol.vol_name, e,
                    )));
                }
                let into_res = tar_file.into_inner();
                match into_res {
                    Ok(gz_file) => {
                        let gz_finish_res = gz_file.finish();
                        if let Err(e) = gz_finish_res {
                            let remove_res = fs::remove_file(&snap_path);
                            if let Err(e) = remove_res {
                                error!(
                                    "failed to remove bad snapshot file {:?}, the error is: {}",
                                    snap_path, e
                                );
                            }
                            return Err(anyhow::anyhow!(format!(
                                    "failed to generate snapshot for volume ID={} and name={}, the error is: {}",
                                    src_vol.vol_id, src_vol.vol_name, e,
                                )));
                        }
                    }
                    Err(e) => {
                        let remove_res = fs::remove_file(&snap_path);
                        if let Err(e) = remove_res {
                            error!(
                                "failed to remove bad snapshot file {:?}, the error is: {}",
                                snap_path, e
                            );
                        }
                        return Err(anyhow::anyhow!(format!(
                                "failed to generate snapshot for volume ID={} and name={}, the error is: {}",
                                src_vol.vol_id, src_vol.vol_name, e,
                            )));
                    }
                }

                let now = std::time::SystemTime::now();
                let snapshot = DatenLordSnapshot::new(
                    snap_name.to_owned(),
                    snap_id.to_string(),
                    src_vol_id.to_owned(),
                    self.get_node_id().to_owned(),
                    snap_path,
                    now,
                    src_vol.get_size(),
                    true, // ready_to_use
                );

                Ok(snapshot)
            }
            Err(e) => Err(anyhow::anyhow!(format!(
                "failed to find source volume ID={}, the error is: {}",
                src_vol_id, e,
            ),)),
        }
    }

    /// Expand volume size, return previous size
    pub fn expand(&self, volume: &mut DatenLordVolume, new_size_bytes: i64) -> anyhow::Result<i64> {
        let old_size_bytes = volume.get_size();
        if volume.expand_size(new_size_bytes) {
            let prv_vol = self.update_volume_meta_data(volume.vol_id.clone(), volume)?;
            assert_eq!(
                prv_vol.get_size(),
                old_size_bytes,
                "the volume size before expand not match"
            );
            Ok(old_size_bytes)
        } else {
            Err(anyhow::anyhow!(
                "the new size={} is smaller than original size={}",
                new_size_bytes,
                old_size_bytes
            ))
        }
    }
}

/// Volume access mode, copied from `VolumeCapability_AccessMode_Mode`
/// because `VolumeCapability_AccessMode_Mode` is not serializable
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum VolumeAccessMode {
    /// Volume access mode unknow
    Unknown = 0,
    /// Can only be published once as read/write on a single node, at
    /// any given time.
    SingleNodeWriter = 1,
    /// Can only be published once as readonly on a single node, at
    /// any given time.
    SingleNodeReadOnly = 2,
    /// Can be published as readonly at multiple nodes simultaneously.
    MultiNodeReadOnly = 3,
    /// Can be published at multiple nodes simultaneously. Only one of
    /// the node can be used as read/write. The rest will be readonly.
    MultiNodeSingleWriter = 4,
    /// Can be published as read/write at multiple nodes
    /// simultaneously.
    MultiNodeMultiWriter = 5,
}

impl From<VolumeCapability_AccessMode_Mode> for VolumeAccessMode {
    fn from(vc: VolumeCapability_AccessMode_Mode) -> Self {
        match vc {
            VolumeCapability_AccessMode_Mode::UNKNOWN => Self::Unknown,
            VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER => Self::SingleNodeWriter,
            VolumeCapability_AccessMode_Mode::SINGLE_NODE_READER_ONLY => Self::SingleNodeReadOnly,
            VolumeCapability_AccessMode_Mode::MULTI_NODE_READER_ONLY => Self::MultiNodeReadOnly,
            VolumeCapability_AccessMode_Mode::MULTI_NODE_SINGLE_WRITER => {
                Self::MultiNodeSingleWriter
            }
            VolumeCapability_AccessMode_Mode::MULTI_NODE_MULTI_WRITER => Self::MultiNodeMultiWriter,
        }
    }
}

/// Volume source, copied from `VolumeContentSource_oneof_type`,
/// because `VolumeContentSource_oneof_type` is not serializable,
/// either source snapshot ID or source volume ID
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum VolumeSource {
    /// Volume source from a snapshot
    Snapshot(String),
    /// Volume source from anther volume
    Volume(String),
}

impl From<VolumeContentSource_oneof_type> for VolumeSource {
    fn from(vcs: VolumeContentSource_oneof_type) -> Self {
        match vcs {
            VolumeContentSource_oneof_type::snapshot(s) => {
                Self::Snapshot(s.get_snapshot_id().to_owned())
            }
            VolumeContentSource_oneof_type::volume(v) => Self::Volume(v.get_volume_id().to_owned()),
        }
    }
}

impl Into<VolumeContentSource_oneof_type> for VolumeSource {
    fn into(self) -> VolumeContentSource_oneof_type {
        match self {
            Self::Snapshot(snap_id) => {
                VolumeContentSource_oneof_type::snapshot(VolumeContentSource_SnapshotSource {
                    snapshot_id: snap_id,
                    ..VolumeContentSource_SnapshotSource::default()
                })
            }
            Self::Volume(vol_id) => {
                VolumeContentSource_oneof_type::volume(VolumeContentSource_VolumeSource {
                    volume_id: vol_id,
                    ..VolumeContentSource_VolumeSource::default()
                })
            }
        }
    }
}

/// `DatenLord` volume
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatenLordVolume {
    /// Volume name
    pub vol_name: String,
    /// Volume ID
    pub vol_id: String,
    /// Volume size in bytes
    pub size_bytes: i64,
    /// The ID of the node the volume stored at
    pub node_id: String,
    /// The volume diretory path
    pub vol_path: PathBuf,
    /// Volume access mode
    pub vol_access_mode: Vec<VolumeAccessMode>,
    /// The content source of the volume
    pub content_source: Option<VolumeSource>,
    /// The volume is ephemeral or not
    pub ephemeral: bool,
}

impl DatenLordVolume {
    /// Create volume helper
    fn new(
        vol_id: String,
        vol_name: String,
        vol_size: i64,
        node_id: String,
        vol_path: PathBuf,
        vol_access_mode: impl Into<Vec<VolumeCapability_AccessMode_Mode>>,
        content_source: Option<VolumeContentSource_oneof_type>,
        ephemeral: bool,
    ) -> anyhow::Result<Self> {
        assert!(!vol_id.is_empty(), "volume ID cannot be empty");
        assert!(!vol_name.is_empty(), "volume name cannot be empty");
        assert!(!node_id.is_empty(), "node ID cannot be empty");
        assert!(vol_size >= 0, "invalid volume size: {}", vol_size);
        let vol_access_mode_vec = vol_access_mode.into();
        let converted_vol_access_mode_vec = vol_access_mode_vec
            .into_iter()
            .map(VolumeAccessMode::from)
            .collect::<Vec<_>>();
        let vol_source = if let Some(vcs) = content_source {
            Some(VolumeSource::from(vcs))
        } else {
            None
        };
        let vol = Self {
            vol_id,
            vol_name,
            size_bytes: vol_size,
            node_id,
            vol_path,
            vol_access_mode: converted_vol_access_mode_vec,
            content_source: vol_source,
            ephemeral,
        };

        if ephemeral {
            debug_assert!(
                vol.content_source.is_none(),
                "ephemeral volume cannot have content source",
            );
        }

        vol.create_vol_dir()?;
        Ok(vol)
    }

    /// Create ephemeral volume
    pub fn build_ephemeral_volume(
        vol_id: &str,
        vol_name: &str,
        node_id: &str,
        vol_path: &Path,
    ) -> anyhow::Result<Self> {
        Self::new(
            vol_id.to_owned(),
            vol_name.to_owned(),
            util::EPHEMERAL_VOLUME_STORAGE_CAPACITY,
            node_id.to_owned(),
            vol_path.to_owned(),
            [VolumeCapability_AccessMode_Mode::SINGLE_NODE_WRITER],
            None, // content source
            true, // ephemeral
        )
    }

    /// Create volume from `CreateVolumeRequest`
    pub fn build_from_create_volume_req(
        req: &CreateVolumeRequest,
        vol_id: &str,
        node_id: &str,
        vol_path: &Path,
    ) -> anyhow::Result<Self> {
        Self::new(
            vol_id.to_owned(),
            req.get_name().to_owned(),
            req.get_capacity_range().get_required_bytes(),
            node_id.to_owned(),
            vol_path.to_owned(),
            req.get_volume_capabilities()
                .iter()
                .map(|vc| vc.get_access_mode().get_mode())
                .collect::<Vec<_>>(),
            if req.has_volume_content_source() {
                req.get_volume_content_source().field_type.clone()
            } else {
                None
            },
            false, // ephemeral
        )
    }

    /// Create volume directory
    fn create_vol_dir(&self) -> anyhow::Result<()> {
        fs::create_dir_all(&self.vol_path).context(format!(
            "failed to create directory={:?} for volume ID={} and name={}",
            self.vol_path, self.vol_id, self.vol_name,
        ))?;
        Ok(())
    }

    /// Delete volume directory
    pub fn delete_directory(&self) -> anyhow::Result<()> {
        std::fs::remove_dir_all(&self.vol_path).context(format!(
            "failed to remove the volume directory: {:?}",
            self.vol_path
        ))?;
        Ok(())
    }

    /// Get volume size
    pub const fn get_size(&self) -> i64 {
        // TODO: use more relaxed ordering
        self.size_bytes
    }

    /// Expand volume size
    pub fn expand_size(&mut self, new_size: i64) -> bool {
        if new_size > self.get_size() {
            self.size_bytes = new_size;
            true
        } else {
            false
        }
    }
}

// impl Drop for DatenLordVolume {
//     fn drop(&mut self) {
//         let res = self.delete();
//         if let Err(e) = res {
//             error!(
//                 "failed to delete volume ID={} and name={} in DatenLordVolume::drop(), \
//                     the error is: {}",
//                 self.vol_id, self.vol_name, e,
//             );
//         }
//     }
// }

/// Snapshot corresponding to a volume
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatenLordSnapshot {
    /// Snapshot name
    pub snap_name: String,
    /// Snapshto ID
    pub snap_id: String,
    /// The source volume ID of the snapshot
    pub vol_id: String,
    /// The ID of the node the snapshot stored at
    pub node_id: String,
    /// Snapshot path
    pub snap_path: PathBuf,
    /// Snapshot creation time
    pub creation_time: std::time::SystemTime,
    /// Snapshot size in bytes
    pub size_bytes: i64,
    /// The snapshot is ready or not
    pub ready_to_use: bool,
}

impl DatenLordSnapshot {
    /// Create `DatenLordSnapshot`
    pub fn new(
        snap_name: String,
        snap_id: String,
        vol_id: String,
        node_id: String,
        snap_path: PathBuf,
        creation_time: std::time::SystemTime,
        size_bytes: i64,
        ready_to_use: bool,
    ) -> Self {
        assert!(!snap_id.is_empty(), "snapshot ID cannot be empty");
        assert!(!snap_name.is_empty(), "snapshot name cannot be empty");
        assert!(!vol_id.is_empty(), "source volume ID cannot be empty");
        assert!(!node_id.is_empty(), "node ID cannot be empty");
        assert!(size_bytes >= 0, "invalid snapshot size: {}", size_bytes);
        Self {
            snap_name,
            snap_id,
            vol_id,
            node_id,
            snap_path,
            creation_time,
            size_bytes,
            ready_to_use,
        }
    }

    /// Delete snapshot file
    pub fn delete_file(&self) -> anyhow::Result<()> {
        nix::unistd::unlink(&self.snap_path).context(format!(
            "failed to unlink snapshot file: {:?}",
            self.snap_path,
        ))?;
        Ok(())
    }
}

// impl Drop for DatenLordSnapshot {
//     fn drop(&mut self) {
//         let res = self.delete();
//         if let Err(e) = res {
//             error!(
//                 "failed to delete snapshot ID={} and name={}, the error is: {}",
//                 self.snap_id, self.snap_name, e,
//             );
//         }
//     }
// }
