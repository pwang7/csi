//! The implementation for CSI controller service

use anyhow::{self, Context};
use grpcio::{RpcContext, RpcStatusCode, UnarySink};
use log::{debug, error, info, warn};
use protobuf::RepeatedField;
use std::cmp::Ordering;
use std::sync::Arc;

use super::csi::{
    ControllerExpandVolumeRequest, ControllerExpandVolumeResponse,
    ControllerGetCapabilitiesRequest, ControllerGetCapabilitiesResponse,
    ControllerGetVolumeRequest, ControllerGetVolumeResponse, ControllerPublishVolumeRequest,
    ControllerPublishVolumeResponse, ControllerServiceCapability,
    ControllerServiceCapability_RPC_Type, ControllerUnpublishVolumeRequest,
    ControllerUnpublishVolumeResponse, CreateSnapshotRequest, CreateSnapshotResponse,
    CreateVolumeRequest, CreateVolumeResponse, DeleteSnapshotRequest, DeleteSnapshotResponse,
    DeleteVolumeRequest, DeleteVolumeResponse, GetCapacityRequest, GetCapacityResponse,
    ListSnapshotsRequest, ListSnapshotsResponse, ListSnapshotsResponse_Entry, ListVolumesRequest,
    ListVolumesResponse, ValidateVolumeCapabilitiesRequest, ValidateVolumeCapabilitiesResponse,
    VolumeCapability, VolumeCapability_AccessMode_Mode, VolumeContentSource_oneof_type,
};
use super::csi_grpc::Controller;
use super::meta_data::{DatenLordSnapshot, MetaData, VolumeSource};
use super::util;

/// for `ControllerService` implmentation
#[derive(Clone)]
pub struct ControllerImpl {
    /// Controller capabilities
    caps: Vec<ControllerServiceCapability>,
    /// Volume meta data for controller
    meta_data: Arc<MetaData>,
}

impl ControllerImpl {
    /// Create `ControllerImpl`
    pub fn new(meta_data: Arc<MetaData>) -> Self {
        let cap_vec = if meta_data.is_ephemeral() {
            Vec::new()
        } else {
            vec![
                ControllerServiceCapability_RPC_Type::CREATE_DELETE_VOLUME,
                ControllerServiceCapability_RPC_Type::CREATE_DELETE_SNAPSHOT,
                ControllerServiceCapability_RPC_Type::CLONE_VOLUME,
                ControllerServiceCapability_RPC_Type::LIST_VOLUMES,
                ControllerServiceCapability_RPC_Type::LIST_SNAPSHOTS,
                ControllerServiceCapability_RPC_Type::EXPAND_VOLUME,
            ]
        };
        let caps = cap_vec
            .into_iter()
            .map(|rpc_type| {
                let mut csc = ControllerServiceCapability::new();
                csc.mut_rpc().set_field_type(rpc_type);
                csc
            })
            .collect();

        Self { caps, meta_data }
    }

    /// Validate request with controller capabilities
    fn validate_request_capability(&self, rpc_type: ControllerServiceCapability_RPC_Type) -> bool {
        rpc_type == ControllerServiceCapability_RPC_Type::UNKNOWN
            || self
                .caps
                .iter()
                .any(|cap| cap.get_rpc().get_field_type() == rpc_type)
    }

    /// Check for already existing volume name, and if found
    /// check for the requested capacity and already allocated capacity
    fn find_existing_volume(
        &self,
        req: &CreateVolumeRequest,
    ) -> (RpcStatusCode, String, Option<CreateVolumeResponse>) {
        let vol_name = req.get_name();
        let get_vol_res = self.meta_data.get_volume_by_name(vol_name);
        match get_vol_res {
            Some(ex_vol) => {
                // It means the volume with the same name already exists
                // need to check if the size of existing volume is the same as in new
                // request
                let volume_size = req.get_capacity_range().get_required_bytes();
                if ex_vol.get_size() != volume_size {
                    return (
                        RpcStatusCode::ALREADY_EXISTS,
                        format!(
                            "volume with the same name={} already exist but with different size",
                            vol_name,
                        ),
                        None,
                    );
                }

                if req.has_volume_content_source() {
                    let ex_vol_content_source = if let Some(ref vcs) = ex_vol.content_source {
                        vcs
                    } else {
                        return (
                            RpcStatusCode::ALREADY_EXISTS,
                            format!(
                                "existing volume ID={} doesn't have content source",
                                ex_vol.vol_id
                            ),
                            None,
                        );
                    };

                    let volume_source = req.get_volume_content_source();
                    if let Some(ref volume_source_type) = volume_source.field_type {
                        match *volume_source_type {
                            VolumeContentSource_oneof_type::snapshot(ref snapshot_source) => {
                                let parent_snap_id = snapshot_source.get_snapshot_id();
                                if let VolumeSource::Snapshot(ref psid) = *ex_vol_content_source {
                                    if psid != parent_snap_id {
                                        return (
                                            RpcStatusCode::ALREADY_EXISTS,
                                            format!(
                                                "existing volume ID={} has parent snapshot ID={}, \
                                                but VolumeContentSource_SnapshotSource has \
                                                parent snapshot ID={}",
                                                ex_vol.vol_id, psid, parent_snap_id,
                                            ),
                                            None,
                                        );
                                    }
                                } else {
                                    return (
                                        RpcStatusCode::ALREADY_EXISTS,
                                        format!(
                                            "existing volume ID={} doesn't have parent snapshot ID",
                                            ex_vol.vol_id
                                        ),
                                        None,
                                    );
                                }
                            }
                            VolumeContentSource_oneof_type::volume(ref volume_source) => {
                                let parent_vol_id = volume_source.get_volume_id();
                                if let VolumeSource::Volume(ref pvid) = *ex_vol_content_source {
                                    if pvid != parent_vol_id {
                                        return (
                                            RpcStatusCode::ALREADY_EXISTS,
                                            format!(
                                                "existing volume ID={} has parent volume ID={}, \
                                                but VolumeContentSource_VolumeSource has \
                                                parent volume ID={}",
                                                ex_vol.vol_id, pvid, parent_vol_id,
                                            ),
                                            None,
                                        );
                                    }
                                } else {
                                    return (
                                        RpcStatusCode::ALREADY_EXISTS,
                                        format!(
                                            "existing volume ID={} doesn't have parent volume ID",
                                            ex_vol.vol_id
                                        ),
                                        None,
                                    );
                                }
                            }
                        }
                    }
                }
                // Return existing volume
                // TODO: make sure that volume still exists?
                let resp = util::build_create_volume_response(req, &ex_vol.vol_id, &ex_vol.node_id);
                (RpcStatusCode::OK, "".to_owned(), Some(resp))
            }
            None => (
                RpcStatusCode::OK,
                format!("failed to find volume with name={}", vol_name),
                None,
            ),
        }
    }

    /// Check for already existing snapshot name, and if found check for the
    /// requested source volume ID matches snapshot that has been created
    fn find_existing_snapshot(
        &self,
        req: &CreateSnapshotRequest,
    ) -> (RpcStatusCode, String, Option<CreateSnapshotResponse>) {
        match self.meta_data.get_snapshot_by_name(req.get_name()) {
            Some(ex_snap) => {
                // The snapshot with the same name already exists need to check
                // if the source volume ID of existing snapshot is the same as in new request
                let snap_name = req.get_name();
                let src_vol_id = req.get_source_volume_id();
                let node_id = self.meta_data.get_node_id();

                if ex_snap.vol_id == src_vol_id {
                    let build_resp_res = util::build_create_snapshot_response(
                        req,
                        &ex_snap.snap_id,
                        &ex_snap.creation_time,
                        ex_snap.size_bytes,
                    );
                    match build_resp_res {
                        Ok(resp) => {
                            info!(
                                "find existing snapshot ID={} and name={}",
                                ex_snap.snap_id, snap_name,
                            );
                            (RpcStatusCode::OK, "".to_owned(), Some(resp))
                        }
                        Err(e) => (
                            RpcStatusCode::INTERNAL,
                            format!(
                                "failed to build CreateSnapshotResponse at controller ID={}, \
                                    the error is: {}",
                                node_id, e,
                            ),
                            None,
                        ),
                    }
                } else {
                    (
                        RpcStatusCode::ALREADY_EXISTS,
                        format!(
                            "snapshot with the same name={} exists on node ID={} \
                                but of different source volume ID",
                            snap_name, node_id,
                        ),
                        None,
                    )
                }
            }
            None => (
                RpcStatusCode::OK,
                format!("no snapshot with name={} exists", req.get_name()),
                None,
            ),
        }
    }

    /// Build list snapshot response
    fn add_snapshot_to_list_response(
        snap: &DatenLordSnapshot,
    ) -> anyhow::Result<ListSnapshotsResponse> {
        let mut entry = ListSnapshotsResponse_Entry::new();
        entry.mut_snapshot().set_size_bytes(snap.size_bytes);
        entry.mut_snapshot().set_snapshot_id(snap.snap_id.clone());
        entry
            .mut_snapshot()
            .set_source_volume_id(snap.vol_id.clone());
        entry.mut_snapshot().set_creation_time(
            util::generate_proto_timestamp(&snap.creation_time)
                .context("failed to convert to proto timestamp")?,
        );
        entry.mut_snapshot().set_ready_to_use(snap.ready_to_use);

        let mut resp = ListSnapshotsResponse::new();
        resp.set_entries(RepeatedField::from_vec(vec![entry]));
        Ok(resp)
    }

    /// Call worker create volume
    fn worker_create_volume(
        &self,
        req: &CreateVolumeRequest,
    ) -> Result<CreateVolumeResponse, (RpcStatusCode, String)> {
        let topology_requirement = if req.has_accessibility_requirements() {
            Some(req.get_accessibility_requirements())
        } else {
            None
        };
        let worker_node = match self.meta_data.select_node(topology_requirement) {
            Ok(n) => n,
            Err(e) => {
                error!("failed to select a node, the error is: {}", e);
                return Err((
                    RpcStatusCode::INTERNAL,
                    format!("failed to select a node, the error is: {}", e,),
                ));
            }
        };
        let client = MetaData::build_worker_client(&worker_node);
        let create_res = client.worker_create_volume(req);
        match create_res {
            Ok(resp) => Ok(resp),
            Err(err) => {
                let (rsc, msg) = match err {
                    grpcio::Error::RpcFailure(s) => (
                        s.status,
                        if let Some(m) = s.details {
                            m
                        } else {
                            format!(
                                "failed to create volume by worker at {}",
                                worker_node.node_id,
                            )
                        },
                    ),
                    e @ grpcio::Error::Codec(..)
                    | e @ grpcio::Error::CallFailure(..)
                    | e @ grpcio::Error::RpcFinished(..)
                    | e @ grpcio::Error::RemoteStopped
                    | e @ grpcio::Error::ShutdownFailed
                    | e @ grpcio::Error::BindFail(..)
                    | e @ grpcio::Error::QueueShutdown
                    | e @ grpcio::Error::GoogleAuthenticationFailed
                    | e @ grpcio::Error::InvalidMetadata(..) => (
                        RpcStatusCode::INTERNAL,
                        format!("failed to create volume, the error is: {}", e),
                    ),
                };
                Err((rsc, msg))
            }
        }
    }
}

impl Controller for ControllerImpl {
    fn create_volume(
        &mut self,
        ctx: RpcContext,
        req: CreateVolumeRequest,
        sink: UnarySink<CreateVolumeResponse>,
    ) {
        debug!("create_volume request: {:?}", req);
        let rpc_type = ControllerServiceCapability_RPC_Type::CREATE_DELETE_VOLUME;
        if !self.validate_request_capability(rpc_type) {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                format!("unsupported capability {:?}", rpc_type),
            );
        }

        let vol_name = req.get_name();
        if vol_name.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                "name missing in request".to_owned(),
            );
        }

        let req_caps = req.get_volume_capabilities();
        if req_caps.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                "volume capabilities missing in request".to_owned(),
            );
        }

        let access_type_block = req_caps.iter().any(VolumeCapability::has_block);
        let access_mode_multi_writer = req_caps.iter().any(|vc| {
            vc.get_access_mode().get_mode()
                == VolumeCapability_AccessMode_Mode::MULTI_NODE_MULTI_WRITER
                || vc.get_access_mode().get_mode()
                    == VolumeCapability_AccessMode_Mode::MULTI_NODE_SINGLE_WRITER
        });
        if access_type_block {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                "access type block not supported".to_owned(),
            );
        }
        if access_mode_multi_writer {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                "access mode MULTI_NODE_SINGLE_WRITER and \
                        MULTI_NODE_MULTI_WRITER not supported"
                    .to_owned(),
            );
        }

        let volume_size = req.get_capacity_range().get_required_bytes();
        if volume_size > util::MAX_VOLUME_STORAGE_CAPACITY {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::OUT_OF_RANGE,
                format!(
                    "requested size {} exceeds maximum allowed {}",
                    volume_size,
                    util::MAX_VOLUME_STORAGE_CAPACITY
                ),
            );
        }
        let (rpc_status_code, err_msg, resp_opt) = self.find_existing_volume(&req);
        if RpcStatusCode::OK == rpc_status_code {
            if let Some(resp) = resp_opt {
                debug!(
                    "found existing volume ID={} and name={}",
                    resp.get_volume().get_volume_id(),
                    vol_name,
                );
                return util::success(&ctx, sink, resp);
            } else {
                debug!("{}", err_msg);
            }
        } else {
            debug!("{}", err_msg);
            return util::fail(&ctx, sink, rpc_status_code, err_msg);
        }

        match self.worker_create_volume(&req) {
            Ok(resp) => util::success(&ctx, sink, resp),
            Err((rpc_status_code, err_msg)) => {
                debug_assert_ne!(
                    rpc_status_code,
                    RpcStatusCode::OK,
                    "the RpcStatusCode should not be OK when error",
                );
                debug!("{}", err_msg);
                util::fail(&ctx, sink, rpc_status_code, err_msg)
            }
        }
    }

    fn delete_volume(
        &mut self,
        ctx: RpcContext,
        req: DeleteVolumeRequest,
        sink: UnarySink<DeleteVolumeResponse>,
    ) {
        debug!("delete_volume request: {:?}", req);

        // Check arguments
        let vol_id = req.get_volume_id();
        if vol_id.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                "volume ID missing in request".to_owned(),
            );
        }

        let rpc_type = ControllerServiceCapability_RPC_Type::CREATE_DELETE_VOLUME;
        if !self.validate_request_capability(rpc_type) {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                format!("unsupported capability {:?}", rpc_type),
            );
        }

        // Do not return gRPC error when delete failed for idempotency
        let vol_res = self.meta_data.get_volume_by_id(vol_id);
        if let Some(vol) = vol_res {
            let node_res = self.meta_data.get_node_by_id(&vol.node_id);
            if let Some(node) = node_res {
                let client = MetaData::build_worker_client(&node);
                let worker_delete_res = client.worker_delete_volume(&req);
                match worker_delete_res {
                    Ok(_) => info!("successfully deleted volume ID={}", vol_id),
                    Err(e) => {
                        warn!(
                            "failed to delete volume ID={} on node ID={}, the error is: {}",
                            vol_id, vol.node_id, e,
                        );
                    }
                }
            } else {
                warn!("failed to find node ID={} to get work port", vol.node_id);
            }
        } else {
            warn!(
                "failed to find volume ID={} to delete on controller ID={}",
                vol_id,
                self.meta_data.get_node_id(),
            );
        }
        let r = DeleteVolumeResponse::new();
        util::success(&ctx, sink, r)
    }

    fn controller_publish_volume(
        &mut self,
        ctx: RpcContext,
        req: ControllerPublishVolumeRequest,
        sink: UnarySink<ControllerPublishVolumeResponse>,
    ) {
        debug!("controller_publish_volume request: {:?}", req);

        util::fail(&ctx, sink, RpcStatusCode::UNIMPLEMENTED, "".to_owned())
    }

    fn controller_unpublish_volume(
        &mut self,
        ctx: RpcContext,
        req: ControllerUnpublishVolumeRequest,
        sink: UnarySink<ControllerUnpublishVolumeResponse>,
    ) {
        debug!("controller_unpublish_volume request: {:?}", req);

        util::fail(&ctx, sink, RpcStatusCode::UNIMPLEMENTED, "".to_owned())
    }

    fn validate_volume_capabilities(
        &mut self,
        ctx: RpcContext,
        mut req: ValidateVolumeCapabilitiesRequest,
        sink: UnarySink<ValidateVolumeCapabilitiesResponse>,
    ) {
        debug!("validate_volume_capabilities request: {:?}", req);

        let vol_id = req.get_volume_id();
        // Check arguments
        if vol_id.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                "volume ID cannot be empty".to_owned(),
            );
        }
        let vol_caps = req.get_volume_capabilities();
        if vol_caps.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                format!("volume ID={} has no volume capabilities in reqeust", vol_id),
            );
        }

        let vol_res = self.meta_data.get_volume_by_id(vol_id);
        if vol_res.is_none() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::NOT_FOUND,
                format!("failed to find volume ID={}", vol_id),
            );
        }

        for cap in vol_caps {
            if !cap.has_mount() && !cap.has_block() {
                return util::fail(
                    &ctx,
                    sink,
                    RpcStatusCode::INVALID_ARGUMENT,
                    "cannot have neither mount nor block access type undefined".to_owned(),
                );
            }
            if cap.has_block() {
                return util::fail(
                    &ctx,
                    sink,
                    RpcStatusCode::INVALID_ARGUMENT,
                    "access type block is not supported".to_owned(),
                );
            }
            // TODO: a real driver would check the capabilities of the given volume with
            // the set of requested capabilities.
        }

        let mut r = ValidateVolumeCapabilitiesResponse::new();
        r.mut_confirmed()
            .set_volume_context(req.take_volume_context());
        r.mut_confirmed()
            .set_volume_capabilities(req.take_volume_capabilities());
        r.mut_confirmed().set_parameters(req.take_parameters());

        util::success(&ctx, sink, r)
    }

    fn list_volumes(
        &mut self,
        ctx: RpcContext,
        req: ListVolumesRequest,
        sink: UnarySink<ListVolumesResponse>,
    ) {
        debug!("list_volumes request: {:?}", req);

        let rpc_type = ControllerServiceCapability_RPC_Type::LIST_VOLUMES;
        if !self.validate_request_capability(rpc_type) {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                format!("unsupported capability {:?}", rpc_type),
            );
        }

        let max_entries = req.get_max_entries();
        let starting_token = req.get_starting_token();
        let (vol_vec, next_pos) = match self.meta_data.list_volumes(starting_token, max_entries) {
            Ok((vol_vec, next_pos)) => (vol_vec, next_pos),
            Err((rpc_status_code, err_msg)) => {
                debug_assert_ne!(
                    rpc_status_code,
                    RpcStatusCode::OK,
                    "the RpcStatusCode should not be OK when error",
                );
                warn!(
                    "failed to list volumes from starting position={} and \
                        max entries={}, the error is: {}",
                    starting_token, max_entries, err_msg,
                );
                return util::fail(&ctx, sink, rpc_status_code, err_msg);
            }
        };
        let list_size = vol_vec.len();
        let mut r = ListVolumesResponse::new();
        r.set_entries(RepeatedField::from_vec(vol_vec));
        r.set_next_token(next_pos.to_string());
        info!("list volumes size: {}", list_size);
        util::success(&ctx, sink, r)
    }

    fn get_capacity(
        &mut self,
        ctx: RpcContext,
        req: GetCapacityRequest,
        sink: UnarySink<GetCapacityResponse>,
    ) {
        debug!("get_capacity request: {:?}", req);

        util::fail(&ctx, sink, RpcStatusCode::UNIMPLEMENTED, "".to_owned())
    }

    fn controller_get_capabilities(
        &mut self,
        ctx: RpcContext,
        req: ControllerGetCapabilitiesRequest,
        sink: UnarySink<ControllerGetCapabilitiesResponse>,
    ) {
        debug!("controller_get_capabilities request: {:?}", req);

        let mut r = ControllerGetCapabilitiesResponse::new();
        r.set_capabilities(RepeatedField::from_vec(self.caps.clone()));
        util::success(&ctx, sink, r)
    }

    fn create_snapshot(
        &mut self,
        ctx: RpcContext,
        req: CreateSnapshotRequest,
        sink: UnarySink<CreateSnapshotResponse>,
    ) {
        debug!("create_snapshot request: {:?}", req);

        let rpc_type = ControllerServiceCapability_RPC_Type::CREATE_DELETE_SNAPSHOT;
        if !self.validate_request_capability(rpc_type) {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                format!("unsupported capability {:?}", rpc_type,),
            );
        }

        let snap_name = req.get_name();
        if snap_name.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                "name missing in request".to_owned(),
            );
        }
        // Check source volume exists
        let src_vol_id = req.get_source_volume_id();
        if src_vol_id.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                "source volume ID missing in request".to_owned(),
            );
        }

        let (rpc_status_code, err_msg, resp_opt) = self.find_existing_snapshot(&req);
        if RpcStatusCode::OK == rpc_status_code {
            if let Some(resp) = resp_opt {
                info!(
                    "find existing snapshot ID={} and name={}",
                    resp.get_snapshot().get_snapshot_id(),
                    snap_name,
                );
                return util::success(&ctx, sink, resp);
            } else {
                debug!("{}", err_msg);
            }
        } else {
            debug!("{}", err_msg);
            return util::fail(&ctx, sink, rpc_status_code, err_msg);
        }

        match self.meta_data.get_volume_by_id(src_vol_id) {
            Some(src_vol) => {
                let node_res = self.meta_data.get_node_by_id(&src_vol.node_id);
                if let Some(node) = node_res {
                    let client = MetaData::build_worker_client(&node);
                    let create_res = client.worker_create_snapshot(&req);
                    match create_res {
                        Ok(r) => util::success(&ctx, sink, r),
                        Err(e) => util::fail(
                            &ctx,
                            sink,
                            RpcStatusCode::INTERNAL,
                            format!("failed to create snapshot, the error is: {}", e),
                        ),
                    }
                } else {
                    warn!("failed to find node ID={} from etcd", src_vol.node_id);
                }
            }
            None => util::fail(
                &ctx,
                sink,
                RpcStatusCode::INTERNAL,
                format!("failed to find source volume ID={}", src_vol_id),
            ),
        }
    }

    fn delete_snapshot(
        &mut self,
        ctx: RpcContext,
        req: DeleteSnapshotRequest,
        sink: UnarySink<DeleteSnapshotResponse>,
    ) {
        debug!("delete_snapshot request: {:?}", req);

        // Check arguments
        let snap_id = req.get_snapshot_id();
        if snap_id.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                "snapshot ID missing in request".to_owned(),
            );
        }

        let rpc_type = ControllerServiceCapability_RPC_Type::CREATE_DELETE_SNAPSHOT;
        if !self.validate_request_capability(rpc_type) {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                format!("unsupported capability {:?}", rpc_type),
            );
        }

        // Do not return gRPC error when delete failed for idempotency
        let snap_res = self.meta_data.get_snapshot_by_id(snap_id);
        if let Some(snap) = snap_res {
            let node_res = self.meta_data.get_node_by_id(&snap.node_id);
            if let Some(node) = node_res {
                let client = MetaData::build_worker_client(&node);
                let worker_delete_res = client.worker_delete_snapshot(&req);
                match worker_delete_res {
                    Ok(_r) => info!("successfully deleted sanpshot ID={}", snap_id),
                    Err(e) => {
                        error!(
                            "failed to delete snapshot ID={} on node ID={}, the error is: {}",
                            snap_id, snap.node_id, e,
                        );
                    }
                }
            } else {
                warn!("failed to find node ID={} to get work port", snap.node_id);
            }
        } else {
            warn!("failed to find snapshot ID={} to delete", snap_id);
        }

        let r = DeleteSnapshotResponse::new();
        util::success(&ctx, sink, r)
    }

    fn list_snapshots(
        &mut self,
        ctx: RpcContext,
        req: ListSnapshotsRequest,
        sink: UnarySink<ListSnapshotsResponse>,
    ) {
        debug!("list_snapshots request: {:?}", req);

        let rpc_type = ControllerServiceCapability_RPC_Type::LIST_SNAPSHOTS;
        if !self.validate_request_capability(rpc_type) {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                format!("unsupported capability {:?}", rpc_type),
            );
        }

        // case 1: snapshot ID is not empty, return snapshots that match the snapshot id.
        let snap_id = req.get_snapshot_id();
        if !snap_id.is_empty() {
            let snap_res = self.meta_data.get_snapshot_by_id(snap_id);
            if let Some(snap) = snap_res {
                let r = match Self::add_snapshot_to_list_response(&snap) {
                    Ok(resp) => resp,
                    Err(e) => {
                        return util::fail(
                            &ctx,
                            sink,
                            RpcStatusCode::INTERNAL,
                            format!(
                                "failed to generate ListSnapshotsResponse, the error is: {}",
                                e,
                            ),
                        );
                    }
                };

                return util::success(&ctx, sink, r);
            } else {
                warn!("failed to list snapshot ID={}", snap_id);
                let r = ListSnapshotsResponse::new();
                return util::success(&ctx, sink, r);
            }
        }

        // case 2: source volume ID is not empty, return snapshots that match the source volume id.
        let src_volume_id = req.get_source_volume_id();
        if !src_volume_id.is_empty() {
            let snap_res = self.meta_data.get_snapshot_by_src_volume_id(src_volume_id);
            if let Some(snap) = snap_res {
                let r = match Self::add_snapshot_to_list_response(&snap) {
                    Ok(resp) => resp,
                    Err(e) => {
                        return util::fail(
                            &ctx,
                            sink,
                            RpcStatusCode::INTERNAL,
                            format!(
                                "failed to generate ListSnapshotsResponse, the error is: {}",
                                e,
                            ),
                        );
                    }
                };

                return util::success(&ctx, sink, r);
            } else {
                warn!(
                    "failed to list snapshot with source volume ID={}",
                    src_volume_id,
                );
                let r = ListSnapshotsResponse::new();
                return util::success(&ctx, sink, r);
            }
        }

        // case 3: no parameter is set, so return all the snapshots
        let max_entries = req.get_max_entries();
        let starting_token = req.get_starting_token();
        let (snap_vec, next_pos) = match self.meta_data.list_snapshots(starting_token, max_entries)
        {
            Ok((snap_vec, next_pos)) => (snap_vec, next_pos),
            Err((rpc_status_code, err_msg)) => {
                debug_assert_ne!(
                    rpc_status_code,
                    RpcStatusCode::OK,
                    "the RpcStatusCode should not be OK when error",
                );
                warn!(
                    "failed to list snapshots from starting position={}, \
                        max entries={}, the error is: {}",
                    starting_token, max_entries, err_msg,
                );
                return util::fail(&ctx, sink, rpc_status_code, err_msg);
            }
        };

        let mut r = ListSnapshotsResponse::new();
        r.set_entries(RepeatedField::from_vec(snap_vec));
        r.set_next_token(next_pos.to_string());
        util::success(&ctx, sink, r)
    }

    fn controller_expand_volume(
        &mut self,
        ctx: RpcContext,
        req: ControllerExpandVolumeRequest,
        sink: UnarySink<ControllerExpandVolumeResponse>,
    ) {
        debug!("controller_expand_volume request: {:?}", req);

        let vol_id = req.get_volume_id();
        if vol_id.is_empty() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                "volume ID not provided".to_owned(),
            );
        }

        if !req.has_capacity_range() {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::INVALID_ARGUMENT,
                "capacity range not provided".to_owned(),
            );
        }
        let cap_range = req.get_capacity_range();
        let capacity = cap_range.get_required_bytes();
        if capacity > util::MAX_VOLUME_STORAGE_CAPACITY {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::OUT_OF_RANGE,
                format!(
                    "requested capacity {} exceeds maximum allowed {}",
                    capacity,
                    util::MAX_VOLUME_STORAGE_CAPACITY,
                ),
            );
        }

        let vol_res = self.meta_data.get_volume_by_id(vol_id);
        let mut ex_vol = if let Some(v) = vol_res {
            v
        } else {
            return util::fail(
                &ctx,
                sink,
                // Assume not found error
                RpcStatusCode::NOT_FOUND,
                format!("failed to find volume ID={}", vol_id),
            );
        };

        match ex_vol.get_size().cmp(&capacity) {
            Ordering::Less => {
                let expand_res = self.meta_data.expand(&mut ex_vol, capacity);
                if let Err(e) = expand_res {
                    panic!("failed to expand volume ID={}, the error is: {}", vol_id, e,);
                }
            }
            Ordering::Greater => {
                return util::fail(&ctx, sink, RpcStatusCode::INVALID_ARGUMENT, format!(
                        "capacity={} to expand in request is smaller than the size={} of volume ID={}",
                        capacity, ex_vol.get_size(), vol_id,
                    ),);
            }
            Ordering::Equal => {
                debug!("capacity equals to volume size, no need to expand");
            }
        }

        let mut r = ControllerExpandVolumeResponse::new();
        r.set_capacity_bytes(capacity);
        r.set_node_expansion_required(true);
        util::success(&ctx, sink, r)
    }

    fn controller_get_volume(
        &mut self,
        ctx: RpcContext,
        req: ControllerGetVolumeRequest,
        sink: UnarySink<ControllerGetVolumeResponse>,
    ) {
        debug!("controller_get_volume request: {:?}", req);

        util::fail(&ctx, sink, RpcStatusCode::UNIMPLEMENTED, "".to_owned())
    }
}
