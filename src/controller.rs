//! The implementation for CSI controller service

use grpcio::*;
use log::{debug, error, info};
use protobuf::RepeatedField;
use std::cmp::Ordering;
use std::sync::Arc;

use super::csi::*;
use super::csi_grpc::Controller;
use super::meta_data::{util, MetaData, VolumeSource};

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
        // A real driver would also need to check that the other
        // fields in VolumeCapabilities are sane. The check above is
        // just enough to pass the "[Testpattern: Dynamic PV (block
        // volmode)] volumeMode should fail in binding dynamic
        // provisioned PV to PVC" storage E2E test.
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
        {
            // Need to check for already existing volume name, and if found
            // check for the requested capacity and already allocated capacity
            let get_vol_res = self.meta_data.get_volume_by_name(vol_name);
            match get_vol_res {
                Ok(ex_vol) => {
                    // It means the volume with the same name already exists
                    // need to check if the size of existing volume is the same as in new
                    // request
                    if ex_vol.get_size() != volume_size {
                        return util::fail(
                            &ctx,
                            sink,
                            RpcStatusCode::ALREADY_EXISTS,
                            format!(
                                "volume with the same name={} already exist but with different size",
                                vol_name,
                            ),
                        );
                    }

                    if req.has_volume_content_source() {
                        let ex_vol_content_source = if let Some(vcs) = &ex_vol.content_source {
                            vcs
                        } else {
                            return util::fail(
                                &ctx,
                                sink,
                                RpcStatusCode::ALREADY_EXISTS,
                                format!(
                                    "existing volume ID={} doesn't have content source",
                                    ex_vol.vol_id
                                ),
                            );
                        };

                        let volume_source = req.get_volume_content_source();
                        if let Some(volume_source_type) = &volume_source.field_type {
                            match volume_source_type {
                                VolumeContentSource_oneof_type::snapshot(snapshot_source) => {
                                    let parent_snap_id = snapshot_source.get_snapshot_id();
                                    if let VolumeSource::Snapshot(psid) = ex_vol_content_source {
                                        if psid != parent_snap_id {
                                            return util::fail(
                                                &ctx,
                                                sink,
                                                RpcStatusCode::ALREADY_EXISTS,
                                                format!(
                                                    "existing volume ID={} has parent snapshot ID={}, \
                                                    but VolumeContentSource_SnapshotSource has \
                                                    parent snapshot ID={}",
                                                    ex_vol.vol_id, psid, parent_snap_id,
                                                ),
                                            );
                                        }
                                    } else {
                                        return util::fail(
                                            &ctx,
                                            sink,
                                            RpcStatusCode::ALREADY_EXISTS,
                                            format!(
                                                "existing volume ID={} doesn't have parent snapshot ID",
                                                ex_vol.vol_id
                                            ),
                                        );
                                    }
                                }
                                VolumeContentSource_oneof_type::volume(volume_source) => {
                                    let parent_vol_id = volume_source.get_volume_id();
                                    if let VolumeSource::Volume(pvid) = ex_vol_content_source {
                                        if pvid != parent_vol_id {
                                            return util::fail(
                                                &ctx,
                                                sink,
                                                RpcStatusCode::ALREADY_EXISTS,
                                                format!(
                                                    "existing volume ID={} has parent volume ID={}, \
                                                    but VolumeContentSource_VolumeSource has \
                                                    parent volume ID={}",
                                                    ex_vol.vol_id, pvid, parent_vol_id,
                                                ),
                                            );
                                        }
                                    } else {
                                        return util::fail(
                                            &ctx,
                                            sink,
                                            RpcStatusCode::ALREADY_EXISTS,
                                            format!(
                                                "existing volume ID={} doesn't have parent volume ID",
                                                ex_vol.vol_id
                                            ),
                                        );
                                    }
                                }
                            }
                        }
                    }
                    // Return existing volume
                    // TODO: make sure that volume still exists?
                    let r =
                        util::build_create_volume_response(&req, &ex_vol.vol_id, &ex_vol.node_id);
                    return util::success(&ctx, sink, r);
                }
                Err(e) => debug!("no volume name={} exists, the error is: {}", vol_name, e,),
            }
        }

        let topology_requirement = if req.has_accessibility_requirements() {
            Some(req.get_accessibility_requirements())
        } else {
            None
        };
        let worker_node = match self.meta_data.select_node(topology_requirement) {
            Ok(n) => n,
            Err(e) => {
                error!("failed to select a node, the error is: {}", e);

                return util::fail(
                    &ctx,
                    sink,
                    RpcStatusCode::INTERNAL,
                    format!("failed to select a node, the error is: {}", e,),
                );
            }
        };
        let client = self.meta_data.build_worker_client(&worker_node.node_id);
        let create_res = client.worker_create_volume(&req);
        match create_res {
            Ok(r) => util::success(&ctx, sink, r),
            Err(err) => {
                let (rsc, msg) = match err {
                    Error::RpcFailure(s) => (
                        s.status,
                        match s.details {
                            Some(m) => m,
                            None => format!(
                                "failed to create volume by worker at {}",
                                worker_node.node_id,
                            ),
                        },
                    ),
                    e => (
                        RpcStatusCode::INTERNAL,
                        format!("failed to create volume, the error is: {}", e),
                    ),
                };
                util::fail(&ctx, sink, rsc, msg)
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
        match vol_res {
            Ok(vol) => {
                let client = self.meta_data.build_worker_client(&vol.node_id);
                let worker_delete_res = client.worker_delete_volume(&req);
                match worker_delete_res {
                    Ok(_) => info!("successfully deleted volume ID={}", vol_id),
                    Err(e) => {
                        error!(
                            "failed to delete volume ID={} on node ID={}, the error is: {}",
                            vol_id, vol.node_id, e,
                        );
                    }
                }
            }
            Err(e) => {
                error!(
                    "failed to find volume ID={} to delete on controller ID={}, the error is: {}",
                    vol_id,
                    self.meta_data.get_node_id(),
                    e,
                );
            }
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
        if let Err(e) = vol_res {
            return util::fail(
                &ctx,
                sink,
                RpcStatusCode::NOT_FOUND,
                format!("failed to find volume ID={}, the error is: {}", vol_id, e),
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
            // A real driver would check the capabilities of the given volume with
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
        // let num_vols = self.meta_data.get_num_of_volumes();
        // let starting_pos = if starting_token.is_empty() {
        //     0
        // } else if let Ok(i) = starting_token.parse::<usize>() {
        //     i
        // } else {
        //     return util::fail(
        //         &ctx,
        //         sink,
        //         RpcStatusCode::ABORTED,
        //         format!("invalid starting position {}", starting_token),
        //     );
        // };
        // if starting_pos > 0 && starting_pos >= num_vols {
        //     return util::fail(&ctx, sink,
        //         RpcStatusCode::ABORTED,
        //         format!(
        //             "invalid starting token={}, larger than or equal to the list size={} of volumes",
        //             starting_token,
        //             num_vols,
        //         ),
        //     );
        // }
        // let (remaining, ofr) = num_vols.overflowing_sub(starting_pos);
        // debug_assert!(
        //     !ofr,
        //     "num_vols={} subtract num_to_list={} overflowed",
        //     num_vols, starting_pos,
        // );

        // let num_to_list = if max_entries > 0 {
        //     if remaining < max_entries.cast() {
        //         remaining
        //     } else {
        //         max_entries.cast()
        //     }
        // } else {
        //     remaining
        // };
        // let (next_pos, ofnp) = starting_pos.overflowing_add(num_to_list);
        // debug_assert!(
        //     !ofnp,
        //     "sarting_pos={} add num_to_list={} overflowed",
        //     starting_pos, num_to_list,
        // );

        let (vol_vec, next_pos) = match self.meta_data.list_volumes(starting_token, max_entries) {
            Ok((rpc_status_code, err_msg, vol_vec, next_pos)) => {
                if RpcStatusCode::OK == rpc_status_code {
                    (vol_vec, next_pos)
                } else {
                    return util::fail(&ctx, sink, rpc_status_code, err_msg);
                }
            }
            Err(e) => {
                error!(
                    "failed to list volumes from starting position={} and \
                        max entries={}, the error is: {}",
                    starting_token, max_entries, e,
                );
                (Vec::new(), 0) // Empty result list, and next_pos
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
        let node_id = self.meta_data.get_node_id();
        {
            // Need to check for already existing snapshot name, and if found check for the
            // requested sourceVolumeId and sourceVolumeId of snapshot that has been created
            match self.meta_data.get_snapshot_by_name(req.get_name()) {
                Ok(ex_snap) => {
                    // The snapshot with the same name already exists need to check
                    // if the source volume ID of existing snapshot is the same as in new request
                    if ex_snap.vol_id == src_vol_id {
                        let build_resp_res = util::build_create_snapshot_response(
                            &req,
                            &ex_snap.snap_id,
                            &ex_snap.creation_time,
                            ex_snap.size_bytes,
                        );
                        match build_resp_res {
                            Ok(r) => {
                                info!(
                                    "find existing snapshot ID={} and name={}",
                                    ex_snap.snap_id, snap_name,
                                );
                                return util::success(&ctx, sink, r);
                            }
                            Err(e) => {
                                return util::fail(
                                    &ctx,
                                    sink,
                                    RpcStatusCode::INTERNAL,
                                    format!(
                                        "failed to build CreateSnapshotResponse at controller ID={}, \
                                            the error is: {}",
                                        node_id, e,
                                    ),
                                );
                            }
                        }
                    } else {
                        return util::fail(
                            &ctx,
                            sink,
                            RpcStatusCode::ALREADY_EXISTS,
                            format!(
                                "snapshot with the same name={} exists on node ID={} \
                                    but of different source volume ID",
                                snap_name, node_id,
                            ),
                        );
                    }
                }
                Err(e) => debug!(
                    "no snapshot with name={} exists, the error is: {}",
                    req.get_name(),
                    e,
                ),
            }
        }

        match self.meta_data.get_volume_by_id(src_vol_id) {
            Ok(src_vol) => {
                let client = self.meta_data.build_worker_client(&src_vol.node_id);
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
            }
            Err(e) => util::fail(
                &ctx,
                sink,
                RpcStatusCode::INTERNAL,
                format!(
                    "failed to find source volume ID={}, the error is: {}",
                    src_vol_id, e,
                ),
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
        match snap_res {
            Ok(snap) => {
                let client = self.meta_data.build_worker_client(&snap.node_id);
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
            }
            Err(e) => {
                error!(
                    "failed to find snapshot ID={} to delete, the error is: {}",
                    snap_id, e,
                );
            }
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
            match snap_res {
                Ok(snap) => {
                    let r = match util::add_snapshot_to_list_response(&snap) {
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
                }
                Err(e) => {
                    error!(
                        "failed to list snapshot ID={}, the error is: {}",
                        snap_id, e,
                    );
                    let r = ListSnapshotsResponse::new();
                    return util::success(&ctx, sink, r);
                }
            }
        }

        // case 2: source volume ID is not empty, return snapshots that match the source volume id.
        let src_volume_id = req.get_source_volume_id();
        if !src_volume_id.is_empty() {
            let snap_res = self.meta_data.get_snapshot_by_src_volume_id(src_volume_id);
            match snap_res {
                Ok(snap) => {
                    let r = match util::add_snapshot_to_list_response(&snap) {
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
                }
                Err(e) => {
                    error!(
                        "failed to list snapshot with source volume ID={}, the error is: {}",
                        src_volume_id, e,
                    );
                    let r = ListSnapshotsResponse::new();
                    return util::success(&ctx, sink, r);
                }
            }
        }

        // case 3: no parameter is set, so return all the snapshots
        let max_entries = req.get_max_entries();
        let starting_token = req.get_starting_token();
        // let num_snaps = self.meta_data.get_num_of_snapshots();
        // let starting_pos = if starting_token.is_empty() {
        //     0
        // } else if let Ok(i) = starting_token.parse::<usize>() {
        //     i
        // } else {
        //     return util::fail(
        //         &ctx,
        //         sink,
        //         RpcStatusCode::ABORTED,
        //         format!("invalid starting position {}", starting_token),
        //     );
        // };
        // if starting_pos > 0 && starting_pos >= num_snaps {
        //     return util::fail(&ctx, sink, RpcStatusCode::ABORTED, format!(
        //             "invalid starting token={}, larger than or equal to the list size={} of snapshots",
        //             starting_token,
        //             num_snaps,
        //         ),);
        // }
        // let remaining = num_snaps - starting_pos;
        // let num_to_list = if max_entries > 0 {
        //     if remaining < max_entries.cast() {
        //         remaining
        //     } else {
        //         max_entries.cast()
        //     }
        // } else {
        //     remaining
        // };
        // let next_pos = starting_pos + num_to_list;
        let (snap_vec, next_pos) = match self.meta_data.list_snapshots(starting_token, max_entries)
        {
            Ok((rpc_status_code, err_msg, snap_vec, next_pos)) => {
                if RpcStatusCode::OK == rpc_status_code {
                    (snap_vec, next_pos)
                } else {
                    return util::fail(&ctx, sink, rpc_status_code, err_msg);
                }
            }
            Err(e) => {
                error!(
                    "failed to list snapshots from starting position={}, \
                        max entries={}, the error is: {}",
                    starting_token, max_entries, e,
                );
                (Vec::new(), 0) // Empty result list and next_pos
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
        let mut ex_vol = match vol_res {
            Ok(v) => v,
            Err(e) => {
                return util::fail(
                    &ctx,
                    sink,
                    // Assume not found error
                    RpcStatusCode::NOT_FOUND,
                    format!("failed to find volume ID={}, the error is: {}", vol_id, e,),
                );
            }
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
