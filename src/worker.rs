//! The implementation for `DatenLord` worker service

use grpcio::*;
use log::{debug, error, info};
use std::sync::Arc;
use uuid::Uuid;

use super::csi::*;
use super::datenlord_worker_grpc::*;
use super::meta_data::{util, DatenLordVolume, MetaData, VolumeSource};

/// for `DatenLord` worker implementation
#[derive(Clone)]
pub struct WorkerImpl {
    /// Volume meta data for this worker
    meta_data: Arc<MetaData>,
}

impl WorkerImpl {
    /// Create `WorkerImpl`
    pub fn new(meta_data: Arc<MetaData>) -> Self {
        Self { meta_data }
    }
}

impl Worker for WorkerImpl {
    fn worker_create_volume(
        &mut self,
        ctx: RpcContext,
        req: CreateVolumeRequest,
        sink: UnarySink<CreateVolumeResponse>,
    ) {
        debug!("worker create_volume request: {:?}", req);

        let vol_id = Uuid::new_v4();
        let vol_id_str = vol_id.to_string();
        let vol_name = req.get_name();
        let vol_size = req.get_capacity_range().get_required_bytes();

        let vol_res = DatenLordVolume::build_from_create_volume_req(
            &req,
            &vol_id_str,
            self.meta_data.get_node_id(),
            &self.meta_data.get_volume_path(&vol_id_str),
        );
        let volume =
            match vol_res {
                Ok(v) => v,
                Err(e) => {
                    return util::fail(
                        &ctx,
                        sink,
                        RpcStatusCode::INTERNAL,
                        format!(
                        "failed to create volume ID={} and name={} on node ID={}, the errir is: {}",
                        vol_id, vol_name, self.meta_data.get_node_id(), e,
                    ),
                    );
                }
            };
        if let Some(content_source) = &volume.content_source {
            match content_source {
                VolumeSource::Snapshot(source_snapshot_id) => {
                    let (rpc_status_code, msg) = self.meta_data.copy_volume_from_snapshot(
                        vol_size,
                        source_snapshot_id,
                        &vol_id.to_string(),
                    );
                    if RpcStatusCode::OK == rpc_status_code {
                        info!(
                            "successfully populated volume ID={} and name={} \
                                from source snapshot ID={} on node ID={}",
                            vol_id,
                            vol_name,
                            source_snapshot_id,
                            self.meta_data.get_node_id(),
                        );
                    } else {
                        error!(
                            "failed to populate volume ID={} and name={} from source snapshot ID={}, \
                                the error is: {}",
                            vol_id,
                            vol_name,
                            source_snapshot_id,
                            msg,
                        );
                        return util::fail(&ctx, sink, rpc_status_code, msg);
                    }
                }
                VolumeSource::Volume(source_volume_id) => {
                    let (rpc_status_code, msg) = self.meta_data.copy_volume_from_volume(
                        vol_size,
                        source_volume_id,
                        &vol_id.to_string(),
                    );
                    if let RpcStatusCode::OK = rpc_status_code {
                        info!(
                            "successfully populated volume ID={} and name={} \
                                from source volume ID={} on node ID={}",
                            vol_id,
                            vol_name,
                            source_volume_id,
                            self.meta_data.get_node_id(),
                        );
                    } else {
                        error!(
                            "failed to populate volume ID={} and name={} \
                                from source volume ID={} on node ID={} \
                                the error is: {}",
                            vol_id,
                            vol_name,
                            source_volume_id,
                            self.meta_data.get_node_id(),
                            msg,
                        );
                        return util::fail(&ctx, sink, rpc_status_code, msg);
                    }
                }
            }
        }

        info!(
            "created volume ID={} and name={} on node ID={:?}",
            volume.vol_id,
            vol_name,
            self.meta_data.get_node_id(),
        );
        let add_res = self
            .meta_data
            .add_volume_meta_data(volume.vol_id.to_owned(), &volume);
        debug_assert!(
            add_res.is_ok(),
            "volume with the same ID={} exists on node ID={}, impossible case",
            vol_id,
            self.meta_data.get_node_id(),
        );

        let r = util::build_create_volume_response(
            &req,
            &vol_id.to_string(),
            self.meta_data.get_node_id(),
        );
        util::success(&ctx, sink, r)
    }

    fn worker_delete_volume(
        &mut self,
        ctx: RpcContext,
        req: DeleteVolumeRequest,
        sink: UnarySink<DeleteVolumeResponse>,
    ) {
        debug!("worker delete_volume request: {:?}", req);

        let vol_id = req.get_volume_id();
        let delete_res = self.meta_data.delete_volume_meta_data(vol_id);
        match delete_res {
            Ok(_vol) => {
                // Volume will be dropped here
                debug!(
                    "successfully delete volume ID={} on node ID={}",
                    vol_id,
                    self.meta_data.get_node_id(),
                );
                let r = DeleteVolumeResponse::new();
                util::success(&ctx, sink, r);
            }
            Err(e) => {
                util::fail(
                    &ctx,
                    sink,
                    RpcStatusCode::NOT_FOUND,
                    format!(
                        "failed to find the volume ID={} to delete on node ID={}, the error is: {}",
                        vol_id,
                        self.meta_data.get_node_id(),
                        e,
                    ),
                );
            }
        }
    }

    fn worker_create_snapshot(
        &mut self,
        ctx: RpcContext,
        req: CreateSnapshotRequest,
        sink: UnarySink<CreateSnapshotResponse>,
    ) {
        debug!("worker create_snapshot request: {:?}", req);

        let snap_id = Uuid::new_v4();
        let snap_id_str = snap_id.to_string();
        let snap_name = req.get_name();
        let src_volume_id = req.get_source_volume_id();
        let node_id = self.meta_data.get_node_id();

        let build_snap_res =
            self.meta_data
                .build_snapshot_from_volume(src_volume_id, &snap_id_str, snap_name);
        match build_snap_res {
            Ok(snapshot) => {
                let build_resp_res = util::build_create_snapshot_response(
                    &req,
                    &snap_id_str,
                    &snapshot.creation_time,
                    snapshot.size_bytes,
                );
                match build_resp_res {
                    Ok(r) => {
                        let add_res = self
                            .meta_data
                            .add_snapshot_meta_data(snap_id_str, &snapshot);
                        debug_assert!(
                            add_res.is_ok(),
                            "snapshot with the same ID={} exists on node ID={}, impossible case",
                            snap_id,
                            node_id,
                        );
                        info!(
                            "create snapshot ID={} and name={} on node ID={}",
                            snap_id,
                            req.get_name(),
                            node_id,
                        );
                        util::success(&ctx, sink, r)
                    }
                    Err(e) => util::fail(
                        &ctx,
                        sink,
                        RpcStatusCode::INTERNAL,
                        format!(
                            "failed to build CreateSnapshotResponse on node ID={}, \
                                the error is: {}",
                            node_id, e,
                        ),
                    ),
                }
            }
            Err(e) => util::fail(
                &ctx,
                sink,
                RpcStatusCode::INTERNAL,
                format!(
                    "failed to create snapshot ID={} on node ID={}, the error is: {}",
                    snap_id_str, node_id, e,
                ),
            ),
        }
    }

    fn worker_delete_snapshot(
        &mut self,
        ctx: RpcContext,
        req: DeleteSnapshotRequest,
        sink: UnarySink<DeleteSnapshotResponse>,
    ) {
        debug!("worker delete_snapshot request: {:?}", req);
        let snap_id = req.get_snapshot_id();
        let delete_res = self.meta_data.delete_snapshot_meta_data(snap_id);
        match delete_res {
            Ok(_) => {
                // Snapshot will be dropped here
                debug!(
                    "successfully delete snapshot ID={} on node ID={}",
                    snap_id,
                    self.meta_data.get_node_id(),
                );
                let r = DeleteSnapshotResponse::new();
                util::success(&ctx, sink, r);
            }
            Err(e) => {
                util::fail(
                    &ctx,
                    sink,
                    RpcStatusCode::NOT_FOUND,
                    format!(
                        "failed to find the snapshot ID={} to delete on node ID={}, the error is: {}",
                        snap_id,
                        self.meta_data.get_node_id(),
                        e,
                    ),
                );
            }
        }
    }
}
