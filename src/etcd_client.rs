//! The etcd client implementation

use anyhow::anyhow;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;

use super::util;

/// The client to communicate with etcd
pub struct EtcdClient {
    /// The inner etcd client
    etcd_rs_client: etcd_rs::Client,
}

impl EtcdClient {
    /// Build etcd client
    pub fn new(etcd_address_vec: Vec<String>) -> anyhow::Result<Self> {
        let etcd_rs_client = smol::run(async move {
            etcd_rs::Client::connect(etcd_rs::ClientConfig {
                endpoints: etcd_address_vec.clone(),
                auth: None,
            })
            .await
            .map_err(|e| {
                anyhow!(format!(
                    "failed to build etcd client to {:?}, the error is: {}",
                    etcd_address_vec, e,
                ))
            })
        })?;
        Ok(Self { etcd_rs_client })
    }

    /// Get key-value list from etcd
    async fn get_list_async<T: DeserializeOwned>(&self, prefix: &str) -> anyhow::Result<Vec<T>> {
        let req = etcd_rs::RangeRequest::new(etcd_rs::KeyRange::prefix(prefix));
        let mut resp =
            self.etcd_rs_client.kv().range(req).await.map_err(|e| {
                anyhow!("failed to get RangeResponse from etcd, the error is: {}", e)
            })?;
        let mut result_vec = Vec::with_capacity(resp.count());
        for kv in resp.take_kvs() {
            let decoded_value: T = util::decode_from_bytes(kv.value())?;
            result_vec.push(decoded_value);
        }
        Ok(result_vec)
    }

    /// Write a key value pair to etcd
    async fn write_to_etcd<T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
        value: &T,
    ) -> anyhow::Result<Option<T>> {
        let bin_value = bincode::serialize(value).map_err(|e| {
            anyhow!(
                "failed to encode {:?} to binary, the error is: {}",
                value,
                e,
            )
        })?;
        let mut req = etcd_rs::PutRequest::new(key, bin_value);
        req.set_prev_kv(true); // Return previous value
        let mut resp = self
            .etcd_rs_client
            .kv()
            .put(req)
            .await
            .map_err(|e| anyhow!("failed to get PutResponse from etcd, the error is: {}", e))?;
        if let Some(pre_kv) = resp.take_prev_kv() {
            let decoded_value: T = util::decode_from_bytes(pre_kv.value())?;
            Ok(Some(decoded_value))
        } else {
            Ok(None)
        }
    }

    /// Delete a key value pair or nothing from etcd
    async fn delete_from_etcd<T: DeserializeOwned + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
    ) -> anyhow::Result<Vec<T>> {
        let mut req = etcd_rs::DeleteRequest::new(etcd_rs::KeyRange::key(key));
        req.set_prev_kv(true);
        let mut resp = self.etcd_rs_client.kv().delete(req).await.map_err(|e| {
            anyhow!(
                "failed to get DeleteResponse from etcd, the error is: {}",
                e,
            )
        })?;

        if resp.has_prev_kvs() {
            let deleted_value_list = resp.take_prev_kvs();

            let mut result_vec = Vec::with_capacity(deleted_value_list.len());
            for kv in deleted_value_list {
                let decoded_value: T = util::decode_from_bytes(kv.value())?;
                result_vec.push(decoded_value);
            }
            Ok(result_vec)
        } else {
            Ok(Vec::new())
        }
    }

    /// Get key-value list from etcd
    pub fn get_list<T: DeserializeOwned>(&self, prefix: &str) -> anyhow::Result<Vec<T>> {
        smol::run(async move { self.get_list_async(prefix).await })
    }

    /// Get zero or one key-value pair from etcd
    pub fn get_at_most_one_value<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> anyhow::Result<Option<T>> {
        let mut value_list = smol::run(async move { self.get_list_async(key).await })?;
        debug_assert!(
            value_list.len() <= 1,
            "failed to get zero or one key={} from etcd, but get {} values",
            key,
            value_list.len(),
        );
        Ok(value_list.pop())
    }

    /// Update a existing key value pair to etcd
    pub fn update_existing_kv<T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
        value: &T,
    ) -> anyhow::Result<T> {
        let write_res = smol::run(async move { self.write_to_etcd(key, value).await })?;
        if let Some(pre_value) = write_res {
            Ok(pre_value)
        } else {
            panic!("failed to replace previous value, return nothing",);
        }
    }

    /// Write a existing key value pair to etcd
    pub fn write_new_kv<T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
        value: &T,
    ) -> anyhow::Result<()> {
        let write_res = smol::run(async move { self.write_to_etcd(key, value).await })?;
        if let Some(pre_value) = write_res {
            panic!(
                "failed to write new key vaule pair, the key={} exists in etcd, \
                    the previous value={:?}",
                key, pre_value,
            );
        } else {
            Ok(())
        }
    }

    /// Delete an existing key value pair from etcd
    pub fn delete_exact_one_value<T: DeserializeOwned + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
    ) -> anyhow::Result<T> {
        let mut deleted_value_vec = smol::run(async move { self.delete_from_etcd(key).await })?;
        debug_assert_eq!(
            deleted_value_vec.len(),
            1,
            "delete {} key value pairs for a single key={}, should delete exactly one",
            deleted_value_vec.len(),
            key,
        );
        let deleted_kv = if let Some(kv) = deleted_value_vec.pop() {
            kv
        } else {
            panic!("failed to get the exactly one deleted key value pair")
        };
        Ok(deleted_kv)
    }

    /// Delete all key value pairs from etcd
    #[allow(dead_code)]
    pub fn delete_all(&self) -> anyhow::Result<()> {
        let mut req = etcd_rs::DeleteRequest::new(etcd_rs::KeyRange::all());
        req.set_prev_kv(false);
        smol::run(async move {
            self.etcd_rs_client
                .kv()
                .delete(req)
                .await
                .map_err(|e| anyhow!("failed to clear all data from etcd, the error is: {}", e,))
        })?;
        Ok(())
    }
}
