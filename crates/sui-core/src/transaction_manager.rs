// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use sui_types::{
    base_types::TransactionDigest, committee::EpochId, error::SuiResult,
    messages::VerifiedCertificate,
};
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;

use crate::authority::{authority_store::ObjectKey, AuthorityMetrics, AuthorityStore};

/// TransactionManager is responsible for managing pending certificates and publishes a stream
/// of certificates ready to be executed. It works together with AuthorityState for receiving
/// pending certificates, and getting notified about committed objects. Executing driver
/// subscribes to the stream of ready certificates published by the TransactionManager, and can
/// execute them in parallel.
/// TODO: use TransactionManager for fullnode.
pub(crate) struct TransactionManager {
    authority_store: Arc<AuthorityStore>,
    missing_inputs: BTreeMap<ObjectKey, (EpochId, TransactionDigest)>,
    pending_certificates: BTreeMap<(EpochId, TransactionDigest), BTreeSet<ObjectKey>>,
    tx_ready_certificates: UnboundedSender<VerifiedCertificate>,
    metrics: Arc<AuthorityMetrics>,
}

impl TransactionManager {
    /// If a node restarts, transaction manager recovers in-memory data from pending certificates and
    /// other persistent data.
    pub(crate) async fn new(
        authority_store: Arc<AuthorityStore>,
        tx_ready_certificates: UnboundedSender<VerifiedCertificate>,
        metrics: Arc<AuthorityMetrics>,
    ) -> TransactionManager {
        let mut transaction_manager = TransactionManager {
            authority_store,
            metrics,
            missing_inputs: BTreeMap::new(),
            pending_certificates: BTreeMap::new(),
            tx_ready_certificates,
        };
        transaction_manager
            .add(
                transaction_manager
                    .authority_store
                    .all_pending_certificates()
                    .unwrap(),
            )
            .expect("Initialize TransactionManager with pending certificates failed.");
        transaction_manager
    }

    /// Enqueues certificates into TransactionManager. Once all of the input objects are available
    /// locally for a certificate, the certified transaction will be sent to execution driver.
    ///
    /// This is  a no-op for certificates that are executing or have finished execution.
    /// Takes shared object locks if needed, and persists the pending certificate for crash
    /// recovery.
    pub(crate) async fn enqueue_to_execute(
        &mut self,
        certs: Vec<VerifiedCertificate>,
    ) -> SuiResult<()> {
        for cert in &certs {
            // Skip processing if the certificate is already enqueued, executing or executed.
            if self
                .pending_certificates
                .contains_key(&(cert.epoch(), *cert.digest()))
            {
                continue;
            }
            // It is necessary to check pending certificates before effects in storage, because
            // during a transaction commit, effects are written before or at the same time as
            // cleaning up pending certificates.
            if self
                .authority_store
                .pending_certificate_exists(cert.epoch(), cert.digest())?
            {
                continue;
            }
            if self.authority_store.effects_exists(cert.digest())? {
                continue;
            }

            // Commit the necessary updates to the authority store.
            if cert.contains_shared_object() {
                self.authority_store
                    .record_pending_shared_object_certificate(cert)
                    .await?;
            } else {
                self.authority_store
                    .record_pending_owned_object_certificate(cert)
                    .await?;
            }
        }
        self.add(certs)?;
        Ok(())
    }

    /// Adds the pending certificates into TransactionManager, assuming the necessary persistent
    /// data have been written into pending certificates, and shared locks tables if needed.
    fn add(&mut self, certs: Vec<VerifiedCertificate>) -> SuiResult<()> {
        for cert in certs {
            // Skip processing if the certificate is already enqueued.
            if self
                .pending_certificates
                .contains_key(&(cert.epoch(), *cert.digest()))
            {
                continue;
            }
            let missing = self
                .authority_store
                .get_missing_input_objects(cert.digest(), &cert.data().data.input_objects()?)
                .expect("Are shared object locks set prior to enqueueing certificates?");
            if missing.is_empty() {
                self.certificate_ready(cert);
                continue;
            }
            let cert_key = (cert.epoch(), *cert.digest());
            for obj_key in missing {
                // TODO: verify the key does not already exist.
                self.missing_inputs.insert(obj_key, cert_key);
                self.pending_certificates
                    .entry(cert_key)
                    .or_default()
                    .insert(obj_key);
            }
        }
        self.metrics
            .transaction_manager_num_missing_objects
            .set(self.missing_inputs.len() as i64);
        self.metrics
            .transaction_manager_num_pending_certificates
            .set(self.pending_certificates.len() as i64);
        Ok(())
    }

    /// Notifies TransactionManager that the given objects have been committed.
    pub(crate) fn committed(
        &mut self,
        epoch: EpochId,
        digest: &TransactionDigest,
        object_keys: Vec<ObjectKey>,
    ) {
        for object_key in object_keys {
            let cert_key = if let Some(key) = self.missing_inputs.remove(&object_key) {
                key
            } else {
                continue;
            };
            let set = self.pending_certificates.entry(cert_key).or_default();
            set.remove(&object_key);
            // This certificate has no missing input. It is ready to execute.
            if set.is_empty() {
                self.pending_certificates.remove(&cert_key);
                // NOTE: failing and ignoring the certificate is fine, if it will be retried at a higher level.
                // Otherwise, this has to crash.
                let cert = match self
                    .authority_store
                    .get_pending_certificate(cert_key.0, &cert_key.1)
                {
                    Ok(Some(cert)) => cert,
                    Ok(None) => {
                        error!(tx_digest = ?cert_key,
                            "Ready certificate not found in the pending table",
                        );
                        continue;
                    }
                    Err(e) => {
                        error!(tx_digest = ?cert_key,
                            "Failed to read pending table: {e}",
                        );

                        continue;
                    }
                };
                self.certificate_ready(cert);
            }
        }
        self.metrics
            .transaction_manager_num_missing_objects
            .set(self.missing_inputs.len() as i64);
        self.metrics
            .transaction_manager_num_pending_certificates
            .set(self.pending_certificates.len() as i64);
        // Remove the certificate that finished execution. If this fails, the only possible
        // mitigation is to crash, retry the execution and retry the removal.
        self.authority_store
            .remove_pending_certificate(epoch, digest)
            .expect("Failed to remove {digest} from pending certificates!");
    }

    /// Marks the given certificate as ready to be executed.
    fn certificate_ready(&self, certificate: VerifiedCertificate) {
        self.metrics.transaction_manager_num_ready.inc();
        let _ = self.tx_ready_certificates.send(certificate);
    }
}
