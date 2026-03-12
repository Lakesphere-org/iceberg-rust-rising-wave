// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashSet;
use std::sync::Arc;

use futures::channel::mpsc::Sender;
use futures::{SinkExt, TryFutureExt};
use itertools::Itertools;
use tracing::debug;
use crate::delete_file_index::DeleteFileIndex;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::io::object_cache::ObjectCache;
use crate::scan::{
    BoundPredicates, DeleteFileContext, ExpressionEvaluatorCache, FileScanTask,
    ManifestEvaluatorCache, PartitionFilterCache,
};
use crate::spec::{
    DataContentType, ManifestContentType, ManifestEntryRef, ManifestFile, ManifestList,
    ManifestStatus, Operation, SchemaRef, SnapshotRef, TableMetadataRef,
};
use crate::utils::ancestors_between;
use crate::{Error, ErrorKind, Result};

type ManifestEntryFilterFn = dyn Fn(&ManifestEntryRef) -> bool + Send + Sync;
/// Wraps a [`ManifestFile`] alongside the objects that are needed
/// to process it in a thread-safe manner
pub(crate) struct ManifestFileContext {
    manifest_file: ManifestFile,

    sender: Sender<ManifestEntryContext>,

    field_ids: Arc<Vec<i32>>,
    bound_predicates: Option<Arc<BoundPredicates>>,
    object_cache: Arc<ObjectCache>,
    snapshot_schema: SchemaRef,
    expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,
    delete_file_index: DeleteFileIndex,

    /// filter manifest entries.
    /// Used for different kind of scans, e.g., only scan newly added files without delete files.
    filter_fn: Option<Arc<ManifestEntryFilterFn>>,
}

/// Wraps a [`ManifestFile`] for stream-based processing (no channels).
/// Used by `plan_files_streams()` for true async streaming.
pub(crate) struct ManifestFileContextForStreams {
    pub manifest_file: ManifestFile,
    pub field_ids: Arc<Vec<i32>>,
    pub bound_predicates: Option<Arc<BoundPredicates>>,
    pub object_cache: Arc<ObjectCache>,
    pub snapshot_schema: SchemaRef,
    pub expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,
    pub delete_file_index: DeleteFileIndex,
    pub filter_fn: Option<Arc<ManifestEntryFilterFn>>,
}

/// Wraps a [`ManifestEntryRef`] alongside the objects that are needed
/// to process it in a thread-safe manner
pub(crate) struct ManifestEntryContext {
    pub manifest_entry: ManifestEntryRef,

    pub expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,
    pub field_ids: Arc<Vec<i32>>,
    pub bound_predicates: Option<Arc<BoundPredicates>>,
    pub partition_spec_id: i32,
    pub snapshot_schema: SchemaRef,
    pub delete_file_index: DeleteFileIndex,
}

impl ManifestFileContext {
    /// Consumes this [`ManifestFileContext`], fetching its Manifest from FileIO and then
    /// streaming its constituent [`ManifestEntries`] to the channel provided in the context
    pub(crate) async fn fetch_manifest_and_stream_manifest_entries(self) -> Result<()> {
        let ManifestFileContext {
            object_cache,
            manifest_file,
            bound_predicates,
            snapshot_schema,
            field_ids,
            mut sender,
            expression_evaluator_cache,
            delete_file_index,
            filter_fn,
        } = self;
        let filter_fn = filter_fn.unwrap_or_else(|| Arc::new(|_| true));

        let manifest = object_cache.get_manifest(&manifest_file).await?;

        for manifest_entry in manifest.entries().iter().filter(|e| filter_fn(e)) {
            let manifest_entry_context = ManifestEntryContext {
                // TODO: refactor to avoid the expensive ManifestEntry clone
                manifest_entry: manifest_entry.clone(),
                expression_evaluator_cache: expression_evaluator_cache.clone(),
                field_ids: field_ids.clone(),
                partition_spec_id: manifest_file.partition_spec_id,
                bound_predicates: bound_predicates.clone(),
                snapshot_schema: snapshot_schema.clone(),
                delete_file_index: delete_file_index.clone(),
            };

            sender
                .send(manifest_entry_context)
                .map_err(|_| Error::new(ErrorKind::Unexpected, "mpsc channel SendError"))
                .await?;
        }

        Ok(())
    }

}

impl ManifestFileContextForStreams {
    /// Fetches the manifest and returns a Vec of [`ManifestEntryContext`]s.
    /// This is used by `plan_files_streams()` for true async streaming without channels.
    pub(crate) async fn fetch_manifest_entries_vec(self) -> Result<Vec<ManifestEntryContext>> {
        use std::time::Instant;
        use tracing::info;
        
        let ManifestFileContextForStreams {
            object_cache,
            manifest_file,
            bound_predicates,
            snapshot_schema,
            field_ids,
            expression_evaluator_cache,
            delete_file_index,
            filter_fn,
        } = self;
        let filter_fn = filter_fn.unwrap_or_else(|| Arc::new(|_| true));

        let manifest_path = manifest_file.manifest_path.clone();
        debug!(
            manifest_path = %manifest_path,
            "Starting to load manifest from S3/cache"
        );
        
        let load_start = Instant::now();
        let manifest = object_cache.get_manifest(&manifest_file).await?;
        let load_duration = load_start.elapsed();

        debug!(
            manifest_path = %manifest_path,
            load_ms = load_duration.as_millis(),
            "Loaded manifest from S3/cache"
        );
        
        let partition_spec_id = manifest_file.partition_spec_id;

        // Collect entries into a Vec
        let entries: Vec<ManifestEntryContext> = manifest
            .entries()
            .iter()
            .filter(|e| filter_fn(e))
            .map(|manifest_entry| ManifestEntryContext {
                manifest_entry: manifest_entry.clone(),
                expression_evaluator_cache: expression_evaluator_cache.clone(),
                field_ids: field_ids.clone(),
                partition_spec_id,
                bound_predicates: bound_predicates.clone(),
                snapshot_schema: snapshot_schema.clone(),
                delete_file_index: delete_file_index.clone(),
            })
            .collect();
        
        Ok(entries)
    }
}

impl ManifestEntryContext {
    /// consume this `ManifestEntryContext`, returning a `FileScanTask`
    /// created from it
    pub(crate) async fn into_file_scan_task(self) -> Result<FileScanTask> {
        let deletes = self
            .delete_file_index
            .get_deletes_for_data_file(
                self.manifest_entry.data_file(),
                self.manifest_entry.sequence_number(),
            )
            .await;

        Ok(FileScanTask {
            start: 0,
            length: self.manifest_entry.file_size_in_bytes(),
            record_count: Some(self.manifest_entry.record_count()),

            data_file_path: self.manifest_entry.file_path().to_string(),
            data_file_content: self.manifest_entry.data_file().content_type(),
            data_file_format: self.manifest_entry.file_format(),

            schema: self.snapshot_schema,
            project_field_ids: self.field_ids.to_vec(),
            predicate: self
                .bound_predicates
                .map(|x| x.as_ref().snapshot_bound_predicate.clone()),

            deletes,
            sequence_number: self.manifest_entry.sequence_number().unwrap_or(0),
            equality_ids: self.manifest_entry.data_file().equality_ids(),
            file_size_in_bytes: self.manifest_entry.data_file().file_size_in_bytes(),

            // Include partition data and spec from manifest entry
            partition: Some(self.manifest_entry.data_file.partition.clone()),
            // TODO: Pass actual PartitionSpec through context chain for native flow
            partition_spec: None,
            // TODO: Extract name_mapping from table metadata property "schema.name-mapping.default"
            name_mapping: None,
        })
    }
}

/// PlanContext wraps a [`SnapshotRef`] alongside all the other
/// objects that are required to perform a scan file plan.
#[derive(Debug)]
pub(crate) struct PlanContext {
    pub snapshot: SnapshotRef,

    pub table_metadata: TableMetadataRef,
    pub snapshot_schema: SchemaRef,
    pub case_sensitive: bool,
    pub predicate: Option<Arc<Predicate>>,
    pub snapshot_bound_predicate: Option<Arc<BoundPredicate>>,
    pub object_cache: Arc<ObjectCache>,
    pub field_ids: Arc<Vec<i32>>,

    pub partition_filter_cache: Arc<PartitionFilterCache>,
    pub manifest_evaluator_cache: Arc<ManifestEvaluatorCache>,
    pub expression_evaluator_cache: Arc<ExpressionEvaluatorCache>,

    // for incremental scan.
    // If `to_snapshot_id` is set, it means incremental scan. `from_snapshot_id` can be `None`.
    pub from_snapshot_id: Option<i64>,
    pub to_snapshot_id: Option<i64>,
}

impl PlanContext {
    pub(crate) async fn get_manifest_list(&self) -> Result<Arc<ManifestList>> {
        self.object_cache
            .as_ref()
            .get_manifest_list(&self.snapshot, &self.table_metadata)
            .await
    }

    fn get_partition_filter(&self, manifest_file: &ManifestFile) -> Result<Arc<BoundPredicate>> {
        let partition_spec_id = manifest_file.partition_spec_id;

        let partition_filter = self.partition_filter_cache.get(
            partition_spec_id,
            &self.table_metadata,
            &self.snapshot_schema,
            self.case_sensitive,
            self.predicate
                .as_ref()
                .ok_or(Error::new(
                    ErrorKind::Unexpected,
                    "Expected a predicate but none present",
                ))?
                .as_ref()
                .bind(self.snapshot_schema.clone(), self.case_sensitive)?,
        )?;

        Ok(partition_filter)
    }

    pub(crate) async fn build_manifest_file_contexts(
        &self,
        manifest_list: Arc<ManifestList>,
        tx_data: Sender<ManifestEntryContext>,
        delete_file_idx: DeleteFileIndex,
        delete_file_tx: Sender<ManifestEntryContext>,
    ) -> Result<Box<impl Iterator<Item = Result<ManifestFileContext>> + 'static>> {
        let mut filter_fn: Option<Arc<ManifestEntryFilterFn>> = None;
        let manifest_files = {
            if let Some(to_snapshot_id) = self.to_snapshot_id {
                // Incremental scan mode:
                // Get all added files between two snapshots.
                // - data files in `Append` and `Overwrite` snapshots are included.
                // - delete files are ignored
                // - `Replace` snapshots (e.g., compaction) are ignored.
                //
                // `latest_snapshot_id` is inclusive, `oldest_snapshot_id` is exclusive.

                let snapshots =
                    ancestors_between(&self.table_metadata, to_snapshot_id, self.from_snapshot_id)
                        .filter(|snapshot| {
                            matches!(
                                snapshot.summary().operation,
                                Operation::Append | Operation::Overwrite
                            )
                        })
                        .collect_vec();
                let snapshot_ids: HashSet<i64> = snapshots
                    .iter()
                    .map(|snapshot| snapshot.snapshot_id())
                    .collect();

                let mut manifest_files = vec![];
                for snapshot in snapshots {
                    let manifest_list = self
                        .object_cache
                        .get_manifest_list(&snapshot, &self.table_metadata)
                        .await?;
                    for entry in manifest_list.entries() {
                        if !snapshot_ids.contains(&entry.added_snapshot_id) {
                            continue;
                        }
                        manifest_files.push(entry.clone());
                    }
                }

                filter_fn = Some(Arc::new(move |entry: &ManifestEntryRef| {
                    matches!(entry.status(), ManifestStatus::Added)
                        && matches!(entry.data_file().content_type(), DataContentType::Data)
                        && (
                            // Is it possible that the snapshot id here is not contained?
                            entry.snapshot_id().is_none()
                                || snapshot_ids.contains(&entry.snapshot_id().unwrap())
                        )
                }));

                manifest_files
            } else {
                manifest_list.entries().to_vec()
            }
        };

        // TODO: Ideally we could ditch this intermediate Vec as we return an iterator.
        let mut filtered_deletes_mfcs = vec![];
        let mut filtered_data_mfcs = vec![];
        for manifest_file in &manifest_files {
            let tx = if manifest_file.content == ManifestContentType::Deletes {
                delete_file_tx.clone()
            } else {
                tx_data.clone()
            };

            let partition_bound_predicate = if self.predicate.is_some() {
                let partition_bound_predicate = self.get_partition_filter(manifest_file)?;

                // evaluate the ManifestFile against the partition filter. Skip
                // if it cannot contain any matching rows
                if !self
                    .manifest_evaluator_cache
                    .get(
                        manifest_file.partition_spec_id,
                        partition_bound_predicate.clone(),
                    )
                    .eval(manifest_file)?
                {
                    continue;
                }

                Some(partition_bound_predicate)
            } else {
                None
            };

            let mfc = self.create_manifest_file_context(
                manifest_file,
                partition_bound_predicate,
                tx,
                delete_file_idx.clone(),
                filter_fn.clone(),
            );

            match manifest_file.content {
                ManifestContentType::Deletes => {
                    filtered_deletes_mfcs.push(Ok(mfc));
                }
                ManifestContentType::Data => {
                    filtered_data_mfcs.push(Ok(mfc));
                }
            }
        }

        // Push deletes manifest first then data manifest files.
        Ok(Box::new(
            filtered_deletes_mfcs
                .into_iter()
                .chain(filtered_data_mfcs.into_iter()),
        ))
    }

    fn create_manifest_file_context(
        &self,
        manifest_file: &ManifestFile,
        partition_filter: Option<Arc<BoundPredicate>>,
        sender: Sender<ManifestEntryContext>,
        delete_file_index: DeleteFileIndex,
        filter_fn: Option<Arc<ManifestEntryFilterFn>>,
    ) -> ManifestFileContext {
        let bound_predicates =
            if let (Some(ref partition_bound_predicate), Some(snapshot_bound_predicate)) =
                (partition_filter, &self.snapshot_bound_predicate)
            {
                Some(Arc::new(BoundPredicates {
                    partition_bound_predicate: partition_bound_predicate.as_ref().clone(),
                    snapshot_bound_predicate: snapshot_bound_predicate.as_ref().clone(),
                }))
            } else {
                None
            };

        ManifestFileContext {
            manifest_file: manifest_file.clone(),
            bound_predicates,
            sender,
            object_cache: self.object_cache.clone(),
            snapshot_schema: self.snapshot_schema.clone(),
            field_ids: self.field_ids.clone(),
            expression_evaluator_cache: self.expression_evaluator_cache.clone(),
            delete_file_index,
            filter_fn,
        }
    }

    /// Builds manifest file contexts for stream-based processing (no channels).
    /// Used by `plan_files_streams()`.
    pub(crate) async fn build_manifest_file_contexts_for_streams(
        &self,
        manifest_list: Arc<ManifestList>,
        delete_file_index: DeleteFileIndex,
    ) -> Result<Vec<ManifestFileContextForStreams>> {
        let mut filter_fn: Option<Arc<ManifestEntryFilterFn>> = None;
        let manifest_files = {
            if let Some(to_snapshot_id) = self.to_snapshot_id {
                // Incremental scan mode
                let snapshots =
                    ancestors_between(&self.table_metadata, to_snapshot_id, self.from_snapshot_id)
                        .filter(|snapshot| {
                            matches!(
                                snapshot.summary().operation,
                                Operation::Append | Operation::Overwrite
                            )
                        })
                        .collect_vec();
                let snapshot_ids: HashSet<i64> = snapshots
                    .iter()
                    .map(|snapshot| snapshot.snapshot_id())
                    .collect();

                let mut manifest_files = vec![];
                for snapshot in snapshots {
                    let manifest_list = self
                        .object_cache
                        .get_manifest_list(&snapshot, &self.table_metadata)
                        .await?;
                    for entry in manifest_list.entries() {
                        if !snapshot_ids.contains(&entry.added_snapshot_id) {
                            continue;
                        }
                        manifest_files.push(entry.clone());
                    }
                }

                filter_fn = Some(Arc::new(move |entry: &ManifestEntryRef| {
                    matches!(entry.status(), ManifestStatus::Added)
                        && matches!(entry.data_file().content_type(), DataContentType::Data)
                        && (entry.snapshot_id().is_none()
                            || snapshot_ids.contains(&entry.snapshot_id().unwrap()))
                }));

                manifest_files
            } else {
                manifest_list.entries().to_vec()
            }
        };

        let mut result = Vec::with_capacity(manifest_files.len());

        // Only process data manifests (delete manifests are handled separately)
        for manifest_file in &manifest_files {
            // Skip delete manifests - they've already been processed into delete_file_index
            if manifest_file.content == ManifestContentType::Deletes {
                continue;
            }

            let partition_bound_predicate = if self.predicate.is_some() {
                let partition_bound_predicate = self.get_partition_filter(manifest_file)?;

                // Skip if it cannot contain any matching rows
                if !self
                    .manifest_evaluator_cache
                    .get(
                        manifest_file.partition_spec_id,
                        partition_bound_predicate.clone(),
                    )
                    .eval(manifest_file)?
                {
                    continue;
                }

                Some(partition_bound_predicate)
            } else {
                None
            };

            let mfc = self.create_manifest_file_context_for_streams(
                manifest_file,
                partition_bound_predicate,
                filter_fn.clone(),
                delete_file_index.clone(),
            );

            result.push(mfc);
        }

        Ok(result)
    }

    fn create_manifest_file_context_for_streams(
        &self,
        manifest_file: &ManifestFile,
        partition_filter: Option<Arc<BoundPredicate>>,
        filter_fn: Option<Arc<ManifestEntryFilterFn>>,
        delete_file_index: DeleteFileIndex,
    ) -> ManifestFileContextForStreams {
        let bound_predicates =
            if let (Some(ref partition_bound_predicate), Some(snapshot_bound_predicate)) =
                (partition_filter, &self.snapshot_bound_predicate)
            {
                Some(Arc::new(BoundPredicates {
                    partition_bound_predicate: partition_bound_predicate.as_ref().clone(),
                    snapshot_bound_predicate: snapshot_bound_predicate.as_ref().clone(),
                }))
            } else {
                None
            };

        ManifestFileContextForStreams {
            manifest_file: manifest_file.clone(),
            bound_predicates,
            object_cache: self.object_cache.clone(),
            snapshot_schema: self.snapshot_schema.clone(),
            field_ids: self.field_ids.clone(),
            expression_evaluator_cache: self.expression_evaluator_cache.clone(),
            delete_file_index,
            filter_fn,
        }
    }

    /// Loads delete file contexts from delete manifests.
    /// Used by `plan_files_streams()` to build the delete file index before processing data files.
    pub(crate) async fn load_delete_file_contexts_for_streams(
        &self,
        manifest_list: &ManifestList,
    ) -> Result<Vec<DeleteFileContext>> {
        let mut delete_contexts = Vec::new();

        // In incremental scan mode, delete files are ignored
        if self.to_snapshot_id.is_some() {
            return Ok(delete_contexts);
        }

        for manifest_file in manifest_list.entries() {
            // Only process delete manifests
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }

            let partition_bound_predicate = if self.predicate.is_some() {
                let partition_bound_predicate = self.get_partition_filter(manifest_file)?;

                // Skip if it cannot contain any matching rows
                if !self
                    .manifest_evaluator_cache
                    .get(
                        manifest_file.partition_spec_id,
                        partition_bound_predicate.clone(),
                    )
                    .eval(manifest_file)?
                {
                    continue;
                }

                Some(partition_bound_predicate)
            } else {
                None
            };

            // Load the manifest
            let manifest = self.object_cache.get_manifest(manifest_file).await?;

            for manifest_entry in manifest.entries() {
                // Skip deleted entries
                if !manifest_entry.is_alive() {
                    continue;
                }

                // Skip if not a delete file (shouldn't happen in delete manifest, but be safe)
                if manifest_entry.content_type() == DataContentType::Data {
                    continue;
                }

                // Apply partition filtering if predicate exists
                if let Some(ref partition_filter) = partition_bound_predicate {
                    let expression_evaluator = self.expression_evaluator_cache.get(
                        manifest_file.partition_spec_id,
                        partition_filter,
                    )?;

                    // Skip if partition doesn't match
                    if !expression_evaluator.eval(manifest_entry.data_file())? {
                        continue;
                    }
                }

                delete_contexts.push(DeleteFileContext {
                    manifest_entry: manifest_entry.clone(),
                    partition_spec_id: manifest_file.partition_spec_id,
                    snapshot_schema: self.snapshot_schema.clone(),
                    field_ids: self.field_ids.clone(),
                });
            }
        }

        Ok(delete_contexts)
    }
}
