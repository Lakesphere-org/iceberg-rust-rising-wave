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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::future::try_join_all;
use uuid::Uuid;

use super::snapshot::{DefaultManifestProcess, MergeManifestProcess, SnapshotProducer};
use super::{
    MANIFEST_MERGE_ENABLED, MANIFEST_MERGE_ENABLED_DEFAULT, MANIFEST_MIN_MERGE_COUNT,
    MANIFEST_MIN_MERGE_COUNT_DEFAULT, MANIFEST_TARGET_SIZE_BYTES,
    MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
};
use crate::error::Result;
use crate::spec::{
    DataContentType, DataFile, Manifest, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestStatus, Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::SnapshotProduceOperation;
use crate::transaction::{ActionCommit, TransactionAction};

/// Transaction action for rewriting files.
pub struct RewriteFilesAction {
    // snapshot_produce_action: SnapshotProduceAction<'a>,
    target_size_bytes: u32,
    min_count_to_merge: u32,
    merge_enabled: bool,

    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,
    snapshot_id: Option<i64>,
    new_data_file_sequence_number: Option<i64>,
    target_branch: Option<String>,
    enable_delete_filter_manager: bool,
    check_file_existence: bool,
    /// If true, uses concurrent manifest loading for better performance with many manifests.
    /// Default is true.
    use_concurrent_manifest_loading: bool,
}

pub struct RewriteFilesOperation;

impl RewriteFilesAction {
    pub fn new() -> Self {
        Self {
            target_size_bytes: MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
            min_count_to_merge: MANIFEST_MIN_MERGE_COUNT_DEFAULT,
            merge_enabled: MANIFEST_MERGE_ENABLED_DEFAULT,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
            added_data_files: Vec::new(),
            added_delete_files: Vec::new(),
            removed_data_files: Vec::new(),
            removed_delete_files: Vec::new(),
            snapshot_id: None,
            new_data_file_sequence_number: None,
            target_branch: None,
            enable_delete_filter_manager: false,
            check_file_existence: false,
            use_concurrent_manifest_loading: true, // Default to concurrent for better performance
        }
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        for file in data_files {
            match file.content_type() {
                DataContentType::Data => self.added_data_files.push(file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.added_delete_files.push(file)
                }
            }
        }

        self
    }

    /// Add remove files to the snapshot.
    pub fn delete_files(mut self, remove_data_files: impl IntoIterator<Item = DataFile>) -> Self {
        for file in remove_data_files {
            match file.content_type() {
                DataContentType::Data => self.removed_data_files.push(file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.removed_delete_files.push(file)
                }
            }
        }

        self
    }

    pub fn set_snapshot_properties(&mut self, properties: HashMap<String, String>) -> &mut Self {
        let target_size_bytes: u32 = properties
            .get(MANIFEST_TARGET_SIZE_BYTES)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
        let min_count_to_merge: u32 = properties
            .get(MANIFEST_MIN_MERGE_COUNT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MIN_MERGE_COUNT_DEFAULT);
        let merge_enabled = properties
            .get(MANIFEST_MERGE_ENABLED)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MERGE_ENABLED_DEFAULT);

        self.target_size_bytes = target_size_bytes;
        self.min_count_to_merge = min_count_to_merge;
        self.merge_enabled = merge_enabled;
        self.snapshot_properties = properties;

        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(&mut self, commit_uuid: Uuid) -> &mut Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Enable delete filter manager for this snapshot.
    /// By default, delete filter manager is disabled.
    pub fn set_enable_delete_filter_manager(mut self, enable_delete_filter_manager: bool) -> Self {
        self.enable_delete_filter_manager = enable_delete_filter_manager;
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot id
    pub fn set_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    pub fn set_target_branch(mut self, target_branch: String) -> Self {
        self.target_branch = Some(target_branch);
        self
    }

    // If the compaction should use the sequence number of the snapshot at compaction start time for
    // new data files, instead of using the sequence number of the newly produced snapshot.
    // This avoids commit conflicts with updates that add newer equality deletes at a higher sequence number.
    pub fn set_new_data_file_sequence_number(mut self, seq: i64) -> Self {
        self.new_data_file_sequence_number = Some(seq);
        self
    }

    pub fn set_check_file_existence(mut self, check: bool) -> Self {
        self.check_file_existence = check;
        self
    }

    /// Enable or disable concurrent manifest loading.
    ///
    /// When enabled (default), all manifests are loaded in parallel using `try_join_all`,
    /// which provides significant performance improvements for tables with many manifest files.
    ///
    /// Set to `false` to use sequential loading if you encounter issues with concurrent I/O.
    pub fn set_use_concurrent_manifest_loading(mut self, use_concurrent: bool) -> Self {
        self.use_concurrent_manifest_loading = use_concurrent;
        self
    }
}

impl SnapshotProduceOperation for RewriteFilesOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // generate delete manifest entries from removed files
        let snapshot = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch());

        if let Some(snapshot) = snapshot {
            let gen_manifest_entry = |old_entry: &Arc<ManifestEntry>| {
                let builder = ManifestEntry::builder()
                    .status(ManifestStatus::Deleted)
                    .snapshot_id(old_entry.snapshot_id().unwrap())
                    .sequence_number(old_entry.sequence_number().unwrap())
                    .file_sequence_number_opt(old_entry.file_sequence_number())
                    .data_file(old_entry.data_file().clone());

                builder.build()
            };

            let manifest_list = snapshot
                .load_manifest_list(
                    snapshot_produce.table.file_io(),
                    snapshot_produce.table.metadata(),
                )
                .await?;

            let mut deleted_entries = Vec::new();

            for manifest_file in manifest_list.entries() {
                let manifest = manifest_file
                    .load_manifest(snapshot_produce.table.file_io())
                    .await?;

                for entry in manifest.entries() {
                    if entry.content_type() == DataContentType::Data
                        && snapshot_produce
                            .removed_data_file_paths
                            .contains(entry.data_file().file_path())
                    {
                        deleted_entries.push(gen_manifest_entry(entry));
                    }

                    if (entry.content_type() == DataContentType::PositionDeletes
                        || entry.content_type() == DataContentType::EqualityDeletes)
                        && snapshot_produce
                            .removed_delete_file_paths
                            .contains(entry.data_file().file_path())
                    {
                        deleted_entries.push(gen_manifest_entry(entry));
                    }
                }
            }

            Ok(deleted_entries)
        } else {
            Ok(vec![])
        }
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let table_metadata_ref = snapshot_produce.table.metadata();
        let file_io_ref = snapshot_produce.table.file_io();

        let Some(snapshot) = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch())
        else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(file_io_ref, table_metadata_ref)
            .await?;

        let mut existing_files = Vec::new();

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(file_io_ref).await?;

            let found_deleted_files: HashSet<_> = manifest
                .entries()
                .iter()
                .filter_map(|entry| {
                    if snapshot_produce
                        .removed_data_file_paths
                        .contains(entry.data_file().file_path())
                        || snapshot_produce
                            .removed_delete_file_paths
                            .contains(entry.data_file().file_path())
                    {
                        Some(entry.data_file().file_path().to_string())
                    } else {
                        None
                    }
                })
                .collect();

            if found_deleted_files.is_empty() {
                existing_files.push(manifest_file.clone());
            } else {
                // Rewrite the manifest file without the deleted data files
                if manifest
                    .entries()
                    .iter()
                    .any(|entry| !found_deleted_files.contains(entry.data_file().file_path()))
                {
                    let mut manifest_writer = snapshot_produce.new_manifest_writer(
                        ManifestContentType::Data,
                        manifest_file.partition_spec_id,
                    )?;

                    for entry in manifest.entries() {
                        // if !found_deleted_files.contains(entry.data_file().file_path()) {
                        //     manifest_writer.add_entry((**entry).clone())?;
                        // }
                        // 1. Skip files being compacted (this part was correct)
                        if found_deleted_files.contains(entry.data_file().file_path()) {
                            continue;
                        }

                        // 2. NEW: Skip entries that are already DELETED (tombstones)
                        //    This is what Java's liveEntries() does automatically
                        if entry.status() == ManifestStatus::Deleted {
                            continue;
                        }

                        // 3. NEW: Use add_existing_entry() instead of add_entry()
                        //    This preserves the EXISTING status instead of changing to ADDED
                        manifest_writer.add_existing_entry((**entry).clone())?;
                    }

                    existing_files.push(manifest_writer.write_manifest_file().await?);
                }
            }
        }

        Ok(existing_files)
    }
}

/// Concurrent version of RewriteFilesOperation that loads all manifests in parallel.
///
/// This implementation provides significant performance improvements over the sequential
/// `RewriteFilesOperation` by:
/// 1. Loading all manifest files concurrently using `try_join_all`
/// 2. Caching loaded manifests to avoid duplicate I/O between `delete_entries` and `existing_manifest`
/// 3. Using efficient HashSet lookups for file path matching
///
/// Use this for tables with many manifest files where I/O latency is a bottleneck.
pub struct RewriteFilesOperationConcurrent {
    /// Cached manifests loaded during the first operation (delete_entries or existing_manifest)
    /// This avoids loading the same manifests twice.
    cached_manifests: std::sync::OnceLock<Vec<(ManifestFile, Manifest)>>,
}

impl RewriteFilesOperationConcurrent {
    pub fn new() -> Self {
        Self {
            cached_manifests: std::sync::OnceLock::new(),
        }
    }

    /// Load all manifests concurrently from a snapshot.
    ///
    /// Returns a vector of (ManifestFile, Manifest) tuples.
    /// This is the core optimization - loading all manifests in parallel instead of sequentially.
    async fn load_all_manifests_concurrent(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<(ManifestFile, Manifest)>> {
        let snapshot = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch());

        let Some(snapshot) = snapshot else {
            return Ok(vec![]);
        };

        let file_io = snapshot_produce.table.file_io();
        let table_metadata = snapshot_produce.table.metadata();

        let manifest_list = snapshot.load_manifest_list(file_io, table_metadata).await?;

        // Create futures for loading all manifests concurrently
        let manifest_futures: Vec<_> = manifest_list
            .entries()
            .iter()
            .map(|manifest_file| {
                let manifest_file = manifest_file.clone();
                let file_io = file_io.clone();
                async move {
                    let manifest = manifest_file.load_manifest(&file_io).await?;
                    Ok::<_, crate::Error>((manifest_file, manifest))
                }
            })
            .collect();

        // Load all manifests in parallel
        try_join_all(manifest_futures).await
    }

    /// Get or load cached manifests.
    ///
    /// If manifests have already been loaded (by a previous call), returns the cached version.
    /// Otherwise, loads all manifests concurrently and caches them.
    async fn get_or_load_manifests(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<&Vec<(ManifestFile, Manifest)>> {
        // Try to get cached manifests first
        if let Some(cached) = self.cached_manifests.get() {
            return Ok(cached);
        }

        // Load manifests concurrently
        let manifests = self.load_all_manifests_concurrent(snapshot_produce).await?;

        // Cache the results (ignore if another thread already set it)
        let _ = self.cached_manifests.set(manifests);

        // Return the cached value (either ours or the other thread's)
        Ok(self.cached_manifests.get().unwrap())
    }

    /// Generate delete entries from pre-loaded manifests.
    ///
    /// This processes the cached manifests to find entries that match the removed file paths.
    fn generate_delete_entries_from_manifests(
        manifests: &[(ManifestFile, Manifest)],
        removed_data_file_paths: &HashSet<String>,
        removed_delete_file_paths: &HashSet<String>,
    ) -> Vec<ManifestEntry> {
        let gen_manifest_entry = |old_entry: &Arc<ManifestEntry>| {
            ManifestEntry::builder()
                .status(ManifestStatus::Deleted)
                .snapshot_id(old_entry.snapshot_id().unwrap())
                .sequence_number(old_entry.sequence_number().unwrap())
                .file_sequence_number_opt(old_entry.file_sequence_number())
                .data_file(old_entry.data_file().clone())
                .build()
        };

        // Pre-calculate capacity hint
        let total_entries: usize = manifests.iter().map(|(_, m)| m.entries().len()).sum();
        let mut deleted_entries = Vec::with_capacity(total_entries / 10); // Estimate ~10% deleted

        for (_manifest_file, manifest) in manifests {
            for entry in manifest.entries() {
                let file_path = entry.data_file().file_path();

                match entry.content_type() {
                    DataContentType::Data => {
                        if removed_data_file_paths.contains(file_path) {
                            deleted_entries.push(gen_manifest_entry(entry));
                        }
                    }
                    DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                        if removed_delete_file_paths.contains(file_path) {
                            deleted_entries.push(gen_manifest_entry(entry));
                        }
                    }
                }
            }
        }

        deleted_entries
    }
}

impl Default for RewriteFilesOperationConcurrent {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotProduceOperation for RewriteFilesOperationConcurrent {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    /// Generate delete manifest entries from removed files - concurrent version.
    ///
    /// This method is called SECOND in the commit flow (after `existing_manifest`).
    /// Uses cached manifests that were loaded by `existing_manifest`.
    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        let snapshot = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch());

        if snapshot.is_none() {
            return Ok(vec![]);
        }

        // Use cached manifests (populated by existing_manifest which is called first)
        let manifests = self.get_or_load_manifests(snapshot_produce).await?;

        // Generate delete entries from the loaded manifests
        let deleted_entries = Self::generate_delete_entries_from_manifests(
            manifests,
            &snapshot_produce.removed_data_file_paths,
            &snapshot_produce.removed_delete_file_paths,
        );

        Ok(deleted_entries)
    }

    /// Get existing manifests, rewriting those that contain deleted files - concurrent version.
    ///
    /// This method is called FIRST in the commit flow. It loads all manifests concurrently
    /// and caches them for use by `delete_entries` (called second).
    async fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(_snapshot) = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch())
        else {
            return Ok(vec![]);
        };

        // Load all manifests concurrently and cache them for delete_entries() which is called later.
        // We need to clone because we'll be mutably borrowing snapshot_produce for manifest writing.
        let manifests: Vec<(ManifestFile, Manifest)> = {
            // Try cache first
            if let Some(cached) = self.cached_manifests.get() {
                cached.clone()
            } else {
                // Load manifests concurrently
                let loaded = self.load_all_manifests_concurrent(snapshot_produce).await?;
                // Cache for delete_entries() which will be called later
                let _ = self.cached_manifests.set(loaded.clone());
                loaded
            }
        };

        let mut existing_files = Vec::with_capacity(manifests.len());

        // Process each manifest - determine which can be kept as-is vs need rewriting
        for (manifest_file, manifest) in manifests {
            // Find files in this manifest that are being deleted
            let found_deleted_files: HashSet<_> = manifest
                .entries()
                .iter()
                .filter_map(|entry| {
                    let file_path = entry.data_file().file_path();
                    if snapshot_produce
                        .removed_data_file_paths
                        .contains(file_path)
                        || snapshot_produce
                            .removed_delete_file_paths
                            .contains(file_path)
                    {
                        Some(file_path.to_string())
                    } else {
                        None
                    }
                })
                .collect();

            if found_deleted_files.is_empty() {
                // No deleted files in this manifest - keep it as-is
                existing_files.push(manifest_file.clone());
            } else {
                // Some files are deleted - check if any entries remain
                let has_remaining_entries = manifest
                    .entries()
                    .iter()
                    .any(|entry| !found_deleted_files.contains(entry.data_file().file_path()));

                if has_remaining_entries {
                    // Rewrite the manifest without the deleted files
                    let mut manifest_writer = snapshot_produce.new_manifest_writer(
                        ManifestContentType::Data,
                        manifest_file.partition_spec_id,
                    )?;

                    for entry in manifest.entries() {
                        // if !found_deleted_files.contains(entry.data_file().file_path()) {
                        //     manifest_writer.add_entry((**entry).clone())?;
                        // }
                        // 1. Skip files being compacted (this part was correct)
                        if found_deleted_files.contains(entry.data_file().file_path()) {
                            continue;
                        }

                        // 2. NEW: Skip entries that are already DELETED (tombstones)
                        //    This is what Java's liveEntries() does automatically
                        if entry.status() == ManifestStatus::Deleted {
                            continue;
                        }

                        // 3. NEW: Use add_existing_entry() instead of add_entry()
                        //    This preserves the EXISTING status instead of changing to ADDED
                        manifest_writer.add_existing_entry((**entry).clone())?;
                    }

                    existing_files.push(manifest_writer.write_manifest_file().await?);
                }
                // If no remaining entries, the manifest is completely removed (not added to existing_files)
            }
        }

        Ok(existing_files)
    }

    /// Returns the manifest cache populated during existing_manifest().
    /// This allows ManifestFilterManager to use already-loaded manifests
    /// instead of reloading them from storage.
    fn get_manifest_cache(&self) -> HashMap<String, Manifest> {
        if let Some(cached) = self.cached_manifests.get() {
            cached
                .iter()
                .map(|(mf, m)| (mf.manifest_path.clone(), m.clone()))
                .collect()
        } else {
            HashMap::new()
        }
    }
}

#[async_trait::async_trait]
impl TransactionAction for RewriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let mut snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_id,
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
            self.removed_data_files.clone(),
            self.removed_delete_files.clone(),
        );

        if let Some(seq) = self.new_data_file_sequence_number {
            snapshot_producer.set_new_data_file_sequence_number(seq);
        }

        if let Some(branch) = &self.target_branch {
            snapshot_producer.set_target_branch(branch.clone());
        }

        if self.enable_delete_filter_manager {
            snapshot_producer.enable_delete_filter_manager();
        }

        if self.check_file_existence {
            snapshot_producer
                .validate_data_file_changes_concurrent()
                .await?;
        }

        // Choose between concurrent and sequential manifest loading
        if self.use_concurrent_manifest_loading {
            // Use concurrent manifest loading for better performance
            let operation = RewriteFilesOperationConcurrent::new();
            if self.merge_enabled {
                let process =
                    MergeManifestProcess::new(self.target_size_bytes, self.min_count_to_merge);
                snapshot_producer.commit(operation, process).await
            } else {
                snapshot_producer
                    .commit(operation, DefaultManifestProcess)
                    .await
            }
        } else {
            // Use sequential manifest loading (original behavior)
            if self.merge_enabled {
                let process =
                    MergeManifestProcess::new(self.target_size_bytes, self.min_count_to_merge);
                snapshot_producer
                    .commit(RewriteFilesOperation, process)
                    .await
            } else {
                snapshot_producer
                    .commit(RewriteFilesOperation, DefaultManifestProcess)
                    .await
            }
        }
    }
}

impl Default for RewriteFilesAction {
    fn default() -> Self {
        Self::new()
    }
}
