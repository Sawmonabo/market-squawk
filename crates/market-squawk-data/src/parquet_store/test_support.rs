//! First-bind crash-boundary fixtures used only by deterministic unit tests.

use market_squawk_platform::ArtifactRoot;

use super::{ObjectStoreConfig, ParquetObjectStore, ParquetStoreError};

impl ParquetObjectStore {
    pub(crate) fn create_legacy_root_fixture(
        root: ArtifactRoot,
        config: ObjectStoreConfig,
        catalog_binding: [u8; 32],
    ) -> Result<(), ParquetStoreError> {
        drop(Self::open(root, config, catalog_binding, None)?);
        Ok(())
    }
}
