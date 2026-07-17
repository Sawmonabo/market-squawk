use super::*;

impl AuthoritativeSourceRegistry {
    pub(super) fn persist_registry_candidate(
        &self,
        candidate: RegistryAuthorityState,
    ) -> Result<(), RegistryError> {
        let observed = self.clock.observe().inspect_err(|_error| {
            if let AuthorityComposition::Durable(durability) = &self.composition {
                durability.invalidate();
            }
        })?;
        self.persist_registry_candidate_at(candidate, observed.wall())
    }

    pub(super) fn persist_registry_candidate_at(
        &self,
        candidate: RegistryAuthorityState,
        wall: Timestamp,
    ) -> Result<(), RegistryError> {
        match &self.composition {
            AuthorityComposition::Durable(durability) => durability
                .persist_registry(candidate, wall)
                .map_err(map_authority_persistence_error),
            AuthorityComposition::EphemeralDiagnostic => Ok(()),
        }
    }
}
