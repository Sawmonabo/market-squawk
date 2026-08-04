use super::*;

/// Code-owned location of the restart-durable research source authority within a workspace.
pub const RESEARCH_SOURCE_AUTHORITY_DIRECTORY: &str = "sources/research-runtime";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UncleanPredecessorPolicy {
    Reject,
    RecoverStructurallyValidExclusiveInstalledReplacement,
}

impl AuthoritativeSourceRegistry {
    /// Opens the restart-durable authority registry and marks its new run generation in-use before
    /// any provider-budget authority can be minted.
    ///
    /// There is deliberately no generic `try_new` production bypass:
    ///
    /// ```compile_fail
    /// use market_squawk_sources::AuthoritativeSourceRegistry;
    ///
    /// let _registry = AuthoritativeSourceRegistry::try_new();
    /// ```
    ///
    /// Arbitrary store implementations cannot be presented as production durability:
    ///
    /// ```compile_fail
    /// use market_squawk_sources::AuthoritativeSourceRegistry;
    ///
    /// #[derive(Debug)]
    /// struct VolatileStore;
    ///
    /// let _registry = AuthoritativeSourceRegistry::try_new_durable(VolatileStore);
    /// ```
    ///
    /// The concrete store is transferred by value; retaining a raw `Arc` store capability is not
    /// accepted:
    ///
    /// ```compile_fail
    /// use std::sync::Arc;
    /// use market_squawk_platform::LocalAuthorityStateStore;
    /// use market_squawk_sources::AuthoritativeSourceRegistry;
    ///
    /// fn rejected(store: Arc<LocalAuthorityStateStore>) {
    ///     let _registry = AuthoritativeSourceRegistry::try_new_durable(store);
    /// }
    /// ```
    ///
    /// The generic persistence backend is not a public production extension point:
    ///
    /// ```compile_fail
    /// use market_squawk_sources::AuthorityStateStore;
    /// ```
    ///
    /// The linear unpublished-run capability cannot be named, cloned, or used by callers:
    ///
    /// ```compile_fail
    /// use market_squawk_sources::UnpublishedAuthoritySession;
    /// ```
    ///
    /// # Errors
    ///
    /// Fails closed when canonical state is unavailable, invalid, temporally ambiguous, or cannot
    /// be durably marked in-use.
    pub fn try_new_durable(
        store: market_squawk_platform::LocalAuthorityStateStore,
    ) -> Result<Self, RegistryError> {
        Self::try_new_durable_with_authorization_subject_resolver(
            store,
            Arc::new(UnconfiguredAuthorizationSubjectResolver),
        )
    }

    /// Opens a restart-durable registry whose provider requests share one product-owned aggregate
    /// rate authority with onboarding and other live/research registries.
    ///
    /// # Errors
    ///
    /// Fails closed on either registry persistence or aggregate rate-authority registration.
    pub fn try_new_durable_with_provider_rate(
        store: market_squawk_platform::LocalAuthorityStateStore,
        provider_rate: crate::ProviderRateAuthority,
    ) -> Result<Self, RegistryError> {
        Self::try_new_durable_with_authorization_subject_resolver_and_provider_rate(
            store,
            Arc::new(UnconfiguredAuthorizationSubjectResolver),
            provider_rate,
        )
    }

    /// Opens the restart-durable registry with trusted account-subject resolution.
    ///
    /// # Errors
    ///
    /// Fails closed on persistence, restore, subject-resolution, or coordinator failure.
    pub fn try_new_durable_with_authorization_subject_resolver(
        store: market_squawk_platform::LocalAuthorityStateStore,
        resolver: Arc<dyn crate::AuthorizationSubjectResolver>,
    ) -> Result<Self, RegistryError> {
        let store: Arc<dyn crate::policy::AuthorityStateStore> = Arc::new(store);
        Self::try_new_durable_with_store_and_authorization_subject_resolver(store, resolver)
    }

    /// Opens a restart-durable registry with both trusted account resolution and product-wide rate
    /// authority.
    ///
    /// # Errors
    ///
    /// Fails closed on persistence, restore, subject resolution, or aggregate registration.
    pub fn try_new_durable_with_authorization_subject_resolver_and_provider_rate(
        store: market_squawk_platform::LocalAuthorityStateStore,
        resolver: Arc<dyn crate::AuthorizationSubjectResolver>,
        provider_rate: crate::ProviderRateAuthority,
    ) -> Result<Self, RegistryError> {
        let store: Arc<dyn crate::policy::AuthorityStateStore> = Arc::new(store);
        Self::try_new_durable_with_store_resolver_clock_and_provider_rate(
            store,
            resolver,
            Arc::new(SystemRawRegistryClock::try_new()?),
            Some(provider_rate),
            UncleanPredecessorPolicy::Reject,
        )
    }

    /// Opens source authority for an exclusively admitted installed-service replacement.
    ///
    /// Structurally valid source registrations, provenance, policies, window consumption, cooldowns,
    /// and generation history are retained. Orphaned in-flight request counts are reconciled to zero
    /// under a checked availability-generation advance because no request owner can survive the
    /// crashed process. The run generation advances and the canonical envelope remains in-use for
    /// the replacement. Invalid state, wall rollback, or any generation exhaustion remains
    /// fail-closed.
    ///
    /// The selected-workspace guard owns the installation-global instance lock and exact prepared
    /// workspace together. This constructor derives the fixed source store from that capability;
    /// it does not accept a separately substitutable store. Default durable constructors do not
    /// accept the guard and continue to reject every unclean predecessor.
    ///
    /// A raw authority-state store cannot be paired with an unrelated installation guard:
    ///
    /// ```compile_fail
    /// use std::sync::Arc;
    /// use market_squawk_platform::{
    ///     InstalledServiceSelectedWorkspaceGuard, LocalAuthorityStateStore,
    /// };
    /// use market_squawk_sources::{
    ///     AuthoritativeSourceRegistry, AuthorizationSubjectResolver, ProviderRateAuthority,
    /// };
    ///
    /// fn rejected(
    ///     store: LocalAuthorityStateStore,
    ///     selected: &InstalledServiceSelectedWorkspaceGuard,
    ///     resolver: Arc<dyn AuthorizationSubjectResolver>,
    ///     provider_rate: ProviderRateAuthority,
    /// ) {
    ///     let _registry = AuthoritativeSourceRegistry::
    ///         try_new_durable_for_exclusive_installed_service_replacement(
    ///             store,
    ///             resolver,
    ///             provider_rate,
    ///             selected,
    ///         );
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Fails closed on invalid persistence, restore, subject resolution, aggregate registration,
    /// trusted-time rollback, or generation exhaustion.
    pub fn try_new_durable_for_exclusive_installed_service_replacement(
        selected_workspace: &market_squawk_platform::InstalledServiceSelectedWorkspaceGuard,
        resolver: Arc<dyn crate::AuthorizationSubjectResolver>,
        provider_rate: crate::ProviderRateAuthority,
    ) -> Result<Self, RegistryError> {
        let control = selected_workspace
            .workspace_paths()
            .control_root()
            .map_err(|_error| RegistryError::AuthorityPersistence)?;
        let store = market_squawk_platform::LocalAuthorityStateStore::try_open(
            control.root().join(RESEARCH_SOURCE_AUTHORITY_DIRECTORY),
        )
        .map_err(|_error| RegistryError::AuthorityPersistence)?;
        let store: Arc<dyn crate::policy::AuthorityStateStore> = Arc::new(store);
        Self::try_new_durable_with_store_resolver_clock_and_provider_rate(
            store,
            resolver,
            Arc::new(SystemRawRegistryClock::try_new()?),
            Some(provider_rate),
            UncleanPredecessorPolicy::RecoverStructurallyValidExclusiveInstalledReplacement,
        )
    }

    #[cfg(test)]
    pub(super) fn try_new_durable_with_store_for_exclusive_installed_replacement_for_test(
        store: Arc<dyn crate::policy::AuthorityStateStore>,
        resolver: Arc<dyn crate::AuthorizationSubjectResolver>,
    ) -> Result<Self, RegistryError> {
        Self::try_new_durable_with_store_resolver_clock_and_provider_rate(
            store,
            resolver,
            Arc::new(SystemRawRegistryClock::try_new()?),
            None,
            UncleanPredecessorPolicy::RecoverStructurallyValidExclusiveInstalledReplacement,
        )
    }

    pub(crate) fn try_new_durable_with_store_and_authorization_subject_resolver(
        store: Arc<dyn crate::policy::AuthorityStateStore>,
        resolver: Arc<dyn crate::AuthorizationSubjectResolver>,
    ) -> Result<Self, RegistryError> {
        let raw_clock: Arc<dyn RawRegistryClockSource> =
            Arc::new(SystemRawRegistryClock::try_new()?);
        Self::try_new_durable_with_store_resolver_and_clock_source(store, resolver, raw_clock)
    }

    pub(super) fn try_new_durable_with_store_resolver_and_clock_source(
        store: Arc<dyn crate::policy::AuthorityStateStore>,
        resolver: Arc<dyn crate::AuthorizationSubjectResolver>,
        raw_clock: Arc<dyn RawRegistryClockSource>,
    ) -> Result<Self, RegistryError> {
        Self::try_new_durable_with_store_resolver_clock_and_provider_rate(
            store,
            resolver,
            raw_clock,
            None,
            UncleanPredecessorPolicy::Reject,
        )
    }

    fn try_new_durable_with_store_resolver_clock_and_provider_rate(
        store: Arc<dyn crate::policy::AuthorityStateStore>,
        resolver: Arc<dyn crate::AuthorizationSubjectResolver>,
        raw_clock: Arc<dyn RawRegistryClockSource>,
        provider_rate: Option<crate::ProviderRateAuthority>,
        unclean_predecessor_policy: UncleanPredecessorPolicy,
    ) -> Result<Self, RegistryError> {
        let clock = Arc::new(SealedRegistryClock::new(raw_clock));
        let now = clock.observe()?.wall();
        let unpublished = match unclean_predecessor_policy {
            UncleanPredecessorPolicy::Reject => {
                AuthorityDurabilitySession::open_unpublished(store, now)
            }
            UncleanPredecessorPolicy::RecoverStructurallyValidExclusiveInstalledReplacement => {
                AuthorityDurabilitySession::open_unpublished_for_exclusive_installed_replacement(
                    store, now,
                )
            }
        }
        .map_err(map_authority_persistence_error)?;
        let durability = Arc::clone(unpublished.session());
        if durability.recovered_unclean() {
            durability.invalidate();
            return Err(RegistryError::UncleanAuthorityPredecessor);
        }
        clock.bind_durability(&durability)?;
        let construction = (|| {
            let state = durability
                .registry_state()
                .map_err(map_authority_persistence_error)?;
            let groups = durability
                .budget_groups()
                .map_err(map_authority_persistence_error)?;
            Self::try_new_with_durable_state(
                state,
                groups,
                clock,
                resolver,
                Arc::clone(&durability),
                provider_rate,
            )
        })();
        match construction {
            Ok(registry) => {
                unpublished
                    .publish()
                    .map_err(map_authority_persistence_error)?;
                Ok(registry)
            }
            Err(error) => {
                if !durability.recovered_unclean() && unpublished.rollback().is_err() {
                    return Err(RegistryError::AuthorityPersistence);
                }
                Err(error)
            }
        }
    }

    /// Creates a bounded process-local registry for production extraction and inspection.
    ///
    /// The caller must supply both trusted authorization-subject resolution and the product-wide
    /// provider-rate authority. Every registered provider budget therefore joins the shared
    /// aggregate authority instead of an isolated quota pool.
    ///
    /// This composition intentionally retains no restart state and cannot mint live source-session
    /// authority. It is suitable only for bounded non-durable extraction or response inspection;
    /// durable research publication and live execution require their dedicated durable
    /// compositions.
    ///
    /// # Errors
    ///
    /// Returns a typed registry error when the trusted clock or process-local registry identity
    /// cannot be established.
    pub fn try_new_in_memory_for_bounded_extraction(
        resolver: Arc<dyn crate::AuthorizationSubjectResolver>,
        provider_rate: crate::ProviderRateAuthority,
    ) -> Result<Self, RegistryError> {
        Self::try_new_in_memory_with_authority_state_clock_resolver_and_provider_rate(
            RegistryAuthorityState::empty(),
            Arc::new(SystemRawRegistryClock::try_new()?),
            resolver,
            Some(provider_rate),
            AuthorityComposition::InMemoryExtractionInspection,
        )
    }

    /// Creates a diagnostic-only in-memory registry with a process-unique instance identity.
    ///
    /// This constructor does not claim restart persistence, does not publish a clean-run marker,
    /// and must not be used to compose production live authority.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::RegistryIdentityExhausted`] if the process-wide identifier space is
    /// exhausted.
    pub fn try_new_ephemeral_for_diagnostics() -> Result<Self, RegistryError> {
        Self::try_new_ephemeral_with_authority_state_for_diagnostics(RegistryAuthorityState::empty())
    }

    /// Creates a diagnostic-only in-memory registry with a trusted subject resolver.
    ///
    /// The resolver is consulted only for account-qualified user-authorized or licensed budgets;
    /// public-interface budgets are derived solely from normalized endpoint origins.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::RegistryIdentityExhausted`] when registry identity is exhausted.
    pub fn try_new_ephemeral_with_authorization_subject_resolver_for_diagnostics(
        resolver: Arc<dyn crate::AuthorizationSubjectResolver>,
    ) -> Result<Self, RegistryError> {
        Self::try_new_in_memory_with_authority_state_clock_resolver_and_provider_rate(
            RegistryAuthorityState::empty(),
            Arc::new(SystemRawRegistryClock::try_new()?),
            resolver,
            None,
            AuthorityComposition::InMemoryDiagnostic,
        )
    }

    /// Restores bounded tombstones into a diagnostic-only in-memory registry.
    ///
    /// Manual state import here is intentionally not a restart-durability claim. Production
    /// composition must use [`Self::try_new_durable`] and an atomic authority-state store.
    ///
    /// # Errors
    ///
    /// Rejects unsupported/tampered state, duplicate sources/budget scopes, or coordinator failure.
    pub fn try_new_ephemeral_with_authority_state_for_diagnostics(
        state: RegistryAuthorityState,
    ) -> Result<Self, RegistryError> {
        Self::try_new_ephemeral_with_authority_state_and_clock_for_diagnostics(
            state,
            Arc::new(SystemRawRegistryClock::try_new()?),
        )
    }

    /// Restores state into a diagnostic-only registry and retains a trusted subject resolver.
    ///
    /// # Errors
    ///
    /// Rejects invalid persisted authority or coordinator conflicts.
    pub fn try_new_ephemeral_with_authority_state_and_authorization_subject_resolver_for_diagnostics(
        state: RegistryAuthorityState,
        resolver: Arc<dyn crate::AuthorizationSubjectResolver>,
    ) -> Result<Self, RegistryError> {
        Self::try_new_in_memory_with_authority_state_clock_resolver_and_provider_rate(
            state,
            Arc::new(SystemRawRegistryClock::try_new()?),
            resolver,
            None,
            AuthorityComposition::InMemoryDiagnostic,
        )
    }

    pub(super) fn try_new_ephemeral_with_authority_state_and_clock_for_diagnostics(
        state: RegistryAuthorityState,
        clock_source: Arc<dyn RawRegistryClockSource>,
    ) -> Result<Self, RegistryError> {
        Self::try_new_in_memory_with_authority_state_clock_resolver_and_provider_rate(
            state,
            clock_source,
            Arc::new(UnconfiguredAuthorizationSubjectResolver),
            None,
            AuthorityComposition::InMemoryDiagnostic,
        )
    }

    fn try_new_in_memory_with_authority_state_clock_resolver_and_provider_rate(
        state: RegistryAuthorityState,
        clock_source: Arc<dyn RawRegistryClockSource>,
        authorization_subject_resolver: Arc<dyn crate::AuthorizationSubjectResolver>,
        provider_rate: Option<crate::ProviderRateAuthority>,
        composition: AuthorityComposition,
    ) -> Result<Self, RegistryError> {
        let clock = Arc::new(SealedRegistryClock::new(clock_source));
        let _initial_time = clock.observe()?;
        let instance_id = NEXT_REGISTRY_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| RegistryError::RegistryIdentityExhausted)?;
        let mut budgets = match provider_rate {
            Some(provider_rate) => {
                ProviderBudgetPool::new_in_memory_with_provider_rate(provider_rate)
            }
            None => ProviderBudgetPool::new().map_err(|_| RegistryError::BudgetCoordinator)?,
        };
        let resolved_policies = state
            .budget_policies
            .as_slice()
            .iter()
            .map(|persisted| {
                persisted
                    .resolve(authorization_subject_resolver.as_ref())
                    .map_err(map_budget_resolution_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
        budgets
            .register_all(&resolved_policies)
            .map_err(|_| RegistryError::BudgetCoordinator)?;
        let history = state
            .sources
            .as_slice()
            .iter()
            .map(|source| {
                (
                    source.source_id.clone(),
                    SourceAuthorityHistory {
                        used_revisions: source.used_revisions.as_slice().to_vec(),
                        latest_revision_evidence: source.latest_revision_evidence.clone(),
                        revoked: source.revoked,
                        last_epoch: source.last_epoch,
                        generation_high_water: source.generation_high_water,
                    },
                )
            })
            .collect();
        Ok(Self {
            instance_id,
            entries: BTreeMap::new(),
            budgets,
            history,
            clock,
            authorization_subject_resolver,
            composition,
        })
    }

    fn try_new_with_durable_state(
        state: RegistryAuthorityState,
        groups: Vec<DurableBudgetGroup>,
        clock: Arc<SealedRegistryClock>,
        authorization_subject_resolver: Arc<dyn crate::AuthorizationSubjectResolver>,
        durability: Arc<AuthorityDurabilitySession>,
        provider_rate: Option<crate::ProviderRateAuthority>,
    ) -> Result<Self, RegistryError> {
        let mut resolved_groups = Vec::new();
        let mut flattened = Vec::new();
        for group in groups {
            let declarations = group
                .declarations()
                .iter()
                .map(|persisted| {
                    persisted
                        .resolve(authorization_subject_resolver.as_ref())
                        .map_err(map_budget_resolution_error)
                })
                .collect::<Result<Vec<_>, _>>()?;
            flattened.extend(
                declarations
                    .iter()
                    .map(|resolved| resolved.persisted().clone()),
            );
            resolved_groups.push((declarations, group.checkpoint().clone()));
        }
        if !same_persisted_policy_set(state.budget_policies(), &flattened) {
            return Err(RegistryError::InvalidAuthorityState);
        }
        let instance_id = NEXT_REGISTRY_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| RegistryError::RegistryIdentityExhausted)?;
        let mut budgets = match provider_rate {
            Some(provider_rate) => ProviderBudgetPool::new_durable_with_provider_rate(
                Arc::clone(&durability),
                provider_rate,
            ),
            None => ProviderBudgetPool::new_durable(Arc::clone(&durability)),
        };
        budgets
            .restore_durable(resolved_groups)
            .map_err(|_| RegistryError::BudgetCoordinator)?;
        let history = history_from_state(&state);
        Ok(Self {
            instance_id,
            entries: BTreeMap::new(),
            budgets,
            history,
            clock,
            authorization_subject_resolver,
            composition: AuthorityComposition::Durable(durability),
        })
    }

    /// Exports bounded serializable tombstones; live handles and leases are deliberately absent.
    ///
    /// # Errors
    ///
    /// Fails if configured source or budget counts exceed persisted bounds.
    pub fn export_authority_state(&self) -> Result<RegistryAuthorityState, RegistryError> {
        authority_state_from_history(&self.history, self.budgets.policies())
    }

    /// Captures a non-consuming clean-restart image under the registry owner's mutation fence.
    ///
    /// The caller must retain its composition-level registry fence while invoking this method.
    /// Later mutations cannot affect the returned immutable bytes.
    ///
    /// # Errors
    ///
    /// Rejects in-memory registries, active source sessions, active provider requests, mismatched
    /// durable checkpoints, unavailable trusted time, and invalid persisted authority state.
    pub fn retain_clean_restart_backup_bytes(&self) -> Result<Box<[u8]>, RegistryError> {
        self.retain_clean_restart_backup()
            .map(|backup| Box::from(backup.as_bytes()))
    }

    /// Validates and canonicalizes an opaque registry clean-restart image without opening runtime
    /// authority.
    ///
    /// # Errors
    ///
    /// Rejects malformed, noncanonical, in-use, future-dated, or non-clean budget state.
    pub fn validate_clean_restart_backup_bytes(bytes: &[u8]) -> Result<Box<[u8]>, RegistryError> {
        RegistryCleanRestartBackup::try_from_bytes(bytes).map(|backup| Box::from(backup.as_bytes()))
    }

    /// Restores an opaque clean-restart image only into an absent production registry store.
    ///
    /// # Errors
    ///
    /// Rejects an occupied target or any payload that fails clean-restart validation.
    pub fn restore_clean_restart_backup_fresh(
        store: market_squawk_platform::LocalAuthorityStateStore,
        bytes: &[u8],
    ) -> Result<(), RegistryError> {
        RegistryCleanRestartBackup::try_from_bytes(bytes)?.restore_fresh(store)
    }

    fn retain_clean_restart_backup(&self) -> Result<RegistryCleanRestartBackup, RegistryError> {
        if self.entries.values().any(|entry| entry.active.is_some()) {
            return Err(RegistryError::ActiveAuthorityAtShutdown);
        }
        let AuthorityComposition::Durable(durability) = &self.composition else {
            return Err(RegistryError::AuthorityPersistence);
        };
        if durability.recovered_unclean() {
            return Err(RegistryError::UncleanAuthorityPredecessor);
        }
        if self
            .budgets
            .has_active_requests()
            .map_err(|_error| RegistryError::AuthorityPersistence)?
        {
            return Err(RegistryError::ActiveAuthorityAtShutdown);
        }
        let proof = self
            .budgets
            .validate_clean_shutdown(durability)
            .map_err(|_error| RegistryError::ActiveAuthorityAtShutdown)?;
        let observed = self.clock.observe()?;
        let bytes = durability
            .export_clean_restart_backup(proof, self.export_authority_state()?, observed.wall())
            .map_err(map_authority_persistence_error)?;
        RegistryCleanRestartBackup::try_from_bytes(&bytes)
    }

    /// Durably marks this run clean after every source session and provider request reconciles.
    ///
    /// Consuming the registry prevents authority minting after the clean marker. Retained durable
    /// budget handles fail closed once their durability session closes.
    ///
    /// # Errors
    ///
    /// Rejects shutdown while a source session or provider request remains active and fails closed
    /// when the final checkpoint cannot be durably replaced.
    pub fn shutdown(self) -> Result<(), RegistryError> {
        if self.entries.values().any(|entry| entry.active.is_some()) {
            return Err(RegistryError::ActiveAuthorityAtShutdown);
        }
        match &self.composition {
            AuthorityComposition::Durable(durability) => {
                if durability.recovered_unclean() {
                    return Err(RegistryError::UncleanAuthorityPredecessor);
                }
                let proof = self
                    .budgets
                    .validate_clean_shutdown(durability)
                    .map_err(|_error| RegistryError::ActiveAuthorityAtShutdown)?;
                let observed = self.clock.observe()?;
                durability
                    .close_clean(proof, self.export_authority_state()?, observed.wall())
                    .map_err(map_authority_persistence_error)?;
            }
            AuthorityComposition::InMemoryDiagnostic
            | AuthorityComposition::InMemoryExtractionInspection => {}
        }
        Ok(())
    }
}
