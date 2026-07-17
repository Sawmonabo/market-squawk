//! Durable declaration-group validation and bounded connectivity reconstruction.

use super::*;

pub(super) fn combine_durable_group(
    declarations: &[ResolvedProviderBudgetPolicy],
) -> Result<ResolvedProviderBudgetPolicy, BudgetPoolError> {
    let first = declarations
        .first()
        .ok_or(BudgetPoolError::CoordinatorCorrupt)?;
    if declarations
        .iter()
        .skip(1)
        .any(|declaration| !first.policy().has_same_limits_as(declaration.policy()))
    {
        return Err(BudgetPoolError::ConflictingPolicy);
    }
    let collision_key = match first.collision_key() {
        BudgetCollisionKey::Account(subject) => {
            if declarations.iter().skip(1).any(|declaration| {
                !matches!(
                    declaration.collision_key(),
                    BudgetCollisionKey::Account(other) if other == subject
                )
            }) {
                return Err(BudgetPoolError::CoordinatorCorrupt);
            }
            BudgetCollisionKey::Account(subject.clone())
        }
        BudgetCollisionKey::Public(_) => combine_connected_public_declarations(declarations)?,
    };
    Ok(ResolvedProviderBudgetPolicy::from_canonical_parts(
        first.persisted().clone(),
        collision_key,
    ))
}

fn combine_connected_public_declarations(
    declarations: &[ResolvedProviderBudgetPolicy],
) -> Result<BudgetCollisionKey, BudgetPoolError> {
    let mut authority_count = 0_usize;
    for declaration in declarations {
        let BudgetCollisionKey::Public(authorities) = declaration.collision_key() else {
            return Err(BudgetPoolError::CoordinatorCorrupt);
        };
        authority_count = authority_count
            .checked_add(authorities.len())
            .filter(|count| *count <= MAX_MERGED_CANONICAL_AUTHORITIES)
            .ok_or(BudgetPoolError::CanonicalAuthorityCapacity)?;
    }
    let mut owners = Vec::new();
    owners
        .try_reserve(authority_count)
        .map_err(|_| BudgetPoolError::CanonicalAuthorityAllocation)?;
    for (declaration_index, declaration) in declarations.iter().enumerate() {
        let BudgetCollisionKey::Public(authorities) = declaration.collision_key() else {
            return Err(BudgetPoolError::CoordinatorCorrupt);
        };
        owners.extend(
            authorities
                .iter()
                .map(|authority| (authority, declaration_index)),
        );
    }
    owners.sort_unstable_by(|left, right| left.0.cmp(right.0));
    let mut parents = Vec::new();
    parents
        .try_reserve(declarations.len())
        .map_err(|_| BudgetPoolError::CoordinatorAllocation)?;
    parents.extend(0..declarations.len());
    for pair in owners.windows(2) {
        if pair[0].0 == pair[1].0 {
            union_declarations(&mut parents, pair[0].1, pair[1].1)?;
        }
    }
    let Some(first) = (!parents.is_empty()).then(|| find_declaration_root(&mut parents, 0)) else {
        return Err(BudgetPoolError::CoordinatorCorrupt);
    };
    for index in 1..parents.len() {
        if find_declaration_root(&mut parents, index) != first {
            return Err(BudgetPoolError::CoordinatorCorrupt);
        }
    }
    let mut combined = declarations
        .first()
        .ok_or(BudgetPoolError::CoordinatorCorrupt)?
        .collision_key()
        .clone();
    for declaration in declarations.iter().skip(1) {
        combined
            .merge_public_authorities(declaration.collision_key())
            .map_err(|error| match error {
                BudgetCollisionMergeError::Capacity => {
                    BudgetPoolError::CanonicalAuthorityCapacity
                }
                BudgetCollisionMergeError::Allocation => {
                    BudgetPoolError::CanonicalAuthorityAllocation
                }
            })?;
    }
    Ok(combined)
}

fn find_declaration_root(parents: &mut [usize], index: usize) -> usize {
    let mut root = index;
    while parents[root] != root {
        root = parents[root];
    }
    let mut current = index;
    while parents[current] != current {
        let next = parents[current];
        parents[current] = root;
        current = next;
    }
    root
}

fn union_declarations(
    parents: &mut [usize],
    left: usize,
    right: usize,
) -> Result<(), BudgetPoolError> {
    if left >= parents.len() || right >= parents.len() {
        return Err(BudgetPoolError::CoordinatorCorrupt);
    }
    let left_root = find_declaration_root(parents, left);
    let right_root = find_declaration_root(parents, right);
    if left_root != right_root {
        parents[right_root] = left_root;
    }
    Ok(())
}
