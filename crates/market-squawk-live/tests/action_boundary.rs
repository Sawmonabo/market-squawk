use market_squawk_live::{
    ActionAuthorityIssueLimit, ActionHookDisposition, CommittedActionContext, CurrentAuthorityGate,
    LiveActionHook, LiveActionHookError,
};
use static_assertions::{assert_impl_all, assert_not_impl_any};

assert_impl_all!(Box<dyn LiveActionHook>: Send);
assert_not_impl_any!(CurrentAuthorityGate<'static>: Clone, Send, Sync, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(CommittedActionContext<'static>: Clone, Send, Sync, serde::Serialize, serde::de::DeserializeOwned);

#[derive(Debug)]
struct NoAction;

impl LiveActionHook for NoAction {
    fn on_committed(
        &mut self,
        _context: CommittedActionContext<'_>,
        _authority: &mut CurrentAuthorityGate<'_>,
    ) -> ActionHookDisposition {
        ActionHookDisposition::NoAction
    }

    fn retained_bytes(&self) -> Result<usize, LiveActionHookError> {
        Ok(std::mem::size_of::<Self>())
    }

    fn maximum_authority_issues(&self) -> ActionAuthorityIssueLimit {
        ActionAuthorityIssueLimit::MIN
    }
}

#[test]
fn action_hook_is_object_safe_and_reports_owned_bytes() -> Result<(), LiveActionHookError> {
    let hook: Box<dyn LiveActionHook> = Box::new(NoAction);
    assert_eq!(hook.retained_bytes()?, 0);
    Ok(())
}
