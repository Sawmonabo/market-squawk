//! Economic-date source requests through the sole credential, budget and capture owner.
use super::*;
use crate::TiingoCorporateActionReceipt;
impl TiingoHttpSource {
    /// Fetches and strictly decodes one maintained exact economic-date query under shared limits.
    pub async fn fetch_corporate_actions(
        &self,
        spec: TiingoRequestSpec,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<TiingoCapturedPage<TiingoCorporateActionReceipt>, TiingoHttpSourceError> {
        if !matches!(
            spec.endpoint(),
            TiingoEndpointFamily::CorporateActionDistributions
                | TiingoEndpointFamily::CorporateActionSplits
        ) {
            return Err(TiingoAdapterError::RequestBuild.into());
        }
        let mut runtime = self.runtime.lock().await;
        let pending = self
            .fetch_raw_locked(&mut runtime, spec.clone(), None, deadline, cancellation)
            .await?;
        let raw = pending.raw;
        let decoded_at = self.decode_timestamp_or_reject(&pending.permit, &raw)?;
        match runtime.decoder.decode_corporate_actions(
            spec,
            raw.http.status,
            &raw.http.body,
            raw.http.received_at,
            decoded_at,
        ) {
            Ok(decoded) => {
                self.settle_decoded_success(&pending.permit, &raw)?;
                Ok(TiingoCapturedPage { raw, decoded })
            }
            Err(error) => self.decode_failure(&mut runtime, error, raw, pending.permit),
        }
    }
}
