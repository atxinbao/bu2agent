//! HCHGatewayTransport equivalent.
//!
//! The trait is the seam: MockTransport (Phase 1), then
//! HttpsTransport / WebSocketTransport / NoiseTransport, all
//! behind the same interface. No business logic lives here;
//! this layer only sends, receives, times out, cancels and reconnects.

use crate::gateway::{GatewayRequest, GatewayResponse, SubscriptionRecord};
use crate::Result;

/// Private-auth intent for confidential/private gateway targets.
/// A failed private-auth check must refuse transport, never
/// silently downgrade to an untrusted transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivateAuthIntent {
    None,
    Required,
    Verified,
}

pub trait GatewayTransport {
    /// Open (or confirm) the transport for a target.
    fn open(&mut self, intent: PrivateAuthIntent) -> Result<()>;

    /// One request with timeout and cancellation handled by the caller.
    /// 401 handling lives in the caller: refresh once, never retry a
    /// write blindly.
    fn request(&mut self, req: GatewayRequest) -> Result<GatewayResponse>;

    /// Subscribe and return the records received so far in this call.
    /// Streaming implementations replace this with an async sequence
    /// in Phase 1; the dedup rule (sequence) does not change.
    fn subscribe(&mut self, req: GatewayRequest) -> Result<Vec<SubscriptionRecord>>;

    fn close(&mut self, reason: &str) -> Result<()>;
}

/// Phase 1 placeholder. Real behaviour (state machine, approval,
/// cancellation, failure injection) belongs in the Mock Gateway
/// service; this type only proves the trait boundary.
pub struct MockTransport;

impl GatewayTransport for MockTransport {
    fn open(&mut self, _intent: PrivateAuthIntent) -> Result<()> {
        todo!("Phase 1: MockTransport.open")
    }
    fn request(&mut self, _req: GatewayRequest) -> Result<GatewayResponse> {
        todo!("Phase 1: MockTransport.request")
    }
    fn subscribe(&mut self, _req: GatewayRequest) -> Result<Vec<SubscriptionRecord>> {
        todo!("Phase 1: MockTransport.subscribe")
    }
    fn close(&mut self, _reason: &str) -> Result<()> {
        Ok(())
    }
}
