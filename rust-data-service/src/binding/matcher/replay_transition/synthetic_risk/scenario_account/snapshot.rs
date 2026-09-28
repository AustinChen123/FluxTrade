//! Detached snapshot evidence, never a financial owner or scheduler clock.
use super::*;
use risk_transition::cancel::identity::{classify, Encoding, Stored};
mod payload;
pub(super) mod wire;
use payload::Payload;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Market,
    Earn,
    Trading,
    Positions,
    OpenOrders,
}
impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Market => "MARKET",
            Self::Earn => "EARN",
            Self::Trading => "TRADING",
            Self::Positions => "POSITIONS",
            Self::OpenOrders => "OPEN_ORDERS",
        }
    }
    fn payload_name(self) -> &'static str {
        match self {
            Self::Market => "MARKET_SNAPSHOT",
            Self::Earn => "EARN_SNAPSHOT",
            Self::Trading => "TRADING_SNAPSHOT",
            Self::Positions => "POSITION_SNAPSHOT",
            Self::OpenOrders => "OPEN_ORDER_SNAPSHOT",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    OwnerCurrent,
    FrozenPollFixture,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Request {
    schema_version: String,
    account_key: AccountKey,
    snapshot_id: String,
    kind: Kind,
    mode: Mode,
    fixture_key: Option<String>,
    captured_at: i64,
    continuation_id: Option<String>,
}
impl Request {
    fn digest(&self) -> Hash {
        let mut e = Encoding::new("SCENARIO_SNAPSHOT_REQUEST_V1");
        e.text(&self.schema_version);
        e.account(&self.account_key);
        e.text(&self.snapshot_id);
        e.text(self.kind.name());
        e.text(match self.mode {
            Mode::OwnerCurrent => "OWNER_CURRENT",
            Mode::FrozenPollFixture => "FROZEN_POLL_FIXTURE",
        });
        e.optional_text(self.fixture_key.as_deref());
        e.integer(self.captured_at);
        e.optional_text(self.continuation_id.as_deref());
        e.finish()
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Fact {
    snapshot_id: String,
    request_digest: Hash,
    kind: Kind,
    captured_account_version: Option<i64>,
    snapshot_as_of: i64,
    continuation_id: Option<String>,
    payload: Payload,
    payload_digest: Hash,
}
impl Fact {
    pub(super) fn delivery_metadata(
        &self,
        kind: &str,
        continuation: Option<&str>,
    ) -> Result<(Option<i64>, i64), Fault> {
        if kind != self.kind.payload_name() || continuation != self.continuation_id.as_deref() {
            return Err("INVALID_SCHEMA");
        }
        Ok((self.captured_account_version, self.snapshot_as_of))
    }
    pub(super) fn encode_payload(&self, e: &mut Encoding) -> Result<(), Fault> {
        self.payload.encode(e)
    }
    fn digest(&self) -> Result<Hash, Fault> {
        let mut e = Encoding::new("SCENARIO_SNAPSHOT_PAYLOAD_V1");
        e.text("SNAPSHOT");
        e.text(&self.snapshot_id);
        e.hash(self.request_digest);
        e.text(self.kind.name());
        e.optional_integer(self.captured_account_version);
        e.integer(self.snapshot_as_of);
        e.optional_text(self.continuation_id.as_deref());
        self.payload.encode(&mut e)?;
        Ok(e.finish())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Store {
    account_key: AccountKey,
    snapshots: BTreeMap<String, Stored<Fact>>,
}
impl Store {
    pub(super) fn lookup(&self, account: &AccountKey, id: &str) -> Result<Fact, Fault> {
        if account != &self.account_key {
            return Err("ACCOUNT_KEY_MISMATCH");
        }
        self.snapshots
            .get(id)
            .map(|s| s.value.clone())
            .ok_or("UNKNOWN_RECEIPT_REFERENCE")
    }
    pub(super) fn new(account_key: AccountKey) -> Self {
        Self {
            account_key,
            snapshots: BTreeMap::new(),
        }
    }
    pub(super) fn capture(
        &mut self,
        owner: &ScenarioAccount,
        request: &Request,
    ) -> Result<Fact, Fault> {
        if request.schema_version != "snapshot_request_v1"
            || request.account_key.validate().is_err()
            || !identity(&request.snapshot_id)
            || request.captured_at < 0
            || request
                .continuation_id
                .as_deref()
                .is_some_and(|v| !identity(v))
            || request.fixture_key.as_deref().is_some_and(|v| !identity(v))
        {
            return Err("INVALID_SCHEMA");
        }
        if request.account_key != self.account_key || owner.key != self.account_key {
            return Err("ACCOUNT_KEY_MISMATCH");
        }
        let digest = request.digest();
        if let Some(fact) = classify(
            self.snapshots.get(&request.snapshot_id),
            digest,
            "SNAPSHOT_ID_CONFLICT",
        )? {
            return Ok(fact.clone());
        }
        let (payload, snapshot_as_of, captured_account_version) = match request.mode {
            Mode::OwnerCurrent => {
                if !matches!(
                    request.kind,
                    Kind::Trading | Kind::Positions | Kind::OpenOrders
                ) || !matches!(
                    owner.profile,
                    ProfileContext::BtcEthScenario { .. } | ProfileContext::GoldenCancel(_)
                ) || request.fixture_key.is_some()
                    || request.captured_at < owner.seed_effective_at
                    || owner
                        .transition
                        .events
                        .values()
                        .any(|(s, _)| s.effective_at > request.captured_at)
                {
                    return Err("INVALID_SCHEMA");
                }
                (
                    payload::current(owner, request.kind).map_err(|_| "NATIVE_INVARIANT")?,
                    request.captured_at,
                    Some(i64::try_from(owner.state_version).map_err(|_| "NATIVE_INVARIANT")?),
                )
            }
            Mode::FrozenPollFixture => {
                let (payload, at) =
                    payload::fixture(request.fixture_key.as_deref().ok_or("INVALID_SCHEMA")?)?;
                (payload, at, None)
            }
        };
        if payload.kind() != request.kind {
            return Err("INVALID_SCHEMA");
        }
        let mut fact = Fact {
            snapshot_id: request.snapshot_id.clone(),
            request_digest: digest,
            kind: request.kind,
            captured_account_version,
            snapshot_as_of,
            continuation_id: request.continuation_id.clone(),
            payload,
            payload_digest: [0; 32],
        };
        fact.payload_digest = fact.digest()?;
        self.snapshots.insert(
            request.snapshot_id.clone(),
            Stored {
                digest,
                value: fact.clone(),
            },
        );
        Ok(fact)
    }
}
#[cfg(test)]
pub(super) mod tests;
