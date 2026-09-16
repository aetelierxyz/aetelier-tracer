use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aetelier_proto::aetelier::v1beta1::{TracerClaim, TracerClaimRole};
use prost::Message;
use rdkafka::ClientConfig;
use rdkafka::client::ClientContext;
use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext};
use uuid::Uuid;

use crate::api::wall_now_ms;

pub const TOPIC: &str = "control.tracer.claims";

pub const ANNOUNCE_INTERVAL_WARN_MS: u64 = 30_000;

const ERROR_LOG_INTERVAL_MS: u64 = 60_000;

struct ClaimContext {
    last_error_log_ms: AtomicU64,
}

impl ClaimContext {
    fn new() -> Self {
        Self {
            last_error_log_ms: AtomicU64::new(0),
        }
    }

    fn should_log(&self, now_ms: u64) -> bool {
        let last = self.last_error_log_ms.load(Ordering::Relaxed);
        now_ms.saturating_sub(last) >= ERROR_LOG_INTERVAL_MS
            && self
                .last_error_log_ms
                .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
    }
}

impl ClientContext for ClaimContext {}

impl ProducerContext for ClaimContext {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {
        if let Err((e, _)) = result
            && self.should_log(wall_now_ms())
        {
            eprintln!("tracer claim delivery failed: {e}");
        }
    }
}

pub struct ClaimPublisher {
    producer: BaseProducer<ClaimContext>,
    claims: Vec<(String, TracerClaimRole)>,
    trace_id: String,
}

impl ClaimPublisher {
    pub fn start(
        brokers: Option<&str>,
        trades_binding: &Path,
        books_binding: Option<&Path>,
        symbol_stem: &str,
        poll_ms: u64,
    ) -> Option<Self> {
        let brokers = match brokers {
            Some(b) if !b.is_empty() => b,
            _ => {
                println!("KAFKA_BROKERS not set - tracer claims disabled");
                return None;
            }
        };
        let halves = [
            (Some(trades_binding), TracerClaimRole::Trades),
            (books_binding, TracerClaimRole::Orderbook),
        ];
        let mut claims = Vec::new();
        for (path, role) in halves {
            let Some(path) = path else { continue };
            match binding_dir_id(path) {
                Some(id) => claims.push((id, role)),
                None => eprintln!(
                    "tracer claim skipped: {} basename is not a binding uuid",
                    path.display()
                ),
            }
        }
        if claims.is_empty() {
            eprintln!("tracer claims disabled: no binding uuid to announce");
            return None;
        }
        let producer: BaseProducer<ClaimContext> = match ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create_with_context(ClaimContext::new())
        {
            Ok(p) => p,
            Err(e) => {
                eprintln!("tracer claims disabled: producer creation failed: {e}");
                return None;
            }
        };
        if poll_ms > ANNOUNCE_INTERVAL_WARN_MS {
            eprintln!(
                "tracer claims: poll interval {poll_ms} ms exceeds {ANNOUNCE_INTERVAL_WARN_MS} ms; claims can lapse between announcements and the platform delete guard may not hold"
            );
        }
        println!(
            "tracer claims: announcing {} binding(s) on {TOPIC}",
            claims.len()
        );
        Some(Self {
            producer,
            claims,
            trace_id: format!("live:{symbol_stem}"),
        })
    }

    /// announce the pair's claims ONLY when a live trace is
    /// actively running (`EngineStatus::live_trace_active`). Returns whether it
    /// announced, so the gate is unit-testable without a broker. An idle /
    /// paused / half-broken pair announces nothing; its overseer claim ages out
    /// and the collector becomes deletable.
    pub fn announce_if_active(
        &self,
        wall_ms: u64,
        status: &tracer_engine::engine::EngineStatus,
    ) -> bool {
        if !status.live_trace_active() {
            return false;
        }
        self.announce(wall_ms);
        true
    }

    pub fn announce(&self, wall_ms: u64) {
        for (binding_id, role) in &self.claims {
            let payload = encode_claim(binding_id, &self.trace_id, *role, wall_ms);
            let record = BaseRecord::to(TOPIC)
                .key(binding_id.as_str())
                .payload(&payload);
            if let Err((e, _)) = self.producer.send(record)
                && self.producer.context().should_log(wall_ms)
            {
                eprintln!("tracer claim enqueue failed: {e}");
            }
        }
        self.producer.poll(Duration::ZERO);
    }
}

fn binding_dir_id(dir: &Path) -> Option<String> {
    let name = dir.file_name()?.to_str()?;
    Uuid::parse_str(name).ok().map(|u| u.to_string())
}

fn encode_claim(binding_id: &str, trace_id: &str, role: TracerClaimRole, wall_ms: u64) -> Vec<u8> {
    TracerClaim {
        binding_id: binding_id.to_string(),
        trace_id: trace_id.to_string(),
        role: role as i32,
        announced_at: Some(prost_types::Timestamp {
            seconds: (wall_ms / 1000) as i64,
            nanos: ((wall_ms % 1000) * 1_000_000) as i32,
        }),
    }
    .encode_to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_binding_dir_basename_is_the_claim_id() {
        let dir = Path::new("/opt/aetelier/datasets/585ccad2-0110-4abe-bfe3-a5ac63eba26d");
        assert_eq!(
            binding_dir_id(dir).as_deref(),
            Some("585ccad2-0110-4abe-bfe3-a5ac63eba26d")
        );
    }

    #[test]
    fn a_trailing_slash_does_not_change_the_claim_id() {
        let dir = Path::new("/opt/aetelier/datasets/585ccad2-0110-4abe-bfe3-a5ac63eba26d/");
        assert_eq!(
            binding_dir_id(dir).as_deref(),
            Some("585ccad2-0110-4abe-bfe3-a5ac63eba26d")
        );
    }

    #[test]
    fn a_non_uuid_basename_never_becomes_a_claim() {
        assert!(binding_dir_id(Path::new("./fixtures/trades")).is_none());
    }

    #[test]
    fn a_claim_round_trips_with_role_and_timestamp() {
        let payload = encode_claim(
            "585ccad2-0110-4abe-bfe3-a5ac63eba26d",
            "live:xyz_TSLA",
            TracerClaimRole::Orderbook,
            1_756_300_000_123,
        );
        let msg = TracerClaim::decode(payload.as_slice()).unwrap();
        assert_eq!(msg.binding_id, "585ccad2-0110-4abe-bfe3-a5ac63eba26d");
        assert_eq!(msg.trace_id, "live:xyz_TSLA");
        assert_eq!(msg.role, TracerClaimRole::Orderbook as i32);
        let ts = msg.announced_at.unwrap();
        assert_eq!(ts.seconds, 1_756_300_000);
        assert_eq!(ts.nanos, 123_000_000);
    }

    #[test]
    fn wire_role_values_match_the_consumer_contract() {
        assert_eq!(TracerClaimRole::Unspecified as i32, 0);
        assert_eq!(TracerClaimRole::Orderbook as i32, 1);
        assert_eq!(TracerClaimRole::Trades as i32, 2);
    }

    #[test]
    fn missing_or_empty_brokers_disable_claims() {
        let dir = Path::new("/d/585ccad2-0110-4abe-bfe3-a5ac63eba26d");
        assert!(ClaimPublisher::start(None, dir, None, "xyz_TSLA", 2_000).is_none());
        assert!(ClaimPublisher::start(Some(""), dir, None, "xyz_TSLA", 2_000).is_none());
    }

    #[test]
    fn a_publisher_needs_at_least_one_binding_uuid() {
        let publisher = ClaimPublisher::start(
            Some("kafka:9092"),
            Path::new("./fixtures/trades"),
            None,
            "xyz_TSLA",
            2_000,
        );
        assert!(publisher.is_none());
    }

    #[test]
    fn the_warn_bound_keeps_three_missed_beats_inside_the_consumer_ttl() {
        let overseer_claim_ttl_ms: u64 = 90_000;
        assert!(ANNOUNCE_INTERVAL_WARN_MS * 3 <= overseer_claim_ttl_ms);
    }

    #[test]
    fn delivery_errors_log_at_most_once_per_interval() {
        let ctx = ClaimContext::new();
        assert!(ctx.should_log(1_000_000));
        assert!(!ctx.should_log(1_000_000 + ERROR_LOG_INTERVAL_MS - 1));
        assert!(ctx.should_log(1_000_000 + ERROR_LOG_INTERVAL_MS));
    }
}

#[cfg(test)]
mod gate_tests {
    use super::*;
    use tracer_engine::engine::EngineStatus;

    fn publisher() -> ClaimPublisher {
        // Broker is never contacted: BaseRecord::send only enqueues, and these
        // tests assert the GATE decision, not delivery.
        ClaimPublisher::start(
            Some("127.0.0.1:59999"),
            Path::new("/opt/aetelier/datasets/585ccad2-0110-4abe-bfe3-a5ac63eba26d"),
            Some(Path::new(
                "/opt/aetelier/datasets/111ccad2-0110-4abe-bfe3-a5ac63eba26d",
            )),
            "xyz_TSLA",
            2_000,
        )
        .expect("publisher constructs with a broker string and uuid bindings")
    }

    fn status(ready: bool, idle: bool, binding_pending: bool) -> EngineStatus {
        EngineStatus {
            ready,
            idle,
            binding_pending,
            ..Default::default()
        }
    }

    #[test]
    fn announces_only_while_a_live_trace_is_active() {
        let p = publisher();
        assert!(
            p.announce_if_active(1_000, &status(true, false, false)),
            "fresh, ready, resolved pair = active = must announce (claim held)"
        );
    }

    #[test]
    fn does_not_announce_when_idle() {
        let p = publisher();
        assert!(
            !p.announce_if_active(1_000, &status(true, true, false)),
            "idle pair must NOT announce — collector becomes deletable"
        );
    }

    #[test]
    fn does_not_announce_when_binding_pending() {
        let p = publisher();
        assert!(!p.announce_if_active(1_000, &status(true, false, true)));
    }

    #[test]
    fn does_not_announce_before_ready() {
        let p = publisher();
        assert!(!p.announce_if_active(1_000, &status(false, false, false)));
    }
}
