use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Unique identifier for a tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TickId(pub u64);

/// Unique identifier for a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub u64);

/// Priority levels for ticks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TickPriority {
    Info,
    Normal,
    Urgent,
    Critical,
}

/// A message left by one agent for another (or broadcast).
#[derive(Debug, Clone)]
pub struct Tick {
    pub id: TickId,
    pub from_agent: String,
    pub to_agent: Option<String>,
    pub topic: String,
    pub body: String,
    pub priority: TickPriority,
    pub timestamp: u64,
    pub ttl_ms: u64,
    pub acked_by: Vec<String>,
}

impl Tick {
    /// Returns true if this tick has expired based on current time.
    pub fn is_expired(&self) -> bool {
        if self.ttl_ms == 0 {
            return false;
        }
        now_ms() > self.timestamp + self.ttl_ms
    }
}

/// Filter for querying ticks.
#[derive(Debug, Clone, Default)]
pub struct TickFilter {
    pub target: Option<String>,
    pub topic: Option<String>,
    pub min_priority: Option<TickPriority>,
    pub since: Option<u64>,
}

/// Acknowledgment when an agent reads/acts on a tick.
#[derive(Debug, Clone)]
pub struct TickAck {
    pub tick_id: TickId,
    pub agent: String,
    pub timestamp: u64,
    pub action_taken: String,
}

#[derive(Debug)]
struct Subscription {
    agent: String,
    topics: Vec<String>,
    last_poll_timestamp: u64,
}

/// Shared board where ticks are posted and consumed.
pub struct TickBoard {
    ticks: Mutex<Vec<Tick>>,
    acks: Mutex<Vec<TickAck>>,
    subscriptions: Mutex<Vec<Subscription>>,
    next_tick_id: AtomicU64,
    next_sub_id: AtomicU64,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis() as u64
}

impl TickBoard {
    pub fn new() -> Self {
        Self {
            ticks: Mutex::new(Vec::new()),
            acks: Mutex::new(Vec::new()),
            subscriptions: Mutex::new(Vec::new()),
            next_tick_id: AtomicU64::new(1),
            next_sub_id: AtomicU64::new(1),
        }
    }

    /// Post a new tick to the board. Returns the tick's ID.
    pub fn post(
        &self,
        from_agent: &str,
        to_agent: Option<&str>,
        topic: &str,
        body: &str,
        priority: TickPriority,
        ttl_ms: u64,
    ) -> TickId {
        let id = TickId(self.next_tick_id.fetch_add(1, Ordering::Relaxed));
        let tick = Tick {
            id,
            from_agent: from_agent.to_string(),
            to_agent: to_agent.map(|s| s.to_string()),
            topic: topic.to_string(),
            body: body.to_string(),
            priority,
            timestamp: now_ms(),
            ttl_ms,
            acked_by: Vec::new(),
        };
        self.ticks.lock().unwrap().push(tick);
        id
    }

    /// Read ticks matching the given filter.
    pub fn read(&self, filter: &TickFilter) -> Vec<Tick> {
        self.ticks
            .lock()
            .unwrap()
            .iter()
            .filter(|t| !t.is_expired())
            .filter(|t| match &filter.target {
                Some(target) => {
                    // Match if to_agent is the target, or if broadcast (None)
                    t.to_agent.as_deref() == Some(target.as_str())
                        || t.to_agent.is_none()
                }
                None => true,
            })
            .filter(|t| match &filter.topic {
                Some(topic) => t.topic == *topic,
                None => true,
            })
            .filter(|t| match &filter.min_priority {
                Some(min) => t.priority >= *min,
                None => true,
            })
            .filter(|t| match filter.since {
                Some(since) => t.timestamp >= since,
                None => true,
            })
            .cloned()
            .collect()
    }

    /// Acknowledge a tick.
    pub fn ack(&self, tick_id: TickId, agent: &str, action_taken: &str) -> TickAck {
        let mut ticks = self.ticks.lock().unwrap();
        if let Some(tick) = ticks.iter_mut().find(|t| t.id == tick_id) {
            if !tick.acked_by.contains(&agent.to_string()) {
                tick.acked_by.push(agent.to_string());
            }
        }
        let ack = TickAck {
            tick_id,
            agent: agent.to_string(),
            timestamp: now_ms(),
            action_taken: action_taken.to_string(),
        };
        self.acks.lock().unwrap().push(ack.clone());
        ack
    }

    /// Subscribe an agent to topics. Returns a subscription ID.
    pub fn subscribe(&self, agent: &str, topics: Vec<String>) -> SubscriptionId {
        let id = SubscriptionId(self.next_sub_id.fetch_add(1, Ordering::Relaxed));
        let sub = Subscription {
            agent: agent.to_string(),
            topics,
            last_poll_timestamp: now_ms(),
        };
        self.subscriptions.lock().unwrap().push(sub);
        id
    }

    /// Poll for new ticks matching a subscription since last poll.
    pub fn poll(&self, subscription_id: SubscriptionId) -> Vec<Tick> {
        let mut subs = self.subscriptions.lock().unwrap();
        let idx = (subscription_id.0 as usize).wrapping_sub(1);
        if idx >= subs.len() {
            return Vec::new();
        }
        let sub = &mut subs[idx];

        let since = sub.last_poll_timestamp;
        let topics = sub.topics.clone();
        let agent = sub.agent.clone();
        let now = now_ms();
        sub.last_poll_timestamp = now;

        self.ticks
            .lock()
            .unwrap()
            .iter()
            .filter(|t| !t.is_expired())
            .filter(|t| t.timestamp >= since)
            .filter(|t| {
                // Match if broadcast or addressed to this agent
                t.to_agent.is_none() || t.to_agent.as_deref() == Some(agent.as_str())
            })
            .filter(|t| topics.iter().any(|topic| t.topic == *topic))
            .cloned()
            .collect()
    }

    /// Remove ticks older than max_age_ms. Returns count removed.
    pub fn cleanup(&self, max_age_ms: u64) -> usize {
        let cutoff = now_ms().saturating_sub(max_age_ms);
        let mut ticks = self.ticks.lock().unwrap();
        let before = ticks.len();
        ticks.retain(|t| t.timestamp >= cutoff);
        before - ticks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_board() -> TickBoard {
        TickBoard::new()
    }

    #[test]
    fn post_creates_tick_with_id() {
        let board = make_board();
        let id = board.post("agent-a", None, "test", "hello", TickPriority::Normal, 0);
        assert_eq!(id, TickId(1));

        let id2 = board.post("agent-b", Some("agent-a"), "reply", "world", TickPriority::Urgent, 0);
        assert_eq!(id2, TickId(2));
    }

    #[test]
    fn read_returns_posted_ticks() {
        let board = make_board();
        board.post("agent-a", None, "test", "hello", TickPriority::Normal, 0);
        board.post("agent-b", None, "test", "world", TickPriority::Normal, 0);

        let ticks = board.read(&TickFilter::default());
        assert_eq!(ticks.len(), 2);
    }

    #[test]
    fn read_with_filter_returns_matching_ticks_only() {
        let board = make_board();
        board.post("agent-a", Some("agent-b"), "direct", "for you", TickPriority::Normal, 0);
        board.post("agent-a", None, "broadcast", "everyone", TickPriority::Normal, 0);

        let filter = TickFilter {
            target: Some("agent-b".to_string()),
            ..Default::default()
        };
        let ticks = board.read(&filter);
        assert_eq!(ticks.len(), 2); // both direct and broadcast
        assert!(ticks.iter().any(|t| t.topic == "direct"));
        assert!(ticks.iter().any(|t| t.topic == "broadcast"));
    }

    #[test]
    fn read_with_topic_filter_works() {
        let board = make_board();
        board.post("a", None, "alerts", "alert!", TickPriority::Normal, 0);
        board.post("a", None, "info", "fyi", TickPriority::Normal, 0);

        let filter = TickFilter {
            topic: Some("alerts".to_string()),
            ..Default::default()
        };
        let ticks = board.read(&filter);
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].topic, "alerts");
    }

    #[test]
    fn read_with_min_priority_filters_correctly() {
        let board = make_board();
        board.post("a", None, "t", "info", TickPriority::Info, 0);
        board.post("a", None, "t", "normal", TickPriority::Normal, 0);
        board.post("a", None, "t", "urgent", TickPriority::Urgent, 0);
        board.post("a", None, "t", "critical", TickPriority::Critical, 0);

        let filter = TickFilter {
            min_priority: Some(TickPriority::Urgent),
            ..Default::default()
        };
        let ticks = board.read(&filter);
        assert_eq!(ticks.len(), 2);
        assert!(ticks.iter().all(|t| t.priority >= TickPriority::Urgent));
    }

    #[test]
    fn ack_records_acknowledgment() {
        let board = make_board();
        let id = board.post("a", None, "t", "hello", TickPriority::Normal, 0);
        let ack = board.ack(id, "agent-b", "read and handled");
        assert_eq!(ack.tick_id, id);
        assert_eq!(ack.agent, "agent-b");
        assert_eq!(ack.action_taken, "read and handled");

        let ticks = board.read(&TickFilter::default());
        assert!(ticks[0].acked_by.contains(&"agent-b".to_string()));
    }

    #[test]
    fn acked_by_tracks_multiple_agents() {
        let board = make_board();
        let id = board.post("a", None, "t", "hello", TickPriority::Normal, 0);
        board.ack(id, "agent-b", "handled");
        board.ack(id, "agent-c", "also handled");

        let ticks = board.read(&TickFilter::default());
        assert_eq!(ticks[0].acked_by.len(), 2);
        assert!(ticks[0].acked_by.contains(&"agent-b".to_string()));
        assert!(ticks[0].acked_by.contains(&"agent-c".to_string()));
    }

    #[test]
    fn subscribe_creates_subscription() {
        let board = make_board();
        let sub_id = board.subscribe("agent-x", vec!["alerts".to_string(), "tasks".to_string()]);
        assert_eq!(sub_id, SubscriptionId(1));
    }

    #[test]
    fn poll_returns_new_ticks_since_last_poll() {
        let board = make_board();
        let sub_id = board.subscribe("agent-x", vec!["alerts".to_string()]);

        // Post a matching tick after subscribe
        board.post("agent-a", None, "alerts", "fire!", TickPriority::Urgent, 0);
        board.post("agent-a", None, "info", "fyi", TickPriority::Info, 0);

        let ticks = board.poll(sub_id);
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].topic, "alerts");
    }

    #[test]
    fn poll_with_subscription_only_returns_matching_topics() {
        let board = make_board();
        let sub_id = board.subscribe("agent-x", vec!["alerts".to_string()]);

        board.post("a", None, "alerts", "a1", TickPriority::Normal, 0);
        board.post("a", None, "tasks", "t1", TickPriority::Normal, 0);
        board.post("a", None, "alerts", "a2", TickPriority::Normal, 0);

        let ticks = board.poll(sub_id);
        assert_eq!(ticks.len(), 2);
        assert!(ticks.iter().all(|t| t.topic == "alerts"));
    }

    #[test]
    fn cleanup_removes_old_ticks() {
        let board = make_board();
        board.post("a", None, "t", "old", TickPriority::Normal, 0);
        board.post("a", None, "t", "also old", TickPriority::Normal, 0);

        // Wait a moment so the ticks are now in the past
        std::thread::sleep(std::time::Duration::from_millis(5));

        // With max_age_ms=1, ticks from >1ms ago should be removed
        let removed = board.cleanup(1);
        assert_eq!(removed, 2);
        assert_eq!(board.read(&TickFilter::default()).len(), 0);
    }

    #[test]
    fn broadcast_ticks_reach_all_subscribers() {
        let board = make_board();
        let sub1 = board.subscribe("agent-x", vec!["news".to_string()]);
        let sub2 = board.subscribe("agent-y", vec!["news".to_string()]);

        board.post("source", None, "news", "breaking!", TickPriority::Normal, 0);

        let t1 = board.poll(sub1);
        let t2 = board.poll(sub2);
        assert_eq!(t1.len(), 1);
        assert_eq!(t2.len(), 1);
    }

    #[test]
    fn ttl_expired_ticks_are_marked_expired() {
        let board = make_board();
        // TTL of 1ms — will expire almost immediately
        let id = board.post("a", None, "t", "ephemeral", TickPriority::Normal, 1);

        // Wait for expiry
        std::thread::sleep(std::time::Duration::from_millis(10));

        let ticks = board.ticks.lock().unwrap();
        let tick = ticks.iter().find(|t| t.id == id).unwrap();
        assert!(tick.is_expired());

        // read() should not return expired ticks
        drop(ticks);
        let visible = board.read(&TickFilter::default());
        assert_eq!(visible.len(), 0);
    }
}
