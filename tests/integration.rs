use plato_tick::*;

#[test]
fn test_tick_board_post_and_read() {
    let board = TickBoard::new();
    let id = board.post("agent-a", None, "alerts", "hello", TickPriority::Normal, 0);
    assert_eq!(id, TickId(1));
    let ticks = board.read(&TickFilter::default());
    assert_eq!(ticks.len(), 1);
    assert_eq!(ticks[0].body, "hello");
    assert_eq!(ticks[0].topic, "alerts");
}

#[test]
fn test_tick_board_directed_messages() {
    let board = TickBoard::new();
    board.post("a", Some("b"), "dm", "for b only", TickPriority::Normal, 0);
    board.post("a", None, "announce", "everyone", TickPriority::Normal, 0);

    let filter_b = TickFilter { target: Some("b".into()), ..Default::default() };
    let ticks_b = board.read(&filter_b);
    assert_eq!(ticks_b.len(), 2); // directed + broadcast

    let filter_c = TickFilter { target: Some("c".into()), ..Default::default() };
    let ticks_c = board.read(&filter_c);
    assert_eq!(ticks_c.len(), 1); // only broadcast
}

#[test]
fn test_tick_priority_ordering() {
    assert!(TickPriority::Critical > TickPriority::Urgent);
    assert!(TickPriority::Urgent > TickPriority::Normal);
    assert!(TickPriority::Normal > TickPriority::Info);
}

#[test]
fn test_tick_ack() {
    let board = TickBoard::new();
    let id = board.post("a", None, "t", "hello", TickPriority::Normal, 0);
    let ack = board.ack(id, "b", "handled");
    assert_eq!(ack.tick_id, id);
    assert_eq!(ack.agent, "b");
    let ticks = board.read(&TickFilter::default());
    assert!(ticks[0].acked_by.contains(&"b".to_string()));
}

#[test]
fn test_tick_subscribe_and_poll() {
    let board = TickBoard::new();
    let sub = board.subscribe("x", vec!["alerts".into(), "news".into()]);
    board.post("src", None, "alerts", "fire!", TickPriority::Urgent, 0);
    board.post("src", None, "chatter", "noise", TickPriority::Info, 0);
    board.post("src", None, "news", "update", TickPriority::Normal, 0);
    let ticks = board.poll(sub);
    assert_eq!(ticks.len(), 2); // alerts + news, not chatter
}

#[test]
fn test_tick_cleanup() {
    let board = TickBoard::new();
    board.post("a", None, "t", "old1", TickPriority::Normal, 0);
    board.post("a", None, "t", "old2", TickPriority::Normal, 0);
    std::thread::sleep(std::time::Duration::from_millis(10));
    let removed = board.cleanup(5);
    assert_eq!(removed, 2);
}

#[test]
fn test_tick_filter_by_topic() {
    let board = TickBoard::new();
    board.post("a", None, "alerts", "a1", TickPriority::Normal, 0);
    board.post("a", None, "tasks", "t1", TickPriority::Normal, 0);
    let filter = TickFilter { topic: Some("alerts".into()), ..Default::default() };
    assert_eq!(board.read(&filter).len(), 1);
}

#[test]
fn test_tick_filter_by_priority() {
    let board = TickBoard::new();
    board.post("a", None, "t", "info", TickPriority::Info, 0);
    board.post("a", None, "t", "normal", TickPriority::Normal, 0);
    board.post("a", None, "t", "urgent", TickPriority::Urgent, 0);
    let filter = TickFilter { min_priority: Some(TickPriority::Urgent), ..Default::default() };
    assert_eq!(board.read(&filter).len(), 1);
}
