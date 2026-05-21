//! ## 06 - Multi-Publisher: 多发布者同一 Topic
//!
//! 多机器人场景中最常见的模式:
//!   - 10 台机器人同时发布传感器数据
//!   - 控制中心订阅通配符接收所有机器人的数据
//!   - 每台机器人也可以发布到相同的共享 topic
//!
//! 本示例模拟 3 个机器人同时发布,控制中心订阅汇总。

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use zenoh::Config;

macro_rules! ze {
    ($expr:expr) => { $expr.await.map_err(|e| anyhow::anyhow!("{e}"))? };
}

static NEXT_PORT: AtomicU16 = AtomicU16::new(17550);

fn pick_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::Relaxed)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("═══ 多发布者同 Topic 测验 ═══\n");

    let port = pick_port();
    let endpoint = format!("tcp/127.0.0.1:{port}");

    // 控制中心监听
    let control = Arc::new(ze!(zenoh::open(listen_config(&endpoint))));

    // 三台机器人连接到控制中心
    let mut robot_sessions = Vec::new();
    for _ in 0..3 {
        robot_sessions.push(Arc::new(ze!(zenoh::open(connect_config(&endpoint)))));
    }

    println!("[Setup] 控制中心监听 {endpoint}, 3 个机器人已连接\n");
    sleep(Duration::from_millis(500)).await;

    // ── 场景 1: 分层 topic, 每个机器人自己的 key ──────────

    println!("── 3 台机器人发布 odometry ──\n");

    let sub = ze!(control.declare_subscriber("fleet/**/odometry"));
    sleep(Duration::from_millis(100)).await;

    let topics = [
        "fleet/f1/robot/alpha/odometry",
        "fleet/f1/robot/bravo/odometry",
        "fleet/f1/robot/charlie/odometry",
    ];

    for (i, (topic, session)) in topics.iter().zip(robot_sessions.iter()).enumerate() {
        let pub_ = ze!(session.declare_publisher(*topic));
        ze!(pub_.put(format!("{{robot: '{}', x: {:.1}, y: {:.1}, theta: {:.2}}}",
            topic, i as f32 * 1.5, i as f32 * 2.0, i as f32 * 0.785)));
    }

    sleep(Duration::from_millis(200)).await;

    println!("[Control] 接收所有 odometry:\n");
    let mut received = 0;
    while let Ok(sample) = sub.recv_async().await {
        let key = sample.key_expr().to_string();
        let payload = sample.payload().try_to_string().unwrap_or_default();
        println!("  [{received}] {key}");
        println!("       → {payload}");
        received += 1;
        if received >= 3 {
            break;
        }
    }

    // ── 场景 2: 同一 key, 多对一 ───────────────────────────

    println!("\n── 同一 key, 多个发布者 (fleet/alerts) ──\n");

    let shared_topic = "fleet/alerts";
    let sub2 = ze!(control.declare_subscriber(shared_topic));
    sleep(Duration::from_millis(100)).await;

    let robots = ["alpha", "bravo", "charlie"];
    for (i, session) in robot_sessions.iter().enumerate() {
        let pub_ = ze!(session.declare_publisher(shared_topic));
        ze!(pub_.put(format!("ALERT from {}: low battery ({}%)", robots[i], 25 - i * 10)));
    }

    sleep(Duration::from_millis(200)).await;

    let mut alerts = 0;
    while let Ok(sample) = sub2.recv_async().await {
        println!("  [Control] 告警: {}", sample.payload().try_to_string().unwrap_or_default());
        alerts += 1;
        if alerts >= 3 {
            break;
        }
    }

    println!("\n═══ 总结 ═══");
    println!("  收到 {received} 条 odometry + {alerts} 条告警 = {} 条消息", received + alerts);
    println!("  多发布者模式:");
    println!("    - 分层 topic: fleet/**/odometry → 每个机器人自己的 key");
    println!("    - 共享 topic: fleet/alerts     → 多对一汇总");
    println!("  两种模式都适用于大型机器人系统 ✓");
    Ok(())
}

fn listen_config(endpoint: &str) -> Config {
    let mut c = Config::default();
    c.insert_json5("scouting/multicast/enabled", "false").ok();
    c.insert_json5("scouting/gossip/enabled", "false").ok();
    c.insert_json5("listen/endpoints", &format!("[\"{endpoint}\"]")).ok();
    c.insert_json5("connect/endpoints", "[]").ok();
    c
}

fn connect_config(endpoint: &str) -> Config {
    let mut c = Config::default();
    c.insert_json5("scouting/multicast/enabled", "false").ok();
    c.insert_json5("scouting/gossip/enabled", "false").ok();
    c.insert_json5("listen/endpoints", "[]").ok();
    c.insert_json5("connect/endpoints", &format!("[\"{endpoint}\"]")).ok();
    c
}