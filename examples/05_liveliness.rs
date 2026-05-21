//! ## 05 - Liveliness: 心跳存活检测
//!
//! 大型机器人系统中,需要检测每个机器人/服务是否在线:
//!   - 机器人定期发心跳, 控制中心监控
//!   - 关键服务挂了需要告警
//!   - 自动故障切换 (failover)
//!
//! Zenoh 的 LivelinessToken 提供了声明式的存活检测机制:
//!   服务端声明 token → Zenoh 自动维护
//!   客户端查询 liveliness → 获取在线服务列表

use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::time::sleep;
use zenoh::Config;

macro_rules! ze {
    ($expr:expr) => { $expr.await.map_err(|e| anyhow::anyhow!("{e}"))? };
}

static NEXT_PORT: AtomicU16 = AtomicU16::new(17500);

fn pick_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::Relaxed)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let port = pick_port();
    let endpoint = format!("tcp/127.0.0.1:{port}");

    let session = ze!(zenoh::open(listen_config(&endpoint)));
    let session_robot1 = ze!(zenoh::open(connect_config(&endpoint)));
    let session_robot2 = ze!(zenoh::open(connect_config(&endpoint)));

    println!("═══ Zenoh Liveliness 测验 ═══\n");
    println!("[Setup] 控制中心监听 {endpoint}, 机器人通过 TCP loopback 连接\n");
    sleep(Duration::from_millis(500)).await;

    // ── 注册存活令牌 ──────────────────────────────────────

    println!("── 注册服务存活令牌 ──\n");

    let _token_r1 = ze!(session_robot1
        .liveliness()
        .declare_token("fleet/f1/robot/r1"));

    let _token_r2 = ze!(session_robot2
        .liveliness()
        .declare_token("fleet/f1/robot/r2"));

    println!("  [Robot-1] 存活令牌已声明: fleet/f1/robot/r1");
    println!("  [Robot-2] 存活令牌已声明: fleet/f1/robot/r2\n");

    sleep(Duration::from_millis(500)).await;

    // ── 控制中心查询存活 ──────────────────────────────────

    println!("── 控制中心查询存活 ──\n");

    let replies = ze!(session
        .liveliness()
        .get("fleet/f1/robot/*")
        .timeout(Duration::from_secs(3)));

    let mut alive = Vec::new();
    while let Ok(reply) = replies.recv_async().await {
        let key = reply.result().unwrap().key_expr().to_string();
        alive.push(key.clone());
        println!("  [Control] 在线: {key}");
    }

    println!("\n── 模拟 Robot-2 下线 ──\n");

    drop(_token_r2);
    sleep(Duration::from_millis(1000)).await;

    println!("[Control] 重新查询...");
    let replies = ze!(session
        .liveliness()
        .get("fleet/f1/robot/*")
        .timeout(Duration::from_secs(3)));

    let mut alive2 = Vec::new();
    while let Ok(reply) = replies.recv_async().await {
        let key = reply.result().unwrap().key_expr().to_string();
        alive2.push(key.clone());
        println!("  [Control] 在线: {key}");
    }

    // ── 声明式订阅 ──────────────────────────────────────

    println!("\n── 声明式订阅 (自动感知变化) ──\n");

    let sub = ze!(session
        .liveliness()
        .declare_subscriber("fleet/f1/robot/*"));

    let session_robot2b = ze!(zenoh::open(connect_config(&endpoint)));
    let _token_r2b = ze!(session_robot2b
        .liveliness()
        .declare_token("fleet/f1/robot/r2"));

    sleep(Duration::from_millis(500)).await;

    if let Ok(sample) = sub.recv_async().await {
        let payload = sample.payload().try_to_string().unwrap_or_default();
        let kind = if payload.is_empty() { "下线" } else { "上线" };
        println!("  [Subscriber] 感知变化: {} {kind}", sample.key_expr());
    }

    println!("\n═══ 总结 ═══");
    println!("  初始在线: {} 个 → 掉线后: {} 个", alive.len(), alive2.len());
    println!();
    println!("  API:");
    println!("    session.liveliness().declare_token(key)       // 声明存活");
    println!("    session.liveliness().get(key).recv()          // 主动查询");
    println!("    session.liveliness().declare_subscriber(key)  // 被动感知 (推荐)");
    println!();
    println!("  实际场景:");
    println!("    控制中心用 declare_subscriber 监控所有机器人");
    println!("    机器人 token 被 drop 时自动触发下线告警 ✓");
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