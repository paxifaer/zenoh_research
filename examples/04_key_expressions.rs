//! ## 04 - Key Expressions: 分层 Key 与通配符路由
//!
//! 机器人系统中典型的 key 层次结构:
//!
//!   fleet/{fleet_id}/robot/{robot_id}/sensor/{type}/{name}
//!   fleet/{fleet_id}/robot/{robot_id}/cmd/{action}
//!   fleet/{fleet_id}/robot/{robot_id}/status/health
//!
//! 通配符:
//!   *     匹配单层 (如 robot/*/status)
//!   **    匹配多层 (如 fleet/**/health)
//!
//! 本示例演示如何用层次化 key 实现精准路由和批量订阅。

use std::time::Duration;
use tokio::time::sleep;
use zenoh::Config;

macro_rules! ze {
    ($expr:expr) => { $expr.await.map_err(|e| anyhow::anyhow!("{e}"))? };
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let session = ze!(zenoh::open(local_config()));

    println!("═══ Key Expressions 测验 ═══\n");

    test_exact_match(&session).await?;
    test_single_wildcard(&session).await?;
    test_multi_wildcard(&session).await?;

    println!("\n═══ 总结 ═══");
    println!("  fleet/f1/robot/r1/sensor/lidar  → 精确匹配");
    println!("  fleet/f1/robot/*/sensor/lidar   → 匹配所有机器人的 lidar");
    println!("  fleet/f1/**/health              → 匹配 fleet 下所有 health");
    Ok(())
}

fn local_config() -> Config {
    let mut c = Config::default();
    c.insert_json5("scouting/multicast/enabled", "false").ok();
    c.insert_json5("scouting/gossip/enabled", "false").ok();
    c.insert_json5("listen/endpoints", "[]").ok();
    c
}

// ── 精确匹配 ───────────────────────────────────────────────────────────

async fn test_exact_match(session: &zenoh::Session) -> anyhow::Result<()> {
    println!("── 精确匹配 ──\n");

    // 先订阅,再发布
    let sub = ze!(session.declare_subscriber("fleet/f1/robot/r1/sensor/lidar"));
    sleep(Duration::from_millis(50)).await;

    ze!(session.declare_publisher("fleet/f1/robot/r1/sensor/lidar"))
        .put("data from r1/lidar").await.map_err(|e| anyhow::anyhow!("{e}"))?;
    ze!(session.declare_publisher("fleet/f1/robot/r1/sensor/imu"))
        .put("data from r1/imu").await.map_err(|e| anyhow::anyhow!("{e}"))?;
    ze!(session.declare_publisher("fleet/f1/robot/r2/sensor/lidar"))
        .put("data from r2/lidar").await.map_err(|e| anyhow::anyhow!("{e}"))?;
    sleep(Duration::from_millis(100)).await;

    let mut count = 0;
    while let Ok(sample) = sub.recv_async().await {
        println!("  精确订阅收到: {}", sample.payload().try_to_string().unwrap_or_default());
        count += 1;
        if count >= 1 {
            break;
        }
    }

    println!("  结果: 精确匹配只收到 1 条 (r1/lidar), 不会收到 imu 或 r2/lidar\n");
    Ok(())
}

// ── 单层通配符 * ───────────────────────────────────────────────────────

async fn test_single_wildcard(session: &zenoh::Session) -> anyhow::Result<()> {
    println!("── 单层通配符 * (匹配所有机器人) ──\n");

    // 先订阅
    let sub = ze!(session.declare_subscriber("fleet/f1/robot/*/sensor/lidar"));
    sleep(Duration::from_millis(50)).await;

    // 再发布
    for i in 1..=3 {
        let topic = format!("fleet/f1/robot/r{i}/sensor/lidar");
        ze!(session.declare_publisher(&topic))
            .put(format!("lidar data r{i}")).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    sleep(Duration::from_millis(100)).await;

    let mut count = 0;
    while let Ok(sample) = sub.recv_async().await {
        println!("  通配符订阅收到: {}", sample.payload().try_to_string().unwrap_or_default());
        count += 1;
        if count >= 3 {
            break;
        }
    }

    println!("  结果: 单层通配符 * 收到 3 条 (r1, r2, r3 的 lidar)\n");
    Ok(())
}

// ── 多层通配符 ** ──────────────────────────────────────────────────────

async fn test_multi_wildcard(session: &zenoh::Session) -> anyhow::Result<()> {
    println!("── 多层通配符 ** (深度匹配) ──\n");

    // 先订阅
    let sub = ze!(session.declare_subscriber("fleet/f1/**/health"));
    sleep(Duration::from_millis(50)).await;

    // 发布到不同深度的 health
    let topics = [
        "fleet/f1/health",
        "fleet/f1/robot/r1/status/health",
        "fleet/f1/robot/r2/status/health",
    ];

    for (i, topic) in topics.iter().enumerate() {
        ze!(session.declare_publisher(*topic))
            .put(format!("health report {}", i + 1)).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    sleep(Duration::from_millis(100)).await;

    let mut count = 0;
    while let Ok(sample) = sub.recv_async().await {
        println!("  ** 订阅收到: {}", sample.payload().try_to_string().unwrap_or_default());
        count += 1;
        if count >= 3 {
            break;
        }
    }

    println!("  结果: ** 跨越多个层级, 收到 3 条所有 health 消息\n");
    Ok(())
}