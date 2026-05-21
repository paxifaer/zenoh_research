//! ## 02 - QoS Profiles: 可靠性 / 优先级 / 拥塞控制
//!
//! 大型机器人系统中,不同数据流需要不同的 QoS:
//!   - 传感器流: BestEffort + Drop (速度优先,丢几帧无所谓)
//!   - 控制指令: Reliable + Block (必须送达,宁可阻塞)
//!   - 心跳信号: BestEffort + Background (低优先级保活)
//!   - 大块传输: Reliable + DataLow (可靠但不抢占资源)
//!
//! 本示例展示四种典型 QoS 配置及其行为差异。

use std::time::Duration;
use tokio::time::sleep;
use zenoh::qos::{CongestionControl, Priority, Reliability};
use zenoh::Config;

macro_rules! ze {
    ($expr:expr) => { $expr.await.map_err(|e| anyhow::anyhow!("{e}"))? };
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let session = ze!(zenoh::open(local_config()));

    println!("═══ Zenoh QoS Profiles 测验 ═══\n");

    test_reliability(&session).await?;
    test_priority(&session).await?;
    test_congestion_control(&session).await?;

    println!("\n═══ 总结 ═══");
    println!("  传感器流:  Reliability::BestEffort + CongestionControl::Drop   + Priority::Data");
    println!("  控制指令:  Reliability::Reliable    + CongestionControl::Block  + Priority::InteractiveHigh");
    println!("  心跳信号:  Reliability::BestEffort + CongestionControl::Drop   + Priority::Background");
    println!("  大块传输:  Reliability::Reliable    + CongestionControl::Block  + Priority::DataLow");
    Ok(())
}

fn local_config() -> Config {
    let mut c = Config::default();
    c.insert_json5("scouting/multicast/enabled", "false").ok();
    c.insert_json5("scouting/gossip/enabled", "false").ok();
    c.insert_json5("listen/endpoints", "[]").ok();
    c
}

// ── 可靠性测试 ─────────────────────────────────────────────────────────

async fn test_reliability(session: &zenoh::Session) -> anyhow::Result<()> {
    println!("── 可靠性: Reliable vs BestEffort ──\n");

    let topic = "qos_test/reliability";

    // Subscriber
    let sub = ze!(session.declare_subscriber(topic));
    sleep(Duration::from_millis(100)).await;

    // Reliable publisher
    let pub_reliable = ze!(session
        .declare_publisher(topic)
        .reliability(Reliability::Reliable));

    // BestEffort publisher
    let pub_best = ze!(session
        .declare_publisher(topic)
        .reliability(Reliability::BestEffort));

    // 发送
    ze!(pub_reliable.put("RELIABLE: critical command"));
    ze!(pub_best.put("BEST_EFFORT: sensor reading"));

    // 接收
    let sample = sub.recv_async().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    println!("  收到: {}", sample.payload().try_to_string().unwrap_or_default());

    let sample = sub.recv_async().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    println!("  收到: {}", sample.payload().try_to_string().unwrap_or_default());

    println!("  结果: 两者都收到。BestEffort 在拥塞时会丢弃,本地测试无差异。\n");
    Ok(())
}

// ── 优先级测试 ─────────────────────────────────────────────────────────

async fn test_priority(session: &zenoh::Session) -> anyhow::Result<()> {
    println!("── 优先级对比 ──\n");

    let priorities = [
        ("RealTime", Priority::RealTime),
        ("InteractiveHigh", Priority::InteractiveHigh),
        ("DataHigh", Priority::DataHigh),
        ("Data", Priority::Data),
        ("DataLow", Priority::DataLow),
        ("Background", Priority::Background),
    ];

    for (name, prio) in &priorities {
        let topic = format!("qos_test/prio/{name}");
        let sub = ze!(session.declare_subscriber(&topic));
        sleep(Duration::from_millis(50)).await;

        let pub_ = ze!(session
            .declare_publisher(&topic)
            .priority(prio.clone()));

        ze!(pub_.put(format!("message with {name} priority")));

        if let Ok(_sample) = sub.recv_async().await {
            println!("  [{name:>16}] 优先级生效, 消息已送达");
        }
    }

    println!("  结果: Zenoh 路由器会根据优先级调度消息,高优先级先转发。\n");
    Ok(())
}

// ── 拥塞控制测试 ───────────────────────────────────────────────────────

async fn test_congestion_control(session: &zenoh::Session) -> anyhow::Result<()> {
    println!("── 拥塞控制: Drop vs Block ──\n");

    let topic = "qos_test/congestion";

    // Drop: 队列满时丢弃新消息
    let drop_topic = format!("{topic}/drop");
    let pub_drop = ze!(session
        .declare_publisher(&drop_topic)
        .congestion_control(CongestionControl::Drop));

    // Block: 队列满时阻塞发送方
    let block_topic = format!("{topic}/block");
    let pub_block = ze!(session
        .declare_publisher(&block_topic)
        .congestion_control(CongestionControl::Block));

    let sub_drop = ze!(session.declare_subscriber(&drop_topic));
    let sub_block = ze!(session.declare_subscriber(&block_topic));
    sleep(Duration::from_millis(100)).await;

    ze!(pub_drop.put("drop ok"));
    ze!(pub_block.put("block ok"));

    println!("  Drop:  {}", sub_drop.recv_async().await.map_err(|e| anyhow::anyhow!("{e}"))?.payload().try_to_string().unwrap_or_default());
    println!("  Block: {}", sub_block.recv_async().await.map_err(|e| anyhow::anyhow!("{e}"))?.payload().try_to_string().unwrap_or_default());

    println!("  结果: 正常负载无差异。高负载时 Drop 丢帧不阻塞, Block 确保送达。\n");
    Ok(())
}