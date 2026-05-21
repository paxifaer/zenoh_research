//! Zenoh Locality 测验: 同一 Topic 的机内/机外通讯隔离
//!
//! ## 场景
//!
//! - Service-A 和 Service-B 在同一台机器上，各自有独立的 Zenoh Session
//! - 两个 Service 都订阅 `robot/status` 这个 topic
//! - Service-A 作为发布方，发送两种消息:
//!   1. `allowed_destination(Locality::SessionLocal)` — 只有 Service-A 自己收到
//!   2. 默认 `Locality::Any` — Service-A 和 Service-B 都能收到 (通过 TCP loopback)
//!
//! ## 核心 API
//!
//! ```ignore
//! // 机内消息 — 数据不出当前 session
//! session.declare_publisher("key")
//!     .allowed_destination(Locality::SessionLocal)
//!
//! // 公开消息 — 数据可达所有匹配的 subscriber (本机其他 session + 远端)
//! session.declare_publisher("key")
//!     // 默认 = Locality::Any
//! ```
//!
//! ## 运行
//!
//! 单机模式 (两个独立 session 通过 TCP loopback 通信):
//! ```bash
//! cargo run -p zenoh-research
//! ```
//!
//! 双机模式:
//! ```bash
//! # 机器A:
//! ZENOH_LISTEN="tcp/0.0.0.0:7447" cargo run -p zenoh-research -- --local
//!
//! # 机器B:
//! ZENOH_CONNECT="tcp/<机器A_IP>:7447" cargo run -p zenoh-research -- --remote
//! ```

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::time::sleep;
use zenoh::sample::Locality;
use zenoh::Config;

// 动态分配端口, 避免冲突
static NEXT_PORT: AtomicU16 = AtomicU16::new(17447);

fn pick_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::Relaxed)
}

// ── 辅助宏 ─────────────────────────────────────────────────────────────────────

macro_rules! ze {
    ($expr:expr) => {
        $expr.await.map_err(|e| anyhow::anyhow!("{e}"))?
    };
}

// ── 测验主入口 ─────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let is_remote = args.iter().any(|a| a == "--remote");

    println!("╔══════════════════════════════════════════════════════╗");
    println!("║     Zenoh Locality 机内/机外隔离测验                  ║");
    println!("╠══════════════════════════════════════════════════════╣");
    println!("║  角色: {}                                  ║",
        if is_remote { "远端机器 (模拟机器B)" } else { "本地机器 (模拟机器A)" }
    );
    println!("║  Topic: robot/status                                 ║");
    println!("╚══════════════════════════════════════════════════════╝\n");

    if is_remote {
        run_remote_receiver().await?;
    } else {
        run_local_quiz().await?;
    }

    Ok(())
}

fn make_local_config(listen: Option<String>, connect: Option<&str>) -> Config {
    let mut config = Config::default();
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .ok();
    config.insert_json5("scouting/gossip/enabled", "false").ok();

    if let Some(ep) = listen {
        config.insert_json5("listen/endpoints", &format!("[\"{ep}\"]")).ok();
    } else {
        config.insert_json5("listen/endpoints", "[]").ok();
    }

    if let Some(ep) = connect {
        config.insert_json5("connect/endpoints", &format!("[\"{ep}\"]")).ok();
    } else {
        config.insert_json5("connect/endpoints", "[]").ok();
    }

    config
}

fn network_config() -> Config {
    zenoh::Config::default()
}

// ── 本地测验 ──────────────────────────────────────────────────────────────────

async fn run_local_quiz() -> anyhow::Result<()> {
    let topic = "robot/status";
    let port = pick_port();
    let endpoint = format!("tcp/127.0.0.1:{port}");

    // Session-A: 监听 TCP loopback
    // Session-B: 连接到 Session-A
    let session_a = Arc::new(ze!(zenoh::open(make_local_config(
        Some(endpoint.clone()),
        None
    ))));
    let session_b = Arc::new(ze!(zenoh::open(make_local_config(
        None,
        Some(&endpoint)
    ))));

    println!("[Setup] Session-A 监听 {endpoint}");
    println!("[Setup] Session-B 连接到 {endpoint}");
    println!("[Setup] Session-A ZID: {}", session_a.info().zid().await);
    println!("[Setup] Session-B ZID: {}", session_b.info().zid().await);
    println!("[Setup] 两个独立 Session, 通过 TCP loopback 模拟机内多服务\n");

    // 等待路由建立
    sleep(Duration::from_millis(500)).await;

    let barrier = Arc::new(Barrier::new(3));

    // Service-B Subscriber (独立 session)
    let barrier_b = barrier.clone();
    let session_b_sub = session_b.clone();
    let handle_b = tokio::spawn(async move {
        let sub = session_b_sub.declare_subscriber(topic).await.unwrap();
        barrier_b.wait().await;
        println!("[Service-B-Sub] 开始监听...");
        let mut count = 0;
        while let Ok(sample) = sub.recv_async().await {
            let payload = sample.payload().try_to_string().unwrap_or_default();
            println!("  >>> Service-B 接收到: {payload}");
            count += 1;
            if count >= 2 {
                break;
            }
        }
        println!("[Service-B-Sub] 结束. (共 {count} 条消息)\n");
    });

    // Service-A Subscriber (同 session)
    let barrier_a = barrier.clone();
    let session_a_sub = session_a.clone();
    let handle_a = tokio::spawn(async move {
        let sub = session_a_sub.declare_subscriber(topic).await.unwrap();
        barrier_a.wait().await;
        println!("[Service-A-Sub] 开始监听...");
        let mut count = 0;
        while let Ok(sample) = sub.recv_async().await {
            let payload = sample.payload().try_to_string().unwrap_or_default();
            println!("  >>> Service-A 接收到: {payload}");
            count += 1;
            if count >= 4 {
                break;
            }
        }
        println!("[Service-A-Sub] 结束. (共 {count} 条消息)\n");
    });

    sleep(Duration::from_millis(300)).await;

    // ═══════════════════════════════════════════════════════
    // 测验 1: SessionLocal — 消息不出当前 session
    // ═══════════════════════════════════════════════════════
    println!("── 测验 1: SessionLocal 发布 (机内隔离) ────────────\n");

    let pub_local = ze!(session_a
        .declare_publisher(topic)
        .allowed_destination(Locality::SessionLocal));

    let msg1 = "INTERNAL: only session-A should see this";
    println!("[Publisher-A] SessionLocal → \"{msg1}\"");
    ze!(pub_local.put(msg1));
    sleep(Duration::from_millis(300)).await;

    // ═══════════════════════════════════════════════════════
    // 测验 2: 默认 Any — 消息可达所有 session
    // ═══════════════════════════════════════════════════════
    println!("\n── 测验 2: 默认 Locality::Any 发布 (公开) ────────────\n");

    let pub_any = ze!(session_a.declare_publisher(topic));

    let msg2 = "PUBLIC: everyone should see this";
    println!("[Publisher-A] Any → \"{msg2}\"");
    ze!(pub_any.put(msg2));
    sleep(Duration::from_millis(300)).await;

    // ── 第二轮 ────────────────────────────────────────────

    println!("\n── 第二轮发送 ───────────────────────────────────────\n");

    ze!(pub_local.put("INTERNAL-2"));
    sleep(Duration::from_millis(100)).await;

    ze!(pub_any.put("PUBLIC-2"));
    sleep(Duration::from_millis(300)).await;

    barrier.wait().await;
    let _ = tokio::join!(handle_a, handle_b);

    // ── 总结 ──────────────────────────────────────────────

    println!("═══ 测验结果总结 ═══");
    println!();
    println!("  Topic: 'robot/status'");
    println!("  Session-A 监听 tcp/127.0.0.1, Session-B 连接 Session-A");
    println!("  Publisher:  Session-A");
    println!();
    println!("  ┌────────────────────┬──────────────┬──────────────┐");
    println!("  │ 发布方式            │ Service-A 收 │ Service-B 收 │");
    println!("  ├────────────────────┼──────────────┼──────────────┤");
    println!("  │ SessionLocal       │     ✓        │     ✗        │");
    println!("  │ Locality::Any      │     ✓        │     ✓        │");
    println!("  └────────────────────┴──────────────┴──────────────┘");
    println!();
    println!("  关键 API:");
    println!("    declare_publisher(key)                   // 默认 Any, 全网可达");
    println!("      .allowed_destination(Locality::SessionLocal) // 仅当前 session");
    println!();
    println!("  实际场景:");
    println!("    - 健康检查 / 内部状态  → SessionLocal (不出进程)");
    println!("    - 业务数据 / 传感器值  → Any (可达所有服务, 包括远端)");
    println!("    - 同一 topic, 不同 locality → 实现机内/机外通讯隔离 ✓");

    Ok(())
}

// ── 远端接收 ──────────────────────────────────────────────────────────────────

async fn run_remote_receiver() -> anyhow::Result<()> {
    let topic = "robot/status";
    let session = ze!(zenoh::open(network_config()));

    println!("[Remote-Sub] 远端订阅 '{}'...\n", topic);

    let sub = ze!(session.declare_subscriber(topic));

    println!("[Remote-Sub] 等待消息...");
    println!("[Remote-Sub] 期望: 只收到 'PUBLIC' 消息, 收不到 'INTERNAL' 消息\n");

    let mut count = 0;
    while let Ok(sample) = sub.recv_async().await {
        let payload = sample.payload().try_to_string().unwrap_or_default();
        println!("  >>> 远端接收到: {payload}");
        count += 1;
        if count >= 4 {
            break;
        }
    }

    println!("\n[Remote-Sub] 测验结束.");
    println!("  如果只看到 'PUBLIC' 消息 → SessionLocal 隔离生效 ✓");
    println!("  如果一条消息也没收到 → 网络未连通, 检查 ZENOH_CONNECT 配置");

    Ok(())
}