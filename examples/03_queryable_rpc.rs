//! ## 03 - Queryable (RPC): 请求/回复模式
//!
//! 机器人系统中常见的 RPC 模式:
//!   - 控制中心查询机器人状态
//!   - 服务发现: "谁提供 SLAM 服务?"
//!   - 参数查询: "当前电池电压?"
//!
//! Zenoh 的 Queryable 提供了 request/reply 语义,
//! 比 pub/sub 更适合一问一答的场景。

use std::time::Duration;
use tokio::time::sleep;
use zenoh::query::Query;
use zenoh::Config;
use zenoh::Wait;

macro_rules! ze {
    ($expr:expr) => { $expr.await.map_err(|e| anyhow::anyhow!("{e}"))? };
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let session = ze!(zenoh::open(local_config()));

    println!("═══ Zenoh Queryable RPC 测验 ═══\n");

    // ── 场景 1: 单 Queryable ────────────────────────────

    println!("── 场景 1: 服务端注册 + 客户端查询 ──\n");

    let service_key = "robot/service/status";

    // 服务端: 注册 Queryable, 响应查询
    let _queryable = ze!(session
        .declare_queryable(service_key)
        .callback(move |query: Query| {
            let payload = query.payload().unwrap().try_to_string().unwrap_or_default();
            println!("  [Server] 收到查询: '{payload}'");
            query
                .reply(
                    service_key,
                    format!("OK: status=healthy, cpu=42%, mem=3.2GB"),
                )
                .wait()
                .unwrap();
        }));

    sleep(Duration::from_millis(200)).await;

    // 客户端: 发送查询,收集回复
    println!("  [Client] 查询 'robot/service/status'...");
    let replies = ze!(session
        .get(service_key)
        .payload("get_status")
        .timeout(Duration::from_secs(3)));

    while let Ok(reply) = replies.recv_async().await {
        let body = reply.result().unwrap().payload().try_to_string().unwrap_or_default();
        println!("  [Client] 收到回复: '{body}'");
        break;
    }

    // ── 场景 2: 多 Queryable (服务发现) ─────────────────

    println!("\n── 场景 2: 服务发现 (一个查询,多个回复) ──\n");

    let discovery_key = "robot/services/*";

    // 注册两个服务
    let _slam = ze!(session
        .declare_queryable("robot/services/slam")
        .callback(move |query: Query| {
            query.reply("robot/services/slam", "SLAM v2.1: running")
                .wait().unwrap();
        }));

    let _nav = ze!(session
        .declare_queryable("robot/services/navigation")
        .callback(move |query: Query| {
            query.reply("robot/services/navigation", "Navigation v1.5: idle")
                .wait().unwrap();
        }));

    sleep(Duration::from_millis(200)).await;

    println!("  [Client] 查询 'robot/services/*' (通配符发现所有服务)...");
    let replies = ze!(session
        .get(discovery_key)
        .timeout(Duration::from_secs(3)));

    let mut count = 0;
    while let Ok(reply) = replies.recv_async().await {
        let key = reply.result().unwrap().key_expr().to_string();
        let body = reply.result().unwrap().payload().try_to_string().unwrap_or_default();
        println!("  [Client] {key} → {body}");
        count += 1;
    }

    println!("\n═══ 总结 ═══");
    println!("  发现 {count} 个服务");
    println!("  declare_queryable(key).callback(fn)  → 注册服务");
    println!("  session.get(key).payload(...)        → 带参数查询");
    println!("  query.reply(key, payload).wait()     → 回复查询");
    println!("  通配符 * 可实现服务发现 ✓");
    Ok(())
}

fn local_config() -> Config {
    let mut c = Config::default();
    c.insert_json5("scouting/multicast/enabled", "false").ok();
    c.insert_json5("scouting/gossip/enabled", "false").ok();
    c.insert_json5("listen/endpoints", "[]").ok();
    c
}