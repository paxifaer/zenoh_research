//! ## 07 - 原生筛选: 用 Key Expression 替代用户回调
//!
//! Zenoh 筛选的正确方式不是在手写 if/else 回调里解析 payload,
//! 而是把筛选维度编码到 **key expression** 里,让路由层替你过滤。
//!
//! 三种 Zenoh 原生筛选手段:
//!
//!   1. Key 层次设计    — 把温度等级、机器人型号写入 key
//!   2. Key 通配符订阅   — subscriber 的 key pattern 自然过滤
//!   3. Selector + Query — 查询时指定筛选条件, Queryable 匹配回复

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

    println!("═══ Zenoh 原生筛选: Key Expression 路由 ═══\n");

    demo_key_hierarchy_filter(&session).await?;
    demo_wildcard_filter(&session).await?;
    demo_query_selector_filter(&session).await?;

    println!("═══ 总结 ═══");
    println!("  Zenoh 的筛选不靠手写 if/else payload 解析:");
    println!("    1. 把维度编码进 key: sensor/temp/{{normal|high|critical}}");
    println!("    2. 订阅时用通配符: sensor/temp/high 或 sensor/temp/*");
    println!("    3. 查询时用 selector: query 里带参数, queryable 匹配回复");
    println!("  路由层自动过滤, subscriber 收到的就是想要的 ✓");
    Ok(())
}

fn local_config() -> Config {
    let mut c = Config::default();
    c.insert_json5("scouting/multicast/enabled", "false").ok();
    c.insert_json5("scouting/gossip/enabled", "false").ok();
    c.insert_json5("listen/endpoints", "[]").ok();
    c
}

// ── 手段 1: Key 层次编码筛选维度 ────────────────────────────────────────────

async fn demo_key_hierarchy_filter(session: &zenoh::Session) -> anyhow::Result<()> {
    println!("── 手段 1: Key 层次编码筛选维度 ──\n");

    // 把温度等级写入 key, 替代在 payload 里手写 filter
    //
    //   错误做法: 全发到 sensor/temperature, 然后 if payload.temp > 80
    //   正确做法: 发到 sensor/temp/normal / sensor/temp/high / sensor/temp/critical
    //
    //   subscriber 只需要订阅 sensor/temp/high, 路由层自动过滤 ✓

    // 告警 subscriber — 只关心高温和严重级别
    let sub_alert = ze!(session.declare_subscriber("sensor/temp/high"));
    let sub_critical = ze!(session.declare_subscriber("sensor/temp/critical"));

    // 常规 subscriber — 只看正常温度
    let sub_normal = ze!(session.declare_subscriber("sensor/temp/normal"));
    sleep(Duration::from_millis(50)).await;

    // 发布: 不同温度发到不同 key
    let readings = [(45, "sensor/temp/normal"), (88, "sensor/temp/high"),
                    (62, "sensor/temp/normal"), (95, "sensor/temp/critical"),
                    (71, "sensor/temp/normal"), (42, "sensor/temp/normal")];

    for (temp, key) in &readings {
        let pub_ = ze!(session.declare_publisher(*key));
        ze!(pub_.put(format!("{{\"temperature\": {temp}}}")));
    }
    sleep(Duration::from_millis(100)).await;

    // 告警 subscriber 只收到 high 和 critical
    println!("  [告警订阅] 只订阅 sensor/temp/high + critical:");
    if let Ok(s) = sub_alert.recv_async().await {
        println!("    → {}", s.payload().try_to_string().unwrap_or_default());
    }
    if let Ok(s) = sub_critical.recv_async().await {
        println!("    → {}", s.payload().try_to_string().unwrap_or_default());
    }

    // 常规 subscriber 只收到 normal
    println!("  [常规订阅] 只订阅 sensor/temp/normal:");
    let mut normal_count = 0;
    while let Ok(s) = sub_normal.recv_async().await {
        println!("    → {}", s.payload().try_to_string().unwrap_or_default());
        normal_count += 1;
        if normal_count >= 4 { break; }
    }
    println!("  结果: 0 行手写筛选代码, 路由层自动隔离\n");
    Ok(())
}

// ── 手段 2: 通配符订阅自动筛选 ────────────────────────────────────────────────

async fn demo_wildcard_filter(session: &zenoh::Session) -> anyhow::Result<()> {
    println!("── 手段 2: 通配符订阅 (key pattern 路由) ──\n");

    // 机器人状态按型号写入 key
    //   robot/spot/status         ← Spot 的状态
    //   robot/atlas/status        ← Atlas 的状态
    //   robot/handle/status       ← Handle 的状态
    //
    // 想看 Spot 的就订阅 robot/spot/status, 路由层自动排除其他型号

    // Spot 专属 subscriber
    let sub_spot = ze!(session.declare_subscriber("robot/spot/status"));
    // 所有机器人 subscriber
    let sub_all = ze!(session.declare_subscriber("robot/*/status"));
    sleep(Duration::from_millis(50)).await;

    let robots = [
        ("spot", "walking"),
        ("atlas", "idle"),
        ("spot", "charging"),
        ("handle", "walking"),
        ("spot", "scanning"),
        ("atlas", "walking"),
    ];

    for (model, status) in &robots {
        let key = format!("robot/{model}/status");
        let pub_ = ze!(session.declare_publisher(&key));
        ze!(pub_.put(format!("{{model: \"{model}\", status: \"{status}\"}}")));
    }
    sleep(Duration::from_millis(100)).await;

    // Spot 专属订阅 — 自动只收 Spot
    println!("  [Spot 专属] 只订阅 robot/spot/status:");
    let mut spot_count = 0;
    while let Ok(s) = sub_spot.recv_async().await {
        println!("    → {}", s.payload().try_to_string().unwrap_or_default());
        spot_count += 1;
        if spot_count >= 3 { break; }
    }

    // 通配符订阅 — 收全部
    println!("  [通配符] 订阅 robot/*/status:");
    let mut all_count = 0;
    while let Ok(s) = sub_all.recv_async().await {
        println!("    → {}", s.payload().try_to_string().unwrap_or_default());
        all_count += 1;
        if all_count >= 6 { break; }
    }

    println!("  结果: Spot 专属收到 3 条, 通配符收到 6 条");
    println!("       不同 subscriber 的 key pattern 就是筛选条件\n");
    Ok(())
}

// ── 手段 3: Query + Selector 筛选 ─────────────────────────────────────────────

async fn demo_query_selector_filter(session: &zenoh::Session) -> anyhow::Result<()> {
    println!("── 手段 3: Query + Selector 按需查询 ──\n");

    // Queryable 注册时响应所有查询, 但 Zenoh 的 selector 机制确保
    // 只有 selector 匹配的 queryable 才会被调用

    // 注册两个"服务", 各自只响应自己的 key
    let _battery_q = ze!(session
        .declare_queryable("query/battery")
        .callback(move |q: Query| {
            q.reply("query/battery", "battery: 78%")
                .wait().unwrap();
        }));

    let _motor_q = ze!(session
        .declare_queryable("query/motor_temp")
        .callback(move |q: Query| {
            q.reply("query/motor_temp", "motor_temp: 42C")
                .wait().unwrap();
        }));

    let _imu_q = ze!(session
        .declare_queryable("query/imu")
        .callback(move |q: Query| {
            q.reply("query/imu", "imu: accel=(0.1,0.2,9.8)")
                .wait().unwrap();
        }));

    sleep(Duration::from_millis(200)).await;

    // 只查电池状态 — selector 精确匹配, 只触发 battery queryable
    println!("  [精确查询] 只查 query/battery:");
    let replies = ze!(session
        .get("query/battery")
        .timeout(Duration::from_secs(2)));
    while let Ok(reply) = replies.recv_async().await {
        let body = reply.result().unwrap().payload().try_to_string().unwrap_or_default();
        println!("    → {body}");
    }

    // 通配符查全部 — selector 匹配所有 queryable
    println!("\n  [批量查询] 查 query/* (通配符):");
    let replies = ze!(session
        .get("query/*")
        .timeout(Duration::from_secs(2)));
    while let Ok(reply) = replies.recv_async().await {
        let key = reply.result().unwrap().key_expr().to_string();
        let body = reply.result().unwrap().payload().try_to_string().unwrap_or_default();
        println!("    → {key}: {body}");
    }

    println!("\n  结果: selector 精确匹配只触发 1 个 queryable");
    println!("       selector 通配符触发 3 个 queryable");
    println!("       筛选完全由 Zenoh 的 selector 路由完成 ✓\n");
    Ok(())
}