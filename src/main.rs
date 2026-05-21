//! Zenoh 机器人系统示例集
//!
//! 运行方式:
//! ```bash
//! cargo run --example <example_name>
//! ```
//!
//! 示例列表:
//!   01_locality_isolation  - 机内/机外通讯隔离 (Locality)
//!   02_qos_profiles        - QoS 配置: 可靠性/优先级/拥塞控制
//!   03_queryable_rpc       - 请求/回复模式 (RPC)
//!   04_key_expressions     - 分层 key 表达式与通配符路由
//!   05_liveliness          - 心跳存活检测
//!   06_multi_publisher     - 多发布者同一 topic (多机器人场景)
//!   07_selective_subscribe - 筛选订阅与查询
//!   08_perf_bench          - 吞吐量/延迟性能基准

fn main() {
    println!("Zenoh 机器人系统示例集\n");
    println!("用法: cargo run --example <name>\n");
    println!("可用示例:");
    for (name, desc) in EXAMPLES {
        println!("  {name:<28} {desc}");
    }
}

const EXAMPLES: &[(&str, &str)] = &[
    ("01_locality_isolation", "机内/机外通讯隔离"),
    ("02_qos_profiles", "QoS: 可靠性/优先级/拥塞控制"),
    ("03_queryable_rpc", "请求/回复 RPC 模式"),
    ("04_key_expressions", "分层 Key 与通配符"),
    ("05_liveliness", "心跳存活检测"),
    ("06_multi_publisher", "多发布者同 Topic"),
    ("07_selective_subscribe", "筛选订阅"),
    ("08_perf_bench", "吞吐/延迟基准测试"),
];