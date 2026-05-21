//! ## 08 - Performance Benchmark: 吞吐量 / 延迟
//!
//! 评估 Zenoh 在机器人场景下的通信性能:
//!   - 吞吐量: 每秒可传输多少条消息
//!   - 延迟: 端到端消息传输时间

use std::time::{Duration, Instant};
use tokio::time::sleep;
use zenoh::Config;

macro_rules! ze {
    ($expr:expr) => { $expr.await.map_err(|e| anyhow::anyhow!("{e}"))? };
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("═══ Zenoh 性能基准测试 ═══\n");

    let session = ze!(zenoh::open(local_config()));

    test_throughput(&session).await?;
    test_latency(&session).await?;

    Ok(())
}

fn local_config() -> Config {
    let mut c = Config::default();
    c.insert_json5("scouting/multicast/enabled", "false").ok();
    c.insert_json5("scouting/gossip/enabled", "false").ok();
    c.insert_json5("listen/endpoints", "[]").ok();
    c
}

// ── 吞吐量测试 ─────────────────────────────────────────────────────────

async fn test_throughput(session: &zenoh::Session) -> anyhow::Result<()> {
    let payload = "x".repeat(64);
    let msg_count = 200;

    println!("── 吞吐量测试 ({msg_count} 条消息, {size}B payload) ──\n",
        size = payload.len());

    let sub = ze!(session.declare_subscriber("bench/throughput"));
    sleep(Duration::from_millis(100)).await;

    let pub_ = ze!(session.declare_publisher("bench/throughput"));

    let start = Instant::now();

    // 先发送
    for i in 0..msg_count {
        pub_.put(format!("{payload}_{i}")).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    // 再接收 (单线程顺序, 测量真实吞吐)
    let mut received = 0;
    for _ in 0..msg_count {
        sub.recv_async().await.map_err(|e| anyhow::anyhow!("{e}"))?;
        received += 1;
    }

    let elapsed = start.elapsed();
    let throughput = received as f64 / elapsed.as_secs_f64();

    println!("  发送: {msg_count} 条");
    println!("  收到: {received} 条");
    println!("  耗时: {elapsed:.2?}");
    println!("  吞吐: {throughput:.0} msg/s");
    println!("  带宽: {:.1} MB/s\n", throughput * payload.len() as f64 / (1024.0 * 1024.0));
    Ok(())
}

// ── 延迟测试 ───────────────────────────────────────────────────────────

async fn test_latency(session: &zenoh::Session) -> anyhow::Result<()> {
    let samples = 50;

    println!("── 延迟测试 ({samples} 次往返) ──\n");

    let sub = ze!(session.declare_subscriber("bench/latency"));
    sleep(Duration::from_millis(100)).await;

    let pub_ = ze!(session.declare_publisher("bench/latency"));

    let mut latencies = Vec::with_capacity(samples);

    for i in 0..samples {
        let t0 = Instant::now();
        pub_.put(format!("ping_{i}")).await.map_err(|e| anyhow::anyhow!("{e}"))?;
        sub.recv_async().await.map_err(|e| anyhow::anyhow!("{e}"))?;
        latencies.push(t0.elapsed());
    }

    latencies.sort();
    let min = latencies.first().unwrap();
    let max = latencies.last().unwrap();
    let avg = latencies.iter().sum::<Duration>() / samples as u32;
    let p50 = latencies[samples / 2];
    let p99 = latencies[(samples as f64 * 0.99) as usize];

    println!("  样本数: {samples}");
    println!("  最小:   {min:.2?}");
    println!("  平均:   {avg:.2?}");
    println!("  P50:    {p50:.2?}");
    println!("  P99:    {p99:.2?}");
    println!("  最大:   {max:.2?}");
    println!();

    // ── 不同大小负载的延迟 ──────────────────────────────

    println!("── 不同负载大小延迟对比 ──\n");

    let sizes = [64, 256, 1024, 4096, 16384];
    println!("  {:>8}  {:>10}  {:>10}", "大小", "延迟", "带宽");

    for size in &sizes {
        let payload = "x".repeat(*size);
        let topic = format!("bench/latency_{size}");
        let sub_s = ze!(session.declare_subscriber(&topic));
        let pub_s = ze!(session.declare_publisher(&topic));
        sleep(Duration::from_millis(50)).await;

        let t0 = Instant::now();
        pub_s.put(&payload).await.map_err(|e| anyhow::anyhow!("{e}"))?;
        sub_s.recv_async().await.map_err(|e| anyhow::anyhow!("{e}"))?;
        let latency = t0.elapsed();

        let bw = *size as f64 / latency.as_secs_f64() / (1024.0 * 1024.0);
        println!("  {:>5} B  {:>8.0?}  {:>8.1} MB/s", size, latency, bw);
    }

    println!("\n═══ 总结 ═══");
    println!("  单 session 内通信延迟通常在微秒级");
    println!("  网络环境中延迟主要由网络 RTT 决定");
    println!("  生产环境建议做端到端测试(含路由器) ✓");
    Ok(())
}