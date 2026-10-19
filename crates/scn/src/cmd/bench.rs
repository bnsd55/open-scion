//! `bench`: many agents commit and land at once against a running server.
//!
//! Each agent writes its own files, so no change should conflict. The bench
//! then walks `main` back to where it started and checks that every landed
//! change is there exactly once.

use anyhow::{Context, Result, ensure};
use scion_core::{Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::v1::{LandRequest, LandState, LogRequest};
use serde_json::json;
use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

use crate::client::{Ctx, id_from, now_unix_secs};
use crate::outln;

/// Arguments of `scn bench`.
#[derive(clap::Args)]
pub struct BenchArgs {
    /// Concurrent agents.
    #[arg(long, default_value_t = 200)]
    agents: usize,
    /// Total lands across all agents.
    #[arg(long, default_value_t = 10_000)]
    lands: usize,
    /// Each agent re-reads `main` before every change (fresh), or keeps the
    /// base it started from (shared).
    #[arg(long, value_enum, default_value_t = Base::Shared)]
    base: Base,
}

/// How each agent picks its base.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum Base {
    /// Keep the base the agent started from. Every land rebases.
    Shared,
    /// Re-read `main` before each change.
    Fresh,
}

#[derive(Default)]
struct Stats {
    landed: Vec<ChangesetId>,
    conflicts: usize,
    rejected: usize,
    /// Time of the Land RPC alone.
    land: Vec<Duration>,
    /// Upload + create + land.
    total: Vec<Duration>,
}

/// Runs the benchmark and prints throughput and latency.
pub async fn bench(ctx: &Ctx, args: BenchArgs) -> Result<()> {
    let BenchArgs {
        agents,
        lands,
        base,
    } = args;
    ensure!(
        agents > 0 && lands > 0,
        "--agents and --lands must be positive"
    );
    let start = ctx.clone().resolve("main").await?;
    let per_agent = lands.div_ceil(agents);
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for agent in 0..agents {
        let count = per_agent.min(lands.saturating_sub(agent * per_agent));
        tasks.spawn(run_agent(ctx.clone(), agent, count, start, base));
    }
    let mut stats = Stats::default();
    while let Some(agent) = tasks.join_next().await {
        let agent = agent.context("agent task panicked")??;
        stats.landed.extend(agent.landed);
        stats.conflicts += agent.conflicts;
        stats.rejected += agent.rejected;
        stats.land.extend(agent.land);
        stats.total.extend(agent.total);
    }
    let elapsed = started.elapsed();
    verify(ctx.clone(), start, &stats.landed).await?;
    report(ctx.json, agents, &mut stats, elapsed)?;
    Ok(())
}

async fn run_agent(
    mut ctx: Ctx,
    agent: usize,
    count: usize,
    start: ChangesetId,
    base: Base,
) -> Result<Stats> {
    let mut stats = Stats::default();
    for i in 0..count {
        let began = Instant::now();
        let parent = match base {
            Base::Shared => start,
            Base::Fresh => ctx.resolve("main").await?,
        };
        // Unique per run, so every run pays for new blobs and runs compare.
        let blob = ctx
            .put_blob(format!("run {start} agent {agent} change {i}\n").into_bytes())
            .await?;
        let path = RepoPath::new(format!("bench/agent-{agent:04}/{i:06}.txt"))?;
        let changeset = Changeset::new(
            vec![parent],
            IdentityTime::new(
                b"scn bench".to_vec(),
                b"bench@localhost".to_vec(),
                now_unix_secs()?,
                0,
            )
            .expect("identity fits the format"),
            None,
            format!("bench: agent {agent} change {i}\n").into_bytes(),
            BTreeMap::from([(
                path,
                Some(FileState::Regular {
                    content: blob,
                    executable: false,
                }),
            )]),
        )
        .expect("changeset fits the format");
        let id = ctx.create_changeset(&changeset).await?;
        let landing = Instant::now();
        let reply = ctx
            .client
            .land(LandRequest {
                repo: ctx.repo.clone(),
                changeset_id: id.as_bytes().to_vec(),
                target_ref: "main".into(),
                request_key: format!("bench:{id}"),
            })
            .await?
            .into_inner();
        stats.land.push(landing.elapsed());
        stats.total.push(began.elapsed());
        match reply.state() {
            LandState::Landed => stats.landed.push(id_from(&reply.result_changeset_id)?),
            LandState::Conflict => stats.conflicts += 1,
            _ => stats.rejected += 1,
        }
    }
    Ok(stats)
}

/// Walks `main` back to `start` and checks that it gained exactly the landed
/// changesets, each once.
async fn verify(mut ctx: Ctx, start: ChangesetId, landed: &[ChangesetId]) -> Result<()> {
    let mut cur = Some(ctx.resolve("main").await?);
    let mut history = HashSet::new();
    'walk: while let Some(from) = cur {
        let mut pages = ctx
            .client
            .log(LogRequest {
                repo: ctx.repo.clone(),
                changeset_id: from.as_bytes().to_vec(),
                limit: 1000,
                path: Vec::new(),
            })
            .await?
            .into_inner();
        cur = None;
        while let Some(page) = pages.message().await? {
            for entry in page.entries {
                let id: ChangesetId = id_from(&entry.changeset_id)?;
                if id == start {
                    break 'walk;
                }
                ensure!(history.insert(id), "changeset {id} appears twice on main");
                cur = Changeset::decode(&entry.changeset)?
                    .parents()
                    .first()
                    .copied();
            }
        }
    }
    check_landed(landed, &history)
}

/// Every reported land is distinct and the reported set equals what `main`
/// gained: no duplicate, no missing, no extra.
fn check_landed(landed: &[ChangesetId], history: &HashSet<ChangesetId>) -> Result<()> {
    let reported: HashSet<ChangesetId> = landed.iter().copied().collect();
    ensure!(
        reported.len() == landed.len(),
        "{} lands reported the same result",
        landed.len() - reported.len()
    );
    ensure!(
        reported == *history,
        "reported lands differ from main: {} reported, {} on main, {} in common",
        reported.len(),
        history.len(),
        reported.intersection(history).count()
    );
    Ok(())
}

/// Percentiles of `sorted`, in milliseconds.
fn percentiles_ms(sorted: &[Duration]) -> [f64; 3] {
    let at = |p: f64| {
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let rank = ((sorted.len().saturating_sub(1)) as f64 * p).round() as usize;
        sorted.get(rank).map_or(0.0, |d| d.as_secs_f64() * 1000.0)
    };
    [at(0.50), at(0.99), at(1.0)]
}

fn report(json: bool, agents: usize, stats: &mut Stats, elapsed: Duration) -> Result<()> {
    stats.land.sort();
    stats.total.sort();
    let [land_p50, land_p99, land_max] = percentiles_ms(&stats.land);
    let [total_p50, total_p99, _] = percentiles_ms(&stats.total);
    let landed = stats.landed.len();
    #[allow(clippy::cast_precision_loss)]
    let rate = landed as f64 / elapsed.as_secs_f64();
    if json {
        let out = json!({
            "agents": agents,
            "landed": landed,
            "conflicts": stats.conflicts,
            "rejected": stats.rejected,
            "seconds": elapsed.as_secs_f64(),
            "lands_per_second": rate,
            "land_ms": { "p50": land_p50, "p99": land_p99, "max": land_max },
            "end_to_end_ms": { "p50": total_p50, "p99": total_p99 },
            "verified_on_main": true,
        });
        outln!("{out}");
        return Ok(());
    }
    outln!(
        "{landed} lands by {agents} agents in {:.1} s: {rate:.0} lands/s",
        elapsed.as_secs_f64()
    );
    outln!("conflicts {}, rejected {}", stats.conflicts, stats.rejected);
    outln!("land RPC     p50 {land_p50:.1} ms  p99 {land_p99:.1} ms  max {land_max:.1} ms");
    outln!("end to end   p50 {total_p50:.1} ms  p99 {total_p99:.1} ms  (upload + create + land)");
    outln!("verified: main gained exactly the {landed} landed changesets");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> ChangesetId {
        ChangesetId::from_bytes([n; 32])
    }

    #[test]
    fn check_landed_needs_exactly_the_reported_set() {
        let history: HashSet<_> = [id(1), id(2)].into();
        assert!(check_landed(&[id(1), id(2)], &history).is_ok());
        assert!(
            check_landed(&[id(1), id(1)], &history).is_err(),
            "duplicate, one missing"
        );
        assert!(
            check_landed(&[id(1)], &history).is_err(),
            "main has an unreported change"
        );
        assert!(
            check_landed(&[id(1), id(2), id(3)], &history).is_err(),
            "reported but not on main"
        );
    }
}
