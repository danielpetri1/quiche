// Copyright (C) 2025, Cloudflare, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
// IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
// PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use rand::seq::SliceRandom;
use rand::thread_rng;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use crate::quic::QuicheConnection;

pub type BoxedScheduler = Arc<dyn PacketScheduler + Send + Sync + 'static>;
const ECF_BETA: f64 = 10.0;
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(C)]
pub enum PacketSchedulingAlgorithm {
    /// LowRTT Path Scheduler algorithm - selects path with lowest RTT
    MinRTT = 0,
    /// Round Robin Path Scheduler algorithm - cycles through available paths
    RoundRobin = 1,
    /// Random Path Scheduler algorithm - randomly selects among available paths
    Random = 2,
    /// Lowest Latency Path Scheduler algorithm - like MinRTT but considers jitter
    LowestLatency = 3,
    /// LowRTT - like MinRTT but only considers paths with available cwnd
    LowRTT = 4,
    /// ECF: may wait for a faster path to become available
    EarliestCompletionFirst = 5,
    /// Stream-Aware Earliest Completion First accounting for HTTP/3 hints.
    StreamAwareEarliestCompletionFirst = 6,
}

impl PacketSchedulingAlgorithm {
    /// Returns the name of the algorithm.
    pub fn name(self) -> &'static str {
        match self {
            PacketSchedulingAlgorithm::MinRTT => "minrtt",
            PacketSchedulingAlgorithm::RoundRobin => "roundrobin",
            PacketSchedulingAlgorithm::Random => "random",
            PacketSchedulingAlgorithm::LowestLatency => "lowestlatency",
            PacketSchedulingAlgorithm::EarliestCompletionFirst => {
                "earliestcompletionfirst"
            }
            PacketSchedulingAlgorithm::StreamAwareEarliestCompletionFirst => {
                "sa-ecf"
            }
            PacketSchedulingAlgorithm::LowRTT => "lowrtt",
        }
    }

    fn active_paths(&self, conn: &QuicheConnection) -> Vec<quiche::PathStats> {
        conn.path_stats()
            .filter(|p| {
                p.active && !matches!(p.state, quiche::PathState::Closed(_))
            })
            .collect()
    }
}

impl FromStr for PacketSchedulingAlgorithm {
    type Err = quiche::Error;

    /// Converts a string to `PacketSchedulingAlgorithm`.
    ///
    /// If `name` is not valid, `Error::PacketScheduler` is returned.
    fn from_str(name: &str) -> Result<Self, quiche::Error> {
        match name {
            "minrtt" => Ok(PacketSchedulingAlgorithm::MinRTT),
            "lowrtt" => Ok(PacketSchedulingAlgorithm::LowRTT),
            "roundrobin" | "rr" => Ok(PacketSchedulingAlgorithm::RoundRobin),
            "random" | "rand" => Ok(PacketSchedulingAlgorithm::Random),
            "lowestlatency" | "ll" => {
                Ok(PacketSchedulingAlgorithm::LowestLatency)
            }
            "ecf" => Ok(PacketSchedulingAlgorithm::EarliestCompletionFirst),
            "sa-ecf" | "eps-aware-ecf" | "stream-aware-ecf" => {
                Ok(PacketSchedulingAlgorithm::StreamAwareEarliestCompletionFirst)
            }
            _ => Err(quiche::Error::UnknownPacketScheduler),
        }
    }
}

/// The result of a scheduling decision.
#[derive(Debug, Clone)]
pub struct SchedulerDecision {
    /// The path to send on.
    pub path: (SocketAddr, SocketAddr),
    /// Optional stream ID hint for quiche's send path.
    /// When `Some`, quiche will prefer this stream.
    pub stream_id: Option<u64>,
}

impl SchedulerDecision {
    /// Path-only decision.
    fn path_only(local: SocketAddr, peer: SocketAddr) -> Self {
        Self {
            path: (local, peer),
            stream_id: None,
        }
    }

    /// A stream-aware scheduling decision.
    fn stream_aware(local: SocketAddr, peer: SocketAddr, stream_id: u64) -> Self {
        Self {
            path: (local, peer),
            stream_id: Some(stream_id),
        }
    }
}

/// Trait defining a packet scheduler that can determine which path to use for
/// sending packets
pub trait PacketScheduler: Debug + Send + Sync + 'static {
    /// Get the next path to send a packet on
    fn next_path(&self, conn: &QuicheConnection) -> Option<SchedulerDecision>;

    /// Returns the name of the scheduler.
    fn name(&self) -> &'static str;

    /// Returns the algorithm used by the scheduler.
    fn algorithm(&self) -> PacketSchedulingAlgorithm;
}

/// Factory for creating scheduler instances
pub struct PacketSchedulerFactory;

impl PacketSchedulerFactory {
    /// Create a new scheduler instance based on the algorithm
    pub fn create(algorithm: PacketSchedulingAlgorithm) -> BoxedScheduler {
        match algorithm {
            PacketSchedulingAlgorithm::MinRTT => Arc::new(MinRTTScheduler::new()),
            PacketSchedulingAlgorithm::LowRTT => Arc::new(LowRTTScheduler::new()),
            PacketSchedulingAlgorithm::RoundRobin => {
                Arc::new(RoundRobinScheduler::new())
            }
            PacketSchedulingAlgorithm::Random => Arc::new(RandomScheduler::new()),
            PacketSchedulingAlgorithm::LowestLatency => {
                Arc::new(LowestLatencyScheduler::new())
            }
            PacketSchedulingAlgorithm::EarliestCompletionFirst => {
                Arc::new(EarliestCompletionFirstScheduler::new())
            }
            PacketSchedulingAlgorithm::StreamAwareEarliestCompletionFirst => {
                Arc::new(StreamAwareEarliestCompletionFirst::new())
            }
        }
    }

    /// Create a new scheduler instance from a string name
    pub fn from_name(name: &str) -> Result<BoxedScheduler, quiche::Error> {
        let algorithm = PacketSchedulingAlgorithm::from_str(name)?;
        Ok(Self::create(algorithm))
    }
}

/// MinRTT Path Scheduler algorithm.
#[derive(Debug, Clone)]
pub struct MinRTTScheduler;

impl MinRTTScheduler {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MinRTTScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl PacketScheduler for MinRTTScheduler {
    fn next_path(&self, conn: &QuicheConnection) -> Option<SchedulerDecision> {
        if let Some((path_id, _)) = conn.get_next_send_path_id(None, None, None) {
            let path = conn.path_stats().find(|p| p.path_id == path_id);
            if let Some(path) = path {
                return Some(SchedulerDecision::path_only(
                    path.local_addr,
                    path.peer_addr,
                ));
            }
        }

        let mut paths = self.algorithm().active_paths(conn);

        if paths.is_empty() {
            return None;
        }

        // Sort paths by RTT (lowest first)
        paths.sort_by(|a, b| a.rtt.cmp(&b.rtt));

        // Return the path with the lowest RTT
        paths
            .first()
            .map(|p| SchedulerDecision::path_only(p.local_addr, p.peer_addr))
    }

    fn name(&self) -> &'static str {
        self.algorithm().name()
    }

    fn algorithm(&self) -> PacketSchedulingAlgorithm {
        PacketSchedulingAlgorithm::MinRTT
    }
}

/// LowRTT Path Scheduler algorithm.
///
/// Like MinRTT but only considers paths with available congestion window,
/// selecting the one with the lowest smoothed RTT among those.
#[derive(Debug, Clone)]
pub struct LowRTTScheduler;

impl LowRTTScheduler {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LowRTTScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl PacketScheduler for LowRTTScheduler {
    fn next_path(&self, conn: &QuicheConnection) -> Option<SchedulerDecision> {
        if let Some((path_id, _)) = conn.get_next_send_path_id(None, None, None) {
            let path = conn.path_stats().find(|p| p.path_id == path_id);
            if let Some(path) = path {
                return Some(SchedulerDecision::path_only(
                    path.local_addr,
                    path.peer_addr,
                ));
            }
        }

        let mut paths: Vec<_> = self
            .algorithm()
            .active_paths(conn)
            .into_iter()
            .filter(|p| p.cwnd_available > 0)
            .collect();

        if paths.is_empty() {
            return None;
        }

        // Sort by smoothed RTT (lowest first)
        paths.sort_by(|a, b| a.rtt.cmp(&b.rtt));

        paths
            .first()
            .map(|p| SchedulerDecision::path_only(p.local_addr, p.peer_addr))
    }

    fn name(&self) -> &'static str {
        self.algorithm().name()
    }

    fn algorithm(&self) -> PacketSchedulingAlgorithm {
        PacketSchedulingAlgorithm::LowRTT
    }
}

/// Round Robin Path Scheduler algorithm.
#[derive(Debug)]
pub struct RoundRobinScheduler {
    current_index: AtomicUsize,
}

impl RoundRobinScheduler {
    pub fn new() -> Self {
        Self {
            current_index: 0.into(),
        }
    }
}

impl Default for RoundRobinScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl PacketScheduler for RoundRobinScheduler {
    fn next_path(&self, conn: &QuicheConnection) -> Option<SchedulerDecision> {
        if let Some((path_id, _)) = conn.get_next_send_path_id(None, None, None) {
            let path = conn.path_stats().find(|p| p.path_id == path_id);
            if let Some(path) = path {
                return Some(SchedulerDecision::path_only(
                    path.local_addr,
                    path.peer_addr,
                ));
            }
        }

        let idx = self.current_index.fetch_add(1, Relaxed);
        let paths: Vec<_> = self
            .algorithm()
            .active_paths(conn)
            .into_iter()
            .filter(|p| p.cwnd_available > 0)
            .collect();

        if paths.is_empty() {
            return None;
        }

        // Get the next path in round-robin fashion
        let path = &paths[idx % paths.len()];

        // Update the index for the next call
        self.current_index.store((idx + 1) % paths.len(), Relaxed);

        Some(SchedulerDecision::path_only(
            path.local_addr,
            path.peer_addr,
        ))
    }

    fn name(&self) -> &'static str {
        self.algorithm().name()
    }

    fn algorithm(&self) -> PacketSchedulingAlgorithm {
        PacketSchedulingAlgorithm::RoundRobin
    }
}

/// Random Path Scheduler algorithm.
#[derive(Debug)]
pub struct RandomScheduler;

impl RandomScheduler {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RandomScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl PacketScheduler for RandomScheduler {
    fn next_path(&self, conn: &QuicheConnection) -> Option<SchedulerDecision> {
        if let Some((path_id, _)) = conn.get_next_send_path_id(None, None, None) {
            let path = conn.path_stats().find(|p| p.path_id == path_id);
            if let Some(path) = path {
                return Some(SchedulerDecision::path_only(
                    path.local_addr,
                    path.peer_addr,
                ));
            }
        }

        let paths = self.algorithm().active_paths(conn);

        if paths.is_empty() {
            return None;
        }

        // Randomly select an active path
        let mut rng = thread_rng();
        paths
            .choose(&mut rng)
            .map(|p| SchedulerDecision::path_only(p.local_addr, p.peer_addr))
    }

    fn name(&self) -> &'static str {
        self.algorithm().name()
    }

    fn algorithm(&self) -> PacketSchedulingAlgorithm {
        PacketSchedulingAlgorithm::Random
    }
}

/// Lowest Latency Path Scheduler algorithm.
#[derive(Debug)]
pub struct LowestLatencyScheduler;

impl LowestLatencyScheduler {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LowestLatencyScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl PacketScheduler for LowestLatencyScheduler {
    fn next_path(&self, conn: &QuicheConnection) -> Option<SchedulerDecision> {
        let mut paths = self.algorithm().active_paths(conn);

        if paths.is_empty() {
            return None;
        }

        // Sort paths by a combination of RTT and RTT variance
        // This is a simple weighted formula that considers both metrics:
        // score = rtt + (2 * rttvar)
        paths.sort_by(|a, b| {
            let a_score = a.rtt + (a.rttvar * 2);
            let b_score = b.rtt + (b.rttvar * 2);
            a_score.cmp(&b_score)
        });

        // Return the path with the lowest latency score
        paths
            .first()
            .map(|p| SchedulerDecision::path_only(p.local_addr, p.peer_addr))
    }

    fn name(&self) -> &'static str {
        self.algorithm().name()
    }

    fn algorithm(&self) -> PacketSchedulingAlgorithm {
        PacketSchedulingAlgorithm::LowestLatency
    }
}

/// Earliest Completion First Path Scheduler algorithm.
#[derive(Debug)]
pub struct EarliestCompletionFirstScheduler {
    waiting: atomic_float::AtomicF64,

    /// ECF hysteresis value
    beta: atomic_float::AtomicF64,
}

impl EarliestCompletionFirstScheduler {
    pub fn new() -> Self {
        Self {
            waiting: 0.0.into(),
            beta: ECF_BETA.into(),
        }
    }
}

impl Default for EarliestCompletionFirstScheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// Based on:
/// Yeon-sup Lim, Erich M. Nahum, Don Towsley, and Richard J. Gibbens. 2017.
/// ECF: An MPTCP Path Scheduler to Manage Heterogeneous Paths.
/// In Proceedings of CoNEXT ’17. ACM, New York, NY, USA, 13 pages.
/// https: //doi.org/10.1145/3143361.3143376
impl PacketScheduler for EarliestCompletionFirstScheduler {
    fn next_path(&self, conn: &QuicheConnection) -> Option<SchedulerDecision> {
        if let Some((path_id, _)) = conn.get_next_send_path_id(None, None, None) {
            let path = conn.path_stats().find(|p| p.path_id == path_id);
            if let Some(path) = path {
                return Some(SchedulerDecision::path_only(
                    path.local_addr,
                    path.peer_addr,
                ));
            }
        }

        // Algorithm 1 - ECF Scheduler
        // Find fastest subflow x_f with smallest RTT
        let mut paths = self.algorithm().active_paths(conn);

        if paths.is_empty() {
            return None;
        }

        // Sort paths by RTT (lowest first)
        paths.sort_by(|a, b| a.rtt.cmp(&b.rtt));

        // Get the fastest path
        let x_f_stats = paths.first()?;
        let x_f_available: bool = x_f_stats.cwnd_available > 0;

        // if x_f is available for packet transfer, use it
        if x_f_available {
            return Some(SchedulerDecision::path_only(
                x_f_stats.local_addr,
                x_f_stats.peer_addr,
            ));
        }

        let x_f_cwnd = x_f_stats.cwnd as f64;
        let x_f_rtt = x_f_stats.rtt;
        let x_f_rttvar = x_f_stats.rttvar;

        // Fastest available path
        let x_s_stats = paths.iter().find(|p| p.cwnd_available > 0)?;

        let x_s_cwnd = x_s_stats.cwnd as f64;
        let x_s_rtt = x_s_stats.rtt;
        let x_s_rttvar = x_s_stats.rttvar;

        // Size of the connection-level send buffer
        let k = conn.send_buffer_size() as f64;

        let n = 1.0 + k / x_f_cwnd;
        let delta = x_f_rttvar.max(x_s_rttvar);

        let waiting = self.waiting.load(Relaxed);
        let beta = self.beta.load(Relaxed);
        let x_f_rtt_s = x_f_rtt.as_secs_f64();
        let x_s_rtt_s = x_s_rtt.as_secs_f64();
        let delta_s = delta.as_secs_f64();

        if n * x_f_rtt_s < (1.0 + waiting * beta) * (x_s_rtt_s + delta_s) {
            if (k * x_s_rtt_s) / x_s_cwnd >= 2.0 * x_f_rtt_s + delta_s {
                self.waiting.store(1.0, Relaxed); // Wait for x_f
                None // No available path
            } else {
                Some(SchedulerDecision::path_only(
                    x_s_stats.local_addr,
                    x_s_stats.peer_addr,
                ))
            }
        } else {
            self.waiting.store(0.0, Relaxed);
            Some(SchedulerDecision::path_only(
                x_s_stats.local_addr,
                x_s_stats.peer_addr,
            ))
        }
    }

    fn name(&self) -> &'static str {
        self.algorithm().name()
    }

    fn algorithm(&self) -> PacketSchedulingAlgorithm {
        PacketSchedulingAlgorithm::EarliestCompletionFirst
    }
}

/// Stream-Aware Earliest Completion First Path Scheduler algorithm.
#[derive(Debug)]
pub struct StreamAwareEarliestCompletionFirst {
    /// ECF hysteresis value
    beta: atomic_float::AtomicF64,

    /// Multiplexing granularity (in bytes per sending opportunity) for incremental streams.
    l: atomic_float::AtomicF64,
}

impl StreamAwareEarliestCompletionFirst {
    pub fn new() -> Self {
        Self {
            beta: ECF_BETA.into(),
            l: 1300.0.into(),
        }
    }
}

impl Default for StreamAwareEarliestCompletionFirst {
    fn default() -> Self {
        Self::new()
    }
}

/// Based on:
/// Alexander Rabitsch, Per Hurtig, and Anna Brunstrom. 2018.
/// A Stream-Aware Multipath QUIC Scheduler for Heterogeneous Paths.
/// In EPIQ ’18: Workshop on the Evolution, Performance, and Interoperability of QUIC, December 4, 2018, Heraklion, Greece.
/// ACM, New York,  NY, USA, 7 pages. https://doi.org/10.1145/3284850.3284855
impl PacketScheduler for StreamAwareEarliestCompletionFirst {
    fn next_path(&self, conn: &QuicheConnection) -> Option<SchedulerDecision> {
        if let Some((path_id, _)) = conn.get_next_send_path_id(None, None, None) {
            let path = conn.path_stats().find(|p| p.path_id == path_id);
            if let Some(path) = path {
                return Some(SchedulerDecision::path_only(
                    path.local_addr,
                    path.peer_addr,
                ));
            }
        }

        // Algorithm 1 - SA-ECF Scheduler
        // Sort paths by RTT
        let mut paths = self.algorithm().active_paths(conn);
        paths.sort_by(|a, b| a.rtt.cmp(&b.rtt));

        if paths.is_empty() {
            return None;
        }

        // Find the fastest path, p_f, with the lowest RTT
        let p_f_stats = paths.first()?;

        let flushable_keys = conn.flushable_keys();

        // If p_f is available, use it with the highest-priority stream.
        // Let EPS handle the stream choice separately later.
        // Prevents excessive qlog markers for the non-waiting case.
        if p_f_stats.cwnd_available > 0 {
            return Some(SchedulerDecision::path_only(
                p_f_stats.local_addr,
                p_f_stats.peer_addr,
            ));
        }

        // Find the fastest available path, p_s, with the lowest RTT
        let p_s_stats = paths.iter().find(|p| p.cwnd_available > 0)?;

        let p_f_rtt_s = p_f_stats.rtt.as_secs_f64();
        let p_f_rttvar = p_f_stats.rttvar;
        let p_f_cwnd = p_f_stats.cwnd as f64;

        let p_s_rtt_s = p_s_stats.rtt.as_secs_f64();
        let p_s_rttvar = p_s_stats.rttvar;
        let p_s_cwnd = p_s_stats.cwnd as f64;

        let delta_s = p_f_rttvar.max(p_s_rttvar).as_secs_f64();
        let beta = self.beta.load(Relaxed);

        for priority_key in &flushable_keys {
            let waiting = if priority_key.waiting.load(Relaxed) {
                1.0
            } else {
                0.0
            };

            // "Bytes until completion" k is the number of bytes sent at the
            // connection level before the individual stream completes.
            // Incremental streams at the same urgency level are sent out in a
            // round-robin fashion. Non-incremental streams are not interleaved.
            let l = self.l.load(Relaxed);
            let bytes_left = conn.stream_send_buffer_size(priority_key.id) as f64;
            let k = if !priority_key.incremental || l >= bytes_left {
                bytes_left
            } else {
                let n = flushable_keys
                    .iter()
                    .filter(|f| {
                        f.urgency == priority_key.urgency && f.incremental
                    })
                    .count()
                    .max(1) as f64;

                bytes_left * n - (n - 1.0) * l
            };

            let n = 1.0 + k / p_f_cwnd;

            if n * p_f_rtt_s < (1.0 + waiting * beta) * (p_s_rtt_s + delta_s) {
                if (k * p_s_rtt_s) / p_s_cwnd >= 2.0 * p_f_rtt_s + delta_s {
                    priority_key.waiting.store(true, Relaxed);
                    continue; // Stream waits for faster path
                } else {
                    return Some(SchedulerDecision::stream_aware(
                        p_s_stats.local_addr,
                        p_s_stats.peer_addr,
                        priority_key.id,
                    ));
                }
            } else {
                priority_key.waiting.store(false, Relaxed);
                return Some(SchedulerDecision::stream_aware(
                    p_s_stats.local_addr,
                    p_s_stats.peer_addr,
                    priority_key.id,
                ));
            }
        }

        None // No transmission
    }

    fn name(&self) -> &'static str {
        self.algorithm().name()
    }

    fn algorithm(&self) -> PacketSchedulingAlgorithm {
        PacketSchedulingAlgorithm::StreamAwareEarliestCompletionFirst
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scheduler_factory_create() {
        let scheduler =
            PacketSchedulerFactory::create(PacketSchedulingAlgorithm::MinRTT);
        assert_eq!(scheduler.algorithm(), PacketSchedulingAlgorithm::MinRTT);
        assert_eq!(scheduler.name(), "minrtt");

        let scheduler =
            PacketSchedulerFactory::create(PacketSchedulingAlgorithm::LowRTT);
        assert_eq!(scheduler.algorithm(), PacketSchedulingAlgorithm::LowRTT);
        assert_eq!(scheduler.name(), "lowrtt");

        let scheduler =
            PacketSchedulerFactory::create(PacketSchedulingAlgorithm::RoundRobin);
        assert_eq!(scheduler.algorithm(), PacketSchedulingAlgorithm::RoundRobin);
        assert_eq!(scheduler.name(), "roundrobin");

        let scheduler =
            PacketSchedulerFactory::create(PacketSchedulingAlgorithm::Random);
        assert_eq!(scheduler.algorithm(), PacketSchedulingAlgorithm::Random);
        assert_eq!(scheduler.name(), "random");

        let scheduler = PacketSchedulerFactory::create(
            PacketSchedulingAlgorithm::LowestLatency,
        );
        assert_eq!(
            scheduler.algorithm(),
            PacketSchedulingAlgorithm::LowestLatency
        );
        assert_eq!(scheduler.name(), "lowestlatency");

        let scheduler = PacketSchedulerFactory::create(
            PacketSchedulingAlgorithm::EarliestCompletionFirst,
        );
        assert_eq!(
            scheduler.algorithm(),
            PacketSchedulingAlgorithm::EarliestCompletionFirst
        );
        assert_eq!(scheduler.name(), "earliestcompletionfirst");

        let scheduler = PacketSchedulerFactory::create(
            PacketSchedulingAlgorithm::StreamAwareEarliestCompletionFirst,
        );
        assert_eq!(
            scheduler.algorithm(),
            PacketSchedulingAlgorithm::StreamAwareEarliestCompletionFirst
        );
        assert_eq!(scheduler.name(), "sa-ecf");
    }

    #[test]
    fn test_round_robin_index_cycling() {
        let scheduler = RoundRobinScheduler::new();

        // Test that the internal index starts at 0
        assert_eq!(scheduler.current_index.load(Relaxed), 0);

        // Test that fetch_add increments the index
        let idx1 = scheduler.current_index.fetch_add(1, Relaxed);
        assert_eq!(idx1, 0);
        assert_eq!(scheduler.current_index.load(Relaxed), 1);

        let idx2 = scheduler.current_index.fetch_add(1, Relaxed);
        assert_eq!(idx2, 1);
        assert_eq!(scheduler.current_index.load(Relaxed), 2);
    }
}
