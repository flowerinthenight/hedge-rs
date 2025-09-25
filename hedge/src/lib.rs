//! A cluster membership Rust library. It is built on [spindle-rs](https://github.com/flowerinthenight/spindle-rs), a
//! distributed locking library built on [Cloud Spanner](https://cloud.google.com/spanner/) and
//! [TrueTime](https://cloud.google.com/spanner/docs/true-time-external-consistency). It is a port (subset only) of
//! the original [hedge](https://github.com/flowerinthenight/hedge), which is written in Go. Ported features include:
//!
//! * Tracking of member nodes - good for clusters with sizes that can change dynamically overtime, such as [GCP MIGs](https://cloud.google.com/compute/docs/instance-groups#managed_instance_groups), and [Kubernetes Deployments](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/);
//! * Leader election - the cluster elects and maintains a single leader node at all times;
//! * List of members - get a list of all member nodes at any time;
//! * Send - any member node can send messages to the leader at any time; and
//! * Broadcast - any member node can broadcast messages to all nodes at any time.

pub mod hedge;
mod protocol;

pub use hedge::{Broadcast, Comms, Op, OpBuilder};

#[macro_use(defer)]
extern crate scopeguard;
