mod protocol;

use anyhow::{Error, Result, anyhow};
use base64ct::{Base64, Encoding};
use crossbeam_channel::{Receiver, Sender, unbounded};
use log::*;
use protocol::*;
use spindle_rs::*;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::io::{BufReader, prelude::*};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[macro_use(defer)]
extern crate scopeguard;

#[derive(Debug)]
pub enum Comms {
    ToLeader { msg: Vec<u8>, tx: mpsc::Sender<Vec<u8>> },
}

#[derive(Debug)]
enum WorkerCtrl {
    TcpServer(TcpStream),
    PingMember(String),
    ToLeader { msg: Vec<u8>, tx: Sender<Vec<u8>> },
}

pub struct Op {
    db: String,
    table: String,
    name: String,
    id: String,
    lock: Vec<Arc<Mutex<Lock>>>,
    leader: Arc<AtomicUsize>,
    lease_ms: u64,
    sync_ms: u64,
    members: Arc<Mutex<HashMap<String, usize>>>,
    tx_worker: Vec<Sender<WorkerCtrl>>,
    tx_toleader: Option<mpsc::Sender<Comms>>,
    active: Arc<AtomicUsize>,
}

impl Op {
    /// Allows for discovery of the builder.
    pub fn builder() -> OpBuilder {
        OpBuilder::default()
    }

    /// TODO:
    pub fn run(&mut self) -> Result<()> {
        {
            let members = self.members.clone();
            let id = self.id.clone();
            if let Ok(mut v) = members.lock() {
                v.insert(id, 0);
            }
        }

        let mut lock_name = String::new();
        write!(&mut lock_name, "hedge/spindle/{}", self.name.clone()).unwrap();
        let mut lease_ms = self.lease_ms;
        if lease_ms == 0 {
            lease_ms = 3_000;
        }

        let (tx_ldr, rx_ldr) = mpsc::channel();
        self.lock = vec![Arc::new(Mutex::new(
            LockBuilder::new()
                .db(self.db.clone())
                .table(self.table.clone())
                .name(lock_name)
                .id(self.id.clone())
                .lease_ms(lease_ms)
                .leader_tx(Some(tx_ldr))
                .build(),
        ))];

        {
            let lc = self.lock[0].clone();
            if let Ok(mut v) = lc.lock() {
                v.run()?;
            }
        }

        // We will use the channel-style callback from spindle_rs.
        let leader_setter = self.leader.clone();
        thread::spawn(move || {
            loop {
                let ldr = rx_ldr.recv();
                match ldr {
                    Ok(v) => leader_setter.store(v, Ordering::Relaxed),
                    Err(_) => {}
                }
            }
        });

        let (tx, rx): (Sender<WorkerCtrl>, Receiver<WorkerCtrl>) = unbounded();
        let rxs: Arc<Mutex<HashMap<usize, Receiver<WorkerCtrl>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cpus = num_cpus::get();

        self.tx_worker = vec![tx.clone()];

        for i in 0..cpus {
            let recv = rxs.clone();

            {
                let mut rv = recv.lock().unwrap();
                rv.insert(i, rx.clone());
            }
        }

        // Start our worker threads for our TCP server.
        for i in 0..cpus {
            let lock = self.lock[0].clone();
            let recv = rxs.clone();
            let members = self.members.clone();
            let leader = self.leader.clone();
            let toleader = match self.tx_toleader.clone() {
                Some(v) => vec![v.clone()],
                None => vec![],
            };

            thread::spawn(move || {
                loop {
                    let mut rx: Option<Receiver<WorkerCtrl>> = None;

                    {
                        let rxval = match recv.lock() {
                            Ok(v) => v,
                            Err(e) => {
                                error!("T{i}: lock failed: {e}");
                                break;
                            }
                        };

                        if let Some(v) = rxval.get(&i) {
                            rx = Some(v.clone());
                        }
                    }

                    match rx.unwrap().recv().unwrap() {
                        WorkerCtrl::TcpServer(stream) => {
                            let start = Instant::now();

                            defer! {
                                info!("[T{i}]: tcp took {:?}", start.elapsed());
                            }

                            handle_protocol(
                                i,
                                stream,
                                leader.load(Ordering::Acquire),
                                members.clone(),
                                toleader.clone(),
                            );
                        }
                        WorkerCtrl::PingMember(name) => {
                            let mut delete = false;
                            let start = Instant::now();

                            defer! {
                                info!("[T{i}]: ping took {:?}", start.elapsed());
                            }

                            'onetime: loop {
                                let hp: Vec<&str> = name.split(":").collect();
                                let hh: Vec<&str> = hp[0].split(".").collect();
                                let ip = SocketAddr::new(
                                    IpAddr::V4(Ipv4Addr::new(
                                        hh[0].parse::<u8>().unwrap(),
                                        hh[1].parse::<u8>().unwrap(),
                                        hh[2].parse::<u8>().unwrap(),
                                        hh[3].parse::<u8>().unwrap(),
                                    )),
                                    hp[1].parse::<u16>().unwrap(),
                                );

                                let mut stream = match TcpStream::connect_timeout(&ip, Duration::from_secs(5)) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        error!("connect_timeout to {name} failed: {e}");
                                        delete = true;
                                        break 'onetime;
                                    }
                                };

                                let mut send = String::new();
                                write!(&mut send, "{}\n", CMD_PING).unwrap();
                                if let Err(_) = stream.write_all(send.as_bytes()) {
                                    break 'onetime;
                                }

                                let mut reader = BufReader::new(&stream);
                                let mut resp = String::new();
                                reader.read_line(&mut resp).unwrap();

                                if !resp.starts_with("+1") {
                                    delete = true
                                }

                                break 'onetime;
                            }

                            if delete {
                                let members = members.clone();
                                if let Ok(mut v) = members.lock() {
                                    v.remove(&name);
                                }
                            }
                        }
                        WorkerCtrl::ToLeader { msg, tx } => {
                            let start = Instant::now();

                            defer! {
                                info!("[T{i}]: toleader took {:?}", start.elapsed());
                            }

                            'onetime: loop {
                                let mut leader = String::new();

                                {
                                    if let Ok(v) = lock.lock() {
                                        let (_, writer, _) = v.has_lock();
                                        write!(&mut leader, "{}", writer).unwrap();
                                    }
                                }

                                if leader.is_empty() {
                                    tx.send("-no leader".as_bytes().to_vec()).unwrap();
                                    break 'onetime;
                                }

                                let encoded = Base64::encode_string(&msg);

                                let hp: Vec<&str> = leader.split(":").collect();
                                let hh: Vec<&str> = hp[0].split(".").collect();
                                let leader_ip = SocketAddr::new(
                                    IpAddr::V4(Ipv4Addr::new(
                                        hh[0].parse::<u8>().unwrap(),
                                        hh[1].parse::<u8>().unwrap(),
                                        hh[2].parse::<u8>().unwrap(),
                                        hh[3].parse::<u8>().unwrap(),
                                    )),
                                    hp[1].parse::<u16>().unwrap(),
                                );

                                let mut stream = match TcpStream::connect_timeout(&leader_ip, Duration::from_secs(5)) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        let mut err = String::new();
                                        write!(&mut err, "-connect_timeout failed: {e}").unwrap();
                                        tx.send(err.as_bytes().to_vec()).unwrap();
                                        break 'onetime;
                                    }
                                };

                                let mut send = String::new();
                                write!(&mut send, "{}{}\n", CMD_SEND, encoded).unwrap();
                                if let Ok(_) = stream.write_all(send.as_bytes()) {
                                    let mut reader = BufReader::new(&stream);
                                    let mut resp = String::new();
                                    reader.read_line(&mut resp).unwrap();
                                    tx.send(resp[..resp.len() - 1].as_bytes().to_vec()).unwrap();
                                }

                                break 'onetime;
                            }
                        }
                    }
                }
            });
        }

        // Start our internal TCP server.
        let tx_tcp = tx.clone();
        let host = self.id.clone();
        thread::spawn(move || {
            info!("starting internal TCP server");
            let listen = TcpListener::bind(host).unwrap();
            for stream in listen.incoming() {
                let stream = match stream {
                    Ok(v) => v,
                    Err(e) => {
                        error!("stream failed: {e}");
                        continue;
                    }
                };

                tx_tcp.send(WorkerCtrl::TcpServer(stream)).unwrap();
            }
        });

        // Start the member tracking and heartbeating thread.
        let mut sync_ms = self.sync_ms;
        if sync_ms == 0 {
            sync_ms = lease_ms;
        }

        let tx_ensure = tx.clone();
        let lock = self.lock[0].clone();
        let leader_track = self.leader.clone();
        let id_1 = self.id.clone();
        let id_0 = self.id.clone();
        let members = self.members.clone();
        thread::spawn(move || {
            loop {
                let start = Instant::now();

                defer! {
                    let mut pause = sync_ms;
                    let latency = start.elapsed().as_millis() as u64;
                    if latency < sync_ms && (pause-latency) > 0 {
                        pause -= latency;
                    }

                    info!("members took {:?}", start.elapsed());
                    thread::sleep(Duration::from_millis(pause));
                }

                if leader_track.load(Ordering::Acquire) == 1 {
                    // We are leader. Ensure liveness of all members.
                    let mut mm: Vec<String> = Vec::new();

                    {
                        if let Ok(v) = members.clone().lock() {
                            for (k, _) in &*v {
                                if k != &id_1 {
                                    mm.push(k.clone());
                                }
                            }
                        }
                    }

                    for name in mm {
                        tx_ensure.send(WorkerCtrl::PingMember(name)).unwrap();
                    }

                    {
                        if let Ok(v) = members.clone().lock() {
                            info!("{} member(s) tracked", v.len());
                        }
                    }
                } else {
                    // We're not leader. Send heartbeats to leader.
                    let mut leader = String::new();

                    {
                        if let Ok(v) = lock.lock() {
                            let (_, writer, _) = v.has_lock();
                            write!(&mut leader, "{}", writer).unwrap();
                        }
                    }

                    if leader.is_empty() {
                        continue;
                    }

                    let hp: Vec<&str> = leader.split(":").collect();
                    let hh: Vec<&str> = hp[0].split(".").collect();
                    let leader_ip = SocketAddr::new(
                        IpAddr::V4(Ipv4Addr::new(
                            hh[0].parse::<u8>().unwrap(),
                            hh[1].parse::<u8>().unwrap(),
                            hh[2].parse::<u8>().unwrap(),
                            hh[3].parse::<u8>().unwrap(),
                        )),
                        hp[1].parse::<u16>().unwrap(),
                    );

                    let mut stream = match TcpStream::connect_timeout(&leader_ip, Duration::from_secs(5)) {
                        Ok(v) => v,
                        Err(e) => {
                            error!("connect_timeout failed: {e}");
                            continue;
                        }
                    };

                    let mut send = String::new();
                    write!(&mut send, "{}{}\n", CMD_PING, id_0).unwrap();
                    if let Ok(_) = stream.write_all(send.as_bytes()) {
                        let mut reader = BufReader::new(&stream);
                        let mut resp = String::new();
                        reader.read_line(&mut resp).unwrap();

                        info!("response: {resp:?}");

                        if resp.chars().nth(0).unwrap() != '+' {
                            continue;
                        }

                        let mm: Vec<&str> = resp[1..resp.len() - 1].split(",").collect();
                        if mm.len() > 0 {
                            if let Ok(mut v) = members.clone().lock() {
                                v.clear();
                                for m in mm {
                                    if m.len() > 0 && !m.starts_with("+") {
                                        v.insert(m.to_string(), 0);
                                    }
                                }
                            }
                        }

                        {
                            if let Ok(v) = members.clone().lock() {
                                info!("{} member(s) tracked", v.len());
                            }
                        }
                    }
                }
            }
        });

        // Finally, set the system active.
        let active = self.active.clone();
        active.store(1, Ordering::Relaxed);

        Ok(())
    }

    /// Returns true if this instance got the lock, together with the name and lock token.
    pub fn has_lock(&self) -> (bool, String, u64) {
        let active = self.active.clone();
        if active.load(Ordering::Acquire) == 0 {
            return (false, String::from(""), 0);
        }

        let lock = self.lock[0].clone();
        if let Ok(v) = lock.lock() {
            return v.has_lock();
        }

        return (false, String::from(""), 0);
    }

    /// Returns a list of current members in the group/cluster.
    pub fn members(&mut self) -> Vec<String> {
        let mut ret: Vec<String> = Vec::new();
        let active = self.active.clone();
        if active.load(Ordering::Acquire) == 0 {
            return ret;
        }

        if let Ok(v) = self.members.lock() {
            for (k, _) in &*v {
                ret.push(k.clone());
            }
        }

        return ret;
    }

    /// TODO: Send to leader.
    pub fn send(&mut self, msg: Vec<u8>) -> Result<Vec<u8>, Error> {
        let active = self.active.clone();
        if active.load(Ordering::Acquire) == 0 {
            return Err(anyhow!("still initializing"));
        }

        let (tx, rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = unbounded();
        self.tx_worker[0].send(WorkerCtrl::ToLeader { msg, tx }).unwrap();
        let r = rx.recv().unwrap();
        match r[0] {
            b'+' => return Ok(r[1..].to_vec()),
            b'-' => return Err(anyhow!(String::from_utf8(r[1..].to_vec()).unwrap())),
            _ => return Err(anyhow!("unknown")),
        }
    }

    pub fn close(&mut self) {
        let lock = self.lock[0].clone();
        if let Ok(mut v) = lock.lock() {
            v.close();
        }
    }
}

/// `LockBuilder` builds an instance of Lock with default values.
#[derive(Default)]
pub struct OpBuilder {
    db: String,
    table: String,
    name: String,
    id: String,
    lease_ms: u64,
    sync_ms: u64,
    tx_toleader: Option<mpsc::Sender<Comms>>,
}

impl OpBuilder {
    pub fn new() -> OpBuilder {
        OpBuilder::default()
    }

    /// Sets the internal lock's Spanner database URL.
    pub fn db(mut self, db: String) -> OpBuilder {
        self.db = db;
        self
    }

    /// Sets the internal lock's Spanner table for backing storage.
    pub fn table(mut self, table: String) -> OpBuilder {
        self.table = table;
        self
    }

    /// Sets the internal lock name.
    pub fn name(mut self, name: String) -> OpBuilder {
        self.name = name;
        self
    }

    /// Sets this instance (or node) id. Format should be `host:port`.
    pub fn id(mut self, id: String) -> OpBuilder {
        self.id = id;
        self
    }

    /// Sets the internal lock's leader lease timeout.
    pub fn lease_ms(mut self, ms: u64) -> OpBuilder {
        self.lease_ms = ms;
        self
    }

    /// Sets the timeout for syncing member info across the group.
    pub fn sync_ms(mut self, ms: u64) -> OpBuilder {
        self.sync_ms = ms;
        self
    }

    /// TODO:
    pub fn toleader(mut self, tx: Option<mpsc::Sender<Comms>>) -> OpBuilder {
        self.tx_toleader = tx;
        self
    }

    pub fn build(self) -> Op {
        Op {
            db: self.db,
            table: self.table,
            name: self.name,
            id: if self.id != "" {
                self.id
            } else {
                let id = Uuid::new_v4();
                id.to_string()
            },
            lock: vec![],
            leader: Arc::new(AtomicUsize::new(0)),
            lease_ms: self.sync_ms,
            sync_ms: self.sync_ms,
            members: Arc::new(Mutex::new(HashMap::new())),
            tx_worker: vec![],
            tx_toleader: self.tx_toleader,
            active: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_run() {
        let op = OpBuilder::new()
            .db("projects/p/instances/i/databases/db".to_string())
            .table("locktable".to_string())
            .name("hedge-rs".to_string())
            .id(":8080".to_string())
            .lease_ms(3_000)
            .build();

        let (locked, _, token) = op.has_lock();
        assert_eq!(locked, false);
        assert_eq!(token, 0);
    }
}
