//! `durable-streams-server prune`, driven through the real binary against a store
//! a real server wrote: the pruned store must boot, recover and list only the
//! kept tenant's streams and subscriptions, and every refusal must leave the
//! data directory untouched.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};

const STORE_ID: &str = "2bc96d0b-9740-4f50-97c6-754b2b27d6b0";
const STORE_GENERATION: &str = "ff8b5fa6-e786-4994-8da0-f14e9e79f318";
const FILESYSTEM_UUID: &str = "253f14d5-cbee-4df8-9e3c-e44c6e41501b";
const DIGEST: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

const MIGHTY_THREAD: &str = "/mighty-next/v1/prod/threads/t-1";
const MIGHTY_WAKE: &str = "/mighty-next/v1/prod/wake/pool";
const MIGHTY_SOURCE: &str =
    "/circuits/v1/source-0123456789abcdef0123456789abcdef01234567/stores/g/shape";
const INDEXED_SHAPE: &str = "/circuits/v1/prod/stores/g/shape.v1";
const INDEXED_WAKE: &str = "/circuits/v1/prod/stores/g/wake/pool";
const INDEXED_RUN: &str = "/agent-runs/v1/prod/run-1";

fn server() -> Command {
    Command::new(env!("CARGO_BIN_EXE_durable-streams-server"))
}

fn bootstrap(dir: &Path) {
    let out = server()
        .args(["bootstrap-store", "--data-dir"])
        .arg(dir)
        .args([
            "--store-id",
            STORE_ID,
            "--store-generation",
            STORE_GENERATION,
            "--protocol-version",
            "1",
            "--layout-version",
            "1",
            "--durability-mode",
            "wal",
            "--wal-shards",
            "1",
            "--stream-lanes",
            "1",
            "--filesystem-uuid",
            FILESYSTEM_UUID,
            "--creation-time",
            "2026-08-27T19:00:00Z",
        ])
        .output()
        .expect("bootstrap");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A WAL server over `dir`, killed (SIGKILL, like the crash-consistent
/// snapshot the prune runs on) when dropped.
struct Server {
    child: Child,
    port: u16,
}

impl Server {
    fn start(dir: &Path) -> Self {
        let port = TcpListener::bind(("127.0.0.1", 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let child = server()
            .args(["--durability", "wal", "--data-dir"])
            .arg(dir)
            .args(["--port", &port.to_string()])
            .args([
                "--store-id",
                STORE_ID,
                "--store-generation",
                STORE_GENERATION,
            ])
            .args(["--protocol-version", "1", "--layout-version", "1"])
            .args([
                "--filesystem-uuid",
                FILESYSTEM_UUID,
                "--artifact-digest",
                DIGEST,
            ])
            .args(["--wal-shards", "1", "--stream-lanes", "1"])
            .args(["--minimum-free-bytes", "0", "--minimum-free-inodes", "0"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn server");
        let server = Self { child, port };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !server.request("GET", "/_admin/ready", &[], b"").0.eq(&200) {
            assert!(
                std::time::Instant::now() < deadline,
                "server never became ready"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        server
    }

    /// One HTTP/1.1 request; (status, body). Status 0 means no connection yet.
    fn request(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> (u16, String) {
        let Ok(mut stream) = TcpStream::connect(("127.0.0.1", self.port)) else {
            return (0, String::new());
        };
        let mut head = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n",
            body.len()
        );
        for (name, value) in headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }
        head.push_str("\r\n");
        stream.write_all(head.as_bytes()).unwrap();
        stream.write_all(body).unwrap();
        let mut response = String::new();
        let _ = stream.read_to_string(&mut response);
        let status = response
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .unwrap_or(0);
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_string())
            .unwrap_or_default();
        (status, body)
    }

    fn create(&self, path: &str, content_type: &str, extra: &[(&str, &str)]) {
        let mut headers = vec![("Content-Type", content_type)];
        headers.extend_from_slice(extra);
        let (status, body) = self.request("PUT", path, &headers, b"");
        assert!(matches!(status, 200 | 201), "PUT {path}: {status} {body}");
    }

    fn append(&self, path: &str, bytes: &[u8]) {
        let headers = [("Content-Type", "application/octet-stream")];
        let (status, body) = self.request("POST", path, &headers, bytes);
        assert!(matches!(status, 200 | 204), "POST {path}: {status} {body}");
    }

    fn subscribe(&self, root: &str, id: &str, stream: &str, wake_stream: &str) {
        let body = format!(
            r#"{{"type":"pull-wake","streams":["{stream}"],"wake_stream":"{wake_stream}"}}"#
        );
        let (status, response) = self.request(
            "PUT",
            &format!("{root}/__ds/subscriptions/{id}"),
            &[("Content-Type", "application/json")],
            body.as_bytes(),
        );
        assert!(
            matches!(status, 200 | 201),
            "subscribe {root} {id}: {status} {response}"
        );
    }

    /// Every stream the recovered store lists, with its durable bytes.
    fn inventory(&self) -> Vec<(String, u64)> {
        let (status, body) = self.request("GET", "/_admin/inventory?limit=1000", &[], b"");
        assert_eq!(status, 200, "{body}");
        let page: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(page["next_cursor"].is_null());
        let mut streams: Vec<(String, u64)> = page["streams"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| {
                (
                    s["path"].as_str().unwrap().to_string(),
                    s["durable_bytes"].as_u64().unwrap(),
                )
            })
            .collect();
        streams.sort();
        streams
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn prune(dir: &Path, tenant: &str, dry_run: bool) -> Output {
    let mut command = server();
    command
        .args(["prune", "--data-dir"])
        .arg(dir)
        .args(["--tenant", tenant]);
    if dry_run {
        command.arg("--dry-run");
    }
    command.output().expect("run prune")
}

/// Every file under `dir` with its size, to prove a refusal or dry run wrote nothing.
fn tree(dir: &Path) -> Vec<(String, u64)> {
    let mut files = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in std::fs::read_dir(&next).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(metadata) = std::fs::metadata(&path) {
                // A running server can unlink a temp file between the listing and this.
                let name = path.strip_prefix(dir).unwrap().display().to_string();
                files.push((name, metadata.len()));
            }
        }
    }
    files.sort();
    files
}

/// A store holding both tenants' streams and a pull-wake subscription each.
fn two_tenant_store(dir: &Path) {
    bootstrap(dir);
    let server = Server::start(dir);
    for path in [MIGHTY_THREAD, MIGHTY_SOURCE, INDEXED_SHAPE, INDEXED_RUN] {
        server.create(path, "application/octet-stream", &[]);
    }
    server.append(MIGHTY_THREAD, b"mighty");
    server.append(MIGHTY_SOURCE, b"source");
    server.append(INDEXED_SHAPE, b"indexed");
    server.append(INDEXED_RUN, b"run");
    server.create(MIGHTY_WAKE, "application/json", &[]);
    server.create(INDEXED_WAKE, "application/json", &[]);
    server.subscribe(
        "/mighty-next/v1/prod",
        "mighty-workers",
        "threads/t-1",
        "wake/pool",
    );
    server.subscribe(
        "/circuits/v1/prod/stores/g",
        "indexed-workers",
        "shape.v1",
        "wake/pool",
    );
}

/// The report row for `label`: (action, streams, bytes, subscriptions).
fn row(stdout: &str, label: &str) -> (String, u64, u64, u64) {
    let line = stdout
        .lines()
        .find(|line| line.starts_with(label))
        .unwrap_or_else(|| panic!("no {label} row in:\n{stdout}"));
    let fields: Vec<&str> = line[label.len()..].split_whitespace().collect();
    (
        fields[1].to_string(),
        fields[2].parse().unwrap(),
        fields[3].parse().unwrap(),
        fields[4].parse().unwrap(),
    )
}

#[test]
fn a_pruned_store_boots_with_only_the_kept_tenant() {
    let dir = tempfile::tempdir().unwrap();
    two_tenant_store(dir.path());
    let before = tree(dir.path());

    let dry = prune(dir.path(), "mighty", true);
    let stdout = String::from_utf8_lossy(&dry.stdout).to_string();
    assert_eq!(
        dry.status.code(),
        Some(0),
        "{stdout}{}",
        String::from_utf8_lossy(&dry.stderr)
    );
    assert_eq!(tree(dir.path()), before, "a dry run must write nothing");
    let (action, streams, _, subscriptions) = row(&stdout, "/mighty-next/v1/prod/…");
    assert_eq!((action.as_str(), streams, subscriptions), ("keep", 2, 1));
    let (action, streams, _, subscriptions) = row(&stdout, "/circuits/v1/source-<40 hex>/…");
    assert_eq!((action.as_str(), streams, subscriptions), ("keep", 1, 0));
    let (action, streams, _, subscriptions) = row(&stdout, "/circuits/v1/prod/…");
    assert_eq!((action.as_str(), streams, subscriptions), ("remove", 2, 1));
    let (action, streams, bytes, subscriptions) = row(&stdout, "/agent-runs/v1/prod/…");
    assert_eq!((action.as_str(), streams, subscriptions), ("remove", 1, 0));
    assert!(bytes > 0, "removed bytes are counted: {stdout}");

    let pruned = prune(dir.path(), "mighty", false);
    assert_eq!(
        pruned.status.code(),
        Some(0),
        "{}{}",
        String::from_utf8_lossy(&pruned.stdout),
        String::from_utf8_lossy(&pruned.stderr)
    );
    assert!(dir.path().join("streams/.lanes").exists());
    assert!(dir.path().join("streams/.lane").exists());

    let server = Server::start(dir.path());
    assert_eq!(
        server.inventory(),
        vec![
            (MIGHTY_SOURCE.to_string(), 6),
            (MIGHTY_THREAD.to_string(), 6),
            (MIGHTY_WAKE.to_string(), 0),
        ]
    );
    let (status, _) = server.request("GET", MIGHTY_THREAD, &[], b"");
    assert_eq!(status, 200);
    let (status, _) = server.request("GET", INDEXED_SHAPE, &[], b"");
    assert_eq!(status, 404);
    let (status, _) = server.request(
        "GET",
        "/mighty-next/v1/prod/__ds/subscriptions/mighty-workers",
        &[],
        b"",
    );
    assert_eq!(status, 200);
    let (status, _) = server.request(
        "GET",
        "/circuits/v1/prod/stores/g/__ds/subscriptions/indexed-workers",
        &[],
        b"",
    );
    assert_eq!(status, 404);
    // The removed paths are free again on the pruned store.
    server.create(INDEXED_SHAPE, "application/octet-stream", &[]);
}

#[test]
fn prune_refuses_a_data_directory_a_running_server_holds() {
    let dir = tempfile::tempdir().unwrap();
    two_tenant_store(dir.path());
    let server = Server::start(dir.path());
    // The running server may still flush sidecars, so compare which exist.
    let sidecars = |dir: &Path| -> Vec<String> {
        tree(dir)
            .into_iter()
            .map(|(name, _)| name)
            .filter(|name| name.ends_with(".meta"))
            .collect()
    };
    let before = sidecars(dir.path());
    let out = prune(dir.path(), "mighty", false);
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("locked"));
    assert_eq!(sidecars(dir.path()), before);
    drop(server);
}

#[test]
fn prune_deletes_nothing_when_a_stream_matches_neither_tenant() {
    let dir = tempfile::tempdir().unwrap();
    two_tenant_store(dir.path());
    {
        let server = Server::start(dir.path());
        server.create(
            "/circuits/v1/dev/stores/g/x",
            "application/octet-stream",
            &[],
        );
    }
    let before = tree(dir.path());
    let out = prune(dir.path(), "mighty", false);
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("\"/circuits/v1/dev/stores/g/x\""),
        "{stderr}"
    );
    assert!(stderr.contains("nothing was deleted"), "{stderr}");
    assert_eq!(tree(dir.path()), before);
}

#[test]
fn prune_deletes_nothing_when_a_kept_stream_is_forked_from_a_removed_one() {
    let dir = tempfile::tempdir().unwrap();
    two_tenant_store(dir.path());
    {
        let server = Server::start(dir.path());
        server.create(
            "/mighty-next/v1/prod/fork",
            "application/octet-stream",
            &[("Stream-Forked-From", INDEXED_SHAPE)],
        );
    }
    let before = tree(dir.path());
    let out = prune(dir.path(), "mighty", false);
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("is forked from \"/circuits/v1/prod/stores/g/shape.v1\""),
        "{stderr}"
    );
    assert_eq!(tree(dir.path()), before);
    // Removing the fork while keeping its parent is allowed: recovery rebuilds
    // the parent's fork refcount from the children that remain.
    let out = prune(dir.path(), "indexed", false);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let server = Server::start(dir.path());
    let paths: Vec<String> = server
        .inventory()
        .into_iter()
        .map(|(path, _)| path)
        .collect();
    assert_eq!(paths, [INDEXED_RUN, INDEXED_SHAPE, INDEXED_WAKE]);
    // With its only fork gone, the parent deletes outright.
    let (status, _) = server.request("DELETE", INDEXED_SHAPE, &[], b"");
    assert_eq!(status, 204);
    let (status, _) = server.request("GET", INDEXED_SHAPE, &[], b"");
    assert_eq!(status, 404);
}

#[test]
fn prune_usage_errors_exit_2() {
    for args in [
        vec!["prune"],
        vec!["prune", "--data-dir", "/nonexistent"],
        vec!["prune", "--tenant", "mighty"],
        vec!["prune", "--data-dir", "/x", "--tenant", "other"],
        vec!["prune", "--data-dir", "/x", "--tenant", "mighty", "--force"],
    ] {
        let out = server().args(&args).output().unwrap();
        assert_eq!(out.status.code(), Some(2), "{args:?}");
    }
}
