//! `durable-streams-server prune`: remove one tenant's streams and subscriptions
//! from a STOPPED store's data directory, so a volume copied by snapshot keeps
//! only the tenant it will serve.
//!
//! Streams are chosen by the `path` recorded in each `.meta` sidecar, matched by
//! whole path segments against the tenant patterns — never by data-file name,
//! which is a truncated, lossy encoding of the path. Every removal is planned
//! before any is made: a stream under neither tenant's patterns, a kept stream
//! forked from a removed one, a quarantined (`.meta.corrupt`) or unparsable
//! sidecar, or a subscription under neither tenant stops the prune with nothing
//! deleted.
//!
//! A removed stream loses its data file, its compaction temp file, its local
//! sealed chunks and its sidecar. Recovery already treats a stream whose files
//! are gone as deleted: the sidecar pass finds nothing, WAL replay skips records
//! for unknown stream ids, and fork refcounts are rebuilt from the surviving
//! graph. Lane markers (`.lanes`, `.lane`) are never touched.
//!
//! The copy this runs on is usually a volume restored from an EBS snapshot,
//! whose blocks load lazily on first read, so sidecar reads and unlinks run on
//! many threads at once.

use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::store::Meta;

/// Concurrent sidecar reads and unlinks. A lazily restored EBS volume serves
/// each first read from S3, so throughput scales with queue depth, not CPU.
const IO_THREADS: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tenant {
    Indexed,
    Mighty,
}

impl Tenant {
    fn name(self) -> &'static str {
        match self {
            Tenant::Indexed => "indexed",
            Tenant::Mighty => "mighty",
        }
    }
}

/// The tenant patterns, in report order. Each is a whole-segment path prefix
/// followed by at least one more segment.
const PATTERNS: [(Tenant, &str); 4] = [
    (Tenant::Indexed, "/circuits/v1/prod/…"),
    (Tenant::Indexed, "/agent-runs/v1/prod/…"),
    (Tenant::Mighty, "/mighty-next/v1/prod/…"),
    (Tenant::Mighty, "/circuits/v1/source-<40 hex>/…"),
];

/// circuitsd scopes each source as `source-<first 40 hex of sha256(source id)>`.
fn is_source_scope(segment: &str) -> bool {
    segment.strip_prefix("source-").is_some_and(|hex| {
        hex.len() == 40 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    })
}

/// Index into `PATTERNS` of the pattern `path` falls under, or `None`. A stream
/// path needs a segment below the pattern's prefix; a subscription root may be
/// the prefix itself (`allow_prefix`).
fn classify(path: &str, allow_prefix: bool) -> Option<usize> {
    let segments: Vec<&str> = path.strip_prefix('/')?.split('/').collect();
    if segments.len() < 3 || segments[..3].iter().any(|s| s.is_empty()) {
        return None;
    }
    let accepted = match segments.get(3) {
        Some(below) => !below.is_empty(),
        None => allow_prefix,
    };
    if !accepted {
        return None;
    }
    match (segments[0], segments[1], segments[2]) {
        ("circuits", "v1", "prod") => Some(0),
        ("agent-runs", "v1", "prod") => Some(1),
        ("mighty-next", "v1", "prod") => Some(2),
        ("circuits", "v1", scope) if is_source_scope(scope) => Some(3),
        _ => None,
    }
}

/// One stream, from its sidecar.
struct Stream {
    sidecar: PathBuf,
    path: String,
    forked_from: Option<String>,
    pattern: Option<usize>,
    /// Data file, compaction temp file and local sealed chunks that exist.
    files: Vec<PathBuf>,
    /// Bytes of the sidecar and every file in `files`.
    bytes: u64,
}

#[derive(Default, Clone, Copy)]
struct Counts {
    streams: u64,
    bytes: u64,
    subscriptions: u64,
}

struct SubscriptionPlan {
    state_path: PathBuf,
    /// The whole persisted document, rewritten with only the kept entries.
    document: serde_json::Value,
    kept: Vec<serde_json::Value>,
    removed: usize,
}

struct Plan {
    keep: Tenant,
    data_dir: PathBuf,
    lane_dirs: Vec<PathBuf>,
    streams: Vec<Stream>,
    /// Indexes into `streams` of the streams to remove.
    remove: Vec<usize>,
    /// Files shared by a kept and a removed stream (colliding compaction temp
    /// names); never removed.
    shared_files: HashSet<PathBuf>,
    subscriptions: Option<SubscriptionPlan>,
    per_pattern: [Counts; 4],
    orphan_data_files: u64,
    orphan_data_bytes: u64,
    problems: Vec<String>,
}

fn exit_usage(message: impl std::fmt::Display) -> ! {
    eprintln!("error: {message}");
    eprintln!("usage: durable-streams-server prune --data-dir <dir> --tenant <indexed|mighty> [--dry-run]");
    std::process::exit(2)
}

fn exit_refused(message: impl std::fmt::Display) -> ! {
    eprintln!("prune: {message}");
    std::process::exit(1)
}

pub fn run(args: &[String]) -> ! {
    let mut data_dir: Option<PathBuf> = None;
    let mut keep: Option<Tenant> = None;
    let mut dry_run = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--dry-run" => dry_run = true,
            flag @ ("--data-dir" | "--tenant") => {
                index += 1;
                let Some(value) = args.get(index) else {
                    exit_usage(format!("{flag} requires a value"));
                };
                if flag == "--data-dir" {
                    data_dir = Some(value.into());
                } else {
                    keep = Some(match value.as_str() {
                        "indexed" => Tenant::Indexed,
                        "mighty" => Tenant::Mighty,
                        other => {
                            exit_usage(format!("--tenant must be indexed or mighty, got {other:?}"))
                        }
                    });
                }
            }
            other => exit_usage(format!("prune unknown argument: {other}")),
        }
        index += 1;
    }
    let data_dir = data_dir.unwrap_or_else(|| exit_usage("prune requires --data-dir"));
    let keep = keep.unwrap_or_else(|| exit_usage("prune requires --tenant"));

    if !data_dir.join("streams").is_dir() {
        exit_refused(format!(
            "{} has no streams/ directory; not a Durable Streams data directory",
            data_dir.display()
        ));
    }
    // Held until exit: no server can open the store while it is being pruned.
    let _lock = crate::data_dir_lock::DataDirLock::acquire(&data_dir).unwrap_or_else(|error| {
        exit_refused(format!(
            "{error}; prune runs only on a stopped store, so stop the server that holds it"
        ))
    });

    let plan = plan(&data_dir, keep).unwrap_or_else(|error| exit_refused(error));
    let mut stdout = io::stdout().lock();
    let _ = write_report(&mut stdout, &plan, dry_run);
    let _ = stdout.flush();
    if !plan.problems.is_empty() {
        for problem in &plan.problems {
            eprintln!("prune: {problem}");
        }
        exit_refused(format!(
            "refusing to prune: {} problem(s) above; nothing was deleted",
            plan.problems.len()
        ));
    }
    if dry_run {
        std::process::exit(0);
    }
    match execute(&plan) {
        Ok(()) => {
            let removed = total(&plan, |tenant| tenant != keep);
            println!(
                "removed {} streams ({} bytes) and {} subscriptions",
                removed.streams, removed.bytes, removed.subscriptions
            );
            std::process::exit(0)
        }
        Err(error) => exit_refused(format!(
            "{error}; the prune stopped part way and can be run again"
        )),
    }
}

/// The lane directories holding stream files: `streams/` itself in the flat
/// layout, else `streams/<0..N>/` as recorded in the `.lanes` marker.
fn lane_dirs(data_dir: &Path) -> io::Result<Vec<PathBuf>> {
    let root = data_dir.join("streams");
    let marker = root.join(".lanes");
    let lanes: usize = match std::fs::read_to_string(&marker) {
        Ok(text) => text.trim().parse().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("corrupt stream-lane marker {}", marker.display()),
            )
        })?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => 1,
        Err(error) => return Err(error),
    };
    Ok(if lanes <= 1 {
        vec![root]
    } else {
        (0..lanes).map(|lane| root.join(lane.to_string())).collect()
    })
}

/// Map `items` through `f` on up to `IO_THREADS` threads, preserving order.
fn parallel_map<T: Sync, R: Send>(items: &[T], f: impl Fn(&T) -> R + Sync) -> Vec<R> {
    let next = AtomicUsize::new(0);
    let threads = IO_THREADS.min(items.len()).max(1);
    let mut results: Vec<Option<R>> = (0..items.len()).map(|_| None).collect();
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| {
                    let mut out = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= items.len() {
                            return out;
                        }
                        out.push((i, f(&items[i])));
                    }
                })
            })
            .collect();
        for worker in workers {
            for (i, result) in worker.join().expect("prune worker panicked") {
                results[i] = Some(result);
            }
        }
    });
    results
        .into_iter()
        .map(|r| r.expect("every item mapped"))
        .collect()
}

/// Size of `path`, or `None` when it does not exist.
fn file_len(path: &Path) -> io::Result<Option<u64>> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(Some(metadata.len())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io::Error::new(
            error.kind(),
            format!("cannot stat {}: {error}", path.display()),
        )),
    }
}

enum SidecarRead {
    Stream(Stream),
    Unparsable(PathBuf),
}

fn read_sidecar(
    sidecar: &Path,
    segments_dir: &Path,
    compact_temps: &HashSet<PathBuf>,
) -> io::Result<SidecarRead> {
    let bytes = std::fs::read(sidecar).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("cannot read stream sidecar {}: {error}", sidecar.display()),
        )
    })?;
    let Ok(meta) = serde_json::from_slice::<Meta>(&bytes) else {
        return Ok(SidecarRead::Unparsable(sidecar.to_path_buf()));
    };
    let name = sidecar
        .file_name()
        .and_then(|n| n.to_str())
        .expect("listed as UTF-8");
    let data = sidecar.with_file_name(name.strip_suffix(".meta").expect("listed as .meta"));
    let mut files = Vec::new();
    let mut total = bytes.len() as u64;
    if let Some(len) = file_len(&data)? {
        total += len;
        files.push(data.clone());
    }
    // Named exactly as the server names it (`with_extension`, so a path with a
    // dot in its last segment shares the stem's temp name).
    let compact = data.with_extension("compact.tmp");
    if compact_temps.contains(&compact) {
        if let Some(len) = file_len(&compact)? {
            total += len;
            files.push(compact);
        }
    }
    for segment in &meta.segments {
        if let Some(local) = &segment.local_file {
            let chunk = segments_dir.join(local);
            if let Some(len) = file_len(&chunk)? {
                total += len;
                files.push(chunk);
            }
        }
    }
    Ok(SidecarRead::Stream(Stream {
        sidecar: sidecar.to_path_buf(),
        pattern: classify(&meta.path, false),
        path: meta.path,
        forked_from: meta.forked_from,
        files,
        bytes: total,
    }))
}

fn plan(data_dir: &Path, keep: Tenant) -> io::Result<Plan> {
    let lane_dirs = lane_dirs(data_dir)?;
    let mut problems = Vec::new();
    let mut sidecars = Vec::new();
    let mut data_files = Vec::new();
    let mut compact_temps = HashSet::new();
    for lane in &lane_dirs {
        for entry in std::fs::read_dir(lane)? {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                problems.push(format!("non-UTF-8 file name in {}", lane.display()));
                continue;
            };
            if name == ".lanes"
                || name == ".lane"
                || name == ".lanes.tmp"
                || entry.file_type()?.is_dir()
            {
                continue;
            }
            if name.ends_with(".meta.corrupt") {
                problems.push(format!(
                    "quarantined sidecar {}: repair or remove it before pruning",
                    path.display()
                ));
            } else if name.ends_with(".meta.tmp") {
                // An interrupted sidecar write; recovery removes it.
            } else if name.ends_with(".compact.tmp") {
                compact_temps.insert(path);
            } else if name.ends_with(".meta") {
                sidecars.push(path);
            } else {
                data_files.push(path);
            }
        }
    }

    let segments_dir = data_dir.join("segments");
    let mut streams = Vec::with_capacity(sidecars.len());
    for read in parallel_map(&sidecars, |sidecar| {
        read_sidecar(sidecar, &segments_dir, &compact_temps)
    }) {
        match read? {
            SidecarRead::Stream(stream) => streams.push(stream),
            SidecarRead::Unparsable(sidecar) => problems.push(format!(
                "unparsable sidecar {}: repair or remove it before pruning",
                sidecar.display()
            )),
        }
    }

    // Data files no sidecar names are orphans that recovery drops at boot.
    let claimed: HashSet<&Path> = streams
        .iter()
        .flat_map(|stream| stream.files.iter().map(PathBuf::as_path))
        .collect();
    let orphans: Vec<&PathBuf> = data_files
        .iter()
        .filter(|path| !claimed.contains(path.as_path()))
        .collect();
    let mut orphan_data_bytes = 0;
    for len in parallel_map(&orphans, |path| file_len(path)) {
        orphan_data_bytes += len?.unwrap_or(0);
    }

    let tenant_of = |pattern: Option<usize>| pattern.map(|p| PATTERNS[p].0);
    let tenant_by_path: HashMap<&str, Option<Tenant>> = streams
        .iter()
        .map(|stream| (stream.path.as_str(), tenant_of(stream.pattern)))
        .collect();
    let mut per_pattern = [Counts::default(); 4];
    let mut remove = Vec::new();
    let mut kept_files = HashSet::new();
    for (index, stream) in streams.iter().enumerate() {
        let Some(pattern) = stream.pattern else {
            problems.push(format!(
                "stream {:?} ({}) matches neither tenant's patterns",
                stream.path,
                stream.sidecar.display()
            ));
            continue;
        };
        per_pattern[pattern].streams += 1;
        per_pattern[pattern].bytes += stream.bytes;
        if PATTERNS[pattern].0 == keep {
            kept_files.extend(stream.files.iter().cloned());
            if let Some(parent) = &stream.forked_from {
                if let Some(Some(tenant)) = tenant_by_path.get(parent.as_str()) {
                    if *tenant != keep {
                        problems.push(format!(
                            "kept stream {:?} is forked from {:?}, which would be removed",
                            stream.path, parent
                        ));
                    }
                }
            }
        } else {
            remove.push(index);
        }
    }
    let shared_files = remove
        .iter()
        .flat_map(|&i| streams[i].files.iter())
        .filter(|file| kept_files.contains(*file))
        .cloned()
        .collect();

    let subscriptions = plan_subscriptions(data_dir, keep, &mut per_pattern, &mut problems)?;
    Ok(Plan {
        keep,
        data_dir: data_dir.to_path_buf(),
        lane_dirs,
        streams,
        remove,
        shared_files,
        subscriptions,
        per_pattern,
        orphan_data_files: orphans.len() as u64,
        orphan_data_bytes,
        problems,
    })
}

/// Sort the persisted subscriptions by the tenant of their stream root (the
/// path before `/__ds`, under which every stream they watch lives).
fn plan_subscriptions(
    data_dir: &Path,
    keep: Tenant,
    per_pattern: &mut [Counts; 4],
    problems: &mut Vec<String>,
) -> io::Result<Option<SubscriptionPlan>> {
    let state_path = data_dir.join("subscriptions").join("state.json");
    let bytes = match std::fs::read(&state_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let document: serde_json::Value = serde_json::from_slice(&bytes).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "corrupt subscription state {}: {error}",
                state_path.display()
            ),
        )
    })?;
    let Some(entries) = document.get("subscriptions").and_then(|s| s.as_array()) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "subscription state {} has no subscriptions list",
                state_path.display()
            ),
        ));
    };
    let mut kept = Vec::new();
    let mut removed = 0;
    for entry in entries {
        let id = entry.get("id").and_then(|v| v.as_str()).unwrap_or("?");
        let root = entry
            .get("stream_root")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let Some(pattern) = classify(root, true) else {
            problems.push(format!(
                "subscription {id:?} under {root:?} matches neither tenant's patterns"
            ));
            continue;
        };
        per_pattern[pattern].subscriptions += 1;
        if PATTERNS[pattern].0 == keep {
            kept.push(entry.clone());
        } else {
            removed += 1;
        }
    }
    Ok(Some(SubscriptionPlan {
        state_path,
        document,
        kept,
        removed,
    }))
}

fn total(plan: &Plan, select: impl Fn(Tenant) -> bool) -> Counts {
    let mut sum = Counts::default();
    for (pattern, counts) in plan.per_pattern.iter().enumerate() {
        if select(PATTERNS[pattern].0) {
            sum.streams += counts.streams;
            sum.bytes += counts.bytes;
            sum.subscriptions += counts.subscriptions;
        }
    }
    sum
}

fn write_report(out: &mut impl Write, plan: &Plan, dry_run: bool) -> io::Result<()> {
    let other = match plan.keep {
        Tenant::Indexed => Tenant::Mighty,
        Tenant::Mighty => Tenant::Indexed,
    };
    writeln!(
        out,
        "prune: keeping {}, removing {}{}",
        plan.keep.name(),
        other.name(),
        if dry_run {
            " (dry run: nothing is deleted)"
        } else {
            ""
        }
    )?;
    writeln!(
        out,
        "{:<32} {:<8} {:<7} {:>10} {:>16} {:>13}",
        "pattern", "tenant", "action", "streams", "bytes", "subscriptions"
    )?;
    fn line(
        out: &mut impl Write,
        label: &str,
        tenant: &str,
        action: &str,
        c: Counts,
    ) -> io::Result<()> {
        writeln!(
            out,
            "{:<32} {:<8} {:<7} {:>10} {:>16} {:>13}",
            label, tenant, action, c.streams, c.bytes, c.subscriptions
        )
    }
    for (pattern, (tenant, label)) in PATTERNS.iter().enumerate() {
        let action = if *tenant == plan.keep {
            "keep"
        } else {
            "remove"
        };
        line(out, label, tenant.name(), action, plan.per_pattern[pattern])?;
    }
    line(
        out,
        "total",
        plan.keep.name(),
        "keep",
        total(plan, |t| t == plan.keep),
    )?;
    line(
        out,
        "total",
        other.name(),
        "remove",
        total(plan, |t| t != plan.keep),
    )?;
    writeln!(
        out,
        "orphan data files left for recovery to drop: {} ({} bytes)",
        plan.orphan_data_files, plan.orphan_data_bytes
    )?;
    writeln!(out, "problems: {}", plan.problems.len())
}

fn remove_if_present(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io::Error::new(
            error.kind(),
            format!("cannot remove {}: {error}", path.display()),
        )),
    }
}

fn fsync_dir(dir: &Path) -> io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

fn execute(plan: &Plan) -> io::Result<()> {
    // The sidecar goes last, so a prune interrupted here still finds the
    // stream by its recorded path when it is run again.
    for result in parallel_map(&plan.remove, |&index| {
        let stream = &plan.streams[index];
        for file in &stream.files {
            if !plan.shared_files.contains(file) {
                remove_if_present(file)?;
            }
        }
        remove_if_present(&stream.sidecar)
    }) {
        result?;
    }
    for dir in &plan.lane_dirs {
        fsync_dir(dir)?;
    }
    let segments = plan.data_dir.join("segments");
    if segments.is_dir() {
        fsync_dir(&segments)?;
    }
    if let Some(subscriptions) = &plan.subscriptions {
        if subscriptions.removed > 0 {
            let mut document = subscriptions.document.clone();
            document["subscriptions"] = serde_json::Value::Array(subscriptions.kept.clone());
            write_atomically(&subscriptions.state_path, &document)?;
        }
    }
    Ok(())
}

/// Replace `path` the way the server persists subscription state: an
/// owner-only temp file, fsynced, renamed over the original, directory fsynced.
/// The prune image runs as root, so the replacement takes the original's owner
/// to stay readable by the server's own user.
fn write_atomically(path: &Path, value: &serde_json::Value) -> io::Result<()> {
    let dir = path.parent().expect("state file has a parent");
    let tmp = dir.join(".state.json.prune.tmp");
    let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp)?;
    file.write_all(&bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let original = std::fs::metadata(path)?;
        std::os::unix::fs::fchown(&file, Some(original.uid()), Some(original.gid()))?;
    }
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, path)?;
    fsync_dir(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOURCE: &str = "source-0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn patterns_match_whole_segments() {
        let cases = [
            ("/circuits/v1/prod/stores/g/x", Some(0)),
            ("/agent-runs/v1/prod/run-1", Some(1)),
            ("/mighty-next/v1/prod/threads/a", Some(2)),
            (&*format!("/circuits/v1/{SOURCE}/stores/g"), Some(3)),
            ("/circuits/v1/production/x", None),
            ("/circuits/v1/prod", None),
            ("/circuits/v1/prod/", None),
            ("/circuits/v1/dev/x", None),
            ("/circuits/v2/prod/x", None),
            ("/mighty-next/v1/prodx/a", None),
            ("circuits/v1/prod/x", None),
            ("/circuits/v1/source-0123/x", None),
            (
                "/circuits/v1/source-0123456789ABCDEF0123456789abcdef01234567/x",
                None,
            ),
            (
                "/circuits/v1/source-0123456789abcdef0123456789abcdef012345678/x",
                None,
            ),
            ("/other/x", None),
        ];
        for (path, expected) in cases {
            assert_eq!(classify(path, false), expected, "{path}");
        }
        assert_eq!(classify("/mighty-next/v1/prod", true), Some(2));
        assert_eq!(classify(&format!("/circuits/v1/{SOURCE}"), true), Some(3));
        assert_eq!(classify("", true), None);
        assert_eq!(classify("/circuits/v1", true), None);
    }

    fn sidecar(dir: &Path, name: &str, id: u64, path: &str, forked_from: Option<&str>) {
        let meta = serde_json::json!({
            "id": id, "path": path, "content_type": "application/octet-stream",
            "ttl_seconds": null, "expires_at_unix": null, "expires_at_raw": null,
            "create_closed": false, "forked_from": forked_from, "fork_offset_raw": null,
            "fork_sub_offset": null, "base_offset": 0, "closed": false, "closed_by": null,
            "producers": {}, "last_seq_header": null, "last_access_unix": 0,
            "ref_count": 0, "soft_deleted": false,
        });
        let data = dir.join("streams").join(format!("{name}~{id}"));
        std::fs::write(&data, b"hello").unwrap();
        std::fs::write(meta_path(&data), serde_json::to_vec(&meta).unwrap()).unwrap();
    }

    fn meta_path(data: &Path) -> PathBuf {
        crate::store::meta_path(data)
    }

    fn store(streams: &[(&str, &str, Option<&str>)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("streams")).unwrap();
        std::fs::write(dir.path().join("streams/.lanes"), "1\n").unwrap();
        std::fs::write(dir.path().join("streams/.lane"), "0").unwrap();
        for (id, (name, path, forked_from)) in streams.iter().enumerate() {
            sidecar(dir.path(), name, id as u64 + 1, path, *forked_from);
        }
        dir
    }

    #[test]
    fn plans_by_recorded_path_and_counts_per_pattern() {
        let source_path = format!("/circuits/v1/{SOURCE}/s");
        let dir = store(&[
            // File names deliberately disagree with the recorded paths.
            ("a", "/mighty-next/v1/prod/a", None),
            ("b", &source_path, None),
            ("c", "/circuits/v1/prod/c", None),
            ("d", "/agent-runs/v1/prod/d", None),
            ("e", "/agent-runs/v1/prod/e", None),
        ]);
        std::fs::write(dir.path().join("streams/orphan~99"), b"xyz").unwrap();
        let plan = plan(dir.path(), Tenant::Mighty).unwrap();
        assert!(plan.problems.is_empty(), "{:?}", plan.problems);
        let streams: Vec<u64> = plan.per_pattern.iter().map(|c| c.streams).collect();
        assert_eq!(streams, [1, 2, 1, 1]);
        assert_eq!(plan.remove.len(), 3);
        assert_eq!(plan.orphan_data_files, 1);
        assert_eq!(plan.orphan_data_bytes, 3);
        let sidecar_len = std::fs::metadata(dir.path().join("streams/a~1.meta"))
            .unwrap()
            .len();
        assert_eq!(plan.per_pattern[2].bytes, sidecar_len + 5);
    }

    #[test]
    fn unknown_streams_and_forks_from_removed_streams_are_problems() {
        let dir = store(&[
            ("a", "/mighty-next/v1/prod/a", Some("/circuits/v1/prod/c")),
            ("c", "/circuits/v1/prod/c", None),
            ("x", "/other/x", None),
        ]);
        let plan = plan(dir.path(), Tenant::Mighty).unwrap();
        assert_eq!(plan.problems.len(), 2, "{:?}", plan.problems);
        assert!(plan.problems.iter().any(|p| p.contains("\"/other/x\"")));
        assert!(plan
            .problems
            .iter()
            .any(|p| p.contains("forked from \"/circuits/v1/prod/c\"")));
        // Forks within the kept tenant, and removed forks of kept parents, are fine.
        let dir = store(&[
            ("a", "/mighty-next/v1/prod/a", None),
            (
                "b",
                "/mighty-next/v1/prod/b",
                Some("/mighty-next/v1/prod/a"),
            ),
            ("c", "/circuits/v1/prod/c", Some("/mighty-next/v1/prod/a")),
        ]);
        assert!(plan_problems(dir.path(), Tenant::Mighty).is_empty());
    }

    fn plan_problems(dir: &Path, keep: Tenant) -> Vec<String> {
        plan(dir, keep).unwrap().problems
    }

    #[test]
    fn quarantined_and_unparsable_sidecars_are_problems() {
        let dir = store(&[("a", "/mighty-next/v1/prod/a", None)]);
        std::fs::write(dir.path().join("streams/b~7"), b"data").unwrap();
        std::fs::write(dir.path().join("streams/b~7.meta.corrupt"), b"torn").unwrap();
        std::fs::write(dir.path().join("streams/c~8"), b"data").unwrap();
        std::fs::write(dir.path().join("streams/c~8.meta"), b"{torn").unwrap();
        let problems = plan_problems(dir.path(), Tenant::Mighty);
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(problems.iter().any(|p| p.contains("quarantined sidecar")));
        assert!(problems.iter().any(|p| p.contains("unparsable sidecar")));
    }

    #[test]
    fn execute_removes_stream_files_but_never_markers_or_shared_temps() {
        let dir = store(&[
            ("a.x", "/mighty-next/v1/prod/a", None),
            ("a.y", "/circuits/v1/prod/c", None),
            ("d", "/agent-runs/v1/prod/d", None),
        ]);
        let streams = dir.path().join("streams");
        // Both `a.x~1` and `a.y~2` name their compaction temp `a.compact.tmp`.
        std::fs::write(streams.join("a.compact.tmp"), b"residual").unwrap();
        std::fs::write(streams.join("d~3.compact.tmp"), b"residual").unwrap();
        let plan = plan(dir.path(), Tenant::Mighty).unwrap();
        assert!(plan.problems.is_empty(), "{:?}", plan.problems);
        execute(&plan).unwrap();
        let mut left: Vec<String> = std::fs::read_dir(&streams)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        left.sort();
        assert_eq!(
            left,
            [".lane", ".lanes", "a.compact.tmp", "a.x~1", "a.x~1.meta"]
        );
    }

    #[test]
    fn subscriptions_follow_their_stream_root() {
        let dir = store(&[("a", "/mighty-next/v1/prod/a", None)]);
        let state = dir.path().join("subscriptions/state.json");
        std::fs::create_dir_all(state.parent().unwrap()).unwrap();
        let document = serde_json::json!({"version": 1, "subscriptions": [
            {"id": "m", "stream_root": "/mighty-next/v1/prod", "extra": [1, 2]},
            {"id": "i", "stream_root": "/circuits/v1/prod/stores/g"},
        ]});
        std::fs::write(&state, serde_json::to_vec(&document).unwrap()).unwrap();
        let plan = plan(dir.path(), Tenant::Mighty).unwrap();
        assert!(plan.problems.is_empty(), "{:?}", plan.problems);
        assert_eq!(plan.per_pattern[0].subscriptions, 1);
        assert_eq!(plan.per_pattern[2].subscriptions, 1);
        execute(&plan).unwrap();
        let after: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&state).unwrap()).unwrap();
        assert_eq!(
            after,
            serde_json::json!({"version": 1, "subscriptions": [
                {"id": "m", "stream_root": "/mighty-next/v1/prod", "extra": [1, 2]},
            ]})
        );

        std::fs::write(
            &state,
            br#"{"version":1,"subscriptions":[{"id":"r","stream_root":""}]}"#,
        )
        .unwrap();
        let problems = plan_problems(dir.path(), Tenant::Mighty);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("subscription \"r\""));
    }

    #[test]
    fn reads_every_lane_of_a_laned_store() {
        let dir = tempfile::tempdir().unwrap();
        let streams = dir.path().join("streams");
        std::fs::create_dir_all(streams.join("0")).unwrap();
        std::fs::create_dir_all(streams.join("1")).unwrap();
        std::fs::write(streams.join(".lanes"), "2\n").unwrap();
        for (lane, path) in [(0, "/mighty-next/v1/prod/a"), (1, "/circuits/v1/prod/c")] {
            let lane_dir = streams.join(lane.to_string());
            std::fs::write(lane_dir.join(".lane"), lane.to_string()).unwrap();
            let data = lane_dir.join(format!("s~{lane}"));
            std::fs::write(&data, b"x").unwrap();
            let meta = serde_json::json!({
                "id": lane, "path": path, "content_type": "application/octet-stream",
                "ttl_seconds": null, "expires_at_unix": null, "expires_at_raw": null,
                "create_closed": false, "forked_from": null, "fork_offset_raw": null,
                "fork_sub_offset": null, "base_offset": 0, "closed": false, "closed_by": null,
                "producers": {}, "last_seq_header": null, "last_access_unix": 0,
                "ref_count": 0, "soft_deleted": false,
            });
            std::fs::write(meta_path(&data), serde_json::to_vec(&meta).unwrap()).unwrap();
        }
        let plan = plan(dir.path(), Tenant::Mighty).unwrap();
        assert!(plan.problems.is_empty(), "{:?}", plan.problems);
        execute(&plan).unwrap();
        assert!(streams.join("0/s~0.meta").exists());
        assert!(!streams.join("1/s~1.meta").exists());
        assert!(!streams.join("1/s~1").exists());
        assert!(streams.join("1/.lane").exists());
    }
}
