//! Object access for the web API when a repository's packs are **not** on
//! this instance (`ObjectAccess::Remote`): read what a request needs from the
//! WAL pack set by range (`walgit_wal::RemotePacks`), fault those objects into
//! the local loose store, and let the unmodified `git` plumbing the renderers
//! already use run against them. History questions (`git log`) cannot be
//! answered by faulting, so the commit walks live here, over the same reader.
//!
//! Everything reports what it is doing through a [`Reporter`] so the SSE
//! envelope can narrate ("Reading tree areas/core…", "Walked 300 commits…").

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use gix_hash::ObjectId;
use gix_object::Kind;
use walgit_wal::remote::Obj;
use walgit_wal::{RemotePacks, Reporter};

use crate::error::ApiError;

/// Cap on objects faulted for one commit or compare diff (pathological diffs
/// get a clear error instead of a multi-minute range-read storm).
pub(crate) const MAX_DIFF_OBJECTS: usize = 20_000;
/// Budget for "newest commit touching path" (tree header).
const NEWEST_BUDGET: usize = 3_000;
/// Budget for a history page walk (skip + n commits, plus skipped TREESAME ones).
const WALK_BUDGET: usize = 50_000;
/// Budget for one bilateral compare walk (`compare/{b}...{h}`): merge base +
/// ahead/behind counts. Past it the counts would be guesses, so the endpoint
/// answers the truthful 503 instead (principle V).
const COMPARE_WALK_BUDGET: usize = 50_000;

pub struct Remote {
    pub packs: Arc<RemotePacks>,
    pub local: walgit_git::LocalRepo,
    pub reporter: Reporter,
    faulted: parking_lot::Mutex<HashSet<ObjectId>>,
}

fn not_found(m: impl Into<String>) -> ApiError {
    ApiError::NotFound(m.into())
}
fn wal(e: walgit_wal::WalError) -> ApiError {
    ApiError::Internal(format!("remote objects: {e}"))
}

/// A parsed commit (what the walks and renderers need).
#[derive(Clone)]
pub struct CommitMeta {
    pub id: ObjectId,
    pub tree: ObjectId,
    pub parents: Vec<ObjectId>,
    pub author: String,
    pub author_email: String,
    pub author_date: String,
    pub committer: String,
    pub commit_date: String,
    pub commit_time: i64,
    pub subject: String,
    pub body: String,
}

pub struct TreeEntryRef {
    pub name: Vec<u8>,
    pub oid: ObjectId,
    pub mode: gix_object::tree::EntryMode,
}

impl Remote {
    pub fn new(packs: Arc<RemotePacks>, local: walgit_git::LocalRepo, reporter: Reporter) -> Self {
        Remote {
            packs,
            local,
            reporter,
            faulted: parking_lot::Mutex::new(HashSet::new()),
        }
    }

    pub fn hash(&self) -> gix_hash::Kind {
        self.packs.hash()
    }

    /// Read an object (404 when absent).
    pub async fn get(&self, oid: &gix_hash::oid) -> Result<Arc<Obj>, ApiError> {
        self.packs
            .find(oid)
            .await
            .map_err(wal)?
            .ok_or_else(|| not_found(format!("object {oid} not in the pack set")))
    }

    /// Read + write into the local loose store (so git can see it).
    pub async fn fault(&self, oid: &gix_hash::oid) -> Result<Arc<Obj>, ApiError> {
        let o = self.get(oid).await?;
        self.write_local(vec![(oid.to_owned(), o.clone())]).await?;
        Ok(o)
    }

    /// Fault a batch concurrently (bounded), skipping what is already local.
    pub async fn fault_many(&self, oids: &[ObjectId]) -> Result<(), ApiError> {
        const PAR: usize = 32;
        let todo: Vec<ObjectId> = {
            let done = self.faulted.lock();
            oids.iter()
                .filter(|o| !done.contains(*o))
                .copied()
                .collect()
        };
        for chunk in todo.chunks(PAR) {
            let results = futures::future::join_all(chunk.iter().map(|o| self.get(o))).await;
            let mut batch = Vec::with_capacity(chunk.len());
            for (oid, r) in chunk.iter().zip(results) {
                batch.push((*oid, r?));
            }
            self.write_local(batch).await?;
        }
        Ok(())
    }

    /// Write freshly read objects into the local loose store, skipping what is
    /// already faulted. Deflating an object and creating and renaming its file
    /// is blocking filesystem work, so a whole batch goes to one
    /// `spawn_blocking` rather than running on the tokio worker that read it
    /// (principle VI: never block the async runtime).
    async fn write_local(&self, batch: Vec<(ObjectId, Arc<Obj>)>) -> Result<(), ApiError> {
        let todo: Vec<(ObjectId, Arc<Obj>)> = {
            let done = self.faulted.lock();
            batch
                .into_iter()
                .filter(|(oid, _)| !done.contains(oid))
                .collect()
        };
        if todo.is_empty() {
            return Ok(());
        }
        let local = self.local.clone();
        let (written, outcome) = tokio::task::spawn_blocking(move || {
            let mut written: Vec<ObjectId> = Vec::with_capacity(todo.len());
            for (oid, o) in todo {
                if let Err(e) = local.write_loose_object(o.kind, &oid, &o.data) {
                    let msg = format!("fault object {oid}: {e}");
                    return (written, Err(ApiError::Internal(msg)));
                }
                written.push(oid);
            }
            (written, Ok(()))
        })
        .await
        .map_err(|e| ApiError::Internal(format!("fault write task: {e}")))?;
        if !written.is_empty() {
            self.faulted.lock().extend(written);
        }
        outcome
    }

    pub async fn kind_and_size(
        &self,
        oid: &gix_hash::oid,
    ) -> Result<Option<(Kind, u64)>, ApiError> {
        self.packs.header(oid).await.map_err(wal)
    }

    /// `rev-parse --verify <rev>^{commit}` without objects on disk: full or
    /// abbreviated sha (unique prefix), tags peeled.
    pub async fn resolve_commitish(&self, rev: &str) -> Result<ObjectId, ApiError> {
        let hex = rev.trim();
        let looks_hex = hex.len() >= 4
            && hex.len() <= self.hash().len_in_hex()
            && hex.bytes().all(|b| b.is_ascii_hexdigit());
        if !looks_hex {
            return Err(not_found(format!("unknown revision {rev}")));
        }
        let oid = if hex.len() == self.hash().len_in_hex() {
            let oid = ObjectId::from_hex(hex.as_bytes())
                .map_err(|_| not_found(format!("unknown revision {rev}")))?;
            if !self.packs.contains(&oid) {
                return Err(not_found(format!("unknown revision {rev}")));
            }
            oid
        } else {
            let prefix = gix_hash::Prefix::from_hex(hex)
                .map_err(|_| not_found(format!("unknown revision {rev}")))?;
            match self.packs.lookup_prefix(prefix) {
                Ok(Some(oid)) => oid,
                Ok(None) => return Err(not_found(format!("unknown revision {rev}"))),
                Err(_) => return Err(not_found(format!("ambiguous revision {rev}"))),
            }
        };
        self.peel_to_commit(oid).await
    }

    async fn peel_to_commit(&self, mut oid: ObjectId) -> Result<ObjectId, ApiError> {
        for _ in 0..16 {
            let (kind, _) = self
                .kind_and_size(&oid)
                .await?
                .ok_or_else(|| not_found(format!("object {oid}")))?;
            match kind {
                Kind::Commit => return Ok(oid),
                Kind::Tag => {
                    let o = self.get(&oid).await?;
                    let tag = gix_object::TagRef::from_bytes(&o.data, self.hash())
                        .map_err(|e| ApiError::Internal(format!("parse tag {oid}: {e}")))?;
                    oid = tag.target();
                }
                _ => return Err(not_found(format!("{oid} is not a commit"))),
            }
        }
        Err(not_found("tag chain too deep"))
    }

    pub async fn commit(&self, oid: &gix_hash::oid) -> Result<CommitMeta, ApiError> {
        let o = self.get(oid).await?;
        if o.kind != Kind::Commit {
            return Err(not_found(format!("{oid} is not a commit")));
        }
        parse_commit(oid.to_owned(), &o.data, self.hash())
    }

    pub async fn tree_entries(&self, oid: &gix_hash::oid) -> Result<Vec<TreeEntryRef>, ApiError> {
        let o = self.get(oid).await?;
        if o.kind != Kind::Tree {
            return Err(not_found(format!("{oid} is not a tree")));
        }
        parse_tree(&o.data, self.hash())
    }

    /// Walk `path` from `tree`; returns the entry (oid, mode) at the end, None if absent.
    pub async fn lookup_path(
        &self,
        tree: ObjectId,
        path: &str,
    ) -> Result<Option<(ObjectId, gix_object::tree::EntryMode)>, ApiError> {
        let mut cur = tree;
        let mut mode: Option<gix_object::tree::EntryMode> = None;
        for seg in path.split('/').filter(|s| !s.is_empty()) {
            let entries = self.tree_entries(&cur).await?;
            let Some(e) = entries.into_iter().find(|e| e.name == seg.as_bytes()) else {
                return Ok(None);
            };
            cur = e.oid;
            mode = Some(e.mode);
            if !e.mode.is_tree() {
                // more segments after a blob => absent
                continue;
            }
        }
        match mode {
            None => Ok(Some((cur, gix_object::tree::EntryKind::Tree.into()))),
            Some(m) => Ok(Some((cur, m))),
        }
    }

    /// Fault the commit, the trees along `path` and the target (tree or blob).
    /// Returns the target entry. 404 if the path does not exist.
    pub async fn fault_path(
        &self,
        commit: &gix_hash::oid,
        path: &str,
    ) -> Result<(CommitMeta, ObjectId, gix_object::tree::EntryMode), ApiError> {
        let c = self.commit(commit).await?;
        self.fault(commit).await?;
        let mut cur = c.tree;
        let mut mode: gix_object::tree::EntryMode = gix_object::tree::EntryKind::Tree.into();
        self.fault(&cur).await?;
        let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        for (i, seg) in segs.iter().enumerate() {
            let entries = self.tree_entries(&cur).await?;
            let Some(e) = entries.into_iter().find(|e| e.name == seg.as_bytes()) else {
                return Err(not_found(format!(
                    "path '{path}' does not exist in {commit}"
                )));
            };
            cur = e.oid;
            mode = e.mode;
            if e.mode.is_tree() {
                self.fault(&cur).await?;
            } else if i + 1 < segs.len() {
                return Err(not_found(format!(
                    "path '{path}' does not exist in {commit}"
                )));
            } else if e.mode.is_blob() {
                // blob: caller decides whether to fault (size check)
            }
        }
        Ok((c, cur, mode))
    }

    /// Oid of `path` in the commit's tree (None if absent). Cached per tree.
    async fn path_oid(
        &self,
        cache: &mut HashMap<ObjectId, Option<ObjectId>>,
        tree: ObjectId,
        path: &str,
    ) -> Result<Option<ObjectId>, ApiError> {
        if let Some(v) = cache.get(&tree) {
            return Ok(*v);
        }
        let v = self.lookup_path(tree, path).await?.map(|(o, _)| o);
        cache.insert(tree, v);
        Ok(v)
    }

    /// History walk in committer-date order (git log's default for non-topo),
    /// with git's default history simplification for `path` (a commit TREESAME
    /// to a parent is dropped and only that parent is followed). Returns up to
    /// `want` shown commits.
    pub async fn walk(
        &self,
        start: ObjectId,
        path: Option<&str>,
        want: usize,
        label: &str,
    ) -> Result<Vec<CommitMeta>, ApiError> {
        self.walk_bounded(
            start,
            path.filter(|p| !p.is_empty()),
            want,
            label,
            WALK_BUDGET,
        )
        .await
    }

    /// `git log -1 <sha> -- <path>` with a budget; None when not found in time.
    pub async fn newest_touching(
        &self,
        start: ObjectId,
        path: &str,
    ) -> Result<Option<CommitMeta>, ApiError> {
        if path.is_empty() {
            return Ok(Some(self.commit(&start).await?));
        }
        // Bounded: reuse walk with a small budget via a local loop.
        let label = format!("Finding the latest commit touching {path}");
        let mut res = self
            .walk_bounded(start, Some(path), 1, &label, NEWEST_BUDGET)
            .await?;
        Ok(res.pop())
    }

    async fn walk_bounded(
        &self,
        start: ObjectId,
        path: Option<&str>,
        want: usize,
        label: &str,
        budget: usize,
    ) -> Result<Vec<CommitMeta>, ApiError> {
        #[derive(PartialEq, Eq)]
        struct Item(i64, u64, ObjectId);
        impl Ord for Item {
            fn cmp(&self, o: &Self) -> std::cmp::Ordering {
                self.0.cmp(&o.0).then_with(|| o.1.cmp(&self.1))
            }
        }
        impl PartialOrd for Item {
            fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(o))
            }
        }
        let mut heap = BinaryHeap::new();
        let mut seen: HashSet<ObjectId> = HashSet::new();
        let mut metas: HashMap<ObjectId, CommitMeta> = HashMap::new();
        let mut path_cache: HashMap<ObjectId, Option<ObjectId>> = HashMap::new();
        let mut seq = 0u64;
        let start_meta = self.commit(&start).await?;
        heap.push(Item(start_meta.commit_time, seq, start));
        metas.insert(start, start_meta);
        seen.insert(start);
        let mut out = Vec::new();
        let mut popped = 0usize;
        while let Some(Item(_, _, oid)) = heap.pop() {
            popped += 1;
            if popped > budget {
                self.reporter
                    .notice(format!("{label}: gave up after {budget} commits"));
                break;
            }
            if popped.is_multiple_of(100) {
                self.reporter
                    .bar(label.to_string(), popped as u64, None, "commits");
            }
            let meta = match metas.remove(&oid) {
                Some(m) => m,
                None => self.commit(&oid).await?,
            };
            let mut follow: Vec<ObjectId> = meta.parents.clone();
            let mut show = true;
            if let Some(p) = path {
                let mine = self.path_oid(&mut path_cache, meta.tree, p).await?;
                if meta.parents.is_empty() {
                    show = mine.is_some();
                } else {
                    let mut treesame_parent = None;
                    for par in &meta.parents {
                        let pm = if let Some(m) = metas.get(par) {
                            m.clone()
                        } else {
                            let m = self.commit(par).await?;
                            metas.insert(*par, m.clone());
                            m
                        };
                        let theirs = self.path_oid(&mut path_cache, pm.tree, p).await?;
                        if theirs == mine {
                            treesame_parent = Some(*par);
                            break;
                        }
                    }
                    if let Some(tp) = treesame_parent {
                        show = false;
                        follow = vec![tp];
                    }
                }
            }
            if show {
                out.push(meta.clone());
                if out.len() >= want {
                    break;
                }
            }
            for par in follow {
                if seen.insert(par) {
                    seq += 1;
                    let pm = if let Some(m) = metas.get(&par) {
                        m.clone()
                    } else {
                        let m = self.commit(&par).await?;
                        metas.insert(par, m.clone());
                        m
                    };
                    heap.push(Item(pm.commit_time, seq, par));
                }
            }
        }
        Ok(out)
    }

    /// Fault everything `git show -M --first-parent` needs for `commit`: the
    /// commit, its first parent, every tree on a differing path, and the
    /// blobs of changed entries (both sides). Root commits diff against the
    /// empty tree.
    pub async fn fault_commit_diff(&self, commit: &gix_hash::oid) -> Result<CommitMeta, ApiError> {
        let c = self.commit(commit).await?;
        self.fault(commit).await?;
        // The renderer diffs against the first parent only
        // (`--diff-merges=first-parent`); git still parses every parent and
        // peeks at their root trees, so fault those (one object each) but walk
        // the diff for the first parent alone — a merge into a monorepo trunk
        // otherwise pulls the whole other-branch delta (20 k+ objects, 503).
        let first_parent_tree = match c.parents.first() {
            Some(p) => {
                let pm = self.commit(p).await?;
                self.fault(p).await?;
                for other in &c.parents[1..] {
                    let om = self.commit(other).await?;
                    self.fault(other).await?;
                    self.fault(&om.tree).await?;
                }
                Some(pm.tree)
            }
            None => None,
        };
        self.fault_tree_diff(first_parent_tree, Some(c.tree))
            .await?;
        Ok(c)
    }

    /// Bilateral paint walk for `compare/{base}...{head}`: git's
    /// paint-down-to-common simplified. Each side is seeded with one flag bit
    /// (set when a queue entry is pushed, so one oid appears at most twice);
    /// pops by `(commit_time desc, hop asc, seq desc)` (see [`Item`]). A pop
    /// whose oid carries both
    /// flags is common — the first such pop is the merge base, its parents are
    /// marked common (every ancestor of a common commit is common) and a
    /// one-sided count already taken for it is retracted. A pop with one flag
    /// counts on that side (`behind` for base, `ahead` for head, which also
    /// collects every head-side meta in pop order — retractions can remove an
    /// already-collected entry, so only the caller's window bounds the page).
    ///
    /// The walk stops at the common frontier, git's `queue_has_nonstale`: once
    /// every queued entry carries both flags, no further pop could count, so
    /// the cost is O(ahead + behind), never O(shared ancestry) — a compare of
    /// adjacent commits on a monorepo trunk walks two commits, not the trunk.
    /// The drain below the frontier still retracts one-sided counts taken
    /// above it (clock skew: a common commit can pop — and count — one-sided
    /// before the frontier is found) and records the merge base, all in
    /// memory; a counted commit whose commonality is only discoverable by
    /// walking commits that were never queued stays counted, the same skew
    /// caveat class git's own merge-base documents. `None` merge base =
    /// unrelated histories. One pop past [`COMPARE_WALK_BUDGET`] ends with the
    /// truthful 503: partial counts would be guesses.
    pub async fn compare_walk(
        &self,
        base: ObjectId,
        head: ObjectId,
        label: &str,
    ) -> Result<CompareWalk, ApiError> {
        const BASE: u8 = 1;
        const HEAD: u8 = 2;
        /// Pop order: `(commit_time desc, hop asc, seq desc)`. `hop` is the
        /// edge distance from the seed that reached the commit, so within one
        /// timestamp (same-second commits — scripted pushes, imports) a
        /// parent can never pop before its child: the order stays
        /// topological where dates tie, which is what lets the frontier close
        /// instead of one side draining below the merge base. git gets the
        /// same property from commit-graph generation numbers; `seq` breaks
        /// the remaining ties newest-push-first so a re-pushed common entry
        /// outranks the descent it has to catch.
        #[derive(PartialEq, Eq)]
        struct Item(i64, usize, u64, ObjectId);
        impl Ord for Item {
            fn cmp(&self, o: &Self) -> std::cmp::Ordering {
                self.0
                    .cmp(&o.0)
                    .then_with(|| o.1.cmp(&self.1))
                    .then_with(|| self.2.cmp(&o.2))
            }
        }
        impl PartialOrd for Item {
            fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(o))
            }
        }
        /// The queue plus its staleness bookkeeping. `live` counts entries per
        /// oid; `nonstale` counts the live entries whose oid does not (yet)
        /// carry both flags. Flags only ever gain bits, so an entry can turn
        /// stale once, and when an oid's flags become both, every live entry
        /// of it was pushed one-sided — `nonstale == 0` is exactly "all queued
        /// entries are common".
        #[derive(Default)]
        struct Queue {
            heap: BinaryHeap<Item>,
            live: HashMap<ObjectId, usize>,
            flags: HashMap<ObjectId, u8>,
            nonstale: usize,
            seq: u64,
        }
        impl Queue {
            fn push(&mut self, time: i64, hop: usize, oid: ObjectId) {
                *self.live.entry(oid).or_insert(0) += 1;
                if self.flags[&oid] != BASE | HEAD {
                    self.nonstale += 1;
                }
                self.heap.push(Item(time, hop, self.seq, oid));
                self.seq += 1;
            }
            /// Give `oid` `bit` when new; true when the bit was added.
            fn flag(&mut self, oid: ObjectId, bit: u8) -> bool {
                let f = self.flags.entry(oid).or_insert(0);
                if *f & bit != 0 {
                    return false;
                }
                *f |= bit;
                if *f == BASE | HEAD && let Some(n) = self.live.get(&oid) {
                    self.nonstale -= *n;
                }
                true
            }
            /// Mark `oid` common outright; true when the flags changed.
            fn mark_common(&mut self, oid: ObjectId) -> bool {
                if self.flags.get(&oid).copied() == Some(BASE | HEAD) {
                    return false;
                }
                self.flags.insert(oid, BASE | HEAD);
                if let Some(n) = self.live.get(&oid) {
                    self.nonstale -= *n;
                }
                true
            }
            fn pop(&mut self) -> Option<(ObjectId, usize)> {
                let Item(_, hop, _, oid) = self.heap.pop()?;
                if let Some(n) = self.live.get_mut(&oid) {
                    *n -= 1;
                }
                if self.flags[&oid] != BASE | HEAD {
                    self.nonstale -= 1;
                }
                Some((oid, hop))
            }
        }
        // Push a queue entry for `oid` carrying `bit` when that bit is new;
        // the meta (for `commit_time`) is read through the reader if unknown.
        async fn enlist(
            remote: &Remote,
            q: &mut Queue,
            metas: &mut HashMap<ObjectId, CommitMeta>,
            oid: ObjectId,
            hop: usize,
            bit: u8,
        ) -> Result<(), ApiError> {
            if !q.flag(oid, bit) {
                return Ok(());
            }
            let time = if let Some(m) = metas.get(&oid) {
                m.commit_time
            } else {
                let m = remote.commit(&oid).await?;
                metas.insert(oid, m.clone());
                m.commit_time
            };
            q.push(time, hop, oid);
            Ok(())
        }
        // Retract a one-sided count that later proved common.
        fn retract(
            counted: &mut HashMap<ObjectId, u8>,
            ahead: &mut usize,
            behind: &mut usize,
            page: &mut Vec<CommitMeta>,
            oid: ObjectId,
        ) {
            match counted.remove(&oid) {
                Some(HEAD) => {
                    *ahead = ahead.saturating_sub(1);
                    page.retain(|m| m.id != oid);
                }
                Some(BASE) => *behind = behind.saturating_sub(1),
                _ => {}
            }
        }
        if base == head {
            return Ok(CompareWalk {
                merge_base: Some(base),
                ahead: 0,
                behind: 0,
                page: Vec::new(),
            });
        }
        let mut q = Queue::default();
        let mut metas: HashMap<ObjectId, CommitMeta> = HashMap::new();
        // Parents of every popped commit: the drain below the frontier walks
        // parent chains to retract skewed counts, and a popped commit's meta
        // is consumed at its pop.
        let mut parents: HashMap<ObjectId, Vec<ObjectId>> = HashMap::new();
        enlist(self, &mut q, &mut metas, base, 0, BASE).await?;
        enlist(self, &mut q, &mut metas, head, 0, HEAD).await?;
        let mut merge_base: Option<ObjectId> = None;
        let mut ahead = 0usize;
        let mut behind = 0usize;
        let mut page: Vec<CommitMeta> = Vec::new();
        let mut counted: HashMap<ObjectId, u8> = HashMap::new();
        let mut popped = 0usize;
        // Frontier loop: stops as soon as every queued entry is common.
        while q.nonstale > 0 && let Some((oid, hop)) = q.pop() {
            popped += 1;
            if popped > COMPARE_WALK_BUDGET {
                return Err(ApiError::ServiceUnavailable(format!(
                    "comparing these refs walks more than {COMPARE_WALK_BUDGET} commits \
                     (ahead + behind of the two sides); too large to answer from the \
                     remote pack set"
                )));
            }
            if popped.is_multiple_of(100) {
                self.reporter
                    .bar(label.to_string(), popped as u64, None, "commits");
            }
            if q.flags[&oid] == BASE | HEAD {
                if merge_base.is_none() {
                    merge_base = Some(oid);
                }
                retract(&mut counted, &mut ahead, &mut behind, &mut page, oid);
                // Ancestors of a common commit are common: mark the parents
                // (both flags) so they pop common too instead of counting.
                let meta = match metas.remove(&oid) {
                    Some(m) => m,
                    None => self.commit(&oid).await?,
                };
                parents.insert(oid, meta.parents.clone());
                for par in &meta.parents {
                    if q.mark_common(*par) {
                        let time = if let Some(m) = metas.get(par) {
                            m.commit_time
                        } else {
                            let m = self.commit(par).await?;
                            metas.insert(*par, m.clone());
                            m.commit_time
                        };
                        q.push(time, hop + 1, *par);
                    }
                }
                continue;
            }
            let side = q.flags[&oid];
            let meta = match metas.remove(&oid) {
                Some(m) => m,
                None => self.commit(&oid).await?,
            };
            parents.insert(oid, meta.parents.clone());
            if side == HEAD {
                counted.insert(oid, HEAD);
                ahead += 1;
                page.push(meta.clone());
            } else {
                counted.insert(oid, BASE);
                behind += 1;
            }
            for par in &meta.parents {
                enlist(self, &mut q, &mut metas, *par, hop + 1, side).await?;
            }
        }
        // Frontier drain, no reads: every queued entry is common. Pop them in
        // pop order so the merge-base pick is the one a full drain would make,
        // then walk the known parent chains retracting one-sided counts. Every
        // queued or popped commit's parents are already in hand (a push reads
        // the meta, a pop stores the parent list), and the descent stops at
        // commits that were never queued — a counted commit cannot hide there,
        // because counting takes a pop and a pop queues the parents.
        let mut stack: Vec<ObjectId> = Vec::new();
        while let Some(Item(_, _, _, oid)) = q.heap.pop() {
            if merge_base.is_none() && q.flags[&oid] == BASE | HEAD {
                merge_base = Some(oid);
            }
            stack.push(oid);
        }
        let mut seen: HashSet<ObjectId> = HashSet::new();
        while let Some(oid) = stack.pop() {
            if !seen.insert(oid) {
                continue;
            }
            retract(&mut counted, &mut ahead, &mut behind, &mut page, oid);
            let ps = match metas.get(&oid) {
                Some(m) => m.parents.clone(),
                None => match parents.get(&oid) {
                    Some(p) => p.clone(),
                    None => continue,
                },
            };
            stack.extend(ps);
        }
        Ok(CompareWalk {
            merge_base,
            ahead,
            behind,
            page,
        })
    }

    /// Fault every tree on a differing path between two trees and the blobs of
    /// the changed entries (both sides); `None` diffs against the empty tree
    /// (a root commit, a side that has no such path). Level-parallel: every
    /// tree pair of the current level is faulted in one concurrent batch
    /// (range reads ~50 ms each; serially a large repository commit took 300
    /// round trips = 15 s), then merge-walked; the blobs it references are
    /// faulted in a second batch. Rounds ≈ path depth. Bounded by
    /// [`MAX_DIFF_OBJECTS`] (the truthful 503 past it).
    pub async fn fault_tree_diff(
        &self,
        base_tree: Option<ObjectId>,
        head_tree: Option<ObjectId>,
    ) -> Result<(), ApiError> {
        if base_tree.is_none() && head_tree.is_none() {
            return Ok(());
        }
        self.reporter.notice("Reading the trees and blobs of the diff".to_string());
        let mut stack: Vec<(Option<ObjectId>, Option<ObjectId>)> = vec![(base_tree, head_tree)];
        let mut count = 0usize;
        while !stack.is_empty() {
            let level = std::mem::take(&mut stack);
            let mut want: Vec<ObjectId> = Vec::new();
            for (a, b) in &level {
                want.extend(a.iter().copied());
                want.extend(b.iter().copied());
            }
            want.sort_unstable();
            want.dedup();
            count += want.len();
            if count > MAX_DIFF_OBJECTS {
                return Err(ApiError::ServiceUnavailable(format!(
                    "this diff touches more than {MAX_DIFF_OBJECTS} objects; too large to render from the remote pack set"
                )));
            }
            self.fault_many(&want).await?;
            self.reporter
                .bar("Reading changed objects", count as u64, None, "objects");
            let mut blobs: Vec<ObjectId> = Vec::new();
            for (a, b) in level {
                let ea = match a {
                    Some(t) => self.tree_entries(&t).await?,
                    None => Vec::new(),
                };
                let eb = match b {
                    Some(t) => self.tree_entries(&t).await?,
                    None => Vec::new(),
                };
                // Merge-walk by git tree order.
                let (mut i, mut j) = (0, 0);
                while i < ea.len() || j < eb.len() {
                    let ord = match (ea.get(i), eb.get(j)) {
                        (Some(x), Some(y)) => {
                            tree_cmp(&x.name, x.mode.is_tree(), &y.name, y.mode.is_tree())
                        }
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => break,
                    };
                    match ord {
                        std::cmp::Ordering::Equal => {
                            let (x, y) = (&ea[i], &eb[j]);
                            i += 1;
                            j += 1;
                            if x.oid == y.oid && x.mode == y.mode {
                                continue;
                            }
                            match (x.mode.is_tree(), y.mode.is_tree()) {
                                (true, true) => stack.push((Some(x.oid), Some(y.oid))),
                                (true, false) => {
                                    stack.push((Some(x.oid), None));
                                    if y.mode.is_blob_or_symlink() {
                                        blobs.push(y.oid);
                                    }
                                }
                                (false, true) => {
                                    stack.push((None, Some(y.oid)));
                                    if x.mode.is_blob_or_symlink() {
                                        blobs.push(x.oid);
                                    }
                                }
                                (false, false) => {
                                    if x.mode.is_blob_or_symlink() {
                                        blobs.push(x.oid);
                                    }
                                    if y.mode.is_blob_or_symlink() && y.oid != x.oid {
                                        blobs.push(y.oid);
                                    }
                                }
                            }
                        }
                        std::cmp::Ordering::Less => {
                            let x = &ea[i];
                            i += 1;
                            if x.mode.is_tree() {
                                stack.push((Some(x.oid), None));
                            } else if x.mode.is_blob_or_symlink() {
                                blobs.push(x.oid);
                            }
                        }
                        std::cmp::Ordering::Greater => {
                            let y = &eb[j];
                            j += 1;
                            if y.mode.is_tree() {
                                stack.push((None, Some(y.oid)));
                            } else if y.mode.is_blob_or_symlink() {
                                blobs.push(y.oid);
                            }
                        }
                    }
                }
            }
            blobs.sort_unstable();
            blobs.dedup();
            count += blobs.len();
            if count > MAX_DIFF_OBJECTS {
                return Err(ApiError::ServiceUnavailable(format!(
                    "this diff touches more than {MAX_DIFF_OBJECTS} objects; too large to render from the remote pack set"
                )));
            }
            self.fault_many(&blobs).await?;
        }
        self.local
            .refresh_async()
            .await
            .map_err(|e| ApiError::Internal(e.to_string()))?;
        Ok(())
    }
}

/// The answer of one bilateral compare walk ([`Remote::compare_walk`]):
/// a merge base (`None` = unrelated histories), the exact ahead/behind
/// counts, and every head-side commit in pop order (newest first) — the
/// caller windows it with `skip`/`n`.
pub struct CompareWalk {
    pub merge_base: Option<ObjectId>,
    pub ahead: usize,
    pub behind: usize,
    pub page: Vec<CommitMeta>,
}

/// git's tree entry ordering: names compared as if trees had a trailing '/'.
fn tree_cmp(a: &[u8], a_tree: bool, b: &[u8], b_tree: bool) -> std::cmp::Ordering {
    let n = a.len().min(b.len());
    match a[..n].cmp(&b[..n]) {
        std::cmp::Ordering::Equal => {}
        o => return o,
    }
    let ca = a.get(n).copied().or(if a_tree { Some(b'/') } else { None });
    let cb = b.get(n).copied().or(if b_tree { Some(b'/') } else { None });
    ca.cmp(&cb)
}

pub fn parse_tree(data: &[u8], hash: gix_hash::Kind) -> Result<Vec<TreeEntryRef>, ApiError> {
    let mut out = Vec::new();
    for e in gix_object::TreeRefIter::from_bytes(data, hash) {
        let e = e.map_err(|e| ApiError::Internal(format!("parse tree: {e}")))?;
        out.push(TreeEntryRef {
            name: e.filename.to_vec(),
            oid: e.oid.to_owned(),
            mode: e.mode,
        });
    }
    Ok(out)
}

fn fmt_time(sig: &gix_actor::SignatureRef<'_>) -> (String, i64) {
    match sig.time() {
        Ok(t) => (
            t.format_or_unix(gix_date::time::format::ISO8601_STRICT),
            t.seconds,
        ),
        Err(_) => (String::new(), 0),
    }
}

pub fn parse_commit(
    id: ObjectId,
    data: &[u8],
    hash: gix_hash::Kind,
) -> Result<CommitMeta, ApiError> {
    let c = gix_object::CommitRef::from_bytes(data, hash)
        .map_err(|e| ApiError::Internal(format!("parse commit {id}: {e}")))?;
    let author = c
        .author()
        .map_err(|e| ApiError::Internal(format!("parse author {id}: {e}")))?;
    let committer = c
        .committer()
        .map_err(|e| ApiError::Internal(format!("parse committer {id}: {e}")))?;
    let (author_date, _) = fmt_time(&author);
    let (commit_date, commit_time) = fmt_time(&committer);
    let msg = c.message();
    let subject = msg.summary().to_string();
    let body = msg
        .body()
        .map(|b| b.to_string())
        .unwrap_or_default()
        .trim()
        .to_string();
    Ok(CommitMeta {
        id,
        tree: c.tree(),
        parents: c.parents().collect(),
        author: author.name.to_string(),
        author_email: author.email.to_string(),
        author_date,
        committer: committer.name.to_string(),
        commit_date,
        commit_time,
        subject,
        body,
    })
}
