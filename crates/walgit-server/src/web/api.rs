//! Read-only JSON API for the web UI (`web/API.md`, v2 contract).
//!
//! Two URL classes: ref-dependent (`refs`, `refs/{branches,tags}`,
//! `resolve`, name-addressed tree/blob/commits/commit) answered from a
//! per-manifest-version [`RefIndex`] with `stale-while-revalidate` + `ETag`,
//! and sha-addressed immutable ones (`tree/<sha>`, `blob/<sha>`,
//! `commits?ref=<sha>`, `commit/<sha>`) rendered once and cached in memory
//! (and, for remotely served repos, in the object store so every instance
//! shares one render cache).
//!
//! Object access (`Need::Objects`) goes through [`RepoHandle::sync_objects`]:
//! packs on disk when they fit this instance, otherwise the remote reader
//! (pack indexes local, data by range read; `web/objects.rs` faults what a
//! git command will touch into the loose store). Anything that cannot answer
//! immediately streams the SSE envelope (`crate::sse`) when the client
//! accepts it: notices + progress from the repo's task channel, then
//! `result`/`error`.

use std::future::Future;
use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderMap, header},
    response::{IntoResponse, Response},
    routing::get,
};
use futures::StreamExt;
use serde::Serialize;
use walgit_store::{GetOptions, ObjectStore, Prefixed, PutBody, PutMode};
use walgit_wal::{ObjectAccess, RepoHandle, Reporter};

use crate::sse::Rendered;
use crate::web::objects::{CommitMeta, Remote, MAX_DIFF_OBJECTS};
use crate::{AppState, auth::AuthError, cache::RefIndex, error::ApiError};

const MAX_BLOB: usize = 2 * 1024 * 1024;
/// Cap on one compare patch (`compare/{base}...{head}`): past it the patch is
/// cut at a whole-file boundary and the answer carries `patch_truncated`.
const MAX_PATCH: usize = 2 * 1024 * 1024;
/// Upper bound on `skip` for the paged endpoints (`commits`, `compare`).
/// Past it the page is certainly empty and the value only risks overflowing
/// the window arithmetic, so it is a clean 400 like a malformed range.
const MAX_SKIP: usize = 1_000_000_000;
const IMMUTABLE: &str = "private, max-age=31536000, immutable";
const SWR: &str = "private, max-age=0, stale-while-revalidate=60";
const DEFAULT_PAGE: usize = 100;
const MAX_PAGE: usize = 1000;
/// Store key prefix (inside the repo prefix) of the shared render cache.
const SHARED_CACHE_PREFIX: &str = "cache/api/v1/";

#[derive(Serialize, Clone)]
pub(crate) struct RefInfo {
    pub(crate) name: String,
    pub(crate) sha: String,
}
#[derive(Serialize)]
struct Refs {
    head: Option<RefInfo>,
}
#[derive(Serialize)]
struct RefPage {
    refs: Vec<RefInfo>,
    more: bool,
}
#[derive(Serialize, Clone)]
struct Resolved {
    #[serde(rename = "ref")]
    ref_name: String,
    sha: String,
    path: String,
    kind: &'static str,
}
#[derive(Serialize, Clone)]
struct Commit {
    sha: String,
    parents: Vec<String>,
    author: String,
    author_email: String,
    author_date: String,
    committer: String,
    commit_date: String,
    subject: String,
    /// The message body WITHOUT the trailer block (see `trailers`).
    body: String,
    /// Git trailers of the message (`Key: value` lines of the last paragraph,
    /// `git interpret-trailers --parse` rules), in order.
    trailers: Vec<super::trailers::Trailer>,
}
impl From<CommitMeta> for Commit {
    fn from(m: CommitMeta) -> Self {
        let (body, trailers) = super::trailers::split_trailers(&m.body);
        Commit {
            sha: m.id.to_string(),
            parents: m
                .parents
                .iter()
                .map(std::string::ToString::to_string)
                .collect(),
            author: m.author,
            author_email: m.author_email,
            author_date: m.author_date,
            committer: m.committer,
            commit_date: m.commit_date,
            subject: m.subject,
            body,
            trailers,
        }
    }
}
impl Commit {
    fn with_body(mut self, raw: &str) -> Self {
        let (body, trailers) = super::trailers::split_trailers(raw.trim());
        self.body = body;
        self.trailers = trailers;
        self
    }
}
#[derive(Serialize)]
struct Tree {
    #[serde(rename = "ref")]
    ref_name: String,
    sha: String,
    path: String,
    entries: Vec<TreeEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<Commit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    readme: Option<Readme>,
}
#[derive(Serialize)]
struct TreeEntry {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    mode: String,
    size: i64,
    sha: String,
}
#[derive(Serialize)]
struct Blob {
    #[serde(rename = "ref")]
    ref_name: String,
    sha: String,
    path: String,
    name: String,
    size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    contents: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    binary: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    too_large: Option<bool>,
}
#[derive(Serialize)]
struct Readme {
    name: String,
    contents: String,
}
#[derive(Serialize)]
struct Commits {
    #[serde(rename = "ref")]
    ref_name: String,
    sha: String,
    commits: Vec<Commit>,
    more: bool,
}
#[derive(Serialize)]
struct Stat {
    path: String,
    additions: i64,
    deletions: i64,
}
#[derive(Serialize)]
struct CommitDetail {
    commit: Commit,
    stats: Vec<Stat>,
    patch: String,
}
#[derive(serde::Deserialize, Default)]
struct CommitQuery {
    #[serde(rename = "ref")]
    ref_: Option<String>,
    path: Option<String>,
    skip: Option<usize>,
    n: Option<usize>,
}
#[derive(serde::Deserialize, Default)]
struct BlobQuery {
    raw: Option<String>,
}
#[derive(serde::Deserialize, Default)]
struct RefListQuery {
    prefix: Option<String>,
    q: Option<String>,
    after: Option<String>,
    n: Option<usize>,
}
#[derive(serde::Deserialize, Default)]
struct CompareQuery {
    skip: Option<usize>,
    n: Option<usize>,
}
#[derive(Serialize)]
struct CompareSide {
    #[serde(rename = "ref")]
    ref_name: String,
    sha: String,
}
#[derive(Serialize)]
struct Compare {
    base: CompareSide,
    head: CompareSide,
    merge_base: String,
    ahead: usize,
    behind: usize,
    /// One page of head-side commits, newest first (§4 `commits` shape).
    commits: Vec<Commit>,
    more: bool,
    /// Complete numstat of the merge-base→head diff (three-dot form).
    stats: Vec<Stat>,
    patch: String,
    patch_truncated: bool,
}

pub fn router(state: Arc<AppState>) -> Router {
    // D26/D27: repo-scoped endpoints live under the repository's own prefix,
    // `/{owner}/{repo}/api/…` (bearer/session lane) and `/{owner}/{repo}/api-browser/…`
    // (browser lane: another origin with `credentials: "include"`);
    // same handlers, the lane differs by credential handling only. Non-repo
    // endpoints keep `/services/api/owners*` and `/api/v1/*`.
    let mut r = Router::new()
        .route("/services/api/instance", get(instance_info))
        .route("/services/api/owners", get(owners))
        .route("/services/api/owners/{owner}", get(owner_repos));
    for base in REPO_API_BASES {
        r = r
            .route(&format!("{base}/refs"), get(refs))
            .route(&format!("{base}/refs/{{kind}}"), get(ref_list))
            .route(&format!("{base}/resolve"), get(resolve_root))
            .route(&format!("{base}/resolve/"), get(resolve_root))
            .route(&format!("{base}/resolve/{{*rest}}"), get(resolve))
            .route(&format!("{base}/tree/{{*rest}}"), get(tree))
            .route(&format!("{base}/blob/{{*rest}}"), get(blob))
            .route(&format!("{base}/commits"), get(commits))
            .route(&format!("{base}/commit/{{sha}}"), get(commit_detail))
            .route(&format!("{base}/compare"), get(compare_root))
            .route(&format!("{base}/compare/"), get(compare_root))
            .route(&format!("{base}/compare/{{*rest}}"), get(compare));
    }
    r.with_state(state)
}

/// Route prefixes of the repo-scoped JSON API (D27): one per lane, both
/// *after* the repository prefix. No lane-first forms, no aliases (banner).
pub const REPO_API_BASES: [&str; 2] = ["/{owner}/{repo}/api", "/{owner}/{repo}/api-browser"];

pub(crate) fn auth_err(e: AuthError) -> ApiError {
    match e {
        AuthError::Invalid | AuthError::Unauthorized => ApiError::Unauthorized,
        AuthError::Forbidden => ApiError::Forbidden,
        AuthError::UntrustedProxy => ApiError::UntrustedProxy,
        AuthError::Unavailable => ApiError::ServiceUnavailable("auth provider unavailable".into()),
    }
}
fn not_found(msg: impl Into<String>) -> ApiError {
    ApiError::NotFound(msg.into())
}
fn internal<E: std::fmt::Display>(e: E) -> ApiError {
    ApiError::Internal(e.to_string())
}

/// One synced view of a repository for the duration of a request.
pub struct Repo {
    id: String,
    local: walgit_git::LocalRepo,
    #[allow(dead_code)]
    version: String,
    pub(crate) index: Arc<RefIndex>,
    handle: Arc<RepoHandle>,
    access: ObjectAccess,
    /// Whether objects are readable (`Need::Objects` satisfied).
    objects: bool,
    reporter: Reporter,
    /// Shared render cache (object store) — set for remotely served repos.
    shared: Option<Prefixed>,
}
impl Repo {
    fn remote(&self) -> Option<Remote> {
        match &self.access {
            ObjectAccess::Remote(r) if self.objects => Some(Remote::new(
                r.clone(),
                self.local.clone(),
                self.reporter.clone(),
            )),
            _ => None,
        }
    }
    /// Upgrade a refs-level view to objects (used by `resolve` for raw revisions).
    async fn need_objects(&mut self, st: &AppState) -> Result<(), ApiError> {
        if self.objects {
            return Ok(());
        }
        let (guard, access) = self
            .handle
            .sync_objects()
            .await
            .map_err(crate::smart::wal_err)?;
        drop(guard);
        self.objects = true;
        self.access = access;
        self.shared = shared_cache(st, &self.handle, &self.access);
        Ok(())
    }
}

/// What a request needs from the local copy: refs only (cheap, always
/// possible) or objects too (packs on disk or the remote reader).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Need {
    Refs,
    Objects,
}

fn shared_cache(st: &AppState, handle: &RepoHandle, access: &ObjectAccess) -> Option<Prefixed> {
    (st.cfg.cache.shared_render_cache && access.is_remote()).then(|| handle.store().clone())
}

async fn open(
    st: &AppState,
    headers: &HeaderMap,
    owner: &str,
    name: &str,
) -> Result<Arc<RepoHandle>, ApiError> {
    st.auth.require_read(headers).await.map_err(auth_err)?;
    let id = walgit_git::RepoId::new(owner, name).map_err(|_| not_found("repository"))?;
    st.registry.open(&id).await.map_err(|e| match e {
        walgit_wal::WalError::NotFound => not_found("repository"),
        _ => internal(e),
    })
}

async fn view(
    st: &AppState,
    handle: Arc<RepoHandle>,
    need: Need,
    reporter: Reporter,
) -> Result<Repo, ApiError> {
    let (guard, access, objects) = match need {
        Need::Refs => (
            handle.sync_refs().await.map_err(crate::smart::wal_err)?,
            ObjectAccess::Local,
            false,
        ),
        Need::Objects => {
            let (g, a) = handle.sync_objects().await.map_err(crate::smart::wal_err)?;
            (g, a, true)
        }
    };
    // The guard is held until the local handle has been cloned and the ref
    // index for this manifest version exists. The local repository itself is
    // thread-safe and subsequent git commands read its synced state.
    let local = handle.local().clone();
    let version = handle
        .manifest_version()
        .map(|v| v.as_str().to_string())
        .unwrap_or_default();
    let id = handle.id().to_string();
    let index = st
        .caches
        .ref_index
        .get_or_build(&id, &version, || local.refs())
        .map_err(internal)?;
    drop(guard);
    let shared = shared_cache(st, &handle, &access);
    Ok(Repo {
        id,
        local,
        version,
        index,
        handle,
        access,
        objects,
        reporter,
        shared,
    })
}

fn shared_key(cache_key: &str) -> String {
    use sha1::Digest;
    let h = sha1::Sha1::digest(cache_key.as_bytes());
    format!("{SHARED_CACHE_PREFIX}{}.json", hex::encode(h))
}

/// Run one endpoint: auth + open, immutable caches, then either a plain
/// response or (when the answer needs long work and the client accepts it)
/// the SSE envelope streaming the repo's progress until the result.
pub(crate) async fn run<F, Fut>(
    st: &Arc<AppState>,
    headers: &HeaderMap,
    owner: &str,
    name: &str,
    need: Need,
    immutable_key: Option<String>,
    work: F,
) -> Result<Response, ApiError>
where
    F: FnOnce(Repo) -> Fut + Send + 'static,
    Fut: Future<Output = Result<Rendered, ApiError>> + Send + 'static,
{
    let handle = open(st, headers, owner, name).await?;
    let slow = need == Need::Objects && !handle.packs_ready();
    if let Some(key) = &immutable_key {
        if let Some(hit) = st.caches.api_immutable.get(key) {
            metrics::counter!("walgit_api_immutable_hit", "tier" => "memory").increment(1);
            return Ok(Rendered::json(hit, IMMUTABLE, None).into_response(headers));
        }
        if slow
            && st.cfg.cache.shared_render_cache
            && let Ok(walgit_store::GetResult::Object { body, meta }) = handle
                .store()
                .get(&shared_key(key), GetOptions::default())
                .await
            && let Ok(b) = walgit_store::util::collect(body, meta.size as usize).await
        {
            metrics::counter!("walgit_api_immutable_hit", "tier" => "store").increment(1);
            st.caches.api_immutable.insert(key.clone(), b.clone());
            return Ok(Rendered::json(b, IMMUTABLE, None).into_response(headers));
        }
    }
    if slow && crate::sse::wants_sse(headers) {
        let (tx, rx) = tokio::sync::broadcast::channel(256);
        let sources = vec![handle.subscribe_progress(), rx];
        let st2 = st.clone();
        let fut = async move {
            let repo = view(&st2, handle, need, Reporter::for_repo(tx)).await?;
            work(repo).await
        };
        return Ok(crate::sse::envelope(sources, fut));
    }
    let repo = view(st, handle, need, Reporter::none()).await?;
    Ok(work(repo).await?.into_response(headers))
}

// ---- response helpers --------------------------------------------------------

pub(crate) fn etag_for(sha: &str) -> String {
    format!("\"{sha}\"")
}
pub(crate) fn json_swr<T: Serialize>(value: &T, etag: Option<&str>) -> Rendered {
    Rendered::json(json_bytes(value), SWR, etag.map(str::to_string))
}
/// SWR JSON whose `ETag` is a digest of the body: for answers with no natural version (a
/// control object outside the WAL, a listing with detail). A revalidation that finds nothing
/// changed ends in a `304` — it saves the transfer, not the server's store reads.
pub(crate) fn json_swr_digest<T: Serialize>(value: &T) -> Rendered {
    use sha1::Digest;
    let body = json_bytes(value);
    let etag = etag_for(&hex::encode(sha1::Sha1::digest(&body)));
    Rendered::json(body, SWR, Some(etag))
}
fn json_bytes<T: Serialize>(value: &T) -> bytes::Bytes {
    bytes::Bytes::from(serde_json::to_vec(value).unwrap_or_default())
}
fn is_full_sha(s: &str) -> bool {
    (s.len() == 40 || s.len() == 64) && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// `skip` of a paged endpoint, bounds-checked: a caller-supplied usize that
/// must never reach window arithmetic unchecked.
fn page_skip(skip: Option<usize>) -> Result<usize, ApiError> {
    let skip = skip.unwrap_or(0);
    if skip > MAX_SKIP {
        return Err(ApiError::BadRequest(format!(
            "skip must be at most {MAX_SKIP}"
        )));
    }
    Ok(skip)
}

/// Finish a ref-or-sha addressed request: immutable (+LRU +shared cache) or
/// SWR+ETag (304 handled by `Rendered::into_response`). `etag` must identify
/// exactly one representation, so paged endpoints fold their query in.
fn finish(
    st: &AppState,
    r: &Repo,
    immutable: bool,
    cache_key: &str,
    etag: &str,
    body: bytes::Bytes,
) -> Rendered {
    if immutable {
        st.caches
            .api_immutable
            .insert(cache_key.to_string(), body.clone());
        if let Some(store) = &r.shared {
            let store = store.clone();
            let key = shared_key(cache_key);
            let b = body.clone();
            tokio::spawn(async move {
                if let Err(e) = store
                    .put(&key, PutBody::Bytes(b), PutMode::Overwrite.into())
                    .await
                {
                    tracing::debug!(error = %e, key, "shared render cache put failed");
                }
            });
        }
        return Rendered::json(body, IMMUTABLE, None);
    }
    Rendered::json(body, SWR, Some(etag_for(etag)))
}

// ---- instance ----------------------------------------------------------------

/// Which instance answered (kind/name/revision/build) — footer of the UI. Not
/// cached: every response should reflect the machine that produced it.
async fn instance_info(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    st.auth.require_read(&headers).await.map_err(auth_err)?;
    let mut r = axum::Json(crate::instance::info(&st.cfg)).into_response();
    r.headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    Ok(r)
}

// ---- owners ------------------------------------------------------------------

/// `?detail=1` on the owner listings (`web/API.md`). Plain calls keep their `string[]`
/// shape; detail answers objects with the human-readable metadata (`crate::metadata`).
#[derive(serde::Deserialize, Default)]
pub(crate) struct ListQuery {
    detail: Option<String>,
}

impl ListQuery {
    fn detail(&self) -> Result<bool, ApiError> {
        match self.detail.as_deref() {
            None | Some("0" | "false") => Ok(false),
            Some("1" | "true") => Ok(true),
            Some(other) => Err(ApiError::BadRequest(format!(
                "detail={other}: expected 1 or 0"
            ))),
        }
    }
}

/// `GET /api/v1/owners[?detail=1]` → `["o", …]`, or `[{name, display_name?, description?}]`:
/// the listing (`Registry::list`, cached `LIST_TTL`) then one profile GET per owner,
/// `metadata::DETAIL_CONCURRENCY` in flight. The detail answer carries a body-digest `ETag`.
pub(crate) async fn owners(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<ListQuery>,
) -> Result<Response, ApiError> {
    let principal = st.auth.require_read(&headers).await.map_err(auth_err)?;
    let detail = q.detail()?;
    let repos = st.registry.list().await.map_err(internal)?;
    let mut out: Vec<String> = repos
        .into_iter()
        .filter(|r| principal.sees_owner(r.owner()))
        .map(|r| r.owner().to_string())
        .collect();
    out.sort();
    out.dedup();
    if detail {
        let rows = crate::metadata::owner_details(&st.store, out)
            .await
            .map_err(internal)?;
        return Ok(json_swr_digest(&rows).into_response(&headers));
    }
    Ok(json_swr(&out, None).into_response(&headers))
}

/// `GET /api/v1/owners/{owner}/repos[?detail=1]` → `["r", …]`, or `[{name, description?}]`
/// (one description GET per repository, as above). `[]` for an unknown owner either way.
pub(crate) async fn owner_repos(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(owner): Path<String>,
    Query(q): Query<ListQuery>,
) -> Result<Response, ApiError> {
    let principal = st.auth.require_read(&headers).await.map_err(auth_err)?;
    let detail = q.detail()?;
    // An owner outside the scope lists like an unknown one: `[]`, not 404.
    let mut repos: Vec<walgit_git::RepoId> = st
        .registry
        .list()
        .await
        .map_err(internal)?
        .into_iter()
        .filter(|r| r.owner() == owner && principal.sees_owner(r.owner()))
        .collect();
    repos.sort_by(|a, b| a.name().cmp(b.name()));
    repos.dedup();
    if detail {
        let rows = crate::metadata::repo_details(&st.store, repos)
            .await
            .map_err(internal)?;
        return Ok(json_swr_digest(&rows).into_response(&headers));
    }
    let out: Vec<String> = repos.into_iter().map(|r| r.name().to_string()).collect();
    Ok(json_swr(&out, None).into_response(&headers))
}

// ---- refs --------------------------------------------------------------------

async fn refs(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    run(
        &st,
        &headers,
        &owner,
        &repo_name,
        Need::Refs,
        None,
        |r| async move {
            let head = r.index.head().map(|(name, sha)| RefInfo { name, sha });
            let etag = etag_for(head.as_ref().map_or("unborn", |h| h.sha.as_str()));
            Ok(json_swr(&Refs { head }, Some(&etag)))
        },
    )
    .await
}

async fn ref_list(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name, kind)): Path<(String, String, String)>,
    Query(q): Query<RefListQuery>,
) -> Result<Response, ApiError> {
    let wants_sse = crate::sse::wants_sse(&headers);
    let handle = open(&st, &headers, &owner, &repo_name).await?;
    let r = view(&st, handle, Need::Refs, Reporter::none()).await?;
    let list = match kind.as_str() {
        "branches" => &r.index.branches,
        "tags" => &r.index.tags,
        _ => return Err(not_found("ref namespace")),
    };
    let n = q.n.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE);
    let prefix = q
        .prefix
        .as_deref()
        .map(|p| p.trim_matches('/'))
        .filter(|p| !p.is_empty())
        .map(|p| format!("{p}/"));
    let needle =
        q.q.as_deref()
            .filter(|s| !s.is_empty())
            .map(str::to_ascii_lowercase);
    let after = q.after.as_deref().unwrap_or("");
    // Byte-sorted: skip straight to the first candidate (> after, >= prefix).
    let lower = match &prefix {
        Some(p) if p.as_str() > after => p.as_str(),
        _ => after,
    };
    let start = list
        .partition_point(|(name, _)| name.as_str() <= lower && name.as_str() != lower)
        .max(list.partition_point(|(name, _)| name.as_str() <= after));
    let mut refs = Vec::with_capacity(n.min(256));
    let mut more = false;
    for (name, sha) in &list[start..] {
        if let Some(p) = &prefix
            && !name.starts_with(p.as_str())
        {
            break; // sorted: no further names share the prefix
        }
        if let Some(nd) = &needle
            && !name.to_ascii_lowercase().contains(nd.as_str())
        {
            continue;
        }
        if refs.len() == n {
            more = true;
            break;
        }
        refs.push(RefInfo {
            name: name.clone(),
            sha: sha.clone(),
        });
    }
    if wants_sse {
        // Streamed form: one `ref` packet per ref, then `done` (web/API.md).
        let mut items: Vec<Result<bytes::Bytes, std::convert::Infallible>> =
            Vec::with_capacity(refs.len() + 1);
        for r in &refs {
            items.push(Ok(crate::sse::packet("ref", r)));
        }
        items.push(Ok(crate::sse::packet(
            "done",
            &serde_json::json!({ "more": more }),
        )));
        let mut resp = crate::sse::sse_response(futures::stream::iter(items));
        resp.headers_mut()
            .insert(header::CACHE_CONTROL, SWR.parse().unwrap());
        return Ok(resp);
    }
    Ok(json_swr(&RefPage { refs, more }, None).into_response(&headers))
}

// ---- resolve -----------------------------------------------------------------

/// §3: longest branch/tag prefix of `rest` wins (branch beats tag on ties);
/// else the first segment must be a commit-ish; empty -> default branch.
async fn resolve_rest(r: &Repo, rest: &str) -> Result<Resolved, ApiError> {
    let rest = rest.trim_matches('/');
    if rest.is_empty() {
        let (name, sha) = r.index.head().ok_or_else(|| not_found("unborn HEAD"))?;
        return Ok(Resolved {
            ref_name: name,
            sha,
            path: String::new(),
            kind: "branch",
        });
    }
    // Candidate prefixes, longest first.
    let mut cut_points: Vec<usize> = rest.match_indices('/').map(|(i, _)| i).collect();
    cut_points.push(rest.len());
    for &cut in cut_points.iter().rev() {
        let name = &rest[..cut];
        let path = rest[cut..].trim_start_matches('/').to_string();
        if let Some(sha) = r.index.branch(name) {
            return Ok(Resolved {
                ref_name: name.to_string(),
                sha: sha.to_string(),
                path,
                kind: "branch",
            });
        }
        if let Some(sha) = r.index.tag(name) {
            return Ok(Resolved {
                ref_name: name.to_string(),
                sha: sha.to_string(),
                path,
                kind: "tag",
            });
        }
    }
    let (first, path) = match rest.split_once('/') {
        Some((f, p)) => (f, p.to_string()),
        None => (rest, String::new()),
    };
    let sha = rev_parse_commit(r, first).await?;
    Ok(Resolved {
        ref_name: first.to_string(),
        sha,
        path,
        kind: "commit",
    })
}

/// Resolve a single revision name (no path): branch, tag, then git rev-parse.
async fn resolve_name(r: &Repo, name: &str) -> Result<Resolved, ApiError> {
    if (name.is_empty() || name == "HEAD")
        && let Some((n, sha)) = r.index.head()
    {
        return Ok(Resolved {
            ref_name: n,
            sha,
            path: String::new(),
            kind: "branch",
        });
    }
    if let Some(sha) = r.index.branch(name) {
        return Ok(Resolved {
            ref_name: name.into(),
            sha: sha.into(),
            path: String::new(),
            kind: "branch",
        });
    }
    if let Some(sha) = r.index.tag(name) {
        return Ok(Resolved {
            ref_name: name.into(),
            sha: sha.into(),
            path: String::new(),
            kind: "tag",
        });
    }
    let sha = rev_parse_commit(r, name).await?;
    Ok(Resolved {
        ref_name: name.into(),
        sha,
        path: String::new(),
        kind: "commit",
    })
}

/// Resolve one side of a `compare/{base}...{head}` (D57): the §3 name rule
/// for names, and `rev-parse^{commit}` for a full sha — a sha that names an
/// annotated tag object peels to its commit on both lanes, exactly what
/// `resolve` answers for the same input (`ref` stays the raw sha, `sha` is
/// the commit).
async fn compare_side(r: &Repo, raw: &str) -> Result<CompareSide, ApiError> {
    if is_full_sha(raw) {
        let sha = rev_parse_commit(r, raw).await?;
        return Ok(CompareSide {
            ref_name: raw.to_string(),
            sha,
        });
    }
    let res = resolve_name(r, raw).await?;
    Ok(CompareSide {
        ref_name: res.ref_name,
        sha: res.sha,
    })
}

/// `rev-parse --verify <rev>^{commit}`: local git when objects are on disk,
/// the pack indexes (unique prefix, tag peel) when served remotely.
async fn rev_parse_commit(r: &Repo, rev: &str) -> Result<String, ApiError> {
    if rev.is_empty() || rev.starts_with('-') {
        return Err(not_found("revision"));
    }
    if !r.objects {
        return Err(not_found(format!("unknown revision {rev}")));
    }
    if let Some(remote) = r.remote() {
        return Ok(remote.resolve_commitish(rev).await?.to_string());
    }
    let out = git(
        &r.local,
        vec![
            "rev-parse".into(),
            "--verify".into(),
            "--quiet".into(),
            "--end-of-options".into(),
            format!("{rev}^{{commit}}"),
        ],
    )
    .await
    .map_err(|_| not_found(format!("unknown revision {rev}")))?;
    let sha = String::from_utf8_lossy(&out).trim().to_string();
    if sha.is_empty() {
        return Err(not_found(format!("unknown revision {rev}")));
    }
    Ok(sha)
}

async fn resolve_root(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    resolve_impl(&st, &headers, &owner, &repo_name, "").await
}
async fn resolve(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name, rest)): Path<(String, String, String)>,
) -> Result<Response, ApiError> {
    resolve_impl(&st, &headers, &owner, &repo_name, &rest).await
}
async fn resolve_impl(
    st: &Arc<AppState>,
    headers: &HeaderMap,
    owner: &str,
    repo_name: &str,
    rest: &str,
) -> Result<Response, ApiError> {
    // Branch/tag names resolve from the index; a raw revision falls back to
    // object access. Refs-only first so huge repos still answer for named refs.
    let handle = open(st, headers, owner, repo_name).await?;
    let mut r = view(st, handle, Need::Refs, Reporter::none()).await?;
    let res = match resolve_rest(&r, rest).await {
        Ok(res) => res,
        Err(ApiError::NotFound(_)) if !r.objects => {
            r.need_objects(st).await?;
            resolve_rest(&r, rest).await?
        }
        Err(e) => return Err(e),
    };
    let etag = etag_for(&res.sha);
    Ok(json_swr(&res, Some(&etag)).into_response(headers))
}

/// Split `{ref}/{path}` for tree/blob: a leading full sha is taken verbatim
/// (immutable response); otherwise §3 resolution (SWR + `ETag`).
fn split_addr(rest: &str) -> Option<(Resolved, bool)> {
    let rest = rest.trim_matches('/');
    let (first, path) = match rest.split_once('/') {
        Some((f, p)) => (f, p.trim_matches('/').to_string()),
        None => (rest, String::new()),
    };
    is_full_sha(first).then(|| {
        (
            Resolved {
                ref_name: first.to_string(),
                sha: first.to_string(),
                path,
                kind: "commit",
            },
            true,
        )
    })
}
async fn resolve_addr(r: &Repo, rest: &str) -> Result<(Resolved, bool), ApiError> {
    if let Some(x) = split_addr(rest) {
        return Ok(x);
    }
    Ok((resolve_rest(r, rest).await?, false))
}

// ---- git plumbing ------------------------------------------------------------

fn output_bytes(out: std::process::Output) -> Result<Vec<u8>, ApiError> {
    if out.status.success() {
        Ok(out.stdout)
    } else {
        Err(not_found(
            String::from_utf8_lossy(&out.stderr).trim().to_string(),
        ))
    }
}
async fn git(local: &walgit_git::LocalRepo, args: Vec<String>) -> Result<Vec<u8>, ApiError> {
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let out = local.git(&refs).await.map_err(internal)?;
    output_bytes(out)
}

/// Run `git` reading stdout up to `cap` bytes, then kill the child: a diff of
/// a long-lived branch against a monorepo trunk can be hundreds of MiB, and
/// the 2 MiB patch that ships must not buffer (let alone compute to
/// completion) all of it. Returns at most `cap` bytes.
async fn git_capped(
    local: &walgit_git::LocalRepo,
    args: &[&str],
    cap: usize,
) -> Result<Vec<u8>, ApiError> {
    use tokio::io::AsyncReadExt;
    let mut child = tokio::process::Command::new("git")
        .current_dir(local.path())
        .env("GIT_DIR", local.path())
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(internal)?;
    let Some(mut out) = child.stdout.take() else {
        return Err(internal(format!("git {}: no stdout", args[0])));
    };
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = vec![0u8; 64 * 1024];
    let mut capped = false;
    loop {
        let n = out.read(&mut chunk).await.map_err(internal)?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n.min(cap - buf.len())]);
        if buf.len() >= cap {
            capped = true;
            break;
        }
    }
    if capped {
        // The child is most likely blocked writing into the pipe we stopped
        // draining; kill it before waiting.
        child.kill().await.map_err(internal)?;
    }
    let status = child.wait().await.map_err(internal)?;
    if !capped && !status.success() {
        return Err(internal(format!("git {} failed: {status}", args[0])));
    }
    Ok(buf)
}

/// Files changed, from `git diff --shortstat` (" 3 files changed, …", empty
/// output for an empty diff). One line of output, so the count is known
/// before the numstat or the patch buffers anything.
fn shortstat_files(out: &[u8]) -> usize {
    String::from_utf8_lossy(out)
        .split(" changed")
        .next()
        .unwrap_or("")
        .split_whitespace()
        .next()
        .and_then(|w| w.parse().ok())
        .unwrap_or(0)
}

fn parse_commit_record(record: &str) -> Option<Commit> {
    let mut p = record.split('\0');
    let sha = p.next()?.trim().to_string();
    if sha.is_empty() {
        return None;
    }
    let parents = p.next()?.split_whitespace().map(str::to_string).collect();
    Some(
        Commit {
            sha,
            parents,
            author: p.next()?.to_string(),
            author_email: p.next()?.to_string(),
            author_date: p.next()?.to_string(),
            committer: p.next()?.to_string(),
            commit_date: p.next()?.to_string(),
            subject: p.next()?.to_string(),
            body: String::new(),
            trailers: Vec::new(),
        }
        .with_body(p.next().unwrap_or("")),
    )
}
fn parse_commits(bytes: &[u8]) -> Vec<Commit> {
    String::from_utf8_lossy(bytes)
        .split('\x1e')
        .filter_map(parse_commit_record)
        .collect()
}
fn log_format() -> String {
    "%x1e%H%x00%P%x00%an%x00%ae%x00%aI%x00%cn%x00%cI%x00%s%x00%b%x00".to_string()
}

// ---- tree ----------------------------------------------------------------------

fn tree_key(repo: &str, sha: &str, path: &str) -> String {
    format!("{repo}\0tree\0{sha}\0{path}")
}

async fn tree(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name, rest)): Path<(String, String, String)>,
) -> Result<Response, ApiError> {
    let key = split_addr(&rest)
        .map(|(res, _)| tree_key(&format!("{owner}/{repo_name}"), &res.sha, &res.path));
    let st2 = st.clone();
    run(
        &st,
        &headers,
        &owner,
        &repo_name,
        Need::Objects,
        key,
        move |r| async move {
            let (res, immutable) = resolve_addr(&r, &rest).await?;
            let key = tree_key(&r.id, &res.sha, &res.path);
            if immutable && let Some(hit) = st2.caches.api_immutable.get(&key) {
                return Ok(Rendered::json(hit, IMMUTABLE, None));
            }
            let body = match r.remote() {
                Some(remote) => render_tree_remote(&remote, &res).await?,
                None => render_tree(&r.local, &res).await?,
            };
            Ok(finish(&st2, &r, immutable, &key, &res.sha, body))
        },
    )
    .await
}

async fn render_tree(
    local: &walgit_git::LocalRepo,
    res: &Resolved,
) -> Result<bytes::Bytes, ApiError> {
    let spec = if res.path.is_empty() {
        format!("{}^{{tree}}", res.sha)
    } else {
        format!("{}:{}", res.sha, res.path)
    };
    let bytes = git(
        local,
        vec!["ls-tree".into(), "-l".into(), "-z".into(), spec],
    )
    .await?;
    let mut entries = Vec::new();
    for item in bytes.split(|b| *b == 0).filter(|x| !x.is_empty()) {
        let Some(tab) = item.iter().position(|b| *b == b'\t') else {
            continue;
        };
        let (meta, name) = item.split_at(tab);
        let name = &name[1..];
        // `ls-tree -l` right-aligns the size with padding spaces.
        let fields: Vec<&[u8]> = meta
            .split(|b| *b == b' ')
            .filter(|f| !f.is_empty())
            .collect();
        if fields.len() < 4 {
            continue;
        }
        let kind = String::from_utf8_lossy(fields[1]).to_string();
        let size = if kind == "blob" {
            String::from_utf8_lossy(fields[3]).parse().unwrap_or(-1)
        } else {
            -1
        };
        entries.push(TreeEntry {
            name: String::from_utf8_lossy(name).to_string(),
            kind,
            mode: String::from_utf8_lossy(fields[0]).to_string(),
            size,
            sha: String::from_utf8_lossy(fields[2]).to_string(),
        });
    }
    sort_entries(&mut entries);
    let commit = newest_commit(local, &res.sha, &res.path)
        .await
        .ok()
        .and_then(|b| parse_commits(&b).into_iter().next());
    let mut readme = None;
    if let Some(e) = readme_entry(&entries)
        && let Ok(content) = git(local, vec!["cat-file".into(), "blob".into(), e.sha.clone()]).await
        && let Ok(s) = String::from_utf8(content)
    {
        readme = Some(Readme {
            name: e.name.clone(),
            contents: s,
        });
    }
    Ok(json_bytes(&Tree {
        ref_name: res.ref_name.clone(),
        sha: res.sha.clone(),
        path: res.path.clone(),
        entries,
        commit,
        readme,
    }))
}

fn sort_entries(entries: &mut [TreeEntry]) {
    entries.sort_by(|a, b| {
        let ad = a.kind == "tree";
        let bd = b.kind == "tree";
        bd.cmp(&ad)
            .then_with(|| a.name.as_bytes().cmp(b.name.as_bytes()))
    });
}
fn readme_entry(entries: &[TreeEntry]) -> Option<&TreeEntry> {
    entries.iter().find(|e| {
        e.kind == "blob"
            && [
                "readme",
                "readme.md",
                "readme.markdown",
                "readme.txt",
                "readme.rst",
            ]
            .contains(&e.name.to_ascii_lowercase().as_str())
    })
}

/// Tree listing straight from the remote pack set: entries from the parsed
/// tree, blob sizes from pack entry headers (concurrent), the newest commit
/// via a bounded walk, README read directly.
async fn render_tree_remote(remote: &Remote, res: &Resolved) -> Result<bytes::Bytes, ApiError> {
    let sha =
        gix_hash::ObjectId::from_hex(res.sha.as_bytes()).map_err(|_| not_found("revision"))?;
    remote.reporter.notice(format!(
        "Reading {} from the WAL pack set",
        if res.path.is_empty() {
            "the root tree".to_string()
        } else {
            format!("tree {}", res.path)
        }
    ));
    let (_c, target, mode) = remote.fault_path(&sha, &res.path).await?;
    if !mode.is_tree() {
        return Err(not_found(format!("'{}' is not a tree", res.path)));
    }
    let raw = remote.tree_entries(&target).await?;
    let total = raw.len();
    let entries: Vec<TreeEntry> = futures::stream::iter(raw)
        .map(|e| async move {
            let kind = match e.mode.kind() {
                gix_object::tree::EntryKind::Tree => "tree",
                gix_object::tree::EntryKind::Commit => "commit",
                _ => "blob",
            };
            let size = if kind == "blob" {
                remote
                    .kind_and_size(&e.oid)
                    .await
                    .ok()
                    .flatten()
                    .map_or(-1, |(_, s)| s as i64)
            } else {
                -1
            };
            TreeEntry {
                name: String::from_utf8_lossy(&e.name).to_string(),
                kind: kind.to_string(),
                mode: format!("{:06o}", e.mode.kind() as u16),
                size,
                sha: e.oid.to_string(),
            }
        })
        .buffer_unordered(32)
        .collect()
        .await;
    if total > 64 {
        remote.reporter.notice(format!("Sized {total} entries"));
    }
    let mut entries = entries;
    sort_entries(&mut entries);
    let commit = remote
        .newest_touching(sha, &res.path)
        .await?
        .map(Commit::from);
    let mut readme = None;
    if let Some(e) = readme_entry(&entries)
        && let Ok(oid) = gix_hash::ObjectId::from_hex(e.sha.as_bytes())
        && let Ok(o) = remote.get(&oid).await
        && let Ok(s) = String::from_utf8(o.data.to_vec())
    {
        readme = Some(Readme {
            name: e.name.clone(),
            contents: s,
        });
    }
    Ok(json_bytes(&Tree {
        ref_name: res.ref_name.clone(),
        sha: res.sha.clone(),
        path: res.path.clone(),
        entries,
        commit,
        readme,
    }))
}

async fn newest_commit(
    local: &walgit_git::LocalRepo,
    sha: &str,
    path: &str,
) -> Result<Vec<u8>, ApiError> {
    let mut a = vec![
        "log".into(),
        "-1".into(),
        format!("--format={}", log_format()),
        sha.into(),
    ];
    if !path.is_empty() {
        a.push("--".into());
        a.push(path.into());
    }
    git(local, a).await
}

// ---- blob ----------------------------------------------------------------------

fn blob_key(repo: &str, sha: &str, path: &str) -> String {
    format!("{repo}\0blob\0{sha}\0{path}")
}

async fn blob(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name, rest)): Path<(String, String, String)>,
    Query(q): Query<BlobQuery>,
) -> Result<Response, ApiError> {
    let raw = q.raw.is_some();
    // `?raw` is a page navigation (the "Raw" link): never the SSE envelope.
    let mut plain_headers = headers.clone();
    if raw {
        plain_headers.remove(header::ACCEPT);
    }
    let key = if raw {
        None
    } else {
        split_addr(&rest)
            .map(|(res, _)| blob_key(&format!("{owner}/{repo_name}"), &res.sha, &res.path))
    };
    let st2 = st.clone();
    run(
        &st,
        &plain_headers,
        &owner,
        &repo_name,
        Need::Objects,
        key,
        move |r| async move {
            let (res, immutable) = resolve_addr(&r, &rest).await?;
            if res.path.is_empty() {
                return Err(not_found("blob path"));
            }
            let name = res.path.rsplit('/').next().unwrap_or(&res.path).to_string();
            let (size, bytes): (i64, Option<Vec<u8>>) = if let Some(remote) = r.remote() {
                let sha = gix_hash::ObjectId::from_hex(res.sha.as_bytes())
                    .map_err(|_| not_found("revision"))?;
                remote
                    .reporter
                    .notice(format!("Reading {} from the WAL pack set", res.path));
                let (_c, target, mode) = remote.fault_path(&sha, &res.path).await?;
                if !mode.is_blob_or_symlink() {
                    return Err(not_found(format!("'{}' is not a file", res.path)));
                }
                let (_, size) = remote
                    .kind_and_size(&target)
                    .await?
                    .ok_or_else(|| not_found("blob"))?;
                if size as usize > MAX_BLOB {
                    (size as i64, None)
                } else {
                    let o = remote.get(&target).await?;
                    (size as i64, Some(o.data.to_vec()))
                }
            } else {
                let bytes = git(
                    &r.local,
                    vec![
                        "cat-file".into(),
                        "blob".into(),
                        format!("{}:{}", res.sha, res.path),
                    ],
                )
                .await?;
                (bytes.len() as i64, Some(bytes))
            };
            let is_text = size <= MAX_BLOB as i64
                && bytes
                    .as_ref()
                    .is_some_and(|b| !b.contains(&0) && std::str::from_utf8(b).is_ok());
            if raw && is_text {
                let etag = etag_for(&res.sha);
                return Ok(Rendered {
                    body: bytes::Bytes::from(bytes.unwrap_or_default()),
                    content_type: "text/plain; charset=utf-8",
                    cache_control: if immutable { IMMUTABLE } else { SWR },
                    etag: (!immutable).then_some(etag),
                });
            }
            let b = if size > MAX_BLOB as i64 {
                Blob {
                    ref_name: res.ref_name.clone(),
                    sha: res.sha.clone(),
                    path: res.path.clone(),
                    name,
                    size,
                    contents: None,
                    binary: None,
                    too_large: Some(true),
                }
            } else if !is_text {
                Blob {
                    ref_name: res.ref_name.clone(),
                    sha: res.sha.clone(),
                    path: res.path.clone(),
                    name,
                    size,
                    contents: None,
                    binary: Some(true),
                    too_large: None,
                }
            } else {
                Blob {
                    ref_name: res.ref_name.clone(),
                    sha: res.sha.clone(),
                    path: res.path.clone(),
                    name,
                    size,
                    contents: Some(
                        String::from_utf8(bytes.unwrap_or_default()).unwrap_or_default(),
                    ),
                    binary: None,
                    too_large: None,
                }
            };
            let key = blob_key(&r.id, &res.sha, &res.path);
            Ok(finish(&st2, &r, immutable, &key, &res.sha, json_bytes(&b)))
        },
    )
    .await
}

// ---- commits -------------------------------------------------------------------

fn commits_key(repo: &str, sha: &str, path: &str, skip: usize, n: usize) -> String {
    format!("{repo}\0commits\0{sha}\0{path}\0{skip}\0{n}")
}

async fn commits(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name)): Path<(String, String)>,
    Query(q): Query<CommitQuery>,
) -> Result<Response, ApiError> {
    let reference = q.ref_.clone().unwrap_or_else(|| "HEAD".into());
    let skip = page_skip(q.skip)?;
    let n = q.n.unwrap_or(35).clamp(1, 200);
    let path = q.path.clone().unwrap_or_default();
    let key = is_full_sha(&reference)
        .then(|| commits_key(&format!("{owner}/{repo_name}"), &reference, &path, skip, n));
    let st2 = st.clone();
    run(
        &st,
        &headers,
        &owner,
        &repo_name,
        Need::Objects,
        key,
        move |r| async move {
            let (res, immutable) = if is_full_sha(&reference) {
                (
                    Resolved {
                        ref_name: reference.clone(),
                        sha: reference.clone(),
                        path: String::new(),
                        kind: "commit",
                    },
                    true,
                )
            } else {
                (resolve_name(&r, &reference).await?, false)
            };
            let key = commits_key(&r.id, &res.sha, &path, skip, n);
            if immutable && let Some(hit) = st2.caches.api_immutable.get(&key) {
                return Ok(Rendered::json(hit, IMMUTABLE, None));
            }
            let mut cs: Vec<Commit> = if let Some(remote) = r.remote() {
                let start = gix_hash::ObjectId::from_hex(res.sha.as_bytes())
                    .map_err(|_| not_found("revision"))?;
                let label = if path.is_empty() {
                    "Walking history".to_string()
                } else {
                    format!("Walking history of {path}")
                };
                remote.reporter.notice(format!(
                    "{label} from {} (reading commits from the WAL pack set)",
                    &res.sha[..12]
                ));
                let all = remote
                    .walk(
                        start,
                        (!path.is_empty()).then_some(path.as_str()),
                        skip.saturating_add(n).saturating_add(1),
                        &label,
                    )
                    .await?;
                all.into_iter().skip(skip).map(Commit::from).collect()
            } else {
                let mut a = vec![
                    "log".into(),
                    format!("--format={}", log_format()),
                    "--no-color".into(),
                    format!("--skip={skip}"),
                    format!("-{count}", count = n.saturating_add(1)),
                    res.sha.clone(),
                ];
                if !path.is_empty() {
                    a.extend(["--".into(), path.clone()]);
                }
                parse_commits(&git(&r.local, a).await?)
            };
            let more = cs.len() > n;
            if more {
                cs.truncate(n);
            }
            let body = json_bytes(&Commits {
                ref_name: res.ref_name.clone(),
                sha: res.sha.clone(),
                commits: cs,
                more,
            });
            // The answer varies with skip/n/path, so the validator must too
            // (RFC 9110: one ETag per representation). A path is folded in as
            // a digest to keep the header value opaque and quotable.
            let mut etag = format!("{}-{skip}-{n}", res.sha);
            if !path.is_empty() {
                use sha1::Digest as _;
                let d = hex::encode(sha1::Sha1::digest(path.as_bytes()));
                etag.push('-');
                etag.push_str(&d[..12]);
            }
            Ok(finish(&st2, &r, immutable, &key, &etag, body))
        },
    )
    .await
}

// ---- commit detail -------------------------------------------------------------

fn commit_key(repo: &str, sha: &str) -> String {
    format!("{repo}\0commit\0{sha}")
}

async fn commit_detail(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name, rev)): Path<(String, String, String)>,
) -> Result<Response, ApiError> {
    let key = is_full_sha(&rev).then(|| commit_key(&format!("{owner}/{repo_name}"), &rev));
    let st2 = st.clone();
    run(
        &st,
        &headers,
        &owner,
        &repo_name,
        Need::Objects,
        key,
        move |r| async move {
            let immutable = is_full_sha(&rev);
            let sha = if immutable {
                rev.clone()
            } else {
                resolve_name(&r, &rev).await?.sha
            };
            let key = commit_key(&r.id, &sha);
            if immutable && let Some(hit) = st2.caches.api_immutable.get(&key) {
                return Ok(Rendered::json(hit, IMMUTABLE, None));
            }
            if let Some(remote) = r.remote() {
                // Fault the commit, its first parent and every object the diff
                // touches into the loose store; `git show` below then runs as-is.
                let oid = gix_hash::ObjectId::from_hex(sha.as_bytes())
                    .map_err(|_| not_found("commit"))?;
                remote.reporter.notice(format!(
                    "Reading commit {} from the WAL pack set",
                    &sha[..12]
                ));
                remote.fault_commit_diff(&oid).await?;
            }
            let commit = parse_commits(
                // `--diff-merges=off`: plain `show -s` on a merge still sets up
                // the combined diff and reads the other parents' subtrees (on a
                // remotely served repo those are exactly the objects we did not
                // fault).
                &git(
                    &r.local,
                    vec![
                        "show".into(),
                        "-s".into(),
                        "--diff-merges=off".into(),
                        format!("--format={}", log_format()),
                        sha.clone(),
                    ],
                )
                .await?,
            )
            .into_iter()
            .next()
            .ok_or_else(|| not_found("commit"))?;
            let stat_out = git(
                &r.local,
                vec![
                    "show".into(),
                    "--format=".into(),
                    "--numstat".into(),
                    "-M".into(),
                    "--diff-merges=first-parent".into(),
                    "--root".into(),
                    sha.clone(),
                ],
            )
            .await?;
            let stats = parse_stats(&stat_out);
            let patch = String::from_utf8_lossy(
                &git(
                    &r.local,
                    vec![
                        "show".into(),
                        "--format=".into(),
                        "-p".into(),
                        "-M".into(),
                        "--no-color".into(),
                        "--no-ext-diff".into(),
                        "--diff-merges=first-parent".into(),
                        "--root".into(),
                        sha.clone(),
                    ],
                )
                .await?,
            )
            .into_owned();
            let body = json_bytes(&CommitDetail {
                commit,
                stats,
                patch,
            });
            Ok(finish(&st2, &r, immutable, &key, &sha, body))
        },
    )
    .await
}
fn parse_stats(bytes: &[u8]) -> Vec<Stat> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(|line| {
            let f: Vec<&str> = line.split('\t').collect();
            if f.len() < 3 || (!f[0].chars().all(|c| c.is_ascii_digit()) && f[0] != "-") {
                return None;
            }
            let path = normalize_rename(f[2]);
            Some(Stat {
                path,
                additions: if f[0] == "-" {
                    -1
                } else {
                    f[0].parse().unwrap_or(-1)
                },
                deletions: if f[1] == "-" {
                    -1
                } else {
                    f[1].parse().unwrap_or(-1)
                },
            })
        })
        .collect()
}
/// `git --numstat -M` prints renames as `old => new` or `prefix/{old => new}/suffix`;
/// return the new path.
fn normalize_rename(s: &str) -> String {
    if let (Some(open), Some(close)) = (s.find('{'), s.rfind('}'))
        && open < close
    {
        let inner = &s[open + 1..close];
        if let Some((_, new)) = inner.split_once(" => ") {
            let mut out = String::with_capacity(s.len());
            out.push_str(&s[..open]);
            out.push_str(new);
            out.push_str(&s[close + 1..]);
            return out.replace("//", "/");
        }
    }
    if let Some((_, new)) = s.split_once(" => ") {
        return new.to_string();
    }
    s.to_string()
}

// ---- compare ---------------------------------------------------------------------

fn compare_key(repo: &str, base: &str, head: &str, skip: usize, n: usize) -> String {
    format!("{repo}\0compare\0{base}\0{head}\0{skip}\0{n}")
}

fn unrelated(base: &str, head: &str) -> ApiError {
    not_found(format!(
        "no merge base: {base} and {head} are unrelated histories"
    ))
}

/// Split `{base}...{head}`. `git check-ref-format` forbids `..` in a ref name
/// and neither a hex sha nor `HEAD` contains a dot-pair, so one `...`
/// separator is unambiguous; any other shape is a clean 400 before anything
/// is resolved.
fn split_compare(rest: &str) -> Result<(&str, &str), ApiError> {
    let rest = rest.trim_matches('/');
    let parts: Vec<&str> = rest.split("...").collect();
    match parts.as_slice() {
        [base, head] if !base.is_empty() && !head.is_empty() => Ok((base, head)),
        _ => Err(ApiError::BadRequest(format!(
            "expected {{base}}...{{head}}, got {rest:?}"
        ))),
    }
}

/// Cut a patch at [`MAX_PATCH`] bytes, preferring a whole-file boundary (the
/// `\n` before a `diff --git ` line, so a patch parser never sees half a
/// file), else the last newline, else a hard cut. Returns the patch and
/// whether it was truncated.
fn cap_patch(bytes: Vec<u8>) -> (String, bool) {
    if bytes.len() <= MAX_PATCH {
        return (String::from_utf8_lossy(&bytes).into_owned(), false);
    }
    let window = &bytes[..MAX_PATCH];
    let cut = window
        .windows(12)
        .rposition(|w| w == b"\ndiff --git ")
        .map_or_else(
            || {
                window
                    .iter()
                    .rposition(|b| *b == b'\n')
                    .map_or(MAX_PATCH, |i| i + 1)
            },
            |i| i + 1,
        );
    (String::from_utf8_lossy(&bytes[..cut]).into_owned(), true)
}

async fn compare_root(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    compare_impl(&st, &headers, &owner, &repo_name, "", &CompareQuery::default()).await
}

async fn compare(
    State(st): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((owner, repo_name, rest)): Path<(String, String, String)>,
    Query(q): Query<CompareQuery>,
) -> Result<Response, ApiError> {
    compare_impl(&st, &headers, &owner, &repo_name, &rest, &q).await
}

/// `GET …/compare/{base}...{head}?skip=&n=` (D57): merge base, ahead/behind,
/// one page of head-side commits, complete numstat and a capped patch of the
/// three-dot (merge-base→head) diff. Two full shas make the answer immutable;
/// anything else is SWR + `ETag: "<base sha>...<head sha>-<skip>-<n>"`.
async fn compare_impl(
    st: &Arc<AppState>,
    headers: &HeaderMap,
    owner: &str,
    repo_name: &str,
    rest: &str,
    q: &CompareQuery,
) -> Result<Response, ApiError> {
    let (base_raw, head_raw) = split_compare(rest).map(|(b, h)| (b.to_string(), h.to_string()))?;
    let skip = page_skip(q.skip)?;
    let n = q.n.unwrap_or(35).clamp(1, 200);
    let immutable = is_full_sha(&base_raw) && is_full_sha(&head_raw);
    let key = immutable.then(|| {
        compare_key(
            &format!("{owner}/{repo_name}"),
            &base_raw,
            &head_raw,
            skip,
            n,
        )
    });
    let st2 = st.clone();
    run(
        st,
        headers,
        owner,
        repo_name,
        Need::Objects,
        key,
        move |r| async move {
            // Principle VII: one resolve per side.
            let base = compare_side(&r, &base_raw).await?;
            let head = compare_side(&r, &head_raw).await?;
            let key = compare_key(&r.id, &base.sha, &head.sha, skip, n);
            if immutable && let Some(hit) = st2.caches.api_immutable.get(&key) {
                return Ok(Rendered::json(hit, IMMUTABLE, None));
            }
            let label = format!("Comparing {}...{}", &base.sha[..12], &head.sha[..12]);
            let (merge_base, ahead, behind, cs, more) = if let Some(remote) = r.remote() {
                remote.reporter.notice(format!(
                    "{label} (walking commits from the WAL pack set)"
                ));
                let b = gix_hash::ObjectId::from_hex(base.sha.as_bytes())
                    .map_err(|_| not_found("revision"))?;
                let h = gix_hash::ObjectId::from_hex(head.sha.as_bytes())
                    .map_err(|_| not_found("revision"))?;
                let walk = remote.compare_walk(b, h, &label).await?;
                let mb = walk
                    .merge_base
                    .ok_or_else(|| unrelated(&base.ref_name, &head.ref_name))?;
                // `ahead` is exact, so the page flag is set arithmetic on it;
                // `skip` is bounds-checked to 400 above.
                let more = walk.ahead > skip && walk.ahead - skip > n;
                let cs: Vec<Commit> = walk
                    .page
                    .into_iter()
                    .skip(skip)
                    .map(Commit::from)
                    .collect();
                // The commits, then every tree/blob the diff touches: git
                // below runs against faulted loose objects, same as
                // `commit/{sha}`.
                remote.fault_many(&[b, h, mb]).await?;
                let base_tree = remote.commit(&mb).await?.tree;
                let head_tree = remote.commit(&h).await?.tree;
                remote
                    .fault_tree_diff(Some(base_tree), Some(head_tree))
                    .await?;
                (mb.to_string(), walk.ahead, walk.behind, cs, more)
            } else {
                // `merge-base` exit 1 with no output = unrelated histories;
                // any other failure is git's own error (an unknown sha side).
                let out = r
                    .local
                    .git(&["merge-base", &base.sha, &head.sha])
                    .await
                    .map_err(internal)?;
                let mb = if out.status.success() {
                    String::from_utf8_lossy(&out.stdout).trim().to_string()
                } else if out.status.code() == Some(1) && out.stdout.is_empty() {
                    return Err(unrelated(&base.ref_name, &head.ref_name));
                } else {
                    return Err(not_found(String::from_utf8_lossy(&out.stderr).trim()));
                };
                // `--left-right --count b...h` prints `behind\tahead`
                // (left = base-only).
                let counts = git(
                    &r.local,
                    vec![
                        "rev-list".into(),
                        "--left-right".into(),
                        "--count".into(),
                        format!("{}...{}", base.sha, head.sha),
                    ],
                )
                .await?;
                let (behind, ahead) = parse_counts(&counts)?;
                let cs = parse_commits(
                    &git(
                        &r.local,
                        vec![
                            "log".into(),
                            format!("--format={}", log_format()),
                            "--no-color".into(),
                            format!("--skip={skip}"),
                            format!("-{}", n.saturating_add(1)),
                            format!("{}..{}", base.sha, head.sha),
                        ],
                    )
                    .await?,
                );
                let more = cs.len() > n;
                (mb, ahead, behind, cs, more)
            };
            let mut cs = cs;
            if cs.len() > n {
                cs.truncate(n);
            }
            // Principle V, both lanes: bound the diff before rendering it.
            // `--shortstat` prints one line, so the file count is known
            // before the numstat (one line per file) or the patch buffers
            // anything; the remote lane faults under the same object count.
            let short = git(
                &r.local,
                vec![
                    "diff".into(),
                    "--shortstat".into(),
                    "-M".into(),
                    "--no-color".into(),
                    merge_base.clone(),
                    head.sha.clone(),
                ],
            )
            .await?;
            if shortstat_files(&short) > MAX_DIFF_OBJECTS {
                return Err(ApiError::ServiceUnavailable(format!(
                    "this diff touches more than {MAX_DIFF_OBJECTS} objects; too large to render"
                )));
            }
            let stat_out = git(
                &r.local,
                vec![
                    "diff".into(),
                    "--numstat".into(),
                    "-M".into(),
                    "--no-color".into(),
                    merge_base.clone(),
                    head.sha.clone(),
                ],
            )
            .await?;
            let stats = parse_stats(&stat_out);
            let (patch, patch_truncated) = cap_patch(
                // Read at most one byte past the cap so `cap_patch` can tell
                // "at the cap" from "past it"; the child is killed there.
                git_capped(
                    &r.local,
                    &[
                        "diff",
                        "-M",
                        "--no-color",
                        "--no-ext-diff",
                        &merge_base,
                        &head.sha,
                    ],
                    MAX_PATCH + 1,
                )
                .await?,
            );
            // Each page is its own representation, so the validator covers
            // skip/n too (RFC 9110: one ETag per representation).
            let etag = format!("{}...{}-{skip}-{n}", base.sha, head.sha);
            let body = json_bytes(&Compare {
                base,
                head,
                merge_base,
                ahead,
                behind,
                commits: cs,
                more,
                stats,
                patch,
                patch_truncated,
            });
            Ok(finish(&st2, &r, immutable, &key, &etag, body))
        },
    )
    .await
}

/// Parse `rev-list --left-right --count` output: `behind\tahead`.
fn parse_counts(bytes: &[u8]) -> Result<(usize, usize), ApiError> {
    let text = String::from_utf8_lossy(bytes);
    let (l, r) = text
        .trim()
        .split_once('\t')
        .ok_or_else(|| internal(format!("rev-list --count output: {text:?}")))?;
    let behind = l
        .trim()
        .parse()
        .map_err(|_| internal(format!("rev-list --count output: {text:?}")))?;
    let ahead = r
        .trim()
        .parse()
        .map_err(|_| internal(format!("rev-list --count output: {text:?}")))?;
    Ok((behind, ahead))
}

#[cfg(test)]
mod tests {
    #[test]
    fn rename_paths() {
        assert_eq!(
            super::normalize_rename("src/{main.rs => app.rs}"),
            "src/app.rs"
        );
        assert_eq!(super::normalize_rename("{a => b}/x.rs"), "b/x.rs");
        assert_eq!(super::normalize_rename("a/{ => sub}/x.rs"), "a/sub/x.rs");
        assert_eq!(super::normalize_rename("old.rs => new.rs"), "new.rs");
        assert_eq!(super::normalize_rename("plain.rs"), "plain.rs");
    }

    #[test]
    fn compare_split() {
        let (b, h) = super::split_compare("main...feature/x").unwrap();
        assert_eq!((b, h), ("main", "feature/x"));
        assert_eq!(super::split_compare("main...feature/x/").unwrap().1, "feature/x");
        // Ref names cannot contain `..`, so a slash-bearing head with `...`
        // splits once; more than two parts or an empty side is a 400.
        for bad in [
            "",
            "main",
            "...main",
            "main...",
            "a...b...c",
            "/",
        ] {
            assert!(
                super::split_compare(bad).is_err(),
                "{bad:?} must be a 400"
            );
        }
    }

    #[test]
    fn compare_patch_cap() {
        let file = |body_len: usize| {
            format!("diff --git a/f b/f\n--- a/f\n+++ b/f\n{}", "x\n".repeat(body_len))
        };
        // Small patches pass through untouched.
        let small = format!("{}{}", file(3), file(3));
        let (p, truncated) = super::cap_patch(small.clone().into_bytes());
        assert_eq!(p, small);
        assert!(!truncated);
        // Oversized: the cut keeps whole files (a `diff --git` boundary).
        let big = format!("{}{}{}", file(600_000), file(600_000), file(600_000));
        assert!(big.len() > super::MAX_PATCH);
        let (p, truncated) = super::cap_patch(big.clone().into_bytes());
        assert!(truncated);
        assert!(p.len() <= super::MAX_PATCH);
        assert!(
            p.ends_with('\n'),
            "cut at a line boundary: {}",
            &p[p.len() - 40..]
        );
        assert!(!p.ends_with("diff --git a/f b/f\n"));
        let files = p.matches("diff --git a/f b/f").count();
        assert_eq!(files, 1, "the first file stays whole, the rest is dropped");
        // One file alone above the cap: no boundary fits, cut at the last
        // newline inside the cap.
        let one = file(super::MAX_PATCH);
        let (p, truncated) = super::cap_patch(one.clone().into_bytes());
        assert!(truncated);
        assert!(p.len() <= super::MAX_PATCH);
        assert!(p.ends_with('\n'));
        assert!(p.starts_with("diff --git a/f b/f"));
    }

    #[test]
    fn paging_skip_bounds() {
        assert_eq!(super::page_skip(None).unwrap(), 0);
        assert_eq!(super::page_skip(Some(0)).unwrap(), 0);
        assert_eq!(super::page_skip(Some(super::MAX_SKIP)).unwrap(), super::MAX_SKIP);
        // A usize that would wrap the window arithmetic is a 400, not a wrap.
        assert!(super::page_skip(Some(super::MAX_SKIP + 1)).is_err());
        assert!(super::page_skip(Some(usize::MAX)).is_err());
    }

    #[test]
    fn shortstat_file_count() {
        assert_eq!(super::shortstat_files(b""), 0, "empty diff prints nothing");
        assert_eq!(super::shortstat_files(b" 1 file changed"), 1);
        assert_eq!(
            super::shortstat_files(b" 3 files changed, 10 insertions(+), 2 deletions(-)"),
            3
        );
        // Never more than a number: garbage parses as zero, the diff is then
        // bounded by the object check downstream, not miscounted upward.
        assert_eq!(super::shortstat_files(b"no summary"), 0);
    }
}
