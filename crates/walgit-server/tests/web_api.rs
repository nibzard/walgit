// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(clippy::indexing_slicing, clippy::string_slice, clippy::unwrap_used)]

//! web/API.md §6 conformance for the read-only JSON API.

mod harness;

use harness::{Server, git_in};
use serde_json::Value;

type TestResult = anyhow::Result<()>;

async fn get(
    server: &Server,
    path: &str,
) -> anyhow::Result<(reqwest::StatusCode, String, Option<String>)> {
    let resp = reqwest::Client::new()
        .get(format!("{}{path}", server.base_url))
        .header("Accept", "application/json")
        .send()
        .await?;
    let status = resp.status();
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(std::string::ToString::to_string);
    let text = resp.text().await?;
    Ok((status, text, ct))
}

async fn get_h(
    server: &Server,
    path: &str,
    extra: &[(&str, &str)],
) -> anyhow::Result<(reqwest::StatusCode, String, reqwest::header::HeaderMap)> {
    let mut req = reqwest::Client::new()
        .get(format!("{}{path}", server.base_url))
        .header("Accept", "application/json");
    for (k, v) in extra {
        req = req.header(*k, *v);
    }
    let resp = req.send().await?;
    let status = resp.status();
    let headers = resp.headers().clone();
    let text = resp.text().await?;
    Ok((status, text, headers))
}
fn hdr(h: &reqwest::header::HeaderMap, k: &str) -> String {
    h.get(k)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

/// `git_in` with extra environment: the clock-skew fixture pins committer
/// dates, because the remote compare walk orders pops by `commit_time`.
fn git_env(
    dir: &std::path::Path,
    envs: &[(&str, &str)],
    args: &[&str],
) -> anyhow::Result<String> {
    let out = std::process::Command::new("git")
        .current_dir(dir)
        .envs(envs.iter().copied())
        .args(args)
        .output()?;
    anyhow::ensure!(
        out.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

async fn json(server: &Server, path: &str) -> anyhow::Result<Value> {
    let (status, text, ct) = get(server, path).await?;
    anyhow::ensure!(status.is_success(), "GET {path} -> {status}: {text}");
    anyhow::ensure!(
        ct.as_deref().unwrap_or("").starts_with("application/json"),
        "content-type {ct:?}"
    );
    Ok(serde_json::from_str(&text)?)
}

/// Build a source repo with the shapes the UI cares about and push it.
fn fixture(server: &Server) -> anyhow::Result<std::path::PathBuf> {
    let dir = tempfile::tempdir()?.keep(); // TODO(hermetic): keep TempDir in fixture
    git_in(&dir, &["init", "-q", "-b", "main"])?;
    git_in(&dir, &["config", "user.email", "t@t"])?;
    git_in(&dir, &["config", "user.name", "Tester"])?;
    std::fs::write(dir.join("README.md"), "# Title\n\nhello\n")?;
    std::fs::create_dir_all(dir.join("src/inner"))?;
    std::fs::write(dir.join("src/main.rs"), "fn main() {}\n")?;
    std::fs::write(dir.join("src/inner/x.txt"), "x\n")?;
    std::fs::write(dir.join("bin.dat"), [0u8, 159, 146, 150, 0, 1, 2])?;
    std::fs::write(dir.join("big.txt"), vec![b'a'; 2 * 1024 * 1024 + 1])?;
    git_in(&dir, &["add", "."])?;
    git_in(&dir, &["commit", "-q", "-m", "initial\n\nbody line"])?;
    // feature/x branch with a nested dir named after a path segment, plus rename.
    git_in(&dir, &["checkout", "-q", "-b", "feature/x"])?;
    std::fs::create_dir_all(dir.join("dir"))?;
    std::fs::write(dir.join("dir/f.txt"), "f\n")?;
    git_in(&dir, &["mv", "src/main.rs", "src/app.rs"])?;
    git_in(&dir, &["add", "."])?;
    git_in(&dir, &["commit", "-q", "-m", "feature work"])?;
    git_in(&dir, &["checkout", "-q", "main"])?;
    std::fs::write(dir.join("src/inner/x.txt"), "xx\n")?;
    git_in(&dir, &["commit", "-qam", "second on main"])?;
    git_in(
        &dir,
        &["merge", "-q", "--no-ff", "-m", "merge feature", "feature/x"],
    )?;
    git_in(&dir, &["tag", "-a", "v1.0", "-m", "release"])?;
    git_in(
        &dir,
        &[
            "-c",
            "tag.forceSignAnnotated=false",
            "-c",
            "tag.gpgsign=false",
            "tag",
            "light",
        ],
    )?;
    for _ in 0..40 {
        git_in(&dir, &["commit", "-q", "--allow-empty", "-m", "filler"])?;
    }
    git_in(
        &dir,
        &["push", "-q", "--mirror", &server.repo_url("o", "r")],
    )?;
    Ok(dir)
}

/// web/API.md §6 against one server (called for the local-packs instance and
/// for a sibling that serves the same repo remotely).
#[allow(clippy::many_single_char_names)]
async fn conformance(
    server: &Server,
    src: &std::path::Path,
    head: &str,
    feature: &str,
    v1_peeled: &str,
) -> TestResult {
    // owners
    assert_eq!(
        json(server, "/services/api/owners").await?,
        serde_json::json!(["o"])
    );
    assert_eq!(
        json(server, "/services/api/owners/o").await?,
        serde_json::json!(["r"])
    );

    // refs: O(1) head only, ETag + 304
    let (st, text, h) = get_h(server, "/o/r/api/refs", &[]).await?;
    assert_eq!(st, 200);
    let refs: Value = serde_json::from_str(&text)?;
    assert_eq!(refs["head"]["name"], "main");
    assert_eq!(refs["head"]["sha"], head);
    let etag = hdr(&h, "etag");
    assert_eq!(etag, format!("\"{head}\""));
    assert!(hdr(&h, "cache-control").contains("stale-while-revalidate"));
    let (st, _, _) = get_h(server, "/o/r/api/refs", &[("If-None-Match", &etag)]).await?;
    assert_eq!(st, 304);
    // ref lists: paged, sorted, filtered
    let p = json(server, "/o/r/api/refs/branches?n=1").await?;
    assert_eq!(p["refs"][0]["name"], "feature/x");
    assert_eq!(p["more"], true);
    let p = json(server, "/o/r/api/refs/branches?after=feature/x&n=5").await?;
    assert_eq!(p["refs"][0]["name"], "main");
    assert_eq!(p["refs"][0]["sha"], head);
    assert_eq!(p["more"], false);
    let p = json(server, "/o/r/api/refs/branches?q=AIN").await?;
    assert_eq!(p["refs"].as_array().unwrap().len(), 1);
    let p = json(server, "/o/r/api/refs/branches?prefix=feature").await?;
    assert_eq!(p["refs"][0]["name"], "feature/x");
    let p = json(server, "/o/r/api/refs/tags").await?;
    let tags = p["refs"].as_array().unwrap();
    assert_eq!(tags.len(), 2);
    let v1 = tags.iter().find(|t| t["name"] == "v1.0").unwrap();
    assert_eq!(v1["sha"], v1_peeled, "annotated tag sha must be peeled");
    assert_eq!(get(server, "/o/r/api/refs/nope").await?.0, 404);
    // SSE form
    let (st, body, h) = get_h(
        server,
        "/o/r/api/refs/tags",
        &[("Accept", "text/event-stream")],
    )
    .await?;
    assert_eq!(st, 200);
    assert!(hdr(&h, "content-type").starts_with("text/event-stream"));
    assert!(body.contains("event: ref\n") && body.contains("event: done\ndata: {\"more\":false}"));

    // resolve
    let (st, text, h) = get_h(server, "/o/r/api/resolve/feature/x/dir", &[]).await?;
    assert_eq!(st, 200);
    let r: Value = serde_json::from_str(&text)?;
    assert_eq!(r["ref"], "feature/x");
    assert_eq!(r["sha"], feature);
    assert_eq!(r["path"], "dir");
    assert_eq!(r["kind"], "branch");
    assert_eq!(hdr(&h, "etag"), format!("\"{feature}\""));
    let r = json(server, "/o/r/api/resolve/v1.0").await?;
    assert_eq!(r["kind"], "tag");
    assert_eq!(r["sha"], v1_peeled);
    let r = json(server, &format!("/o/r/api/resolve/{}/src", &head[..8])).await?;
    assert_eq!(r["kind"], "commit");
    assert_eq!(r["sha"], head);
    assert_eq!(r["path"], "src");
    let r = json(server, "/o/r/api/resolve/").await?;
    assert_eq!(r["ref"], "main");
    let (st, _, ct) = get(server, "/o/r/api/resolve/nope/x").await?;
    assert_eq!(st, 404);
    assert!(!ct.unwrap_or_default().contains("json"));

    // tree root
    let (st, text, h) = get_h(server, "/o/r/api/tree/main", &[]).await?;
    assert_eq!(st, 200);
    let tree: Value = serde_json::from_str(&text)?;
    assert_eq!(tree["ref"], "main");
    assert_eq!(tree["sha"], head);
    assert_eq!(tree["path"], "");
    assert!(hdr(&h, "cache-control").contains("stale-while-revalidate"));
    assert_eq!(hdr(&h, "etag"), format!("\"{head}\""));
    let names: Vec<&str> = tree["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["dir", "src", "README.md", "big.txt", "bin.dat"],
        "dirs first, then byte order"
    );
    let src_entry = &tree["entries"][1];
    assert_eq!(src_entry["type"], "tree");
    assert_eq!(src_entry["mode"], "040000");
    assert_eq!(src_entry["size"], -1);
    let readme_entry = &tree["entries"][2];
    assert_eq!(readme_entry["type"], "blob");
    assert_eq!(readme_entry["mode"], "100644");
    assert_eq!(readme_entry["size"], "# Title\n\nhello\n".len());
    assert_eq!(tree["readme"]["name"], "README.md");
    assert!(
        tree["readme"]["contents"]
            .as_str()
            .unwrap()
            .starts_with("# Title")
    );
    assert_eq!(tree["commit"]["sha"].as_str().unwrap().len(), 40);

    // longest-ref rule: feature/x + path dir
    let t = json(server, "/o/r/api/tree/feature/x/dir").await?;
    assert_eq!(t["ref"], "feature/x");
    assert_eq!(t["path"], "dir");
    assert_eq!(t["entries"][0]["name"], "f.txt");
    // subtree commit = newest commit touching the path
    let t = json(server, "/o/r/api/tree/main/src/inner").await?;
    assert_eq!(t["entries"][0]["name"], "x.txt");
    assert_eq!(t["commit"]["subject"], "second on main");
    // blob path as tree -> 404 plain text
    let (st, body, ct) = get(server, "/o/r/api/tree/main/README.md").await?;
    assert_eq!(st, 404);
    assert!(
        !ct.unwrap_or_default().contains("json"),
        "404 must be plain text: {body}"
    );
    // sha as ref -> immutable
    let (st, text, h) = get_h(server, &format!("/o/r/api/tree/{feature}"), &[]).await?;
    assert_eq!(st, 200);
    let t: Value = serde_json::from_str(&text)?;
    assert_eq!(t["ref"], feature);
    assert_eq!(t["sha"], feature);
    assert!(hdr(&h, "cache-control").contains("immutable"));
    // second hit served from the immutable LRU
    let (st, text2, _) = get_h(server, &format!("/o/r/api/tree/{feature}"), &[]).await?;
    assert_eq!(st, 200);
    assert_eq!(text, text2);

    // blob
    let b = json(server, "/o/r/api/blob/main/README.md").await?;
    assert_eq!(b["name"], "README.md");
    assert_eq!(b["path"], "README.md");
    assert_eq!(b["contents"], "# Title\n\nhello\n");
    let (st, raw, ct) = get(server, "/o/r/api/blob/main/README.md?raw").await?;
    assert_eq!(st, 200);
    assert!(ct.unwrap_or_default().starts_with("text/plain"));
    assert_eq!(raw, "# Title\n\nhello\n");
    let b = json(server, "/o/r/api/blob/main/bin.dat").await?;
    assert_eq!(b["binary"], true);
    assert!(b.get("contents").is_none());
    let b = json(server, "/o/r/api/blob/main/big.txt").await?;
    assert_eq!(b["too_large"], true);
    assert_eq!(b["size"], 2 * 1024 * 1024 + 1);
    assert_eq!(get(server, "/o/r/api/blob/main/nope.txt").await?.0, 404);

    // commits + pagination
    let c = json(server, "/o/r/api/commits?ref=main&path=&skip=0").await?;
    assert_eq!(c["ref"], "main");
    assert_eq!(c["sha"], head);
    let (_, _, h) = get_h(
        server,
        &format!("/o/r/api/commits?ref={head}&path=&skip=0"),
        &[],
    )
    .await?;
    assert!(hdr(&h, "cache-control").contains("immutable"));
    let commits = c["commits"].as_array().unwrap();
    assert_eq!(commits.len(), 35);
    assert_eq!(c["more"], true);
    assert_eq!(commits[0]["sha"], head);
    assert!(commits[0]["parents"].is_array());
    let c2 = json(server, "/o/r/api/commits?ref=main&skip=35&n=50").await?;
    assert_eq!(c2["more"], false);
    let total = 35 + c2["commits"].as_array().unwrap().len();
    let expected: usize = git_in(src, &["rev-list", "--count", "main"])?
        .trim()
        .parse()?;
    assert_eq!(total, expected);
    let c = json(server, "/o/r/api/commits?ref=main&path=src/inner/x.txt").await?;
    let subjects: Vec<&str> = c["commits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["subject"].as_str().unwrap())
        .collect();
    assert_eq!(subjects, vec!["second on main", "initial"]);
    let first = &c["commits"][1];
    assert_eq!(first["body"], "body line");
    assert_eq!(first["parents"], serde_json::json!([]));
    assert!(first["author_date"].as_str().unwrap().contains('T'));
    assert_eq!(get(server, "/o/r/api/commits?ref=nope").await?.0, 404);
    // commits pages carry their own validator too: skip (and path) are in the
    // ETag, so one page never 304s another (RFC 9110).
    let (_, _, h) = get_h(server, "/o/r/api/commits?ref=main&n=2", &[]).await?;
    let e = hdr(&h, "etag");
    let (st, _, _) = get_h(
        server,
        "/o/r/api/commits?ref=main&n=2&skip=2",
        &[("If-None-Match", &e)],
    )
    .await?;
    assert_eq!(st, 200, "commits page 2 must not 304 on page 1's ETag");

    // commit detail: rename + merge (first-parent)
    let d = json(server, &format!("/o/r/api/commit/{feature}")).await?;
    assert_eq!(d["commit"]["sha"], feature);
    let paths: Vec<&str> = d["stats"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["path"].as_str().unwrap())
        .collect();
    assert!(
        paths.contains(&"src/app.rs"),
        "renamed file appears once with new path: {paths:?}"
    );
    assert!(!paths.contains(&"src/main.rs"));
    assert!(d["patch"].as_str().unwrap().contains("diff --git a/"));
    let m = json(server, &format!("/o/r/api/commit/{head}")).await?;
    // HEAD is a filler empty commit; find the merge commit instead.
    assert_eq!(m["stats"], serde_json::json!([]));
    let merge = git_in(src, &["rev-parse", "main~40"])?.trim().to_string();
    let m = json(server, &format!("/o/r/api/commit/{merge}")).await?;
    assert_eq!(m["commit"]["parents"].as_array().unwrap().len(), 2);
    assert!(
        !m["stats"].as_array().unwrap().is_empty(),
        "merge diffed against first parent must have stats"
    );
    assert!(m["patch"].as_str().unwrap().contains("diff --git"));
    assert!(!m["patch"].as_str().unwrap().contains("diff --cc"));
    // short sha and 404
    let d = json(server, &format!("/o/r/api/commit/{}", &feature[..10])).await?;
    assert_eq!(d["commit"]["sha"], feature);
    let (_, _, h) = get_h(server, &format!("/o/r/api/commit/{}", &feature[..10]), &[]).await?;
    assert_eq!(hdr(&h, "etag"), format!("\"{feature}\""));
    let (_, _, h) = get_h(server, &format!("/o/r/api/commit/{feature}"), &[]).await?;
    assert!(hdr(&h, "cache-control").contains("immutable"));
    let (st, _, ct) = get(server, "/o/r/api/commit/deadbeef").await?;
    assert_eq!(st, 404);
    assert!(!ct.unwrap_or_default().contains("json"));

    // compare: shape + exact counts vs git. feature/x was merged into main,
    // so its tip is the merge base of main...feature/x (ahead 0); the branch
    // shape shows against the commit it forked from.
    let initial = git_in(src, &["rev-parse", "main~42"])?.trim().to_string();
    let (st, text, h) = get_h(server, "/o/r/api/compare/main...feature/x", &[]).await?;
    assert_eq!(st, 200, "{text}");
    let c: Value = serde_json::from_str(&text)?;
    assert_eq!(c["base"], serde_json::json!({ "ref": "main", "sha": head }));
    assert_eq!(c["head"], serde_json::json!({ "ref": "feature/x", "sha": feature }));
    assert_eq!(c["merge_base"], feature, "feature/x is merged: it IS the merge base");
    let counts = git_in(src, &["rev-list", "--left-right", "--count", "main...feature/x"])?
        .trim()
        .to_string();
    let (behind, ahead): (usize, usize) = counts
        .split_once('\t')
        .map(|(b, a)| (b.parse().unwrap(), a.parse().unwrap()))
        .unwrap();
    assert_eq!(c["ahead"], ahead, "ahead vs git: {counts}");
    assert_eq!(c["behind"], behind, "behind vs git: {counts}");
    assert_eq!(ahead, 0, "fixture: feature/x is fully merged");
    assert_eq!(behind, 42);
    assert_eq!(c["commits"], serde_json::json!([]));
    assert_eq!(c["more"], false);
    assert_eq!(c["stats"], serde_json::json!([]));
    assert_eq!(c["patch"], "");
    // names → SWR + ETag of the two resolved shas; If-None-Match → 304
    assert!(hdr(&h, "cache-control").contains("stale-while-revalidate"));
    let etag = hdr(&h, "etag");
    assert_eq!(etag, format!("\"{head}...{feature}-0-35\""));
    let (st, _, _) = get_h(
        server,
        "/o/r/api/compare/main...feature/x",
        &[("If-None-Match", &etag)],
    )
    .await?;
    assert_eq!(st, 304);
    // Each page is its own representation: skip/n are in the ETag, so page 2
    // is never page 1's 304 (RFC 9110).
    let (st, _, h2) = get_h(
        server,
        "/o/r/api/compare/main...feature/x?skip=35&n=35",
        &[("If-None-Match", &etag)],
    )
    .await?;
    assert_eq!(st, 200, "a different page must not 304");
    assert_ne!(hdr(&h2, "etag"), etag);

    // the branch shape: initial...feature/x has the merge base at the fork
    let (st, text, _) = get_h(server, &format!("/o/r/api/compare/{initial}...feature/x"), &[]).await?;
    assert_eq!(st, 200);
    let c: Value = serde_json::from_str(&text)?;
    assert_eq!(c["merge_base"], initial);
    assert_eq!(c["ahead"], 1);
    assert_eq!(c["behind"], 0);
    let cs = c["commits"].as_array().unwrap();
    assert_eq!(cs.len(), 1);
    assert_eq!(cs[0]["sha"], feature);
    assert_eq!(cs[0]["subject"], "feature work");
    assert_eq!(c["more"], false);
    let paths: Vec<&str> = c["stats"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["path"].as_str().unwrap())
        .collect();
    assert!(
        paths.contains(&"src/app.rs") && paths.contains(&"dir/f.txt"),
        "stats carry the rename and the new file: {paths:?}"
    );
    assert!(c["patch"].as_str().unwrap().contains("diff --git a/"));
    assert_eq!(c["patch_truncated"], false);
    // percent-encoded slash in the head = the same ref (SDK per-segment enc)
    let c2 = json(server, &format!("/o/r/api/compare/{initial}...feature%2Fx")).await?;
    assert_eq!(c2["head"]["ref"], "feature/x");
    assert_eq!(c2["head"]["sha"], feature);
    // both sides full shas → immutable, byte-identical second answer
    let (st, text, h) = get_h(server, &format!("/o/r/api/compare/{initial}...{feature}"), &[]).await?;
    assert_eq!(st, 200);
    assert!(hdr(&h, "cache-control").contains("immutable"));
    let (st2, text2, _) = get_h(server, &format!("/o/r/api/compare/{initial}...{feature}"), &[]).await?;
    assert_eq!(st2, 200);
    assert_eq!(text, text2, "second hit served from the immutable cache");
    // the other direction pages through main's 42 commits
    let c = json(server, "/o/r/api/compare/feature/x...main?n=1").await?;
    assert_eq!(c["ahead"], 42);
    assert_eq!(c["behind"], 0);
    assert_eq!(c["more"], true);
    assert_eq!(c["commits"].as_array().unwrap().len(), 1);
    assert_eq!(c["commits"][0]["sha"], head);
    let c = json(server, "/o/r/api/compare/feature/x...main?skip=1&n=1").await?;
    assert_eq!(c["commits"][0]["subject"], "filler");
    // identical sides: empty diff, itself as merge base
    let c = json(server, "/o/r/api/compare/main...main").await?;
    assert_eq!(c["merge_base"], head);
    assert_eq!(c["ahead"], 0);
    assert_eq!(c["behind"], 0);
    assert_eq!(c["commits"], serde_json::json!([]));
    assert_eq!(c["stats"], serde_json::json!([]));
    assert_eq!(c["patch"], "");
    // malformed ranges: 400, plain text, before any resolution
    for bad in [
        "/o/r/api/compare",
        "/o/r/api/compare/",
        "/o/r/api/compare/main",
        "/o/r/api/compare/...main",
        "/o/r/api/compare/main...",
        "/o/r/api/compare/a...b...c",
    ] {
        let (st, body, ct) = get(server, bad).await?;
        assert_eq!(st, 400, "{bad}: {body}");
        assert!(!ct.unwrap_or_default().contains("json"), "{bad}");
    }
    // unknown sides
    assert_eq!(get(server, "/o/r/api/compare/main...nope").await?.0, 404);
    assert_eq!(get(server, "/o/r/api/compare/nope...main").await?.0, 404);
    // a full tag-object sha peels on both lanes, as `resolve` does
    let tag_sha = git_in(src, &["rev-parse", "v1.0"])?.trim().to_string();
    let c = json(server, &format!("/o/r/api/compare/{tag_sha}...main")).await?;
    assert_eq!(c["base"]["ref"], tag_sha, "ref stays the raw sha");
    assert_eq!(c["base"]["sha"], v1_peeled, "sha is the peeled commit");
    assert_eq!(c["merge_base"], v1_peeled);
    assert_eq!(c["ahead"], 40, "the 40 filler commits");
    // an absurd skip is a 400, never a wrapped window
    for path in [
        "/o/r/api/compare/main...feature/x?skip=18446744073709551615&n=1",
        "/o/r/api/commits?ref=main&skip=18446744073709551615",
        "/o/r/api/compare/main...feature/x?skip=1000000001",
    ] {
        let (st, body, _) = get(server, path).await?;
        assert_eq!(st, 400, "{path}: {body}");
    }
    // the largest accepted skip answers coherently (empty, no more)
    let c = json(server, "/o/r/api/compare/main...feature/x?skip=1000000000&n=1").await?;
    assert_eq!(c["commits"], serde_json::json!([]));
    assert_eq!(c["more"], false);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_md_conformance() -> TestResult {
    let server = Server::start().await?;
    // Empty instance.
    assert_eq!(
        json(&server, "/services/api/owners").await?,
        serde_json::json!([])
    );
    assert_eq!(
        json(&server, "/services/api/owners/nobody").await?,
        serde_json::json!([])
    );

    server.put_repo("o", "r").await?;
    let src = fixture(&server)?;
    let head = git_in(&src, &["rev-parse", "HEAD"])?.trim().to_string();
    let feature = git_in(&src, &["rev-parse", "feature/x"])?
        .trim()
        .to_string();
    let v1_peeled = git_in(&src, &["rev-parse", "v1.0^{commit}"])?
        .trim()
        .to_string();
    conformance(&server, &src, &head, &feature, &v1_peeled).await?;

    // unknown repo
    assert_eq!(get(&server, "/o/nope/api/refs").await?.0, 404);
    // page route -> index.html
    let (st, html, ct) = get(&server, "/o/r/tree/main/anything").await?;
    assert_eq!(st, 200);
    assert!(ct.unwrap_or_default().starts_with("text/html"));
    assert!(html.contains("<html"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_repo_refs() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("o", "empty").await?;
    let refs = json(&server, "/o/empty/api/refs").await?;
    assert!(refs["head"].is_null());
    let p = json(&server, "/o/empty/api/refs/branches").await?;
    assert_eq!(p["refs"], serde_json::json!([]));
    assert_eq!(p["more"], false);
    assert_eq!(get(&server, "/o/empty/api/resolve/").await?.0, 404);
    Ok(())
}

/// The same contract on an instance whose `cache.max_bytes` is too small for
/// the repo's packs: objects are read from the store by range (indexes local),
/// nothing is materialized, and long answers stream the SSE envelope.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_objects_conformance() -> TestResult {
    let big = Server::start().await?;
    big.put_repo("o", "r").await?;
    let src = fixture(&big)?;
    let head = git_in(&src, &["rev-parse", "HEAD"])?.trim().to_string();
    let feature = git_in(&src, &["rev-parse", "feature/x"])?
        .trim()
        .to_string();
    let v1_peeled = git_in(&src, &["rev-parse", "v1.0^{commit}"])?
        .trim()
        .to_string();

    let small = big
        .start_sibling_with(|cfg| {
            cfg.cache.max_bytes = bytesize::ByteSize::b(1);
        })
        .await?;

    // First object request with SSE accept: envelope with task/notice packets and a result.
    let resp = reqwest::Client::new()
        .get(format!("{}/o/r/api/tree/{head}", small.base_url))
        .header("Accept", "application/json, text/event-stream")
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    assert!(
        hdr(resp.headers(), "content-type").starts_with("text/event-stream"),
        "first remote render streams"
    );
    let text = resp.text().await?;
    assert!(
        text.contains("event: notice\n"),
        "narrates what it does: {text}"
    );
    assert!(
        text.contains("event: task\n"),
        "remote-index task announced: {text}"
    );
    let result_line = text
        .split("\n\n")
        .find(|p| p.starts_with("event: result"))
        .expect("result packet");
    let body: Value =
        serde_json::from_str(result_line.trim_start_matches("event: result\ndata: "))?;
    assert_eq!(body["sha"], head);
    assert!(body["entries"].as_array().unwrap().len() >= 5);

    // Second time: served from the immutable cache as plain JSON even with SSE accept.
    let resp = reqwest::Client::new()
        .get(format!("{}/o/r/api/tree/{head}", small.base_url))
        .header("Accept", "application/json, text/event-stream")
        .send()
        .await?;
    assert!(hdr(resp.headers(), "content-type").starts_with("application/json"));
    assert!(hdr(resp.headers(), "cache-control").contains("immutable"));

    // Full contract, plain JSON.
    conformance(&small, &src, &head, &feature, &v1_peeled).await?;
    assert!(
        !small.registry_has_packs("o", "r").await,
        "small front must not materialize packs"
    );

    // Tasks are discoverable: the remote-index task ran here and finished ok.
    let t = json(&small, "/o/r/api/tasks").await?;
    let recent = t["recent"].as_array().unwrap();
    let ri = recent
        .iter()
        .find(|r| r["kind"] == "remote-index")
        .expect("remote-index task");
    assert_eq!(ri["ok"], true);
    assert!(t["running"].as_array().unwrap().is_empty());
    // Attach to the finished task: replay + result.
    let (st, text, h) = get_h(
        &small,
        &format!("/o/r/api/tasks/{}", ri["id"].as_str().unwrap()),
        &[("Accept", "text/event-stream")],
    )
    .await?;
    assert_eq!(st, 200);
    assert!(hdr(&h, "content-type").starts_with("text/event-stream"));
    assert!(text.contains("event: result\n"), "{text}");

    // Overview (WAL tab) renders without packs and says so.
    let o = json(&small, "/o/r/api/overview").await?;
    assert_eq!(o["local"]["objects"], "remote");
    assert!(
        o["health"]["issues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i.as_str().unwrap().contains("exceeds"))
    );
    Ok(())
}

/// D15/D27: the repo-scoped API is `/{owner}/{repo}/api/…` (and `…/api-browser/…`);
/// the pre-D15 `/services/api/{owner}/{repo}/…` shape is gone (no aliases —
/// AGENTS banner).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_api_prefix_is_gone() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("o", "r").await?;
    let src = fixture(&server)?;
    let _ = git_in(&src, &["rev-parse", "HEAD"])?;
    for path in [
        "refs",
        "resolve/main",
        "tree/main",
        "compare/main...main",
        "tasks",
        "overview",
    ] {
        let (sa, a, _) = get_h(&server, &format!("/o/r/api/{path}"), &[]).await?;
        assert_eq!(sa, 200, "{path}: {a}");
        let (sb, _, _) = get_h(&server, &format!("/o/r/api-browser/{path}"), &[]).await?;
        assert_eq!(sb, 200, "{path} on the browser lane");
        let (sc, _, _) = get_h(&server, &format!("/services/api/o/r/{path}"), &[]).await?;
        assert_eq!(sc, 404, "{path}: /services/api/o/r must be gone");
    }
    Ok(())
}

/// Pushes are `/<area>/<repository>.git` only — no `.git` is a pkt-line ERR.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_requires_area_repository_git() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("area", "repository").await?;
    let (st, body, _) = get_h(
        &server,
        "/area/repository/info/refs?service=git-receive-pack",
        &[],
    )
    .await?;
    assert_eq!(st, 200, "{body}");
    assert!(
        body.contains("<area>/<repository>.git"),
        "refusal must name the required URL shape: {body:?}"
    );
    let (ok, ad, _) = get_h(
        &server,
        "/area/repository.git/info/refs?service=git-receive-pack",
        &[],
    )
    .await?;
    assert_eq!(ok, 200, "{ad}");
    assert!(!ad.contains("push URL must be"), "{ad:?}");
    Ok(())
}

/// A browser on localhost is sent to walgit.localhost (same port). Git is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn browser_localhost_redirects_to_walgit_localhost() -> TestResult {
    let server = Server::start().await?;
    let url = format!("{}/", server.base_url);
    let resp = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?
        .get(&url)
        .header("Accept", "text/html")
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FOUND,
        "{}",
        resp.status()
    );
    let loc = resp
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        loc.contains("walgit.localhost"),
        "browser should be sent to walgit.localhost, got {loc}"
    );
    let git = reqwest::Client::new()
        .get(format!(
            "{}/area/repository.git/info/refs?service=git-upload-pack",
            server.base_url
        ))
        .header("User-Agent", "git/2.46.0")
        .send()
        .await?;
    assert_ne!(git.status(), reqwest::StatusCode::TEMPORARY_REDIRECT);
    Ok(())
}

/// The happy path on a plain 3-commit branch off main (D57): merge base at
/// the fork point, exact counts, newest-first head-side commits, paging.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compare_three_commit_branch() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("o", "r").await?;
    let dir = tempfile::tempdir()?.keep();
    git_in(&dir, &["init", "-q", "-b", "main"])?;
    git_in(&dir, &["config", "user.email", "t@t"])?;
    git_in(&dir, &["config", "user.name", "Tester"])?;
    std::fs::write(dir.join("base.txt"), "base\n")?;
    git_in(&dir, &["add", "."])?;
    git_in(&dir, &["commit", "-q", "-m", "root"])?;
    let fork = git_in(&dir, &["rev-parse", "HEAD"])?.trim().to_string();
    git_in(&dir, &["checkout", "-q", "-b", "topic/deep/name"])?;
    for i in 1..=3 {
        std::fs::write(dir.join(format!("f{i}.txt")), format!("f{i}\n"))?;
        git_in(&dir, &["add", "."])?;
        git_in(&dir, &["commit", "-q", "-m", &format!("feature {i}")])?;
    }
    git_in(&dir, &["checkout", "-q", "main"])?;
    for i in 1..=2 {
        std::fs::write(dir.join(format!("m{i}.txt")), format!("m{i}\n"))?;
        git_in(&dir, &["add", "."])?;
        git_in(&dir, &["commit", "-q", "-m", &format!("main {i}")])?;
    }
    let main_tip = git_in(&dir, &["rev-parse", "main"])?.trim().to_string();
    let topic_tip = git_in(&dir, &["rev-parse", "topic/deep/name"])?
        .trim()
        .to_string();
    git_in(
        &dir,
        &["push", "-q", "--mirror", &server.repo_url("o", "r")],
    )?;

    // A slash-bearing branch name resolves on both sides of `...`.
    let c = json(&server, "/o/r/api/compare/main...topic/deep/name").await?;
    assert_eq!(c["merge_base"], fork);
    assert_eq!(c["ahead"], 3);
    assert_eq!(c["behind"], 2);
    let subjects: Vec<&str> = c["commits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["subject"].as_str().unwrap())
        .collect();
    assert_eq!(subjects, vec!["feature 3", "feature 2", "feature 1"]);
    assert_eq!(c["more"], false);
    let paths: Vec<&str> = c["stats"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["f1.txt", "f2.txt", "f3.txt"]);
    assert!(c["patch"].as_str().unwrap().contains("+f2"));

    // Paging: n=2 leaves one more; skip walks to it.
    let c = json(&server, "/o/r/api/compare/main...topic/deep/name?n=2").await?;
    let subjects: Vec<&str> = c["commits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["subject"].as_str().unwrap())
        .collect();
    assert_eq!(subjects, vec!["feature 3", "feature 2"]);
    assert_eq!(c["more"], true);
    let c = json(&server, "/o/r/api/compare/main...topic/deep/name?n=2&skip=2").await?;
    let subjects: Vec<&str> = c["commits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["subject"].as_str().unwrap())
        .collect();
    assert_eq!(subjects, vec!["feature 1"]);
    assert_eq!(c["more"], false);

    // The reverse direction is the mirror image (and exercises the ETag).
    let (_, text, h) = get_h(&server, "/o/r/api/compare/topic/deep/name...main", &[]).await?;
    let c: Value = serde_json::from_str(&text)?;
    assert_eq!(c["ahead"], 2);
    assert_eq!(c["behind"], 3);
    assert_eq!(hdr(&h, "etag"), format!("\"{topic_tip}...{main_tip}-0-35\""));
    Ok(())
}

/// `patch_truncated`: the patch stops at 2 MiB on a whole-file boundary;
/// `stats` stays complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compare_patch_truncated() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("o", "r").await?;
    let dir = tempfile::tempdir()?.keep();
    git_in(&dir, &["init", "-q", "-b", "main"])?;
    git_in(&dir, &["config", "user.email", "t@t"])?;
    git_in(&dir, &["config", "user.name", "Tester"])?;
    std::fs::write(dir.join("empty"), "")?;
    git_in(&dir, &["add", "."])?;
    git_in(&dir, &["commit", "-q", "-m", "root"])?;
    git_in(&dir, &["checkout", "-q", "-b", "big"])?;
    // Two files whose patches together exceed the 2 MiB cap, each alone under
    // it: the cut must keep the first file whole and drop the second.
    std::fs::write(dir.join("big1.txt"), vec![b'1'; 1200 * 1024])?;
    std::fs::write(dir.join("big2.txt"), vec![b'2'; 1200 * 1024])?;
    git_in(&dir, &["add", "."])?;
    git_in(&dir, &["commit", "-q", "-m", "big files"])?;
    git_in(
        &dir,
        &["push", "-q", "--mirror", &server.repo_url("o", "r")],
    )?;

    let c = json(&server, "/o/r/api/compare/main...big").await?;
    assert_eq!(c["ahead"], 1);
    assert_eq!(c["patch_truncated"], true, "{}", c["patch"].as_str().unwrap().len());
    let patch = c["patch"].as_str().unwrap();
    assert!(patch.len() <= 2 * 1024 * 1024);
    assert!(patch.contains("diff --git a/big1.txt b/big1.txt"));
    assert!(!patch.contains("diff --git a/big2.txt b/big2.txt"));
    let paths: Vec<&str> = c["stats"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["big1.txt", "big2.txt"], "stats stay complete");
    Ok(())
}

/// The unrelated-histories contract for one lane.
async fn assert_unrelated(server: &Server) -> anyhow::Result<()> {
    let (st, body, ct) = get(server, "/o/orphan/api/compare/main...side").await?;
    assert_eq!(st, 404, "{body}");
    assert!(body.contains("no merge base"), "{body}");
    assert!(body.contains("main") && body.contains("side"), "{body}");
    assert!(!ct.unwrap_or_default().contains("json"));
    Ok(())
}

/// `compare/{b}...{h}` with no common ancestor: a 404 naming both sides — on
/// the local-packs instance and on a 1-byte-cache sibling, where the remote
/// walk drains without ever finding a merge base.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compare_unrelated_histories() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("o", "orphan").await?;
    let dir = tempfile::tempdir()?.keep();
    git_in(&dir, &["init", "-q", "-b", "main"])?;
    git_in(&dir, &["config", "user.email", "t@t"])?;
    git_in(&dir, &["config", "user.name", "Tester"])?;
    std::fs::write(dir.join("a.txt"), "a\n")?;
    git_in(&dir, &["add", "."])?;
    git_in(&dir, &["commit", "-q", "-m", "root a"])?;
    git_in(&dir, &["checkout", "-q", "--orphan", "side"])?;
    git_in(&dir, &["rm", "-rqf", "."])?;
    std::fs::write(dir.join("b.txt"), "b\n")?;
    git_in(&dir, &["add", "."])?;
    git_in(&dir, &["commit", "-q", "-m", "root b"])?;
    git_in(
        &dir,
        &["push", "-q", "--mirror", &server.repo_url("o", "orphan")],
    )?;

    assert_unrelated(&server).await?;
    let small = server
        .start_sibling_with(|cfg| {
            cfg.cache.max_bytes = bytesize::ByteSize::b(1);
        })
        .await?;
    assert_unrelated(&small).await?;
    Ok(())
}

/// The criss-cross contract for one lane: exact counts and a merge base git
/// itself accepts.
async fn assert_criss_cross(server: &Server, bases: &[String]) -> anyhow::Result<()> {
    let c = json(server, "/o/cc/api/compare/main...topic").await?;
    assert_eq!(c["ahead"], 1, "only M2 is topic-side: {}", c["ahead"]);
    assert_eq!(c["behind"], 1, "only M1 is main-side");
    let mb = c["merge_base"].as_str().unwrap();
    assert!(
        bases.contains(&mb.to_string()),
        "merge base {mb} must be one of {bases:?}"
    );
    Ok(())
}

/// Criss-cross (two merge bases): counts exact, and the reported merge base
/// is one git itself accepts (`merge-base --all`) — asserted on both lanes,
/// so the remote walk's pick is covered too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compare_criss_cross() -> TestResult {
    let server = Server::start().await?;
    server.put_repo("o", "cc").await?;
    let dir = tempfile::tempdir()?.keep();
    git_in(&dir, &["init", "-q", "-b", "main"])?;
    git_in(&dir, &["config", "user.email", "t@t"])?;
    git_in(&dir, &["config", "user.name", "Tester"])?;
    std::fs::write(dir.join("a"), "a\n")?;
    git_in(&dir, &["add", "."])?;
    git_in(&dir, &["commit", "-q", "-m", "A"])?;
    git_in(&dir, &["branch", "topic"])?;
    std::fs::write(dir.join("b"), "b\n")?;
    git_in(&dir, &["add", "."])?;
    git_in(&dir, &["commit", "-q", "-m", "B"])?;
    let b = git_in(&dir, &["rev-parse", "HEAD"])?.trim().to_string();
    git_in(&dir, &["checkout", "-q", "topic"])?;
    std::fs::write(dir.join("c"), "c\n")?;
    git_in(&dir, &["add", "."])?;
    git_in(&dir, &["commit", "-q", "-m", "C"])?;
    // main merges topic; topic merges main's pre-merge tip B by sha — the
    // classic two-merge-base shape.
    git_in(&dir, &["checkout", "-q", "main"])?;
    git_in(&dir, &["merge", "-q", "--no-ff", "-m", "M1", "topic"])?;
    git_in(&dir, &["checkout", "-q", "topic"])?;
    git_in(&dir, &["merge", "-q", "--no-ff", "-m", "M2", &b])?;
    git_in(
        &dir,
        &["push", "-q", "--mirror", &server.repo_url("o", "cc")],
    )?;
    let all = git_in(&dir, &["merge-base", "--all", "main", "topic"])?;
    let bases: Vec<String> = all.lines().map(str::trim).map(str::to_string).collect();
    assert_eq!(bases.len(), 2, "fixture must be a criss-cross: {all}");

    assert_criss_cross(&server, &bases).await?;
    let small = server
        .start_sibling_with(|cfg| {
            cfg.cache.max_bytes = bytesize::ByteSize::b(1);
        })
        .await?;
    assert_criss_cross(&small, &bases).await?;
    Ok(())
}

/// The clock-skew contract for one lane: counts exact after retraction, and
/// every page window holds real head-side commits.
async fn assert_skew(server: &Server, c2: &str) -> anyhow::Result<()> {
    let c = json(server, "/o/skewrepo/api/compare/skewbase...skew").await?;
    assert_eq!(c["merge_base"], c2, "C2 is the merge base");
    assert_eq!(c["ahead"], 5, "M, H1..H4: {}", c["ahead"]);
    assert_eq!(c["behind"], 1, "only D is base-side");
    // A page window over the retraction-corrected head side.
    let c = json(server, "/o/skewrepo/api/compare/skewbase...skew?n=3").await?;
    let subjects: Vec<&str> = c["commits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["subject"].as_str().unwrap())
        .collect();
    assert_eq!(subjects, vec!["M", "H4", "H3"], "no common commit, no gap");
    assert_eq!(c["more"], true);
    // The documented pattern (skip += commits.length) delivers the rest.
    let c = json(server, "/o/skewrepo/api/compare/skewbase...skew?n=3&skip=3").await?;
    let subjects: Vec<&str> = c["commits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["subject"].as_str().unwrap())
        .collect();
    assert_eq!(subjects, vec!["H2", "H1"]);
    assert_eq!(c["more"], false, "ahead 5, skip 3 + n 3 covers it");
    Ok(())
}

/// Clock skew (a common commit dated newer than its descendants): commits
/// counted head-side above the frontier are retracted, so counts stay exact
/// and every page carries real head-side commits. The remote walk is where
/// retraction and the page window interact, so both lanes are asserted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compare_skewed_dates() -> TestResult {
    /// Pin both dates of one commit: the walk orders pops by `commit_time`.
    fn at(d: &'static str) -> Vec<(&'static str, &'static str)> {
        vec![("GIT_AUTHOR_DATE", d), ("GIT_COMMITTER_DATE", d)]
    }
    let big = Server::start().await?;
    big.put_repo("o", "skewrepo").await?;
    let dir = tempfile::tempdir()?.keep();
    git_in(&dir, &["init", "-q", "-b", "main"])?;
    git_in(&dir, &["config", "user.email", "t@t"])?;
    git_in(&dir, &["config", "user.name", "Tester"])?;
    // R (Apr); skewbase: C1 (Jun 1) -> C2 (Jun 2) -> D (Jan, skewed older
    // than its own history); skew: R -> H1..H4 (May) -> M (merge of C2,
    // May 5). C2 is the merge base; C1/C2/R are common but dated newer than
    // D, so the walk counts them head-side before the frontier is found.
    let r_env = at("2026-04-01T12:00:00Z");
    git_env(&dir, &r_env, &["commit", "-q", "--allow-empty", "-m", "R"])?;
    let root = git_in(&dir, &["rev-parse", "HEAD"])?.trim().to_string();
    git_in(&dir, &["checkout", "-q", "-b", "skewbase"])?;
    git_env(&dir, &at("2026-06-01T12:00:00Z"), &["commit", "-q", "--allow-empty", "-m", "C1"])?;
    git_env(&dir, &at("2026-06-02T12:00:00Z"), &["commit", "-q", "--allow-empty", "-m", "C2"])?;
    let c2 = git_in(&dir, &["rev-parse", "HEAD"])?.trim().to_string();
    git_env(&dir, &at("2026-01-01T12:00:00Z"), &["commit", "-q", "--allow-empty", "-m", "D"])?;
    git_in(&dir, &["checkout", "-q", "-b", "skew", &root])?;
    for (i, d) in [
        ("1", "2026-05-01T12:00:00Z"),
        ("2", "2026-05-02T12:00:00Z"),
        ("3", "2026-05-03T12:00:00Z"),
        ("4", "2026-05-04T12:00:00Z"),
    ] {
        git_env(
            &dir,
            &at(d),
            &["commit", "-q", "--allow-empty", "-m", &format!("H{i}")],
        )?;
    }
    git_env(
        &dir,
        &at("2026-05-05T12:00:00Z"),
        &["merge", "-q", "--no-ff", "-m", "M", &c2],
    )?;
    git_in(
        &dir,
        &["push", "-q", "--mirror", &big.repo_url("o", "skewrepo")],
    )?;

    assert_skew(&big, &c2).await?;
    let small = big
        .start_sibling_with(|cfg| {
            cfg.cache.max_bytes = bytesize::ByteSize::b(1);
        })
        .await?;
    assert_skew(&small, &c2).await?;
    Ok(())
}

/// The remote walk stops at the common frontier: a tiny range over deep
/// shared history pops ahead+behind commits, not the shared ancestry. The
/// walk reports a `commits` progress bar per 100 pops, so the SSE envelope
/// of a remotely served compare is the cheapest observable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compare_walk_stops_at_frontier() -> TestResult {
    let big = Server::start().await?;
    big.put_repo("o", "deep").await?;
    let dir = tempfile::tempdir()?.keep();
    git_in(&dir, &["init", "-q", "-b", "main"])?;
    git_in(&dir, &["config", "user.email", "t@t"])?;
    git_in(&dir, &["config", "user.name", "Tester"])?;
    // 120 commits of shared history — more than one progress bar's worth.
    for _ in 0..120 {
        git_in(&dir, &["commit", "-q", "--allow-empty", "-m", "trunk"])?;
    }
    let main_tip = git_in(&dir, &["rev-parse", "HEAD"])?.trim().to_string();
    git_in(&dir, &["checkout", "-q", "-b", "topic"])?;
    git_in(&dir, &["commit", "-q", "--allow-empty", "-m", "one commit on top"])?;
    let topic_tip = git_in(&dir, &["rev-parse", "HEAD"])?.trim().to_string();
    git_in(
        &dir,
        &["push", "-q", "--mirror", &big.repo_url("o", "deep")],
    )?;
    let small = big
        .start_sibling_with(|cfg| {
            cfg.cache.max_bytes = bytesize::ByteSize::b(1);
        })
        .await?;

    let resp = reqwest::Client::new()
        .get(format!("{}/o/deep/api/compare/main...topic", small.base_url))
        .header("Accept", "application/json, text/event-stream")
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    assert!(
        hdr(resp.headers(), "content-type").starts_with("text/event-stream"),
        "remote compare streams"
    );
    let text = resp.text().await?;
    assert!(
        !text.contains("\"unit\":\"commits\""),
        "a 1-commit range must not walk the 120-commit shared ancestry: {text}"
    );
    let result = text
        .split("\n\n")
        .find(|p| p.starts_with("event: result"))
        .expect("result packet");
    let c: Value =
        serde_json::from_str(result.trim_start_matches("event: result\ndata: "))?;
    assert_eq!(c["ahead"], 1);
    assert_eq!(c["behind"], 0);
    assert_eq!(c["merge_base"], main_tip);
    assert_eq!(c["commits"][0]["sha"], topic_tip);
    Ok(())
}
