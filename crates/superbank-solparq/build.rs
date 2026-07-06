use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=SUPERBANK_GIT_SHA");
    println!("cargo:rerun-if-env-changed=GITHUB_SHA");
    emit_rerun_for_git_head();

    // Prefer an explicitly provided SHA (CI/release builds), otherwise fall back
    // to asking git directly so local builds also embed the commit hash.
    let sha = std::env::var("SUPERBANK_GIT_SHA")
        .ok()
        .or_else(|| std::env::var("GITHUB_SHA").ok())
        .map(|sha| sha.trim().to_string())
        .filter(|sha| !sha.is_empty())
        .or_else(git_head_sha);

    let version = std::env::var("CARGO_PKG_VERSION").unwrap_or_default();

    match sha {
        Some(sha) => {
            println!("cargo:rustc-env=SUPERBANK_GIT_SHA={sha}");
            let short = &sha[..sha.len().min(12)];
            println!("cargo:rustc-env=SUPERBANK_LONG_VERSION={version} ({short})");
        }
        None => {
            println!("cargo:rustc-env=SUPERBANK_LONG_VERSION={version}");
        }
    }
}

fn git_head_sha() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8(output.stdout).ok()?.trim().to_string();
    if sha.is_empty() { None } else { Some(sha) }
}

/// Rebuild when HEAD moves so the embedded SHA stays in sync during local dev.
fn emit_rerun_for_git_head() {
    let Ok(output) = Command::new("git")
        .args(["rev-parse", "--absolute-git-dir"])
        .output()
    else {
        return;
    };
    if !output.status.success() {
        return;
    }
    if let Ok(git_dir) = String::from_utf8(output.stdout) {
        let git_dir = git_dir.trim();
        if !git_dir.is_empty() {
            println!("cargo:rerun-if-changed={git_dir}/HEAD");
            println!("cargo:rerun-if-changed={git_dir}/packed-refs");
        }
    }
}
