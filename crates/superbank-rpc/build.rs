// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

fn emit_git_sha() {
    println!("cargo:rerun-if-env-changed=SUPERBANK_GIT_SHA");
    println!("cargo:rerun-if-env-changed=GITHUB_SHA");

    let sha = std::env::var("SUPERBANK_GIT_SHA")
        .ok()
        .or_else(|| std::env::var("GITHUB_SHA").ok())
        .unwrap_or_default();

    let sha = sha.trim();
    if sha.is_empty() {
        return;
    }

    println!("cargo:rustc-env=SUPERBANK_GIT_SHA={sha}");
}

#[cfg(feature = "grpc-streaming")]
fn compile_protos() -> Result<(), Box<dyn std::error::Error>> {
    let protos = ["proto/superbank.proto", "proto/confirmed_block.proto"];
    tonic_prost_build::configure().compile_protos(&protos, &["proto"])?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    emit_git_sha();

    #[cfg(feature = "grpc-streaming")]
    compile_protos()?;

    Ok(())
}
