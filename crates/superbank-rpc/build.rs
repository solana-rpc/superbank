// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

#[cfg(feature = "grpc-streaming")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = ["proto/superbank.proto", "proto/confirmed_block.proto"];
    tonic_prost_build::configure().compile_protos(&protos, &["proto"])?;
    Ok(())
}

#[cfg(not(feature = "grpc-streaming"))]
fn main() {}
