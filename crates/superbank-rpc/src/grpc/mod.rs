// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

pub(crate) mod generated {
    pub(crate) mod superbank {
        tonic::include_proto!("superbank");
    }

    pub(crate) mod confirmed_block {
        #![allow(dead_code)]
        tonic::include_proto!("solana.storage.confirmed_block");
    }
}

pub(crate) mod service;
mod wire;
