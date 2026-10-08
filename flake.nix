# SPDX-License-Identifier: AGPL-3.0-only
#
# Copyright 2025-2026 Triton One Limited. All rights reserved.
#

{
  description = "Superbank (Rust) dev environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      rust-overlay,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
        };
        lib = pkgs.lib;
        rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

        dockerCli =
          if pkgs.stdenv.isLinux then
            pkgs.docker
          else if builtins.hasAttr "docker-client" pkgs then
            pkgs."docker-client"
          else
            pkgs.docker;

        dockerBuildx =
          if builtins.hasAttr "docker-buildx" pkgs then
            pkgs."docker-buildx"
          else
            null;
      in
      {
        formatter = pkgs.alejandra;

        devShells.default = pkgs.mkShell {
          # Prefer nativeBuildInputs/buildInputs for env var wiring (e.g. PKG_CONFIG_PATH).
          nativeBuildInputs = with pkgs; [
            pkg-config
          ];

          buildInputs = with pkgs; [
            openssl
          ];

          packages =
            with pkgs;
            [
              bashInteractive
              git
              curl
              jq
              gawk
              gnused
              coreutils

              # Compiler and components come from the repository's exact pin.
              rustToolchain
              rust-analyzer

              # Repo scripts/tests.
              python3
              protobuf
              k6
              patchelf

              # Local dev (Tilt + Docker + Kubernetes).
              tilt
              kubectl
              kind
              k9s

              dockerCli
            ]
            ++ lib.optionals (dockerBuildx != null) [ dockerBuildx ];

          shellHook = ''
            # Keep these as warnings so the shell always starts.
            # Only print in interactive shells (avoid noise for `nix develop -c ...`).
            if [ -n "''${PS1:-}" ] && [ -z "''${DOCKER_HOST:-}" ]; then
              docker_sock=""
              if [ -S /var/run/docker.sock ]; then
                docker_sock="/var/run/docker.sock"
              elif [ -n "''${XDG_RUNTIME_DIR:-}" ] && [ -S "''${XDG_RUNTIME_DIR}/docker.sock" ]; then
                docker_sock="''${XDG_RUNTIME_DIR}/docker.sock"
              elif [ -S "''${HOME}/.docker/run/docker.sock" ]; then
                docker_sock="''${HOME}/.docker/run/docker.sock"
              fi

              if [ -z "$docker_sock" ]; then
                cat <<'MSG'
[warn] Docker daemon not detected.
  - This shell provides the Docker CLI, but you still need a running Docker daemon.
  - Install/start Docker on the host (or set DOCKER_HOST).
MSG
              elif [ ! -r "$docker_sock" ] || [ ! -w "$docker_sock" ]; then
                # Common case: /var/run/docker.sock is root:docker 0660.
                # You need to be in the `docker` group (or use sudo) to access it.
                groups=" $(id -nG 2>/dev/null || true) "
                if [ "''${groups}" != *" docker "* ]; then
                  cat <<MSG
[warn] Docker daemon socket exists but is not accessible: $docker_sock
  - Your user is not in the 'docker' group.
  - Fix:
      sudo usermod -aG docker $USER
      newgrp docker    # or log out/in
MSG
                else
                  cat <<MSG
[warn] Docker daemon socket exists but is not accessible: $docker_sock
  - Check filesystem permissions on the socket and that the daemon is healthy.
MSG
                fi
              fi
            fi

            if [ -n "''${PS1:-}" ] && ! kubectl config current-context >/dev/null 2>&1; then
              cat <<'MSG'
[warn] kubectl has no current context.
  - Create a local cluster: kind create cluster
  - Or select an existing context: kubectl config use-context <name>
MSG
            fi
          '';
        };
      }
    );
}
