# Tiltfile for local Superbank development (Kubernetes)

ci_settings(
    timeout=os.getenv("SUPERBANK_E2E_TILT_TIMEOUT", "65m"),
    readiness_timeout=os.getenv("SUPERBANK_E2E_TILT_READINESS_TIMEOUT", "35m"),
)

# Optional local registry (e.g., Kind local registry at localhost:5001).
local_registry_host = os.getenv("LOCAL_REGISTRY_HOST", "")
local_registry_host_from_cluster = os.getenv("LOCAL_REGISTRY_HOST_FROM_CLUSTER", "")
if local_registry_host:
    if local_registry_host_from_cluster:
        default_registry(
            local_registry_host,
            host_from_cluster=local_registry_host_from_cluster,
        )
    else:
        default_registry(local_registry_host)

DEFAULT_IMAGE_REPO = "superbank-dev"
image_repo = os.getenv("SUPERBANK_IMAGE_REPO", DEFAULT_IMAGE_REPO)

DEFAULT_NAMESPACE = "superbank-dev"
namespace = os.getenv("SUPERBANK_NAMESPACE", DEFAULT_NAMESPACE)

ingest_mode = os.getenv("SUPERBANK_INGEST_MODE", "rpc")


def _yaml_with_overrides(path):
    # Tilt >= 0.36 returns a Blob (not a plain string). Cast to string for string ops.
    y = "{}".format(read_file(path))
    if namespace != DEFAULT_NAMESPACE:
        y = y.replace("namespace: {}".format(DEFAULT_NAMESPACE), "namespace: {}".format(namespace))
        y = y.replace("name: {}".format(DEFAULT_NAMESPACE), "name: {}".format(namespace))
    if image_repo != DEFAULT_IMAGE_REPO:
        y = y.replace("image: {}".format(DEFAULT_IMAGE_REPO), "image: {}".format(image_repo))
    if path == "deploy/k8s/30-superbank-ingest-rpc-job.yaml":
        y = _apply_rpc_ingest_overrides(y)
    # k8s_yaml treats strings as file paths; wrap YAML content as a Blob.
    return blob(y)


def _quote_yaml_string(value):
    return value.replace("\\", "\\\\").replace("\"", "\\\"")


def _replace_env_value(y, name, default, value):
    if not value:
        return y
    old = "        - name: {}\n          value: \"{}\"".format(name, default)
    new = "        - name: {}\n          value: \"{}\"".format(name, _quote_yaml_string(value))
    return y.replace(old, new)


def _apply_rpc_ingest_overrides(y):
    y = _replace_env_value(
        y,
        "RPC_URL",
        "https://api.mainnet-beta.solana.com",
        os.getenv("SUPERBANK_INGEST_RPC_URL", ""),
    )
    y = _replace_env_value(
        y,
        "RPC_FROM_SLOT",
        "0",
        os.getenv("SUPERBANK_INGEST_RPC_FROM_SLOT", ""),
    )
    y = _replace_env_value(
        y,
        "RPC_SLOT_COUNT",
        "5000",
        os.getenv("SUPERBANK_INGEST_SLOT_COUNT", ""),
    )
    y = _replace_env_value(
        y,
        "RPC_MAX_INFLIGHT",
        "4",
        os.getenv("SUPERBANK_INGEST_RPC_MAX_INFLIGHT", ""),
    )
    y = _replace_env_value(
        y,
        "RPC_RETRY_BACKOFF_MS",
        "1000",
        os.getenv("SUPERBANK_INGEST_RPC_RETRY_BACKOFF_MS", ""),
    )
    return y


def _indent_block(text, spaces):
    prefix = " " * spaces
    # Tilt >= 0.36 returns file contents as a Blob, which does not support string methods.
    lines = "{}".format(text).splitlines()
    if len(lines) == 0:
        return prefix
    return "\n".join([prefix + l for l in lines])


clickhouse_ddl_configmap_yaml = blob(
"""
apiVersion: v1
kind: ConfigMap
metadata:
  name: clickhouse-ddl
  namespace: {namespace}
data:
  transactions.sql: |
{transactions}
  blocks_metadata.sql: |
{blocks}
  entries.sql: |
{entries}
  gsfa.sql: |
{gsfa}
  gsfa_hot.sql: |
{gsfa_hot}
  signatures.sql: |
{signatures}
  token_owner_activity.sql: |
{token_owner_activity}
""".format(
    namespace=namespace,
    transactions=_indent_block(read_file("ddl/local/transactions.sql"), 4),
    blocks=_indent_block(read_file("ddl/local/blocks_metadata.sql"), 4),
    entries=_indent_block(read_file("ddl/local/entries.sql"), 4),
    gsfa=_indent_block(read_file("ddl/local/gsfa.sql"), 4),
    gsfa_hot=_indent_block(read_file("ddl/local/gsfa_hot.sql"), 4),
    signatures=_indent_block(read_file("ddl/local/signatures.sql"), 4),
    token_owner_activity=_indent_block(read_file("ddl/local/token_owner_activity.sql"), 4),
)
)


# Build binaries locally (fast incremental) and stage them for the runtime image.
local_resource(
    "superbank-build",
    cmd="""
set -eu
cargo build --release -p superbank -p superbank-rpc
rm -rf dist
mkdir -p dist
cp -f target/release/superbank dist/
cp -f target/release/superbank-rpc dist/

# When built inside a Nix dev shell, Rust binaries can end up referencing a
# /nix/store dynamic linker. Our runtime image is Ubuntu, so we patch the
# interpreter to a standard location so the binaries can actually exec.
if command -v patchelf >/dev/null 2>&1; then
  for b in dist/superbank dist/superbank-rpc; do
    interp="$(patchelf --print-interpreter "$b" 2>/dev/null || true)"
    if [ -n "${interp}" ] && printf '%s' "${interp}" | grep -q '^/nix/store/'; then
      echo "[superbank-build] Patching interpreter for ${b} (${interp} -> /lib64/ld-linux-x86-64.so.2)"
      patchelf --set-interpreter /lib64/ld-linux-x86-64.so.2 "$b"
    fi
  done
else
  if command -v file >/dev/null 2>&1 && file dist/superbank dist/superbank-rpc 2>/dev/null | grep -q '/nix/store/'; then
    cat >&2 <<'MSG'
error: patchelf is required to run Nix-built binaries in the Ubuntu runtime image.
  - Run Tilt inside the Nix dev shell (provides patchelf), e.g.:
      nix develop -c tilt up --stream
  - Or install patchelf and re-run.
MSG
    exit 1
  fi
fi
""",
    deps=[
        "Cargo.toml",
        "Cargo.lock",
        "rust-toolchain.toml",
        "crates",
    ],
)

# Build the runtime image from staged artifacts.
docker_build(
    image_repo,
    ".",
    dockerfile="deploy/docker/Dockerfile.superbank-dev-runtime",
    only=["dist", "deploy/docker/Dockerfile.superbank-dev-runtime"],
)

manifest_files = [
    "deploy/k8s/10-clickhouse.yaml",
    "deploy/k8s/11-clickhouse-ddl-job.yaml",
    "deploy/k8s/20-superbank-rpc.yaml",
]

yamls = []
yamls.append(_yaml_with_overrides("deploy/k8s/00-namespace.yaml"))

clickhouse_user = os.getenv("SUPERBANK_CLICKHOUSE_USER", "default")
clickhouse_password = os.getenv("SUPERBANK_CLICKHOUSE_PASSWORD", "superbank")

# Store ClickHouse creds in a Secret so they don't end up in plain manifests.
yamls.append(
    blob(
        """
apiVersion: v1
kind: Secret
metadata:
  name: superbank-clickhouse
  namespace: {namespace}
type: Opaque
stringData:
  CLICKHOUSE_USER: |-
{user}
  CLICKHOUSE_PASSWORD: |-
{password}
""".format(
            namespace=namespace,
            user=_indent_block(clickhouse_user, 4),
            password=_indent_block(clickhouse_password, 4),
        )
    )
)

if ingest_mode == "grpc":
    dm_endpoint = os.getenv("DRAGONSMOUTH_ENDPOINT", "")
    dm_token = os.getenv("DRAGONSMOUTH_X_TOKEN", "")
    if not dm_endpoint:
        fail("SUPERBANK_INGEST_MODE=grpc requires DRAGONSMOUTH_ENDPOINT")

    # Store gRPC creds in a Secret so they don't end up in plain manifests.
    yamls.append(
        blob(
            """
apiVersion: v1
kind: Secret
metadata:
  name: superbank-dragonsmouth
  namespace: {namespace}
type: Opaque
stringData:
  DRAGONSMOUTH_ENDPOINT: |-
{endpoint}
  DRAGONSMOUTH_X_TOKEN: |-
{token}
""".format(
                namespace=namespace,
                endpoint=_indent_block(dm_endpoint, 4),
                token=_indent_block(dm_token, 4),
            )
        )
    )
    manifest_files.append("deploy/k8s/31-superbank-ingest-grpc.yaml")
else:
    manifest_files.append("deploy/k8s/30-superbank-ingest-rpc-job.yaml")

yamls.append(clickhouse_ddl_configmap_yaml)
yamls.extend([_yaml_with_overrides(p) for p in manifest_files])
for y in yamls:
    k8s_yaml(y)

k8s_resource(
    "clickhouse",
    port_forwards=[8123, 9000],
)
k8s_resource(
    "clickhouse-ddl",
    resource_deps=["clickhouse"],
)
k8s_resource(
    "superbank-rpc",
    port_forwards=[8899],
    resource_deps=["superbank-build", "clickhouse-ddl"],
)
if ingest_mode == "grpc":
    k8s_resource(
        "superbank-ingest-grpc",
        resource_deps=["superbank-build", "clickhouse-ddl"],
    )
else:
    k8s_resource(
        "superbank-ingest-rpc",
        resource_deps=["superbank-build", "clickhouse-ddl"],
        pod_readiness="ignore",
    )

# Manual action for re-applying ClickHouse DDL after changing files in ddl/.
local_resource(
    "apply-clickhouse-schema",
    cmd="""
set -eu

ns="${SUPERBANK_NAMESPACE:-superbank-dev}"
ch_user="${SUPERBANK_CLICKHOUSE_USER:-default}"
ch_password="${SUPERBANK_CLICKHOUSE_PASSWORD:-superbank}"

kubectl -n "$ns" wait --for=condition=Ready pod -l app=clickhouse --timeout=300s
pod="$(kubectl -n "$ns" get pod -l app=clickhouse -o jsonpath='{.items[0].metadata.name}')"

for f in \\
  ddl/local/transactions.sql \\
  ddl/local/blocks_metadata.sql \\
  ddl/local/entries.sql \\
  ddl/local/gsfa.sql \\
  ddl/local/gsfa_hot.sql \\
  ddl/local/signatures.sql \\
  ddl/local/token_owner_activity.sql
do
  echo "[apply-clickhouse-schema] Applying $f..."
  cat "$f" | kubectl -n "$ns" exec -i "$pod" -c clickhouse -- clickhouse-client --user "$ch_user" --password "$ch_password" --multiquery
done

echo "[apply-clickhouse-schema] Done."
""",
    deps=[
        "ddl/local/transactions.sql",
        "ddl/local/blocks_metadata.sql",
        "ddl/local/entries.sql",
        "ddl/local/gsfa.sql",
        "ddl/local/gsfa_hot.sql",
        "ddl/local/signatures.sql",
        "ddl/local/token_owner_activity.sql",
    ],
    resource_deps=["clickhouse"],
    trigger_mode=TRIGGER_MODE_MANUAL,
    auto_init=False,
)
