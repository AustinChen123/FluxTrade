#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROTO_DIR="${RITHMIC_PROTO_DIR:-}"
RUST_TOOLCHAIN="${RITHMIC_RUST_TOOLCHAIN:-1.85.0}"

if [[ -z "$PROTO_DIR" || ! -d "$PROTO_DIR" ]]; then
    echo "RITHMIC_PROTO_DIR must point to the external Rithmic proto directory." >&2
    exit 1
fi

if ! rustup run "$RUST_TOOLCHAIN" cargo --version >/dev/null 2>&1; then
    echo "Rust $RUST_TOOLCHAIN is required. Install it with:" >&2
    echo "  rustup toolchain install $RUST_TOOLCHAIN --profile minimal" >&2
    exit 1
fi

cd "$REPO_ROOT/python-strategy"
RUSTUP_TOOLCHAIN="$RUST_TOOLCHAIN" RITHMIC_PROTO_DIR="$PROTO_DIR" \
    uv run --no-sync maturin develop --release \
    --manifest-path ../rust-data-service/Cargo.toml \
    --features rithmic

uv run --no-sync python -c '
import fluxtrade_core

required = (
    "rithmic_ledger_snapshot",
    "RithmicLedgerOrder",
    "RithmicLedgerFill",
    "RithmicLedgerPosition",
    "RithmicLedgerAccountSummary",
    "RithmicLedgerSnapshot",
)
missing = [name for name in required if not hasattr(fluxtrade_core, name)]
if missing:
    raise SystemExit(f"missing Rithmic ledger bindings: {missing}")
print(f"verified Rithmic ledger bindings: {fluxtrade_core.__file__}")
'
