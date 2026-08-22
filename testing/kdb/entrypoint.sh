#!/usr/bin/env bash
set -euo pipefail

secret_path=/run/secrets/kdb-license-b64
if [[ ! -s "$secret_path" ]]; then
    echo "KDB license secret is missing or empty: $secret_path" >&2
    exit 1
fi

umask 077
license_dir=/run/kdb-license
install -d -m 0700 "$license_dir"
if ! base64 --decode "$secret_path" >"$license_dir/kc.lic"; then
    rm -f "$license_dir/kc.lic"
    echo "KDB license secret is not valid base64" >&2
    exit 1
fi

export QLIC="$license_dir"
exec q "$@"
