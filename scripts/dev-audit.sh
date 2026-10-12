#!/usr/bin/env bash
# Development only. Show the directory's own audit log for one DID, one
# line an operation, and what lodestar holds for it.
#   dev-audit.sh <did>
set -euo pipefail
did="$1"
echo "--- the directory's audit log"
curl -sS -m 30 "https://plc.directory/$did/log/audit" | python3 -c '
import json, sys
for op in json.load(sys.stdin):
    o = op["operation"]
    aka = o.get("alsoKnownAs") or ([o.get("handle")] if o.get("handle") else [])
    pds = (o.get("services") or {}).get("atproto_pds", {}).get("endpoint") or o.get("service")
    print(op["createdAt"], "nullified" if op.get("nullified") else "ok       ", o.get("type"), "cid", op["cid"][-8:], "prev", (o.get("prev") or "none")[-8:], aka, pds)
'
echo "--- lodestar's history, newest first (from the API on port ${2:-8087})"
curl -sS -m 10 "http://127.0.0.1:${2:-8087}/account/$did/history?limit=200" | python3 -c '
import json, sys
h = json.load(sys.stdin)
print("status:", h.get("status"))
for e in h.get("entries", []):
    print(e["changed_at"], e["handle_claim"], e["pds"])
'
