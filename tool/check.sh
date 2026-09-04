#!/bin/bash
# Analyzes and tests the package.
set -uo pipefail
cd "$(dirname "$0")/.."

failures=0

if dart analyze > /tmp/orbis_net_analyze.log 2>&1; then
  echo "  ok    analyze"
else
  echo "  FAIL  analyze"; tail -20 /tmp/orbis_net_analyze.log; failures=$((failures+1))
fi

if dart test > /tmp/orbis_net_test.log 2>&1; then
  summary=$(tr '\r' '\n' < /tmp/orbis_net_test.log | tail -1 \
    | sed -e 's/\x1b\[[0-9;]*m//g' -e 's/^[0-9:]* //')
  echo "  ok    $summary"
else
  echo "  FAIL  tests"; tail -25 /tmp/orbis_net_test.log; failures=$((failures+1))
fi

if dart format --output=none --set-exit-if-changed lib test > /dev/null 2>&1; then
  echo "  ok    format"
else
  echo "  FAIL  format"; failures=$((failures+1))
fi

echo
[ "$failures" -eq 0 ] && echo "everything green" || echo "$failures failing step(s)"
exit "$failures"
