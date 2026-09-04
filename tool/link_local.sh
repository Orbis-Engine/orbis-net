#!/bin/bash
# Points this package at a sibling engine checkout instead of the git
# dependency, so a change to the core can be tried here without pushing it.
#
# The cost of separate repositories is exactly this: a cross-repo change needs
# a step a monorepo would not. The step is one command, and the override is not
# committed, so nobody accidentally ships a build wired to a local path.
set -euo pipefail
cd "$(dirname "$0")/.."

ENGINE=${1:-../orbis}
CORE="$ENGINE/packages/orbis_core"

if [ ! -f "$CORE/pubspec.yaml" ]; then
  echo "No engine checkout at $ENGINE."
  echo "Clone Orbis-Engine/orbis beside this one, or pass its path."
  exit 1
fi

cat > pubspec_overrides.yaml <<YAML
# Written by tool/link_local.sh. Not committed.
dependency_overrides:
  orbis_core:
    path: $(cd "$CORE" && pwd)
YAML

dart pub get > /dev/null
echo "orbis_net -> $(cd "$CORE" && pwd)"
