#!/bin/bash
# ==============================================================
# Panchi Collection - Flatten Script (run on VPS)
# --------------------------------------------------------------
# Walks layers/<layer>/<tier>/*.png and moves each file back up
# into layers/<layer>/, renaming it with a #weight suffix so
# HashLips can read rarity directly from the filename.
#
# Weight map:
#   legendary = 2
#   rare      = 4
#   uncommon  = 8
#   common    = 15
#   none      = 15   (only present under earring/face/effect/headgear)
#
# NOTE: "background" has no "mythic" tier processed here - the
# 1/1 "birth of panchi.png" should already live OUTSIDE layers/
# (e.g. Panchi/mythic-1of1/) per the manual-insert plan, so it's
# never picked up by this script or by HashLips.
# ==============================================================

set -e

LAYERS_DIR="/root/Panchi/layers"

declare -A WEIGHTS=(
  [legendary]=2
  [rare]=4
  [uncommon]=8
  [common]=15
  [none]=15
)

TIERS=("legendary" "rare" "uncommon" "common" "none")

echo "===== FLATTENING LAYERS ====="

for layer_path in "$LAYERS_DIR"/*/; do
  layer_name=$(basename "$layer_path")

  # Skip anything that isn't one of the known layers (safety net)
  echo "== $layer_name =="

  for tier in "${TIERS[@]}"; do
    tier_path="${layer_path}${tier}"

    if [ ! -d "$tier_path" ]; then
      continue
    fi

    weight="${WEIGHTS[$tier]}"

    shopt -s nullglob
    for file in "$tier_path"/*.png; do
      filename=$(basename "$file")
      name="${filename%.png}"

      # Avoid double-tagging if a file somehow already has a #weight suffix
      if [[ "$name" == *"#"* ]]; then
        new_name="${name}.png"
      else
        new_name="${name}#${weight}.png"
      fi

      dest="${layer_path}${new_name}"

      if [ -e "$dest" ]; then
        echo "  [SKIP - already exists] $new_name"
        continue
      fi

      mv "$file" "$dest"
      echo "  [$tier -> #${weight}] $filename -> $new_name"
    done
    shopt -u nullglob

    # Remove now-empty tier folder to keep layers/ clean for HashLips
    rmdir "$tier_path" 2>/dev/null || echo "  (tier folder '$tier' not empty, left in place - check leftovers)"
  done

  echo ""
done

echo "===== DONE ====="
echo "Each layer folder should now contain flat files like:"
echo "  layers/earring/stud#15.png"
echo "  layers/earring/none#15.png"
echo ""
echo "Double check no leftover tier folders remain, then run HashLips generation."
