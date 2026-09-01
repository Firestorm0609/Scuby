#!/data/data/com.termux/files/usr/bin/bash
# ==============================================================
# Download a single Panchi NFT image from the VPS to your phone.
#
# Usage:
#   ./get_nft.sh 1        -> downloads 1.jpg
#   ./get_nft.sh 2222     -> downloads 2222.png (the 1/1 mythic)
#   ./get_nft.sh          -> prompts you for a token number
# ==============================================================

VPS_USER="root"
VPS_IP="164.92.134.150"
REMOTE_DIR="/root/hashlips/hashlips_art_engine/build/images_jpg"

# Make sure Termux can actually write to your phone's shared storage.
# Run this once if you haven't already: termux-setup-storage
DEST_DIR="$HOME/storage/downloads/panchi_nfts"
mkdir -p "$DEST_DIR"

TOKEN_ID="$1"
if [ -z "$TOKEN_ID" ]; then
  read -p "Which token number do you want (1-2222)? " TOKEN_ID
fi

# Validate it's a number in range
if ! [[ "$TOKEN_ID" =~ ^[0-9]+$ ]] || [ "$TOKEN_ID" -lt 1 ] || [ "$TOKEN_ID" -gt 2222 ]; then
  echo "Please enter a valid token number between 1 and 2222."
  exit 1
fi

# Token 2222 (the 1/1 mythic) was kept as PNG; everything else is JPG.
if [ "$TOKEN_ID" -eq 2222 ]; then
  FILE_NAME="2222.png"
else
  FILE_NAME="${TOKEN_ID}.jpg"
fi

echo "Downloading ${FILE_NAME} from the VPS..."
scp "${VPS_USER}@${VPS_IP}:${REMOTE_DIR}/${FILE_NAME}" "${DEST_DIR}/${FILE_NAME}"

if [ $? -eq 0 ]; then
  echo "Done. Saved to: ${DEST_DIR}/${FILE_NAME}"
  echo "You'll find it in your phone's Downloads/panchi_nfts folder."
else
  echo "Something went wrong - check the token number and your VPS connection."
fi
