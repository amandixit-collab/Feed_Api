#!/usr/bin/env bash
set -euo pipefail

# --------------------------------------------------
# Logging setup (ADDED)
# --------------------------------------------------
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"

RUN_TS=$(date +%s)
LOG_FILE="${LOG_DIR}/analyze_feed_partner_${1:-unknown}_${RUN_TS}.log"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "======================================"
echo "Script started at: $(date)"
echo "Command: $0 $*"
echo "Log file: $LOG_FILE"
echo "======================================"

# --------------------------------------------------
# Arguments
# --------------------------------------------------
if [[ $# -lt 4 ]]; then
  echo "Usage:"
  echo "$0 <partner_id> <s3_feed_file> <s3_output_path> <distinguish_id>"
  exit 1
fi

PARTNER_ID="$1"
S3_FEED_FILE="$2"
S3_OUTPUT_PATH="$3"
DISTINGUISH_ID="$4"

TIMESTAMP=$(date +%s)
GENERATED_AT=$(date)

TMP_FEED="/tmp/${PARTNER_ID}_feed_${TIMESTAMP}.xml.gz"
TMP_STATS="/tmp/${PARTNER_ID}_analyzed_feed_file_${DISTINGUISH_ID}_${TIMESTAMP}.txt"

FINAL_S3_FILE="${S3_OUTPUT_PATH%/}/${PARTNER_ID}_analyzed_feed_file_${DISTINGUISH_ID}_${TIMESTAMP}.txt"

echo "Partner ID: $PARTNER_ID"
echo "Input feed: $S3_FEED_FILE"
echo "Output path: $S3_OUTPUT_PATH"
echo "Distinguish ID: $DISTINGUISH_ID"
echo "--------------------------------------"

# --------------------------------------------------
# Download feed
# --------------------------------------------------
echo "⬇️ Downloading feed from S3..."
aws s3 cp "$S3_FEED_FILE" "$TMP_FEED"
echo "✅ Download completed: $TMP_FEED"

FILTER_CMD="zcat \"$TMP_FEED\" |
  grep '<product_inStock>true</product_inStock>' |
  grep '<variation_status>AVAILABLE</variation_status>' |
  grep '<visible>true</visible>' |
  grep '<product_status>AVAILABLE</product_status>' |
  grep '<product_publishing_status>PUBLISHED</product_publishing_status>'"

# --------------------------------------------------
# Generate stats
# --------------------------------------------------
echo "📊 Generating stats..."

{
  echo "Partner ID: $PARTNER_ID"
  echo "File: $(basename "$S3_FEED_FILE")"
  echo "Distinguish ID: $DISTINGUISH_ID"
  echo "Generated at: $GENERATED_AT"
  echo "======================================"

  echo "Total Rows:"
  eval "$FILTER_CMD" | wc -l || true

  echo
  echo "Gender count:"
  eval "$FILTER_CMD" | grep -c '</Gender>' || true

  echo
  echo "Size count:"
  eval "$FILTER_CMD" | grep -i -E -c '</Size>|<key>size</key>|<key>siblings_size</key>|<key>variation_size</key>' || true

  echo
  echo "Color count:"
  eval "$FILTER_CMD" | grep -i -E -c '</Color>|<key>color</key>|<key>siblings_color</key>|<key>variation_color</key>' || true

  echo
  echo "Current Price count:"
  eval "$FILTER_CMD" | grep -c '</price>' || true

  echo
  echo "List Price count:"
  eval "$FILTER_CMD" | grep -c '</list_price>' || true

  echo
  echo "Distinct zzcategory count stats:"
  eval "$FILTER_CMD" \
    | awk -F '<zcategories>' '{print $2}' \
    | awk -F '</zcategories>' '{print $1}' \
    | sort | uniq -c || true

  echo
  echo "Stock availability count stats:"
  eval "$FILTER_CMD" \
    | awk -F '<availability>' '{print $2}' \
    | awk -F '</availability>' '{print $1}' \
    | sort | uniq -c || true

  echo
  echo "Url count:"
  eval "$FILTER_CMD" | grep -c '</url>' || true

  echo
  echo "Image count:"
  eval "$FILTER_CMD" | grep -c '</photo>' || true

  echo
  echo "Extra Image count:"
  eval "$FILTER_CMD" | grep -c '</additional_image_urls>' || true

  echo
  echo "Few Url links:"
  eval "$FILTER_CMD" \
    | awk -F '<url>' '{print $2}' \
    | awk -F '</url>' '{print $1}' \
    | head || true

  echo
  echo "Few image links:"
  eval "$FILTER_CMD" \
    | awk -F '<photo>' '{print $2}' \
    | awk -F '</photo>' '{print $1}' \
    | head || true

  echo
  echo "First 20 product names:"
  eval "$FILTER_CMD" \
    | awk -F '<name>' '{print $2}' \
    | awk -F '</name>' '{print $1}' \
    | head -20 || true

  echo
  echo "Few distinct brands:"
  eval "$FILTER_CMD" \
    | awk -F '<brand>' '{print $2}' \
    | awk -F '</brand>' '{print $1}' \
    | sort | uniq | head || true

} > "$TMP_STATS"

echo "✅ Stats file created: $TMP_STATS"

# --------------------------------------------------
# Upload to S3
# --------------------------------------------------
echo "☁️ Uploading stats to S3..."
aws s3 cp "$TMP_STATS" "$FINAL_S3_FILE"
echo "✅ Upload completed: $FINAL_S3_FILE"

# --------------------------------------------------
# Cleanup
# --------------------------------------------------
echo "🧹 Cleaning up temporary files..."
rm -f "$TMP_FEED" "$TMP_STATS"
echo "✅ Cleanup completed"

echo "======================================"
echo "Script finished successfully at: $(date)"
echo "======================================"
