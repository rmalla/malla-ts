#!/bin/bash
# Backup script for malla-ts.com
# Backs up PostgreSQL database + project filesystem to DigitalOcean Spaces
#
# Usage:
#   ./scripts/backup.sh          # full backup (db + files)
#   ./scripts/backup.sh db       # database only
#   ./scripts/backup.sh files    # filesystem only
#
# Cron (daily at 3am):
#   0 3 * * * /var/www/html/malla-ts.com/scripts/backup.sh >> /var/log/mallats-backup.log 2>&1

set -euo pipefail

# --- Config ---
PROJECT_DIR="/var/www/html/malla-ts.com"
BACKUP_BUCKET="s3://nas-vmziyehdkokocizxtsqy/backups/do-malla-ts"
DB_NAME="mallats_db"
DB_USER="mallats_user"
DB_HOST="localhost"
export PGPASSWORD="${DB_PASSWORD:-$(grep -oP 'DB_PASSWORD=\K.*' "$PROJECT_DIR/.env" 2>/dev/null)}"
RETENTION_DAYS=30
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
TMP_DIR=$(mktemp -d /tmp/mallats-backup-XXXX)

trap 'rm -rf "$TMP_DIR"' EXIT

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

MODE="${1:-full}"

# --- Database backup ---
backup_db() {
    log "Starting database backup..."
    local dump_file="$TMP_DIR/mallats_db_${TIMESTAMP}.sql.gz"

    pg_dump -h "$DB_HOST" -U "$DB_USER" "$DB_NAME" \
        --no-owner --no-privileges \
        | gzip -9 > "$dump_file"

    local size
    size=$(du -h "$dump_file" | cut -f1)
    log "Database dump: $size compressed"

    s3cmd put "$dump_file" "${BACKUP_BUCKET}/dbs/" --no-progress
    log "Database backup uploaded to ${BACKUP_BUCKET}/dbs/"
}

# --- Filesystem backup ---
backup_files() {
    log "Starting filesystem backup..."
    local tar_file="$TMP_DIR/mallats_files_${TIMESTAMP}.tar.gz"

    tar czf "$tar_file" \
        -C /var/www/html \
        --exclude='malla-ts.com/venv' \
        --exclude='malla-ts.com/.git' \
        --exclude='malla-ts.com/imports' \
        --exclude='malla-ts.com/archive' \
        --exclude='malla-ts.com/__pycache__' \
        --exclude='*.pyc' \
        --exclude='malla-ts.com/static' \
        malla-ts.com

    local size
    size=$(du -h "$tar_file" | cut -f1)
    log "Filesystem archive: $size compressed"

    s3cmd put "$tar_file" "${BACKUP_BUCKET}/files/" --no-progress
    log "Filesystem backup uploaded to ${BACKUP_BUCKET}/files/"
}

# --- Cleanup old backups ---
cleanup_old() {
    log "Cleaning up backups older than ${RETENTION_DAYS} days..."
    local cutoff
    cutoff=$(date -d "-${RETENTION_DAYS} days" +%Y-%m-%d)

    for prefix in dbs files; do
        s3cmd ls "${BACKUP_BUCKET}/${prefix}/" 2>/dev/null | while read -r line; do
            file_date=$(echo "$line" | awk '{print $1}')
            file_path=$(echo "$line" | awk '{print $4}')
            if [[ -n "$file_date" && -n "$file_path" && "$file_date" < "$cutoff" ]]; then
                log "  Removing old backup: $file_path"
                s3cmd del "$file_path" --no-progress
            fi
        done
    done
    log "Cleanup complete."
}

# --- Run ---
log "=== Malla-TS Backup Started (mode: $MODE) ==="

case "$MODE" in
    db)
        backup_db
        ;;
    files)
        backup_files
        ;;
    full)
        backup_db
        backup_files
        cleanup_old
        ;;
    *)
        echo "Usage: $0 {full|db|files}"
        exit 1
        ;;
esac

log "=== Backup Complete ==="
