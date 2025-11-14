#!/bin/bash

# Setup automatic cleanup cron jobs
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "⏰ Setting up automatic cleanup cron jobs..."

# Create cron jobs
(crontab -l 2>/dev/null; echo "# CDC Cleanup Jobs") | crontab -
(crontab -l 2>/dev/null; echo "0 2 * * * $SCRIPT_DIR/cleanup.sh >> /var/log/cdc-cleanup.log 2>&1") | crontab -
(crontab -l 2>/dev/null; echo "*/30 * * * * docker system prune -f --volumes >> /var/log/cdc-cleanup.log 2>&1") | crontab -

echo "✅ Cron jobs added:"
echo "  - Daily cleanup at 2 AM"
echo "  - Docker system prune every 30 minutes"
echo ""
echo "📝 View logs: tail -f /var/log/cdc-cleanup.log"
echo "📋 View cron jobs: crontab -l"
