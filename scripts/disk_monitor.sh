#!/bin/bash

# CDC Disk Usage Monitor
echo "📊 CDC Disk Usage Report"
echo "========================"

# System disk usage
echo "💾 System Disk Usage:"
df -h / | tail -1

echo ""
echo "🐳 Docker Space Usage:"
docker system df

echo ""
echo "📦 Container Resource Usage:"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"

echo ""
echo "📁 Data Directory Sizes:"
echo "Kafka data: $(du -sh /tmp/kafka_data 2>/dev/null || echo 'N/A')"
echo "ClickHouse data: $(du -sh /tmp/clickhouse_data 2>/dev/null || echo 'N/A')"
echo "ClickHouse logs: $(du -sh /tmp/clickhouse_logs 2>/dev/null || echo 'N/A')"

echo ""
echo "🔍 Large Docker Log Files:"
find /var/lib/docker/containers -name "*-json.log" -size +10M -exec ls -lh {} \; 2>/dev/null | head -5 || echo "No large log files found"

echo ""
echo "⚠️  Disk Usage Warnings:"
DISK_USAGE=$(df / | tail -1 | awk '{print $5}' | sed 's/%//')
if [ "$DISK_USAGE" -gt 80 ]; then
    echo "🚨 Disk usage is ${DISK_USAGE}% - Consider running cleanup!"
elif [ "$DISK_USAGE" -gt 70 ]; then
    echo "⚠️  Disk usage is ${DISK_USAGE}% - Monitor closely"
else
    echo "✅ Disk usage is ${DISK_USAGE}% - Normal"
fi
