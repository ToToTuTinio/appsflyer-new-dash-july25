#!/bin/bash

# Show logs in real-time with timestamps, filtering but keeping important requests
echo "Showing important logs (Press Ctrl+C to stop)..."
echo "----------------------------------------"
(tail -f gunicorn.out & tail -f worker.log) | while read line; do
    # Keep important page navigation and API calls
    if echo "$line" | grep -q "GET /api/\|GET /dashboard\|GET /stats\|GET /fraud\|POST /get_stats\|POST /get_fraud\|POST /start-report\|GET /report-status\|POST /event-selections\|GET /active-apps\|POST /clear-apps-cache\|DEBUG in app:\|ERROR in app:\|WARNING in app:\|\[REPORT\]\|\[API\]\|\[FRAUD\]"; then
        # Add timestamp and print the line
        echo "$(date '+%Y-%m-%d %H:%M:%S') $line"
    # Keep non-HTTP request logs (usually application logs)
    elif ! echo "$line" | grep -q "GET\|POST\|PUT\|DELETE\|HEAD\|OPTIONS\|200\|301\|302\|304\|400\|401\|403\|404\|500\|502\|503\|504"; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') $line"
    fi
done 