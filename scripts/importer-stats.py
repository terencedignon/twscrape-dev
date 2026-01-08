#!/usr/bin/env python3
"""
Importer stats reporter - aggregated crawler/importer metrics to Discord.

Queries service_events checkpoints and sends formatted notifications showing:
- Queue depth per importer type
- Import rates (5m/15m/1h windows)
- Rate trends (speeding up/slowing down vs previous period)
- ETA to clear queue
- Error counts

Usage:
    python scripts/importer-stats.py              # Print to stdout
    python scripts/importer-stats.py --notify     # Send to Discord
    python scripts/importer-stats.py --notify --verbose

Designed to run every 15 minutes via systemd timer.
"""

import argparse
import asyncio
import json
import os
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import asyncpg

# Add parent dir to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from pg_config import DatabaseConfig

STATE_FILE = Path("/tmp/importer_stats_state.json")

# Importer types to track
IMPORTER_TYPES = ['network', 'tweet', 'profile', 'notifications', 'media']


def load_previous_state() -> dict:
    """Load previous run state from file."""
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text())
    except Exception:
        pass
    return {}


def save_state(state: dict):
    """Save current state for next run."""
    try:
        STATE_FILE.write_text(json.dumps(state, default=str))
    except Exception:
        pass


def fmt_num(n: int | float) -> str:
    """Format number with K/M suffix."""
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n/1_000:.1f}k"
    return str(int(n))


def fmt_rate(rate: float) -> str:
    """Format rate as jobs/hr."""
    if rate >= 1000:
        return f"{rate/1000:.1f}k/hr"
    return f"{int(rate)}/hr"


def fmt_duration(seconds: float) -> str:
    """Format seconds as human readable duration."""
    if seconds < 0 or seconds > 86400 * 7:  # > 7 days = effectively infinite
        return ">7d"
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds // 60)}m"
    elif seconds < 86400:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h{mins}m" if mins else f"{hours}h"
    else:
        days = int(seconds // 86400)
        hours = int((seconds % 86400) // 3600)
        return f"{days}d{hours}h" if hours else f"{days}d"


def trend_arrow(current: float, previous: float | None, threshold: float = 0.1) -> str:
    """Get trend arrow based on rate change."""
    if previous is None or previous == 0:
        return ""

    pct_change = (current - previous) / previous

    if pct_change > threshold:
        return " ↑"
    elif pct_change < -threshold:
        return " ↓"
    return ""


def delta_str(current: int, previous: int | None) -> str:
    """Format value with delta indicator."""
    if previous is None:
        return str(current)
    delta = current - previous
    if delta > 0:
        return f"{current} (+{delta})"
    elif delta < 0:
        return f"{current} ({delta})"
    return str(current)


async def get_importer_stats(pool: asyncpg.Pool) -> dict:
    """
    Get current importer stats from service_events checkpoints.

    Returns dict keyed by importer type with:
    - queue_pending: current queue depth
    - imported_5m, imported_15m, imported_1h: jobs imported in window
    - rate_5m, rate_15m, rate_1h: extrapolated hourly rates
    - failed_1h: failures in last hour
    - last_checkpoint: timestamp of most recent checkpoint
    """
    stats = {}

    for importer_type in IMPORTER_TYPES:
        service_name = f"{importer_type}_importer"

        # Get most recent checkpoint for this service
        row = await pool.fetchrow("""
            SELECT
                ts,
                details->>'queue_pending' as queue_pending,
                details->>'imported_5m' as imported_5m,
                details->>'imported_15m' as imported_15m,
                details->>'imported_1h' as imported_1h,
                details->>'rate_5m' as rate_5m,
                details->>'failed_1h' as failed_1h
            FROM service_events
            WHERE service = $1
              AND event = 'checkpoint'
              AND ts > NOW() - INTERVAL '30 minutes'
            ORDER BY ts DESC
            LIMIT 1
        """, service_name)

        if row and row['queue_pending']:
            queue = int(row['queue_pending'] or 0)
            imported_5m = int(row['imported_5m'] or 0)
            imported_15m = int(row['imported_15m'] or 0)
            imported_1h = int(row['imported_1h'] or 0)
            rate_5m = int(row['rate_5m'] or 0)
            failed_1h = int(row['failed_1h'] or 0)

            # Calculate rates from different windows
            rate_15m = imported_15m * 4  # 15min * 4 = 1hr
            rate_1h = imported_1h

            # Use 15m rate for ETA (more stable than 5m)
            eta_seconds = (queue / rate_15m * 3600) if rate_15m > 0 else float('inf')

            stats[importer_type] = {
                'queue_pending': queue,
                'imported_5m': imported_5m,
                'imported_15m': imported_15m,
                'imported_1h': imported_1h,
                'rate_5m': rate_5m,
                'rate_15m': rate_15m,
                'rate_1h': rate_1h,
                'failed_1h': failed_1h,
                'eta_seconds': eta_seconds,
                'last_checkpoint': row['ts'].isoformat() if row['ts'] else None,
                'active': True,
            }
        else:
            # No recent checkpoint - importer may be stopped
            # Check queue directly from crawl_jobs
            queue_row = await pool.fetchrow("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'uploaded') as pending,
                    COUNT(*) FILTER (WHERE status = 'imported' AND imported_at > NOW() - INTERVAL '1 hour') as imported_1h
                FROM control.crawl_jobs
                WHERE job_type = $1
            """, importer_type)

            if queue_row:
                stats[importer_type] = {
                    'queue_pending': queue_row['pending'],
                    'imported_5m': 0,
                    'imported_15m': 0,
                    'imported_1h': queue_row['imported_1h'],
                    'rate_5m': 0,
                    'rate_15m': 0,
                    'rate_1h': queue_row['imported_1h'],
                    'failed_1h': 0,
                    'eta_seconds': float('inf') if queue_row['pending'] > 0 else 0,
                    'last_checkpoint': None,
                    'active': False,
                }

    return stats


async def get_global_totals(pool: asyncpg.Pool) -> dict:
    """Get global totals across all job types."""
    row = await pool.fetchrow("""
        SELECT
            COUNT(*) FILTER (WHERE status = 'uploaded') as total_pending,
            COUNT(*) FILTER (WHERE status = 'importing') as total_importing,
            COUNT(*) FILTER (WHERE status = 'imported' AND imported_at > NOW() - INTERVAL '5 minutes') as imported_5m,
            COUNT(*) FILTER (WHERE status = 'imported' AND imported_at > NOW() - INTERVAL '15 minutes') as imported_15m,
            COUNT(*) FILTER (WHERE status = 'imported' AND imported_at > NOW() - INTERVAL '1 hour') as imported_1h,
            COUNT(*) FILTER (WHERE status = 'failed' AND last_error_at > NOW() - INTERVAL '1 hour') as failed_1h
        FROM control.crawl_jobs
    """)

    return {
        'total_pending': row['total_pending'],
        'total_importing': row['total_importing'],
        'imported_5m': row['imported_5m'],
        'imported_15m': row['imported_15m'],
        'imported_1h': row['imported_1h'],
        'failed_1h': row['failed_1h'],
    }


def format_report(stats: dict, totals: dict, prev_state: dict, verbose: bool = False) -> str:
    """Format stats report for Discord."""
    lines = []
    lines.append("```")

    # Per-importer stats
    for importer_type in IMPORTER_TYPES:
        if importer_type not in stats:
            continue

        s = stats[importer_type]
        queue = s['queue_pending']
        rate = s['rate_15m']  # Use 15m rate as primary
        eta = s['eta_seconds']

        # Skip if no queue and no activity
        if queue == 0 and rate == 0 and s['imported_1h'] == 0:
            continue

        # Get previous rate for trend
        prev_rate = prev_state.get(f'{importer_type}_rate_15m')
        trend = trend_arrow(rate, prev_rate)

        # Status indicator
        status = "" if s['active'] else " (stopped)"

        # Format line
        queue_str = fmt_num(queue).rjust(5)
        rate_str = fmt_rate(rate).rjust(8)
        eta_str = fmt_duration(eta).rjust(5) if queue > 0 else "  -  "

        lines.append(f"{importer_type[:8]:<8} {queue_str} queue | {rate_str}{trend} | ETA {eta_str}{status}")

        # Verbose: show window breakdown
        if verbose:
            lines.append(f"         5m:{s['imported_5m']} 15m:{s['imported_15m']} 1h:{s['imported_1h']} fail:{s['failed_1h']}")

    # Separator
    lines.append("-" * 45)

    # Global totals
    total_queue = totals['total_pending']
    total_importing = totals['total_importing']
    total_5m = totals['imported_5m']
    total_15m = totals['imported_15m']
    total_1h = totals['imported_1h']
    total_failed = totals['failed_1h']

    # Rate from 15m window
    total_rate = total_15m * 4
    prev_total_rate = prev_state.get('total_rate_15m')
    total_trend = trend_arrow(total_rate, prev_total_rate)

    # Global ETA
    global_eta = (total_queue / total_rate * 3600) if total_rate > 0 else float('inf')

    lines.append(f"TOTAL    {fmt_num(total_queue).rjust(5)} queue | {fmt_rate(total_rate).rjust(8)}{total_trend} | ETA {fmt_duration(global_eta).rjust(5)}")
    lines.append(f"         5m:{total_5m} 15m:{total_15m} 1h:{total_1h} | importing:{total_importing} failed:{total_failed}")

    lines.append("```")

    return '\n'.join(lines)


def send_notification(summary: str):
    """Send via notify.py."""
    script_dir = Path(__file__).parent
    notify_script = script_dir / "notify.py"
    venv_python = script_dir.parent / ".venv" / "bin" / "python3"

    if not notify_script.exists():
        print("notify.py not found", file=sys.stderr)
        return False

    python = str(venv_python) if venv_python.exists() else "python3"

    try:
        result = subprocess.run(
            [python, str(notify_script), "success",
             "--job", "importer-stats",
             "--summary", summary,
             "--tags", "#importer,#stats"],
            timeout=10,
            env=os.environ,
            capture_output=True,
        )
        return result.returncode == 0
    except Exception as e:
        print(f"Failed to send notification: {e}", file=sys.stderr)
        return False


async def main():
    parser = argparse.ArgumentParser(description="Importer stats reporter")
    parser.add_argument("--notify", "-n", action="store_true", help="Send to Discord")
    parser.add_argument("--verbose", "-v", action="store_true", help="Include detailed breakdown")
    args = parser.parse_args()

    # Source secrets if not in environment
    secrets_file = Path(__file__).parent.parent / "secrets.sh"
    if secrets_file.exists() and 'DISCORD_WEBHOOK_STATUS' not in os.environ:
        try:
            with open(secrets_file) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('export ') and '=' in line:
                        var = line[7:]
                        key, _, value = var.partition('=')
                        value = value.strip('"').strip("'")
                        os.environ[key] = value
        except Exception:
            pass

    # Connect to database
    config = DatabaseConfig()
    pool = await asyncpg.create_pool(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.user,
        password=config.password,
        min_size=1,
        max_size=2,
    )

    try:
        # Load previous state
        prev_state = load_previous_state()

        # Get current stats
        stats = await get_importer_stats(pool)
        totals = await get_global_totals(pool)

        # Format report
        report = format_report(stats, totals, prev_state, verbose=args.verbose)

        # Print to stdout
        print(f"Importer Stats ({datetime.now(timezone.utc).strftime('%H:%M UTC')})")
        print(report)

        # Send notification if requested
        if args.notify:
            # Prepend header for Discord
            discord_msg = f"Importer Stats ({datetime.now(timezone.utc).strftime('%H:%M UTC')})\n{report}"
            if send_notification(discord_msg):
                print("Notification sent")
            else:
                print("Failed to send notification", file=sys.stderr)

        # Save state for next run
        new_state = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'total_rate_15m': totals['imported_15m'] * 4,
        }
        for importer_type, s in stats.items():
            new_state[f'{importer_type}_rate_15m'] = s['rate_15m']
            new_state[f'{importer_type}_queue'] = s['queue_pending']

        save_state(new_state)

    finally:
        await pool.close()


if __name__ == '__main__':
    asyncio.run(main())
