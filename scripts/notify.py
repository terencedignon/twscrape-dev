#!/usr/bin/env python3
"""
Discord notification helper for scheduled jobs.

Usage:
    notify.py success --job <name> --summary <text> [--duration <secs>] [--host <hostname>]
    notify.py failure --job <name> --summary <text> --exit-code <n> [--duration <secs>] [--host <hostname>]

Environment variables:
    DISCORD_WEBHOOK_STATUS  - webhook URL for success notifications
    DISCORD_WEBHOOK_ALERTS  - webhook URL for failure notifications
"""

import argparse
import os
import socket
import sys

import boto3
from discord_webhook import DiscordWebhook, DiscordEmbed


def get_ssm_parameter(name: str) -> str | None:
    """Fetch a parameter from AWS SSM Parameter Store."""
    try:
        ssm = boto3.client("ssm")
        resp = ssm.get_parameter(Name=name, WithDecryption=True)
        return resp["Parameter"]["Value"]
    except Exception:
        return None


def format_duration(seconds: int) -> str:
    """Format seconds as human-readable duration."""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        mins = seconds // 60
        secs = seconds % 60
        return f"{mins}m {secs}s" if secs else f"{mins}m"
    else:
        hours = seconds // 3600
        mins = (seconds % 3600) // 60
        return f"{hours}h {mins}m" if mins else f"{hours}h"


def send_notification(
    status: str,
    job: str,
    summary: str,
    duration: int | None = None,
    exit_code: int | None = None,
    host: str | None = None,
    tags: list[str] | None = None,
):
    """Send a Discord notification."""
    host = host or socket.gethostname().split(".")[0]

    if status == "success":
        emoji = "✅"
        webhook_url = os.environ.get("DISCORD_WEBHOOK_STATUS") or get_ssm_parameter("/twscrape/discord-webhook-status")
        color = int("57F287", 16)  # green
    elif status == "start":
        emoji = "🚀"
        webhook_url = os.environ.get("DISCORD_WEBHOOK_STATUS") or get_ssm_parameter("/twscrape/discord-webhook-status")
        color = int("5865F2", 16)  # blurple
    else:  # failure
        emoji = "❌"
        webhook_url = os.environ.get("DISCORD_WEBHOOK_ALERTS") or get_ssm_parameter("/twscrape/discord-webhook-alerts")
        color = int("ED4245", 16)  # red
    title = f"{emoji} {job}"

    if not webhook_url:
        print(f"Warning: No webhook URL for {status}", file=sys.stderr)
        return

    summary = (summary or "").strip()
    if not summary:
        summary = "Completed" if status == "success" else "Failed"
    if "\n" in summary:
        summary = f"```\n{summary}\n```"

    webhook = DiscordWebhook(url=webhook_url)

    embed = DiscordEmbed(
        title=title,
        description=summary,
        color=color,
    )

    embed.add_embed_field(name="Host", value=f"`{host}`", inline=True)

    if duration is not None:
        embed.add_embed_field(name="Duration", value=f"`{format_duration(duration)}`", inline=True)

    if exit_code is not None:
        embed.add_embed_field(name="Exit Code", value=f"`{exit_code}`", inline=True)

    # Add searchable tags - derive from job name if not provided
    if tags is None:
        # Auto-generate tag from job name: "system-stats" -> "#stats", "check-accounts" -> "#accounts"
        tag_word = job.split("-")[-1] if "-" in job else job
        tags = [f"#{tag_word}"]
    tag_str = " ".join(tags)
    embed.add_embed_field(name="Tags", value=tag_str, inline=True)

    embed.set_timestamp()
    webhook.add_embed(embed)

    response = webhook.execute()
    if not response.ok:
        print(f"Warning: Discord webhook failed: {response.status_code}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Send Discord notifications")
    parser.add_argument("status", choices=["success", "failure", "start"])
    parser.add_argument("--job", required=True, help="Job name")
    parser.add_argument("--summary", required=True, help="Summary message")
    parser.add_argument("--duration", type=int, help="Duration in seconds")
    parser.add_argument("--exit-code", type=int, help="Exit code (for failures)")
    parser.add_argument("--host", help="Hostname (defaults to current host)")
    parser.add_argument("--tags", help="Searchable tags (comma-separated, e.g. '#backup,#db')")

    args = parser.parse_args()

    tags = None
    if args.tags:
        tags = [t.strip() for t in args.tags.split(",")]

    send_notification(
        status=args.status,
        job=args.job,
        summary=args.summary,
        duration=args.duration,
        exit_code=args.exit_code,
        host=args.host,
        tags=tags,
    )


if __name__ == "__main__":
    main()
