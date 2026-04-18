#!/usr/bin/env python3
"""
flowguard/cli.py  →  python -m flowguard
─────────────────────────────────────────
Usage:
  python -m flowguard --tasks tasks.csv --logs app.log --emails emails.json
  python -m flowguard --tasks tasks.json --format json
  python -m flowguard --tasks tasks.csv --output report.md --format markdown
  python -m flowguard --demo
"""

import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import flowguard
from flowguard import FlowGuardEngine, parse_tasks, parse_logs, parse_emails
from flowguard.reporter import TerminalReporter, JsonReporter, MarkdownReporter


def main():
    parser = argparse.ArgumentParser(
        prog="flowguard",
        description="FlowGuard — Work Failure Detector",
    )
    parser.add_argument("--tasks",   help="Tasks file (CSV or JSON)")
    parser.add_argument("--logs",    help="Log file (any common format)")
    parser.add_argument("--emails",  help="Emails file (JSON or plain text)")
    parser.add_argument("--output",  help="Save report to this file")
    parser.add_argument("--format",  choices=["terminal","json","markdown"],
                        default="terminal", help="Output format (default: terminal)")
    parser.add_argument("--window",  type=float, default=30,
                        help="Analysis window in days (default: 30)")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--demo",    action="store_true", help="Run with sample data")
    parser.add_argument("--version", action="store_true")
    args = parser.parse_args()

    if args.version:
        print(f"flowguard {flowguard.__version__}")
        return

    tasks, logs, emails = [], [], []

    if args.demo:
        tasks, logs, emails = _load_demo_data()
    else:
        if args.tasks:
            content = Path(args.tasks).read_text()
            hint    = "csv" if args.tasks.endswith(".csv") else "json" if args.tasks.endswith(".json") else "auto"
            tasks   = parse_tasks(content, hint=hint)
            print(f"  Loaded {len(tasks)} tasks from {args.tasks}")

        if args.logs:
            content = Path(args.logs).read_text()
            logs    = parse_logs(content)
            print(f"  Loaded {len(logs)} log entries from {args.logs}")

        if args.emails:
            content = Path(args.emails).read_text()
            hint    = "json" if args.emails.endswith(".json") else "auto"
            emails  = parse_emails(content, hint=hint)
            print(f"  Loaded {len(emails)} emails from {args.emails}")

    if not tasks and not logs and not emails:
        print("\n  No inputs provided. Use --tasks, --logs, --emails, or --demo\n")
        parser.print_help()
        return

    engine = FlowGuardEngine()
    report = engine.analyze(tasks=tasks, logs=logs, emails=emails,
                             window_days=args.window)

    if args.format == "json":
        reporter = JsonReporter()
        if args.output:
            reporter.save(report, args.output)
        else:
            reporter.print(report)
    elif args.format == "markdown":
        reporter = MarkdownReporter()
        if args.output:
            reporter.save(report, args.output)
            print(f"  Saved markdown report to: {args.output}")
        else:
            reporter.print(report)
    else:
        TerminalReporter(verbose=args.verbose).print(report)
        if args.output:
            if args.output.endswith(".json"):
                JsonReporter().save(report, args.output)
            elif args.output.endswith(".md"):
                MarkdownReporter().save(report, args.output)
            else:
                JsonReporter().save(report, args.output)


def _load_demo_data():
    """Load built-in sample data for demo."""
    from flowguard.parsers import parse_tasks_csv, parse_logs, parse_emails_json
    from datetime import datetime, timedelta

    now = datetime.now()

    tasks_csv = f"""id,title,owner,status,priority,deadline,created_at
TASK-001,Deploy new API version,alice@co.com,in_progress,critical,{(now - timedelta(days=3)).strftime('%Y-%m-%d')},{(now - timedelta(days=14)).strftime('%Y-%m-%d')}
TASK-002,Write migration scripts,,pending,high,{(now - timedelta(days=1)).strftime('%Y-%m-%d')},{(now - timedelta(days=10)).strftime('%Y-%m-%d')}
TASK-003,Update documentation,bob@co.com,pending,medium,{(now + timedelta(days=5)).strftime('%Y-%m-%d')},{(now - timedelta(days=20)).strftime('%Y-%m-%d')}
TASK-004,Fix authentication bug,carol@co.com,blocked,critical,{(now - timedelta(days=5)).strftime('%Y-%m-%d')},{(now - timedelta(days=8)).strftime('%Y-%m-%d')}
TASK-005,Performance testing,dave@co.com,pending,high,{(now + timedelta(days=2)).strftime('%Y-%m-%d')},{(now - timedelta(days=7)).strftime('%Y-%m-%d')}
TASK-006,Security audit,,in_progress,critical,{(now - timedelta(days=2)).strftime('%Y-%m-%d')},{(now - timedelta(days=30)).strftime('%Y-%m-%d')}
TASK-007,Database backup verification,alice@co.com,done,high,{(now + timedelta(days=1)).strftime('%Y-%m-%d')},{(now - timedelta(days=5)).strftime('%Y-%m-%d')}
"""

    logs_text = f"""{(now - timedelta(hours=48)).strftime('%Y-%m-%d %H:%M:%S')} ERROR auth-service: Failed to validate JWT token for user_id=1042
{(now - timedelta(hours=47)).strftime('%Y-%m-%d %H:%M:%S')} ERROR auth-service: Database connection timeout after 30s
{(now - timedelta(hours=47, minutes=1)).strftime('%Y-%m-%d %H:%M:%S')} ERROR auth-service: Failed to validate JWT token for user_id=1043
{(now - timedelta(hours=47, minutes=2)).strftime('%Y-%m-%d %H:%M:%S')} ERROR auth-service: Failed to validate JWT token for user_id=1044
{(now - timedelta(hours=47, minutes=3)).strftime('%Y-%m-%d %H:%M:%S')} ERROR auth-service: Failed to validate JWT token for user_id=1045
{(now - timedelta(hours=47, minutes=4)).strftime('%Y-%m-%d %H:%M:%S')} ERROR auth-service: Failed to validate JWT token for user_id=1046
{(now - timedelta(hours=20)).strftime('%Y-%m-%d %H:%M:%S')} INFO deploy-service: Starting deployment pipeline for v2.1.0
{(now - timedelta(hours=19)).strftime('%Y-%m-%d %H:%M:%S')} WARN deploy-service: Migration step 3 taking longer than expected
{(now - timedelta(hours=18)).strftime('%Y-%m-%d %H:%M:%S')} ERROR deploy-service: Migration failed: constraint violation on table users
{(now - timedelta(hours=5)).strftime('%Y-%m-%d %H:%M:%S')} CRITICAL payment-service: Payment processing halted — stripe webhook verification failed
{(now - timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')} ERROR auth-service: Failed to validate JWT token for user_id=2001
{(now - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')} INFO monitoring: Health check passed for api-gateway
"""

    emails_json = json.dumps([
        {
            "timestamp": (now - timedelta(hours=72)).isoformat(),
            "from": "alice@co.com",
            "to": ["team@co.com"],
            "subject": "URGENT: API deployment blocked — need help immediately",
            "body": "Hi team, the API deployment is critically blocked. We missed the deadline and the client is asking for an urgent update. This needs to be resolved ASAP or we will escalate to management.",
        },
        {
            "timestamp": (now - timedelta(hours=48)).isoformat(),
            "from": "manager@co.com",
            "to": ["alice@co.com", "bob@co.com"],
            "subject": "Re: URGENT: API deployment blocked — need help immediately",
            "body": "Alice, Bob - this is urgent. Please provide a status update by EOD. The client is waiting and this is a critical blocker for the whole project.",
            "is_reply": True,
        },
        {
            "timestamp": (now - timedelta(hours=24)).isoformat(),
            "from": "client@external.com",
            "to": ["manager@co.com"],
            "subject": "Re: URGENT: API deployment blocked — need help immediately",
            "body": "We have been waiting 3 days for this deployment. This is completely unacceptable. If this is not resolved immediately we will escalate to your executive team. This is a critical emergency for our operations.",
            "is_reply": True,
        },
        {
            "timestamp": (now - timedelta(hours=12)).isoformat(),
            "from": "carol@co.com",
            "to": ["manager@co.com"],
            "subject": "Auth bug still not fixed — overdue",
            "body": "Just a reminder that the authentication bug (TASK-004) is still blocked. I've been waiting for the security team to unblock me. This task was supposed to be done 5 days ago.",
        },
        {
            "timestamp": (now - timedelta(hours=6)).isoformat(),
            "from": "bob@co.com",
            "to": ["alice@co.com"],
            "subject": "Migration scripts — who owns this?",
            "body": "Hi Alice, I noticed TASK-002 (migration scripts) has no owner assigned. Do you know who is responsible? The deadline was yesterday.",
        },
    ])

    tasks  = parse_tasks_csv(tasks_csv)
    logs   = parse_logs(logs_text)
    emails = parse_emails_json(emails_json)
    return tasks, logs, emails


if __name__ == "__main__":
    main()
