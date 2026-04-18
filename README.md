# 🔴 FlowGuard — Work Failure Detector

> Detect missed tasks, delays, ownership gaps, log errors, and escalating communications — before they become disasters.

FlowGuard ingests your tasks (CSV/JSON), logs (any format), and emails (JSON/text), then runs 5 detectors across all of them to surface exactly what's going wrong and who owns it.

---

## Quick Start

```bash
git clone https://github.com/OrbitScript/flowguard
cd flowguard

# Run with built-in demo data
python -m flowguard --demo

# Run against your own data
python -m flowguard --tasks tasks.csv --logs app.log --emails emails.json
```

---

## What It Detects

### 5 Detectors

| Detector | What It Finds |
|---|---|
| **Missed Tasks** | Tasks past deadline, severity scaled by days overdue × priority |
| **Ownership Gaps** | Unassigned tasks, owners with no recent activity in logs or email |
| **Delays / Stalls** | Tasks stuck in the same status too long; approaching deadline with no movement |
| **Log Errors** | Error bursts, repeated error patterns, CRITICAL/FATAL events |
| **Email Escalation** | High-urgency emails, escalating threads, unanswered urgent messages |

### Example Output

```
  💀  CRITICAL (4)
────────────────────────────────────────

  ▸ Missed deadline: Deploy new API version
    Detector: Missed Task  ·  ID: missed_task_001

    Task 'Deploy new API version' is 3.9 days overdue (priority: critical).
    Status is still 'in_progress'.

    Evidence:
      · Deadline: 2025-04-15 00:00
      · Overdue by: 3.9 days
      · Priority: critical

    Owners: alice@co.com

    → Immediately contact owner(s) ['alice@co.com'] to assess current state.

  ▸ Unowned task: Security audit
    Detector: Ownership Gap

    Task 'Security audit' has no assigned owner. It is in_progress with
    priority 'critical'.

    → Assign an owner immediately. Unowned tasks fall through the cracks.
```

---

## Input Formats

### Tasks
```bash
# CSV (flexible column names)
id,title,owner,status,deadline,priority
T-001,Deploy API,alice@co.com,in_progress,2025-06-01,critical

# JSON array
[{"id": "T-001", "title": "Deploy API", "owner": "alice", "deadline": "2025-06-01"}]

# Plain text (GitHub-style)
- [ ] Fix login bug @alice !high due:2025-06-01
- [x] Write tests @bob
```

### Logs (auto-detected)
```
# Python logging, JSON logs, syslog, Apache — all work
2024-03-15 14:30:00,123 ERROR auth: JWT validation failed
{"timestamp": "2024-03-15T14:30:00", "level": "CRITICAL", "message": "DB down"}
Jan 15 14:30:01 hostname service[123]: something failed
```

### Emails
```json
[{
  "from": "alice@co.com",
  "to": ["manager@co.com"],
  "subject": "URGENT: Deployment blocked",
  "body": "This is critical. Need help ASAP.",
  "timestamp": "2024-03-15T10:00:00"
}]
```

---

## CLI Reference

```bash
flowguard --demo                              # Run with sample data
flowguard --tasks tasks.csv                   # Tasks only
flowguard --tasks tasks.json --logs app.log   # Tasks + logs
flowguard --tasks t.csv --logs l.log --emails e.json  # All inputs

flowguard --format json     # JSON output
flowguard --format markdown # Markdown output
flowguard --output report.json  # Save to file
flowguard --window 14       # Analyze last 14 days (default: 30)
flowguard --verbose         # Extra detail per finding
```

---

## Python API

```python
from flowguard import FlowGuardEngine, parse_tasks, parse_logs, parse_emails

tasks  = parse_tasks(open("tasks.csv").read())
logs   = parse_logs(open("app.log").read())
emails = parse_emails(open("emails.json").read())

engine = FlowGuardEngine()
report = engine.analyze(tasks=tasks, logs=logs, emails=emails)

print(f"{report.critical_count} critical, {report.high_count} high")

for finding in report.findings:
    print(f"[{finding.severity.value}] {finding.title}")
    print(f"  → {finding.recommended}")
```

---

## Architecture

```
Inputs (CSV/JSON/logs/emails)
        ↓
    Parsers
        ↓
  Task / LogEntry / EmailRecord objects
        ↓
  FlowGuardEngine.analyze()
    ├── MissedTaskDetector
    ├── OwnershipGapDetector
    ├── DelayDetector
    ├── LogErrorDetector
    └── EmailEscalationDetector
        ↓
  FailureReport (sorted by severity)
        ↓
  TerminalReporter / JsonReporter / MarkdownReporter
```

---

## Zero Dependencies

Pure Python 3.8+ stdlib. No pip installs required.

```bash
python -m flowguard --demo   # works immediately
```

---

## License

MIT
