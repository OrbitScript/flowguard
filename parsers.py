"""
flowguard/parsers.py
─────────────────────
Input parsers: convert raw files into Task, LogEntry, EmailRecord objects.

Supported formats:
  Tasks:  CSV, JSON (array or object-per-line), plain text
  Logs:   Standard log formats (Apache/nginx-style, Python logging, syslog, JSON)
  Emails: JSON array, mbox-lite, plain text
"""

from __future__ import annotations

import re
import csv
import json
import io
from datetime import datetime
from typing import List, Optional, Dict, Any

from flowguard.engine import (
    Task, TaskStatus, LogEntry, EmailRecord
)


# ─── Date Parsing ──────────────────────────────────────────────────────────────

_DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y",
    "%d %b %Y",
    "%b %d %Y",
    "%b %d %H:%M:%S",   # syslog style
]

def parse_dt(s: str, default: Optional[datetime] = None) -> Optional[datetime]:
    """Try multiple date formats and return the first that parses."""
    if not s or not isinstance(s, str):
        return default
    s = s.strip()
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            # Fix year for syslog-style (no year)
            if dt.year == 1900:
                dt = dt.replace(year=datetime.now().year)
            return dt
        except ValueError:
            continue
    return default


def _normalize_status(s: str) -> TaskStatus:
    s = (s or "").lower().strip()
    mapping = {
        "done": TaskStatus.DONE,
        "complete": TaskStatus.DONE, "completed": TaskStatus.DONE,
        "closed": TaskStatus.DONE, "resolved": TaskStatus.DONE,
        "in_progress": TaskStatus.IN_PROGRESS,
        "in progress": TaskStatus.IN_PROGRESS,
        "wip": TaskStatus.IN_PROGRESS, "active": TaskStatus.IN_PROGRESS,
        "started": TaskStatus.IN_PROGRESS, "doing": TaskStatus.IN_PROGRESS,
        "blocked": TaskStatus.BLOCKED, "on hold": TaskStatus.BLOCKED,
        "waiting": TaskStatus.BLOCKED,
        "cancelled": TaskStatus.CANCELLED, "canceled": TaskStatus.CANCELLED,
        "dropped": TaskStatus.CANCELLED,
        "pending": TaskStatus.PENDING, "open": TaskStatus.PENDING,
        "todo": TaskStatus.PENDING, "to do": TaskStatus.PENDING,
        "new": TaskStatus.PENDING, "backlog": TaskStatus.PENDING,
    }
    return mapping.get(s, TaskStatus.UNKNOWN)


# ─── Task Parsers ─────────────────────────────────────────────────────────────

def parse_tasks_csv(content: str) -> List[Task]:
    """
    Parse tasks from CSV.
    Expected columns (flexible — tries to match by name):
      id, title/name/task, owner/assigned_to/assignee, status,
      deadline/due_date/due, created_at/created, priority
    """
    tasks = []
    reader = csv.DictReader(io.StringIO(content.strip()))

    COL_ALIASES = {
        "id":           ["id", "task_id", "ticket", "issue"],
        "title":        ["title", "name", "task", "description", "summary"],
        "owner":        ["owner", "assigned_to", "assignee", "responsible"],
        "status":       ["status", "state", "stage"],
        "deadline":     ["deadline", "due_date", "due", "due_by", "end_date", "target_date"],
        "created_at":   ["created_at", "created", "date_created", "start_date"],
        "updated_at":   ["updated_at", "updated", "last_updated", "modified"],
        "priority":     ["priority", "prio", "importance", "urgency"],
        "assignees":    ["assignees", "team", "collaborators"],
    }

    def find_col(row: dict, aliases: list) -> Optional[str]:
        lower_row = {k.lower().strip(): v for k, v in row.items()}
        for a in aliases:
            if a in lower_row:
                return lower_row[a]
        return None

    for i, row in enumerate(reader):
        task_id    = find_col(row, COL_ALIASES["id"])     or str(i + 1)
        title      = find_col(row, COL_ALIASES["title"])  or "(untitled)"
        owner      = find_col(row, COL_ALIASES["owner"])
        status_str = find_col(row, COL_ALIASES["status"]) or "pending"
        deadline   = parse_dt(find_col(row, COL_ALIASES["deadline"]) or "")
        created    = parse_dt(find_col(row, COL_ALIASES["created_at"]) or "")
        updated    = parse_dt(find_col(row, COL_ALIASES["updated_at"]) or "")
        priority   = (find_col(row, COL_ALIASES["priority"]) or "medium").lower()
        assignees_raw = find_col(row, COL_ALIASES["assignees"]) or ""
        assignees  = [a.strip() for a in re.split(r'[,;|]', assignees_raw) if a.strip()]

        # Clean owner
        if owner:
            owner = owner.strip() or None

        task = Task(
            id=str(task_id).strip(),
            title=str(title).strip(),
            owner=owner,
            assignees=assignees,
            status=_normalize_status(status_str),
            deadline=deadline,
            created_at=created,
            updated_at=updated,
            priority=priority,
            source="csv",
            raw=dict(row),
        )
        tasks.append(task)

    return tasks


def parse_tasks_json(content: str) -> List[Task]:
    """
    Parse tasks from JSON.
    Accepts: array of task objects, or newline-delimited JSON objects.
    """
    content = content.strip()
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        # Try NDJSON
        data = []
        for line in content.splitlines():
            line = line.strip()
            if line:
                try:
                    data.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    if isinstance(data, dict):
        # Maybe it's wrapped: {"tasks": [...]}
        data = data.get("tasks") or data.get("items") or data.get("data") or [data]

    tasks = []
    for i, obj in enumerate(data):
        if not isinstance(obj, dict):
            continue

        def get(*keys, default=None):
            for k in keys:
                for k2 in [k, k.lower(), k.upper()]:
                    if k2 in obj:
                        return obj[k2]
            return default

        task_id    = get("id", "task_id", "ticket_id") or str(i + 1)
        title      = get("title", "name", "summary", "description") or "(untitled)"
        owner      = get("owner", "assigned_to", "assignee")
        status_str = get("status", "state") or "pending"
        deadline   = parse_dt(str(get("deadline", "due_date", "due") or ""))
        created    = parse_dt(str(get("created_at", "created", "date") or ""))
        updated    = parse_dt(str(get("updated_at", "updated", "modified") or ""))
        priority   = str(get("priority", "prio") or "medium").lower()
        assignees  = get("assignees", "team") or []
        if isinstance(assignees, str):
            assignees = [a.strip() for a in re.split(r'[,;|]', assignees) if a.strip()]

        tasks.append(Task(
            id=str(task_id),
            title=str(title),
            owner=str(owner).strip() if owner else None,
            assignees=assignees if isinstance(assignees, list) else [],
            status=_normalize_status(str(status_str)),
            deadline=deadline,
            created_at=created,
            updated_at=updated,
            priority=priority,
            source="json",
            raw=obj,
        ))

    return tasks


def parse_tasks_text(content: str) -> List[Task]:
    """
    Parse tasks from plain text (one task per line).
    Lines like: "- [ ] Fix login bug @alice !high due:2025-05-01"
    Or: "TODO: Deploy to production (alice) - HIGH PRIORITY"
    """
    tasks   = []
    counter = 0

    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        counter += 1

        # Status from checkbox
        if re.match(r'^-?\s*\[x\]', line, re.IGNORECASE):
            status = TaskStatus.DONE
        elif re.match(r'^-?\s*\[\s\]', line):
            status = TaskStatus.PENDING
        elif re.search(r'\bblocked\b', line, re.IGNORECASE):
            status = TaskStatus.BLOCKED
        elif re.search(r'\bwip\b|in.progress', line, re.IGNORECASE):
            status = TaskStatus.IN_PROGRESS
        else:
            status = TaskStatus.PENDING

        # Extract @owner
        owner_match = re.search(r'@(\w+)', line)
        owner = owner_match.group(1) if owner_match else None

        # Extract priority
        prio_match = re.search(r'!(critical|high|medium|low)', line, re.IGNORECASE)
        priority = prio_match.group(1).lower() if prio_match else "medium"

        # Extract due date
        due_match = re.search(r'due:(\S+)', line, re.IGNORECASE)
        deadline = parse_dt(due_match.group(1)) if due_match else None

        # Clean title
        title = re.sub(r'@\w+|!\w+|due:\S+|\[.?\]|[-*]', '', line).strip()
        title = re.sub(r'\s+', ' ', title) or line[:60]

        tasks.append(Task(
            id=f"txt_{counter}",
            title=title,
            owner=owner,
            status=status,
            deadline=deadline,
            priority=priority,
            source="text",
            raw={"line": line},
        ))

    return tasks


# ─── Log Parsers ──────────────────────────────────────────────────────────────

# Common log patterns
_LOG_PATTERNS = [
    # Python logging: 2024-01-15 14:23:01,234 ERROR module: message
    (r'^(?P<ts>\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}[,.]?\d*)\s+'
     r'(?P<level>DEBUG|INFO|WARN|WARNING|ERROR|CRITICAL|FATAL)\s+'
     r'(?:(?P<src>[\w.]+):\s+)?(?P<msg>.+)$',
     "python"),
    # ISO with level: [2024-01-15T14:23:01] [ERROR] service: message
    (r'^\[(?P<ts>\d{4}-\d{2}-\d{2}T[\d:]+)\]\s*\[(?P<level>\w+)\]\s*'
     r'(?:(?P<src>[\w.-]+):\s*)?(?P<msg>.+)$',
     "iso_bracket"),
    # Syslog: Jan 15 14:23:01 hostname service[pid]: message
    (r'^(?P<ts>[A-Za-z]{3}\s+\d+\s+\d{2}:\d{2}:\d{2})\s+'
     r'(?P<src>[\w.-]+)\[?\d*\]?:\s*(?P<msg>.+)$',
     "syslog"),
    # JSON log: {"timestamp": "...", "level": "...", "message": "..."}
    # handled separately
    # Simple: ERROR: message  or  [ERROR] message
    (r'^(?:\[(?P<level>DEBUG|INFO|WARN|WARNING|ERROR|CRITICAL|FATAL)\]|'
     r'(?P<level2>DEBUG|INFO|WARN|WARNING|ERROR|CRITICAL|FATAL):)\s*(?P<msg>.+)$',
     "simple"),
    # Apache/nginx: [Thu Jan 15 14:23:01 2024] [error] message
    (r'^\[(?P<ts>[A-Za-z]{3}\s+[A-Za-z]{3}\s+\d+\s+\d{2}:\d{2}:\d{2}\s+\d{4})\]\s*'
     r'\[(?P<level>\w+)\]\s*(?:(?P<src>[\w.-]+):\s*)?(?P<msg>.+)$',
     "apache"),
]
_LOG_COMPILED = [(re.compile(p, re.IGNORECASE), name) for p, name in _LOG_PATTERNS]


def parse_logs(content: str) -> List[LogEntry]:
    """
    Parse log entries from multi-format log text.
    Returns list of LogEntry objects, best-effort parsed.
    """
    entries = []
    lines = content.splitlines()
    now   = datetime.now()

    for line in lines:
        line = line.strip()
        if not line:
            continue

        entry = _try_parse_log_line(line, now)
        if entry:
            entries.append(entry)

    return entries


def _try_parse_log_line(line: str, fallback_dt: datetime) -> Optional[LogEntry]:
    """Try each pattern and return a LogEntry if matched."""

    # Try JSON first
    if line.startswith("{"):
        try:
            obj  = json.loads(line)
            ts   = parse_dt(str(
                obj.get("timestamp") or obj.get("time") or obj.get("@timestamp") or ""
            )) or fallback_dt
            level = str(obj.get("level") or obj.get("severity") or "INFO").upper()
            msg   = str(obj.get("message") or obj.get("msg") or obj.get("text") or line)
            src   = str(obj.get("service") or obj.get("source") or obj.get("logger") or "")
            return LogEntry(timestamp=ts, level=level, message=msg, source=src, raw=line)
        except (json.JSONDecodeError, Exception):
            pass

    # Try regex patterns
    for pattern, pname in _LOG_COMPILED:
        m = pattern.match(line)
        if m:
            gd    = m.groupdict()
            ts    = parse_dt(gd.get("ts") or "") or fallback_dt
            level = (gd.get("level") or gd.get("level2") or "INFO").upper()
            msg   = (gd.get("msg") or "").strip()
            src   = (gd.get("src") or "").strip()
            return LogEntry(timestamp=ts, level=level, message=msg, source=src, raw=line)

    # Fallback: treat whole line as INFO message
    level = "INFO"
    for lvl in ("ERROR", "WARN", "WARNING", "CRITICAL", "FATAL", "DEBUG"):
        if lvl in line.upper():
            level = lvl
            break
    return LogEntry(
        timestamp=fallback_dt,
        level=level,
        message=line[:300],
        source="",
        raw=line,
    )


# ─── Email Parsers ────────────────────────────────────────────────────────────

def parse_emails_json(content: str) -> List[EmailRecord]:
    """
    Parse emails from JSON array.

    Expected fields:
      timestamp/date/sent_at, from/sender, to/recipients,
      subject, body/content, message_id, thread_id, in_reply_to
    """
    content = content.strip()
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        # NDJSON
        data = []
        for line in content.splitlines():
            line = line.strip()
            if line:
                try:
                    data.append(json.loads(line))
                except Exception:
                    pass

    if isinstance(data, dict):
        data = data.get("emails") or data.get("messages") or [data]

    emails = []
    for obj in data:
        if not isinstance(obj, dict):
            continue

        def get(*keys, default=None):
            for k in keys:
                for k2 in [k, k.lower()]:
                    if k2 in obj:
                        return obj[k2]
            return default

        ts   = parse_dt(str(get("timestamp", "date", "sent_at", "time") or "")) or datetime.now()
        sender = str(get("from", "sender", "from_address") or "unknown@unknown.com")
        recip  = get("to", "recipients", "to_address") or []
        if isinstance(recip, str):
            recip = [r.strip() for r in re.split(r'[,;]', recip) if r.strip()]
        subject = str(get("subject", "title") or "(no subject)")
        body    = str(get("body", "content", "text", "html") or "")
        msg_id  = str(get("message_id", "id") or "")
        thread  = str(get("thread_id", "thread") or "")
        is_reply = bool(get("in_reply_to", "is_reply") or
                        subject.lower().startswith("re:") or
                        subject.lower().startswith("fwd:"))

        emails.append(EmailRecord(
            timestamp=ts,
            sender=_normalize_email(sender),
            recipients=[_normalize_email(r) for r in recip],
            subject=subject,
            body=body[:2000],  # cap body size
            message_id=msg_id,
            thread_id=thread,
            is_reply=is_reply,
            raw=json.dumps(obj),
        ))

    return emails


def parse_emails_text(content: str) -> List[EmailRecord]:
    """
    Parse emails from plain text format.

    Supported format (one email per block, separated by blank lines or '---'):
      From: alice@example.com
      To: bob@example.com
      Subject: Fix deployment
      Date: 2024-01-15 14:00
      Body:
      <body text>
    """
    emails   = []
    blocks   = re.split(r'\n\s*---+\s*\n|\n{3,}', content.strip())

    for block in blocks:
        block = block.strip()
        if not block:
            continue

        def extract(field: str) -> str:
            m = re.search(rf'^{field}:\s*(.+)$', block, re.IGNORECASE | re.MULTILINE)
            return m.group(1).strip() if m else ""

        sender    = extract("from") or extract("sender") or "unknown"
        to_str    = extract("to") or extract("recipients") or ""
        subject   = extract("subject") or "(no subject)"
        date_str  = extract("date") or extract("sent") or extract("timestamp") or ""
        ts        = parse_dt(date_str) or datetime.now()
        recip     = [r.strip() for r in re.split(r'[,;]', to_str) if r.strip()]

        # Body is everything after the header block
        body_match = re.search(r'(?:body|content|message|text):\s*\n?(.*)',
                                block, re.IGNORECASE | re.DOTALL)
        if body_match:
            body = body_match.group(1).strip()
        else:
            # Body = lines without a "Key: value" prefix
            header_end = 0
            for line in block.splitlines():
                if re.match(r'^\w[\w\s]*:\s', line):
                    header_end += len(line) + 1
                else:
                    break
            body = block[header_end:].strip()

        is_reply = subject.lower().startswith("re:") or subject.lower().startswith("fwd:")

        emails.append(EmailRecord(
            timestamp=ts,
            sender=_normalize_email(sender),
            recipients=[_normalize_email(r) for r in recip],
            subject=subject,
            body=body[:2000],
            is_reply=is_reply,
            raw=block,
        ))

    return emails


def parse_emails_mbox(content: str) -> List[EmailRecord]:
    """
    Parse emails from mbox-like format (From_ line separated).
    """
    emails = []
    blocks = re.split(r'(?m)^From .+ \d{4}$', content)
    headers_list = re.findall(r'(?m)^From .+ \d{4}$', content)

    for i, block in enumerate(blocks):
        if not block.strip():
            continue
        # Parse headers
        header_block = block.split('\n\n', 1)
        headers_text = header_block[0]
        body         = header_block[1].strip() if len(header_block) > 1 else ""

        def get_header(name: str) -> str:
            m = re.search(rf'^{name}:\s*(.+)$', headers_text, re.IGNORECASE | re.MULTILINE)
            return m.group(1).strip() if m else ""

        sender   = get_header("from") or (headers_list[i-1].split()[1] if i < len(headers_list) else "unknown")
        to_str   = get_header("to") or ""
        subject  = get_header("subject") or "(no subject)"
        date_str = get_header("date") or ""
        msg_id   = get_header("message-id") or get_header("message_id") or ""
        in_reply = get_header("in-reply-to") or ""

        ts    = parse_dt(date_str) or datetime.now()
        recip = [r.strip() for r in re.split(r'[,;]', to_str) if r.strip()]

        emails.append(EmailRecord(
            timestamp=ts,
            sender=_normalize_email(sender),
            recipients=[_normalize_email(r) for r in recip],
            subject=subject,
            body=body[:2000],
            message_id=msg_id,
            is_reply=bool(in_reply) or subject.lower().startswith("re:"),
            raw=block,
        ))

    return emails


# ─── Auto-detect & parse ──────────────────────────────────────────────────────

def parse_tasks(content: str, hint: str = "auto") -> List[Task]:
    """
    Auto-detect format (CSV/JSON/text) and parse tasks.
    hint: "csv" | "json" | "text" | "auto"
    """
    content = content.strip()
    if not content:
        return []

    if hint == "json" or (hint == "auto" and _looks_like_json(content)):
        return parse_tasks_json(content)
    elif hint == "csv" or (hint == "auto" and _looks_like_csv(content)):
        return parse_tasks_csv(content)
    else:
        return parse_tasks_text(content)


def parse_emails(content: str, hint: str = "auto") -> List[EmailRecord]:
    """Auto-detect email format and parse."""
    content = content.strip()
    if not content:
        return []

    if hint == "json" or (hint == "auto" and _looks_like_json(content)):
        return parse_emails_json(content)
    elif hint == "mbox" or (hint == "auto" and content.startswith("From ")):
        return parse_emails_mbox(content)
    else:
        return parse_emails_text(content)


def _looks_like_csv(s: str) -> bool:
    first = s.splitlines()[0]
    return first.count(",") >= 2 or first.count("\t") >= 2

def _looks_like_json(s: str) -> bool:
    return s.startswith(("{", "["))

def _normalize_email(s: str) -> str:
    """Extract email address from 'Name <email>' format."""
    m = re.search(r'<([^>]+)>', s)
    if m:
        return m.group(1).lower().strip()
    # bare email
    m2 = re.search(r'[\w.+-]+@[\w.-]+\.\w+', s)
    if m2:
        return m2.group(0).lower().strip()
    return s.lower().strip()
