"""
flowguard/engine.py
────────────────────
Core detection engine for FlowGuard.

Models:
  Task           — a work item with owner, deadline, status
  LogEntry       — a parsed log line with timestamp + level + message
  EmailRecord    — a parsed email with sender, recipients, subject, body, timestamp
  WorkEvent      — normalized event from any source

Detectors:
  MissedTaskDetector     — tasks past deadline without completion
  DelayDetector          — tasks taking significantly longer than expected
  OwnershipGapDetector   — tasks with no owner, or owner who went silent
  EscalationDetector     — repeated failures or escalating urgency in communications
  SilenceDetector        — expected activity that never happened

FlowGuardEngine — orchestrates all detectors, produces a FailureReport
"""

from __future__ import annotations

import re
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum


# ─── Enums ────────────────────────────────────────────────────────────────────

class Severity(Enum):
    LOW      = "low"
    MEDIUM   = "medium"
    HIGH     = "high"
    CRITICAL = "critical"

class TaskStatus(Enum):
    PENDING     = "pending"
    IN_PROGRESS = "in_progress"
    DONE        = "done"
    BLOCKED     = "blocked"
    CANCELLED   = "cancelled"
    UNKNOWN     = "unknown"


# ─── Data Models ──────────────────────────────────────────────────────────────

@dataclass
class Task:
    id:           str
    title:        str
    owner:        Optional[str]              = None
    assignees:    List[str]                  = field(default_factory=list)
    status:       TaskStatus                 = TaskStatus.PENDING
    created_at:   Optional[datetime]         = None
    deadline:     Optional[datetime]         = None
    completed_at: Optional[datetime]         = None
    updated_at:   Optional[datetime]         = None
    priority:     str                        = "medium"   # low/medium/high/critical
    tags:         List[str]                  = field(default_factory=list)
    source:       str                        = "manual"   # manual/csv/json/log
    raw:          Dict[str, Any]             = field(default_factory=dict)

    @property
    def is_overdue(self) -> bool:
        if self.deadline is None:
            return False
        if self.status in (TaskStatus.DONE, TaskStatus.CANCELLED):
            return False
        return datetime.now() > self.deadline

    @property
    def days_overdue(self) -> float:
        if not self.is_overdue or self.deadline is None:
            return 0.0
        return (datetime.now() - self.deadline).total_seconds() / 86400

    @property
    def age_days(self) -> float:
        if self.created_at is None:
            return 0.0
        return (datetime.now() - self.created_at).total_seconds() / 86400

    @property
    def has_owner(self) -> bool:
        return bool(self.owner) or bool(self.assignees)

    @property
    def all_owners(self) -> List[str]:
        owners = []
        if self.owner:
            owners.append(self.owner)
        owners.extend(a for a in self.assignees if a not in owners)
        return owners


@dataclass
class LogEntry:
    timestamp:  datetime
    level:      str           # INFO/WARN/ERROR/CRITICAL/DEBUG
    message:    str
    source:     str           = ""   # service/component name
    task_ref:   Optional[str] = None # task ID if mentioned
    raw:        str           = ""

    @property
    def is_error(self) -> bool:
        return self.level.upper() in ("ERROR", "CRITICAL", "FATAL")

    @property
    def is_warning(self) -> bool:
        return self.level.upper() in ("WARN", "WARNING")


@dataclass
class EmailRecord:
    timestamp:    datetime
    sender:       str
    recipients:   List[str]
    subject:      str
    body:         str
    message_id:   str         = ""
    thread_id:    str         = ""
    is_reply:     bool        = False
    raw:          str         = ""

    @property
    def all_participants(self) -> List[str]:
        return list({self.sender, *self.recipients})

    @property
    def urgency_score(self) -> int:
        """0-10 urgency score based on subject/body keywords."""
        text  = (self.subject + " " + self.body).lower()
        score = 0
        high   = ["urgent", "asap", "immediately", "critical", "blocker",
                  "escalat", "overdue", "missed", "failed", "emergency"]
        medium = ["please", "reminder", "follow up", "waiting", "delay",
                  "concern", "issue", "problem", "stuck", "behind"]
        for word in high:
            if word in text:
                score += 2
        for word in medium:
            if word in text:
                score += 1
        return min(score, 10)


@dataclass
class WorkEvent:
    """Normalized event from any source (task/log/email)."""
    timestamp:   datetime
    event_type:  str                  # task_update/log_error/email_received/etc
    actor:       Optional[str]        # who did it
    subject:     str                  # what happened
    task_ref:    Optional[str]        = None
    severity:    Severity             = Severity.LOW
    source_type: str                  = "unknown"  # task/log/email
    raw:         Any                  = None


# ─── Findings ─────────────────────────────────────────────────────────────────

@dataclass
class Finding:
    """A single detected failure or risk."""
    id:           str
    detector:     str
    severity:     Severity
    title:        str
    description:  str
    task:         Optional[Task]          = None
    evidence:     List[str]               = field(default_factory=list)
    owners:       List[str]               = field(default_factory=list)
    recommended:  str                     = ""
    tags:         List[str]               = field(default_factory=list)
    detected_at:  datetime                = field(default_factory=datetime.now)

    def to_dict(self) -> Dict:
        return {
            "id":           self.id,
            "detector":     self.detector,
            "severity":     self.severity.value,
            "title":        self.title,
            "description":  self.description,
            "task_id":      self.task.id if self.task else None,
            "task_title":   self.task.title if self.task else None,
            "evidence":     self.evidence,
            "owners":       self.owners,
            "recommended":  self.recommended,
            "tags":         self.tags,
            "detected_at":  self.detected_at.isoformat(),
        }


@dataclass
class FailureReport:
    """Complete analysis report from FlowGuard."""
    generated_at:   datetime
    findings:       List[Finding]
    tasks_analyzed: int
    logs_analyzed:  int
    emails_analyzed: int
    analysis_window_days: float

    @property
    def critical_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == Severity.CRITICAL)

    @property
    def high_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == Severity.HIGH)

    @property
    def medium_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == Severity.MEDIUM)

    @property
    def low_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == Severity.LOW)

    @property
    def total(self) -> int:
        return len(self.findings)

    def by_severity(self, sev: Severity) -> List[Finding]:
        return [f for f in self.findings if f.severity == sev]

    def by_owner(self, owner: str) -> List[Finding]:
        return [f for f in self.findings if owner in f.owners]

    def to_dict(self) -> Dict:
        return {
            "generated_at":       self.generated_at.isoformat(),
            "summary": {
                "total":          self.total,
                "critical":       self.critical_count,
                "high":           self.high_count,
                "medium":         self.medium_count,
                "low":            self.low_count,
            },
            "inputs": {
                "tasks":   self.tasks_analyzed,
                "logs":    self.logs_analyzed,
                "emails":  self.emails_analyzed,
            },
            "findings": [f.to_dict() for f in self.findings],
        }


# ─── Base Detector ────────────────────────────────────────────────────────────

class BaseDetector:
    DETECTOR_NAME = "base"

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self._finding_counter = 0

    def _next_id(self) -> str:
        self._finding_counter += 1
        return f"{self.DETECTOR_NAME}_{self._finding_counter:03d}"

    def detect(
        self,
        tasks:  List[Task],
        logs:   List[LogEntry],
        emails: List[EmailRecord],
    ) -> List[Finding]:
        raise NotImplementedError


# ─── Detector 1: Missed Tasks ─────────────────────────────────────────────────

class MissedTaskDetector(BaseDetector):
    """
    Detects tasks that are past their deadline without being completed.
    Severity scales with how overdue they are and the task priority.
    """
    DETECTOR_NAME = "missed_task"

    PRIORITY_MULTIPLIER = {
        "critical": 2.0,
        "high":     1.5,
        "medium":   1.0,
        "low":      0.5,
    }

    def detect(self, tasks, logs, emails) -> List[Finding]:
        findings = []

        for task in tasks:
            if not task.is_overdue:
                continue

            days = task.days_overdue
            prio = task.priority.lower()
            mult = self.PRIORITY_MULTIPLIER.get(prio, 1.0)
            effective_days = days * mult

            if effective_days >= 7:
                sev = Severity.CRITICAL
            elif effective_days >= 3:
                sev = Severity.HIGH
            elif effective_days >= 1:
                sev = Severity.MEDIUM
            else:
                sev = Severity.LOW

            evidence = [
                f"Deadline: {task.deadline.strftime('%Y-%m-%d %H:%M') if task.deadline else 'unknown'}",
                f"Overdue by: {days:.1f} days",
                f"Current status: {task.status.value}",
                f"Priority: {task.priority}",
            ]
            if task.updated_at:
                evidence.append(f"Last updated: {task.updated_at.strftime('%Y-%m-%d %H:%M')}")

            # Check if any log errors mention this task
            task_errors = [
                l for l in logs
                if task.id in l.message or (task.title and task.title.lower() in l.message.lower())
            ]
            if task_errors:
                evidence.append(f"Related log errors: {len(task_errors)}")
                evidence.append(f"  Latest: {task_errors[-1].message[:80]}")

            findings.append(Finding(
                id=self._next_id(),
                detector=self.DETECTOR_NAME,
                severity=sev,
                title=f"Missed deadline: {task.title}",
                description=(
                    f"Task '{task.title}' is {days:.1f} days overdue "
                    f"(priority: {task.priority}). "
                    f"Status is still '{task.status.value}'."
                ),
                task=task,
                evidence=evidence,
                owners=task.all_owners,
                recommended=(
                    f"Immediately contact owner(s) {task.all_owners or ['(unassigned)']} "
                    f"to assess current state and reset deadline or escalate."
                ),
                tags=["overdue", "missed-deadline", prio],
            ))

        return findings


# ─── Detector 2: Ownership Gaps ───────────────────────────────────────────────

class OwnershipGapDetector(BaseDetector):
    """
    Detects tasks with no owner, or where the owner has been silent
    for too long relative to task urgency.
    """
    DETECTOR_NAME = "ownership_gap"

    def __init__(self, config=None):
        super().__init__(config)
        self.silence_threshold_days = self.config.get("silence_threshold_days", 3)
        self.critical_silence_days  = self.config.get("critical_silence_days", 1)

    def detect(self, tasks, logs, emails) -> List[Finding]:
        findings = []

        # Build activity map: owner → last seen timestamp
        activity: Dict[str, datetime] = {}
        for log in logs:
            # Try to find person references in logs
            for person in _extract_names(log.message):
                if person not in activity or log.timestamp > activity[person]:
                    activity[person] = log.timestamp
        for email in emails:
            if email.sender not in activity or email.timestamp > activity[email.sender]:
                activity[email.sender] = email.timestamp
            for r in email.recipients:
                if r not in activity or email.timestamp > activity[r]:
                    activity[r] = email.timestamp

        for task in tasks:
            if task.status in (TaskStatus.DONE, TaskStatus.CANCELLED):
                continue

            # --- No owner ---
            if not task.has_owner:
                sev = Severity.HIGH if task.priority in ("high", "critical") else Severity.MEDIUM
                if task.deadline and task.days_overdue > 0:
                    sev = Severity.CRITICAL
                findings.append(Finding(
                    id=self._next_id(),
                    detector=self.DETECTOR_NAME,
                    severity=sev,
                    title=f"Unowned task: {task.title}",
                    description=(
                        f"Task '{task.title}' has no assigned owner. "
                        f"It is {task.status.value} with priority '{task.priority}'."
                    ),
                    task=task,
                    evidence=[
                        "Owner: (none)",
                        f"Status: {task.status.value}",
                        f"Priority: {task.priority}",
                        f"Age: {task.age_days:.1f} days",
                    ],
                    owners=[],
                    recommended="Assign an owner immediately. Unowned tasks tend to fall through the cracks.",
                    tags=["unowned", "ownership-gap"],
                ))
                continue

            # --- Owner silent ---
            for owner in task.all_owners:
                last_seen = activity.get(owner)
                if last_seen is None:
                    # Owner never seen in logs or emails
                    sev = Severity.MEDIUM
                    if task.deadline and datetime.now() > task.deadline - timedelta(days=1):
                        sev = Severity.HIGH
                    findings.append(Finding(
                        id=self._next_id(),
                        detector=self.DETECTOR_NAME,
                        severity=sev,
                        title=f"Silent owner: {owner} on '{task.title}'",
                        description=(
                            f"Owner '{owner}' has no recorded activity in logs or emails. "
                            f"Task '{task.title}' may be orphaned."
                        ),
                        task=task,
                        evidence=[
                            f"Owner: {owner}",
                            "Last activity: not found in any source",
                            f"Task status: {task.status.value}",
                        ],
                        owners=[owner],
                        recommended=f"Reach out to {owner} immediately. Verify they are still active on this task.",
                        tags=["silent-owner", "ownership-gap"],
                    ))
                else:
                    silence_days = (datetime.now() - last_seen).total_seconds() / 86400
                    threshold = (
                        self.critical_silence_days
                        if task.priority in ("critical", "high")
                        else self.silence_threshold_days
                    )
                    if silence_days > threshold:
                        sev = Severity.HIGH if silence_days > threshold * 2 else Severity.MEDIUM
                        findings.append(Finding(
                            id=self._next_id(),
                            detector=self.DETECTOR_NAME,
                            severity=sev,
                            title=f"Owner gone quiet: {owner} on '{task.title}'",
                            description=(
                                f"Owner '{owner}' has been silent for {silence_days:.1f} days "
                                f"(threshold: {threshold}d) on task '{task.title}'."
                            ),
                            task=task,
                            evidence=[
                                f"Owner: {owner}",
                                f"Last activity: {last_seen.strftime('%Y-%m-%d %H:%M')}",
                                f"Silence: {silence_days:.1f} days",
                                f"Threshold: {threshold} days",
                            ],
                            owners=[owner],
                            recommended=f"Follow up with {owner}. Check if they are blocked or need reassignment.",
                            tags=["silent-owner", "communication-gap"],
                        ))

        return findings


# ─── Detector 3: Delay Patterns ───────────────────────────────────────────────

class DelayDetector(BaseDetector):
    """
    Detects tasks that are progressing much slower than expected,
    or tasks stuck in the same status for too long.
    """
    DETECTOR_NAME = "delay"

    STATUS_AGE_THRESHOLDS = {
        TaskStatus.PENDING:     7,   # pending > 7 days = concern
        TaskStatus.IN_PROGRESS: 14,  # in progress > 14 days without update = concern
        TaskStatus.BLOCKED:     2,   # blocked > 2 days = concern
    }

    def __init__(self, config=None):
        super().__init__(config)
        # Allow overriding thresholds
        for status, default in self.STATUS_AGE_THRESHOLDS.items():
            key = f"{status.value}_threshold_days"
            self.STATUS_AGE_THRESHOLDS[status] = self.config.get(key, default)

    def detect(self, tasks, logs, emails) -> List[Finding]:
        findings = []

        for task in tasks:
            if task.status in (TaskStatus.DONE, TaskStatus.CANCELLED):
                continue

            threshold = self.STATUS_AGE_THRESHOLDS.get(task.status)
            if threshold is None:
                continue

            # Check time since last update
            last_touch = task.updated_at or task.created_at
            if last_touch is None:
                continue

            stuck_days = (datetime.now() - last_touch).total_seconds() / 86400
            if stuck_days < threshold:
                continue

            # Scale severity
            ratio = stuck_days / threshold
            if ratio >= 3:
                sev = Severity.HIGH
            elif ratio >= 2:
                sev = Severity.MEDIUM
            else:
                sev = Severity.LOW

            # Escalate if deadline approaching
            if task.deadline:
                days_until_due = (task.deadline - datetime.now()).total_seconds() / 86400
                if days_until_due < 2:
                    sev = Severity.CRITICAL
                elif days_until_due < 5:
                    sev = Severity.HIGH

            status_label = {
                TaskStatus.PENDING:     "hasn't been started",
                TaskStatus.IN_PROGRESS: "has had no updates",
                TaskStatus.BLOCKED:     "has been blocked",
            }.get(task.status, "is stalled")

            findings.append(Finding(
                id=self._next_id(),
                detector=self.DETECTOR_NAME,
                severity=sev,
                title=f"Stalled task: {task.title}",
                description=(
                    f"Task '{task.title}' {status_label} for {stuck_days:.1f} days "
                    f"(expected max: {threshold}d in status '{task.status.value}')."
                ),
                task=task,
                evidence=[
                    f"Status: {task.status.value}",
                    f"Last touched: {last_touch.strftime('%Y-%m-%d %H:%M')}",
                    f"Days without update: {stuck_days:.1f}",
                    f"Threshold: {threshold} days",
                    f"Deadline: {task.deadline.strftime('%Y-%m-%d') if task.deadline else 'none'}",
                ],
                owners=task.all_owners,
                recommended=(
                    f"Check with {task.all_owners or ['(unassigned)']} "
                    f"why this task is stalled. Update status or unblock."
                ),
                tags=["stalled", "delay", task.status.value],
            ))

        return findings


# ─── Detector 4: Log Error Patterns ──────────────────────────────────────────

class LogErrorDetector(BaseDetector):
    """
    Detects error bursts, repeated failures, and critical log events
    that suggest something is broken in the workflow.
    """
    DETECTOR_NAME = "log_error"

    def __init__(self, config=None):
        super().__init__(config)
        self.error_burst_threshold  = self.config.get("error_burst_threshold", 5)
        self.error_burst_window_min = self.config.get("error_burst_window_min", 60)
        self.repeated_error_threshold = self.config.get("repeated_error_threshold", 3)

    def detect(self, tasks, logs, emails) -> List[Finding]:
        findings = []
        errors = [l for l in logs if l.is_error]

        if not errors:
            return findings

        # 1. Error burst detection
        findings.extend(self._detect_burst(errors))

        # 2. Repeated identical errors
        findings.extend(self._detect_repeated(errors))

        # 3. Critical-level events
        findings.extend(self._detect_criticals(logs))

        return findings

    def _detect_burst(self, errors: List[LogEntry]) -> List[Finding]:
        if len(errors) < self.error_burst_threshold:
            return []

        findings = []
        # Sliding window
        for i, anchor in enumerate(errors):
            window_end = anchor.timestamp + timedelta(minutes=self.error_burst_window_min)
            burst = [e for e in errors[i:] if e.timestamp <= window_end]
            if len(burst) >= self.error_burst_threshold:
                sources = list({e.source for e in burst if e.source})
                findings.append(Finding(
                    id=self._next_id(),
                    detector=self.DETECTOR_NAME,
                    severity=Severity.HIGH,
                    title=f"Error burst: {len(burst)} errors in {self.error_burst_window_min}min",
                    description=(
                        f"Detected {len(burst)} errors within a {self.error_burst_window_min}-minute window "
                        f"starting at {anchor.timestamp.strftime('%Y-%m-%d %H:%M')}."
                    ),
                    evidence=[
                        f"Window start: {anchor.timestamp.strftime('%Y-%m-%d %H:%M')}",
                        f"Error count: {len(burst)}",
                        f"Sources: {sources or ['(unknown)']}",
                        f"First error: {burst[0].message[:80]}",
                        f"Last error:  {burst[-1].message[:80]}",
                    ],
                    recommended="Investigate the root cause of the error spike. Check system health and task states.",
                    tags=["log-burst", "error-spike"],
                ))
                break  # Only report first burst per analysis
        return findings

    def _detect_repeated(self, errors: List[LogEntry]) -> List[Finding]:
        from collections import Counter
        # Normalize messages to find duplicates
        normalized = Counter()
        msg_examples: Dict[str, List[LogEntry]] = {}
        for e in errors:
            # Remove numbers/IDs to normalize
            key = re.sub(r'\b\d+\b', 'N', e.message.lower())[:80]
            normalized[key] += 1
            msg_examples.setdefault(key, []).append(e)

        findings = []
        for key, count in normalized.items():
            if count >= self.repeated_error_threshold:
                examples = msg_examples[key]
                findings.append(Finding(
                    id=self._next_id(),
                    detector=self.DETECTOR_NAME,
                    severity=Severity.MEDIUM,
                    title=f"Repeated error ({count}x): {examples[0].message[:50]}",
                    description=(
                        f"The same error pattern occurred {count} times: "
                        f"'{examples[0].message[:80]}'"
                    ),
                    evidence=[
                        f"Occurrences: {count}",
                        f"First: {examples[0].timestamp.strftime('%Y-%m-%d %H:%M')}",
                        f"Last:  {examples[-1].timestamp.strftime('%Y-%m-%d %H:%M')}",
                        f"Source: {examples[0].source or '(unknown)'}",
                    ],
                    recommended="This recurring error suggests a systemic issue. Find the root cause rather than suppressing the error.",
                    tags=["repeated-error", "systemic"],
                ))
        return findings

    def _detect_criticals(self, logs: List[LogEntry]) -> List[Finding]:
        criticals = [l for l in logs if l.level.upper() in ("CRITICAL", "FATAL")]
        findings  = []
        for entry in criticals:
            findings.append(Finding(
                id=self._next_id(),
                detector=self.DETECTOR_NAME,
                severity=Severity.CRITICAL,
                title=f"CRITICAL log: {entry.message[:60]}",
                description=f"A CRITICAL log event was recorded: {entry.message}",
                evidence=[
                    f"Timestamp: {entry.timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
                    f"Level: {entry.level}",
                    f"Source: {entry.source or '(unknown)'}",
                    f"Message: {entry.message}",
                ],
                recommended="Treat all CRITICAL/FATAL log events as production incidents requiring immediate attention.",
                tags=["critical-log", "incident"],
            ))
        return findings


# ─── Detector 5: Email Escalation ─────────────────────────────────────────────

class EmailEscalationDetector(BaseDetector):
    """
    Detects escalating urgency in email threads, unanswered emails,
    and communication patterns that indicate things are going wrong.
    """
    DETECTOR_NAME = "email_escalation"

    def __init__(self, config=None):
        super().__init__(config)
        self.urgency_threshold     = self.config.get("urgency_threshold", 4)
        self.unanswered_hours      = self.config.get("unanswered_hours", 24)
        self.escalation_score      = self.config.get("escalation_score", 8)

    def detect(self, tasks, logs, emails) -> List[Finding]:
        findings = []

        if not emails:
            return findings

        # 1. High-urgency emails
        findings.extend(self._detect_urgent(emails))

        # 2. Escalating threads (urgency increasing over thread)
        findings.extend(self._detect_escalation(emails))

        # 3. Unanswered emails
        findings.extend(self._detect_unanswered(emails))

        return findings

    def _detect_urgent(self, emails: List[EmailRecord]) -> List[Finding]:
        findings = []
        for email in emails:
            score = email.urgency_score
            if score >= self.escalation_score:
                sev = Severity.CRITICAL
            elif score >= self.urgency_threshold + 3:
                sev = Severity.HIGH
            elif score >= self.urgency_threshold:
                sev = Severity.MEDIUM
            else:
                continue

            findings.append(Finding(
                id=self._next_id(),
                detector=self.DETECTOR_NAME,
                severity=sev,
                title=f"High-urgency email: {email.subject[:60]}",
                description=(
                    f"Email from {email.sender} has urgency score {score}/10. "
                    f"Subject: '{email.subject}'"
                ),
                evidence=[
                    f"From: {email.sender}",
                    f"To: {', '.join(email.recipients[:3])}",
                    f"Subject: {email.subject}",
                    f"Urgency score: {score}/10",
                    f"Timestamp: {email.timestamp.strftime('%Y-%m-%d %H:%M')}",
                    f"Preview: {email.body[:100]}",
                ],
                owners=list(email.all_participants),
                recommended=f"Respond to {email.sender} immediately. Urgency score is {score}/10.",
                tags=["urgent-email", "escalation"],
            ))
        return findings

    def _detect_escalation(self, emails: List[EmailRecord]) -> List[Finding]:
        """Detect threads where urgency is increasing over time."""
        threads: Dict[str, List[EmailRecord]] = {}
        for email in emails:
            tid = email.thread_id or email.subject.lower().replace("re:", "").strip()[:40]
            threads.setdefault(tid, []).append(email)

        findings = []
        for tid, thread in threads.items():
            if len(thread) < 2:
                continue
            thread_sorted = sorted(thread, key=lambda e: e.timestamp)
            scores = [e.urgency_score for e in thread_sorted]
            # Check if urgency is trending upward
            if scores[-1] > scores[0] + 3 and scores[-1] >= self.urgency_threshold:
                findings.append(Finding(
                    id=self._next_id(),
                    detector=self.DETECTOR_NAME,
                    severity=Severity.HIGH,
                    title=f"Escalating thread: '{thread_sorted[0].subject[:50]}'",
                    description=(
                        f"Email thread '{thread_sorted[0].subject}' shows escalating urgency "
                        f"({scores[0]} → {scores[-1]} out of 10) over {len(thread)} messages."
                    ),
                    evidence=[
                        f"Thread length: {len(thread)} emails",
                        f"Urgency trend: {' → '.join(str(s) for s in scores)}",
                        f"Participants: {', '.join(set(e.sender for e in thread))}",
                        f"Latest: {thread_sorted[-1].subject}",
                    ],
                    owners=list({e.sender for e in thread}),
                    recommended="This escalating thread needs immediate resolution. Arrange a call or meeting.",
                    tags=["escalating-thread", "communication-failure"],
                ))
        return findings

    def _detect_unanswered(self, emails: List[EmailRecord]) -> List[Finding]:
        """Detect emails that haven't received a reply."""
        # Build reply map
        replied_ids = {e.message_id for e in emails if e.is_reply and e.message_id}
        # Map subject-based threading
        subjects_seen: Dict[str, datetime] = {}
        for email in sorted(emails, key=lambda e: e.timestamp):
            subj = email.subject.lower().replace("re:", "").replace("fwd:", "").strip()
            if email.is_reply or subj in subjects_seen:
                subjects_seen[subj] = email.timestamp  # updated = replied
            else:
                subjects_seen[subj] = email.timestamp

        # Find emails with high urgency that appear unanswered
        findings = []
        for email in emails:
            if email.urgency_score < self.urgency_threshold:
                continue
            subj = email.subject.lower().replace("re:", "").strip()
            age_hours = (datetime.now() - email.timestamp).total_seconds() / 3600
            if age_hours < self.unanswered_hours:
                continue
            # Check if there's a reply after this email
            replies_after = [
                e for e in emails
                if e.timestamp > email.timestamp
                and e.subject.lower().replace("re:", "").strip() == subj
                and e.is_reply
            ]
            if not replies_after:
                findings.append(Finding(
                    id=self._next_id(),
                    detector=self.DETECTOR_NAME,
                    severity=Severity.HIGH,
                    title=f"Unanswered urgent email: '{email.subject[:50]}'",
                    description=(
                        f"High-urgency email from {email.sender} has gone unanswered "
                        f"for {age_hours:.0f} hours."
                    ),
                    evidence=[
                        f"From: {email.sender}",
                        f"Sent: {email.timestamp.strftime('%Y-%m-%d %H:%M')}",
                        f"Unanswered for: {age_hours:.0f} hours",
                        f"Urgency score: {email.urgency_score}/10",
                        f"Recipients: {', '.join(email.recipients[:3])}",
                    ],
                    owners=email.recipients[:3],
                    recommended=f"Recipients {email.recipients[:2]} must respond to {email.sender} immediately.",
                    tags=["unanswered-email", "communication-gap"],
                ))
        return findings


# ─── FlowGuard Engine ─────────────────────────────────────────────────────────

class FlowGuardEngine:
    """
    Orchestrates all detectors and produces a FailureReport.

    Usage:
        engine = FlowGuardEngine()
        report = engine.analyze(tasks=tasks, logs=logs, emails=emails)
        print(report.to_dict())
    """

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.detectors = [
            MissedTaskDetector(self.config.get("missed_task", {})),
            OwnershipGapDetector(self.config.get("ownership_gap", {})),
            DelayDetector(self.config.get("delay", {})),
            LogErrorDetector(self.config.get("log_error", {})),
            EmailEscalationDetector(self.config.get("email_escalation", {})),
        ]

    def analyze(
        self,
        tasks:  List[Task]       = None,
        logs:   List[LogEntry]   = None,
        emails: List[EmailRecord] = None,
        window_days: float       = 30,
    ) -> FailureReport:
        tasks  = tasks  or []
        logs   = logs   or []
        emails = emails or []

        # Filter to analysis window
        cutoff = datetime.now() - timedelta(days=window_days)
        logs   = [l for l in logs   if l.timestamp >= cutoff]
        emails = [e for e in emails if e.timestamp >= cutoff]

        all_findings: List[Finding] = []
        for detector in self.detectors:
            try:
                found = detector.detect(tasks, logs, emails)
                all_findings.extend(found)
            except Exception as e:
                import traceback
                print(f"[FlowGuard] Detector {detector.DETECTOR_NAME} failed: {e}")
                traceback.print_exc()

        # Sort: critical first, then by severity
        sev_order = {Severity.CRITICAL: 0, Severity.HIGH: 1,
                     Severity.MEDIUM: 2, Severity.LOW: 3}
        all_findings.sort(key=lambda f: sev_order.get(f.severity, 4))

        return FailureReport(
            generated_at=datetime.now(),
            findings=all_findings,
            tasks_analyzed=len(tasks),
            logs_analyzed=len(logs),
            emails_analyzed=len(emails),
            analysis_window_days=window_days,
        )


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _extract_names(text: str) -> List[str]:
    """Extract likely person names or email-style identifiers from text."""
    # email-style: alice@example.com → alice
    emails_found = re.findall(r'\b[\w.+-]+@[\w.-]+\.\w+\b', text)
    names = [e.split("@")[0] for e in emails_found]
    return names
