"""
tests/test_flowguard.py
─────────────────────────
Full test suite for FlowGuard.
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from flowguard.engine import (
    Task, TaskStatus, LogEntry, EmailRecord, Severity,
    MissedTaskDetector, OwnershipGapDetector, DelayDetector,
    LogErrorDetector, EmailEscalationDetector,
    FlowGuardEngine
)
from flowguard.parsers import (
    parse_tasks_csv, parse_tasks_json, parse_tasks_text,
    parse_logs, parse_emails_json, parse_emails_text,
    parse_tasks, parse_emails, parse_dt,
)

# ── Helpers ───────────────────────────────────────────────────────────────────
now = datetime.now

def future(days): return now() + timedelta(days=days)
def past(days):   return now() - timedelta(days=days)

def make_task(id="T1", title="Test Task", owner="alice@co.com",
              status=TaskStatus.PENDING, deadline=None,
              created=None, updated=None, priority="medium"):
    return Task(
        id=id, title=title, owner=owner,
        status=status, deadline=deadline or future(7),
        created_at=created or past(5),
        updated_at=updated or past(1),
        priority=priority,
    )

def make_log(msg="test error", level="ERROR", ts=None, source="svc"):
    return LogEntry(
        timestamp=ts or now(),
        level=level, message=msg, source=source,
    )

def make_email(subject="test", body="", sender="a@b.com",
               recipients=None, urgency_words=None, ts=None, is_reply=False):
    if urgency_words:
        body = " ".join(urgency_words) + " " + body
    return EmailRecord(
        timestamp=ts or now(),
        sender=sender,
        recipients=recipients or ["b@c.com"],
        subject=subject,
        body=body,
        is_reply=is_reply,
    )


# ─── parse_dt ─────────────────────────────────────────────────────────────────

class TestParseDt:
    def test_iso(self):
        dt = parse_dt("2024-03-15T14:30:00")
        assert dt.year == 2024 and dt.month == 3 and dt.day == 15

    def test_date_only(self):
        dt = parse_dt("2024-06-01")
        assert dt.year == 2024 and dt.month == 6

    def test_slash_format(self):
        dt = parse_dt("15/03/2024 10:00")
        assert dt.year == 2024 and dt.month == 3

    def test_invalid_returns_none(self):
        assert parse_dt("not a date") is None
        assert parse_dt("") is None
        assert parse_dt(None) is None


# ─── Task Parsers ─────────────────────────────────────────────────────────────

class TestTaskParsers:
    def test_csv_basic(self):
        csv = "id,title,owner,status,deadline\n1,Fix bug,alice,in_progress,2025-06-01\n"
        tasks = parse_tasks_csv(csv)
        assert len(tasks) == 1
        t = tasks[0]
        assert t.id == "1"
        assert t.title == "Fix bug"
        assert t.owner == "alice"
        assert t.status == TaskStatus.IN_PROGRESS
        assert t.deadline is not None

    def test_csv_multiple_rows(self):
        csv = "id,title,status\n1,A,pending\n2,B,done\n3,C,blocked\n"
        tasks = parse_tasks_csv(csv)
        assert len(tasks) == 3
        assert tasks[0].status == TaskStatus.PENDING
        assert tasks[1].status == TaskStatus.DONE
        assert tasks[2].status == TaskStatus.BLOCKED

    def test_csv_flexible_columns(self):
        csv = "task,assigned_to,state,due_date\nDeploy,bob,wip,2025-05-01\n"
        tasks = parse_tasks_csv(csv)
        assert tasks[0].title == "Deploy"
        assert tasks[0].owner == "bob"
        assert tasks[0].status == TaskStatus.IN_PROGRESS

    def test_json_array(self):
        data = json.dumps([
            {"id": "T1", "title": "Refactor", "status": "pending", "owner": "carol"}
        ])
        tasks = parse_tasks_json(data)
        assert len(tasks) == 1
        assert tasks[0].title == "Refactor"

    def test_json_wrapped(self):
        data = json.dumps({"tasks": [
            {"id": "T1", "title": "Deploy", "status": "done"}
        ]})
        tasks = parse_tasks_json(data)
        assert len(tasks) == 1

    def test_text_checkbox(self):
        text = "- [ ] Fix login @alice !high due:2025-06-01\n- [x] Write tests @bob\n"
        tasks = parse_tasks_text(text)
        assert len(tasks) == 2
        assert tasks[0].status == TaskStatus.PENDING
        assert tasks[0].owner == "alice"
        assert tasks[0].priority == "high"
        assert tasks[1].status == TaskStatus.DONE
        assert tasks[1].owner == "bob"

    def test_auto_detect_csv(self):
        csv = "id,title,status\n1,Task,pending\n"
        tasks = parse_tasks(csv)
        assert len(tasks) == 1

    def test_auto_detect_json(self):
        data = json.dumps([{"id": "1", "title": "T", "status": "pending"}])
        tasks = parse_tasks(data)
        assert len(tasks) == 1

    def test_empty_returns_empty(self):
        assert parse_tasks("") == []
        assert parse_tasks("   ") == []


# ─── Log Parsers ──────────────────────────────────────────────────────────────

class TestLogParsers:
    def test_python_format(self):
        log = "2024-03-15 14:30:00,123 ERROR mymodule: Something failed\n"
        entries = parse_logs(log)
        assert len(entries) == 1
        assert entries[0].level == "ERROR"
        assert "Something failed" in entries[0].message

    def test_simple_format(self):
        log = "ERROR: database connection refused\nWARN: retry attempt 3\nINFO: done\n"
        entries = parse_logs(log)
        assert len(entries) == 3
        errors = [e for e in entries if e.is_error]
        assert len(errors) == 1

    def test_json_logs(self):
        lines = '\n'.join([
            json.dumps({"timestamp": "2024-03-15T14:00:00", "level": "ERROR",
                        "message": "Auth failed", "service": "auth"}),
            json.dumps({"timestamp": "2024-03-15T14:01:00", "level": "INFO",
                        "message": "OK", "service": "api"}),
        ])
        entries = parse_logs(lines)
        assert len(entries) == 2
        assert entries[0].level == "ERROR"
        assert entries[0].source == "auth"

    def test_is_error_property(self):
        entries = parse_logs("ERROR: bad\nCRITICAL: worse\nINFO: ok\n")
        errors = [e for e in entries if e.is_error]
        assert len(errors) == 2

    def test_empty_returns_empty(self):
        assert parse_logs("") == []
        assert parse_logs("   \n  \n") == []


# ─── Email Parsers ────────────────────────────────────────────────────────────

class TestEmailParsers:
    def test_json_basic(self):
        data = json.dumps([{
            "from": "alice@co.com",
            "to": ["bob@co.com"],
            "subject": "Urgent: Fix this now",
            "body": "Please fix ASAP. This is critical and urgent.",
            "timestamp": "2024-03-15T10:00:00",
        }])
        emails = parse_emails_json(data)
        assert len(emails) == 1
        assert emails[0].sender == "alice@co.com"
        assert emails[0].urgency_score > 3

    def test_urgency_score(self):
        high = make_email(subject="URGENT ASAP critical", body="emergency blocker")
        low  = make_email(subject="Weekly sync", body="See you tomorrow")
        assert high.urgency_score > low.urgency_score
        assert high.urgency_score >= 4

    def test_text_email(self):
        text = """From: alice@co.com
To: bob@co.com
Subject: Fix deployment
Date: 2024-03-15 10:00
Body:
This deployment is blocking us. Please help immediately.

---

From: bob@co.com
To: alice@co.com, carol@co.com
Subject: Re: Fix deployment
Date: 2024-03-15 11:00
Body:
On it, will fix ASAP.
"""
        emails = parse_emails_text(text)
        assert len(emails) == 2
        assert emails[0].sender == "alice@co.com"
        assert "bob@co.com" in emails[1].sender

    def test_reply_detection(self):
        data = json.dumps([
            {"from": "a@b.com", "to": ["c@d.com"],
             "subject": "Re: something", "body": "reply",
             "timestamp": "2024-03-15T10:00:00"}
        ])
        emails = parse_emails_json(data)
        assert emails[0].is_reply


# ─── Task Model ───────────────────────────────────────────────────────────────

class TestTaskModel:
    def test_is_overdue(self):
        overdue = make_task(status=TaskStatus.PENDING, deadline=past(2))
        on_time = make_task(status=TaskStatus.PENDING, deadline=future(2))
        done    = make_task(status=TaskStatus.DONE,    deadline=past(2))
        assert overdue.is_overdue is True
        assert on_time.is_overdue is False
        assert done.is_overdue is False

    def test_days_overdue(self):
        t = make_task(status=TaskStatus.PENDING, deadline=past(5))
        assert t.days_overdue >= 4.9

    def test_has_owner(self):
        with_owner    = make_task(owner="alice")
        without_owner = Task(id="T", title="T")
        assert with_owner.has_owner is True
        assert without_owner.has_owner is False

    def test_all_owners(self):
        t = Task(id="T", title="T", owner="alice", assignees=["bob", "carol"])
        assert "alice" in t.all_owners
        assert "bob"   in t.all_owners
        assert "carol" in t.all_owners


# ─── Detectors ────────────────────────────────────────────────────────────────

class TestMissedTaskDetector:
    def test_detects_overdue(self):
        tasks = [make_task(status=TaskStatus.PENDING, deadline=past(3))]
        findings = MissedTaskDetector().detect(tasks, [], [])
        assert len(findings) == 1
        assert findings[0].detector == "missed_task"

    def test_no_finding_for_done(self):
        tasks = [make_task(status=TaskStatus.DONE, deadline=past(3))]
        findings = MissedTaskDetector().detect(tasks, [], [])
        assert len(findings) == 0

    def test_no_finding_for_future_deadline(self):
        tasks = [make_task(status=TaskStatus.PENDING, deadline=future(3))]
        findings = MissedTaskDetector().detect(tasks, [], [])
        assert len(findings) == 0

    def test_severity_scales_with_days(self):
        tasks_7d = [make_task(status=TaskStatus.PENDING,
                              deadline=past(7), priority="medium")]
        tasks_1d = [make_task(status=TaskStatus.PENDING,
                              deadline=past(1), priority="medium")]
        f7 = MissedTaskDetector().detect(tasks_7d, [], [])
        f1 = MissedTaskDetector().detect(tasks_1d, [], [])
        sev_order = {Severity.CRITICAL: 0, Severity.HIGH: 1,
                     Severity.MEDIUM: 2, Severity.LOW: 3}
        assert sev_order[f7[0].severity] <= sev_order[f1[0].severity]

    def test_critical_priority_raises_severity(self):
        t_low    = make_task(status=TaskStatus.PENDING, deadline=past(1), priority="low")
        t_crit   = make_task(status=TaskStatus.PENDING, deadline=past(1), priority="critical")
        f_low    = MissedTaskDetector().detect([t_low], [], [])
        f_crit   = MissedTaskDetector().detect([t_crit], [], [])
        sev_order = {Severity.CRITICAL: 0, Severity.HIGH: 1,
                     Severity.MEDIUM: 2, Severity.LOW: 3}
        assert sev_order[f_crit[0].severity] <= sev_order[f_low[0].severity]


class TestOwnershipGapDetector:
    def test_detects_unowned_task(self):
        tasks = [Task(id="T", title="Orphan Task", status=TaskStatus.IN_PROGRESS,
                      created_at=past(10), priority="high")]
        findings = OwnershipGapDetector().detect(tasks, [], [])
        assert any(f.detector == "ownership_gap" for f in findings)
        assert any("nowned" in f.title for f in findings)

    def test_no_finding_for_owned_task(self):
        tasks = [make_task(owner="alice@co.com", status=TaskStatus.PENDING)]
        emails = [make_email(sender="alice@co.com", ts=now())]
        findings = OwnershipGapDetector().detect(tasks, [], emails)
        # Should not flag "silent owner" if active
        silent = [f for f in findings if "silent" in f.title.lower()]
        assert len(silent) == 0

    def test_detects_silent_owner(self):
        tasks = [make_task(owner="ghost@co.com", status=TaskStatus.IN_PROGRESS,
                           deadline=future(1), priority="high")]
        # No log/email activity from ghost@co.com
        findings = OwnershipGapDetector().detect(tasks, [], [])
        assert any("ghost@co.com" in f.title for f in findings)

    def test_skips_done_tasks(self):
        tasks = [Task(id="T", title="Done Task", status=TaskStatus.DONE)]
        findings = OwnershipGapDetector().detect(tasks, [], [])
        assert len(findings) == 0


class TestDelayDetector:
    def test_detects_stalled_pending(self):
        tasks = [make_task(status=TaskStatus.PENDING,
                           created=past(10), updated=past(10))]
        findings = DelayDetector().detect(tasks, [], [])
        assert len(findings) >= 1

    def test_detects_stalled_blocked(self):
        tasks = [make_task(status=TaskStatus.BLOCKED,
                           created=past(5), updated=past(5))]
        findings = DelayDetector().detect(tasks, [], [])
        assert any(f.detector == "delay" for f in findings)

    def test_no_finding_for_recently_updated(self):
        tasks = [make_task(status=TaskStatus.IN_PROGRESS,
                           created=past(10), updated=past(1))]
        findings = DelayDetector().detect(tasks, [], [])
        assert len(findings) == 0

    def test_no_finding_for_done(self):
        tasks = [make_task(status=TaskStatus.DONE,
                           created=past(30), updated=past(30))]
        findings = DelayDetector().detect(tasks, [], [])
        assert len(findings) == 0


class TestLogErrorDetector:
    def test_detects_critical_log(self):
        logs = [make_log("Payment system down", "CRITICAL")]
        findings = LogErrorDetector().detect([], logs, [])
        assert any(f.severity == Severity.CRITICAL for f in findings)

    def test_detects_error_burst(self):
        # 6 errors within 1 minute
        base = past(1)
        logs = [
            make_log(f"Error {i}", "ERROR",
                     ts=base + timedelta(seconds=i*5))
            for i in range(6)
        ]
        findings = LogErrorDetector({"error_burst_threshold": 5}).detect([], logs, [])
        assert any("burst" in f.title.lower() for f in findings)

    def test_detects_repeated_errors(self):
        logs = [make_log("DB connection refused", "ERROR") for _ in range(4)]
        findings = LogErrorDetector({"repeated_error_threshold": 3}).detect([], logs, [])
        assert any("repeated" in f.title.lower() for f in findings)

    def test_no_finding_for_info_logs(self):
        logs = [make_log("All good", "INFO") for _ in range(10)]
        findings = LogErrorDetector().detect([], logs, [])
        assert len(findings) == 0


class TestEmailEscalationDetector:
    def test_detects_urgent_email(self):
        emails = [make_email(
            subject="URGENT ASAP critical emergency",
            body="This is a critical blocker. Escalating immediately.",
            ts=past(0.1),
        )]
        findings = EmailEscalationDetector({"urgency_threshold": 2}).detect([], [], emails)
        assert len(findings) >= 1
        assert any("urgent" in f.title.lower() or "escalat" in f.title.lower()
                   for f in findings)

    def test_no_finding_for_normal_email(self):
        emails = [make_email(subject="Team lunch tomorrow", body="See you at noon.")]
        findings = EmailEscalationDetector().detect([], [], emails)
        assert len(findings) == 0

    def test_detects_escalating_thread(self):
        # Same subject = same thread
        emails = [
            make_email(subject="Deployment update", body="Just checking in",
                       ts=past(3)),
            make_email(subject="Deployment update", body="still waiting please respond",
                       ts=past(2)),
            make_email(subject="Deployment update",
                       body="URGENT ASAP critical blocker emergency escalate",
                       ts=past(1)),
        ]
        findings = EmailEscalationDetector({"urgency_threshold": 2}).detect([], [], emails)
        assert any("escal" in f.title.lower() for f in findings)


# ─── Full Engine ──────────────────────────────────────────────────────────────

class TestFlowGuardEngine:
    def test_analyze_empty(self):
        engine = FlowGuardEngine()
        report = engine.analyze()
        assert report.total == 0
        assert report.tasks_analyzed == 0

    def test_analyze_basic(self):
        tasks = [
            make_task("T1", "Overdue critical", deadline=past(5),
                      status=TaskStatus.PENDING, priority="critical"),
            make_task("T2", "Done task", deadline=past(1),
                      status=TaskStatus.DONE),
        ]
        engine = FlowGuardEngine()
        report = engine.analyze(tasks=tasks)
        assert report.total >= 1
        assert any(f.task and f.task.id == "T1" for f in report.findings)

    def test_report_sorted_by_severity(self):
        tasks = [
            make_task("T1", deadline=past(1), status=TaskStatus.PENDING, priority="low"),
            make_task("T2", deadline=past(10), status=TaskStatus.PENDING, priority="critical"),
        ]
        report = FlowGuardEngine().analyze(tasks=tasks)
        sev_order = {Severity.CRITICAL: 0, Severity.HIGH: 1,
                     Severity.MEDIUM: 2, Severity.LOW: 3}
        orders = [sev_order[f.severity] for f in report.findings]
        assert orders == sorted(orders)

    def test_analysis_window_filters_old_logs(self):
        old_log = make_log("old error", "CRITICAL", ts=past(60))
        new_log = make_log("new critical", "CRITICAL", ts=past(1))
        report  = FlowGuardEngine().analyze(logs=[old_log, new_log], window_days=30)
        # Should only include new_log
        assert report.logs_analyzed == 1

    def test_report_to_dict(self):
        tasks  = [make_task("T1", deadline=past(3),
                            status=TaskStatus.PENDING, priority="high")]
        report = FlowGuardEngine().analyze(tasks=tasks)
        d      = report.to_dict()
        assert "generated_at" in d
        assert "summary"      in d
        assert "findings"     in d
        assert isinstance(d["findings"], list)

    def test_report_by_severity(self):
        tasks = [make_task("T1", deadline=past(10), status=TaskStatus.PENDING,
                           priority="critical")]
        report = FlowGuardEngine().analyze(tasks=tasks)
        assert len(report.by_severity(Severity.CRITICAL)) == report.critical_count


# ─── Reporters ────────────────────────────────────────────────────────────────

class TestReporters:
    def _make_report(self):
        tasks = [
            make_task("T1", deadline=past(5), status=TaskStatus.PENDING,
                      priority="critical", owner="alice@co.com"),
            Task(id="T2", title="Unowned", status=TaskStatus.PENDING,
                 created_at=past(10), priority="high"),
        ]
        logs = [make_log("CRITICAL system failure", "CRITICAL")]
        return FlowGuardEngine().analyze(tasks=tasks, logs=logs)

    def test_json_reporter(self):
        from flowguard.reporter import JsonReporter
        report = self._make_report()
        out    = JsonReporter().render(report)
        data   = json.loads(out)
        assert "findings" in data
        assert "summary"  in data

    def test_markdown_reporter(self):
        from flowguard.reporter import MarkdownReporter
        report = self._make_report()
        md     = MarkdownReporter().render(report)
        assert "# FlowGuard Report" in md
        assert "## Summary"         in md

    def test_json_reporter_save(self, tmp_path):
        from flowguard.reporter import JsonReporter
        report = self._make_report()
        path   = str(tmp_path / "report.json")
        JsonReporter().save(report, path)
        with open(path) as f:
            data = json.load(f)
        assert "findings" in data

    def test_markdown_reporter_save(self, tmp_path):
        from flowguard.reporter import MarkdownReporter
        report = self._make_report()
        path   = str(tmp_path / "report.md")
        MarkdownReporter().save(report, path)
        content = Path(path).read_text()
        assert "FlowGuard" in content


# ─── Demo Smoke Test ──────────────────────────────────────────────────────────

class TestDemo:
    def test_demo_produces_findings(self):
        from flowguard.cli import _load_demo_data
        tasks, logs, emails = _load_demo_data()
        assert len(tasks)  > 0
        assert len(logs)   > 0
        assert len(emails) > 0

        report = FlowGuardEngine().analyze(tasks=tasks, logs=logs, emails=emails)
        assert report.total > 0
        assert report.tasks_analyzed == len(tasks)


# ─── Runner ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import traceback
    passed = failed = 0
    test_classes = [
        TestParseDt, TestTaskParsers, TestLogParsers, TestEmailParsers,
        TestTaskModel, TestMissedTaskDetector, TestOwnershipGapDetector,
        TestDelayDetector, TestLogErrorDetector, TestEmailEscalationDetector,
        TestFlowGuardEngine, TestReporters, TestDemo,
    ]
    for cls in test_classes:
        instance = cls()
        for name in dir(cls):
            if not name.startswith("test_"):
                continue
            method = getattr(instance, name)
            try:
                # Handle pytest fixtures (tmp_path)
                import inspect
                sig = inspect.signature(method)
                if "tmp_path" in sig.parameters:
                    import tempfile
                    with tempfile.TemporaryDirectory() as tmpdir:
                        method(tmp_path=Path(tmpdir))
                else:
                    method()
                print(f"  ✓ {cls.__name__}.{name}")
                passed += 1
            except Exception as e:
                print(f"  ✗ {cls.__name__}.{name}: {e}")
                if "--verbose" in sys.argv:
                    traceback.print_exc()
                failed += 1
    print(f"\n  {passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
