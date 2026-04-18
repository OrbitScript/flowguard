"""
flowguard/reporter.py
──────────────────────
Terminal reporter and export utilities for FlowGuard.

Outputs:
  TerminalReporter — colorized, structured terminal output
  JsonReporter     — structured JSON output
  MarkdownReporter — markdown report for tickets/wikis
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime
from typing import List, Optional

from flowguard.engine import FailureReport, Finding, Severity


# ─── ANSI ─────────────────────────────────────────────────────────────────────

class C:
    RST  = "\033[0m";  BOLD = "\033[1m";  DIM  = "\033[2m"; ITALIC = "\033[3m"
    RED  = "\033[31m"; GRN  = "\033[32m"; YLW  = "\033[33m"; BLU  = "\033[34m"
    MGT  = "\033[35m"; CYN  = "\033[36m"; GRY  = "\033[90m"
    BRED = "\033[91m"; BGRN = "\033[92m"; BYLW = "\033[93m"; BBLU = "\033[94m"
    BMGT = "\033[95m"; BCYN = "\033[96m"; BWHT = "\033[97m"

def col(t, *c): return "".join(C.__dict__[k] for k in c) + str(t) + C.RST
def tw(): return shutil.get_terminal_size((100, 24)).columns
def hr(ch="─", c="GRY"): print(col(ch * tw(), c))


SEV_COLOR = {
    Severity.CRITICAL: ("BRED", "BOLD"),
    Severity.HIGH:     ("BYLW", "BOLD"),
    Severity.MEDIUM:   ("BCYN",),
    Severity.LOW:      ("GRY",),
}
SEV_ICON = {
    Severity.CRITICAL: "💀",
    Severity.HIGH:     "🔴",
    Severity.MEDIUM:   "🟡",
    Severity.LOW:      "🟢",
}
SEV_LABEL = {
    Severity.CRITICAL: "CRITICAL",
    Severity.HIGH:     "HIGH",
    Severity.MEDIUM:   "MEDIUM",
    Severity.LOW:      "LOW",
}

DETECTOR_LABELS = {
    "missed_task":        "Missed Task",
    "ownership_gap":      "Ownership Gap",
    "delay":              "Delay / Stall",
    "log_error":          "Log Errors",
    "email_escalation":   "Email Escalation",
}

BANNER = """
  ███████╗██╗      ██████╗ ██╗    ██╗ ██████╗ ██╗   ██╗ █████╗ ██████╗ ██████╗
  ██╔════╝██║     ██╔═══██╗██║    ██║██╔════╝ ██║   ██║██╔══██╗██╔══██╗██╔══██╗
  █████╗  ██║     ██║   ██║██║ █╗ ██║██║  ███╗██║   ██║███████║██████╔╝██║  ██║
  ██╔══╝  ██║     ██║   ██║██║███╗██║██║   ██║██║   ██║██╔══██║██╔══██╗██║  ██║
  ██║     ███████╗╚██████╔╝╚███╔███╔╝╚██████╔╝╚██████╔╝██║  ██║██║  ██║██████╔╝
  ╚═╝     ╚══════╝ ╚═════╝  ╚══╝╚══╝  ╚═════╝  ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝
"""


class TerminalReporter:
    """Print a full, colorized FlowGuard report to the terminal."""

    def __init__(self, verbose: bool = False, no_color: bool = False):
        self.verbose   = verbose
        self.no_color  = no_color

    def _col(self, t, *c):
        if self.no_color:
            return str(t)
        return col(t, *c)

    def print(self, report: FailureReport):
        self._print_banner()
        self._print_header(report)
        self._print_summary_bar(report)

        if not report.findings:
            print()
            print(self._col("  ✓  No issues detected. All clear.", "BGRN", "BOLD"))
            print()
            return

        # Group by severity
        for sev in [Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM, Severity.LOW]:
            findings = report.by_severity(sev)
            if not findings:
                continue
            self._print_severity_section(sev, findings)

        self._print_owner_summary(report)
        self._print_footer(report)

    def _print_banner(self):
        for line in BANNER.strip("\n").split("\n"):
            print(self._col(line, "BRED"))
        print(self._col("  Work Failure Detection Engine", "DIM"))
        print()

    def _print_header(self, report: FailureReport):
        hr()
        print(self._col(
            f"  Generated: {report.generated_at.strftime('%Y-%m-%d %H:%M:%S')}  ·  "
            f"Window: {report.analysis_window_days:.0f}d  ·  "
            f"Tasks: {report.tasks_analyzed}  ·  "
            f"Logs: {report.logs_analyzed}  ·  "
            f"Emails: {report.emails_analyzed}",
            "GRY"
        ))
        hr()

    def _print_summary_bar(self, report: FailureReport):
        total = report.total
        print()
        if total == 0:
            print(self._col("  ✓  No issues found", "BGRN", "BOLD"))
        else:
            parts = []
            if report.critical_count:
                parts.append(self._col(f"  {report.critical_count} CRITICAL", "BRED", "BOLD"))
            if report.high_count:
                parts.append(self._col(f"  {report.high_count} HIGH", "BYLW", "BOLD"))
            if report.medium_count:
                parts.append(self._col(f"  {report.medium_count} MEDIUM", "BCYN"))
            if report.low_count:
                parts.append(self._col(f"  {report.low_count} LOW", "GRY"))
            print("  Issues found: " + "   ".join(parts))
        print()

    def _print_severity_section(self, sev: Severity, findings: List[Finding]):
        icon  = SEV_ICON[sev]
        label = SEV_LABEL[sev]
        colors = SEV_COLOR[sev]

        hr("─")
        print(self._col(f"  {icon}  {label} ({len(findings)})", *colors))
        hr("─")
        print()

        for finding in findings:
            self._print_finding(finding)

    def _print_finding(self, f: Finding):
        colors = SEV_COLOR[f.severity]
        det    = DETECTOR_LABELS.get(f.detector, f.detector)

        # Title line
        print(self._col(f"  ▸ {f.title}", *colors))
        print(self._col(f"    Detector: {det}  ·  ID: {f.id}", "GRY"))
        print()

        # Description
        desc_lines = _wrap(f.description, tw() - 8)
        for line in desc_lines:
            print("    " + self._col(line, "BWHT"))
        print()

        # Evidence
        if f.evidence:
            print(self._col("    Evidence:", "GRY"))
            for ev in f.evidence:
                print(self._col(f"      · {ev}", "DIM"))
            print()

        # Owners
        if f.owners:
            owners_str = ", ".join(f.owners)
            print(self._col(f"    Owners: {owners_str}", "CYN"))
            print()

        # Recommendation
        if f.recommended:
            rec_lines = _wrap(f"→ {f.recommended}", tw() - 8)
            for line in rec_lines:
                print("    " + self._col(line, "BGRN"))
            print()

        if f.tags:
            tags_str = "  ".join(self._col(f"#{t}", "DIM") for t in f.tags)
            print(f"    {tags_str}")
            print()

        hr("·")
        print()

    def _print_owner_summary(self, report: FailureReport):
        """Show which owners have the most issues."""
        owner_counts: dict = {}
        for f in report.findings:
            for owner in f.owners:
                owner_counts[owner] = owner_counts.get(owner, 0) + 1

        if not owner_counts:
            return

        sorted_owners = sorted(owner_counts.items(), key=lambda x: -x[1])

        hr()
        print(self._col("  👥  Issues by Owner", "BWHT", "BOLD"))
        hr()
        print()
        for owner, count in sorted_owners[:10]:
            bar_w = min(count * 3, 30)
            bar   = "█" * bar_w
            sev_str = ""
            critical = sum(1 for f in report.findings
                          if owner in f.owners and f.severity == Severity.CRITICAL)
            if critical:
                sev_str = self._col(f"  {critical} critical", "BRED")
            print(f"  {self._col(owner.ljust(24), 'BCYN')}  "
                  f"{self._col(bar, 'BYLW')}  {count}{sev_str}")
        print()

    def _print_footer(self, report: FailureReport):
        hr()
        print()
        total = report.total
        crit  = report.critical_count
        msg   = (
            f"  {total} issue(s) detected."
            + (f" {crit} CRITICAL — immediate action required." if crit else "")
        )
        color = "BRED" if crit else "BYLW" if report.high_count else "BCYN"
        print(self._col(msg, color, "BOLD"))
        print()
        hr()
        print()


class JsonReporter:
    """Output report as JSON."""

    def __init__(self, indent: int = 2):
        self.indent = indent

    def render(self, report: FailureReport) -> str:
        return json.dumps(report.to_dict(), indent=self.indent, default=str)

    def print(self, report: FailureReport):
        print(self.render(report))

    def save(self, report: FailureReport, path: str):
        with open(path, "w") as f:
            f.write(self.render(report))
        print(f"  Saved report to: {path}")


class MarkdownReporter:
    """Output report as Markdown — for GitHub issues, Confluence, Notion, etc."""

    def render(self, report: FailureReport) -> str:
        lines = []
        lines.append("# FlowGuard Report")
        lines.append(f"*Generated: {report.generated_at.strftime('%Y-%m-%d %H:%M')}*\n")

        lines.append("## Summary")
        lines.append(f"| Severity | Count |")
        lines.append(f"|---|---|")
        lines.append(f"| 💀 Critical | {report.critical_count} |")
        lines.append(f"| 🔴 High     | {report.high_count} |")
        lines.append(f"| 🟡 Medium   | {report.medium_count} |")
        lines.append(f"| 🟢 Low      | {report.low_count} |")
        lines.append(f"| **Total**  | **{report.total}** |")
        lines.append("")

        lines.append("## Inputs")
        lines.append(f"- Tasks analyzed: {report.tasks_analyzed}")
        lines.append(f"- Log entries: {report.logs_analyzed}")
        lines.append(f"- Emails: {report.emails_analyzed}")
        lines.append(f"- Analysis window: {report.analysis_window_days:.0f} days")
        lines.append("")

        if not report.findings:
            lines.append("## ✅ No issues found")
            return "\n".join(lines)

        lines.append("## Findings")

        for sev in [Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM, Severity.LOW]:
            findings = report.by_severity(sev)
            if not findings:
                continue
            icon = SEV_ICON[sev]
            lines.append(f"\n### {icon} {SEV_LABEL[sev]} ({len(findings)})")

            for f in findings:
                det = DETECTOR_LABELS.get(f.detector, f.detector)
                lines.append(f"\n#### {f.title}")
                lines.append(f"*Detector: {det} · ID: `{f.id}`*\n")
                lines.append(f.description)
                lines.append("")
                if f.evidence:
                    lines.append("**Evidence:**")
                    for ev in f.evidence:
                        lines.append(f"- {ev}")
                    lines.append("")
                if f.owners:
                    lines.append(f"**Owners:** {', '.join(f.owners)}")
                    lines.append("")
                if f.recommended:
                    lines.append(f"**Recommendation:** {f.recommended}")
                    lines.append("")

        return "\n".join(lines)

    def print(self, report: FailureReport):
        print(self.render(report))

    def save(self, report: FailureReport, path: str):
        with open(path, "w") as f:
            f.write(self.render(report))


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _wrap(text: str, width: int) -> List[str]:
    """Wrap text to width, preserving existing newlines."""
    import textwrap
    return textwrap.wrap(text, width=max(width, 40))
