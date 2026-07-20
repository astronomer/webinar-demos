"""
Presentation helpers for the AstroTrips demo.

Keeps rendering logic out of the DAG files so the pipeline definitions stay
readable. render_finding turns a structured Finding into a boxed summary
suitable for the task logs.
"""

import textwrap

from include.models import Finding

_WIDTH = 78


def render_finding(finding: Finding | dict) -> str:
    """Render a Finding as a boxed, log-friendly summary string.

    Accepts either a Finding or the dict Common AI may hand back through XCom.
    """
    if not isinstance(finding, Finding):
        finding = Finding.model_validate(finding)

    def rule(char: str = "─") -> str:
        return char * _WIDTH

    def block(label: str, text: str) -> list[str]:
        lines = [f"  {label}"]
        for line in textwrap.wrap(text, width=_WIDTH - 4) or [""]:
            lines.append(f"    {line}")
        return lines

    out = [
        "",
        f"╭{rule()}╮",
        f"  ASTROTRIPS · AUTONOMOUS REVENUE INVESTIGATION      confidence: {finding.confidence.upper()}",
        f"├{rule()}┤",
        *block("HEADLINE", finding.headline),
        "",
        *block("ROOT CAUSE", finding.root_cause),
        "",
        "  SUPPORTING EVIDENCE",
    ]
    for item in finding.supporting_evidence:
        wrapped = textwrap.wrap(item, width=_WIDTH - 6)
        out.append(f"    • {wrapped[0] if wrapped else ''}")
        out.extend(f"      {line}" for line in wrapped[1:])
    out += [
        "",
        *block("RECOMMENDED NEXT STEP", finding.recommended_next_step),
        f"╰{rule()}╯",
        "",
    ]

    return "\n".join(out)
