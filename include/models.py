"""
Structured-output models
"""

from typing import Literal

from pydantic import BaseModel, Field

QuestionType = Literal["trend", "comparison", "breakdown", "ranking", "anomaly", "other"]


class QuerySpec(BaseModel):
    """Structured interpretation of a natural-language analytics question."""

    question_type: QuestionType = Field(description="The shape of the question being asked.")
    metrics: list[str] = Field(
        default_factory=list,
        description="Business metrics requested, e.g. net_revenue, passengers, refunds.",
    )
    dimensions: list[str] = Field(
        default_factory=list,
        description="Group-by dimensions requested, e.g. planet, promo_code, month.",
    )
    time_range: str = Field(
        default="",
        description="Time scope in plain words, e.g. 'last quarter', 'all of 2025'.",
    )
    is_answerable: bool = Field(
        description="True only if this can be answered from the AstroTrips warehouse."
    )
    reason: str = Field(
        description="If not answerable, one plain sentence on why (missing data, out of scope)."
    )


class Answer(BaseModel):
    """Plain-language answer composed from real query results."""

    headline: str = Field(description="One-sentence direct answer to the question.")
    explanation: str = Field(description="A short paragraph grounded strictly in the rows.")
    caveats: list[str] = Field(
        default_factory=list, description="Any limitations or assumptions worth stating."
    )
    followups: list[str] = Field(
        default_factory=list, description="Up to three sensible follow-up questions."
    )


class Finding(BaseModel):
    """Structured result of the autonomous revenue investigation."""

    headline: str = Field(description="One-sentence summary of what happened.")
    root_cause: str = Field(description="The single most likely driver, stated plainly.")
    supporting_evidence: list[str] = Field(
        default_factory=list,
        description="Concrete figures/facts from the queries that support the root cause.",
    )
    recommended_next_step: str = Field(description="One actionable next step for the business.")
    confidence: Literal["low", "medium", "high"] = Field(
        description="Confidence in the root cause given the evidence gathered."
    )
