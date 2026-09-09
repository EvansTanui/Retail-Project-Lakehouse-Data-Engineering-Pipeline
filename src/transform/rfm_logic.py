"""
RFM (Recency, Frequency, Monetary) customer segmentation logic.

Kept as plain Python so the scoring rules can be unit tested directly,
independent of how recency/frequency/monetary get computed in Spark
(typically via a groupBy/agg on the silver sales table). The gold
transform calls score_customer() inside a pandas UDF (applyInPandas).
"""

from dataclasses import dataclass


@dataclass
class RFMScore:
    recency_score: int  # 1 (long ago) to 5 (very recent)
    frequency_score: int  # 1 (rare) to 5 (frequent)
    monetary_score: int  # 1 (low spend) to 5 (high spend)
    segment: str


def _score_from_thresholds(value: float, thresholds: list[float], reverse: bool = False) -> int:
    """Map a value to a 1-5 score given 4 ascending threshold cut points.
    reverse=True means a *smaller* value scores higher (used for recency,
    where fewer days-since-last-purchase is better)."""
    score = 1
    for i, t in enumerate(thresholds):
        if value >= t:
            score = i + 2
    return (6 - score) if reverse else score


def score_customer(
    recency_days: float,
    frequency: int,
    monetary: float,
    recency_thresholds: list[float] = [30, 60, 120, 240],
    frequency_thresholds: list[float] = [2, 5, 10, 20],
    monetary_thresholds: list[float] = [50, 150, 400, 1000],
) -> RFMScore:
    """Score a single customer on recency/frequency/monetary and assign a
    named segment. Thresholds are tunable per business context; the
    defaults assume days-since-last-purchase for recency and total spend
    in the same currency unit as the sales table for monetary."""

    r = _score_from_thresholds(recency_days, recency_thresholds, reverse=True)
    f = _score_from_thresholds(frequency, frequency_thresholds)
    m = _score_from_thresholds(monetary, monetary_thresholds)

    segment = _assign_segment(r, f, m)
    return RFMScore(recency_score=r, frequency_score=f, monetary_score=m, segment=segment)


def _assign_segment(r: int, f: int, m: int) -> str:
    """Simple, explainable segment rules on top of the 1-5 R/F/M scores."""
    avg = (r + f + m) / 3

    if r >= 4 and f >= 4 and m >= 4:
        return "champions"
    if r >= 4 and f <= 2:
        return "new_customers"
    if r <= 2 and f >= 4 and m >= 4:
        return "at_risk_high_value"
    if r <= 2 and f <= 2 and m <= 2:
        return "lost"
    if avg >= 3.5:
        return "loyal"
    if avg <= 2:
        return "hibernating"
    return "needs_attention"
