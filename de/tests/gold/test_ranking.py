from dataclasses import replace

from de.gold.calculators.explanation import KpiCalculation
from de.gold.calculators.priority import PriorityResult, rank_priorities


def _result(intersection, score, congestion, queue):
    calculation = KpiCalculation("INTERSECTION_PRIORITY_WINDOW", "v1.0", score, "SCORE_0_100", "VALID", "VALID", (), "{}")
    return PriorityResult(intersection, "window", score, congestion, queue, calculation)


def test_ranking_uses_all_locked_tie_breakers():
    rows = (_result("J2", 80, 70, 50), _result("J1", 80, 70, 50), _result("J3", 70, 90, 90))
    ranked = {item.intersection_id: item.rank for item in rank_priorities(rows)}
    assert ranked == {"J1": 1, "J2": 2, "J3": 3}

