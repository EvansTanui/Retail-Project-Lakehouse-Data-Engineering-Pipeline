from src.transform.rfm_logic import score_customer


def test_champion_customer():
    result = score_customer(recency_days=5, frequency=25, monetary=1500)
    assert result.segment == "champions"
    assert result.recency_score == 5
    assert result.frequency_score == 5
    assert result.monetary_score == 5


def test_new_customer():
    result = score_customer(recency_days=3, frequency=1, monetary=40)
    assert result.segment == "new_customers"


def test_at_risk_high_value_customer():
    result = score_customer(recency_days=300, frequency=22, monetary=1200)
    assert result.segment == "at_risk_high_value"


def test_lost_customer():
    result = score_customer(recency_days=400, frequency=1, monetary=20)
    assert result.segment == "lost"


def test_scores_are_within_bounds():
    result = score_customer(recency_days=90, frequency=8, monetary=300)
    for score in (result.recency_score, result.frequency_score, result.monetary_score):
        assert 1 <= score <= 5
