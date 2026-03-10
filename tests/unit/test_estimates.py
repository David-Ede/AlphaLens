from __future__ import annotations

from fg.normalization.estimates import normalize_estimates
from fg.settings import Settings


def test_normalize_estimates_supports_live_fmp_stable_shape(settings: Settings) -> None:
    payload = {
        "ticker": "ABBV",
        "as_of_date": "2026-03-10",
        "period": "annual",
        "rows": [
            {
                "symbol": "ABBV",
                "date": "2027-12-31",
                "epsAvg": 12.34,
                "epsHigh": 13.2,
                "epsLow": 11.8,
                "numAnalystsEps": 7,
            }
        ],
    }

    frame = normalize_estimates(settings, "0001551152", payload)

    assert len(frame) == 1
    row = frame.iloc[0]
    assert int(row["target_fiscal_year"]) == 2027
    assert str(row["target_period_end_date"]) == "2027-12-31"
    assert float(row["mean_value"]) == 12.34
    assert float(row["high_value"]) == 13.2
    assert float(row["low_value"]) == 11.8
    assert int(row["analyst_count"]) == 7
