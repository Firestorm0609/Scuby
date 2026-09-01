"""
test_scuby.py — Unit tests for Scuby's core modules.

Run with: python -m pytest test_scuby.py -v
"""
import time
import pytest
from datetime import datetime, timezone, timedelta


# ═══════════════════════════════════════════════════════════════════════════════
# gemscore.py tests
# ═══════════════════════════════════════════════════════════════════════════════

from gemscore import (
    calculate_gem_score,
    _score_liq,
    _score_age,
    _score_vol_mcap,
    _score_mcap,
    _score_price_change,
    _score_liq_mcap,
    _score_vol_consistency,
    _score_risk,
    _score_pattern_heuristic,
    WEIGHTS,
    _MAX_POS,
)


class TestScoreLiq:
    def test_ultra_high_liquidity(self):
        score, note = _score_liq(250_000)
        assert score == 1.0
        assert "ultra-deep" in note

    def test_zero_liquidity(self):
        score, note = _score_liq(0)
        assert score == 0.0
        assert "dangerously low" in note

    def test_sweet_spot(self):
        score, note = _score_liq(50_000)
        assert score == 0.9
        assert "solid" in note

    def test_boundaries(self):
        # Exact boundary values
        s1, _ = _score_liq(5_000)
        s2, _ = _score_liq(4_999)
        assert s1 > s2  # should be strictly better


class TestScoreAge:
    def test_sweet_spot_30_90min(self):
        score, note = _score_age(0.75)  # 45 minutes
        assert score == 1.0
        assert "SWEET SPOT" in note

    def test_too_old(self):
        score, _ = _score_age(72)
        assert score <= 0.2

    def test_very_new(self):
        score, note = _score_age(0.05)  # 3 minutes
        assert score == 0.3
        assert "too new" in note

    def test_prime_window(self):
        score, _ = _score_age(1.25)  # 75 minutes
        assert score == 1.0


class TestScoreVolMcap:
    def test_extreme_momentum(self):
        score, note = _score_vol_mcap(100_000, 50_000)  # 2.0x
        assert score == 1.0

    def test_zero_mcap(self):
        score, _ = _score_vol_mcap(1000, 0)
        assert score == 0.3

    def test_low_activity(self):
        score, _ = _score_vol_mcap(100, 100_000)  # 0.001x
        assert score < 0.2


class TestScoreMcap:
    def test_prime_zone(self):
        score, note = _score_mcap(20_000)
        assert score == 1.0
        assert "prime" in note

    def test_ultra_micro(self):
        score, _ = _score_mcap(1_000)
        assert score == 0.4

    def test_very_large(self):
        score, _ = _score_mcap(50_000_000)
        assert score == 0.15


class TestScorePriceChange:
    def test_parabolic_penalty(self):
        s_high, _ = _score_price_change(250, 0)  # >200%
        s_peak, _ = _score_price_change(150, 0)   # 100-200%
        assert s_high < s_peak  # parabolic should score lower

    def test_dump(self):
        score, _ = _score_price_change(-70, 0)
        assert score <= 0.1

    def test_flat(self):
        score, _ = _score_price_change(0, 0)
        assert score == 0.5


class TestScoreRisk:
    def test_low_risk(self):
        score, note = _score_risk({"score": 50, "risks": []})
        assert score == 0.0
        assert "LOW" in note

    def test_high_risk(self):
        score, note = _score_risk({"score": 800, "risks": [{"level": "danger", "name": "mint authority"}]})
        assert score == -1.0
        assert "HIGH" in note

    def test_no_report(self):
        score, note = _score_risk({})
        assert score == 0.0


class TestScorePatternHeuristic:
    def test_prime_setup(self):
        score, note = _score_pattern_heuristic(
            vol_mcap=3.5, age_h=0.75, liq=10_000, liq_mcap=0.2, mcap=50_000
        )
        assert score == 1.0
        assert "PRIME" in note

    def test_graduation_zone(self):
        score, note = _score_pattern_heuristic(
            vol_mcap=0.3, age_h=1.5, liq=5_000, liq_mcap=0.1, mcap=70_000
        )
        assert score >= 0.8
        assert "Graduation" in note

    def test_baseline(self):
        score, _ = _score_pattern_heuristic(
            vol_mcap=0.01, age_h=24, liq=500, liq_mcap=0.01, mcap=1_000_000
        )
        assert score == 0.5


class TestCalculateGemScore:
    def _make_pair(self, **overrides):
        pair = {
            "marketCap": 50_000,
            "fdv": 50_000,
            "liquidity": {"usd": 10_000},
            "volume": {"h1": 30_000, "h24": 200_000},
            "priceChange": {"h1": 25.0, "h6": 50.0, "h24": 100.0},
            "priceUsd": "0.001",
            "pairCreatedAt": (time.time() * 1000) - (45 * 60 * 1000),  # 45 min ago
        }
        pair.update(overrides)
        return pair

    def test_returns_valid_score(self):
        result = calculate_gem_score(self._make_pair(), {}, {})
        assert 0 <= result["score"] <= 100
        assert "grade" in result
        assert "breakdown" in result
        assert "meta" in result

    def test_perfect_token_scores_high(self):
        pair = self._make_pair(
            marketCap=25_000,
            liquidity={"usd": 15_000},
            volume={"h1": 50_000, "h24": 200_000},
            priceChange={"h1": 100.0},
            pairCreatedAt=(time.time() * 1000) - (60 * 60 * 1000),  # 60 min ago
        )
        result = calculate_gem_score(pair, {"score": 50, "risks": []}, {})
        assert result["score"] >= 70  # should be solid/hot

    def test_rug_token_scores_low(self):
        # A token with good signals but HIGH rug risk should score lower than one without
        clean = calculate_gem_score(self._make_pair(), {}, {})
        rug   = calculate_gem_score(
            self._make_pair(),
            {"score": 800, "risks": [{"level": "danger", "name": "mint authority"}]},
            {},
        )
        assert rug["score"] < clean["score"]  # rug risk should drag it down

    def test_zero_mcap_handled(self):
        pair = self._make_pair(marketCap=0, fdv=0)
        result = calculate_gem_score(pair, {}, {})
        assert result["score"] >= 0  # no crash

    def test_breakdown_has_all_categories(self):
        result = calculate_gem_score(self._make_pair(), {}, {})
        expected = {
            "Liquidity", "Age", "Momentum", "MCap range", "Price action",
            "Pattern", "Liq quality", "Vol health", "Rug risk",
        }
        assert set(result["breakdown"].keys()) == expected

    def test_weights_sum_correctly(self):
        positive = sum(v for v in WEIGHTS.values() if v > 0)
        assert positive == _MAX_POS
        assert positive > 0  # weights must be defined


# ═══════════════════════════════════════════════════════════════════════════════
# reminders.py tests
# ═══════════════════════════════════════════════════════════════════════════════

from reminders import parse_reminder_time


class TestParseReminderTime:
    def test_minutes(self):
        ts = parse_reminder_time("30m")
        assert ts is not None
        now = time.time()
        delta = ts - now
        assert 29 * 60 <= delta <= 31 * 60

    def test_hours(self):
        ts = parse_reminder_time("2h")
        assert ts is not None
        delta = ts - time.time()
        assert 115 * 60 <= delta <= 125 * 60

    def test_seconds(self):
        ts = parse_reminder_time("60s")
        assert ts is not None
        delta = ts - time.time()
        assert 55 <= delta <= 65

    def test_days(self):
        ts = parse_reminder_time("1d")
        assert ts is not None
        delta = ts - time.time()
        assert 23 * 3600 <= delta <= 25 * 3600

    def test_in_minutes(self):
        ts = parse_reminder_time("in 15 minutes")
        assert ts is not None
        delta = ts - time.time()
        assert 14 * 60 <= delta <= 16 * 60

    def test_clock_time_3pm(self):
        ts = parse_reminder_time("3pm")
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        assert dt.hour == 15
        assert dt.minute == 0

    def test_clock_time_9am(self):
        ts = parse_reminder_time("9am")
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        assert dt.hour == 9

    def test_clock_with_minutes(self):
        ts = parse_reminder_time("3:30pm")
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        assert dt.hour == 15
        assert dt.minute == 30

    def test_tomorrow(self):
        ts = parse_reminder_time("tomorrow 9am")
        assert ts is not None
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        now = datetime.now(timezone.utc)
        assert dt.day == (now + timedelta(days=1)).day
        assert dt.hour == 9

    def test_invalid_returns_none(self):
        assert parse_reminder_time("banana") is None
        assert parse_reminder_time("") is None
        assert parse_reminder_time("xyz") is None


# ═══════════════════════════════════════════════════════════════════════════════
# smart_filters.py tests
# ═══════════════════════════════════════════════════════════════════════════════

from smart_filters import pair_matches_filter


class TestPairMatchesFilter:
    def _make_pair(self, **overrides):
        pair = {
            "baseToken": {"address": "ABC123def456ghi789jkl012mno345pqr678stu901"},
            "marketCap": 50_000,
            "fdv": 50_000,
            "liquidity": {"usd": 10_000},
            "priceChange": {"h1": 25.0},
            "pairCreatedAt": (time.time() * 1000) - (30 * 60 * 1000),  # 30 min ago
            "volume": {"h1": 5_000},
        }
        pair.update(overrides)
        return pair

    def test_basic_match(self):
        pair = self._make_pair()
        flt = {"mcap_min": 10_000, "mcap_max": 100_000}
        assert pair_matches_filter(pair, flt, time.time() * 1000) is True

    def test_mcap_too_low(self):
        pair = self._make_pair(marketCap=5_000)
        flt = {"mcap_min": 10_000}
        assert pair_matches_filter(pair, flt, time.time() * 1000) is False

    def test_mcap_too_high(self):
        pair = self._make_pair(marketCap=200_000)
        flt = {"mcap_max": 100_000}
        assert pair_matches_filter(pair, flt, time.time() * 1000) is False

    def test_liq_too_low(self):
        pair = self._make_pair()
        flt = {"liq_min": 50_000}
        assert pair_matches_filter(pair, flt, time.time() * 1000) is False

    def test_age_filter(self):
        # Pair created 2 hours ago
        pair = self._make_pair(
            pairCreatedAt=(time.time() * 1000) - (120 * 60 * 1000)
        )
        flt = {"age_max_minutes": 60}
        assert pair_matches_filter(pair, flt, time.time() * 1000) is False

    def test_pct_change_min(self):
        pair = self._make_pair()
        pair["priceChange"]["h1"] = 10.0
        flt = {"pct_change_min": 50.0}
        assert pair_matches_filter(pair, flt, time.time() * 1000) is False

    def test_vol_min(self):
        pair = self._make_pair()
        pair["volume"]["h1"] = 100
        flt = {"vol_min_1h": 10_000}
        assert pair_matches_filter(pair, flt, time.time() * 1000) is False

    def test_empty_address_rejects(self):
        pair = self._make_pair()
        pair["baseToken"]["address"] = ""
        flt = {}
        assert pair_matches_filter(pair, flt, time.time() * 1000) is False

    def test_all_null_fields_matches(self):
        """A filter with all None fields should match any valid pair."""
        pair = self._make_pair()
        flt = {
            "mcap_min": None, "mcap_max": None,
            "liq_min": None, "liq_max": None,
            "age_max_minutes": None,
            "pct_change_min": None, "pct_change_max": None,
            "vol_min_1h": None,
        }
        assert pair_matches_filter(pair, flt, time.time() * 1000) is True


# ═══════════════════════════════════════════════════════════════════════════════
# ai.py tests (regex parser and canned replies)
# ═══════════════════════════════════════════════════════════════════════════════

from ai import _regex_parse_filter


class TestRegexParseFilter:
    def test_mcap_range(self):
        flt = _regex_parse_filter("mcap between 10k and 20k")
        assert flt["mcap_min"] == 10_000
        assert flt["mcap_max"] == 20_000

    def test_mcap_above(self):
        flt = _regex_parse_filter("above 50k mcap")
        assert flt["mcap_min"] == 50_000

    def test_mcap_under(self):
        flt = _regex_parse_filter("under 100k cap")
        assert flt["mcap_max"] == 100_000

    def test_liquidity(self):
        flt = _regex_parse_filter("liq over 5k")
        assert flt["liq_min"] == 5_000

    def test_new_keyword(self):
        flt = _regex_parse_filter("new tokens")
        assert flt["age_max_minutes"] == 60

    def test_very_new_keyword(self):
        flt = _regex_parse_filter("very new pairs")
        assert flt["age_max_minutes"] == 30

    def test_pct_change(self):
        flt = _regex_parse_filter("up more than 50%")
        assert flt["pct_change_min"] == 50.0

    def test_volume(self):
        flt = _regex_parse_filter("vol over 10k")
        assert flt["vol_min_1h"] == 10_000

    def test_millions(self):
        flt = _regex_parse_filter("mcap 1m to 5m")
        assert flt["mcap_min"] == 1_000_000
        assert flt["mcap_max"] == 5_000_000

    def test_label_generated(self):
        flt = _regex_parse_filter("mcap between 10k and 20k")
        assert "label" in flt
        assert len(flt["label"]) > 0


# ═══════════════════════════════════════════════════════════════════════════════
# utils.py tests (escape_md, safe_float, is_owner)
# ═══════════════════════════════════════════════════════════════════════════════

from utils import escape_md, safe_float, is_owner


class TestEscapeMd:
    def test_escapes_special_chars(self):
        result = escape_md("hello_world")
        assert "\\_" in result

    def test_empty_string(self):
        assert escape_md("") == ""

    def test_no_special_chars(self):
        assert escape_md("abc123") == "abc123"

    def test_parentheses(self):
        result = escape_md("test (1)")
        assert "\\(" in result
        assert "\\)" in result


class TestSafeFloat:
    def test_valid_number(self):
        assert safe_float("3.14") == 3.14

    def test_integer_string(self):
        assert safe_float("42") == 42.0

    def test_invalid_returns_default(self):
        assert safe_float("banana") == 0.0

    def test_none_returns_default(self):
        assert safe_float(None) == 0.0

    def test_custom_default(self):
        assert safe_float(None, -1.0) == -1.0

    def test_already_float(self):
        assert safe_float(3.14) == 3.14


class TestIsOwner:
    def test_none_owner_id(self):
        # OWNER_ID is None by default in test environment
        assert is_owner(12345) is False

    def test_matching_owner(self):
        import utils
        original = utils.OWNER_ID
        try:
            utils.OWNER_ID = 12345
            assert is_owner(12345) is True
        finally:
            utils.OWNER_ID = original

    def test_non_matching_owner(self):
        import utils
        original = utils.OWNER_ID
        try:
            utils.OWNER_ID = 12345
            assert is_owner(99999) is False
        finally:
            utils.OWNER_ID = original


# ═══════════════════════════════════════════════════════════════════════════════
# ai.py conversation LRU tests
# ═══════════════════════════════════════════════════════════════════════════════

from ai import _conversations, _conversations_ts, _push_history, _get_history, clear_history, _MAX_CONVERSATIONS


class TestConversationLRU:
    def setup_method(self):
        _conversations.clear()
        _conversations_ts.clear()

    def test_push_and_get(self):
        _push_history(1, "user", "hello")
        hist = _get_history(1)
        assert len(hist) == 1
        assert hist[0]["content"] == "hello"

    def test_max_history_cap(self):
        for i in range(30):
            _push_history(1, "user", f"msg {i}")
        hist = _get_history(1)
        assert len(hist) <= 20  # _MAX_HISTORY * 2

    def test_clear_history(self):
        _push_history(1, "user", "hello")
        clear_history(1)
        assert 1 not in _conversations

    def test_eviction_on_push(self):
        # Fill up with many users
        for uid in range(_MAX_CONVERSATIONS + 50):
            _push_history(uid, "user", f"msg from {uid}")
        # Should have evicted some
        assert len(_conversations) <= _MAX_CONVERSATIONS

    def test_timestamps_tracked(self):
        _push_history(42, "user", "hello")
        assert 42 in _conversations_ts
