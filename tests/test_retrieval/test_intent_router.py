"""Intent router must match on whole words only."""

from __future__ import annotations

import pytest

from src.retrieval.intent_router import IntentRouter


@pytest.fixture
def router() -> IntentRouter:
    return IntentRouter()


def test_default_intent_is_specific(router: IntentRouter) -> None:
    assert router.classify("what is Python") == "specific"
    assert router.classify("") == "specific"


def test_global_theme_keywords(router: IntentRouter) -> None:
    for q in (
        "summarize the corpus",
        "give me a summary of the docs",
        "what are the main themes",
        "big-picture overview",
    ):
        assert router.classify(q) == "global_theme", q


def test_multi_hop_keywords(router: IntentRouter) -> None:
    for q in (
        "how does Alice relate to Bob",
        "find the path between X and Y",
        "what connects these people",
        "show relationships here",
    ):
        assert router.classify(q) == "multi_hop", q


def test_no_substring_misrouting(router: IntentRouter) -> None:
    """Previous substring matcher routed these wrong."""
    # "related" used to match "relatedness" / "correlate" — not anymore.
    assert router.classify("find correlated articles") == "specific"
    # "overview" as a substring of "overviewed" shouldn't match.
    assert router.classify("overviewed papers") == "specific"


def test_disabled_lanes_fall_through() -> None:
    r = IntentRouter(enable_global=False, enable_multihop=False)
    assert r.classify("summarize everything") == "specific"
    assert r.classify("how does A connect to B") == "specific"


def test_punctuation_tolerated(router: IntentRouter) -> None:
    assert router.classify("Summary?") == "global_theme"
    assert router.classify("Relationship!") == "multi_hop"
