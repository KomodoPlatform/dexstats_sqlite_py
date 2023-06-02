#!/usr/bin/env python3
import sys
import pytest
sys.path.append("../dexstats_sqlite_py")
import models


@pytest.fixture
def setup_cache_updates():
    yield models.CacheUpdate()


def test_gecko(setup_cache_updates):
    updates = setup_cache_updates
    assert "result" in updates.gecko_data()


def test_summary(setup_cache_updates):
    updates = setup_cache_updates
    assert "result" in updates.summary()


def test_ticker(setup_cache_updates):
    updates = setup_cache_updates
    assert "result" in updates.ticker()


def test_adex(setup_cache_updates):
    updates = setup_cache_updates
    assert "result" in updates.adex()


def test_adex_fortnight(setup_cache_updates):
    updates = setup_cache_updates
    assert "result" in updates.adex_fortnight()


def test_coins(setup_cache_updates):
    updates = setup_cache_updates
    assert "result" in updates.coins()


def test_coins_config(setup_cache_updates):
    updates = setup_cache_updates
    assert "result" in updates.coins_config()
