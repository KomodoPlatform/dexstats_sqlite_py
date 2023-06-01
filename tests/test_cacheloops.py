#!/usr/bin/env python3
import sys
import pytest
sys.path.append("../dexstats_sqlite_py")
import cache_loops


@pytest.fixture
def setup_loops():
    loops = cache_loops.CacheLoops()
    yield loops


def test_gecko_loop(setup_loops):
    loops = setup_loops
    assert loops.refresh_gecko_cache()


def test_summary_loop(setup_loops):
    loops = setup_loops
    assert loops.refresh_summary_cache()


def test_ticker_loop(setup_loops):
    loops = setup_loops
    assert loops.refresh_ticker_cache()


def test_adex_loop(setup_loops):
    loops = setup_loops
    assert loops.refresh_adex_cache()


def test_adex_fortnight_loop(setup_loops):
    loops = setup_loops
    assert loops.refresh_adex_fortnight_cache()
