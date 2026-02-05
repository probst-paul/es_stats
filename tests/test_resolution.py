from __future__ import annotations

import pytest

from es_stats.domain.windows import WindowAnchor, WindowSpec
from es_stats.services.resolution import choose_resolution


def test_choose_resolution_prefers_30m_when_all_windows_align() -> None:
    windows = [
        WindowSpec(WindowAnchor.TRADING_DATE_CT, 510, 959, "RTH"),
        WindowSpec(WindowAnchor.TRADING_DATE_CT, 1020, 509, "ON"),
    ]
    assert choose_resolution(windows) == "30m"


def test_choose_resolution_uses_1m_for_non_aligned_start() -> None:
    windows = [WindowSpec(WindowAnchor.TRADING_DATE_CT, 515, 959, "misaligned")]
    assert choose_resolution(windows) == "1m"


def test_choose_resolution_uses_1m_for_non_aligned_end() -> None:
    windows = [WindowSpec(WindowAnchor.TRADING_DATE_CT, 510, 958, "misaligned")]
    assert choose_resolution(windows) == "1m"


def test_choose_resolution_uses_1m_if_any_window_needs_it() -> None:
    windows = [
        WindowSpec(WindowAnchor.TRADING_DATE_CT, 510, 959, "RTH"),
        WindowSpec(WindowAnchor.TRADING_DATE_CT, 540, 554, "15m slice"),
    ]
    assert choose_resolution(windows) == "1m"


def test_choose_resolution_rejects_empty_windows() -> None:
    with pytest.raises(ValueError, match="at least one window"):
        choose_resolution([])
