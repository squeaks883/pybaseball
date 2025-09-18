from __future__ import annotations

from pathlib import Path
import tempfile

import pandas as pd
import pytest
import requests

from pybaseball.datasources import ourlads


class DummyResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if not (200 <= self.status_code < 300):
            raise requests.HTTPError(f"status code: {self.status_code}")


def fake_get_factory(html: str):
    def _fake_get(url: str, timeout: int = 30) -> DummyResponse:  # pragma: no cover - simple wrapper
        return DummyResponse(html)

    return _fake_get


SAMPLE_HTML = """
<table>
    <thead>
        <tr>
            <th>Pos</th>
            <th>No</th>
            <th>No</th>
            <th>No</th>
        </tr>
        <tr>
            <th></th>
            <th>Player 1</th>
            <th>Player 2</th>
            <th>Player 3</th>
        </tr>
    </thead>
    <tbody>
        <tr><td>Offense</td><td></td><td></td><td></td></tr>
        <tr><td>QB</td><td>Allen, Josh 26S</td><td></td><td></td></tr>
        <tr><td>RB</td><td>Cook, James</td><td>Murray, Latavius 28</td><td></td></tr>
        <tr><td>WR</td><td>Diggs, Stefon 26S</td><td>Davis, Gabriel</td><td>Shakir, Khalil</td></tr>
        <tr><td>SWR</td><td>Beasley, Cole</td><td></td><td></td></tr>
        <tr><td>Defense</td><td></td><td></td><td></td></tr>
    </tbody>
</table>
"""


def test_read_starters_ourlads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ourlads.requests, "get", fake_get_factory(SAMPLE_HTML))

    result = ourlads.read_starters_ourlads(["buf"])

    expected = pd.DataFrame(
        [
            {"team": "buf", "position": "QB", "player": "Josh Allen", "status": "ACT"},
            {"team": "buf", "position": "RB1", "player": "James Cook", "status": "ACT"},
            {"team": "buf", "position": "RB2", "player": "Latavius Murray", "status": "ACT"},
            {"team": "buf", "position": "WR1", "player": "Stefon Diggs", "status": "ACT"},
            {"team": "buf", "position": "WR2", "player": "Gabriel Davis", "status": "ACT"},
            {"team": "buf", "position": "WR3", "player": "Khalil Shakir", "status": "ACT"},
            {"team": "buf", "position": "SLOT", "player": "Cole Beasley", "status": "ACT"},
        ]
    )

    result = result.sort_values("position").reset_index(drop=True)
    expected = expected.sort_values("position").reset_index(drop=True)
    pd.testing.assert_frame_equal(result, expected)


def test_read_starters_with_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ourlads.requests, "get", fake_get_factory(SAMPLE_HTML))

    with tempfile.TemporaryDirectory() as tmp_dir:
        override_path = Path(tmp_dir) / "starters_override.csv"
        override_df = pd.DataFrame(
            [{"team": "buf", "position": "WR1", "player": "Override Receiver"}]
        )
        override_df.to_csv(override_path, index=False)

        result = ourlads.read_starters_ourlads(["buf"], override_path=override_path)

    wr1 = result[result["position"] == "WR1"]
    assert len(wr1) == 1
    assert wr1.iloc[0]["player"] == "Override Receiver"
    assert wr1.iloc[0]["status"] == "ACT"

    remaining_positions = set(result["position"])
    assert {"QB", "RB1", "RB2", "WR2", "WR3", "SLOT"}.issubset(remaining_positions)
