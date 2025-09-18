"""Utilities for scraping Ourlads NFL depth chart data."""
from __future__ import annotations

from io import StringIO
from pathlib import Path
import re
from typing import Iterable, Optional

import pandas as pd
import requests


_COLUMNS = ["team", "position", "player", "status"]
_PLAYER_COLUMN_PATTERN = re.compile(r"^(?:No\s*)?Player", re.IGNORECASE)


def _ourlads_url(team: str) -> str:
    return f"https://www.ourlads.com/nfldepthcharts/pfdepthchart/{team}"


def _empty_result() -> pd.DataFrame:
    return pd.DataFrame(columns=_COLUMNS)


def _flatten_columns(columns: pd.Index) -> list[str]:
    flattened: list[str] = []
    for col in columns:
        if isinstance(col, tuple):
            parts = [
                str(part).strip()
                for part in col
                if str(part).strip() not in {"", "nan"} and not str(part).startswith("Unnamed:")
            ]
            flat = " ".join(parts).strip()
        else:
            flat = str(col).strip()
        flattened.append(flat)
    return flattened


def _clean_name(raw: str) -> str:
    cleaned = re.sub(r"\s*\d+\w*$", "", raw)
    cleaned = re.sub(r"\s*\(.*?\)", "", cleaned)
    cleaned = cleaned.strip()
    if "," in cleaned:
        parts = [part.strip() for part in cleaned.split(",", 1)]
        if len(parts) == 2:
            cleaned = f"{parts[1]} {parts[0]}".strip()
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned


def _map_pos(row_pos: str, slot_idx: Optional[int]) -> Optional[str]:
    if slot_idx is None:
        return None
    row_pos = row_pos.upper()
    if row_pos in {"QB", "RB", "FB", "TE"}:
        if row_pos == "RB":
            return "RB1" if slot_idx == 1 else "RB2"
        if row_pos == "TE":
            return "TE1" if slot_idx == 1 else "TE2"
        return row_pos
    if row_pos in {"WR", "LWR", "RWR", "SWR", "WR-SLOT", "SLOT"}:
        if row_pos in {"SWR", "WR-SLOT", "SLOT"}:
            return "SLOT" if slot_idx == 1 else None
        lookup = ["WR1", "WR2", "WR3", "WR4", "WR5"]
        if 1 <= slot_idx <= len(lookup):
            return lookup[slot_idx - 1]
    return None


def ourlads_scrape_team(team: str) -> pd.DataFrame:
    """Scrape the Ourlads depth chart for a single team."""
    url = _ourlads_url(team)
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException:
        return _empty_result()

    try:
        tables = pd.read_html(StringIO(response.text))
    except ValueError:
        return _empty_result()

    if not tables:
        return _empty_result()

    df = tables[0].copy()
    if df.empty:
        return _empty_result()

    df.columns = _flatten_columns(df.columns)
    if df.columns.empty:
        return _empty_result()
    df.rename(columns={df.columns[0]: "Pos"}, inplace=True)

    keep_cols = ["Pos"] + [col for col in df.columns[1:] if _PLAYER_COLUMN_PATTERN.match(col)]
    if len(keep_cols) <= 1:
        return _empty_result()
    df = df[keep_cols]
    df["Pos"] = df["Pos"].astype(str).str.strip()

    positions = df["Pos"].fillna("")
    upper_positions = positions.str.upper()

    offense_mask = upper_positions.eq("OFFENSE")
    defense_mask = upper_positions.eq("DEFENSE")

    if offense_mask.any():
        start_idx = offense_mask[offense_mask].index[0]
        if defense_mask.any():
            end_idx = defense_mask[defense_mask].index[0]
        else:
            end_idx = df.index[-1] + 1
        offense_df = df.loc[start_idx + 1 : end_idx - 1]
    else:
        if defense_mask.any():
            end_idx = defense_mask[defense_mask].index[0]
            offense_df = df.loc[: end_idx - 1]
        else:
            offense_df = df

    offense_df = offense_df[offense_df["Pos"].astype(str).str.strip().ne("")]
    offense_df = offense_df[~offense_df["Pos"].str.contains("Offense|Defense|Special", case=False, na=False)]
    if offense_df.empty:
        return _empty_result()

    player_columns = [col for col in offense_df.columns if col != "Pos"]
    slot_order = {col: idx + 1 for idx, col in enumerate(player_columns)}

    long_df = offense_df.melt(id_vars="Pos", var_name="slot", value_name="raw", ignore_index=False)
    long_df.dropna(subset=["raw"], inplace=True)
    long_df["raw"] = long_df["raw"].astype(str).str.strip()
    long_df = long_df[long_df["raw"].ne("")]
    if long_df.empty:
        return _empty_result()

    long_df["player"] = long_df["raw"].apply(_clean_name)
    long_df["slot_id"] = long_df["slot"].map(slot_order)
    long_df["position"] = long_df.apply(lambda row: _map_pos(row["Pos"], row["slot_id"]), axis=1)
    long_df.dropna(subset=["position"], inplace=True)
    if long_df.empty:
        return _empty_result()

    long_df["team"] = team
    long_df["status"] = "ACT"
    result = long_df.drop_duplicates(subset=["position", "player"], keep="first")
    return result[["team", "position", "player", "status"]].reset_index(drop=True)


def read_starters_ourlads(
    teams: Iterable[str], *, override_path: Optional[str | Path] = Path("starters_override.csv")
) -> pd.DataFrame:
    """Read starters for a collection of teams from Ourlads."""
    frames = [ourlads_scrape_team(team) for team in teams]
    if frames:
        out = pd.concat(frames, ignore_index=True)
    else:
        out = _empty_result()

    if out.empty:
        out = _empty_result()

    if override_path is not None:
        path = Path(override_path)
        if path.exists():
            overrides = pd.read_csv(path)
            if overrides.empty:
                return out
            overrides = overrides.copy()
            if "status" not in overrides.columns:
                overrides["status"] = "ACT"
            overrides = overrides[[col for col in _COLUMNS if col in overrides.columns]]
            for column in _COLUMNS:
                if column not in overrides.columns:
                    overrides[column] = "" if column != "status" else "ACT"
            overrides = overrides[_COLUMNS]

            if not out.empty:
                keys = overrides[["team", "position"]].drop_duplicates()
                out = out.merge(keys, on=["team", "position"], how="left", indicator=True)
                out = out[out["_merge"] == "left_only"].drop(columns="_merge")
            if out.empty:
                out = overrides.copy()
            else:
                out = pd.concat([out, overrides], ignore_index=True)
    return out.reset_index(drop=True)
