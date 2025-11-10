"""Hybrid Ice v2.3+ training simulation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List


@dataclass(frozen=True)
class PlayerProfile:
    """Attribute container describing an athlete in the hybrid program."""

    name: str
    edge_work: float
    transition_speed: float
    puck_control: float
    strength: float
    stamina: float


@dataclass(frozen=True)
class TrainingSessionResult:
    """Outcome of the simulated training session."""

    name: str
    ice_index: float
    fatigue_index: float
    recommendation: str


def _validate_intensity(intensity: float) -> None:
    if intensity <= 0:
        raise ValueError("intensity must be a positive value")


def _weighted_attribute_sum(profile: PlayerProfile) -> float:
    return (
        profile.edge_work * 0.30
        + profile.transition_speed * 0.25
        + profile.puck_control * 0.20
        + profile.strength * 0.15
        + profile.stamina * 0.10
    )


def _fatigue_baseline(profile: PlayerProfile) -> float:
    return (profile.strength * 0.40 + profile.stamina * 0.60) / 100.0


def evaluate_player(profile: PlayerProfile, *, intensity: float = 1.0) -> TrainingSessionResult:
    """Evaluate a single player for the requested training intensity."""

    _validate_intensity(intensity)

    ice_index = round(_weighted_attribute_sum(profile) * intensity, 2)
    fatigue_index = round(max(0.0, intensity * 1.35 - _fatigue_baseline(profile)), 2)

    if fatigue_index < 0.50:
        recommendation = "Ready"
    elif fatigue_index < 0.90:
        recommendation = "Monitor"
    else:
        recommendation = "Recovery"

    return TrainingSessionResult(
        name=profile.name,
        ice_index=ice_index,
        fatigue_index=fatigue_index,
        recommendation=recommendation,
    )


def run_simulation(
    profiles: Iterable[PlayerProfile], *, intensity: float = 1.0
) -> List[TrainingSessionResult]:
    """Run the hybrid ice simulation over the provided profiles."""

    _validate_intensity(intensity)

    results = [evaluate_player(profile, intensity=intensity) for profile in profiles]
    return sorted(results, key=lambda result: result.ice_index, reverse=True)


def format_results(results: Iterable[TrainingSessionResult]) -> str:
    """Format results as a simple text table."""

    rows = ["Name                 Ice Idx  Fatigue  Recommendation"]
    for result in results:
        rows.append(
            f"{result.name:<20}  {result.ice_index:>7.2f}   "
            f"{result.fatigue_index:>6.2f}   {result.recommendation}"
        )
    return "\n".join(rows)
