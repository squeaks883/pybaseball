"""Standalone Hybrid Ice v2.3+ simulation runner."""

from __future__ import annotations

from pybaseball.simulations.hybrid_ice import (
    PlayerProfile,
    format_results,
    run_simulation,
)


def _sample_profiles() -> list[PlayerProfile]:
    return [
        PlayerProfile(
            name="Riley 'Edge' Dawson",
            edge_work=90,
            transition_speed=94,
            puck_control=89,
            strength=82,
            stamina=85,
        ),
        PlayerProfile(
            name="Mika Snow",
            edge_work=88,
            transition_speed=91,
            puck_control=86,
            strength=78,
            stamina=88,
        ),
        PlayerProfile(
            name="Jordan Frost",
            edge_work=84,
            transition_speed=86,
            puck_control=90,
            strength=80,
            stamina=83,
        ),
    ]


def main() -> None:
    profiles = _sample_profiles()
    intensity = 1.12

    results = run_simulation(profiles, intensity=intensity)

    print("HYBRID ICE V2.3+ SIMULATION FORGE")
    print("=" * 34)
    print(f"Configured training intensity: {intensity}")
    print()
    print(format_results(results))


if __name__ == "__main__":
    main()
