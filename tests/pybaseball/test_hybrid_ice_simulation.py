from pybaseball.simulations.hybrid_ice import (
    PlayerProfile,
    evaluate_player,
    format_results,
    run_simulation,
)


def _profiles():
    return [
        PlayerProfile(
            name="Taylor Blades",
            edge_work=90,
            transition_speed=85,
            puck_control=80,
            strength=70,
            stamina=75,
        ),
        PlayerProfile(
            name="Casey Summit",
            edge_work=86,
            transition_speed=88,
            puck_control=82,
            strength=78,
            stamina=80,
        ),
    ]


def test_run_simulation_orders_by_ice_index():
    profiles = _profiles()
    results = run_simulation(profiles, intensity=1.0)

    assert [result.name for result in results] == ["Casey Summit", "Taylor Blades"]
    assert results[0].ice_index == 83.9
    assert results[1].ice_index == 82.25


def test_evaluate_player_recommendations():
    profile = PlayerProfile(
        name="Jordan Ice",
        edge_work=88,
        transition_speed=87,
        puck_control=89,
        strength=60,
        stamina=58,
    )

    result = evaluate_player(profile, intensity=1.2)
    assert result.recommendation == "Recovery"


def test_format_results_creates_table():
    results = run_simulation(_profiles(), intensity=1.0)
    table = format_results(results)

    assert "Name" in table.splitlines()[0]
    assert "Casey Summit" in table
    assert "Taylor Blades" in table
