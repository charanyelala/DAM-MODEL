from __future__ import annotations

from py_backend.config import BatteryConfig


def temperature_factor(cell_temp_c: float, reference_temp_c: float = 25.0) -> float:
    """Q10-style aging acceleration.

    A value of 2 per 10C is a practical first approximation for dispatch
    optimization. Vendor test data should replace this when available.
    """
    return max(0.35, min(4.0, 2 ** ((cell_temp_c - reference_temp_c) / 10.0)))


def soc_stress_factor(soc_fraction: float) -> float:
    """Calendar aging is worse at high SOC and also undesirable near extremes."""
    if soc_fraction >= 0.90:
        return 1.45
    if soc_fraction >= 0.75:
        return 1.20
    if soc_fraction <= 0.10:
        return 1.15
    if 0.35 <= soc_fraction <= 0.65:
        return 0.85
    return 1.0


def c_rate_factor(power_mw: float, capacity_mwh: float) -> float:
    c_rate = power_mw / max(0.001, capacity_mwh)
    if c_rate <= 0.35:
        return 0.9
    if c_rate <= 0.7:
        return 1.0
    return min(1.6, 1.0 + (c_rate - 0.7) * 0.8)


def cycle_degradation_cost_eur_per_mwh(
    *,
    battery: BatteryConfig,
    cell_temp_c: float,
    power_mw: float,
) -> float:
    """Marginal degradation cost per MWh throughput.

    Assumption: cycle_life_to_80pct means 20% capacity loss after that many
    equivalent full cycles at reference conditions.
    """
    replacement_total = battery.replacement_cost_eur_per_mwh * battery.capacity_mwh
    capacity_loss_cost_per_pct = replacement_total / 100.0
    pct_loss_per_efc = 20.0 / max(1.0, battery.cycle_life_to_80pct)
    pct_loss_per_mwh = pct_loss_per_efc / max(0.001, battery.capacity_mwh)
    factor = temperature_factor(cell_temp_c, battery.degradation_reference_temp_c) * c_rate_factor(power_mw, battery.capacity_mwh)
    return capacity_loss_cost_per_pct * pct_loss_per_mwh * factor


def interval_calendar_fade_pct(
    *,
    battery: BatteryConfig,
    cell_temp_c: float,
    soc_fraction: float,
    interval_hours: float = 0.25,
) -> float:
    annual_pct = battery.calendar_fade_pct_per_year
    base = annual_pct * interval_hours / (365.0 * 24.0)
    return base * temperature_factor(cell_temp_c, battery.degradation_reference_temp_c) * soc_stress_factor(soc_fraction)


def estimate_cell_temp_c(ambient_temp_c: float, battery: BatteryConfig, power_mw: float = 0.0) -> float:
    """Container cell temperature proxy for Greece climate.

    Uses ambient temperature, thermal-management offset, and a small power heat
    term. This is a proxy until BMS temperature telemetry exists.
    """
    c_rate = power_mw / max(0.001, battery.capacity_mwh)
    return ambient_temp_c + battery.thermal_management_delta_c + 4.0 * c_rate
