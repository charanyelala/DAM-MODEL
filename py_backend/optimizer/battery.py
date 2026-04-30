"""
Battery LP optimizer for Greek DAM day-ahead scheduling.

Formulation
-----------
Variables (N = number of 15-min MTU slots, Δt = 0.25 h):
  p_c(t)  charge power [MW],  t = 0..N-1
  p_d(t)  discharge power [MW]
  e(t)    state of charge at end of slot t [MWh]

Objective — maximize net revenue over the day:
  max  Σ_t  Δt · [ (λ(t) − c_deg)·p_d(t)  −  (λ(t) + c_deg)·p_c(t) ]
  ↔ minimize the negated objective fed to linprog.

Constraints:
  (1) SoC dynamics    e(t) = e(t−1) + η_c·p_c(t)·Δt − p_d(t)·Δt/η_d  ∀t
      with e(−1) ≡ E_init
  (2) SoC bounds      E_min ≤ e(t) ≤ E_max                             ∀t
  (3) Charge limit    0 ≤ p_c(t) ≤ P_c^max                             ∀t
  (4) Discharge limit 0 ≤ p_d(t) ≤ P_d^max                             ∀t
  (5) End SoC target  e(N−1) ≥ E_init       (battery left no worse off)
  (6) Cycle budget    Σ_t p_c(t)·Δt ≤ n_cyc·E_cap  (if max_daily_cycles > 0)

No binary variables are needed: since η_c·η_d < 1, simultaneous
charge+discharge is always suboptimal and the LP relaxation is tight.

Solved with scipy.optimize.linprog (HiGHS backend, millisecond solve time).
Falls back to the greedy heuristic if scipy is not installed or LP fails.
"""
from __future__ import annotations

import math
from py_backend.config import BatteryConfig
from py_backend.models.degradation import interval_calendar_fade_pct


def optimize_battery(forecasts: list[dict], battery: BatteryConfig) -> list[dict]:
    try:
        from scipy.optimize import linprog
        result = _lp_optimize(forecasts, battery, linprog)
        if result is not None:
            return result
    except ImportError:
        pass
    return _greedy_optimize(forecasts, battery)


# ── LP optimizer ──────────────────────────────────────────────────────────────

def _lp_optimize(
    forecasts: list[dict],
    battery: BatteryConfig,
    linprog,
) -> list[dict] | None:
    N = len(forecasts)
    if N == 0:
        return []

    dt = 0.25  # hours per slot (15-min MTU)
    E_cap = battery.capacity_mwh
    E_min = E_cap * battery.min_soc_fraction
    E_max = E_cap * battery.max_soc_fraction
    E_init = E_cap * battery.initial_soc_fraction
    eta_c = battery.charge_efficiency
    eta_d = battery.discharge_efficiency
    c_deg = battery.degradation_eur_per_mwh
    P_c_max = battery.max_charge_mw
    P_d_max = battery.max_discharge_mw

    prices = [f["priceP50"] for f in forecasts]
    discharge_values = [f.get("adjustedDischargeValue", f["priceP50"]) for f in forecasts]
    charge_costs = [f.get("adjustedChargeCost", f["priceP50"]) for f in forecasts]
    hold_values = [f.get("adjustedHoldValue", 0.0) for f in forecasts]
    degradation_costs = [max(c_deg, f.get("degradationCostEurPerMwh", c_deg)) for f in forecasts]

    # ── Objective (linprog minimises) ────────────────────────────────────────
    # Variables: x = [p_c(0..N-1), p_d(0..N-1), e(0..N-1)]
    c_obj = (
        [dt * (charge_costs[t] + degradation_costs[t]) for t in range(N)]       # adjusted charge cost
        + [-dt * (discharge_values[t] - degradation_costs[t] - hold_values[t]) for t in range(N)] # adjusted discharge value
        + [0.0] * N                                        # SoC has no direct cost
    )

    # ── Equality: SoC dynamics ────────────────────────────────────────────────
    # Row t:  -η_c·dt·p_c(t)  +  dt/η_d·p_d(t)  +  e(t)  −  e(t−1)  =  rhs
    # rhs = E_init when t=0 (e(−1) = E_init moved to RHS), else 0
    A_eq = []
    b_eq = []
    for t in range(N):
        row = [0.0] * (3 * N)
        row[t] = -eta_c * dt           # p_c(t)
        row[N + t] = dt / eta_d        # p_d(t)
        row[2 * N + t] = 1.0           # e(t)
        if t > 0:
            row[2 * N + t - 1] = -1.0  # −e(t−1)
            b_eq.append(0.0)
        else:
            b_eq.append(E_init)        # e(−1) = E_init
        A_eq.append(row)

    # ── Inequality constraints ────────────────────────────────────────────────
    A_ub: list[list[float]] = []
    b_ub: list[float] = []

    # End-of-day SoC ≥ E_init  →  −e(N−1) ≤ −E_init
    end_row = [0.0] * (3 * N)
    end_row[3 * N - 1] = -1.0
    A_ub.append(end_row)
    b_ub.append(-E_init)

    # Cycle budget: total energy charged ≤ max_daily_cycles · E_cap
    if battery.max_daily_cycles > 0:
        cyc_row = [0.0] * (3 * N)
        for t in range(N):
            cyc_row[t] = dt
        A_ub.append(cyc_row)
        b_ub.append(battery.max_daily_cycles * E_cap)

    # ── Variable bounds ───────────────────────────────────────────────────────
    bounds = (
        [
            (
                0.0,
                P_c_max
                * forecasts[t].get("availabilityFactor", 1.0)
                * forecasts[t].get("thermalDeratingFactor", 1.0),
            )
            for t in range(N)
        ]
        + [
            (
                0.0,
                P_d_max
                * forecasts[t].get("availabilityFactor", 1.0)
                * forecasts[t].get("thermalDeratingFactor", 1.0),
            )
            for t in range(N)
        ]
        + [(E_min, E_max)] * N
    )

    res = linprog(
        c_obj,
        A_ub=A_ub,
        b_ub=b_ub,
        A_eq=A_eq,
        b_eq=b_eq,
        bounds=bounds,
        method="highs",
    )

    if res.status != 0:
        return None  # infeasible / numerical issue — let caller fall back

    x = res.x
    p_c = x[:N]
    p_d = x[N: 2 * N]
    e = x[2 * N:]

    rows = []
    for t, forecast in enumerate(forecasts):
        charge = float(p_c[t])
        discharge = float(p_d[t])
        soc = float(e[t])
        price = prices[t]
        discharge_value = discharge_values[t]
        charge_cost = charge_costs[t]
        hold_value = hold_values[t]
        deg_cost = degradation_costs[t]

        # Threshold below which we treat a value as effectively zero (numerical noise)
        _EPS = 1e-3
        if charge > _EPS and discharge < _EPS:
            action = "charge"
        elif discharge > _EPS and charge < _EPS:
            action = "discharge"
        else:
            action = "idle"
            charge = discharge = 0.0

        revenue = dt * (
            discharge * (discharge_value - hold_value)
            - charge * charge_cost
            - (charge + discharge) * deg_cost
        )
        throughput_mwh = (charge + discharge) * dt
        cycle_fade_pct = _cycle_fade_pct(throughput_mwh, deg_cost, battery)
        calendar_fade_pct = interval_calendar_fade_pct(
            battery=battery,
            cell_temp_c=forecast.get("cellTempC", 25.0),
            soc_fraction=soc / max(0.001, battery.capacity_mwh),
            interval_hours=dt,
        )
        rows.append({
            "timestamp": forecast["timestamp"],
            "mtu": forecast["mtu"],
            "action": action,
            "chargeMw": round(charge, 3),
            "dischargeMw": round(discharge, 3),
            "socMwh": round(soc, 3),
            "priceForecast": round(price, 2),
            "adjustedDischargeValue": round(discharge_value, 2),
            "adjustedChargeCost": round(charge_cost, 2),
            "adjustedHoldValue": round(hold_value, 2),
            "degradationCostEurPerMwh": round(deg_cost, 4),
            "degradationCostEur": round(throughput_mwh * deg_cost, 2),
            "capacityFadePct": round(cycle_fade_pct + calendar_fade_pct, 8),
            "expectedRevenueEur": round(revenue, 2),
            "explanation": _explain_lp(action, forecast, charge, discharge, price, deg_cost, hold_value),
        })
    return rows


def _explain_lp(
    action: str, forecast: dict, charge: float, discharge: float, price: float, c_deg: float, hold_value: float
) -> str:
    if action == "charge":
        return (
            f"LP: charge {charge:.2f} MW — price €{price:.1f}/MWh is in the "
            f"low-price window identified by the day-ahead schedule."
        )
    if action == "discharge":
        return (
            f"LP: discharge {discharge:.2f} MW — price €{price:.1f}/MWh exceeds the "
            f"opportunity cost threshold set by the full-day LP, after reserving {hold_value:.1f} EUR/MWh as SOC option value."
        )
    return (
        "LP: idle — marginal revenue from cycling does not cover degradation "
        f"(€{c_deg}/MWh) at this price."
    )


# ── Greedy fallback ──────────────────────────────────────────────────────────

def _greedy_optimize(forecasts: list[dict], battery: BatteryConfig) -> list[dict]:
    """
    Simple threshold heuristic used only when scipy is unavailable or the LP
    is numerically infeasible.  Charges below the 28th price percentile and
    discharges above the 72nd.
    """
    E_min = battery.capacity_mwh * battery.min_soc_fraction
    E_max = battery.capacity_mwh * battery.max_soc_fraction
    soc = battery.capacity_mwh * battery.initial_soc_fraction
    eta = math.sqrt(battery.charge_efficiency * battery.discharge_efficiency)
    c_deg = battery.degradation_eur_per_mwh
    dt = 0.25

    sorted_charge_costs = sorted(row.get("adjustedChargeCost", row["priceP50"]) for row in forecasts)
    sorted_discharge_values = sorted(
        row.get("adjustedDischargeValue", row["priceP50"]) - row.get("adjustedHoldValue", 0.0)
        for row in forecasts
    )
    low_cut = sorted_charge_costs[int(len(sorted_charge_costs) * 0.28)]
    high_cut = sorted_discharge_values[int(len(sorted_discharge_values) * 0.72)]

    rows = []
    for forecast in forecasts:
        price = forecast["priceP50"]
        charge_cost = forecast.get("adjustedChargeCost", price)
        discharge_value = forecast.get("adjustedDischargeValue", price)
        hold_value = forecast.get("adjustedHoldValue", 0.0)
        deg_cost = max(c_deg, forecast.get("degradationCostEurPerMwh", c_deg))
        charge = discharge = 0.0
        action = "idle"
        can_charge = soc < E_max - 1e-3
        can_discharge = soc > E_min + 1e-3

        if can_discharge and (
            discharge_value - hold_value >= high_cut
            or forecast["probabilitySpike"] > 0.55
            or forecast["regime"] == "spike-risk"
        ):
            discharge = min(
                battery.max_discharge_mw
                * forecast.get("availabilityFactor", 1.0)
                * forecast.get("thermalDeratingFactor", 1.0),
                (soc - E_min) / dt,
            )
            soc -= discharge * dt
            action = "discharge"
        elif can_charge and (
            charge_cost <= low_cut
            or forecast["probabilityNegative"] > 0.35
            or forecast["regime"] == "zero-negative-risk"
        ):
            charge = min(
                battery.max_charge_mw
                * forecast.get("availabilityFactor", 1.0)
                * forecast.get("thermalDeratingFactor", 1.0),
                (E_max - soc) / (dt * eta),
            )
            soc += charge * dt * eta
            action = "charge"

        revenue = dt * (
            discharge * (discharge_value - hold_value) - charge * charge_cost - (charge + discharge) * deg_cost
        )
        throughput_mwh = (charge + discharge) * dt
        cycle_fade_pct = _cycle_fade_pct(throughput_mwh, deg_cost, battery)
        calendar_fade_pct = interval_calendar_fade_pct(
            battery=battery,
            cell_temp_c=forecast.get("cellTempC", 25.0),
            soc_fraction=soc / max(0.001, battery.capacity_mwh),
            interval_hours=dt,
        )
        rows.append({
            "timestamp": forecast["timestamp"],
            "mtu": forecast["mtu"],
            "action": action,
            "chargeMw": round(charge, 3),
            "dischargeMw": round(discharge, 3),
            "socMwh": round(soc, 3),
            "priceForecast": price,
            "adjustedDischargeValue": round(discharge_value, 2),
            "adjustedChargeCost": round(charge_cost, 2),
            "adjustedHoldValue": round(hold_value, 2),
            "degradationCostEurPerMwh": round(deg_cost, 4),
            "degradationCostEur": round(throughput_mwh * deg_cost, 2),
            "capacityFadePct": round(cycle_fade_pct + calendar_fade_pct, 8),
            "expectedRevenueEur": round(revenue, 2),
            "explanation": _explain_greedy(action, forecast),
        })
    return rows


def _cycle_fade_pct(throughput_mwh: float, deg_cost_eur_per_mwh: float, battery: BatteryConfig) -> float:
    replacement_total = battery.replacement_cost_eur_per_mwh * battery.capacity_mwh
    if replacement_total <= 0:
        return 0.0
    return (throughput_mwh * deg_cost_eur_per_mwh) / replacement_total * 100.0


def _explain_greedy(action: str, forecast: dict) -> str:
    if action == "charge":
        return (
            "Greedy fallback: charge — zero/negative price probability elevated."
            if forecast["probabilityNegative"] > 0.35
            else "Greedy fallback: charge — slot is in the low-price percentile group."
        )
    if action == "discharge":
        return (
            "Greedy fallback: discharge — spike probability elevated."
            if forecast["probabilitySpike"] > 0.55
            else "Greedy fallback: discharge — slot is in the high-price percentile group."
        )
    return "Greedy fallback: idle — spread does not justify cycling after losses."
