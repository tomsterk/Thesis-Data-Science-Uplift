import numpy as np
import pandas as pd

from pathlib import Path

from sklearn.model_selection import StratifiedKFold
from sklearn.preprocessing import OneHotEncoder
from causalml.inference.tree import UpliftRandomForestClassifier

import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter

from typing import Any, Dict, Iterable, Optional, Tuple, Union

################################################################################################
# IEEE Access figure settings
# - 3.5 in (88 mm) single-column width
# - Arial, 8 pt labels / 7 pt ticks
# - Vector PDF, TrueType fonts embedded (fonttype 42)
# - Grayscale-safe: greys + distinct linestyles/markers, no color coding
################################################################################################

OUTPUT_DIR = Path("Output/ieee_figures")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

plt.style.use("default")
plt.rcParams.update({
    "font.family":     "Arial",
    "font.size":       8,
    "axes.labelsize":  8,
    "axes.linewidth":  0.6,
    "xtick.labelsize": 7,
    "ytick.labelsize": 7,
    "legend.fontsize": 7,
    "pdf.fonttype":    42,
    "ps.fonttype":     42,
})
FIGSIZE = (3.5, 2.4)

# Grayscale-safe styling per model position: (grey level, linestyle, marker)
MODEL_STYLES = [
    ("0.0",  "-",  "o"),
    ("0.35", "--", "s"),
    ("0.55", "-.", "^"),
    ("0.2",  "-",  "D"),
]


def coerce_metrics_to_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()

    df[cols] = (
        df[cols]
        .replace({",": ""}, regex=True)
        .apply(pd.to_numeric, errors="coerce")
    )
    return df

def uplift_by_decile_bin(
    df,
    treatment_col="treatment",
    outcome_col="reactivated",
    size=10,
    binary_uplift=True,
):
    """
    Compute per-decile and cumulative uplift metrics from a scored DataFrame.

    Expects `df` to be pre-sorted by predicted uplift (descending), so that
    bin 1 contains the highest-scoring customers and bin `size` the lowest.

    """
    n = len(df)
    bins = range(1, size + 1)
    results = []

    for b in bins:
        # --- Slice the bin using ceiling-based index boundaries ---
        start = int(np.ceil(n * (b - 1) / size))
        end = int(np.ceil(n * b / size))
        subset = df.iloc[start:end]

        # --- Identify treated vs. control rows within this bin ---
        if binary_uplift:
            # Binary uplift format: treatment_col contains string labels.
            t = subset[treatment_col].astype(str).str.strip()
            t_lower = t.str.lower()
            is_control = t_lower.str.startswith("control")
            last_digit = t_lower.str.extract(r"(\d)\s*$", expand=False)
            is_treated = (~is_control) & last_digit.notna()
        else:
            # MTUM format: treatment_col is integer-coded.
            # 0 = control; any non-zero, non-null value = treated.
            t = subset[treatment_col]
            is_control = t.eq(0)
            is_treated = t.ne(0) & t.notna()

        # --- Per-bin counts and conversion rates ---
        treated_n = int(is_treated.sum())
        control_n = int(is_control.sum())
        treated_converted_n = int(subset.loc[is_treated, outcome_col].sum())
        control_converted_n = int(subset.loc[is_control, outcome_col].sum())
        treated_rate = float(subset.loc[is_treated, outcome_col].mean()) if treated_n > 0 else 0.0
        control_rate = float(subset.loc[is_control, outcome_col].mean()) if control_n > 0 else 0.0

        results.append(
            {
                "bin": b,
                "bin_start_idx": start,
                "bin_end_idx": end,
                "bin_n": len(subset),
                "treated_n": treated_n,
                "control_n": control_n,
                "treated_converted_n": treated_converted_n,
                "control_converted_n": control_converted_n,
                "treated_rate": treated_rate,
                "control_rate": control_rate,
                "uplift": treated_rate - control_rate,  # per-bin raw uplift
            }
        )

    df_out = pd.DataFrame(results).sort_values("bin").reset_index(drop=True)

    # --- Cumulative statistics (top-k targeting perspective) ---
    # Running totals of treated/control group sizes and conversions
    df_out["cum_treated_n"] = df_out["treated_n"].cumsum()
    df_out["cum_control_n"] = df_out["control_n"].cumsum()
    df_out["cum_treated_converted_n"] = df_out["treated_converted_n"].cumsum()
    df_out["cum_control_converted_n"] = df_out["control_converted_n"].cumsum()

    # Cumulative conversion rates 
    df_out["cum_treated_rate"] = (
        df_out["cum_treated_converted_n"] / df_out["cum_treated_n"].replace(0, np.nan)
    ).fillna(0.0)
    df_out["cum_control_rate"] = (
        df_out["cum_control_converted_n"] / df_out["cum_control_n"].replace(0, np.nan)
    ).fillna(0.0)

    # Fraction of total population targeted so far (x-axis for Qini-style curves)
    df_out["cum_population_frac"] = df_out["bin_n"].cumsum() / df_out["bin_n"].sum()

    # --- Incremental gains curve ---
    # inc_gains = cumulative uplift × fraction targeted, i.e. the area-based
    # measure of extra conversions gained by targeting the top-k% vs. not treating.
    df_out["inc_gains"] = (
        (df_out["cum_treated_rate"] - df_out["cum_control_rate"])
        * df_out["cum_population_frac"]
    )

    # Random targeting baseline: a straight line from origin to the final
    df_out["random_expected"] = df_out["cum_population_frac"] * df_out["inc_gains"].iloc[-1]

    # Lift over random
    df_out["lift_over_random"] = df_out["inc_gains"] - df_out["random_expected"]

    return df_out

def calc_auuc(df):
    """Area between uplift curve and random baseline (trapezoid rule)."""
    x = np.concatenate([[0], df["cum_population_frac"].values])
    y = np.concatenate([[0], df["lift_over_random"].values])
    return np.trapezoid(y, x)

def _style_qini_axes(ax):
    """Apply the shared IEEE axis styling to a Qini plot."""
    ax.set_facecolor("white")
    ax.set_xlabel("% Targeted")
    ax.set_ylabel("Cumulative Incremental Gain")
    ax.xaxis.set_major_formatter(PercentFormatter(xmax=1, decimals=0))
    ax.yaxis.set_major_formatter(PercentFormatter(xmax=1, decimals=2))
    ax.grid(True, axis="y", color="0.85", linewidth=0.5, zorder=0)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_linewidth(0.6)
    ax.spines["bottom"].set_linewidth(0.6)
    ax.tick_params(width=0.6, length=3, labelsize=7)
    ax.margins(x=0.02)
    ax.legend(frameon=False, loc="upper left", handlelength=1.8, borderpad=0.4)
    
def plot_incremental_response_rate(uplift_curve_df):
    """Single-model Qini curve with random baseline and AUUC annotation."""
    df = uplift_curve_df.copy()
    df["pct_targeted"] = df["bin"] / df["bin"].max()
    final_inc_gain = df["inc_gains"].iloc[-1]
    auuc = calc_auuc(df)

    fig, ax = plt.subplots(figsize=FIGSIZE)

    ax.plot(
        df["pct_targeted"], df["inc_gains"],
        color="0.0", linestyle="-", marker="o", markersize=3.5,
        markeredgewidth=0, linewidth=1.1, label="Model Uplift", zorder=3,
    )
    ax.plot(
        [0, 1], [0, final_inc_gain],
        color="0.55", linestyle=(0, (4, 3)), linewidth=0.9,
        label="Random Targeting", zorder=2,
    )

    _style_qini_axes(ax)

    leg = ax.get_legend()
    fig.canvas.draw()
    leg_box = leg.get_window_extent().transformed(ax.transAxes.inverted())
    _style_qini_axes(ax)

    leg = ax.get_legend()
    fig.canvas.draw()
    leg_box = leg.get_window_extent().transformed(ax.transAxes.inverted())
    ax.text(
        leg_box.x0 + 0.02, leg_box.y0 - 0.02, f"AUUC = {auuc:.5f}",
        transform=ax.transAxes, ha="left", va="top", fontsize=7,
        bbox=dict(facecolor="white", edgecolor="0.3", linewidth=0.5, pad=2.5),
    )

    fig.tight_layout()
    return fig
    
# Color + grayscale-safe styling per model position: (color, linestyle, marker)
MODEL_STYLES = [
    ("#000000", "-",  "o"),  # black
    ("#0072B2", "--", "s"),  # blue
    ("#D55E00", "-.", "^"),  # vermillion
    ("#009E73", "-",  "D"),  # green
]
def plot_combined_incremental_response_rate(qini_bins_by_model):
    """Multi-model Qini curves with a single averaged random baseline."""
    fig, ax = plt.subplots(figsize=FIGSIZE)

    # average final_inc_gain across all models for the random diagonal
    avg_final = qini_bins_by_model.groupby("model_paper").apply(
        lambda g: g["inc_gains"].iloc[-1]
    ).mean()

    for i, (model, g) in enumerate(qini_bins_by_model.groupby("model_paper")):
        df = g.copy()
        df["pct_targeted"] = df["bin"] / df["bin"].max()
        color, linestyle, marker = MODEL_STYLES[i % len(MODEL_STYLES)]
        ax.plot(
            df["pct_targeted"], df["inc_gains"],
            color=color, linestyle=linestyle, marker=marker,
            markersize=3.5, markeredgewidth=0, linewidth=1.1, label=model, zorder=3,
        )

    # single random targeting diagonal (averaged across models)
    ax.plot(
        [0, 1], [0, avg_final],
        color="0.7", linestyle=":", linewidth=0.9,
        label="Random Targeting", zorder=2,
    )

    _style_qini_axes(ax)
    ax.legend(
        frameon=False, loc="upper left", bbox_to_anchor=(1.02, 1.0),
        handlelength=1.8, borderpad=0.2, labelspacing=0.3,
    )
    fig.tight_layout()
    return fig



################################################################################################
# Function to return the prior probabilities of treatments per treatment group, i.e. 0 = control
# Used for counteracting the imbalance of treatment groups
################################################################################################


def get_treatment_probs_from_y_true(
    df: pd.DataFrame,
    *,
    y_true_col: str = "y_true",
) -> Dict[int, float]:
    """
    Compute P(T=t) using only the last digit of y_true (e.g., 'reactivated_3' -> 3).
    Returns a dict like {0: 0.52, 1: 0.11, ...}.
    """
    probs = (
        df[y_true_col]
        .astype(str)
        .str[-1]
        .astype(int)
        .value_counts(normalize=True)
        .sort_index()
        .to_dict()
    )

    return probs



#####################################################################################
# Function to calculate the uplift per treatment using the modified outcome approach 
#####################################################################################
def uplift_mmoa(
    df: pd.DataFrame,
    *,
    k: int,
    resp_prefix: str = "reactivated",
    nonresp_prefix: str = "no_reactivated",
    treatment_probs: Dict[int, float],
    return_parts: bool = True,
) -> Union[pd.Series, Tuple[pd.Series, pd.DataFrame]]:
    r0 = f"p_{resp_prefix}_0"
    rk = f"p_{resp_prefix}_{k}"
    nr0 = f"p_{nonresp_prefix}_0"
    nrk = f"p_{nonresp_prefix}_{k}"

    pt_control = float(treatment_probs.get(0, 0.0))
    pt_treat = float(treatment_probs.get(k, 0.0))

    part_rt_k = df[rk] / pt_treat
    part_nrt_0 = df[nr0] / pt_control
    part_nrt_k = df[nrk] / pt_treat
    part_rt_0 = df[r0] / pt_control

    tau_hat = (part_rt_k + part_nrt_0) - (part_nrt_k + part_rt_0)

    if not return_parts:
        return tau_hat

    parts = pd.DataFrame(
        {
            f"part_rt_{k}": part_rt_k,
            "part_nrt_0": part_nrt_0,
            f"part_nrt_{k}": part_nrt_k,
            "part_rt_0": part_rt_0,
            "pt_control": pt_control,
            f"pt_treat_{k}": pt_treat,
        },
        index=df.index,
    )

    return tau_hat, parts



####################################################################################
# Loop over treatments and calculate the uplift per treatment
####################################################################################
def add_uplifts(
    df: pd.DataFrame,
    k_values: Iterable[int],
    resp_prefix: str,
    nonresp_prefix: str,
    *,
    treatment_probs: Dict[int, float],
    y_true_col: str = "y_true",
) -> pd.DataFrame:
    df = df.copy()

    for k in k_values:
        required_cols = [
            f"p_{resp_prefix}_0",
            f"p_{resp_prefix}_{k}",
            f"p_{nonresp_prefix}_0",
            f"p_{nonresp_prefix}_{k}",
        ]
        if not all(c in df.columns for c in required_cols):
            continue

        tau_k, _ = uplift_mmoa(
            df,
            k=k,
            resp_prefix=resp_prefix,
            nonresp_prefix=nonresp_prefix,
            treatment_probs=treatment_probs,
            return_parts=True,
        )

        df[f"uplift_{k}"] = tau_k

    return df