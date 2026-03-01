import numpy as np
import pandas as pd


from sklearn.model_selection import StratifiedKFold
from sklearn.preprocessing import OneHotEncoder
from causalml.inference.tree import UpliftRandomForestClassifier 

import plotly.express as px
import plotly.graph_objects as go

from typing import Any, Dict, Iterable, Optional, Tuple, Union


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
    outcome_col="cameback",
    size=10,
    binary_uplift=True,
):
    n = len(df)
    bins = range(1, size + 1)

    results = []

    for b in bins:
        start = int(np.ceil(n * (b - 1) / size))
        end = int(np.ceil(n * b / size))
        subset = df.iloc[start:end]

        # Apply for the binary uplift models treatment vs control fo each incentive 
        if binary_uplift:
            # --- MTUM logic (your provided block) ---
            t = subset[treatment_col].astype(str).str.strip()
            t_lower = t.str.lower()

            # Control rows look like "control_1", "control_2", ...
            is_control = t_lower.str.startswith("control")

            # Everything else is treated, but only if it has a trailing digit
            last_digit = t_lower.str.extract(r"(\d)\s*$", expand=False)
            is_treated = (~is_control) & last_digit.notna()

        # Apply for the mtum the comparison of all control 
        else:
            t = subset[treatment_col]
            is_control = t.eq(0)
            is_treated = t.ne(0) & t.notna()

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
                "uplift": treated_rate - control_rate,
            }
        )

    df_out = pd.DataFrame(results).sort_values("bin").reset_index(drop=True)

    df_out["cum_treated_n"] = df_out["treated_n"].cumsum()
    df_out["cum_control_n"] = df_out["control_n"].cumsum()
    df_out["cum_treated_converted_n"] = df_out["treated_converted_n"].cumsum()
    df_out["cum_control_converted_n"] = df_out["control_converted_n"].cumsum()

    df_out["cum_treated_rate"] = (
        df_out["cum_treated_converted_n"] / df_out["cum_treated_n"].replace(0, np.nan)
    ).fillna(0.0)

    df_out["cum_control_rate"] = (
        df_out["cum_control_converted_n"] / df_out["cum_control_n"].replace(0, np.nan)
    ).fillna(0.0)

    df_out["cum_population_frac"] = df_out["bin_n"].cumsum() / df_out["bin_n"].sum()

    df_out["inc_gains"] = (
        (df_out["cum_treated_rate"] - df_out["cum_control_rate"])
        * df_out["cum_population_frac"]
    )

    return df_out


    
def plot_incremental_response_rate(uplift_curve_df):
    df = uplift_curve_df.copy()

    df["pct_targeted"] = df["bin"] / df["bin"].max()
    final_inc_gain = df["inc_gains"].iloc[-1]

    fig = px.line(
        df,
        x="pct_targeted",
        y="inc_gains",
        markers=True,
        labels={
            "pct_targeted": "% Targeted",
            "inc_gains": "Incremental Response Rate",
        },
        title="Incremental Response Rate",
    )

    fig.add_trace(
        go.Scatter(
            x=[0, 1],
            y=[0, final_inc_gain],
            mode="lines",
            name="Random Targeting",
            line=dict(dash="dash"),
        )
    )

    fig.update_layout(
        template="plotly_white",
        title_x=0.5,
        legend_title_text="",
        xaxis=dict(tickformat=".0%"),
        yaxis=dict(tickformat=".2%"),
    )

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
    Compute P(T=t) using only the last digit of y_true (e.g., 'cameback_3' -> 3).
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
    resp_prefix: str = "cameback",
    nonresp_prefix: str = "no_cameback",
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