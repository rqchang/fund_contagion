import pandas as pd
import numpy as np
import os
import sys
import pyreadr
from linearmodels.iv.absorbing import AbsorbingLS
import matplotlib.pyplot as plt

try:
    _root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
except NameError:
    _root = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, _root)
from utils.set_path import PROC_DIR, OUT_DIR


""" Read data """
data = pd.read_csv(os.path.join(PROC_DIR, "crsp/mf_holdings_bond_cat.csv"))
print("Read holdings data:", data.shape)

bm = pyreadr.read_r(os.path.join(PROC_DIR, "merges/bond_returns_master.rds"))[None]
bm["cusip8"]    = bm["cusip"].str[:8]
bm["date_m_id"] = bm["mdate"].astype(int)

f_cls = pd.read_csv(os.path.join(PROC_DIR, "crsp/mf_invclass.csv"), usecols=["crsp_portno", "fund_cat"])
print("Fund category distribution:\n", f_cls["fund_cat"].value_counts())


""" Clean data """
" Lag bond characteristics "
BM_CHARS = ["ret_eom", "bid_ask", "cs_yield", "rating_rank", "tmt", "amt_outstanding"]
bm = bm.dropna(subset=["cusip8"]).sort_values(["cusip8", "date_m_id"]).reset_index(drop=True)
bm["date_lag"] = bm.groupby("cusip8")["date_m_id"].shift(1)

def prev_month_id(d):
    y, m = divmod(int(d), 100)
    return (y - 1) * 100 + 12 if m == 1 else y * 100 + (m - 1)

bm["prev_date"] = bm["date_m_id"].map(prev_month_id)
for col in BM_CHARS:
    raw_lag = bm.groupby("cusip8")[col].shift(1)
    bm[f"{col}_lag"] = np.where(bm["date_lag"] == bm["prev_date"], raw_lag, np.nan)

bm_merge = bm[["cusip8", "date_m_id"] + [f"{c}_lag" for c in BM_CHARS]].drop_duplicates(["cusip8", "date_m_id"])
del bm

data = data.sort_values(["crsp_portno", "cusip8", "date_m_id"]).reset_index(drop=True)
for col, grp in [("w", ["crsp_portno", "cusip8"]), ("aum", ["crsp_portno", "cusip8"])]:
    raw_lag  = data.groupby(grp)[col].shift(1)
    date_lag = data.groupby(grp)["date_m_id"].shift(1)
    data[f"{col}_lag"] = np.where(date_lag == data["prev_date_m_id"], raw_lag, np.nan)

data = pd.merge(data, bm_merge, on=["cusip8", "date_m_id"], how="left")
del bm_merge


" Add fund categories "
panel = pd.merge(data, f_cls[["crsp_portno", "fund_cat"]], on="crsp_portno", how="left")
print("After adding fund cats:", panel.shape)
del data, f_cls


""" Construct IV and endogenous variables """
" Construct AggFlowShock (value-weighted by lagged AUM) "
# AggFlowShock_{c,t} = sum_{f in c} AUM_{f,t-1} * Flow_{f,t} / sum_{f in c} AUM_{f,t-1}
# Value-weighting by lagged AUM ensures the measure reflects the aggregate investor-driven
# demand shock for the category, with larger funds receiving proportionally more weight.
# Using lagged AUM makes the weights predetermined w.r.t. contemporaneous outcomes.
fund_flow = (panel.drop_duplicates(["crsp_portno", "date_m_id"])
                  [["crsp_portno", "date_m_id", "flow", "aum_lag", "fund_cat"]]
                  .dropna(subset=["flow", "fund_cat"])
                  .copy().reset_index(drop=True))

fund_flow["flow_x_aum"] = fund_flow["flow"] * fund_flow["aum_lag"].fillna(0)

cat_agg = (fund_flow.groupby(["fund_cat", "date_m_id"])
                    .agg(cat_flow_waum=("flow_x_aum", "sum"),
                         cat_aum_sum=("aum_lag",      "sum"))
                    .reset_index())
cat_agg["agg_flow_shock"] = np.where(
    cat_agg["cat_aum_sum"] > 0,
    cat_agg["cat_flow_waum"] / cat_agg["cat_aum_sum"],
    np.nan
)
fund_flow = pd.merge(fund_flow, cat_agg[["fund_cat", "date_m_id", "agg_flow_shock"]],
                     on=["fund_cat", "date_m_id"], how="left")
del cat_agg
print(f"AggFlowShock valid: {fund_flow['agg_flow_shock'].notna().sum():,} fund-months")
print(fund_flow["agg_flow_shock"].describe())

panel = pd.merge(panel, fund_flow[["crsp_portno", "date_m_id", "agg_flow_shock"]],
                 on=["crsp_portno", "date_m_id"], how="left")
del fund_flow


" Construct Z_{-f,i,t} (same-category, LOO Bartik instrument) "
# Z_{-f,i,t} = sum_{g != f, g in F_t, c(g)=c(f)} w_{g,i,t-1} * AggFlowShock_{c(g),t}
#
# Selection: g in F_t = actual outflow funds (outflow_fund flag, flow_g < 0).
# Same-category: c(g) = c(f), so only peer outflow funds contribute.
# Share: w_{g,i,t-1} = lagged portfolio weight of bond i in outflow fund g.
# Shift: AggFlowShock_{c(g),t} = category-level value-weighted flow shock.
#
# LOO (g != f): since f is an inflow fund, f ∉ F_t automatically, so the LOO
# constraint is satisfied without any adjustment.
panel["z_contrib"] = np.where(
    panel["outflow_fund"],                           # g in F_t: actual outflow funds only
    panel["w_lag"] * panel["agg_flow_shock"],        # share × shift
    0
)

# Sum z_contrib within (bond, month, fund_cat) to get Z for inflow funds in that category
z_cat = (panel[panel["z_contrib"] != 0]
         .groupby(["cusip8", "date_m_id", "fund_cat"])["z_contrib"]
         .sum().rename("z_iv_cat").reset_index())
panel = panel.merge(z_cat, on=["cusip8", "date_m_id", "fund_cat"], how="left")
del z_cat
panel["z_iv_cat"] = panel["z_iv_cat"].fillna(0)
print(f"z_iv_cat: non-zero = {(panel['z_iv_cat'] != 0).sum():,} / {len(panel):,}")


" Construct OutflowIntensity_{-f,i,t} (LOO, continuous endogenous variable) "
# OutflowIntensity_{-f,i,t} = -sum_{g in F_t, g != f} Δpar_{g,i,t} * 1{Δpar < 0}
#                              / AmtOutstanding_{i,t-1}
#
# LOO (g != f): f is an inflow fund so f ∉ F_t; the LOO is automatically satisfied.
# Only actual sales (Δpar < 0) by outflow funds are counted.
# Normalized by lagged bond amount outstanding.
outflow_sales = (
    panel[panel["outflow_fund"] & (panel["delta_paramt"] < 0)]
    .groupby(["cusip8", "date_m_id"])["delta_paramt"]
    .sum().abs().rename("outflow_paramt_sold").reset_index()
)
panel = pd.merge(panel, outflow_sales, on=["cusip8", "date_m_id"], how="left")
del outflow_sales
panel["outflow_intensity"] = (
    panel["outflow_paramt_sold"].fillna(0)
    / panel["amt_outstanding_lag"].replace(0, np.nan)
)
print(f"OutflowIntensity: {panel['outflow_intensity'].notna().sum():,} non-null, "
      f"mean = {panel['outflow_intensity'].mean():.4f}")

fig, axes = plt.subplots(1, 2, figsize=(13, 4))
for ax, col, title, xlabel in [
    (axes[0], "z_iv_cat",          r"$Z_{-f,i,t}$ (Same-category Bartik IV)",  r"$Z_{-f,i,t}$"),
    (axes[1], "outflow_intensity",  r"OutflowIntensity$_{-f,i,t}$",             r"OutflowIntensity$_{-f,i,t}$"),
]:
    vals = panel[col].dropna()
    p1, p99 = vals.quantile(0.01), vals.quantile(0.99)
    ax.hist(vals.clip(p1, p99), bins=50, color="steelblue", edgecolor="white", linewidth=0.3)
    ax.axvline(0, color="black", linewidth=0.8, linestyle="--")
    ax.set_title(title + " (winsorized 1/99)")
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Fund-bond-months")
fig.tight_layout()
plt.show()


""" Run IV """
" Inflow fund subsample — shared holdings restriction "
# Contagion requires the inflow fund to already hold the bond at t-1 (w_{f,i,t-1} > 0).
# Observations where the inflow fund has no prior position reflect new entry (liquidity
# provision), not spillovers through overlapping holdings. Filtering to w_lag > 0 ensures
# we focus on the margin where contagion can mechanically arise.
df_in = panel[
    ~panel["outflow_fund"] &
    panel["delta_paramt"].notna() &
    (panel["w_lag"] > 0)
].copy().reset_index(drop=True)
del panel
print(f"\nInflow fund / shared-holdings obs: {len(df_in):,}")

df_in["sale"]        = df_in["is_sale"].astype(float)
df_in["neg_dparamt"] = -df_in["delta_paramt"]

# Rescale
df_in["ret_eom_lag"]         = df_in["ret_eom_lag"] * 100
df_in["bid_ask_lag"]         = df_in["bid_ask_lag"] * 100
df_in["amt_outstanding_lag"] = df_in["amt_outstanding_lag"] / 1e6
df_in["aum_lag"]             = df_in["aum_lag"] / 1e6

# Winsorize
WINS_COLS = ["neg_dparamt", "w_lag", "aum_lag", "ret_eom_lag", "bid_ask_lag",
             "cs_yield_lag", "tmt_lag", "amt_outstanding_lag",
             "z_iv_cat", "outflow_intensity"]
for col in WINS_COLS:
    if col in df_in.columns and df_in[col].notna().any():
        p1  = df_in[col].quantile(0.01)
        p99 = df_in[col].quantile(0.99)
        df_in[col] = df_in[col].clip(p1, p99)
        print(f"  {col}: winsorized to [{p1:.4g}, {p99:.4g}]")


" Stress indicator "
# High-stress months = months where median bond credit spread exceeds its time-series median.
# Kept for future heterogeneity analysis; not used in the baseline specification.
monthly_cs       = df_in.groupby("date_m_id")["cs_yield_lag"].median()
stress_threshold = monthly_cs.median()
stress_months    = set(monthly_cs[monthly_cs > stress_threshold].index)
df_in["stress"]  = df_in["date_m_id"].isin(stress_months).astype(float)
n_stress = df_in["stress"].sum()
print(f"\nHigh-stress months: {len(stress_months)} / {len(monthly_cs)}"
      f"  |  obs: {int(n_stress):,} ({n_stress / len(df_in) * 100:.1f}%)")

# Time-series of monthly median credit spread with stress periods shaded
cs_series = monthly_cs.sort_index()
dates = pd.to_datetime(cs_series.index.astype(str), format="%Y%m")

fig, ax = plt.subplots(figsize=(12, 4))
ax.plot(dates, cs_series.values, color="steelblue", linewidth=1.2, label="Median credit spread")
ax.axhline(stress_threshold, color="black", linewidth=0.8, linestyle="--", label=f"Threshold ({stress_threshold:.4f})")

# Shade high-stress months
is_stress = cs_series.index.isin(stress_months)
for i, (dt, stressed) in enumerate(zip(dates, is_stress)):
    if stressed:
        ax.axvspan(dt, dates[min(i + 1, len(dates) - 1)], alpha=0.2, color="firebrick", linewidth=0)

ax.set_title("Monthly Median Credit Spread and High-Stress Periods (shaded)")
ax.set_xlabel("Month")
ax.set_ylabel("Credit Spread (lagged, %)")
ax.legend()
fig.tight_layout()
plt.show()


" Controls and regression sample "
# First-stage controls: lagged bond characteristics X_{i,t-1} only.
# These are predetermined at the bond level and orthogonal to fund-specific unobservables.
CONTROLS_FS = ["ret_eom_lag", "bid_ask_lag", "cs_yield_lag",
               "rating_rank_lag", "tmt_lag", "amt_outstanding_lag"]

# Second-stage controls: bond characteristics + fund-level portfolio weight and AUM.
# Fund-level controls absorb variation in trading activity driven by fund size and position.
CONTROLS_SS = CONTROLS_FS + ["w_lag", "aum_lag"]
df_reg = df_in.dropna(subset=["z_iv_cat", "outflow_intensity"] + CONTROLS_SS).reset_index(drop=True)

# save down regression sample
df_reg.to_csv(os.path.join(PROC_DIR, "crsp/reg_panel_fit2.csv"))
del df_in
print(f"\nRegression sample: {len(df_reg):,}")

# Fixed effects: fund FE + time FE in both stages (α_f + δ_t)
df_reg["fund_fe"] = pd.Categorical(df_reg["crsp_portno"])
df_reg["time_fe"] = pd.Categorical(df_reg["date_m_id"])
absorb_r = df_reg[["fund_fe", "time_fe"]]
clust_r  = pd.Categorical(df_reg["cusip8"])    # cluster by bond


" Regression helper "
def run_reg(df, y_col, X_cols, label, absorb_fe, clust):
    y   = df[y_col]
    X   = df[X_cols]
    res = AbsorbingLS(y, X, absorb=absorb_fe, drop_absorbed=True).fit(
        cov_type="clustered", clusters=clust)
    print(f"\n--- {label} ---")
    print(res.summary)
    return res

def print_fs_F(res, iv_var="z_iv_cat"):
    t = res.tstats[iv_var]
    print(f"  First-stage t({iv_var}): {t:.3f}  (approx F = {t**2:.2f})")


" First stage "
# OutflowIntensity_{-f,i,t} = π Z_{-f,i,t} + Γ X_{i,t-1} + α_f + δ_t + u_{f,i,t}
print("\n" + "="*70)
print("FIRST STAGE: OutflowIntensity ~ Z_cat + bond controls + fund FE + time FE")
print("="*70)

res_fs = run_reg(df_reg, "outflow_intensity", ["z_iv_cat"] + CONTROLS_FS,
                 "FS: OutflowIntensity ~ Z_cat", absorb_r, clust_r)
print_fs_F(res_fs, "z_iv_cat")

# recover fitted values
df_reg["oi_hat"] = res_fs.fitted_values.values


" Second stage "
# Sale_{f,i,t} = β OIhat_{-f,i,t} + Γ X_{f,i,t-1} + α_f + δ_t + ε_{f,i,t}
print("\n" + "="*70)
print("SECOND STAGE")
print("="*70)

res_ss1a = run_reg(df_reg, "sale",        ["oi_hat"],               "SS-1a: Sale ~ OIhat",          absorb_r, clust_r)
res_ss1b = run_reg(df_reg, "sale",        ["oi_hat"] + CONTROLS_SS, "SS-1b: Sale ~ OIhat + ctrl",   absorb_r, clust_r)
res_ss2a = run_reg(df_reg, "neg_dparamt", ["oi_hat"],               "SS-2a: -ΔPar ~ OIhat",         absorb_r, clust_r)
res_ss2b = run_reg(df_reg, "neg_dparamt", ["oi_hat"] + CONTROLS_SS, "SS-2b: -ΔPar ~ OIhat + ctrl",  absorb_r, clust_r)


""" Save results """
os.makedirs(os.path.join(OUT_DIR, "tables"), exist_ok=True)

VAR_LABELS = {
    "z_iv_cat":          r"$Z_{-f,i,t}$ (Same-category)",
    "oi_hat":            r"$\widehat{\text{OutflowIntensity}}_{-f,i,t}$",
    "oi_hat_s":          r"$\widehat{\text{OutflowIntensity}}_{-f,i,t}$",
    "w_lag":             r"Weight$_{f,i,t-1}$",
    "aum_lag":           r"AUM$_{f,t-1}$ (\$B)",
    "ret_eom_lag":       r"Return$_{i,t-1}$ (\%)",
    "bid_ask_lag":       r"Bid-Ask$_{i,t-1}$ (\%)",
    "cs_yield_lag":      r"Credit Spread$_{i,t-1}$ (\%)",
    "rating_rank_lag":   r"Rating$_{i,t-1}$",
    "tmt_lag":           r"Maturity$_{i,t-1}$ (yrs)",
    "amt_outstanding_lag": r"Amt Outstanding$_{i,t-1}$ (\$M)",
}


def build_latex_iv(spec_fs, specs_ss, var_order_fs, var_order_ss,
                   has_controls, dep_var_labels, caption, label):
    def _stars(pval):
        if pval < 0.01: return "***"
        if pval < 0.05: return "**"
        if pval < 0.10: return "*"
        return ""

    def cell_dict(spec_list, var_order):
        cells, nobs = {}, {}
        for lbl, res in spec_list:
            nobs[lbl] = int(res.nobs)
            for var in var_order:
                if var in res.params.index:
                    c = f"{res.params[var]:.4f}{_stars(res.pvalues[var])}"
                    s = f"({res.std_errors[var]:.4f})"
                    cells[(lbl, var)] = (c, s)
        return cells, nobs

    cells_fs, nobs_fs = cell_dict(spec_fs,  var_order_fs)
    cells_ss, nobs_ss = cell_dict(specs_ss, var_order_ss)

    all_specs = [l for l, _ in spec_fs] + [l for l, _ in specs_ss]
    all_cells = {**cells_fs, **cells_ss}
    all_nobs  = {**nobs_fs,  **nobs_ss}
    n_cols    = len(all_specs)
    n_fs      = len(spec_fs)
    n_ss      = len(specs_ss)

    dep_var_row  = " & " + " & ".join(dep_var_labels[s] for s in all_specs) + r" \\"
    controls_row = " & ".join(r"$\checkmark$" if has_controls.get(s, False) else ""
                               for s in all_specs)

    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        rf"\caption{{{caption}}}",
        rf"\label{{{label}}}",
        r"{\small",
        r"\begin{tabular}{l" + "c" * n_cols + "}",
        r"\toprule",
        rf" & \multicolumn{{{n_fs}}}{{c}}{{First Stage}} "
        rf"& \multicolumn{{{n_ss}}}{{c}}{{Second Stage}} \\",
        rf"\cmidrule(lr){{2-{n_fs+1}}}\cmidrule(lr){{{n_fs+2}-{n_cols+1}}}",
        " & " + " & ".join(all_specs) + r" \\",
        dep_var_row,
        r"\midrule",
    ]

    all_var_order = (
        [v for v in var_order_fs if v not in var_order_ss] +
        [v for v in var_order_ss if v not in var_order_fs] +
        [v for v in var_order_fs if v in var_order_ss]
    )
    for var in all_var_order:
        lbl_str  = VAR_LABELS.get(var, var)
        coef_row = lbl_str
        se_row   = ""
        for col in all_specs:
            c, s = all_cells.get((col, var), ("", ""))
            coef_row += f" & {c}"
            se_row   += f" & {s}"
        lines.append(coef_row + r" \\")
        lines.append(se_row   + r" \\")

    lines += [
        r"\midrule",
        r"Fund FE & "  + " & ".join([r"$\checkmark$"] * n_cols) + r" \\",
        r"Month FE & " + " & ".join([r"$\checkmark$"] * n_cols) + r" \\",
        r"Controls & " + controls_row + r" \\",
        r"\midrule",
        r"$N$ & " + " & ".join(f"{all_nobs[c]:,}" for c in all_specs) + r" \\",
        r"\bottomrule",
        r"\end{tabular}",
        r"}",
        r"\begin{tablenotes}\small",
        r"\item \textit{Notes:} Sample: inflow fund--bond--months with $w_{f,i,t-1}>0$."
        r" Instrument $Z_{-f,i,t}$ aggregates lagged portfolio weights of same-category"
        r" outflow funds weighted by the category-level AUM-weighted flow shock"
        r" (Eq.~\ref{eqn:iv_samecat})."
        r" Endogenous variable $\text{OutflowIntensity}_{-f,i,t}$ is the fraction of"
        r" bond $i$ amount outstanding sold by outflow funds (Eq.~\ref{eqn:outflow_intensity_loo})."
        r" Fund and month FEs absorbed in both stages."
        r" SE clustered by bond.",
        r"*** $p<0.01$, ** $p<0.05$, * $p<0.10$.",
        r"\end{tablenotes}",
        r"\end{table}",
    ]
    return "\n".join(lines)


# ── Table 1: Baseline IV ──────────────────────────────────────────────────────
tex1 = build_latex_iv(
    spec_fs        = [("(FS)",    res_fs)],
    specs_ss       = [("(IV-1a)", res_ss1a), ("(IV-1b)", res_ss1b),
                      ("(IV-2a)", res_ss2a), ("(IV-2b)", res_ss2b)],
    var_order_fs   = ["z_iv_cat"] + CONTROLS_FS,
    var_order_ss   = ["oi_hat"]   + CONTROLS_SS,
    has_controls   = {"(FS)": False, "(IV-1a)": False, "(IV-1b)": True,
                      "(IV-2a)": False, "(IV-2b)": True},
    dep_var_labels = {"(FS)":    r"OutflowIntensity$_{-f,i,t}$",
                      "(IV-1a)": r"Sale$_{f,i,t}$",
                      "(IV-1b)": r"Sale$_{f,i,t}$",
                      "(IV-2a)": r"$-\Delta$Paramt$_{f,i,t}$",
                      "(IV-2b)": r"$-\Delta$Paramt$_{f,i,t}$"},
    caption = r"Contagion Test: IV Estimates (Same-Category Bartik, Shared Holdings)",
    label   = "tab:contagion_iv_fix1",
)
with open(os.path.join(OUT_DIR, "tables/contagion_iv_fix.tex"), "w") as f:
    f.write(tex1)



# ── Tables 2a/2b: Stress subgroups ───────────────────────────────────────────
for tag, file_sfx in [("Hi", "stress_hi"), ("Lo", "stress_lo")]:
    r = stress_results[tag]
    lbl_long = ("High-Stress Months (above-median credit spread)"
                if tag == "Hi" else "Low-Stress Months (below-median credit spread)")
    tex_s = build_latex_iv(
        spec_fs        = [(f"(FS)",    r["fs"])],
        specs_ss       = [(f"(IV-1b)", r["ss1b"]),
                          (f"(IV-2b)", r["ss2b"])],
        var_order_fs   = ["z_iv_cat"] + CONTROLS_FS,
        var_order_ss   = ["oi_hat_s"] + CONTROLS_SS,
        has_controls   = {"(FS)": False, "(IV-1b)": True, "(IV-2b)": True},
        dep_var_labels = {"(FS)":    r"OutflowIntensity$_{-f,i,t}$",
                          "(IV-1b)": r"Sale$_{f,i,t}$",
                          "(IV-2b)": r"$-\Delta$Paramt$_{f,i,t}$"},
        caption = rf"Contagion Test: {lbl_long}",
        label   = f"tab:contagion_iv_{file_sfx}",
    )
    fname = f"contagion_iv_{file_sfx}.tex"
    with open(os.path.join(OUT_DIR, f"tables/{fname}"), "w") as f:
        f.write(tex_s)
    print(f"Saved {fname}")
