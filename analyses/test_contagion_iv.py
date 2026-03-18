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


""" Parameters """
# Pre-sample period for estimating category-level beta
PRE_SAMPLE_END = 201112    # Dec 2011: data before the main sample used to estimate beta_cat

# Fund classification thresholds (based on max share of holdings across time)
IG_RTG_THRESH   = 10       # rating_rank <= 10 → IG
LONG_TMT_THRESH = 10       # tmt >= 10 years → long-duration
SHARE_IG_MIN    = 0.95     # fund classified as IG if max shareIG >= 0.95
SHARE_LONG_MIN  = 0.05     # fund classified as Long if max shareLong >= 0.05
SHARE_RETAIL    = 0.85     # fund classified as Retail if share_tna_retail >= 0.90


""" Read data """
# Fund-bond-month level holdings with bond_cat and fund_type
data = pd.read_csv(os.path.join(PROC_DIR, "crsp/mf_holdings_bond_cat.csv"))
print("Read holdings data:", data.shape)
print("Columns:", data.columns.tolist())

# Bond-month level characteristics
bm = pyreadr.read_r(os.path.join(PROC_DIR, "merges/bond_returns_master.rds"))[None]
print("Read bond-month characteristics:", bm.shape)
bm["cusip8"]    = bm["cusip"].str[:8]
bm["date_m_id"] = bm["mdate"].astype(int)

# retail vs institutional MF
mf_retail = pyreadr.read_r(os.path.join(PROC_DIR, "crsp/mf_retail_fm.rds"))[None]
print("Read MF retail identifier:", mf_retail.shape)
print("Columns:", mf_retail.columns.tolist())
print(mf_retail["retail_fund"].value_counts(dropna=False))


""" Merge with bond characteristics """
# Lag bond and fund-level characteristics
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
print(bm_merge.count())

data = data.sort_values(["crsp_portno", "cusip8", "date_m_id"]).reset_index(drop=True)
for col, grp in [("w",   ["crsp_portno", "cusip8"]),
                 ("aum", ["crsp_portno", "cusip8"])]:
    raw_lag  = data.groupby(grp)[col].shift(1)
    date_lag = data.groupby(grp)["date_m_id"].shift(1)
    data[f"{col}_lag"] = np.where(date_lag == data["prev_date_m_id"], raw_lag, np.nan)

data = pd.merge(data, bm_merge, on=["cusip8", "date_m_id"], how="left")
print(f"After merging bond-month chars: {data.shape}")


""" Classify funds into 8 categories """
# Long/Short × IG/HY × Retail/Inst
# Use lagged bond characteristics to measure fund-level exposure to IG and long-duration bonds.
# Category c(f) = fund_rtg / fund_ttm / fund_inv (e.g., "IG/Long/Inst")

# Holdings-level: paramt weighted by IG and long-duration flags
hc = data[["crsp_portno", "date_m_id", "paramt", "rating_rank_lag", "tmt_lag"]].copy()

fund_total = (hc.groupby(["crsp_portno", "date_m_id"])["paramt"]
               .sum().rename("total_paramt").reset_index())
fund_ig    = (hc[hc["rating_rank_lag"] <= IG_RTG_THRESH]
               .groupby(["crsp_portno", "date_m_id"])["paramt"]
               .sum().rename("ig_paramt").reset_index())
fund_long  = (hc[hc["tmt_lag"] >= LONG_TMT_THRESH]
               .groupby(["crsp_portno", "date_m_id"])["paramt"]
               .sum().rename("long_paramt").reset_index())

fund_month_cls = (fund_total
                  .merge(fund_ig,   on=["crsp_portno", "date_m_id"], how="left")
                  .merge(fund_long, on=["crsp_portno", "date_m_id"], how="left"))
fund_month_cls[["ig_paramt", "long_paramt"]] = fund_month_cls[["ig_paramt", "long_paramt"]].fillna(0)
fund_month_cls["shareIG"]   = fund_month_cls["ig_paramt"]   / fund_month_cls["total_paramt"]
fund_month_cls["shareLong"] = fund_month_cls["long_paramt"] / fund_month_cls["total_paramt"]

# Take the max share across time per fund (same approach as commented PCA code)
f_cls = (fund_month_cls.groupby("crsp_portno")[["shareIG", "shareLong"]]
                       .max().reset_index())
f_cls["fund_rtg"] = np.where(f_cls["shareIG"]   >= SHARE_IG_MIN,   "IG",    "HY")
f_cls["fund_ttm"] = np.where(f_cls["shareLong"] >= SHARE_LONG_MIN, "Long",  "Short")

# Retail vs Institutional: max share_tna_retail across months per fund
retail_cls = (mf_retail.groupby("crsp_portno")["share_tna_retail"]
                        .max().rename("share_tna_retail_max").reset_index())
f_cls = f_cls.merge(retail_cls, on="crsp_portno", how="left")
f_cls["fund_inv"] = np.where(f_cls["share_tna_retail_max"] >= SHARE_RETAIL, "Retail", "Inst")
print(f"Retail/Inst split (SHARE_RETAIL={SHARE_RETAIL}):")
print(f_cls["fund_inv"].value_counts(dropna=False))

f_cls["fund_cat"] = f_cls["fund_rtg"] + "/" + f_cls["fund_ttm"] + "/" + f_cls["fund_inv"]
print("Fund category distribution:")
print(f_cls["fund_cat"].value_counts())

# Merge fund_cat onto the panel
panel = pd.merge(data, f_cls[["crsp_portno", "fund_cat"]], on="crsp_portno", how="left")
print("After adding MF invclass:", panel.shape)

# Save down identifier
f_cls.to_csv(os.path.join(PROC_DIR, "crsp/mf_invclass.csv"))


""" Construct IV """
" Category-level average flow (leave-one-out) "
# AggFlowShock_{c(f),t} = mean flow of funds in category c(f) at t, excluding fund f itself
fund_flow = (panel.drop_duplicates(["crsp_portno", "date_m_id"])
                  [["crsp_portno", "date_m_id", "flow", "fund_cat"]]
                 .dropna(subset=["flow", "fund_cat"])
                 .copy()
                 .reset_index(drop=True))

cat_agg = (fund_flow.groupby(["fund_cat", "date_m_id"])
                    .agg(cat_flow_sum=("flow", "sum"), cat_n=("flow", "count"))
                    .reset_index())
fund_flow = fund_flow.merge(cat_agg, on=["fund_cat", "date_m_id"], how="left")

# Leave-one-out: subtract own flow, divide by (n-1)
fund_flow["agg_flow_shock"] = np.where(
    fund_flow["cat_n"] > 1,
    (fund_flow["cat_flow_sum"] - fund_flow["flow"]) / (fund_flow["cat_n"] - 1),
    np.nan
)
print(f"AggFlowShock: {fund_flow['agg_flow_shock'].notna().sum():,} valid fund-months")


" Category-level beta (pre-sample) "
# Estimate one beta per fund category using pre-sample data (date_m_id <= PRE_SAMPLE_END).
# Pooling all funds within a category produces a much more stable estimate than fund-level
# rolling OLS, while still preserving the Bartik structure: FlowHat = beta_cat * AggFlowShock.

def _beta_ols(grp):
    """OLS slope of flow on agg_flow_shock; returns NaN if insufficient data."""
    x = grp["agg_flow_shock"].values
    y = grp["flow"].values
    valid = np.isfinite(x) & np.isfinite(y)
    if valid.sum() < 12:
        return np.nan
    xv, yv = x[valid], y[valid]
    xm = xv.mean()
    denom = ((xv - xm) ** 2).sum()
    if denom < 1e-12:
        return np.nan
    return float(((xv - xm) * (yv - yv.mean())).sum() / denom)

pre = fund_flow[fund_flow["date_m_id"] <= PRE_SAMPLE_END].dropna(subset=["flow", "agg_flow_shock"])
beta_cat = (pre.groupby("fund_cat")
               .apply(_beta_ols, include_groups=False)
               .rename("beta_cat")
               .reset_index())
print("Category-level beta estimates:")
print(beta_cat.to_string(index=False))

fund_flow = fund_flow.merge(beta_cat, on="fund_cat", how="left")


" Predicted flow "
# FlowHat_{f,t} = beta_cat * AggFlowShock_{c(f),t}
fund_flow["flow_hat"] = fund_flow["beta_cat"] * fund_flow["agg_flow_shock"]
print(f"FlowHat: {fund_flow['flow_hat'].notna().sum():,} / {len(fund_flow):,} valid fund-months ({fund_flow['flow_hat'].notna().mean()*100:.1f}%)")

fig, axes = plt.subplots(1, 3, figsize=(14, 4))
for ax, col, title in zip(axes,
                           ["agg_flow_shock", "beta_cat", "flow_hat"],
                           [r"AggFlowShock$_{c(f),t}$",
                            r"$\hat{\beta}_{c(f)}$ (pre-sample)",
                            r"$\widehat{\mathrm{Flow}}_{f,t}$"]):
    vals = fund_flow[col].dropna()
    p1, p99 = vals.quantile(0.01), vals.quantile(0.99)
    ax.hist(vals.clip(p1, p99), bins=50, color="steelblue", edgecolor="white", linewidth=0.3)
    ax.axvline(0, color="black", linewidth=0.8, linestyle="--")
    ax.set_title(title)
    ax.set_xlabel("Value (winsorized 1/99)")
    ax.set_ylabel("Count")
fig.tight_layout()
plt.show()


" Construct IV Z_{i,t} "
# Bartik shift-share, leave-one-out
# ============================================================
# Z_{i,t} = sum_f w_{f,i,t-1} * FlowHat_{f,t}
#
# Leave-one-out for inflow fund f_0:
#   Z_{i,t,-f_0} = Z_{i,t} - w_{f_0,i,t-1} * FlowHat_{f_0,t}
#
# This is efficient: compute the full Z first, then subtract the focal fund's contribution.

# Merge flow_hat onto the fund-bond-month panel
panel = panel.merge(fund_flow[["crsp_portno", "date_m_id", "flow_hat"]],
                    on=["crsp_portno", "date_m_id"], how="left")
print("After adding flow hat:", panel.shape)

# Contribution of each fund-bond-month to Z: only funds with predicted outflows (flow_hat < 0)
# Restricting to predicted-outflow funds ensures Z captures exogenous redemption pressure only,
# preventing inflow fund holdings from contaminating the instrument (sign fix).
panel["z_contrib"] = np.where(panel["flow_hat"] < 0,
                               panel["w_lag"] * panel["flow_hat"],
                               0)

# Full bond-month Z: sum outflow-fund contributions across all funds
z_full = (panel[panel["z_contrib"] != 0]
               .groupby(["cusip8", "date_m_id"])["z_contrib"]
               .sum().rename("z_full").reset_index())

panel = pd.merge(panel, z_full, on=["cusip8", "date_m_id"], how="left")
panel["z_full"] = panel["z_full"].fillna(0)

# LOO IV: for focal inflow fund f_0, subtract its own contribution (if flow_hat < 0)
# For inflow funds flow_hat >= 0 so z_contrib = 0 → LOO has no effect for them,
# but we keep the structure for consistency.
panel["z_iv"] = panel["z_full"] - np.where(~panel["outflow_fund"], panel["z_contrib"], 0)
print(f"IV constructed. Non-null: {panel['z_iv'].notna().sum():,} / {len(panel):,} ({panel['z_iv'].notna().mean()*100:.1f}%)")
print(panel["z_iv"].describe())

p1, p99 = panel["z_iv"].quantile(0.01), panel["z_iv"].quantile(0.99)
fig, ax = plt.subplots(figsize=(7, 4))
ax.hist(panel["z_iv"].clip(p1, p99), bins=50, color="steelblue", edgecolor="white", linewidth=0.3)
ax.axvline(0, color="black", linewidth=0.8, linestyle="--")
ax.set_title(r"$Z_{i,t,-f}$ (LOO Bartik IV, winsorized 1/99)")
ax.set_xlabel(r"$Z_{i,t,-f}$")
ax.set_ylabel("Fund-bond-months")
fig.tight_layout()
plt.show()


""" Run IV regression """
" Inflow fund subsample "
df_in = panel[~panel["outflow_fund"] & panel["delta_paramt"].notna()].copy().reset_index(drop=True)
print(f"\nInflow fund observations: {len(df_in):,}")

df_in["sale"]        = df_in["is_sale"].astype(float)
df_in["neg_dparamt"] = -df_in["delta_paramt"]
df_in["common_sold"] = (df_in["bond_cat"] == "common_sold").astype(float)
df_in["fire_sold"]   = (df_in["bond_cat"] == "fire_sold").astype(float)

# Rescale (same as OLS)
df_in["ret_eom_lag"]         = df_in["ret_eom_lag"] * 100
df_in["bid_ask_lag"]         = df_in["bid_ask_lag"] * 100
df_in["amt_outstanding_lag"] = df_in["amt_outstanding_lag"] / 1e6

# Winsorize
WINS_COLS = ["neg_dparamt", "w_lag", "ret_eom_lag", "bid_ask_lag",
             "cs_yield_lag", "amt_outstanding_lag", "z_iv"]
print("\n--- Distributions before winsorizing ---")
print(df_in[["sale", "common_sold", "z_iv"] + WINS_COLS]
      .describe(percentiles=[.01, .05, .25, .5, .75, .95, .99]).T.to_string())

for col in WINS_COLS:
    p1  = df_in[col].quantile(0.01)
    p99 = df_in[col].quantile(0.99)
    df_in[col] = df_in[col].clip(p1, p99)
    print(f"  {col}: winsorized to [{p1:.4g}, {p99:.4g}]")


" First stage (bond-month level) "
# 1[CommonSold]_{i,t} = pi * Z_{i,t} + Gamma * X_{i,t-1} + alpha_i + delta_t + u_{i,t}
# bond FE = alpha_i, time FE = delta_t
# Controls X: lagged bond characteristics (no fund-level controls at this stage)
CONTROLS_FS = ["ret_eom_lag", "bid_ask_lag", "cs_yield_lag", "amt_outstanding_lag"]

# Collapse to bond-month: take first observation per bond-month (Z and bond chars are bond-level)
bm_panel = (df_in.drop_duplicates(["cusip8", "date_m_id"])
                 [["cusip8", "date_m_id", "common_sold", "z_iv"] + CONTROLS_FS]
                 .dropna(subset=["z_iv"] + CONTROLS_FS)
                 .copy()
                 .reset_index(drop=True))
print(f"\nBond-month panel for first stage: {len(bm_panel):,}")

bm_panel["bond_fe"] = pd.Categorical(bm_panel["cusip8"])
bm_panel["time_fe"] = pd.Categorical(bm_panel["date_m_id"])
absorb_fs = bm_panel[["bond_fe", "time_fe"]]
clust_fs  = pd.Categorical(bm_panel["cusip8"])

y_fs = bm_panel["common_sold"]
X_fs = bm_panel[["z_iv"] + CONTROLS_FS]
res_fs = AbsorbingLS(y_fs, X_fs, absorb=absorb_fs, drop_absorbed=True).fit(
    cov_type="clustered", clusters=clust_fs)
print("\n--- First Stage ---")
print(res_fs.summary)

# Kleibergen-Paap style first-stage F (approximate: t-stat^2 on Z)
t_z = res_fs.tstats["z_iv"]
print(f"First-stage t-stat on Z: {t_z:.3f}  (approx F = {t_z**2:.2f})")

# Fitted values of CommonSold from first stage
bm_panel["common_sold_hat"] = res_fs.fitted_values.values


# ============================================================
# 10. Second stage (fund-bond-month level for inflow funds)
# ============================================================
# Sale_{f,i,t} = beta * CommonSoldHat_{i,t} + Gamma * X_{f,i,t-1} + alpha_f + delta_t + eps
# Note: SEs do not correct for the generated regressor (CommonSoldHat); treat as approximate.

CONTROLS_SS = ["w_lag", "ret_eom_lag", "bid_ask_lag", "cs_yield_lag", "amt_outstanding_lag"]

df_ss = df_in.merge(bm_panel[["cusip8", "date_m_id", "common_sold_hat"]],
                    on=["cusip8", "date_m_id"], how="inner")
df_ss = df_ss.dropna(subset=CONTROLS_SS + ["common_sold_hat"]).reset_index(drop=True)
print(f"\nObservations in second stage: {len(df_ss):,} "
      f"({len(df_ss)/len(df_in)*100:.1f}% of df_in)")

df_ss["fund_fe"] = pd.Categorical(df_ss["crsp_portno"])
df_ss["time_fe"] = pd.Categorical(df_ss["date_m_id"])
absorb_ss = df_ss[["fund_fe", "time_fe"]]
clust_ss  = pd.Categorical(df_ss["cusip8"])

def run_reg(df, y_col, X_cols, label, absorb_fe, clust):
    y   = df[y_col]
    X   = df[X_cols]
    res = AbsorbingLS(y, X, absorb=absorb_fe, drop_absorbed=True).fit(
        cov_type="clustered", clusters=clust)
    print(f"\n--- {label} ---")
    print(res.summary)
    return res

# Extensive margin
res_iv1a = run_reg(df_ss, "sale", ["common_sold_hat"],
                   "IV 1a: Sale ~ CommonSoldHat", absorb_ss, clust_ss)
res_iv1b = run_reg(df_ss, "sale", ["common_sold_hat"] + CONTROLS_SS,
                   "IV 1b: Sale ~ CommonSoldHat + controls", absorb_ss, clust_ss)

# Intensive margin
res_iv2a = run_reg(df_ss, "neg_dparamt", ["common_sold_hat"],
                   "IV 2a: -ΔParamt ~ CommonSoldHat", absorb_ss, clust_ss)
res_iv2b = run_reg(df_ss, "neg_dparamt", ["common_sold_hat"] + CONTROLS_SS,
                   "IV 2b: -ΔParamt ~ CommonSoldHat + controls", absorb_ss, clust_ss)


# ============================================================
# 11. Save results
# ============================================================
os.makedirs(os.path.join(OUT_DIR, "tables"), exist_ok=True)

def fmt(val, decimals=4): return f"{val:.{decimals}f}"
def stars(pval):
    if pval < 0.01: return "***"
    if pval < 0.05: return "**"
    if pval < 0.10: return "*"
    return ""

VAR_LABELS_IV = {
    "z_iv":                r"$Z_{i,t}$",
    "common_sold_hat":     r"$\widehat{CommonSold}_{i,t}$",
    "w_lag":               r"Weight$_{f,i,t-1}$",
    "ret_eom_lag":         r"Return$_{i,t-1}$ (\%)",
    "bid_ask_lag":         r"Bid-Ask$_{i,t-1}$ (\%)",
    "cs_yield_lag":        r"Credit Spread$_{i,t-1}$ (\%)",
    "amt_outstanding_lag": r"Amt Outstanding$_{i,t-1}$ (\$M)",
}

specs_fs = [("(FS)",    res_fs)]
specs_iv = [
    ("(IV-1a)", res_iv1a), ("(IV-1b)", res_iv1b),
    ("(IV-2a)", res_iv2a), ("(IV-2b)", res_iv2b),
]

# CSV output
for name, spec_list, var_order in [
    ("iv_fs",  specs_fs,  ["z_iv"] + CONTROLS_FS),
    ("iv_ss",  specs_iv,  ["common_sold_hat"] + CONTROLS_SS),
]:
    rows = []
    for label, res in spec_list:
        for var in var_order:
            if var in res.params.index:
                rows.append({
                    "spec":     label,
                    "variable": var,
                    "coef":     fmt(res.params[var]),
                    "se":       f"({fmt(res.std_errors[var])})",
                    "sig":      stars(res.pvalues[var]),
                    "N":        f"{int(res.nobs):,}",
                })
    pd.DataFrame(rows).to_csv(os.path.join(OUT_DIR, f"tables/contagion_{name}.csv"), index=False)
    print(f"Saved tables/contagion_{name}.csv")

# LaTeX table: first stage + second stage combined
def build_latex_iv(spec_fs, specs_ss, var_order_fs, var_order_ss,
                   caption, label):
    def cell_dict(spec_list, var_order):
        cells, nobs = {}, {}
        for lbl, res in spec_list:
            nobs[lbl] = int(res.nobs)
            for var in var_order:
                if var in res.params.index:
                    c = f"{res.params[var]:.4f}{stars(res.pvalues[var])}"
                    s = f"({res.std_errors[var]:.4f})"
                    cells[(lbl, var)] = (c, s)
        return cells, nobs

    cells_fs, nobs_fs = cell_dict(spec_fs,  var_order_fs)
    cells_ss, nobs_ss = cell_dict(specs_ss, var_order_ss)

    all_specs  = [l for l, _ in spec_fs]  + [l for l, _ in specs_ss]
    all_cells  = {**cells_fs, **cells_ss}
    all_nobs   = {**nobs_fs,  **nobs_ss}
    n_cols = len(all_specs)

    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        rf"\caption{{{caption}}}",
        rf"\label{{{label}}}",
        r"{\small",
        r"\begin{tabular}{l" + "c" * n_cols + "}",
        r"\toprule",
        rf" & \multicolumn{{1}}{{c}}{{First Stage}} "
        rf"& \multicolumn{{{len(specs_ss)}}}{{c}}{{Second Stage}} \\",
        rf"\cmidrule(lr){{2-2}}\cmidrule(lr){{3-{n_cols+1}}}",
        " & " + " & ".join(all_specs) + r" \\",
        rf" & $1[\text{{CommonSold}}]_{{i,t}}$"
        + " & Sale$_{{f,i,t}}$" * 2
        + r" & $-\Delta$Paramt$_{{f,i,t}}$" * 2 + r" \\",
        r"\midrule",
    ]

    all_var_order = list(dict.fromkeys(var_order_fs + var_order_ss))
    for var in all_var_order:
        lbl_str = VAR_LABELS_IV.get(var, var)
        coef_row = lbl_str
        se_row   = ""
        for col in all_specs:
            key = (col, var)
            c, s = all_cells.get(key, ("", ""))
            coef_row += f" & {c}"
            se_row   += f" & {s}"
        lines.append(coef_row + r" \\")
        lines.append(se_row   + r" \\")

    controls_row  = [r"$\checkmark$" if i >= 1 else "" for i in range(n_cols)]
    lines += [
        r"\midrule",
        r"Fund / Bond FE & " + " & ".join([r"$\checkmark$"] * n_cols) + r" \\",
        r"Month FE & "       + " & ".join([r"$\checkmark$"] * n_cols) + r" \\",
        r"Controls & "       + " & ".join(controls_row) + r" \\",
        r"\midrule",
        r"$N$ & " + " & ".join(f"{all_nobs[c]:,}" for c in all_specs) + r" \\",
        r"\bottomrule",
        r"\end{tabular}",
        r"}",
        r"\begin{tablenotes}\small",
        r"\item \textit{Notes:} Instrument $Z_{i,t}$ is the Bartik shift-share predicted sale"
        r" pressure (Eq.~\ref{eqn:iv}), constructed leave-one-out.",
        r"First stage: bond and month FEs absorbed; SE clustered by bond.",
        r"Second stage: fund and month FEs absorbed; SE clustered by bond.",
        r"Second-stage SEs do not correct for the generated regressor.",
        r"*** $p<0.01$, ** $p<0.05$, * $p<0.10$.",
        r"\end{tablenotes}",
        r"\end{table}",
    ]
    return "\n".join(lines)


tex_iv = build_latex_iv(
    specs_fs   = specs_fs,
    specs_ss   = specs_iv,
    var_order_fs = ["z_iv"] + CONTROLS_FS,
    var_order_ss = ["common_sold_hat"] + CONTROLS_SS,
    caption = "Contagion Test: IV Estimates (Bartik Shift-Share)",
    label   = "tab:contagion_iv",
)
tex_path = os.path.join(OUT_DIR, "tables/contagion_iv.tex")
with open(tex_path, "w") as f:
    f.write(tex_iv)
print(f"Saved {tex_path}")
