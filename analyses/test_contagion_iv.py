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
del bm
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
del hc, fund_total, fund_ig, fund_long
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
del data, fund_month_cls, bm_merge
print("After adding MF invclass:", panel.shape)

# Save down identifier
f_cls.to_csv(os.path.join(PROC_DIR, "crsp/mf_invclass.csv"))
del f_cls


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
del cat_agg

# Leave-one-out: subtract own flow, divide by (n-1)
fund_flow["agg_flow_shock"] = np.where(
    fund_flow["cat_n"] > 1,
    (fund_flow["cat_flow_sum"] - fund_flow["flow"]) / (fund_flow["cat_n"] - 1),
    np.nan
)
print(f"AggFlowShock: {fund_flow['agg_flow_shock'].notna().sum():,} valid fund-months")


" Predicted flow "
# FlowHat_{f,t} = AggFlowShock_{c(f),t} directly (no beta scaling)
fund_flow["flow_hat"] = fund_flow["agg_flow_shock"]
print(f"FlowHat: {fund_flow['flow_hat'].notna().sum():,} / {len(fund_flow):,} valid fund-months ({fund_flow['flow_hat'].notna().mean()*100:.1f}%)")

fig, ax = plt.subplots(figsize=(7, 4))
vals = fund_flow["agg_flow_shock"].dropna()
p1, p99 = vals.quantile(0.01), vals.quantile(0.99)
ax.hist(vals.clip(p1, p99), bins=50, color="steelblue", edgecolor="white", linewidth=0.3)
ax.axvline(0, color="black", linewidth=0.8, linestyle="--")
ax.set_title(r"AggFlowShock$_{c(f),t}$ (= FlowHat, winsorized 1/99)")
ax.set_xlabel("Value")
ax.set_ylabel("Fund-months")
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
del fund_flow
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
del z_full
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
#del panel
print(f"\nInflow fund observations: {len(df_in):,}")

df_in["sale"]        = df_in["is_sale"].astype(float)
df_in["neg_dparamt"] = -df_in["delta_paramt"]
df_in["common_sold"] = (df_in["bond_cat"] == "common_sold").astype(float)
df_in["fire_sold"]   = (df_in["bond_cat"] == "fire_sold").astype(float)

# Rescale (same as OLS)
df_in["ret_eom_lag"]         = df_in["ret_eom_lag"] * 100
df_in["bid_ask_lag"]         = df_in["bid_ask_lag"] * 100
df_in["amt_outstanding_lag"] = df_in["amt_outstanding_lag"] / 1e6
df_in["aum_lag"]             = df_in["aum_lag"] / 1e6

# Winsorize
WINS_COLS = ["neg_dparamt", "w_lag", "aum_lag", "ret_eom_lag", "bid_ask_lag",
             "cs_yield_lag", "tmt_lag", "amt_outstanding_lag", "z_iv"]
print("\n--- Distributions before winsorizing ---")
print(df_in[["sale", "common_sold", "z_iv"] + WINS_COLS]
      .describe(percentiles=[.01, .05, .25, .5, .75, .95, .99]).T.to_string())

for col in WINS_COLS:
    p1  = df_in[col].quantile(0.01)
    p99 = df_in[col].quantile(0.99)
    df_in[col] = df_in[col].clip(p1, p99)
    print(f"  {col}: winsorized to [{p1:.4g}, {p99:.4g}]")

n_cols = 3
n_rows = int(np.ceil(len(WINS_COLS) / n_cols))
fig, axes = plt.subplots(n_rows, n_cols, figsize=(14, n_rows * 3))
axes = axes.flatten()
for ax, col in zip(axes, WINS_COLS):
    ax.hist(df_in[col].dropna(), bins=50, color="steelblue", edgecolor="white", linewidth=0.3)
    ax.axvline(0, color="black", linewidth=0.8, linestyle="--")
    ax.set_title(col)
    ax.set_xlabel("Value (winsorized 1/99)")
    ax.set_ylabel("Count")
for ax in axes[len(WINS_COLS):]:
    ax.set_visible(False)
fig.tight_layout()
plt.show()


" Controls "
# Both stages use the same fund-bond-month panel → identical controls throughout.
CONTROLS = ["ret_eom_lag", "bid_ask_lag", "cs_yield_lag", "rating_rank_lag", 
            "tmt_lag", "amt_outstanding_lag", "w_lag", "aum_lag"]
CONTROLS_FS = CONTROLS
CONTROLS_SS = CONTROLS

# Working sample: drop missing controls and z_iv
df_reg = df_in.dropna(subset=["z_iv"] + CONTROLS_SS).reset_index(drop=True)
del df_in
print(f"\nRegression sample: {len(df_reg):,}")

df_reg["fund_fe"] = pd.Categorical(df_reg["crsp_portno"])
df_reg["time_fe"] = pd.Categorical(df_reg["date_m_id"])
absorb_r = df_reg[["fund_fe", "time_fe"]]
clust_r  = pd.Categorical(df_reg["cusip8"])

#df_reg.to_csv(os.path.join(PROC_DIR, "crsp/reg_panel_fit.csv"))

" First stage (fund-bond-month level) "
# 1[CommonSold]_{f,i,t} = pi * Z_{f,i,t} + Gamma * X_{i,t-1} + alpha_f + delta_t + u_{f,i,t}
# Z_{f,i,t} is the LOO Bartik instrument; same fund FE + time FE as second stage.
# common_sold has no within-fund variation (bond-month outcome), so fund FE absorbs
# the fund-average commonality; identification comes from cross-bond variation within fund-time.
y_fs  = df_reg["common_sold"]
X_fs  = df_reg[["z_iv"] + CONTROLS_FS]
res_fs = AbsorbingLS(y_fs, X_fs, absorb=absorb_r, drop_absorbed=True).fit(
    cov_type="clustered", clusters=clust_r)
print("\n--- First Stage ---")
print(res_fs.summary)

# Kleibergen-Paap style first-stage F (approximate: t-stat^2 on Z)
t_z = res_fs.tstats["z_iv"]
print(f"First-stage t-stat on Z: {t_z:.3f}  (approx F = {t_z**2:.2f})")
# First-stage t-stat on Z: -14.606  (approx F = 213.33)

# Get fitted values of common sold
df_reg["common_sold_hat"] = res_fs.fitted_values.values


" Second stage (fund-bond-month level) "
# Sale_{f,i,t} = beta * CommonSoldHat_{f,i,t} + Gamma * X_{f,i,t-1} + alpha_f + delta_t + eps
# Note: SEs do not correct for the generated regressor; treat as approximate.
df_ss     = df_reg
absorb_ss = absorb_r
clust_ss  = clust_r

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


""" Save results """
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
    "aum_lag":             r"AUM$_{f,t-1}$ (\$B)",
    "ret_eom_lag":         r"Return$_{i,t-1}$ (\%)",
    "bid_ask_lag":         r"Bid-Ask$_{i,t-1}$ (\%)",
    "cs_yield_lag":        r"Credit Spread$_{i,t-1}$ (\%)",
    "rating_rank_lag":     r"Rating$_{i,t-1}$",
    "tmt_lag":             r"Maturity$_{i,t-1}$ (yrs)",
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

# Per-spec metadata
HAS_CONTROLS = {
    "(FS)":    False,
    "(IV-1a)": False,
    "(IV-1b)": True,
    "(IV-2a)": False,
    "(IV-2b)": True,
}
DEP_VAR_LABELS = {
    "(FS)":    r"$1[\text{CommonSold}]_{i,t}$",
    "(IV-1a)": r"Sale$_{f,i,t}$",
    "(IV-1b)": r"Sale$_{f,i,t}$",
    "(IV-2a)": r"$-\Delta$Paramt$_{f,i,t}$",
    "(IV-2b)": r"$-\Delta$Paramt$_{f,i,t}$",
}

# LaTeX table: first stage + second stage combined
def build_latex_iv(spec_fs, specs_ss, var_order_fs, var_order_ss,
                   has_controls, dep_var_labels,
                   caption, label):
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
    n_cols = len(all_specs)
    n_fs   = len(spec_fs)
    n_ss   = len(specs_ss)

    dep_var_row  = " & " + " & ".join(dep_var_labels[s] for s in all_specs) + r" \\"
    controls_row = " & ".join(r"$\checkmark$" if has_controls.get(s, False) else "" for s in all_specs)

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

    # FS-only vars first, then SS-only vars, then shared controls
    all_var_order = (
        [v for v in var_order_fs if v not in var_order_ss] +
        [v for v in var_order_ss if v not in var_order_fs] +
        [v for v in var_order_fs if v in var_order_ss]
    )
    for var in all_var_order:
        lbl_str  = VAR_LABELS_IV.get(var, var)
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
        r"Fund FE & " + " & ".join([r"$\checkmark$"] * n_cols) + r" \\",
        r"Month FE & "       + " & ".join([r"$\checkmark$"] * n_cols) + r" \\",
        r"Controls & "       + controls_row + r" \\",
        r"\midrule",
        r"$N$ & " + " & ".join(f"{all_nobs[c]:,}" for c in all_specs) + r" \\",
        r"\bottomrule",
        r"\end{tabular}",
        r"}",
        r"\begin{tablenotes}\small",
        r"\item \textit{Notes:} Instrument $Z_{i,t}$ is the Bartik shift-share predicted sale"
        r" pressure (Eq.~\ref{eqn:iv}), constructed leave-one-out.",
        r"First stage (fund-bond-month): fund and month FEs absorbed.",
        r"Second stage (fund-bond-month): fund and month FEs absorbed; same controls.",
        r"SE clustered by bond. Second-stage SEs do not correct for the generated regressor.",
        r"*** $p<0.01$, ** $p<0.05$, * $p<0.10$.",
        r"\end{tablenotes}",
        r"\end{table}",
    ]
    return "\n".join(lines)


tex_iv = build_latex_iv(
    spec_fs        = specs_fs,
    specs_ss       = specs_iv,
    var_order_fs   = ["z_iv"] + CONTROLS_FS,
    var_order_ss   = ["common_sold_hat"] + CONTROLS_SS,
    has_controls   = HAS_CONTROLS,
    dep_var_labels = DEP_VAR_LABELS,
    caption = "Contagion Test: IV Estimates (Bartik Shift-Share)",
    label   = "tab:contagion_iv",
)
tex_path = os.path.join(OUT_DIR, "tables/contagion_iv2.tex")
with open(tex_path, "w") as f:
    f.write(tex_iv)
print(f"Saved {tex_path}")
