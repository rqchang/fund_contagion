import pandas as pd
import numpy as np
import os
import sys
import pyreadr
import matplotlib.pyplot as plt
from linearmodels.iv.absorbing import AbsorbingLS

try:
    _root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
except NameError:
    _root = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, _root)
from utils.set_path import PROC_DIR, RAW_DIR, OUT_DIR, TEMP_DIR


""" Read in data """
# Fund-bond-month level holdings with bond_cat and fund_type
data = pd.read_csv(os.path.join(PROC_DIR, "crsp/mf_holdings_bond_cat.csv"))
print("Read holdings data:", data.shape)
print("Columns:", data.columns.tolist())

# Bond-month level characteristics
bm = pyreadr.read_r(os.path.join(PROC_DIR, "merges/bond_returns_master.rds"))[None]
print("Read bond-month characteristics:", bm.shape)
bm["cusip8"]    = bm["cusip"].str[:8]
bm["date_m_id"] = bm["mdate"].astype(int)


""" Merge with chars """
# Lag bond-month chars within cusip8 (ret_eom, bid_ask, cs_yield, rating_rank, tmt, amt_outstanding)
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

# Lag fund-bond-level (w) and fund-level (aum) chars from data before filtering
data = data.sort_values(["crsp_portno", "cusip8", "date_m_id"]).reset_index(drop=True)

for col, grp in [("w",   ["crsp_portno", "cusip8"]),
                 ("aum", ["crsp_portno", "cusip8"])]:
    raw_lag  = data.groupby(grp)[col].shift(1)
    date_lag = data.groupby(grp)["date_m_id"].shift(1)
    data[f"{col}_lag"] = np.where(date_lag == data["prev_date_m_id"], raw_lag, np.nan)

# Merge bm chars onto data
data = pd.merge(data, bm_merge, on=["cusip8", "date_m_id"], how="left")
print(f"After merging bond-month chars: {data.shape}")


""" Contagion test """
" Subsample "
# Restrict to inflow funds with valid delta_paramt (observed trade)
df_in = data[~data["outflow_fund"] & data["delta_paramt"].notna()].copy().reset_index(drop=True)
print(f"\nInflow fund observations (valid lag): {len(df_in):,}")

# Outcome and regressors
df_in["sale"]        = df_in["is_sale"].astype(float)
df_in["neg_dparamt"] = -df_in["delta_paramt"]          # positive = sold more par value
df_in["common_sold"] = (df_in["bond_cat"] == "common_sold").astype(float)
df_in["fire_sold"]   = (df_in["bond_cat"] == "fire_sold").astype(float)

# Rescale to comparable units to avoid numerical absorption in AbsorbingLS:
#   ret_eom_lag  : decimal → % (×100),  1 = 1pp return
#   bid_ask_lag  : decimal → % (×100),  1 = 1pp spread
#   cs_yield_lag : already in %,         1 = 1pp credit spread
#   amt_outstanding_lag: $ → $M (÷1e6), 1 = $1M outstanding
df_in['ret_eom_lag']         = df_in['ret_eom_lag'] * 100
df_in['bid_ask_lag']         = df_in['bid_ask_lag'] * 100
df_in['amt_outstanding_lag'] = df_in['amt_outstanding_lag'] / 1e6

# Save down data 
df_in.to_csv(os.path.join(PROC_DIR, "merges/inflow_ifm.csv"), index=False)


" Winsorization "
WINS_COLS = ["neg_dparamt", "w_lag", "ret_eom_lag", "bid_ask_lag", "cs_yield_lag", "amt_outstanding_lag"]

# Distribution before
print("\n--- Distributions before winsorizing ---")
print(df_in[["sale", "common_sold", "fire_sold"] + WINS_COLS + ["rating_rank_lag"]]
      .describe(percentiles=[.01, .05, .25, .5, .75, .95, .99]).T.to_string())
print(f"\nbond_cat counts:\n{df_in['bond_cat'].value_counts()}")

# Winsorize at 1st/99th
for col in WINS_COLS:
    p1  = df_in[col].quantile(0.01)
    p99 = df_in[col].quantile(0.99)
    df_in[col] = df_in[col].clip(p1, p99)
    print(f"  {col}: winsorized to [{p1:.4g}, {p99:.4g}]")

# Distribution after
print("\n--- Distributions after winsorizing ---")
print(df_in[["sale", "common_sold", "fire_sold"] + WINS_COLS + ["rating_rank_lag"]]
      .describe(percentiles=[.01, .05, .25, .5, .75, .95, .99]).T.to_string())


" Fixed effects and clustering "
# Drop rows with any missing control to keep N consistent across specs
CONTROLS = ["w_lag", "ret_eom_lag", "bid_ask_lag", "cs_yield_lag", "amt_outstanding_lag"]
df_reg = df_in.dropna(subset=CONTROLS).reset_index(drop=True)

df_reg["fund_fe"] = pd.Categorical(df_reg["crsp_portno"])
df_reg["time_fe"] = pd.Categorical(df_reg["date_m_id"])
absorb_r   = df_reg[["fund_fe", "time_fe"]]
clusters_r = pd.Categorical(df_reg["cusip8"])
print(f"\nObservations with all controls non-missing: {len(df_reg):,} "
      f"({len(df_reg)/len(df_in)*100:.1f}% of df_in)")  # (79.6% of df_in)

# Regression function
def run_reg(df, y_col, X_cols, label, absorb_fe, clust):
    y   = df[y_col]
    X   = df[X_cols]
    res = AbsorbingLS(y, X, absorb=absorb_fe, drop_absorbed=True).fit(cov_type="clustered", clusters=clust)
    print(f"\n--- {label} ---")
    print(res.summary)
    return res


" Equation 1: Extensive margin (sale dummy) "
controls = CONTROLS

# (1a) sale ~ CommonSold + FEs
res1a = run_reg(df_reg, "sale", ["common_sold"],                   
                 "1a: Sale ~ CommonSold", absorb_r, clusters_r)

# (1b) sale ~ CommonSold + FireSold + FEs
res1b = run_reg(df_reg, "sale", ["common_sold", "fire_sold"],        
                "1b: Sale ~ CommonSold + FireSold", absorb_r, clusters_r)

# (1c) sale ~ CommonSold + controls + FEs
res1c = run_reg(df_reg, "sale", ["common_sold"] + controls,          
                "1c: Sale ~ CommonSold + controls", absorb_r, clusters_r)

# (1d) sale ~ CommonSold + FireSold + controls + FEs
res1d = run_reg(df_reg, "sale", ["common_sold", "fire_sold"] + controls, 
                "1d: Sale ~ CommonSold + FireSold + controls", absorb_r, clusters_r)


" Equation 2: Intensive margin (−delta_paramt) "
# (2a) -delta_paramt ~ CommonSold + FEs
res2a = run_reg(df_reg, "neg_dparamt", ["common_sold"],                    
                "2a: −ΔParamt ~ CommonSold",  absorb_r, clusters_r)

# (2b) -delta_paramt ~ CommonSold + FireSold + FEs
res2b = run_reg(df_reg, "neg_dparamt", ["common_sold", "fire_sold"],        
                "2b: −ΔParamt ~ CommonSold + FireSold", absorb_r, clusters_r)

# (2c) -delta_paramt ~ CommonSold + controls + FEs
res2c = run_reg(df_reg, "neg_dparamt", ["common_sold"] + controls,          
                "2c: −ΔParamt ~ CommonSold + controls", absorb_r, clusters_r)

# (2d) -delta_paramt ~ CommonSold + FireSold + controls + FEs
res2d = run_reg(df_reg, "neg_dparamt", ["common_sold", "fire_sold"] + controls, 
                "2d: −ΔParamt ~ CommonSold + FireSold + controls", absorb_r, clusters_r)


# """ Summary table """
def fmt(val, decimals=4):
    return f"{val:.{decimals}f}"

def stars(pval):
    if pval < 0.01: return "***"
    if pval < 0.05: return "**"
    if pval < 0.10: return "*"
    return ""

specs = [
    ("(1a)", res1a), ("(1b)", res1b), ("(1c)", res1c), ("(1d)", res1d),
    ("(2a)", res2a), ("(2b)", res2b), ("(2c)", res2c), ("(2d)", res2d),
]

# rows = []
# for label, res in specs:
#     for var in ["common_sold", "fire_sold"] + CONTROLS:
#         if var in res.params.index:
#             coef = res.params[var]
#             se   = res.std_errors[var]
#             pval = res.pvalues[var]
#             rows.append({"spec": label, "variable": var,
#                          "coef": fmt(coef), "se": f"({fmt(se)})",
#                          "sig": stars(pval), "N": f"{int(res.nobs):,}"})

# summary = pd.DataFrame(rows)
# print("\n=== Contagion Test Results ===")
# print(summary.to_string(index=False))
# print("Fixed effects: fund + month | SE clustered by bond")

# # Test β1 > β2 in Equation 1b, 1d, 2b, 2d
# for label, res in [("(1b)", res1b), ("(1d)", res1d), ("(2b)", res2b), ("(2d)", res2d)]:
#     if "common_sold" in res.params.index and "fire_sold" in res.params.index:
#         b1, b2 = res.params["common_sold"], res.params["fire_sold"]
#         print(f"{label}: β1(CommonSold)={b1:.4f}, β2(FireSold)={b2:.4f}, "
#               f"β1>β2: {b1 > b2} (diff={b1-b2:.4f})")


""" Save results """
os.makedirs(os.path.join(OUT_DIR, "tables"), exist_ok=True)

# Split specs by equation
specs1 = [("(1a)", res1a), ("(1b)", res1b), ("(1c)", res1c), ("(1d)", res1d)]
specs2 = [("(2a)", res2a), ("(2b)", res2b), ("(2c)", res2c), ("(2d)", res2d)]

VAR_LABELS = {
    "common_sold":          r"CommonSold$_{i,t}$",
    "fire_sold":            r"FireSold$_{i,t}$",
    "w_lag":                r"Weight$_{f,i,t-1}$",
    "ret_eom_lag":          r"Return$_{i,t-1}$ (\%)",
    "bid_ask_lag":          r"Bid-Ask$_{i,t-1}$ (\%)",
    "cs_yield_lag":         r"Credit Spread$_{i,t-1}$ (\%)",
    "amt_outstanding_lag":  r"Amt Outstanding$_{i,t-1}$ (\$M)",
}
VAR_ORDER = ["common_sold", "fire_sold"] + CONTROLS


def build_cell_dict(spec_list):
    cell, nobs = {}, {}
    for label, res in spec_list:
        nobs[label] = int(res.nobs)
        for var in VAR_ORDER:
            if var in res.params.index:
                coef = res.params[var]
                se   = res.std_errors[var]
                pval = res.pvalues[var]
                cell[(label, var)] = (f"{coef:.4f}{stars(pval)}", f"({se:.4f})")
    return cell, nobs


def tex_cell(cell, key):
    return cell[key] if key in cell else ("", "")


def build_latex(spec_list, col_order, caption, label, dep_var_label):
    cell, nobs = build_cell_dict(spec_list)
    col_header = " & ".join(col_order)
    n_cols = len(col_order)
    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        rf"\caption{{{caption}}}",
        rf"\label{{{label}}}",
        r"{\small",
        r"\begin{tabular}{l" + "c" * n_cols + "}",
        r"\toprule",
        rf" & \multicolumn{{{n_cols}}}{{c}}{{{dep_var_label}}} \\",
        rf"\cmidrule(lr){{2-{n_cols + 1}}}",
        f" & {col_header} \\\\",
        r"\midrule",
    ]
    for var in VAR_ORDER:
        coef_row = VAR_LABELS.get(var, var)
        se_row   = ""
        for col in col_order:
            c, s = tex_cell(cell, (col, var))
            coef_row += f" & {c}"
            se_row   += f" & {s}"
        lines.append(coef_row + r" \\")
        lines.append(se_row   + r" \\")
    lines += [
        r"\midrule",
        r"Fund FE & "     + " & ".join([r"$\checkmark$"] * n_cols) + r" \\",
        r"Month FE & "    + " & ".join([r"$\checkmark$"] * n_cols) + r" \\",
        r"Controls & "    + " & ".join(["", "", r"$\checkmark$", r"$\checkmark$"]) + r" \\",
        r"\midrule",
        r"$N$ & " + " & ".join(f"{nobs[c]:,}" for c in col_order) + r" \\",
        r"\bottomrule",
        r"\end{tabular}",
        r"}",
        r"\begin{tablenotes}\small",
        r"\item \textit{Notes:} Sample restricted to inflow fund-bond-months with valid lagged paramt.",
        r"Controls: weight$_{t-1}$, return$_{t-1}$, bid-ask$_{t-1}$, credit spread$_{t-1}$, amount outstanding$_{t-1}$.",
        r"Fund and month fixed effects absorbed. Standard errors clustered by bond.",
        r"*** $p<0.01$, ** $p<0.05$, * $p<0.10$.",
        r"\end{tablenotes}",
        r"\end{table}",
    ]
    return "\n".join(lines)


# CSV: save separately
for name, spec_list in [("eq1", specs1), ("eq2", specs2)]:
    rows = []
    for label, res in spec_list:
        for var in VAR_ORDER:
            if var in res.params.index:
                rows.append({"spec": label, "variable": var,
                             "coef": fmt(res.params[var]),
                             "se":   f"({fmt(res.std_errors[var])})",
                             "sig":  stars(res.pvalues[var]),
                             "N":    f"{int(res.nobs):,}"})
    pd.DataFrame(rows).to_csv(os.path.join(OUT_DIR, f"tables/contagion_{name}.csv"), index=False)
    print(f"Saved tables/contagion_{name}.csv")

# LaTeX: save separately
tex1 = build_latex(specs1, ["(1a)", "(1b)", "(1c)", "(1d)"],
                   "Contagion Test: Extensive Margin (Sale Dummy)",
                   "tab:contagion_eq1", "Sale$_{f,i,t}$")
tex2 = build_latex(specs2, ["(2a)", "(2b)", "(2c)", "(2d)"],
                   r"Contagion Test: Intensive Margin ($-\Delta$Paramt)",
                   "tab:contagion_eq2", r"$-\Delta$Paramt$_{f,i,t}$")

for name, tex in [("eq1", tex1), ("eq2", tex2)]:
    tex_path = os.path.join(OUT_DIR, f"tables/contagion_{name}.tex")
    with open(tex_path, "w") as f:
        f.write(tex)
    print(f"Saved {tex_path}")
