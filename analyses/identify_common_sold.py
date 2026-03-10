import pandas as pd
import pyreadr
import numpy as np
import os
import sys
import matplotlib.pyplot as plt

try:
    _root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
except NameError:
    _root = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, _root)
from utils.set_path import PROC_DIR, RAW_DIR, OUT_DIR, TEMP_DIR

PLOT_END = 202403   # last month to include in plot


""" Read in data"""
# Fund-bond-month level holdings
crsp = pyreadr.read_r(os.path.join(PROC_DIR, "crsp/mf_corp_holdings_m.rds"))[None]
print("Read CRSP monthly holdings:", crsp.shape)
print("Columns:", crsp.columns.tolist())
print("date_m_id range:", crsp["date_m_id"].min(), "-", crsp["date_m_id"].max())


""" Identify commonly sold bonds """
" Step 1: Compute change in paramt to identify sales "
# Subset to post-2007Q3 (paramt not reliable before 200710)
crsp = crsp[(crsp["date_m_id"] >= 200710)]
crsp = crsp.sort_values(["crsp_portno", "cusip8", "date_m_id"]).reset_index(drop=True)
print(f"After subsetting to [200710, 202409): {crsp.shape}")

# Lagged paramt within each fund-bond pair
crsp["paramt_lag"] = crsp.groupby(["crsp_portno", "cusip8"])["paramt"].shift(1)
crsp["date_m_id_lag"] = crsp.groupby(["crsp_portno", "cusip8"])["date_m_id"].shift(1)

# Build a YYYYMM -> prev-month YYYYMM lookup so we can check for consecutive months
def prev_month_id(date_m_id):
    year, month = divmod(date_m_id, 100)
    if month == 1:
        return (year - 1) * 100 + 12
    return year * 100 + (month - 1)

crsp["prev_date_m_id"] = crsp["date_m_id"].map(prev_month_id)

# Only treat lag as valid if it comes from the immediately preceding month
crsp["paramt_lag"] = np.where(
    crsp["date_m_id_lag"] == crsp["prev_date_m_id"],
    crsp["paramt_lag"],
    np.nan
)

# delta_paramt: negative means the fund reduced its holding
crsp["delta_paramt"] = crsp["paramt"] - crsp["paramt_lag"]
print(crsp['delta_paramt'].describe())

# A fund-bond-month is a "sale" if paramt declined vs last month
crsp["is_sale"] = (crsp["delta_paramt"] < 0) & crsp["delta_paramt"].notna()
print(crsp["is_sale"].value_counts())
print("\nSale observations:", crsp["is_sale"].sum(),
      f"({crsp['is_sale'].mean()*100:.1f}% of fund-bond-months with valid lag)")
# Sale observations: 14007699 (34.8% of fund-bond-months with valid lag)

# drop unused columns
crsp.drop(columns=["share_corporate", "date_m_id_lag", "paramt_lag", "prev_date_m_id", "w", "price_eom", "mkt_cap", "amt_outstanding"],
          inplace=True, errors="ignore")


" Step 2: Classify funds as outflow vs non-outflow "
# flow is percentage flow: (AUM - (1+ret)*AUM_lag) / AUM_lag
crsp["outflow_fund"] = crsp["flow"] < 0   # True = fund had net outflow this month
fund_month = crsp.drop_duplicates(["crsp_portno", "date_m_id"])
print("\nFund-month outflow share:", fund_month["outflow_fund"].mean().round(3))

# Time series: count of inflow and outflow funds per month
flow_ts = (
    fund_month.groupby("date_m_id")["outflow_fund"]
    .agg(n_outflow="sum", n_total="count")
    .reset_index()
)
flow_ts = flow_ts[flow_ts['date_m_id']<= PLOT_END]
flow_ts["n_inflow"] = flow_ts["n_total"] - flow_ts["n_outflow"]
flow_ts["date"] = pd.to_datetime(flow_ts["date_m_id"].astype(str), format="%Y%m")
flow_q = flow_ts.set_index("date").resample("QE")[["n_outflow", "n_inflow"]].mean()

sell_ts = (
    crsp[crsp["is_sale"]]
    .drop_duplicates(["crsp_portno", "date_m_id"])
    .groupby("date_m_id")["outflow_fund"]
    .agg(n_outflow_sell="sum", n_total_sell="count")
    .reset_index()
)
sell_ts["n_inflow_sell"] = sell_ts["n_total_sell"] - sell_ts["n_outflow_sell"]
sell_ts = sell_ts[sell_ts['date_m_id']<= PLOT_END]
sell_ts["date"] = pd.to_datetime(sell_ts["date_m_id"].astype(str), format="%Y%m")
sell_q = sell_ts.set_index("date").resample("QE")[["n_outflow_sell", "n_inflow_sell"]].mean()

# NBER recession periods (peak, trough)
RECESSIONS = [
    ("2007-12-01", "2009-06-30"),   # Great Recession
    ("2020-02-01", "2020-04-30"),   # COVID-19
]

def add_recessions(ax):
    for start, end in RECESSIONS:
        ax.axvspan(pd.Timestamp(start), pd.Timestamp(end),
                   color="grey", alpha=0.2, label="_nolegend_")

# Plot time series of number of funds
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 7), sharex=True)

add_recessions(ax1)
ax1.plot(flow_q.index, flow_q["n_outflow"], label="Outflow funds", color="firebrick")
ax1.plot(flow_q.index, flow_q["n_inflow"],  label="Inflow funds",  color="steelblue")
ax1.set_ylabel("Number of funds (quarterly avg)")
ax1.set_title("(a) All funds")
ax1.legend()

add_recessions(ax2)
ax2.plot(sell_q.index, sell_q["n_outflow_sell"], label="Outflow funds (sellers)", color="firebrick")
ax2.plot(sell_q.index, sell_q["n_inflow_sell"],  label="Inflow funds (sellers)",  color="steelblue")
ax2.set_xlabel("Quarter")
ax2.set_ylabel("Number of funds selling (quarterly avg)")
ax2.set_title("(b) Funds selling at least one bond")
ax2.legend()

fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "plots/ts_num_funds_q.pdf"), bbox_inches="tight")
plt.show()


" Step 3: Identify commonly sold bonds "
# Baseline: (p, r) = (75, 75) for outflow funds; (p', r') = (50, 50) for inflow funds
P_OUT, R_OUT = 75, 75
P_IN,  R_IN  = 50, 50

crsp["_inflow_fund"] = ~crsp["outflow_fund"]
crsp["_s_out"] = crsp["outflow_fund"] & crsp["is_sale"]
crsp["_s_in"]  = crsp["_inflow_fund"] & crsp["is_sale"]

bond_month_stats = (
    crsp.groupby(["cusip8", "date_m_id"], sort=False)
    .agg(H_out=("outflow_fund", "sum"), S_out=("_s_out", "sum"),
         H_in= ("_inflow_fund", "sum"), S_in= ("_s_in",  "sum"))
    .reset_index()
)

crsp.drop(columns=["_inflow_fund", "_s_out", "_s_in"], inplace=True)
bond_month_stats["share_out"] = bond_month_stats["S_out"] / bond_month_stats["H_out"]
bond_month_stats["share_in"]  = bond_month_stats["S_in"]  / bond_month_stats["H_in"]

# Distribution-based thresholds within each month:
#   q^g_{p,t}  = p-th percentile of Share^g_{i,t} across bonds i in month t
#   c^g_{r,t}  = r-th percentile of S^g_{i,t}     across bonds i in month t
def _monthly_pctile(df, col, pct, new_col):
    df[new_col] = df.groupby("date_m_id")[col].transform(lambda x: x.quantile(pct / 100))

_monthly_pctile(bond_month_stats, "share_out", P_OUT, "q_out")
_monthly_pctile(bond_month_stats, "S_out",     R_OUT, "c_out")
_monthly_pctile(bond_month_stats, "share_in",  P_IN,  "q_in")
_monthly_pctile(bond_month_stats, "S_in",      R_IN,  "c_in")

# OutflowSell_{i,t} = 1 if share_out >= q_out AND S_out >= c_out
# InflowPart_{i,t}  = 1 if share_in  >= q_in  AND S_in  >= c_in
outflow_sell = (bond_month_stats["share_out"] >= bond_month_stats["q_out"]) & \
               (bond_month_stats["S_out"]     >= bond_month_stats["c_out"])
inflow_part  = (bond_month_stats["share_in"]  >= bond_month_stats["q_in"])  & \
               (bond_month_stats["S_in"]      >= bond_month_stats["c_in"])

bond_month_stats["bond_cat"] = np.select(
    [outflow_sell & ~inflow_part, outflow_sell & inflow_part],
    ["fire_sold",                 "common_sold"],
    default="other"
)

# Summary
cat_counts = bond_month_stats["bond_cat"].value_counts()
total_bm   = len(bond_month_stats)
print(cat_counts)

# Time series of bond counts by category, aggregated to quarterly
CAT_ORDER  = ["fire_sold", "common_sold", "other"]
CAT_LABELS = ["Fire-sold", "Commonly sold", "Other"]
CAT_COLORS = ["firebrick", "darkorange", "grey"]

bond_month_stats["date"] = pd.to_datetime(bond_month_stats["date_m_id"].astype(str), format="%Y%m")
cat_ts = (
    bond_month_stats[bond_month_stats['date_m_id']<= PLOT_END].groupby(["date", "bond_cat"])
    .size()
    .unstack(fill_value=0)
    .reindex(columns=CAT_ORDER, fill_value=0)
)
cat_q = cat_ts.resample("QE").mean()

fig, ax = plt.subplots(figsize=(12, 4))
add_recessions(ax)
for col, label, color in zip(CAT_ORDER, CAT_LABELS, CAT_COLORS):
    ax.plot(cat_q.index, cat_q[col], label=label, color=color)
ax.set_xlabel("Quarter")
ax.set_ylabel("Number of bond-months (quarterly avg)")
ax.legend()
fig.tight_layout()
fig.savefig(os.path.join(OUT_DIR, "plots/ts_bond_cat_q.pdf"), bbox_inches="tight")
plt.show()


" Step 4: Funds trading on commonly sold bonds "
data = pd.merge(
    crsp,
    bond_month_stats[["cusip8", "date_m_id", "bond_cat", "share_out", "share_in",
                       "H_out", "S_out", "H_in", "S_in"]],
    on=["cusip8", "date_m_id"], how="left"
)

data["is_buy"] = (data["delta_paramt"] > 0) & data["delta_paramt"].notna()
data["is_trade"] = data["is_sale"] | data["is_buy"]

# Suppose 10 outflow funds and 10 inflow funds hold a bond
# 2 outflow funds sell → share_out = 20% 
# 1 inflow fund sells → share_in = 10% 
# → bond classified as common_sold
# But the other 8 inflow funds and 8 outflow funds are still in the data 
# some of them may be buying that bond (increasing their position)

# So n_buy_common captures funds that were on the buy side of a bond that 
# the market as a whole was commonly selling. 
# it identifies the liquidity providers (or contrarian buyers) for commonly sold bonds.
funds_on_common = (
    data[(data["bond_cat"] == "common_sold") & data["is_trade"]]
    .groupby(["crsp_portno", "date_m_id"])
    .agg(
        n_common_sold_bonds=("cusip8", "nunique"),
        n_sale_common=("is_sale", "sum"),
        n_buy_common=("is_buy", "sum"),
        outflow_fund=("outflow_fund", "first"),
        flow=("flow", "first"),
        aum=("aum", "first")
    )
    .reset_index()
)

# Sanity check: n_sale_common + n_buy_common should equal n_common_sold_bonds
funds_on_common["_check"] = funds_on_common["n_sale_common"] + funds_on_common["n_buy_common"]
mismatches = (funds_on_common["_check"] != funds_on_common["n_common_sold_bonds"]).sum()
print(f"\nCheck n_sale_common + n_buy_common == n_common_sold_bonds: "
      f"{mismatches} mismatches out of {len(funds_on_common):,} rows")
funds_on_common.drop(columns="_check", inplace=True)

# add fund group
funds_on_common["date"] = pd.to_datetime(funds_on_common["date_m_id"].astype(str), format="%Y%m")
funds_on_common["group"] = funds_on_common["outflow_fund"].map({True: "Outflow", False: "Inflow"})
print(funds_on_common["outflow_fund"].value_counts())


""" Plots: funds_on_common """
# ------------------------------------------------------------------ #
# (a) Time series of n_common_sold_bonds distribution by fund type  #
# ------------------------------------------------------------------ #
quantiles = {"p25": 0.25, "p50": 0.50, "p75": 0.75}

fig, ax = plt.subplots(figsize=(12, 4))
for grp, color in [("Outflow", "firebrick"), ("Inflow", "steelblue")]:
    sub = (
        funds_on_common[
            (funds_on_common["group"] == grp) &
            (funds_on_common["date_m_id"] <= 202403)
        ]
        .groupby("date")["n_common_sold_bonds"]
        .quantile(list(quantiles.values()))
        .unstack()
    )
    sub.columns = list(quantiles.keys())
    sub_q = sub.resample("QE").mean()

    ax.plot(sub_q.index, sub_q["p50"], color=color, label=f"{grp} (median)")
    ax.fill_between(sub_q.index, sub_q["p25"], sub_q["p75"],
                    alpha=0.25, color=color, label=f"{grp} IQR")

add_recessions(ax)
ax.set_xlabel("Quarter")
ax.set_ylabel("n_common_sold_bonds")
ax.set_title("(a) Distribution of commonly sold bonds traded per fund-month, by fund type")
ax.legend(fontsize=8)
fig.tight_layout()
#fig.savefig(os.path.join(OUT_DIR, "plots/ts_dist_n_common_sold_bonds.pdf"), bbox_inches="tight")
plt.show()


# ------------------------------------------------------------------ #
# (b) Time series: share of outflow funds among common-sold traders  #
# ------------------------------------------------------------------ #
trader_ts = (
    funds_on_common[funds_on_common["date_m_id"] <= 202403]
    .groupby("date")["outflow_fund"]
    .agg(n_outflow="sum", n_total="count")
    .resample("QE").sum()    # sum counts within quarter, then divide
)
trader_ts["share_outflow"] = trader_ts["n_outflow"] / trader_ts["n_total"]

fig, ax = plt.subplots(figsize=(12, 4))
add_recessions(ax)
ax.plot(trader_ts.index, trader_ts["share_outflow"], color="firebrick")
ax.axhline(0.5, color="black", linewidth=0.8, linestyle="--")
ax.set_xlabel("Quarter")
ax.set_ylabel("Share of outflow funds")
ax.set_title("(b) Share of outflow funds among funds trading commonly sold bonds")
fig.tight_layout()
#fig.savefig(os.path.join(OUT_DIR, "plots/ts_share_outflow_common_q.pdf"), bbox_inches="tight")
plt.show()

# ------------------------------------------------------------------ #
# (c) Box plot: n_sale_common distribution by flow group             #
# ------------------------------------------------------------------ #
fig, ax = plt.subplots(figsize=(6, 4))
groups = [
    funds_on_common.loc[funds_on_common["group"] == g, "n_sale_common"].clip(upper=20)
    for g in ["Outflow", "Inflow"]
]
ax.boxplot(groups, labels=["Outflow", "Inflow"], showfliers=False)
ax.set_ylabel("# commonly sold bonds sold (clipped at 20)")
ax.set_title("(c) Distribution of commonly sold bond sales per fund-month")
fig.tight_layout()
#fig.savefig(os.path.join(OUT_DIR, "plots/box_sale_common.pdf"), bbox_inches="tight")
plt.show()

print("Saved plots for funds_on_common.")


# ================================================================= #
# Save outputs                                                       #
# ================================================================= #
os.makedirs(os.path.join(PROC_DIR, "analyses"), exist_ok=True)

# 1. Full panel with bond_cat and trade flags
crsp.to_parquet(os.path.join(PROC_DIR, "analyses/mf_holdings_bond_cat.parquet"), index=False)
print("\nSaved mf_holdings_bond_cat.parquet")

# 2. Bond-month level statistics and classification
bond_month_stats.to_parquet(os.path.join(PROC_DIR, "analyses/bond_month_stats.parquet"), index=False)
print("Saved bond_month_stats.parquet")

# 3. Funds trading on commonly sold bonds
funds_on_common.to_parquet(os.path.join(PROC_DIR, "analyses/funds_on_common_sold.parquet"), index=False)
print("Saved funds_on_common_sold.parquet")
