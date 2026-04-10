import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from databricks import sql
from dotenv import load_dotenv

from util.helpers import to_opt_series, to_opt, human, fmt_int

st.set_page_config(page_title="Optio Insights", layout="wide")
load_dotenv()


# --------- Databricks config ---------

def get_db_config():
    if "databricks" in st.secrets:
        return st.secrets["databricks"]
    return {
        "server_hostname": os.getenv("DATABRICKS_SERVER_HOSTNAME", ""),
        "http_path": os.getenv("DATABRICKS_HTTP_PATH", ""),
        "access_token": os.getenv("DATABRICKS_TOKEN", ""),
    }


def validate_db_config(DB):
    host = DB.get("server_hostname", "")
    http_path = DB.get("http_path", "")
    token = DB.get("access_token", "")

    if not host or not http_path or not token:
        st.write("Has databricks secrets:", "databricks" in st.secrets)
        st.write("Hostname present:", bool(DB.get("server_hostname")))
        st.write("HTTP path present:", bool(DB.get("http_path")))
        st.write("Token present:", bool(DB.get("access_token")))
        return False, "Missing one or more of server_hostname/http_path/access_token."
    if host.startswith("http://") or host.startswith("https://"):
        return False, "server_hostname must NOT include http(s):// (use only the hostname)."
    if not http_path.startswith("/sql/"):
        return False, "http_path must start with /sql/..."
    return True, ""


DB = get_db_config()
ok, msg = validate_db_config(DB)
if not ok:
    st.error(f"Databricks config invalid: {msg}")
    st.stop()


# --------- Query helper ---------

@st.cache_data(ttl=3600)
def query_df(q: str, params=None) -> pd.DataFrame:
    params = params or {}
    params = dict(sorted(params.items()))
    try:
        with sql.connect(
            server_hostname=DB["server_hostname"],
            http_path=DB["http_path"],
            access_token=DB["access_token"],
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(q, parameters=params)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        raise RuntimeError(f"Databricks query failed. Details: {e}") from e


# ------------------- cached data loaders -----------------

@st.cache_data(ttl=3600)
def get_dt_options() -> list[str]:
    df = query_df(
        """
        SELECT dt
        FROM optio_warehouse.gold.gold_supply_stake_lock_daily
        ORDER BY dt DESC
        LIMIT 365
        """
    )
    return df["dt"].tolist()


@st.cache_data(ttl=3600)
def get_supply_df(dt: str) -> pd.DataFrame:
    return query_df(
        """
        SELECT
          total_supply_uopt,
          staked_uopt,
          locked_uopt,
          staked_unlocked_uopt,
          liquid_est_uopt,
          supply_dt_used,
          circulating_supply_uopt,
          api_total_supply_uopt
        FROM optio_warehouse.gold.gold_supply_stake_lock_daily
        WHERE dt = :dt
        """,
        {"dt": dt},
    )


@st.cache_data(ttl=3600)
def get_unlock_buckets_df(dt: str) -> pd.DataFrame:
    return query_df(
        """
        SELECT unlock_bucket, locked_uopt, lock_count, wallet_count
        FROM optio_warehouse.gold.gold_unlock_buckets_daily
        WHERE dt = :dt
        """,
        {"dt": dt},
    )


@st.cache_data(ttl=3600)
def get_unlock_calendar_df(dt: str) -> pd.DataFrame:
    return query_df(
        """
        SELECT unlock_date, days_to_unlock, unlocking_uopt, wallet_count, lock_count
        FROM optio_warehouse.gold.gold_lock_calendar_daily
        WHERE dt = :dt
        ORDER BY unlock_date ASC
        """,
        {"dt": dt},
    )


@st.cache_data(ttl=3600)
def get_top_unlock_dates_df(dt: str) -> pd.DataFrame:
    return query_df(
        """
        SELECT unlock_date, days_to_unlock, unlocking_uopt, wallet_count, lock_count
        FROM optio_warehouse.gold.gold_lock_calendar_daily
        WHERE dt = :dt
        ORDER BY unlocking_uopt DESC, unlock_date ASC
        LIMIT 30
        """,
        {"dt": dt},
    )


@st.cache_data(ttl=3600)
def get_holder_distribution_df(dt: str) -> pd.DataFrame:
    return query_df(
        """
        SELECT holding_bucket, wallet_count, total_locked_uopt
        FROM optio_warehouse.gold.gold_locked_holder_distribution_daily
        WHERE dt = :dt
        """,
        {"dt": dt},
    )


@st.cache_data(ttl=3600)
def get_unlock_drift_df_for_dates(selected_dts: tuple[str, ...]) -> pd.DataFrame:
    if not selected_dts:
        return pd.DataFrame(columns=["dt", "unlock_week", "unlocking_uopt"])

    in_clause = ", ".join([f"'{dt}'" for dt in selected_dts])

    return query_df(f"""
        SELECT
            dt,
            DATE_TRUNC('week', unlock_date) AS unlock_week,
            SUM(unlocking_uopt) AS unlocking_uopt
        FROM optio_warehouse.gold.gold_lock_calendar_daily
        WHERE dt IN ({in_clause})
        GROUP BY dt, DATE_TRUNC('week', unlock_date)
        ORDER BY dt ASC, unlock_week ASC
    """)

@st.cache_data(ttl=3600)
def get_available_snapshot_dates() -> pd.DataFrame:
    return query_df("""
        SELECT DISTINCT dt
        FROM optio_warehouse.gold.gold_lock_calendar_daily
        ORDER BY dt ASC
    """)

# --------- Shared chart settings ---------

COMPONENT_ORDER = [
    "Staked & locked",
    "Staked (unlocked)",
    "Liquid (circulating)",
    "Non-circulating (API gap)",
]

COLOR_MAP = {
    "Liquid (circulating)": "#93C5FD",
    "Staked (unlocked)": "#3B82F6",
    "Staked & locked": "#1E3A8A",
    "Non-circulating (API gap)": "#64748B",
}

PLOTLY_BLUE = "#3B82F6"
PLOTLY_DARK = "#1E3A8A"
PLOTLY_LIGHT = "#93C5FD"
PLOTLY_SLATE = "#64748B"

BUCKET_ORDER = ["<1W", "<1M", "<6M", "<12M", "<18M", "<24M", "24M"]
HOLDER_ORDER = ["<10k", "10k-100k", "100k-1M", "1M-10M", "10M-50M", "50M-100M", "100M+"]
DRIFT_LINE_COLORS = ["#3B82F6", "#60A5FA", "#93C5FD", "#BFDBFE", "#DBEAFE", "#E0F2FE", "#EFF6FF"]

WEEKDAY_BASIS = 2  # Monday=0, Tuesday=1, Wednesday=2, ...
MAX_HISTORICAL_SNAPSHOTS = 10
WEEKDAY_LABEL = "Wednesdays"


def base_layout(title=None):
    return dict(
        title=title,
        margin=dict(l=10, r=10, t=45, b=10),
        legend_title_text="",
        xaxis_title=None,
        yaxis_title=None,
    )


# --------- UI header ---------

st.title("Optio Daily Insights")

_, top_right = st.columns([3, 1], vertical_alignment="bottom")
with top_right:
    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()


dt_options = get_dt_options()
if not dt_options:
    st.error("No dt values found in gold_supply_stake_lock_daily.")
    st.stop()

dt = dt_options[0]
st.caption(f"Showing latest available daily snapshot: {dt}. Values displayed in OPT.")

# --------- get available snapshot dates and determine historical snapshots to show in unlock drift comparison ---------
available_dt_df = get_available_snapshot_dates()
available_dts = sorted(pd.to_datetime(available_dt_df["dt"]).dt.normalize().unique())

latest_dt_ts = pd.Timestamp(max(available_dts))
latest_dt = latest_dt_ts.strftime("%Y-%m-%d")

historical_snapshot_dts = [
    pd.Timestamp(d).strftime("%Y-%m-%d")
    for d in reversed(available_dts)
    if pd.Timestamp(d).weekday() == WEEKDAY_BASIS and pd.Timestamp(d) < latest_dt_ts
][:MAX_HISTORICAL_SNAPSHOTS]

historical_snapshot_dts = list(reversed(historical_snapshot_dts))
selected_dts = tuple([latest_dt] + historical_snapshot_dts)


# --------- Supply KPIs + percent breakdown ---------

try:
    supply = get_supply_df(dt)
except Exception as e:
    st.error(str(e))
    st.stop()

if supply.empty:
    st.error(f"No supply row found for dt={dt}")
    st.stop()

r = supply.iloc[0]

chain_total_opt = float(to_opt(r["total_supply_uopt"]))
circ_opt = float(to_opt(r["circulating_supply_uopt"]))
staked_opt = float(to_opt(r["staked_uopt"]))
locked_opt = float(to_opt(r["locked_uopt"]))
api_total_opt = to_opt(r["api_total_supply_uopt"])
supply_dt_used = str(r["supply_dt_used"])

if locked_opt > staked_opt + 1e-9:
    st.warning("Data sanity: locked > staked (should not happen).")

non_circ_opt = max(chain_total_opt - circ_opt, 0.0)
staked_unlocked_opt = max(staked_opt - locked_opt, 0.0)
liquid_circ_opt = circ_opt - staked_opt
if liquid_circ_opt < -1e-6:
    st.warning("Circulating supply (API) is lower than staked (chain). Clamping circulating-liquid to 0 for chart.")
liquid_circ_opt = max(liquid_circ_opt, 0.0)

donut_df = pd.DataFrame(
    {
        "Component": COMPONENT_ORDER,
        "OPT": [locked_opt, staked_unlocked_opt, liquid_circ_opt, non_circ_opt],
    }
)
donut_df["Percent"] = donut_df["OPT"] / chain_total_opt * 100 if chain_total_opt else 0.0
donut_df["OPT_label"] = donut_df["OPT"].map(human)
donut_df["Percent_label"] = donut_df["Percent"].map(lambda x: f"{x:.2f}%")

donut_sum = donut_df["OPT"].sum()
if abs(donut_sum - chain_total_opt) > 1.0:
    st.warning(f"Sanity: donut sums to {donut_sum:,.2f} OPT but chain total is {chain_total_opt:,.2f} OPT")

k1, k2, k3, k4 = st.columns(4)
k1.metric("Chain total supply", human(chain_total_opt))
k2.metric("API circulating supply", human(circ_opt))
k3.metric("Total staked", human(staked_opt))
k4.metric("Total locked", human(locked_opt))

row1_left, row1_right = st.columns([2, 1], vertical_alignment="top")

with row1_left:
    fig = px.pie(
        donut_df,
        values="OPT",
        names="Component",
        hole=0.55,
        category_orders={"Component": COMPONENT_ORDER},
        color="Component",
        color_discrete_map=COLOR_MAP,
    )

    fig.update_traces(
        textinfo="percent+label",
        textposition="inside",
        hovertemplate="<b>%{label}</b><br>%{value:,.0f} OPT<br>%{percent}<extra></extra>",
    )
    fig.update_layout(**base_layout())
    fig.add_annotation(
        text=f"{human(chain_total_opt)}<br><span style='font-size:12px'>Chain total</span>",
        x=0.5,
        y=0.5,
        showarrow=False,
        font=dict(size=16),
    )

    st.plotly_chart(fig, width='stretch')
    st.caption(
        f"Donut is normalized to chain total supply. Circulating supply is from Optio API (as of {supply_dt_used}). "
        f"Non-circulating = chain total - API circulating."
    )

with row1_right:
    show = donut_df[["Component", "Percent_label", "OPT_label"]].rename(
        columns={"Percent_label": "% of Supply", "OPT_label": "Amount (OPT)"}
    )
    st.dataframe(show, width='stretch', hide_index=True)

st.divider()


# --------- Row 2: Unlock buckets + holder distribution ---------

row2_left, row2_right = st.columns(2, vertical_alignment="center")

with row2_left:
    st.subheader("Unlock Buckets")
    try:
        buckets = get_unlock_buckets_df(dt)
    except Exception as e:
        st.error(str(e))
        st.stop()

    if buckets.empty:
        st.warning("No unlock bucket data for this dt.")
    else:
        buckets["unlock_bucket"] = pd.Categorical(
            buckets["unlock_bucket"], categories=BUCKET_ORDER, ordered=True
        )
        buckets["locked_opt"] = to_opt_series(buckets["locked_uopt"])
        buckets = buckets.sort_values("unlock_bucket")

        fig = px.bar(
            buckets,
            x="unlock_bucket",
            y="locked_opt",
            category_orders={"unlock_bucket": BUCKET_ORDER},
            color_discrete_sequence=[PLOTLY_DARK],
        )
        fig.update_traces(
            customdata=buckets[["wallet_count", "lock_count"]].to_numpy(),
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Locked: %{y:,.0f} OPT<br>"
                "Wallets: %{customdata[0]:,.0f}<br>"
                "Locks: %{customdata[1]:,.0f}"
                "<extra></extra>"
            ),
        )
        fig.update_layout(**base_layout("Total locked OPT by time-to-unlock bucket"))
        st.plotly_chart(fig, width='stretch')

        show = buckets[["unlock_bucket", "locked_opt", "wallet_count", "lock_count"]].copy()
        show["locked_OPT"] = show["locked_opt"].map(human)
        show["wallet_count"] = show["wallet_count"].map(fmt_int)
        show["lock_count"] = show["lock_count"].map(fmt_int)
        show = show.drop(columns=["locked_opt"]).rename(columns={"unlock_bucket": "bucket"})
        st.dataframe(show, width='stretch', hide_index=True)

with row2_right:
    st.subheader("Locked Holder Distribution")
    try:
        dist = get_holder_distribution_df(dt)
    except Exception as e:
        st.error(str(e))
        st.stop()

    if dist.empty:
        st.warning("No holder distribution data for this dt.")
    else:
        dist["holding_bucket"] = pd.Categorical(
            dist["holding_bucket"], categories=HOLDER_ORDER, ordered=True
        )
        dist["total_locked_opt"] = to_opt_series(dist["total_locked_uopt"])
        dist = dist.sort_values("holding_bucket")

        metric = st.radio(
            "Holder distribution metric",
            ["Wallet count", "Locked OPT"],
            horizontal=True,
            key="holder_dist_metric",
        )

        if metric == "Wallet count":
            y_col = "wallet_count"
            chart_title = "Wallet count by locked-OPT bucket"
            color = PLOTLY_LIGHT
            customdata = dist[["total_locked_opt"]].to_numpy()
            hovertemplate = (
                "<b>%{x}</b><br>"
                "Wallets: %{y:,.0f}<br>"
                "Locked: %{customdata[0]:,.0f} OPT"
                "<extra></extra>"
            )
        else:
            y_col = "total_locked_opt"
            chart_title = "Total locked OPT by bucket"
            color = PLOTLY_BLUE
            customdata = dist[["wallet_count"]].to_numpy()
            hovertemplate = (
                "<b>%{x}</b><br>"
                "Locked: %{y:,.0f} OPT<br>"
                "Wallets: %{customdata[0]:,.0f}"
                "<extra></extra>"
            )

        fig = px.bar(
            dist,
            x="holding_bucket",
            y=y_col,
            category_orders={"holding_bucket": HOLDER_ORDER},
            color_discrete_sequence=[color],
        )
        fig.update_traces(customdata=customdata, hovertemplate=hovertemplate)
        fig.update_layout(**base_layout(chart_title))
        st.plotly_chart(fig, width='stretch')

        show = dist[["holding_bucket", "wallet_count", "total_locked_opt"]].copy()
        show["wallet_count"] = show["wallet_count"].map(fmt_int)
        show["total_locked_OPT"] = show["total_locked_opt"].map(human)
        show = show.drop(columns=["total_locked_opt"]).rename(columns={"holding_bucket": "bucket"})
        st.dataframe(show, width='stretch', hide_index=True)

st.divider()


# --------- Unlock calendar ---------

st.subheader("Unlock Calendar")

calendar_metric = st.radio(
    "Calendar granularity",
    ["Weekly", "Daily"],
    horizontal=True,
    key="unlock_calendar_granularity",
)

try:
    cal = get_unlock_calendar_df(dt)
    top_unlocks = get_top_unlock_dates_df(dt)
except Exception as e:
    st.error(str(e))
    st.stop()

if cal.empty:
    st.warning("No unlock calendar data for this dt.")
else:
    cal["unlock_date"] = pd.to_datetime(cal["unlock_date"])
    cal["unlocking_opt"] = to_opt_series(cal["unlocking_uopt"])
    cal = cal.sort_values("unlock_date")

    if calendar_metric == "Weekly":
        cal_plot = (
            cal.assign(week_start=cal["unlock_date"].dt.to_period("W").dt.start_time)
            .groupby("week_start", as_index=False)
            .agg(
                unlocking_opt=("unlocking_opt", "sum"),
                wallet_count=("wallet_count", "sum"),
                lock_count=("lock_count", "sum"),
            )
            .rename(columns={"week_start": "plot_date"})
        )
        chart_title = "Unlocking OPT over time (weekly)"
    else:
        cal_plot = cal[["unlock_date", "unlocking_opt", "wallet_count", "lock_count"]].copy()
        cal_plot = cal_plot.rename(columns={"unlock_date": "plot_date"})
        chart_title = "Unlocking OPT over time (daily)"

    fig = px.bar(
        cal_plot,
        x="plot_date",
        y="unlocking_opt",
        color_discrete_sequence=[PLOTLY_BLUE],
    )
    fig.update_traces(
        customdata=cal_plot[["wallet_count", "lock_count"]].to_numpy(),
        hovertemplate=(
            "<b>%{x|%Y-%m-%d}</b><br>"
            "Unlocking: %{y:,.0f} OPT<br>"
            "Wallets: %{customdata[0]:,.0f}<br>"
            "Locks: %{customdata[1]:,.0f}"
            "<extra></extra>"
        ),
    )
    fig.update_layout(**base_layout(chart_title))
    st.plotly_chart(fig, use_container_width=True)

st.divider()


# --------- Definitions & methodology ---------

st.subheader("Definitions & Methodology")

with st.expander("How to read supply metrics", expanded=False):
    st.markdown(
        """
        - **Chain total supply**: minted supply from on-chain state.
        - **Circulating supply**: supply reported by the Optio API.
        - **Locked**: OPT currently in active lockups.
        - **Staked**: OPT delegated in staking pools.
        - **Locked is a subset of staked**: locked OPT is included within staked OPT, so these should not be added together independently.
        - **Staked (unlocked)**: staked OPT that is not currently locked.
        - **Liquid (circulating)**: API circulating supply after separating out staked balances for the supply visualization.
        - **Non-circulating (API gap)**: chain total supply minus API circulating supply.
        """
    )

with st.expander("Data sources", expanded=False):
    st.markdown(
        """
        - **Blockchain data**: Staking pools, lockups, unlock schedules, and wallet distributions are derived directly from Optio on-chain state. These values represent the actual balances and lock parameters recorded on the blockchain.
        - **Official Optio API**: Circulating supply and total supply metrics are sourced from the official Optio statistics API. These values reflect the supply figures publicly reported by the Optio ecosystem.
        - **Daily snapshot**: All values shown on this dashboard represent a daily snapshot of chain state for the latest available date.
        - **Supply definitions**: Some charts combine blockchain-derived values with API-reported supply metrics. When both are used together, the difference between chain total supply and API circulating supply is shown explicitly rather than hidden.
        """
    )
