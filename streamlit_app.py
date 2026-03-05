import os
import streamlit as st
import pandas as pd
from databricks import sql

from util.helpers import to_opt_series, to_opt, human, fmt_int

from dotenv import load_dotenv

st.set_page_config(page_title="Optio Insights", layout="wide")
load_dotenv()


# --------- Databricks config ---------

def get_db_config():
    # Prefer Streamlit secrets when deployed
    if "databricks" in st.secrets:
        return st.secrets["databricks"]
    # Fallback to local env vars
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
        return False, "Missing one or more of server_hostname/http_path/access_token."
    if host.startswith("http://") or host.startswith("https://"):
        return False, "server_hostname must NOT include http(s):// (use only the hostname)."
    if not http_path.startswith("/sql/"):
        return False, "http_path must start with /sql/..."
    # Token check is advisory; service principal tokens can vary, but PATs start with dapi
    return True, ""

DB = get_db_config()
ok, msg = validate_db_config(DB)
if not ok:
    st.error(f"Databricks config invalid: {msg}")
    st.stop()

# --------- Query helper ---------

@st.cache_data(ttl=3600)  # cache for 60 minutes
def query_df(q: str, params=None) -> pd.DataFrame:
    params = params or {}
    # normalize params so caching keys are stable
    params = dict(sorted(params.items()))
    try:
        with sql.connect(
            server_hostname=DB["server_hostname"],
            http_path=DB["http_path"],
            access_token=DB["access_token"],
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(q, params or {})
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        # If your local DNS flakes, this will show cleanly instead of hanging forever.
        raise RuntimeError(f"Databricks query failed (possible local DNS/network). Details: {e}") from e


# ------------------- dt options -----------------

@st.cache_data(ttl=3600)
def get_dt_options() -> list[str]:
    df = query_df("""
        SELECT dt
        FROM optio_warehouse.gold.gold_supply_stake_lock_daily
        ORDER BY dt DESC
        LIMIT 365
    """)
    return df["dt"].tolist()
# --------- UI header ---------

st.title("Optio Daily Insights (MVP)")

top_left, top_right = st.columns([3, 1], vertical_alignment="bottom")
with top_right:
    if st.button("Refresh data"):
        st.cache_data.clear()
        st.session_state.loaded_dt = None
        st.rerun()

# Available dates

dt_options = get_dt_options()
if not dt_options:
    st.error("No dt values found in gold_supply_stake_lock_daily.")
    st.stop()

dt = st.selectbox("Select dt", dt_options)
st.caption(f"Showing Gold-layer aggregates for dt = {dt}. Values displayed in OPT.")

if "loaded_dt" not in st.session_state:
    st.session_state.loaded_dt = None

load_clicked = st.button("Load", type="primary")

# Auto-load on first page view (optional). If you want strict manual load, remove this block.
if st.session_state.loaded_dt is None and dt_options:
    st.session_state.loaded_dt = dt  # load default once

if load_clicked:
    st.session_state.loaded_dt = dt

if st.session_state.loaded_dt is None:
    st.stop()

if st.session_state.loaded_dt != dt:
    st.info("Select a date and click **Load** to refresh the dashboard.")

dt = st.session_state.loaded_dt

# --------- Supply KPIs + percent breakdown ---------

try:
    supply = query_df("""
        SELECT
          total_supply_uopt,
          staked_uopt,
          locked_uopt,
          liquid_est_uopt
        FROM optio_warehouse.gold.gold_supply_stake_lock_daily
        WHERE dt = :dt
    """, {"dt": dt})
except Exception as e:
    st.error(str(e))
    st.stop()

if supply.empty:
    st.error(f"No supply row found for dt={dt}")
    st.stop()

r = supply.iloc[0]
total_opt = to_opt(r["total_supply_uopt"])
staked_opt = to_opt(r["staked_uopt"])
locked_opt = to_opt(r["locked_uopt"])
liquid_opt = to_opt(r["liquid_est_uopt"])

import plotly.express as px

total_opt = float(total_opt)
staked_opt = float(staked_opt)
locked_opt = float(locked_opt)

if locked_opt > staked_opt + 1e-9:
    st.warning("Data sanity: locked > staked (should not happen).")
if staked_opt > total_opt + 1e-9:
    st.warning("Data sanity: staked > total (should not happen).")

staked_unlocked_opt = max(staked_opt - locked_opt, 0.0)
liquid_opt = max(total_opt - staked_opt, 0.0)

donut_df = pd.DataFrame({
    "Component": ["Staked & locked", "Staked (unlocked)", "Liquid (not staked)"],
    "OPT": [locked_opt, staked_unlocked_opt, liquid_opt],
})
donut_df["Percent"] = (donut_df["OPT"] / total_opt * 100) if total_opt else 0.0
donut_df["OPT_label"] = donut_df["OPT"].map(human)
donut_df["Percent_label"] = donut_df["Percent"].map(lambda x: f"{x:.2f}%")

left, right = st.columns([2, 1], vertical_alignment="top")

with left:
    # Keep a consistent ordering everywhere
    COMPONENT_ORDER = ["Staked & locked", "Staked (unlocked)", "Liquid (not staked)"]

    # Unified Optio-ish blues (light -> dark)
    COLOR_MAP = {
        "Liquid (not staked)": "#93C5FD",  # light blue
        "Staked (unlocked)": "#3B82F6",  # mid blue
        "Staked & locked": "#1E3A8A",  # dark blue
    }

    donut_df["Component"] = pd.Categorical(
        donut_df["Component"], categories=COMPONENT_ORDER, ordered=True
    )
    donut_df = donut_df.sort_values("Component")

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

    fig.update_layout(
        margin=dict(l=10, r=10, t=30, b=10),
        legend_title_text="",
    )
    # Center label (optional but nice)
    fig.add_annotation(
        text=f"{human(total_opt)}<br><span style='font-size:12px'>Total OPT</span>",
        x=0.5, y=0.5, showarrow=False, font=dict(size=16)
    )

    st.plotly_chart(fig, use_container_width=True)

with right:
    show = donut_df[["Component", "Percent_label", "OPT_label"]].rename(
        columns={"Percent_label": "% of Supply", "OPT_label": "Amount (OPT)"}
    )
    st.dataframe(show, use_container_width=True, hide_index=True)
st.divider()

# --------- Unlock buckets ---------

st.subheader("Unlock Buckets")

try:
    buckets = query_df("""
        SELECT unlock_bucket, locked_uopt, lock_count, wallet_count
        FROM optio_warehouse.gold.gold_unlock_buckets_daily
        WHERE dt = :dt
    """, {"dt": dt})
except Exception as e:
    st.error(str(e))
    st.stop()

if buckets.empty:
    st.warning("No unlock bucket data for this dt.")
else:
    BUCKET_ORDER = ["<1W", "<1M", "<6M", "<12M", "<18M", "<24M", "24M"]

    buckets["unlock_bucket"] = pd.Categorical(
        buckets["unlock_bucket"], categories=BUCKET_ORDER, ordered=True
    )
    buckets["locked_opt"] = to_opt_series(buckets["locked_uopt"])
    buckets = buckets.sort_values("unlock_bucket")

    left, right = st.columns([2, 1], vertical_alignment="top")
    with left:
        st.caption("Total locked OPT by time-to-unlock bucket")
        st.bar_chart(buckets.set_index("unlock_bucket")["locked_opt"])

    with right:
        st.caption("Bucket details")
        show = buckets[["unlock_bucket", "locked_opt", "wallet_count", "lock_count"]].copy()
        show["locked_OPT"] = show["locked_opt"].map(human)
        show["wallet_count"] = show["wallet_count"].map(fmt_int)
        show["lock_count"] = show["lock_count"].map(fmt_int)
        show = show.drop(columns=["locked_opt"]).rename(columns={"unlock_bucket": "bucket"})
        st.dataframe(show, use_container_width=True, hide_index=True)

st.divider()

# --------- Unlock calendar ---------

st.subheader("Unlock Calendar")

try:
    cal = query_df("""
        SELECT unlock_date, days_to_unlock, unlocking_uopt, wallet_count, lock_count
        FROM optio_warehouse.gold.gold_lock_calendar_daily
        WHERE dt = :dt
        ORDER BY unlock_date ASC
        LIMIT 2000
    """, {"dt": dt})
except Exception as e:
    st.error(str(e))
    st.stop()

if cal.empty:
    st.warning("No unlock calendar data for this dt.")
else:
    cal["unlock_date"] = pd.to_datetime(cal["unlock_date"])
    cal["unlocking_opt"] = to_opt_series(cal["unlocking_uopt"])
    cal = cal.sort_values("unlock_date")

    st.caption("Unlocking OPT over time (by unlock date)")
    st.line_chart(cal.set_index("unlock_date")["unlocking_opt"])

    st.caption("Largest upcoming unlock dates (top 30)")
    top = cal.sort_values("unlocking_opt", ascending=False).head(30).copy()
    top["unlocking_OPT"] = top["unlocking_opt"].map(human)
    top["wallet_count"] = top["wallet_count"].map(fmt_int)
    top["lock_count"] = top["lock_count"].map(fmt_int)
    top = top[["unlock_date", "days_to_unlock", "unlocking_OPT", "wallet_count", "lock_count"]]
    st.dataframe(top, use_container_width=True, hide_index=True)

st.divider()

# --------- Holder distribution ---------

st.subheader("Locked Holder Distribution")

try:
    dist = query_df("""
        SELECT holding_bucket, wallet_count, total_locked_uopt
        FROM optio_warehouse.gold.gold_locked_holder_distribution_daily
        WHERE dt = :dt
    """, {"dt": dt})
except Exception as e:
    st.error(str(e))
    st.stop()

if dist.empty:
    st.warning("No holder distribution data for this dt.")
else:
    HOLDER_ORDER = ["<10k", "10k-100k", "100k-1M", "1M-10M", "10M-50M", "50M-100M", "100M+"]

    dist["holding_bucket"] = pd.Categorical(
        dist["holding_bucket"], categories=HOLDER_ORDER, ordered=True
    )
    dist["total_locked_opt"] = to_opt_series(dist["total_locked_uopt"])
    dist = dist.sort_values("holding_bucket")

    left, right = st.columns(2, vertical_alignment="top")
    with left:
        st.caption("Wallet count by locked-OPT bucket")
        st.bar_chart(dist.set_index("holding_bucket")["wallet_count"])

    with right:
        st.caption("Total locked OPT by bucket")
        st.bar_chart(dist.set_index("holding_bucket")["total_locked_opt"])

    st.caption("Bucket details")
    show = dist[["holding_bucket", "wallet_count", "total_locked_opt"]].copy()
    show["wallet_count"] = show["wallet_count"].map(fmt_int)
    show["total_locked_OPT"] = show["total_locked_opt"].map(human)
    show = show.drop(columns=["total_locked_opt"]).rename(columns={"holding_bucket": "bucket"})
    st.dataframe(show, use_container_width=True, hide_index=True)
