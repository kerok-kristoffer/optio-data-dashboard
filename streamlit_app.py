import os
import streamlit as st
import pandas as pd
from databricks import sql

from util.helpers import UOPT_PER_OPT, to_opt_series, to_opt

st.set_page_config(page_title="Optio Insights", layout="wide")

from dotenv import load_dotenv
load_dotenv()


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


DB = get_db_config()
if not all([DB["server_hostname"], DB["http_path"], DB["access_token"]]):
    st.error("Missing Databricks connection config. Set Streamlit secrets or local .env vars.")
    st.stop()


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

    if not token.startswith("dapi"):
        return False, "access_token should look like a Databricks personal access token (starts with dapi...)."

    return True, ""

ok, msg = validate_db_config(DB)
if not ok:
    st.error(f"Databricks config invalid: {msg}")
    st.stop()

@st.cache_data(ttl=60)
def query_df(q: str, params=None) -> pd.DataFrame:
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



st.title("Optio Daily Insights (MVP)")

# Pick a dt from available gold rows
dt_options = query_df("""
    SELECT dt
    FROM optio_warehouse.gold.gold_supply_stake_lock_daily
    ORDER BY dt DESC
    LIMIT 180
""")["dt"].tolist()

dt = st.selectbox("Select dt", dt_options)

st.caption(f"Showing Gold-layer aggregates for dt = {dt}. Values are in OPT.")
# --- Supply / stake / lock KPIs ---
supply = query_df("""
SELECT
  total_supply_uopt,
  staked_uopt,
  locked_uopt,
  liquid_est_uopt
FROM optio_warehouse.gold.gold_supply_stake_lock_daily
WHERE dt = :dt
""", {"dt": dt})

if supply.empty:
    st.error(f"No supply row found for dt={dt}")
    st.stop()

r = supply.iloc[0]
total_opt = to_opt(r["total_supply_uopt"])
staked_opt = to_opt(r["staked_uopt"])
locked_opt = to_opt(r["locked_uopt"])
liquid_opt = to_opt(r["liquid_est_uopt"])

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Supply (OPT)", f"{total_opt:,.0f}")
c2.metric("Staked (OPT)", f"{staked_opt:,.0f}")
c3.metric("Locked (OPT)", f"{locked_opt:,.0f}")
c4.metric("Liquid est. (OPT)", f"{liquid_opt:,.0f}")

# Supply composition (simple, clear)
st.caption("Supply composition (single-day snapshot)")
composition = pd.DataFrame({
    "Component": ["Staked", "Locked", "Liquid est."],
    "OPT": [staked_opt, locked_opt, liquid_opt]
})
st.bar_chart(composition.set_index("Component")["OPT"])


st.divider()

# --- Unlock buckets ---
st.subheader("Unlock Buckets")

buckets = query_df("""
SELECT unlock_bucket, locked_uopt, lock_count, wallet_count
FROM optio_warehouse.gold.gold_unlock_buckets_daily
WHERE dt = :dt
""", {"dt": dt})

if buckets.empty:
    st.warning("No unlock bucket data for this dt.")
else:
    BUCKET_ORDER = ["1W", "1M", "6M", "12M", "18M", "24M", "24M+"]

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
        st.caption("Counts per bucket")
        show = buckets[["unlock_bucket", "locked_opt", "wallet_count", "lock_count"]].copy()
        show.rename(columns={"locked_opt": "locked_OPT"}, inplace=True)
        st.dataframe(show, use_container_width=True, hide_index=True)


st.divider()

# --- Unlock calendar ---
st.subheader("Unlock Calendar")

cal = query_df("""
SELECT unlock_date, days_to_unlock, unlocking_uopt, wallet_count, lock_count
FROM optio_warehouse.gold.gold_lock_calendar_daily
WHERE dt = :dt
ORDER BY unlock_date ASC
LIMIT 2000
""", {"dt": dt})

if cal.empty:
    st.warning("No unlock calendar data for this dt.")
else:
    cal["unlock_date"] = pd.to_datetime(cal["unlock_date"])
    cal["unlocking_opt"] = to_opt_series(cal["unlocking_uopt"])
    cal = cal.sort_values("unlock_date")

    st.caption("Unlocking OPT over time (by unlock date)")
    st.line_chart(cal.set_index("unlock_date")["unlocking_opt"])

    st.caption("Top upcoming unlock dates (largest first)")
    top = cal.sort_values("unlocking_opt", ascending=False).head(30).copy()
    top = top[["unlock_date", "days_to_unlock", "unlocking_opt", "wallet_count", "lock_count"]]
    top.rename(columns={"unlocking_opt": "unlocking_OPT"}, inplace=True)
    st.dataframe(top, use_container_width=True, hide_index=True)


st.divider()

# --- Holder distribution ---
st.subheader("Locked Holder Distribution")

dist = query_df("""
SELECT holding_bucket, wallet_count, total_locked_uopt
FROM optio_warehouse.gold.gold_locked_holder_distribution_daily
WHERE dt = :dt
""", {"dt": dt})

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

    st.caption("Table")
    show = dist[["holding_bucket", "wallet_count", "total_locked_opt"]].copy()
    show.rename(columns={"total_locked_opt": "total_locked_OPT"}, inplace=True)
    st.dataframe(show, use_container_width=True, hide_index=True)


if st.button("Refresh data"):
    st.cache_data.clear()