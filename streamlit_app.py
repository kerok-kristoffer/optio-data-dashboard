import streamlit as st
import pandas as pd
from databricks import sql

st.set_page_config(page_title="Optio Insights", layout="wide")

DB = st.secrets["databricks"]

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
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Supply (uOPT)", f"{int(r['total_supply_uopt']):,}")
c2.metric("Staked (uOPT)", f"{int(r['staked_uopt']):,}")
c3.metric("Locked (uOPT)", f"{int(r['locked_uopt']):,}")
c4.metric("Liquid est. (uOPT)", f"{int(r['liquid_est_uopt']):,}")

st.divider()

# --- Unlock buckets ---
st.subheader("Unlock Buckets")
buckets = query_df("""
SELECT unlock_bucket, locked_uopt, lock_count, wallet_count
FROM optio_warehouse.gold.gold_unlock_buckets_daily
WHERE dt = :dt
""", {"dt": dt})

st.bar_chart(buckets.set_index("unlock_bucket")["locked_uopt"])
st.dataframe(buckets, use_container_width=True)

st.divider()

# --- Unlock calendar ---
st.subheader("Unlock Calendar")
cal = query_df("""
SELECT unlock_date, days_to_unlock, unlocking_uopt, wallet_count, lock_count
FROM optio_warehouse.gold.gold_lock_calendar_daily
WHERE dt = :dt
ORDER BY days_to_unlock ASC
LIMIT 500
""", {"dt": dt})

st.line_chart(cal.set_index("days_to_unlock")["unlocking_uopt"])
st.dataframe(cal, use_container_width=True)

st.divider()

# --- Holder distribution ---
st.subheader("Locked Holder Distribution")
dist = query_df("""
SELECT holding_bucket, wallet_count, total_locked_uopt
FROM optio_warehouse.gold.gold_locked_holder_distribution_daily
WHERE dt = :dt
""", {"dt": dt})

st.bar_chart(dist.set_index("holding_bucket")["wallet_count"])
st.dataframe(dist, use_container_width=True)
