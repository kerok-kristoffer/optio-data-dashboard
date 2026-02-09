import os
import streamlit as st
import pandas as pd
from databricks import sql

from util.helpers import to_opt_series, to_opt, human, fmt_int

st.set_page_config(page_title="Optio Insights", layout="wide")

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

@st.cache_data(ttl=300)  # cache for 5 minutes
def query_df(q: str, params=None) -> pd.DataFrame:
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

# --------- UI header ---------

st.title("Optio Daily Insights (MVP)")

top_left, top_right = st.columns([3, 1], vertical_alignment="bottom")
with top_right:
    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()

# Available dates
try:
    dt_df = query_df("""
        SELECT dt
        FROM optio_warehouse.gold.gold_supply_stake_lock_daily
        ORDER BY dt DESC
        LIMIT 365
    """)
except Exception as e:
    st.error(str(e))
    st.stop()

dt_options = dt_df["dt"].tolist() if not dt_df.empty else []
if not dt_options:
    st.error("No dt values found in gold_supply_stake_lock_daily.")
    st.stop()

dt = st.selectbox("Select dt", dt_options)
st.caption(f"Showing Gold-layer aggregates for dt = {dt}. Values displayed in OPT.")

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

# Quick sanity check in the UI: recompute liquid
recomputed_liquid = total_opt - staked_opt - locked_opt
liquid_delta = liquid_opt - recomputed_liquid

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Supply (OPT)", human(total_opt))
c1.caption(f"{total_opt:,.0f}")

c2.metric("Staked (OPT)", human(staked_opt))
c2.caption(f"{staked_opt:,.0f}")

c3.metric("Locked (OPT)", human(locked_opt))
c3.caption(f"{locked_opt:,.0f}")

c4.metric("Liquid est. (OPT)", human(liquid_opt))
c4.caption(f"{liquid_opt:,.0f}")

# Liquid sanity hint (should be ~0 difference; if not, show it)
if abs(liquid_delta) > 0.5:  # allow tiny rounding drift
    st.warning(
        f"Sanity check: liquid_est differs from (total - staked - locked) by {liquid_delta:,.4f} OPT"
    )

st.divider()

st.subheader("Supply Breakdown (%)")
pct_df = pd.DataFrame({
    "Component": ["Staked", "Locked", "Liquid est."],
    "Percent": [
        (staked_opt / total_opt * 100) if total_opt else 0.0,
        (locked_opt / total_opt * 100) if total_opt else 0.0,
        (liquid_opt / total_opt * 100) if total_opt else 0.0,
    ],
    "OPT": [staked_opt, locked_opt, liquid_opt],
})
pct_df["Percent_label"] = pct_df["Percent"].map(lambda x: f"{x:.2f}%")
pct_df["OPT_label"] = pct_df["OPT"].map(human)

left, right = st.columns([2, 1], vertical_alignment="top")
with left:
    st.caption("Percent of total supply (Staked / Locked / Liquid est.)")
    st.bar_chart(pct_df.set_index("Component")["Percent"])
with right:
    show = pct_df[["Component", "Percent_label", "OPT_label"]].rename(
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
