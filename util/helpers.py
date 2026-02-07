import pandas as pd

UOPT_PER_OPT = 1_000_000

def to_opt(x):
    if x is None:
        return None
    return float(x) / UOPT_PER_OPT


def to_opt_series(s: pd.Series) -> pd.Series:
    # handles decimals/strings safely
    return pd.to_numeric(s, errors="coerce") / UOPT_PER_OPT


def fmt_opt(x) -> str:
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return "—"