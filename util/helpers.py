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


def human(n: float) -> str:
    # 1234 -> 1.23K, 1_234_567 -> 1.23M, 1_234_567_890 -> 1.23B
    n = float(n)
    for unit, div in [("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)]:
        if abs(n) >= div:
            return f"{n/div:.2f}{unit}"
    return f"{n:,.0f}"

def fmt_int(n) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return "—"