"""Historical calibration analysis for Polymarket BTC 15-min markets.

Pipeline (idempotent — each phase skips if its output already exists, unless
--redownload is given):

    explore         : download 5 recent closed BTC 15m events, dump JSON, schema
    probe           : test prices-history + series filter (one-shot diagnostics)
    download-meta   : phase A — paginate Gamma /events, save market metadata
    enrich-prices   : phase B — CLOB /prices-history → price at 7 horizons per market
    enrich-binance  : phase C — Binance /klines → realized vol 15m + momentum 5m
    analyze         : phase D — clean, classify regimes, calibrate (global + stratified)

Convenience shortcuts:
    --download-all  : phases A+B+C in sequence
    --all           : explore + A + B + C + analyze

The Gamma API (https://gamma-api.polymarket.com) and CLOB API
(https://clob.polymarket.com) are public; Binance public market data is too.
No credentials required.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

import requests

# ---- Paths
HERE = Path(__file__).resolve().parent
SAMPLE_JSON = HERE / "sample_events.json"
META_PARQUET = HERE / "btc_15m_meta.parquet"
PRICES_PARQUET = HERE / "btc_15m_prices.parquet"
DATASET_PARQUET = HERE / "btc_15m_calibration_dataset.parquet"
CALIBRATION_CSV = HERE / "calibration_global.csv"
STRATIFIED_CSV = HERE / "calibration_stratified.csv"
PLOT_GLOBAL_PNG = HERE / "calibration_plot.png"
PLOT_GRID_PNG = HERE / "calibration_grid_2x4.png"
HEATMAP_STRAT_PNG = HERE / "edge_heatmap_t5m_stratified.png"

# ---- API
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
BINANCE_BASE = "https://api.binance.com"
SLUG_RE = re.compile(r"^btc-(?:updown|up-or-down)-15m-\d+$")
USER_AGENT = "polyrec-research/0.1 (historical calibration)"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

# ---- Rate limits (seconds between requests, per host)
GAMMA_INTERVAL = 0.22       # ~4.5 req/s — conservative
CLOB_INTERVAL = 0.08        # ~12.5 req/s — well under any Polymarket cap we've seen
BINANCE_INTERVAL = 0.10     # ~10 req/s — well under 20 req/s Binance public cap

# ---- Horizons (seconds before market end)
HORIZONS_S = [60, 120, 180, 300, 420, 600, 780]
HORIZON_LABELS = {60: "T-1m", 120: "T-2m", 180: "T-3m", 300: "T-5m",
                  420: "T-7m", 600: "T-10m", 780: "T-13m"}
PRICE_COLS = [f"p_t-{h}s" for h in HORIZONS_S]
GAP_COLS = [f"gap_t-{h}s" for h in HORIZONS_S]


def http_get(host_base: str, path: str, params: dict[str, Any], timeout: float = 15.0) -> Any:
    r = SESSION.get(f"{host_base}{path}", params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _preview(v: Any) -> str:
    s = json.dumps(v, ensure_ascii=False) if not isinstance(v, str) else repr(v)
    return s if len(s) <= 110 else s[:107] + "..."


def _maybe_json(v: Any) -> Any:
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return v
    return v


def _parse_iso(s: Any) -> Optional[datetime]:
    if not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


# ----------------------------------------------------------------------
# PHASE 1 — explore: small sample + schema printout
# ----------------------------------------------------------------------
def explore_sample(force: bool = False) -> None:
    if SAMPLE_JSON.exists() and not force:
        print(f"[explore] {SAMPLE_JSON.name} already exists; reading and printing schema only.")
        with SAMPLE_JSON.open("r", encoding="utf-8") as f:
            sample = json.load(f)
    else:
        print("[explore] fetching closed BTC 15m events ...")
        sample = http_get(GAMMA_BASE, "/events", {
            "series_slug": "btc-up-or-down-15m", "closed": "true",
            "limit": 5, "order": "endDate", "ascending": "false",
        })
        sample = [e for e in sample if SLUG_RE.match(str(e.get("slug", "")))][:5]
        if not sample:
            sys.exit("[explore] FAIL: no BTC 15m events matched")
        with SAMPLE_JSON.open("w", encoding="utf-8") as f:
            json.dump(sample, f, indent=2, ensure_ascii=False)
        print(f"[explore] saved {len(sample)} events to {SAMPLE_JSON}")

    print("\n" + "=" * 70)
    print("SCHEMA — top-level event keys (first event)")
    print("=" * 70)
    e0 = sample[0]
    for k in sorted(e0.keys()):
        print(f"  {k:<22} {type(e0[k]).__name__:<10} {_preview(e0[k])}")

    print("\n" + "=" * 70)
    print("FIELDS WE EXTRACT (per market)")
    print("=" * 70)
    for i, e in enumerate(sample):
        ms = e.get("markets") or []
        if not ms:
            continue
        m = ms[0]
        rec = {
            "slug": e.get("slug"),
            "end_date": e.get("endDate"),
            "outcomes": _maybe_json(m.get("outcomes")),
            "outcomePrices": _maybe_json(m.get("outcomePrices")),
            "lastTradePrice": m.get("lastTradePrice"),
            "volumeNum": m.get("volumeNum"),
        }
        print(f"  [{i}] {json.dumps(rec, ensure_ascii=False)}")


# ----------------------------------------------------------------------
# PHASE 1.5 — probes
# ----------------------------------------------------------------------
def probe_series_filter() -> None:
    print("\n" + "=" * 70)
    print("PROBE — Gamma /events server-side filters")
    print("=" * 70)
    variants = [
        ({"series_slug": "btc-up-or-down-15m", "closed": "true", "limit": 5,
          "order": "endDate", "ascending": "false"}, "series_slug + order=endDate desc"),
        ({"slug": "btc-updown-15m-1779714000"}, "exact slug lookup (recent)"),
    ]
    for params, label in variants:
        try:
            r = SESSION.get(f"{GAMMA_BASE}/events", params=params, timeout=15)
            if r.status_code == 200:
                batch = r.json()
                btc15m = [e for e in batch if SLUG_RE.match(str(e.get("slug", "")))]
                print(f"  {label!r}: total={len(batch)} btc15m={len(btc15m)}")
            else:
                print(f"  {label!r}: status={r.status_code}")
        except Exception as exc:
            print(f"  {label!r}: ERROR {exc}")
        time.sleep(GAMMA_INTERVAL)


def probe_prices_history() -> None:
    print("\n" + "=" * 70)
    print("PROBE — CLOB /prices-history")
    print("=" * 70)
    if not SAMPLE_JSON.exists():
        print("[probe] sample_events.json missing — run --explore first")
        return
    with SAMPLE_JSON.open("r", encoding="utf-8") as f:
        sample = json.load(f)
    if not sample:
        print("[probe] empty sample")
        return
    e = sample[0]
    m = (e.get("markets") or [{}])[0]
    token_ids = _maybe_json(m.get("clobTokenIds"))
    outcomes = _maybe_json(m.get("outcomes"))
    if not (isinstance(token_ids, list) and isinstance(outcomes, list)):
        print("[probe] bad tokens/outcomes")
        return
    up_idx = outcomes.index("Up") if "Up" in outcomes else 0
    up_token = str(token_ids[up_idx])
    end_dt = _parse_iso(e.get("endDate"))
    end_ts = int(end_dt.timestamp())
    start_ts = end_ts - 900
    r = SESSION.get(f"{CLOB_BASE}/prices-history",
                    params={"market": up_token, "startTs": start_ts,
                            "endTs": end_ts, "fidelity": 1}, timeout=15)
    if r.status_code != 200:
        print(f"  status={r.status_code} body={r.text[:200]}")
        return
    hist = r.json().get("history", [])
    in_window = [x for x in hist if start_ts <= x.get("t", 0) <= end_ts]
    print(f"  {len(in_window)} points in window")
    for h in HORIZONS_S:
        target = end_ts - h
        nearest = min(in_window, key=lambda x: abs(x.get("t", 0) - target))
        gap = abs(nearest.get("t", 0) - target)
        print(f"  T-{h:>4}s: gap={gap}s price={nearest.get('p')}")


# ----------------------------------------------------------------------
# PHASE A — download market metadata
# ----------------------------------------------------------------------
def download_meta(days: int, force: bool = False, page_size: int = 100) -> "pd.DataFrame":
    import pandas as pd
    if META_PARQUET.exists() and not force:
        print(f"[meta] {META_PARQUET.name} exists — skipping (use --redownload to force)")
        return pd.read_parquet(META_PARQUET)

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    print(f"[meta] target: closed BTC 15m markets endDate >= {cutoff.isoformat()}")
    rows: list[dict] = []
    seen: set[str] = set()
    offset = 0
    while True:
        batch = http_get(GAMMA_BASE, "/events", {
            "series_slug": "btc-up-or-down-15m",
            "closed": "true",
            "limit": page_size,
            "offset": offset,
            "order": "endDate",
            "ascending": "false",
        })
        if not isinstance(batch, list) or not batch:
            print(f"[meta] empty page at offset={offset}, stop")
            break
        oldest_end: Optional[datetime] = None
        added = 0
        for e in batch:
            slug = str(e.get("slug", ""))
            if not SLUG_RE.match(slug) or slug in seen:
                continue
            end_dt = _parse_iso(e.get("endDate"))
            if end_dt is None:
                continue
            if oldest_end is None or end_dt < oldest_end:
                oldest_end = end_dt
            if end_dt < cutoff:
                continue
            for m in (e.get("markets") or []):
                rec = _extract_meta_row(e, m)
                if rec is None:
                    continue
                rows.append(rec)
                seen.add(slug)
                added += 1
                break  # one market per event for this series
        print(f"[meta] offset={offset:>5}  page={len(batch)}  added={added}  "
              f"total={len(rows)}  oldest_end={oldest_end.isoformat() if oldest_end else 'n/a'}")
        if oldest_end is not None and oldest_end < cutoff:
            print(f"[meta] cutoff reached, stop")
            break
        offset += page_size
        time.sleep(GAMMA_INTERVAL)

    df = pd.DataFrame(rows)
    df.to_parquet(META_PARQUET, index=False)
    print(f"[meta] saved {len(df)} rows to {META_PARQUET}")
    return df


def _extract_meta_row(event: dict, market: dict) -> Optional[dict]:
    slug = event.get("slug")
    end_dt = _parse_iso(event.get("endDate"))
    if end_dt is None:
        return None
    end_ts = int(end_dt.timestamp())
    start_ts = end_ts - 900

    outcomes = _maybe_json(market.get("outcomes"))
    outcome_prices = _maybe_json(market.get("outcomePrices"))
    token_ids = _maybe_json(market.get("clobTokenIds"))
    if not (isinstance(outcomes, list) and isinstance(outcome_prices, list)
            and isinstance(token_ids, list)):
        return None
    if "Up" not in outcomes or "Down" not in outcomes:
        return None
    up_idx = outcomes.index("Up")
    down_idx = outcomes.index("Down")
    if up_idx >= len(token_ids) or up_idx >= len(outcome_prices):
        return None

    outcome_up_won: Optional[int] = None
    try:
        p_up_resolved = float(outcome_prices[up_idx])
        if p_up_resolved in (0.0, 1.0):
            outcome_up_won = int(p_up_resolved)
    except (TypeError, ValueError):
        pass

    try:
        volume = float(market.get("volumeNum") or 0.0)
    except (TypeError, ValueError):
        volume = 0.0

    return {
        "slug": slug,
        "end_ts": end_ts,
        "start_ts": start_ts,
        "end_date_iso": event.get("endDate"),
        "up_token": str(token_ids[up_idx]),
        "down_token": str(token_ids[down_idx]),
        "outcome_up_won": outcome_up_won,
        "volume_usd": volume,
        "last_trade_price_settled": market.get("lastTradePrice"),
    }


# ----------------------------------------------------------------------
# PHASE B — enrich with CLOB prices at 7 horizons
# ----------------------------------------------------------------------
def enrich_with_prices(force: bool = False) -> "pd.DataFrame":
    import pandas as pd
    if PRICES_PARQUET.exists() and not force:
        print(f"[prices] {PRICES_PARQUET.name} exists — skipping")
        return pd.read_parquet(PRICES_PARQUET)
    if not META_PARQUET.exists():
        sys.exit("[prices] meta parquet missing — run --download-meta first")

    meta = pd.read_parquet(META_PARQUET)
    print(f"[prices] enriching {len(meta)} markets ...")

    out_rows: list[dict] = []
    started = time.monotonic()
    fail_count = 0
    for idx, row in enumerate(meta.itertuples(index=False), start=1):
        end_ts = int(row.end_ts)
        start_ts = int(row.start_ts)
        prices, gaps, n_points = _fetch_horizons(row.up_token, start_ts, end_ts)
        if prices is None:
            fail_count += 1
        rec = dict(row._asdict())
        for h, p, g in zip(HORIZONS_S, prices or [None] * len(HORIZONS_S),
                           gaps or [None] * len(HORIZONS_S)):
            rec[f"p_t-{h}s"] = p
            rec[f"gap_t-{h}s"] = g
        rec["clob_points_in_window"] = n_points
        out_rows.append(rec)

        if idx % 250 == 0:
            elapsed = time.monotonic() - started
            rate = idx / elapsed
            eta = (len(meta) - idx) / rate if rate > 0 else 0
            print(f"[prices] {idx}/{len(meta)}  rate={rate:.1f}/s  eta={eta/60:.1f}min  fails={fail_count}")
        time.sleep(CLOB_INTERVAL)

    df = pd.DataFrame(out_rows)
    df.to_parquet(PRICES_PARQUET, index=False)
    print(f"[prices] saved {len(df)} rows to {PRICES_PARQUET} (fails={fail_count})")
    return df


def _fetch_horizons(token_id: str, start_ts: int, end_ts: int):
    """Returns (prices_list, gaps_list, n_points_in_window) or (None,None,0) on failure."""
    try:
        r = SESSION.get(f"{CLOB_BASE}/prices-history",
                        params={"market": token_id, "startTs": start_ts,
                                "endTs": end_ts, "fidelity": 1},
                        timeout=15)
        if r.status_code != 200:
            return None, None, 0
        hist = r.json().get("history", [])
    except Exception:
        return None, None, 0
    in_window = [x for x in hist if start_ts <= x.get("t", 0) <= end_ts]
    if not in_window:
        return None, None, 0
    prices = []
    gaps = []
    for h in HORIZONS_S:
        target = end_ts - h
        nearest = min(in_window, key=lambda x: abs(x.get("t", 0) - target))
        prices.append(float(nearest.get("p", 0.0)))
        gaps.append(int(abs(nearest.get("t", 0) - target)))
    return prices, gaps, len(in_window)


# ----------------------------------------------------------------------
# PHASE C — enrich with Binance regime features
# ----------------------------------------------------------------------
def enrich_with_binance(force: bool = False) -> "pd.DataFrame":
    import pandas as pd
    if DATASET_PARQUET.exists() and not force:
        print(f"[binance] {DATASET_PARQUET.name} exists — skipping")
        return pd.read_parquet(DATASET_PARQUET)
    if not PRICES_PARQUET.exists():
        sys.exit("[binance] prices parquet missing — run --enrich-prices first")

    df = pd.read_parquet(PRICES_PARQUET)
    print(f"[binance] enriching {len(df)} markets with realized vol + momentum ...")

    vols: list[Optional[float]] = []
    moms: list[Optional[float]] = []
    started = time.monotonic()
    fail = 0
    for idx, row in enumerate(df.itertuples(index=False), start=1):
        start_ts = int(row.start_ts)
        v, m = _fetch_binance_regime(start_ts)
        if v is None and m is None:
            fail += 1
        vols.append(v)
        moms.append(m)
        if idx % 500 == 0:
            elapsed = time.monotonic() - started
            rate = idx / elapsed
            eta = (len(df) - idx) / rate if rate > 0 else 0
            print(f"[binance] {idx}/{len(df)}  rate={rate:.1f}/s  eta={eta/60:.1f}min  fails={fail}")
        time.sleep(BINANCE_INTERVAL)

    df = df.assign(btc_realized_vol_15m=vols, btc_momentum_5m=moms)
    df.to_parquet(DATASET_PARQUET, index=False)
    print(f"[binance] saved final dataset to {DATASET_PARQUET} (fails={fail})")
    return df


def _fetch_binance_regime(market_start_ts: int):
    """Pull 16 minutes of 1m klines ending at market_start_ts, compute:
       - realized_vol_15m : stdev of 1m log returns over the 15m PRE-window
       - momentum_5m      : (close[-1] / close[-6]) - 1   (last 5 closed minutes)
    """
    import math
    end_ms = market_start_ts * 1000
    start_ms = end_ms - 16 * 60 * 1000  # 16 minutes back, to get 15 closed candles
    try:
        r = SESSION.get(f"{BINANCE_BASE}/api/v3/klines",
                        params={"symbol": "BTCUSDT", "interval": "1m",
                                "startTime": start_ms, "endTime": end_ms,
                                "limit": 20},
                        timeout=10)
        if r.status_code != 200:
            return None, None
        klines = r.json()
    except Exception:
        return None, None
    if not isinstance(klines, list) or len(klines) < 6:
        return None, None
    closes = []
    for k in klines:
        try:
            closes.append(float(k[4]))
        except (TypeError, ValueError, IndexError):
            return None, None
    if len(closes) < 6:
        return None, None
    # Use the last 15 closed candles for realized vol; closes is at most 20, take last 15
    series = closes[-16:] if len(closes) >= 16 else closes
    rets = []
    for a, b in zip(series, series[1:]):
        if a > 0 and b > 0:
            rets.append(math.log(b / a))
    if len(rets) < 5:
        return None, None
    mean = sum(rets) / len(rets)
    var = sum((x - mean) ** 2 for x in rets) / (len(rets) - 1)
    realized_vol = math.sqrt(var)
    # Momentum: last 5 minutes => closes[-1] / closes[-6]
    if closes[-6] == 0:
        return realized_vol, None
    momentum = closes[-1] / closes[-6] - 1.0
    return realized_vol, momentum


# ----------------------------------------------------------------------
# PHASE D — clean, classify regimes, calibrate (global + stratified)
# ----------------------------------------------------------------------
def classify_regimes(df) -> "pd.DataFrame":
    import pandas as pd
    import numpy as np
    df = df.copy()
    # Vol terciles
    v = df["btc_realized_vol_15m"]
    has_v = v.notna()
    if has_v.any():
        q33, q66 = np.nanpercentile(v[has_v], [33.33, 66.67])
        def _vbin(x):
            if x is None or (isinstance(x, float) and np.isnan(x)):
                return None
            if x < q33:
                return "low"
            if x < q66:
                return "mid"
            return "high"
        df["regime_vol"] = v.map(_vbin)
        df.attrs["vol_terciles"] = (float(q33), float(q66))
    else:
        df["regime_vol"] = None

    def _dbin(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        if x > 0.001:
            return "up"
        if x < -0.001:
            return "down"
        return "sideways"
    df["regime_dir"] = df["btc_momentum_5m"].map(_dbin)
    return df


def clean_dataset(min_volume: float = 1000.0):
    import pandas as pd
    if not DATASET_PARQUET.exists():
        sys.exit(f"[clean] missing {DATASET_PARQUET}, run --enrich-binance first")
    df = pd.read_parquet(DATASET_PARQUET)
    total = len(df)
    reasons: dict[str, int] = {}

    has_outcome = df["outcome_up_won"].isin([0, 1])
    reasons["missing_outcome"] = int((~has_outcome).sum())
    df = df[has_outcome]

    has_vol = df["volume_usd"].fillna(0) >= min_volume
    reasons["below_min_volume"] = int((~has_vol).sum())
    df = df[has_vol]

    has_any_price = df[PRICE_COLS].notna().any(axis=1)
    reasons["no_price_horizon"] = int((~has_any_price).sum())
    df = df[has_any_price]

    df = df.drop_duplicates(subset=["slug"]).reset_index(drop=True)
    df = classify_regimes(df)
    print(f"[clean] in={total} out={len(df)} (kept)")
    for k, v in reasons.items():
        print(f"[clean]   dropped {k:<22} {v}")
    if "vol_terciles" in df.attrs:
        q33, q66 = df.attrs["vol_terciles"]
        print(f"[clean] vol terciles: low<{q33:.5f}  mid<{q66:.5f}  high>=")
    return df


# ---- Calibration core
def _calibrate(p_arr, y_arr, edges) -> list[dict]:
    import numpy as np
    from scipy import stats
    # Symmetric reflection (each market contributes p_up and 1-p_up)
    p_all = np.concatenate([p_arr, 1.0 - p_arr])
    y_all = np.concatenate([y_arr, 1 - y_arr])
    mask = p_all >= 0.50
    p_sym = p_all[mask]
    y_sym = y_all[mask]
    idx = np.digitize(p_sym, edges) - 1
    out = []
    for b in range(len(edges) - 1):
        sel = idx == b
        n = int(sel.sum())
        if n == 0:
            out.append({"bucket_low": edges[b], "bucket_high": edges[b+1],
                        "n": 0, "p_mean": float("nan"), "hit_rate": float("nan"),
                        "edge": float("nan"), "se": float("nan"),
                        "binom_p": float("nan"), "significant": False})
            continue
        p_mean = float(p_sym[sel].mean())
        hit = float(y_sym[sel].mean())
        edge = hit - p_mean
        se = float(np.sqrt(hit * (1 - hit) / n))
        wins = int(y_sym[sel].sum())
        binom_p = float(stats.binomtest(wins, n, p_mean, alternative="two-sided").pvalue) if 0 < p_mean < 1 else float("nan")
        out.append({"bucket_low": float(edges[b]), "bucket_high": float(edges[b+1]),
                    "n": n, "p_mean": p_mean, "hit_rate": hit, "edge": edge,
                    "se": se, "binom_p": binom_p,
                    "significant": (abs(edge) > 0.02) and (binom_p < 0.05)})
    return out


def analyze_calibration(df) -> None:
    import pandas as pd
    import numpy as np

    if len(df) == 0:
        print("[analyze] empty dataset")
        return

    edges = np.array([0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.001])

    # ---- GLOBAL calibration per horizon ----
    global_rows: list[dict] = []
    for h in HORIZONS_S:
        col = f"p_t-{h}s"
        sub = df[df[col].notna() & df["outcome_up_won"].isin([0, 1])]
        p = sub[col].to_numpy(float)
        y = sub["outcome_up_won"].to_numpy(int)
        if len(p) == 0:
            continue
        for r in _calibrate(p, y, edges):
            r["horizon_s"] = h
            r["horizon_label"] = HORIZON_LABELS[h]
            r["n_markets"] = len(p)
            global_rows.append(r)

    gtab = pd.DataFrame(global_rows)
    gtab.to_csv(CALIBRATION_CSV, index=False)
    _print_global_table(gtab)
    _plot_grid(gtab)

    # ---- STRATIFIED by regime ----
    strat_rows: list[dict] = []
    for strat_field, label in [("regime_vol", "vol"), ("regime_dir", "dir")]:
        if strat_field not in df.columns:
            continue
        for level in [v for v in df[strat_field].dropna().unique()]:
            sub_df = df[df[strat_field] == level]
            for h in HORIZONS_S:
                col = f"p_t-{h}s"
                sub = sub_df[sub_df[col].notna() & sub_df["outcome_up_won"].isin([0, 1])]
                p = sub[col].to_numpy(float)
                y = sub["outcome_up_won"].to_numpy(int)
                if len(p) == 0:
                    continue
                for r in _calibrate(p, y, edges):
                    r["strat_field"] = strat_field
                    r["strat_level"] = level
                    r["horizon_s"] = h
                    r["horizon_label"] = HORIZON_LABELS[h]
                    r["n_markets"] = len(p)
                    strat_rows.append(r)

    stab = pd.DataFrame(strat_rows)
    stab.to_csv(STRATIFIED_CSV, index=False)
    print(f"\n[strat] saved {len(stab)} stratified rows to {STRATIFIED_CSV}")
    _plot_heatmap_t5m(stab)
    _verdict(gtab, stab, len(df))


def _print_global_table(gtab) -> None:
    import pandas as pd
    print("\n" + "=" * 110)
    print("GLOBAL CALIBRATION (symmetric, by horizon)")
    print("=" * 110)
    for h in HORIZONS_S:
        sub = gtab[(gtab["horizon_s"] == h) & (gtab["n"] > 0)]
        if sub.empty:
            continue
        n_m = int(sub["n_markets"].iloc[0])
        print(f"\n--- {HORIZON_LABELS[h]}  (markets with this horizon = {n_m})")
        print(f"{'bucket':<14} {'n':>6} {'p_mean':>8} {'hit':>8} {'edge':>8} {'se':>8} {'p':>10} {'sig':>4}")
        for _, r in sub.iterrows():
            bk = f"[{r['bucket_low']:.2f},{r['bucket_high']:.2f})"
            print(f"{bk:<14} {int(r['n']):>6} {r['p_mean']:>8.4f} {r['hit_rate']:>8.4f} "
                  f"{r['edge']:+8.4f} {r['se']:>8.4f} {r['binom_p']:>10.4g} {'YES' if r['significant'] else '':>4}")


def _plot_grid(gtab) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(2, 4, figsize=(16, 9), sharex=True, sharey=True)
    flat = axes.flatten()
    for ax, h in zip(flat, HORIZONS_S):
        sub = gtab[(gtab["horizon_s"] == h) & (gtab["n"] > 0)]
        ax.plot([0, 1], [0, 1], "--", color="gray", lw=1)
        if not sub.empty:
            ax.errorbar(sub["p_mean"], sub["hit_rate"], yerr=sub["se"],
                        fmt="o", capsize=3, color="C0")
            for _, r in sub.iterrows():
                ax.annotate(f"n={int(r['n'])}", (r["p_mean"], r["hit_rate"]),
                            textcoords="offset points", xytext=(4, -4), fontsize=7)
        ax.set_xlim(0.45, 1.02)
        ax.set_ylim(0.45, 1.05)
        ax.set_title(HORIZON_LABELS[h], fontsize=11)
        ax.grid(alpha=0.3)
    # hide unused
    for ax in flat[len(HORIZONS_S):]:
        ax.axis("off")
    fig.suptitle("Polymarket BTC 15-min — calibration by horizon (symmetric)", fontsize=13)
    fig.supxlabel("market-implied probability (UP)")
    fig.supylabel("empirical hit rate")
    fig.tight_layout()
    fig.savefig(PLOT_GRID_PNG, dpi=110)
    plt.close(fig)
    # Single global plot (horizon T-5m as the headline view)
    fig, ax = plt.subplots(figsize=(7, 7))
    sub = gtab[(gtab["horizon_s"] == 300) & (gtab["n"] > 0)]
    ax.plot([0, 1], [0, 1], "--", color="gray")
    if not sub.empty:
        ax.errorbar(sub["p_mean"], sub["hit_rate"], yerr=sub["se"],
                    fmt="o", capsize=4, color="C0")
        for _, r in sub.iterrows():
            ax.annotate(f"n={int(r['n'])}", (r["p_mean"], r["hit_rate"]),
                        textcoords="offset points", xytext=(6, -4), fontsize=8)
    ax.set_xlim(0.45, 1.02)
    ax.set_ylim(0.45, 1.05)
    ax.set_xlabel("price UP (market-implied probability)")
    ax.set_ylabel("hit rate")
    ax.set_title("Polymarket BTC 15-min — calibration @ T-5m (symmetric)")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(PLOT_GLOBAL_PNG, dpi=120)
    plt.close(fig)
    print(f"[plot] saved {PLOT_GLOBAL_PNG.name} and {PLOT_GRID_PNG.name}")


def _plot_heatmap_t5m(stab) -> None:
    """Edge heatmap for T-5m: regime levels (rows) × probability bucket (columns)."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    sub = stab[stab["horizon_s"] == 300]
    if sub.empty:
        print("[heatmap] no T-5m data, skipping")
        return

    # Build a (rows, cols) edge matrix for vol regime + dir regime stacked vertically
    levels = [("regime_vol", "low"), ("regime_vol", "mid"), ("regime_vol", "high"),
              ("regime_dir", "down"), ("regime_dir", "sideways"), ("regime_dir", "up")]
    bucket_labels = []
    seen_buckets = sub.drop_duplicates(subset=["bucket_low"]).sort_values("bucket_low")
    for _, r in seen_buckets.iterrows():
        bucket_labels.append(f"[{r['bucket_low']:.2f},{r['bucket_high']:.2f})")

    grid = np.full((len(levels), len(bucket_labels)), np.nan)
    counts = np.zeros((len(levels), len(bucket_labels)), dtype=int)
    sig_mask = np.zeros((len(levels), len(bucket_labels)), dtype=bool)
    for i, (field, lvl) in enumerate(levels):
        s = sub[(sub["strat_field"] == field) & (sub["strat_level"] == lvl)]
        for _, r in s.iterrows():
            if r["n"] == 0:
                continue
            j = next((k for k, b in enumerate(bucket_labels)
                      if abs(r["bucket_low"] - float(b.split(",")[0].lstrip("["))) < 1e-9), None)
            if j is None:
                continue
            grid[i, j] = r["edge"]
            counts[i, j] = int(r["n"])
            sig_mask[i, j] = bool(r["significant"])

    fig, ax = plt.subplots(figsize=(11, 5))
    vmax = float(np.nanmax(np.abs(grid))) if not np.all(np.isnan(grid)) else 0.1
    vmax = max(vmax, 0.05)
    im = ax.imshow(grid, cmap="RdBu_r", vmin=-vmax, vmax=vmax, aspect="auto")
    ax.set_xticks(range(len(bucket_labels)))
    ax.set_xticklabels(bucket_labels, rotation=45, ha="right", fontsize=9)
    ax.set_yticks(range(len(levels)))
    ax.set_yticklabels([f"{f.split('_')[1]}={l}" for f, l in levels], fontsize=9)
    for i in range(grid.shape[0]):
        for j in range(grid.shape[1]):
            if not np.isnan(grid[i, j]):
                txt = f"{grid[i, j]:+.03f}\n(n={counts[i, j]})"
                if sig_mask[i, j]:
                    txt += "*"
                ax.text(j, i, txt, ha="center", va="center", fontsize=7,
                        color="white" if abs(grid[i, j]) > vmax * 0.6 else "black")
    ax.set_title("Edge heatmap @ T-5m  (edge = hit_rate − price_mean; * = p<0.05)")
    fig.colorbar(im, ax=ax, label="edge")
    fig.tight_layout()
    fig.savefig(HEATMAP_STRAT_PNG, dpi=120)
    plt.close(fig)
    print(f"[heatmap] saved {HEATMAP_STRAT_PNG.name}")


def _verdict(gtab, stab, n_markets: int) -> None:
    print("\n" + "=" * 110)
    print("VERDICT")
    print("=" * 110)
    sig_global = gtab[gtab["significant"]] if len(gtab) else gtab
    print(f"Dataset: {n_markets} cleaned markets")
    print(f"Global significant buckets (|edge|>2% & p<0.05): {len(sig_global)}")
    if len(sig_global):
        for _, r in sig_global.iterrows():
            print(f"  {r['horizon_label']} [{r['bucket_low']:.2f},{r['bucket_high']:.2f}): "
                  f"p_mean={r['p_mean']:.3f} hit={r['hit_rate']:.3f} edge={r['edge']:+.3f} "
                  f"n={int(r['n'])} p={r['binom_p']:.3g}")

    sig_strat = stab[stab["significant"] & (stab["n"] >= 50)] if len(stab) else stab
    print(f"\nStratified significant buckets (|edge|>2% & p<0.05 & n>=50): {len(sig_strat)}")
    if len(sig_strat):
        for _, r in sig_strat.iterrows():
            print(f"  {r['strat_field']}={r['strat_level']} {r['horizon_label']} "
                  f"[{r['bucket_low']:.2f},{r['bucket_high']:.2f}): "
                  f"p_mean={r['p_mean']:.3f} hit={r['hit_rate']:.3f} "
                  f"edge={r['edge']:+.3f} n={int(r['n'])} p={r['binom_p']:.3g}")

    print("\n" + "-" * 110)
    if len(sig_strat) >= 1:
        print("  >>> EDGE IN A REGIME with p<0.05 AND n>=50  ===>  PROCEED with live run")
    elif len(sig_global) >= 1 and len(sig_strat) == 0:
        print("  >>> Global edge exists but no regime-specific edge survives  ===>  SUSPECT OVERFITTING; further validation needed")
    else:
        print("  >>> No significant edge in any regime  ===>  CLOSE the project")
    print("-" * 110)


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--explore", action="store_true")
    p.add_argument("--probe", action="store_true")
    p.add_argument("--download-meta", action="store_true")
    p.add_argument("--enrich-prices", action="store_true")
    p.add_argument("--enrich-binance", action="store_true")
    p.add_argument("--download-all", action="store_true", help="A + B + C")
    p.add_argument("--analyze", action="store_true")
    p.add_argument("--all", action="store_true")
    p.add_argument("--redownload", action="store_true")
    p.add_argument("--days", type=int, default=60)
    p.add_argument("--min-volume", type=float, default=1000.0)
    args = p.parse_args(argv)

    flagged = any([args.explore, args.probe, args.download_meta,
                   args.enrich_prices, args.enrich_binance, args.download_all,
                   args.analyze, args.all])
    if not flagged:
        args.explore = True

    if args.all or args.explore:
        explore_sample(force=args.redownload)
    if args.probe:
        probe_series_filter()
        probe_prices_history()
    if args.all or args.download_all or args.download_meta:
        download_meta(days=args.days, force=args.redownload)
    if args.all or args.download_all or args.enrich_prices:
        enrich_with_prices(force=args.redownload)
    if args.all or args.download_all or args.enrich_binance:
        enrich_with_binance(force=args.redownload)
    if args.all or args.analyze:
        df = clean_dataset(min_volume=args.min_volume)
        analyze_calibration(df)
    return 0


if __name__ == "__main__":
    sys.exit(main())
