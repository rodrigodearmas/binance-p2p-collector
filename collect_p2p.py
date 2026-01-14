
# collect_p2p.py
import os, csv, json, datetime as dt, unicodedata

import requests

API_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"  # NO oficial

# --- Config ---
ASSET      = os.getenv("ASSET", "USDT")
FIAT       = os.getenv("FIAT",  "VES")
PAY_TYPES  = [p.strip() for p in os.getenv("PAY_TYPES", "").split(",") if p.strip()]
ROWS       = int(os.getenv("ROWS", "50"))
CSV_PATH   = os.getenv("CSV_PATH", "binance_p2p_prices.csv")
START_DATE = os.getenv("START_DATE", "")
END_DATE   = os.getenv("END_DATE",   "")
MIN_ORDER  = float(os.getenv("MIN_ORDER", "100"))
RETRIES    = int(os.getenv("RETRIES", "2"))

HEADERS = {
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Origin": "https://p2p.binance.com",
    "User-Agent": "Mozilla/5.0"
}

def now_utc():
    return dt.datetime.now(dt.timezone.utc)

def in_window():
    n = now_utc()
    if START_DATE:
        try:
            s = dt.datetime.fromisoformat(START_DATE.replace("Z", "+00:00"))
            if n < s: return False
        except: pass
    if END_DATE:
        try:
            e = dt.datetime.fromisoformat(END_DATE.replace("Z", "+00:00"))
            if n > e: return False
        except: pass
    return True

def normalize_text(s):
    if not s: return ""
    # quita acentos y pasa a minúsculas
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower().strip()

PAY_TOKENS_NORM = [normalize_text(x) for x in PAY_TYPES]

def payment_method_matches(methods):
    """methods: lista de objetos tradeMethods del anuncio"""
    if not PAY_TOKENS_NORM:
        return True
    names_norm = []
    for m in (methods or []):
        name = m.get("tradeMethodName") or m.get("tradeMethodShortName") or ""
        names_norm.append(normalize_text(name))
    # hay match si cualquier token aparece en cualquiera de los nombres normalizados
    return any(any(tok in nm for tok in PAY_TOKENS_NORM) for nm in names_norm)

def parse_float(s):
    try:
        return float(s)
    except:
        return None

def fetch_side(trade_type):
    """
    trade_type: 'SELL' (comprar cripto) | 'BUY' (vender cripto)
    Devuelve dict con counters y precios filtrados.
    """
    payload = {
        "page": 1,
        "rows": ROWS,
        "payTypes": [],              # no filtramos aquí; filtramos por nombre luego
        "publisherType": None,       # cualquiera
        "asset": ASSET,
        "tradeType": trade_type,
        "fiat": FIAT,
        "merchantCheck": False
    }

    last_err = None
    for i in range(RETRIES + 1):
        try:
            r = requests.post(API_URL, headers=HEADERS, data=json.dumps(payload), timeout=30)
            if r.status_code == 200:
                j = r.json()
                data = j.get("data", [])
                total = len(data)

                # 1) filtro por método de pago (por nombre normalizado)
                tmp = []
                for row in data:
                    adv = (row or {}).get("adv", {}) or {}
                    methods = (row or {}).get("adv", {}).get("tradeMethods", []) or (row or {}).get("tradeMethods", [])
                    if payment_method_matches(methods):
                        tmp.append(row)
                after_pay = len(tmp)

                # 2) filtro por monto mínimo de orden
                filtered_rows = []
                prices = []
                after_minorder = 0
                for row in tmp:
                    adv = (row or {}).get("adv", {}) or {}
                    price_f = parse_float(adv.get("price"))
                    min_f   = parse_float(adv.get("minSingleTransAmount"))
                    max_f   = parse_float(adv.get("maxSingleTransAmount"))

                    valid_order = True
                    if min_f is not None and min_f > MIN_ORDER:
                        valid_order = False
                    if max_f is not None and max_f < MIN_ORDER:
                        valid_order = False

                    if valid_order:
                        after_minorder += 1
                        if price_f is not None:
                            prices.append(price_f)
                            filtered_rows.append(row)

                # depuración: imprime contadores
                print(f"[{trade_type}] total={total} after_pay={after_pay} after_minorder={after_minorder} with_prices={len(prices)}")

                if not prices:
                    return {
                        "prices": [],
                        "count": 0,
                        "best": None,
                        "min_price": None,
                        "max_price": None
                    }

                best = (min(prices) if trade_type == "SELL" else max(prices))
                return {
                    "prices": prices,
                    "count": len(prices),
                    "best": best,
                    "min_price": min(prices),
                    "max_price": max(prices)
                }
            else:
                last_err = f"HTTP {r.status_code}"
        except Exception as e:
            last_err = str(e)
        import time; time.sleep(1.5 * (i + 1))

    print(f"[{trade_type}] request failed: {last_err}")
    return {"prices": [], "count": 0, "best": None, "min_price": None, "max_price": None}

def avg_top10(prices, trade_type):
    if not prices:
        return None
    if trade_type == "SELL":
        top = sorted(prices)[:10]
    else:
        top = sorted(prices, reverse=True)[:10]
    return sum(top)/len(top) if top else None

def NA_if_none(x):
    return x if x is not None else "NA"

def main():
    if not in_window():
        print("Fuera de ventana de captura. No se registra.")
        return

    ts = now_utc().isoformat()
    sell = fetch_side("SELL")
    buy  = fetch_side("BUY")

    avg_sell_top10 = avg_top10(sell["prices"], "SELL")
    avg_buy_top10  = avg_top10(buy["prices"],  "BUY")

    spread = None
    if sell["best"] is not None and buy["best"] is not None:
        spread = buy["best"] - sell["best"]

    header = [
        "timestamp_utc","asset","fiat","pay_types","min_order",
        "best_sell","sell_count","sell_min_price","sell_max_price","avg_sell_top10",
        "best_buy","buy_count","buy_min_price","buy_max_price","avg_buy_top10",
        "spread_buy_minus_sell","rows_considered"
    ]
    row = [
        ts, ASSET, FIAT, ",".join(PAY_TYPES), MIN_ORDER,
        NA_if_none(sell["best"]), sell["count"], NA_if_none(sell["min_price"]), NA_if_none(sell["max_price"]), NA_if_none(avg_sell_top10),
        NA_if_none(buy["best"]),  buy["count"],  NA_if_none(buy["min_price"]),  NA_if_none(buy["max_price"]),  NA_if_none(avg_buy_top10),
        NA_if_none(spread), ROWS
    ]

    file_exists = os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(header)
        w.writerow(row)
    print("OK:", row)

if __name__ == "__main__":
    main()
