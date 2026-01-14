
# collect_p2p.py
import os, csv, json, datetime as dt
import requests

# Endpoint NO oficial usado por la web de Binance P2P:
API_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"

# --- Config desde variables de entorno ---
ASSET = os.getenv("ASSET", "USDT")            # Activo cripto
FIAT  = os.getenv("FIAT",  "VES")             # Moneda fiat
PAY_TYPES = [p for p in os.getenv("PAY_TYPES", "Pago Móvil,Banesco,Mercantil").split(",") if p]
ROWS = int(os.getenv("ROWS", "50"))           # Traemos más filas para filtrar top10
CSV_PATH = os.getenv("CSV_PATH", "binance_p2p_prices.csv")
START_DATE = os.getenv("START_DATE", "")      # ISO8601, e.g., "2026-01-15T04:00:00Z" (00:00 VET)
END_DATE   = os.getenv("END_DATE", "")        # ISO8601, e.g., "2026-01-22T04:00:00Z" (+7 días)
MIN_ORDER  = float(os.getenv("MIN_ORDER", "100"))  # Monto objetivo de orden (en FIAT)
RETRIES    = int(os.getenv("RETRIES", "2"))        # Reintentos si falla

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
            if n < s:
                return False
        except:
            pass
    if END_DATE:
        try:
            e = dt.datetime.fromisoformat(END_DATE.replace("Z", "+00:00"))
            if n > e:
                return False
        except:
            pass
    return True

def fetch_side(trade_type):
    """
    trade_type: 'SELL' (comprar cripto) | 'BUY' (vender cripto)
    Devuelve dict con: prices(lista), count, best (min SELL / max BUY), min_price, max_price
    """
    payload = {
        "page": 1,
        "rows": ROWS,
        "payTypes": PAY_TYPES,       # ¡Ojo! Deben coincidir con los tokens internos de Binance.
        "publisherType": None,       # Sin filtro (cualquiera)
        "asset": ASSET,
        "tradeType": trade_type,
        "fiat": FIAT,
        "merchantCheck": False       # Sin limitar a merchants/verificados
        # Nota: 'transAmount' existió en ejemplos antiguos; filtramos post-respuesta por MIN_ORDER.
    }
    last_err = None
    for i in range(RETRIES + 1):
        try:
            r = requests.post(API_URL, headers=HEADERS, data=json.dumps(payload), timeout=30)
            if r.status_code == 200:
                j = r.json()
                data = j.get("data", [])
                # Filtramos por MIN_ORDER: el anuncio debe permitir operar ese monto (min<=100<=max)
                prices = []
                for row in data:
                    adv = row.get("adv", {}) or {}
                    # Campos típicos (string): 'price', 'minSingleTransAmount', 'maxSingleTransAmount'
                    price = adv.get("price")
                    min_amt = adv.get("minSingleTransAmount")
                    max_amt = adv.get("maxSingleTransAmount")
                    try:
                        price_f = float(price) if price else None
                        min_f = float(min_amt) if min_amt else None
                        max_f = float(max_amt) if max_amt else None
                    except:
                        price_f, min_f, max_f = None, None, None

                    # Si no hay límites declarados, lo aceptamos; si hay, validamos MIN_ORDER
                    valid_order = True
                    if min_f is not None and min_f > MIN_ORDER:
                        valid_order = False
                    if max_f is not None and max_f < MIN_ORDER:
                        valid_order = False

                    if price_f is not None and valid_order:
                        prices.append(price_f)

                if not prices:
                    return {
                        "prices": [],
                        "count": 0,
                        "best": None,
                        "min_price": None,
                        "max_price": None
                    }
                if trade_type == "SELL":
                    best = min(prices)            # Mejor para comprar cripto
                else:
                    best = max(prices)            # Mejor para vender cripto
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
        # backoff simple
        import time; time.sleep(1.5 * (i + 1))

    # Opción (a): si falla, devolvemos NA
    return {"prices": [], "count": 0, "best": None, "min_price": None, "max_price": None}

def avg_top10(prices, trade_type):
    if not prices:
        return None
    # SELL: top10 más baratos; BUY: top10 más caros
    if trade_type == "SELL":
        top = sorted(prices)[:10]
    else:
        top = sorted(prices, reverse=True)[:10]
    return sum(top)/len(top) if top else None

def main():
    if not in_window():
        print("Fuera de ventana de captura. No se registra.")
        return

    ts = now_utc().isoformat()
    sell = fetch_side("SELL")
    buy  = fetch_side("BUY")

    avg_sell_top10 = avg_top10(sell["prices"], "SELL")
    avg_buy_top10  = avg_top10(buy["prices"],  "BUY")

    # spread = mejor BUY - mejor SELL (si ambos existen)
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
        sell["best"], sell["count"], sell["min_price"], sell["max_price"], avg_sell_top10,
        buy["best"],  buy["count"],  buy["min_price"],  buy["max_price"],  avg_buy_top10,
        spread, ROWS
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

