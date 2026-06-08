"""
═══════════════════════════════════════════════════════════════════════════════
  BROKER 10 API SERVER - TRADER CRISTÃO (Standalone)
  Servidor Flask standalone - conecta direto à Broker 10
  Não precisa de pastas http/ e ws/ - tudo embutido
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import sys
import json
import time
import threading
import requests
import websocket
import ssl
from datetime import datetime
from collections import defaultdict, deque
from flask import Flask, request, jsonify
from flask_cors import CORS

print("=" * 60)
print("  BROKER 10 API SERVER - TRADER CRISTÃO (Standalone)")
print("=" * 60)

# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTES E CÓDIGOS DOS ATIVOS
# ═══════════════════════════════════════════════════════════════════════════════
ACTIVES = {
    "EURUSD": 1, "EURGBP": 2, "GBPJPY": 3, "EURJPY": 4, "GBPUSD": 5,
    "USDJPY": 6, "AUDCAD": 7, "NZDUSD": 8, "USDRUB": 10, "BAIDU": 33,
    "AIG": 41, "GS": 50, "MORSTAN": 53, "USDCHF": 72, "XAUUSD": 74,
    "EURUSD-OTC": 76, "EURGBP-OTC": 77, "USDCHF-OTC": 78, "EURJPY-OTC": 79,
    "NZDUSD-OTC": 80, "GBPUSD-OTC": 81, "GBPJPY-OTC": 84, "USDJPY-OTC": 85,
    "AUDCAD-OTC": 86, "ALIBABA": 87, "AUDUSD": 99, "USDCAD": 100,
    "AUDJPY": 101, "GBPCAD": 102, "GBPCHF": 103, "GBPAUD": 104,
    "EURCAD": 105, "CHFJPY": 106, "CADCHF": 107, "EURAUD": 108,
    "USDNOK": 168, "EURNZD": 212, "USDSEK": 219, "USDTRY": 220,
    "SNAP": 756, "BTCUSD": 816, "XRPUSD": 817, "ETHUSD": 818,
    "LTCUSD": 819, "EOSUSD": 864, "USDINR": 865, "USDPLN": 866,
    "USDBRL": 867, "USDZAR": 868, "USDSGD": 892, "USDHKD": 893,
    "AUDCHF": 943, "AUDNZD": 944, "CADJPY": 945, "EURCHF": 946,
    "GBPNZD": 947, "USOUSD": 971, "EURUSD-OTC-L": 1345,
    "EURJPY-OTC-L": 1346, "EURGBP-OTC-L": 1347, "AUDCAD-OTC-L": 1348,
    "USDZAR-OTC": 1380, "USDSGD-OTC": 1381, "USDHKD-OTC": 1382,
    "BTCUSD-OTC-L": 1520, "ETHUSD-OTC-L": 1521, "PENUSD-OTC-L": 1545,
    "USDBRL-OTC-L": 1546, "USDCOP-OTC-L": 1547, "USDMXN-OTC-L": 1548,
    "XAUUSD-OTC": 1857, "XAGUSD-OTC": 1858, "USOUSD-OTC": 1859,
    "EURUSD-op": 1861, "EURJPY-op": 1864, "USDJPY-op": 1865,
    "GBPJPY-op": 1866, "GBPUSD-op": 1867, "AUDCAD-op": 1868,
    "AUDJPY-op": 1869, "AUDUSD-op": 1870, "CADCHF-op": 1871,
    "USDCAD-op": 1878, "AUDCHF-op": 1884, "NZDUSD-op": 1896,
    "CISCO-op": 1902, "CITI-op": 1903, "SNAP-op": 1904,
    "TESLA-op": 1905, "MCDON-op": 1906, "MORSTAN-op": 1907,
    "ALIBABA-op": 1908, "AMAZON-op": 1909, "APPLE-op": 1910,
    "BAIDU-op": 1911, "XAUUSD-op": 1912, "BTCUSD-op": 1916,
    "ETHUSD-op": 1918, "XRPUSD-op": 1921, "COKE-op": 1922,
    "MSFT-op": 1923, "NIKE-op": 1924, "FACEBOOK-op": 1925,
    "GOOGLE-op": 1926, "GS-op": 1927, "INTEL-op": 1928,
    "JPM-op": 1929, "AIG-op": 1930, "UKOUSD-OTC": 1931,
    "GOOGLE-OTC": 1933, "AMAZON-OTC": 1935, "TESLA-OTC": 1936,
    "FB-OTC": 1937, "APPLE-OTC": 1938, "SP500-OTC": 1971,
    "USNDAQ100-OTC": 1972, "US30-OTC": 1973, "CARDANO-OTC": 1974,
    "TRON-OTC": 1976, "DOGECOIN-OTC": 1977, "SOLUSD-OTC": 1978,
    "SP35-OTC": 2044, "FR40-OTC": 2045, "GER30-OTC": 2046,
    "UK100-OTC": 2047, "AUS200-OTC": 2048, "HK33-OTC": 2049,
    "EU50-OTC": 2050, "JP225-OTC": 2051, "Dollar_Index": 2062,
    "Yen_Index": 2063, "US30/JP225": 2064, "US100/JP225": 2065,
    "US500/JP225": 2066, "AMZN/ALIBABA": 2067, "AMZN/EBAY": 2068,
    "NVDA/AMD": 2069, "GOOGLE/MSFT": 2070, "XAU/XAG": 2071,
    "TESLA/FORD": 2072, "MSFT/AAPL": 2073, "INTEL/IBM": 2074,
    "NFLX/AMZN": 2075, "META/GOOGLE": 2076, "GER30/UK100": 2078,
    "US30/JP225-OTC": 2079, "US100/JP225-OTC": 2080,
    "US500/JP225-OTC": 2081, "AMZN/ALIBABA-OTC": 2082,
    "AMZN/EBAY-OTC": 2083, "NVDA/AMD-OTC": 2084,
    "GOOGLE/MSFT-OTC": 2085, "XAU/XAG-OTC": 2086,
    "TESLA/FORD-OTC": 2087, "MSFT/AAPL-OTC": 2088,
    "INTEL/IBM-OTC": 2089, "NFLX/AMZN-OTC": 2090,
    "TON/USD-OTC": 2091, "GER30/UK100-OTC": 2093,
    "META/GOOGLE-OTC": 2094, "NOTCOIN-OTC": 2096,
    "BIDU-OTC": 2097, "INTEL-OTC": 2098, "MSFT-OTC": 2099,
    "CITI-OTC": 2100, "COKE-OTC": 2101, "JPM-OTC": 2102,
    "MCDON-OTC": 2103, "MORSTAN-OTC": 2104, "NIKE-OTC": 2105,
    "ALIBABA-OTC": 2106, "XRPUSD-OTC": 2107, "US2000-OTC": 2108,
    "AIG-OTC": 2109, "GS-OTC": 2110, "AUDUSD-OTC": 2111,
    "USDCAD-OTC": 2112, "AUDJPY-OTC": 2113, "GBPCAD-OTC": 2114,
    "GBPCHF-OTC": 2115, "GBPAUD-OTC": 2116, "EURCAD-OTC": 2117,
    "CHFJPY-OTC": 2118, "CADCHF-OTC": 2119, "EURAUD-OTC": 2120,
    "USDNOK-OTC": 2121, "EURNZD-OTC": 2122, "USDSEK-OTC": 2123,
    "USDTRY-OTC": 2124, "SNAP-OTC": 2125, "LTCUSD-OTC": 2126,
    "EOSUSD-OTC": 2127, "USDPLN-OTC": 2128, "AUDCHF-OTC": 2129,
    "AUDNZD-OTC": 2130, "EURCHF-OTC": 2131, "GBPNZD-OTC": 2132,
    "EURGBP_GS": 2133, "CADJPY-OTC": 2136, "NZDCAD-OTC": 2137,
    "NZDJPY-OTC": 2138, "ICPUSD-OTC": 2139, "IMXUSD-OTC": 2140,
    "JUPUSD-OTC": 2141, "BONKUSD-OTC": 2142, "LINKUSD-OTC": 2143,
    "WIFUSD-OTC": 2144, "PEPEUSD-OTC": 2145, "FLOKIUSD-OTC": 2146,
    "GALAUSD-OTC": 2147, "BCHUSD-OTC": 2148, "DOTUSD-OTC": 2149,
    "ATOMUSD-OTC": 2150, "INJUSD-OTC": 2151, "SEIUSD-OTC": 2152,
    "IOTAUSD-OTC": 2153, "BEAMUSD-OTC": 2154, "DASHUSD-OTC": 2155,
    "ARBUSD-OTC": 2156, "WLDUSD-OTC": 2157, "ORDIUSD-OTC": 2158,
    "SATSUSD-OTC": 2159, "PYTHUSD-OTC": 2160, "RONINUSD-OTC": 2161,
    "TIAUSD-OTC": 2162, "MANAUSD-OTC": 2163, "SANDUSD-OTC": 2164,
    "GRTUSD-OTC": 2165, "STXUSD-OTC": 2166, "MATICUSD-OTC": 2167,
    "NEARUSD-OTC": 2168, "EURTHB-OTC": 2181, "USDTHB-OTC": 2182,
    "JPYTHB-OTC": 2183, "TRUMPvsHARRIS-OTC": 2185, "HMSTR-OTC": 2192,
    "CHFNOK-OTC": 2200, "NOKJPY-OTC": 2201, "NZDCHF-OTC": 2202
}

# ═══════════════════════════════════════════════════════════════════════════════
# CLASSE DA API BROKER 10 (Standalone)
# ═══════════════════════════════════════════════════════════════════════════════
class Broker10API:
    def __init__(self, email, password):
        self.email = email
        self.password = password
        self.session = requests.Session()
        self.session.verify = False
        self.ssid = None
        self.connected = False
        self.balance = None
        self.currency = None
        self.mode = "PRACTICE"
        self.ws = None
        self.ws_thread = None
        self.timesync = None
        self.candles_data = None
        self.balances_raw = None
        self.lock = threading.Lock()
        self.realtime_candles = defaultdict(lambda: defaultdict(dict))
        self.candle_check = defaultdict(lambda: defaultdict(dict))

    def connect(self):
        try:
            print(f"[+] Conectando com {self.email}...")

            # Login HTTP
            login_data = {
                "identifier": self.email,
                "password": self.password,
                "token": None
            }
            response = self.session.post(
                "https://api.trade.broker10.com/v2/login",
                data=login_data,
                headers={"User-Agent": "Mozilla/5.0"}
            )

            if response.status_code != 200:
                return False, f"Login falhou: {response.status_code}"

            # Pega SSID
            try:
                self.ssid = response.cookies["ssid"]
            except:
                return False, "SSID não encontrado no cookie"

            # Conecta WebSocket
            self._connect_websocket()

            self.connected = True
            print("[+] Conectado com sucesso!")
            return True, None

        except Exception as e:
            return False, str(e)

    def _connect_websocket(self):
        def on_message(ws, message):
            try:
                msg = json.loads(message)
                if msg.get("name") == "timeSync":
                    self.timesync = msg.get("msg")
                elif msg.get("name") == "candles":
                    self.candles_data = msg.get("msg", {}).get("candles", [])
                elif msg.get("name") == "balances":
                    self.balances_raw = msg
                    for balance in msg.get("msg", []):
                        if balance.get("type") == 4:
                            self.balance = balance.get("amount", 0)
                            self.currency = balance.get("currency", "USD")
            except:
                pass

        def on_open(ws):
            print("[+] WebSocket conectado")
            # Envia autenticação
            auth_msg = {
                "name": "authenticate",
                "msg": {"ssid": self.ssid, "protocol": 3}
            }
            ws.send(json.dumps(auth_msg))

        self.ws = websocket.WebSocketApp(
            "wss://ws.trade.broker10.com/echo/websocket",
            on_message=on_message,
            on_open=on_open
        )

        self.ws_thread = threading.Thread(target=self.ws.run_forever, kwargs={
            "sslopt": {"cert_reqs": ssl.CERT_NONE}
        })
        self.ws_thread.daemon = True
        self.ws_thread.start()
        time.sleep(3)  # Aguarda conexão

    def get_balance(self):
        if not self.connected:
            return None
        return self.balance

    def get_currency(self):
        if not self.connected:
            return None
        return self.currency

    def get_balance_mode(self):
        return self.mode

    def get_candles(self, active, interval, count, endtime):
        if not self.connected:
            return []

        active_id = ACTIVES.get(active)
        if not active_id:
            return []

        # Solicita candles via WebSocket
        req_id = str(int(time.time() * 1000))
        msg = {
            "name": "sendMessage",
            "msg": {
                "name": "get-candles",
                "version": "2.0",
                "body": {
                    "active_id": active_id,
                    "size": interval,
                    "to": int(endtime),
                    "count": count
                }
            },
            "request_id": req_id
        }

        try:
            self.ws.send(json.dumps(msg))
            # Aguarda resposta
            start = time.time()
            while self.candles_data is None and time.time() - start < 10:
                time.sleep(0.1)

            result = self.candles_data or []
            self.candles_data = None  # Reseta para próxima chamada
            return result

        except Exception as e:
            print(f"[!] Erro ao pegar candles: {e}")
            return []

    def buy_binary(self, active, amount, direction, expiration):
        if not self.connected:
            return False, None

        active_id = ACTIVES.get(active)
        if not active_id:
            return False, "Ativo não encontrado"

        # Calcula expiração
        now = time.time()
        exp = int(now + (expiration * 60))

        req_id = str(int(time.time() * 1000))
        msg = {
            "name": "sendMessage",
            "msg": {
                "name": "binary-options.open-option",
                "version": "1.0",
                "body": {
                    "price": amount,
                    "act": active_id,
                    "dir": direction,
                    "exp": exp,
                    "time": int(now),
                    "user_balance_id": 0
                }
            },
            "request_id": req_id
        }

        try:
            self.ws.send(json.dumps(msg))
            return True, req_id
        except Exception as e:
            return False, str(e)

    def close(self):
        if self.ws:
            self.ws.close()
        self.connected = False


# ═══════════════════════════════════════════════════════════════════════════════
# FLASK APP
# ═══════════════════════════════════════════════════════════════════════════════
app = Flask(__name__)
CORS(app)

# Estado global
api_instance = None
api_lock = threading.Lock()


def get_api():
    global api_instance
    if api_instance is None or not api_instance.connected:
        email = os.environ.get("BROKER_EMAIL", "")
        password = os.environ.get("BROKER_PASSWORD", "")
        if email and password:
            api_instance = Broker10API(email, password)
            api_instance.connect()
    return api_instance


# ═══════════════════════════════════════════════════════════════════════════════
# ROTAS
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/", methods=["GET"])
def status():
    api = get_api()
    return jsonify({
        "status": "online",
        "service": "Broker 10 API Server - TRADER CRISTÃO",
        "version": "3.0 Standalone",
        "connected": api.connected if api else False,
        "balance": api.balance if api else None,
        "currency": api.currency if api else None,
        "mode": api.mode if api else "PRACTICE",
        "timestamp": datetime.now().isoformat()
    })


@app.route("/connect", methods=["POST"])
def connect():
    data = request.get_json() or {}

    if data.get("email"):
        os.environ["BROKER_EMAIL"] = data["email"]
    if data.get("password"):
        os.environ["BROKER_PASSWORD"] = data["password"]

    global api_instance
    api_instance = None
    api = get_api()

    return jsonify({
        "success": api.connected if api else False,
        "connected": api.connected if api else False,
        "balance": api.balance if api else None,
        "currency": api.currency if api else None,
        "mode": api.mode if api else "PRACTICE"
    })


@app.route("/balance", methods=["GET"])
def balance():
    api = get_api()
    if not api or not api.connected:
        return jsonify({"error": "Não conectado"}), 503

    return jsonify({
        "balance": api.get_balance(),
        "currency": api.get_currency(),
        "mode": api.get_balance_mode()
    })


@app.route("/actives", methods=["GET"])
def actives():
    return jsonify({
        "actives": ACTIVES,
        "total": len(ACTIVES),
        "otc": [k for k in ACTIVES.keys() if "OTC" in k],
        "normal": [k for k in ACTIVES.keys() if "OTC" not in k]
    })


@app.route("/candles/<active>", methods=["GET"])
def candles(active):
    api = get_api()
    if not api or not api.connected:
        return jsonify({"error": "Não conectado"}), 503

    interval = int(request.args.get("interval", 60))
    count = int(request.args.get("count", 10))

    try:
        with api_lock:
            data = api.get_candles(active, interval, count, time.time())

        formatted = []
        for c in data:
            formatted.append({
                "from": c.get("from"),
                "to": c.get("to"),
                "open": c.get("open"),
                "close": c.get("close"),
                "max": c.get("max"),
                "min": c.get("min"),
                "volume": c.get("volume"),
                "color": "green" if c.get("close", 0) >= c.get("open", 0) else "red"
            })

        return jsonify({
            "active": active,
            "interval": interval,
            "count": len(formatted),
            "candles": formatted
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/buy", methods=["POST"])
def buy():
    api = get_api()
    if not api or not api.connected:
        return jsonify({"error": "Não conectado"}), 503

    data = request.get_json() or {}
    active = data.get("active")
    amount = data.get("amount", 1)
    direction = data.get("direction", "CALL")
    expiration = data.get("expiration", 1)

    if not active:
        return jsonify({"error": "Par de moedas não especificado"}), 400

    try:
        with api_lock:
            status, order_id = api.buy_binary(active, amount, direction, expiration)

        return jsonify({
            "success": status,
            "order_id": order_id,
            "active": active,
            "direction": direction,
            "amount": amount,
            "expiration": expiration
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# INICIALIZAÇÃO
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 5000))

    print("\n" + "=" * 60)
    print(f"INICIANDO SERVIDOR NA PORTA {PORT}")
    print("=" * 60)

    # Tenta conectar automaticamente
    if os.environ.get("BROKER_EMAIL") and os.environ.get("BROKER_PASSWORD"):
        print("[*] Credenciais encontradas, conectando...")
        get_api()

    app.run(host="0.0.0.0", port=PORT, debug=False)
