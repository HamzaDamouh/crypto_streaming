import urllib.request
import urllib.error
import json
import base64

# Configuration
GRAFANA_URL = "http://localhost:3000"
AUTH = base64.b64encode(b"admin:cryptopulse123").decode("utf-8")
HEADERS = {
    "Authorization": f"Basic {AUTH}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

def make_request(endpoint, method="GET", data=None):
    url = f"{GRAFANA_URL}{endpoint}"
    req = urllib.request.Request(url, method=method, headers=HEADERS)
    if data:
        req.data = json.dumps(data).encode("utf-8")
    
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        print(f"Error {e.code} for {method} {url}: {error_body}")
        raise

def setup_datasource():
    print("Setting up Redis datasource...")
    ds_payload = {
        "name": "CryptoPulse-Redis",
        "type": "redis-datasource",
        "url": "redis://cryptopulse-redis:6379",
        "access": "proxy",
        "basicAuth": False
    }
    
    try:
        make_request("/api/datasources", method="POST", data=ds_payload)
        print("Datasource created successfully.")
        return
    except urllib.error.HTTPError as e:
        if e.code == 409:
            print("Datasource already exists. Initializing...")
            return
        raise

def create_dashboard():
    print("Creating Dashboard...")
    
    dashboard = {
        "id": None,
        "uid": "cryptopulse-live",
        "title": "CryptoPulse Live",
        "timezone": "browser",
        "refresh": "2s",
        "time": {
            "from": "now-5m",
            "to": "now"
        },
        "panels": [
            # Panel 1: BTC Price & VWAP (Left Half)
            {
                "title": "BTC Price & VWAP (5s)",
                "type": "timeseries",
                "gridPos": {"h": 10, "w": 12, "x": 0, "y": 0},
                "targets": [
                    {
                        "datasource": {"uid": "CryptoPulse-Redis"},
                        "command": "ts.get",
                        "key": "cryptopulse:ts:BTCUSDT:price",
                        "type": "command",
                        "refId": "A",
                        "legendFormat": "Price"
                    },
                    {
                        "datasource": {"uid": "CryptoPulse-Redis"},
                        "command": "ts.get", 
                        "key": "cryptopulse:ts:BTCUSDT:vwap_5s",
                        "type": "command",
                        "refId": "B", 
                         "legendFormat": "VWAP (5s)"
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "custom": {
                            "lineWidth": 2,
                            "drawStyle": "line"
                        }
                    }
                }
            },
            # Panel 2: RSI (Right Half)
            {
                "title": "RSI Momentum (14)",
                "type": "timeseries",
                "gridPos": {"h": 10, "w": 12, "x": 12, "y": 0},
                "targets": [
                    {
                        "datasource": {"uid": "CryptoPulse-Redis"},
                        "command": "ts.get",
                        "key": "cryptopulse:ts:BTCUSDT:rsi",
                        "type": "command",
                        "refId": "A",
                        "legendFormat": "BTC"
                    },
                    {
                        "datasource": {"uid": "CryptoPulse-Redis"},
                         "command": "ts.get",
                        "key": "cryptopulse:ts:ETHUSDT:rsi",
                        "type": "command",
                        "refId": "B",
                        "legendFormat": "ETH"
                    },
                     {
                        "datasource": {"uid": "CryptoPulse-Redis"},
                         "command": "ts.get",
                        "key": "cryptopulse:ts:SOLUSDT:rsi",
                        "type": "command",
                        "refId": "C",
                        "legendFormat": "SOL"
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "min": 0,
                        "max": 100,
                        "color": {"mode": "thresholds"},
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "red", "value": 70},
                                {"color": "red", "value": 30} 
                            ]
                        },
                        "custom": {
                            "drawStyle": "line",
                            "lineInterpolation": "smooth"
                        }
                    },
                    "overrides": [
                         {
                            "matcher": {"id": "byName", "options": "BTC"},
                            "properties": [{"id": "color", "value": {"fixedColor": "orange", "mode": "fixed"}}]
                         }
                    ]
                }
            },
            # Panel 3: ETH & SOL Prices (Bottom Full Width)
            {
                "title": "Altcoin Prices (ETH & SOL)",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 24, "x": 0, "y": 10},
                "targets": [
                    {
                        "datasource": {"uid": "CryptoPulse-Redis"},
                        "command": "ts.get",
                        "key": "cryptopulse:ts:ETHUSDT:price",
                        "type": "command", 
                        "refId": "A",
                         "legendFormat": "ETH Price"
                    },
                    {
                        "datasource": {"uid": "CryptoPulse-Redis"},
                        "command": "ts.get",
                        "key": "cryptopulse:ts:SOLUSDT:price", 
                        "type": "command",
                        "refId": "B",
                        "legendFormat": "SOL Price"
                    }
                ]
            }
        ],
        "schemaVersion": 38
    }

    payload = {
        "dashboard": dashboard,
        "overwrite": True
    }

    try:
        make_request("/api/dashboards/db", method="POST", data=payload)
        print("Dashboard 'CryptoPulse Live' created successfully.")
    except Exception as e:
        print(f"Failed to create dashboard: {e}")

if __name__ == "__main__":
    setup_datasource()
    create_dashboard()
