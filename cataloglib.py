import requests
import inspect
import re
import time
from functools import wraps

class MarketOpsCatalog:
    def __init__(self, url="http://localhost:7000/catalog", user="admin_user"):
        self.url = url
        self.headers = {"X-User": user}

    def _predict_logic(self, func):
        """Analyze function source code for metadata prediction."""
        code = inspect.getsource(func)
        # Defaults
        meta = {"type": "FILE", "format": "CSV", "path": "local_storage"}
        
        if "http" in code or "requests" in code:
            meta.update({"type": "API", "format": "JSON"})
        elif "s3://" in code:
            meta.update({"type": "CLOUD", "format": "PARQUET"})
        
        # Regex to find the first quoted string (usually a path or URL)
        path_match = re.search(r"['\"](.*?)['\"]", code)
        if path_match:
            meta["path"] = path_match.group(1)
        return meta

    def _register(self, endpoint, payload):
        """Internal helper to push data to Java/Postgres backend."""
        try:
            res = requests.post(f"{self.url}/{endpoint}", json=payload, headers=self.headers)
            if res.status_code in [200, 201]:
                print(f"✅ Registered {payload.get('id')} to {endpoint}")
            else:
                print(f"⚠️ Warning {res.status_code}: {res.text}")
        except Exception as e:
            print(f"❌ Connection Error: {e}")

    # --- DECORATORS FOR ALL OBJECTS ---

    def DataSource(self, **overrides):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                pred = self._predict_logic(func)
                payload = {
                    "id": func.__name__,
                    "name": func.__name__,
                    "type": overrides.get("type", pred["type"]),
                    "format": overrides.get("format", pred["format"]),
                    "connectionData": overrides.get("connectionData", pred["path"])
                }
                self._register("data-sources", payload)
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def DataSet(self, **overrides):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                payload = {
                    "id": func.__name__,
                    "name": overrides.get("name", func.__name__),
                    "description": overrides.get("description", "Auto-generated dataset"),
                    "path": overrides.get("path", "internal_registry")
                }
                self._register("data-sets", payload)
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def Model(self, **overrides):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                payload = {
                    "id": func.__name__,
                    "modelName": overrides.get("name", func.__name__),
                    "parameters": overrides.get("parameters", {})
                }
                self._register("models", payload)
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def ETL(self, **overrides):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                payload = {
                    "id": func.__name__,
                    "name": func.__name__,
                    "language": "python",
                    "triggerType": overrides.get("trigger", "manual")
                }
                self._register("etl", payload)
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def Lineage(self, source_id, asset_id, model_id=None):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                result = func(*args, **kwargs)
                payload = {
                    "id": f"lin_{int(time.time())}",
                    "dataSourceId": source_id,
                    "marketAssetId": asset_id,
                    "modelRegistryId": model_id,
                    "userId": self.headers.get("X-User")
                }
                self._register("lineage", payload)
                return result
            return wrapper
        return decorator

# Initialize the catalog
market = MarketOpsCatalog()