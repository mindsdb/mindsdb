import os
import cachetools
from flask import Flask, request


Cache = cachetools.TTLCache(maxsize=20, ttl=int(os.environ.get("RECORDS_TTL", 60)))
app = Flask(__name__)


@app.route("/")
def index():
    return "MindsDB Hanler Discovery", 200


@app.route("/register", methods=["POST"])
def register():
    try:
        params = request.json
        host = params.get("host")
        port = params.get("port")
        _type = params.get("type")
        Cache[(host, port)] = _type
        return "OK", 200
    except Exception as e:
        return str(e), 500


@app.route("/discover")
def discover():
    res = {}
    try:
        for k in Cache:
            _type = Cache[k]
            rec = {"host": k[0], "port": k[1]}
            if _type not in res:
                res[_type] = [rec]
            else:
                res[_type].append(rec)
    except Exception as e:
        return {"error": str(e)}, 500
    return res, 200


if __name__ == "__main__":
    host = os.environ.get("HOST", "127.0.0.1")
    port = os.environ.get("PORT", 5000)
    app.run(debug=True, host=host, port=port)
