import os

import pymysql
from google.cloud.sql.connector import Connector
from flask import Flask, jsonify, request

app = Flask(__name__)

ALLOWED_ENVIRONMENTS = frozenset({"staging", "preview"})

# DB_NAME secret format: INSTANCE_CONNECTION_NAME:DB_NAME:DB_USER:DB_PASSWORD
# e.g. project:region:instance:mydb:readonly_user:s3cret
_db_connection = None


def _parse_db_secret():
    global _db_connection
    if _db_connection is not None:
        return _db_connection
    raw = os.environ.get("DB_NAME")
    if not raw:
        raise ValueError("DB_NAME environment variable is required")
    parts = raw.rsplit(":", 3)
    if len(parts) != 4:
        raise ValueError(
            "DB_NAME must be INSTANCE_CONNECTION_NAME:DB_NAME:DB_USER:DB_PASSWORD"
        )
    _db_connection = {
        "instance": parts[0],
        "db": parts[1],
        "user": parts[2],
        "password": parts[3],
    }
    return _db_connection


def get_connection():
    cfg = _parse_db_secret()
    connector = Connector()
    return connector.connect(
        cfg["instance"],
        "pymysql",
        user=cfg["user"],
        password=cfg["password"],
        db=cfg["db"],
    )


@app.route("/")
def health():
    return jsonify({"status": "ok"})


@app.route("/account-id")
def account_id():
    wallet_id = request.args.get("wallet_id")
    environment = request.args.get("environment")

    if not wallet_id:
        return jsonify({"error": "Missing wallet_id"}), 400
    if not environment:
        return jsonify({"error": "Missing environment"}), 400
    if environment not in ALLOWED_ENVIRONMENTS:
        return jsonify({"error": f"Invalid environment. Allowed: {sorted(ALLOWED_ENVIRONMENTS)}"}), 400

    schema = f"{environment}_wallet"
    table = "modulr_wallets"

    try:
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT account_id FROM `{schema}`.`{table}` WHERE wallet_id = %s",
                    (wallet_id,),
                )
                row = cursor.fetchone()
        finally:
            conn.close()

        if row:
            return jsonify({"account_id": row[0]})
        return jsonify({"error": "No account found for wallet_id"}), 404

    except pymysql.Error as e:
        return jsonify({"error": str(e)}), 500
