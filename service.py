import os

import pymysql
from google.cloud.sql.connector import Connector, IPTypes
from flask import Flask, jsonify, request

app = Flask(__name__)

INSTANCE_CONNECTION_NAME = "terranova-staging-shared:europe-west1:terranova-staging-shared-mysql"
DB_USER = "wallywally-service"

ALLOWED_ENVIRONMENTS = frozenset({
    "apl_staging",
    "cas_staging",
    "preview",
    *(f"staging-{i:02d}" for i in range(1, 21)),
    *(f"preview-{i:02d}" for i in range(1, 21)),
})

# Required constants:
#   INSTANCE_CONNECTION_NAME - PROJECT:REGION:INSTANCE
#   DB_USER                  - Cloud SQL IAM DB user (SA email sans
#                              ".gserviceaccount.com" for MySQL)
# Optional:
#   DB_IP_TYPE               - "PUBLIC" (default) or "PRIVATE"
#
# Authentication uses Application Default Credentials:
#   - Locally:     `gcloud auth application-default login` (optionally with
#                  --impersonate-service-account=<DB_USER>@...gserviceaccount.com)
#   - On Cloud Run: the attached service account
_connector: Connector | None = None


def _get_connector() -> Connector:
    global _connector
    if _connector is None:
        _connector = Connector()
    return _connector


def get_connection():
    instance = INSTANCE_CONNECTION_NAME
    user = DB_USER
    ip_type = IPTypes[os.environ.get("DB_IP_TYPE", "PUBLIC").upper()]
    return _get_connector().connect(
        instance,
        "pymysql",
        user=user,
        enable_iam_auth=True,
        ip_type=ip_type,
    )


@app.route("/")
def health():
    return jsonify({"status": "ok"})


# Get the account ID for a given wallet ID and environment
#
# Example:
#   GET /account-id?wallet_id=3&environment=apl_staging
#
# Returns:
#   { "account_id": "A2100GSHXP" }
#
# Errors:
#   - 400: Missing wallet_id or environment
#   - 404: No account found for wallet_id
#   - 500: Internal server error
#
# Environment examples:
#   - apl_staging
#   - cas_staging
#   - preview
#   - staging-02
#   - staging-14
#   - preview-04
#   - preview-11
#
@app.route("/account-id")
def account_id():
    wallet_id_raw = request.args.get("wallet_id")
    environment = request.args.get("environment")

    if not wallet_id_raw:
        return jsonify({"error": "Missing wallet_id"}), 400
    if not environment:
        return jsonify({"error": "Missing environment"}), 400

    try:
        wallet_id = int(wallet_id_raw)
    except ValueError:
        return jsonify({"error": "wallet_id must be an integer"}), 400

    if environment not in ALLOWED_ENVIRONMENTS:
        return jsonify({"error": "Invalid environment"}), 400

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

    except pymysql.Error:
        app.logger.exception("DB error")
        return jsonify({"error": "Internal server error"}), 500
