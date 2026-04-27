import os
from functools import wraps

import pymysql
import requests as http
from google.cloud.sql.connector import Connector, IPTypes
from flask import Flask, jsonify, request, session

app = Flask(__name__)

# Required env vars:
#   INSTANCE_CONNECTION_NAME - PROJECT:REGION:INSTANCE
#   DB_USER                  - Cloud SQL IAM DB user (SA email sans
#                              ".gserviceaccount.com" for MySQL)
#   SECRET_KEY               - Random secret for signing session cookies.
#                              Generate with: python3 -c "import secrets; print(secrets.token_hex(32))"
#                              Set via Cloud Run --set-env-vars or Secret Manager.
# Optional:
#   DB_IP_TYPE               - "PUBLIC" (default) or "PRIVATE"
#
# Authentication uses Application Default Credentials for DB access:
#   - Locally:     `gcloud auth application-default login`
#   - On Cloud Run: the attached service account

_ALLOWED_DOMAIN = "getground.co.uk"
_GOOGLE_TOKENINFO_URL = "https://oauth2.googleapis.com/tokeninfo"

app.config.update(
    SECRET_KEY=os.environ.get("SECRET_KEY", "dev-only-change-in-production"),
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="None",
)

INSTANCE_CONNECTION_NAME = "terranova-staging-shared:europe-west1:terranova-staging-shared-mysql"
DB_USER = "wallywally-service"

ALLOWED_ENVIRONMENTS = frozenset({
    "apl_staging",
    "cas_staging",
    "preview",
    *(f"staging-{i:02d}_staging" for i in range(1, 21)),
    *(f"preview-{i:02d}" for i in range(1, 21)),
})

WALLET_SUFFIX = "_wallet"
EPH_PREVIEW_WALLET_SUFFIX = "-wallet"

_connector: Connector | None = None


def _get_connector() -> Connector:
    global _connector
    if _connector is None:
        _connector = Connector()
    return _connector


def get_connection():
    ip_type = IPTypes[os.environ.get("DB_IP_TYPE", "PUBLIC").upper()]
    return _get_connector().connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",
        user=DB_USER,
        enable_iam_auth=True,
        ip_type=ip_type,
    )


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "email" not in session:
            return jsonify({"error": "Unauthenticated"}), 401
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Auth endpoints
# ---------------------------------------------------------------------------

@app.route("/auth/login", methods=["POST"])
def auth_login():
    data = request.get_json(silent=True) or {}
    access_token = data.get("access_token")
    if not access_token:
        return jsonify({"error": "Missing access_token"}), 400

    try:
        resp = http.get(
            _GOOGLE_TOKENINFO_URL,
            params={"access_token": access_token},
            timeout=5,
        )
        resp.raise_for_status()
    except http.exceptions.RequestException:
        return jsonify({"error": "Could not verify token"}), 401

    info = resp.json()

    if info.get("email_verified") != "true":
        return jsonify({"error": "Email not verified"}), 401

    email = info.get("email", "")
    if not email.endswith(f"@{_ALLOWED_DOMAIN}"):
        return jsonify({"error": "Unauthorized domain"}), 403

    session["email"] = email
    return jsonify({"email": email})


@app.route("/auth/me")
def auth_me():
    email = session.get("email")
    if not email:
        return jsonify({"error": "Unauthenticated"}), 401
    return jsonify({"email": email})


@app.route("/auth/logout", methods=["POST"])
def auth_logout():
    session.clear()
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# App endpoints
# ---------------------------------------------------------------------------

@app.route("/")
def health():
    return jsonify({"status": "ok"})


@app.route("/account-id")
@require_auth
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

    schema = None
    if environment.startswith("preview-"):
        schema = f"{environment}{EPH_PREVIEW_WALLET_SUFFIX}"
    else:
        schema = f"{environment}{WALLET_SUFFIX}"
    if not schema:
        return jsonify({"error": "Invalid environment"}), 400

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
