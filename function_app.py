import os
import json
import logging
import time
import io
import hmac
import hashlib
import urllib.parse
from datetime import datetime, timezone, timedelta

import azure.functions as func
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.data.tables import TableServiceClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
import requests
import redis

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

CONTAINER_NAME = "datasets"
BLOB_NAME = "All_Diets.csv"
CLEANED_BLOB_NAME = "All_Diets_clean.csv"

USERS_TABLE_NAME = "Users"
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = 24
FRONTEND_URL = "https://ambitious-river-07f0e800f.7.azurestaticapps.net"
GITHUB_AUTHORIZE_URL = "https://github.com/login/oauth/authorize"
GITHUB_TOKEN_URL = "https://github.com/login/oauth/access_token"
GITHUB_USER_URL = "https://api.github.com/user"
GITHUB_USER_EMAILS_URL = "https://api.github.com/user/emails"


# ---------------------------------------------------------------------------
# Redis client (reused across warm invocations of the Function App)
# ---------------------------------------------------------------------------
_redis_client = None


def get_redis_client():
    global _redis_client
    if _redis_client is None:
        host = os.environ["REDIS_HOST"]
        password = os.environ["REDIS_KEY"]
        _redis_client = redis.Redis(
            host=host,
            port=6380,
            password=password,
            ssl=True,
            decode_responses=True,
        )
    return _redis_client


def read_cache(key: str):
    r = get_redis_client()
    cached = r.get(key)
    if cached is None:
        return None
    return json.loads(cached)


# ---------------------------------------------------------------------------
# Auth: table storage, password hashing, JWT sessions, OAuth state signing
# ---------------------------------------------------------------------------
_table_service_client = None


def get_users_table_client():
    global _table_service_client
    if _table_service_client is None:
        connect_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        _table_service_client = TableServiceClient.from_connection_string(connect_str)
    return _table_service_client.get_table_client(USERS_TABLE_NAME)


def normalize_email(email: str) -> str:
    return email.strip().lower()


def get_user_by_email(email: str):
    table = get_users_table_client()
    try:
        return table.get_entity(partition_key="user", row_key=normalize_email(email))
    except ResourceNotFoundError:
        return None


def create_user_entity(email: str, display_name: str, password_hash: str, auth_provider: str, github_id: str = ""):
    table = get_users_table_client()
    entity = {
        "PartitionKey": "user",
        "RowKey": normalize_email(email),
        "DisplayName": display_name,
        "PasswordHash": password_hash,
        "AuthProvider": auth_provider,
        "GitHubId": github_id,
        "CreatedAt": datetime.now(timezone.utc).isoformat(),
    }
    table.upsert_entity(entity)
    return entity


def issue_jwt(email: str, display_name: str) -> str:
    secret = os.environ["JWT_SECRET"]
    now = datetime.now(timezone.utc)
    payload = {
        "sub": normalize_email(email),
        "name": display_name,
        "iat": now,
        "exp": now + timedelta(hours=JWT_EXPIRY_HOURS),
    }
    return jwt.encode(payload, secret, algorithm=JWT_ALGORITHM)


def get_authenticated_user(req: func.HttpRequest):
    """Returns the decoded JWT payload if the request has a valid Bearer token, else None."""
    auth_header = req.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    token = auth_header[len("Bearer "):]
    try:
        secret = os.environ["JWT_SECRET"]
        return jwt.decode(token, secret, algorithms=[JWT_ALGORITHM])
    except jwt.PyJWTError as e:
        logging.warning(f"JWT verification failed: {e}")
        return None


def require_auth_response(req: func.HttpRequest):
    """Returns an HttpResponse(401) if unauthenticated, else None (meaning: proceed)."""
    user = get_authenticated_user(req)
    if user is None:
        return func.HttpResponse(
            json.dumps({"error": "Unauthorized — please log in."}),
            mimetype="application/json", status_code=401)
    return None


def make_oauth_state() -> str:
    """Self-contained, signed CSRF token for the OAuth redirect — no server-side session needed."""
    secret = os.environ["JWT_SECRET"]
    ts = str(int(time.time()))
    sig = hmac.new(secret.encode(), ts.encode(), hashlib.sha256).hexdigest()
    return f"{ts}.{sig}"


def verify_oauth_state(state: str, max_age_seconds: int = 600) -> bool:
    try:
        secret = os.environ["JWT_SECRET"]
        ts, sig = state.split(".")
        expected_sig = hmac.new(secret.encode(), ts.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected_sig):
            return False
        return (time.time() - int(ts)) <= max_age_seconds
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Blob helpers
# ---------------------------------------------------------------------------
def get_blob_service_client():
    connect_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    return BlobServiceClient.from_connection_string(connect_str)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df["Diet_type"] = df["Diet_type"].str.lower().str.strip()
    df["Protein(g)"] = df["Protein(g)"].fillna(df["Protein(g)"].mean())
    df["Carbs(g)"] = df["Carbs(g)"].fillna(df["Carbs(g)"].mean())
    df["Fat(g)"] = df["Fat(g)"].fillna(df["Fat(g)"].mean())
    return df


def save_cleaned_csv(df: pd.DataFrame):
    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=CLEANED_BLOB_NAME)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    blob_client.upload_blob(csv_bytes, overwrite=True)


def compute_and_cache_results(df: pd.DataFrame):
    """Runs ONCE per file change (called only from the blob trigger)."""
    r = get_redis_client()

    # Insights: avg macros per diet type (Bar Chart + Heatmap)
    avg_macros = df.groupby("Diet_type")[["Protein(g)", "Carbs(g)", "Fat(g)"]].mean()
    insights_result = avg_macros.reset_index().to_dict(orient="records")

    # Recipes: top 5 protein-rich recipes per diet type (Pie Chart)
    top_protein = (
        df.sort_values("Protein(g)", ascending=False)
        .groupby("Diet_type")
        .head(5)[["Diet_type", "Recipe_name", "Protein(g)", "Cuisine_type"]]
    )
    recipes_result = top_protein.to_dict(orient="records")

    # Clusters: protein-by-cuisine for top 50 protein-rich recipes (Scatter Plot)
    top50 = df.sort_values("Protein(g)", ascending=False).head(50)
    clusters_result = top50[["Cuisine_type", "Protein(g)", "Diet_type"]].to_dict(orient="records")

    # Full cleaned dataset, used by the /search endpoint (filter + keyword + pagination)
    all_recipes = df[
        ["Recipe_name", "Diet_type", "Cuisine_type", "Protein(g)", "Carbs(g)", "Fat(g)"]
    ].to_dict(orient="records")

    computed_at = datetime.now(timezone.utc).isoformat()

    r.set("cache:insights", json.dumps(insights_result))
    r.set("cache:recipes", json.dumps(recipes_result))
    r.set("cache:clusters", json.dumps(clusters_result))
    r.set("cache:all_recipes", json.dumps(all_recipes))
    r.set("cache:computed_at", computed_at)

    logging.info(f"[CACHE REFRESH] Recalculated all results at {computed_at} — {len(df)} rows processed.")


# ---------------------------------------------------------------------------
# Blob trigger — fires ONLY when All_Diets.csv itself changes
# ---------------------------------------------------------------------------
@app.blob_trigger(
    arg_name="myblob",
    path=f"{CONTAINER_NAME}/{BLOB_NAME}",
    connection="AZURE_STORAGE_CONNECTION_STRING",
)
def on_diet_csv_changed(myblob: func.InputStream):
    logging.info(f"[BLOB TRIGGER] {myblob.name} changed ({myblob.length} bytes) — starting data cleaning.")
    start = time.time()

    raw_bytes = myblob.read()
    df = pd.read_csv(io.BytesIO(raw_bytes))
    df = clean_dataframe(df)

    save_cleaned_csv(df)
    compute_and_cache_results(df)

    elapsed_ms = round((time.time() - start) * 1000, 2)
    logging.info(f"[BLOB TRIGGER] Cleaning + caching complete in {elapsed_ms} ms.")


# ---------------------------------------------------------------------------
# HTTP endpoints — read from cache ONLY, never recompute
# ---------------------------------------------------------------------------
@app.route(route="insights", methods=["GET"])
def insights(req: func.HttpRequest) -> func.HttpResponse:
    start = time.time()
    try:
        auth_error = require_auth_response(req)
        if auth_error:
            return auth_error

        data = read_cache("cache:insights")
        if data is None:
            return func.HttpResponse(
                json.dumps({"error": "No cached data yet — upload All_Diets.csv to trigger processing."}),
                mimetype="application/json", status_code=404)
        response = {"data": data, "source": "cache", "execution_time_ms": round((time.time() - start) * 1000, 2)}
        return func.HttpResponse(json.dumps(response), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error(f"Error in insights: {e}")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)


@app.route(route="recipes", methods=["GET"])
def recipes(req: func.HttpRequest) -> func.HttpResponse:
    start = time.time()
    try:
        auth_error = require_auth_response(req)
        if auth_error:
            return auth_error

        data = read_cache("cache:recipes")
        if data is None:
            return func.HttpResponse(
                json.dumps({"error": "No cached data yet — upload All_Diets.csv to trigger processing."}),
                mimetype="application/json", status_code=404)
        response = {"data": data, "source": "cache", "execution_time_ms": round((time.time() - start) * 1000, 2)}
        return func.HttpResponse(json.dumps(response), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error(f"Error in recipes: {e}")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)


@app.route(route="clusters", methods=["GET"])
def clusters(req: func.HttpRequest) -> func.HttpResponse:
    start = time.time()
    try:
        auth_error = require_auth_response(req)
        if auth_error:
            return auth_error

        data = read_cache("cache:clusters")
        if data is None:
            return func.HttpResponse(
                json.dumps({"error": "No cached data yet — upload All_Diets.csv to trigger processing."}),
                mimetype="application/json", status_code=404)
        response = {"data": data, "source": "cache", "execution_time_ms": round((time.time() - start) * 1000, 2)}
        return func.HttpResponse(json.dumps(response), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error(f"Error in clusters: {e}")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)


# ---------------------------------------------------------------------------
# New: search / filter / paginate recipes
# ---------------------------------------------------------------------------
@app.route(route="search", methods=["GET"])
def search(req: func.HttpRequest) -> func.HttpResponse:
    start = time.time()
    try:
        auth_error = require_auth_response(req)
        if auth_error:
            return auth_error

        all_recipes = read_cache("cache:all_recipes")
        if all_recipes is None:
            return func.HttpResponse(
                json.dumps({"error": "No cached data yet — upload All_Diets.csv to trigger processing."}),
                mimetype="application/json", status_code=404)

        diet_type = (req.params.get("diet_type") or "").strip().lower()
        keyword = (req.params.get("keyword") or "").strip().lower()
        page = max(int(req.params.get("page", 1)), 1)
        page_size = min(max(int(req.params.get("page_size", 10)), 1), 100)

        results = all_recipes
        if diet_type and diet_type != "all":
            results = [r for r in results if r["Diet_type"] == diet_type]
        if keyword:
            results = [r for r in results if keyword in r["Recipe_name"].lower()]

        total_count = len(results)
        total_pages = max((total_count + page_size - 1) // page_size, 1)
        start_idx = (page - 1) * page_size
        page_results = results[start_idx:start_idx + page_size]

        response = {
            "data": page_results,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_count": total_count,
                "total_pages": total_pages,
            },
            "source": "cache",
            "execution_time_ms": round((time.time() - start) * 1000, 2),
        }
        return func.HttpResponse(json.dumps(response), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error(f"Error in search: {e}")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)

# ---------------------------------------------------------------------------
# Auth endpoints: register, login, GitHub OAuth
# ---------------------------------------------------------------------------
@app.route(route="register", methods=["POST"])
def register(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        email = normalize_email(body.get("email", ""))
        password = body.get("password", "")
        display_name = (body.get("name") or email.split("@")[0]).strip()

        if not email or "@" not in email:
            return func.HttpResponse(json.dumps({"error": "A valid email is required."}),
                                      mimetype="application/json", status_code=400)
        if len(password) < 8:
            return func.HttpResponse(json.dumps({"error": "Password must be at least 8 characters."}),
                                      mimetype="application/json", status_code=400)

        existing = get_user_by_email(email)
        if existing is not None:
            return func.HttpResponse(json.dumps({"error": "An account with this email already exists."}),
                                      mimetype="application/json", status_code=409)

        password_hash = generate_password_hash(password)
        create_user_entity(email, display_name, password_hash, auth_provider="local")

        token = issue_jwt(email, display_name)
        return func.HttpResponse(
            json.dumps({"token": token, "name": display_name, "email": email}),
            mimetype="application/json", status_code=201)
    except Exception as e:
        logging.error(f"Error in register: {e}")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)


@app.route(route="login", methods=["POST"])
def login(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        email = normalize_email(body.get("email", ""))
        password = body.get("password", "")

        user = get_user_by_email(email)
        if user is None or user.get("AuthProvider") != "local" or not user.get("PasswordHash"):
            return func.HttpResponse(json.dumps({"error": "Invalid email or password."}),
                                      mimetype="application/json", status_code=401)

        if not check_password_hash(user["PasswordHash"], password):
            return func.HttpResponse(json.dumps({"error": "Invalid email or password."}),
                                      mimetype="application/json", status_code=401)

        token = issue_jwt(email, user["DisplayName"])
        return func.HttpResponse(
            json.dumps({"token": token, "name": user["DisplayName"], "email": email}),
            mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error(f"Error in login: {e}")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)


@app.route(route="oauth/github/login", methods=["GET"])
def oauth_github_login(req: func.HttpRequest) -> func.HttpResponse:
    client_id = os.environ["GITHUB_CLIENT_ID"]
    redirect_uri = f"https://{req.url.split('/')[2]}/api/oauth/github/callback"
    state = make_oauth_state()

    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": "read:user user:email",
        "state": state,
    }
    authorize_url = f"{GITHUB_AUTHORIZE_URL}?{urllib.parse.urlencode(params)}"

    return func.HttpResponse(status_code=302, headers={"Location": authorize_url})


@app.route(route="oauth/github/callback", methods=["GET"])
def oauth_github_callback(req: func.HttpRequest) -> func.HttpResponse:
    try:
        code = req.params.get("code")
        state = req.params.get("state", "")

        if not code or not verify_oauth_state(state):
            return func.HttpResponse(
                json.dumps({"error": "Invalid or expired OAuth state."}),
                mimetype="application/json", status_code=400)

        client_id = os.environ["GITHUB_CLIENT_ID"]
        client_secret = os.environ["GITHUB_CLIENT_SECRET"]
        redirect_uri = f"https://{req.url.split('/')[2]}/api/oauth/github/callback"

        token_res = requests.post(
            GITHUB_TOKEN_URL,
            headers={"Accept": "application/json"},
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "code": code,
                "redirect_uri": redirect_uri,
            },
            timeout=10,
        )
        token_data = token_res.json()
        access_token = token_data.get("access_token")
        if not access_token:
            logging.error(f"GitHub token exchange failed: {token_data}")
            return func.HttpResponse(
                json.dumps({"error": "GitHub authentication failed."}),
                mimetype="application/json", status_code=400)

        gh_headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/vnd.github+json"}
        user_res = requests.get(GITHUB_USER_URL, headers=gh_headers, timeout=10)
        gh_user = user_res.json()

        email = gh_user.get("email")
        if not email:
            emails_res = requests.get(GITHUB_USER_EMAILS_URL, headers=gh_headers, timeout=10)
            for e in emails_res.json():
                if e.get("primary"):
                    email = e.get("email")
                    break
        if not email:
            email = f"{gh_user['login']}@users.noreply.github.com"

        display_name = gh_user.get("name") or gh_user.get("login")

        create_user_entity(
            email, display_name, password_hash="",
            auth_provider="github", github_id=str(gh_user.get("id", "")),
        )

        token = issue_jwt(email, display_name)
        redirect_params = urllib.parse.urlencode({"token": token, "name": display_name, "email": email})
        return func.HttpResponse(
            status_code=302,
            headers={"Location": f"{FRONTEND_URL}/#{redirect_params}"},
        )
    except Exception as e:
        logging.error(f"Error in oauth_github_callback: {e}")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)