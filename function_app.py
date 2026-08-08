import os
import json
import logging
import time
import io
from datetime import datetime, timezone

import azure.functions as func
import pandas as pd
from azure.storage.blob import BlobServiceClient
import redis

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

CONTAINER_NAME = "datasets"
BLOB_NAME = "All_Diets.csv"
CLEANED_BLOB_NAME = "All_Diets_clean.csv"


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