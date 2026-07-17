import os
import azure.functions as func
import pandas as pd
import io
import json
import logging
import time
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

CONTAINER_NAME = "datasets"
BLOB_NAME = "All_Diets.csv"


def load_dataframe():
    """Connects to Azure Blob Storage, downloads the CSV, and returns a cleaned DataFrame."""
    connect_str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)

    stream = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(stream))

    df["Diet_type"] = df["Diet_type"].str.lower().str.strip()
    df["Protein(g)"] = df["Protein(g)"].fillna(df["Protein(g)"].mean())
    df["Carbs(g)"] = df["Carbs(g)"].fillna(df["Carbs(g)"].mean())
    df["Fat(g)"] = df["Fat(g)"].fillna(df["Fat(g)"].mean())

    return df


@app.route(route="insights", methods=["GET"])
def insights(req: func.HttpRequest) -> func.HttpResponse:
    """Returns average macronutrients per diet type (feeds Bar Chart + Heatmap)."""
    start = time.time()
    try:
        df = load_dataframe()
        avg_macros = df.groupby("Diet_type")[["Protein(g)", "Carbs(g)", "Fat(g)"]].mean()
        result = avg_macros.reset_index().to_dict(orient="records")

        response = {
            "data": result,
            "execution_time_ms": round((time.time() - start) * 1000, 2)
        }
        return func.HttpResponse(json.dumps(response), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error(f"Error in insights: {e}")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)


@app.route(route="recipes", methods=["GET"])
def recipes(req: func.HttpRequest) -> func.HttpResponse:
    """Returns top 5 protein-rich recipes per diet type (feeds Pie Chart)."""
    start = time.time()
    try:
        df = load_dataframe()
        top_protein = (
            df.sort_values("Protein(g)", ascending=False)
            .groupby("Diet_type")
            .head(5)[["Diet_type", "Recipe_name", "Protein(g)", "Cuisine_type"]]
        )
        result = top_protein.to_dict(orient="records")

        response = {
            "data": result,
            "execution_time_ms": round((time.time() - start) * 1000, 2)
        }
        return func.HttpResponse(json.dumps(response), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error(f"Error in recipes: {e}")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)


@app.route(route="clusters", methods=["GET"])
def clusters(req: func.HttpRequest) -> func.HttpResponse:
    """Returns protein-by-cuisine data points for the top 50 protein-rich recipes (feeds Scatter Plot)."""
    start = time.time()
    try:
        df = load_dataframe()
        top50 = df.sort_values("Protein(g)", ascending=False).head(50)
        result = top50[["Cuisine_type", "Protein(g)", "Diet_type"]].to_dict(orient="records")

        response = {
            "data": result,
            "execution_time_ms": round((time.time() - start) * 1000, 2)
        }
        return func.HttpResponse(json.dumps(response), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error(f"Error in clusters: {e}")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)