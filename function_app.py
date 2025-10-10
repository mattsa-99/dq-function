import json
import logging
import os
from datetime import datetime

import azure.functions as func
from pydantic import ValidationError

from models import DataContractRequest, build_yaml
from google import genai  # SDK nuevo

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "OPTIONS, POST, GET",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
}


# ------------------- YAML -------------------
@app.function_name(name="generate_contract")
@app.route(route="generate_contract", methods=["OPTIONS", "POST"])
def generate_contract(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return func.HttpResponse(status_code=204, headers=CORS_HEADERS)

    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse(
            body=json.dumps({"error": "Invalid or empty JSON body"}),
            mimetype="application/json",
            headers=CORS_HEADERS,
            status_code=400,
        )

    try:
        model = DataContractRequest.model_validate(payload)
    except ValidationError as ve:
        return func.HttpResponse(
            body=ve.json(),
            mimetype="application/json",
            headers=CORS_HEADERS,
            status_code=422,
        )

    yaml_text = build_yaml(model)
    return func.HttpResponse(
        body=yaml_text,
        mimetype="text/yaml",
        headers={**CORS_HEADERS, "Content-Disposition": 'attachment; filename=\"data_contract.yaml\"'},
        status_code=200,
    )

# ------------------- GEMINI: sugerencias -------------------
@app.function_name(name="suggest_metadata")
@app.route(route="suggest_metadata", methods=["OPTIONS", "POST"])
def suggest_metadata(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return func.HttpResponse(status_code=204, headers=CORS_HEADERS)

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return func.HttpResponse(
            body=json.dumps({"error": "Missing GEMINI_API_KEY app setting"}),
            mimetype="application/json",
            headers=CORS_HEADERS,
            status_code=500,
        )

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            body=json.dumps({"error": "Invalid JSON"}),
            mimetype="application/json",
            headers=CORS_HEADERS,
            status_code=400,
        )

    csv_text = (body or {}).get("csv_text", "")
    table_name = (body or {}).get("table_name", "unknown_table")

    if not csv_text.strip():
        return func.HttpResponse(
            body=json.dumps({"error": "csv_text is required"}),
            mimetype="application/json",
            headers=CORS_HEADERS,
            status_code=400,
        )

    # Limitar tamaño para costo/latencia
    csv_short = csv_text[: 64 * 1024]

    # Cliente Gemini (SDK nuevo)
    client = genai.Client(api_key=api_key)

    # Construimos un “schema ejemplo” en JSON con json.dumps para evitar problemas de llaves en f-strings
    schema_example = {
        "table_name": table_name,
        "table_description": "…",
        "columns": [
            {
                "name": "col_name",
                "description": "…",
                "suggested_type": "string|int|float|boolean|timestamp|date|email|id|category|currency|json|unknown",
                "nullable": True,
            }
        ],
    }
    schema_json = json.dumps(schema_example, ensure_ascii=False, indent=2)

    prompt = (
        "You are a data documentation assistant.\n"
        "Given a CSV sample, infer:\n"
        "- a short Spanish description of the table's business meaning,\n"
        "- for each column, a short Spanish description and a probable semantic type\n"
        '  ("string","int","float","boolean","timestamp","date","email","id","category","currency","json","unknown").\n'
        "Return valid JSON ONLY with this structure (keys/types as example; replace ellipses):\n\n"
        f"{schema_json}\n\n"
        "CSV SAMPLE:\n"
        "```csv\n"
        f"{csv_short}\n"
        "```\n"
    )

    try:
        resp = client.models.generate_content(
            model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
            contents=prompt,
            config={"response_mime_type": "application/json"},
        )
        data = json.loads(resp.text)
    except Exception as e:
        logging.exception("Gemini generate/parse error")
        return func.HttpResponse(
            body=json.dumps({"error": f"Gemini error: {str(e)}"}),
            mimetype="application/json",
            headers=CORS_HEADERS,
            status_code=500,
        )

    return func.HttpResponse(
        body=json.dumps(data, ensure_ascii=False),
        mimetype="application/json",
        headers=CORS_HEADERS,
        status_code=200,
    )
