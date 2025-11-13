import json
import logging
import os
import uuid
from datetime import datetime

import boto3
import requests
from bs4 import BeautifulSoup

DYNAMO_TABLE = os.environ.get("TABLE_NAME")
URL_IGP = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
API_SISMOS_URL = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/sismos"

dynamodb = boto3.resource("dynamodb")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; IGP-Scraper/1.0; +https)",
    "Accept": "application/json, text/html;q=0.9",
}


def _format_fecha_local(iso_str):
    if not iso_str:
        return None

    cleaned = iso_str.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1]

    try:
        dt = datetime.fromisoformat(cleaned.replace("T", " "))
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except ValueError:
        return iso_str


def _normalize_api_item(item):
    if not item:
        return None

    codigo = (item.get("codigo") or "").strip()

    return {
        "id": codigo or str(uuid.uuid4()),
        "reporte": codigo,
        "referencia": item.get("referencia"),
        "fecha_hora_local": _format_fecha_local(item.get("fecha_hora")),
        "magnitud": str(item.get("magnitud")) if item.get("magnitud") is not None else None,
        "profundidad_km": item.get("profundidad"),
        "latitud": item.get("latitud"),
        "longitud": item.get("longitud"),
        "tipo_evento": item.get("tipo_evento"),
        "numero": item.get("numero"),
        "simulacro": item.get("simulacro"),
        "created_at": item.get("created_at"),
    }


def _fetch_from_api(limit):
    try:
        res = requests.get(API_SISMOS_URL, headers=HEADERS, timeout=20)
        res.raise_for_status()
        payload = res.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("No se pudo obtener la API de sismos: %s", exc)
        return []

    earthquakes = []
    for row in payload[:limit]:
        normalized = _normalize_api_item(row)
        if normalized:
            earthquakes.append(normalized)

    if not earthquakes:
        logger.warning("La API de sismos devolvió una lista vacía.")

    return earthquakes


def _fetch_from_html(limit):
    res = requests.get(URL_IGP, headers=HEADERS, timeout=20)
    res.raise_for_status()

    soup = BeautifulSoup(res.text, "html.parser")

    rows = soup.select("table tbody tr")
    earthquakes = []

    for row in rows[:limit]:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue

        reporte = cols[0].get_text(strip=True)
        referencia = cols[1].get_text(strip=True)
        fecha_hora = cols[2].get_text(strip=True)
        magnitud = cols[3].get_text(strip=True)

        earthquakes.append({
            "id": reporte or str(uuid.uuid4()),
            "reporte": reporte,
            "referencia": referencia,
            "fecha_hora_local": fecha_hora,
            "magnitud": magnitud,
        })

    if not earthquakes:
        logger.warning("No se encontraron filas en la tabla HTML del IGP.")

    return earthquakes


def scrape_last_earthquakes(limit=10):
    """Obtiene los últimos sismos usando primero la API pública y luego HTML como respaldo."""

    earthquakes = _fetch_from_api(limit)
    if earthquakes:
        return earthquakes

    logger.info("Usando fallback HTML para obtener sismos recientes.")
    return _fetch_from_html(limit)


def store_in_dynamodb(earthquakes, table_name=None):
    """Inserta los sismos en DynamoDB."""
    if not earthquakes:
        logger.info("No hay sismos para insertar en DynamoDB.")
        return

    table_name = table_name or DYNAMO_TABLE
    if not table_name:
        raise RuntimeError("TABLE_NAME no está configurado en las variables de entorno.")

    table = dynamodb.Table(table_name)
    with table.batch_writer() as batch:
        for item in earthquakes:
            batch.put_item(Item=item)


def lambda_handler(event, context):
    """Endpoint público: scrapea e inserta en DynamoDB."""

    try:
        earthquakes = scrape_last_earthquakes()

        store_in_dynamodb(earthquakes)

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps({
                "count": len(earthquakes),
                "items": earthquakes
            }, ensure_ascii=False),
        }

    except Exception as e:
        logger.exception("Fallo al procesar la solicitud")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
