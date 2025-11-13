import json
import logging
import os
import uuid

import boto3
import requests
from bs4 import BeautifulSoup

DYNAMO_TABLE = os.environ.get("TABLE_NAME")
URL_IGP = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

dynamodb = boto3.resource("dynamodb")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; IGP-Scraper/1.0; +https)"
}


def scrape_last_earthquakes(limit=10):
    """
    Scrapea los últimos sismos del IGP con la estructura real del HTML.
    """

    res = requests.get(URL_IGP, headers=HEADERS, timeout=20)
    res.raise_for_status()

    soup = BeautifulSoup(res.text, "html.parser")

    # Toma todas las filas de la tabla (cada tr es un sismo)
    rows = soup.select("table tbody tr")

    earthquakes = []

    for row in rows[:limit]:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue  # evita filas vacías

        reporte = cols[0].get_text(strip=True)
        referencia = cols[1].get_text(strip=True)
        fecha_hora = cols[2].get_text(strip=True)
        magnitud = cols[3].get_text(strip=True)

        earthquakes.append({
            "id": reporte or str(uuid.uuid4()),  # pk principal
            "reporte": reporte,
            "referencia": referencia,
            "fecha_hora_local": fecha_hora,
            "magnitud": magnitud,
        })

    return earthquakes


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
