import json
import requests
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]

OUTPUT_DIR = BASE_DIR / "data" / "geospatial"

OUTPUT_FILE = OUTPUT_DIR / "indonesia_provinces.geojson"

METADATA_FILE = OUTPUT_DIR / "boundary_metadata.json"


def download_boundaries():

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_FILE.exists() and METADATA_FILE.exists():
        print("Boundary dataset already exists.")
        return OUTPUT_FILE

    api_url = (
        "https://www.geoboundaries.org/"
        "api/current/gbOpen/IDN/ADM1/"
    )

    response = requests.get(api_url, timeout=60)
    response.raise_for_status()

    metadata = response.json()

    download_url = metadata["gjDownloadURL"]

    response = requests.get(download_url, timeout=180)
    response.raise_for_status()

    geojson = response.json()

    if geojson.get("type") != "FeatureCollection":
        raise ValueError("Invalid GeoJSON dataset.")

    OUTPUT_FILE.write_text(
        json.dumps(geojson, ensure_ascii=False),
        encoding="utf-8"
    )

    METADATA_FILE.write_text(
        json.dumps(metadata, indent=4),
        encoding="utf-8"
    )

    print("Boundary dataset downloaded.")
    print("Total provinces:", len(geojson["features"]))
    print("Saved to:", OUTPUT_FILE)

    return OUTPUT_FILE


if __name__ == "__main__":
    download_boundaries()