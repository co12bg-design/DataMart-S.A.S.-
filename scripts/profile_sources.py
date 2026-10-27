"""Perfilado inicial de CSV: columnas, tipos, nulos, duplicados y muestras.

Uso desde la raíz del repositorio:
    python scripts/profile_sources.py
Los resultados se guardan en reports/ para documentar la exploración previa.
"""
from __future__ import annotations

import json
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
INPUTS = [ROOT / "data/raw/data.csv", ROOT / "data/raw/online_retail.csv"]
OUTPUT = ROOT / "reports"


def profile(path: Path) -> dict:
    """Carga un CSV como texto para no alterar IDs ni códigos de producto."""
    if not path.exists():
        return {"file": str(path.relative_to(ROOT)), "status": "MISSING", "message": "Descarga el dataset y colócalo en data/raw/."}
    df = pd.read_csv(path, dtype=str, keep_default_na=True, encoding_errors="replace", low_memory=False)
    return {
        "file": str(path.relative_to(ROOT)),
        "status": "OK",
        "rows": int(len(df)),
        "columns": list(df.columns),
        "dtypes": {str(k): str(v) for k, v in df.dtypes.items()},
        "nulls": {str(k): int(v) for k, v in df.isna().sum().items()},
        "exact_duplicate_rows": int(df.duplicated().sum()),
        "sample": df.head(5).fillna("").to_dict(orient="records"),
    }


def main() -> None:
    OUTPUT.mkdir(exist_ok=True)
    results = [profile(path) for path in INPUTS]
    target = OUTPUT / "source_profile.json"
    target.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    for result in results:
        print(f"\n## {result['file']} — {result['status']}")
        if result["status"] == "OK":
            print(f"Rows: {result['rows']} | exact duplicates: {result['exact_duplicate_rows']}")
            print("Columns:", ", ".join(result["columns"]))
            print("Nulls:", result["nulls"])
        else:
            print(result["message"])
    print(f"\nDetailed report: {target.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
