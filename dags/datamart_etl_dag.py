"""
DataMart S.A.S. - Pipeline ETL con Apache Airflow TaskFlow API.

Flujo:
    1. Extraer dos fuentes CSV.
    2. Transformar, normalizar y deduplicar los registros.
    3. Validar la calidad de los datos.
    4. Cargar productos, transacciones y rechazos en PostgreSQL.
    5. Validar el Data Warehouse después de la carga.

Tecnologías:
    - Python 3.11
    - Apache Airflow 2.10.5
    - pandas
    - PostgreSQL
    - psycopg2

La carga utiliza claves únicas para evitar duplicados cuando se
vuelve a ejecutar el pipeline, siempre que existan las restricciones
correspondientes en PostgreSQL.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pendulum
import psycopg2
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from psycopg2 import sql
from psycopg2.extras import Json, execute_values

from etl_pipeline import (
    load_csv,
    resolve_input,
    transform_sources,
)


# =====================================================================
# CONFIGURACIÓN GENERAL
# =====================================================================

LOG = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data")
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"

WAREHOUSE_SCHEMA = "analytics"

REQUIRED_TABLES = {
    "dim_product",
    "fact_transactions",
    "rejected_records",
}

ALLOWED_TRANSACTION_TYPES = {"SALE", "RETURN"}

# Esquemas esperados para los datos intermedios.
PRODUCT_COLUMNS = [
    "product_code",
    "canonical_description",
    "category",
]

FACT_COLUMNS = [
    "transaction_key",
    "invoice_no",
    "product_code",
    "description",
    "quantity",
    "unit_price",
    "transaction_ts",
    "customer_id",
    "country",
    "source_name",
    "transaction_type",
    "gross_revenue",
    "return_amount",
    "net_revenue",
]

REJECT_COLUMNS = [
    "rejection_key",
    "source_name",
    "reason",
    "record_payload",
]


# =====================================================================
# FUNCIONES AUXILIARES
# =====================================================================

def get_warehouse_connection():
    """
    Abre una conexión a PostgreSQL usando la conexión de Airflow.

    La conexión warehouse_postgres debe tener configurados el host,
    el puerto, la base de datos, el usuario y la contraseña.
    """
    config = BaseHook.get_connection("warehouse_postgres")

    if not config.host or not config.login or not config.password:
        raise ValueError(
            "La conexión warehouse_postgres no tiene todas "
            "las credenciales necesarias."
        )

    if not config.schema:
        raise ValueError(
            "La conexión warehouse_postgres no tiene una base de datos "
            "configurada en el campo Schema."
        )

    return psycopg2.connect(
        host=config.host,
        port=config.port or 5432,
        dbname=config.schema,
        user=config.login,
        password=config.password,
        connect_timeout=10,
    )


def is_missing(value: Any) -> bool:
    """Comprueba si un valor escalar es nulo sin alterar diccionarios."""
    if value is None:
        return True

    if isinstance(value, (dict, list, tuple)):
        return False

    try:
        result = pd.isna(value)
        return bool(result)
    except (TypeError, ValueError):
        return False


def json_safe(value: Any) -> Any:
    """
    Convierte valores de pandas y NumPy a tipos serializables en JSON.

    Los valores ausentes se convierten en None y los timestamps se
    convierten a texto ISO para conservar la información de auditoría.
    """
    if is_missing(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if hasattr(value, "item"):
        try:
            return json_safe(value.item())
        except (TypeError, ValueError):
            pass

    if isinstance(value, dict):
        return {
            str(key): json_safe(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]

    return value


def stable_record_key(payload: dict[str, Any]) -> str:
    """Genera una clave SHA-256 determinista para un registro rechazado."""
    serialized = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    )

    return hashlib.sha256(
        serialized.encode("utf-8")
    ).hexdigest()


def read_json_dataframe(path: str) -> pd.DataFrame:
    """Lee un archivo JSON de staging y valida que contenga registros."""
    file_path = Path(path)

    if not file_path.is_file():
        raise FileNotFoundError(
            f"No se encontró el archivo intermedio: {file_path}"
        )

    dataframe = pd.read_json(
        file_path,
        orient="records",
    )

    return dataframe


def require_columns(
    dataframe: pd.DataFrame,
    required_columns: list[str],
    dataframe_name: str,
) -> None:
    """Detiene el pipeline si faltan columnas obligatorias."""
    missing = set(required_columns) - set(dataframe.columns)

    if missing:
        raise ValueError(
            f"El DataFrame '{dataframe_name}' no contiene "
            f"las columnas requeridas: {sorted(missing)}"
        )


# =====================================================================
# DEFINICIÓN DEL DAG
# =====================================================================

@dag(
    dag_id="datamart_daily_etl",
    description=(
        "ETL diario de fuentes CSV hacia el Data Warehouse PostgreSQL"
    ),
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["datamart", "etl", "postgresql", "data-quality"],
)
def datamart_daily_etl():
    """Define las tareas y dependencias del proceso ETL."""

    # -----------------------------------------------------------------
    # TAREA 1: EXTRAER LA PRIMERA FUENTE
    # -----------------------------------------------------------------

    @task
    def extract_source_one() -> str:
        """Extrae el CSV principal y lo guarda en staging."""
        STAGING_DIR.mkdir(parents=True, exist_ok=True)

        configured_path = Variable.get(
            "source_one_path",
            default_var=str(RAW_DIR / "data.csv"),
        )

        source_path = resolve_input(
            configured_path,
            str(DATA_DIR / "seeds" / "data.csv"),
        )

        dataframe = load_csv(
            source_path,
            source="ecommerce_data",
        )

        output_path = STAGING_DIR / "source_one.csv"
        dataframe.to_csv(output_path, index=False)

        LOG.info(
            "Fuente 1 extraída: %s filas desde %s",
            len(dataframe),
            source_path,
        )

        return str(output_path)

    # -----------------------------------------------------------------
    # TAREA 2: EXTRAER LA SEGUNDA FUENTE
    # -----------------------------------------------------------------

    @task
    def extract_source_two() -> str:
        """Localiza el segundo CSV y lo guarda en staging."""
        STAGING_DIR.mkdir(parents=True, exist_ok=True)

        configured_path = Variable.get(
            "source_two_path",
            default_var=str(RAW_DIR / "online_retail.csv"),
        )

        candidates = [
            configured_path,
            str(RAW_DIR / "online_retail.csv"),
            str(RAW_DIR / "online_retail_II.csv"),
            str(DATA_DIR / "seeds" / "online_retail_II.csv"),
        ]

        source_path = next(
            (
                candidate
                for candidate in candidates
                if Path(candidate).is_file()
            ),
            None,
        )

        if source_path is None:
            raise FileNotFoundError(
                "No se encontró el segundo CSV. "
                f"Rutas revisadas: {candidates}"
            )

        dataframe = load_csv(
            source_path,
            source="online_retail_ii",
        )

        output_path = STAGING_DIR / "source_two.csv"
        dataframe.to_csv(output_path, index=False)

        LOG.info(
            "Fuente 2 extraída: %s filas desde %s",
            len(dataframe),
            source_path,
        )

        return str(output_path)

    # -----------------------------------------------------------------
    # TAREA 3: TRANSFORMAR Y PREPARAR LOS DATOS
    # -----------------------------------------------------------------

    @task
    def transform_data(
        path_one: str,
        path_two: str,
    ) -> dict[str, str]:
        """
        Transforma ambas fuentes y genera archivos intermedios.

        La función transform_sources se encarga de normalizar las
        columnas, validar registros y deduplicar las transacciones.
        """
        STAGING_DIR.mkdir(parents=True, exist_ok=True)

        source_one = pd.read_csv(path_one, dtype=str)
        source_two = pd.read_csv(path_two, dtype=str)

        facts, products, rejects = transform_sources(
            source_one,
            source_two,
        )

        # -------------------------------------------------------------
        # Adaptar la dimensión de productos al esquema de PostgreSQL.
        # -------------------------------------------------------------

        # La transformación genera "description"; la dimensión espera
        # "canonical_description".
        products = products.rename(
            columns={"description": "canonical_description"}
        )

        # Asegurar las columnas del esquema incluso si no hay productos.
        for column in PRODUCT_COLUMNS:
            if column not in products.columns:
                products[column] = pd.NA

        # La base de datos no admite categorías nulas. Como las fuentes
        # actuales no proporcionan una categoría, se usa un marcador.
        products["category"] = (
            products["category"]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
            .fillna("UNCLASSIFIED")
        )

        products = products[PRODUCT_COLUMNS]

        # -------------------------------------------------------------
        # Preparar claves y payloads de los registros rechazados.
        # -------------------------------------------------------------

        if rejects.empty:
            # Conservar las columnas aunque no haya rechazos.
            rejects = pd.DataFrame(columns=REJECT_COLUMNS)

        else:
            reject_records = []

            for record in rejects.to_dict(orient="records"):
                # El payload conserva los datos originales del registro
                # y la razón por la cual no fue aceptado.
                payload = {
                    str(key): json_safe(value)
                    for key, value in record.items()
                }

                # Normalizar la razón de rechazo para la auditoría.
                reason = payload.get("reason")

                if not reason:
                    reason = payload.get("rejection_reason")

                if not reason:
                    reason = "unspecified_rejection"

                source_name = payload.get("source_name")

                # La clave no incluye campos generados durante la carga.
                # Así se obtiene una huella estable del rechazo.
                rejection_key = stable_record_key(
                    {
                        "source_name": source_name,
                        "reason": reason,
                        "record_payload": payload,
                    }
                )

                payload_json = json.dumps(
                    payload,
                    sort_keys=True,
                    ensure_ascii=False,
                    default=str,
                )

                reject_records.append(
                    {
                        "rejection_key": rejection_key,
                        "source_name": source_name,
                        "reason": reason,
                        "record_payload": payload_json,
                    }
                )

            rejects = pd.DataFrame(
                reject_records,
                columns=REJECT_COLUMNS,
            )

        # -------------------------------------------------------------
        # Guardar los DataFrames transformados como JSON.
        # -------------------------------------------------------------

        outputs = {
            "facts": STAGING_DIR / "facts.json",
            "products": STAGING_DIR / "products.json",
            "rejects": STAGING_DIR / "rejects.json",
        }

        facts.to_json(
            outputs["facts"],
            orient="records",
            date_format="iso",
        )

        products.to_json(
            outputs["products"],
            orient="records",
        )

        rejects.to_json(
            outputs["rejects"],
            orient="records",
        )

        LOG.info(
            "Transformación completada: %s transacciones, "
            "%s productos y %s rechazos.",
            len(facts),
            len(products),
            len(rejects),
        )

        return {
            name: str(path)
            for name, path in outputs.items()
        }

    # -----------------------------------------------------------------
    # TAREA 4: VALIDACIÓN DE CALIDAD ANTES DE LA CARGA
    # -----------------------------------------------------------------

    @task
    def validate_quality(
        paths: dict[str, str],
    ) -> dict[str, str]:
        """
        Valida las transacciones antes de escribir en PostgreSQL.

        Si alguna regla falla, Airflow marca la tarea como fallida y
        evita que se ejecute la carga posterior.
        """
        facts = read_json_dataframe(paths["facts"])

        if facts.empty:
            raise ValueError(
                "No hay transacciones válidas para cargar."
            )

        require_columns(
            facts,
            FACT_COLUMNS,
            "facts",
        )

        # Las claves deben existir y ser únicas en el lote.
        if facts["transaction_key"].isna().any():
            raise ValueError(
                "Hay transacciones sin transaction_key."
            )

        if facts["transaction_key"].duplicated().any():
            raise ValueError(
                "Hay transaction_key duplicadas en el lote."
            )

        # Los precios de venta y devolución deben ser positivos.
        prices = pd.to_numeric(
            facts["unit_price"],
            errors="coerce",
        )

        if prices.isna().any() or (prices <= 0).any():
            raise ValueError(
                "Hay precios nulos, inválidos o no positivos."
            )

        # Verificar los tipos de transacción permitidos.
        if facts["transaction_type"].isna().any():
            raise ValueError(
                "Hay transacciones sin tipo."
            )

        actual_types = set(
            facts["transaction_type"].unique()
        )

        invalid_types = (
            actual_types - ALLOWED_TRANSACTION_TYPES
        )

        if invalid_types:
            raise ValueError(
                "Se encontraron tipos de transacción no permitidos: "
                f"{sorted(invalid_types)}"
            )

        # Verificar la coherencia aritmética de los ingresos.
        gross = pd.to_numeric(
            facts["gross_revenue"],
            errors="coerce",
        )

        return_amount = pd.to_numeric(
            facts["return_amount"],
            errors="coerce",
        )

        net = pd.to_numeric(
            facts["net_revenue"],
            errors="coerce",
        )

        if (
            gross.isna().any()
            or return_amount.isna().any()
            or net.isna().any()
        ):
            raise ValueError(
                "Hay valores inválidos en las columnas de ingresos."
            )

        reconciled = (
            net - (gross - return_amount)
        ).abs() <= 0.01

        if not reconciled.all():
            raise ValueError(
                "Los ingresos netos no coinciden con "
                "ingresos brutos menos devoluciones."
            )

        LOG.info(
            "Validación previa aprobada: %s transacciones.",
            len(facts),
        )

        return paths

    # -----------------------------------------------------------------
    # TAREA 5: CARGAR LOS DATOS EN POSTGRESQL
    # -----------------------------------------------------------------

    @task
    def load_warehouse(
        paths: dict[str, str],
    ) -> None:
        """
        Carga productos, transacciones y rechazos en PostgreSQL.

        Los productos se actualizan por product_code.
        Las transacciones existentes no se insertan otra vez.
        Los rechazos se protegen mediante rejection_key.
        """
        facts = read_json_dataframe(paths["facts"])
        products = read_json_dataframe(paths["products"])
        rejects = read_json_dataframe(paths["rejects"])

        # Comprobar la estructura antes de abrir la transacción SQL.
        require_columns(
            products,
            PRODUCT_COLUMNS,
            "products",
        )

        require_columns(
            rejects,
            REJECT_COLUMNS,
            "rejects",
        )

        if not facts.empty:
            require_columns(
                facts,
                FACT_COLUMNS,
                "facts",
            )

        # Garantizar una categoría válida incluso si el archivo
        # intermedio contiene valores nulos.
        if not products.empty:
            products["category"] = (
                products["category"]
                .astype("string")
                .str.strip()
                .replace("", pd.NA)
                .fillna("UNCLASSIFIED")
            )

        # Configurar el tamaño de los lotes de inserción.
        batch_size = int(
            Variable.get(
                "batch_size",
                default_var="5000",
            )
        )

        if batch_size < 1:
            raise ValueError(
                "La variable batch_size debe ser mayor que cero."
            )

        connection = get_warehouse_connection()

        try:
            # El contexto SQL confirma los cambios al terminar y hace
            # rollback si ocurre una excepción dentro de la transacción.
            with connection:
                with connection.cursor() as cursor:

                    # -------------------------------------------------
                    # Cargar la dimensión de productos.
                    # -------------------------------------------------

                    if not products.empty:
                        product_rows = [
                            tuple(
                                None if is_missing(value) else value
                                for value in row
                            )
                            for row in products[
                                PRODUCT_COLUMNS
                            ].itertuples(
                                index=False,
                                name=None,
                            )
                        ]

                        execute_values(
                            cursor,
                            """
                            INSERT INTO analytics.dim_product (
                                product_code,
                                canonical_description,
                                category
                            ) VALUES %s
                            ON CONFLICT (product_code) DO UPDATE SET
                                canonical_description =
                                    EXCLUDED.canonical_description,
                                category = EXCLUDED.category
                            """,
                            product_rows,
                            page_size=batch_size,
                        )

                    # -------------------------------------------------
                    # Cargar transacciones sin duplicar claves.
                    # -------------------------------------------------

                    if not facts.empty:
                        fact_rows = [
                            tuple(
                                None if is_missing(value) else value
                                for value in row
                            )
                            for row in facts[
                                FACT_COLUMNS
                            ].itertuples(
                                index=False,
                                name=None,
                            )
                        ]

                        execute_values(
                            cursor,
                            """
                            INSERT INTO analytics.fact_transactions (
                                transaction_key,
                                invoice_no,
                                product_code,
                                description,
                                quantity,
                                unit_price,
                                transaction_ts,
                                customer_id,
                                country,
                                source_name,
                                transaction_type,
                                gross_revenue,
                                return_amount,
                                net_revenue
                            ) VALUES %s
                            ON CONFLICT (transaction_key) DO NOTHING
                            """,
                            fact_rows,
                            page_size=batch_size,
                        )

                    # -------------------------------------------------
                    # Guardar los registros rechazados para auditoría.
                    # -------------------------------------------------

                    if not rejects.empty:
                        reject_rows = []

                        # Usar diccionarios evita problemas con los
                        # nombres de campos de pandas.itertuples().
                        for record in rejects.to_dict(
                            orient="records"
                        ):
                            payload = record["record_payload"]

                            # El JSON puede llegar como texto desde
                            # staging o como un objeto ya decodificado.
                            if isinstance(payload, str):
                                payload = json.loads(payload)

                            if not isinstance(payload, dict):
                                raise ValueError(
                                    "record_payload debe contener "
                                    "un objeto JSON."
                                )

                            rejection_key = record["rejection_key"]
                            source_name = record["source_name"]
                            reason = record["reason"]

                            if is_missing(rejection_key):
                                raise ValueError(
                                    "Se encontró un rechazo sin "
                                    "rejection_key."
                                )

                            if is_missing(reason):
                                reason = "unspecified_rejection"

                            if is_missing(source_name):
                                source_name = "unknown_source"

                            reject_rows.append(
                                (
                                    rejection_key,
                                    source_name,
                                    reason,
                                    Json(payload),
                                )
                            )

                        execute_values(
                            cursor,
                            """
                            INSERT INTO analytics.rejected_records (
                                rejection_key,
                                source_name,
                                reason,
                                record_payload
                            ) VALUES %s
                            ON CONFLICT (rejection_key) DO NOTHING
                            """,
                            reject_rows,
                            page_size=batch_size,
                        )

            LOG.info(
                "Carga completada: %s transacciones procesadas, "
                "%s productos y %s rechazos.",
                len(facts),
                len(products),
                len(rejects),
            )

        finally:
            connection.close()

    # -----------------------------------------------------------------
    # TAREA 6: VALIDACIÓN FINAL DEL DATA WAREHOUSE
    # -----------------------------------------------------------------

    @task
    def validate_warehouse() -> None:
        """
        Comprueba directamente los datos almacenados en PostgreSQL.

        Esta tarea es de solo lectura. Si encuentra inconsistencias,
        falla y Airflow no marca el DAG como completamente exitoso.
        """
        connection = get_warehouse_connection()

        try:
            with connection.cursor() as cursor:

                # Verificar que las tablas requeridas existan.
                cursor.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_name = ANY(%s)
                    """,
                    (
                        WAREHOUSE_SCHEMA,
                        list(REQUIRED_TABLES),
                    ),
                )

                existing_tables = {
                    row[0]
                    for row in cursor.fetchall()
                }

                missing_tables = (
                    REQUIRED_TABLES - existing_tables
                )

                if missing_tables:
                    raise ValueError(
                        "Faltan tablas en el esquema analytics: "
                        f"{sorted(missing_tables)}"
                    )

                LOG.info(
                    "Todas las tablas requeridas existen."
                )

                # Cada consulta debe devolver cero problemas.
                checks = {
                    "claves nulas": """
                        SELECT COUNT(*)
                        FROM analytics.fact_transactions
                        WHERE transaction_key IS NULL
                    """,
                    "claves duplicadas": """
                        SELECT COUNT(*)
                        FROM (
                            SELECT transaction_key
                            FROM analytics.fact_transactions
                            GROUP BY transaction_key
                            HAVING COUNT(*) > 1
                        ) AS duplicates
                    """,
                    "precios inválidos": """
                        SELECT COUNT(*)
                        FROM analytics.fact_transactions
                        WHERE unit_price IS NULL
                           OR unit_price <= 0
                    """,
                    "ingresos inconsistentes": """
                        SELECT COUNT(*)
                        FROM analytics.fact_transactions
                        WHERE gross_revenue IS NULL
                           OR return_amount IS NULL
                           OR net_revenue IS NULL
                           OR ABS(
                               net_revenue -
                               (gross_revenue - return_amount)
                           ) > 0.01
                    """,
                }

                for description, query in checks.items():
                    cursor.execute(query)
                    problem_count = cursor.fetchone()[0]

                    if problem_count != 0:
                        raise ValueError(
                            f"Validación fallida: {description}. "
                            f"Registros con problemas: {problem_count}."
                        )

                    LOG.info(
                        "Validación aprobada: %s.",
                        description,
                    )

                # Registrar los conteos finales de las tablas.
                for table in sorted(REQUIRED_TABLES):
                    query = sql.SQL(
                        "SELECT COUNT(*) FROM {}.{}"
                    ).format(
                        sql.Identifier(WAREHOUSE_SCHEMA),
                        sql.Identifier(table),
                    )

                    cursor.execute(query)
                    row_count = cursor.fetchone()[0]

                    LOG.info(
                        "Conteo final: %s.%s = %s registros.",
                        WAREHOUSE_SCHEMA,
                        table,
                        row_count,
                    )

                # Resumir ventas, devoluciones e ingresos.
                cursor.execute(
                    """
                    SELECT
                        COUNT(*),
                        COUNT(*) FILTER (
                            WHERE transaction_type = 'SALE'
                        ),
                        COUNT(*) FILTER (
                            WHERE transaction_type = 'RETURN'
                        ),
                        COALESCE(SUM(gross_revenue), 0),
                        COALESCE(SUM(return_amount), 0),
                        COALESCE(SUM(net_revenue), 0)
                    FROM analytics.fact_transactions
                    """
                )

                (
                    total,
                    sales,
                    returns,
                    gross,
                    return_amount,
                    net,
                ) = cursor.fetchone()

                LOG.info("RESUMEN FINAL DEL DATA WAREHOUSE")
                LOG.info("Transacciones: %s", f"{total:,}")
                LOG.info("Ventas: %s", f"{sales:,}")
                LOG.info("Devoluciones: %s", f"{returns:,}")
                LOG.info("Ingreso bruto: %s", f"{gross:,.2f}")
                LOG.info(
                    "Valor de devoluciones: %s",
                    f"{return_amount:,.2f}",
                )
                LOG.info("Ingreso neto: %s", f"{net:,.2f}")

                LOG.info(
                    "VALIDACIÓN FINAL APROBADA: "
                    "Data Warehouse consistente."
                )

        finally:
            connection.close()

    # -----------------------------------------------------------------
    # DEPENDENCIAS DEL PIPELINE
    # -----------------------------------------------------------------

    # Las dos fuentes se extraen en paralelo.
    source_one = extract_source_one()
    source_two = extract_source_two()

    # Transformación y validación antes de la carga.
    transformed = transform_data(
        source_one,
        source_two,
    )

    validated = validate_quality(transformed)

    # La validación final se ejecuta únicamente después de la carga.
    loaded = load_warehouse(validated)
    final_validation = validate_warehouse()

    loaded >> final_validation


# Registrar el DAG para que Airflow lo descubra.
datamart_daily_etl()