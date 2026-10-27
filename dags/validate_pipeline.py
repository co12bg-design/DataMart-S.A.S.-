"""
DataMart S.A.S. - Validador del Data Warehouse.

Comprueba la conexión, las tablas, los registros cargados,
las claves duplicadas, los precios y el resumen de ventas.

Este script es de solo lectura: no modifica datos.
"""

import os
import sys

import psycopg2
from psycopg2 import sql


REQUIRED_TABLES = [
    "dim_product",
    "fact_transactions",
    "rejected_records",
]


def get_connection():
    """Conecta con PostgreSQL usando variables de entorno."""
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_DB_HOST", "warehouse-db"),
        port=int(os.getenv("WAREHOUSE_DB_PORT", "5432")),
        dbname=os.getenv("WAREHOUSE_DB_NAME", "datamart"),
        user=os.getenv("WAREHOUSE_DB_USER", "warehouse"),
        password=os.getenv("WAREHOUSE_DB_PASSWORD", ""),
        connect_timeout=10,
    )


def table_exists(cursor, table_name):
    """Verifica si una tabla existe en el esquema analytics."""
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'analytics'
              AND table_name = %s
        )
        """,
        (table_name,),
    )
    return cursor.fetchone()[0]


def count_rows(cursor, table_name):
    """Cuenta las filas de una tabla permitida."""
    query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
        sql.Identifier("analytics"),
        sql.Identifier(table_name),
    )
    cursor.execute(query)
    return cursor.fetchone()[0]


def check_query(cursor, name, query):
    """Ejecuta una comprobación que devuelve un conteo de problemas."""
    cursor.execute(query)
    count = cursor.fetchone()[0]
    passed = count == 0

    print(
        f"[{'PASS' if passed else 'FAIL'}] {name}: "
        f"{count:,} registros con problemas."
    )
    return passed


def main():
    """Ejecuta todas las validaciones y genera un informe."""
    print("=" * 65)
    print("DATAMART S.A.S. - VALIDACIÓN DEL DATA WAREHOUSE")
    print("=" * 65)

    try:
        connection = get_connection()
    except Exception as error:
        print(f"[FAIL] Conexión a PostgreSQL: {error}")
        return 1

    checks = []

    try:
        with connection.cursor() as cursor:
            print("[PASS] Conexión a PostgreSQL establecida.")

            missing = [
                table
                for table in REQUIRED_TABLES
                if not table_exists(cursor, table)
            ]

            if missing:
                print(
                    "[FAIL] Faltan tablas en analytics: "
                    + ", ".join(missing)
                )
                return 1

            print("[PASS] Existen las tres tablas requeridas.")

            # Conteo de registros de cada tabla.
            for table in REQUIRED_TABLES:
                count = count_rows(cursor, table)
                print(f"[INFO] analytics.{table}: {count:,} registros.")

                if table != "rejected_records":
                    checks.append(count > 0)
                    print(
                        f"[{'PASS' if count > 0 else 'FAIL'}] "
                        f"{table} contiene datos."
                    )

            # Verificar claves de transacción nulas.
            checks.append(
                check_query(
                    cursor,
                    "Claves de transacción no nulas",
                    """
                    SELECT COUNT(*)
                    FROM analytics.fact_transactions
                    WHERE transaction_key IS NULL
                    """,
                )
            )

            # Verificar claves repetidas.
            checks.append(
                check_query(
                    cursor,
                    "Claves de transacción únicas",
                    """
                    SELECT COUNT(*)
                    FROM (
                        SELECT transaction_key
                        FROM analytics.fact_transactions
                        GROUP BY transaction_key
                        HAVING COUNT(*) > 1
                    ) AS duplicated
                    """,
                )
            )

            # Verificar precios válidos.
            checks.append(
                check_query(
                    cursor,
                    "Precios positivos y no nulos",
                    """
                    SELECT COUNT(*)
                    FROM analytics.fact_transactions
                    WHERE unit_price IS NULL OR unit_price <= 0
                    """,
                )
            )

            # Verificar coherencia de ingresos por fila.
            checks.append(
                check_query(
                    cursor,
                    "Cálculo coherente de ingresos netos",
                    """
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
                )
            )

            # Resumen para el informe.
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

            total, sales, returns, gross, return_amount, net = (
                cursor.fetchone()
            )

            print("\nRESUMEN DEL DATA WAREHOUSE")
            print(f"Transacciones:       {total:,}")
            print(f"Ventas:              {sales:,}")
            print(f"Devoluciones:        {returns:,}")
            print(f"Ingreso bruto:       {gross:,.2f}")
            print(f"Valor devoluciones:  {return_amount:,.2f}")
            print(f"Ingreso neto:        {net:,.2f}")

    except Exception as error:
        print(f"\n[FAIL] Error durante la validación: {error}")
        return 1
    finally:
        connection.close()

    print("\n" + "=" * 65)

    if all(checks):
        print("RESULTADO: TODAS LAS VALIDACIONES APROBADAS.")
        return 0

    print("RESULTADO: HAY VALIDACIONES FALLIDAS.")
    return 1


if __name__ == "__main__":
    sys.exit(main())