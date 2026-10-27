# Decisiones técnicas — DataMarket S.A.S.

## Arquitectura
- **Airflow 2.10.5** ejecuta un DAG diario con `LocalExecutor`; Docker Compose administra los contenedores.
- **Dos PostgreSQL separados**: `airflow-db` guarda metadatos de orquestación; `warehouse-db` guarda el modelo analítico.
- **Esquema `analytics`**: `dim_product` contiene el código, la descripción canónica y categoría; `fact_transactions` conserva cada línea de factura y distingue ventas/devoluciones; `rejected_records` guarda el origen, motivo y payload rechazado. La vista `v_daily_product_revenue` facilita el análisis diario por producto.
- La API opcional de productos no se implementó. La categoría se asigna con reglas heurísticas explícitas y una categoría `Uncategorized`; debe reemplazarse por un catálogo maestro cuando DataMart lo provea.

## Casos ambiguos
1. **Customer ID nulo:** se conservan las transacciones y `customer_id` queda SQL `NULL`; no se inventa una identidad. Las consultas separan clientes identificados y anónimos.
2. **Descripción canónica:** se eliminan espacios externos y se compactan espacios; para elegir el nombre canónico se convierte a mayúsculas, se selecciona la descripción más frecuente y, en empate, la alfabéticamente primera. Se conserva la descripción observada en el hecho para auditar variaciones.
3. **Duplicados entre datasets:** se genera SHA-256 a partir de factura, código normalizado, cantidad, precio, fecha UTC, cliente y país. La primera ocurrencia se conserva. Es una clave compuesta reproducible; puede colisionar conceptualmente si ambos archivos reutilizan los mismos identificadores para operaciones distintas, por lo que debe validarse con el propietario del dato.
4. **Cantidad menor o igual a cero:** se clasifica como `RETURN`. Las ventas tienen `gross_revenue = quantity * unit_price`; las devoluciones tienen `return_amount = abs(quantity * unit_price)`; `net_revenue = gross_revenue - return_amount`. Las devoluciones no se suman como ventas brutas.
5. **Precio no positivo, fecha inválida, cantidad no entera, factura o código ausente:** la fila se rechaza y queda registrada en `rejected_records` con el motivo y el payload. La regla de precio positivo se aplica a todas las líneas, incluyendo devoluciones, porque el precio unitario describe el valor del artículo y la cantidad ya determina el signo.
6. **Fechas:** se parsean con reglas por fuente y se convierten a timestamps UTC. La fuente 1 usa interpretación mes/día/año y la fuente 2 día/mes/año; los registros ambiguos deben revisarse contra los originales.
7. **Monedas:** los datasets de Kaggle son de comercio minorista del Reino Unido y no proporcionan una moneda por fila ni tasas de conversión. Por ello, el pipeline no inventa una conversión COP/MXN/PEN; los importes sólo son comparables dentro de la moneda/unidad de la fuente. En una implementación real se debe añadir moneda y tabla de tasas de cambio.

## Idempotencia y operación
- `transaction_key` es determinista y la tabla de hechos usa `ON CONFLICT DO NOTHING`.
- Los productos se actualizan por `product_code`; los rechazos se insertan con clave hash y `ON CONFLICT DO NOTHING`.
- `max_active_runs=1`, dos reintentos y demora de tres minutos reducen ejecuciones simultáneas y toleran fallos transitorios.
- La conexión `warehouse_postgres` y las variables operativas se crean en `airflow-init`; no se incluyen credenciales en el código.

## Limitaciones conocidas
- El repositorio incluye seeds pequeños para validar el entorno sin credenciales de Kaggle. Para análisis final se deben colocar los dos CSV completos en `data/raw/`.
- La categoría es heurística y no reemplaza el catálogo de 3.000 referencias.
- La deduplicación es por clave compuesta exacta; no elimina registros similares que difieran en fecha, precio o cantidad.
- Docker debe estar instalado en la máquina de ejecución. No fue posible verificar el arranque real de contenedores en el entorno donde se preparó este paquete porque no hay Docker CLI disponible.
