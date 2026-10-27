# Guía paso a paso: qué construye el proyecto y cómo comprobarlo

## Paso 1 — Aislar los servicios
Docker Compose levanta dos bases PostgreSQL distintas. `airflow-db` pertenece a Airflow y no guarda las ventas. `warehouse-db` es el destino analítico. Esta separación evita mezclar tablas internas del orquestador con las del negocio.

## Paso 2 — Inicializar sin configuración manual
`airflow-init` espera a que ambas bases estén saludables. Después migra el esquema interno de Airflow, crea el administrador, registra `warehouse_postgres` y crea las variables de ruta y lote. Las contraseñas se leen de `.env`, archivo ignorado por Git.

## Paso 3 — Extraer cada fuente por separado
`extract_source_one` lee el CSV de ecommerce y `extract_source_two` lee Online Retail II. Se guardan copias de trabajo en el volumen compartido `data/staging`. Si faltan los archivos completos, se usan los seeds para probar el flujo.

## Paso 4 — Unificar columnas y normalizar
`etl_pipeline.py` convierte los distintos nombres de columnas a un esquema común. Normaliza códigos de producto a mayúsculas y elimina espacios, limpia espacios en descripciones, conserva `Customer ID` nulo como SQL NULL y convierte fechas a UTC. La interpretación de fecha depende de la fuente porque los CSV no usan el mismo formato.

## Paso 5 — Rechazar lo que no puede analizarse
Una fila se rechaza si no tiene código o factura, si la cantidad no es un entero, si la fecha no puede interpretarse o si el precio unitario no es positivo. Cada rechazo conserva fuente, motivo y payload original normalizado para auditoría.

## Paso 6 — Separar ventas y devoluciones
Cantidad positiva equivale a `SALE`; cantidad menor o igual a cero equivale a `RETURN`. El valor de ventas se registra en `gross_revenue`; el importe absoluto de la devolución, en `return_amount`; `net_revenue` es ventas menos devoluciones. Las agregaciones por producto y día producen el neto del periodo.

## Paso 7 — Deduplicar y elegir descripción canónica
Se calcula una clave SHA-256 a partir de atributos de negocio reproducibles. La primera aparición de una clave se conserva. Para cada código se selecciona la descripción más frecuente después de normalizarla a mayúsculas; los empates se resuelven por orden alfabético. El hecho conserva la descripción observada para poder detectar inconsistencias.

## Paso 8 — Validar antes de cargar
La tarea `validate_data_quality` comprueba que haya registros válidos, que los precios sean positivos, que las claves no se repitan y que los tipos de transacción sean reconocidos. Si falla una regla, la carga no continúa.

## Paso 9 — Cargar de forma idempotente
La dimensión de productos se actualiza por código. Las transacciones y los rechazos usan `ON CONFLICT DO NOTHING` con claves deterministas. Repetir la misma carga no duplica hechos. `max_active_runs=1` evita que dos ejecuciones del DAG compitan entre sí.

## Paso 10 — Validar el resultado
Primero verifica que las tareas del DAG terminen en verde. Después consulta los conteos de hechos y rechazos. Finalmente ejecuta `sql/queries.sql`, que incluye consultas para evolución mensual, categorías, ranking de productos, países y ticket promedio, clientes anónimos, descripciones inconsistentes y códigos únicos.

## Qué queda pendiente antes de entregar a un evaluador
1. Colocar los dos CSV completos de Kaggle en `data/raw/`.
2. Levantar el stack en una máquina con Docker y conservar capturas o logs de ejecución satisfactoria.
3. Ejecutar las consultas con los datasets completos y redactar una recomendación de producto citando los valores obtenidos. No se debe presentar una recomendación cuantitativa basada en los seeds, porque son datos sintéticos de prueba.
4. Confirmar la convención exacta de fechas y el significado de los identificadores con el responsable del dataset si hay registros ambiguos.
