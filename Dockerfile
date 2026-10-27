FROM apache/airflow:2.10.5-python3.11

# Copiar las dependencias del proyecto.
COPY requirements.txt /requirements.txt

# Instalar como usuario airflow, sin usar --user.
RUN pip install --no-cache-dir -r /requirements.txt