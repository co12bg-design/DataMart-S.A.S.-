"""Crea un .env local con secretos aleatorios sin sobrescribir uno existente."""
from __future__ import annotations
import base64
import os
import secrets
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
example = ROOT / ".env.example"
target = ROOT / ".env"
if target.exists():
    raise SystemExit("Ya existe .env; no se modificó para evitar sobrescribir secretos.")
text = example.read_text(encoding="utf-8")
replacements = {
    "change_me_airflow_db": secrets.token_urlsafe(24),
    "REPLACE_WITH_GENERATED_FERNET_KEY": base64.urlsafe_b64encode(os.urandom(32)).decode("ascii"),
    "replace_with_long_random_secret": secrets.token_urlsafe(48),
    "change_me_admin": secrets.token_urlsafe(18),
    "change_me_warehouse": secrets.token_urlsafe(24),
}
for old, new in replacements.items():
    text = text.replace(old, new)
target.write_text(text, encoding="utf-8")
try:
    target.chmod(0o600)
except OSError:
    pass
print("Archivo .env creado con secretos aleatorios. No lo subas a Git.")
print("Usuario administrador de Airflow: admin")
print("La contraseña se encuentra en .env (AIRFLOW_ADMIN_PASSWORD).")
