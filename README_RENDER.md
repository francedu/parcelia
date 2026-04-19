# Despliegue en Render

## 1) Sube este proyecto a GitHub
No subas la base SQLite ni la carpeta `venv`.

## 2) Crea el despliegue en Render
Tienes dos opciones:

### Opción A: Blueprint (recomendada)
1. En Render, entra a **New > Blueprint**.
2. Conecta tu repositorio.
3. Render detectará `render.yaml` y creará:
   - un **Web Service**
   - una base **PostgreSQL** gestionada
4. Antes de desplegar, define `ADMIN_PASSWORD` con una clave segura.
5. Haz deploy.

### Opción B: Manual
1. **New > PostgreSQL**
2. **New > Web Service**
3. Configura:
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `gunicorn app:app --bind 0.0.0.0:$PORT --workers ${WEB_CONCURRENCY:-2} --timeout 120`
   - Health Check Path: `/healthz`
4. Variables de entorno:
   - `DATABASE_URL` = cadena de conexión de tu Postgres de Render
   - `SECRET_KEY` = una clave larga y aleatoria
   - `APP_DEBUG` = `0`
   - `APP_HOST` = `0.0.0.0`
   - `ADMIN_USER` = `admin`
   - `ADMIN_PASSWORD` = una clave segura
   - `ADMIN_NAME` = `Administrador`

## 3) Importar tus datos actuales a la base de Render
### Desde tu computador
1. Copia la cadena `DATABASE_URL` de Render.
2. En tu terminal local, dentro de este proyecto:

```bash
export DATABASE_URL='postgresql://...'
python3 -m pip install -r requirements.txt
python3 migrate_sqlite_to_postgres.py
```

Eso copiará los datos desde `instance/contabilidad_condominio.db` a PostgreSQL.

## 4) Login inicial
Si la tabla `usuarios` está vacía, la app crea automáticamente el admin usando:
- `ADMIN_USER`
- `ADMIN_PASSWORD`
- `ADMIN_NAME`

Si ya migraste tus usuarios desde SQLite/PostgreSQL, se respetarán esos datos.

## 5) Importante
- No uses SQLite en Render para producción.
- Los archivos locales del servicio son efímeros. La carpeta `backups/` no debe considerarse respaldo permanente.
- Usa siempre PostgreSQL para el entorno público.


## Checklist de producción

- Configura `SECRET_KEY` segura y única.
- Define `ADMIN_PASSWORD` manualmente; no dejes valores por defecto.
- Usa PostgreSQL administrado y valida `DATABASE_URL`.
- Mantén `APP_DEBUG=0` en producción.
- Verifica que `TRUST_PROXY=1` si Render está delante de la app.
- No subas `.venv`, `venv`, `__pycache__` ni archivos `.db` reales al repositorio.
- Revisa `/healthz` después del deploy para confirmar conectividad con base de datos.

- En local o pruebas por HTTP, deja `SESSION_COOKIE_SECURE=0` o usa `APP_ENV=development` para que login, formularios y demo funcionen correctamente sin HTTPS.


## Envío de correos desde la landing

Para que el formulario `Solicitar contacto` envíe correos a `parceliaMP@gmail.com`, configura estas variables en Render:

```
MAIL_TO=parceliaMP@gmail.com
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=parceliaMP@gmail.com
SMTP_PASSWORD=TU_APP_PASSWORD_DE_GMAIL
SMTP_USE_TLS=1
SMTP_USE_SSL=0
MAIL_FROM=parceliaMP@gmail.com
```

En Gmail debes crear una **App Password** y usarla en `SMTP_PASSWORD`.
