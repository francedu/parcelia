# Contabilidad Condominio

Aplicación web en Flask para llevar la contabilidad del condominio.

## Instalación

```bash
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
python3 app.py
```

## Acceso inicial

- usuario: `admin`
- contraseña: `admin123`

## Motores soportados

Esta versión funciona con:
- **SQLite** por defecto
- **PostgreSQL** usando `DATABASE_URL`

## Usar SQLite

Solo ejecuta:

```bash
python3 app.py
```

La base se guarda en:

```text
instance/contabilidad_condominio.db
```

## Usar PostgreSQL

### 1) Crea la base y el usuario

Ejemplo:

```sql
CREATE DATABASE contabilidad;
CREATE USER contabilidad_user WITH PASSWORD '1234';
GRANT ALL PRIVILEGES ON DATABASE contabilidad TO contabilidad_user;
```

### 2) Define la conexión

```bash
export DATABASE_URL='postgresql://contabilidad_user:1234@localhost/contabilidad'
```

### 3) Levanta la app

```bash
python3 app.py
```

## Migrar datos desde SQLite a PostgreSQL

Con PostgreSQL ya creado y `DATABASE_URL` definido:

```bash
python3 migrate_sqlite_to_postgres.py
```

Si tu archivo SQLite está en otra ruta:

```bash
SQLITE_PATH=/ruta/a/tu.db python3 migrate_sqlite_to_postgres.py
```

## Respaldos

- Con **SQLite** genera un `.db`
- Con **PostgreSQL** intenta generar un `.sql` usando `pg_dump`

Si `pg_dump` no está disponible, instala PostgreSQL client tools o agrega `pg_dump` al PATH.

## Usarla en tu red local

```bash
APP_HOST=0.0.0.0 APP_PORT=5001 python3 app.py
```

Luego entra desde otro equipo con:

```text
http://IP-DE-TU-MAC:5001
```

## Qué incluye esta versión para condominio

- base adaptada para un condominio de 21 parcelas
- registro de parcelas, pagos de gasto común, ingresos, egresos y categorías
- soporte real para SQLite y PostgreSQL
- misma app, sin reescribir tus vistas ni formularios
- script de migración de datos desde SQLite
- respaldo compatible con ambos motores

## Qué cambió en esta versión

- soporte real para SQLite y PostgreSQL
- misma app, sin reescribir tus vistas ni formularios
- script de migración de datos desde SQLite
- respaldo compatible con ambos motores

- En local o pruebas por HTTP, deja `SESSION_COOKIE_SECURE=0` o usa `APP_ENV=development` para que login, formularios y demo funcionen correctamente sin HTTPS.


## API móvil básica

El proyecto ahora incluye una API JSON para una futura app móvil, sincronizada con la misma base de datos que la web.

### Endpoints disponibles

- `GET /api/health`
- `POST /api/auth/login`
- `GET /api/me`
- `POST /api/auth/change-password`
- `GET /api/dashboard`
- `GET /api/parcelas`
- `GET /api/movimientos`

### Login

```bash
curl -X POST https://TU-APP.onrender.com/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"demo@parcelia.cl","password":"123456","mode":"demo"}'
```

La respuesta entrega un token tipo Bearer. Luego úsalo así:

```bash
curl https://TU-APP.onrender.com/api/me \
  -H "Authorization: Bearer TU_TOKEN"
```

### Variables útiles

- `API_TOKEN_MAX_AGE_SECONDS` → duración del token, por defecto 7 días
- `API_ALLOWED_ORIGINS` → orígenes permitidos para CORS, por defecto `*`

### Nota

La API reutiliza la misma lógica de usuarios, roles y base PostgreSQL. La app móvil debe consumir la API por HTTPS; no debe conectarse directo a PostgreSQL.
