# Parcelia multi-condominio SaaS aplicado

Este paquete agrega una capa SaaS segura y compatible con la base actual.

## Archivos incluidos

- `backend/app.py`
- `backend/templates/base.html`
- `backend/templates/usuarios_form.html`

## Qué cambia

1. Nueva tabla lógica `usuario_condominios` para permitir que un usuario tenga acceso a uno o varios condominios.
2. Se mantiene compatibilidad con `usuarios.condominio_id` como condominio principal.
3. Al iniciar el backend, se hace backfill automático desde `usuarios.condominio_id` hacia `usuario_condominios`.
4. El selector de condominio ya no es solo para admin global: también aparece para usuarios con más de un condominio habilitado.
5. La API expone:
   - `GET /api/condominios`
   - `POST /api/condominios/seleccionar`
6. El usuario de la API ahora devuelve `condominios` y `condominio_id` activo.
7. Las consultas siguen filtrando por `condominio_id`, evitando mezclar datos entre condominios.

## Cómo instalar

1. Haz backup/commit:

```bash
git add .
git commit -m "backup antes multi-condominio saas"
```

2. Reemplaza los archivos incluidos.

3. Sube a Render:

```bash
git add .
git commit -m "multi-condominio saas"
git push
```

4. Render ejecutará la migración suave al iniciar la app.

## Uso

- Admin global: rol `admin` sin `condominio_id`.
- Admin local: rol `admin` con `condominio_id`.
- Usuario multi-condominio: edítalo desde Usuarios y selecciona varios condominios en “Condominios habilitados”.

## Nota

Este cambio no duplica bases de datos. Mantiene una sola base con aislamiento por `condominio_id`, que es el patrón recomendado para SaaS multi-tenant pequeño/mediano.
