# Versión multi-condominio v2

Cambios incluidos en esta versión:

- Selector de condominio en la barra superior para administrador global.
- El administrador global puede ver y administrar todos los condominios.
- El administrador local (usuario con rol `admin` y condominio asignado) solo puede administrar usuarios de su propio condominio.
- Los usuarios no administradores deben quedar asignados a un condominio.
- La navegación mantiene el `condominio_id` seleccionado para no perder contexto al cambiar de módulo.
- El lenguaje visible del sistema sigue orientado a parcelas/unidades, aunque internamente algunas tablas conservan el nombre histórico `parcelas` para compatibilidad.

## Perfiles recomendados

- **Admin global**: rol `admin` sin condominio asignado.
- **Admin local**: rol `admin` con un condominio asignado.
- **Presidente / tesorero / secretario / comité**: siempre asociados a un condominio.

## Arranque local

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
APP_HOST=0.0.0.0 APP_PORT=5001 python3 app.py
```

## Acceso desde red local

Abre desde otro equipo de la misma red:

```text
http://IP_DE_TU_EQUIPO:5001
```


## Ajustes v6
- Rutas públicas de parcelas: `/parcelas`, `/parcelas/nuevo`, `/parcelas/<id>`, `/parcelas/<id>/editar`.
- Se mantienen alias legados `/parcelas` para compatibilidad.
- Plantillas y navegación actualizadas para usar el nombre `Parcelas`.
