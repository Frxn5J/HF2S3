# Despliegue de HF2S3 en Coolify (producción)

Esta guía cubre una instalación nueva y, sobre todo, la **actualización de una instalación existente** (con datos ya subidos) a esta versión, que cambia el formato de cifrado y elimina los valores por defecto inseguros.

> **Antes de empezar:** la clave maestra (`HF2S3_MASTER_KEY`) cifra todo lo que hay en Hugging Face. **Si la pierdes, los datos no se pueden recuperar.** Guarda una copia fuera del servidor (gestor de contraseñas).

---

## 1. Instalación nueva

1. **Genera los secretos** (no los escribas a mano ni copies ejemplos de la documentación):

   ```bash
   docker run --rm ghcr.io/tu-usuario/hf2s3:latest /app/hf2s3 keygen   # o: docker compose run --rm hf2s3 /app/hf2s3 keygen
   ```

   Imprime `HF2S3_MASTER_KEY`, `HF2S3_ACCESS_KEY`, `HF2S3_SECRET_KEY`, `ADMIN_USERNAME`, `ADMIN_PASSWORD` y `HF2S3_METRICS_TOKEN`.

2. En Coolify: **+ New Resource → Docker Compose** (o repositorio Git con el `Dockerfile`) y pega el contenido de [`docker-compose.yml`](docker-compose.yml).

3. En **Environment Variables** define, como mínimo:

   | Variable | Descripción |
   |---|---|
   | `HF2S3_MASTER_KEY` | Obligatoria. 32 bytes aleatorios en base64. |
   | `HF2S3_ACCESS_KEY` / `HF2S3_SECRET_KEY` | Credenciales que usarán tus clientes S3. Secret de 24+ caracteres. |
   | `ADMIN_USERNAME` / `ADMIN_PASSWORD` | Acceso al panel. Contraseña de 12+ caracteres. |
   | `HF2S3_PUBLIC_URL` | URL externa, p. ej. `https://s3.tudominio.com`. |

   El servicio **se niega a arrancar** si falta alguna de las obligatorias o si son valores conocidos (`admin123`, `hf2s3-secret-key`, la frase maestra de ejemplo…). El mensaje de error dice qué corregir.

4. **Domains**: `https://s3.tudominio.com`. **Port**: `8080`. Coolify (Traefik) emite el certificado.

5. **Persistent Storage**: volumen `hf2s3_data` montado en `/data` (ya declarado en el compose). Contiene la base de datos y los backups.

6. **Healthcheck**: el compose ya usa `/api/ready` (comprueba la base de datos). `/api/health` solo indica que el proceso está vivo.

### Detalles importantes con el proxy

- **`Host` sin modificar.** Las firmas SigV4 (clientes S3 y enlaces prefirmados) incluyen la cabecera `Host`. Traefik la conserva por defecto; si pones otro proxy delante, no la reescribas.
- `HF2S3_TRUST_PROXY=true` (valor por defecto del compose) hace que se lean `X-Forwarded-For` y `X-Forwarded-Proto` para el límite de intentos de login y para marcar la cookie como `Secure`. **Actívalo solo si el proxy sobrescribe esas cabeceras**; si expones el contenedor directamente a Internet, ponlo en `false`.
- No publiques el puerto 8080 del contenedor: solo debe ser accesible a través del proxy (el compose usa `expose`, no `ports`).

---

## 2. Actualizar una instalación existente (con datos)

La versión anterior guardaba la clave maestra **en claro en la base de datos** (y por defecto era `hf2s3-aes-master-passphrase-2026`), aceptaba S3 sin verificar la firma y dejaba `/media` público. Esta versión lo corrige, pero el **ciphertext ya subido sigue cifrado con la clave antigua** hasta que lo re-cifres. Sigue estos pasos en orden.

### Paso 0 — Contención y copia de seguridad

1. Si el servicio está expuesto a Internet, **restringe el acceso ya** (lista de IPs en Traefik o quita temporalmente el dominio): con la versión anterior cualquiera que conozca el Access Key ID podía leer y borrar.
2. **Descarga un backup** de la base de datos desde el panel (Configuración → Descargar Base de Datos) y guárdalo fuera del servidor. Verifica que abre (`sqlite3 backup.db "PRAGMA integrity_check"`).

### Paso 1 — Nuevos secretos

Genera un juego nuevo (`keygen`, ver arriba) y define en Coolify **todas** las variables obligatorias. **No cambies el volumen `hf2s3_data`.**

No definas `HF2S3_LEGACY_MASTER_KEYS`: la clave antigua se encuentra sola en la base de datos (paso 2).

### Paso 2 — Desplegar

Al arrancar por primera vez, el log muestra (formato JSON):

- `encrypted stored secrets at rest` → los tokens de HF y secretos que estaban en claro se cifran con una clave derivada de la nueva clave maestra.
- `the previous master key was stored in plaintext ... moved (encrypted) to the legacy key list` → la frase antigua se mueve **cifrada** a `legacy_master_keys` y se elimina el texto plano de la base de datos.
- `some chunks still use an old encryption format or key; run hf2s3 rekey` → aviso de los trozos pendientes.

Los datos antiguos **siguen siendo legibles**: el servicio usa la clave legacy solo para leerlos. Comprueba que puedes descargar un archivo.

> Si el arranque falla con `HF2S3_MASTER_KEY does not match the key this database was encrypted with`, estás usando una clave distinta a la que cifró esta base de datos. Nada se ha modificado.

### Paso 3 — Rotar credenciales

Tokens de Hugging Face, claves del bucket de caché, contraseña de admin y claves S3 estuvieron en claro en la base de datos y en los backups anteriores. Rótalos:

- Tokens de HF: crea tokens nuevos en huggingface.co y actualiza cada cuenta en el panel (Cuentas → Editar).
- Claves de los buckets de caché: recréalas en Hugging Face y actualiza los buckets.
- S3: `HF2S3_ACCESS_KEY`/`HF2S3_SECRET_KEY` ya son nuevos (paso 1); actualiza tus clientes.
- Las copias de seguridad antiguas siguen conteniendo los secretos viejos: bórralas.

### Paso 4 — Re-cifrar los datos existentes

Dentro del contenedor (en Coolify: *Terminal* del servicio, o `docker exec -it <contenedor> sh`). Antepón `su-exec app` para ejecutarlos como el usuario de la aplicación: la terminal entra como root y crearía ficheros de la base de datos que la app no podría escribir después.

```bash
su-exec app /app/hf2s3 rekey --dry-run      # cuántos trozos y cuántos MiB hay que procesar
su-exec app /app/hf2s3 rekey                # descarga, descifra con la clave antigua, re-cifra con la nueva y sube
```

- Es **reanudable e idempotente**: si se interrumpe, vuelve a ejecutarlo.
- La base de datos solo pasa a apuntar a la copia nueva **después** de que esta esté confirmada en Hugging Face; la copia vieja se borra al final. Un trozo que no se pueda descifrar se **informa y no se toca**.
- Respeta los límites de commits de Hugging Face; con muchos datos puede tardar horas.

Cuando termine sin fallos:

```bash
su-exec app /app/hf2s3 rekey --purge-legacy-keys   # elimina la clave antigua guardada (ya no hace falta)
```

### Paso 5 — Purgar el historial de los datasets

Borrar un archivo LFS en Hugging Face **no lo elimina del historial git**, y los datasets son públicos: el ciphertext antiguo sigue descargable y ocupando cuota. Para quitarlo:

```bash
su-exec app /app/hf2s3 squash            # simulación: lista qué haría
su-exec app /app/hf2s3 squash --yes      # reescribe el historial (irreversible)
```

> **Sé realista con la confidencialidad:** si la clave antigua era la frase por defecto (o cualquiera conocida) y los datasets eran públicos, **cualquiera que haya descargado esos trozos antes puede descifrarlos**. `squash` evita nuevas descargas, pero no puede deshacer copias ya hechas. Trata como expuesto lo que subiste antes de esta actualización.

### Paso 6 — Verificación

- `/api/ready` responde `ready`; el panel muestra «Todos los fragmentos usan el formato y la clave actuales».
- `rclone lsf`/`aws s3 ls` funcionan con las credenciales nuevas y **fallan** con el Access Key ID solo.
- `https://s3.tudominio.com/media/<bucket>/<clave>` sin firma devuelve 403; el botón **Enlace** del panel genera un enlace firmado que sí reproduce.

---

## 3. Operación

### Backups

- Cada `HF2S3_BACKUP_INTERVAL_HOURS` (24 por defecto) se guarda una copia consistente en `/data/backups` (se conservan las últimas `HF2S3_BACKUP_KEEP`).
- Si hay un bucket de caché configurado, se sube además una **copia cifrada con la clave maestra** a `_hf2s3/backups/` en ese bucket. Sin la base de datos no hay forma de saber qué trozos forman cada archivo: **prueba tus restauraciones**.
- Descarga manual: panel → Configuración, o `hf2s3 backup fichero.db`.

### Restaurar

**Desde el panel (recomendado):** Configuración → *Restaurar desde un respaldo*.

1. Selecciona el archivo (`.db`, o `.db.enc` si es la copia cifrada del bucket) y pulsa **Subir y validar**. Se comprueba **todo antes de tocar nada**: que es una base de HF2S3 íntegra, que no es de una versión más nueva, que la clave maestra actual la puede abrir y que el servicio arrancaría con ella (credenciales incluidas).
2. Revisa el resumen (objetos y cuentas de la copia frente a los actuales) y los avisos. Una copia más antigua no tiene lo subido después: esos fragmentos quedarán huérfanos en Hugging Face.
3. Escribe tu contraseña de administrador y pulsa **Restaurar y reiniciar**. El servicio deja de aceptar peticiones nuevas, termina las que están en curso, se reinicia solo (en el contenedor mantiene el mismo proceso, no hace falta que Coolify lo levante) y en unos segundos la página se recarga; tendrás que volver a iniciar sesión.

Garantías:
- La base **actual** se conserva como `hf2s3_metadata.db.pre-restore-<fecha>` en `/data` (se guardan las 3 últimas).
- Si la restaurada pasara la validación pero aun así no arrancara, el servicio **vuelve solo a la anterior** y lo indica en el panel (la rechazada queda como `.rejected-<fecha>`).
- Una copia de una versión anterior (con la clave maestra en claro) se restaura bien: esa clave se guarda cifrada como clave legacy; después ejecuta `hf2s3 rekey`.
- Tamaño máximo de subida: `HF2S3_RESTORE_MAX_MB` (1024 por defecto). Las copias cifradas (`.enc`) de más de 256 MB se restauran por línea de comandos.

**Por línea de comandos** (si el panel no es accesible): con el servicio **parado** en Coolify (la restauración no se hace en caliente), en el servidor y con un contenedor de un solo uso que monta el mismo volumen:

```bash
docker run --rm -v hf2s3_data:/data -v "$PWD":/in hf2s3:latest \
  /app/hf2s3 restore /in/hf2s3-backup-20260928-101500.000.db

# copia cifrada del bucket (necesita la clave maestra):
docker run --rm -v hf2s3_data:/data -v "$PWD":/in -e HF2S3_MASTER_KEY hf2s3:latest \
  /app/hf2s3 restore /in/hf2s3-backup-....db.enc
```

(`hf2s3_data` es el nombre del volumen; si definiste `HF2S3_VOLUME_NAME`, usa ese. El nombre de la imagen es el que Coolify muestre para el servicio.)

Valida la integridad, conserva el fichero anterior como `.pre-restore` y las migraciones pendientes se aplican al arrancar.

### Rotar la clave maestra

Si sospechas que `HF2S3_MASTER_KEY` se ha filtrado:

1. Genera una nueva (`hf2s3 keygen`) y **guarda la anterior**.
2. Define `HF2S3_MASTER_KEY=<nueva>` y `HF2S3_LEGACY_MASTER_KEYS=<anterior>`, y reinicia. Al arrancar, los tokens y secretos de la base de datos se re-cifran con la clave nueva (el log lo indica) y los datos siguen legibles.
3. Ejecuta `/app/hf2s3 rekey` para re-cifrar los trozos con la clave nueva y después `/app/hf2s3 squash --yes` para purgar el ciphertext antiguo del historial.
4. Quita `HF2S3_LEGACY_MASTER_KEYS` y reinicia.

Sin la clave anterior en `HF2S3_LEGACY_MASTER_KEYS`, el arranque se **niega** (no toca nada) en lugar de ignorar en silencio los secretos que no puede descifrar.

### Vaciar y quitar una cuenta de Hugging Face

Panel → Cuentas → **Vaciar**: desactiva la cuenta y copia sus trozos a las demás. Cuando termine y la cola de borrado esté vacía, ya puedes **Eliminar** la cuenta (mientras tenga datos, el panel lo impide).

### Métricas y logs

- Logs en JSON por stdout (sin secretos ni query strings de URLs firmadas).
- Define `HF2S3_METRICS_TOKEN` para habilitar `/metrics` (Prometheus, `Authorization: Bearer <token>`): peticiones por ruta/estado, borrados pendientes, trozos por re-cifrar, cuentas limitadas…

### Actualizaciones sin pérdida

`stop_grace_period` da tiempo a terminar las subidas en curso (`HF2S3_SHUTDOWN_TIMEOUT_SECONDS`). Las migraciones de esquema son versionadas; una base de datos de una versión **más nueva** que el binario se rechaza en lugar de dañarse.

### Contenedor y volumen

La imagen arranca como root solo para hacer `chown` del volumen `/data` (los volúmenes creados por versiones anteriores pertenecían a root) y ejecuta el gateway como el usuario `app` (uid 10001), con sistema de ficheros de solo lectura, sin capacidades extra y `no-new-privileges`.
