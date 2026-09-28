# HF2S3: pasarela S3 multi-nivel sobre Hugging Face

**HF2S3** es una pasarela de almacenamiento escrita en **Go** con API compatible con **AWS S3 / Cloudflare R2**. Reparte los archivos entre varias cuentas de **Hugging Face** para sumar su cuota gratuita y servirlos como un único almacenamiento, pensado sobre todo para vídeo y otros archivos multimedia.

- **Tier 2 (frío, copia maestra):** trozos de 32 MB cifrados con **AES-256-GCM** en datasets de Hugging Face de una o varias cuentas.
- **Tier 1 (caché):** copia sin cifrar en buckets de Hugging Face Storage (`s3.hf.co`). Los clientes que usan enlaces firmados reciben un **302 directo** al bucket: los bytes no pasan por tu VPS.

> ⚠️ **Léelo antes de usarlo con datos que te importen** — ver [Modelo de amenazas y límites](#modelo-de-amenazas-y-límites).

---

## Arquitectura

```
   Cliente S3 (SDK/rclone)            Navegador / <video>
        firma por cabecera             URL firmada (?X-Amz-Signature=…)
                 \                          /
                  v                        v
              +----------------------------------+
              |  HF2S3  ── verifica SigV4 ──     |   panel web: cookie HttpOnly,
              |          (todas las rutas)       |   CSP, sin secretos en claro
              +----------------+-----------------+
                               |
              ¿copia en caché? |
             +-----------------+------------------+
             | sí                                 | no
   presign  → 302 (URL firmada)                    v
   cabecera → proxy desde el bucket      descarga trozos del dataset,
                                         verifica y descifra, sirve, y
                                         promociona a caché en segundo plano
```

- Cada trozo se cifra con una clave derivada por HKDF de la clave maestra y **ligado a su ruta remota** (AAD): no se puede intercambiar un trozo por otro sin que falle la autenticación. Además se verifica el SHA-256 del texto plano.
- Los metadatos (qué trozos forman cada objeto, dónde están) viven en **SQLite**, con migraciones versionadas. Los borrados remotos van por una **cola persistente con reintentos**, así que un fallo de Hugging Face no deja trozos huérfanos ni cuota consumida.
- Los trozos de un objeto se confirman en **un commit por cuenta** (no uno por trozo) para no agotar el límite de commits de Hugging Face; si una cuenta responde con límite de peticiones, se conmuta a otra.
- Subidas multipart, `Range`, peticiones condicionales, paginación de listados v1/v2 (con `encoding-type=url`), `aws-chunked` (AWS CLI v2, boto3 recientes) con verificación de firma por trozo y de checksums.

## Seguridad: cómo se autentica cada cosa

| Ruta | Autenticación |
|---|---|
| API S3 (`/bucket/clave`) | **SigV4 verificado de verdad** (cabecera o URL prefirmada): firma, desfase de reloj (±15 min), caducidad (≤ 7 días). Falla en cerrado: sin credenciales configuradas rechaza todo. |
| `/media/{bucket}/{clave}` | Igual que la API S3: exige **URL prefirmada**. Se generan con cualquier SDK (`generate_presigned_url`) o desde el botón **Enlace** del panel. |
| Panel `/api/*` | Cookie `HttpOnly; SameSite=Strict` (+ `Secure` bajo HTTPS), contraseña con PBKDF2-SHA256, límite de intentos, comprobación de origen (CSRF), CSP estricta, `nosniff`. |

Los secretos guardados en la base de datos (tokens de HF, claves de buckets, secreto S3, claves legacy) se cifran con una clave derivada de `HF2S3_MASTER_KEY`. Las respuestas HTTP nunca incluyen secretos ni detalles internos de errores.

## Modelo de amenazas y límites

Lo que **sí** protege el diseño:

- Quien descargue los datasets públicos de Hugging Face solo ve trozos opacos cifrados; sin `HF2S3_MASTER_KEY` no puede leerlos ni alterarlos sin que se detecte.
- Quien conozca el Access Key ID (o adivine `/media/bucket/clave`) no puede leer, escribir ni borrar: hace falta una firma válida.
- Un volcado de la base de datos o un backup no revela tokens ni secretos sin la clave maestra.

Lo que **no** protege (y debes saber):

- **La caché (Tier 1) está sin cifrar** en tus buckets de Hugging Face. Si esos buckets o sus claves se comprometen, se ve el contenido en claro.
- **Quien controle el servidor** (o su entorno) tiene la clave maestra y ve todo.
- Los datasets del nivel frío son **públicos por diseño** (descargas sin token). El cifrado es lo único que los protege: si la clave maestra fuera conocida (p. ej. la frase de ejemplo de versiones antiguas), el contenido queda expuesto. Por eso esta versión **exige** una clave aleatoria y ofrece `rekey` + `squash` para migrar (ver [COOLIFY.md](COOLIFY.md)).
- Los enlaces prefirmados dan acceso a un objeto hasta que caducan a quien los tenga.
- Los nombres/tamaños de archivo no están cifrados en Hugging Face (los trozos son `data/<uuid>.enc`, pero su número y tamaño sí se ven).
- **Términos de uso de Hugging Face.** Usar varias cuentas para sumar cuota y alojar datos ajenos al ML en datasets públicos probablemente **incumple sus condiciones**. Si una cuenta se suspende, pierdes los datos alojados en ella (la copia maestra vive allí). No lo uses como único almacenamiento de datos que no puedas permitirte perder; mantén otra copia.

---

## Puesta en marcha

Requisitos: Go 1.24+ (compilación) o Docker.

```bash
go build -o hf2s3 ./cmd/hf2s3

./hf2s3 keygen > .env.local         # secretos aleatorios (los avisos van a stderr)
# edita el fichero: añade HF2S3_PUBLIC_URL y, si quieres, el bucket de caché
set -a; . ./.env.local; set +a
./hf2s3                              # = ./hf2s3 serve
```

Sin esas variables el servicio no arranca. Para probar en local sin configurar nada: `HF2S3_DEV=1 ./hf2s3` (defaults inseguros; **nunca** con datos reales).

Despliegue en producción: [COOLIFY.md](COOLIFY.md) (incluye la actualización desde versiones anteriores).

### Comandos

| Comando | Qué hace |
|---|---|
| `hf2s3` / `hf2s3 serve` | Ejecuta la pasarela. |
| `hf2s3 keygen` | Imprime un `.env` con secretos aleatorios nuevos. |
| `hf2s3 rekey [--dry-run]` | Re-cifra los trozos en formato/clave antiguos con la clave actual (reanudable). |
| `hf2s3 squash [--account ID] --yes` | Purga el ciphertext antiguo del historial git de los datasets y libera cuota. |
| `hf2s3 backup <fichero>` | Copia consistente de la base de datos. |
| `hf2s3 restore <fichero[.enc]>` | Restaura desde la línea de comandos (con el servicio parado). Desde el panel: Configuración → *Restaurar desde un respaldo* (validación previa, confirmación con contraseña y reinicio automático). |

### Configuración

Todo se define por variables de entorno (o `.env`); ver [`.env.example`](.env.example). Lo definido en el entorno **manda sobre la base de datos** y es de solo lectura en el panel.

| Variable | Descripción |
|---|---|
| `HF2S3_MASTER_KEY` | **Obligatoria.** 32 bytes aleatorios (base64/hex). |
| `HF2S3_ACCESS_KEY`, `HF2S3_SECRET_KEY` | **Obligatorias.** Credenciales de los clientes S3 (secreto ≥ 24 caracteres). |
| `ADMIN_USERNAME`, `ADMIN_PASSWORD` | **Obligatorias.** Panel (contraseña ≥ 12 caracteres). |
| `HF2S3_PUBLIC_URL` | URL externa (enlaces firmados y snippets). |
| `HF2S3_TRUST_PROXY` | `true` tras un proxy que sobrescribe `X-Forwarded-*`. |
| `S3_GET_REDIRECT` | `auto` (por defecto), `always`, `never`. |
| `HF2S3_CORS_ORIGINS` | Orígenes de navegador permitidos (vacío = ninguno). |
| `HF2S3_LEGACY_MASTER_KEYS` | Frases antiguas, solo para leer datos previos a esta versión. |
| `HF2S3_DB`, `PORT`, `HF2S3_CHUNK_SIZE_MB`, `HF2S3_REGION` | Como siempre. |
| `HF2S3_BACKUP_INTERVAL_HOURS`, `HF2S3_BACKUP_KEEP`, `HF2S3_BACKUP_DIR` | Backups automáticos. |
| `HF2S3_METRICS_TOKEN` | Habilita `/metrics` (Prometheus). |
| `HF2S3_LOG_FORMAT` / `HF2S3_LOG_LEVEL` | `json`/`text`, `debug`/`info`/`warn`/`error`. |
| `HF_STORAGE_*` | Bucket de caché (también configurable en el panel, con varios buckets). |

## Uso desde clientes

Para SDKs y rclone la pasarela **sirve el contenido ella misma** (los clientes de firma por cabecera no siguen redirecciones de forma fiable); la redirección directa a Hugging Face se reserva para enlaces firmados.

```ini
# rclone
[hf2s3]
type = s3
provider = Other
access_key_id = <HF2S3_ACCESS_KEY>
secret_access_key = <HF2S3_SECRET_KEY>
endpoint = https://s3.tudominio.com
region = us-east-1
```

```python
# boto3: enlace de 1 hora para un <video>
url = s3.generate_presigned_url('get_object', Params={'Bucket': 'media', 'Key': 'clip.mp4'}, ExpiresIn=3600)
```

Para usar el alias `/media`, apunta el cliente a `https://s3.tudominio.com/media` como endpoint (estilo *path*) o usa el botón **Enlace** del panel; el resultado se reproduce con saltos (`Range`) desde la caché o el nivel frío.

No implementado (responde `501 NotImplemented` en lugar de aparentar soporte): `CopyObject`/`UploadPartCopy`, versionado, ACL, etiquetas, políticas, `ListParts`/`ListMultipartUploads`. Los buckets llamados `media`, `api` y `static` están reservados.

## Desarrollo

```bash
go vet ./...
go test ./...                 # en Linux/macOS añade -race
node --check pkg/dashboard/static/app.js
```

Los tests incluyen **vectores oficiales de AWS** (firma prefirmada, GET con `Range`, PUT con `$`, subida por trozos firmados y CRC64-NVME), migración desde una base de datos antigua, re-cifrado, limpieza de huérfanos y el arranque seguro.

Estructura: `cmd/hf2s3` (arranque y subcomandos), `pkg/sigv4`, `pkg/s3api` (verificación de firma, `aws-chunked`, rutas S3), `pkg/storage` (pool, caché, GC, re-cifrado), `pkg/crypto` (anillo de claves, cifrado de secretos), `pkg/db` (SQLite y migraciones), `pkg/hfclient` / `pkg/hfstorage` (clientes de Hugging Face), `pkg/dashboard` (panel), `pkg/config`, `pkg/backup`, `pkg/metrics`.

## Licencia

Ver [LICENSE](LICENSE).
