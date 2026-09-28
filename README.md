# HF2S3: Hugging Face Multi-Account & Multi-Tier S3/R2 Cloud Gateway

**HF2S3** es una pasarela de almacenamiento inteligente de alto rendimiento desarrollada en **Go** que unifica cuentas y repositorios de **Hugging Face** en una arquitectura de almacenamiento multi-nivel, compatible con la API **AWS S3 / Cloudflare R2** y con una API REST multimedia dedicada.

El sistema implementa dos niveles de almacenamiento complementarios:
1. **Tier 1 (Caché S3 de Alta Velocidad)**: Buckets privados de Hugging Face (`s3.hf.co`) que almacenan medios sin cifrar y entregan directamente los bytes al cliente mediante **URLs prefirmadas AWS SigV4 (HTTP 302 / 307 Redirect)**. El VPS **nunca consume ancho de banda de descarga** y las credenciales de HF permanecen estrictamente ocultas.
2. **Tier 2 (Frío / Almacenamiento Masivo en Datasets)**: Datasets públicos en Hugging Face Hub que albergan copias maestras originales protegidas con **cifrado en reposo AES-256-GCM (Zero-Knowledge)**. Las descargas son libres, gratuitas y de alta velocidad sin cuotas de autenticación.

Al conectar $N$ cuentas gratuitas de Hugging Face, HF2S3 conforma un pool distribuido masivo de **$N \times 100\text{ GB}$**, permitiendo montar unidades de red o interactuar con clientes estándar como **Rclone, Cyberduck, AWS CLI, DuckDB, Boto3** o navegadores web y reproductores de video.

---

## Arquitectura Multi-Nivel Inteligente

```
                          +-------------------------------+
                          |     Cliente / Reproductor     |
                          | (Navegador, Rclone, Video UI) |
                          +---------------+---------------+
                                          |
                        1. GET /media/{bucket}/{key}
                        o AWS S3 GetObject
                                          |
                                  +-------v-------+
                                  |     HF2S3     |
                                  |    Gateway    |
                                  +-------+-------+
                                          |
                       ¿Está en Caché S3? | (object_locations)
                       +------------------+------------------+
                       |                                     |
               SÍ (Cache Hit)                          NO (Cache Miss)
                       |                                     |
        +--------------v--------------+       +--------------v--------------+
        |  Genera URL Prefirmada      |       |  1. Descarga Chunks de Hub  |
        |  SigV4 (15 min)             |       |  2. Descifra AES-256-GCM    |
        |  Responde HTTP 302 Redirect |       |  3. Envía stream al cliente |
        +--------------+--------------+       |  4. Asíncrono: Promueve     |
                       |                      |     a Caché S3 (sin cifrar) |
                       |                      +--------------+--------------+
                       v                                     |
        +-----------------------------+                      v
        |     Hugging Face Storage    |       +-----------------------------+
        |  (s3.hf.co / Bucket Privado)|       |    Hugging Face Datasets    |
        |  Descarga directa sin VPS   |       | (Hub Público / AES-256-GCM) |
        +-----------------------------+       +-----------------------------+
```

### Principios Fundamentales
- **Entrega Directa sin Paso por el VPS**: En el Tier 1, los bytes viajan directamente desde los servidores perimetrales de Hugging Face (`s3.hf.co`) hacia el cliente. Tu servidor/VPS solo procesa metadatos ligeros y firmas criptográficas.
- **Seguridad Cero-Conocimiento en Nivel Frío**: Los datasets públicos contienen únicamente chunks opacos cifrados con AES-256-GCM y nonce aleatorio. Nadie puede inspeccionar el contenido sin la clave maestra `HF2S3_MASTER_KEY`.
- **Desalojo Seguro (Eviction)**: Al desalojar archivos del Tier 1 (por políticas de espacio o manualmente desde el panel), solo se elimina la copia sin cifrar del bucket de caché. La **copia maestra original en el dataset público nunca se elimina**.
- **Promoción Automática**: Cualquier petición a un objeto no presente en caché se descifra al vuelo, se entrega al cliente y se promueve en segundo plano al Tier 1.

---

## Características Principales

1. **Arquitectura Multi-Nivel y Ubicaciones Múltiples**:
   - Seguimiento transaccional de estados de objetos en SQLite (`object_locations`).
   - Soporte para políticas de desalojo LRU (*Least Recently Used*).

2. **API Dual (S3 Compatible + REST Multimedia)**:
   - **API S3 Estándar**: `GetObject`, `PutObject`, `HeadObject`, `DeleteObject`, `ListObjectsV2`, `ListBuckets`, `CreateBucket`, `DeleteBucket`.
   - **API Multimedia Directa**: `/media/{bucket}/{key}` que responde con redirección HTTP 302 Found a la URL prefirmada si está en caché, o transmite directamente el contenido multimedia.
   - Encabezados de diagnóstico: `X-HF2S3-Cache-Status: HIT | MISS` y `X-HF2S3-Tiers: cache,cold`.

3. **Pool Distribuido de Datasets Públicos**:
   - Agrupación automática de cuentas de Hugging Face.
   - Algoritmo de balanceo por menor utilización relativa (*Least-Used Weighted*).
   - Descargas de alta velocidad mediante endpoints de Hub sin necesidad de token de autorización para lecturas.

4. **Cifrado Zero-Knowledge en Reposo (AES-256-GCM)**:
   - Los archivos se fragmentan en chunks (por defecto 32 MB).
   - Cada chunk se cifra localmente con **AES-256-GCM** y un Nonce aleatorio de 12 bytes.

5. **Panel Web de Administración Moderno (Dark Glassmorphism)**:
   - Monitoreo en tiempo real del pool y uso de almacenamiento.
   - Indicador visual por archivo: badges `⚡ Caché S3` y `❄️ Dataset Público`.
   - Botón **Stream** para reproducción o enlace directo.
   - Acciones de **Desalojar Caché** y **Promover a Caché** con un clic.
   - Configuración gráfica de credenciales S3 y parámetros de Hugging Face Storage.
   - Respaldo íntegro y en caliente de base de datos SQLite (`.db`).

6. **Despliegue Llave en Mano en Coolify y Docker**:
   - Imagen Docker multi-etapa ultra ligera (<30 MB) con certificados CA y usuario no-root.
   - Archivo `docker-compose.yml` preconfigurado con volumen persistente `/data` y healthcheck automático.

---

## Requisitos y Compilación

- **Go**: 1.22 o superior (probado en Go 1.24/1.27 Windows amd64 y Linux).
- Compilación nativa pura sin dependencias externas pesadas.

### Compilar el ejecutable

```powershell
go build -v -o hf2s3.exe ./cmd/hf2s3
```

---

## Configuración y Variables de Entorno

Puedes configurar el gateway mediante archivo `.env` o flags de línea de comandos:

| Flag | Variable de Entorno | Valor por Defecto | Descripción |
|---|---|---|---|
| `-port` | `PORT` | `8080` | Puerto HTTP para la API S3, REST y Panel Web |
| `-db` | `HF2S3_DB` | `hf2s3_metadata.db` | Ruta del archivo de base de datos SQLite |
| `-chunk-size` | `HF2S3_CHUNK_SIZE_MB` | `32` | Tamaño del fragmento en Megabytes para el nivel frío |
| `-access-key` | `HF2S3_ACCESS_KEY` | `hf2s3-access-key` | S3 Gateway Access Key ID |
| `-secret-key` | `HF2S3_SECRET_KEY` | `hf2s3-secret-key` | S3 Gateway Secret Access Key |
| `-master-key` | `HF2S3_MASTER_KEY` | (Generada) | Frase de paso para derivación de clave AES-256-GCM |
| `-region` | `HF2S3_REGION` | `us-east-1` | Región S3 reportada |
| `-admin-user` | `ADMIN_USERNAME` | `admin` | Usuario administrador del dashboard |
| `-admin-pass` | `ADMIN_PASSWORD` | `admin123` | Contraseña del panel web |
| `-hf-storage-endpoint` | `HF_STORAGE_ENDPOINT` | `https://s3.hf.co` | Endpoint S3 de Hugging Face Storage (Tier 1) |
| `-hf-storage-region` | `HF_STORAGE_REGION` | `us-east-1` | Región de Hugging Face Storage |
| `-hf-storage-access-key`| `HF_STORAGE_ACCESS_KEY` | `""` | Access Key ID de HF Storage (`HFAK...`) |
| `-hf-storage-secret-key`| `HF_STORAGE_SECRET_KEY` | `""` | Secret Key de Hugging Face Storage |
| `-hf-storage-bucket` | `HF_STORAGE_BUCKET` | `""` | Nombre del bucket de caché en Hugging Face |

> [!TIP]
> **Configuración 100% desde la Interfaz Gráfica**:
> No necesitas editar archivos `.env` ni pasar flags por terminal. Todo el sistema (credenciales de administrador, credenciales S3, buckets de caché HF Storage, clave maestra AES y tamaño de fragmentos) se puede configurar y actualizar directamente desde la pestaña **Configuración** del panel web (`http://localhost:8080/`), guardándose en la base de datos persistente SQLite.

---

## Ejemplo de Configuración `.env` (Opcional)

```ini
ADMIN_USERNAME=admin
ADMIN_PASSWORD=clave-segura-panel-2026

HF2S3_ACCESS_KEY=mi-access-key-s3
HF2S3_SECRET_KEY=mi-secret-key-s3
HF2S3_MASTER_KEY=mi-passphrase-maestra-aes-ultra-secreta

HF2S3_DB=./hf2s3_metadata.db
HF2S3_CHUNK_SIZE_MB=32
PORT=8080

# Hugging Face Storage Buckets (Tier 1 Cache)
HF_STORAGE_ENDPOINT=https://s3.hf.co
HF_STORAGE_REGION=us-east-1
HF_STORAGE_ACCESS_KEY=HFAKxxxxxxxxxxxxxxxxxxxx
HF_STORAGE_SECRET_KEY=yyyyyyyyyyyyyyyyyyyyyyyy
HF_STORAGE_BUCKET=mi-cache-multimedia
```

---

## Conexión con Clientes S3

### 1. Rclone (Montar como Disco Local)

```ini
[hf2s3]
type = s3
provider = Other
env_auth = false
access_key_id = hf2s3-access-key
secret_access_key = hf2s3-secret-key
endpoint = http://localhost:8080
region = us-east-1
```

Comandos útiles:
```powershell
rclone lsd hf2s3:
rclone mount hf2s3:mi-bucket X: --vfs-cache-mode full
```

### 2. AWS CLI

```powershell
export AWS_ACCESS_KEY_ID="hf2s3-access-key"
export AWS_SECRET_ACCESS_KEY="hf2s3-secret-key"

# Listar objetos
aws --endpoint-url=http://localhost:8080 s3 ls s3://mi-bucket/

# Descargar objeto
aws --endpoint-url=http://localhost:8080 s3 cp s3://mi-bucket/video.mp4 ./video.mp4
```

### 3. API REST Multimedia para Navegadores

Puedes incrustar o transmitir directamente los archivos en reproductores HTML5 o aplicaciones frontend:

```html
<!-- Si el archivo está en caché S3, HF2S3 redirige al instante (302) a s3.hf.co -->
<video controls src="http://localhost:8080/media/mi-bucket/video.mp4"></video>
```

---

## Estructura del Proyecto

- [`cmd/hf2s3/main.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/cmd/hf2s3/main.go): Inicialización del sistema, persistencia y enrutador unificado.
- [`pkg/hfstorage/s3client.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/hfstorage/s3client.go): Cliente puro en Go para Hugging Face Storage Buckets con firmas AWS SigV4 y presign de URLs.
- [`pkg/storage/multitier.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/storage/multitier.go): Orquestación multi-nivel, auto-promoción, resolución de URLs presigned y desalojo seguro de caché.
- [`pkg/storage/pool.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/storage/pool.go): Motor de distribución multi-cuenta de datasets, fragmentación y streaming.
- [`pkg/hfclient/client.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/hfclient/client.go): Cliente API de Hugging Face (datasets públicos y privados, commit y descarga).
- [`pkg/crypto/cipher.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/crypto/cipher.go): Cifrado y descifrado autenticado AES-256-GCM.
- [`pkg/db/db.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/db/db.go): Persistencia SQLite con soporte para ubicaciones multi-nivel (`object_locations`).
- [`pkg/s3api/router.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/s3api/router.go): Enrutador S3 compatible y ruta `/media/{bucket}/{key}`.
- [`pkg/dashboard/api.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/dashboard/api.go): Endpoints REST del panel y gestión de configuraciones de caché.
- [`pkg/dashboard/static/`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/dashboard/static/): SPA del panel de control web (`index.html`, `style.css`, `app.js`).

---

## Pruebas y Validación

Ejecutar la suite completa de pruebas unitarias y de integración:

```powershell
go test -v ./...
```
