# HF2S3: Hugging Face Multi-Account S3/R2 Cloud Gateway

**HF2S3** es una pasarela de almacenamiento de alto rendimiento desarrollada en **Go** que unifica múltiples cuentas de **Hugging Face** para crear un espacio de almacenamiento compatible con la API **AWS S3 / Cloudflare R2**.

Cada cuenta gratuita de Hugging Face provee espacio en repositorios o datasets privados. Al conectar $N$ cuentas de 100 GB, HF2S3 conforma un pool distribuido de **$N \times 100\text{ GB}$**, permitiendo montar unidades de red o interactuar con clientes estándar como **Rclone, Cyberduck, AWS CLI, DuckDB o Boto3**.

---

## Características Principales

1. **Pool Distribuido Multi-Cuenta**:
   - Agrupación automática de cuentas de Hugging Face.
   - Algoritmo de balanceo por menor utilización relativa (*Least-Used Weighted*) para distribuir los chunks equitativamente.
   - Creación automática de repositorios dataset en modo privado (`hf2s3-vault` o personalizado).

2. **Cifrado Zero-Knowledge en Reposo (AES-256-GCM)**:
   - Los archivos entrantes se fragmentan en chunks (por defecto 32 MB).
   - Cada chunk es cifrado localmente con **AES-256-GCM** y un Nonce aleatorio de 12 bytes antes de enviarse a Hugging Face.
   - Hugging Face almacena únicamente blobs binarios opacos cifrados (`data/<uuid>.enc`).

3. **Compatibilidad Total S3 / Cloudflare R2**:
   - Soporte para `ListBuckets`, `CreateBucket`, `DeleteBucket`, `HeadBucket`.
   - Soporte para `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `DeleteObjects`.
   - Soporte para lecturas parciales con encabezado HTTP `Range: bytes=start-end` (streaming y reproducción multimedia).
   - Soporte para subidas multipart (`InitiateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`).
   - Autenticación con firmas AWS SigV4 y SigV2.

4. **Panel Web de Administración y Explorador de Archivos**:
   - Interfaz gráfica moderna en modo oscuro (*Dark Glassmorphism*).
   - **Gestor de Cuentas HF**: Alta con verificación instantánea de token `whoami`, cálculo de espacio y desactivación temporal.
   - **Creador y Gestor de Buckets**: Alta y baja de buckets compatibles con convenciones DNS de S3.
   - **Explorador y Visor de Archivos**: Subida por arrastrar y soltar (*drag-and-drop*), descarga directa, borrado e **inspección de chunks** (para auditar qué cuenta de Hugging Face almacena cada bloque del archivo).
   - **Guía Rápida de Conexión**: Fragmentos listos para copiar de Rclone, AWS CLI y Python Boto3.

5. **Seguridad y Acceso de Administrador**:
   - **Login de Administrador**: Autenticación protegida para el panel de control web mediante variables de entorno `ADMIN_USERNAME` y `ADMIN_PASSWORD`.
   - **Tokens de Sesión Criptográficos**: Sesiones con cookies HTTP-only y tokens de autorización `Bearer` / `X-Admin-Token`.

6. **Respaldo y Migración de Base de Datos sin Pérdida**:
   - Exportación de snapshot SQLite en caliente (`VACUUM INTO` + `wal_checkpoint`) desde la interfaz web o mediante `GET /api/admin/backup`.
   - Genera un archivo `.db` autónomo y consistente que puedes migrar a cualquier otro servidor o volumen Docker sin perder cuentas, buckets ni objetos.

7. **Despliegue Llave en Mano en Coolify y Docker**:
   - Imagen Docker multi-etapa ultra ligera (<30 MB) con certificados CA y usuario no-root.
   - Archivo `docker-compose.yml` preconfigurado con volumen persistente `/data` y healthcheck automático.
   - Consulta la [**Guía Completa de Despliegue en Coolify (COOLIFY.md)**](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/COOLIFY.md).

8. **Arquitectura de Alta Concurrencia y Multi-Usuario**:
   - **SQLite en Modo WAL de Alta Escala**: Múltiples conexiones simultáneas (`runtime.NumCPU() * 4`), lecturas concurrentes sin bloqueo, `cache_size` de 64 MB y timeout de contención de 30 segundos.
   - **Caché en Memoria (`sync.Map`)**: Comprobación instantánea de existencia de buckets en $O(1)$ sin consulta SQL en cada petición.
   - **Pool de Buffers en Memoria (`sync.Pool`)**: Reutilización de búferes de 32 MB para transferencias concurrentes, eliminando la recolección de basura (*GC pauses*) y fugas de memoria.
   - **Balanceo Dinámico en Vuelo (*In-Flight Accounting*)**: Distribución automática y en tiempo real de subidas concurrentes entre las cuentas de Hugging Face disponibles para evitar saturar un único token o repositorio.
   - **Subidas en Paralelo**: Chunks de archivos grandes se suben de forma concurrente con semáforo acotado (hasta 4x más velocidad).
   - **Pipeline de Descargas con Prefetch Asíncrono**: Mientras el cliente consume el chunk $N$, el chunk $N+1$ ya se descarga y descifra en segundo plano, maximizando el rendimiento de streaming.
   - **HTTP Client con Connection Pooling y Reintentos Automáticos**: Hasta 1000 conexiones inactivas globales, 200 por host, y reintentos con *Exponential Backoff* y *Jitter* ante respuestas 429 o 503 de Hugging Face.
   - **Protección contra Bloqueos de Conexión**: `ReadHeaderTimeout` y `IdleTimeout` configurados para resistir ataques Slowloris manteniendo streaming ininterrumpido.


---

## Arquitectura del Sistema

```
                      +-----------------------------+
                      |   Clientes S3 / R2 / Web    |
                      | (Rclone, Cyberduck, Navegador)|
                      +--------------+--------------+
                                     |
                             HTTP :8080 (REST)
                                     |
                      +--------------v--------------+
                      |           HF2S3             |
                      |   Gateway Unificado en Go   |
                      +--------------+--------------+
                                     |
              +----------------------+----------------------+
              |                      |                      |
      [ AES-256-GCM ]        [ SQLite Metadatos ]    [ Gestor de Pool ]
      (Cifrado de Chunks)     (Catálogo y Estado)   (Balanceo Cuentas)
              |                                             |
              +----------------------+----------------------+
                                     |
                        Hugging Face Commit API
                                     |
         +---------------------------+---------------------------+
         |                           |                           |
+--------v--------+         +--------v--------+         +--------v--------+
| Cuenta HF #1    |         | Cuenta HF #2    |         | Cuenta HF #N    |
| (100 GB Privado)|         | (100 GB Privado)|         | (100 GB Privado)|
+-----------------+         +-----------------+         +-----------------+
```

---

## Requisitos y Compilación

- **Go**: 1.22 o superior (probado en Go 1.27.1 Windows amd64).
- No requiere dependencias del sistema externo (compilación nativa pura).

### Compilar el ejecutable

```powershell
go build -v -o hf2s3.exe ./cmd/hf2s3
```

---

## Ejecución del Servidor

Para iniciar el servidor con los valores por defecto (puerto `8080`, base de datos `hf2s3_metadata.db`):

```powershell
.\hf2s3.exe
```

### Opciones de Línea de Comandos y Variables de Entorno

| Flag | Variable de Entorno | Valor por Defecto | Descripción |
|---|---|---|---|
| `-port` | `PORT` o `HF2S3_PORT` | `8080` | Puerto HTTP para la API S3 y el Dashboard Web |
| `-db` | `HF2S3_DB` | `hf2s3_metadata.db` | Ruta del archivo de base de datos SQLite |
| `-chunk-size` | `HF2S3_CHUNK_SIZE_MB` | `32` | Tamaño del fragmento en Megabytes |
| `-access-key` | `HF2S3_ACCESS_KEY` | `hf2s3-access-key` | S3 Access Key ID |
| `-secret-key` | `HF2S3_SECRET_KEY` | `hf2s3-secret-key` | S3 Secret Access Key |
| `-master-key` | `HF2S3_MASTER_KEY` | (Generada) | Frase de paso para la derivación de clave AES-256 |
| `-region` | `HF2S3_REGION` | `us-east-1` | Región S3 reportada |

Ejemplo con parámetros personalizados:

```powershell
.\hf2s3.exe -port 9000 -access-key "mi-usuario-s3" -secret-key "mi-clave-super-secreta" -chunk-size 50
```

---

## Uso del Panel de Control Web

1. Abre tu navegador en `http://localhost:8080/`.
2. Dirígete a la pestaña **Cuentas HF** y haz clic en **+ Conectar Cuenta**.
3. Ingresa un token de Hugging Face con permisos de escritura (*Write*) obtenido en [huggingface.co/settings/tokens](https://huggingface.co/settings/tokens).
4. El sistema verificará tu usuario y creará automáticamente un dataset privado (por defecto `hf2s3-vault`).
5. Repite el proceso con tantas cuentas de Hugging Face como desees para expandir la capacidad del pool.
6. En la pestaña **Buckets y Archivos**, crea tus buckets y comienza a subir archivos mediante la interfaz o mediante clientes S3.

---

## Conexión con Clientes S3 / Cloudflare R2

### 1. Rclone (Montar como Disco Local)

Agrega la siguiente sección en tu archivo de configuración de Rclone (`%APPDATA%/rclone/rclone.conf` o `~/.config/rclone/rclone.conf`):

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

Para listar buckets:
```powershell
rclone lsd hf2s3:
```

Para montar como una letra de unidad en Windows (ej. `X:`):
```powershell
rclone mount hf2s3:mi-bucket X: --vfs-cache-mode full
```

### 2. AWS CLI

```powershell
# Configurar variables en la sesión
$env:AWS_ACCESS_KEY_ID="hf2s3-access-key"
$env:AWS_SECRET_ACCESS_KEY="hf2s3-secret-key"

# Listar buckets
aws --endpoint-url=http://localhost:8080 s3 ls

# Subir un archivo
aws --endpoint-url=http://localhost:8080 s3 cp ./archivo.iso s3://mi-bucket/archivo.iso

# Sincronizar un directorio completo
aws --endpoint-url=http://localhost:8080 s3 sync ./datos s3://mi-bucket/datos/
```

### 3. Cyberduck

1. Abre Cyberduck y selecciona **Nueva Conexión**.
2. Elige el perfil **Amazon S3** (o crea un perfil genérico S3).
3. **Servidor**: `localhost`
4. **Puerto**: `8080`
5. Desmarca **Usar SSL** si estás ejecutándolo localmente en HTTP.
6. **Access Key ID**: `hf2s3-access-key`
7. **Secret Access Key**: `hf2s3-secret-key`

### 4. Python (Boto3)

```python
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:8080',
    aws_access_key_id='hf2s3-access-key',
    aws_secret_access_key='hf2s3-secret-key',
    region_name='us-east-1'
)

# Listar buckets
response = s3.list_buckets()
for b in response['Buckets']:
    print(f"Bucket: {b['Name']}")

# Subir archivo
s3.upload_file('documento.pdf', 'mi-bucket', 'documento.pdf')

# Descargar archivo
s3.download_file('mi-bucket', 'documento.pdf', 'descargado.pdf')
```

---

## Estructura del Código

- [`cmd/hf2s3/main.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/cmd/hf2s3/main.go): Punto de entrada, configuración CLI, inicialización del pool y enrutador unificado.
- [`pkg/models/models.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/models/models.go): Definición de tipos y entidades del dominio (cuentas, buckets, objetos, chunks, multiparts).
- [`pkg/crypto/cipher.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/crypto/cipher.go): Cifrado y descifrado autenticado AES-256-GCM.
- [`pkg/db/db.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/db/db.go): Capa de persistencia SQLite transaccional con modo WAL.
- [`pkg/hfclient/client.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/hfclient/client.go): Cliente HTTP nativo para la API de Hugging Face (Whoami, Create Repo, Commit Chunks, Download, Treesize).
- [`pkg/storage/pool.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/storage/pool.go): Motor de distribución multi-cuenta, fragmentación de chunks, subidas, descargas por streaming y operaciones multipart.
- [`pkg/s3api/router.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/s3api/router.go): Enrutador y controladores de la API REST S3 / R2 compatible con soporte SigV4.
- [`pkg/dashboard/api.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/dashboard/api.go): Endpoints REST de administración y servidor de interfaz embebida.
- [`pkg/dashboard/static/`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/pkg/dashboard/static/): Aplicación web de panel de control (`index.html`, `style.css`, `app.js`).

---

## Verificación de Calidad y Pruebas Unitarias

El proyecto cuenta con cobertura de pruebas en todas sus capas:

```powershell
go test -v ./...
```

---

## Benchmark Automático de Concurrencia y Capacidad (1 Gbps)

Se incluye una herramienta CLI de pruebas de estrés automatizadas ([`cmd/benchmark/main.go`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/cmd/benchmark/main.go)) compilada en [`benchmark.exe`](file:///c:/Users/Desarrollo/Documents/OhMyVita/HF2S3/benchmark.exe).

Esta herramienta eleva de forma escalonada la cantidad de usuarios concurrentes realizando ciclos continuos de subida (`PUT`), descarga (`GET`) y validación de integridad contra la API S3:

```powershell
# Ejecutar test de estrés automático con detección de saturación
.\benchmark.exe -endpoint http://localhost:8080 -bucket test -start-users 5 -max-users 100 -step 10
```

### Opciones de la Herramienta de Benchmark:

| Flag | Valor por Defecto | Descripción |
|---|---|---|
| `-endpoint` | `http://localhost:8080` | URL del gateway HF2S3 |
| `-bucket` | `test` | Nombre del bucket a probar |
| `-access-key` | `hf2s3-access-key` | S3 Access Key ID |
| `-secret-key` | `hf2s3-secret-key` | S3 Secret Access Key |
| `-file-size-kb` | `256` | Tamaño de archivo de prueba en KB |
| `-start-users` | `5` | Usuarios iniciales en la prueba |
| `-step` | `10` | Incremento de usuarios por cada paso |
| `-max-users` | `100` | Límite máximo de usuarios a testear |
| `-duration` | `5s` | Duración de muestreo en cada nivel |
| `-auto-detect` | `true` | Se detiene automáticamente al detectar saturación |

