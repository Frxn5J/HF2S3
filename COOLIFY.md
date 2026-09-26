# Guía de Despliegue en Coolify: HF2S3 Gateway

Esta guía detalla paso a paso cómo desplegar **HF2S3** en [Coolify](https://coolify.io/) con persistencia de datos, certificados SSL automáticos y variables de entorno seguras.

---

## 1. Opciones de Despliegue en Coolify

Coolify soporta despliegues desde un repositorio Git o directamente mediante Docker Compose.

### Opción A: Despliegue desde Repositorio Git (Recomendado)

1. En el panel de Coolify, entra a tu **Proyecto** y selecciona **+ New Resource**.
2. Elige **Public Repository** o **Private Repository** (GitHub / GitLab).
3. Ingresa la URL de tu repositorio con el código de HF2S3.
4. En **Build Pack**, Coolify detectará automáticamente el archivo `Dockerfile`.
5. En **Ports Exposes**, define `8080`.
6. En **Domains**, ingresa tu dominio o subdominio (ejemplo: `https://s3.tudominio.com`). Coolify gestionará automáticamente los certificados SSL con Let's Encrypt.
7. Haz clic en **Deploy**.

---

### Opción B: Despliegue mediante Docker Compose

1. En Coolify, selecciona **+ New Resource** $\rightarrow$ **Docker Compose**.
2. Pega el contenido del archivo [`docker-compose.yml`](docker-compose.yml):

```yaml
version: '3.8'

services:
  hf2s3:
    build:
      context: .
      dockerfile: Dockerfile
    image: hf2s3:latest
    container_name: hf2s3-gateway
    restart: unless-stopped
    ports:
      - "8080:8080"
    environment:
      - PORT=8080
      - HF2S3_DB=/data/hf2s3_metadata.db
      - ADMIN_USERNAME=${ADMIN_USERNAME:-admin}
      - ADMIN_PASSWORD=${ADMIN_PASSWORD:-admin123}
      - HF2S3_ACCESS_KEY=${HF2S3_ACCESS_KEY:-hf2s3-access-key}
      - HF2S3_SECRET_KEY=${HF2S3_SECRET_KEY:-hf2s3-secret-key}
      - HF2S3_MASTER_KEY=${HF2S3_MASTER_KEY:-hf2s3-aes-master-passphrase-2026}
      - HF2S3_CHUNK_SIZE_MB=${HF2S3_CHUNK_SIZE_MB:-32}
      - HF2S3_REGION=${HF2S3_REGION:-us-east-1}
    volumes:
      - hf2s3_data:/data
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8080/api/health"]
      interval: 15s
      timeout: 5s
      retries: 3
      start_period: 5s

volumes:
  hf2s3_data:
    name: hf2s3_data
```

---

## 2. Configuración de Variables de Entorno en Coolify

En la pestaña **Environment Variables** de tu aplicación en Coolify, define las siguientes variables:

| Variable | Valor de Ejemplo | Descripción |
|---|---|---|
| `ADMIN_USERNAME` | `admin` | Usuario para iniciar sesión en el panel web |
| `ADMIN_PASSWORD` | `TuPasswordSeguro2026!` | Contraseña del panel web de administración |
| `HF2S3_ACCESS_KEY` | `mi-access-key-prod` | S3 Access Key ID para tus clientes y backends |
| `HF2S3_SECRET_KEY` | `mi-secret-key-muy-larga` | S3 Secret Access Key para autenticación S3 |
| `HF2S3_MASTER_KEY` | `frase-maestra-aes-256-gcm` | Frase para cifrar los chunks en Hugging Face |
| `HF2S3_DB` | `/data/hf2s3_metadata.db` | Ruta persistente de la base de datos SQLite |
| `HF2S3_CHUNK_SIZE_MB` | `32` | Tamaño del chunk (32 MB recomendado) |
| `HF2S3_REGION` | `us-east-1` | Región S3 informada |

> [!WARNING]
> **Importante:** Conserva tu `HF2S3_MASTER_KEY`. Si cambias esta frase en el futuro, no podrás descifrar los chunks previamente subidos a Hugging Face.

---

## 3. Persistencia de Datos (Volumen `/data`)

La base de datos SQLite con las cuentas, tokens y catálogo de archivos se almacena en `/data/hf2s3_metadata.db`.

En Coolify:
- En la sección **Storages / Persistent Storage**, asegúrate de que el volumen esté montado:
  - **Source / Name**: `hf2s3_data`
  - **Destination Path**: `/data`

De esta forma, cuando Coolify actualice la aplicación o reconstruya la imagen Docker, **la base de datos se mantendrá intacta**.

---

## 4. Migración de Base de Datos entre Instancias

### Cómo descargar la base de datos actual:
1. Entra a tu panel web de HF2S3 en `http://localhost:8080` (o tu instancia actual).
2. Ve a la pestaña **Configuración**.
3. En la tarjeta **Respaldo y Migración de Base de Datos**, haz clic en **Descargar Base de Datos (.db)**.
4. Obtendrás un archivo `hf2s3_backup_YYYY-MM-DD_HHMMSS.db`.

### Cómo restaurar en la instancia de Coolify:
Hay dos opciones muy sencillas:

#### Opción 1: Mediante SCP o File Manager en el Servidor
Copia el archivo descargado al volumen persistente de Coolify:
```bash
# En el servidor de Coolify
docker cp hf2s3_backup_2026-09-26.db hf2s3-gateway:/data/hf2s3_metadata.db
docker restart hf2s3-gateway
```

#### Opción 2: Montando el archivo antes del primer arranque
Coloca el archivo como `hf2s3_metadata.db` en el directorio de volumen de Docker en el servidor host:
`/var/lib/docker/volumes/hf2s3_data/_data/hf2s3_metadata.db`

¡Listo! Al reiniciar el contenedor, el nuevo servidor en Coolify reconocerá todas las cuentas de Hugging Face, buckets y objetos existentes de forma inmediata.
