package dashboard

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"hf2s3/pkg/db"
	"hf2s3/pkg/models"
)

const (
	maxShareItems      = 500
	maxShareTitleRunes = 120
	minSharePassword   = 6
	maxSharePassword   = 200
	minShareLifetime   = time.Minute
	maxShareLifetime   = 10 * 365 * 24 * time.Hour
	folderPageLimit    = 200
)

// shareView is a share as the console shows it: with its public URL.
type shareView struct {
	*models.Share
	URL     string `json:"url"`
	Expired bool   `json:"expired"`
}

func (h *DashboardHandler) shareURL(r *http.Request, token string) string {
	scheme, host := h.externalBase(r)
	return scheme + "://" + host + "/s/" + token
}

func (h *DashboardHandler) viewShare(r *http.Request, s *models.Share) shareView {
	return shareView{Share: s, URL: h.shareURL(r, s.Token), Expired: s.Expired(time.Now())}
}

func newShareToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// handleCreateShare turns a selection of files and folders into a link.
func (h *DashboardHandler) handleCreateShare(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Bucket           string             `json:"bucket"`
		Title            string             `json:"title"`
		Password         string             `json:"password"`
		ExpiresInSeconds int64              `json:"expires_in_seconds"` // 0 = never expires
		Items            []models.ShareItem `json:"items"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Solicitud inválida")
		return
	}
	ctx := r.Context()

	if ok, err := h.pool.DB().BucketExists(ctx, req.Bucket); err != nil || !ok {
		h.writeError(w, http.StatusNotFound, "El bucket no existe")
		return
	}
	if len(req.Items) == 0 {
		h.writeError(w, http.StatusBadRequest, "Selecciona al menos un archivo o carpeta")
		return
	}
	if len(req.Items) > maxShareItems {
		h.writeError(w, http.StatusBadRequest, "Demasiados elementos en un solo enlace")
		return
	}

	title := strings.TrimSpace(req.Title)
	if utf8.RuneCountInString(title) > maxShareTitleRunes {
		h.writeError(w, http.StatusBadRequest, "El título es demasiado largo")
		return
	}

	var passwordHash string
	if req.Password != "" {
		if len(req.Password) < minSharePassword || len(req.Password) > maxSharePassword {
			h.writeError(w, http.StatusBadRequest, "La contraseña debe tener entre 6 y 200 caracteres")
			return
		}
		hash, err := HashPassword(req.Password)
		if err != nil {
			h.fail(w, http.StatusInternalServerError, "No se pudo proteger el enlace", err)
			return
		}
		passwordHash = hash
	}

	var expiresAt *time.Time
	if req.ExpiresInSeconds != 0 {
		d := time.Duration(req.ExpiresInSeconds) * time.Second
		if req.ExpiresInSeconds < 0 || d < minShareLifetime || d > maxShareLifetime {
			h.writeError(w, http.StatusBadRequest, "La duración debe estar entre 1 minuto y 10 años")
			return
		}
		t := time.Now().UTC().Add(d)
		expiresAt = &t
	}

	seen := map[models.ShareItem]bool{}
	items := make([]models.ShareItem, 0, len(req.Items))
	for _, it := range req.Items {
		if it.Key == "" || len(it.Key) > maxKeyBytes {
			h.writeError(w, http.StatusBadRequest, "Clave de archivo inválida")
			return
		}
		if seen[it] {
			continue
		}
		seen[it] = true
		if it.IsPrefix {
			if !strings.HasSuffix(it.Key, "/") {
				h.writeError(w, http.StatusBadRequest, "Las carpetas deben terminar en /")
				return
			}
			first, err := h.pool.DB().ListObjectsPage(ctx, req.Bucket, it.Key, "", false, 1)
			if err != nil {
				h.fail(w, http.StatusInternalServerError, "No se pudo comprobar la carpeta", err)
				return
			}
			if len(first) == 0 {
				h.writeError(w, http.StatusBadRequest, "La carpeta «"+it.Key+"» está vacía")
				return
			}
		} else if _, err := h.pool.DB().HeadObject(ctx, req.Bucket, it.Key); err != nil {
			if errors.Is(err, db.ErrNotFound) {
				h.writeError(w, http.StatusNotFound, "No existe el archivo «"+it.Key+"»")
			} else {
				h.fail(w, http.StatusInternalServerError, "No se pudo comprobar el archivo", err)
			}
			return
		}
		items = append(items, it)
	}

	token, err := newShareToken()
	if err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo generar el enlace", err)
		return
	}
	share := &models.Share{Bucket: req.Bucket, Title: title, PasswordHash: passwordHash, ExpiresAt: expiresAt, Items: items}
	if err := h.pool.DB().CreateShare(ctx, share, token); err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo guardar el enlace", err)
		return
	}
	h.writeJSON(w, http.StatusCreated, h.viewShare(r, share))
}

func (h *DashboardHandler) handleListShares(w http.ResponseWriter, r *http.Request) {
	shares, err := h.pool.DB().ListShares(r.Context())
	if err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudieron listar los enlaces", err)
		return
	}
	views := make([]shareView, 0, len(shares))
	for _, s := range shares {
		views = append(views, h.viewShare(r, s))
	}
	h.writeJSON(w, http.StatusOK, views)
}

func (h *DashboardHandler) handleDeleteShare(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		h.writeError(w, http.StatusBadRequest, "ID inválido")
		return
	}
	switch err := h.pool.DB().DeleteShare(r.Context(), id); {
	case errors.Is(err, db.ErrNotFound):
		h.writeError(w, http.StatusNotFound, "El enlace no existe")
	case err != nil:
		h.fail(w, http.StatusInternalServerError, "No se pudo revocar el enlace", err)
	default:
		h.dropShareSessions(id)
		h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
	}
}

// handleListFolder lists the immediate children of a prefix, so the console
// can browse buckets as folders.
func (h *DashboardHandler) handleListFolder(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	bucket := q.Get("bucket")
	if bucket == "" {
		h.writeError(w, http.StatusBadRequest, "Bucket parameter is required")
		return
	}
	page, err := h.pool.DB().ListFolder(r.Context(), bucket, q.Get("prefix"), q.Get("cursor"), folderPageLimit)
	if err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudieron listar los objetos", err)
		return
	}
	if page.Folders == nil {
		page.Folders = []string{}
	}
	if page.Objects == nil {
		page.Objects = []models.Object{}
	}
	h.writeJSON(w, http.StatusOK, map[string]any{"folders": page.Folders, "objects": page.Objects, "next": page.Next})
}
