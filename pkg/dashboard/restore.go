package dashboard

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// RestoreSummary describes a database backup that has been uploaded, validated
// and staged, waiting for the administrator to confirm.
type RestoreSummary struct {
	Filename        string     `json:"filename"`
	Encrypted       bool       `json:"encrypted"`
	StagedAt        time.Time  `json:"staged_at"`
	SchemaVersion   int        `json:"schema_version"`
	Accounts        int64      `json:"accounts"`
	Buckets         int64      `json:"buckets"`
	Objects         int64      `json:"objects"`
	Chunks          int64      `json:"chunks"`
	TotalBytes      int64      `json:"total_bytes"`
	LatestObjectAt  *time.Time `json:"latest_object_at,omitempty"`
	CurrentAccounts int64      `json:"current_accounts"`
	CurrentObjects  int64      `json:"current_objects"`
	Warnings        []string   `json:"warnings,omitempty"`
}

// RestoreResult is the outcome of the last restore applied at start-up.
type RestoreResult struct {
	At             time.Time `json:"at"`
	OK             bool      `json:"ok"`
	Message        string    `json:"message"`
	PreviousBackup string    `json:"previous_backup,omitempty"`
}

// RestoreError carries a message that is safe to show to the administrator
// (a validation failure such as "wrong master key"), as opposed to an internal error.
type RestoreError struct{ Message string }

func (e *RestoreError) Error() string { return e.Message }

// RestoreManager stages, confirms and applies database restores. Applying one
// restarts the process: the database file is replaced while nothing has it open.
type RestoreManager interface {
	// UploadDir is where uploads are written (a directory on the data volume).
	UploadDir() string
	// MaxUploadBytes bounds the size of an uploaded backup.
	MaxUploadBytes() int64
	// Stage validates the uploaded file (taking ownership of it) and stages it.
	Stage(ctx context.Context, uploadedPath, filename string) (*RestoreSummary, error)
	Pending() *RestoreSummary
	Cancel() error
	// Apply schedules the restart that swaps the database in.
	Apply() error
	LastResult() *RestoreResult
}

// SetRestoreManager enables the restore endpoints.
func (h *DashboardHandler) SetRestoreManager(m RestoreManager) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.restore = m
}

func (h *DashboardHandler) restoreManager(w http.ResponseWriter) RestoreManager {
	h.mu.RLock()
	m := h.restore
	h.mu.RUnlock()
	if m == nil {
		h.writeError(w, http.StatusNotImplemented, "La restauración desde el panel no está disponible en este despliegue. Usa: hf2s3 restore <archivo>")
	}
	return m
}

// handleRestoreStatus reports the staged restore (if any) and the last result.
func (h *DashboardHandler) handleRestoreStatus(w http.ResponseWriter, r *http.Request) {
	m := h.restoreManager(w)
	if m == nil {
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]any{
		"pending":        m.Pending(),
		"last_result":    m.LastResult(),
		"max_upload_mb":  m.MaxUploadBytes() >> 20,
		"restart_needed": true,
	})
}

// handleRestoreUpload streams the uploaded backup to disk (never into memory),
// validates it and stages it. Nothing is replaced until the administrator confirms.
func (h *DashboardHandler) handleRestoreUpload(w http.ResponseWriter, r *http.Request) {
	m := h.restoreManager(w)
	if m == nil {
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, m.MaxUploadBytes()+(1<<20))
	mr, err := r.MultipartReader()
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "Solicitud multipart inválida")
		return
	}

	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			h.writeError(w, http.StatusBadRequest, "No se recibió ningún archivo")
			return
		}
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "No se pudo leer la subida (¿supera el tamaño máximo de "+sizeMB(m.MaxUploadBytes())+"?)")
			return
		}
		if part.FormName() != "database" {
			_, _ = io.Copy(io.Discard, io.LimitReader(part, 1<<20))
			continue
		}

		tmp, err := os.CreateTemp(m.UploadDir(), "restore-upload-*.tmp")
		if err != nil {
			h.fail(w, http.StatusInternalServerError, "No se pudo guardar el archivo subido", err)
			return
		}
		tmpPath := tmp.Name()
		n, copyErr := io.Copy(tmp, io.LimitReader(part, m.MaxUploadBytes()+1))
		closeErr := tmp.Close()
		if copyErr != nil || closeErr != nil {
			_ = os.Remove(tmpPath)
			h.writeError(w, http.StatusBadRequest, "La subida se interrumpió o superó el tamaño máximo de "+sizeMB(m.MaxUploadBytes()))
			return
		}
		if n > m.MaxUploadBytes() {
			_ = os.Remove(tmpPath)
			h.writeError(w, http.StatusRequestEntityTooLarge, "El archivo supera el tamaño máximo de "+sizeMB(m.MaxUploadBytes()))
			return
		}

		summary, err := m.Stage(r.Context(), tmpPath, filepath.Base(part.FileName()))
		var restoreErr *RestoreError
		switch {
		case errors.As(err, &restoreErr):
			h.writeError(w, http.StatusBadRequest, restoreErr.Message)
		case err != nil:
			h.fail(w, http.StatusInternalServerError, "No se pudo validar la copia de seguridad", err)
		default:
			slog.Info("database backup staged for restore", "file", summary.Filename, "objects", summary.Objects)
			h.writeJSON(w, http.StatusOK, summary)
		}
		return
	}
}

// handleRestoreApply confirms the staged restore. It needs the admin password:
// replacing the database is the most destructive action the console offers.
func (h *DashboardHandler) handleRestoreApply(w http.ResponseWriter, r *http.Request) {
	m := h.restoreManager(w)
	if m == nil {
		return
	}
	var req struct {
		CurrentPassword string `json:"current_password"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid payload")
		return
	}
	if !h.adminAuth.VerifyPassword(req.CurrentPassword) {
		h.writeError(w, http.StatusForbidden, "La contraseña no es correcta")
		return
	}
	if m.Pending() == nil {
		h.writeError(w, http.StatusConflict, "No hay ninguna copia pendiente de restaurar")
		return
	}
	if err := m.Apply(); err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo iniciar la restauración", err)
		return
	}
	slog.Warn("database restore confirmed by the administrator; restarting to apply it")
	h.writeJSON(w, http.StatusAccepted, map[string]any{
		"restarting": true,
		"message":    "El servicio se está reiniciando para aplicar la restauración.",
	})
}

func (h *DashboardHandler) handleRestoreCancel(w http.ResponseWriter, r *http.Request) {
	m := h.restoreManager(w)
	if m == nil {
		return
	}
	if err := m.Cancel(); err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo cancelar la restauración", err)
		return
	}
	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func sizeMB(b int64) string {
	mb := b >> 20
	if mb >= 1024 {
		return itoa(mb>>10) + " GB"
	}
	return itoa(mb) + " MB"
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
