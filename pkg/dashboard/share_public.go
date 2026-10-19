package dashboard

import (
	"archive/zip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"hf2s3/pkg/db"
	"hf2s3/pkg/models"
	"hf2s3/pkg/s3api"
)

// Shared links are served from /s/{token}. They are public by design: the
// token is the credential (128 random bits), optionally backed by a password.
// Everything here is read-only and limited to the keys the share covers.

const (
	shareCookieName  = "hf2s3_share"
	shareSessionTTL  = 12 * time.Hour
	maxShareSessions = 20000
	maxShareTokenLen = 64
	maxZipFiles      = 20000
	minRedirectTTL   = 30 * time.Second
	maxRedirectTTL   = time.Hour
)

// shareCSP is the console policy plus what the public page needs: media and
// images may come from the cache bucket (https) after a redirect.
const shareCSP = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; " +
	"font-src https://fonts.gstatic.com; img-src 'self' data: https:; media-src 'self' https:; connect-src 'self'; " +
	"object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'"

type shareSession struct {
	shareID int64
	expires time.Time
}

// shareSessions remembers which browsers entered the right password. It is
// in memory: a restart just asks for the password again.
type shareSessions struct {
	mu sync.Mutex
	m  map[string]shareSession
}

func newShareSessions() *shareSessions { return &shareSessions{m: map[string]shareSession{}} }

func (s *shareSessions) create(shareID int64, now time.Time) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	token := base64.RawURLEncoding.EncodeToString(b)

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.m) >= maxShareSessions {
		for k, v := range s.m {
			if !now.Before(v.expires) {
				delete(s.m, k)
			}
		}
		if len(s.m) >= maxShareSessions { // flood: forget everyone, they re-enter the password
			s.m = map[string]shareSession{}
		}
	}
	s.m[token] = shareSession{shareID: shareID, expires: now.Add(shareSessionTTL)}
	return token, nil
}

func (s *shareSessions) valid(token string, shareID int64, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[token]
	if ok && !now.Before(v.expires) {
		delete(s.m, token)
		return false
	}
	return ok && v.shareID == shareID
}

func (s *shareSessions) dropShare(shareID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range s.m {
		if v.shareID == shareID {
			delete(s.m, k)
		}
	}
}

func (h *DashboardHandler) dropShareSessions(shareID int64) { h.shareSess.dropShare(shareID) }

func newShareLimiter(max int) *loginLimiter {
	return &loginLimiter{perIP: map[string][]time.Time{}, window: 5 * time.Minute, maxPerIP: max, maxGlobal: 1 << 30}
}

// shareHeaders keeps shared pages out of caches, search engines and referrers.
func shareHeaders(w http.ResponseWriter) {
	hd := w.Header()
	hd.Set("Content-Security-Policy", shareCSP)
	hd.Set("Cache-Control", "no-store")
	hd.Set("X-Robots-Tag", "noindex, nofollow")
	hd.Set("Referrer-Policy", "no-referrer")
	hd.Set("X-Content-Type-Options", "nosniff")
}

// openShare resolves the token of the request. It answers 404 for unknown or
// revoked links and 410 for expired ones, and reports whether to continue.
func (h *DashboardHandler) openShare(w http.ResponseWriter, r *http.Request) (*models.Share, bool) {
	shareHeaders(w)
	token := r.PathValue("token")
	if token == "" || len(token) > maxShareTokenLen {
		h.writeError(w, http.StatusNotFound, "Este enlace no existe o fue revocado")
		return nil, false
	}
	sh, err := h.pool.DB().GetShareByToken(r.Context(), token)
	switch {
	case errors.Is(err, db.ErrNotFound):
		h.writeError(w, http.StatusNotFound, "Este enlace no existe o fue revocado")
		return nil, false
	case err != nil:
		h.fail(w, http.StatusInternalServerError, "No se pudo abrir el enlace", err)
		return nil, false
	}
	if sh.Expired(time.Now()) {
		h.writeError(w, http.StatusGone, "Este enlace ha caducado")
		return nil, false
	}
	return sh, true
}

func (h *DashboardHandler) shareUnlocked(r *http.Request, sh *models.Share) bool {
	if !sh.HasPassword {
		return true
	}
	c, err := r.Cookie(shareCookieName)
	return err == nil && h.shareSess.valid(c.Value, sh.ID, time.Now())
}

// openUnlockedShare is openShare plus the password check.
func (h *DashboardHandler) openUnlockedShare(w http.ResponseWriter, r *http.Request) (*models.Share, bool) {
	sh, ok := h.openShare(w, r)
	if !ok {
		return nil, false
	}
	if !h.shareUnlocked(r, sh) {
		h.writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "Este enlace está protegido con contraseña", "locked": true})
		return nil, false
	}
	return sh, true
}

// shareRoot is the folder the share opens in: the single shared folder when
// that is all there is, otherwise "" (a virtual root listing the items).
func shareRoot(sh *models.Share) string {
	if len(sh.Items) == 1 && sh.Items[0].IsPrefix {
		return sh.Items[0].Key
	}
	return ""
}

func (h *DashboardHandler) handleSharePage(w http.ResponseWriter, r *http.Request) {
	shareHeaders(w)
	page, err := staticFS.ReadFile("static/share.html")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(page)
}

func (h *DashboardHandler) handleShareInfo(w http.ResponseWriter, r *http.Request) {
	sh, ok := h.openShare(w, r)
	if !ok {
		return
	}
	info := map[string]any{
		"has_password": sh.HasPassword,
		"expires_at":   sh.ExpiresAt,
	}
	if unlocked := h.shareUnlocked(r, sh); unlocked {
		root := shareRoot(sh)
		name := sh.Title
		if name == "" && root != "" {
			name = baseName(root)
		}
		info["unlocked"] = true
		info["title"] = name
		info["root"] = root
	} else {
		info["unlocked"] = false
	}
	h.writeJSON(w, http.StatusOK, info)
}

func (h *DashboardHandler) handleShareUnlock(w http.ResponseWriter, r *http.Request) {
	sh, ok := h.openShare(w, r)
	if !ok {
		return
	}
	if !sh.HasPassword {
		h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
		return
	}

	now := time.Now()
	ipKey := h.adminAuth.ClientIP(r) + "|" + strconv.FormatInt(sh.ID, 10)
	shareKey := strconv.FormatInt(sh.ID, 10)
	wait := h.shareIPLimiter.retryAfter(ipKey, now)
	if w2 := h.shareLimiter.retryAfter(shareKey, now); w2 > wait {
		wait = w2
	}
	if wait > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())+1))
		h.writeError(w, http.StatusTooManyRequests, "Demasiados intentos. Espera unos minutos.")
		return
	}

	var req struct {
		Password string `json:"password"`
	}
	if err := decodeJSON(w, r, &req); err != nil || len(req.Password) > maxSharePassword {
		h.writeError(w, http.StatusBadRequest, "Solicitud inválida")
		return
	}
	if !VerifyPassword(sh.PasswordHash, req.Password) {
		h.shareIPLimiter.fail(ipKey, now)
		h.shareLimiter.fail(shareKey, now)
		h.writeError(w, http.StatusForbidden, "Contraseña incorrecta")
		return
	}
	h.shareIPLimiter.success(ipKey)

	session, err := h.shareSess.create(sh.ID, now)
	if err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo abrir el enlace", err)
		return
	}
	ttl := shareSessionTTL
	if sh.ExpiresAt != nil {
		if rem := time.Until(*sh.ExpiresAt); rem < ttl {
			ttl = rem
		}
	}
	http.SetCookie(w, &http.Cookie{
		Name:     shareCookieName,
		Value:    session,
		Path:     "/s/" + r.PathValue("token"),
		MaxAge:   int(ttl.Seconds()),
		HttpOnly: true,
		Secure:   h.adminAuth.IsSecureRequest(r),
		SameSite: http.SameSiteLaxMode,
	})
	h.writeJSON(w, http.StatusOK, map[string]bool{"success": true})
}

type shareFileEntry struct {
	Name        string    `json:"name"`
	Key         string    `json:"key"`
	Size        int64     `json:"size"`
	Kind        string    `json:"kind"` // video, audio, image or file
	ContentType string    `json:"content_type,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type shareFolderEntry struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

func fileEntry(o *models.Object) shareFileEntry {
	ctype, kind := inlineType(o.ContentType, o.Key)
	if kind == "" {
		kind = "file"
	}
	return shareFileEntry{Name: baseName(o.Key), Key: o.Key, Size: o.Size, Kind: kind, ContentType: ctype, UpdatedAt: o.UpdatedAt}
}

func (h *DashboardHandler) handleShareList(w http.ResponseWriter, r *http.Request) {
	sh, ok := h.openUnlockedShare(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	q := r.URL.Query()
	path := q.Get("path")
	if path == "" {
		path = shareRoot(sh)
	}

	folders := []shareFolderEntry{}
	files := []shareFileEntry{}
	next := ""

	if path == "" { // virtual root: the shared items themselves
		for _, it := range sh.Items {
			if it.IsPrefix {
				folders = append(folders, shareFolderEntry{Name: baseName(it.Key), Path: it.Key})
				continue
			}
			obj, err := h.pool.DB().HeadObject(ctx, sh.Bucket, it.Key)
			if errors.Is(err, db.ErrNotFound) {
				continue // deleted after sharing
			}
			if err != nil {
				h.fail(w, http.StatusInternalServerError, "No se pudo leer el contenido", err)
				return
			}
			files = append(files, fileEntry(obj))
		}
	} else {
		if !strings.HasSuffix(path, "/") || !sh.CoversFolder(path) {
			h.writeError(w, http.StatusNotFound, "La carpeta no existe en este enlace")
			return
		}
		page, err := h.pool.DB().ListFolder(ctx, sh.Bucket, path, q.Get("cursor"), folderPageLimit)
		if err != nil {
			h.fail(w, http.StatusInternalServerError, "No se pudo leer el contenido", err)
			return
		}
		for _, f := range page.Folders {
			folders = append(folders, shareFolderEntry{Name: baseName(f), Path: f})
		}
		for i := range page.Objects {
			files = append(files, fileEntry(&page.Objects[i]))
		}
		next = page.Next
	}

	h.writeJSON(w, http.StatusOK, map[string]any{"path": path, "folders": folders, "files": files, "next": next})
}

func (h *DashboardHandler) countShareDownload(sh *models.Share, n int64) {
	go func() {
		if err := h.pool.DB().AddShareDownloads(context.Background(), sh.ID, n); err != nil {
			slog.Warn("could not count a shared download", "err", err)
		}
	}()
}

// handleShareFile serves one shared object: streamed with Range support for
// players, or as a download. Cached objects are redirected to the cache
// bucket so the bytes bypass this server; the redirect never outlives the link.
func (h *DashboardHandler) handleShareFile(w http.ResponseWriter, r *http.Request) {
	sh, ok := h.openUnlockedShare(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	key := r.URL.Query().Get("k")
	if key == "" || !sh.Covers(key) {
		h.writeError(w, http.StatusNotFound, "Archivo no encontrado")
		return
	}
	obj, err := h.pool.DB().HeadObject(ctx, sh.Bucket, key)
	if errors.Is(err, db.ErrNotFound) {
		h.writeError(w, http.StatusNotFound, "Archivo no encontrado")
		return
	}
	if err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo leer el archivo", err)
		return
	}

	ctype, kind := inlineType(obj.ContentType, obj.Key)
	inline := kind != "" && r.URL.Query().Get("dl") != "1"

	byteRange, unsatisfiable := s3api.ParseRange(r.Header.Get("Range"), obj.Size)
	if unsatisfiable {
		w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(obj.Size, 10))
		h.writeError(w, http.StatusRequestedRangeNotSatisfiable, "Rango no válido")
		return
	}
	if r.Method == http.MethodGet && (byteRange == nil || byteRange.Start == 0) {
		h.countShareDownload(sh, 1) // one per file, not one per player range request
	}

	if obj.HasCache && r.Method == http.MethodGet {
		ttl := maxRedirectTTL
		if sh.ExpiresAt != nil {
			if rem := time.Until(*sh.ExpiresAt); rem < ttl {
				ttl = rem
			}
		}
		if ttl < minRedirectTTL {
			ttl = minRedirectTTL
		}
		if url := h.pool.PresignedCacheURL(ctx, obj, ttl); strings.HasPrefix(url, "https://") {
			w.Header().Set("Location", url)
			w.WriteHeader(http.StatusFound)
			return
		}
	}

	hd := w.Header()
	hd.Set("ETag", obj.ETag)
	hd.Set("Last-Modified", obj.UpdatedAt.UTC().Format(http.TimeFormat))
	hd.Set("Accept-Ranges", "bytes")
	hd.Set("Content-Security-Policy", "default-src 'none'; sandbox")
	disp := "attachment"
	hd.Set("Content-Type", "application/octet-stream")
	if inline {
		disp = "inline"
		hd.Set("Content-Type", ctype)
	}
	if v := mime.FormatMediaType(disp, map[string]string{"filename": baseName(obj.Key)}); v != "" {
		disp = v
	}
	hd.Set("Content-Disposition", disp)

	status, length := http.StatusOK, obj.Size
	if byteRange != nil {
		status, length = http.StatusPartialContent, byteRange.End-byteRange.Start+1
		hd.Set("Content-Range", "bytes "+strconv.FormatInt(byteRange.Start, 10)+"-"+strconv.FormatInt(byteRange.End, 10)+"/"+strconv.FormatInt(obj.Size, 10))
	}
	hd.Set("Content-Length", strconv.FormatInt(length, 10))
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}

	_, reader, err := h.pool.GetObject(ctx, sh.Bucket, key, byteRange)
	if err != nil {
		hd.Del("Content-Range")
		hd.Del("Content-Length")
		hd.Set("Content-Type", "application/json")
		if errors.Is(err, db.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, "Archivo no encontrado")
		} else {
			h.fail(w, http.StatusBadGateway, "No se pudo leer el archivo", err)
		}
		return
	}
	defer reader.Close()
	w.WriteHeader(status)
	_, _ = io.Copy(w, reader)
}

// zipObjects gathers the objects to archive for a share (or one of its
// folders) with the name each gets inside the archive.
type zipEntry struct {
	name string
	obj  models.Object
}

func (h *DashboardHandler) collectZip(r *http.Request, sh *models.Share, path string) ([]zipEntry, error) {
	ctx := r.Context()
	var entries []zipEntry
	seen := map[string]bool{}
	add := func(o models.Object, base string) error {
		if seen[o.Key] || strings.HasSuffix(o.Key, "/") {
			return nil
		}
		if len(entries) >= maxZipFiles {
			return errTooManyFiles
		}
		seen[o.Key] = true
		entries = append(entries, zipEntry{name: safeZipName(strings.TrimPrefix(o.Key, base)), obj: o})
		return nil
	}
	walk := func(prefix, base string) error {
		after := ""
		for {
			page, err := h.pool.DB().ListObjectsPage(ctx, sh.Bucket, prefix, after, false, 500)
			if err != nil {
				return err
			}
			for _, o := range page {
				if err := add(o, base); err != nil {
					return err
				}
			}
			if len(page) < 500 {
				return nil
			}
			after = page[len(page)-1].Key
		}
	}

	if path != "" { // one folder: the archive holds that folder
		base := path[:strings.LastIndex(strings.TrimSuffix(path, "/"), "/")+1]
		return entries, walk(path, base)
	}
	for _, it := range sh.Items {
		if it.IsPrefix {
			if err := walk(it.Key, ""); err != nil {
				return nil, err
			}
			continue
		}
		obj, err := h.pool.DB().HeadObject(ctx, sh.Bucket, it.Key)
		if errors.Is(err, db.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if err := add(*obj, ""); err != nil {
			return nil, err
		}
	}
	return entries, nil
}

var errTooManyFiles = errors.New("too many files for one archive")

// handleShareZip streams everything the share covers (or one folder of it) as
// an uncompressed ZIP: media is already compressed, so storing is free.
func (h *DashboardHandler) handleShareZip(w http.ResponseWriter, r *http.Request) {
	sh, ok := h.openUnlockedShare(w, r)
	if !ok {
		return
	}
	path := r.URL.Query().Get("path")
	archiveName := sh.Title
	switch {
	case path != "":
		if !strings.HasSuffix(path, "/") || !sh.CoversFolder(path) {
			h.writeError(w, http.StatusNotFound, "La carpeta no existe en este enlace")
			return
		}
		archiveName = baseName(path)
	case archiveName == "" && shareRoot(sh) != "":
		archiveName = baseName(shareRoot(sh))
	}
	if archiveName == "" {
		archiveName = "compartido"
	}

	root := shareRoot(sh)
	zipPath := path
	if zipPath == "" {
		zipPath = root
	}
	entries, err := h.collectZip(r, sh, zipPath)
	if errors.Is(err, errTooManyFiles) {
		h.writeError(w, http.StatusRequestEntityTooLarge, "Hay demasiados archivos para un solo ZIP; descarga por carpetas")
		return
	}
	if err != nil {
		h.fail(w, http.StatusInternalServerError, "No se pudo preparar el ZIP", err)
		return
	}
	if len(entries) == 0 {
		h.writeError(w, http.StatusNotFound, "No hay archivos que descargar")
		return
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })

	hd := w.Header()
	hd.Set("Content-Type", "application/zip")
	hd.Set("Content-Security-Policy", "default-src 'none'; sandbox")
	if v := mime.FormatMediaType("attachment", map[string]string{"filename": archiveName + ".zip"}); v != "" {
		hd.Set("Content-Disposition", v)
	}
	h.countShareDownload(sh, 1)

	zw := zip.NewWriter(w)
	for _, e := range entries {
		fw, err := zw.CreateHeader(&zip.FileHeader{Name: e.name, Method: zip.Store, Modified: e.obj.UpdatedAt})
		if err != nil {
			panic(http.ErrAbortHandler)
		}
		_, reader, err := h.pool.GetObject(r.Context(), sh.Bucket, e.obj.Key, nil)
		if err != nil {
			slog.Error("shared zip: could not read an object", "err", err)
			panic(http.ErrAbortHandler) // cut the connection: a truncated zip must not look complete
		}
		_, err = io.Copy(fw, reader)
		reader.Close()
		if err != nil {
			panic(http.ErrAbortHandler)
		}
	}
	if err := zw.Close(); err != nil {
		panic(http.ErrAbortHandler)
	}
}

// safeZipName keeps an archive entry inside the extraction folder: S3 keys may
// hold "..", "." or leading slashes, which a careless unzip would follow.
func safeZipName(name string) string {
	parts := strings.Split(name, "/")
	kept := parts[:0]
	for _, p := range parts {
		if p != "" && p != "." && p != ".." {
			kept = append(kept, p)
		}
	}
	if len(kept) == 0 {
		return "file"
	}
	return strings.Join(kept, "/")
}
