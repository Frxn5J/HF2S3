// HF2S3 shared link viewer. Read-only: browse, play, download.
(() => {
  'use strict';

  const token = location.pathname.split('/')[2] || '';
  const base = '/s/' + token;
  const $ = (id) => document.getElementById(id);

  const views = ['shareLoading', 'shareError', 'shareGate', 'shareView'];
  function show(name) {
    views.forEach((v) => $(v).classList.toggle('is-hidden', v !== name));
  }

  // Everything that comes from the server is put in the page with textContent
  // or attributes, never as HTML.
  function el(tag, props, ...children) {
    const node = document.createElement(tag);
    Object.entries(props || {}).forEach(([k, v]) => {
      if (k === 'class') node.className = v;
      else if (k === 'text') node.textContent = v;
      else node.setAttribute(k, v);
    });
    children.forEach((c) => c && node.appendChild(c));
    return node;
  }

  function icon(kind) {
    const paths = {
      folder: '<path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"></path>',
      video: '<rect x="2" y="4" width="20" height="16" rx="2"></rect><polygon points="10 9 15 12 10 15 10 9"></polygon>',
      audio: '<path d="M9 18V5l12-2v13"></path><circle cx="6" cy="18" r="3"></circle><circle cx="18" cy="16" r="3"></circle>',
      image: '<rect x="3" y="3" width="18" height="18" rx="2"></rect><circle cx="8.5" cy="8.5" r="1.5"></circle><polyline points="21 15 16 10 5 21"></polyline>',
      file: '<path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path><polyline points="13 2 13 9 20 9"></polyline>',
    };
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('viewBox', '0 0 24 24');
    svg.setAttribute('width', '18');
    svg.setAttribute('height', '18');
    svg.setAttribute('fill', 'none');
    svg.setAttribute('stroke', 'currentColor');
    svg.setAttribute('stroke-width', '2');
    svg.setAttribute('aria-hidden', 'true');
    svg.innerHTML = paths[kind] || paths.file; // constant markup, no server data
    return svg;
  }

  function formatBytes(bytes) {
    if (!bytes) return '0 B';
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), sizes.length - 1);
    return parseFloat((bytes / Math.pow(1024, i)).toFixed(2)) + ' ' + sizes[i];
  }

  function fileURL(key, download) {
    return base + '/file?k=' + encodeURIComponent(key) + (download ? '&dl=1' : '');
  }

  // --- State ---------------------------------------------------------------
  let info = null;
  let root = '';
  let path = '';
  let folders = [];
  let files = [];
  let next = '';
  let expiryTimer = null;

  function showError(title, text, glyph) {
    $('shareErrorTitle').textContent = title;
    $('shareErrorText').textContent = text;
    $('shareErrorIcon').textContent = glyph || '!';
    show('shareError');
  }

  async function getJSON(url) {
    const res = await fetch(url, { credentials: 'same-origin' });
    let data = {};
    try { data = await res.json(); } catch (e) { /* not JSON */ }
    return { res, data };
  }

  function describeExpiry(expiresAt) {
    if (!expiresAt) return 'Sin fecha de caducidad';
    const left = new Date(expiresAt).getTime() - Date.now();
    if (left <= 0) return 'Caducado';
    const mins = Math.ceil(left / 60000);
    let rel;
    if (mins < 60) rel = mins + ' min';
    else if (mins < 60 * 48) rel = Math.round(mins / 60) + ' h';
    else rel = Math.round(mins / 1440) + ' días';
    return 'Caduca en ' + rel + ' · ' + new Date(expiresAt).toLocaleString();
  }

  function startExpiryClock(expiresAt, ...targets) {
    clearInterval(expiryTimer);
    const paint = () => targets.forEach((t) => { t.textContent = describeExpiry(expiresAt); });
    paint();
    if (expiresAt) expiryTimer = setInterval(paint, 30000);
  }

  // --- Boot ----------------------------------------------------------------
  async function boot() {
    show('shareLoading');
    let r;
    try {
      r = await getJSON(base + '/info');
    } catch (e) {
      showError('Sin conexión', 'No se pudo contactar con el servidor. Inténtalo de nuevo.');
      return;
    }
    if (r.res.status === 410) {
      showError('Este enlace ha caducado', 'Pide a quien te lo envió que genere uno nuevo.', '⏱');
      return;
    }
    if (!r.res.ok) {
      showError('Enlace no disponible', 'No existe o fue revocado por quien lo compartió.');
      return;
    }
    info = r.data;
    if (!info.unlocked) {
      startExpiryClock(info.expires_at, $('shareGateExpiry'));
      show('shareGate');
      $('shareGatePassword').focus();
      return;
    }
    root = info.root || '';
    $('shareTitle').textContent = info.title || 'Archivos compartidos';
    document.title = info.title || 'Archivos compartidos';
    startExpiryClock(info.expires_at, $('shareExpiry'));
    show('shareView');
    await openFolder(root);
  }

  $('shareGateForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const err = $('shareGateError');
    const btn = $('shareGateButton');
    err.classList.add('is-hidden');
    btn.disabled = true;
    try {
      const res = await fetch(base + '/unlock', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: $('shareGatePassword').value }),
      });
      if (res.ok) {
        $('shareGatePassword').value = '';
        await boot();
        return;
      }
      let data = {};
      try { data = await res.json(); } catch (x) { /* not JSON */ }
      err.textContent = data.error || 'No se pudo abrir el enlace';
      err.classList.remove('is-hidden');
      $('shareGatePassword').select();
    } catch (x) {
      err.textContent = 'Sin conexión con el servidor';
      err.classList.remove('is-hidden');
    } finally {
      btn.disabled = false;
    }
  });

  // --- Browsing ------------------------------------------------------------
  async function openFolder(target) {
    path = target;
    folders = [];
    files = [];
    next = '';
    await loadPage('');
  }

  async function loadPage(cursor) {
    const q = '?path=' + encodeURIComponent(path) + (cursor ? '&cursor=' + encodeURIComponent(cursor) : '');
    let r;
    try {
      r = await getJSON(base + '/list' + q);
    } catch (e) {
      showError('Sin conexión', 'No se pudo contactar con el servidor. Inténtalo de nuevo.');
      return;
    }
    if (r.res.status === 401) return boot(); // the password session ended
    if (r.res.status === 410 || r.res.status === 404) return boot();
    if (!r.res.ok) {
      showError('No se pudo cargar', r.data.error || 'Error inesperado');
      return;
    }
    path = r.data.path;
    folders = folders.concat(r.data.folders || []);
    files = files.concat(r.data.files || []);
    next = r.data.next || '';
    render();
  }

  function renderCrumbs() {
    const nav = $('shareCrumbs');
    nav.textContent = '';
    const rootName = root ? root.replace(/\/$/, '').split('/').pop() : 'Inicio';
    const trail = [{ name: info.title || rootName, path: root }];
    let acc = root;
    (path.startsWith(root) ? path.slice(root.length) : path).split('/').filter(Boolean).forEach((seg) => {
      acc += seg + '/';
      trail.push({ name: seg, path: acc });
    });
    trail.forEach((c, i) => {
      if (i > 0) nav.appendChild(el('span', { class: 'share-crumb-sep', text: '/' }));
      if (i === trail.length - 1) {
        nav.appendChild(el('span', { class: 'share-crumb current', text: c.name }));
      } else {
        const b = el('button', { class: 'share-crumb', type: 'button', text: c.name });
        b.addEventListener('click', () => openFolder(c.path));
        nav.appendChild(b);
      }
    });
  }

  function render() {
    renderCrumbs();

    const zip = $('shareZipButton');
    const atTop = path === root;
    zip.setAttribute('href', base + '/zip' + (atTop ? '' : '?path=' + encodeURIComponent(path)));
    $('shareZipLabel').textContent = atTop ? 'Descargar todo (.zip)' : 'Descargar carpeta (.zip)';
    zip.classList.toggle('is-hidden', folders.length + files.length === 0);

    const rows = $('shareRows');
    rows.textContent = '';
    if (folders.length + files.length === 0) {
      rows.appendChild(el('tr', {}, (() => {
        const td = el('td', { class: 'empty-state', colspan: '4', text: 'Esta carpeta está vacía.' });
        return td;
      })()));
    }

    folders.forEach((f) => {
      const open = el('button', { class: 'share-name-link', type: 'button' }, icon('folder'), el('strong', { text: f.name }));
      open.addEventListener('click', () => openFolder(f.path));
      rows.appendChild(el('tr', {},
        el('td', {}, open),
        el('td', { class: 'share-col-size', text: '—' }),
        el('td', { class: 'share-col-date', text: '' }),
        el('td', { class: 'share-col-actions' },
          el('a', { class: 'btn btn-xs btn-secondary', href: base + '/zip?path=' + encodeURIComponent(f.path), text: '.zip' }))));
    });

    files.forEach((f) => {
      const playable = f.kind === 'video' || f.kind === 'audio' || f.kind === 'image';
      const name = playable
        ? el('button', { class: 'share-name-link', type: 'button' }, icon(f.kind), el('strong', { text: f.name }))
        : el('span', { class: 'file-key-cell' }, icon('file'), el('strong', { text: f.name }));
      const actions = el('td', { class: 'share-col-actions' });
      if (playable) {
        const label = f.kind === 'image' ? 'Ver' : 'Reproducir';
        const play = el('button', { class: 'btn btn-xs btn-outline', type: 'button', text: label });
        play.addEventListener('click', () => openPlayer(f));
        name.addEventListener('click', () => openPlayer(f));
        actions.appendChild(play);
      }
      actions.appendChild(el('a', { class: 'btn btn-xs btn-secondary', href: fileURL(f.key, true), download: f.name, text: 'Descargar' }));
      rows.appendChild(el('tr', {},
        el('td', {}, name),
        el('td', { class: 'share-col-size', text: formatBytes(f.size) }),
        el('td', { class: 'share-col-date', text: new Date(f.updated_at).toLocaleDateString() }),
        actions));
    });

    $('shareMoreWrap').classList.toggle('is-hidden', !next);
  }

  $('shareMore').addEventListener('click', () => loadPage(next));

  // --- Player --------------------------------------------------------------
  const player = $('sharePlayer');
  let playing = null;

  function openPlayer(f) {
    closePlayer();
    $('sharePlayerTitle').textContent = f.name;
    $('sharePlayerDownload').setAttribute('href', fileURL(f.key, true));
    $('sharePlayerDownload').setAttribute('download', f.name);
    $('sharePlayerError').classList.add('is-hidden');

    const src = fileURL(f.key, false);
    let node;
    if (f.kind === 'image') {
      node = el('img', { class: 'share-media', alt: f.name, src });
    } else {
      node = el(f.kind === 'audio' ? 'audio' : 'video', { class: 'share-media', controls: '', autoplay: '', playsinline: '', preload: 'metadata', src });
    }
    node.addEventListener('error', () => $('sharePlayerError').classList.remove('is-hidden'));
    $('sharePlayerStage').appendChild(node);
    playing = node;
    player.classList.remove('hidden');
    $('sharePlayerDone').focus();
  }

  function closePlayer() {
    if (playing) {
      // Dropping the source stops the download as well as the playback.
      if (playing.pause) playing.pause();
      playing.removeAttribute('src');
      if (playing.load) playing.load();
      playing.remove();
      playing = null;
    }
    $('sharePlayerStage').textContent = '';
    player.classList.add('hidden');
  }

  $('sharePlayerClose').addEventListener('click', closePlayer);
  $('sharePlayerDone').addEventListener('click', closePlayer);
  player.addEventListener('click', (e) => { if (e.target === player) closePlayer(); });
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && playing) closePlayer(); });

  boot();
})();
