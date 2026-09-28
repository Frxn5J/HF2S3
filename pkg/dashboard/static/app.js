// HF2S3 Dashboard Controller
document.addEventListener('DOMContentLoaded', () => {
  // Escape untrusted text (object keys, account names, server messages...) before it
  // is placed inside an HTML template. Everything dynamic in innerHTML goes through this.
  function escapeHtml(value) {
    return String(value === null || value === undefined ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  // Global fetch wrapper: the session lives in an HttpOnly cookie that the browser sends by
  // itself (no token is ever readable from JavaScript). It only handles 401 -> login overlay.
  const originalFetch = window.fetch;
  window.fetch = async function(url, options = {}) {
    options = Object.assign({ credentials: 'same-origin' }, options || {});
    const res = await originalFetch(url, options);
    if (res.status === 401 && typeof url === 'string' && url.startsWith('/api/') && !url.includes('/api/auth/')) {
      showLoginOverlay();
    }
    return res;
  };

  // State
  let currentTab = 'overview';
  let activeBucket = null;
  let allFiles = [];
  let accountsCache = [];
  let settingsCache = {};

  // DOM Elements
  const navItems = document.querySelectorAll('.nav-item');
  const tabPanes = document.querySelectorAll('.tab-pane');
  const pageTitle = document.getElementById('pageTitle');
  const pageSubtitle = document.getElementById('pageSubtitle');
  const btnRefresh = document.getElementById('btnRefresh');
  const toastContainer = document.getElementById('toastContainer');
  const displayUsername = document.getElementById('displayUsername');
  const btnLogout = document.getElementById('btnLogout');
  const btnDownloadBackup = document.getElementById('btnDownloadBackup');

  // Login Modal Elements
  const loginOverlay = document.getElementById('loginOverlay');
  const formLogin = document.getElementById('formLogin');
  const loginUser = document.getElementById('loginUser');
  const loginPass = document.getElementById('loginPass');
  const loginError = document.getElementById('loginError');

  // Overview Elements
  const metricTotalCapacity = document.getElementById('metricTotalCapacity');
  const metricStoragePercent = document.getElementById('metricStoragePercent');
  const poolProgressBar = document.getElementById('poolProgressBar');
  const metricUsedStorage = document.getElementById('metricUsedStorage');
  const metricFreeStorage = document.getElementById('metricFreeStorage');
  const metricActiveAccounts = document.getElementById('metricActiveAccounts');
  const metricBucketsCount = document.getElementById('metricBucketsCount');
  const metricObjectsCount = document.getElementById('metricObjectsCount');
  const overviewAccountsGrid = document.getElementById('overviewAccountsGrid');
  const navAccountsCount = document.getElementById('navAccountsCount');

  // Accounts Tab
  const accountsTableBody = document.getElementById('accountsTableBody');
  const btnOpenAddAccountModal = document.getElementById('btnOpenAddAccountModal');
  const btnGoAddAccount = document.getElementById('btnGoAddAccount');
  const modalAddAccount = document.getElementById('modalAddAccount');
  const formAddAccount = document.getElementById('formAddAccount');
  const btnSubmitAccount = document.getElementById('btnSubmitAccount');
  const modalEditAccount = document.getElementById('modalEditAccount');
  const formEditAccount = document.getElementById('formEditAccount');
  const btnSubmitEditAccount = document.getElementById('btnSubmitEditAccount');

  // Cache Buckets Elements (Tier 1 S3 Cache)
  const cacheBucketsTableBody = document.getElementById('cacheBucketsTableBody');
  const btnOpenAddCacheBucketModal = document.getElementById('btnOpenAddCacheBucketModal');
  const modalAddCacheBucket = document.getElementById('modalAddCacheBucket');
  const formAddCacheBucket = document.getElementById('formAddCacheBucket');
  const btnSubmitCacheBucket = document.getElementById('btnSubmitCacheBucket');

  // Overview Cache Elements
  const metricCacheTotalCapacity = document.getElementById('metricCacheTotalCapacity');
  const metricCacheStoragePercent = document.getElementById('metricCacheStoragePercent');
  const cacheProgressBar = document.getElementById('cacheProgressBar');
  const metricCacheUsedStorage = document.getElementById('metricCacheUsedStorage');
  const metricCacheFreeStorage = document.getElementById('metricCacheFreeStorage');
  const metricCachedObjectsCount = document.getElementById('metricCachedObjectsCount');
  const metricCacheBucketsCount = document.getElementById('metricCacheBucketsCount');
  const metricCacheBucketsActive = document.getElementById('metricCacheBucketsActive');

  // Storage Tab
  const bucketsListContainer = document.getElementById('bucketsListContainer');
  const btnCreateBucketModal = document.getElementById('btnCreateBucketModal');
  const modalCreateBucket = document.getElementById('modalCreateBucket');
  const formCreateBucket = document.getElementById('formCreateBucket');
  const activeBucketName = document.getElementById('activeBucketName');
  const activeBucketStats = document.getElementById('activeBucketStats');
  const filesTableBody = document.getElementById('filesTableBody');
  const fileSearchInput = document.getElementById('fileSearchInput');
  const uploadDropzone = document.getElementById('uploadDropzone');
  const fileUploadInput = document.getElementById('fileUploadInput');
  const btnUploadToFileBrowser = document.getElementById('btnUploadToFileBrowser');
  const btnQuickUpload = document.getElementById('btnQuickUpload');
  const uploadProgressCard = document.getElementById('uploadProgressCard');
  const uploadingFileName = document.getElementById('uploadingFileName');
  const uploadProgressBar = document.getElementById('uploadProgressBar');

  // Chunks Modal
  const modalInspectChunks = document.getElementById('modalInspectChunks');
  const chunkInspectSummary = document.getElementById('chunkInspectSummary');
  const chunksTableBody = document.getElementById('chunksTableBody');

  // Connect Tab
  const cfgEndpoint = document.getElementById('cfgEndpoint');
  const cfgAccessKey = document.getElementById('cfgAccessKey');
  const cfgSecretKey = document.getElementById('cfgSecretKey');
  const cfgRegion = document.getElementById('cfgRegion');
  const snippetTabs = document.querySelectorAll('.snippet-tab');
  const snippetCodeDisplay = document.getElementById('snippetCodeDisplay');
  const btnCopySnippet = document.getElementById('btnCopySnippet');

  // Settings Tab - S3 Credentials
  const settingsForm = document.getElementById('settingsForm');
  const setAccessKey = document.getElementById('setAccessKey');
  const setSecretKey = document.getElementById('setSecretKey');
  const setRegion = document.getElementById('setRegion');

  // Settings Tab - Admin Credentials Elements
  const formAdminAuth = document.getElementById('formAdminAuth');
  const setAdminUser = document.getElementById('setAdminUser');
  const setAdminPass = document.getElementById('setAdminPass');
  const setAdminPassConfirm = document.getElementById('setAdminPassConfirm');
  const settingsCurrentAdminBadge = document.getElementById('settingsCurrentAdminBadge');

  // Settings Tab - Crypto & Chunk Settings Elements
  const formCrypto = document.getElementById('formCrypto');
  const cryptoKeyInfo = document.getElementById('cryptoKeyInfo');
  const setChunkSize = document.getElementById('setChunkSize');
  const setAdminCurrentPass = document.getElementById('setAdminCurrentPass');
  const btnRotateS3Secret = document.getElementById('btnRotateS3Secret');
  const rotatedSecretBox = document.getElementById('rotatedSecretBox');
  const rotatedSecretOutput = document.getElementById('rotatedSecretOutput');

  // Settings Tab - HF Storage Cache Settings Elements
  const hfStorageForm = document.getElementById('hfStorageForm');
  const setHFStorageEndpoint = document.getElementById('setHFStorageEndpoint');
  const setHFStorageRegion = document.getElementById('setHFStorageRegion');
  const setHFStorageAccessKey = document.getElementById('setHFStorageAccessKey');
  const setHFStorageSecretKey = document.getElementById('setHFStorageSecretKey');
  const setHFStorageBucket = document.getElementById('setHFStorageBucket');
  const hfStorageStatusBadge = document.getElementById('hfStorageStatusBadge');


  // Tab Titles
  const tabMetadata = {
    overview: { title: 'Panel General', sub: 'Monitoreo del pool unificado de almacenamiento y nodos Hugging Face' },
    accounts: { title: 'Cuentas Hugging Face', sub: 'Gestiona tokens y repositorios privados de cada cuenta asociada' },
    storage: { title: 'Buckets y Explorador de Archivos', sub: 'Navega, sube y descarga objetos cifrados en tus buckets S3' },
    connect: { title: 'Conexión S3 / R2 Compatible', sub: 'Parámetros y configuraciones para montar y usar en cualquier cliente' },
    settings: { title: 'Configuraciones del Servidor', sub: 'Ajustes de credenciales de acceso y motor criptográfico' }
  };

  // Helper formatters
  function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  function showToast(msg, type = 'success') {
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = msg;
    toastContainer.appendChild(toast);
    setTimeout(() => {
      toast.remove();
    }, 4000);
  }

  // Tab Switching
  function switchTab(tabId) {
    currentTab = tabId;
    navItems.forEach(item => {
      item.classList.toggle('active', item.dataset.tab === tabId);
    });
    tabPanes.forEach(pane => {
      pane.classList.toggle('active', pane.id === `pane-${tabId}`);
    });
    if (tabMetadata[tabId]) {
      pageTitle.textContent = tabMetadata[tabId].title;
      pageSubtitle.textContent = tabMetadata[tabId].sub;
    }
    loadDataForCurrentTab();
  }

  navItems.forEach(item => {
    item.addEventListener('click', () => switchTab(item.dataset.tab));
  });

  if (btnGoAddAccount) {
    btnGoAddAccount.addEventListener('click', () => switchTab('accounts'));
  }

  // Modal handlers
  document.querySelectorAll('[data-close]').forEach(btn => {
    btn.addEventListener('click', () => {
      const modalId = btn.dataset.close;
      document.getElementById(modalId)?.classList.add('hidden');
    });
  });

  // Keyboard accessibility: Close modals with Escape (R-32)
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      [modalAddAccount, modalEditAccount, modalAddCacheBucket, modalCreateBucket, modalInspectChunks].forEach(m => {
        if (m && !m.classList.contains('hidden')) {
          m.classList.add('hidden');
        }
      });
    }
  });

  // Close modals on backdrop click
  [modalAddAccount, modalEditAccount, modalAddCacheBucket, modalCreateBucket, modalInspectChunks].forEach(modal => {
    if (modal) {
      modal.addEventListener('click', (e) => {
        if (e.target === modal) {
          modal.classList.add('hidden');
        }
      });
    }
  });

  if (btnOpenAddAccountModal) {
    btnOpenAddAccountModal.addEventListener('click', () => {
      modalAddAccount.classList.remove('hidden');
    });
  }

  if (btnOpenAddCacheBucketModal) {
    btnOpenAddCacheBucketModal.addEventListener('click', () => {
      if (modalAddCacheBucket) modalAddCacheBucket.classList.remove('hidden');
    });
  }

  if (btnCreateBucketModal) {
    btnCreateBucketModal.addEventListener('click', () => {
      modalCreateBucket.classList.remove('hidden');
    });
  }

  // Copy to clipboard buttons
  document.querySelectorAll('.btn-copy').forEach(btn => {
    btn.addEventListener('click', () => {
      const targetId = btn.dataset.target;
      const input = document.getElementById(targetId);
      if (input) {
        navigator.clipboard.writeText(input.value);
        showToast('Copiado al portapapeles');
      }
    });
  });

  // --- API LOADERS ---

  async function loadStats() {
    try {
      const res = await fetch('/api/stats');
      if (!res.ok) return;
      const data = await res.json();

      const capGB = (data.total_capacity_bytes / (1024 * 1024 * 1024)).toFixed(1);
      metricTotalCapacity.textContent = `${capGB} GB`;
      metricUsedStorage.textContent = formatBytes(data.total_used_bytes);
      metricFreeStorage.textContent = formatBytes(data.total_free_bytes);

      if (data.total_capacity_bytes > 0) {
        const capGB = (data.total_capacity_bytes / (1024 * 1024 * 1024)).toFixed(1);
        metricTotalCapacity.textContent = `${capGB} GB`;
        metricFreeStorage.textContent = formatBytes(data.total_free_bytes);
        let pct = ((data.total_used_bytes / data.total_capacity_bytes) * 100).toFixed(1);
        metricStoragePercent.textContent = `${pct}% usado`;
        poolProgressBar.style.width = `${Math.max(pct, 2)}%`;
      } else {
        metricTotalCapacity.textContent = 'Dinámica';
        metricFreeStorage.textContent = 'Elástica';
        metricStoragePercent.textContent = 'Capacidad elástica';
        poolProgressBar.style.width = '100%';
      }

      metricActiveAccounts.textContent = `${data.active_accounts} / ${data.total_accounts}`;
      metricBucketsCount.textContent = data.total_buckets;
      metricObjectsCount.textContent = data.total_objects;
      navAccountsCount.textContent = (data.total_accounts || 0) + (data.total_cache_buckets || 0);

      // Cache Stats (Tier 1 S3 Cache Pool)
      const cacheCapGB = ((data.total_cache_capacity_bytes || 0) / (1024 * 1024 * 1024)).toFixed(1);
      if (metricCacheTotalCapacity) metricCacheTotalCapacity.textContent = `${cacheCapGB} GB`;
      if (metricCacheUsedStorage) metricCacheUsedStorage.textContent = formatBytes(data.total_cache_used_bytes || 0);
      const cacheFree = (data.total_cache_capacity_bytes || 0) - (data.total_cache_used_bytes || 0);
      if (metricCacheFreeStorage) metricCacheFreeStorage.textContent = formatBytes(cacheFree > 0 ? cacheFree : 0);

      let cachePct = 0;
      if (data.total_cache_capacity_bytes > 0) {
        cachePct = (((data.total_cache_used_bytes || 0) / data.total_cache_capacity_bytes) * 100).toFixed(1);
      }
      if (metricCacheStoragePercent) metricCacheStoragePercent.textContent = `${cachePct}% usado`;
      if (cacheProgressBar) cacheProgressBar.style.width = `${Math.max(cachePct, data.total_cache_capacity_bytes > 0 ? 2 : 0)}%`;

      if (metricCacheBucketsCount) metricCacheBucketsCount.textContent = data.total_cache_buckets || 0;
      if (metricCacheBucketsActive) metricCacheBucketsActive.textContent = `${data.active_cache_buckets || 0} / ${data.total_cache_buckets || 0}`;
      if (metricCachedObjectsCount) metricCachedObjectsCount.textContent = data.cached_objects || 0;
    } catch (e) {
      console.error('Failed to load stats', e);
    }
  }

  async function loadAccounts() {
    try {
      const res = await fetch('/api/accounts');
      if (!res.ok) return;
      accountsCache = await res.json();

      // Render Overview Grid
      overviewAccountsGrid.innerHTML = '';
      if (accountsCache.length === 0) {
        overviewAccountsGrid.innerHTML = `
          <div class="account-card" style="grid-column: 1 / -1; text-align: center; padding: 30px;">
            <p style="color: var(--text-muted); margin-bottom: 12px;">No tienes cuentas de Hugging Face conectadas aún.</p>
            <button class="btn btn-primary btn-sm" id="btnFirstAccount">+ Conectar Primera Cuenta (Dataset Público)</button>
          </div>
        `;
        document.getElementById('btnFirstAccount')?.addEventListener('click', () => {
          modalAddAccount.classList.remove('hidden');
        });
      } else {
        accountsCache.forEach(acc => {
          const hasQuota = acc.quota_bytes > 0;
          const usedPct = hasQuota ? ((acc.used_bytes / acc.quota_bytes) * 100).toFixed(1) : 0;
          const rl = acc.rate_limit || {};
          const apiRem = rl.api_remaining !== undefined ? rl.api_remaining : 1000;
          const apiLim = rl.api_limit || 1000;
          const resetInS = rl.api_reset_in_s || 300;
          const mins = Math.floor(resetInS / 60);
          const secs = resetInS % 60;
          const resetStr = mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
          const resRem = rl.resolvers_remaining !== undefined ? rl.resolvers_remaining : 5000;
          const resLim = rl.resolvers_limit || 5000;

          let cardStatusColor = 'var(--accent-emerald)';
          if (rl.is_throttled) {
            cardStatusColor = 'var(--accent-rose)';
          } else if (apiRem <= 50) {
            cardStatusColor = 'var(--accent-amber)';
          }

          const card = document.createElement('div');
          card.className = 'account-card';
          card.innerHTML = `
            <div class="account-card-header">
              <div>
                <div class="account-user">${escapeHtml(acc.name)}</div>
                <div class="account-repo" style="font-size:0.8rem;color:var(--text-muted);">
                  <code>${escapeHtml(acc.repo_name)}</code>
                  <span style="font-size:0.65rem;color:var(--accent-cyan);margin-left:4px;font-weight:600;">(PÚBLICO)</span>
                </div>
              </div>
              <span class="${acc.is_active ? 'badge-active' : 'badge-inactive'}">
                ${acc.is_active ? 'Activo' : 'Pausado'}
              </span>
            </div>
            ${hasQuota ? `
            <div class="progress-bar-wrap">
              <div class="progress-bar" style="width: ${Math.max(usedPct, 2)}%"></div>
            </div>
            <div class="progress-stats">
              <span>${formatBytes(acc.used_bytes)} / ${formatBytes(acc.quota_bytes)}</span>
              <span>${usedPct}%</span>
            </div>` : `
            <div class="progress-stats" style="margin-top: 10px;">
              <span>Almacenado: <strong>${formatBytes(acc.used_bytes)}</strong></span>
              <span style="color: var(--accent-cyan); font-weight: 600;">Cuota Dinámica</span>
            </div>`}
            <div style="margin-top: 10px; padding-top: 8px; border-top: 1px solid var(--border-subtle); font-size: 0.75rem;">
              <div style="display: flex; justify-content: space-between; margin-bottom: 2px;">
                <span style="color: var(--text-muted);">Límite API (5 min):</span>
                <strong style="color: ${cardStatusColor};">${apiRem} / ${apiLim} reqs</strong>
              </div>
              <div style="font-size: 0.68rem; color: var(--text-dim); display: flex; justify-content: space-between;">
                <span>Resolvers: ${resRem}/${resLim}</span>
                <span>Reinicio: ${resetStr}</span>
              </div>
            </div>
          `;
          overviewAccountsGrid.appendChild(card);
        });
      }

      // Render Accounts Table
      accountsTableBody.innerHTML = '';
      if (accountsCache.length === 0) {
        accountsTableBody.innerHTML = '<tr><td colspan="7" class="empty-state">No hay cuentas conectadas</td></tr>';
      } else {
        accountsCache.forEach(acc => {
          const tr = document.createElement('tr');
          const quotaDisplay = acc.quota_bytes > 0 
            ? formatBytes(acc.quota_bytes)
            : `<span class="pill-badge" style="color:var(--accent-cyan);background:rgba(6,182,212,0.12);border:1px solid rgba(6,182,212,0.3);padding:2px 8px;border-radius:12px;font-size:0.75rem;font-weight:600;">Dinámica</span>`;
          const quotaGBVal = acc.quota_bytes > 0 ? (acc.quota_bytes / (1024 * 1024 * 1024)).toFixed(0) : 0;

          const rl = acc.rate_limit || {};
          const apiRem = rl.api_remaining !== undefined ? rl.api_remaining : 1000;
          const apiLim = rl.api_limit || 1000;
          const apiPct = Math.max(0, Math.min(100, Math.round((apiRem / apiLim) * 100)));
          const resetInS = rl.api_reset_in_s || 300;
          const mins = Math.floor(resetInS / 60);
          const secs = resetInS % 60;
          const resetStr = mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
          const resRem = rl.resolvers_remaining !== undefined ? rl.resolvers_remaining : 5000;
          const resLim = rl.resolvers_limit || 5000;
          const pagesRem = rl.pages_remaining !== undefined ? rl.pages_remaining : 200;
          const pagesLim = rl.pages_limit || 200;

          let barColor = 'var(--accent-emerald)';
          let statusColor = 'var(--accent-emerald)';
          let statusText = `${apiRem} / ${apiLim} disp.`;
          if (rl.is_throttled) {
            barColor = 'var(--accent-rose)';
            statusColor = 'var(--accent-rose)';
            statusText = `Throttled (${rl.cooldown_remaining_s || 15}s)`;
          } else if (apiRem <= 50) {
            barColor = 'var(--accent-amber)';
            statusColor = 'var(--accent-amber)';
          }

          tr.innerHTML = `
            <td>
              <strong>${escapeHtml(acc.name)}</strong><br/>
              <span style="font-size:0.75rem; color:var(--text-dim)">@${escapeHtml(acc.username)}</span>
            </td>
            <td>
              <code>${escapeHtml(acc.repo_name)}</code>
              <span style="font-size:0.68rem;padding:2px 6px;border-radius:4px;background:rgba(6,182,212,0.12);color:var(--accent-cyan);border:1px solid rgba(6,182,212,0.25);font-weight:600;margin-left:4px;">PÚBLICO</span>
            </td>
            <td title="Límites Hugging Face (5 min): API: ${apiRem}/${apiLim} | Resolvers: ${resRem}/${resLim} | Pages: ${pagesRem}/${pagesLim}">
              <div style="min-width: 140px;">
                <div style="display: flex; justify-content: space-between; font-size: 0.75rem; margin-bottom: 3px;">
                  <span style="font-weight: 600; color: ${statusColor};">${statusText}</span>
                  <span style="color: var(--text-dim); font-size: 0.7rem;">${apiPct}%</span>
                </div>
                <div class="progress-bar-wrap" style="height: 5px; margin-bottom: 4px;">
                  <div class="progress-bar" style="width: ${apiPct}%; background: ${barColor};"></div>
                </div>
                <div style="font-size: 0.68rem; color: var(--text-dim); display: flex; justify-content: space-between;">
                  <span>Ventana 5m</span>
                  <span>Reinicio: ${resetStr}</span>
                </div>
              </div>
            </td>
            <td>${formatBytes(acc.used_bytes)}</td>
            <td>${quotaDisplay}</td>
            <td>
              <span class="${acc.is_active ? 'badge-active' : 'badge-inactive'}">
                ${acc.is_active ? 'En Servicio' : 'Desactivado'}
              </span>
            </td>
            <td>
              <button class="btn btn-xs btn-secondary btn-sync-acc" data-id="${escapeHtml(acc.id)}" title="Sincronizar uso">Sync</button>
              <button class="btn btn-xs btn-outline btn-edit-acc" data-id="${escapeHtml(acc.id)}" title="Editar detalles de la cuenta">Editar</button>
              <button class="btn btn-xs btn-outline btn-quota-acc" data-id="${escapeHtml(acc.id)}" data-quota="${quotaGBVal}" title="Ajustar cuota en GB (0 para Dinámica)">Cuota</button>
              <button class="btn btn-xs btn-outline btn-toggle-acc" data-id="${escapeHtml(acc.id)}">
                ${acc.is_active ? 'Desactivar' : 'Activar'}
              </button>
              <button class="btn btn-xs btn-outline btn-drain-acc" data-id="${escapeHtml(acc.id)}" title="Mueve todos sus datos a las demás cuentas para poder eliminarla">Vaciar</button>
              <button class="btn btn-xs btn-danger btn-del-acc" data-id="${escapeHtml(acc.id)}">Eliminar</button>
            </td>
          `;
          accountsTableBody.appendChild(tr);
        });

        // Attach action handlers
        document.querySelectorAll('.btn-edit-acc').forEach(btn => {
          btn.addEventListener('click', () => {
            const id = parseInt(btn.dataset.id, 10);
            const acc = accountsCache.find(a => a.id === id);
            if (!acc) return;
            document.getElementById('editAccountIdInput').value = acc.id;
            document.getElementById('editAccountNameInput').value = acc.name || '';
            document.getElementById('editAccountUsernameInput').value = `@${acc.username || ''}`;
            document.getElementById('editAccountTokenInput').value = '';
            document.getElementById('editAccountRepoInput').value = acc.repo_name || '';
            const quotaGB = acc.quota_bytes > 0 ? (acc.quota_bytes / (1024 * 1024 * 1024)).toFixed(0) : 0;
            document.getElementById('editAccountQuotaInput').value = quotaGB;
            document.getElementById('editAccountStatusInput').value = acc.is_active ? 'true' : 'false';
            modalEditAccount.classList.remove('hidden');
          });
        });

        document.querySelectorAll('.btn-quota-acc').forEach(btn => {
          btn.addEventListener('click', async () => {
            const id = btn.dataset.id;
            const currentGB = btn.dataset.quota;
            const input = prompt('Ingresa la cuota en GB para este dataset (0 para Dinámica / Elástica):', currentGB);
            if (input === null) return;
            const quotaGB = parseInt(input, 10);
            if (isNaN(quotaGB) || quotaGB < 0) {
              showToast('Cuota inválida', 'error');
              return;
            }
            try {
              await fetch(`/api/accounts/${id}/quota`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ quota_gb: quotaGB })
              });
              loadAccounts();
              loadStats();
              showToast(quotaGB === 0 ? 'Cuota establecida como Dinámica' : `Cuota actualizada a ${quotaGB} GB`);
            } catch (e) {
              showToast('Error al actualizar cuota', 'error');
            }
          });
        });

        document.querySelectorAll('.btn-toggle-acc').forEach(btn => {
          btn.addEventListener('click', async () => {
            const id = btn.dataset.id;
            await fetch(`/api/accounts/${id}/toggle`, { method: 'POST' });
            loadAccounts();
            loadStats();
          });
        });

        document.querySelectorAll('.btn-sync-acc').forEach(btn => {
          btn.addEventListener('click', async () => {
            const id = btn.dataset.id;
            showToast('Sincronizando almacenamiento con Hugging Face...');
            await fetch(`/api/accounts/${id}/sync`, { method: 'POST' });
            loadAccounts();
            loadStats();
            showToast('Sincronización completada');
          });
        });

        document.querySelectorAll('.btn-drain-acc').forEach(btn => {
          btn.addEventListener('click', async () => {
            if (!confirm('¿Vaciar esta cuenta? Se desactivará y todos sus datos se copiarán a las demás cuentas activas. Puede tardar.')) return;
            const id = btn.dataset.id;
            const res = await fetch(`/api/accounts/${id}/drain`, { method: 'POST' });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) {
              showToast(data.error || 'No se pudo iniciar el vaciado', 'error');
              return;
            }
            showToast(`Vaciado iniciado (${data.total || 0} fragmentos)`);
            const poll = setInterval(async () => {
              const st = await (await fetch(`/api/accounts/${id}/drain`)).json().catch(() => null);
              if (!st) { clearInterval(poll); return; }
              if (!st.running) {
                clearInterval(poll);
                showToast(st.error ? st.error : 'Vaciado completado', st.error ? 'error' : 'success');
                loadAccounts();
                loadStats();
              }
            }, 3000);
            loadAccounts();
          });
        });

        document.querySelectorAll('.btn-del-acc').forEach(btn => {
          btn.addEventListener('click', async () => {
            if (!confirm('¿Seguro que deseas eliminar esta cuenta?')) return;
            const id = btn.dataset.id;
            const res = await fetch(`/api/accounts/${id}`, { method: 'DELETE' });
            if (!res.ok) {
              const err = await res.json().catch(() => ({}));
              showToast(err.error || 'No se pudo eliminar la cuenta', 'error');
              return;
            }
            loadAccounts();
            loadStats();
            showToast('Cuenta removida');
          });
        });
      }
    } catch (e) {
      console.error('Failed to load accounts', e);
    }
  }

  // Add Account form submission
  formAddAccount.addEventListener('submit', async (e) => {
    e.preventDefault();
    const token = document.getElementById('hfTokenInput').value.trim();
    const name = document.getElementById('hfAccountNameInput').value.trim();
    const repo = document.getElementById('hfRepoNameInput').value.trim();
    const quota = parseInt(document.getElementById('hfQuotaInput').value, 10) || 0;

    btnSubmitAccount.disabled = true;
    btnSubmitAccount.innerHTML = '<span class="btn-text">Verificando en HF...</span>';

    try {
      const res = await fetch('/api/accounts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token, name, repo_name: repo, quota_gb: quota })
      });
      const data = await res.json();
      if (!res.ok) {
        showToast(data.error || 'Error al conectar cuenta', 'error');
      } else {
        showToast(`¡Cuenta @${data.username} conectada con éxito!`);
        modalAddAccount.classList.add('hidden');
        formAddAccount.reset();
        document.getElementById('hfRepoNameInput').value = 'hf2s3-vault';
        document.getElementById('hfQuotaInput').value = '0';
        loadAccounts();
        loadStats();
      }
    } catch (err) {
      showToast('Error de conexión con el servidor', 'error');
    } finally {
      btnSubmitAccount.disabled = false;
      btnSubmitAccount.innerHTML = '<span class="btn-text">Verificar y Conectar</span>';
    }
  });

  // Edit Account form submission
  if (formEditAccount) {
    formEditAccount.addEventListener('submit', async (e) => {
      e.preventDefault();
      const id = document.getElementById('editAccountIdInput').value;
      const name = document.getElementById('editAccountNameInput').value.trim();
      const token = document.getElementById('editAccountTokenInput').value.trim();
      const repo = document.getElementById('editAccountRepoInput').value.trim();
      const quota = parseInt(document.getElementById('editAccountQuotaInput').value, 10) || 0;
      const isActive = document.getElementById('editAccountStatusInput').value === 'true';

      btnSubmitEditAccount.disabled = true;
      btnSubmitEditAccount.innerHTML = '<span class="btn-text">Guardando...</span>';

      try {
        const payload = {
          name: name,
          repo_name: repo,
          quota_gb: quota,
          is_active: isActive
        };
        if (token) {
          payload.token = token;
        }

        const res = await fetch(`/api/accounts/${id}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        const data = await res.json();

        if (!res.ok) {
          showToast(data.error || 'Error al actualizar cuenta', 'error');
        } else {
          showToast('Detalles de cuenta actualizados exitosamente');
          modalEditAccount.classList.add('hidden');
          loadAccounts();
          loadStats();
        }
      } catch (err) {
        showToast('Error de conexión al actualizar la cuenta', 'error');
      } finally {
        btnSubmitEditAccount.disabled = false;
        btnSubmitEditAccount.innerHTML = '<span class="btn-text">Guardar Cambios</span>';
      }
    });
  }

  // --- CACHE BUCKETS (TIER 1 S3 CACHE) ---

  async function loadCacheBuckets() {
    if (!cacheBucketsTableBody) return;
    try {
      const res = await fetch('/api/cache-buckets');
      if (!res.ok) return;
      const buckets = await res.json();

      cacheBucketsTableBody.innerHTML = '';
      if (!buckets || buckets.length === 0) {
        cacheBucketsTableBody.innerHTML = `
          <tr>
            <td colspan="7" class="empty-state">
              No tienes ningún bucket S3 de caché configurado. Haz clic en <strong>+ Conectar Bucket S3 de Caché</strong> para habilitar descargas directas a velocidad máxima sin ancho de banda VPS.
            </td>
          </tr>
        `;
        return;
      }

      buckets.forEach(cb => {
        const usedPct = cb.quota_bytes > 0 ? ((cb.used_bytes / cb.quota_bytes) * 100).toFixed(1) : 0;
        const quotaGB = (cb.quota_bytes / (1024 * 1024 * 1024)).toFixed(0);
        const row = document.createElement('tr');
        row.innerHTML = `
          <td><strong>${escapeHtml(cb.name)}</strong></td>
          <td><code>${escapeHtml(cb.bucket_name)}</code></td>
          <td><span style="font-size:0.85rem;color:var(--text-muted);">${escapeHtml(cb.endpoint)} (${escapeHtml(cb.region)})</span></td>
          <td>
            <div class="progress-bar-wrap" style="width: 130px; margin-bottom: 4px;">
              <div class="progress-bar" style="width: ${Math.max(usedPct, 2)}%; background: linear-gradient(90deg, #6366f1, #a855f7);"></div>
            </div>
            <span style="font-size: 0.75rem; color: var(--text-dim);">${formatBytes(cb.used_bytes)} (${usedPct}%)</span>
          </td>
          <td>${quotaGB} GB</td>
          <td>
            <span class="${cb.is_active ? 'badge-active' : 'badge-inactive'}">
              ${cb.is_active ? 'Activo' : 'Pausado'}
            </span>
          </td>
          <td>
            <button class="btn btn-xs btn-outline btn-toggle-cb" data-id="${escapeHtml(cb.id)}">
              ${cb.is_active ? 'Pausar' : 'Activar'}
            </button>
            <button class="btn btn-xs btn-danger btn-del-cb" data-id="${escapeHtml(cb.id)}" title="Eliminar este bucket de caché">
              Eliminar
            </button>
          </td>
        `;
        cacheBucketsTableBody.appendChild(row);
      });

      // Handlers for Toggle & Delete
      document.querySelectorAll('.btn-toggle-cb').forEach(btn => {
        btn.addEventListener('click', async () => {
          const id = btn.dataset.id;
          await fetch(`/api/cache-buckets/${id}/toggle`, { method: 'POST' });
          loadCacheBuckets();
          loadStats();
        });
      });

      document.querySelectorAll('.btn-del-cb').forEach(btn => {
        btn.addEventListener('click', async () => {
          if (!confirm('¿Seguro que deseas eliminar este bucket de caché S3? Los archivos cacheados aquí no se perderán (se conservan en el dataset masivo).')) return;
          const id = btn.dataset.id;
          await fetch(`/api/cache-buckets/${id}`, { method: 'DELETE' });
          loadCacheBuckets();
          loadStats();
          showToast('Bucket de caché eliminado');
        });
      });
    } catch (e) {
      console.error('Failed to load cache buckets', e);
    }
  }

  // Add Cache Bucket form submission
  if (formAddCacheBucket) {
    formAddCacheBucket.addEventListener('submit', async (e) => {
      e.preventDefault();
      const name = document.getElementById('cacheBucketNameInput').value.trim();
      const endpoint = document.getElementById('cacheBucketEndpointInput').value.trim();
      const region = document.getElementById('cacheBucketRegionInput').value.trim();
      const accessKey = document.getElementById('cacheBucketAccessKeyInput').value.trim();
      const secretKey = document.getElementById('cacheBucketSecretKeyInput').value.trim();
      const bucketName = document.getElementById('cacheBucketTargetInput').value.trim();
      const quotaGB = parseInt(document.getElementById('cacheBucketQuotaInput').value, 10) || 100;

      btnSubmitCacheBucket.disabled = true;
      btnSubmitCacheBucket.innerHTML = '<span class="btn-text">Verificando en HF Storage...</span>';

      try {
        const res = await fetch('/api/cache-buckets', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name,
            endpoint,
            region,
            access_key: accessKey,
            secret_key: secretKey,
            bucket_name: bucketName,
            quota_bytes: quotaGB * 1024 * 1024 * 1024
          })
        });

        if (!res.ok) {
          const err = await res.json().catch(() => ({}));
          showToast(err.error || 'Error conectando bucket S3 de caché', 'error');
          return;
        }

        showToast(`Bucket de caché ${name} conectado con éxito (${quotaGB} GB añadidos al pool)`);
        formAddCacheBucket.reset();
        document.getElementById('cacheBucketEndpointInput').value = 'https://s3.hf.co';
        document.getElementById('cacheBucketRegionInput').value = 'us-east-1';
        document.getElementById('cacheBucketQuotaInput').value = '100';
        modalAddCacheBucket.classList.add('hidden');
        loadCacheBuckets();
        loadStats();
      } catch (err) {
        showToast('Error de conexión con el gateway', 'error');
      } finally {
        btnSubmitCacheBucket.disabled = false;
        btnSubmitCacheBucket.innerHTML = '<span class="btn-text">Verificar y Conectar Bucket</span>';
      }
    });
  }

  // --- BUCKETS & FILES ---

  async function loadBuckets() {
    try {
      const res = await fetch('/api/buckets');
      if (!res.ok) return;
      const buckets = await res.json();

      bucketsListContainer.innerHTML = '';
      if (buckets.length === 0) {
        bucketsListContainer.innerHTML = '<p style="color:var(--text-dim);font-size:0.8rem;padding:8px;">No hay buckets creados</p>';
        activeBucket = null;
        renderFiles([]);
        return;
      }

      buckets.forEach(b => {
        const btn = document.createElement('div');
        btn.className = `bucket-btn ${activeBucket === b.name ? 'active' : ''}`;
        btn.innerHTML = `
          <span class="bucket-name-wrap">
            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"></path></svg>
            <span>${escapeHtml(b.name)}</span>
          </span>
          <div class="bucket-btn-actions">
            <span class="bucket-item-badge">${escapeHtml(b.object_count)}</span>
            <button class="btn btn-xs btn-danger btn-del-bucket" data-name="${escapeHtml(b.name)}" title="Eliminar bucket">&times;</button>
          </div>
        `;
        btn.addEventListener('click', (e) => {
          if (e.target.classList.contains('btn-del-bucket')) return;
          selectBucket(b.name);
        });
        bucketsListContainer.appendChild(btn);
      });

      // Delete bucket handler
      document.querySelectorAll('.btn-del-bucket').forEach(btn => {
        btn.addEventListener('click', async (e) => {
          e.stopPropagation();
          const name = btn.dataset.name;
          if (!confirm(`¿Eliminar el bucket "${name}"? Debe estar vacío.`)) return;
          const res = await fetch(`/api/buckets/${encodeURIComponent(name)}`, { method: 'DELETE' });
          if (!res.ok) {
            const err = await res.json();
            showToast(err.error || 'No se pudo eliminar el bucket', 'error');
          } else {
            showToast(`Bucket ${name} eliminado`);
            if (activeBucket === name) activeBucket = null;
            loadBuckets();
            loadStats();
          }
        });
      });

      if (!activeBucket && buckets.length > 0) {
        selectBucket(buckets[0].name);
      }
    } catch (e) {
      console.error('Failed to load buckets', e);
    }
  }

  function selectBucket(name) {
    activeBucket = name;
    activeBucketName.textContent = name;
    document.querySelectorAll('.bucket-btn').forEach(btn => {
      btn.classList.toggle('active', btn.textContent.includes(name));
    });
    loadObjectsForBucket(name);
  }

  // Create Bucket
  formCreateBucket.addEventListener('submit', async (e) => {
    e.preventDefault();
    const name = document.getElementById('bucketNameInput').value.trim();
    try {
      const res = await fetch('/api/buckets', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name })
      });
      if (!res.ok) {
        const err = await res.json();
        showToast(err.error || 'Error al crear bucket', 'error');
      } else {
        showToast(`Bucket "${name}" creado exitosamente`);
        modalCreateBucket.classList.add('hidden');
        formCreateBucket.reset();
        activeBucket = name;
        loadBuckets();
        loadStats();
      }
    } catch (e) {
      showToast('Error de comunicación con el servidor', 'error');
    }
  });

  async function loadObjectsForBucket(bucket) {
    try {
      filesTableBody.innerHTML = '<tr><td colspan="6" class="empty-state">Cargando archivos...</td></tr>';
      const res = await fetch(`/api/objects?bucket=${encodeURIComponent(bucket)}`);
      if (!res.ok) return;
      allFiles = await res.json() || [];
      activeBucketStats.textContent = `${allFiles.length} archivo(s)`;
      renderFiles(allFiles);
    } catch (e) {
      console.error('Failed to load objects', e);
    }
  }

  function renderFiles(files) {
    filesTableBody.innerHTML = '';
    if (!files || files.length === 0) {
      filesTableBody.innerHTML = '<tr><td colspan="7" class="empty-state">Este bucket no contiene archivos aún. Sube uno arrastrándolo arriba.</td></tr>';
      return;
    }

    files.forEach(f => {
      let tierBadges = '<span class="tier-badge tier-none">Sin Ubicación</span>';
      if (f.has_cache || f.has_cold) {
        tierBadges = '<div class="tier-badges-wrap">';
        if (f.has_cache) {
          tierBadges += '<span class="tier-badge tier-cache" title="En Caché HF Storage S3 (Descarga Directa 302)">⚡ Caché S3</span>';
        }
        if (f.has_cold) {
          tierBadges += '<span class="tier-badge tier-cold" title="Original Cifrado en Dataset Hub">❄️ Dataset Público</span>';
        }
        tierBadges += '</div>';
      }

      let cacheActionBtn = '';
      if (f.has_cache) {
        cacheActionBtn = `<button class="btn btn-xs btn-outline btn-evict-file" data-key="${escapeHtml(f.key)}" title="Desalojar copia de la caché S3 (el original cifrado se preserva en dataset)">Desalojar</button>`;
      } else if (f.has_cold) {
        cacheActionBtn = `<button class="btn btn-xs btn-outline btn-promote-file" data-key="${escapeHtml(f.key)}" title="Promover copia sin cifrar a la caché S3">Promover</button>`;
      }

      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>
          <span class="file-key-cell">
            <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path><polyline points="13 2 13 9 20 9"></polyline></svg>
            <strong>${escapeHtml(f.key)}</strong>
          </span>
        </td>
        <td>${formatBytes(f.size)}</td>
        <td>${tierBadges}</td>
        <td><code style="font-size:0.75rem">${escapeHtml((f.etag || '').replace(/"/g, ''))}</code></td>
        <td><span style="font-size:0.75rem;color:var(--text-dim)">${escapeHtml(f.content_type || 'binary')}</span></td>
        <td><span style="font-size:0.78rem">${new Date(f.updated_at).toLocaleString()}</span></td>
        <td>
          <button class="btn btn-xs btn-outline btn-stream-file" data-key="${escapeHtml(f.key)}" title="Genera un enlace firmado con caducidad para reproducir o descargar">Enlace</button>
          <button class="btn btn-xs btn-secondary btn-dl-file" data-key="${escapeHtml(f.key)}">Descargar</button>
          ${cacheActionBtn}
          <button class="btn btn-xs btn-outline btn-inspect-file" data-key="${escapeHtml(f.key)}" title="Ver distribución de chunks en HF">Chunks</button>
          <button class="btn btn-xs btn-danger btn-del-file" data-key="${escapeHtml(f.key)}">Borrar</button>
        </td>
      `;
      filesTableBody.appendChild(tr);
    });

    // Actions
    document.querySelectorAll('.btn-stream-file').forEach(btn => {
      btn.addEventListener('click', async () => {
        const key = btn.dataset.key;
        try {
          const res = await fetch('/api/objects/presign', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ bucket: activeBucket, key, expires_seconds: 3600 })
          });
          const data = await res.json();
          if (!res.ok) {
            showToast(data.error || 'No se pudo generar el enlace', 'error');
            return;
          }
          try { await navigator.clipboard.writeText(data.url); } catch (e) { /* clipboard may be unavailable */ }
          showToast('Enlace firmado copiado (caduca en 1 hora)');
          window.open(data.url, '_blank', 'noopener,noreferrer');
        } catch (e) {
          showToast('Error de comunicación con el gateway', 'error');
        }
      });
    });

    document.querySelectorAll('.btn-dl-file').forEach(btn => {
      btn.addEventListener('click', () => {
        const key = btn.dataset.key;
        window.location.href = `/api/objects/download?bucket=${encodeURIComponent(activeBucket)}&key=${encodeURIComponent(key)}`;
      });
    });

    document.querySelectorAll('.btn-del-file').forEach(btn => {
      btn.addEventListener('click', async () => {
        const key = btn.dataset.key;
        if (!confirm(`¿Eliminar ${key} de ${activeBucket}? Los chunks se liberarán de Hugging Face.`)) return;
        await fetch(`/api/objects?bucket=${encodeURIComponent(activeBucket)}&key=${encodeURIComponent(key)}`, { method: 'DELETE' });
        showToast(`Archivo ${key} eliminado`);
        loadObjectsForBucket(activeBucket);
        loadStats();
      });
    });

    document.querySelectorAll('.btn-inspect-file').forEach(btn => {
      btn.addEventListener('click', () => {
        inspectChunks(btn.dataset.key);
      });
    });

    document.querySelectorAll('.btn-evict-file').forEach(btn => {
      btn.addEventListener('click', async () => {
        const key = btn.dataset.key;
        if (!confirm(`¿Desalojar "${key}" de la Caché S3? El original cifrado en el dataset se mantendrá intacto.`)) return;
        try {
          const res = await fetch('/api/objects/evict-cache', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ bucket: activeBucket, key })
          });
          const data = await res.json();
          if (!res.ok) {
            showToast(data.error || 'Error al desalojar de caché', 'error');
          } else {
            showToast(`"${key}" desalojado de la caché S3`);
            loadObjectsForBucket(activeBucket);
          }
        } catch (e) {
          showToast('Error de comunicación con el gateway', 'error');
        }
      });
    });

    document.querySelectorAll('.btn-promote-file').forEach(btn => {
      btn.addEventListener('click', async () => {
        const key = btn.dataset.key;
        btn.disabled = true;
        btn.textContent = 'Promoviendo...';
        showToast(`Promoviendo "${key}" a la caché S3...`);
        try {
          const res = await fetch('/api/objects/promote-cache', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ bucket: activeBucket, key })
          });
          const data = await res.json();
          if (!res.ok) {
            showToast(data.error || 'Error al promover a caché', 'error');
            btn.disabled = false;
            btn.textContent = 'Promover';
          } else {
            showToast(`¡"${key}" promovido a la caché S3!`);
            loadObjectsForBucket(activeBucket);
          }
        } catch (e) {
          showToast('Error de comunicación con el gateway', 'error');
          btn.disabled = false;
          btn.textContent = 'Promover';
        }
      });
    });
  }

  // Filter files
  fileSearchInput.addEventListener('input', () => {
    const q = fileSearchInput.value.toLowerCase().trim();
    if (!q) {
      renderFiles(allFiles);
    } else {
      const filtered = allFiles.filter(f => f.key.toLowerCase().includes(q));
      renderFiles(filtered);
    }
  });

  // Inspect Chunks Modal
  async function inspectChunks(key) {
    try {
      const res = await fetch(`/api/objects/detail?bucket=${encodeURIComponent(activeBucket)}&key=${encodeURIComponent(key)}`);
      if (!res.ok) return;
      const data = await res.json();

      chunkInspectSummary.innerHTML = `
        <div style="margin-bottom:14px;background:rgba(255,255,255,0.03);padding:12px;border-radius:var(--radius-md);">
          <div><strong>Objeto:</strong> ${escapeHtml(data.object.key)} (${formatBytes(data.object.size)})</div>
          <div style="font-size:0.8rem;color:var(--text-dim);margin-top:4px;">Dividido en <strong>${data.chunks.length}</strong> chunks cifrados con AES-256-GCM y distribuidos en el pool de Hugging Face.</div>
        </div>
      `;

      chunksTableBody.innerHTML = '';
      data.chunks.forEach(c => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>#${Number(c.chunk_index) + 1}</td>
          <td>${Number(c.offset_bytes)} - ${Number(c.offset_bytes) + Number(c.size_bytes)}</td>
          <td>${formatBytes(c.size_bytes)}</td>
          <td>${formatBytes(c.cipher_size_bytes)}</td>
          <td><span class="badge-active">${escapeHtml(c.account_name)}</span></td>
          <td><code>${escapeHtml(c.remote_path)}</code></td>
        `;
        chunksTableBody.appendChild(tr);
      });

      modalInspectChunks.classList.remove('hidden');
    } catch (e) {
      console.error('Failed to inspect chunks', e);
    }
  }

  // --- UPLOAD HANDLER ---

  function triggerUploadFile(file) {
    if (!activeBucket) {
      showToast('Por favor selecciona o crea un bucket primero', 'error');
      return;
    }
    uploadFile(file, activeBucket);
  }

  async function uploadFile(file, bucket) {
    uploadProgressCard.classList.remove('hidden');
    uploadingFileName.textContent = `Cifrando y distribuyendo "${file.name}" (${formatBytes(file.size)})...`;
    uploadProgressBar.style.width = '30%';

    const formData = new FormData();
    formData.append('bucket', bucket);
    formData.append('key', file.name);
    formData.append('file', file);

    try {
      uploadProgressBar.style.width = '70%';
      const res = await fetch('/api/objects/upload', {
        method: 'POST',
        body: formData
      });
      const data = await res.json();
      if (!res.ok) {
        showToast(data.error || 'Fallo en la subida', 'error');
      } else {
        uploadProgressBar.style.width = '100%';
        showToast(`"${file.name}" subido y cifrado con éxito`);
        loadObjectsForBucket(bucket);
        loadStats();
        loadAccounts();
      }
    } catch (e) {
      showToast('Error de subida', 'error');
    } finally {
      setTimeout(() => {
        uploadProgressCard.classList.add('hidden');
        uploadProgressBar.style.width = '0%';
      }, 1000);
    }
  }

  // Drag & drop
  uploadDropzone.addEventListener('click', () => fileUploadInput.click());
  btnUploadToFileBrowser.addEventListener('click', () => fileUploadInput.click());
  btnQuickUpload.addEventListener('click', () => {
    switchTab('storage');
    fileUploadInput.click();
  });

  fileUploadInput.addEventListener('change', () => {
    if (fileUploadInput.files.length > 0) {
      triggerUploadFile(fileUploadInput.files[0]);
    }
  });

  uploadDropzone.addEventListener('dragover', (e) => {
    e.preventDefault();
    uploadDropzone.classList.add('dragover');
  });
  uploadDropzone.addEventListener('dragleave', () => {
    uploadDropzone.classList.remove('dragover');
  });
  uploadDropzone.addEventListener('drop', (e) => {
    e.preventDefault();
    uploadDropzone.classList.remove('dragover');
    if (e.dataTransfer.files.length > 0) {
      triggerUploadFile(e.dataTransfer.files[0]);
    }
  });

  // --- CONNECT TAB & SETTINGS ---

  async function loadSettings() {
    try {
      const res = await fetch('/api/settings');
      if (!res.ok) return;
      settingsCache = await res.json();

      cfgEndpoint.value = settingsCache.endpoint;
      cfgAccessKey.value = settingsCache.access_key_id;
      cfgSecretKey.value = settingsCache.secret_access_key_set ? '•••••••• (solo visible al rotarlo)' : '';
      cfgRegion.value = settingsCache.s3_region;

      setAccessKey.value = settingsCache.access_key_id;
      setSecretKey.value = '';
      setRegion.value = settingsCache.s3_region;

      // Populate Admin Settings
      if (setAdminUser) setAdminUser.value = settingsCache.admin_username || 'admin';
      if (settingsCurrentAdminBadge) settingsCurrentAdminBadge.textContent = settingsCache.admin_username || 'admin';

      // Populate Crypto & Chunking Settings
      if (cryptoKeyInfo) {
        const parts = [`Clave de cifrado activa: ${settingsCache.encryption_key_id || '?'}`];
        if (settingsCache.chunks_needing_rekey > 0) {
          parts.push(`${settingsCache.chunks_needing_rekey} fragmento(s) aún en formato antiguo: ejecuta "hf2s3 rekey" en el servidor.`);
        } else {
          parts.push('Todos los fragmentos usan el formato y la clave actuales.');
        }
        cryptoKeyInfo.textContent = parts.join(' ');
      }
      if (setChunkSize) setChunkSize.value = String(settingsCache.chunk_size_mb || 32);

      // Populate HF Storage Cache settings
      if (setHFStorageEndpoint) setHFStorageEndpoint.value = settingsCache.hf_storage_endpoint || 'https://s3.hf.co';
      if (setHFStorageRegion) setHFStorageRegion.value = settingsCache.hf_storage_region || 'us-east-1';
      if (setHFStorageAccessKey) setHFStorageAccessKey.value = settingsCache.hf_storage_access_key || '';
      if (setHFStorageBucket) setHFStorageBucket.value = settingsCache.hf_storage_bucket || '';

      if (hfStorageStatusBadge) {
        if (settingsCache.hf_storage_configured) {
          hfStorageStatusBadge.className = 'badge-active';
          hfStorageStatusBadge.textContent = '⚡ En Servicio (Direct SigV4 302)';
        } else {
          hfStorageStatusBadge.className = 'badge-inactive';
          hfStorageStatusBadge.textContent = 'No Configurado';
        }
      }

      renderSnippet('rclone');
      loadRestoreStatus();
    } catch (e) {
      console.error('Failed to load settings', e);
    }
  }

  function renderSnippet(type) {
    if (settingsCache.snippets && settingsCache.snippets[type]) {
      snippetCodeDisplay.textContent = settingsCache.snippets[type];
    }
  }

  snippetTabs.forEach(tab => {
    tab.addEventListener('click', () => {
      snippetTabs.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      renderSnippet(tab.dataset.snippet);
    });
  });

  btnCopySnippet.addEventListener('click', () => {
    navigator.clipboard.writeText(snippetCodeDisplay.textContent);
    showToast('Configuración copiada');
  });

  // Admin Account Settings Form
  if (formAdminAuth) {
    formAdminAuth.addEventListener('submit', async (e) => {
      e.preventDefault();
      const user = setAdminUser.value.trim();
      const pass = setAdminPass.value;
      const passConfirm = setAdminPassConfirm.value;

      if (!user) {
        showToast('El usuario administrador no puede estar vacío', 'error');
        return;
      }

      if (pass !== '' && pass !== passConfirm) {
        showToast('Las contraseñas no coinciden', 'error');
        return;
      }

      const payload = {
        admin_username: user
      };
      if (pass !== '') {
        payload.admin_password = pass;
        payload.current_password = setAdminCurrentPass ? setAdminCurrentPass.value : '';
      }

      try {
        const res = await fetch('/api/settings', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        if (res.ok) {
          showToast('Credenciales de administrador actualizadas con éxito');
          const changedPassword = pass !== '';
          setAdminPass.value = '';
          setAdminPassConfirm.value = '';
          if (setAdminCurrentPass) setAdminCurrentPass.value = '';
          if (displayUsername) displayUsername.textContent = user;
          if (changedPassword) {
            showToast('Contraseña cambiada: vuelve a iniciar sesión');
            showLoginOverlay();
          }
          loadSettings();
        } else {
          const err = await res.json();
          showToast(err.error || 'Error al actualizar administrador', 'error');
        }
      } catch (err) {
        showToast('Error de comunicación con el gateway', 'error');
      }
    });
  }

  // S3 Gateway Settings Form
  settingsForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const payload = {
      access_key_id: setAccessKey.value.trim(),
      s3_region: setRegion.value.trim()
    };
    try {
      const res = await fetch('/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (res.ok) {
        showToast('Credenciales S3 actualizadas exitosamente');
        loadSettings();
      } else {
        const err = await res.json().catch(() => ({}));
        showToast(err.error || 'No se pudieron guardar las credenciales', 'error');
      }
    } catch (e) {
      showToast('Error al guardar credenciales', 'error');
    }
  });

  // Rotate the S3 secret: the new value is shown once and never stored in the page.
  if (btnRotateS3Secret) {
    btnRotateS3Secret.addEventListener('click', async () => {
      if (!confirm('¿Generar un nuevo secreto S3? Los clientes con el secreto anterior dejarán de funcionar.')) return;
      try {
        const res = await fetch('/api/settings/rotate-s3-secret', { method: 'POST' });
        const data = await res.json();
        if (!res.ok) {
          showToast(data.error || 'No se pudo rotar el secreto', 'error');
          return;
        }
        if (rotatedSecretOutput) {
          rotatedSecretOutput.textContent = data.secret_access_key;
          rotatedSecretBox.classList.remove('hidden');
        }
        showToast('Nuevo secreto generado. Guárdalo ahora: no volverá a mostrarse.');
        loadSettings();
      } catch (e) {
        showToast('Error de comunicación con el gateway', 'error');
      }
    });
  }

  // HF Storage Cache Form
  if (hfStorageForm) {
    hfStorageForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      const payload = {
        hf_storage_endpoint: setHFStorageEndpoint.value.trim(),
        hf_storage_region: setHFStorageRegion.value.trim(),
        hf_storage_access_key: setHFStorageAccessKey.value.trim(),
        hf_storage_secret_key: setHFStorageSecretKey.value.trim(),
        hf_storage_bucket: setHFStorageBucket.value.trim()
      };
      try {
        const res = await fetch('/api/settings', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        if (res.ok) {
          showToast('Configuración de Caché HF Storage guardada');
          loadSettings();
        } else {
          const err = await res.json();
          showToast(err.error || 'Error al guardar configuración de caché', 'error');
        }
      } catch (e) {
        showToast('Error de comunicación con el gateway', 'error');
      }
    });
  }

  // Chunking settings (the encryption master key is configured on the server, never here)
  if (formCrypto) {
    formCrypto.addEventListener('submit', async (e) => {
      e.preventDefault();
      const chunk_size_mb = parseInt(setChunkSize.value, 10);
      if (!chunk_size_mb || chunk_size_mb < 1) {
        showToast('El tamaño de fragmento debe ser un número positivo', 'error');
        return;
      }
      try {
        const res = await fetch('/api/settings', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chunk_size_mb })
        });
        if (res.ok) {
          showToast('Parámetros criptográficos y de fragmentación actualizados');
          loadSettings();
        } else {
          const err = await res.json();
          showToast(err.error || 'Error al guardar parámetros de almacenamiento frío', 'error');
        }
      } catch (err) {
        showToast('Error de comunicación con el gateway', 'error');
      }
    });
  }

  // --- DATABASE RESTORE (upload -> validate -> confirm -> controlled restart) ---
  const formRestoreUpload = document.getElementById('formRestoreUpload');
  const restoreFileInput = document.getElementById('restoreFileInput');
  const btnChooseRestoreFile = document.getElementById('btnChooseRestoreFile');
  const restoreFileName = document.getElementById('restoreFileName');
  const btnUploadRestore = document.getElementById('btnUploadRestore');
  const restoreUploadProgress = document.getElementById('restoreUploadProgress');
  const restoreUploadBar = document.getElementById('restoreUploadBar');
  const restorePendingBox = document.getElementById('restorePendingBox');
  const restorePendingFile = document.getElementById('restorePendingFile');
  const restoreSummaryList = document.getElementById('restoreSummaryList');
  const restoreWarningList = document.getElementById('restoreWarningList');
  const restoreConfirmPass = document.getElementById('restoreConfirmPass');
  const btnApplyRestore = document.getElementById('btnApplyRestore');
  const btnCancelRestore = document.getElementById('btnCancelRestore');
  const restoreLastResult = document.getElementById('restoreLastResult');
  const restoreLastResultText = document.getElementById('restoreLastResultText');
  const restoreUnavailable = document.getElementById('restoreUnavailable');
  const restoreRestartOverlay = document.getElementById('restoreRestartOverlay');
  const restoreRestartText = document.getElementById('restoreRestartText');
  let restoreMaxMB = 1024;

  // Everything below is written with textContent: file names and messages are untrusted.
  function addListItem(list, text) {
    const li = document.createElement('li');
    li.textContent = text;
    list.appendChild(li);
  }

  function renderPendingRestore(p) {
    if (!p) {
      restorePendingBox.classList.add('hidden');
      return;
    }
    restorePendingFile.textContent = p.filename + (p.encrypted ? ' (cifrada)' : '');
    restoreSummaryList.textContent = '';
    restoreWarningList.textContent = '';
    addListItem(restoreSummaryList, `Objetos: ${p.objects} en la copia (ahora: ${p.current_objects})`);
    addListItem(restoreSummaryList, `Cuentas de Hugging Face: ${p.accounts} en la copia (ahora: ${p.current_accounts})`);
    addListItem(restoreSummaryList, `Buckets: ${p.buckets} · Fragmentos: ${p.chunks} · Datos: ${formatBytes(p.total_bytes)}`);
    if (p.latest_object_at) {
      addListItem(restoreSummaryList, `Último objeto de la copia: ${new Date(p.latest_object_at).toLocaleString()}`);
    }
    (p.warnings || []).forEach(w => addListItem(restoreWarningList, '⚠ ' + w));
    restorePendingBox.classList.remove('hidden');
  }

  async function loadRestoreStatus() {
    if (!restorePendingBox) return;
    try {
      const res = await fetch('/api/admin/restore');
      if (res.status === 501) {
        restoreUnavailable.classList.remove('hidden');
        formRestoreUpload.classList.add('hidden');
        return;
      }
      if (!res.ok) return;
      const data = await res.json();
      restoreMaxMB = data.max_upload_mb || restoreMaxMB;
      renderPendingRestore(data.pending);
      if (data.last_result) {
        const r = data.last_result;
        restoreLastResultText.textContent = `Última restauración (${new Date(r.at).toLocaleString()}): ${r.message}`;
        restoreLastResult.classList.remove('hidden');
      } else {
        restoreLastResult.classList.add('hidden');
      }
    } catch (e) {
      console.error('Failed to load restore status', e);
    }
  }

  if (btnChooseRestoreFile) {
    btnChooseRestoreFile.addEventListener('click', () => restoreFileInput.click());
    restoreFileInput.addEventListener('change', () => {
      const f = restoreFileInput.files[0];
      if (f) {
        restoreFileName.textContent = `${f.name} (${formatBytes(f.size)})`;
        btnUploadRestore.disabled = false;
      } else {
        restoreFileName.textContent = 'Ningún archivo seleccionado';
        btnUploadRestore.disabled = true;
      }
    });

    formRestoreUpload.addEventListener('submit', (e) => {
      e.preventDefault();
      const file = restoreFileInput.files[0];
      if (!file) return;
      if (file.size > restoreMaxMB * 1024 * 1024) {
        showToast(`El archivo supera el máximo permitido (${restoreMaxMB} MB)`, 'error');
        return;
      }

      const form = new FormData();
      form.append('database', file);
      const xhr = new XMLHttpRequest();
      xhr.open('POST', '/api/admin/restore');
      xhr.upload.onprogress = (ev) => {
        if (ev.lengthComputable) restoreUploadBar.style.width = Math.round((ev.loaded / ev.total) * 100) + '%';
      };
      xhr.onloadstart = () => {
        btnUploadRestore.disabled = true;
        btnUploadRestore.textContent = 'Subiendo y validando…';
        restoreUploadProgress.classList.remove('hidden');
        restoreUploadBar.style.width = '0%';
      };
      xhr.onloadend = () => {
        btnUploadRestore.textContent = 'Subir y validar';
        btnUploadRestore.disabled = !restoreFileInput.files[0];
        restoreUploadProgress.classList.add('hidden');
        let body = {};
        try { body = JSON.parse(xhr.responseText); } catch (e) { /* not JSON */ }
        if (xhr.status === 401) { showLoginOverlay(); return; }
        if (xhr.status === 200) {
          showToast('Copia validada. Revisa el resumen y confirma para restaurar.');
          renderPendingRestore(body);
          restoreFileInput.value = '';
          restoreFileName.textContent = 'Ningún archivo seleccionado';
          btnUploadRestore.disabled = true;
        } else {
          showToast(body.error || 'No se pudo validar la copia de seguridad', 'error');
        }
      };
      xhr.onerror = () => showToast('Error de comunicación durante la subida', 'error');
      xhr.send(form);
    });

    btnCancelRestore.addEventListener('click', async () => {
      await fetch('/api/admin/restore', { method: 'DELETE' });
      restoreConfirmPass.value = '';
      renderPendingRestore(null);
      showToast('Restauración cancelada; no se ha modificado nada');
    });

    btnApplyRestore.addEventListener('click', async () => {
      if (!restoreConfirmPass.value) {
        showToast('Introduce tu contraseña de administrador para confirmar', 'error');
        return;
      }
      if (!confirm('¿Reemplazar la base de datos actual por la copia validada? El servicio se reiniciará.')) return;

      btnApplyRestore.disabled = true;
      try {
        const res = await fetch('/api/admin/restore/apply', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ current_password: restoreConfirmPass.value })
        });
        const data = await res.json().catch(() => ({}));
        restoreConfirmPass.value = '';
        if (!res.ok) {
          showToast(data.error || 'No se pudo iniciar la restauración', 'error');
          btnApplyRestore.disabled = false;
          return;
        }
        waitForRestart();
      } catch (e) {
        showToast('Error de comunicación con el gateway', 'error');
        btnApplyRestore.disabled = false;
      }
    });
  }

  // After confirming, wait for the service to go down and come back, then reload
  // (the session does not survive the restart, so the login screen appears).
  function waitForRestart() {
    restorePendingBox.classList.add('hidden');
    restoreRestartOverlay.classList.remove('hidden');
    const started = Date.now();
    let sawDown = false;
    const timer = setInterval(async () => {
      const elapsed = (Date.now() - started) / 1000;
      try {
        const res = await originalFetch('/api/ready', { cache: 'no-store' });
        if (res.ok && (sawDown || elapsed > 12)) {
          clearInterval(timer);
          restoreRestartText.textContent = 'Servicio de nuevo en línea. Recargando…';
          setTimeout(() => window.location.reload(), 800);
        } else if (!res.ok) {
          sawDown = true;
        }
      } catch (e) {
        sawDown = true;
      }
      if (elapsed > 120) {
        clearInterval(timer);
        restoreRestartText.textContent = 'El servicio tarda en volver. Comprueba los registros del servidor y recarga la página.';
      }
    }, 1000);
  }

  // Global Refresh
  btnRefresh.addEventListener('click', () => {
    loadDataForCurrentTab();
    showToast('Datos actualizados');
  });

  function loadDataForCurrentTab() {
    loadStats();
    if (currentTab === 'overview') {
      loadAccounts();
    } else if (currentTab === 'accounts') {
      loadAccounts();
      loadCacheBuckets();
    } else if (currentTab === 'storage') {
      loadBuckets();
    } else if (currentTab === 'connect' || currentTab === 'settings') {
      loadSettings();
    }
  }

  // Backup Download Handler
  if (btnDownloadBackup) {
    btnDownloadBackup.addEventListener('click', () => {
      showToast('Generando respaldo íntegro de base de datos...');
      window.location.href = '/api/admin/backup';
    });
  }

  // Auth UI Handlers
  function showLoginOverlay() {
    if (loginOverlay) {
      loginOverlay.classList.remove('hidden');
      if (loginError) loginError.classList.add('hidden');
    }
  }

  function hideLoginOverlay() {
    if (loginOverlay) {
      loginOverlay.classList.add('hidden');
    }
  }

  if (formLogin) {
    formLogin.addEventListener('submit', async (e) => {
      e.preventDefault();
      if (loginError) loginError.classList.add('hidden');

      const username = loginUser.value.trim();
      const password = loginPass.value;

      try {
        const res = await originalFetch('/api/auth/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ username, password })
        });

        if (!res.ok) {
          const errData = await res.json().catch(() => ({}));
          loginError.textContent = errData.error || 'Credenciales inválidas';
          loginError.classList.remove('hidden');
          return;
        }

        const data = await res.json();
        loginPass.value = '';
        if (displayUsername) displayUsername.textContent = data.username || 'admin';
        hideLoginOverlay();
        showToast('Sesión de administrador iniciada', 'success');
        loadAllData();
      } catch (err) {
        if (loginError) {
          loginError.textContent = 'Error de conexión con el gateway';
          loginError.classList.remove('hidden');
        }
      }
    });
  }

  if (btnLogout) {
    btnLogout.addEventListener('click', async () => {
      try {
        await fetch('/api/auth/logout', { method: 'POST' });
      } catch (e) {}
      showLoginOverlay();
      showToast('Sesión cerrada');
    });
  }

  async function checkAuth() {
    try {
      const res = await fetch('/api/auth/check');
      const data = await res.json();
      if (data.authenticated) {
        hideLoginOverlay();
        if (displayUsername) displayUsername.textContent = data.username || 'admin';
        loadAllData();
      } else {
        showLoginOverlay();
      }
    } catch (e) {
      showLoginOverlay();
    }
  }

  function loadAllData() {
    loadStats();
    loadAccounts();
    loadCacheBuckets();
    loadBuckets();
    loadSettings();
  }

  // Initial Boot with Auth Verification
  checkAuth();
});
