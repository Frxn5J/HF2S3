// HF2S3 Dashboard Controller
document.addEventListener('DOMContentLoaded', () => {
  // Global fetch interceptor to inject admin session token and handle 401
  const originalFetch = window.fetch;
  window.fetch = async function(url, options = {}) {
    options = options || {};
    options.headers = options.headers || {};
    const token = localStorage.getItem('hf2s3_admin_token') || '';
    if (token) {
      if (options.headers instanceof Headers) {
        options.headers.set('X-Admin-Token', token);
        options.headers.set('Authorization', 'Bearer ' + token);
      } else if (Array.isArray(options.headers)) {
        options.headers.push(['X-Admin-Token', token]);
        options.headers.push(['Authorization', 'Bearer ' + token]);
      } else {
        options.headers['X-Admin-Token'] = token;
        options.headers['Authorization'] = 'Bearer ' + token;
      }
    }
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
  const setMasterKey = document.getElementById('setMasterKey');
  const btnToggleMasterKeyVisibility = document.getElementById('btnToggleMasterKeyVisibility');
  const btnGenerateMasterKey = document.getElementById('btnGenerateMasterKey');
  const setChunkSize = document.getElementById('setChunkSize');

  // Settings Tab - HF Storage Cache Settings Elements
  const hfStorageForm = document.getElementById('hfStorageForm');
  const setHFStorageEndpoint = document.getElementById('setHFStorageEndpoint');
  const setHFStorageRegion = document.getElementById('setHFStorageRegion');
  const setHFStorageAccessKey = document.getElementById('setHFStorageAccessKey');
  const setHFStorageSecretKey = document.getElementById('setHFStorageSecretKey');
  const setHFStorageBucket = document.getElementById('setHFStorageBucket');
  const hfStorageStatusBadge = document.getElementById('hfStorageStatusBadge');

  // Settings Tab - Database Backup & Restore Elements
  const formRestoreBackup = document.getElementById('formRestoreBackup');
  const restoreBackupInput = document.getElementById('restoreBackupInput');
  const btnChooseBackupFile = document.getElementById('btnChooseBackupFile');
  const restoreBackupFileName = document.getElementById('restoreBackupFileName');
  const btnSubmitRestore = document.getElementById('btnSubmitRestore');

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
      [modalAddAccount, modalCreateBucket, modalInspectChunks].forEach(m => {
        if (m && !m.classList.contains('hidden')) {
          m.classList.add('hidden');
        }
      });
    }
  });

  // Close modals on backdrop click
  [modalAddAccount, modalCreateBucket, modalInspectChunks].forEach(modal => {
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

      let pct = 0;
      if (data.total_capacity_bytes > 0) {
        pct = ((data.total_used_bytes / data.total_capacity_bytes) * 100).toFixed(1);
      }
      metricStoragePercent.textContent = `${pct}% usado`;
      poolProgressBar.style.width = `${Math.max(pct, 2)}%`;

      metricActiveAccounts.textContent = `${data.active_accounts} / ${data.total_accounts}`;
      metricBucketsCount.textContent = data.total_buckets;
      metricObjectsCount.textContent = data.total_objects;
      navAccountsCount.textContent = data.total_accounts;
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
            <button class="btn btn-primary btn-sm" id="btnFirstAccount">+ Conectar Primera Cuenta (100 GB)</button>
          </div>
        `;
        document.getElementById('btnFirstAccount')?.addEventListener('click', () => {
          modalAddAccount.classList.remove('hidden');
        });
      } else {
        accountsCache.forEach(acc => {
          const usedPct = ((acc.used_bytes / acc.quota_bytes) * 100).toFixed(1);
          const card = document.createElement('div');
          card.className = 'account-card';
          card.innerHTML = `
            <div class="account-card-header">
              <div>
                <div class="account-user">${acc.name}</div>
                <div class="account-repo">${acc.repo_name}</div>
              </div>
              <span class="${acc.is_active ? 'badge-active' : 'badge-inactive'}">
                ${acc.is_active ? 'Activo' : 'Pausado'}
              </span>
            </div>
            <div class="progress-bar-wrap">
              <div class="progress-bar" style="width: ${Math.max(usedPct, 2)}%"></div>
            </div>
            <div class="progress-stats">
              <span>${formatBytes(acc.used_bytes)} / ${formatBytes(acc.quota_bytes)}</span>
              <span>${usedPct}%</span>
            </div>
          `;
          overviewAccountsGrid.appendChild(card);
        });
      }

      // Render Accounts Table
      accountsTableBody.innerHTML = '';
      if (accountsCache.length === 0) {
        accountsTableBody.innerHTML = '<tr><td colspan="6" class="empty-state">No hay cuentas conectadas</td></tr>';
      } else {
        accountsCache.forEach(acc => {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td>
              <strong>${acc.name}</strong><br/>
              <span style="font-size:0.75rem; color:var(--text-dim)">@${acc.username}</span>
            </td>
            <td><code>${acc.repo_name}</code></td>
            <td>${formatBytes(acc.used_bytes)}</td>
            <td>${formatBytes(acc.quota_bytes)}</td>
            <td>
              <span class="${acc.is_active ? 'badge-active' : 'badge-inactive'}">
                ${acc.is_active ? 'En Servicio' : 'Desactivado'}
              </span>
            </td>
            <td>
              <button class="btn btn-xs btn-secondary btn-sync-acc" data-id="${acc.id}" title="Sincronizar uso">Sync</button>
              <button class="btn btn-xs btn-outline btn-toggle-acc" data-id="${acc.id}">
                ${acc.is_active ? 'Desactivar' : 'Activar'}
              </button>
              <button class="btn btn-xs btn-danger btn-del-acc" data-id="${acc.id}">Eliminar</button>
            </td>
          `;
          accountsTableBody.appendChild(tr);
        });

        // Attach action handlers
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

        document.querySelectorAll('.btn-del-acc').forEach(btn => {
          btn.addEventListener('click', async () => {
            if (!confirm('¿Seguro que deseas eliminar esta cuenta?')) return;
            const id = btn.dataset.id;
            await fetch(`/api/accounts/${id}`, { method: 'DELETE' });
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
    const quota = parseInt(document.getElementById('hfQuotaInput').value, 10);

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
            <span>${b.name}</span>
          </span>
          <div class="bucket-btn-actions">
            <span class="bucket-item-badge">${b.object_count}</span>
            <button class="btn btn-xs btn-danger btn-del-bucket" data-name="${b.name}" title="Eliminar bucket">&times;</button>
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
          const res = await fetch(`/api/buckets/${name}`, { method: 'DELETE' });
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
      const res = await fetch(`/api/objects?bucket=${bucket}`);
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
        cacheActionBtn = `<button class="btn btn-xs btn-outline btn-evict-file" data-key="${f.key}" title="Desalojar copia de la caché S3 (el original cifrado se preserva en dataset)">Desalojar</button>`;
      } else if (f.has_cold) {
        cacheActionBtn = `<button class="btn btn-xs btn-outline btn-promote-file" data-key="${f.key}" title="Promover copia sin cifrar a la caché S3">Promover</button>`;
      }

      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td>
          <span class="file-key-cell">
            <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path><polyline points="13 2 13 9 20 9"></polyline></svg>
            <strong>${f.key}</strong>
          </span>
        </td>
        <td>${formatBytes(f.size)}</td>
        <td>${tierBadges}</td>
        <td><code style="font-size:0.75rem">${(f.etag || '').replace(/"/g, '')}</code></td>
        <td><span style="font-size:0.75rem;color:var(--text-dim)">${f.content_type || 'binary'}</span></td>
        <td><span style="font-size:0.78rem">${new Date(f.updated_at).toLocaleString()}</span></td>
        <td>
          <a href="/media/${encodeURIComponent(activeBucket)}/${encodeURIComponent(f.key)}" target="_blank" class="btn btn-xs btn-outline btn-stream" title="Descarga Directa o Streaming Multimedia">Stream</a>
          <button class="btn btn-xs btn-secondary btn-dl-file" data-key="${f.key}">Descargar</button>
          ${cacheActionBtn}
          <button class="btn btn-xs btn-outline btn-inspect-file" data-key="${f.key}" title="Ver distribución de chunks en HF">Chunks</button>
          <button class="btn btn-xs btn-danger btn-del-file" data-key="${f.key}">Borrar</button>
        </td>
      `;
      filesTableBody.appendChild(tr);
    });

    // Actions
    document.querySelectorAll('.btn-dl-file').forEach(btn => {
      btn.addEventListener('click', () => {
        const key = btn.dataset.key;
        window.location.href = `/api/objects/download?bucket=${activeBucket}&key=${encodeURIComponent(key)}`;
      });
    });

    document.querySelectorAll('.btn-del-file').forEach(btn => {
      btn.addEventListener('click', async () => {
        const key = btn.dataset.key;
        if (!confirm(`¿Eliminar ${key} de ${activeBucket}? Los chunks se liberarán de Hugging Face.`)) return;
        await fetch(`/api/objects?bucket=${activeBucket}&key=${encodeURIComponent(key)}`, { method: 'DELETE' });
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
      const res = await fetch(`/api/objects/detail?bucket=${activeBucket}&key=${encodeURIComponent(key)}`);
      if (!res.ok) return;
      const data = await res.json();

      chunkInspectSummary.innerHTML = `
        <div style="margin-bottom:14px;background:rgba(255,255,255,0.03);padding:12px;border-radius:var(--radius-md);">
          <div><strong>Objeto:</strong> ${data.object.key} (${formatBytes(data.object.size)})</div>
          <div style="font-size:0.8rem;color:var(--text-dim);margin-top:4px;">Dividido en <strong>${data.chunks.length}</strong> chunks cifrados con AES-256-GCM y distribuidos en el pool de Hugging Face.</div>
        </div>
      `;

      chunksTableBody.innerHTML = '';
      data.chunks.forEach(c => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>#${c.chunk_index + 1}</td>
          <td>${c.offset_bytes} - ${c.offset_bytes + c.size_bytes}</td>
          <td>${formatBytes(c.size_bytes)}</td>
          <td>${formatBytes(c.cipher_size_bytes)}</td>
          <td><span class="badge-active">${c.account_name}</span></td>
          <td><code>${c.remote_path}</code></td>
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
      cfgSecretKey.value = settingsCache.secret_access_key;
      cfgRegion.value = settingsCache.s3_region;

      setAccessKey.value = settingsCache.access_key_id;
      setSecretKey.value = settingsCache.secret_access_key;
      setRegion.value = settingsCache.s3_region;

      // Populate Admin Settings
      if (setAdminUser) setAdminUser.value = settingsCache.admin_username || 'admin';
      if (settingsCurrentAdminBadge) settingsCurrentAdminBadge.textContent = settingsCache.admin_username || 'admin';

      // Populate Crypto & Chunking Settings
      if (setMasterKey) setMasterKey.value = settingsCache.master_key || '';
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
      }

      try {
        const res = await fetch('/api/settings', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        if (res.ok) {
          showToast('Credenciales de administrador actualizadas con éxito');
          setAdminPass.value = '';
          setAdminPassConfirm.value = '';
          if (displayUsername) displayUsername.textContent = user;
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
      secret_access_key: setSecretKey.value.trim(),
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
      }
    } catch (e) {
      showToast('Error al guardar credenciales', 'error');
    }
  });

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

  // Crypto & Chunking Settings
  if (btnToggleMasterKeyVisibility && setMasterKey) {
    btnToggleMasterKeyVisibility.addEventListener('click', () => {
      if (setMasterKey.type === 'password') {
        setMasterKey.type = 'text';
        btnToggleMasterKeyVisibility.textContent = 'Ocultar';
      } else {
        setMasterKey.type = 'password';
        btnToggleMasterKeyVisibility.textContent = 'Mostrar';
      }
    });
  }

  if (btnGenerateMasterKey && setMasterKey) {
    btnGenerateMasterKey.addEventListener('click', () => {
      const charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()-_=+';
      const array = new Uint8Array(32);
      window.crypto.getRandomValues(array);
      let key = '';
      for (let i = 0; i < array.length; i++) {
        key += charset[array[i] % charset.length];
      }
      setMasterKey.value = key;
      setMasterKey.type = 'text';
      if (btnToggleMasterKeyVisibility) btnToggleMasterKeyVisibility.textContent = 'Ocultar';
      showToast('Nueva clave maestra aleatoria generada (asegúrate de guardarla)');
    });
  }

  if (formCrypto) {
    formCrypto.addEventListener('submit', async (e) => {
      e.preventDefault();
      const master_key = setMasterKey.value.trim();
      const chunk_size_mb = parseInt(setChunkSize.value, 10);
      if (!master_key) {
        showToast('La clave maestra no puede estar vacía', 'error');
        return;
      }
      try {
        const res = await fetch('/api/settings', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ master_key, chunk_size_mb })
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

  // Database Restore Handlers
  if (btnChooseBackupFile && restoreBackupInput) {
    btnChooseBackupFile.addEventListener('click', () => {
      restoreBackupInput.click();
    });
    restoreBackupInput.addEventListener('change', () => {
      if (restoreBackupInput.files.length > 0) {
        const file = restoreBackupInput.files[0];
        restoreBackupFileName.textContent = `${file.name} (${formatBytes(file.size)})`;
        btnSubmitRestore.disabled = false;
      } else {
        restoreBackupFileName.textContent = 'Ningún archivo seleccionado';
        btnSubmitRestore.disabled = true;
      }
    });
  }

  if (formRestoreBackup) {
    formRestoreBackup.addEventListener('submit', async (e) => {
      e.preventDefault();
      if (!restoreBackupInput.files || restoreBackupInput.files.length === 0) return;
      const file = restoreBackupInput.files[0];
      if (!confirm(`¿Restaurar la base de datos desde "${file.name}"? La configuración y metadatos actuales serán reemplazados.`)) {
        return;
      }

      btnSubmitRestore.disabled = true;
      btnSubmitRestore.textContent = 'Restaurando...';
      showToast('Importando base de datos SQLite...');

      const formData = new FormData();
      formData.append('database', file);

      try {
        const res = await fetch('/api/admin/restore', {
          method: 'POST',
          body: formData
        });
        const data = await res.json();
        if (!res.ok) {
          showToast(data.error || 'Fallo en la restauración', 'error');
        } else {
          showToast('¡Base de datos restaurada exitosamente! Actualizando interfaz...');
          restoreBackupInput.value = '';
          restoreBackupFileName.textContent = 'Ningún archivo seleccionado';
          loadAllData();
        }
      } catch (err) {
        showToast('Error de comunicación con el gateway', 'error');
      } finally {
        btnSubmitRestore.disabled = false;
        btnSubmitRestore.textContent = 'Restaurar Base de Datos';
      }
    });
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
    } else if (currentTab === 'storage') {
      loadBuckets();
    } else if (currentTab === 'connect' || currentTab === 'settings') {
      loadSettings();
    }
  }

  // Backup Download Handler
  if (btnDownloadBackup) {
    btnDownloadBackup.addEventListener('click', () => {
      const token = localStorage.getItem('hf2s3_admin_token') || '';
      showToast('Generando respaldo íntegro de base de datos...');
      window.location.href = `/api/admin/backup?token=${encodeURIComponent(token)}`;
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
        localStorage.setItem('hf2s3_admin_token', data.token);
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
      localStorage.removeItem('hf2s3_admin_token');
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
    loadBuckets();
    loadSettings();
  }

  // Initial Boot with Auth Verification
  checkAuth();
});
