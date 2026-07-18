// ═══════════════════════════════════════════════════════════
// CRM Константа — Service Worker
// Стратегія: network-first для коду (html/js/css) — свіжа версія
// завжди з мережі, кеш лише як офлайн-фолбек. Статика (шрифти,
// картинки) — cache-first для швидкості.
// ═══════════════════════════════════════════════════════════

// Міняйте версію при потребі примусово скинути ВСІ кеші користувачів
var CACHE_VERSION = 'crm-v2026-07-18';

// Нова версія воркера активується одразу, без очікування закриття вкладок
self.addEventListener('install', function (e) {
  self.skipWaiting();
});

// При активації: видаляємо ВСІ старі кеші (включно з кешами старого воркера)
// і одразу перебираємо контроль над відкритими вкладками
self.addEventListener('activate', function (e) {
  e.waitUntil(
    caches.keys().then(function (keys) {
      return Promise.all(
        keys.filter(function (k) { return k !== CACHE_VERSION; })
            .map(function (k) { return caches.delete(k); })
      );
    }).then(function () {
      return self.clients.claim();
    })
  );
});

function isCodeRequest(req, url) {
  if (req.mode === 'navigate') return true; // сама сторінка
  return /\.(html|js|css|json)(\?|$)/.test(url.pathname);
}

self.addEventListener('fetch', function (e) {
  var req = e.request;
  if (req.method !== 'GET') return;

  var url;
  try { url = new URL(req.url); } catch (err) { return; }

  // Зовнішні ресурси (Supabase, Google Fonts CSS-запити тощо) — не чіпаємо
  if (url.origin !== self.location.origin) return;

  if (isCodeRequest(req, url)) {
    // ── NETWORK-FIRST: код завжди свіжий, кеш лише коли офлайн ──
    e.respondWith(
      fetch(req).then(function (res) {
        if (res && res.ok) {
          var clone = res.clone();
          caches.open(CACHE_VERSION).then(function (c) { c.put(req, clone); });
        }
        return res;
      }).catch(function () {
        return caches.match(req).then(function (cached) {
          return cached || caches.match('./index.html');
        });
      })
    );
  } else {
    // ── CACHE-FIRST: картинки, шрифти, іконки ──
    e.respondWith(
      caches.match(req).then(function (cached) {
        if (cached) return cached;
        return fetch(req).then(function (res) {
          if (res && res.ok) {
            var clone = res.clone();
            caches.open(CACHE_VERSION).then(function (c) { c.put(req, clone); });
          }
          return res;
        });
      })
    );
  }
});
