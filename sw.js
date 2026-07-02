const CACHE_NAME = 'konstanta-crm-v3';
const ASSETS = [
  '/konstanta-crm/',
  '/konstanta-crm/index.html',
  '/konstanta-crm/app7.js',
  '/konstanta-crm/manifest.json',
  '/konstanta-crm/icon-192.png',
  '/konstanta-crm/icon-512.png'
];

self.addEventListener('install', function(e) {
  e.waitUntil(caches.open(CACHE_NAME).then(c => c.addAll(ASSETS)));
  self.skipWaiting();
});

self.addEventListener('activate', function(e) {
  e.waitUntil(
    caches.keys().then(keys => Promise.all(
      keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))
    ))
  );
  self.clients.claim();
});

self.addEventListener('fetch', function(e) {
  if(e.request.url.includes('supabase.co')) return;
  e.respondWith(
    caches.match(e.request).then(cached => cached || fetch(e.request))
  );
});
