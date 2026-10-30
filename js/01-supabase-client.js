window.SupabaseMini = (function(){

  function createClient(url, anonKey){
    var _url = url.replace(/\/$/, '');
    var _key = anonKey;
    var _token = null;
    var _refreshToken = null;
    var _authListeners = [];
    var _realtimeWs = null;

    // ── Headers ──────────────────────────────────────────
    function headers(){
      var h = {
        'Content-Type': 'application/json',
        'apikey': _key,
        'Authorization': 'Bearer ' + (_token || _key)
      };
      return h;
    }

    // ── REST query builder ────────────────────────────────
    function from(table){
      return {
        select: function(cols){ return query(table, 'GET', null, cols||'*', {}); },
        insert: function(data){ return query(table, 'POST', data, null, {}); },
        update: function(data){ return queryUpdate(table, data); },
        delete: function(){ return queryDelete(table); },
        upsert: function(data){ return query(table, 'POST', data, null, {'Prefer':'resolution=merge-duplicates'}); }
      };
    }

    function query(table, method, body, select, extraHeaders){
      var params = select && select !== '*' ? '?select=' + encodeURIComponent(select) : '';
      var url_full = _url + '/rest/v1/' + table + params;
      var h = Object.assign({}, headers(), {'Prefer': 'return=representation'}, extraHeaders||{});
      return {
        _table: table, _method: method, _body: body, _url: url_full, _h: h,
        _filters: [],
        eq: function(col, val){ this._filters.push(col + '=eq.' + encodeURIComponent(val)); return this; },
        neq: function(col, val){ this._filters.push(col + '=neq.' + encodeURIComponent(val)); return this; },
        order: function(col, opts){ this._filters.push('order=' + col + (opts&&opts.ascending===false?'.desc':'.asc')); return this; },
        limit: function(n){ this._filters.push('limit=' + n); return this; },
        range: function(from, to){ this._filters.push('offset=' + from); this._filters.push('limit=' + (to - from + 1)); return this; },
        single: function(){ this._isSingle=true; return this; },
        then: function(resolve, reject){ return this._exec().then(resolve, reject); },
        _exec: async function(){
          var u = this._url;
          if(this._filters.length){
            var sep = u.includes('?') ? '&' : '?';
            u += sep + this._filters.join('&');
          }
          if(this._isSingle) this._h['Accept'] = 'application/vnd.pgrst.object+json';
          // keepalive: дозволяє запиту завершитись навіть якщо iOS Safari
          // згортає вкладку/PWA одразу після натискання "Зберегти" —
          // без цього iOS може обірвати fetch і зміна ніколи не долетить до бази.
          var opts = { method: this._method, headers: this._h, keepalive: true };
          if(this._body) opts.body = JSON.stringify(this._body);
          // keepalive у браузерах обмежує розмір тіла запиту (~64KB) — для великих
          // payload (масові операції) вимикаємо keepalive, щоб не зрізати запит.
          try{
            if(opts.body && opts.body.length > 60000) opts.keepalive = false;
          }catch(e){}
          try {
            var res = await fetch(u, opts);
            var text = await res.text();
            var data = text ? JSON.parse(text) : null;
            if(!res.ok) return { data: null, error: { message: (data&&data.message)||res.statusText, code: res.status } };
            return { data: data, error: null };
          } catch(e) {
            return { data: null, error: { message: e.message } };
          }
        }
      };
    }

    function queryUpdate(table, updateData){
      var q = query(table, 'PATCH', updateData, null, {});
      return q;
    }

    function queryDelete(table){
      var q = query(table, 'DELETE', null, null, {});
      return q;
    }

    // ── Auth ──────────────────────────────────────────────
    var auth = {
      signInWithPassword: async function(creds){
        try {
          var res = await fetch(_url + '/auth/v1/token?grant_type=password', {
            method: 'POST',
            headers: { 'Content-Type':'application/json', 'apikey': _key },
            body: JSON.stringify({ email: creds.email, password: creds.password })
          });
          var data = await res.json();
          if(!res.ok) return { data: null, error: { message: data.error_description||data.msg||'Login failed' } };
          _token = data.access_token;
          _refreshToken = data.refresh_token;
          var user = data.user;
          localStorage.setItem('sb_token', _token);
          localStorage.setItem('sb_refresh', _refreshToken||'');
          try{ var _ea=data.expires_at||(Math.floor(Date.now()/1000)+(data.expires_in||3600)); localStorage.setItem('sb_expat', String(_ea)); }catch(e){}
          localStorage.setItem('sb_user', JSON.stringify(user));
          _authListeners.forEach(function(cb){ cb('SIGNED_IN', {user: user}); });
          return { data: { user: user, session: data }, error: null };
        } catch(e) {
          return { data: null, error: { message: e.message } };
        }
      },
      signOut: async function(){
        try {
          await fetch(_url + '/auth/v1/logout', {
            method: 'POST', headers: headers()
          });
        } catch(e){}
        _token = null; _refreshToken = null;
        localStorage.removeItem('sb_token');
        localStorage.removeItem('sb_refresh');
        localStorage.removeItem('sb_user');
        _authListeners.forEach(function(cb){ cb('SIGNED_OUT', null); });
        return { error: null };
      },
      refreshSession: async function(){
        var rt = _refreshToken || localStorage.getItem('sb_refresh');
        if(!rt) return { data:{session:null}, error:{message:'No refresh token'} };
        try{
          var res = await fetch(_url + '/auth/v1/token?grant_type=refresh_token', {
            method:'POST',
            headers:{ 'apikey':_key, 'Content-Type':'application/json' },
            body: JSON.stringify({ refresh_token: rt })
          });
          var data = await res.json().catch(function(){ return {}; });
          if(!res.ok || !data.access_token){
            return { data:{session:null}, error:{message:(data.error_description||data.msg||'Refresh failed')} };
          }
          _token = data.access_token;
          _refreshToken = data.refresh_token || rt;
          var _ea = data.expires_at || (Math.floor(Date.now()/1000) + (data.expires_in||3600));
          localStorage.setItem('sb_token', _token);
          localStorage.setItem('sb_refresh', _refreshToken);
          localStorage.setItem('sb_expat', String(_ea));
          if(data.user) localStorage.setItem('sb_user', JSON.stringify(data.user));
          return { data:{ session: Object.assign({}, data, {access_token:_token, expires_at:_ea}) }, error:null };
        }catch(e){
          return { data:{session:null}, error:{message:e.message} };
        }
      },
      getSession: async function(){
        var token = localStorage.getItem('sb_token');
        var userStr = localStorage.getItem('sb_user');
        if(!token || !userStr) return { data: { session: null }, error: null };
        // Verify token is still valid
        try {
          var res = await fetch(_url + '/auth/v1/user', {
            headers: { 'apikey': _key, 'Authorization': 'Bearer ' + token }
          });
          if(!res.ok){
            localStorage.removeItem('sb_token');
            localStorage.removeItem('sb_user');
            return { data: { session: null }, error: null };
          }
          _token = token;
          var user = await res.json();
          return { data: { session: { user: user, access_token: token, expires_at: parseInt(localStorage.getItem('sb_expat')||'0',10) } }, error: null };
        } catch(e) {
          return { data: { session: null }, error: null };
        }
      },
      onAuthStateChange: function(cb){
        _authListeners.push(cb);
        return { data: { subscription: { unsubscribe: function(){ _authListeners = _authListeners.filter(function(l){ return l!==cb; }); } } } };
      },
      getUser: async function(){
        var token = _token || localStorage.getItem('sb_token');
        if(!token) return { data: { user: null }, error: null };
        try {
          var res = await fetch(_url + '/auth/v1/user', {
            headers: { 'apikey': _key, 'Authorization': 'Bearer ' + token }
          });
          var user = await res.json();
          return { data: { user: user }, error: null };
        } catch(e) {
          return { data: { user: null }, error: null };
        }
      }
    };

    // ── Realtime (simplified polling fallback) ────────────
    function channel(name){
      return {
        on: function(type, opts, cb){ this._cb = cb; this._opts = opts; return this; },
        subscribe: function(){
          // Use polling every 5s as fallback for realtime
          if(this._cb){
            var cb = this._cb;
            var opts = this._opts||{};
            var table = opts.table;
            var lastPoll = Date.now();
            if(table){
              setInterval(async function(){
                // Just trigger a refresh - skip if currently saving
                if(typeof refreshPage === 'function' && !window._saving) refreshPage(table);
              }, 30000);
            }
          }
          return this;
        }
      };
    }

    function removeChannel(ch){}

    // ── RPC ──────────────────────────────────────────────
    function rpc(fn, params){
      return query('rpc/' + fn, 'POST', params, null, {});
    }

    // ── Storage (мінімальна реалізація — лише те, що реально використовується:
    //    підписані посилання для завантаження файлів резервних копій) ──────
    function storageFrom(bucket){
      return {
        createSignedUrl: async function(path, expiresIn){
          try{
            var res = await fetch(_url + '/storage/v1/object/sign/' + bucket + '/' + encodeURIComponent(path).replace(/%2F/g,'/'), {
              method: 'POST',
              headers: headers(),
              body: JSON.stringify({ expiresIn: expiresIn || 60 })
            });
            var data = await res.json();
            if(!res.ok) return { data: null, error: { message: data.message || res.statusText } };
            // API повертає відносний шлях (напр. "/object/sign/bucket/path?token=...") —
            // додаємо базовий URL, щоб отримати повне посилання, готове для window.open
            var signedURL = data.signedURL || data.signedUrl || '';
            var fullUrl = signedURL.indexOf('http') === 0 ? signedURL : (_url + '/storage/v1' + signedURL);
            return { data: { signedUrl: fullUrl }, error: null };
          }catch(e){
            return { data: null, error: { message: e.message } };
          }
        },
        upload: async function(path, fileBody, opts){
          try{
            var h = Object.assign({}, headers());
            delete h['Content-Type']; // браузер сам виставить multipart/binary Content-Type
            if(opts&&opts.contentType) h['Content-Type']=opts.contentType;
            var res = await fetch(_url + '/storage/v1/object/' + bucket + '/' + encodeURIComponent(path).replace(/%2F/g,'/'), {
              method: 'POST', headers: h, body: fileBody
            });
            var data = await res.json().catch(function(){ return null; });
            if(!res.ok) return { data: null, error: { message: (data&&data.message)||res.statusText } };
            return { data: data, error: null };
          }catch(e){
            return { data: null, error: { message: e.message } };
          }
        },
        remove: async function(paths){
          try{
            var res = await fetch(_url + '/storage/v1/object/' + bucket, {
              method: 'DELETE', headers: headers(), body: JSON.stringify({ prefixes: paths })
            });
            var data = await res.json().catch(function(){ return null; });
            if(!res.ok) return { data: null, error: { message: (data&&data.message)||res.statusText } };
            return { data: data, error: null };
          }catch(e){
            return { data: null, error: { message: e.message } };
          }
        }
      };
    }
    var storage = { from: storageFrom };

    return { from: from, auth: auth, channel: channel, removeChannel: removeChannel, rpc: rpc, storage: storage };
  }

  return { createClient: createClient };
})();

// Make it available as 'supabase' global (matching SDK interface)
window.supabase = window.SupabaseMini;
