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
        single: function(){ this._isSingle=true; return this; },
        then: function(resolve, reject){ return this._exec().then(resolve, reject); },
        _exec: async function(){
          var u = this._url;
          if(this._filters.length){
            var sep = u.includes('?') ? '&' : '?';
            u += sep + this._filters.join('&');
          }
          if(this._isSingle) this._h['Accept'] = 'application/vnd.pgrst.object+json';
          var opts = { method: this._method, headers: this._h };
          if(this._body) opts.body = JSON.stringify(this._body);
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

    function queryUpdate(table, updateData){ return query(table, 'PATCH', updateData, null, {}); }
    function queryDelete(table){ return query(table, 'DELETE', null, null, {}); }

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
          if(!res.ok) return { data: null, error: { message: data.error_description||data.msg||'Помилка входу' } };
          _token = data.access_token;
          _refreshToken = data.refresh_token;
          var user = data.user;
          localStorage.setItem('sb_token', _token);
          localStorage.setItem('sb_refresh', _refreshToken||'');
          localStorage.setItem('sb_user', JSON.stringify(user));
          _authListeners.forEach(function(cb){ cb('SIGNED_IN', {user: user}); });
          return { data: { user: user, session: data }, error: null };
        } catch(e) {
          return { data: null, error: { message: e.message } };
        }
      },
      signOut: async function(){
        try {
          await fetch(_url + '/auth/v1/logout', { method: 'POST', headers: headers() });
        } catch(e){}
        _token = null; _refreshToken = null;
        localStorage.removeItem('sb_token');
        localStorage.removeItem('sb_refresh');
        localStorage.removeItem('sb_user');
        _authListeners.forEach(function(cb){ cb('SIGNED_OUT', null); });
        return { error: null };
      },
      getSession: async function(){
        var token = localStorage.getItem('sb_token');
        var userStr = localStorage.getItem('sb_user');
        if(!token || !userStr) return { data: { session: null }, error: null };
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
          return { data: { session: { user: user, access_token: token } }, error: null };
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

    function channel(name){
      return {
        on: function(type, opts, cb){ this._cb = cb; this._opts = opts; return this; },
        subscribe: function(){
          if(this._cb){
            var table = (this._opts||{}).table;
            if(table){
              setInterval(async function(){
                if(typeof refreshPage === 'function' && !window._saving) refreshPage(table);
              }, 30000);
            }
          }
          return this;
        }
      };
    }

    function removeChannel(ch){}
    function rpc(fn, params){ return query('rpc/' + fn, 'POST', params, null, {}); }

    return { from: from, auth: auth, channel: channel, removeChannel: removeChannel, rpc: rpc };
  }

  return { createClient: createClient };
})();

window.supabase = window.SupabaseMini;

// ── Ініціалізація клієнта Supabase ─────────────────────
// ЗАМІНІТЬ ЦІ ЗНАЧЕННЯ НА ВЛАСНІ, ЯКЩО ВОНИ ВІДРІЗНЯЮТЬСЯ
var SUBA_URL = "https://your-supabase-url.supabase.co"; 
var SUBA_KEY = "your-anon-key-here";
var _sb = window.SupabaseMini.createClient(SUBA_URL, SUBA_KEY);

var CU = null; // Поточний користувач
var S = {};    // Глобальний стан даних

window.__startTime = Date.now();
window.onerror = function(msg, src, line, col, err) {
  if(msg === 'Script error.' || msg === 'Script error') return false;
  var div = document.createElement('div');
  div.style.cssText = 'position:fixed;top:0;left:0;right:0;background:#f8d7da;color:#721c24;padding:16px;font-family:monospace;font-size:13px;z-index:99999;border-bottom:2px solid #f5c6cb';
  div.innerHTML = '<strong>JS Error at line ' + line + ':</strong><br>' + msg + '<br><small>' + (err ? err.stack : '') + '</small>';
  if (document.body) document.body.appendChild(div);
  else document.addEventListener('DOMContentLoaded', function(){ document.body.appendChild(div); });
  return false;
};

// ── ФУНКЦІЯ АВТОРИЗАЦІЇ (яка викликається кнопкою) ────
async function doLogin(e) {
  if(e) e.preventDefault();
  var emailEl = document.getElementById('auth-email');
  var passEl = document.getElementById('auth-password');
  if(!emailEl || !passEl) return;

  var email = emailEl.value.trim();
  var password = passEl.value;

  if(!email || !password) {
    alert('Будь ласка, заповніть усі поля!');
    return;
  }

  var btn = document.querySelector('#form-auth button');
  if(btn) { btn.disabled = true; btn.textContent = 'Вхід...'; }

  var res = await _sb.auth.signInWithPassword({ email: email, password: password });
  
  if(btn) { btn.disabled = false; btn.textContent = 'Увійти'; }

  if(res.error) {
    alert('Помилка входу: ' + res.error.message);
  } else {
    CU = res.data.user;
    location.reload(); // Перезавантажуємо сторінку після успішного входу
  }
}

async function initApp() {
  var res = await _sb.auth.getSession();
  if(res.data && res.data.session) {
    CU = res.data.session.user;
    var authPage = document.getElementById('page-auth');
    var mainLayout = document.getElementById('main-layout');
    if(authPage) authPage.style.display = 'none';
    if(mainLayout) mainLayout.style.display = 'flex';
    // Тут зазвичай викликається завантаження даних, наприклад: loadAllData();
  } else {
    var authPage = document.getElementById('page-auth');
    var mainLayout = document.getElementById('main-layout');
    if(authPage) authPage.style.display = 'flex';
    if(mainLayout) mainLayout.style.display = 'none';
  }
}

function R() {
  if(!CU || !CU.user_metadata) return 'tutor';
  return CU.user_metadata.role || 'tutor';
}

function fd(dateStr) {
  if(!dateStr) return '—';
  var parts = dateStr.split('-');
  if(parts.length !== 3) return dateStr;
  return parts[2] + '.' + parts[1] + '.' + parts[0];
}

function localDateStr(d){
  if(typeof d === 'string') return d;
  var y=d.getFullYear(), m=String(d.getMonth()+1).padStart(2,'0'), dd=String(d.getDate()).padStart(2,'0');
  return y+'-'+m+'-'+dd;
}

function onLessStatChange(){
  var stat=(document.getElementById('l-stat')||{value:''}).value;
  var dur=parseInt((document.getElementById('l-dur')||{value:'60'}).value)||60;
  var mkWrap=document.getElementById('l-makeup-wrap');
  var msWrap=document.getElementById('l-miss-wrap');
  var spWrap=document.getElementById('l-split-wrap');
  if(mkWrap) mkWrap.style.display=stat==='makeup'?'block':'none';
  if(msWrap) msWrap.style.display=(stat==='missed'||stat==='makeup')?'block':'none';
  if(spWrap) spWrap.style.display=((stat==='missed'||stat==='makeup')&&dur>=60)?'block':'none';
}

// Решта функцій інтерфейсу
function updateInvPhone(){
  var sid=(document.getElementById('inv-student')||{value:''}).value;
  var wrap=document.getElementById('inv-phone-wrap');
  if(!sid||!wrap){if(wrap)wrap.style.display='none';return;}
  var s=(S.students||[]).find(function(x){return x.id===sid;});
  wrap.style.display=(s&&(s.phone||s.parentPhone))?'block':'none';
  var ph=document.getElementById('inv-phone');
  if(ph&&s) ph.value=s.phone||s.parentPhone||'';
}

// Реєструємо функції глобально для доступу з HTML
window.doLogin = doLogin;
window.initApp = initApp;
window.onLessStatChange = onLessStatChange;
window.updateInvPhone = updateInvPhone;

document.addEventListener('DOMContentLoaded', initApp);
