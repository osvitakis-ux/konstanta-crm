
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
                // Just trigger a refresh - loadTableFresh handles the actual reload
                if(typeof refreshPage === 'function') refreshPage(table);
              }, 8000);
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

    return { from: from, auth: auth, channel: channel, removeChannel: removeChannel, rpc: rpc };
  }

  return { createClient: createClient };
})();

// Make it available as 'supabase' global (matching SDK interface)
window.supabase = window.SupabaseMini;

window.__startTime = Date.now();
window.onerror = function(msg, src, line, col, err) {
  // Skip CORS errors from external scripts
  if(msg === 'Script error.' || msg === 'Script error') {
    console.warn('External script error (possibly CDN) - check network');
    return false;
  }
  var div = document.createElement('div');
  div.style.cssText = 'position:fixed;top:0;left:0;right:0;background:#f8d7da;color:#721c24;padding:16px;font-family:monospace;font-size:13px;z-index:99999;border-bottom:2px solid #f5c6cb';
  div.innerHTML = '<strong>JS Error at line ' + line + ':</strong><br>' + msg + '<br><small>' + (err ? err.stack : '') + '</small>';
  document.body ? document.body.appendChild(div) : document.addEventListener('DOMContentLoaded', function(){ document.body.appendChild(div); });
  return false;
};




// =
//  CRM - SUPABASE EDITION
// =

// = Constants & Roles =

var ROLES = {
  god: {
    label:'\u0411\u043E\u0433 \u0441\u0438\u0441\u0442\u0435\u043C\u0438', icon:'\u26A1', color:'var(--god2)',
    avatarBg:'linear-gradient(135deg,#2e3192,#5b60d4)',
    nav:['dashboard','students','tutors','schedule','lessons','payments','reports','users','settings'],
    can:{students:true,tutors:true,lessons:true,payments:true,users:true,settings:true,danger:true,deleteAny:true},
    seeIncome:true, seeAll:true, canEditUsers:true, showGodBanner:true
  },
  director: {
    label:'\u0414\u0438\u0440\u0435\u043A\u0442\u043E\u0440', icon:'\uD83D\uDC51', color:'var(--dir)',
    avatarBg:'linear-gradient(135deg,#d9e021,#fcee21)',
    nav:['dashboard','students','tutors','schedule','lessons','payments','reports','users','settings'],
    can:{students:true,tutors:true,lessons:true,payments:true,users:true,settings:true,danger:false,deleteAny:true},
    seeIncome:true, seeAll:true, canEditUsers:true, showGodBanner:false
  },
  admin: {
    label:'\u0410\u0434\u043C\u0456\u043D\u0456\u0441\u0442\u0440\u0430\u0442\u043E\u0440', icon:'\uD83D\uDEE1\uFE0F', color:'var(--adm)',
    avatarBg:'linear-gradient(135deg,#29abe2,#3fa9f5)',
    nav:['dashboard','students','tutors','schedule','lessons','payments','reports'],
    can:{students:true,tutors:true,lessons:true,payments:true,users:false,settings:true,danger:false,deleteAny:false},
    seeIncome:true, seeAll:true, canEditUsers:false, showGodBanner:false
  },
  network_admin: {
    label:'\u0410\u0434\u043C\u0456\u043D \u043C\u0435\u0440\u0435\u0436\u0456', icon:'\uD83C\uDF10', color:'var(--god2)',
    avatarBg:'linear-gradient(135deg,#5b60d4,#29abe2)',
    nav:['dashboard','students','tutors','schedule','lessons','payments','reports','users','settings'],
    can:{students:true,tutors:true,lessons:true,payments:true,users:true,settings:true,danger:false,deleteAny:true},
    seeIncome:true, seeAll:true, canEditUsers:true, showGodBanner:false
  },
  tutor: {
    label:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440', icon:'\uD83D\uDCDA', color:'var(--tut)',
    avatarBg:'linear-gradient(135deg,#22b573,#7ac943)',
    nav:['dashboard','students','schedule','lessons','profile'],
    can:{students:true,tutors:false,lessons:true,payments:false,users:false,settings:false,danger:false,deleteAny:false},
    seeIncome:false, seeAll:false, canEditUsers:false, showGodBanner:false
  },
  };

var NAV_CFG = [
  {id:'dashboard',  ico:'\u229E',  lbl:'\u0414\u0430\u0448\u0431\u043E\u0440\u0434',     sec:'\u0413\u043E\u043B\u043E\u0432\u043D\u0435'},
  {id:'students',   ico:'\u25CE',  lbl:'\u0423\u0447\u043D\u0456',         sec:'\u0413\u043E\u043B\u043E\u0432\u043D\u0435', badge:true},
  {id:'tutors',     ico:'\u25C8',  lbl:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438',    sec:'\u0413\u043E\u043B\u043E\u0432\u043D\u0435'},
  {id:'schedule',   ico:'\u25A6',  lbl:'\u0420\u043E\u0437\u043A\u043B\u0430\u0434',      sec:'\u041D\u0430\u0432\u0447\u0430\u043D\u043D\u044F'},
  {id:'lessons',    ico:'\u25C9',  lbl:'\u0417\u0430\u043D\u044F\u0442\u0442\u044F',      sec:'\u041D\u0430\u0432\u0447\u0430\u043D\u043D\u044F'},
  {id:'payments',   ico:'\u25C8',  lbl:'\u041E\u043F\u043B\u0430\u0442\u0430',       sec:'\u0424\u0456\u043D\u0430\u043D\u0441\u0438'},
  {id:'reports',    ico:'\u25E7',  lbl:'\u0410\u043D\u0430\u043B\u0456\u0442\u0438\u043A\u0430',    sec:'\u0424\u0456\u043D\u0430\u043D\u0441\u0438'},
  {id:'analytics',  ico:'\u25A4',  lbl:'\u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043A\u0430',   sec:'\u0424\u0456\u043D\u0430\u043D\u0441\u0438'},
  {id:'users',      ico:'\u25CE',  lbl:'\u0410\u043A\u0430\u0443\u043D\u0442\u0438',      sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'branches',   ico:'\uD83C\uDFE2',  lbl:'\u0424\u0456\u043B\u0456\u0457',         sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'settings',   ico:'\u25C9',  lbl:'\u041D\u0430\u043B\u0430\u0448\u0442\u0443\u0432\u0430\u043D\u043D\u044F', sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'profile',    ico:'\u25A3',  lbl:'\u041C\u0456\u0439 \u043F\u0440\u043E\u0444\u0456\u043B\u044C',  sec:'\u041E\u0441\u043E\u0431\u0438\u0441\u0442\u0435'},
];

var DEFAULT_NAV_CFG = NAV_CFG;

var PLABELS={dashboard:'\u0414\u0430\u0448\u0431\u043E\u0440\u0434',students:'\u0423\u0447\u043D\u0456',tutors:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438',schedule:'\u0420\u043E\u0437\u043A\u043B\u0430\u0434',lessons:'\u0417\u0430\u043D\u044F\u0442\u0442\u044F',payments:'\u041E\u043F\u043B\u0430\u0442\u0430',reports:'\u0410\u043D\u0430\u043B\u0456\u0442\u0438\u043A\u0430',users:'\u0410\u043A\u0430\u0443\u043D\u0442\u0438',settings:'\u041D\u0430\u043B\u0430\u0448\u0442\u0443\u0432\u0430\u043D\u043D\u044F',profile:'\u041C\u0456\u0439 \u043F\u0440\u043E\u0444\u0456\u043B\u044C',analytics:'\u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043A\u0430'};

var UA_PERMS=[
  {k:'students',  lbl:'\u0423\u0447\u043D\u0456 \u2014 \u043F\u0435\u0440\u0435\u0433\u043B\u044F\u0434 \u0456 \u0440\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u043D\u043D\u044F'},
  {k:'tutors',    lbl:'\u0412\u0438\u043A\u043B\u0430\u0434\u0430\u0447\u0456 \u2014 \u0440\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u043D\u043D\u044F'},
  {k:'lessons',   lbl:'\u0417\u0430\u043D\u044F\u0442\u0442\u044F \u2014 \u0440\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u043D\u043D\u044F'},
  {k:'payments',  lbl:'\u041E\u043F\u043B\u0430\u0442\u0430 \u2014 \u043F\u0435\u0440\u0435\u0433\u043B\u044F\u0434 \u0456 \u0440\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u043D\u043D\u044F'},
  {k:'users',     lbl:'\u0410\u043A\u0430\u0443\u043D\u0442\u0438 \u2014 \u0443\u043F\u0440\u0430\u0432\u043B\u0456\u043D\u043D\u044F'},
  {k:'settings',  lbl:'\u041D\u0430\u043B\u0430\u0448\u0442\u0443\u0432\u0430\u043D\u043D\u044F \u0446\u0435\u043D\u0442\u0440\u0443'},
  {k:'danger',    lbl:'\u041D\u0435\u0431\u0435\u0437\u043F\u0435\u0447\u043D\u0430 \u0437\u043E\u043D\u0430'},
  {k:'seeIncome', lbl:'\u0411\u0430\u0447\u0438\u0442\u0438 \u0444\u0456\u043D\u0430\u043D\u0441\u0438 \u0442\u0430 \u0434\u043E\u0445\u043E\u0434\u0438'},
  {k:'seeAll',    lbl:'\u0411\u0430\u0447\u0438\u0442\u0438 \u0432\u0441\u0456 \u0437\u0430\u043F\u0438\u0441\u0438 (\u043D\u0435 \u0442\u0456\u043B\u044C\u043A\u0438 \u0441\u0432\u043E\u0457)'},
  {k:'deleteAny', lbl:'\u0412\u0438\u0434\u0430\u043B\u044F\u0442\u0438 \u0431\u0443\u0434\u044C-\u044F\u043A\u0456 \u0437\u0430\u043F\u0438\u0441\u0438'},
];

var UA_PAGES=[
  {id:'dashboard', ico:'\u229E',lbl:'\u0414\u0430\u0448\u0431\u043E\u0440\u0434',     sec:'\u0413\u043E\u043B\u043E\u0432\u043D\u0435'},
  {id:'students',  ico:'\u25CE',lbl:'\u0423\u0447\u043D\u0456',         sec:'\u0413\u043E\u043B\u043E\u0432\u043D\u0435'},
  {id:'tutors',    ico:'\u25C8',lbl:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438',    sec:'\u0413\u043E\u043B\u043E\u0432\u043D\u0435'},
  {id:'schedule',  ico:'\u25A6',lbl:'\u0420\u043E\u0437\u043A\u043B\u0430\u0434',      sec:'\u041D\u0430\u0432\u0447\u0430\u043D\u043D\u044F'},
  {id:'lessons',   ico:'\u25C9',lbl:'\u0417\u0430\u043D\u044F\u0442\u0442\u044F',      sec:'\u041D\u0430\u0432\u0447\u0430\u043D\u043D\u044F'},
  {id:'payments',  ico:'\u25C8',lbl:'\u041E\u043F\u043B\u0430\u0442\u0430',       sec:'\u0424\u0456\u043D\u0430\u043D\u0441\u0438'},
  {id:'reports',   ico:'\u25E7',lbl:'\u0410\u043D\u0430\u043B\u0456\u0442\u0438\u043A\u0430',    sec:'\u0424\u0456\u043D\u0430\u043D\u0441\u0438'},
  {id:'users',     ico:'\u25CE',lbl:'\u0410\u043A\u0430\u0443\u043D\u0442\u0438',      sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'settings',  ico:'\u25C9',lbl:'\u041D\u0430\u043B\u0430\u0448\u0442\u0443\u0432\u0430\u043D\u043D\u044F', sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'profile',   ico:'\u25A3',lbl:'\u041C\u0456\u0439 \u043F\u0440\u043E\u0444\u0456\u043B\u044C',  sec:'\u041E\u0441\u043E\u0431\u0438\u0441\u0442\u0435'},
];

var PERM_LABELS = {
  students:'\u0423\u0447\u043D\u0456 \u2014 \u043F\u0435\u0440\u0435\u0433\u043B\u044F\u0434 \u0456 \u0440\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u043D\u043D\u044F',
  tutors:'\u0412\u0438\u043A\u043B\u0430\u0434\u0430\u0447\u0456 \u2014 \u0440\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u043D\u043D\u044F',
  lessons:'\u0417\u0430\u043D\u044F\u0442\u0442\u044F \u2014 \u0440\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u043D\u043D\u044F',
  payments:'\u041E\u043F\u043B\u0430\u0442\u0430 \u2014 \u043F\u0435\u0440\u0435\u0433\u043B\u044F\u0434 \u0456 \u0440\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u043D\u043D\u044F',
  users:'\u0410\u043A\u0430\u0443\u043D\u0442\u0438 \u2014 \u0443\u043F\u0440\u0430\u0432\u043B\u0456\u043D\u043D\u044F',
  settings:'\u041D\u0430\u043B\u0430\u0448\u0442\u0443\u0432\u0430\u043D\u043D\u044F \u0446\u0435\u043D\u0442\u0440\u0443',
  danger:'\u041D\u0435\u0431\u0435\u0437\u043F\u0435\u0447\u043D\u0430 \u0437\u043E\u043D\u0430 (\u0441\u043A\u0438\u0434\u0430\u043D\u043D\u044F \u0434\u0430\u043D\u0438\u0445)',
  seeIncome:'\u0411\u0430\u0447\u0438\u0442\u0438 \u0444\u0456\u043D\u0430\u043D\u0441\u0438 \u0442\u0430 \u0434\u043E\u0445\u043E\u0434\u0438',
  seeAll:'\u0411\u0430\u0447\u0438\u0442\u0438 \u0432\u0441\u0456 \u0437\u0430\u043F\u0438\u0441\u0438 (\u043D\u0435 \u0442\u0456\u043B\u044C\u043A\u0438 \u0441\u0432\u043E\u0457)'
};

var COMM_TYPES={
  call:  {ico:'\uD83D\uDCDE', label:'\u0414\u0437\u0432\u0456\u043D\u043E\u043A',     color:'#29abe2'},
  msg:   {ico:'\uD83D\uDCAC', label:'\u041F\u043E\u0432\u0456\u0434\u043E\u043C\u043B\u0435\u043D\u043D\u044F',color:'#22b573'},
  meeting:{ico:'\uD83E\uDD1D',label:'\u0417\u0443\u0441\u0442\u0440\u0456\u0447',     color:'#d9e021'},
  email: {ico:'\uD83D\uDCE7', label:'Email',       color:'#a78bfa'},
  other: {ico:'\uD83D\uDCCB', label:'\u0406\u043D\u0448\u0435',        color:'#7a8aaa'},
};

var DEFAULT_PERMS = {
  god:     {students:true,tutors:true,lessons:true,payments:true,users:true,settings:true,danger:true,seeIncome:true,seeAll:true},
  director:{students:true,tutors:true,lessons:true,payments:true,users:true,settings:true,danger:false,seeIncome:true,seeAll:true},
  admin:   {students:true,tutors:false,lessons:true,payments:true,users:false,settings:false,danger:false,seeIncome:true,seeAll:true},
  tutor:   {students:false,tutors:false,lessons:true,payments:false,users:false,settings:false,danger:false,seeIncome:false,seeAll:false}
};

var RIGHTS_MATRIX = [
  ['\u0424\u0443\u043D\u043A\u0446\u0456\u044F','\u26A1 \u0411\u043E\u0433','\uD83D\uDC51 \u0414\u0438\u0440\u0435\u043A\u0442\u043E\u0440','\uD83D\uDEE1\uFE0F \u0410\u0434\u043C\u0456\u043D','\uD83D\uDCDA \u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440'],
  ['\u041F\u0435\u0440\u0435\u0433\u043B\u044F\u0434 \u0443\u0441\u0456\u0445 \u0443\u0447\u043D\u0456\u0432','\u2705','\u2705','\u2705','\u2705 \u0421\u0432\u043E\u0457\u0445'],
  ['\u0420\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u043D\u043D\u044F \u0443\u0447\u043D\u0456\u0432','\u2705','\u2705','\u2705','\u274C'],
  ['\u041F\u0435\u0440\u0435\u0433\u043B\u044F\u0434 \u0432\u0438\u043A\u043B\u0430\u0434\u0430\u0447\u0456\u0432','\u2705','\u2705','\u2705 \u043E\u0433\u043B\u044F\u0434','\u274C'],
  ['\u0420\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u043D\u043D\u044F \u0432\u0438\u043A\u043B\u0430\u0434\u0430\u0447\u0456\u0432','\u2705','\u2705','\u274C','\u274C'],
  ['\u0420\u043E\u0437\u043A\u043B\u0430\u0434 \u2014 \u0432\u0441\u0456','\u2705','\u2705','\u2705','\u2705 \u0421\u0432\u0456\u0439'],
  ['\u0417\u0430\u043D\u044F\u0442\u0442\u044F \u2014 \u0440\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u043D\u043D\u044F','\u2705','\u2705','\u2705','\u2705 \u0421\u0432\u043E\u0457'],
  ['\u0424\u0456\u043D\u0430\u043D\u0441\u0438 / \u043E\u043F\u043B\u0430\u0442\u0438','\u2705','\u2705','\u2705','\u274C'],
  ['\u0410\u043D\u0430\u043B\u0456\u0442\u0438\u043A\u0430 \u2014 \u0434\u043E\u0445\u043E\u0434\u0438','\u2705','\u2705','\u2705','\u274C'],
  ['\u0423\u043F\u0440\u0430\u0432\u043B\u0456\u043D\u043D\u044F \u0430\u043A\u0430\u0443\u043D\u0442\u0430\u043C\u0438','\u2705','\u2705','\u274C','\u274C'],
  ['\u041D\u0430\u043B\u0430\u0448\u0442\u0443\u0432\u0430\u043D\u043D\u044F \u0446\u0435\u043D\u0442\u0440\u0443','\u2705','\u2705','\u274C','\u274C'],
  ['\u041D\u0435\u0431\u0435\u0437\u043F\u0435\u0447\u043D\u0430 \u0437\u043E\u043D\u0430','\u2705','\u274C','\u274C','\u274C'],
  ['\u0420\u043E\u043B\u044C "\u0411\u043E\u0433" \u0456\u043D\u0448\u0438\u043C','\u2705','\u274C','\u274C','\u274C'],
  ['\u0421\u043A\u0438\u0434\u0430\u043D\u043D\u044F \u0432\u0441\u0456\u0445 \u0434\u0430\u043D\u0438\u0445','\u2705','\u274C','\u274C','\u274C'],
];

// = UI & Render functions =

function R(){return CU?.role||'tutor';}

function P(){return ROLES[R()];}

function userPerms(){
  // Returns merged permissions: role defaults + user-level overrides
  if(!CU)return {};
  var up=CU.perms||{};
  var rp=P().can||{};
  return Object.assign({},rp,up.can||{});
}

function userNav(){
  // Returns nav pages: role defaults + user-level nav overrides
  if(!CU)return [];
  var up=CU.perms||{};
  var roleNav=ROLES[R()].nav||[];
  // User can have pages removed (hide:[]) or added (show:[])
  var hide=up.hideNav||[];
  var show=up.showNav||[];
  var nav=roleNav.filter(function(p){return !hide.includes(p);});
  show.forEach(function(p){if(!nav.includes(p))nav.push(p);});
  return nav;
}

function can(k){
  return userPerms()[k]||false;
}

function isSuperAdmin(){
  return R()==='god' || R()==='network_admin';
}

function currentBranch(){
  return S.currentBranchId || null;
}

function branchName(id){
  const b=(S.branches||[]).find(x=>x.id===id);
  return b?b.name:'\u2014';
}

function filterByBranch(arr){
  const bid=currentBranch();
  // Super admins with no specific branch selected see all
  if(!bid && isSuperAdmin()) return arr;
  // If no branch set and not super admin, use user's branch
  const activeBid = bid || myBranchId();
  if(!activeBid) return arr;
  return (arr||[]).filter(x=>!x.branchId||x.branchId===activeBid);
}

function myBranchId(){
  // For branch-level users, return their assigned branch
  if(isSuperAdmin()) return currentBranch();
  return CU?.branchId || (S.branches[0]?.id);
}

function mkAv(fn,ln,sz=30){
  const cs=['#6c8fff','#a78bfa','#34d399','#f59e0b','#f87171','#0ea5e9','#ec4899','#ff6b35'];
  const i=((fn||'A').charCodeAt(0)+((ln||'B').charCodeAt(0)))%cs.length;
  return ("<div class=\"av\" style=\"background:"+(cs[i])+";width:"+(sz)+"px;height:"+(sz)+"px;font-size:"+(sz*.38)+"px;color:#fff\">"+((fn||'?')[0])+((ln||'')[0]||'')+"</div>");
}

function bst(s){
  var m={active:'bg',trial:'bb',paused:'by',completed:'br',planned:'bb',done:'bg',cancelled:'br',missed:'br',makeup:'by',paid:'bg',pending:'by',overdue:'br'};
  var l={active:'Активний',trial:'Пробне',paused:'Призупин.',completed:'Завершив',planned:'Планов.',done:'Проведено',cancelled:'Скасов.',missed:'Пропущено',makeup:'Відпрацювання',paid:'Оплачено',pending:'Очікується',overdue:'Прострочено'};
  return '<span class="badge '+(m[s]||'bb')+'">'+( l[s]||s)+'</span>';
}

function fd(d){if(!d)return '\u2014';return new Date(d).toLocaleDateString('uk-UA',{day:'2-digit',month:'2-digit',year:'numeric'});}

function fd2(l,p){document.getElementById('lu').value=l;document.getElementById('lp').value=p;}

function sn(id){const s=S.students.find(x=>x.id===id);return s?s.fn+' '+s.ln:'\u2014';}

function tn(id){const t=S.tutors.find(x=>x.id===id);return t?t.fn+' '+t.ln:'\u2014';}

function mkToast(msg,type='success'){
  const e=document.createElement('div');e.className=("toast "+(type));
  e.innerHTML=("<span>"+(type==='success'?'\u2705':'\u274C')+"</span> "+(msg));
  document.body.appendChild(e);setTimeout(()=>e.remove(),3000);
}

function popSel(id,arr,valKey,lblFn,placeholder='\u2014'){const el=document.getElementById(id);if(!el)return;const cur=el.value;el.innerHTML=("<option value=\"\">"+(placeholder)+"</option>")+arr.map(x=>("<option value=\""+(x[valKey])+"\">"+(lblFn(x))+"</option>")).join('');el.value=cur;}

function openM(id){document.getElementById(id).classList.add('open');}

function closeM(id){const el=document.getElementById(id);if(el){el.style.display='none';el.classList.remove('open');}S.editId=null;}

function toggleSidebar(){
  var sb=document.querySelector('.sb');
  var ov=document.getElementById('sb-overlay');
  if(!sb)return;
  sb.classList.toggle('open');
  if(ov)ov.classList.toggle('open',sb.classList.contains('open'));
}

function closeSidebar(){
  var sb=document.querySelector('.sb');
  var ov=document.getElementById('sb-overlay');
  if(sb)sb.classList.remove('open');
  if(ov)ov.classList.remove('open');
}

function myLessons(){
  const all=filterByBranch(S.lessons);
  if(P().seeAll)return all;
  const mt=S.tutors.find(t=>t.accId===CU?.id);
  return mt?all.filter(l=>l.tutorId===mt.id):[];
}

function myStudents(){
  var all=filterByBranch(S.students||[]);
  try{ if(P().seeAll) return all; }catch(e){ return all; }
  var cuId = CU ? CU.id : null;
  var mt=S.tutors.find(function(t){return t.accId===cuId||t.acc_uid===cuId;});
  return mt ? all.filter(function(s){return s.tutorId===mt.id||s.tutor_id===mt.id;}) : all;
}

function myTutor(){return S.tutors.find(t=>t.accId===CU?.id)||null;}

function calcPrice(subjectName, tutorId, grade, dur){
  // Match rules by specificity: most specific wins
  const rules = S.pricingRules || [];
  if(!rules.length){
    // Fallback: subject base price
    const subj = (S.subjects||[]).find(s=>s.name===subjectName);
    return subj && subj.price ? parseFloat(subj.price) : 0;
  }
  // Score each rule: +3 subject, +2 tutor, +1 grade, +1 dur
  let best = null, bestScore = -1;
  rules.forEach(function(r){
    if(!r.price) return;
    var score = 0, match = true;
    if(r.subjectMatch){
      if(subjectName && subjectName.toLowerCase().includes(r.subjectMatch.toLowerCase())) score+=3;
      else { match=false; }
    }
    if(r.tutorId){
      if(r.tutorId === tutorId) score+=2;
      else { match=false; }
    }
    if(r.gradeMatch){
      var g = String(grade||'');
      if(g && g.toLowerCase().includes(r.gradeMatch.toLowerCase())) score+=1;
      else { match=false; }
    }
    if(r.durMin){
      if(parseInt(dur||60) >= parseInt(r.durMin)) score+=1;
      else { match=false; }
    }
    if(match && score >= bestScore){
      bestScore = score; best = r;
    }
  });
  return best ? parseFloat(best.price) : 0;
}

function autoFillPrice(){
  const subj   = document.getElementById('l-subj')?.value || '';
  const tutorId= document.getElementById('l-tutor')?.value || '';
  const dur    = document.getElementById('l-dur')?.value || 60;
  // Get student grade
  const stId   = document.getElementById('l-std')?.value || '';
  const st     = (S.students||[]).find(s=>s.id===stId);
  const grade  = st?.grade || '';
  const price  = calcPrice(subj, tutorId, grade, dur);
  const pEl    = document.getElementById('l-price');
  if(pEl && price) pEl.value = price;
  if(price) mkToast('\u0426\u0456\u043D\u0443 \u043F\u0456\u0434\u0456\u0431\u0440\u0430\u043D\u043E: '+price+' \u20B4','info');
  else mkToast('\u041F\u0440\u0430\u0432\u0438\u043B\u043E \u0446\u0456\u043D\u0438 \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E','error');
}

function getWeekRange(offset){
  var now=new Date();
  var day=now.getDay()===0?6:now.getDay()-1;
  var mon=new Date(now); mon.setDate(now.getDate()-day+offset*7); mon.setHours(0,0,0,0);
  var sun=new Date(mon); sun.setDate(mon.getDate()+6); sun.setHours(23,59,59,999);
  var fmt=function(d){return d.toLocaleDateString('uk-UA',{day:'numeric',month:'short'});};
  return {mon:mon,sun:sun,label:fmt(mon)+' \u2014 '+fmt(sun)};
}

function inWeek(dateStr,wr){
  if(!dateStr)return false;
  var d=new Date(dateStr+'T12:00:00');
  return d>=wr.mon&&d<=wr.sun;
}

function dashKpiWeek(dir){
  if(dir===0)S.dashWeekOffset=0;
  else S.dashWeekOffset=(S.dashWeekOffset||0)+dir;
  renderDashKpi();
  renderDashTrends();
}

function renderDash(){
  try{ renderDashStats(); }catch(e){ console.error('renderDashStats:',e); }
  try{ renderDashKpi(); }catch(e){ console.error('renderDashKpi:',e); }
  try{ renderDashTrends(); }catch(e){ console.error('renderDashTrends:',e); showErr('renderDashTrends: '+e.message); }
  try{ renderCommLog(); }catch(e){ console.error('renderCommLog:',e); }
  try{ renderDashBottom(); }catch(e){ console.error('renderDashBottom:',e); }
}
function showErr(msg){
  var d=document.getElementById('debug-err')||document.createElement('div');
  d.id='debug-err';
  d.style.cssText='position:fixed;bottom:10px;right:10px;background:#f8d7da;color:#721c24;padding:12px 16px;border-radius:8px;font-size:12px;font-family:monospace;z-index:9999;max-width:400px;word-break:break-all';
  d.textContent=msg;
  document.body.appendChild(d);
}

function renderDashStats(){
  var ml=myLessons(), ms=myStudents(), now=new Date();
  var monthL=ml.filter(function(l){
    var d=new Date(l.date);
    return d.getMonth()===now.getMonth()&&d.getFullYear()===now.getFullYear();
  });
  var nb=document.getElementById('nb-s'); if(nb)nb.textContent=S.students.length;
  var statsHtml='<div class="sc blue">'
    +'<div class="slbl">\u0410\u043A\u0442\u0438\u0432\u043D\u0438\u0445 \u0443\u0447\u043D\u0456\u0432</div>'
    +'<div class="sval">'+ms.filter(function(s){return s.status==='active';}).length+'</div>'
    +'<div class="ssub">\u0417\u0430\u0433\u0430\u043B\u043E\u043C: '+ms.length+'</div><span class="sico">\u25CE</span></div>'
    +'<div class="sc green">'
    +'<div class="slbl">\u0417\u0430\u043D\u044F\u0442\u044C \u0446\u044C\u043E\u0433\u043E \u043C\u0456\u0441\u044F\u0446\u044F</div>'
    +'<div class="sval">'+monthL.length+'</div>'
    +'<div class="ssub">\u041F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E: '+monthL.filter(function(l){return l.status==='done'||l.status==='completed';}).length+'</div>'
    +'<span class="sico">\u25C9</span></div>';
  if(P().seeIncome && R()!=='tutor'){
    var inc=S.payments.filter(function(p){
      var d=new Date(p.date);
      return p.status==='paid'&&d.getMonth()===now.getMonth()&&d.getFullYear()===now.getFullYear();
    }).reduce(function(a,p){return a+p.amount;},0);
    statsHtml+='<div class="sc yellow">'
      +'<div class="slbl">\u0414\u043E\u0445\u0456\u0434 \u0446\u044C\u043E\u0433\u043E \u043C\u0456\u0441\u044F\u0446\u044F</div>'
      +'<div class="sval">'+inc.toLocaleString('uk-UA')+'\u20B4</div>'
      +'<div class="ssub">\u041E\u0442\u0440\u0438\u043C\u0430\u043D\u043E</div><span class="sico">\u25C8</span></div>';
  } else {
    statsHtml+='<div class="sc yellow" style="opacity:.4">'
      +'<div class="slbl">\u0414\u043E\u0445\u0456\u0434 \u043C\u0456\u0441\u044F\u0446\u044F</div>'
      +'<div class="sval">\uD83D\uDD12</div>'
      +'<div class="ssub">\u0422\u0456\u043B\u044C\u043A\u0438 \u0434\u0438\u0440\u0435\u043A\u0442\u043E\u0440</div><span class="sico">\u25C8</span></div>';
  }
  statsHtml+='<div class="sc red">'
    +'<div class="slbl">\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0456\u0432</div>'
    +'<div class="sval">'+S.tutors.length+'</div>'
    +'<div class="ssub">\u0410\u043A\u0442\u0438\u0432\u043D\u0438\u0445</div><span class="sico">\u25C8</span></div>';
  document.getElementById('dash-stats').innerHTML=statsHtml;
}

function renderDashKpi(){
  var offset=S.dashWeekOffset||0;
  var wr=getWeekRange(offset);
  var lbl=document.getElementById('dash-week-lbl');
  if(lbl)lbl.textContent=wr.label;
  var tlbl=document.getElementById('dash-tutor-week-lbl');
  if(tlbl)tlbl.textContent=wr.label;

  var allL=S.lessons;
  var weekL=allL.filter(function(l){return inWeek(l.date,wr);});
  var weekComms=(S.comms||[]).filter(function(c){return inWeek(c.date,wr);});

  var done    = weekL.filter(function(l){return l.status==='done'||l.status==='completed';}).length;
  var missed  = weekL.filter(function(l){return l.status==='missed'||l.status==='absent';}).length;
  var makeup  = weekL.filter(function(l){return l.status==='makeup';}).length;
  var cancelled=weekL.filter(function(l){return l.status==='cancelled';}).length;
  var planned = weekL.filter(function(l){return l.status==='planned'||l.status==='scheduled';}).length;
  var totalComms=weekComms.length;
  var total   = weekL.length;
  var pct     = total>0?Math.round(done/total*100):0;

  var wrPrev=getWeekRange(offset-1);
  var prevL  =allL.filter(function(l){return inWeek(l.date,wrPrev);});
  var prevDone=prevL.filter(function(l){return l.status==='done'||l.status==='completed';}).length;
  var prevMissed=prevL.filter(function(l){return l.status==='missed'||l.status==='absent';}).length;
  var prevComms=(S.comms||[]).filter(function(c){return inWeek(c.date,wrPrev);}).length;
  var prevPct =prevL.length>0?Math.round(prevDone/prevL.length*100):0;

  function trend(cur,prev){
    if(prev===0&&cur===0)return {cls:'same',txt:'\u2014 0'};
    if(prev===0)return {cls:'up',txt:'\u2191 \u043D\u043E\u0432\u0438\u0439'};
    var d=cur-prev;
    return d>0?{cls:'up',txt:'\u2191 +'+d}:d<0?{cls:'down',txt:'\u2193 '+d}:{cls:'same',txt:'= '+cur};
  }

  var kpis=[
    {ico:'\u2705',val:done,     lbl:'\u041F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E \u0437\u0430\u043D\u044F\u0442\u044C', sub:planned+' \u0449\u0435 \u0437\u0430\u043F\u043B\u0430\u043D\u043E\u0432\u0430\u043D\u043E',         accent:'var(--tut)',    tr:trend(done,prevDone)},
    {ico:'\u274C',val:missed,   lbl:'\u041F\u0440\u043E\u043F\u0443\u0449\u0435\u043D\u043E \u0443\u0447\u043D\u044F\u043C\u0438', sub:'\u0421\u043A\u0430\u0441\u043E\u0432\u0430\u043D\u043E: '+cancelled,            accent:'var(--danger)', tr:trend(missed,prevMissed)},
    {ico:'\uD83D\uDCAC',val:totalComms,lbl:'\u041A\u043E\u043C\u0443\u043D\u0456\u043A\u0430\u0446\u0456\u0439',     sub:'\u0414\u0437\u0432\u0456\u043D\u043A\u0438 \u0442\u0430 \u043F\u043E\u0432\u0456\u0434\u043E\u043C\u043B\u0435\u043D\u043D\u044F',          accent:'var(--adm)',    tr:trend(totalComms,prevComms)},
    {ico:'\uD83D\uDCC8',val:pct+'%',  lbl:'\u0412\u0438\u043A\u043E\u043D\u0430\u043D\u043D\u044F \u043F\u043B\u0430\u043D\u0443',  sub:done+' \u0437 '+total+' \u0437\u0430\u043D\u044F\u0442\u044C',         accent:'#a78bfa',      tr:trend(pct,prevPct)},
  ];

  var wkpiEl=document.getElementById('dash-week-kpi');
  if(wkpiEl){
    wkpiEl.innerHTML=kpis.map(function(k){
      return '<div class="kpi-card" style="--kpi-accent:'+k.accent+'">'
        +'<div class="kpi-ico">'+k.ico+'</div>'
        +'<div class="kpi-val">'+k.val+'</div>'
        +'<div class="kpi-lbl">'+k.lbl+'</div>'
        +'<div class="kpi-sub">'+k.sub+'</div>'
        +'<div class="kpi-badge '+k.tr.cls+'">'+k.tr.txt+'</div>'
        +'</div>';
    }).join('');
  }

  // = Per-tutor KPI table =
  var tbody=document.getElementById('dash-tutor-kpi');
  if(!tbody)return;

  var tutors=R()==='tutor'
    ? S.tutors.filter(function(t){return t.accId===CU.id;})
    : S.tutors;

  if(!tutors.length){
    tbody.innerHTML='<tr><td colspan="8" class="empty" style="padding:20px">\u041D\u0435\u043C\u0430\u0454 \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0456\u0432</td></tr>';
    return;
  }

  var maxDone=Math.max.apply(null,tutors.map(function(t){
    return weekL.filter(function(l){return l.tutorId===t.id&&(l.status==='done'||l.status==='completed');}).length;
  }).concat([1]));

  // Summary footer row
  var totalDone=0,totalMissed=0,totalCancel=0,totalPlanned=0,totalTutComms=0,totalStudents=0;

  var rows=tutors.map(function(t){
    var tl=weekL.filter(function(l){return l.tutorId===t.id;});
    var tDone   =tl.filter(function(l){return l.status==='done'||l.status==='completed';}).length;
    var tMissed =tl.filter(function(l){return l.status==='missed'||l.status==='absent';}).length;
    var tCancel =tl.filter(function(l){return l.status==='cancelled';}).length;
    var tPlanned=tl.filter(function(l){return l.status==='planned'||l.status==='scheduled';}).length;
    var tComms  =weekComms.filter(function(c){return c.tutorId===t.id;}).length;
    var tStudents=S.students.filter(function(s){return s.tutorId===t.id&&s.status==='active';}).length;
    var tTotal  =tDone+tMissed; // denominator: only lessons that happened or were missed
    var tPct    =tTotal>0?Math.round(tDone/tTotal*100):tPlanned>0?0:100;
    var barW    =maxDone>0?Math.round(tDone/maxDone*100):0;
    var pctColor=tPct>=80?'var(--tut)':tPct>=50?'var(--dir)':'var(--danger)';

    totalDone+=tDone; totalMissed+=tMissed; totalCancel+=tCancel;
    totalPlanned+=tPlanned; totalTutComms+=tComms; totalStudents+=tStudents;

    // Trend vs prev week
    var prevTl=prevL.filter(function(l){return l.tutorId===t.id;});
    var prevTDone=prevTl.filter(function(l){return l.status==='done'||l.status==='completed';}).length;
    var trendTxt='', trendCls='same';
    var dd=tDone-prevTDone;
    if(dd>0){trendTxt='\u2191+'+dd;trendCls='up';}
    else if(dd<0){trendTxt='\u2193'+dd;trendCls='down';}
    else if(prevTDone>0){trendTxt='='+tDone;trendCls='same';}

    return '<tr>'
      +'<td><div style="display:flex;align-items:center;gap:8px">'+mkAv(t.fn,t.ln,28)
      +'<div><div style="font-weight:600;font-size:13px">'+t.fn+' '+t.ln+'</div>'
      +'<div style="font-size:10px;color:var(--t3)">'+( t.subj||'\u2014')+'</div></div></div></td>'

      +'<td><div style="display:flex;align-items:center;gap:8px">'
      +'<span style="font-weight:700;font-size:18px;font-family:Syne,sans-serif;color:var(--tut)">'+tDone+'</span>'
      +(trendTxt?'<span class="kpi-badge '+trendCls+'" style="font-size:9px">'+trendTxt+'</span>':'')
      +'</div>'
      +'<div class="mini-bar"><div class="mini-fill" style="width:'+barW+'%;background:var(--tut)"></div></div></td>'

      +'<td style="text-align:center">'
      +'<span style="font-weight:600;font-size:15px;color:var(--t2)">'+tPlanned+'</span>'
      +'</td>'

      +'<td style="text-align:center">'
      +'<span style="font-weight:700;font-size:16px;color:'+(tMissed>0?'var(--danger)':'var(--t3)')+'">'+tMissed+'</span>'
      +'</td>'

      +'<td style="text-align:center">'
      +'<span style="font-size:14px;color:'+(tCancel>0?'var(--warn)':'var(--t3)')+'">'+tCancel+'</span>'
      +'</td>'

      +'<td><div style="display:flex;align-items:center;gap:6px;justify-content:center">'
      +'<span style="font-weight:700;font-size:16px;color:var(--adm)">'+tComms+'</span>'

      +'</div></td>'

      +'<td style="text-align:center">'
      +'<span style="font-size:14px">'+tStudents+'</span>'
      +'</td>'

      +'<td>'
      +'<div style="font-weight:700;font-size:15px;color:'+pctColor+'">'+tPct+'%</div>'
      +'<div style="font-size:10px;color:var(--t3)">'+tDone+' / '+(tDone+tMissed)+'</div>'
      +'</td>'
      +'</tr>';
  }).join('');

  // Total row
  var totalEffective=totalDone+totalMissed;
  var totalPct=totalEffective>0?Math.round(totalDone/totalEffective*100):0;
  var totalPctColor=totalPct>=80?'var(--tut)':totalPct>=50?'var(--dir)':'var(--danger)';
  rows+='<tr style="background:rgba(255,255,255,.03);font-weight:700;border-top:2px solid var(--b1)">'
    +'<td><span style="font-size:12px;color:var(--t2);letter-spacing:.5px">\u0420\u0410\u0417\u041E\u041C / \u0421\u0415\u0420\u0415\u0414\u041D\u0404</span></td>'
    +'<td><span style="font-size:18px;font-family:Syne,sans-serif;color:var(--tut)">'+totalDone+'</span></td>'
    +'<td style="text-align:center;color:var(--t2)">'+totalPlanned+'</td>'
    +'<td style="text-align:center;color:'+(totalMissed>0?'var(--danger)':'var(--t3)')+'">'+totalMissed+'</td>'
    +'<td style="text-align:center;color:var(--warn)">'+totalCancel+'</td>'
    +'<td style="text-align:center;color:var(--adm)">'+totalTutComms+'</td>'
    +'<td style="text-align:center">'+totalStudents+'</td>'
    +'<td><span style="font-weight:700;color:'+totalPctColor+'">'+totalPct+'%</span></td>'
    +'</tr>';

  tbody.innerHTML=rows;
}

function renderDashTrends(){
  var offset=S.dashWeekOffset||0;
  var weeks=[];
  for(var i=3;i>=0;i--){
    var wr=getWeekRange(offset-i);
    var weekL=S.lessons.filter(function(l){return inWeek(l.date,wr);});
    var weekComms=(S.comms||[]).filter(function(c){return inWeek(c.date,wr);});
    weeks.push({
      wr:wr,
      done:weekL.filter(function(l){return l.status==='done'||l.status==='completed';}).length,
      missed:weekL.filter(function(l){return l.status==='missed'||l.status==='absent';}).length,
      planned:weekL.filter(function(l){return l.status==='planned'||l.status==='scheduled';}).length,
      comms:weekComms.length,
    });
  }

  function miniChart(containerId,data,colorFn,keyFn,unit){
    var el=document.getElementById(containerId);
    if(!el)return;
    var max=Math.max.apply(null,data.map(keyFn).concat([1]));
    var tutors=R()==='tutor'
      ? S.tutors.filter(function(t){return t.accId===CU.id;})
      : S.tutors;

    // Global bars
    var barsHtml='';
    data.forEach(function(w,i){
      var val=keyFn(w);
      var h=Math.max(4,Math.round(val/max*70));
      var isActive=(i===data.length-1);
      barsHtml+='<div class="trend-bar-wrap">'
        +'<div class="trend-val">'+val+(unit||'')+'</div>'
        +'<div class="trend-bar" style="height:'+h+'px;background:'+(isActive?colorFn:colorFn.replace(')',', .5)').replace('var(','rgba('))+'"></div>'
        +'<div class="trend-lbl">'+w.wr.mon.toLocaleDateString('uk-UA',{day:'2-digit',month:'2-digit'})+'</div>'
        +'</div>';
    });

    // Per-tutor trend lines (last 4 weeks)
    var tutorRows='';
    tutors.slice(0,6).forEach(function(t){
      var vals=data.map(function(w){
        var wl=S.lessons.filter(function(l){return inWeek(l.date,w.wr)&&(l.tutor_id===t.id||l.tutorId===t.id);});
        if(containerId==='dash-trend-comms'){
          return (S.comms||[]).filter(function(c){return inWeek(c.date,w.wr)&&(c.tutor_id===t.id||c.tutorId===t.id);}).length;
        }
        return wl.filter(function(l){return l.status==='done'||l.status==='completed';}).length;
      });
      var tutMax=Math.max.apply(null,vals.concat([1]));
      var segs=vals.map(function(v,i){
        var h=Math.max(3,Math.round(v/tutMax*28));
        var isLast=(i===vals.length-1);
        return '<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:2px">'
          +'<div style="font-size:9px;color:var(--t3);font-family:JetBrains Mono,monospace">'+v+'</div>'
          +'<div style="width:100%;height:'+h+'px;border-radius:3px 3px 0 0;background:'+(isLast?colorFn:'var(--b2)')+'"></div>'
          +'</div>';
      }).join('');
      tutorRows+='<div style="display:flex;align-items:center;gap:8px;margin-top:6px;padding-top:6px;border-top:1px solid rgba(255,255,255,.04)">'
        +mkAv(t.fn,t.ln,20)
        +'<span style="font-size:11px;color:var(--t2);width:80px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+t.fn+' '+t.ln+'</span>'
        +'<div style="flex:1;display:flex;align-items:flex-end;gap:4px;height:36px">'+segs+'</div>'
        +'<span style="font-size:13px;font-weight:700;font-family:JetBrains Mono,monospace;color:var(--t1);width:24px;text-align:right">'+vals[vals.length-1]+'</span>'
        +'</div>';
    });

    el.innerHTML='<div style="display:flex;flex-direction:column">'
      +'<div class="trend-chart">'+barsHtml+'</div>'
      +tutorRows
      +'</div>';
  }

  miniChart('dash-trend-lessons',weeks,'var(--tut)',function(w){return w.done;},'');
  miniChart('dash-trend-comms',weeks,'var(--adm)',function(w){return w.comms;},'');
}

// \u2500\u2500 Bottom row \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
function renderDashBottom(){
  var now=new Date();
  var ml=myLessons();

  // Upcoming lessons
  var up=[].concat(ml).filter(function(l){
    return new Date(l.date+'T'+(l.time||'00:00'))>=now&&l.status!=='cancelled';
  }).sort(function(a,b){
    return new Date(a.date+'T'+a.time)-new Date(b.date+'T'+b.time);
  }).slice(0,6);
  document.getElementById('dt-lessons').innerHTML=up.length
    ?up.map(function(l){
      return '<tr><td>'+sn(l.studentId)+'</td><td>'+l.subject+'</td>'
        +'<td style="font-family:JetBrains Mono,monospace;font-size:11px">'+fd(l.date)+' '+(l.time||'')+'</td>'
        +'<td>'+bst(l.status)+'</td></tr>';
    }).join('')
    :'<tr><td colspan="4" class="empty" style="padding:20px">\u0417\u0430\u043D\u044F\u0442\u044C \u043D\u0435 \u0437\u0430\u043F\u043B\u0430\u043D\u043E\u0432\u0430\u043D\u043E</td></tr>';

  // Right panel
  var rt=document.getElementById('dash-rt');
  var rb=document.getElementById('dash-rb');
  if(P().seeIncome && R()!=='tutor'){
    if(rt)rt.textContent='\u041E\u0441\u0442\u0430\u043D\u043D\u0456 \u043F\u043B\u0430\u0442\u0435\u0436\u0456';
    var rec=[].concat(S.payments).sort(function(a,b){return new Date(b.date)-new Date(a.date);}).slice(0,6);
    if(rb)rb.innerHTML=rec.length
      ?'<table><thead><tr><th>\u0423\u0447\u0435\u043D\u044C</th><th>\u0421\u0443\u043C\u0430</th><th>\u0414\u0430\u0442\u0430</th><th>\u0421\u0442\u0430\u0442\u0443\u0441</th></tr></thead><tbody>'
        +rec.map(function(p){
          return '<tr><td>'+sn(p.studentId)+'</td>'
            +'<td style="font-family:JetBrains Mono,monospace">'+(p.amount||0).toLocaleString('uk-UA')+'\u20B4</td>'
            +'<td style="font-size:11px">'+fd(p.date)+'</td>'
            +'<td>'+bst(p.status)+'</td></tr>';
        }).join('')+'</tbody></table>'
      :'<div class="empty" style="padding:20px"><div class="ei">\uD83D\uDCB3</div>\u041F\u043B\u0430\u0442\u0435\u0436\u0456\u0432 \u043D\u0435\u043C\u0430\u0454</div>';
  } else {
    if(rt)rt.textContent='\u041C\u043E\u0457 \u0443\u0447\u043D\u0456';
    var msArr=myStudents();
    if(rb)rb.innerHTML=msArr.length
      ?'<table><thead><tr><th>\u0406\u043C\'\u044F</th><th>\u041F\u0440\u0435\u0434\u043C\u0435\u0442</th><th>\u0421\u0442\u0430\u0442\u0443\u0441</th></tr></thead><tbody>'
        +msArr.map(function(s){
          return '<tr><td>'+s.fn+' '+s.ln+'</td><td>'+(s.subject||'\u2014')+'</td><td>'+bst(s.status)+'</td></tr>';
        }).join('')+'</tbody></table>'
      :'<div class="empty" style="padding:20px"><div class="ei">\uD83D\uDC65</div>\u0423\u0447\u043D\u0456\u0432 \u043D\u0435 \u043F\u0440\u0438\u0437\u043D\u0430\u0447\u0435\u043D\u043E</div>';
  }

  // Subjects chart
  var sc={};ml.forEach(function(l){sc[l.subject]=(sc[l.subject]||0)+1;});
  var colors=['var(--adm)','var(--tut)','var(--dir)','var(--god2)','#a78bfa','#0ea5e9'];
  var maxS=Math.max.apply(null,Object.values(sc).concat([1]));
  document.getElementById('dash-subj').innerHTML=Object.entries(sc)
    .sort(function(a,b){return b[1]-a[1];}).slice(0,6)
    .map(function(e,i){
      return '<div style="margin-bottom:9px">'
        +'<div style="display:flex;justify-content:space-between;margin-bottom:3px">'
        +'<span style="font-size:12px">'+e[0]+'</span>'
        +'<span style="font-size:11px;color:var(--t2);font-family:JetBrains Mono,monospace">'+e[1]+'</span></div>'
        +'<div class="pb"><div class="pf" style="width:'+Math.round(e[1]/maxS*100)+'%;background:'+colors[i%colors.length]+'"></div></div>'
        +'</div>';
    }).join('')||'<div class="empty"><div class="ei">\uD83D\uDCCA</div>\u041D\u0435\u043C\u0430\u0454 \u0434\u0430\u043D\u0438\u0445</div>';

  // Payments status
  var paid=S.payments.filter(function(p){return p.status==='paid';}).reduce(function(a,p){return a+p.amount;},0);
  var pend=S.payments.filter(function(p){return p.status==='pending';}).reduce(function(a,p){return a+p.amount;},0);
  var over=S.payments.filter(function(p){return p.status==='overdue';}).reduce(function(a,p){return a+p.amount;},0);
  document.getElementById('dash-pay').innerHTML=(P().seeIncome && R()!=='tutor')
    ?'<div class="ms"><span class="msl">\u2705 \u041E\u043F\u043B\u0430\u0447\u0435\u043D\u043E</span><span class="msv" style="color:var(--tut)">'+paid.toLocaleString('uk-UA')+'\u20B4</span></div>'
    +'<div class="ms"><span class="msl">\u23F3 \u041E\u0447\u0456\u043A\u0443\u0454\u0442\u044C\u0441\u044F</span><span class="msv" style="color:var(--dir)">'+pend.toLocaleString('uk-UA')+'\u20B4</span></div>'
    +'<div class="ms"><span class="msl">\u26A0\uFE0F \u041F\u0440\u043E\u0441\u0442\u0440\u043E\u0447\u0435\u043D\u043E</span><span class="msv" style="color:var(--danger)">'+over.toLocaleString('uk-UA')+'\u20B4</span></div>'
    +'<div class="ms"><span class="msl">\u0412\u0441\u044C\u043E\u0433\u043E \u043F\u043B\u0430\u0442\u0435\u0436\u0456\u0432</span><span class="msv">'+S.payments.length+'</span></div>'
    :(R()==='tutor'?'':'<div class="empty"><div class="ei">\uD83D\uDD12</div>\u0414\u043E\u0441\u0442\u0443\u043F\u043D\u043E \u0434\u0438\u0440\u0435\u043A\u0442\u043E\u0440\u0443 \u0442\u0430 \u0430\u0434\u043C\u0456\u043D\u0456\u0441\u0442\u0440\u0430\u0442\u043E\u0440\u0443</div>');
  // Hide payment cards entirely for tutors
  var rbCard = document.getElementById('dash-rb-card');
  var payCard = document.getElementById('dash-pay-card');
  var isTutor = R()==='tutor';
  if(rbCard)  rbCard.style.display  = isTutor ? 'none' : '';
  if(payCard) payCard.style.display = isTutor ? 'none' : '';

  // Hide old comm block - comms now shown inline
  var cb=document.getElementById('dash-comm-block');
  if(cb)cb.style.display='none';
  // Hide payments block for tutors
  var pyBlock=document.getElementById('dash-pay');
  if(pyBlock&&R()==='tutor') pyBlock.closest('.card') && (pyBlock.closest('.card').style.display='none');
}

function renderCommLog(){
  var el  = document.getElementById('dash-comm-log');
  var el2 = document.getElementById('dash-comm-log2');
  if(!el && !el2) return;
  var comms=[].concat(S.comms||[]).sort(function(a,b){return (b.date||'').localeCompare(a.date||'');}).slice(0,20);
  var typeIco={call:'📞',message:'💬',meeting:'🤝',email:'📧',other:'📋',msg:'💬',meet:'🤝'};
  var html;
  if(!comms.length){
    html='<div class="empty" style="padding:20px"><div class="ei">💬</div>Комунікацій ще не записано</div>';
  } else {
    html=comms.map(function(c){
      var tutor=S.tutors.find(function(t){return t.id===c.tutorId;})||{fn:'',ln:''};
      var student=c.studentId?S.students.find(function(s){return s.id===c.studentId;}):null;
      return '<div class="comm-item">'        +'<div class="comm-ico">'+(typeIco[c.type]||'📋')+'</div>'        +'<div class="comm-body">'        +'<div class="comm-meta">'        +'<span class="comm-type">'+(c.type||'інше')+'</span>'        +'<span class="comm-tutor">'+tutor.fn+' '+tutor.ln+'</span>'        +(student?'<span style="font-size:11px;color:var(--t3)">→ '+student.fn+' '+student.ln+'</span>':'')
        +'<span class="comm-date">'+fd((c.date||'').slice(0,10))+'</span>'        +'</div>'        +'<div class="comm-note">'+(c.note||'—')+'</div>'        +'</div>'        +'<button onclick="delComm(this.dataset.id)" data-id="'+c.id+'" style="background:none;border:none;color:var(--t3);cursor:pointer;font-size:14px;flex-shrink:0">&times;</button>'        +'</div>';
    }).join('');
  }
  if(el)  el.innerHTML  = html;
  if(el2) el2.innerHTML = html;
}

function sfilt(f,el){sfCur=f;document.querySelectorAll('#sfchips .chip').forEach(c=>c.classList.remove('active'));el.classList.add('active');renderStudents();}

function renderStudents(){
  var data=myStudents();
  if(sfCur!=='all') data=data.filter(function(s){return s.status===sfCur;});
  var tot=document.getElementById('st-total');
  if(tot) tot.textContent=data.length+' \u0437 '+myStudents().length;
  var ce=can('students');
  var html=data.length?data.map(function(s){
    var btns=ce
      ?('<button class="btn btn-g btn-sm" onclick="openStudM(this.dataset.id)" data-id="'+s.id+'">\u270F\uFE0F</button>'
        +'<button class="btn btn-sm" style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger)" onclick="delStudent(this.dataset.id)" data-id="'+s.id+'">\uD83D\uDDD1</button>')
      :'<span style="font-size:10px;color:var(--t3)">\u043F\u0435\u0440\u0435\u0433\u043B\u044F\u0434</span>';
    return '<tr>'
      +'<td><div style="display:flex;align-items:center;gap:8px">'+mkAv(s.fn,s.ln)+'<div><div style="font-weight:600;font-size:13px">'+s.fn+' '+s.ln+'</div></div></div></td>'
      +'<td style="font-size:12px;color:var(--t2)">'+(s.age||'\u2014')+' / '+(s.grade||'\u2014')+'</td>'
      +'<td>'+(s.subject||'\u2014')+'</td>'
      +'<td>'+(s.tutorId?tn(s.tutorId):'\u2014')+'</td>'
      +'<td>'+bst(s.status)+'</td>'
      +'<td style="font-size:12px;color:var(--t2)">'+(s.parentPhone||s.phone||s.email||'\u2014')+'</td>'
      +'<td><div style="display:flex;gap:3px">'+btns+'</div></td>'
      +'</tr>';
  }).join(''):'<tr><td colspan="7"><div class="empty"><div class="ei">\uD83D\uDC65</div>\u0423\u0447\u043D\u0456\u0432 \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E</div></td></tr>';
  document.getElementById('st-table').innerHTML=html;
}
function renderLessons(){
  var sf=document.getElementById('lf-subj'), stf=document.getElementById('lf-stat');
  var cv=sf&&sf.value||'', sv=stf&&stf.value||'';
  if(sf){
    var c=sf.value;
    sf.innerHTML='<option value="">\u0412\u0441\u0456 \u043F\u0440\u0435\u0434\u043C\u0435\u0442\u0438</option>'
      +S.subjects.map(function(s){return '<option value="'+s.name+'">'+s.name+'</option>';}).join('');
    sf.value=c;
  }
  var data=[].concat(myLessons()).sort(function(a,b){return new Date(b.date+'T'+b.time)-new Date(a.date+'T'+a.time);});
  if(cv) data=data.filter(function(l){return l.subject===cv;});
  if(sv) data=data.filter(function(l){return l.status===sv;});
  var ce=can('lessons');
  var ri=function(l){return l.recurId?'<span title="\u041F\u043E\u0432\u0442\u043E\u0440\u044E\u0432\u0430\u043D\u0435" style="color:var(--adm);font-size:10px">\uD83D\uDD01</span>':'';};
  var html=data.length?data.map(function(l){
    var btns=ce
      ?('<button class="btn btn-g btn-sm" onclick="openLessM(this.dataset.id)" data-id="'+l.id+'">\u270F\uFE0F</button>'
        +'<button class="btn btn-sm" style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger)" onclick="delLesson(this.dataset.id)" data-id="'+l.id+'">\uD83D\uDDD1</button>')
      :'<span style="font-size:10px;color:var(--t3)">\u043F\u0435\u0440\u0435\u0433\u043B\u044F\u0434</span>';
    return '<tr>'
      +'<td>'+sn(l.studentId)+'</td>'
      +'<td>'+l.subject+' '+ri(l)+'</td>'
      +'<td>'+(l.tutorId?tn(l.tutorId):'\u2014')+'</td>'
      +'<td style="font-family:JetBrains Mono,monospace;font-size:11px">'+fd(l.date)+' '+(l.time||'')+'</td>'
      +'<td>'+(l.dur||60)+' \u0445\u0432</td>'
      +'<td style="font-size:12px;color:var(--t2);max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+(l.notes||'\u2014')+'</td>'
      +'<td>'+bst(l.status)+'</td>'
      +'<td><div style="display:flex;gap:3px">'+btns+'</div></td>'
      +'</tr>';
  }).join(''):'<tr><td colspan="8"><div class="empty"><div class="ei">\uD83D\uDCDA</div>\u0417\u0430\u043D\u044F\u0442\u044C \u043D\u0435\u043C\u0430\u0454</div></td></tr>';
  document.getElementById('lt-table').innerHTML=html;
}
function renderPayments(){
  var paid=S.payments.filter(function(p){return p.status==='paid';}).reduce(function(a,p){return a+p.amount;},0);
  var pend=S.payments.filter(function(p){return p.status==='pending';}).reduce(function(a,p){return a+p.amount;},0);
  var over=S.payments.filter(function(p){return p.status==='overdue';}).reduce(function(a,p){return a+p.amount;},0);
  document.getElementById('py-paid').textContent=paid.toLocaleString('uk-UA')+'\u20B4';
  document.getElementById('py-pend').textContent=pend.toLocaleString('uk-UA')+'\u20B4';
  document.getElementById('py-over').textContent=over.toLocaleString('uk-UA')+'\u20B4';
  var mm={cash:'\u0413\u043E\u0442\u0456\u0432\u043A\u0430',card:'\u041A\u0430\u0440\u0442\u043A\u0430',transfer:'\u041F\u0435\u0440\u0435\u043A\u0430\u0437'};
  var ce=can('payments');
  var data=[].concat(S.payments).sort(function(a,b){return new Date(b.date)-new Date(a.date);});
  var html=data.length?data.map(function(p){
    var btns=ce
      ?('<button class="btn btn-g btn-sm" onclick="openPayM(this.dataset.id)" data-id="'+p.id+'">\u270F\uFE0F</button>'
        +'<button class="btn btn-sm" style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger)" onclick="delPay(this.dataset.id)" data-id="'+p.id+'">\uD83D\uDDD1</button>')
      :'\u2014';
    return '<tr>'
      +'<td>'+sn(p.studentId)+'</td>'
      +'<td style="font-family:JetBrains Mono,monospace">'+((p.amount||0).toLocaleString('uk-UA'))+'\u20B4</td>'
      +'<td>'+(mm[p.method]||p.method)+'</td>'
      +'<td style="font-size:11px">'+fd(p.date)+'</td>'
      +'<td style="font-size:12px">'+(p.month||'\u2014')+'</td>'
      +'<td style="font-size:12px;color:var(--t2)">'+(p.note||'\u2014')+'</td>'
      +'<td>'+bst(p.status)+'</td>'
      +'<td><div style="display:flex;gap:3px">'+btns+'</div></td>'
      +'</tr>';
  }).join(''):'<tr><td colspan="8"><div class="empty"><div class="ei">\uD83D\uDCB3</div>\u041F\u043B\u0430\u0442\u0435\u0436\u0456\u0432 \u043D\u0435\u043C\u0430\u0454</div></td></tr>';
  document.getElementById('pt-table').innerHTML=html;
}
function renderCustomPage(pageId){
  var pel=document.getElementById('pg-'+pageId);
  if(!pel){
    pel=document.createElement('div');
    pel.className='page';
    pel.id='pg-'+pageId;
    document.getElementById('content').appendChild(pel);
  }
  var cfg=(S.godConfig)||{};
  var navItems=cfg.navItems||[].concat(NAV_CFG);
  var pageInfo=navItems.find(function(n){return n.id===pageId;})||{lbl:'\u0412\u043B\u0430\u0441\u043D\u0430 \u0441\u0442\u043E\u0440\u0456\u043D\u043A\u0430',ico:'\u2B50'};
  var notes=(cfg.customPageNotes||{})[pageId]||'';
  var saveBtn=R()==='god'
    ?('<button class="btn btn-p btn-sm" style="margin-top:8px" onclick="saveCustomPageNotes(this.dataset.pid)" data-pid="'+pageId+'">\uD83D\uDCBE \u0417\u0431\u0435\u0440\u0435\u0433\u0442\u0438 \u043D\u043E\u0442\u0430\u0442\u043A\u0438</button>')
    :'';
  pel.innerHTML=''
    +'<div class="god-banner" style="margin-bottom:16px">'
      +'<span class="god-banner-icon">'+pageInfo.ico+'</span>'
      +'<div><div class="god-banner-title">'+pageInfo.lbl+'</div>'
      +'<div class="god-banner-text">\u0412\u043B\u0430\u0441\u043D\u0430 \u0441\u0442\u043E\u0440\u0456\u043D\u043A\u0430, \u0441\u0442\u0432\u043E\u0440\u0435\u043D\u0430 \u0447\u0435\u0440\u0435\u0437 \u041a\u043e\u043d\u0441\u0442\u0440\u0443\u043a\u0442\u043e\u0440 \u0456\u043d\u0442\u0435\u0440\u0444\u0435\u0439\u0441\u0443</div></div>'
    +'</div>'
    +'<div class="card">'
      +'<div class="ch"><span class="ct">\u0412\u043c\u0456\u0441\u0442 \u0441\u0442\u043e\u0440\u0456\u043d\u043a\u0438 \u00ab'+pageInfo.lbl+'\u00bb</span></div>'
      +'<div style="padding:24px">'
        +'<div style="margin-bottom:14px">'
          +'<label style="font-size:11px;color:var(--t2);font-weight:600;text-transform:uppercase;letter-spacing:.4px">\u041d\u043e\u0442\u0430\u0442\u043a\u0438 / \u043e\u043f\u0438\u0441 \u0441\u0442\u043e\u0440\u0456\u043d\u043a\u0438</label>'
          +'<textarea id="custom-page-notes-'+pageId+'" style="width:100%;margin-top:6px;min-height:120px;font-size:13px" placeholder="\u0414\u043e\u0434\u0430\u0439\u0442\u0435 \u043e\u043f\u0438\u0441 \u0430\u0431\u043e \u043d\u043e\u0442\u0430\u0442\u043a\u0438...">'+notes+'</textarea>'
          +saveBtn
        +'</div>'
        +'<div style="margin-top:20px;padding:16px;background:var(--s2);border-radius:10px;border:1px dashed var(--b2)">'
          +'<div style="font-size:12px;color:var(--t3);text-align:center">'
            +'<div style="font-size:24px;margin-bottom:8px">\uD83D\uDEA7</div>'
            +'\u0426\u044f \u0441\u0442\u043e\u0440\u0456\u043d\u043a\u0430 \u043f\u043e\u0440\u043e\u0436\u043d\u044f.'
          +'</div>'
        +'</div>'
      +'</div>'
    +'</div>';
  pel.classList.add('active');
}

function saveCustomPageNotes(pageId){
  var cfg=(S.godConfig)||{};
  if(!cfg.customPageNotes) cfg.customPageNotes={};
  var el=document.getElementById('custom-page-notes-'+pageId);
  cfg.customPageNotes[pageId]=el?el.value:'';
  gcSave('customPageNotes',cfg.customPageNotes);
  mkToast('\u041d\u043e\u0442\u0430\u0442\u043a\u0438 \u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043d\u043e');
}

function gcGetConfig(){if(!S.godConfig)S.godConfig={};return S.godConfig;}

function gcSet(key,val){
  if(!S.godConfig)S.godConfig={};
  if(val===null||val===undefined)delete S.godConfig[key];
  else S.godConfig[key]=val;
  // Demo communications
  if(!S.comms||!S.comms.length){
    S.comms=[
      {id:'cm1',tutorId:'t1',studentId:'s1',date:localDateStr(new Date()),type:'call',note:'\u041E\u0431\u0433\u043E\u0432\u043E\u0440\u0438\u043B\u0438 \u043F\u043B\u0430\u043D \u0437\u0430\u043D\u044F\u0442\u044C \u043D\u0430 \u043C\u0456\u0441\u044F\u0446\u044C',createdAt:new Date().toISOString()},
      {id:'cm2',tutorId:'t2',studentId:'s4',date:localDateStr(new Date()),type:'msg',note:'\u041D\u0430\u0433\u0430\u0434\u0443\u0432\u0430\u043D\u043D\u044F \u043F\u0440\u043E \u0434\u043E\u043C\u0430\u0448\u043D\u0454 \u0437\u0430\u0432\u0434\u0430\u043D\u043D\u044F',createdAt:new Date().toISOString()},
      {id:'cm3',tutorId:'t3',studentId:'s6',date:localDateStr(new Date()),type:'meeting',note:'\u0411\u0430\u0442\u044C\u043A\u0456\u0432\u0441\u044C\u043A\u0456 \u0437\u0431\u043E\u0440\u0438',createdAt:new Date().toISOString()},
    ];
  }
  saveS();
}

function gcTab(id,el){
  document.querySelectorAll('.gc-tab').forEach(function(t){t.classList.remove('active');});
  document.querySelectorAll('.gc-panel').forEach(function(p){p.classList.remove('active');});
  el.classList.add('active');
  document.getElementById('gcp-'+id).classList.add('active');
}

function renderConstructor(){
  if(R()!=='god'){mkToast('\u0422\u0456\u043B\u044C\u043A\u0438 \u0411\u043E\u0433 \u0441\u0438\u0441\u0442\u0435\u043C\u0438','error');return;}
  gcRenderRoles();
  gcRenderNav();
  gcRenderFields();
  gcRenderLabels();
}

function gcRenderRoles(){
  var cfg=gcGetConfig();
  var sp=cfg.perms||{};
  var rl=[
    {role:'god',ico:'\u26A1',lbl:'\u0411\u043E\u0433 \u0441\u0438\u0441\u0442\u0435\u043C\u0438',clr:'var(--god2)',locked:true},
    {role:'director',ico:'\uD83D\uDC51',lbl:'\u0414\u0438\u0440\u0435\u043A\u0442\u043E\u0440',clr:'var(--dir)',locked:false},
    {role:'admin',ico:'\uD83D\uDEE1',lbl:'\u0410\u0434\u043C\u0456\u043D\u0456\u0441\u0442\u0440\u0430\u0442\u043E\u0440',clr:'var(--adm)',locked:false},
    {role:'tutor',ico:'\uD83D\uDCDA',lbl:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440',clr:'var(--tut)',locked:false},
  ];
  var cont=document.getElementById('gc-role-editor');
  cont.innerHTML='';
  var grid=document.createElement('div');grid.className='role-editor';
  rl.forEach(function(ri){
    var rp=Object.assign({},DEFAULT_PERMS[ri.role],sp[ri.role]||{});
    var card=document.createElement('div');card.className='role-card';
    var hd=document.createElement('div');hd.className='role-card-head';
    var s1=document.createElement('span');s1.textContent=ri.ico;
    var s2=document.createElement('span');
    s2.style.cssText='font-weight:700;font-size:13px;color:'+ri.clr;
    s2.textContent=ri.lbl;
    hd.appendChild(s1);hd.appendChild(s2);
    if(ri.locked){var s3=document.createElement('span');s3.style.cssText='font-size:10px;color:var(--t3);margin-left:auto';s3.textContent='\u043D\u0435\u0437\u043C\u0456\u043D\u043D\u0430';hd.appendChild(s3);}
    card.appendChild(hd);
    var bd=document.createElement('div');bd.className='role-card-body';
    Object.keys(PERM_LABELS).forEach(function(key){
      var pr=document.createElement('div');pr.className='perm-row';
      var pl=document.createElement('span');pl.className='perm-label';pl.textContent=PERM_LABELS[key];
      var tl=document.createElement('label');tl.className='toggle';
      var cb=document.createElement('input');cb.type='checkbox';cb.checked=!!rp[key];if(ri.locked)cb.disabled=true;
      (function(role,k){cb.addEventListener('change',function(){gcLivePermChange(role,k,this.checked);});})(ri.role,key);
      var sl=document.createElement('span');sl.className='toggle-slider';
      tl.appendChild(cb);tl.appendChild(sl);
      pr.appendChild(pl);pr.appendChild(tl);bd.appendChild(pr);
    });
    card.appendChild(bd);grid.appendChild(card);
  });
  cont.appendChild(grid);
  var bar=document.createElement('div');bar.className='gc-save-bar';
  bar.innerHTML='<span style="font-size:12px;color:var(--tut)">&#10003; \u0417\u043C\u0456\u043D\u0438 \u043C\u0438\u0442\u0442\u0454\u0432\u0456</span>'
    +'<button class="btn btn-g btn-sm" style="margin-left:auto" onclick="gcResetRoles()">&#8635; \u0421\u043A\u0438\u043D\u0443\u0442\u0438</button>';
  cont.appendChild(bar);
}

function gcLivePermChange(role,key,val){
  // 1. Save to config
  var cfg=gcGetConfig();
  if(!cfg.perms)cfg.perms={};
  if(!cfg.perms[role])cfg.perms[role]={};
  cfg.perms[role][key]=val;
  gcSet('perms',cfg.perms);
  // 2. Apply live to ROLES object
  if(ROLES[role]){
    ROLES[role].can[key]=val;
    if(key==='seeIncome')ROLES[role].seeIncome=val;
    if(key==='seeAll')ROLES[role].seeAll=val;
  }
  // 3. Refresh current view if needed
  var pg=S.currentPage;
  if(pg==='dashboard')renderDash();
  mkToast(role+': '+PERM_LABELS[key]+' \u2192 '+(val?'\u2705':'\u274C'));
}

function gcResetRoles(){
  if(!confirm('\u0421\u043A\u0438\u043D\u0443\u0442\u0438 \u0432\u0441\u0456 \u043F\u0440\u0430\u0432\u0430 \u0434\u043E \u0441\u0442\u0430\u043D\u0434\u0430\u0440\u0442\u043D\u0438\u0445?'))return;
  gcSet('perms',null);
  // Restore ROLES
  ['director','admin','tutor'].forEach(function(role){
    ROLES[role].can=Object.assign({},DEFAULT_PERMS[role]);
    ROLES[role].seeIncome=DEFAULT_PERMS[role].seeIncome;
    ROLES[role].seeAll=DEFAULT_PERMS[role].seeAll;
  });
  gcRenderRoles();
  mkToast('\u041F\u0440\u0430\u0432\u0430 \u0441\u043A\u0438\u043D\u0443\u0442\u043E \u0434\u043E \u0441\u0442\u0430\u043D\u0434\u0430\u0440\u0442\u043D\u0438\u0445');
}

function gcGetNavItems(){
  var cfg=gcGetConfig();
  if(cfg.navItems&&cfg.navItems.length)return cfg.navItems;
  return DEFAULT_NAV_CFG.map(function(n){return Object.assign({},n);});
}

function gcRenderNav(){
  var ni=gcGetNavItems();
  var rk=['god','director','admin','tutor'];
  var rico={god:'\u26A1',director:'\uD83D\uDC51',admin:'\uD83D\uDEE1',tutor:'\uD83D\uDCDA'};
  var el=document.getElementById('gc-nav-editor');
  var wrap=document.createElement('div');wrap.className='nav-editor';
  ni.forEach(function(n,i){
    var rArr=n.roles||rk;
    var row=document.createElement('div');row.className='nav-edit-row';row.draggable=true;
    (function(idx){
      row.addEventListener('dragstart',function(e){gcDragStart(e,idx);});
      row.addEventListener('dragover',function(e){gcDragOver(e,idx);});
      row.addEventListener('drop',function(e){gcDrop(e,idx);});
      row.addEventListener('dragleave',function(){gcDragLeave();});
    })(i);
    var dh=document.createElement('span');dh.className='drag-handle';dh.innerHTML='&#8283;';row.appendChild(dh);
    var icoI=document.createElement('input');icoI.type='text';icoI.value=n.ico||'';
    icoI.style.cssText='width:44px;font-size:16px;text-align:center;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t1);padding:4px 6px';
    (function(idx){icoI.addEventListener('input',function(){gcLiveNavChange(idx,'ico',this.value);});})(i);
    row.appendChild(icoI);
    var lblI=document.createElement('input');lblI.type='text';lblI.value=n.lbl||'';
    lblI.style.cssText='flex:1;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t1);padding:5px 8px;font-size:13px';
    (function(idx){lblI.addEventListener('input',function(){gcLiveNavChange(idx,'lbl',this.value);});})(i);
    row.appendChild(lblI);
    var secI=document.createElement('input');secI.type='text';secI.value=n.sec||'';
    secI.style.cssText='width:95px;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t2);padding:5px 8px;font-size:11px';
    (function(idx){secI.addEventListener('input',function(){gcLiveNavChange(idx,'sec',this.value);});})(i);
    row.appendChild(secI);
    var cbWrap=document.createElement('div');cbWrap.className='nav-vis-checkboxes';
    rk.forEach(function(r){
      var lbl=document.createElement('label');lbl.className='nav-vis-cb';lbl.title=r;
      var cb=document.createElement('input');cb.type='checkbox';cb.checked=rArr.includes(r);
      (function(idx,role){cb.addEventListener('change',function(){gcLiveNavRole(idx,role,this.checked);});})(i,r);
      var ico=document.createElement('span');ico.textContent=rico[r]||r;
      lbl.appendChild(cb);lbl.appendChild(ico);cbWrap.appendChild(lbl);
    });
    row.appendChild(cbWrap);
    if(n.custom){
      var del=document.createElement('button');del.className='btn btn-sm';
      del.style.cssText='background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger);padding:4px 8px';
      del.innerHTML='&times;';
      (function(idx){del.addEventListener('click',function(){gcDelNavItem(idx);});})(i);
      row.appendChild(del);
    }
    wrap.appendChild(row);
  });
  el.innerHTML='';el.appendChild(wrap);
  var bar=document.createElement('div');bar.className='gc-save-bar';
  bar.innerHTML='<span style="font-size:12px;color:var(--tut)">&#10003; \u0417\u043C\u0456\u043D\u0438 \u043C\u0438\u0442\u0442\u0454\u0432\u0456</span>'
    +'<button class="btn btn-g btn-sm" style="margin-left:auto" onclick="gcResetNav()">&#8635; \u0421\u043A\u0438\u043D\u0443\u0442\u0438</button>';
  el.appendChild(bar);
}

function gcEsc(str){return (str||'').replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;');}

function gcLiveNavChange(idx,key,val){
  var items=gcGetNavItems();
  items[idx][key]=val;
  gcSet('navItems',items);
  buildSidebar();
  // Highlight current active
  var nel=document.getElementById('ni-'+S.currentPage);
  if(nel)nel.classList.add('active');
}

function gcLiveNavRole(idx,role,checked){
  var items=gcGetNavItems();
  if(!items[idx].roles)items[idx].roles=['god','director','admin','tutor'];
  if(checked){if(!items[idx].roles.includes(role))items[idx].roles.push(role);}
  else items[idx].roles=items[idx].roles.filter(function(r){return r!==role;});
  gcSet('navItems',items);
  buildSidebar();
  var nel=document.getElementById('ni-'+S.currentPage);
  if(nel)nel.classList.add('active');
}

function gcDelNavItem(idx){
  var items=gcGetNavItems();
  if(!items[idx].custom){mkToast('\u0421\u0438\u0441\u0442\u0435\u043C\u043D\u0456 \u0432\u043A\u043B\u0430\u0434\u043A\u0438 \u043D\u0435 \u043C\u043E\u0436\u043D\u0430 \u0432\u0438\u0434\u0430\u043B\u044F\u0442\u0438','error');return;}
  items.splice(idx,1);
  gcSet('navItems',items);
  gcRenderNav();
  buildSidebar();
}

function gcAddNavItem(){
  var ico=document.getElementById('gc-new-ico').value.trim()||'\u2B50';
  var lbl=document.getElementById('gc-new-lbl').value.trim();
  if(!lbl){mkToast('\u0412\u0432\u0435\u0434\u0456\u0442\u044C \u043D\u0430\u0437\u0432\u0443 \u0432\u043A\u043B\u0430\u0434\u043A\u0438','error');return;}
  var sec=document.getElementById('gc-new-sec').value.trim()||'\u0406\u043D\u0448\u0435';
  var items=gcGetNavItems();
  items.push({id:'custom_'+uid(),ico:ico,lbl:lbl,sec:sec,badge:false,roles:['god','director','admin','tutor'],custom:true});
  gcSet('navItems',items);
  document.getElementById('gc-new-ico').value='';
  document.getElementById('gc-new-lbl').value='';
  document.getElementById('gc-new-sec').value='';
  gcRenderNav();
  buildSidebar();
  mkToast('\u0412\u043A\u043B\u0430\u0434\u043A\u0443 "'+lbl+'" \u0434\u043E\u0434\u0430\u043D\u043E');
}

function gcResetNav(){
  if(!confirm('\u0421\u043A\u0438\u043D\u0443\u0442\u0438 \u043D\u0430\u0432\u0456\u0433\u0430\u0446\u0456\u044E \u0434\u043E \u0441\u0442\u0430\u043D\u0434\u0430\u0440\u0442\u043D\u043E\u0457?'))return;
  gcSet('navItems',null);
  gcRenderNav();
  buildSidebar();
  var nel=document.getElementById('ni-'+S.currentPage);
  if(nel)nel.classList.add('active');
  mkToast('\u041D\u0430\u0432\u0456\u0433\u0430\u0446\u0456\u044E \u0441\u043A\u0438\u043D\u0443\u0442\u043E');
}

function gcDragStart(e,idx){_gcDragSrc=idx;e.dataTransfer.effectAllowed='move';}

function gcDragOver(e,idx){
  e.preventDefault();e.dataTransfer.dropEffect='move';
  document.querySelectorAll('.nav-edit-row').forEach(function(r,i){
    r.classList.toggle('drag-over',i===idx&&i!==_gcDragSrc);
  });
}

function gcDragLeave(){document.querySelectorAll('.nav-edit-row').forEach(function(r){r.classList.remove('drag-over');});}

function gcDrop(e,targetIdx){
  e.preventDefault();gcDragLeave();
  if(_gcDragSrc===null||_gcDragSrc===targetIdx){_gcDragSrc=null;return;}
  var items=gcGetNavItems();
  var moved=items.splice(_gcDragSrc,1)[0];
  items.splice(targetIdx,0,moved);
  _gcDragSrc=null;
  gcSet('navItems',items);
  gcRenderNav();
  buildSidebar();
  var nel=document.getElementById('ni-'+S.currentPage);
  if(nel)nel.classList.add('active');
}

function gcGetFields(){
  var cfg=gcGetConfig();
  return (cfg.customFields||[]).slice();
}

function gcRenderFields(){
  var fields=gcGetFields();
  var targets={student:'\u0423\u0447\u0435\u043D\u044C',lesson:'\u0417\u0430\u043D\u044F\u0442\u0442\u044F',tutor:'\u0412\u0438\u043A\u043B\u0430\u0434\u0430\u0447',payment:'\u041F\u043B\u0430\u0442\u0456\u0436'};
  var el=document.getElementById('gc-field-editor');
  if(!fields.length){
    el.innerHTML='<div class="empty" style="padding:24px"><div class="ei">\uD83D\uDDC2</div>\u0414\u043E\u0434\u0430\u0442\u043A\u043E\u0432\u0438\u0445 \u043F\u043E\u043B\u0456\u0432 \u043D\u0435\u043C\u0430\u0454.<br><span style="font-size:12px">\u0421\u043A\u043E\u0440\u0438\u0441\u0442\u0430\u0439\u0442\u0435\u0441\u044F \u0444\u043E\u0440\u043C\u043E\u044E \u043D\u0438\u0436\u0447\u0435, \u0449\u043E\u0431 \u0434\u043E\u0434\u0430\u0442\u0438 \u043F\u0435\u0440\u0448\u0435 \u043F\u043E\u043B\u0435.</span></div>';
    return;
  }
  var rows='';
  fields.forEach(function(f,i){
    var tSel='';
    Object.keys(targets).forEach(function(k){
      tSel+='<option value="'+k+'"'+(f.target===k?' selected':'')+'>'+targets[k]+'</option>';
    });
    var extraInput='';
    if(f.type==='select'){
      extraInput='<input type="text" value="'+gcEsc((f.options||[]).join('; '))+'" placeholder="\u0412\u0430\u0440\u0456\u0430\u043D\u0442\u0438 \u0447\u0435\u0440\u0435\u0437 ; "'+
        ' style="flex:1;min-width:100px;font-size:11px;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t1);padding:4px 8px"'+
        ' oninput="gcLiveFieldOpts('+i+',this.value)">';
    } else {
      extraInput='<span style="flex:1"></span>';
    }
    rows+='<div class="field-row">'
      +'<span style="color:var(--t3);font-size:14px;cursor:grab;padding:0 2px">&#8283;</span>'
      +'<span class="field-type-badge">'+FIELD_TYPE_ICONS[f.type]+' '+FIELD_TYPE_LABELS[f.type]+'</span>'
      +'<input type="text" value="'+gcEsc(f.label||'')+'" placeholder="\u041D\u0430\u0437\u0432\u0430 \u043F\u043E\u043B\u044F"'
      +' style="flex:1;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t1);padding:5px 8px;font-size:13px;font-family:Karla,sans-serif"'
      +' oninput="gcLiveFieldLabel('+i+',this.value)">'
      +'<select style="width:95px;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t2);padding:4px 6px;font-size:11px" onchange="gcLiveFieldTarget('+i+',this.value)">'+tSel+'</select>'
      +extraInput
      +'<label style="display:flex;align-items:center;gap:4px;font-size:11px;color:var(--t2);white-space:nowrap;cursor:pointer">'
      +'<input type="checkbox" '+(f.required?'checked':'')+' style="accent-color:var(--god2);cursor:pointer" onchange="gcLiveFieldReq('+i+',this.checked)"> *\u043E\u0431\u043E\u0432</label>'
      +'<button class="btn btn-sm" style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger)" onclick="gcDelField('+i+')">&times;</button>'
      +'</div>';
  });
  el.innerHTML='<div class="field-editor">'+rows+'</div>'
    +'<div class="gc-save-bar">'
    +'<span style="font-size:12px;color:var(--tut)">&#10003; \u041F\u043E\u043B\u044F \u0432\u0456\u0434\u043E\u0431\u0440\u0430\u0436\u0430\u044E\u0442\u044C\u0441\u044F \u0443 \u0444\u043E\u0440\u043C\u0430\u0445 \u043F\u0440\u0438 \u0434\u043E\u0434\u0430\u0432\u0430\u043D\u043D\u0456 \u0437\u0430\u043F\u0438\u0441\u0456\u0432</span>'
    +'<button class="btn btn-g btn-sm" style="margin-left:auto" onclick="gcClearFields()">&#128465; \u041E\u0447\u0438\u0441\u0442\u0438\u0442\u0438 \u0432\u0441\u0456</button>'
    +'</div>';
}

function gcLiveFieldLabel(idx,val){var f=gcGetFields();f[idx].label=val;gcSet('customFields',f);}

function gcLiveFieldTarget(idx,val){var f=gcGetFields();f[idx].target=val;gcSet('customFields',f);}

function gcLiveFieldReq(idx,val){var f=gcGetFields();f[idx].required=val;gcSet('customFields',f);}

function gcLiveFieldOpts(idx,val){
  var f=gcGetFields();
  f[idx].options=val.split(';').map(function(x){return x.trim();}).filter(Boolean);
  gcSet('customFields',f);
}

function gcDelField(idx){
  var f=gcGetFields();f.splice(idx,1);gcSet('customFields',f);gcRenderFields();
  mkToast('\u041F\u043E\u043B\u0435 \u0432\u0438\u0434\u0430\u043B\u0435\u043D\u043E');
}

function gcClearFields(){
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0432\u0441\u0456 \u0434\u043E\u0434\u0430\u0442\u043A\u043E\u0432\u0456 \u043F\u043E\u043B\u044F?'))return;
  gcSet('customFields',[]);gcRenderFields();mkToast('\u041F\u043E\u043B\u044F \u043E\u0447\u0438\u0449\u0435\u043D\u043E');
}

function gcAddField(){
  var type=document.getElementById('gc-field-type').value;
  var label=document.getElementById('gc-field-lbl').value.trim();
  var target=document.getElementById('gc-field-target').value;
  var optsRaw=document.getElementById('gc-field-opts').value;
  if(type!=='divider'&&type!=='label'&&!label){mkToast('\u0412\u0432\u0435\u0434\u0456\u0442\u044C \u043D\u0430\u0437\u0432\u0443 \u043F\u043E\u043B\u044F','error');return;}
  var f=gcGetFields();
  var obj={id:'cf_'+uid(),type:type,label:label||(type==='divider'?'---':'\u0417\u0430\u0433\u043E\u043B\u043E\u0432\u043E\u043A'),target:target,required:false};
  if(type==='select'&&optsRaw)obj.options=optsRaw.split(';').map(function(x){return x.trim();}).filter(Boolean);
  f.push(obj);
  gcSet('customFields',f);
  document.getElementById('gc-field-lbl').value='';
  document.getElementById('gc-field-opts').value='';
  gcRenderFields();
  mkToast('"'+obj.label+'" \u0434\u043E\u0434\u0430\u043D\u043E \u0434\u043E \u0444\u043E\u0440\u043C\u0438 "'+target+'"');
}

function gcGetLabels(){
  var cfg=gcGetConfig();
  var out={};
  Object.keys(DEFAULT_LABELS_MAP).forEach(function(k){out[k]=DEFAULT_LABELS_MAP[k].def;});
  Object.assign(out,cfg.labels||{});
  return out;
}

function gcRenderLabels(){
  var labels=gcGetLabels();
  var el=document.getElementById('gc-labels-editor');
  var wrapper=document.createElement('div');
  wrapper.style.cssText='display:grid;grid-template-columns:1fr 1fr;gap:12px';
  Object.keys(DEFAULT_LABELS_MAP).forEach(function(key){
    var def=DEFAULT_LABELS_MAP[key];
    var div=document.createElement('div');
    div.className='fgr';
    var lbl=document.createElement('label');
    lbl.style.cssText='display:flex;align-items:center;justify-content:space-between;gap:4px';
    var lblText=document.createElement('span');
    lblText.textContent=def.label;
    var resetBtn=document.createElement('button');
    resetBtn.type='button';
    resetBtn.innerHTML='&#8635;';
    resetBtn.title='\u0421\u043A\u0438\u043D\u0443\u0442\u0438 \u0434\u043E \u0441\u0442\u0430\u043D\u0434\u0430\u0440\u0442\u043D\u043E\u0433\u043E';
    resetBtn.style.cssText='background:none;border:none;color:var(--t3);cursor:pointer;font-size:13px;padding:0 2px';
    resetBtn.addEventListener('click',function(){gcResetLabel(key);});
    lbl.appendChild(lblText);
    lbl.appendChild(resetBtn);
    var inp=document.createElement('input');
    inp.type='text';
    inp.value=labels[key]||'';
    inp.placeholder=def.def;
    inp.style.cssText='width:100%;background:var(--s2);border:1px solid var(--b1);border-radius:8px;color:var(--t1);padding:9px 12px;font-size:13px;outline:none;transition:border .15s';
    inp.addEventListener('focus',function(){this.style.borderColor='var(--god2)';});
    inp.addEventListener('blur',function(){this.style.borderColor='var(--b1)';});
    inp.addEventListener('input',function(){gcLiveLabelChange(key,this.value);});
    div.appendChild(lbl);
    div.appendChild(inp);
    wrapper.appendChild(div);
  });
  var saveBar=document.createElement('div');
  saveBar.className='gc-save-bar';
  saveBar.innerHTML='<span style="font-size:12px;color:var(--tut)">&#10003; \u0422\u0435\u043A\u0441\u0442\u0438 \u043E\u043D\u043E\u0432\u043B\u044E\u044E\u0442\u044C\u0441\u044F \u0432 \u0456\u043D\u0442\u0435\u0440\u0444\u0435\u0439\u0441\u0456 \u043C\u0438\u0442\u0442\u0454\u0432\u043E</span>'
    +'<button class="btn btn-g btn-sm" style="margin-left:auto" onclick="gcResetAllLabels()">&#8635; \u0421\u043A\u0438\u043D\u0443\u0442\u0438 \u0432\u0441\u0456</button>';
  el.innerHTML='';
  el.appendChild(wrapper);
  el.appendChild(saveBar);
}

function gcLiveLabelChange(key,val){
  var cfg=gcGetConfig();
  if(!cfg.labels)cfg.labels={};
  if(val===DEFAULT_LABELS_MAP[key].def||val===''){delete cfg.labels[key];} else {cfg.labels[key]=val;}
  gcSet('labels',cfg.labels&&Object.keys(cfg.labels).length?cfg.labels:null);
  // Apply live
  gcApplyLabel(key,val||DEFAULT_LABELS_MAP[key].def);
}

function gcApplyLabel(key,val){
  var ptitle=document.getElementById('ptitle');
  if(key==='studentsTitle'){PLABELS.students=val;if(S.currentPage==='students'&&ptitle)ptitle.textContent=val;}
  if(key==='tutorsTitle'){PLABELS.tutors=val;if(S.currentPage==='tutors'&&ptitle)ptitle.textContent=val;}
  if(key==='lessonsTitle'){PLABELS.lessons=val;if(S.currentPage==='lessons'&&ptitle)ptitle.textContent=val;}
  if(key==='paymentsTitle'){PLABELS.payments=val;if(S.currentPage==='payments'&&ptitle)ptitle.textContent=val;}
  if(key==='scheduleTitle'){PLABELS.schedule=val;if(S.currentPage==='schedule'&&ptitle)ptitle.textContent=val;}
  if(key==='reportsTitle'){PLABELS.reports=val;if(S.currentPage==='reports'&&ptitle)ptitle.textContent=val;}
  if(key==='appName'){var sblt=document.querySelector('.sblt');if(sblt)sblt.textContent=val;}
  if(key==='loginTitle'){var elt=document.querySelector('.lh');if(elt)elt.textContent=val;}
  if(key==='loginSub'){var els=document.querySelector('.lsub');if(els)els.textContent=val;}
  var ab=document.getElementById('addbtn');
  if(ab&&key==='addStudent'&&S.currentPage==='students')ab.textContent='+ '+val;
  if(ab&&key==='addLesson'&&(S.currentPage==='lessons'||S.currentPage==='schedule'))ab.textContent='+ '+val;
  if(ab&&key==='addPayment'&&S.currentPage==='payments')ab.textContent='+ '+val;
  if(ab&&key==='addTutor'&&S.currentPage==='tutors')ab.textContent='+ '+val;
  buildSidebar();
  var nel=document.getElementById('ni-'+S.currentPage);if(nel)nel.classList.add('active');
}

function gcResetLabel(key){
  var cfg=gcGetConfig();
  if(cfg.labels){delete cfg.labels[key];gcSet('labels',Object.keys(cfg.labels).length?cfg.labels:null);}
  gcRenderLabels();
  gcApplyLabel(key,DEFAULT_LABELS_MAP[key].def);
}

function gcResetAllLabels(){
  if(!confirm('\u0421\u043A\u0438\u043D\u0443\u0442\u0438 \u0432\u0441\u0456 \u0442\u0435\u043A\u0441\u0442\u0438 \u0434\u043E \u0441\u0442\u0430\u043D\u0434\u0430\u0440\u0442\u043D\u0438\u0445?'))return;
  gcSet('labels',null);
  var labels={};
  Object.keys(DEFAULT_LABELS_MAP).forEach(function(k){labels[k]=DEFAULT_LABELS_MAP[k].def;});
  Object.keys(labels).forEach(function(k){gcApplyLabel(k,labels[k]);});
  gcRenderLabels();
  mkToast('\u0412\u0441\u0456 \u0442\u0435\u043A\u0441\u0442\u0438 \u0441\u043A\u0438\u043D\u0443\u0442\u043E');
}

function applyGodConfig(){
  var cfg=gcGetConfig();
  // Apply permissions
  if(cfg.perms){
    ['director','admin','tutor'].forEach(function(role){
      if(cfg.perms[role]){
        Object.assign(ROLES[role].can,cfg.perms[role]);
        if('seeIncome' in cfg.perms[role])ROLES[role].seeIncome=cfg.perms[role].seeIncome;
        if('seeAll' in cfg.perms[role])ROLES[role].seeAll=cfg.perms[role].seeAll;
      }
    });
  }
  // Apply labels
  if(cfg.labels){
    Object.keys(cfg.labels).forEach(function(k){
      gcApplyLabel(k,cfg.labels[k]);
    });
  }
}

function renderCustomFields(target,containerId){
  var cfg=gcGetConfig();
  var fields=(cfg.customFields||[]).filter(function(f){return f.target===target;});
  if(!fields.length)return;
  var el=document.getElementById(containerId);
  if(!el)return;
  var html='<div class="fgr full" style="border-top:1px solid var(--b1);padding-top:12px;margin-top:6px">'
    +'<label style="color:var(--adm);letter-spacing:.5px">&#9889; \u0414\u043E\u0434\u0430\u0442\u043A\u043E\u0432\u0456 \u043F\u043E\u043B\u044F</label></div>';
  fields.forEach(function(f){
    if(f.type==='divider'){
      html+='<div style="grid-column:1/-1;border-top:1px solid var(--b1);margin:4px 0;font-size:11px;color:var(--t3);padding-top:4px">'+(f.label!=='---'?f.label:'')+'</div>';
    } else if(f.type==='label'){
      html+='<div class="fgr full"><div style="font-size:12px;font-weight:700;color:var(--adm);letter-spacing:.5px;text-transform:uppercase;margin-top:6px">'+f.label+'</div></div>';
    } else if(f.type==='checkbox'){
      html+='<div class="fgr"><label style="display:flex;align-items:center;gap:8px;cursor:pointer"><input type="checkbox" id="cf_'+f.id+'" style="accent-color:var(--adm);width:14px;height:14px"> <span>'+f.label+(f.required?' *':'')+'</span></label></div>';
    } else if(f.type==='select'){
      var opts='<option value="">\u041E\u0431\u0435\u0440\u0456\u0442\u044C...</option>';
      (f.options||[]).forEach(function(o){opts+='<option>'+o+'</option>';});
      html+='<div class="fgr"><label>'+f.label+(f.required?' *':'')+'</label><select id="cf_'+f.id+'">'+opts+'</select></div>';
    } else if(f.type==='textarea'){
      html+='<div class="fgr full"><label>'+f.label+(f.required?' *':'')+'</label><textarea id="cf_'+f.id+'" placeholder="'+f.label+'..."></textarea></div>';
    } else {
      var itype=f.type==='number'?'number':(f.type==='date'?'date':'text');
      html+='<div class="fgr"><label>'+f.label+(f.required?' *':'')+'</label><input id="cf_'+f.id+'" type="'+itype+'" placeholder="'+f.label+(f.required?' *':'')+'"></div>';
    }
  });
  el.insertAdjacentHTML('beforeend',html);
}

function gcSaveRoles(){mkToast('\u041F\u0440\u0430\u0432\u0430 \u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043D\u043E \u2705');}

function gcSaveNav(){buildSidebar();mkToast('\u041D\u0430\u0432\u0456\u0433\u0430\u0446\u0456\u044E \u043E\u043D\u043E\u0432\u043B\u0435\u043D\u043E \u2705');}

function gcSaveFields(){mkToast('\u041F\u043E\u043B\u044F \u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043D\u043E \u2705');}

function gcResetLabels(){gcResetAllLabels();}

function gcSaveLabels(){mkToast('\u0422\u0435\u043A\u0441\u0442\u0438 \u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043D\u043E \u2705');}

function gSearch(q){if(q.length>1&&S.currentPage!=='students'){nav('students');}}

// = Supabase integration =
// =
// SUPABASE CONFIG -    
// app.supabase.com  Project Settings  API
// =
var SUPABASE_URL  = 'https://rndxbvwisppxnhvrzwqi.supabase.co';
var SUPABASE_ANON = 'sb_publishable_21KKA9MELBdwMRj4XG0riw_NuLYzpAw';

// =
// APP STATE
// =
var _sb = null;
var CU  = null;   // current user profile from DB
var S   = {
  students:[], tutors:[], lessons:[], payments:[],
  users:[], subjects:[], comms:[], branches:[], pricingRules:[],
  settings:{}, currentBranchId:null,
  weekOffset:0, dayOffset:0, dashWeekOffset:0,
  currentPage:'dashboard', editId:null, schView:'week',
  sfCur:'all', godConfig:null
};
var sfCur = 'all';
var _channels = [];
var _syncTimer = null;

// =
// INIT
// =

// =
// ANALYTICS DASHBOARD
// =
function renderAnalytics(){
  var pg = document.getElementById('pg-analytics');
  if(!pg) return;

  // Date range filter
  var rangeEl = document.getElementById('an-range');
  var range   = rangeEl ? rangeEl.value : '30';
  var now     = new Date();
  var fromDate = new Date(now);
  if(range === 'week')  fromDate.setDate(now.getDate()-7);
  else if(range === '30') fromDate.setDate(now.getDate()-30);
  else if(range === '90') fromDate.setDate(now.getDate()-90);
  else if(range === 'year') fromDate.setFullYear(now.getFullYear()-1);
  else fromDate = new Date(0); // all time
  var fromStr = localDateStr(fromDate);

  var lessons  = (S.lessons||[]).filter(function(l){ return l.date >= fromStr; });
  var comms    = (S.comms||[]).filter(function(c){ return c.date >= fromStr; });
  var students = S.students || [];
  var tutors   = S.tutors   || [];
  var branches = S.branches || [{id:'all', name:'\u0412\u0441\u0456 \u0444\u0456\u043B\u0456\u0457'}];

  // = Helper =
  function kpi(icon, label, value, sub, color){
    return '<div class="an-kpi" style="--kc:'+color+'">'
      +'<div class="an-kpi-ico">'+icon+'</div>'
      +'<div class="an-kpi-val">'+value+'</div>'
      +'<div class="an-kpi-lbl">'+label+'</div>'
      +(sub?'<div class="an-kpi-sub">'+sub+'</div>':'')
      +'</div>';
  }

  function statRow(label, value, total, color){
    var pct = total ? Math.round(value/total*100) : 0;
    return '<div class="an-row">'
      +'<div class="an-row-lbl">'+label+'</div>'
      +'<div class="an-row-bar"><div class="an-row-fill" style="width:'+pct+'%;background:'+color+'"></div></div>'
      +'<div class="an-row-val">'+value+'</div>'
      +'</div>';
  }

  function calcStats(lessonsArr, commsArr, studentsArr){
    var done     = lessonsArr.filter(function(l){return l.status==='done'||l.status==='completed';}).length;
    var missed   = lessonsArr.filter(function(l){return l.status==='missed'||l.status==='absent';}).length;
    var cancelled= lessonsArr.filter(function(l){return l.status==='cancelled';}).length;
    var planned  = lessonsArr.filter(function(l){return l.status==='planned'||l.status==='scheduled';}).length;
    var total    = lessonsArr.length;
    var income   = lessonsArr.filter(function(l){return l.status==='done'||l.status==='completed';})
                    .reduce(function(s,l){return s+(parseFloat(l.price)||0);},0);
    return { done, missed, cancelled, planned, total, income,
      students: studentsArr.length,
      comms: commsArr.length };
  }

  // = OVERALL stats =
  var overallStats = calcStats(lessons, comms, students);

  // = BY BRANCH =
  var branchRows = '';
  var allBranches = (S.branches||[]);
  if(!allBranches.length) allBranches = [{id:null,name:'\u0417\u0430\u0433\u0430\u043B\u044C\u043D\u0430'}];

  allBranches.forEach(function(b){
    var bLessons  = b.id ? lessons.filter(function(l){return l.branchId===b.id||l.branch_id===b.id;}) : lessons;
    var bComms    = b.id ? comms.filter(function(c){return c.branchId===b.id||c.branch_id===b.id;}) : comms;
    var bStudents = b.id ? students.filter(function(s){return s.branchId===b.id||s.branch_id===b.id;}) : students;
    var bs = calcStats(bLessons, bComms, bStudents);
    var maxL = Math.max(overallStats.total, 1);
    branchRows += '<div class="an-branch-card">'
      +'<div class="an-branch-title">\uD83C\uDFE2 '+b.name+'</div>'
      +'<div class="an-kpi-row">'
      +kpi('\u2705','\u041F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E',   bs.done,     null,'var(--tut)')
      +kpi('\u274C','\u041F\u0440\u043E\u043F\u0443\u0449\u0435\u043D\u043E',   bs.missed,   null,'var(--danger)')
      +kpi('\uD83D\uDD04','\u0412\u0456\u0434\u043F\u0440\u0430\u0446\u044C\u043E\u0432\u0430\u043D\u043E',bs.done,    null,'var(--adm)')
      +kpi('\uD83D\uDCAC','\u041A\u043E\u043C\u0443\u043D\u0456\u043A\u0430\u0446\u0456\u0439', bs.comms,    null,'var(--god2)')
      +kpi('\uD83D\uDC65','\u0423\u0447\u043D\u0456\u0432',       bs.students, null,'var(--dir)')
      +'</div>'
      +'<div style="margin-top:8px">'
      +statRow('\u041F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E',    bs.done,     Math.max(bs.total,1),'var(--tut)')
      +statRow('\u041F\u0440\u043E\u043F\u0443\u0449\u0435\u043D\u043E',    bs.missed,   Math.max(bs.total,1),'var(--danger)')
      +statRow('\u0421\u043A\u0430\u0441\u043E\u0432\u0430\u043D\u043E',    bs.cancelled,Math.max(bs.total,1),'var(--t3)')
      +'</div>'
      +'</div>';
  });

  // = BY TUTOR =
  var tutorRows = '';
  var filtBranch = document.getElementById('an-branch')?.value||'';
  var filtTutor  = document.getElementById('an-tutor')?.value||'';
  var visibleTutors = tutors.filter(function(t){
    if(filtBranch && t.branchId!==filtBranch && t.branch_id!==filtBranch) return false;
    if(filtTutor  && t.id!==filtTutor) return false;
    return true;
  });
  if(!visibleTutors.length) visibleTutors = tutors;

  var maxDone = Math.max.apply(null, visibleTutors.map(function(t){
    return lessons.filter(function(l){return l.tutorId===t.id&&(l.status==='done'||l.status==='completed');}).length;
  }).concat([1]));

  visibleTutors.forEach(function(t){
    var tL = lessons.filter(function(l){return l.tutorId===t.id||l.tutor_id===t.id;});
    var tC = comms.filter(function(c){return c.tutorId===t.id||c.tutor_id===t.id;});
    var tS = students.filter(function(s){return s.tutorId===t.id||s.tutor_id===t.id;});
    var ts = calcStats(tL, tC, tS);
    var pctDone = maxDone ? Math.round(ts.done/maxDone*100) : 0;
    tutorRows += '<div class="an-tutor-row">'
      +'<div style="display:flex;align-items:center;gap:8px;min-width:140px">'
      +mkAv(t.fn,t.ln,32)
      +'<div><div style="font-weight:600;font-size:13px">'+t.fn+' '+t.ln+'</div>'
      +'<div style="font-size:11px;color:var(--t2)">'+t.subj+'</div></div>'
      +'</div>'
      +'<div class="an-tutor-stats">'
      +'<div class="an-stat-cell" title="\u041F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E"><span class="an-stat-ico">\u2705</span>'+ts.done+'</div>'
      +'<div class="an-stat-cell" title="\u041F\u0440\u043E\u043F\u0443\u0449\u0435\u043D\u043E"><span class="an-stat-ico">\u274C</span>'+ts.missed+'</div>'
      +'<div class="an-stat-cell" title="\u0421\u043A\u0430\u0441\u043E\u0432\u0430\u043D\u043E"><span class="an-stat-ico">\uD83D\uDEAB</span>'+ts.cancelled+'</div>'
      +'<div class="an-stat-cell" title="\u041A\u043E\u043C\u0443\u043D\u0456\u043A\u0430\u0446\u0456\u0439"><span class="an-stat-ico">\uD83D\uDCAC</span>'+ts.comms+'</div>'
      +'<div class="an-stat-cell" title="\u0423\u0447\u043D\u0456\u0432"><span class="an-stat-ico">\uD83D\uDC65</span>'+ts.students+'</div>'
      +'</div>'
      +'<div class="an-bar-wrap"><div class="an-bar-fill" style="width:'+pctDone+'%"></div></div>'
      +'</div>';
  });

  // = Populate filters =
  var branchSel = document.getElementById('an-branch');
  if(branchSel && branchSel.children.length <= 1){
    (S.branches||[]).forEach(function(b){
      var opt=document.createElement('option'); opt.value=b.id; opt.textContent=b.name; branchSel.appendChild(opt);
    });
  }
  var tutorSel = document.getElementById('an-tutor');
  if(tutorSel && tutorSel.children.length <= 1){
    tutors.forEach(function(t){
      var opt=document.createElement('option'); opt.value=t.id; opt.textContent=t.fn+' '+t.ln; tutorSel.appendChild(opt);
    });
  }

  // = Render =
  var rangeLabel = {week:'\u0422\u0438\u0436\u0434\u0435\u043D\u044C','30':'30 \u0434\u043D\u0456\u0432','90':'3 \u043C\u0456\u0441\u044F\u0446\u0456',year:'\u0420\u0456\u043A',all:'\u0417\u0430 \u0432\u0435\u0441\u044C \u0447\u0430\u0441'}[range]||range;
  document.getElementById('an-content').innerHTML =
    // Overall summary
    '<div class="an-section-title">\uD83C\uDF10 \u0417\u0430\u0433\u0430\u043B\u044C\u043D\u0430 \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043A\u0430 \u2014 '+rangeLabel+'</div>'
    +'<div class="an-kpi-row an-kpi-row--big">'
    +kpi('\u2705','\u041F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E \u0443\u0440\u043E\u043A\u0456\u0432',   overallStats.done,     overallStats.total+' \u0432\u0441\u044C\u043E\u0433\u043E','var(--tut)')
    +kpi('\u274C','\u041F\u0440\u043E\u043F\u0443\u0449\u0435\u043D\u043E \u0443\u0440\u043E\u043A\u0456\u0432',   overallStats.missed,   Math.round((overallStats.missed/(Math.max(overallStats.total,1)))*100)+'%','var(--danger)')
    +kpi('\uD83D\uDD04','\u0412\u0456\u0434\u043F\u0440\u0430\u0446\u044C\u043E\u0432\u0430\u043D\u043E',      overallStats.done,     Math.round(overallStats.income)+' \u20B4','var(--adm)')
    +kpi('\uD83D\uDCAC','\u041A\u043E\u043C\u0443\u043D\u0456\u043A\u0430\u0446\u0456\u0439',        overallStats.comms,    '\u0437 \u0431\u0430\u0442\u044C\u043A\u0430\u043C\u0438','var(--god2)')
    +kpi('\uD83D\uDC65','\u0410\u043A\u0442\u0438\u0432\u043D\u0438\u0445 \u0443\u0447\u043D\u0456\u0432',     students.filter(function(s){return s.status==='active';}).length, students.length+' \u0432\u0441\u044C\u043E\u0433\u043E','var(--dir)')
    +'</div>'

    // By branch
    + (allBranches.length > 1
      ? '<div class="an-section-title" style="margin-top:20px">\uD83C\uDFE2 \u041F\u043E \u0444\u0456\u043B\u0456\u044F\u0445</div>'
        +'<div class="an-branches-grid">'+branchRows+'</div>'
      : '')

    // By tutor
    +'<div class="an-section-title" style="margin-top:20px">\uD83D\uDC64 \u041F\u043E \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0430\u0445</div>'
    +'<div class="an-tutor-header">'
    +'<div style="min-width:140px">\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440</div>'
    +'<div class="an-tutor-stats"><span>\u2705 \u041F\u0440\u043E\u0432\u0456\u0432</span><span>\u274C \u041F\u0440\u043E\u043F\u0443\u0441\u043A</span><span>\uD83D\uDEAB \u0421\u043A\u0430\u0441\u043E\u0432</span><span>\uD83D\uDCAC \u041A\u043E\u043C\u0443\u043D</span><span>\uD83D\uDC65 \u0423\u0447\u043D\u0456</span></div>'
    +'<div style="flex:1;font-size:10px;color:var(--t2);padding-left:8px">% \u0432\u0456\u0434 \u043B\u0456\u0434\u0435\u0440\u0430</div>'
    +'</div>'
    +'<div>'+tutorRows+'</div>';
}


// == localStorage stubs (not used in Supabase version) ==
function saveS(){ 
  // In Supabase version, data is saved to DB in real-time
  // godConfig is kept in memory only
}
function loadS(){}
function saveSess(){}
function loadSess(){}
function seedData(){}


// ═══════════════════════════════════════
// BACKUP & RESTORE
// ═══════════════════════════════════════
async function exportBackup(){
  var btn = document.getElementById('backup-btn');
  if(btn){ btn.disabled=true; btn.textContent='Завантаження...'; }
  try{
    // Load all data fresh from Supabase
    var tables = ['branches','tutors','students','lessons','payments','subjects','comms','pricing_rules','settings'];
    var backup = { version:1, created: new Date().toISOString(), data:{} };
    for(var i=0;i<tables.length;i++){
      var res = await _sb.from(tables[i]).select('*');
      backup.data[tables[i]] = res.data || [];
    }
    // Also include profiles (without sensitive auth data)
    var prof = await _sb.from('profiles').select('id,email,fn,ln,role,branch_id,perms');
    backup.data['profiles'] = prof.data || [];

    // Download as JSON file
    var json = JSON.stringify(backup, null, 2);
    var blob = new Blob([json], {type:'application/json'});
    var url  = URL.createObjectURL(blob);
    var a    = document.createElement('a');
    var date = localDateStr(new Date());
    a.href     = url;
    a.download = 'konstanta-backup-' + date + '.json';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    mkToast('Резервну копію збережено');
  }catch(e){
    mkToast('Помилка: '+e.message,'error');
  }
  if(btn){ btn.disabled=false; btn.textContent='⬇ Завантажити резервну копію'; }
}

function importBackupClick(){
  document.getElementById('backup-file-input').click();
}

async function importBackup(input){
  var file = input.files[0];
  if(!file){ return; }
  var btn = document.getElementById('restore-btn');
  if(btn){ btn.disabled=true; btn.textContent='Відновлення...'; }

  try{
    var text = await file.text();
    var backup = JSON.parse(text);

    if(!backup.version || !backup.data){
      mkToast('Невірний формат файлу','error');
      if(btn){btn.disabled=false;btn.textContent='⬆ Відновити з копії';}
      return;
    }

    if(!confirm('Відновити дані з копії від '+backup.created.slice(0,10)+'?\n\n⚠ Це перезапише ВСІ поточні дані!')){
      if(btn){btn.disabled=false;btn.textContent='⬆ Відновити з копії';}
      input.value='';
      return;
    }

    var stats = {};
    // Restore tables in correct order (deps first)
    var order = ['branches','subjects','pricing_rules','tutors','students','lessons','payments','comms','settings'];
    for(var i=0;i<order.length;i++){
      var table = order[i];
      var rows  = backup.data[table];
      if(!rows || !rows.length){ stats[table]=0; continue; }
      // Delete existing
      await _sb.from(table).delete().neq('id','');
      // Insert backup rows in chunks of 50
      var inserted = 0;
      for(var j=0;j<rows.length;j+=50){
        var chunk = rows.slice(j,j+50);
        var res = await _sb.from(table).insert(chunk);
        if(!res.error) inserted += chunk.length;
      }
      stats[table] = inserted;
    }

    // Reload all data
    await loadAll();
    renderSch && renderSch();
    nav(S.currentPage||'dashboard');

    var summary = Object.entries(stats).map(function(e){return e[0]+': '+e[1];}).join(', ');
    mkToast('Відновлено! '+summary);
  }catch(e){
    mkToast('Помилка відновлення: '+e.message,'error');
  }
  if(btn){btn.disabled=false;btn.textContent='⬆ Відновити з копії';}
  input.value='';
}

async function initApp(){
  // Wait for Supabase SDK to load (retry up to 3s)
  var sdkWait = 0;
  while(typeof supabase === 'undefined' && sdkWait < 30){
    await new Promise(function(r){setTimeout(r,100);});
    sdkWait++;
  }
  if(typeof supabase === 'undefined'){
    document.body.innerHTML = '<div style="padding:40px;font-family:Arial;text-align:center"><h2>❌ Помилка завантаження</h2><p>Не вдалось завантажити Supabase SDK. Перезавантажте сторінку.</p><button onclick="location.reload()" style="padding:10px 20px;background:#29abe2;color:#fff;border:none;border-radius:8px;cursor:pointer;font-size:14px;margin-top:16px">🔄 Перезавантажити</button></div>';
    return;
  }
  if(SUPABASE_URL === 'PASTE_YOUR_SUPABASE_URL'){
    var s=document.getElementById('setup'); if(s) s.style.display='flex';
    return;
  }
  var createClient = supabase.createClient;
  _sb = createClient(SUPABASE_URL, SUPABASE_ANON);
  var setupEl = document.getElementById('setup');
  var lsEl    = document.getElementById('ls');
  var asEl    = document.getElementById('as');
  if(setupEl) setupEl.style.display = 'none';
  // Show loading, hide both screens until session checked
  if(lsEl) lsEl.style.display = 'none';
  if(asEl) asEl.style.display = 'none';

  // Show loading spinner
  var loadDiv = document.createElement('div');
  loadDiv.id = 'app-loading';
  loadDiv.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;display:flex;align-items:center;justify-content:center;background:var(--bg,#0f1117);z-index:9999;flex-direction:column;gap:16px';
  loadDiv.innerHTML = '<div style="width:40px;height:40px;border:3px solid rgba(255,255,255,.1);border-top-color:#29abe2;border-radius:50%;animation:spin 0.8s linear infinite"></div>'
    + '<div style="color:rgba(255,255,255,.5);font-size:13px;font-family:Karla,sans-serif">Завантаження...</div>';
  var style = document.createElement('style');
  style.textContent = '@keyframes spin{to{transform:rotate(360deg)}}';
  document.head.appendChild(style);
  document.body.appendChild(loadDiv);

  function hideLoading(){
    var el = document.getElementById('app-loading');
    if(el) el.remove();
  }

  var _sess = await _sb.auth.getSession();
  var session = _sess.data && _sess.data.session;

  if(session){
    CU = null;
    await loadProfile(session.user);
    hideLoading();
    startApp();
  } else {
    hideLoading();
    if(lsEl) lsEl.style.display = 'flex';
  }

  _sb.auth.onAuthStateChange(async function(event, session){
    if(event === 'SIGNED_IN' && session){
      await loadProfile(session.user);
      hideLoading();
      startApp();
    } else if(event === 'SIGNED_OUT'){
      CU = null;
      stopChannels();
      if(asEl) asEl.style.display = 'none';
      if(lsEl) lsEl.style.display = 'flex';
    }
  });
}

// =
// AUTH
// =
async function doLogin(){
  var email=document.getElementById('lu').value.trim();
  var pass=document.getElementById('lp').value;
  var err=document.getElementById('lerr');
  var btn=document.getElementById('lbtn');
  if(err)err.style.display='none';
  if(btn){btn.disabled=true;btn.textContent='Входжу...';}
  if(!_sb){
    if(err){err.textContent='Помилка ініціалізації';err.style.display='block';}
    if(btn){btn.disabled=false;btn.textContent='Увійти';}
    return;
  }
  try{
    var res=await _sb.auth.signInWithPassword({email:email,password:pass});
    if(res.error){
      var msgs={'Invalid login credentials':'Невірний email або пароль','Email not confirmed':'Підтвердіть email','Too many requests':'Забагато спроб'};
      if(err){err.textContent=msgs[res.error.message]||res.error.message;err.style.display='block';}
      if(btn){btn.disabled=false;btn.textContent='Увійти';}
    }
  }catch(e){
    if(err){err.textContent='Помилка: '+e.message;err.style.display='block';}
    if(btn){btn.disabled=false;btn.textContent='Увійти';}
  }
}

async function doLogout(){
  stopChannels();
  await _sb.auth.signOut();
}

async function loadProfile(authUser){
  var _r2 = await _sb.from('profiles').select('*').eq('id', authUser.id).single(); var data = _r2.data;
  if(data){ CU = data; }
  else {
    const np = { id:authUser.id, email:authUser.email,
      fn: authUser.email.split('@')[0], ln:'', role:'tutor', perms:{} };
    await _sb.from('profiles').insert(np);
    CU = np;
  }
}

function uid(){ return crypto.randomUUID ? crypto.randomUUID() : Date.now().toString(36)+Math.random().toString(36).slice(2); }

// =
// SYNC INDICATOR
// =
function setSaving(){
  var dot=document.getElementById('syncdot'), lbl=document.getElementById('sync-lbl');
  if(dot) dot.className='sync-dot saving';
  if(lbl){ lbl.textContent='\u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043D\u043D\u044F\u2026'; lbl.style.color='var(--warn)'; }
}
function setSynced(){
  clearTimeout(_syncTimer);
  var dot=document.getElementById('syncdot'), lbl=document.getElementById('sync-lbl');
  if(dot) dot.className='sync-dot ok';
  if(lbl){ lbl.textContent='\u0441\u0438\u043D\u0445\u0440\u043E\u043D\u0456\u0437\u043E\u0432\u0430\u043D\u043E'; lbl.style.color='var(--tut)'; }
  _syncTimer = setTimeout(function(){ if(lbl){lbl.textContent='\u043E\u043D\u043B\u0430\u0439\u043D';lbl.style.color='var(--t3)';} }, 2500);
}

// =
// DATA LOADING
// =
async function loadAll(){
  setSaving();
  const tables = [
    { table:'branches',      key:'branches' },
    { table:'tutors',        key:'tutors' },
    { table:'students',      key:'students' },
    { table:'lessons',       key:'lessons',  order:'date' },
    { table:'payments',      key:'payments', order:'date' },
    { table:'subjects',      key:'subjects' },
    { table:'comms',         key:'comms',    order:'date' },
    { table:'pricing_rules', key:'pricingRules' },
  ];
  const results = await Promise.all(
    tables.map(function(t){
      var q = _sb.from(t.table).select('*');
      if(t.order) q = q.order(t.order, { ascending:false });
      return q;
    })
  );
  tables.forEach(function(t, i){ S[t.key] = results[i].data || []; });

  // Settings
  var _set = await _sb.from('settings').select('*').eq('id','main').single(); var set = _set.data;
  S.settings = set || {};

  // Users (profiles)
  var _users = await _sb.from('profiles').select('*'); var users = _users.data;
  S.users = users || [];

  // Normalize field names (snake_case  camelCase for UI compat)
  S.students = S.students.map(normalizeStudent);
  S.lessons  = S.lessons.map(normalizeLesson);
  S.payments = S.payments.map(normalizePayment);
  S.tutors   = S.tutors.map(normalizeTutor);
  S.comms    = S.comms.map(normalizeComm);
  S.pricingRules = S.pricingRules.map(normalizePricingRule);

  setSynced();
}

// Normalize DB rows to match UI field names
function normalizeStudent(r){ 
  var tutorIds = r.tutor_ids ? (Array.isArray(r.tutor_ids) ? r.tutor_ids : r.tutor_ids.split(',').filter(Boolean)) : (r.tutor_id ? [r.tutor_id] : []);
  return Object.assign({}, r, { tutorId:r.tutor_id, tutorIds:tutorIds, branchId:r.branch_id, parentFn:r.parent_fn, parentPhone:r.parent_phone }); 
}
function normalizeLesson(r){  return Object.assign({}, r, { studentId:r.student_id, tutorId:r.tutor_id, branchId:r.branch_id, recurId:r.recur_id, recurType:r.recur_type, recurIndex:r.recur_index }); }
function normalizePayment(r){ return Object.assign({}, r, { studentId:r.student_id, branchId:r.branch_id }); }
function normalizeTutor(r){   return Object.assign({}, r, { accId:r.acc_uid, branchId:r.branch_id }); }
function normalizeComm(r){    return Object.assign({}, r, { tutorId:r.tutor_id, studentId:r.student_id, branchId:r.branch_id }); }
function normalizePricingRule(r){ return Object.assign({}, r, { subjectMatch:r.subject_match, tutorId:r.tutor_id, gradeMatch:r.grade_match, durMin:r.dur_min }); }

// =
// REALTIME
// =
function startChannels(){
  var tableMap = {
    students:'students', tutors:'tutors', lessons:'lessons',
    payments:'payments', subjects:'subjects', comms:'comms',
    pricing_rules:'pricingRules', branches:'branches', profiles:'users'
  };
  Object.keys(tableMap).forEach(function(table){
    var key = tableMap[table];
    var ch = _sb.channel('rt:'+table)
      .on('postgres_changes',{ event:'*', schema:'public', table:table }, function(payload){
        handleChange(key, table, payload);
      })
      .subscribe();
    _channels.push(ch);
  });
}

function stopChannels(){
  _channels.forEach(function(ch){ try{ _sb.removeChannel(ch); }catch(e){} });
  _channels = [];
}

function handleChange(key, table, payload){
  setSynced();
  var ev  = payload.eventType;
  var row = payload.new;
  var old = payload.old;

  // Normalize
  var norm = { students:normalizeStudent, lessons:normalizeLesson,
    payments:normalizePayment, tutors:normalizeTutor,
    comms:normalizeComm, pricingRules:normalizePricingRule };
  if(norm[key] && row) row = norm[key](row);

  if(ev==='INSERT')      S[key] = (S[key]||[]).concat([row]);
  else if(ev==='UPDATE') S[key] = (S[key]||[]).map(function(r){ return r.id===row.id ? row : r; });
  else if(ev==='DELETE') S[key] = (S[key]||[]).filter(function(r){ return r.id !== old.id; });

  refreshPage(key);
}

function refreshPage(key){
  var pg = S.currentPage;
  var map = {
    students:['students','dashboard'], tutors:['tutors','dashboard'],
    lessons:['lessons','schedule','dashboard','profile'], payments:['payments','dashboard'],
    comms:['dashboard','profile'], subjects:['settings','lessons'],
    students:['students','dashboard','profile'], tutors:['tutors','dashboard','profile'],
    pricingRules:['settings'], branches:['settings'], users:['users']
  };
  if((map[key]||[]).includes(pg)){
    if(pg==='dashboard')  renderDash();
    else if(pg==='students')  renderStudents();
    else if(pg==='tutors')    renderTutors();
    else if(pg==='schedule')  renderSch();
    else if(pg==='lessons')   renderLessons();
    else if(pg==='payments')  renderPayments();
    else if(pg==='settings')  renderSettings();
    else if(pg==='users')     renderUsers();
  }
}

// =
// DB HELPERS
// =

async function loadTableFresh(table){
  var tableMap = {
    students:'students', tutors:'tutors', lessons:'lessons',
    payments:'payments', subjects:'subjects', comms:'comms',
    pricing_rules:'pricingRules', branches:'branches'
  };
  var key = tableMap[table];
  if(!key) return;
  var norm = {students:normalizeStudent,lessons:normalizeLesson,
    payments:normalizePayment,tutors:normalizeTutor,
    comms:normalizeComm,pricingRules:normalizePricingRule};
  var res = await _sb.from(table).select('*');
  if(res.error) return;
  var data = res.data || [];
  S[key] = norm[key] ? data.map(norm[key]) : data;
  setSynced();
  refreshPage(key);
}

async function dbInsert(table, data){
  setSaving();
  var _ri = await _sb.from(table).insert(data); var error = _ri.error;
  if(error){ mkToast('Помилка: '+error.message,'error'); throw error; }
  setTimeout(function(){ loadTableFresh(table); }, 800);
}
async function dbUpdate(table, id, data){
  setSaving();
  // profiles table has no updated_at column
  var noTimestamp = ['profiles'];
  var updateData = noTimestamp.indexOf(table) >= 0 
    ? Object.assign({}, data)
    : Object.assign({}, data, {updated_at: new Date().toISOString()});
  var _ru = await _sb.from(table).update(updateData).eq('id', id); var error = _ru.error;
  if(error){ mkToast('Помилка: '+error.message,'error'); throw error; }
  setTimeout(function(){ loadTableFresh(table); }, 800);
}
async function dbDelete(table, id){
  setSaving();
  var _rd = await _sb.from(table).delete().eq('id',id); var error = _rd.error;
  if(error){ mkToast('Помилка: '+error.message,'error'); throw error; }
  setTimeout(function(){ loadTableFresh(table); }, 500);
}

// =
// SAVE FUNCTIONS
// =
async function saveStudent(){
  var fn=document.getElementById('s-fn').value.trim(), ln=document.getElementById('s-ln').value.trim();
  if(!fn||!ln){ mkToast("\u0406\u043C'\u044F \u0442\u0430 \u043F\u0440\u0456\u0437\u0432\u0438\u0449\u0435 \u043E\u0431\u043E\u0432'\u044F\u0437\u043A\u043E\u0432\u0456",'error'); return; }
  var obj={
    fn, ln,
    age:    document.getElementById('s-age')?.value||null,
    grade:  document.getElementById('s-grade')?.value||'',
    phone:  document.getElementById('s-phone')?.value||'',
    email:  document.getElementById('s-email')?.value||'',
    subject:document.getElementById('s-subj')?.value||'',
    tutor_id:(function(){var cbs=document.querySelectorAll('.st-tutor-cb:checked');return cbs.length?cbs[0].value:null;})(),
    tutor_ids:(function(){return Array.from(document.querySelectorAll('.st-tutor-cb:checked')).map(function(cb){return cb.value;}).join(',');})(),
    status: document.getElementById('s-status')?.value||'active',
    src:    document.getElementById('s-src')?.value||'referral',
    notes:  document.getElementById('s-notes')?.value||'',
    parent_fn:   (document.getElementById('s-parent-fn')?.value||'').trim(),
    parent_phone:(document.getElementById('s-parent-phone')?.value||'').trim(),
    branch_id: myBranchId()||null,
  };
  // Auto-link to current tutor if none selected
  if(R()==='tutor' && !obj.tutor_id){
    var mt=myTutor();
    if(mt){ obj.tutor_id=mt.id; obj.tutor_ids=mt.id; }
  }
  try{
    if(S.editId){ await dbUpdate('students',S.editId,obj); mkToast('\u0423\u0447\u043D\u044F \u043E\u043D\u043E\u0432\u043B\u0435\u043D\u043E'); }
    else         { await dbInsert('students',Object.assign({id:uid()},obj)); mkToast('\u0423\u0447\u043D\u044F \u0434\u043E\u0434\u0430\u043D\u043E'); }
    closeM('mo-student'); S.editId=null;
  }catch(e){}
}

async function delStudent(id){
  if(!can('students')){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432','error'); return; }
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0443\u0447\u043D\u044F?')) return;
  try{ await dbDelete('students',id); mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); }catch(e){}
}

async function saveTutor(){
  var fn=document.getElementById('t-fn').value.trim(), ln=document.getElementById('t-ln').value.trim();
  if(!fn||!ln){ mkToast("\u0406\u043C'\u044F \u0442\u0430 \u043F\u0440\u0456\u0437\u0432\u0438\u0449\u0435 \u043E\u0431\u043E\u0432'\u044F\u0437\u043A\u043E\u0432\u0456",'error'); return; }
  var obj={
    fn, ln,
    phone:  document.getElementById('t-phone')?.value||'',
    email:  document.getElementById('t-email')?.value||'',
    subj:   document.getElementById('t-subj')?.value||'',
    rate:   document.getElementById('t-rate')?.value||null,
    bio:    document.getElementById('t-bio')?.value||'',
    rating: parseInt(document.getElementById('t-rating')?.value)||5,
    branch_id: myBranchId()||null,
  };
  try{
    if(S.editId){ await dbUpdate('tutors',S.editId,obj); mkToast('\u041E\u043D\u043E\u0432\u043B\u0435\u043D\u043E'); }
    else         { await dbInsert('tutors',Object.assign({id:uid()},obj)); mkToast('\u0412\u0438\u043A\u043B\u0430\u0434\u0430\u0447\u0430 \u0434\u043E\u0434\u0430\u043D\u043E'); }
    closeM('mo-tutor'); S.editId=null;
  }catch(e){}
}

async function delTutor(id){
  if(!can('tutors')){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432','error'); return; }
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0432\u0438\u043A\u043B\u0430\u0434\u0430\u0447\u0430?')) return;
  try{ await dbDelete('tutors',id); mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); }catch(e){}
}

async function saveLesson(){
  var stdEl=document.getElementById('l-std'); 
  var dateEl=document.getElementById('l-date');
  var studentId=stdEl?stdEl.value:''; 
  var date=dateEl?dateEl.value:'';
  if(!studentId||!date){ mkToast("\u0423\u0447\u0435\u043D\u044C \u0442\u0430 \u0434\u0430\u0442\u0430 \u043E\u0431\u043E\u0432'\u044F\u0437\u043A\u043E\u0432\u0456",'error'); return; }
  var recurType = document.getElementById('l-recur')?.value||'none';
  var obj={
    student_id: studentId,
    tutor_id:   document.getElementById('l-tutor')?.value||null,
    subject:    document.getElementById('l-subj')?.value||'',
    date, time: document.getElementById('l-time')?.value||'',
    dur:    parseInt(document.getElementById('l-dur')?.value)||60,
    price:  parseFloat(document.getElementById('l-price')?.value)||0,
    status: document.getElementById('l-stat')?.value||'planned',
    notes:  document.getElementById('l-notes')?.value||'',
    branch_id: myBranchId()||null,
  };
  try{
    if(S.editId){ await dbUpdate('lessons',S.editId,obj); mkToast('\u041E\u043D\u043E\u0432\u043B\u0435\u043D\u043E'); closeM('mo-lesson'); }
    else if(recurType && recurType!=='none'){
      var endDate  = document.getElementById('l-recur-end')?.value;
      var count    = parseInt(document.getElementById('l-recur-count')?.value)||10;
      var interval = parseInt(document.getElementById('l-recur-interval')?.value)||1;
      var dates    = genRecurDates(date, recurType, endDate, count, interval);
      var recurId  = uid();
      for(var i=0;i<dates.length;i++){
        await dbInsert('lessons',Object.assign({id:uid()},obj,{date:dates[i],recur_id:recurId,recur_type:recurType,recur_index:i}));
      }
      mkToast('\u0414\u043E\u0434\u0430\u043D\u043E '+dates.length+' \u0437\u0430\u043D\u044F\u0442\u044C'); closeM('mo-lesson');
    } else {
      await dbInsert('lessons',Object.assign({id:uid()},obj)); mkToast('\u0417\u0430\u043D\u044F\u0442\u0442\u044F \u0434\u043E\u0434\u0430\u043D\u043E'); closeM('mo-lesson');
    }
    S.editId=null;
  }catch(e){}
}

async function delLesson(id){
  if(!can('lessons')){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432','error'); return; }
  var l=(S.lessons||[]).find(function(x){return x.id===id;});
  if(l && l.recurId){ S.editId=id; openM('mo-del-recur'); }
  else{ if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0437\u0430\u043D\u044F\u0442\u0442\u044F?')) return; try{ await dbDelete('lessons',id); mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); }catch(e){} }
}

async function doDelLesson(mode){
  var id=S.editId, l=(S.lessons||[]).find(function(x){return x.id===id;});
  if(!l){ closeM('mo-del-recur'); return; }
  closeM('mo-del-recur'); S.editId=null;
  try{
    if(mode==='one'){ await dbDelete('lessons',id); mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); }
    else if(mode==='future'){
      var toDelete=(S.lessons||[]).filter(function(x){return x.recurId===l.recurId&&x.recurIndex>=l.recurIndex;});
      for(var i=0;i<toDelete.length;i++) await dbDelete('lessons',toDelete[i].id);
      mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E '+toDelete.length+' \u0437\u0430\u043D\u044F\u0442\u044C');
    } else {
      var all=(S.lessons||[]).filter(function(x){return x.recurId===l.recurId;});
      for(var i=0;i<all.length;i++) await dbDelete('lessons',all[i].id);
      mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E \u0441\u0435\u0440\u0456\u044E ('+all.length+')');
    }
  }catch(e){}
}

async function savePayment(){
  var studentId=document.getElementById('p-std')?.value, amount=parseFloat(document.getElementById('p-amt')?.value);
  if(!studentId||!amount){ mkToast("\u0423\u0447\u0435\u043D\u044C \u0442\u0430 \u0441\u0443\u043C\u0430 \u043E\u0431\u043E\u0432'\u044F\u0437\u043A\u043E\u0432\u0456",'error'); return; }
  var obj={
    student_id:studentId, amount,
    method: document.getElementById('p-mth')?.value||'cash',
    date:   document.getElementById('p-date')?.value,
    month:  document.getElementById('p-mon')?.value||'',
    status: document.getElementById('p-stat')?.value||'paid',
    note:   document.getElementById('p-note')?.value||'',
    branch_id: myBranchId()||null,
  };
  try{
    if(S.editId){ await dbUpdate('payments',S.editId,obj); mkToast('\u041E\u043D\u043E\u0432\u043B\u0435\u043D\u043E'); }
    else         { await dbInsert('payments',Object.assign({id:uid()},obj)); mkToast('\u0417\u0430\u043F\u0438\u0441\u0430\u043D\u043E'); }
    closeM('mo-payment'); S.editId=null;
  }catch(e){}
}

async function delPay(id){
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u043F\u043B\u0430\u0442\u0456\u0436?')) return;
  try{ await dbDelete('payments',id); mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); }catch(e){}
}


function updateParentInfo(){
  var sel = document.getElementById('cm-student');
  var wrap = document.getElementById('cm-parent-wrap');
  var info = document.getElementById('cm-parent-info');
  if(!sel || !wrap || !info) return;
  var sid = sel.value;
  var s = sid ? (S.students||[]).find(function(x){return x.id===sid;}) : null;
  if(s && (s.parentFn||s.parent_fn||s.parentPhone||s.parent_phone)){
    var fn = s.parentFn||s.parent_fn||'';
    var ph = s.parentPhone||s.parent_phone||'';
    info.innerHTML = (fn?'<strong>'+fn+'</strong>':'')+(ph?' — <a href="tel:'+ph+'">'+ph+'</a>':'');
    wrap.style.display = 'block';
  } else {
    wrap.style.display = 'none';
  }
}

async function saveComm(){
  var tutorId=document.getElementById('cm-tutor')?.value, date=document.getElementById('cm-date')?.value;
  if(!tutorId){ mkToast('\u041E\u0431\u0435\u0440\u0456\u0442\u044C \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0430','error'); return; }
  if(!date)   { mkToast('\u0412\u043A\u0430\u0436\u0456\u0442\u044C \u0434\u0430\u0442\u0443','error'); return; }
  try{
    await dbInsert('comms',{ id:uid(), tutor_id:tutorId,
      student_id:document.getElementById('cm-student')?.value||null,
      date, type:document.getElementById('cm-type')?.value||'call',
      note:document.getElementById('cm-note')?.value||'',
      branch_id:myBranchId()||null });
    closeM('mo-comm'); mkToast('Записано');
  }catch(e){ mkToast('Помилка: '+(e.message||e),'error'); }
}

async function delComm(id){
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438?')) return;
  try{ await dbDelete('comms',id); mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); }catch(e){}
}

async function saveSettings(){
  try{
    await _sb.from('settings').upsert({
      id:'main',
      name:    document.getElementById('set-name')?.value||'',
      phone:   document.getElementById('set-phone')?.value||'',
      email:   document.getElementById('set-email')?.value||'',
      address: document.getElementById('set-addr')?.value||'',
      updated_at: new Date().toISOString()
    });
    mkToast('\u0417\u0431\u0435\u0440\u0435\u0436\u0435\u043D\u043E');
  }catch(e){ mkToast('\u041F\u043E\u043C\u0438\u043B\u043A\u0430','error'); }
}

async function addSubj(){
  var name=(document.getElementById('ns-name')?.value||'').trim(), price=document.getElementById('ns-price')?.value;
  if(!name){ mkToast('\u0412\u0432\u0435\u0434\u0456\u0442\u044C \u043D\u0430\u0437\u0432\u0443','error'); return; }
  try{
    await dbInsert('subjects',{id:uid(),name,price:price||null,branch_id:myBranchId()||null});
    document.getElementById('ns-name').value='';
    document.getElementById('ns-price').value='';
    mkToast('\u0414\u043E\u0434\u0430\u043D\u043E');
  }catch(e){}
}

async function delSubj(id){
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u043F\u0440\u0435\u0434\u043C\u0435\u0442?')) return;
  try{ await dbDelete('subjects',id); mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); }catch(e){}
}

async function addBranch(){
  var nm=(document.getElementById('new-branch-name')?.value||'').trim();
  var addr=(document.getElementById('new-branch-addr')?.value||'').trim();
  if(!nm){ mkToast('\u0412\u0432\u0435\u0434\u0456\u0442\u044C \u043D\u0430\u0437\u0432\u0443 \u0444\u0456\u043B\u0456\u0457','error'); return; }
  try{
    await dbInsert('branches',{id:'b'+uid(),name:nm,address:addr,phone:''});
    document.getElementById('new-branch-name').value='';
    document.getElementById('new-branch-addr').value='';
    mkToast('\u0424\u0456\u043B\u0456\u044E \u0434\u043E\u0434\u0430\u043D\u043E');
  }catch(e){}
}

async function delBranch(id){
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0444\u0456\u043B\u0456\u044E?')) return;
  try{ await dbDelete('branches',id); if(S.currentBranchId===id)S.currentBranchId=null; mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); }catch(e){}
}

async function editBranch(id){
  var b=(S.branches||[]).find(function(x){return x.id===id;});
  if(!b) return;
  var nm=prompt('\u041D\u0430\u0437\u0432\u0430 \u0444\u0456\u043B\u0456\u0457:',b.name); if(!nm) return;
  var addr=prompt('\u0410\u0434\u0440\u0435\u0441\u0430:',b.address||'');
  try{ await dbUpdate('branches',id,{name:nm,address:addr}); mkToast('\u041E\u043D\u043E\u0432\u043B\u0435\u043D\u043E'); }catch(e){}
}

// Pricing rules
async function savePriceRule(){
  var name=(document.getElementById('pr-name')?.value||'').trim();
  var price=parseFloat(document.getElementById('pr-price')?.value||0);
  if(!name||!price){ mkToast("\u041D\u0430\u0437\u0432\u0430 \u0442\u0430 \u0446\u0456\u043D\u0430 \u043E\u0431\u043E\u0432'\u044F\u0437\u043A\u043E\u0432\u0456",'error'); return; }
  var editId=document.getElementById('pr-edit-id')?.value||'';
  var obj={
    name, price,
    subject_match: document.getElementById('pr-subj')?.value||'',
    tutor_id:      document.getElementById('pr-tutor')?.value||'',
    grade_match:   document.getElementById('pr-grade')?.value||'',
    dur_min:       parseInt(document.getElementById('pr-dur')?.value)||null,
    branch_id:     myBranchId()||null,
  };
  try{
    if(editId){ await dbUpdate('pricing_rules',editId,obj); mkToast('\u041E\u043D\u043E\u0432\u043B\u0435\u043D\u043E'); }
    else       { await dbInsert('pricing_rules',Object.assign({id:uid()},obj)); mkToast('\u041F\u0440\u0430\u0432\u0438\u043B\u043E \u0434\u043E\u0434\u0430\u043D\u043E'); }
    ['pr-name','pr-price','pr-subj','pr-grade','pr-dur'].forEach(function(f){ var el=document.getElementById(f);if(el)el.value=''; });
    var pt=document.getElementById('pr-tutor');if(pt)pt.value='';
    var pi=document.getElementById('pr-edit-id');if(pi)pi.value='';
    var pb=document.getElementById('pr-save-btn');if(pb)pb.textContent='+ \u0414\u043E\u0434\u0430\u0442\u0438 \u043F\u0440\u0430\u0432\u0438\u043B\u043E';
  }catch(e){}
}

async function editPriceRule(id){
  var r=(S.pricingRules||[]).find(function(x){return x.id===id;});
  if(!r) return;
  var set=function(elId,val){var el=document.getElementById(elId);if(el)el.value=val||'';};
  set('pr-name',r.name);set('pr-price',r.price);set('pr-subj',r.subjectMatch||r.subject_match);
  set('pr-grade',r.gradeMatch||r.grade_match);set('pr-dur',r.durMin||r.dur_min);set('pr-edit-id',r.id);
  var pt=document.getElementById('pr-tutor');if(pt)pt.value=r.tutorId||r.tutor_id||'';
  var pb=document.getElementById('pr-save-btn');if(pb)pb.textContent='\uD83D\uDCBE \u0417\u0431\u0435\u0440\u0435\u0433\u0442\u0438 \u0437\u043C\u0456\u043D\u0438';
}

async function delPriceRule(id){
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u043F\u0440\u0430\u0432\u0438\u043B\u043E?')) return;
  try{ await dbDelete('pricing_rules',id); mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); }catch(e){}
}

// Users management (profiles table)
async function renderUsers(){
  var list=document.getElementById('ut-list');
  if(!list) return;
  list.innerHTML='<div class="empty"><div class="ei">\u23F3</div>\u0417\u0430\u0432\u0430\u043D\u0442\u0430\u0436\u0435\u043D\u043D\u044F\u2026</div>';
  var _users2 = await _sb.from('profiles').select('*'); var users = _users2.data;
  S.users = users || [];
  list.innerHTML='';
  (users||[]).forEach(function(u){
    var ro=ROLES[u.role]||ROLES.tutor;
    var canEdit=R()==='god'||(R()==='director'&&u.role!=='god');
    var canDel=(R()==='god'&&u.id!==CU?.id)||(R()==='director'&&u.role!=='god'&&u.id!==CU?.id);
    var row=document.createElement('div'); row.className='ulr';
    var av=document.createElement('div'); av.className='av uav';
    av.style.cssText='background:'+ro.avatarBg+';width:38px;height:38px;font-size:14px;font-weight:700;flex-shrink:0;color:#fff';
    av.textContent=(u.fn?.[0]||'?')+(u.ln?.[0]||'');
    var info=document.createElement('div');info.className='uin';
    info.innerHTML='<div class="uinn">'+u.fn+' '+(u.ln||'')+'</div><div class="uinm">'+(u.email||'\u2014')+'</div>';
    var rpill=document.createElement('span');rpill.className='rpill '+u.role;
    rpill.innerHTML=ro.icon+' '+ro.label;
    var btns=document.createElement('div');btns.style.cssText='display:flex;gap:6px;margin-left:auto;align-items:center';
    if(canEdit){
      var eb=document.createElement('button');eb.className='btn btn-g btn-sm';eb.innerHTML='\u270F\uFE0F';
      (function(id){eb.onclick=function(){openUserM(id);};})(u.id);btns.appendChild(eb);
      var ab=document.createElement('button');ab.className='btn btn-p btn-sm';ab.textContent='\uD83D\uDD10 \u0414\u043E\u0441\u0442\u0443\u043F';
      (function(id){ab.onclick=function(){openUserAccessM(id);};})(u.id);btns.appendChild(ab);
    }
    if(canDel){
      var db=document.createElement('button');db.className='btn btn-sm btn-d';db.innerHTML='\uD83D\uDDD1';
      (function(id){db.onclick=function(){delUser(id);};})(u.id);btns.appendChild(db);
    }
    row.appendChild(av);row.appendChild(info);row.appendChild(rpill);row.appendChild(btns);
    list.appendChild(row);
  });
  if(!users?.length) list.innerHTML='<div class="empty"><div class="ei">\uD83D\uDC64</div>\u041D\u0435\u043C\u0430\u0454 \u0430\u043A\u0430\u0443\u043D\u0442\u0456\u0432</div>';
}

async function openUserM(id){
  S.editId=id;
  var u=(S.users||[]).find(function(x){return x.id===id;});
  if(!u) return;
  document.getElementById('mu-title').textContent='\u0420\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u0442\u0438 \u0430\u043A\u0430\u0443\u043D\u0442';
  document.getElementById('u-fn').value=u.fn||'';
  document.getElementById('u-ln').value=u.ln||'';
  document.getElementById('u-email').value=u.email||'';
  document.getElementById('u-role').value=u.role||'tutor';
  toggleTutLink();
  popSel('u-tlink',S.tutors,'id',function(t){return t.fn+' '+t.ln;},'\u041F\u0440\u0438\u0432\'\u044F\u0437\u0430\u0442\u0438 \u0434\u043E \u0432\u0438\u043A\u043B\u0430\u0434\u0430\u0447\u0430');
  var linked=(S.tutors||[]).find(function(t){return t.acc_uid===id||t.accId===id;});
  if(linked) document.getElementById('u-tlink').value=linked.id;
  openM('mo-user');
}

async function saveUser(){
  if(!S.editId) return;
  var fn=document.getElementById('u-fn').value.trim(), ln=document.getElementById('u-ln').value.trim();
  var role=document.getElementById('u-role').value;
  try{
    await dbUpdate('profiles',S.editId,{fn,ln,role});
    // Link tutor
    var tutorId=document.getElementById('u-tlink')?.value;
    if(role==='tutor'&&tutorId){
      await _sb.from('tutors').update({acc_uid:S.editId}).eq('id',tutorId);
    }
    if(CU?.id===S.editId){ CU=Object.assign({},CU,{fn,ln,role}); updateSBUser(); buildSidebar(); }
    mkToast('\u041E\u043D\u043E\u0432\u043B\u0435\u043D\u043E'); closeM('mo-user'); S.editId=null; renderUsers();
  }catch(e){ mkToast('Помилка збереження: '+(e.message||e),'error'); console.error('saveUser error:',e); }
}

async function delUser(id){
  if(id===CU?.id){ mkToast('\u041D\u0435 \u043C\u043E\u0436\u043D\u0430 \u0432\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0441\u0432\u0456\u0439 \u0430\u043A\u0430\u0443\u043D\u0442','error'); return; }
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0430\u043A\u0430\u0443\u043D\u0442?')) return;
  try{ await dbDelete('profiles',id); mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); renderUsers(); }catch(e){}
}

// Access editor
var _uaUserId=null;
async function openUserAccessM(id){
  var u=(S.users||[]).find(function(x){return x.id===id;});
  if(!u){ mkToast('\u041D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E','error'); return; }
  _uaUserId=id;
  document.querySelectorAll('.ua-tab').forEach(function(t,i){t.classList.toggle('active',i===0);});
  document.querySelectorAll('.ua-panel').forEach(function(p,i){p.classList.toggle('active',i===0);});
  buildUAHeader(u); buildUAPerms(u); buildUANav(u); buildUASummary(u);
  openM('mo-user-access');
}

async function uaPermChange(key,val,roleDefault){
  var u=(S.users||[]).find(function(x){return x.id===_uaUserId;});
  if(!u) return;
  var perms=JSON.parse(JSON.stringify(u.perms||{}));
  if(!perms.can) perms.can={};
  if(val===roleDefault) delete perms.can[key]; else perms.can[key]=val;
  if(!Object.keys(perms.can).length) delete perms.can;
  try{ await dbUpdate('profiles',_uaUserId,{perms}); if(CU?.id===_uaUserId)CU.perms=perms; }catch(e){}
  var u2=(S.users||[]).find(function(x){return x.id===_uaUserId;}); if(u2){u2.perms=perms;buildUASummary(u2);}
}

async function uaResetPerm(key){
  var u=(S.users||[]).find(function(x){return x.id===_uaUserId;});
  if(!u) return;
  var perms=JSON.parse(JSON.stringify(u.perms||{}));
  if(perms.can){delete perms.can[key];if(!Object.keys(perms.can).length)delete perms.can;}
  try{ await dbUpdate('profiles',_uaUserId,{perms}); if(CU?.id===_uaUserId)CU.perms=perms; }catch(e){}
  var u2=(S.users||[]).find(function(x){return x.id===_uaUserId;}); if(u2){u2.perms=perms;buildUAPerms(u2);buildUASummary(u2);}
}

async function uaNavChange(pageId,show,isInRole){
  var u=(S.users||[]).find(function(x){return x.id===_uaUserId;});
  if(!u) return;
  var perms=JSON.parse(JSON.stringify(u.perms||{}));
  if(!perms.hideNav)perms.hideNav=[];if(!perms.showNav)perms.showNav=[];
  if(show){
    perms.hideNav=perms.hideNav.filter(function(p){return p!==pageId;});
    if(!isInRole&&!perms.showNav.includes(pageId))perms.showNav.push(pageId);
  } else {
    perms.showNav=perms.showNav.filter(function(p){return p!==pageId;});
    if(isInRole&&!perms.hideNav.includes(pageId))perms.hideNav.push(pageId);
  }
  if(!perms.hideNav.length)delete perms.hideNav;
  if(!perms.showNav.length)delete perms.showNav;
  try{ await dbUpdate('profiles',_uaUserId,{perms}); if(CU?.id===_uaUserId){CU.perms=perms;buildSidebar();} }catch(e){}
  var u2=(S.users||[]).find(function(x){return x.id===_uaUserId;}); if(u2){u2.perms=perms;buildUASummary(u2);}
}

async function resetAllUserAccess(){
  var u=(S.users||[]).find(function(x){return x.id===_uaUserId;});
  if(!confirm('\u0421\u043A\u0438\u043D\u0443\u0442\u0438 \u043D\u0430\u043B\u0430\u0448\u0442\u0443\u0432\u0430\u043D\u043D\u044F \u0434\u043B\u044F '+(u?.fn||'')+' '+(u?.ln||'')+' ?')) return;
  try{
    await dbUpdate('profiles',_uaUserId,{perms:{}});
    if(CU?.id===_uaUserId){CU.perms={};buildSidebar();}
    var u2=(S.users||[]).find(function(x){return x.id===_uaUserId;}); if(u2){u2.perms={};buildUAPerms(u2);buildUANav(u2);buildUASummary(u2);}
    renderUsers(); mkToast('\u0421\u043A\u0438\u043D\u0443\u0442\u043E');
  }catch(e){}
}

function setBranch(id){
  S.currentBranchId = id||null;
  updateBranchSelector();
  renderSch && renderSch();
  nav(S.currentPage||'dashboard');
}

// =
// CLEAR DATA (god only)
// =
async function clearData(what){
  if(R()!=='god'){mkToast('\u0422\u0456\u043B\u044C\u043A\u0438 \u0411\u043E\u0433','error');return;}
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 '+what+' \u0434\u0430\u043D\u0456? \u0426\u0435 \u043D\u0435\u0437\u0432\u043E\u0440\u043E\u0442\u043D\u043E!')) return;
  var tables={lessons:['lessons'],payments:['payments'],all:['comms','payments','lessons','students','tutors']};
  var toDelete=tables[what]||[];
  try{
    for(var _ti=0; _ti<toDelete.length; _ti++) await _sb.from(toDelete[_ti]).delete().neq('id','');
    mkToast('\u041E\u0447\u0438\u0449\u0435\u043D\u043E');
  }catch(e){mkToast('\u041F\u043E\u043C\u0438\u043B\u043A\u0430','error');}
}

// =
// APP START
// =
async function startApp(){
  document.getElementById('ls').style.display='none';
  document.getElementById('as').style.display='block';
  await loadAll();
  startChannels();
  buildSidebar(); updateSBUser(); updateBranchSelector();
  // Set role class on body for CSS-based hiding
  document.body.className = document.body.className.replace(/\brole-\w+\b/g, '');
  document.body.classList.add('role-' + (CU ? CU.role : 'tutor'));
  // Restore last visited page
  var lastPage = '';
  try{ lastPage = localStorage.getItem('sb_page')||''; }catch(e){}
  var allowedPages = userNav();
  if(lastPage && allowedPages.indexOf(lastPage) >= 0){
    nav(lastPage);
  } else {
    nav('dashboard');
  }
}

// Keyboard

document.addEventListener('keydown',function(e){
  if(e.key==='Enter'&&document.getElementById('ls').style.display!=='none') doLogin();
  if(e.key==='Escape') document.querySelectorAll('.mo').forEach(function(m){m.style.display='none';});
});

// Expose key functions to window scope explicitly
window.doLogin   = doLogin;
window.doLogout  = doLogout;
window.openStudM = openStudM;
window.openTutM  = openTutM;
window.openLessM = openLessM;
window.openPayM  = openPayM;
window.openCommM = openCommM;
window.openUserM = openUserM;
window.saveUser  = saveUser;
window.saveStudent = saveStudent;
window.saveTutor = saveTutor;
window.saveLesson = saveLesson;
window.savePayment = savePayment;
window.saveComm  = saveComm;
window.saveSettings = saveSettings;
window.nav       = nav;
window.closeM    = closeM;
window.openM     = openM;
window.delStudent = delStudent;
window.delTutor  = delTutor;
window.delLesson = delLesson;
window.delPay    = delPay;
window.delComm   = delComm;
window.openAdd   = openAdd;
window.chWk      = chWk;
window.schSetView = schSetView;
window.toggleRecurOpts = toggleRecurOpts;
window.previewRecur = previewRecur;
window.sfilt     = sfilt;
window.dashKpiWeek = dashKpiWeek;
window.setBranch = setBranch;
window.clearData = clearData;
window.exportBackup = exportBackup;
window.importBackupClick = importBackupClick;
window.importBackup = importBackup;
window.addSubj   = addSubj;
window.delSubj   = delSubj;
window.addBranch = addBranch;
window.delBranch = delBranch;
window.editBranch = editBranch;
window.savePriceRule = savePriceRule;
window.editPriceRule = editPriceRule;
window.delPriceRule = delPriceRule;
window.uaTab     = uaTab;
window.openUserAccessM = openUserAccessM;
window.resetAllUserAccess = resetAllUserAccess;
window.gcTab     = gcTab;
window.gcAddNavItem = gcAddNavItem;
window.gcAddField = gcAddField;
window.gcResetNav = gcResetNav;
window.gcResetRoles = gcResetRoles;
window.gcResetLabels = gcResetLabels;
window.gcDelField = gcDelField;
window.gcSaveLabels = gcSaveLabels;
window.gSearch   = gSearch;
window.toggleTutLink = toggleTutLink;
window.updateParentInfo = updateParentInfo;
window.toggleProfileEdit = toggleProfileEdit;
window.saveProfileEdit = saveProfileEdit;
window.doDelLesson = doDelLesson;
window.saveCustomPageNotes = saveCustomPageNotes;
window.renderAnalytics = renderAnalytics;

// Boot
document.addEventListener('DOMContentLoaded', initApp);

// Tutor checkbox visual feedback
document.addEventListener('change', function(e){
  if(e.target && e.target.classList.contains('st-tutor-cb')){
    var lbl = e.target.closest('label');
    if(lbl){
      lbl.style.background = e.target.checked ? 'rgba(41,171,226,.15)' : 'var(--s1)';
      lbl.style.borderColor = e.target.checked ? 'var(--adm)' : 'var(--b1)';
    }
  }
});


