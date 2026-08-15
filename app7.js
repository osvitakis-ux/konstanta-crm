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

var ROLES = {
  god: {
    label:'\u0411\u043E\u0433 \u0441\u0438\u0441\u0442\u0435\u043C\u0438', icon:'\u26A1', color:'var(--god2)',
    avatarBg:'linear-gradient(135deg,#2e3192,#5b60d4)',
    nav:['dashboard','students','tutors','schedule','lessons','comms','payments','payroll','acts','crm','tasks','audit','invoice-log','invoice','reports','users','settings','telephony'],
    can:{students:true,tutors:true,lessons:true,payments:true,users:true,settings:true,danger:true,deleteAny:true},
    seeIncome:true, seeAll:true, canEditUsers:true, showGodBanner:true
  },
  director: {
    label:'\u0414\u0438\u0440\u0435\u043A\u0442\u043E\u0440', icon:'\uD83D\uDC51', color:'var(--dir)',
    avatarBg:'linear-gradient(135deg,#d9e021,#fcee21)',
    nav:['dashboard','students','tutors','schedule','lessons','comms','payments','payroll','acts','crm','tasks','invoice','reports','users','settings','telephony'],
    can:{students:true,tutors:true,lessons:true,payments:true,users:true,settings:true,danger:false,deleteAny:true},
    seeIncome:true, seeAll:true, canEditUsers:true, showGodBanner:false
  },
  admin: {
    label:'\u0410\u0434\u043C\u0456\u043D\u0456\u0441\u0442\u0440\u0430\u0442\u043E\u0440', icon:'\uD83D\uDEE1\uFE0F', color:'var(--adm)',
    avatarBg:'linear-gradient(135deg,#29abe2,#3fa9f5)',
    nav:['dashboard','students','tutors','schedule','lessons','comms','crm','tasks','invoice','reports','telephony'],
    can:{students:true,tutors:true,lessons:true,comms:true,payments:false,users:false,settings:false,danger:false,deleteAny:true},
    seeIncome:false, seeAll:true, canEditUsers:false, showGodBanner:false
  },
  network_admin: {
    label:'\u0410\u0434\u043C\u0456\u043D \u043C\u0435\u0440\u0435\u0436\u0456', icon:'\uD83C\uDF10', color:'var(--god2)',
    avatarBg:'linear-gradient(135deg,#5b60d4,#29abe2)',
    nav:['dashboard','students','tutors','schedule','lessons','comms','payments','crm','tasks','invoice','reports','users','settings'],
    can:{students:true,tutors:true,lessons:true,payments:true,users:true,settings:true,danger:false,deleteAny:true},
    seeIncome:true, seeAll:true, canEditUsers:true, showGodBanner:false
  },
  tutor: {
    label:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440', icon:'\uD83D\uDCDA', color:'var(--tut)',
    avatarBg:'linear-gradient(135deg,#22b573,#7ac943)',
    nav:['dashboard','students','schedule','lessons','comms','tasks','profile'],
    can:{students:false,tutors:false,lessons:true,comms:true,payments:false,users:false,settings:false,danger:false,deleteAny:false},
    seeIncome:false, seeAll:false, canEditUsers:false, showGodBanner:false
  },
  };

var NAV_CFG = [
  {id:'dashboard',  ico:'\u229E',  lbl:'\u0414\u0430\u0448\u0431\u043E\u0440\u0434',     sec:'\u0413\u043E\u043B\u043E\u0432\u043D\u0435'},
  {id:'students',   ico:'\u25CE',  lbl:'\u0423\u0447\u043D\u0456',         sec:'\u0413\u043E\u043B\u043E\u0432\u043D\u0435', badge:true},
  {id:'tutors',     ico:'\u25C8',  lbl:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438',    sec:'\u0413\u043E\u043B\u043E\u0432\u043D\u0435'},
  {id:'schedule',   ico:'\u25A6',  lbl:'\u0420\u043E\u0437\u043A\u043B\u0430\u0434',      sec:'\u041D\u0430\u0432\u0447\u0430\u043D\u043D\u044F'},
  {id:'lessons',    ico:'\u25C9',  lbl:'\u0417\u0430\u043D\u044F\u0442\u0442\u044F',      sec:'\u041D\u0430\u0432\u0447\u0430\u043D\u043D\u044F'},
  {id:'comms',      ico:'\u25CE',  lbl:'\u041A\u043E\u043C\u0443\u043D\u0456\u043A\u0430\u0446\u0456\u0457', sec:'\u041D\u0430\u0432\u0447\u0430\u043D\u043D\u044F'},
  {id:'payments',   ico:'\u25C8',  lbl:'\u041E\u043F\u043B\u0430\u0442\u0430',       sec:'\u0424\u0456\u043D\u0430\u043D\u0441\u0438'},
  {id:'payroll',    ico:'\u20B4',  lbl:'\u0417\u0430\u0440\u043F\u043B\u0430\u0442\u0438',     sec:'\u0424\u0456\u043D\u0430\u043D\u0441\u0438'},
  {id:'invoice',    ico:'\u25C8',  lbl:'\u0420\u0430\u0445\u0443\u043D\u043E\u043A',  sec:'\u0424\u0456\u043D\u0430\u043D\u0441\u0438'},
  {id:'reports',    ico:'\u25E7',  lbl:'\u0410\u043D\u0430\u043B\u0456\u0442\u0438\u043A\u0430',    sec:'\u0424\u0456\u043D\u0430\u043D\u0441\u0438'},
  {id:'crm',        ico:'\u25A4',  lbl:'CRM',              sec:'\u041C\u0435\u043D\u0435\u0434\u0436\u043C\u0435\u043D\u0442'},
  {id:'tasks',      ico:'\u2611',  lbl:'\u0417\u0430\u0432\u0434\u0430\u043D\u043D\u044F',      sec:'\u041C\u0435\u043D\u0435\u0434\u0436\u043C\u0435\u043D\u0442'},
  {id:'audit',      ico:'\uD83D\uDD0D',  lbl:'\u0406\u0441\u0442\u043E\u0440\u0456\u044F \u0437\u043C\u0456\u043D',   sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'invoice-log',ico:'\uD83D\uDCCB',  lbl:'\u041B\u043E\u0433 \u0440\u0430\u0445\u0443\u043D\u043A\u0456\u0432', sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'acts',       ico:'\uD83D\uDCC4',  lbl:'\u0410\u043A\u0442\u0438 \u0440\u043E\u0431\u0456\u0442',    sec:'\u0424\u0456\u043D\u0430\u043D\u0441\u0438'},
  {id:'users',      ico:'\u25CE',  lbl:'\u0410\u043A\u0430\u0443\u043D\u0442\u0438',      sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'branches',   ico:'\uD83C\uDFE2',  lbl:'\u0424\u0456\u043B\u0456\u0457',         sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'telephony',  ico:'\u25C9',  lbl:'\u0422\u0435\u043B\u0435\u0444\u043E\u043D\u0456\u044F', sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'settings',   ico:'\u25C9',  lbl:'\u041D\u0430\u043B\u0430\u0448\u0442\u0443\u0432\u0430\u043D\u043D\u044F', sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'profile',    ico:'\u25A3',  lbl:'\u041C\u0456\u0439 \u043F\u0440\u043E\u0444\u0456\u043B\u044C', sec:'\u041E\u0441\u043E\u0431\u0438\u0441\u0442\u0435'},
];

var DEFAULT_NAV_CFG = NAV_CFG;

var PLABELS={dashboard:'Дашборд',students:'Учні',tutors:'Репетитори',schedule:'Розклад',lessons:'Заняття',payments:'Оплата',reports:'Аналітика',users:'Акаунти',settings:'Налаштування',profile:'Мій профіль',crm:'CRM',analytics:'Статистика',comms:'Комунікації',missed:'Пропущені уроки',invoice:'Рахунок',branches:'Філії',telephony:'Телефонія',tasks:'Завдання',payroll:'Зарплати',audit:'Історія змін',acts:'Акти робіт'};

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
  if(mkWrap) mkWrap.style.display=(stat==='makeup'||stat==='makeup_planned')?'block':'none';
  if(msWrap) msWrap.style.display=(stat==='missed'||stat==='makeup'||stat==='makeup_planned')?'block':'none';
  var canSplit=dur>=60 && (stat==='missed'||stat==='makeup'||stat==='makeup_planned');
  if(spWrap) spWrap.style.display=canSplit?'block':'none';
  // Блок об'єднання — показуємо завжди для будь-якого статусу
  var mergeWrap=document.getElementById('l-merge-wrap');
  if(mergeWrap) mergeWrap.style.display='block';
  populateMergeSelect();
  // Ініціалізуємо поле частин якщо порожнє
  var splitInput=document.getElementById('split-parts-input');
  if(splitInput && canSplit && !splitInput.value) splitInput.value='30, 60';
  if(canSplit) updateSplitPreview();
}

function renderCommsPage(){
  var tbody=document.getElementById('comms-tbody');
  if(!tbody)return;

  // Populate student filter (popSelSearch сам зберігає поточне значення)
  var fStudSel=document.getElementById('comm-f-student');
  if(fStudSel){
    popSelSearch('comm-f-student', [{id:'',fn:'\u0412\u0441\u0456 \u0443\u0447\u043d\u0456',ln:''}].concat(myStudents().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');})), 'id', function(s){return s.fn+(s.ln?' '+s.ln:'');}, '');
  }

  // Populate tutor filter
  var fTutSel=document.getElementById('comm-f-tutor');
  if(fTutSel && R()!=='tutor'){
    popSelSearch('comm-f-tutor', [{id:'',fn:'\u0412\u0441\u0456 \u0440\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0438',ln:''}].concat(S.tutors||[]), 'id', function(t){return t.fn+(t.ln?' '+t.ln:'');}, '');
  }

  // Читаємо значення ПІСЛЯ оновлення списків
  var fStud =(document.getElementById('comm-f-student')||{value:''}).value;
  var fTutor=(document.getElementById('comm-f-tutor')||{value:''}).value;
  var fType =(document.getElementById('comm-f-type')||{value:''}).value;

  var _selfId=null;
  if(R()==='tutor'){ var _mt=myTutor(); if(_mt) _selfId=_mt.id; }
  var comms=[].concat(S.comms||[]).sort(function(a,b){return (b.date||'').localeCompare(a.date||'');});
  if(_selfId) comms=comms.filter(function(c){return (c.tutorId||c.tutor_id)===_selfId;});
  if(fStud) comms=comms.filter(function(c){return (c.studentId||c.student_id)===fStud;});
  if(fTutor) comms=comms.filter(function(c){return (c.tutorId||c.tutor_id)===fTutor;});
  if(fType) comms=comms.filter(function(c){
    var t=c.type||'';
    if(fType==='message') return t==='message'||t==='msg';
    if(fType==='meeting') return t==='meeting'||t==='meet';
    return t===fType;
  });
  var ico={call:'📞',message:'💬',meeting:'🤝',email:'📧',other:'📋',msg:'💬',meet:'🤝'};
  if(!comms.length){
    tbody.innerHTML='<tr><td colspan="6" style="text-align:center;padding:20px;color:var(--t3)">Комунікацій немає</td></tr>';
    return;
  }
  tbody.innerHTML=comms.map(function(c){
    var tutor=(S.tutors||[]).find(function(t){return t.id===(c.tutorId||c.tutor_id);});
    var student=(S.students||[]).find(function(s){return s.id===(c.studentId||c.student_id);});
    var canDel=can('deleteAny')||(_selfId&&(c.tutorId||c.tutor_id)===_selfId);
    var actBtns=canDel
      ?('<button onclick="openCommM(null,\''+c.id+'\')" title="Редагувати" style="border:none;background:none;cursor:pointer;font-size:13px;padding:4px 6px;border-radius:6px">✏️</button>'
        +'<button onclick="delComm(\''+c.id+'\')" title="Видалити" style="border:none;background:none;cursor:pointer;font-size:14px;padding:4px 6px;border-radius:6px;color:var(--danger)">🗑</button>')
      :'';
    return '<tr><td style="font-size:11px;color:var(--t2)">'+fd(c.date)+'</td>'
      +'<td>'+(ico[c.type]||'📋')+' '+(c.type||'—')+'</td>'
      +'<td>'+(student?student.fn+' '+student.ln:'—')+'</td>'
      +'<td>'+(tutor?tutor.fn+' '+tutor.ln:'—')+'</td>'
      +'<td>'+(c.note||'—')+'</td>'
      +'<td style="text-align:right;white-space:nowrap">'+actBtns+'</td></tr>';
  }).join('');
}

function renderMissedLessons(){
  var tbody=document.getElementById('missed-tbody');
  if(!tbody)return;

  // Populate student filter
  var fStudSel=document.getElementById('missed-f-student');
  if(fStudSel){
    var curVal=fStudSel.value;
    popSelSearch('missed-f-student', [{id:'',fn:'\u0412\u0441\u0456 \u0443\u0447\u043D\u0456',ln:''}].concat(myStudents().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');})), 'id', function(s){return s.fn+(s.ln?' '+s.ln:'');}, '');
    if(curVal){ fStudSel.value=curVal; if(fStudSel._updateSearch) fStudSel._updateSearch(); }
  }
  var fStud=(fStudSel||{value:''}).value;
  var _myTc=R()==='tutor'?myTutor():null;

  // ALL missed lessons - full period, no date restriction
  var allMissed=(S.lessons||[]).filter(function(l){
    return l.status==='missed';
  });
  if(_myTc) allMissed=allMissed.filter(function(l){return (l.tutorId||l.tutor_id)===_myTc.id;});
  if(fStud) allMissed=allMissed.filter(function(l){return (l.studentId||l.student_id)===fStud;});
  allMissed=allMissed.sort(function(a,b){return (b.date||'').localeCompare(a.date||'');});

  if(!allMissed.length){
    tbody.innerHTML='<tr><td colspan="7" style="text-align:center;padding:20px;color:var(--t3)">Пропущених немає</td></tr>';
    return;
  }

  tbody.innerHTML=allMissed.map(function(l){
    var s=(S.students||[]).find(function(x){return x.id===(l.studentId||l.student_id);});
    var t=(S.tutors||[]).find(function(x){return x.id===(l.tutorId||l.tutor_id);});
    var covered=isCoveredMissed(l);

    // Find paired makeup lesson
    var makeupL=covered?(S.lessons||[]).find(function(x){
      return x.status==='makeup'
        && (x.studentId||x.student_id)===(l.studentId||l.student_id)
        && (x.tutorId||x.tutor_id)===(l.tutorId||l.tutor_id);
    }):null;

    var statusBadge=covered
      ?'<span style="color:#22c55e;font-weight:600">✅ Відпрацьовано</span>'
      :'<span style="color:#ef4444;font-weight:600">❌ Не відпрацьовано</span>';

    var makeupDateStr=l.makeup_date?fd(l.makeup_date)
      :(makeupL?fd(makeupL.date):'—');

    return '<tr style="'+(covered?'opacity:.6':'')+'"><td style="font-size:11px">'+fd(l.date)+'</td>'
      +'<td><b>'+(s?s.fn+' '+s.ln:'—')+'</b></td>'
      +'<td style="font-size:11px">'+(t?t.fn+' '+t.ln:'—')+'</td>'
      +'<td style="font-size:11px">'+(l.subject||'—')+'</td>'
      +'<td>'+statusBadge+'</td>'
      +'<td style="font-size:11px">'+fd(l.date)+'</td>'
      +'<td style="font-size:11px">'+makeupDateStr+'</td></tr>';
  }).join('');
}
async function deleteLessonFromModal(){
  if(!S.editId)return;
  if(!confirm('Видалити цей урок?'))return;
  var _id=S.editId;
  closeM('mo-lesson');
  try{ await dbDelete('lessons',_id); }catch(e){}
}

async function deleteLessonSeriesFromModal(){
  if(!S.editId)return;
  var l=(S.lessons||[]).find(function(x){return x.id===S.editId;});
  if(!l||!l.recurId){mkToast('Немає серії','error');return;}
  var series=S.lessons.filter(function(x){return x.recurId===l.recurId;});
  if(!confirm('Видалити всю серію? ('+series.length+' уроків)'))return;
  closeM('mo-lesson');
  try{
    // Через dbDelete (не напряму _sb) — щоб зберегти запис в Історії змін,
    // повтор при протермінованому токені й захист від закриття вкладки під час видалення
    for(var i=0;i<series.length;i++) await dbDelete('lessons',series[i].id);
    mkToast('Серію видалено'); if(S.currentPage==='schedule') renderSch();
  }catch(e){mkToast('Помилка: '+e.message,'error');}
}

function scheduleDailyRatingUpdate(){}

window.onLessStatChange = onLessStatChange;
window.renderCommsPage = renderCommsPage;
window.renderMissedLessons = renderMissedLessons;
window.deleteLessonFromModal = deleteLessonFromModal;
window.deleteLessonSeriesFromModal = deleteLessonSeriesFromModal;



// ═══════════════════════════════════
// ДОДАТКОВІ ФУНКЦІЇ — рахунки, viber, розбивка
// ═══════════════════════════════════

function updateInvPhone(){
  var sid=(document.getElementById('inv-student')||{value:''}).value;
  var wrap=document.getElementById('inv-phone-wrap');
  if(!sid||!wrap){if(wrap)wrap.style.display='none';return;}
  var s=(S.students||[]).find(function(x){return x.id===sid;});
  wrap.style.display=(s&&(s.phone||s.parentPhone))?'block':'none';
  var ph=document.getElementById('inv-phone');
  if(ph&&s) ph.value=s.phone||s.parentPhone||'';
}

function openViberContact(){
  var sid=(document.getElementById('inv-student')||{value:''}).value;
  var s=(S.students||[]).find(function(x){return x.id===sid;});
  var phone=(s&&(s.phone||s.parentPhone)||'').replace(/\D/g,'');
  if(phone) window.open('viber://chat?number='+phone);
  else mkToast('Немає телефону','error');
}

function sendViberFromPanel(){
  var sid=(document.getElementById('inv-student')||{value:''}).value;
  var s=(S.students||[]).find(function(x){return x.id===sid;});
  if(!s){mkToast('Оберіть учня','error');return;}
  var phone=((s.phone||s.parentPhone)||'').replace(/\D/g,'');
  if(phone) window.open('viber://chat?number='+phone);
  else mkToast('Немає телефону','error');
}

function openBranchM(id){
  var b=id?(S.branches||[]).find(function(x){return x.id===id;}):null;
  ['name','addr','phone','email'].forEach(function(f){
    var el=document.getElementById('br-'+f);
    if(el) el.value=b?(b[f]||b[f.replace('addr','address')]||''):'';
  });
  ['pay_recipient','pay_address','pay_card','pay_bank','pay_edrpou','pay_purpose'].forEach(function(f){
    var el=document.getElementById('br-'+f.replace(/_/g,'-'));
    if(el) el.value=b?(b[f]||''):'';
  });
  S.editId=id||null;
  openM('mo-branch');
}

async function saveBranchModal(){
  var name=(document.getElementById('br-name')||{value:''}).value.trim();
  if(!name){mkToast('Введіть назву','error');return;}
  var obj={
    name:name,
    address:(document.getElementById('br-addr')||{value:''}).value,
    phone:(document.getElementById('br-phone')||{value:''}).value,
    email:(document.getElementById('br-email')||{value:''}).value,
    pay_recipient:(document.getElementById('br-pay-recipient')||{value:''}).value,
    pay_address:(document.getElementById('br-pay-address')||{value:''}).value,
    pay_card:(document.getElementById('br-pay-card')||{value:''}).value,
    pay_bank:(document.getElementById('br-pay-bank')||{value:''}).value,
    pay_edrpou:(document.getElementById('br-pay-edrpou')||{value:''}).value,
    pay_purpose:(document.getElementById('br-pay-purpose')||{value:''}).value
  };
  try{
    if(S.editId) await dbUpdate('branches',S.editId,obj);
    else{obj.id=uid();await dbInsert('branches',obj);}
    mkToast('Збережено');
    closeM('mo-branch');
  }catch(e){mkToast('Помилка: '+e.message,'error');}
}

async function splitLessonToChunks(chunkMin){
  chunkMin = chunkMin||30;
  var id=S.editId;
  if(!id){mkToast('Не знайдено урок','error');return;}
  var orig=(S.lessons||[]).find(function(l){return l.id===id;});
  if(!orig){mkToast('Урок не знайдено','error');return;}
  var curDur=parseInt((document.getElementById('l-dur')||{value:'60'}).value)||parseInt(orig.dur)||60;
  var nParts=Math.floor(curDur/chunkMin);
  if(nParts<2){mkToast('Тривалість мінімум '+(chunkMin*2)+' хв для розбиття на '+chunkMin+' хв','error');return;}
  if(!confirm('Розбити ('+curDur+' хв) на '+nParts+' × '+chunkMin+' хв?'))return;
  var lt=orig.time||'10:00';
  var lh0=parseInt(lt.split(':')[0]);
  var lm0=parseInt(lt.split(':')[1]||'0');
  var base={
    student_id:orig.studentId||orig.student_id,
    tutor_id:orig.tutorId||orig.tutor_id,
    subject:orig.subject||'',date:orig.date,
    status:orig.status||'missed',dur:chunkMin,
    price:orig.price||0, // ставка за годину лишається незмінною — сума частин збігається через тривалості
    branch_id:orig.branchId||orig.branch_id||null,
    split_group_id:id,split_index:0
  };
  try{
    await dbUpdate('lessons',id,{dur:chunkMin,price:base.price,split_group_id:id,split_index:0});
    // Оновлюємо локально одразу — щоб розклад/список занять показали розбиття без затримки
    Object.assign(orig,{dur:chunkMin,price:base.price,split_group_id:id,split_index:0});
    for(var p=1;p<nParts;p++){
      var totalMins=lm0+chunkMin*p;
      var newH=lh0+Math.floor(totalMins/60);
      var newM=totalMins%60;
      var newTime=String(newH).padStart(2,'0')+':'+String(newM).padStart(2,'0');
      var _chunk=Object.assign({},base,{id:uid(),time:newTime,split_index:p});
      await dbInsert('lessons',_chunk);
      S.lessons.push(normalizeLesson(_chunk));
    }
    mkToast('Розбито на '+nParts+' × '+chunkMin+' хв');
    closeM('mo-lesson');
    refreshPage('lessons'); if(S.currentPage==='schedule') renderSch();
  }catch(e){mkToast('Помилка: '+e.message,'error');}
}
function splitLessonTo30(){ return splitLessonToChunks(30); }
function splitLessonTo60(){ return splitLessonToChunks(60); }

function splitPreset(val){
  var el=document.getElementById('split-parts-input');
  if(el){ el.value=val; updateSplitPreview(); }
}

function updateSplitPreview(){
  var id=S.editId;
  var orig=id?(S.lessons||[]).find(function(l){return l.id===id;}):null;
  var curDur=parseInt((document.getElementById('l-dur')||{value:'60'}).value)||parseInt((orig||{}).dur)||60;
  var input=(document.getElementById('split-parts-input')?.value||'').trim();
  var preview=document.getElementById('split-preview');
  if(!preview) return;
  if(!input){preview.textContent='';return;}
  var parts=input.split(',').map(function(s){return parseInt(s.trim())||0;}).filter(function(v){return v>0;});
  if(!parts.length){preview.textContent='';return;}
  var sum=parts.reduce(function(a,b){return a+b;},0);
  if(sum!==curDur){
    preview.textContent='\u26A0\uFE0F \u0421\u0443\u043c\u0430 ('+sum+' \u0445\u0432) \u043d\u0435 \u0434\u043e\u0440\u0456\u0432\u043d\u044e\u0454 \u0437\u0430\u0433\u0430\u043b\u044c\u043d\u0456\u0439 \u0442\u0440\u0438\u0432\u0430\u043b\u043e\u0441\u0442\u0456 ('+curDur+' \u0445\u0432)';
    preview.style.color='var(--danger)';
  } else {
    preview.textContent='\u2713 '+parts.map(function(p,i){return '\u0427\u0430\u0441\u0442\u0438\u043d\u0430 '+(i+1)+': '+p+' \u0445\u0432';}).join(' + ');
    preview.style.color='var(--tut)';
  }
}

async function splitLessonCustom(){
  var id=S.editId;
  if(!id){mkToast('\u041d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e \u0443\u0440\u043e\u043a','error');return;}
  var orig=(S.lessons||[]).find(function(l){return l.id===id;});
  if(!orig){mkToast('\u0423\u0440\u043e\u043a \u043d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e','error');return;}
  var curDur=parseInt((document.getElementById('l-dur')||{value:'60'}).value)||parseInt(orig.dur)||60;
  var input=(document.getElementById('split-parts-input')?.value||'').trim();
  var parts=input.split(',').map(function(s){return parseInt(s.trim())||0;}).filter(function(v){return v>0;});
  if(parts.length<2){mkToast('\u0412\u043a\u0430\u0436\u0456\u0442\u044c \u043c\u0456\u043d\u0456\u043c\u0443\u043c 2 \u0447\u0430\u0441\u0442\u0438\u043d\u0438 \u0447\u0435\u0440\u0435\u0437 \u043a\u043e\u043c\u0443','error');return;}
  var sum=parts.reduce(function(a,b){return a+b;},0);
  if(sum!==curDur){mkToast('\u0421\u0443\u043c\u0430 \u0447\u0430\u0441\u0442\u0438\u043d ('+sum+' \u0445\u0432) \u043d\u0435 \u0434\u043e\u0440\u0456\u0432\u043d\u044e\u0454 \u0442\u0440\u0438\u0432\u0430\u043b\u043e\u0441\u0442\u0456 ('+curDur+' \u0445\u0432)','error');return;}
  if(!confirm('\u0420\u043e\u0437\u0431\u0438\u0442\u0438 ('+curDur+' \u0445\u0432) \u043d\u0430: '+parts.join(' + ')+' \u0445\u0432?')) return;
  var lt=orig.time||'10:00';
  var lh0=parseInt(lt.split(':')[0]);
  var lm0=parseInt(lt.split(':')[1]||'0');
  var base={
    student_id:orig.studentId||orig.student_id,
    tutor_id:orig.tutorId||orig.tutor_id,
    subject:orig.subject||'',date:orig.date,
    status:orig.status||'missed',
    price:orig.price||0, // зберігаємо оригінальну погодинну ставку — інакше частини перерахуються за поточним правилом студента, що може відрізнятись від фактичної ціни цього заняття
    branch_id:orig.branchId||orig.branch_id||null,
    split_group_id:id
  };
  try{
    await dbUpdate('lessons',id,{dur:parts[0],split_group_id:id,split_index:0});
    Object.assign(orig,{dur:parts[0],split_group_id:id,split_index:0});
    var offset=lm0+parts[0];
    for(var i=1;i<parts.length;i++){
      var nh=lh0+Math.floor(offset/60);
      var nm=offset%60;
      var t=String(nh).padStart(2,'0')+':'+String(nm).padStart(2,'0');
      var _chunk=Object.assign({},base,{id:uid(),time:t,dur:parts[i],split_index:i});
      await dbInsert('lessons',_chunk);
      S.lessons.push(normalizeLesson(_chunk));
      offset+=parts[i];
    }
    mkToast('\u0420\u043e\u0437\u0431\u0438\u0442\u043e: '+parts.join(' + ')+' \u0445\u0432');
    closeM('mo-lesson');
    refreshPage('lessons'); if(S.currentPage==='schedule') renderSch();
  }catch(e){mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+e.message,'error');}
}


function populateMergeSelect(){
  var sel=document.getElementById('l-merge-select');
  if(!sel) return;
  var id=S.editId;
  var orig=id?(S.lessons||[]).find(function(l){return l.id===id;}):null;
  if(!orig){ sel.innerHTML='<option value="">\u2014 \u043e\u0431\u0435\u0440\u0456\u0442\u044c \u0443\u0440\u043e\u043a \u2014</option>'; return; }

  var sid=orig.studentId||orig.student_id;
  var date=orig.date;
  var time=orig.time||'';

  // Шукаємо уроки того самого учня в той самий день (крім поточного)
  var candidates=(S.lessons||[]).filter(function(l){
    if(l.id===id) return false;
    if((l.studentId||l.student_id)!==sid) return false;
    if(l.date!==date) return false;
    return true;
  }).sort(function(a,b){return (a.time||'').localeCompare(b.time||'');});

  sel.innerHTML='<option value="">\u2014 \u043e\u0431\u0435\u0440\u0456\u0442\u044c \u0443\u0440\u043e\u043a \u2014</option>'
    +candidates.map(function(l){
      var tutor=(S.tutors||[]).find(function(t){return t.id===(l.tutorId||l.tutor_id);});
      var lbl=(l.time||'??:??')+' \u2014 '+(l.subject||'\u0431\u0435\u0437 \u043f\u0440\u0435\u0434\u043c\u0435\u0442\u0430')+' ('+(l.dur||60)+' \u0445\u0432)'+(tutor?' \u2014 '+tutor.fn+' '+tutor.ln:'');
      return '<option value="'+l.id+'">'+lbl+'</option>';
    }).join('');

  sel.onchange=function(){
    var preview=document.getElementById('l-merge-preview');
    if(!preview) return;
    if(!sel.value){preview.textContent='';return;}
    var other=(S.lessons||[]).find(function(l){return l.id===sel.value;});
    if(!other){preview.textContent='';return;}
    var totalDur=(parseFloat(orig.dur)||60)+(parseFloat(other.dur)||60);
    var earlierTime=[orig.time,other.time].sort()[0]||orig.time;
    preview.textContent='\u0420\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442: \u0443\u0440\u043e\u043a \u043e '+earlierTime+', \u0442\u0440\u0438\u0432\u0430\u043b\u0456\u0441\u0442\u044c '+totalDur+' \u0445\u0432';
  };
}

async function mergeLessons(){
  var id=S.editId;
  var orig=id?(S.lessons||[]).find(function(l){return l.id===id;}):null;
  if(!orig){mkToast('\u041d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e \u0443\u0440\u043e\u043a','error');return;}
  var sel=document.getElementById('l-merge-select');
  var otherId=sel?sel.value:'';
  if(!otherId){mkToast('\u041e\u0431\u0435\u0440\u0456\u0442\u044c \u0443\u0440\u043e\u043a \u0434\u043b\u044f \u043e\u0431\u2019\u0454\u0434\u043d\u0430\u043d\u043d\u044f','error');return;}
  var other=(S.lessons||[]).find(function(l){return l.id===otherId;});
  if(!other){mkToast('\u0423\u0440\u043e\u043a \u043d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e','error');return;}

  var totalDur=(parseFloat(orig.dur)||60)+(parseFloat(other.dur)||60);
  // Залишаємо той що раніше починається
  var keep=([orig,other].sort(function(a,b){return (a.time||'').localeCompare(b.time||'');}))[0];
  var remove=keep.id===orig.id?other:orig;

  if(!confirm('\u041e\u0431\u2019\u0454\u0434\u043d\u0430\u0442\u0438 \u0443\u0440\u043e\u043a\u0438 \u0432 \u043e\u0434\u0438\u043d \u0442\u0440\u0438\u0432\u0430\u043b\u0456\u0441\u0442\u044e '+totalDur+' \u0445\u0432?')) return;

  try{
    // Оновлюємо перший урок — нова тривалість, прибираємо split
    await dbUpdate('lessons',keep.id,{
      dur:totalDur,
      split_group_id:null,
      split_index:null
    });
    Object.assign(keep,{dur:totalDur,split_group_id:null,split_index:null,splitGroupId:null,splitIndex:null});
    // Видаляємо другий урок
    await dbDelete('lessons',remove.id);
    mkToast('\u0423\u0440\u043e\u043a\u0438 \u043e\u0431\u2019\u0454\u0434\u043d\u0430\u043d\u043e: '+totalDur+' \u0445\u0432');
    closeM('mo-lesson');
    refreshPage('lessons'); if(S.currentPage==='schedule') renderSch();
  }catch(e){mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+e.message,'error');}
}

window.mergeLessons=mergeLessons;
window.populateMergeSelect=populateMergeSelect;

/*
 * ПРАВИЛА РОЗРАХУНКУ РЕЙТИНГУ РЕПЕТИТОРА (4 тижні)
 * Базується на останніх 28 днях.
 *
 * ФОРМУЛА:
 *   Проведено  = done + completed + makeup (відпрацьовані = зараховуються)
 *   Пропущено  = missed (лише без відпрацювання)
 *   Відсоток   = Проведено / (Проведено + Пропущено) × 100
 *
 * ШКАЛА:
 *   ⭐⭐⭐⭐⭐ (5) — 90%+ і жодного непокритого пропуску
 *   ⭐⭐⭐⭐  (4) — 80–89% або є 1 пропуск але відпрацьований
 *   ⭐⭐⭐   (3) — 65–79%
 *   ⭐⭐    (2) — 40–64%
 *   ⭐     (1) — менше 40%
 *   Немає занять за 4 тижні → 5 (нейтральний)
 *
 * БОНУС: якщо всі пропущені мають makeup — штраф зменшується на 1 зірку
 */
function calcTutorRating(tutorId){
  var now=new Date(), fourWeeksAgo=new Date(now);
  fourWeeksAgo.setDate(now.getDate()-28);
  var from=localDateStr(fourWeeksAgo), today=localDateStr(now);

  var lessons=(S.lessons||[]).filter(function(l){
    return (l.tutorId||l.tutor_id)===tutorId && l.date>=from && l.date<=today;
  });

  if(!lessons.length) return 5; // немає занять — нейтральний рейтинг

  // Проведені: done, completed, makeup (відпрацювання теж рахується)
  var done   = lessons.filter(function(l){
    return isDoneLesson(l)||l.status==='makeup';
  }).length;

  // Пропущені без відпрацювання
  var missed = lessons.filter(function(l){ return l.status==='missed'; });
  var missedCount = missed.length;

  // Пропущені, які мають дату відпрацювання (makeup_date) — частково закриті
  var coveredMissed = missed.filter(function(l){ return l.makeup_date; }).length;
  var uncoveredMissed = missedCount - coveredMissed;

  var total = done + missedCount;
  var pct = Math.round(done / total * 100);

  // Базовий рейтинг по відсотку
  var rating;
  if(pct >= 90)      rating = 5;
  else if(pct >= 80) rating = 4;
  else if(pct >= 65) rating = 3;
  else if(pct >= 40) rating = 2;
  else               rating = 1;

  // Штраф: непокриті пропуски знижують рейтинг
  if(uncoveredMissed >= 3)      rating = Math.max(1, rating - 2);
  else if(uncoveredMissed >= 1) rating = Math.max(1, rating - 1);

  // Бонус: якщо всі пропуски відпрацьовані — рейтинг не штрафується
  if(missedCount > 0 && uncoveredMissed === 0) rating = Math.min(5, rating + 1);

  // Максимум 5 зірок тільки якщо взагалі немає непокритих пропусків
  if(uncoveredMissed > 0 && rating === 5) rating = 4;

  return rating;
}

async function updateAllTutorRatings(){
  if(!_sb||!CU) return;
  if(R()!=='god'&&R()!=='director'&&R()!=='admin') return;
  for(var i=0;i<(S.tutors||[]).length;i++){
    var t=S.tutors[i];
    var nr=calcTutorRating(t.id);
    if(nr!==t.rating){
      try{await _sb.from('tutors').update({rating:nr}).eq('id',t.id);t.rating=nr;}catch(e){}
    }
  }
}

async function logInvoice(channel,recipient,studentId,from,to,lessonsCount,total){
  if(!CU||!_sb||window._viewerMode) return;
  try{
    var r=await _sb.from('invoice_log').insert({
      sent_by:CU.id,student_id:studentId||null,
      period_from:from||null,period_to:to||null,
      lessons_count:lessonsCount||0,total_amount:total||0,
      channel:channel,recipient:recipient||'',branch_id:myBranchId()||null
    });
    if(r&&r.error) console.warn('[invoice_log] insert error:', r.error.message);
  }catch(e){ console.warn('[invoice_log] insert exception:', e&&e.message); }
}

async function renderInvoiceLog(){
  var tbody=document.getElementById('inv-log-tbody');
  if(!tbody)return;
  // Фільтр по відправнику
  var uSel=document.getElementById('ilf-user');
  if(uSel){
    var prev=uSel.value;
    uSel.innerHTML='<option value="">\u0412\u0441\u0456 \u0432\u0456\u0434\u043F\u0440\u0430\u0432\u043D\u0438\u043A\u0438</option>'
      +(S.users||[]).slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');})
        .map(function(u){return '<option value="'+u.id+'">'+u.fn+' '+u.ln+'</option>';}).join('');
    uSel.value=prev;
  }
  var fUser=(uSel||{value:''}).value;
  try{
    function buildQ(){ var q=_sb.from('invoice_log').select('*').order('sent_at',{ascending:false}).limit(200); if(fUser) q=q.eq('sent_by',fUser); return q; }
    var res=await buildQ();
    if(res.error && await refreshIfExpired(res.error)) res=await buildQ();
    if(res.error)throw res.error;
    var rows=res.data||[];
    console.log('[invoice_log] rows returned:', rows.length);
    if(!rows.length){
      tbody.innerHTML='<tr><td colspan="7" style="text-align:center;padding:20px;color:var(--t3)">'
        +'\u0429\u0435 \u043D\u0435\u043C\u0430\u0454 \u0432\u0456\u0434\u043F\u0440\u0430\u0432\u043B\u0435\u043D\u0438\u0445 \u0440\u0430\u0445\u0443\u043D\u043A\u0456\u0432.<br>'
        +'<span style="font-size:11px">\u0417\u0430\u043F\u0438\u0441 \u0437\u2019\u044F\u0432\u043B\u044F\u0454\u0442\u044C\u0441\u044F \u043F\u0456\u0441\u043B\u044F \u0432\u0456\u0434\u043F\u0440\u0430\u0432\u043A\u0438 \u0440\u0430\u0445\u0443\u043D\u043A\u0443 \u0447\u0435\u0440\u0435\u0437 Viber / Telegram \u0430\u0431\u043E \u043A\u043E\u043F\u0456\u044E\u0432\u0430\u043D\u043D\u044F.</span><br>'
        +'<button class="btn btn-g btn-sm" style="margin-top:8px" onclick="invoiceLogSelfTest()">\ud83e\uddea \u041F\u0435\u0440\u0435\u0432\u0456\u0440\u0438\u0442\u0438 \u0437\u0430\u043F\u0438\u0441 \u0443 \u0431\u0430\u0437\u0443</button>'
        +'</td></tr>';
      return;
    }
    tbody.innerHTML=rows.map(function(r){
      var student=(S.students||[]).find(function(s){return s.id===r.student_id;});
      var sender=(S.users||[]).find(function(u){return u.id===r.sent_by;});
      var sentAt=r.sent_at?new Date(r.sent_at).toLocaleString('uk-UA',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'}):'—';
      var period=(r.period_from?fd(r.period_from):'')+(r.period_to?' – '+fd(r.period_to):'');
      return '<tr>'
        +'<td style="font-size:11px;color:var(--t2)">'+sentAt+'</td>'
        +'<td><b>'+(student?student.fn+' '+student.ln:'—')+'</b></td>'
        +'<td style="font-size:11px">'+period+'</td>'
        +'<td style="font-size:11px">'+(r.lessons_count||0)+' / '+(r.total_amount||0)+' грн</td>'
        +'<td><span style="font-size:11px;padding:2px 8px;border-radius:20px;background:rgba(41,171,226,.15);color:var(--adm)">'+(r.channel||'')+'</span></td>'
        +'<td style="font-size:11px">'+(r.recipient||'—')+'</td>'
        +'<td style="font-size:11px;color:var(--t2)">'+(sender?sender.fn+' '+sender.ln:'—')+'</td>'
        +'</tr>';
    }).join('');
  }catch(e){
    console.error('[invoice_log] read error:', e);
    tbody.innerHTML='<tr><td colspan="7" style="color:var(--danger);padding:16px;text-align:center">'
      +'\u041F\u043E\u043C\u0438\u043B\u043A\u0430 \u0447\u0438\u0442\u0430\u043D\u043D\u044F \u0437 \u0431\u0430\u0437\u0438: '+(e.message||e)+'<br>'
      +'<span style="font-size:11px;color:var(--t2)">\u0406\u043C\u043E\u0432\u0456\u0440\u043D\u043E \u0432\u0456\u0434\u0441\u0443\u0442\u043D\u044F \u0442\u0430\u0431\u043B\u0438\u0446\u044F invoice_log \u0430\u0431\u043E RLS-\u043F\u043E\u043B\u0456\u0442\u0438\u043A\u0430 SELECT. \u0414\u0438\u0432. \u0456\u043D\u0441\u0442\u0440\u0443\u043A\u0446\u0456\u044E.</span>'
      +'</td></tr>';
  }
}

// Самотест: пише тестовий запис у invoice_log і одразу читає назад — показує точну причину
async function invoiceLogSelfTest(){
  if(!_sb){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u0437\u2019\u0454\u0434\u043D\u0430\u043D\u043D\u044F','error'); return; }
  if(window._viewerMode){ mkToast('\u0420\u0435\u0436\u0438\u043C \u043F\u0435\u0440\u0435\u0433\u043B\u044F\u0434\u0443 \u2014 \u0437\u0430\u043F\u0438\u0441 \u043D\u0435\u0434\u043E\u0441\u0442\u0443\u043F\u043D\u0438\u0439','error'); return; }
  mkToast('\u0422\u0435\u0441\u0442\u0443\u0454\u043C\u043E\u2026');
  try{
    var ins=await _sb.from('invoice_log').insert({
      sent_by:CU?CU.id:null, student_id:null, channel:'test',
      recipient:'\u0441\u0430\u043C\u043E\u0442\u0435\u0441\u0442', lessons_count:0, total_amount:0,
      branch_id:myBranchId()||null
    });
    if(ins.error){ alert('\u274C \u0417\u0410\u041F\u0418\u0421 \u0417\u0410\u0411\u041B\u041E\u041A\u041E\u0412\u0410\u041D\u041E:\n\n'+ins.error.message+'\n\n\u0426\u0435 \u043E\u0437\u043D\u0430\u0447\u0430\u0454, \u0449\u043E RLS-\u043F\u043E\u043B\u0456\u0442\u0438\u043A\u0430 INSERT \u0432\u0456\u0434\u0441\u0443\u0442\u043D\u044F \u0430\u0431\u043E \u0442\u0430\u0431\u043B\u0438\u0446\u0456 \u043D\u0435\u043C\u0430\u0454.'); return; }
    var sel=await _sb.from('invoice_log').select('*').eq('channel','test').limit(5);
    if(sel.error){ alert('\u26A0 \u0417\u0430\u043F\u0438\u0441 \u041F\u0420\u041E\u0419\u0428\u041E\u0412, \u0430\u043B\u0435 \u0427\u0418\u0422\u0410\u041D\u041D\u042F \u0417\u0410\u0411\u041B\u041E\u041A\u041E\u0412\u0410\u041D\u041E:\n\n'+sel.error.message+'\n\n\u041F\u043E\u0442\u0440\u0456\u0431\u043D\u0430 RLS-\u043F\u043E\u043B\u0456\u0442\u0438\u043A\u0430 SELECT.'); return; }
    alert('\u2705 \u0411\u0430\u0437\u0430 \u043F\u0440\u0430\u0446\u044E\u0454! \u0417\u0430\u043F\u0438\u0441 \u0441\u0442\u0432\u043E\u0440\u0435\u043D\u043E \u0456 \u043F\u0440\u043E\u0447\u0438\u0442\u0430\u043D\u043E ('+(sel.data?sel.data.length:0)+' \u0442\u0435\u0441\u0442. \u0437\u0430\u043F\u0438\u0441\u0456\u0432).\n\n\u041E\u0442\u0436\u0435, \u043F\u0440\u043E\u0431\u043B\u0435\u043C\u0430 \u0431\u0443\u043B\u0430 \u0432 \u0442\u043E\u043C\u0443, \u0449\u043E \u0440\u0430\u0445\u0443\u043D\u043A\u0438 \u043D\u0435 \u0432\u0456\u0434\u043F\u0440\u0430\u0432\u043B\u044F\u043B\u0438\u0441\u044C \u043F\u0456\u0441\u043B\u044F \u043E\u043D\u043E\u0432\u043B\u0435\u043D\u043D\u044F. \u0412\u0456\u0434\u043F\u0440\u0430\u0432\u0442\u0435 \u0440\u0430\u0445\u0443\u043D\u043E\u043A \u2014 \u0456 \u0432\u0456\u043D \u0437\u2019\u044F\u0432\u0438\u0442\u044C\u0441\u044F \u0442\u0443\u0442.');
    renderInvoiceLog();
  }catch(e){ alert('\u041F\u043E\u043C\u0438\u043B\u043A\u0430: '+(e.message||e)); }
}
window.invoiceLogSelfTest=invoiceLogSelfTest;
window.renderInvoiceLog=renderInvoiceLog;

function renderInvoicePage(){
  var pg=document.getElementById('pg-invoice');
  if(!pg)return;

  // Populate student select
  var sSel=document.getElementById('inv-student');
  if(sSel){
    var cur=sSel.value;
    var invStudents=[{id:'',fn:'\u2014 \u043e\u0431\u0435\u0440\u0456\u0442\u044c \u0443\u0447\u043d\u044f \u2014',ln:''}].concat(myStudents().filter(function(s){return s.status==='active'||s.status==='trial';}).sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');}));
    popSelSearch('inv-student', invStudents, 'id', function(s){return s.fn+(s.ln?' '+s.ln:'');}, '');
    if(cur){ sSel.value=cur; if(sSel._updateSearch) sSel._updateSearch(); }
  }

  var sid=(sSel||{value:''}).value;
  var student=sid?(S.students||[]).find(function(x){return x.id===sid;}):null;
  var phone=student?(student.parentPhone||student.phone||''):'';

  // Populate branch select (requisites for payment)
  var brSel=document.getElementById('inv-branch');
  if(brSel){
    var curBr=brSel.value;
    var brOpts=(S.branches||[]).map(function(b){return '<option value="'+b.id+'"'+(b.id===curBr?' selected':'')+'>'+b.name+'</option>';}).join('');
    brSel.innerHTML=brOpts || '<option value="">\u0424\u0456\u043b\u0456\u0457 \u043d\u0435 \u0441\u0442\u0432\u043e\u0440\u0435\u043d\u043e</option>';
    if(!curBr && (S.branches||[]).length){
      var defBrId = myBranchId() || S.branches[0].id;
      brSel.value = defBrId;
    }
  }
  var bid=(brSel||{value:''}).value;
  var branch=bid?(S.branches||[]).find(function(b){return b.id===bid;}):null;

  var dateFrom=(document.getElementById('inv-from')||{value:''}).value;
  var dateTo=(document.getElementById('inv-to')||{value:''}).value;
  var fallbackPrice=parseFloat((document.getElementById('inv-price')||{value:'0'}).value)||0;

  var now2=new Date();
  var defaultFrom=localDateStr(now2);
  var defaultTo=localDateStr(new Date(now2.getFullYear(),now2.getMonth()+1,0));
  if(!dateFrom) dateFrom=defaultFrom;
  if(!dateTo) dateTo=defaultTo;

  // Тільки ЗАПЛАНОВАНІ заняття — рахунок виставляється наперед (передоплата)
  var validStatuses=['planned','scheduled'];
  var lessons=sid?(S.lessons||[]).filter(function(l){
    if((l.studentId||l.student_id)!==sid) return false;
    if(validStatuses.indexOf(l.status)<0) return false;
    if(l.date<dateFrom) return false;
    if(l.date>dateTo) return false;
    return true;
  }).sort(function(a,b){return a.date.localeCompare(b.date)||(a.time||'').localeCompare(b.time||'');}):[];

  // Вартість: знімок у занятті → ставка учня за правилами (предмет/репетитор) → резервна "Ціна за годину"
  function lessonCost(l){
    var dur=(parseFloat(l.dur)||60)/60;
    var rate;
    if(l.price!=null && l.price!=='' && !isNaN(parseFloat(l.price)) && parseFloat(l.price)>0) rate=parseFloat(l.price);
    else{
      var sr=studentRate(student, l.subject, l.tutorId||l.tutor_id);
      rate=sr>0?sr:fallbackPrice;
    }
    return Math.round(dur*rate*10)/10;
  }

  var totalHours=Math.round(lessons.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  var total=Math.round(lessons.reduce(function(s,l){return s+lessonCost(l);},0)*10)/10;

  var groups={};
  var groupOrder=[];
  lessons.forEach(function(l){
    var tid=l.tutorId||l.tutor_id;
    var tutor=tid?(S.tutors||[]).find(function(t){return t.id===tid;}):null;
    var subj=l.subject||'\u0406\u043d\u0448\u0435';
    var key=(tid||'')+'|'+subj;
    if(!groups[key]){
      groups[key]={tutor:tutor, subj:subj, lessons:[]};
      groupOrder.push(key);
    }
    groups[key].lessons.push(l);
  });

  var invText='';
  if(student&&lessons.length){
    var monthsNom=['\u0421\u0456\u0447\u0435\u043D\u044C','\u041B\u044E\u0442\u0438\u0439','\u0411\u0435\u0440\u0435\u0437\u0435\u043D\u044C','\u041A\u0432\u0456\u0442\u0435\u043D\u044C','\u0422\u0440\u0430\u0432\u0435\u043D\u044C','\u0427\u0435\u0440\u0432\u0435\u043D\u044C','\u041B\u0438\u043F\u0435\u043D\u044C','\u0421\u0435\u0440\u043F\u0435\u043D\u044C','\u0412\u0435\u0440\u0435\u0441\u0435\u043D\u044C','\u0416\u043E\u0432\u0442\u0435\u043D\u044C','\u041B\u0438\u0441\u0442\u043E\u043F\u0430\u0434','\u0413\u0440\u0443\u0434\u0435\u043D\u044C'];
    var monthsGen=['\u0441\u0456\u0447\u043D\u044F','\u043B\u044E\u0442\u043E\u0433\u043E','\u0431\u0435\u0440\u0435\u0437\u043D\u044F','\u043A\u0432\u0456\u0442\u043D\u044F','\u0442\u0440\u0430\u0432\u043D\u044F','\u0447\u0435\u0440\u0432\u043D\u044F','\u043B\u0438\u043F\u043D\u044F','\u0441\u0435\u0440\u043F\u043D\u044F','\u0432\u0435\u0440\u0435\u0441\u043D\u044F','\u0436\u043E\u0432\u0442\u043D\u044F','\u043B\u0438\u0441\u0442\u043E\u043F\u0430\u0434\u0430','\u0433\u0440\u0443\u0434\u043D\u044F'];
    var _fromD=new Date(dateFrom+'T00:00:00');
    var invMonthNom=monthsNom[_fromD.getMonth()];
    var dueDate=new Date(_fromD.getFullYear(), _fromD.getMonth(), 1);
    var dueStr='1 '+monthsGen[dueDate.getMonth()];
    var cfg=S.settings||{};
    var centerName=cfg.name||'\u041A\u043E\u043D\u0441\u0442\u0430\u043D\u0442\u0430';

    var lines=[];
    lines.push('\u0412\u0430\u0441 \u0432\u0456\u0442\u0430\u0454 \u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0441\u044C\u043A\u0438\u0439 \u0446\u0435\u043D\u0442\u0440 '+centerName+'! \u0420\u0430\u0445\u0443\u043D\u043E\u043A \u043D\u0430 '+invMonthNom+':');

    groupOrder.forEach(function(key){
      var g=groups[key];
      var gCount=g.lessons.length;
      var gTotal=Math.round(g.lessons.reduce(function(s,l){return s+lessonCost(l);},0)*100)/100;
      // Ставка за годину для показу в рядку "N уроків * ЦІНА грн/год" — беремо ставку групи
      // (усі уроки групи мають один предмет+репетитор, тож ставка з правил учня спільна)
      var gTid=g.tutor?g.tutor.id:null;
      var gRate=studentRate(student, g.subj, gTid);
      if(!gRate||gRate<=0){ var gHrsForRate=g.lessons.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0); gRate=gHrsForRate>0?Math.round(gTotal/gHrsForRate):0; }
      lines.push(g.subj);
      lines.push(gCount+' '+prPlural(gCount,'\u0443\u0440\u043E\u043A','\u0443\u0440\u043E\u043A\u0438','\u0443\u0440\u043E\u043A\u0456\u0432')+' * '+gRate+' \u0433\u0440\u043D/\u0433\u043E\u0434 =  '+gTotal+' \u0433\u0440\u043D.');
    });

    if(groupOrder.length>1){
      lines.push('\u0423\u0421\u042C\u041E\u0413\u041E \u0414\u041E \u0421\u041F\u041B\u0410\u0422\u0418: '+total+' \u0433\u0440\u043D.');
    }

    lines.push('\u041F\u043B\u0430\u0442\u0456\u0436 \u043F\u0440\u043E\u0432\u043E\u0434\u0438\u0442\u044C\u0441\u044F   \u0434\u043E '+dueStr+' \u0437\u0430 \u0442\u0430\u043A\u0438\u043C\u0438 \u0440\u0435\u043A\u0432\u0456\u0437\u0438\u0442\u0430\u043C\u0438:');
    if(branch){
      if(branch.pay_recipient) lines.push('\u041E\u0442\u0440\u0438\u043C\u0443\u0432\u0430\u0447: '+branch.pay_recipient);
      if(branch.pay_edrpou) lines.push('\u041A\u043E\u0434 \u043E\u0442\u0440\u0438\u043C\u0443\u0432\u0430\u0447\u0430: '+branch.pay_edrpou);
      if(branch.pay_card) lines.push('IBAN: '+branch.pay_card);
      if(branch.pay_bank) lines.push('\u041D\u0430\u0437\u0432\u0430 \u0431\u0430\u043D\u043A\u0443: '+branch.pay_bank);
    }
    var studentInitial=(student.fn?student.fn[0]+'.':'');
    lines.push('\u041F\u0440\u0438\u0437\u043D\u0430\u0447\u0435\u043D\u043D\u044F \u043F\u043B\u0430\u0442\u0435\u0436\u0443: \u0437\u0430 \u043E\u0441\u0432\u0456\u0442\u043D\u0456 \u043F\u043E\u0441\u043B\u0443\u0433\u0438, '+student.ln+' '+studentInitial+' \u0443\u0447\u043D\u044F');
    lines.push('\u0417\u0430\u0437\u0434\u0430\u043B\u0435\u0433\u0456\u0434\u044C \u0434\u044F\u043A\u0443\u0454\u043C\u043E \u0437\u0430 \u0432\u0447\u0430\u0441\u043D\u0443 \u0441\u043F\u043B\u0430\u0442\u0443 \ud83d\ude0a');

    invText=lines.join('\n');
  }

  var invEl=document.getElementById('invoice-content');
  if(!invEl)return;

  var branchSelHtml='<div class="fgr" style="margin-bottom:10px"><label>\uD83C\uDFE2 \u0424\u0456\u043b\u0456\u044f (\u0440\u0435\u043a\u0432\u0456\u0437\u0438\u0442\u0438 \u043e\u043f\u043b\u0430\u0442\u0438)</label><select id="inv-branch" onchange="renderInvoicePage()" style="font-size:12px"></select></div>';

  var leftCol = '<div>'
    + branchSelHtml
    + '<div class="fgr" style="margin-bottom:10px"><label>\u0412\u0456\u0434 \u0434\u0430\u0442\u0438</label><input type="date" id="inv-from" value="'+dateFrom+'" onchange="renderInvoicePage()" style="font-size:12px"></div>'
    + '<div class="fgr" style="margin-bottom:10px"><label>\u0414\u043e \u0434\u0430\u0442\u0438</label><input type="date" id="inv-to" value="'+dateTo+'" onchange="renderInvoicePage()" style="font-size:12px"></div>'
    + '<div class="fgr" style="margin-bottom:10px"><label>\u0426\u0456\u043d\u0430 \u0437\u0430 \u0433\u043e\u0434\u0438\u043d\u0443 \u20b4 (\u044f\u043a\u0449\u043e \u0432 \u0437\u0430\u043d\u044f\u0442\u0442\u0456 \u043d\u0435 \u0432\u043a\u0430\u0437\u0430\u043d\u0430)</label><input type="number" id="inv-price" value="'+fallbackPrice+'" onchange="renderInvoicePage()" style="font-size:12px" placeholder="400"></div>'
    + '<div style="font-size:11px;color:var(--t3);margin-bottom:10px">\uD83D\uDCA1 \u0420\u0430\u0445\u0443\u043d\u043e\u043a \u0444\u043e\u0440\u043c\u0443\u0454\u0442\u044c\u0441\u044f \u0442\u0456\u043b\u044c\u043a\u0438 \u0456\u0437 \u0417\u0410\u041f\u041b\u0410\u041d\u041e\u0412\u0410\u041d\u0418\u0425 \u0437\u0430\u043d\u044f\u0442\u044c (\u043f\u0435\u0440\u0435\u0434\u043e\u043f\u043b\u0430\u0442\u0430). \u042f\u043a\u0449\u043e \u0432 \u0437\u0430\u043d\u044f\u0442\u0442\u0456 \u0432\u043a\u0430\u0437\u0430\u043d\u0430 \u0432\u043b\u0430\u0441\u043d\u0430 \u0446\u0456\u043d\u0430 \u2014 \u0432\u043e\u043d\u0430 \u043c\u0430\u0454 \u043f\u0440\u0456\u043e\u0440\u0438\u0442\u0435\u0442. \u0420\u0430\u0445\u0443\u043d\u043e\u043a \u0430\u0432\u0442\u043e\u043c\u0430\u0442\u0438\u0447\u043d\u043e \u043e\u0431\u2019\u0454\u0434\u043d\u0443\u0454 \u0437\u0430\u043d\u044f\u0442\u0442\u044f \u0432\u0441\u0456\u0445 \u0440\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0456\u0432 \u0442\u0430 \u043f\u0440\u0435\u0434\u043c\u0435\u0442\u0456\u0432.</div>'
    + (student
      ? (phone?'<div style="font-size:12px;color:var(--t2);margin-bottom:6px">\uD83D\uDCF1 \u0422\u0435\u043b\u0435\u0444\u043e\u043d: <b>'+phone+'</b></div>':'<div style="color:var(--danger);font-size:12px;margin-bottom:6px">\u26A0\uFE0F \u0422\u0435\u043b\u0435\u0444\u043e\u043d \u043d\u0435 \u0432\u043a\u0430\u0437\u0430\u043d\u043e</div>')
      : '<div style="font-size:12px;color:var(--t3);margin-bottom:12px">\u041e\u0431\u0435\u0440\u0456\u0442\u044c \u0443\u0447\u043d\u044f \u0449\u043e\u0431 \u0441\u0444\u043e\u0440\u043c\u0443\u0432\u0430\u0442\u0438 \u0440\u0430\u0445\u0443\u043d\u043e\u043a</div>')
    + (student ? ((student.email||student.parent_email)?'<div style="font-size:12px;color:var(--t2);margin-bottom:12px">\u2709\uFE0F Email: <b>'+(student.email||student.parent_email)+'</b></div>':'<div style="color:var(--warn);font-size:12px;margin-bottom:12px">\u2709\uFE0F Email \u043d\u0435 \u0432\u043a\u0430\u0437\u0430\u043d\u043e (\u0434\u043e\u0434\u0430\u0439\u0442\u0435 \u0432 \u043a\u0430\u0440\u0442\u0446\u0456 \u0443\u0447\u043d\u044f)</div>') : '')
    + '<div style="display:flex;gap:8px;flex-wrap:wrap">'
    + (phone?'<button class="btn btn-p btn-sm" onclick="sendInvoiceViber()" style="background:#7360f2">\uD83D\uDFE3 Viber</button>':'')
    + (phone?'<button class="btn btn-p btn-sm" onclick="sendInvoiceTelegram()" style="background:#2196f3">\u2708\uFE0F Telegram</button>':'')
    + (student?'<button class="btn btn-p btn-sm" onclick="sendInvoiceEmail()" style="background:#ea4335">\u2709\uFE0F Email</button>':'')
    + '<button class="btn btn-g btn-sm" onclick="copyInvoiceText()">\uD83D\uDCCB \u041a\u043e\u043f\u0456\u044e\u0432\u0430\u0442\u0438</button>'
    + '</div>'
    + '</div>';

  // Редагований текст: якщо користувач правив, а параметри не змінились — зберігаємо його правки
  var _esc=function(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;');};
  var prevGen=window._invGenText||'';
  var keepEdit=(window._invEdited && window._invText!=null && invText===prevGen);
  var showText=keepEdit?window._invText:invText;
  if(!keepEdit) window._invEdited=false;
  window._invGenText=invText;

  var rightCol = '<div>'
    + '<textarea id="inv-text" oninput="window._invText=this.value;window._invEdited=true;" '
    + 'placeholder="'+(!sid?'Оберіть учня щоб побачити рахунок':'Немає запланованих занять за цей період')+'" '
    + 'style="width:100%;box-sizing:border-box;background:var(--s2);border-radius:10px;padding:14px;font-family:JetBrains Mono,monospace;font-size:12px;white-space:pre-wrap;line-height:1.6;min-height:260px;border:1px solid var(--b1);color:var(--t1);resize:vertical">'
    + _esc(showText)
    + '</textarea>'
    + '<div style="margin-top:8px;display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap">'
    + '<div style="font-size:12px;color:var(--t2)">Занять: '+lessons.length+' | Репетиторів: '+groupOrder.length+' | Годин: '+totalHours+' | Сума: <b>'+total+'₴</b></div>'
    + '<button class="btn btn-g btn-sm" onclick="window._invEdited=false;renderInvoicePage();" title="Повернути автоматичний текст">↺ Скинути</button>'
    + '</div>'
    + '</div>';

  invEl.innerHTML='<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">'+leftCol+rightCol+'</div>';

  // Populate branch select after innerHTML is set
  var brSel2=document.getElementById('inv-branch');
  if(brSel2){
    var brOpts2=(S.branches||[]).map(function(b){return '<option value="'+b.id+'"'+(b.id===bid?' selected':'')+'>'+b.name+'</option>';}).join('');
    brSel2.innerHTML=brOpts2 || '<option value="">\u0424\u0456\u043b\u0456\u0457 \u043d\u0435 \u0441\u0442\u0432\u043e\u0440\u0435\u043d\u043e</option>';
    if(bid) brSel2.value=bid;
  }

  window._invText=showText;
  window._invPhone=phone;
  window._invMeta={ studentId:sid||null, from:(document.getElementById('inv-from')||{}).value||dateFrom, to:(document.getElementById('inv-to')||{}).value||dateTo, lessons:lessons.length, total:total };
}

function _logInvoiceSend(channel, recipient){
  var m=window._invMeta||{};
  try{ logInvoice(channel, recipient, m.studentId, m.from, m.to, m.lessons, m.total); }catch(e){}
  // Оновити панель статусу з невеликою затримкою (щоб запис устиг зберегтись)
  setTimeout(function(){ try{ if(typeof renderInvoiceStatus==='function' && document.getElementById('inv-status-body')) renderInvoiceStatus(); }catch(e){} }, 1200);
}
function sendInvoiceViber(){
  var phone=(window._invPhone||'').replace(/\D/g,'');
  var text=encodeURIComponent(window._invText||'');
  if(!phone){mkToast('Немає телефону','error');return;}
  _logInvoiceSend('viber', phone);
  window.open('viber://chat?number='+phone+'&text='+text);
}

function sendInvoiceTelegram(){
  var phone=(window._invPhone||'').replace(/\D/g,'');
  var text=encodeURIComponent(window._invText||'');
  if(!phone){mkToast('Немає телефону','error');return;}
  // Try to open Telegram with phone
  _logInvoiceSend('telegram', phone);
  window.open('tg://resolve?phone='+phone+'&text='+text);
  // Fallback: copy to clipboard
  navigator.clipboard&&navigator.clipboard.writeText(window._invText||'').then(function(){
    mkToast('Текст скопійовано — вставте у Telegram','info');
  });
}

function copyInvoiceText(){
  var txt=window._invText||'';
  if(!txt){mkToast('Немає рахунку','error');return;}
  _logInvoiceSend('copy', (window._invMeta&&window._invMeta.studentId)?'—':'');
  navigator.clipboard?navigator.clipboard.writeText(txt).then(function(){mkToast('Скопійовано ✅');}):mkToast('Скопіюйте вручну','info');
}

window.renderInvoicePage=renderInvoicePage;
window.sendInvoiceViber=sendInvoiceViber;
window.sendInvoiceTelegram=sendInvoiceTelegram;
window.copyInvoiceText=copyInvoiceText;

// ══════════ СТАТУС РОЗСИЛКИ РАХУНКІВ ══════════
// Показує всіх учнів із запланованими заняттями за місяць і чи надіслано їм рахунок.
async function renderInvoiceStatus(){
  var body=document.getElementById('inv-status-body');
  if(!body) return;
  var perEl=document.getElementById('ivs-period');
  if(perEl && !perEl.value){ var n=new Date(); perEl.value=n.getFullYear()+'-'+String(n.getMonth()+1).padStart(2,'0'); }
  var per=(perEl&&perEl.value)||'';
  var filter=(document.getElementById('ivs-filter')||{value:''}).value;
  var brSelIv=document.getElementById('ivs-branch');
  var _canFilterBr=isSuperAdmin()||R()==='director'||R()==='admin';
  if(brSelIv && _canFilterBr){
    var _prevBrIv=brSelIv.value;
    brSelIv.innerHTML='<option value="">\u0412\u0441\u0456 \u0444\u0456\u043B\u0456\u0457</option>'
      +(S.branches||[]).slice().sort(function(a,b){return (a.name||'').localeCompare(b.name||'','uk');})
        .map(function(b){return '<option value="'+b.id+'">'+b.name+'</option>';}).join('');
    brSelIv.value=_prevBrIv;
  }
  var fBranchIv=_canFilterBr?(document.getElementById('ivs-branch')||{value:''}).value:'';
  if(!per){ body.innerHTML=''; return; }

  // Хто має заплановані заняття цього місяця (їм потрібен рахунок = передоплата)
  var need={};
  (S.lessons||[]).forEach(function(l){
    if(String(l.date||'').slice(0,7)!==per) return;
    if(!(l.status==='planned'||l.status==='scheduled'||!l.status)) return;
    var sid=l.studentId||l.student_id; if(!sid) return;
    if(!need[sid]) need[sid]={count:0};
    need[sid].count++;
  });

  // Витягуємо лог відправлень за цей місяць
  var sentMap={};
  try{
    var from=per+'-01';
    var toM=new Date(parseInt(per.split('-')[0]), parseInt(per.split('-')[1]), 0);
    var to=per+'-'+String(toM.getDate()).padStart(2,'0');
    function buildQ(){ return _sb.from('invoice_log').select('*').gte('period_from',from).lte('period_from',to+'T23:59:59').limit(1000); }
    var res=await buildQ();
    if(res.error && typeof refreshIfExpired==='function' && await refreshIfExpired(res.error)) res=await buildQ();
    // Простіший і надійніший варіант: тягнемо останні 500 і фільтруємо за датою відправлення в періоді
    if(res.error){
      res=await _sb.from('invoice_log').select('*').order('sent_at',{ascending:false}).limit(500);
    }
    (res.data||[]).forEach(function(r){
      var when=String(r.sent_at||'').slice(0,7);
      // Рахуємо рахунок «за цей місяць», якщо його період або дата відправлення потрапляють у місяць
      var pf=String(r.period_from||'').slice(0,7);
      if(r.student_id && (pf===per || when===per)){
        if(!sentMap[r.student_id]) sentMap[r.student_id]=[];
        sentMap[r.student_id].push(r);
      }
    });
  }catch(e){ console.warn('[inv-status] read error', e); }

  // Формуємо список: усі учні, кому потрібен рахунок
  // Показуємо ВСІХ активних учнів, а не лише тих, у кого є заняття цього
  // місяця. Інакше учні без занять (щойно додані, у відпустці тощо)
  // випадали зі списку, і рахунок їм ніхто не виставляв.
  var rows=(S.students||[])
    .filter(function(st){ return st.status==='active'||st.status==='trial'; })
    .map(function(st){
      return {
        st: st,
        sid: st.id,
        count: (need[st.id]||{}).count||0,
        sent: sentMap[st.id]||[]
      };
    });

  if(filter==='sent') rows=rows.filter(function(r){return r.sent.length;});
  if(filter==='notsent') rows=rows.filter(function(r){return !r.sent.length;});
  if(fBranchIv) rows=rows.filter(function(r){
    var st=r.st;
    if(Array.isArray(st.branchIds)&&st.branchIds.length) return st.branchIds.indexOf(fBranchIv)>=0;
    return (st.branchId||st.branch_id)===fBranchIv;
  });

  rows.sort(function(a,b){
    // не відіслані зверху, далі за іменем
    var sa=a.sent.length?1:0, sb=b.sent.length?1:0;
    if(sa!==sb) return sa-sb;
    return ((a.st.fn||'')+(a.st.ln||'')).localeCompare((b.st.fn||'')+(b.st.ln||''),'uk');
  });

  var sentCount=rows.filter(function(r){return r.sent.length;}).length;
  // Загальна кількість — за фактично показаними рядками, а не за тими,
  // у кого є заняття. Інакше лічильник показував менше, ніж у списку.
  var total=rows.length;

  var CH={viber:'\uD83D\uDFE3 Viber',telegram:'\u2708\uFE0F Telegram',email:'\u2709\uFE0F Email',copy:'\uD83D\uDCCB \u041a\u043e\u043f\u0456\u044f'};

  var head='<div style="display:flex;gap:16px;padding:8px 14px 12px;flex-wrap:wrap">'
    +'<div style="font-size:13px"><b style="font-size:20px;color:var(--tut)">'+sentCount+'</b> / '+total+' \u0432\u0456\u0434\u0456\u0441\u043b\u0430\u043d\u043e</div>'
    +'<div style="flex:1;min-width:120px;align-self:center"><div style="height:8px;background:var(--s3);border-radius:10px;overflow:hidden"><div style="height:100%;width:'+(total?Math.round(sentCount/total*100):0)+'%;background:var(--tut);transition:width .4s"></div></div></div>'
    +'</div>';

  if(!rows.length){ body.innerHTML=head+'<div style="padding:20px;text-align:center;color:var(--t3)">\u041d\u0435\u043c\u0430\u0454 \u0443\u0447\u043d\u0456\u0432 \u0456\u0437 \u0437\u0430\u043d\u044f\u0442\u0442\u044f\u043c\u0438 \u0446\u044c\u043e\u0433\u043e \u043c\u0456\u0441\u044f\u0446\u044f</div>'; return; }

  var list=rows.map(function(r){
    var isSent=r.sent.length>0;
    var last=isSent?r.sent.slice().sort(function(a,b){return String(b.sent_at).localeCompare(String(a.sent_at));})[0]:null;
    var av=isSent?'var(--tut)':'var(--warn)';
    var badge=isSent
      ? '<span style="background:rgba(34,181,115,.14);color:var(--tut);font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px">\u2705 \u0412\u0456\u0434\u0456\u0441\u043b\u0430\u043d\u043e</span>'
      : '<span style="background:rgba(230,126,34,.14);color:var(--warn);font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px">\u23f3 \u041d\u0435 \u0432\u0456\u0434\u0456\u0441\u043b\u0430\u043d\u043e</span>';
    var detail=isSent && last
      ? '<div style="font-size:11px;color:var(--t3)">'+(CH[last.channel]||last.channel||'')+' \u00b7 '+(last.sent_at?new Date(last.sent_at).toLocaleDateString('uk-UA'):'')+(r.sent.length>1?' \u00b7 \u0432\u0441\u044c\u043e\u0433\u043e '+r.sent.length:'')+'</div>'
      : '<div style="font-size:11px;color:var(--t3)">'+(r.count
          ? r.count+' \u0437\u0430\u043f\u043b. \u0437\u0430\u043d\u044f\u0442\u044c'
          : '\u2014 \u0431\u0435\u0437 \u0437\u0430\u043d\u044f\u0442\u044c \u0446\u044c\u043e\u0433\u043e \u043c\u0456\u0441\u044f\u0446\u044f')+'</div>';
    return '<div style="display:flex;align-items:center;gap:12px;padding:9px 14px;border-bottom:1px solid var(--s3)">'
      +'<div style="width:34px;height:34px;border-radius:10px;background:'+av+';color:#fff;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:13px;flex-shrink:0">'+((r.st.fn||' ')[0]+(r.st.ln||' ')[0]).toUpperCase()+'</div>'
      +'<div style="flex:1;min-width:0"><div style="font-weight:600;font-size:13px">'+r.st.fn+' '+r.st.ln+'</div>'+detail+'</div>'
      +badge
      +'<button class="btn btn-g btn-sm" onclick="invStatusPick(\''+r.sid+'\')" title="\u0421\u0444\u043e\u0440\u043c\u0443\u0432\u0430\u0442\u0438 \u0440\u0430\u0445\u0443\u043d\u043e\u043a">\u2192</button>'
      +'</div>';
  }).join('');

  body.innerHTML=head+list;
}
// Клік по «→» у статусі — обираємо учня у формі рахунку
function invStatusPick(sid){
  var sel=document.getElementById('inv-student');
  if(sel){ sel.value=sid; renderInvoicePage(); }
  var card=document.getElementById('pg-invoice');
  if(card) card.scrollIntoView({behavior:'smooth',block:'start'});
}
window.renderInvoiceStatus=renderInvoiceStatus;
window.invStatusPick=invStatusPick;
window.sendInvoiceEmail=sendInvoiceEmail;

// ══════════ АКТИ ВИКОНАНИХ РОБІТ ══════════
// Доступно лише богу та директору. Формується по учню за період на основі
// ПРОВЕДЕНИХ занять (done/completed/makeup) — тобто фактично наданих послуг.
// Технічне зауваження: точна юридична форма акта залежить від договору
// з клієнтом і системи оподаткування — узгодьте шаблон з бухгалтером
// перед першою розсилкою. Тут закладені обов'язкові реквізити первинного
// документа (назва, дата, сторони, зміст/обсяг послуг, підписи) за
// орієнтиром ст.9 Закону "Про бухгалтерський облік".
function canActs(){ return R()==='god'||R()==='director'; }

function actPeriod(){
  var el=document.getElementById('act-period');
  if(el&&el.value) return el.value;
  var n=new Date(); return n.getFullYear()+'-'+String(n.getMonth()+1).padStart(2,'0');
}

// Проведені послуги учня за період (done/completed/makeup — те, що реально надано)
function actLessonsFor(sid, period){
  return (S.lessons||[]).filter(function(l){
    if((l.studentId||l.student_id)!==sid) return false;
    if(String(l.date||'').slice(0,7)!==period) return false;
    return isDoneLesson(l)||l.status==='makeup';
  }).sort(function(a,b){return String(a.date).localeCompare(String(b.date));});
}

function actLogFor(sid, period){
  return (S.actLog||[]).filter(function(r){return (r.studentId||r.student_id)===sid && r.period===period;});
}

function renderActsPage(){
  var wrap=document.getElementById('acts-body');
  if(!wrap) return;
  if(!canActs()){ wrap.innerHTML='<div style="padding:20px;color:var(--t3)">\u0414\u043E\u0441\u0442\u0443\u043F \u043B\u0438\u0448\u0435 \u0434\u043B\u044F \u0431\u043E\u0433\u0430 \u0442\u0430 \u0434\u0438\u0440\u0435\u043A\u0442\u043E\u0440\u0430</div>'; return; }
  var per=actPeriod();
  var perEl=document.getElementById('act-period');
  if(perEl&&!perEl.value) perEl.value=per;
  var filter=(document.getElementById('act-filter')||{value:''}).value;
  var brSelAct=document.getElementById('act-branch');
  var _canFilterBrA=isSuperAdmin()||R()==='director'||R()==='admin';
  if(brSelAct && _canFilterBrA){
    var _prevBrA=brSelAct.value;
    brSelAct.innerHTML='<option value="">\u0412\u0441\u0456 \u0444\u0456\u043B\u0456\u0457</option>'
      +(S.branches||[]).slice().sort(function(a,b){return (a.name||'').localeCompare(b.name||'','uk');})
        .map(function(b){return '<option value="'+b.id+'">'+b.name+'</option>';}).join('');
    brSelAct.value=_prevBrA;
  }
  var fBranchAct=_canFilterBrA?(document.getElementById('act-branch')||{value:''}).value:'';

  // Показуємо ВСІХ активних учнів, навіть без проведених занять цього
  // місяця — інакше вони випадали зі списку й акт їм ніхто не формував.
  var students=(S.students||[])
    .filter(function(x){ return x.status==='active'||x.status==='trial'; })
    .slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');});
  var rows=[];
  students.forEach(function(s){
    var lessons=actLessonsFor(s.id, per);
    var log=actLogFor(s.id, per);
    var signed=log.some(function(r){return r.status==='signed';});
    var sent=log.length>0;
    rows.push({s:s, lessons:lessons, log:log, signed:signed, sent:sent});
  });
  if(filter==='sent') rows=rows.filter(function(r){return r.sent;});
  if(filter==='notsent') rows=rows.filter(function(r){return !r.sent;});
  if(filter==='signed') rows=rows.filter(function(r){return r.signed;});
  if(fBranchAct) rows=rows.filter(function(r){
    var st=r.s;
    if(Array.isArray(st.branchIds)&&st.branchIds.length) return st.branchIds.indexOf(fBranchAct)>=0;
    return (st.branchId||st.branch_id)===fBranchAct;
  });

  rows.sort(function(a,b){
    var wa=a.signed?2:(a.sent?1:0), wb=b.signed?2:(b.sent?1:0);
    if(wa!==wb) return wa-wb;
    return (a.s.fn+a.s.ln).localeCompare(b.s.fn+b.s.ln,'uk');
  });

  var signedCount=rows.filter(function(r){return r.signed;}).length;
  var sentCount=rows.filter(function(r){return r.sent;}).length;
  var total=rows.length;

  var head='<div style="display:flex;gap:16px;padding:8px 4px 14px;flex-wrap:wrap;align-items:center">'
    +'<div style="font-size:13px"><b style="font-size:20px;color:var(--tut)">'+signedCount+'</b> / '+total+' \u043f\u0456\u0434\u043f\u0438\u0441\u0430\u043D\u043E \u00B7 '+sentCount+' \u043D\u0430\u0434\u0456\u0441\u043B\u0430\u043D\u043E</div>'
    +'<div style="flex:1;min-width:120px"><div style="height:8px;background:var(--s3);border-radius:10px;overflow:hidden"><div style="height:100%;width:'+(total?Math.round(signedCount/total*100):0)+'%;background:var(--tut);transition:width .4s"></div></div></div>'
    +'</div>';

  if(!rows.length){ wrap.innerHTML=head+'<div style="padding:24px;text-align:center;color:var(--t3)">\u0417\u0430 \u0446\u0435\u0439 \u043F\u0435\u0440\u0456\u043E\u0434 \u043D\u0435\u043C\u0430\u0454 \u043F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u0438\u0445 \u0437\u0430\u043D\u044F\u0442\u044C</div>'; return; }

  var list=rows.map(function(r){
    var hours=Math.round(r.lessons.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
    var sum=Math.round(r.lessons.reduce(function(s,l){return s+lessonTotal(l);},0)*100)/100;
    var badge=r.signed
      ? '<span style="background:rgba(34,181,115,.14);color:var(--tut);font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px">\u2713 \u041F\u0456\u0434\u043F\u0438\u0441\u0430\u043D\u043E</span>'
      : (r.sent
        ? '<span style="background:rgba(230,126,34,.14);color:var(--warn);font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px">\u2709 \u041D\u0430\u0434\u0456\u0441\u043B\u0430\u043D\u043E</span>'
        : '<span style="background:rgba(148,163,184,.16);color:var(--t3);font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px">\u25CB \u041D\u0435 \u0441\u0444\u043E\u0440\u043C\u043E\u0432\u0430\u043D\u043E</span>');
    return '<div style="display:flex;align-items:center;gap:12px;padding:10px 14px;border-bottom:1px solid var(--s3)">'
      +'<div style="width:34px;height:34px;border-radius:10px;background:var(--adm);color:#fff;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:13px;flex-shrink:0">'+((r.s.fn||' ')[0]+(r.s.ln||' ')[0]).toUpperCase()+'</div>'
      +'<div style="flex:1;min-width:0"><div style="font-weight:600;font-size:13px">'+r.s.fn+' '+r.s.ln+'</div>'
        +'<div style="font-size:11px;color:var(--t3)">'+(r.lessons.length
            ? r.lessons.length+' \u043F\u043E\u0441\u043B\u0443\u0433 \u00B7 '+hours+'\u0433 \u00B7 '+sum+'\u20B4'
            : '\u2014 \u0431\u0435\u0437 \u043F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u0438\u0445 \u0437\u0430\u043D\u044F\u0442\u044C')+'</div></div>'
      +badge
      +'<button class="btn btn-g btn-sm" onclick="openActEditM(\''+r.s.id+'\')" title="\u0420\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u0442\u0438/\u0441\u0444\u043E\u0440\u043C\u0443\u0432\u0430\u0442\u0438/\u0434\u0440\u0443\u043A\u0443\u0432\u0430\u0442\u0438">\u270F\uFE0F \u0410\u043A\u0442</button>'
      +(!r.signed?'<button class="btn btn-g btn-sm" onclick="markActSigned(\''+r.s.id+'\')" title="\u041F\u043E\u0437\u043D\u0430\u0447\u0438\u0442\u0438 \u043F\u0456\u0434\u043F\u0438\u0441\u0430\u043D\u0438\u043C \u0432\u0440\u0443\u0447\u043D\u0443">\u2713 \u041F\u0456\u0434\u043F\u0438\u0441\u0430\u043D\u043E</button>':'')
      +'</div>';
  }).join('');

  wrap.innerHTML=head+list;
}

// Позначити акт підписаним вручну (коли клієнт повернув підписаний документ)
async function markActSigned(sid){
  if(!canActs()) return;
  var per=actPeriod();
  var obj={ id:uid(), student_id:sid, period:per, status:'signed', channel:'manual',
    sent_by:CU?CU.id:null, sent_at:new Date().toISOString(), signed_at:new Date().toISOString() };
  try{
    await dbInsert('act_log', obj);
    S.actLog=(S.actLog||[]).concat([Object.assign({},obj,{studentId:sid,sentBy:obj.sent_by,signedAt:obj.signed_at})]);
    mkToast('\u041F\u043E\u0437\u043D\u0430\u0447\u0435\u043D\u043E \u044F\u043A \u043F\u0456\u0434\u043F\u0438\u0441\u0430\u043D\u0438\u0439 \u2705');
    renderActsPage();
  }catch(e){ mkToast('\u041F\u043E\u043C\u0438\u043B\u043A\u0430: '+(e.message||e),'error'); }
}

// Друк акта: генерує документ із реквізитами, переліком послуг і двома підписами
function openActEditM(sid){
  if(!canActs()) return;
  var s=(S.students||[]).find(function(x){return x.id===sid;});
  if(!s){ mkToast('\u0423\u0447\u0435\u043D\u044C \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E','error'); return; }
  var per=actPeriod();
  var lessons=actLessonsFor(sid, per);
  if(!lessons.length){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u0438\u0445 \u0437\u0430\u043D\u044F\u0442\u044C \u0437\u0430 \u0446\u0435\u0439 \u043F\u0435\u0440\u0456\u043E\u0434','error'); return; }

  // Групуємо за предметом+репетитором — початкові рядки акта (можна редагувати нижче)
  var groups={}, order=[];
  lessons.forEach(function(l){
    var tid=l.tutorId||l.tutor_id;
    var t=tid?(S.tutors||[]).find(function(x){return x.id===tid;}):null;
    var key=(tid||'')+'|'+(l.subject||'');
    if(!groups[key]){ groups[key]={tutor:t, subj:l.subject||'\u0406\u043D\u0448\u0435', lessons:[]}; order.push(key); }
    groups[key].lessons.push(l);
  });

  var rows=order.map(function(key){
    var g=groups[key];
    var gHours=Math.round(g.lessons.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
    var gSum=Math.round(g.lessons.reduce(function(s,l){return s+lessonTotal(l);},0)*100)/100;
    return { desc:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0441\u044C\u043A\u0456 \u043F\u043E\u0441\u043B\u0443\u0433\u0438: '+g.subj+(g.tutor?' ('+g.tutor.fn+' '+g.tutor.ln+')':''), hours:gHours, sum:gSum };
  });

  window._actEditSid=sid;
  window._actEditPeriod=per;
  var who=document.getElementById('act-edit-who');
  if(who) who.textContent=s.fn+' '+s.ln+' \u00B7 '+actPeriod();
  var noteEl=document.getElementById('act-edit-note');
  if(noteEl) noteEl.value='';
  var cName=document.getElementById('act-edit-client-name'); if(cName) cName.value=s.parentFn||(s.fn+' '+s.ln);
  var cPhone=document.getElementById('act-edit-client-phone'); if(cPhone) cPhone.value=s.parentPhone||s.phone||'';
  var cAddr=document.getElementById('act-edit-client-addr'); if(cAddr) cAddr.value=s.address||'';
  renderActEditRows(rows);
  openM('mo-act-edit');
}

function renderActEditRows(rows){
  var tb=document.getElementById('act-edit-rows');
  if(!tb) return;
  tb.innerHTML='';
  rows.forEach(function(r){ addActEditRow(r); });
}

function addActEditRow(r){
  r=r||{desc:'',hours:'',sum:''};
  var tb=document.getElementById('act-edit-rows');
  if(!tb) return;
  var tr=document.createElement('tr');
  tr.innerHTML='<td style="padding:3px 6px"><input class="ae-desc" value="'+String(r.desc||'').replace(/"/g,'&quot;')+'" style="width:100%;font-size:12.5px;padding:5px 7px"></td>'
    +'<td style="padding:3px 4px"><input class="ae-hours" type="number" step="0.1" value="'+(r.hours!=null?r.hours:'')+'" style="width:100%;font-size:12.5px;padding:5px 5px;text-align:center" oninput="recalcActEditTotal()"></td>'
    +'<td style="padding:3px 4px"><input class="ae-sum" type="number" step="0.01" value="'+(r.sum!=null?r.sum:'')+'" style="width:100%;font-size:12.5px;padding:5px 5px;text-align:right" oninput="recalcActEditTotal()"></td>'
    +'<td style="text-align:center"><button type="button" onclick="this.closest(\'tr\').remove();recalcActEditTotal()" style="border:none;background:none;cursor:pointer;color:var(--danger);font-size:13px">\u2715</button></td>';
  tb.appendChild(tr);
  recalcActEditTotal();
}

function recalcActEditTotal(){
  var sum=Array.from(document.querySelectorAll('#act-edit-rows .ae-sum')).reduce(function(s,el){return s+(parseFloat(el.value)||0);},0);
  var el=document.getElementById('act-edit-total');
  if(el) el.textContent=(Math.round(sum*100)/100).toLocaleString('uk-UA')+' \u20B4';
}

function collectActEditRows(){
  return Array.from(document.querySelectorAll('#act-edit-rows tr')).map(function(tr){
    return {
      desc:(tr.querySelector('.ae-desc')||{value:''}).value.trim(),
      hours:parseFloat((tr.querySelector('.ae-hours')||{value:'0'}).value)||0,
      sum:parseFloat((tr.querySelector('.ae-sum')||{value:'0'}).value)||0
    };
  }).filter(function(r){ return r.desc||r.sum; });
}

function printActFromEdit(){
  var sid=window._actEditSid, per=window._actEditPeriod;
  if(!sid||!per) return;
  var rows=collectActEditRows();
  if(!rows.length){ mkToast('\u0414\u043E\u0434\u0430\u0439\u0442\u0435 \u0445\u043E\u0447\u0430 \u043E\u0434\u0438\u043D \u0440\u044F\u0434\u043E\u043A','error'); return; }
  var note=(document.getElementById('act-edit-note')||{value:''}).value.trim();
  var clientInfo={
    name:(document.getElementById('act-edit-client-name')||{value:''}).value.trim(),
    phone:(document.getElementById('act-edit-client-phone')||{value:''}).value.trim(),
    addr:(document.getElementById('act-edit-client-addr')||{value:''}).value.trim()
  };
  closeM('mo-act-edit');
  printAct(sid, per, rows, note, clientInfo);
}

// Друк акта: генерує документ із реквізитами, переліком послуг (можливо відредагованим) і двома підписами
function printAct(sid, per, editedRows, editedNote, clientInfo){
  if(!canActs()) return;
  var s=(S.students||[]).find(function(x){return x.id===sid;});
  if(!s){ mkToast('\u0423\u0447\u0435\u043D\u044C \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E','error'); return; }
  per=per||actPeriod();

  var bid=myBranchId();
  var branch=(S.branches||[]).find(function(b){return b.id===bid;}) || (S.branches||[])[0];
  var cfg=S.settings||{};

  var rows;
  if(editedRows && editedRows.length){
    rows=editedRows;
  } else {
    var lessons=actLessonsFor(sid, per);
    if(!lessons.length){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u0438\u0445 \u0437\u0430\u043D\u044F\u0442\u044C \u0437\u0430 \u0446\u0435\u0439 \u043F\u0435\u0440\u0456\u043E\u0434','error'); return; }
    var groups={}, order=[];
    lessons.forEach(function(l){
      var tid=l.tutorId||l.tutor_id;
      var t=tid?(S.tutors||[]).find(function(x){return x.id===tid;}):null;
      var key=(tid||'')+'|'+(l.subject||'');
      if(!groups[key]){ groups[key]={tutor:t, subj:l.subject||'\u0406\u043D\u0448\u0435', lessons:[]}; order.push(key); }
      groups[key].lessons.push(l);
    });
    rows=order.map(function(key){
      var g=groups[key];
      var gHours=Math.round(g.lessons.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
      var gSum=Math.round(g.lessons.reduce(function(s,l){return s+lessonTotal(l);},0)*100)/100;
      return { desc:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0441\u044C\u043A\u0456 \u043F\u043E\u0441\u043B\u0443\u0433\u0438: '+g.subj+(g.tutor?' ('+g.tutor.fn+' '+g.tutor.ln+')':''), hours:gHours, sum:gSum };
    });
  }

  var totalHours=Math.round(rows.reduce(function(s,r){return s+(parseFloat(r.hours)||0);},0)*10)/10;
  var totalSum=Math.round(rows.reduce(function(s,r){return s+(parseFloat(r.sum)||0);},0)*100)/100;

  var actNo=(s.id||'').slice(0,6).toUpperCase()+'-'+per.replace('-','');
  var perLbl=(function(){ var p=per.split('-'); var m=['\u0441\u0456\u0447\u0435\u043D\u044C','\u043B\u044E\u0442\u0438\u0439','\u0431\u0435\u0440\u0435\u0437\u0435\u043D\u044C','\u043A\u0432\u0456\u0442\u0435\u043D\u044C','\u0442\u0440\u0430\u0432\u0435\u043D\u044C','\u0447\u0435\u0440\u0432\u0435\u043D\u044C','\u043B\u0438\u043F\u0435\u043D\u044C','\u0441\u0435\u0440\u043F\u0435\u043D\u044C','\u0432\u0435\u0440\u0435\u0441\u0435\u043D\u044C','\u0436\u043E\u0432\u0442\u0435\u043D\u044C','\u043B\u0438\u0441\u0442\u043E\u043F\u0430\u0434','\u0433\u0440\u0443\u0434\u0435\u043D\u044C'][parseInt(p[1])-1]; return m+' '+p[0]; })();

  var rowsHtml='';
  rows.forEach(function(r,i){
    rowsHtml+='<tr><td>'+(i+1)+'</td><td>'+r.desc+'</td><td class="c">\u0433\u043E\u0434.</td><td class="c">'+r.hours+'</td><td class="r">'+r.sum+'</td></tr>';
  });

  var executor=branch?(branch.pay_recipient||cfg.name||'\u2014'):(cfg.name||'\u2014');
  var execAddr=branch?(branch.pay_address||branch.address||''):'';
  var execEdrpou=branch?(branch.pay_edrpou||''):'';
  var execBank=branch?((branch.pay_bank||'')+(branch.pay_card?', '+branch.pay_card:'')):'';

  // Реквізити замовника: якщо редаговані в модалці — беремо їх, інакше з картки учня
  // (пріоритет — ПІБ батьків/опікуна, бо саме вони юридична сторона договору)
  var clientName=(clientInfo&&clientInfo.name)?clientInfo.name:(s.parentFn||(s.fn+' '+s.ln));
  var clientPhone=(clientInfo&&clientInfo.phone!=null)?clientInfo.phone:(s.parentPhone||s.phone||'');
  var clientAddr=(clientInfo&&clientInfo.addr!=null)?clientInfo.addr:(s.address||'');

  var w=window.open('','_blank');
  w.document.write('<!DOCTYPE html><html><head><meta charset="utf-8"><title>\u0410\u043A\u0442 \u0432\u0438\u043A\u043E\u043D\u0430\u043D\u0438\u0445 \u0440\u043E\u0431\u0456\u0442</title>'
    +'<style>body{font-family:Arial,sans-serif;max-width:720px;margin:24px auto;color:#111;font-size:13px;line-height:1.5}'
    +'h1{font-size:16px;text-align:center;margin-bottom:2px}'
    +'.sub{text-align:center;color:#555;font-size:12px;margin-bottom:20px}'
    +'.parties{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px;font-size:12.5px}'
    +'.parties b{display:block;margin-bottom:4px;font-size:12px;text-transform:uppercase;letter-spacing:.3px;color:#555}'
    +'table{width:100%;border-collapse:collapse;font-size:12.5px;margin-top:10px}'
    +'th,td{border:1px solid #999;padding:6px 8px;text-align:left} th{background:#f2f2f2;font-size:11.5px}'
    +'.c{text-align:center} .r{text-align:right}'
    +'.tot td{font-weight:700;background:#f7f7f7}'
    +'.sumtext{margin:14px 0;font-size:13px}'
    +'.note{margin:10px 0;font-size:12.5px;color:#333;white-space:pre-wrap}'
    +'.sign-wrap{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-top:44px}'
    +'.sign-line{border-top:1px solid #111;margin-top:46px;padding-top:4px;font-size:11.5px;color:#333}'
    +'.legal-note{margin-top:26px;font-size:10.5px;color:#888;border-top:1px dashed #ccc;padding-top:8px}'
    +'@media print{.noprint{display:none}}</style></head><body>'
    +'<h1>\u0410\u041A\u0422 \u2116 '+actNo+'</h1>'
    +'<div class="sub">\u043D\u0430\u0434\u0430\u043D\u043D\u044F \u043E\u0441\u0432\u0456\u0442\u043D\u0456\u0445 \u043F\u043E\u0441\u043B\u0443\u0433 \u0437\u0430 '+perLbl+'<br>\u0432\u0456\u0434 '+new Date().toLocaleDateString('uk-UA')+'</div>'
    +'<div class="parties">'
      +'<div><b>\u0412\u0438\u043A\u043E\u043D\u0430\u0432\u0435\u0446\u044C</b>'+executor
        +(execEdrpou?'<br>\u0406\u041F\u041D/\u0404\u0414\u0420\u041F\u041E\u0423: '+execEdrpou:'')
        +(execAddr?'<br>'+execAddr:'')
        +(execBank?'<br>'+execBank:'')
      +'</div>'
      +'<div><b>\u0417\u0430\u043C\u043E\u0432\u043D\u0438\u043A</b>'+clientName
        +(clientPhone?'<br>'+clientPhone:'')
        +(clientAddr?'<br>'+clientAddr:'')
      +'</div>'
    +'</div>'
    +'<p>\u0426\u0438\u043C \u0430\u043A\u0442\u043E\u043C \u043F\u0456\u0434\u0442\u0432\u0435\u0440\u0434\u0436\u0443\u0454\u0442\u044C\u0441\u044F, \u0449\u043E \u0412\u0438\u043A\u043E\u043D\u0430\u0432\u0435\u0446\u044C \u043D\u0430\u0434\u0430\u0432 \u043D\u0438\u0436\u0447\u0435\u043D\u0430\u0432\u0435\u0434\u0435\u043D\u0456 \u043F\u043E\u0441\u043B\u0443\u0433\u0438, \u0430 \u0417\u0430\u043C\u043E\u0432\u043D\u0438\u043A \u043F\u0440\u0438\u0439\u043D\u044F\u0432 \u0457\u0445 \u0443 \u043F\u043E\u0432\u043D\u043E\u043C\u0443 \u043E\u0431\u0441\u044F\u0437\u0456 \u0431\u0435\u0437 \u0437\u0430\u0443\u0432\u0430\u0436\u0435\u043D\u044C \u0449\u043E\u0434\u043E \u044F\u043A\u043E\u0441\u0442\u0456:</p>'
    +'<table><thead><tr><th>\u2116</th><th>\u041D\u0430\u0439\u043C\u0435\u043D\u0443\u0432\u0430\u043D\u043D\u044F \u043F\u043E\u0441\u043B\u0443\u0433\u0438</th><th class="c">\u041E\u0434.\u0432\u0438\u043C.</th><th class="c">\u041A\u0456\u043B\u044C\u043A\u0456\u0441\u0442\u044C</th><th class="r">\u0421\u0443\u043C\u0430, \u20B4</th></tr></thead><tbody>'
    +rowsHtml
    +'<tr class="tot"><td colspan="3"></td><td class="c">'+totalHours+'</td><td class="r">'+totalSum+'</td></tr>'
    +'</tbody></table>'
    +(editedNote?'<div class="note">'+editedNote.replace(/</g,'&lt;')+'</div>':'')
    +'<div class="sumtext">\u0420\u0430\u0437\u043E\u043C \u043D\u0430\u0434\u0430\u043D\u043E \u043F\u043E\u0441\u043B\u0443\u0433 \u043D\u0430 \u0441\u0443\u043C\u0443 <b>'+totalSum+' \u0433\u0440\u043D.</b> \u041F\u0440\u0435\u0442\u0435\u043D\u0437\u0456\u0439 \u0443 \u0417\u0430\u043C\u043E\u0432\u043D\u0438\u043A\u0430 \u0449\u043E\u0434\u043E \u044F\u043A\u043E\u0441\u0442\u0456, \u043E\u0431\u0441\u044F\u0433\u0443 \u0442\u0430 \u0442\u0435\u0440\u043C\u0456\u043D\u0456\u0432 \u043D\u0430\u0434\u0430\u043D\u043D\u044F \u043F\u043E\u0441\u043B\u0443\u0433 \u043D\u0435 \u043C\u0430\u0454.</div>'
    +'<div class="sign-wrap">'
      +'<div>\u0412\u0438\u043A\u043E\u043D\u0430\u0432\u0435\u0446\u044C:<div class="sign-line">'+executor+' \u00A0\u00A0\u00A0\u00A0 \u041F\u0456\u0434\u043F\u0438\u0441: ______________</div></div>'
      +'<div>\u0417\u0430\u043C\u043E\u0432\u043D\u0438\u043A:<div class="sign-line">'+clientName+' \u00A0\u00A0\u00A0\u00A0 \u041F\u0456\u0434\u043F\u0438\u0441: ______________</div></div>'
    +'</div>'
    +'<div class="legal-note">\u0424\u043E\u0440\u043C\u0430 \u0434\u043E\u043A\u0443\u043C\u0435\u043D\u0442\u0430 \u043E\u0440\u0456\u0454\u043D\u0442\u043E\u0432\u0430\u043D\u0430 \u043D\u0430 \u043E\u0431\u043E\u0432\u2019\u044F\u0437\u043A\u043E\u0432\u0456 \u0440\u0435\u043A\u0432\u0456\u0437\u0438\u0442\u0438 \u043F\u0435\u0440\u0432\u0438\u043D\u043D\u043E\u0433\u043E \u0434\u043E\u043A\u0443\u043C\u0435\u043D\u0442\u0430 (\u0441\u0442.9 \u0417\u0430\u043A\u043E\u043D\u0443 \u00AB\u041F\u0440\u043E \u0431\u0443\u0445\u0433\u0430\u043B\u0442\u0435\u0440\u0441\u044C\u043A\u0438\u0439 \u043E\u0431\u043B\u0456\u043A\u00BB). \u041F\u0435\u0440\u0435\u0432\u0456\u0440\u0442\u0435 \u0432\u0456\u0434\u043F\u043E\u0432\u0456\u0434\u043D\u0456\u0441\u0442\u044C \u0448\u0430\u0431\u043B\u043E\u043D\u0443 \u0432\u0430\u0448\u043E\u043C\u0443 \u0434\u043E\u0433\u043E\u0432\u043E\u0440\u0443 \u0442\u0430 \u0441\u0438\u0441\u0442\u0435\u043C\u0456 \u043E\u043F\u043E\u0434\u0430\u0442\u043A\u0443\u0432\u0430\u043D\u043D\u044F \u0437 \u0431\u0443\u0445\u0433\u0430\u043B\u0442\u0435\u0440\u043E\u043C.</div>'
    +'<button class="noprint" onclick="window.print()" style="margin-top:20px;padding:8px 18px;font-size:14px;cursor:pointer">\uD83D\uDDA8 \u0414\u0440\u0443\u043A\u0443\u0432\u0430\u0442\u0438 / \u0417\u0431\u0435\u0440\u0435\u0433\u0442\u0438 \u044F\u043A PDF</button>'
    +'</body></html>');
  w.document.close();
  setTimeout(function(){ try{w.focus();}catch(e){} }, 300);

  // Логуємо факт формування (не підпис!)
  var obj={ id:uid(), student_id:sid, period:per, status:'sent', channel:'print',
    sent_by:CU?CU.id:null, sent_at:new Date().toISOString(), signed_at:null };
  dbInsert('act_log', obj).then(function(){
    if(!(S.actLog||[]).some(function(x){return x.id===obj.id;}))
      S.actLog=(S.actLog||[]).concat([Object.assign({},obj,{studentId:sid,sentBy:obj.sent_by})]);
    renderActsPage();
  }).catch(function(){});
}

window.openActEditM=openActEditM;
window.addActEditRow=addActEditRow;
window.recalcActEditTotal=recalcActEditTotal;
window.printActFromEdit=printActFromEdit;
window.renderActsPage=renderActsPage;

window.printAct=printAct;
window.markActSigned=markActSigned;

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
  return R()==='god' || R()==='network_admin' || R()==='director';
}

function currentBranch(){
  return S.currentBranchId || null;
}

function branchName(id){
  const b=(S.branches||[]).find(x=>x.id===id);
  return b?b.name:'\u2014';
}

function filterByBranch(arr){
  if(!arr) return [];
  if(R()==='tutor') return arr;
  const bid=currentBranch();
  if(!bid&&isSuperAdmin()) return arr;
  if(bid){
    // Хтось свідомо обрав конкретну філію в перемикачі — фільтруємо саме під неї
    return arr.filter(function(x){
      if(Array.isArray(x.branchIds) && x.branchIds.length) return x.branchIds.indexOf(bid)>=0;
      return !x.branchId||x.branchId===bid;
    });
  }
  // Немає обраної конкретної філії: показуємо дані З УСІХ філій, до яких прив'язаний
  // САМ користувач (адмін може відповідати за кілька філій одночасно, як і репетитор/учень).
  var myBranches=(Array.isArray(CU&&CU.branchIds)&&CU.branchIds.length)?CU.branchIds:((CU&&CU.branchId)?[CU.branchId]:null);
  if(!myBranches||!myBranches.length) return arr; // адміну не призначено жодної філії — не обмежуємо (безпечний дефолт)
  return arr.filter(function(x){
    if(Array.isArray(x.branchIds) && x.branchIds.length) return x.branchIds.some(function(b){return myBranches.indexOf(b)>=0;});
    return !x.branchId||myBranches.indexOf(x.branchId)>=0;
  });
}
function myBranchId(){
  // Для суперадмінів — поточно обрана в перемикачі філія (чи все, якщо не обрано).
  // Для звичайного адміна — ПЕРША з його власних призначених філій (використовується
  // лише як дефолт при СТВОРЕННІ нового запису, не для показу/фільтрації списків —
  // для цього тепер filterByBranch() враховує ВСІ філії адміна одразу).
  // РАНІШЕ тут був небезпечний фолбек на S.branches[0] — перша-ліпша філія в базі,
  // що прив'язувало будь-якого адміна без явного branchId до випадкової філії.
  if(isSuperAdmin()) return currentBranch();
  if(Array.isArray(CU&&CU.branchIds)&&CU.branchIds.length) return CU.branchIds[0];
  return (CU&&CU.branchId)||null;
}

function mkAv(fn,ln,sz,photo){
  sz=sz||30;
  if(photo) return '<div class="av" style="width:'+sz+'px;height:'+sz+'px;overflow:hidden;flex-shrink:0"><img src="'+photo+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%"></div>';
  const cs=['#6c8fff','#a78bfa','#34d399','#f59e0b','#f87171','#0ea5e9','#ec4899','#ff6b35'];
  const i=((fn||'A').charCodeAt(0)+((ln||'B').charCodeAt(0)))%cs.length;
  return '<div class="av" style="background:'+cs[i]+';width:'+sz+'px;height:'+sz+'px;font-size:'+(sz*.38)+'px;color:#fff">'+((fn||'?')[0])+((ln||'')[0]||'')+'</div>';
}

function bst(s){
  var m={active:'bg',trial:'bb',paused:'by',completed:'br',inactive:'bn',request:'bb',planned:'bb',done:'bg',testing:'bp',cancelled:'br',missed:'br',makeup:'by',makeup_planned:'bn',paid:'bg',pending:'by',overdue:'br'};
  var l={active:'Активний',trial:'Пробне',paused:'Призупин.',completed:'Завершив',inactive:'Неактивний',request:'Запит',planned:'Планов.',done:'Проведено',testing:'Тестування',cancelled:'Скасов.',missed:'Пропущено',makeup:'Відпрацьовано',makeup_planned:'План. відпрац.',paid:'Оплачено',pending:'Очікується',overdue:'Прострочено'};
  return '<span class="badge '+(m[s]||'bb')+'">'+( l[s]||s)+'</span>';
}

function fd(d){if(!d)return '\u2014';return new Date(d).toLocaleDateString('uk-UA',{day:'2-digit',month:'2-digit',year:'numeric'});}

function fd2(l,p){document.getElementById('lu').value=l;document.getElementById('lp').value=p;}

function sn(id){const s=S.students.find(x=>x.id===id);retu
