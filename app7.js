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

    return { from: from, auth: auth, channel: channel, removeChannel: removeChannel, rpc: rpc };
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
    can:{students:true,tutors:false,lessons:true,comms:true,payments:false,users:false,settings:false,danger:false,deleteAny:false},
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
function deleteLessonFromModal(){
  if(!S.editId)return;
  if(!confirm('Видалити цей урок?'))return;
  dbDelete('lessons',S.editId);
  closeM('mo-lesson');
}

async function deleteLessonSeriesFromModal(){
  if(!S.editId)return;
  var l=(S.lessons||[]).find(function(x){return x.id===S.editId;});
  if(!l||!l.recurId){mkToast('Немає серії','error');return;}
  var series=S.lessons.filter(function(x){return x.recurId===l.recurId;});
  if(!confirm('Видалити всю серію? ('+series.length+' уроків)'))return;
  try{
    for(var i=0;i<series.length;i++) await _sb.from('lessons').delete().eq('id',series[i].id);
    S.lessons=S.lessons.filter(function(x){return x.recurId!==l.recurId;});
    mkToast('Серію видалено'); closeM('mo-lesson'); renderSch();
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
    for(var p=1;p<nParts;p++){
      var totalMins=lm0+chunkMin*p;
      var newH=lh0+Math.floor(totalMins/60);
      var newM=totalMins%60;
      var newTime=String(newH).padStart(2,'0')+':'+String(newM).padStart(2,'0');
      await dbInsert('lessons',Object.assign({},base,{id:uid(),time:newTime,split_index:p}));
    }
    mkToast('Розбито на '+nParts+' × '+chunkMin+' хв');
    closeM('mo-lesson');
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
    status:orig.status||'missed',price:null,
    branch_id:orig.branchId||orig.branch_id||null,
    split_group_id:id
  };
  try{
    await dbUpdate('lessons',id,{dur:parts[0],split_group_id:id,split_index:0});
    var offset=lm0+parts[0];
    for(var i=1;i<parts.length;i++){
      var nh=lh0+Math.floor(offset/60);
      var nm=offset%60;
      var t=String(nh).padStart(2,'0')+':'+String(nm).padStart(2,'0');
      await dbInsert('lessons',Object.assign({},base,{id:uid(),time:t,dur:parts[i],split_index:i}));
      offset+=parts[i];
    }
    mkToast('\u0420\u043e\u0437\u0431\u0438\u0442\u043e: '+parts.join(' + ')+' \u0445\u0432');
    closeM('mo-lesson');
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
    // Видаляємо другий урок
    await dbDelete('lessons',remove.id);
    mkToast('\u0423\u0440\u043e\u043a\u0438 \u043e\u0431\u2019\u0454\u0434\u043d\u0430\u043d\u043e: '+totalDur+' \u0445\u0432');
    closeM('mo-lesson');
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
    return l.status==='done'||l.status==='completed'||l.status==='makeup';
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
    var lines=[];
    lines.push('\uD83D\uDCCB \u0420\u0410\u0425\u0423\u041D\u041E\u041A \u041d\u0410 \u041e\u041f\u041b\u0410\u0422\u0423');
    lines.push('\u0423\u0447\u0435\u043d\u044c: '+student.fn+' '+student.ln);
    lines.push('\u041f\u0435\u0440\u0456\u043e\u0434: '+(dateFrom?fd(dateFrom)+' \u2014 ':'')+fd(dateTo));
    lines.push('\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500');

    groupOrder.forEach(function(key){
      var g=groups[key];
      var gHours=Math.round(g.lessons.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
      var gTotal=Math.round(g.lessons.reduce(function(s,l){return s+lessonCost(l);},0)*10)/10;
      lines.push('\uD83D\uDC64 '+(g.tutor?g.tutor.fn+' '+g.tutor.ln:'\u0411\u0435\u0437 \u0440\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0430')+' \u2014 '+g.subj);
      g.lessons.forEach(function(l,i){
        var dur=(parseFloat(l.dur)||60)/60;
        lines.push('  '+(i+1)+'. '+fd(l.date)+(l.time?' '+l.time:'')+' \u2014 '+dur+'\u0433\u043e\u0434 = '+lessonCost(l)+'\u20b4');
      });
      lines.push('  \u0420\u0430\u0437\u043e\u043c: '+gHours+'\u0433\u043e\u0434 / '+gTotal+'\u20b4');
      lines.push('');
    });

    lines.push('\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500');
    lines.push('\u0412\u0421\u042c\u041e\u0413\u041e \u0433\u043e\u0434\u0438\u043d: '+totalHours);
    lines.push('\u0414\u041e \u0421\u041f\u041b\u0410\u0422\u0418: '+total+' \u20b4');

    // Реквізити для оплати
    if(branch && (branch.pay_recipient||branch.pay_card||branch.pay_bank)){
      lines.push('');
      lines.push('\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500');
      lines.push('\uD83D\uDCB3 \u0420\u0415\u041a\u0412\u0406\u0417\u0418\u0422\u0418 \u0414\u041b\u042f \u041e\u041f\u041b\u0410\u0422\u0418');
      if(branch.pay_recipient) lines.push('\u041e\u0442\u0440\u0438\u043c\u0443\u0432\u0430\u0447: '+branch.pay_recipient);
      if(branch.pay_card) lines.push('IBAN: '+branch.pay_card);
      if(branch.pay_bank) lines.push('\u0411\u0430\u043d\u043a: '+branch.pay_bank);
      if(branch.pay_edrpou) lines.push('\u0404\u0414\u0420\u041f\u041e\u0423/\u0406\u041f\u041d: '+branch.pay_edrpou);
      lines.push('\u041f\u0440\u0438\u0437\u043d\u0430\u0447\u0435\u043d\u043d\u044f: '+(branch.pay_purpose||'\u041e\u043f\u043b\u0430\u0442\u0430 \u0437\u0430 \u043d\u0430\u0432\u0447\u0430\u043d\u043d\u044f')+' \u2014 '+student.fn+' '+student.ln);
    }

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
  var ids=Object.keys(need);
  var rows=ids.map(function(sid){
    var st=(S.students||[]).find(function(x){return x.id===sid;});
    var sent=sentMap[sid]||[];
    return { st:st, sid:sid, count:need[sid].count, sent:sent };
  }).filter(function(r){ return r.st; });

  if(filter==='sent') rows=rows.filter(function(r){return r.sent.length;});
  if(filter==='notsent') rows=rows.filter(function(r){return !r.sent.length;});

  rows.sort(function(a,b){
    // не відіслані зверху, далі за іменем
    var sa=a.sent.length?1:0, sb=b.sent.length?1:0;
    if(sa!==sb) return sa-sb;
    return ((a.st.fn||'')+(a.st.ln||'')).localeCompare((b.st.fn||'')+(b.st.ln||''),'uk');
  });

  var sentCount=rows.filter(function(r){return r.sent.length;}).length;
  var total=Object.keys(need).length;

  var CH={viber:'\uD83D\uDFE3 Viber',telegram:'\u2708\uFE0F Telegram',email:'\u2709\uFE0F Email',copy:'\uD83D\uDCCB \u041a\u043e\u043f\u0456\u044f'};

  var head='<div style="display:flex;gap:16px;padding:8px 14px 12px;flex-wrap:wrap">'
    +'<div style="font-size:13px"><b style="font-size:20px;color:var(--tut)">'+sentCount+'</b> / '+total+' \u0432\u0456\u0434\u0456\u0441\u043b\u0430\u043d\u043e</div>'
    +'<div style="flex:1;min-width:120px;align-self:center"><div style="height:8px;background:var(--s3);border-radius:10px;overflow:hidden"><div style="height:100%;width:'+(total?Math.round(sentCount/total*100):0)+'%;background:var(--tut);transition:width .4s"></div></div></div>'
    +'</div>';

  if(!rows.length){ body.innerHTML=head+'<div style="padding:20px;text-align:center;color:var(--t3)">\u041d\u0435\u043c\u0430\u0454 \u0443\u0447\u043d\u0456\u0432 \u0456\u0437 \u0437\u0430\u043f\u043b\u0430\u043d\u043e\u0432\u0430\u043d\u0438\u043c\u0438 \u0437\u0430\u043d\u044f\u0442\u0442\u044f\u043c\u0438 \u0446\u044c\u043e\u0433\u043e \u043c\u0456\u0441\u044f\u0446\u044f</div>'; return; }

  var list=rows.map(function(r){
    var isSent=r.sent.length>0;
    var last=isSent?r.sent.slice().sort(function(a,b){return String(b.sent_at).localeCompare(String(a.sent_at));})[0]:null;
    var av=isSent?'var(--tut)':'var(--warn)';
    var badge=isSent
      ? '<span style="background:rgba(34,181,115,.14);color:var(--tut);font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px">\u2705 \u0412\u0456\u0434\u0456\u0441\u043b\u0430\u043d\u043e</span>'
      : '<span style="background:rgba(230,126,34,.14);color:var(--warn);font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px">\u23f3 \u041d\u0435 \u0432\u0456\u0434\u0456\u0441\u043b\u0430\u043d\u043e</span>';
    var detail=isSent && last
      ? '<div style="font-size:11px;color:var(--t3)">'+(CH[last.channel]||last.channel||'')+' \u00b7 '+(last.sent_at?new Date(last.sent_at).toLocaleDateString('uk-UA'):'')+(r.sent.length>1?' \u00b7 \u0432\u0441\u044c\u043e\u0433\u043e '+r.sent.length:'')+'</div>'
      : '<div style="font-size:11px;color:var(--t3)">'+r.count+' \u0437\u0430\u043f\u043b. \u0437\u0430\u043d\u044f\u0442\u044c</div>';
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
    return l.status==='done'||l.status==='completed'||l.status==='makeup';
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

  var students=(S.students||[]).slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');});
  var rows=[];
  students.forEach(function(s){
    var lessons=actLessonsFor(s.id, per);
    if(!lessons.length) return;
    var log=actLogFor(s.id, per);
    var signed=log.some(function(r){return r.status==='signed';});
    var sent=log.length>0;
    rows.push({s:s, lessons:lessons, log:log, signed:signed, sent:sent});
  });
  if(filter==='sent') rows=rows.filter(function(r){return r.sent;});
  if(filter==='notsent') rows=rows.filter(function(r){return !r.sent;});
  if(filter==='signed') rows=rows.filter(function(r){return r.signed;});

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
        +'<div style="font-size:11px;color:var(--t3)">'+r.lessons.length+' \u043F\u043E\u0441\u043B\u0443\u0433 \u00B7 '+hours+'\u0433 \u00B7 '+sum+'\u20B4</div></div>'
      +badge
      +'<button class="btn btn-g btn-sm" onclick="printAct(\''+r.s.id+'\')" title="\u0421\u0444\u043E\u0440\u043C\u0443\u0432\u0430\u0442\u0438/\u0434\u0440\u0443\u043A\u0443\u0432\u0430\u0442\u0438">\uD83D\uDDA8 \u0410\u043A\u0442</button>'
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
function printAct(sid){
  if(!canActs()) return;
  var s=(S.students||[]).find(function(x){return x.id===sid;});
  if(!s){ mkToast('\u0423\u0447\u0435\u043D\u044C \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E','error'); return; }
  var per=actPeriod();
  var lessons=actLessonsFor(sid, per);
  if(!lessons.length){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u0438\u0445 \u0437\u0430\u043D\u044F\u0442\u044C \u0437\u0430 \u0446\u0435\u0439 \u043F\u0435\u0440\u0456\u043E\u0434','error'); return; }

  var bid=myBranchId();
  var branch=(S.branches||[]).find(function(b){return b.id===bid;}) || (S.branches||[])[0];
  var cfg=S.settings||{};

  // Групуємо за предметом+репетитором — рядки акта як послуги
  var groups={}, order=[];
  lessons.forEach(function(l){
    var tid=l.tutorId||l.tutor_id;
    var t=tid?(S.tutors||[]).find(function(x){return x.id===tid;}):null;
    var key=(tid||'')+'|'+(l.subject||'');
    if(!groups[key]){ groups[key]={tutor:t, subj:l.subject||'\u0406\u043D\u0448\u0435', lessons:[]}; order.push(key); }
    groups[key].lessons.push(l);
  });

  var totalHours=Math.round(lessons.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  var totalSum=Math.round(lessons.reduce(function(s,l){return s+lessonTotal(l);},0)*100)/100;

  var actNo=(s.id||'').slice(0,6).toUpperCase()+'-'+per.replace('-','');
  var perLbl=(function(){ var p=per.split('-'); var m=['\u0441\u0456\u0447\u0435\u043D\u044C','\u043B\u044E\u0442\u0438\u0439','\u0431\u0435\u0440\u0435\u0437\u0435\u043D\u044C','\u043A\u0432\u0456\u0442\u0435\u043D\u044C','\u0442\u0440\u0430\u0432\u0435\u043D\u044C','\u0447\u0435\u0440\u0432\u0435\u043D\u044C','\u043B\u0438\u043F\u0435\u043D\u044C','\u0441\u0435\u0440\u043F\u0435\u043D\u044C','\u0432\u0435\u0440\u0435\u0441\u0435\u043D\u044C','\u0436\u043E\u0432\u0442\u0435\u043D\u044C','\u043B\u0438\u0441\u0442\u043E\u043F\u0430\u0434','\u0433\u0440\u0443\u0434\u0435\u043D\u044C'][parseInt(p[1])-1]; return m+' '+p[0]; })();

  var rowsHtml='';
  var idx=1;
  order.forEach(function(key){
    var g=groups[key];
    var gHours=Math.round(g.lessons.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
    var gSum=Math.round(g.lessons.reduce(function(s,l){return s+lessonTotal(l);},0)*100)/100;
    rowsHtml+='<tr><td>'+(idx++)+'</td><td>\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0441\u044C\u043A\u0456 \u043F\u043E\u0441\u043B\u0443\u0433\u0438: '+g.subj+(g.tutor?' ('+g.tutor.fn+' '+g.tutor.ln+')':'')+'</td><td class="c">\u0433\u043E\u0434.</td><td class="c">'+gHours+'</td><td class="r">'+gSum+'</td></tr>';
  });

  var executor=branch?(branch.pay_recipient||cfg.name||'\u2014'):(cfg.name||'\u2014');
  var execAddr=branch?(branch.pay_address||branch.address||''):'';
  var execEdrpou=branch?(branch.pay_edrpou||''):'';
  var execBank=branch?((branch.pay_bank||'')+(branch.pay_card?', '+branch.pay_card:'')):'';

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
      +'<div><b>\u0417\u0430\u043C\u043E\u0432\u043D\u0438\u043A</b>'+s.fn+' '+s.ln
        +((s.parentPhone||s.phone)?'<br>'+(s.parentPhone||s.phone):'')
        +((s.address)?'<br>'+s.address:'')
      +'</div>'
    +'</div>'
    +'<p>\u0426\u0438\u043C \u0430\u043A\u0442\u043E\u043C \u043F\u0456\u0434\u0442\u0432\u0435\u0440\u0434\u0436\u0443\u0454\u0442\u044C\u0441\u044F, \u0449\u043E \u0412\u0438\u043A\u043E\u043D\u0430\u0432\u0435\u0446\u044C \u043D\u0430\u0434\u0430\u0432 \u043D\u0438\u0436\u0447\u0435\u043D\u0430\u0432\u0435\u0434\u0435\u043D\u0456 \u043F\u043E\u0441\u043B\u0443\u0433\u0438, \u0430 \u0417\u0430\u043C\u043E\u0432\u043D\u0438\u043A \u043F\u0440\u0438\u0439\u043D\u044F\u0432 \u0457\u0445 \u0443 \u043F\u043E\u0432\u043D\u043E\u043C\u0443 \u043E\u0431\u0441\u044F\u0437\u0456 \u0431\u0435\u0437 \u0437\u0430\u0443\u0432\u0430\u0436\u0435\u043D\u044C \u0449\u043E\u0434\u043E \u044F\u043A\u043E\u0441\u0442\u0456:</p>'
    +'<table><thead><tr><th>\u2116</th><th>\u041D\u0430\u0439\u043C\u0435\u043D\u0443\u0432\u0430\u043D\u043D\u044F \u043F\u043E\u0441\u043B\u0443\u0433\u0438</th><th class="c">\u041E\u0434.\u0432\u0438\u043C.</th><th class="c">\u041A\u0456\u043B\u044C\u043A\u0456\u0441\u0442\u044C</th><th class="r">\u0421\u0443\u043C\u0430, \u20B4</th></tr></thead><tbody>'
    +rowsHtml
    +'<tr class="tot"><td colspan="3"></td><td class="c">'+totalHours+'</td><td class="r">'+totalSum+'</td></tr>'
    +'</tbody></table>'
    +'<div class="sumtext">\u0420\u0430\u0437\u043E\u043C \u043D\u0430\u0434\u0430\u043D\u043E \u043F\u043E\u0441\u043B\u0443\u0433 \u043D\u0430 \u0441\u0443\u043C\u0443 <b>'+totalSum+' \u0433\u0440\u043D.</b> \u041F\u0440\u0435\u0442\u0435\u043D\u0437\u0456\u0439 \u0443 \u0417\u0430\u043C\u043E\u0432\u043D\u0438\u043A\u0430 \u0449\u043E\u0434\u043E \u044F\u043A\u043E\u0441\u0442\u0456, \u043E\u0431\u0441\u044F\u0433\u0443 \u0442\u0430 \u0442\u0435\u0440\u043C\u0456\u043D\u0456\u0432 \u043D\u0430\u0434\u0430\u043D\u043D\u044F \u043F\u043E\u0441\u043B\u0443\u0433 \u043D\u0435 \u043C\u0430\u0454.</div>'
    +'<div class="sign-wrap">'
      +'<div>\u0412\u0438\u043A\u043E\u043D\u0430\u0432\u0435\u0446\u044C:<div class="sign-line">'+executor+' \u00A0\u00A0\u00A0\u00A0 \u041F\u0456\u0434\u043F\u0438\u0441: ______________</div></div>'
      +'<div>\u0417\u0430\u043C\u043E\u0432\u043D\u0438\u043A:<div class="sign-line">'+s.fn+' '+s.ln+' \u00A0\u00A0\u00A0\u00A0 \u041F\u0456\u0434\u043F\u0438\u0441: ______________</div></div>'
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
  if(!arr) return [];
  if(R()==='tutor') return arr;
  const bid=currentBranch();
  if(!bid&&isSuperAdmin()) return arr;
  const activeBid=bid||myBranchId();
  if(!activeBid) return arr;
  return arr.filter(function(x){return !x.branchId||x.branchId===activeBid;});
}function myBranchId(){
  // For branch-level users, return their assigned branch
  if(isSuperAdmin()) return currentBranch();
  return CU?.branchId || (S.branches[0]?.id);
}

function mkAv(fn,ln,sz,photo){
  sz=sz||30;
  if(photo) return '<div class="av" style="width:'+sz+'px;height:'+sz+'px;overflow:hidden;flex-shrink:0"><img src="'+photo+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%"></div>';
  const cs=['#6c8fff','#a78bfa','#34d399','#f59e0b','#f87171','#0ea5e9','#ec4899','#ff6b35'];
  const i=((fn||'A').charCodeAt(0)+((ln||'B').charCodeAt(0)))%cs.length;
  return '<div class="av" style="background:'+cs[i]+';width:'+sz+'px;height:'+sz+'px;font-size:'+(sz*.38)+'px;color:#fff">'+((fn||'?')[0])+((ln||'')[0]||'')+'</div>';
}

function bst(s){
  var m={active:'bg',trial:'bb',paused:'by',completed:'br',planned:'bb',done:'bg',cancelled:'br',missed:'br',makeup:'by',makeup_planned:'bn',paid:'bg',pending:'by',overdue:'br'};
  var l={active:'Активний',trial:'Пробне',paused:'Призупин.',completed:'Завершив',planned:'Планов.',done:'Проведено',cancelled:'Скасов.',missed:'Пропущено',makeup:'Відпрацьовано',makeup_planned:'План. відпрац.',paid:'Оплачено',pending:'Очікується',overdue:'Прострочено'};
  return '<span class="badge '+(m[s]||'bb')+'">'+( l[s]||s)+'</span>';
}

function fd(d){if(!d)return '\u2014';return new Date(d).toLocaleDateString('uk-UA',{day:'2-digit',month:'2-digit',year:'numeric'});}

function fd2(l,p){document.getElementById('lu').value=l;document.getElementById('lp').value=p;}

function sn(id){const s=S.students.find(x=>x.id===id);return s?s.fn+' '+s.ln:'\u2014';}
// Прізвище та ініціал: "Мілентьєва В."
function snShort(id){const s=S.students.find(x=>x.id===id);return s?((s.ln||'')+(s.fn?' '+s.fn[0]+'.':'')).trim()||'\u2014':'\u2014';}
// Розкладання уроків дня по вертикальних смугах: уроки, що перетинаються в часі,
// діляться по ширині клітинки. Повертає Map(lessonId -> {lane, count}).
function schAssignLanes(dayLessons){
  var evs=dayLessons.map(function(l){
    var p=(l.time||'08:00').split(':');
    var st=(parseInt(p[0])||8)*60+(parseInt(p[1])||0);
    return {l:l, s:st, e:st+(parseFloat(l.dur)||60)};
  }).sort(function(a,b){return a.s-b.s||a.e-b.e;});
  var out=new Map();
  var cluster=[], clusterEnd=-1;
  function flush(){
    if(!cluster.length) return;
    var lanes=[];
    cluster.forEach(function(ev){
      var lane=-1;
      for(var i=0;i<lanes.length;i++){ if(lanes[i]<=ev.s){ lane=i; break; } }
      if(lane<0){ lane=lanes.length; lanes.push(0); }
      lanes[lane]=ev.e;
      ev.lane=lane;
    });
    cluster.forEach(function(ev){ out.set(ev.l.id,{lane:ev.lane,count:lanes.length}); });
    cluster=[];
  }
  evs.forEach(function(ev){
    if(cluster.length && ev.s>=clusterEnd){ flush(); clusterEnd=-1; }
    cluster.push(ev);
    clusterEnd=Math.max(clusterEnd,ev.e);
  });
  flush();
  return out;
}

function tn(id){const t=S.tutors.find(x=>x.id===id);return t?t.fn+' '+t.ln:'\u2014';}
// Вартість заняття: ставка ЗА ГОДИНУ × (тривалість/60).
// Ставка: знімок у занятті (l.price) → якщо нема, ставка з картки учня за правилами.
function lessonTotal(l){
  var dur=(parseFloat(l.dur)||60)/60;
  var rate=(l.price!=null&&l.price!==''&&!isNaN(parseFloat(l.price))&&parseFloat(l.price)>0)?parseFloat(l.price):(function(){
    var sid=l.studentId||l.student_id;
    var st=sid?(S.students||[]).find(function(s){return s.id===sid;}):null;
    return studentRate(st, l.subject, l.tutorId||l.tutor_id);
  })();
  return Math.round(dur*rate*100)/100;
}

// Ставка учня для конкретного предмета/репетитора.
// Правила в st.rates: [{subject:'', tutor_id:'', rate:N}], порожнє поле = будь-який.
// Пріоритет: предмет+репетитор (3) → предмет (2) → репетитор (1) → базова hourly_rate.
function studentRate(st, subject, tutorId){
  if(!st) return 0;
  var rules=[];
  try{ rules=typeof st.rates==='string'?JSON.parse(st.rates||'[]'):(Array.isArray(st.rates)?st.rates:[]); }catch(e){ rules=[]; }
  var subjN=(subject||'').trim().toLowerCase();
  var best=null, bestScore=-1;
  rules.forEach(function(r){
    if(isNaN(parseFloat(r.rate))) return;
    var rs=(r.subject||'').trim().toLowerCase();
    if(rs && rs!==subjN) return;          // предмет вказано і не збігся
    if(r.tutor_id && r.tutor_id!==tutorId) return; // репетитора вказано і не збігся
    var score=(rs?2:0)+(r.tutor_id?1:0);
    if(score>bestScore){ bestScore=score; best=r; }
  });
  if(best) return parseFloat(best.rate);
  var b=parseFloat(st.hourly_rate);
  return isNaN(b)?0:b;
}

// ── UI рядків ставок у картці учня ──
function addRateRow(rule){
  rule=rule||{};
  var list=document.getElementById('s-rates-list');
  if(!list) return;
  var row=document.createElement('div');
  row.className='s-rate-row';
  row.style.cssText='display:flex;gap:6px;align-items:center;flex-wrap:wrap';
  row.innerHTML=
    '<input class="sr-subj" list="subj-list-s" placeholder="Предмет (будь-який)" value="'+String(rule.subject||'').replace(/"/g,'&quot;')+'" style="flex:2;min-width:110px;font-size:12px;padding:5px 8px">'
    +'<select class="sr-tutor" style="flex:2;min-width:110px;font-size:12px;padding:5px 8px">'
      +'<option value="">Будь-який репетитор</option>'
      +(S.tutors||[]).slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');})
        .map(function(t){return '<option value="'+t.id+'"'+(t.id===(rule.tutor_id||'')?' selected':'')+'>'+t.fn+' '+t.ln+'</option>';}).join('')
    +'</select>'
    +'<input class="sr-rate" type="number" placeholder="₴/год" value="'+(rule.rate!=null?rule.rate:'')+'" style="width:80px;font-size:12px;padding:5px 8px">'
    +'<button type="button" title="Прибрати" style="border:none;background:none;cursor:pointer;color:var(--danger);font-size:14px;padding:2px 6px" onclick="this.parentElement.remove()">✕</button>';
  list.appendChild(row);
  // Підказки предметів у рядку ставки (працюють і на телефонах)
  var _si=row.querySelector('.sr-subj');
  if(_si){ _si.id=_si.id||('sr-subj-'+Math.random().toString(36).slice(2,8)); makeSuggest(_si.id, allKnownSubjects); }
}
// Похідні дані зі ставок учня: предмети та репетитори (з фолбеком на legacy-поля)
function studentRatesArr(st){
  if(!st) return [];
  try{ var r=typeof st.rates==='string'?JSON.parse(st.rates||'[]'):(Array.isArray(st.rates)?st.rates:[]); return r||[]; }catch(e){ return []; }
}
function studentSubjects(st){
  var out=[];
  studentRatesArr(st).forEach(function(r){ var s=(r.subject||'').trim(); if(s&&out.indexOf(s)<0) out.push(s); });
  if(!out.length && st && st.subject) out.push(st.subject);
  return out;
}
function studentTutorIds(st){
  var out=[];
  studentRatesArr(st).forEach(function(r){ if(r.tutor_id&&out.indexOf(r.tutor_id)<0) out.push(r.tutor_id); });
  if(!out.length && st){
    var legacy=(Array.isArray(st.tutorIds)&&st.tutorIds.length)?st.tutorIds:(st.tutorId?[st.tutorId]:[]);
    legacy.forEach(function(t){ if(t&&out.indexOf(t)<0) out.push(t); });
  }
  return out;
}

function collectRateRows(){
  return Array.from(document.querySelectorAll('#s-rates-list .s-rate-row')).map(function(row){
    var rv=parseFloat(row.querySelector('.sr-rate')?.value);
    return {
      subject:(row.querySelector('.sr-subj')?.value||'').trim(),
      tutor_id:row.querySelector('.sr-tutor')?.value||'',
      rate:(!isNaN(rv)&&rv>0)?rv:null
    };
  }).filter(function(r){ return r.subject||r.tutor_id||r.rate; });
}

// Розбір правил ставок учня (рядок JSON або масив)
function studentRateRules(st){
  if(!st) return [];
  try{ return typeof st.rates==='string'?JSON.parse(st.rates||'[]'):(Array.isArray(st.rates)?st.rates:[]); }catch(e){ return []; }
}

function mkToast(msg,type='success'){
  const e=document.createElement('div');e.className=("toast "+(type));
  e.innerHTML=("<span>"+(type==='success'?'\u2705':'\u274C')+"</span> "+(msg));
  document.body.appendChild(e);setTimeout(()=>e.remove(),3000);
}

function popSel(id,arr,valKey,lblFn,placeholder='\u2014'){const el=document.getElementById(id);if(!el)return;el.innerHTML=("<option value=\"\">"+(placeholder)+"</option>")+arr.map(x=>("<option value=\""+(x[valKey])+"\">"+(lblFn(x))+"</option>")).join('');}
function popSelSearch(id,arr,valKey,lblFn,placeholder){
  var el=document.getElementById(id);
  var savedVal=el?el.value:'';
  popSel(id,arr,valKey,lblFn,placeholder);
  // Відновлюємо значення якщо воно є серед нових options
  if(el&&savedVal){
    el.value=savedVal;
    // якщо option не знайдено — скидаємо
    if(el.value!==savedVal) el.value='';
  }
  makeSearchable(id);
  if(el&&el._updateSearch) el._updateSearch();
}

function openM(id){
  var el=document.getElementById(id);
  if(!el) return;
  el.style.display='';
  el.style.pointerEvents='';
  el.classList.add('open');
}

function closeM(id){
  var el=document.getElementById(id);
  if(el){
    el.style.display='none';
    el.classList.remove('open');
    el.style.pointerEvents='none';
  }
  S.editId=null;
  // Reset pointer events on all modals
  document.querySelectorAll('.mo').forEach(function(m){
    if(!m.classList.contains('open') && m.style.display==='none'){
      m.style.pointerEvents='none';
    }
  });
}

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
  var all = filterByBranch(S.lessons || []);
  if(P().seeAll) return all;
  var cuId = CU ? CU.id : null;
  var mt = S.tutors.find(function(t){
    return t.accId === cuId || t.acc_uid === cuId;
  });
  if(!mt) return [];
  return all.filter(function(l){
    return l.tutorId === mt.id || l.tutor_id === mt.id;
  });
}
function myStudents(){
  var all = filterByBranch(S.students || []);
  try{ if(P().seeAll) return all; }catch(e){ return all; }
  if(R() !== 'tutor') return all;
  var cuId = CU ? CU.id : null;
  var mt = S.tutors.find(function(t){
    return t.accId === cuId || t.acc_uid === cuId || t.id === cuId;
  });
  if(!mt) return all; // fallback: show all if tutor record not found
  return all.filter(function(s){
    if(s.tutorId === mt.id || s.tutor_id === mt.id) return true;
    if(Array.isArray(s.tutorIds) && s.tutorIds.indexOf(mt.id) >= 0) return true;
    // also check raw tutor_ids string
    if(typeof s.tutor_ids === 'string' && s.tutor_ids.split(',').indexOf(mt.id) >= 0) return true;
    return false;
  });
}
function myTutor(){
  var cuId = CU ? CU.id : null;
  return S.tutors.find(function(t){
    return t.accId === cuId || t.acc_uid === cuId;
  }) || null;
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
  var nb=document.getElementById('nb-s'); if(nb)nb.textContent=myStudents().filter(function(s){return s.status==='active';}).length;
  var statsHtml='<div class="sc blue">'
    +'<div class="slbl">\u0410\u043A\u0442\u0438\u0432\u043D\u0438\u0445 \u0443\u0447\u043D\u0456\u0432</div>'
    +'<div class="sval">'+ms.filter(function(s){return s.status==='active';}).length+'</div>'
    +'<div class="ssub">\u0417\u0430\u0433\u0430\u043B\u043E\u043C: '+ms.length+'</div><span class="sico">\u25CE</span></div>'
    +'<div class="sc green">'
    +'<div class="slbl">\u0417\u0430\u043D\u044F\u0442\u044C \u0446\u044C\u043E\u0433\u043E \u043C\u0456\u0441\u044F\u0446\u044F</div>'
    +'<div class="sval">'+Math.round(monthL.filter(function(l){return l.status!=='cancelled';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10+'</div>'
    +'<div class="ssub">\u041F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E: '+Math.round(monthL.filter(function(l){return l.status==='done'||l.status==='completed'||l.status==='makeup';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10+'</div>'
    +'<span class="sico">\u25C9</span></div>';
  if(P().seeIncome && R()!=='tutor'){
    var inc=monthL.filter(function(l){
      return l.status==='done'||l.status==='completed'||l.status==='makeup';
    }).reduce(function(a,l){return a+lessonTotal(l);},0);
    statsHtml+='<div class="sc yellow">'      +'<div class="slbl">Дохід цього місяця</div>'      +'<div class="sval">'+Math.round(inc).toLocaleString('uk-UA')+'₴</div>'      +'<div class="ssub">Отримано</div><span class="sico">◈</span></div>';
    } else if(R()!=='tutor') {


    statsHtml+='<div class="sc yellow" style="opacity:.4">'
      +'<div class="slbl">\u0414\u043E\u0445\u0456\u0434 \u043C\u0456\u0441\u044F\u0446\u044F</div>'
      +'<div class="sval">\uD83D\uDD12</div>'
      +'<div class="ssub">\u0422\u0456\u043B\u044C\u043A\u0438 \u0434\u0438\u0440\u0435\u043A\u0442\u043E\u0440</div><span class="sico">\u25C8</span></div>';
  }
    if(R()!=='tutor'){
  statsHtml+='<div class="sc red">'
    +'<div class="slbl">\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0456\u0432</div>'
    +'<div class="sval">'+S.tutors.length+'</div>'
    +'<div class="ssub">\u0410\u043A\u0442\u0438\u0432\u043D\u0438\u0445</div><span class="sico">\u25C8</span></div>';
  }
  document.getElementById('dash-stats').innerHTML=statsHtml;
}

function renderDashKpi(){
  var offset=S.dashWeekOffset||0;
  var wr=getWeekRange(offset);
  var lbl=document.getElementById('dash-week-lbl');
  if(lbl)lbl.textContent=wr.label;
  var tlbl=document.getElementById('dash-tutor-week-lbl');
  if(tlbl)tlbl.textContent=wr.label;

  var allL=myLessons();
  var weekL=allL.filter(function(l){return inWeek(l.date,wr);});
  var _myT=myTutor();
  var weekComms=(S.comms||[]).filter(function(c){
    return inWeek(c.date,wr)&&(R()!=='tutor'||!_myT||(c.tutorId||c.tutor_id)===_myT.id);
  });

  // Години проведених занять (тиждень)
  var doneH = Math.round(weekL.filter(function(l){return l.status==='done'||l.status==='completed'||l.status==='makeup';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  // Години запланованих (тиждень, без скасованих)
  var plannedH = Math.round(weekL.filter(function(l){return l.status==='planned'||l.status==='scheduled';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  // Загальні години тижня (проведені + заплановані, без скасованих і пропущених)
  var totalH = Math.round(weekL.filter(function(l){return l.status!=='cancelled';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  // Пропущені — некомпенсовані за 3 місяці, в годинах
  var threeMonthsAgo = new Date(); threeMonthsAgo.setMonth(threeMonthsAgo.getMonth()-3);
  var from3m = localDateStr(threeMonthsAgo);
  var missed3m = uncoveredMissedFilter(allL.filter(function(l){ return l.date>=from3m; }));
  var missedH = Math.round(allL.filter(function(l){return l.status==='missed'&&l.date>=from3m;}).reduce(function(s,l){return s+uncoveredMissedHours(l);},0)*10)/10;
  var cancelled = weekL.filter(function(l){return l.status==='cancelled';}).length;
  var plannedCnt = weekL.filter(function(l){return l.status==='planned'||l.status==='scheduled';}).length;
  var totalComms = weekComms.length;
  // Виконання плану = проведені / (проведені + заплановані) * 100
  var pct = totalH>0 ? Math.round(doneH/totalH*100) : 0;

  var wrPrev=getWeekRange(offset-1);
  var prevL  =allL.filter(function(l){return inWeek(l.date,wrPrev);});
  var prevDoneH =Math.round(prevL.filter(function(l){return l.status==='done'||l.status==='completed'||l.status==='makeup';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  var prevTotalH=Math.round(prevL.filter(function(l){return l.status!=='cancelled';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  var prevMissed3m=uncoveredMissedFilter(prevL);
  var prevMissedH =Math.round(prevMissed3m.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  var prevComms=(S.comms||[]).filter(function(c){return inWeek(c.date,wrPrev);}).length;
  var prevPct=prevTotalH>0?Math.round(prevDoneH/prevTotalH*100):0;

  function trend(cur,prev){
    if(prev===0&&cur===0)return {cls:'same',txt:'\u2014 0'};
    if(prev===0)return {cls:'up',txt:'\u2191 \u043d\u043e\u0432\u0438\u0439'};
    var d=Math.round((cur-prev)*10)/10;
    return d>0?{cls:'up',txt:'\u2191 +'+d}:d<0?{cls:'down',txt:'\u2193 '+d}:{cls:'same',txt:'= '+cur};
  }

  var kpis=[
    {ico:'\u2705',val:doneH+'\u0433', lbl:'\u041f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u043e \u0437\u0430\u043d\u044f\u0442\u044c', sub:plannedCnt+' \u0449\u0435 \u0437\u0430\u043f\u043b\u0430\u043d\u043e\u0432\u0430\u043d\u043e', accent:'var(--tut)', tr:trend(doneH,prevDoneH)},
    {ico:'\u274c',val:missedH+'\u0433', lbl:'\u041f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u043e \u0443\u0447\u043d\u044f\u043c\u0438', sub:'\u0421\u043a\u0430\u0441\u043e\u0432\u0430\u043d\u043e: '+cancelled, accent:'var(--danger)', tr:trend(missedH,prevMissedH)},
    {ico:'\ud83d\udcac',val:totalComms, lbl:'\u041a\u043e\u043c\u0443\u043d\u0456\u043a\u0430\u0446\u0456\u0439', sub:'\u0414\u0437\u0432\u0456\u043d\u043a\u0438 \u0442\u0430 \u043f\u043e\u0432\u0456\u0434\u043e\u043c\u043b\u0435\u043d\u043d\u044f', accent:'var(--adm)', tr:trend(totalComms,prevComms)},
    {ico:'\ud83d\udcc8',val:pct+'%', lbl:'\u0412\u0438\u043a\u043e\u043d\u0430\u043d\u043d\u044f \u043f\u043b\u0430\u043d\u0443', sub:doneH+'\u0433 \u0437 '+totalH+'\u0433', accent:'#a78bfa', tr:trend(pct,prevPct)},
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
    ? S.tutors.filter(function(t){return CU && t.accId===CU.id;})
    : S.tutors;

  if(!tutors.length){
    tbody.innerHTML='<tr><td colspan="8" class="empty" style="padding:20px">\u041D\u0435\u043C\u0430\u0454 \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0456\u0432</td></tr>';
    return;
  }

  var maxDoneH=Math.max.apply(null,tutors.map(function(t){
    return weekL.filter(function(l){return (l.tutorId===t.id||l.tutor_id===t.id)&&(l.status==='done'||l.status==='completed'||l.status==='makeup');}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0);
  }).concat([1]));

  var totalDoneH=0,totalMissedH=0,totalPlanned=0,totalTutComms=0,totalStudents=0;
  var rowsArr=[];
  tutors.forEach(function(t){
    var tl=weekL.filter(function(l){return l.tutorId===t.id||l.tutor_id===t.id;});
    // Проведено — години за тиждень
    var tDoneH  =Math.round(tl.filter(function(l){return l.status==='done'||l.status==='completed'||l.status==='makeup';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
    // Заплановано — кількість
    var tPlanned=tl.filter(function(l){return l.status==='planned'||l.status==='scheduled';}).length;
    // Заплановані години
    var tPlannedH=Math.round(tl.filter(function(l){return l.status==='planned'||l.status==='scheduled';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
    // Пропущені — некомпенсовані за 3 місяці, в годинах
    var t3mL=allL.filter(function(l){return (l.tutorId===t.id||l.tutor_id===t.id)&&l.date>=from3m;});
    var tMissedH=Math.round(t3mL.filter(function(l){return l.status==='missed';}).reduce(function(s,l){return s+uncoveredMissedHours(l);},0)*10)/10;
    var tComms  =weekComms.filter(function(c){return c.tutorId===t.id||c.tutor_id===t.id;}).length;
    var tStudents=S.students.filter(function(s){return (s.tutorId===t.id||s.tutor_id===t.id)&&s.status==='active';}).length;
    // Виконання = проведено / (проведено + заплановано) * 100
    var tTotalH =Math.round((tDoneH+tPlannedH)*10)/10;
    var tPct    =tTotalH>0?Math.round(tDoneH/tTotalH*100):(tPlanned>0?0:100);
    var barW    =maxDoneH>0?Math.round(tDoneH/maxDoneH*100):0;
    var pctColor=tPct>=80?'var(--tut)':tPct>=50?'var(--dir)':'var(--danger)';

    totalDoneH+=tDoneH; totalMissedH+=tMissedH;
    totalPlanned+=tPlanned; totalTutComms+=tComms; totalStudents+=tStudents;

    // Тренд проведених годин vs минулий тиждень
    var prevTl=prevL.filter(function(l){return l.tutorId===t.id||l.tutor_id===t.id;});
    var prevTDoneH=Math.round(prevTl.filter(function(l){return l.status==='done'||l.status==='completed'||l.status==='makeup';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
    var trendTxt='', trendCls='same';
    var dd=Math.round((tDoneH-prevTDoneH)*10)/10;
    if(dd>0){trendTxt='\u2191+'+dd;trendCls='up';}
    else if(dd<0){trendTxt='\u2193'+dd;trendCls='down';}
    else if(prevTDoneH>0){trendTxt='='+tDoneH;trendCls='same';}

    var rowHtml = '<tr>'
      +'<td style="min-width:180px"><div style="display:flex;align-items:center;gap:10px">'+mkAv(t.fn,t.ln,34,t.photo)
      +'<div style="min-width:0"><div style="font-weight:600;font-size:12px;line-height:1.3">'+t.fn+'</div>'
      +'<div style="font-weight:700;font-size:12px;line-height:1.3">'+t.ln+'</div>'
      +'<div style="font-size:10px;color:var(--t3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:130px">'+(t.subj||'\u2014')+'</div></div></div></td>'

      +'<td><div style="display:flex;align-items:center;gap:8px">'
      +'<span style="font-weight:700;font-size:18px;font-family:Syne,sans-serif;color:var(--tut)">'+tDoneH+'\u0433</span>'
      +(trendTxt?'<span class="kpi-badge '+trendCls+'" style="font-size:9px">'+trendTxt+'</span>':'')
      +'</div>'
      +'<div class="mini-bar"><div class="mini-fill" style="width:'+barW+'%;background:var(--tut)"></div></div></td>'

      +'<td style="text-align:center">'
      +'<span style="font-weight:600;font-size:15px;color:var(--t2)">'+tPlanned+'</span>'
      +'</td>'

      +'<td style="text-align:center">'
      +'<span style="font-weight:700;font-size:16px;color:'+(tMissedH>0?'var(--danger)':'var(--t3)')+'">'+tMissedH+'\u0433</span>'
      +'</td>'

      +'<td style="text-align:center">'
      +'<span style="font-weight:700;font-size:16px;color:var(--adm)">'+tComms+'</span>'
      +'</td>'

      +'<td style="text-align:center">'
      +'<span style="font-size:14px">'+tStudents+'</span>'
      +'</td>'

      +'<td>'
      +'<div style="font-weight:700;font-size:15px;color:'+pctColor+'">'+tPct+'%</div>'
      +'<div style="font-size:10px;color:var(--t3)">'+tDoneH+'\u0433 / '+tTotalH+'\u0433</div>'
      +'</td>'
      +'</tr>';
    rowsArr.push(rowHtml);
  });
  var rows = rowsArr.join('');

  // Підсумковий рядок
  totalDoneH=Math.round(totalDoneH*10)/10;
  totalMissedH=Math.round(totalMissedH*10)/10;
  var totalPct=0;
  if(R()!=='tutor'){
    var allTotalH=Math.round(weekL.filter(function(l){return l.status!=='cancelled';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
    totalPct=allTotalH>0?Math.round(totalDoneH/allTotalH*100):0;
    var totalPctColor=totalPct>=80?'var(--tut)':totalPct>=50?'var(--dir)':'var(--danger)';
    rows+='<tr style="background:rgba(255,255,255,.03);font-weight:700;border-top:2px solid var(--b1)">'
    +'<td><span style="font-size:12px;color:var(--t2);letter-spacing:.5px">\u0420\u0410\u0417\u041e\u041c / \u0421\u0415\u0420\u0415\u0414\u041d\u0404</span></td>'
    +'<td><span style="font-size:18px;font-family:Syne,sans-serif;color:var(--tut)">'+totalDoneH+'\u0433</span></td>'
    +'<td style="text-align:center;color:var(--t2)">'+totalPlanned+'</td>'
    +'<td style="text-align:center;color:'+(totalMissedH>0?'var(--danger)':'var(--t3)')+'">'+totalMissedH+'\u0433</td>'
    +'<td style="text-align:center;color:var(--adm)">'+totalTutComms+'</td>'
    +'<td style="text-align:center">'+totalStudents+'</td>'
    +'<td><span style="font-weight:700;color:'+totalPctColor+'">'+totalPct+'%</span></td>'
    +'</tr>';
  }
  tbody.innerHTML=rows;
}

function renderDashTrends(){
  if(!CU) return;
  var offset = S.dashWeekOffset||0;
  var curWr = getWeekRange(offset);
  var trendLbl = document.getElementById('dash-trend-week-lbl');
  if(trendLbl) trendLbl.textContent = curWr.label;
  var weeks = [];
  for(var i=3;i>=0;i--){
    var wr = getWeekRange(offset-i);
    var _mt2=myTutor();
    var weekL = myLessons().filter(function(l){return inWeek(l.date,wr);});
    var weekComms = (S.comms||[]).filter(function(c){
      return inWeek(c.date,wr)&&(R()!=='tutor'||!_mt2||(c.tutorId||c.tutor_id)===_mt2.id);
    });
    weeks.push({
      wr:wr,
      done: Math.round(weekL.filter(function(l){return l.status==='done'||l.status==='completed'||l.status==='makeup';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10,
      missed: uncoveredMissedFilter(weekL).length,
      planned:weekL.filter(function(l){return l.status==='planned'||l.status==='scheduled';}).length,
      comms:  weekComms.length,
    });
  }

  var tutors = R()==='tutor'
    ? S.tutors.filter(function(t){return CU && t.accId===CU.id;})
    : S.tutors;

  function miniChart(containerId, data, color, keyFn){
    var el = document.getElementById(containerId);
    if(!el) return;
    var max = Math.max.apply(null, data.map(keyFn).concat([1]));

    // Header bars
    var barsHtml = '<div class="trend-weeks">';
    data.forEach(function(w, i){
      var val = keyFn(w);
      var pct = Math.round(val/max*100);
      var isNow = (i===data.length-1);
      var fmt = {day:'2-digit',month:'2-digit'};
      var sun = new Date(w.wr.mon); sun.setDate(sun.getDate()+6);
      var lbl = w.wr.mon.toLocaleDateString('uk-UA',fmt)+'–'+sun.toLocaleDateString('uk-UA',fmt);
      barsHtml += '<div class="trend-week'+(isNow?' trend-week-now':'')+'">'
        +'<div class="trend-week-val">'+val+'</div>'
        +'<div class="trend-week-bar-wrap">'
          +'<div class="trend-week-bar" style="width:'+pct+'%;background:'+color+'"></div>'
        +'</div>'
        +'<div class="trend-week-lbl">'+lbl+'</div>'
        +'</div>';
    });
    barsHtml += '</div>';

    // Per-tutor rows
    var tutorRows = '';
    tutors.slice(0,6).forEach(function(t){
      var vals = data.map(function(w){
        if(containerId==='dash-trend-comms'){
          return (S.comms||[]).filter(function(c){return inWeek(c.date,w.wr)&&(c.tutor_id===t.id||c.tutorId===t.id);}).length;
        }
        return S.lessons.filter(function(l){return inWeek(l.date,w.wr)&&(l.tutor_id===t.id||l.tutorId===t.id)&&(l.status==='done'||l.status==='completed'||l.status==='makeup');}).length;
      });
      var tMax = Math.max.apply(null, vals.concat([1]));
      var total = vals[vals.length-1];

      tutorRows += '<div class="trend-tutor-row">'
        + mkAv(t.fn,t.ln,24,t.photo)
        + '<div class="trend-tutor-name">'+(t.ln||t.fn)+' '+(t.fn?t.fn[0]+'.':'')+'</div>'
        + '<div class="trend-tutor-bars">'
        + vals.map(function(v,i){
            var pct = Math.round(v/tMax*100);
            var isNow = (i===vals.length-1);
            return '<div class="trend-tutor-col">'
              +'<div class="trend-tutor-num">'+v+'</div>'
              +'<div class="trend-tutor-bar-wrap">'
                +'<div class="trend-tutor-bar" style="width:'+pct+'%;background:'+(isNow?color:'var(--b2)')+'"></div>'
              +'</div>'
              +'</div>';
          }).join('')
        + '</div>'
        + '<div class="trend-tutor-total">'+total+'</div>'
        + '</div>';
    });

    el.innerHTML = barsHtml + (tutorRows ? '<div class="trend-tutor-list">'+tutorRows+'</div>' : '');
  }

  miniChart('dash-trend-lessons', weeks, 'var(--tut)', function(w){return w.done;});
  miniChart('dash-trend-comms',   weeks, 'var(--adm)', function(w){return w.comms;});
}

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
      ?'<div style="max-height:220px;overflow-y:auto"><table><thead><tr><th>\u0423\u0447\u0435\u043D\u044C</th><th>\u0421\u0443\u043C\u0430</th><th>\u0414\u0430\u0442\u0430</th><th>\u0421\u0442\u0430\u0442\u0443\u0441</th></tr></thead><tbody>'
        +rec.map(function(p){
          return '<tr><td>'+sn(p.studentId)+'</td>'
            +'<td style="font-family:JetBrains Mono,monospace">'+(p.amount||0).toLocaleString('uk-UA')+'\u20B4</td>'
            +'<td style="font-size:11px">'+fd(p.date)+'</td>'
            +'<td>'+bst(p.status)+'</td></tr>';
        }).join('')+'</tbody></table></div>'
      :'<div class="empty" style="padding:20px"><div class="ei">\uD83D\uDCB3</div>\u041F\u043B\u0430\u0442\u0435\u0436\u0456\u0432 \u043D\u0435\u043C\u0430\u0454</div>';
  } else {
    if(rt)rt.textContent='\u0423\u0447\u043D\u0456';
    var msArr=myStudents();
    if(rb)rb.innerHTML=msArr.length
      ?'<div style="max-height:220px;overflow-y:auto"><table><thead><tr><th>\u0406\u043C\'\u044F</th><th>\u041F\u0440\u0435\u0434\u043C\u0435\u0442</th><th>\u0421\u0442\u0430\u0442\u0443\u0441</th></tr></thead><tbody>'
        +msArr.map(function(s){
          return '<tr><td>'+s.fn+' '+s.ln+'</td><td>'+(s.subject||'\u2014')+'</td><td>'+bst(s.status)+'</td></tr>';
        }).join('')+'</tbody></table></div>'
      :'<div class="empty" style="padding:20px"><div class="ei">\uD83D\uDC65</div>\u0423\u0447\u043D\u0456\u0432 \u043D\u0435 \u043F\u0440\u0438\u0437\u043D\u0430\u0447\u0435\u043D\u043E</div>';
  }

  // Subjects chart
  var sc={};ml.forEach(function(l){sc[l.subject]=(sc[l.subject]||0)+(parseFloat(l.dur)||60)/60;});
  var colors=['var(--adm)','var(--tut)','var(--dir)','var(--god2)','#a78bfa','#0ea5e9'];
  var maxS=Math.max.apply(null,Object.values(sc).concat([1]));
  var dashSubjEl=document.getElementById('dash-subj');
  if(dashSubjEl) dashSubjEl.innerHTML=Object.entries(sc)
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
  // dash-pay card removed from dashboard
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
  var _myTc=myTutor();
  var comms=[].concat(S.comms||[])
    .filter(function(c){return R()!=='tutor'||!_myTc||(c.tutorId||c.tutor_id)===_myTc.id;})
    .sort(function(a,b){return (b.date||'').localeCompare(a.date||'');}).slice(0,20);
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
  var _q=(document.getElementById('gsearch')||{value:''}).value.toLowerCase().trim();
  var data=myStudents().filter(function(s){
    if(!_q) return true;
    return (s.fn+' '+s.ln).toLowerCase().includes(_q)
        || (s.ln+' '+s.fn).toLowerCase().includes(_q)
        || (s.phone||'').includes(_q)
        || (s.parentPhone||'').includes(_q)
        || (s.email||'').toLowerCase().includes(_q);
  });
  if(sfCur!=='all') data=data.filter(function(s){return s.status===sfCur;});
  var tot=document.getElementById('st-total');
  if(tot) tot.textContent=data.length+' \u0437 '+myStudents().length;
  var ce=can('students');
  var html=data.length?data.map(function(s){
    var btns=ce
      ?('<button class="btn btn-g btn-sm" onclick="openStudM(this.dataset.id)" data-id="'+s.id+'">\u270F\uFE0F</button>'
        +'<button class="btn btn-sm" style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger)" onclick="delStudent(this.dataset.id)" data-id="'+s.id+'">\uD83D\uDDD1</button>')
      :'<span style="font-size:10px;color:var(--t3)">\u043F\u0435\u0440\u0435\u0433\u043B\u044F\u0434</span>';
    var _tids=studentTutorIds(s);
    var _tnames=_tids.map(tn).filter(function(n){return n&&n!=='\u2014';});
    var _subjTxt=studentSubjects(s).join(', ')||s.subject||'';
    return '<tr>'
      +'<td><div style="display:flex;align-items:center;gap:8px">'+mkAv(s.fn,s.ln)+'<div><div style="font-weight:600;font-size:13px">'+s.fn+' '+s.ln+'</div></div></div></td>'
      +'<td style="font-size:12px;color:var(--t2)">'+(s.age||'\u2014')+' / '+(s.grade||'\u2014')+'</td>'
      +'<td>'+(_subjTxt||'\u2014')+'</td>'
      +'<td style="font-size:12px;line-height:1.6">'+(_tnames.length?_tnames.join('<br>'):'\u2014')+'</td>'
      +'<td>'+bst(s.status)+'</td>'
      +'<td style="font-size:12px;color:var(--t2)">'+(s.parentPhone||s.phone||s.email||'\u2014')+'</td>'
      +'<td><div style="display:flex;gap:3px">'+btns+'</div></td>'
      +'</tr>';
  }).join(''):'<tr><td colspan="7"><div class="empty"><div class="ei">\uD83D\uDC65</div>\u0423\u0447\u043D\u0456\u0432 \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E</div></td></tr>';
  document.getElementById('st-table').innerHTML=html;
}
function renderLessons(){
  var stf = document.getElementById('lf-stat');
  var sdf = document.getElementById('lf-student');
  var ttf = document.getElementById('lf-tutor');
  var sv  = stf ? stf.value : '';
  var sdv = sdf ? sdf.value : '';
  var tv  = ttf ? ttf.value : '';

  if(sdf){
    var cur = sdf.value;
    popSelSearch('lf-student', [{id:'',fn:'Всі учні',ln:''}].concat(myStudents().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');})), 'id', function(s){return s.fn+(s.ln?' '+s.ln:'');}, '');
    if(cur){ sdf.value=cur; if(sdf._updateSearch) sdf._updateSearch(); }
  }
  // Фільтр по репетиторах (адміни/директори/бог; для репетиторів прихований через .tutor-hidden)
  if(ttf && R()!=='tutor'){
    var curT = ttf.value;
    popSelSearch('lf-tutor', [{id:'',fn:'Всі репетитори',ln:''}].concat((S.tutors||[]).slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');})), 'id', function(t){return t.fn+(t.ln?' '+t.ln:'');}, '');
    if(curT){ ttf.value=curT; if(ttf._updateSearch) ttf._updateSearch(); }
  }
  // Якщо селект обгорнутий пошуковим полем — ховаємо/показуємо всю обгортку за роллю
  if(ttf && ttf.parentElement && ttf.parentElement.classList.contains('srch-wrap')){
    ttf.parentElement.style.display = R()==='tutor' ? 'none' : '';
  }

  var data = [].concat(myLessons()).filter(function(l){
    // Покриті missed не показуємо в загальному списку (є як makeup)
    if(!sv && l.status==='missed' && isCoveredMissed(l)) return false;
    return true;
  }).sort(function(a,b){
    return new Date(b.date+'T'+(b.time||'00:00'))-new Date(a.date+'T'+(a.time||'00:00'));
  });
  if(sdv) data = data.filter(function(l){return (l.studentId||l.student_id)===sdv;});
  if(tv)  data = data.filter(function(l){return (l.tutorId||l.tutor_id)===tv;});
  if(sv)  data = data.filter(function(l){
    if(sv==='done') return l.status==='done'||l.status==='completed'||l.status==='makeup';
    if(sv==='missed') return l.status==='missed' && !isCoveredMissed(l);
    return l.status===sv;
  });

  var hasMissed = sv==='missed'||sv==='makeup'||sv==='makeup_planned'||(!sv&&data.some(function(l){return l.status==='missed'||l.status==='makeup'||l.status==='makeup_planned';}));
  var mc=document.getElementById('lt-miss-col'); if(mc) mc.style.display=hasMissed?'':'none';
  var mkc=document.getElementById('lt-makeup-col'); if(mkc) mkc.style.display=hasMissed?'':'none';

  var ce = can('lessons');
  var ri = function(l){return l.recurId?'<span style="color:var(--adm);font-size:10px">🔁</span>':'';};
  var html = data.length ? data.map(function(l){
    var mc2=hasMissed?('<td style="font-size:11px;color:var(--danger)">'+(l.missed_date?fd(l.missed_date):'—')+'</td>'):'';
    var mk2=hasMissed?('<td style="font-size:11px;color:#f59e0b">'+(l.makeup_date?fd(l.makeup_date):'—')+'</td>'):'';
    var btns=ce
      ?('<button class="btn btn-g btn-sm" onclick="openLessM(this.dataset.id)" data-id="'+l.id+'">✏️</button>'
        +'<button class="btn btn-sm" style="background:rgba(248,113,113,.1);color:var(--danger)" onclick="delLesson(this.dataset.id)" data-id="'+l.id+'">🗑</button>')
      :'';
    return '<tr>'
      +'<td>'+sn(l.studentId||l.student_id)+'</td>'
      +'<td>'+(l.subject||'—')+' '+ri(l)+'</td>'
      +'<td style="font-size:12px">'+(l.tutorId||l.tutor_id?tn(l.tutorId||l.tutor_id):'—')+'</td>'
      +'<td style="font-family:JetBrains Mono,monospace;font-size:11px">'+fd(l.date)+' '+(l.time||'')+'</td>'
      +'<td>'+(l.dur||60)+' хв</td>'+mc2+mk2
      +'<td style="font-size:11px;color:var(--t2)">'+(l.notes||'—')+(l.games?'<div style="color:var(--adm);margin-top:2px">🎧 '+l.games+'</div>':'')+'</td>'
      +'<td>'+bst(l.status)+'</td>'
      +'<td><div style="display:flex;gap:3px">'+btns+'</div></td></tr>';
  }).join('')
  : '<tr><td colspan="10"><div class="empty"><div class="ei">📚</div>Занять немає</div></td></tr>';
  document.getElementById('lt-table').innerHTML = html;
}

function renderPayments(){
  // Show invoice toolbar only for god/director
  var invToolbar = document.getElementById('inv-toolbar');
  if(invToolbar) invToolbar.style.display = (R()==='god'||R()==='director') ? 'block' : 'none';

  // Допоміжна функція вартості заняття (ставка за годину × тривалість)
  function lessonAmt(l){
    return lessonTotal(l);
  }

  var myLess = myLessons();
  // Отримано = проведені заняття
  var paid = myLess.filter(function(l){
    return l.status==='done'||l.status==='completed'||l.status==='makeup';
  }).reduce(function(a,l){ return a+lessonAmt(l); }, 0);

  // Очікується = заплановані заняття
  var pend = myLess.filter(function(l){
    return l.status==='planned'||l.status==='scheduled';
  }).reduce(function(a,l){ return a+lessonAmt(l); }, 0);

  // Різниця = Очікується − Отримано
  var diff = pend - paid;

  document.getElementById('py-paid').textContent = Math.round(paid).toLocaleString('uk-UA')+'\u20B4';
  document.getElementById('py-pend').textContent = Math.round(pend).toLocaleString('uk-UA')+'\u20B4';
  var overEl = document.getElementById('py-over');
  if(overEl){
    overEl.textContent = (diff>=0?'+':'')+Math.round(diff).toLocaleString('uk-UA')+'\u20B4';
    overEl.style.color = diff<0 ? 'var(--danger)' : diff>0 ? 'var(--tut)' : '';
  }
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

function gSearch(q){
  if(S.currentPage!=='students') nav('students');
  else renderStudents();
}

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
  var pg=document.getElementById('pg-reports');
  if(!pg)return;

  // Populate tutor filter
  var tutSel=document.getElementById('an-tutor');
  if(tutSel){
    var curTut=tutSel.value;
    tutSel.innerHTML='<option value="">\u0412\u0441\u0456 \u0440\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0438</option>'
      +(S.tutors||[]).map(function(t){return '<option value="'+t.id+'"'+(t.id===curTut?' selected':'')+'>'+t.fn+' '+t.ln+'</option>';}).join('');
    if(curTut) tutSel.value=curTut;
  }

  // Date range
  var range=(document.getElementById('rc-range')||{value:'month'}).value;
  var now=new Date(), fromDate=new Date(now);
  if(range==='week'){
    var day=now.getDay()||7; fromDate=new Date(now); fromDate.setDate(now.getDate()-day+1); fromDate.setHours(0,0,0,0);
  } else if(range==='2week'){
    fromDate.setDate(now.getDate()-14);
  } else if(range==='4week'){
    fromDate.setDate(now.getDate()-28);
  } else if(range==='month'){
    fromDate=new Date(now.getFullYear(),now.getMonth(),1);
  } else if(range==='3month'){
    fromDate=new Date(now.getFullYear(),now.getMonth()-3,1);
  } else if(range==='6month'){
    fromDate=new Date(now.getFullYear(),now.getMonth()-6,1);
  } else if(range==='year'){
    fromDate=new Date(now.getFullYear(),0,1);
  } else {
    fromDate=new Date(0);
  }
  var fromStr=localDateStr(fromDate);
  var toStr=localDateStr(now);

  // Range label
  var lbl=document.getElementById('an-range-lbl');
  if(lbl && range!=='all'){
    lbl.textContent=fromDate.toLocaleDateString('uk-UA',{day:'numeric',month:'short'})
      +' \u2014 '+now.toLocaleDateString('uk-UA',{day:'numeric',month:'short',year:'numeric'});
  } else if(lbl){ lbl.textContent=''; }

  var selTutor=(tutSel||{value:''}).value;
  var allLessons=(S.lessons||[]).filter(function(l){return l.date>=fromStr&&l.date<=toStr;});
  var lessons=selTutor?allLessons.filter(function(l){return (l.tutorId||l.tutor_id)===selTutor;}):allLessons;
  var students=selTutor
    ?(S.students||[]).filter(function(s){return (s.tutorId||s.tutor_id)===selTutor||((s.tutorIds||[]).indexOf(selTutor)>=0);})
    :(S.students||[]);

  function h(arr){return Math.round(arr.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;}

  var done    =lessons.filter(function(l){return l.status==='done'||l.status==='completed'||l.status==='makeup';});
  var missed  =uncoveredMissedFilter(lessons.filter(function(l){return l.status!=='cancelled';}));
  var planned =lessons.filter(function(l){return l.status==='planned'||l.status==='scheduled';});
  var cancelled=lessons.filter(function(l){return l.status==='cancelled';});

  var doneH   =h(done), missedH=h(missed), plannedH=h(planned);
  var totalH  =Math.round((doneH+missedH+plannedH+h(cancelled))*10)/10;
  var income  =done.reduce(function(s,l){return s+lessonTotal(l);},0);
  var activeStudents=students.filter(function(s){return s.status==='active';}).length;
  var pct     =totalH>0?Math.round(doneH/totalH*100):0;

  // Subjects breakdown (від done)
  var subjMap={};
  done.forEach(function(l){var s=l.subject||'\u0406\u043d\u0448\u0435'; subjMap[s]=(subjMap[s]||0)+(parseFloat(l.dur)||60)/60;});
  var subjArr=Object.keys(subjMap).map(function(k){return{name:k,v:Math.round(subjMap[k]*10)/10};}).sort(function(a,b){return b.v-a.v;});

  // Tutors breakdown — всі репетитори, навіть без занять у період
  var tutorArr=[];
  if(!selTutor){
    var tutMap={};
    done.forEach(function(l){var tid=l.tutorId||l.tutor_id; if(tid) tutMap[tid]=(tutMap[tid]||0)+(parseFloat(l.dur)||60)/60;});
    // Include ALL tutors, even with 0 hours
    tutorArr=(S.tutors||[]).map(function(t){
      return{name:t.fn+' '+t.ln, v:Math.round((tutMap[t.id]||0)*10)/10, id:t.id};
    }).sort(function(a,b){return b.v-a.v;});
  }

  var COLORS=['#6366f1','#22c55e','#f59e0b','#ef4444','#14b8a6','#ec4899','#8b5cf6','#f97316','#06b6d4','#84cc16'];

  function pieChart(canvasId,data,total){
    var c=document.getElementById(canvasId);
    if(!c||!c.getContext)return;
    var ctx=c.getContext('2d');
    var cx=c.width/2,cy=c.height/2,r=Math.min(cx,cy)-8;
    ctx.clearRect(0,0,c.width,c.height);
    if(!total){ctx.beginPath();ctx.arc(cx,cy,r,0,2*Math.PI);ctx.fillStyle='#e5e7eb';ctx.fill();
      ctx.fillStyle='var(--t3)';ctx.font='11px sans-serif';ctx.textAlign='center';ctx.textBaseline='middle';ctx.fillText('0г',cx,cy);return;}
    var start=-Math.PI/2;
    data.forEach(function(d,i){
      if(!d.v)return;
      var angle=(d.v/total)*2*Math.PI;
      ctx.beginPath();ctx.moveTo(cx,cy);ctx.arc(cx,cy,r,start,start+angle);
      ctx.fillStyle=COLORS[i%COLORS.length];ctx.fill();start+=angle;
    });
    ctx.beginPath();ctx.arc(cx,cy,r*0.55,0,2*Math.PI);ctx.fillStyle='var(--s1)';ctx.fill();
    ctx.fillStyle='var(--t1)';ctx.font='bold 13px sans-serif';ctx.textAlign='center';ctx.textBaseline='middle';
    ctx.fillText(total+'\u0433',cx,cy);
  }

  function legend(data,total){
    return data.map(function(d,i){
      var pct2=total?Math.round(d.v/total*100):0;
      return '<div style="display:flex;align-items:center;gap:6px;margin-bottom:5px;font-size:12px">'
        +'<span style="width:10px;height:10px;border-radius:50%;background:'+COLORS[i%COLORS.length]+';flex-shrink:0;display:inline-block"></span>'
        +'<span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+d.name+'</span>'
        +'<span style="color:var(--t2);font-size:11px;white-space:nowrap">'+d.v+'\u0433 ('+pct2+'%)</span>'
        +'</div>';
    }).join('');
  }

  function kpiBox(ico,lbl,val,color){
    return '<div style="flex:1;min-width:120px;background:var(--s2);border-radius:12px;padding:12px 14px;border-left:3px solid '+color+'">'
      +'<div style="font-size:18px;font-weight:700;color:'+color+'">'+val+'</div>'
      +'<div style="font-size:11px;color:var(--t2);margin-top:2px">'+ico+' '+lbl+'</div>'
      +'</div>';
  }

  var statusData=[
    {name:'\u041f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u043e',v:doneH},
    {name:'\u041f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u043e',v:missedH},
    {name:'\u0417\u0430\u043f\u043b\u0430\u043d\u043e\u0432\u0430\u043d\u043e',v:plannedH},
    {name:'\u0421\u043a\u0430\u0441\u043e\u0432\u0430\u043d\u043e',v:h(cancelled)}
  ].filter(function(d){return d.v>0;});

  var tutDoneH=h(done); // для центру діаграми репетиторів

  document.getElementById('an-content').innerHTML=
    // KPI
    '<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:16px">'
    +kpiBox('\u2705','\u0413\u043e\u0434\u0438\u043d \u043f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u043e',doneH,'#22c55e')
    +kpiBox('\u274c','\u0413\u043e\u0434\u0438\u043d \u043f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u043e',missedH,'#ef4444')
    +kpiBox('\uD83D\uDCC5','\u0417\u0430\u043f\u043b\u0430\u043d\u043e\u0432\u0430\u043d\u043e',plannedH,'#6366f1')
    +(P().seeIncome?kpiBox('\uD83D\uDCB0','\u0414\u043e\u0445\u0456\u0434',income+'\u20b4','#14b8a6'):'')
    +kpiBox('\uD83D\uDC65','\u0410\u043a\u0442\u0438\u0432\u043d\u0438\u0445 \u0443\u0447\u043d\u0456\u0432',activeStudents,'#f59e0b')
    +kpiBox('\uD83D\uDCC8','\u0412\u0438\u043a\u043e\u043d\u0430\u043d\u043d\u044f',pct+'%','#8b5cf6')
    +'</div>'
    // Charts
    +'<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:16px">'
    // Status
    +'<div class="card"><div class="ch"><span class="ct">\u0421\u0442\u0430\u0442\u0443\u0441\u0438 \u0437\u0430\u043d\u044f\u0442\u044c</span></div>'
    +'<div style="display:flex;align-items:center;gap:16px;padding:12px">'
    +'<canvas id="pie-status" width="140" height="140" style="flex-shrink:0"></canvas>'
    +'<div style="flex:1;min-width:0">'+legend(statusData,totalH)+'</div>'
    +'</div></div>'
    // Subjects
    +(subjArr.length
      ?'<div class="card"><div class="ch"><span class="ct">\u041f\u043e \u043f\u0440\u0435\u0434\u043c\u0435\u0442\u0430\u0445 (\u0433\u043e\u0434)</span></div>'
      +'<div style="display:flex;align-items:center;gap:16px;padding:12px">'
      +'<canvas id="pie-subj" width="140" height="140" style="flex-shrink:0"></canvas>'
      +'<div style="flex:1;min-width:0">'+legend(subjArr,doneH)+'</div>'
      +'</div></div>':'')
    // Tutors — all tutors
    +(!selTutor
      ?'<div class="card"><div class="ch"><span class="ct">\u041f\u043e \u0440\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0430\u0445 (\u0433\u043e\u0434)</span></div>'
      +'<div style="display:flex;align-items:center;gap:16px;padding:12px">'
      +'<canvas id="pie-tutor" width="140" height="140" style="flex-shrink:0"></canvas>'
      +'<div style="flex:1;min-width:0">'+legend(tutorArr,tutDoneH)+'</div>'
      +'</div></div>':'')
    +'</div>';

  setTimeout(function(){
    pieChart('pie-status',statusData,totalH);
    if(subjArr.length) pieChart('pie-subj',subjArr,doneH);
    if(!selTutor) pieChart('pie-tutor',tutorArr,tutDoneH);
  },50);
}

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
    mkToast('\u0420\u0435\u0437\u0435\u0440\u0432\u043d\u0443 \u043a\u043e\u043f\u0456\u044e \u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043d\u043e \u2705');
    var _eL={branches:'\uD83C\uDFE2 \u0424.',tutors:'\uD83D\uDC64 \u0420.',students:'\uD83D\uDC65 \u0423.',lessons:'\uD83D\uDCCB \u0417.',payments:'\uD83D\uDCB3 \u041f.',subjects:'\uD83D\uDCDA \u041f.',comms:'\uD83D\uDCAC \u041a.',pricing_rules:'\uD83D\uDCB0 \u041f.',settings:'\u2699 \u041d.',profiles:'\uD83D\uDC64 \u041f.'};
    var _eR=['\uD83D\uDCE4 \u0417\u0431\u0435\u0440\u0435\u0436\u0435\u043d\u043e:'];
    Object.keys(backup.data).forEach(function(t){_eR.push((_eL[t]||t)+': '+(backup.data[t]||[]).length+' \u0437\u0430\u043f.');});
    alert(_eR.join('\n'));
  }catch(e){
    mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+e.message,'error');
  }
  if(btn){ btn.disabled=false; btn.textContent='\u2b07 \u0417\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0438\u0442\u0438 \u0440\u0435\u0437\u0435\u0440\u0432\u043d\u0443 \u043a\u043e\u043f\u0456\u044e'; }
}

function importBackupClick(){
  document.getElementById('backup-file-input').click();
}

async function importBackup(input){
  var file = input.files[0];
  if(!file){ return; }
  var btn = document.getElementById('restore-btn');
  if(btn){ btn.disabled=true; btn.textContent='\u0412\u0456\u0434\u043d\u043e\u0432\u043b\u0435\u043d\u043d\u044f...'; }

  try{
    var text = await file.text();
    var backup = JSON.parse(text);

    if(!backup.version || !backup.data){
      mkToast('\u041d\u0435\u0432\u0456\u0440\u043d\u0438\u0439 \u0444\u043e\u0440\u043c\u0430\u0442 \u0444\u0430\u0439\u043b\u0443','error');
      if(btn){btn.disabled=false;btn.textContent='\u2B06 \u0412\u0456\u0434\u043d\u043e\u0432\u0438\u0442\u0438 \u0437 \u043a\u043e\u043f\u0456\u0457';}
      return;
    }

    var created = (backup.created||'').slice(0,10) || '\u043d\u0435\u0432\u0456\u0434\u043e\u043c\u043e';
    if(!confirm('\u0412\u0456\u0434\u043d\u043e\u0432\u0438\u0442\u0438 \u0434\u0430\u043d\u0456 \u0437 \u043a\u043e\u043f\u0456\u0457 \u0432\u0456\u0434 '+created+'?\n\n\u26A0 \u0426\u0435 \u043f\u0435\u0440\u0435\u0437\u0430\u043f\u0438\u0448\u0435 \u0412\u0421\u0406 \u043f\u043e\u0442\u043e\u0447\u043d\u0456 \u0434\u0430\u043d\u0456!')){
      if(btn){btn.disabled=false;btn.textContent='\u2B06 \u0412\u0456\u0434\u043d\u043e\u0432\u0438\u0442\u0438 \u0437 \u043a\u043e\u043f\u0456\u0457';}
      input.value='';
      return;
    }

    var stats = {};
    var errors = [];
    // Restore tables in correct order (deps first)
    var order = ['branches','subjects','pricing_rules','tutors','students','lessons','payments','comms'];
    for(var i=0;i<order.length;i++){
      var table = order[i];
      var rows  = backup.data[table];
      if(!rows || !rows.length){ stats[table]=0; continue; }
      // Upsert backup rows in chunks of 50 (без delete — щоб не блокувало RLS)
      var inserted = 0;
      for(var j=0;j<rows.length;j+=50){
        var chunk = rows.slice(j,j+50);
        var res = await _sb.from(table).upsert(chunk, {onConflict:'id'});
        if(res.error){ errors.push(table+': '+res.error.message); }
        else inserted += chunk.length;
      }
      stats[table] = inserted;
    }

    // Settings: single upsert (may not have id field)
    if(backup.data.settings && backup.data.settings.length){
      var sr = await _sb.from('settings').upsert(backup.data.settings, {onConflict:'id'});
      if(sr.error) errors.push('settings: '+sr.error.message);
      else stats.settings = backup.data.settings.length;
    }

    if(errors.length){
      console.error('Backup import errors:', errors);
      mkToast('\u0412\u0456\u0434\u043d\u043e\u0432\u043b\u0435\u043d\u043e \u0437 \u043f\u043e\u043c\u0438\u043b\u043a\u0430\u043c\u0438. \u0414\u0435\u0442\u0430\u043b\u0456 \u0432 \u043a\u043e\u043d\u0441\u043e\u043b\u0456 (F12)','error');
    } else {
      mkToast('\u0412\u0456\u0434\u043d\u043e\u0432\u043b\u0435\u043d\u043e \u2705');
    }

    // Reload all data and re-render
    await loadAll();
    nav(S.currentPage||'dashboard');

    // Import summary
    var _iL={branches:'\uD83C\uDFE2 \u0424\u0456\u043b\u0456\u0457',tutors:'\uD83D\uDC64 \u0420\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0438',students:'\uD83D\uDC65 \u0423\u0447\u043d\u0456',lessons:'\uD83D\uDCCB \u0417\u0430\u043d\u044f\u0442\u0442\u044f',payments:'\uD83D\uDCB3 \u041f\u043b\u0430\u0442\u0435\u0436\u0456',subjects:'\uD83D\uDCDA \u041f\u0440\u0435\u0434\u043c\u0435\u0442\u0438',comms:'\uD83D\uDCAC \u041a\u043e\u043c\u0443\u043d\u0456\u043a\u0430\u0446\u0456\u0457',pricing_rules:'\uD83D\uDCB0 \u041f\u0440\u0430\u0432\u0438\u043b\u0430',settings:'\u2699\uFE0F \u041d\u0430\u043b\u0430\u0448\u0442.'};
    var _iR=['\uD83D\uDCE5 \u0417\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0435\u043d\u043e \u0437 \u043a\u043e\u043f\u0456\u0457 \u0432\u0456\u0434 '+created+':'];
    Object.entries(stats).forEach(function(e){_iR.push((_iL[e[0]]||e[0])+': '+e[1]+' \u0437\u0430\u043f\u0438\u0441\u0456\u0432');});
    if(errors.length) _iR.push('\n\u26A0\uFE0F \u041f\u043e\u043c\u0438\u043b\u043a\u0438: '+errors.join('; '));
    alert(_iR.join('\n'));

  }catch(e){
    console.error('importBackup exception:', e);
    mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+e.message,'error');
  }
  if(btn){btn.disabled=false;btn.textContent='\u2B06 \u0412\u0456\u0434\u043d\u043e\u0432\u0438\u0442\u0438 \u0437 \u043a\u043e\u043f\u0456\u0457';}
  input.value='';
}

async function initApp(){
  initTheme();
  // ── VIEWER MODE: відкриття резервної копії без Supabase ──
  var _isViewer = window.location.search.includes('viewer=1');
  if(_isViewer){
    var _bkRaw = sessionStorage.getItem('crm_backup');
    if(!_bkRaw){ document.body.innerHTML='<div style="padding:40px;text-align:center"><h2>Помилка</h2><p>Дані не знайдено. <a href="viewer.html">Відкрийте резервну копію знову</a></p></div>'; return; }
    var _bk = JSON.parse(_bkRaw);
    var _d  = _bk.data;
    S.students     = (_d.students    ||[]).map(normalizeStudent);
    S.tutors       = (_d.tutors      ||[]).map(normalizeTutor);
    S.lessons      = (_d.lessons     ||[]).map(normalizeLesson);
    S.payments     = (_d.payments    ||[]).map(normalizePayment);
    S.comms        = (_d.comms       ||[]).map(normalizeComm);
    S.branches     = _d.branches     ||[];
    S.subjects     = _d.subjects     ||[];
    S.pricingRules = (_d.pricing_rules||[]).map(normalizePricingRule);
    S.tasks        = (_d.tasks       ||[]).map(normalizeTask);
    S.payrollItems = (_d.payroll_items||[]).map(normalizePayrollItem);
    S.settings     = (_d.settings    ||[{}])[0]||{};
    S.users        = _d.profiles     ||[];
    // Мок поточного користувача — God-режим для перегляду всього
    CU = { id:'viewer', fn:'Перегляд', ln:'(резервна копія)', role:'god', perms:{} };
    applyGodConfig();
    // Ховаємо форму логіну, показуємо головний екран
    var asEl=document.getElementById('as'); if(asEl)asEl.style.display='block';
    var lsEl=document.getElementById('ls'); if(lsEl)lsEl.style.display='none';
    var setupEl=document.getElementById('setup'); if(setupEl)setupEl.style.display='none';
    // Прибираємо лоадер якщо є
    var loadDiv=document.getElementById('app-loading'); if(loadDiv)loadDiv.remove();
    // Ін'єкція CSS для перемикання сторінок (як в startApp)
    if(!document.getElementById('__pcss__')){
      var _st=document.createElement('style');
      _st.id='__pcss__';
      _st.textContent='.page{display:none!important}.page.active{display:block!important;padding-bottom:40px}';
      document.head.appendChild(_st);
    }
    window._viewerMode = true;
    buildSidebar(); updateSBUser(); updateBranchSelector();
    document.body.className = document.body.className.replace(/\brole-\w+\b/g,'');
    document.body.classList.add('role-god');
    // Показуємо банер "Режим перегляду"
    var banner=document.createElement('div');
    banner.style.cssText='position:fixed;top:0;left:0;right:0;z-index:9999;background:#f59e0b;color:#fff;text-align:center;padding:5px;font-size:12px;font-weight:700;pointer-events:none';
    banner.textContent='\uD83D\uDC41 \u0420\u0415\u0416\u0418\u041c \u041f\u0415\u0420\u0415\u0413\u041b\u042f\u0414\u0423 \u2014 \u0420\u0435\u0437\u0435\u0440\u0432\u043d\u0430 \u043a\u043e\u043f\u0456\u044f \u0432\u0456\u0434 '+((_bk.created||'').slice(0,10))+' \u2014 \u0417\u043c\u0456\u043d\u0438 \u043d\u0435 \u0437\u0431\u0435\u0440\u0456\u0433\u0430\u044e\u0442\u044c\u0441\u044f';
    document.body.appendChild(banner);
    var contentEl=document.getElementById('content'); if(contentEl)contentEl.style.paddingTop='30px';
    nav('dashboard');
    return;
  }
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
  _sb = createClient(SUPABASE_URL, SUPABASE_ANON, {
    auth: {
      autoRefreshToken: true,
      persistSession: true,
      detectSessionInUrl: false
    }
  });
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
    } else if(event === 'TOKEN_REFRESHED' && session){
      // Токен оновлено — нічого додаткового не потрібно, Supabase JS вже використовує новий
      console.log('Token refreshed automatically');
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
  window._pendingWrites = (window._pendingWrites||0) + 1;
  var dot=document.getElementById('syncdot'), lbl=document.getElementById('sync-lbl');
  if(dot) dot.className='sync-dot saving';
  if(lbl){ lbl.textContent='\u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043D\u043D\u044F\u2026'; lbl.style.color='var(--warn)'; }
  // Запобіжник: якщо через якийсь неврахований шлях setSynced не викличеться,
  // не блокуємо закриття вкладки назавжди — знімаємо прапорець через 15с.
  clearTimeout(window._pendingWritesSafety);
  window._pendingWritesSafety = setTimeout(function(){ window._pendingWrites = 0; }, 15000);
}
function setSynced(){
  window._pendingWrites = Math.max(0, (window._pendingWrites||1) - 1);
  clearTimeout(_syncTimer);
  var dot=document.getElementById('syncdot'), lbl=document.getElementById('sync-lbl');
  if(dot) dot.className='sync-dot ok';
  if(lbl){ lbl.textContent='\u0441\u0438\u043D\u0445\u0440\u043E\u043D\u0456\u0437\u043E\u0432\u0430\u043D\u043E'; lbl.style.color='var(--tut)'; }
  _syncTimer = setTimeout(function(){ if(lbl){lbl.textContent='\u043E\u043D\u043B\u0430\u0439\u043D';lbl.style.color='var(--t3)';} }, 2500);
}
// Захист від втрати даних на iOS: якщо користувач намагається закрити вкладку
// чи перезавантажити сторінку ПОКИ запис ще летить до бази — попереджаємо.
// Це головний захист від "не зберігається на iPhone" — коли людина тисне
// Зберегти і одразу згортає Safari/PWA, не чекаючи підтвердження.
if(!window._unloadGuardSet){
  window._unloadGuardSet = true;
  window.addEventListener('beforeunload', function(e){
    if(window._pendingWrites > 0){
      e.preventDefault();
      e.returnValue = '';
      return '';
    }
  });
}

// =
// DATA LOADING
// =
async function loadAll(){
  setSaving();
  try{
  await ensureFreshSession(); // оновити токен завчасно, якщо він майже протермінований
  const tables = [
    { table:'branches',      key:'branches' },
    { table:'tutors',        key:'tutors' },
    { table:'students',      key:'students' },
    { table:'lessons',       key:'lessons',  order:'date' },
    { table:'payments',      key:'payments', order:'date' },
    { table:'subjects',      key:'subjects' },
    { table:'comms',         key:'comms',    order:'date' },
    { table:'pricing_rules', key:'pricingRules' },
    { table:'tasks',         key:'tasks' },
    { table:'payroll_items', key:'payrollItems' },
    { table:'act_log',       key:'actLog' },
  ];
  function fetchAll(){
    return Promise.all(tables.map(function(t){
      var q = _sb.from(t.table).select('*');
      if(t.order) q = q.order(t.order, { ascending:false });
      return q;
    }));
  }
  var results = await fetchAll();
  // Якщо будь-який запит впав через протермінований токен — оновлюємо сесію і повторюємо весь батч
  var jwtErr = results.find(function(r){ return r && r.error && (String((r.error.message||'')+(r.error.code||'')).includes('JWT')||String((r.error.message||'')).includes('expired')||r.error.status===401); });
  if(jwtErr && await refreshIfExpired(jwtErr.error)){
    results = await fetchAll();
  }
  tables.forEach(function(t, i){ if(results[i] && !results[i].error && Array.isArray(results[i].data)) S[t.key] = results[i].data; });

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
  S.tasks = (S.tasks||[]).map(normalizeTask);
  S.payrollItems = (S.payrollItems||[]).map(normalizePayrollItem);
  S.actLog = (S.actLog||[]).map(function(r){return Object.assign({},r,{studentId:r.student_id,sentBy:r.sent_by,signedAt:r.signed_at});});
  try{ updateTaskAlert(); checkNewTaskNotifications(); }catch(e){}

  } finally {
    setSynced();
  }
}

// Normalize DB rows to match UI field names
function normalizeStudent(r){ 
  var tutorIds = r.tutor_ids ? (Array.isArray(r.tutor_ids) ? r.tutor_ids : r.tutor_ids.split(',').filter(Boolean)) : (r.tutor_id ? [r.tutor_id] : []);
  return Object.assign({}, r, { tutorId:r.tutor_id, crmStage:r.crm_stage||null, crmResponsible:r.crm_responsible||null, crmDate:r.crm_date||null, tutorIds:tutorIds, branchId:r.branch_id, parentFn:r.parent_fn, parentPhone:r.parent_phone }); 
}
function normalizeLesson(r){  return Object.assign({}, r, { studentId:r.student_id, tutorId:r.tutor_id, branchId:r.branch_id, recurId:r.recur_id, recurType:r.recur_type, recurIndex:r.recur_index }); }
function normalizePayment(r){ return Object.assign({}, r, { studentId:r.student_id, branchId:r.branch_id }); }
function normalizeTutor(r){   return Object.assign({}, r, { accId:r.acc_uid, branchId:r.branch_id }); }
function normalizeComm(r){    return Object.assign({}, r, { tutorId:r.tutor_id, studentId:r.student_id, branchId:r.branch_id }); }
function normalizePricingRule(r){ return Object.assign({}, r, { subjectMatch:r.subject_match, tutorId:r.tutor_id, gradeMatch:r.grade_match, durMin:r.dur_min }); }
function normalizeTask(r){ return Object.assign({}, r, { assigneeId:r.assignee_id, creatorId:r.creator_id, branchId:r.branch_id, deadlineTime:r.deadline_time, doneAt:r.done_at }); }
function normalizePayrollItem(r){ return Object.assign({}, r, { tutorId:r.tutor_id, createdBy:r.created_by }); }

// =
// REALTIME
// =
function startChannels(){
  var tableMap = {
    students:'students', tutors:'tutors', lessons:'lessons',
    payments:'payments', subjects:'subjects', comms:'comms',
    pricing_rules:'pricingRules', branches:'branches', profiles:'users',
    tasks:'tasks', payroll_items:'payrollItems'
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
    comms:normalizeComm, pricingRules:normalizePricingRule, tasks:normalizeTask, payrollItems:normalizePayrollItem };
  if(norm[key] && row) row = norm[key](row);

  if(ev==='INSERT')      S[key] = (S[key]||[]).concat([row]);
  else if(ev==='UPDATE') S[key] = (S[key]||[]).map(function(r){ return r.id===row.id ? row : r; });
  else if(ev==='DELETE') S[key] = (S[key]||[]).filter(function(r){ return r.id !== old.id; });

  refreshPage(key);
}

function refreshPage(key){
  try{updateTaskAlert();}catch(e){}
  if(typeof S === 'undefined' || !S.currentPage) return;
  var pg = S.currentPage;
  if(key==='tasks'){ try{updateTaskAlert(); checkNewTaskNotifications();}catch(e){} }
  if(key==='tasks' && pg==='tasks'){ try{renderTasks();}catch(e){} }
  if(key==='payrollItems' && pg==='payroll'){ try{renderPayroll();}catch(e){} }
  var map = {
    students:['students','dashboard','profile','crm'],
    tutors:['tutors','dashboard','profile'],
    lessons:['lessons','schedule','dashboard','profile'],
    payments:['payments','dashboard'],
    comms:['dashboard','profile'],
    subjects:['settings','lessons'],
    pricingRules:['settings'],
    branches:['settings'],
    users:['users']
  };
  if(!(map[key]||[]).includes(pg)) return;
  try {
    if(pg==='dashboard'  && typeof renderDash      ==='function') renderDash();
    else if(pg==='students'  && typeof renderStudents ==='function') renderStudents();
    else if(pg==='tutors'    && typeof renderTutors   ==='function') renderTutors();
    else if(pg==='schedule'  && typeof renderSch      ==='function') renderSch();
    else if(pg==='lessons'   && typeof renderLessons  ==='function') renderLessons();
    else if(pg==='payments'  && typeof renderPayments ==='function') renderPayments();
    else if(pg==='settings'  && typeof renderSettings ==='function') renderSettings();
    else if(pg==='users'     && typeof renderUsers    ==='function') renderUsers();
    else if(pg==='profile'   && typeof renderProfile  ==='function') renderProfile();
    else if(pg==='crm'       && typeof renderCrm      ==='function') renderCrm();
  else if(pg==='telephony' && typeof renderTelephony==='function') renderTelephony();
  } catch(e) { console.warn('refreshPage error:', e); }
}

// =
// DB HELPERS
// =

async function loadTableFresh(table){
  var tableMap = {
    students:'students', tutors:'tutors', lessons:'lessons',
    payments:'payments', subjects:'subjects', comms:'comms',
    pricing_rules:'pricingRules', branches:'branches', tasks:'tasks', payroll_items:'payrollItems'
  };
  var key = tableMap[table];
  if(!key) return;
  var norm = {students:normalizeStudent,lessons:normalizeLesson,
    payments:normalizePayment,tutors:normalizeTutor,
    comms:normalizeComm,pricingRules:normalizePricingRule,tasks:normalizeTask,payrollItems:normalizePayrollItem};
  var res = await _sb.from(table).select('*');
  if(res.error && await refreshIfExpired(res.error)){
    res = await _sb.from(table).select('*'); // повтор після оновлення сесії
  }
  if(res.error) return;
  var data = res.data || [];
  S[key] = norm[key] ? data.map(norm[key]) : data;
  setSynced();
  refreshPage(key);
}

// Автоматично оновлює сесію якщо JWT протермінований
async function refreshIfExpired(error){
  if(!error) return false;
  var msg = (error.message||'') + ' ' + (error.code||'') + ' ' + (error.hint||'');
  if(msg.includes('JWT expired') || msg.includes('token is expired') || msg.includes('No API key') || msg.includes('PGRST301') || error.status === 401){
    try{
      var r = await _sb.auth.refreshSession();
      if(r.error){
        // Не вдалось оновити — пробуємо м'яко перезайти з наявною сесією
        var s = await _sb.auth.getSession();
        if(s.data && s.data.session) return true;
        mkToast('\u0421\u0435\u0441\u0456\u044f \u0437\u0430\u043a\u0456\u043d\u0447\u0438\u043b\u0430\u0441\u044c. \u0423\u0432\u0456\u0439\u0434\u0456\u0442\u044c \u0437\u043d\u043e\u0432\u0443.','error');
        return false;
      }
      return true; // успішно оновлено
    }catch(e){ return false; }
  }
  return false;
}

// Читання з бази з автоповтором при протермінованому токені.
// buildQuery — функція, що будує запит: (from)=>from('table').select('*')...
async function sbSelect(buildQuery){
  var res = await buildQuery(_sb.from.bind(_sb));
  if(res && res.error && await refreshIfExpired(res.error)){
    res = await buildQuery(_sb.from.bind(_sb)); // повтор після оновлення сесії
  }
  return res;
}

// Проактивне оновлення токена: якщо до закінчення < 5 хв — оновлюємо завчасно.
// Викликається перед завантаженням даних і періодично, щоб не ловити "JWT expired".
async function ensureFreshSession(){
  try{
    var s = await _sb.auth.getSession();
    var sess = s.data && s.data.session;
    if(!sess) return;
    var expAt = sess.expires_at ? sess.expires_at*1000 : 0; // сек → мс
    if(expAt && (expAt - Date.now() < 5*60*1000)){
      await _sb.auth.refreshSession();
    }
  }catch(e){}
}
window.ensureFreshSession = ensureFreshSession;


// ══════════ ЖУРНАЛ ЗМІН (AUDIT LOG) ══════════
var AUDIT_TABLE_LABELS = {
  students:'\u0423\u0447\u043D\u0456', lessons:'\u0417\u0430\u043D\u044F\u0442\u0442\u044F', payments:'\u041F\u043B\u0430\u0442\u0435\u0436\u0456',
  comms:'\u041A\u043E\u043C\u0443\u043D\u0456\u043A\u0430\u0446\u0456\u0457', tutors:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438', subjects:'\u041F\u0440\u0435\u0434\u043C\u0435\u0442\u0438',
  tasks:'\u0417\u0430\u0432\u0434\u0430\u043D\u043D\u044F', payroll_items:'\u0417\u0430\u0440\u043F\u043B\u0430\u0442\u0438', pricing_rules:'\u0426\u0456\u043D\u0438',
  branches:'\u0424\u0456\u043B\u0456\u0457', profiles:'\u0410\u043A\u0430\u0443\u043D\u0442\u0438'
};
var AUDIT_ACTION_LABELS = { insert:'\u0441\u0442\u0432\u043E\u0440\u0435\u043D\u043E', update:'\u0437\u043C\u0456\u043D\u0435\u043D\u043E', delete:'\u0432\u0438\u0434\u0430\u043B\u0435\u043D\u043E' };

// Короткий опис запису для журналу (щоб бачити ЩО саме змінили)
// Мапа полів → людські назви + форматери значень для журналу змін
var AUDIT_FIELD_LABELS = {
  fn:'\u0406\u043C\u02BC\u044F', ln:'\u041F\u0440\u0456\u0437\u0432\u0438\u0449\u0435', phone:'\u0422\u0435\u043B\u0435\u0444\u043E\u043D', email:'Email',
  subject:'\u041F\u0440\u0435\u0434\u043C\u0435\u0442', date:'\u0414\u0430\u0442\u0430', time:'\u0427\u0430\u0441', dur:'\u0422\u0440\u0438\u0432\u0430\u043B\u0456\u0441\u0442\u044C',
  status:'\u0421\u0442\u0430\u0442\u0443\u0441', price:'\u0426\u0456\u043D\u0430', notes:'\u041D\u043E\u0442\u0430\u0442\u043A\u0438', hw:'\u0414\u043E\u043C\u0430\u0448\u043D\u0454', games:'\u0410\u0443\u0434\u0456\u044E\u0432\u0430\u043D\u043D\u044F/\u0456\u0433\u0440\u0438',
  amount:'\u0421\u0443\u043C\u0430', method:'\u041C\u0435\u0442\u043E\u0434', grade:'\u041A\u043B\u0430\u0441', hourly_rate:'\u0421\u0442\u0430\u0432\u043A\u0430', tutor_id:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440',
  student_id:'\u0423\u0447\u0435\u043D\u044C', title:'\u041D\u0430\u0437\u0432\u0430', descr:'\u041E\u043F\u0438\u0441', deadline:'\u0414\u0435\u0434\u043B\u0430\u0439\u043D', report:'\u0417\u0432\u0456\u0442',
  label:'\u041F\u043E\u044F\u0441\u043D\u0435\u043D\u043D\u044F', percent:'\u0412\u0456\u0434\u0441\u043E\u0442\u043E\u043A', name:'\u041D\u0430\u0437\u0432\u0430', address:'\u0410\u0434\u0440\u0435\u0441\u0430',
  type:'\u0422\u0438\u043F', note:'\u0422\u0435\u043A\u0441\u0442', assignee_id:'\u0412\u0456\u0434\u043F\u043E\u0432\u0456\u0434\u0430\u043B\u044C\u043D\u0438\u0439', availability:'\u0420\u043E\u0431\u043E\u0447\u0456 \u0433\u043E\u0434\u0438\u043D\u0438'
};
var AUDIT_STATUS_LABELS = { planned:'\u0417\u0430\u043F\u043B\u0430\u043D\u043E\u0432\u0430\u043D\u043E', done:'\u041F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E', completed:'\u041F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E', missed:'\u041F\u0440\u043E\u043F\u0443\u0449\u0435\u043D\u043E', makeup:'\u0412\u0456\u0434\u043F\u0440\u0430\u0446\u044C\u043E\u0432\u0430\u043D\u043E', makeup_planned:'\u0412\u0456\u0434\u043F\u0440. \u0437\u0430\u043F\u043B\u0430\u043D\u043E\u0432\u0430\u043D\u043E', cancelled:'\u0421\u043A\u0430\u0441\u043E\u0432\u0430\u043D\u043E' };

function auditFmtVal(field, v){
  if(v==null||v==='') return '\u2014';
  if(field==='status') return AUDIT_STATUS_LABELS[v]||v;
  if(field==='tutor_id'){ var t=(S.tutors||[]).find(function(x){return x.id===v;}); return t?(t.fn+' '+t.ln):v; }
  if(field==='student_id'){ var s=(S.students||[]).find(function(x){return x.id===v;}); return s?(s.fn+' '+s.ln):v; }
  if(field==='assignee_id'){ var u=(S.users||[]).find(function(x){return x.id===v;}); return u?(u.fn+' '+u.ln):v; }
  if(field==='availability') return '(\u043E\u043D\u043E\u0432\u043B\u0435\u043D\u043E)';
  var sv=String(v); return sv.length>40?sv.slice(0,40)+'\u2026':sv;
}

// Порівнює новий об'єкт зі старим записом і повертає опис змін
function auditDiff(table, id, newData){
  try{
    var stateKey={students:'students',lessons:'lessons',payments:'payments',comms:'comms',tutors:'tutors',subjects:'subjects',tasks:'tasks',payroll_items:'payrollItems',branches:'branches',profiles:'users'}[table];
    var oldRec=stateKey?((S[stateKey]||[]).find(function(x){return x.id===id;})):null;
    if(!oldRec) return null;
    var parts=[], skip=['updated_at','created_at','id','branch_id','split_group_id','split_index','recurId','recur_id'];
    Object.keys(newData||{}).forEach(function(k){
      if(skip.indexOf(k)>=0) return;
      // camelCase-дублікати в стані (tutorId vs tutor_id) — беремо snake з newData, порівнюємо з обома
      var camel=k.replace(/_([a-z])/g,function(m,c){return c.toUpperCase();});
      var oldV=(oldRec[k]!==undefined?oldRec[k]:oldRec[camel]);
      var newV=newData[k];
      var oldN=(oldV==null?'':String(oldV)), newN=(newV==null?'':String(newV));
      if(oldN===newN) return;
      var lbl=AUDIT_FIELD_LABELS[k]||k;
      parts.push(lbl+': '+auditFmtVal(k,oldV)+' \u2192 '+auditFmtVal(k,newV));
    });
    return parts.length?parts.join(', '):null;
  }catch(e){ return null; }
}

function auditDescribe(table, data, id){
  try{
    if(!data) return id||'';
    if(table==='students'||table==='tutors'||table==='profiles') return ((data.fn||'')+' '+(data.ln||'')).trim()||id||'';
    if(table==='lessons'){ var st=(S.students||[]).find(function(s){return s.id===(data.student_id||data.studentId);}); return (data.subject||'')+(st?' \u2014 '+st.fn+' '+st.ln:''); }
    if(table==='payments'){ return (data.amount!=null?data.amount+'\u20B4':'')+(data.method?' ('+data.method+')':''); }
    if(table==='comms'){ return data.type||''; }
    if(table==='subjects'){ return data.name||''; }
    if(table==='tasks'){ return data.title||''; }
    if(table==='payroll_items'){ return (data.label||'')+(data.amount!=null?' '+data.amount+'\u20B4':'')+(data.percent!=null?' '+data.percent+'%':''); }
    if(table==='branches'){ return data.name||''; }
    return data.name||data.title||data.label||id||'';
  }catch(e){ return id||''; }
}

// Запис у журнал (не блокує основну дію, тихо ковтає помилки)
async function auditLog(action, table, id, data, diffText){
  try{
    if(!CU||!_sb||window._viewerMode) return;
    if(table==='audit_log') return; // без рекурсії
    // Для update показуємо ЩО саме змінилось; для insert/delete — опис запису
    var base=auditDescribe(table,data,id);
    var descr = (action==='update' && diffText) ? (base?base+' \u00B7 '+diffText:diffText) : base;
    var r=await _sb.from('audit_log').insert({
      user_id:CU.id, user_name:((CU.fn||'')+' '+(CU.ln||'')).trim(), user_role:CU.role||'',
      action:action, table_name:table, record_id:(id!=null?String(id):null),
      descr:descr, branch_id:myBranchId()||null
    });
    if(r&&r.error) console.warn('[audit_log] insert error:', r.error.message);
  }catch(e){ console.warn('[audit_log] insert exception:', e&&e.message); }
}

async function dbInsert(table, data){
  if(window._viewerMode){mkToast('\u0420\u0435\u0436\u0438\u043c \u043f\u0435\u0440\u0435\u0433\u043b\u044f\u0434\u0443 \u2014 \u0437\u043c\u0456\u043d\u0438 \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0456','error');return;}
  setSaving();
  try{
    var _ri = await _sb.from(table).insert(data); var error = _ri.error;
    if(error && await refreshIfExpired(error)){
      _ri = await _sb.from(table).insert(data); error = _ri.error;
    }
    if(error){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+error.message,'error'); throw error; }
    auditLog('insert', table, (data&&data.id)||null, data);
    setTimeout(function(){ loadTableFresh(table); }, 800);
  } finally {
    setSynced();
  }
}
async function dbUpdate(table, id, data){
  if(window._viewerMode){mkToast('\u0420\u0435\u0436\u0438\u043c \u043f\u0435\u0440\u0435\u0433\u043b\u044f\u0434\u0443 \u2014 \u0437\u043c\u0456\u043d\u0438 \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0456','error');return;}
  var _diff = auditDiff(table, id, data); // рахуємо до апдейту, поки старий запис у стані
  setSaving();
  try{
    // profiles table has no updated_at column
    var noTimestamp = ['profiles'];
    var updateData = noTimestamp.indexOf(table) >= 0 
      ? Object.assign({}, data)
      : Object.assign({}, data, {updated_at: new Date().toISOString()});
    var _ru = await _sb.from(table).update(updateData).eq('id', id); var error = _ru.error;
    if(error && await refreshIfExpired(error)){
      _ru = await _sb.from(table).update(updateData).eq('id', id); error = _ru.error;
    }
    if(error){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+error.message,'error'); throw error; }
    auditLog('update', table, id, data, _diff);
    setTimeout(function(){ loadTableFresh(table); }, 800);
  } finally {
    setSynced();
  }
}
async function dbDelete(table, id){
  if(window._viewerMode){mkToast('\u0420\u0435\u0436\u0438\u043c \u043f\u0435\u0440\u0435\u0433\u043b\u044f\u0434\u0443 \u2014 \u0437\u043c\u0456\u043d\u0438 \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0456','error');return;}
  setSaving();
  try{
    var _rd = await _sb.from(table).delete().eq('id',id); var error = _rd.error;
    if(error && await refreshIfExpired(error)){
      _rd = await _sb.from(table).delete().eq('id',id); error = _rd.error;
    }
    if(error){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+error.message,'error'); throw error; }
    auditLog('delete', table, id, (S[({students:'students',lessons:'lessons',payments:'payments',comms:'comms',tutors:'tutors',subjects:'subjects',tasks:'tasks',payroll_items:'payrollItems'}[table])||'']||[]).find(function(x){return x.id===id;}));
    setTimeout(function(){ loadTableFresh(table); }, 500);
  } finally {
    setSynced();
  }
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
    hourly_rate: (function(){var v=parseFloat(document.getElementById('s-rate')?.value);return isNaN(v)?null:v;})(),
    rates: collectRateRows(),
    phone:  document.getElementById('s-phone')?.value||'',
    email:  document.getElementById('s-email')?.value||'',
    // Предмет і репетитори виводяться з "Окремих ставок" (legacy-поля лишаються синхронізованими для таблиць/фільтрів/доступів)
    subject:(function(){var rr=collectRateRows();var ss=[];rr.forEach(function(r){var s=(r.subject||'').trim();if(s&&ss.indexOf(s)<0)ss.push(s);});return ss.join(', ');})(),
    tutor_id:(function(){var rr=collectRateRows();var f=rr.find(function(r){return r.tutor_id;});return f?f.tutor_id:null;})(),
    tutor_ids:(function(){var rr=collectRateRows();var tt=[];rr.forEach(function(r){if(r.tutor_id&&tt.indexOf(r.tutor_id)<0)tt.push(r.tutor_id);});return tt.join(',');})(),
    status: document.getElementById('s-status')?.value||'active',
    src:    document.getElementById('s-src')?.value||'referral',
    notes:  document.getElementById('s-notes')?.value||'',
    parent_fn:   (document.getElementById('s-parent-fn')?.value||'').trim(),
    parent_phone:(document.getElementById('s-parent-phone')?.value||'').trim(),
    crm_stage: document.getElementById('s-crm-stage')?.value||'lead',
    crm_responsible: document.getElementById('s-crm-resp')?.value||null,
    branch_id: myBranchId()||null,
  };
  // Auto-link to current tutor if none selected
  if(R()==='tutor' && !obj.tutor_id){
    var mt=myTutor();
    if(mt){ obj.tutor_id=mt.id; obj.tutor_ids=mt.id;
      if(!(obj.rates||[]).length) obj.rates=[{subject:'',tutor_id:mt.id,rate:0}];
    }
  }
  window._saving = true;
  try{
    var saved;
    if(S.editId){
      saved = await dbUpdate('students',S.editId,obj);
      var norm = normalizeStudent(Object.assign({id:S.editId},obj));
      var i = S.students.findIndex(function(x){return x.id===S.editId;});
      if(i>=0) S.students[i] = norm; else S.students.push(norm);
      mkToast('\u0423\u0447\u043D\u044F \u043E\u043D\u043E\u0432\u043B\u0435\u043D\u043E');
    } else {
      var newId = uid();
      var norm = normalizeStudent(Object.assign({id:newId},obj));
      await dbInsert('students',Object.assign({id:newId},obj));
      S.students.push(norm);
      mkToast('\u0423\u0447\u043D\u044F \u0434\u043E\u0434\u0430\u043D\u043E');
    }
    closeM('mo-student'); S.editId=null;
    window._saving=false; refreshPage('students');
  }catch(e){ window._saving=false; mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(e.message||e),'error'); }
}

async function delStudent(id){
  if(!can('students')){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432','error'); return; }
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0443\u0447\u043D\u044F?')) return;
  try{ await dbDelete('students',id); mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); }catch(e){}
}

// ── ОБ'ЄДНАННЯ ДУБЛІКАТІВ УЧНІВ ──────────────────────
// Знаходить учнів з однаковим ім'ям+прізвищем, зливає їх в один запис:
// - репетитори обох записів об'єднуються (tutor_ids)
// - порожні поля (телефон, клас, предмет тощо) заповнюються з дублікатів
// - усі заняття, платежі, комунікації та лог рахунків переносяться на основний запис
// - дублікати видаляються з бази
// Нормалізація ПІБ: ігнорує регістр, зайві пробіли, порядок "ім'я/прізвище"
// та латинські літери-двійники (i,o,a,e,c,p,x,y,k,m,t), випадково введені замість кириличних.
function _normNameKey(s){
  var raw=((s.fn||'')+' '+(s.ln||'')).toLowerCase();
  var lookalike={'a':'\u0430','e':'\u0435','i':'\u0456','o':'\u043E','p':'\u0440','c':'\u0441','x':'\u0445','y':'\u0443','k':'\u043A','m':'\u043C','t':'\u0442','b':'\u0432','h':'\u043D','n':'\u043F'};
  raw=raw.replace(/[a-z]/g,function(ch){return lookalike[ch]||ch;});
  raw=raw.replace(/['\u2019\u02BC`\u0301-]/g,'').replace(/\s+/g,' ').trim();
  // сортуємо слова — щоб "Вікторія Мілентьєва" і "Мілентьєва Вікторія" збігались
  return raw.split(' ').filter(Boolean).sort().join(' ');
}

async function mergeDuplicateStudents(){
  if(R()!=='god'&&R()!=='director'){ mkToast('\u0414\u043E\u0441\u0442\u0443\u043F\u043D\u043E \u043B\u0438\u0448\u0435 \u0434\u0438\u0440\u0435\u043A\u0442\u043E\u0440\u0443','error'); return; }
  if(window._viewerMode){ mkToast('\u0420\u0435\u0436\u0438\u043C \u043F\u0435\u0440\u0435\u0433\u043B\u044F\u0434\u0443 \u2014 \u0437\u043C\u0456\u043D\u0438 \u043D\u0435\u0434\u043E\u0441\u0442\u0443\u043F\u043D\u0456','error'); return; }

  var groups={};
  (S.students||[]).forEach(function(s){
    var key=_normNameKey(s);
    if(!key) return;
    (groups[key]=groups[key]||[]).push(s);
  });
  var dupKeys=Object.keys(groups).filter(function(k){return groups[k].length>1;});
  console.log('[merge] \u0433\u0440\u0443\u043F \u0432\u0441\u044C\u043E\u0433\u043E:',Object.keys(groups).length,'\u0434\u0443\u0431\u043B\u0456\u043A\u0430\u0442\u0456\u0432:',dupKeys.length,dupKeys.map(function(k){return groups[k].map(function(s){return s.fn+' '+s.ln+' ['+s.id+']';});}));
  if(!dupKeys.length){ mkToast('\u0414\u0443\u0431\u043B\u0456\u043A\u0430\u0442\u0456\u0432 \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E. \u042F\u043A\u0449\u043E \u0432\u043E\u043D\u0438 \u0454 \u2014 \u043F\u0435\u0440\u0435\u0432\u0456\u0440\u0442\u0435 \u043D\u0430\u043F\u0438\u0441\u0430\u043D\u043D\u044F \u041F\u0406\u0411 (F12 > Console \u2014 \u0434\u0435\u0442\u0430\u043B\u0456)'); return; }

  var summary=dupKeys.map(function(k){var g=groups[k];return '\u2022 '+g[0].fn+' '+g[0].ln+' \u2014 '+g.length+' \u0437\u0430\u043F\u0438\u0441\u0438(\u0456\u0432)';}).join('\n');
  if(!confirm('\u0417\u043D\u0430\u0439\u0434\u0435\u043D\u043E \u0434\u0443\u0431\u043B\u0456\u043A\u0430\u0442\u0438:\n'+summary+'\n\n\u041E\u0431\u02BC\u0454\u0434\u043D\u0430\u0442\u0438? \u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438 \u0431\u0443\u0434\u0443\u0442\u044C \u043E\u0431\u02BC\u0454\u0434\u043D\u0430\u043D\u0456, \u0432\u0441\u0456 \u0437\u0430\u043D\u044F\u0442\u0442\u044F, \u043F\u043B\u0430\u0442\u0435\u0436\u0456 \u0442\u0430 \u043A\u043E\u043C\u0443\u043D\u0456\u043A\u0430\u0446\u0456\u0457 \u043F\u0435\u0440\u0435\u043D\u0435\u0441\u0435\u043D\u043E \u043D\u0430 \u043E\u0434\u0438\u043D \u0437\u0430\u043F\u0438\u0441. \u0426\u0435 \u043D\u0435\u0437\u0432\u043E\u0440\u043E\u0442\u043D\u043E.')) return;

  setSaving();
  var merged=0, errors=[];
  try{
    for(var gi=0; gi<dupKeys.length; gi++){
      var list=groups[dupKeys[gi]].slice().sort(function(a,b){
        return String(a.created_at||a.createdAt||'').localeCompare(String(b.created_at||b.createdAt||''));
      });
      var primary=list[0], dups=list.slice(1);

      // Об'єднуємо репетиторів з усіх записів (без повторів, зі збереженням порядку)
      var tids=[];
      list.forEach(function(s){
        var st=(Array.isArray(s.tutorIds)&&s.tutorIds.length)?s.tutorIds:(s.tutorId?[s.tutorId]:[]);
        st.forEach(function(t){ if(t&&tids.indexOf(t)<0) tids.push(t); });
      });

      // Порожні поля основного запису заповнюємо даними з дублікатів
      var patch={ tutor_ids:tids.join(','), tutor_id:tids[0]||null };
      var fields=[['phone','phone'],['email','email'],['grade','grade'],['age','age'],['subject','subject'],['notes','notes'],['parent_fn','parentFn'],['parent_phone','parentPhone'],['status','status']];
      fields.forEach(function(f){
        var col=f[0], camel=f[1];
        if(!primary[camel]&&!primary[col]){
          var d=dups.find(function(x){return x[camel]||x[col];});
          if(d) patch[col]=d[camel]||d[col];
        }
      });

      var ru=await _sb.from('students').update(patch).eq('id',primary.id);
      if(ru.error && await refreshIfExpired(ru.error)) ru=await _sb.from('students').update(patch).eq('id',primary.id);
      if(ru.error){ errors.push(primary.fn+' '+primary.ln+': '+ru.error.message); continue; }

      // Переносимо пов'язані записи та видаляємо дублікати
      var ok=true;
      for(var di=0; di<dups.length; di++){
        var dup=dups[di];
        var tables=['lessons','payments','comms','invoice_log'];
        for(var ti=0; ti<tables.length; ti++){
          var up=await _sb.from(tables[ti]).update({student_id:primary.id}).eq('student_id',dup.id);
          if(up.error && await refreshIfExpired(up.error)) up=await _sb.from(tables[ti]).update({student_id:primary.id}).eq('student_id',dup.id);
          // invoice_log може бути відсутнім у частині інсталяцій — пропускаємо помилку "таблиці немає"
          if(up.error && tables[ti]!=='invoice_log'){ errors.push(dup.fn+' '+dup.ln+' ('+tables[ti]+'): '+up.error.message); ok=false; break; }
        }
        if(!ok) break;
        var del=await _sb.from('students').delete().eq('id',dup.id);
        if(del.error && await refreshIfExpired(del.error)) del=await _sb.from('students').delete().eq('id',dup.id);
        if(del.error){ errors.push(dup.fn+' '+dup.ln+': '+del.error.message); ok=false; break; }
      }
      if(ok) merged++;
    }
  }catch(e){ errors.push(e.message||String(e)); }

  // Перезавантажуємо зачеплені таблиці та оновлюємо інтерфейс
  await loadTableFresh('students');
  await loadTableFresh('lessons');
  await loadTableFresh('payments');
  await loadTableFresh('comms');

  if(errors.length){ console.error('[merge] \u043F\u043E\u043C\u0438\u043B\u043A\u0438:',errors); mkToast('\u041E\u0431\u02BC\u0454\u0434\u043D\u0430\u043D\u043E: '+merged+', \u043F\u043E\u043C\u0438\u043B\u043A\u0438: '+errors[0],'error'); }
  else mkToast('\u041E\u0431\u02BC\u0454\u0434\u043D\u0430\u043D\u043E \u0433\u0440\u0443\u043F: '+merged);
  setSynced();
}

// \u0411\u0435\u0437\u043f\u0435\u0447\u043d\u0435 \u0432\u0438\u0434\u0430\u043b\u0435\u043d\u043d\u044f \u043b\u0456\u0434\u0430 \u0437 CRM-\u0434\u043e\u0448\u043a\u0438:
// - \u044f\u043a\u0449\u043e \u0446\u0435 \u0449\u0435 \u043d\u0435 \u0430\u043a\u0442\u0438\u0432\u043d\u0438\u0439 \u0443\u0447\u0435\u043d\u044c (\u043d\u0435 status==='active') \u2014 \u0432\u0438\u0434\u0430\u043b\u044f\u0454\u043c\u043e\u0441\u044f \u0444\u0456\u0437\u0438\u0447\u043d\u043e
// - \u044f\u043a\u0449\u043e \u0432\u0436\u0435 \u0430\u043a\u0442\u0438\u0432\u043d\u0438\u0439 \u0443\u0447\u0435\u043d\u044c \u2014 \u043b\u0438\u0448\u0435 \u043f\u0440\u0438\u0431\u0438\u0440\u0430\u0454\u043c\u043e \u0437 CRM-\u0434\u043e\u0448\u043a\u0438 (\u0441\u0442\u0430\u0432\u0438\u043c\u043e \u0435\u0442\u0430\u043f lost), \u0441\u0430\u043c \u0443\u0447\u0435\u043d\u044c \u0437\u0430\u043b\u0438\u0448\u0430\u0454\u0442\u044c\u0441\u044f
async function delLead(id){
  if(!can('students')){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432','error'); return; }
  var s=(S.students||[]).find(function(x){return x.id===id;});
  if(!s){mkToast('\u041b\u0456\u0434 \u043d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e','error');return;}

  // \u0412\u0438\u0434\u0430\u043b\u0435\u043d\u043d\u044f \u0437 CRM \u043d\u0456\u043a\u043e\u043b\u0438 \u043d\u0435 \u0432\u0438\u0434\u0430\u043b\u044f\u0454 \u0441\u0430\u043c\u043e\u0433\u043e \u0443\u0447\u043d\u044f \u0437 \u0441\u0438\u0441\u0442\u0435\u043c\u0438.
  // \u041c\u0438 \u043f\u0440\u043e\u0441\u0442\u043e \u043f\u043e\u0437\u043d\u0430\u0447\u0430\u0454\u043c\u043e crm_stage='removed' \u2014 \u0442\u0430\u043a\u0438\u0439 \u0437\u0430\u043f\u0438\u0441 \u0431\u0456\u043b\u044c\u0448\u0435 \u043d\u0435 \u0437'\u044f\u0432\u043b\u044f\u0454\u0442\u044c\u0441\u044f \u043d\u0456 \u0432 \u043e\u0434\u043d\u0456\u0439 \u043a\u043e\u043b\u043e\u043d\u0446\u0456 \u0434\u043e\u0448\u043a\u0438,
  // \u0430\u043b\u0435 \u0441\u0430\u043c \u0443\u0447\u0435\u043d\u044c (\u0437 \u0443\u0441\u0456\u043c\u0430 \u0437\u0430\u043d\u044f\u0442\u0442\u044f\u043c\u0438/\u043f\u043b\u0430\u0442\u0435\u0436\u0430\u043c\u0438) \u043b\u0438\u0448\u0430\u0454\u0442\u044c\u0441\u044f \u0432 \u0441\u0438\u0441\u0442\u0435\u043c\u0456 \u0431\u0435\u0437 \u0437\u043c\u0456\u043d.
  if(!confirm('\u0412\u0438\u0434\u0430\u043b\u0438\u0442\u0438 \u00ab'+s.fn+' '+s.ln+'\u00bb \u0437 CRM-\u0434\u043e\u0448\u043a\u0438? \u0421\u0430\u043c \u0443\u0447\u0435\u043d\u044c \u0437\u0430\u043b\u0438\u0448\u0438\u0442\u044c\u0441\u044f \u0432 \u0441\u0438\u0441\u0442\u0435\u043c\u0456, \u0437\u043c\u0456\u043d\u044e\u0454\u0442\u044c\u0441\u044f \u043b\u0438\u0448\u0435 \u0439\u043e\u0433\u043e \u043f\u043e\u043b\u043e\u0436\u0435\u043d\u043d\u044f \u0432 CRM.')) return;
  try{
    setSaving();
    var _u = await _sb.from('students').update({crm_stage:'removed', updated_at:new Date().toISOString()}).eq('id',id);
    if(_u.error){ console.error('delLead update error:', _u.error); mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+_u.error.message,'error'); return; }
    var idx=(S.students||[]).findIndex(function(x){return x.id===id;});
    if(idx>=0){ S.students[idx].crmStage='removed'; S.students[idx].crm_stage='removed'; }
    if(S.currentPage==='crm') renderCrm();
    mkToast('\u0412\u0438\u0434\u0430\u043b\u0435\u043d\u043e \u0437 CRM (\u0443\u0447\u0435\u043d\u044c \u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043d\u043e)');
  }catch(e){ console.error('delLead exception:', e); mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(e.message||e),'error'); }
  finally{ setSynced(); }
}
window.delLead = delLead;

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
    acc_uid: document.getElementById('t-acc')?.value||null,
    branch_id: myBranchId()||null,
  };
  window._saving = true;
  try{
    if(S.editId){ await dbUpdate('tutors',S.editId,obj); mkToast('\u041E\u043D\u043E\u0432\u043B\u0435\u043D\u043E'); }
    else         { await dbInsert('tutors',Object.assign({id:uid()},obj)); mkToast('\u0412\u0438\u043A\u043B\u0430\u0434\u0430\u0447\u0430 \u0434\u043E\u0434\u0430\u043D\u043E'); }
    closeM('mo-tutor'); S.editId=null; window._saving=false; refreshPage('tutors');
  }catch(e){ window._saving=false; mkToast('Помилка: '+(e.message||e),'error'); }
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
  // Попередження про неробочий час (не блокує)
  var _wt=(S.tutors||[]).find(function(t){return t.id===(document.getElementById('l-tutor')?document.getElementById('l-tutor').value:null);});
  var _wtime=document.getElementById('l-time')?document.getElementById('l-time').value:'';
  if(_wt && _wtime && typeof tutorAvail==='function' && tutorAvail(_wt)){
    var _wh=parseInt(_wtime.split(':')[0]);
    if(!isNaN(_wh) && !isWorkingHour(_wt, dowMon1FromDate(date), _wh)){
      if(!confirm('\u26A0 '+_wt.fn+' '+_wt.ln+' \u043F\u043E\u0437\u043D\u0430\u0447\u0438\u0432(\u043B\u0430) \u0446\u0435\u0439 \u0447\u0430\u0441 \u044F\u043A \u043D\u0435\u0440\u043E\u0431\u043E\u0447\u0438\u0439. \u0412\u0441\u0435 \u043E\u0434\u043D\u043E \u043F\u043E\u0441\u0442\u0430\u0432\u0438\u0442\u0438 \u0443\u0440\u043E\u043A?')) return;
    }
  }
  var recurType = document.getElementById('l-recur')?.value||'none';
  var _stat = document.getElementById('l-stat')?.value||'planned';
  var obj={
    student_id: studentId,
    tutor_id:   document.getElementById('l-tutor')?.value||null,
    subject:    document.getElementById('l-subj')?.value||'',
    date:       date,
    time:       document.getElementById('l-time')?.value||'',
    dur:        parseInt(document.getElementById('l-dur')?.value)||60,
    price:      (function(){
      // Ставка з картки учня за правилами (предмет/репетитор) на момент збереження — знімок
      var _sid=document.getElementById('l-std')?.value;
      var _st=_sid?(S.students||[]).find(function(s){return s.id===_sid;}):null;
      return studentRate(_st, document.getElementById('l-subj')?.value||'', document.getElementById('l-tutor')?.value||'');
    })(),
    status:     _stat,
    notes:      document.getElementById('l-notes')?.value||'',
    branch_id:  myBranchId()||null,
    missed_date: (_stat==='missed'||_stat==='makeup'||_stat==='makeup_planned') ? (document.getElementById('l-miss-date')?.value||null) : null,
    makeup_date: (_stat==='makeup'||_stat==='makeup_planned') ? (document.getElementById('l-makeup-date')?.value||null) : null,
    hw:          document.getElementById('l-hw')?.value||null,
    games:       document.getElementById('l-games')?.value||null,
  };
  window._saving = true;
  try{
    if(S.editId){ await dbUpdate('lessons',S.editId,obj); mkToast('\u041E\u043D\u043E\u0432\u043B\u0435\u043D\u043E'); closeM('mo-lesson'); window._saving=false; refreshPage('lessons'); if(S.currentPage==='schedule') renderSch(); }
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
  window._saving = true;
  try{
    if(S.editId){ await dbUpdate('payments',S.editId,obj); mkToast('\u041E\u043D\u043E\u0432\u043B\u0435\u043D\u043E'); }
    else         { await dbInsert('payments',Object.assign({id:uid()},obj)); mkToast('\u0417\u0430\u043F\u0438\u0441\u0430\u043D\u043E'); }
    closeM('mo-payment'); S.editId=null; window._saving=false; refreshPage('payments');
  }catch(e){ window._saving=false; mkToast('Помилка: '+(e.message||e),'error'); }
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
  window._saving = true;
  var obj={ tutor_id:tutorId,
    student_id:document.getElementById('cm-student')?.value||null,
    date, type:document.getElementById('cm-type')?.value||'call',
    note:document.getElementById('cm-note')?.value||'' };
  try{
    if(window._editCommId){
      var _cid=window._editCommId;
      await dbUpdate('comms',_cid,obj);
      // Миттєве оновлення локального стану
      var _lc=(S.comms||[]).find(function(c){return c.id===_cid;});
      if(_lc) Object.assign(_lc,obj,{tutorId:obj.tutor_id,studentId:obj.student_id});
      window._editCommId=null;
      closeM('mo-comm'); mkToast('\u0417\u0431\u0435\u0440\u0435\u0436\u0435\u043D\u043E'); window._saving=false;
    } else {
      await dbInsert('comms',Object.assign({id:uid(),branch_id:myBranchId()||null},obj));
      closeM('mo-comm'); mkToast('Записано'); window._saving=false;
    }
    // Add to local S.comms immediately
    if(S.currentPage==='comms') renderCommsPage();
    if(typeof renderCommLog==='function') renderCommLog();
  }catch(e){ window._saving=false; mkToast('Помилка: '+(e.message||e),'error'); }
}

async function delComm(id){
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u043A\u043E\u043C\u0443\u043D\u0456\u043A\u0430\u0446\u0456\u044E?')) return;
  try{
    await dbDelete('comms',id);
    S.comms=(S.comms||[]).filter(function(c){return c.id!==id;});
    if(S.currentPage==='comms') renderCommsPage();
    mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E');
  }catch(e){}
}

async function saveSettings(){
  try{
    await _sb.from('settings').upsert({
      id:'main',
      name:    document.getElementById('set-name')?.value||'',
      phone:   document.getElementById('set-phone')?.value||'',
      email:   document.getElementById('set-email')?.value||'',
      address: document.getElementById('set-addr')?.value||'',
      payment_details: document.getElementById('set-payment')?.value||'',
      unisender_key: document.getElementById('set-unisender-key')?.value||'',
      viber_sender:  document.getElementById('set-viber-sender')?.value||'',
      updated_at: new Date().toISOString()
    });
    mkToast('\u0417\u0431\u0435\u0440\u0435\u0436\u0435\u043D\u043E');
  }catch(e){ mkToast('\u041F\u043E\u043C\u0438\u043B\u043A\u0430','error'); }
}

async function addSubj(){
  var name=(document.getElementById('ns-name')?.value||'').trim();
  if(!name){ mkToast('\u0412\u0432\u0435\u0434\u0456\u0442\u044C \u043D\u0430\u0437\u0432\u0443','error'); return; }
  try{
    await dbInsert('subjects',{id:uid(),name:name,branch_id:myBranchId()||null});
    document.getElementById('ns-name').value='';
    mkToast('\u0414\u043E\u0434\u0430\u043D\u043E');
  }catch(e){}
}

async function delSubj(id){
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u043F\u0440\u0435\u0434\u043C\u0435\u0442?')) return;
  try{
    await dbDelete('subjects',id);
    S.subjects=(S.subjects||[]).filter(function(s){return s.id!==id;});
    if(typeof renderSettings==='function'&&S.currentPage==='settings') renderSettings();
    mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E');
  }catch(e){}
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
  openBranchM(id);
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
  S.editId=id||null;
  var u=id?(S.users||[]).find(function(x){return x.id===id;}):null;
  if(id&&!u) return;

  var titleEl=document.getElementById('mu-title');
  if(titleEl) titleEl.textContent=id?'Редагувати акаунт':'Новий акаунт';

  var fnEl=document.getElementById('u-fn');     if(fnEl)    fnEl.value=u?u.fn||'':'';
  var lnEl=document.getElementById('u-ln');     if(lnEl)    lnEl.value=u?u.ln||'':'';
  var emEl=document.getElementById('u-email');  if(emEl)  { emEl.value=u?u.email||'':''; emEl.disabled=!!id; emEl.style.opacity=id?'0.6':'1'; }
  var roEl=document.getElementById('u-role');   if(roEl)    roEl.value=u?u.role||'tutor':'tutor';
  toggleTutLink();
  popSel('u-tlink',S.tutors,'id',function(t){return t.fn+' '+t.ln;},'\u041f\u0440\u0438\u0432\u2019\u044f\u0437\u0430\u0442\u0438 \u0434\u043e \u0432\u0438\u043a\u043b\u0430\u0434\u0430\u0447\u0430');
  if(id){
    var linked=(S.tutors||[]).find(function(t){return t.acc_uid===id||t.accId===id;});
    if(linked){ var tlEl=document.getElementById('u-tlink'); if(tlEl) tlEl.value=linked.id; }
  }
  openM('mo-user');
}

async function saveUser(){
  var fn=document.getElementById('u-fn').value.trim();
  var ln=document.getElementById('u-ln').value.trim();
  var role=document.getElementById('u-role').value;
  var email=document.getElementById('u-email').value.trim();

  if(!fn){ mkToast('\u0412\u0432\u0435\u0434\u0456\u0442\u044c \u0456\u043c\u2019\u044f','error'); return; }

  try{
    if(!S.editId){
      // Новий акаунт — запрошуємо через Supabase Admin API
      if(!email){ mkToast('\u0412\u0432\u0435\u0434\u0456\u0442\u044c email','error'); return; }
      var _inv = await _sb.auth.admin.inviteUserByEmail(email, {
        data:{ fn, ln, role }
      });
      if(_inv.error) throw _inv.error;
      // Одразу оновимо профіль щойно створеного користувача
      if(_inv.data?.user?.id){
        await _sb.from('profiles').update({fn,ln,role}).eq('id',_inv.data.user.id);
      }
      mkToast('\u0417\u0430\u043f\u0440\u043e\u0448\u0435\u043d\u043d\u044f \u043d\u0430\u0434\u0456\u0441\u043b\u0430\u043d\u043e \u043d\u0430 '+email);
    } else {
      // Редагування існуючого
      await dbUpdate('profiles',S.editId,{fn,ln,role});
      var tutorId=document.getElementById('u-tlink')?.value;
      if(role==='tutor'&&tutorId){
        await _sb.from('tutors').update({acc_uid:S.editId}).eq('id',tutorId);
      }
      if(CU?.id===S.editId){ CU=Object.assign({},CU,{fn,ln,role}); updateSBUser(); buildSidebar(); }
      mkToast('\u041e\u043d\u043e\u0432\u043b\u0435\u043d\u043e');
    }
    closeM('mo-user'); S.editId=null; renderUsers();
  }catch(e){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(e.message||e),'error'); console.error('saveUser error:',e); }
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
  try{ buildUAHeader(u); }catch(e){ console.error('buildUAHeader error:',e); }
  try{ buildUAPerms(u); }catch(e){ console.error('buildUAPerms error:',e); }
  try{ buildUANav(u); }catch(e){ console.error('buildUANav error:',e); }
  try{ buildUASummary(u); }catch(e){ console.error('buildUASummary error:',e); }
  openM('mo-user-access');
}

async function uaPermChange(key,val,roleDefault){
  var u=(S.users||[]).find(function(x){return x.id===_uaUserId;});
  if(!u) return;
  var perms=JSON.parse(JSON.stringify(u.perms||{}));
  if(!perms.can) perms.can={};
  if(val===roleDefault) delete perms.can[key]; else perms.can[key]=val;
  if(!Object.keys(perms.can).length) delete perms.can;
  try{ await dbUpdate('profiles',_uaUserId,{perms}); if(CU?.id===_uaUserId)CU.perms=perms; }catch(e){ mkToast('Помилка: '+(e.message||e),'error'); console.error('ua error:',e); return; }
  var u2=(S.users||[]).find(function(x){return x.id===_uaUserId;}); if(u2){u2.perms=perms;buildUASummary(u2);}
}

async function uaResetPerm(key){
  var u=(S.users||[]).find(function(x){return x.id===_uaUserId;});
  if(!u) return;
  var perms=JSON.parse(JSON.stringify(u.perms||{}));
  if(perms.can){delete perms.can[key];if(!Object.keys(perms.can).length)delete perms.can;}
  try{ await dbUpdate('profiles',_uaUserId,{perms}); if(CU?.id===_uaUserId)CU.perms=perms; }catch(e){ mkToast('Помилка: '+(e.message||e),'error'); console.error('ua error:',e); return; }
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
  try{ await dbUpdate('profiles',_uaUserId,{perms}); if(CU?.id===_uaUserId){CU.perms=perms;buildSidebar();} }catch(e){ mkToast('Помилка: '+(e.message||e),'error'); console.error('ua error:',e); return; }
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
// ── QUICK ACTION POPUP ──────────────────────
var _quickLessonId = null;

function showQuickPopup(lessonId, x, y){
  _quickLessonId = lessonId;
  var l = (S.lessons||[]).find(function(x){return x.id===lessonId;});
  if(!l) return;
  var s = (S.students||[]).find(function(st){return st.id===(l.studentId||l.student_id);});
  var title = document.getElementById('qp-title');
  if(title) title.textContent = (s?s.fn+' '+s.ln:'?')+' · '+(l.time||'');
  var pop = document.getElementById('quick-popup');
  if(!pop) return;
  pop.style.display = 'block';
  // Position near click
  var pw = 190, ph = 200;
  var lx = Math.min(x, window.innerWidth - pw - 10);
  var ly = Math.min(y, window.innerHeight - ph - 10);
  pop.style.left = lx + 'px';
  pop.style.top = ly + 'px';
  // Close on outside click
  setTimeout(function(){
    document.addEventListener('click', closeQuickPopup, {once:true});
  }, 10);
}

function closeQuickPopup(){
  var pop = document.getElementById('quick-popup');
  if(pop) pop.style.display = 'none';
}

async function quickSetStatus(status){
  closeQuickPopup();
  if(!_quickLessonId) return;
  try{
    await dbUpdate('lessons', _quickLessonId, {status: status});
    mkToast(status==='done'?'✅ Проведено':status==='missed'?'❌ Пропущено':status==='cancelled'?'🚫 Скасовано':'📅 Заплановано');
    if(S.currentPage==='schedule') renderSch();
    if(S.currentPage==='lessons') renderLessons();
  }catch(e){ mkToast('Помилка: '+e.message,'error'); }
}

function quickEdit(){
  closeQuickPopup();
  if(_quickLessonId) openLessM(_quickLessonId);
}

window.showQuickPopup = showQuickPopup;
window.closeQuickPopup = closeQuickPopup;
window.quickSetStatus = quickSetStatus;
window.quickEdit = quickEdit;

// ── CTRL+K GLOBAL SEARCH ────────────────────
document.addEventListener('keydown', function(e){
  if((e.ctrlKey||e.metaKey) && e.key==='k'){
    e.preventDefault();
    var gs = document.getElementById('gsearch');
    if(gs){ gs.focus(); gs.select(); }
  }
  if(e.key==='Escape'){
    closeQuickPopup();
  }
});

// ── EXCEL EXPORT ────────────────────────────
function exportToExcel(type){
  if(R()!=='god'&&R()!=='director'){ mkToast('\u0415\u043A\u0441\u043F\u043E\u0440\u0442 \u0434\u043E\u0441\u0442\u0443\u043F\u043D\u0438\u0439 \u043B\u0438\u0448\u0435 \u0434\u0438\u0440\u0435\u043A\u0442\u043E\u0440\u0443','error'); return; }
  var rows=[], headers=[], data=[];

  if(type==='students'){
    headers=['Імʼя','Прізвище','Телефон','Предмет','Статус','Клас','Нотатки'];
    data=(S.students||[]).map(function(s){return [s.fn||'',s.ln||'',s.phone||'',s.subject||'',s.status||'',s.grade||'',s.notes||''];});
  } else if(type==='lessons'){
    headers=['Учень','Репетитор','Предмет','Дата','Час','Тривалість','Статус','Ціна'];
    data=myLessons().map(function(l){
      var st=(S.students||[]).find(function(s){return s.id===(l.studentId||l.student_id);});
      var tu=(S.tutors||[]).find(function(t){return t.id===(l.tutorId||l.tutor_id);});
      return [(st?st.fn+' '+st.ln:''),(tu?tu.fn+' '+tu.ln:''),(l.subject||''),(l.date||''),(l.time||''),(l.dur||60)+' хв',(l.status||''),(l.price||'')];
    });
  } else if(type==='payments'){
    headers=['Учень','Сума','Дата','Статус','Нотатки'];
    data=(S.payments||[]).map(function(p){
      var st=(S.students||[]).find(function(s){return s.id===(p.studentId||p.student_id);});
      return [(st?st.fn+' '+st.ln:''),(p.amount||''),(p.date||''),(p.status||''),(p.notes||'')];
    });
  }

  // Build CSV (Excel-compatible UTF-8 BOM)
  var csv='\uFEFF'+headers.join(';')+'\n';
  data.forEach(function(row){
    csv+=row.map(function(cell){
      var s=String(cell||'').replace(/"/g,'""');
      return '"'+s+'"';
    }).join(';')+'\n';
  });

  var blob=new Blob([csv],{type:'text/csv;charset=utf-8'});
  var url=URL.createObjectURL(blob);
  var a=document.createElement('a');
  a.href=url; a.download='konstanta_'+type+'_'+new Date().toISOString().slice(0,10)+'.csv';
  a.click(); URL.revokeObjectURL(url);
  mkToast('📊 Експорт завершено');
}
window.exportToExcel = exportToExcel;

function toggleTheme(){
  var isDark=document.documentElement.getAttribute('data-theme')==='dark';
  var newTheme=isDark?'light':'dark';
  document.documentElement.setAttribute('data-theme',newTheme==='dark'?'dark':'');
  localStorage.setItem('crm_theme',newTheme);
  var btn=document.getElementById('theme-btn');
  if(btn) btn.textContent=newTheme==='dark'?'☀️':'🌙';
}
function initTheme(){
  var saved=localStorage.getItem('crm_theme')||'light';
  if(saved==='dark') document.documentElement.setAttribute('data-theme','dark');
  var btn=document.getElementById('theme-btn');
  if(btn) btn.textContent=saved==='dark'?'☀️':'🌙';
}
window.toggleTheme=toggleTheme;

async function startApp(){
  // Проактивне оновлення токена: періодично та при поверненні на вкладку/у застосунок.
  // Це головний захист від "JWT expired" після сну ноутбука чи згорнутого PWA.
  if(!window._sessTimersSet){
    window._sessTimersSet=true;
    setInterval(function(){ try{ ensureFreshSession(); }catch(e){} }, 4*60*1000); // кожні 4 хв
    document.addEventListener('visibilitychange', function(){
      if(document.visibilityState==='visible'){ try{ ensureFreshSession(); }catch(e){} }
    });
    window.addEventListener('focus', function(){ try{ ensureFreshSession(); }catch(e){} });
    window.addEventListener('online', function(){ try{ ensureFreshSession(); }catch(e){} });
  }
  // Inject page visibility CSS
  if(!document.getElementById('__pcss__')){
    var _st=document.createElement('style');
    _st.id='__pcss__';
    _st.textContent='.page{display:none!important}.page.active{display:block!important;padding-bottom:40px}';
    document.head.appendChild(_st);
  }
  document.getElementById('ls').style.display='none';
  document.getElementById('as').style.display='block';

  // Refresh helper — re-renders current page
  function reRender(){
    var pg = S.currentPage;
    if(!pg) return;
    try{
      if(pg==='dashboard')  renderDash();
      else if(pg==='students')  renderStudents();
      else if(pg==='tutors')    renderTutors();
      else if(pg==='schedule')  renderSch();
      else if(pg==='lessons')   renderLessons();
      else if(pg==='payments')  renderPayments();
      else if(pg==='reports')   renderAllAnalytics();
      else if(pg==='users')     renderUsers();
      else if(pg==='settings')  renderSettings();
      else if(pg==='profile')   renderProfile();
      else if(pg==='comms')     renderCommsPage();
      else if(pg==='missed')    renderMissedLessons();
      else if(pg==='crm')       renderCrm();
    }catch(e){ console.warn('reRender:', e); }
  }

  // Load data, build UI, navigate — all in one await
  await loadAll();
  startChannels();
  buildSidebar(); updateSBUser(); updateBranchSelector();
  document.body.className = document.body.className.replace(/\brole-\w+\b/g, '');
  document.body.classList.add('role-' + (CU ? CU.role : 'tutor'));

  var lastPage = '';
  try{ lastPage = localStorage.getItem('sb_page')||''; }catch(e){}
  var allowedPages = userNav();
  // Відновлюємо останню відкриту сторінку (якщо вона досі доступна цій ролі),
  // інакше відкриваємо дашборд за замовчуванням
  var pageToOpen = (lastPage && allowedPages.indexOf(lastPage) >= 0) ? lastPage : 'dashboard';
  try{ nav(pageToOpen); }catch(e){ try{ nav('dashboard'); }catch(e2){} }

  // Second silent load to catch any data that arrived after first load
  loadAll().then(function(){
    buildSidebar(); updateSBUser();
    try{renderDash();}catch(e){}
    reRender();
  }).catch(function(){});

}  // startApp end

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

// ══════════ ЗАВДАННЯ (TASKS) ══════════
// Керівні ролі ставлять завдання з дедлайнами відповідальним адмінам/директорам.
// Прострочене відкрите завдання підсвічує пункт меню відповідального червоним.
function taskRoles(){ return ['god','director','admin','network_admin']; }
// Хто може БУТИ відповідальним (отримувати завдання): керівники + репетитори
function taskAssigneeRoles(){ return ['god','director','admin','network_admin','tutor']; }
function canManageTasks(){ return taskRoles().indexOf(R())>=0; }

function taskIsOverdue(t){
  if(t.status==='done') return false;
  if(!t.deadline) return false;
  var dl=new Date(String(t.deadline).slice(0,10)+'T'+(t.deadlineTime||t.deadline_time||'23:59'));
  return dl.getTime()<Date.now();
}

function taskAssigneeName(t){
  var u=(S.users||[]).find(function(x){return x.id===(t.assigneeId||t.assignee_id);});
  return u?(u.fn+' '+u.ln):'\u2014';
}

function taskCreatorName(t){
  var cid=t.creatorId||t.creator_id;
  if(!cid) return '\u2014';
  var u=(S.users||[]).find(function(x){return x.id===cid;});
  return u?(u.fn+' '+u.ln):'\u2014';
}

function renderTasks(){
  var tbody=document.getElementById('tasks-tbody');
  if(!tbody) return;
  var isMgr=canManageTasks();
  var fSt=(document.getElementById('tf-status')||{value:''}).value;
  var fAs=(document.getElementById('tf-assignee')||{value:''}).value;

  // Керівники ставлять завдання й фільтрують по відповідальному; репетитори — лише свої, без цих контролів
  var asSel=document.getElementById('tf-assignee');
  var addBtn=document.getElementById('task-add-btn');
  if(addBtn) addBtn.style.display=isMgr?'inline-flex':'none';
  if(asSel){
    asSel.style.display=isMgr?'':'none';
    if(isMgr){
      var prev=asSel.value;
      asSel.innerHTML='<option value="">Всі відповідальні</option>'
        +(S.users||[]).filter(function(u){return taskAssigneeRoles().indexOf(u.role)>=0;})
          .sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');})
          .map(function(u){return '<option value="'+u.id+'">'+u.fn+' '+u.ln+'</option>';}).join('');
      asSel.value=prev;
    }
  }

  var list=(S.tasks||[]).slice();
  // Репетитор (і будь-хто без прав керування) бачить ЛИШЕ призначені йому завдання
  if(!isMgr) list=list.filter(function(t){return (t.assigneeId||t.assignee_id)===(CU&&CU.id);});
  if(fAs) list=list.filter(function(t){return (t.assigneeId||t.assignee_id)===fAs;});
  if(fSt==='open')    list=list.filter(function(t){return t.status!=='done'&&!taskIsOverdue(t);});
  if(fSt==='overdue') list=list.filter(function(t){return taskIsOverdue(t);});
  if(fSt==='done')    list=list.filter(function(t){return t.status==='done';});

  // Сортування: прострочені → відкриті за дедлайном → виконані
  list.sort(function(a,b){
    var oa=taskIsOverdue(a)?0:(a.status==='done'?2:1);
    var ob=taskIsOverdue(b)?0:(b.status==='done'?2:1);
    if(oa!==ob) return oa-ob;
    return String(a.deadline||'9999').localeCompare(String(b.deadline||'9999'));
  });

  if(!list.length){
    tbody.innerHTML='<tr><td colspan="7" style="text-align:center;padding:20px;color:var(--t3)">\u0417\u0430\u0432\u0434\u0430\u043D\u044C \u043D\u0435\u043C\u0430\u0454</td></tr>';
    updateTaskAlert();
    return;
  }

  tbody.innerHTML=list.map(function(t){
    var over=taskIsOverdue(t);
    var st=t.status==='done'
      ?'<span class="badge bg">\u2713 \u0412\u0438\u043A\u043E\u043D\u0430\u043D\u043E</span>'
      :(over?'<span class="badge br">\u26A0 \u041F\u0440\u043E\u0441\u0442\u0440\u043E\u0447\u0435\u043D\u043E</span>':'<span class="badge bb">\u25CB \u0412\u0456\u0434\u043A\u0440\u0438\u0442\u0435</span>');
    var dl=t.deadline?(fd(t.deadline)+((t.deadlineTime||t.deadline_time)?' '+(t.deadlineTime||t.deadline_time):'')):'\u2014';
    var mine=(t.assigneeId||t.assignee_id)===(CU&&CU.id);
    var canDel=(R()==='god'||R()==='director');
    var doneBtn=t.status==='done'
      ?'<button onclick="toggleTaskDone(\''+t.id+'\')" title="\u041F\u043E\u0432\u0435\u0440\u043D\u0443\u0442\u0438 \u0432 \u0440\u043E\u0431\u043E\u0442\u0443" style="border:none;background:none;cursor:pointer;font-size:14px;padding:4px 6px">\u21A9</button>'
      :'<button onclick="toggleTaskDone(\''+t.id+'\')" title="\u041F\u043E\u0437\u043D\u0430\u0447\u0438\u0442\u0438 \u0432\u0438\u043A\u043E\u043D\u0430\u043D\u0438\u043C" style="border:none;background:none;cursor:pointer;font-size:14px;padding:4px 6px;color:var(--tut)">\u2705</button>';
    return '<tr'+(over?' style="background:rgba(248,113,113,.07)"':'')+'>'
      +'<td><div style="font-weight:600;font-size:13px'+(t.status==='done'?';text-decoration:line-through;opacity:.6':'')+'">'+(t.title||'\u2014')+'</div>'
        +(t.descr?'<div style="font-size:11px;color:var(--t2);margin-top:2px">'+t.descr+'</div>':'')
        +(t.report?'<div style="font-size:11px;color:var(--tut);margin-top:3px">\uD83D\uDCDD '+t.report+'</div>':'')+'</td>'
      +'<td style="font-size:12px">'+taskCreatorName(t)
        +(t.created_at||t.createdAt?'<div style="font-size:10px;color:var(--t3)">'+fd(String(t.created_at||t.createdAt).slice(0,10))+'</div>':'')+'</td>'
      +'<td style="font-size:12px'+(mine?';font-weight:700':'')+'">'+taskAssigneeName(t)+(mine?' \uD83D\uDC64':'')+'</td>'
      +'<td style="font-size:12px'+(over?';color:var(--danger);font-weight:700':'')+'">'+dl+'</td>'
      +'<td>'+st+'</td>'
      +'<td style="font-size:11px;color:var(--t3)">'+(t.doneAt||t.done_at?fd(String(t.doneAt||t.done_at).slice(0,10)):'\u2014')+'</td>'
      +'<td style="text-align:right;white-space:nowrap">'+doneBtn
        +'<button onclick="openTaskM(\''+t.id+'\')" title="\u0420\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u0442\u0438" style="border:none;background:none;cursor:pointer;font-size:13px;padding:4px 6px">\u270F\uFE0F</button>'
        +(canDel?'<button onclick="delTask(\''+t.id+'\')" title="\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438" style="border:none;background:none;cursor:pointer;font-size:14px;padding:4px 6px;color:var(--danger)">\uD83D\uDDD1</button>':'')
      +'</td></tr>';
  }).join('');
  updateTaskAlert();
}

function openTaskM(id){
  if(!canManageTasks()){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432','error'); return; }
  window._editTaskId=id||null;
  var t=id?(S.tasks||[]).find(function(x){return x.id===id;}):null;
  var ttl=document.querySelector('#mo-task .mdlt');
  if(ttl) ttl.textContent=t?'\u0420\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u0442\u0438 \u0437\u0430\u0432\u0434\u0430\u043D\u043D\u044F':'\u041D\u043E\u0432\u0435 \u0437\u0430\u0432\u0434\u0430\u043D\u043D\u044F';
  var cInfo=document.getElementById('tk-creator-info');
  if(cInfo){
    if(t&&(t.creatorId||t.creator_id)){
      cInfo.style.display='block';
      cInfo.textContent='\uD83D\uDC64 \u041F\u043E\u0441\u0442\u0430\u0432\u0438\u0432: '+taskCreatorName(t)+(t.created_at||t.createdAt?' \u00B7 '+fd(String(t.created_at||t.createdAt).slice(0,10)):'');
    } else cInfo.style.display='none';
  }
  var asSel=document.getElementById('tk-assignee');
  if(asSel){
    var _mgrs=(S.users||[]).filter(function(u){return taskRoles().indexOf(u.role)>=0;})
      .sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');});
    var _tuts=(S.users||[]).filter(function(u){return u.role==='tutor';})
      .sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');});
    var html='<option value="">\u041E\u0431\u0435\u0440\u0456\u0442\u044C \u0432\u0456\u0434\u043F\u043E\u0432\u0456\u0434\u0430\u043B\u044C\u043D\u043E\u0433\u043E</option>';
    if(_mgrs.length){
      html+='<optgroup label="\u041A\u0435\u0440\u0456\u0432\u043D\u0438\u0446\u0442\u0432\u043E / \u0430\u0434\u043C\u0456\u043D\u0456\u0441\u0442\u0440\u0430\u0446\u0456\u044F">'
        +_mgrs.map(function(u){var rl=(ROLES[u.role]||{}).label||u.role;return '<option value="'+u.id+'">'+u.fn+' '+u.ln+' ('+rl+')</option>';}).join('')
        +'</optgroup>';
    }
    if(_tuts.length){
      html+='<optgroup label="\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438">'
        +_tuts.map(function(u){return '<option value="'+u.id+'">'+u.fn+' '+u.ln+'</option>';}).join('')
        +'</optgroup>';
    }
    asSel.innerHTML=html;
    asSel.value=t?(t.assigneeId||t.assignee_id||''):'';
  }
  document.getElementById('tk-title').value=t?(t.title||''):'';
  document.getElementById('tk-descr').value=t?(t.descr||''):'';
  document.getElementById('tk-deadline').value=t?String(t.deadline||'').slice(0,10):localDateStr(new Date());
  document.getElementById('tk-time').value=t?((t.deadlineTime||t.deadline_time)||''):'';
  openM('mo-task');
}

async function saveTask(){
  if(!canManageTasks()){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432','error'); return; }
  var title=(document.getElementById('tk-title')||{value:''}).value.trim();
  var assignee=(document.getElementById('tk-assignee')||{value:''}).value;
  var deadline=(document.getElementById('tk-deadline')||{value:''}).value;
  if(!title){ mkToast('\u0412\u043A\u0430\u0436\u0456\u0442\u044C \u043D\u0430\u0437\u0432\u0443 \u0437\u0430\u0432\u0434\u0430\u043D\u043D\u044F','error'); return; }
  if(!assignee){ mkToast('\u041E\u0431\u0435\u0440\u0456\u0442\u044C \u0432\u0456\u0434\u043F\u043E\u0432\u0456\u0434\u0430\u043B\u044C\u043D\u043E\u0433\u043E','error'); return; }
  if(!deadline){ mkToast('\u0412\u043A\u0430\u0436\u0456\u0442\u044C \u0434\u0435\u0434\u043B\u0430\u0439\u043D','error'); return; }
  var obj={
    title:title,
    descr:(document.getElementById('tk-descr')||{value:''}).value||'',
    assignee_id:assignee,
    deadline:deadline,
    deadline_time:(document.getElementById('tk-time')||{value:''}).value||null,
    branch_id:myBranchId()||null
  };
  window._saving=true;
  try{
    if(window._editTaskId){
      var _tid=window._editTaskId;
      await dbUpdate('tasks',_tid,obj);
      var _lt=(S.tasks||[]).find(function(x){return x.id===_tid;});
      if(_lt) Object.assign(_lt,obj,normalizeTask(obj));
      window._editTaskId=null;
    } else {
      var _new=Object.assign({id:uid(),status:'open',creator_id:CU?CU.id:null,created_at:new Date().toISOString()},obj);
      await dbInsert('tasks',_new);
      if(!(S.tasks||[]).some(function(x){return x.id===_new.id;})) S.tasks=(S.tasks||[]).concat([normalizeTask(_new)]);
    }
    closeM('mo-task'); mkToast('\u0417\u0431\u0435\u0440\u0435\u0436\u0435\u043D\u043E'); window._saving=false;
    renderTasks();
  }catch(e){ window._saving=false; mkToast('\u041F\u043E\u043C\u0438\u043B\u043A\u0430: '+(e.message||e),'error'); }
}

async function toggleTaskDone(id){
  var t=(S.tasks||[]).find(function(x){return x.id===id;});
  if(!t) return;
  if(t.status!=='done'){
    // Позначення виконаним — через модалку з описом виконаної роботи
    window._doneTaskId=id;
    var rEl=document.getElementById('tk-report');
    if(rEl) rEl.value=t.report||'';
    var tEl=document.getElementById('tk-done-title');
    if(tEl) tEl.textContent=t.title||'';
    openM('mo-task-done');
    return;
  }
  // Повернення в роботу — одразу (опис роботи зберігається в історії)
  try{
    await dbUpdate('tasks',id,{status:'open',done_at:null});
    Object.assign(t,{status:'open',done_at:null,doneAt:null});
    renderTasks();
    mkToast('\u041F\u043E\u0432\u0435\u0440\u043D\u0443\u0442\u043E \u0432 \u0440\u043E\u0431\u043E\u0442\u0443');
  }catch(e){}
}

async function confirmTaskDone(){
  var id=window._doneTaskId;
  var t=id?(S.tasks||[]).find(function(x){return x.id===id;}):null;
  if(!t){ closeM('mo-task-done'); return; }
  var patch={status:'done',done_at:new Date().toISOString(),report:(document.getElementById('tk-report')||{value:''}).value||''};
  try{
    await dbUpdate('tasks',id,patch);
    Object.assign(t,patch,{doneAt:patch.done_at});
    window._doneTaskId=null;
    closeM('mo-task-done');
    renderTasks();
    mkToast('\u0417\u0430\u0432\u0434\u0430\u043D\u043D\u044F \u0432\u0438\u043A\u043E\u043D\u0430\u043D\u043E \u2705');
  }catch(e){}
}

async function delTask(id){
  if(R()!=='god'&&R()!=='director'){ mkToast('\u0412\u0438\u0434\u0430\u043B\u044F\u0442\u0438 \u0437\u0430\u0432\u0434\u0430\u043D\u043D\u044F \u043C\u043E\u0436\u0443\u0442\u044C \u043B\u0438\u0448\u0435 \u0434\u0438\u0440\u0435\u043A\u0442\u043E\u0440 \u0442\u0430 \u0431\u043E\u0433','error'); return; }
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0437\u0430\u0432\u0434\u0430\u043D\u043D\u044F?')) return;
  try{
    await dbDelete('tasks',id);
    S.tasks=(S.tasks||[]).filter(function(x){return x.id!==id;});
    renderTasks();
    mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E');
  }catch(e){}
}

// Червона підсвітка пункту меню відповідального, поки є прострочені завдання
// Тристановий індикатор пункту "Завдання":
// нема моїх завдань → без змін; є відкриті → жовто-синя пульсація; є прострочені → червона.
function updateTaskAlert(){
  var nel=document.getElementById('ni-tasks');
  if(!nel) return;
  var myId=CU&&CU.id;
  var mine=(S.tasks||[]).filter(function(t){return (t.assigneeId||t.assignee_id)===myId;});
  var myOpen=mine.filter(function(t){return t.status!=='done'&&!taskIsOverdue(t);}).length;
  var myOver=mine.filter(function(t){return taskIsOverdue(t);}).length;

  nel.classList.remove('ni-task-open','ni-task-alert');
  var badge=nel.querySelector('.task-nbadge');
  var activeCount=myOver+myOpen;
  if(myOver>0){
    nel.classList.add('ni-task-alert');       // червона пульсація (пропущені)
  } else if(myOpen>0){
    nel.classList.add('ni-task-open');         // жовто-синя пульсація (є відкриті)
  }
  if(activeCount>0){
    if(!badge){ badge=document.createElement('span'); badge.className='task-nbadge'; nel.appendChild(badge); }
    badge.textContent=activeCount;
    badge.style.background = myOver>0 ? 'var(--danger)' : 'var(--adm)';
  } else if(badge){ badge.remove(); }
}

// Push + тост про нові призначені мені завдання (порівнюємо з тим, що бачили раніше)
function checkNewTaskNotifications(){
  try{
    var myId=CU&&CU.id; if(!myId) return;
    var seen=window._seenTaskIds||null;
    var mineNow=(S.tasks||[]).filter(function(t){return (t.assigneeId||t.assignee_id)===myId && t.status!=='done';});
    var idsNow=mineNow.map(function(t){return t.id;});
    if(seen===null){ window._seenTaskIds=idsNow; return; } // перший прохід — просто запам'ятати
    var fresh=mineNow.filter(function(t){return seen.indexOf(t.id)<0;});
    window._seenTaskIds=idsNow;
    fresh.forEach(function(t){ notifyNewTask(t); });
  }catch(e){}
}

function notifyNewTask(t){
  var creator=(S.users||[]).find(function(u){return u.id===(t.creatorId||t.creator_id);});
  var body=(t.title||'\u0417\u0430\u0432\u0434\u0430\u043D\u043D\u044F')
    +(t.deadline?'\n\u0414\u0435\u0434\u043B\u0430\u0439\u043D: '+fd(t.deadline)+((t.deadlineTime||t.deadline_time)?' '+(t.deadlineTime||t.deadline_time):''):'')
    +(creator?'\n\u0412\u0456\u0434: '+creator.fn+' '+creator.ln:'');
  // Тост завжди
  mkToast('\ud83d\udccb \u041D\u043E\u0432\u0435 \u0437\u0430\u0432\u0434\u0430\u043D\u043D\u044F: '+(t.title||''));
  // Системний push, якщо дозволено
  try{
    if('Notification' in window){
      if(Notification.permission==='granted'){
        new Notification('\ud83d\udccb \u041D\u043E\u0432\u0435 \u0437\u0430\u0432\u0434\u0430\u043D\u043D\u044F', {body:body, tag:'task-'+t.id});
      } else if(Notification.permission!=='denied'){
        Notification.requestPermission().then(function(p){
          if(p==='granted') new Notification('\ud83d\udccb \u041D\u043E\u0432\u0435 \u0437\u0430\u0432\u0434\u0430\u043D\u043D\u044F', {body:body, tag:'task-'+t.id});
        });
      }
    }
  }catch(e){}
}

setInterval(function(){ try{updateTaskAlert(); checkNewTaskNotifications(); if(S&&S.currentPage==='tasks')renderTasks();}catch(e){} }, 60000);

window.renderTasks=renderTasks;
window.openTaskM=openTaskM;
window.saveTask=saveTask;
window.toggleTaskDone=toggleTaskDone;
window.confirmTaskDone=confirmTaskDone;
window.delTask=delTask;
window.updateTaskAlert=updateTaskAlert;

// ══════════ ЗАРПЛАТИ РЕПЕТИТОРІВ ══════════
// База = вартість проведених уроків (done/completed) + відпрацювань (makeup) за місяць,
// помножена на коефіцієнт 0.4. Ручні пункти: сума ₴ (±) або % від бази.
var PAYROLL_COEF_DEFAULT = 0.4;
// Поточний коефіцієнт: з поля на сторінці → зі збереженого → 0.4 за замовчуванням
function payrollCoef(){
  var el=document.getElementById('pr-coef');
  var v=el&&el.value!==''?parseFloat(el.value):NaN;
  if(isNaN(v)){ try{ v=parseFloat(localStorage.getItem('payroll_coef')); }catch(e){} }
  if(isNaN(v)||v<=0||v>1) v=PAYROLL_COEF_DEFAULT;
  return v;
}
function prSetCoef(){
  var el=document.getElementById('pr-coef');
  if(el){ var v=parseFloat(el.value); if(!isNaN(v)&&v>0&&v<=1){ try{ localStorage.setItem('payroll_coef', String(v)); }catch(e){} } }
  renderPayroll();
}

function canPayroll(){ return R()==='god'||R()==='director'; }

function payrollPeriod(){
  var el=document.getElementById('pr-period');
  if(el&&el.value) return el.value;
  var n=new Date();
  return n.getFullYear()+'-'+String(n.getMonth()+1).padStart(2,'0');
}

// Проведені уроки та відпрацювання репетитора за місяць period='YYYY-MM'
function payrollBase(tutorId, period){
  var done=[], makeup=[];
  (S.lessons||[]).forEach(function(l){
    if((l.tutorId||l.tutor_id)!==tutorId) return;
    if(String(l.date||'').slice(0,7)!==period) return;
    if(l.status==='done'||l.status==='completed') done.push(l);
    else if(l.status==='makeup') makeup.push(l);
  });
  function hrs(a){ return Math.round(a.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10; }
  function sum(a){ return Math.round(a.reduce(function(s,l){return s+lessonTotal(l);},0)*100)/100; }
  var revenue=sum(done)+sum(makeup);
  var k=payrollCoef();
  var doneSalary=Math.round(sum(done)*k*100)/100;
  var makeupSalary=Math.round(sum(makeup)*k*100)/100;
  return {
    doneCount:done.length, doneHours:hrs(done), doneSum:sum(done), doneSalary:doneSalary,
    makeupCount:makeup.length, makeupHours:hrs(makeup), makeupSum:sum(makeup), makeupSalary:makeupSalary,
    revenue:revenue,
    base:Math.round((doneSalary+makeupSalary)*100)/100
  };
}

function payrollItemsFor(tutorId, period){
  return (S.payrollItems||[]).filter(function(i){
    return (i.tutorId||i.tutor_id)===tutorId && i.period===period;
  });
}

function payrollItemAmount(item, base){
  if(item.percent!=null&&item.percent!==''&&!isNaN(parseFloat(item.percent)))
    return Math.round(base*parseFloat(item.percent)/100*100)/100;
  return Math.round((parseFloat(item.amount)||0)*100)/100;
}

function payrollTotal(tutorId, period){
  var b=payrollBase(tutorId, period);
  var extra=payrollItemsFor(tutorId, period).reduce(function(s,i){return s+payrollItemAmount(i,b.base);},0);
  return { base:b, extra:Math.round(extra*100)/100, total:Math.round((b.base+extra)*100)/100 };
}

function renderPayroll(){
  var wrap=document.getElementById('payroll-body');
  if(!wrap) return;
  if(!canPayroll()){ wrap.innerHTML='<div style="padding:20px;color:var(--t3)">\u0414\u043E\u0441\u0442\u0443\u043F \u043B\u0438\u0448\u0435 \u0434\u043B\u044F \u0434\u0438\u0440\u0435\u043A\u0442\u043E\u0440\u0430</div>'; return; }
  var per=payrollPeriod();
  var perEl=document.getElementById('pr-period');
  if(perEl&&!perEl.value) perEl.value=per;
  var coefEl=document.getElementById('pr-coef');
  if(coefEl&&coefEl.value==='') coefEl.value=payrollCoef();
  var fT=(document.getElementById('pr-tutor-filter')||{value:''}).value;

  var tSel=document.getElementById('pr-tutor-filter');
  if(tSel){
    var prev=tSel.value;
    tSel.innerHTML='<option value="">\u0412\u0441\u0456 \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438</option>'
      +(S.tutors||[]).slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');})
        .map(function(t){return '<option value="'+t.id+'">'+t.fn+' '+t.ln+'</option>';}).join('');
    tSel.value=prev;
  }

  var tutors=(S.tutors||[]).slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');});
  if(fT) tutors=tutors.filter(function(t){return t.id===fT;});

  // Кольори-акценти для аватарів (циклічно)
  var AV=['#29abe2','#22b573','#f59e0b','#8b5cf6','#ec4899','#0ea5e9','#14b8a6','#f97316'];
  function initials(t){ return ((t.fn||' ')[0]+(t.ln||' ')[0]).toUpperCase(); }
  function money(n){ return (Math.round(n*100)/100).toLocaleString('uk-UA'); }

  // Спершу рахуємо все, щоб зробити зведення
  var rows=[], grand=0, sumDone=0, sumMakeup=0, sumExtra=0, activeCount=0;
  tutors.forEach(function(t){
    var pt=payrollTotal(t.id, per), b=pt.base;
    var hasData=b.doneCount||b.makeupCount||payrollItemsFor(t.id,per).length;
    if(!hasData && fT==='') return;
    grand+=pt.total; sumDone+=b.doneSalary; sumMakeup+=b.makeupSalary; sumExtra+=pt.extra;
    if(hasData) activeCount++;
    rows.push({t:t, pt:pt, b:b});
  });

  var html='';

  // ── Зведена панель ──
  if(rows.length){
    var maxBar=Math.max(sumDone+sumMakeup, 1);
    html+='<div class="pr-summary">'
      +'<div class="pr-sum-main">'
        +'<div class="pr-sum-label">\u0424\u043E\u043D\u0434 \u043E\u043F\u043B\u0430\u0442\u0438 \u0437\u0430 '+prMonthName(per)+'</div>'
        +'<div class="pr-sum-grand">'+money(grand)+'<span class="pr-cur">\u20B4</span></div>'
        +'<div class="pr-sum-sub">'+activeCount+' '+prPlural(activeCount,'\u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440','\u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438','\u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0456\u0432')+' \u00B7 \u043A\u043E\u0435\u0444. '+payrollCoef()+'</div>'
      +'</div>'
      +'<div class="pr-sum-break">'
        +'<div class="pr-break-item"><span class="pr-dot" style="background:#22b573"></span><span class="pr-break-lbl">\u0423\u0440\u043E\u043A\u0438</span><span class="pr-break-val">'+money(sumDone)+'\u20B4</span></div>'
        +'<div class="pr-break-item"><span class="pr-dot" style="background:#f59e0b"></span><span class="pr-break-lbl">\u0412\u0456\u0434\u043F\u0440\u0430\u0446\u044E\u0432\u0430\u043D\u043D\u044F</span><span class="pr-break-val">'+money(sumMakeup)+'\u20B4</span></div>'
        +'<div class="pr-break-item"><span class="pr-dot" style="background:#8b5cf6"></span><span class="pr-break-lbl">\u0414\u043E\u0434\u0430\u0442\u043A\u043E\u0432\u0435</span><span class="pr-break-val" style="color:'+(sumExtra<0?'var(--danger)':'inherit')+'">'+(sumExtra>=0?'+':'')+money(sumExtra)+'\u20B4</span></div>'
      +'</div>'
    +'</div>';
  }

  // ── Картки репетиторів ──
  html+='<div class="pr-grid">';
  rows.forEach(function(r, idx){
    var t=r.t, pt=r.pt, b=r.b;
    var av=AV[idx % AV.length];
    var items=payrollItemsFor(t.id, per);
    var itemsHtml=items.map(function(i){
      var amt=payrollItemAmount(i,b.base);
      var neg=amt<0;
      return '<div class="pr-item">'
        +'<span class="pr-item-lbl">'+(i.label||'\u2014')+(i.percent!=null&&i.percent!==''?' <span style="color:var(--t3)">('+i.percent+'%)</span>':'')+'</span>'
        +'<span class="pr-item-amt '+(neg?'neg':'pos')+'">'+(amt>=0?'+':'')+money(amt)+'\u20B4'
        +'<button onclick="delPayrollItem(\''+i.id+'\')" title="\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438" class="pr-item-del">\u2715</button></span>'
      +'</div>';
    }).join('');

    // Прогрес-бар: частка уроків vs відпрацювань у базі
    var baseSum=b.doneSalary+b.makeupSalary;
    var donePct=baseSum>0?Math.round(b.doneSalary/baseSum*100):0;

    html+='<div class="pr-card" style="--av:'+av+'">'
      +'<div class="pr-card-head">'
        +'<div class="pr-avatar" style="background:'+av+'">'+initials(t)+'</div>'
        +'<div class="pr-name-wrap"><div class="pr-name">'+t.fn+' '+t.ln+'</div>'
          +'<div class="pr-meta">'+b.doneCount+' \u0443\u0440\u043E\u043A\u0456\u0432 \u00B7 '+b.makeupCount+' \u0432\u0456\u0434\u043F\u0440.</div></div>'
        +'<div class="pr-total-badge">'+money(pt.total)+'\u20B4</div>'
      +'</div>'
      +'<div class="pr-body">'
        +'<div class="pr-line"><span class="pr-line-ico" style="color:#22b573">\u25CF</span><span class="pr-line-lbl">\u041F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E \u0443\u0440\u043E\u043A\u0456\u0432 <span class="pr-hrs">'+b.doneCount+' / '+b.doneHours+'\u0433 \u00D7'+payrollCoef()+'</span></span><span class="pr-line-val">'+money(b.doneSalary)+'\u20B4</span></div>'
        +'<div class="pr-line"><span class="pr-line-ico" style="color:#f59e0b">\u25CF</span><span class="pr-line-lbl">\u0412\u0456\u0434\u043F\u0440\u0430\u0446\u044C\u043E\u0432\u0430\u043D\u043E <span class="pr-hrs">'+b.makeupCount+' / '+b.makeupHours+'\u0433 \u00D7'+payrollCoef()+'</span></span><span class="pr-line-val">'+money(b.makeupSalary)+'\u20B4</span></div>'
        +(baseSum>0?'<div class="pr-bar"><div class="pr-bar-done" style="width:'+donePct+'%"></div><div class="pr-bar-make" style="width:'+(100-donePct)+'%"></div></div>':'')
        +(itemsHtml?'<div class="pr-items">'+itemsHtml+'</div>':'')
      +'</div>'
      +'<div class="pr-card-foot">'
        +'<button class="btn btn-g btn-sm" onclick="openPayrollItemM(\''+t.id+'\')">+ \u041F\u0443\u043D\u043A\u0442</button>'
        +'<button class="btn btn-g btn-sm" onclick="printPayroll(\''+t.id+'\')">\uD83D\uDDA8 \u0414\u0440\u0443\u043A</button>'
        +'<div class="pr-foot-total">\u0420\u0430\u0437\u043E\u043C <b>'+money(pt.total)+'\u20B4</b></div>'
      +'</div>'
    +'</div>';
  });
  html+='</div>';

  if(!rows.length) html='<div class="pr-empty"><div style="font-size:38px;margin-bottom:8px">\uD83D\uDCB0</div>\u0417\u0430 \u0446\u0435\u0439 \u043C\u0456\u0441\u044F\u0446\u044C \u043D\u0435\u043C\u0430\u0454 \u043F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u0438\u0445 \u0437\u0430\u043D\u044F\u0442\u044C</div>';
  wrap.innerHTML=html;
}

// Місяць прописом для періоду 'YYYY-MM'
function prMonthName(per){
  var p=String(per||'').split('-');
  var m=['\u0441\u0456\u0447\u0435\u043D\u044C','\u043B\u044E\u0442\u0438\u0439','\u0431\u0435\u0440\u0435\u0437\u0435\u043D\u044C','\u043A\u0432\u0456\u0442\u0435\u043D\u044C','\u0442\u0440\u0430\u0432\u0435\u043D\u044C','\u0447\u0435\u0440\u0432\u0435\u043D\u044C','\u043B\u0438\u043F\u0435\u043D\u044C','\u0441\u0435\u0440\u043F\u0435\u043D\u044C','\u0432\u0435\u0440\u0435\u0441\u0435\u043D\u044C','\u0436\u043E\u0432\u0442\u0435\u043D\u044C','\u043B\u0438\u0441\u0442\u043E\u043F\u0430\u0434','\u0433\u0440\u0443\u0434\u0435\u043D\u044C'][(parseInt(p[1])||1)-1];
  return (m||'')+' '+(p[0]||'');
}
function prPlural(n, one, few, many){
  var n10=n%10, n100=n%100;
  if(n10===1&&n100!==11) return one;
  if(n10>=2&&n10<=4&&(n100<10||n100>=20)) return few;
  return many;
}

function openPayrollItemM(tutorId){
  if(!canPayroll()) return;
  window._prItemTutor=tutorId;
  var t=(S.tutors||[]).find(function(x){return x.id===tutorId;});
  var nEl=document.getElementById('pri-tutor-name');
  if(nEl) nEl.textContent=t?(t.fn+' '+t.ln+' \u00B7 '+payrollPeriod()):'';
  document.getElementById('pri-label').value='';
  document.getElementById('pri-amount').value='';
  document.getElementById('pri-percent').value='';
  openM('mo-payroll-item');
}

async function savePayrollItem(){
  if(!canPayroll()) return;
  var label=(document.getElementById('pri-label')||{value:''}).value.trim();
  var amount=(document.getElementById('pri-amount')||{value:''}).value;
  var percent=(document.getElementById('pri-percent')||{value:''}).value;
  if(!label){ mkToast('\u0412\u043A\u0430\u0436\u0456\u0442\u044C \u043F\u043E\u044F\u0441\u043D\u0435\u043D\u043D\u044F','error'); return; }
  if(!amount&&!percent){ mkToast('\u0412\u043A\u0430\u0436\u0456\u0442\u044C \u0441\u0443\u043C\u0443 \u0430\u0431\u043E \u0432\u0456\u0434\u0441\u043E\u0442\u043E\u043A','error'); return; }
  var obj={ id:uid(), tutor_id:window._prItemTutor, period:payrollPeriod(),
    label:label,
    amount:amount!==''?parseFloat(amount):null,
    percent:percent!==''?parseFloat(percent):null,
    created_by:CU?CU.id:null, created_at:new Date().toISOString() };
  try{
    await dbInsert('payroll_items',obj);
    if(!(S.payrollItems||[]).some(function(x){return x.id===obj.id;}))
      S.payrollItems=(S.payrollItems||[]).concat([normalizePayrollItem(obj)]);
    closeM('mo-payroll-item'); mkToast('\u0414\u043E\u0434\u0430\u043D\u043E');
    renderPayroll();
  }catch(e){ mkToast('\u041F\u043E\u043C\u0438\u043B\u043A\u0430: '+(e.message||e),'error'); }
}

async function delPayrollItem(id){
  if(!canPayroll()) return;
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u043F\u0443\u043D\u043A\u0442?')) return;
  try{
    await dbDelete('payroll_items',id);
    S.payrollItems=(S.payrollItems||[]).filter(function(x){return x.id!==id;});
    renderPayroll();
  }catch(e){}
}

// Друк відомості: tutorId — один репетитор, null — усі разом
function printPayroll(tutorId){
  if(!canPayroll()) return;
  var per=payrollPeriod();
  var perLbl=(function(){ var p=per.split('-'); var m=['\u0441\u0456\u0447\u0435\u043D\u044C','\u043B\u044E\u0442\u0438\u0439','\u0431\u0435\u0440\u0435\u0437\u0435\u043D\u044C','\u043A\u0432\u0456\u0442\u0435\u043D\u044C','\u0442\u0440\u0430\u0432\u0435\u043D\u044C','\u0447\u0435\u0440\u0432\u0435\u043D\u044C','\u043B\u0438\u043F\u0435\u043D\u044C','\u0441\u0435\u0440\u043F\u0435\u043D\u044C','\u0432\u0435\u0440\u0435\u0441\u0435\u043D\u044C','\u0436\u043E\u0432\u0442\u0435\u043D\u044C','\u043B\u0438\u0441\u0442\u043E\u043F\u0430\u0434','\u0433\u0440\u0443\u0434\u0435\u043D\u044C'][parseInt(p[1])-1]; return m+' '+p[0]; })();
  var tutors=tutorId?(S.tutors||[]).filter(function(t){return t.id===tutorId;})
    :(S.tutors||[]).slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');});
  var grand=0, body='';
  tutors.forEach(function(t){
    var pt=payrollTotal(t.id, per), b=pt.base;
    if(!tutorId&&!b.doneCount&&!b.makeupCount&&!payrollItemsFor(t.id,per).length) return;
    grand+=pt.total;
    var rows='<tr><td>\u041F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E \u0443\u0440\u043E\u043A\u0456\u0432: '+b.doneCount+' ('+b.doneHours+' \u0433\u043E\u0434) \u00D7 '+payrollCoef()+'</td><td class="r"><b>'+b.doneSalary+' \u20B4</b></td></tr>'
      +'<tr><td>\u0412\u0456\u0434\u043F\u0440\u0430\u0446\u044C\u043E\u0432\u0430\u043D\u043E: '+b.makeupCount+' ('+b.makeupHours+' \u0433\u043E\u0434) \u00D7 '+payrollCoef()+'</td><td class="r"><b>'+b.makeupSalary+' \u20B4</b></td></tr>';
    payrollItemsFor(t.id,per).forEach(function(i){
      var amt=payrollItemAmount(i,b.base);
      rows+='<tr><td>'+(i.label||'')+(i.percent!=null&&i.percent!==''?' ('+i.percent+'%)':'')+'</td><td class="r">'+(amt>=0?'+':'')+amt+' \u20B4</td></tr>';
    });
    rows+='<tr class="tot"><td><b>\u0420\u0430\u0437\u043E\u043C \u0434\u043E \u0432\u0438\u043F\u043B\u0430\u0442\u0438</b></td><td class="r"><b>'+pt.total+' \u20B4</b></td></tr>';
    body+='<h3>'+t.fn+' '+t.ln+'</h3><table>'+rows+'</table>'
      +'<div class="sign">\u041F\u0456\u0434\u043F\u0438\u0441 \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0430: ______________________</div>';
  });
  if(!tutorId&&tutors.length>1) body+='<h2 class="grand">\u0412\u0421\u042C\u041E\u0413\u041E \u0424\u041E\u041D\u0414 \u041E\u041F\u041B\u0410\u0422\u0418: '+Math.round(grand*100)/100+' \u20B4</h2>';
  var w=window.open('','_blank');
  w.document.write('<!DOCTYPE html><html><head><meta charset="utf-8"><title>\u0412\u0456\u0434\u043E\u043C\u0456\u0441\u0442\u044C \u0437\u0430\u0440\u043F\u043B\u0430\u0442\u0438</title>'
    +'<style>body{font-family:Arial,sans-serif;max-width:640px;margin:24px auto;color:#111}'
    +'h1{font-size:18px;margin-bottom:2px} .per{color:#666;font-size:13px;margin-bottom:18px}'
    +'h3{margin:18px 0 6px;font-size:15px;border-bottom:2px solid #111;padding-bottom:3px}'
    +'table{width:100%;border-collapse:collapse;font-size:13px}'
    +'td{padding:4px 2px;border-bottom:1px solid #ddd} .r{text-align:right;white-space:nowrap}'
    +'.sub td{border-top:1.5px solid #999} .tot td{border-top:2px solid #111;border-bottom:none;font-size:14px}'
    +'.sign{margin:14px 0 22px;font-size:12px;color:#444}'
    +'.grand{margin-top:24px;font-size:16px;border-top:3px double #111;padding-top:10px;text-align:right}'
    +'@media print{.noprint{display:none}}</style></head><body>'
    +'<h1>\u0412\u0456\u0434\u043E\u043C\u0456\u0441\u0442\u044C \u043D\u0430\u0440\u0430\u0445\u0443\u0432\u0430\u043D\u043D\u044F \u0437\u0430\u0440\u043F\u043B\u0430\u0442\u0438</h1>'
    +'<div class="per">\u041F\u0435\u0440\u0456\u043E\u0434: '+perLbl+' \u00B7 \u0421\u0444\u043E\u0440\u043C\u043E\u0432\u0430\u043D\u043E: '+new Date().toLocaleDateString('uk-UA')+'</div>'
    +body
    +'<button class="noprint" onclick="window.print()" style="margin-top:16px;padding:8px 18px;font-size:14px;cursor:pointer">\uD83D\uDDA8 \u0414\u0440\u0443\u043A\u0443\u0432\u0430\u0442\u0438</button>'
    +'</body></html>');
  w.document.close();
  setTimeout(function(){ try{w.print();}catch(e){} }, 400);
}

window.renderPayroll=renderPayroll;
// ══════════ РЕНДЕР ЖУРНАЛУ ЗМІН ══════════
async function renderAudit(){
  var tbody=document.getElementById('audit-tbody');
  if(!tbody) return;
  if(R()!=='god'){ tbody.innerHTML='<tr><td colspan="6" style="text-align:center;padding:20px;color:var(--t3)">\u0414\u043E\u0441\u0442\u0443\u043F\u043D\u043E \u043B\u0438\u0448\u0435 \u0431\u043E\u0433\u0443</td></tr>'; return; }

  // Фільтр по користувачу
  var uSel=document.getElementById('af-user');
  if(uSel){
    var prev=uSel.value;
    uSel.innerHTML='<option value="">\u0412\u0441\u0456 \u043A\u043E\u0440\u0438\u0441\u0442\u0443\u0432\u0430\u0447\u0456</option>'
      +(S.users||[]).slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');})
        .map(function(u){var rl=(ROLES[u.role]||{}).label||u.role||'';return '<option value="'+u.id+'">'+u.fn+' '+u.ln+' ('+rl+')</option>';}).join('');
    uSel.value=prev;
  }
  var fUser=(uSel||{value:''}).value;
  var fAction=(document.getElementById('af-action')||{value:''}).value;

  tbody.innerHTML='<tr><td colspan="6" style="text-align:center;padding:16px;color:var(--t3)">\u0417\u0430\u0432\u0430\u043D\u0442\u0430\u0436\u0435\u043D\u043D\u044F...</td></tr>';
  try{
    function buildAQ(){ var q=_sb.from('audit_log').select('*').order('created_at',{ascending:false}).limit(500); if(fUser) q=q.eq('user_id',fUser); if(fAction) q=q.eq('action',fAction); return q; }
    var res=await buildAQ();
    if(res.error && await refreshIfExpired(res.error)) res=await buildAQ();
    if(res.error) throw res.error;
    var rows=res.data||[];
    if(!rows.length){ tbody.innerHTML='<tr><td colspan="6" style="text-align:center;padding:20px;color:var(--t3)">\u0417\u0430\u043F\u0438\u0441\u0456\u0432 \u043D\u0435\u043C\u0430\u0454</td></tr>'; return; }
    tbody.innerHTML=rows.map(function(r){
      var when=r.created_at?new Date(r.created_at).toLocaleString('uk-UA',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'}):'\u2014';
      var actLbl=AUDIT_ACTION_LABELS[r.action]||r.action||'';
      var actColor=r.action==='delete'?'var(--danger)':r.action==='insert'?'var(--tut)':'var(--adm)';
      var tblLbl=AUDIT_TABLE_LABELS[r.table_name]||r.table_name||'';
      var rl=(ROLES[r.user_role]||{}).label||r.user_role||'';
      return '<tr>'
        +'<td style="font-size:11px;color:var(--t2);white-space:nowrap">'+when+'</td>'
        +'<td style="font-size:12px"><b>'+(r.user_name||'\u2014')+'</b><div style="font-size:10px;color:var(--t3)">'+rl+'</div></td>'
        +'<td><span style="font-size:11px;font-weight:700;color:'+actColor+'">'+actLbl+'</span> <span style="font-size:11px;color:var(--t2)">'+tblLbl+'</span></td>'
        +'<td style="font-size:12px">'+(r.descr||'\u2014')+'</td>'
        +'<td style="font-size:10px;color:var(--t3)">'+(r.record_id||'')+'</td>'
        +'<td style="text-align:right"><button onclick="delAuditRow(\''+r.id+'\')" title="\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0437\u0430\u043F\u0438\u0441" style="border:none;background:none;cursor:pointer;color:var(--danger);font-size:13px;padding:2px 6px">\uD83D\uDDD1</button></td>'
        +'</tr>';
    }).join('');
  }catch(e){
    tbody.innerHTML='<tr><td colspan="6" style="text-align:center;padding:20px;color:var(--danger)">\u041F\u043E\u043C\u0438\u043B\u043A\u0430: '+(e.message||e)+'</td></tr>';
  }
}
window.renderAudit=renderAudit;

async function delAuditRow(id){
  if(R()!=='god'){ mkToast('\u0414\u043E\u0441\u0442\u0443\u043F\u043D\u043E \u043B\u0438\u0448\u0435 \u0431\u043E\u0433\u0443','error'); return; }
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0446\u0435\u0439 \u0437\u0430\u043F\u0438\u0441 \u0436\u0443\u0440\u043D\u0430\u043B\u0443?')) return;
  try{
    var r=await _sb.from('audit_log').delete().eq('id',id);
    if(r.error && await refreshIfExpired(r.error)) r=await _sb.from('audit_log').delete().eq('id',id);
    if(r.error){ mkToast('\u041F\u043E\u043C\u0438\u043B\u043A\u0430: '+r.error.message,'error'); return; }
    renderAudit();
  }catch(e){ mkToast('\u041F\u043E\u043C\u0438\u043B\u043A\u0430: '+(e.message||e),'error'); }
}

async function clearAuditLog(){
  if(R()!=='god'){ mkToast('\u0414\u043E\u0441\u0442\u0443\u043F\u043D\u043E \u043B\u0438\u0448\u0435 \u0431\u043E\u0433\u0443','error'); return; }
  // Очищення враховує активні фільтри — щоб можна було чистити вибірково
  var fUser=(document.getElementById('af-user')||{value:''}).value;
  var fAction=(document.getElementById('af-action')||{value:''}).value;
  var scope=(fUser||fAction)?'\u0432\u0456\u0434\u0444\u0456\u043B\u044C\u0442\u0440\u043E\u0432\u0430\u043D\u0456 \u0437\u0430\u043F\u0438\u0441\u0438':'\u0412\u0421\u042E \u0456\u0441\u0442\u043E\u0440\u0456\u044E \u0437\u043C\u0456\u043D';
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 '+scope+'? \u0426\u0435 \u043D\u0435\u0437\u0432\u043E\u0440\u043E\u0442\u043D\u043E.')) return;
  try{
    var q=_sb.from('audit_log').delete();
    if(fUser) q=q.eq('user_id',fUser);
    if(fAction) q=q.eq('action',fAction);
    if(!fUser && !fAction) q=q.neq('id','00000000-0000-0000-0000-000000000000'); // видалити все
    var r=await q;
    if(r.error && await refreshIfExpired(r.error)){
      q=_sb.from('audit_log').delete();
      if(fUser) q=q.eq('user_id',fUser);
      if(fAction) q=q.eq('action',fAction);
      if(!fUser && !fAction) q=q.neq('id','00000000-0000-0000-0000-000000000000');
      r=await q;
    }
    if(r.error){ mkToast('\u041F\u043E\u043C\u0438\u043B\u043A\u0430: '+r.error.message,'error'); return; }
    mkToast('\u0416\u0443\u0440\u043D\u0430\u043B \u043E\u0447\u0438\u0449\u0435\u043D\u043E');
    renderAudit();
  }catch(e){ mkToast('\u041F\u043E\u043C\u0438\u043B\u043A\u0430: '+(e.message||e),'error'); }
}
window.delAuditRow=delAuditRow;
window.clearAuditLog=clearAuditLog;

window.openPayrollItemM=openPayrollItemM;
window.savePayrollItem=savePayrollItem;
window.delPayrollItem=delPayrollItem;
window.printPayroll=printPayroll;
window.prSetCoef=prSetCoef;


window.mergeDuplicateStudents = mergeDuplicateStudents;
window.addRateRow = addRateRow;

// ── Автозаповнення предмета/репетитора в занятті зі ставок обраного учня ──
function lessApplyStudentRates(){
  var sid=document.getElementById('l-std')?.value;
  var st=sid?(S.students||[]).find(function(x){return x.id===sid;}):null;
  if(!st) return;
  var rules=studentRateRules(st);
  var subs=[],tuts=[];
  rules.forEach(function(r){
    if(r.subject&&subs.indexOf(r.subject)<0)subs.push(r.subject);
    if(r.tutor_id&&tuts.indexOf(r.tutor_id)<0)tuts.push(r.tutor_id);
  });
  // Фолбек на старі поля, якщо ставки ще не заповнені
  if(!subs.length&&st.subject) subs=String(st.subject).split(',').map(function(x){return x.trim();}).filter(Boolean);
  if(!tuts.length){ var lt=(st.tutorIds&&st.tutorIds.length)?st.tutorIds:(st.tutorId?[st.tutorId]:[]); tuts=lt.slice(); }
  // Підказки предметів: спочатку предмети учня, далі — загальний довідник
  var dl=document.getElementById('subj-list-l');
  if(dl&&subs.length){
    dl.innerHTML=subs.map(function(x){return '<option value="'+x+'">';}).join('')
      +(S.subjects||[]).filter(function(x){return subs.indexOf(x.name)<0;}).map(function(x){return '<option value="'+x.name+'">';}).join('');
  }
  var subjEl=document.getElementById('l-subj');
  if(subjEl&&!subjEl.value&&subs.length===1) subjEl.value=subs[0];
  var tEl=document.getElementById('l-tutor');
  if(tEl&&tuts.length===1){ tEl.value=tuts[0]; if(tEl._updateSearch)tEl._updateSearch(); }
  lessPickTutorForSubject();
}

// Коли обрано предмет — підбираємо репетитора за правилом ставок
function lessPickTutorForSubject(){
  var sid=document.getElementById('l-std')?.value;
  var st=sid?(S.students||[]).find(function(x){return x.id===sid;}):null;
  if(!st) return;
  var subj=(document.getElementById('l-subj')?.value||'').trim().toLowerCase();
  if(!subj) return;
  var rules=studentRateRules(st);
  var match=rules.find(function(r){return (r.subject||'').trim().toLowerCase()===subj&&r.tutor_id;});
  if(!match) match=rules.find(function(r){return !(r.subject||'').trim()&&r.tutor_id;});
  if(match){
    var tEl=document.getElementById('l-tutor');
    if(tEl){ tEl.value=match.tutor_id; if(tEl._updateSearch)tEl._updateSearch(); }
  }
}
// Усі відомі предмети: довідник + ставки учнів + наявні заняття
function allKnownSubjects(){
  var set=[];
  function add(v){ v=(v||'').trim(); if(v&&set.indexOf(v)<0) set.push(v); }
  (S.subjects||[]).forEach(function(x){ add(x&&x.name); });
  (S.students||[]).forEach(function(st){ studentRateRules(st).forEach(function(r){ add(r.subject); }); });
  (S.lessons||[]).forEach(function(l){ add(l.subject); });
  return set.sort(function(a,b){return a.localeCompare(b,'uk');});
}

// Кастомний дропдаун підказок для текстового інпута (працює і на телефонах,
// на відміну від нативного <datalist>). getOptions() повертає масив рядків.
function makeSuggest(inputId, getOptions){
  var inp=document.getElementById(inputId);
  if(!inp||inp.dataset.suggest==='1') return;
  inp.dataset.suggest='1';
  // Прибираємо нативний datalist — щоб порожній браузерний попап не перекривав кастомний
  inp.removeAttribute('list');
  var wrap=document.createElement('div');
  wrap.style.cssText='position:relative;display:block;width:100%;flex:1;min-width:0';
  inp.parentNode.insertBefore(wrap,inp);
  wrap.appendChild(inp);
  var drop=document.createElement('div');
  wrap.appendChild(drop);
  var backdrop=null;

  function isMobile(){ return window.innerWidth<=640; }

  function ensureBackdrop(){
    if(backdrop) return;
    backdrop=document.createElement('div');
    backdrop.style.cssText='position:fixed;inset:0;background:rgba(0,0,0,.4);z-index:9999;display:none';
    backdrop.addEventListener('mousedown',function(e){ e.preventDefault(); closeDrop(); });
    document.body.appendChild(backdrop);
  }

  var isOpen=false;

  function positionDrop(){
    if(isMobile()){
      ensureBackdrop();
      // На мобільному — на весь екран знизу (bottom sheet), як нативний піквер репетиторів
      drop.style.cssText='position:fixed;left:0;right:0;bottom:0;top:auto;max-height:65vh;min-height:200px;'
        +'background:var(--s1);border-radius:18px 18px 0 0;box-shadow:0 -8px 30px rgba(0,0,0,.25);'
        +'z-index:10000;overflow-y:auto;padding:8px 0 max(8px,env(safe-area-inset-bottom))';
    } else {
      drop.style.cssText='position:absolute;top:100%;left:0;right:0;background:var(--s1);border:1px solid var(--b1);border-radius:8px;max-height:200px;overflow-y:auto;z-index:9999;box-shadow:0 4px 16px rgba(0,0,0,.18)';
    }
    // Позиціонування не має чіпати видимість — інакше resize від появи клавіатури ховає щойно відкритий список
    drop.style.display=isOpen?'block':'none';
    if(backdrop) backdrop.style.display=isOpen?'block':'none';
  }
  function closeDrop(){
    isOpen=false;
    drop.style.display='none';
    if(backdrop) backdrop.style.display='none';
  }
  positionDrop();

  function render(q){
    var opts=getOptions()||[];
    var f=q?opts.filter(function(o){return o.toLowerCase().includes(q.toLowerCase());}):opts;
    if(!f.length){ closeDrop(); return; }
    var mob=isMobile();
    drop.innerHTML='';
    if(mob){
      var handle=document.createElement('div');
      handle.style.cssText='width:36px;height:4px;background:var(--b2);border-radius:4px;margin:2px auto 8px';
      drop.appendChild(handle);
    }
    f.slice(0,40).forEach(function(o){
      var it=document.createElement('div');
      it.textContent=o;
      it.style.cssText=mob
        ? 'padding:14px 18px;cursor:pointer;font-size:15px;color:var(--t1);border-bottom:1px solid var(--s3)'
        : 'padding:8px 12px;cursor:pointer;font-size:13px;color:var(--t1)';
      it.addEventListener('mouseenter',function(){it.style.background='var(--s2)';});
      it.addEventListener('mouseleave',function(){it.style.background='';});
      it.addEventListener('mousedown',function(e){
        e.preventDefault();
        inp.value=o;
        closeDrop();
        inp.dispatchEvent(new Event('change'));
      });
      drop.appendChild(it);
    });
    isOpen=true;
    drop.style.display='block';
    if(backdrop) backdrop.style.display='block';
  }
  inp.addEventListener('focus',function(){ positionDrop(); render(inp.value); });
  inp.addEventListener('input',function(){ render(inp.value); });
  inp.addEventListener('blur',function(){
    if(isMobile()) return; // на мобільному закриває лише вибір пункту або тап по фону
    setTimeout(function(){ closeDrop(); },200);
  });
  // На resize перебудовуємо лише розміри/позицію (fixed/absolute), не видимість —
  // це головний фікс: відкриття клавіатури на мобільному теж генерує resize.
  window.addEventListener('resize', positionDrop);
}
window.makeSuggest=makeSuggest;
window.allKnownSubjects=allKnownSubjects;
window.lessApplyStudentRates=lessApplyStudentRates;
window.lessPickTutorForSubject=lessPickTutorForSubject;
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

function sRenderTutorTags(ids){
  var list=document.getElementById('s-tutor-list');
  if(!list) return;
  list.innerHTML=(ids||[]).map(function(tid){
    var t=(S.tutors||[]).find(function(x){return x.id===tid;});
    if(!t) return '';
    return '<span class="s-tutor-tag" data-id="'+tid+'" style="display:inline-flex;align-items:center;gap:5px;padding:4px 10px;border:1px solid var(--adm);border-radius:20px;background:rgba(41,171,226,.1);font-size:12px;color:var(--adm)">'
      +t.fn+' '+t.ln
      +'<button type="button" onclick="sRemoveTutor(\''+tid+'\')" style="background:none;border:none;cursor:pointer;color:var(--adm);font-size:14px;line-height:1;padding:0;margin-left:2px">×</button>'
      +'</span>';
  }).join('');
}

function sAddTutor(){
  var input=document.getElementById('s-tutor-input');
  if(!input||!input.value.trim()) return;
  var val=input.value.trim();
  // Шукаємо репетитора по імені
  var t=(S.tutors||[]).find(function(x){return (x.fn+' '+x.ln).toLowerCase()===val.toLowerCase();});
  if(!t){ mkToast('Репетитора не знайдено в списку','error'); return; }
  // Перевіряємо чи вже доданий
  var existing=Array.from(document.querySelectorAll('.s-tutor-tag')).map(function(el){return el.dataset.id;});
  if(existing.indexOf(t.id)>=0){ mkToast('Вже доданий','error'); return; }
  existing.push(t.id);
  sRenderTutorTags(existing);
  input.value='';
}

function sRemoveTutor(id){
  var existing=Array.from(document.querySelectorAll('.s-tutor-tag')).map(function(el){return el.dataset.id;}).filter(function(x){return x!==id;});
  sRenderTutorTags(existing);
}

window.sAddTutor=sAddTutor;
window.sRemoveTutor=sRemoveTutor;

function openStudM(id=null){
  if(!can('students')){mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432','error');return;}
  S.editId=id;document.getElementById('ms-title').textContent=id?'\u0420\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u0442\u0438 \u0443\u0447\u043D\u044F':'\u041D\u043E\u0432\u0438\u0439 \u0443\u0447\u0435\u043D\u044C';
  // Populate subject datalist for student modal
  var dl_s=document.getElementById('subj-list-s');
  if(dl_s){dl_s.innerHTML=(S.subjects||[]).map(function(x){return '<option value="'+x.name+'">';}).join('');}
  // Populate tutor datalist for searchable input
  var tutorDl=document.getElementById('s-tutor-datalist');
  if(tutorDl){tutorDl.innerHTML=(S.tutors||[]).map(function(t){return '<option value="'+t.fn+' '+t.ln+'" data-id="'+t.id+'">';}).join('');}
  // Clear tutor tags and hidden select
  var stList=document.getElementById('s-tutor-list');
  var stInput=document.getElementById('s-tutor-input');
  if(stList) stList.innerHTML='';
  if(stInput) stInput.value='';
  var stSel=document.getElementById('s-tutor');
  if(stSel){stSel.innerHTML=(S.tutors||[]).map(function(t){return '<option value="'+t.id+'">'+t.fn+' '+t.ln+'</option>';}).join('');}
  // Populate CRM responsible select (admins/directors/god)
  var respSel=document.getElementById('s-crm-resp');
  if(respSel){
    respSel.innerHTML='<option value="">\u2014 \u043d\u0435 \u043f\u0440\u0438\u0437\u043d\u0430\u0447\u0435\u043d\u043e \u2014</option>'
      +(S.users||[]).filter(function(u){return u.role==='god'||u.role==='director'||u.role==='admin';})
        .map(function(u){return '<option value="'+u.id+'">'+u.fn+' '+u.ln+'</option>';}).join('');
  }
  const flds=['fn','ln','age','grade','phone','email','notes'];
  const pflds=[];
  if(id){const s=S.students.find(x=>x.id===id);if(s){flds.forEach(f=>{const el=document.getElementById('s-'+f);if(el)el.value=s[f]||'';});
  var _sSubjEl=document.getElementById('s-subj'); if(_sSubjEl)_sSubjEl.value=s.subject||'';
  // Render tutor tags (legacy, if element exists)
  var _tIds=s.tutorIds||(s.tutorId?[s.tutorId]:[]);
  sRenderTutorTags(_tIds);document.getElementById('s-status').value=s.status||'active';document.getElementById('s-src').value=s.src||'referral';
      var crmStEl=document.getElementById('s-crm-stage'); if(crmStEl) crmStEl.value=getCrmStage(s);
      var crmRespEl=document.getElementById('s-crm-resp'); if(crmRespEl) crmRespEl.value=s.crmResponsible||'';
      var pf=document.getElementById('s-parent-fn');if(pf)pf.value=s.parentFn||'';
      var pp=document.getElementById('s-parent-phone');if(pp)pp.value=s.parentPhone||'';
      var rt=document.getElementById('s-rate');if(rt)rt.value=(s.hourly_rate!=null?s.hourly_rate:'');
      var _rl=document.getElementById('s-rates-list');
      if(_rl){_rl.innerHTML='';var _rr=studentRateRules(s);
        if(!_rr.length){
          // Міграція: у старого учня ставок ще немає — заповнюємо зі старих полів предмет/репетитори
          var _lt=(s.tutorIds&&s.tutorIds.length)?s.tutorIds:(s.tutorId?[s.tutorId]:[]);
          if(_lt.length) _rr=_lt.map(function(tid){return {subject:s.subject||'',tutor_id:tid,rate:(s.hourly_rate!=null?s.hourly_rate:null)};});
          else if(s.subject) _rr=[{subject:s.subject,tutor_id:'',rate:(s.hourly_rate!=null?s.hourly_rate:null)}];
        }
        _rr.forEach(function(r){addRateRow(r);});}}}
  else{flds.forEach(f=>{const el=document.getElementById('s-'+f);if(el)el.value='';});pflds.forEach(f=>{const el=document.getElementById('s-'+f);if(el)el.value='';});document.getElementById('s-status').value='active';document.getElementById('s-src').value='referral';
    var rtN=document.getElementById('s-rate'); if(rtN) rtN.value='';
    var _rlN=document.getElementById('s-rates-list'); if(_rlN) _rlN.innerHTML='';
    var crmStEl2=document.getElementById('s-crm-stage'); if(crmStEl2) crmStEl2.value='lead';
    var crmRespEl2=document.getElementById('s-crm-resp'); if(crmRespEl2) crmRespEl2.value='';
  }
  renderCustomFields('student','mo-student-cf');
  renderStudentCard(id);
  var invBtn = document.getElementById('inv-btn');
  if(invBtn) invBtn.style.display = (id && (R()==='god'||R()==='director')) ? 'inline-flex' : 'none';
  openM('mo-student');
}


function renderStudentCard(id){
  var card=document.getElementById('ms-card');
  if(!card) return;
  if(!id){ card.style.display='none'; return; }
  var s=(S.students||[]).find(function(x){return x.id===id;});
  if(!s){ card.style.display='none'; return; }
  card.style.display='block';

  // Аватар
  var av=document.getElementById('ms-av');
  if(av){ av.textContent=(s.fn||'?')[0]+(s.ln||'')[0]||''; }

  // Ім'я та мета
  var nm=document.getElementById('ms-fullname');
  if(nm) nm.textContent=(s.fn||'')+' '+(s.ln||'');
  var tutors=(s.tutorIds||[]).map(function(tid){
    var t=(S.tutors||[]).find(function(x){return x.id===tid;});
    return t?t.fn+' '+t.ln:'';
  }).filter(Boolean).join(', ');
  var meta=document.getElementById('ms-meta');
  if(meta) meta.textContent=[s.subject,tutors,s.grade].filter(Boolean).join(' · ');

  // Теги
  var tags=document.getElementById('ms-tags');
  if(tags){
    var statusLabels={active:'Активний',trial:'Пробний',paused:'Призупинений',completed:'Завершив'};
    var statusColors={active:'#eaf3de;color:#3B6D11',trial:'#e6f1fb;color:#185FA5',paused:'#faeeda;color:#854F0B',completed:'#f1efe8;color:#5f5e5a'};
    var tagHtml='<span style="font-size:11px;padding:2px 8px;border-radius:4px;background:'+(statusColors[s.status]||'#f1efe8;color:#444')+'">'+(statusLabels[s.status]||s.status)+'</span>';

    // Пропуски
    var missedCount=(S.lessons||[]).filter(function(l){return (l.studentId||l.student_id)===id&&l.status==='missed'&&!isCoveredMissed(l);}).length;
    if(missedCount>0) tagHtml+='<span style="font-size:11px;padding:2px 8px;border-radius:4px;background:#fcebeb;color:#A32D2D">'+missedCount+' пропуск'+(missedCount===1?'':'и')+'</span>';

    tags.innerHTML=tagHtml;
  }

  // Таймлайн — події за останній місяць
  var tl=document.getElementById('ms-timeline');
  if(tl){
    var events=[];
    var monthAgo=new Date(); monthAgo.setMonth(monthAgo.getMonth()-1);
    var monthAgoStr=monthAgo.toISOString().slice(0,10);

    // Заняття за місяць
    var sLessons=(S.lessons||[]).filter(function(l){
      return (l.studentId||l.student_id)===id && (l.date||'')>=monthAgoStr;
    }).sort(function(a,b){return (b.date||'').localeCompare(a.date||'');});
    var icons2={done:'\u2705',completed:'\u2705',makeup:'\uD83D\uDD04',missed:'\u274C',planned:'\uD83D\uDCC5',cancelled:'\uD83D\uDEAB'};
    var lbls2={done:'\u0423\u0440\u043E\u043A \u043F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E',completed:'\u0423\u0440\u043E\u043A \u043F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E',makeup:'\u0412\u0456\u0434\u043F\u0440\u0430\u0446\u044E\u0432\u0430\u043D\u043D\u044F',missed:'\u041F\u0440\u043E\u043F\u0443\u0441\u043A',planned:'\u0417\u0430\u043F\u043B\u0430\u043D\u043E\u0432\u0430\u043D\u0438\u0439 \u0443\u0440\u043E\u043A',cancelled:'\u0421\u043A\u0430\u0441\u043E\u0432\u0430\u043D\u043E'};
    var clr2={done:'var(--tut)',completed:'var(--tut)',makeup:'#f59e0b',missed:'var(--danger)',planned:'var(--adm)',cancelled:'var(--t3)'};
    sLessons.forEach(function(l){
      events.push({date:l.date,color:clr2[l.status]||'var(--t2)',
        text:(icons2[l.status]||'\uD83D\uDCDA')+' '+(lbls2[l.status]||l.status)+' \u00B7 '+fd(l.date)+(l.time?' \u00B7 '+l.time:'')+(l.subject?' \u00B7 '+l.subject:'')});
    });

    // Комунікації за місяць
    var sComms=(S.comms||[]).filter(function(c){
      return (c.studentId||c.student_id)===id && (c.date||'')>=monthAgoStr;
    }).sort(function(a,b){return (b.date||'').localeCompare(a.date||'');}).slice(0,5);
    sComms.forEach(function(c){
      var ico={call:'\uD83D\uDCDE',message:'\uD83D\uDCAC',meeting:'\uD83E\uDD1D',email:'\uD83D\uDCE7'};
      events.push({date:c.date,color:'var(--t2)',text:(ico[c.type]||'\uD83D\uDCCB')+' '+(c.note||c.type||'\u041A\u043E\u043C\u0443\u043D\u0456\u043A\u0430\u0446\u0456\u044F')+' \u00B7 '+fd(c.date)});
    });

    events.sort(function(a,b){return (b.date||'').localeCompare(a.date||'');});
    events=events.slice(0,20);

    tl.innerHTML=events.length
      ? events.map(function(e){
          return '<div style="display:flex;gap:6px;align-items:flex-start;margin-bottom:2px">'
            +'<span style="width:8px;height:8px;border-radius:50%;background:'+(e.color||'var(--b1)')+';flex-shrink:0;margin-top:3px;margin-left:-14px;border:1.5px solid var(--s2)"></span>'
            +'<span style="font-size:11px;color:var(--t2)">'+e.text+'</span></div>';
        }).join('')
      : '<div style="font-size:11px;color:var(--t3)">\u041F\u043E\u0434\u0456\u0439 \u0437\u0430 \u043E\u0441\u0442\u0430\u043D\u043D\u0456\u0439 \u043C\u0456\u0441\u044F\u0446\u044C \u043D\u0435\u043C\u0430\u0454</div>';
  }
}
window.renderStudentCard = renderStudentCard;

function openTutM(id=null){
  if(!can('tutors')){mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432','error');return;}
  S.editId=id;document.getElementById('mt-title').textContent=id?'\u0420\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u0442\u0438 \u0432\u0438\u043A\u043B\u0430\u0434\u0430\u0447\u0430':'\u041D\u043E\u0432\u0438\u0439 \u0432\u0438\u043A\u043B\u0430\u0434\u0430\u0447';

  // Populate account select: only show users with role 'tutor' not already linked to another tutor
  var accSel=document.getElementById('t-acc');
  if(accSel){
    var linkedIds=(S.tutors||[]).filter(function(t){return t.id!==id;}).map(function(t){return t.accId||t.acc_uid;}).filter(Boolean);
    var opts=(S.users||[]).filter(function(u){
      return u.role==='tutor' && linkedIds.indexOf(u.id)<0;
    }).map(function(u){
      return '<option value="'+u.id+'">'+(u.fn||'')+' '+(u.ln||'')+(u.username?' (@'+u.username+')':'')+'</option>';
    }).join('');
    accSel.innerHTML='<option value="">\u2014 \u043D\u0435 \u043F\u0440\u0438\u0432\'\u044F\u0437\u0430\u043D\u043E \u2014</option>'+opts;
  }

  if(id){const t=S.tutors.find(x=>x.id===id);if(t){['fn','ln','phone','email','bio'].forEach(f=>{const el=document.getElementById('t-'+f);if(el)el.value=t[f]||'';});document.getElementById('t-subj').value=t.subj||'';if(document.getElementById('t-rate'))document.getElementById('t-rate').value=t.rate||'';
    if(accSel) accSel.value=t.accId||t.acc_uid||'';
  }}
  else{['fn','ln','phone','email','subj','rate','bio'].forEach(f=>{const el=document.getElementById('t-'+f);if(el)el.value='';});if(accSel)accSel.value='';}
  renderCustomFields('tutor','mo-tutor-cf');
  openM('mo-tutor');
}


function openLessM(id, date, time){
  if(!can('lessons')){mkToast('Немає прав','error');return;}
  S.editId = id||null;

  // Clear ALL fields first
  ['l-std','l-subj','l-tutor','l-price','l-notes',
   'l-miss-date','l-makeup-date','l-hw','l-games'].forEach(function(f){
    var el=document.getElementById(f); if(el) el.value='';
  });
  // Автозаповнення предмета/репетитора зі ставок учня (слухачі вішаються один раз)
  var _stdEl=document.getElementById('l-std');
  if(_stdEl&&!_stdEl._ratesWired){ _stdEl._ratesWired=true; _stdEl.addEventListener('change',lessApplyStudentRates); }
  var _sjEl=document.getElementById('l-subj');
  if(_sjEl&&!_sjEl._ratesWired){ _sjEl._ratesWired=true; _sjEl.addEventListener('change',lessPickTutorForSubject); }
  document.getElementById('l-dur').value = 60;
  document.getElementById('l-stat').value = 'planned';
  var re2=document.getElementById('l-recur'); if(re2) re2.value='none';
  var re3=document.getElementById('l-recur-end'); if(re3) re3.value='';
  var re4=document.getElementById('l-recur-count'); if(re4) re4.value='';
  var re5=document.getElementById('l-recur-interval'); if(re5) re5.value='7';
  ['l-miss-wrap','l-makeup-wrap','l-split-wrap','l-merge-wrap','recur-preview'].forEach(function(id2){
    var el=document.getElementById(id2); if(el) el.style.display='none';
  });

  // Populate dropdowns after clearing
  document.getElementById('ml-title').textContent = id ? 'Редагувати заняття' : 'Нове заняття';
  popSel('l-std', myStudents(), 'id', function(s){return s.fn+' '+s.ln;}, 'Оберіть учня');
  makeSearchable('l-std');
  if(document.getElementById('l-std')._updateSearch) document.getElementById('l-std')._updateSearch();
  var dl_l = document.getElementById('subj-list-l');
  if(dl_l) dl_l.innerHTML = allKnownSubjects().map(function(x){return '<option value="'+x+'">';}).join('');
  // Кастомний дропдаун (надійний на телефонах): предмети обраного учня першими, далі всі відомі
  makeSuggest('l-subj', function(){
    var sid=document.getElementById('l-std')?.value;
    var st=sid?(S.students||[]).find(function(x){return x.id===sid;}):null;
    var mine=[];
    if(st){
      studentRateRules(st).forEach(function(r){ var v=(r.subject||'').trim(); if(v&&mine.indexOf(v)<0) mine.push(v); });
      if(!mine.length&&st.subject) mine=String(st.subject).split(',').map(function(x){return x.trim();}).filter(Boolean);
    }
    return mine.concat(allKnownSubjects().filter(function(x){return mine.indexOf(x)<0;}));
  });
  popSel('l-tutor', S.tutors, 'id', function(t){return t.fn+' '+t.ln;}, 'Викладач');
  makeSearchable('l-tutor'); if(document.getElementById('l-tutor')._updateSearch) document.getElementById('l-tutor')._updateSearch();
  if(typeof toggleRecurOpts === 'function') toggleRecurOpts();

  if(id){
    var l = (S.lessons||[]).find(function(x){return x.id===id;});
    if(l){
      document.getElementById('l-std').value = l.studentId||l.student_id||'';
      if(document.getElementById('l-std')._updateSearch) document.getElementById('l-std')._updateSearch();
      document.getElementById('l-subj').value = l.subject||'';
      document.getElementById('l-tutor').value = l.tutorId||l.tutor_id||'';
      if(document.getElementById('l-tutor')._updateSearch) document.getElementById('l-tutor')._updateSearch();
      document.getElementById('l-date').value = l.date||'';
      document.getElementById('l-time').value = l.time||'10:00';
      document.getElementById('l-dur').value = l.dur||60;
      document.getElementById('l-stat').value = l.status||'planned';
      var _lp=document.getElementById('l-price'); if(_lp) _lp.value = l.price||'';
      document.getElementById('l-notes').value = l.notes||'';
      var _lg=document.getElementById('l-games'); if(_lg) _lg.value = l.games||'';
      // Load missed/makeup dates and hw
      var missEl=document.getElementById('l-miss-date');
      var autoMissDate = l.missed_date||'';
      // Якщо це makeup і є split_group_id — автоматично знаходимо дату пропущеного
      if(!autoMissDate && (l.status==='makeup'||l.status==='makeup_planned') && l.split_group_id){
        var origMissed=(S.lessons||[]).find(function(x){
          return x.id===l.split_group_id && x.status==='missed';
        });
        if(origMissed) autoMissDate=origMissed.date;
      }
      if(missEl) missEl.value=autoMissDate;
      var makeupEl=document.getElementById('l-makeup-date');
      if(makeupEl) makeupEl.value=l.makeup_date||'';
      var hwEl=document.getElementById('l-hw');
      if(hwEl) hwEl.value=l.hw||'';
      if(typeof onLessStatChange==='function') onLessStatChange();
      if(l.recurId){
        var siblings = (S.lessons||[]).filter(function(x){return x.recurId===l.recurId;});
        var box = document.getElementById('recur-preview');
        if(box){box.style.display='block';box.innerHTML='<span style="color:var(--adm)">🔁 Повторюване</span> — серія з <b>'+siblings.length+'</b> занять.';}
      }
    }
  } else {
    document.getElementById('l-date').value = date||localDateStr(new Date());
    document.getElementById('l-time').value = time||'10:00';
    var mt = myTutor();
    if(mt){ document.getElementById('l-tutor').value = mt.id; if(document.getElementById('l-tutor')._updateSearch) document.getElementById('l-tutor')._updateSearch(); }
  }

  if(typeof renderCustomFields==='function') renderCustomFields('lesson','mo-lesson-cf');
  var db=document.getElementById('del-lesson-btn'); if(db) db.style.display=id?'inline-flex':'none';
  var sb=document.getElementById('del-series-btn');
  if(sb){var lr=id?(S.lessons||[]).find(function(l){return l.id===id;}):null;sb.style.display=(lr&&lr.recurId)?'inline-flex':'none';}
  // Показуємо блок об'єднання і заповнюємо список
  var mw=document.getElementById('l-merge-wrap');
  if(mw) mw.style.display='block';
  populateMergeSelect();
  openM('mo-lesson');
}


function openPayM(id=null){
  if(!can('payments')){mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432','error');return;}
  S.editId=id;document.getElementById('mp-title').textContent=id?'\u0420\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u0442\u0438 \u043F\u043B\u0430\u0442\u0456\u0436':'\u041D\u043E\u0432\u0438\u0439 \u043F\u043B\u0430\u0442\u0456\u0436';
  popSel('p-std',S.students,'id',function(s){return s.fn+' '+s.ln;},'\u041E\u0431\u0435\u0440\u0456\u0442\u044C \u0443\u0447\u043D\u044F');
  makeSearchable('p-std'); if(document.getElementById('p-std')._updateSearch) document.getElementById('p-std')._updateSearch();
  const months=['\u0421\u0456\u0447\u0435\u043D\u044C','\u041B\u044E\u0442\u0438\u0439','\u0411\u0435\u0440\u0435\u0437\u0435\u043D\u044C','\u041A\u0432\u0456\u0442\u0435\u043D\u044C','\u0422\u0440\u0430\u0432\u0435\u043D\u044C','\u0427\u0435\u0440\u0432\u0435\u043D\u044C','\u041B\u0438\u043F\u0435\u043D\u044C','\u0421\u0435\u0440\u043F\u0435\u043D\u044C','\u0412\u0435\u0440\u0435\u0441\u0435\u043D\u044C','\u0416\u043E\u0432\u0442\u0435\u043D\u044C','\u041B\u0438\u0441\u0442\u043E\u043F\u0430\u0434','\u0413\u0440\u0443\u0434\u0435\u043D\u044C'];
  document.getElementById('p-date').value=localDateStr(new Date());
  document.getElementById('p-mon').value=months[new Date().getMonth()];
  if(id){const p=S.payments.find(x=>x.id===id);if(p){document.getElementById('p-std').value=p.studentId||'';if(document.getElementById('p-std')&&document.getElementById('p-std')._updateSearch) document.getElementById('p-std')._updateSearch();;document.getElementById('p-amt').value=p.amount||'';document.getElementById('p-mth').value=p.method||'cash';document.getElementById('p-date').value=p.date||'';document.getElementById('p-stat').value=p.status||'paid';document.getElementById('p-mon').value=p.month||months[new Date().getMonth()];document.getElementById('p-note').value=p.note||'';}}
  else{document.getElementById('p-std').value='';if(document.getElementById('p-std')&&document.getElementById('p-std')._updateSearch) document.getElementById('p-std')._updateSearch();;document.getElementById('p-amt').value='';document.getElementById('p-mth').value='cash';document.getElementById('p-stat').value='paid';document.getElementById('p-note').value='';}
  renderCustomFields('payment','mo-payment-cf');
  openM('mo-payment');
}


function openCommM(tutorId, commId){
  // Allow all logged-in users to add comms
  var mo=document.getElementById('mo-comm');
  if(!mo){ console.error('mo-comm not found'); return; }
  // Режим редагування: якщо передано commId — заповнюємо форму наявним записом
  window._editCommId = commId || null;
  var editC = commId ? (S.comms||[]).find(function(c){return c.id===commId;}) : null;
  if(commId && !editC){ mkToast('\u0417\u0430\u043F\u0438\u0441 \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E','error'); window._editCommId=null; return; }
  if(editC && !tutorId) tutorId = editC.tutorId || editC.tutor_id;
  var titleEl = mo.querySelector('.mdlt');
  if(titleEl) titleEl.textContent = editC ? '\u0420\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u0442\u0438 \u043A\u043E\u043C\u0443\u043D\u0456\u043A\u0430\u0446\u0456\u044E' : '\u041A\u043E\u043C\u0443\u043D\u0456\u043A\u0430\u0446\u0456\u044F';
  // Tutor select - hide for tutor role (pre-select own)
  var tSel=document.getElementById('cm-tutor');
  var tWrap=tSel?tSel.closest('.fgr'):null;
  var mt=myTutor();
  if(R()==='tutor'){
    if(tWrap) tWrap.style.display='none';
    if(tSel&&mt) tSel.innerHTML='<option value="'+mt.id+'">'+mt.fn+' '+mt.ln+'</option>';
  } else {
    if(tWrap) tWrap.style.display='';
    if(tSel){
      var selId=tutorId||(mt?mt.id:'');
      tSel.innerHTML='<option value="">Оберіть репетитора</option>'
        +(S.tutors||[]).map(function(t){
          return '<option value="'+t.id+'"'+(t.id===selId?' selected':'')+'>'+t.fn+' '+t.ln+'</option>';
        }).join('');
    }
  }
  // Student select - use myStudents()
  var sSel=document.getElementById('cm-student');
  if(sSel){
    sSel.innerHTML='<option value="">Учень (необов\'язково)</option>'
      +myStudents().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');})
        .map(function(s){return '<option value="'+s.id+'">'+s.fn+' '+s.ln+'</option>';}).join('');
    if(editC) sSel.value = editC.studentId || editC.student_id || '';
  }
  // Apply searchable to tutor+student selects
  if(R()!=='tutor'){ makeSearchable('cm-tutor'); if(document.getElementById('cm-tutor')._updateSearch) document.getElementById('cm-tutor')._updateSearch(); }
  makeSearchable('cm-student'); if(document.getElementById('cm-student')._updateSearch) document.getElementById('cm-student')._updateSearch();
  // Type field (у старих записах тип може бути message/meeting — мапимо на значення селекта)
  var typeEl=document.getElementById('cm-type');
  if(typeEl){
    var tmap={message:'msg',meeting:'meet'};
    typeEl.value = editC ? (tmap[editC.type]||editC.type||'call') : 'call';
  }
  // Note field
  var noteEl=document.getElementById('cm-note');
  if(noteEl) noteEl.value = editC ? (editC.note||'') : '';
  // Date = existing or today
  var dateEl=document.getElementById('cm-date');
  if(dateEl) dateEl.value = editC ? String(editC.date||'').slice(0,10) : localDateStr(new Date());
  if(editC && typeof updateParentInfo==='function') updateParentInfo();
  openM('mo-comm');
}


function nav(page){
  // Allow custom pages (added by God constructor) and built-in allowed pages
  const isCustomPage=page.startsWith('custom_');
  if(!isCustomPage&&!userNav().includes(page)){mkToast('\u041D\u0435\u043C\u0430\u0454 \u0434\u043E\u0441\u0442\u0443\u043F\u0443 \u0434\u043E \u0446\u044C\u043E\u0433\u043E \u0440\u043E\u0437\u0434\u0456\u043B\u0443','error');return;}
  document.querySelectorAll('.page').forEach(p=>{p.classList.remove('active');p.style.display=''});
  document.querySelectorAll('.ni').forEach(n=>n.classList.remove('active'));
  const pel=document.getElementById('pg-'+page);if(pel)pel.classList.add('active');
  const nel=document.getElementById('ni-'+page);
  if(nel){nel.classList.add('active');nel.className=nel.className.replace(/ (god|dir|tut)/g,'');if(R()==='god')nel.classList.add('god');else if(R()==='director')nel.classList.add('dir');else if(R()==='tutor')nel.classList.add('tut');}
  document.getElementById('ptitle').textContent=(PLABELS[page]||page);


  S.currentPage=page;
  try{ localStorage.setItem('sb_page', page); }catch(e){}
  const addMap={students:'\u0414\u043E\u0434\u0430\u0442\u0438 \u0443\u0447\u043D\u044F',tutors:'\u0414\u043E\u0434\u0430\u0442\u0438 \u0432\u0438\u043A\u043B\u0430\u0434\u0430\u0447\u0430',lessons:'\u0414\u043E\u0434\u0430\u0442\u0438 \u0437\u0430\u043D\u044F\u0442\u0442\u044F',payments:'\u0414\u043E\u0434\u0430\u0442\u0438 \u043F\u043B\u0430\u0442\u0456\u0436',schedule:'\u0414\u043E\u0434\u0430\u0442\u0438 \u0437\u0430\u043D\u044F\u0442\u0442\u044F',users:'\u0414\u043E\u0434\u0430\u0442\u0438 \u0430\u043A\u0430\u0443\u043D\u0442'};
  const ab=document.getElementById('addbtn');
  if(addMap[page]&&can(page==='users'?'users':page==='students'?'students':page==='tutors'?'tutors':page==='payments'?'payments':'lessons')){ab.textContent='+ '+addMap[page];ab.style.display='flex';}
  else ab.style.display='none';
  if(page==='dashboard'){
    renderDash(); // render with existing data immediately
  }
  if(page==='students')renderStudents();
  if(page==='tutors')renderTutors();
  if(page==='schedule')renderSch();
  if(page==='lessons')renderLessons();
  if(page==='payments')renderPayments();
  if(page==='reports')renderAllAnalytics();
  if(page==='branches'){renderBranches();renderBranchStats();}
  if(page==='users')renderUsers();
  if(page==='settings')renderSettings();
  if(page==='profile'){try{renderProfile();}catch(e){console.error('renderProfile:',e);}}
  if(page==='comms'){try{renderCommsPage();}catch(e){console.error('renderCommsPage:',e);}}
  if(page==='invoice'){ renderInvoicePage(); try{renderInvoiceStatus();}catch(e){} }
  if(page==='invoice-log') renderInvoiceLog();
  if(page==='missed') renderMissedLessons();
  if(page==='tasks'){try{renderTasks();}catch(e){console.error('renderTasks:',e);}}
  if(page==='payroll'){try{renderPayroll();}catch(e){console.error('renderPayroll:',e);}}
  if(page==='audit'){try{renderAudit();}catch(e){console.error('renderAudit:',e);}}
  if(page==='acts'){try{renderActsPage();}catch(e){console.error('renderActsPage:',e);}}
  if(page==='invoice'){ renderInvoicePage(); try{renderInvoiceStatus();}catch(e){} }
  if(page==='invoice-log') renderInvoiceLog();
  var _crmEl=document.getElementById('pg-crm');
  if(page==='crm'){if(_crmEl)_crmEl.style.display='flex';renderCrm();}
  else{if(_crmEl)_crmEl.style.display='none';}
    if(isCustomPage)renderCustomPage(page);
  try{updateTaskAlert();}catch(e){}
  if(window.innerWidth<=768)closeSidebar();
}


function openAdd(){
  const p=S.currentPage;
  if(p==='students')openStudM();
  else if(p==='tutors')openTutM();
  else if(p==='lessons'||p==='schedule')openLessM();
  else if(p==='payments')openPayM();
  else if(p==='users')openUserM();
}


function chWk(d){
  const view=S.schView||'week';
  if(view==='day'){if(d===0)S.dayOffset=0;else S.dayOffset=(S.dayOffset||0)+d;}
  else{if(d===0)S.weekOffset=0;else S.weekOffset=(S.weekOffset||0)+d;}
  renderSch();
}


function schSetView(v){
  S.schView = v;
  if(v === 'week') S.weekOffset = S.weekOffset || 0;
  else             S.dayOffset  = S.dayOffset  || 0;
  renderSch();
}


function toggleRecurOpts(){
  const v=document.getElementById('l-recur').value;
  const none=v==='none';
  document.getElementById('recur-interval-wrap').style.display=v==='custom'?'flex':'none';
  document.getElementById('recur-end-wrap').style.display=none?'none':'flex';
  document.getElementById('recur-count-wrap').style.display=none?'none':'flex';
  document.getElementById('recur-preview').style.display='none';
  document.getElementById('recur-preview-btn').style.display=none?'none':'flex';
}


function previewRecur(){
  const date=document.getElementById('l-date').value;
  const recur=document.getElementById('l-recur').value;
  const endDate=document.getElementById('l-recur-end').value;
  const count=document.getElementById('l-recur-count').value;
  const interval=document.getElementById('l-recur-interval').value;
  if(!date||recur==='none')return;
  const dates=genRecurDates(date,recur,endDate,count||52,interval);
  const allDates=[date,...dates];
  const labels={daily:'\u0429\u043E\u0434\u043D\u044F',weekly:'\u0429\u043E\u0442\u0438\u0436\u043D\u044F',biweekly:'\u0427\u0435\u0440\u0435\u0437 \u0442\u0438\u0436\u0434\u0435\u043D\u044C',monthly:'\u0429\u043E\u043C\u0456\u0441\u044F\u0446\u044F (\u0434\u0430\u0442\u0430)','monthly-dow':'\u0429\u043E\u043C\u0456\u0441\u044F\u0446\u044F (\u0434\u0435\u043D\u044C \u0442\u0438\u0436\u043D\u044F)',custom:('\u041A\u043E\u0436\u043D\u0456 '+(interval)+' \u0434\u043D\u0456\u0432')};
  const box=document.getElementById('recur-preview');
  box.style.display='block';
  box.innerHTML=('<div style="color:var(--adm);font-weight:600;margin-bottom:6px">\uD83D\uDD01 '+(labels[recur])+' \u2014 '+(allDates.length)+' \u0437\u0430\u043D\u044F\u0442\u044C:</div>')+
    allDates.slice(0,10).map(d=>('<span style="display:inline-block;background:var(--s1);border:1px solid var(--b1);border-radius:5px;padding:2px 8px;margin:2px;font-family:JetBrains Mono,monospace;font-size:11px">'+(fd(d))+'</span>')).join('')+
    (allDates.length>10?('<span style="margin-left:4px;color:var(--t3)">+'+(allDates.length-10)+' \u0449\u0435...</span>'):'');
}


function uaTab(id,el){
  document.querySelectorAll('.ua-tab').forEach(function(t){t.classList.remove('active');});
  document.querySelectorAll('.ua-panel').forEach(function(p){p.classList.remove('active');});
  el.classList.add('active');
  document.getElementById('uap-'+id).classList.add('active');
}


function toggleTutLink(){const r=document.getElementById('u-role').value;document.getElementById('u-tlink-wrap').style.display=r==='tutor'?'flex':'none';}


function toggleProfileEdit(){
  var form = document.getElementById('pr-edit-form');
  if(!form) return;
  var mt = myTutor();
  if(form.style.display === 'none'){
    if(mt){
      var set = function(id,val){ var el=document.getElementById(id); if(el) el.value=val||''; };
      set('pr-fn', mt.fn); set('pr-ln', mt.ln); set('pr-phone', mt.phone);
      set('pr-email', mt.email); set('pr-subj2', mt.subj);
      set('pr-rate', mt.rate); set('pr-bio', mt.bio);
    }
    form.style.display = 'block';
  } else {
    form.style.display = 'none';
  }
}

async function saveProfileEdit(){
  var mt = myTutor();
  if(!mt){ mkToast('Профіль репетитора не знайдено','error'); return; }
  var get = function(id){ var el=document.getElementById(id); return el?el.value.trim():''; };
  var obj = { fn:get('pr-fn'), ln:get('pr-ln'), phone:get('pr-phone'),
    email:get('pr-email'), subj:get('pr-subj2'), bio:get('pr-bio') };
  if(!obj.fn){ mkToast("Ім'я обов'язкове",'error'); return; }
  try{
    await dbUpdate('tutors', mt.id, obj);
    if(CU){ await dbUpdate('profiles', CU.id, {fn:obj.fn, ln:obj.ln});
      CU = Object.assign({}, CU, {fn:obj.fn, ln:obj.ln}); updateSBUser(); }
    mkToast('Профіль оновлено');
    document.getElementById('pr-edit-form').style.display = 'none';
    renderProfile();
  }catch(e){ mkToast('Помилка: '+(e.message||e),'error'); }
}

function buildSidebar(){
  const cfg=(S.godConfig)||{};
  const navItems=cfg.navItems?[...cfg.navItems]:[...NAV_CFG];
  const role=R();
  const allowed=userNav();
  let html='',lastSec='';
  navItems.forEach(n=>{
    const isBuiltin=allowed.includes(n.id);
    const isCustom=n.custom;
    const roleAllowed=(n.roles||[]).includes(role);
    if(!isBuiltin&&!isCustom)return;
    if(isCustom&&!roleAllowed)return;
    if(n.sec!==lastSec){html+=('<div class="nsec">'+(n.sec)+'</div>');lastSec=n.sec;}
    html+=('<div class="ni" id="ni-'+(n.id)+'" onclick="nav(\''+(n.id)+'\')"><span class="nico">'+(n.ico)+'</span>'+(n.lbl)+(n.badge?`<span class="nbadge" id="nb-s">0</span>`:'')+'</div>');
  });
  document.getElementById('sbnav').innerHTML=html;
}



var UA_PERMS = [
  {k:'students',  lbl:'\u0423\u0447\u043d\u0456',           icon:'\u25CE'},
  {k:'tutors',    lbl:'\u0420\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0438',    icon:'\u25C8'},
  {k:'lessons',   lbl:'\u0417\u0430\u043d\u044f\u0442\u0442\u044f',      icon:'\u25C9'},
  {k:'payments',  lbl:'\u041e\u043f\u043b\u0430\u0442\u0430',       icon:'\u25C8'},
  {k:'users',     lbl:'\u0410\u043a\u0430\u0443\u043d\u0442\u0438',      icon:'\u25CE'},
  {k:'settings',  lbl:'\u041d\u0430\u043b\u0430\u0448\u0442\u0443\u0432\u0430\u043d\u043d\u044f', icon:'\u25C9'},
  {k:'comms',     lbl:'\u041a\u043e\u043c\u0443\u043d\u0456\u043a\u0430\u0446\u0456\u0457', icon:'\u25CE'},
  {k:'danger',    lbl:'\u041d\u0435\u0431\u0435\u0437\u043f\u0435\u0447\u043d\u0456 \u0434\u0456\u0457',  icon:'\u26A0\uFE0F'},
  {k:'deleteAny', lbl:'\u0412\u0438\u0434\u0430\u043b\u0435\u043d\u043d\u044f',    icon:'\uD83D\uDDD1'},
  {k:'seeIncome', lbl:'\u0411\u0430\u0447\u0438\u0442\u0438 \u0434\u043e\u0445\u043e\u0434\u0438', icon:'\uD83D\uDCB0'},
];
// UA_PAGES ініціалізується з NAV_CFG при першому зверненні
function getUAPages(){ return NAV_CFG.map(function(n){ return {id:n.id, lbl:n.lbl, ico:n.ico, sec:n.sec}; }); }

function buildUAHeader(u){
  var ro=ROLES[u.role];
  var el=document.getElementById('ua-user-info');
  if(!el)return;
  el.innerHTML='';
  var wrap=document.createElement('div');
  wrap.style.cssText='display:flex;align-items:center;gap:12px;margin-bottom:10px';
  var av=document.createElement('div');
  av.className='av';
  av.style.cssText='width:44px;height:44px;flex-shrink:0;overflow:hidden;border-radius:50%';
  var _mt2=myTutor();
  if(_mt2&&_mt2.photo){
    av.innerHTML='<img src="'+_mt2.photo+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%">';
  } else {
    av.style.cssText+=(';background:'+ro.avatarBg+';font-size:17px;font-weight:700;color:#fff;display:flex;align-items:center;justify-content:center');
    av.textContent=(u.fn[0]||'')+(u.ln[0]||'');
  }
  var info=document.createElement('div');
  info.innerHTML='<div style="font-weight:700;font-size:15px">'+u.fn+' '+u.ln+'</div>'
    +'<div style="font-size:12px;color:var(--t2);margin-top:2px">'+(u.email||'')
    +' &bull; <span class="rpill '+u.role+'" style="font-size:10px;padding:1px 8px">'+ro.icon+' '+ro.label+'</span></div>';
  wrap.appendChild(av);wrap.appendChild(info);
  var hint=document.createElement('div');
  hint.style.cssText='font-size:11px;color:var(--t3);padding:8px 12px;background:var(--s2);border:1px solid var(--b1);border-radius:8px;line-height:1.5';
  hint.innerHTML='\u26A1 \u0420\u043E\u043B\u044C \u0432\u0438\u0437\u043D\u0430\u0447\u0430\u0454 <strong style="color:var(--t1)">\u0431\u0430\u0437\u043E\u0432\u0456</strong> \u043F\u0440\u0430\u0432\u0430. \u0422\u0443\u0442 \u043C\u043E\u0436\u043D\u0430 \u0434\u043E\u0434\u0430\u0442\u0438 \u0430\u0431\u043E \u0437\u043D\u044F\u0442\u0438 \u0434\u043E\u0441\u0442\u0443\u043F \u0434\u043B\u044F <strong style="color:var(--dir)">\u0446\u044C\u043E\u0433\u043E \u043A\u043E\u043D\u043A\u0440\u0435\u0442\u043D\u043E\u0433\u043E \u0430\u043A\u0430\u0443\u043D\u0442\u0443</strong>.';
  el.appendChild(wrap);el.appendChild(hint);
}



function buildUANav(u){
  var ro=ROLES[u.role];
  var roleNav=ro.nav||[];
  var up=u.perms||{};
  var hideNav=up.hideNav||[];
  var showNav=up.showNav||[];
  var el=document.getElementById('ua-nav-grid');
  if(!el)return;
  el.innerHTML='';
  getUAPages().forEach(function(pg){
    var inRole=roleNav.includes(pg.id);
    var isOn=(inRole&&!hideNav.includes(pg.id))||showNav.includes(pg.id);
    var item=document.createElement('div');
    item.className='ua-nav-item'+(isOn?' checked':'');
    var cb=document.createElement('input');cb.type='checkbox';cb.checked=isOn;
    (function(pageId,isInRole){
      cb.addEventListener('change',function(){
        uaNavChange(pageId,this.checked,isInRole);
        this.closest('.ua-nav-item').classList.toggle('checked',this.checked);
      });
    })(pg.id,inRole);
    var ico=document.createElement('span');ico.className='ua-nav-ico';ico.textContent=pg.ico;
    var info=document.createElement('div');info.style.flex='1';
    info.innerHTML='<div class="ua-nav-lbl">'+pg.lbl+'</div>'
      +'<div class="ua-nav-sec">'+pg.sec+(inRole?' \u00B7 \u0454 \u0432 \u0440\u043E\u043B\u0456':' \u00B7 \u043D\u0435 \u0432 \u0440\u043E\u043B\u0456')+'</div>';
    item.appendChild(cb);item.appendChild(ico);item.appendChild(info);
    el.appendChild(item);
  });
}



function buildUAPerms(u){
  var ro=ROLES[u.role];
  var roleCan=ro.can||{};
  var up=u.perms||{};
  var custCan=up.can||{};
  var el=document.getElementById('ua-perms-grid');
  if(!el)return;
  el.innerHTML='';
  UA_PERMS.forEach(function(p){
    var roleVal=!!(roleCan[p.k]||ro[p.k]);
    var hasOverride=p.k in custCan;
    var effectiveVal=hasOverride?custCan[p.k]:roleVal;

    var item=document.createElement('div');item.className='ua-perm-row';

    var left=document.createElement('div');
    var lbl=document.createElement('div');lbl.className='ua-perm-label';lbl.textContent=p.lbl;
    var sub=document.createElement('div');
    sub.style.cssText='font-size:10px;margin-top:2px;display:flex;align-items:center;gap:4px';
    if(hasOverride){
      var sp=document.createElement('span');sp.style.color='var(--dir)';sp.textContent='\u2699 \u0456\u043D\u0434\u0438\u0432\u0456\u0434\u0443\u0430\u043B\u044C\u043D\u043E';
      var rb=document.createElement('button');
      rb.style.cssText='background:none;border:none;color:var(--t3);cursor:pointer;font-size:10px;padding:0';
      rb.textContent='\u21BA \u0441\u043A\u0438\u043D\u0443\u0442\u0438';
      (function(key){rb.addEventListener('click',function(){uaResetPerm(key);});})(p.k);
      sub.appendChild(sp);sub.appendChild(rb);
    } else {
      sub.textContent='\u0437 \u0440\u043E\u043B\u0456: '+(roleVal?'\u2705 \u0442\u0430\u043A':'\u274C \u043D\u0456');
      sub.style.color='var(--t3)';
    }
    left.appendChild(lbl);left.appendChild(sub);

    var tgl=document.createElement('label');tgl.className='toggle';
    var cb=document.createElement('input');cb.type='checkbox';cb.checked=effectiveVal;
    (function(key,rv){
      cb.addEventListener('change',function(){
        uaPermChange(key,this.checked,rv);
        // Rebuild this item's sub label
        var subEl=this.closest('.ua-perm-row').querySelector('div > div:last-child');
        subEl.innerHTML='';subEl.style.cssText='font-size:10px;margin-top:2px;display:flex;align-items:center;gap:4px';
        var sp2=document.createElement('span');sp2.style.color='var(--dir)';sp2.textContent='\u2699 \u0456\u043D\u0434\u0438\u0432\u0456\u0434\u0443\u0430\u043B\u044C\u043D\u043E';
        var rb2=document.createElement('button');rb2.style.cssText='background:none;border:none;color:var(--t3);cursor:pointer;font-size:10px;padding:0';
        rb2.textContent='\u21BA \u0441\u043A\u0438\u043D\u0443\u0442\u0438';
        (function(k){rb2.addEventListener('click',function(){uaResetPerm(k);});})(key);
        subEl.appendChild(sp2);subEl.appendChild(rb2);
      });
    })(p.k,roleVal);
    var sl=document.createElement('span');sl.className='toggle-slider';
    tgl.appendChild(cb);tgl.appendChild(sl);
    item.appendChild(left);item.appendChild(tgl);
    el.appendChild(item);
  });
}



function buildUASummary(u){
  var el=document.getElementById('ua-summary');
  if(!el)return;
  var up=u.perms||{};
  var ro=ROLES[u.role];
  var roleCan=ro.can||{};
  var custCan=up.can||{};
  var hideNav=up.hideNav||[];
  var showNav=up.showNav||[];
  var html='';

  if(Object.keys(custCan).length){
    html+='<div style="font-weight:600;font-size:11px;color:var(--dir);margin-bottom:8px;text-transform:uppercase;letter-spacing:.5px">\u2699 \u0406\u043D\u0434\u0438\u0432\u0456\u0434\u0443\u0430\u043B\u044C\u043D\u0456 \u043F\u0440\u0430\u0432\u0430:</div>';
    html+='<div style="display:flex;flex-direction:column;gap:4px;margin-bottom:12px">';
    Object.keys(custCan).forEach(function(k){
      var def=UA_PERMS.find(function(p){return p.k===k;});
      var lbl=def?def.lbl:k;
      var rv=!!(roleCan[k]||ro[k]);
      html+='<div style="display:flex;align-items:center;gap:8px;padding:6px 10px;background:var(--s2);border-radius:7px">'
        +'<span>'+(custCan[k]?'\u2705':'\u274C')+'</span>'
        +'<span style="flex:1;font-size:12px">'+lbl+'</span>'
        +'<span style="font-size:10px;color:var(--t3)">\u0440\u043E\u043B\u044C: '+(rv?'\u2705':'\u274C')+'</span>'
        +'<button class="ua-sum-reset" data-pkey="'+k+'" style="background:none;border:none;color:var(--t3);cursor:pointer;font-size:11px;padding:2px 4px">\u21BA</button>'
        +'</div>';
    });
    html+='</div>';
  }

  if(hideNav.length||showNav.length){
    html+='<div style="font-weight:600;font-size:11px;color:var(--dir);margin-bottom:8px;text-transform:uppercase;letter-spacing:.5px">\uD83D\uDCCB \u041D\u0430\u0432\u0456\u0433\u0430\u0446\u0456\u044F \u0437\u043C\u0456\u043D\u0435\u043D\u0430:</div>';
    html+='<div style="display:flex;flex-direction:column;gap:3px">';
    var _uap=getUAPages();
    hideNav.forEach(function(p){var pg=_uap.find(function(x){return x.id===p;});html+='<div style="font-size:12px;color:var(--danger)">\u274C \u041F\u0440\u0438\u0445\u043E\u0432\u0430\u043D\u043E: '+(pg?pg.ico+' '+pg.lbl:p)+'</div>';});
    showNav.forEach(function(p){var pg=_uap.find(function(x){return x.id===p;});html+='<div style="font-size:12px;color:var(--tut)">\u2705 \u0414\u043E\u0434\u0430\u043D\u043E: '+(pg?pg.ico+' '+pg.lbl:p)+'</div>';});
    html+='</div>';
  }

  if(!html){html='<div style="color:var(--t3);font-size:12px;padding:8px 0">\u0406\u043D\u0434\u0438\u0432\u0456\u0434\u0443\u0430\u043B\u044C\u043D\u0438\u0445 \u043D\u0430\u043B\u0430\u0448\u0442\u0443\u0432\u0430\u043D\u044C \u043D\u0435\u043C\u0430\u0454 \u2014 \u0434\u0456\u044E\u0442\u044C \u043F\u0440\u0430\u0432\u0430 \u0440\u043E\u043B\u0456.</div>';}

  el.innerHTML=html;
  el.querySelectorAll('.ua-sum-reset').forEach(function(btn){
    btn.addEventListener('click',function(){uaResetPerm(this.dataset.pkey);});
  });

  var rb=document.getElementById('ua-reset-all');
  if(rb){var hasAny=!!(u.perms&&(Object.keys(u.perms.can||{}).length||(u.perms.hideNav||[]).length||(u.perms.showNav||[]).length));rb.style.display=hasAny?'flex':'none';}
}



function genRecurDates(startDate,recurType,endDate,count,interval){
  const dates=[];
  const start=new Date(startDate+'T12:00:00');
  const end=endDate?new Date(endDate+'T23:59:59'):null;
  const maxCount=count?Math.min(parseInt(count),200):104;
  let cur=new Date(start);
  while(dates.length<maxCount){
    if(end&&cur>end)break;
    dates.push(localDateStr(cur));
    const next=new Date(cur);
    if(recurType==='daily'){next.setDate(next.getDate()+1);}
    else if(recurType==='weekly'){next.setDate(next.getDate()+7);}
    else if(recurType==='biweekly'){next.setDate(next.getDate()+14);}
    else if(recurType==='monthly'){next.setMonth(next.getMonth()+1);}
    else if(recurType==='monthly-dow'){
      const dow=start.getDay();const weekNum=Math.floor((start.getDate()-1)/7);
      next.setMonth(next.getMonth()+1);next.setDate(1);
      while(next.getDay()!==dow)next.setDate(next.getDate()+1);
      next.setDate(next.getDate()+weekNum*7);
    }
    else if(recurType==='custom'){next.setDate(next.getDate()+Math.max(1,parseInt(interval)||7));}
    else break;
    if(end&&next>end)break;
    cur=next;
  }
  return dates;
}



function renderBranches(){
  var el=document.getElementById('branch-list');
  if(!el)return;
  var html='';
  (S.branches||[]).forEach(function(b){
    var bid=b.id;
    var isActive=S.currentBranchId===bid;
    var reqLines='';
    if(b.pay_recipient) reqLines+='<div style="font-size:11px;color:var(--t2)">\uD83D\uDCB3 '+b.pay_recipient+'</div>';
    if(b.pay_card)      reqLines+='<div style="font-size:11px;color:var(--t2)">'+b.pay_card+(b.pay_bank?' \u00B7 '+b.pay_bank:'')+'</div>';
    html+='<div class="ms" style="flex-direction:column;align-items:stretch;gap:6px;padding:12px 0;border-bottom:1px solid var(--b1)">'
      +'<div style="display:flex;align-items:center;gap:8px">'
        +'<div style="flex:1">'
          +'<div style="font-weight:700;font-size:13px">'+(isActive?'\u2705 ':'')+b.name+'</div>'
          +(b.address?'<div style="font-size:11px;color:var(--t2)">\uD83D\uDCCD '+b.address+'</div>':'')
          +(b.phone?'<div style="font-size:11px;color:var(--t2)">\uD83D\uDCDE '+b.phone+'</div>':'')
          +(b.email?'<div style="font-size:11px;color:var(--t2)">\u2709\uFE0F '+b.email+'</div>':'')
          +(reqLines?'<div style="margin-top:4px">'+reqLines+'</div>':'')
        +'</div>'
        +'<div style="display:flex;gap:6px;flex-shrink:0">'
          +'<button class="btn btn-g btn-sm" onclick="editBranch(this.dataset.id)" data-id="'+bid+'">\u270F\uFE0F</button>'
          +(S.branches.length>1?'<button class="btn btn-sm btn-d" onclick="delBranch(this.dataset.id)" data-id="'+bid+'">\uD83D\uDDD1</button>':'')
        +'</div>'
      +'</div>'
    +'</div>';
  });
  el.innerHTML=html||'<div style="font-size:12px;color:var(--t3);padding:8px 0">\u041D\u0435\u043C\u0430\u0454 \u0444\u0456\u043B\u0456\u0439</div>';
}



function renderPricingRules(){
  var el = document.getElementById('pricing-rules-list');
  if(!el) return;
  var rules = S.pricingRules || [];
  if(!rules.length){
    el.innerHTML = '<div style="font-size:12px;color:var(--t3);padding:8px 0">\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432\u0438\u043B. \u0414\u043E\u0434\u0430\u0439\u0442\u0435 \u043F\u0435\u0440\u0448\u0435 \u043F\u0440\u0430\u0432\u0438\u043B\u043E \u043D\u0438\u0436\u0447\u0435.</div>';
    return;
  }
  el.innerHTML = rules.map(function(r){
    var tags = [];
    if(r.subjectMatch) tags.push('\uD83D\uDCDA '+r.subjectMatch);
    if(r.tutorId){ var t=(S.tutors||[]).find(x=>x.id===r.tutorId); if(t) tags.push('\uD83D\uDC64 '+t.fn+' '+t.ln); }
    if(r.gradeMatch) tags.push('\uD83C\uDFEB '+r.gradeMatch+' \u043A\u043B.');
    if(r.durMin) tags.push('\u23F1 \u0432\u0456\u0434 '+r.durMin+' \u0445\u0432');
    return '<div class="ms" style="align-items:center">'+
      '<div style="flex:1">'+
        '<div style="font-weight:600;font-size:13px">'+r.name+' \u2014 <span style="color:var(--tut)">'+r.price+' \u20B4</span></div>'+
        '<div style="font-size:11px;color:var(--t2);margin-top:2px">'+(tags.length?tags.join(' \u00B7 '):'\u0417\u0430\u0441\u0442\u043E\u0441\u043E\u0432\u0443\u0454\u0442\u044C\u0441\u044F \u0434\u043E \u0432\u0441\u0456\u0445')+'</div>'+
      '</div>'+
      '<div style="display:flex;gap:6px">'+
        '<button class="btn btn-g btn-sm" onclick="editPriceRule(r.id)">\u270F\uFE0F</button>'+
        '<button class="btn btn-sm btn-d" onclick="delPriceRule(r.id)">\uD83D\uDDD1</button>'+
      '</div>'+
    '</div>';
  }).join('');
}



function renderProfile(){
  const mt = myTutor();
  var _pi = document.getElementById('pr-info');
  if(!_pi) return;

  if(mt){
    // Photo: show real photo or initials avatar
    var photoHtml = mt.photo
      ? '<img src="'+mt.photo+'" style="width:56px;height:56px;object-fit:cover;border-radius:50%">'
      : '<div style="width:56px;height:56px;border-radius:50%;background:var(--adm2);display:flex;align-items:center;justify-content:center;font-size:22px;font-weight:700;color:#fff;font-family:Syne,sans-serif">'+(mt.fn||'?')[0]+(mt.ln||'')[0]+'</div>';

    var doneH = Math.round(myLessons().filter(function(l){
      return l.status==='done'||l.status==='completed'||l.status==='makeup';
    }).reduce(function(s,l){ return s+(parseFloat(l.dur)||60)/60; },0)*10)/10;

    _pi.innerHTML =
      '<div style="display:flex;align-items:center;gap:14px;margin-bottom:16px">'+photoHtml
      +'<div><div style="font-size:17px;font-weight:700;font-family:Syne,sans-serif">'+mt.fn+' '+mt.ln+'</div>'
      +'<div style="font-size:12px;color:var(--t2);margin-top:2px">'+(mt.subj||'—')+'</div></div></div>'
      +'<div class="ms"><span class="msl">Телефон</span><span class="msv" style="font-family:inherit">'+(mt.phone||'—')+'</span></div>'
      +'<div class="ms"><span class="msl">Email</span><span class="msv" style="font-family:inherit">'+(mt.email||'—')+'</span></div>'
      +'<div class="ms"><span class="msl">Занять проведено</span><span class="msv">'+doneH+'</span></div>'
      +(mt.bio?'<div style="margin-top:12px;padding:10px;background:var(--s2);border-radius:8px;font-size:12px;color:var(--t2)">'+mt.bio+'</div>':'');
  } else {
    _pi.innerHTML = '<div class="empty"><div class="ei">🔗</div>Ваш акаунт не прив\'язаний до профілю викладача</div>';
  }

  // Pre-fill edit form fields
  if(mt){
    var setV = function(id,v){ var el=document.getElementById(id); if(el) el.value=v||''; };
    setV('pr-fn', mt.fn); setV('pr-ln', mt.ln); setV('pr-phone', mt.phone);
    setV('pr-email', mt.email); setV('pr-subj2', mt.subj); setV('pr-bio', mt.bio);
    // Photo preview
    var prev = document.getElementById('pr-photo-preview');
    if(prev && mt.photo) prev.innerHTML = '<img src="'+mt.photo+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%">';
  }
}
function renderAllAnalytics(){
  renderAnalytics();
  renderReports();
}

function renderReports(){
  // === Діапазон дат ===
  var range = (document.getElementById('rc-range')||{value:'month'}).value;
  var now = new Date();
  var fromDate = new Date(now);

  if(range === 'week'){
    var day = now.getDay()||7;
    fromDate = new Date(now); fromDate.setDate(now.getDate() - day + 1);
    fromDate.setHours(0,0,0,0);
  } else if(range === '2week'){
    fromDate.setDate(now.getDate() - 14);
  } else if(range === '4week'){
    fromDate.setDate(now.getDate() - 28);
  } else if(range === 'month'){
    fromDate = new Date(now.getFullYear(), now.getMonth(), 1);
  } else if(range === '3month'){
    fromDate = new Date(now.getFullYear(), now.getMonth() - 3, 1);
  } else if(range === '6month'){
    fromDate = new Date(now.getFullYear(), now.getMonth() - 6, 1);
  } else if(range === 'year'){
    fromDate = new Date(now.getFullYear(), 0, 1);
  } else {
    fromDate = new Date(0);
  }
  var fromStr = localDateStr(fromDate);
  var toStr   = localDateStr(now);

  // Підпис діапазону
  var lbl = document.getElementById('rc-range-lbl');
  if(lbl && range !== 'all'){
    lbl.textContent = fromDate.toLocaleDateString('uk-UA',{day:'numeric',month:'short'})
      + ' — ' + now.toLocaleDateString('uk-UA',{day:'numeric',month:'short',year:'numeric'});
  } else if(lbl){ lbl.textContent = ''; }

  // Ховаємо доходи для адміна
  var incCard = document.getElementById('rc-income-card');
  if(incCard) incCard.style.display = (R()==='admin') ? 'none' : '';

  // === Фільтрація занять ===
  var lessons = (S.lessons||[]).filter(function(l){
    return l.date >= fromStr && l.date <= toStr && l.status !== 'cancelled';
  });

  function hrs(arr){ return Math.round(arr.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10; }

  // === Доходи по місяцях (₴) — всі проведені заняття року, незалежно від фільтра ===
  var months = ['Січ','Лют','Бер','Кві','Тра','Чер','Лип','Сер','Вер','Жов','Лис','Гру'];
  var md = new Array(12).fill(0);
  var curYear = new Date().getFullYear();
  var validInc = ['done','completed','makeup'];
  myLessons().filter(function(l){
    if(!l.date) return false;
    if(validInc.indexOf(l.status)<0) return false;
    return new Date(l.date).getFullYear() === curYear;
  }).forEach(function(l){
    var mon = new Date(l.date).getMonth();
    var cost = 0;
    if(l.price!=null && l.price!=='' && !isNaN(parseFloat(l.price))){
      cost = lessonTotal(l);
    } else {
      // фолбек: поле amount з платежу відповідного заняття
      var pay = (S.payments||[]).find(function(p){return p.lessonId===l.id||p.lesson_id===l.id;});
      if(pay) cost = parseFloat(pay.amount)||0;
    }
    md[mon] += cost;
  });
  md = md.map(function(v){return Math.round(v);});
  var maxI = Math.max.apply(null, md.concat([1]));
  var incEl = document.getElementById('rc-income');
  if(incEl) incEl.innerHTML = md.map(function(v,i){
    var lbl = v>0 ? (v>=1000 ? (Math.round(v/100)/10)+'к' : v) : '';
    return '<div class="bw"><div class="bar" style="height:'+(v/maxI*100)+'%;background:linear-gradient(180deg,var(--adm),var(--adm2))">'
      +(lbl?'<div style="position:absolute;top:-16px;left:50%;transform:translateX(-50%);font-size:8px;color:var(--t2);white-space:nowrap;font-family:JetBrains Mono,monospace">'+lbl+'₴</div>':'')
      +'</div><div class="blbl">'+months[i]+'</div></div>';
  }).join('');
  // === Заняття по предметах (год) ===
  var sc = {};
  lessons.forEach(function(l){
    if(!l.subject) return;
    sc[l.subject] = (sc[l.subject]||0) + (parseFloat(l.dur)||60)/60;
  });
  var totalSubjH = hrs(lessons) || 1;
  var cols = ['var(--adm)','var(--tut)','var(--dir)','var(--god)','#a78bfa','#0ea5e9','#f59e0b','#ec4899'];
  var subjEl = document.getElementById('rc-subj');
  if(subjEl) subjEl.innerHTML = Object.entries(sc).sort(function(a,b){return b[1]-a[1];})
    .map(function(e,i){
      var s=e[0], h=Math.round(e[1]*10)/10, pct=Math.round(e[1]/totalSubjH*100);
      return '<div style="margin-bottom:10px">'
        +'<div style="display:flex;justify-content:space-between;margin-bottom:3px">'
        +'<span style="font-size:12px">'+s+'</span>'
        +'<span style="font-size:11px;color:var(--t2);font-family:JetBrains Mono,monospace">'+h+'\u0433 ('+pct+'%)</span></div>'
        +'<div class="pb"><div class="pf" style="width:'+(e[1]/totalSubjH*100)+'%;background:'+cols[i%cols.length]+'"></div></div></div>';
    }).join('') || '<div class="empty"><div class="ei">\uD83D\uDCDA</div>\u041d\u0435\u043c\u0430\u0454 \u0434\u0430\u043d\u0438\u0445</div>';

  // === Завантаженість репетиторів (год) ===
  var tl = {};
  lessons.forEach(function(l){
    var tid = l.tutorId||l.tutor_id;
    if(tid) tl[tid] = (tl[tid]||0) + (parseFloat(l.dur)||60)/60;
  });
  // Включаємо ВСІХ репетиторів, навіть з 0 годин у періоді
  var entries = (S.tutors||[]).map(function(t){
    return {id:t.id, h:Math.round((tl[t.id]||0)*10)/10};
  }).sort(function(a,b){return b.h-a.h;});
  var maxT = entries.length ? Math.max.apply(null, entries.map(function(e){return e.h;}).concat([1])) : 1;
  var tloadEl = document.getElementById('rc-tload');
  if(tloadEl) tloadEl.innerHTML = entries.map(function(e){
    return '<div style="margin-bottom:10px">'
      +'<div style="display:flex;justify-content:space-between;margin-bottom:3px">'
      +'<span style="font-size:12px">'+tn(e.id)+'</span>'
      +'<span style="font-size:11px;color:var(--t2);font-family:JetBrains Mono,monospace">'+e.h+'\u0433</span></div>'
      +'<div class="pb"><div class="pf" style="width:'+(e.h/maxT*100)+'%;background:var(--adm)"></div></div></div>';
  }).join('') || '<div class="empty"><div class="ei">\uD83E\uDDD1\u200D\uD83C\uDFEB</div>\u041d\u0435\u043c\u0430\u0454 \u0434\u0430\u043d\u0438\u0445</div>';

  // === Загальна статистика ===
  var doneL  = lessons.filter(function(l){return l.status==='done'||l.status==='completed'||l.status==='makeup';});
  var missedL= uncoveredMissedFilter(lessons);
  var doneH  = hrs(doneL);
  var missedH= hrs(missedL);
  var totalH = hrs(lessons);
  var pct    = totalH>0 ? Math.round(doneH/totalH*100) : 0;
  var totalInc = (S.payments||[]).filter(function(p){
    return p.status==='paid' && p.date>=fromStr && p.date<=toStr;
  }).reduce(function(a,p){return a+p.amount;},0);
  var showIncome = P().seeIncome && R()!=='tutor' && R()!=='admin';

  var genEl = document.getElementById('rc-gen');
  if(genEl) genEl.innerHTML =
    '<div class="ms"><span class="msl">\u0412\u0441\u044c\u043e\u0433\u043e \u0443\u0447\u043d\u0456\u0432</span><span class="msv">'+(S.students.length)+'</span></div>'
    +'<div class="ms"><span class="msl">\u0410\u043a\u0442\u0438\u0432\u043d\u0438\u0445 \u0443\u0447\u043d\u0456\u0432</span><span class="msv">'+(S.students.filter(function(s){return s.status==='active';}).length)+'</span></div>'
    +'<div class="ms"><span class="msl">\u041f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u043e (\u0433\u043e\u0434)</span><span class="msv" style="color:var(--tut)">'+doneH+'\u0433</span></div>'
    +'<div class="ms"><span class="msl">\u041f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u043e (\u0433\u043e\u0434)</span><span class="msv" style="color:var(--danger)">'+missedH+'\u0433</span></div>'
    +'<div class="ms"><span class="msl">\u0412\u0438\u043a\u043e\u043d\u0430\u043d\u043d\u044f \u043f\u043b\u0430\u043d\u0443</span><span class="msv" style="color:'+(pct>=80?'var(--tut)':pct>=50?'var(--dir)':'var(--danger)')+'">'+pct+'%</span></div>'
    +(showIncome
      ?'<div class="ms"><span class="msl">\u0414\u043e\u0445\u0456\u0434 \u0437\u0430 \u043f\u0435\u0440\u0456\u043e\u0434</span><span class="msv" style="color:var(--tut)">'+(totalInc.toLocaleString('uk-UA'))+'\u20b4</span></div>'
      :'')
    +'<div class="ms"><span class="msl">\u0412\u0438\u043a\u043b\u0430\u0434\u0430\u0447\u0456\u0432</span><span class="msv">'+(S.tutors.length)+'</span></div>';
}


function renderSch(){
  const view = S.schView || 'week';
  // Update UI
  const btnW = document.getElementById('sch-btn-week');
  const btnD = document.getElementById('sch-btn-day');
  const tf   = document.getElementById('sch-tutor-filter');
  if(btnW) btnW.classList.toggle('active-view', view==='week');
  if(btnD) btnD.classList.toggle('active-view', view==='day');
  // Фільтр по репетиторах доступний адмінам/директорам/богу в обох режимах (тиждень і день)
  if(tf){
    tf.style.display = R()!=='tutor' ? 'block' : 'none';
    const prev = tf.value;
    tf.innerHTML = '<option value="">\u0412\u0441\u0456 \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438</option>' +
      (S.tutors||[]).slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');}).map(t=>('<option value="'+(t.id)+'">'+(t.fn)+' '+(t.ln)+'</option>')).join('');
    tf.value = prev;
  }
  // Update prev/next labels
  const prevBtn = document.getElementById('sch-prev');
  const nextBtn = document.getElementById('sch-next');
  if(prevBtn) prevBtn.textContent = view==='day' ? '\u2190 \u0412\u0447\u043E\u0440\u0430' : '\u2190 \u041F\u043E\u043F\u0435\u0440\u0435\u0434\u043D\u0456\u0439';
  if(nextBtn) nextBtn.textContent = view==='day' ? '\u0417\u0430\u0432\u0442\u0440\u0430 \u2192' : '\u041D\u0430\u0441\u0442\u0443\u043F\u043D\u0438\u0439 \u2192';

  if(view === 'week') renderSchWeek();
  else                renderSchDay();

  var _at=document.getElementById('avail-toggle-btn');
  var _att=(typeof availTargetTutor==='function')?availTargetTutor():null;
  var _fill=document.getElementById('avail-fill-btn'), _clr=document.getElementById('avail-clear-btn'), _hint=document.getElementById('avail-hint');
  if(_at){
    if(view!=='week' && S._availMode){ S._availMode=false; }
    _at.style.display = view==='week' ? 'inline-flex' : 'none';
    _at.classList.toggle('btn-p', !!S._availMode);
    _at.textContent = S._availMode ? '\u2713 \u0413\u043E\u0442\u043E\u0432\u043E' : '\uD83D\uDD52 \u0420\u043E\u0431\u043E\u0447\u0456 \u0433\u043E\u0434\u0438\u043D\u0438';
    var _editing = S._availMode && _att;
    if(_fill) _fill.style.display = _editing ? 'inline-flex':'none';
    if(_clr)  _clr.style.display  = _editing ? 'inline-flex':'none';
    if(_hint){
      _hint.style.display = S._availMode ? 'inline':'none';
      _hint.textContent = _att ? ('\u041A\u043B\u0456\u043A\u0430\u0439\u0442\u0435 \u043F\u043E \u043A\u043B\u0456\u0442\u0438\u043D\u043A\u0430\u0445: '+_att.fn+' '+_att.ln) : '\u041E\u0431\u0435\u0440\u0456\u0442\u044C \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0430 \u0443 \u0444\u0456\u043B\u044C\u0442\u0440\u0456';
    }
  }
}



function renderSchDay(){
  const now    = new Date();
  const offset = S.dayOffset || 0;
  const day    = new Date(now);
  day.setDate(now.getDate() + offset);
  day.setHours(0,0,0,0);
  const ds = localDateStr(day);
  const dnames = ['\u041D\u0435\u0434\u0456\u043B\u044F','\u041F\u043E\u043D\u0435\u0434\u0456\u043B\u043E\u043A','\u0412\u0456\u0432\u0442\u043E\u0440\u043E\u043A','\u0421\u0435\u0440\u0435\u0434\u0430','\u0427\u0435\u0442\u0432\u0435\u0440','\u041F\'\u044F\u0442\u043D\u0438\u0446\u044F','\u0421\u0443\u0431\u043E\u0442\u0430'];
  document.getElementById('wklbl').textContent =
    dnames[day.getDay()] + ', ' + day.toLocaleDateString('uk-UA',{day:'numeric',month:'long'});

  // Populate tutor filter
  const tf = document.getElementById('sch-tutor-filter');
  if(tf){
    tf.style.display = R()==='tutor' ? 'none' : '';
    const prev = tf.value;
    tf.innerHTML = '<option value="">\u0412\u0441\u0456 \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438</option>' +
      (S.tutors||[]).map(t=>('<option value="'+(t.id)+'">'+(t.fn)+' '+(t.ln)+'</option>')).join('');
    tf.value = prev;
  }

  // Determine tutors to show
  const filterTutor = tf ? tf.value : '';
  let tutors = P().seeAll ? (S.tutors||[]) : (S.tutors||[]).filter(function(t){
    return t.accId===CU?.id || t.acc_uid===CU?.id;
  });
  if(filterTutor) tutors = tutors.filter(t=>t.id===filterTutor);


  const START_H = 8, END_H = 21;
  const ROW_H = 48;
  const totalHrs = END_H - START_H;

  const _schStat = (document.getElementById('sch-status-filter')||{value:''}).value;
  const ml = myLessons().filter(function(l){
    if(l.status==='cancelled') return false;
    if(_schStat){
      if(_schStat==='planned') return l.status==='planned'||l.status==='scheduled'||!l.status;
      if(_schStat==='completed') return l.status==='done'||l.status==='completed'||l.status==='makeup';
      return l.status===_schStat;
    }
    return true;
  });

  const cols = tutors.length || 1;
  let html = '<div class="schh" style="background:var(--s1)">\u0427\u0430\u0441</div>';
  if(tutors.length === 0){
    html += '<div class="schh">\u041D\u0435\u043C\u0430\u0454 \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0456\u0432</div>';
  } else {
    tutors.forEach(function(t){
      html += '<div class="schh"><div style="font-weight:700;font-size:12px">'+t.fn+'</div><div style="font-size:10px;color:var(--t2)">'+t.ln+'</div></div>';
    });
  }

  // Time column
  html += '<div style="position:relative;height:'+(totalHrs*ROW_H)+'px;background:var(--s2);border-right:1px solid var(--b1)">';
  for(var h=START_H;h<END_H;h++){
    html += '<div style="position:absolute;top:'+((h-START_H)*ROW_H)+'px;height:'+ROW_H+'px;width:100%;display:flex;align-items:flex-start;padding-top:3px;justify-content:center;font-size:10px;color:var(--t3);font-family:JetBrains Mono,monospace;box-sizing:border-box;border-top:1px solid var(--b1)">'+String(h).padStart(2,'0')+':00</div>';
  }
  html += '</div>';

  if(tutors.length === 0){
    html += '<div style="position:relative;height:'+(totalHrs*ROW_H)+'px"></div>';
  } else {
    tutors.forEach(function(t){
      const dayLessons = ml.filter(function(l){
        return l.date===ds && (l.tutorId===t.id || l.tutor_id===t.id);
      });
      html += '<div style="position:relative;height:'+(totalHrs*ROW_H)+'px;border-right:1px solid var(--b1)">';
      for(var hh=START_H;hh<END_H;hh++){
        var slotOnclick = 'openLessM(null,\''+ds+'\',\''+String(hh).padStart(2,'0')+':00\')';
        html += '<div onclick="'+slotOnclick+'" style="position:absolute;top:'+((hh-START_H)*ROW_H)+'px;left:0;right:0;height:'+ROW_H+'px;border-top:1px solid var(--b1);box-sizing:border-box;cursor:pointer"></div>';
      }
      var laneMapD=schAssignLanes(dayLessons);
      dayLessons.forEach(function(l){
        const parts = (l.time||'08:00').split(':');
        const lh = parseInt(parts[0])||8;
        const lm = parseInt(parts[1])||0;
        const dur = parseFloat(l.dur)||60;
        const topPx = (lh-START_H)*ROW_H + (lm/60)*ROW_H;
        const heightPx = Math.max((dur/60)*ROW_H, 18);
        if(lh<START_H||lh>=END_H) return;
        var _isCov = l.status==='missed' && isCoveredMissed(l);
        var _unHrs = l.status==='missed'?uncoveredMissedHours(l):0;
        var _isPartial = !_isCov && l.status==='missed' && _unHrs*60 < (parseFloat(l.dur)||60);
        var ecl = _isCov ? 'ec-covered'
          : _isPartial ? 'ec-partial'
          : l.status==='missed'  ? 'ec-miss'
          : l.status==='makeup'  ? 'ec-make'
          : l.status==='makeup_planned' ? 'ec-makeplan'
          : (l.status==='completed'||l.status==='done') ? 'ec-done'
          : 'ec-plan';
        var liD=laneMapD.get(l.id)||{lane:0,count:1};
        var wPctD=100/liD.count;
        var posCssD='left:calc('+(liD.lane*wPctD)+'% + 2px);width:calc('+wPctD+'% - 4px);';
        var canDel = can('lessons');
        html += '<div class="sche '+ecl+'" style="position:absolute;top:'+topPx+'px;'+posCssD+'height:'+(heightPx-2)+'px;box-sizing:border-box;overflow:hidden;z-index:2;cursor:pointer"'
          +' onclick="event.stopPropagation();showQuickPopup(\''+l.id+'\',event.clientX,event.clientY)">'
          +'<div style="font-weight:700;font-size:10px;line-height:1.2">'+(l.recurId?'\uD83D\uDD01 ':'')+snShort(l.studentId||l.student_id)+'</div>'
          +(heightPx>28?'<div style="font-weight:400;opacity:.8;font-size:9px">'+(l.subject||'')+'</div>':'')
          +(heightPx>40?'<div style="opacity:.6;font-size:9px">'+(l.time||'')+(dur>=60?' \u00B7 '+Math.floor(dur/60)+'\u0433'+(dur%60?dur%60+'\u0445\u0432':''):'\u00B7 '+dur+'\u0445\u0432')+'</div>':'')
          +(_isPartial&&heightPx>52?'<div style="font-size:9px;font-weight:700">\u26A0 \u0437\u0430\u043B\u0438\u0448\u0438\u043B\u043E\u0441\u044C '+_unHrs+'\u0433</div>':'')
          +(canDel?'<span onclick="event.stopPropagation();delLesson(\''+l.id+'\')" style="position:absolute;top:2px;right:3px;font-size:10px;opacity:.6;cursor:pointer;line-height:1" title="\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438">\u2715</span>':'')
          +'</div>';
      });
      html += '</div>';
    });
  }

  const g = document.getElementById('schg');
  // При багатьох репетиторах даємо колонкам фіксовану мін. ширину і вмикаємо горизонтальний скрол,
  // інакше 1fr стискає всіх у ширину екрана і підписи/уроки не влазять.
  var colW = cols>=4 ? '150px' : '1fr';
  g.style.gridTemplateColumns = '52px repeat('+cols+','+colW+')';
  g.style.gridTemplateRows    = 'auto '+(totalHrs*ROW_H)+'px';
  g.style.minWidth = cols>=4 ? (52 + cols*150) + 'px' : '';
  g.innerHTML = html;
}




// ══════════ ДОСТУПНІСТЬ РЕПЕТИТОРІВ (робочі години) ══════════
function tutorAvail(t){
  if(!t) return null;
  var a=t.availability;
  if(a==null||a==='') return null;
  try{ a=(typeof a==='string')?JSON.parse(a):a; }catch(e){ return null; }
  if(!a||typeof a!=='object') return null;
  var any=Object.keys(a).some(function(k){return Array.isArray(a[k])&&a[k].length;});
  return any?a:null;
}
function isWorkingHour(t, dowMon1, hour){
  var a=tutorAvail(t);
  if(!a) return true;
  var arr=a[String(dowMon1)]||[];
  return arr.indexOf(hour)>=0;
}
function dowMon1FromDate(ds){
  var d=new Date(ds+'T00:00:00'); var g=d.getDay(); return g===0?7:g;
}
function availTargetTutor(){
  var _sf=document.getElementById('sch-tutor-filter');
  var _schTut=_sf?_sf.value:'';
  if(R()==='tutor'){ var mt=myTutor(); return mt||null; }
  if(_schTut) return (S.tutors||[]).find(function(t){return t.id===_schTut;})||null;
  return null;
}
function toggleAvailMode(){
  if(!S._availMode){
    var tgt=availTargetTutor();
    if(!tgt){ mkToast('Спочатку оберіть репетитора у фільтрі','error'); return; }
  }
  S._availMode=!S._availMode;
  renderSch();
}
async function availToggleCell(dowMon1, hour){
  var t=availTargetTutor();
  if(!t) return;
  var a=tutorAvail(t)||{};
  a=JSON.parse(JSON.stringify(a));
  var key=String(dowMon1);
  a[key]=a[key]||[];
  var idx=a[key].indexOf(hour);
  if(idx>=0) a[key].splice(idx,1); else a[key].push(hour);
  a[key].sort(function(x,y){return x-y;});
  t.availability=a;
  renderSch();
  try{ await dbUpdate('tutors', t.id, {availability:JSON.stringify(a)}); }catch(e){}
}
function openAvailFillM(){
  var t=availTargetTutor();
  if(!t){ mkToast('Оберіть репетитора','error'); return; }
  openM('mo-avail-fill');
}
async function availApplyFill(){
  var t=availTargetTutor();
  if(!t){ mkToast('Оберіть репетитора','error'); return; }
  var days=Array.from(document.querySelectorAll('#af-days input:checked')).map(function(c){return c.value;});
  if(!days.length){ mkToast('Оберіть хоча б один день','error'); return; }
  var from=parseInt((document.getElementById('af-from')||{value:'9'}).value);
  var to=parseInt((document.getElementById('af-to')||{value:'18'}).value);
  if(isNaN(from)||isNaN(to)||from>=to){ mkToast('Некоректний діапазон годин','error'); return; }
  var replace=(document.getElementById('af-replace')||{checked:true}).checked;
  var a=replace?{}:JSON.parse(JSON.stringify(tutorAvail(t)||{}));
  days.forEach(function(dw){
    var arr=replace?[]:(a[dw]||[]);
    for(var h=from;h<to;h++){ if(arr.indexOf(h)<0) arr.push(h); }
    arr.sort(function(x,y){return x-y;});
    a[dw]=arr;
  });
  t.availability=a;
  closeM('mo-avail-fill');
  renderSch();
  mkToast('Робочі години оновлено');
  try{ await dbUpdate('tutors', t.id, {availability:JSON.stringify(a)}); }catch(e){}
}
window.openAvailFillM=openAvailFillM;
window.availApplyFill=availApplyFill;
async function availClear(){
  var t=availTargetTutor();
  if(!t){ mkToast('Оберіть репетитора','error'); return; }
  if(!confirm('Очистити всі робочі години?')) return;
  t.availability=null; renderSch();
  try{ await dbUpdate('tutors', t.id, {availability:null}); }catch(e){}
}
window.toggleAvailMode=toggleAvailMode;
window.availToggleCell=availToggleCell;
window.availClear=availClear;

function renderSchWeek(){
  const now=new Date(), sow=new Date(now);
  const dy=now.getDay()===0?6:now.getDay()-1;
  sow.setDate(now.getDate()-dy+S.weekOffset*7); sow.setHours(0,0,0,0);
  const days=Array.from({length:7},(_,i)=>{const d=new Date(sow);d.setDate(sow.getDate()+i);return d;});
  const dnames=['Пн','Вт','Ср','Чт','Пт','Сб','Нд'];
  document.getElementById('wklbl').textContent=days[0].toLocaleDateString('uk-UA',{day:'numeric',month:'short'})+' — '+days[6].toLocaleDateString('uk-UA',{day:'numeric',month:'short'});

  const ROW_H = 48; // px per hour
  const _schStat=(document.getElementById('sch-status-filter')||{value:''}).value;
  const _schTut=(document.getElementById('sch-tutor-filter')||{value:''}).value;
  const ml=myLessons().filter(function(l){
    if(l.status==='cancelled') return false;
    if(_schTut && (l.tutorId||l.tutor_id)!==_schTut) return false;
    if(_schStat){
      if(_schStat==='planned') return l.status==='planned'||l.status==='scheduled'||!l.status;
      if(_schStat==='completed') return l.status==='done'||l.status==='completed'||l.status==='makeup';
      return l.status===_schStat;
    }
    return true;
  });

  const START_H = 8, END_H = 21;
  const totalHrs = END_H - START_H;

  // Build header
  let html = '<div class="schh" style="background:var(--s1)">Час</div>';
  days.forEach(function(d,i){
    const today=d.toDateString()===now.toDateString();
    html+='<div class="schh" style="'+(today?'color:var(--adm);border-bottom:2px solid var(--adm)':'')+'">'
      +dnames[i]+'<br><span style="font-size:9px;font-weight:400;color:var(--t3);font-family:JetBrains Mono,monospace">'
      +d.getDate()+'.'+String(d.getMonth()+1).padStart(2,'0')+'</span></div>';
  });

  // Time column + day columns using position:relative inside fixed-height container
  // Time slots column
  html += '<div style="position:relative;height:'+(totalHrs*ROW_H)+'px;background:var(--s2);border-right:1px solid var(--b1)">';
  for(var h=START_H;h<END_H;h++){
    html+='<div style="position:absolute;top:'+(( h-START_H)*ROW_H)+'px;height:'+ROW_H+'px;width:100%;display:flex;align-items:flex-start;padding-top:3px;justify-content:center;font-size:10px;color:var(--t3);font-family:JetBrains Mono,monospace;box-sizing:border-box;border-top:1px solid var(--b1)">'
      +String(h).padStart(2,'0')+':00</div>';
  }
  html += '</div>';

  // Day columns
  var _availTut = availTargetTutor();
  var _availMode = S._availMode && !!_availTut;
  days.forEach(function(d){
    const ds=d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');
    const dow=dowMon1FromDate(ds);
    const dayLessons=ml.filter(function(l){return l.date===ds;});
    html+='<div style="position:relative;height:'+(totalHrs*ROW_H)+'px;border-right:1px solid var(--b1)">';
    // Hour grid lines
    for(var h=START_H;h<END_H;h++){
      var _working = _availTut ? isWorkingHour(_availTut, dow, h) : true;
      var _offBg = (_availTut && !_working)
        ? 'background:repeating-linear-gradient(45deg,rgba(148,163,184,.14),rgba(148,163,184,.14) 6px,rgba(148,163,184,.05) 6px,rgba(148,163,184,.05) 12px);'
        : (_availTut && _working ? 'background:rgba(34,197,94,.07);' : '');
      var _cellClick = _availMode
        ? ('availToggleCell('+dow+','+h+')')
        : ('openLessM(null,\''+ds+'\',\''+String(h).padStart(2,'0')+':00\')');
      html+='<div class="'+(_availMode?'avail-cell':'')+'" onclick="event.stopPropagation();'+_cellClick+'" style="position:absolute;top:'+((h-START_H)*ROW_H)+'px;left:0;right:0;height:'+ROW_H+'px;border-top:1px solid var(--b1);box-sizing:border-box;cursor:pointer;'+_offBg+'">'
        +(_availMode&&_working?'<span style="position:absolute;top:2px;left:3px;font-size:9px;color:#16a34a;opacity:.7">\u2713</span>':'')
        +'</div>';
    }
    // Lessons — уроки, що перетинаються в часі, ділять клітинку по вертикалі
    var laneMap=schAssignLanes(dayLessons);
    dayLessons.forEach(function(l){
      const parts=(l.time||'08:00').split(':');
      const lh=parseInt(parts[0])||8;
      const lm=parseInt(parts[1])||0;
      const dur=parseFloat(l.dur)||60;
      const topPx=(lh-START_H)*ROW_H + (lm/60)*ROW_H;
      const heightPx=Math.max((dur/60)*ROW_H, 18);
      if(lh<START_H||lh>=END_H) return;
      var _isCov=l.status==='missed'&&isCoveredMissed(l);
      var _unHrs=l.status==='missed'?uncoveredMissedHours(l):0;
      var _isPartial=!_isCov&&l.status==='missed'&&_unHrs*60<(parseFloat(l.dur)||60);
      var ecl=_isCov?'ec-covered'
        :_isPartial?'ec-partial'
        :l.status==='missed'?'ec-miss'
        :l.status==='makeup'?'ec-make'
        :l.status==='makeup_planned'?'ec-makeplan'
        :(l.status==='completed'||l.status==='done')?'ec-done'
        :'ec-plan';
      var li=laneMap.get(l.id)||{lane:0,count:1};
      var wPct=100/li.count;
      var posCss='left:calc('+(li.lane*wPct)+'% + 2px);width:calc('+wPct+'% - 4px);';
      var canDel=can('lessons');
      html+='<div class="sche '+ecl+'" style="position:absolute;top:'+topPx+'px;'+posCss+'height:'+(heightPx-2)+'px;box-sizing:border-box;overflow:hidden;z-index:2;cursor:pointer"'
        +' onclick="event.stopPropagation();showQuickPopup(\''+l.id+'\',event.clientX,event.clientY)">'
        +'<div style="font-weight:700;font-size:10px;line-height:1.2">'+(l.recurId?'🔁 ':'')+snShort(l.studentId||l.student_id)+'</div>'
        +(heightPx>28?'<div style="font-weight:400;opacity:.8;font-size:9px">'+(l.subject||'')+'</div>':'')
        +(heightPx>40?'<div style="opacity:.6;font-size:9px">'+(l.time||'')+(dur>=60?' · '+Math.floor(dur/60)+'г'+(dur%60?dur%60+'хв':''):' · '+dur+'хв')+'</div>':'')
        +(_isPartial&&heightPx>52?'<div style="font-size:9px;font-weight:700">⚠ залишилось '+_unHrs+'г</div>':'')
        +(canDel?'<span onclick="event.stopPropagation();delLesson(\''+l.id+'\')" style="position:absolute;top:2px;right:3px;font-size:10px;opacity:.6;cursor:pointer;line-height:1" title="Видалити">✕</span>':'')
        +'</div>';
    });
    html+='</div>';
  });

  const g=document.getElementById('schg');
  g.style.gridTemplateColumns='52px repeat(7,1fr)';
  g.style.gridTemplateRows='auto '+(totalHrs*ROW_H)+'px';
  g.innerHTML=html;
}




function renderTutors(){
  var isAdmin=R()==='god'||R()==='director'||R()==='admin'||R()==='network_admin';
  var ce=can('tutors');
  var rows='';
  S.tutors.forEach(function(t){
    var acc=S.users.find(function(u){return u.id===t.accId||u.id===t.acc_uid;});
    var cnt=S.students.filter(function(s){
      return (s.tutorId===t.id||s.tutor_id===t.id||(s.tutorIds&&s.tutorIds.indexOf(t.id)>=0))&&s.status==='active';
    }).length;
    var branchBadge=isSuperAdmin()&&!currentBranch()
      ?('<span class="badge" style="background:rgba(167,139,250,.12);color:#a78bfa;font-size:10px">'+branchName(t.branchId||t.branch_id)+'</span>'):'';
    var editBtns=isAdmin
      ?('<div style="display:inline-flex;gap:4px;margin-left:8px">'
        +'<button class="btn btn-g btn-sm" onclick="openTutM(\'' +t.id+ '\')" title="\u0420\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u0442\u0438">\u270f\ufe0f</button>'
        +(ce?('<button class="btn btn-sm" style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger)" onclick="delTutor(\'' +t.id+ '\')" title="\u0412\u0438\u0434\u0430\u043b\u0438\u0442\u0438">\uD83D\uDDD1</button>'):'')
        +'</div>')
      :'';
    var accHtml=acc
      ?('<div style="display:flex;align-items:center;gap:6px">'+mkAv(acc.fn||'?',acc.ln||'',24)
        +'<div><div style="font-size:12px;font-weight:600">'+(acc.fn||'')+' '+(acc.ln||'')+'</div>'
        +'<div style="font-size:10px;color:var(--t2)">'+(acc.email||'')+'</div></div>'
        +'<span class="rpill '+acc.role+'" style="font-size:10px;padding:2px 8px">'+ROLES[acc.role].icon+' '+ROLES[acc.role].label+'</span>'
        +'</div>')
      :'<span style="font-size:11px;color:var(--t3)">\u2014 \u0430\u043a\u0430\u0443\u043d\u0442 \u043d\u0435 \u043f\u0440\u0438\u0432\u2019\u044f\u0437\u0430\u043d\u043e</span>';
    rows+='<tr>'
      +'<td><div style="display:flex;align-items:center;gap:10px">'+mkAv(t.fn,t.ln,36,t.photo)
      +'<div><div style="font-weight:600;font-size:13px">'+t.fn+' '+t.ln+'</div>'
      +(t.subj?'<div style="font-size:11px;color:var(--t2)">'+t.subj+'</div>':'')
      +'</div></div></td>'
      +'<td>'+accHtml+'</td>'
      +'<td style="text-align:center"><span class="badge bb">'+cnt+'</span></td>'
      +'<td style="text-align:right">'+branchBadge+editBtns+'</td>'
      +'</tr>';
  });
  document.getElementById('tt-table').innerHTML=rows||
    '<tr><td colspan="4"><div class="empty"><div class="ei">\uD83E\uDDD1\u200D\uD83C\uDFEB</div>\u0420\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0456\u0432 \u043d\u0435\u043c\u0430\u0454</div></td></tr>';
}

function updateBranchSelector(){
  var el=document.getElementById('branch-sel');
  if(!el) return;
  var bid=S.currentBranchId;
  el.innerHTML='<option value="">\uD83C\uDF10 \u0412\u0441\u0456 \u0444\u0456\u043B\u0456\u0457</option>'+
    (S.branches||[]).map(function(b){
      return '<option value="'+b.id+'"'+(bid===b.id?' selected':'')+'>'+b.name+'</option>';
    }).join('');
}



function updateSBUser(){
  if(!CU)return;
  const r=ROLES[CU.role];
  const av=document.getElementById('sb-av');
  var _mt=myTutor();
  if(_mt&&_mt.photo){
    av.style.background='none';av.style.width='34px';av.style.height='34px';av.style.padding='0';av.style.overflow='hidden';
    av.innerHTML='<img src="'+_mt.photo+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%">';
  } else {
    av.style.background=r.avatarBg;av.style.width='34px';av.style.height='34px';av.style.fontSize='13px';av.style.color=CU.role==='director'?'#1b1464':'#fff';av.style.fontFamily="'Syne',sans-serif";av.style.fontWeight='700';
    av.textContent=(CU.fn[0]||'')+(CU.ln[0]||'');
  }
  document.getElementById('sb-name').textContent=CU.fn+' '+CU.ln;
  document.getElementById('sb-rpill').innerHTML=('<span class="rpill '+(CU.role)+'">'+(r.icon)+' '+(r.label)+'</span>');
}



// CRM KANBAN
var CRM_COLS = [
  {id:'lead',     lbl:'Новий лід',                   ico:'⬤', color:'#f59e0b'},
  {id:'request',  lbl:'Запит',                                       ico:'✉', color:'#3b82f6'},
  {id:'trial',    lbl:'Тестовий урок', ico:'◎', color:'#8b5cf6'},
  {id:'contract', lbl:'Підписання договору', ico:'✍', color:'#06b6d4'},
  {id:'invoice',  lbl:'Виставлення рахунку', ico:'▤', color:'#f97316'},
  {id:'payment',  lbl:'Оплата',                                 ico:'◈', color:'#10b981'},
  {id:'won',      lbl:'Успішно реалізовано', ico:'✅', color:'#22c55e'},
  {id:'lost',     lbl:'Не реалізовано', ico:'❌', color:'#ef4444'},
];

function getCrmStage(s){
  if(!s) return 'lead';
  if(s.crmStage) return s.crmStage;
  var map={active:'won',trial:'trial',paused:'lost',completed:'won'};
  return map[s.status]||'lead';
}

async function setCrmStage(studentId, stage){
  var i=(S.students||[]).findIndex(function(s){return s.id===studentId;});
  var prev=i>=0?(S.students[i].crmStage||S.students[i].crm_stage):null;
  if(i>=0){S.students[i].crmStage=stage;S.students[i].crm_stage=stage;}
  renderCrm();
  try{
    await dbUpdate('students',studentId,{crm_stage:stage});
    mkToast('Етап оновлено');
  }catch(e){
    if(i>=0){S.students[i].crmStage=prev;S.students[i].crm_stage=prev;}
    renderCrm();
    mkToast('Помилка: '+e.message,'error');
  }
}

function openAddLead(){
  openStudM(null);
  document.getElementById('ms-title').textContent='\u041d\u043e\u0432\u0438\u0439 \u043b\u0456\u0434';
  document.getElementById('s-status').value='trial';
  var crmStEl=document.getElementById('s-crm-stage'); if(crmStEl) crmStEl.value='lead';
}


function renderCrm(){
  var el = document.getElementById('crm-board');
  if(!el) return;

  var fStage = (document.getElementById('crm-f-stage')||{value:''}).value||'';
  var fMonth = (document.getElementById('crm-f-month')||{value:''}).value||'';
  var fResp  = (document.getElementById('crm-f-resp') ||{value:''}).value||'';

  // Populate responsible select on first render
  var respSel = document.getElementById('crm-f-resp');
  if(respSel && respSel.options.length <= 1){
    (S.users||[]).filter(function(u){ return u.role==='god'||u.role==='director'||u.role==='admin'; })
      .forEach(function(u){
        var o = document.createElement('option');
        o.value = u.id; o.textContent = u.fn+' '+u.ln;
        respSel.appendChild(o);
      });
    respSel.value = fResp;
  }

  var students = (S.students||[]).filter(function(s){
    if(getCrmStage(s)==='removed') return false; // приховано з CRM-дошки, учень лишається в системі
    if(fStage && getCrmStage(s) !== fStage) return false;
    if(fMonth && (s.crmDate||'').slice(0,7) !== fMonth) return false;
    if(fResp  && s.crmResponsible !== fResp) return false;
    return true;
  });

  var groups = {};
  CRM_COLS.forEach(function(c){ groups[c.id] = []; });
  students.forEach(function(s){
    var st = getCrmStage(s);
    if(!groups[st]) st = 'lead';
    groups[st].push(s);
  });

  var cols = fStage ? CRM_COLS.filter(function(c){ return c.id===fStage; }) : CRM_COLS;

  // Update stats bar
  var statsEl = document.getElementById('crm-stats');
  if(statsEl){
    var total = students.length;
    var won   = students.filter(function(s){ return getCrmStage(s)==='won'; }).length;
    var lost  = students.filter(function(s){ return getCrmStage(s)==='lost'; }).length;
    var conv  = total>0 ? Math.round(won/total*100) : 0;
    statsEl.innerHTML =
      '<span style="background:var(--s2);border:1px solid var(--b1);border-radius:20px;padding:5px 14px">\u0412\u0441\u044c\u043e\u0433\u043e: <b>'+total+'</b></span>'
      +'<span style="background:var(--tut-bg);border:1px solid rgba(34,181,115,.25);border-radius:20px;padding:5px 14px;color:var(--tut)">\u0423\u0441\u043f\u0456\u0448\u043d\u043e: <b>'+won+'</b></span>'
      +'<span style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.25);border-radius:20px;padding:5px 14px;color:var(--danger)">\u041d\u0435 \u0440\u0435\u0430\u043b.: <b>'+lost+'</b></span>'
      +'<span style="background:var(--adm-bg);border:1px solid rgba(41,171,226,.25);border-radius:20px;padding:5px 14px;color:var(--adm)">\u041a\u043e\u043d\u0432\u0435\u0440\u0441\u0456\u044f: <b>'+conv+'%</b></span>';
  }

  el.innerHTML = '';

  cols.forEach(function(col){
    var cards = groups[col.id]||[];

    var colDiv = document.createElement('div');
    colDiv.className = 'crm-col';
    colDiv.addEventListener('dragover',  function(e){ crmDragOver(e); });
    colDiv.addEventListener('dragleave', function(e){ crmDragLeave(e); });
    colDiv.addEventListener('drop',      function(e){ crmDrop(e, col.id); });

    var hdr = document.createElement('div');
    hdr.className = 'crm-col-hdr';
    hdr.style.borderTop = '3px solid ' + col.color;
    hdr.innerHTML = '<span class="crm-col-hdr-ico">'+col.ico+'</span>'
      + '<span class="crm-col-lbl">'+col.lbl+'</span>'
      + '<span class="crm-col-cnt">'+cards.length+'</span>';
    colDiv.appendChild(hdr);

    var body = document.createElement('div');
    body.className = 'crm-col-body';

    if(!cards.length){
      var empty = document.createElement('div');
      empty.className = 'crm-empty-col';
      empty.textContent = '\u041f\u043e\u0440\u043e\u0436\u043d\u044c\u043e';
      body.appendChild(empty);
    }

    cards.forEach(function(s){
      var tutor = s.tutorId ? (S.tutors||[]).find(function(t){ return t.id===s.tutorId; }) : null;
      var resp  = s.crmResponsible ? (S.users||[]).find(function(u){ return u.id===s.crmResponsible; }) : null;
      var lc    = (S.comms||[]).filter(function(c){ return c.studentId===s.id; })
                    .sort(function(a,b){ return (b.date||'')>(a.date||'')?1:-1; })[0];

      var card = document.createElement('div');
      card.className = 'crm-card';
      card.draggable = true;

      var sid = s.id;
      card.addEventListener('dragstart', function(e){ crmDragStart(e, sid); });
      card.addEventListener('dragend',   crmDragEnd);

      var info = document.createElement('div');
      info.innerHTML =
        '<div class="crm-card-name">'+s.fn+' '+s.ln+'</div>'
        +(s.subject ? '<div class="crm-card-subj">'+s.subject+'</div>' : '')
        +(tutor ? '<div class="crm-card-meta">◈ '+tutor.fn+' '+tutor.ln+'</div>' : '')
        +(resp  ? '<div class="crm-card-meta" style="color:var(--dir)">★ '+resp.fn+' '+resp.ln+'</div>' : '')
        +((s.phone||s.parentPhone) ? '<div class="crm-card-meta">☎ '+(s.phone||s.parentPhone)+'</div>' : '')
        +(s.crmDate ? '<div class="crm-card-comm">▣ '+fd(s.crmDate)+'</div>' : '')
        +(lc ? '<div class="crm-card-comm">◎ '+fd(lc.date)+'</div>' : '');
      info.querySelector('.crm-card-name').addEventListener('click', function(){ openStudM(sid); });
      card.appendChild(info);

      // Action buttons
      var acts = document.createElement('div');
      acts.className = 'crm-card-actions';

      var editBtn = document.createElement('button');
      editBtn.className = 'crm-mv-btn';
      editBtn.title = 'Редагувати';
      editBtn.textContent = '✏';
      editBtn.addEventListener('click', function(e){ e.stopPropagation(); openCrmCard(sid); });
      acts.appendChild(editBtn);

      CRM_COLS.filter(function(c){ return c.id !== col.id; }).forEach(function(c){
        var btn = document.createElement('button');
        btn.className = 'crm-mv-btn';
        btn.title = '→ ' + c.lbl;
        btn.textContent = c.ico;
        (function(cid){ btn.addEventListener('click', function(e){ e.stopPropagation(); setCrmStage(sid, cid); }); })(c.id);
        acts.appendChild(btn);
      });

      var delBtn = document.createElement('button');
      delBtn.className = 'crm-mv-btn crm-del-btn';
      delBtn.title = 'Видалити лід';
      delBtn.textContent = '🗑';
      delBtn.addEventListener('click', function(e){ e.stopPropagation(); delLead(sid); });
      acts.appendChild(delBtn);

      card.appendChild(acts);
      body.appendChild(card);
    });

    colDiv.appendChild(body);
    el.appendChild(colDiv);
  });
  setTimeout(crmInitScroll, 50);
}


async function setCrmStage(studentId, stage){
  var i=(S.students||[]).findIndex(function(s){return s.id===studentId;});
  var prev=i>=0?(S.students[i].crmStage||S.students[i].crm_stage):null;
  if(i>=0){S.students[i].crmStage=stage;S.students[i].crm_stage=stage;}
  renderCrm();
  try{
    await dbUpdate('students',studentId,{crm_stage:stage});
    mkToast('Етап оновлено');
  }catch(e){
    if(i>=0){S.students[i].crmStage=prev;S.students[i].crm_stage=prev;}
    renderCrm();
    mkToast('Помилка: '+e.message,'error');
  }
}


function openCrmCard(studentId){
  var s=(S.students||[]).find(function(x){return x.id===studentId;});
  if(!s)return;
  var mo=document.getElementById('mo-crm-card');
  if(!mo){openStudM(studentId);return;}
  document.getElementById('crm-card-name').textContent=s.fn+' '+s.ln;
  var stageSel=document.getElementById('crm-card-stage');
  if(stageSel)stageSel.value=getCrmStage(s);
  var respSel=document.getElementById('crm-card-resp');
  if(respSel){
    respSel.innerHTML='<option value="">—</option>'
      +(S.users||[]).filter(function(u){return u.role==='god'||u.role==='director'||u.role==='admin';})
        .map(function(u){return '<option value="'+u.id+'"'+(s.crmResponsible===u.id?' selected':'')+'>'+u.fn+' '+u.ln+'</option>';}).join('');
  }
  var dateSel=document.getElementById('crm-card-date');
  if(dateSel)dateSel.value=s.crmDate||'';
  var notesSel=document.getElementById('crm-card-notes');
  if(notesSel)notesSel.value=s.crm_notes||'';
  S._crmEditId=studentId;
  openM('mo-crm-card');
}

async function saveCrmCard(){
  var id=S._crmEditId;if(!id)return;
  var stage=document.getElementById('crm-card-stage').value;
  var resp=document.getElementById('crm-card-resp').value;
  var date=document.getElementById('crm-card-date').value;
  var notes=document.getElementById('crm-card-notes').value;
  var i=(S.students||[]).findIndex(function(s){return s.id===id;});
  if(i>=0){
    S.students[i].crmStage=stage;S.students[i].crm_stage=stage;
    S.students[i].crmResponsible=resp||null;S.students[i].crm_responsible=resp||null;
    S.students[i].crmDate=date||null;S.students[i].crm_date=date||null;
    S.students[i].crm_notes=notes||null;
  }
  closeM('mo-crm-card');renderCrm();
  try{
    await dbUpdate('students',id,{crm_stage:stage,crm_responsible:resp||null,crm_date:date||null,crm_notes:notes||null});
    mkToast('Збережено');
  }catch(e){mkToast('Помилка: '+e.message,'error');}
}

function crmClearFilters(){
  var s=document.getElementById('crm-f-stage'),m=document.getElementById('crm-f-month'),r=document.getElementById('crm-f-resp');
  if(s)s.value='';if(m)m.value='';if(r)r.value='';
  renderCrm();
}

var _crmDragId=null;
function crmDragStart(e,id){_crmDragId=id;e.dataTransfer.effectAllowed='move';setTimeout(function(){if(e.target)e.target.style.opacity='0.4';},0);}
function crmDragEnd(e){if(e.target)e.target.style.opacity='1';document.querySelectorAll('.crm-col').forEach(function(c){c.classList.remove('crm-over');});}
function crmDragOver(e){e.preventDefault();document.querySelectorAll('.crm-col').forEach(function(c){c.classList.remove('crm-over');});e.currentTarget.classList.add('crm-over');}
function crmDragLeave(e){if(!e.currentTarget.contains(e.relatedTarget))e.currentTarget.classList.remove('crm-over');}
function crmDrop(e,colId){e.preventDefault();document.querySelectorAll('.crm-col').forEach(function(c){c.classList.remove('crm-over');});if(_crmDragId){setCrmStage(_crmDragId,colId);_crmDragId=null;}}

function crmScroll(dir){
  var el = document.getElementById('crm-board-scroll');
  if(!el) return;
  el.scrollBy({left: dir * 240, behavior: 'smooth'});
}

function crmUpdateScrollBtns(){
  var el = document.getElementById('crm-board-scroll');
  var btnL = document.getElementById('crm-scroll-left');
  var btnR = document.getElementById('crm-scroll-right');
  if(!el || !btnL || !btnR) return;
  var atLeft  = el.scrollLeft <= 10;
  var atRight = el.scrollLeft >= el.scrollWidth - el.clientWidth - 10;
  btnL.classList.toggle('visible', !atLeft);
  btnR.classList.toggle('visible', !atRight);
}

function crmInitScroll(){
  var el = document.getElementById('crm-board-scroll');
  if(!el || el._crmScrollInit) return;
  el._crmScrollInit = true;

  // Mouse wheel horizontal scroll (no Shift needed)
  el.addEventListener('wheel', function(e){
    if(Math.abs(e.deltaX) < Math.abs(e.deltaY)){
      e.preventDefault();
      el.scrollBy({left: e.deltaY * 2, behavior: 'auto'});
    }
    crmUpdateScrollBtns();
  }, {passive: false});

  // Update arrow visibility on scroll
  el.addEventListener('scroll', crmUpdateScrollBtns);

  // Touch swipe
  var touchX = 0;
  el.addEventListener('touchstart', function(e){ touchX = e.touches[0].clientX; }, {passive:true});
  el.addEventListener('touchmove', function(e){
    var dx = touchX - e.touches[0].clientX;
    el.scrollLeft += dx;
    touchX = e.touches[0].clientX;
    crmUpdateScrollBtns();
  }, {passive:true});

  crmUpdateScrollBtns();
}

// ═══════════════════════════════════════
// INVOICE (РАХУНОК-ФАКТУРА)
// ═══════════════════════════════════════

function openInvoicePanel(){
  if(R()!=='god' && R()!=='director'){
    mkToast('Доступ заборонено','error'); return;
  }
  var card = document.getElementById('inv-card');
  if(!card) return;

  // Populate student select
  var sel = document.getElementById('inv-student');
  if(sel){
    sel.innerHTML = '<option value="">— оберіть учня —</option>'
      + (S.students||[]).map(function(s){
          return '<option value="'+s.id+'">'+s.fn+' '+s.ln+'</option>';
        }).join('');
  }

  // Default period: current month
  var now = new Date();
  var y = now.getFullYear(), m = now.getMonth();
  document.getElementById('inv-date-from').value = y+'-'+String(m+1).padStart(2,'0')+'-01';
  document.getElementById('inv-date-to').value   = y+'-'+String(m+1).padStart(2,'0')+'-'+new Date(y,m+1,0).getDate();
  document.getElementById('inv-price').value     = '';
  document.getElementById('inv-email').value     = '';
  document.getElementById('inv-notes').value     = '';

  // Load saved payment details
  var cfg = S.settings||{};
  var payEl = document.getElementById('inv-payment');
  if(payEl) payEl.value = cfg.payment_details||'';

  document.getElementById('inv-preview').innerHTML = '';
  card.style.display = 'block';
  card.scrollIntoView({behavior:'smooth', block:'start'});
}


function calcInvoiceLessons(){
  var selEl = document.getElementById('inv-student');
  var sid   = selEl ? selEl.value : (S._invoiceStudentId||'');
  var from  = document.getElementById('inv-date-from').value;
  var to    = document.getElementById('inv-date-to').value;
  var price = parseFloat(document.getElementById('inv-price').value)||0;

  // Count PLANNED (not yet conducted) lessons
  var lessons = (S.lessons||[]).filter(function(l){
    return (l.studentId===sid||l.student_id===sid)
      && (l.status==='planned'||l.status==='scheduled')
      && l.date >= from && l.date <= to;
  });

  var totalHours = Math.round(lessons.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  var total = Math.round(totalHours * price * 10)/10;
  var el = document.getElementById('inv-preview');
  if(!el) return;

  if(!lessons.length){
    el.innerHTML = '<div style="color:var(--t3);font-size:12px;padding:8px 0">'
      +'\u041d\u0435\u043c\u0430\u0454 \u0437\u0430\u043f\u043b\u0430\u043d\u043e\u0432\u0430\u043d\u0438\u0445 \u0443\u0440\u043e\u043a\u0456\u0432 \u0437\u0430 \u0446\u0435\u0439 \u043f\u0435\u0440\u0456\u043e\u0434</div>';
    return;
  }

  // Sort by date+time
  lessons.sort(function(a,b){ return (a.date+' '+(a.time||'')).localeCompare(b.date+' '+(b.time||'')); });

  var rows = lessons.map(function(l, i){
    var tutor = l.tutorId ? (S.tutors||[]).find(function(t){return t.id===l.tutorId;}) : null;
    return '<tr>'
      +'<td>'+(i+1)+'</td>'
      +'<td>'+fd(l.date)+'</td>'
      +'<td>'+(l.time||'\u2014')+'</td>'
      +'<td>'+(l.subject||l.notes||'\u2014')+'</td>'
      +'<td>'+(tutor ? tutor.fn+' '+tutor.ln : '\u2014')+'</td>'
      +'<td style="text-align:right">'+(price ? price+' \u0433\u0440\u043d' : '\u2014')+'</td>'
      +'</tr>';
  }).join('');

  el.innerHTML = '<table class="inv-table">'
    +'<thead><tr>'
    +'<th style="width:28px">#</th>'
    +'<th>\u0414\u0430\u0442\u0430</th>'
    +'<th>\u0427\u0430\u0441</th>'
    +'<th>\u041f\u0440\u0435\u0434\u043c\u0435\u0442</th>'
    +'<th>\u0420\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440</th>'
    +'<th style="text-align:right">\u0421\u0443\u043c\u0430</th>'
    +'</tr></thead>'
    +'<tbody>'+rows+'</tbody>'
    +'<tfoot><tr>'
    +'<td colspan="5" style="font-weight:700">'
      +'\u0420\u0410\u0417\u041e\u041c: '+lessons.length+' \u0443\u0440\u043e\u043a'+'\u0456\u0432'
    +'</td>'
    +'<td style="text-align:right;font-weight:700;color:var(--adm)">'
      +(price ? total+' \u0433\u0440\u043d' : '\u2014')
    +'</td>'
    +'</tr></tfoot>'
    +'</table>';
}

function sendInvoiceEmail(){
  var m = window._invMeta || {};
  var s = m.studentId ? (S.students||[]).find(function(x){return x.id===m.studentId;}) : null;
  if(!s){ mkToast('\u041e\u0431\u0435\u0440\u0456\u0442\u044c \u0443\u0447\u043d\u044f','error'); return; }
  var email = (s.email||s.parent_email||'').trim();
  if(!email){
    email = prompt('Email \u043e\u0442\u0440\u0438\u043c\u0443\u0432\u0430\u0447\u0430 (\u043d\u0435 \u0432\u043a\u0430\u0437\u0430\u043d\u043e \u0432 \u043a\u0430\u0440\u0442\u0446\u0456):', '');
    if(!email) return;
  }
  var cfg = S.settings||{};
  var subj = '\u0420\u0430\u0445\u0443\u043d\u043e\u043a \u043d\u0430 \u043e\u043f\u043b\u0430\u0442\u0443'
    + (cfg.name?' \u2014 '+cfg.name:'')
    + (m.from||m.to?' ('+(m.from?fd(m.from)+' \u2013 ':'')+(m.to?fd(m.to):'')+')':'');
  var body = window._invText || '';
  _logInvoiceSend('email', email);
  var href = 'mailto:'+encodeURIComponent(email)
    + '?subject='+encodeURIComponent(subj)
    + '&body='+encodeURIComponent(body);
  window.location.href = href;
  mkToast('\u0412\u0456\u0434\u043a\u0440\u0438\u0432\u0430\u0454\u043c\u043e \u043f\u043e\u0448\u0442\u043e\u0432\u0438\u0439 \u043a\u043b\u0456\u0454\u043d\u0442\u2026');
}

window.calcInvoiceLessons = calcInvoiceLessons;
window.sendInvoiceEmail = sendInvoiceEmail;
window.openInvoicePanel = openInvoicePanel;
window.updateInvPhone = updateInvPhone;
// Boot
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


var RIGHTS_MATRIX = [
  ['Дія', 'Бог', 'Директор', 'Адмін', 'Репетитор'],
  ['Учні — перегляд',        '✅','✅','✅','✅'],
  ['Учні — редагування',     '✅','✅','✅','❌'],
  ['Репетитори — перегляд',  '✅','✅','✅','❌'],
  ['Репетитори — редагування','✅','✅','✅','❌'],
  ['Заняття — перегляд',     '✅','✅','✅','✅'],
  ['Заняття — редагування',  '✅','✅','✅','✅'],
  ['Оплата — перегляд',      '✅','✅','❌','❌'],
  ['Оплата — редагування',   '✅','✅','❌','❌'],
  ['Комунікації',            '✅','✅','✅','✅'],
  ['CRM-дошка',              '✅','✅','✅','❌'],
  ['Акаунти',                '✅','✅','❌','❌'],
  ['Налаштування',           '✅','✅','❌','❌'],
  ['Видалення будь-чого',    '✅','✅','✅','❌'],
  ['Небезпечна зона',        '✅','❌','❌','❌'],
];

function renderSettings(){
  var gcWrap = document.getElementById('god-constructor-wrap');
  if(gcWrap) gcWrap.style.display = (R()==='god') ? 'block' : 'none';
  var setNameEl=document.getElementById('set-name'); if(setNameEl) setNameEl.value=S.settings.name||'';
  var setSubjEl=document.getElementById('set-subj-list'); if(setSubjEl) setSubjEl.innerHTML=S.subjects.map((s,i)=>('<div class="ms"><span class="msl">'+(s.name)+'</span><div style="display:flex;align-items:center;gap:8px"><button class="btn btn-sm btn-d" style="padding:2px 6px" onclick="delSubj(\''+(s.id)+'\')">\u00D7</button></div></div>')).join('');
  // God-only sections
  const isGod=R()==='god';
  var gbEl=document.getElementById('god-banner-settings'); if(gbEl) gbEl.style.display=isGod?'flex':'none';
  var rsEl=document.getElementById('rights-section'); if(rsEl) rsEl.style.display=isGod?'block':'none';
  var dzEl=document.getElementById('danger-zone'); if(dzEl) dzEl.style.display=isGod?'block':'none';
  if(isGod){
    // Build rights matrix
    let rt='<thead><tr>'+RIGHTS_MATRIX[0].map((h,i)=>('<th style="'+(i===1?'color:var(--god)':i===2?'color:var(--dir)':i===3?'color:var(--adm)':i===4?'color:var(--tut)':'')+'">'+(h)+'</th>')).join('')+'</tr></thead><tbody>';
    for(let i=1;i<RIGHTS_MATRIX.length;i++){
      rt+='<tr>'+RIGHTS_MATRIX[i].map((c,j)=>('<td style="'+(c.startsWith('\u2705')?'color:var(--tut)':c.startsWith('\u274C')?'color:var(--danger)':'')+'">'+(c)+'</td>')).join('')+'</tr>';
    }
    rt+='</tbody>';
    document.getElementById('rights-table').innerHTML=rt;
  }
  renderBranches();
}



async function uploadTutorPhoto(input){
  var file=input.files[0];
  if(!file)return;
  if(file.size>2*1024*1024){mkToast('Файл занадто великий (макс 2MB)','error');return;}
  var reader=new FileReader();
  reader.onload=async function(e){
    var dataUrl=e.target.result;
    var mt=myTutor();
    if(!mt){mkToast('Профіль не знайдено','error');return;}
    try{
      await dbUpdate('tutors',mt.id,{photo:dataUrl});
      // Update local
      var idx=S.tutors.findIndex(function(t){return t.id===mt.id;});
      if(idx>=0) S.tutors[idx].photo=dataUrl;
      // Update photo preview in form
      var prev=document.getElementById('pr-photo-preview');
      if(prev) prev.innerHTML='<img src="'+dataUrl+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%">';
      mkToast('Фото збережено ✅');
      renderProfile();
    }catch(err){mkToast('Помилка: '+err.message,'error');}
  };
  reader.readAsDataURL(file);
}
window.uploadTutorPhoto=uploadTutorPhoto;


// ═══════════════════════════════════
// SEARCHABLE SELECT UTILITY
// ═══════════════════════════════════
function makeSearchable(selectId){
  var sel=document.getElementById(selectId);
  if(!sel) return;

  // Already initialized — just re-sync options into dropdown
  if(sel.dataset.searchable==='1'){
    sel._updateSearch && sel._updateSearch();
    return;
  }
  sel.dataset.searchable='1';
  sel.style.display='none';

  var wrap=document.createElement('div');
  wrap.className='srch-wrap';
  wrap.style.cssText='position:relative;display:inline-block;width:100%';
  sel.parentNode.insertBefore(wrap,sel);
  wrap.appendChild(sel);

  var inp=document.createElement('input');
  inp.placeholder=sel.options[0]?sel.options[0].text:'Пошук...';
  inp.style.cssText='width:100%;padding:6px 10px;border:1.5px solid var(--b1);border-radius:8px;background:var(--s1);font-size:13px;box-sizing:border-box;color:var(--t1)';
  inp.setAttribute('autocomplete','off');
  wrap.insertBefore(inp,sel);

  var drop=document.createElement('div');
  drop.style.cssText='position:absolute;top:100%;left:0;right:0;background:var(--s1);border:1px solid var(--b1);border-radius:8px;max-height:220px;overflow-y:auto;z-index:9999;display:none;box-shadow:0 4px 16px rgba(0,0,0,.18)';
  wrap.appendChild(drop);

  function renderDrop(q){
    drop.innerHTML='';
    var opts=Array.from(sel.options);
    var filtered=q?opts.filter(function(o){return o.text.toLowerCase().includes(q.toLowerCase());}):opts;
    if(!filtered.length){ drop.style.display='none'; return; }
    filtered.forEach(function(o){
      var item=document.createElement('div');
      item.textContent=o.text;
      item.dataset.val=o.value;
      item.style.cssText='padding:8px 12px;cursor:pointer;font-size:13px;color:var(--t1)';
      item.addEventListener('mouseenter',function(){item.style.background='var(--s2)';});
      item.addEventListener('mouseleave',function(){item.style.background='';});
      item.addEventListener('mousedown',function(e){
        e.preventDefault();
        sel.value=o.value;
        inp.value=o.value?o.text:'';
        drop.style.display='none';
        inp.placeholder=sel.options[0]?sel.options[0].text:'Пошук...';
        sel.dispatchEvent(new Event('change'));
      });
      drop.appendChild(item);
    });
    drop.style.display='block';
  }

  inp.addEventListener('focus',function(){ inp.select(); renderDrop(''); });
  inp.addEventListener('input',function(){ sel.value=''; renderDrop(inp.value); });
  inp.addEventListener('blur',function(){ setTimeout(function(){ drop.style.display='none'; },200); });

  // Called after options are refreshed externally
  sel._updateSearch=function(){
    var opt=sel.value?Array.from(sel.options).find(function(o){return o.value===sel.value;}):null;
    inp.value=opt?opt.text:'';
    inp.placeholder=sel.options[0]?sel.options[0].text:'Пошук...';
  };
  sel._updateSearch();
}
window.makeSearchable=makeSearchable;

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


// Перевіряє чи пропущений урок є відпрацьованим
// (є makeup_date АБО є урок зі статусом makeup для того ж учня в тих самих даних)
// Перевіряє чи makeup-урок є парою до якогось пропущеного (зворотній зв'язок)
function isMakeupForMissed(l){
  if(l.status!=='makeup') return false;
  if(l.missed_date) return true;
  var sid = l.studentId||l.student_id;
  var tid = l.tutorId||l.tutor_id;
  return (S.lessons||[]).some(function(x){
    if(x.status!=='missed') return false;
    if((x.studentId||x.student_id)!==sid) return false;
    if((x.tutorId||x.tutor_id)!==tid) return false;
    return x.makeup_date===l.date || x.split_group_id===l.id || l.split_group_id===x.id;
  });
}

// Сума хвилин ПРОВЕДЕНИХ відпрацювань, пов'язаних із цим пропущеним уроком
function coveredMissedMinutes(l){
  var sid = l.studentId||l.student_id;
  var ldate = l.date;
  var lgroup = l.split_group_id;

  // Якщо пропуск — частина split-групи, покриття розподіляється МІЖ частинами послідовно,
  // а не роздається кожній повністю. Рахуємо на рівні всієї групи.
  if(lgroup){
    // Усі missed-частини групи, впорядковані (split_index, потім час)
    var parts = (S.lessons||[]).filter(function(x){
      return x.status==='missed' && (x.split_group_id===lgroup) && ((x.studentId||x.student_id)===sid);
    }).sort(function(a,b){
      var ai=(a.split_index!=null?a.split_index:999), bi=(b.split_index!=null?b.split_index:999);
      if(ai!==bi) return ai-bi;
      return String(a.time||'').localeCompare(String(b.time||''));
    });
    // Усі проведені відпрацювання, пов'язані з групою (по split_group_id або missed_date будь-якої частини)
    var partDates = parts.map(function(p){return p.date;});
    var makeupsG = (S.lessons||[]).filter(function(x){
      if(x.status!=='makeup') return false;
      if((x.studentId||x.student_id)!==sid) return false;
      if(x.split_group_id===lgroup) return true;
      if(x.missed_date && partDates.indexOf(x.missed_date)>=0) return true;
      return false;
    });
    var pool = makeupsG.reduce(function(s,x){ return s+(parseFloat(x.dur)||60); }, 0);
    // Розподіляємо пул хвилин по частинах по порядку; для нашої частини повертаємо її долю
    for(var i=0;i<parts.length;i++){
      var need = parseFloat(parts[i].dur)||60;
      var give = Math.min(pool, need);
      pool -= give;
      if(parts[i].id===l.id) return give; // скільки хвилин дісталось саме цій частині
    }
    // Якщо не знайшли себе серед частин — падаємо у загальну гілку нижче
  }

  var makeups = (S.lessons||[]).filter(function(x){
    if(x.status!=='makeup') return false; // рахуються лише ПРОВЕДЕНІ відпрацювання
    if((x.studentId||x.student_id)!==sid) return false;
    // 1. Явне поле missed_date у відпрацювання
    if(x.missed_date===ldate) return true;
    // 2. Пропущений урок вказує на дату відпрацювання
    if(l.makeup_date && x.date===l.makeup_date) return true;
    return false;
  });
  return makeups.reduce(function(s,x){ return s+(parseFloat(x.dur)||60); }, 0);
}

// Пропуск вважається покритим ЛИШЕ коли сума годин відпрацювань >= пропущених годин.
// (Раніше makeup_date давав "покрито" без порівняння тривалостей — 2 пропущені години
//  "закривались" 1 годиною відпрацювання.)
function isCoveredMissed(l){
  if(l.status!=='missed') return false;
  return coveredMissedMinutes(l) >= (parseFloat(l.dur)||60);
}

function getUncoveredMissed(lessons){
  return (lessons||[]).filter(function(l){
    return (l.status==='missed') && !isCoveredMissed(l);
  });
}

// Фільтр для дашборду: пропущені без відпрацювання
// Повертає непокриту тривалість пропущеного уроку в годинах
function uncoveredMissedHours(l){
  if(l.status!=='missed') return 0;
  var uncovered=Math.max(0,(parseFloat(l.dur)||60)-coveredMissedMinutes(l));
  return Math.round(uncovered/60*10)/10;
}

function uncoveredMissedFilter(lessons){
  return (lessons||[]).filter(function(l){
    return (l.status==='missed'||l.status==='absent') && !isCoveredMissed(l);
  });
}

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


// ═══════════════════════════════════════════════════════
// TELEPHONY MODULE
// ═══════════════════════════════════════════════════════

var TEL_SETTINGS_KEY = 'crm_tel_settings';
var TEL_LOG_KEY      = 'crm_tel_log';

function telGetSettings(){
  try{ return JSON.parse(localStorage.getItem(TEL_SETTINGS_KEY)||'{}'); }catch(e){ return {}; }
}
function telSaveSettingsLocal(obj){
  localStorage.setItem(TEL_SETTINGS_KEY, JSON.stringify(obj));
}
async function telGetLog(filter){
  if(!_sb) return [];
  try{
    var q = _sb.from('call_logs').select('*').order('created_at',{ascending:false}).limit(200);
    if(filter) q = q.eq('direction', filter);
    var _r = await q;
    return _r.data || [];
  }catch(e){ console.error('telGetLog:',e); return []; }
}
async function telAddLog(entry){
  if(!_sb) return;
  try{
    await _sb.from('call_logs').insert({
      direction:   entry.direction||'inbound',
      phone:       entry.phone||'',
      caller_name: entry.callerName||null,
      duration:    entry.duration||0,
      status:      entry.status||'completed',
      record_url:  entry.recordUrl||null,
      student_id:  entry.studentId||null,
      zadarma_id:  entry.zadarmaId||null,
    });
  }catch(e){ console.error('telAddLog:',e); }
}

function renderTelephony(){
  // Access guard
  var r = R();
  if(r !== 'god' && r !== 'director' && r !== 'admin' && r !== 'network_admin'){
    document.getElementById('pg-telephony').innerHTML =
      '<div class="empty" style="padding:60px"><div class="ei">🔒</div>Доступ лише для адміністраторів та директорів</div>';
    return;
  }

  // Кнопка налаштувань — тільки для god, director, network_admin
  var canSettings = r==='god' || r==='director' || r==='network_admin';
  var settingsBtn = document.getElementById('tel-settings-toggle-btn');
  if(settingsBtn) settingsBtn.style.display = canSettings ? '' : 'none';

  var cfg = telGetSettings();

  // Populate fields
  var setV = function(id,v){ var el=document.getElementById(id); if(el && v!==undefined) el.value=v||''; };
  var setCh = function(id,v){ var el=document.getElementById(id); if(el) el.checked=!!v; };
  setV('tel-provider', cfg.provider);
  setV('tel-url',      cfg.url);
  setV('tel-key',      cfg.key);
  setV('tel-secret',   cfg.secret);
  setV('tel-did',      cfg.did);
  setV('tel-ivr',      cfg.ivr);
  setCh('tel-record',  cfg.record);
  setCh('tel-popup',   cfg.popup !== false);
  setCh('tel-autolog', cfg.autolog !== false);

  // Webhook URL
  var whEl = document.getElementById('tel-webhook-url');
  var _wUrls={kyivstar:'https://rndxbvwisppxnhvrzwqi.supabase.co/functions/v1/kyivstar-webhook',zadarma:'https://rndxbvwisppxnhvrzwqi.supabase.co/functions/v1/zadarma-webhook'};
  if(whEl) whEl.value = _wUrls[cfg.provider]||'https://rndxbvwisppxnhvrzwqi.supabase.co/functions/v1/zadarma-webhook';

  telProviderChange();
  telUpdateStatus(cfg.provider && cfg.key ? 'configured' : 'none');
  renderTelLog();
}

function telProviderChange(){
  var prov = (document.getElementById('tel-provider')||{value:''}).value;
  var rowUrl = document.getElementById('tel-row-url');
  var rowWh  = document.getElementById('tel-row-webhook');
  if(rowUrl) rowUrl.style.display = (prov==='zadarma'||prov==='binotel'||prov==='ringostat'||prov==='kyivstar') ? 'none' : '';
  if(rowWh)  rowWh.style.display  = prov ? '' : 'none';
  // Show provider-specific hints
  var hints = {
    kyivstar: 'Київстар Бізнес АТС: отримайте FMC Token на fmc.kyivstar.ua/crm-integration. Скопіюйте Webhook URL нижче і вставте в поле «URL віддаленої системи» в кабінеті Київстару',
    zadarma:  'Zadarma: отримайте API Key та Secret в особистому кабінеті → Налаштування → API',
    binotel:  'Binotel: API ключ у Binotel кабінеті → Інтеграції → API',
    ringostat:'Ringostat: токен у розділі Інтеграції → API',
    asterisk: 'Asterisk AMI: вкажіть хост:порт (наприклад 192.168.1.10:5038), логін та пароль з /etc/asterisk/manager.conf',
    custom:   'Вкажіть URL вашого API та ключ автентифікації'
  };
  var hintEl = document.getElementById('tel-provider-hint');
  if(!hintEl){
    hintEl = document.createElement('div');
    hintEl.id = 'tel-provider-hint';
    hintEl.style.cssText = 'font-size:11px;color:var(--t3);margin-top:6px;padding:8px;background:var(--s2);border-radius:6px';
    var provRow = document.getElementById('tel-provider');
    if(provRow && provRow.closest('.fgr')) provRow.closest('.fgr').appendChild(hintEl);
  }
  hintEl.textContent = hints[prov]||'';
  hintEl.style.display = hints[prov] ? '' : 'none';
}

function telSaveSettings(){
  var get  = function(id){ var el=document.getElementById(id); return el?el.value.trim():''; };
  var getCh = function(id){ var el=document.getElementById(id); return el?el.checked:false; };
  var cfg = {
    provider: get('tel-provider'),
    url:      get('tel-url'),
    key:      get('tel-key'),
    secret:   get('tel-secret'),
    did:      get('tel-did'),
    ivr:      get('tel-ivr'),
    record:   getCh('tel-record'),
    popup:    getCh('tel-popup'),
    autolog:  getCh('tel-autolog'),
    autolead: getCh('tel-autolead'),
  };
  if(!cfg.provider){ mkToast('Оберіть провайдера','error'); return; }
  if(!cfg.key){ mkToast('Введіть API Key / Token','error'); return; }
  telSaveSettingsLocal(cfg);
  mkToast('Налаштування збережено ✅');
  telUpdateStatus('configured');
  closeM('mo-tel-settings');
}

async function telTestConn(){
  var cfg = telGetSettings();
  if(!cfg.provider||!cfg.key){ mkToast('Спочатку збережіть налаштування','error'); return; }
  mkToast('Перевіряємо з\'єднання…');
  telUpdateStatus('checking');
  // Simulate check (real integration requires server-side proxy due to CORS)
  await new Promise(function(r){setTimeout(r,1500);});
  // In real implementation: call provider API via backend proxy
  var ok = !!(cfg.key && cfg.provider);
  telUpdateStatus(ok ? 'configured' : 'error');
  mkToast(ok
    ? 'Налаштування збережено. Реальний тест відбудеться при першому дзвінку через Zadarma.'
    : '❌ Заповніть API Key та провайдера', ok?'':'error');
}

function telUpdateStatus(state){
  var dot = document.getElementById('tel-status-dot');
  var lbl = document.getElementById('tel-status-lbl');
  if(!dot||!lbl) return;
  var states = {
    none:       {color:'var(--t3)',  text:'Не налаштовано'},
    configured: {color:'#f59e0b',   text:'Налаштовано (не перевірено)'},
    checking:   {color:'#60a5fa',   text:'Перевірка…'},
    ok:         {color:'#22c55e',   text:'Підключено'},
    error:      {color:'#ef4444',   text:'Помилка з\'єднання'},
  };
  var s = states[state]||states.none;
  dot.style.background = s.color;
  lbl.textContent = s.text;
  lbl.style.color = s.color;
}

function telCopyWebhook(){
  var el = document.getElementById('tel-webhook-url');
  if(!el) return;
  navigator.clipboard.writeText(el.value).then(function(){
    mkToast('URL скопійовано 📋');
  }).catch(function(){
    el.select(); document.execCommand('copy'); mkToast('URL скопійовано 📋');
  });
}

function telToggleSettings(){
  var cfg = telGetSettings();
  // Заповнюємо модалку поточними налаштуваннями
  if(cfg.provider){
    document.getElementById('tel-provider').value = cfg.provider;
    telSelectProv(cfg.provider);
  }
  var fields = {
    'tel-url': cfg.url||'',
    'tel-key': cfg.key||'',
    'tel-secret': cfg.secret||'',
    'tel-did': cfg.did||'',
    'tel-ivr': cfg.ivr||''
  };
  Object.entries(fields).forEach(function(kv){
    var el=document.getElementById(kv[0]); if(el) el.value=kv[1];
  });
  var checks = {'tel-record':cfg.record,'tel-popup':cfg.popup,'tel-autolog':cfg.autolog,'tel-autolead':cfg.autolead};
  Object.entries(checks).forEach(function(kv){
    var el=document.getElementById(kv[0]); if(el) el.checked=!!kv[1];
  });
  // Webhook URL
  var _wUrls={kyivstar:'https://rndxbvwisppxnhvrzwqi.supabase.co/functions/v1/kyivstar-webhook',zadarma:'https://rndxbvwisppxnhvrzwqi.supabase.co/functions/v1/zadarma-webhook'};
  var whEl=document.getElementById('tel-webhook-url');
  if(whEl) whEl.value=_wUrls[cfg.provider]||'https://rndxbvwisppxnhvrzwqi.supabase.co/functions/v1/zadarma-webhook';
  // Синхронізуємо статус
  var dot=document.getElementById('tel-modal-dot');
  var lbl=document.getElementById('tel-modal-status-lbl');
  var pill=document.getElementById('tel-modal-status-pill');
  var sdot=document.getElementById('tel-status-dot');
  var slbl=document.getElementById('tel-status-lbl');
  if(dot&&sdot) dot.style.background=sdot.style.background;
  if(lbl&&slbl) lbl.textContent=slbl.textContent;
  openM('mo-tel-settings');
}
window.telToggleSettings = telToggleSettings;

function telSelectProv(prov){
  document.getElementById('tel-provider').value = prov;
  document.querySelectorAll('.prov-card').forEach(function(c){
    c.classList.toggle('sel', c.dataset.prov===prov);
  });
  telProviderChange();
}
window.telSelectProv = telSelectProv;

async function renderTelLog(){
  var body = document.getElementById('tel-log-body');
  if(!body) return;
  var filter = (document.getElementById('tel-log-filter')||{value:''}).value;
  body.innerHTML = '<div style="padding:20px;text-align:center;color:var(--t3)">\u0417\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0435\u043d\u043d\u044f\u2026</div>';
  var log = await telGetLog(filter||null);

  if(!log.length){
    body.innerHTML = '<div class="empty"><div class="ei">\u260e\ufe0f</div>\u0416\u0443\u0440\u043d\u0430\u043b \u0434\u0437\u0432\u0456\u043d\u043a\u0456\u0432 \u043f\u043e\u0440\u043e\u0436\u043d\u0456\u0439</div>';
    return;
  }

  var dirIcon = {inbound:'\u260e', outbound:'\u260e', missed:'\u260e'};
  var dirLbl  = {inbound:'\u0412\u0445\u0456\u0434\u043d\u0438\u0439', outbound:'\u0412\u0438\u0445\u0456\u0434\u043d\u0438\u0439', missed:'\u041f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u0438\u0439'};
  var dirClr  = {inbound:'var(--tut)', outbound:'var(--adm)', missed:'#ef4444'};

  body.innerHTML = '<table style="width:100%;border-collapse:collapse">'
    + '<thead><tr style="font-size:11px;color:var(--t2)">'
    + '<th style="padding:6px 8px;text-align:left">\u0422\u0438\u043f</th>'
    + '<th style="padding:6px 8px;text-align:left">\u041d\u043e\u043c\u0435\u0440</th>'
    + '<th style="padding:6px 8px;text-align:left">\u0423\u0447\u0435\u043d\u044c/\u041a\u043b\u0456\u0454\u043d\u0442</th>'
    + '<th style="padding:6px 8px;text-align:left">\u0422\u0440\u0438\u0432\u0430\u043b\u0456\u0441\u0442\u044c</th>'
    + '<th style="padding:6px 8px;text-align:left">\u0414\u0430\u0442\u0430/\u0427\u0430\u0441</th>'
    + '<th style="padding:6px 8px;text-align:left">\u0417\u0430\u043f\u0438\u0441</th>'
    + '<th style="padding:6px 8px"></th>'
    + '</tr></thead><tbody>'
    + log.map(function(e){
        var sid = e.student_id || e.studentId;
        var s = sid ? (S.students||[]).find(function(x){return x.id===sid;}) : null;
        var sName = s ? s.fn+' '+s.ln : (e.caller_name||e.callerName||'\u2014');
        var dt = e.created_at ? new Date(e.created_at) : null;
        var dtStr = dt ? dt.toLocaleDateString('uk-UA',{day:'2-digit',month:'2-digit',year:'2-digit'})
          +' '+dt.toLocaleTimeString('uk-UA',{hour:'2-digit',minute:'2-digit'}) : '\u2014';
        var dur = e.duration ? (Math.floor(e.duration/60)+'\u0445\u0432 '+(e.duration%60)+'\u0441') : '\u2014';
        var recUrl = e.recording_url || e.record_url || e.recordUrl;
        var dir = e.status==='missed' ? 'missed' : (e.direction||'inbound');
        return '<tr style="border-top:1px solid var(--b1);font-size:13px">'
          +'<td style="padding:8px;color:'+(dirClr[dir]||'var(--t1)')+'">'+'\u260e'+' '+(dirLbl[dir]||dir)+'</td>'
          +'<td style="padding:8px;font-family:JetBrains Mono,monospace;font-size:12px"><a href="tel:'+(e.caller_phone||e.phone||'')+'" style="color:var(--adm);text-decoration:none">'+(e.caller_phone||e.phone||'\u2014')+'</a></td>'
          +'<td style="padding:8px">'+sName+'</td>'
          +'<td style="padding:8px;font-family:JetBrains Mono,monospace;font-size:12px">'+dur+'</td>'
          +'<td style="padding:8px;font-size:11px;color:var(--t2)">'+dtStr+'</td>'
          +'<td style="padding:8px">'+(recUrl
            ? '<audio controls style="height:28px;max-width:180px"><source src="'+recUrl+'"></audio>'
            : '<span style="color:var(--t3);font-size:11px">\u043d\u0435\u043c\u0430\u0454</span>')+'</td>'
          +'<td style="padding:8px">'
            +(sid ? '<button class="btn btn-g btn-sm" onclick="openStudM(\''+sid+'\')">\ud83d\udc64</button>' : '<button class="btn btn-g btn-sm" onclick="telLinkStudent(\''+e.id+'\',\''+(e.caller_phone||e.phone||'')+'\')">\u041f\u0440\u0438\u0432\u2019\u044f\u0437\u0430\u0442\u0438</button>')
          +'</td>'
          +'</tr>';
      }).join('')
    + '</tbody></table>';
}


async function telLinkStudent(logId, phone){
  var found = (S.students||[]).find(function(s){
    return s.phone && s.phone.replace(/\D/g,'') === phone.replace(/\D/g,'');
  });
  if(found){
    try{
      await _sb.from('call_logs').update({student_id:found.id, caller_name:found.fn+' '+found.ln}).eq('id',logId);
      mkToast('\u041f\u0440\u0438\u0432\u02bc\u044f\u0437\u0430\u043d\u043e \u0434\u043e '+found.fn+' '+found.ln);
      renderTelLog();
    }catch(e){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+e.message,'error'); }
  } else {
    mkToast('\u0423\u0447\u043d\u044f \u0437 \u0442\u0430\u043a\u0438\u043c \u043d\u043e\u043c\u0435\u0440\u043e\u043c \u043d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e. \u041f\u0435\u0440\u0435\u0432\u0456\u0440\u0442\u0435 \u043d\u043e\u043c\u0435\u0440 \u0432 \u043a\u0430\u0440\u0442\u0446\u0456 \u0443\u0447\u043d\u044f.','error');
  }
}
window.telLinkStudent = telLinkStudent;
