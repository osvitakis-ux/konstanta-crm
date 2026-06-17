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
          if(this._cb){
            var cb = this._cb;
            var opts = this._opts||{};
            var table = opts.table;
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

    function rpc(fn, params){
      return query('rpc/' + fn, 'POST', params, null, {});
    }

    return { from: from, auth: auth, channel: channel, removeChannel: removeChannel, rpc: rpc };
  }

  return { createClient: createClient };
})();

window.supabase = window.SupabaseMini;

// ── Ініціалізація вашого проекту з актуальними ключами ──
var SUBA_URL = "https://21KKA9MELBdwMRj4XG0riw.supabase.co";
var SUBA_KEY = "sb_publishable_21KKA9MELBdwMRj4XG0riw_NuLYzpAw";
var _sb = window.SupabaseMini.createClient(SUBA_URL, SUBA_KEY);

var CU = null; 
var S = {};    

window.__startTime = Date.now();
window.onerror = function(msg, src, line, col, err) {
  if(msg === 'Script error.' || msg === 'Script error') {
    console.warn('External script error (possibly CDN) - check network');
    return false;
  }
  var div = document.createElement('div');
  div.style.cssText = 'position:fixed;top:0;left:0;right:0;background:#f8d7da;color:#721c24;padding:16px;font-family:monospace;font-size:13px;z-index:99999;border-bottom:2px solid #f5c6cb';
  div.innerHTML = '<strong>JS Error at line ' + line + ':</strong><br>' + msg + '<br><small>' + (err ? err.stack : '') + '</small>';
  if (document.body) {
    document.body.appendChild(div);
  } else {
    document.addEventListener('DOMContentLoaded', function(){ document.body.appendChild(div); });
  }
  return false;
};

// ── Авторизація користувача ────────────────────────────
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
    location.reload(); 
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
    if(typeof loadAllData === 'function') loadAllData();
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

// ═══════════════════════════════════
// ДОДАТКОВІ ФУНКЦІЇ CRM
// ═══════════════════════════════════

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

function renderCommsPage(){
  var tbody=document.getElementById('comms-tbody');
  if(!tbody)return;
  var _selfId=null;
  if(R()==='tutor'){
    var _myT=myTutor();
    if(_myT) _selfId=_myT.id;
  }
  var fStud=(document.getElementById('comm-f-student')||{value:''}).value;
  var fTutor=(document.getElementById('comm-f-tutor')||{value:''}).value;
  var fType=(document.getElementById('comm-f-type')||{value:''}).value;
  var comms=[].concat(S.comms||[]).sort(function(a,b){return (b.date||'').localeCompare(a.date||'');});
  if(_selfId) comms=comms.filter(function(c){return (c.tutorId||c.tutor_id)===_selfId;});
  if(fStud) comms=comms.filter(function(c){return (c.studentId||c.student_id)===fStud;});
  if(fTutor) comms=comms.filter(function(c){return (c.tutorId||c.tutor_id)===fTutor;});
  if(fType) comms=comms.filter(function(c){return c.type===fType;});
  var ico={call:'📞',message:'💬',meeting:'🤝',email:'📧',other:'📋',msg:'💬',meet:'🤝'};
  if(!comms.length){
    tbody.innerHTML='<tr><td colspan="5" style="text-align:center;padding:20px;color:var(--t3)">Комунікацій немає</td></tr>';
    return;
  }
  tbody.innerHTML=comms.map(function(c){
    var tutor=(S.tutors||[]).find(function(t){return t.id===(c.tutorId||c.tutor_id);});
    var student=(S.students||[]).find(function(s){return s.id===(c.studentId||c.student_id);});
    return '<tr><td style="font-size:11px;color:var(--t2)">'+fd(c.date)+'</td>'
      +'<td>'+(ico[c.type]||'📋')+' '+(c.type||'—')+'</td>'
      +'<td>'+(student?student.fn+' '+student.ln:'—')+'</td>'
      +'<td>'+(tutor?tutor.fn+' '+tutor.ln:'—')+'</td>'
      +'<td>'+(c.note||'—')+'</td></tr>';
  }).join('');
}

function renderMissedLessons(){
  var tbody=document.getElementById('missed-tbody');
  if(!tbody)return;
  var fStud=(document.getElementById('missed-f-student')||{value:''}).value;
  var _selfId=R()==='tutor'?(myTutor()||{}).id:null;
  var missed=(S.lessons||[]).filter(function(l){
    return (l.status==='missed'||l.status==='makeup')&&(!_selfId||(l.tutorId||l.tutor_id)===_selfId);
  }).sort(function(a,b){return (b.date||'').localeCompare(a.date||'');});
  if(fStud) missed=missed.filter(function(l){return (l.studentId||l.student_id)===fStud;});
  if(!missed.length){
    tbody.innerHTML='<tr><td colspan="7" style="text-align:center;padding:20px;color:var(--t3)">Пропущених немає</td></tr>';
    return;
  }
  tbody.innerHTML=missed.map(function(l){
    var s=(S.students||[]).find(function(x){return x.id===(l.studentId||l.student_id);});
    var t=(S.tutors||[]).find(function(x){return x.id===(l.tutorId||l.tutor_id);});
    var stl=l.status==='missed'
      ?'<span style="color:#ef4444;font-weight:600">Пропущено</span>'
      :'<span style="color:#f59e0b;font-weight:600">Відпрацювання</span>';
    return '<tr><td>'+fd(l.date)+'</td>'
      +'<td>'+(s?s.fn+' '+s.ln:'—')+'</td>'
      +'<td>'+(t?t.fn+' '+t.ln:'—')+'</td>'
      +'<td>'+(l.subject||'—')+'</td>'
      +'<td>'+stl+'</td>'
      +'<td style="font-size:11px">'+(l.missed_date?fd(l.missed_date):'—')+'</td>'
      +'<td style="font-size:11px">'+(l.makeup_date?fd(l.makeup_date):'—')+'</td></tr>';
  }).join('');
}

function deleteLessonFromModal(){
  if(!S.editId)return;
  if(!confirm('Видалити цей урок?'))return;
  if(typeof dbDelete === 'function') dbDelete('lessons',S.editId);
  closeM('mo-lesson');
}

async function deleteLessonSeriesFromModal(){
  if(!S.editId)return;
  var l=(S.lessons||[]).find(function(x){return x.id===S.editId;});
  if(!l||!l.recurId){ if(typeof mkToast==='function') mkToast('Немає серії','error'); return; }
  var series=S.lessons.filter(function(x){return x.recurId===l.recurId;});
  if(!confirm('Видалити всю серію? ('+series.length+' уроків)'))return;
  try{
    for(var i=0;i<series.length;i++) await _sb.from('lessons').delete().eq('id',series[i].id);
    S.lessons=S.lessons.filter(function(x){return x.recurId!==l.recurId;});
    if(typeof mkToast==='function') mkToast('Серію видалено'); 
    closeM('mo-lesson'); 
    if(typeof renderSch === 'function') renderSch();
  }catch(e){ if(typeof mkToast==='function') mkToast('Помилка: '+e.message,'error'); }
}

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
  else if(typeof mkToast==='function') mkToast('Немає телефону','error');
}

function sendViberFromPanel(){
  var sid=(document.getElementById('inv-student')||{value:''}).value;
  var s=(S.students||[]).find(function(x){return x.id===sid;});
  if(!s){ if(typeof mkToast==='function') mkToast('Оберіть учня','error'); return; }
  var phone=((s.phone||s.parentPhone)||'').replace(/\D/g,'');
  if(phone) window.open('viber://chat?number='+phone);
  else if(typeof mkToast==='function') mkToast('Немає телефону','error');
}

function openBranchM(id){
  var b=id?(S.branches||[]).find(function(x){return x.id===id;}):null;
  ['name','addr','phone','email'].forEach(function(f){
    var el=document.getElementById('br-'+f);
    if(el) el.value=b?(b[f]||b[f.replace('addr','address')]||''):'';
  });
  S.editId=id||null;
  if(typeof openM === 'function') openM('mo-branch');
}

async function saveBranchModal(){
  var name=(document.getElementById('br-name')||{value:''}).value.trim();
  if(!name){ if(typeof mkToast==='function') mkToast('Введіть назву','error'); return; }
  var obj={
    name:name,
    address:(document.getElementById('br-addr')||{value:''}).value,
    phone:(document.getElementById('br-phone')||{value:''}).value,
    email:(document.getElementById('br-email')||{value:''}).value
  };
  try{
    if(S.editId) { if(typeof dbUpdate==='function') await dbUpdate('branches',S.editId,obj); }
    else{ if(typeof uid==='function') obj.id=uid(); if(typeof dbInsert==='function') await dbInsert('branches',obj); }
    if(typeof mkToast==='function') mkToast('Збережено');
    if(typeof closeM === 'function') closeM('mo-branch');
  }catch(e){ if(typeof mkToast==='function') mkToast('Помилка: '+e.message,'error'); }
}

async function splitLessonTo30(){
  var id=S.editId;
  if(!id){ if(typeof mkToast==='function') mkToast('Не знайдено урок','error'); return; }
  var orig=(S.lessons||[]).find(function(l){return l.id===id;});
  if(!orig){ if(typeof mkToast==='function') mkToast('Урок не знайдено','error'); return; }
  var curDur=parseInt((document.getElementById('l-dur')||{value:'60'}).value)||parseInt(orig.dur)||60;
  var nParts=Math.floor(curDur/30);
  if(nParts<2){ if(typeof mkToast==='function') mkToast('Тривалість мінімум 60 хв','error'); return; }
  if(!confirm('Розбити ('+curDur+' хв) на '+nParts+' × 30 хв?'))return;
  var lt=orig.time||'10:00';
  var lh0=parseInt(lt.split(':')[0]);
  var lm0=parseInt(lt.split(':')[1]||'0');
  var base={
    student_id:orig.studentId||orig.student_id,
    tutor_id:orig.tutorId||orig.tutor_id,
    subject:orig.subject||'',date:orig.date,
    status:orig.status||'missed',dur:30,
    price:Math.round((orig.price||0)/nParts),
    branch_id:orig.branchId||orig.branch_id||null,
    split_group_id:id,split_index:0
  };
  try{
    if(typeof dbUpdate==='function') await dbUpdate('lessons',id,{dur:30,price:base.price,split_group_id:id,split_index:0});
    for(var p=1;p<nParts;p++){
      var totalMins=lm0+30*p;
      var newH=lh0+Math.floor(totalMins/60);
      var newM=totalMins%60;
      var newTime=String(newH).padStart(2,'0')+':'+String(newM).padStart(2,'0');
      if(typeof uid==='function' && typeof dbInsert==='function') {
        await dbInsert('lessons',Object.assign({},base,{id:uid(),time:newTime,split_index:p}));
      }
    }
    if(typeof mkToast==='function') mkToast('Розбито на '+nParts+' × 30 хв');
    if(typeof closeM === 'function') closeM('mo-lesson');
  }catch(e){ if(typeof mkToast==='function') mkToast('Помилка: '+e.message,'error'); }
}

function calcTutorRating(tutorId){
  var now=new Date(), fourWeeksAgo=new Date(now);
  fourWeeksAgo.setDate(now.getDate()-28);
  var from=localDateStr(fourWeeksAgo), today=localDateStr(now);
  var lessons=(S.lessons||[]).filter(function(l){
    return (l.tutorId||l.tutor_id)===tutorId&&l.date>=from&&l.date<=today;
  });
  var done=lessons.filter(function(l){return l.status==='done'||l.status==='completed'||l.status==='makeup';}).length;
  var missed=lessons.filter(function(l){return l.status==='missed';}).length;
  var total=done+missed;
  var pct=total>0?Math.round(done/total*100):null;
  if(pct===null) return 5;
  if(pct>=90&&missed===0) return 5;
  if(pct>=75) return 4;
  if(pct>=60) return 3;
  if(pct>=40) return 2;
  return 1;
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
  if(!CU||!_sb) return;
  try{
    var bId = typeof myBranchId === 'function' ? myBranchId() : null;
    await _sb.from('invoice_log').insert({
      sent_by:CU.id,student_id:studentId||null,
      period_from:from||null,period_to:to||null,
      lessons_count:lessonsCount||0,total_amount:total||0,
      channel:channel,recipient:recipient||'',branch_id:bId
    });
  }catch(e){}
}

async function renderInvoiceLog(){
  var tbody=document.getElementById('inv-log-tbody');
  if(!tbody)return;
  try{
    var res=await _sb.from('invoice_log').select('*').order('sent_at',{ascending:false}).limit(200);
    if(res.error)throw res.error;
    var rows=res.data||[];
    if(!rows.length){tbody.innerHTML='<tr><td colspan="7" style="text-align:center;padding:20px;color:var(--t3)">Рахунків немає</td></tr>';return;}
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
    tbody.innerHTML='<tr><td colspan="7" style="color:var(--danger)">Помилка: '+e.message+'</td></tr>';
  }
}

function renderInvoicePage(){
  var sel=document.getElementById('inv-student');
  if(sel && typeof myStudents === 'function' && myStudents().length){
    if(sel.options.length<=1){
      sel.innerHTML='<option value="">— оберіть учня —</option>'
        +myStudents().map(function(s){return '<option value="'+s.id+'">'+s.fn+' '+s.ln+'</option>';}).join('');
    }
  }
}

// Прив'язка функцій до глобального простору імен (window)
window.doLogin = doLogin;
window.initApp = initApp;
window.onLessStatChange = onLessStatChange;
window.renderCommsPage = renderCommsPage;
window.renderMissedLessons = renderMissedLessons;
window.deleteLessonFromModal = deleteLessonFromModal;
window.deleteLessonSeriesFromModal = deleteLessonSeriesFromModal;
window.updateInvPhone = updateInvPhone;
window.openViberContact = openViberContact;
window.sendViberFromPanel = sendViberFromPanel;
window.openBranchM = openBranchM;
window.saveBranchModal = saveBranchModal;
window.splitLessonTo30 = splitLessonTo30;
window.calcTutorRating = calcTutorRating;
window.updateAllTutorRatings = updateAllTutorRatings;
window.logInvoice = logInvoice;
window.renderInvoiceLog = renderInvoiceLog;
window.renderInvoicePage = renderInvoicePage;

document.addEventListener('DOMContentLoaded', initApp);
