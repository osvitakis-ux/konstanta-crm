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
  
  if (document.body) {
    document.body.appendChild(div);
  } else {
    document.addEventListener('DOMContentLoaded', function(){ 
      document.body.appendChild(div); 
    });
  }
  return false;
};

// ═══════════════════════════════════
// ДОДАТКОВІ ФУНКЦІЇ
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
    email:(document.getElementById('br-email')||{value:''}).value
  };
  try{
    if(S.editId) await dbUpdate('branches',S.editId,obj);
    else{obj.id=uid();await dbInsert('branches',obj);}
    mkToast('Збережено');
    closeM('mo-branch');
  }catch(e){mkToast('Помилка: '+e.message,'error');}
}

async function splitLessonTo30(){
  var id=S.editId;
  if(!id){mkToast('Не знайдено урок','error');return;}
  var orig=(S.lessons||[]).find(function(l){return l.id===id;});
  if(!orig){mkToast('Урок не знайдено','error');return;}
  var curDur=parseInt((document.getElementById('l-dur')||{value:'60'}).value)||parseInt(orig.dur)||60;
  var nParts=Math.floor(curDur/30);
  if(nParts<2){mkToast('Тривалість мінімум 60 хв','error');return;}
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
    await dbUpdate('lessons',id,{dur:30,price:base.price,split_group_id:id,split_index:0});
    for(var p=1;p<nParts;p++){
      var totalMins=lm0+30*p;
      var newH=lh0+Math.floor(totalMins/60);
      var newM=totalMins%60;
      var newTime=String(newH).padStart(2,'0')+':'+String(newM).padStart(2,'0');
      await dbInsert('lessons',Object.assign({},base,{id:uid(),time:newTime,split_index:p}));
    }
    mkToast('Розбито на '+nParts+' × 30 хв');
    closeM('mo-lesson');
  }catch(e){mkToast('Помилка: '+e.message,'error');}
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
    await _sb.from('invoice_log').insert({
      sent_by:CU.id,student_id:studentId||null,
      period_from:from||null,period_to:to||null,
      lessons_count:lessonsCount||0,total_amount:total||0,
      channel:channel,recipient:recipient||'',branch_id:myBranchId()||null
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
  if(sel && myStudents().length){
    if(sel.options.length<=1){
      sel.innerHTML='<option value="">— оберіть учня —</option>'
        +myStudents().map(function(s){return '<option value="'+s.id+'">'+s.fn+' '+s.ln+'</option>';}).join('');
    }
  }
}

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

// ═══════════════════════════════════
// CRM - SUPABASE EDITION
// ═══════════════════════════════════

// = Constants & Roles =

var ROLES = {
  god: {
    label:'\u0411\u043E\u0433 \u0441\u0438\u0441\u0442\u0435\u043C\u0438', icon:'\u26A1', color:'var(--god2)',
    avatarBg:'linear-gradient(135deg,#2e3192,#5b60d4)',
    nav:['dashboard','students','tutors','schedule','lessons','comms','missed','payments','invoice','invoice-log','reports','analytics','crm','users','settings'],
    can:{students:true,tutors:true,lessons:true,payments:true,users:true,settings:true,danger:true,deleteAny:true},
    seeIncome:true, seeAll:true, canEditUsers:true, showGodBanner:true
  },
  director: {
    label:'\u0414\u0438\u0440\u0435\u043A\u0442\u043E\u0440', icon:'\uD83D\uDC51', color:'var(--dir)',
    avatarBg:'linear-gradient(135deg,#d9e021,#fcee21)',
    nav:['dashboard','students','tutors','schedule','lessons','comms','missed','payments','invoice','invoice-log','reports','analytics','crm','users','settings'],
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
    nav:['dashboard','students','tutors','schedule','lessons','comms','missed','payments','invoice','invoice-log','reports','analytics','crm','users','settings'],
    can:{students:true,tutors:true,lessons:true,payments:true,users:true,settings:true,danger:false,deleteAny:true},
    seeIncome:true, seeAll:true, canEditUsers:true, showGodBanner:false
  },
  tutor: {
    label:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440', icon:'\uD83D\uDCDA', color:'var(--tut)',
    avatarBg:'linear-gradient(135deg,#22b573,#7ac943)',
    nav:['dashboard','students','schedule','lessons','comms','missed','profile'],
    can:{students:true,tutors:false,lessons:true,payments:false,users:false,settings:false,danger:false,deleteAny:false},
    seeIncome:false, seeAll:false, canEditUsers:false, showGodBanner:false
  }
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
  {id:'crm',        ico:'▤',       lbl:'CRM',                  sec:'Менеджмент'},
  {id:'comms',      ico:'\u25CE',  lbl:'\u041a\u043e\u043c\u0443\u043d\u0456\u043a\u0430\u0446\u0456\u0457', sec:'\u041d\u0430\u0432\u0447\u0430\u043d\u043d\u044f'},
  {id:'missed',     ico:'\u25C9',  lbl:'\u041f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u0456',   sec:'\u041d\u0430\u0432\u0447\u0430\u043d\u043d\u044f'},
  {id:'invoice',    ico:'\u25A6',  lbl:'\u0420\u0430\u0445\u0443\u043d\u043e\u043a',                  sec:'\u0424\u0456\u043n\u0430\u043d\u0441\u0438'},
  {id:'invoice-log',ico:'\u25A4',  lbl:'\u041b\u043e\u0433 \u0440\u0430\u0445\u0443\u043d\u043a\u0456\u0432', sec:'\u0424\u0456\u043d\u0430\u043d\u0441\u0438'},
  {id:'profile',    ico:'\u25A3',  lbl:'\u041C\u0456\u0439 \u043F\u0440\u043E\u0444\u0456\u043B\u044C',  sec:'\u041E\u0441\u043E\u0431\u0438\u0441\u0442\u0435'}
];

var DEFAULT_NAV_CFG = NAV_CFG;
var PLABELS={dashboard:'\u0414\u0430\u0448\u0431\u043E\u0440\u0434',students:'\u0423\u0447\u043D\u0456',tutors:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438',schedule:'\u0420\u043E\u0437\u043A\u043B\u0430\u0434',lessons:'\u0417\u0430\u043D\u044F\u0442\u0442\u044F',payments:'\u041E\u043F\u043B\u0430\u0442\u0430',reports:'\u0410\u043D\u0430\u043B\u0456\u0442\u0438\u043A\u0430',users:'\u0410\u043A\u0430\u0443\u043D\u0442\u0438',settings:'\u041D\u0430\u043B\u0430\u0448\u0442\u0443\u0432\u0430\u043D\u043D\u044F',profile:'\u041C\u0456\u0439 \u043F\u0440\u043E\u0444\u0456\u043B\u044C',crm:'CRM',analytics:'\u0421\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043A\u0430'};
