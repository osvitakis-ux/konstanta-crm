

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

// ═══════════════════════════════════════════════════════════════
//  ЛІДИ З ДЗВІНКІВ
//  Окрема сутність: не картка учня. Видалення ліда не чіпає
//  журнал телефонії й не призводить до повторного створення.
// ═══════════════════════════════════════════════════════════════
// Ручна синхронізація лідів із журналу дзвінків + чітка діагностика в тостах.
async function telSyncLeadsNow(){
  window._phoneLeadInsertBlocked = false; // скидаємо блок, щоб спробувати знову
  mkToast('Синхронізую ліди з дзвінків…','info');
  try{
    var log = await telGetLog(null);
    if(!log || !log.length){
      mkToast('У журналі немає дзвінків. Перевірте синхронізацію телефонії (call_logs порожній).','error');
      return;
    }
    function tail10(v){ return String(v||'').replace(/\D/g,'').slice(-10); }
    var studentTails={};
    (S.students||[]).forEach(function(s){ var a=tail10(s.phone); if(a)studentTails[a]=1; var b=tail10(s.parentPhone||s.parent_phone); if(b)studentTails[b]=1; });
    var ignSet={};
    try{ var _ign=await _sb.from('ignored_phones').select('phone'); (_ign.data||[]).forEach(function(x){var t=tail10(x.phone);if(t)ignSet[t]=1;}); }catch(e){}
    var leadTails={};
    try{ var _lr=await _sb.from('phone_leads').select('phone'); (_lr.data||[]).forEach(function(x){var t=tail10(x.phone);if(t)leadTails[t]=1;}); }catch(e){}
    var unknown={};
    log.forEach(function(e){
      var phone=(((e.direction==='outbound')?(e.callee_phone||e.callee||''):(e.caller_phone||''))||e.phone||'').trim();
      var t=tail10(phone);
      if(!t || t.length<7) return;
      if(studentTails[t]||ignSet[t]||leadTails[t]) return;
      if(!unknown[t]) unknown[t]=phone;
    });
    var keys=Object.keys(unknown);
    if(!keys.length){ mkToast('Дзвінків: '+log.length+'. Нових номерів немає — усі вже відомі (учні/ліди).','info'); renderLeads(); return; }
    // пробуємо СТВОРИТИ перший лід — щоб зловити точну помилку доступу/схеми
    var _ins=null;
    try{ _ins = await _sb.from('phone_leads').insert({ phone:unknown[keys[0]], phone_key:keys[0], status:'new', calls_count:1, last_call:new Date().toISOString() }); }
    catch(ex){ _ins={error:ex}; }
    if(_ins && _ins.error){
      var m=(_ins.error && (_ins.error.message||_ins.error.hint||_ins.error.code))||JSON.stringify(_ins.error);
      mkToast('Не вдалося створити лід: '+m,'error');
      console.error('[telSyncLeadsNow] phone_leads INSERT error →', _ins.error);
      return;
    }
    var created=1;
    for(var i=1;i<keys.length;i++){
      try{ var r=await _sb.from('phone_leads').insert({ phone:unknown[keys[i]], phone_key:keys[i], status:'new', calls_count:1, last_call:new Date().toISOString() }); if(!(r&&r.error)) created++; }catch(e){}
    }
    mkToast('Готово! Дзвінків: '+log.length+', створено нових лідів: '+created);
    renderLeads();
  }catch(e){ mkToast('Помилка синхронізації: '+(e.message||e),'error'); console.error('[telSyncLeadsNow]', e); }
}
window.telSyncLeadsNow=telSyncLeadsNow;

async function renderLeads(){
  var body=document.getElementById('leads-body');
  var sum=document.getElementById('lead-summary');
  if(!body) return;
  body.innerHTML=skeleton('rows',6);

  var rows=[];
  try{
    var r=await _sb.from('phone_leads').select('*').order('last_call',{ascending:false}).limit(300);
    if(r.error) throw r.error;
    rows=r.data||[];
  }catch(e){
    body.innerHTML='<div class="empty"><div class="ei">\u26a0\ufe0f</div>'
      +'\u0422\u0430\u0431\u043b\u0438\u0446\u044f \u043b\u0456\u0434\u0456\u0432 \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0430.<br>'
      +'<span style="font-size:12px">\u0412\u0438\u043a\u043e\u043d\u0430\u0439\u0442\u0435 phone_leads_setup.sql \u0443 Supabase</span></div>';
    if(sum) sum.innerHTML='';
    return;
  }

  var q=((document.getElementById('lead-search')||{value:''}).value||'').replace(/\D/g,'');
  var f=(document.getElementById('lead-filter')||{value:'new'}).value;

  var all=rows.slice();
  var shown=all.filter(function(l){
    if(q && String(l.phone||'').replace(/\D/g,'').indexOf(q)<0) return false;
    if(f==='dismissed') return l.dismissed;
    if(l.dismissed) return false;               // видалені показуємо лише у своєму фільтрі
    if(f==='') return true;
    return (l.status||'new')===f;
  });

  // Підсумки
  if(sum){
    var cNew=all.filter(function(l){return !l.dismissed&&(l.status||'new')==='new';}).length;
    var cWork=all.filter(function(l){return !l.dismissed&&l.status==='contacted';}).length;
    var cConv=all.filter(function(l){return l.status==='converted';}).length;
    var cDism=all.filter(function(l){return l.dismissed;}).length;
    sum.innerHTML='<span>\ud83c\udd95 \u041d\u043e\u0432\u0456 <b style="color:var(--adm)">'+cNew+'</b></span>'
      +'<span>\ud83d\udcde \u0412 \u0440\u043e\u0431\u043e\u0442\u0456 <b style="color:var(--warn)">'+cWork+'</b></span>'
      +'<span>\u2705 \u0421\u0442\u0430\u043b\u0438 \u0443\u0447\u043d\u044f\u043c\u0438 <b style="color:var(--tut)">'+cConv+'</b></span>'
      +'<span>\ud83d\uddd1 \u0412\u0438\u0434\u0430\u043b\u0435\u043d\u0456 <b style="color:var(--t3)">'+cDism+'</b></span>';
  }

  if(!shown.length){
    body.innerHTML='<div class="empty"><div class="ei">\u260e</div>\u041d\u0435\u043c\u0430\u0454 \u043b\u0456\u0434\u0456\u0432 \u0443 \u0446\u0456\u0439 \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0456\u0457</div>';
    return;
  }

  var STL={new:['bb','\u041d\u043e\u0432\u0438\u0439'],contacted:['by','\u0412 \u0440\u043e\u0431\u043e\u0442\u0456'],
           converted:['bg','\u0421\u0442\u0430\u0432 \u0443\u0447\u043d\u0435\u043c'],rejected:['br','\u0412\u0456\u0434\u043c\u043e\u0432\u0430']};

  body.innerHTML='<div style="overflow-x:auto"><table class="tel-tbl"><thead><tr>'
    +'<th>\u041d\u043e\u043c\u0435\u0440</th><th>\u0414\u0437\u0432\u0456\u043d\u043a\u0456\u0432</th>'
    +'<th>\u041e\u0441\u0442\u0430\u043d\u043d\u0456\u0439</th><th>\u0421\u0442\u0430\u0442\u0443\u0441</th>'
    +'<th>\u041d\u043e\u0442\u0430\u0442\u043a\u0430</th><th></th></tr></thead><tbody>'
    +shown.map(function(l){
      var st=STL[l.status||'new']||STL.new;
      var d=l.last_call?new Date(l.last_call):null;
      var ds=d?String(d.getDate()).padStart(2,'0')+'.'+String(d.getMonth()+1).padStart(2,'0')
             +' '+String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0'):'\u2014';
      return '<tr'+(l.dismissed?' style="opacity:.55"':'')+'>'
        +'<td><a class="tel-num" href="tel:'+l.phone+'">'+l.phone+'</a></td>'
        +'<td style="text-align:center">'+(l.calls_count||1)+'</td>'
        +'<td style="font-size:11.5px;color:var(--t2)">'+ds+'</td>'
        +'<td>'+(l.dismissed
            ? '<span class="badge" style="background:var(--s3);color:var(--t3)">\u0412\u0438\u0434\u0430\u043b\u0435\u043d\u043e</span>'
            : '<span class="badge '+st[0]+'">'+st[1]+'</span>')+'</td>'
        +'<td style="font-size:11.5px;color:var(--t2);max-width:200px">'+(l.note||'\u2014')+'</td>'
        +'<td><div style="display:flex;gap:4px;justify-content:flex-end">'
          +(l.dismissed
            ? '<button class="btn btn-g btn-sm" onclick="leadRestore(\''+l.id+'\')" title="\u041f\u043e\u0432\u0435\u0440\u043d\u0443\u0442\u0438">\u21b6</button>'
            : '<button class="btn btn-g btn-sm" onclick="leadSetStatus(\''+l.id+'\',\'contacted\')" title="\u0412\u0437\u044f\u0442\u0438 \u0432 \u0440\u043e\u0431\u043e\u0442\u0443">\ud83d\udcde</button>'
             +'<button class="btn btn-p btn-sm" onclick="leadToStudent(\''+l.id+'\')" title="\u0421\u0442\u0432\u043e\u0440\u0438\u0442\u0438 \u0443\u0447\u043d\u044f">\ud83d\udc64+</button>'
             +'<button class="btn btn-sm" style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger)" onclick="leadDismiss(\''+l.id+'\')" title="\u041f\u0440\u0438\u0431\u0440\u0430\u0442\u0438">\ud83d\uddd1</button>')
        +'</div></td>'
      +'</tr>';
    }).join('')
    +'</tbody></table></div>';
}
window.renderLeads=renderLeads;

/** Прибирає ліда зі списку. Дзвінки в телефонії лишаються. */
async function leadDismiss(id){
  if(!confirm('\u041f\u0440\u0438\u0431\u0440\u0430\u0442\u0438 \u0446\u0435\u0439 \u043b\u0456\u0434?\n\n'
    +'\u0416\u0443\u0440\u043d\u0430\u043b \u0434\u0437\u0432\u0456\u043d\u043a\u0456\u0432 \u041d\u0415 \u0437\u043c\u0456\u043d\u0438\u0442\u044c\u0441\u044f. '
    +'\u041f\u0440\u0438 \u043d\u0430\u0441\u0442\u0443\u043f\u043d\u0456\u0439 \u0441\u0438\u043d\u0445\u0440\u043e\u043d\u0456\u0437\u0430\u0446\u0456\u0457 \u0432\u0456\u043d \u041d\u0415 \u0437\u2019\u044f\u0432\u0438\u0442\u044c\u0441\u044f \u0437\u043d\u043e\u0432\u0443.')) return;
  try{
    await _sb.from('phone_leads').update({dismissed:true, dismissed_at:new Date().toISOString(),
      updated_at:new Date().toISOString()}).eq('id',id);
    mkToast('\u041b\u0456\u0434 \u043f\u0440\u0438\u0431\u0440\u0430\u043d\u043e');
    renderLeads();
  }catch(e){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(e.message||e),'error'); }
}
window.leadDismiss=leadDismiss;

async function leadRestore(id){
  try{
    await _sb.from('phone_leads').update({dismissed:false, dismissed_at:null,
      updated_at:new Date().toISOString()}).eq('id',id);
    mkToast('\u041b\u0456\u0434 \u043f\u043e\u0432\u0435\u0440\u043d\u0443\u0442\u043e');
    renderLeads();
  }catch(e){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(e.message||e),'error'); }
}
window.leadRestore=leadRestore;

async function leadSetStatus(id, st){
  try{
    await _sb.from('phone_leads').update({status:st, updated_at:new Date().toISOString()}).eq('id',id);
    renderLeads();
  }catch(e){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(e.message||e),'error'); }
}
window.leadSetStatus=leadSetStatus;

/** Створює картку учня з ліда — вручну, за рішенням адміністратора */
async function leadToStudent(id){
  try{
    var r=await _sb.from('phone_leads').select('*').eq('id',id).limit(1);
    var lead=(r.data||[])[0];
    if(!lead) return;
    var nm=prompt('\u0406\u043c\u2019\u044f \u0443\u0447\u043d\u044f:', lead.name||'');
    if(nm===null) return;
    nm=String(nm).trim();
    if(!nm){ mkToast('\u0406\u043c\u2019\u044f \u043e\u0431\u043e\u0432\u2019\u044f\u0437\u043a\u043e\u0432\u0435','error'); return; }
    var parts=nm.split(/\s+/);
    var sid=uid();
    await dbInsert('students',{ id:sid, fn:parts[0], ln:parts.slice(1).join(' ')||'',
      phone:lead.phone, status:'trial', crm_stage:'trial', src:'internet' });
    await _sb.from('phone_leads').update({status:'converted', student_id:sid,
      updated_at:new Date().toISOString()}).eq('id',id);
    // Прив'язуємо минулі дзвінки цього номера до нової картки
    try{
      var tail=String(lead.phone||'').replace(/\D/g,'').slice(-10);
      var cl=await _sb.from('call_logs').select('id,caller_phone,callee_phone');
      (cl.data||[]).forEach(async function(c){
        var a=String(c.caller_phone||'').replace(/\D/g,'').slice(-10);
        var b=String(c.callee_phone||'').replace(/\D/g,'').slice(-10);
        if(a===tail||b===tail) await _sb.from('call_logs').update({student_id:sid}).eq('id',c.id);
      });
    }catch(e){}
    mkToast('\u2705 \u0423\u0447\u043d\u044f \u0441\u0442\u0432\u043e\u0440\u0435\u043d\u043e');
    renderLeads();
  }catch(e){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(e.message||e),'error'); }
}
window.leadToStudent=leadToStudent;

function renderTelephony(){
  // Access guard — дзвінки може слухати бог/директор/адмін; НАЛАШТУВАННЯ (нижче) — лише бог
  var r = R();
  if(r !== 'god' && r !== 'director' && r !== 'admin'){
    document.getElementById('pg-telephony').innerHTML =
      '<div class="empty" style="padding:60px"><div class="ei">🔒</div>Доступ лише для адміністраторів та директора</div>';
    return;
  }

  // Кнопка "⚙ Налаштування" (провайдер, API-ключі, вебхук) — виключно для бога системи
  var canSettings = r==='god';
  var settingsBtn = document.getElementById('tel-settings-toggle-btn');
  if(settingsBtn) settingsBtn.style.display = canSettings ? '' : 'none';
  var syncHint = document.getElementById('tel-sync-hint');
  if(syncHint) syncHint.style.display = canSettings ? 'block' : 'none';
  var diagBtn = document.getElementById('tel-diag-btn');
  if(diagBtn) diagBtn.style.display = canSettings ? '' : 'none';

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
  if(whEl) whEl.value = _wUrls[cfg.provider]||'';

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

  // Оновлюємо саме значення webhook URL під ЩОЙНО обраного провайдера — раніше це
  // поле оновлювалось лише один раз при відкритті модалки (за старим збереженим
  // провайдером), тому вибір "Київстар" не оновлював URL, доки не збережеш і не
  // відкриєш налаштування знову.
  var _wUrls={kyivstar:'https://rndxbvwisppxnhvrzwqi.supabase.co/functions/v1/kyivstar-webhook',zadarma:'https://rndxbvwisppxnhvrzwqi.supabase.co/functions/v1/zadarma-webhook'};
  var whEl = document.getElementById('tel-webhook-url');
  if(whEl) whEl.value = _wUrls[prov] || '';

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
  if(R()!=='god'){ mkToast('\u041d\u0430\u043b\u0430\u0448\u0442\u0443\u0432\u0430\u043d\u043d\u044f \u0442\u0435\u043b\u0435\u0444\u043e\u043d\u0456\u0457 \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0456 \u043b\u0438\u0448\u0435 \u0431\u043e\u0433\u0443 \u0441\u0438\u0441\u0442\u0435\u043c\u0438','error'); return; }
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
  if(R()!=='god'){ mkToast('\u041d\u0430\u043b\u0430\u0448\u0442\u0443\u0432\u0430\u043d\u043d\u044f \u0442\u0435\u043b\u0435\u0444\u043e\u043d\u0456\u0457 \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0456 \u043b\u0438\u0448\u0435 \u0431\u043e\u0433\u0443 \u0441\u0438\u0441\u0442\u0435\u043c\u0438','error'); return; }
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
  if(R()!=='god'){ mkToast('\u041d\u0430\u043b\u0430\u0448\u0442\u0443\u0432\u0430\u043d\u043d\u044f \u0442\u0435\u043b\u0435\u0444\u043e\u043d\u0456\u0457 \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0456 \u043b\u0438\u0448\u0435 \u0431\u043e\u0433\u0443 \u0441\u0438\u0441\u0442\u0435\u043c\u0438','error'); return; }
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
  if(whEl) whEl.value=_wUrls[cfg.provider]||'';
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

// Завантажує запис розмови через серверну функцію (браузер не може
// звертатись до API Київстару напряму — там секретний токен) і одразу
// підставляє програвач у ту саму клітинку таблиці.
// Запускає серверну синхронізацію з Київстаром: підтягує реальну тривалість,
// record_id (для записів розмов), час дзвінка до відповіді та код завершення.
// Показує діагностику, якщо базову адресу чи спосіб авторизації ще не визначено.
// Діагностика зʼєднання з API Київстару. Показує, чи Supabase взагалі
// може достукатись до їхніх серверів, чи проблема в токені/параметрах.
async function diagnoseKyivstar(){
  if(R()!=='god'){ mkToast('\u0414\u043e\u0441\u0442\u0443\u043f \u043b\u0438\u0448\u0435 \u0434\u043b\u044f \u0431\u043e\u0433\u0430','error'); return; }
  mkToast('\u23f3 \u041f\u0435\u0440\u0435\u0432\u0456\u0440\u043a\u0430 \u0437\u2019\u0454\u0434\u043d\u0430\u043d\u043d\u044f\u2026');
  try{
    var _sess=await _sb.auth.getSession();
    var jwt=_sess?.data?.session?.access_token;
    var res=await fetch('https://rndxbvwisppxnhvrzwqi.supabase.co/functions/v1/sync-call-history',{
      method:'POST',
      headers:{'Content-Type':'application/json','Authorization':'Bearer '+jwt},
      body:JSON.stringify({diagnose:true})
    });
    var data=await res.json();
    console.log('[\u0434\u0456\u0430\u0433\u043d\u043e\u0441\u0442\u0438\u043a\u0430]', data);
    var txt=(data.diagnostics||[data.error||'\u043d\u0435\u043c\u0430\u0454 \u0434\u0430\u043d\u0438\u0445']).join('\n');
    alert('\u0414\u0456\u0430\u0433\u043d\u043e\u0441\u0442\u0438\u043a\u0430 \u0437\u2019\u0454\u0434\u043d\u0430\u043d\u043d\u044f \u0437 \u041a\u0438\u0457\u0432\u0441\u0442\u0430\u0440\u043e\u043c:\n\n'+txt);
  }catch(e){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(e.message||e),'error'); }
}
window.diagnoseKyivstar=diagnoseKyivstar;

async function syncCallHistory(){
  if(R()!=='god'){ mkToast('\u0414\u043e\u0441\u0442\u0443\u043f \u043b\u0438\u0448\u0435 \u0434\u043b\u044f \u0431\u043e\u0433\u0430 \u0441\u0438\u0441\u0442\u0435\u043c\u0438','error'); return; }
  var btn=document.getElementById('tel-sync-btn');
  if(btn){ btn.disabled=true; btn.textContent='\u23f3 \u0421\u0438\u043d\u0445\u0440\u043e\u043d\u0456\u0437\u0430\u0446\u0456\u044f\u2026'; }
  try{
    var _sess=await _sb.auth.getSession();
    var jwt=_sess?.data?.session?.access_token;
    var res=await fetch('https://rndxbvwisppxnhvrzwqi.supabase.co/functions/v1/sync-call-history',{
      method:'POST',
      headers:{'Content-Type':'application/json','Authorization':'Bearer '+jwt},
      body:JSON.stringify({hours:24})
    });
    var data=await res.json();
    console.log('[sync-call-history] \u0432\u0456\u0434\u043f\u043e\u0432\u0456\u0434\u044c:', data);

    if(!res.ok){
      // Показуємо докладну діагностику — які адреси й способи авторизації пробувались
      var att=((data.attempts||[]).concat(data.log||[])).join('\n')||'(\u043f\u043e\u0440\u043e\u0436\u043d\u044c\u043e)';
      alert('\u041d\u0435 \u0432\u0434\u0430\u043b\u043e\u0441\u044f \u0437\u0432\u0435\u0440\u043d\u0443\u0442\u0438\u0441\u044c \u0434\u043e API \u041a\u0438\u0457\u0432\u0441\u0442\u0430\u0440\u0443.\n\n'
        +(data.error||'')+'\n\n'+(data.hint||'')+'\n\n\u0421\u043f\u0440\u043e\u0431\u0438:\n'+att);
      throw new Error(data.error||'\u041f\u043e\u043c\u0438\u043b\u043a\u0430 \u0441\u0435\u0440\u0432\u0435\u0440\u0430');
    }

    mkToast('\u2705 \u041e\u043d\u043e\u0432\u043b\u0435\u043d\u043e '+(data.updated||0)+' \u0434\u0437\u0432\u0456\u043d\u043a\u0456\u0432, \u0456\u0437 \u0437\u0430\u043f\u0438\u0441\u043e\u043c: '+(data.withRecording||0));
    if(data.connectivity) alert('\u0414\u0456\u0430\u0433\u043d\u043e\u0441\u0442\u0438\u043a\u0430 \u0437\u0432\'\u044f\u0437\u043a\u0443:\n\n'+data.connectivity+'\n\n\u0416\u0443\u0440\u043d\u0430\u043b:\n'+((data.log||[]).join('\n')));
    // Підказуємо, які значення варто зберегти секретами, щоб не перебирати щоразу
    if(data.apiBase && data.authMode){
      console.log('%c[\u041a\u0438\u0457\u0432\u0441\u0442\u0430\u0440] \u0417\u0431\u0435\u0440\u0435\u0436\u0456\u0442\u044c \u0441\u0435\u043a\u0440\u0435\u0442\u0430\u043c\u0438: KYIVSTAR_API_BASE='+data.apiBase+'  KYIVSTAR_AUTH_MODE='+data.authMode,'font-weight:bold;color:#22c55e');
    }
    await renderTelLog();
  }catch(e){
    mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(e.message||e),'error');
  }finally{
    if(btn){ btn.disabled=false; btn.textContent='\ud83d\udd04 \u0421\u0438\u043d\u0445\u0440\u043e\u043d\u0456\u0437\u0443\u0432\u0430\u0442\u0438 \u0437\u0430\u043f\u0438\u0441\u0438'; }
  }
}
window.syncCallHistory=syncCallHistory;

/** Перемикає швидкість відтворення запису: 1× → 1.5× → 2× → 1× */
function telSpeed(id, btn){
  var a=document.getElementById('aud-'+id);
  if(!a) return;
  var steps=[1,1.5,2];
  var cur=steps.indexOf(a.playbackRate);
  var next=steps[(cur+1)%steps.length];
  a.playbackRate=next;
  btn.textContent=next+'\u00d7';
}
window.telSpeed=telSpeed;

async function playCallRecord(callLogId){
  var cell=document.getElementById('rec-cell-'+callLogId);
  if(cell) cell.innerHTML='<span style="font-size:11px;color:var(--t3)">\u23f3\u2026</span>';
  try{
    // Знаходимо шлях до вже завантаженого файлу в нашому сховищі.
    // Напряму до Київстару не звертаємось: по-перше, там потрібен секретний
    // токен, по-друге, сервери Supabase не мають до нього мережевого доступу.
    var _r=await _sb.from('call_logs').select('*').eq('id',callLogId).limit(1);
    var row=(_r.data||[])[0];
    if(!row||!row.recording_path)
      throw new Error('\u0417\u0430\u043f\u0438\u0441 \u0449\u0435 \u043d\u0435 \u0437\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0435\u043d\u043e. \u0417\u0430\u043f\u0443\u0441\u0442\u0456\u0442\u044c konstanta-sync.js');

    var _s=await _sb.storage.from('call-records').createSignedUrl(row.recording_path,3600);
    if(_s.error||!_s.data?.signedUrl) throw new Error(_s.error?.message||'\u041d\u0435 \u0432\u0434\u0430\u043b\u043e\u0441\u044f \u043e\u0442\u0440\u0438\u043c\u0430\u0442\u0438 \u043f\u043e\u0441\u0438\u043b\u0430\u043d\u043d\u044f');
    // Завантажуємо файл ПОВНІСТЮ в памʼять браузера і віддаємо програвачу
    // локальне посилання. Це єдиний спосіб зробити перемотування миттєвим:
    // при потоковому відтворенні браузер має щоразу довантажувати шматки з
    // сервера, і повзунок або не рухається, або стрибає з великою затримкою.
    if(cell) cell.innerHTML='<span style="font-size:11px;color:var(--t3)">\u23f3 \u0437\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0435\u043d\u043d\u044f\u2026</span>';
    var resp=await fetch(_s.data.signedUrl);
    if(!resp.ok) throw new Error('HTTP '+resp.status);
    var blob=await resp.blob();
    var localUrl=URL.createObjectURL(blob);

    if(cell) cell.innerHTML=
       '<div style="display:flex;align-items:center;gap:5px">'
      +'<audio id="aud-'+callLogId+'" controls autoplay preload="auto" controlsList="nodownload">'
        +'<source src="'+localUrl+'" type="audio/mpeg"></audio>'
      +'<button class="btn btn-g btn-sm" style="padding:2px 6px;font-size:10px" '
        +'onclick="telSpeed(\''+callLogId+'\',this)" title="\u0428\u0432\u0438\u0434\u043a\u0456\u0441\u0442\u044c \u0432\u0456\u0434\u0442\u0432\u043e\u0440\u0435\u043d\u043d\u044f">1\u00d7</button>'
      +'</div>';
  }catch(e){
    if(cell) cell.innerHTML='<span style="font-size:11px;color:var(--danger)">\u274c</span>';
    mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(e.message||e),'error');
  }
}
window.playCallRecord=playCallRecord;

// Поточне сортування журналу дзвінків
var telSort = { col:'date', dir:'desc' };

function telSetSort(col){
  if(telSort.col===col) telSort.dir = telSort.dir==='desc' ? 'asc' : 'desc';
  else { telSort.col=col; telSort.dir='desc'; }
  renderTelLog();
}
window.telSetSort = telSetSort;

async function renderTelLog(){
  var body = document.getElementById('tel-log-body');
  if(!body) return;
  var typeF   = (document.getElementById('tel-log-filter')||{value:''}).value;
  var periodF = (document.getElementById('tel-period')||{value:''}).value;
  var q       = ((document.getElementById('tel-search')||{value:''}).value||'').toLowerCase().trim();

  body.innerHTML = skeleton('rows',5);

  // Напрямок фільтруємо на сервері, решту — локально
  var srvFilter = (typeF==='inbound'||typeF==='outbound') ? typeF : null;
  var log = await telGetLog(srvFilter);

  try{ await telAutoCreateLeads(log); }catch(e){ console.warn('[telAutoCreateLeads]', e); }

  // ── Фільтрація ──
  var now = Date.now();
  var rows = (log||[]).filter(function(e){
    var isMissed = e.status==='missed' || (!e.duration && e.status!=='alerting');
    if(typeF==='missed' && !isMissed) return false;
    if(typeF==='hasrec' && !e.recording_path && !e.recording_url && !e.record_url) return false;

    if(periodF){
      var t = new Date(e.created_at||0).getTime();
      if(periodF==='today'){
        var d0=new Date(); d0.setHours(0,0,0,0);
        if(t < d0.getTime()) return false;
      } else {
        if(now - t > parseInt(periodF)*86400000) return false;
      }
    }

    if(q){
      var sid=e.studentId||e.student_id;
      var st=sid?(S.students||[]).find(function(x){return x.id===sid;}):null;
      var hay=[(e.caller_phone||''),(e.callee_phone||''),(e.phone||''),(e.caller_name||''),
               st?(st.fn+' '+st.ln):''].join(' ').toLowerCase();
      if(hay.indexOf(q)<0) return false;
    }
    return true;
  });

  // ── Сортування ──
  rows.sort(function(a,b){
    var v;
    if(telSort.col==='dur')      v=(parseInt(a.duration)||0)-(parseInt(b.duration)||0);
    else if(telSort.col==='who'){
      var nm=function(e){
        var sid=e.studentId||e.student_id;
        var st=sid?(S.students||[]).find(function(x){return x.id===sid;}):null;
        return (st?st.fn+' '+st.ln:(e.caller_name||'\uffff')).toLowerCase();
      };
      v=nm(a).localeCompare(nm(b),'uk');
    }
    else if(telSort.col==='type') v=String(a.direction||'').localeCompare(String(b.direction||''));
    else v=new Date(a.created_at||0)-new Date(b.created_at||0);
    return telSort.dir==='desc' ? -v : v;
  });

  // ── Зведення ──
  var sum=document.getElementById('tel-summary');
  if(sum){
    var inC=0,outC=0,missC=0,totalSec=0,recC=0;
    rows.forEach(function(e){
      var isMissed = e.status==='missed' || (!e.duration && e.status!=='alerting');
      if(isMissed) missC++;
      else if(String(e.direction)==='outbound') outC++;
      else inC++;
      totalSec += parseInt(e.duration)||0;
      if(e.recording_path) recC++;
    });
    var mins=Math.floor(totalSec/60);
    sum.innerHTML =
       '<span class="tel-sum-item">\ud83d\udcca \u0423\u0441\u044c\u043e\u0433\u043e <b>'+rows.length+'</b></span>'
      +'<span class="tel-sum-item" style="color:#16a34a">\ud83d\udce5 \u0412\u0445\u0456\u0434\u043d\u0456 <b>'+inC+'</b></span>'
      +'<span class="tel-sum-item" style="color:#2563eb">\ud83d\udce4 \u0412\u0438\u0445\u0456\u0434\u043d\u0456 <b>'+outC+'</b></span>'
      +'<span class="tel-sum-item" style="color:#dc2626">\u274c \u041f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u0456 <b>'+missC+'</b></span>'
      +'<span class="tel-sum-item">\u23f1 \u0420\u043e\u0437\u043c\u043e\u0432 <b>'+mins+'</b> \u0445\u0432</span>'
      +'<span class="tel-sum-item">\ud83c\udfa7 \u0417\u0430\u043f\u0438\u0441\u0456\u0432 <b>'+recC+'</b></span>';
  }

  if(!rows.length){
    body.innerHTML='<div class="tel-empty"><div style="font-size:34px;margin-bottom:8px">\ud83d\udcde</div>'
      +((q||typeF||periodF)
        ? '\u0417\u0430 \u0446\u0438\u043c\u0438 \u0444\u0456\u043b\u044c\u0442\u0440\u0430\u043c\u0438 \u0434\u0437\u0432\u0456\u043d\u043a\u0456\u0432 \u043d\u0435\u043c\u0430\u0454'
        : '\u0416\u0443\u0440\u043d\u0430\u043b \u0434\u0437\u0432\u0456\u043d\u043a\u0456\u0432 \u043f\u043e\u0440\u043e\u0436\u043d\u0456\u0439')+'</div>';
    return;
  }

  // ── Заголовок таблиці із сортуванням ──
  function th(col,label,extra){
    var on = telSort.col===col;
    var ico = on ? (telSort.dir==='desc'?'\u25bc':'\u25b2') : '\u21c5';
    return '<th class="sortable'+(on?' sorted':'')+'" onclick="telSetSort(\''+col+'\')"'+(extra||'')+'>'
      +label+'<span class="sort-ico">'+ico+'</span></th>';
  }

  var html='<div style="overflow-x:auto;max-height:70vh"><table class="tel-tbl"><thead><tr>'
    +th('type','\u0422\u0438\u043f')
    +'<th>\u0417\u0432\u0456\u0434\u043a\u0438</th><th>\u041a\u0443\u0434\u0438</th>'
    +th('who','\u0423\u0447\u0435\u043d\u044c / \u043a\u043b\u0456\u0454\u043d\u0442')
    +th('dur','\u0422\u0440\u0438\u0432\u0430\u043b\u0456\u0441\u0442\u044c')
    +th('date','\u0414\u0430\u0442\u0430')
    +'<th>\u0417\u0430\u043f\u0438\u0441</th><th></th></tr></thead><tbody>';

  html += rows.map(function(e){
    var sid=e.studentId||e.student_id;
    var st=sid?(S.students||[]).find(function(x){return x.id===sid;}):null;
    var isMissed = e.status==='missed' || (!e.duration && e.status!=='alerting');
    var isOut = String(e.direction)==='outbound';

    var dirHtml = isMissed
      ? '<span class="tel-dir tel-dir-miss">\u274c \u041f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u0438\u0439</span>'
      : (isOut ? '<span class="tel-dir tel-dir-out">\ud83d\udce4 \u0412\u0438\u0445\u0456\u0434\u043d\u0438\u0439</span>'
               : '<span class="tel-dir tel-dir-in">\ud83d\udce5 \u0412\u0445\u0456\u0434\u043d\u0438\u0439</span>');

    // Розділяємо номери: хто дзвонив і кому дзвонили.
    // Для вхідного «звідки» — номер клієнта, «куди» — номер центру.
    var fromPhone = e.caller_phone||e.phone||'';
    var toPhone   = e.callee_phone||'';
    var phone = fromPhone||toPhone;
    var secs = parseInt(e.duration)||0;
    var dur = secs ? (Math.floor(secs/60)+'\u0445\u0432 '+String(secs%60).padStart(2,'0')+'\u0441') : '\u2014';

    var d=new Date(e.created_at||0);
    var dateHtml = isNaN(d.getTime()) ? '\u2014'
      : '<span class="tel-date"><b>'+String(d.getDate()).padStart(2,'0')+'.'+String(d.getMonth()+1).padStart(2,'0')+'.'+String(d.getFullYear()).slice(2)+'</b>'
        +String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0')+'</span>';

    var whoTxt = st ? (st.fn+' '+st.ln) : (e.caller_name||'\u2014');
    var isLead = !st && /\u041d\u043e\u0432\u0438\u0439 \u043b\u0456\u0434|\u0414\u0437\u0432\u0456\u043d\u043e\u043a/.test(whoTxt);
    var whoHtml = st
      ? '<span class="tel-who">'+whoTxt+'</span>'
      : '<span class="tel-who'+(isLead?' tel-who-lead':'')+'">'+whoTxt+'</span>';

    var recUrl = e.recording_url || e.record_url || e.recordUrl;
    // preload="metadata" — щоб браузер одразу знав тривалість і повзунок
    // перемотування працював без повного завантаження файлу
    var recHtml = recUrl
      ? '<audio controls preload="metadata" controlsList="nodownload"><source src="'+recUrl+'" type="audio/mpeg"></audio>'
      : (e.recording_path
        ? '<button class="btn btn-g btn-sm" onclick="playCallRecord(\''+e.id+'\')">\ud83c\udfa7 \u0421\u043b\u0443\u0445\u0430\u0442\u0438</button>'
        : ((e.record_id||e.recordId)
          ? '<span style="color:var(--t3);font-size:11px" title="\u0417\u0430\u043f\u0443\u0441\u0442\u0456\u0442\u044c konstanta-sync.js">\u23f3 \u043d\u0435 \u0437\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0435\u043d\u043e</span>'
          : '<span style="color:var(--t3);font-size:11px">\u2014</span>'));

    return '<tr class="'+(isMissed?'row-miss':(isOut?'row-out':'row-in'))+'">'
      +'<td>'+dirHtml+'</td>'
      +'<td>'+(fromPhone
          ? '<a class="tel-num" href="tel:'+fromPhone+'">'+fromPhone+'</a>'
          : '<span style="color:var(--t3)">\u2014</span>')+'</td>'
      +'<td>'+(toPhone
          ? '<a class="tel-num" href="tel:'+toPhone+'">'+toPhone+'</a>'
          : '<span style="color:var(--t3)">\u2014</span>')+'</td>'
      +'<td>'+whoHtml+'</td>'
      +'<td class="tel-dur"'+(isMissed?' style="color:var(--t3)"':'')+'>'+dur+'</td>'
      +'<td>'+dateHtml+'</td>'
      +'<td id="rec-cell-'+e.id+'">'+recHtml+'</td>'
      +'<td style="text-align:right">'
        +(sid ? '<button class="btn btn-g btn-sm" onclick="openStudM(\''+sid+'\')" title="\u041a\u0430\u0440\u0442\u043a\u0430 \u0443\u0447\u043d\u044f">\ud83d\udc64</button>'
              : '<button class="btn btn-g btn-sm" onclick="telLinkStudent(\''+e.id+'\',\''+phone+'\')" title="\u041f\u0440\u0438\u0432\'\u044f\u0437\u0430\u0442\u0438 \u0434\u043e \u0443\u0447\u043d\u044f">\ud83d\udd17</button>')
      +'</td></tr>';
  }).join('');

  html += '</tbody></table></div>';
  body.innerHTML = html;
}


// Автоматичне створення нового ліда в CRM з дзвінка від невідомого номера.
// Раніше перемикач "Автостворення ліда" в налаштуваннях телефонії існував
// в інтерфейсі, але ніде не використовувався — дзвінки ніяк не потрапляли в CRM.
async function telAutoCreateLeads(log){
  var cfg = telGetSettings();
  if(cfg.autolead===false) return; // перемикач вимкнено — нічого не робимо
  if(!log||!log.length) return;
  function tail10(v){ return String(v||'').replace(/\D/g,'').slice(-10); }

  // Номери НАЯВНИХ учнів/лідів у картках — такі дзвінки не потрапляють у ліди
  var studentByTail={};
  (S.students||[]).forEach(function(s){
    var a=tail10(s.phone); if(a && !studentByTail[a]) studentByTail[a]=s;
    var b=tail10(s.parentPhone||s.parent_phone); if(b && !studentByTail[b]) studentByTail[b]=s;
  });
  // Ігноровані номери (видалені ліди) — не створюємо знову
  var ignSet={};
  try{ var _ign=await _sb.from('ignored_phones').select('phone'); (_ign.data||[]).forEach(function(x){var t=tail10(x.phone);if(t)ignSet[t]=1;}); }catch(e){}
  // Наявні ліди у phone_leads — щоб не дублювати
  var leadByTail={};
  try{ var _lr=await _sb.from('phone_leads').select('id,phone'); (_lr.data||[]).forEach(function(x){var t=tail10(x.phone);if(t)leadByTail[t]=1;}); }catch(e){}

  var created=0;
  for(var i=0;i<log.length;i++){
    var e=log[i];
    // Номер КЛІЄНТА: для вихідного — кому дзвонили, для вхідного — хто дзвонив
    var phone=(((e.direction==='outbound')?(e.callee_phone||e.callee||''):(e.caller_phone||''))||e.phone||'').trim();
    var tail=tail10(phone);
    if(!tail || tail.length<7) continue; // некоректний/надто короткий номер

    // Якщо номер уже належить учню/ліду в картках — прив'язуємо дзвінок до нього, лід НЕ створюємо
    if(studentByTail[tail]){
      if(!(e.student_id||e.studentId)){
        var s=studentByTail[tail];
        try{ await _sb.from('call_logs').update({student_id:s.id, caller_name:(s.fn+' '+s.ln).trim()}).eq('id',e.id); e.student_id=s.id; e.caller_name=(s.fn+' '+s.ln).trim(); }catch(_){}
      }
      continue;
    }
    if(ignSet[tail]) continue;      // видалений/ігнорований номер
    if(leadByTail[tail]) continue;  // лід із цим номером уже є в інбоксі
    if(window._phoneLeadInsertBlocked) continue; // вставка недоступна (RLS) — не спамимо

    // Новий номер — створюємо ЛІД у phone_leads (інбокс "Ліди з дзвінків"), НЕ учня
    var callTime=e.call_time||e.started_at||e.created_at||e.last_call||new Date().toISOString();
    var _ins=null;
    try{ _ins = await _sb.from('phone_leads').insert({ phone:phone, phone_key:tail, status:'new', calls_count:1, last_call:callTime }); }
    catch(ex){ _ins = { error: ex }; }
    if(_ins && _ins.error){
      // Вставку відхилено (найімовірніше немає RLS-політики INSERT для phone_leads).
      // Зупиняємось, щоб НЕ засипати консоль сотнями однакових помилок.
      window._phoneLeadInsertBlocked = true;
      console.warn('[telAutoCreateLeads] phone_leads INSERT відхилено (перевірте RLS-політику). Зупинено, щоб не спамити.', _ins.error);
      continue;
    }
    leadByTail[tail]=1; created++;
  }
  if(created && S.currentPage==='leads'){ try{ renderLeads(); }catch(e){} }
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
