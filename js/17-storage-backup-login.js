
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
  // ── TRAINING MODE: ігрова пісочниця для нових репетиторів (тестові дані, ніщо не пише в Supabase) ──
  var _isTraining = window.location.search.includes('training=1');
  if(_isTraining){ initTrainingMode(); return; }
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
    S.users        = (_d.profiles||[]).map(normalizeUser);
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
    buildSidebar();
  try{ buildBottomNav(); }catch(e){} updateSBUser(); updateBranchSelector();
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
    var _ok = await loadProfile(session.user);
    hideLoading();
    if(_ok) startApp();
  } else {
    hideLoading();
    if(lsEl) lsEl.style.display = 'flex';
  }

  _sb.auth.onAuthStateChange(async function(event, session){
    if(event === 'SIGNED_IN' && session){
      var _ok2 = await loadProfile(session.user);
      hideLoading();
      if(_ok2) startApp();
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
  if(data && data.disabled){
    // Доступ вимкнено (звільнений/заблокований) — виходимо, у застосунок не пускаємо.
    try{ await _sb.auth.signOut(); }catch(e){}
    CU=null;
    try{ showAccessDisabled(); }catch(e){}
    return false;
  }
  if(data){ CU = normalizeUser(data); }
  else {
    const np = { id:authUser.id, email:authUser.email,
      fn: authUser.email.split('@')[0], ln:'', role:'tutor', perms:{} };
    await _sb.from('profiles').insert(np);
    CU = normalizeUser(np);
  }
  return true;
}
function showAccessDisabled(){
  var asEl=document.getElementById('as'); if(asEl) asEl.style.display='none';
  var lsEl=document.getElementById('ls'); if(lsEl) lsEl.style.display='flex';
  try{ mkToast('\u0414\u043e\u0441\u0442\u0443\u043f \u0432\u0438\u043c\u043a\u043d\u0435\u043d\u043e. \u0417\u0432\u0435\u0440\u043d\u0456\u0442\u044c\u0441\u044f \u0434\u043e \u0430\u0434\u043c\u0456\u043d\u0456\u0441\u0442\u0440\u0430\u0442\u043e\u0440\u0430.','error'); }catch(e){}
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
