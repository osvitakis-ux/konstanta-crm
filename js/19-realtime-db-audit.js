
// =
// REALTIME
// =
function startChannels(){
  if(window._channelsStarted) return; window._channelsStarted=true;
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
  // Дані прийшли — прибираємо скелетон першого завантаження
  try{ var _bs=document.getElementById('boot-skeleton'); if(_bs) _bs.remove(); }catch(e){}
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

// Дебаунс фонового оновлення таблиці: якщо кілька insert/update/delete
// по одній таблиці відбуваються швидко поспіль (напр. видалення серії
// занять), замість N окремих мережевих запитів робимо один.
var _refreshTimers={};
function scheduleTableRefresh(table, delay){
  if(_refreshTimers[table]) clearTimeout(_refreshTimers[table]);
  _refreshTimers[table]=setTimeout(function(){
    delete _refreshTimers[table];
    loadTableFresh(table);
  }, delay);
}

var _loadingFresh={};
async function loadTableFresh(table){
  if(_loadingFresh[table]){ _loadingFresh[table]='again'; return; }   // вже вантажиться — не паралелимо
  _loadingFresh[table]='run';
  try{ await _loadTableFreshCore(table); }
  finally{ var _ag=(_loadingFresh[table]==='again'); _loadingFresh[table]=null; if(_ag) scheduleTableRefresh(table,300); }
}
async function _loadTableFreshCore(table){
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
  // Посторінкове читання — та сама причина, що й у loadAll: без цього
  // фонове оновлення обрізало б таблицю на 1000 записах і "загубило" решту.
  var PAGE=1000, offset=0, data=[], res=null;
  while(true){
    res=null;
    for(var attempt=0; attempt<4; attempt++){
      res = await _sb.from(table).select('*').range(offset, offset + PAGE - 1);
      if(!res.error) break;
      if(await refreshIfExpired(res.error)) continue;                    // JWT — оновили сесію, повтор
      await new Promise(function(r){ setTimeout(r, 400*(attempt+1)); }); // тимчасова помилка — пауза й повтор
    }
    if(res.error) return;                                                // після кількох спроб — не чіпаємо локальні дані
    var batch = res.data || [];
    data = data.concat(batch);
    if(batch.length === 0) break;   // рядків більше немає
    offset += batch.length;         // рухаємось за фактичною кількістю
    if(offset > 500000) break;
  }
  var fresh = norm[key] ? data.map(norm[key]) : data;
  // Об'єднуємо замість повної заміни: якщо щойно збережений запис ще не встиг
  // "доїхати" до цього select (мережева затримка/кешування), не втрачаємо його —
  // інакше він на мить з'являється (з локального оптимістичного оновлення)
  // і одразу зникає, коли ця фонова синхронізація перезаписує масив без нього.
  var freshIds={}; fresh.forEach(function(r){ if(r&&r.id!=null) freshIds[r.id]=true; });
  var kept=(S[key]||[]).filter(function(r){ return r&&r.id!=null && !freshIds[r.id]; });
  // Злиття + дедуп за id: свіжі дані з БД — пріоритетні, дублікатів не лишається
  var _merged=fresh.concat(kept), _seen={}, _out=[];
  _merged.forEach(function(r){ if(r&&r.id!=null){ if(_seen[r.id]) return; _seen[r.id]=true; } _out.push(r); });
  S[key] = _out;
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
    var rt = localStorage.getItem('sb_refresh');
    if(!rt) return;
    var expAt = parseInt(localStorage.getItem('sb_expat')||'0',10)*1000; // сек → мс
    // Оновлюємо проактивно, поки токен ще живий (за 10 хв до завершення або якщо час невідомий)
    if(!expAt || (expAt - Date.now() < 10*60*1000)){
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
    if(table==='payroll_items'){ return (data.label||'')+(data.amount!=null?' '+data.amount+'\u20B4':''); }
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
  if(window._trainingMode){
    var _tk=({students:'students',lessons:'lessons',payments:'payments',comms:'comms',tutors:'tutors',subjects:'subjects',tasks:'tasks',payroll_items:'payrollItems'})[table];
    var _normFnsIns={students:normalizeStudent,lessons:normalizeLesson,payments:normalizePayment,tutors:normalizeTutor,comms:normalizeComm,tasks:normalizeTask,payroll_items:normalizePayrollItem};
    if(_tk){
      var _normed = _normFnsIns[table] ? _normFnsIns[table](data) : data;
      S[_tk]=(S[_tk]||[]).concat([_normed]);
      refreshPage(_tk);
      if(S.currentPage==='schedule' && typeof renderSch==='function') renderSch();
    }
    setTimeout(function(){ try{trainingCheckProgress&&trainingCheckProgress();}catch(e){} }, 50);
    return;
  }
  setSaving();
  try{
    var _ri = await _sb.from(table).insert(data); var error = _ri.error;
    if(error && await refreshIfExpired(error)){
      _ri = await _sb.from(table).insert(data); error = _ri.error;
    }
    if(error){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+error.message,'error'); throw error; }
    auditLog('insert', table, (data&&data.id)||null, data);
    if(data && data.id && !window._undoInProgress){ var _iid=data.id; pushUndo({ label:undoLabel('insert',table), undo:function(){ return dbDelete(table, _iid); }, redo:function(){ return dbInsert(table, data); } }); }
    scheduleTableRefresh(table, 800);
  } finally {
    setSynced();
  }
}
// ═══════════════════════════════════════════════════════════════════
// СКАСУВАТИ / ПОВЕРНУТИ (undo / redo) — стрілки + Ctrl+Z / Ctrl+Shift+Z
// ═══════════════════════════════════════════════════════════════════
window._undoStack = window._undoStack || [];
window._redoStack = window._redoStack || [];
window._undoInProgress = false;
var UNDO_MAX = 25;
var UNDO_LABELS = { students:'учень', lessons:'заняття', payments:'платіж', comms:'комунікація', tutors:'репетитор', subjects:'предмет', tasks:'задача', payroll_items:'нарахування', contracts:'договір', act_log:'акт', pricing_rules:'правило цін', branches:'корпус' };
var UNDO_ACTIONS = { update:'редагування', insert:'додавання', delete:'видалення' };
function undoLabel(action, table){ return (UNDO_ACTIONS[action]||action)+' · '+(UNDO_LABELS[table]||table); }
function pushUndo(entry){
  if(window._undoInProgress) return;
  if(window._viewerMode || window._trainingMode) return;
  window._undoStack.push(entry);
  while(window._undoStack.length > UNDO_MAX) window._undoStack.shift();
  window._redoStack = []; // нова дія обнуляє «Повернути»
  updateUndoRedoBtns();
}
function updateUndoRedoBtns(){
  var u=document.getElementById('undobtn'), r=document.getElementById('redobtn');
  var nu=(window._undoStack||[]).length, nr=(window._redoStack||[]).length;
  if(u){ u.style.display=(nu>0 && !window._viewerMode)?'':'none'; if(nu>0){ var lu=window._undoStack[nu-1]; u.title='Скасувати: '+(lu.label||'останню зміну')+' (Ctrl+Z)'; } }
  if(r){ r.style.display=(nr>0 && !window._viewerMode)?'':'none'; if(nr>0){ var lr=window._redoStack[nr-1]; r.title='Повернути: '+(lr.label||'')+' (Ctrl+Shift+Z)'; } }
}
function updateUndoBtn(){ updateUndoRedoBtns(); } // сумісність
async function performUndo(){
  if(window._undoInProgress) return;
  var entry=(window._undoStack||[]).pop();
  if(!entry){ mkToast('Немає що скасовувати'); updateUndoRedoBtns(); return; }
  window._undoInProgress = true;
  try{
    await entry.undo();
    window._redoStack.push(entry);
    mkToast('↩ Скасовано: '+(entry.label||'зміну'));
  }catch(e){
    mkToast('Не вдалося скасувати: '+((e&&e.message)||e),'error');
    window._undoStack.push(entry);
  } finally {
    window._undoInProgress = false;
    updateUndoRedoBtns();
  }
}
async function performRedo(){
  if(window._undoInProgress) return;
  var entry=(window._redoStack||[]).pop();
  if(!entry){ mkToast('Немає що повертати'); updateUndoRedoBtns(); return; }
  window._undoInProgress = true;
  try{
    await entry.redo();
    window._undoStack.push(entry);
    mkToast('↪ Повернено: '+(entry.label||'зміну'));
  }catch(e){
    mkToast('Не вдалося повернути: '+((e&&e.message)||e),'error');
    window._redoStack.push(entry);
  } finally {
    window._undoInProgress = false;
    updateUndoRedoBtns();
  }
}
window.performUndo = performUndo;
window.performRedo = performRedo;
window.pushUndo = pushUndo;
window.updateUndoBtn = updateUndoRedoBtns;
window.updateUndoRedoBtns = updateUndoRedoBtns;
// Ctrl+Z (скасувати), Ctrl+Shift+Z / Ctrl+Y (повернути) — крім полів вводу
document.addEventListener('keydown', function(e){
  if(!(e.ctrlKey||e.metaKey)) return;
  var t=(e.target&&e.target.tagName||'').toLowerCase();
  if(t==='input'||t==='textarea'||(e.target&&e.target.isContentEditable)) return;
  var k=(e.key||'').toLowerCase();
  if(k==='z' && !e.shiftKey){ e.preventDefault(); performUndo(); }
  else if((k==='z' && e.shiftKey) || k==='y'){ e.preventDefault(); performRedo(); }
});

async function dbUpdate(table, id, data){
  if(window._viewerMode){mkToast('\u0420\u0435\u0436\u0438\u043c \u043f\u0435\u0440\u0435\u0433\u043b\u044f\u0434\u0443 \u2014 \u0437\u043c\u0456\u043d\u0438 \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0456','error');return;}
  if(window._trainingMode){
    var _tk2=({students:'students',lessons:'lessons',payments:'payments',comms:'comms',tutors:'tutors',subjects:'subjects',tasks:'tasks',payroll_items:'payrollItems'})[table];
    var _normFnsUpd={students:normalizeStudent,lessons:normalizeLesson,payments:normalizePayment,tutors:normalizeTutor,comms:normalizeComm,tasks:normalizeTask,payroll_items:normalizePayrollItem};
    if(_tk2){
      var _entry=(S[_tk2]||[]).find(function(x){return x.id===id;});
      if(_entry){
        Object.assign(_entry, data);
        if(_normFnsUpd[table]) Object.assign(_entry, _normFnsUpd[table](_entry));
      }
      refreshPage(_tk2);
      if(S.currentPage==='schedule' && typeof renderSch==='function') renderSch();
    }
    setTimeout(function(){ try{trainingCheckProgress&&trainingCheckProgress();}catch(e){} }, 50);
    return;
  }
  var _diff = auditDiff(table, id, data); // рахуємо до апдейту, поки старий запис у стані
  var _undoOld=null;
  if(!window._undoInProgress && !window._viewerMode && !window._trainingMode){
    var _uk=({students:'students',lessons:'lessons',payments:'payments',comms:'comms',tutors:'tutors',subjects:'subjects',tasks:'tasks',payroll_items:'payrollItems'})[table]||'';
    var _ue=_uk?(S[_uk]||[]).find(function(x){return x.id===id;}):null;
    if(_ue){ _undoOld={}; Object.keys(data).forEach(function(k){ _undoOld[k]=(_ue[k]===undefined?null:_ue[k]); }); }
  }
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
    if(_undoOld){ pushUndo({ label:undoLabel('update',table), undo:function(){ return dbUpdate(table, id, _undoOld); }, redo:function(){ return dbUpdate(table, id, data); } }); }
    // Патчимо локальний стан ОДРАЗУ — інакше редагування (заняття, учні, платежі тощо)
    // не показувалось миттєво і чекало на фонове (дебаунсоване) оновлення за 800мс.
    var _updKey=({students:'students',lessons:'lessons',payments:'payments',comms:'comms',tutors:'tutors',subjects:'subjects',tasks:'tasks',payroll_items:'payrollItems'})[table]||'';
    var _normFns={students:normalizeStudent,lessons:normalizeLesson,payments:normalizePayment,tutors:normalizeTutor,comms:normalizeComm,tasks:normalizeTask,payroll_items:normalizePayrollItem};
    if(_updKey){
      var _entry=(S[_updKey]||[]).find(function(x){return x.id===id;});
      if(_entry){
        Object.assign(_entry, updateData);
        if(_normFns[table]) Object.assign(_entry, _normFns[table](_entry));
      }
      refreshPage(_updKey);
    }
    scheduleTableRefresh(table, 800);
  } finally {
    setSynced();
  }
}
async function dbDelete(table, id){
  if(window._viewerMode){mkToast('\u0420\u0435\u0436\u0438\u043c \u043f\u0435\u0440\u0435\u0433\u043b\u044f\u0434\u0443 \u2014 \u0437\u043c\u0456\u043d\u0438 \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0456','error');return;}
  if(window._trainingMode){
    var _tk3=({students:'students',lessons:'lessons',payments:'payments',comms:'comms',tutors:'tutors',subjects:'subjects',tasks:'tasks',payroll_items:'payrollItems'})[table];
    if(_tk3){
      S[_tk3]=(S[_tk3]||[]).filter(function(x){return x.id!==id;});
      refreshPage(_tk3);
      if(S.currentPage==='schedule' && typeof renderSch==='function') renderSch();
    }
    setTimeout(function(){ try{trainingCheckProgress&&trainingCheckProgress();}catch(e){} }, 50);
    return;
  }
  setSaving();
  try{
    var _delKey=({students:'students',lessons:'lessons',payments:'payments',comms:'comms',tutors:'tutors',subjects:'subjects',tasks:'tasks',payroll_items:'payrollItems'})[table]||'';
    var _undoRow=null;
    if(!window._undoInProgress && !window._viewerMode && !window._trainingMode){
      try{ var _rs=await _sb.from(table).select('*').eq('id',id).single(); if(_rs && _rs.data) _undoRow=_rs.data; }catch(e){}
    }
    var _rd = await _sb.from(table).delete().eq('id',id); var error = _rd.error;
    if(error && await refreshIfExpired(error)){
      _rd = await _sb.from(table).delete().eq('id',id); error = _rd.error;
    }
    if(error){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+error.message,'error'); throw error; }
    auditLog('delete', table, id, (S[_delKey]||[]).find(function(x){return x.id===id;}));
    if(_undoRow){ pushUndo({ label:undoLabel('delete',table), undo:function(){ return dbInsert(table, _undoRow); }, redo:function(){ return dbDelete(table, id); } }); }
    // Прибираємо з локального стану ОДРАЗУ — інакше фонове злиття в loadTableFresh
    // (яке зберігає локальні записи, відсутні у свіжій відповіді, щоб не губити щойно
    // додані через затримку мережі) помилково "воскресить" щойно видалений запис.
    if(_delKey) S[_delKey]=(S[_delKey]||[]).filter(function(x){return x.id!==id;});
    refreshPage(_delKey);
    scheduleTableRefresh(table, 500);
  } finally {
    setSynced();
  }
}
