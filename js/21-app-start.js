
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
  // Позиціюємо біля кліку, але тримаємо меню повністю в межах екрана.
  // Розміри МІРЯЄМО (getBoundingClientRect), а не вгадуємо — інакше при
  // довгому заголовку чи низькому вікні низ/правий край обрізався.
  var margin = 10;
  pop.style.left = margin + 'px';
  pop.style.top = margin + 'px'; // тимчасово, щоб замір не спотворивсь краєм
  var rect = pop.getBoundingClientRect();
  var pw = rect.width || 190;
  var ph = rect.height || 200;
  var maxLeft = Math.max(margin, window.innerWidth  - pw - margin);
  var maxTop  = Math.max(margin, window.innerHeight - ph - margin);
  var lx = Math.min(Math.max(x, margin), maxLeft);
  var ly = Math.min(Math.max(y, margin), maxTop);
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
  if(lessonLocked(_quickLessonId)){ mkToast('\u0417\u0430\u043d\u044f\u0442\u0442\u044f \u0437\u0430\u043c\u043e\u0440\u043e\u0436\u0435\u043d\u0435 \u2014 \u0440\u0435\u0434\u0430\u0433\u0443\u0454 \u0430\u0434\u043c\u0456\u043d\u0456\u0441\u0442\u0440\u0430\u0442\u043e\u0440 \u0430\u0431\u043e \u0434\u0438\u0440\u0435\u043a\u0442\u043e\u0440','error'); return; }
  try{
    var upd={status: status};
    // Заморожуємо ставку в момент ПЕРШОГО проведення: далі зміни ставок це заняття
    // (минулий запис) не змінять. Тест — фіксована ціна тесту на цей момент.
    var _les=(S.lessons||[]).find(function(x){return x.id===_quickLessonId;});
    if(_les && isConductedStatus(status) && _les.status!==status){
      var _rate = (status==='testing') ? payrollTestPrice() : (function(){
        var _st=(S.students||[]).find(function(s){return s.id===(_les.studentId||_les.student_id);});
        return studentRate(_st, _les.subject, _les.tutorId||_les.tutor_id)||0;
      })();
      if(_rate>0) upd.price=_rate;
    }
    await dbUpdate('lessons', _quickLessonId, upd);
    mkToast(status==='burned'?'🔥 Згоріле':status==='testing'?'🧪 Тестування':status==='done'?'✅ Проведено':status==='missed'?'❌ Пропущено':status==='cancelled'?'🚫 Скасовано':'📅 Заплановано');
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

// Обробник Ctrl+K реєструється на початку файлу (див. секцію INIT),
// щоб він працював навіть якщо десь нижче станеться помилка.

// ── EXCEL EXPORT ────────────────────────────
function exportToExcel(type){
  if(R()!=='god'&&R()!=='director'){ mkToast('\u0415\u043A\u0441\u043F\u043E\u0440\u0442 \u0434\u043E\u0441\u0442\u0443\u043F\u043D\u0438\u0439 \u043B\u0438\u0448\u0435 \u0434\u0438\u0440\u0435\u043A\u0442\u043E\u0440\u0443','error'); return; }
  var rows=[], headers=[], data=[];

  if(type==='students'){
    headers=['Імʼя','Прізвище','Телефон','Предмет','Статус','Клас','Нотатки'];
    data=(S.students||[]).map(function(s){return [s.fn||'',s.ln||'',s.phone||'',s.subject||'',s.status||'',s.grade||'',s.notes||''];});
  } else if(type==='lessons'){
    headers=['Учень','Репетитор','Предмет','Дата','Час','Тривалість','Статус','Ціна','Тема уроку','Домашнє завдання','Література','Аудіювання та ігри'];
    data=myLessons().map(function(l){
      var st=(S.students||[]).find(function(s){return s.id===(l.studentId||l.student_id);});
      var tu=(S.tutors||[]).find(function(t){return t.id===(l.tutorId||l.tutor_id);});
      return [(st?st.fn+' '+st.ln:''),(tu?tu.fn+' '+tu.ln:''),(l.subject||''),(l.date||''),(l.time||''),(l.dur||60)+' хв',(l.status||''),(l.price||''),(l.notes||''),(l.hw||''),(l.literature||''),(l.games||'')];
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
  buildSidebar();
  try{ buildBottomNav(); }catch(e){} updateSBUser(); updateBranchSelector();
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
    buildSidebar();
  try{ buildBottomNav(); }catch(e){} updateSBUser();
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
window.openUserM = openUserM;
window.saveUser  = saveUser;
window.saveStudent = saveStudent;
window.saveTutor = saveTutor;
window.saveLesson = saveLesson;
window.savePayment = savePayment;
window.saveComm  = saveComm;
window.saveSettings = saveSettings;
window.closeM    = closeM;
window.openM     = openM;
window.delStudent = delStudent;
