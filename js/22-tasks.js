
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
        +_mgrs.map(function(u){return '<option value="'+u.id+'">'+u.fn+' '+u.ln+'</option>';}).join('')
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
  try{ playSound('task'); }catch(e){}
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
// Формат: розбивка по учнях (Прізвище І. | К-ть | Ціна | Коеф. | Сума).
// У «кількість проведених» входять: проведені (done/completed), відпрацювання
// проведені (makeup), згорілі (burned) та тестування (testing).
// Тестування має ФІКСОВАНУ ціну (payrollTestPrice, за замовч. 500 ₴, редаговано в UI).
// Коеф. = Ціна × 0.4 (виплата репетитору за один урок); Сума = К-ть × Коеф.
// Заняття одного учня за РІЗНИМИ цінами йдуть окремими рядками (розбивка по ціні).
// Разом = сума по рядках + ручні пункти (₴, ±).
var PAYROLL_COEF_DEFAULT = 0.4;
var PAYROLL_TEST_PRICE_DEFAULT = 500; // фіксована ціна тестування (₴), редаговано в UI
// Статуси занять, що йдуть у зарплату (включно з тестуванням).
var PAYROLL_STATUSES = ['done','completed','makeup','burned','testing'];
