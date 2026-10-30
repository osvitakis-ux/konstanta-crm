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
        .map(function(u){return '<option value="'+u.id+'">'+u.fn+' '+u.ln+'</option>';}).join('');
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
  lessApplyTutorSubject(); // предмет за обраним репетитором (з картки учня)
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
// Предмет, який ОБРАНИЙ репетитор викладає цьому учневі (записаний у картці учня),
// автоматично підставляється в поле предмета заняття.
function lessApplyTutorSubject(){
  if(window._lessLoading) return;
  var sid=document.getElementById('l-std')?.value;
  var tid=document.getElementById('l-tutor')?.value;
  var st=sid?(S.students||[]).find(function(x){return x.id===sid;}):null;
  var subjEl=document.getElementById('l-subj');
  if(!st||!tid||!subjEl) return;
  var m=studentRateRules(st).find(function(r){ return r.tutor_id===tid && (r.subject||'').trim(); });
  if(m){ subjEl.value=m.subject; }
}
window.lessApplyTutorSubject=lessApplyTutorSubject;
// Усі відомі предмети: довідник + ставки учнів + наявні заняття
function allKnownSubjects(){
  var set=[];
  function add(v){ v=(v||'').trim(); if(v&&set.indexOf(v)<0) set.push(v); }
  (S.subjects||[]).forEach(function(x){ add(x&&x.name); });
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
  // Кладемо в body, а не у wrap: модалка (.mdl) має CSS-анімацію з transform,
  // яка створює containing block для position:fixed — через це bottom-sheet
  // обрізався і не скролився через overflow:hidden модалки. У body такої проблеми немає.
  document.body.appendChild(drop);
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
      var r=inp.getBoundingClientRect();
      drop.style.cssText='position:fixed;background:var(--s1);border:1px solid var(--b1);border-radius:8px;max-height:200px;overflow-y:auto;z-index:9999;box-shadow:0 4px 16px rgba(0,0,0,.18)';
      drop.style.top=(r.bottom)+'px';
      drop.style.left=(r.left)+'px';
      drop.style.width=(r.width)+'px';
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
window.updateParentInfo = updateParentInfo;
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

// Дата створення учня + історія змін статусів (з журналу аудиту, без окремої таблиці)
async function renderStudentHistory(id){
  var box=document.getElementById('s-history'); if(!box) return;
  // Лише адміністратори та директори (не репетитори)
  if(!id || ['god','network_admin','director','admin'].indexOf(R())<0){ box.innerHTML=''; return; }
  var st=(S.students||[]).find(function(x){return x.id===id;})||{};
  var LBL={active:'\u0410\u043a\u0442\u0438\u0432\u043d\u0438\u0439',request:'\u0417\u0430\u043f\u0438\u0442',trial:'\u041f\u0440\u043e\u0431\u043d\u0435',paused:'\u041f\u0440\u0438\u0437\u0443\u043f\u0438\u043d\u0435\u043d\u0438\u0439',inactive:'\u041d\u0435\u0430\u043a\u0442\u0438\u0432\u043d\u0438\u0439',completed:'\u0417\u0430\u0432\u0435\u0440\u0448\u0438\u0432'};
  var fmtD=function(iso){ try{ return new Date(iso).toLocaleString('uk-UA',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'}); }catch(e){ return String(iso||'').slice(0,10); } };
  var created=st.created_at||st.createdAt;
  var head='<div style="display:flex;align-items:center;gap:8px;font-size:12.5px;color:var(--t2);padding:6px 0;border-top:1px solid var(--b1)">\uD83D\uDCC5 \u0414\u043e\u0434\u0430\u043d\u0438\u0439: <b style="color:var(--t1)">'+(created?fmtD(created):'\u2014')+'</b></div>';
  box.innerHTML=head+'<div style="font-size:11.5px;color:var(--t3)">\u0417\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0435\u043d\u043d\u044f \u0456\u0441\u0442\u043e\u0440\u0456\u0457 \u0441\u0442\u0430\u0442\u0443\u0441\u0456\u0432\u2026</div>';
  var changes=[];
  try{
    if(_sb){
      var r=await _sb.from('audit_log').select('descr,created_at').eq('table_name','students').eq('record_id',String(id)).order('created_at',{ascending:true}).limit(300);
      ((r&&r.data)||[]).forEach(function(row){
        var m=String(row.descr||'').match(/\u0421\u0442\u0430\u0442\u0443\u0441:\s*[^\u2192]*\u2192\s*([^,\u00B7]+)/);
        if(m){ var raw=m[1].trim(); changes.push({at:row.created_at, to:(LBL[raw]||(raw==='\u041f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u043e'?'\u0417\u0430\u0432\u0435\u0440\u0448\u0438\u0432':raw))}); }
      });
    }
  }catch(e){ /* тихо */ }
  var body;
  if(!changes.length){
    body='<div style="font-size:11.5px;color:var(--t3);padding-bottom:4px">\u0417\u043c\u0456\u043d \u0441\u0442\u0430\u0442\u0443\u0441\u0443 \u043d\u0435 \u0437\u0430\u0444\u0456\u043a\u0441\u043e\u0432\u0430\u043d\u043e. \u041f\u043e\u0442\u043e\u0447\u043d\u0438\u0439: <b>'+(LBL[st.status]||st.status||'\u2014')+'</b></div>';
  } else {
    body='<div style="font-size:10.5px;text-transform:uppercase;letter-spacing:.4px;color:var(--t3);font-weight:700;margin:2px 0 4px">\u0406\u0441\u0442\u043e\u0440\u0456\u044f \u0441\u0442\u0430\u0442\u0443\u0441\u0456\u0432</div>'
      + changes.map(function(c){ return '<div style="display:flex;justify-content:space-between;gap:10px;font-size:12px;padding:3px 0;border-bottom:1px solid var(--s3)"><span style="color:var(--t3)">'+fmtD(c.at)+'</span><span style="font-weight:600">\u2192 '+c.to+'</span></div>'; }).join('');
  }
  box.innerHTML=head+body;
}
// ══════════════ ДОГОВОРИ (тільки для «бога») ══════════════
var MONTHS_UK=['\u0441\u0456\u0447\u043d\u044f','\u043b\u044e\u0442\u043e\u0433\u043e','\u0431\u0435\u0440\u0435\u0437\u043d\u044f','\u043a\u0432\u0456\u0442\u043d\u044f','\u0442\u0440\u0430\u0432\u043d\u044f','\u0447\u0435\u0440\u0432\u043d\u044f','\u043b\u0438\u043f\u043d\u044f','\u0441\u0435\u0440\u043f\u043d\u044f','\u0432\u0435\u0440\u0435\u0441\u043d\u044f','\u0436\u043e\u0432\u0442\u043d\u044f','\u043b\u0438\u0441\u0442\u043e\u043f\u0430\u0434\u0430','\u0433\u0440\u0443\u0434\u043d\u044f'];
