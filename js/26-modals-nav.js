
function openStudM(id=null){
  var _canEditStud = can('students');
  // Без права редагування картку МОЖНА відкрити лише для ПЕРЕГЛЯДУ існуючого учня.
  // Створення нового (id===null) залишається недоступним.
  if(!_canEditStud && !id){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432 \u043D\u0430 \u0441\u0442\u0432\u043E\u0440\u0435\u043D\u043D\u044F \u0443\u0447\u043D\u0456\u0432','error'); return; }
  S.editId=id;
  document.getElementById('ms-title').textContent = !_canEditStud
    ? '\uD83D\uDC41 \u041A\u0430\u0440\u0442\u043A\u0430 \u0443\u0447\u043D\u044F (\u043F\u0435\u0440\u0435\u0433\u043B\u044F\u0434)'
    : (id?'\u0420\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u0442\u0438 \u0443\u0447\u043D\u044F':'\u041D\u043E\u0432\u0438\u0439 \u0443\u0447\u0435\u043D\u044C');
  // Populate subject datalist for student modal
  var dl_s=document.getElementById('subj-list-s');
  if(dl_s){dl_s.innerHTML=(S.subjects||[]).map(function(x){return '<option value="'+x.name+'">';}).join('');}
  // Populate branch checkboxes (god/director/admin only, hidden for others via CSS)
  var brListS=document.getElementById('s-branch-list');
  var _preSelBranchesS=id?((S.students.find(x=>x.id===id)||{}).branchIds||[]):[];
  if(brListS){
    brListS.innerHTML=(S.branches||[]).slice().sort(function(a,b){return (a.name||'').localeCompare(b.name||'','uk');})
      .map(function(b){
        var checked=_preSelBranchesS.indexOf(b.id)>=0?' checked':'';
        return '<label class="af-day"><input type="checkbox" class="s-branch-cb" value="'+b.id+'"'+checked+'> '+b.name+'</label>';
      }).join('') || '<span style="font-size:11px;color:var(--t3)">\u0424\u0456\u043B\u0456\u0439 \u0449\u0435 \u043D\u0435 \u0441\u0442\u0432\u043E\u0440\u0435\u043D\u043E</span>';
  }
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
      var _fpEl=document.getElementById('s-first-pay'); if(_fpEl) _fpEl.value=(['god','network_admin','director','admin'].indexOf(R())>=0 && s.first_payment!=null&&s.first_payment!=='')?s.first_payment:'';
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
    var _rlN=document.getElementById('s-rates-list'); if(_rlN) _rlN.innerHTML='';
    var _fpElN=document.getElementById('s-first-pay'); if(_fpElN) _fpElN.value='';
    var crmStEl2=document.getElementById('s-crm-stage'); if(crmStEl2) crmStEl2.value='lead';
    var crmRespEl2=document.getElementById('s-crm-resp'); if(crmRespEl2) crmRespEl2.value='';
  }
  renderCustomFields('student','mo-student-cf');
  renderStudentCard(id);
  var invBtn = document.getElementById('inv-btn');
  if(invBtn) invBtn.style.display = (id && (R()==='god'||R()==='director')) ? 'inline-flex' : 'none';

  // ── Режим лише перегляду (репетитор) ──────────────────────────
  // Блокуємо всі поля картки й ховаємо "Зберегти", щоб дані було видно,
  // але змінити їх було неможливо. Захист на рівні saveStudent() теж лишається.
  var _mo = document.getElementById('mo-student');
  if(_mo){
    _mo.querySelectorAll('input, select, textarea').forEach(function(el){
      el.disabled = !_canEditStud;
      if(!_canEditStud) el.style.cursor='default';
    });
    // Кнопки додавання/видалення рядків ставок теж вимикаємо
    _mo.querySelectorAll('.mdlb button').forEach(function(b){ b.disabled = !_canEditStud; });
    var saveBtn=document.getElementById('ms-save-btn');
    if(saveBtn) saveBtn.style.display = _canEditStud ? '' : 'none';
    var cancelBtn=document.getElementById('ms-cancel-btn');
    if(cancelBtn) cancelBtn.textContent = _canEditStud ? '\u0421\u043A\u0430\u0441\u0443\u0432\u0430\u0442\u0438' : '\u0417\u0430\u043A\u0440\u0438\u0442\u0438';
  }
  openM('mo-student');
  try{ renderStudentHistory(id); }catch(e){ var _h=document.getElementById('s-history'); if(_h) _h.innerHTML=''; }
  try{ renderStudentContracts(id); }catch(e){ var _c=document.getElementById('s-contracts'); if(_c) _c.innerHTML=''; }
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
  // Populate branch checkboxes (god/director/admin only, hidden for others via CSS)
  var brListT=document.getElementById('t-branch-list');
  var _preSelBranches=id?((S.tutors.find(x=>x.id===id)||{}).branchIds||[]):[];
  if(brListT){
    brListT.innerHTML=(S.branches||[]).slice().sort(function(a,b){return (a.name||'').localeCompare(b.name||'','uk');})
      .map(function(b){
        var checked=_preSelBranches.indexOf(b.id)>=0?' checked':'';
        return '<label class="af-day"><input type="checkbox" class="t-branch-cb" value="'+b.id+'"'+checked+'> '+b.name+'</label>';
      }).join('') || '<span style="font-size:11px;color:var(--t3)">\u0424\u0456\u043B\u0456\u0439 \u0449\u0435 \u043D\u0435 \u0441\u0442\u0432\u043E\u0440\u0435\u043D\u043E</span>';
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
   'l-miss-date','l-makeup-date','l-hw','l-games','l-lit'].forEach(function(f){
    var el=document.getElementById(f); if(el) el.value='';
  });
  // Автозаповнення предмета/репетитора зі ставок учня (слухачі вішаються один раз)
  var _stdEl=document.getElementById('l-std');
  if(_stdEl&&!_stdEl._ratesWired){ _stdEl._ratesWired=true; _stdEl.addEventListener('change',lessApplyStudentRates); }
  var _sjEl=document.getElementById('l-subj');
  if(_sjEl&&!_sjEl._ratesWired){ _sjEl._ratesWired=true; _sjEl.addEventListener('change',lessPickTutorForSubject); }
  var _tuEl=document.getElementById('l-tutor');
  if(_tuEl&&!_tuEl._subjWired){ _tuEl._subjWired=true; _tuEl.addEventListener('change',lessApplyTutorSubject); }
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
  // Список репетиторів: активні + (якщо редагуємо) поточний репетитор заняття,
  // навіть якщо він заархівований — щоб коректно показати ім'я.
  var _lTuts=activeTutors();
  if(id){ var _le=(S.lessons||[]).find(function(x){return x.id===id;}); var _lt=_le&&(_le.tutorId||_le.tutor_id); if(_lt && !_lTuts.some(function(t){return t.id===_lt;})){ var _at=(S.tutors||[]).find(function(t){return t.id===_lt;}); if(_at) _lTuts=_lTuts.concat([_at]); } }
  popSel('l-tutor', _lTuts, 'id', function(t){return t.fn+' '+t.ln+(t.archived?' (з архіву)':'');}, 'Викладач');
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
      var _ll=document.getElementById('l-lit'); if(_ll) _ll.value = l.literature||'';
      // Load missed/makeup dates and hw
      window._lessLoading=true;   // блокуємо автопідстановку під час завантаження
      var missEl=document.getElementById('l-miss-date');
      var makeupEl=document.getElementById('l-makeup-date');
      var _sid=l.studentId||l.student_id, _tid=l.tutorId||l.tutor_id;

      // ── Дата ПРОПУСКУ (для відпрацювання) ──────────────────────
      var autoMissDate = l.missed_date||'';
      // 1) Через прив'язку розбитого заняття
      if(!autoMissDate && (l.status==='makeup'||l.status==='makeup_planned') && l.split_group_id){
        var origMissed=(S.lessons||[]).find(function(x){
          return x.id===l.split_group_id && x.status==='missed';
        });
        if(origMissed) autoMissDate=origMissed.date;
      }
      // 2) Якщо прив'язки немає — підставляємо найсвіжіший НЕПОКРИТИЙ пропуск
      //    цього ж учня в цього ж репетитора. Це рятує від ручного вводу
      //    й від помилкових дат, коли пропуску насправді не було.
      if(!autoMissDate && (l.status==='makeup'||l.status==='makeup_planned') && _sid){
        var cand=(S.lessons||[]).filter(function(x){
          if(x.status!=='missed') return false;
          if((x.studentId||x.student_id)!==_sid) return false;
          if(_tid && (x.tutorId||x.tutor_id)!==_tid) return false;
          if(String(x.date||'') > String(l.date||'')) return false;   // пропуск має бути РАНІШЕ
          try{ return uncoveredMissedHours(x) > 0; }catch(e){ return true; }
        }).sort(function(a,b){ return String(b.date).localeCompare(String(a.date)); });
        if(cand.length) autoMissDate=cand[0].date;
      }
      if(missEl) missEl.value=autoMissDate;

      // ── Дата ВІДПРАЦЮВАННЯ (для пропущеного заняття) ───────────
      var autoMakeupDate = l.makeup_date||'';
      // Для самого відпрацювання дата відпрацювання — це його ж дата
      if(!autoMakeupDate && (l.status==='makeup'||l.status==='makeup_planned')) autoMakeupDate=l.date||'';
      // Якщо не вказана — шукаємо реальне відпрацювання цього пропуску
      if(!autoMakeupDate && l.status==='missed' && _sid){
        var mk=(S.lessons||[]).filter(function(x){
          if(x.status!=='makeup' && x.status!=='makeup_planned') return false;
          if((x.studentId||x.student_id)!==_sid) return false;
          if(_tid && (x.tutorId||x.tutor_id)!==_tid) return false;
          // Прив'язка або за датою пропуску, або через розбиття
          return x.missed_date===l.date || x.split_group_id===l.id;
        }).sort(function(a,b){ return String(a.date).localeCompare(String(b.date)); });
        if(mk.length) autoMakeupDate=mk[0].date;
      }
      if(makeupEl) makeupEl.value=autoMakeupDate;
      var hwEl=document.getElementById('l-hw');
      if(hwEl) hwEl.value=l.hw||'';
      if(typeof onLessStatChange==='function') onLessStatChange();
      // Список пропусків перебудувався — повертаємо збережене значення
      if(missEl && autoMissDate){
        missEl.value=autoMissDate;
        if(!missEl.value){
          // Пропущеного заняття вже немає в списку — додаємо окремим пунктом
          var _d=String(autoMissDate).split('-');
          var _ds=_d.length===3?_d[2]+'.'+_d[1]+'.'+_d[0].slice(2):autoMissDate;
          missEl.insertAdjacentHTML('beforeend','<option value="'+autoMissDate+'">'+_ds+'</option>');
          missEl.value=autoMissDate;
        }
      }
      if(makeupEl && autoMakeupDate) makeupEl.value=autoMakeupDate;
      window._lessLoading=false;
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
  var db=document.getElementById('del-lesson-btn'); if(db){ var _elDel=(S.lessons||[]).find(function(x){return x.id===id;}); db.style.display=(id && (canMoveLessons() || (!isPlannedLesson(_elDel) && !lessonLocked(_elDel))))?'inline-flex':'none'; }
  var sb=document.getElementById('del-series-btn');
  if(sb){var lr=id?(S.lessons||[]).find(function(l){return l.id===id;}):null;sb.style.display=(lr&&lr.recurId)?'inline-flex':'none';}
  // Показуємо блок об'єднання і заповнюємо список
  var mw=document.getElementById('l-merge-wrap');
  if(mw) mw.style.display='block';
  populateMergeSelect();
  // Кнопки диктування — вставляємо при відкритті картки
  try{
    [['vb-notes','l-notes'],['vb-hw','l-hw'],['vb-lit','l-lit']].forEach(function(pr){
      var holder=document.getElementById(pr[0]);
      if(holder){
        holder.innerHTML=voiceBtn(pr[1]);
        var vb=holder.querySelector('.voice-btn');
        if(vb) voiceBtnLongPress(vb);
      }
    });
  }catch(e){ console.error('voiceBtn:',e); }
  // Репетитор не може міняти день/час уже створеного ПЛАНОВОГО заняття (тільки адмін і вище)
  (function(){
    var _el=(S.lessons||[]).find(function(x){return x.id===id;});
    var lockMv = (!!id && !canMoveLessons() && isPlannedLesson(_el));
    ['l-date','l-time'].forEach(function(fid){
      var el=document.getElementById(fid);
      if(el){ el.disabled=lockMv; el.title=lockMv?'\u041f\u0435\u0440\u0435\u043d\u043e\u0441\u0438\u0442\u0438 \u043f\u043b\u0430\u043d\u043e\u0432\u0456 \u0437\u0430\u043d\u044f\u0442\u0442\u044f \u043c\u043e\u0436\u0435 \u043b\u0438\u0448\u0435 \u0430\u0434\u043c\u0456\u043d\u0456\u0441\u0442\u0440\u0430\u0442\u043e\u0440/\u0434\u0438\u0440\u0435\u043a\u0442\u043e\u0440':''; el.style.opacity=lockMv?'0.6':''; }
    });
  })();
  // Заморожене заняття: тему/ДЗ/літературу можна правити ДО КІНЦЯ МІСЯЦЯ; решту полів — ні
  (function(){
    var _lk = id ? lessonLocked(id) : false;
    var _jfLk = id ? journalFieldsLocked(id) : false;
    // журнальні поля: заблоковані лише якщо минулий місяць
    ['l-notes','l-hw','l-lit','l-games'].forEach(function(f){ var el=document.getElementById(f); if(el){ el.disabled=(_lk&&_jfLk); el.style.opacity=(_lk&&_jfLk)?'0.6':''; } });
    // решта полів: заблоковані, якщо заняття заморожене (для не-адмінів)
    ['l-stat','l-price','l-dur','l-std','l-subj','l-tutor','l-miss-date','l-makeup-date'].forEach(function(f){ var el=document.getElementById(f); if(el){ el.disabled=_lk; el.style.opacity=_lk?'0.6':''; } });
    if(_lk){ ['l-date','l-time'].forEach(function(f){ var el=document.getElementById(f); if(el){ el.disabled=true; el.style.opacity='0.6'; } }); }
    var hint=document.getElementById('l-lock-hint');
    if(hint){
      var _msg = (_lk && !_jfLk) ? '\uD83D\uDCDD \u0417\u0430\u043d\u044f\u0442\u0442\u044f \u0437\u0430 \u043c\u0438\u043d\u0443\u043b\u0438\u0439 \u0442\u0438\u0436\u0434\u0435\u043d\u044c: \u043c\u043e\u0436\u043d\u0430 \u0440\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u0442\u0438 \u043b\u0438\u0448\u0435 \u0442\u0435\u043c\u0443, \u0414\u0417 \u0442\u0430 \u043b\u0456\u0442\u0435\u0440\u0430\u0442\u0443\u0440\u0443 (\u0434\u043e \u043a\u0456\u043d\u0446\u044f \u043c\u0456\u0441\u044f\u0446\u044f)'
        : (_lk && _jfLk) ? '\uD83D\uDD12 \u0417\u0430\u043d\u044f\u0442\u0442\u044f \u0437\u0430\u043c\u043e\u0440\u043e\u0436\u0435\u043d\u0435 \u2014 \u0440\u0435\u0434\u0430\u0433\u0443\u0454 \u0430\u0434\u043c\u0456\u043d\u0456\u0441\u0442\u0440\u0430\u0442\u043e\u0440 \u0430\u0431\u043e \u0434\u0438\u0440\u0435\u043a\u0442\u043e\u0440' : '';
      hint.innerHTML=_msg; hint.style.display=_msg?'block':'none';
    }
  })();
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
  try{ var _bs2=document.getElementById('boot-skeleton'); if(_bs2) _bs2.remove(); }catch(e){}
  try{ initSwipeGestures(); }catch(e){}
  // На дашборді — привітання за часом доби замість сухого «Дашборд»
  var _pt=document.getElementById('ptitle');
  if(_pt){
    if(page==='dashboard' && CU && CU.fn){
      var _h=new Date().getHours();
      var _greet = _h>=5&&_h<12 ? '\u0414\u043e\u0431\u0440\u043e\u0433\u043e \u0440\u0430\u043d\u043a\u0443'
                 : _h>=12&&_h<18 ? '\u0414\u043e\u0431\u0440\u0438\u0439 \u0434\u0435\u043d\u044c'
                 : _h>=18&&_h<23 ? '\u0414\u043e\u0431\u0440\u043e\u0433\u043e \u0432\u0435\u0447\u043e\u0440\u0430'
                 : '\u0414\u043e\u0431\u0440\u043e\u0457 \u043d\u043e\u0447\u0456';
      var _n=String(CU.fn).trim(), _voc=_n;
      // Кличний відмінок: Ольга → Ольго, Іван → Іване
      if(/\u0430$/.test(_n)) _voc=_n.slice(0,-1)+'\u043e';
      else if(/[\u0431\u0432\u0433\u0434\u0437\u043a\u043b\u043c\u043d\u043f\u0440\u0441\u0442\u0444\u0445]$/i.test(_n)) _voc=_n+'\u0435';
      _pt.textContent=_greet+', '+_voc+'!';
    } else {
      _pt.textContent=(PLABELS[page]||page);
    }
  }


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
  if(page==='telephony'){try{renderTelephony();}catch(e){console.error('renderTelephony:',e);}}
  if(page==='leads'){try{renderLeads();}catch(e){console.error('renderLeads:',e);}}
  if(page==='contracts'){try{renderContractsPage();}catch(e){console.error('renderContractsPage:',e);}}
  if(page==='invoice'){ renderInvoicePage(); try{renderInvoiceStatus();}catch(e){} }
  if(page==='invoice-log') renderInvoiceLog();
  var _crmEl=document.getElementById('pg-crm');
  if(page==='crm'){if(_crmEl)_crmEl.style.display='flex';renderCrm();}
  else{if(_crmEl)_crmEl.style.display='none';}
    if(isCustomPage)renderCustomPage(page);
  try{updateTaskAlert();}catch(e){}
  try{ buildBottomNav(); }catch(e){}   // підсвітити активний пункт
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
  else if(view==='month'){if(d===0)S.monthOffset=0;else S.monthOffset=(S.monthOffset||0)+d;}
  else{if(d===0)S.weekOffset=0;else S.weekOffset=(S.weekOffset||0)+d;}
  renderSch();
}


function schSetView(v){
  S.schView = v;
  if(v === 'week') S.weekOffset = S.weekOffset || 0;
  else if(v === 'month') S.monthOffset = S.monthOffset || 0;
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


function toggleTutLink(){
  const r=document.getElementById('u-role').value;
  document.getElementById('u-tlink-wrap').style.display=r==='tutor'?'flex':'none';
  var bw=document.getElementById('u-branch-wrap');
  if(bw) bw.style.display=(r==='admin'||r==='director')?'flex':'none';
}


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
  var bioLen = (obj.bio||'').length;
  console.log('[profile] зберігаю bio:', bioLen, 'символів');
  // Перевіряємо, що поле віддає повний текст (а не обрізаний браузером)
  var _el=document.getElementById('pr-bio');
  if(_el) console.log('[profile] у полі вводу:', _el.value.length,
    '| перші 40:', JSON.stringify(_el.value.slice(0,40)),
    '| останні 40:', JSON.stringify(_el.value.slice(-40)));
  try{
    await dbUpdate('tutors', mt.id, obj);
    if(CU){ await dbUpdate('profiles', CU.id, {fn:obj.fn, ln:obj.ln});
      CU = Object.assign({}, CU, {fn:obj.fn, ln:obj.ln}); updateSBUser(); }

    // Перевіряємо, що саме опинилось у базі — бо збереження може
    // «пройти» без помилки, а текст усе одно обрізатись
    try{
      var chk = await _sb.from('tutors').select('bio').eq('id', mt.id).limit(1);
      var savedBio = ((chk.data||[])[0]||{}).bio || '';
      console.log('[profile] у базі:', savedBio.length, 'символів');
      if(savedBio.length < bioLen){
        mkToast('\u26a0\ufe0f \u0417\u0431\u0435\u0440\u0435\u0436\u0435\u043d\u043e '+savedBio.length
          +' \u0456\u0437 '+bioLen+' \u0441\u0438\u043c\u0432\u043e\u043b\u0456\u0432','error');
      } else {
        mkToast('\u041f\u0440\u043e\u0444\u0456\u043b\u044c \u043e\u043d\u043e\u0432\u043b\u0435\u043d\u043e');
      }
      // Оновлюємо стан свіжими даними з бази
      var mtRef=(S.tutors||[]).find(function(t){return t.id===mt.id;});
      if(mtRef) mtRef.bio = savedBio;
    }catch(e){
      mkToast('\u041f\u0440\u043e\u0444\u0456\u043b\u044c \u043e\u043d\u043e\u0432\u043b\u0435\u043d\u043e');
    }

    document.getElementById('pr-edit-form').style.display = 'none';
    renderProfile();
  }catch(e){ mkToast('Помилка: '+(e.message||e),'error'); }
}

// ═══════════════════════════════════════════════════════════════
//  НИЖНЯ НАВІГАЦІЯ (мобільні)
//  П'ять найчастіших розділів — під великим пальцем.
//  Останній пункт «Ще» відкриває повне бічне меню.
// ═══════════════════════════════════════════════════════════════
var BOTTOM_NAV = {
  god:      ['dashboard','schedule','students','lessons'],
  director: ['dashboard','schedule','students','payroll'],
  admin:    ['dashboard','schedule','students','lessons'],
  network_admin: ['dashboard','schedule','students','lessons'],
  tutor:    ['dashboard','schedule','lessons','students']
};
