
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
    return isDoneLesson(l)||l.status==='makeup';
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
  var brSelAct=document.getElementById('act-branch');
  var _canFilterBrA=isSuperAdmin()||R()==='director'||R()==='admin';
  if(brSelAct && _canFilterBrA){
    var _prevBrA=brSelAct.value;
    brSelAct.innerHTML='<option value="">\u0412\u0441\u0456 \u0444\u0456\u043B\u0456\u0457</option>'
      +(S.branches||[]).slice().sort(function(a,b){return (a.name||'').localeCompare(b.name||'','uk');})
        .map(function(b){return '<option value="'+b.id+'">'+b.name+'</option>';}).join('');
    brSelAct.value=_prevBrA;
  }
  var fBranchAct=_canFilterBrA?(document.getElementById('act-branch')||{value:''}).value:'';

  // Показуємо ВСІХ активних учнів, навіть без проведених занять цього
  // місяця — інакше вони випадали зі списку й акт їм ніхто не формував.
  var students=(S.students||[])
    .filter(function(x){ return x.status==='active'||x.status==='trial'; })
    .slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');});
  var rows=[];
  students.forEach(function(s){
    var lessons=actLessonsFor(s.id, per);
    var log=actLogFor(s.id, per);
    var signed=log.some(function(r){return r.status==='signed';});
    var sent=log.length>0;
    rows.push({s:s, lessons:lessons, log:log, signed:signed, sent:sent});
  });
  if(filter==='sent') rows=rows.filter(function(r){return r.sent;});
  if(filter==='notsent') rows=rows.filter(function(r){return !r.sent;});
  if(filter==='signed') rows=rows.filter(function(r){return r.signed;});
  if(fBranchAct) rows=rows.filter(function(r){
    var st=r.s;
    if(Array.isArray(st.branchIds)&&st.branchIds.length) return st.branchIds.indexOf(fBranchAct)>=0;
    return (st.branchId||st.branch_id)===fBranchAct;
  });

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
        +'<div style="font-size:11px;color:var(--t3)">'+(r.lessons.length
            ? r.lessons.length+' \u043F\u043E\u0441\u043B\u0443\u0433 \u00B7 '+hours+'\u0433 \u00B7 '+sum+'\u20B4'
            : '\u2014 \u0431\u0435\u0437 \u043F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u0438\u0445 \u0437\u0430\u043D\u044F\u0442\u044C')+'</div></div>'
      +badge
      +'<button class="btn btn-g btn-sm" onclick="openActEditM(\''+r.s.id+'\')" title="\u0420\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u0442\u0438/\u0441\u0444\u043E\u0440\u043C\u0443\u0432\u0430\u0442\u0438/\u0434\u0440\u0443\u043A\u0443\u0432\u0430\u0442\u0438">\u270F\uFE0F \u0410\u043A\u0442</button>'
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
function openActEditM(sid){
  if(!canActs()) return;
  var s=(S.students||[]).find(function(x){return x.id===sid;});
  if(!s){ mkToast('\u0423\u0447\u0435\u043D\u044C \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E','error'); return; }
  var per=actPeriod();
  var lessons=actLessonsFor(sid, per);
  if(!lessons.length){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u0438\u0445 \u0437\u0430\u043D\u044F\u0442\u044C \u0437\u0430 \u0446\u0435\u0439 \u043F\u0435\u0440\u0456\u043E\u0434','error'); return; }

  // Групуємо за предметом+репетитором — початкові рядки акта (можна редагувати нижче)
  var groups={}, order=[];
  lessons.forEach(function(l){
    var tid=l.tutorId||l.tutor_id;
    var t=tid?(S.tutors||[]).find(function(x){return x.id===tid;}):null;
    var key=(tid||'')+'|'+(l.subject||'');
    if(!groups[key]){ groups[key]={tutor:t, subj:l.subject||'\u0406\u043D\u0448\u0435', lessons:[]}; order.push(key); }
    groups[key].lessons.push(l);
  });

  var rows=order.map(function(key){
    var g=groups[key];
    var gHours=Math.round(g.lessons.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
    var gSum=Math.round(g.lessons.reduce(function(s,l){return s+lessonTotal(l);},0)*100)/100;
    return { desc:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0441\u044C\u043A\u0456 \u043F\u043E\u0441\u043B\u0443\u0433\u0438: '+g.subj+(g.tutor?' ('+g.tutor.fn+' '+g.tutor.ln+')':''), hours:gHours, sum:gSum };
  });

  window._actEditSid=sid;
  window._actEditPeriod=per;
  var who=document.getElementById('act-edit-who');
  if(who) who.textContent=s.fn+' '+s.ln+' \u00B7 '+actPeriod();
  var noteEl=document.getElementById('act-edit-note');
  if(noteEl) noteEl.value='';
  var cName=document.getElementById('act-edit-client-name'); if(cName) cName.value=s.parentFn||(s.fn+' '+s.ln);
  var cPhone=document.getElementById('act-edit-client-phone'); if(cPhone) cPhone.value=s.parentPhone||s.phone||'';
  var cAddr=document.getElementById('act-edit-client-addr'); if(cAddr) cAddr.value=s.address||'';
  renderActEditRows(rows);
  openM('mo-act-edit');
}

function renderActEditRows(rows){
  var tb=document.getElementById('act-edit-rows');
  if(!tb) return;
  tb.innerHTML='';
  rows.forEach(function(r){ addActEditRow(r); });
}

function addActEditRow(r){
  r=r||{desc:'',hours:'',sum:''};
  var tb=document.getElementById('act-edit-rows');
  if(!tb) return;
  var tr=document.createElement('tr');
  tr.innerHTML='<td style="padding:3px 6px"><input class="ae-desc" value="'+String(r.desc||'').replace(/"/g,'&quot;')+'" style="width:100%;font-size:12.5px;padding:5px 7px"></td>'
    +'<td style="padding:3px 4px"><input class="ae-hours" type="number" step="0.1" value="'+(r.hours!=null?r.hours:'')+'" style="width:100%;font-size:12.5px;padding:5px 5px;text-align:center" oninput="recalcActEditTotal()"></td>'
    +'<td style="padding:3px 4px"><input class="ae-sum" type="number" step="0.01" value="'+(r.sum!=null?r.sum:'')+'" style="width:100%;font-size:12.5px;padding:5px 5px;text-align:right" oninput="recalcActEditTotal()"></td>'
    +'<td style="text-align:center"><button type="button" onclick="this.closest(\'tr\').remove();recalcActEditTotal()" style="border:none;background:none;cursor:pointer;color:var(--danger);font-size:13px">\u2715</button></td>';
  tb.appendChild(tr);
  recalcActEditTotal();
}

function recalcActEditTotal(){
  var sum=Array.from(document.querySelectorAll('#act-edit-rows .ae-sum')).reduce(function(s,el){return s+(parseFloat(el.value)||0);},0);
  var el=document.getElementById('act-edit-total');
  if(el) el.textContent=(Math.round(sum*100)/100).toLocaleString('uk-UA')+' \u20B4';
}

function collectActEditRows(){
  return Array.from(document.querySelectorAll('#act-edit-rows tr')).map(function(tr){
    return {
      desc:(tr.querySelector('.ae-desc')||{value:''}).value.trim(),
      hours:parseFloat((tr.querySelector('.ae-hours')||{value:'0'}).value)||0,
      sum:parseFloat((tr.querySelector('.ae-sum')||{value:'0'}).value)||0
    };
  }).filter(function(r){ return r.desc||r.sum; });
}

function printActFromEdit(){
  var sid=window._actEditSid, per=window._actEditPeriod;
  if(!sid||!per) return;
  var rows=collectActEditRows();
  if(!rows.length){ mkToast('\u0414\u043E\u0434\u0430\u0439\u0442\u0435 \u0445\u043E\u0447\u0430 \u043E\u0434\u0438\u043D \u0440\u044F\u0434\u043E\u043A','error'); return; }
  var note=(document.getElementById('act-edit-note')||{value:''}).value.trim();
  var clientInfo={
    name:(document.getElementById('act-edit-client-name')||{value:''}).value.trim(),
    phone:(document.getElementById('act-edit-client-phone')||{value:''}).value.trim(),
    addr:(document.getElementById('act-edit-client-addr')||{value:''}).value.trim()
  };
  closeM('mo-act-edit');
  printAct(sid, per, rows, note, clientInfo);
}
