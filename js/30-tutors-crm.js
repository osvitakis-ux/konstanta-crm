



function renderTutors(){
  var isAdmin=R()==='god'||R()==='director'||R()==='admin'||R()==='network_admin';
  var ce=can('tutors');
  var rows='';
  S.tutors.forEach(function(t){
    var acc=S.users.find(function(u){return u.id===t.accId||u.id===t.acc_uid;});
    var cnt=S.students.filter(function(s){
      return (s.tutorId===t.id||s.tutor_id===t.id||(s.tutorIds&&s.tutorIds.indexOf(t.id)>=0))&&s.status==='active';
    }).length;
    var canSeeBranches=isSuperAdmin()||R()==='director'||R()==='admin';
    var branchBadge=canSeeBranches
      ?((t.branchIds&&t.branchIds.length)
        ? t.branchIds.map(function(bid){return '<span class="badge" style="background:rgba(167,139,250,.12);color:#a78bfa;font-size:10px;margin:1px 3px 1px 0;display:inline-block">'+branchName(bid)+'</span>';}).join('')
        : (t.branchId||t.branch_id ? '<span class="badge" style="background:rgba(167,139,250,.12);color:#a78bfa;font-size:10px">'+branchName(t.branchId||t.branch_id)+'</span>' : ''))
      :'';
    var editBtns=isAdmin
      ?('<div style="display:inline-flex;gap:4px;margin-left:8px">'
        +'<button class="btn btn-g btn-sm" onclick="openTutM(\'' +t.id+ '\')" title="\u0420\u0435\u0434\u0430\u0433\u0443\u0432\u0430\u0442\u0438">\u270f\ufe0f</button>'
        +'<button class="btn btn-g btn-sm" onclick="openTransferStudents(\'' +t.id+ '\')" title="\u041f\u0435\u0440\u0435\u0434\u0430\u0442\u0438 \u0443\u0447\u043d\u0456\u0432 \u0456\u043d\u0448\u043e\u043c\u0443 \u0440\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0443">\uD83D\uDC65\u2192</button>'
        +(t.archived
            ? '<button class="btn btn-g btn-sm" onclick="restoreTutor(\'' +t.id+ '\')" title="\u0412\u0456\u0434\u043d\u043e\u0432\u0438\u0442\u0438">\u267b\ufe0f</button>'
            : '<button class="btn btn-g btn-sm" onclick="archiveTutor(\'' +t.id+ '\')" title="\u0417\u0430\u0430\u0440\u0445\u0456\u0432\u0443\u0432\u0430\u0442\u0438 (\u0437\u0432\u0456\u043b\u044c\u043d\u0438\u0432\u0441\u044f)">\uD83D\uDCE6</button>')
        +(ce?('<button class="btn btn-sm" style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger)" onclick="delTutor(\'' +t.id+ '\')" title="\u0412\u0438\u0434\u0430\u043b\u0438\u0442\u0438">\uD83D\uDDD1</button>'):'')
        +'</div>')
      :'';
    var archBadge = t.archived ? '<span class="badge" style="background:rgba(148,163,184,.18);color:#94a3b8;font-size:10px;margin-left:6px">\uD83D\uDCE6 \u0417 \u0430\u0440\u0445\u0456\u0432\u0443</span>' : '';
    var accHtml=acc
      ?('<div style="display:flex;align-items:center;gap:6px">'+mkAv(acc.fn||'?',acc.ln||'',24)
        +'<div><div style="font-size:12px;font-weight:600">'+(acc.fn||'')+' '+(acc.ln||'')+'</div>'
        +'<div style="font-size:10px;color:var(--t2)">'+(acc.email||'')+'</div></div>'
        +'<span class="rpill '+acc.role+'" style="font-size:10px;padding:2px 8px">'+ROLES[acc.role].icon+' '+ROLES[acc.role].label+'</span>'
        +'</div>')
      :'<span style="font-size:11px;color:var(--t3)">\u2014 \u0430\u043a\u0430\u0443\u043d\u0442 \u043d\u0435 \u043f\u0440\u0438\u0432\u2019\u044f\u0437\u0430\u043d\u043e</span>';
    rows+='<tr'+(t.archived?' style="opacity:.55"':'')+'>'
      +'<td><div style="display:flex;align-items:center;gap:10px">'+mkAv(t.fn,t.ln,36,t.photo)
      +'<div><div style="font-weight:600;font-size:13px">'+t.fn+' '+t.ln+archBadge+'</div>'
      +(t.subj?'<div style="font-size:11px;color:var(--t2)">'+t.subj+'</div>':'')
      +(branchBadge?'<div style="margin-top:3px">'+branchBadge+'</div>':'')
      +'</div></div></td>'
      +'<td>'+accHtml+'</td>'
      +'<td style="text-align:center"><span class="badge bb">'+cnt+'</span></td>'
      +'<td style="text-align:right">'+editBtns+'</td>'
      +'</tr>';
  });
  document.getElementById('tt-table').innerHTML=rows||
    '<tr><td colspan="4"><div class="empty"><div class="ei">\uD83E\uDDD1\u200D\uD83C\uDFEB</div>\u0420\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0456\u0432 \u043d\u0435\u043c\u0430\u0454</div></td></tr>';
}

function updateBranchSelector(){
  var el=document.getElementById('branch-sel');
  if(!el) return;
  var bid=S.currentBranchId;
  el.innerHTML='<option value="">\uD83C\uDF10 \u0412\u0441\u0456 \u0444\u0456\u043B\u0456\u0457</option>'+
    (S.branches||[]).map(function(b){
      return '<option value="'+b.id+'"'+(bid===b.id?' selected':'')+'>'+b.name+'</option>';
    }).join('');
}



function updateSBUser(){
  if(!CU)return;
  const r=ROLES[CU.role];
  const av=document.getElementById('sb-av');
  var _mt=myTutor();
  if(_mt&&_mt.photo){
    av.style.background='none';av.style.width='34px';av.style.height='34px';av.style.padding='0';av.style.overflow='hidden';
    av.innerHTML='<img src="'+_mt.photo+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%">';
  } else {
    av.style.background=r.avatarBg;av.style.width='34px';av.style.height='34px';av.style.fontSize='13px';av.style.color=CU.role==='director'?'#1b1464':'#fff';av.style.fontFamily="'Syne',sans-serif";av.style.fontWeight='700';
    av.textContent=(CU.fn[0]||'')+(CU.ln[0]||'');
  }
  document.getElementById('sb-name').textContent=CU.fn+' '+CU.ln;
  document.getElementById('sb-rpill').innerHTML=('<span class="rpill '+(CU.role)+'">'+(r.icon)+' '+(r.label)+'</span>');
}



// CRM KANBAN
var CRM_COLS = [
  {id:'lead',     lbl:'Новий лід',                   ico:'⬤', color:'#f59e0b'},
  {id:'request',  lbl:'Запит',                                       ico:'✉', color:'#3b82f6'},
  {id:'trial',    lbl:'Тестовий урок', ico:'◎', color:'#8b5cf6'},
  {id:'contract', lbl:'Підписання договору', ico:'✍', color:'#06b6d4'},
  {id:'invoice',  lbl:'Виставлення рахунку', ico:'▤', color:'#f97316'},
  {id:'payment',  lbl:'Оплата',                                 ico:'◈', color:'#10b981'},
  {id:'won',      lbl:'Успішно реалізовано', ico:'✅', color:'#22c55e'},
  {id:'lost',     lbl:'Не реалізовано', ico:'❌', color:'#ef4444'},
];

function getCrmStage(s){
  if(!s) return 'lead';
  if(s.crmStage) return s.crmStage;
  var map={active:'won',trial:'trial',paused:'lost',completed:'won'};
  return map[s.status]||'lead';
}

async function setCrmStage(studentId, stage){
  var i=(S.students||[]).findIndex(function(s){return s.id===studentId;});
  var prev=i>=0?(S.students[i].crmStage||S.students[i].crm_stage):null;
  if(i>=0){S.students[i].crmStage=stage;S.students[i].crm_stage=stage;}
  renderCrm();
  try{
    await dbUpdate('students',studentId,{crm_stage:stage});
    mkToast('Етап оновлено');
  }catch(e){
    if(i>=0){S.students[i].crmStage=prev;S.students[i].crm_stage=prev;}
    renderCrm();
    mkToast('Помилка: '+e.message,'error');
  }
}

function openAddLead(){
  openStudM(null);
  document.getElementById('ms-title').textContent='\u041d\u043e\u0432\u0438\u0439 \u043b\u0456\u0434';
  document.getElementById('s-status').value='trial';
  var crmStEl=document.getElementById('s-crm-stage'); if(crmStEl) crmStEl.value='lead';
}


function renderCrm(){
  var el = document.getElementById('crm-board');
  if(!el) return;

  var fStage = (document.getElementById('crm-f-stage')||{value:''}).value||'';
  var fMonth = (document.getElementById('crm-f-month')||{value:''}).value||'';
  var fResp  = (document.getElementById('crm-f-resp') ||{value:''}).value||'';

  // Populate responsible select on first render
  var respSel = document.getElementById('crm-f-resp');
  if(respSel && respSel.options.length <= 1){
    (S.users||[]).filter(function(u){ return u.role==='god'||u.role==='director'||u.role==='admin'; })
      .forEach(function(u){
        var o = document.createElement('option');
        o.value = u.id; o.textContent = u.fn+' '+u.ln;
        respSel.appendChild(o);
      });
    respSel.value = fResp;
  }

  // Дошка показує воронку; при виборі МІСЯЦЯ фільтруємо за ДАТОЮ СТВОРЕННЯ учня.
  var students = (S.students||[]).filter(function(s){
    if(getCrmStage(s)==='removed') return false; // приховано з CRM-дошки, учень лишається в системі
    if(fStage && getCrmStage(s) !== fStage) return false;
    if(fResp  && s.crmResponsible !== fResp) return false;
    if(fMonth){ var _cd=String(s.created_at||s.createdAt||s.crm_date||s.crmDate||'').slice(0,7); if(_cd!==fMonth) return false; }
    return true;
  });

  var groups = {};
  CRM_COLS.forEach(function(c){ groups[c.id] = []; });
  students.forEach(function(s){
    var st = getCrmStage(s);
    if(!groups[st]) st = 'lead';
    groups[st].push(s);
  });

  var cols = fStage ? CRM_COLS.filter(function(c){ return c.id===fStage; }) : CRM_COLS;

  // Update stats bar
  var statsEl = document.getElementById('crm-stats');
  if(statsEl){
    var total = students.length;
    var won   = students.filter(function(s){ return getCrmStage(s)==='won'; }).length;
    var lost  = students.filter(function(s){ return getCrmStage(s)==='lost'; }).length;
    var conv  = total>0 ? Math.round(won/total*100) : 0;
    statsEl.innerHTML =
      '<span style="background:var(--s2);border:1px solid var(--b1);border-radius:20px;padding:5px 14px">\u0412\u0441\u044c\u043e\u0433\u043e: <b>'+total+'</b></span>'
      +'<span style="background:var(--tut-bg);border:1px solid rgba(34,181,115,.25);border-radius:20px;padding:5px 14px;color:var(--tut)">\u0423\u0441\u043f\u0456\u0448\u043d\u043e: <b>'+won+'</b></span>'
      +'<span style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.25);border-radius:20px;padding:5px 14px;color:var(--danger)">\u041d\u0435 \u0440\u0435\u0430\u043b.: <b>'+lost+'</b></span>'
      +'<span style="background:var(--adm-bg);border:1px solid rgba(41,171,226,.25);border-radius:20px;padding:5px 14px;color:var(--adm)">\u041a\u043e\u043d\u0432\u0435\u0440\u0441\u0456\u044f: <b>'+conv+'%</b></span>';
    // Суми реалізованих угод — лише для директорів (за ОБРАНИМ місяцем і відповідальним)
    try{
    if(isSuperAdmin()){
      var _now=new Date();
      var _pad=function(n){return String(n).padStart(2,'0');};
      var _curMonth=_now.getFullYear()+'-'+_pad(_now.getMonth()+1);
      var _monthKey=fMonth||_curMonth;                       // обраний місяць або поточний
      var _my=parseInt(_monthKey.slice(0,4),10), _mm=parseInt(_monthKey.slice(5,7),10);
      var _mStart=_monthKey+'-01';
      var _mEnd=_monthKey+'-'+_pad(new Date(_my,_mm,0).getDate());
      var _mo=realizedDealsSum(fResp, _mStart, _mEnd);
      var _showWeek=(!fMonth || fMonth===_curMonth);         // тиждень має сенс лише для поточного місяця
      var _wStart=weekStartMonday();
      var _wEndD=new Date(_wStart+'T00:00:00'); _wEndD.setDate(_wEndD.getDate()+6);
      var _fmt=function(d){return d.getFullYear()+'-'+_pad(d.getMonth()+1)+'-'+_pad(d.getDate());};
      var _wk=_showWeek?realizedDealsSum(fResp,_wStart,_fmt(_wEndD)):{sum:0,cnt:0,list:[]};
      var _chip=function(lbl,r){return '<span style="background:var(--tut-bg);border:1px solid rgba(34,181,115,.3);border-radius:20px;padding:5px 14px;color:var(--tut)">\uD83D\uDCB0 '+lbl+': <b>'+money(r.sum)+'\u20B4</b> \u00b7 '+r.cnt+' \u0443\u0433.</span>';};
      if(_showWeek) statsEl.innerHTML += _chip('\u0420\u0435\u0430\u043b\u0456\u0437\u043e\u0432\u0430\u043d\u043e \u0437\u0430 \u0442\u0438\u0436\u0434\u0435\u043d\u044c', _wk);
      statsEl.innerHTML += _chip('\u0417\u0430 '+prMonthName(_monthKey), _mo);
      // Розбивка реалізованих угод ПО ПРІЗВИЩАХ
      var _detailEl=document.getElementById('crm-deals-detail');
      if(_detailEl){
        var _mkList=function(arr){
          if(!arr.length) return '<div style="color:var(--t3);font-size:12px;padding:2px 0">\u041d\u0435\u043c\u0430\u0454</div>';
          return arr.map(function(d){ return '<div style="display:flex;justify-content:space-between;gap:10px;padding:3px 0;border-bottom:1px solid var(--s3);font-size:12.5px"><span>'+d.name+'</span><span style="font-weight:600;white-space:nowrap">'+money(d.amount)+'\u20B4</span></div>'; }).join('');
        };
        var _cols='';
        if(_showWeek) _cols+='<div><div style="font-size:11px;text-transform:uppercase;letter-spacing:.4px;color:var(--t3);font-weight:700;margin-bottom:4px">\u0417\u0430 \u0442\u0438\u0436\u0434\u0435\u043d\u044c ('+money(_wk.sum)+'\u20B4)</div>'+_mkList(_wk.list)+'</div>';
        _cols+='<div><div style="font-size:11px;text-transform:uppercase;letter-spacing:.4px;color:var(--t3);font-weight:700;margin-bottom:4px">\u0417\u0430 '+prMonthName(_monthKey)+' ('+money(_mo.sum)+'\u20B4)</div>'+_mkList(_mo.list)+'</div>';
        _detailEl.innerHTML='<details style="background:var(--s2);border:1px solid var(--b1);border-radius:10px;padding:8px 14px;margin-bottom:12px"><summary style="cursor:pointer;font-size:13px;font-weight:600;color:var(--t1)">\uD83D\uDCCB \u0420\u0435\u0430\u043b\u0456\u0437\u043e\u0432\u0430\u043d\u0456 \u0443\u0433\u043e\u0434\u0438 \u043f\u043e \u043f\u0440\u0456\u0437\u0432\u0438\u0449\u0430\u0445</summary><div style="display:grid;grid-template-columns:'+(_showWeek?'1fr 1fr':'1fr')+';gap:18px;margin-top:10px">'+_cols+'</div></details>';
      }
    } else { var _dd=document.getElementById('crm-deals-detail'); if(_dd) _dd.innerHTML=''; }
    }catch(_crmDealsErr){ console.warn('[renderCrm deals block]', _crmDealsErr); }
  }

  el.innerHTML = '';

  cols.forEach(function(col){
    var cards = groups[col.id]||[];

    var colDiv = document.createElement('div');
    colDiv.className = 'crm-col';
    colDiv.addEventListener('dragover',  function(e){ crmDragOver(e); });
    colDiv.addEventListener('dragleave', function(e){ crmDragLeave(e); });
    colDiv.addEventListener('drop',      function(e){ crmDrop(e, col.id); });

    var hdr = document.createElement('div');
    hdr.className = 'crm-col-hdr';
    hdr.style.borderTop = '3px solid ' + col.color;
    hdr.innerHTML = '<span class="crm-col-hdr-ico">'+col.ico+'</span>'
      + '<span class="crm-col-lbl">'+col.lbl+'</span>'
      + '<span class="crm-col-cnt">'+cards.length+'</span>';
    colDiv.appendChild(hdr);

    var body = document.createElement('div');
    body.className = 'crm-col-body';

    if(!cards.length){
      var empty = document.createElement('div');
      empty.className = 'crm-empty-col';
      empty.textContent = '\u041f\u043e\u0440\u043e\u0436\u043d\u044c\u043e';
      body.appendChild(empty);
    }

    cards.forEach(function(s){
      var tutor = s.tutorId ? (S.tutors||[]).find(function(t){ return t.id===s.tutorId; }) : null;
      var resp  = s.crmResponsible ? (S.users||[]).find(function(u){ return u.id===s.crmResponsible; }) : null;
      var lc    = (S.comms||[]).filter(function(c){ return c.studentId===s.id; })
                    .sort(function(a,b){ return (b.date||'')>(a.date||'')?1:-1; })[0];

      var card = document.createElement('div');
      card.className = 'crm-card';
      card.draggable = true;

      var sid = s.id;
      card.addEventListener('dragstart', function(e){ crmDragStart(e, sid); });
      card.addEventListener('dragend',   crmDragEnd);

      var info = document.createElement('div');
      info.innerHTML =
        '<div class="crm-card-name">'+s.fn+' '+s.ln+'</div>'
        +(s.subject ? '<div class="crm-card-subj">'+s.subject+'</div>' : '')
        +(tutor ? '<div class="crm-card-meta">◈ '+tutor.fn+' '+tutor.ln+'</div>' : '')
        +(resp  ? '<div class="crm-card-meta" style="color:var(--dir)">★ '+resp.fn+' '+resp.ln+'</div>' : '')
        +((s.phone||s.parentPhone) ? '<div class="crm-card-meta">☎ '+(s.phone||s.parentPhone)+'</div>' : '')
        +(s.crmDate ? '<div class="crm-card-comm">▣ '+fd(s.crmDate)+'</div>' : '')
        +(lc ? '<div class="crm-card-comm">◎ '+fd(lc.date)+'</div>' : '');
      info.querySelector('.crm-card-name').addEventListener('click', function(){ openStudM(sid); });
      card.appendChild(info);

      // Action buttons
      var acts = document.createElement('div');
      acts.className = 'crm-card-actions';

      var editBtn = document.createElement('button');
      editBtn.className = 'crm-mv-btn';
      editBtn.title = 'Редагувати';
      editBtn.textContent = '✏';
      editBtn.addEventListener('click', function(e){ e.stopPropagation(); openCrmCard(sid); });
      acts.appendChild(editBtn);

      CRM_COLS.filter(function(c){ return c.id !== col.id; }).forEach(function(c){
        var btn = document.createElement('button');
        btn.className = 'crm-mv-btn';
        btn.title = '→ ' + c.lbl;
        btn.textContent = c.ico;
        (function(cid){ btn.addEventListener('click', function(e){ e.stopPropagation(); setCrmStage(sid, cid); }); })(c.id);
        acts.appendChild(btn);
      });

      var delBtn = document.createElement('button');
      delBtn.className = 'crm-mv-btn crm-del-btn';
      delBtn.title = 'Видалити лід';
      delBtn.textContent = '🗑';
      delBtn.addEventListener('click', function(e){ e.stopPropagation(); delLead(sid); });
      acts.appendChild(delBtn);

      card.appendChild(acts);
      body.appendChild(card);
    });

    colDiv.appendChild(body);
    el.appendChild(colDiv);
  });
  setTimeout(crmInitScroll, 50);
}


async function setCrmStage(studentId, stage){
  var i=(S.students||[]).findIndex(function(s){return s.id===studentId;});
  var prev=i>=0?(S.students[i].crmStage||S.students[i].crm_stage):null;
  var prevStatus=i>=0?S.students[i].status:null;
  var upd={crm_stage:stage};
  // Перехід у "Успішно реалізовано" = лід стає УЧНЕМ (активним).
  // "Не реалізовано"/"Запит"/"Новий лід" — лишається лідом (статус не чіпаємо).
  if(stage==='won' && ['active','trial'].indexOf(prevStatus)<0) upd.status='active';
  else if(stage==='trial' && (prevStatus==='request'||prevStatus==='lead')) upd.status='trial';
  if(i>=0){
    S.students[i].crmStage=stage; S.students[i].crm_stage=stage;
    if(upd.status){ S.students[i].status=upd.status; }
  }
  renderCrm();
  try{
    await dbUpdate('students',studentId,upd);
    mkToast(upd.status==='active'?'\u0413\u043e\u0442\u043e\u0432\u043e — \u043b\u0456\u0434 \u0441\u0442\u0430\u0432 \u0443\u0447\u043d\u0435\u043c':'\u0415\u0442\u0430\u043f \u043e\u043d\u043e\u0432\u043b\u0435\u043d\u043e');
  }catch(e){
    if(i>=0){ S.students[i].crmStage=prev; S.students[i].crm_stage=prev; if(prevStatus!=null) S.students[i].status=prevStatus; }
    renderCrm();
    mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+e.message,'error');
  }
}


function openCrmCard(studentId){
  var s=(S.students||[]).find(function(x){return x.id===studentId;});
  if(!s)return;
  var mo=document.getElementById('mo-crm-card');
  if(!mo){openStudM(studentId);return;}
  document.getElementById('crm-card-name').textContent=s.fn+' '+s.ln;
  var stageSel=document.getElementById('crm-card-stage');
  if(stageSel)stageSel.value=getCrmStage(s);
  var respSel=document.getElementById('crm-card-resp');
  if(respSel){
    respSel.innerHTML='<option value="">—</option>'
      +(S.users||[]).filter(function(u){return u.role==='god'||u.role==='director'||u.role==='admin';})
        .map(function(u){return '<option value="'+u.id+'"'+(s.crmResponsible===u.id?' selected':'')+'>'+u.fn+' '+u.ln+'</option>';}).join('');
  }
  var dateSel=document.getElementById('crm-card-date');
  if(dateSel)dateSel.value=s.crmDate||'';
  var notesSel=document.getElementById('crm-card-notes');
  if(notesSel)notesSel.value=s.crm_notes||'';
  S._crmEditId=studentId;
  openM('mo-crm-card');
}

async function saveCrmCard(){
  var id=S._crmEditId;if(!id)return;
  var stage=document.getElementById('crm-card-stage').value;
  var resp=document.getElementById('crm-card-resp').value;
  var date=document.getElementById('crm-card-date').value;
  var notes=document.getElementById('crm-card-notes').value;
  var i=(S.students||[]).findIndex(function(s){return s.id===id;});
  if(i>=0){
    S.students[i].crmStage=stage;S.students[i].crm_stage=stage;
    S.students[i].crmResponsible=resp||null;S.students[i].crm_responsible=resp||null;
    S.students[i].crmDate=date||null;S.students[i].crm_date=date||null;
    S.students[i].crm_notes=notes||null;
  }
  closeM('mo-crm-card');renderCrm();
  try{
    await dbUpdate('students',id,{crm_stage:stage,crm_responsible:resp||null,crm_date:date||null,crm_notes:notes||null});
    mkToast('Збережено');
  }catch(e){mkToast('Помилка: '+e.message,'error');}
}

function crmClearFilters(){
  var s=document.getElementById('crm-f-stage'),m=document.getElementById('crm-f-month'),r=document.getElementById('crm-f-resp');
  if(s)s.value='';if(m)m.value='';if(r)r.value='';
  renderCrm();
}

var _crmDragId=null;
function crmDragStart(e,id){_crmDragId=id;e.dataTransfer.effectAllowed='move';setTimeout(function(){if(e.target)e.target.style.opacity='0.4';},0);}
function crmDragEnd(e){if(e.target)e.target.style.opacity='1';document.querySelectorAll('.crm-col').forEach(function(c){c.classList.remove('crm-over');});}
function crmDragOver(e){e.preventDefault();document.querySelectorAll('.crm-col').forEach(function(c){c.classList.remove('crm-over');});e.currentTarget.classList.add('crm-over');}
function crmDragLeave(e){if(!e.currentTarget.contains(e.relatedTarget))e.currentTarget.classList.remove('crm-over');}
function crmDrop(e,colId){e.preventDefault();document.querySelectorAll('.crm-col').forEach(function(c){c.classList.remove('crm-over');});if(_crmDragId){setCrmStage(_crmDragId,colId);_crmDragId=null;}}

function crmScroll(dir){
  var el = document.getElementById('crm-board-scroll');
  if(!el) return;
  el.scrollBy({left: dir * 240, behavior: 'smooth'});
}

function crmUpdateScrollBtns(){
  var el = document.getElementById('crm-board-scroll');
  var btnL = document.getElementById('crm-scroll-left');
  var btnR = document.getElementById('crm-scroll-right');
  if(!el || !btnL || !btnR) return;
  var atLeft  = el.scrollLeft <= 10;
  var atRight = el.scrollLeft >= el.scrollWidth - el.clientWidth - 10;
  btnL.classList.toggle('visible', !atLeft);
  btnR.classList.toggle('visible', !atRight);
}

function crmInitScroll(){
  var el = document.getElementById('crm-board-scroll');
  if(!el || el._crmScrollInit) return;
  el._crmScrollInit = true;

  // Mouse wheel horizontal scroll (no Shift needed)
  el.addEventListener('wheel', function(e){
    if(Math.abs(e.deltaX) < Math.abs(e.deltaY)){
      e.preventDefault();
      el.scrollBy({left: e.deltaY * 2, behavior: 'auto'});
    }
    crmUpdateScrollBtns();
  }, {passive: false});

  // Update arrow visibility on scroll
  el.addEventListener('scroll', crmUpdateScrollBtns);

  // Touch swipe
  var touchX = 0;
  el.addEventListener('touchstart', function(e){ touchX = e.touches[0].clientX; }, {passive:true});
  el.addEventListener('touchmove', function(e){
    var dx = touchX - e.touches[0].clientX;
    el.scrollLeft += dx;
    touchX = e.touches[0].clientX;
    crmUpdateScrollBtns();
  }, {passive:true});

  crmUpdateScrollBtns();
}
