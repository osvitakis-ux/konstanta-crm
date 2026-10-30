
function renderStudents(){
  var _q=(document.getElementById('gsearch')||{value:''}).value.toLowerCase().trim();
  var _canFilterExtra=(R()==='god'||R()==='director'||R()==='admin');
  var brSel=document.getElementById('sf-branch');
  if(brSel && _canFilterExtra){
    var _prevBr=brSel.value;
    brSel.innerHTML='<option value="">\u0412\u0441\u0456 \u0444\u0456\u043B\u0456\u0457</option>'
      +(S.branches||[]).slice().sort(function(a,b){return (a.name||'').localeCompare(b.name||'','uk');})
        .map(function(b){return '<option value="'+b.id+'">'+b.name+'</option>';}).join('');
    brSel.value=_prevBr;
  }
  var tutSel=document.getElementById('sf-tutor');
  if(tutSel && _canFilterExtra){
    var _prevTut=tutSel.value;
    tutSel.innerHTML='<option value="">\u0412\u0441\u0456 \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438</option>'
      +(S.tutors||[]).slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');})
        .map(function(t){return '<option value="'+t.id+'">'+t.fn+' '+t.ln+'</option>';}).join('');
    tutSel.value=_prevTut;
  }
  var _fBranch=_canFilterExtra?(document.getElementById('sf-branch')||{value:''}).value:'';
  var _fTutor=_canFilterExtra?(document.getElementById('sf-tutor')||{value:''}).value:'';
  var _fSort=_canFilterExtra?(document.getElementById('sf-sort')||{value:''}).value:'';
  var data=myStudents().filter(function(s){
    if(!_q) return true;
    var _qd=_q.replace(/\D/g,''); // цифри запиту — для пошуку за телефоном
    if((s.fn+' '+s.ln).toLowerCase().includes(_q)) return true;
    if((s.ln+' '+s.fn).toLowerCase().includes(_q)) return true;
    if((s.email||'').toLowerCase().includes(_q)) return true;
    if(_qd.length>=3){ // телефон зіставляємо ЛИШЕ за цифрами (щоб +380/380/0/пробіли збігались)
      if(String(s.phone||'').replace(/\D/g,'').includes(_qd)) return true;
      if(String(s.parentPhone||s.parent_phone||'').replace(/\D/g,'').includes(_qd)) return true;
    }
    return false;
  });
  if(sfCur!=='all') data=data.filter(function(s){return s.status===sfCur;});
  if(_fBranch) data=data.filter(function(s){
    if(Array.isArray(s.branchIds)&&s.branchIds.length) return s.branchIds.indexOf(_fBranch)>=0;
    return (s.branchId||s.branch_id)===_fBranch;
  });
  if(_fTutor) data=data.filter(function(s){return studentTutorIds(s).indexOf(_fTutor)>=0;});
  if(_fSort==='az') data=data.slice().sort(function(a,b){return (a.ln+' '+a.fn).localeCompare(b.ln+' '+b.fn,'uk');});
  else if(_fSort==='za') data=data.slice().sort(function(a,b){return (b.ln+' '+b.fn).localeCompare(a.ln+' '+a.fn,'uk');});
  var tot=document.getElementById('st-total');
  if(tot) tot.textContent=data.length+' \u0437 '+myStudents().length;
  var ce=can('students');
  // ── Заплановані години на місяць біля кожного репетитора ──────────
  // Місяць береться з <input type="month" id="st-hours-month">; за
  // замовчуванням — поточний. Рахуємо один прохід по всіх уроках і
  // будуємо індекс хвилин за ключем studentId|tutorId.
  var _ymNow=function(){ var n=new Date(); return n.getFullYear()+'-'+String(n.getMonth()+1).padStart(2,'0'); };
  var _hoursMonthEl=document.getElementById('st-hours-month');
  if(_hoursMonthEl && !_hoursMonthEl.value) _hoursMonthEl.value=_ymNow();
  var _hoursYM=(_hoursMonthEl&&_hoursMonthEl.value)||_ymNow();
  var _ymFrom=_hoursYM+'-01';
  var _ymTo=_hoursYM+'-'+String(new Date(parseInt(_hoursYM.slice(0,4),10), parseInt(_hoursYM.slice(5,7),10), 0).getDate()).padStart(2,'0');
  var _plannedIdx={};
  (S.lessons||[]).forEach(function(l){
    if(!(l.status==='planned'||l.status==='scheduled'||!l.status)) return; // лише заплановані
    var d=String(l.date||''); if(d<_ymFrom||d>_ymTo) return;               // лише обраний місяць
    var sid=l.studentId||l.student_id, tid=l.tutorId||l.tutor_id;
    if(!sid||!tid) return;
    var k=sid+'|'+tid;
    _plannedIdx[k]=(_plannedIdx[k]||0)+(parseFloat(l.dur)||60);
  });
  var html=data.length?data.map(function(s){
    var btns=ce
      ?('<button class="btn btn-g btn-sm" onclick="openStudM(this.dataset.id)" data-id="'+s.id+'">\u270F\uFE0F</button>'
        +'<button class="btn btn-sm" style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger)" onclick="delStudent(this.dataset.id)" data-id="'+s.id+'">\uD83D\uDDD1</button>')
      :('<button class="btn btn-g btn-sm" onclick="openStudM(this.dataset.id)" data-id="'+s.id+'" title="\u041F\u0435\u0440\u0435\u0433\u043B\u044F\u043D\u0443\u0442\u0438 \u043A\u0430\u0440\u0442\u043A\u0443">\uD83D\uDC41</button>');
    var _tids=studentTutorIds(s);
    // Імена репетиторів із кольоровою крапкою — той самий колір, що й у розкладі
    var _tnames=_tids.map(function(tid){
      var nm=tn(tid);
      if(!nm||nm==='\u2014') return null;
      var _mins=_plannedIdx[s.id+'|'+tid]||0;
      var _h=Math.round(_mins/60*10)/10;
      var _hClr=_h>0?'var(--t2)':'var(--t3)';
      var _hBadge='<span style="color:'+_hClr+';font-size:11px;font-family:JetBrains Mono,monospace;margin-left:2px" title="Заплановані години за '+_hoursYM+'">\u00B7 '+_h+' \u0433\u043E\u0434</span>';
      return '<span style="display:inline-flex;align-items:center;gap:5px">'+tutorDot(tid)+nm+_hBadge+'</span>';
    }).filter(Boolean);
    var _subjTxt=studentSubjects(s).join(', ')||s.subject||'';
    var _canSeeBranchesS=isSuperAdmin()||R()==='director'||R()==='admin';
    var _branchBadgeS=_canSeeBranchesS
      ?((s.branchIds&&s.branchIds.length)
        ? s.branchIds.map(function(bid){return '<span class="badge" style="background:rgba(167,139,250,.12);color:#a78bfa;font-size:10px;margin:1px 3px 1px 0;display:inline-block">'+branchName(bid)+'</span>';}).join('')
        : (s.branchId||s.branch_id ? '<span class="badge" style="background:rgba(167,139,250,.12);color:#a78bfa;font-size:10px">'+branchName(s.branchId||s.branch_id)+'</span>' : ''))
      :'';
    return '<tr>'
      +'<td><div style="display:flex;align-items:center;gap:8px">'+mkAv(s.fn,s.ln)+'<div><div style="font-weight:600;font-size:13px">'+s.fn+' '+s.ln+'</div>'+(_branchBadgeS?'<div style="margin-top:3px">'+_branchBadgeS+'</div>':'')+'</div></div></td>'
      +'<td style="font-size:12px;color:var(--t2)">'+(s.age||'\u2014')+' / '+(s.grade||'\u2014')+'</td>'
      +'<td>'+(_subjTxt||'\u2014')+'</td>'
      +'<td style="font-size:12px;line-height:1.6">'+(_tnames.length?_tnames.join('<br>'):'\u2014')+'</td>'
      +'<td>'+bst(s.status)+'</td>'
      +'<td style="font-size:12px;color:var(--t2)">'+(s.parentPhone||s.phone||s.email||'\u2014')+'</td>'
      +'<td><div style="display:flex;gap:3px">'+btns+'</div></td>'
      +'</tr>';
  }).join(''):'<tr><td colspan="7"><div class="empty"><div class="ei">\uD83D\uDC65</div>\u0423\u0447\u043D\u0456\u0432 \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E</div></td></tr>';
  document.getElementById('st-table').innerHTML=html;
}
// ── Перегляд занять по тижнях / місяцях ──────────────────────
var lfOffset = 0;

function lfPeriodRange(){
  var mode=(document.getElementById('lf-period')||{value:''}).value;
  if(!mode) return null;
  var now=new Date();
  if(mode==='week'){
    var dow=now.getDay()===0?6:now.getDay()-1;
    var mon=new Date(now); mon.setDate(now.getDate()-dow+lfOffset*7); mon.setHours(0,0,0,0);
    var sun=new Date(mon); sun.setDate(mon.getDate()+6);
    var f=function(d){return d.toLocaleDateString('uk-UA',{day:'numeric',month:'short'});};
    return {from:localDateStr(mon), to:localDateStr(sun), label:f(mon)+' \u2014 '+f(sun)};
  }
  var first=new Date(now.getFullYear(), now.getMonth()+lfOffset, 1);
  var last=new Date(now.getFullYear(), now.getMonth()+lfOffset+1, 0);
  var mn=['\u0421\u0456\u0447\u0435\u043d\u044c','\u041b\u044e\u0442\u0438\u0439','\u0411\u0435\u0440\u0435\u0437\u0435\u043d\u044c','\u041a\u0432\u0456\u0442\u0435\u043d\u044c','\u0422\u0440\u0430\u0432\u0435\u043d\u044c','\u0427\u0435\u0440\u0432\u0435\u043d\u044c','\u041b\u0438\u043f\u0435\u043d\u044c','\u0421\u0435\u0440\u043f\u0435\u043d\u044c','\u0412\u0435\u0440\u0435\u0441\u0435\u043d\u044c','\u0416\u043e\u0432\u0442\u0435\u043d\u044c','\u041b\u0438\u0441\u0442\u043e\u043f\u0430\u0434','\u0413\u0440\u0443\u0434\u0435\u043d\u044c'];
  return {from:localDateStr(first), to:localDateStr(last), label:mn[first.getMonth()]+' '+first.getFullYear()};
}

function lfSetPeriod(){ lfOffset=0; renderLessons(); }
function lfShift(d){
  if(d===0) lfOffset=0; else lfOffset+=d;
  renderLessons();
}
window.lfSetPeriod=lfSetPeriod;
window.lfShift=lfShift;

function renderLessons(){
  var stf = document.getElementById('lf-stat');
  var sdf = document.getElementById('lf-student');
  var ttf = document.getElementById('lf-tutor');
  var sv  = stf ? stf.value : '';
  var sdv = sdf ? sdf.value : '';
  var tv  = ttf ? ttf.value : '';

  // Період (тиждень / місяць) із навігацією стрілками
  var lfR = lfPeriodRange();
  var navEl = document.getElementById('lf-nav');
  var navLbl = document.getElementById('lf-nav-lbl');
  if(navEl) navEl.style.display = lfR ? 'inline-flex' : 'none';
  if(navLbl && lfR) navLbl.textContent = lfR.label + (lfOffset===0 ? '' : '');

  if(sdf){
    var cur = sdf.value;
    // Після вибору репетитора — лише його учні
    var studList = myStudents();
    if(tv){ studList = studList.filter(function(s){ return studentTutorIds(s).indexOf(tv)>=0; }); }
    studList = studList.sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');});
    if(cur && !studList.some(function(s){return s.id===cur;})){ cur=''; sdv=''; } // обраний учень не цього репетитора — скидаємо
    popSelSearch('lf-student', [{id:'',fn:'Всі учні',ln:''}].concat(studList), 'id', function(s){return s.fn+(s.ln?' '+s.ln:'');}, '');
    sdf.value=cur; if(sdf._updateSearch) sdf._updateSearch();
  }
  // Фільтр по репетиторах (адміни/директори/бог; для репетиторів прихований через .tutor-hidden)
  if(ttf && R()!=='tutor'){
    var curT = ttf.value;
    popSelSearch('lf-tutor', [{id:'',fn:'Всі репетитори',ln:''}].concat((S.tutors||[]).slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');})), 'id', function(t){return t.fn+(t.ln?' '+t.ln:'');}, '');
    if(curT){ ttf.value=curT; if(ttf._updateSearch) ttf._updateSearch(); }
  }
  // Якщо селект обгорнутий пошуковим полем — ховаємо/показуємо всю обгортку за роллю
  if(ttf && ttf.parentElement && ttf.parentElement.classList.contains('srch-wrap')){
    ttf.parentElement.style.display = R()==='tutor' ? 'none' : '';
  }

  var data = [].concat(myLessons()).filter(function(l){
    // Покриті missed не показуємо в загальному списку (є як makeup)
    if(!sv && l.status==='missed' && isCoveredMissed(l)) return false;
    return true;
  }).sort(function(a,b){
    return new Date(b.date+'T'+(b.time||'00:00'))-new Date(a.date+'T'+(a.time||'00:00'));
  });
  // Обмеження періодом, якщо обрано тиждень/місяць
  if(lfR) data = data.filter(function(l){
    var d=String(l.date||''); return d>=lfR.from && d<=lfR.to;
  });
  if(sdv) data = data.filter(function(l){return (l.studentId||l.student_id)===sdv;});
  if(tv)  data = data.filter(function(l){return (l.tutorId||l.tutor_id)===tv;});
  if(sv)  data = data.filter(function(l){
    if(sv==='done') return isDoneLesson(l)||l.status==='makeup';
    if(sv==='missed') return l.status==='missed' && !isCoveredMissed(l);
    return l.status===sv;
  });

  var hasMissed = sv==='missed'||sv==='makeup'||sv==='makeup_planned'||(!sv&&data.some(function(l){return l.status==='missed'||l.status==='makeup'||l.status==='makeup_planned';}));
  var mc=document.getElementById('lt-miss-col'); if(mc) mc.style.display=hasMissed?'':'none';
  var mkc=document.getElementById('lt-makeup-col'); if(mkc) mkc.style.display=hasMissed?'':'none';

  var ce = can('lessons');
  var ri = function(l){return l.recurId?'<span style="color:var(--adm);font-size:10px">🔁</span>':'';};
  var html = data.length ? data.map(function(l){
    var mc2=hasMissed?('<td style="font-size:11px;color:var(--danger)">'+(l.missed_date?fd(l.missed_date):'—')+'</td>'):'';
    var mk2=hasMissed?('<td style="font-size:11px;color:#f59e0b">'+(l.makeup_date?fd(l.makeup_date):'—')+'</td>'):'';
    var btns=ce
      ?('<button class="btn btn-g btn-sm" onclick="openLessM(this.dataset.id)" data-id="'+l.id+'">✏️</button>'
        +'<button class="btn btn-sm" style="background:rgba(248,113,113,.1);color:var(--danger)" onclick="delLesson(this.dataset.id)" data-id="'+l.id+'">🗑</button>')
      :'';
    return '<tr>'
      +'<td>'+sn(l.studentId||l.student_id)+'</td>'
      +'<td>'+(l.subject||'—')+' '+ri(l)+'</td>'
      +'<td style="font-size:12px">'+(l.tutorId||l.tutor_id?tn(l.tutorId||l.tutor_id):'—')+'</td>'
      +'<td style="font-family:JetBrains Mono,monospace;font-size:11px">'+fd(l.date)+' '+(l.time||'')+'</td>'
      +'<td>'+(l.dur||60)+' хв</td>'+mc2+mk2
      +'<td style="font-size:11px;color:var(--t2);max-width:220px">'
        +(l.notes?'<div>📌 '+l.notes+'</div>':'')
        +(l.hw?'<div style="margin-top:2px">📝 '+l.hw+'</div>':'')
        +(l.literature?'<div style="margin-top:2px;color:#8b5cf6">📚 '+l.literature+'</div>':'')
        +(l.games?'<div style="color:var(--adm);margin-top:2px">🎧 '+l.games+'</div>':'')
        +(!l.notes&&!l.hw&&!l.games&&!l.literature?'—':'')
      +'</td>'
      +'<td>'+bst(l.status)+'</td>'
      +'<td><div style="display:flex;gap:3px">'+btns+'</div></td></tr>';
  }).join('')
  : '<tr><td colspan="10"><div class="empty"><div class="ei">📚</div>Занять немає</div></td></tr>';
  document.getElementById('lt-table').innerHTML = html;
}

function renderPayments(){
  // Show invoice toolbar only for god/director
  var invToolbar = document.getElementById('inv-toolbar');
  if(invToolbar) invToolbar.style.display = (R()==='god'||R()==='director') ? 'block' : 'none';

  // Допоміжна функція вартості заняття (ставка за годину × тривалість)
  function lessonAmt(l){
    return lessonTotal(l);
  }

  var myLess = myLessons();
  // Отримано = проведені заняття
  var paid = myLess.filter(function(l){
    return isDoneLesson(l)||l.status==='makeup';
  }).reduce(function(a,l){ return a+lessonAmt(l); }, 0);

  // Очікується = заплановані заняття
  var pend = myLess.filter(function(l){
    return l.status==='planned'||l.status==='scheduled';
  }).reduce(function(a,l){ return a+lessonAmt(l); }, 0);

  // Різниця = Очікується − Отримано
  var diff = pend - paid;

  document.getElementById('py-paid').textContent = Math.round(paid).toLocaleString('uk-UA')+'\u20B4';
  document.getElementById('py-pend').textContent = Math.round(pend).toLocaleString('uk-UA')+'\u20B4';
  var overEl = document.getElementById('py-over');
  if(overEl){
    overEl.textContent = (diff>=0?'+':'')+Math.round(diff).toLocaleString('uk-UA')+'\u20B4';
    overEl.style.color = diff<0 ? 'var(--danger)' : diff>0 ? 'var(--tut)' : '';
  }
  var mm={cash:'\u0413\u043E\u0442\u0456\u0432\u043A\u0430',card:'\u041A\u0430\u0440\u0442\u043A\u0430',transfer:'\u041F\u0435\u0440\u0435\u043A\u0430\u0437'};
  var ce=can('payments');
  var data=[].concat(S.payments).sort(function(a,b){return new Date(b.date)-new Date(a.date);});
  var html=data.length?data.map(function(p){
    var btns=ce
      ?('<button class="btn btn-g btn-sm" onclick="openPayM(this.dataset.id)" data-id="'+p.id+'">\u270F\uFE0F</button>'
        +'<button class="btn btn-sm" style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger)" onclick="delPay(this.dataset.id)" data-id="'+p.id+'">\uD83D\uDDD1</button>')
      :'\u2014';
    // Іконка способу оплати — швидше зчитується, ніж текст
    var MI={cash:'\ud83d\udcb5',card:'\ud83d\udcb3',transfer:'\ud83c\udfe6'};
    var st=(S.students||[]).find(function(x){return x.id===p.studentId;});
    var amt=p.amount||0;
    return '<tr>'
      +'<td><div style="display:flex;align-items:center;gap:9px">'
        +mkAv(st?st.fn:'?', st?st.ln:'', 32, st?st.photo:null)
        +'<div><div style="font-weight:600;font-size:13px">'+sn(p.studentId)+'</div>'
        +'<div style="font-size:10.5px;color:var(--t3)">'+fd(p.date)+'</div></div></div></td>'
      +'<td><span style="font-family:JetBrains Mono,monospace;font-size:15px;font-weight:800;'
        +'color:'+(amt>0?'var(--tut)':'var(--t3)')+'">'
        +amt.toLocaleString('uk-UA')+'\u20B4</span></td>'
      +'<td><span style="display:inline-flex;align-items:center;gap:5px;font-size:12.5px">'
        +(MI[p.method]||'\ud83d\udcb0')+' '+(mm[p.method]||p.method)+'</span></td>'
      +'<td style="font-size:11px">'+fd(p.date)+'</td>'
      +'<td style="font-size:12px">'+(p.month||'\u2014')+'</td>'
      +'<td style="font-size:12px;color:var(--t2)">'+(p.note||'\u2014')+'</td>'
      +'<td>'+bst(p.status)+'</td>'
      +'<td><div style="display:flex;gap:3px">'+btns+'</div></td>'
      +'</tr>';
  }).join(''):'<tr><td colspan="8"><div class="empty"><div class="ei">\uD83D\uDCB3</div>'
      +'<b>\u041f\u043b\u0430\u0442\u0435\u0436\u0456\u0432 \u0449\u0435 \u043d\u0435\u043c\u0430\u0454</b>'
      +'<div class="eh">\u0417\u0430\u043f\u0438\u0441\u0438 \u043f\u0440\u043e \u043e\u043f\u043b\u0430\u0442\u0438 \u0437\u2019\u044f\u0432\u043b\u044f\u0442\u044c\u0441\u044f \u0442\u0443\u0442 \u043f\u0456\u0441\u043b\u044f \u0434\u043e\u0434\u0430\u0432\u0430\u043d\u043d\u044f</div>'
      +'</div></td></tr>';
  document.getElementById('pt-table').innerHTML=html;
}
function renderCustomPage(pageId){
  var pel=document.getElementById('pg-'+pageId);
  if(!pel){
    pel=document.createElement('div');
    pel.className='page';
    pel.id='pg-'+pageId;
    document.getElementById('content').appendChild(pel);
  }
  var cfg=(S.godConfig)||{};
  var navItems=cfg.navItems||[].concat(NAV_CFG);
  var pageInfo=navItems.find(function(n){return n.id===pageId;})||{lbl:'\u0412\u043B\u0430\u0441\u043D\u0430 \u0441\u0442\u043E\u0440\u0456\u043D\u043A\u0430',ico:'\u2B50'};
  var notes=(cfg.customPageNotes||{})[pageId]||'';
  var saveBtn=R()==='god'
    ?('<button class="btn btn-p btn-sm" style="margin-top:8px" onclick="saveCustomPageNotes(this.dataset.pid)" data-pid="'+pageId+'">\uD83D\uDCBE \u0417\u0431\u0435\u0440\u0435\u0433\u0442\u0438 \u043D\u043E\u0442\u0430\u0442\u043A\u0438</button>')
    :'';
  pel.innerHTML=''
    +'<div class="god-banner" style="margin-bottom:16px">'
      +'<span class="god-banner-icon">'+pageInfo.ico+'</span>'
      +'<div><div class="god-banner-title">'+pageInfo.lbl+'</div>'
      +'<div class="god-banner-text">\u0412\u043B\u0430\u0441\u043D\u0430 \u0441\u0442\u043E\u0440\u0456\u043D\u043A\u0430, \u0441\u0442\u0432\u043E\u0440\u0435\u043D\u0430 \u0447\u0435\u0440\u0435\u0437 \u041a\u043e\u043d\u0441\u0442\u0440\u0443\u043a\u0442\u043e\u0440 \u0456\u043d\u0442\u0435\u0440\u0444\u0435\u0439\u0441\u0443</div></div>'
    +'</div>'
    +'<div class="card">'
      +'<div class="ch"><span class="ct">\u0412\u043c\u0456\u0441\u0442 \u0441\u0442\u043e\u0440\u0456\u043d\u043a\u0438 \u00ab'+pageInfo.lbl+'\u00bb</span></div>'
      +'<div style="padding:24px">'
        +'<div style="margin-bottom:14px">'
          +'<label style="font-size:11px;color:var(--t2);font-weight:600;text-transform:uppercase;letter-spacing:.4px">\u041d\u043e\u0442\u0430\u0442\u043a\u0438 / \u043e\u043f\u0438\u0441 \u0441\u0442\u043e\u0440\u0456\u043d\u043a\u0438</label>'
          +'<textarea id="custom-page-notes-'+pageId+'" style="width:100%;margin-top:6px;min-height:120px;font-size:13px" placeholder="\u0414\u043e\u0434\u0430\u0439\u0442\u0435 \u043e\u043f\u0438\u0441 \u0430\u0431\u043e \u043d\u043e\u0442\u0430\u0442\u043a\u0438...">'+notes+'</textarea>'
          +saveBtn
        +'</div>'
        +'<div style="margin-top:20px;padding:16px;background:var(--s2);border-radius:10px;border:1px dashed var(--b2)">'
          +'<div style="font-size:12px;color:var(--t3);text-align:center">'
            +'<div style="font-size:24px;margin-bottom:8px">\uD83D\uDEA7</div>'
            +'\u0426\u044f \u0441\u0442\u043e\u0440\u0456\u043d\u043a\u0430 \u043f\u043e\u0440\u043e\u0436\u043d\u044f.'
          +'</div>'
        +'</div>'
      +'</div>'
    +'</div>';
  pel.classList.add('active');
}

function saveCustomPageNotes(pageId){
  var cfg=(S.godConfig)||{};
  if(!cfg.customPageNotes) cfg.customPageNotes={};
  var el=document.getElementById('custom-page-notes-'+pageId);
  cfg.customPageNotes[pageId]=el?el.value:'';
  gcSave('customPageNotes',cfg.customPageNotes);
  mkToast('\u041d\u043e\u0442\u0430\u0442\u043a\u0438 \u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043d\u043e');
}
