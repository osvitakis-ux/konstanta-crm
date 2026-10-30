
function toggleSidebar(){
  var sb=document.querySelector('.sb');
  var ov=document.getElementById('sb-overlay');
  if(!sb)return;
  sb.classList.toggle('open');
  if(ov)ov.classList.toggle('open',sb.classList.contains('open'));
}

function closeSidebar(){
  var sb=document.querySelector('.sb');
  var ov=document.getElementById('sb-overlay');
  if(sb)sb.classList.remove('open');
  if(ov)ov.classList.remove('open');
}

function myLessons(){
  var all = filterByBranch(S.lessons || []);
  if(P().seeAll) return all;
  var cuId = CU ? CU.id : null;
  var mt = S.tutors.find(function(t){
    return t.accId === cuId || t.acc_uid === cuId;
  });
  if(!mt) return [];
  return all.filter(function(l){
    return l.tutorId === mt.id || l.tutor_id === mt.id;
  });
}
function myStudents(){
  var all = filterByBranch(S.students || []);
  try{ if(P().seeAll) return all; }catch(e){ return all; }
  if(R() !== 'tutor') return all;
  var cuId = CU ? CU.id : null;
  var mt = S.tutors.find(function(t){
    return t.accId === cuId || t.acc_uid === cuId || t.id === cuId;
  });
  if(!mt) return all; // fallback: show all if tutor record not found
  return all.filter(function(s){
    if(s.tutorId === mt.id || s.tutor_id === mt.id) return true;
    if(Array.isArray(s.tutorIds) && s.tutorIds.indexOf(mt.id) >= 0) return true;
    // also check raw tutor_ids string
    if(typeof s.tutor_ids === 'string' && s.tutor_ids.split(',').indexOf(mt.id) >= 0) return true;
    return false;
  });
}
function myTutor(){
  var cuId = CU ? CU.id : null;
  return S.tutors.find(function(t){
    return t.accId === cuId || t.acc_uid === cuId;
  }) || null;
}
function autoFillPrice(){
  const subj   = document.getElementById('l-subj')?.value || '';
  const tutorId= document.getElementById('l-tutor')?.value || '';
  const dur    = document.getElementById('l-dur')?.value || 60;
  // Get student grade
  const stId   = document.getElementById('l-std')?.value || '';
  const st     = (S.students||[]).find(s=>s.id===stId);
  const grade  = st?.grade || '';
  const price  = calcPrice(subj, tutorId, grade, dur);
  const pEl    = document.getElementById('l-price');
  if(pEl && price) pEl.value = price;
  if(price) mkToast('\u0426\u0456\u043D\u0443 \u043F\u0456\u0434\u0456\u0431\u0440\u0430\u043D\u043E: '+price+' \u20B4','info');
  else mkToast('\u041F\u0440\u0430\u0432\u0438\u043B\u043E \u0446\u0456\u043D\u0438 \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E','error');
}

function getWeekRange(offset){
  var now=new Date();
  var day=now.getDay()===0?6:now.getDay()-1;
  var mon=new Date(now); mon.setDate(now.getDate()-day+offset*7); mon.setHours(0,0,0,0);
  var sun=new Date(mon); sun.setDate(mon.getDate()+6); sun.setHours(23,59,59,999);
  var fmt=function(d){return d.toLocaleDateString('uk-UA',{day:'numeric',month:'short'});};
  return {mon:mon,sun:sun,label:fmt(mon)+' \u2014 '+fmt(sun)};
}

function inWeek(dateStr,wr){
  if(!dateStr)return false;
  var d=new Date(dateStr+'T12:00:00');
  return d>=wr.mon&&d<=wr.sun;
}

function dashKpiWeek(dir){
  if(dir===0)S.dashWeekOffset=0;
  else S.dashWeekOffset=(S.dashWeekOffset||0)+dir;
  renderDashKpi();
  renderDashTrends();
}

// ═══════════════════════════════════════════════════════════════
//  РЕЙТИНГ РЕПЕТИТОРА
//  Показує, наскільки сумлінно ведеться електронний журнал.
//  Рахується з наявних даних, без окремих таблиць.
// ═══════════════════════════════════════════════════════════════
var RATING_WEIGHTS = { journal:30, hw:20, timely:25, forgotten:25 };

/** Заняття репетитора за місяць (period = 'YYYY-MM') */
/**
 * Заняття репетитора за період.
 * period — або 'YYYY-MM' (місяць), або {from:'YYYY-MM-DD', to:'YYYY-MM-DD'}
 */
function ratingLessons(tutorId, period){
  return (S.lessons||[]).filter(function(l){
    if((l.tutorId||l.tutor_id)!==tutorId) return false;
    var d=String(l.date||'');
    if(period && typeof period==='object') return d>=period.from && d<=period.to;
    return d.slice(0,7)===period;
  });
}

/** Поточний період рейтингу: місяць або тиждень зі зсувом */
var rtMode='month', rtOffset=0;
function rtPeriod(){
  var now=new Date();
  if(rtMode==='week'){
    var dow=now.getDay()===0?6:now.getDay()-1;
    var mon=new Date(now); mon.setDate(now.getDate()-dow+rtOffset*7); mon.setHours(0,0,0,0);
    var sun=new Date(mon); sun.setDate(mon.getDate()+6);
    var f=function(d){return d.toLocaleDateString('uk-UA',{day:'numeric',month:'short'});};
    return {from:localDateStr(mon), to:localDateStr(sun), label:f(mon)+' \u2014 '+f(sun), isWeek:true};
  }
  var d=new Date(now.getFullYear(), now.getMonth()+rtOffset, 1);
  var key=d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0');
  return {key:key, label:prMonthName(key), isWeek:false};
}
function rtSetMode(m){ rtMode=m; rtOffset=0; renderRatingBlock(); }
function rtShift(dir){ if(dir===0) rtOffset=0; else rtOffset+=dir; renderRatingBlock(); }
window.rtSetMode=rtSetMode; window.rtShift=rtShift;

/** Скільки днів минуло від заняття до моменту, коли його відзначили */
function markDelayDays(l){
  if(!l.updated_at || !l.date) return null;
  var lessonDay=new Date(l.date+'T23:59:59').getTime();
  var marked=new Date(l.updated_at).getTime();
  if(isNaN(lessonDay)||isNaN(marked)) return null;
  var d=(marked-lessonDay)/86400000;
  return d<0?0:d;   // відзначив у день заняття або раніше — без затримки
}

/**
 * Рейтинг репетитора 0–100 із розбивкою по складових
 * і переліком конкретних недоробок.
 */
/**
 * СУВОРА ПЕРЕВІРКА ЖУРНАЛУ — пошук ознак формального заповнення.
 *
 * Усе рахується з наявних даних: дати/часу заняття та моменту, коли його
 * востаннє змінювали (updated_at). Це не доводить обману, але показує
 * місця, які варто перевірити людині.
 */
function journalRedFlags(tutorId, period){
  var all = ratingLessons(tutorId, period);
  // Згорілі заняття виключаємо: журнал для них заповнювати нічого,
  // тож вимагати тему й ДЗ було б несправедливо
  var done = all.filter(function(l){ return isDoneLesson(l) && !isBurnedLesson(l) && l.status!=='testing'; });

  var flags = {
    beforeStart: [],   // відмічено ДО початку заняття
    bulk: [],          // пачка занять закрита за одну хвилину
    nightly: [],       // відмічено вночі (00:00–05:59)
    shortTopic: [],    // тема з 1–3 символів
    repeated: [],      // однакова тема поспіль
    longDelay: []      // відмічено пізніше ніж за 7 днів
  };

  // ── 1. Відмічено раніше, ніж заняття мало початися ──
  done.forEach(function(l){
    if(!l.updated_at || !l.date || !l.time) return;
    var startMs = new Date(l.date + 'T' + l.time + ':00').getTime();
    var markMs  = new Date(l.updated_at).getTime();
    if(isNaN(startMs) || isNaN(markMs)) return;
    // 10 хвилин допуску: могли відмітити перед самим початком
    if(markMs < startMs - 10*60*1000) flags.beforeStart.push(l);
  });

  // ── 2. Масове закриття: 5+ занять в одну хвилину ──
  var byMinute = {};
  done.forEach(function(l){
    if(!l.updated_at) return;
    var key = String(l.updated_at).slice(0,16); // до хвилини
    (byMinute[key] = byMinute[key] || []).push(l);
  });
  Object.keys(byMinute).forEach(function(k){
    if(byMinute[k].length >= 5) flags.bulk = flags.bulk.concat(byMinute[k]);
  });

  // ── 3. Відмітки серед ночі ──
  done.forEach(function(l){
    if(!l.updated_at) return;
    var h = new Date(l.updated_at).getHours();
    if(h >= 0 && h < 6) flags.nightly.push(l);
  });

  // ── 4. Надто короткі теми ──
  done.forEach(function(l){
    var t = String(l.notes||'').trim();
    if(t && t.length <= 3) flags.shortTopic.push(l);
  });

  // ── 5. Однакова тема в кількох заняттях поспіль ──
  var sorted = done.slice().sort(function(a,b){
    return String(a.date+(a.time||'')).localeCompare(String(b.date+(b.time||'')));
  });
  var run = [];
  for(var i=0;i<sorted.length;i++){
    var cur = String(sorted[i].notes||'').trim().toLowerCase();
    var prev = i>0 ? String(sorted[i-1].notes||'').trim().toLowerCase() : null;
    if(cur && cur === prev){
      if(!run.length) run.push(sorted[i-1]);
      run.push(sorted[i]);
    } else {
      if(run.length >= 3) flags.repeated = flags.repeated.concat(run);
      run = [];
    }
  }
  if(run.length >= 3) flags.repeated = flags.repeated.concat(run);

  // ── 6. Дуже пізні відмітки ──
  done.forEach(function(l){
    var d = markDelayDays(l);
    if(d !== null && d > 7) flags.longDelay.push(l);
  });

  // Скільки унікальних занять потребує уваги
  var flagged = {};
  Object.keys(flags).forEach(function(k){
    flags[k].forEach(function(l){ flagged[l.id]=true; });
  });

  return {
    flags: flags,
    flaggedCount: Object.keys(flagged).length,
    doneCount: done.length,
    // Штраф до рейтингу: до 30 пунктів
    penalty: Math.min(30, Object.keys(flagged).length * 5)
  };
}
window.journalRedFlags = journalRedFlags;

function tutorRating(tutorId, period){
  period = period || new Date().toISOString().slice(0,7);
  var all = ratingLessons(tutorId, period);
  var todayStr = localDateStr(new Date());

  // Проведені — саме їх журнал і має бути заповнений
  // Згорілі заняття виключаємо: журнал для них заповнювати нічого,
  // тож вимагати тему й ДЗ було б несправедливо
  var done = all.filter(function(l){ return isDoneLesson(l) && !isBurnedLesson(l) && l.status!=='testing'; });
  // Забуті: дата минула, а статус досі "заплановане"
  var forgotten = all.filter(function(l){
    return (l.status==='planned'||l.status==='scheduled'||!l.status)
        && String(l.date||'') < todayStr;
  });

  var noTopic = done.filter(function(l){ return !String(l.notes||'').trim(); });
  var noHw    = done.filter(function(l){ return !String(l.hw||'').trim(); });

  // ── Складові ──
  var pJournal = done.length ? Math.round((done.length-noTopic.length)/done.length*100) : 100;
  var pHw      = done.length ? Math.round((done.length-noHw.length)/done.length*100) : 100;

  // Вчасність: чим швидше відзначено заняття, тим краще
  var delays = done.map(markDelayDays).filter(function(d){ return d!==null; });
  var avgDelay = delays.length ? delays.reduce(function(a,b){return a+b;},0)/delays.length : 0;
  var pTimely = 100;
  if(delays.length){
    if(avgDelay<=1) pTimely=100;
    else if(avgDelay<=3) pTimely=75;
    else if(avgDelay<=7) pTimely=45;
    else pTimely=10;
  }

  // Забуті заняття: кожне знімає 20 пунктів цієї складової
  var pForgotten = Math.max(0, 100 - forgotten.length*20);

  var score = Math.round(
      pJournal   * RATING_WEIGHTS.journal   / 100
    + pHw        * RATING_WEIGHTS.hw        / 100
    + pTimely    * RATING_WEIGHTS.timely    / 100
    + pForgotten * RATING_WEIGHTS.forgotten / 100
  );

  // Сувора перевірка: ознаки формального заповнення знижують рейтинг
  var rf = journalRedFlags(tutorId, period);
  score = Math.max(0, score - rf.penalty);

  return {
    score: score,
    lessonsTotal: all.length,
    doneCount: done.length,
    parts: {
      journal:{ pct:pJournal, weight:RATING_WEIGHTS.journal, label:'\u0422\u0435\u043c\u0430 \u0443\u0440\u043e\u043a\u0443' },
      hw:{ pct:pHw, weight:RATING_WEIGHTS.hw, label:'\u0414\u043e\u043c\u0430\u0448\u043d\u0454 \u0437\u0430\u0432\u0434\u0430\u043d\u043d\u044f' },
      timely:{ pct:pTimely, weight:RATING_WEIGHTS.timely, label:'\u0412\u0447\u0430\u0441\u043d\u0456\u0441\u0442\u044c \u0432\u0456\u0434\u043c\u0456\u0442\u043e\u043a' },
      forgotten:{ pct:pForgotten, weight:RATING_WEIGHTS.forgotten, label:'\u0411\u0435\u0437 \u0437\u0430\u0431\u0443\u0442\u0438\u0445 \u0437\u0430\u043d\u044f\u0442\u044c' }
    },
    issues: { forgotten:forgotten, noTopic:noTopic, noHw:noHw, avgDelay:Math.round(avgDelay*10)/10 },
    redFlags: rf
  };
}
window.tutorRating = tutorRating;

/** Колір за значенням рейтингу */
function ratingColor(score){
  if(score>=90) return '#22c55e';
  if(score>=75) return '#84cc16';
  if(score>=60) return '#f59e0b';
  return '#ef4444';
}

/** Круговий індикатор */
function ratingDial(score, prevScore){
  var col=ratingColor(score), R=52, C=2*Math.PI*R;
  var off=C*(1-score/100);
  var trend='';
  if(prevScore!=null && prevScore!==score){
    var d=score-prevScore;
    trend='<div class="rt-trend" style="color:'+(d>0?'#22c55e':'#ef4444')+'">'
        +(d>0?'\u2191 +':'\u2193 ')+d+' \u0434\u043e \u043c\u0438\u043d\u0443\u043b\u043e\u0433\u043e</div>';
  }
  return '<div class="rt-dial">'
    +'<svg width="120" height="120" viewBox="0 0 120 120">'
      +'<circle cx="60" cy="60" r="'+R+'" fill="none" stroke="var(--b1)" stroke-width="10"/>'
      +'<circle cx="60" cy="60" r="'+R+'" fill="none" stroke="'+col+'" stroke-width="10"'
        +' stroke-linecap="round" stroke-dasharray="'+C.toFixed(1)+'"'
        +' stroke-dashoffset="'+off.toFixed(1)+'"/>'
    +'</svg>'
    +'<div class="rt-dial-txt"><div class="rt-score" style="color:'+col+'">'+score+'</div>'+trend+'</div>'
  +'</div>';
}

/** Блок рейтингу на дашборді */
/**
 * Хто має право бачити й контролювати рейтинг ведення журналів.
 * Завжди: бог системи та директор.
 * Додатково: ОДИН адміністратор, призначений у налаштуваннях.
 */
function canReviewJournals(){
  var r=R();
  if(r==='god'||r==='director') return true;
  if(r!=='admin') return false;
  var assigned=(S.settings||{}).journal_reviewer;
  return !!assigned && CU && CU.id===assigned;
}
window.canReviewJournals=canReviewJournals;

/** Заповнює список адміністраторів у налаштуваннях */
function renderJournalReviewerSetting(){
  var sec=document.getElementById('journal-reviewer-section');
  if(!sec) return;
  // Призначати відповідального може лише бог або директор
  var canAssign=(R()==='god'||R()==='director');
  sec.style.display=canAssign?'block':'none';
  if(!canAssign) return;
  var sel=document.getElementById('set-journal-reviewer');
  if(!sel) return;
  var admins=(S.users||[]).filter(function(u){return u.role==='admin';})
    .sort(function(a,b){return ((a.fn||'')+' '+(a.ln||'')).localeCompare((b.fn||'')+' '+(b.ln||''),'uk');});
  sel.innerHTML='<option value="">\u2014 \u043d\u0435 \u043f\u0440\u0438\u0437\u043d\u0430\u0447\u0435\u043d\u043e \u2014</option>'
    +admins.map(function(u){return '<option value="'+u.id+'">'+(u.fn||'')+' '+(u.ln||'')+'</option>';}).join('');
  sel.value=(S.settings||{}).journal_reviewer||'';
  if(!admins.length){
    sel.innerHTML='<option value="">\u2014 \u043d\u0435\u043c\u0430\u0454 \u0430\u0434\u043c\u0456\u043d\u0456\u0441\u0442\u0440\u0430\u0442\u043e\u0440\u0456\u0432 \u2014</option>';
  }
}
window.renderJournalReviewerSetting=renderJournalReviewerSetting;

/** Зберігає відповідального адміністратора */
async function saveJournalReviewer(){
  if(!(R()==='god'||R()==='director')){ mkToast('\u041d\u0435\u043c\u0430\u0454 \u043f\u0440\u0430\u0432','error'); return; }
  var val=(document.getElementById('set-journal-reviewer')||{value:''}).value||null;
  try{
    await _sb.from('settings').upsert({ id:'main', journal_reviewer:val, updated_at:new Date().toISOString() });
    S.settings=Object.assign({}, S.settings||{}, {journal_reviewer:val});
    var who=val?((S.users||[]).find(function(u){return u.id===val;})||{}) : null;
    mkToast(val
      ? '\u2705 \u0412\u0456\u0434\u043f\u043e\u0432\u0456\u0434\u0430\u043b\u044c\u043d\u0438\u0439: '+((who.fn||'')+' '+(who.ln||''))
      : '\u0412\u0456\u0434\u043f\u043e\u0432\u0456\u0434\u0430\u043b\u044c\u043d\u043e\u0433\u043e \u0437\u043d\u044f\u0442\u043e');
  }catch(e){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(e.message||e),'error'); }
}
window.saveJournalReviewer=saveJournalReviewer;

function renderRatingBlock(){
  var wrap=document.getElementById('dash-rating');
  if(!wrap) return;
  var role=R();

  // ── Репетитор бачить СВІЙ рейтинг ──
  if(role==='tutor'){
    var mt=myTutor();
    if(!mt){ wrap.innerHTML=''; return; }
    var P=rtPeriod();
    var per = P.isWeek ? {from:P.from,to:P.to} : P.key;
    // Попередній період для порівняння
    var prevPer;
    if(P.isWeek){
      var pm=new Date(P.from+'T12:00:00'); pm.setDate(pm.getDate()-7);
      var ps=new Date(pm); ps.setDate(pm.getDate()+6);
      prevPer={from:localDateStr(pm), to:localDateStr(ps)};
    } else {
      var pd=new Date(P.key+'-01T12:00:00'); pd.setMonth(pd.getMonth()-1);
      prevPer=pd.getFullYear()+'-'+String(pd.getMonth()+1).padStart(2,'0');
    }
    var r=tutorRating(mt.id, per);
    var prev=tutorRating(mt.id, prevPer);
    var hasPrev = prev.lessonsTotal>0;

    if(!r.lessonsTotal){
      wrap.innerHTML='<div class="rt-card"><div class="rt-body">'
        +'<div class="rt-title">\ud83d\udcd6 \u0412\u0435\u0434\u0435\u043d\u043d\u044f \u0436\u0443\u0440\u043d\u0430\u043b\u0443</div>'
        +'<div class="rt-sub" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">'
          +'<span style="display:inline-flex;gap:3px">'
            +'<button class="btn btn-g btn-sm rt-nav-btn" style="font-size:11px'+(rtMode==='week'?';background:var(--adm);color:#fff':'')+'" onclick="rtSetMode(\'week\')">\u0422\u0438\u0436\u0434\u0435\u043d\u044c</button>'
            +'<button class="btn btn-g btn-sm rt-nav-btn" style="font-size:11px'+(rtMode==='month'?';background:var(--adm);color:#fff':'')+'" onclick="rtSetMode(\'month\')">\u041c\u0456\u0441\u044f\u0446\u044c</button>'
          +'</span>'
          +'<span style="display:inline-flex;gap:3px;align-items:center">'
            +'<button class="btn btn-g btn-sm rt-nav-btn" style="font-size:11px" onclick="rtShift(-1)">\u2190</button>'
            +'<b style="min-width:110px;text-align:center">'+P.label+'</b>'
            +'<button class="btn btn-g btn-sm rt-nav-btn" style="font-size:11px" onclick="rtShift(1)">\u2192</button>'
            +(rtOffset!==0?'<button class="btn btn-g btn-sm rt-nav-btn" style="font-size:11px" onclick="rtShift(0)">\u0417\u0430\u0440\u0430\u0437</button>':'')
          +'</span>'
          +'<span>\u041d\u0435\u043c\u0430\u0454 \u0437\u0430\u043d\u044f\u0442\u044c \u0437\u0430 \u0446\u0435\u0439 \u043f\u0435\u0440\u0456\u043e\u0434</span>'
        +'</div>'
        +'</div></div>';
      return;
    }

    // Смужки складових
    var bars='';
    ['journal','hw','timely','forgotten'].forEach(function(k){
      var p=r.parts[k];
      bars+='<div class="rt-bar-row">'
        +'<span class="rt-bar-lbl">'+p.label+'</span>'
        +'<span class="rt-bar-track"><span class="rt-bar-fill" style="width:'+p.pct+'%;background:'+ratingColor(p.pct)+'"></span></span>'
        +'<span class="rt-bar-val" style="color:'+ratingColor(p.pct)+'">'+p.pct+'%</span>'
      +'</div>';
    });

    // Конкретні недоробки — з можливістю одразу виправити
    var iss='', I=r.issues;
    if(I.forgotten.length){
      var dates=I.forgotten.slice(0,4).map(function(l){var d=String(l.date||'').split('-');return d.length===3?d[2]+'.'+d[1]:l.date;}).join(', ')
              +(I.forgotten.length>4?' \u0442\u0430 \u0456\u043d.':'');
      iss+='<div class="rt-issue rt-issue-bad" onclick="ratingFixOpen(\''+I.forgotten[0].id+'\')">'
        +'<span>\u274c</span><span class="rt-issue-txt"><b>'+I.forgotten.length+'</b> '
        +prPlural(I.forgotten.length,'\u0437\u0430\u043d\u044f\u0442\u0442\u044f \u043d\u0435 \u0432\u0456\u0434\u0437\u043d\u0430\u0447\u0435\u043d\u043e','\u0437\u0430\u043d\u044f\u0442\u0442\u044f \u043d\u0435 \u0432\u0456\u0434\u0437\u043d\u0430\u0447\u0435\u043d\u043e','\u0437\u0430\u043d\u044f\u0442\u044c \u043d\u0435 \u0432\u0456\u0434\u0437\u043d\u0430\u0447\u0435\u043d\u043e')
        +' \u2014 '+dates+'</span><span class="rt-issue-go">\u0432\u0438\u043f\u0440\u0430\u0432\u0438\u0442\u0438 \u2192</span></div>';
    }
    if(I.noTopic.length){
      iss+='<div class="rt-issue rt-issue-warn" onclick="ratingFixOpen(\''+I.noTopic[0].id+'\')">'
        +'<span>\ud83d\udcdd</span><span class="rt-issue-txt"><b>'+I.noTopic.length+'</b> '
        +prPlural(I.noTopic.length,'\u0437\u0430\u043d\u044f\u0442\u0442\u044f \u0431\u0435\u0437 \u0442\u0435\u043c\u0438 \u0443\u0440\u043e\u043a\u0443','\u0437\u0430\u043d\u044f\u0442\u0442\u044f \u0431\u0435\u0437 \u0442\u0435\u043c\u0438 \u0443\u0440\u043e\u043a\u0443','\u0437\u0430\u043d\u044f\u0442\u044c \u0431\u0435\u0437 \u0442\u0435\u043c\u0438 \u0443\u0440\u043e\u043a\u0443')
        +'</span><span class="rt-issue-go">\u0437\u0430\u043f\u043e\u0432\u043d\u0438\u0442\u0438 \u2192</span></div>';
    }
    if(I.noHw.length){
      iss+='<div class="rt-issue rt-issue-warn" onclick="ratingFixOpen(\''+I.noHw[0].id+'\')">'
        +'<span>\ud83d\udcda</span><span class="rt-issue-txt"><b>'+I.noHw.length+'</b> '
        +prPlural(I.noHw.length,'\u0437\u0430\u043d\u044f\u0442\u0442\u044f \u0431\u0435\u0437 \u0414\u0417','\u0437\u0430\u043d\u044f\u0442\u0442\u044f \u0431\u0435\u0437 \u0414\u0417','\u0437\u0430\u043d\u044f\u0442\u044c \u0431\u0435\u0437 \u0414\u0417')
        +'</span><span class="rt-issue-go">\u0437\u0430\u043f\u043e\u0432\u043d\u0438\u0442\u0438 \u2192</span></div>';
    }
    if(I.avgDelay>3){
      iss+='<div class="rt-issue rt-issue-warn" style="cursor:default">'
        +'<span>\u23f1</span><span class="rt-issue-txt">\u0421\u0435\u0440\u0435\u0434\u043d\u044f \u0437\u0430\u0442\u0440\u0438\u043c\u043a\u0430 \u0432\u0456\u0434\u043c\u0456\u0442\u043a\u0438: <b>'+I.avgDelay+'</b> \u0434\u043d.</span></div>';
    }
    if(!iss){
      iss='<div class="rt-issue rt-issue-ok"><span>\u2705</span>'
        +'<span class="rt-issue-txt">\u0416\u0443\u0440\u043d\u0430\u043b \u0432\u0435\u0434\u0435\u0442\u044c\u0441\u044f \u0431\u0435\u0437\u0434\u043e\u0433\u0430\u043d\u043d\u043e \u2014 \u0436\u043e\u0434\u043d\u0438\u0445 \u0437\u0430\u0443\u0432\u0430\u0436\u0435\u043d\u044c</span></div>';
    }

    // Бейджі за досягнення
    var badges='';
    if(r.score===100){
      badges+='<span class="rt-badge">\ud83c\udfc6 \u0406\u0434\u0435\u0430\u043b\u044c\u043d\u0438\u0439 \u043c\u0456\u0441\u044f\u0446\u044c</span>';
      // Святкуємо один раз за період — а не при кожному відкритті дашборду
      try{
        var _pk=(P&&P.isWeek)?('week-'+P.from):('month-'+(P?P.key:''));
        celebrateOnce('rating100-'+mt.id+'-'+_pk,
          '\ud83c\udfc6 \u0406\u0434\u0435\u0430\u043b\u044c\u043d\u0438\u0439 \u0436\u0443\u0440\u043d\u0430\u043b! \u0412\u0456\u0442\u0430\u0454\u043c\u043e!');
      }catch(e){}
    }
    if(!I.forgotten.length&&r.doneCount>=5) badges+='<span class="rt-badge">\u2705 \u0416\u043e\u0434\u043d\u043e\u0433\u043e \u0437\u0430\u0431\u0443\u0442\u043e\u0433\u043e</span>';
    if(I.avgDelay<=1&&r.doneCount>=5) badges+='<span class="rt-badge">\u26a1 \u0412\u0456\u0434\u043c\u0456\u0442\u043a\u0438 \u0432 \u0434\u0435\u043d\u044c \u0437\u0430\u043d\u044f\u0442\u0442\u044f</span>';
    if(r.parts.journal.pct===100&&r.doneCount>=5) badges+='<span class="rt-badge">\ud83d\udcdd \u0416\u0443\u0440\u043d\u0430\u043b \u0431\u0435\u0437 \u043f\u0440\u043e\u043f\u0443\u0441\u043a\u0456\u0432</span>';

    wrap.innerHTML='<div class="rt-card">'
      +ratingDial(r.score, hasPrev?prev.score:null)
      +'<div class="rt-body">'
        +'<div class="rt-title">\ud83d\udcd6 \u0412\u0435\u0434\u0435\u043d\u043d\u044f \u0436\u0443\u0440\u043d\u0430\u043b\u0443</div>'
        +'<div class="rt-sub" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">'
          +'<span style="display:inline-flex;gap:3px">'
            +'<button class="btn btn-g btn-sm rt-nav-btn" style="font-size:11px'+(rtMode==='week'?';background:var(--adm);color:#fff':'')+'" onclick="rtSetMode(\'week\')">\u0422\u0438\u0436\u0434\u0435\u043d\u044c</button>'
            +'<button class="btn btn-g btn-sm rt-nav-btn" style="font-size:11px'+(rtMode==='month'?';background:var(--adm);color:#fff':'')+'" onclick="rtSetMode(\'month\')">\u041c\u0456\u0441\u044f\u0446\u044c</button>'
          +'</span>'
          +'<span style="display:inline-flex;gap:3px;align-items:center">'
            +'<button class="btn btn-g btn-sm rt-nav-btn" style="font-size:11px" onclick="rtShift(-1)">\u2190</button>'
            +'<b style="min-width:110px;text-align:center">'+P.label+'</b>'
            +'<button class="btn btn-g btn-sm rt-nav-btn" style="font-size:11px" onclick="rtShift(1)">\u2192</button>'
            +(rtOffset!==0?'<button class="btn btn-g btn-sm rt-nav-btn" style="font-size:11px" onclick="rtShift(0)">\u0417\u0430\u0440\u0430\u0437</button>':'')
          +'</span>'
          +'<span>'+r.doneCount+' \u043f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u0438\u0445 \u0437 '+r.lessonsTotal+'</span>'
        +'</div>'
        +'<div class="rt-bars">'+bars+'</div>'
        +'<div class="rt-issues">'+iss+'</div>'
        +(badges?'<div class="rt-badges">'+badges+'</div>':'')
      +'</div>'
    +'</div>';
    return;
  }

  // ── Керівництво бачить таблицю всіх репетиторів ──
  if(canReviewJournals()){
    var P2=rtPeriod();
    var per2 = P2.isWeek ? {from:P2.from,to:P2.to} : P2.key;
    var rows=(S.tutors||[]).map(function(t){ return {t:t, r:tutorRating(t.id, per2)}; })
      .filter(function(x){ return x.r.lessonsTotal>0; })
      .sort(function(a,b){ return a.r.score-b.r.score; }); // проблемні — зверху
    if(!rows.length){ wrap.innerHTML=''; return; }

    var html='<div class="card" style="margin-bottom:16px"><div class="ch">'
      +'<span class="ct">\ud83d\udcd6 \u0420\u0435\u0439\u0442\u0438\u043d\u0433 \u0432\u0435\u0434\u0435\u043d\u043d\u044f \u0436\u0443\u0440\u043d\u0430\u043b\u0456\u0432</span>'
      +'<span style="display:inline-flex;gap:3px;align-items:center;margin-left:auto">'
        +'<button class="btn btn-g btn-sm rt-nav-btn" style="font-size:11px'+(rtMode==='week'?';background:var(--adm);color:#fff':'')+'" onclick="rtSetMode(\'week\')">\u0422\u0438\u0436\u0434\u0435\u043d\u044c</button>'
        +'<button class="btn btn-g btn-sm rt-nav-btn" style="font-size:11px'+(rtMode==='month'?';background:var(--adm);color:#fff':'')+'" onclick="rtSetMode(\'month\')">\u041c\u0456\u0441\u044f\u0446\u044c</button>'
        +'<button class="btn btn-p btn-sm" style="font-size:11px;margin-right:5px" onclick="openWrapped()" title="\u041f\u0456\u0434\u0441\u0443\u043c\u043a\u0438 \u043c\u0456\u0441\u044f\u0446\u044f">\u2728 \u041f\u0456\u0434\u0441\u0443\u043c\u043a\u0438</button>'
        +'<button class="btn btn-g btn-sm rt-nav-btn" style="font-size:11px" onclick="rtShift(-1)">\u2190</button>'
        +'<b style="font-size:11px;min-width:105px;text-align:center">'+P2.label+'</b>'
        +'<button class="btn btn-g btn-sm rt-nav-btn" style="font-size:11px" onclick="rtShift(1)">\u2192</button>'
        +(rtOffset!==0?'<button class="btn btn-g btn-sm rt-nav-btn" style="font-size:11px" onclick="rtShift(0)">\u0417\u0430\u0440\u0430\u0437</button>':'')
      +'</span></div>'
      +'<div style="overflow-x:auto"><table><thead><tr>'
      +'<th>\u0420\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440</th><th style="text-align:center">\u0420\u0435\u0439\u0442\u0438\u043d\u0433</th>'
      +'<th style="text-align:center">\u0422\u0435\u043c\u0430</th><th style="text-align:center">\u0414\u0417</th>'
      +'<th style="text-align:center">\u0412\u0447\u0430\u0441\u043d\u043e</th><th style="text-align:center">\u041d\u0435 \u0432\u0456\u0434\u0437\u043d\u0430\u0447\u0435\u043d\u043e</th><th style="text-align:center">\u041f\u043e\u0442\u0440\u0435\u0431\u0443\u0454 \u0443\u0432\u0430\u0433\u0438</th>'
      +'</tr></thead><tbody>';
    rows.forEach(function(x){
      var col=ratingColor(x.r.score);
      var fg=x.r.issues.forgotten.length;
      html+='<tr>'
        +'<td><div style="display:flex;align-items:center;gap:9px">'+mkAv(x.t.fn,x.t.ln,30,x.t.photo)
          +'<div><div style="font-weight:600;font-size:13px">'+x.t.fn+' '+x.t.ln+'</div>'
          +'<div style="font-size:10.5px;color:var(--t3)">'+x.r.doneCount+' \u043f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u0438\u0445</div></div></div></td>'
        +'<td style="text-align:center"><span style="font-size:17px;font-weight:800;color:'+col+';font-family:\'JetBrains Mono\',monospace">'+x.r.score+'</span></td>'
        +'<td style="text-align:center;color:'+ratingColor(x.r.parts.journal.pct)+';font-weight:700">'+x.r.parts.journal.pct+'%</td>'
        +'<td style="text-align:center;color:'+ratingColor(x.r.parts.hw.pct)+';font-weight:700">'+x.r.parts.hw.pct+'%</td>'
        +'<td style="text-align:center;color:'+ratingColor(x.r.parts.timely.pct)+';font-weight:700">'+x.r.parts.timely.pct+'%</td>'
        +'<td style="text-align:center">'+(fg
            ? '<span class="badge br">'+fg+'</span>'
            : '<span style="color:var(--t3)">\u2014</span>')+'</td>'
        +'<td style="text-align:center">'+((x.r.redFlags&&x.r.redFlags.flaggedCount)
            ? '<span class="badge br" title="\u041e\u0437\u043d\u0430\u043a\u0438 \u0444\u043e\u0440\u043c\u0430\u043b\u044c\u043d\u043e\u0433\u043e \u0437\u0430\u043f\u043e\u0432\u043d\u0435\u043d\u043d\u044f">\u26a0\ufe0f '+x.r.redFlags.flaggedCount+'</span>'
            : '<span style="color:var(--t3)">\u2014</span>')+'</td>'
      +'</tr>';
    });
    html+='</tbody></table></div></div>';
    wrap.innerHTML=html;
    return;
  }
  wrap.innerHTML='';
}
window.renderRatingBlock=renderRatingBlock;

/** Відкриває заняття для виправлення просто з блоку рейтингу */
function ratingFixOpen(lessonId){
  if(typeof openLessM==='function') openLessM(lessonId);
}
window.ratingFixOpen=ratingFixOpen;
