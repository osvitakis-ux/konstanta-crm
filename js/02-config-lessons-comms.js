
var ROLES = {
  god: {
    label:'\u0411\u043E\u0433 \u0441\u0438\u0441\u0442\u0435\u043C\u0438', icon:'\u26A1', color:'var(--god2)',
    avatarBg:'linear-gradient(135deg,#2e3192,#5b60d4)',
    nav:['dashboard','students','tutors','schedule','lessons','comms','payments','payroll','acts','crm','contracts','leads','tasks','audit','invoice-log','invoice','reports','users','settings','telephony'],
    can:{students:true,tutors:true,lessons:true,payments:true,users:true,settings:true,danger:true,deleteAny:true},
    seeIncome:true, seeAll:true, canEditUsers:true, showGodBanner:true
  },
  director: {
    label:'\u0414\u0438\u0440\u0435\u043A\u0442\u043E\u0440', icon:'\uD83D\uDC51', color:'var(--dir)',
    avatarBg:'linear-gradient(135deg,#d9e021,#fcee21)',
    nav:['dashboard','students','tutors','schedule','lessons','comms','payments','payroll','acts','crm','leads','tasks','invoice','reports','users','settings','telephony'],
    can:{students:true,tutors:true,lessons:true,payments:true,users:true,settings:true,danger:false,deleteAny:true},
    seeIncome:true, seeAll:true, canEditUsers:true, showGodBanner:false
  },
  admin: {
    label:'\u0410\u0434\u043C\u0456\u043D\u0456\u0441\u0442\u0440\u0430\u0442\u043E\u0440', icon:'\uD83D\uDEE1\uFE0F', color:'var(--adm)',
    avatarBg:'linear-gradient(135deg,#29abe2,#3fa9f5)',
    nav:['dashboard','students','tutors','schedule','lessons','comms','crm','leads','tasks','invoice','reports','telephony'],
    can:{students:true,tutors:true,lessons:true,comms:true,payments:false,users:false,settings:false,danger:false,deleteAny:true},
    seeIncome:false, seeAll:true, canEditUsers:false, showGodBanner:false
  },
  network_admin: {
    label:'\u0410\u0434\u043C\u0456\u043D \u043C\u0435\u0440\u0435\u0436\u0456', icon:'\uD83C\uDF10', color:'var(--god2)',
    avatarBg:'linear-gradient(135deg,#5b60d4,#29abe2)',
    nav:['dashboard','students','tutors','schedule','lessons','comms','payments','crm','leads','tasks','invoice','reports','users','settings'],
    can:{students:true,tutors:true,lessons:true,payments:true,users:true,settings:true,danger:false,deleteAny:true},
    seeIncome:true, seeAll:true, canEditUsers:true, showGodBanner:false
  },
  tutor: {
    label:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440', icon:'\uD83D\uDCDA', color:'var(--tut)',
    avatarBg:'linear-gradient(135deg,#22b573,#7ac943)',
    nav:['dashboard','students','schedule','lessons','comms','tasks','profile'],
    can:{students:false,tutors:false,lessons:true,comms:true,payments:false,users:false,settings:false,danger:false,deleteAny:false},
    seeIncome:false, seeAll:false, canEditUsers:false, showGodBanner:false
  },
  };

var NAV_CFG = [
  {id:'dashboard',  ico:'\u229E',  lbl:'\u0414\u0430\u0448\u0431\u043E\u0440\u0434',     sec:'\u0413\u043E\u043B\u043E\u0432\u043D\u0435'},
  {id:'students',   ico:'\u25CE',  lbl:'\u0423\u0447\u043D\u0456',         sec:'\u0413\u043E\u043B\u043E\u0432\u043D\u0435', badge:true},
  {id:'tutors',     ico:'\u25C8',  lbl:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438',    sec:'\u0413\u043E\u043B\u043E\u0432\u043D\u0435'},
  {id:'schedule',   ico:'\u25A6',  lbl:'\u0420\u043E\u0437\u043A\u043B\u0430\u0434',      sec:'\u041D\u0430\u0432\u0447\u0430\u043D\u043D\u044F'},
  {id:'lessons',    ico:'\u25C9',  lbl:'\u0417\u0430\u043D\u044F\u0442\u0442\u044F',      sec:'\u041D\u0430\u0432\u0447\u0430\u043D\u043D\u044F'},
  {id:'comms',      ico:'\u25CE',  lbl:'\u041A\u043E\u043C\u0443\u043D\u0456\u043A\u0430\u0446\u0456\u0457', sec:'\u041D\u0430\u0432\u0447\u0430\u043D\u043D\u044F'},
  {id:'payments',   ico:'\u25C8',  lbl:'\u041E\u043F\u043B\u0430\u0442\u0430',       sec:'\u0424\u0456\u043D\u0430\u043D\u0441\u0438'},
  {id:'payroll',    ico:'\u20B4',  lbl:'\u0417\u0430\u0440\u043F\u043B\u0430\u0442\u0438',     sec:'\u0424\u0456\u043D\u0430\u043D\u0441\u0438'},
  {id:'invoice',    ico:'\u25C8',  lbl:'\u0420\u0430\u0445\u0443\u043D\u043E\u043A',  sec:'\u0424\u0456\u043D\u0430\u043D\u0441\u0438'},
  {id:'reports',    ico:'\u25E7',  lbl:'\u0410\u043D\u0430\u043B\u0456\u0442\u0438\u043A\u0430',    sec:'\u0424\u0456\u043D\u0430\u043D\u0441\u0438'},
  {id:'crm',        ico:'\u25A4',  lbl:'CRM',              sec:'\u041C\u0435\u043D\u0435\u0434\u0436\u043C\u0435\u043D\u0442'},
  {id:'contracts',  ico:'\uD83D\uDCC4',  lbl:'\u0414\u043E\u0433\u043E\u0432\u043E\u0440\u0438', sec:'\u041C\u0435\u043D\u0435\u0434\u0436\u043C\u0435\u043D\u0442'},
  {id:'leads',      ico:'\u260E',  lbl:'\u041B\u0456\u0434\u0438 \u0437 \u0434\u0437\u0432\u0456\u043D\u043A\u0456\u0432', sec:'\u041C\u0435\u043D\u0435\u0434\u0436\u043C\u0435\u043D\u0442'},
  {id:'tasks',      ico:'\u2611',  lbl:'\u0417\u0430\u0432\u0434\u0430\u043D\u043D\u044F',      sec:'\u041C\u0435\u043D\u0435\u0434\u0436\u043C\u0435\u043D\u0442'},
  {id:'audit',      ico:'\uD83D\uDD0D',  lbl:'\u0406\u0441\u0442\u043E\u0440\u0456\u044F \u0437\u043C\u0456\u043D',   sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'invoice-log',ico:'\uD83D\uDCCB',  lbl:'\u041B\u043E\u0433 \u0440\u0430\u0445\u0443\u043D\u043A\u0456\u0432', sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'acts',       ico:'\uD83D\uDCC4',  lbl:'\u0410\u043A\u0442\u0438 \u0440\u043E\u0431\u0456\u0442',    sec:'\u0424\u0456\u043D\u0430\u043D\u0441\u0438'},
  {id:'users',      ico:'\u25CE',  lbl:'\u0410\u043A\u0430\u0443\u043D\u0442\u0438',      sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'branches',   ico:'\uD83C\uDFE2',  lbl:'\u0424\u0456\u043B\u0456\u0457',         sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'telephony',  ico:'\u25C9',  lbl:'\u0422\u0435\u043B\u0435\u0444\u043E\u043D\u0456\u044F', sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'settings',   ico:'\u25C9',  lbl:'\u041D\u0430\u043B\u0430\u0448\u0442\u0443\u0432\u0430\u043D\u043D\u044F', sec:'\u0421\u0438\u0441\u0442\u0435\u043C\u0430'},
  {id:'profile',    ico:'\u25A3',  lbl:'\u041C\u0456\u0439 \u043F\u0440\u043E\u0444\u0456\u043B\u044C', sec:'\u041E\u0441\u043E\u0431\u0438\u0441\u0442\u0435'},
];

var DEFAULT_NAV_CFG = NAV_CFG;

var PLABELS={dashboard:'Дашборд',students:'Учні',tutors:'Репетитори',schedule:'Розклад',lessons:'Заняття',payments:'Оплата',reports:'Аналітика',users:'Акаунти',settings:'Налаштування',profile:'Мій профіль',crm:'CRM',analytics:'Статистика',comms:'Комунікації',missed:'Пропущені уроки',invoice:'Рахунок',branches:'Філії',telephony:'Телефонія',tasks:'Завдання',payroll:'Зарплати',audit:'Історія змін',acts:'Акти робіт'};

function localDateStr(d){
  if(typeof d === 'string') return d;
  var y=d.getFullYear(), m=String(d.getMonth()+1).padStart(2,'0'), dd=String(d.getDate()).padStart(2,'0');
  return y+'-'+m+'-'+dd;
}

/**
 * Заповнює список «Дата пропуску» реальними пропущеними заняттями
 * САМЕ цього учня й САМЕ цього репетитора.
 *
 * Раніше дата вводилась вручну, і легко було помилитись — вказати
 * день, коли пропуску не було, або пропуск іншого репетитора.
 * Тепер обирається зі списку, тож відпрацювання завжди прив'язується
 * до справжнього пропуску.
 */
function populateMissedSelect(){
  var sel=document.getElementById('l-miss-date');
  var hint=document.getElementById('l-miss-hint');
  if(!sel) return;

  var sid=(document.getElementById('l-std')||{value:''}).value;
  var tid=(document.getElementById('l-tutor')||{value:''}).value;
  var stat=(document.getElementById('l-stat')||{value:''}).value;
  var cur=sel.value;   // збережемо поточний вибір

  // Для статусу «Пропущене» дата пропуску = дата самого заняття.
  // Вводити її вручну немає сенсу, тому підставляємо автоматично
  // і робимо поле недоступним для редагування.
  if(stat==='missed'){
    var ld=(document.getElementById('l-date')||{value:''}).value||'';
    if(ld){
      var dp=ld.split('-');
      var lbl=dp.length===3 ? dp[2]+'.'+dp[1]+'.'+dp[0] : ld;
      sel.innerHTML='<option value="'+ld+'">'+lbl+'</option>';
      sel.value=ld;
      if(hint) hint.textContent='\u0414\u0430\u0442\u0430 \u043f\u0456\u0434\u0441\u0442\u0430\u0432\u043b\u0435\u043d\u0430 \u0430\u0432\u0442\u043e\u043c\u0430\u0442\u0438\u0447\u043d\u043e \u0437 \u0434\u0430\u0442\u0438 \u0437\u0430\u043d\u044f\u0442\u0442\u044f';
    } else {
      sel.innerHTML='<option value="">\u0421\u043f\u0435\u0440\u0448\u0443 \u0432\u043a\u0430\u0436\u0456\u0442\u044c \u0434\u0430\u0442\u0443 \u0437\u0430\u043d\u044f\u0442\u0442\u044f</option>';
      if(hint) hint.textContent='';
    }
    sel.disabled=true;
    return;
  }
  sel.disabled=false;

  if(!sid){
    sel.innerHTML='<option value="">\u0421\u043f\u0435\u0440\u0448\u0443 \u043e\u0431\u0435\u0440\u0456\u0442\u044c \u0443\u0447\u043d\u044f</option>';
    if(hint) hint.textContent='';
    return;
  }

  // Пропущені заняття цього учня, ще не покриті повністю.
  // Репетитор бачить СВОЇ пропуски. Пропуски В ІНШОГО репетитора показуються ЛИШЕ
  // якщо той репетитор ЗААРХІВОВАНИЙ (звільнився і його учнів передали цьому) —
  // щоб не було видно пропусків суміжних активних репетиторів.
  var miss=(S.lessons||[]).filter(function(l){
    if(l.status!=='missed') return false;
    if((l.studentId||l.student_id)!==sid) return false;
    // Виключаємо себе (якщо редагуємо існуюче заняття)
    if(S.editId && l.id===S.editId) return false;
    var lt=l.tutorId||l.tutor_id;
    if(tid && lt!==tid && !tutorArchived(lt)) return false; // чужий активний репетитор — приховуємо
    return true;
  }).sort(function(a,b){
    // спершу пропуски цього ж репетитора, потім за датою (новіші вгорі)
    if(tid){ var am=(a.tutorId||a.tutor_id)===tid?0:1, bm=(b.tutorId||b.tutor_id)===tid?0:1; if(am!==bm) return am-bm; }
    return String(b.date).localeCompare(String(a.date));
  });

  if(!miss.length){
    sel.innerHTML='<option value="">\u041d\u0435\u043c\u0430\u0454 \u043f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u0438\u0445 \u0437\u0430\u043d\u044f\u0442\u044c</option>';
    if(hint) hint.textContent='\u0423 \u0446\u044c\u043e\u0433\u043e \u0443\u0447\u043d\u044f \u043d\u0435\u043c\u0430\u0454 \u043d\u0435\u043f\u043e\u043a\u0440\u0438\u0442\u0438\u0445 \u043f\u0440\u043e\u043f\u0443\u0441\u043a\u0456\u0432';
    return;
  }

  sel.innerHTML='<option value="">\u2014 \u043e\u0431\u0435\u0440\u0456\u0442\u044c \u043f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u0435 \u0437\u0430\u043d\u044f\u0442\u0442\u044f \u2014</option>'
    + miss.map(function(l){
        var d=String(l.date||'').split('-');
        var ds=d.length===3 ? d[2]+'.'+d[1]+'.'+d[0].slice(2) : l.date;
        var hrs=Math.round((parseFloat(l.dur)||60)/60*10)/10;
        // Скільки ще лишилось відпрацювати
        var unc=0;
        try{ unc=uncoveredMissedHours(l); }catch(e){ unc=hrs; }
        var mark = unc<=0 ? ' \u2705 \u0432\u0456\u0434\u043f\u0440\u0430\u0446\u044c\u043e\u0432\u0430\u043d\u043e'
                 : (unc<hrs ? ' \u2014 \u0437\u0430\u043b\u0438\u0448\u0438\u043b\u043e\u0441\u044c '+unc+'\u0433' : '');
        // Якщо пропуск був в ІНШОГО репетитора — показуємо його ім'я
        var _ot=l.tutorId||l.tutor_id;
        var _otMark=(tid && _ot && _ot!==tid) ? ' \u00b7 \uD83D\uDC64 '+tn(_ot) : '';
        return '<option value="'+l.date+'">'+ds+' \u00b7 '+(l.time||'')
             +' \u00b7 '+hrs+'\u0433'+(l.subject?' \u00b7 '+l.subject:'')+_otMark+mark+'</option>';
      }).join('');

  // Повертаємо попередній вибір, якщо він досі є у списку
  if(cur){
    sel.value=cur;
    if(!sel.value && cur){
      // Дата збереглась, але заняття вже немає — додаємо як окремий пункт,
      // щоб не втратити прив'язку при редагуванні
      var d2=String(cur).split('-');
      var ds2=d2.length===3 ? d2[2]+'.'+d2[1]+'.'+d2[0].slice(2) : cur;
      sel.insertAdjacentHTML('beforeend','<option value="'+cur+'">'+ds2+' (\u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043d\u0430 \u0434\u0430\u0442\u0430)</option>');
      sel.value=cur;
    }
  }
  if(hint) hint.textContent='\u0417\u043d\u0430\u0439\u0434\u0435\u043d\u043e \u043f\u0440\u043e\u043f\u0443\u0441\u043a\u0456\u0432: '+miss.length;
}
window.populateMissedSelect=populateMissedSelect;

function onLessStatChange(){
  var stat=(document.getElementById('l-stat')||{value:''}).value;
  var dur=parseInt((document.getElementById('l-dur')||{value:'60'}).value)||60;
  var mkWrap=document.getElementById('l-makeup-wrap');
  var msWrap=document.getElementById('l-miss-wrap');
  var spWrap=document.getElementById('l-split-wrap');
  if(mkWrap) mkWrap.style.display=(stat==='makeup'||stat==='makeup_planned')?'block':'none';
  if(msWrap) msWrap.style.display=(stat==='missed'||stat==='makeup'||stat==='makeup_planned'||stat==='burned')?'block':'none';
  var canSplit=dur>=60 && (stat==='missed'||stat==='makeup'||stat==='makeup_planned');
  if(spWrap) spWrap.style.display=canSplit?'block':'none';
  // Блок об'єднання — показуємо завжди для будь-якого статусу
  var mergeWrap=document.getElementById('l-merge-wrap');
  if(mergeWrap) mergeWrap.style.display='block';
  populateMergeSelect();
  // Ініціалізуємо поле частин якщо порожнє
  var splitInput=document.getElementById('split-parts-input');
  if(splitInput && canSplit && !splitInput.value) splitInput.value='30, 60';
  if(canSplit) updateSplitPreview();
  try{ populateMissedSelect(); }catch(e){ console.error('populateMissedSelect:',e); }
  // Кнопки диктування в полях журналу
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
  try{ autoFillMakeupDates(); }catch(e){ console.error('autoFillMakeupDates:',e); }
}

/**
 * Автоматично підставляє дати, які система вже знає:
 *  • для ВІДПРАЦЮВАННЯ — дату найсвіжішого непокритого пропуску
 *  • для ПРОПУЩЕНОГО  — дату відпрацювання, якщо воно вже заплановане
 *  • для відпрацювання без дати — сьогоднішню (або дату самого заняття)
 * Заповнює лише ПОРОЖНІ поля, введене вручну не перетирає.
 */
function autoFillMakeupDates(){
  // Не чіпаємо, поки картка ще завантажується — інакше автопідстановка
  // перетирає збережені значення наявного заняття
  if(window._lessLoading) return;
  var stat=(document.getElementById('l-stat')||{value:''}).value;
  var sid=(document.getElementById('l-std')||{value:''}).value;
  var tid=(document.getElementById('l-tutor')||{value:''}).value;
  var lessonDate=(document.getElementById('l-date')||{value:''}).value;
  var missEl=document.getElementById('l-miss-date');
  var mkEl=document.getElementById('l-makeup-date');
  if(!sid) return;

  // ── Відпрацювання: підставляємо пропуск, який ще не покрито ──
  if((stat==='makeup'||stat==='makeup_planned') && missEl && !missEl.value){
    var _mkCands=function(mode){ // mode: 'own' — свої; 'archived' — чужі лише архівних
      return (S.lessons||[]).filter(function(x){
        if(x.status!=='missed') return false;
        if((x.studentId||x.student_id)!==sid) return false;
        var xt=x.tutorId||x.tutor_id;
        if(mode==='own'){ if(tid && xt!==tid) return false; }
        else { if(!(tid && xt!==tid && tutorArchived(xt))) return false; }
        if(S.editId && x.id===S.editId) return false;
        if(lessonDate && String(x.date||'') > lessonDate) return false;
        try{ return uncoveredMissedHours(x) > 0; }catch(e){ return true; }
      }).sort(function(a,b){ return String(b.date).localeCompare(String(a.date)); });
    };
    var cand=_mkCands('own'); if(!cand.length) cand=_mkCands('archived'); // фолбек: пропуск звільненого репетитора
    if(cand.length){
      missEl.value=cand[0].date;
      var hint=document.getElementById('l-miss-hint');
      if(hint) hint.textContent='\u2713 \u041f\u0456\u0434\u0441\u0442\u0430\u0432\u043b\u0435\u043d\u043e \u043d\u0430\u0439\u0441\u0432\u0456\u0436\u0456\u0448\u0438\u0439 \u043d\u0435\u043f\u043e\u043a\u0440\u0438\u0442\u0438\u0439 \u043f\u0440\u043e\u043f\u0443\u0441\u043a';
    }
    // Дата самого відпрацювання = дата заняття
    if(mkEl && !mkEl.value && lessonDate) mkEl.value=lessonDate;
  }

  // ── Пропущене: якщо відпрацювання вже є — показуємо його дату ──
  if(stat==='missed' && mkEl && !mkEl.value){
    var mk=(S.lessons||[]).filter(function(x){
      if(x.status!=='makeup' && x.status!=='makeup_planned') return false;
      if((x.studentId||x.student_id)!==sid) return false;
      if(tid && (x.tutorId||x.tutor_id)!==tid) return false;
      if(S.editId && x.missed_date===lessonDate) return true;
      return S.editId ? x.split_group_id===S.editId : false;
    }).sort(function(a,b){ return String(a.date).localeCompare(String(b.date)); });
    if(mk.length) mkEl.value=mk[0].date;
  }
}
window.autoFillMakeupDates=autoFillMakeupDates;

function renderCommsPage(){
  var tbody=document.getElementById('comms-tbody');
  if(!tbody)return;

  // Populate student filter (popSelSearch сам зберігає поточне значення)
  // Репетитора читаємо ДО списку учнів, щоб лишити лише його учнів
  var _fTutPre=(document.getElementById('comm-f-tutor')||{value:''}).value;
  var fStudSel=document.getElementById('comm-f-student');
  if(fStudSel){
    var _curS=fStudSel.value;
    var _sList=myStudents();
    if(_fTutPre){ _sList=_sList.filter(function(s){ return studentTutorIds(s).indexOf(_fTutPre)>=0; }); }
    _sList=_sList.sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');});
    popSelSearch('comm-f-student', [{id:'',fn:'\u0412\u0441\u0456 \u0443\u0447\u043d\u0456',ln:''}].concat(_sList), 'id', function(s){return s.fn+(s.ln?' '+s.ln:'');}, '');
    if(_curS && !_sList.some(function(s){return s.id===_curS;})){ fStudSel.value=''; if(fStudSel._updateSearch) fStudSel._updateSearch(); }
  }

  // Populate tutor filter
  var fTutSel=document.getElementById('comm-f-tutor');
  if(fTutSel && R()!=='tutor'){
    popSelSearch('comm-f-tutor', [{id:'',fn:'\u0412\u0441\u0456 \u0440\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0438',ln:''}].concat(S.tutors||[]), 'id', function(t){return t.fn+(t.ln?' '+t.ln:'');}, '');
  }

  // Читаємо значення ПІСЛЯ оновлення списків
  var fStud =(document.getElementById('comm-f-student')||{value:''}).value;
  var fTutor=(document.getElementById('comm-f-tutor')||{value:''}).value;
  var fType =(document.getElementById('comm-f-type')||{value:''}).value;

  var _selfId=null;
  if(R()==='tutor'){ var _mt=myTutor(); if(_mt) _selfId=_mt.id; }
  var comms=[].concat(S.comms||[]).sort(function(a,b){return (b.date||'').localeCompare(a.date||'');});
  if(_selfId) comms=comms.filter(function(c){return (c.tutorId||c.tutor_id)===_selfId;});
  if(fStud) comms=comms.filter(function(c){return (c.studentId||c.student_id)===fStud;});
  if(fTutor) comms=comms.filter(function(c){return (c.tutorId||c.tutor_id)===fTutor;});
  if(fType) comms=comms.filter(function(c){
    var t=c.type||'';
    if(fType==='message') return t==='message'||t==='msg';
    if(fType==='meeting') return t==='meeting'||t==='meet';
    return t===fType;
  });
  var ico={call:'📞',message:'💬',meeting:'🤝',email:'📧',other:'📋',msg:'💬',meet:'🤝'};
  if(!comms.length){
    tbody.innerHTML='<tr><td colspan="6" style="text-align:center;padding:20px;color:var(--t3)">Комунікацій немає</td></tr>';
    return;
  }
  tbody.innerHTML=comms.map(function(c){
    var tutor=(S.tutors||[]).find(function(t){return t.id===(c.tutorId||c.tutor_id);});
    var student=(S.students||[]).find(function(s){return s.id===(c.studentId||c.student_id);});
    var canDel=can('deleteAny')||(_selfId&&(c.tutorId||c.tutor_id)===_selfId);
    var actBtns=canDel
      ?('<button onclick="openCommM(null,\''+c.id+'\')" title="Редагувати" style="border:none;background:none;cursor:pointer;font-size:13px;padding:4px 6px;border-radius:6px">✏️</button>'
        +'<button onclick="delComm(\''+c.id+'\')" title="Видалити" style="border:none;background:none;cursor:pointer;font-size:14px;padding:4px 6px;border-radius:6px;color:var(--danger)">🗑</button>')
      :'';
    return '<tr><td style="font-size:11px;color:var(--t2)">'+fd(c.date)+'</td>'
      +'<td>'+(ico[c.type]||'📋')+' '+(c.type||'—')+'</td>'
      +'<td>'+(student?student.fn+' '+student.ln:'—')+'</td>'
      +'<td>'+(tutor?tutor.fn+' '+tutor.ln:'—')+'</td>'
      +'<td>'+(c.note||'—')+'</td>'
      +'<td style="text-align:right;white-space:nowrap">'+actBtns+'</td></tr>';
  }).join('');
}

function renderMissedLessons(){
  var tbody=document.getElementById('missed-tbody');
  if(!tbody)return;

  // Populate student filter
  var fStudSel=document.getElementById('missed-f-student');
  if(fStudSel){
    var curVal=fStudSel.value;
    popSelSearch('missed-f-student', [{id:'',fn:'\u0412\u0441\u0456 \u0443\u0447\u043D\u0456',ln:''}].concat(myStudents().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');})), 'id', function(s){return s.fn+(s.ln?' '+s.ln:'');}, '');
    if(curVal){ fStudSel.value=curVal; if(fStudSel._updateSearch) fStudSel._updateSearch(); }
  }
  var fStud=(fStudSel||{value:''}).value;
  var _myTc=R()==='tutor'?myTutor():null;

  // ALL missed lessons - full period, no date restriction
  var allMissed=(S.lessons||[]).filter(function(l){
    return l.status==='missed';
  });
  if(_myTc) allMissed=allMissed.filter(function(l){return (l.tutorId||l.tutor_id)===_myTc.id;});
  if(fStud) allMissed=allMissed.filter(function(l){return (l.studentId||l.student_id)===fStud;});
  allMissed=allMissed.sort(function(a,b){return (b.date||'').localeCompare(a.date||'');});

  if(!allMissed.length){
    tbody.innerHTML='<tr><td colspan="7" style="text-align:center;padding:20px;color:var(--t3)">Пропущених немає</td></tr>';
    return;
  }

  tbody.innerHTML=allMissed.map(function(l){
    var s=(S.students||[]).find(function(x){return x.id===(l.studentId||l.student_id);});
    var t=(S.tutors||[]).find(function(x){return x.id===(l.tutorId||l.tutor_id);});
    var covered=isCoveredMissed(l);

    // Find paired makeup lesson
    var makeupL=covered?(S.lessons||[]).find(function(x){
      return x.status==='makeup'
        && (x.studentId||x.student_id)===(l.studentId||l.student_id)
        && (x.tutorId||x.tutor_id)===(l.tutorId||l.tutor_id);
    }):null;

    var statusBadge=covered
      ?'<span style="color:#22c55e;font-weight:600">✅ Відпрацьовано</span>'
      :'<span style="color:#ef4444;font-weight:600">❌ Не відпрацьовано</span>';

    var makeupDateStr=l.makeup_date?fd(l.makeup_date)
      :(makeupL?fd(makeupL.date):'—');

    return '<tr style="'+(covered?'opacity:.6':'')+'"><td style="font-size:11px">'+fd(l.date)+'</td>'
      +'<td><b>'+(s?s.fn+' '+s.ln:'—')+'</b></td>'
      +'<td style="font-size:11px">'+(t?t.fn+' '+t.ln:'—')+'</td>'
      +'<td style="font-size:11px">'+(l.subject||'—')+'</td>'
      +'<td>'+statusBadge+'</td>'
      +'<td style="font-size:11px">'+fd(l.date)+'</td>'
      +'<td style="font-size:11px">'+makeupDateStr+'</td></tr>';
  }).join('');
}
async function deleteLessonFromModal(){
  if(!S.editId)return;
  var _dl=(S.lessons||[]).find(function(x){return x.id===S.editId;});
  if(_dl && isPlannedLesson(_dl) && !canMoveLessons()){ mkToast('\u0412\u0438\u0434\u0430\u043b\u044f\u0442\u0438 \u043f\u043b\u0430\u043d\u043e\u0432\u0456 \u0437\u0430\u043d\u044f\u0442\u0442\u044f \u043c\u043e\u0436\u0435 \u043b\u0438\u0448\u0435 \u0430\u0434\u043c\u0456\u043d\u0456\u0441\u0442\u0440\u0430\u0442\u043e\u0440 \u0430\u0431\u043e \u0434\u0438\u0440\u0435\u043a\u0442\u043e\u0440','error'); return; }
  if(lessonLocked(S.editId)){ mkToast('\u0417\u0430\u043d\u044f\u0442\u0442\u044f \u0437\u0430\u043c\u043e\u0440\u043e\u0436\u0435\u043d\u0435 \u2014 \u0440\u0435\u0434\u0430\u0433\u0443\u0454 \u0430\u0434\u043c\u0456\u043d\u0456\u0441\u0442\u0440\u0430\u0442\u043e\u0440 \u0430\u0431\u043e \u0434\u0438\u0440\u0435\u043a\u0442\u043e\u0440 \u2014 \u0432\u0438\u0434\u0430\u043b\u0435\u043d\u043d\u044f \u0437\u0430\u0431\u043e\u0440\u043e\u043d\u0435\u043d\u043e','error'); return; }
  if(!confirm('Видалити цей урок?'))return;
  var _id=S.editId;
  closeM('mo-lesson');
  try{ await dbDelete('lessons',_id); }catch(e){}
}

async function deleteLessonSeriesFromModal(){
  if(!S.editId)return;
  var l=(S.lessons||[]).find(function(x){return x.id===S.editId;});
  if(!l||!l.recurId){mkToast('Немає серії','error');return;}
  var series=S.lessons.filter(function(x){return x.recurId===l.recurId;});
  if(!confirm('Видалити всю серію? ('+series.length+' уроків)'))return;
  closeM('mo-lesson');
  try{
    // Через dbDelete (не напряму _sb) — щоб зберегти запис в Історії змін,
    // повтор при протермінованому токені й захист від закриття вкладки під час видалення
    for(var i=0;i<series.length;i++) await dbDelete('lessons',series[i].id);
    mkToast('Серію видалено'); if(S.currentPage==='schedule') renderSch();
  }catch(e){mkToast('Помилка: '+e.message,'error');}
}

function scheduleDailyRatingUpdate(){}

window.onLessStatChange = onLessStatChange;
window.renderCommsPage = renderCommsPage;
window.renderMissedLessons = renderMissedLessons;
window.deleteLessonFromModal = deleteLessonFromModal;
window.deleteLessonSeriesFromModal = deleteLessonSeriesFromModal;
