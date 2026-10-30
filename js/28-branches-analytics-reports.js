


function renderBranches(){
  var el=document.getElementById('branch-list');
  if(!el)return;
  var html='';
  (S.branches||[]).forEach(function(b){
    var bid=b.id;
    var isActive=S.currentBranchId===bid;
    var reqLines='';
    if(b.pay_recipient) reqLines+='<div style="font-size:11px;color:var(--t2)">\uD83D\uDCB3 '+b.pay_recipient+'</div>';
    if(b.pay_card)      reqLines+='<div style="font-size:11px;color:var(--t2)">'+b.pay_card+(b.pay_bank?' \u00B7 '+b.pay_bank:'')+'</div>';
    html+='<div class="ms" style="flex-direction:column;align-items:stretch;gap:6px;padding:12px 0;border-bottom:1px solid var(--b1)">'
      +'<div style="display:flex;align-items:center;gap:8px">'
        +'<div style="flex:1">'
          +'<div style="font-weight:700;font-size:13px">'+(isActive?'\u2705 ':'')+b.name+'</div>'
          +(b.address?'<div style="font-size:11px;color:var(--t2)">\uD83D\uDCCD '+b.address+'</div>':'')
          +(b.phone?'<div style="font-size:11px;color:var(--t2)">\uD83D\uDCDE '+b.phone+'</div>':'')
          +(b.email?'<div style="font-size:11px;color:var(--t2)">\u2709\uFE0F '+b.email+'</div>':'')
          +(reqLines?'<div style="margin-top:4px">'+reqLines+'</div>':'')
        +'</div>'
        +'<div style="display:flex;gap:6px;flex-shrink:0">'
          +'<button class="btn btn-g btn-sm" onclick="editBranch(this.dataset.id)" data-id="'+bid+'">\u270F\uFE0F</button>'
          +(S.branches.length>1?'<button class="btn btn-sm btn-d" onclick="delBranch(this.dataset.id)" data-id="'+bid+'">\uD83D\uDDD1</button>':'')
        +'</div>'
      +'</div>'
    +'</div>';
  });
  el.innerHTML=html||'<div style="font-size:12px;color:var(--t3);padding:8px 0">\u041D\u0435\u043C\u0430\u0454 \u0444\u0456\u043B\u0456\u0439</div>';
}



function renderPricingRules(){
  var el = document.getElementById('pricing-rules-list');
  if(!el) return;
  var rules = S.pricingRules || [];
  if(!rules.length){
    el.innerHTML = '<div style="font-size:12px;color:var(--t3);padding:8px 0">\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432\u0438\u043B. \u0414\u043E\u0434\u0430\u0439\u0442\u0435 \u043F\u0435\u0440\u0448\u0435 \u043F\u0440\u0430\u0432\u0438\u043B\u043E \u043D\u0438\u0436\u0447\u0435.</div>';
    return;
  }
  el.innerHTML = rules.map(function(r){
    var tags = [];
    if(r.subjectMatch) tags.push('\uD83D\uDCDA '+r.subjectMatch);
    if(r.tutorId){ var t=(S.tutors||[]).find(x=>x.id===r.tutorId); if(t) tags.push('\uD83D\uDC64 '+t.fn+' '+t.ln); }
    if(r.gradeMatch) tags.push('\uD83C\uDFEB '+r.gradeMatch+' \u043A\u043B.');
    if(r.durMin) tags.push('\u23F1 \u0432\u0456\u0434 '+r.durMin+' \u0445\u0432');
    return '<div class="ms" style="align-items:center">'+
      '<div style="flex:1">'+
        '<div style="font-weight:600;font-size:13px">'+r.name+' \u2014 <span style="color:var(--tut)">'+r.price+' \u20B4</span></div>'+
        '<div style="font-size:11px;color:var(--t2);margin-top:2px">'+(tags.length?tags.join(' \u00B7 '):'\u0417\u0430\u0441\u0442\u043E\u0441\u043E\u0432\u0443\u0454\u0442\u044C\u0441\u044F \u0434\u043E \u0432\u0441\u0456\u0445')+'</div>'+
      '</div>'+
      '<div style="display:flex;gap:6px">'+
        '<button class="btn btn-g btn-sm" onclick="editPriceRule(r.id)">\u270F\uFE0F</button>'+
        '<button class="btn btn-sm btn-d" onclick="delPriceRule(r.id)">\uD83D\uDDD1</button>'+
      '</div>'+
    '</div>';
  }).join('');
}



function renderProfile(){
  const mt = myTutor();
  // Показуємо поточний стан вимикача звуків
  try{
    var _sc=document.getElementById('pr-sounds');
    if(_sc) _sc.checked = soundsOn();
  }catch(e){}
  var _pi = document.getElementById('pr-info');
  if(!_pi) return;

  if(mt){
    // Photo: show real photo or initials avatar
    var photoHtml = mt.photo
      ? '<img src="'+mt.photo+'" style="width:56px;height:56px;object-fit:cover;border-radius:50%">'
      : '<div style="width:56px;height:56px;border-radius:50%;background:var(--adm2);display:flex;align-items:center;justify-content:center;font-size:22px;font-weight:700;color:#fff;font-family:Syne,sans-serif">'+(mt.fn||'?')[0]+(mt.ln||'')[0]+'</div>';

    var doneH = Math.round(myLessons().filter(function(l){
      return isDoneLesson(l)||l.status==='makeup';
    }).reduce(function(s,l){ return s+(parseFloat(l.dur)||60)/60; },0)*10)/10;

    _pi.innerHTML =
      '<div style="display:flex;align-items:center;gap:14px;margin-bottom:16px">'+photoHtml
      +'<div><div style="font-size:17px;font-weight:700;font-family:Syne,sans-serif">'+mt.fn+' '+mt.ln+'</div>'
      +'<div style="font-size:12px;color:var(--t2);margin-top:2px">'+(mt.subj||'—')+'</div></div></div>'
      +'<div class="ms"><span class="msl">Телефон</span><span class="msv" style="font-family:inherit">'+(mt.phone||'—')+'</span></div>'
      +'<div class="ms"><span class="msl">Email</span><span class="msv" style="font-family:inherit">'+(mt.email||'—')+'</span></div>'
      +'<div class="ms"><span class="msl">Занять проведено</span><span class="msv">'+doneH+'</span></div>'
      +(mt.bio?'<div style="margin-top:12px;padding:10px;background:var(--s2);border-radius:8px;font-size:12px;color:var(--t2)">'+mt.bio+'</div>':'');
  } else {
    _pi.innerHTML = '<div class="empty"><div class="ei">🔗</div>Ваш акаунт не прив\'язаний до профілю викладача</div>';
  }

  // Pre-fill edit form fields
  if(mt){
    var setV = function(id,v){ var el=document.getElementById(id); if(el) el.value=v||''; };
    setV('pr-fn', mt.fn); setV('pr-ln', mt.ln); setV('pr-phone', mt.phone);
    setV('pr-email', mt.email); setV('pr-subj2', mt.subj); setV('pr-bio', mt.bio);
    // Photo preview
    var prev = document.getElementById('pr-photo-preview');
    if(prev && mt.photo) prev.innerHTML = '<img src="'+mt.photo+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%">';
  }
}
/**
 * Деталізація по кліку на сегмент діаграми.
 * Показує конкретні заняття, з яких складається цифра —
 * щоб можна було перевірити, звідки вона взялась.
 */
function anDrill(kind, key){
  var ctx=window._anCtx||{};
  var from=ctx.fromStr, to=ctx.toStr, selTutor=ctx.selTutor;
  if(!from||!to) return;

  var pool=filterByBranch((S.lessons||[]).filter(function(l){
    return l.date>=from && l.date<=to;
  }));
  if(selTutor) pool=pool.filter(function(l){return (l.tutorId||l.tutor_id)===selTutor;});

  var title='', list=[];
  if(kind==='status'){
    if(key==='done'){ list=pool.filter(function(l){return isDoneLesson(l)||l.status==='makeup';}); title='\u041f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u0456 \u0437\u0430\u043d\u044f\u0442\u0442\u044f'; }
    else if(key==='missed'){ list=uncoveredMissedFilter(pool.filter(function(l){return l.status!=='cancelled';})); title='\u041f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u0456 \u0437\u0430\u043d\u044f\u0442\u0442\u044f'; }
    else if(key==='planned'){ list=pool.filter(function(l){return l.status==='planned'||l.status==='scheduled';}); title='\u0417\u0430\u043f\u043b\u0430\u043d\u043e\u0432\u0430\u043d\u0456 \u0437\u0430\u043d\u044f\u0442\u0442\u044f'; }
    else { list=pool.filter(function(l){return l.status==='cancelled';}); title='\u0421\u043a\u0430\u0441\u043e\u0432\u0430\u043d\u0456 \u0437\u0430\u043d\u044f\u0442\u0442\u044f'; }
  } else if(kind==='subject'){
    list=pool.filter(function(l){
      if(!(isDoneLesson(l)||l.status==='makeup')) return false;
      var k=String(l.subject||'\u0406\u043d\u0448\u0435').trim().toLowerCase().replace(/\s+/g,' ');
      return k===String(key).toLowerCase();
    });
    title='\u041f\u0440\u0435\u0434\u043c\u0435\u0442: '+(list[0]?(list[0].subject||key):key);
  } else if(kind==='tutor'){
    list=pool.filter(function(l){
      return (isDoneLesson(l)||l.status==='makeup') && (l.tutorId||l.tutor_id)===key;
    });
    var t=(S.tutors||[]).find(function(x){return x.id===key;});
    title='\u0420\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440: '+(t?t.fn+' '+t.ln:key);
  }

  list.sort(function(a,b){return String(b.date).localeCompare(String(a.date));});
  var hrs=Math.round(list.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;

  var rows=list.map(function(l){
    var st=(S.students||[]).find(function(x){return x.id===(l.studentId||l.student_id);});
    var tu=(S.tutors||[]).find(function(x){return x.id===(l.tutorId||l.tutor_id);});
    var d=String(l.date||'').split('-');
    return '<tr style="cursor:pointer" onclick="closeM(\'mo-an-drill\');openLessM(\''+l.id+'\')">'
      +'<td style="padding:6px 8px;font-family:JetBrains Mono,monospace;font-size:11.5px">'
        +(d.length===3?d[2]+'.'+d[1]:l.date)+(l.time?' '+l.time:'')+'</td>'
      +'<td style="padding:6px 8px">'+(st?st.fn+' '+st.ln:'\u2014')+'</td>'
      +'<td style="padding:6px 8px;font-size:11.5px;color:var(--t2)">'+(l.subject||'\u2014')+'</td>'
      +'<td style="padding:6px 8px;font-size:11.5px;color:var(--t2)">'+(tu?tu.fn+' '+tu.ln:'\u2014')+'</td>'
      +'<td style="padding:6px 8px;text-align:right;font-family:JetBrains Mono,monospace;font-size:11.5px">'
        +Math.round((parseFloat(l.dur)||60)/60*10)/10+'\u0433</td>'
      +'<td style="padding:6px 8px;text-align:center">'+bst(l.status)+'</td>'
    +'</tr>';
  }).join('');

  var el=document.getElementById('mo-an-drill');
  if(!el){
    el=document.createElement('div');
    el.className='mo'; el.id='mo-an-drill';
    document.body.appendChild(el);
  }
  el.innerHTML='<div class="mdl" style="max-width:860px">'
    +'<div class="mdlh"><div class="mdlt">'+title+'</div>'
      +'<button class="xbtn" onclick="closeM(\'mo-an-drill\')">\u00d7</button></div>'
    +'<div class="mdlb" style="max-height:70vh;overflow:auto">'
      +'<div style="font-size:12px;color:var(--t3);margin-bottom:10px">'
        +list.length+' \u0437\u0430\u043d\u044f\u0442\u044c \u00b7 '+hrs+'\u0433 \u00b7 '
        +'<span style="opacity:.8">\u043a\u043b\u0456\u043a \u043f\u043e \u0440\u044f\u0434\u043a\u0443 \u2014 \u0432\u0456\u0434\u043a\u0440\u0438\u0442\u0438 \u0437\u0430\u043d\u044f\u0442\u0442\u044f</span></div>'
      +(list.length
        ? '<table style="width:100%;border-collapse:collapse;font-size:12.5px">'
          +'<thead><tr style="border-bottom:1px solid var(--b1)">'
          +'<th style="text-align:left;padding:6px 8px;font-size:10px;color:var(--t3)">\u0414\u0410\u0422\u0410</th>'
          +'<th style="text-align:left;padding:6px 8px;font-size:10px;color:var(--t3)">\u0423\u0427\u0415\u041d\u042c</th>'
          +'<th style="text-align:left;padding:6px 8px;font-size:10px;color:var(--t3)">\u041f\u0420\u0415\u0414\u041c\u0415\u0422</th>'
          +'<th style="text-align:left;padding:6px 8px;font-size:10px;color:var(--t3)">\u0420\u0415\u041f\u0415\u0422\u0418\u0422\u041e\u0420</th>'
          +'<th style="text-align:right;padding:6px 8px;font-size:10px;color:var(--t3)">\u0413\u041e\u0414</th>'
          +'<th style="text-align:center;padding:6px 8px;font-size:10px;color:var(--t3)">\u0421\u0422\u0410\u0422\u0423\u0421</th>'
          +'</tr></thead><tbody>'+rows+'</tbody></table>'
        : '<div class="empty"><div class="ei">\ud83d\udced</div>\u041d\u0435\u043c\u0430\u0454 \u0437\u0430\u043d\u044f\u0442\u044c</div>')
    +'</div></div>';
  openM('mo-an-drill');
}
window.anDrill=anDrill;

function renderAllAnalytics(){
  renderAnalytics();
  renderReports();
}

// ═══════════════════════════════════════════════════════════════
//  ТАЙМЛАЙН АНАЛІТИКИ — навігація в часі
// ═══════════════════════════════════════════════════════════════
// Зсув відносно поточного періоду: 0 = зараз, -1 = попередній тощо
var rcOffset = 0;

/** Стрілки «назад / вперед / зараз» */
function rcShiftPeriod(dir){
  if(dir===0) rcOffset=0;
  else rcOffset += dir;
  if(rcOffset>0) rcOffset=0;          // у майбутнє далі поточного не йдемо
  if(rcOffset<-36) rcOffset=-36;      // і не глибше трьох років
  renderAllAnalytics();
}
window.rcShiftPeriod=rcShiftPeriod;

/** Перехід на конкретний місяць кліком по стовпчику */
function rcGoToMonth(off){
  rcOffset = off;
  var sel=document.getElementById('rc-range');
  if(sel) sel.value='month';          // клік по місяцю = режим місяця
  renderAllAnalytics();
}
window.rcGoToMonth=rcGoToMonth;

/**
 * Малює смужки останніх 12 місяців: висота — кількість проведених годин.
 * Дає одразу побачити сезонність і провали, а не гортати наосліп.
 */
function renderTimeline(){
  var wrap=document.getElementById('rc-tl-bars');
  var lblEl=document.getElementById('rc-tl-label');
  if(!wrap) return;

  var months=['Січ','Лют','Бер','Кві','Тра','Чер','Лип','Сер','Вер','Жов','Лис','Гру'];
  var now=new Date();
  var base=new Date(now.getFullYear(), now.getMonth()+rcOffset, 1);

  // Підпис поточного вибору
  if(lblEl){
    var sel=(document.getElementById('rc-range')||{value:'month'}).value;
    var nm=months[base.getMonth()]+' '+base.getFullYear();
    lblEl.textContent = rcOffset===0
      ? (sel==='month' ? nm+' (поточний)' : 'Поточний період')
      : nm;
  }
  var nextBtn=document.getElementById('rc-tl-next');
  if(nextBtn) nextBtn.disabled = rcOffset>=0;

  // Дані по 12 місяцях, що закінчуються обраним
  var bars=[], maxH=1;
  for(var i=11;i>=0;i--){
    var d=new Date(base.getFullYear(), base.getMonth()-i, 1);
    var key=d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0');
    var h=0;
    myLessons().forEach(function(l){
      if(String(l.date||'').slice(0,7)!==key) return;
      if(!(isDoneLesson(l)||l.status==='makeup')) return;
      h+=(parseFloat(l.dur)||60)/60;
    });
    h=Math.round(h*10)/10;
    if(h>maxH) maxH=h;
    bars.push({ off: rcOffset-i, key:key, h:h,
      lbl:months[d.getMonth()], year:d.getFullYear(),
      isFuture: d > new Date(now.getFullYear(), now.getMonth(), 1) });
  }

  wrap.innerHTML=bars.map(function(b){
    var cls='rc-tl-bar'+(b.off===rcOffset?' cur':'')+(b.isFuture?' fut':'');
    var hPct=Math.max(3, Math.round(b.h/maxH*100));
    return '<div class="'+cls+'" onclick="rcGoToMonth('+b.off+')" title="'+b.lbl+' '+b.year+'">'
      +'<span class="rc-tl-tip">'+b.h+'г</span>'
      +'<div class="fill" style="height:'+hPct+'%"></div>'
      +'<div class="lb">'+b.lbl+'</div>'
    +'</div>';
  }).join('');
}
window.renderTimeline=renderTimeline;

function renderReports(){
  // === Діапазон дат ===
  var range = (document.getElementById('rc-range')||{value:'month'}).value;
  // «now» зсувається таймлайном: rcOffset=-1 означає попередній місяць.
  // Для місячних режимів беремо кінець зсунутого місяця, для решти — той самий день.
  var realNow = new Date();
  var now;
  if(rcOffset===0){
    now = realNow;
  } else if(range==='month'||range==='3month'||range==='6month'){
    now = new Date(realNow.getFullYear(), realNow.getMonth()+rcOffset+1, 0);
  } else {
    now = new Date(realNow.getFullYear(), realNow.getMonth()+rcOffset, realNow.getDate());
  }
  var fromDate = new Date(now);

  if(range === 'week'){
    var day = now.getDay()||7;
    fromDate = new Date(now); fromDate.setDate(now.getDate() - day + 1);
    fromDate.setHours(0,0,0,0);
  } else if(range === '2week'){
    fromDate.setDate(now.getDate() - 14);
  } else if(range === '4week'){
    fromDate.setDate(now.getDate() - 28);
  } else if(range === 'month'){
    fromDate = new Date(now.getFullYear(), now.getMonth(), 1);
  } else if(range === '3month'){
    fromDate = new Date(now.getFullYear(), now.getMonth() - 3, 1);
  } else if(range === '6month'){
    fromDate = new Date(now.getFullYear(), now.getMonth() - 6, 1);
  } else if(range === 'year'){
    fromDate = new Date(now.getFullYear(), 0, 1);
  } else {
    fromDate = new Date(0);
  }
  var fromStr = localDateStr(fromDate);
  // Кінець періоду — не «сьогодні», а кінець обраного проміжку.
  // Інакше заплановані заняття в майбутньому не потрапляли в розрахунок,
  // і показник «Заплановано» був заниженим.
  var toDate;
  if(range==='month')       toDate = new Date(now.getFullYear(), now.getMonth()+1, 0);
  else if(range==='year')   toDate = new Date(now.getFullYear(), 11, 31);
  else if(range==='3month'||range==='6month') toDate = new Date(now.getFullYear(), now.getMonth()+1, 0);
  else if(range==='all')    toDate = new Date(now.getFullYear()+5, 11, 31);
  else                      toDate = now;
  var toStr   = localDateStr(toDate);

  // Підпис діапазону
  var lbl = document.getElementById('rc-range-lbl');
  if(lbl && range !== 'all'){
    lbl.textContent = fromDate.toLocaleDateString('uk-UA',{day:'numeric',month:'short'})
      + ' — ' + now.toLocaleDateString('uk-UA',{day:'numeric',month:'short',year:'numeric'});
  } else if(lbl){ lbl.textContent = ''; }

  // Ховаємо доходи для адміна
  var incCard = document.getElementById('rc-income-card');
  if(incCard) incCard.style.display = (R()==='admin') ? 'none' : '';

  try{ renderTimeline(); }catch(e){ console.error('renderTimeline:',e); }

  // === Фільтрація занять ===
  var lessons = (S.lessons||[]).filter(function(l){
    return l.date >= fromStr && l.date <= toStr && l.status !== 'cancelled';
  });

  function hrs(arr){ return Math.round(arr.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10; }

  // === Доходи по місяцях (₴) — всі проведені заняття року, незалежно від фільтра ===
  var months = ['Січ','Лют','Бер','Кві','Тра','Чер','Лип','Сер','Вер','Жов','Лис','Гру'];
  var md = new Array(12).fill(0);
  var curYear = new Date().getFullYear();
  // Тестування теж оплачується (див. isDoneLesson), тому має входити в дохід
  var validInc = ['done','completed','testing','makeup'];
  myLessons().filter(function(l){
    if(!l.date) return false;
    if(validInc.indexOf(l.status)<0) return false;
    return new Date(l.date).getFullYear() === curYear;
  }).forEach(function(l){
    var mon = new Date(l.date).getMonth();
    var cost = 0;
    if(l.price!=null && l.price!=='' && !isNaN(parseFloat(l.price))){
      cost = lessonTotal(l);
    } else {
      // фолбек: поле amount з платежу відповідного заняття
      var pay = (S.payments||[]).find(function(p){return p.lessonId===l.id||p.lesson_id===l.id;});
      if(pay) cost = parseFloat(pay.amount)||0;
    }
    md[mon] += cost;
  });
  md = md.map(function(v){return Math.round(v);});
  var maxI = Math.max.apply(null, md.concat([1]));
  var incEl = document.getElementById('rc-income');
  if(incEl) incEl.innerHTML = md.map(function(v,i){
    var lbl = v>0 ? (v>=1000 ? (Math.round(v/100)/10)+'к' : v) : '';
    return '<div class="bw"><div class="bar" style="height:'+(v/maxI*100)+'%;background:linear-gradient(180deg,var(--adm),var(--adm2))">'
      +(lbl?'<div style="position:absolute;top:-16px;left:50%;transform:translateX(-50%);font-size:8px;color:var(--t2);white-space:nowrap;font-family:JetBrains Mono,monospace">'+lbl+'₴</div>':'')
      +'</div><div class="blbl">'+months[i]+'</div></div>';
  }).join('');
  // === Заняття по предметах (год) ===
  // Об'єднуємо предмети, що відрізняються лише регістром чи пробілами:
  // «Англійська мова» і «англійська мова» — це один предмет, а не два.
  var sc = {}, scLabel = {};
  lessons.forEach(function(l){
    if(!l.subject) return;
    var key = String(l.subject).trim().toLowerCase().replace(/\s+/g,' ');
    if(!key) return;
    // Для показу лишаємо найпоширеніший варіант написання
    if(!scLabel[key]) scLabel[key] = String(l.subject).trim();
    sc[key] = (sc[key]||0) + (parseFloat(l.dur)||60)/60;
  });
  var totalSubjH = hrs(lessons) || 1;
  var cols = ['var(--adm)','var(--tut)','var(--dir)','var(--god)','#a78bfa','#0ea5e9','#f59e0b','#ec4899'];
  var subjEl = document.getElementById('rc-subj');
  if(subjEl) subjEl.innerHTML = Object.entries(sc).sort(function(a,b){return b[1]-a[1];})
    .map(function(e,i){
      var s=scLabel[e[0]]||e[0], h=Math.round(e[1]*10)/10, pct=Math.round(e[1]/totalSubjH*100);
      return '<div style="margin-bottom:10px">'
        +'<div style="display:flex;justify-content:space-between;margin-bottom:3px">'
        +'<span style="font-size:12px">'+s+'</span>'
        +'<span style="font-size:11px;color:var(--t2);font-family:JetBrains Mono,monospace">'+h+'\u0433 ('+pct+'%)</span></div>'
        +'<div class="pb"><div class="pf" style="width:'+(e[1]/totalSubjH*100)+'%;background:'+cols[i%cols.length]+'"></div></div></div>';
    }).join('') || '<div class="empty"><div class="ei">\uD83D\uDCDA</div>\u041d\u0435\u043c\u0430\u0454 \u0434\u0430\u043d\u0438\u0445</div>';

  // === Завантаженість репетиторів (год) ===
  var tl = {};
  lessons.forEach(function(l){
    var tid = l.tutorId||l.tutor_id;
    if(tid) tl[tid] = (tl[tid]||0) + (parseFloat(l.dur)||60)/60;
  });
  // Включаємо ВСІХ репетиторів, навіть з 0 годин у періоді
  var entries = (S.tutors||[]).map(function(t){
    return {id:t.id, h:Math.round((tl[t.id]||0)*10)/10};
  }).sort(function(a,b){return b.h-a.h;});
  var maxT = entries.length ? Math.max.apply(null, entries.map(function(e){return e.h;}).concat([1])) : 1;
  var tloadEl = document.getElementById('rc-tload');
  if(tloadEl) tloadEl.innerHTML = entries.map(function(e){
    return '<div style="margin-bottom:10px">'
      +'<div style="display:flex;justify-content:space-between;margin-bottom:3px">'
      +'<span style="font-size:12px">'+tn(e.id)+'</span>'
      +'<span style="font-size:11px;color:var(--t2);font-family:JetBrains Mono,monospace">'+e.h+'\u0433</span></div>'
      +'<div class="pb"><div class="pf" style="width:'+(e.h/maxT*100)+'%;background:var(--adm)"></div></div></div>';
  }).join('') || '<div class="empty"><div class="ei">\uD83E\uDDD1\u200D\uD83C\uDFEB</div>\u041d\u0435\u043c\u0430\u0454 \u0434\u0430\u043d\u0438\u0445</div>';

  // === Загальна статистика ===
  var doneL  = lessons.filter(function(l){return isDoneLesson(l)||l.status==='makeup';});
  var missedL= uncoveredMissedFilter(lessons);
  var doneH  = hrs(doneL);
  var missedH= hrs(missedL);
  var totalH = hrs(lessons);
  var pct    = totalH>0 ? Math.round(doneH/totalH*100) : 0;
  var totalInc = (S.payments||[]).filter(function(p){
    return p.status==='paid' && p.date>=fromStr && p.date<=toStr;
  }).reduce(function(a,p){return a+p.amount;},0);
  var showIncome = P().seeIncome && R()!=='tutor' && R()!=='admin';

  var genEl = document.getElementById('rc-gen');
  if(genEl) genEl.innerHTML =
    '<div class="ms"><span class="msl">\u0412\u0441\u044c\u043e\u0433\u043e \u0443\u0447\u043d\u0456\u0432</span><span class="msv">'+(myStudents().length)+'</span></div>'
    +'<div class="ms"><span class="msl">\u0410\u043a\u0442\u0438\u0432\u043d\u0438\u0445 \u0443\u0447\u043d\u0456\u0432</span><span class="msv">'+(myStudents().filter(function(s){return s.status==='active'||s.status==='trial';}).length)+'</span></div>'
    +'<div class="ms"><span class="msl">\u041f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u043e (\u0433\u043e\u0434)</span><span class="msv" style="color:var(--tut)">'+doneH+'\u0433</span></div>'
    +'<div class="ms"><span class="msl">\u041f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u043e (\u0433\u043e\u0434)</span><span class="msv" style="color:var(--danger)">'+missedH+'\u0433</span></div>'
    +'<div class="ms"><span class="msl">\u0412\u0438\u043a\u043e\u043d\u0430\u043d\u043d\u044f \u043f\u043b\u0430\u043d\u0443</span><span class="msv" style="color:'+(pct>=80?'var(--tut)':pct>=50?'var(--dir)':'var(--danger)')+'">'+pct+'%</span></div>'
    +(showIncome
      ?'<div class="ms"><span class="msl">\u0414\u043e\u0445\u0456\u0434 \u0437\u0430 \u043f\u0435\u0440\u0456\u043e\u0434</span><span class="msv" style="color:var(--tut)">'+(totalInc.toLocaleString('uk-UA'))+'\u20b4</span></div>'
      :'')
    +'<div class="ms"><span class="msl">\u0412\u0438\u043a\u043b\u0430\u0434\u0430\u0447\u0456\u0432</span><span class="msv">'+(S.tutors.length)+'</span></div>';
}
