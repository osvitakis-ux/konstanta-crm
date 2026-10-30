

function renderSch(){
  try{ renderSchLegend(); }catch(e){}
  const view = S.schView || 'week';
  // Update UI
  const btnW = document.getElementById('sch-btn-week');
  const btnD = document.getElementById('sch-btn-day');
  const btnM = document.getElementById('sch-btn-month');
  const tf   = document.getElementById('sch-tutor-filter');
  if(btnW) btnW.classList.toggle('active-view', view==='week');
  if(btnD) btnD.classList.toggle('active-view', view==='day');
  if(btnM) btnM.classList.toggle('active-view', view==='month');
  // Фільтр по репетиторах доступний адмінам/директорам/богу в обох режимах (тиждень і день)
  if(tf){
    tf.style.display = R()!=='tutor' ? 'block' : 'none';
    const prev = tf.value;
    tf.innerHTML = '<option value="">\u0412\u0441\u0456 \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438</option>' +
      activeTutors().slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');}).map(t=>('<option value="'+(t.id)+'">'+(t.fn)+' '+(t.ln)+'</option>')).join('');
    tf.value = prev;
  }
  // Update prev/next labels
  const prevBtn = document.getElementById('sch-prev');
  const nextBtn = document.getElementById('sch-next');
  if(prevBtn) prevBtn.textContent = view==='day' ? '\u2190 \u0412\u0447\u043E\u0440\u0430' : view==='month' ? '\u2190 \u041F\u043E\u043F\u0435\u0440\u0435\u0434\u043D\u0456\u0439 \u043C\u0456\u0441\u044F\u0446\u044C' : '\u2190 \u041F\u043E\u043F\u0435\u0440\u0435\u0434\u043D\u0456\u0439';
  if(nextBtn) nextBtn.textContent = view==='day' ? '\u0417\u0430\u0432\u0442\u0440\u0430 \u2192' : view==='month' ? '\u041D\u0430\u0441\u0442\u0443\u043F\u043D\u0438\u0439 \u043C\u0456\u0441\u044F\u0446\u044C \u2192' : '\u041D\u0430\u0441\u0442\u0443\u043F\u043D\u0438\u0439 \u2192';

  var schgEl=document.getElementById('schg'), schmEl=document.getElementById('schm');
  if(view === 'month'){
    if(schgEl) schgEl.style.display='none';
    if(schmEl) schmEl.style.display='block';
    renderSchMonth();
  } else {
    if(schmEl) schmEl.style.display='none';
    if(schgEl) schgEl.style.display='grid';
    if(view === 'week') renderSchWeek();
    else                renderSchDay();
  }

  var _at=document.getElementById('avail-toggle-btn');
  var _att=(typeof availTargetTutor==='function')?availTargetTutor():null;
  var _fill=document.getElementById('avail-fill-btn'), _clr=document.getElementById('avail-clear-btn'), _hint=document.getElementById('avail-hint');
  if(_at){
    if(view!=='week' && S._availMode){ S._availMode=false; }
    _at.style.display = view==='week' ? 'inline-flex' : 'none';
    _at.classList.toggle('btn-p', !!S._availMode);
    _at.textContent = S._availMode ? '\u2713 \u0413\u043E\u0442\u043E\u0432\u043E' : '\uD83D\uDD52 \u0420\u043E\u0431\u043E\u0447\u0456 \u0433\u043E\u0434\u0438\u043D\u0438';
    var _editing = S._availMode && _att;
    if(_fill) _fill.style.display = _editing ? 'inline-flex':'none';
    if(_clr)  _clr.style.display  = _editing ? 'inline-flex':'none';
    if(_hint){
      _hint.style.display = S._availMode ? 'inline':'none';
      _hint.textContent = _att ? ('\u041A\u043B\u0456\u043A\u0430\u0439\u0442\u0435 \u043F\u043E \u043A\u043B\u0456\u0442\u0438\u043D\u043A\u0430\u0445: '+_att.fn+' '+_att.ln) : '\u041E\u0431\u0435\u0440\u0456\u0442\u044C \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0430 \u0443 \u0444\u0456\u043B\u044C\u0442\u0440\u0456';
    }
  }
}



function renderSchMonth(){
  const now=new Date();
  const offset=S.monthOffset||0;
  const first=new Date(now.getFullYear(), now.getMonth()+offset, 1);
  const monthNames=['\u0421\u0456\u0447\u0435\u043D\u044C','\u041B\u044E\u0442\u0438\u0439','\u0411\u0435\u0440\u0435\u0437\u0435\u043D\u044C','\u041A\u0432\u0456\u0442\u0435\u043D\u044C','\u0422\u0440\u0430\u0432\u0435\u043D\u044C','\u0427\u0435\u0440\u0432\u0435\u043D\u044C','\u041B\u0438\u043F\u0435\u043D\u044C','\u0421\u0435\u0440\u043F\u0435\u043D\u044C','\u0412\u0435\u0440\u0435\u0441\u0435\u043D\u044C','\u0416\u043E\u0432\u0442\u0435\u043D\u044C','\u041B\u0438\u0441\u0442\u043E\u043F\u0430\u0434','\u0413\u0440\u0443\u0434\u0435\u043D\u044C'];
  const lblEl=document.getElementById('wklbl');
  if(lblEl) lblEl.textContent=monthNames[first.getMonth()]+' '+first.getFullYear();

  // Сітка з понеділка: скільки днів попереднього місяця показати перед 1-м числом
  const firstDow=(first.getDay()===0?7:first.getDay()); // 1=Пн..7=Нд
  const gridStart=new Date(first);
  gridStart.setDate(first.getDate()-(firstDow-1));
  const daysInGrid=42; // 6 тижнів — стабільна висота сітки для будь-якого місяця

  const _schStat=(document.getElementById('sch-status-filter')||{value:''}).value;
  const _schTut=(document.getElementById('sch-tutor-filter')||{value:''}).value;
  const ml=myLessons().filter(function(l){
    if(_schTut && (l.tutorId||l.tutor_id)!==_schTut) return false;
    if(_schStat){
      if(_schStat==='planned') return l.status==='planned'||l.status==='scheduled'||!l.status;
      if(_schStat==='completed') return isDoneLesson(l)||l.status==='makeup';
      return l.status===_schStat;
    }
    return l.status!=='cancelled';
  });
  const byDate={};
  ml.forEach(function(l){ (byDate[l.date]=byDate[l.date]||[]).push(l); });

  const dnames=['\u041F\u043D','\u0412\u0442','\u0421\u0440','\u0427\u0442','\u041F\u0442','\u0421\u0431','\u041D\u0434'];
  let html='<div class="schm-grid">';
  dnames.forEach(function(dn){ html+='<div class="schm-head">'+dn+'</div>'; });

  for(let i=0;i<daysInGrid;i++){
    const d=new Date(gridStart); d.setDate(gridStart.getDate()+i);
    const ds=localDateStr(d);
    const isOther=d.getMonth()!==first.getMonth();
    const isToday=d.toDateString()===now.toDateString();
    const dayLessons=(byDate[ds]||[]).slice().sort(function(a,b){return (a.time||'').localeCompare(b.time||'');});
    let evHtml='';
    dayLessons.forEach(function(l){
      const isCov=l.status==='missed'&&isCoveredMissed(l);
      const isPart=!isCov&&l.status==='missed'&&uncoveredMissedHours(l)*60<(parseFloat(l.dur)||60);
      const ecl=isCov?'ec-covered':isPart?'ec-partial':l.status==='missed'?'ec-miss':l.status==='makeup'?'ec-make':l.status==='makeup_planned'?'ec-makeplan':l.status==='burned'?'ec-burned':l.status==='testing'?'ec-testing':isDoneLesson(l)?'ec-done':'ec-plan';
      // Товщина смужки ∝ тривалості: 1 год = 20px, 2 год = 40px (вдвічі товща)
      var _hrs=(parseFloat(l.dur)||60)/60;
      var _h=Math.round(_hrs*20);
      evHtml+='<div class="schm-ev '+ecl+'" style="min-height:'+_h+'px;box-shadow:inset 3px 0 0 '+tutorColor(l.tutorId||l.tutor_id)+'" onclick="event.stopPropagation();showQuickPopup(\''+l.id+'\',event.clientX,event.clientY)" title="'+(l.time||'')+' '+snShort(l.studentId||l.student_id)+' '+(l.subject||'')+' \u00B7 '+_hrs+' \u0433\u043E\u0434">'
        +'<b>'+(l.time||'')+'</b> '+(l.status==='burned'?'\ud83d\udd25 ':'')+snShort(l.studentId||l.student_id)+'</div>';
    });
    html+='<div class="schm-cell'+(isOther?' other-month':'')+(isToday?' is-today':'')+'" onclick="schGoToDay(\''+ds+'\')">'
      +'<div class="schm-daynum">'+d.getDate()+'</div>'
      +evHtml
      +'</div>';
  }
  html+='</div>';
  var el=document.getElementById('schm');
  if(el) el.innerHTML=html;
}

// Клік по клітинці місяця — переходимо в денний вигляд саме на цю дату
function schGoToDay(ds){
  var now=new Date(); now.setHours(0,0,0,0);
  var target=new Date(ds+'T00:00:00');
  var diffDays=Math.round((target-now)/86400000);
  S.dayOffset=diffDays;
  S.schView='day';
  renderSch();
}
window.schGoToDay=schGoToDay;
window.renderSchMonth=renderSchMonth;

// ── Попап «повний розклад на день» у місячному вигляді ──
var _dayPopHandler=null;
function closeDayPopover(){
  var p=document.getElementById('day-popover');
  if(p) p.remove();
  if(_dayPopHandler){ document.removeEventListener('click', _dayPopHandler); _dayPopHandler=null; }
}
function showDayPopover(ds, x, y){
  closeDayPopover();
  if(typeof closeQuickPopup==='function') closeQuickPopup();
  // ті самі фільтри, що й у місячній сітці
  var _schStat=(document.getElementById('sch-status-filter')||{value:''}).value;
  var _schTut=(document.getElementById('sch-tutor-filter')||{value:''}).value;
  var lessons=myLessons().filter(function(l){
    if(l.date!==ds) return false;
    if(_schTut && (l.tutorId||l.tutor_id)!==_schTut) return false;
    if(_schStat){
      if(_schStat==='planned') return l.status==='planned'||l.status==='scheduled'||!l.status;
      if(_schStat==='completed') return isDoneLesson(l)||l.status==='makeup';
      return l.status===_schStat;
    }
    return l.status!=='cancelled';
  }).slice().sort(function(a,b){return (a.time||'').localeCompare(b.time||'');});
  var dt=new Date(ds+'T00:00:00');
  var dn=['\u041D\u0434','\u041F\u043D','\u0412\u0442','\u0421\u0440','\u0427\u0442','\u041F\u0442','\u0421\u0431'][dt.getDay()];
  var mn=['\u0441\u0456\u0447\u043D\u044F','\u043B\u044E\u0442\u043E\u0433\u043E','\u0431\u0435\u0440\u0435\u0437\u043D\u044F','\u043A\u0432\u0456\u0442\u043D\u044F','\u0442\u0440\u0430\u0432\u043D\u044F','\u0447\u0435\u0440\u0432\u043D\u044F','\u043B\u0438\u043F\u043D\u044F','\u0441\u0435\u0440\u043F\u043D\u044F','\u0432\u0435\u0440\u0435\u0441\u043D\u044F','\u0436\u043E\u0432\u0442\u043D\u044F','\u043B\u0438\u0441\u0442\u043E\u043F\u0430\u0434\u0430','\u0433\u0440\u0443\u0434\u043D\u044F'][dt.getMonth()];
  var rows=lessons.map(function(l){
    var color=tutorColor(l.tutorId||l.tutor_id);
    var st = l.status==='burned'?'\uD83D\uDD25':l.status==='missed'?'\u274C':l.status==='makeup'?'\u21A9\uFE0F':l.status==='makeup_planned'?'\uD83D\uDD52':l.status==='testing'?'\uD83E\uDDEA':isDoneLesson(l)?'\u2705':'';
    return '<div class="dp-row" style="border-left:3px solid '+color+'" onclick="event.stopPropagation();_dayRowClick(\''+l.id+'\',event)">'
      +'<span class="dp-time">'+(l.time||'')+'</span>'
      +'<span class="dp-name">'+snShort(l.studentId||l.student_id)+'</span>'
      +'<span class="dp-subj">'+(l.subject||'')+'</span>'
      +'<span class="dp-st">'+st+'</span>'
    +'</div>';
  }).join('');
  var pop=document.createElement('div');
  pop.id='day-popover'; pop.className='day-popover';
  pop.innerHTML='<div class="dp-head"><span>'+dn+', '+dt.getDate()+' '+mn+'</span><span class="dp-count">'+lessons.length+'</span></div>'
    +'<div class="dp-list">'+(rows||'<div class="dp-empty">\u041D\u0435\u043C\u0430\u0454 \u0437\u0430\u043D\u044F\u0442\u044C</div>')+'</div>'
    +'<div class="dp-foot"><button class="btn btn-g btn-sm" style="width:100%" onclick="event.stopPropagation();closeDayPopover();schGoToDay(\''+ds+'\')">\u0412\u0456\u0434\u043A\u0440\u0438\u0442\u0438 \u0434\u0435\u043D\u044C \u2192</button></div>';
  document.body.appendChild(pop);
  var margin=10;
  pop.style.left=margin+'px'; pop.style.top=margin+'px'; // тимчасово, щоб виміряти
  var rect=pop.getBoundingClientRect();
  var pw=rect.width||280, ph=rect.height||320;
  var lx=Math.min(Math.max(x, margin), Math.max(margin, window.innerWidth  - pw - margin));
  var ly=Math.min(Math.max(y, margin), Math.max(margin, window.innerHeight - ph - margin));
  pop.style.left=lx+'px'; pop.style.top=ly+'px';
  _dayPopHandler=function(){ closeDayPopover(); };
  setTimeout(function(){ document.addEventListener('click', _dayPopHandler, {once:true}); }, 10);
}
function _dayRowClick(id, ev){
  closeDayPopover();
  if(typeof showQuickPopup==='function') showQuickPopup(id, ev.clientX, ev.clientY);
}
window.showDayPopover=showDayPopover;
window.closeDayPopover=closeDayPopover;
window._dayRowClick=_dayRowClick;

function renderSchDay(){
  const now    = new Date();
  const offset = S.dayOffset || 0;
  const day    = new Date(now);
  day.setDate(now.getDate() + offset);
  day.setHours(0,0,0,0);
  const ds = localDateStr(day);
  const dnames = ['\u041D\u0435\u0434\u0456\u043B\u044F','\u041F\u043E\u043D\u0435\u0434\u0456\u043B\u043E\u043A','\u0412\u0456\u0432\u0442\u043E\u0440\u043E\u043A','\u0421\u0435\u0440\u0435\u0434\u0430','\u0427\u0435\u0442\u0432\u0435\u0440','\u041F\'\u044F\u0442\u043D\u0438\u0446\u044F','\u0421\u0443\u0431\u043E\u0442\u0430'];
  document.getElementById('wklbl').textContent =
    dnames[day.getDay()] + ', ' + day.toLocaleDateString('uk-UA',{day:'numeric',month:'long'});

  // Populate tutor filter
  const tf = document.getElementById('sch-tutor-filter');
  if(tf){
    tf.style.display = R()==='tutor' ? 'none' : '';
    const prev = tf.value;
    tf.innerHTML = '<option value="">\u0412\u0441\u0456 \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438</option>' +
      activeTutors().map(t=>('<option value="'+(t.id)+'">'+(t.fn)+' '+(t.ln)+'</option>')).join('');
    tf.value = prev;
  }

  // Determine tutors to show
  const filterTutor = tf ? tf.value : '';
  let tutors = P().seeAll ? (S.tutors||[]) : (S.tutors||[]).filter(function(t){
    return t.accId===CU?.id || t.acc_uid===CU?.id;
  });
  if(filterTutor) tutors = tutors.filter(t=>t.id===filterTutor);


  const START_H = 7, END_H = 23; // сітка 07:00–23:00; останній рядок 22:00–23:00
  const ROW_H = 48;
  const totalHrs = END_H - START_H;

  const _schStat = (document.getElementById('sch-status-filter')||{value:''}).value;
  const ml = myLessons().filter(function(l){
    if(l.status==='cancelled') return false;
    if(_schStat){
      if(_schStat==='planned') return l.status==='planned'||l.status==='scheduled'||!l.status;
      if(_schStat==='completed') return isDoneLesson(l)||l.status==='makeup';
      return l.status===_schStat;
    }
    return true;
  });

  const cols = tutors.length || 1;
  let html = '<div class="schh" style="background:var(--s1)">\u0427\u0430\u0441</div>';
  if(tutors.length === 0){
    html += '<div class="schh">\u041D\u0435\u043C\u0430\u0454 \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0456\u0432</div>';
  } else {
    tutors.forEach(function(t){
      html += '<div class="schh"><div style="font-weight:700;font-size:12px">'+t.fn+'</div><div style="font-size:10px;color:var(--t2)">'+t.ln+'</div></div>';
    });
  }

  // Time column
  html += '<div style="position:relative;height:'+(totalHrs*ROW_H)+'px;background:var(--s2);border-right:1px solid var(--b1)">';
  for(var h=START_H;h<END_H;h++){
    html += '<div style="position:absolute;top:'+((h-START_H)*ROW_H)+'px;height:'+ROW_H+'px;width:100%;display:flex;align-items:flex-start;padding-top:3px;justify-content:center;font-size:10px;color:var(--t3);font-family:JetBrains Mono,monospace;box-sizing:border-box;border-top:1px solid var(--b1)">'+String(h).padStart(2,'0')+':00</div>';
  }
  html += '</div>';

  if(tutors.length === 0){
    html += '<div style="position:relative;height:'+(totalHrs*ROW_H)+'px"></div>';
  } else {
    tutors.forEach(function(t){
      const dayLessons = ml.filter(function(l){
        return l.date===ds && (l.tutorId===t.id || l.tutor_id===t.id);
      });
      html += '<div style="position:relative;height:'+(totalHrs*ROW_H)+'px;border-right:1px solid var(--b1)">';
      for(var hh=START_H;hh<END_H;hh++){
        var slotOnclick = 'openLessM(null,\''+ds+'\',\''+String(hh).padStart(2,'0')+':00\')';
        html += '<div onclick="'+slotOnclick+'" style="position:absolute;top:'+((hh-START_H)*ROW_H)+'px;left:0;right:0;height:'+ROW_H+'px;border-top:1px solid var(--b1);box-sizing:border-box;cursor:pointer"></div>';
      }
      var laneMapD=schAssignLanes(dayLessons);
      dayLessons.forEach(function(l){
        const parts = (l.time||'08:00').split(':');
        const lh = parseInt(parts[0])||8;
        const lm = parseInt(parts[1])||0;
        const dur = parseFloat(l.dur)||60;
        const topPx = (lh-START_H)*ROW_H + (lm/60)*ROW_H;
        const heightPx = Math.max((dur/60)*ROW_H, 18);
        if(lh<START_H||lh>=END_H) return;
        var _isCov = l.status==='missed' && isCoveredMissed(l);
        var _unHrs = l.status==='missed'?uncoveredMissedHours(l):0;
        var _isPartial = !_isCov && l.status==='missed' && _unHrs*60 < (parseFloat(l.dur)||60);
        var ecl = _isCov ? 'ec-covered'
          : _isPartial ? 'ec-partial'
          : l.status==='missed'  ? 'ec-miss'
          : l.status==='makeup'  ? 'ec-make'
          : l.status==='makeup_planned' ? 'ec-makeplan'
          : l.status==='burned' ? 'ec-burned' : l.status==='testing' ? 'ec-testing' : isDoneLesson(l) ? 'ec-done'
          : 'ec-plan';
        var liD=laneMapD.get(l.id)||{lane:0,count:1};
        var wPctD=100/liD.count;
        var posCssD='left:calc('+(liD.lane*wPctD)+'% + 2px);width:calc('+wPctD+'% - 4px);';
        var canDel = can('lessons');
        html += '<div class="sche '+ecl+'" style="position:absolute;top:'+topPx+'px;'+posCssD+'height:'+(heightPx-2)+'px;box-sizing:border-box;overflow:hidden;z-index:2;cursor:pointer;'
          +'box-shadow:inset 3px 0 0 '+tutorColor(l.tutorId||l.tutor_id)+'"'
          +' title="'+(l.time||'')+' \u00B7 '+snShort(l.studentId||l.student_id)+(l.subject?' \u00B7 '+l.subject:'')+' \u00B7 '+Math.round(dur/60*10)/10+'\u0433"'
          +' onclick="event.stopPropagation();showQuickPopup(\''+l.id+'\',event.clientX,event.clientY)">'
          +'<div style="font-weight:700;font-size:10px;line-height:1.2;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;padding-right:12px">'+(l.status==='burned'?'\uD83D\uDD25 ':'')+(l.recurId?'\uD83D\uDD01 ':'')+snShort(l.studentId||l.student_id)+'</div>'
          +(heightPx>28?'<div style="font-weight:400;opacity:.8;font-size:9px;line-height:1.15;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+(l.subject||'')+'</div>':'')
          +(heightPx>40?'<div style="opacity:.6;font-size:9px;line-height:1.15;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+(l.time||'')+(dur>=60?' \u00B7 '+Math.floor(dur/60)+'\u0433'+(dur%60?dur%60+'\u0445\u0432':''):'\u00B7 '+dur+'\u0445\u0432')+'</div>':'')
          +(_isPartial&&heightPx>52?'<div style="font-size:9px;font-weight:700">\u26A0 \u0437\u0430\u043B\u0438\u0448\u0438\u043B\u043E\u0441\u044C '+_unHrs+'\u0433</div>':'')
          +(canDel?'<span onclick="event.stopPropagation();delLesson(\''+l.id+'\')" style="position:absolute;top:2px;right:3px;font-size:10px;opacity:.6;cursor:pointer;line-height:1" title="\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438">\u2715</span>':'')
          +'</div>';
      });
      html += '</div>';
    });
  }

  const g = document.getElementById('schg');
  // При багатьох репетиторах даємо колонкам фіксовану мін. ширину і вмикаємо горизонтальний скрол,
  // інакше 1fr стискає всіх у ширину екрана і підписи/уроки не влазять.
  var colW = cols>=4 ? '150px' : '1fr';
  g.style.gridTemplateColumns = '52px repeat('+cols+','+colW+')';
  g.style.gridTemplateRows    = 'auto '+(totalHrs*ROW_H)+'px';
  g.style.minWidth = cols>=4 ? (52 + cols*150) + 'px' : '';
  g.innerHTML = html;
}




// ══════════ ДОСТУПНІСТЬ РЕПЕТИТОРІВ (робочі години) ══════════
function tutorAvail(t){
  if(!t) return null;
  var a=t.availability;
  if(a==null||a==='') return null;
  try{ a=(typeof a==='string')?JSON.parse(a):a; }catch(e){ return null; }
  if(!a||typeof a!=='object') return null;
  var any=Object.keys(a).some(function(k){return Array.isArray(a[k])&&a[k].length;});
  return any?a:null;
}
function isWorkingHour(t, dowMon1, hour){
  var a=tutorAvail(t);
  if(!a) return true;
  var arr=a[String(dowMon1)]||[];
  return arr.indexOf(hour)>=0;
}
function dowMon1FromDate(ds){
  var d=new Date(ds+'T00:00:00'); var g=d.getDay(); return g===0?7:g;
}
function availTargetTutor(){
  var _sf=document.getElementById('sch-tutor-filter');
  var _schTut=_sf?_sf.value:'';
  if(R()==='tutor'){ var mt=myTutor(); return mt||null; }
  if(_schTut) return (S.tutors||[]).find(function(t){return t.id===_schTut;})||null;
  return null;
}
function toggleAvailMode(){
  if(!S._availMode){
    var tgt=availTargetTutor();
    if(!tgt){ mkToast('Спочатку оберіть репетитора у фільтрі','error'); return; }
  }
  S._availMode=!S._availMode;
  renderSch();
}
async function availToggleCell(dowMon1, hour){
  var t=availTargetTutor();
  if(!t) return;
  var a=tutorAvail(t)||{};
  a=JSON.parse(JSON.stringify(a));
  var key=String(dowMon1);
  a[key]=a[key]||[];
  var idx=a[key].indexOf(hour);
  if(idx>=0) a[key].splice(idx,1); else a[key].push(hour);
  a[key].sort(function(x,y){return x-y;});
  t.availability=a;
  renderSch();
  try{ await dbUpdate('tutors', t.id, {availability:JSON.stringify(a)}); }catch(e){}
}
function openAvailFillM(){
  var t=availTargetTutor();
  if(!t){ mkToast('Оберіть репетитора','error'); return; }
  openM('mo-avail-fill');
}
async function availApplyFill(){
  var t=availTargetTutor();
  if(!t){ mkToast('Оберіть репетитора','error'); return; }
  var days=Array.from(document.querySelectorAll('#af-days input:checked')).map(function(c){return c.value;});
  if(!days.length){ mkToast('Оберіть хоча б один день','error'); return; }
  var from=parseInt((document.getElementById('af-from')||{value:'9'}).value);
  var to=parseInt((document.getElementById('af-to')||{value:'18'}).value);
  if(isNaN(from)||isNaN(to)||from>=to){ mkToast('Некоректний діапазон годин','error'); return; }
  var replace=(document.getElementById('af-replace')||{checked:true}).checked;
  var a=replace?{}:JSON.parse(JSON.stringify(tutorAvail(t)||{}));
  days.forEach(function(dw){
    var arr=replace?[]:(a[dw]||[]);
    for(var h=from;h<to;h++){ if(arr.indexOf(h)<0) arr.push(h); }
    arr.sort(function(x,y){return x-y;});
    a[dw]=arr;
  });
  t.availability=a;
  closeM('mo-avail-fill');
  renderSch();
  mkToast('Робочі години оновлено');
  try{ await dbUpdate('tutors', t.id, {availability:JSON.stringify(a)}); }catch(e){}
}
window.openAvailFillM=openAvailFillM;
window.availApplyFill=availApplyFill;
async function availClear(){
  var t=availTargetTutor();
  if(!t){ mkToast('Оберіть репетитора','error'); return; }
  if(!confirm('Очистити всі робочі години?')) return;
  t.availability=null; renderSch();
  try{ await dbUpdate('tutors', t.id, {availability:null}); }catch(e){}
}
window.toggleAvailMode=toggleAvailMode;
window.availToggleCell=availToggleCell;
window.availClear=availClear;

function renderSchWeek(){
  const now=new Date(), sow=new Date(now);
  const dy=now.getDay()===0?6:now.getDay()-1;
  sow.setDate(now.getDate()-dy+S.weekOffset*7); sow.setHours(0,0,0,0);
  const days=Array.from({length:7},(_,i)=>{const d=new Date(sow);d.setDate(sow.getDate()+i);return d;});
  const dnames=['Пн','Вт','Ср','Чт','Пт','Сб','Нд'];
  document.getElementById('wklbl').textContent=days[0].toLocaleDateString('uk-UA',{day:'numeric',month:'short'})+' — '+days[6].toLocaleDateString('uk-UA',{day:'numeric',month:'short'});

  const ROW_H = 48; // px per hour
  const _schStat=(document.getElementById('sch-status-filter')||{value:''}).value;
  const _schTut=(document.getElementById('sch-tutor-filter')||{value:''}).value;
  const ml=myLessons().filter(function(l){
    if(l.status==='cancelled') return false;
    if(_schTut && (l.tutorId||l.tutor_id)!==_schTut) return false;
    if(_schStat){
      if(_schStat==='planned') return l.status==='planned'||l.status==='scheduled'||!l.status;
      if(_schStat==='completed') return isDoneLesson(l)||l.status==='makeup';
      return l.status===_schStat;
    }
    return true;
  });

  const START_H = 7, END_H = 23; // сітка 07:00–23:00; останній рядок 22:00–23:00
  const totalHrs = END_H - START_H;

  // Build header
  let html = '<div class="schh" style="background:var(--s1)">Час</div>';
  days.forEach(function(d,i){
    const today=d.toDateString()===now.toDateString();
    html+='<div class="schh" style="'+(today?'color:var(--adm);border-bottom:2px solid var(--adm)':'')+'">'
      +dnames[i]+'<br><span style="font-size:9px;font-weight:400;color:var(--t3);font-family:JetBrains Mono,monospace">'
      +d.getDate()+'.'+String(d.getMonth()+1).padStart(2,'0')+'</span></div>';
  });

  // Time column + day columns using position:relative inside fixed-height container
  // Time slots column
  html += '<div style="position:relative;height:'+(totalHrs*ROW_H)+'px;background:var(--s2);border-right:1px solid var(--b1)">';
  for(var h=START_H;h<END_H;h++){
    html+='<div style="position:absolute;top:'+(( h-START_H)*ROW_H)+'px;height:'+ROW_H+'px;width:100%;display:flex;align-items:flex-start;padding-top:3px;justify-content:center;font-size:10px;color:var(--t3);font-family:JetBrains Mono,monospace;box-sizing:border-box;border-top:1px solid var(--b1)">'
      +String(h).padStart(2,'0')+':00</div>';
  }
  html += '</div>';

  // Day columns
  var _availTut = availTargetTutor();
  var _availMode = S._availMode && !!_availTut;
  days.forEach(function(d){
    const ds=d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');
    const dow=dowMon1FromDate(ds);
    const dayLessons=ml.filter(function(l){return l.date===ds;});
    html+='<div style="position:relative;height:'+(totalHrs*ROW_H)+'px;border-right:1px solid var(--b1)">';
    // Hour grid lines
    for(var h=START_H;h<END_H;h++){
      var _working = _availTut ? isWorkingHour(_availTut, dow, h) : true;
      var _offBg = (_availTut && !_working)
        ? 'background:repeating-linear-gradient(45deg,rgba(148,163,184,.14),rgba(148,163,184,.14) 6px,rgba(148,163,184,.05) 6px,rgba(148,163,184,.05) 12px);'
        : (_availTut && _working ? 'background:rgba(34,197,94,.07);' : '');
      var _cellClick = _availMode
        ? ('availToggleCell('+dow+','+h+')')
        : ('openLessM(null,\''+ds+'\',\''+String(h).padStart(2,'0')+':00\')');
      html+='<div class="'+(_availMode?'avail-cell':'sch-drop')+'"'
        +(_availMode?'':' ondragover="schDragOver(event)" ondragleave="schDragLeave(event)"'
          +' ondrop="schDrop(event,\''+ds+'\',\''+String(h).padStart(2,'0')+':00\')"')
        +' onclick="event.stopPropagation();'+_cellClick+'" style="position:absolute;top:'+((h-START_H)*ROW_H)+'px;left:0;right:0;height:'+ROW_H+'px;border-top:1px solid var(--b1);box-sizing:border-box;cursor:pointer;'+_offBg+'">'
        +(_availMode&&_working?'<span style="position:absolute;top:2px;left:3px;font-size:9px;color:#16a34a;opacity:.7">\u2713</span>':'')
        +'</div>';
    }
    // Lessons — уроки, що перетинаються в часі, ділять клітинку по вертикалі
    var laneMap=schAssignLanes(dayLessons);
    dayLessons.forEach(function(l){
      const parts=(l.time||'08:00').split(':');
      const lh=parseInt(parts[0])||8;
      const lm=parseInt(parts[1])||0;
      const dur=parseFloat(l.dur)||60;
      const topPx=(lh-START_H)*ROW_H + (lm/60)*ROW_H;
      const heightPx=Math.max((dur/60)*ROW_H, 18);
      if(lh<START_H||lh>=END_H) return;
      var _isCov=l.status==='missed'&&isCoveredMissed(l);
      var _unHrs=l.status==='missed'?uncoveredMissedHours(l):0;
      var _isPartial=!_isCov&&l.status==='missed'&&_unHrs*60<(parseFloat(l.dur)||60);
      var ecl=_isCov?'ec-covered'
        :_isPartial?'ec-partial'
        :l.status==='missed'?'ec-miss'
        :l.status==='makeup'?'ec-make'
        :l.status==='makeup_planned'?'ec-makeplan'
        :l.status==='burned'?'ec-burned':l.status==='testing'?'ec-testing':isDoneLesson(l)?'ec-done'
        :'ec-plan';
      var li=laneMap.get(l.id)||{lane:0,count:1};
      var wPct=100/li.count;
      var posCss='left:calc('+(li.lane*wPct)+'% + 2px);width:calc('+wPct+'% - 4px);';
      var canDel=can('lessons');
      html+='<div class="sche '+ecl+'"'
        +(canDel?' draggable="true" ondragstart="schDragStart(event,\''+l.id+'\')" ondragend="schDragEnd(event)"':'')
        +' style="position:absolute;top:'+topPx+'px;'+posCss+'height:'+(heightPx-2)+'px;box-sizing:border-box;overflow:hidden;z-index:2;cursor:pointer;'
        +'box-shadow:inset 3px 0 0 '+tutorColor(l.tutorId||l.tutor_id)+'"'
        +' title="'+(l.time||'')+' · '+snShort(l.studentId||l.student_id)+(l.subject?' · '+l.subject:'')+' · '+Math.round(dur/60*10)/10+'г"'
        +' onclick="event.stopPropagation();showQuickPopup(\''+l.id+'\',event.clientX,event.clientY)">'
        +'<div style="font-weight:700;font-size:10px;line-height:1.2;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;padding-right:12px">'+(l.status==='burned'?'🔥 ':'')+(l.recurId?'🔁 ':'')+snShort(l.studentId||l.student_id)+'</div>'
        +(heightPx>28?'<div style="font-weight:400;opacity:.8;font-size:9px;line-height:1.15;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+(l.subject||'')+'</div>':'')
        +(heightPx>40?'<div style="opacity:.6;font-size:9px;line-height:1.15;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+(l.time||'')+(dur>=60?' · '+Math.floor(dur/60)+'г'+(dur%60?dur%60+'хв':''):' · '+dur+'хв')+'</div>':'')
        +(_isPartial&&heightPx>52?'<div style="font-size:9px;font-weight:700">⚠ залишилось '+_unHrs+'г</div>':'')
        +(canDel?'<span onclick="event.stopPropagation();delLesson(\''+l.id+'\')" style="position:absolute;top:2px;right:3px;font-size:10px;opacity:.6;cursor:pointer;line-height:1" title="Видалити">✕</span>':'')
        +'</div>';
    });
    html+='</div>';
  });

  const g=document.getElementById('schg');
  g.style.gridTemplateColumns='52px repeat(7,1fr)';
  g.style.gridTemplateRows='auto '+(totalHrs*ROW_H)+'px';
  g.innerHTML=html;
}
