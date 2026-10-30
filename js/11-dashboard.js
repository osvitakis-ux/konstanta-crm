
function renderDash(){
  // Розумні підказки — усім, хто керує процесом
  try{
    var _ic=document.getElementById('insights-card');
    var _ir=R();
    var _ishow=(_ir==='god'||_ir==='director'||_ir==='admin'||_ir==='network_admin');
    if(_ic){
      _ic.style.display=_ishow?'block':'none';
      if(_ishow) renderInsights();
    }
  }catch(e){ console.error('insights:',e); }

  // Живий пульс — керівництву й адміністраторам, які ведуть роботу центру
  try{
    var _pc=document.getElementById('pulse-card');
    var _r=R();
    var _show=(_r==='god'||_r==='director'||_r==='admin'||_r==='network_admin');
    if(_pc){
      _pc.style.display=_show?'block':'none';
      if(_show) startPulse();
    }
  }catch(e){ console.error('pulse:',e); }
  try{ renderRatingBlock(); }catch(e){ console.error('renderRatingBlock:',e); }
  try{ renderDashStats(); }catch(e){ console.error('renderDashStats:',e); }
  try{ renderDashKpi(); }catch(e){ console.error('renderDashKpi:',e); }
  try{ renderDashTrends(); }catch(e){ console.error('renderDashTrends:',e); showErr('renderDashTrends: '+e.message); }
  try{ renderCommLog(); }catch(e){ console.error('renderCommLog:',e); }
  try{ renderDashBottom(); }catch(e){ console.error('renderDashBottom:',e); }
}
function showErr(msg){
  var d=document.getElementById('debug-err')||document.createElement('div');
  d.id='debug-err';
  d.style.cssText='position:fixed;bottom:10px;right:10px;background:#f8d7da;color:#721c24;padding:12px 16px;border-radius:8px;font-size:12px;font-family:monospace;z-index:9999;max-width:400px;word-break:break-all';
  d.textContent=msg;
  document.body.appendChild(d);
}

function renderDashStats(){
  var ml=myLessons(), ms=myStudents(), now=new Date();
  var monthL=ml.filter(function(l){
    var d=new Date(l.date);
    return d.getMonth()===now.getMonth()&&d.getFullYear()===now.getFullYear();
  });
  var nb=document.getElementById('nb-s'); if(nb)nb.textContent=myStudents().filter(function(s){return s.status==='active';}).length;
  // Тренди за 6 місяців для міні-графіків у картках
  var _spkLessons = monthlySeries(function(key){
    return myLessons().filter(function(l){
      return String(l.date||'').slice(0,7)===key && (isDoneLesson(l)||l.status==='makeup');
    }).reduce(function(a,l){ return a+(parseFloat(l.dur)||60)/60; },0);
  });
  var _spkIncome = monthlySeries(function(key){
    return myLessons().filter(function(l){
      return String(l.date||'').slice(0,7)===key && (isDoneLesson(l)||l.status==='makeup');
    }).reduce(function(a,l){ return a+(lessonTotal(l)||0); },0);
  });
  var _spkStudents = monthlySeries(function(key){
    // Скільки унікальних учнів мали заняття того місяця
    var ids={};
    myLessons().forEach(function(l){
      if(String(l.date||'').slice(0,7)===key) ids[l.studentId||l.student_id]=1;
    });
    return Object.keys(ids).length;
  });

  var statsHtml='<div class="sc blue">'
    +'<div class="slbl">\u0410\u043A\u0442\u0438\u0432\u043D\u0438\u0445 \u0443\u0447\u043D\u0456\u0432</div>'
    +'<div class="sval">'+ms.filter(function(s){return s.status==='active';}).length+'</div>'
    +'<div class="ssub">\u0417\u0430\u0433\u0430\u043B\u043E\u043C: '+ms.length+'</div>'
    +'<div class="spk-wrap">'+sparkline(_spkStudents,'var(--adm)')+'</div>'
    +'<span class="sico">\u25CE</span></div>'
    +'<div class="sc green">'
    +'<div class="slbl">\u0417\u0430\u043D\u044F\u0442\u044C \u0446\u044C\u043E\u0433\u043E \u043C\u0456\u0441\u044F\u0446\u044F</div>'
    +'<div class="sval">'+Math.round(monthL.filter(function(l){return l.status==='planned'||l.status==='scheduled'||isDoneLesson(l)||l.status==='makeup';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10+'</div>'
    +'<div class="ssub">\u041F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E: '+Math.round(monthL.filter(function(l){return isDoneLesson(l)||l.status==='makeup';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10+'</div>'
    +'<div class="spk-wrap">'+sparkline(_spkLessons,'var(--tut)')+'</div>'
    +'<span class="sico">\u25C9</span></div>';
  if(P().seeIncome && R()!=='tutor'){
    var inc=monthL.filter(function(l){
      return isDoneLesson(l)||l.status==='makeup';
    }).reduce(function(a,l){return a+lessonTotal(l);},0);
    statsHtml+='<div class="sc yellow">'      +'<div class="slbl">Дохід цього місяця</div>'      +'<div class="sval">'+Math.round(inc).toLocaleString('uk-UA')+'₴</div>'      +'<div class="ssub">Отримано</div>'
      +'<div class="spk-wrap">'+sparkline(_spkIncome,'#f59e0b')+'</div>'
      +'<span class="sico">◈</span></div>';
    } else if(R()!=='tutor') {


    statsHtml+='<div class="sc yellow" style="opacity:.4">'
      +'<div class="slbl">\u0414\u043E\u0445\u0456\u0434 \u043C\u0456\u0441\u044F\u0446\u044F</div>'
      +'<div class="sval">\uD83D\uDD12</div>'
      +'<div class="ssub">\u0422\u0456\u043B\u044C\u043A\u0438 \u0434\u0438\u0440\u0435\u043A\u0442\u043E\u0440</div><span class="sico">\u25C8</span></div>';
  }
    if(R()!=='tutor'){
  statsHtml+='<div class="sc red">'
    +'<div class="slbl">\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0456\u0432</div>'
    +'<div class="sval">'+S.tutors.length+'</div>'
    +'<div class="ssub">\u0410\u043A\u0442\u0438\u0432\u043D\u0438\u0445</div><span class="sico">\u25C8</span></div>';
  }
  document.getElementById('dash-stats').innerHTML=statsHtml;
}

function renderDashKpi(){
  var offset=S.dashWeekOffset||0;
  var wr=getWeekRange(offset);
  var lbl=document.getElementById('dash-week-lbl');
  if(lbl)lbl.textContent=wr.label;
  var tlbl=document.getElementById('dash-tutor-week-lbl');
  if(tlbl)tlbl.textContent=wr.label;

  var allL=myLessons();
  var weekL=allL.filter(function(l){return inWeek(l.date,wr);});
  var _myT=myTutor();
  var weekComms=(S.comms||[]).filter(function(c){
    return inWeek(c.date,wr)&&(R()!=='tutor'||!_myT||(c.tutorId||c.tutor_id)===_myT.id);
  });

  // Години проведених занять (тиждень)
  var doneH = Math.round(weekL.filter(function(l){return isDoneLesson(l)||l.status==='makeup';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  // Години запланованих (тиждень, без скасованих)
  var plannedH = Math.round(weekL.filter(function(l){return l.status==='planned'||l.status==='scheduled';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  // Загальні години тижня (проведені + заплановані, без скасованих і пропущених)
  var totalH = Math.round(weekL.filter(function(l){return l.status!=='cancelled';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  // Пропущені — некомпенсовані за 3 місяці, в годинах
  var threeMonthsAgo = new Date(); threeMonthsAgo.setMonth(threeMonthsAgo.getMonth()-3);
  var from3m = localDateStr(threeMonthsAgo);
  var missed3m = uncoveredMissedFilter(allL.filter(function(l){ return l.date>=from3m; }));
  var missedH = Math.round(allL.filter(function(l){return l.status==='missed'&&l.date>=from3m;}).reduce(function(s,l){return s+uncoveredMissedHours(l);},0)*10)/10;
  var cancelled = weekL.filter(function(l){return l.status==='cancelled';}).length;
  var plannedCnt = weekL.filter(function(l){return l.status==='planned'||l.status==='scheduled';}).length;
  var totalComms = weekComms.length;
  // Виконання плану = проведені / (проведені + заплановані) * 100
  var pct = totalH>0 ? Math.round(doneH/totalH*100) : 0;

  var wrPrev=getWeekRange(offset-1);
  var prevL  =allL.filter(function(l){return inWeek(l.date,wrPrev);});
  var prevDoneH =Math.round(prevL.filter(function(l){return isDoneLesson(l)||l.status==='makeup';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  var prevTotalH=Math.round(prevL.filter(function(l){return l.status!=='cancelled';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  var prevMissed3m=uncoveredMissedFilter(prevL);
  var prevMissedH =Math.round(prevMissed3m.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  var prevComms=(S.comms||[]).filter(function(c){return inWeek(c.date,wrPrev);}).length;
  var prevPct=prevTotalH>0?Math.round(prevDoneH/prevTotalH*100):0;

  function trend(cur,prev){
    if(prev===0&&cur===0)return {cls:'same',txt:'\u2014 0'};
    if(prev===0)return {cls:'up',txt:'\u2191 \u043d\u043e\u0432\u0438\u0439'};
    var d=Math.round((cur-prev)*10)/10;
    return d>0?{cls:'up',txt:'\u2191 +'+d}:d<0?{cls:'down',txt:'\u2193 '+d}:{cls:'same',txt:'= '+cur};
  }

  var kpis=[
    {ico:'\u2705',val:doneH+'\u0433', lbl:'\u041f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u043e \u0437\u0430\u043d\u044f\u0442\u044c', sub:plannedCnt+' \u0449\u0435 \u0437\u0430\u043f\u043b\u0430\u043d\u043e\u0432\u0430\u043d\u043e', accent:'var(--tut)', tr:trend(doneH,prevDoneH)},
    {ico:'\u274c',val:missedH+'\u0433', lbl:'\u041f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u043e \u0443\u0447\u043d\u044f\u043c\u0438', sub:'\u0421\u043a\u0430\u0441\u043e\u0432\u0430\u043d\u043e: '+cancelled, accent:'var(--danger)', tr:trend(missedH,prevMissedH)},
    {ico:'\ud83d\udcac',val:totalComms, lbl:'\u041a\u043e\u043c\u0443\u043d\u0456\u043a\u0430\u0446\u0456\u0439', sub:'\u0414\u0437\u0432\u0456\u043d\u043a\u0438 \u0442\u0430 \u043f\u043e\u0432\u0456\u0434\u043e\u043c\u043b\u0435\u043d\u043d\u044f', accent:'var(--adm)', tr:trend(totalComms,prevComms)},
    {ico:'\ud83d\udcc8',val:pct+'%', lbl:'\u0412\u0438\u043a\u043e\u043d\u0430\u043d\u043d\u044f \u043f\u043b\u0430\u043d\u0443', sub:doneH+'\u0433 \u0437 '+totalH+'\u0433', accent:'#a78bfa', tr:trend(pct,prevPct)},
  ];
  var wkpiEl=document.getElementById('dash-week-kpi');
  if(wkpiEl){
    wkpiEl.innerHTML=kpis.map(function(k){
      return '<div class="kpi-card" style="--kpi-accent:'+k.accent+'">'
        +'<div class="kpi-ico">'+k.ico+'</div>'
        +'<div class="kpi-val">'+k.val+'</div>'
        +'<div class="kpi-lbl">'+k.lbl+'</div>'
        +'<div class="kpi-sub">'+k.sub+'</div>'
        +'<div class="kpi-badge '+k.tr.cls+'">'+k.tr.txt+'</div>'
        +'</div>';
    }).join('');
  }

  // = Per-tutor KPI table =
  var tbody=document.getElementById('dash-tutor-kpi');
  if(!tbody)return;

  var tutors=R()==='tutor'
    ? S.tutors.filter(function(t){return CU && t.accId===CU.id;})
    : S.tutors;

  if(!tutors.length){
    tbody.innerHTML='<tr><td colspan="8" class="empty" style="padding:20px">\u041D\u0435\u043C\u0430\u0454 \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0456\u0432</td></tr>';
    return;
  }

  var maxDoneH=Math.max.apply(null,tutors.map(function(t){
    return weekL.filter(function(l){return (l.tutorId===t.id||l.tutor_id===t.id)&&(isDoneLesson(l)||l.status==='makeup');}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0);
  }).concat([1]));

  var totalDoneH=0,totalMissedH=0,totalPlanned=0,totalTutComms=0,totalStudents=0;
  var rowsArr=[];
  tutors.forEach(function(t){
    var tl=weekL.filter(function(l){return l.tutorId===t.id||l.tutor_id===t.id;});
    // Проведено — години за тиждень
    var tDoneH  =Math.round(tl.filter(function(l){return isDoneLesson(l)||l.status==='makeup';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
    // Заплановано — кількість
    var tPlanned=tl.filter(function(l){return l.status==='planned'||l.status==='scheduled';}).length;
    // Заплановані години
    var tPlannedH=Math.round(tl.filter(function(l){return l.status==='planned'||l.status==='scheduled';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
    // Пропущені — за ЦЕЙ ТИЖДЕНЬ (раніше рахувалось за 3 місяці, що не збігалось з періодом таблиці)
    var tMissedH=Math.round(tl.filter(function(l){return l.status==='missed';}).reduce(function(s,l){return s+uncoveredMissedHours(l);},0)*10)/10;
    var tComms  =weekComms.filter(function(c){return c.tutorId===t.id||c.tutor_id===t.id;}).length;
    var tStudents=S.students.filter(function(s){return s.status==='active'&&studentTutorIds(s).indexOf(t.id)>=0;}).length;
    // Виконання = проведено / (проведено + заплановано + пропущено) * 100 —
    // пропуски мають знижувати відсоток, а не ігноруватись (інакше 100% навіть при повних пропусках)
    var tTotalH =Math.round((tDoneH+tPlannedH+tMissedH)*10)/10;
    var tPct    =tTotalH>0?Math.round(tDoneH/tTotalH*100):(tPlanned>0?0:100);
    var barW    =maxDoneH>0?Math.round(tDoneH/maxDoneH*100):0;
    var pctColor=tPct>=80?'var(--tut)':tPct>=50?'var(--dir)':'var(--danger)';

    totalDoneH+=tDoneH; totalMissedH+=tMissedH;
    totalPlanned+=tPlanned; totalTutComms+=tComms; totalStudents+=tStudents;

    // Тренд проведених годин vs минулий тиждень
    var prevTl=prevL.filter(function(l){return l.tutorId===t.id||l.tutor_id===t.id;});
    var prevTDoneH=Math.round(prevTl.filter(function(l){return isDoneLesson(l)||l.status==='makeup';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
    var trendTxt='', trendCls='same';
    var dd=Math.round((tDoneH-prevTDoneH)*10)/10;
    if(dd>0){trendTxt='\u2191+'+dd;trendCls='up';}
    else if(dd<0){trendTxt='\u2193'+dd;trendCls='down';}
    else if(prevTDoneH>0){trendTxt='='+tDoneH;trendCls='same';}

    var rowHtml = '<tr>'
      +'<td style="min-width:180px"><div style="display:flex;align-items:center;gap:10px">'+mkAv(t.fn,t.ln,34,t.photo)
      +'<div style="min-width:0"><div style="font-weight:600;font-size:12px;line-height:1.3">'+t.fn+'</div>'
      +'<div style="font-weight:700;font-size:12px;line-height:1.3">'+t.ln+'</div>'
      +'<div style="font-size:10px;color:var(--t3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:130px">'+(t.subj||'\u2014')+'</div></div></div></td>'

      +'<td><div style="display:flex;align-items:center;gap:8px">'
      +'<span style="font-weight:700;font-size:18px;font-family:Syne,sans-serif;color:var(--tut)">'+tDoneH+'\u0433</span>'
      +(trendTxt?'<span class="kpi-badge '+trendCls+'" style="font-size:9px">'+trendTxt+'</span>':'')
      +'</div>'
      +'<div class="mini-bar"><div class="mini-fill" style="width:'+barW+'%;background:var(--tut)"></div></div></td>'

      +'<td style="text-align:center">'
      +'<span style="font-weight:600;font-size:15px;color:var(--t2)">'+tPlanned+'</span>'
      +'</td>'

      +'<td style="text-align:center">'
      +'<span style="font-weight:700;font-size:16px;color:'+(tMissedH>0?'var(--danger)':'var(--t3)')+'">'+tMissedH+'\u0433</span>'
      +'</td>'

      +'<td style="text-align:center">'
      +'<span style="font-weight:700;font-size:16px;color:var(--adm)">'+tComms+'</span>'
      +'</td>'

      +'<td style="text-align:center">'
      +'<span style="font-size:14px">'+tStudents+'</span>'
      +'</td>'

      +'<td>'
      +'<div style="font-weight:700;font-size:15px;color:'+pctColor+'">'+tPct+'%</div>'
      +'<div style="font-size:10px;color:var(--t3)">'+tDoneH+'\u0433 / '+tTotalH+'\u0433</div>'
      +'</td>'
      +'</tr>';
    rowsArr.push(rowHtml);
  });
  var rows = rowsArr.join('');

  // Підсумковий рядок
  totalDoneH=Math.round(totalDoneH*10)/10;
  totalMissedH=Math.round(totalMissedH*10)/10;
  var totalPct=0;
  if(R()!=='tutor'){
    var allTotalH=Math.round(weekL.filter(function(l){return l.status!=='cancelled';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
    totalPct=allTotalH>0?Math.round(totalDoneH/allTotalH*100):0;
    var totalPctColor=totalPct>=80?'var(--tut)':totalPct>=50?'var(--dir)':'var(--danger)';
    rows+='<tr style="background:rgba(255,255,255,.03);font-weight:700;border-top:2px solid var(--b1)">'
    +'<td><span style="font-size:12px;color:var(--t2);letter-spacing:.5px">\u0420\u0410\u0417\u041e\u041c / \u0421\u0415\u0420\u0415\u0414\u041d\u0404</span></td>'
    +'<td><span style="font-size:18px;font-family:Syne,sans-serif;color:var(--tut)">'+totalDoneH+'\u0433</span></td>'
    +'<td style="text-align:center;color:var(--t2)">'+totalPlanned+'</td>'
    +'<td style="text-align:center;color:'+(totalMissedH>0?'var(--danger)':'var(--t3)')+'">'+totalMissedH+'\u0433</td>'
    +'<td style="text-align:center;color:var(--adm)">'+totalTutComms+'</td>'
    +'<td style="text-align:center">'+totalStudents+'</td>'
    +'<td><span style="font-weight:700;color:'+totalPctColor+'">'+totalPct+'%</span></td>'
    +'</tr>';
  }
  tbody.innerHTML=rows;
}

function renderDashTrends(){
  if(!CU) return;
  var offset = S.dashWeekOffset||0;
  var curWr = getWeekRange(offset);
  var trendLbl = document.getElementById('dash-trend-week-lbl');
  if(trendLbl) trendLbl.textContent = curWr.label;
  var weeks = [];
  for(var i=3;i>=0;i--){
    var wr = getWeekRange(offset-i);
    var _mt2=myTutor();
    var weekL = myLessons().filter(function(l){return inWeek(l.date,wr);});
    var weekComms = (S.comms||[]).filter(function(c){
      return inWeek(c.date,wr)&&(R()!=='tutor'||!_mt2||(c.tutorId||c.tutor_id)===_mt2.id);
    });
    weeks.push({
      wr:wr,
      done: Math.round(weekL.filter(function(l){return isDoneLesson(l)||l.status==='makeup';}).reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10,
      missed: uncoveredMissedFilter(weekL).length,
      planned:weekL.filter(function(l){return l.status==='planned'||l.status==='scheduled';}).length,
      comms:  weekComms.length,
    });
  }

  var tutors = R()==='tutor'
    ? S.tutors.filter(function(t){return CU && t.accId===CU.id;})
    : S.tutors;

  function miniChart(containerId, data, color, keyFn){
    var el = document.getElementById(containerId);
    if(!el) return;
    var max = Math.max.apply(null, data.map(keyFn).concat([1]));

    // Header bars
    var barsHtml = '<div class="trend-weeks">';
    data.forEach(function(w, i){
      var val = keyFn(w);
      var pct = Math.round(val/max*100);
      var isNow = (i===data.length-1);
      var fmt = {day:'2-digit',month:'2-digit'};
      var sun = new Date(w.wr.mon); sun.setDate(sun.getDate()+6);
      var lbl = w.wr.mon.toLocaleDateString('uk-UA',fmt)+'–'+sun.toLocaleDateString('uk-UA',fmt);
      barsHtml += '<div class="trend-week'+(isNow?' trend-week-now':'')+'">'
        +'<div class="trend-week-val">'+val+'</div>'
        +'<div class="trend-week-bar-wrap">'
          +'<div class="trend-week-bar" style="width:'+pct+'%;background:'+color+'"></div>'
        +'</div>'
        +'<div class="trend-week-lbl">'+lbl+'</div>'
        +'</div>';
    });
    barsHtml += '</div>';

    // Per-tutor rows
    var tutorRows = '';
    tutors.slice(0,6).forEach(function(t){
      var vals = data.map(function(w){
        if(containerId==='dash-trend-comms'){
          return (S.comms||[]).filter(function(c){return inWeek(c.date,w.wr)&&(c.tutor_id===t.id||c.tutorId===t.id);}).length;
        }
        return S.lessons.filter(function(l){return inWeek(l.date,w.wr)&&(l.tutor_id===t.id||l.tutorId===t.id)&&(isDoneLesson(l)||l.status==='makeup');}).length;
      });
      var tMax = Math.max.apply(null, vals.concat([1]));
      var total = vals[vals.length-1];

      tutorRows += '<div class="trend-tutor-row">'
        + mkAv(t.fn,t.ln,24,t.photo)
        + '<div class="trend-tutor-name">'+(t.ln||t.fn)+' '+(t.fn?t.fn[0]+'.':'')+'</div>'
        + '<div class="trend-tutor-bars">'
        + vals.map(function(v,i){
            var pct = Math.round(v/tMax*100);
            var isNow = (i===vals.length-1);
            return '<div class="trend-tutor-col">'
              +'<div class="trend-tutor-num">'+v+'</div>'
              +'<div class="trend-tutor-bar-wrap">'
                +'<div class="trend-tutor-bar" style="width:'+pct+'%;background:'+(isNow?color:'var(--b2)')+'"></div>'
              +'</div>'
              +'</div>';
          }).join('')
        + '</div>'
        + '<div class="trend-tutor-total">'+total+'</div>'
        + '</div>';
    });

    el.innerHTML = barsHtml + (tutorRows ? '<div class="trend-tutor-list">'+tutorRows+'</div>' : '');
  }

  miniChart('dash-trend-lessons', weeks, 'var(--tut)', function(w){return w.done;});
  miniChart('dash-trend-comms',   weeks, 'var(--adm)', function(w){return w.comms;});
}

function renderDashBottom(){
  var now=new Date();
  var ml=myLessons();

  // Upcoming lessons
  var up=[].concat(ml).filter(function(l){
    return new Date(l.date+'T'+(l.time||'00:00'))>=now&&l.status!=='cancelled';
  }).sort(function(a,b){
    return new Date(a.date+'T'+a.time)-new Date(b.date+'T'+b.time);
  }).slice(0,6);
  document.getElementById('dt-lessons').innerHTML=up.length
    ?up.map(function(l){
      return '<tr><td>'+sn(l.studentId)+'</td><td>'+l.subject+'</td>'
        +'<td style="font-family:JetBrains Mono,monospace;font-size:11px">'+fd(l.date)+' '+(l.time||'')+'</td>'
        +'<td>'+bst(l.status)+'</td></tr>';
    }).join('')
    :'<tr><td colspan="4" class="empty" style="padding:20px">\u0417\u0430\u043D\u044F\u0442\u044C \u043D\u0435 \u0437\u0430\u043F\u043B\u0430\u043D\u043E\u0432\u0430\u043D\u043E</td></tr>';

  // Right panel
  var rt=document.getElementById('dash-rt');
  var rb=document.getElementById('dash-rb');
  if(P().seeIncome && R()!=='tutor'){
    if(rt)rt.textContent='\u041E\u0441\u0442\u0430\u043D\u043D\u0456 \u043F\u043B\u0430\u0442\u0435\u0436\u0456';
    var rec=[].concat(S.payments).sort(function(a,b){return new Date(b.date)-new Date(a.date);}).slice(0,6);
    if(rb)rb.innerHTML=rec.length
      ?'<div style="max-height:220px;overflow-y:auto"><table><thead><tr><th>\u0423\u0447\u0435\u043D\u044C</th><th>\u0421\u0443\u043C\u0430</th><th>\u0414\u0430\u0442\u0430</th><th>\u0421\u0442\u0430\u0442\u0443\u0441</th></tr></thead><tbody>'
        +rec.map(function(p){
          return '<tr><td>'+sn(p.studentId)+'</td>'
            +'<td style="font-family:JetBrains Mono,monospace">'+(p.amount||0).toLocaleString('uk-UA')+'\u20B4</td>'
            +'<td style="font-size:11px">'+fd(p.date)+'</td>'
            +'<td>'+bst(p.status)+'</td></tr>';
        }).join('')+'</tbody></table></div>'
      :'<div class="empty" style="padding:20px"><div class="ei">\uD83D\uDCB3</div>\u041F\u043B\u0430\u0442\u0435\u0436\u0456\u0432 \u043D\u0435\u043C\u0430\u0454</div>';
  } else {
    if(rt)rt.textContent='\u0423\u0447\u043D\u0456';
    var msArr=myStudents();
    if(rb)rb.innerHTML=msArr.length
      ?'<div style="max-height:220px;overflow-y:auto"><table><thead><tr><th>\u0406\u043C\'\u044F</th><th>\u041F\u0440\u0435\u0434\u043C\u0435\u0442</th><th>\u0421\u0442\u0430\u0442\u0443\u0441</th></tr></thead><tbody>'
        +msArr.map(function(s){
          return '<tr><td>'+s.fn+' '+s.ln+'</td><td>'+(s.subject||'\u2014')+'</td><td>'+bst(s.status)+'</td></tr>';
        }).join('')+'</tbody></table></div>'
      :'<div class="empty" style="padding:20px"><div class="ei">\uD83D\uDC65</div>\u0423\u0447\u043D\u0456\u0432 \u043D\u0435 \u043F\u0440\u0438\u0437\u043D\u0430\u0447\u0435\u043D\u043E</div>';
  }

  // Subjects chart
  var sc={};ml.forEach(function(l){sc[l.subject]=(sc[l.subject]||0)+(parseFloat(l.dur)||60)/60;});
  var colors=['var(--adm)','var(--tut)','var(--dir)','var(--god2)','#a78bfa','#0ea5e9'];
  var maxS=Math.max.apply(null,Object.values(sc).concat([1]));
  var dashSubjEl=document.getElementById('dash-subj');
  if(dashSubjEl) dashSubjEl.innerHTML=Object.entries(sc)
    .sort(function(a,b){return b[1]-a[1];}).slice(0,6)
    .map(function(e,i){
      return '<div style="margin-bottom:9px">'
        +'<div style="display:flex;justify-content:space-between;margin-bottom:3px">'
        +'<span style="font-size:12px">'+e[0]+'</span>'
        +'<span style="font-size:11px;color:var(--t2);font-family:JetBrains Mono,monospace">'+e[1]+'</span></div>'
        +'<div class="pb"><div class="pf" style="width:'+Math.round(e[1]/maxS*100)+'%;background:'+colors[i%colors.length]+'"></div></div>'
        +'</div>';
    }).join('')||'<div class="empty"><div class="ei">\uD83D\uDCCA</div>\u041D\u0435\u043C\u0430\u0454 \u0434\u0430\u043D\u0438\u0445</div>';

  // Payments status
  var paid=S.payments.filter(function(p){return p.status==='paid';}).reduce(function(a,p){return a+p.amount;},0);
  var pend=S.payments.filter(function(p){return p.status==='pending';}).reduce(function(a,p){return a+p.amount;},0);
  var over=S.payments.filter(function(p){return p.status==='overdue';}).reduce(function(a,p){return a+p.amount;},0);
  // dash-pay card removed from dashboard
  var rbCard = document.getElementById('dash-rb-card');
  var payCard = document.getElementById('dash-pay-card');
  var isTutor = R()==='tutor';
  if(rbCard)  rbCard.style.display  = isTutor ? 'none' : '';
  if(payCard) payCard.style.display = isTutor ? 'none' : '';

  // Hide old comm block - comms now shown inline
  var cb=document.getElementById('dash-comm-block');
  if(cb)cb.style.display='none';
  // Hide payments block for tutors
  var pyBlock=document.getElementById('dash-pay');
  if(pyBlock&&R()==='tutor') pyBlock.closest('.card') && (pyBlock.closest('.card').style.display='none');
}

function renderCommLog(){
  var el  = document.getElementById('dash-comm-log');
  var el2 = document.getElementById('dash-comm-log2');
  if(!el && !el2) return;
  var _myTc=myTutor();
  var comms=[].concat(S.comms||[])
    .filter(function(c){return R()!=='tutor'||!_myTc||(c.tutorId||c.tutor_id)===_myTc.id;})
    .sort(function(a,b){return (b.date||'').localeCompare(a.date||'');}).slice(0,20);
  var typeIco={call:'📞',message:'💬',meeting:'🤝',email:'📧',other:'📋',msg:'💬',meet:'🤝'};
  var html;
  if(!comms.length){
    html='<div class="empty" style="padding:20px"><div class="ei">💬</div>Комунікацій ще не записано</div>';
  } else {
    html=comms.map(function(c){
      var tutor=S.tutors.find(function(t){return t.id===c.tutorId;})||{fn:'',ln:''};
      var student=c.studentId?S.students.find(function(s){return s.id===c.studentId;}):null;
      return '<div class="comm-item">'        +'<div class="comm-ico">'+(typeIco[c.type]||'📋')+'</div>'        +'<div class="comm-body">'        +'<div class="comm-meta">'        +'<span class="comm-type">'+(c.type||'інше')+'</span>'        +'<span class="comm-tutor">'+tutor.fn+' '+tutor.ln+'</span>'        +(student?'<span style="font-size:11px;color:var(--t3)">→ '+student.fn+' '+student.ln+'</span>':'')
        +'<span class="comm-date">'+fd((c.date||'').slice(0,10))+'</span>'        +'</div>'        +'<div class="comm-note">'+(c.note||'—')+'</div>'        +'</div>'        +'<button onclick="delComm(this.dataset.id)" data-id="'+c.id+'" style="background:none;border:none;color:var(--t3);cursor:pointer;font-size:14px;flex-shrink:0">&times;</button>'        +'</div>';
    }).join('');
  }
  if(el)  el.innerHTML  = html;
  if(el2) el2.innerHTML = html;
}

function sfilt(f,el){sfCur=f;document.querySelectorAll('#sfchips .chip').forEach(c=>c.classList.remove('active'));el.classList.add('active');renderStudents();}
