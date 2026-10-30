
// =
// INIT
// =

// =
// ANALYTICS DASHBOARD
// =
function renderAnalytics(){
  var pg=document.getElementById('pg-reports');
  if(!pg)return;

  // Populate tutor filter
  var tutSel=document.getElementById('an-tutor');
  if(tutSel){
    var curTut=tutSel.value;
    tutSel.innerHTML='<option value="">\u0412\u0441\u0456 \u0440\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0438</option>'
      +(S.tutors||[]).map(function(t){return '<option value="'+t.id+'"'+(t.id===curTut?' selected':'')+'>'+t.fn+' '+t.ln+'</option>';}).join('');
    if(curTut) tutSel.value=curTut;
  }

  // Date range
  var range=(document.getElementById('rc-range')||{value:'month'}).value;
  var now=new Date(), fromDate=new Date(now);
  if(range==='week'){
    var day=now.getDay()||7; fromDate=new Date(now); fromDate.setDate(now.getDate()-day+1); fromDate.setHours(0,0,0,0);
  } else if(range==='2week'){
    fromDate.setDate(now.getDate()-14);
  } else if(range==='4week'){
    fromDate.setDate(now.getDate()-28);
  } else if(range==='month'){
    fromDate=new Date(now.getFullYear(),now.getMonth(),1);
  } else if(range==='3month'){
    fromDate=new Date(now.getFullYear(),now.getMonth()-3,1);
  } else if(range==='6month'){
    fromDate=new Date(now.getFullYear(),now.getMonth()-6,1);
  } else if(range==='year'){
    fromDate=new Date(now.getFullYear(),0,1);
  } else {
    fromDate=new Date(0);
  }
  var fromStr=localDateStr(fromDate);
  // Кінець періоду — кінець ОБРАНОГО проміжку, а не «сьогодні».
  // Інакше заплановані заняття далі по місяцю не потрапляли в розрахунок.
  var toDateA;
  if(range==='month'||range==='3month'||range==='6month')
    toDateA=new Date(now.getFullYear(),now.getMonth()+1,0);
  else if(range==='year') toDateA=new Date(now.getFullYear(),11,31);
  else if(range==='all')  toDateA=new Date(now.getFullYear()+5,11,31);
  else toDateA=now;
  var toStr=localDateStr(toDateA);

  // Range label
  var lbl=document.getElementById('an-range-lbl');
  if(lbl && range!=='all'){
    lbl.textContent=fromDate.toLocaleDateString('uk-UA',{day:'numeric',month:'short'})
      +' \u2014 '+now.toLocaleDateString('uk-UA',{day:'numeric',month:'short',year:'numeric'});
  } else if(lbl){ lbl.textContent=''; }

  var selTutor=(tutSel||{value:''}).value;
  var allLessons=(S.lessons||[]).filter(function(l){return l.date>=fromStr&&l.date<=toStr;});
  var lessons=selTutor?allLessons.filter(function(l){return (l.tutorId||l.tutor_id)===selTutor;}):allLessons;
  // Фільтр філії має діяти й тут — інакше при обраній філії цифри
  // рахувались по всій мережі
  allLessons=filterByBranch(allLessons); lessons=filterByBranch(lessons);
  var students=selTutor
    ?(S.students||[]).filter(function(s){return (s.tutorId||s.tutor_id)===selTutor||((s.tutorIds||[]).indexOf(selTutor)>=0);})
    :(S.students||[]);

  function h(arr){return Math.round(arr.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;}

  var done    =lessons.filter(function(l){return isDoneLesson(l)||l.status==='makeup';});
  var missed  =uncoveredMissedFilter(lessons.filter(function(l){return l.status!=='cancelled';}));
  var planned =lessons.filter(function(l){return l.status==='planned'||l.status==='scheduled';});
  var cancelled=lessons.filter(function(l){return l.status==='cancelled';});

  var doneH   =h(done), missedH=h(missed), plannedH=h(planned);
  var totalH  =Math.round((doneH+missedH+plannedH+h(cancelled))*10)/10;
  var income  =done.reduce(function(s,l){return s+lessonTotal(l);},0);
  var activeStudents=filterByBranch(students).filter(function(s){return s.status==='active'||s.status==='trial';}).length;
  var pct     =totalH>0?Math.round(doneH/totalH*100):0;

  // Subjects breakdown (від done)
  // Об'єднуємо предмети, що відрізняються лише регістром чи пробілами
  var subjMap={}, subjLabel={};
  done.forEach(function(l){
    var raw=String(l.subject||'\u0406\u043d\u0448\u0435').trim();
    var key=raw.toLowerCase().replace(/\s+/g,' ');
    if(!subjLabel[key]) subjLabel[key]=raw;
    subjMap[key]=(subjMap[key]||0)+(parseFloat(l.dur)||60)/60;
  });
  var subjArr=Object.keys(subjMap).map(function(k){
    return{name:subjLabel[k]||k, key:k, v:Math.round(subjMap[k]*10)/10};
  }).sort(function(a,b){return b.v-a.v;});

  // Tutors breakdown — всі репетитори, навіть без занять у період
  var tutorArr=[];
  if(!selTutor){
    var tutMap={};
    done.forEach(function(l){var tid=l.tutorId||l.tutor_id; if(tid) tutMap[tid]=(tutMap[tid]||0)+(parseFloat(l.dur)||60)/60;});
    // Include ALL tutors, even with 0 hours
    tutorArr=filterByBranch(S.tutors||[]).map(function(t){
      return{name:t.fn+' '+t.ln, v:Math.round((tutMap[t.id]||0)*10)/10, id:t.id};
    }).sort(function(a,b){return b.v-a.v;});
  }

  var COLORS=['#6366f1','#22c55e','#f59e0b','#ef4444','#14b8a6','#ec4899','#8b5cf6','#f97316','#06b6d4','#84cc16'];

  // Зберігаємо контекст, щоб деталізація знала, які саме заняття показувати
  window._anCtx={fromStr:fromStr,toStr:toStr,selTutor:selTutor};

  function pieChart(canvasId,data,total){
    var c=document.getElementById(canvasId);
    if(!c||!c.getContext)return;
    var ctx=c.getContext('2d');
    var cx=c.width/2,cy=c.height/2,r=Math.min(cx,cy)-8;
    ctx.clearRect(0,0,c.width,c.height);
    if(!total){ctx.beginPath();ctx.arc(cx,cy,r,0,2*Math.PI);ctx.fillStyle='#e5e7eb';ctx.fill();
      ctx.fillStyle='var(--t3)';ctx.font='11px sans-serif';ctx.textAlign='center';ctx.textBaseline='middle';ctx.fillText('0г',cx,cy);return;}
    var start=-Math.PI/2;
    data.forEach(function(d,i){
      if(!d.v)return;
      var angle=(d.v/total)*2*Math.PI;
      ctx.beginPath();ctx.moveTo(cx,cy);ctx.arc(cx,cy,r,start,start+angle);
      ctx.fillStyle=COLORS[i%COLORS.length];ctx.fill();start+=angle;
    });
    ctx.beginPath();ctx.arc(cx,cy,r*0.55,0,2*Math.PI);ctx.fillStyle='var(--s1)';ctx.fill();
    ctx.fillStyle='var(--t1)';ctx.font='bold 13px sans-serif';ctx.textAlign='center';ctx.textBaseline='middle';
    ctx.fillText(total+'\u0433',cx,cy);
  }

  /**
   * Легенда діаграми. Кожен рядок клікабельний — відкриває перелік
   * занять, з яких складається саме цей сегмент.
   */
  function legend(data,total,kind){
    return data.map(function(d,i){
      var pct2=total?Math.round(d.v/total*100):0;
      var arg=JSON.stringify(String(d.key||d.id||d.name)).replace(/"/g,'&quot;');
      return '<div class="an-lg-row" onclick="anDrill(&quot;'+kind+'&quot;,'+arg+')" '
        +'title="\u041f\u043e\u043a\u0430\u0437\u0430\u0442\u0438 \u0437\u0430\u043d\u044f\u0442\u0442\u044f" '
        +'style="display:flex;align-items:center;gap:6px;margin-bottom:5px;font-size:12px;cursor:pointer;padding:3px 5px;border-radius:6px;transition:.12s">'
        +'<span style="width:10px;height:10px;border-radius:50%;background:'+COLORS[i%COLORS.length]+';flex-shrink:0;display:inline-block"></span>'
        +'<span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+d.name+'</span>'
        +'<span style="color:var(--t2);font-size:11px;white-space:nowrap">'+d.v+'\u0433 ('+pct2+'%)</span>'
        +'</div>';
    }).join('');
  }

  function kpiBox(ico,lbl,val,color){
    return '<div style="flex:1;min-width:120px;background:var(--s2);border-radius:12px;padding:12px 14px;border-left:3px solid '+color+'">'
      +'<div style="font-size:18px;font-weight:700;color:'+color+'">'+val+'</div>'
      +'<div style="font-size:11px;color:var(--t2);margin-top:2px">'+ico+' '+lbl+'</div>'
      +'</div>';
  }

  var statusData=[
    {name:'\u041f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u043e',v:doneH,key:'done'},
    {name:'\u041f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u043e',v:missedH,key:'missed'},
    {name:'\u0417\u0430\u043f\u043b\u0430\u043d\u043e\u0432\u0430\u043d\u043e',v:plannedH,key:'planned'},
    {name:'\u0421\u043a\u0430\u0441\u043e\u0432\u0430\u043d\u043e',v:h(cancelled),key:'cancelled'}
  ].filter(function(d){return d.v>0;});

  var tutDoneH=h(done); // для центру діаграми репетиторів

  document.getElementById('an-content').innerHTML=
    // KPI
    '<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:16px">'
    +kpiBox('\u2705','\u0413\u043e\u0434\u0438\u043d \u043f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u043e',doneH,'#22c55e')
    +kpiBox('\u274c','\u0413\u043e\u0434\u0438\u043d \u043f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u043e',missedH,'#ef4444')
    +kpiBox('\uD83D\uDCC5','\u0417\u0430\u043f\u043b\u0430\u043d\u043e\u0432\u0430\u043d\u043e',plannedH,'#6366f1')
    +(P().seeIncome?kpiBox('\uD83D\uDCB0','\u0414\u043e\u0445\u0456\u0434',income+'\u20b4','#14b8a6'):'')
    +kpiBox('\uD83D\uDC65','\u0410\u043a\u0442\u0438\u0432\u043d\u0438\u0445 \u0443\u0447\u043d\u0456\u0432',activeStudents,'#f59e0b')
    +kpiBox('\uD83D\uDCC8','\u0412\u0438\u043a\u043e\u043d\u0430\u043d\u043d\u044f',pct+'%','#8b5cf6')
    +'</div>'
    // Charts
    +'<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:16px">'
    // Status
    +'<div class="card"><div class="ch"><span class="ct">\u0421\u0442\u0430\u0442\u0443\u0441\u0438 \u0437\u0430\u043d\u044f\u0442\u044c</span></div>'
    +'<div style="display:flex;align-items:center;gap:16px;padding:12px">'
    +'<canvas id="pie-status" width="140" height="140" style="flex-shrink:0"></canvas>'
    +'<div style="flex:1;min-width:0">'+legend(statusData,totalH,'status')+'</div>'
    +'</div></div>'
    // Subjects
    +(subjArr.length
      ?'<div class="card"><div class="ch"><span class="ct">\u041f\u043e \u043f\u0440\u0435\u0434\u043c\u0435\u0442\u0430\u0445 (\u0433\u043e\u0434)</span></div>'
      +'<div style="display:flex;align-items:center;gap:16px;padding:12px">'
      +'<canvas id="pie-subj" width="140" height="140" style="flex-shrink:0"></canvas>'
      +'<div style="flex:1;min-width:0">'+legend(subjArr,doneH,'subject')+'</div>'
      +'</div></div>':'')
    // Tutors — all tutors
    +(!selTutor
      ?'<div class="card"><div class="ch"><span class="ct">\u041f\u043e \u0440\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0430\u0445 (\u0433\u043e\u0434)</span></div>'
      +'<div style="display:flex;align-items:center;gap:16px;padding:12px">'
      +'<canvas id="pie-tutor" width="140" height="140" style="flex-shrink:0"></canvas>'
      +'<div style="flex:1;min-width:0">'+legend(tutorArr,tutDoneH,'tutor')+'</div>'
      +'</div></div>':'')
    +'</div>';

  setTimeout(function(){
    pieChart('pie-status',statusData,totalH);
    if(subjArr.length) pieChart('pie-subj',subjArr,doneH);
    if(!selTutor) pieChart('pie-tutor',tutorArr,tutDoneH);
  },50);
}
