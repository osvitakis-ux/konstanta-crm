
// ═══════════════════════════════════════════════════════════════
//  ДРУК РОЗКЛАДУ НА ТИЖДЕНЬ
//  Щоб повісити на стіну в центрі або віддати репетитору.
// ═══════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════
//  ГОЛОСОВЕ ЗАПОВНЕННЯ ПОЛІВ
//  Репетитору швидше продиктувати тему уроку після заняття,
//  ніж набирати її на телефоні. Працює в Chrome та Edge.
// ═══════════════════════════════════════════════════════════════
var _rec = null, _recTarget = null;
// Стан диктування між автоперезапусками сесій розпізнавання
var _recBtn = null, _recField = null;
var _recBase = '';        // текст, що був у полі до початку
var _recAccum = '';       // накопичене за попередні сесії
var _recWantStop = false; // користувач натиснув «зупинити»
var _recRestartTimer = null;
var _recTimer = null, _recSeconds = 0;

// ═══════════════════════════════════════════════════════════════
//  WRAPPED — АНІМОВАНИЙ ПІДСУМОК МІСЯЦЯ
//  Слайди з головними цифрами: скільки провели, хто найактивніший,
//  який предмет попереду. Замість таблиць — розповідь.
// ═══════════════════════════════════════════════════════════════
var _wrIdx = 0, _wrSlides = [];

// ═══════════════════════════════════════════════════════════════
//  ЖИВИЙ ПУЛЬС — стан філій просто зараз
//  Показує, де що відбувається цієї хвилини: скільки занять іде,
//  хто на місці, чи є вільні вікна. Замість гортання розкладу.
// ═══════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════
//  РОЗУМНІ ПІДКАЗКИ
//  Система сама помічає закономірності в даних і пише про них
//  людською мовою. Без жодного AI — чиста логіка на ваших даних,
//  тому працює миттєво, безкоштовно й не потребує інтернету.
// ═══════════════════════════════════════════════════════════════
function smartInsights(){
  var out=[];
  var now=new Date();
  var today=localDateStr(now);
  var per=today.slice(0,7);
  var L=filterByBranch(S.lessons||[]);
  var ST=filterByBranch(S.students||[]).filter(function(x){
    return x.status==='active'||x.status==='trial';
  });

  // ── 1. Учні, які зникли ────────────────────────────────────
  var lost=[];
  ST.forEach(function(st){
    var last=null;
    L.forEach(function(l){
      if((l.studentId||l.student_id)!==st.id) return;
      if(!(isDoneLesson(l)||l.status==='makeup')) return;
      if(!last || String(l.date)>last) last=String(l.date);
    });
    // Заплановані заняття попереду означають, що учень не зник
    var hasFuture=L.some(function(l){
      return (l.studentId||l.student_id)===st.id && String(l.date)>today
        && (l.status==='planned'||l.status==='scheduled');
    });
    if(hasFuture) return;
    if(!last){ return; }   // новий учень без занять — окремий випадок
    var days=Math.round((now-new Date(last+'T12:00:00'))/86400000);
    if(days>=21) lost.push({st:st, days:days});
  });
  if(lost.length){
    lost.sort(function(a,b){ return b.days-a.days; });
    out.push({
      lvl:'warn', ico:'\ud83d\udc4b',
      title: lost.length+' '+prPlural(lost.length,'\u0443\u0447\u0435\u043d\u044c \u0437\u043d\u0438\u043a','\u0443\u0447\u043d\u0456 \u0437\u043d\u0438\u043a\u043b\u0438','\u0443\u0447\u043d\u0456\u0432 \u0437\u043d\u0438\u043a\u043b\u0438'),
      text: lost.slice(0,3).map(function(x){
              return (x.st.fn||'')+' '+(x.st.ln||'')+' \u2014 '+x.days+' \u0434\u043d.';
            }).join(', ') + (lost.length>3?' \u0442\u0430 \u0456\u043d.':''),
      hint:'\u041d\u0435\u043c\u0430\u0454 \u0437\u0430\u043d\u044f\u0442\u044c \u043f\u043e\u043d\u0430\u0434 3 \u0442\u0438\u0436\u043d\u0456 \u0439 \u043d\u0456\u0447\u043e\u0433\u043e \u043d\u0435 \u0437\u0430\u043f\u043b\u0430\u043d\u043e\u0432\u0430\u043d\u043e',
      act:function(){ nav('students'); }
    });
  }

  // ── 2. Невідзначені заняття (це прямі гроші) ───────────────
  var forgotten=L.filter(function(l){
    return (l.status==='planned'||l.status==='scheduled'||!l.status) && String(l.date||'')<today;
  });
  if(forgotten.length){
    var byT={};
    forgotten.forEach(function(l){
      var t=l.tutorId||l.tutor_id; if(t) byT[t]=(byT[t]||0)+1;
    });
    var worst=Object.keys(byT).sort(function(a,b){return byT[b]-byT[a];})[0];
    out.push({
      lvl:'bad', ico:'\u26a0\ufe0f',
      title: forgotten.length+' \u0437\u0430\u043d\u044f\u0442\u044c \u043d\u0435 \u0432\u0456\u0434\u0437\u043d\u0430\u0447\u0435\u043d\u043e',
      text: worst ? ('\u041d\u0430\u0439\u0431\u0456\u043b\u044c\u0448\u0435 \u0432 '+tn(worst)+' \u2014 '+byT[worst]) : '',
      hint:'\u0422\u0430\u043a\u0456 \u0437\u0430\u043d\u044f\u0442\u0442\u044f \u043d\u0435 \u043f\u043e\u0442\u0440\u0430\u043f\u043b\u044f\u044e\u0442\u044c \u043d\u0456 \u0432 \u0437\u0430\u0440\u043f\u043b\u0430\u0442\u0443, \u043d\u0456 \u0432 \u0440\u0430\u0445\u0443\u043d\u043a\u0438',
      act:function(){ nav('lessons'); }
    });
  }

  // ── 3. Непокриті пропуски ──────────────────────────────────
  var unc=L.filter(function(l){
    return l.status==='missed' && String(l.date||'').slice(0,7)===per
      && uncoveredMissedHours(l)>0;
  });
  if(unc.length>=3){
    out.push({
      lvl:'warn', ico:'\ud83d\udd01',
      title: unc.length+' \u043f\u0440\u043e\u043f\u0443\u0441\u043a\u0456\u0432 \u0431\u0435\u0437 \u0432\u0456\u0434\u043f\u0440\u0430\u0446\u044e\u0432\u0430\u043d\u043d\u044f',
      text:'\u0426\u044c\u043e\u0433\u043e \u043c\u0456\u0441\u044f\u0446\u044f',
      hint:'\u0412\u0430\u0440\u0442\u043e \u0437\u0430\u043f\u043b\u0430\u043d\u0443\u0432\u0430\u0442\u0438 \u0432\u0456\u0434\u043f\u0440\u0430\u0446\u044e\u0432\u0430\u043d\u043d\u044f',
      act:function(){ nav('lessons'); }
    });
  }

  // ── 4. Ведення журналу просіло ─────────────────────────────
  (S.tutors||[]).forEach(function(t){
    var r=tutorRating(t.id, per);
    if(r.lessonsTotal<3) return;
    if(r.score<60){
      out.push({
        lvl:'warn', ico:'\ud83d\udcd6',
        title:(t.fn||'')+' '+(t.ln||'')+': \u0440\u0435\u0439\u0442\u0438\u043d\u0433 '+r.score,
        text:(r.issues.noTopic.length?r.issues.noTopic.length+' \u0431\u0435\u0437 \u0442\u0435\u043c\u0438':'')
            +(r.issues.noHw.length?(r.issues.noTopic.length?', ':'')+r.issues.noHw.length+' \u0431\u0435\u0437 \u0414\u0417':''),
        hint:'\u0416\u0443\u0440\u043d\u0430\u043b \u0432\u0435\u0434\u0435\u0442\u044c\u0441\u044f \u043d\u0435\u0434\u0431\u0430\u0439\u043b\u0438\u0432\u043e',
        act:function(){ nav('dashboard'); }
      });
    }
  });

  // ── 5. Порівняння з минулим місяцем ────────────────────────
  var pd=new Date(now.getFullYear(), now.getMonth()-1, 1);
  var prevPer=pd.getFullYear()+'-'+String(pd.getMonth()+1).padStart(2,'0');
  var hrsNow=0, hrsPrev=0;
  L.forEach(function(l){
    if(!(isDoneLesson(l)||l.status==='makeup')) return;
    var k=String(l.date||'').slice(0,7);
    var h=(parseFloat(l.dur)||60)/60;
    if(k===per) hrsNow+=h; else if(k===prevPer) hrsPrev+=h;
  });
  // Порівнюємо лише зіставні відрізки місяця
  var dayOfMonth=now.getDate();
  var daysPrev=new Date(pd.getFullYear(), pd.getMonth()+1, 0).getDate();
  var prevAtSameDay=hrsPrev*(Math.min(dayOfMonth,daysPrev)/daysPrev);
  if(prevAtSameDay>=5){
    var diff=Math.round((hrsNow-prevAtSameDay)/prevAtSameDay*100);
    if(Math.abs(diff)>=15){
      out.push({
        lvl: diff>0?'good':'warn', ico: diff>0?'\ud83d\udcc8':'\ud83d\udcc9',
        title:(diff>0?'+':'')+diff+'% \u0433\u043e\u0434\u0438\u043d \u0434\u043e \u043c\u0438\u043d\u0443\u043b\u043e\u0433\u043e \u043c\u0456\u0441\u044f\u0446\u044f',
        text:Math.round(hrsNow)+'\u0433 \u043f\u0440\u043e\u0442\u0438 '+Math.round(prevAtSameDay)+'\u0433 \u043d\u0430 \u0446\u0435\u0439 \u0436\u0435 \u0434\u0435\u043d\u044c',
        hint:'',
        act:function(){ nav('reports'); }
      });
    }
  }

  // ── 6. Усе гаразд ──────────────────────────────────────────
  if(!out.length){
    out.push({ lvl:'good', ico:'\u2728',
      title:'\u0423\u0441\u0435 \u043f\u0456\u0434 \u043a\u043e\u043d\u0442\u0440\u043e\u043b\u0435\u043c',
      text:'\u041d\u0456\u0447\u043e\u0433\u043e, \u0449\u043e \u043f\u043e\u0442\u0440\u0435\u0431\u0443\u0454 \u0443\u0432\u0430\u0433\u0438',
      hint:'' });
  }

  return out.slice(0,6);
}
window.smartInsights=smartInsights;

function renderInsights(){
  var box=document.getElementById('insights-body');
  if(!box) return;
  var list=smartInsights();
  window._insights=list;
  box.innerHTML=list.map(function(x,i){
    return '<div class="ins ins-'+x.lvl+'"'+(x.act?' onclick="insAct('+i+')" style="cursor:pointer"':'')+'>'
      +'<span class="ins-ico">'+x.ico+'</span>'
      +'<span class="ins-txt">'
        +'<b>'+x.title+'</b>'
        +(x.text?'<span>'+x.text+'</span>':'')
        +(x.hint?'<i>'+x.hint+'</i>':'')
      +'</span>'
      +(x.act?'<span class="ins-go">\u2192</span>':'')
    +'</div>';
  }).join('');
}
window.renderInsights=renderInsights;

function insAct(i){
  var x=(window._insights||[])[i];
  if(x && x.act) x.act();
}
window.insAct=insAct;

/**
 * Філії, до яких належить заняття.
 * Репетитор може працювати в кількох корпусах, тому повертаємо масив.
 */
function lessonBranchIds(l){
  // 1) У самого заняття
  var own=l.branchId||l.branch_id;
  if(own) return [own];

  // 2) У репетитора — він фізично проводить заняття в корпусі
  var t=(S.tutors||[]).find(function(x){return x.id===(l.tutorId||l.tutor_id);});
  if(t){
    var tb=t.branchIds||t.branch_ids;
    if(tb){
      var arr=Array.isArray(tb)?tb:String(tb).split(',').filter(Boolean);
      if(arr.length) return arr;
    }
    if(t.branchId||t.branch_id) return [t.branchId||t.branch_id];
  }

  // 3) У учня — запасний варіант
  var st=(S.students||[]).find(function(x){return x.id===(l.studentId||l.student_id);});
  if(st){
    var sb=st.branchIds||st.branch_ids;
    if(sb){
      var arr2=Array.isArray(sb)?sb:String(sb).split(',').filter(Boolean);
      if(arr2.length) return arr2;
    }
    if(st.branchId||st.branch_id) return [st.branchId||st.branch_id];
  }
  return [];
}
window.lessonBranchIds=lessonBranchIds;

function pulseData(){
  var now=new Date();
  var today=localDateStr(now);
  var nowMin=now.getHours()*60+now.getMinutes();

  var branches=(S.branches||[]).slice();
  if(!branches.length) branches=[{id:null,name:'\u0426\u0435\u043d\u0442\u0440'}];

  return branches.map(function(b){
    // Заняття цієї філії сьогодні.
    // Пріоритет визначення філії:
    //   1) філія самого заняття, якщо вказана
    //   2) філія РЕПЕТИТОРА — саме він фізично працює в корпусі
    //   3) філія учня — як запасний варіант
    var lessons=(S.lessons||[]).filter(function(l){
      if(l.date!==today || l.status==='cancelled') return false;
      if(!b.id) return true;
      return lessonBranchIds(l).indexOf(b.id)>=0;
    });

    var live=[], soon=[], done=0;
    lessons.forEach(function(l){
      var tp=String(l.time||'00:00').split(':');
      var start=(+tp[0])*60+(+tp[1]||0);
      var end=start+(parseFloat(l.dur)||60);
      if(nowMin>=start && nowMin<end) live.push(l);
      else if(start>nowMin && start-nowMin<=90) soon.push(l);
      else if(nowMin>=end) done++;
    });

    // Хто зараз викладає
    var tutors={};
    live.forEach(function(l){ var t=l.tutorId||l.tutor_id; if(t) tutors[t]=1; });

    return {
      id:b.id, name:b.name||'\u0424\u0456\u043b\u0456\u044f',
      total:lessons.length, live:live, soon:soon, done:done,
      tutorIds:Object.keys(tutors),
      liveList:live.map(function(l){
        var st=(S.students||[]).find(function(x){return x.id===(l.studentId||l.student_id);});
        var tp=String(l.time||'').split(':');
        var start=(+tp[0])*60+(+tp[1]||0);
        var left=Math.max(0, Math.round(start+(parseFloat(l.dur)||60)-nowMin));
        return {
          student: st?(st.fn+' '+st.ln):'\u2014',
          tutor: tn(l.tutorId||l.tutor_id)||'\u2014',
          tutorId: l.tutorId||l.tutor_id,
          subject: l.subject||'',
          time: l.time||'', left: left
        };
      }),
      soonList:soon.slice(0,3).map(function(l){
        var st=(S.students||[]).find(function(x){return x.id===(l.studentId||l.student_id);});
        var tp=String(l.time||'').split(':');
        var start=(+tp[0])*60+(+tp[1]||0);
        return {
          student: st?(st.fn+' '+st.ln):'\u2014',
          time: l.time||'', inMin: Math.max(0, start-nowMin),
          tutorId: l.tutorId||l.tutor_id
        };
      })
    };
  });
}
window.pulseData=pulseData;

/** Детальне вікно філії — увесь день, а не лише поточна хвилина */
function openBranchDetail(bid){
  var data=pulseData();
  var b=data.find(function(x){ return String(x.id)===String(bid); }) || data[0];
  if(!b) return;

  var now=new Date();
  var today=localDateStr(now);
  var nowMin=now.getHours()*60+now.getMinutes();

  // Усі заняття філії сьогодні, за часом
  var all=(S.lessons||[]).filter(function(l){
    if(l.date!==today || l.status==='cancelled') return false;
    if(!b.id) return true;
    return lessonBranchIds(l).indexOf(b.id)>=0;
  }).sort(function(x,y){ return String(x.time||'').localeCompare(String(y.time||'')); });

  // Репетитори, що працюють у філії сьогодні
  var tutMap={};
  all.forEach(function(l){
    var t=l.tutorId||l.tutor_id; if(!t) return;
    if(!tutMap[t]) tutMap[t]={id:t, total:0, hrs:0, live:false};
    tutMap[t].total++;
    tutMap[t].hrs+=(parseFloat(l.dur)||60)/60;
    var tp=String(l.time||'00:00').split(':');
    var st2=(+tp[0])*60+(+tp[1]||0);
    if(nowMin>=st2 && nowMin<st2+(parseFloat(l.dur)||60)) tutMap[t].live=true;
  });
  var tutors=Object.keys(tutMap).map(function(k){return tutMap[k];})
    .sort(function(a,c){ return c.hrs-a.hrs; });

  var rows=all.map(function(l){
    var st=(S.students||[]).find(function(x){return x.id===(l.studentId||l.student_id);});
    var tp=String(l.time||'00:00').split(':');
    var start=(+tp[0])*60+(+tp[1]||0);
    var end=start+(parseFloat(l.dur)||60);
    var state = nowMin>=start&&nowMin<end ? 'live'
              : nowMin>=end ? 'past' : 'future';
    var left = state==='live' ? Math.round(end-nowMin) : 0;
    return '<tr class="bd-'+state+'">'
      +'<td class="bd-time">'+(l.time||'')+'</td>'
      +'<td>'+(st?st.fn+' '+st.ln:'\u2014')+'</td>'
      +'<td>'+tutorDot(l.tutorId||l.tutor_id,7)+' '+(tn(l.tutorId||l.tutor_id)||'\u2014')+'</td>'
      +'<td style="color:var(--t2)">'+(l.subject||'\u2014')+'</td>'
      +'<td class="c">'+(l.dur||60)+'\u0445\u0432</td>'
      +'<td class="c">'+(state==='live'
          ? '<span class="bd-badge live">\u0439\u0434\u0435 \u00b7 '+left+'\u0445\u0432</span>'
          : state==='past' ? bst(l.status)
          : '<span class="bd-badge soon">\u0441\u043a\u043e\u0440\u043e</span>')+'</td>'
    +'</tr>';
  }).join('');

  var el=document.getElementById('mo-branch');
  if(!el){
    el=document.createElement('div');
    el.className='mo'; el.id='mo-branch';
    document.body.appendChild(el);
  }
  el.innerHTML='<div class="mdl" style="max-width:860px">'
    +'<div class="mdlh">'
      +'<div class="mdlt"><span class="pb-dot'+(b.live.length?' on':'')+'" style="margin-right:8px"></span>'
        +b.name+'</div>'
      +'<button class="xbtn" onclick="closeM(\'mo-branch\')">\u00d7</button></div>'
    +'<div class="mdlb" style="max-height:74vh;overflow:auto">'
      // Зведення
      +'<div class="bd-stats">'
        +'<div class="bd-stat"><b>'+b.live.length+'</b><span>\u0439\u0434\u0435 \u0437\u0430\u0440\u0430\u0437</span></div>'
        +'<div class="bd-stat"><b>'+b.soon.length+'</b><span>\u043d\u0430\u0439\u0431\u043b\u0438\u0436\u0447\u0456 1.5\u0433</span></div>'
        +'<div class="bd-stat"><b>'+b.done+'</b><span>\u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u043e</span></div>'
        +'<div class="bd-stat"><b>'+all.length+'</b><span>\u0443\u0441\u044c\u043e\u0433\u043e \u0441\u044c\u043e\u0433\u043e\u0434\u043d\u0456</span></div>'
      +'</div>'
      // Репетитори
      +(tutors.length
        ? '<div class="bd-sect">\u0420\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0438 \u0441\u044c\u043e\u0433\u043e\u0434\u043d\u0456</div>'
          +'<div class="bd-tutors">'+tutors.map(function(t){
            return '<div class="bd-tut'+(t.live?' live':'')+'">'
              +tutorDot(t.id,9)
              +'<span><b>'+(tn(t.id)||'\u2014')+'</b>'
              +'<span>'+t.total+' \u0437\u0430\u043d. \u00b7 '+(Math.round(t.hrs*10)/10)+'\u0433</span></span>'
              +(t.live?'<span class="bd-now">\u0437\u0430\u0440\u0430\u0437</span>':'')
            +'</div>';
          }).join('')+'</div>'
        : '')
      // Розклад дня
      +'<div class="bd-sect">\u0420\u043e\u0437\u043a\u043b\u0430\u0434 \u0434\u043d\u044f</div>'
      +(all.length
        ? '<div style="overflow-x:auto"><table class="bd-table"><thead><tr>'
          +'<th>\u0427\u0430\u0441</th><th>\u0423\u0447\u0435\u043d\u044c</th><th>\u0420\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440</th>'
          +'<th>\u041f\u0440\u0435\u0434\u043c\u0435\u0442</th><th>\u0422\u0440\u0438\u0432.</th><th>\u0421\u0442\u0430\u043d</th>'
          +'</tr></thead><tbody>'+rows+'</tbody></table></div>'
        : '<div class="empty"><div class="ei">\ud83d\udcc5</div>'
          +'<b>\u0421\u044c\u043e\u0433\u043e\u0434\u043d\u0456 \u0437\u0430\u043d\u044f\u0442\u044c \u043d\u0435\u043c\u0430\u0454</b></div>')
    +'</div></div>';
  openM('mo-branch');
}
window.openBranchDetail=openBranchDetail;

function renderPulse(){
  var wrap=document.getElementById('pulse-body');
  if(!wrap) return;
  var data=pulseData();
  var totalLive=data.reduce(function(a,b){ return a+b.live.length; },0);

  var hdr=document.getElementById('pulse-live-count');
  if(hdr) hdr.textContent=totalLive;

  wrap.innerHTML=data.map(function(b){
    var isLive=b.live.length>0;
    return '<div class="pulse-branch'+(isLive?' live':'')+'" '
      +'onclick="openBranchDetail(\''+(b.id||'')+'\')" title="\u041f\u043e\u0434\u0440\u043e\u0431\u0438\u0446\u0456">'
      +'<div class="pb-head">'
        +'<span class="pb-dot'+(isLive?' on':'')+'"></span>'
        +'<b>'+b.name+'</b>'
        +'<span class="pb-stat">'+(isLive
            ? b.live.length+' \u0437\u0430\u0440\u0430\u0437 \u0456\u0434\u0435'
            : '\u0442\u0438\u0448\u0430')+'</span>'
      +'</div>'
      // Заняття, що йдуть просто зараз
      +(b.liveList.length
        ? '<div class="pb-list">'+b.liveList.map(function(x){
            return '<div class="pb-item">'
              +tutorDot(x.tutorId,7)
              +'<span class="pb-who"><b>'+x.student+'</b><span>'+x.tutor+(x.subject?' \u00b7 '+x.subject:'')+'</span></span>'
              +'<span class="pb-left">'+x.left+'\u0445\u0432</span>'
            +'</div>';
          }).join('')+'</div>'
        : '')
      // Найближчі
      +(b.soonList.length
        ? '<div class="pb-soon">\u0421\u043a\u043e\u0440\u043e: '+b.soonList.map(function(x){
            return x.time+' '+x.student+' <i>(\u0447\u0435\u0440\u0435\u0437 '+x.inMin+'\u0445\u0432)</i>';
          }).join(' \u00b7 ')+'</div>'
        : '')
      +'<div class="pb-foot">\u0421\u044c\u043e\u0433\u043e\u0434\u043d\u0456: '+b.total
        +' \u00b7 \u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u043e '+b.done+'</div>'
    +'</div>';
  }).join('');
}
window.renderPulse=renderPulse;

var _pulseTimer=null;
function startPulse(){
  if(_pulseTimer) return;
  renderPulse();
  // Оновлюємо щохвилини — цього досить, і не навантажує
  _pulseTimer=setInterval(function(){
    if(S.currentPage==='dashboard') renderPulse();
  }, 60000);
}
window.startPulse=startPulse;

function wrappedData(period){
  period = period || new Date().toISOString().slice(0,7);
  var L = filterByBranch((S.lessons||[]).filter(function(l){
    return String(l.date||'').slice(0,7)===period;
  }));
  var done = L.filter(function(l){ return isDoneLesson(l)||l.status==='makeup'; });
  var hrs = Math.round(done.reduce(function(a,l){ return a+(parseFloat(l.dur)||60)/60; },0)*10)/10;

  // Найактивніший репетитор
  var byTut={};
  done.forEach(function(l){
    var t=l.tutorId||l.tutor_id; if(!t) return;
    byTut[t]=(byTut[t]||0)+(parseFloat(l.dur)||60)/60;
  });
  var topTut=Object.keys(byTut).sort(function(a,b){return byTut[b]-byTut[a];})[0];

  // Найпопулярніший предмет (з урахуванням регістру)
  var bySubj={}, subjLbl={};
  done.forEach(function(l){
    var raw=String(l.subject||'\u0406\u043d\u0448\u0435').trim();
    var k=raw.toLowerCase().replace(/\s+/g,' ');
    if(!subjLbl[k]) subjLbl[k]=raw;
    bySubj[k]=(bySubj[k]||0)+1;
  });
  var topSubj=Object.keys(bySubj).sort(function(a,b){return bySubj[b]-bySubj[a];})[0];

  // Найстаранніший учень
  var byStud={};
  done.forEach(function(l){
    var sid=l.studentId||l.student_id; if(!sid) return;
    byStud[sid]=(byStud[sid]||0)+1;
  });
  var topStud=Object.keys(byStud).sort(function(a,b){return byStud[b]-byStud[a];})[0];

  // Найзавантаженіший день тижня
  var byDow=[0,0,0,0,0,0,0];
  done.forEach(function(l){
    var d=new Date(l.date+'T12:00:00');
    if(!isNaN(d)) byDow[d.getDay()===0?6:d.getDay()-1]++;
  });
  var dowNames=['\u041f\u043e\u043d\u0435\u0434\u0456\u043b\u043e\u043a','\u0412\u0456\u0432\u0442\u043e\u0440\u043e\u043a','\u0421\u0435\u0440\u0435\u0434\u0430','\u0427\u0435\u0442\u0432\u0435\u0440','\u041f\u2019\u044f\u0442\u043d\u0438\u0446\u044f','\u0421\u0443\u0431\u043e\u0442\u0430','\u041d\u0435\u0434\u0456\u043b\u044f'];
  var topDow=byDow.indexOf(Math.max.apply(null,byDow));

  var income=done.reduce(function(a,l){ return a+(lessonTotal(l)||0); },0);
  var studCount=Object.keys(byStud).length;

  return {
    period:period, lessons:done.length, hours:hrs, income:Math.round(income),
    students:studCount,
    topTutor: topTut ? {name:tn(topTut), hours:Math.round(byTut[topTut]*10)/10, id:topTut} : null,
    topSubject: topSubj ? {name:subjLbl[topSubj], count:bySubj[topSubj]} : null,
    topStudent: topStud ? {name:sn(topStud), count:byStud[topStud]} : null,
    topDow: byDow[topDow] ? {name:dowNames[topDow], count:byDow[topDow]} : null
  };
}
window.wrappedData=wrappedData;

function openWrapped(period){
  var d=wrappedData(period);
  if(!d.lessons){ mkToast('\u0417\u0430 \u0446\u0435\u0439 \u043c\u0456\u0441\u044f\u0446\u044c \u0449\u0435 \u043d\u0435\u043c\u0430\u0454 \u0434\u0430\u043d\u0438\u0445','error'); return; }

  var mName=prMonthName(d.period);
  _wrSlides=[
    {bg:'linear-gradient(135deg,#6366f1,#8b5cf6)', ico:'\ud83d\udcc5',
     big:mName.split(' ')[0], sub:'\u041f\u0456\u0434\u0441\u0443\u043c\u043a\u0438 \u043c\u0456\u0441\u044f\u0446\u044f',
     note:'\u041f\u043e\u0433\u043b\u044f\u043d\u044c\u043c\u043e, \u044f\u043a \u0432\u0456\u043d \u043c\u0438\u043d\u0443\u0432'},
    {bg:'linear-gradient(135deg,#22c55e,#14b8a6)', ico:'\u2705',
     big:d.lessons, sub:'\u0437\u0430\u043d\u044f\u0442\u044c \u043f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u043e',
     note:d.hours+' \u0433\u043e\u0434\u0438\u043d \u043d\u0430\u0432\u0447\u0430\u043d\u043d\u044f'},
    {bg:'linear-gradient(135deg,#f59e0b,#f97316)', ico:'\ud83d\udc65',
     big:d.students, sub:'\u0443\u0447\u043d\u0456\u0432 \u043d\u0430\u0432\u0447\u0430\u043b\u043e\u0441\u044c',
     note:d.topStudent ? '\u041d\u0430\u0439\u0441\u0442\u0430\u0440\u0430\u043d\u043d\u0456\u0448\u0438\u0439: '+d.topStudent.name+' ('+d.topStudent.count+')' : ''}
  ];
  if(d.topTutor) _wrSlides.push({bg:'linear-gradient(135deg,#ec4899,#f43f5e)', ico:'\ud83c\udfc6',
     big:d.topTutor.name, sub:'\u043d\u0430\u0439\u0430\u043a\u0442\u0438\u0432\u043d\u0456\u0448\u0438\u0439 \u0440\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440',
     note:d.topTutor.hours+' \u0433\u043e\u0434\u0438\u043d \u0437\u0430 \u043c\u0456\u0441\u044f\u0446\u044c', small:true});
  if(d.topSubject) _wrSlides.push({bg:'linear-gradient(135deg,#0ea5e9,#06b6d4)', ico:'\ud83d\udcda',
     big:d.topSubject.name, sub:'\u043d\u0430\u0439\u043f\u043e\u043f\u0443\u043b\u044f\u0440\u043d\u0456\u0448\u0438\u0439 \u043f\u0440\u0435\u0434\u043c\u0435\u0442',
     note:d.topSubject.count+' \u0437\u0430\u043d\u044f\u0442\u044c', small:true});
  if(d.topDow) _wrSlides.push({bg:'linear-gradient(135deg,#8b5cf6,#6366f1)', ico:'\u26a1',
     big:d.topDow.name, sub:'\u043d\u0430\u0439\u0437\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0435\u043d\u0456\u0448\u0438\u0439 \u0434\u0435\u043d\u044c',
     note:d.topDow.count+' \u0437\u0430\u043d\u044f\u0442\u044c', small:true});
  if(R()==='god'||R()==='director') _wrSlides.push({bg:'linear-gradient(135deg,#22c55e,#84cc16)', ico:'\ud83d\udcb0',
     big:d.income.toLocaleString('uk-UA')+'\u20b4', sub:'\u0434\u043e\u0445\u0456\u0434 \u0437\u0430 \u043c\u0456\u0441\u044f\u0446\u044c', note:'', small:true});
  _wrSlides.push({bg:'linear-gradient(135deg,#6366f1,#ec4899)', ico:'\ud83c\udf89',
     big:'\u0414\u044f\u043a\u0443\u0454\u043c\u043e!', sub:'\u0417\u0430 \u0447\u0443\u0434\u043e\u0432\u0438\u0439 \u043c\u0456\u0441\u044f\u0446\u044c \u0440\u043e\u0431\u043e\u0442\u0438',
     note:'', small:true, last:true});

  _wrIdx=0;
  var el=document.getElementById('wrapped');
  if(el){ el.classList.add('open'); wrRender(); }
}
window.openWrapped=openWrapped;

function wrRender(){
  var box=document.getElementById('wr-slide');
  var dots=document.getElementById('wr-dots');
  if(!box) return;
  var sl=_wrSlides[_wrIdx];
  if(!sl) return;
  box.style.background=sl.bg;
  box.innerHTML='<div class="wr-inner">'
    +'<div class="wr-ico">'+sl.ico+'</div>'
    +'<div class="wr-big'+(sl.small?' sm':'')+'">'+sl.big+'</div>'
    +'<div class="wr-sub">'+sl.sub+'</div>'
    +(sl.note?'<div class="wr-note">'+sl.note+'</div>':'')
    +(sl.last?'<button class="wr-close-btn" onclick="wrClose()">\u0417\u0430\u043a\u0440\u0438\u0442\u0438</button>':'')
  +'</div>';
  // Перезапускаємо анімацію появи
  box.classList.remove('anim'); void box.offsetWidth; box.classList.add('anim');
  if(dots) dots.innerHTML=_wrSlides.map(function(_,i){
    return '<span class="wr-dot'+(i===_wrIdx?' on':'')+'" onclick="wrGo('+i+')"></span>';
  }).join('');
  if(sl.last){ try{ confetti({count:80}); }catch(e){} }
}

function wrGo(i){ _wrIdx=Math.max(0,Math.min(i,_wrSlides.length-1)); wrRender(); }
function wrNext(){ if(_wrIdx<_wrSlides.length-1){ _wrIdx++; wrRender(); } else wrClose(); }
function wrPrev(){ if(_wrIdx>0){ _wrIdx--; wrRender(); } }
function wrClose(){ var el=document.getElementById('wrapped'); if(el) el.classList.remove('open'); }
window.wrGo=wrGo; window.wrNext=wrNext; window.wrPrev=wrPrev; window.wrClose=wrClose;
