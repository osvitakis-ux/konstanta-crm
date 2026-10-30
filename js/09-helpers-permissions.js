
function R(){return CU?.role||'tutor';}

function P(){return ROLES[R()];}

function userPerms(){
  // Returns merged permissions: role defaults + user-level overrides
  if(!CU)return {};
  var up=CU.perms||{};
  var rp=P().can||{};
  return Object.assign({},rp,up.can||{});
}

function userNav(){
  // Returns nav pages: role defaults + user-level nav overrides
  if(!CU)return [];
  var up=CU.perms||{};
  var roleNav=ROLES[R()].nav||[];
  // User can have pages removed (hide:[]) or added (show:[])
  var hide=up.hideNav||[];
  var show=up.showNav||[];
  var nav=roleNav.filter(function(p){return !hide.includes(p);});
  show.forEach(function(p){if(!nav.includes(p))nav.push(p);});
  return nav;
}

function can(k){
  return userPerms()[k]||false;
}

function isSuperAdmin(){
  return R()==='god' || R()==='network_admin' || R()==='director';
}

function currentBranch(){
  return S.currentBranchId || null;
}

function branchName(id){
  const b=(S.branches||[]).find(x=>x.id===id);
  return b?b.name:'\u2014';
}

function filterByBranch(arr){
  if(!arr) return [];
  if(R()==='tutor') return arr;
  const bid=currentBranch();
  if(!bid&&isSuperAdmin()) return arr;
  if(bid){
    // Хтось свідомо обрав конкретну філію в перемикачі — фільтруємо саме під неї
    return arr.filter(function(x){
      if(Array.isArray(x.branchIds) && x.branchIds.length) return x.branchIds.indexOf(bid)>=0;
      return !x.branchId||x.branchId===bid;
    });
  }
  // Немає обраної конкретної філії: показуємо дані З УСІХ філій, до яких прив'язаний
  // САМ користувач (адмін може відповідати за кілька філій одночасно, як і репетитор/учень).
  var myBranches=(Array.isArray(CU&&CU.branchIds)&&CU.branchIds.length)?CU.branchIds:((CU&&CU.branchId)?[CU.branchId]:null);
  if(!myBranches||!myBranches.length) return arr; // адміну не призначено жодної філії — не обмежуємо (безпечний дефолт)
  return arr.filter(function(x){
    if(Array.isArray(x.branchIds) && x.branchIds.length) return x.branchIds.some(function(b){return myBranches.indexOf(b)>=0;});
    return !x.branchId||myBranches.indexOf(x.branchId)>=0;
  });
}
function myBranchId(){
  // Для суперадмінів — поточно обрана в перемикачі філія (чи все, якщо не обрано).
  // Для звичайного адміна — ПЕРША з його власних призначених філій (використовується
  // лише як дефолт при СТВОРЕННІ нового запису, не для показу/фільтрації списків —
  // для цього тепер filterByBranch() враховує ВСІ філії адміна одразу).
  // РАНІШЕ тут був небезпечний фолбек на S.branches[0] — перша-ліпша філія в базі,
  // що прив'язувало будь-якого адміна без явного branchId до випадкової філії.
  if(isSuperAdmin()) return currentBranch();
  if(Array.isArray(CU&&CU.branchIds)&&CU.branchIds.length) return CU.branchIds[0];
  return (CU&&CU.branchId)||null;
}

// ═══════════════════════════════════════════════════════════════
//  КОЛІР РЕПЕТИТОРА — однаковий у всій системі
//  Дає змогу впізнавати, чиє це заняття, одним поглядом на розклад.
//  Колір стабільний: рахується з id, тож не змінюється між сеансами.
// ═══════════════════════════════════════════════════════════════
var TUTOR_COLORS = ['#6366f1','#22c55e','#f59e0b','#ec4899','#14b8a6',
                    '#8b5cf6','#f97316','#06b6d4','#84cc16','#e11d48',
                    '#0ea5e9','#a855f7'];

function tutorColor(tutorId){
  if(!tutorId) return 'var(--b2)';
  var t=(S.tutors||[]).find(function(x){return x.id===tutorId;});
  if(t && t.color) return t.color;      // власний колір із картки, якщо заданий
  var str=String(tutorId), h=0;
  for(var i=0;i<str.length;i++){ h=(h*31 + str.charCodeAt(i)) >>> 0; }
  return TUTOR_COLORS[h % TUTOR_COLORS.length];
}
window.tutorColor = tutorColor;

/** Кольорова крапка репетитора — для списків і легенд */
/** Легенда кольорів репетиторів над розкладом */
function renderSchLegend(){
  // Легенду прибрано за запитом — просто ховаємо елемент, якщо він є
  var el=document.getElementById('sch-legend');
  if(el){ el.style.display='none'; el.innerHTML=''; }
}
window.renderSchLegend=renderSchLegend;

function tutorDot(tutorId, size){
  size = size||8;
  return '<span style="display:inline-block;width:'+size+'px;height:'+size+'px;'
    +'border-radius:50%;background:'+tutorColor(tutorId)+';flex-shrink:0"></span>';
}
window.tutorDot = tutorDot;

/**
 * Скелетон завантаження — показує форму майбутнього вмісту.
 * Краще за спінер: сторінка не «стрибає», коли дані приходять,
 * і очікування суб'єктивно коротше.
 * kind: 'rows' | 'cards'
 */
/**
 * Sparkline — крихітний графік тренду прямо в картці KPI.
 * Показує динаміку за останні 6 місяців: одразу видно, росте
 * показник чи падає, без переходу в аналітику.
 */
// ═══════════════════════════════════════════════════════════════
//  ПЕРЕНЕСЕННЯ ЗАНЯТТЯ МИШЕЮ В РОЗКЛАДІ
//  Перетягнув на іншу клітинку — дата й час змінились.
//  Швидше, ніж відкривати картку й міняти два поля.
// ═══════════════════════════════════════════════════════════════
var _schDragId = null;

function schDragStart(e, lessonId){
  var _dl=(S.lessons||[]).find(function(x){return x.id===lessonId;});
  if(_dl && isPlannedLesson(_dl) && !canMoveLessons()){ try{ e.preventDefault(); }catch(_){}; return; } // тутор не переносить ПЛАНОВІ
  _schDragId = lessonId;
  try{
    e.dataTransfer.effectAllowed='move';
    e.dataTransfer.setData('text/plain', lessonId);
  }catch(err){}
  if(e.target && e.target.classList) e.target.classList.add('sch-dragging');
}
window.schDragStart = schDragStart;

function schDragEnd(e){
  _schDragId = null;
  if(e.target && e.target.classList) e.target.classList.remove('sch-dragging');
  document.querySelectorAll('.sch-drop-hover').forEach(function(el){
    el.classList.remove('sch-drop-hover');
  });
}
window.schDragEnd = schDragEnd;

function schDragOver(e){
  if(!_schDragId) return;
  e.preventDefault();
  try{ e.dataTransfer.dropEffect='move'; }catch(err){}
  if(e.currentTarget && e.currentTarget.classList) e.currentTarget.classList.add('sch-drop-hover');
}
window.schDragOver = schDragOver;

function schDragLeave(e){
  if(e.currentTarget && e.currentTarget.classList) e.currentTarget.classList.remove('sch-drop-hover');
}
window.schDragLeave = schDragLeave;

async function schDrop(e, newDate, newTime){
  e.preventDefault();
  e.stopPropagation();
  if(e.currentTarget && e.currentTarget.classList) e.currentTarget.classList.remove('sch-drop-hover');

  var id = _schDragId || (e.dataTransfer && e.dataTransfer.getData('text/plain'));
  _schDragId = null;
  if(!id) return;
  if(!can('lessons')){ mkToast('\u041d\u0435\u043c\u0430\u0454 \u043f\u0440\u0430\u0432','error'); return; }

  var l=(S.lessons||[]).find(function(x){return x.id===id;});
  if(!l) return;
  if(isPlannedLesson(l) && !canMoveLessons()){ mkToast('\u041f\u0435\u0440\u0435\u043d\u043e\u0441\u0438\u0442\u0438 \u043f\u043b\u0430\u043d\u043e\u0432\u0456 \u0437\u0430\u043d\u044f\u0442\u0442\u044f \u043c\u043e\u0436\u0435 \u043b\u0438\u0448\u0435 \u0430\u0434\u043c\u0456\u043d\u0456\u0441\u0442\u0440\u0430\u0442\u043e\u0440 \u0430\u0431\u043e \u0434\u0438\u0440\u0435\u043a\u0442\u043e\u0440','error'); return; }
  if(lessonLocked(id)){ mkToast('\u0417\u0430\u043d\u044f\u0442\u0442\u044f \u0437\u0430\u043c\u043e\u0440\u043e\u0436\u0435\u043d\u0435 \u2014 \u0440\u0435\u0434\u0430\u0433\u0443\u0454 \u0430\u0434\u043c\u0456\u043d\u0456\u0441\u0442\u0440\u0430\u0442\u043e\u0440 \u0430\u0431\u043e \u0434\u0438\u0440\u0435\u043a\u0442\u043e\u0440','error'); return; }
  // Нічого не змінилось — не смикаємо базу
  if(l.date===newDate && String(l.time||'').slice(0,5)===newTime) return;

  var st=(S.students||[]).find(function(x){return x.id===(l.studentId||l.student_id);});
  var dp=String(newDate).split('-');
  var dLbl=dp.length===3 ? dp[2]+'.'+dp[1] : newDate;
  if(!confirm('\u041f\u0435\u0440\u0435\u043d\u0435\u0441\u0442\u0438 \u0437\u0430\u043d\u044f\u0442\u0442\u044f'
    +(st?' \u2014 '+st.fn+' '+st.ln:'')+'\n\n'
    +'\u0417 '+String(l.date||'').split('-').reverse().slice(0,2).join('.')+' '+(l.time||'')
    +'\n\u041d\u0430 '+dLbl+' '+newTime+'?')) return;

  try{
    await dbUpdate('lessons', id, { date:newDate, time:newTime });
    mkToast('\u2705 \u041f\u0435\u0440\u0435\u043d\u0435\u0441\u0435\u043d\u043e \u043d\u0430 '+dLbl+' '+newTime);
    renderSch();
  }catch(err){
    mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(err.message||err),'error');
  }
}
window.schDrop = schDrop;

function sparkline(values, color, w, hgt){
  values = (values||[]).map(function(v){ return parseFloat(v)||0; });
  if(values.length<2) return '';
  w = w||70; hgt = hgt||22;
  var max=Math.max.apply(null, values), min=Math.min.apply(null, values);
  var range = (max-min)||1;
  var step = w/(values.length-1);
  var pts = values.map(function(v,i){
    var x=(i*step).toFixed(1);
    var y=(hgt - ((v-min)/range)*(hgt-4) - 2).toFixed(1);
    return x+','+y;
  });
  var last=values[values.length-1], prev=values[values.length-2];
  var trendUp = last>=prev;
  color = color || (trendUp ? 'var(--tut)' : 'var(--danger)');
  // Заливка під лінією — робить тренд помітнішим при швидкому погляді
  var area = 'M0,'+hgt+' L'+pts.join(' L')+' L'+w+','+hgt+' Z';
  var uid = 'spk'+Math.random().toString(36).slice(2,8);
  return '<svg class="spark" width="'+w+'" height="'+hgt+'" viewBox="0 0 '+w+' '+hgt+'" '
    +'style="overflow:visible" aria-hidden="true">'
    +'<defs><linearGradient id="'+uid+'" x1="0" y1="0" x2="0" y2="1">'
      +'<stop offset="0%" stop-color="'+color+'" stop-opacity=".28"/>'
      +'<stop offset="100%" stop-color="'+color+'" stop-opacity="0"/>'
    +'</linearGradient></defs>'
    +'<path d="'+area+'" fill="url(#'+uid+')"/>'
    +'<polyline points="'+pts.join(' ')+'" fill="none" stroke="'+color+'" '
      +'stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>'
    +'<circle cx="'+(w).toFixed(1)+'" cy="'+pts[pts.length-1].split(',')[1]+'" r="2.4" fill="'+color+'"/>'
  +'</svg>';
}
window.sparkline = sparkline;

/** Значення показника за останні N місяців — для sparkline */
function monthlySeries(fn, months){
  months = months||6;
  var out=[], now=new Date();
  for(var i=months-1;i>=0;i--){
    var d=new Date(now.getFullYear(), now.getMonth()-i, 1);
    var key=d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0');
    out.push(fn(key)||0);
  }
  return out;
}
window.monthlySeries = monthlySeries;

function skeleton(kind, count){
  count = count || 5;
  var out='';
  if(kind==='cards'){
    for(var c=0;c<count;c++) out+='<div class="skel skel-card"></div>';
    return '<div style="padding:4px">'+out+'</div>';
  }
  for(var i=0;i<count;i++){
    // Різна ширина рядків — виглядає природніше за однакові смуги
    var w1=55+((i*17)%25), w2=25+((i*11)%20);
    out+='<div class="skel-row">'
      +'<div class="skel skel-av"></div>'
      +'<div style="flex:1;display:flex;flex-direction:column;gap:6px">'
        +'<div class="skel skel-line" style="width:'+w1+'%"></div>'
        +'<div class="skel skel-line" style="width:'+w2+'%;height:9px;opacity:.65"></div>'
      +'</div>'
      +'<div class="skel skel-line" style="width:64px"></div>'
    +'</div>';
  }
  return '<div style="padding:4px 0">'+out+'</div>';
}
window.skeleton = skeleton;

function mkAv(fn,ln,sz,photo){
  sz=sz||30;
  if(photo) return '<div class="av" style="width:'+sz+'px;height:'+sz+'px;overflow:hidden;flex-shrink:0"><img src="'+photo+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%"></div>';
  const cs=['#6c8fff','#a78bfa','#34d399','#f59e0b','#f87171','#0ea5e9','#ec4899','#ff6b35'];
  const i=((fn||'A').charCodeAt(0)+((ln||'B').charCodeAt(0)))%cs.length;
  return '<div class="av" style="background:'+cs[i]+';width:'+sz+'px;height:'+sz+'px;font-size:'+(sz*.38)+'px;color:#fff">'+((fn||'?')[0])+((ln||'')[0]||'')+'</div>';
}

function bst(s){
  var m={active:'bg',trial:'bb',paused:'by',completed:'br',inactive:'bn',request:'bb',planned:'bb',done:'bg',testing:'bp',burned:'bo',cancelled:'br',missed:'br',makeup:'by',makeup_planned:'bn',paid:'bg',pending:'by',overdue:'br'};
  var l={active:'Активний',trial:'Пробне',paused:'Призупин.',completed:'Завершив',inactive:'Неактивний',request:'Запит',planned:'Планов.',done:'Проведено',testing:'Тестування',burned:'\uD83D\uDD25 Згоріле',cancelled:'Скасов.',missed:'Пропущено',makeup:'Відпрацьовано',makeup_planned:'План. відпрац.',paid:'Оплачено',pending:'Очікується',overdue:'Прострочено'};
  return '<span class="badge '+(m[s]||'bb')+'">'+( l[s]||s)+'</span>';
}

function fd(d){if(!d)return '\u2014';return new Date(d).toLocaleDateString('uk-UA',{day:'2-digit',month:'2-digit',year:'numeric'});}

function fd2(l,p){document.getElementById('lu').value=l;document.getElementById('lp').value=p;}

function sn(id){const s=S.students.find(x=>x.id===id);return s?s.fn+' '+s.ln:'\u2014';}
// Прізвище та ініціал: "Мілентьєва В."
function snShort(id){const s=S.students.find(x=>x.id===id);return s?((s.ln||'')+(s.fn?' '+s.fn[0]+'.':'')).trim()||'\u2014':'\u2014';}
// Глобальний форматер грошей (renderPayroll має свій локальний — цей для решти місць)
function money(n){ return (Math.round((+n||0)*100)/100).toLocaleString('uk-UA'); }
// Розкладання уроків дня по вертикальних смугах: уроки, що перетинаються в часі,
// діляться по ширині клітинки. Повертає Map(lessonId -> {lane, count}).
function schAssignLanes(dayLessons){
  var evs=dayLessons.map(function(l){
    var p=(l.time||'08:00').split(':');
    var st=(parseInt(p[0])||8)*60+(parseInt(p[1])||0);
    return {l:l, s:st, e:st+(parseFloat(l.dur)||60)};
  }).sort(function(a,b){return a.s-b.s||a.e-b.e;});
  var out=new Map();
  var cluster=[], clusterEnd=-1;
  function flush(){
    if(!cluster.length) return;
    var lanes=[];
    cluster.forEach(function(ev){
      var lane=-1;
      for(var i=0;i<lanes.length;i++){ if(lanes[i]<=ev.s){ lane=i; break; } }
      if(lane<0){ lane=lanes.length; lanes.push(0); }
      lanes[lane]=ev.e;
      ev.lane=lane;
    });
    cluster.forEach(function(ev){ out.set(ev.l.id,{lane:ev.lane,count:lanes.length}); });
    cluster=[];
  }
  evs.forEach(function(ev){
    if(cluster.length && ev.s>=clusterEnd){ flush(); clusterEnd=-1; }
    cluster.push(ev);
    clusterEnd=Math.max(clusterEnd,ev.e);
  });
  flush();
  return out;
}

function tn(id){const t=S.tutors.find(x=>x.id===id);return t?t.fn+' '+t.ln:'\u2014';}
// Репетитори, доступні для НОВИХ призначень (не заархівовані). Заархівовані
// лишаються в S.tutors, щоб їхні минулі заняття й далі показували ім'я.
function activeTutors(){ return (S.tutors||[]).filter(function(t){return !t.archived;}); }
function tutorArchived(id){ var t=(S.tutors||[]).find(function(x){return x.id===id;}); return !!(t&&t.archived); }
async function archiveTutor(id){
  if(!confirm('Заархівувати репетитора?\n\nЙого минулі заняття збережуться та ЗАМОРОЗЯТЬСЯ (їх не можна буде змінювати), а сам він зникне зі списків для нових занять і призначень. Учнів після цього передайте іншим репетиторам у їхніх картках.')) return;
  try{ await dbUpdate('tutors', id, {archived:true}); mkToast('\u0420\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0430 \u0437\u0430\u0430\u0440\u0445\u0456\u0432\u043e\u0432\u0430\u043d\u043e'); renderTutors(); }
  catch(e){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+e.message,'error'); }
}
async function restoreTutor(id){
  try{ await dbUpdate('tutors', id, {archived:false}); mkToast('\u0420\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0430 \u0432\u0456\u0434\u043d\u043e\u0432\u043b\u0435\u043d\u043e'); renderTutors(); }
  catch(e){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+e.message,'error'); }
}
window.archiveTutor=archiveTutor; window.restoreTutor=restoreTutor;
// Понеділок поточного тижня (YYYY-MM-DD). Заняття з датою РАНІШЕ за нього — минулий тиждень.
function weekStartMonday(){
  var d=new Date();
  var day=d.getDay(); var diff=(day===0?6:day-1); // 0=Нд → 6 днів від Пн
  var mon=new Date(d.getFullYear(), d.getMonth(), d.getDate()-diff);
  return mon.getFullYear()+'-'+String(mon.getMonth()+1).padStart(2,'0')+'-'+String(mon.getDate()).padStart(2,'0');
}
// Заморожене (read-only) заняття:
//  • ПРОВЕДЕНЕ заняття за МИНУЛІ тижні (цього тижня ще можна редагувати);
//  • або проведене заняття заархівованого репетитора.
// Директор (і вище) може редагувати будь-що — для нього блокування не діє.
function lessonLocked(l){
  if(typeof l==='string') l=(S.lessons||[]).find(function(x){return x.id===l;});
  if(!l) return false;
  // Адміністратор і директор (та вище) редагують заморожені заняття; тутори — ні.
  if(['god','network_admin','director','admin'].indexOf(R())>=0) return false;
  if(!isConductedStatus(l.status)) return false;         // не проведене — вільно
  if(tutorArchived(l.tutorId||l.tutor_id)) return true;  // архівний репетитор — заморожено
  return String(l.date||'') < weekStartMonday();         // минулі тижні — заморожено
}
window.lessonLocked=lessonLocked;
// Тему, ДЗ та літературу репетитор може правити до КІНЦЯ МІСЯЦЯ (а не лише тижня).
// Заблоковано лише якщо заняття в МИНУЛОМУ місяці (адмін/директор — завжди можуть).
function journalFieldsLocked(l){
  if(typeof l==='string') l=(S.lessons||[]).find(function(x){return x.id===l;});
  if(!l) return false;
  if(['god','network_admin','director','admin'].indexOf(R())>=0) return false;
  var cur=new Date(); var curMonth=cur.getFullYear()+'-'+String(cur.getMonth()+1).padStart(2,'0');
  return String(l.date||'').slice(0,7) < curMonth;   // минулий місяць — журнал заблоковано
}
window.journalFieldsLocked=journalFieldsLocked;
// Переносити (міняти день/час) заняття можуть лише адмін і вище. Репетитор
// після встановлення заняття не може його переміщувати.
function canMoveLessons(){ return ['god','network_admin','director','admin'].indexOf(R())>=0; }
window.canMoveLessons=canMoveLessons;
// «Планове» заняття для обмежень — ЗВИЧАЙНЕ заплановане (planned/scheduled/без статусу).
// Заплановані ВІДПРАЦЮВАННЯ (makeup_planned) сюди НЕ входять — їх репетитор може
// переносити/видаляти. Саме звичайні планові репетитор не може переносити/видаляти.
function isPlannedLesson(l){ var s=l&&l.status; return !s || s==='planned' || s==='scheduled'; }
window.isPlannedLesson=isPlannedLesson;

// ── Передача учнів від одного репетитора іншому (масово або по одному) ──
function openTransferStudents(fromTutorId){
  var from=(S.tutors||[]).find(function(t){return t.id===fromTutorId;});
  if(!from) return;
  var studs=(S.students||[]).filter(function(s){ return studentTutorIds(s).indexOf(fromTutorId)>=0; })
    .sort(function(a,b){return (a.ln+' '+a.fn).localeCompare(b.ln+' '+b.fn,'uk');});
  if(!studs.length){ mkToast('\u0423 \u0446\u044c\u043e\u0433\u043e \u0440\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0430 \u043d\u0435\u043c\u0430\u0454 \u0443\u0447\u043d\u0456\u0432','info'); return; }
  var targets=activeTutors().filter(function(t){return t.id!==fromTutorId;})
    .sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');});
  closeTransferStudents();
  var ov=document.createElement('div');
  ov.id='transfer-overlay';
  ov.style.cssText='position:fixed;inset:0;z-index:900;background:rgba(0,0,0,.55);display:flex;align-items:center;justify-content:center;padding:16px';
  ov.onclick=function(e){ if(e.target===ov) closeTransferStudents(); };
  ov.innerHTML=
    '<div style="background:var(--s1);border:1px solid var(--b1);border-radius:14px;width:min(440px,96vw);max-height:88vh;display:flex;flex-direction:column;overflow:hidden">'
      +'<div style="padding:14px 18px;border-bottom:1px solid var(--b1);font-weight:700;font-size:15px">\uD83D\uDC65 \u041f\u0435\u0440\u0435\u0434\u0430\u0442\u0438 \u0443\u0447\u043d\u0456\u0432</div>'
      +'<div style="padding:14px 18px;overflow-y:auto">'
        +'<div style="font-size:12px;color:var(--t2);margin-bottom:10px">\u0412\u0456\u0434: <b>'+from.fn+' '+from.ln+'</b></div>'
        +'<label style="font-size:11px;color:var(--t3);text-transform:uppercase;letter-spacing:.4px;font-weight:600">\u041a\u043e\u043c\u0443</label>'
        +'<select id="tr-to" style="width:100%;margin:4px 0 14px;font-size:13px;padding:8px;border:1px solid var(--b1);border-radius:8px;background:var(--s2);color:var(--t1)">'
          +'<option value="">\u2014 \u043e\u0431\u0435\u0440\u0456\u0442\u044c \u0440\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0430 \u2014</option>'
          +targets.map(function(t){return '<option value="'+t.id+'">'+t.fn+' '+t.ln+'</option>';}).join('')
        +'</select>'
        +'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">'
          +'<span style="font-size:11px;color:var(--t3);text-transform:uppercase;letter-spacing:.4px;font-weight:600">\u0423\u0447\u043d\u0456 ('+studs.length+')</span>'
          +'<label style="font-size:12px;color:var(--t2);cursor:pointer;display:inline-flex;align-items:center;gap:5px"><input type="checkbox" id="tr-all" checked onchange="trToggleAll(this)"> \u0443\u0441\u0456</label>'
        +'</div>'
        +'<div style="border:1px solid var(--b1);border-radius:8px;max-height:240px;overflow-y:auto">'
          +studs.map(function(s){return '<label style="display:flex;align-items:center;gap:9px;padding:8px 11px;border-bottom:1px solid var(--s3);font-size:13px;cursor:pointer"><input type="checkbox" class="tr-cb" value="'+s.id+'" checked> '+snShort(s.id)+'</label>';}).join('')
        +'</div>'
        +'<div style="font-size:11px;color:var(--t3);margin-top:10px">\u041c\u0438\u043d\u0443\u043b\u0456 \u0437\u0430\u043d\u044f\u0442\u0442\u044f \u043b\u0438\u0448\u0430\u044e\u0442\u044c\u0441\u044f \u0437\u0430 \u043f\u043e\u043f\u0435\u0440\u0435\u0434\u043d\u0456\u043c \u0440\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u043e\u043c; \u0437\u043c\u0456\u043d\u044e\u0454\u0442\u044c\u0441\u044f \u043b\u0438\u0448\u0435 \u043c\u0430\u0439\u0431\u0443\u0442\u043d\u0454 \u043f\u0440\u0438\u0437\u043d\u0430\u0447\u0435\u043d\u043d\u044f.</div>'
      +'</div>'
      +'<div style="padding:12px 18px;border-top:1px solid var(--b1);display:flex;gap:8px;justify-content:flex-end">'
        +'<button class="btn btn-g btn-sm" onclick="closeTransferStudents()">\u0421\u043a\u0430\u0441\u0443\u0432\u0430\u0442\u0438</button>'
        +'<button class="btn btn-p btn-sm" onclick="doTransferStudents(\''+fromTutorId+'\')">\u041f\u0435\u0440\u0435\u0434\u0430\u0442\u0438</button>'
      +'</div>'
    +'</div>';
  document.body.appendChild(ov);
}
function trToggleAll(cb){ document.querySelectorAll('.tr-cb').forEach(function(c){ c.checked=cb.checked; }); }
function closeTransferStudents(){ var o=document.getElementById('transfer-overlay'); if(o) o.remove(); }
async function doTransferStudents(fromId){
  var toEl=document.getElementById('tr-to');
  var toId=toEl?toEl.value:'';
  if(!toId){ mkToast('\u041e\u0431\u0435\u0440\u0456\u0442\u044c \u0440\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0430-\u043e\u0442\u0440\u0438\u043c\u0443\u0432\u0430\u0447\u0430','error'); return; }
  var ids=Array.from(document.querySelectorAll('.tr-cb:checked')).map(function(c){return c.value;});
  if(!ids.length){ mkToast('\u041e\u0431\u0435\u0440\u0456\u0442\u044c \u0445\u043e\u0447\u0430 \u0431 \u043e\u0434\u043d\u043e\u0433\u043e \u0443\u0447\u043d\u044f','error'); return; }
  closeTransferStudents();
  var ok=0;
  for(var i=0;i<ids.length;i++){
    var st=(S.students||[]).find(function(x){return x.id===ids[i];});
    if(!st) continue;
    var rules=studentRatesArr(st).map(function(r){ return {subject:(r.subject||''), tutor_id:(r.tutor_id||''), rate:(r.rate!=null?r.rate:null)}; });
    var wasInRules=rules.some(function(r){return r.tutor_id===fromId;});
    if(wasInRules){
      rules.forEach(function(r){ if(r.tutor_id===fromId) r.tutor_id=toId; });
    } else {
      // Учень прив'язаний лише через старі поля — створюємо ставку для нового репетитора
      var subj0=st.subject?String(st.subject).split(',')[0].trim():'';
      rules.push({subject:subj0, tutor_id:toId, rate:null});
    }
    // Прибираємо точні дублікати (предмет+репетитор+ставка)
    var seen={}, dedup=[];
    rules.forEach(function(r){ var k=(r.subject||'')+'|'+(r.tutor_id||'')+'|'+(r.rate!=null?r.rate:''); if(!seen[k]){ seen[k]=1; dedup.push(r); } });
    // Похідні поля — так само, як у saveStudent
    var subj=(function(){var ss=[];dedup.forEach(function(r){var s=(r.subject||'').trim();if(s&&ss.indexOf(s)<0)ss.push(s);});return ss.join(', ');})();
    var tId=(function(){var f=dedup.find(function(r){return r.tutor_id;});return f?f.tutor_id:null;})();
    var tIds=(function(){var tt=[];dedup.forEach(function(r){if(r.tutor_id&&tt.indexOf(r.tutor_id)<0)tt.push(r.tutor_id);});return tt.join(',');})();
    try{ await dbUpdate('students', st.id, { rates:dedup, subject:subj, tutor_id:tId, tutor_ids:tIds }); ok++; }catch(e){}
  }
  mkToast('\u041f\u0435\u0440\u0435\u0434\u0430\u043d\u043e \u0443\u0447\u043d\u0456\u0432: '+ok);
  try{ renderTutors(); }catch(e){}
  if(S.currentPage==='students'){ try{ renderStudents(); }catch(e){} }
}
window.openTransferStudents=openTransferStudents;
window.closeTransferStudents=closeTransferStudents;
window.doTransferStudents=doTransferStudents;
window.trToggleAll=trToggleAll;
// Вартість заняття: ставка ЗА ГОДИНУ × (тривалість/60).
// Ставка: знімок у занятті (l.price) → якщо нема, ставка з картки учня за правилами.
function lessonTotal(l){
  var dur=(parseFloat(l.dur)||60)/60;
  var rate=(l.price!=null&&l.price!==''&&!isNaN(parseFloat(l.price))&&parseFloat(l.price)>0)?parseFloat(l.price):(function(){
    var sid=l.studentId||l.student_id;
    var st=sid?(S.students||[]).find(function(s){return s.id===sid;}):null;
    return studentRate(st, l.subject, l.tutorId||l.tutor_id);
  })();
  return Math.round(dur*rate*100)/100;
}

// Ставка учня для конкретного предмета/репетитора.
// Правила в st.rates: [{subject:'', tutor_id:'', rate:N}], порожнє поле = будь-який.
// Пріоритет: предмет+репетитор (3) → предмет (2) → репетитор (1) → базова hourly_rate.
function studentRate(st, subject, tutorId){
  if(!st) return 0;
  var rules=[];
  try{ rules=typeof st.rates==='string'?JSON.parse(st.rates||'[]'):(Array.isArray(st.rates)?st.rates:[]); }catch(e){ rules=[]; }
  var subjN=(subject||'').trim().toLowerCase();
  var best=null, bestScore=-1;
  rules.forEach(function(r){
    if(isNaN(parseFloat(r.rate))) return;
    var rs=(r.subject||'').trim().toLowerCase();
    if(rs && rs!==subjN) return;          // предмет вказано і не збігся
    if(r.tutor_id && r.tutor_id!==tutorId) return; // репетитора вказано і не збігся
    var score=(rs?2:0)+(r.tutor_id?1:0);
    if(score>bestScore){ bestScore=score; best=r; }
  });
  if(best) return parseFloat(best.rate);
  var b=parseFloat(st.hourly_rate);
  return isNaN(b)?0:b;
}

// ── UI рядків ставок у картці учня ──
function addRateRow(rule){
  rule=rule||{};
  var list=document.getElementById('s-rates-list');
  if(!list) return;
  var row=document.createElement('div');
  row.className='s-rate-row';
  row.style.cssText='display:flex;gap:6px;align-items:center;flex-wrap:wrap';
  row.innerHTML=
    '<input class="sr-subj" list="subj-list-s" placeholder="Предмет (будь-який)" value="'+String(rule.subject||'').replace(/"/g,'&quot;')+'" style="flex:2;min-width:110px;font-size:12px;padding:5px 8px">'
    +'<select class="sr-tutor" style="flex:2;min-width:110px;font-size:12px;padding:5px 8px">'
      +'<option value="">Будь-який репетитор</option>'
      +(function(){
        var _ts=activeTutors();
        if(rule.tutor_id && !_ts.some(function(t){return t.id===rule.tutor_id;})){ var _a=(S.tutors||[]).find(function(t){return t.id===rule.tutor_id;}); if(_a) _ts=_ts.concat([_a]); }
        return _ts.slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');})
        .map(function(t){return '<option value="'+t.id+'"'+(t.id===(rule.tutor_id||'')?' selected':'')+'>'+t.fn+' '+t.ln+(t.archived?' (з архіву)':'')+'</option>';}).join('');
      })()
    +'</select>'
    +'<input class="sr-rate" type="number" placeholder="₴/год" value="'+(rule.rate!=null?rule.rate:'')+'" style="width:80px;font-size:12px;padding:5px 8px">'
    +'<button type="button" title="Прибрати" style="border:none;background:none;cursor:pointer;color:var(--danger);font-size:14px;padding:2px 6px" onclick="this.parentElement.remove()">✕</button>';
  list.appendChild(row);
  // Підказки предметів у рядку ставки (працюють і на телефонах)
  var _si=row.querySelector('.sr-subj');
  if(_si){ _si.id=_si.id||('sr-subj-'+Math.random().toString(36).slice(2,8)); makeSuggest(_si.id, allKnownSubjects); }
}
// Похідні дані зі ставок учня: предмети та репетитори (з фолбеком на legacy-поля)
function studentRatesArr(st){
  if(!st) return [];
  try{ var r=typeof st.rates==='string'?JSON.parse(st.rates||'[]'):(Array.isArray(st.rates)?st.rates:[]); return r||[]; }catch(e){ return []; }
}
function studentSubjects(st){
  var out=[];
  studentRatesArr(st).forEach(function(r){ var s=(r.subject||'').trim(); if(s&&out.indexOf(s)<0) out.push(s); });
  if(!out.length && st && st.subject) out.push(st.subject);
  return out;
}
function studentTutorIds(st){
  var out=[];
  studentRatesArr(st).forEach(function(r){ if(r.tutor_id&&out.indexOf(r.tutor_id)<0) out.push(r.tutor_id); });
  if(!out.length && st){
    var legacy=(Array.isArray(st.tutorIds)&&st.tutorIds.length)?st.tutorIds:(st.tutorId?[st.tutorId]:[]);
    legacy.forEach(function(t){ if(t&&out.indexOf(t)<0) out.push(t); });
  }
  return out;
}

function collectRateRows(){
  return Array.from(document.querySelectorAll('#s-rates-list .s-rate-row')).map(function(row){
    var rv=parseFloat(row.querySelector('.sr-rate')?.value);
    return {
      subject:(row.querySelector('.sr-subj')?.value||'').trim(),
      tutor_id:row.querySelector('.sr-tutor')?.value||'',
      rate:(!isNaN(rv)&&rv>0)?rv:null
    };
  }).filter(function(r){ return r.subject||r.tutor_id||r.rate; });
}

// Розбір правил ставок учня (рядок JSON або масив)
function studentRateRules(st){
  if(!st) return [];
  try{ return typeof st.rates==='string'?JSON.parse(st.rates||'[]'):(Array.isArray(st.rates)?st.rates:[]); }catch(e){ return []; }
}

function mkToast(msg,type='success'){
  const e=document.createElement('div');e.className=("toast "+(type));
  e.innerHTML=("<span>"+(type==='success'?'\u2705':'\u274C')+"</span> "+(msg));
  document.body.appendChild(e);setTimeout(()=>e.remove(),3000);
}

function popSel(id,arr,valKey,lblFn,placeholder='\u2014'){const el=document.getElementById(id);if(!el)return;el.innerHTML=("<option value=\"\">"+(placeholder)+"</option>")+arr.map(x=>("<option value=\""+(x[valKey])+"\">"+(lblFn(x))+"</option>")).join('');}
function popSelSearch(id,arr,valKey,lblFn,placeholder){
  var el=document.getElementById(id);
  var savedVal=el?el.value:'';
  popSel(id,arr,valKey,lblFn,placeholder);
  // Відновлюємо значення якщо воно є серед нових options
  if(el&&savedVal){
    el.value=savedVal;
    // якщо option не знайдено — скидаємо
    if(el.value!==savedVal) el.value='';
  }
  makeSearchable(id);
  // Оновлюємо підказку ПІСЛЯ наповнення списку — інакше поле лишається
  // візуально порожнім і незрозуміло, що це фільтр
  var el2=document.getElementById(id);
  if(el2){
    var w=el2.closest('.srch-wrap');
    var inp=w?w.querySelector('input'):null;
    if(inp){
      // Беремо перший НЕПОРОЖНІЙ пункт: popSel додає попереду порожній,
      // через нього поле виглядало без підказки й здавалось зламаним
      var ph='';
      for(var oi=0;oi<el2.options.length;oi++){
        var t=(el2.options[oi].text||'').trim();
        if(t){ ph=t; break; }
      }
      inp.placeholder = ph || '\u041f\u043e\u0448\u0443\u043a\u2026';
    }
    if(el2._updateSearch) el2._updateSearch();
  }
}

function openM(id){
  var el=document.getElementById(id);
  if(!el) return;
  el.style.display='';
  el.style.pointerEvents='';
  el.classList.add('open');
}

function closeM(id){
  // Зупиняємо диктування, щоб мікрофон не лишався ввімкненим
  try{ voiceStop(); }catch(e){}
  var el=document.getElementById(id);
  if(el){
    el.style.display='none';
    el.classList.remove('open');
    el.style.pointerEvents='none';
  }
  S.editId=null;
  // Reset pointer events on all modals
  document.querySelectorAll('.mo').forEach(function(m){
    if(!m.classList.contains('open') && m.style.display==='none'){
      m.style.pointerEvents='none';
    }
  });
}
