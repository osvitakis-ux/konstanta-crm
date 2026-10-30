


async function uploadTutorPhoto(input){
  var file=input.files[0];
  if(!file)return;
  if(file.size>2*1024*1024){mkToast('Файл занадто великий (макс 2MB)','error');return;}
  var reader=new FileReader();
  reader.onload=async function(e){
    var dataUrl=e.target.result;
    var mt=myTutor();
    if(!mt){mkToast('Профіль не знайдено','error');return;}
    try{
      await dbUpdate('tutors',mt.id,{photo:dataUrl});
      // Update local
      var idx=S.tutors.findIndex(function(t){return t.id===mt.id;});
      if(idx>=0) S.tutors[idx].photo=dataUrl;
      // Update photo preview in form
      var prev=document.getElementById('pr-photo-preview');
      if(prev) prev.innerHTML='<img src="'+dataUrl+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%">';
      mkToast('Фото збережено ✅');
      renderProfile();
    }catch(err){mkToast('Помилка: '+err.message,'error');}
  };
  reader.readAsDataURL(file);
}
window.uploadTutorPhoto=uploadTutorPhoto;


// ═══════════════════════════════════
// SEARCHABLE SELECT UTILITY
// ═══════════════════════════════════
function makeSearchable(selectId){
  var sel=document.getElementById(selectId);
  if(!sel) return;

  // Already initialized — just re-sync options into dropdown
  if(sel.dataset.searchable==='1'){
    sel._updateSearch && sel._updateSearch();
    return;
  }
  sel.dataset.searchable='1';
  sel.style.display='none';

  var wrap=document.createElement('div');
  wrap.className='srch-wrap';
  wrap.style.cssText='position:relative;display:inline-block;width:100%';
  sel.parentNode.insertBefore(wrap,sel);
  wrap.appendChild(sel);

  var inp=document.createElement('input');
  // Підказку беремо з першого пункту списку. Якщо на момент створення
  // список ще порожній — ставимо загальну, інакше поле виглядає порожнім
  // і незрозуміло, що це фільтр.
  inp.placeholder=(sel.options[0] && sel.options[0].text) ? sel.options[0].text : '\u041f\u043e\u0448\u0443\u043a\u2026';
  inp.dataset.autoPlaceholder='1';
  inp.style.cssText='width:100%;padding:6px 10px;border:1.5px solid var(--b1);border-radius:8px;background:var(--s1);font-size:13px;box-sizing:border-box;color:var(--t1)';
  inp.setAttribute('autocomplete','off');
  wrap.insertBefore(inp,sel);

  var drop=document.createElement('div');
  // У body, не у wrap: інакше overflow:hidden модалки (.mdl) обрізає й блокує скрол списку,
  // бо CSS-анімація модалки з transform створює containing block для position:fixed/absolute нащадків.
  document.body.appendChild(drop);
  function positionSearchDrop(){
    var r=inp.getBoundingClientRect();
    drop.style.position='fixed';
    drop.style.top=r.bottom+'px';
    drop.style.left=r.left+'px';
    drop.style.width=r.width+'px';
  }
  drop.style.cssText='background:var(--s1);border:1px solid var(--b1);border-radius:8px;max-height:220px;overflow-y:auto;z-index:9999;display:none;box-shadow:0 4px 16px rgba(0,0,0,.18)';
  positionSearchDrop();
  window.addEventListener('resize', function(){ if(drop.style.display!=='none') positionSearchDrop(); });
  window.addEventListener('scroll', function(){ if(drop.style.display!=='none') positionSearchDrop(); }, true);

  function renderDrop(q){
    drop.innerHTML='';
    var opts=Array.from(sel.options);
    var filtered=q?opts.filter(function(o){return o.text.toLowerCase().includes(q.toLowerCase());}):opts;
    if(!filtered.length){ drop.style.display='none'; return; }
    filtered.forEach(function(o){
      var item=document.createElement('div');
      item.textContent=o.text;
      item.dataset.val=o.value;
      item.style.cssText='padding:8px 12px;cursor:pointer;font-size:13px;color:var(--t1)';
      item.addEventListener('mouseenter',function(){item.style.background='var(--s2)';});
      item.addEventListener('mouseleave',function(){item.style.background='';});
      item.addEventListener('mousedown',function(e){
        e.preventDefault();
        sel.value=o.value;
        inp.value=o.value?o.text:'';
        drop.style.display='none';
        inp.placeholder=sel.options[0]?sel.options[0].text:'Пошук...';
        sel.dispatchEvent(new Event('change'));
      });
      drop.appendChild(item);
    });
    drop.style.display='block';
  }

  inp.addEventListener('focus',function(){ inp.select(); positionSearchDrop(); renderDrop(''); });
  inp.addEventListener('input',function(){ sel.value=''; renderDrop(inp.value); });
  inp.addEventListener('blur',function(){ setTimeout(function(){ drop.style.display='none'; },200); });

  // Called after options are refreshed externally
  sel._updateSearch=function(){
    var opt=sel.value?Array.from(sel.options).find(function(o){return o.value===sel.value;}):null;
    inp.value=opt?opt.text:'';
    var ph2='';
    for(var pi=0;pi<sel.options.length;pi++){
      var tt=(sel.options[pi].text||'').trim();
      if(tt){ ph2=tt; break; }
    }
    inp.placeholder = ph2 || '\u041f\u043e\u0448\u0443\u043a\u2026';
  };
  sel._updateSearch();
}
window.makeSearchable=makeSearchable;

document.addEventListener('DOMContentLoaded', initApp);

// Tutor checkbox visual feedback
document.addEventListener('change', function(e){
  if(e.target && e.target.classList.contains('st-tutor-cb')){
    var lbl = e.target.closest('label');
    if(lbl){
      lbl.style.background = e.target.checked ? 'rgba(41,171,226,.15)' : 'var(--s1)';
      lbl.style.borderColor = e.target.checked ? 'var(--adm)' : 'var(--b1)';
    }
  }
});


// Перевіряє чи пропущений урок є відпрацьованим
// (є makeup_date АБО є урок зі статусом makeup для того ж учня в тих самих даних)
// Перевіряє чи makeup-урок є парою до якогось пропущеного (зворотній зв'язок)
function isMakeupForMissed(l){
  if(l.status!=='makeup') return false;
  if(l.missed_date) return true;
  var sid = l.studentId||l.student_id;
  var tid = l.tutorId||l.tutor_id;
  return (S.lessons||[]).some(function(x){
    if(x.status!=='missed') return false;
    if((x.studentId||x.student_id)!==sid) return false;
    if((x.tutorId||x.tutor_id)!==tid) return false;
    return x.makeup_date===l.date || x.split_group_id===l.id || l.split_group_id===x.id;
  });
}

// Сума хвилин ПРОВЕДЕНИХ відпрацювань, пов'язаних із цим пропущеним уроком
function coveredMissedMinutes(l){
  var sid = l.studentId||l.student_id;
  var ldate = l.date;

  // Усі пропущені заняття ЦЬОГО учня в ЦЕЙ САМЕ день — незалежно від того, чи це
  // частини одного розбитого уроку (split_group_id), чи два зовсім різні уроки,
  // що просто збіглися датою. Раніше зіставлення йшло лише за датою (missed_date),
  // тому одне відпрацювання "бачили" одразу всі пропуски того дня і кожен вважав
  // себе покритим повністю. Тепер пул хвилин відпрацювань розподіляється між ними
  // ПОСЛІДОВНО (за часом), а не роздається кожному окремо.
  // ВАЖЛИВО: враховуємо ще й РЕПЕТИТОРА. Один учень може мати в один день
  // заняття у двох різних репетиторів і пропустити обидва. Відпрацювання,
  // яке провів один репетитор, не має закривати пропуск іншого —
  // раніше зіставлення йшло лише за учнем і датою, тож пропуски
  // "закривались" чужими відпрацюваннями.
  var tid = l.tutorId||l.tutor_id;
  var sameDayParts = (S.lessons||[]).filter(function(x){
    return x.status==='missed' && x.date===ldate
        && ((x.studentId||x.student_id)===sid)
        && ((x.tutorId||x.tutor_id)===tid);
  }).sort(function(a,b){
    var ai=(a.split_index!=null?a.split_index:999), bi=(b.split_index!=null?b.split_index:999);
    if(ai!==bi) return ai-bi;
    return String(a.time||'').localeCompare(String(b.time||''));
  });

  // Пул відпрацювань, пов'язаних із цим днем: за explicit missed_date, за split_group_id
  // будь-якої з частин, або за посиланням makeup_date з боку пропущеного уроку.
  var groupIds=[]; sameDayParts.forEach(function(p){ if(p.split_group_id&&groupIds.indexOf(p.split_group_id)<0) groupIds.push(p.split_group_id); });
  var makeupDates=[]; sameDayParts.forEach(function(p){ if(p.makeup_date&&makeupDates.indexOf(p.makeup_date)<0) makeupDates.push(p.makeup_date); });
  var missTutorArchived = tutorArchived(tid); // репетитор пропуску звільнений?
  var makeups = (S.lessons||[]).filter(function(x){
    var isMk = (x.status==='makeup');
    var isBurnedMk = (x.status==='burned' && x.missed_date); // згоріле-як-відпрацювання: лише з явним посиланням
    if(!isMk && !isBurnedMk) return false; // рахуються проведені makeup + згорілі, що відпрацьовують пропуск
    if((x.studentId||x.student_id)!==sid) return false;
    var xtid = x.tutorId||x.tutor_id;
    if(xtid===tid){
      // Той самий репетитор — стандартне зіставлення.
      if(x.missed_date===ldate) return true;
      if(isMk && x.split_group_id && groupIds.indexOf(x.split_group_id)>=0) return true;
      if(isMk && makeupDates.indexOf(x.date)>=0) return true;
      return false;
    }
    // ІНШИЙ репетитор: зараховуємо лише якщо репетитор пропуску ЗААРХІВОВАНИЙ
    // (звільнився) і відпрацювання ЯВНО посилається на цей пропуск (missed_date).
    // Для активних репетиторів обмеження лишається: урок одного не закриває пропуск
    // іншого (напр. Осєтрової — Когута), навіть якщо учень і день збігаються.
    if(missTutorArchived && x.missed_date===ldate) return true;
    return false;
  });
  var pool = makeups.reduce(function(s,x){ return s+(parseFloat(x.dur)||60); }, 0);

  // Розподіляємо пул послідовно між усіма пропусками цього дня; повертаємо частку саме l.
  for(var i=0;i<sameDayParts.length;i++){
    var need = parseFloat(sameDayParts[i].dur)||60;
    var give = Math.min(pool, need);
    pool -= give;
    if(sameDayParts[i].id===l.id) return give;
  }
  return 0; // l не знайдено серед власних пропусків цього дня (не мало б статись)
}

// Пропуск вважається покритим ЛИШЕ коли сума годин відпрацювань >= пропущених годин.
// (Раніше makeup_date давав "покрито" без порівняння тривалостей — 2 пропущені години
//  "закривались" 1 годиною відпрацювання.)
function isCoveredMissed(l){
  if(l.status!=='missed') return false;
  return coveredMissedMinutes(l) >= (parseFloat(l.dur)||60);
}

function getUncoveredMissed(lessons){
  return (lessons||[]).filter(function(l){
    return (l.status==='missed') && !isCoveredMissed(l);
  });
}

// Фільтр для дашборду: пропущені без відпрацювання
// Повертає непокриту тривалість пропущеного уроку в годинах
function uncoveredMissedHours(l){
  if(l.status!=='missed') return 0;
  var uncovered=Math.max(0,(parseFloat(l.dur)||60)-coveredMissedMinutes(l));
  return Math.round(uncovered/60*10)/10;
}

function uncoveredMissedFilter(lessons){
  return (lessons||[]).filter(function(l){
    return (l.status==='missed'||l.status==='absent') && !isCoveredMissed(l);
  });
}

document.addEventListener('DOMContentLoaded', initApp);

// Tutor checkbox visual feedback
document.addEventListener('change', function(e){
  if(e.target && e.target.classList.contains('st-tutor-cb')){
    var lbl = e.target.closest('label');
    if(lbl){
      lbl.style.background = e.target.checked ? 'rgba(41,171,226,.15)' : 'var(--s1)';
      lbl.style.borderColor = e.target.checked ? 'var(--adm)' : 'var(--b1)';
    }
  }
});
