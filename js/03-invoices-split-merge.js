


// ═══════════════════════════════════
// ДОДАТКОВІ ФУНКЦІЇ — рахунки, viber, розбивка
// ═══════════════════════════════════

function updateInvPhone(){
  var sid=(document.getElementById('inv-student')||{value:''}).value;
  var wrap=document.getElementById('inv-phone-wrap');
  if(!sid||!wrap){if(wrap)wrap.style.display='none';return;}
  var s=(S.students||[]).find(function(x){return x.id===sid;});
  wrap.style.display=(s&&(s.phone||s.parentPhone))?'block':'none';
  var ph=document.getElementById('inv-phone');
  if(ph&&s) ph.value=s.phone||s.parentPhone||'';
}

function openViberContact(){
  var sid=(document.getElementById('inv-student')||{value:''}).value;
  var s=(S.students||[]).find(function(x){return x.id===sid;});
  var phone=(s&&(s.phone||s.parentPhone)||'').replace(/\D/g,'');
  if(phone) window.open('viber://chat?number='+phone);
  else mkToast('Немає телефону','error');
}

function sendViberFromPanel(){
  var sid=(document.getElementById('inv-student')||{value:''}).value;
  var s=(S.students||[]).find(function(x){return x.id===sid;});
  if(!s){mkToast('Оберіть учня','error');return;}
  var phone=((s.phone||s.parentPhone)||'').replace(/\D/g,'');
  if(phone) window.open('viber://chat?number='+phone);
  else mkToast('Немає телефону','error');
}

function openBranchM(id){
  var b=id?(S.branches||[]).find(function(x){return x.id===id;}):null;
  ['name','addr','phone','email'].forEach(function(f){
    var el=document.getElementById('br-'+f);
    if(el) el.value=b?(b[f]||b[f.replace('addr','address')]||''):'';
  });
  ['pay_recipient','pay_address','pay_card','pay_bank','pay_edrpou','pay_purpose'].forEach(function(f){
    var el=document.getElementById('br-'+f.replace(/_/g,'-'));
    if(el) el.value=b?(b[f]||''):'';
  });
  S.editId=id||null;
  openM('mo-branch');
}

async function saveBranchModal(){
  var name=(document.getElementById('br-name')||{value:''}).value.trim();
  if(!name){mkToast('Введіть назву','error');return;}
  var obj={
    name:name,
    address:(document.getElementById('br-addr')||{value:''}).value,
    phone:(document.getElementById('br-phone')||{value:''}).value,
    email:(document.getElementById('br-email')||{value:''}).value,
    pay_recipient:(document.getElementById('br-pay-recipient')||{value:''}).value,
    pay_address:(document.getElementById('br-pay-address')||{value:''}).value,
    pay_card:(document.getElementById('br-pay-card')||{value:''}).value,
    pay_bank:(document.getElementById('br-pay-bank')||{value:''}).value,
    pay_edrpou:(document.getElementById('br-pay-edrpou')||{value:''}).value,
    pay_purpose:(document.getElementById('br-pay-purpose')||{value:''}).value
  };
  try{
    if(S.editId) await dbUpdate('branches',S.editId,obj);
    else{obj.id=uid();await dbInsert('branches',obj);}
    mkToast('Збережено');
    closeM('mo-branch');
  }catch(e){mkToast('Помилка: '+e.message,'error');}
}

async function splitLessonToChunks(chunkMin){
  chunkMin = chunkMin||30;
  var id=S.editId;
  if(!id){mkToast('Не знайдено урок','error');return;}
  var orig=(S.lessons||[]).find(function(l){return l.id===id;});
  if(!orig){mkToast('Урок не знайдено','error');return;}
  var curDur=parseInt((document.getElementById('l-dur')||{value:'60'}).value)||parseInt(orig.dur)||60;
  var nParts=Math.floor(curDur/chunkMin);
  if(nParts<2){mkToast('Тривалість мінімум '+(chunkMin*2)+' хв для розбиття на '+chunkMin+' хв','error');return;}
  if(!confirm('Розбити ('+curDur+' хв) на '+nParts+' × '+chunkMin+' хв?'))return;
  var lt=orig.time||'10:00';
  var lh0=parseInt(lt.split(':')[0]);
  var lm0=parseInt(lt.split(':')[1]||'0');
  var base={
    student_id:orig.studentId||orig.student_id,
    tutor_id:orig.tutorId||orig.tutor_id,
    subject:orig.subject||'',date:orig.date,
    status:orig.status||'missed',dur:chunkMin,
    price:orig.price||0, // ставка за годину лишається незмінною — сума частин збігається через тривалості
    branch_id:orig.branchId||orig.branch_id||null,
    split_group_id:id,split_index:0
  };
  try{
    await dbUpdate('lessons',id,{dur:chunkMin,price:base.price,split_group_id:id,split_index:0});
    // Оновлюємо локально одразу — щоб розклад/список занять показали розбиття без затримки
    Object.assign(orig,{dur:chunkMin,price:base.price,split_group_id:id,split_index:0});
    for(var p=1;p<nParts;p++){
      var totalMins=lm0+chunkMin*p;
      var newH=lh0+Math.floor(totalMins/60);
      var newM=totalMins%60;
      var newTime=String(newH).padStart(2,'0')+':'+String(newM).padStart(2,'0');
      var _chunk=Object.assign({},base,{id:uid(),time:newTime,split_index:p});
      await dbInsert('lessons',_chunk);
      S.lessons.push(normalizeLesson(_chunk));
    }
    mkToast('Розбито на '+nParts+' × '+chunkMin+' хв');
    closeM('mo-lesson');
    refreshPage('lessons'); if(S.currentPage==='schedule') renderSch();
  }catch(e){mkToast('Помилка: '+e.message,'error');}
}
function splitLessonTo30(){ return splitLessonToChunks(30); }
function splitLessonTo60(){ return splitLessonToChunks(60); }

function splitPreset(val){
  var el=document.getElementById('split-parts-input');
  if(el){ el.value=val; updateSplitPreview(); }
}

function updateSplitPreview(){
  var id=S.editId;
  var orig=id?(S.lessons||[]).find(function(l){return l.id===id;}):null;
  var curDur=parseInt((document.getElementById('l-dur')||{value:'60'}).value)||parseInt((orig||{}).dur)||60;
  var input=(document.getElementById('split-parts-input')?.value||'').trim();
  var preview=document.getElementById('split-preview');
  if(!preview) return;
  if(!input){preview.textContent='';return;}
  var parts=input.split(',').map(function(s){return parseInt(s.trim())||0;}).filter(function(v){return v>0;});
  if(!parts.length){preview.textContent='';return;}
  var sum=parts.reduce(function(a,b){return a+b;},0);
  if(sum!==curDur){
    preview.textContent='\u26A0\uFE0F \u0421\u0443\u043c\u0430 ('+sum+' \u0445\u0432) \u043d\u0435 \u0434\u043e\u0440\u0456\u0432\u043d\u044e\u0454 \u0437\u0430\u0433\u0430\u043b\u044c\u043d\u0456\u0439 \u0442\u0440\u0438\u0432\u0430\u043b\u043e\u0441\u0442\u0456 ('+curDur+' \u0445\u0432)';
    preview.style.color='var(--danger)';
  } else {
    preview.textContent='\u2713 '+parts.map(function(p,i){return '\u0427\u0430\u0441\u0442\u0438\u043d\u0430 '+(i+1)+': '+p+' \u0445\u0432';}).join(' + ');
    preview.style.color='var(--tut)';
  }
}

async function splitLessonCustom(){
  var id=S.editId;
  if(!id){mkToast('\u041d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e \u0443\u0440\u043e\u043a','error');return;}
  var orig=(S.lessons||[]).find(function(l){return l.id===id;});
  if(!orig){mkToast('\u0423\u0440\u043e\u043a \u043d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e','error');return;}
  var curDur=parseInt((document.getElementById('l-dur')||{value:'60'}).value)||parseInt(orig.dur)||60;
  var input=(document.getElementById('split-parts-input')?.value||'').trim();
  var parts=input.split(',').map(function(s){return parseInt(s.trim())||0;}).filter(function(v){return v>0;});
  if(parts.length<2){mkToast('\u0412\u043a\u0430\u0436\u0456\u0442\u044c \u043c\u0456\u043d\u0456\u043c\u0443\u043c 2 \u0447\u0430\u0441\u0442\u0438\u043d\u0438 \u0447\u0435\u0440\u0435\u0437 \u043a\u043e\u043c\u0443','error');return;}
  var sum=parts.reduce(function(a,b){return a+b;},0);
  if(sum!==curDur){mkToast('\u0421\u0443\u043c\u0430 \u0447\u0430\u0441\u0442\u0438\u043d ('+sum+' \u0445\u0432) \u043d\u0435 \u0434\u043e\u0440\u0456\u0432\u043d\u044e\u0454 \u0442\u0440\u0438\u0432\u0430\u043b\u043e\u0441\u0442\u0456 ('+curDur+' \u0445\u0432)','error');return;}
  if(!confirm('\u0420\u043e\u0437\u0431\u0438\u0442\u0438 ('+curDur+' \u0445\u0432) \u043d\u0430: '+parts.join(' + ')+' \u0445\u0432?')) return;
  var lt=orig.time||'10:00';
  var lh0=parseInt(lt.split(':')[0]);
  var lm0=parseInt(lt.split(':')[1]||'0');
  var base={
    student_id:orig.studentId||orig.student_id,
    tutor_id:orig.tutorId||orig.tutor_id,
    subject:orig.subject||'',date:orig.date,
    status:orig.status||'missed',
    price:orig.price||0, // зберігаємо оригінальну погодинну ставку — інакше частини перерахуються за поточним правилом студента, що може відрізнятись від фактичної ціни цього заняття
    branch_id:orig.branchId||orig.branch_id||null,
    split_group_id:id
  };
  try{
    await dbUpdate('lessons',id,{dur:parts[0],split_group_id:id,split_index:0});
    Object.assign(orig,{dur:parts[0],split_group_id:id,split_index:0});
    var offset=lm0+parts[0];
    for(var i=1;i<parts.length;i++){
      var nh=lh0+Math.floor(offset/60);
      var nm=offset%60;
      var t=String(nh).padStart(2,'0')+':'+String(nm).padStart(2,'0');
      var _chunk=Object.assign({},base,{id:uid(),time:t,dur:parts[i],split_index:i});
      await dbInsert('lessons',_chunk);
      S.lessons.push(normalizeLesson(_chunk));
      offset+=parts[i];
    }
    mkToast('\u0420\u043e\u0437\u0431\u0438\u0442\u043e: '+parts.join(' + ')+' \u0445\u0432');
    closeM('mo-lesson');
    refreshPage('lessons'); if(S.currentPage==='schedule') renderSch();
  }catch(e){mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+e.message,'error');}
}


function populateMergeSelect(){
  var sel=document.getElementById('l-merge-select');
  if(!sel) return;
  var id=S.editId;
  var orig=id?(S.lessons||[]).find(function(l){return l.id===id;}):null;
  if(!orig){ sel.innerHTML='<option value="">\u2014 \u043e\u0431\u0435\u0440\u0456\u0442\u044c \u0443\u0440\u043e\u043a \u2014</option>'; return; }

  var sid=orig.studentId||orig.student_id;
  var date=orig.date;
  var time=orig.time||'';

  // Шукаємо уроки того самого учня в той самий день (крім поточного)
  var candidates=(S.lessons||[]).filter(function(l){
    if(l.id===id) return false;
    if((l.studentId||l.student_id)!==sid) return false;
    if(l.date!==date) return false;
    return true;
  }).sort(function(a,b){return (a.time||'').localeCompare(b.time||'');});

  sel.innerHTML='<option value="">\u2014 \u043e\u0431\u0435\u0440\u0456\u0442\u044c \u0443\u0440\u043e\u043a \u2014</option>'
    +candidates.map(function(l){
      var tutor=(S.tutors||[]).find(function(t){return t.id===(l.tutorId||l.tutor_id);});
      var lbl=(l.time||'??:??')+' \u2014 '+(l.subject||'\u0431\u0435\u0437 \u043f\u0440\u0435\u0434\u043c\u0435\u0442\u0430')+' ('+(l.dur||60)+' \u0445\u0432)'+(tutor?' \u2014 '+tutor.fn+' '+tutor.ln:'');
      return '<option value="'+l.id+'">'+lbl+'</option>';
    }).join('');

  sel.onchange=function(){
    var preview=document.getElementById('l-merge-preview');
    if(!preview) return;
    if(!sel.value){preview.textContent='';return;}
    var other=(S.lessons||[]).find(function(l){return l.id===sel.value;});
    if(!other){preview.textContent='';return;}
    var totalDur=(parseFloat(orig.dur)||60)+(parseFloat(other.dur)||60);
    var earlierTime=[orig.time,other.time].sort()[0]||orig.time;
    preview.textContent='\u0420\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442: \u0443\u0440\u043e\u043a \u043e '+earlierTime+', \u0442\u0440\u0438\u0432\u0430\u043b\u0456\u0441\u0442\u044c '+totalDur+' \u0445\u0432';
  };
}

async function mergeLessons(){
  var id=S.editId;
  var orig=id?(S.lessons||[]).find(function(l){return l.id===id;}):null;
  if(!orig){mkToast('\u041d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e \u0443\u0440\u043e\u043a','error');return;}
  var sel=document.getElementById('l-merge-select');
  var otherId=sel?sel.value:'';
  if(!otherId){mkToast('\u041e\u0431\u0435\u0440\u0456\u0442\u044c \u0443\u0440\u043e\u043a \u0434\u043b\u044f \u043e\u0431\u2019\u0454\u0434\u043d\u0430\u043d\u043d\u044f','error');return;}
  var other=(S.lessons||[]).find(function(l){return l.id===otherId;});
  if(!other){mkToast('\u0423\u0440\u043e\u043a \u043d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e','error');return;}

  var totalDur=(parseFloat(orig.dur)||60)+(parseFloat(other.dur)||60);
  // Залишаємо той що раніше починається
  var keep=([orig,other].sort(function(a,b){return (a.time||'').localeCompare(b.time||'');}))[0];
  var remove=keep.id===orig.id?other:orig;

  if(!confirm('\u041e\u0431\u2019\u0454\u0434\u043d\u0430\u0442\u0438 \u0443\u0440\u043e\u043a\u0438 \u0432 \u043e\u0434\u0438\u043d \u0442\u0440\u0438\u0432\u0430\u043b\u0456\u0441\u0442\u044e '+totalDur+' \u0445\u0432?')) return;

  try{
    // Оновлюємо перший урок — нова тривалість, прибираємо split
    await dbUpdate('lessons',keep.id,{
      dur:totalDur,
      split_group_id:null,
      split_index:null
    });
    Object.assign(keep,{dur:totalDur,split_group_id:null,split_index:null,splitGroupId:null,splitIndex:null});
    // Видаляємо другий урок
    await dbDelete('lessons',remove.id);
    mkToast('\u0423\u0440\u043e\u043a\u0438 \u043e\u0431\u2019\u0454\u0434\u043d\u0430\u043d\u043e: '+totalDur+' \u0445\u0432');
    closeM('mo-lesson');
    refreshPage('lessons'); if(S.currentPage==='schedule') renderSch();
  }catch(e){mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+e.message,'error');}
}

window.mergeLessons=mergeLessons;
window.populateMergeSelect=populateMergeSelect;

/*
 * ПРАВИЛА РОЗРАХУНКУ РЕЙТИНГУ РЕПЕТИТОРА (4 тижні)
 * Базується на останніх 28 днях.
 *
 * ФОРМУЛА:
 *   Проведено  = done + completed + makeup (відпрацьовані = зараховуються)
 *   Пропущено  = missed (лише без відпрацювання)
 *   Відсоток   = Проведено / (Проведено + Пропущено) × 100
 *
 * ШКАЛА:
 *   ⭐⭐⭐⭐⭐ (5) — 90%+ і жодного непокритого пропуску
 *   ⭐⭐⭐⭐  (4) — 80–89% або є 1 пропуск але відпрацьований
 *   ⭐⭐⭐   (3) — 65–79%
 *   ⭐⭐    (2) — 40–64%
 *   ⭐     (1) — менше 40%
 *   Немає занять за 4 тижні → 5 (нейтральний)
 *
 * БОНУС: якщо всі пропущені мають makeup — штраф зменшується на 1 зірку
 */
function calcTutorRating(tutorId){
  var now=new Date(), fourWeeksAgo=new Date(now);
  fourWeeksAgo.setDate(now.getDate()-28);
  var from=localDateStr(fourWeeksAgo), today=localDateStr(now);

  var lessons=(S.lessons||[]).filter(function(l){
    return (l.tutorId||l.tutor_id)===tutorId && l.date>=from && l.date<=today;
  });

  if(!lessons.length) return 5; // немає занять — нейтральний рейтинг

  // Проведені: done, completed, makeup (відпрацювання теж рахується)
  var done   = lessons.filter(function(l){
    return isDoneLesson(l)||l.status==='makeup';
  }).length;

  // Пропущені без відпрацювання
  var missed = lessons.filter(function(l){ return l.status==='missed'; });
  var missedCount = missed.length;

  // Пропущені, які мають дату відпрацювання (makeup_date) — частково закриті
  var coveredMissed = missed.filter(function(l){ return l.makeup_date; }).length;
  var uncoveredMissed = missedCount - coveredMissed;

  var total = done + missedCount;
  var pct = Math.round(done / total * 100);

  // Базовий рейтинг по відсотку
  var rating;
  if(pct >= 90)      rating = 5;
  else if(pct >= 80) rating = 4;
  else if(pct >= 65) rating = 3;
  else if(pct >= 40) rating = 2;
  else               rating = 1;

  // Штраф: непокриті пропуски знижують рейтинг
  if(uncoveredMissed >= 3)      rating = Math.max(1, rating - 2);
  else if(uncoveredMissed >= 1) rating = Math.max(1, rating - 1);

  // Бонус: якщо всі пропуски відпрацьовані — рейтинг не штрафується
  if(missedCount > 0 && uncoveredMissed === 0) rating = Math.min(5, rating + 1);

  // Максимум 5 зірок тільки якщо взагалі немає непокритих пропусків
  if(uncoveredMissed > 0 && rating === 5) rating = 4;

  return rating;
}

async function updateAllTutorRatings(){
  if(!_sb||!CU) return;
  if(R()!=='god'&&R()!=='director'&&R()!=='admin') return;
  for(var i=0;i<(S.tutors||[]).length;i++){
    var t=S.tutors[i];
    var nr=calcTutorRating(t.id);
    if(nr!==t.rating){
      try{await _sb.from('tutors').update({rating:nr}).eq('id',t.id);t.rating=nr;}catch(e){}
    }
  }
}

async function logInvoice(channel,recipient,studentId,from,to,lessonsCount,total){
  if(!CU||!_sb||window._viewerMode) return;
  try{
    var r=await _sb.from('invoice_log').insert({
      sent_by:CU.id,student_id:studentId||null,
      period_from:from||null,period_to:to||null,
      lessons_count:lessonsCount||0,total_amount:total||0,
      channel:channel,recipient:recipient||'',branch_id:myBranchId()||null
    });
    if(r&&r.error) console.warn('[invoice_log] insert error:', r.error.message);
  }catch(e){ console.warn('[invoice_log] insert exception:', e&&e.message); }
}

async function renderInvoiceLog(){
  var tbody=document.getElementById('inv-log-tbody');
  if(!tbody)return;
  // Фільтр по відправнику
  var uSel=document.getElementById('ilf-user');
  if(uSel){
    var prev=uSel.value;
    uSel.innerHTML='<option value="">\u0412\u0441\u0456 \u0432\u0456\u0434\u043F\u0440\u0430\u0432\u043D\u0438\u043A\u0438</option>'
      +(S.users||[]).slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');})
        .map(function(u){return '<option value="'+u.id+'">'+u.fn+' '+u.ln+'</option>';}).join('');
    uSel.value=prev;
  }
  var fUser=(uSel||{value:''}).value;
  try{
    function buildQ(){ var q=_sb.from('invoice_log').select('*').order('sent_at',{ascending:false}).limit(200); if(fUser) q=q.eq('sent_by',fUser); return q; }
    var res=await buildQ();
    if(res.error && await refreshIfExpired(res.error)) res=await buildQ();
    if(res.error)throw res.error;
    var rows=res.data||[];
    console.log('[invoice_log] rows returned:', rows.length);
    if(!rows.length){
      tbody.innerHTML='<tr><td colspan="7" style="text-align:center;padding:20px;color:var(--t3)">'
        +'\u0429\u0435 \u043D\u0435\u043C\u0430\u0454 \u0432\u0456\u0434\u043F\u0440\u0430\u0432\u043B\u0435\u043D\u0438\u0445 \u0440\u0430\u0445\u0443\u043D\u043A\u0456\u0432.<br>'
        +'<span style="font-size:11px">\u0417\u0430\u043F\u0438\u0441 \u0437\u2019\u044F\u0432\u043B\u044F\u0454\u0442\u044C\u0441\u044F \u043F\u0456\u0441\u043B\u044F \u0432\u0456\u0434\u043F\u0440\u0430\u0432\u043A\u0438 \u0440\u0430\u0445\u0443\u043D\u043A\u0443 \u0447\u0435\u0440\u0435\u0437 Viber / Telegram \u0430\u0431\u043E \u043A\u043E\u043F\u0456\u044E\u0432\u0430\u043D\u043D\u044F.</span><br>'
        +'<button class="btn btn-g btn-sm" style="margin-top:8px" onclick="invoiceLogSelfTest()">\ud83e\uddea \u041F\u0435\u0440\u0435\u0432\u0456\u0440\u0438\u0442\u0438 \u0437\u0430\u043F\u0438\u0441 \u0443 \u0431\u0430\u0437\u0443</button>'
        +'</td></tr>';
      return;
    }
    tbody.innerHTML=rows.map(function(r){
      var student=(S.students||[]).find(function(s){return s.id===r.student_id;});
      var sender=(S.users||[]).find(function(u){return u.id===r.sent_by;});
      var sentAt=r.sent_at?new Date(r.sent_at).toLocaleString('uk-UA',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'}):'—';
      var period=(r.period_from?fd(r.period_from):'')+(r.period_to?' – '+fd(r.period_to):'');
      return '<tr>'
        +'<td style="font-size:11px;color:var(--t2)">'+sentAt+'</td>'
        +'<td><b>'+(student?student.fn+' '+student.ln:'—')+'</b></td>'
        +'<td style="font-size:11px">'+period+'</td>'
        +'<td style="font-size:11px">'+(r.lessons_count||0)+' / '+(r.total_amount||0)+' грн</td>'
        +'<td><span style="font-size:11px;padding:2px 8px;border-radius:20px;background:rgba(41,171,226,.15);color:var(--adm)">'+(r.channel||'')+'</span></td>'
        +'<td style="font-size:11px">'+(r.recipient||'—')+'</td>'
        +'<td style="font-size:11px;color:var(--t2)">'+(sender?sender.fn+' '+sender.ln:'—')+'</td>'
        +'</tr>';
    }).join('');
  }catch(e){
    console.error('[invoice_log] read error:', e);
    tbody.innerHTML='<tr><td colspan="7" style="color:var(--danger);padding:16px;text-align:center">'
      +'\u041F\u043E\u043C\u0438\u043B\u043A\u0430 \u0447\u0438\u0442\u0430\u043D\u043D\u044F \u0437 \u0431\u0430\u0437\u0438: '+(e.message||e)+'<br>'
      +'<span style="font-size:11px;color:var(--t2)">\u0406\u043C\u043E\u0432\u0456\u0440\u043D\u043E \u0432\u0456\u0434\u0441\u0443\u0442\u043D\u044F \u0442\u0430\u0431\u043B\u0438\u0446\u044F invoice_log \u0430\u0431\u043E RLS-\u043F\u043E\u043B\u0456\u0442\u0438\u043A\u0430 SELECT. \u0414\u0438\u0432. \u0456\u043D\u0441\u0442\u0440\u0443\u043A\u0446\u0456\u044E.</span>'
      +'</td></tr>';
  }
}

// Самотест: пише тестовий запис у invoice_log і одразу читає назад — показує точну причину
async function invoiceLogSelfTest(){
  if(!_sb){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u0437\u2019\u0454\u0434\u043D\u0430\u043D\u043D\u044F','error'); return; }
  if(window._viewerMode){ mkToast('\u0420\u0435\u0436\u0438\u043C \u043F\u0435\u0440\u0435\u0433\u043B\u044F\u0434\u0443 \u2014 \u0437\u0430\u043F\u0438\u0441 \u043D\u0435\u0434\u043E\u0441\u0442\u0443\u043F\u043D\u0438\u0439','error'); return; }
  mkToast('\u0422\u0435\u0441\u0442\u0443\u0454\u043C\u043E\u2026');
  try{
    var ins=await _sb.from('invoice_log').insert({
      sent_by:CU?CU.id:null, student_id:null, channel:'test',
      recipient:'\u0441\u0430\u043C\u043E\u0442\u0435\u0441\u0442', lessons_count:0, total_amount:0,
      branch_id:myBranchId()||null
    });
    if(ins.error){ alert('\u274C \u0417\u0410\u041F\u0418\u0421 \u0417\u0410\u0411\u041B\u041E\u041A\u041E\u0412\u0410\u041D\u041E:\n\n'+ins.error.message+'\n\n\u0426\u0435 \u043E\u0437\u043D\u0430\u0447\u0430\u0454, \u0449\u043E RLS-\u043F\u043E\u043B\u0456\u0442\u0438\u043A\u0430 INSERT \u0432\u0456\u0434\u0441\u0443\u0442\u043D\u044F \u0430\u0431\u043E \u0442\u0430\u0431\u043B\u0438\u0446\u0456 \u043D\u0435\u043C\u0430\u0454.'); return; }
    var sel=await _sb.from('invoice_log').select('*').eq('channel','test').limit(5);
    if(sel.error){ alert('\u26A0 \u0417\u0430\u043F\u0438\u0441 \u041F\u0420\u041E\u0419\u0428\u041E\u0412, \u0430\u043B\u0435 \u0427\u0418\u0422\u0410\u041D\u041D\u042F \u0417\u0410\u0411\u041B\u041E\u041A\u041E\u0412\u0410\u041D\u041E:\n\n'+sel.error.message+'\n\n\u041F\u043E\u0442\u0440\u0456\u0431\u043D\u0430 RLS-\u043F\u043E\u043B\u0456\u0442\u0438\u043A\u0430 SELECT.'); return; }
    alert('\u2705 \u0411\u0430\u0437\u0430 \u043F\u0440\u0430\u0446\u044E\u0454! \u0417\u0430\u043F\u0438\u0441 \u0441\u0442\u0432\u043E\u0440\u0435\u043D\u043E \u0456 \u043F\u0440\u043E\u0447\u0438\u0442\u0430\u043D\u043E ('+(sel.data?sel.data.length:0)+' \u0442\u0435\u0441\u0442. \u0437\u0430\u043F\u0438\u0441\u0456\u0432).\n\n\u041E\u0442\u0436\u0435, \u043F\u0440\u043E\u0431\u043B\u0435\u043C\u0430 \u0431\u0443\u043B\u0430 \u0432 \u0442\u043E\u043C\u0443, \u0449\u043E \u0440\u0430\u0445\u0443\u043D\u043A\u0438 \u043D\u0435 \u0432\u0456\u0434\u043F\u0440\u0430\u0432\u043B\u044F\u043B\u0438\u0441\u044C \u043F\u0456\u0441\u043B\u044F \u043E\u043D\u043E\u0432\u043B\u0435\u043D\u043D\u044F. \u0412\u0456\u0434\u043F\u0440\u0430\u0432\u0442\u0435 \u0440\u0430\u0445\u0443\u043D\u043E\u043A \u2014 \u0456 \u0432\u0456\u043D \u0437\u2019\u044F\u0432\u0438\u0442\u044C\u0441\u044F \u0442\u0443\u0442.');
    renderInvoiceLog();
  }catch(e){ alert('\u041F\u043E\u043C\u0438\u043B\u043A\u0430: '+(e.message||e)); }
}
window.invoiceLogSelfTest=invoiceLogSelfTest;
window.renderInvoiceLog=renderInvoiceLog;

function renderInvoicePage(){
  var pg=document.getElementById('pg-invoice');
  if(!pg)return;

  // Populate student select
  var sSel=document.getElementById('inv-student');
  if(sSel){
    var cur=sSel.value;
    var invStudents=[{id:'',fn:'\u2014 \u043e\u0431\u0435\u0440\u0456\u0442\u044c \u0443\u0447\u043d\u044f \u2014',ln:''}].concat(myStudents().filter(function(s){return s.status==='active'||s.status==='trial';}).sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');}));
    popSelSearch('inv-student', invStudents, 'id', function(s){return s.fn+(s.ln?' '+s.ln:'');}, '');
    if(cur){ sSel.value=cur; if(sSel._updateSearch) sSel._updateSearch(); }
  }

  var sid=(sSel||{value:''}).value;
  var student=sid?(S.students||[]).find(function(x){return x.id===sid;}):null;
  var phone=student?(student.parentPhone||student.phone||''):'';

  // Populate branch select (requisites for payment)
  var brSel=document.getElementById('inv-branch');
  if(brSel){
    var curBr=brSel.value;
    var brOpts=(S.branches||[]).map(function(b){return '<option value="'+b.id+'"'+(b.id===curBr?' selected':'')+'>'+b.name+'</option>';}).join('');
    brSel.innerHTML=brOpts || '<option value="">\u0424\u0456\u043b\u0456\u0457 \u043d\u0435 \u0441\u0442\u0432\u043e\u0440\u0435\u043d\u043e</option>';
    if(!curBr && (S.branches||[]).length){
      var defBrId = myBranchId() || S.branches[0].id;
      brSel.value = defBrId;
    }
  }
  var bid=(brSel||{value:''}).value;
  var branch=bid?(S.branches||[]).find(function(b){return b.id===bid;}):null;

  var dateFrom=(document.getElementById('inv-from')||{value:''}).value;
  var dateTo=(document.getElementById('inv-to')||{value:''}).value;
  var fallbackPrice=parseFloat((document.getElementById('inv-price')||{value:'0'}).value)||0;

  var now2=new Date();
  var defaultFrom=localDateStr(now2);
  var defaultTo=localDateStr(new Date(now2.getFullYear(),now2.getMonth()+1,0));
  if(!dateFrom) dateFrom=defaultFrom;
  if(!dateTo) dateTo=defaultTo;

  // Тільки ЗАПЛАНОВАНІ заняття — рахунок виставляється наперед (передоплата)
  var validStatuses=['planned','scheduled'];
  var lessons=sid?(S.lessons||[]).filter(function(l){
    if((l.studentId||l.student_id)!==sid) return false;
    if(validStatuses.indexOf(l.status)<0) return false;
    if(l.date<dateFrom) return false;
    if(l.date>dateTo) return false;
    return true;
  }).sort(function(a,b){return a.date.localeCompare(b.date)||(a.time||'').localeCompare(b.time||'');}):[];

  // Вартість: знімок у занятті → ставка учня за правилами (предмет/репетитор) → резервна "Ціна за годину"
  function lessonCost(l){
    var dur=(parseFloat(l.dur)||60)/60;
    var rate;
    if(l.price!=null && l.price!=='' && !isNaN(parseFloat(l.price)) && parseFloat(l.price)>0) rate=parseFloat(l.price);
    else{
      var sr=studentRate(student, l.subject, l.tutorId||l.tutor_id);
      rate=sr>0?sr:fallbackPrice;
    }
    return Math.round(dur*rate*10)/10;
  }

  var totalHours=Math.round(lessons.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  var total=Math.round(lessons.reduce(function(s,l){return s+lessonCost(l);},0)*10)/10;

  var groups={};
  var groupOrder=[];
  lessons.forEach(function(l){
    var tid=l.tutorId||l.tutor_id;
    var tutor=tid?(S.tutors||[]).find(function(t){return t.id===tid;}):null;
    var subj=l.subject||'\u0406\u043d\u0448\u0435';
    var key=(tid||'')+'|'+subj;
    if(!groups[key]){
      groups[key]={tutor:tutor, subj:subj, lessons:[]};
      groupOrder.push(key);
    }
    groups[key].lessons.push(l);
  });

  var invText='';
  if(student&&lessons.length){
    var monthsNom=['\u0421\u0456\u0447\u0435\u043D\u044C','\u041B\u044E\u0442\u0438\u0439','\u0411\u0435\u0440\u0435\u0437\u0435\u043D\u044C','\u041A\u0432\u0456\u0442\u0435\u043D\u044C','\u0422\u0440\u0430\u0432\u0435\u043D\u044C','\u0427\u0435\u0440\u0432\u0435\u043D\u044C','\u041B\u0438\u043F\u0435\u043D\u044C','\u0421\u0435\u0440\u043F\u0435\u043D\u044C','\u0412\u0435\u0440\u0435\u0441\u0435\u043D\u044C','\u0416\u043E\u0432\u0442\u0435\u043D\u044C','\u041B\u0438\u0441\u0442\u043E\u043F\u0430\u0434','\u0413\u0440\u0443\u0434\u0435\u043D\u044C'];
    var monthsGen=['\u0441\u0456\u0447\u043D\u044F','\u043B\u044E\u0442\u043E\u0433\u043E','\u0431\u0435\u0440\u0435\u0437\u043D\u044F','\u043A\u0432\u0456\u0442\u043D\u044F','\u0442\u0440\u0430\u0432\u043D\u044F','\u0447\u0435\u0440\u0432\u043D\u044F','\u043B\u0438\u043F\u043D\u044F','\u0441\u0435\u0440\u043F\u043D\u044F','\u0432\u0435\u0440\u0435\u0441\u043D\u044F','\u0436\u043E\u0432\u0442\u043D\u044F','\u043B\u0438\u0441\u0442\u043E\u043F\u0430\u0434\u0430','\u0433\u0440\u0443\u0434\u043D\u044F'];
    var _fromD=new Date(dateFrom+'T00:00:00');
    var invMonthNom=monthsNom[_fromD.getMonth()];
    var dueDate=new Date(_fromD.getFullYear(), _fromD.getMonth(), 1);
    var dueStr='1 '+monthsGen[dueDate.getMonth()];
    var cfg=S.settings||{};
    var centerName=cfg.name||'\u041A\u043E\u043D\u0441\u0442\u0430\u043D\u0442\u0430';

    var lines=[];
    lines.push('\u0412\u0430\u0441 \u0432\u0456\u0442\u0430\u0454 \u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0441\u044C\u043A\u0438\u0439 \u0446\u0435\u043D\u0442\u0440 '+centerName+'! \u0420\u0430\u0445\u0443\u043D\u043E\u043A \u043D\u0430 '+invMonthNom+':');

    groupOrder.forEach(function(key){
      var g=groups[key];
      var gCount=g.lessons.length;
      var gTotal=Math.round(g.lessons.reduce(function(s,l){return s+lessonCost(l);},0)*100)/100;
      // Ставка за годину для показу в рядку "N уроків * ЦІНА грн/год" — беремо ставку групи
      // (усі уроки групи мають один предмет+репетитор, тож ставка з правил учня спільна)
      var gTid=g.tutor?g.tutor.id:null;
      var gRate=studentRate(student, g.subj, gTid);
      var gHours=Math.round(g.lessons.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
      if(!gRate||gRate<=0){ gRate=gHours>0?Math.round(gTotal/gHours):0; }
      lines.push(g.subj);
      lines.push(gHours+' \u0433\u043E\u0434 * '+gRate+' \u0433\u0440\u043D/\u0433\u043E\u0434 =  '+gTotal+' \u0433\u0440\u043D.');
    });

    if(groupOrder.length>1){
      lines.push('\u0423\u0421\u042C\u041E\u0413\u041E \u0414\u041E \u0421\u041F\u041B\u0410\u0422\u0418: '+total+' \u0433\u0440\u043D.');
    }

    lines.push('\u041F\u043B\u0430\u0442\u0456\u0436 \u043F\u0440\u043E\u0432\u043E\u0434\u0438\u0442\u044C\u0441\u044F   \u0434\u043E '+dueStr+' \u0437\u0430 \u0442\u0430\u043A\u0438\u043C\u0438 \u0440\u0435\u043A\u0432\u0456\u0437\u0438\u0442\u0430\u043C\u0438:');
    if(branch){
      if(branch.pay_recipient) lines.push('\u041E\u0442\u0440\u0438\u043C\u0443\u0432\u0430\u0447: '+branch.pay_recipient);
      if(branch.pay_edrpou) lines.push('\u041A\u043E\u0434 \u043E\u0442\u0440\u0438\u043C\u0443\u0432\u0430\u0447\u0430: '+branch.pay_edrpou);
      if(branch.pay_card) lines.push('IBAN: '+branch.pay_card);
      if(branch.pay_bank) lines.push('\u041D\u0430\u0437\u0432\u0430 \u0431\u0430\u043D\u043A\u0443: '+branch.pay_bank);
    }
    var studentInitial=(student.fn?student.fn[0]+'.':'');
    lines.push('\u041F\u0440\u0438\u0437\u043D\u0430\u0447\u0435\u043D\u043D\u044F \u043F\u043B\u0430\u0442\u0435\u0436\u0443: \u0437\u0430 \u043E\u0441\u0432\u0456\u0442\u043D\u0456 \u043F\u043E\u0441\u043B\u0443\u0433\u0438, '+student.ln+' '+studentInitial);
    lines.push('\u0417\u0430\u0437\u0434\u0430\u043B\u0435\u0433\u0456\u0434\u044C \u0434\u044F\u043A\u0443\u0454\u043C\u043E \u0437\u0430 \u0432\u0447\u0430\u0441\u043D\u0443 \u0441\u043F\u043B\u0430\u0442\u0443 \ud83d\ude0a');

    invText=lines.join('\n');
  }

  var invEl=document.getElementById('invoice-content');
  if(!invEl)return;

  var branchSelHtml='<div class="fgr" style="margin-bottom:10px"><label>\uD83C\uDFE2 \u0424\u0456\u043b\u0456\u044f (\u0440\u0435\u043a\u0432\u0456\u0437\u0438\u0442\u0438 \u043e\u043f\u043b\u0430\u0442\u0438)</label><select id="inv-branch" onchange="renderInvoicePage()" style="font-size:12px"></select></div>';

  var leftCol = '<div>'
    + branchSelHtml
    + '<div class="fgr" style="margin-bottom:10px"><label>\u0412\u0456\u0434 \u0434\u0430\u0442\u0438</label><input type="date" id="inv-from" value="'+dateFrom+'" onchange="renderInvoicePage()" style="font-size:12px"></div>'
    + '<div class="fgr" style="margin-bottom:10px"><label>\u0414\u043e \u0434\u0430\u0442\u0438</label><input type="date" id="inv-to" value="'+dateTo+'" onchange="renderInvoicePage()" style="font-size:12px"></div>'
    + '<div class="fgr" style="margin-bottom:10px"><label>\u0426\u0456\u043d\u0430 \u0437\u0430 \u0433\u043e\u0434\u0438\u043d\u0443 \u20b4 (\u044f\u043a\u0449\u043e \u0432 \u0437\u0430\u043d\u044f\u0442\u0442\u0456 \u043d\u0435 \u0432\u043a\u0430\u0437\u0430\u043d\u0430)</label><input type="number" id="inv-price" value="'+fallbackPrice+'" onchange="renderInvoicePage()" style="font-size:12px" placeholder="400"></div>'
    + '<div style="font-size:11px;color:var(--t3);margin-bottom:10px">\uD83D\uDCA1 \u0420\u0430\u0445\u0443\u043d\u043e\u043a \u0444\u043e\u0440\u043c\u0443\u0454\u0442\u044c\u0441\u044f \u0442\u0456\u043b\u044c\u043a\u0438 \u0456\u0437 \u0417\u0410\u041f\u041b\u0410\u041d\u041e\u0412\u0410\u041d\u0418\u0425 \u0437\u0430\u043d\u044f\u0442\u044c (\u043f\u0435\u0440\u0435\u0434\u043e\u043f\u043b\u0430\u0442\u0430). \u042f\u043a\u0449\u043e \u0432 \u0437\u0430\u043d\u044f\u0442\u0442\u0456 \u0432\u043a\u0430\u0437\u0430\u043d\u0430 \u0432\u043b\u0430\u0441\u043d\u0430 \u0446\u0456\u043d\u0430 \u2014 \u0432\u043e\u043d\u0430 \u043c\u0430\u0454 \u043f\u0440\u0456\u043e\u0440\u0438\u0442\u0435\u0442. \u0420\u0430\u0445\u0443\u043d\u043e\u043a \u0430\u0432\u0442\u043e\u043c\u0430\u0442\u0438\u0447\u043d\u043e \u043e\u0431\u2019\u0454\u0434\u043d\u0443\u0454 \u0437\u0430\u043d\u044f\u0442\u0442\u044f \u0432\u0441\u0456\u0445 \u0440\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0456\u0432 \u0442\u0430 \u043f\u0440\u0435\u0434\u043c\u0435\u0442\u0456\u0432.</div>'
    + (student
      ? (phone?'<div style="font-size:12px;color:var(--t2);margin-bottom:6px">\uD83D\uDCF1 \u0422\u0435\u043b\u0435\u0444\u043e\u043d: <b>'+phone+'</b></div>':'<div style="color:var(--danger);font-size:12px;margin-bottom:6px">\u26A0\uFE0F \u0422\u0435\u043b\u0435\u0444\u043e\u043d \u043d\u0435 \u0432\u043a\u0430\u0437\u0430\u043d\u043e</div>')
      : '<div style="font-size:12px;color:var(--t3);margin-bottom:12px">\u041e\u0431\u0435\u0440\u0456\u0442\u044c \u0443\u0447\u043d\u044f \u0449\u043e\u0431 \u0441\u0444\u043e\u0440\u043c\u0443\u0432\u0430\u0442\u0438 \u0440\u0430\u0445\u0443\u043d\u043e\u043a</div>')
    + (student ? ((student.email||student.parent_email)?'<div style="font-size:12px;color:var(--t2);margin-bottom:12px">\u2709\uFE0F Email: <b>'+(student.email||student.parent_email)+'</b></div>':'<div style="color:var(--warn);font-size:12px;margin-bottom:12px">\u2709\uFE0F Email \u043d\u0435 \u0432\u043a\u0430\u0437\u0430\u043d\u043e (\u0434\u043e\u0434\u0430\u0439\u0442\u0435 \u0432 \u043a\u0430\u0440\u0442\u0446\u0456 \u0443\u0447\u043d\u044f)</div>') : '')
    + '<div style="display:flex;gap:8px;flex-wrap:wrap">'
    + (phone?'<button class="btn btn-p btn-sm" onclick="sendInvoiceViber()" style="background:#7360f2">\uD83D\uDFE3 Viber</button>':'')
    + (phone?'<button class="btn btn-p btn-sm" onclick="sendInvoiceTelegram()" style="background:#2196f3">\u2708\uFE0F Telegram</button>':'')
    + (student?'<button class="btn btn-p btn-sm" onclick="sendInvoiceEmail()" style="background:#ea4335">\u2709\uFE0F Email</button>':'')
    + '<button class="btn btn-g btn-sm" onclick="copyInvoiceText()">\uD83D\uDCCB \u041a\u043e\u043f\u0456\u044e\u0432\u0430\u0442\u0438</button>'
    + '</div>'
    + '</div>';

  // Редагований текст: якщо користувач правив, а параметри не змінились — зберігаємо його правки
  var _esc=function(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;');};
  var prevGen=window._invGenText||'';
  var keepEdit=(window._invEdited && window._invText!=null && invText===prevGen);
  var showText=keepEdit?window._invText:invText;
  if(!keepEdit) window._invEdited=false;
  window._invGenText=invText;

  var rightCol = '<div>'
    + '<textarea id="inv-text" oninput="window._invText=this.value;window._invEdited=true;" '
    + 'placeholder="'+(!sid?'Оберіть учня щоб побачити рахунок':'Немає запланованих занять за цей період')+'" '
    + 'style="width:100%;box-sizing:border-box;background:var(--s2);border-radius:10px;padding:14px;font-family:JetBrains Mono,monospace;font-size:12px;white-space:pre-wrap;line-height:1.6;min-height:260px;border:1px solid var(--b1);color:var(--t1);resize:vertical">'
    + _esc(showText)
    + '</textarea>'
    + '<div style="margin-top:8px;display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap">'
    + '<div style="font-size:12px;color:var(--t2)">Занять: '+lessons.length+' | Репетиторів: '+groupOrder.length+' | Годин: '+totalHours+' | Сума: <b>'+total+'₴</b></div>'
    + '<button class="btn btn-g btn-sm" onclick="window._invEdited=false;renderInvoicePage();" title="Повернути автоматичний текст">↺ Скинути</button>'
    + '</div>'
    + '</div>';

  invEl.innerHTML='<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">'+leftCol+rightCol+'</div>';

  // Populate branch select after innerHTML is set
  var brSel2=document.getElementById('inv-branch');
  if(brSel2){
    var brOpts2=(S.branches||[]).map(function(b){return '<option value="'+b.id+'"'+(b.id===bid?' selected':'')+'>'+b.name+'</option>';}).join('');
    brSel2.innerHTML=brOpts2 || '<option value="">\u0424\u0456\u043b\u0456\u0457 \u043d\u0435 \u0441\u0442\u0432\u043e\u0440\u0435\u043d\u043e</option>';
    if(bid) brSel2.value=bid;
  }

  window._invText=showText;
  window._invPhone=phone;
  window._invMeta={ studentId:sid||null, from:(document.getElementById('inv-from')||{}).value||dateFrom, to:(document.getElementById('inv-to')||{}).value||dateTo, lessons:lessons.length, total:total };
}

function _logInvoiceSend(channel, recipient){
  var m=window._invMeta||{};
  try{ logInvoice(channel, recipient, m.studentId, m.from, m.to, m.lessons, m.total); }catch(e){}
  // Оновити панель статусу з невеликою затримкою (щоб запис устиг зберегтись)
  setTimeout(function(){ try{ if(typeof renderInvoiceStatus==='function' && document.getElementById('inv-status-body')) renderInvoiceStatus(); }catch(e){} }, 1200);
}
function sendInvoiceViber(){
  var phone=(window._invPhone||'').replace(/\D/g,'');
  if(!phone){mkToast('Немає телефону','error');return;}
  _logInvoiceSend('viber', phone);
  var _open=function(){ window.open('viber://chat?number='+phone); };
  // Текст у саме посилання не вставляємо. Спершу копіюємо рахунок у буфер
  // обміну (поки вкладка у фокусі — так запис надійно спрацьовує), потім
  // відкриваємо чат Viber, щоб вставити вручну.
  if(navigator.clipboard && navigator.clipboard.writeText){
    navigator.clipboard.writeText(window._invText||'').then(
      function(){ mkToast('Текст скопійовано — вставте у Viber'); },
      function(){ mkToast('Не вдалося скопіювати — натисніть «Копіювати»','error'); }
    ).then(_open);
  } else {
    _open();
    mkToast('Скопіюйте текст кнопкою «Копіювати»','error');
  }
}

function sendInvoiceTelegram(){
  var phone=(window._invPhone||'').replace(/\D/g,'');
  var text=encodeURIComponent(window._invText||'');
  if(!phone){mkToast('Немає телефону','error');return;}
  // Try to open Telegram with phone
  _logInvoiceSend('telegram', phone);
  window.open('tg://resolve?phone='+phone+'&text='+text);
  // Fallback: copy to clipboard
  navigator.clipboard&&navigator.clipboard.writeText(window._invText||'').then(function(){
    mkToast('Текст скопійовано — вставте у Telegram','info');
  });
}

function copyInvoiceText(){
  var txt=window._invText||'';
  if(!txt){mkToast('Немає рахунку','error');return;}
  _logInvoiceSend('copy', (window._invMeta&&window._invMeta.studentId)?'—':'');
  navigator.clipboard?navigator.clipboard.writeText(txt).then(function(){mkToast('Скопійовано ✅');}):mkToast('Скопіюйте вручну','info');
}

window.renderInvoicePage=renderInvoicePage;
window.sendInvoiceViber=sendInvoiceViber;
window.sendInvoiceTelegram=sendInvoiceTelegram;
window.copyInvoiceText=copyInvoiceText;

// ══════════ СТАТУС РОЗСИЛКИ РАХУНКІВ ══════════
// Показує всіх учнів із запланованими заняттями за місяць і чи надіслано їм рахунок.
async function renderInvoiceStatus(){
  var body=document.getElementById('inv-status-body');
  if(!body) return;
  var perEl=document.getElementById('ivs-period');
  if(perEl && !perEl.value){ var n=new Date(); perEl.value=n.getFullYear()+'-'+String(n.getMonth()+1).padStart(2,'0'); }
  var per=(perEl&&perEl.value)||'';
  var filter=(document.getElementById('ivs-filter')||{value:''}).value;
  var brSelIv=document.getElementById('ivs-branch');
  var _canFilterBr=isSuperAdmin()||R()==='director'||R()==='admin';
  if(brSelIv && _canFilterBr){
    var _prevBrIv=brSelIv.value;
    brSelIv.innerHTML='<option value="">\u0412\u0441\u0456 \u0444\u0456\u043B\u0456\u0457</option>'
      +(S.branches||[]).slice().sort(function(a,b){return (a.name||'').localeCompare(b.name||'','uk');})
        .map(function(b){return '<option value="'+b.id+'">'+b.name+'</option>';}).join('');
    brSelIv.value=_prevBrIv;
  }
  var fBranchIv=_canFilterBr?(document.getElementById('ivs-branch')||{value:''}).value:'';
  if(!per){ body.innerHTML=''; return; }

  // Хто має заплановані заняття цього місяця (їм потрібен рахунок = передоплата)
  var need={};
  (S.lessons||[]).forEach(function(l){
    if(String(l.date||'').slice(0,7)!==per) return;
    if(!(l.status==='planned'||l.status==='scheduled'||!l.status)) return;
    var sid=l.studentId||l.student_id; if(!sid) return;
    if(!need[sid]) need[sid]={count:0};
    need[sid].count++;
  });

  // Витягуємо лог відправлень за цей місяць
  var sentMap={};
  try{
    var from=per+'-01';
    var toM=new Date(parseInt(per.split('-')[0]), parseInt(per.split('-')[1]), 0);
    var to=per+'-'+String(toM.getDate()).padStart(2,'0');
    function buildQ(){ return _sb.from('invoice_log').select('*').gte('period_from',from).lte('period_from',to+'T23:59:59').limit(1000); }
    var res=await buildQ();
    if(res.error && typeof refreshIfExpired==='function' && await refreshIfExpired(res.error)) res=await buildQ();
    // Простіший і надійніший варіант: тягнемо останні 500 і фільтруємо за датою відправлення в періоді
    if(res.error){
      res=await _sb.from('invoice_log').select('*').order('sent_at',{ascending:false}).limit(500);
    }
    (res.data||[]).forEach(function(r){
      var when=String(r.sent_at||'').slice(0,7);
      // Рахуємо рахунок «за цей місяць», якщо його період або дата відправлення потрапляють у місяць
      var pf=String(r.period_from||'').slice(0,7);
      if(r.student_id && (pf===per || when===per)){
        if(!sentMap[r.student_id]) sentMap[r.student_id]=[];
        sentMap[r.student_id].push(r);
      }
    });
  }catch(e){ console.warn('[inv-status] read error', e); }

  // Формуємо список: усі учні, кому потрібен рахунок
  // Показуємо ВСІХ активних учнів, а не лише тих, у кого є заняття цього
  // місяця. Інакше учні без занять (щойно додані, у відпустці тощо)
  // випадали зі списку, і рахунок їм ніхто не виставляв.
  var rows=(S.students||[])
    .filter(function(st){ return st.status==='active'||st.status==='trial'; })
    .map(function(st){
      return {
        st: st,
        sid: st.id,
        count: (need[st.id]||{}).count||0,
        sent: sentMap[st.id]||[]
      };
    });

  if(filter==='sent') rows=rows.filter(function(r){return r.sent.length;});
  if(filter==='notsent') rows=rows.filter(function(r){return !r.sent.length;});
  if(fBranchIv) rows=rows.filter(function(r){
    var st=r.st;
    if(Array.isArray(st.branchIds)&&st.branchIds.length) return st.branchIds.indexOf(fBranchIv)>=0;
    return (st.branchId||st.branch_id)===fBranchIv;
  });

  rows.sort(function(a,b){
    // не відіслані зверху, далі за іменем
    var sa=a.sent.length?1:0, sb=b.sent.length?1:0;
    if(sa!==sb) return sa-sb;
    return ((a.st.fn||'')+(a.st.ln||'')).localeCompare((b.st.fn||'')+(b.st.ln||''),'uk');
  });

  var sentCount=rows.filter(function(r){return r.sent.length;}).length;
  // Загальна кількість — за фактично показаними рядками, а не за тими,
  // у кого є заняття. Інакше лічильник показував менше, ніж у списку.
  var total=rows.length;

  var CH={viber:'\uD83D\uDFE3 Viber',telegram:'\u2708\uFE0F Telegram',email:'\u2709\uFE0F Email',copy:'\uD83D\uDCCB \u041a\u043e\u043f\u0456\u044f'};

  var head='<div style="display:flex;gap:16px;padding:8px 14px 12px;flex-wrap:wrap">'
    +'<div style="font-size:13px"><b style="font-size:20px;color:var(--tut)">'+sentCount+'</b> / '+total+' \u0432\u0456\u0434\u0456\u0441\u043b\u0430\u043d\u043e</div>'
    +'<div style="flex:1;min-width:120px;align-self:center"><div style="height:8px;background:var(--s3);border-radius:10px;overflow:hidden"><div style="height:100%;width:'+(total?Math.round(sentCount/total*100):0)+'%;background:var(--tut);transition:width .4s"></div></div></div>'
    +'</div>';

  if(!rows.length){ body.innerHTML=head+'<div style="padding:20px;text-align:center;color:var(--t3)">\u041d\u0435\u043c\u0430\u0454 \u0443\u0447\u043d\u0456\u0432 \u0456\u0437 \u0437\u0430\u043d\u044f\u0442\u0442\u044f\u043c\u0438 \u0446\u044c\u043e\u0433\u043e \u043c\u0456\u0441\u044f\u0446\u044f</div>'; return; }

  var list=rows.map(function(r){
    var isSent=r.sent.length>0;
    var last=isSent?r.sent.slice().sort(function(a,b){return String(b.sent_at).localeCompare(String(a.sent_at));})[0]:null;
    var av=isSent?'var(--tut)':'var(--warn)';
    var badge=isSent
      ? '<span style="background:rgba(34,181,115,.14);color:var(--tut);font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px">\u2705 \u0412\u0456\u0434\u0456\u0441\u043b\u0430\u043d\u043e</span>'
      : '<span style="background:rgba(230,126,34,.14);color:var(--warn);font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px">\u23f3 \u041d\u0435 \u0432\u0456\u0434\u0456\u0441\u043b\u0430\u043d\u043e</span>';
    var detail=isSent && last
      ? '<div style="font-size:11px;color:var(--t3)">'+(CH[last.channel]||last.channel||'')+' \u00b7 '+(last.sent_at?new Date(last.sent_at).toLocaleDateString('uk-UA'):'')+(r.sent.length>1?' \u00b7 \u0432\u0441\u044c\u043e\u0433\u043e '+r.sent.length:'')+'</div>'
      : '<div style="font-size:11px;color:var(--t3)">'+(r.count
          ? r.count+' \u0437\u0430\u043f\u043b. \u0437\u0430\u043d\u044f\u0442\u044c'
          : '\u2014 \u0431\u0435\u0437 \u0437\u0430\u043d\u044f\u0442\u044c \u0446\u044c\u043e\u0433\u043e \u043c\u0456\u0441\u044f\u0446\u044f')+'</div>';
    return '<div style="display:flex;align-items:center;gap:12px;padding:9px 14px;border-bottom:1px solid var(--s3)">'
      +'<div style="width:34px;height:34px;border-radius:10px;background:'+av+';color:#fff;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:13px;flex-shrink:0">'+((r.st.fn||' ')[0]+(r.st.ln||' ')[0]).toUpperCase()+'</div>'
      +'<div style="flex:1;min-width:0"><div style="font-weight:600;font-size:13px">'+r.st.fn+' '+r.st.ln+'</div>'+detail+'</div>'
      +badge
      +'<button class="btn btn-g btn-sm" onclick="invStatusPick(\''+r.sid+'\')" title="\u0421\u0444\u043e\u0440\u043c\u0443\u0432\u0430\u0442\u0438 \u0440\u0430\u0445\u0443\u043d\u043e\u043a">\u2192</button>'
      +'</div>';
  }).join('');

  body.innerHTML=head+list;
}
// Клік по «→» у статусі — обираємо учня у формі рахунку
function invStatusPick(sid){
  var sel=document.getElementById('inv-student');
  if(sel){ sel.value=sid; renderInvoicePage(); }
  var card=document.getElementById('pg-invoice');
  if(card) card.scrollIntoView({behavior:'smooth',block:'start'});
}
window.renderInvoiceStatus=renderInvoiceStatus;
window.invStatusPick=invStatusPick;
