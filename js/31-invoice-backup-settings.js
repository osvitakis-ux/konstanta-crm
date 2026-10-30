
// ═══════════════════════════════════════
// INVOICE (РАХУНОК-ФАКТУРА)
// ═══════════════════════════════════════

function openInvoicePanel(){
  if(R()!=='god' && R()!=='director'){
    mkToast('Доступ заборонено','error'); return;
  }
  var card = document.getElementById('inv-card');
  if(!card) return;

  // Populate student select
  var sel = document.getElementById('inv-student');
  if(sel){
    sel.innerHTML = '<option value="">— оберіть учня —</option>'
      + (S.students||[]).map(function(s){
          return '<option value="'+s.id+'">'+s.fn+' '+s.ln+'</option>';
        }).join('');
  }

  // Default period: current month
  var now = new Date();
  var y = now.getFullYear(), m = now.getMonth();
  document.getElementById('inv-date-from').value = y+'-'+String(m+1).padStart(2,'0')+'-01';
  document.getElementById('inv-date-to').value   = y+'-'+String(m+1).padStart(2,'0')+'-'+new Date(y,m+1,0).getDate();
  document.getElementById('inv-price').value     = '';
  document.getElementById('inv-email').value     = '';
  document.getElementById('inv-notes').value     = '';

  // Load saved payment details
  var cfg = S.settings||{};
  var payEl = document.getElementById('inv-payment');
  if(payEl) payEl.value = cfg.payment_details||'';

  document.getElementById('inv-preview').innerHTML = '';
  card.style.display = 'block';
  card.scrollIntoView({behavior:'smooth', block:'start'});
}


function calcInvoiceLessons(){
  var selEl = document.getElementById('inv-student');
  var sid   = selEl ? selEl.value : (S._invoiceStudentId||'');
  var from  = document.getElementById('inv-date-from').value;
  var to    = document.getElementById('inv-date-to').value;
  var price = parseFloat(document.getElementById('inv-price').value)||0;

  // Count PLANNED (not yet conducted) lessons
  var lessons = (S.lessons||[]).filter(function(l){
    return (l.studentId===sid||l.student_id===sid)
      && (l.status==='planned'||l.status==='scheduled')
      && l.date >= from && l.date <= to;
  });

  var totalHours = Math.round(lessons.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
  var total = Math.round(totalHours * price * 10)/10;
  var el = document.getElementById('inv-preview');
  if(!el) return;

  if(!lessons.length){
    el.innerHTML = '<div style="color:var(--t3);font-size:12px;padding:8px 0">'
      +'\u041d\u0435\u043c\u0430\u0454 \u0437\u0430\u043f\u043b\u0430\u043d\u043e\u0432\u0430\u043d\u0438\u0445 \u0443\u0440\u043e\u043a\u0456\u0432 \u0437\u0430 \u0446\u0435\u0439 \u043f\u0435\u0440\u0456\u043e\u0434</div>';
    return;
  }

  // Sort by date+time
  lessons.sort(function(a,b){ return (a.date+' '+(a.time||'')).localeCompare(b.date+' '+(b.time||'')); });

  var rows = lessons.map(function(l, i){
    var tutor = l.tutorId ? (S.tutors||[]).find(function(t){return t.id===l.tutorId;}) : null;
    return '<tr>'
      +'<td>'+(i+1)+'</td>'
      +'<td>'+fd(l.date)+'</td>'
      +'<td>'+(l.time||'\u2014')+'</td>'
      +'<td>'+(l.subject||l.notes||'\u2014')+'</td>'
      +'<td>'+(tutor ? tutor.fn+' '+tutor.ln : '\u2014')+'</td>'
      +'<td style="text-align:right">'+(price ? price+' \u0433\u0440\u043d' : '\u2014')+'</td>'
      +'</tr>';
  }).join('');

  el.innerHTML = '<table class="inv-table">'
    +'<thead><tr>'
    +'<th style="width:28px">#</th>'
    +'<th>\u0414\u0430\u0442\u0430</th>'
    +'<th>\u0427\u0430\u0441</th>'
    +'<th>\u041f\u0440\u0435\u0434\u043c\u0435\u0442</th>'
    +'<th>\u0420\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440</th>'
    +'<th style="text-align:right">\u0421\u0443\u043c\u0430</th>'
    +'</tr></thead>'
    +'<tbody>'+rows+'</tbody>'
    +'<tfoot><tr>'
    +'<td colspan="5" style="font-weight:700">'
      +'\u0420\u0410\u0417\u041e\u041c: '+lessons.length+' \u0443\u0440\u043e\u043a'+'\u0456\u0432'
    +'</td>'
    +'<td style="text-align:right;font-weight:700;color:var(--adm)">'
      +(price ? total+' \u0433\u0440\u043d' : '\u2014')
    +'</td>'
    +'</tr></tfoot>'
    +'</table>';
}

function sendInvoiceEmail(){
  var m = window._invMeta || {};
  var s = m.studentId ? (S.students||[]).find(function(x){return x.id===m.studentId;}) : null;
  if(!s){ mkToast('\u041e\u0431\u0435\u0440\u0456\u0442\u044c \u0443\u0447\u043d\u044f','error'); return; }
  var email = (s.email||s.parent_email||'').trim();
  if(!email){
    email = prompt('Email \u043e\u0442\u0440\u0438\u043c\u0443\u0432\u0430\u0447\u0430 (\u043d\u0435 \u0432\u043a\u0430\u0437\u0430\u043d\u043e \u0432 \u043a\u0430\u0440\u0442\u0446\u0456):', '');
    if(!email) return;
  }
  var cfg = S.settings||{};
  var subj = '\u0420\u0430\u0445\u0443\u043d\u043e\u043a \u043d\u0430 \u043e\u043f\u043b\u0430\u0442\u0443'
    + (cfg.name?' \u2014 '+cfg.name:'')
    + (m.from||m.to?' ('+(m.from?fd(m.from)+' \u2013 ':'')+(m.to?fd(m.to):'')+')':'');
  var body = window._invText || '';
  _logInvoiceSend('email', email);
  var href = 'mailto:'+encodeURIComponent(email)
    + '?subject='+encodeURIComponent(subj)
    + '&body='+encodeURIComponent(body);
  window.location.href = href;
  mkToast('\u0412\u0456\u0434\u043a\u0440\u0438\u0432\u0430\u0454\u043c\u043e \u043f\u043e\u0448\u0442\u043e\u0432\u0438\u0439 \u043a\u043b\u0456\u0454\u043d\u0442\u2026');
}

window.calcInvoiceLessons = calcInvoiceLessons;
window.sendInvoiceEmail = sendInvoiceEmail;
window.openInvoicePanel = openInvoicePanel;
window.updateInvPhone = updateInvPhone;
// Boot
function calcPrice(subjectName, tutorId, grade, dur){
  // Match rules by specificity: most specific wins
  const rules = S.pricingRules || [];
  if(!rules.length){
    // Fallback: subject base price
    const subj = (S.subjects||[]).find(s=>s.name===subjectName);
    return subj && subj.price ? parseFloat(subj.price) : 0;
  }
  // Score each rule: +3 subject, +2 tutor, +1 grade, +1 dur
  let best = null, bestScore = -1;
  rules.forEach(function(r){
    if(!r.price) return;
    var score = 0, match = true;
    if(r.subjectMatch){
      if(subjectName && subjectName.toLowerCase().includes(r.subjectMatch.toLowerCase())) score+=3;
      else { match=false; }
    }
    if(r.tutorId){
      if(r.tutorId === tutorId) score+=2;
      else { match=false; }
    }
    if(r.gradeMatch){
      var g = String(grade||'');
      if(g && g.toLowerCase().includes(r.gradeMatch.toLowerCase())) score+=1;
      else { match=false; }
    }
    if(r.durMin){
      if(parseInt(dur||60) >= parseInt(r.durMin)) score+=1;
      else { match=false; }
    }
    if(match && score >= bestScore){
      bestScore = score; best = r;
    }
  });
  return best ? parseFloat(best.price) : 0;
}


var RIGHTS_MATRIX = [
  ['Дія', 'Бог', 'Директор', 'Адмін', 'Репетитор'],
  ['Учні — перегляд',        '✅','✅','✅','✅'],
  ['Учні — редагування',     '✅','✅','✅','❌'],
  ['Репетитори — перегляд',  '✅','✅','✅','❌'],
  ['Репетитори — редагування','✅','✅','✅','❌'],
  ['Заняття — перегляд',     '✅','✅','✅','✅'],
  ['Заняття — редагування',  '✅','✅','✅','✅'],
  ['Оплата — перегляд',      '✅','✅','❌','❌'],
  ['Оплата — редагування',   '✅','✅','❌','❌'],
  ['Комунікації',            '✅','✅','✅','✅'],
  ['CRM-дошка',              '✅','✅','✅','❌'],
  ['Акаунти',                '✅','✅','❌','❌'],
  ['Налаштування',           '✅','✅','❌','❌'],
  ['Видалення будь-чого',    '✅','✅','✅','❌'],
  ['Небезпечна зона',        '✅','❌','❌','❌'],
];

// ── Резервне копіювання: статус, ручний запуск, список копій ──
/**
 * Самоперевірка резервного копіювання.
 * Порівнює, скільки записів у базі й скільки в останній копії —
 * так одразу видно, чи не випала якась таблиця.
 */
/**
 * Заглядає ВСЕРЕДИНУ файлу копії й показує, що там насправді.
 * Журнал може казати «ok», а всередині бракувати таблиць —
 * це єдиний спосіб переконатись напевно.
 */
async function inspectBackupFile(filePath){
  try{
    mkToast('\u23f3 \u0417\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0435\u043d\u043d\u044f \u043a\u043e\u043f\u0456\u0457\u2026');
    var sg=await _sb.storage.from('backups').createSignedUrl(filePath, 300);
    if(sg.error||!sg.data?.signedUrl) throw new Error(sg.error?.message||'\u041d\u0435\u043c\u0430\u0454 \u043f\u043e\u0441\u0438\u043b\u0430\u043d\u043d\u044f');
    var res=await fetch(sg.data.signedUrl);
    if(!res.ok) throw new Error('HTTP '+res.status);
    var bk=await res.json();
    var t=bk.tables||{};

    // Порівнюємо з тим, що зараз у базі
    var rows=[];
    var keys=Object.keys(t).sort();
    for(var i=0;i<keys.length;i++){
      var k=keys[i];
      var inBk=Array.isArray(t[k])?t[k].length:0;
      var inDb=null;
      try{
        var q=await _sb.from(k).select('id').limit(20000);
        inDb=(q.data||[]).length;
      }catch(e){}
      rows.push({t:k, bk:inBk, db:inDb});
    }

    var missing=rows.filter(function(r){ return r.db>0 && r.bk===0; });
    var msg='\u0424\u0430\u0439\u043b: '+filePath+'\n'
      +'\u0421\u0442\u0432\u043e\u0440\u0435\u043d\u043e: '+(bk.created_at||bk.created||'?')+'\n\n'
      +'\u0422\u0430\u0431\u043b\u0438\u0446\u044f: \u0432 \u043a\u043e\u043f\u0456\u0457 / \u0432 \u0431\u0430\u0437\u0456\n'
      +'\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n'
      +rows.map(function(r){
          var mark = (r.db>0&&r.bk===0) ? '  \u274c' : (r.bk<(r.db||0) ? '  \u26a0' : '');
          return r.t+': '+r.bk+' / '+(r.db===null?'?':r.db)+mark;
        }).join('\n');

    if(missing.length){
      msg+='\n\n\u274c \u041d\u0415 \u041f\u041e\u0422\u0420\u0410\u041f\u0418\u041b\u0418 \u0412 \u041a\u041e\u041f\u0406\u042e: '+missing.map(function(r){return r.t;}).join(', ')
        +'\n\u041f\u0435\u0440\u0435\u0434\u0435\u043f\u043b\u043e\u0439\u0442\u0435 daily-backup \u0456 \u0441\u0442\u0432\u043e\u0440\u0456\u0442\u044c \u043d\u043e\u0432\u0443 \u043a\u043e\u043f\u0456\u044e.';
    }
    alert(msg);
    console.log('[backup inspect]', rows);
  }catch(e){
    mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(e.message||e),'error');
  }
}
window.inspectBackupFile=inspectBackupFile;

async function checkBackupHealth(){
  var box=document.getElementById('backup-health');
  if(box) box.innerHTML='<div style="padding:12px;color:var(--t3);font-size:12px">\u041f\u0435\u0440\u0435\u0432\u0456\u0440\u043a\u0430\u2026</div>';
  try{
    // 1. Остання копія
    var lg=await _sb.from('backup_log').select('*').order('created_at',{ascending:false}).limit(1);
    var last=(lg.data||[])[0];
    if(!last){
      if(box) box.innerHTML='<div class="ins ins-bad"><span class="ins-ico">\u26a0\ufe0f</span>'
        +'<span class="ins-txt"><b>\u041a\u043e\u043f\u0456\u0439 \u0449\u0435 \u043d\u0435\u043c\u0430\u0454</b>'
        +'<span>\u0416\u043e\u0434\u043d\u043e\u0433\u043e \u0437\u0430\u043f\u0438\u0441\u0443 \u0432 \u0436\u0443\u0440\u043d\u0430\u043b\u0456</span></span></div>';
      return;
    }

    // 2. Скільки днів минуло
    var ageH=Math.round((Date.now()-new Date(last.created_at).getTime())/3600000);
    var ageTxt = ageH<24 ? ageH+' \u0433\u043e\u0434 \u0442\u043e\u043c\u0443'
               : Math.round(ageH/24)+' \u0434\u043d. \u0442\u043e\u043c\u0443';

    // Помилка при створенні копії — найважливіший сигнал
    if(last.status && last.status!=='ok' && last.status!=='success'){
      if(box) box.innerHTML='<div class="ins ins-bad"><span class="ins-ico">\ud83d\udd34</span>'
        +'<span class="ins-txt"><b>\u041e\u0441\u0442\u0430\u043d\u043d\u044f \u043a\u043e\u043f\u0456\u044f \u0437 \u043f\u043e\u043c\u0438\u043b\u043a\u043e\u044e</b>'
        +'<span>'+(last.error_message||last.status)+'</span></span></div>';
      return;
    }

    // 3. Порівнюємо кількість записів у ключових таблицях
    var TBL=['students','tutors','lessons','payments','call_logs'];
    var counts={};
    for(var i=0;i<TBL.length;i++){
      try{
        var r=await _sb.from(TBL[i]).select('id').limit(20000);
        counts[TBL[i]]=(r.data||[]).length;
      }catch(e){ counts[TBL[i]]=null; }
    }
    var dbTotal=Object.keys(counts).reduce(function(a,k){ return a+(counts[k]||0); },0);
    // У журналі зберігається row_counts — розбивка по таблицях,
    // а не одне число. Підсумовуємо.
    var bkTotal=0, bkTables=0;
    try{
      var rc=last.row_counts;
      if(typeof rc==='string') rc=JSON.parse(rc);
      if(rc && typeof rc==='object'){
        Object.keys(rc).forEach(function(k){
          bkTotal += parseInt(rc[k])||0;
          bkTables++;
        });
      }
    }catch(e){}

    // Копія має містити приблизно стільки ж або більше (там ще й інші таблиці)
    var ratio = dbTotal ? bkTotal/dbTotal : 0;
    var lvl = ageH>48 ? 'bad' : (ratio<0.8 ? 'warn' : 'good');
    var title = ageH>48
      ? '\u041a\u043e\u043f\u0456\u044f \u0437\u0430\u0441\u0442\u0430\u0440\u0456\u043b\u0430'
      : (ratio<0.8
          ? '\u041c\u043e\u0436\u043b\u0438\u0432\u043e, \u043d\u0435 \u0432\u0441\u0435 \u043f\u043e\u0442\u0440\u0430\u043f\u043b\u044f\u0454 \u0432 \u043a\u043e\u043f\u0456\u044e'
          : '\u0420\u0435\u0437\u0435\u0440\u0432\u043d\u0435 \u043a\u043e\u043f\u0456\u044e\u0432\u0430\u043d\u043d\u044f \u0441\u043f\u0440\u0430\u0432\u043d\u0435');

    // Найважливіша перевірка: чи є в копії ВСІ таблиці, що є в базі.
    // Саме так виявляється ситуація, коли нову таблицю додали в CRM,
    // але забули внести до списку резервного копіювання.
    var missing=[];
    try{
      var rc2=last.row_counts;
      if(typeof rc2==='string') rc2=JSON.parse(rc2);
      TBL.forEach(function(t){
        if(counts[t]>0 && rc2 && !(t in rc2)) missing.push(t);
      });
      // Перевіряємо й таблиці, доданих пізніше
      ['phone_leads','ignored_phones'].forEach(function(t){
        if(rc2 && !(t in rc2)) missing.push(t);
      });
    }catch(e){}

    if(missing.length){
      if(box) box.innerHTML='<div class="ins ins-bad"><span class="ins-ico">\ud83d\udd34</span>'
        +'<span class="ins-txt"><b>\u0423 \u043a\u043e\u043f\u0456\u044e \u043d\u0435 \u043f\u043e\u0442\u0440\u0430\u043f\u043b\u044f\u044e\u0442\u044c \u0442\u0430\u0431\u043b\u0438\u0446\u0456</b>'
        +'<span>'+missing.join(', ')+'</span>'
        +'<i>\u0426\u0456 \u0434\u0430\u043d\u0456 \u0431\u0443\u0434\u0435 \u0432\u0442\u0440\u0430\u0447\u0435\u043d\u043e \u043f\u0440\u0438 \u0432\u0456\u0434\u043d\u043e\u0432\u043b\u0435\u043d\u043d\u0456 \u2014 \u043f\u0435\u0440\u0435\u0434\u0435\u043f\u043b\u043e\u0439\u0442\u0435 daily-backup</i></span></div>';
      return;
    }

    var det=TBL.map(function(t){
      return t+': '+(counts[t]===null?'?':counts[t]);
    }).join(' \u00b7 ');

    if(box) box.innerHTML='<div class="ins ins-'+lvl+'">'
      +'<span class="ins-ico">'+(lvl==='good'?'\u2705':lvl==='warn'?'\u26a0\ufe0f':'\ud83d\udd34')+'</span>'
      +'<span class="ins-txt"><b>'+title+'</b>'
      +'<span>\u041e\u0441\u0442\u0430\u043d\u043d\u044f: '+ageTxt
        +' \u00b7 \u0437\u0430\u043f\u0438\u0441\u0456\u0432: '+bkTotal.toLocaleString('uk-UA')
        +' \u00b7 \u0442\u0430\u0431\u043b\u0438\u0446\u044c: '+bkTables+'</span>'
      +'<i>\u0412 \u0431\u0430\u0437\u0456 \u0437\u0430\u0440\u0430\u0437: '+det+'</i></span></div>';
  }catch(e){
    if(box) box.innerHTML='<div class="ins ins-bad"><span class="ins-ico">\u26a0\ufe0f</span>'
      +'<span class="ins-txt"><b>\u041d\u0435 \u0432\u0434\u0430\u043b\u043e\u0441\u044f \u043f\u0435\u0440\u0435\u0432\u0456\u0440\u0438\u0442\u0438</b>'
      +'<span>'+(e.message||e)+'</span></span></div>';
  }
}
window.checkBackupHealth=checkBackupHealth;

async function renderBackupStatus(){
  var dot=document.getElementById('backup-status-dot'), txt=document.getElementById('backup-status-txt');
  if(!dot||!txt) return;
  try{
    var _r=await _sb.from('backup_log').select('*').order('created_at',{ascending:false}).limit(1);
    var last=(_r.data||[])[0];
    if(!last){
      dot.style.background='#ef4444'; txt.textContent='\u0429\u0435 \u0436\u043E\u0434\u043D\u043E\u0457 \u043A\u043E\u043F\u0456\u0457 \u043D\u0435 \u0431\u0443\u043B\u043E \u0437\u0440\u043E\u0431\u043B\u0435\u043D\u043E';
      return;
    }
    var when=new Date(last.created_at);
    var hoursAgo=(Date.now()-when.getTime())/3600000;
    var totalRows=Object.values(last.row_counts||{}).reduce(function(s,n){return s+(n||0);},0);
    var sizeKb=Math.round((last.file_size_bytes||0)/1024);
    if(last.status==='error'){
      dot.style.background='#ef4444';
      txt.textContent='\u041E\u0441\u0442\u0430\u043D\u043D\u044F \u0441\u043F\u0440\u043E\u0431\u0430 \u0437\u0430\u0432\u0435\u0440\u0448\u0438\u043B\u0430\u0441\u044C \u043F\u043E\u043C\u0438\u043B\u043A\u043E\u044E: '+(last.error_message||'');
    } else if(hoursAgo>26){
      dot.style.background='#f59e0b';
      txt.textContent='\u26A0\uFE0F \u041E\u0441\u0442\u0430\u043D\u043D\u044F \u043A\u043E\u043F\u0456\u044F \u0431\u0443\u043B\u0430 '+when.toLocaleString('uk-UA')+' \u2014 \u0434\u0430\u0432\u043D\u0456\u0448\u0435 \u0434\u043E\u0431\u0438 24 \u0433\u043E\u0434\u0438\u043D. \u041F\u0435\u0440\u0435\u0432\u0456\u0440\u0442\u0435 cron-\u0437\u0430\u0432\u0434\u0430\u043D\u043D\u044F';
    } else {
      dot.style.background='#22c55e';
      txt.textContent='\u2705 \u041E\u0441\u0442\u0430\u043D\u043D\u044F \u043A\u043E\u043F\u0456\u044F: '+when.toLocaleString('uk-UA')+' \u00B7 '+totalRows+' \u0437\u0430\u043F\u0438\u0441\u0456\u0432 \u00B7 '+sizeKb+' \u041A\u0411'+(last.github_committed?' \u00B7 \uD83D\uDCE6 GitHub \u2713':' \u00B7 \u26A0\uFE0F GitHub \u043D\u0435 \u043D\u0430\u043B\u0430\u0448\u0442\u043E\u0432\u0430\u043D\u043E');
    }
  }catch(e){
    dot.style.background='#ef4444'; txt.textContent='\u041D\u0435 \u0432\u0434\u0430\u043B\u043E\u0441\u044F \u043F\u0435\u0440\u0435\u0432\u0456\u0440\u0438\u0442\u0438 \u0441\u0442\u0430\u0442\u0443\u0441: '+(e.message||e);
  }
}

async function triggerBackupNow(){
  if(R()!=='god'){ mkToast('\u0414\u043E\u0441\u0442\u0443\u043F \u043B\u0438\u0448\u0435 \u0434\u043B\u044F \u0431\u043E\u0433\u0430 \u0441\u0438\u0441\u0442\u0435\u043C\u0438','error'); return; }
  var btn=document.getElementById('backup-now-btn');
  if(btn){ btn.disabled=true; btn.textContent='\u23F3 \u0421\u0442\u0432\u043E\u0440\u0435\u043D\u043D\u044F \u043A\u043E\u043F\u0456\u0457...'; }
  try{
    var _sess=await _sb.auth.getSession();
    var jwt=_sess?.data?.session?.access_token;
    var res=await fetch('https://rndxbvwisppxnhvrzwqi.supabase.co/functions/v1/supabase-functions-deploy-daily-backup',{
      method:'POST',
      headers:{'Content-Type':'application/json','Authorization':'Bearer '+jwt},
      body:JSON.stringify({trigger:'manual'})
    });
    var data=await res.json();
    if(!res.ok) throw new Error(data.error||'\u041F\u043E\u043C\u0438\u043B\u043A\u0430 \u0441\u0435\u0440\u0432\u0435\u0440\u0430');
    mkToast('\u2705 \u041A\u043E\u043F\u0456\u044E \u0441\u0442\u0432\u043E\u0440\u0435\u043D\u043E: '+Object.values(data.rowCounts||{}).reduce(function(s,n){return s+(n||0);},0)+' \u0437\u0430\u043F\u0438\u0441\u0456\u0432');
    renderBackupStatus();
  }catch(e){
    mkToast('\u041F\u043E\u043C\u0438\u043B\u043A\u0430: '+(e.message||e),'error');
  }finally{
    if(btn){ btn.disabled=false; btn.textContent='\uD83D\uDCBE \u0417\u0440\u043E\u0431\u0438\u0442\u0438 \u043A\u043E\u043F\u0456\u044E \u0437\u0430\u0440\u0430\u0437'; }
  }
}

async function renderBackupList(){
  var wrap=document.getElementById('backup-list');
  if(!wrap) return;
  wrap.innerHTML='<div style="padding:10px;color:var(--t3);font-size:12px">\u0417\u0430\u0432\u0430\u043D\u0442\u0430\u0436\u0435\u043D\u043D\u044F\u2026</div>';
  try{
    var _r=await _sb.from('backup_log').select('*').order('created_at',{ascending:false}).limit(30);
    var rows=_r.data||[];
    if(!rows.length){ wrap.innerHTML='<div style="padding:10px;color:var(--t3);font-size:12px">\u041A\u043E\u043F\u0456\u0439 \u0449\u0435 \u043D\u0435\u043C\u0430\u0454</div>'; return; }
    var html='<table style="width:100%;font-size:12px"><thead><tr><th style="text-align:left">\u0414\u0430\u0442\u0430</th><th>\u0417\u0430\u043F\u0438\u0441\u0456\u0432</th><th>\u0420\u043E\u0437\u043C\u0456\u0440</th><th>GitHub</th><th></th></tr></thead><tbody>';
    rows.forEach(function(r){
      var totalRows=Object.values(r.row_counts||{}).reduce(function(s,n){return s+(n||0);},0);
      var sizeKb=Math.round((r.file_size_bytes||0)/1024);
      html+='<tr>'
        +'<td>'+new Date(r.created_at).toLocaleString('uk-UA')+'</td>'
        +'<td style="text-align:center">'+(r.status==='error'?'\u274C':totalRows)+'</td>'
        +'<td style="text-align:center">'+sizeKb+' \u041A\u0411</td>'
        +'<td style="text-align:center">'+(r.github_committed?'\u2705':'\u2014')+'</td>'
        +'<td style="text-align:center">'
          +'<button class="btn btn-g btn-sm" onclick="inspectBackupFile(\''+r.file_path+'\')" title="\u041f\u043e\u0434\u0438\u0432\u0438\u0442\u0438\u0441\u044c, \u0449\u043e \u0432\u0441\u0435\u0440\u0435\u0434\u0438\u043d\u0456">\ud83d\udd0d</button> '
          +'<button class="btn btn-g btn-sm" onclick="downloadBackup(\''+r.file_path+'\')">\u2B07</button> '
          +'<button class="btn btn-p btn-sm" onclick="restoreFromBackup(\''+r.file_path+'\')">\u267B\uFE0F</button></td>'
      +'</tr>';
    });
    html+='</tbody></table>';
    wrap.innerHTML=html;
  }catch(e){ wrap.innerHTML='<div style="padding:10px;color:var(--danger);font-size:12px">\u041F\u043E\u043C\u0438\u043B\u043A\u0430: '+(e.message||e)+'</div>'; }
}

async function downloadBackup(filePath){
  try{
    var _r=await _sb.storage.from('backups').createSignedUrl(filePath,60);
    if(_r.error) throw _r.error;
    window.open(_r.data.signedUrl,'_blank');
  }catch(e){ mkToast('\u041F\u043E\u043C\u0438\u043B\u043A\u0430 \u0437\u0430\u0432\u0430\u043D\u0442\u0430\u0436\u0435\u043D\u043D\u044F: '+(e.message||e),'error'); }
}

async function restoreFromBackup(filePath){
  if(R()!=='god'){ mkToast('\u0414\u043E\u0441\u0442\u0443\u043F \u043B\u0438\u0448\u0435 \u0434\u043B\u044F \u0431\u043E\u0433\u0430 \u0441\u0438\u0441\u0442\u0435\u043C\u0438','error'); return; }
  if(!confirm('\u0412\u0456\u0434\u043D\u043E\u0432\u0438\u0442\u0438 \u0434\u0430\u043D\u0456 \u0437 \u0446\u0456\u0454\u0457 \u043A\u043E\u043F\u0456\u0457?\n\n'
    +'\u0426\u0435 \u0411\u0415\u0417\u041F\u0415\u0427\u041D\u041E: \u0431\u0443\u0434\u0443\u0442\u044C \u043F\u043E\u0432\u0435\u0440\u043D\u0435\u043D\u0456 \u041B\u0418\u0428\u0415 \u0442\u0456 \u0437\u0430\u043F\u0438\u0441\u0438, \u044F\u043A\u0438\u0445 \u0417\u0410\u0420\u0410\u0417 \u041D\u0415\u041C\u0410\u0404 \u0432 \u0431\u0430\u0437\u0456 (\u0437\u0430 id). '
    +'\u0406\u0441\u043D\u0443\u044E\u0447\u0456 \u0437\u0430\u043F\u0438\u0441\u0438 \u041D\u0415 \u0431\u0443\u0434\u0443\u0442\u044C \u043F\u0435\u0440\u0435\u043F\u0438\u0441\u0430\u043D\u0456 \u0447\u0438 \u0437\u0430\u0434\u0432\u043E\u0454\u043D\u0456. \u041D\u0456\u0447\u043E\u0433\u043E \u043D\u0435 \u0432\u0438\u0434\u0430\u043B\u044F\u0454\u0442\u044C\u0441\u044F.')) return;

  var loadingToast=mkToast('\u23F3 \u0412\u0456\u0434\u043D\u043E\u0432\u043B\u0435\u043D\u043D\u044F...');
  try{
    var _sess=await _sb.auth.getSession();
    var jwt=_sess?.data?.session?.access_token;
    var res=await fetch('https://rndxbvwisppxnhvrzwqi.supabase.co/functions/v1/restore-backup',{
      method:'POST',
      headers:{'Content-Type':'application/json','Authorization':'Bearer '+jwt},
      body:JSON.stringify({file_path:filePath})
    });
    var data=await res.json();
    if(!res.ok) throw new Error(data.error||'\u041F\u043E\u043C\u0438\u043B\u043A\u0430 \u0441\u0435\u0440\u0432\u0435\u0440\u0430');

    var restored=data.restoredCounts||{};
    var totalRestored=Object.values(restored).reduce(function(s,n){return s+(n||0);},0);
    var details=Object.keys(restored).filter(function(t){return restored[t]>0;})
      .map(function(t){return t+': +'+restored[t];}).join(', ');

    // ВАЖЛИВО: розрізняємо «нічого не треба було відновлювати» і
    // «спроба була, але вставка впала». Раніше обидва випадки
    // показували заспокійливе повідомлення, і причина губилась.
    var errs=data.errors||{};
    var errKeys=Object.keys(errs).filter(function(k){ return errs[k]; });
    var failed=data.failedSamples||[];

    if(errKeys.length || failed.length){
      var msg='\u0412\u0456\u0434\u043d\u043e\u0432\u043b\u0435\u043d\u043d\u044f \u0437\u0430\u0432\u0435\u0440\u0448\u0438\u043b\u043e\u0441\u044c \u0437 \u043f\u043e\u043c\u0438\u043b\u043a\u0430\u043c\u0438.\n\n';
      if(totalRestored) msg+='\u0412\u0456\u0434\u043d\u043e\u0432\u043b\u0435\u043d\u043e: '+totalRestored+'\n';
      if(data.failedCount) msg+='\u041d\u0435 \u0432\u0434\u0430\u043b\u043e\u0441\u044f: '+data.failedCount+'\n';
      msg+='\n\u041f\u0440\u0438\u0447\u0438\u043d\u0438:\n';
      errKeys.forEach(function(k){ msg+='\u2022 '+k+': '+errs[k]+'\n'; });
      failed.slice(0,5).forEach(function(f){
        msg+='\u2022 '+f.table+' ('+String(f.id).slice(0,8)+'\u2026): '+f.reason+'\n';
      });
      alert(msg);
      console.error('[restore] помилки:', errs, failed);
    } else if(totalRestored===0){
      mkToast('\u2139\uFE0F \u041D\u0456\u0447\u043E\u0433\u043E \u0432\u0456\u0434\u043D\u043E\u0432\u043B\u044E\u0432\u0430\u0442\u0438 \u2014 \u0443\u0441\u0456 \u0437\u0430\u043F\u0438\u0441\u0438 \u0437 \u0446\u0456\u0454\u0457 \u043A\u043E\u043F\u0456\u0457 \u0432\u0436\u0435 \u0454 \u0432 \u0431\u0430\u0437\u0456');
    } else {
      mkToast('\u2705 \u0412\u0456\u0434\u043D\u043E\u0432\u043B\u0435\u043D\u043E '+totalRestored+' \u0437\u0430\u043F\u0438\u0441\u0456\u0432 ('+details+'). \u041E\u043D\u043E\u0432\u043B\u044E\u044E \u0435\u043A\u0440\u0430\u043D\u2026');
    }
    // КРИТИЧНО: сервер уже дописав рядки в базу, але локальний стан (S.lessons тощо)
    // у браузері про це нічого не знає — без явного перезавантаження таблиць
    // розклад/списки продовжать показувати старі (кешовані) дані, ніби нічого
    // не відновилось, хоча в базі все вже на місці.
    var tablesToReload=Object.keys(restored).filter(function(t){return restored[t]>0;});
    for(var i=0;i<tablesToReload.length;i++){
      try{ await loadTableFresh(tablesToReload[i]); }catch(e){ console.warn('[restore] reload failed for', tablesToReload[i], e); }
    }
    // Оновлюємо видиму сторінку, щоб одразу побачити повернуті дані (напр. розклад)
    if(typeof nav==='function' && S.currentPage) nav(S.currentPage);
    if(S.currentPage==='schedule' && typeof renderSch==='function') renderSch();
  }catch(e){
    mkToast('\u041F\u043E\u043C\u0438\u043B\u043A\u0430 \u0432\u0456\u0434\u043D\u043E\u0432\u043B\u0435\u043D\u043D\u044F: '+(e.message||e),'error');
  }
}
window.restoreFromBackup=restoreFromBackup;
window.triggerBackupNow=triggerBackupNow;
window.renderBackupList=renderBackupList;
window.downloadBackup=downloadBackup;

function renderSettings(){
  var gcWrap = document.getElementById('god-constructor-wrap');
  if(gcWrap) gcWrap.style.display = (R()==='god') ? 'block' : 'none';
  var setNameEl=document.getElementById('set-name'); if(setNameEl) setNameEl.value=S.settings.name||'';
  var setSubjEl=document.getElementById('set-subj-list'); if(setSubjEl) setSubjEl.innerHTML=S.subjects.map((s,i)=>('<div class="ms"><span class="msl">'+(s.name)+'</span><div style="display:flex;align-items:center;gap:8px"><button class="btn btn-sm btn-d" style="padding:2px 6px" onclick="delSubj(\''+(s.id)+'\')">\u00D7</button></div></div>')).join('');
  // God-only sections
  const isGod=R()==='god';
  var gbEl=document.getElementById('god-banner-settings'); if(gbEl) gbEl.style.display=isGod?'flex':'none';
  var rsEl=document.getElementById('rights-section'); if(rsEl) rsEl.style.display=isGod?'block':'none';
  var dzEl=document.getElementById('danger-zone'); if(dzEl) dzEl.style.display=isGod?'block':'none';
  var bkEl=document.getElementById('backup-card'); if(bkEl) bkEl.style.display=isGod?'block':'none';
  try{ renderJournalReviewerSetting(); }catch(e){ console.error('renderJournalReviewerSetting:',e); }
  if(isGod){ try{ renderBackupStatus(); }catch(e){} }
  if(isGod){
    // Build rights matrix
    let rt='<thead><tr>'+RIGHTS_MATRIX[0].map((h,i)=>('<th style="'+(i===1?'color:var(--god)':i===2?'color:var(--dir)':i===3?'color:var(--adm)':i===4?'color:var(--tut)':'')+'">'+(h)+'</th>')).join('')+'</tr></thead><tbody>';
    for(let i=1;i<RIGHTS_MATRIX.length;i++){
      rt+='<tr>'+RIGHTS_MATRIX[i].map((c,j)=>('<td style="'+(c.startsWith('\u2705')?'color:var(--tut)':c.startsWith('\u274C')?'color:var(--danger)':'')+'">'+(c)+'</td>')).join('')+'</tr>';
    }
    rt+='</tbody>';
    document.getElementById('rights-table').innerHTML=rt;
  }
  renderBranches();
}
