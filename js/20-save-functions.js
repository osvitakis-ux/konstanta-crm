
// =
// SAVE FUNCTIONS
// =
async function saveStudent(){
  if(!can('students')){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432','error'); return; }
  var fn=document.getElementById('s-fn').value.trim(), ln=document.getElementById('s-ln').value.trim();
  if(!fn||!ln){ mkToast("\u0406\u043C'\u044F \u0442\u0430 \u043F\u0440\u0456\u0437\u0432\u0438\u0449\u0435 \u043E\u0431\u043E\u0432'\u044F\u0437\u043A\u043E\u0432\u0456",'error'); return; }
  var obj={
    fn, ln,
    age:    document.getElementById('s-age')?.value||null,
    grade:  document.getElementById('s-grade')?.value||'',
    rates: collectRateRows(),
    phone:  document.getElementById('s-phone')?.value||'',
    email:  document.getElementById('s-email')?.value||'',
    // Предмет і репетитори виводяться з "Окремих ставок" (legacy-поля лишаються синхронізованими для таблиць/фільтрів/доступів)
    subject:(function(){var rr=collectRateRows();var ss=[];rr.forEach(function(r){var s=(r.subject||'').trim();if(s&&ss.indexOf(s)<0)ss.push(s);});return ss.join(', ');})(),
    tutor_id:(function(){var rr=collectRateRows();var f=rr.find(function(r){return r.tutor_id;});return f?f.tutor_id:null;})(),
    tutor_ids:(function(){var rr=collectRateRows();var tt=[];rr.forEach(function(r){if(r.tutor_id&&tt.indexOf(r.tutor_id)<0)tt.push(r.tutor_id);});return tt.join(',');})(),
    status: document.getElementById('s-status')?.value||'active',
    src:    document.getElementById('s-src')?.value||'referral',
    notes:  document.getElementById('s-notes')?.value||'',
    parent_fn:   (document.getElementById('s-parent-fn')?.value||'').trim(),
    parent_phone:(document.getElementById('s-parent-phone')?.value||'').trim(),
    crm_stage: document.getElementById('s-crm-stage')?.value||'lead',
    crm_responsible: document.getElementById('s-crm-resp')?.value||null,
    branch_ids: (function(){ return Array.from(document.querySelectorAll('.s-branch-cb:checked')).map(function(el){return el.value;}).join(','); })(),
    branch_id: (function(){ var ids=Array.from(document.querySelectorAll('.s-branch-cb:checked')).map(function(el){return el.value;}); return ids.length?ids[0]:(myBranchId()||null); })(),
  };
  // ── Перша оплата (бачать/редагують лише адмін/директор) ──
  // Фіксуємо суму + МІСЯЦЬ внесення + хто вніс (для 10% у зарплату адміна).
  // Дата й автор ставляться при ПЕРШОМУ внесенні й далі не змінюються.
  var _existStud = S.editId ? (S.students||[]).find(function(x){return x.id===S.editId;}) : null;
  var _canFP = ['god','network_admin','director','admin'].indexOf(R())>=0;
  var _fpHadBefore = _existStud && _existStud.first_payment!=null && _existStud.first_payment!=='';
  if(_canFP){
    var _fpv=parseFloat(document.getElementById('s-first-pay')?.value);
    var _fpHasVal = !isNaN(_fpv)&&_fpv>0;
    // Поля надсилаємо лише якщо є сума АБО в учня вона вже була (щоб очистити).
    // Інакше не чіпаємо — тоді збереження працює навіть без колонок у БД.
    if(_fpHasVal || _fpHadBefore){
      obj.first_payment = _fpHasVal ? _fpv : null;
      if(obj.first_payment){
        if(_existStud && _existStud.first_payment_date){
          obj.first_payment_date = _existStud.first_payment_date;               // місяць не змінюємо
          obj.first_payment_by   = _existStud.first_payment_by || (CU&&CU.id) || null;
        } else {
          obj.first_payment_date = new Date().toISOString().slice(0,10);        // сьогодні
          obj.first_payment_by   = (CU&&CU.id) || null;
        }
      } else {
        obj.first_payment_date = null;
        obj.first_payment_by   = null;
      }
    }
  } else if(_fpHadBefore){
    // Репетитор без доступу редагує учня, у якого вже є перша оплата — зберігаємо
    obj.first_payment      = _existStud.first_payment;
    obj.first_payment_date = _existStud.first_payment_date||null;
    obj.first_payment_by   = _existStud.first_payment_by||null;
  }
  // Auto-link to current tutor if none selected
  if(R()==='tutor' && !obj.tutor_id){
    var mt=myTutor();
    if(mt){ obj.tutor_id=mt.id; obj.tutor_ids=mt.id;
      if(!(obj.rates||[]).length) obj.rates=[{subject:'',tutor_id:mt.id,rate:0}];
    }
  }
  window._saving = true;
  try{
    var saved;
    if(S.editId){
      saved = await dbUpdate('students',S.editId,obj);
      var norm = normalizeStudent(Object.assign({id:S.editId},obj));
      var i = S.students.findIndex(function(x){return x.id===S.editId;});
      if(i>=0) S.students[i] = norm; else S.students.push(norm);
      mkToast('\u0423\u0447\u043D\u044F \u043E\u043D\u043E\u0432\u043B\u0435\u043D\u043E');
    } else {
      var newId = uid();
      var norm = normalizeStudent(Object.assign({id:newId},obj));
      await dbInsert('students',Object.assign({id:newId},obj));
      S.students.push(norm);
      mkToast('\u0423\u0447\u043D\u044F \u0434\u043E\u0434\u0430\u043D\u043E');
    }
    closeM('mo-student'); S.editId=null;
    window._saving=false; refreshPage('students');
  }catch(e){ window._saving=false; mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(e.message||e),'error'); }
}

/**
 * Масове видалення лідів із телефонії, які так і не стали учнями.
 * Номери одразу потрапляють до списку ігнорованих, тому синхронізація
 * не створить їх повторно.
 */
async function cleanupPhoneLeads(){
  if(!can('students')){ mkToast('\u041d\u0435\u043c\u0430\u0454 \u043f\u0440\u0430\u0432','error'); return; }

  // Лід із телефонії: стадія lead + типове імʼя, яке ставить інтеграція
  var leads=(S.students||[]).filter(function(s){
    var stage=s.crmStage||s.crm_stage;
    var nm=((s.fn||'')+' '+(s.ln||'')).trim();
    return stage==='lead' && /^(\u0414\u0437\u0432\u0456\u043d\u043e\u043a|\u041d\u043e\u0432\u0438\u0439 \u043b\u0456\u0434)/i.test(nm);
  });

  if(!leads.length){ mkToast('\u041b\u0456\u0434\u0456\u0432 \u0456\u0437 \u0442\u0435\u043b\u0435\u0444\u043e\u043d\u0456\u0457 \u043d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e'); return; }

  if(!confirm('\u0417\u043d\u0430\u0439\u0434\u0435\u043d\u043e '+leads.length+' \u043b\u0456\u0434\u0456\u0432 \u0456\u0437 \u0442\u0435\u043b\u0435\u0444\u043e\u043d\u0456\u0457.\n\n'
    +'\u0412\u0438\u0434\u0430\u043b\u0438\u0442\u0438 \u0432\u0441\u0456\u0445 \u0456 \u0431\u0456\u043b\u044c\u0448\u0435 \u043d\u0435 \u0441\u0442\u0432\u043e\u0440\u044e\u0432\u0430\u0442\u0438 \u0457\u0445 \u0430\u0432\u0442\u043e\u043c\u0430\u0442\u0438\u0447\u043d\u043e?\n\n'
    +'\u0406\u0441\u0442\u043e\u0440\u0456\u044f \u0434\u0437\u0432\u0456\u043d\u043a\u0456\u0432 \u0417\u0410\u041b\u0418\u0428\u0418\u0422\u042c\u0421\u042f \u2014 \u0432\u0438\u0434\u0430\u043b\u044f\u044e\u0442\u044c\u0441\u044f \u043b\u0438\u0448\u0435 \u043a\u0430\u0440\u0442\u043a\u0438 \u0443\u0447\u043d\u0456\u0432.')) return;

  var ok=0, fail=0;
  for(var i=0;i<leads.length;i++){
    var st=leads[i];
    var tail=String(st.phone||st.parentPhone||st.parent_phone||'').replace(/\D/g,'').slice(-10);
    try{
      if(tail){
        await _sb.from('ignored_phones').upsert({
          phone: tail,
          reason: '\u041c\u0430\u0441\u043e\u0432\u0435 \u043e\u0447\u0438\u0449\u0435\u043d\u043d\u044f \u043b\u0456\u0434\u0456\u0432',
          created_by: CU?CU.id:null
        });
      }
      // ВАЖЛИВО: спершу відв'язуємо дзвінки від картки, яку видаляємо.
      // Інакше база видаляє їх разом з учнем (каскадне видалення),
      // і журнал телефонії втрачає записи.
      try{
        await _sb.from('call_logs').update({student_id:null}).eq('student_id', st.id);
      }catch(e){ console.warn('unlink calls:', e); }

      await dbDelete('students', st.id);
      ok++;
    }catch(e){ fail++; console.warn('cleanupPhoneLeads:', st.id, e); }
  }
  mkToast('\u2705 \u0412\u0438\u0434\u0430\u043b\u0435\u043d\u043e '+ok+' \u043b\u0456\u0434\u0456\u0432'+(fail?(', \u043f\u043e\u043c\u0438\u043b\u043e\u043a: '+fail):''));
  renderStudents();
}
window.cleanupPhoneLeads=cleanupPhoneLeads;

async function delStudent(id){
  if(!can('students')){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432','error'); return; }
  var st=(S.students||[]).find(function(x){return x.id===id;});
  var isLead = st && (st.crmStage==='lead' || st.crm_stage==='lead');
  var phone = st ? (st.phone||st.parentPhone||st.parent_phone||'') : '';
  var tail = String(phone).replace(/\D/g,'').slice(-10);

  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0443\u0447\u043D\u044F?')) return;

  // Якщо видаляємо ЛІДА з телефонії — запамʼятовуємо номер, щоб скрипт
  // синхронізації не створив його знову з історії дзвінків Київстару.
  var addToIgnore = false;
  if(isLead && tail){
    addToIgnore = confirm('\u0426\u0435 \u043B\u0456\u0434 \u0456\u0437 \u0442\u0435\u043b\u0435\u0444\u043e\u043d\u0456\u0457.\n\n'
      +'\u0414\u043e\u0434\u0430\u0442\u0438 \u043d\u043e\u043c\u0435\u0440 '+phone+' \u0434\u043e \u0441\u043f\u0438\u0441\u043a\u0443 \u0456\u0433\u043d\u043e\u0440\u043e\u0432\u0430\u043d\u0438\u0445, '
      +'\u0449\u043e\u0431 \u043b\u0456\u0434 \u041d\u0415 \u0441\u0442\u0432\u043e\u0440\u044e\u0432\u0430\u0432\u0441\u044f \u0437\u043d\u043e\u0432\u0443 \u043f\u0440\u0438 \u043d\u0430\u0441\u0442\u0443\u043f\u043d\u0456\u0439 \u0441\u0438\u043d\u0445\u0440\u043e\u043d\u0456\u0437\u0430\u0446\u0456\u0457?\n\n'
      +'\u041e\u041a \u2014 \u0431\u0456\u043b\u044c\u0448\u0435 \u043d\u0435 \u0441\u0442\u0432\u043e\u0440\u044e\u0432\u0430\u0442\u0438\n'
      +'\u0421\u043a\u0430\u0441\u0443\u0432\u0430\u0442\u0438 \u2014 \u0432\u0438\u0434\u0430\u043b\u0438\u0442\u0438 \u043b\u0438\u0448\u0435 \u0437\u0430\u0440\u0430\u0437');
  }

  try{
    // Відв'язуємо дзвінки, щоб вони не зникли разом із карткою
    try{
      await _sb.from('call_logs').update({student_id:null}).eq('student_id', id);
    }catch(e){ console.warn('unlink calls:', e); }

    await dbDelete('students',id);
    if(addToIgnore){
      try{
        await _sb.from('ignored_phones').upsert({
          phone: tail,
          reason: '\u0412\u0438\u0434\u0430\u043b\u0435\u043d\u0438\u0439 \u043b\u0456\u0434: '+((st.fn||'')+' '+(st.ln||'')).trim(),
          created_by: CU?CU.id:null
        });
        mkToast('\u0412\u0438\u0434\u0430\u043b\u0435\u043d\u043e. \u041d\u043e\u043c\u0435\u0440 \u0431\u0456\u043b\u044c\u0448\u0435 \u043d\u0435 \u0441\u0442\u0432\u043e\u0440\u044e\u0432\u0430\u0442\u0438\u043c\u0435 \u043b\u0456\u0434\u0456\u0432');
      }catch(e){
        mkToast('\u0412\u0438\u0434\u0430\u043b\u0435\u043d\u043e, \u0430\u043b\u0435 \u043d\u0435 \u0432\u0434\u0430\u043b\u043e\u0441\u044f \u0434\u043e\u0434\u0430\u0442\u0438 \u0432 \u0441\u043f\u0438\u0441\u043e\u043a: '+(e.message||e),'error');
      }
    } else {
      mkToast('\u0412\u0438\u0434\u0430\u043b\u0435\u043d\u043e');
    }
  }catch(e){}
}

// ── ОБ'ЄДНАННЯ ДУБЛІКАТІВ УЧНІВ ──────────────────────
// Знаходить учнів з однаковим ім'ям+прізвищем, зливає їх в один запис:
// - репетитори обох записів об'єднуються (tutor_ids)
// - порожні поля (телефон, клас, предмет тощо) заповнюються з дублікатів
// - усі заняття, платежі, комунікації та лог рахунків переносяться на основний запис
// - дублікати видаляються з бази
// Нормалізація ПІБ: ігнорує регістр, зайві пробіли, порядок "ім'я/прізвище"
// та латинські літери-двійники (i,o,a,e,c,p,x,y,k,m,t), випадково введені замість кириличних.
function _normNameKey(s){
  var raw=((s.fn||'')+' '+(s.ln||'')).toLowerCase();
  var lookalike={'a':'\u0430','e':'\u0435','i':'\u0456','o':'\u043E','p':'\u0440','c':'\u0441','x':'\u0445','y':'\u0443','k':'\u043A','m':'\u043C','t':'\u0442','b':'\u0432','h':'\u043D','n':'\u043F'};
  raw=raw.replace(/[a-z]/g,function(ch){return lookalike[ch]||ch;});
  raw=raw.replace(/['\u2019\u02BC`\u0301-]/g,'').replace(/\s+/g,' ').trim();
  // сортуємо слова — щоб "Вікторія Мілентьєва" і "Мілентьєва Вікторія" збігались
  return raw.split(' ').filter(Boolean).sort().join(' ');
}

async function mergeDuplicateStudents(){
  if(R()!=='god'&&R()!=='director'){ mkToast('\u0414\u043E\u0441\u0442\u0443\u043F\u043D\u043E \u043B\u0438\u0448\u0435 \u0434\u0438\u0440\u0435\u043A\u0442\u043E\u0440\u0443','error'); return; }
  if(window._viewerMode){ mkToast('\u0420\u0435\u0436\u0438\u043C \u043F\u0435\u0440\u0435\u0433\u043B\u044F\u0434\u0443 \u2014 \u0437\u043C\u0456\u043D\u0438 \u043D\u0435\u0434\u043E\u0441\u0442\u0443\u043F\u043D\u0456','error'); return; }

  var groups={};
  (S.students||[]).forEach(function(s){
    var key=_normNameKey(s);
    if(!key) return;
    (groups[key]=groups[key]||[]).push(s);
  });
  var dupKeys=Object.keys(groups).filter(function(k){return groups[k].length>1;});
  console.log('[merge] \u0433\u0440\u0443\u043F \u0432\u0441\u044C\u043E\u0433\u043E:',Object.keys(groups).length,'\u0434\u0443\u0431\u043B\u0456\u043A\u0430\u0442\u0456\u0432:',dupKeys.length,dupKeys.map(function(k){return groups[k].map(function(s){return s.fn+' '+s.ln+' ['+s.id+']';});}));
  if(!dupKeys.length){ mkToast('\u0414\u0443\u0431\u043B\u0456\u043A\u0430\u0442\u0456\u0432 \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E. \u042F\u043A\u0449\u043E \u0432\u043E\u043D\u0438 \u0454 \u2014 \u043F\u0435\u0440\u0435\u0432\u0456\u0440\u0442\u0435 \u043D\u0430\u043F\u0438\u0441\u0430\u043D\u043D\u044F \u041F\u0406\u0411 (F12 > Console \u2014 \u0434\u0435\u0442\u0430\u043B\u0456)'); return; }

  var summary=dupKeys.map(function(k){var g=groups[k];return '\u2022 '+g[0].fn+' '+g[0].ln+' \u2014 '+g.length+' \u0437\u0430\u043F\u0438\u0441\u0438(\u0456\u0432)';}).join('\n');
  if(!confirm('\u0417\u043D\u0430\u0439\u0434\u0435\u043D\u043E \u0434\u0443\u0431\u043B\u0456\u043A\u0430\u0442\u0438:\n'+summary+'\n\n\u041E\u0431\u02BC\u0454\u0434\u043D\u0430\u0442\u0438? \u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438 \u0431\u0443\u0434\u0443\u0442\u044C \u043E\u0431\u02BC\u0454\u0434\u043D\u0430\u043D\u0456, \u0432\u0441\u0456 \u0437\u0430\u043D\u044F\u0442\u0442\u044F, \u043F\u043B\u0430\u0442\u0435\u0436\u0456 \u0442\u0430 \u043A\u043E\u043C\u0443\u043D\u0456\u043A\u0430\u0446\u0456\u0457 \u043F\u0435\u0440\u0435\u043D\u0435\u0441\u0435\u043D\u043E \u043D\u0430 \u043E\u0434\u0438\u043D \u0437\u0430\u043F\u0438\u0441. \u0426\u0435 \u043D\u0435\u0437\u0432\u043E\u0440\u043E\u0442\u043D\u043E.')) return;

  setSaving();
  var merged=0, errors=[];
  try{
    for(var gi=0; gi<dupKeys.length; gi++){
      var list=groups[dupKeys[gi]].slice().sort(function(a,b){
        return String(a.created_at||a.createdAt||'').localeCompare(String(b.created_at||b.createdAt||''));
      });
      var primary=list[0], dups=list.slice(1);

      // Об'єднуємо репетиторів з усіх записів (без повторів, зі збереженням порядку)
      var tids=[];
      list.forEach(function(s){
        var st=(Array.isArray(s.tutorIds)&&s.tutorIds.length)?s.tutorIds:(s.tutorId?[s.tutorId]:[]);
        st.forEach(function(t){ if(t&&tids.indexOf(t)<0) tids.push(t); });
      });

      // Об'єднуємо "Окремі ставки" (rates) з УСІХ записів — унікальні за парою предмет+репетитор.
      // Без цього дублікат із власними ставками (інший предмет/репетитор) втрачав їх при об'єднанні.
      var mergedRates=[];
      list.forEach(function(s){
        studentRateRules(s).forEach(function(r){
          var key=(r.subject||'').trim().toLowerCase()+'|'+(r.tutor_id||'');
          if(!mergedRates.some(function(x){return ((x.subject||'').trim().toLowerCase()+'|'+(x.tutor_id||''))===key;})) mergedRates.push(r);
        });
      });
      var subjNames=[]; mergedRates.forEach(function(r){ if(r.subject && subjNames.indexOf(r.subject)<0) subjNames.push(r.subject); });
      var tidsFromRates=[]; mergedRates.forEach(function(r){ if(r.tutor_id && tidsFromRates.indexOf(r.tutor_id)<0) tidsFromRates.push(r.tutor_id); });
      var finalTutorIds=tidsFromRates.length?tidsFromRates:tids;

      // Об'єднуємо філії (учень/репетитор може працювати в кількох одночасно)
      var brIds=[];
      list.forEach(function(s){
        var bi=(Array.isArray(s.branchIds)&&s.branchIds.length)?s.branchIds:(s.branchId||s.branch_id?[s.branchId||s.branch_id]:[]);
        bi.forEach(function(b){ if(b&&brIds.indexOf(b)<0) brIds.push(b); });
      });

      // Порожні поля основного запису заповнюємо даними з дублікатів
      var patch={
        tutor_ids: finalTutorIds.join(','), tutor_id: finalTutorIds[0]||null,
        rates: mergedRates,
        subject: mergedRates.length?subjNames.join(', '):undefined, // якщо є ставки — предмет виводиться з них
        branch_ids: brIds.join(','), branch_id: brIds[0]||null
      };
      if(patch.subject===undefined) delete patch.subject; // без ставок — не чіпаємо legacy-поле subject нижче (заповниться з дублікатів як і раніше)
      if(!mergedRates.length){
        // Жодних "Окремих ставок" у жодного з дублікатів — об'єднуємо старе текстове поле subject
        var subjSet=[];
        list.forEach(function(s){ (s.subject||'').split(',').map(function(x){return x.trim();}).filter(Boolean).forEach(function(x){ if(subjSet.indexOf(x)<0) subjSet.push(x); }); });
        if(subjSet.length) patch.subject=subjSet.join(', ');
      }
      var fields=[['phone','phone'],['email','email'],['grade','grade'],['age','age'],['notes','notes'],['parent_fn','parentFn'],['parent_phone','parentPhone'],['status','status']];
      fields.forEach(function(f){
        var col=f[0], camel=f[1];
        if(!primary[camel]&&!primary[col]){
          var d=dups.find(function(x){return x[camel]||x[col];});
          if(d) patch[col]=d[camel]||d[col];
        }
      });

      var ru=await _sb.from('students').update(patch).eq('id',primary.id);
      if(ru.error && await refreshIfExpired(ru.error)) ru=await _sb.from('students').update(patch).eq('id',primary.id);
      if(ru.error){ errors.push(primary.fn+' '+primary.ln+': '+ru.error.message); continue; }

      // Переносимо пов'язані записи та видаляємо дублікати
      var ok=true;
      for(var di=0; di<dups.length; di++){
        var dup=dups[di];
        var tables=['lessons','payments','comms','invoice_log'];
        for(var ti=0; ti<tables.length; ti++){
          var up=await _sb.from(tables[ti]).update({student_id:primary.id}).eq('student_id',dup.id);
          if(up.error && await refreshIfExpired(up.error)) up=await _sb.from(tables[ti]).update({student_id:primary.id}).eq('student_id',dup.id);
          // invoice_log може бути відсутнім у частині інсталяцій — пропускаємо помилку "таблиці немає"
          if(up.error && tables[ti]!=='invoice_log'){ errors.push(dup.fn+' '+dup.ln+' ('+tables[ti]+'): '+up.error.message); ok=false; break; }
        }
        if(!ok) break;
        var del=await _sb.from('students').delete().eq('id',dup.id);
        if(del.error && await refreshIfExpired(del.error)) del=await _sb.from('students').delete().eq('id',dup.id);
        if(del.error){ errors.push(dup.fn+' '+dup.ln+': '+del.error.message); ok=false; break; }
      }
      if(ok) merged++;
    }
  }catch(e){ errors.push(e.message||String(e)); }

  // Перезавантажуємо зачеплені таблиці та оновлюємо інтерфейс
  await loadTableFresh('students');
  await loadTableFresh('lessons');
  await loadTableFresh('payments');
  await loadTableFresh('comms');

  if(errors.length){ console.error('[merge] \u043F\u043E\u043C\u0438\u043B\u043A\u0438:',errors); mkToast('\u041E\u0431\u02BC\u0454\u0434\u043D\u0430\u043D\u043E: '+merged+', \u043F\u043E\u043C\u0438\u043B\u043A\u0438: '+errors[0],'error'); }
  else mkToast('\u041E\u0431\u02BC\u0454\u0434\u043D\u0430\u043D\u043E \u0433\u0440\u0443\u043F: '+merged);
  setSynced();
}

// \u0411\u0435\u0437\u043f\u0435\u0447\u043d\u0435 \u0432\u0438\u0434\u0430\u043b\u0435\u043d\u043d\u044f \u043b\u0456\u0434\u0430 \u0437 CRM-\u0434\u043e\u0448\u043a\u0438:
// - \u044f\u043a\u0449\u043e \u0446\u0435 \u0449\u0435 \u043d\u0435 \u0430\u043a\u0442\u0438\u0432\u043d\u0438\u0439 \u0443\u0447\u0435\u043d\u044c (\u043d\u0435 status==='active') \u2014 \u0432\u0438\u0434\u0430\u043b\u044f\u0454\u043c\u043e\u0441\u044f \u0444\u0456\u0437\u0438\u0447\u043d\u043e
// - \u044f\u043a\u0449\u043e \u0432\u0436\u0435 \u0430\u043a\u0442\u0438\u0432\u043d\u0438\u0439 \u0443\u0447\u0435\u043d\u044c \u2014 \u043b\u0438\u0448\u0435 \u043f\u0440\u0438\u0431\u0438\u0440\u0430\u0454\u043c\u043e \u0437 CRM-\u0434\u043e\u0448\u043a\u0438 (\u0441\u0442\u0430\u0432\u0438\u043c\u043e \u0435\u0442\u0430\u043f lost), \u0441\u0430\u043c \u0443\u0447\u0435\u043d\u044c \u0437\u0430\u043b\u0438\u0448\u0430\u0454\u0442\u044c\u0441\u044f
async function delLead(id){
  if(!can('students')){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432','error'); return; }
  var s=(S.students||[]).find(function(x){return x.id===id;});
  if(!s){mkToast('\u041b\u0456\u0434 \u043d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e','error');return;}

  // \u0412\u0438\u0434\u0430\u043b\u0435\u043d\u043d\u044f \u0437 CRM \u043d\u0456\u043a\u043e\u043b\u0438 \u043d\u0435 \u0432\u0438\u0434\u0430\u043b\u044f\u0454 \u0441\u0430\u043c\u043e\u0433\u043e \u0443\u0447\u043d\u044f \u0437 \u0441\u0438\u0441\u0442\u0435\u043c\u0438.
  // \u041c\u0438 \u043f\u0440\u043e\u0441\u0442\u043e \u043f\u043e\u0437\u043d\u0430\u0447\u0430\u0454\u043c\u043e crm_stage='removed' \u2014 \u0442\u0430\u043a\u0438\u0439 \u0437\u0430\u043f\u0438\u0441 \u0431\u0456\u043b\u044c\u0448\u0435 \u043d\u0435 \u0437'\u044f\u0432\u043b\u044f\u0454\u0442\u044c\u0441\u044f \u043d\u0456 \u0432 \u043e\u0434\u043d\u0456\u0439 \u043a\u043e\u043b\u043e\u043d\u0446\u0456 \u0434\u043e\u0448\u043a\u0438,
  // \u0430\u043b\u0435 \u0441\u0430\u043c \u0443\u0447\u0435\u043d\u044c (\u0437 \u0443\u0441\u0456\u043c\u0430 \u0437\u0430\u043d\u044f\u0442\u0442\u044f\u043c\u0438/\u043f\u043b\u0430\u0442\u0435\u0436\u0430\u043c\u0438) \u043b\u0438\u0448\u0430\u0454\u0442\u044c\u0441\u044f \u0432 \u0441\u0438\u0441\u0442\u0435\u043c\u0456 \u0431\u0435\u0437 \u0437\u043c\u0456\u043d.
  if(!confirm('\u0412\u0438\u0434\u0430\u043b\u0438\u0442\u0438 \u00ab'+s.fn+' '+s.ln+'\u00bb \u0437 CRM-\u0434\u043e\u0448\u043a\u0438? \u0421\u0430\u043c \u0443\u0447\u0435\u043d\u044c \u0437\u0430\u043b\u0438\u0448\u0438\u0442\u044c\u0441\u044f \u0432 \u0441\u0438\u0441\u0442\u0435\u043c\u0456, \u0437\u043c\u0456\u043d\u044e\u0454\u0442\u044c\u0441\u044f \u043b\u0438\u0448\u0435 \u0439\u043e\u0433\u043e \u043f\u043e\u043b\u043e\u0436\u0435\u043d\u043d\u044f \u0432 CRM.')) return;
  try{
    setSaving();
    var _u = await _sb.from('students').update({crm_stage:'removed', updated_at:new Date().toISOString()}).eq('id',id);
    if(_u.error){ console.error('delLead update error:', _u.error); mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+_u.error.message,'error'); return; }
    var idx=(S.students||[]).findIndex(function(x){return x.id===id;});
    if(idx>=0){ S.students[idx].crmStage='removed'; S.students[idx].crm_stage='removed'; }
    if(S.currentPage==='crm') renderCrm();
    mkToast('\u0412\u0438\u0434\u0430\u043b\u0435\u043d\u043e \u0437 CRM (\u0443\u0447\u0435\u043d\u044c \u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043d\u043e)');
  }catch(e){ console.error('delLead exception:', e); mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(e.message||e),'error'); }
  finally{ setSynced(); }
}
window.delLead = delLead;

async function saveTutor(){
  var fn=document.getElementById('t-fn').value.trim(), ln=document.getElementById('t-ln').value.trim();
  if(!fn||!ln){ mkToast("\u0406\u043C'\u044F \u0442\u0430 \u043F\u0440\u0456\u0437\u0432\u0438\u0449\u0435 \u043E\u0431\u043E\u0432'\u044F\u0437\u043A\u043E\u0432\u0456",'error'); return; }
  var obj={
    fn, ln,
    phone:  document.getElementById('t-phone')?.value||'',
    email:  document.getElementById('t-email')?.value||'',
    subj:   document.getElementById('t-subj')?.value||'',
    rate:   document.getElementById('t-rate')?.value||null,
    bio:    document.getElementById('t-bio')?.value||'',
    rating: parseInt(document.getElementById('t-rating')?.value)||5,
    acc_uid: document.getElementById('t-acc')?.value||null,
    branch_ids: (function(){ return Array.from(document.querySelectorAll('.t-branch-cb:checked')).map(function(el){return el.value;}).join(','); })(),
    branch_id: (function(){ var ids=Array.from(document.querySelectorAll('.t-branch-cb:checked')).map(function(el){return el.value;}); return ids.length?ids[0]:(myBranchId()||null); })(),
  };
  window._saving = true;
  try{
    if(S.editId){ await dbUpdate('tutors',S.editId,obj); mkToast('\u041E\u043D\u043E\u0432\u043B\u0435\u043D\u043E'); }
    else         { await dbInsert('tutors',Object.assign({id:uid()},obj)); mkToast('\u0412\u0438\u043A\u043B\u0430\u0434\u0430\u0447\u0430 \u0434\u043E\u0434\u0430\u043D\u043E'); }
    closeM('mo-tutor'); S.editId=null; window._saving=false; refreshPage('tutors');
  }catch(e){ window._saving=false; mkToast('Помилка: '+(e.message||e),'error'); }
}

async function delTutor(id){
  if(!can('tutors')){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432','error'); return; }
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0432\u0438\u043A\u043B\u0430\u0434\u0430\u0447\u0430?')) return;
  try{ await dbDelete('tutors',id); mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); }catch(e){}
}

async function saveLesson(){
  if(S.editId && lessonLocked(S.editId)){
    if(journalFieldsLocked(S.editId)){ mkToast('\u0417\u0430\u043d\u044f\u0442\u0442\u044f \u0437\u0430\u043c\u043e\u0440\u043e\u0436\u0435\u043d\u0435 \u2014 \u0440\u0435\u0434\u0430\u0433\u0443\u0454 \u0430\u0434\u043c\u0456\u043d\u0456\u0441\u0442\u0440\u0430\u0442\u043e\u0440 \u0430\u0431\u043e \u0434\u0438\u0440\u0435\u043a\u0442\u043e\u0440 \u2014 \u0437\u043c\u0456\u043d\u0438 \u0437\u0430\u0431\u043e\u0440\u043e\u043d\u0435\u043d\u0456','error'); return; }
    // Минулий тиждень, але ще ПОТОЧНИЙ місяць — дозволяємо правити ЛИШЕ журнал (тема/ДЗ/література)
    var _jid=S.editId;
    var _jf={ notes:document.getElementById('l-notes')?.value||'', hw:document.getElementById('l-hw')?.value||null, literature:document.getElementById('l-lit')?.value||null, games:document.getElementById('l-games')?.value||null };
    try{
      await dbUpdate('lessons', _jid, _jf);
      var _ll=(S.lessons||[]).find(function(x){return x.id===_jid;});
      if(_ll){ _ll.notes=_jf.notes; _ll.hw=_jf.hw; _ll.literature=_jf.literature; _ll.games=_jf.games; }
      mkToast('\u0416\u0443\u0440\u043d\u0430\u043b \u043e\u043d\u043e\u0432\u043b\u0435\u043d\u043e (\u0442\u0435\u043c\u0430 / \u0414\u0417 / \u043b\u0456\u0442\u0435\u0440\u0430\u0442\u0443\u0440\u0430)');
      closeM('mo-lesson'); S.editId=null;
      try{ if(typeof renderSch==='function') renderSch(); }catch(e){}
    }catch(e){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(e.message||e),'error'); }
    return;
  }
  var stdEl=document.getElementById('l-std'); 
  var dateEl=document.getElementById('l-date');
  var studentId=stdEl?stdEl.value:''; 
  var date=dateEl?dateEl.value:'';
  // Репетитор не може ПЕРЕМІЩУВАТИ вже створене ПЛАНОВЕ заняття — фіксуємо день/час як в оригіналі.
  var _lockMove = S.editId && !canMoveLessons();
  var _origMove = _lockMove ? (S.lessons||[]).find(function(x){return x.id===S.editId;}) : null;
  if(_origMove && !isPlannedLesson(_origMove)) _origMove=null; // тільки планові
  if(_origMove){ date = _origMove.date || date; }
  if(!studentId||!date){ mkToast("\u0423\u0447\u0435\u043D\u044C \u0442\u0430 \u0434\u0430\u0442\u0430 \u043E\u0431\u043E\u0432'\u044F\u0437\u043A\u043E\u0432\u0456",'error'); return; }
  // Попередження про неробочий час (не блокує)
  var _wt=(S.tutors||[]).find(function(t){return t.id===(document.getElementById('l-tutor')?document.getElementById('l-tutor').value:null);});
  var _wtime=document.getElementById('l-time')?document.getElementById('l-time').value:'';
  if(_wt && _wtime && typeof tutorAvail==='function' && tutorAvail(_wt)){
    var _wh=parseInt(_wtime.split(':')[0]);
    if(!isNaN(_wh) && !isWorkingHour(_wt, dowMon1FromDate(date), _wh)){
      if(!confirm('\u26A0 '+_wt.fn+' '+_wt.ln+' \u043F\u043E\u0437\u043D\u0430\u0447\u0438\u0432(\u043B\u0430) \u0446\u0435\u0439 \u0447\u0430\u0441 \u044F\u043A \u043D\u0435\u0440\u043E\u0431\u043E\u0447\u0438\u0439. \u0412\u0441\u0435 \u043E\u0434\u043D\u043E \u043F\u043E\u0441\u0442\u0430\u0432\u0438\u0442\u0438 \u0443\u0440\u043E\u043A?')) return;
    }
  }
  var recurType = document.getElementById('l-recur')?.value||'none';
  var _stat = document.getElementById('l-stat')?.value||'planned';
  var obj={
    student_id: studentId,
    tutor_id:   document.getElementById('l-tutor')?.value||null,
    subject:    document.getElementById('l-subj')?.value||'',
    date:       date,
    time:       (_origMove ? (_origMove.time||'') : (document.getElementById('l-time')?.value||'')),
    dur:        parseInt(document.getElementById('l-dur')?.value)||60,
    price:      (function(){
      // ЗАМОРОЖУВАННЯ МИНУЛОГО: якщо редагуємо заняття, що ВЖЕ проведене, ціну не
      // чіпаємо — зміни ставок/предмета на минулі записи не впливають.
      if(S.editId){
        var _existing=(S.lessons||[]).find(function(x){return x.id===S.editId;});
        if(_existing && isConductedStatus(_existing.status) && parseFloat(_existing.price)>0){
          return parseFloat(_existing.price);
        }
      }
      // Створення / планове / момент проведення — беремо ПОТОЧНУ ставку (знімок).
      // Тест — фіксована ціна тесту.
      var _sid=document.getElementById('l-std')?.value;
      var _st=_sid?(S.students||[]).find(function(s){return s.id===_sid;}):null;
      var _computed = (_stat==='testing')
        ? payrollTestPrice()
        : studentRate(_st, document.getElementById('l-subj')?.value||'', document.getElementById('l-tutor')?.value||'');
      if(_computed>0) return _computed;
      // Немає ставки (0) — при РЕДАГУВАННІ не затираємо вже наявну ціну.
      if(S.editId){
        var _ex2=(S.lessons||[]).find(function(x){return x.id===S.editId;});
        if(_ex2 && parseFloat(_ex2.price)>0) return parseFloat(_ex2.price);
      }
      return 0;
    })(),
    status:     _stat,
    notes:      document.getElementById('l-notes')?.value||'',
    branch_id:  myBranchId()||null,
    missed_date: (_stat==='missed'||_stat==='makeup'||_stat==='makeup_planned'||_stat==='burned') ? (document.getElementById('l-miss-date')?.value||null) : null,
    makeup_date: (_stat==='makeup'||_stat==='makeup_planned') ? (document.getElementById('l-makeup-date')?.value||null) : null,
    hw:          document.getElementById('l-hw')?.value||null,
    games:       document.getElementById('l-games')?.value||null,
    literature:  document.getElementById('l-lit')?.value||null,
  };
  window._saving = true;
  try{
    if(S.editId){
      if(recurType && recurType!=='none'){
        // Перетворюємо вже існуюче одноразове заняття на серію повторюваних —
        // раніше ця гілка просто ігнорувала вибір "Повторення" при редагуванні.
        var endDate  = document.getElementById('l-recur-end')?.value;
        var count    = parseInt(document.getElementById('l-recur-count')?.value)||10;
        var interval = parseInt(document.getElementById('l-recur-interval')?.value)||1;
        var dates    = genRecurDates(date, recurType, endDate, count, interval);
        var recurId  = uid();
        // Існуюче заняття стає 0-м записом серії (дата збігається, лише додаємо recur-поля)
        var firstPatch = Object.assign({}, obj, {recur_id:recurId, recur_type:recurType, recur_index:0});
        await dbUpdate('lessons', S.editId, firstPatch);
        var _newRecurLessons=[];
        for(var i=1;i<dates.length;i++){
          var _lo2=Object.assign({id:uid()},obj,{date:dates[i],recur_id:recurId,recur_type:recurType,recur_index:i});
          await dbInsert('lessons',_lo2);
          _newRecurLessons.push(normalizeLesson(_lo2));
        }
        S.lessons=(S.lessons||[]).concat(_newRecurLessons);
        mkToast('\u041F\u0435\u0440\u0435\u0442\u0432\u043E\u0440\u0435\u043D\u043E \u043D\u0430 \u0441\u0435\u0440\u0456\u044E: '+dates.length+' \u0437\u0430\u043D\u044F\u0442\u044C'); closeM('mo-lesson');
        window._saving=false;
        refreshPage('lessons'); if(S.currentPage==='schedule') renderSch();
      } else {
        await dbUpdate('lessons',S.editId,obj); mkToast('\u041E\u043D\u043E\u0432\u043B\u0435\u043D\u043E'); closeM('mo-lesson'); window._saving=false; refreshPage('lessons'); if(S.currentPage==='schedule') renderSch();
      }
    }
    else if(recurType && recurType!=='none'){
      var endDate  = document.getElementById('l-recur-end')?.value;
      var count    = parseInt(document.getElementById('l-recur-count')?.value)||10;
      var interval = parseInt(document.getElementById('l-recur-interval')?.value)||1;
      var dates    = genRecurDates(date, recurType, endDate, count, interval);
      var recurId  = uid();
      var newLessons=[];
      for(var i=0;i<dates.length;i++){
        var _lo=Object.assign({id:uid()},obj,{date:dates[i],recur_id:recurId,recur_type:recurType,recur_index:i});
        await dbInsert('lessons',_lo);
        newLessons.push(normalizeLesson(_lo));
      }
      // Одразу додаємо в локальний стан і перемальовуємо — не чекаємо фонового loadTableFresh
      S.lessons=(S.lessons||[]).concat(newLessons);
      mkToast('\u0414\u043E\u0434\u0430\u043D\u043E '+dates.length+' \u0437\u0430\u043D\u044F\u0442\u044C'); closeM('mo-lesson');
      window._saving=false;
      refreshPage('lessons'); if(S.currentPage==='schedule') renderSch();
    } else {
      var _newLesson=Object.assign({id:uid()},obj);
      await dbInsert('lessons',_newLesson);
      // Одразу додаємо в локальний стан і перемальовуємо — не чекаємо фонового loadTableFresh
      S.lessons=(S.lessons||[]).concat([normalizeLesson(_newLesson)]);
      mkToast('\u0417\u0430\u043D\u044F\u0442\u0442\u044F \u0434\u043E\u0434\u0430\u043D\u043E'); closeM('mo-lesson');
      window._saving=false;
      refreshPage('lessons'); if(S.currentPage==='schedule') renderSch();
    }
    S.editId=null;
  }catch(e){ window._saving=false; }
}

async function delLesson(id){
  if(!can('lessons')){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u0430\u0432','error'); return; }
  var l=(S.lessons||[]).find(function(x){return x.id===id;});
  if(l && isPlannedLesson(l) && !canMoveLessons()){ mkToast('\u0412\u0438\u0434\u0430\u043b\u044f\u0442\u0438 \u043f\u043b\u0430\u043d\u043e\u0432\u0456 \u0437\u0430\u043d\u044f\u0442\u0442\u044f \u043c\u043e\u0436\u0435 \u043b\u0438\u0448\u0435 \u0430\u0434\u043c\u0456\u043d\u0456\u0441\u0442\u0440\u0430\u0442\u043e\u0440 \u0430\u0431\u043e \u0434\u0438\u0440\u0435\u043a\u0442\u043e\u0440','error'); return; }
  if(lessonLocked(id)){ mkToast('\u0417\u0430\u043d\u044f\u0442\u0442\u044f \u0437\u0430\u043c\u043e\u0440\u043e\u0436\u0435\u043d\u0435 \u2014 \u0440\u0435\u0434\u0430\u0433\u0443\u0454 \u0430\u0434\u043c\u0456\u043d\u0456\u0441\u0442\u0440\u0430\u0442\u043e\u0440 \u0430\u0431\u043e \u0434\u0438\u0440\u0435\u043a\u0442\u043e\u0440 \u2014 \u0432\u0438\u0434\u0430\u043b\u0435\u043d\u043d\u044f \u0437\u0430\u0431\u043e\u0440\u043e\u043d\u0435\u043d\u043e','error'); return; }
  if(l && l.recurId){ S.editId=id; openM('mo-del-recur'); }
  else{ if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0437\u0430\u043D\u044F\u0442\u0442\u044F?')) return; try{ await dbDelete('lessons',id); mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); }catch(e){} }
}

async function doDelLesson(mode){
  var id=S.editId, l=(S.lessons||[]).find(function(x){return x.id===id;});
  if(l && isPlannedLesson(l) && !canMoveLessons()){ mkToast('\u0412\u0438\u0434\u0430\u043b\u044f\u0442\u0438 \u043f\u043b\u0430\u043d\u043e\u0432\u0456 \u0437\u0430\u043d\u044f\u0442\u0442\u044f \u043c\u043e\u0436\u0435 \u043b\u0438\u0448\u0435 \u0430\u0434\u043c\u0456\u043d\u0456\u0441\u0442\u0440\u0430\u0442\u043e\u0440 \u0430\u0431\u043e \u0434\u0438\u0440\u0435\u043a\u0442\u043e\u0440','error'); closeM('mo-del-recur'); return; }
  if(!l){ closeM('mo-del-recur'); return; }
  closeM('mo-del-recur'); S.editId=null;
  try{
    if(mode==='one'){ await dbDelete('lessons',id); mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); }
    else if(mode==='future'){
      if(!l.recurId){ mkToast('\u0426\u0435 \u0437\u0430\u043D\u044F\u0442\u0442\u044F \u043D\u0435 \u043D\u0430\u043B\u0435\u0436\u0438\u0442\u044C \u0434\u043E \u0441\u0435\u0440\u0456\u0457 \u043f\u043e\u0432\u0442\u043e\u0440\u0435\u043d\u044c \u2014 \u0432\u0438\u0434\u0430\u043b\u044f\u044e \u043b\u0438\u0448\u0435 \u0439\u043e\u0433\u043e \u043e\u0434\u043d\u043e\u0433\u043e','error'); await dbDelete('lessons',id); return; }
      var toDelete=(S.lessons||[]).filter(function(x){return x.recurId===l.recurId&&x.recurIndex>=l.recurIndex;});
      for(var i=0;i<toDelete.length;i++) await dbDelete('lessons',toDelete[i].id);
      mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E '+toDelete.length+' \u0437\u0430\u043D\u044F\u0442\u044C');
    } else {
      if(!l.recurId){ mkToast('\u0426\u0435 \u0437\u0430\u043D\u044F\u0442\u0442\u044F \u043D\u0435 \u043D\u0430\u043B\u0435\u0436\u0438\u0442\u044C \u0434\u043E \u0441\u0435\u0440\u0456\u0457 \u043f\u043e\u0432\u0442\u043e\u0440\u0435\u043d\u044c \u2014 \u0432\u0438\u0434\u0430\u043b\u044f\u044e \u043b\u0438\u0448\u0435 \u0439\u043e\u0433\u043e \u043e\u0434\u043d\u043e\u0433\u043e','error'); await dbDelete('lessons',id); return; }
      var all=(S.lessons||[]).filter(function(x){return x.recurId===l.recurId;});
      for(var i=0;i<all.length;i++) await dbDelete('lessons',all[i].id);
      mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E \u0441\u0435\u0440\u0456\u044E ('+all.length+')');
    }
  }catch(e){}
}

async function savePayment(){
  var studentId=document.getElementById('p-std')?.value, amount=parseFloat(document.getElementById('p-amt')?.value);
  if(!studentId||!amount){ mkToast("\u0423\u0447\u0435\u043D\u044C \u0442\u0430 \u0441\u0443\u043C\u0430 \u043E\u0431\u043E\u0432'\u044F\u0437\u043A\u043E\u0432\u0456",'error'); return; }
  var obj={
    student_id:studentId, amount,
    method: document.getElementById('p-mth')?.value||'cash',
    date:   document.getElementById('p-date')?.value,
    month:  document.getElementById('p-mon')?.value||'',
    status: document.getElementById('p-stat')?.value||'paid',
    note:   document.getElementById('p-note')?.value||'',
    branch_id: myBranchId()||null,
  };
  window._saving = true;
  try{
    if(S.editId){ await dbUpdate('payments',S.editId,obj); mkToast('\u041E\u043D\u043E\u0432\u043B\u0435\u043D\u043E'); }
    else         { await dbInsert('payments',Object.assign({id:uid()},obj)); mkToast('\u0417\u0430\u043F\u0438\u0441\u0430\u043D\u043E'); }
    closeM('mo-payment'); S.editId=null; window._saving=false; refreshPage('payments');
  }catch(e){ window._saving=false; mkToast('Помилка: '+(e.message||e),'error'); }
}

async function delPay(id){
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u043F\u043B\u0430\u0442\u0456\u0436?')) return;
  try{ await dbDelete('payments',id); mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); }catch(e){}
}


function updateParentInfo(){
  var sel = document.getElementById('cm-student');
  var wrap = document.getElementById('cm-parent-wrap');
  var info = document.getElementById('cm-parent-info');
  if(!sel || !wrap || !info) return;
  var sid = sel.value;
  var s = sid ? (S.students||[]).find(function(x){return x.id===sid;}) : null;
  if(s && (s.parentFn||s.parent_fn||s.parentPhone||s.parent_phone)){
    var fn = s.parentFn||s.parent_fn||'';
    var ph = s.parentPhone||s.parent_phone||'';
    info.innerHTML = (fn?'<strong>'+fn+'</strong>':'')+(ph?' — <a href="tel:'+ph+'">'+ph+'</a>':'');
    wrap.style.display = 'block';
  } else {
    wrap.style.display = 'none';
  }
}

async function saveComm(){
  var tutorId=document.getElementById('cm-tutor')?.value, date=document.getElementById('cm-date')?.value;
  if(!tutorId){ mkToast('\u041E\u0431\u0435\u0440\u0456\u0442\u044C \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0430','error'); return; }
  if(!date)   { mkToast('\u0412\u043A\u0430\u0436\u0456\u0442\u044C \u0434\u0430\u0442\u0443','error'); return; }
  window._saving = true;
  var obj={ tutor_id:tutorId,
    student_id:document.getElementById('cm-student')?.value||null,
    date, type:document.getElementById('cm-type')?.value||'call',
    note:document.getElementById('cm-note')?.value||'' };
  try{
    if(window._editCommId){
      var _cid=window._editCommId;
      await dbUpdate('comms',_cid,obj);
      // Миттєве оновлення локального стану
      var _lc=(S.comms||[]).find(function(c){return c.id===_cid;});
      if(_lc) Object.assign(_lc,obj,{tutorId:obj.tutor_id,studentId:obj.student_id});
      window._editCommId=null;
      closeM('mo-comm'); mkToast('\u0417\u0431\u0435\u0440\u0435\u0436\u0435\u043D\u043E'); window._saving=false;
    } else {
      await dbInsert('comms',Object.assign({id:uid(),branch_id:myBranchId()||null},obj));
      closeM('mo-comm'); mkToast('Записано'); window._saving=false;
    }
    // Add to local S.comms immediately
    if(S.currentPage==='comms') renderCommsPage();
    if(typeof renderCommLog==='function') renderCommLog();
  }catch(e){ window._saving=false; mkToast('Помилка: '+(e.message||e),'error'); }
}

async function delComm(id){
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u043A\u043E\u043C\u0443\u043D\u0456\u043A\u0430\u0446\u0456\u044E?')) return;
  try{
    await dbDelete('comms',id);
    S.comms=(S.comms||[]).filter(function(c){return c.id!==id;});
    if(S.currentPage==='comms') renderCommsPage();
    mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E');
  }catch(e){}
}

async function saveSettings(){
  try{
    await _sb.from('settings').upsert({
      id:'main',
      name:    document.getElementById('set-name')?.value||'',
      phone:   document.getElementById('set-phone')?.value||'',
      email:   document.getElementById('set-email')?.value||'',
      address: document.getElementById('set-addr')?.value||'',
      payment_details: document.getElementById('set-payment')?.value||'',
      unisender_key: document.getElementById('set-unisender-key')?.value||'',
      viber_sender:  document.getElementById('set-viber-sender')?.value||'',
      updated_at: new Date().toISOString()
    });
    mkToast('\u0417\u0431\u0435\u0440\u0435\u0436\u0435\u043D\u043E');
  }catch(e){ mkToast('\u041F\u043E\u043C\u0438\u043B\u043A\u0430','error'); }
}

async function addSubj(){
  var name=(document.getElementById('ns-name')?.value||'').trim();
  if(!name){ mkToast('\u0412\u0432\u0435\u0434\u0456\u0442\u044C \u043D\u0430\u0437\u0432\u0443','error'); return; }
  try{
    await dbInsert('subjects',{id:uid(),name:name,branch_id:myBranchId()||null});
    document.getElementById('ns-name').value='';
    mkToast('\u0414\u043E\u0434\u0430\u043D\u043E');
  }catch(e){}
}

async function delSubj(id){
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u043F\u0440\u0435\u0434\u043C\u0435\u0442?')) return;
  try{
    await dbDelete('subjects',id);
    S.subjects=(S.subjects||[]).filter(function(s){return s.id!==id;});
    if(typeof renderSettings==='function'&&S.currentPage==='settings') renderSettings();
    mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E');
  }catch(e){}
}

async function addBranch(){
  var nm=(document.getElementById('new-branch-name')?.value||'').trim();
  var addr=(document.getElementById('new-branch-addr')?.value||'').trim();
  if(!nm){ mkToast('\u0412\u0432\u0435\u0434\u0456\u0442\u044C \u043D\u0430\u0437\u0432\u0443 \u0444\u0456\u043B\u0456\u0457','error'); return; }
  try{
    await dbInsert('branches',{id:'b'+uid(),name:nm,address:addr,phone:''});
    document.getElementById('new-branch-name').value='';
    document.getElementById('new-branch-addr').value='';
    mkToast('\u0424\u0456\u043B\u0456\u044E \u0434\u043E\u0434\u0430\u043D\u043E');
  }catch(e){}
}

async function delBranch(id){
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0444\u0456\u043B\u0456\u044E?')) return;
  try{ await dbDelete('branches',id); if(S.currentBranchId===id)S.currentBranchId=null; mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); }catch(e){}
}

async function editBranch(id){
  openBranchM(id);
}

// Pricing rules
async function savePriceRule(){
  var name=(document.getElementById('pr-name')?.value||'').trim();
  var price=parseFloat(document.getElementById('pr-price')?.value||0);
  if(!name||!price){ mkToast("\u041D\u0430\u0437\u0432\u0430 \u0442\u0430 \u0446\u0456\u043D\u0430 \u043E\u0431\u043E\u0432'\u044F\u0437\u043A\u043E\u0432\u0456",'error'); return; }
  var editId=document.getElementById('pr-edit-id')?.value||'';
  var obj={
    name, price,
    subject_match: document.getElementById('pr-subj')?.value||'',
    tutor_id:      document.getElementById('pr-tutor')?.value||'',
    grade_match:   document.getElementById('pr-grade')?.value||'',
    dur_min:       parseInt(document.getElementById('pr-dur')?.value)||null,
    branch_id:     myBranchId()||null,
  };
  try{
    if(editId){ await dbUpdate('pricing_rules',editId,obj); mkToast('\u041E\u043D\u043E\u0432\u043B\u0435\u043D\u043E'); }
    else       { await dbInsert('pricing_rules',Object.assign({id:uid()},obj)); mkToast('\u041F\u0440\u0430\u0432\u0438\u043B\u043E \u0434\u043E\u0434\u0430\u043D\u043E'); }
    ['pr-name','pr-price','pr-subj','pr-grade','pr-dur'].forEach(function(f){ var el=document.getElementById(f);if(el)el.value=''; });
    var pt=document.getElementById('pr-tutor');if(pt)pt.value='';
    var pi=document.getElementById('pr-edit-id');if(pi)pi.value='';
    var pb=document.getElementById('pr-save-btn');if(pb)pb.textContent='+ \u0414\u043E\u0434\u0430\u0442\u0438 \u043F\u0440\u0430\u0432\u0438\u043B\u043E';
  }catch(e){}
}

async function editPriceRule(id){
  var r=(S.pricingRules||[]).find(function(x){return x.id===id;});
  if(!r) return;
  var set=function(elId,val){var el=document.getElementById(elId);if(el)el.value=val||'';};
  set('pr-name',r.name);set('pr-price',r.price);set('pr-subj',r.subjectMatch||r.subject_match);
  set('pr-grade',r.gradeMatch||r.grade_match);set('pr-dur',r.durMin||r.dur_min);set('pr-edit-id',r.id);
  var pt=document.getElementById('pr-tutor');if(pt)pt.value=r.tutorId||r.tutor_id||'';
  var pb=document.getElementById('pr-save-btn');if(pb)pb.textContent='\uD83D\uDCBE \u0417\u0431\u0435\u0440\u0435\u0433\u0442\u0438 \u0437\u043C\u0456\u043D\u0438';
}

async function delPriceRule(id){
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u043F\u0440\u0430\u0432\u0438\u043B\u043E?')) return;
  try{ await dbDelete('pricing_rules',id); mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); }catch(e){}
}

// Users management (profiles table)
async function renderUsers(){
  var list=document.getElementById('ut-list');
  if(!list) return;
  list.innerHTML='<div class="empty"><div class="ei">\u23F3</div>\u0417\u0430\u0432\u0430\u043D\u0442\u0430\u0436\u0435\u043D\u043D\u044F\u2026</div>';
  var _users2 = await _sb.from('profiles').select('*'); var users = _users2.data;
  S.users = (users || []).map(normalizeUser);
  list.innerHTML='';
  (users||[]).forEach(function(u){
    var ro=ROLES[u.role]||ROLES.tutor;
    var canEdit=R()==='god'||(R()==='director'&&u.role!=='god');
    var canDel=(R()==='god'&&u.id!==CU?.id)||(R()==='director'&&u.role!=='god'&&u.id!==CU?.id);
    var row=document.createElement('div'); row.className='ulr';
    var av=document.createElement('div'); av.className='av uav';
    av.style.cssText='background:'+ro.avatarBg+';width:38px;height:38px;font-size:14px;font-weight:700;flex-shrink:0;color:#fff';
    av.textContent=(u.fn?.[0]||'?')+(u.ln?.[0]||'');
    var info=document.createElement('div');info.className='uin';
    info.innerHTML='<div class="uinn">'+u.fn+' '+(u.ln||'')+'</div><div class="uinm">'+(u.email||'\u2014')+'</div>';
    var rpill=document.createElement('span');rpill.className='rpill '+u.role;
    rpill.innerHTML=ro.icon+' '+ro.label;
    var btns=document.createElement('div');btns.style.cssText='display:flex;gap:6px;margin-left:auto;align-items:center';
    if(u.disabled){
      var dbadge=document.createElement('span');
      dbadge.className='badge';
      dbadge.style.cssText='background:rgba(248,113,113,.14);color:var(--danger);font-size:10px;font-weight:700';
      dbadge.textContent='\uD83D\uDEAB \u0414\u043E\u0441\u0442\u0443\u043F \u0432\u0438\u043C\u043A.';
      btns.appendChild(dbadge);
    }
    if(canEdit){
      var eb=document.createElement('button');eb.className='btn btn-g btn-sm';eb.innerHTML='\u270F\uFE0F';
      (function(id){eb.onclick=function(){openUserM(id);};})(u.id);btns.appendChild(eb);
      var ab=document.createElement('button');ab.className='btn btn-p btn-sm';ab.textContent='\uD83D\uDD10 \u0414\u043E\u0441\u0442\u0443\u043F';
      (function(id){ab.onclick=function(){openUserAccessM(id);};})(u.id);btns.appendChild(ab);
    }
    if(canDel){
      var bb=document.createElement('button');bb.className='btn btn-sm '+(u.disabled?'btn-g':'btn-d');
      bb.textContent=u.disabled?'\uD83D\uDD13 \u0423\u0432\u0456\u043C\u043A.':'\uD83D\uDEAB \u0412\u0438\u043C\u043A.';
      bb.title=u.disabled?'\u0412\u0456\u0434\u043D\u043E\u0432\u0438\u0442\u0438 \u0434\u043E\u0441\u0442\u0443\u043F':'\u0417\u0430\u0431\u043B\u043E\u043A\u0443\u0432\u0430\u0442\u0438 \u0434\u043E\u0441\u0442\u0443\u043F (\u0437\u0432\u0456\u043B\u044C\u043D\u0435\u043D\u043D\u044F)';
      (function(id){bb.onclick=function(){toggleUserAccess(id);};})(u.id);btns.appendChild(bb);
      var db=document.createElement('button');db.className='btn btn-sm btn-d';db.innerHTML='\uD83D\uDDD1';
      (function(id){db.onclick=function(){delUser(id);};})(u.id);btns.appendChild(db);
    }
    row.appendChild(av);row.appendChild(info);row.appendChild(rpill);row.appendChild(btns);
    list.appendChild(row);
  });
  if(!users?.length) list.innerHTML='<div class="empty"><div class="ei">\uD83D\uDC64</div>\u041D\u0435\u043C\u0430\u0454 \u0430\u043A\u0430\u0443\u043D\u0442\u0456\u0432</div>';
}

async function openUserM(id){
  S.editId=id||null;
  var u=id?(S.users||[]).find(function(x){return x.id===id;}):null;
  if(id&&!u) return;

  var titleEl=document.getElementById('mu-title');
  if(titleEl) titleEl.textContent=id?'Редагувати акаунт':'Новий акаунт';

  var fnEl=document.getElementById('u-fn');     if(fnEl)    fnEl.value=u?u.fn||'':'';
  var lnEl=document.getElementById('u-ln');     if(lnEl)    lnEl.value=u?u.ln||'':'';
  var emEl=document.getElementById('u-email');  if(emEl)  { emEl.value=u?u.email||'':''; emEl.disabled=!!id; emEl.style.opacity=id?'0.6':'1'; }
  var roEl=document.getElementById('u-role');   if(roEl)    roEl.value=u?u.role||'tutor':'tutor';
  toggleTutLink();
  popSel('u-tlink',S.tutors,'id',function(t){return t.fn+' '+t.ln;},'\u041f\u0440\u0438\u0432\u2019\u044f\u0437\u0430\u0442\u0438 \u0434\u043e \u0432\u0438\u043a\u043b\u0430\u0434\u0430\u0447\u0430');
  if(id){
    var linked=(S.tutors||[]).find(function(t){return t.acc_uid===id||t.accId===id;});
    if(linked){ var tlEl=document.getElementById('u-tlink'); if(tlEl) tlEl.value=linked.id; }
  }
  var brListU=document.getElementById('u-branch-list');
  if(brListU){
    var _preSelU=(u&&u.branchIds)||[];
    brListU.innerHTML=(S.branches||[]).slice().sort(function(a,b){return (a.name||'').localeCompare(b.name||'','uk');})
      .map(function(b){
        var checked=_preSelU.indexOf(b.id)>=0?' checked':'';
        return '<label class="af-day"><input type="checkbox" class="u-branch-cb" value="'+b.id+'"'+checked+'> '+b.name+'</label>';
      }).join('') || '<span style="font-size:11px;color:var(--t3)">\u0424\u0456\u043B\u0456\u0439 \u0449\u0435 \u043D\u0435 \u0441\u0442\u0432\u043E\u0440\u0435\u043D\u043E</span>';
  }
  openM('mo-user');
}

async function saveUser(){
  var fn=document.getElementById('u-fn').value.trim();
  var ln=document.getElementById('u-ln').value.trim();
  var role=document.getElementById('u-role').value;
  var email=document.getElementById('u-email').value.trim();
  var branchIds=Array.from(document.querySelectorAll('.u-branch-cb:checked')).map(function(el){return el.value;});
  var branchPatch={ branch_ids:branchIds.join(','), branch_id:branchIds[0]||null };

  if(!fn){ mkToast('\u0412\u0432\u0435\u0434\u0456\u0442\u044c \u0456\u043c\u2019\u044f','error'); return; }

  try{
    if(!S.editId){
      // Новий акаунт — запрошуємо через Supabase Admin API
      if(!email){ mkToast('\u0412\u0432\u0435\u0434\u0456\u0442\u044c email','error'); return; }
      var _inv = await _sb.auth.admin.inviteUserByEmail(email, {
        data:{ fn, ln, role }
      });
      if(_inv.error) throw _inv.error;
      // Одразу оновимо профіль щойно створеного користувача
      if(_inv.data?.user?.id){
        await _sb.from('profiles').update(Object.assign({fn,ln,role},branchPatch)).eq('id',_inv.data.user.id);
      }
      mkToast('\u0417\u0430\u043f\u0440\u043e\u0448\u0435\u043d\u043d\u044f \u043d\u0430\u0434\u0456\u0441\u043b\u0430\u043d\u043e \u043d\u0430 '+email);
    } else {
      // Редагування існуючого
      await dbUpdate('profiles',S.editId,Object.assign({fn,ln,role},branchPatch));
      var tutorId=document.getElementById('u-tlink')?.value;
      if(role==='tutor'&&tutorId){
        await _sb.from('tutors').update({acc_uid:S.editId}).eq('id',tutorId);
      }
      if(CU?.id===S.editId){ CU=Object.assign({},CU,{fn,ln,role},normalizeUser(branchPatch)); updateSBUser(); buildSidebar(); }
      mkToast('\u041e\u043d\u043e\u0432\u043b\u0435\u043d\u043e');
    }
    closeM('mo-user'); S.editId=null; renderUsers();
  }catch(e){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430: '+(e.message||e),'error'); console.error('saveUser error:',e); }
}

async function toggleUserAccess(id){
  var u=(S.users||[]).find(function(x){return x.id===id;});
  if(!u){ mkToast('\u041D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E','error'); return; }
  if(id===(CU&&CU.id)){ mkToast('\u041D\u0435 \u043C\u043E\u0436\u043D\u0430 \u0437\u0430\u0431\u043B\u043E\u043A\u0443\u0432\u0430\u0442\u0438 \u0432\u043B\u0430\u0441\u043D\u0438\u0439 \u0430\u043A\u0430\u0443\u043D\u0442','error'); return; }
  var willDisable=!u.disabled;
  if(willDisable && !confirm('\u0417\u0430\u0431\u043B\u043E\u043A\u0443\u0432\u0430\u0442\u0438 \u0434\u043E\u0441\u0442\u0443\u043F \u0434\u043B\u044F '+(u.fn||'')+' '+(u.ln||'')+'?\n\n\u041B\u044E\u0434\u0438\u043D\u0430 \u0431\u0456\u043B\u044C\u0448\u0435 \u043D\u0435 \u0437\u043C\u043E\u0436\u0435 \u0443\u0432\u0456\u0439\u0442\u0438 \u0432 \u0441\u0438\u0441\u0442\u0435\u043C\u0443. \u0410\u043A\u0430\u0443\u043D\u0442 \u0456 \u0432\u0441\u0456 \u0437\u0430\u043F\u0438\u0441\u0438 \u0437\u0430\u043B\u0438\u0448\u0430\u044E\u0442\u044C\u0441\u044F, \u0434\u043E\u0441\u0442\u0443\u043F \u043C\u043E\u0436\u043D\u0430 \u0432\u0456\u0434\u043D\u043E\u0432\u0438\u0442\u0438 \u0431\u0443\u0434\u044C-\u043A\u043E\u043B\u0438.')) return;
  try{ await dbUpdate('profiles', id, {disabled:willDisable}); mkToast(willDisable?'\u0414\u043E\u0441\u0442\u0443\u043F \u0437\u0430\u0431\u043B\u043E\u043A\u043E\u0432\u0430\u043D\u043E':'\u0414\u043E\u0441\u0442\u0443\u043F \u0432\u0456\u0434\u043D\u043E\u0432\u043B\u0435\u043D\u043E'); renderUsers(); }
  catch(e){ mkToast('\u041F\u043E\u043C\u0438\u043B\u043A\u0430: '+e.message,'error'); }
}
window.toggleUserAccess=toggleUserAccess;
async function delUser(id){
  if(id===CU?.id){ mkToast('\u041D\u0435 \u043C\u043E\u0436\u043D\u0430 \u0432\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0441\u0432\u0456\u0439 \u0430\u043A\u0430\u0443\u043D\u0442','error'); return; }
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0430\u043A\u0430\u0443\u043D\u0442?')) return;
  try{ await dbDelete('profiles',id); mkToast('\u0412\u0438\u0434\u0430\u043B\u0435\u043D\u043E'); renderUsers(); }catch(e){}
}

// Access editor
var _uaUserId=null;
async function openUserAccessM(id){
  var u=(S.users||[]).find(function(x){return x.id===id;});
  if(!u){ mkToast('\u041D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E','error'); return; }
  _uaUserId=id;
  document.querySelectorAll('.ua-tab').forEach(function(t,i){t.classList.toggle('active',i===0);});
  document.querySelectorAll('.ua-panel').forEach(function(p,i){p.classList.toggle('active',i===0);});
  try{ buildUAHeader(u); }catch(e){ console.error('buildUAHeader error:',e); }
  try{ buildUAPerms(u); }catch(e){ console.error('buildUAPerms error:',e); }
  try{ buildUANav(u); }catch(e){ console.error('buildUANav error:',e); }
  try{ buildUASummary(u); }catch(e){ console.error('buildUASummary error:',e); }
  openM('mo-user-access');
}

async function uaPermChange(key,val,roleDefault){
  var u=(S.users||[]).find(function(x){return x.id===_uaUserId;});
  if(!u) return;
  var perms=JSON.parse(JSON.stringify(u.perms||{}));
  if(!perms.can) perms.can={};
  if(val===roleDefault) delete perms.can[key]; else perms.can[key]=val;
  if(!Object.keys(perms.can).length) delete perms.can;
  try{ await dbUpdate('profiles',_uaUserId,{perms}); if(CU?.id===_uaUserId)CU.perms=perms; }catch(e){ mkToast('Помилка: '+(e.message||e),'error'); console.error('ua error:',e); return; }
  var u2=(S.users||[]).find(function(x){return x.id===_uaUserId;}); if(u2){u2.perms=perms;buildUASummary(u2);}
}

async function uaResetPerm(key){
  var u=(S.users||[]).find(function(x){return x.id===_uaUserId;});
  if(!u) return;
  var perms=JSON.parse(JSON.stringify(u.perms||{}));
  if(perms.can){delete perms.can[key];if(!Object.keys(perms.can).length)delete perms.can;}
  try{ await dbUpdate('profiles',_uaUserId,{perms}); if(CU?.id===_uaUserId)CU.perms=perms; }catch(e){ mkToast('Помилка: '+(e.message||e),'error'); console.error('ua error:',e); return; }
  var u2=(S.users||[]).find(function(x){return x.id===_uaUserId;}); if(u2){u2.perms=perms;buildUAPerms(u2);buildUASummary(u2);}
}

async function uaNavChange(pageId,show,isInRole){
  var u=(S.users||[]).find(function(x){return x.id===_uaUserId;});
  if(!u) return;
  var perms=JSON.parse(JSON.stringify(u.perms||{}));
  if(!perms.hideNav)perms.hideNav=[];if(!perms.showNav)perms.showNav=[];
  if(show){
    perms.hideNav=perms.hideNav.filter(function(p){return p!==pageId;});
    if(!isInRole&&!perms.showNav.includes(pageId))perms.showNav.push(pageId);
  } else {
    perms.showNav=perms.showNav.filter(function(p){return p!==pageId;});
    if(isInRole&&!perms.hideNav.includes(pageId))perms.hideNav.push(pageId);
  }
  if(!perms.hideNav.length)delete perms.hideNav;
  if(!perms.showNav.length)delete perms.showNav;
  try{ await dbUpdate('profiles',_uaUserId,{perms}); if(CU?.id===_uaUserId){CU.perms=perms;buildSidebar();} }catch(e){ mkToast('Помилка: '+(e.message||e),'error'); console.error('ua error:',e); return; }
  var u2=(S.users||[]).find(function(x){return x.id===_uaUserId;}); if(u2){u2.perms=perms;buildUASummary(u2);}
}

async function resetAllUserAccess(){
  var u=(S.users||[]).find(function(x){return x.id===_uaUserId;});
  if(!confirm('\u0421\u043A\u0438\u043D\u0443\u0442\u0438 \u043D\u0430\u043B\u0430\u0448\u0442\u0443\u0432\u0430\u043D\u043D\u044F \u0434\u043B\u044F '+(u?.fn||'')+' '+(u?.ln||'')+' ?')) return;
  try{
    await dbUpdate('profiles',_uaUserId,{perms:{}});
    if(CU?.id===_uaUserId){CU.perms={};buildSidebar();}
    var u2=(S.users||[]).find(function(x){return x.id===_uaUserId;}); if(u2){u2.perms={};buildUAPerms(u2);buildUANav(u2);buildUASummary(u2);}
    renderUsers(); mkToast('\u0421\u043A\u0438\u043D\u0443\u0442\u043E');
  }catch(e){}
}

function setBranch(id){
  S.currentBranchId = id||null;
  updateBranchSelector();
  renderSch && renderSch();
  nav(S.currentPage||'dashboard');
}
