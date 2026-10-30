function payrollQualifies(l){ return !!l && PAYROLL_STATUSES.indexOf(l.status)>=0; }
// Формат годин: ціле без дробу (3), інакше з десятковими (1.5, 3.25)
function fmtHours(h){ h=Math.round((parseFloat(h)||0)*100)/100; return String(h); }
// Ціна тесту: з поля на сторінці → зі збереженого → 500 за замовчуванням
function payrollTestPrice(){
  var el=document.getElementById('pr-test-price');
  var v=el&&el.value!==''?parseFloat(el.value):NaN;
  if(isNaN(v)){ try{ v=parseFloat(localStorage.getItem('payroll_test_price')); }catch(e){} }
  if(isNaN(v)||v<0) v=PAYROLL_TEST_PRICE_DEFAULT;
  return v;
}
function prSetTestPrice(){
  var el=document.getElementById('pr-test-price');
  if(el){ var v=parseFloat(el.value); if(!isNaN(v)&&v>=0){ try{ localStorage.setItem('payroll_test_price', String(v)); }catch(e){} } }
  renderPayroll();
}
// Поточний коефіцієнт: з поля на сторінці → зі збереженого → 0.4 за замовчуванням
function payrollCoef(){
  var el=document.getElementById('pr-coef');
  var v=el&&el.value!==''?parseFloat(el.value):NaN;
  if(isNaN(v)){ try{ v=parseFloat(localStorage.getItem('payroll_coef')); }catch(e){} }
  if(isNaN(v)||v<=0||v>1) v=PAYROLL_COEF_DEFAULT;
  return v;
}
function prSetCoef(){
  var el=document.getElementById('pr-coef');
  if(el){ var v=parseFloat(el.value); if(!isNaN(v)&&v>0&&v<=1){ try{ localStorage.setItem('payroll_coef', String(v)); }catch(e){} } }
  renderPayroll();
}

function canPayroll(){ return R()==='god'||R()==='director'; }

function payrollPeriod(){
  var el=document.getElementById('pr-period');
  if(el&&el.value) return el.value;
  var n=new Date();
  return n.getFullYear()+'-'+String(n.getMonth()+1).padStart(2,'0');
}

// Заняття репетитора за місяць, згруповані по (учень + ціна). Кожна ціна — окремий рядок.
function payrollBase(tutorId, period){
  var coef=payrollCoef();
  var testPrice=payrollTestPrice();
  var groups={}, order=[];
  var doneCount=0, makeupCount=0, burnedCount=0, testCount=0;
  (S.lessons||[]).forEach(function(l){
    if((l.tutorId||l.tutor_id)!==tutorId) return;
    if(String(l.date||'').slice(0,7)!==period) return;
    if(!payrollQualifies(l)) return;
    var sid=l.studentId||l.student_id||'';
    var isTest=(l.status==='testing');
    // Ціна = ЗАМОРОЖЕНИЙ знімок ставки заняття (l.price), зафіксований у момент
    // проведення. Зміни ставок у картці учня минулі (проведені) заняття НЕ змінюють.
    // Якщо знімку немає (старі записи) — беремо поточну ставку як запас.
    var price;
    var p=parseFloat(l.price);
    if(!isNaN(p)&&p>0){ price=Math.round(p); }
    else if(isTest){ price=testPrice; }
    else {
      var st=(S.students||[]).find(function(s){return s.id===sid;});
      price=Math.round(studentRate(st, l.subject, l.tutorId||l.tutor_id)||0);
    }
    var key=sid+'|'+price+'|'+(isTest?'T':'L');            // розбивка по ставці + тип
    if(!groups[key]){ groups[key]={sid:sid, price:price, isTest:isTest, cnt:0, hours:0}; order.push(key); }
    groups[key].cnt++;
    groups[key].hours += (parseFloat(l.dur)||60)/60;       // фактичні години заняття
    if(l.status==='makeup') makeupCount++;
    else if(l.status==='burned') burnedCount++;
    else if(isTest) testCount++;
    else doneCount++;
  });
  var totalHours=0;
  var rows=order.map(function(key){
    var g=groups[key];
    var hours=Math.round(g.hours*100)/100;           // Год. — сума фактичних годин
    var coefv=Math.round(g.price*coef);              // Коеф. — виплата за годину
    var sum=Math.round(hours*coefv*100)/100;         // Сума = Год. × Коеф.
    totalHours+=hours;
    return { name:(g.sid?snShort(g.sid):'\u2014'), isTest:g.isTest, cnt:g.cnt, hours:hours, price:g.price, coef:coefv, sum:sum };
  }).sort(function(a,b){
    var c=String(a.name).localeCompare(String(b.name),'uk'); if(c!==0) return c;
    if(a.isTest!==b.isTest) return a.isTest?1:-1;    // тести — після звичайних
    return a.price-b.price;
  });
  var base=Math.round(rows.reduce(function(s,r){return s+r.sum;},0)*100)/100;
  return {
    rows:rows,
    doneCount:doneCount, makeupCount:makeupCount, burnedCount:burnedCount, testCount:testCount,
    totalCount:doneCount+makeupCount+burnedCount+testCount,
    totalHours:Math.round(totalHours*100)/100,
    base:base
  };
}

function payrollItemsFor(tutorId, period){
  return (S.payrollItems||[]).filter(function(i){
    return (i.tutorId||i.tutor_id)===tutorId && !( i.adminId||i.admin_id ) && i.period===period;
  });
}

// Пункти зарплати адміністратора — на адмінів НЕ поширюється коефіцієнт ×0.4,
// у них немає "бази" з уроків, лише ручні пункти (сума як є).
function payrollItemsForAdmin(adminId, period){
  return (S.payrollItems||[]).filter(function(i){
    return (i.adminId||i.admin_id)===adminId && i.period===period;
  });
}
var PAYROLL_FIRSTPAY_PCT = 0.10; // 10% від першої оплати учня — адміну, що її вніс
// Бонус адміна за перші оплати, внесені САМЕ у цьому місяці (period='YYYY-MM').
function firstPaymentBonusInfo(adminId, period){
  var count=0, base=0, list=[];
  (S.students||[]).forEach(function(s){
    var amt=parseFloat(s.first_payment); if(isNaN(amt)||amt<=0) return;
    if((s.first_payment_by||'')!==adminId) return;                      // вніс саме цей адмін
    if(String(s.first_payment_date||'').slice(0,7)!==period) return;    // саме цей місяць
    count++; base+=amt;
    list.push({ name:(s.id?snShort(s.id):'\u2014'), amount:amt });
  });
  list.sort(function(a,b){ return String(a.name).localeCompare(String(b.name),'uk'); });
  return { count:count, base:Math.round(base*100)/100, bonus:Math.round(base*PAYROLL_FIRSTPAY_PCT*100)/100, students:list };
}
// Сума успішно реалізованих угод (за першими оплатами) за період [from..to] за ДАТОЮ
// СТВОРЕННЯ учня (щоб збігалося з фільтром по часу в CRM), з фільтром за відповідальним.
function realizedDealsSum(respAdminId, from, to){
  var sum=0, cnt=0, list=[];
  (S.students||[]).forEach(function(s){
    var amt=parseFloat(s.first_payment); if(isNaN(amt)||amt<=0) return;
    if(respAdminId && (s.crmResponsible||s.crm_responsible||'')!==respAdminId) return;
    var d=String(s.created_at||s.createdAt||s.first_payment_date||'').slice(0,10); if(!d || d<from || d>to) return;
    sum+=amt; cnt++;
    list.push({ name:(s.id?snShort(s.id):'\u2014'), amount:amt, date:d });
  });
  list.sort(function(a,b){ return String(a.name).localeCompare(String(b.name),'uk'); });
  return { sum:Math.round(sum*100)/100, cnt:cnt, list:list };
}
function payrollTotalAdmin(adminId, period){
  var items=payrollItemsForAdmin(adminId, period);
  var itemsTotal=items.reduce(function(s,i){return s+payrollItemAmount(i);},0);
  var fp=firstPaymentBonusInfo(adminId, period);
  return { items:items, firstPay:fp, total:Math.round((itemsTotal+fp.bonus)*100)/100 };
}

function payrollItemAmount(item, base){
  return Math.round((parseFloat(item.amount)||0)*100)/100;
}

function payrollTotal(tutorId, period){
  var b=payrollBase(tutorId, period);
  var extra=payrollItemsFor(tutorId, period).reduce(function(s,i){return s+payrollItemAmount(i,b.base);},0);
  return { base:b, extra:Math.round(extra*100)/100, total:Math.round((b.base+extra)*100)/100 };
}

function renderPayroll(){
  var wrap=document.getElementById('payroll-body');
  if(!wrap) return;
  if(!canPayroll()){ wrap.innerHTML='<div style="padding:20px;color:var(--t3)">\u0414\u043E\u0441\u0442\u0443\u043F \u043B\u0438\u0448\u0435 \u0434\u043B\u044F \u0434\u0438\u0440\u0435\u043A\u0442\u043E\u0440\u0430</div>'; return; }
  var per=payrollPeriod();
  var perEl=document.getElementById('pr-period');
  if(perEl&&!perEl.value) perEl.value=per;
  var coefEl=document.getElementById('pr-coef');
  if(coefEl&&coefEl.value==='') coefEl.value=payrollCoef();
  var testEl=document.getElementById('pr-test-price');
  if(testEl&&testEl.value==='') testEl.value=payrollTestPrice();
  var fT=(document.getElementById('pr-tutor-filter')||{value:''}).value;

  var tSel=document.getElementById('pr-tutor-filter');
  if(tSel){
    var prev=tSel.value;
    tSel.innerHTML='<option value="">\u0412\u0441\u0456 \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438</option>'
      +(S.tutors||[]).slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');})
        .map(function(t){return '<option value="'+t.id+'">'+t.fn+' '+t.ln+'</option>';}).join('');
    tSel.value=prev;
  }

  var tutors=(S.tutors||[]).slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');});
  if(fT) tutors=tutors.filter(function(t){return t.id===fT;});

  // Кольори-акценти для аватарів (циклічно)
  var AV=['#29abe2','#22b573','#f59e0b','#8b5cf6','#ec4899','#0ea5e9','#14b8a6','#f97316'];
  function initials(t){ return ((t.fn||' ')[0]+(t.ln||' ')[0]).toUpperCase(); }
  function money(n){ return (Math.round(n*100)/100).toLocaleString('uk-UA'); }

  // Спершу рахуємо все, щоб зробити зведення
  var rows=[], grand=0, sumExtra=0, activeCount=0;
  var cntDone=0, cntMakeup=0, cntBurned=0, cntTest=0;
  tutors.forEach(function(t){
    var pt=payrollTotal(t.id, per), b=pt.base;
    var hasData=b.totalCount||payrollItemsFor(t.id,per).length;
    if(!hasData && fT==='') return;
    grand+=pt.total; sumExtra+=pt.extra;
    cntDone+=b.doneCount; cntMakeup+=b.makeupCount; cntBurned+=b.burnedCount; cntTest+=b.testCount;
    if(hasData) activeCount++;
    rows.push({t:t, pt:pt, b:b});
  });

  // Адміністратори рахуються ОКРЕМО і НЕ входять у загальний фонд (фонд = лише репетитори).
  var _adminsPre=(S.users||[]).filter(function(u){return u.role==='admin';});
  var _adminGrandPre=0, _adminActiveCount=0;
  _adminsPre.forEach(function(a){
    var pt=payrollTotalAdmin(a.id, per);
    if(!pt.items.length && fT!=='') return;
    _adminGrandPre+=pt.total;
    if(pt.items.length) _adminActiveCount++;
  });
  // grand НЕ включає адмінів (за вимогою)

  var html='';

  // ── Зведена панель ──
  if(rows.length || _adminsPre.length){
    html+='<div class="pr-summary">'
      +'<div class="pr-sum-main">'
        +'<div class="pr-sum-label">Фонд репетиторів за '+prMonthName(per)+'</div>'
        +'<div class="pr-sum-grand">'+money(grand)+'<span class="pr-cur">\u20B4</span></div>'
        +'<div class="pr-sum-sub">'+activeCount+' '+prPlural(activeCount,'репетитор','репетитори','репетиторів')+' \u00B7 коеф. '+payrollCoef()+'</div>'
      +'</div>'
      +'<div class="pr-sum-break">'
        +'<div class="pr-break-item"><span class="pr-dot" style="background:#22b573"></span><span class="pr-break-lbl">Проведені</span><span class="pr-break-val">'+cntDone+'</span></div>'
        +'<div class="pr-break-item"><span class="pr-dot" style="background:#f59e0b"></span><span class="pr-break-lbl">Відпрацювання</span><span class="pr-break-val">'+cntMakeup+'</span></div>'
        +'<div class="pr-break-item"><span class="pr-dot" style="background:#ef4444"></span><span class="pr-break-lbl">Згорілі</span><span class="pr-break-val">'+cntBurned+'</span></div>'
        +'<div class="pr-break-item"><span class="pr-dot" style="background:#0ea5e9"></span><span class="pr-break-lbl">Тести</span><span class="pr-break-val">'+cntTest+'</span></div>'
        +'<div class="pr-break-item"><span class="pr-dot" style="background:#8b5cf6"></span><span class="pr-break-lbl">Додаткове</span><span class="pr-break-val" style="color:'+(sumExtra<0?'var(--danger)':'inherit')+'">'+(sumExtra>=0?'+':'')+money(sumExtra)+'\u20B4</span></div>'
      +'</div>'
    +'</div>';
  }

  // ── Картки репетиторів ──
  html+='<div class="pr-grid">';
  rows.forEach(function(r, idx){
    var t=r.t, pt=r.pt, b=r.b;
    var av=AV[idx % AV.length];
    var items=payrollItemsFor(t.id, per);
    var itemsHtml=items.map(function(i){
      var amt=payrollItemAmount(i,b.base);
      var neg=amt<0;
      return '<div class="pr-item">'
        +'<span class="pr-item-lbl">'+(i.label||'\u2014')+'</span>'
        +'<span class="pr-item-amt '+(neg?'neg':'pos')+'">'+(amt>=0?'+':'')+money(amt)+'\u20B4'
        +'<button onclick="delPayrollItem(\''+i.id+'\')" title="\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438" class="pr-item-del">\u2715</button></span>'
      +'</div>';
    }).join('');

    // Розбивка по учнях: Прізвище І. | К-ть | Ціна | Коеф. | Сума
    var studTbl;
    if(b.rows.length){
      studTbl='<table class="pr-stud"><thead><tr>'
          +'<th>Учень</th><th class="c">К-ть</th><th class="c">Год</th><th class="c">Ціна</th><th class="c">Коеф.</th><th class="r">Сума</th>'
        +'</tr></thead><tbody>'
        +b.rows.map(function(r){
          return '<tr><td class="pr-stud-name">'+r.name+(r.isTest?' <span class="pr-test-tag">тест</span>':'')+'</td>'
            +'<td class="c">'+r.cnt+'</td>'
            +'<td class="c">'+fmtHours(r.hours)+'</td>'
            +'<td class="c">'+r.price+'</td>'
            +'<td class="c">'+r.coef+'</td>'
            +'<td class="r"><b>'+money(r.sum)+'</b></td></tr>';
        }).join('')
        +'</tbody><tfoot><tr><td>Разом</td><td class="c">'+b.totalCount+'</td><td class="c">'+fmtHours(b.totalHours)+'</td><td></td><td></td><td class="r"><b>'+money(b.base)+'\u20B4</b></td></tr></tfoot></table>';
    } else {
      studTbl='<div class="pr-empty-mini">Немає проведених занять за місяць</div>';
    }
    var metaTxt=b.doneCount+' пров. \u00B7 '+b.makeupCount+' відпр. \u00B7 '+b.burnedCount+' згор.'+(b.testCount?' \u00B7 '+b.testCount+' тест.':'');

    html+='<div class="pr-card" style="--av:'+av+'">'
      +'<div class="pr-card-head">'
        +'<div class="pr-avatar" style="background:'+av+'">'+initials(t)+'</div>'
        +'<div class="pr-name-wrap"><div class="pr-name">'+t.fn+' '+t.ln+'</div>'
          +'<div class="pr-meta">'+metaTxt+'</div></div>'
        +'<div class="pr-total-badge">'+money(pt.total)+'\u20B4</div>'
      +'</div>'
      +'<div class="pr-body">'
        +studTbl
        +(itemsHtml?'<div class="pr-items">'+itemsHtml+'</div>':'')
      +'</div>'
      +'<div class="pr-card-foot">'
        +'<button class="btn btn-g btn-sm" onclick="openPayrollItemM(\''+t.id+'\')">+ \u041F\u0443\u043D\u043A\u0442</button>'
        +'<button class="btn btn-g btn-sm" onclick="printPayroll(\''+t.id+'\')">\uD83D\uDDA8 \u0414\u0440\u0443\u043A</button>'
        +'<div class="pr-foot-total">\u0420\u0430\u0437\u043E\u043C <b>'+money(pt.total)+'\u20B4</b></div>'
      +'</div>'
    +'</div>';
  });
  html+='</div>';

  // ── Зарплати адміністраторів (без бази з уроків, без коефіцієнта ×0.4) ──
  var admins=(S.users||[]).filter(function(u){return u.role==='admin';})
    .sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');});
  var adminRows=[];
  if(admins.length){
    var adminGrand=0;
    admins.forEach(function(a,idx){
      var pt=payrollTotalAdmin(a.id, per);
      if(!pt.items.length && !(pt.firstPay&&pt.firstPay.bonus>0) && fT!=='') return;
      adminGrand+=pt.total;
      adminRows.push({a:a, pt:pt, av:AV[idx % AV.length]});
    });
    html+='<div style="margin:22px 0 10px;font-size:13px;font-weight:700;color:var(--t2);text-transform:uppercase;letter-spacing:.4px">\uD83D\uDC64 \u0417\u0430\u0440\u043F\u043B\u0430\u0442\u0438 \u0430\u0434\u043C\u0456\u043D\u0456\u0441\u0442\u0440\u0430\u0442\u043E\u0440\u0456\u0432 <span style="font-weight:400;color:var(--t3);text-transform:none">(\u0431\u0435\u0437 \u043A\u043E\u0435\u0444. \u00D70.4 \u2014 \u043B\u0438\u0448\u0435 \u0432\u0440\u0443\u0447\u043D\u0456 \u043F\u0443\u043D\u043A\u0442\u0438)</span></div>';
    html+='<div class="pr-grid">';
    adminRows.forEach(function(r){
      var a=r.a, pt=r.pt, av=r.av;
      var itemsHtml=pt.items.map(function(i){
        var amt=payrollItemAmount(i);
        var neg=amt<0;
        return '<div class="pr-item">'
          +'<span class="pr-item-lbl">'+(i.label||'\u2014')+'</span>'
          +'<span class="pr-item-amt '+(neg?'neg':'pos')+'">'+(amt>=0?'+':'')+money(amt)+'\u20B4'
          +'<button onclick="delPayrollItem(\''+i.id+'\')" title="\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438" class="pr-item-del">\u2715</button></span>'
        +'</div>';
      }).join('');
      var fpHtml = (pt.firstPay&&pt.firstPay.bonus>0)
        ? '<div class="pr-item"><span class="pr-item-lbl">💵 10% від перших оплат ('+pt.firstPay.count+' \u00B7 '+money(pt.firstPay.base)+'\u20B4)</span><span class="pr-item-amt pos">+'+money(pt.firstPay.bonus)+'\u20B4</span></div>'
          + ((pt.firstPay.students&&pt.firstPay.students.length)
              ? '<div style="font-size:11px;color:var(--t3);padding:1px 0 5px 10px;line-height:1.55">'
                  + pt.firstPay.students.map(function(st){ return '\u2022 '+st.name+' \u2014 '+money(st.amount)+'\u20B4 (10% = '+money(Math.round(st.amount*PAYROLL_FIRSTPAY_PCT*100)/100)+'\u20B4)'; }).join('<br>')
                + '</div>'
              : '')
        : '';
      var bodyInner = (fpHtml||itemsHtml)
        ? '<div class="pr-items">'+fpHtml+itemsHtml+'</div>'
        : '<div style="font-size:12px;color:var(--t3);padding:4px 0">\u041D\u0435\u043C\u0430\u0454 \u043F\u0443\u043D\u043A\u0442\u0456\u0432 \u0437\u0430 \u0446\u0435\u0439 \u043C\u0456\u0441\u044F\u0446\u044C</div>';
      html+='<div class="pr-card" style="--av:'+av+'">'
        +'<div class="pr-card-head">'
          +'<div class="pr-avatar" style="background:'+av+'">'+initials(a)+'</div>'
          +'<div class="pr-name-wrap"><div class="pr-name">'+a.fn+' '+a.ln+'</div>'
            +'<div class="pr-meta">\uD83D\uDC64 \u0410\u0434\u043C\u0456\u043D\u0456\u0441\u0442\u0440\u0430\u0442\u043E\u0440</div></div>'
          +'<div class="pr-total-badge">'+money(pt.total)+'\u20B4</div>'
        +'</div>'
        +'<div class="pr-body">'
          +bodyInner
        +'</div>'
        +'<div class="pr-card-foot">'
          +'<button class="btn btn-g btn-sm" onclick="openPayrollItemM(\''+a.id+'\',\'admin\')">+ \u041F\u0443\u043D\u043A\u0442</button>'
          +'<button class="btn btn-g btn-sm" onclick="printPayroll(\''+a.id+'\',\'admin\')">\uD83D\uDDA8 \u0414\u0440\u0443\u043A</button>'
          +'<div class="pr-foot-total">\u0420\u0430\u0437\u043E\u043C <b>'+money(pt.total)+'\u20B4</b></div>'
        +'</div>'
      +'</div>';
    });
    html+='</div>';
  }

  if(!rows.length && !adminRows.length) html='<div class="pr-empty"><div style="font-size:38px;margin-bottom:8px">\uD83D\uDCB0</div>\u0417\u0430 \u0446\u0435\u0439 \u043C\u0456\u0441\u044F\u0446\u044C \u043D\u0435\u043C\u0430\u0454 \u043F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u0438\u0445 \u0437\u0430\u043D\u044F\u0442\u044C</div>';
  wrap.innerHTML=html;
}

// Місяць прописом для періоду 'YYYY-MM'
function prMonthName(per){
  var p=String(per||'').split('-');
  var m=['\u0441\u0456\u0447\u0435\u043D\u044C','\u043B\u044E\u0442\u0438\u0439','\u0431\u0435\u0440\u0435\u0437\u0435\u043D\u044C','\u043A\u0432\u0456\u0442\u0435\u043D\u044C','\u0442\u0440\u0430\u0432\u0435\u043D\u044C','\u0447\u0435\u0440\u0432\u0435\u043D\u044C','\u043B\u0438\u043F\u0435\u043D\u044C','\u0441\u0435\u0440\u043F\u0435\u043D\u044C','\u0432\u0435\u0440\u0435\u0441\u0435\u043D\u044C','\u0436\u043E\u0432\u0442\u0435\u043D\u044C','\u043B\u0438\u0441\u0442\u043E\u043F\u0430\u0434','\u0433\u0440\u0443\u0434\u0435\u043D\u044C'][(parseInt(p[1])||1)-1];
  return (m||'')+' '+(p[0]||'');
}
function prPlural(n, one, few, many){
  var n10=n%10, n100=n%100;
  if(n10===1&&n100!==11) return one;
  if(n10>=2&&n10<=4&&(n100<10||n100>=20)) return few;
  return many;
}

function openPayrollItemM(id, type){
  if(!canPayroll()) return;
  type=type||'tutor';
  window._prItemType=type;
  window._prItemTutor=type==='tutor'?id:null;
  window._prItemAdmin=type==='admin'?id:null;
  var person=type==='admin'?(S.users||[]).find(function(x){return x.id===id;}):(S.tutors||[]).find(function(x){return x.id===id;});
  var nEl=document.getElementById('pri-tutor-name');
  if(nEl) nEl.textContent=person?(person.fn+' '+person.ln+' \u00B7 '+payrollPeriod()):'';
  document.getElementById('pri-label').value='';
  document.getElementById('pri-amount').value='';
  openM('mo-payroll-item');
}

async function savePayrollItem(){
  if(!canPayroll()) return;
  var label=(document.getElementById('pri-label')||{value:''}).value.trim();
  var amount=(document.getElementById('pri-amount')||{value:''}).value;
  if(!label){ mkToast('\u0412\u043A\u0430\u0436\u0456\u0442\u044C \u043F\u043E\u044F\u0441\u043D\u0435\u043D\u043D\u044F','error'); return; }
  if(!amount){ mkToast('\u0412\u043A\u0430\u0436\u0456\u0442\u044C \u0441\u0443\u043C\u0443','error'); return; }
  var obj={ id:uid(), tutor_id:window._prItemTutor||null, admin_id:window._prItemAdmin||null, period:payrollPeriod(),
    label:label,
    amount:amount!==''?parseFloat(amount):null,
    created_by:CU?CU.id:null, created_at:new Date().toISOString() };
  try{
    await dbInsert('payroll_items',obj);
    if(!(S.payrollItems||[]).some(function(x){return x.id===obj.id;}))
      S.payrollItems=(S.payrollItems||[]).concat([normalizePayrollItem(obj)]);
    closeM('mo-payroll-item'); mkToast('\u0414\u043E\u0434\u0430\u043D\u043E');
    renderPayroll();
  }catch(e){ mkToast('\u041F\u043E\u043C\u0438\u043B\u043A\u0430: '+(e.message||e),'error'); }
}

async function delPayrollItem(id){
  if(!canPayroll()) return;
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u043F\u0443\u043D\u043A\u0442?')) return;
  try{
    await dbDelete('payroll_items',id);
    S.payrollItems=(S.payrollItems||[]).filter(function(x){return x.id!==id;});
    renderPayroll();
  }catch(e){}
}

// Друк відомості: tutorId — один репетитор, null — усі разом
function printPayroll(recipientId, type){
  if(!canPayroll()) return;
  var per=payrollPeriod();
  var perLbl=(function(){ var p=per.split('-'); var m=['\u0441\u0456\u0447\u0435\u043D\u044C','\u043B\u044E\u0442\u0438\u0439','\u0431\u0435\u0440\u0435\u0437\u0435\u043D\u044C','\u043A\u0432\u0456\u0442\u0435\u043D\u044C','\u0442\u0440\u0430\u0432\u0435\u043D\u044C','\u0447\u0435\u0440\u0432\u0435\u043D\u044C','\u043B\u0438\u043F\u0435\u043D\u044C','\u0441\u0435\u0440\u043F\u0435\u043D\u044C','\u0432\u0435\u0440\u0435\u0441\u0435\u043D\u044C','\u0436\u043E\u0432\u0442\u0435\u043D\u044C','\u043B\u0438\u0441\u0442\u043E\u043F\u0430\u0434','\u0433\u0440\u0443\u0434\u0435\u043D\u044C'][parseInt(p[1])-1]; return m+' '+p[0]; })();
  var AV=['#29abe2','#22b573','#f59e0b','#8b5cf6','#ec4899','#0ea5e9','#14b8a6','#f97316'];
  function initials(t){ return ((t.fn||' ')[0]+(t.ln||' ')[0]).toUpperCase(); }
  function money(n){ return (Math.round(n*100)/100).toLocaleString('uk-UA'); }

  // Друк одного адміністратора: лише ручні пункти, без бази й коефіцієнта ×0.4
  if(type==='admin'){
    var admin=(S.users||[]).find(function(x){return x.id===recipientId;});
    if(!admin){ mkToast('\u0410\u0434\u043C\u0456\u043D\u0456\u0441\u0442\u0440\u0430\u0442\u043E\u0440\u0430 \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E','error'); return; }
    var ptA=payrollTotalAdmin(recipientId, per);
    var rowsA=ptA.items.map(function(i){
      var amt=payrollItemAmount(i);
      return '<tr><td class="ilbl">'+(i.label||'')+'</td><td class="r '+(amt<0?'neg':'pos')+'">'+(amt>=0?'+':'')+money(amt)+' \u20B4</td></tr>';
    }).join('');
    if(ptA.firstPay&&ptA.firstPay.bonus>0){
      var _fpRows='<tr><td class="ilbl">\uD83D\uDCB5 10% \u0432\u0456\u0434 \u043F\u0435\u0440\u0448\u0438\u0445 \u043E\u043F\u043B\u0430\u0442 ('+ptA.firstPay.count+' \u00B7 '+money(ptA.firstPay.base)+' \u20B4)</td><td class="r pos">+'+money(ptA.firstPay.bonus)+' \u20B4</td></tr>';
      (ptA.firstPay.students||[]).forEach(function(st){
        _fpRows += '<tr><td class="ilbl" style="padding-left:22px;color:#888">\u2022 '+st.name+' \u2014 '+money(st.amount)+' \u20B4</td><td class="r" style="color:#888">10% = '+money(Math.round(st.amount*PAYROLL_FIRSTPAY_PCT*100)/100)+' \u20B4</td></tr>';
      });
      rowsA = _fpRows + rowsA;
    }
    var w=window.open('','_blank');
    w.document.write('<!DOCTYPE html><html><head><meta charset="utf-8"><title>\u0412\u0456\u0434\u043E\u043C\u0456\u0441\u0442\u044C \u0437\u0430\u0440\u043F\u043B\u0430\u0442\u0438</title>'
      +'<style>'+printWatermarkCSS()+'*{box-sizing:border-box} body{font-family:Arial,Helvetica,sans-serif;max-width:520px;margin:20px auto;color:#1a1a2e}'
      +'.head{background:linear-gradient(135deg,#2e3192,#5b60d4);border-radius:16px;padding:22px 26px;color:#fff;margin-bottom:20px}'
      +'.head h1{font-size:15px;margin:0 0 4px;font-weight:600;letter-spacing:.3px;opacity:.9;text-transform:uppercase}'
      +'.head .sum{font-size:34px;font-weight:800;letter-spacing:-1px}'
      +'.head .per{font-size:12px;opacity:.85;margin-top:2px}'
      +'.card{border:1px solid #e2e2ea;border-radius:14px;overflow:hidden;position:relative}'
      +'.card::before{content:"";position:absolute;top:0;left:0;bottom:0;width:4px;background:'+AV[0]+'}'
      +'.chead{display:flex;align-items:center;gap:10px;padding:14px 16px 10px}'
      +'.avatar{width:36px;height:36px;border-radius:10px;background:'+AV[0]+';color:#fff;display:flex;align-items:center;justify-content:center;font-weight:800;font-size:13px}'
      +'.cname{flex:1;font-size:14px;font-weight:700}'
      +'table.rows{width:100%;border-collapse:collapse;font-size:12px}'
      +'table.rows td{padding:5px 16px;border-bottom:1px solid #f0f0f5}'
      +'.r{text-align:right;white-space:nowrap;font-weight:700} .r.pos{color:#22b573} .r.neg{color:#e74c3c}'
      +'.cfoot{display:flex;justify-content:space-between;align-items:center;padding:10px 16px;margin-top:8px;background:#f7f7fb;font-size:12px;color:#555}'
      +'.cfoot b{font-size:14px;color:#1a1a2e}'
      +'.sign{padding:8px 16px 14px;font-size:10.5px;color:#888}'
      +'@media print{.noprint{display:none}}</style></head><body>'
      +printWatermarkHTML()
      +'<div class="head"><h1>\u0412\u0456\u0434\u043E\u043C\u0456\u0441\u0442\u044C \u043D\u0430\u0440\u0430\u0445\u0443\u0432\u0430\u043D\u043D\u044F \u0437\u0430\u0440\u043F\u043B\u0430\u0442\u0438</h1><div class="sum">'+money(ptA.total)+' \u20B4</div>'
        +'<div class="per">'+perLbl+' \u00B7 \u0410\u0434\u043C\u0456\u043D\u0456\u0441\u0442\u0440\u0430\u0442\u043E\u0440 \u00B7 \u0421\u0444\u043E\u0440\u043C\u043E\u0432\u0430\u043D\u043E: '+new Date().toLocaleDateString('uk-UA')+'</div></div>'
      +'<div class="card"><div class="chead"><div class="avatar">'+initials(admin)+'</div><div class="cname">'+admin.fn+' '+admin.ln+'</div></div>'
      +'<table class="rows">'+(rowsA||'<tr><td class="ilbl" style="color:#999">\u041D\u0435\u043C\u0430\u0454 \u043F\u0443\u043D\u043A\u0442\u0456\u0432</td><td></td></tr>')+'</table>'
      +'<div class="cfoot"><span>\u0420\u0430\u0437\u043E\u043C \u0434\u043E \u0432\u0438\u043F\u043B\u0430\u0442\u0438</span><b>'+money(ptA.total)+' \u20B4</b></div>'
      +'<div class="sign">\u041F\u0456\u0434\u043F\u0438\u0441 \u0430\u0434\u043C\u0456\u043D\u0456\u0441\u0442\u0440\u0430\u0442\u043E\u0440\u0430: ______________________</div></div>'
      +'<button class="noprint" onclick="window.print()" style="margin-top:20px;padding:10px 20px;font-size:14px;cursor:pointer;border-radius:8px;border:none;background:#2e3192;color:#fff">\uD83D\uDDA8 \u0414\u0440\u0443\u043A\u0443\u0432\u0430\u0442\u0438</button>'
      +'</body></html>');
    w.document.close();
    setTimeout(function(){ try{w.print();}catch(e){} }, 400);
    return;
  }

  // ── ВЕДОМІСТЬ (усі): узагальнена таблиця по репетиторах + адміністраторах ──
  if(!recipientId){
    var tuts=(S.tutors||[]).slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');});
    var tFund=0, tRows='', tD=0,tM=0,tB=0,tT=0,tL=0,tH=0;
    tuts.forEach(function(t){
      var pt=payrollTotal(t.id, per), b=pt.base;
      if(!b.totalCount && !payrollItemsFor(t.id,per).length) return;
      tFund+=pt.total; tD+=b.doneCount; tM+=b.makeupCount; tB+=b.burnedCount; tT+=b.testCount; tL+=b.totalCount; tH+=b.totalHours;
      tRows+='<tr><td>'+t.fn+' '+t.ln+'</td>'
        +'<td class="c">'+b.doneCount+'</td><td class="c">'+b.makeupCount+'</td><td class="c">'+b.burnedCount+'</td>'
        +'<td class="c">'+b.testCount+'</td><td class="c">'+b.totalCount+'</td><td class="c">'+fmtHours(b.totalHours)+'</td>'
        +'<td class="r"><b>'+money(pt.total)+' \u20B4</b></td></tr>';
    });
    if(!tRows) tRows='<tr><td colspan="8" style="color:#999">\u041D\u0435\u043C\u0430\u0454 \u0434\u0430\u043D\u0438\u0445</td></tr>';
    var adms=(S.users||[]).filter(function(u){return u.role==='admin';})
      .sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');});
    var aFund=0, aRows='';
    adms.forEach(function(a){
      var pt=payrollTotalAdmin(a.id, per);
      if(!pt.items.length) return;
      aFund+=pt.total;
      aRows+='<tr><td>'+a.fn+' '+a.ln+'</td><td class="r"><b>'+money(pt.total)+' \u20B4</b></td></tr>';
    });
    var adminBlock = aRows
      ? '<h2>\u0410\u0434\u043C\u0456\u043D\u0456\u0441\u0442\u0440\u0430\u0442\u043E\u0440\u0438 <span class="note">(\u043E\u043A\u0440\u0435\u043C\u043E, \u043D\u0435 \u0432 \u0444\u043E\u043D\u0434\u0456 \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0456\u0432)</span></h2>'
        +'<table><thead><tr><th>\u0410\u0434\u043C\u0456\u043D\u0456\u0441\u0442\u0440\u0430\u0442\u043E\u0440</th><th class="r">\u0421\u0443\u043C\u0430</th></tr></thead>'
        +'<tbody>'+aRows+'<tr class="tot"><td>\u0420\u0430\u0437\u043E\u043C</td><td class="r">'+money(aFund)+' \u20B4</td></tr></tbody></table>'
      : '';
    var wS=window.open('','_blank');
    wS.document.write('<!DOCTYPE html><html><head><meta charset="utf-8"><title>\u0412\u0456\u0434\u043E\u043C\u0456\u0441\u0442\u044C \u0437\u0430\u0440\u043F\u043B\u0430\u0442\u0438</title>'
      +'<style>'+printWatermarkCSS()
      +'*{box-sizing:border-box} body{font-family:Arial,Helvetica,sans-serif;max-width:820px;margin:20px auto;color:#1a1a2e;background:#fff}'
      +'.head{background:linear-gradient(135deg,#2e3192,#5b60d4);border-radius:16px;padding:22px 26px;color:#fff;margin-bottom:18px}'
      +'.head h1{font-size:15px;margin:0 0 4px;font-weight:600;letter-spacing:.3px;opacity:.9;text-transform:uppercase}'
      +'.head .sum{font-size:34px;font-weight:800;letter-spacing:-1px}'
      +'.head .per{font-size:12px;opacity:.85;margin-top:2px}'
      +'h2{font-size:13px;text-transform:uppercase;letter-spacing:.4px;color:#2e3192;margin:20px 0 8px}'
      +'h2 .note{font-weight:400;text-transform:none;color:#999;font-size:11px}'
      +'table{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:6px}'
      +'th{background:#f2f2f7;text-align:left;padding:7px 10px;font-size:10px;text-transform:uppercase;letter-spacing:.3px;color:#555;border-bottom:2px solid #d8d8e0}'
      +'th.c,td.c{text-align:center} th.r,td.r{text-align:right;white-space:nowrap}'
      +'td{padding:6px 10px;border-bottom:1px solid #eee}'
      +'tr.tot td{border-top:2px solid #1a1a2e;font-weight:800;background:#faf7ef}'
      +'.foot-tot{margin-top:14px;font-size:13px;text-align:right;color:#555;border-top:1px dashed #ccc;padding-top:10px}'
      +'.foot-tot b{font-size:16px;color:#1a1a2e}'
      +'@media print{.noprint{display:none}}</style></head><body>'
      +printWatermarkHTML()
      +'<div class="head"><h1>\u0412\u0456\u0434\u043E\u043C\u0456\u0441\u0442\u044C \u043D\u0430\u0440\u0430\u0445\u0443\u0432\u0430\u043D\u043D\u044F \u0437\u0430\u0440\u043F\u043B\u0430\u0442\u0438</h1>'
        +'<div class="sum">'+money(tFund)+' \u20B4</div>'
        +'<div class="per">'+perLbl+' \u00B7 \u0424\u043E\u043D\u0434 \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0456\u0432 \u00B7 \u043A\u043E\u0435\u0444. '+payrollCoef()+' \u00B7 \u0421\u0444\u043E\u0440\u043C\u043E\u0432\u0430\u043D\u043E: '+new Date().toLocaleDateString('uk-UA')+'</div></div>'
      +'<h2>\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438</h2>'
      +'<table><thead><tr><th>\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440</th><th class="c">\u041F\u0440\u043E\u0432.</th><th class="c">\u0412\u0456\u0434\u043F\u0440.</th><th class="c">\u0417\u0433\u043E\u0440.</th><th class="c">\u0422\u0435\u0441\u0442\u0438</th><th class="c">\u0417\u0430\u043D\u044F\u0442\u044C</th><th class="c">\u0413\u043E\u0434\u0438\u043D</th><th class="r">\u0421\u0443\u043C\u0430</th></tr></thead>'
        +'<tbody>'+tRows
        +'<tr class="tot"><td>\u0420\u0430\u0437\u043E\u043C</td><td class="c">'+tD+'</td><td class="c">'+tM+'</td><td class="c">'+tB+'</td><td class="c">'+tT+'</td><td class="c">'+tL+'</td><td class="c">'+fmtHours(tH)+'</td><td class="r">'+money(tFund)+' \u20B4</td></tr>'
        +'</tbody></table>'
      +adminBlock
      +(aRows?'<div class="foot-tot">\u0420\u0430\u0437\u043E\u043C \u0434\u043E \u0432\u0438\u043F\u043B\u0430\u0442\u0438 (\u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0438 + \u0430\u0434\u043C\u0456\u043D\u0456\u0441\u0442\u0440\u0430\u0442\u043E\u0440\u0438): <b>'+money(tFund+aFund)+' \u20B4</b></div>':'')
      +'<button class="noprint" onclick="window.print()" style="margin-top:20px;padding:10px 20px;font-size:14px;cursor:pointer;border-radius:8px;border:none;background:#2e3192;color:#fff">\uD83D\uDDA8 \u0414\u0440\u0443\u043A\u0443\u0432\u0430\u0442\u0438</button>'
      +'</body></html>');
    wS.document.close();
    setTimeout(function(){ try{wS.print();}catch(e){} }, 400);
    return;
  }

  var tutorId=recipientId;
  var tutors=tutorId?(S.tutors||[]).filter(function(t){return t.id===tutorId;})
    :(S.tutors||[]).slice().sort(function(a,b){return (a.fn+' '+a.ln).localeCompare(b.fn+' '+b.ln,'uk');});
  var grand=0, cards='', activeCount=0;
  tutors.forEach(function(t,idx){
    var pt=payrollTotal(t.id, per), b=pt.base;
    if(!tutorId&&!b.totalCount&&!payrollItemsFor(t.id,per).length) return;
    grand+=pt.total; activeCount++;
    var av=AV[idx % AV.length];
    var itemsHtml=payrollItemsFor(t.id,per).map(function(i){
      var amt=payrollItemAmount(i,b.base);
      return '<tr><td class="ilbl" colspan="5">'+(i.label||'')+'</td>'
        +'<td class="r '+(amt<0?'neg':'pos')+'">'+(amt>=0?'+':'')+money(amt)+' \u20B4</td></tr>';
    }).join('');
    var studRows=b.rows.length
      ? b.rows.map(function(r){
          return '<tr><td class="ilbl">'+r.name+(r.isTest?' <span class="muted">(тест)</span>':'')+'</td><td class="c">'+r.cnt+'</td><td class="c">'+fmtHours(r.hours)+'</td><td class="c">'+r.price+'</td><td class="c">'+r.coef+'</td><td class="r pos">'+money(r.sum)+' \u20B4</td></tr>';
        }).join('')
      : '<tr><td class="ilbl" colspan="6" style="color:#999">\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u0438\u0445 \u0437\u0430\u043D\u044F\u0442\u044C</td></tr>';
    cards+='<div class="card" style="--av:'+av+'">'
      +'<div class="chead">'
        +'<div class="avatar">'+initials(t)+'</div>'
        +'<div class="cname"><b>'+t.fn+' '+t.ln+'</b><span class="cmeta">'+b.doneCount+' \u043F\u0440\u043E\u0432. \u00B7 '+b.makeupCount+' \u0432\u0456\u0434\u043F\u0440. \u00B7 '+b.burnedCount+' \u0437\u0433\u043E\u0440.'+(b.testCount?' \u00B7 '+b.testCount+' \u0442\u0435\u0441\u0442.':'')+'</span></div>'
        +'<div class="ctotal">'+money(pt.total)+' \u20B4</div>'
      +'</div>'
      +'<table class="rows">'
        +'<thead><tr><th class="ilbl">\u0423\u0447\u0435\u043D\u044C</th><th class="c">\u041A-\u0442\u044C</th><th class="c">\u0413\u043E\u0434</th><th class="c">\u0426\u0456\u043D\u0430</th><th class="c">\u041A\u043E\u0435\u0444.</th><th class="r">\u0421\u0443\u043C\u0430</th></tr></thead>'
        +'<tbody>'
          +studRows
          +'<tr class="subt"><td class="ilbl" colspan="5"><b>\u0420\u0430\u0437\u043E\u043C \u0437\u0430 \u0443\u0440\u043E\u043A\u0438 ('+b.totalCount+' \u00B7 '+fmtHours(b.totalHours)+' \u0433\u043E\u0434)</b></td><td class="r"><b>'+money(b.base)+' \u20B4</b></td></tr>'
          +itemsHtml
        +'</tbody>'
      +'</table>'
      +'<div class="cfoot"><span>\u0420\u0430\u0437\u043E\u043C \u0434\u043E \u0432\u0438\u043F\u043B\u0430\u0442\u0438</span><b>'+money(pt.total)+' \u20B4</b></div>'
      +'<div class="sign">\u041F\u0456\u0434\u043F\u0438\u0441 \u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0430: ______________________</div>'
    +'</div>';
  });

  var w=window.open('','_blank');
  w.document.write('<!DOCTYPE html><html><head><meta charset="utf-8"><title>\u0412\u0456\u0434\u043E\u043C\u0456\u0441\u0442\u044C \u0437\u0430\u0440\u043F\u043B\u0430\u0442\u0438</title>'
    +'<style>'
    +'*{box-sizing:border-box} body{font-family:Arial,Helvetica,sans-serif;max-width:900px;margin:20px auto;color:#1a1a2e;background:#fff}'
    +'.head{background:linear-gradient(135deg,#2e3192,#5b60d4);border-radius:16px;padding:22px 26px;color:#fff;margin-bottom:20px}'
    +'.head h1{font-size:15px;margin:0 0 4px;font-weight:600;letter-spacing:.3px;opacity:.9;text-transform:uppercase}'
    +'.head .sum{font-size:34px;font-weight:800;letter-spacing:-1px}'
    +'.head .per{font-size:12px;opacity:.85;margin-top:2px}'
    +'.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}'
    +'.card{border:1px solid #e2e2ea;border-radius:14px;padding:0;overflow:hidden;position:relative;break-inside:avoid;page-break-inside:avoid}'
    +'.card::before{content:"";position:absolute;top:0;left:0;bottom:0;width:4px;background:var(--av)}'
    +'.chead{display:flex;align-items:center;gap:10px;padding:14px 16px 10px}'
    +'.avatar{width:36px;height:36px;border-radius:10px;background:var(--av);color:#fff;display:flex;align-items:center;justify-content:center;font-weight:800;font-size:13px;flex-shrink:0}'
    +'.cname{flex:1;display:flex;flex-direction:column;font-size:13px}'
    +'.cname b{font-size:14px}'
    +'.cmeta{font-size:10.5px;color:#888;margin-top:1px}'
    +'.ctotal{font-weight:800;font-size:16px;color:var(--av);white-space:nowrap}'
    +'table.rows{width:100%;border-collapse:collapse;font-size:11.5px;padding:0 16px}'
    +'table.rows th{padding:6px 16px;text-align:left;font-size:9.5px;text-transform:uppercase;letter-spacing:.3px;color:#888;border-bottom:1px solid #e2e2ea;font-weight:700}'
    +'table.rows td{padding:4px 16px;border-bottom:1px solid #f0f0f5}'
    +'table.rows .c{text-align:center} table.rows th.c{text-align:center} table.rows th.r{text-align:right}'
    +'table.rows .subt td{border-top:1px solid #d0d0da;background:#faf7ef}'
    +'.ilbl{color:#333} .muted{color:#999;font-size:10px}'
    +'.r{text-align:right;white-space:nowrap;font-weight:700}'
    +'.r.pos{color:#22b573} .r.neg{color:#e74c3c}'
    +'.bar{display:flex;height:5px;margin:8px 16px 0;border-radius:10px;overflow:hidden;background:#eee}'
    +'.bar-done{background:#22b573} .bar-make{background:#f59e0b}'
    +'.cfoot{display:flex;justify-content:space-between;align-items:center;padding:10px 16px;margin-top:8px;background:#f7f7fb;font-size:12px;color:#555}'
    +'.cfoot b{font-size:14px;color:#1a1a2e}'
    +'.sign{padding:8px 16px 14px;font-size:10.5px;color:#888}'
    +'.grand{margin-top:22px;font-size:18px;font-weight:800;text-align:right;border-top:3px double #1a1a2e;padding-top:12px}'
    +'@media print{.noprint{display:none} .card{box-shadow:none}}'
    +'@media(max-width:600px){.grid{grid-template-columns:1fr}}'
    +'</style></head><body>'
    +'<div class="head"><h1>\u0412\u0456\u0434\u043E\u043C\u0456\u0441\u0442\u044C \u043D\u0430\u0440\u0430\u0445\u0443\u0432\u0430\u043D\u043D\u044F \u0437\u0430\u0440\u043F\u043B\u0430\u0442\u0438</h1>'
      +'<div class="sum">'+money(grand)+' \u20B4</div>'
      +'<div class="per">'+perLbl+' \u00B7 '+activeCount+' '+(activeCount===1?'\u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440':'\u0440\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0456\u0432')+' \u00B7 \u0421\u0444\u043E\u0440\u043C\u043E\u0432\u0430\u043D\u043E: '+new Date().toLocaleDateString('uk-UA')+'</div>'
    +'</div>'
    +'<div class="grid">'+cards+'</div>'
    +(!tutorId&&tutors.length>1?'<div class="grand">\u0412\u0421\u042C\u041E\u0413\u041E \u0424\u041E\u041D\u0414 \u041E\u041F\u041B\u0410\u0422\u0418: '+money(grand)+' \u20B4</div>':'')
    +'<button class="noprint" onclick="window.print()" style="margin-top:20px;padding:10px 20px;font-size:14px;cursor:pointer;border-radius:8px;border:none;background:#2e3192;color:#fff">\uD83D\uDDA8 \u0414\u0440\u0443\u043A\u0443\u0432\u0430\u0442\u0438</button>'
    +'</body></html>');
  w.document.close();
  setTimeout(function(){ try{w.print();}catch(e){} }, 400);
}

window.renderPayroll=renderPayroll;
