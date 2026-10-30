
/** Дії, доступні поточному користувачу */
function cmdActions(){
  var acts=[];
  // Сторінки
  var _allowed=userNav()||[];
  (NAV_CFG||[]).forEach(function(c){
    if(_allowed.indexOf(c.id)<0) return;
    acts.push({ico:c.ico||'\u25CF', title:c.lbl, sub:'\u041f\u0435\u0440\u0435\u0439\u0442\u0438',
                kind:'page', run:function(){ nav(c.id); }});
  });
  // Швидкі дії
  if(can('lessons')) acts.push({ico:'\u2795', title:'\u041d\u043e\u0432\u0435 \u0437\u0430\u043d\u044f\u0442\u0442\u044f',
    sub:'\u0421\u0442\u0432\u043e\u0440\u0438\u0442\u0438', kind:'act', run:function(){ nav('schedule'); openLessM(); }});
  if(can('students')) acts.push({ico:'\ud83d\udc64', title:'\u041d\u043e\u0432\u0438\u0439 \u0443\u0447\u0435\u043d\u044c',
    sub:'\u0421\u0442\u0432\u043e\u0440\u0438\u0442\u0438', kind:'act', run:function(){ nav('students'); openStudM(); }});
  if(can('payments')) acts.push({ico:'\ud83d\udcb0', title:'\u041d\u043e\u0432\u0430 \u043e\u043f\u043b\u0430\u0442\u0430',
    sub:'\u0421\u0442\u0432\u043e\u0440\u0438\u0442\u0438', kind:'act', run:function(){ nav('payments'); if(typeof openPayM==='function') openPayM(); }});
  acts.push({ico:'\ud83c\udf19', title:'\u041f\u0435\u0440\u0435\u043c\u043a\u043d\u0443\u0442\u0438 \u0442\u0435\u043c\u0443',
    sub:'\u0421\u0432\u0456\u0442\u043b\u0430 / \u0442\u0435\u043c\u043d\u0430', kind:'act',
    run:function(){ if(typeof toggleTheme==='function') toggleTheme(); }});
  return acts;
}

/** Формує список результатів під запит */
function cmdResults(q){
  q=String(q||'').toLowerCase().trim();
  var out=[];

  // Учні — найчастіший сценарій пошуку
  if(q.length>=2){
    var _qd=q.replace(/\D/g,'');
    myStudents().filter(function(st){
      var nm=((st.fn||'')+' '+(st.ln||'')).toLowerCase();
      var nm2=((st.ln||'')+' '+(st.fn||'')).toLowerCase();
      if(nm.indexOf(q)>=0 || nm2.indexOf(q)>=0) return true;
      if(_qd && _qd.length>=3){
        var ph=String(st.phone||'').replace(/\D/g,'');
        var pph=String(st.parentPhone||st.parent_phone||'').replace(/\D/g,'');
        if(ph.indexOf(_qd)>=0 || pph.indexOf(_qd)>=0) return true;
      }
      return false;
    }).slice(0,6).forEach(function(st){
      out.push({ico:'\ud83c\udf93', title:(st.fn||'')+' '+(st.ln||''),
        sub:(st.phone||st.parentPhone||st.parent_phone||'\u0423\u0447\u0435\u043d\u044c'), kind:'student',
        run:function(){ nav('students'); setTimeout(function(){ openStudM(st.id); },120); }});
    });
  }

  // Сторінки й дії
  cmdActions().forEach(function(a){
    if(!q || String(a.title).toLowerCase().indexOf(q)>=0) out.push(a);
  });
  return out.slice(0,10);
}

function cmdOpen(){
  var el=document.getElementById('cmdk');
  if(!el) return;
  el.classList.add('open');
  var inp=document.getElementById('cmdk-input');
  if(inp){ inp.value=''; inp.focus(); }
  _cmdSel=0;
  cmdRender('');
}
window.cmdOpen=cmdOpen;

function cmdClose(){
  var el=document.getElementById('cmdk');
  if(el) el.classList.remove('open');
}
window.cmdClose=cmdClose;

function cmdRender(q){
  var box=document.getElementById('cmdk-list');
  if(!box) return;
  var res=cmdResults(q);
  window._cmdRes=res;
  if(!res.length){
    box.innerHTML='<div style="padding:22px;text-align:center;color:var(--t3);font-size:13px">'
      +'\u041d\u0456\u0447\u043e\u0433\u043e \u043d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e</div>';
    return;
  }
  if(_cmdSel>=res.length) _cmdSel=0;
  // Використовуємо mousedown, а не click: клік не встигав спрацювати,
  // бо поле вводу втрачало фокус і список перемальовувався раніше.
  box.innerHTML=res.map(function(r,i){
    return '<div class="cmdk-item'+(i===_cmdSel?' sel':'')+'" data-i="'+i+'" '
      +'onmousedown="event.preventDefault();cmdRun('+i+')">'
      +'<span class="cmdk-ico">'+r.ico+'</span>'
      +'<span class="cmdk-txt"><b>'+r.title+'</b><span>'+(r.sub||'')+'</span></span>'
      +(i===_cmdSel?'<span class="cmdk-hint">\u21b5</span>':'')
    +'</div>';
  }).join('');

  // Підсвітка при наведенні — через клас, без перемальовування списку
  Array.prototype.forEach.call(box.querySelectorAll('.cmdk-item'), function(el){
    el.addEventListener('mouseenter', function(){
      box.querySelectorAll('.cmdk-item.sel').forEach(function(x){ x.classList.remove('sel'); });
      el.classList.add('sel');
      _cmdSel=parseInt(el.dataset.i)||0;
    });
  });
}
window.cmdRender=cmdRender;

function cmdRun(i){
  var res=window._cmdRes||[];
  var r=res[i];
  if(!r) return;
  cmdClose();
  // Даємо вікну закритись, перш ніж відкривати наступне —
  // інакше картка учня одразу зникала разом із палітрою
  setTimeout(function(){
    try{ r.run(); }catch(e){ console.error('cmdRun:',e); }
  }, 60);
}
window.cmdRun=cmdRun;

function cmdKey(e){
  var res=window._cmdRes||[];
  if(e.key==='ArrowDown'){ e.preventDefault(); _cmdSel=Math.min(_cmdSel+1,res.length-1); cmdRender(e.target.value); }
  else if(e.key==='ArrowUp'){ e.preventDefault(); _cmdSel=Math.max(_cmdSel-1,0); cmdRender(e.target.value); }
  else if(e.key==='Enter'){ e.preventDefault(); cmdRun(_cmdSel); }
  else if(e.key==='Escape'){ cmdClose(); }
}
window.cmdKey=cmdKey;

function gSearch(q){
  if(S.currentPage!=='students') nav('students');
  else renderStudents();
}
