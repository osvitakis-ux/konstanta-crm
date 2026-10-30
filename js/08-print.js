try{ if(document.readyState!=='loading') setupGlobalVoiceMic(); else document.addEventListener('DOMContentLoaded', setupGlobalVoiceMic); }catch(e){}

function printSchedule(){
  // Понеділок обраного тижня (тиждень починається з понеділка)
  var _now=new Date();
  var _dow=_now.getDay()===0?6:_now.getDay()-1;
  var wkStart=new Date(_now);
  wkStart.setDate(_now.getDate()-_dow+(S.weekOffset||0)*7);
  wkStart.setHours(0,0,0,0);
  var days=[], dayNames=['\u041f\u043e\u043d\u0435\u0434\u0456\u043b\u043e\u043a','\u0412\u0456\u0432\u0442\u043e\u0440\u043e\u043a',
    '\u0421\u0435\u0440\u0435\u0434\u0430','\u0427\u0435\u0442\u0432\u0435\u0440','\u041f\u2019\u044f\u0442\u043d\u0438\u0446\u044f',
    '\u0421\u0443\u0431\u043e\u0442\u0430','\u041d\u0435\u0434\u0456\u043b\u044f'];
  for(var i=0;i<7;i++){
    var d=new Date(wkStart); d.setDate(wkStart.getDate()+i);
    days.push({date:localDateStr(d), name:dayNames[i],
               lbl:String(d.getDate()).padStart(2,'0')+'.'+String(d.getMonth()+1).padStart(2,'0')});
  }

  // Фільтр репетитора з екрана переноситься й у друк
  var selTut=(document.getElementById('sch-tutor-filter')||{value:''}).value;
  var all=myLessons().filter(function(l){
    if(l.status==='cancelled') return false;
    if(selTut && (l.tutorId||l.tutor_id)!==selTut) return false;
    return l.date>=days[0].date && l.date<=days[6].date;
  }).sort(function(a,b){
    return String(a.date+(a.time||'')).localeCompare(String(b.date+(b.time||'')));
  });

  if(!all.length){ mkToast('\u041d\u0435\u043c\u0430\u0454 \u0437\u0430\u043d\u044f\u0442\u044c \u0437\u0430 \u0446\u0435\u0439 \u0442\u0438\u0436\u0434\u0435\u043d\u044c','error'); return; }

  var bid=myBranchId();
  var branch=(S.branches||[]).find(function(b){return b.id===bid;})||(S.branches||[])[0];
  var tutName = selTut ? tn(selTut) : '';

  var body=days.map(function(d){
    var dayL=all.filter(function(l){ return l.date===d.date; });
    if(!dayL.length) return '';
    return '<div class="day">'
      +'<div class="dayh">'+d.name+' <span>'+d.lbl+'</span></div>'
      +'<table><thead><tr>'
        +'<th style="width:52px">\u0427\u0430\u0441</th>'
        +'<th>\u0423\u0447\u0435\u043d\u044c</th>'
        +'<th>\u041f\u0440\u0435\u0434\u043c\u0435\u0442</th>'
        +(selTut?'':'<th>\u0420\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440</th>')
        +'<th style="width:46px">\u0425\u0432</th>'
      +'</tr></thead><tbody>'
      +dayL.map(function(l){
        var st=(S.students||[]).find(function(x){return x.id===(l.studentId||l.student_id);});
        return '<tr>'
          +'<td class="tm">'+(l.time||'')+'</td>'
          +'<td>'+(st?st.fn+' '+st.ln:'\u2014')+'</td>'
          +'<td>'+(l.subject||'\u2014')+'</td>'
          +(selTut?'':'<td>'+(tn(l.tutorId||l.tutor_id)||'\u2014')+'</td>')
          +'<td class="c">'+(l.dur||60)+'</td>'
        +'</tr>';
      }).join('')
      +'</tbody></table></div>';
  }).join('');

  var f=function(d){ var p=String(d).split('-'); return p[2]+'.'+p[1]+'.'+p[0]; };
  var w=window.open('','_blank');
  if(!w){ mkToast('\u0414\u043e\u0437\u0432\u043e\u043b\u044c\u0442\u0435 \u0441\u043f\u043b\u0438\u0432\u0430\u044e\u0447\u0456 \u0432\u0456\u043a\u043d\u0430','error'); return; }
  w.document.write('<html><head><meta charset="utf-8"><title>\u0420\u043e\u0437\u043a\u043b\u0430\u0434</title>'
    +'<style>'+printWatermarkCSS()
    +'body{font-family:Arial,sans-serif;max-width:760px;margin:18px auto;color:#111;font-size:12px}'
    +'h1{font-size:17px;text-align:center;margin:0 0 3px}'
    +'.sub{text-align:center;color:#555;font-size:12px;margin-bottom:16px}'
    +'.day{margin-bottom:14px;break-inside:avoid}'
    +'.dayh{background:#eef2f7;padding:5px 9px;font-weight:700;font-size:12.5px;'
      +'border-left:3px solid #29abe2;border-radius:3px}'
    +'.dayh span{float:right;color:#666;font-weight:400}'
    +'table{width:100%;border-collapse:collapse;margin-top:4px}'
    +'th,td{border:1px solid #d5d5d5;padding:4px 7px;text-align:left}'
    +'th{background:#fafafa;font-size:10.5px;text-transform:uppercase;letter-spacing:.3px;color:#666}'
    +'.tm{font-family:monospace;font-weight:700}'
    +'.c{text-align:center}'
    +'@media print{ .noprint{display:none} @page{margin:12mm} }'
    +'</style></head><body>'
    +printWatermarkHTML()
    +'<h1>\u0420\u043e\u0437\u043a\u043b\u0430\u0434 \u0437\u0430\u043d\u044f\u0442\u044c</h1>'
    +'<div class="sub">'+f(days[0].date)+' \u2014 '+f(days[6].date)
      +(tutName?' \u00b7 '+tutName:'')
      +(branch&&branch.name?' \u00b7 '+branch.name:'')+'</div>'
    +body
    +printFooterHTML(branch)
    +'<button class="noprint" onclick="window.print()" style="margin-top:16px;padding:8px 18px;font-size:14px;cursor:pointer">'
      +'\uD83D\uDDA8 \u0414\u0440\u0443\u043a\u0443\u0432\u0430\u0442\u0438 / PDF</button>'
    +'</body></html>');
  w.document.close();
  setTimeout(function(){ try{ w.print(); }catch(e){} }, 420);
}
window.printSchedule=printSchedule;

function printWatermarkCSS(){
  return 'body{position:relative}'
    // Сам знак: великий, дуже блідий, під текстом
    +'.wm{position:fixed;top:50%;left:50%;transform:translate(-50%,-50%) rotate(-28deg);'
      +'font-size:88px;font-weight:900;letter-spacing:6px;color:#000;opacity:.045;'
      +'white-space:nowrap;z-index:0;pointer-events:none;user-select:none}'
    +'.wm-sub{position:fixed;top:calc(50% + 62px);left:50%;'
      +'transform:translate(-50%,-50%) rotate(-28deg);'
      +'font-size:15px;letter-spacing:7px;color:#000;opacity:.05;'
      +'white-space:nowrap;z-index:0;pointer-events:none;user-select:none}'
    // Вміст — над знаком
    +'body>*:not(.wm):not(.wm-sub){position:relative;z-index:1}'
    // Колонтитул із реквізитами
    +'.doc-foot{margin-top:26px;padding-top:10px;border-top:1px solid #ddd;'
      +'font-size:10px;color:#888;text-align:center;line-height:1.6}'
    +'@media print{ .wm,.wm-sub{position:fixed} @page{margin:14mm} }';
}

/** Розмітка водяного знака */
function printWatermarkHTML(){
  var cfg=S.settings||{};
  var name=String(cfg.name||'\u041a\u043e\u043d\u0441\u0442\u0430\u043d\u0442\u0430').toUpperCase();
  return '<div class="wm">'+name+'</div>'
       + '<div class="wm-sub">\u0420\u0415\u041f\u0415\u0422\u0418\u0422\u041e\u0420\u0421\u042c\u041a\u0418\u0419 \u0426\u0415\u041d\u0422\u0420</div>';
}

/** Колонтитул із реквізитами центру */
function printFooterHTML(branch){
  var cfg=S.settings||{};
  var parts=[];
  parts.push(String(cfg.name||'\u0420\u0426 \u00ab\u041a\u043e\u043d\u0441\u0442\u0430\u043d\u0442\u0430\u00bb'));
  if(branch&&branch.name) parts.push(branch.name);
  if(branch&&branch.pay_address) parts.push(branch.pay_address);
  if(branch&&branch.pay_edrpou) parts.push('\u0404\u0414\u0420\u041f\u041e\u0423/\u041a\u043e\u0434: '+branch.pay_edrpou);
  if(cfg.phone) parts.push('\u0442\u0435\u043b. '+cfg.phone);
  var d=new Date();
  var stamp=String(d.getDate()).padStart(2,'0')+'.'+String(d.getMonth()+1).padStart(2,'0')+'.'+d.getFullYear()
    +' '+String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0');
  return '<div class="doc-foot">'+parts.join(' \u00b7 ')
    +'<br>\u0421\u0444\u043e\u0440\u043c\u043e\u0432\u0430\u043d\u043e \u0432 CRM '+stamp+'</div>';
}
window.printWatermarkCSS=printWatermarkCSS;
window.printWatermarkHTML=printWatermarkHTML;
window.printFooterHTML=printFooterHTML;

function printAct(sid, per, editedRows, editedNote, clientInfo){
  if(!canActs()) return;
  var s=(S.students||[]).find(function(x){return x.id===sid;});
  if(!s){ mkToast('\u0423\u0447\u0435\u043D\u044C \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E','error'); return; }
  per=per||actPeriod();

  var bid=myBranchId();
  var branch=(S.branches||[]).find(function(b){return b.id===bid;}) || (S.branches||[])[0];
  var cfg=S.settings||{};

  var rows;
  if(editedRows && editedRows.length){
    rows=editedRows;
  } else {
    var lessons=actLessonsFor(sid, per);
    if(!lessons.length){ mkToast('\u041D\u0435\u043C\u0430\u0454 \u043F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u0438\u0445 \u0437\u0430\u043D\u044F\u0442\u044C \u0437\u0430 \u0446\u0435\u0439 \u043F\u0435\u0440\u0456\u043E\u0434','error'); return; }
    var groups={}, order=[];
    lessons.forEach(function(l){
      var tid=l.tutorId||l.tutor_id;
      var t=tid?(S.tutors||[]).find(function(x){return x.id===tid;}):null;
      var key=(tid||'')+'|'+(l.subject||'');
      if(!groups[key]){ groups[key]={tutor:t, subj:l.subject||'\u0406\u043D\u0448\u0435', lessons:[]}; order.push(key); }
      groups[key].lessons.push(l);
    });
    rows=order.map(function(key){
      var g=groups[key];
      var gHours=Math.round(g.lessons.reduce(function(s,l){return s+(parseFloat(l.dur)||60)/60;},0)*10)/10;
      var gSum=Math.round(g.lessons.reduce(function(s,l){return s+lessonTotal(l);},0)*100)/100;
      return { desc:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440\u0441\u044C\u043A\u0456 \u043F\u043E\u0441\u043B\u0443\u0433\u0438: '+g.subj+(g.tutor?' ('+g.tutor.fn+' '+g.tutor.ln+')':''), hours:gHours, sum:gSum };
    });
  }

  var totalHours=Math.round(rows.reduce(function(s,r){return s+(parseFloat(r.hours)||0);},0)*10)/10;
  var totalSum=Math.round(rows.reduce(function(s,r){return s+(parseFloat(r.sum)||0);},0)*100)/100;

  var actNo=(s.id||'').slice(0,6).toUpperCase()+'-'+per.replace('-','');
  var perLbl=(function(){ var p=per.split('-'); var m=['\u0441\u0456\u0447\u0435\u043D\u044C','\u043B\u044E\u0442\u0438\u0439','\u0431\u0435\u0440\u0435\u0437\u0435\u043D\u044C','\u043A\u0432\u0456\u0442\u0435\u043D\u044C','\u0442\u0440\u0430\u0432\u0435\u043D\u044C','\u0447\u0435\u0440\u0432\u0435\u043D\u044C','\u043B\u0438\u043F\u0435\u043D\u044C','\u0441\u0435\u0440\u043F\u0435\u043D\u044C','\u0432\u0435\u0440\u0435\u0441\u0435\u043D\u044C','\u0436\u043E\u0432\u0442\u0435\u043D\u044C','\u043B\u0438\u0441\u0442\u043E\u043F\u0430\u0434','\u0433\u0440\u0443\u0434\u0435\u043D\u044C'][parseInt(p[1])-1]; return m+' '+p[0]; })();

  var rowsHtml='';
  rows.forEach(function(r,i){
    rowsHtml+='<tr><td>'+(i+1)+'</td><td>'+r.desc+'</td><td class="c">\u0433\u043E\u0434.</td><td class="c">'+r.hours+'</td><td class="r">'+r.sum+'</td></tr>';
  });

  var executor=branch?(branch.pay_recipient||cfg.name||'\u2014'):(cfg.name||'\u2014');
  var execAddr=branch?(branch.pay_address||branch.address||''):'';
  var execEdrpou=branch?(branch.pay_edrpou||''):'';
  var execBank=branch?((branch.pay_bank||'')+(branch.pay_card?', '+branch.pay_card:'')):'';

  // Реквізити замовника: якщо редаговані в модалці — беремо їх, інакше з картки учня
  // (пріоритет — ПІБ батьків/опікуна, бо саме вони юридична сторона договору)
  var clientName=(clientInfo&&clientInfo.name)?clientInfo.name:(s.parentFn||(s.fn+' '+s.ln));
  var clientPhone=(clientInfo&&clientInfo.phone!=null)?clientInfo.phone:(s.parentPhone||s.phone||'');
  var clientAddr=(clientInfo&&clientInfo.addr!=null)?clientInfo.addr:(s.address||'');

  var w=window.open('','_blank');
  w.document.write('<!DOCTYPE html><html><head><meta charset="utf-8"><title>\u0410\u043A\u0442 \u0432\u0438\u043A\u043E\u043D\u0430\u043D\u0438\u0445 \u0440\u043E\u0431\u0456\u0442</title>'
    +'<style>'+printWatermarkCSS()+'body{font-family:Arial,sans-serif;max-width:720px;margin:24px auto;color:#111;font-size:13px;line-height:1.5}'
    +'h1{font-size:16px;text-align:center;margin-bottom:2px}'
    +'.sub{text-align:center;color:#555;font-size:12px;margin-bottom:20px}'
    +'.parties{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px;font-size:12.5px}'
    +'.parties b{display:block;margin-bottom:4px;font-size:12px;text-transform:uppercase;letter-spacing:.3px;color:#555}'
    +'table{width:100%;border-collapse:collapse;font-size:12.5px;margin-top:10px}'
    +'th,td{border:1px solid #999;padding:6px 8px;text-align:left} th{background:#f2f2f2;font-size:11.5px}'
    +'.c{text-align:center} .r{text-align:right}'
    +'.tot td{font-weight:700;background:#f7f7f7}'
    +'.sumtext{margin:14px 0;font-size:13px}'
    +'.note{margin:10px 0;font-size:12.5px;color:#333;white-space:pre-wrap}'
    +'.sign-wrap{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-top:44px}'
    +'.sign-line{border-top:1px solid #111;margin-top:46px;padding-top:4px;font-size:11.5px;color:#333}'
    +'.legal-note{margin-top:26px;font-size:10.5px;color:#888;border-top:1px dashed #ccc;padding-top:8px}'
    +'@media print{.noprint{display:none}}</style></head><body>'
    +printWatermarkHTML()
    +'<h1>\u0410\u041A\u0422 \u2116 '+actNo+'</h1>'
    +'<div class="sub">\u043D\u0430\u0434\u0430\u043D\u043D\u044F \u043E\u0441\u0432\u0456\u0442\u043D\u0456\u0445 \u043F\u043E\u0441\u043B\u0443\u0433 \u0437\u0430 '+perLbl+'<br>\u0432\u0456\u0434 '+new Date().toLocaleDateString('uk-UA')+'</div>'
    +'<div class="parties">'
      +'<div><b>\u0412\u0438\u043A\u043E\u043D\u0430\u0432\u0435\u0446\u044C</b>'+executor
        +(execEdrpou?'<br>\u0406\u041F\u041D/\u0404\u0414\u0420\u041F\u041E\u0423: '+execEdrpou:'')
        +(execAddr?'<br>'+execAddr:'')
        +(execBank?'<br>'+execBank:'')
      +'</div>'
      +'<div><b>\u0417\u0430\u043C\u043E\u0432\u043D\u0438\u043A</b>'+clientName
        +(clientPhone?'<br>'+clientPhone:'')
        +(clientAddr?'<br>'+clientAddr:'')
      +'</div>'
    +'</div>'
    +'<p>\u0426\u0438\u043C \u0430\u043A\u0442\u043E\u043C \u043F\u0456\u0434\u0442\u0432\u0435\u0440\u0434\u0436\u0443\u0454\u0442\u044C\u0441\u044F, \u0449\u043E \u0412\u0438\u043A\u043E\u043D\u0430\u0432\u0435\u0446\u044C \u043D\u0430\u0434\u0430\u0432 \u043D\u0438\u0436\u0447\u0435\u043D\u0430\u0432\u0435\u0434\u0435\u043D\u0456 \u043F\u043E\u0441\u043B\u0443\u0433\u0438, \u0430 \u0417\u0430\u043C\u043E\u0432\u043D\u0438\u043A \u043F\u0440\u0438\u0439\u043D\u044F\u0432 \u0457\u0445 \u0443 \u043F\u043E\u0432\u043D\u043E\u043C\u0443 \u043E\u0431\u0441\u044F\u0437\u0456 \u0431\u0435\u0437 \u0437\u0430\u0443\u0432\u0430\u0436\u0435\u043D\u044C \u0449\u043E\u0434\u043E \u044F\u043A\u043E\u0441\u0442\u0456:</p>'
    +'<table><thead><tr><th>\u2116</th><th>\u041D\u0430\u0439\u043C\u0435\u043D\u0443\u0432\u0430\u043D\u043D\u044F \u043F\u043E\u0441\u043B\u0443\u0433\u0438</th><th class="c">\u041E\u0434.\u0432\u0438\u043C.</th><th class="c">\u041A\u0456\u043B\u044C\u043A\u0456\u0441\u0442\u044C</th><th class="r">\u0421\u0443\u043C\u0430, \u20B4</th></tr></thead><tbody>'
    +rowsHtml
    +'<tr class="tot"><td colspan="3"></td><td class="c">'+totalHours+'</td><td class="r">'+totalSum+'</td></tr>'
    +'</tbody></table>'
    +(editedNote?'<div class="note">'+editedNote.replace(/</g,'&lt;')+'</div>':'')
    +'<div class="sumtext">\u0420\u0430\u0437\u043E\u043C \u043D\u0430\u0434\u0430\u043D\u043E \u043F\u043E\u0441\u043B\u0443\u0433 \u043D\u0430 \u0441\u0443\u043C\u0443 <b>'+totalSum+' \u0433\u0440\u043D.</b> \u041F\u0440\u0435\u0442\u0435\u043D\u0437\u0456\u0439 \u0443 \u0417\u0430\u043C\u043E\u0432\u043D\u0438\u043A\u0430 \u0449\u043E\u0434\u043E \u044F\u043A\u043E\u0441\u0442\u0456, \u043E\u0431\u0441\u044F\u0433\u0443 \u0442\u0430 \u0442\u0435\u0440\u043C\u0456\u043D\u0456\u0432 \u043D\u0430\u0434\u0430\u043D\u043D\u044F \u043F\u043E\u0441\u043B\u0443\u0433 \u043D\u0435 \u043C\u0430\u0454.</div>'
    +'<div class="sign-wrap">'
      +'<div>\u0412\u0438\u043A\u043E\u043D\u0430\u0432\u0435\u0446\u044C:<div class="sign-line">'+executor+' \u00A0\u00A0\u00A0\u00A0 \u041F\u0456\u0434\u043F\u0438\u0441: ______________</div></div>'
      +'<div>\u0417\u0430\u043C\u043E\u0432\u043D\u0438\u043A:<div class="sign-line">'+clientName+' \u00A0\u00A0\u00A0\u00A0 \u041F\u0456\u0434\u043F\u0438\u0441: ______________</div></div>'
    +'</div>'
    +'<div class="legal-note">\u0424\u043E\u0440\u043C\u0430 \u0434\u043E\u043A\u0443\u043C\u0435\u043D\u0442\u0430 \u043E\u0440\u0456\u0454\u043D\u0442\u043E\u0432\u0430\u043D\u0430 \u043D\u0430 \u043E\u0431\u043E\u0432\u2019\u044F\u0437\u043A\u043E\u0432\u0456 \u0440\u0435\u043A\u0432\u0456\u0437\u0438\u0442\u0438 \u043F\u0435\u0440\u0432\u0438\u043D\u043D\u043E\u0433\u043E \u0434\u043E\u043A\u0443\u043C\u0435\u043D\u0442\u0430 (\u0441\u0442.9 \u0417\u0430\u043A\u043E\u043D\u0443 \u00AB\u041F\u0440\u043E \u0431\u0443\u0445\u0433\u0430\u043B\u0442\u0435\u0440\u0441\u044C\u043A\u0438\u0439 \u043E\u0431\u043B\u0456\u043A\u00BB). \u041F\u0435\u0440\u0435\u0432\u0456\u0440\u0442\u0435 \u0432\u0456\u0434\u043F\u043E\u0432\u0456\u0434\u043D\u0456\u0441\u0442\u044C \u0448\u0430\u0431\u043B\u043E\u043D\u0443 \u0432\u0430\u0448\u043E\u043C\u0443 \u0434\u043E\u0433\u043E\u0432\u043E\u0440\u0443 \u0442\u0430 \u0441\u0438\u0441\u0442\u0435\u043C\u0456 \u043E\u043F\u043E\u0434\u0430\u0442\u043A\u0443\u0432\u0430\u043D\u043D\u044F \u0437 \u0431\u0443\u0445\u0433\u0430\u043B\u0442\u0435\u0440\u043E\u043C.</div>'
    +printFooterHTML(branch)
    +'<button class="noprint" onclick="window.print()" style="margin-top:20px;padding:8px 18px;font-size:14px;cursor:pointer">\uD83D\uDDA8 \u0414\u0440\u0443\u043A\u0443\u0432\u0430\u0442\u0438 / \u0417\u0431\u0435\u0440\u0435\u0433\u0442\u0438 \u044F\u043A PDF</button>'
    +'</body></html>');
  w.document.close();
  setTimeout(function(){ try{w.focus();}catch(e){} }, 300);

  // Логуємо факт формування (не підпис!)
  var obj={ id:uid(), student_id:sid, period:per, status:'sent', channel:'print',
    sent_by:CU?CU.id:null, sent_at:new Date().toISOString(), signed_at:null };
  dbInsert('act_log', obj).then(function(){
    if(!(S.actLog||[]).some(function(x){return x.id===obj.id;}))
      S.actLog=(S.actLog||[]).concat([Object.assign({},obj,{studentId:sid,sentBy:obj.sent_by})]);
    renderActsPage();
  }).catch(function(){});
}

window.openActEditM=openActEditM;
window.addActEditRow=addActEditRow;
window.recalcActEditTotal=recalcActEditTotal;
window.printActFromEdit=printActFromEdit;
window.renderActsPage=renderActsPage;

window.printAct=printAct;
window.markActSigned=markActSigned;
