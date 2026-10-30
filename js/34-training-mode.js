
// ══════════════════════════════════════════════════════════
// НАВЧАЛЬНИЙ РЕЖИМ (ПІСОЧНИЦЯ) — ігровий тур для нових репетиторів
// Активується через ?training=1. Усі дані тестові, живуть лише в
// пам'яті вкладки (S), жоден запис ніколи не йде в Supabase.
// ══════════════════════════════════════════════════════════

var TRAINING_QUESTS = [
  { id:'schedule',  icon:'\uD83D\uDCC5', badge:'\uD83D\uDCC5', title:'\u041F\u0435\u0440\u0448\u0438\u0439 \u043F\u043E\u0433\u043B\u044F\u0434',
    descr:'\u0412\u0456\u0434\u043A\u0440\u0438\u0439\u0442\u0435 \u0441\u0432\u0456\u0439 \u0440\u043E\u0437\u043A\u043B\u0430\u0434',
    check:function(){ return TR._visited.schedule===true; } },
  { id:'student',   icon:'\uD83D\uDC64', badge:'\uD83E\uDDD1\u200D\uD83C\uDF93', title:'\u0417\u043D\u0430\u0439\u043E\u043C\u0441\u0442\u0432\u043E \u0437 \u0443\u0447\u043D\u0435\u043C',
    descr:'\u0412\u0456\u0434\u043A\u0440\u0438\u0439\u0442\u0435 \u043A\u0430\u0440\u0442\u043A\u0443 \u0431\u0443\u0434\u044C-\u044F\u043A\u043E\u0433\u043E \u0443\u0447\u043D\u044F',
    check:function(){ return TR._visited.student===true; } },
  { id:'donelesson',icon:'\u2705', badge:'\u2705', title:'\u041F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E \u0437\u0430\u043D\u044F\u0442\u0442\u044F',
    descr:'\u041F\u043E\u0437\u043D\u0430\u0447\u0442\u0435 \u0431\u0443\u0434\u044C-\u044F\u043A\u0435 \u0437\u0430\u043D\u044F\u0442\u0442\u044F \u044F\u043A "\u041F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E"',
    check:function(){ return (S.lessons||[]).some(function(l){return l.status==='done' && l.id!=='tr-l3';}); } },
  { id:'missedlesson',icon:'\u274C', badge:'\uD83D\uDD34', title:'\u041F\u0440\u043E\u043F\u0443\u0449\u0435\u043D\u0435 \u0437\u0430\u043D\u044F\u0442\u0442\u044F',
    descr:'\u041F\u043E\u0437\u043D\u0430\u0447\u0442\u0435 \u0431\u0443\u0434\u044C-\u044F\u043A\u0435 \u0437\u0430\u043D\u044F\u0442\u0442\u044F \u044F\u043A "\u041F\u0440\u043E\u043F\u0443\u0449\u0435\u043D\u043E" \u0432 \u0440\u043E\u0437\u043A\u043B\u0430\u0434\u0456',
    check:function(){ return (S.lessons||[]).some(function(l){return l.status==='missed' && l.id!=='tr-l5';}); } },
  { id:'makeup',icon:'\uD83D\uDD01', badge:'\uD83D\uDD01', title:'\u0417\u0430\u043F\u043B\u0430\u043D\u043E\u0432\u0430\u043D\u0435 \u0432\u0456\u0434\u043F\u0440\u0430\u0446\u044E\u0432\u0430\u043D\u043D\u044F',
    descr:'\u0414\u043B\u044F \u043F\u0440\u043E\u043F\u0443\u0449\u0435\u043D\u043E\u0433\u043E \u0437\u0430\u043D\u044F\u0442\u0442\u044F \u043F\u043E\u0441\u0442\u0430\u0432\u0442\u0435 \u0432\u0456\u0434\u043F\u0440\u0430\u0446\u044E\u0432\u0430\u043D\u043D\u044F (\u0441\u0442\u0430\u0442\u0443\u0441 "\u0412\u0456\u0434\u043F\u0440. \u0437\u0430\u043F\u043B\u0430\u043D\u043E\u0432\u0430\u043D\u043E")',
    check:function(){ return (S.lessons||[]).some(function(l){return l.status==='makeup_planned'||l.status==='makeup';}); } },
  { id:'monthplan_recur', icon:'\uD83D\uDD01', badge:'\uD83D\uDCC6', title:'\u041C\u0456\u0441\u044F\u0447\u043D\u0438\u0439 \u043F\u043B\u0430\u043D \u0456\u0437 \u043F\u043E\u0432\u0442\u043E\u0440\u0435\u043D\u043D\u044F\u043C',
    descr:'\u0412\u0456\u0434\u043A\u0440\u0438\u0439\u0442\u0435 "\u0417\u0430\u043D\u044F\u0442\u0442\u044F" \u2192 "+ \u0414\u043E\u0434\u0430\u0442\u0438 \u0437\u0430\u043D\u044F\u0442\u0442\u044F" \u2192 \u043E\u0431\u0435\u0440\u0456\u0442\u044C \u0443\u0447\u043D\u044F \u0456 \u0447\u0430\u0441 \u2192 \u0443 \u043F\u043E\u043B\u0456 "\u041F\u043E\u0432\u0442\u043E\u0440\u0435\u043D\u043D\u044F" \u043E\u0431\u0435\u0440\u0456\u0442\u044C "\u0429\u043E\u0442\u0438\u0436\u043D\u044F" \u0456 \u0432\u043A\u0430\u0436\u0456\u0442\u044C \u043A\u0456\u043B\u044C\u043A\u0456\u0441\u0442\u044C \u043F\u043E\u0432\u0442\u043E\u0440\u0456\u0432 (\u043D\u0430\u043F\u0440. 4, \u043D\u0430 \u0432\u0435\u0441\u044C \u043C\u0456\u0441\u044F\u0446\u044C) \u2192 \u0417\u0431\u0435\u0440\u0435\u0433\u0442\u0438. \u0421\u0442\u0432\u043E\u0440\u0438\u0442\u044C\u0441\u044F \u043E\u0434\u0440\u0430\u0437\u0443 \u043A\u0456\u043B\u044C\u043A\u0430 \u0437\u0430\u043D\u044F\u0442\u044C \u043D\u0430\u043F\u0435\u0440\u0435\u0434 \u2014 \u043D\u0435 \u0442\u0440\u0435\u0431\u0430 \u0434\u043E\u0434\u0430\u0432\u0430\u0442\u0438 \u043A\u043E\u0436\u043D\u0435 \u043E\u043A\u0440\u0435\u043C\u043E.',
    check:function(){ return (S.lessons||[]).some(function(l){return !!(l.recurId||l.recur_id);}); } },
  { id:'monthplan_single', icon:'\u2795', badge:'\uD83D\uDCCC', title:'\u041E\u0434\u043D\u043E\u0440\u0430\u0437\u043E\u0432\u0435 \u0437\u0430\u043D\u044F\u0442\u0442\u044F \u0431\u0435\u0437 \u043F\u043E\u0432\u0442\u043E\u0440\u0443',
    descr:'\u0414\u043E\u0434\u0430\u0439\u0442\u0435 \u0449\u0435 \u043E\u0434\u043D\u0435 \u0437\u0430\u043D\u044F\u0442\u0442\u044F \u0434\u043B\u044F \u0431\u0443\u0434\u044C-\u044F\u043A\u043E\u0433\u043E \u0443\u0447\u043D\u044F, \u0430\u043B\u0435 \u0446\u044C\u043E\u0433\u043E \u0440\u0430\u0437\u0443 \u0437\u0430\u043B\u0438\u0448\u0456\u0442\u044C \u043F\u043E\u043B\u0435 "\u041F\u043E\u0432\u0442\u043E\u0440\u0435\u043D\u043D\u044F" = "\u0411\u0435\u0437 \u043F\u043E\u0432\u0442\u043E\u0440\u0443" \u2014 \u0442\u0430\u043A \u0434\u043E\u0434\u0430\u044E\u0442\u044C \u0440\u0430\u0437\u043E\u0432\u0456, \u043D\u0435\u0440\u0435\u0433\u0443\u043B\u044F\u0440\u043D\u0456 \u0437\u0430\u043D\u044F\u0442\u0442\u044F.',
    check:function(){ return (S.lessons||[]).some(function(l){return !l.recurId && !l.recur_id && TR._seedLessonIds.indexOf(l.id)<0 && l.id!=='tr-l-mk-partial';}); } },
  { id:'partial_coverage', icon:'\uD83D\uDFE3', badge:'\uD83C\uDFA8', title:'\u0427\u0430\u0441\u0442\u043A\u043E\u0432\u0435 \u0432\u0456\u0434\u043F\u0440\u0430\u0446\u044E\u0432\u0430\u043D\u043D\u044F \u0456 \u043A\u043E\u043B\u044C\u043E\u0440\u0438',
    descr:'\u0423 \u0411\u043E\u0433\u0434\u0430\u043D\u0430 \u0454 \u043F\u0440\u043E\u043F\u0443\u0449\u0435\u043D\u0435 \u0437\u0430\u043D\u044F\u0442\u0442\u044F \u043D\u0430 2 \u0433\u043E\u0434\u0438\u043D\u0438 (\u0447\u0435\u0440\u0432\u043E\u043D\u0435 \u2014 ❌ \u043F\u0440\u043E\u043F\u0443\u0449\u0435\u043D\u043E). \u0421\u0442\u0432\u043E\u0440\u0456\u0442\u044C \u0432\u0456\u0434\u043F\u0440\u0430\u0446\u044E\u0432\u0430\u043D\u043D\u044F \u043D\u0430 1 \u0433\u043E\u0434\u0438\u043D\u0443 \u0434\u043B\u044F \u0446\u044C\u043E\u0433\u043E \u0436 \u043F\u0440\u043E\u043F\u0443\u0441\u043A\u0443 (\u0432\u043A\u0430\u0436\u0456\u0442\u044C \u0442\u0443 \u0441\u0430\u043C\u0443 \u0434\u0430\u0442\u0443 \u043F\u0440\u043E\u043F\u0443\u0441\u043A\u0443 \u0443 \u043F\u043E\u043B\u0456 "\u0414\u0430\u0442\u0430 \u043F\u0440\u043E\u043F\u0443\u0441\u043A\u0443"). \u0417\u0432\u0435\u0440\u043D\u0456\u0442\u044C \u0443\u0432\u0430\u0433\u0443 \u2014 \u043A\u043E\u043B\u0456\u0440 \u043F\u0440\u043E\u043F\u0443\u0449\u0435\u043D\u043E\u0433\u043E \u0437\u0430\u043D\u044F\u0442\u0442\u044F \u0437\u043C\u0456\u043D\u0438\u0442\u044C\u0441\u044F \u0437 \u0447\u0435\u0440\u0432\u043E\u043D\u043E\u0433\u043E \u043D\u0430 \u0444\u0456\u043E\u043B\u0435\u0442\u043E\u0432\u0438\u0439 \u043F\u0443\u043D\u043A\u0442\u0438\u0440\u043D\u0438\u0439 (\u2014 \u0432\u0456\u0434\u043F\u0440\u0430\u0446\u044C\u043E\u0432\u0430\u043D\u043E \u0447\u0430\u0441\u0442\u043A\u043E\u0432\u043E), \u0430 \u041D\u0415 \u043D\u0430 \u0437\u0435\u043B\u0435\u043D\u0438\u0439/\u0444\u0456\u043E\u043B\u0435\u0442\u043E\u0432\u0438\u0439 \u0441\u0443\u0446\u0456\u043B\u044C\u043D\u0438\u0439 \u2014 \u0431\u043E \u043F\u043E\u043A\u0440\u0438\u0442\u043E \u043B\u0438\u0448\u0435 1 \u0433\u043E\u0434\u0438\u043D\u0443 \u0437 2.',
    check:function(){
      var missed=(S.lessons||[]).find(function(l){return l.id==='tr-l5';});
      if(!missed) return false;
      try{
        var covered = isCoveredMissed(missed);
        var uncH = uncoveredMissedHours(missed);
        return !covered && uncH>0 && uncH*60 < (parseFloat(missed.dur)||120);
      }catch(e){ return false; }
    } },
  { id:'homework',  icon:'\uD83D\uDCDD', badge:'\uD83D\uDCDA', title:'\u0414\u043E\u0434\u0430\u043D\u043E \u0434\u043E\u043C\u0430\u0448\u043D\u0454 \u0437\u0430\u0432\u0434\u0430\u043D\u043D\u044F',
    descr:'\u0417\u0430\u043F\u043E\u0432\u043D\u0456\u0442\u044C \u043F\u043E\u043B\u0435 "\u0414\u043E\u043C\u0430\u0448\u043D\u0454 \u0437\u0430\u0432\u0434\u0430\u043D\u043D\u044F" \u0432 \u0431\u0443\u0434\u044C-\u044F\u043A\u043E\u043C\u0443 \u0437\u0430\u043D\u044F\u0442\u0442\u0456',
    check:function(){ return (S.lessons||[]).some(function(l){return (l.hw||'').trim().length>0;}); } },
  { id:'comm',      icon:'\uD83D\uDCAC', badge:'\uD83D\uDDE8\uFE0F', title:'\u041F\u0435\u0440\u0448\u0435 \u043F\u043E\u0432\u0456\u0434\u043E\u043C\u043B\u0435\u043D\u043D\u044F',
    descr:'\u0414\u043E\u0434\u0430\u0439\u0442\u0435 \u0437\u0430\u043F\u0438\u0441 \u0443 "\u041A\u043E\u043C\u0443\u043D\u0456\u043A\u0430\u0446\u0456\u044F\u0445" \u0434\u043B\u044F \u0431\u0443\u0434\u044C-\u044F\u043A\u043E\u0433\u043E \u0443\u0447\u043D\u044F',
    check:function(){ return (S.comms||[]).length>0; } },
  { id:'task',      icon:'\u2611\uFE0F', badge:'\uD83C\uDFC6', title:'\u0417\u0430\u0432\u0434\u0430\u043D\u043D\u044F \u0432\u0438\u043A\u043E\u043D\u0430\u043D\u043E',
    descr:'\u0412\u0456\u0434\u043A\u0440\u0438\u0439\u0442\u0435 "\u0417\u0430\u0432\u0434\u0430\u043D\u043D\u044F" \u0456 \u043F\u043E\u0437\u043D\u0430\u0447\u0442\u0435 \u043E\u0434\u043D\u0435 \u044F\u043A \u0432\u0438\u043A\u043E\u043D\u0430\u043D\u0435',
    check:function(){ return (S.tasks||[]).some(function(t){return t.status==='done';}); } },
  { id:'profile',   icon:'\uD83D\uDC64', badge:'\u2B50', title:'\u041E\u0441\u043E\u0431\u0438\u0441\u0442\u0438\u0439 \u043F\u0440\u043E\u0444\u0456\u043B\u044C',
    descr:'\u0417\u0430\u0439\u0434\u0456\u0442\u044C \u0443 "\u041C\u0456\u0439 \u043F\u0440\u043E\u0444\u0456\u043B\u044C" \u0456 \u0437\u0430\u043F\u043E\u0432\u043D\u0456\u0442\u044C \u043A\u043E\u0440\u043E\u0442\u043A\u0443 \u0431\u0456\u043E\u0433\u0440\u0430\u0444\u0456\u044E',
    check:function(){ return TR._visited.profile===true; } }
];

var TR = { _visited:{}, _done:{}, _lastCheck:0 };

function trainingSeedData(){
  var today=new Date();
  var ds=function(off){ var d=new Date(today); d.setDate(today.getDate()+off); return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0'); };
  var tutorId='tr-tut-1';
  S.tutors=[{id:tutorId,accId:tutorId,acc_uid:tutorId,fn:'\u0422\u0440\u0435\u043D\u0443\u0432\u0430\u043B\u044C\u043D\u0438\u0439',ln:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440',phone:'',email:'',subj:'\u041C\u0430\u0442\u0435\u043C\u0430\u0442\u0438\u043A\u0430',branchIds:[]}];
  S.students=[
    {id:'tr-s1',fn:'\u0410\u043D\u043D\u0430',ln:'\u041F\u0440\u0438\u043A\u043B\u0430\u0434',status:'active',grade:'7 \u043A\u043B\u0430\u0441',phone:'+380501112233',tutorId:tutorId,tutorIds:[tutorId],rates:[{subject:'\u041C\u0430\u0442\u0435\u043C\u0430\u0442\u0438\u043A\u0430',tutor_id:tutorId,rate:400}]},
    {id:'tr-s2',fn:'\u0411\u043E\u0433\u0434\u0430\u043D',ln:'\u0417\u0440\u0430\u0437\u043E\u043A',status:'active',grade:'9 \u043A\u043B\u0430\u0441',phone:'+380501112234',tutorId:tutorId,tutorIds:[tutorId],rates:[{subject:'\u041C\u0430\u0442\u0435\u043C\u0430\u0442\u0438\u043A\u0430',tutor_id:tutorId,rate:400}]}
  ];
  S.lessons=[
    {id:'tr-l1',studentId:'tr-s1',student_id:'tr-s1',tutorId:tutorId,tutor_id:tutorId,subject:'\u041C\u0430\u0442\u0435\u043C\u0430\u0442\u0438\u043A\u0430',date:ds(0),time:'10:00',dur:60,price:400,status:'planned'},
    {id:'tr-l2',studentId:'tr-s2',student_id:'tr-s2',tutorId:tutorId,tutor_id:tutorId,subject:'\u041C\u0430\u0442\u0435\u043C\u0430\u0442\u0438\u043A\u0430',date:ds(1),time:'14:00',dur:60,price:400,status:'planned'},
    {id:'tr-l3',studentId:'tr-s1',student_id:'tr-s1',tutorId:tutorId,tutor_id:tutorId,subject:'\u041C\u0430\u0442\u0435\u043C\u0430\u0442\u0438\u043A\u0430',date:ds(-2),time:'10:00',dur:60,price:400,status:'done'},
    {id:'tr-l5',studentId:'tr-s2',student_id:'tr-s2',tutorId:tutorId,tutor_id:tutorId,subject:'\u041C\u0430\u0442\u0435\u043C\u0430\u0442\u0438\u043A\u0430',date:ds(-3),time:'12:00',dur:120,price:400,status:'missed'}
  ];
  S.tasks=[
    {id:'tr-t1',title:'\u041F\u0435\u0440\u0435\u0432\u0456\u0440\u0438\u0442\u0438 \u0434\u043E\u043C\u0430\u0448\u043D\u044E \u0440\u043E\u0431\u043E\u0442\u0443 \u0410\u043D\u043D\u0438',assigneeId:tutorId,assignee_id:tutorId,creatorId:'tr-dir',status:'open',deadline:ds(2)}
  ];
  S.payments=[]; S.comms=[]; S.branches=[]; S.subjects=[{name:'\u041C\u0430\u0442\u0435\u043C\u0430\u0442\u0438\u043A\u0430'}]; S.pricingRules=[]; S.payrollItems=[]; S.settings={name:'\u041A\u043E\u043D\u0441\u0442\u0430\u043D\u0442\u0430 (\u043D\u0430\u0432\u0447\u0430\u043D\u043D\u044F)'};
  S.users=[{id:tutorId,fn:'\u0422\u0440\u0435\u043D\u0443\u0432\u0430\u043B\u044C\u043D\u0438\u0439',ln:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440',role:'tutor'}];
  CU={ id:tutorId, fn:'\u0422\u0440\u0435\u043D\u0443\u0432\u0430\u043B\u044C\u043D\u0438\u0439', ln:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440', role:'tutor', perms:{} };
  TR._seedLessonIds = (S.lessons||[]).map(function(l){return l.id;});
}

function initTrainingMode(){
  trainingSeedData();
  window._trainingMode = true;
  window._viewerMode = false; // записи ДОЗВОЛЕНІ, просто локальні (перехоплюються нижче)

  var asEl=document.getElementById('as'); if(asEl)asEl.style.display='block';
  var lsEl=document.getElementById('ls'); if(lsEl)lsEl.style.display='none';
  var setupEl=document.getElementById('setup'); if(setupEl)setupEl.style.display='none';
  var loadDiv=document.getElementById('app-loading'); if(loadDiv)loadDiv.remove();
  if(!document.getElementById('__pcss__')){
    var _st=document.createElement('style'); _st.id='__pcss__';
    _st.textContent='.page{display:none!important}.page.active{display:block!important;padding-bottom:40px}';
    document.head.appendChild(_st);
  }
  buildSidebar();
  try{ buildBottomNav(); }catch(e){} updateSBUser();
  document.body.className = document.body.className.replace(/\brole-\w+\b/g,'');
  document.body.classList.add('role-tutor');

  var banner=document.createElement('div');
  banner.style.cssText='position:fixed;top:0;left:0;right:0;z-index:9999;background:linear-gradient(90deg,#8b5cf6,#ec4899);color:#fff;text-align:center;padding:6px;font-size:12px;font-weight:700';
  banner.innerHTML='\uD83C\uDF93 НАВЧАЛЬНИЙ РЕЖИМ — тестові дані, нічого не впливає на реальну CRM';
  document.body.appendChild(banner);
  var contentEl=document.getElementById('content'); if(contentEl)contentEl.style.paddingTop='34px';

  trainingBuildPanel();
  nav('dashboard');
  setTimeout(trainingCheckProgress, 500);
  setInterval(trainingCheckProgress, 2000);
}

function trainingBuildPanel(){
  if(document.getElementById('tr-panel')) return;
  var done=trainingLoadProgress();
  var doneCount=Object.keys(done).filter(function(k){return done[k];}).length;
  var wrap=document.createElement('div');
  wrap.id='tr-panel';
  wrap.style.cssText='position:fixed;right:16px;bottom:16px;width:280px;background:var(--s1);border:1px solid var(--b1);border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.18);z-index:9998;font-family:inherit;overflow:hidden;transition:transform .25s';
  wrap.innerHTML =
    '<div id="tr-head" style="background:linear-gradient(135deg,#8b5cf6,#ec4899);color:#fff;padding:12px 14px;cursor:pointer;display:flex;justify-content:space-between;align-items:center">'
      +'<div><b style="font-size:13px">\uD83C\uDFAE Навчальні квести</b><div id="tr-progress-txt" style="font-size:11px;opacity:.9">'+doneCount+' / '+TRAINING_QUESTS.length+' виконано</div></div>'
      +'<span id="tr-toggle-ico" style="font-size:16px">\u2212</span>'
    +'</div>'
    +'<div style="height:5px;background:rgba(255,255,255,.25);margin-top:-5px">'
      +'<div id="tr-bar" style="height:100%;background:#fff;width:'+Math.round(doneCount/TRAINING_QUESTS.length*100)+'%;transition:width .4s"></div>'
    +'</div>'
    +'<div id="tr-body" style="max-height:320px;overflow-y:auto;padding:8px"></div>'
    +'<div id="tr-legend" style="padding:8px 10px;border-top:1px solid var(--b1)">'
      +'<div style="font-size:10.5px;font-weight:700;color:var(--t3);text-transform:uppercase;margin-bottom:5px">\uD83C\uDFA8 \u041A\u043E\u043B\u044C\u043E\u0440\u0438 \u0437\u0430\u043D\u044F\u0442\u044C</div>'
      +'<div style="display:flex;flex-direction:column;gap:3px;font-size:11px">'
        +'<div><span style="display:inline-block;width:10px;height:10px;border-radius:3px;background:#29abe2;margin-right:6px"></span>\u0417\u0430\u043F\u043B\u0430\u043D\u043E\u0432\u0430\u043D\u043E</div>'
        +'<div><span style="display:inline-block;width:10px;height:10px;border-radius:3px;background:#22c55e;margin-right:6px"></span>\u041F\u0440\u043E\u0432\u0435\u0434\u0435\u043D\u043E</div>'
        +'<div><span style="display:inline-block;width:10px;height:10px;border-radius:3px;background:#ef4444;margin-right:6px"></span>\u041F\u0440\u043E\u043F\u0443\u0449\u0435\u043D\u043E</div>'
        +'<div><span style="display:inline-block;width:10px;height:10px;border-radius:3px;background:#8b5cf6;margin-right:6px"></span>\u0412\u0456\u0434\u043F\u0440\u0430\u0446\u044C\u043E\u0432\u0430\u043D\u043E \u043F\u043E\u0432\u043D\u0456\u0441\u0442\u044E</div>'
        +'<div><span style="display:inline-block;width:10px;height:10px;border-radius:3px;background:#c4b5fd;border:1px dashed #7c3aed;margin-right:6px"></span>\u0412\u0456\u0434\u043F\u0440\u0430\u0446\u044C\u043E\u0432\u0430\u043D\u043E \u0447\u0430\u0441\u0442\u043A\u043E\u0432\u043E</div>'
        +'<div><span style="display:inline-block;width:10px;height:10px;border-radius:3px;background:#94a3b8;margin-right:6px"></span>\u0412\u0456\u0434\u043F\u0440. \u0437\u0430\u043F\u043B\u0430\u043D\u043E\u0432\u0430\u043D\u043E</div>'
      +'</div>'
    +'</div>'
    +'<div id="tr-badges" style="display:flex;gap:4px;flex-wrap:wrap;padding:8px 10px;border-top:1px solid var(--b1)"></div>'
    +'<div style="padding:8px 10px;border-top:1px solid var(--b1)">'
      +'<button id="tr-reset-btn" style="width:100%;padding:7px;font-size:11.5px;border:1px solid var(--b1);border-radius:8px;background:var(--s2);color:var(--t2);cursor:pointer">\uD83D\uDD04 \u0421\u043A\u0438\u043D\u0443\u0442\u0438 \u043F\u0440\u043E\u0433\u0440\u0435\u0441 \u0456 \u043F\u043E\u0447\u0430\u0442\u0438 \u0437\u0430\u043D\u043E\u0432\u043E</button>'
    +'</div>';
  document.body.appendChild(wrap);
  document.getElementById('tr-head').onclick=function(){
    var body=document.getElementById('tr-body'), badges=document.getElementById('tr-badges'), legend=document.getElementById('tr-legend'), ico=document.getElementById('tr-toggle-ico');
    var collapsed = body.style.display==='none';
    body.style.display = collapsed?'block':'none';
    badges.style.display = collapsed?'flex':'none';
    if(legend) legend.style.display = collapsed?'block':'none';
    ico.textContent = collapsed?'\u2212':'+';
  };
  var resetBtn=document.getElementById('tr-reset-btn');
  if(resetBtn) resetBtn.onclick=function(e){
    e.stopPropagation();
    if(!confirm('\u0421\u043A\u0438\u043D\u0443\u0442\u0438 \u0432\u0435\u0441\u044C \u043F\u0440\u043E\u0433\u0440\u0435\u0441 \u0456 \u043F\u043E\u0432\u0435\u0440\u043D\u0443\u0442\u0438 \u0442\u0435\u0441\u0442\u043E\u0432\u0456 \u0434\u0430\u043D\u0456 \u0434\u043E \u043F\u043E\u0447\u0430\u0442\u043A\u043E\u0432\u043E\u0433\u043E \u0441\u0442\u0430\u043D\u0443? \u0412\u0441\u0456 \u0432\u0430\u0448\u0456 \u0442\u0435\u0441\u0442\u043E\u0432\u0456 \u0437\u043C\u0456\u043D\u0438 \u0431\u0443\u0434\u0435 \u0432\u0442\u0440\u0430\u0447\u0435\u043D\u043E.')) return;
    localStorage.removeItem('training_progress');
    TR._done={}; TR._visited={};
    trainingSeedData();
    if(typeof nav==='function') nav(S.currentPage||'dashboard');
    trainingRenderQuestList();
    mkToast('\uD83D\uDD04 \u041F\u0440\u043E\u0433\u0440\u0435\u0441 \u0441\u043A\u0438\u043D\u0443\u0442\u043E, \u0442\u0435\u0441\u0442\u043E\u0432\u0456 \u0434\u0430\u043D\u0456 \u043F\u043E\u0432\u0435\u0440\u043D\u0435\u043D\u043E');
  };
  trainingRenderQuestList();
}

function trainingLoadProgress(){
  try{ return JSON.parse(localStorage.getItem('training_progress')||'{}'); }catch(e){ return {}; }
}
function trainingSaveProgress(done){
  try{ localStorage.setItem('training_progress', JSON.stringify(done)); }catch(e){}
}

function trainingRenderQuestList(){
  var body=document.getElementById('tr-body');
  if(!body) return;
  var done=trainingLoadProgress();
  body.innerHTML = TRAINING_QUESTS.map(function(q){
    var isDone = !!done[q.id];
    return '<div style="display:flex;align-items:center;gap:8px;padding:7px 6px;border-radius:8px;'+(isDone?'opacity:.55':'')+'">'
      +'<span style="font-size:16px">'+(isDone?'\u2705':q.icon)+'</span>'
      +'<div style="flex:1;min-width:0"><div style="font-size:12px;font-weight:600;'+(isDone?'text-decoration:line-through':'')+'">'+q.title+'</div>'
      +'<div style="font-size:10.5px;color:var(--t3)">'+q.descr+'</div></div>'
    +'</div>';
  }).join('');
  var badgesEl=document.getElementById('tr-badges');
  if(badgesEl){
    badgesEl.innerHTML = TRAINING_QUESTS.filter(function(q){return done[q.id];})
      .map(function(q){return '<span title="'+q.title+'" style="font-size:20px">'+q.badge+'</span>';}).join('')
      || '<span style="font-size:11px;color:var(--t3)">Бейджі з\'являться тут</span>';
  }
}

function trainingCelebrate(quest){
  var pop=document.createElement('div');
  pop.style.cssText='position:fixed;top:50%;left:50%;transform:translate(-50%,-50%) scale(0.6);z-index:10001;background:var(--s1);border:2px solid #8b5cf6;border-radius:20px;padding:28px 36px;text-align:center;box-shadow:0 20px 60px rgba(0,0,0,.3);opacity:0;transition:all .35s cubic-bezier(.34,1.56,.64,1)';
  pop.innerHTML='<div style="font-size:48px">'+quest.badge+'</div><div style="font-weight:800;font-size:16px;margin-top:8px">Квест виконано!</div><div style="font-size:13px;color:var(--t2);margin-top:2px">'+quest.title+'</div>';
  document.body.appendChild(pop);
  requestAnimationFrame(function(){ pop.style.opacity='1'; pop.style.transform='translate(-50%,-50%) scale(1)'; });
  setTimeout(function(){ pop.style.opacity='0'; pop.style.transform='translate(-50%,-50%) scale(0.6)'; setTimeout(function(){pop.remove();}, 350); }, 1800);
  try{ mkToast('\uD83C\uDF89 Квест "'+quest.title+'" виконано!'); }catch(e){}
}

function trainingCheckProgress(){
  if(!window._trainingMode) return;
  var done=trainingLoadProgress();
  var changed=false;
  TRAINING_QUESTS.forEach(function(q){
    if(done[q.id]) return;
    var ok=false;
    try{ ok=!!q.check(); }catch(e){}
    if(ok){ done[q.id]=true; changed=true; trainingCelebrate(q); }
  });
  if(changed){
    trainingSaveProgress(done);
    trainingRenderQuestList();
    var doneCount=Object.keys(done).filter(function(k){return done[k];}).length;
    var txt=document.getElementById('tr-progress-txt'); if(txt) txt.textContent=doneCount+' / '+TRAINING_QUESTS.length+' виконано';
    var bar=document.getElementById('tr-bar'); if(bar) bar.style.width=Math.round(doneCount/TRAINING_QUESTS.length*100)+'%';
    if(doneCount===TRAINING_QUESTS.length){
      setTimeout(function(){
        try{ confetti({count:90}); }catch(e){}
        mkToast('\uD83C\uDFC6 Вітаємо! Ви пройшли весь навчальний тур!');
      }, 2000);
    }
  }
}

// Позначаємо відвідування сторінок (для квестів schedule/student/profile)
var _trainingOrigNav = window.nav;
window.nav = function(page){
  var r = _trainingOrigNav.apply(this, arguments);
  if(window._trainingMode){
    if(page==='schedule') TR._visited.schedule=true;
    if(page==='profile') TR._visited.profile=true;
    trainingCheckProgress();
  }
  return r;
};
var _trainingOrigOpenStudM = window.openStudM;
if(_trainingOrigOpenStudM){
  window.openStudM = function(id){
    var r=_trainingOrigOpenStudM.apply(this, arguments);
    if(window._trainingMode && id){ TR._visited.student=true; trainingCheckProgress(); }
    return r;
  };
}
