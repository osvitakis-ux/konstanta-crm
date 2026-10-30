
// =
// DATA LOADING
// =
async function loadAll(){
  setSaving();
  // Показуємо скелетон на видимій сторінці, поки вантажаться дані —
  // краще за порожній екран під час першого входу
  try{
    var _pg=document.querySelector('.page.active');
    if(_pg && _pg.innerText.trim().length<40){
      var _sk=document.createElement('div');
      _sk.id='boot-skeleton';
      _sk.innerHTML=skeleton('cards',3)+skeleton('rows',5);
      _pg.appendChild(_sk);
    }
  }catch(e){}
  try{
  await ensureFreshSession(); // оновити токен завчасно, якщо він майже протермінований
  const tables = [
    { table:'branches',      key:'branches' },
    { table:'tutors',        key:'tutors' },
    { table:'students',      key:'students' },
    { table:'lessons',       key:'lessons',  order:'date' },
    { table:'payments',      key:'payments', order:'date' },
    { table:'subjects',      key:'subjects' },
    { table:'comms',         key:'comms',    order:'date' },
    { table:'pricing_rules', key:'pricingRules' },
    { table:'tasks',         key:'tasks' },
  ];
  // Завантажуємо КОЖНУ таблицю посторінково.
  // PostgREST за замовчуванням віддає максимум 1000 рядків. Раніше запит ішов
  // без пагінації, тож щойно занять ставало більше 1000, найстаріші (сортування
  // за датою у спадному порядку) просто переставали приходити — і зникали
  // з розкладу "пластами", починаючи з найдавніших дат.
  // Одна сторінка з ПОВТОРНИМИ спробами. Раніше будь-яка помилка на N-й сторінці
  // (мережевий збій, скидання токена, короткий ліміт запитів) обривала цикл і ціла
  // таблиця приходила порожня/частково — а дані "з'являлися" лише на другому,
  // фоновому завантаженні. Тепер кожну сторінку пробуємо кілька разів.
  async function fetchPage(t, offset, PAGE){
    var res=null;
    for(var attempt=0; attempt<4; attempt++){
      var q = _sb.from(t.table).select('*');
      if(t.order) q = q.order(t.order, { ascending:false });
      q = q.range(offset, offset + PAGE - 1);
      res = await q;
      if(!res.error) return res;
      if(await refreshIfExpired(res.error)) continue;        // JWT — оновили сесію, одразу повтор
      await new Promise(function(r){ setTimeout(r, 400*(attempt+1)); }); // тимчасова помилка — пауза й повтор
    }
    return res;
  }
  async function fetchTableAll(t){
    // Рухаємось за ФАКТИЧНОЮ кількістю рядків, а не за PAGE: якщо сервер (Supabase
    // max-rows) віддає менше за запитане, ми не зупиняємось передчасно й не пропускаємо
    // рядки. Зупинка лише коли сторінка порожня.
    var PAGE=1000, offset=0, all=[], lastErr=null;
    while(true){
      var res = await fetchPage(t, offset, PAGE);
      if(res && res.error){ lastErr=res.error; break; }
      var batch = (res && res.data) || [];
      all = all.concat(batch);
      if(batch.length === 0) break;   // рядків більше немає
      offset += batch.length;         // наступний запит — з фактичного зсуву
      if(offset > 500000) break;      // запобіжник
    }
    return { data: all, error: lastErr };
  }
  function fetchAll(){
    return Promise.all(tables.map(fetchTableAll));
  }
  // Критичні таблиці + налаштування + користувачі — одним паралельним батчем
  var _p1 = await Promise.all([ fetchAll(), _sb.from('settings').select('*').eq('id','main').single(), _sb.from('profiles').select('*') ]);
  var results = _p1[0];
  // Ще одна страховка: якщо якийсь запит усе ж упав на JWT — оновлюємо сесію і повторюємо весь батч
  var jwtErr = results.find(function(r){ return r && r.error && (String((r.error.message||'')+(r.error.code||'')).includes('JWT')||String((r.error.message||'')).includes('expired')||r.error.status===401); });
  if(jwtErr && await refreshIfExpired(jwtErr.error)){
    results = await fetchAll();
  }
  tables.forEach(function(t, i){ if(results[i] && !results[i].error && Array.isArray(results[i].data)) S[t.key] = results[i].data; });
  S.settings = (_p1[1] && _p1[1].data) || {};
  S.users = ((_p1[2] && _p1[2].data) || []).map(normalizeUser);

  // Normalize field names (snake_case  camelCase for UI compat)
  S.students = S.students.map(normalizeStudent);
  S.lessons  = S.lessons.map(normalizeLesson);
  S.payments = S.payments.map(normalizePayment);
  S.tutors   = S.tutors.map(normalizeTutor);
  S.comms    = S.comms.map(normalizeComm);
  S.pricingRules = S.pricingRules.map(normalizePricingRule);
  S.tasks = (S.tasks||[]).map(normalizeTask);
  S.payrollItems = (S.payrollItems||[]).map(normalizePayrollItem);
  S.actLog = (S.actLog||[]).map(function(r){return Object.assign({},r,{studentId:r.student_id,sentBy:r.sent_by,signedAt:r.signed_at});});
  try{ updateTaskAlert(); checkNewTaskNotifications(); }catch(e){}

  // Фонове довантаження сторінко-специфічних таблиць (договори, зарплатні нарахування, акти) —
  // не блокує перший показ дашборда/розкладу; вони потрібні лише на власних сторінках.
  (async function loadBgTables(){
    try{
      var bg=[ {table:'contracts',key:'contracts',order:'created_at'}, {table:'payroll_items',key:'payrollItems'}, {table:'act_log',key:'actLog'} ];
      var r = await Promise.all(bg.map(fetchTableAll));
      bg.forEach(function(t,i){ if(r[i] && !r[i].error && Array.isArray(r[i].data)) S[t.key]=r[i].data; });
      S.payrollItems=(S.payrollItems||[]).map(normalizePayrollItem);
      S.actLog=(S.actLog||[]).map(function(x){return Object.assign({},x,{studentId:x.student_id,sentBy:x.sent_by,signedAt:x.signed_at});});
      try{ if(S.currentPage==='contracts'&&typeof renderContractsPage==='function') renderContractsPage(); }catch(e){}
      try{ if(S.currentPage==='acts'&&typeof renderActsPage==='function') renderActsPage(); }catch(e){}
      try{ if(S.currentPage==='payroll'&&typeof renderPayroll==='function') renderPayroll(); }catch(e){}
    }catch(e){}
  })();

  } finally {
    setSynced();
  }
}

// Normalize DB rows to match UI field names
function normalizeStudent(r){ 
  var tutorIds = r.tutor_ids ? (Array.isArray(r.tutor_ids) ? r.tutor_ids : r.tutor_ids.split(',').filter(Boolean)) : (r.tutor_id ? [r.tutor_id] : []);
  var bIds = r.branch_ids ? (Array.isArray(r.branch_ids) ? r.branch_ids : String(r.branch_ids).split(',').filter(Boolean)) : (r.branch_id ? [r.branch_id] : []);
  return Object.assign({}, r, { tutorId:r.tutor_id, crmStage:r.crm_stage||null, crmResponsible:r.crm_responsible||null, crmDate:r.crm_date||null, tutorIds:tutorIds, branchId:r.branch_id, branchIds:bIds, parentFn:r.parent_fn, parentPhone:r.parent_phone }); 
}
// Нормалізація профілів (S.users) — раніше не існувала ВЗАГАЛІ, тому CU.branchId
// завжди був undefined, і будь-який звичайний адмін помилково "прив'язувався" до
// першої-ліпшої філії в списку (S.branches[0]) замість своєї реальної філії/філій.
function normalizeUser(r){
  var bIds = r.branch_ids ? (Array.isArray(r.branch_ids) ? r.branch_ids : String(r.branch_ids).split(',').filter(Boolean)) : (r.branch_id ? [r.branch_id] : []);
  return Object.assign({}, r, { branchId:r.branch_id, branchIds:bIds });
}
// Статуси, що вважаються ПРОВЕДЕНИМ заняттям (оплачуються, входять в акти,
// рахуються в статистиці). "testing" — тестування/діагностика рівня учня:
// заняття реально відбулось, тому рахується як проведене.
var DONE_STATUSES = ['done','completed','testing','burned'];
// «Проведені» статуси = виконана робота. Ставка/дані таких занять ЗАМОРОЖУЮТЬСЯ:
// подальші зміни ставок чи інших даних на них не впливають (минуле незмінне).
var CONDUCTED_STATUSES = ['done','completed','testing','burned','makeup'];
function isConductedStatus(s){ return CONDUCTED_STATUSES.indexOf(s)>=0; }

// «Згоріле» заняття: учень не прийшов і не попередив.
// Оплачується як проведене (репетитор чекав), але журнал для нього
// заповнювати нічого — тож у перевірці ведення журналу воно не бере участі.
function isBurnedLesson(l){ return !!l && l.status==='burned'; }
window.isBurnedLesson = isBurnedLesson;
function isDoneLesson(l){
  return DONE_STATUSES.indexOf(l && l.status) >= 0;
}
window.isDoneLesson = isDoneLesson;

function normalizeLesson(r){  return Object.assign({}, r, { studentId:r.student_id, tutorId:r.tutor_id, branchId:r.branch_id, recurId:r.recur_id, recurType:r.recur_type, recurIndex:r.recur_index }); }
function normalizePayment(r){ return Object.assign({}, r, { studentId:r.student_id, branchId:r.branch_id }); }
function normalizeTutor(r){
  var bIds = r.branch_ids ? (Array.isArray(r.branch_ids) ? r.branch_ids : String(r.branch_ids).split(',').filter(Boolean)) : (r.branch_id ? [r.branch_id] : []);
  return Object.assign({}, r, { accId:r.acc_uid, branchId:r.branch_id, branchIds:bIds });
}
function normalizeComm(r){    return Object.assign({}, r, { tutorId:r.tutor_id, studentId:r.student_id, branchId:r.branch_id }); }
function normalizePricingRule(r){ return Object.assign({}, r, { subjectMatch:r.subject_match, tutorId:r.tutor_id, gradeMatch:r.grade_match, durMin:r.dur_min }); }
function normalizeTask(r){ return Object.assign({}, r, { assigneeId:r.assignee_id, creatorId:r.creator_id, branchId:r.branch_id, deadlineTime:r.deadline_time, doneAt:r.done_at }); }
function normalizePayrollItem(r){ return Object.assign({}, r, { tutorId:r.tutor_id, adminId:r.admin_id, createdBy:r.created_by }); }
