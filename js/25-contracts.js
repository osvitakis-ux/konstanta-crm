function fmtContractDate(iso){ var d=new Date(iso+'T00:00:00'); if(isNaN(d.getTime())) return iso||''; return '\u00ab'+String(d.getDate()).padStart(2,'0')+'\u00bb '+MONTHS_UK[d.getMonth()]+' '+d.getFullYear(); }
function fmtDatePlain(iso){ var d=new Date(iso+'T00:00:00'); if(isNaN(d.getTime())) return iso||'________'; return d.getDate()+' '+MONTHS_UK[d.getMonth()]+' '+d.getFullYear(); }
function nextContractNumber(){
  var year=new Date().getFullYear(), max=0;
  (S.contracts||[]).forEach(function(c){ var m=String(c.number||'').match(new RegExp('^'+year+'\\/(\\d+)$')); if(m){ var n=parseInt(m[1],10); if(n>max) max=n; } });
  return year+'/'+String(max+1).padStart(3,'0');
}
function buildContractHTML(d){
  var esc=function(s){ return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); };
  var line=function(n){ return new Array((n||40)+1).join('_'); };
  var val=function(v,n){ return (v!=null && String(v).trim()!=='') ? esc(v) : line(n||30); };
  var bval=function(v,n){ return '<b>'+val(v,n)+'</b>'; };
  var dmy=function(iso){ if(!iso) return line(22); var p=String(iso).split('-'); return p.length===3? p[2]+'.'+p[1]+'.'+p[0] : esc(iso); };
  var relWords=['батьком','матір’ю','опікуном'];
  var relIdx=({'батько':0,'матір':1,'опікун':2})[d.relation]; if(relIdx==null) relIdx=-1;
  var relHtml=relWords.map(function(w,i){ return i===relIdx? '<b><u>'+w+'</u></b>' : w; }).join(', ');
  var sd = '<b>'+(d.signDate? fmtContractDate(d.signDate)+' р.' : '«____»_____________ 20___ р.')+'</b>';
  var tms = (d.times&&String(d.times).trim())? esc(d.times) : '______';
  var endTxt = d.endDate? fmtDatePlain(d.endDate) : '31 серпня 2027';
  var centerLines = String(d.center||'').split('\n').map(function(x){ return esc(x); }).join('<br>');
  // Послуги: тільки заповнені, з перенумерацією
  var sList=(d.services||[]).filter(function(x){ return x && String(x).trim()!==''; });
  var svcHtml=''; sList.forEach(function(sv,i){ svcHtml+='<p>2.1.'+(i+1)+'. <b>'+esc(sv)+'</b></p>'; });

  var H='<!doctype html><html lang="uk"><head><meta charset="utf-8"><title>Договір '+esc(d.number||'')+'</title><style>'
    +'@page{size:A4;margin:10mm 10mm 10mm 15mm}'
    +'body{font-family:"Times New Roman",serif;font-size:11pt;line-height:1.3;color:#000;margin:0}'
    +'@media screen{body{max-width:210mm;margin:0 auto;padding:10mm 10mm 10mm 15mm;box-sizing:border-box}}'
    +'h1{font-size:12.5pt;text-align:center;margin:0 0 8px;font-weight:bold}'
    +'.sec{font-weight:bold;margin:10px 0 4px;text-align:center}'
    +'p{margin:4px 0;text-align:justify}'
    +'u{text-decoration:underline}'
    +'.hl{background:#fff23d}'
    +'.top{display:flex;justify-content:space-between;margin:2px 0 8px}'
    +'.cols{display:flex;justify-content:space-between;gap:24px;margin-top:12px}'
    +'.cols>div{width:48%}'
    +'.cols p{text-align:left;margin:2px 0}'
    +'@media print{.noprint{display:none}}'
    +'</style></head><body>';

  H+='<h1>ДОГОВІР про надання репетиторських послуг № '+bval(d.number,14)+'</h1>';
  H+='<p class="top"><span>м. Одеса</span><span>'+sd+'</span></p>';

  H+='<p>Фізична особа - підприємець Когут Іван Сергійович <b>РЦ «Константа»</b>, який діє на підставі виписки з Єдиного державного реєстру юридичних осіб, фізичних осіб-підприємців та громадських формувань, що іменується надалі «Виконавець», з однієї сторони,   та '+bval(d.zamovnyk,60)+',</p>';
  H+='<p>РНОКПП '+bval(d.rnokpp,20)+', що іменується надалі «Замовник», який (а) є '+relHtml+' (необхідне підкреслити) дитини '+bval(d.dytyna,60)+', дата народження <b>'+dmy(d.dob)+'</b>, з другої сторони, які разом надалі іменовані «Сторони», а кожен окремо - «Сторона», уклали цей Договір про   надання репетиторських послуг №'+bval(d.number,12)+'                             від   '+sd+', надалі за текстом – «Договір», про наступне:</p>';

  H+='<p class="sec">1. ПРЕДМЕТ ДОГОВОРУ.</p>';
  H+='<p>1.1. Виконавець зобов’язується надати репетиторські послуги, а Замовник зобов’язується прийняти та оплатити надані послуги (надалі за текстом Договору – «Послуги») в порядку та на умовах визначених цим Договором.</p>';

  H+='<p class="sec">2. ВІДОМОСТІ ПРО ПОСЛУГИ.</p>';
  H+='<p>2.1. Згідно з цього Договору, Виконавець надає Замовнику наступні репетиторські послуги, які включають:</p>';
  H+=svcHtml;
  H+='<p>2.2. Репетиторські послуги, що надаються за цим Договором, можуть також включати й інші, ніж ті, що зазначені в п. 2.1. цього Договору, подібні Послуги, необхідність надання та тривалість яких визначається за погодженням Сторін.</p>';
  H+='<p>2.3. Прийняття Замовником послуг оформляється актом надання послуг.</p>';

  H+='<p class="sec">3. РОЗМІР ТА УМОВИ ОПЛАТИ.</p>';
  H+='<p>3.1. Всі суми, що підлягають сплаті за цим Договором, здійснюються в національній валюті України у безготівковій формі оплати на реквізити , зазначені в п.8 даного договору.</p>';
  H+='<p>3.2. Ціни та тривалість Послуг встановлюється відповідно до п. 2.1.  до цього Договору.</p>';
  H+='<p>3.3. Надання послуг надається шляхом проведення запланованих за розкладом індивідуальних та групових занять <b>'+tms+'</b> рази на тиждень за очною формою навчання.</p>';
  H+='<p>3.4. Надання послуг здійснюється на умовах попередньої (авансової) сплати. Розмір сплати розраховується як ціну уроку помножено на  кількість уроків, визначеної Замовником та Виконавцем після тестування  за кількістю календарних днів поточного місяця. На сплату даного рахунку є <span class="hl">3 робочих дні</span>, але не пізніше, ніж <span class="hl">за добу</span> до проведення планового (наступного після тестування) уроку.</p>';
  H+='<p>3.5. Рахунок на сплату наступного місяця навчання відправляється Виконавцем з 25-го до 30(31)-го числа поточного місяця і оплачується Замовником  до <span class="hl">1 числа</span> місяця, в якому планується надаватись послуга. На підтвердження сплати Замовник отримує квитанцію від банка, фото якої надсилає Виконавцю.</p>';
  H+='<p>3.6. У разі пропуску заплановані заняття підлягають <span class="hl">відпрацюванню</span> протягом поточного та наступних 2 (двох) місяців поза плановим часом на підставі графіка відпрацювання, який узгоджується Сторонами. </p>';
  H+='<p>3.7. Вартість пропущених занять <span class="hl">не вираховується з рахунку на наступний навчальний місяць</span>, а відпрацьовується відповідно п. 3.6.</p>';
  H+='<p>3.8. У разі пропуску заняття Замовником без попередження про <span class="hl">відсутність менше ,ніж за 1 год</span> до заняття без поважних обставин/форс-мажорів (повітряна тривога, робота ППО, різке погіршення погодних умов, шкода здоров’ю дитини під час дороги на заняття, погіршення стану здоров’я батьків/опікунів, які приводять дитину на заняття) грошові кошти не повертаються  та такий пропуск не відпрацьовується, тобто заняття вважається проведеним, а послуга наданою.</p>';
  H+='<p>3.9. Якщо Замовник планує <span class="hl">змінити розклад</span> навчання або кількість уроків на тиждень на наступний місяць, то має повідомити Замовнику <span class="hl">до 25 числа</span> поточного місяця в письмовому вигляді.</p>';
  H+='<p>3.10. Внесені кошти за послугу не повертаються Замовнику, а можуть бути перераховані на іншу послугу, зазначену в даному Договорі за попередньою усною згодою Замовника.</p>';
  H+='<p>3.11. Матеріальну відповідальність за поламані меблі та канцелярське приладдя, що використовується неповнолітньою дитиною Замовника / Замовником несе сам Замовник і у випадку пошкодження сплачується Замовником в сумі матеріалів + робота .</p>';
  H+='<p>3.12. Порушення п. 3.3 - п. 3.10 призводить до розірвання даного договору і не зберігає за Замовником попередній розклад та ціну за урок. </p>';
  H+='<p>3.13. Акт про надані послуги надається Виконавцем за вимогою Замовника та підписується Сторонами до 10 (десятого) числа місяця, наступного за календарним місяцем, в якому фактично надавалися послуги. </p>';
  H+='<p>3.14. У випадку відсутності письмової вимоги надання Акту про надані послуги Замовником на протязі 5 днів Акт вважається наданим, а послуги, надані Виконавцем, вважаються прийнятими.</p>';
  H+='<p>3.15. Акт наданих послуг створюється виконавцем за письмовою вимогою Замовника в електронній формі, підписується електронним цифровим підписом  та відправляється на вказану ним електронну пошту.</p>';
  H+='<p>3.16. Замовник зобов’язується протягом 5 (п’яти) робочих днів з дати отримання Акта наданих послуг надіслати підписаний за допомогою свого електронного цифрового підпису Акт наданих послуг. У разі, якщо в зазначений термін Замовником не буде відправлений підписаний Акт, або вмотивована відмова від його підписання, послуги, надані Виконавцем, вважаються прийнятими.</p>';

  H+='<p class="sec">4. ОБОВ’ЯЗКИ СТОРІН.</p>';
  H+='<p>4.1. Обов’язки Виконавця:</p>';
  H+='<p>4.1.1. Своєчасно та якісно надавати послуги, зазначені в п. 2.1. цього Договору.</p>';
  H+='<p>4.1.2. При виникненні обставин, що перешкоджають належному виконанню своїх зобов’язань, згідно з цим Договором, терміново повідомити про це Замовника і не пізніше, ніж за 1 год до початку заняття. </p>';
  H+='<p>4.1.3. Складати та передавати Замовнику  акти про надання послуг на електронну пошту Замовника або в роздрукованому вигляді.</p>';
  H+='<p>4.2. Обов’язки Замовника:</p>';
  H+='<p>4.2.1. Приймати від Виконавця послуги, що надаються згідно з цим Договором.</p>';
  H+='<p>4.2.2. Після перевірки достовірності актів про надання послуг Виконавцем підписувати акти протягом 5-ти календарних днів з моменту їх одержання.</p>';
  H+='<p>4.2.3. Оплачувати послуги на умовах та в порядку, зазначеному в розділі 3 цього Договору.</p>';

  H+='<p class="sec">5. ВІДПОВІДАЛЬНІСТЬ СТОРІН ТА ВИРІШЕННЯ СПОРІВ.</p>';
  H+='<p>5.1. У випадку порушення своїх зобов’язань за цим Договором Сторони несуть відповідальність, визначену цим Договором та чинним законодавством. Порушенням зобов’язання є його невиконання або неналежне виконання, тобто виконання з порушенням умов, визначених змістом зобов’язання.</p>';
  H+='<p>5.2. Сторони не несуть відповідальність за порушення своїх зобов’язань за цим Договором, якщо воно сталося не з їх вини. Сторона вважається невинною, якщо вона доведе, що вжила всіх залежних від неї заходів для належного виконання зобов’язання.</p>';
  H+='<p>5.3. Жодна із Сторін не несе відповідальність за невиконання чи неналежне виконання своїх зобов’язань по цьому Договору, якщо це невиконання чи неналежне виконання зумовлені дією обставин непереборної сили (форс- мажорних обставин). Сторона, для якої склались форс-мажорні обставини, зобов’язана не пізніше ніж за 24 години з дати настання таких обставин повідомити про це іншу Сторону.</p>';

  H+='<p class="sec">6. СТРОК ДІЇ ДОГОВОРУ ТА ІНШІ УМОВИ.</p>';
  H+='<p>6.1. Договір набуває чинності з моменту його підписання Сторонами і діє до моменту повного виконання Сторонами своїх зобов’язань за цим Договором, <span class="hl">але не пізніше, ніж до кінця навчального року, тобто до <b>'+endTxt+'</b> року.</span></p>';
  H+='<p>6.2. Умови даного Договору можуть бути змінені за взаємною згодою Сторін з обов’язковим складанням письмового документа.</p>';
  H+='<p>6.3. Договір може бути розірваний за взаємною згодою сторін. Одностороннє розірвання Договору можливе лише в випадках, передбачених цим Договором та законодавством України.</p>';
  H+='<p>6.4. Кожна з сторін має право розірвати цей Договір в односторонньому порядку, попередньо письмово повідомивши про це другу сторону за 3 дні.</p>';
  H+='<p>6.5. Сторони підтверджують, що цей Договір містить усі істотні умови, передбачені для договорів цього виду, і жодна зі Сторін не буде посилатися у майбутньому на недосягнення згоди щодо істотних умов договору як на підставу вважати його неукладеним або недійсним.</p>';
  H+='<p>6.6. Усі спори, що пов’язані з цим Договором, вирішуються шляхом переговорів між Сторонами. Якщо спір не може бути вирішений шляхом переговорів, він вирішується в судовому порядку за встановленою підвідомчістю та підсудністю такого спору, визначеному відповідним чинним законодавством України.</p>';
  H+='<p>6.7. Даний Договір укладено у двох оригінальних примірниках, по одному для кожної із сторін.</p>';
  H+='<p>6.8. Підписуючи цей договір, відповідно до статті 207 ЦК України, Сторони погодили використання електронних документів та кваліфікованого електронного підпису (далі — КЕП) в документообігу в межах виконання цього Договору.</p>';
  H+='<p>6.9. У випадках, не передбачених даним Договором, сторони керуються нормами чинного законодавства.</p>';
  H+='<p>6.10. Після підписання цього Договору всі попередні переговори за ним, листування, попередні угоди та протоколи про наміри з питань, що так чи інакше стосуються цього Договору, втрачають юридичну силу.</p>';

  H+='<p class="sec">7. ПЕРСОНАЛЬНІ ДАНІ.</p>';
  H+='<p>7.1. Фізичні особи, що підписали цей Договір, своїми підписами засвідчують, що вони надали згоду на обробку своїх персональних даних, наданих згідно цього Договору, з метою забезпечення реалізації відповідних цивільно-правових та господарсько-правових відносин Сторін у базах персональних даних контрагентів Сторін, та підтверджують, що вони повідомлені про включення цієї інформації до бази персональних даних контрагентів Сторін та права суб’єкта персональних даних згідно ст. 8 Закону України «Про захист персональних даних».</p>';
  H+='<p>7.2 Підписуючи цей документ, Замовник надає Репетиторському центру «Константа» право безстроково відтворювати, відображати та поширювати в усіх країнах світу в будь-якому традиційному або електронному медіа-форматі зображення Замовника та його неповнолітньої дитини , що показано на фотографіях та відеоматеріалах, про які йдеться нижче і які належать РЦ «Константа» , з метою просування та популяризації освіти, а також для просування ідей миру, толерантності та прав дитини. Використання фотографій та відеоматеріалів можливе у редакційних цілях - для публікації у журналах, газетах, електронних засобах масової інформації, звітах, на веб-сайтах тощо. РЦ «Константа» також може використовувати їх у рекламних і комунікаційних кампаніях  - у своїй друкованій, зовнішній рекламі, рекламі в Інтернеті тощо.</p>';
  H+='<p>7.3. Замовник відмовляється від будь-яких претензій на компенсацію або відшкодування шкоди, пов’язаної з використанням РЦ «Константа» його чи дитячого зображення, а також від права вимагати надавати на перегляд або затвердження фінальні версії фотографій.</p>';

  H+='<p class="sec">8. ЮРИДИЧНІ АДРЕСИ, БАНКІВСЬКІ РЕКВІЗИТИ ТА ПІДПИСИ СТОРІН.</p>';
  H+='<div class="cols">';
  H+= '<div><p><b>Виконавець:</b></p><p>'+centerLines+'</p><p>______________________  Іван КОГУТ</p></div>';
  H+= '<div><p><b>Замовник:</b></p>'
     +'<p>'+val(d.zamovnyk,45)+'</p>'
     +'<p>Адреса: '+val(d.address,45)+'</p>'
     +'<p>РНОКПП '+val(d.rnokpp,32)+'</p>'
     +'<p>Контактний телефон: '+val(d.parentPhone,25)+'</p>'
     +'<p>Ел. адреса: '+val(d.email,25)+'</p>'
     +'<p style="margin-top:16px">_________________        ____________________</p>'
     +'<p style="text-align:center;font-size:9pt">(Підпис)                              (Ім’я  Прізвище)</p>'
     +'</div>';
  H+='</div>';
  H+='</body></html>';
  return H;
}

function contractServicesFromStudent(s){
  var out=[], seen={};
  var rates=Array.isArray(s.rates)?s.rates:[];
  rates.forEach(function(r){ var sub=(r&&r.subject)||''; if(sub&&!seen[sub]){seen[sub]=1;out.push(sub);} });
  if(!out.length && s.subject) String(s.subject).split(',').forEach(function(x){ x=x.trim(); if(x&&!seen[x]){seen[x]=1;out.push(x);} });
  return out;
}
// ── Реквізити корпусів (для розділу 8 договору) ──
var CONTRACT_CORPUS_LABELS={corpus2:'Корпус 2', corpus3:'Корпус 3', shevchenko:'Шевченківський корпус'};
var CONTRACT_CORPUS_DEFAULT='ФОП Когут Іван Сергійович РЦ «Константа»\nВиписка з ЄДР № 2005400010002001903 від 26.05.2025 р.\nАдреса: 67900, Одеська область, Подільський район, с. Окни, вул. Центральна, буд. 8, кв. 8\nРНОКПП 3274321537\nРахунок IBAN: UA653052990000026003024921336\nАТ КБ «ПРИВАТБАНК»\nКонтактний телефон: +380970280685\nЕл. адреса: rc.constanta123@gmail.com';
function getCorpusReqAll(){ try{ var raw=localStorage.getItem('contract_corpus_req'); if(raw) return JSON.parse(raw); }catch(e){} return {}; }
function getCorpusReq(key){ var all=getCorpusReqAll(); return (all && all[key]!=null && all[key]!=='') ? all[key] : CONTRACT_CORPUS_DEFAULT; }
function saveCorpusReq(key, val){ var all=getCorpusReqAll(); all[key]=val; try{ localStorage.setItem('contract_corpus_req', JSON.stringify(all)); }catch(e){} }
function onCorpusChange(){ var sel=document.getElementById('ct-corpus'); if(!sel) return; var ta=document.getElementById('ct-center'); if(ta) ta.value=getCorpusReq(sel.value||'corpus2'); }
window.onCorpusChange=onCorpusChange;

function openContractDialog(studentId){
  if(R()!=='god'){ mkToast('Доступно лише для адміна системи','error'); return; }
  window._contractStudentId=studentId||null;
  var set=function(id,v){ var el=document.getElementById(id); if(el) el.value=(v==null?'':v); };
  set('ct-number', nextContractNumber());
  set('ct-date', new Date().toISOString().slice(0,10));
  var s = studentId ? (S.students||[]).find(function(x){return x.id===studentId;}) : null;
  if(s){
    var svc=contractServicesFromStudent(s);
    set('ct-zamovnyk', s.parentFn||'');
    set('ct-dytyna', ((s.ln||'')+' '+(s.fn||'')).trim());
    set('ct-dob', s.child_dob||s.dob||'');
    set('ct-parent-phone', s.parentPhone||'');
    set('ct-phone', s.phone||'');
    set('ct-email', s.email||'');
    set('ct-grade', s.grade||'');
    set('ct-s1', svc[0]||''); set('ct-s2', svc[1]||''); set('ct-s3', svc[2]||'');
    set('ct-rnokpp','');
  } else {
    ['ct-zamovnyk','ct-dytyna','ct-dob','ct-parent-phone','ct-phone','ct-email','ct-grade','ct-s1','ct-s2','ct-s3','ct-rnokpp'].forEach(function(id){set(id,'');});
  }
  var _rel=document.getElementById('ct-relation'); if(_rel) _rel.value='батько';
  set('ct-times','');
  set('ct-address','');
  (function(){ var _n=new Date(); var _y=_n.getMonth()>=8?_n.getFullYear()+1:_n.getFullYear(); set('ct-enddate', _y+'-08-31'); })();
  var _c=document.getElementById('ct-corpus'); if(_c) _c.value='corpus2';
  onCorpusChange();
  openM('mo-contract');
}
function openNewContract(){ openContractDialog(null); }
window.openNewContract=openNewContract;

async function generateContract(){
  if(R()!=='god') return;
  var g=function(id){ return (document.getElementById(id)||{value:''}).value.trim(); };
  var corpus=g('ct-corpus')||'corpus2';
  var d={
    number:g('ct-number'), signDate:g('ct-date'), endDate:g('ct-enddate'),
    zamovnyk:g('ct-zamovnyk'), rnokpp:g('ct-rnokpp'), relation:g('ct-relation'), address:g('ct-address'),
    dytyna:g('ct-dytyna'), dob:g('ct-dob'),
    services:[g('ct-s1'),g('ct-s2'),g('ct-s3')], times:g('ct-times'),
    parentPhone:g('ct-parent-phone'), phone:g('ct-phone'), email:g('ct-email'), grade:g('ct-grade'),
    corpus:corpus, center:g('ct-center')
  };
  var _req=[['number','Номер договору'],['signDate','Дата підписання'],['endDate','Договір діє до'],['zamovnyk','Замовник (ПІБ)'],['parentPhone','Телефон замовника'],['rnokpp','РНОКПП'],['relation','Ким є дитині'],['address','Адреса замовника'],['dytyna','ПІБ дитини'],['dob','Дата народження'],['grade','Клас'],['times','Занять на тиждень'],['corpus','Корпус'],['center','Реквізити корпусу']];
  var _miss=[]; _req.forEach(function(f){ if(!String(d[f[0]]||'').trim()) _miss.push(f[1]); });
  if(!(d.services||[]).some(function(x){return String(x||'').trim();})) _miss.push('Хоча б одна послуга (п. 2.1)');
  if(_miss.length){ mkToast("Заповніть обов'язкові поля: "+_miss.join(', '),'error'); return; }
  saveCorpusReq(corpus, d.center);
  var w=window.open('','_blank');
  if(w){ w.document.write(buildContractHTML(d)); w.document.close(); setTimeout(function(){ try{w.focus();w.print();}catch(e){} },400); }
  try{
    var rec={ id:uid(), number:d.number, sign_date:d.signDate, student_id:window._contractStudentId||null, status:'signed', data:d, created_by:(CU&&CU.id)||null, created_at:new Date().toISOString() };
    await dbInsert('contracts', rec);
    if(!(S.contracts||[]).some(function(x){return x.id===rec.id;})) S.contracts=(S.contracts||[]).concat([rec]);
    mkToast('Договір '+d.number+' збережено');
    closeM('mo-contract');
    if(S.currentPage==='contracts') renderContractsPage();
    try{ if(window._contractStudentId) renderStudentContracts(window._contractStudentId); }catch(e){}
  }catch(e){ var _m=(e&&e.message)||String(e); if(/contract/i.test(_m)&&/(exist|relation|schema|could not find|not find)/i.test(_m)) mkToast('Таблиця "contracts" не створена в Supabase — виконайте SQL-міграцію.','error'); else mkToast('Помилка збереження: '+_m,'error'); }
}

async function createStudentFromContract(cid){
  if(R()!=='god'){ mkToast('Доступно лише для адміна системи','error'); return; }
  var c=(S.contracts||[]).find(function(x){return x.id===cid;}); if(!c){ mkToast('Договір не знайдено','error'); return; }
  if(c.student_id){ mkToast('Картку учня вже сформовано для цього договору','error'); return; }
  var d=c.data||{};
  var pib=String(d.dytyna||'').trim().split(/\s+/).filter(Boolean);
  var ln=pib[0]||'', fn=pib.slice(1).join(' ')||'';
  if(!fn && ln){ fn=ln; ln=''; }
  var services=(d.services||[]).filter(Boolean);
  if(!fn && !ln){ mkToast('У договорі не вказано ПІБ дитини','error'); return; }
  if(!confirm('Сформувати картку учня «'+((fn+' '+ln).trim())+'» з даних договору № '+c.number+'?')) return;
  var obj={
    id:uid(), fn:fn, ln:ln,
    phone:d.phone||'', email:d.email||'', grade:d.grade||'',
    parent_fn:d.zamovnyk||'', parent_phone:d.parentPhone||'',
    status:'active', src:'referral', crm_stage:'won',
    subject: services.join(', ')
  };
  try{
    await dbInsert('students', obj);
    if(!(S.students||[]).some(function(x){return x.id===obj.id;})) S.students=(S.students||[]).concat([normalizeStudent(obj)]);
    await dbUpdate('contracts', cid, {student_id:obj.id});
    c.student_id=obj.id;
    mkToast('Картку учня сформовано ✓');
    if(S.currentPage==='contracts') renderContractsPage();
    try{ if(S.currentPage==='students' && typeof renderStudents==='function') renderStudents(); }catch(e){}
  }catch(e){ mkToast('Помилка створення картки: '+(e.message||e),'error'); }
}
window.createStudentFromContract=createStudentFromContract;

function renderContractsPage(){
  var el=document.getElementById('pg-contracts'); if(!el) return;
  var esc=function(s){ return String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); };
  if(R()!=='god'){ el.innerHTML='<div style="padding:20px;color:var(--t3)">Доступно лише для адміна системи</div>'; return; }
  var list=(S.contracts||[]).slice().sort(function(a,b){ return (String(b.sign_date||'').localeCompare(String(a.sign_date||''))) || (String(b.created_at||'').localeCompare(String(a.created_at||''))); });
  var head='<div style="display:flex;align-items:center;gap:12px;margin-bottom:14px">'
    +'<h2 style="margin:0;font-size:19px">📄 Договори</h2>'
    +'<span style="color:var(--t3);font-size:13px">'+list.length+'</span>'
    +'<button class="btn btn-p" style="margin-left:auto" onclick="openNewContract()">＋ Додати договір</button></div>';
  var body;
  if(!list.length){
    body='<div style="padding:26px;text-align:center;color:var(--t3);border:1px dashed var(--b1);border-radius:12px">Ще немає договорів. Натисніть «＋ Додати договір», заповніть картку — потім за потреби «Сформувати картку учня».</div>';
  } else {
    body='<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:13px">'
      +'<thead><tr style="text-align:left;color:var(--t3);border-bottom:2px solid var(--b1)">'
      +'<th style="padding:8px">№</th><th style="padding:8px">Дата</th><th style="padding:8px">Замовник</th><th style="padding:8px">Дитина</th><th style="padding:8px">Корпус</th><th style="padding:8px">Картка учня</th><th style="padding:8px;text-align:right">Дії</th></tr></thead><tbody>'
      + list.map(function(c){
          var d=c.data||{};
          var studCell = c.student_id
            ? '<span style="color:var(--ok,#16a34a);font-weight:600">✓ створено</span>'
            : '<button class="btn btn-p btn-sm" onclick="createStudentFromContract(\''+c.id+'\')">＋ Сформувати картку учня</button>';
          return '<tr style="border-bottom:1px solid var(--s3)">'
            +'<td style="padding:8px;font-weight:600">'+esc(c.number)+'</td>'
            +'<td style="padding:8px;color:var(--t3);white-space:nowrap">'+esc(fmtContractDate(c.sign_date))+'</td>'
            +'<td style="padding:8px">'+esc(d.zamovnyk||'')+'</td>'
            +'<td style="padding:8px">'+esc(d.dytyna||'')+'</td>'
            +'<td style="padding:8px;white-space:nowrap">'+esc(CONTRACT_CORPUS_LABELS[d.corpus]||'—')+'</td>'
            +'<td style="padding:8px">'+studCell+'</td>'
            +'<td style="padding:8px;text-align:right;white-space:nowrap">'
              +'<button class="btn btn-g btn-sm" onclick="reprintContract(\''+c.id+'\')" title="Відкрити / друк">🖨</button> '
              +'<button class="btn btn-d btn-sm" onclick="delContract(\''+c.id+'\')" title="Видалити">🗑</button>'
            +'</td></tr>';
        }).join('')
      +'</tbody></table></div>';
  }
  el.innerHTML=head+body;
}
window.renderContractsPage=renderContractsPage;
window.openContractDialog=openContractDialog; window.generateContract=generateContract;
async function delContract(id){
  if(R()!=='god') return;
  if(!confirm('\u0412\u0438\u0434\u0430\u043b\u0438\u0442\u0438 \u0446\u0435\u0439 \u0434\u043e\u0433\u043e\u0432\u0456\u0440 \u0437 \u0456\u0441\u0442\u043e\u0440\u0456\u0457?')) return;
  try{ await dbDelete('contracts', id); S.contracts=(S.contracts||[]).filter(function(x){return x.id!==id;}); var sid=window._contractStudentId; if(sid) renderStudentContracts(sid); if(S.currentPage==='contracts') renderContractsPage(); mkToast('\u0412\u0438\u0434\u0430\u043b\u0435\u043d\u043e'); }catch(e){ mkToast('\u041f\u043e\u043c\u0438\u043b\u043a\u0430','error'); }
}
window.delContract=delContract;
function reprintContract(id){
  var c=(S.contracts||[]).find(function(x){return x.id===id;}); if(!c) return;
  var d=c.data||{}; d.number=d.number||c.number; d.signDate=d.signDate||c.sign_date;
  var w=window.open('','_blank'); if(w){ w.document.write(buildContractHTML(d)); w.document.close(); setTimeout(function(){ try{w.focus();w.print();}catch(e){} },400); }
}
window.reprintContract=reprintContract;
function renderStudentContracts(studentId){
  var box=document.getElementById('s-contracts'); if(!box) return;
  if(R()!=='god'){ box.innerHTML=''; return; }
  var list=(S.contracts||[]).filter(function(c){return c.student_id===studentId;}).sort(function(a,b){return String(b.sign_date).localeCompare(String(a.sign_date));});
  var head='<div style="display:flex;align-items:center;gap:8px;border-top:1px solid var(--b1);padding-top:8px;margin-top:4px">'
    +'<b style="font-size:13px">\uD83D\uDCC4 \u0414\u043e\u0433\u043e\u0432\u043e\u0440\u0438</b>'
    +'<button class="btn btn-p btn-sm" style="margin-left:auto" onclick="openContractDialog(\''+studentId+'\')">\uFF0B \u0421\u0444\u043e\u0440\u043c\u0443\u0432\u0430\u0442\u0438 \u0434\u043e\u0433\u043e\u0432\u0456\u0440</button></div>';
  if(!list.length){ box.innerHTML=head+'<div style="font-size:12px;color:var(--t3);padding:4px 0">\u0429\u0435 \u043d\u0435\u043c\u0430\u0454 \u0434\u043e\u0433\u043e\u0432\u043e\u0440\u0456\u0432</div>'; return; }
  box.innerHTML=head+list.map(function(c){
    return '<div style="display:flex;align-items:center;gap:8px;font-size:12.5px;padding:4px 0;border-bottom:1px solid var(--s3)">'
      +'<span style="font-weight:600">\u2116 '+c.number+'</span>'
      +'<span style="color:var(--t3)">'+fmtContractDate(c.sign_date)+'</span>'
      +'<button class="btn btn-g btn-sm" style="margin-left:auto;padding:2px 8px" onclick="reprintContract(\''+c.id+'\')" title="\u0412\u0456\u0434\u043a\u0440\u0438\u0442\u0438/\u0434\u0440\u0443\u043a">\uD83D\uDDA8</button>'
      +'<button class="btn btn-d btn-sm" style="padding:2px 8px" onclick="delContract(\''+c.id+'\')" title="\u0412\u0438\u0434\u0430\u043b\u0438\u0442\u0438">\uD83D\uDDD1</button>'
      +'</div>';
  }).join('');
}
window.renderStudentContracts=renderStudentContracts;
