
function voiceSupported(){
  return !!(window.SpeechRecognition || window.webkitSpeechRecognition);
}
window.voiceSupported = voiceSupported;

function voiceToggle(fieldId, btn){
  if(!voiceSupported()){
    mkToast('\u0413\u043e\u043b\u043e\u0441\u043e\u0432\u0435 \u0432\u0432\u0435\u0434\u0435\u043d\u043d\u044f \u043f\u0440\u0430\u0446\u044e\u0454 \u0432 Chrome \u0430\u0431\u043e Edge','error');
    return;
  }
  // Повторне натискання — зупинка
  if(_rec && _recTarget===fieldId){ voiceStop(); return; }
  if(_rec) voiceStop();

  var field=document.getElementById(fieldId);
  if(!field) return;

  _recTarget=fieldId;
  _recBtn=btn;
  _recField=field;
  // Текст, що вже був у полі до початку диктування
  _recBase = field.value ? field.value.trim()+' ' : '';
  // Накопичене за ВСІ сесії розпізнавання (між автоперезапусками)
  _recAccum = '';
  _recWantStop = false;

  if(btn) btn.classList.add('rec-on');
  field.classList.add('rec-field');
  mkToast('\ud83c\udfa4 \u0413\u043e\u0432\u043e\u0440\u0456\u0442\u044c\u2026 (\u043d\u0430\u0442\u0438\u0441\u043d\u0456\u0442\u044c \u0449\u0435 \u0440\u0430\u0437, \u0449\u043e\u0431 \u0437\u0443\u043f\u0438\u043d\u0438\u0442\u0438)');

  // Лічильник часу запису — щоб було видно, що мікрофон працює
  _recSeconds=0;
  if(_recTimer) clearInterval(_recTimer);
  _recTimer=setInterval(function(){
    _recSeconds++;
    if(_recBtn) _recBtn.textContent=_recSeconds<60
      ? _recSeconds+'\u0441'
      : Math.floor(_recSeconds/60)+':'+String(_recSeconds%60).padStart(2,'0');
  },1000);
  // Підказка через 8 секунд — щоб було зрозуміло, що запис триває
  setTimeout(function(){
    if(_recTimer && !_recWantStop) mkToast('\ud83c\udfa4 \u0417\u0430\u043f\u0438\u0441 \u0442\u0440\u0438\u0432\u0430\u0454\u2026');
  }, 8000);

  voiceStartSession();
}
window.voiceToggle=voiceToggle;

/**
 * Запускає одну сесію розпізнавання.
 *
 * Мобільний Chrome обриває розпізнавання через кілька секунд тиші —
 * це його вбудована поведінка, вимкнути її не можна. Тому ми
 * перезапускаємо сесію автоматично, а текст накопичуємо окремо,
 * щоб він не губився й не дублювався між сесіями.
 */
/**
 * Режим діагностики диктування.
 * Показує на екрані, що саме віддає браузер — потрібен, коли
 * поведінка на конкретному пристрої відрізняється від типової.
 */
function voiceDebugToggle(){
  window._voiceDebug = !window._voiceDebug;
  var dbg=document.getElementById('voice-debug');
  if(!dbg){
    dbg=document.createElement('pre');
    dbg.id='voice-debug';
    dbg.style.cssText='position:fixed;left:6px;right:6px;bottom:70px;max-height:42vh;'
      +'overflow:auto;background:rgba(0,0,0,.88);color:#7fff9f;font-size:10px;'
      +'padding:9px;border-radius:9px;z-index:9999;white-space:pre-wrap;margin:0;'
      +'font-family:monospace;line-height:1.35';
    dbg.onclick=function(){
      // Копіювання в буфер — щоб можна було надіслати
      try{
        navigator.clipboard.writeText(dbg.textContent);
        mkToast('\u0421\u043a\u043e\u043f\u0456\u0439\u043e\u0432\u0430\u043d\u043e');
      }catch(e){}
    };
    document.body.appendChild(dbg);
  }
  dbg.style.display = window._voiceDebug ? 'block' : 'none';
  dbg.textContent = window._voiceDebug
    ? '\u0414\u0456\u0430\u0433\u043d\u043e\u0441\u0442\u0438\u043a\u0430 \u0443\u0432\u0456\u043c\u043a\u043d\u0435\u043d\u0430. \u041d\u0430\u0442\u0438\u0441\u043d\u0456\u0442\u044c \ud83c\udfa4 \u0456 \u043f\u0440\u043e\u0434\u0438\u043a\u0442\u0443\u0439\u0442\u0435 \u0444\u0440\u0430\u0437\u0443.\n'
      +'\u0422\u0430\u043f \u043f\u043e \u0446\u044c\u043e\u043c\u0443 \u0432\u0456\u043a\u043d\u0443 \u2014 \u0441\u043a\u043e\u043f\u0456\u044e\u0432\u0430\u0442\u0438.\n\n'
    : '';
  mkToast(window._voiceDebug
    ? '\ud83d\udd0d \u0414\u0456\u0430\u0433\u043d\u043e\u0441\u0442\u0438\u043a\u0430 \u0443\u0432\u0456\u043c\u043a\u043d\u0435\u043d\u0430'
    : '\u0414\u0456\u0430\u0433\u043d\u043e\u0441\u0442\u0438\u043a\u0430 \u0432\u0438\u043c\u043a\u043d\u0435\u043d\u0430');
}
window.voiceDebugToggle=voiceDebugToggle;

function voiceStartSession(){
  if(_recWantStop) return;
  var SR = window.SpeechRecognition || window.webkitSpeechRecognition;
  try{ _rec = new SR(); }catch(e){ voiceStop(); return; }

  _rec.lang='uk-UA';
  // На мобільному continuous однаково ігнорується, тож не покладаємось на нього
  _rec.continuous=true;
  _rec.interimResults=true;
  _rec.maxAlternatives=1;

  // Скільки фінальних фрагментів цієї сесії ВЖЕ збережено.
  // Це ключ до розвʼязання: зберігаємо кожен фрагмент ОДРАЗУ, як тільки
  // він став фінальним, а не в onend. Раніше onend на мобільному
  // спрацьовував із затримкою або двічі — через це перше слово
  // потрапляло в текст по кілька разів.
  var myRec = _rec;   // фіксуємо саме цей екземпляр
  myRec._sessionText = '';
  myRec._flushed = false;

  _rec.onresult=function(e){
    // Ігноруємо події від старої сесії, якщо вона встигла відповісти пізніше
    if(myRec !== _rec) return;

    // Збираємо ВЕСЬ фінальний текст цієї сесії наново.
    //
    // Чому саме так: діагностика на реальному пристрої показала, що
    // браузер надсилає порожні фрагменти впереміш зі справжніми, а
    // нумерація зсувається. Рахувати «скільки вже збережено» за номером
    // виявилось ненадійно. Тому щоразу перебираємо все й порівнюємо
    // з попереднім станом цієї ж сесії.
    // ЧОМУ САМЕ ТАК: діагностика на реальному пристрої показала, що
    // браузер надсилає НАКОПИЧУВАЛЬНІ фрагменти — кожен наступний
    // містить попередній усередині:
    //     [2]F "1"
    //     [3]F "1 2"      ← це не нове слово, а те саме плюс продовження
    // Якщо просто склеїти всі фрагменти, виходить «1 1 2».
    // Тому беремо НАЙДОВШИЙ фрагмент, що містить попередні, а не суму.
    var finals=[], interim='';
    for(var i=0;i<e.results.length;i++){
      var t=String(e.results[i][0].transcript||'').trim();
      if(!t) continue;                      // порожні фрагменти пропускаємо
      if(e.results[i].isFinal) finals.push(t);
      else interim += t + ' ';
    }

    var sessionFinal='';
    for(var k=0;k<finals.length;k++){
      var cur=finals[k];
      var curL=cur.toLowerCase();
      var accL=sessionFinal.trim().toLowerCase();
      if(!accL){
        sessionFinal = cur + ' ';
      } else if(curL.indexOf(accL)===0){
        // Новий фрагмент ПОЧИНАЄТЬСЯ з уже зібраного — він його розширює
        sessionFinal = cur + ' ';
      } else if(accL.indexOf(curL)===0){
        // Навпаки: новий коротший і вже міститься — пропускаємо
      } else {
        // Справді новий шматок — дописуємо
        sessionFinal += cur + ' ';
      }
    }
    // Замінюємо текст ЦІЄЇ сесії, а не дописуємо — тож повтори неможливі
    myRec._sessionText = sessionFinal;

    var full=(_recBase + _recAccum + sessionFinal + interim).replace(/\s+/g,' ').trim();
    if(_recField) _recField.value=full;

    // Режим діагностики: показує, що САМЕ віддає браузер
    if(window._voiceDebug){
      var dbg=document.getElementById('voice-debug');
      if(dbg){
        var line='['+new Date().toLocaleTimeString('uk-UA')+'] results='+e.results.length
          +' resultIndex='+e.resultIndex+'\n';
        for(var j=0;j<e.results.length;j++){
          line+='  ['+j+']'+(e.results[j].isFinal?'F':'i')+' "'+e.results[j][0].transcript+'"\n';
        }
        line+='  \u2192 accum: "'+_recAccum.trim()+'"\n';
        dbg.textContent=line+dbg.textContent;
      }
    }
  };

  _rec.onerror=function(e){
    if(e.error==='not-allowed' || e.error==='service-not-allowed'){
      mkToast('\u0414\u043e\u0437\u0432\u043e\u043b\u044c\u0442\u0435 \u0434\u043e\u0441\u0442\u0443\u043f \u0434\u043e \u043c\u0456\u043a\u0440\u043e\u0444\u043e\u043d\u0430','error');
      _recWantStop=true;
      voiceStop();
    }
    // 'no-speech', 'aborted', 'network' — звичні на мобільному,
    // перезапуск відбудеться в onend
  };

  _rec.onend=function(){
    if(myRec !== _rec) return;    // подія від старої сесії — ігноруємо
    // Переносимо текст завершеної сесії в загальне накопичення.
    // Прапорець гарантує, що це станеться рівно один раз, навіть
    // якщо браузер викличе onend повторно.
    if(myRec._sessionText && !myRec._flushed){
      myRec._flushed = true;
      _recAccum += myRec._sessionText;
    }
    console.log('[voice] сесія завершилась, накопичено:', _recAccum.length, 'символів');
    if(_recWantStop){ voiceFinalize(); return; }

    // Браузер обірвав сесію сам — запускаємо наступну.
    // Текст уже збережений в _recAccum, тож нічого не губиться.
    if(_recRestartTimer) clearTimeout(_recRestartTimer);
    _recRestartTimer=setTimeout(function(){
      if(!_recWantStop) voiceStartSession();
    }, 200);
  };

  console.log('[voice] старт сесії');
  try{ myRec.start(); }
  catch(e){
    if(_recRestartTimer) clearTimeout(_recRestartTimer);
    _recRestartTimer=setTimeout(function(){
      if(!_recWantStop) voiceStartSession();
    }, 450);
  }
}

/** Остаточно вимикає диктування */
function voiceStop(){
  _recWantStop=true;
  // Зберігаємо те, що вже надиктовано в поточній сесії
  try{
    if(_rec && _rec._sessionText && !_rec._flushed){
      _rec._flushed = true;
      _recAccum += _rec._sessionText;
    }
  }catch(e){}
  if(_recRestartTimer){ clearTimeout(_recRestartTimer); _recRestartTimer=null; }
  try{ if(_rec){ _rec.onend=null; _rec.onresult=null; _rec.stop(); } }catch(e){}
  voiceFinalize();
}
window.voiceStop=voiceStop;

function voiceFinalize(){
  _rec=null;
  _recTarget=null;
  if(_recTimer){ clearInterval(_recTimer); _recTimer=null; }
  // Повертаємо кнопці іконку мікрофона замість лічильника
  if(_recBtn) _recBtn.textContent='\ud83c\udfa4';
  if(_recRestartTimer){ clearTimeout(_recRestartTimer); _recRestartTimer=null; }
  document.querySelectorAll('.rec-on').forEach(function(b){ b.classList.remove('rec-on'); });
  document.querySelectorAll('.rec-field').forEach(function(f){ f.classList.remove('rec-field'); });
  _recBtn=null; _recField=null;
}

function dedupePhrases(txt){
  var s=String(txt||'').trim();
  if(!s) return '';
  var words=s.split(/\s+/);
  // Шукаємо повтор блоку від 8 слів до 2 — довші перевіряємо першими,
  // щоб не розрізати справжній повтор на частини
  for(var len=Math.min(8, Math.floor(words.length/2)); len>=2; len--){
    for(var i=0; i+len*2<=words.length; i++){
      var a=words.slice(i,i+len).join(' ').toLowerCase();
      var b=words.slice(i+len,i+len*2).join(' ').toLowerCase();
      if(a===b && a.length>6){
        words.splice(i+len, len);   // прибираємо другий блок
        i--;                        // перевіряємо це саме місце ще раз
      }
    }
  }
  return words.join(' ');
}
window.dedupePhrases=dedupePhrases;



/** Кнопка мікрофона біля поля */
/** Довге натискання на 🎤 вмикає діагностику */
function voiceBtnLongPress(el){
  var t=null;
  el.addEventListener('touchstart', function(){
    t=setTimeout(function(){ voiceDebugToggle(); }, 900);
  }, {passive:true});
  ['touchend','touchmove','touchcancel'].forEach(function(ev){
    el.addEventListener(ev, function(){ if(t){ clearTimeout(t); t=null; } }, {passive:true});
  });
  // На компʼютері — правою кнопкою
  el.addEventListener('contextmenu', function(e){ e.preventDefault(); voiceDebugToggle(); });
}
window.voiceBtnLongPress=voiceBtnLongPress;

function voiceBtn(fieldId){
  // Кнопку показуємо завжди: якщо браузер не підтримує розпізнавання,
  // натискання пояснить, у чому річ, замість мовчазної відсутності кнопки
  var sup=voiceSupported();
  return '<button type="button" class="voice-btn'+(sup?'':' off')+'" '
    +'title="'+(sup?'\u041d\u0430\u0434\u0438\u043a\u0442\u0443\u0432\u0430\u0442\u0438'
                  :'\u041f\u043e\u0442\u0440\u0456\u0431\u0435\u043d Chrome \u0430\u0431\u043e Edge')+'" '
    +'onclick="voiceToggle(\''+fieldId+'\',this)">\ud83c\udfa4</button>';
}
window.voiceBtn=voiceBtn;

// ── ГЛОБАЛЬНИЙ голосовий ввід: мікрофон зʼявляється біля БУДЬ-ЯКОГО текстового поля ──
function setupGlobalVoiceMic(){
  if(window._globalVoiceMicReady) return;
  window._globalVoiceMicReady=true;
  if(!voiceSupported()) return; // браузер не підтримує розпізнавання — мікрофон не показуємо
  var mic=document.createElement('button');
  mic.type='button'; mic.id='global-voice-mic'; mic.className='voice-btn'; mic.textContent='\ud83c\udfa4';
  mic.title='\u041d\u0430\u0434\u0438\u043a\u0442\u0443\u0432\u0430\u0442\u0438 \u0432 \u0446\u0435 \u043f\u043e\u043b\u0435';
  // position:fixed і right:auto перекривають клас .voice-btn (absolute/right:6px)
  mic.style.cssText='position:fixed;right:auto;z-index:99998;display:none;width:30px;height:30px;padding:0;align-items:center;justify-content:center;box-shadow:0 2px 8px rgba(0,0,0,.18)';
  document.body.appendChild(mic);
  var lastField=null;
  function isTextField(el){
    if(!el||!el.tagName||el.readOnly||el.disabled) return false;
    if(el.tagName==='TEXTAREA') return true;
    if(el.tagName==='INPUT'){ var t=(el.getAttribute('type')||'text').toLowerCase(); return ['text','search','tel','email','url',''].indexOf(t)>=0; }
    return false;
  }
  // Якщо поле вже має власну кнопку 🎤 поруч — глобальний мікрофон не дублюємо
  function hasOwnVoiceBtn(el){ var p=el.parentElement; for(var i=0;i<3&&p;i++){ if(p.querySelector&&p.querySelector('.voice-btn:not(#global-voice-mic)')) return true; p=p.parentElement; } return false; }
  function place(){
    if(!lastField) return;
    var r=lastField.getBoundingClientRect();
    if(r.width===0&&r.height===0){ mic.style.display='none'; return; }
    var top=(lastField.tagName==='TEXTAREA') ? r.top+6 : r.top+(r.height-30)/2;
    mic.style.top=Math.max(6,top)+'px';
    mic.style.left=Math.max(6,r.right-36)+'px';
  }
  document.addEventListener('focusin', function(e){
    var el=e.target; if(el===mic) return;
    if(isTextField(el) && !hasOwnVoiceBtn(el)){
      lastField=el;
      if(!el.id) el.id='vf_'+Math.random().toString(36).slice(2,9);
      place(); mic.style.display='flex';
    }
  });
  document.addEventListener('focusout', function(e){
    if(e.target===mic) return;
    setTimeout(function(){
      var af=document.activeElement;
      if(af===mic||isTextField(af)) return;      // фокус на мікрофоні або іншому полі
      if(_rec && _recField===lastField) return;  // триває диктування — лишаємо
      mic.style.display='none';
    },160);
  });
  window.addEventListener('scroll', function(){ if(mic.style.display!=='none') place(); }, true);
  window.addEventListener('resize', function(){ if(mic.style.display!=='none') place(); });
  mic.addEventListener('mousedown', function(e){ e.preventDefault(); }); // не знімати фокус із поля
  mic.addEventListener('click', function(e){
    e.preventDefault();
    if(!lastField) return;
    if(!lastField.id) lastField.id='vf_'+Math.random().toString(36).slice(2,9);
    voiceToggle(lastField.id, mic);
  });
}
window.setupGlobalVoiceMic=setupGlobalVoiceMic;
