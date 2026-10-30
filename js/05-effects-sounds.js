
// Друк акта: генерує документ із реквізитами, переліком послуг (можливо відредагованим) і двома підписами
// ═══════════════════════════════════════════════════════════════
//  ВОДЯНИЙ ЗНАК ДЛЯ ДРУКОВАНИХ ДОКУМЕНТІВ
//  Робить акти, рахунки й відомості впізнаваними та солідними,
//  а підробити їх — складніше.
// ═══════════════════════════════════════════════════════════════

/** CSS водяного знака й колонтитула — додається у <style> документа */
// ═══════════════════════════════════════════════════════════════
//  КОНФЕТІ — лише на справді рідкісні досягнення.
//  Якщо сипати на кожну дію, воно швидко перестає щось означати,
//  тому подій навмисно мало.
// ═══════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════
//  ЗВУКОВІ СИГНАЛИ
//  За замовчуванням ВИМКНЕНІ: у спільному кабінеті звуки швидше
//  дратують, ніж допомагають. Вмикаються свідомо в профілі.
//  Генеруються кодом — без зовнішніх файлів.
// ═══════════════════════════════════════════════════════════════
function soundsOn(){
  try{ return localStorage.getItem('crm_sounds')==='1'; }catch(e){ return false; }
}
function setSounds(on){
  try{ localStorage.setItem('crm_sounds', on?'1':'0'); }catch(e){}
  if(on) playSound('ok');
}
window.soundsOn=soundsOn;
window.setSounds=setSounds;

var _audioCtx=null;
/** kind: 'ok' | 'task' | 'call' */
function playSound(kind){
  if(!soundsOn()) return;
  try{
    _audioCtx = _audioCtx || new (window.AudioContext||window.webkitAudioContext)();
    var ctx=_audioCtx;
    if(ctx.state==='suspended') ctx.resume();

    // Короткі м'які тони — не різкі й не гучні
    var notes = kind==='task' ? [[660,0],[880,.09]]
              : kind==='call' ? [[520,0],[660,.08],[520,.16]]
              : [[880,0]];
    notes.forEach(function(n){
      var osc=ctx.createOscillator(), gain=ctx.createGain();
      osc.type='sine';
      osc.frequency.value=n[0];
      var t0=ctx.currentTime+n[1];
      // Плавне наростання й згасання, щоб не було клацання
      gain.gain.setValueAtTime(0, t0);
      gain.gain.linearRampToValueAtTime(.055, t0+.012);
      gain.gain.exponentialRampToValueAtTime(.0001, t0+.19);
      osc.connect(gain); gain.connect(ctx.destination);
      osc.start(t0); osc.stop(t0+.2);
    });
  }catch(e){}
}
window.playSound=playSound;

function confetti(opts){
  opts=opts||{};
  // Повага до системного налаштування «зменшити рух»
  try{
    if(window.matchMedia && window.matchMedia('(prefers-reduced-motion:reduce)').matches) return;
  }catch(e){}

  var count=opts.count||70;
  var colors=['#6366f1','#22c55e','#f59e0b','#ec4899','#14b8a6','#f97316','#8b5cf6'];
  var cvs=document.createElement('canvas');
  cvs.style.cssText='position:fixed;inset:0;width:100%;height:100%;pointer-events:none;z-index:9999';
  document.body.appendChild(cvs);
  var ctx=cvs.getContext('2d');
  var W=cvs.width=window.innerWidth, H=cvs.height=window.innerHeight;

  var bits=[];
  for(var i=0;i<count;i++){
    bits.push({
      x: W*(0.15+Math.random()*0.7),
      y: -20-Math.random()*H*0.3,
      w: 6+Math.random()*6,
      h: 8+Math.random()*8,
      c: colors[(Math.random()*colors.length)|0],
      vx: (Math.random()-0.5)*2.4,
      vy: 2+Math.random()*3,
      rot: Math.random()*Math.PI,
      vr: (Math.random()-0.5)*0.22
    });
  }

  var start=Date.now(), DUR=2600;
  (function frame(){
    var t=Date.now()-start;
    if(t>DUR){ cvs.remove(); return; }
    ctx.clearRect(0,0,W,H);
    // Плавне згасання наприкінці
    var fade = t>DUR-700 ? (DUR-t)/700 : 1;
    bits.forEach(function(b){
      b.x+=b.vx; b.y+=b.vy; b.rot+=b.vr;
      b.vy+=0.045;                 // тяжіння
      ctx.save();
      ctx.globalAlpha=fade;
      ctx.translate(b.x,b.y);
      ctx.rotate(b.rot);
      ctx.fillStyle=b.c;
      ctx.fillRect(-b.w/2,-b.h/2,b.w,b.h);
      ctx.restore();
    });
    requestAnimationFrame(frame);
  })();
}
window.confetti=confetti;

/**
 * Показує святкування лише ОДИН раз для конкретної події.
 * Ключ зберігається, щоб конфетті не сипалось при кожному
 * перемальовуванні сторінки.
 */
function celebrateOnce(key, message){
  try{
    var seen=JSON.parse(localStorage.getItem('celebrated')||'{}');
    if(seen[key]) return false;
    seen[key]=Date.now();
    localStorage.setItem('celebrated', JSON.stringify(seen));
  }catch(e){}
  confetti();
  if(message) mkToast(message);
  return true;
}
window.celebrateOnce=celebrateOnce;
