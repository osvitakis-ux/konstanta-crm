
// ═══════════════════════════════════════════════════════════════
//  СВАЙП-ЖЕСТИ (мобільні)
//  Гортання ліворуч/праворуч перемикає тижні в розкладі —
//  природніше, ніж цілитись у маленькі стрілки.
// ═══════════════════════════════════════════════════════════════
function initSwipeGestures(){
  if(window._swipeInited) return;
  window._swipeInited = true;

  var startX=0, startY=0, startT=0, tracking=false;

  document.addEventListener('touchstart', function(e){
    if(e.touches.length!==1) return;
    // Не заважаємо гортанню таблиць і полям вводу
    var t=e.target;
    if(t.closest && (t.closest('[style*="overflow-x"]') || t.closest('input,textarea,select,.mo'))) return;
    startX=e.touches[0].clientX;
    startY=e.touches[0].clientY;
    startT=Date.now();
    tracking=true;
  }, {passive:true});

  document.addEventListener('touchend', function(e){
    if(!tracking) return;
    tracking=false;
    var dx=e.changedTouches[0].clientX-startX;
    var dy=e.changedTouches[0].clientY-startY;
    var dt=Date.now()-startT;

    // Жест зараховуємо, якщо: горизонтальний, достатньо довгий і швидкий
    if(Math.abs(dx)<70) return;              // надто короткий
    if(Math.abs(dy)>Math.abs(dx)*0.6) return; // це вертикальне гортання
    if(dt>600) return;                        // надто повільно — радше випадковість

    var page=S.currentPage;
    if(page==='schedule'){
      // Ліворуч = вперед у часі, праворуч = назад (як гортання сторінок)
      chWk(dx<0 ? 1 : -1);
      mkToast(dx<0 ? '\u2192 \u041d\u0430\u0441\u0442\u0443\u043f\u043d\u0438\u0439' : '\u2190 \u041f\u043e\u043f\u0435\u0440\u0435\u0434\u043d\u0456\u0439');
    }
  }, {passive:true});
}
window.initSwipeGestures=initSwipeGestures;

function buildBottomNav(){
  var el=document.getElementById('bottom-nav');
  if(!el) return;
  var role=R();
  var allowed=(ROLES[role]&&ROLES[role].nav)||[];
  var want=(BOTTOM_NAV[role]||BOTTOM_NAV.tutor).filter(function(id){
    return allowed.indexOf(id)>=0;
  });

  var html=want.map(function(id){
    var cfg=(NAV_CFG||[]).find(function(x){return x.id===id;});
    if(!cfg) return '';
    var active=(S.currentPage===id)?' active':'';
    // Лічильник завдань — щоб не пропустити нове
    var badge='';
    if(id==='tasks'){
      var n=(S.tasks||[]).filter(function(t){
        return t.status!=='done' && (t.assignee===CU.id||t.assigned_to===CU.id);
      }).length;
      if(n) badge='<span class="bn-badge">'+(n>9?'9+':n)+'</span>';
    }
    return '<button class="bn-item'+active+'" onclick="nav(\''+id+'\')">'
      +'<span class="bn-ico">'+(cfg.ico||'\u25CF')+'</span>'
      +'<span>'+shortLabel(cfg.lbl)+'</span>'+badge+'</button>';
  }).join('');

  // «Ще» — відкриває повне бічне меню
  html+='<button class="bn-item" onclick="toggleSidebar()">'
    +'<span class="bn-ico">\u2261</span><span>\u0429\u0435</span></button>';

  el.innerHTML=html;
}
window.buildBottomNav=buildBottomNav;

/** Коротка назва для вузької кнопки */
function shortLabel(lbl){
  var map={
    '\u0414\u0430\u0448\u0431\u043e\u0440\u0434':'\u0413\u043e\u043b\u043e\u0432\u043d\u0430',
    '\u0420\u043e\u0437\u043a\u043b\u0430\u0434':'\u0420\u043e\u0437\u043a\u043b\u0430\u0434',
    '\u0417\u0430\u043d\u044f\u0442\u0442\u044f':'\u0417\u0430\u043d\u044f\u0442\u0442\u044f',
    '\u0423\u0447\u043d\u0456':'\u0423\u0447\u043d\u0456',
    '\u0417\u0430\u0440\u043f\u043b\u0430\u0442\u0438':'\u0417\u0430\u0440\u043f\u043b\u0430\u0442\u0430'
  };
  var t=String(lbl||'');
  if(map[t]) return map[t];
  return t.length>9 ? t.slice(0,8)+'\u2026' : t;
}

function buildSidebar(){
  const cfg=(S.godConfig)||{};
  const navItems=cfg.navItems?[...cfg.navItems]:[...NAV_CFG];
  const role=R();
  const allowed=userNav();
  let html='',lastSec='';
  navItems.forEach(n=>{
    const isBuiltin=allowed.includes(n.id);
    const isCustom=n.custom;
    const roleAllowed=(n.roles||[]).includes(role);
    if(!isBuiltin&&!isCustom)return;
    if(isCustom&&!roleAllowed)return;
    if(n.sec!==lastSec){html+=('<div class="nsec">'+(n.sec)+'</div>');lastSec=n.sec;}
    html+=('<div class="ni" id="ni-'+(n.id)+'" onclick="nav(\''+(n.id)+'\')"><span class="nico">'+(n.ico)+'</span>'+(n.lbl)+(n.badge?`<span class="nbadge" id="nb-s">0</span>`:'')+'</div>');
  });
  document.getElementById('sbnav').innerHTML=html;
}



var UA_PERMS = [
  {k:'students',  lbl:'\u0423\u0447\u043d\u0456',           icon:'\u25CE'},
  {k:'tutors',    lbl:'\u0420\u0435\u043f\u0435\u0442\u0438\u0442\u043e\u0440\u0438',    icon:'\u25C8'},
  {k:'lessons',   lbl:'\u0417\u0430\u043d\u044f\u0442\u0442\u044f',      icon:'\u25C9'},
  {k:'payments',  lbl:'\u041e\u043f\u043b\u0430\u0442\u0430',       icon:'\u25C8'},
  {k:'users',     lbl:'\u0410\u043a\u0430\u0443\u043d\u0442\u0438',      icon:'\u25CE'},
  {k:'settings',  lbl:'\u041d\u0430\u043b\u0430\u0448\u0442\u0443\u0432\u0430\u043d\u043d\u044f', icon:'\u25C9'},
  {k:'comms',     lbl:'\u041a\u043e\u043c\u0443\u043d\u0456\u043a\u0430\u0446\u0456\u0457', icon:'\u25CE'},
  {k:'danger',    lbl:'\u041d\u0435\u0431\u0435\u0437\u043f\u0435\u0447\u043d\u0456 \u0434\u0456\u0457',  icon:'\u26A0\uFE0F'},
  {k:'deleteAny', lbl:'\u0412\u0438\u0434\u0430\u043b\u0435\u043d\u043d\u044f',    icon:'\uD83D\uDDD1'},
  {k:'seeIncome', lbl:'\u0411\u0430\u0447\u0438\u0442\u0438 \u0434\u043e\u0445\u043e\u0434\u0438', icon:'\uD83D\uDCB0'},
];
// UA_PAGES ініціалізується з NAV_CFG при першому зверненні
function getUAPages(){ return NAV_CFG.map(function(n){ return {id:n.id, lbl:n.lbl, ico:n.ico, sec:n.sec}; }); }

function buildUAHeader(u){
  var ro=ROLES[u.role];
  var el=document.getElementById('ua-user-info');
  if(!el)return;
  el.innerHTML='';
  var wrap=document.createElement('div');
  wrap.style.cssText='display:flex;align-items:center;gap:12px;margin-bottom:10px';
  var av=document.createElement('div');
  av.className='av';
  av.style.cssText='width:44px;height:44px;flex-shrink:0;overflow:hidden;border-radius:50%';
  var _mt2=myTutor();
  if(_mt2&&_mt2.photo){
    av.innerHTML='<img src="'+_mt2.photo+'" style="width:100%;height:100%;object-fit:cover;border-radius:50%">';
  } else {
    av.style.cssText+=(';background:'+ro.avatarBg+';font-size:17px;font-weight:700;color:#fff;display:flex;align-items:center;justify-content:center');
    av.textContent=(u.fn[0]||'')+(u.ln[0]||'');
  }
  var info=document.createElement('div');
  info.innerHTML='<div style="font-weight:700;font-size:15px">'+u.fn+' '+u.ln+'</div>'
    +'<div style="font-size:12px;color:var(--t2);margin-top:2px">'+(u.email||'')
    +' &bull; <span class="rpill '+u.role+'" style="font-size:10px;padding:1px 8px">'+ro.icon+' '+ro.label+'</span></div>';
  wrap.appendChild(av);wrap.appendChild(info);
  var hint=document.createElement('div');
  hint.style.cssText='font-size:11px;color:var(--t3);padding:8px 12px;background:var(--s2);border:1px solid var(--b1);border-radius:8px;line-height:1.5';
  hint.innerHTML='\u26A1 \u0420\u043E\u043B\u044C \u0432\u0438\u0437\u043D\u0430\u0447\u0430\u0454 <strong style="color:var(--t1)">\u0431\u0430\u0437\u043E\u0432\u0456</strong> \u043F\u0440\u0430\u0432\u0430. \u0422\u0443\u0442 \u043C\u043E\u0436\u043D\u0430 \u0434\u043E\u0434\u0430\u0442\u0438 \u0430\u0431\u043E \u0437\u043D\u044F\u0442\u0438 \u0434\u043E\u0441\u0442\u0443\u043F \u0434\u043B\u044F <strong style="color:var(--dir)">\u0446\u044C\u043E\u0433\u043E \u043A\u043E\u043D\u043A\u0440\u0435\u0442\u043D\u043E\u0433\u043E \u0430\u043A\u0430\u0443\u043D\u0442\u0443</strong>.';
  el.appendChild(wrap);el.appendChild(hint);
}



function buildUANav(u){
  var ro=ROLES[u.role];
  var roleNav=ro.nav||[];
  var up=u.perms||{};
  var hideNav=up.hideNav||[];
  var showNav=up.showNav||[];
  var el=document.getElementById('ua-nav-grid');
  if(!el)return;
  el.innerHTML='';
  getUAPages().forEach(function(pg){
    var inRole=roleNav.includes(pg.id);
    var isOn=(inRole&&!hideNav.includes(pg.id))||showNav.includes(pg.id);
    var item=document.createElement('div');
    item.className='ua-nav-item'+(isOn?' checked':'');
    var cb=document.createElement('input');cb.type='checkbox';cb.checked=isOn;
    (function(pageId,isInRole){
      cb.addEventListener('change',function(){
        uaNavChange(pageId,this.checked,isInRole);
        this.closest('.ua-nav-item').classList.toggle('checked',this.checked);
      });
    })(pg.id,inRole);
    var ico=document.createElement('span');ico.className='ua-nav-ico';ico.textContent=pg.ico;
    var info=document.createElement('div');info.style.flex='1';
    info.innerHTML='<div class="ua-nav-lbl">'+pg.lbl+'</div>'
      +'<div class="ua-nav-sec">'+pg.sec+(inRole?' \u00B7 \u0454 \u0432 \u0440\u043E\u043B\u0456':' \u00B7 \u043D\u0435 \u0432 \u0440\u043E\u043B\u0456')+'</div>';
    item.appendChild(cb);item.appendChild(ico);item.appendChild(info);
    el.appendChild(item);
  });
}



function buildUAPerms(u){
  var ro=ROLES[u.role];
  var roleCan=ro.can||{};
  var up=u.perms||{};
  var custCan=up.can||{};
  var el=document.getElementById('ua-perms-grid');
  if(!el)return;
  el.innerHTML='';
  UA_PERMS.forEach(function(p){
    var roleVal=!!(roleCan[p.k]||ro[p.k]);
    var hasOverride=p.k in custCan;
    var effectiveVal=hasOverride?custCan[p.k]:roleVal;

    var item=document.createElement('div');item.className='ua-perm-row';

    var left=document.createElement('div');
    var lbl=document.createElement('div');lbl.className='ua-perm-label';lbl.textContent=p.lbl;
    var sub=document.createElement('div');
    sub.style.cssText='font-size:10px;margin-top:2px;display:flex;align-items:center;gap:4px';
    if(hasOverride){
      var sp=document.createElement('span');sp.style.color='var(--dir)';sp.textContent='\u2699 \u0456\u043D\u0434\u0438\u0432\u0456\u0434\u0443\u0430\u043B\u044C\u043D\u043E';
      var rb=document.createElement('button');
      rb.style.cssText='background:none;border:none;color:var(--t3);cursor:pointer;font-size:10px;padding:0';
      rb.textContent='\u21BA \u0441\u043A\u0438\u043D\u0443\u0442\u0438';
      (function(key){rb.addEventListener('click',function(){uaResetPerm(key);});})(p.k);
      sub.appendChild(sp);sub.appendChild(rb);
    } else {
      sub.textContent='\u0437 \u0440\u043E\u043B\u0456: '+(roleVal?'\u2705 \u0442\u0430\u043A':'\u274C \u043D\u0456');
      sub.style.color='var(--t3)';
    }
    left.appendChild(lbl);left.appendChild(sub);

    var tgl=document.createElement('label');tgl.className='toggle';
    var cb=document.createElement('input');cb.type='checkbox';cb.checked=effectiveVal;
    (function(key,rv){
      cb.addEventListener('change',function(){
        uaPermChange(key,this.checked,rv);
        // Rebuild this item's sub label
        var subEl=this.closest('.ua-perm-row').querySelector('div > div:last-child');
        subEl.innerHTML='';subEl.style.cssText='font-size:10px;margin-top:2px;display:flex;align-items:center;gap:4px';
        var sp2=document.createElement('span');sp2.style.color='var(--dir)';sp2.textContent='\u2699 \u0456\u043D\u0434\u0438\u0432\u0456\u0434\u0443\u0430\u043B\u044C\u043D\u043E';
        var rb2=document.createElement('button');rb2.style.cssText='background:none;border:none;color:var(--t3);cursor:pointer;font-size:10px;padding:0';
        rb2.textContent='\u21BA \u0441\u043A\u0438\u043D\u0443\u0442\u0438';
        (function(k){rb2.addEventListener('click',function(){uaResetPerm(k);});})(key);
        subEl.appendChild(sp2);subEl.appendChild(rb2);
      });
    })(p.k,roleVal);
    var sl=document.createElement('span');sl.className='toggle-slider';
    tgl.appendChild(cb);tgl.appendChild(sl);
    item.appendChild(left);item.appendChild(tgl);
    el.appendChild(item);
  });
}



function buildUASummary(u){
  var el=document.getElementById('ua-summary');
  if(!el)return;
  var up=u.perms||{};
  var ro=ROLES[u.role];
  var roleCan=ro.can||{};
  var custCan=up.can||{};
  var hideNav=up.hideNav||[];
  var showNav=up.showNav||[];
  var html='';

  if(Object.keys(custCan).length){
    html+='<div style="font-weight:600;font-size:11px;color:var(--dir);margin-bottom:8px;text-transform:uppercase;letter-spacing:.5px">\u2699 \u0406\u043D\u0434\u0438\u0432\u0456\u0434\u0443\u0430\u043B\u044C\u043D\u0456 \u043F\u0440\u0430\u0432\u0430:</div>';
    html+='<div style="display:flex;flex-direction:column;gap:4px;margin-bottom:12px">';
    Object.keys(custCan).forEach(function(k){
      var def=UA_PERMS.find(function(p){return p.k===k;});
      var lbl=def?def.lbl:k;
      var rv=!!(roleCan[k]||ro[k]);
      html+='<div style="display:flex;align-items:center;gap:8px;padding:6px 10px;background:var(--s2);border-radius:7px">'
        +'<span>'+(custCan[k]?'\u2705':'\u274C')+'</span>'
        +'<span style="flex:1;font-size:12px">'+lbl+'</span>'
        +'<span style="font-size:10px;color:var(--t3)">\u0440\u043E\u043B\u044C: '+(rv?'\u2705':'\u274C')+'</span>'
        +'<button class="ua-sum-reset" data-pkey="'+k+'" style="background:none;border:none;color:var(--t3);cursor:pointer;font-size:11px;padding:2px 4px">\u21BA</button>'
        +'</div>';
    });
    html+='</div>';
  }

  if(hideNav.length||showNav.length){
    html+='<div style="font-weight:600;font-size:11px;color:var(--dir);margin-bottom:8px;text-transform:uppercase;letter-spacing:.5px">\uD83D\uDCCB \u041D\u0430\u0432\u0456\u0433\u0430\u0446\u0456\u044F \u0437\u043C\u0456\u043D\u0435\u043D\u0430:</div>';
    html+='<div style="display:flex;flex-direction:column;gap:3px">';
    var _uap=getUAPages();
    hideNav.forEach(function(p){var pg=_uap.find(function(x){return x.id===p;});html+='<div style="font-size:12px;color:var(--danger)">\u274C \u041F\u0440\u0438\u0445\u043E\u0432\u0430\u043D\u043E: '+(pg?pg.ico+' '+pg.lbl:p)+'</div>';});
    showNav.forEach(function(p){var pg=_uap.find(function(x){return x.id===p;});html+='<div style="font-size:12px;color:var(--tut)">\u2705 \u0414\u043E\u0434\u0430\u043D\u043E: '+(pg?pg.ico+' '+pg.lbl:p)+'</div>';});
    html+='</div>';
  }

  if(!html){html='<div style="color:var(--t3);font-size:12px;padding:8px 0">\u0406\u043D\u0434\u0438\u0432\u0456\u0434\u0443\u0430\u043B\u044C\u043D\u0438\u0445 \u043D\u0430\u043B\u0430\u0448\u0442\u0443\u0432\u0430\u043D\u044C \u043D\u0435\u043C\u0430\u0454 \u2014 \u0434\u0456\u044E\u0442\u044C \u043F\u0440\u0430\u0432\u0430 \u0440\u043E\u043B\u0456.</div>';}

  el.innerHTML=html;
  el.querySelectorAll('.ua-sum-reset').forEach(function(btn){
    btn.addEventListener('click',function(){uaResetPerm(this.dataset.pkey);});
  });

  var rb=document.getElementById('ua-reset-all');
  if(rb){var hasAny=!!(u.perms&&(Object.keys(u.perms.can||{}).length||(u.perms.hideNav||[]).length||(u.perms.showNav||[]).length));rb.style.display=hasAny?'flex':'none';}
}



function genRecurDates(startDate,recurType,endDate,count,interval){
  const dates=[];
  const start=new Date(startDate+'T12:00:00');
  const end=endDate?new Date(endDate+'T23:59:59'):null;
  const maxCount=count?Math.min(parseInt(count),200):104;
  let cur=new Date(start);
  while(dates.length<maxCount){
    if(end&&cur>end)break;
    dates.push(localDateStr(cur));
    const next=new Date(cur);
    if(recurType==='daily'){next.setDate(next.getDate()+1);}
    else if(recurType==='weekly'){next.setDate(next.getDate()+7);}
    else if(recurType==='biweekly'){next.setDate(next.getDate()+14);}
    else if(recurType==='monthly'){next.setMonth(next.getMonth()+1);}
    else if(recurType==='monthly-dow'){
      const dow=start.getDay();const weekNum=Math.floor((start.getDate()-1)/7);
      next.setMonth(next.getMonth()+1);next.setDate(1);
      while(next.getDay()!==dow)next.setDate(next.getDate()+1);
      next.setDate(next.getDate()+weekNum*7);
    }
    else if(recurType==='custom'){next.setDate(next.getDate()+Math.max(1,parseInt(interval)||7));}
    else break;
    if(end&&next>end)break;
    cur=next;
  }
  return dates;
}
