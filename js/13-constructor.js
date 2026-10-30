
function gcGetConfig(){if(!S.godConfig)S.godConfig={};return S.godConfig;}

function gcSet(key,val){
  if(!S.godConfig)S.godConfig={};
  if(val===null||val===undefined)delete S.godConfig[key];
  else S.godConfig[key]=val;
  // Demo communications
  if(!S.comms||!S.comms.length){
    S.comms=[
      {id:'cm1',tutorId:'t1',studentId:'s1',date:localDateStr(new Date()),type:'call',note:'\u041E\u0431\u0433\u043E\u0432\u043E\u0440\u0438\u043B\u0438 \u043F\u043B\u0430\u043D \u0437\u0430\u043D\u044F\u0442\u044C \u043D\u0430 \u043C\u0456\u0441\u044F\u0446\u044C',createdAt:new Date().toISOString()},
      {id:'cm2',tutorId:'t2',studentId:'s4',date:localDateStr(new Date()),type:'msg',note:'\u041D\u0430\u0433\u0430\u0434\u0443\u0432\u0430\u043D\u043D\u044F \u043F\u0440\u043E \u0434\u043E\u043C\u0430\u0448\u043D\u0454 \u0437\u0430\u0432\u0434\u0430\u043D\u043D\u044F',createdAt:new Date().toISOString()},
      {id:'cm3',tutorId:'t3',studentId:'s6',date:localDateStr(new Date()),type:'meeting',note:'\u0411\u0430\u0442\u044C\u043A\u0456\u0432\u0441\u044C\u043A\u0456 \u0437\u0431\u043E\u0440\u0438',createdAt:new Date().toISOString()},
    ];
  }
  saveS();
}

function gcTab(id,el){
  document.querySelectorAll('.gc-tab').forEach(function(t){t.classList.remove('active');});
  document.querySelectorAll('.gc-panel').forEach(function(p){p.classList.remove('active');});
  el.classList.add('active');
  document.getElementById('gcp-'+id).classList.add('active');
}

function renderConstructor(){
  if(R()!=='god'){mkToast('\u0422\u0456\u043B\u044C\u043A\u0438 \u0411\u043E\u0433 \u0441\u0438\u0441\u0442\u0435\u043C\u0438','error');return;}
  gcRenderRoles();
  gcRenderNav();
  gcRenderFields();
  gcRenderLabels();
}

function gcRenderRoles(){
  var cfg=gcGetConfig();
  var sp=cfg.perms||{};
  var rl=[
    {role:'god',ico:'\u26A1',lbl:'\u0411\u043E\u0433 \u0441\u0438\u0441\u0442\u0435\u043C\u0438',clr:'var(--god2)',locked:true},
    {role:'director',ico:'\uD83D\uDC51',lbl:'\u0414\u0438\u0440\u0435\u043A\u0442\u043E\u0440',clr:'var(--dir)',locked:false},
    {role:'admin',ico:'\uD83D\uDEE1',lbl:'\u0410\u0434\u043C\u0456\u043D\u0456\u0441\u0442\u0440\u0430\u0442\u043E\u0440',clr:'var(--adm)',locked:false},
    {role:'tutor',ico:'\uD83D\uDCDA',lbl:'\u0420\u0435\u043F\u0435\u0442\u0438\u0442\u043E\u0440',clr:'var(--tut)',locked:false},
  ];
  var cont=document.getElementById('gc-role-editor');
  cont.innerHTML='';
  var grid=document.createElement('div');grid.className='role-editor';
  rl.forEach(function(ri){
    var rp=Object.assign({},DEFAULT_PERMS[ri.role],sp[ri.role]||{});
    var card=document.createElement('div');card.className='role-card';
    var hd=document.createElement('div');hd.className='role-card-head';
    var s1=document.createElement('span');s1.textContent=ri.ico;
    var s2=document.createElement('span');
    s2.style.cssText='font-weight:700;font-size:13px;color:'+ri.clr;
    s2.textContent=ri.lbl;
    hd.appendChild(s1);hd.appendChild(s2);
    if(ri.locked){var s3=document.createElement('span');s3.style.cssText='font-size:10px;color:var(--t3);margin-left:auto';s3.textContent='\u043D\u0435\u0437\u043C\u0456\u043D\u043D\u0430';hd.appendChild(s3);}
    card.appendChild(hd);
    var bd=document.createElement('div');bd.className='role-card-body';
    Object.keys(PERM_LABELS).forEach(function(key){
      var pr=document.createElement('div');pr.className='perm-row';
      var pl=document.createElement('span');pl.className='perm-label';pl.textContent=PERM_LABELS[key];
      var tl=document.createElement('label');tl.className='toggle';
      var cb=document.createElement('input');cb.type='checkbox';cb.checked=!!rp[key];if(ri.locked)cb.disabled=true;
      (function(role,k){cb.addEventListener('change',function(){gcLivePermChange(role,k,this.checked);});})(ri.role,key);
      var sl=document.createElement('span');sl.className='toggle-slider';
      tl.appendChild(cb);tl.appendChild(sl);
      pr.appendChild(pl);pr.appendChild(tl);bd.appendChild(pr);
    });
    card.appendChild(bd);grid.appendChild(card);
  });
  cont.appendChild(grid);
  var bar=document.createElement('div');bar.className='gc-save-bar';
  bar.innerHTML='<span style="font-size:12px;color:var(--tut)">&#10003; \u0417\u043C\u0456\u043D\u0438 \u043C\u0438\u0442\u0442\u0454\u0432\u0456</span>'
    +'<button class="btn btn-g btn-sm" style="margin-left:auto" onclick="gcResetRoles()">&#8635; \u0421\u043A\u0438\u043D\u0443\u0442\u0438</button>';
  cont.appendChild(bar);
}

function gcLivePermChange(role,key,val){
  // 1. Save to config
  var cfg=gcGetConfig();
  if(!cfg.perms)cfg.perms={};
  if(!cfg.perms[role])cfg.perms[role]={};
  cfg.perms[role][key]=val;
  gcSet('perms',cfg.perms);
  // 2. Apply live to ROLES object
  if(ROLES[role]){
    ROLES[role].can[key]=val;
    if(key==='seeIncome')ROLES[role].seeIncome=val;
    if(key==='seeAll')ROLES[role].seeAll=val;
  }
  // 3. Refresh current view if needed
  var pg=S.currentPage;
  if(pg==='dashboard')renderDash();
  mkToast(role+': '+PERM_LABELS[key]+' \u2192 '+(val?'\u2705':'\u274C'));
}

function gcResetRoles(){
  if(!confirm('\u0421\u043A\u0438\u043D\u0443\u0442\u0438 \u0432\u0441\u0456 \u043F\u0440\u0430\u0432\u0430 \u0434\u043E \u0441\u0442\u0430\u043D\u0434\u0430\u0440\u0442\u043D\u0438\u0445?'))return;
  gcSet('perms',null);
  // Restore ROLES
  ['director','admin','tutor'].forEach(function(role){
    ROLES[role].can=Object.assign({},DEFAULT_PERMS[role]);
    ROLES[role].seeIncome=DEFAULT_PERMS[role].seeIncome;
    ROLES[role].seeAll=DEFAULT_PERMS[role].seeAll;
  });
  gcRenderRoles();
  mkToast('\u041F\u0440\u0430\u0432\u0430 \u0441\u043A\u0438\u043D\u0443\u0442\u043E \u0434\u043E \u0441\u0442\u0430\u043D\u0434\u0430\u0440\u0442\u043D\u0438\u0445');
}

function gcGetNavItems(){
  var cfg=gcGetConfig();
  if(cfg.navItems&&cfg.navItems.length)return cfg.navItems;
  return DEFAULT_NAV_CFG.map(function(n){return Object.assign({},n);});
}

function gcRenderNav(){
  var ni=gcGetNavItems();
  var rk=['god','director','admin','tutor'];
  var rico={god:'\u26A1',director:'\uD83D\uDC51',admin:'\uD83D\uDEE1',tutor:'\uD83D\uDCDA'};
  var el=document.getElementById('gc-nav-editor');
  var wrap=document.createElement('div');wrap.className='nav-editor';
  ni.forEach(function(n,i){
    var rArr=n.roles||rk;
    var row=document.createElement('div');row.className='nav-edit-row';row.draggable=true;
    (function(idx){
      row.addEventListener('dragstart',function(e){gcDragStart(e,idx);});
      row.addEventListener('dragover',function(e){gcDragOver(e,idx);});
      row.addEventListener('drop',function(e){gcDrop(e,idx);});
      row.addEventListener('dragleave',function(){gcDragLeave();});
    })(i);
    var dh=document.createElement('span');dh.className='drag-handle';dh.innerHTML='&#8283;';row.appendChild(dh);
    var icoI=document.createElement('input');icoI.type='text';icoI.value=n.ico||'';
    icoI.style.cssText='width:44px;font-size:16px;text-align:center;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t1);padding:4px 6px';
    (function(idx){icoI.addEventListener('input',function(){gcLiveNavChange(idx,'ico',this.value);});})(i);
    row.appendChild(icoI);
    var lblI=document.createElement('input');lblI.type='text';lblI.value=n.lbl||'';
    lblI.style.cssText='flex:1;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t1);padding:5px 8px;font-size:13px';
    (function(idx){lblI.addEventListener('input',function(){gcLiveNavChange(idx,'lbl',this.value);});})(i);
    row.appendChild(lblI);
    var secI=document.createElement('input');secI.type='text';secI.value=n.sec||'';
    secI.style.cssText='width:95px;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t2);padding:5px 8px;font-size:11px';
    (function(idx){secI.addEventListener('input',function(){gcLiveNavChange(idx,'sec',this.value);});})(i);
    row.appendChild(secI);
    var cbWrap=document.createElement('div');cbWrap.className='nav-vis-checkboxes';
    rk.forEach(function(r){
      var lbl=document.createElement('label');lbl.className='nav-vis-cb';lbl.title=r;
      var cb=document.createElement('input');cb.type='checkbox';cb.checked=rArr.includes(r);
      (function(idx,role){cb.addEventListener('change',function(){gcLiveNavRole(idx,role,this.checked);});})(i,r);
      var ico=document.createElement('span');ico.textContent=rico[r]||r;
      lbl.appendChild(cb);lbl.appendChild(ico);cbWrap.appendChild(lbl);
    });
    row.appendChild(cbWrap);
    if(n.custom){
      var del=document.createElement('button');del.className='btn btn-sm';
      del.style.cssText='background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger);padding:4px 8px';
      del.innerHTML='&times;';
      (function(idx){del.addEventListener('click',function(){gcDelNavItem(idx);});})(i);
      row.appendChild(del);
    }
    wrap.appendChild(row);
  });
  el.innerHTML='';el.appendChild(wrap);
  var bar=document.createElement('div');bar.className='gc-save-bar';
  bar.innerHTML='<span style="font-size:12px;color:var(--tut)">&#10003; \u0417\u043C\u0456\u043D\u0438 \u043C\u0438\u0442\u0442\u0454\u0432\u0456</span>'
    +'<button class="btn btn-g btn-sm" style="margin-left:auto" onclick="gcResetNav()">&#8635; \u0421\u043A\u0438\u043D\u0443\u0442\u0438</button>';
  el.appendChild(bar);
}

function gcEsc(str){return (str||'').replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;');}

function gcLiveNavChange(idx,key,val){
  var items=gcGetNavItems();
  items[idx][key]=val;
  gcSet('navItems',items);
  buildSidebar();
  try{ buildBottomNav(); }catch(e){}
  // Highlight current active
  var nel=document.getElementById('ni-'+S.currentPage);
  if(nel)nel.classList.add('active');
}

function gcLiveNavRole(idx,role,checked){
  var items=gcGetNavItems();
  if(!items[idx].roles)items[idx].roles=['god','director','admin','tutor'];
  if(checked){if(!items[idx].roles.includes(role))items[idx].roles.push(role);}
  else items[idx].roles=items[idx].roles.filter(function(r){return r!==role;});
  gcSet('navItems',items);
  buildSidebar();
  try{ buildBottomNav(); }catch(e){}
  var nel=document.getElementById('ni-'+S.currentPage);
  if(nel)nel.classList.add('active');
}

function gcDelNavItem(idx){
  var items=gcGetNavItems();
  if(!items[idx].custom){mkToast('\u0421\u0438\u0441\u0442\u0435\u043C\u043D\u0456 \u0432\u043A\u043B\u0430\u0434\u043A\u0438 \u043D\u0435 \u043C\u043E\u0436\u043D\u0430 \u0432\u0438\u0434\u0430\u043B\u044F\u0442\u0438','error');return;}
  items.splice(idx,1);
  gcSet('navItems',items);
  gcRenderNav();
  buildSidebar();
  try{ buildBottomNav(); }catch(e){}
}

function gcAddNavItem(){
  var ico=document.getElementById('gc-new-ico').value.trim()||'\u2B50';
  var lbl=document.getElementById('gc-new-lbl').value.trim();
  if(!lbl){mkToast('\u0412\u0432\u0435\u0434\u0456\u0442\u044C \u043D\u0430\u0437\u0432\u0443 \u0432\u043A\u043B\u0430\u0434\u043A\u0438','error');return;}
  var sec=document.getElementById('gc-new-sec').value.trim()||'\u0406\u043D\u0448\u0435';
  var items=gcGetNavItems();
  items.push({id:'custom_'+uid(),ico:ico,lbl:lbl,sec:sec,badge:false,roles:['god','director','admin','tutor'],custom:true});
  gcSet('navItems',items);
  document.getElementById('gc-new-ico').value='';
  document.getElementById('gc-new-lbl').value='';
  document.getElementById('gc-new-sec').value='';
  gcRenderNav();
  buildSidebar();
  try{ buildBottomNav(); }catch(e){}
  mkToast('\u0412\u043A\u043B\u0430\u0434\u043A\u0443 "'+lbl+'" \u0434\u043E\u0434\u0430\u043D\u043E');
}

function gcResetNav(){
  if(!confirm('\u0421\u043A\u0438\u043D\u0443\u0442\u0438 \u043D\u0430\u0432\u0456\u0433\u0430\u0446\u0456\u044E \u0434\u043E \u0441\u0442\u0430\u043D\u0434\u0430\u0440\u0442\u043D\u043E\u0457?'))return;
  gcSet('navItems',null);
  gcRenderNav();
  buildSidebar();
  try{ buildBottomNav(); }catch(e){}
  var nel=document.getElementById('ni-'+S.currentPage);
  if(nel)nel.classList.add('active');
  mkToast('\u041D\u0430\u0432\u0456\u0433\u0430\u0446\u0456\u044E \u0441\u043A\u0438\u043D\u0443\u0442\u043E');
}

function gcDragStart(e,idx){_gcDragSrc=idx;e.dataTransfer.effectAllowed='move';}

function gcDragOver(e,idx){
  e.preventDefault();e.dataTransfer.dropEffect='move';
  document.querySelectorAll('.nav-edit-row').forEach(function(r,i){
    r.classList.toggle('drag-over',i===idx&&i!==_gcDragSrc);
  });
}

function gcDragLeave(){document.querySelectorAll('.nav-edit-row').forEach(function(r){r.classList.remove('drag-over');});}

function gcDrop(e,targetIdx){
  e.preventDefault();gcDragLeave();
  if(_gcDragSrc===null||_gcDragSrc===targetIdx){_gcDragSrc=null;return;}
  var items=gcGetNavItems();
  var moved=items.splice(_gcDragSrc,1)[0];
  items.splice(targetIdx,0,moved);
  _gcDragSrc=null;
  gcSet('navItems',items);
  gcRenderNav();
  buildSidebar();
  try{ buildBottomNav(); }catch(e){}
  var nel=document.getElementById('ni-'+S.currentPage);
  if(nel)nel.classList.add('active');
}

function gcGetFields(){
  var cfg=gcGetConfig();
  return (cfg.customFields||[]).slice();
}

function gcRenderFields(){
  var fields=gcGetFields();
  var targets={student:'\u0423\u0447\u0435\u043D\u044C',lesson:'\u0417\u0430\u043D\u044F\u0442\u0442\u044F',tutor:'\u0412\u0438\u043A\u043B\u0430\u0434\u0430\u0447',payment:'\u041F\u043B\u0430\u0442\u0456\u0436'};
  var el=document.getElementById('gc-field-editor');
  if(!fields.length){
    el.innerHTML='<div class="empty" style="padding:24px"><div class="ei">\uD83D\uDDC2</div>\u0414\u043E\u0434\u0430\u0442\u043A\u043E\u0432\u0438\u0445 \u043F\u043E\u043B\u0456\u0432 \u043D\u0435\u043C\u0430\u0454.<br><span style="font-size:12px">\u0421\u043A\u043E\u0440\u0438\u0441\u0442\u0430\u0439\u0442\u0435\u0441\u044F \u0444\u043E\u0440\u043C\u043E\u044E \u043D\u0438\u0436\u0447\u0435, \u0449\u043E\u0431 \u0434\u043E\u0434\u0430\u0442\u0438 \u043F\u0435\u0440\u0448\u0435 \u043F\u043E\u043B\u0435.</span></div>';
    return;
  }
  var rows='';
  fields.forEach(function(f,i){
    var tSel='';
    Object.keys(targets).forEach(function(k){
      tSel+='<option value="'+k+'"'+(f.target===k?' selected':'')+'>'+targets[k]+'</option>';
    });
    var extraInput='';
    if(f.type==='select'){
      extraInput='<input type="text" value="'+gcEsc((f.options||[]).join('; '))+'" placeholder="\u0412\u0430\u0440\u0456\u0430\u043D\u0442\u0438 \u0447\u0435\u0440\u0435\u0437 ; "'+
        ' style="flex:1;min-width:100px;font-size:11px;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t1);padding:4px 8px"'+
        ' oninput="gcLiveFieldOpts('+i+',this.value)">';
    } else {
      extraInput='<span style="flex:1"></span>';
    }
    rows+='<div class="field-row">'
      +'<span style="color:var(--t3);font-size:14px;cursor:grab;padding:0 2px">&#8283;</span>'
      +'<span class="field-type-badge">'+FIELD_TYPE_ICONS[f.type]+' '+FIELD_TYPE_LABELS[f.type]+'</span>'
      +'<input type="text" value="'+gcEsc(f.label||'')+'" placeholder="\u041D\u0430\u0437\u0432\u0430 \u043F\u043E\u043B\u044F"'
      +' style="flex:1;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t1);padding:5px 8px;font-size:13px;font-family:Karla,sans-serif"'
      +' oninput="gcLiveFieldLabel('+i+',this.value)">'
      +'<select style="width:95px;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t2);padding:4px 6px;font-size:11px" onchange="gcLiveFieldTarget('+i+',this.value)">'+tSel+'</select>'
      +extraInput
      +'<label style="display:flex;align-items:center;gap:4px;font-size:11px;color:var(--t2);white-space:nowrap;cursor:pointer">'
      +'<input type="checkbox" '+(f.required?'checked':'')+' style="accent-color:var(--god2);cursor:pointer" onchange="gcLiveFieldReq('+i+',this.checked)"> *\u043E\u0431\u043E\u0432</label>'
      +'<button class="btn btn-sm" style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger)" onclick="gcDelField('+i+')">&times;</button>'
      +'</div>';
  });
  el.innerHTML='<div class="field-editor">'+rows+'</div>'
    +'<div class="gc-save-bar">'
    +'<span style="font-size:12px;color:var(--tut)">&#10003; \u041F\u043E\u043B\u044F \u0432\u0456\u0434\u043E\u0431\u0440\u0430\u0436\u0430\u044E\u0442\u044C\u0441\u044F \u0443 \u0444\u043E\u0440\u043C\u0430\u0445 \u043F\u0440\u0438 \u0434\u043E\u0434\u0430\u0432\u0430\u043D\u043D\u0456 \u0437\u0430\u043F\u0438\u0441\u0456\u0432</span>'
    +'<button class="btn btn-g btn-sm" style="margin-left:auto" onclick="gcClearFields()">&#128465; \u041E\u0447\u0438\u0441\u0442\u0438\u0442\u0438 \u0432\u0441\u0456</button>'
    +'</div>';
}

function gcLiveFieldLabel(idx,val){var f=gcGetFields();f[idx].label=val;gcSet('customFields',f);}

function gcLiveFieldTarget(idx,val){var f=gcGetFields();f[idx].target=val;gcSet('customFields',f);}

function gcLiveFieldReq(idx,val){var f=gcGetFields();f[idx].required=val;gcSet('customFields',f);}

function gcLiveFieldOpts(idx,val){
  var f=gcGetFields();
  f[idx].options=val.split(';').map(function(x){return x.trim();}).filter(Boolean);
  gcSet('customFields',f);
}

function gcDelField(idx){
  var f=gcGetFields();f.splice(idx,1);gcSet('customFields',f);gcRenderFields();
  mkToast('\u041F\u043E\u043B\u0435 \u0432\u0438\u0434\u0430\u043B\u0435\u043D\u043E');
}

function gcClearFields(){
  if(!confirm('\u0412\u0438\u0434\u0430\u043B\u0438\u0442\u0438 \u0432\u0441\u0456 \u0434\u043E\u0434\u0430\u0442\u043A\u043E\u0432\u0456 \u043F\u043E\u043B\u044F?'))return;
  gcSet('customFields',[]);gcRenderFields();mkToast('\u041F\u043E\u043B\u044F \u043E\u0447\u0438\u0449\u0435\u043D\u043E');
}

function gcAddField(){
  var type=document.getElementById('gc-field-type').value;
  var label=document.getElementById('gc-field-lbl').value.trim();
  var target=document.getElementById('gc-field-target').value;
  var optsRaw=document.getElementById('gc-field-opts').value;
  if(type!=='divider'&&type!=='label'&&!label){mkToast('\u0412\u0432\u0435\u0434\u0456\u0442\u044C \u043D\u0430\u0437\u0432\u0443 \u043F\u043E\u043B\u044F','error');return;}
  var f=gcGetFields();
  var obj={id:'cf_'+uid(),type:type,label:label||(type==='divider'?'---':'\u0417\u0430\u0433\u043E\u043B\u043E\u0432\u043E\u043A'),target:target,required:false};
  if(type==='select'&&optsRaw)obj.options=optsRaw.split(';').map(function(x){return x.trim();}).filter(Boolean);
  f.push(obj);
  gcSet('customFields',f);
  document.getElementById('gc-field-lbl').value='';
  document.getElementById('gc-field-opts').value='';
  gcRenderFields();
  mkToast('"'+obj.label+'" \u0434\u043E\u0434\u0430\u043D\u043E \u0434\u043E \u0444\u043E\u0440\u043C\u0438 "'+target+'"');
}

function gcGetLabels(){
  var cfg=gcGetConfig();
  var out={};
  Object.keys(DEFAULT_LABELS_MAP).forEach(function(k){out[k]=DEFAULT_LABELS_MAP[k].def;});
  Object.assign(out,cfg.labels||{});
  return out;
}

function gcRenderLabels(){
  var labels=gcGetLabels();
  var el=document.getElementById('gc-labels-editor');
  var wrapper=document.createElement('div');
  wrapper.style.cssText='display:grid;grid-template-columns:1fr 1fr;gap:12px';
  Object.keys(DEFAULT_LABELS_MAP).forEach(function(key){
    var def=DEFAULT_LABELS_MAP[key];
    var div=document.createElement('div');
    div.className='fgr';
    var lbl=document.createElement('label');
    lbl.style.cssText='display:flex;align-items:center;justify-content:space-between;gap:4px';
    var lblText=document.createElement('span');
    lblText.textContent=def.label;
    var resetBtn=document.createElement('button');
    resetBtn.type='button';
    resetBtn.innerHTML='&#8635;';
    resetBtn.title='\u0421\u043A\u0438\u043D\u0443\u0442\u0438 \u0434\u043E \u0441\u0442\u0430\u043D\u0434\u0430\u0440\u0442\u043D\u043E\u0433\u043E';
    resetBtn.style.cssText='background:none;border:none;color:var(--t3);cursor:pointer;font-size:13px;padding:0 2px';
    resetBtn.addEventListener('click',function(){gcResetLabel(key);});
    lbl.appendChild(lblText);
    lbl.appendChild(resetBtn);
    var inp=document.createElement('input');
    inp.type='text';
    inp.value=labels[key]||'';
    inp.placeholder=def.def;
    inp.style.cssText='width:100%;background:var(--s2);border:1px solid var(--b1);border-radius:8px;color:var(--t1);padding:9px 12px;font-size:13px;outline:none;transition:border .15s';
    inp.addEventListener('focus',function(){this.style.borderColor='var(--god2)';});
    inp.addEventListener('blur',function(){this.style.borderColor='var(--b1)';});
    inp.addEventListener('input',function(){gcLiveLabelChange(key,this.value);});
    div.appendChild(lbl);
    div.appendChild(inp);
    wrapper.appendChild(div);
  });
  var saveBar=document.createElement('div');
  saveBar.className='gc-save-bar';
  saveBar.innerHTML='<span style="font-size:12px;color:var(--tut)">&#10003; \u0422\u0435\u043A\u0441\u0442\u0438 \u043E\u043D\u043E\u0432\u043B\u044E\u044E\u0442\u044C\u0441\u044F \u0432 \u0456\u043D\u0442\u0435\u0440\u0444\u0435\u0439\u0441\u0456 \u043C\u0438\u0442\u0442\u0454\u0432\u043E</span>'
    +'<button class="btn btn-g btn-sm" style="margin-left:auto" onclick="gcResetAllLabels()">&#8635; \u0421\u043A\u0438\u043D\u0443\u0442\u0438 \u0432\u0441\u0456</button>';
  el.innerHTML='';
  el.appendChild(wrapper);
  el.appendChild(saveBar);
}

function gcLiveLabelChange(key,val){
  var cfg=gcGetConfig();
  if(!cfg.labels)cfg.labels={};
  if(val===DEFAULT_LABELS_MAP[key].def||val===''){delete cfg.labels[key];} else {cfg.labels[key]=val;}
  gcSet('labels',cfg.labels&&Object.keys(cfg.labels).length?cfg.labels:null);
  // Apply live
  gcApplyLabel(key,val||DEFAULT_LABELS_MAP[key].def);
}

function gcApplyLabel(key,val){
  var ptitle=document.getElementById('ptitle');
  if(key==='studentsTitle'){PLABELS.students=val;if(S.currentPage==='students'&&ptitle)ptitle.textContent=val;}
  if(key==='tutorsTitle'){PLABELS.tutors=val;if(S.currentPage==='tutors'&&ptitle)ptitle.textContent=val;}
  if(key==='lessonsTitle'){PLABELS.lessons=val;if(S.currentPage==='lessons'&&ptitle)ptitle.textContent=val;}
  if(key==='paymentsTitle'){PLABELS.payments=val;if(S.currentPage==='payments'&&ptitle)ptitle.textContent=val;}
  if(key==='scheduleTitle'){PLABELS.schedule=val;if(S.currentPage==='schedule'&&ptitle)ptitle.textContent=val;}
  if(key==='reportsTitle'){PLABELS.reports=val;if(S.currentPage==='reports'&&ptitle)ptitle.textContent=val;}
  if(key==='appName'){var sblt=document.querySelector('.sblt');if(sblt)sblt.textContent=val;}
  if(key==='loginTitle'){var elt=document.querySelector('.lh');if(elt)elt.textContent=val;}
  if(key==='loginSub'){var els=document.querySelector('.lsub');if(els)els.textContent=val;}
  var ab=document.getElementById('addbtn');
  if(ab&&key==='addStudent'&&S.currentPage==='students')ab.textContent='+ '+val;
  if(ab&&key==='addLesson'&&(S.currentPage==='lessons'||S.currentPage==='schedule'))ab.textContent='+ '+val;
  if(ab&&key==='addPayment'&&S.currentPage==='payments')ab.textContent='+ '+val;
  if(ab&&key==='addTutor'&&S.currentPage==='tutors')ab.textContent='+ '+val;
  buildSidebar();
  try{ buildBottomNav(); }catch(e){}
  var nel=document.getElementById('ni-'+S.currentPage);if(nel)nel.classList.add('active');
}

function gcResetLabel(key){
  var cfg=gcGetConfig();
  if(cfg.labels){delete cfg.labels[key];gcSet('labels',Object.keys(cfg.labels).length?cfg.labels:null);}
  gcRenderLabels();
  gcApplyLabel(key,DEFAULT_LABELS_MAP[key].def);
}

function gcResetAllLabels(){
  if(!confirm('\u0421\u043A\u0438\u043D\u0443\u0442\u0438 \u0432\u0441\u0456 \u0442\u0435\u043A\u0441\u0442\u0438 \u0434\u043E \u0441\u0442\u0430\u043D\u0434\u0430\u0440\u0442\u043D\u0438\u0445?'))return;
  gcSet('labels',null);
  var labels={};
  Object.keys(DEFAULT_LABELS_MAP).forEach(function(k){labels[k]=DEFAULT_LABELS_MAP[k].def;});
  Object.keys(labels).forEach(function(k){gcApplyLabel(k,labels[k]);});
  gcRenderLabels();
  mkToast('\u0412\u0441\u0456 \u0442\u0435\u043A\u0441\u0442\u0438 \u0441\u043A\u0438\u043D\u0443\u0442\u043E');
}

function applyGodConfig(){
  var cfg=gcGetConfig();
  // Apply permissions
  if(cfg.perms){
    ['director','admin','tutor'].forEach(function(role){
      if(cfg.perms[role]){
        Object.assign(ROLES[role].can,cfg.perms[role]);
        if('seeIncome' in cfg.perms[role])ROLES[role].seeIncome=cfg.perms[role].seeIncome;
        if('seeAll' in cfg.perms[role])ROLES[role].seeAll=cfg.perms[role].seeAll;
      }
    });
  }
  // Apply labels
  if(cfg.labels){
    Object.keys(cfg.labels).forEach(function(k){
      gcApplyLabel(k,cfg.labels[k]);
    });
  }
}

function renderCustomFields(target,containerId){
  var cfg=gcGetConfig();
  var fields=(cfg.customFields||[]).filter(function(f){return f.target===target;});
  if(!fields.length)return;
  var el=document.getElementById(containerId);
  if(!el)return;
  var html='<div class="fgr full" style="border-top:1px solid var(--b1);padding-top:12px;margin-top:6px">'
    +'<label style="color:var(--adm);letter-spacing:.5px">&#9889; \u0414\u043E\u0434\u0430\u0442\u043A\u043E\u0432\u0456 \u043F\u043E\u043B\u044F</label></div>';
  fields.forEach(function(f){
    if(f.type==='divider'){
      html+='<div style="grid-column:1/-1;border-top:1px solid var(--b1);margin:4px 0;font-size:11px;color:var(--t3);padding-top:4px">'+(f.label!=='---'?f.label:'')+'</div>';
    } else if(f.type==='label'){
      html+='<div class="fgr full"><div style="font-size:12px;font-weight:700;color:var(--adm);letter-spacing:.5px;text-transform:uppercase;margin-top:6px">'+f.label+'</div></div>';
    } else if(f.type==='checkbox'){
      html+='<div class="fgr"><label style="display:flex;align-items:center;gap:8px;cursor:pointer"><input type="checkbox" id="cf_'+f.id+'" style="accent-color:var(--adm);width:14px;height:14px"> <span>'+f.label+(f.required?' *':'')+'</span></label></div>';
    } else if(f.type==='select'){
      var opts='<option value="">\u041E\u0431\u0435\u0440\u0456\u0442\u044C...</option>';
      (f.options||[]).forEach(function(o){opts+='<option>'+o+'</option>';});
      html+='<div class="fgr"><label>'+f.label+(f.required?' *':'')+'</label><select id="cf_'+f.id+'">'+opts+'</select></div>';
    } else if(f.type==='textarea'){
      html+='<div class="fgr full"><label>'+f.label+(f.required?' *':'')+'</label><textarea id="cf_'+f.id+'" placeholder="'+f.label+'..."></textarea></div>';
    } else {
      var itype=f.type==='number'?'number':(f.type==='date'?'date':'text');
      html+='<div class="fgr"><label>'+f.label+(f.required?' *':'')+'</label><input id="cf_'+f.id+'" type="'+itype+'" placeholder="'+f.label+(f.required?' *':'')+'"></div>';
    }
  });
  el.insertAdjacentHTML('beforeend',html);
}

function gcSaveRoles(){mkToast('\u041F\u0440\u0430\u0432\u0430 \u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043D\u043E \u2705');}

function gcSaveNav(){buildSidebar();mkToast('\u041D\u0430\u0432\u0456\u0433\u0430\u0446\u0456\u044E \u043E\u043D\u043E\u0432\u043B\u0435\u043D\u043E \u2705');}

function gcSaveFields(){mkToast('\u041F\u043E\u043B\u044F \u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043D\u043E \u2705');}

function gcResetLabels(){gcResetAllLabels();}

function gcSaveLabels(){mkToast('\u0422\u0435\u043A\u0441\u0442\u0438 \u0437\u0431\u0435\u0440\u0435\u0436\u0435\u043D\u043E \u2705');}

// ═══════════════════════════════════════════════════════════════
//  КОМАНДНА ПАЛІТРА (Ctrl+K)
//  Один рядок замість блукання по меню: пошук учнів, перехід
//  на сторінки та швидкі дії — усе з клавіатури.
// ═══════════════════════════════════════════════════════════════
var _cmdSel = 0;
