
// = Supabase integration =
// =
// SUPABASE CONFIG -    
// app.supabase.com  Project Settings  API
// =
var SUPABASE_URL  = 'https://rndxbvwisppxnhvrzwqi.supabase.co';
var SUPABASE_ANON = 'sb_publishable_21KKA9MELBdwMRj4XG0riw_NuLYzpAw';

// ═══════════════════════════════════════════════════════════════
//  ГАРЯЧІ КЛАВІШІ
//  Реєструємо НА ПОЧАТКУ файлу: якщо десь нижче станеться помилка,
//  палітра все одно відкриватиметься.
//  Ctrl+K — основна комбінація. Ctrl+/ — запасна, бо Ctrl+K
//  у деяких браузерах і розширеннях перехоплюється своїм пошуком.
// ═══════════════════════════════════════════════════════════════
document.addEventListener('keydown', function(e){
  var key=String(e.key||'').toLowerCase();
  if((e.ctrlKey||e.metaKey) && (key==='k' || key==='/')){
    e.preventDefault();
    e.stopPropagation();
    if(typeof cmdOpen==='function'){ cmdOpen(); return; }
    var gs=document.getElementById('gsearch');
    if(gs){ gs.focus(); gs.select(); }
  }
  if(e.key==='Escape'){
    if(typeof cmdClose==='function') cmdClose();
    if(typeof closeQuickPopup==='function') closeQuickPopup();
  }
}, true);   // true = перехоплюємо раніше за інші обробники

// =
// APP STATE
// =
var _sb = null;
var CU  = null;   // current user profile from DB
var S   = {
  students:[], tutors:[], lessons:[], payments:[],
  users:[], subjects:[], comms:[], branches:[], pricingRules:[],
  settings:{}, currentBranchId:null,
  weekOffset:0, dayOffset:0, dashWeekOffset:0,
  currentPage:'dashboard', editId:null, schView:'week',
  sfCur:'all', godConfig:null
};
var sfCur = 'all';
var _channels = [];
var _syncTimer = null;
