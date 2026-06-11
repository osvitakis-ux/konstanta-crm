window.SupabaseMini = {
  createClient: function(url, anonKey) {
    var _url = url.replace(/\/$/, ""), _key = anonKey, _token = null, _refreshToken = null, _authListeners = [];
    function headers() {
      return {
        "Content-Type": "application/json",
        apikey: _key,
        Authorization: "Bearer " + (_token || _key)
      };
    }
    function query(table, method, body, select, extraHeaders) {
      return select = select && "*" !== select ? "?select=" + encodeURIComponent(select) : "", 
      {
        _table: table,
        _method: method,
        _body: body,
        _url: _url + "/rest/v1/" + table + select,
        _h: Object.assign({}, headers(), {
          Prefer: "return=representation"
        }, extraHeaders || {}),
        _filters: [],
        eq: function(col, val) {
          return this._filters.push(col + "=eq." + encodeURIComponent(val)), this;
        },
        neq: function(col, val) {
          return this._filters.push(col + "=neq." + encodeURIComponent(val)), this;
        },
        order: function(col, opts) {
          return this._filters.push("order=" + col + (opts && !1 === opts.ascending ? ".desc" : ".asc")), 
          this;
        },
        limit: function(n) {
          return this._filters.push("limit=" + n), this;
        },
        single: function() {
          return this._isSingle = !0, this;
        },
        then: function(resolve, reject) {
          return this._exec().then(resolve, reject);
        },
        _exec: async function() {
          var u = this._url, sep = (this._filters.length && (sep = u.includes("?") ? "&" : "?", 
          u += sep + this._filters.join("&")), this._isSingle && (this._h.Accept = "application/vnd.pgrst.object+json"), 
          {
            method: this._method,
            headers: this._h
          });
          this._body && (sep.body = JSON.stringify(this._body));
          try {
            var res = await fetch(u, sep), text = await res.text(), data = text ? JSON.parse(text) : null;
            return res.ok ? {
              data: data,
              error: null
            } : {
              data: null,
              error: {
                message: data && data.message || res.statusText,
                code: res.status
              }
            };
          } catch (e) {
            return {
              data: null,
              error: {
                message: e.message
              }
            };
          }
        }
      };
    }
    return {
      from: function(table) {
        return {
          select: function(cols) {
            return query(table, "GET", null, cols || "*", {});
          },
          insert: function(data) {
            return query(table, "POST", data, null, {});
          },
          update: function(data) {
            return (table => query(table, "PATCH", data, null, {}))(table);
          },
          delete: function() {
            return (table => query(table, "DELETE", null, null, {}))(table);
          },
          upsert: function(data) {
            return query(table, "POST", data, null, {
              Prefer: "resolution=merge-duplicates"
            });
          }
        };
      },
      auth: {
        signInWithPassword: async function(creds) {
          try {
            var res = await fetch(_url + "/auth/v1/token?grant_type=password", {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                apikey: _key
              },
              body: JSON.stringify({
                email: creds.email,
                password: creds.password
              })
            }), data = await res.json();
            if (!res.ok) return {
              data: null,
              error: {
                message: data.error_description || data.msg || "Login failed"
              }
            };
            _token = data.access_token, _refreshToken = data.refresh_token;
            var user = data.user;
            return localStorage.setItem("sb_token", _token), localStorage.setItem("sb_refresh", _refreshToken || ""), 
            localStorage.setItem("sb_user", JSON.stringify(user)), _authListeners.forEach(function(cb) {
              cb("SIGNED_IN", {
                user: user
              });
            }), {
              data: {
                user: user,
                session: data
              },
              error: null
            };
          } catch (e) {
            return {
              data: null,
              error: {
                message: e.message
              }
            };
          }
        },
        signOut: async function() {
          try {
            await fetch(_url + "/auth/v1/logout", {
              method: "POST",
              headers: headers()
            });
          } catch (e) {}
          return _refreshToken = _token = null, localStorage.removeItem("sb_token"), 
          localStorage.removeItem("sb_refresh"), localStorage.removeItem("sb_user"), 
          _authListeners.forEach(function(cb) {
            cb("SIGNED_OUT", null);
          }), {
            error: null
          };
        },
        getSession: async function() {
          var token = localStorage.getItem("sb_token"), userStr = localStorage.getItem("sb_user");
          if (!token || !userStr) return {
            data: {
              session: null
            },
            error: null
          };
          try {
            var res = await fetch(_url + "/auth/v1/user", {
              headers: {
                apikey: _key,
                Authorization: "Bearer " + token
              }
            });
            return res.ok ? (_token = token, {
              data: {
                session: {
                  user: await res.json(),
                  access_token: token
                }
              },
              error: null
            }) : (localStorage.removeItem("sb_token"), localStorage.removeItem("sb_user"), 
            {
              data: {
                session: null
              },
              error: null
            });
          } catch (e) {
            return {
              data: {
                session: null
              },
              error: null
            };
          }
        },
        onAuthStateChange: function(cb) {
          return _authListeners.push(cb), {
            data: {
              subscription: {
                unsubscribe: function() {
                  _authListeners = _authListeners.filter(function(l) {
                    return l !== cb;
                  });
                }
              }
            }
          };
        },
        getUser: async function() {
          var token = _token || localStorage.getItem("sb_token");
          if (!token) return {
            data: {
              user: null
            },
            error: null
          };
          try {
            return {
              data: {
                user: await (await fetch(_url + "/auth/v1/user", {
                  headers: {
                    apikey: _key,
                    Authorization: "Bearer " + token
                  }
                })).json()
              },
              error: null
            };
          } catch (e) {
            return {
              data: {
                user: null
              },
              error: null
            };
          }
        }
      },
      channel: function(name) {
        return {
          on: function(type, opts, cb) {
            return this._cb = cb, this._opts = opts, this;
          },
          subscribe: function() {
            var table;
            return this._cb && (this._cb, table = (this._opts || {}).table, Date.now(), 
            table) && setInterval(async function() {
              "function" != typeof refreshPage || window._saving || refreshPage(table);
            }, 3e4), this;
          }
        };
      },
      removeChannel: function(ch) {},
      rpc: function(fn, params) {
        return query("rpc/" + fn, "POST", params, null, {});
      }
    };
  }
}

function studentSearch(input, hiddenId, callbackFn) {
  var val = input.value;
  var dl = document.getElementById(hiddenId + '-list');
  var hidden = document.getElementById(hiddenId);
  var q = val.toLowerCase();
  var src = hiddenId === 'p-std' ? (S.students || []) : myStudents();
  var matches = src.filter(function(s) {
    return (s.fn + ' ' + s.ln).toLowerCase().includes(q) ||
           (s.ln + ' ' + s.fn).toLowerCase().includes(q) ||
           (s.ln).toLowerCase().startsWith(q);
  }).slice(0, 30);
  if (dl) dl.innerHTML = matches.map(function(s) {
    return '<option value="' + s.fn + ' ' + s.ln + '" data-id="' + s.id + '">';
  }).join('');
  var exact = src.find(function(s) { return s.fn + ' ' + s.ln === val || s.ln + ' ' + s.fn === val; });
  if (exact && hidden) hidden.value = exact.id;
  else if (matches.length === 1 && hidden) hidden.value = matches[0].id;
  else if (hidden && !val) hidden.value = '';
  if (callbackFn && window[callbackFn]) window[callbackFn]();
}

function populateStudentSearch(fieldId, src) {
  src = src || myStudents();
  var dl = document.getElementById(fieldId + '-list');
  if (dl) dl.innerHTML = (src || []).map(function(s) {
    return '<option value="' + s.fn + ' ' + s.ln + '" data-id="' + s.id + '">';
  }).join('');
}

function setStudentSearch(fieldId, studentId) {
  var hidden = document.getElementById(fieldId);
  var input = document.getElementById(fieldId + '-search');
  if (hidden) hidden.value = studentId || '';
  if (input && studentId) {
    var s = (S.students || []).find(function(x) { return x.id === studentId; });
    if (s) input.value = s.fn + ' ' + s.ln;
    else input.value = '';
  } else if (input) input.value = '';
}

function renderCommsPage() {
  var tbody = document.getElementById('comms-tbody');
  if (!tbody) return;
  var _tutorSelfId = null;
  if (typeof R === 'function' && R() === 'tutor') {
    var _myT = (S.tutors || []).find(function(t) { return CU && (t.accId === CU.id || t.acc_uid === CU.id); });
    if (_myT) _tutorSelfId = _myT.id;
  }
  var fStud = (document.getElementById('comm-f-student') || {value:''}).value;
  var fTutor = (document.getElementById('comm-f-tutor') || {value:''}).value;
  var fType = (document.getElementById('comm-f-type') || {value:''}).value;
  var comms = [].concat(S.comms || []).sort(function(a, b) { return (b.date || '').localeCompare(a.date || ''); });
  if (_tutorSelfId) comms = comms.filter(function(c) { return (c.tutorId || c.tutor_id) === _tutorSelfId; });
  if (fStud) comms = comms.filter(function(c) { return (c.studentId || c.student_id) === fStud; });
  if (fTutor) comms = comms.filter(function(c) { return (c.tutorId || c.tutor_id) === fTutor; });
  if (fType) comms = comms.filter(function(c) { return c.type === fType; });
  var ico = {call: '\u260E', message: '\uD83D\uDCAC', meeting: '\uD83E\uDD1D', email: '\u2709', other: '\uD83D\uDCCB'};
  if (!comms.length) {
    tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;padding:20px;color:var(--t3)">' + '\u041a\u043e\u043c\u0443\u043d\u0456\u043a\u0430\u0446\u0456\u0439 \u043d\u0435\u043c\u0430\u0454' + '</td></tr>';
    return;
  }
  tbody.innerHTML = comms.map(function(c) {
    var tutor = (S.tutors || []).find(function(t) { return t.id === (c.tutorId || c.tutor_id); });
    var student = (S.students || []).find(function(s) { return s.id === (c.studentId || c.student_id); });
    return '<tr><td style="font-size:11px;color:var(--t2)">' + fd(c.date) + '</td>'
      + '<td>' + (ico[c.type] || '\uD83D\uDCCB') + ' ' + (c.type || '\u2014') + '</td>'
      + '<td>' + (student ? student.fn + ' ' + student.ln : '\u2014') + '</td>'
      + '<td>' + (tutor ? tutor.fn + ' ' + tutor.ln : '\u2014') + '</td>'
      + '<td>' + (c.note || '\u2014') + '</td></tr>';
  }).join('');
}

function renderMissedLessons() {
  var tbody = document.getElementById('missed-tbody');
  if (!tbody) return;
  var fStud = (document.getElementById('missed-f-student') || {value:''}).value;
  var _tutorSelfId2 = null;
  if (typeof R === 'function' && R() === 'tutor') {
    var _myT2 = (S.tutors || []).find(function(t) { return CU && (t.accId === CU.id || t.acc_uid === CU.id); });
    if (_myT2) _tutorSelfId2 = _myT2.id;
  }
  var missed = (S.lessons || []).filter(function(l) {
    return (l.status === 'missed' || l.status === 'makeup') &&
           (!_tutorSelfId2 || (l.tutorId || l.tutor_id) === _tutorSelfId2);
  }).sort(function(a, b) { return (b.date || '').localeCompare(a.date || ''); });
  if (fStud) missed = missed.filter(function(l) { return (l.studentId || l.student_id) === fStud; });
  if (!missed.length) {
    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:20px;color:var(--t3)">' + '\u041f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u0438\u0445 \u0443\u0440\u043e\u043a\u0456\u0432 \u043d\u0435\u043c\u0430\u0454' + '</td></tr>';
    return;
  }
  tbody.innerHTML = missed.map(function(l) {
    var student = (S.students || []).find(function(s) { return s.id === (l.studentId || l.student_id); });
    var tutor = (S.tutors || []).find(function(t) { return t.id === (l.tutorId || l.tutor_id); });
    var stLbl = l.status === 'missed'
      ? '<span style="color:#ef4444;font-weight:600">\u041f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u043e</span>'
      : '<span style="color:#f59e0b;font-weight:600">\u0412\u0456\u0434\u043f\u0440\u0430\u0446\u044c\u043e\u0432\u0430\u043d\u043e</span>';
    return '<tr><td>' + fd(l.date) + '</td>'
      + '<td>' + (student ? student.fn + ' ' + student.ln : '\u2014') + '</td>'
      + '<td>' + (tutor ? tutor.fn + ' ' + tutor.ln : '\u2014') + '</td>'
      + '<td>' + (l.subject || '\u2014') + '</td>'
      + '<td>' + stLbl + '</td>'
      + '<td style="font-size:11px;color:var(--t2)">' + (l.missed_date ? fd(l.missed_date) : '\u2014') + '</td>'
      + '<td style="font-size:11px;color:var(--t2)">' + (l.makeup_date ? fd(l.makeup_date) : '\u2014') + '</td></tr>';
  }).join('');
}

async function logInvoice(channel, recipient, studentId, from, to, lessonsCount, total) {
  if (!CU || !_sb) return;
  try {
    await _sb.from('invoice_log').insert({
      sent_by: CU.id, student_id: studentId || null,
      period_from: from || null, period_to: to || null,
      lessons_count: lessonsCount || 0, total_amount: total || 0,
      channel: channel, recipient: recipient || '', branch_id: currentBranch() || null
    });
  } catch(e) { console.warn('logInvoice error:', e); }
}

function onLessStatChange() {
  var stat = (document.getElementById('l-stat') || {value:''}).value;
  var mkWrap = document.getElementById('l-makeup-wrap');
  var msWrap = document.getElementById('l-miss-wrap');
  if (mkWrap) mkWrap.style.display = stat === 'makeup' ? 'block' : 'none';
  if (msWrap) msWrap.style.display = (stat === 'missed' || stat === 'makeup') ? 'block' : 'none';
}

window.studentSearch = studentSearch;
window.populateStudentSearch = populateStudentSearch;
window.setStudentSearch = setStudentSearch;
window.renderCommsPage = renderCommsPage;
window.renderMissedLessons = renderMissedLessons;
window.logInvoice = logInvoice;
window.onLessStatChange = onLessStatChange;
window.supabase = window.SupabaseMini, window.__startTime = Date.now();

var ROLES = {
  god: {
    label: "Бог системи",
    icon: "⚡",
    color: "var(--god2)",
    avatarBg: "linear-gradient(135deg,#2e3192,#5b60d4)",
    nav: [ "dashboard", "students", "tutors", "schedule", "lessons", "comms", "missed", "payments", "reports", "crm", "invoice", "invoice-log", "comms", "missed", "users", "settings" ],
    can: {
      students: !0,
      tutors: !0,
      lessons: !0,
      payments: !0,
      users: !0,
      settings: !0,
      danger: !0,
      deleteAny: !0
    },
    seeIncome: !0,
    seeAll: !0,
    canEditUsers: !0,
    showGodBanner: !0
  },
  director: {
    label: "Директор",
    icon: "\uD83D\uDCDA",
    color: "var(--dir)",
    avatarBg: "linear-gradient(135deg,#d9e021,#fcee21)",
    nav: [ "dashboard", "students", "tutors", "schedule", "lessons", "comms", "missed", "payments", "reports", "crm", "invoice", "invoice-log", "comms", "missed", "users", "settings" ],
    can: {
      students: !0,
      tutors: !0,
      lessons: !0,
      payments: !0,
      users: !0,
      settings: !0,
      danger: !(window.onerror = function(msg, src, line, col, err) {
        var div;
        return "Script error." !== msg && "Script error" !== msg && ((div = document.createElement("div")).style.cssText = "position:fixed;top:0;left:0;right:0;background:#f8d7da;color:#721c24;padding:16px;font-family:monospace;font-size:13px;z-index:99999;border-bottom:2px solid #f5c6cb", 
        div.innerHTML = "<strong>JS Error at line " + line + ":</strong><br>" + msg + "<br><small>" + (err ? err.stack : "") + "</small>", 
        document.body ? document.body.appendChild(div) : document.addEventListener("DOMContentLoaded", function() {
          document.body.appendChild(div);
        })), !1;
      }),
      deleteAny: !0
    },
    seeIncome: !0,
    seeAll: !0,
    canEditUsers: !0,
    showGodBanner: !1
  },
  admin: {
    label: "Адміністратор",
    icon: "\uD83C\uDF10",
    color: "var(--adm)",
    avatarBg: "linear-gradient(135deg,#29abe2,#3fa9f5)",
    nav: [ "dashboard", "students", "tutors", "schedule", "lessons", "comms", "missed", "crm", "invoice" ],
    can: {
      students: !0,
      tutors: !0,
      lessons: !0,
      payments: !0,
      users: !1,
      settings: !0,
      danger: !1,
      deleteAny: !1
    },
    seeIncome: !0,
    seeAll: !0,
    canEditUsers: !1,
    showGodBanner: !1
  },
  network_admin: {
    label: "Адмін мережі",
    icon: "\uD83C\uDFE2",
    color: "var(--god2)",
    avatarBg: "linear-gradient(135deg,#5b60d4,#29abe2)",
    nav: [ "dashboard", "students", "tutors", "schedule", "lessons", "comms", "missed", "payments", "reports", "crm", "invoice", "invoice-log", "comms", "missed", "users", "settings" ],
    can: {
      students: !0,
      tutors: !0,
      lessons: !0,
      payments: !0,
      users: !0,
      settings: !0,
      danger: !1,
      deleteAny: !0
    },
    seeIncome: !0,
    seeAll: !0,
    canEditUsers: !0,
    showGodBanner: !1
  },
  tutor: {
    label: "Репетитор",
    icon: "\uD83D\uDEE1",
    color: "var(--tut)",
    avatarBg: "linear-gradient(135deg,#22b573,#7ac943)",
    nav: [ "dashboard", "students", "schedule", "lessons", "comms", "missed", "profile" ],
    can: {
      students: !0,
      tutors: !1,
      lessons: !0,
      payments: !1,
      users: !1,
      settings: !1,
      danger: !1,
      deleteAny: !1
    },
    seeIncome: !1,
    seeAll: !1,
    canEditUsers: !1,
    showGodBanner: !1
  }
}, NAV_CFG = [ {
  id: "dashboard",
  ico: "\u229E",
  lbl: "Дашборд",
  sec: "Головне"
}, {
  id: "students",
  ico: "\u25CE",
  lbl: "Учні",
  sec: "Головне",
  badge: !0
}, {
  id: "tutors",
  ico: "\u25C8",
  lbl: "Репетитори",
  sec: "Головне"
}, {
  id: "schedule",
  ico: "\u25A6",
  lbl: "Розклад",
  sec: "Навчання"
}, {
  id: "lessons",
  ico: "\u25C9",
  lbl: "Заняття",
  sec: "Навчання"
}, {
  id: "payments",
  ico: "\u25C8",
  lbl: "Оплата",
  sec: "Фінанси"
}, {
  id: "reports",
  ico: "\u25E7",
  lbl: "Аналітика",
  sec: "Фінанси"
}, {
  id: "analytics",
  ico: "\u25A4",
  lbl: "Статистика",
  sec: "Фінанси"
}, {
  id: "users",
  ico: "\u25CE",
  lbl: "Акаунти",
  sec: "Система"
}, {
  id: "branches",
  ico: "\uD83D\uDCCB",
  lbl: "Філії",
  sec: "Система"
}, {
  id: "settings",
  ico: "\u25C9",
  lbl: "Налаштування",
  sec: "Система"
}, {
  id: "crm",
  ico: "▤",
  lbl: "CRM",
  sec: "Менеджмент"
}, {
  id: "invoice",
  ico: "\u25A4",
  lbl: "Рахунок",
  sec: "Рахунок"
}, {
  id: "invoice-log",
  ico: "▤",
  lbl: "Історія",
  sec: "Рахунок"
}, {
  id: "comms",
  ico: "\u25A4",
  lbl: "Комунікації",
  sec: "Навчання"
}, {
  id: "missed",
  ico: "\u25A4",
  lbl: "Пропущені",
  sec: "Навчання"
}, {
  id: "profile",
  ico: "\u25A3",
  lbl: "Мій профіль",
  sec: "Особисте"
} ], DEFAULT_NAV_CFG = NAV_CFG, PLABELS = {
  dashboard: "Дашборд",
  students: "Учні",
  tutors: "Репетитори",
  schedule: "Розклад",
  lessons: "Заняття",
  payments: "Оплата",
  reports: "Аналітика",
  users: "Акаунти",
  settings: "Налаштування",
  profile: "Мій профіль",
  crm: "CRM",
  invoice: "Рахунок",
  comms: "Комунікації",
  missed: "Пропущені уроки",
  "invoice-log": "Історія рахунків",
  analytics: "Статистика"
}, UA_PERMS = [ {
  k: "students",
  lbl: "Учні — перегляд і редагування"
}, {
  k: "tutors",
  lbl: "Викладачі — редагування"
}, {
  k: "lessons",
  lbl: "Заняття — редагування"
}, {
  k: "payments",
  lbl: "Оплата — перегляд і редагування"
}, {
  k: "users",
  lbl: "Акаунти — управління"
}, {
  k: "settings",
  lbl: "Налаштування центру"
}, {
  k: "danger",
  lbl: "Небезпечна зона"
}, {
  k: "seeIncome",
  lbl: "Бачити фінанси та доходи"
}, {
  k: "seeAll",
  lbl: "Бачити всі записи (не тільки свої)"
}, {
  k: "deleteAny",
  lbl: "Видаляти будь-які записи"
} ], UA_PAGES = [ {
  id: "dashboard",
  ico: "⊞",
  lbl: "Дашборд",
  sec: "Головне"
}, {
  id: "students",
  ico: "◎",
  lbl: "Учні",
  sec: "Головне"
}, {
  id: "tutors",
  ico: "◈",
  lbl: "Репетитори",
  sec: "Головне"
}, {
  id: "schedule",
  ico: "▦",
  lbl: "Розклад",
  sec: "Навчання"
}, {
  id: "lessons",
  ico: "◉",
  lbl: "Заняття",
  sec: "Навчання"
}, {
  id: "payments",
  ico: "◈",
  lbl: "Оплата",
  sec: "Фінанси"
}, {
  id: "reports",
  ico: "◧",
  lbl: "Аналітика",
  sec: "Фінанси"
}, {
  id: "users",
  ico: "◎",
  lbl: "Акаунти",
  sec: "Система"
}, {
  id: "settings",
  ico: "◉",
  lbl: "Налаштування",
  sec: "Система"
}, {
  id: "profile",
  ico: "▣",
  lbl: "Мій профіль",
  sec: "Особисте"
} ], PERM_LABELS = {
  students: "Учні — перегляд і редагування",
  tutors: "Викладачі — редагування",
  lessons: "Заняття — редагування",
  payments: "Оплата — перегляд і редагування",
  users: "Акаунти — управління",
  settings: "Налаштування центру",
  danger: "Небезпечна зона (скидання даних)",
  seeIncome: "Бачити фінанси та доходи",
  seeAll: "Бачити всі записи (не тільки свої)"
}, COMM_TYPES = {
  call: {
    ico: "\uD83D\uDCDE",
    label: "Дзвінок",
    color: "#29abe2"
  },
  msg: {
    ico: "\uD83D\uDCAC",
    label: "Повідомлення",
    color: "#22b573"
  },
  meeting: {
    ico: "\uD83D\uDCAC",
    label: "Зустріч",
    color: "#d9e021"
  },
  email: {
    ico: "὎7",
    label: "Email",
    color: "#a78bfa"
  },
  other: {
    ico: "\uD83E\uDD1D",
    label: "Інше",
    color: "#7a8aaa"
  }
}, DEFAULT_PERMS = {
  god: {
    students: !0,
    tutors: !0,
    lessons: !0,
    payments: !0,
    users: !0,
    settings: !0,
    danger: !0,
    seeIncome: !0,
    seeAll: !0
  },
  director: {
    students: !0,
    tutors: !0,
    lessons: !0,
    payments: !0,
    users: !0,
    settings: !0,
    danger: !1,
    seeIncome: !0,
    seeAll: !0
  },
  admin: {
    students: !0,
    tutors: !1,
    lessons: !0,
    payments: !0,
    users: !1,
    settings: !1,
    danger: !1,
    seeIncome: !0,
    seeAll: !0
  },
  tutor: {
    students: !1,
    tutors: !1,
    lessons: !0,
    payments: !1,
    users: !1,
    settings: !1,
    danger: !1,
    seeIncome: !1,
    seeAll: !1
  }
}, RIGHTS_MATRIX = [ [ "Функція", "⚡ Бог", "ὅ1 Директор", "Ὦ1️ Адмін", "ὍA Репетитор" ], [ "Перегляд усіх учнів", "✅", "✅", "✅", "✅ Своїх" ], [ "Редагування учнів", "✅", "✅", "✅", "❌" ], [ "Перегляд викладачів", "✅", "✅", "✅ огляд", "❌" ], [ "Редагування викладачів", "✅", "✅", "❌", "❌" ], [ "Розклад — всі", "✅", "✅", "✅", "✅ Свій" ], [ "Заняття — редагування", "✅", "✅", "✅", "✅ Свої" ], [ "Фінанси / оплати", "✅", "✅", "✅", "❌" ], [ "Аналітика — доходи", "✅", "✅", "✅", "❌" ], [ "Управління акаунтами", "✅", "✅", "❌", "❌" ], [ "Налаштування центру", "✅", "✅", "❌", "❌" ], [ "Небезпечна зона", "✅", "❌", "❌", "❌" ], [ 'Роль "Бог" іншим', "✅", "❌", "❌", "❌" ], [ "Скидання всіх даних", "✅", "❌", "❌", "❌" ] ];

function R() {
  return CU?.role || "tutor";
}

function P() {
  return ROLES[R()];
}

function userPerms() {
  var up, rp;
  return CU ? (up = CU.perms || {}, rp = P().can || {}, Object.assign({}, rp, up.can || {})) : {};
}

function userNav() {
  var roleNav, hide, up, nav;
  return CU ? (up = CU.perms || {}, roleNav = ROLES[R()].nav || [], hide = up.hideNav || [], 
  up = up.showNav || [], nav = roleNav.filter(function(p) {
    return !hide.includes(p);
  }), up.forEach(function(p) {
    nav.includes(p) || nav.push(p);
  }), nav) : [];
}

function can(k) {
  return userPerms()[k] || !1;
}

function isSuperAdmin() {
  return "god" === R() || "network_admin" === R();
}

function currentBranch() {
  return S.currentBranchId || null;
}

function branchName(id) {
  var b = (S.branches || []).find(x => x.id === id);
  return b ? b.name : "—";
}

function filterByBranch(arr) {
  var bid = currentBranch();
  if (!bid && isSuperAdmin()) return arr;
  let activeBid = bid || myBranchId();
  return activeBid ? (arr || []).filter(x => !x.branchId || x.branchId === activeBid) : arr;
}

function myBranchId() {
  return isSuperAdmin() ? currentBranch() : CU?.branchId || S.branches[0]?.id;
}

function mkAv(fn, ln, sz = 30) {
  var cs = [ "#6c8fff", "#a78bfa", "#34d399", "#f59e0b", "#f87171", "#0ea5e9", "#ec4899", "#ff6b35" ];
  return '<div class="av" style="background:' + cs[((fn || "A").charCodeAt(0) + (ln || "B").charCodeAt(0)) % cs.length] + ";width:" + sz + "px;height:" + sz + "px;font-size:" + .38 * sz + 'px;color:#fff">' + (fn || "?")[0] + ((ln || "")[0] || "") + "</div>";
}

function bst(s) {
  return '<span class="badge ' + ({
    active: "bg",
    trial: "bb",
    paused: "by",
    completed: "br",
    planned: "bb",
    done: "bg",
    cancelled: "br",
    missed: "br",
    makeup: "by",
    paid: "bg",
    pending: "by",
    overdue: "br"
  }[s] || "bb") + '">' + ({
    active: "Активний",
    trial: "Пробне",
    paused: "Призупин.",
    completed: "Завершив",
    planned: "Планов.",
    done: "Проведено",
    cancelled: "Скасов.",
    missed: "Пропущено",
    makeup: "Відпрацювання",
    paid: "Оплачено",
    pending: "Очікується",
    overdue: "Прострочено"
  }[s] || s) + "</span>";
}

function localDateStr(date) {
  return date ? (date = date instanceof Date ? date : new Date(date)).getFullYear() + "-" + String(date.getMonth() + 1).padStart(2, "0") + "-" + String(date.getDate()).padStart(2, "0") : "";
}

function fd(d) {
  return d ? new Date(d).toLocaleDateString("uk-UA", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric"
  }) : "—";
}

function fd2(l, p) {
  document.getElementById("lu").value = l, document.getElementById("lp").value = p;
}

function sn(id) {
  var s = S.students.find(x => x.id === id);
  return s ? s.fn + " " + s.ln : "—";
}

function tn(id) {
  var t = S.tutors.find(x => x.id === id);
  return t ? t.fn + " " + t.ln : "—";
}

function mkToast(msg, type = "success") {
  let e = document.createElement("div");
  e.className = "toast " + type, e.innerHTML = "<span>" + ("success" === type ? "✅" : "❌") + "</span> " + msg, 
  document.body.appendChild(e), setTimeout(() => e.remove(), 3e3);
}

function popSel(id, arr, valKey, lblFn, placeholder = "—") {
  var cur;
  (id = document.getElementById(id)) && (cur = id.value, id.innerHTML = '<option value="">' + placeholder + "</option>" + arr.map(x => '<option value="' + x[valKey] + '">' + lblFn(x) + "</option>").join(""), 
  id.value = cur);
}

function openM(id) {
  (id = document.getElementById(id)) && (id.style.display = "flex", id.style.pointerEvents = "all", 
  id.classList.add("open"));
}

function closeM(id) {
  (id = document.getElementById(id)) && (id.style.display = "none", id.classList.remove("open"), 
  id.style.pointerEvents = "none"), S.editId = null, document.querySelectorAll(".mo").forEach(function(m) {
    m.classList.contains("open") || "none" !== m.style.display || (m.style.pointerEvents = "none");
  });
}

function toggleSidebar() {
  var sb = document.querySelector(".sb"), ov = document.getElementById("sb-overlay");
  sb && (sb.classList.toggle("open"), ov) && ov.classList.toggle("open", sb.classList.contains("open"));
}

function closeSidebar() {
  var sb = document.querySelector(".sb"), ov = document.getElementById("sb-overlay");
  sb && sb.classList.remove("open"), ov && ov.classList.remove("open");
}

function myLessons() {
  var all = filterByBranch(S.lessons);
  if (P().seeAll) return all;
  let mt = S.tutors.find(t => t.accId === CU?.id);
  return mt ? all.filter(l => l.tutorId === mt.id) : [];
}

function myStudents() {
  var all = S.students || [];
  try {
    if (P().seeAll) return all;
  } catch (e) {
    return all;
  }
  var all = filterByBranch(all), cuId = CU ? CU.id : null, mt = S.tutors.find(function(t) {
    return t.accId === cuId || t.acc_uid === cuId;
  });
  return mt ? all.filter(function(s) {
    return s.tutorId === mt.id || s.tutor_id === mt.id;
  }) : all;
}

function myTutor() {
  return S.tutors.find(t => t.accId === CU?.id) || null;
}

function calcPrice(subjectName, tutorId, grade, dur) {
  var subj, rules = S.pricingRules || [];
  if (!rules.length) return (subj = (S.subjects || []).find(s => s.name === subjectName)) && subj.price ? parseFloat(subj.price) : 0;
  let best = null, bestScore = -1;
  return rules.forEach(function(r) {
    var score, match, g;
    r.price && (match = !(score = 0), r.subjectMatch && (subjectName && subjectName.toLowerCase().includes(r.subjectMatch.toLowerCase()) ? score += 3 : match = !1), 
    r.tutorId && (r.tutorId === tutorId ? score += 2 : match = !1), r.gradeMatch && ((g = String(grade || "")) && g.toLowerCase().includes(r.gradeMatch.toLowerCase()) ? score += 1 : match = !1), 
    r.durMin && (parseInt(dur || 60) >= parseInt(r.durMin) ? score += 1 : match = !1), 
    match) && score >= bestScore && (bestScore = score, best = r);
  }), best ? parseFloat(best.price) : 0;
}

function autoFillPrice() {
  let subj = document.getElementById("l-subj")?.value || "", tutorId = document.getElementById("l-tutor")?.value || "", dur = document.getElementById("l-dur")?.value || 60, stId = document.getElementById("l-std")?.value || "", price = calcPrice(subj, tutorId, (S.students || []).find(s => s.id === stId)?.grade || "", dur), pEl = document.getElementById("l-price");
  pEl && price && (pEl.value = price), price ? mkToast("Ціну підібрано: " + price + " ₴", "info") : mkToast("Правило ціни не знайдено", "error");
}

function getWeekRange(offset) {
  function fmt(d) {
    return d.toLocaleDateString("uk-UA", {
      day: "numeric",
      month: "short"
    });
  }
  var now = new Date(), day = 0 === now.getDay() ? 6 : now.getDay() - 1, mon = new Date(now);
  return mon.setDate(now.getDate() - day + 7 * offset), mon.setHours(0, 0, 0, 0), 
  (now = new Date(mon)).setDate(mon.getDate() + 6), now.setHours(23, 59, 59, 999), 
  {
    mon: mon,
    sun: now,
    label: fmt(mon) + " — " + fmt(now)
  };
}

function inWeek(dateStr, wr) {
  return !!dateStr && (dateStr = new Date(dateStr + "T12:00:00")) >= wr.mon && dateStr <= wr.sun;
}

function dashKpiWeek(dir) {
  S.dashWeekOffset = 0 === dir ? 0 : (S.dashWeekOffset || 0) + dir, renderDashKpi(), 
  renderDashTrends();
}

function renderDash() {
  try {
    renderDashStats();
  } catch (e) {}
  try {
    renderDashKpi();
  } catch (e) {}
  try {
    renderDashTrends();
  } catch (e) {
    showErr("renderDashTrends: " + e.message);
  }
  try {
    renderCommLog();
  } catch (e) {}
  try {
    renderDashBottom();
  } catch (e) {}
}

function showErr(msg) {
  var d = document.getElementById("debug-err") || document.createElement("div");
  d.id = "debug-err", d.style.cssText = "position:fixed;bottom:10px;right:10px;background:#f8d7da;color:#721c24;padding:12px 16px;border-radius:8px;font-size:12px;font-family:monospace;z-index:9999;max-width:400px;word-break:break-all", 
  d.textContent = msg, document.body.appendChild(d);
}

function renderDashStats() {
  var _mt2, _cnt, ml = myLessons(), ms = myStudents(), now = new Date(), ml = ml.filter(function(l) {
    return (l = new Date(l.date)).getMonth() === now.getMonth() && l.getFullYear() === now.getFullYear();
  }), nb = ((nb = document.getElementById("nb-s")) && (_cnt = "tutor" === R() ? (_mt2 = S.tutors ? S.tutors.find(function(t) {
    return CU && (t.accId === CU.id || t.acc_uid === CU.id);
  }) : null) ? (S.students || []).filter(function(s) {
    return (s.tutorId === _mt2.id || s.tutor_id === _mt2.id) && "active" === s.status;
  }).length : 0 : myStudents().filter(function(s) {
    return "active" === s.status;
  }).length, nb.textContent = _cnt), '<div class="sc blue"><div class="slbl">Активних учнів</div><div class="sval">' + ms.filter(function(s) {
    return "active" === s.status;
  }).length + '</div><div class="ssub">Загалом: ' + ms.length + '</div><span class="sico">◎</span></div><div class="sc green"><div class="slbl">Занять цього місяця</div><div class="sval">' + ml.length + '</div><div class="ssub">Проведено: ' + ml.filter(function(l) {
    return "done" === l.status || "completed" === l.status;
  }).length + '</div><span class="sico">◉</span></div>');
  P().seeIncome && "tutor" !== R() && "admin" !== R() ? nb += '<div class="sc yellow"><div class="slbl">Дохід цього місяця</div><div class="sval">' + S.payments.filter(function(p) {
    var d = new Date(p.date);
    return "paid" === p.status && d.getMonth() === now.getMonth() && d.getFullYear() === now.getFullYear();
  }).reduce(function(a, p) {
    return a + p.amount;
  }, 0).toLocaleString("uk-UA") + '₴</div><div class="ssub">Отримано</div><span class="sico">◈</span></div>' : nb += '<div class="sc yellow" style="opacity:.4"><div class="slbl">Дохід місяця</div><div class="sval">ὑ2</div><div class="ssub">Тільки директор</div><span class="sico">◈</span></div>', 
  "tutor" !== R() && (nb += '<div class="sc red"><div class="slbl">Репетиторів</div><div class="sval">' + S.tutors.length + '</div><div class="ssub">Активних</div><span class="sico">◈</span></div>'), 
  document.getElementById("dash-stats").innerHTML = nb;
}

function renderDashKpi() {
  var wr = getWeekRange(offset = S.dashWeekOffset || 0);
  (lbl = document.getElementById("dash-week-lbl")) && (lbl.textContent = wr.label);
  (lbl = document.getElementById("dash-tutor-week-lbl")) && (lbl.textContent = wr.label);
  var lbl = "tutor" === R() ? myLessons() : S.lessons, _selfTutorRec = "tutor" === R() ? (S.tutors || []).find(function(t) {
    return CU && (t.accId === CU.id || t.acc_uid === CU.id);
  }) : null, weekL = lbl.filter(function(l) {
    return inWeek(l.date, wr);
  }), weekComms = (S.comms || []).filter(function(c) {
    return !(!inWeek(c.date, wr) || _selfTutorRec && (c.tutorId || c.tutor_id) !== _selfTutorRec.id);
  }), done = weekL.filter(function(l) {
    return "done" === l.status || "completed" === l.status;
  }).length, missed = weekL.filter(function(l) {
    return "missed" === l.status || "absent" === l.status;
  }).length, cancelled = (weekL.filter(function(l) {
    return "makeup" === l.status;
  }).length, weekL.filter(function(l) {
    return "cancelled" === l.status;
  }).length), planned = weekL.filter(function(l) {
    return "planned" === l.status || "scheduled" === l.status;
  }).length, totalComms = weekComms.length, total = weekL.length, pct = 0 < total ? Math.round(done / total * 100) : 0, wrPrev = getWeekRange(offset - 1), prevL = lbl.filter(function(l) {
    return inWeek(l.date, wrPrev);
  }), offset = prevL.filter(function(l) {
    return "done" === l.status || "completed" === l.status;
  }).length, lbl = prevL.filter(function(l) {
    return "missed" === l.status || "absent" === l.status;
  }).length, prevComms = (S.comms || []).filter(function(c) {
    return inWeek(c.date, wrPrev);
  }).length, prevPct = 0 < prevL.length ? Math.round(offset / prevL.length * 100) : 0;
  function trend(cur, prev) {
    return 0 === prev && 0 === cur ? {
      cls: "same",
      txt: "— 0"
    } : 0 === prev ? {
      cls: "up",
      txt: "↑ новий"
    } : 0 < (prev = cur - prev) ? {
      cls: "up",
      txt: "↑ +" + prev
    } : prev < 0 ? {
      cls: "down",
      txt: "↓ " + prev
    } : {
      cls: "same",
      txt: "= " + cur
    };
  }
  var maxDone, totalDone, totalMissed, totalPlanned, totalTutComms, totalStudents, rowsArr, planned = [ {
    ico: "✅",
    val: done,
    lbl: "Проведено занять",
    sub: planned + " ще заплановано",
    accent: "var(--tut)",
    tr: trend(done, offset)
  }, {
    ico: "❌",
    val: missed,
    lbl: "Пропущено учнями",
    sub: "Скасовано: " + cancelled,
    accent: "var(--danger)",
    tr: trend(missed, lbl)
  }, {
    ico: "\uD83D\uDCAC",
    val: totalComms,
    lbl: "Комунікацій",
    sub: "Дзвінки та повідомлення",
    accent: "var(--adm)",
    tr: trend(totalComms, prevComms)
  }, {
    ico: "\uD83D\uDCC8",
    val: pct + "%",
    lbl: "Виконання плану",
    sub: done + " з " + total + " занять",
    accent: "#a78bfa",
    tr: trend(pct, prevPct)
  } ];
  (offset = document.getElementById("dash-week-kpi")) && (offset.innerHTML = planned.map(function(k) {
    return '<div class="kpi-card" style="--kpi-accent:' + k.accent + '"><div class="kpi-ico">' + k.ico + '</div><div class="kpi-val">' + k.val + '</div><div class="kpi-lbl">' + k.lbl + '</div><div class="kpi-sub">' + k.sub + '</div><div class="kpi-badge ' + k.tr.cls + '">' + k.tr.txt + "</div></div>";
  }).join("")), (cancelled = document.getElementById("dash-tutor-kpi")) && ((missed = "tutor" === R() ? S.tutors.filter(function(t) {
    return CU && (t.accId === CU.id || t.acc_uid === CU.id);
  }) : S.tutors).length ? (maxDone = Math.max.apply(null, missed.map(function(t) {
    return weekL.filter(function(l) {
      return l.tutorId === t.id && ("done" === l.status || "completed" === l.status);
    }).length;
  }).concat([ 1 ])), totalStudents = totalTutComms = totalPlanned = totalMissed = totalDone = 0, 
  rowsArr = [], missed.forEach(function(t) {
    var tDone = (tl = weekL.filter(function(l) {
      return l.tutorId === t.id;
    })).filter(function(l) {
      return "done" === l.status || "completed" === l.status;
    }).length, tMissed = tl.filter(function(l) {
      return "missed" === l.status || "absent" === l.status;
    }).length, tl = tl.filter(function(l) {
      return "planned" === l.status || "scheduled" === l.status;
    }).length, tComms = weekComms.filter(function(c) {
      return c.tutorId === t.id;
    }).length, tStudents = S.students.filter(function(s) {
      return s.tutorId === t.id && "active" === s.status;
    }).length, tTotal = 0 < (tTotal = tDone + tMissed) ? Math.round(tDone / tTotal * 100) : 0 < tl ? 0 : 100, barW = 0 < maxDone ? Math.round(tDone / maxDone * 100) : 0, pctColor = 80 <= tTotal ? "var(--tut)" : 50 <= tTotal ? "var(--dir)" : "var(--danger)", prevTDone = (totalDone += tDone, 
    totalMissed += tMissed, totalPlanned += tl, totalTutComms += tComms, totalStudents += tStudents, 
    prevL.filter(function(l) {
      return l.tutorId === t.id;
    }).filter(function(l) {
      return "done" === l.status || "completed" === l.status;
    }).length), trendTxt = "", trendCls = "same", dd = (0 < (dd = tDone - prevTDone) ? (trendTxt = "↑+" + dd, 
    trendCls = "up") : dd < 0 ? (trendTxt = "↓" + dd, trendCls = "down") : 0 < prevTDone && (trendTxt = "=" + tDone, 
    trendCls = "same"), '<tr><td><div style="display:flex;align-items:center;gap:8px">' + mkAv(t.fn, t.ln, 28) + '<div><div style="font-weight:600;font-size:13px">' + t.fn + " " + t.ln + '</div><div style="font-size:10px;color:var(--t3)">' + (t.subj || "—") + '</div></div></div></td><td><div style="display:flex;align-items:center;gap:8px"><span style="font-weight:700;font-size:18px;font-family:Syne,sans-serif;color:var(--tut)">' + tDone + "</span>" + (trendTxt ? '<span class="kpi-badge ' + trendCls + '" style="font-size:9px">' + trendTxt + "</span>" : "") + '</div><div class="mini-bar"><div class="mini-fill" style="width:' + barW + '%;background:var(--tut)"></div></div></td><td style="text-align:center"><span style="font-weight:600;font-size:15px;color:var(--t2)">' + tl + '</span></td><td style="text-align:center"><span style="font-weight:700;font-size:16px;color:' + (0 < tMissed ? "var(--danger)" : "var(--t3)") + '">' + tMissed + '</span></td><td><div style="display:flex;align-items:center;gap:6px;justify-content:center"><span style="font-weight:700;font-size:16px;color:var(--adm)">' + tComms + '</span></div></td><td style="text-align:center"><span style="font-size:14px">' + tStudents + '</span></td><td><div style="font-weight:700;font-size:15px;color:' + pctColor + '">' + tTotal + '%</div><div style="font-size:10px;color:var(--t3)">' + tDone + " / " + (tDone + tMissed) + "</div></td></tr>");
    rowsArr.push(dd);
  }), lbl = rowsArr.join(""), done = 80 <= (prevComms = 0 < (totalComms = totalDone + totalMissed) ? Math.round(totalDone / totalComms * 100) : 0) ? "var(--tut)" : 50 <= prevComms ? "var(--dir)" : "var(--danger)", 
  "tutor" !== R() && (lbl += '<tr style="background:rgba(255,255,255,.03);font-weight:700;border-top:2px solid var(--b1)"><td><span style="font-size:12px;color:var(--t2);letter-spacing:.5px">РАЗОМ / СЕРЕДНЄ</span></td><td><span style="font-size:18px;font-family:Syne,sans-serif;color:var(--tut)">' + totalDone + '</span></td><td style="text-align:center;color:var(--t2)">' + totalPlanned + '</td><td style="text-align:center;color:' + (0 < totalMissed ? "var(--danger)" : "var(--t3)") + '">' + totalMissed + '</td><td style="text-align:center;color:var(--adm)">' + totalTutComms + '</td><td style="text-align:center">' + totalStudents + '</td><td><span style="font-weight:700;color:' + done + '">' + prevComms + "%</span></td></tr>"), 
  cancelled.innerHTML = lbl) : cancelled.innerHTML = '<tr><td colspan="8" class="empty" style="padding:20px">Немає репетиторів</td></tr>');
}

function renderDashTrends() {
  if (CU) {
    for (var _selfTR = "tutor" === R() ? (S.tutors || []).find(function(t) {
      return CU && (t.accId === CU.id || t.acc_uid === CU.id);
    }) : null, _trendLessons = _selfTR ? S.lessons.filter(function(l) {
      return (l.tutorId || l.tutor_id) === _selfTR.id;
    }) : S.lessons, _trendComms = _selfTR ? (S.comms || []).filter(function(c) {
      return (c.tutorId || c.tutor_id) === _selfTR.id;
    }) : S.comms || [], offset = S.dashWeekOffset || 0, weeks = [], i = 3; 0 <= i; i--) {
      var wr = getWeekRange(offset - i), weekL = _trendLessons.filter(function(l) {
        return inWeek(l.date, wr);
      }), weekComms = _trendComms.filter(function(c) {
        return inWeek(c.date, wr);
      });
      weeks.push({
        wr: wr,
        done: weekL.filter(function(l) {
          return "done" === l.status || "completed" === l.status;
        }).length,
        missed: weekL.filter(function(l) {
          return "missed" === l.status || "absent" === l.status;
        }).length,
        planned: weekL.filter(function(l) {
          return "planned" === l.status || "scheduled" === l.status;
        }).length,
        comms: weekComms.length
      });
    }
    var tutors = "tutor" === R() ? S.tutors.filter(function(t) {
      return CU && t.accId === CU.id;
    }) : S.tutors;
    miniChart("dash-trend-lessons", weeks, "var(--tut)", function(w) {
      return w.done;
    }), miniChart("dash-trend-comms", weeks, "var(--adm)", function(w) {
      return w.comms;
    });
  }
  function miniChart(containerId, data, color, keyFn) {
    var max, barsHtml, tutorRows, el = document.getElementById(containerId);
    el && (max = Math.max.apply(null, data.map(keyFn).concat([ 1 ])), barsHtml = '<div class="trend-weeks">', 
    data.forEach(function(w, i) {
      var val = keyFn(w), pct = Math.round(val / max * 100), i = i === data.length - 1, fmt = {
        day: "2-digit",
        month: "2-digit"
      }, sun = new Date(w.wr.mon), w = (sun.setDate(sun.getDate() + 6), w.wr.mon.toLocaleDateString("uk-UA", fmt) + "–" + sun.toLocaleDateString("uk-UA", fmt));
      barsHtml += '<div class="trend-week' + (i ? " trend-week-now" : "") + '"><div class="trend-week-val">' + val + '</div><div class="trend-week-bar-wrap"><div class="trend-week-bar" style="width:' + pct + "%;background:" + color + '"></div></div><div class="trend-week-lbl">' + w + "</div></div>";
    }), barsHtml += "</div>", tutorRows = "", tutors.slice(0, 6).forEach(function(t) {
      var vals = data.map(function(w) {
        return ("dash-trend-comms" === containerId ? (S.comms || []).filter(function(c) {
          return inWeek(c.date, w.wr) && (c.tutor_id === t.id || c.tutorId === t.id);
        }) : S.lessons.filter(function(l) {
          return inWeek(l.date, w.wr) && (l.tutor_id === t.id || l.tutorId === t.id) && ("done" === l.status || "completed" === l.status);
        })).length;
      }), tMax = Math.max.apply(null, vals.concat([ 1 ])), total = vals[vals.length - 1];
      tutorRows += '<div class="trend-tutor-row">' + mkAv(t.fn, t.ln, 26) + '<div class="trend-tutor-name">' + t.fn + " " + t.ln + '</div><div class="trend-tutor-bars">' + vals.map(function(v, i) {
        return '<div class="trend-tutor-col"><div class="trend-tutor-num">' + v + '</div><div class="trend-tutor-bar-wrap"><div class="trend-tutor-bar" style="width:' + Math.round(v / tMax * 100) + "%;background:" + (i === vals.length - 1 ? color : "var(--b2)") + '"></div></div></div>';
      }).join("") + '</div><div class="trend-tutor-total">' + total + "</div></div>";
    }), el.innerHTML = barsHtml + (tutorRows ? '<div class="trend-tutor-list">' + tutorRows + "</div>" : ""));
  }
}

function renderDashBottom() {
  var now = new Date(), ml = myLessons(), up = [].concat(ml).filter(function(l) {
    return new Date(l.date + "T" + (l.time || "00:00")) >= now && "cancelled" !== l.status;
  }).sort(function(a, b) {
    return new Date(a.date + "T" + a.time) - new Date(b.date + "T" + b.time);
  }).slice(0, 6), up = (document.getElementById("dt-lessons").innerHTML = up.length ? up.map(function(l) {
    return "<tr><td>" + sn(l.studentId) + "</td><td>" + l.subject + '</td><td style="font-family:JetBrains Mono,monospace;font-size:11px">' + fd(l.date) + " " + (l.time || "") + "</td><td>" + bst(l.status) + "</td></tr>";
  }).join("") : '<tr><td colspan="4" class="empty" style="padding:20px">Занять не заплановано</td></tr>', 
  document.getElementById("dash-rt")), rb = document.getElementById("dash-rb"), sc = (P().seeIncome && "tutor" !== R() && "admin" !== R() ? (up && (up.textContent = "Останні платежі"), 
  rec = [].concat(S.payments).sort(function(a, b) {
    return new Date(b.date) - new Date(a.date);
  }).slice(0, 6), rb && (rb.innerHTML = rec.length ? "<table><thead><tr><th>Учень</th><th>Сума</th><th>Дата</th><th>Статус</th></tr></thead><tbody>" + rec.map(function(p) {
    return "<tr><td>" + sn(p.studentId) + '</td><td style="font-family:JetBrains Mono,monospace">' + (p.amount || 0).toLocaleString("uk-UA") + '₴</td><td style="font-size:11px">' + fd(p.date) + "</td><td>" + bst(p.status) + "</td></tr>";
  }).join("") + "</tbody></table>" : '<div class="empty" style="padding:20px"><div class="ei">Ὃ3</div>Платежів немає</div>')) : (up && (up.textContent = "tutor" === R() ? "Мої учні" : "Учні"), 
  rec = myStudents(), rb && (rb.innerHTML = rec.length ? "<table><thead><tr><th>Ім'я</th><th>Предмет</th><th>Статус</th></tr></thead><tbody>" + rec.map(function(s) {
    return "<tr><td>" + s.fn + " " + s.ln + "</td><td>" + (s.subject || "—") + "</td><td>" + bst(s.status) + "</td></tr>";
  }).join("") + "</tbody></table>" : '<div class="empty" style="padding:20px"><div class="ei">὆5</div>Учнів не призначено</div>')), 
  {}), colors = (ml.forEach(function(l) {
    sc[l.subject] = (sc[l.subject] || 0) + 1;
  }), [ "var(--adm)", "var(--tut)", "var(--dir)", "var(--god2)", "#a78bfa", "#0ea5e9" ]), maxS = Math.max.apply(null, Object.values(sc).concat([ 1 ])), up = (document.getElementById("dash-subj").innerHTML = Object.entries(sc).sort(function(a, b) {
    return b[1] - a[1];
  }).slice(0, 6).map(function(e, i) {
    return '<div style="margin-bottom:9px"><div style="display:flex;justify-content:space-between;margin-bottom:3px"><span style="font-size:12px">' + e[0] + '</span><span style="font-size:11px;color:var(--t2);font-family:JetBrains Mono,monospace">' + e[1] + '</span></div><div class="pb"><div class="pf" style="width:' + Math.round(e[1] / maxS * 100) + "%;background:" + colors[i % colors.length] + '"></div></div></div>';
  }).join("") || '<div class="empty"><div class="ei">ὌA</div>Немає даних</div>', 
  S.payments.filter(function(p) {
    return "paid" === p.status;
  }).reduce(function(a, p) {
    return a + p.amount;
  }, 0)), rb = S.payments.filter(function(p) {
    return "pending" === p.status;
  }).reduce(function(a, p) {
    return a + p.amount;
  }, 0), rec = S.payments.filter(function(p) {
    return "overdue" === p.status;
  }).reduce(function(a, p) {
    return a + p.amount;
  }, 0), ml = (document.getElementById("dash-pay").innerHTML = P().seeIncome && "tutor" !== R() && "admin" !== R() ? '<div class="ms"><span class="msl">✅ Оплачено</span><span class="msv" style="color:var(--tut)">' + up.toLocaleString("uk-UA") + '₴</span></div><div class="ms"><span class="msl">⏳ Очікується</span><span class="msv" style="color:var(--dir)">' + rb.toLocaleString("uk-UA") + '₴</span></div><div class="ms"><span class="msl">⚠️ Прострочено</span><span class="msv" style="color:var(--danger)">' + rec.toLocaleString("uk-UA") + '₴</span></div><div class="ms"><span class="msl">Всього платежів</span><span class="msv">' + S.payments.length + "</span></div>" : "tutor" === R() || "admin" === R() ? "" : '<div class="empty"><div class="ei">ὑ2</div>Доступнь', 
  document.getElementById("dash-rb-card")), up = document.getElementById("dash-pay-card"), rb = "tutor" === R() || "admin" === R();
  ml && (ml.style.display = rb ? "none" : ""), up && (up.style.display = rb ? "none" : ""), 
  (rec = document.getElementById("dash-comm-block")) && (rec.style.display = "none"), 
  !(ml = document.getElementById("dash-pay")) || "tutor" !== R() && "admin" !== R() || ml.closest(".card") && (ml.closest(".card").style.display = "none");
}

function renderCommLog() {
  var typeIco, comms, el = document.getElementById("dash-comm-log"), el2 = document.getElementById("dash-comm-log2");
  (el || el2) && (comms = [].concat(S.comms || []).sort(function(a, b) {
    return (b.date || "").localeCompare(a.date || "");
  }).slice(0, 20), typeIco = {
    call: "\uD83D\uDCDE",
    message: "\uD83D\uDCAC",
    meeting: "\uD83D\uDCAC",
    email: "὎7",
    other: "\uD83E\uDD1D",
    msg: "\uD83D\uDCAC",
    meet: "\uD83D\uDCAC"
  }, comms = comms.length ? comms.map(function(c) {
    var tutor = S.tutors.find(function(t) {
      return t.id === c.tutorId;
    }) || {
      fn: "",
      ln: ""
    }, student = c.studentId ? S.students.find(function(s) {
      return s.id === c.studentId;
    }) : null;
    return '<div class="comm-item"><div class="comm-ico">' + (typeIco[c.type] || "\uD83E\uDD1D") + '</div><div class="comm-body"><div class="comm-meta"><span class="comm-type">' + (c.type || "інше") + '</span><span class="comm-tutor">' + tutor.fn + " " + tutor.ln + "</span>" + (student ? '<span style="font-size:11px;color:var(--t3)">→ ' + student.fn + " " + student.ln + "</span>" : "") + '<span class="comm-date">' + fd((c.date || "").slice(0, 10)) + '</span></div><div class="comm-note">' + (c.note || "—") + '</div></div><button onclick="delComm(this.dataset.id)" data-id="' + c.id + '" style="background:none;border:none;color:var(--t3);cursor:pointer;font-size:14px;flex-shrink:0">&times;</button></div>';
  }).join("") : '<div class="empty" style="padding:20px"><div class="ei">ὊC</div>Комунікацій ще не записано</div>', 
  el && (el.innerHTML = comms), el2) && (el2.innerHTML = comms);
}

function sfilt(f, el) {
  sfCur = f, document.querySelectorAll("#sfchips .chip").forEach(c => c.classList.remove("active")), 
  el.classList.add("active"), renderStudents();
}

function renderStudents() {
  var data = myStudents();
  "all" !== sfCur && (data = data.filter(function(s) {
    return s.status === sfCur;
  }));
  (tot = document.getElementById("st-total")) && (tot.textContent = data.length + " з " + myStudents().length);
  var ce = can("students"), tot = data.length ? data.map(function(s) {
    var btns = ce ? '<button class="btn btn-g btn-sm" onclick="openStudM(this.dataset.id)" data-id="' + s.id + '">✏️</button><button class="btn btn-sm" style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger)" onclick="delStudent(this.dataset.id)" data-id="' + s.id + '">Ὕ1</button>' : '<span style="font-size:10px;color:var(--t3)">перегляд</span>';
    return '<tr><td><div style="display:flex;align-items:center;gap:8px">' + mkAv(s.fn, s.ln) + '<div><div style="font-weight:600;font-size:13px">' + s.fn + " " + s.ln + '</div></div></div></td><td style="font-size:12px;color:var(--t2)">' + (s.age || "—") + " / " + (s.grade || "—") + "</td><td>" + (s.subject || "—") + "</td><td>" + (s.tutorId ? tn(s.tutorId) : "—") + "</td><td>" + bst(s.status) + '</td><td style="font-size:12px;color:var(--t2)">' + (s.parentPhone || s.phone || s.email || "—") + '</td><td><div style="display:flex;gap:3px">' + btns + "</div></td></tr>";
  }).join("") : '<tr><td colspan="7"><div class="empty"><div class="ei">὆5</div>Учнів не знайдено</div></td></tr>';
  document.getElementById("st-table").innerHTML = tot;
}

function renderLessons() {
  var sf = document.getElementById("lf-subj"), stf = document.getElementById("lf-stat"), cv = sf && sf.value || "", sv = stf && stf.value || "", sf = (sf && (stf = sf.value, 
  sf.innerHTML = '<option value="">Всі предмети</option>' + S.subjects.map(function(s) {
    return '<option value="' + s.name + '">' + s.name + "</option>";
  }).join(""), sf.value = stf), [].concat(myLessons()).sort(function(a, b) {
    return new Date(b.date + "T" + b.time) - new Date(a.date + "T" + a.time);
  })), ce = (cv && (sf = sf.filter(function(l) {
    return l.subject === cv;
  })), sv && (sf = sf.filter(function(l) {
    return l.status === sv;
  })), can("lessons")), stf = sf.length ? sf.map(function(l) {
    var btns = ce ? '<button class="btn btn-g btn-sm" onclick="openLessM(this.dataset.id)" data-id="' + l.id + '">✏️</button><button class="btn btn-sm" style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger)" onclick="delLesson(this.dataset.id)" data-id="' + l.id + '">Ὕ1</button>' : '<span style="font-size:10px;color:var(--t3)">перегляд</span>';
    return "<tr><td>" + sn(l.studentId) + "</td><td>" + l.subject + " " + (l.recurId ? '<span title="Повторюване" style="color:var(--adm);font-size:10px">ὐ1</span>' : "") + "</td><td>" + (l.tutorId ? tn(l.tutorId) : "—") + '</td><td style="font-family:JetBrains Mono,monospace;font-size:11px">' + fd(l.date) + " " + (l.time || "") + "</td><td>" + (l.dur || 60) + ' хв</td><td style="font-size:12px;color:var(--t2);max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + (l.notes || "—") + "</td><td>" + bst(l.status) + '</td><td><div style="display:flex;gap:3px">' + btns + "</div></td></tr>";
  }).join("") : '<tr><td colspan="8"><div class="empty"><div class="ei">ὍA</div>Занять немає</div></td></tr>';
  document.getElementById("lt-table").innerHTML = stf;
}

function renderPayments() {
  (invToolbar = document.getElementById("inv-toolbar")) && (invToolbar.style.display = "god" === R() || "director" === R() ? "block" : "none");
  var invToolbar = S.payments.filter(function(p) {
    return "paid" === p.status;
  }).reduce(function(a, p) {
    return a + p.amount;
  }, 0), pend = S.payments.filter(function(p) {
    return "pending" === p.status;
  }).reduce(function(a, p) {
    return a + p.amount;
  }, 0), over = S.payments.filter(function(p) {
    return "overdue" === p.status;
  }).reduce(function(a, p) {
    return a + p.amount;
  }, 0), mm = (document.getElementById("py-paid").textContent = invToolbar.toLocaleString("uk-UA") + "₴", 
  document.getElementById("py-pend").textContent = pend.toLocaleString("uk-UA") + "₴", 
  document.getElementById("py-over").textContent = over.toLocaleString("uk-UA") + "₴", 
  {
    cash: "Готівка",
    card: "Картка",
    transfer: "Переказ"
  }), ce = can("payments"), pend = (invToolbar = [].concat(S.payments).sort(function(a, b) {
    return new Date(b.date) - new Date(a.date);
  })).length ? invToolbar.map(function(p) {
    var btns = ce ? '<button class="btn btn-g btn-sm" onclick="openPayM(this.dataset.id)" data-id="' + p.id + '">✏️</button><button class="btn btn-sm" style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger)" onclick="delPay(this.dataset.id)" data-id="' + p.id + '">Ὕ1</button>' : "—";
    return "<tr><td>" + sn(p.studentId) + '</td><td style="font-family:JetBrains Mono,monospace">' + (p.amount || 0).toLocaleString("uk-UA") + "₴</td><td>" + (mm[p.method] || p.method) + '</td><td style="font-size:11px">' + fd(p.date) + '</td><td style="font-size:12px">' + (p.month || "—") + '</td><td style="font-size:12px;color:var(--t2)">' + (p.note || "—") + "</td><td>" + bst(p.status) + '</td><td><div style="display:flex;gap:3px">' + btns + "</div></td></tr>";
  }).join("") : '<tr><td colspan="8"><div class="empty"><div class="ei">Ὃ3</div>Платежів немає</div></td></tr>';
  document.getElementById("pt-table").innerHTML = pend;
}

function renderCustomPage(pageId) {
  var pel = document.getElementById("pg-" + pageId);
  pel || ((pel = document.createElement("div")).className = "page", pel.id = "pg-" + pageId, 
  document.getElementById("content").appendChild(pel));
  var pageInfo = ((cfg = S.godConfig || {}).navItems || [].concat(NAV_CFG)).find(function(n) {
    return n.id === pageId;
  }) || {
    lbl: "Власна сторінка",
    ico: "⭐"
  }, cfg = (cfg.customPageNotes || {})[pageId] || "", saveBtn = "god" === R() ? '<button class="btn btn-p btn-sm" style="margin-top:8px" onclick="saveCustomPageNotes(this.dataset.pid)" data-pid="' + pageId + '">ὋE Зберегти нотатки</button>' : "";
  pel.innerHTML = '<div class="god-banner" style="margin-bottom:16px"><span class="god-banner-icon">' + pageInfo.ico + '</span><div><div class="god-banner-title">' + pageInfo.lbl + '</div><div class="god-banner-text">Власна сторінка, створена через Конструктор інтерфейсу</div></div></div><div class="card"><div class="ch"><span class="ct">Вміст сторінки «' + pageInfo.lbl + '»</span></div><div style="padding:24px"><div style="margin-bottom:14px"><label style="font-size:11px;color:var(--t2);font-weight:600;text-transform:uppercase;letter-spacing:.4px">Нотатки / опис сторінки</label><textarea id="custom-page-notes-' + pageId + '" style="width:100%;margin-top:6px;min-height:120px;font-size:13px" placeholder="Додайте опис або нотатки...">' + cfg + "</textarea>" + saveBtn + '</div><div style="margin-top:20px;padding:16px;background:var(--s2);border-radius:10px;border:1px dashed var(--b2)"><div style="font-size:12px;color:var(--t3);text-align:center"><div style="font-size:24px;margin-bottom:8px">Ὢ7</div>Ця сторінка порожня.</div></div></div></div>', 
  pel.classList.add("active");
}

function saveCustomPageNotes(pageId) {
  var cfg = S.godConfig || {}, el = (cfg.customPageNotes || (cfg.customPageNotes = {}), 
  document.getElementById("custom-page-notes-" + pageId));
  cfg.customPageNotes[pageId] = el ? el.value : "", gcSave("customPageNotes", cfg.customPageNotes), 
  mkToast("Нотатки збережено");
}

function gcGetConfig() {
  return S.godConfig || (S.godConfig = {}), S.godConfig;
}

function gcSet(key, val) {
  S.godConfig || (S.godConfig = {}), null == val ? delete S.godConfig[key] : S.godConfig[key] = val, 
  S.comms && S.comms.length || (S.comms = [ {
    id: "cm1",
    tutorId: "t1",
    studentId: "s1",
    date: localDateStr(new Date()),
    type: "call",
    note: "Обговорили план занять на місяць",
    createdAt: new Date().toISOString()
  }, {
    id: "cm2",
    tutorId: "t2",
    studentId: "s4",
    date: localDateStr(new Date()),
    type: "msg",
    note: "Нагадування про домашнє завдання",
    createdAt: new Date().toISOString()
  }, {
    id: "cm3",
    tutorId: "t3",
    studentId: "s6",
    date: localDateStr(new Date()),
    type: "meeting",
    note: "Батьківські збори",
    createdAt: new Date().toISOString()
  } ]), saveS();
}

function gcTab(id, el) {
  document.querySelectorAll(".gc-tab").forEach(function(t) {
    t.classList.remove("active");
  }), document.querySelectorAll(".gc-panel").forEach(function(p) {
    p.classList.remove("active");
  }), el.classList.add("active"), document.getElementById("gcp-" + id).classList.add("active");
}

function renderConstructor() {
  "god" !== R() ? mkToast("Тільки Бог системи", "error") : (gcRenderRoles(), gcRenderNav(), 
  gcRenderFields(), gcRenderLabels());
}

function gcRenderRoles() {
  var sp = gcGetConfig().perms || {}, cont = document.getElementById("gc-role-editor"), grid = (cont.innerHTML = "", 
  document.createElement("div")), bar = ([ {
    role: "god",
    ico: "⚡",
    lbl: "Бог системи",
    clr: "var(--god2)",
    locked: !0
  }, {
    role: "director",
    ico: "\uD83D\uDCDA",
    lbl: "Директор",
    clr: "var(--dir)",
    locked: !(grid.className = "role-editor")
  }, {
    role: "admin",
    ico: "\uD83C\uDF10",
    lbl: "Адміністратор",
    clr: "var(--adm)",
    locked: !1
  }, {
    role: "tutor",
    ico: "\uD83D\uDEE1",
    lbl: "Репетитор",
    clr: "var(--tut)",
    locked: !1
  } ].forEach(function(ri) {
    var rp = Object.assign({}, DEFAULT_PERMS[ri.role], sp[ri.role] || {}), card = document.createElement("div"), hd = (card.className = "role-card", 
    document.createElement("div")), s1 = (hd.className = "role-card-head", document.createElement("span")), s2 = (s1.textContent = ri.ico, 
    document.createElement("span")), bd = (s2.style.cssText = "font-weight:700;font-size:13px;color:" + ri.clr, 
    s2.textContent = ri.lbl, hd.appendChild(s1), hd.appendChild(s2), ri.locked && ((s1 = document.createElement("span")).style.cssText = "font-size:10px;color:var(--t3);margin-left:auto", 
    s1.textContent = "незмінна", hd.appendChild(s1)), card.appendChild(hd), document.createElement("div"));
    bd.className = "role-card-body", Object.keys(PERM_LABELS).forEach(function(key) {
      var role, k, pr = document.createElement("div"), pl = (pr.className = "perm-row", 
      document.createElement("span")), tl = (pl.className = "perm-label", pl.textContent = PERM_LABELS[key], 
      document.createElement("label")), cb = (tl.className = "toggle", document.createElement("input"));
      cb.type = "checkbox", cb.checked = !!rp[key], ri.locked && (cb.disabled = !0), 
      role = ri.role, k = key, cb.addEventListener("change", function() {
        gcLivePermChange(role, k, this.checked);
      }), (key = document.createElement("span")).className = "toggle-slider", tl.appendChild(cb), 
      tl.appendChild(key), pr.appendChild(pl), pr.appendChild(tl), bd.appendChild(pr);
    }), card.appendChild(bd), grid.appendChild(card);
  }), cont.appendChild(grid), document.createElement("div"));
  bar.className = "gc-save-bar", bar.innerHTML = '<span style="font-size:12px;color:var(--tut)">&#10003; Зміни миттєві</span><button class="btn btn-g btn-sm" style="margin-left:auto" onclick="gcResetRoles()">&#8635; Скинути</button>', 
  cont.appendChild(bar);
}

function gcLivePermChange(role, key, val) {
  var cfg = gcGetConfig();
  cfg.perms || (cfg.perms = {}), cfg.perms[role] || (cfg.perms[role] = {}), cfg.perms[role][key] = val, 
  gcSet("perms", cfg.perms), ROLES[role] && (ROLES[role].can[key] = val, "seeIncome" === key && (ROLES[role].seeIncome = val), 
  "seeAll" === key) && (ROLES[role].seeAll = val), "dashboard" === S.currentPage && renderDash(), 
  mkToast(role + ": " + PERM_LABELS[key] + " → " + (val ? "✅" : "❌"));
}

function gcResetRoles() {
  confirm("Скинути всі права до стандартних?") && (gcSet("perms", null), [ "director", "admin", "tutor" ].forEach(function(role) {
    ROLES[role].can = Object.assign({}, DEFAULT_PERMS[role]), ROLES[role].seeIncome = DEFAULT_PERMS[role].seeIncome, 
    ROLES[role].seeAll = DEFAULT_PERMS[role].seeAll;
  }), gcRenderRoles(), mkToast("Права скинуто до стандартних"));
}

function gcGetNavItems() {
  var cfg = gcGetConfig();
  return cfg.navItems && cfg.navItems.length ? cfg.navItems : DEFAULT_NAV_CFG.map(function(n) {
    return Object.assign({}, n);
  });
}

function gcRenderNav() {
  var ni = gcGetNavItems(), rk = [ "god", "director", "admin", "tutor" ], rico = {
    god: "⚡",
    director: "\uD83D\uDCDA",
    admin: "\uD83C\uDF10",
    tutor: "\uD83D\uDEE1"
  }, el = document.getElementById("gc-nav-editor"), wrap = document.createElement("div");
  wrap.className = "nav-editor", ni.forEach(function(n, i) {
    var idx, del, rArr = n.roles || rk, row = document.createElement("div"), dh = (row.className = "nav-edit-row", 
    row.draggable = !0, idx = i, row.addEventListener("dragstart", function(e) {
      gcDragStart(e, idx);
    }), row.addEventListener("dragover", function(e) {
      gcDragOver(e, idx);
    }), row.addEventListener("drop", function(e) {
      gcDrop(e, idx);
    }), row.addEventListener("dragleave", function() {
      gcDragLeave();
    }), document.createElement("span")), icoI = (dh.className = "drag-handle", dh.innerHTML = "&#8283;", 
    row.appendChild(dh), document.createElement("input")), lblI = (icoI.type = "text", 
    icoI.value = n.ico || "", icoI.style.cssText = "width:44px;font-size:16px;text-align:center;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t1);padding:4px 6px", 
    (idx => {
      icoI.addEventListener("input", function() {
        gcLiveNavChange(idx, "ico", this.value);
      });
    })(i), row.appendChild(icoI), document.createElement("input")), secI = (lblI.type = "text", 
    lblI.value = n.lbl || "", lblI.style.cssText = "flex:1;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t1);padding:5px 8px;font-size:13px", 
    (idx => {
      lblI.addEventListener("input", function() {
        gcLiveNavChange(idx, "lbl", this.value);
      });
    })(i), row.appendChild(lblI), document.createElement("input")), cbWrap = (secI.type = "text", 
    secI.value = n.sec || "", secI.style.cssText = "width:95px;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t2);padding:5px 8px;font-size:11px", 
    (idx => {
      secI.addEventListener("input", function() {
        gcLiveNavChange(idx, "sec", this.value);
      });
    })(i), row.appendChild(secI), document.createElement("div"));
    cbWrap.className = "nav-vis-checkboxes", rk.forEach(function(r) {
      var idx, role, lbl = document.createElement("label"), cb = (lbl.className = "nav-vis-cb", 
      lbl.title = r, document.createElement("input")), ico = (cb.type = "checkbox", 
      cb.checked = rArr.includes(r), idx = i, role = r, cb.addEventListener("change", function() {
        gcLiveNavRole(idx, role, this.checked);
      }), document.createElement("span"));
      ico.textContent = rico[r] || r, lbl.appendChild(cb), lbl.appendChild(ico), 
      cbWrap.appendChild(lbl);
    }), row.appendChild(cbWrap), n.custom && ((del = document.createElement("button")).className = "btn btn-sm", 
    del.style.cssText = "background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger);padding:4px 8px", 
    del.innerHTML = "&times;", (idx => {
      del.addEventListener("click", function() {
        gcDelNavItem(idx);
      });
    })(i), row.appendChild(del)), wrap.appendChild(row);
  }), el.innerHTML = "", el.appendChild(wrap), (ni = document.createElement("div")).className = "gc-save-bar", 
  ni.innerHTML = '<span style="font-size:12px;color:var(--tut)">&#10003; Зміни миттєві</span><button class="btn btn-g btn-sm" style="margin-left:auto" onclick="gcResetNav()">&#8635; Скинути</button>', 
  el.appendChild(ni);
}

function gcEsc(str) {
  return (str || "").replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");
}

function gcLiveNavChange(idx, key, val) {
  var items = gcGetNavItems();
  items[idx][key] = val, gcSet("navItems", items), buildSidebar(), (idx = document.getElementById("ni-" + S.currentPage)) && idx.classList.add("active");
}

function gcLiveNavRole(idx, role, checked) {
  var items = gcGetNavItems();
  items[idx].roles || (items[idx].roles = [ "god", "director", "admin", "tutor" ]), 
  checked ? items[idx].roles.includes(role) || items[idx].roles.push(role) : items[idx].roles = items[idx].roles.filter(function(r) {
    return r !== role;
  }), gcSet("navItems", items), buildSidebar(), (checked = document.getElementById("ni-" + S.currentPage)) && checked.classList.add("active");
}

function gcDelNavItem(idx) {
  var items = gcGetNavItems();
  items[idx].custom ? (items.splice(idx, 1), gcSet("navItems", items), gcRenderNav(), 
  buildSidebar()) : mkToast("Системні вкладки не можна видаляти", "error");
}

function gcAddNavItem() {
  var sec, items, ico = document.getElementById("gc-new-ico").value.trim() || "⭐", lbl = document.getElementById("gc-new-lbl").value.trim();
  lbl ? (sec = document.getElementById("gc-new-sec").value.trim() || "Інше", (items = gcGetNavItems()).push({
    id: "custom_" + uid(),
    ico: ico,
    lbl: lbl,
    sec: sec,
    badge: !1,
    roles: [ "god", "director", "admin", "tutor" ],
    custom: !0
  }), gcSet("navItems", items), document.getElementById("gc-new-ico").value = "", 
  document.getElementById("gc-new-lbl").value = "", document.getElementById("gc-new-sec").value = "", 
  gcRenderNav(), buildSidebar(), mkToast('Вкладку "' + lbl + '" додано')) : mkToast("Введіть назву вкладки", "error");
}

function gcResetNav() {
  var nel;
  confirm("Скинути навігацію до стандартної?") && (gcSet("navItems", null), gcRenderNav(), 
  buildSidebar(), (nel = document.getElementById("ni-" + S.currentPage)) && nel.classList.add("active"), 
  mkToast("Навігацію скинуто"));
}

function gcDragStart(e, idx) {
  _gcDragSrc = idx, e.dataTransfer.effectAllowed = "move";
}

function gcDragOver(e, idx) {
  e.preventDefault(), e.dataTransfer.dropEffect = "move", document.querySelectorAll(".nav-edit-row").forEach(function(r, i) {
    r.classList.toggle("drag-over", i === idx && i !== _gcDragSrc);
  });
}

function gcDragLeave() {
  document.querySelectorAll(".nav-edit-row").forEach(function(r) {
    r.classList.remove("drag-over");
  });
}

function gcDrop(e, targetIdx) {
  var moved;
  e.preventDefault(), gcDragLeave(), null === _gcDragSrc || _gcDragSrc === targetIdx ? _gcDragSrc = null : (moved = (e = gcGetNavItems()).splice(_gcDragSrc, 1)[0], 
  e.splice(targetIdx, 0, moved), _gcDragSrc = null, gcSet("navItems", e), gcRenderNav(), 
  buildSidebar(), (targetIdx = document.getElementById("ni-" + S.currentPage)) && targetIdx.classList.add("active"));
}

function gcGetFields() {
  return (gcGetConfig().customFields || []).slice();
}

function gcRenderFields() {
  var rows, fields = gcGetFields(), targets = {
    student: "Учень",
    lesson: "Заняття",
    tutor: "Викладач",
    payment: "Платіж"
  }, el = document.getElementById("gc-field-editor");
  fields.length ? (rows = "", fields.forEach(function(f, i) {
    var tSel = "", extraInput = (Object.keys(targets).forEach(function(k) {
      tSel += '<option value="' + k + '"' + (f.target === k ? " selected" : "") + ">" + targets[k] + "</option>";
    }), "select" === f.type ? '<input type="text" value="' + gcEsc((f.options || []).join("; ")) + '" placeholder="Варіанти через ; " style="flex:1;min-width:100px;font-size:11px;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t1);padding:4px 8px" oninput="gcLiveFieldOpts(' + i + ',this.value)">' : '<span style="flex:1"></span>');
    rows += '<div class="field-row"><span style="color:var(--t3);font-size:14px;cursor:grab;padding:0 2px">&#8283;</span><span class="field-type-badge">' + FIELD_TYPE_ICONS[f.type] + " " + FIELD_TYPE_LABELS[f.type] + '</span><input type="text" value="' + gcEsc(f.label || "") + '" placeholder="Назва поля" style="flex:1;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t1);padding:5px 8px;font-size:13px;font-family:Karla,sans-serif" oninput="gcLiveFieldLabel(' + i + ',this.value)"><select style="width:95px;background:var(--s1);border:1px solid var(--b1);border-radius:6px;color:var(--t2);padding:4px 6px;font-size:11px" onchange="gcLiveFieldTarget(' + i + ',this.value)">' + tSel + "</select>" + extraInput + '<label style="display:flex;align-items:center;gap:4px;font-size:11px;color:var(--t2);white-space:nowrap;cursor:pointer"><input type="checkbox" ' + (f.required ? "checked" : "") + ' style="accent-color:var(--god2);cursor:pointer" onchange="gcLiveFieldReq(' + i + ',this.checked)"> *обов</label><button class="btn btn-sm" style="background:rgba(248,113,113,.1);border:1px solid rgba(248,113,113,.2);color:var(--danger)" onclick="gcDelField(' + i + ')">&times;</button></div>';
  }), el.innerHTML = '<div class="field-editor">' + rows + '</div><div class="gc-save-bar"><span style="font-size:12px;color:var(--tut)">&#10003; Поля відображаються у формах при додаванні записів</span><button class="btn btn-g btn-sm" style="margin-left:auto" onclick="gcClearFields()">&#128465; Очистити всі</button></div>') : el.innerHTML = '<div class="empty" style="padding:24px"><div class="ei">὜2</div>Додаткових полів немає.<br><span style="font-size:12px">Скористайтеся формою нижче, щоб додати перше поле.</span></div>';
}

function gcLiveFieldLabel(idx, val) {
  var f = gcGetFields();
  f[idx].label = val, gcSet("customFields", f);
}

function gcLiveFieldTarget(idx, val) {
  var f = gcGetFields();
  f[idx].target = val, gcSet("customFields", f);
}

function gcLiveFieldReq(idx, val) {
  var f = gcGetFields();
  f[idx].required = val, gcSet("customFields", f);
}

function gcLiveFieldOpts(idx, val) {
  var f = gcGetFields();
  f[idx].options = val.split(";").map(function(x) {
    return x.trim();
  }).filter(Boolean), gcSet("customFields", f);
}

function gcDelField(idx) {
  var f = gcGetFields();
  f.splice(idx, 1), gcSet("customFields", f), gcRenderFields(), mkToast("Поле видалено");
}

function gcClearFields() {
  confirm("Видалити всі додаткові поля?") && (gcSet("customFields", []), gcRenderFields(), 
  mkToast("Поля очищено"));
}

function gcAddField() {
  var f, type = document.getElementById("gc-field-type").value, label = document.getElementById("gc-field-lbl").value.trim(), target = document.getElementById("gc-field-target").value, optsRaw = document.getElementById("gc-field-opts").value;
  "divider" === type || "label" === type || label ? (f = gcGetFields(), label = {
    id: "cf_" + uid(),
    type: type,
    label: label || ("divider" === type ? "---" : "Заголовок"),
    target: target,
    required: !1
  }, "select" === type && optsRaw && (label.options = optsRaw.split(";").map(function(x) {
    return x.trim();
  }).filter(Boolean)), f.push(label), gcSet("customFields", f), document.getElementById("gc-field-lbl").value = "", 
  document.getElementById("gc-field-opts").value = "", gcRenderFields(), mkToast('"' + label.label + '" додано до форми "' + target + '"')) : mkToast("Введіть назву поля", "error");
}

function gcGetLabels() {
  var cfg = gcGetConfig(), out = {};
  return Object.keys(DEFAULT_LABELS_MAP).forEach(function(k) {
    out[k] = DEFAULT_LABELS_MAP[k].def;
  }), Object.assign(out, cfg.labels || {}), out;
}

function gcRenderLabels() {
  var labels = gcGetLabels(), el = document.getElementById("gc-labels-editor"), wrapper = document.createElement("div"), saveBar = (wrapper.style.cssText = "display:grid;grid-template-columns:1fr 1fr;gap:12px", 
  Object.keys(DEFAULT_LABELS_MAP).forEach(function(key) {
    var def = DEFAULT_LABELS_MAP[key], div = document.createElement("div"), lbl = (div.className = "fgr", 
    document.createElement("label")), lblText = (lbl.style.cssText = "display:flex;align-items:center;justify-content:space-between;gap:4px", 
    document.createElement("span")), resetBtn = (lblText.textContent = def.label, 
    document.createElement("button"));
    resetBtn.type = "button", resetBtn.innerHTML = "&#8635;", resetBtn.title = "Скинути до стандартного", 
    resetBtn.style.cssText = "background:none;border:none;color:var(--t3);cursor:pointer;font-size:13px;padding:0 2px", 
    resetBtn.addEventListener("click", function() {
      gcResetLabel(key);
    }), lbl.appendChild(lblText), lbl.appendChild(resetBtn), (lblText = document.createElement("input")).type = "text", 
    lblText.value = labels[key] || "", lblText.placeholder = def.def, lblText.style.cssText = "width:100%;background:var(--s2);border:1px solid var(--b1);border-radius:8px;color:var(--t1);padding:9px 12px;font-size:13px;outline:none;transition:border .15s", 
    lblText.addEventListener("focus", function() {
      this.style.borderColor = "var(--god2)";
    }), lblText.addEventListener("blur", function() {
      this.style.borderColor = "var(--b1)";
    }), lblText.addEventListener("input", function() {
      gcLiveLabelChange(key, this.value);
    }), div.appendChild(lbl), div.appendChild(lblText), wrapper.appendChild(div);
  }), document.createElement("div"));
  saveBar.className = "gc-save-bar", saveBar.innerHTML = '<span style="font-size:12px;color:var(--tut)">&#10003; Тексти оновлюються в інтерфейсі миттєво</span><button class="btn btn-g btn-sm" style="margin-left:auto" onclick="gcResetAllLabels()">&#8635; Скинути всі</button>', 
  el.innerHTML = "", el.appendChild(wrapper), el.appendChild(saveBar);
}

function gcLiveLabelChange(key, val) {
  var cfg = gcGetConfig();
  cfg.labels || (cfg.labels = {}), val === DEFAULT_LABELS_MAP[key].def || "" === val ? delete cfg.labels[key] : cfg.labels[key] = val, 
  gcSet("labels", cfg.labels && Object.keys(cfg.labels).length ? cfg.labels : null), 
  gcApplyLabel(key, val || DEFAULT_LABELS_MAP[key].def);
}

function gcApplyLabel(key, val) {
  var ptitle = document.getElementById("ptitle");
  "studentsTitle" === key && (PLABELS.students = val, "students" === S.currentPage) && ptitle && (ptitle.textContent = val), 
  "tutorsTitle" === key && (PLABELS.tutors = val, "tutors" === S.currentPage) && ptitle && (ptitle.textContent = val), 
  "lessonsTitle" === key && (PLABELS.lessons = val, "lessons" === S.currentPage) && ptitle && (ptitle.textContent = val), 
  "paymentsTitle" === key && (PLABELS.payments = val, "payments" === S.currentPage) && ptitle && (ptitle.textContent = val), 
  "scheduleTitle" === key && (PLABELS.schedule = val, "schedule" === S.currentPage) && ptitle && (ptitle.textContent = val), 
  "reportsTitle" === key && (PLABELS.reports = val, "reports" === S.currentPage) && ptitle && (ptitle.textContent = val), 
  "appName" === key && (ptitle = document.querySelector(".sblt")) && (ptitle.textContent = val), 
  "loginTitle" === key && (ptitle = document.querySelector(".lh")) && (ptitle.textContent = val), 
  "loginSub" === key && (ptitle = document.querySelector(".lsub")) && (ptitle.textContent = val), 
  (ptitle = document.getElementById("addbtn")) && "addStudent" === key && "students" === S.currentPage && (ptitle.textContent = "+ " + val), 
  !ptitle || "addLesson" !== key || "lessons" !== S.currentPage && "schedule" !== S.currentPage || (ptitle.textContent = "+ " + val), 
  ptitle && "addPayment" === key && "payments" === S.currentPage && (ptitle.textContent = "+ " + val), 
  ptitle && "addTutor" === key && "tutors" === S.currentPage && (ptitle.textContent = "+ " + val), 
  buildSidebar(), (key = document.getElementById("ni-" + S.currentPage)) && key.classList.add("active");
}

function gcResetLabel(key) {
  var cfg = gcGetConfig();
  cfg.labels && (delete cfg.labels[key], gcSet("labels", Object.keys(cfg.labels).length ? cfg.labels : null)), 
  gcRenderLabels(), gcApplyLabel(key, DEFAULT_LABELS_MAP[key].def);
}

function gcResetAllLabels() {
  var labels;
  confirm("Скинути всі тексти до стандартних?") && (gcSet("labels", null), labels = {}, 
  Object.keys(DEFAULT_LABELS_MAP).forEach(function(k) {
    labels[k] = DEFAULT_LABELS_MAP[k].def;
  }), Object.keys(labels).forEach(function(k) {
    gcApplyLabel(k, labels[k]);
  }), gcRenderLabels(), mkToast("Всі тексти скинуто"));
}

function applyGodConfig() {
  var cfg = gcGetConfig();
  cfg.perms && [ "director", "admin", "tutor" ].forEach(function(role) {
    cfg.perms[role] && (Object.assign(ROLES[role].can, cfg.perms[role]), "seeIncome" in cfg.perms[role] && (ROLES[role].seeIncome = cfg.perms[role].seeIncome), 
    "seeAll" in cfg.perms[role]) && (ROLES[role].seeAll = cfg.perms[role].seeAll);
  }), cfg.labels && Object.keys(cfg.labels).forEach(function(k) {
    gcApplyLabel(k, cfg.labels[k]);
  });
}

function renderCustomFields(target, containerId) {
  var html, fields = (gcGetConfig().customFields || []).filter(function(f) {
    return f.target === target;
  });
  fields.length && (containerId = document.getElementById(containerId)) && (html = '<div class="fgr full" style="border-top:1px solid var(--b1);padding-top:12px;margin-top:6px"><label style="color:var(--adm);letter-spacing:.5px">&#9889; Додаткові поля</label></div>', 
  fields.forEach(function(f) {
    var opts, itype;
    "divider" === f.type ? html += '<div style="grid-column:1/-1;border-top:1px solid var(--b1);margin:4px 0;font-size:11px;color:var(--t3);padding-top:4px">' + ("---" !== f.label ? f.label : "") + "</div>" : "label" === f.type ? html += '<div class="fgr full"><div style="font-size:12px;font-weight:700;color:var(--adm);letter-spacing:.5px;text-transform:uppercase;margin-top:6px">' + f.label + "</div></div>" : "checkbox" === f.type ? html += '<div class="fgr"><label style="display:flex;align-items:center;gap:8px;cursor:pointer"><input type="checkbox" id="cf_' + f.id + '" style="accent-color:var(--adm);width:14px;height:14px"> <span>' + f.label + (f.required ? " *" : "") + "</span></label></div>" : "select" === f.type ? (opts = '<option value="">Оберіть...</option>', 
    (f.options || []).forEach(function(o) {
      opts += "<option>" + o + "</option>";
    }), html += '<div class="fgr"><label>' + f.label + (f.required ? " *" : "") + '</label><select id="cf_' + f.id + '">' + opts + "</select></div>") : "textarea" === f.type ? html += '<div class="fgr full"><label>' + f.label + (f.required ? " *" : "") + '</label><textarea id="cf_' + f.id + '" placeholder="' + f.label + '..."></textarea></div>' : (itype = "number" === f.type ? "number" : "date" === f.type ? "date" : "text", 
    html += '<div class="fgr"><label>' + f.label + (f.required ? " *" : "") + '</label><input id="cf_' + f.id + '" type="' + itype + '" placeholder="' + f.label + (f.required ? " *" : "") + '"></div>');
  }), containerId.insertAdjacentHTML("beforeend", html));
}

function gcSaveRoles() {
  mkToast("Права збережено ✅");
}

function gcSaveNav() {
  buildSidebar(), mkToast("Навігацію оновлено ✅");
}

function gcSaveFields() {
  mkToast("Поля збережено ✅");
}

function gcResetLabels() {
  gcResetAllLabels();
}

function gcSaveLabels() {
  mkToast("Тексти збережено ✅");
}

function gSearch(q) {
  1 < q.length && "students" !== S.currentPage && nav("students");
}

var SUPABASE_URL = "https://rndxbvwisppxnhvrzwqi.supabase.co", SUPABASE_ANON = "sb_publishable_21KKA9MELBdwMRj4XG0riw_NuLYzpAw", _sb = null, CU = null, S = {
  students: [],
  tutors: [],
  lessons: [],
  payments: [],
  users: [],
  subjects: [],
  comms: [],
  branches: [],
  pricingRules: [],
  settings: {},
  currentBranchId: null,
  weekOffset: 0,
  dayOffset: 0,
  dashWeekOffset: 0,
  currentPage: "dashboard",
  editId: null,
  schView: "week",
  sfCur: "all",
  godConfig: null
}, sfCur = "all", _channels = [], _syncTimer = null;

function renderAnalytics() {
  var rangeEl, fromStr, lessons, comms, students, now, overallStats, branchRows, fromDate, tutorRows, filtBranch, filtTutor, maxDone, branchSel, tutorSel, visibleTutors;
  function kpi(icon, label, value, sub, color) {
    return '<div class="an-kpi" style="--kc:' + color + '"><div class="an-kpi-ico">' + icon + '</div><div class="an-kpi-val">' + value + '</div><div class="an-kpi-lbl">' + label + "</div>" + (sub ? '<div class="an-kpi-sub">' + sub + "</div>" : "") + "</div>";
  }
  function statRow(label, value, total, color) {
    return '<div class="an-row"><div class="an-row-lbl">' + label + '</div><div class="an-row-bar"><div class="an-row-fill" style="width:' + (total ? Math.round(value / total * 100) : 0) + "%;background:" + color + '"></div></div><div class="an-row-val">' + value + "</div></div>";
  }
  function calcStats(lessonsArr, commsArr, studentsArr) {
    return {
      done: lessonsArr.filter(function(l) {
        return "done" === l.status || "completed" === l.status;
      }).length,
      missed: lessonsArr.filter(function(l) {
        return "missed" === l.status || "absent" === l.status;
      }).length,
      cancelled: lessonsArr.filter(function(l) {
        return "cancelled" === l.status;
      }).length,
      planned: lessonsArr.filter(function(l) {
        return "planned" === l.status || "scheduled" === l.status;
      }).length,
      total: lessonsArr.length,
      income: lessonsArr.filter(function(l) {
        return "done" === l.status || "completed" === l.status;
      }).reduce(function(s, l) {
        return s + (parseFloat(l.price) || 0);
      }, 0),
      students: studentsArr.length,
      comms: commsArr.length
    };
  }
  document.getElementById("pg-analytics") && (rangeEl = (rangeEl = document.getElementById("an-range")) ? rangeEl.value : "30", 
  now = new Date(), fromDate = new Date(now), "week" === rangeEl ? fromDate.setDate(now.getDate() - 7) : "30" === rangeEl ? fromDate.setDate(now.getDate() - 30) : "90" === rangeEl ? fromDate.setDate(now.getDate() - 90) : "year" === rangeEl ? fromDate.setFullYear(now.getFullYear() - 1) : fromDate = new Date(0), 
  fromStr = localDateStr(fromDate), lessons = (S.lessons || []).filter(function(l) {
    return l.date >= fromStr;
  }), comms = (S.comms || []).filter(function(c) {
    return c.date >= fromStr;
  }), students = S.students || [], now = S.tutors || [], S.branches, overallStats = calcStats(lessons, comms, students), 
  branchRows = "", (fromDate = (fromDate = S.branches || []).length ? fromDate : [ {
    id: null,
    name: "Загальна"
  } ]).forEach(function(b) {
    var bs = calcStats(b.id ? lessons.filter(function(l) {
      return l.branchId === b.id || l.branch_id === b.id;
    }) : lessons, b.id ? comms.filter(function(c) {
      return c.branchId === b.id || c.branch_id === b.id;
    }) : comms, b.id ? students.filter(function(s) {
      return s.branchId === b.id || s.branch_id === b.id;
    }) : students);
    Math.max(overallStats.total, 1), branchRows += '<div class="an-branch-card"><div class="an-branch-title">Ἶ2 ' + b.name + '</div><div class="an-kpi-row">' + kpi("✅", "Проведено", bs.done, null, "var(--tut)") + kpi("❌", "Пропущено", bs.missed, null, "var(--danger)") + kpi("ὐ4", "Відпрацьовано", bs.done, null, "var(--adm)") + kpi("\uD83D\uDCAC", "Комунікацій", bs.comms, null, "var(--god2)") + kpi("὆5", "Учнів", bs.students, null, "var(--dir)") + '</div><div style="margin-top:8px">' + statRow("Проведено", bs.done, Math.max(bs.total, 1), "var(--tut)") + statRow("Пропущено", bs.missed, Math.max(bs.total, 1), "var(--danger)") + statRow("Скасовано", bs.cancelled, Math.max(bs.total, 1), "var(--t3)") + "</div></div>";
  }), tutorRows = "", filtBranch = document.getElementById("an-branch")?.value || "", 
  filtTutor = document.getElementById("an-tutor")?.value || "", (visibleTutors = now.filter(function(t) {
    return !(filtBranch && t.branchId !== filtBranch && t.branch_id !== filtBranch || filtTutor && t.id !== filtTutor);
  })).length || (visibleTutors = now), maxDone = Math.max.apply(null, visibleTutors.map(function(t) {
    return lessons.filter(function(l) {
      return l.tutorId === t.id && ("done" === l.status || "completed" === l.status);
    }).length;
  }).concat([ 1 ])), visibleTutors.forEach(function(t) {
    var ts = calcStats(lessons.filter(function(l) {
      return l.tutorId === t.id || l.tutor_id === t.id;
    }), comms.filter(function(c) {
      return c.tutorId === t.id || c.tutor_id === t.id;
    }), students.filter(function(s) {
      return s.tutorId === t.id || s.tutor_id === t.id;
    })), pctDone = maxDone ? Math.round(ts.done / maxDone * 100) : 0;
    tutorRows += '<div class="an-tutor-row"><div style="display:flex;align-items:center;gap:8px;min-width:140px">' + mkAv(t.fn, t.ln, 32) + '<div><div style="font-weight:600;font-size:13px">' + t.fn + " " + t.ln + '</div><div style="font-size:11px;color:var(--t2)">' + t.subj + '</div></div></div><div class="an-tutor-stats"><div class="an-stat-cell" title="Проведено"><span class="an-stat-ico">✅</span>' + ts.done + '</div><div class="an-stat-cell" title="Пропущено"><span class="an-stat-ico">❌</span>' + ts.missed + '</div><div class="an-stat-cell" title="Комунікацій"><span class="an-stat-ico">ὊC</span>' + ts.comms + '</div><div class="an-stat-cell" title="Учнів"><span class="an-stat-ico">὆5</span>' + ts.students + '</div></div><div class="an-bar-wrap"><div class="an-bar-fill" style="width:' + pctDone + '%"></div></div></div>';
  }), (branchSel = document.getElementById("an-branch")) && branchSel.children.length <= 1 && (S.branches || []).forEach(function(b) {
    var opt = document.createElement("option");
    opt.value = b.id, opt.textContent = b.name, branchSel.appendChild(opt);
  }), (tutorSel = document.getElementById("an-tutor")) && tutorSel.children.length <= 1 && now.forEach(function(t) {
    var opt = document.createElement("option");
    opt.value = t.id, opt.textContent = t.fn + " " + t.ln, tutorSel.appendChild(opt);
  }), visibleTutors = {
    week: "Тиждень",
    30: "30 днів",
    90: "3 місяці",
    year: "Рік",
    all: "За весь час"
  }[rangeEl] || rangeEl, document.getElementById("an-content").innerHTML = '<div class="an-section-title">ἱ0 Загальна статистика — ' + visibleTutors + '</div><div class="an-kpi-row an-kpi-row--big">' + kpi("✅", "Проведено уроків", overallStats.done, overallStats.total + " всього", "var(--tut)") + kpi("❌", "Пропущено уроків", overallStats.missed, Math.round(overallStats.missed / Math.max(overallStats.total, 1) * 100) + "%", "var(--danger)") + kpi("ὐ4", "Відпрацьовано", overallStats.done, Math.round(overallStats.income) + " ₴", "var(--adm)") + kpi("\uD83D\uDCAC", "Комунікацій", overallStats.comms, "з батьками", "var(--god2)") + kpi("὆5", "Активних учнів", students.filter(function(s) {
    return "active" === s.status;
  }).length, students.length + " всього", "var(--dir)") + "</div>" + (1 < fromDate.length ? '<div class="an-section-title" style="margin-top:20px">Ἶ2 По філіях</div><div class="an-branches-grid">' + branchRows + "</div>" : "") + '<div class="an-section-title" style="margin-top:20px">὆4 По репетиторах</div><div class="an-tutor-header"><div style="min-width:140px">Репетитор</div><div class="an-tutor-stats"><span>✅ Провів</span><span>❌ Пропуск</span><span>ὪB Скасов</span><span>ὊC Комун</span><span>὆5 Учні</span></div><div style="flex:1;font-size:10px;color:var(--t2);padding-left:8px">% від лідера</div></div><div>' + tutorRows + "</div>");
}

function saveS() {}

function loadS() {}

function saveSess() {}

function loadSess() {}

function seedData() {}

async function exportBackup() {
  var btn = document.getElementById("backup-btn");
  btn && (btn.disabled = !0, btn.textContent = "Завантаження...");
  try {
    for (var tables = [ "settings", "branches", "subjects", "pricing_rules", "tutors", "students", "lessons", "payments", "comms", "custom_fields" ], backup = {
      version: 1,
      created: new Date().toISOString(),
      data: {}
    }, i = 0; i < tables.length; i++) {
      var res = await _sb.from(tables[i]).select("*");
      backup.data[tables[i]] = res.data || [];
    }
    var prof = await _sb.from("profiles").select("id,email,fn,ln,role,branch_id,perms"), json = (backup.data.profiles = prof.data || [], 
    JSON.stringify(backup, null, 2)), blob = new Blob([ json ], {
      type: "application/json"
    }), url = URL.createObjectURL(blob), a = document.createElement("a"), date = localDateStr(new Date());
    a.href = url, a.download = "konstanta-backup-" + date + ".json", document.body.appendChild(a), 
    a.click(), document.body.removeChild(a), URL.revokeObjectURL(url), mkToast("Резервну копію збережено");
  } catch (e) {
    mkToast("Помилка: " + e.message, "error");
  }
  btn && (btn.disabled = !1, btn.textContent = "⬇ Завантажити резервну копію");
}

function importBackupClick() {
  document.getElementById("backup-file-input").click();
}

async function importBackup(input) {
  var file = input.files[0];
  if (file) {
    var btn = document.getElementById("restore-btn");
    btn && (btn.disabled = !0, btn.textContent = "Відновлення...");
    try {
      var text = await file.text(), backup = JSON.parse(text);
      if (!backup.version || !backup.data) return mkToast("Невірний формат файлу", "error"), 
      void (btn && (btn.disabled = !1, btn.textContent = "⬆ Відновити з копії"));
      if (!confirm("Відновити дані з копії від " + backup.created.slice(0, 10) + "?\n\n⚠ Це перезапише ВСІ поточні дані!")) return btn && (btn.disabled = !1, 
      btn.textContent = "⬆ Відновити з копії"), void (input.value = "");
      for (var stats = {}, errors = [], deleteOrder = [ "comms", "payments", "lessons", "students", "tutors", "pricing_rules", "subjects", "branches", "settings" ], di = 0; di < deleteOrder.length; di++) {
        var dr, dt = deleteOrder[di];
        backup.data[dt] && backup.data[dt].length && (dr = await _sb.from(dt).delete().neq("id", "______none______")).error && errors.push("del " + dt + ": " + dr.error.message);
      }
      for (var order = [ "settings", "branches", "subjects", "pricing_rules", "tutors", "students", "lessons", "payments", "comms" ], i = 0; i < order.length; i++) {
        var table = order[i], rows = backup.data[table];
        if (rows && rows.length) {
          for (var inserted = 0, j = 0; j < rows.length; j += 50) {
            var chunk = rows.slice(j, j + 50), res = await _sb.from(table).upsert(chunk, {
              onConflict: "id"
            });
            res.error ? errors.push(table + ": " + res.error.message) : inserted += chunk.length;
          }
          stats[table] = inserted;
        } else stats[table] = 0;
      }
      if (errors.length && await loadAll(), renderSch && renderSch(), nav(S.currentPage || "dashboard"), 
      backup.data.profiles && backup.data.profiles.length) {
        for (var pi = 0; pi < backup.data.profiles.length; pi += 50) {
          var pc = backup.data.profiles.slice(pi, pi + 50);
          await _sb.from("profiles").upsert(pc, {
            onConflict: "id"
          });
        }
        stats.profiles = backup.data.profiles.length;
      }
      mkToast("Відновлено! " + Object.entries(stats).map(function(e) {
        return e[0] + ": " + e[1];
      }).join(", "));
    } catch (e) {
      mkToast("Помилка відновлення: " + e.message, "error");
    }
    btn && (btn.disabled = !1, btn.textContent = "⬆ Відновити з копії"), input.value = "";
  }
}

var _presenceInterval = null;

async function updatePresence(online) {
  if (CU && _sb) try {
    await _sb.from("profiles").update({
      is_online: online,
      last_seen: new Date().toISOString()
    }).eq("id", CU.id);
  } catch (e) {}
}

function startPresence() {
  updatePresence(!0), _presenceInterval && clearInterval(_presenceInterval), _presenceInterval = setInterval(function() {
    updatePresence(!0);
  }, 3e4), document.addEventListener("visibilitychange", function() {
    document.hidden ? updatePresence(!1) : updatePresence(!0);
  }), window.addEventListener("beforeunload", function() {
    var url, body;
    navigator.sendBeacon && CU && (url = _sb.supabaseUrl + "/rest/v1/profiles?id=eq." + CU.id, 
    body = JSON.stringify({
      is_online: !1,
      last_seen: new Date().toISOString()
    }), navigator.sendBeacon(url, body));
  });
}

function stopPresence() {
  _presenceInterval && (clearInterval(_presenceInterval), _presenceInterval = null), 
  updatePresence(!1);
}

function formatLastSeen(ts) {
  var now;
  return ts ? (now = new Date(), ts = new Date(ts), (now = Math.floor((now - ts) / 1e3)) < 60 ? "щойно" : now < 3600 ? Math.floor(now / 60) + " хв тому" : now < 86400 ? Math.floor(now / 3600) + " год тому" : 1 === (now = Math.floor(now / 86400)) ? "вчора" : now < 7 ? now + " дн. тому" : ts.toLocaleDateString("uk-UA", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit"
  })) : "ніколи";
}

function presenceDot(isOnline) {
  return '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' + (isOnline ? "#22c55e" : "#94a3b8") + ';margin-right:5px;flex-shrink:0" title="' + (isOnline ? "Онлайн" : "Офлайн") + '"></span>';
}

async function initApp() {
  for (var lsEl, asEl, style, s, sdkWait = 0; "undefined" == typeof supabase && sdkWait < 30; ) await new Promise(function(r) {
    setTimeout(r, 100);
  }), sdkWait++;
  function hideLoading() {
    var el = document.getElementById("app-loading");
    el && el.remove();
  }
  "undefined" == typeof supabase ? document.body.innerHTML = '<div style="padding:40px;font-family:Arial;text-align:center"><h2>❌ Помилка завантаження</h2><p>Не вдалось завантажити Supabase SDK. Перезавантажте сторінку.</p><button onclick="location.reload()" style="padding:10px 20px;background:#29abe2;color:#fff;border:none;border-radius:8px;cursor:pointer;font-size:14px;margin-top:16px">ὐ4 Перезавантажити</button></div>' : "PASTE_YOUR_SUPABASE_URL" === SUPABASE_URL ? (s = document.getElementById("setup")) && (s.style.display = "flex") : (s = supabase.createClient, 
  _sb = s(SUPABASE_URL, SUPABASE_ANON), s = document.getElementById("setup"), lsEl = document.getElementById("ls"), 
  asEl = document.getElementById("as"), s && (s.style.display = "none"), lsEl && (lsEl.style.display = "none"), 
  asEl && (asEl.style.display = "none"), (s = document.createElement("div")).id = "app-loading", 
  s.style.cssText = "position:fixed;top:0;left:0;right:0;bottom:0;display:flex;align-items:center;justify-content:center;background:var(--bg,#0f1117);z-index:9999;flex-direction:column;gap:16px", 
  s.innerHTML = '<div style="width:40px;height:40px;border:3px solid rgba(255,255,255,.1);border-top-color:#29abe2;border-radius:50%;animation:spin 0.8s linear infinite"></div><div style="color:rgba(255,255,255,.5);font-size:13px;font-family:Karla,sans-serif">Завантаження...</div>', 
  (style = document.createElement("style")).textContent = "@keyframes spin{to{transform:rotate(360deg)}}", 
  document.head.appendChild(style), document.body.appendChild(s), (s = (style = await _sb.auth.getSession()).data && style.data.session) ? (CU = null, 
  await loadProfile(s.user), hideLoading(), startApp()) : (hideLoading(), lsEl && (lsEl.style.display = "flex")), 
  _sb.auth.onAuthStateChange(async function(event, session) {
    "SIGNED_IN" === event && session ? (await loadProfile(session.user), hideLoading(), 
    startApp()) : "SIGNED_OUT" === event && (CU = null, stopChannels(), asEl && (asEl.style.display = "none"), 
    lsEl) && (lsEl.style.display = "flex");
  }));
}

async function doLogin() {
  var email = document.getElementById("lu").value.trim(), pass = document.getElementById("lp").value, err = document.getElementById("lerr"), btn = document.getElementById("lbtn");
  if (err && (err.style.display = "none"), btn && (btn.disabled = !0, btn.textContent = "Входжу..."), 
  _sb) try {
    var res = await _sb.auth.signInWithPassword({
      email: email,
      password: pass
    });
    res.error && (err && (err.textContent = {
      "Invalid login credentials": "Невірний email або пароль",
      "Email not confirmed": "Підтвердіть email",
      "Too many requests": "Забагато спроб"
    }[res.error.message] || res.error.message, err.style.display = "block"), btn) && (btn.disabled = !1, 
    btn.textContent = "Увійти");
  } catch (e) {
    err && (err.textContent = "Помилка: " + e.message, err.style.display = "block"), 
    btn && (btn.disabled = !1, btn.textContent = "Увійти");
  } else err && (err.textContent = "Помилка ініціалізації", err.style.display = "block"), 
  btn && (btn.disabled = !1, btn.textContent = "Увійти");
}

async function doLogout() {
  stopPresence(), stopChannels(), await _sb.auth.signOut();
}

async function loadProfile(authUser) {
  var data = (await _sb.from("profiles").select("*").eq("id", authUser.id).single()).data;
  CU = data || (data = {
    id: authUser.id,
    email: authUser.email,
    fn: authUser.email.split("@")[0],
    ln: "",
    role: "tutor",
    perms: {}
  }, await _sb.from("profiles").insert(data), data);
}

function uid() {
  return crypto.randomUUID ? crypto.randomUUID() : Date.now().toString(36) + Math.random().toString(36).slice(2);
}

function setSaving() {
  var dot = document.getElementById("syncdot"), lbl = document.getElementById("sync-lbl");
  dot && (dot.className = "sync-dot saving"), lbl && (lbl.textContent = "збереження…", 
  lbl.style.color = "var(--warn)");
}

function setSynced() {
  clearTimeout(_syncTimer);
  var dot = document.getElementById("syncdot"), lbl = document.getElementById("sync-lbl");
  dot && (dot.className = "sync-dot ok"), lbl && (lbl.textContent = "синхронізовано", 
  lbl.style.color = "var(--tut)"), _syncTimer = setTimeout(function() {
    lbl && (lbl.textContent = "онлайн", lbl.style.color = "var(--t3)");
  }, 2500);
}

async function loadAll() {
  setSaving();
  let tables = [ {
    table: "branches",
    key: "branches"
  }, {
    table: "tutors",
    key: "tutors"
  }, {
    table: "students",
    key: "students"
  }, {
    table: "lessons",
    key: "lessons",
    order: "date"
  }, {
    table: "payments",
    key: "payments",
    order: "date"
  }, {
    table: "subjects",
    key: "subjects"
  }, {
    table: "comms",
    key: "comms",
    order: "date",
    tutorFilter: !0
  }, {
    table: "pricing_rules",
    key: "pricingRules"
  } ], results = await Promise.all(tables.map(function(t) {
    var q = _sb.from(t.table).select("*");
    return t.order ? q.order(t.order, {
      ascending: !1
    }) : q;
  })), _selfT, _commsQ;
  tables.forEach(function(t, i) {
    S[t.key] = results[i].data || [];
  }), CU && "tutor" === CU.role && (S.tutors = S.tutors.map(normalizeTutor), _selfT = S.tutors.find(function(t) {
    return t.accId === CU.id || t.acc_uid === CU.id;
  })) && (_commsQ = await _sb.from("comms").select("*").eq("tutor_id", _selfT.id).order("date", {
    ascending: !1
  }), S.comms = _commsQ.data || []);
  var set = (await _sb.from("settings").select("*").eq("id", "main").single()).data, set = (S.settings = set || {}, 
  (await _sb.from("profiles").select("*")).data);
  S.users = set || [], S.students = S.students.map(normalizeStudent), S.lessons = S.lessons.map(normalizeLesson), 
  S.payments = S.payments.map(normalizePayment), S.tutors = S.tutors.map(normalizeTutor), 
  S.comms = S.comms.map(normalizeComm), S.pricingRules = S.pricingRules.map(normalizePricingRule), 
  setSynced();
}

function normalizeStudent(r) {
  var tutorIds = r.tutor_ids ? Array.isArray(r.tutor_ids) ? r.tutor_ids : r.tutor_ids.split(",").filter(Boolean) : r.tutor_id ? [ r.tutor_id ] : [];
  return Object.assign({}, r, {
    tutorId: r.tutor_id,
    crmStage: r.crm_stage || null,
    crmResponsible: r.crm_responsible || null,
    crmDate: r.crm_date || null,
    tutorIds: tutorIds,
    branchId: r.branch_id,
    parentFn: r.parent_fn,
    parentPhone: r.parent_phone
  });
}

function normalizeLesson(r) {
  return Object.assign({}, r, {
    studentId: r.student_id,
    tutorId: r.tutor_id,
    branchId: r.branch_id,
    recurId: r.recur_id,
    recurType: r.recur_type,
    recurIndex: r.recur_index
  });
}

function normalizePayment(r) {
  return Object.assign({}, r, {
    studentId: r.student_id,
    branchId: r.branch_id
  });
}

function normalizeTutor(r) {
  return Object.assign({}, r, {
    accId: r.acc_uid,
    branchId: r.branch_id
  });
}

function normalizeComm(r) {
  return Object.assign({}, r, {
    tutorId: r.tutor_id,
    studentId: r.student_id,
    branchId: r.branch_id
  });
}

function normalizePricingRule(r) {
  return Object.assign({}, r, {
    subjectMatch: r.subject_match,
    tutorId: r.tutor_id,
    gradeMatch: r.grade_match,
    durMin: r.dur_min
  });
}

function startChannels() {
  var tableMap = {
    students: "students",
    tutors: "tutors",
    lessons: "lessons",
    payments: "payments",
    subjects: "subjects",
    comms: "comms",
    pricing_rules: "pricingRules",
    branches: "branches",
    profiles: "users"
  };
  Object.keys(tableMap).forEach(function(table) {
    var key = tableMap[table], ch = _sb.channel("rt:" + table).on("postgres_changes", {
      event: "*",
      schema: "public",
      table: table
    }, function(payload) {
      handleChange(key, table, payload);
    }).subscribe();
    _channels.push(ch);
  });
}

function stopChannels() {
  _channels.forEach(function(ch) {
    try {
      _sb.removeChannel(ch);
    } catch (e) {}
  }), _channels = [];
}

function handleChange(key, table, payload) {
  setSynced();
  var ev = payload.eventType, row = payload.new, old = payload.old;
  (payload = {
    students: normalizeStudent,
    lessons: normalizeLesson,
    payments: normalizePayment,
    tutors: normalizeTutor,
    comms: normalizeComm,
    pricingRules: normalizePricingRule
  })[key] && (row = row && payload[key](row)), "INSERT" === ev ? S[key] = (S[key] || []).concat([ row ]) : "UPDATE" === ev ? S[key] = (S[key] || []).map(function(r) {
    return r.id === row.id ? row : r;
  }) : "DELETE" === ev && (S[key] = (S[key] || []).filter(function(r) {
    return r.id !== old.id;
  })), refreshPage(key);
}

function refreshPage(key) {
  if (void 0 !== S && S.currentPage) {
    var pg = S.currentPage;
    if (({
      students: [ "students", "dashboard", "profile" ],
      tutors: [ "tutors", "dashboard", "profile" ],
      lessons: [ "lessons", "schedule", "dashboard", "profile" ],
      payments: [ "payments", "dashboard" ],
      comms: [ "dashboard", "profile" ],
      subjects: [ "settings", "lessons" ],
      pricingRules: [ "settings" ],
      branches: [ "settings" ],
      users: [ "users" ]
    }[key] || []).includes(pg)) try {
      "dashboard" === pg && "function" == typeof renderDash ? renderDash() : "students" === pg && "function" == typeof renderStudents ? renderStudents() : "tutors" === pg && "function" == typeof renderTutors ? renderTutors() : "schedule" === pg && "function" == typeof renderSch ? renderSch() : "lessons" === pg && "function" == typeof renderLessons ? renderLessons() : "payments" === pg && "function" == typeof renderPayments ? renderPayments() : "settings" === pg && "function" == typeof renderSettings ? renderSettings() : "users" === pg && "function" == typeof renderUsers ? renderUsers() : "profile" === pg && "function" == typeof renderProfile && renderProfile();
    } catch (e) {}
  }
}

async function loadTableFresh(table) {
  var norm, key = {
    students: "students",
    tutors: "tutors",
    lessons: "lessons",
    payments: "payments",
    subjects: "subjects",
    comms: "comms",
    pricing_rules: "pricingRules",
    branches: "branches"
  }[table];
  key && (norm = {
    students: normalizeStudent,
    lessons: normalizeLesson,
    payments: normalizePayment,
    tutors: normalizeTutor,
    comms: normalizeComm,
    pricingRules: normalizePricingRule
  }, (table = await _sb.from(table).select("*")).error || (table = table.data || [], 
  S[key] = norm[key] ? table.map(norm[key]) : table, setSynced(), refreshPage(key)));
}

async function dbInsert(table, data) {
  if (setSaving(), data = (await _sb.from(table).insert(data)).error) throw mkToast("Помилка: " + data.message, "error"), 
  data;
  setTimeout(function() {
    loadTableFresh(table);
  }, 800);
}

async function dbUpdate(table, id, data) {
  if (setSaving(), data = 0 <= [ "profiles" ].indexOf(table) ? Object.assign({}, data) : Object.assign({}, data, {
    updated_at: new Date().toISOString()
  }), data = (await _sb.from(table).update(data).eq("id", id)).error) throw mkToast("Помилка: " + data.message, "error"), 
  data;
  setTimeout(function() {
    loadTableFresh(table);
  }, 800);
}

async function dbDelete(table, id) {
  if (setSaving(), id = (await _sb.from(table).delete().eq("id", id)).error) throw mkToast("Помилка: " + id.message, "error"), 
  id;
  setTimeout(function() {
    loadTableFresh(table);
  }, 500);
}

async function saveStudent() {
  var fn = document.getElementById("s-fn").value.trim(), ln = document.getElementById("s-ln").value.trim();
  if (fn && ln) {
    var i, newId, norm, ln = {
      fn: fn,
      ln: ln,
      age: document.getElementById("s-age")?.value || null,
      grade: document.getElementById("s-grade")?.value || "",
      phone: document.getElementById("s-phone")?.value || "",
      email: document.getElementById("s-email")?.value || "",
      subject: document.getElementById("s-subj")?.value || "",
      tutor_id: (fn = document.querySelectorAll(".st-tutor-cb:checked")).length ? fn[0].value : null,
      tutor_ids: Array.from(document.querySelectorAll(".st-tutor-cb:checked")).map(function(cb) {
        return cb.value;
      }).join(","),
      status: document.getElementById("s-status")?.value || "active",
      src: document.getElementById("s-src")?.value || "referral",
      notes: document.getElementById("s-notes")?.value || "",
      parent_fn: (document.getElementById("s-parent-fn")?.value || "").trim(),
      parent_phone: (document.getElementById("s-parent-phone")?.value || "").trim(),
      branch_id: document.getElementById("s-branch")?.value || myBranchId() || null
    };
    "tutor" !== R() || ln.tutor_id || (fn = myTutor()) && (ln.tutor_id = fn.id, 
    ln.tutor_ids = fn.id), window._saving = !0;
    try {
      S.editId ? (await dbUpdate("students", S.editId, ln), norm = normalizeStudent(Object.assign({
        id: S.editId
      }, ln)), 0 <= (i = S.students.findIndex(function(x) {
        return x.id === S.editId;
      })) ? S.students[i] = norm : S.students.push(norm), mkToast("Учня оновлено")) : (newId = uid(), 
      norm = normalizeStudent(Object.assign({
        id: newId
      }, ln)), await dbInsert("students", Object.assign({
        id: newId
      }, ln)), S.students.push(norm), mkToast("Учня додано")), closeM("mo-student"), 
      S.editId = null, window._saving = !1, refreshPage("students");
    } catch (e) {
      window._saving = !1, mkToast("Помилка: " + (e.message || e), "error");
    }
  } else mkToast("Ім'я та прізвище обов'язкові", "error");
}

async function delStudent(id) {
  if (can("students")) {
    if (confirm("Видалити учня?")) try {
      await dbDelete("students", id), mkToast("Видалено");
    } catch (e) {}
  } else mkToast("Немає прав", "error");
}

async function saveTutor() {
  var fn = document.getElementById("t-fn").value.trim(), ln = document.getElementById("t-ln").value.trim();
  if (fn && ln) {
    fn = {
      fn: fn,
      ln: ln,
      phone: document.getElementById("t-phone")?.value || "",
      email: document.getElementById("t-email")?.value || "",
      subj: document.getElementById("t-subj")?.value || "",
      rate: document.getElementById("t-rate")?.value || null,
      bio: document.getElementById("t-bio")?.value || "",
      rating: parseInt(document.getElementById("t-rating")?.value) || 5,
      branch_id: myBranchId() || null
    }, window._saving = !0;
    try {
      S.editId ? (await dbUpdate("tutors", S.editId, fn), mkToast("Оновлено")) : (await dbInsert("tutors", Object.assign({
        id: uid()
      }, fn)), mkToast("Викладача додано")), closeM("mo-tutor"), S.editId = null, 
      window._saving = !1, refreshPage("tutors");
    } catch (e) {
      window._saving = !1, mkToast("Помилка: " + (e.message || e), "error");
    }
  } else mkToast("Ім'я та прізвище обов'язкові", "error");
}

async function delTutor(id) {
  if (can("tutors")) {
    if (confirm("Видалити викладача?")) try {
      await dbDelete("tutors", id), mkToast("Видалено");
    } catch (e) {}
  } else mkToast("Немає прав", "error");
}

async function saveLesson() {
  var stdEl = document.getElementById("l-std"), dateEl = document.getElementById("l-date"), stdEl = stdEl ? stdEl.value : "", dateEl = dateEl ? dateEl.value : "";
  if (stdEl && dateEl) {
    var recurType = document.getElementById("l-recur")?.value || "none", obj = {
      student_id: stdEl,
      tutor_id: document.getElementById("l-tutor")?.value || null,
      subject: document.getElementById("l-subj")?.value || "",
      date: dateEl,
      time: document.getElementById("l-time")?.value || "",
      dur: parseInt(document.getElementById("l-dur")?.value) || 60,
      price: parseFloat(document.getElementById("l-price")?.value) || 0,
      status: document.getElementById("l-stat")?.value || "planned",
      notes: document.getElementById("l-notes")?.value || "",
      hw: (document.getElementById("l-hw") || {
        value: ""
      }).value || "",
      missed_date: (document.getElementById("l-missed-date") || {
        value: null
      }).value || null,
      makeup_date: (document.getElementById("l-makeup-date") || {
        value: null
      }).value || null,
      branch_id: myBranchId() || null
    };
    window._saving = !0;
    try {
      if (S.editId) await dbUpdate("lessons", S.editId, obj), mkToast("Оновлено"), 
      closeM("mo-lesson"), window._saving = !1, refreshPage("lessons"), "schedule" === S.currentPage && renderSch(); else {
        if (recurType && "none" !== recurType) {
          for (var dates = genRecurDates(dateEl, recurType, document.getElementById("l-recur-end")?.value, parseInt(document.getElementById("l-recur-count")?.value) || 10, parseInt(document.getElementById("l-recur-interval")?.value) || 1), recurId = uid(), i = 0; i < dates.length; i++) await dbInsert("lessons", Object.assign({
            id: uid()
          }, obj, {
            date: dates[i],
            recur_id: recurId,
            recur_type: recurType,
            recur_index: i
          }));
          mkToast("Додано " + dates.length + " занять");
        } else await dbInsert("lessons", Object.assign({
          id: uid()
        }, obj)), mkToast("Заняття додано");
        closeM("mo-lesson");
      }
      S.editId = null;
    } catch (e) {}
  } else mkToast("Учень та дата обов'язкові", "error");
}

async function delLesson(id) {
  if (can("lessons")) {
    var l = (S.lessons || []).find(function(x) {
      return x.id === id;
    });
    if (l && l.recurId) S.editId = id, openM("mo-del-recur"); else if (confirm("Видалити заняття?")) try {
      await dbDelete("lessons", id), mkToast("Видалено");
    } catch (e) {}
  } else mkToast("Немає прав", "error");
}

async function doDelLesson(mode) {
  var id = S.editId, l = (S.lessons || []).find(function(x) {
    return x.id === id;
  });
  if (l) {
    closeM("mo-del-recur"), S.editId = null;
    try {
      if ("one" === mode) await dbDelete("lessons", id), mkToast("Видалено"); else if ("future" === mode) {
        for (var toDelete = (S.lessons || []).filter(function(x) {
          return x.recurId === l.recurId && x.recurIndex >= l.recurIndex;
        }), i = 0; i < toDelete.length; i++) await dbDelete("lessons", toDelete[i].id);
        mkToast("Видалено " + toDelete.length + " занять");
      } else {
        for (var all = (S.lessons || []).filter(function(x) {
          return x.recurId === l.recurId;
        }), i = 0; i < all.length; i++) await dbDelete("lessons", all[i].id);
        mkToast("Видалено серію (" + all.length + ")");
      }
    } catch (e) {}
  } else closeM("mo-del-recur");
}

async function savePayment() {
  var studentId = document.getElementById("p-std")?.value, amount = parseFloat(document.getElementById("p-amt")?.value);
  if (studentId && amount) {
    studentId = {
      student_id: studentId,
      amount: amount,
      method: document.getElementById("p-mth")?.value || "cash",
      date: document.getElementById("p-date")?.value,
      month: document.getElementById("p-mon")?.value || "",
      status: document.getElementById("p-stat")?.value || "paid",
      note: document.getElementById("p-note")?.value || "",
      branch_id: myBranchId() || null
    }, window._saving = !0;
    try {
      S.editId ? (await dbUpdate("payments", S.editId, studentId), mkToast("Оновлено")) : (await dbInsert("payments", Object.assign({
        id: uid()
      }, studentId)), mkToast("Записано")), closeM("mo-payment"), S.editId = null, 
      window._saving = !1, refreshPage("payments");
    } catch (e) {
      window._saving = !1, mkToast("Помилка: " + (e.message || e), "error");
    }
  } else mkToast("Учень та сума обов'язкові", "error");
}

async function delPay(id) {
  if (confirm("Видалити платіж?")) try {
    await dbDelete("payments", id), mkToast("Видалено");
  } catch (e) {}
}

function updateParentInfo() {
  var sid, fn, sel = document.getElementById("cm-student"), wrap = document.getElementById("cm-parent-wrap"), info = document.getElementById("cm-parent-info");
  sel && wrap && info && ((sel = (sid = sel.value) ? (S.students || []).find(function(x) {
    return x.id === sid;
  }) : null) && (sel.parentFn || sel.parent_fn || sel.parentPhone || sel.parent_phone) ? (fn = sel.parentFn || sel.parent_fn || "", 
  sel = sel.parentPhone || sel.parent_phone || "", info.innerHTML = (fn ? "<strong>" + fn + "</strong>" : "") + (sel ? ' — <a href="tel:' + sel + '">' + sel + "</a>" : ""), 
  wrap.style.display = "block") : wrap.style.display = "none");
}

async function saveComm() {
  var tutorId = document.getElementById("cm-tutor")?.value, date = document.getElementById("cm-date")?.value;
  if (tutorId) if (date) {
    window._saving = !0;
    try {
      await dbInsert("comms", {
        id: uid(),
        tutor_id: tutorId,
        student_id: document.getElementById("cm-student")?.value || null,
        date: date,
        type: document.getElementById("cm-type")?.value || "call",
        note: document.getElementById("cm-note")?.value || "",
        branch_id: myBranchId() || null
      }), closeM("mo-comm"), mkToast("Записано"), window._saving = !1, refreshPage("comms");
    } catch (e) {
      window._saving = !1, mkToast("Помилка: " + (e.message || e), "error");
    }
  } else mkToast("Вкажіть дату", "error"); else mkToast("Оберіть репетитора", "error");
}

async function delComm(id) {
  if (confirm("Видалити?")) try {
    await dbDelete("comms", id), mkToast("Видалено");
  } catch (e) {}
}

async function saveSettings() {
  try {
    await _sb.from("settings").upsert({
      id: "main",
      name: document.getElementById("set-name")?.value || "",
      phone: document.getElementById("set-phone")?.value || "",
      email: document.getElementById("set-email")?.value || "",
      address: document.getElementById("set-addr")?.value || "",
      payment_details: document.getElementById("set-payment")?.value || "",
      unisender_key: document.getElementById("set-unisender-key")?.value || "",
      viber_sender: document.getElementById("set-viber-sender")?.value || "",
      updated_at: new Date().toISOString()
    }), mkToast("Збережено");
  } catch (e) {
    mkToast("Помилка", "error");
  }
}

async function addSubj() {
  var name = (document.getElementById("ns-name")?.value || "").trim(), price = document.getElementById("ns-price")?.value;
  if (name) try {
    await dbInsert("subjects", {
      id: uid(),
      name: name,
      price: price || null,
      branch_id: myBranchId() || null
    }), document.getElementById("ns-name").value = "", document.getElementById("ns-price").value = "", 
    mkToast("Додано");
  } catch (e) {} else mkToast("Введіть назву", "error");
}

async function delSubj(id) {
  if (confirm("Видалити предмет?")) try {
    await dbDelete("subjects", id), mkToast("Видалено");
  } catch (e) {}
}

function addBranch() {
  openAddBranchModal();
}

async function delBranch(id) {
  if (confirm("Видалити філію?")) try {
    await dbDelete("branches", id), S.currentBranchId === id && (S.currentBranchId = null), 
    mkToast("Видалено");
  } catch (e) {}
}

async function editBranch(id) {
  var b = (S.branches || []).find(function(x) {
    return x.id === id;
  });
  if (b) {
    var nm = prompt("Назва філії:", b.name);
    if (nm) {
      b = prompt("Адреса:", b.address || "");
      try {
        await dbUpdate("branches", id, {
          name: nm,
          address: b
        }), mkToast("Оновлено");
      } catch (e) {}
    }
  }
}

async function savePriceRule() {
  var name = (document.getElementById("pr-name")?.value || "").trim(), price = parseFloat(document.getElementById("pr-price")?.value || 0);
  if (name && price) {
    var editId = document.getElementById("pr-edit-id")?.value || "", name = {
      name: name,
      price: price,
      subject_match: document.getElementById("pr-subj")?.value || "",
      tutor_id: document.getElementById("pr-tutor")?.value || "",
      grade_match: document.getElementById("pr-grade")?.value || "",
      dur_min: parseInt(document.getElementById("pr-dur")?.value) || null,
      branch_id: myBranchId() || null
    };
    try {
      editId ? (await dbUpdate("pricing_rules", editId, name), mkToast("Оновлено")) : (await dbInsert("pricing_rules", Object.assign({
        id: uid()
      }, name)), mkToast("Правило додано")), [ "pr-name", "pr-price", "pr-subj", "pr-grade", "pr-dur" ].forEach(function(f) {
        (f = document.getElementById(f)) && (f.value = "");
      });
      var pt = document.getElementById("pr-tutor"), pi = (pt && (pt.value = ""), 
      document.getElementById("pr-edit-id")), pb = (pi && (pi.value = ""), document.getElementById("pr-save-btn"));
      pb && (pb.textContent = "+ Додати правило");
    } catch (e) {}
  } else mkToast("Назва та ціна обов'язкові", "error");
}

async function editPriceRule(id) {
  var set, r = (S.pricingRules || []).find(function(x) {
    return x.id === id;
  });
  r && ((set = function(elId, val) {
    (elId = document.getElementById(elId)) && (elId.value = val || "");
  })("pr-name", r.name), set("pr-price", r.price), set("pr-subj", r.subjectMatch || r.subject_match), 
  set("pr-grade", r.gradeMatch || r.grade_match), set("pr-dur", r.durMin || r.dur_min), 
  set("pr-edit-id", r.id), (set = document.getElementById("pr-tutor")) && (set.value = r.tutorId || r.tutor_id || ""), 
  set = document.getElementById("pr-save-btn")) && (set.textContent = "ὋE Зберегти зміни");
}

async function delPriceRule(id) {
  if (confirm("Видалити правило?")) try {
    await dbDelete("pricing_rules", id), mkToast("Видалено");
  } catch (e) {}
}

async function renderUsers() {
  var users, list = document.getElementById("ut-list");
  list && (list.innerHTML = '<div class="empty"><div class="ei">⏳</div>Завантаження…</div>', 
  users = (await _sb.from("profiles").select("*")).data, S.users = users || [], 
  list.innerHTML = "", (users || []).forEach(function(u) {
    var ab, id, db, ro = ROLES[u.role] || ROLES.tutor, canEdit = "god" === R() || "director" === R() && "god" !== u.role, canDel = "god" === R() && u.id !== CU?.id || "director" === R() && "god" !== u.role && u.id !== CU?.id, row = document.createElement("div"), av = (row.className = "ulr", 
    document.createElement("div")), info = (av.className = "av uav", av.style.cssText = "background:" + ro.avatarBg + ";width:38px;height:38px;font-size:14px;font-weight:700;flex-shrink:0;color:#fff", 
    av.textContent = (u.fn?.[0] || "?") + (u.ln?.[0] || ""), document.createElement("div")), lastSeenStr = (info.className = "uin", 
    u.is_online, u.last_seen ? formatLastSeen(u.last_seen) : "ніколи");
    info.innerHTML = '<div class="uinn" style="display:flex;align-items:center;gap:5px"><span style="width:8px;height:8px;border-radius:50%;background:' + (u.is_online ? "#22c55e" : "#94a3b8") + ';flex-shrink:0;display:inline-block"></span>' + u.fn + " " + (u.ln || "") + '</div><div class="uinm">' + (u.email || "—") + '</div> <div style="font-size:10px;color:var(--t3)">' + (u.is_online ? "• Онлайн" : "Вхід: " + lastSeenStr) + "</div>", 
    (lastSeenStr = document.createElement("span")).className = "rpill " + u.role, 
    lastSeenStr.innerHTML = ro.icon + " " + ro.label, (ro = document.createElement("div")).style.cssText = "display:flex;gap:6px;margin-left:auto;align-items:center", 
    canEdit && ((canEdit = document.createElement("button")).className = "btn btn-g btn-sm", 
    canEdit.innerHTML = "✏️", id = u.id, canEdit.onclick = function() {
      openUserM(id);
    }, ro.appendChild(canEdit), (ab = document.createElement("button")).className = "btn btn-p btn-sm", 
    ab.textContent = "ὑ0 Доступ", (id => {
      ab.onclick = function() {
        openUserAccessM(id);
      };
    })(u.id), ro.appendChild(ab)), canDel && ((db = document.createElement("button")).className = "btn btn-sm btn-d", 
    db.innerHTML = "Ὕ1", (id => {
      db.onclick = function() {
        delUser(id);
      };
    })(u.id), ro.appendChild(db)), row.appendChild(av), row.appendChild(info), row.appendChild(lastSeenStr), 
    row.appendChild(ro), list.appendChild(row);
  }), users?.length || (list.innerHTML = '<div class="empty"><div class="ei">὆4</div>Немає акаунтів</div>'));
}

async function openUserM(id) {
  S.editId = id;
  var u = (S.users || []).find(function(x) {
    return x.id === id;
  });
  u && (document.getElementById("mu-title").textContent = "Редагувати акаунт", document.getElementById("u-fn").value = u.fn || "", 
  document.getElementById("u-ln").value = u.ln || "", document.getElementById("u-email").value = u.email || "", 
  document.getElementById("u-role").value = u.role || "tutor", toggleTutLink(), 
  popSel("u-tlink", S.tutors, "id", function(t) {
    return t.fn + " " + t.ln;
  }, "Прив'язати до викладача"), (u = (S.tutors || []).find(function(t) {
    return t.acc_uid === id || t.accId === id;
  })) && (document.getElementById("u-tlink").value = u.id), openM("mo-user"));
}

async function saveUser() {
  if (S.editId) {
    var fn = document.getElementById("u-fn").value.trim(), ln = document.getElementById("u-ln").value.trim(), role = document.getElementById("u-role").value;
    try {
      await dbUpdate("profiles", S.editId, {
        fn: fn,
        ln: ln,
        role: role
      });
      var tutorId = document.getElementById("u-tlink")?.value;
      "tutor" === role && tutorId && await _sb.from("tutors").update({
        acc_uid: S.editId
      }).eq("id", tutorId), CU?.id === S.editId && (CU = Object.assign({}, CU, {
        fn: fn,
        ln: ln,
        role: role
      }), updateSBUser(), buildSidebar()), mkToast("Оновлено"), closeM("mo-user"), 
      S.editId = null, renderUsers();
    } catch (e) {
      mkToast("Помилка збереження: " + (e.message || e), "error");
    }
  }
}

async function delUser(id) {
  if (id === CU?.id) mkToast("Не можна видалити свій акаунт", "error"); else if (confirm("Видалити акаунт?")) try {
    await dbDelete("profiles", id), mkToast("Видалено"), renderUsers();
  } catch (e) {}
}

var _uaUserId = null;

async function openUserAccessM(id) {
  var u = (S.users || []).find(function(x) {
    return x.id === id;
  });
  u ? (_uaUserId = id, document.querySelectorAll(".ua-tab").forEach(function(t, i) {
    t.classList.toggle("active", 0 === i);
  }), document.querySelectorAll(".ua-panel").forEach(function(p, i) {
    p.classList.toggle("active", 0 === i);
  }), buildUAHeader(u), buildUAPerms(u), buildUANav(u), buildUASummary(u), openM("mo-user-access")) : mkToast("Не знайдено", "error");
}

async function uaPermChange(key, val, roleDefault) {
  var u = (S.users || []).find(function(x) {
    return x.id === _uaUserId;
  });
  if (u) {
    (u = JSON.parse(JSON.stringify(u.perms || {}))).can || (u.can = {}), val === roleDefault ? delete u.can[key] : u.can[key] = val, 
    Object.keys(u.can).length || delete u.can;
    try {
      await dbUpdate("profiles", _uaUserId, {
        perms: u
      }), CU?.id === _uaUserId && (CU.perms = u);
    } catch (e) {}
    (roleDefault = (S.users || []).find(function(x) {
      return x.id === _uaUserId;
    })) && (roleDefault.perms = u, buildUASummary(roleDefault));
  }
}

async function uaResetPerm(key) {
  var u = (S.users || []).find(function(x) {
    return x.id === _uaUserId;
  });
  if (u) {
    (u = JSON.parse(JSON.stringify(u.perms || {}))).can && (delete u.can[key], Object.keys(u.can).length || delete u.can);
    try {
      await dbUpdate("profiles", _uaUserId, {
        perms: u
      }), CU?.id === _uaUserId && (CU.perms = u);
    } catch (e) {}
    (key = (S.users || []).find(function(x) {
      return x.id === _uaUserId;
    })) && (key.perms = u, buildUAPerms(key), buildUASummary(key));
  }
}

async function uaNavChange(pageId, show, isInRole) {
  var u = (S.users || []).find(function(x) {
    return x.id === _uaUserId;
  });
  if (u) {
    (u = JSON.parse(JSON.stringify(u.perms || {}))).hideNav || (u.hideNav = []), 
    u.showNav || (u.showNav = []), show ? (u.hideNav = u.hideNav.filter(function(p) {
      return p !== pageId;
    }), isInRole || u.showNav.includes(pageId) || u.showNav.push(pageId)) : (u.showNav = u.showNav.filter(function(p) {
      return p !== pageId;
    }), isInRole && !u.hideNav.includes(pageId) && u.hideNav.push(pageId)), u.hideNav.length || delete u.hideNav, 
    u.showNav.length || delete u.showNav;
    try {
      await dbUpdate("profiles", _uaUserId, {
        perms: u
      }), CU?.id === _uaUserId && (CU.perms = u, buildSidebar());
    } catch (e) {}
    (show = (S.users || []).find(function(x) {
      return x.id === _uaUserId;
    })) && (show.perms = u, buildUASummary(show));
  }
}

async function resetAllUserAccess() {
  var u = (S.users || []).find(function(x) {
    return x.id === _uaUserId;
  });
  if (confirm("Скинути налаштування для " + (u?.fn || "") + " " + (u?.ln || "") + " ?")) try {
    await dbUpdate("profiles", _uaUserId, {
      perms: {}
    }), CU?.id === _uaUserId && (CU.perms = {}, buildSidebar());
    var u2 = (S.users || []).find(function(x) {
      return x.id === _uaUserId;
    });
    u2 && (u2.perms = {}, buildUAPerms(u2), buildUANav(u2), buildUASummary(u2)), 
    renderUsers(), mkToast("Скинуто");
  } catch (e) {}
}

function setBranch(id) {
  S.currentBranchId = id || null, updateBranchSelector(), renderSch && renderSch(), 
  nav(S.currentPage || "dashboard");
}

async function clearData(what) {
  if ("god" !== R()) mkToast("Тільки Бог", "error"); else if (confirm("Видалити " + what + " дані? Це незворотно!")) {
    var toDelete = {
      lessons: [ "lessons" ],
      payments: [ "payments" ],
      all: [ "comms", "payments", "lessons", "students", "tutors" ]
    }[what] || [];
    try {
      for (var _ti = 0; _ti < toDelete.length; _ti++) await _sb.from(toDelete[_ti]).delete().neq("id", "");
      mkToast("Очищено");
    } catch (e) {
      mkToast("Помилка", "error");
    }
  }
}

async function startApp() {
  startPresence(), document.getElementById("ls").style.display = "none", document.getElementById("as").style.display = "block", 
  await loadAll(), startChannels(), buildSidebar(), updateSBUser(), updateBranchSelector(), 
  document.body.className = document.body.className.replace(/\brole-\w+\b/g, ""), 
  document.body.classList.add("role-" + (CU ? CU.role : "tutor"));
  var lastPage = "";
  try {
    lastPage = localStorage.getItem("sb_page") || "";
  } catch (e) {}
  var allowedPages = userNav(), allowedPages = lastPage && 0 <= allowedPages.indexOf(lastPage) ? lastPage : "dashboard";
  try {
    nav(allowedPages);
  } catch (e) {
    nav("dashboard");
  }
  loadAll().then(function() {
    buildSidebar(), updateSBUser();
    var pg = S.currentPage;
    if (pg) try {
      "dashboard" === pg ? renderDash() : "students" === pg ? renderStudents() : "tutors" === pg ? renderTutors() : "schedule" === pg ? renderSch() : "lessons" === pg ? renderLessons() : "payments" === pg ? renderPayments() : "reports" === pg ? renderReports() : "users" === pg ? renderUsers() : "settings" === pg ? renderSettings() : "profile" === pg ? renderProfile() : "crm" === pg && renderCrm();
    } catch (e) {}
  }).catch(function() {});
}

function lStdSearch(val) {
  var q, matches, dl = document.getElementById("l-std-list"), hidden = document.getElementById("l-std");
  dl && (q = val.toLowerCase(), matches = (S.students || []).filter(function(s) {
    return (s.fn + " " + s.ln).toLowerCase().includes(q) || (s.ln + " " + s.fn).toLowerCase().includes(q);
  }).slice(0, 20), dl.innerHTML = matches.map(function(s) {
    return '<option value="' + s.fn + " " + s.ln + '" data-id="' + s.id + '">';
  }).join(""), (dl = (S.students || []).find(function(s) {
    return s.fn + " " + s.ln === val || s.ln + " " + s.fn === val;
  })) && hidden ? hidden.value = dl.id : 1 === matches.length && hidden ? hidden.value = matches[0].id : hidden && (hidden.value = ""));
}

function openStudM(id = null) {
  if (can("students")) {
    S.editId = id, document.getElementById("ms-title").textContent = id ? "Редагувати учня" : "Новий учень";
    (dl_s = document.getElementById("subj-list-s")) && (dl_s.innerHTML = (S.subjects || []).map(function(x) {
      return '<option value="' + x.name + '">';
    }).join(""));
    var _tIds, dl_s = document.getElementById("s-tutor-list"), stSel = ((stSel = document.getElementById("s-tutor")) && (stSel.innerHTML = S.tutors.map(function(t) {
      return '<option value="' + t.id + '">' + t.fn + " " + t.ln + "</option>";
    }).join("")), dl_s && (dl_s.innerHTML = S.tutors.map(function(t) {
      return '<label style="display:flex;align-items:center;gap:6px;cursor:pointer;padding:4px 10px;border:1px solid var(--b1);border-radius:20px;background:var(--s1);font-size:12px;user-select:none"><input type="checkbox" class="st-tutor-cb" value="' + t.id + '" style="accent-color:var(--adm)">' + mkAv(t.fn, t.ln, 20) + "<span>" + t.fn + " " + t.ln + "</span></label>";
    }).join("")), [ "fn", "ln", "age", "grade", "phone", "email", "notes" ]);
    if (id) {
      let s = S.students.find(x => x.id === id);
      s && (stSel.forEach(f => {
        var el = document.getElementById("s-" + f);
        el && (el.value = s[f] || "");
      }), document.getElementById("s-subj").value = s.subject || "", _tIds = s.tutorIds || (s.tutorId ? [ s.tutorId ] : []), 
      document.querySelectorAll(".st-tutor-cb").forEach(function(cb) {
        cb.checked = 0 <= _tIds.indexOf(cb.value), cb.closest("label").style.background = cb.checked ? "rgba(41,171,226,.15)" : "var(--s1)", 
        cb.closest("label").style.borderColor = cb.checked ? "var(--adm)" : "var(--b1)";
      }), document.getElementById("s-status").value = s.status || "active", document.getElementById("s-src").value = s.src || "referral", 
      (dl_s = document.getElementById("s-parent-fn")) && (dl_s.value = s.parentFn || ""), 
      dl_s = document.getElementById("s-parent-phone")) && (dl_s.value = s.parentPhone || "");
    } else stSel.forEach(f => {
      (f = document.getElementById("s-" + f)) && (f.value = "");
    }), [].forEach(f => {
      (f = document.getElementById("s-" + f)) && (f.value = "");
    }), document.getElementById("s-status").value = "active", document.getElementById("s-src").value = "referral";
    (dl_s = document.getElementById("s-branch")) && (dl_s.innerHTML = '<option value="">— головна —</option>' + (S.branches || []).map(function(b) {
      return '<option value="' + b.id + '">' + b.name + "</option>";
    }).join(""), id) && (stSel = (S.students || []).find(function(x) {
      return x.id === id;
    })) && (dl_s.value = stSel.branchId || ""), renderCustomFields("student", "mo-student-cf"), 
    (dl_s = document.getElementById("inv-btn")) && (dl_s.style.display = !id || "god" !== R() && "director" !== R() ? "none" : "inline-flex"), 
    openM("mo-student");
  } else mkToast("Немає прав", "error");
}

function openTutM(id = null) {
  if (can("tutors")) {
    if (S.editId = id, document.getElementById("mt-title").textContent = id ? "Редагувати викладача" : "Новий викладач", 
    id) {
      let t = S.tutors.find(x => x.id === id);
      t && ([ "fn", "ln", "phone", "email", "bio" ].forEach(f => {
        var el = document.getElementById("t-" + f);
        el && (el.value = t[f] || "");
      }), document.getElementById("t-subj").value = t.subj || "", document.getElementById("t-rate").value = t.rate || "");
    } else [ "fn", "ln", "phone", "email", "subj", "rate", "bio" ].forEach(f => {
      (f = document.getElementById("t-" + f)) && (f.value = "");
    });
    renderCustomFields("tutor", "mo-tutor-cf");
    var _t, tBranchSel = document.getElementById("t-branch");
    tBranchSel && (tBranchSel.innerHTML = '<option value="">— головна —</option>' + (S.branches || []).map(function(b) {
      return '<option value="' + b.id + '">' + b.name + "</option>";
    }).join(""), id) && (_t = (S.tutors || []).find(function(x) {
      return x.id === id;
    })) && (tBranchSel.value = _t.branchId || ""), openM("mo-tutor");
  } else mkToast("Немає прав", "error");
}

function openLessM(id = null, date = null, time = null) {
  if (can("lessons")) {
    if (S.editId = id, document.getElementById("ml-title").textContent = id ? "Редагувати заняття" : "Нове заняття", 
    popSel("l-std", myStudents(), "id", function(s) {
      return s.fn + " " + s.ln;
    }, "Оберіть учня"), (dl_l = document.getElementById("subj-list-l")) && (dl_l.innerHTML = (S.subjects || []).map(function(x) {
      return '<option value="' + x.name + '">';
    }).join("")), popSel("l-tutor", S.tutors, "id", function(t) {
      return t.fn + " " + t.ln;
    }, "Викладач"), document.getElementById("l-recur").value = "none", document.getElementById("l-recur-end").value = "", 
    document.getElementById("l-recur-count").value = "", document.getElementById("l-recur-interval").value = "7", 
    document.getElementById("recur-preview").style.display = "none", toggleRecurOpts(), 
    id) {
      let l = S.lessons.find(x => x.id === id);
      l && (document.getElementById("l-std").value = l.studentId || "", (dl_l = (S.students || []).find(function(x) {
        return x.id === l.studentId;
      })) && (sf = document.getElementById("l-std-search")) && (sf.value = dl_l.fn + " " + dl_l.ln), 
      document.getElementById("l-subj").value = l.subject || "", document.getElementById("l-tutor").value = l.tutorId || "", 
      document.getElementById("l-date").value = l.date || "", document.getElementById("l-time").value = l.time || "10:00", 
      document.getElementById("l-dur").value = l.dur || 60, document.getElementById("l-stat").value = l.status || "planned", 
      document.getElementById("l-price").value = l.price || "", document.getElementById("l-notes").value = l.notes || "", 
      (sf = document.getElementById("l-hw")) && (sf.value = l.hw || ""), (dl_l = document.getElementById("l-missed-date")) && (dl_l.value = l.missed_date || ""), 
      (sf = document.getElementById("l-makeup-date")) && (sf.value = l.makeup_date || ""), 
      "function" == typeof onLessStatChange && onLessStatChange(), l.recurId) && (dl_l = S.lessons.filter(x => x.recurId === l.recurId), 
      (sf = document.getElementById("recur-preview")).style.display = "block", sf.innerHTML = '<span style="color:var(--adm)">ὐ1 Повторюване заняття</span> — серія з <b>' + dl_l.length + "</b> занять. Редагування змінює тільки <b>це</b> заняття.");
    } else {
      [ "l-std", "l-subj", "l-tutor", "l-price", "l-notes" ].forEach(f => document.getElementById(f).value = ""), 
      document.getElementById("l-date").value = date || localDateStr(new Date()), 
      document.getElementById("l-time").value = time || "10:00", document.getElementById("l-dur").value = 60, 
      document.getElementById("l-stat").value = "planned";
      var dl_l, sf = document.getElementById("l-std-search");
      sf && (sf.value = ""), (dl_l = document.getElementById("l-missed-date")) && (dl_l.value = ""), 
      (date = document.getElementById("l-makeup-date")) && (date.value = ""), "function" == typeof onLessStatChange && onLessStatChange(), 
      (time = myTutor()) && (document.getElementById("l-tutor").value = time.id);
    }
    renderCustomFields("lesson", "mo-lesson-cf"), (sf = document.getElementById("del-lesson-btn")) && (sf.style.display = id && can("lessons") ? "inline-flex" : "none"), 
    openM("mo-lesson");
  } else mkToast("Немає прав", "error");
}

function openPayM(id = null) {
  var months, p;
  can("payments") ? (S.editId = id, document.getElementById("mp-title").textContent = id ? "Редагувати платіж" : "Новий платіж", 
  popSel("p-std", S.students, "id", function(s) {
    return s.fn + " " + s.ln;
  }, "Оберіть учня"), months = [ "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень", "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень" ], 
  document.getElementById("p-date").value = localDateStr(new Date()), document.getElementById("p-mon").value = months[new Date().getMonth()], 
  id ? (p = S.payments.find(x => x.id === id)) && (setStudentSearch("p-std", p.studentId || ""), 
  document.getElementById("p-amt").value = p.amount || "", document.getElementById("p-mth").value = p.method || "cash", 
  document.getElementById("p-date").value = p.date || "", document.getElementById("p-stat").value = p.status || "paid", 
  document.getElementById("p-mon").value = p.month || months[new Date().getMonth()], 
  document.getElementById("p-note").value = p.note || "") : (document.getElementById("p-std").value = "", 
  document.getElementById("p-amt").value = "", document.getElementById("p-mth").value = "cash", 
  document.getElementById("p-stat").value = "paid", document.getElementById("p-note").value = ""), 
  renderCustomFields("payment", "mo-payment-cf"), openM("mo-payment")) : mkToast("Немає прав", "error");
}

function openCommM(tutorId) {
  var _myT;
  can("lessons") ? document.getElementById("mo-comm") && ("function" != typeof R || "tutor" !== R() || tutorId || (_myT = (S.tutors || []).find(function(t) {
    return CU && (t.accId === CU.id || t.acc_uid === CU.id);
  })) && (tutorId = _myT.id), (_myT = document.getElementById("cm-tutor-wrap")) && (_myT.style.display = "function" == typeof R && "tutor" === R() ? "none" : ""), 
  (_myT = document.getElementById("cm-tutor")) && (_myT.innerHTML = '<option value="">Оберіть репетитора</option>' + S.tutors.map(function(t) {
    return '<option value="' + t.id + '"' + (t.id === tutorId ? " selected" : "") + ">" + t.fn + " " + t.ln + "</option>";
  }).join("")), (_myT = document.getElementById("cm-student")) && (_myT.innerHTML = '<option value="">Учень (необов\'язково)</option>' + S.students.map(function(s) {
    return '<option value="' + s.id + '">' + s.fn + " " + s.ln + "</option>";
  }).join("")), (_myT = document.getElementById("cm-date")) && (_myT.value = localDateStr(new Date())), 
  openM("mo-comm")) : mkToast("Немає прав", "error");
}

function nav(page) {
  window.innerWidth < 900 && (sb = document.querySelector(".sb"), overlay = document.getElementById("sb-overlay"), 
  sb && sb.classList.remove("open"), overlay && overlay.classList.remove("open"), 
  document.body.classList.remove("sb-open"));
  var sb = page.startsWith("custom_");
  if (sb || userNav().includes(page)) {
    document.querySelectorAll(".page").forEach(p => p.classList.remove("active")), 
    document.querySelectorAll(".ni").forEach(n => n.classList.remove("active")), 
    (overlay = document.getElementById("pg-" + page)) && overlay.classList.add("active"), 
    (overlay = document.getElementById("ni-" + page)) && (overlay.classList.add("active"), 
    overlay.className = overlay.className.replace(/ (god|dir|tut)/g, ""), "god" === R() ? overlay.classList.add("god") : "director" === R() ? overlay.classList.add("dir") : "tutor" === R() && overlay.classList.add("tut")), 
    document.getElementById("ptitle").textContent = (PLABELS[page] || page) + "", 
    S.currentPage = page;
    try {
      localStorage.setItem("sb_page", page);
    } catch (e) {}
    var overlay = {
      students: "Додати учня",
      tutors: "Додати викладача",
      lessons: "Додати заняття",
      payments: "Додати платіж",
      schedule: "Додати заняття",
      users: "Додати акаунт"
    }, ab = document.getElementById("addbtn");
    if (overlay[page] && can("users" === page ? "users" : "students" === page ? "students" : "tutors" === page ? "tutors" : "payments" === page ? "payments" : "lessons") ? (ab.textContent = "+ " + overlay[page], 
    ab.style.display = "flex") : ab.style.display = "none", "dashboard" === page && renderDash(), 
    "students" === page && renderStudents(), "tutors" === page && renderTutors(), 
    "schedule" === page && renderSch(), "lessons" === page && renderLessons(), "payments" === page && renderPayments(), 
    "reports" === page && renderReports(), "branches" === page && (renderBranches(), 
    renderBranchStats()), "users" === page && renderUsers(), "settings" === page && renderSettings(), 
    "profile" === page) try {
      renderProfile();
    } catch (e) {}
    var overlay = document.getElementById("pg-invoice"), ab = document.getElementById("pg-comms"), _ctf = ("comms" === page ? (ab && (ab.style.display = "block"), 
    document.getElementById("comm-f-student"), document.getElementById("comm-f-student-search") && populateStudentSearch("comm-f-student", myStudents()), 
    (_ctf = document.getElementById("comm-f-tutor")) && _ctf.options.length <= 1 && (_ctf.innerHTML = "<option value=>Усі репетитори</option>" + (S.tutors || []).map(function(t) {
      return "<option value=+t.id+>" + t.fn + " " + t.ln + "</option>";
    }).join(""))) : ab && (ab.style.display = "none"), document.getElementById("pg-missed")), ab = ("missed" === page ? (_ctf && (_ctf.style.display = "block"), 
    document.getElementById("missed-f-student") && populateStudentSearch("missed-f-student", myStudents())) : _ctf && (_ctf.style.display = "none"), 
    document.getElementById("pg-invoice-log")), _ctf = ("invoice-log" === page ? ab && (ab.style.display = "block", 
    renderInvoiceLog()) : ab && (ab.style.display = "none"), "invoice" === page ? (overlay && (overlay.style.display = "block"), 
    renderInvoicePage()) : overlay && (overlay.style.display = "none"), document.getElementById("pg-crm"));
    "crm" === page ? (_ctf && (_ctf.style.display = "flex"), renderCrm()) : _ctf && (_ctf.style.display = "none"), 
    "analytics" === page && renderAnalytics(), sb && renderCustomPage(page), window.innerWidth <= 768 && closeSidebar();
  } else mkToast("Немає доступу до цього розділу", "error");
}

function openAdd() {
  var p = S.currentPage;
  "students" === p ? openStudM() : "tutors" === p ? openTutM() : "lessons" === p || "schedule" === p ? openLessM() : "payments" === p ? openPayM() : "users" === p && openUserM();
}

function chWk(d) {
  "day" === (S.schView || "week") ? S.dayOffset = 0 === d ? 0 : (S.dayOffset || 0) + d : S.weekOffset = 0 === d ? 0 : (S.weekOffset || 0) + d, 
  renderSch();
}

function schSetView(v) {
  void 0 === S.weekOffset && (S.weekOffset = 0), void 0 === S.dayOffset && (S.dayOffset = 0), 
  "week" === (S.schView = v) ? S.weekOffset = S.weekOffset || 0 : S.dayOffset = S.dayOffset || 0, 
  renderSch();
}

function toggleRecurOpts() {
  var v = document.getElementById("l-recur").value, none = "none" === v;
  document.getElementById("recur-interval-wrap").style.display = "custom" === v ? "flex" : "none", 
  document.getElementById("recur-end-wrap").style.display = none ? "none" : "flex", 
  document.getElementById("recur-count-wrap").style.display = none ? "none" : "flex", 
  document.getElementById("recur-preview").style.display = "none", document.getElementById("recur-preview-btn").style.display = none ? "none" : "flex";
}

function previewRecur() {
  var date = document.getElementById("l-date").value, recur = document.getElementById("l-recur").value, endDate = document.getElementById("l-recur-end").value, count = document.getElementById("l-recur-count").value, interval = document.getElementById("l-recur-interval").value;
  date && "none" !== recur && (date = [ date, ...genRecurDates(date, recur, endDate, count || 52, interval) ], 
  endDate = {
    daily: "Щодня",
    weekly: "Щотижня",
    biweekly: "Через тиждень",
    monthly: "Щомісяця (дата)",
    "monthly-dow": "Щомісяця (день тижня)",
    custom: "Кожні " + interval + " днів"
  }, (count = document.getElementById("recur-preview")).style.display = "block", 
  count.innerHTML = '<div style="color:var(--adm);font-weight:600;margin-bottom:6px">ὐ1 ' + endDate[recur] + " — " + date.length + " занять:</div>" + date.slice(0, 10).map(d => '<span style="display:inline-block;background:var(--s1);border:1px solid var(--b1);border-radius:5px;padding:2px 8px;margin:2px;font-family:JetBrains Mono,monospace;font-size:11px">' + fd(d) + "</span>").join("") + (10 < date.length ? '<span style="margin-left:4px;color:var(--t3)">+' + (date.length - 10) + " ще...</span>" : ""));
}

function uaTab(id, el) {
  document.querySelectorAll(".ua-tab").forEach(function(t) {
    t.classList.remove("active");
  }), document.querySelectorAll(".ua-panel").forEach(function(p) {
    p.classList.remove("active");
  }), el.classList.add("active"), document.getElementById("uap-" + id).classList.add("active");
}

function toggleTutLink() {
  var r = document.getElementById("u-role").value;
  document.getElementById("u-tlink-wrap").style.display = "tutor" === r ? "flex" : "none";
}

function toggleProfileEdit() {
  var mt, set, form = document.getElementById("pr-edit-form");
  form && (mt = myTutor(), "none" === form.style.display ? (mt && ((set = function(id, val) {
    (id = document.getElementById(id)) && (id.value = val || "");
  })("pr-fn", mt.fn), set("pr-ln", mt.ln), set("pr-phone", mt.phone), set("pr-email", mt.email), 
  set("pr-subj", mt.subj), set("pr-rate", mt.rate), set("pr-bio", mt.bio)), form.style.display = "block") : form.style.display = "none");
}

async function saveProfileEdit() {
  var mt = myTutor();
  if (mt) {
    var get = function(id) {
      return (id = document.getElementById(id)) ? id.value.trim() : "";
    };
    if ((get = {
      fn: get("pr-fn"),
      ln: get("pr-ln"),
      phone: get("pr-phone"),
      email: get("pr-email"),
      subj: get("pr-subj"),
      rate: get("pr-rate") || null,
      bio: get("pr-bio")
    }).fn) try {
      await dbUpdate("tutors", mt.id, get), CU && (await dbUpdate("profiles", CU.id, {
        fn: get.fn,
        ln: get.ln
      }), CU = Object.assign({}, CU, {
        fn: get.fn,
        ln: get.ln
      }), updateSBUser()), mkToast("Профіль оновлено"), document.getElementById("pr-edit-form").style.display = "none", 
      renderProfile();
    } catch (e) {
      mkToast("Помилка: " + (e.message || e), "error");
    } else mkToast("Ім'я обов'язкове", "error");
  } else mkToast("Профіль репетитора не знайдено", "error");
}

function buildSidebar() {
  let cfg = S.godConfig || {}, navItems = cfg.navItems ? [ ...cfg.navItems ] : [ ...NAV_CFG ], role = R(), allowed = userNav(), html = "", lastSec = "";
  navItems.forEach(n => {
    var _mt, isBuiltin = allowed.includes(n.id), isCustom = n.custom, roleAllowed = (n.roles || []).includes(role);
    !isBuiltin && !isCustom || isCustom && !roleAllowed || (n.sec !== lastSec && (html += '<div class="nsec">' + n.sec + "</div>", 
    lastSec = n.sec), html += '<div class="ni" id="ni-' + n.id + '" onclick="nav(\'' + n.id + '\')"><span class="nico">' + n.ico + "</span>" + n.lbl + (n.badge ? '<span class="nbadge" id="nb-s">' + (("function" == typeof R && "tutor" === R() ? (_mt = (S.tutors || []).find(function(t) {
      return CU && (t.accId === CU.id || t.acc_uid === CU.id);
    })) ? (S.students || []).filter(function(s) {
      return (s.tutorId === _mt.id || s.tutor_id === _mt.id) && "active" === s.status;
    }).length : 0 : (S.students || []).filter(function(s) {
      return "active" === s.status;
    }).length) || "") + "</span>" : "") + "</div>");
  }), document.getElementById("sbnav").innerHTML = html;
}

function buildUAHeader(u) {
  var wrap, av, info, ro = ROLES[u.role], el = document.getElementById("ua-user-info");
  el && (el.innerHTML = "", (wrap = document.createElement("div")).style.cssText = "display:flex;align-items:center;gap:12px;margin-bottom:10px", 
  (av = document.createElement("div")).className = "av", av.style.cssText = "background:" + ro.avatarBg + ";width:44px;height:44px;font-size:17px;font-weight:700;color:#fff;flex-shrink:0", 
  av.textContent = (u.fn[0] || "") + (u.ln[0] || ""), (info = document.createElement("div")).innerHTML = '<div style="font-weight:700;font-size:15px">' + u.fn + " " + u.ln + '</div><div style="font-size:12px;color:var(--t2);margin-top:2px">@' + u.login + ' &bull; <span class="rpill ' + u.role + '" style="font-size:10px;padding:1px 8px">' + ro.icon + " " + ro.label + "</span></div>", 
  wrap.appendChild(av), wrap.appendChild(info), (u = document.createElement("div")).style.cssText = "font-size:11px;color:var(--t3);padding:8px 12px;background:var(--s2);border:1px solid var(--b1);border-radius:8px;line-height:1.5", 
  u.innerHTML = '⚡ Роль визначає <strong style="color:var(--t1)">базові</strong> права. Тут можна додати або зняти доступ для <strong style="color:var(--dir)">цього конкретного акаунту</strong>.', 
  el.appendChild(wrap), el.appendChild(u));
}

function buildUANav(u) {
  var roleNav = ROLES[u.role].nav || [], hideNav = (u = u.perms || {}).hideNav || [], showNav = u.showNav || [], el = document.getElementById("ua-nav-grid");
  el && (el.innerHTML = "", UA_PAGES.forEach(function(pg) {
    var pageId, isInRole, inRole = roleNav.includes(pg.id), isOn = inRole && !hideNav.includes(pg.id) || showNav.includes(pg.id), item = document.createElement("div"), cb = (item.className = "ua-nav-item" + (isOn ? " checked" : ""), 
    document.createElement("input"));
    cb.type = "checkbox", cb.checked = isOn, pageId = pg.id, isInRole = inRole, 
    cb.addEventListener("change", function() {
      uaNavChange(pageId, this.checked, isInRole), this.closest(".ua-nav-item").classList.toggle("checked", this.checked);
    });
    (isOn = document.createElement("span")).className = "ua-nav-ico", isOn.textContent = pg.ico;
    var info = document.createElement("div");
    info.style.flex = "1", info.innerHTML = '<div class="ua-nav-lbl">' + pg.lbl + '</div><div class="ua-nav-sec">' + pg.sec + (inRole ? " · є в ролі" : " · не в ролі") + "</div>", 
    item.appendChild(cb), item.appendChild(isOn), item.appendChild(info), el.appendChild(item);
  }));
}

function buildUAPerms(u) {
  var ro = ROLES[u.role], roleCan = ro.can || {}, custCan = (u.perms || {}).can || {}, el = document.getElementById("ua-perms-grid");
  el && (el.innerHTML = "", UA_PERMS.forEach(function(p) {
    var key, roleVal = !(!roleCan[p.k] && !ro[p.k]), hasOverride = p.k in custCan, effectiveVal = hasOverride ? custCan[p.k] : roleVal, item = document.createElement("div"), left = (item.className = "ua-perm-row", 
    document.createElement("div")), lbl = document.createElement("div"), sub = (lbl.className = "ua-perm-label", 
    lbl.textContent = p.lbl, document.createElement("div"));
    sub.style.cssText = "font-size:10px;margin-top:2px;display:flex;align-items:center;gap:4px", 
    hasOverride ? ((hasOverride = document.createElement("span")).style.color = "var(--dir)", 
    hasOverride.textContent = "⚙ індивідуально", (rb = document.createElement("button")).style.cssText = "background:none;border:none;color:var(--t3);cursor:pointer;font-size:10px;padding:0", 
    rb.textContent = "↺ скинути", key = p.k, rb.addEventListener("click", function() {
      uaResetPerm(key);
    }), sub.appendChild(hasOverride), sub.appendChild(rb)) : (sub.textContent = "з ролі: " + (roleVal ? "✅ так" : "❌ ні"), 
    sub.style.color = "var(--t3)"), left.appendChild(lbl), left.appendChild(sub);
    (hasOverride = document.createElement("label")).className = "toggle";
    var cb = document.createElement("input"), rb = (cb.type = "checkbox", cb.checked = effectiveVal, 
    ((key, rv) => {
      cb.addEventListener("change", function() {
        uaPermChange(key, this.checked, rv);
        var k, subEl = this.closest(".ua-perm-row").querySelector("div > div:last-child"), sp2 = (subEl.innerHTML = "", 
        subEl.style.cssText = "font-size:10px;margin-top:2px;display:flex;align-items:center;gap:4px", 
        document.createElement("span")), rb2 = (sp2.style.color = "var(--dir)", 
        sp2.textContent = "⚙ індивідуально", document.createElement("button"));
        rb2.style.cssText = "background:none;border:none;color:var(--t3);cursor:pointer;font-size:10px;padding:0", 
        rb2.textContent = "↺ скинути", k = key, rb2.addEventListener("click", function() {
          uaResetPerm(k);
        }), subEl.appendChild(sp2), subEl.appendChild(rb2);
      });
    })(p.k, roleVal), document.createElement("span"));
    rb.className = "toggle-slider", hasOverride.appendChild(cb), hasOverride.appendChild(rb), 
    item.appendChild(left), item.appendChild(hasOverride), el.appendChild(item);
  }));
}

function buildUASummary(u) {
  var ro, roleCan, custCan, html, hideNav, up, el = document.getElementById("ua-summary");
  el && (up = u.perms || {}, ro = ROLES[u.role], roleCan = ro.can || {}, custCan = up.can || {}, 
  hideNav = up.hideNav || [], up = up.showNav || [], html = "", Object.keys(custCan).length && (html += '<div style="font-weight:600;font-size:11px;color:var(--dir);margin-bottom:8px;text-transform:uppercase;letter-spacing:.5px">⚙ Індивідуальні права:</div><div style="display:flex;flex-direction:column;gap:4px;margin-bottom:12px">', 
  Object.keys(custCan).forEach(function(k) {
    var def = (def = UA_PERMS.find(function(p) {
      return p.k === k;
    })) ? def.lbl : k, rv = !(!roleCan[k] && !ro[k]);
    html += '<div style="display:flex;align-items:center;gap:8px;padding:6px 10px;background:var(--s2);border-radius:7px"><span>' + (custCan[k] ? "✅" : "❌") + '</span><span style="flex:1;font-size:12px">' + def + '</span><span style="font-size:10px;color:var(--t3)">роль: ' + (rv ? "✅" : "❌") + '</span><button class="ua-sum-reset" data-pkey="' + k + '" style="background:none;border:none;color:var(--t3);cursor:pointer;font-size:11px;padding:2px 4px">↺</button></div>';
  }), html += "</div>"), (hideNav.length || up.length) && (html += '<div style="font-weight:600;font-size:11px;color:var(--dir);margin-bottom:8px;text-transform:uppercase;letter-spacing:.5px">ὌB Навігація змінена:</div><div style="display:flex;flex-direction:column;gap:3px">', 
  hideNav.forEach(function(p) {
    var pg = UA_PAGES.find(function(x) {
      return x.id === p;
    });
    html += '<div style="font-size:12px;color:var(--danger)">❌ Приховано: ' + (pg ? pg.ico + " " + pg.lbl : p) + "</div>";
  }), up.forEach(function(p) {
    var pg = UA_PAGES.find(function(x) {
      return x.id === p;
    });
    html += '<div style="font-size:12px;color:var(--tut)">✅ Додано: ' + (pg ? pg.ico + " " + pg.lbl : p) + "</div>";
  }), html += "</div>"), html = html || '<div style="color:var(--t3);font-size:12px;padding:8px 0">Індивідуальних налаштувань немає — діють права ролі.</div>', 
  el.innerHTML = html, el.querySelectorAll(".ua-sum-reset").forEach(function(btn) {
    btn.addEventListener("click", function() {
      uaResetPerm(this.dataset.pkey);
    });
  }), hideNav = document.getElementById("ua-reset-all")) && (up = !(!u.perms || !(Object.keys(u.perms.can || {}).length || (u.perms.hideNav || []).length || (u.perms.showNav || []).length)), 
  hideNav.style.display = up ? "flex" : "none");
}

function genRecurDates(startDate, recurType, endDate, count, interval) {
  let dates = [], start = new Date(startDate + "T12:00:00"), end = endDate ? new Date(endDate + "T23:59:59") : null, maxCount = count ? Math.min(parseInt(count), 200) : 104, cur = new Date(start);
  for (let i = 0; i < maxCount && !(end && cur > end); i++) {
    dates.push(localDateStr(cur));
    var next = new Date(cur);
    if ("daily" === recurType) next.setDate(next.getDate() + 1); else if ("weekly" === recurType) next.setDate(next.getDate() + 7); else if ("biweekly" === recurType) next.setDate(next.getDate() + 14); else if ("monthly" === recurType) next.setMonth(next.getMonth() + 1); else if ("monthly-dow" === recurType) {
      var dow = start.getDay(), weekNum = Math.floor((start.getDate() - 1) / 7);
      for (next.setMonth(next.getMonth() + 1), next.setDate(1); next.getDay() !== dow; ) next.setDate(next.getDate() + 1);
      next.setDate(next.getDate() + 7 * weekNum);
    } else "custom" === recurType && next.setDate(next.getDate() + Math.max(1, parseInt(interval) || 7));
    if (end && end < next) break;
    if (cur = next, maxCount - 1 <= dates.length) break;
  }
  return dates;
}

function renderBranches() {
  var html, el = document.getElementById("branch-list");
  el && (html = "", (S.branches || []).forEach(function(b) {
    var bid = b.id, isActive = S.currentBranchId === bid, delBtn = 1 < S.branches.length ? '<button class="btn btn-sm btn-d" onclick="delBranch(this.dataset.id)" data-id="' + bid + '">Ὕ1</button>' : "";
    html += '<div class="ms"><div style="flex:1"><div style="font-weight:600;font-size:13px">' + (isActive ? "✅ " : "") + b.name + "</div>" + (b.address ? '<div style="font-size:11px;color:var(--t2)">' + b.address + "</div>" : "") + '</div><div style="display:flex;gap:6px"><button class="btn btn-g btn-sm" onclick="editBranch(this.dataset.id)" data-id="' + bid + '">✏️</button>' + delBtn + "</div></div>";
  }), el.innerHTML = html || '<div style="font-size:12px;color:var(--t3)">Немає філій</div>');
}

function renderPricingRules() {
  var rules, el = document.getElementById("pricing-rules-list");
  el && ((rules = S.pricingRules || []).length ? el.innerHTML = rules.map(function(r) {
    var t, tags = [];
    return r.subjectMatch && tags.push("ὍA " + r.subjectMatch), r.tutorId && (t = (S.tutors || []).find(x => x.id === r.tutorId)) && tags.push("὆4 " + t.fn + " " + t.ln), 
    r.gradeMatch && tags.push("ἾB " + r.gradeMatch + " кл."), r.durMin && tags.push("⏱ від " + r.durMin + " хв"), 
    '<div class="ms" style="align-items:center"><div style="flex:1"><div style="font-weight:600;font-size:13px">' + r.name + ' — <span style="color:var(--tut)">' + r.price + ' ₴</span></div><div style="font-size:11px;color:var(--t2);margin-top:2px">' + (tags.length ? tags.join(" · ") : "Застосовується до всіх") + '</div></div><div style="display:flex;gap:6px"><button class="btn btn-g btn-sm" onclick="editPriceRule(r.id)">✏️</button><button class="btn btn-sm btn-d" onclick="delPriceRule(r.id)">Ὕ1</button></div></div>';
  }).join("") : el.innerHTML = '<div style="font-size:12px;color:var(--t3);padding:8px 0">Немає правил. Додайте перше правило нижче.</div>');
}

function renderProfile() {
  var mt = myTutor(), _pi = ((_pi = document.getElementById("pr-info")) && (_pi.innerHTML = mt ? '\n    <div style="display:flex;align-items:center;gap:14px;margin-bottom:16px">' + mkAv(mt.fn, mt.ln, 48) + '<div><div style="font-size:17px;font-weight:700;font-family:Syne,sans-serif">' + mt.fn + " " + mt.ln + '</div><div style="font-size:12px;color:var(--t2);margin-top:2px">' + (mt.subj || "—") + '</div></div></div>\n    <div class="ms"><span class="msl">Телефон</span><span class="msv" style="font-family:inherit">' + (mt.phone || "—") + '</span></div>\n    <div class="ms"><span class="msl">Email</span><span class="msv" style="font-family:inherit">' + (mt.email || "—") + '</span></div>\n    <div class="ms"><span class="msl">Ставка</span><span class="msv">' + (mt.rate || "—") + '₴/год</span></div>\n    <div class="ms"><span class="msl">Рейтинг</span><span class="msv">' + "⭐".repeat(mt.rating || 5) + '</span></div>\n    <div class="ms"><span class="msl">Занять проведено</span><span class="msv">' + myLessons().filter(l => "done" === l.status).length + "</span></div>\n    " + (mt.bio ? `<div style="margin-top:12px;padding:10px;background:var(--s2);border-radius:8px;font-size:12px;color:var(--t2)">${mt.bio}</div>` : "") + "\n  " : '<div class="empty"><div class="ei">ὑ7</div>Ваш акаунт не прив\'язаний до профілю викладача</div>'), 
  myStudents());
  (mt = document.getElementById("pr-students")) && (mt.innerHTML = _pi.length ? _pi.map(s => "<tr><td>" + s.fn + " " + s.ln + "</td><td>" + (s.subject || "—") + "</td><td>" + bst(s.status) + "</td></tr>").join("") : '<tr><td colspan="3"><div class="empty" style="padding:14px">Немає учнів</div></td></tr>');
}

function renderReports() {
  let months = [ "Січ", "Лют", "Бер", "Кві", "Тра", "Чер", "Лип", "Сер", "Вер", "Жов", "Лис", "Гру" ], md = new Array(12).fill(0), maxI = (S.payments.filter(p => "paid" === p.status).forEach(p => {
    var d = new Date(p.date);
    md[d.getMonth()] += p.amount;
  }), Math.max(...md, 1)), sc = (document.getElementById("rc-income").innerHTML = md.map((v, i) => '<div class="bw"><div class="bar" style="height:' + v / maxI * 100 + '%;background:linear-gradient(180deg,var(--adm),var(--adm2))">' + (0 < v ? `<div style="position:absolute;top:-16px;left:50%;transform:translateX(-50%);font-size:8px;color:var(--t2);white-space:nowrap;font-family:JetBrains Mono,monospace">${1e3 <= v ? (v / 1e3).toFixed(0) + "к" : v}</div>` : "") + '</div><div class="blbl">' + months[i] + "</div></div>").join(""), 
  {}), totalL = (S.lessons.forEach(l => {
    sc[l.subject] = (sc[l.subject] || 0) + 1;
  }), S.lessons.length || 1), cols = [ "var(--adm)", "var(--tut)", "var(--dir)", "var(--god)", "#a78bfa", "#0ea5e9" ], tl = (document.getElementById("rc-subj").innerHTML = Object.entries(sc).sort((a, b) => b[1] - a[1]).map(([ s, c ], i) => '<div style="margin-bottom:10px"><div style="display:flex;justify-content:space-between;margin-bottom:3px"><span style="font-size:12px">' + s + '</span><span style="font-size:11px;color:var(--t2);font-family:JetBrains Mono,monospace">' + c + " (" + Math.round(c / totalL * 100) + '%)</span></div><div class="pb"><div class="pf" style="width:' + c / totalL * 100 + "%;background:" + cols[i % cols.length] + '"></div></div></div>').join("") || '<div class="empty"><div class="ei">ὍA</div>Немає даних</div>', 
  {}), maxT = (S.lessons.forEach(l => {
    l.tutorId && (tl[l.tutorId] = (tl[l.tutorId] || 0) + 1);
  }), Math.max(...Object.values(tl), 1));
  document.getElementById("rc-tload").innerHTML = Object.entries(tl).map(([ id, c ]) => '<div style="margin-bottom:10px"><div style="display:flex;justify-content:space-between;margin-bottom:3px"><span style="font-size:12px">' + tn(id) + '</span><span style="font-size:11px;color:var(--t2);font-family:JetBrains Mono,monospace">' + c + ' занять</span></div><div class="pb"><div class="pf" style="width:' + c / maxT * 100 + '%"></div></div></div>').join("") || '<div class="empty"><div class="ei">ᾝ1‍ἾB</div>Немає даних</div>';
  var totalInc = S.payments.filter(p => "paid" === p.status).reduce((a, p) => a + p.amount, 0);
  document.getElementById("rc-gen").innerHTML = '\n    <div class="ms"><span class="msl">Всього учнів</span><span class="msv">' + S.students.length + '</span></div>\n    <div class="ms"><span class="msl">Активних учнів</span><span class="msv">' + S.students.filter(s => "active" === s.status).length + '</span></div>\n    <div class="ms"><span class="msl">Всього занять</span><span class="msv">' + S.lessons.length + '</span></div>\n    <div class="ms"><span class="msl">Загальний дохід</span><span class="msv" style="color:var(--tut)">' + totalInc.toLocaleString("uk-UA") + '₴</span></div>\n    <div class="ms"><span class="msl">Середня вартість</span><span class="msv">' + (S.lessons.length ? (totalInc / S.lessons.length).toFixed(0) + "₴" : "—") + '</span></div>\n    <div class="ms"><span class="msl">Викладачів</span><span class="msv">' + S.tutors.length + "</span></div>";
}

function renderSch() {
  var view = S.schView || "week", btnW = document.getElementById("sch-btn-week"), btnD = document.getElementById("sch-btn-day"), tf = document.getElementById("sch-tutor-filter"), btnW = (btnW && btnW.classList.toggle("active-view", "week" === view), 
  btnD && btnD.classList.toggle("active-view", "day" === view), tf && (tf.style.display = can("tutors") && "tutor" !== R() ? "block" : "none"), 
  document.getElementById("sch-prev")), btnD = document.getElementById("sch-next");
  btnW && (btnW.textContent = "day" === view ? "← Вчора" : "← Попередній"), btnD && (btnD.textContent = "day" === view ? "Завтра →" : "Наступний →"), 
  ("week" === view ? renderSchWeek : renderSchDay)();
}

function renderSchDay() {
  var now = new Date(), offset = S.dayOffset || 0, day = new Date(now);
  day.setDate(now.getDate() + offset), day.setHours(0, 0, 0, 0);
  let ds = localDateStr(day), filterTutor = (document.getElementById("wklbl").textContent = [ "Неділя", "Понеділок", "Вівторок", "Середа", "Четвер", "П'ятниця", "Субота" ][day.getDay()] + ", " + day.toLocaleDateString("uk-UA", {
    day: "numeric",
    month: "long"
  }), (now = document.getElementById("sch-tutor-filter")) && (offset = now.value, 
  now.innerHTML = '<option value="">Всі репетитори</option>' + (S.tutors || []).map(t => '<option value="' + t.id + '">' + t.fn + " " + t.ln + "</option>").join(""), 
  now.value = offset), now ? now.value : ""), tutors = P().seeAll ? S.tutors || [] : (S.tutors || []).filter(t => t.accId === CU?.id), hrs = (filterTutor && (tutors = tutors.filter(t => t.id === filterTutor)), 
  Array.from({
    length: 13
  }, (_, i) => i + 8)), ecls = [ "ec0", "ec1", "ec2", "ec3", "ec4" ], ml = myLessons(), cols = tutors.length || 1, html = '<div class="schh" style="background:var(--s1)">Час</div>';
  0 === tutors.length ? html += '<div class="schh">Немає репетиторів</div>' : tutors.forEach(t => {
    html += '<div class="schh"><div style="font-weight:700;font-size:12px">' + t.fn + '</div><div style="font-size:10px;color:var(--t2)">' + t.ln + "</div></div>";
  }), hrs.forEach(h => {
    html += '<div class="scht">' + String(h).padStart(2, "0") + ":00</div>", 0 === tutors.length ? html += '<div class="schc"></div>' : tutors.forEach(t => {
      var lsns = ml.filter(l => l.date === ds && l.tutorId === t.id && parseInt((l.time || "0:0").split(":")[0]) === h && "cancelled" !== l.status);
      html += '<div class="schc" onclick="openLessM(null,\'' + ds + "','" + String(h).padStart(2, "0") + ":00')\">", 
      lsns.forEach((l, i) => {
        html += '<div class="sche ' + ecls[i % ecls.length] + '" onclick="event.stopPropagation();openLessM(\'' + l.id + '\')">\n            <div style="font-weight:700;font-size:11px">' + l.subject + '</div>\n            <div class="sche-tutor">' + sn(l.studentId).split(" ")[0] + "</div>\n          </div>";
      }), html += "</div>";
    });
  }), (day = document.getElementById("schg")).style.gridTemplateColumns = "52px repeat(" + cols + ",1fr)", 
  day.style.gridTemplateRows = "auto repeat(" + hrs.length + ",46px)", day.innerHTML = html;
}

function renderSchWeek() {
  var tf2 = document.getElementById("sch-tutor-filter"), _schTid = (tf2 && tf2.options.length <= 1 && (tf2.innerHTML = '<option value="">Усі репетитори</option>' + (S.tutors || []).map(function(t) {
    return '<option value="' + t.id + '">' + t.fn + " " + t.ln + "</option>";
  }).join("")), (document.getElementById("sch-tutor-filter") || {
    value: ""
  }).value), _schStat = (document.getElementById("sch-status-filter") || {
    value: ""
  }).value, ml = myLessons().filter(function(l) {
    return !(_schTid && l.tutorId !== _schTid && l.tutor_id !== _schTid || _schStat && ("planned" === _schStat ? "planned" !== l.status && "scheduled" !== l.status && l.status : l.status !== _schStat));
  }), now = new Date(), sow = new Date(now), tf2 = 0 === now.getDay() ? 6 : now.getDay() - 1, days = (sow.setDate(now.getDate() - tf2 + 7 * S.weekOffset), 
  sow.setHours(0, 0, 0, 0), Array.from({
    length: 7
  }, function(_, i) {
    var d = new Date(sow);
    return d.setDate(sow.getDate() + i), d;
  })), dnames = [ "Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд" ], tf2 = (document.getElementById("wklbl").textContent = days[0].toLocaleDateString("uk-UA", {
    day: "numeric",
    month: "short"
  }) + " — " + days[6].toLocaleDateString("uk-UA", {
    day: "numeric",
    month: "short"
  }), Array.from({
    length: 13
  }, function(_, i) {
    return i + 8;
  })), html = '<div class="schh" style="background:var(--s1)">Час</div>', g = (days.forEach(function(d, i) {
    var today = d.toDateString() === now.toDateString();
    html += '<div class="schh" style="' + (today ? "color:var(--adm);font-weight:700" : "") + '">' + dnames[i] + '<br><span style="font-size:11px;opacity:.7">' + d.toLocaleDateString("uk-UA", {
      day: "numeric",
      month: "short"
    }) + "</span></div>";
  }), tf2.forEach(function(h) {
    html += '<div class="scht">' + String(h).padStart(2, "0") + ":00</div>", days.forEach(function(d) {
      var ds = d.getFullYear() + "-" + String(d.getMonth() + 1).padStart(2, "0") + "-" + String(d.getDate()).padStart(2, "0"), d = ml.filter(function(l) {
        return l.date === ds && parseInt((l.time || "0:0").split(":")[0]) === h && "cancelled" !== l.status;
      });
      html += '<div class="schc" onclick="openLessM(null,\'' + ds + "','" + String(h).padStart(2, "0") + ":00')\">", 
      d.forEach(function(l) {
        var ecl = "completed" === l.status ? "ec-done" : "missed" === l.status ? "ec-miss" : "makeup" === l.status ? "ec-make" : "ec-plan", durMin = l.dur || 60, heightPx = Math.max(40, Math.round(durMin / 60 * 46) - 4);
        html += '<div class="sche ' + ecl + '" style="min-height:' + heightPx + 'px" onclick="event.stopPropagation();openLessM(\'' + l.id + '\')"><div style="font-weight:700">' + (l.recurId ? "ὐ1 " : "") + "<span>" + l.subject + "</span>" + (60 !== durMin ? '<span style="font-size:10px;opacity:.65"> (' + (60 <= durMin ? durMin / 60 + "год" : durMin + "хв") + ")" : "") + '</div><div style="opacity:.75">' + sn(l.studentId).split(" ")[0] + "</div></div>";
      }), html += "</div>";
    });
  }), document.getElementById("schg"));
  g.style.gridTemplateColumns = "52px repeat(7,1fr)", g.style.gridTemplateRows = "auto repeat(" + tf2.length + ",46px)", 
  g.innerHTML = html;
}


function updateSBUser(){
  if(!CU) return;
  var r = ROLES[CU.role];
  var av = document.getElementById('sb-av');
  if(av){
    av.style.background = r ? r.avatarBg : '#888';
    av.style.width = '34px'; av.style.height = '34px';
    av.style.fontSize = '13px'; av.style.borderRadius = '50%';
    av.style.display = 'flex'; av.style.alignItems = 'center';
    av.style.justifyContent = 'center';
    av.style.color = CU.role === 'director' ? '#1b1464' : '#fff';
    av.style.fontFamily = "'Syne',sans-serif"; av.style.fontWeight = '700';
    av.textContent = (CU.fn ? CU.fn[0] : '') + (CU.ln ? CU.ln[0] : '');
  }
  var nm = document.getElementById('sb-name');
  if(nm) nm.textContent = CU.fn + ' ' + CU.ln;
  var rp = document.getElementById('sb-rpill');
  if(rp && r) rp.innerHTML = '<span class="rpill ' + CU.role + '">' + r.icon + ' ' + r.label + '</span>';
}

initApp();
