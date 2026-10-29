#!/usr/bin/env node
// Хук Claude Code: після кожної зміни .js або .html перевіряє синтаксис JavaScript.
// Якщо знайдено помилку — повертає код 2, і Claude бачить її та виправляє одразу.
// Ручний запуск: node .claude/hooks/check-syntax.js шлях/до/файлу.js
const fs = require('fs');
const vm = require('vm');

function check(file) {
  if (!/\.(m?js|html?)$/i.test(file) || !fs.existsSync(file)) return [];
  const src = fs.readFileSync(file, 'utf8');
  const errors = [];
  const parse = (code, label, lineOffset) => {
    try {
      new vm.Script(code, { filename: label, lineOffset });
    } catch (e) {
      const at = (e.stack || '').split('\n')[0];
      errors.push(`${at}\n${e.name}: ${e.message}`);
    }
  };
  if (/\.m?js$/i.test(file)) {
    parse(src, file, 0);
  } else {
    // Вбудовані <script> без src і не модулі/JSON
    const re = /<script(\s[^>]*)?>([\s\S]*?)<\/script>/gi;
    let m;
    while ((m = re.exec(src))) {
      const attrs = m[1] || '';
      if (/\bsrc\s*=/i.test(attrs)) continue;
      if (/type\s*=\s*["']?(module|application\/(ld\+)?json|text\/template)/i.test(attrs)) continue;
      const line = src.slice(0, m.index + m[0].indexOf('>') + 1).split('\n').length - 1;
      parse(m[2], file, line);
    }
  }
  return errors;
}

function main() {
  let files = process.argv.slice(2);
  if (!files.length) {
    let input = '';
    try { input = fs.readFileSync(0, 'utf8'); } catch (_) {}
    try {
      const data = JSON.parse(input || '{}');
      const p = data.tool_input && (data.tool_input.file_path || data.tool_input.notebook_path);
      if (p) files = [p];
    } catch (_) {}
  }
  const errors = files.flatMap(check);
  if (errors.length) {
    process.stderr.write('❌ Синтаксична помилка JavaScript — виправ перед продовженням:\n' + errors.join('\n\n') + '\n');
    process.exit(2);
  }
}
main();
