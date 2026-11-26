/* HungryHop Food Delivery - AI Assistant Widget (self-contained component)
   Talks to the Flask backend at POST /chat, which applies the R13 guardrail
   and forwards allowed questions to the HungryHop Genie agent. */
(function () {
  'use strict';

  /* ---------- AI backend configuration (separate from UI) ---------- */
  var AIW_API = { endpoint: '/chat', method: 'POST', contentType: 'application/json' };
  var AIW_FALLBACK = "I'm having trouble connecting right now. Please try again in a little while.";
  var AIW_WELCOME = "Hi there! 👋 I'm your HungryHop assistant. Ask me about orders, late deliveries, GMV, restaurants, zones, or the compensation policy.";
  var AIW_SUGGESTIONS = [
    'What was the late rate by delivery class?',
    'Show me the monthly GMV trend',
    'Which are the top 10 late restaurants?',
    'What is the cancellation rate by zone?'
  ];

  var root = document.getElementById('aiw-root');
  var trigger = document.getElementById('aiw-trigger');
  var win = document.getElementById('aiw-window');
  var msgs = document.getElementById('aiw-messages');
  var sugg = document.getElementById('aiw-sugg');
  var input = document.getElementById('aiw-input');
  var sendBtn = document.getElementById('aiw-send');
  var badge = document.getElementById('aiw-badge');
  var tip = document.getElementById('aiw-tip');
  var themeBtn = document.getElementById('aiw-theme');
  var busy = false, welcomed = false;

  /* ---------- Short-term memory: current conversation history ---------- */
  var history = [];

  function remember(role, text) {
    if (!text) { return; }
    history.push({ role: role, text: String(text) });
    if (history.length > 24) { history = history.slice(-24); }
  }

  function ts() { return new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }); }
  function scrollDown() { msgs.scrollTop = msgs.scrollHeight; }

  /* Render light markdown (bold, italic, code, bullets, headings) as safe HTML */
  function mdToHtml(text) {
    var s = String(text);
    s = s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    s = s.replace(/`([^`]+)`/g, '<code>$1</code>');
    s = s.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
    s = s.replace(/(^|[^*\w])\*([^*\n]+)\*(?!\w)/g, '$1<em>$2</em>');
    s = s.replace(/^#{1,6} (.+)$/gm, '<strong>$1</strong>');
    s = s.replace(/^[-*] +/gm, '&bull; ');
    s = s.replace(/\n/g, '<br>');
    return s;
  }

  function addMsg(text, who) {
    var d = document.createElement('div');
    d.className = 'aiw-msg ' + (who === 'user' ? 'aiw-user' : 'aiw-bot');
    var body = document.createElement('span');
    if (who === 'bot') { body.innerHTML = mdToHtml(text); }
    else { body.textContent = text; }
    d.appendChild(body);
    var time = document.createElement('span');
    time.className = 'aiw-time';
    time.textContent = ts();
    d.appendChild(time);
    msgs.appendChild(d);
    scrollDown();
    return d;
  }

  var typingEl = null;
  function showTyping() {
    hideTyping();
    typingEl = document.createElement('div');
    typingEl.className = 'aiw-msg aiw-bot aiw-typing';
    typingEl.setAttribute('aria-label', 'Assistant is typing');
    typingEl.appendChild(document.createElement('i'));
    typingEl.appendChild(document.createElement('i'));
    typingEl.appendChild(document.createElement('i'));
    msgs.appendChild(typingEl);
    scrollDown();
  }
  function hideTyping() { if (typingEl) { typingEl.remove(); typingEl = null; } }

  /* ---------- AI backend call (swap this function to change providers) ---------- */
  function sendMessageToAI(question) {
    return fetch(AIW_API.endpoint, {
      method: AIW_API.method,
      headers: { 'Content-Type': AIW_API.contentType },
      body: JSON.stringify({ question: question, history: history.slice(-12) })
    }).then(function (r) { return r.json(); });
  }

  function renderReply(d) {
    hideTyping();
    if (d && d.blocked) { addMsg(d.blocked_msg || 'I cannot help with that request.', 'bot'); }
    else if (d && d.error) { addMsg(AIW_FALLBACK, 'bot'); }
    else if (d && d.answer) { addMsg(d.answer, 'bot'); remember('assistant', d.answer); }
    else { addMsg(AIW_FALLBACK, 'bot'); }
  }

  function send(preset) {
    var q = (preset || input.value).trim();
    if (!q || busy) return;
    input.value = '';
    sugg.style.display = 'none';
    addMsg(q, 'user');
    remember('user', q);
    busy = true; sendBtn.disabled = true;
    showTyping();
    sendMessageToAI(q)
      .then(renderReply)
      .catch(function () { hideTyping(); addMsg(AIW_FALLBACK, 'bot'); })
      .then(function () { busy = false; sendBtn.disabled = false; if (win.classList.contains('aiw-open')) input.focus(); });
  }

  function buildChips() {
    sugg.innerHTML = '';
    AIW_SUGGESTIONS.forEach(function (s) {
      var b = document.createElement('button');
      b.className = 'aiw-chip';
      b.type = 'button';
      b.textContent = s;
      b.addEventListener('click', function () { send(s); });
      sugg.appendChild(b);
    });
    sugg.style.display = '';
  }

  function openChat() {
    if (!welcomed) { addMsg(AIW_WELCOME, 'bot'); buildChips(); welcomed = true; }
    win.classList.add('aiw-open');
    trigger.setAttribute('aria-expanded', 'true');
    badge.hidden = true;
    tip.classList.remove('show');
    input.focus();
  }
  function closeChat() {
    win.classList.remove('aiw-open');
    trigger.setAttribute('aria-expanded', 'false');
    trigger.focus();
  }
  function clearChat() {
    msgs.innerHTML = '';
    history = [];
    welcomed = false;
    addMsg(AIW_WELCOME, 'bot');
    buildChips();
  }

  trigger.addEventListener('click', function () {
    if (win.classList.contains('aiw-open')) { closeChat(); } else { openChat(); }
  });
  document.getElementById('aiw-min').addEventListener('click', closeChat);
  document.getElementById('aiw-close').addEventListener('click', closeChat);
  document.getElementById('aiw-clear').addEventListener('click', clearChat);
  sendBtn.addEventListener('click', function () { send(); });
  input.addEventListener('keydown', function (e) { if (e.key === 'Enter') { e.preventDefault(); send(); } });
  document.addEventListener('keydown', function (e) { if (e.key === 'Escape' && win.classList.contains('aiw-open')) closeChat(); });
  document.addEventListener('click', function (e) {
    if (!win.classList.contains('aiw-open')) return;
    if (!win.contains(e.target) && !trigger.contains(e.target)) closeChat();
  });
  themeBtn.addEventListener('click', function () {
    root.classList.toggle('aiw-dark');
    themeBtn.textContent = root.classList.contains('aiw-dark') ? '☀️' : '🌙';
  });
  if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
    root.classList.add('aiw-dark');
    themeBtn.textContent = '☀️';
  }
  setTimeout(function () {
    if (!welcomed) { tip.classList.add('show'); badge.hidden = false; }
    setTimeout(function () { tip.classList.remove('show'); }, 6000);
  }, 1500);
})();
