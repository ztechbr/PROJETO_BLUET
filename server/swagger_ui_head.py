"""Fragmento HTML/JS injetado no Swagger UI via Flasgger (`SWAGGER['head_text']`)."""

from __future__ import annotations


def swagger_auth_expiry_head_text(ttl_ms: int) -> str:
    """
    ttl_ms > 0: após autorizar, mostra data/hora-alvo neste navegador (lembrete visual).
    ttl_ms == 0: apenas indica autorizado sem data (token estático no servidor).
    """
    ttl_json = str(int(ttl_ms))
    return f"""<style id="bluesensores-swagger-auth-style">
#bluesensores-swagger-auth-expiry {{
  display: inline-block;
  margin-left: 14px;
  padding: 4px 10px;
  font-size: 13px;
  line-height: 1.35;
  color: #0f5132;
  background: rgba(209, 250, 229, 0.95);
  border: 1px solid rgba(16, 185, 129, 0.45);
  border-radius: 9999px;
  vertical-align: middle;
  white-space: normal;
  max-width: min(560px, 92vw);
}}
#bluesensores-swagger-auth-expiry.hidden {{ display: none !important; }}
</style>
<script>
(function () {{
  const TTL_MS = {ttl_json};
  const STORAGE_UNTIL = 'bluesensores_swagger_auth_until_ms';
  const NOTE_ID = 'bluesensores-swagger-auth-expiry';

  function findAuthBtn() {{
    return (
      document.querySelector('.topbar-wrapper button.authorize') ||
      document.querySelector('.swagger-ui .topbar button.authorize') ||
      document.querySelector('button.authorize.btn')
    );
  }}

  function isAuthorized(btn) {{
    return !!(btn && btn.classList.contains('locked'));
  }}

  function formatUntil(tsMs) {{
    try {{
      return new Date(Number(tsMs)).toLocaleString('pt-BR', {{
        dateStyle: 'short',
        timeStyle: 'medium'
      }});
    }} catch (e) {{
      return String(tsMs);
    }}
  }}

  function ensureNoteEl(anchorBtn) {{
    let el = document.getElementById(NOTE_ID);
    if (!anchorBtn || !anchorBtn.parentElement) return null;
    if (!el) {{
      el = document.createElement('span');
      el.id = NOTE_ID;
      el.className = 'hidden';
      el.setAttribute('aria-live', 'polite');
      anchorBtn.insertAdjacentElement('afterend', el);
    }}
    return el;
  }}

  let wasLocked = false;

  function tick() {{
    const btn = findAuthBtn();
    const locked = isAuthorized(btn);
    const note = ensureNoteEl(btn);

    if (!locked && wasLocked) {{
      try {{ sessionStorage.removeItem(STORAGE_UNTIL); }} catch (e) {{}}
    }}
    if (locked && TTL_MS > 0) {{
      try {{
        if (!sessionStorage.getItem(STORAGE_UNTIL)) {{
          sessionStorage.setItem(STORAGE_UNTIL, String(Date.now() + TTL_MS));
        }}
      }} catch (e) {{}}
    }}
    wasLocked = locked;

    if (!note) return;

    if (!locked) {{
      note.classList.add('hidden');
      note.textContent = '';
      return;
    }}

    note.classList.remove('hidden');

    let untilTxt = '';
    if (TTL_MS > 0) {{
      try {{
        const raw = sessionStorage.getItem(STORAGE_UNTIL);
        if (raw) untilTxt = formatUntil(raw);
      }} catch (e) {{}}
    }}

    if (TTL_MS > 0 && untilTxt) {{
      note.textContent = 'Já autorizado — expiração (neste navegador): ' + untilTxt;
    }} else {{
      note.textContent =
        'Já autorizado — o token configurado em API_TOKEN no servidor não expira automaticamente; ' +
        'renove aqui após alterar o .env.';
    }}

    try {{
      const raw = sessionStorage.getItem(STORAGE_UNTIL);
      if (TTL_MS > 0 && raw && Date.now() > Number(raw)) {{
        note.textContent =
          'Autorização exibida expirou (neste navegador); clique em Authorize e aplique de novo.';
        note.style.color = '#7c2d12';
        note.style.borderColor = 'rgba(251, 146, 60, 0.65)';
        note.style.background = 'rgba(254, 215, 170, 0.95)';
      }} else {{
        note.style.color = '';
        note.style.borderColor = '';
        note.style.background = '';
      }}
    }} catch (e) {{}}
  }}

  window.addEventListener('load', function () {{
    tick();
    // Só polling: MutationObserver disparava milhares de vezes durante o React do Swagger e podia travar a página.
    setInterval(tick, 950);
  }});
}})();
</script>
"""
