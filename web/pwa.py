import json

from django.http import HttpResponse, JsonResponse
from django.templatetags.static import static
from django.utils import timezone
from django.views.decorators.http import require_POST
from django.contrib.auth.decorators import login_required

from core.services.permissions import require_active_parish
from notifications.models import PushSubscription


PWA_NAME = "Acoli"
PWA_SHORT_NAME = "Acoli"
PWA_DESCRIPTION = "Sistema de escalas para acolitos."
PWA_THEME_COLOR = "#0f5e4d"
PWA_BACKGROUND_COLOR = "#f3efe6"
PWA_LANG = "pt-BR"
SW_VERSION = "1"


def manifest(request):
    icon_192 = static("pwa/icon-192.png")
    icon_512 = static("pwa/icon-512.png")
    icon_maskable_512 = static("pwa/icon-maskable-512.png")

    data = {
        "name": PWA_NAME,
        "short_name": PWA_SHORT_NAME,
        "description": PWA_DESCRIPTION,
        "lang": PWA_LANG,
        "start_url": "/?source=pwa",
        "id": "/?source=pwa",
        "scope": "/",
        "display": "standalone",
        "orientation": "portrait",
        "theme_color": PWA_THEME_COLOR,
        "background_color": PWA_BACKGROUND_COLOR,
        "icons": [
            {"src": icon_192, "sizes": "192x192", "type": "image/png"},
            {"src": icon_512, "sizes": "512x512", "type": "image/png"},
            {"src": icon_maskable_512, "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
        ],
        "shortcuts": [
            {"name": "Minhas escalas", "short_name": "Escalas", "url": "/assignments/?source=pwa", "icons": [{"src": icon_192, "sizes": "192x192"}]},
            {"name": "Calendario", "short_name": "Calendario", "url": "/calendar/?source=pwa", "icons": [{"src": icon_192, "sizes": "192x192"}]},
            {"name": "Trocas", "short_name": "Trocas", "url": "/swap-requests/?source=pwa", "icons": [{"src": icon_192, "sizes": "192x192"}]},
            {"name": "Eventos", "short_name": "Eventos", "url": "/events/interest/?source=pwa", "icons": [{"src": icon_192, "sizes": "192x192"}]},
        ],
    }
    response = JsonResponse(data, json_dumps_params={"ensure_ascii": True})
    response["Content-Type"] = "application/manifest+json"
    response["Cache-Control"] = "no-store"
    response["Pragma"] = "no-cache"
    response["Expires"] = "0"
    return response


def service_worker(request):
    offline_html = (
        "<!doctype html><html lang=\"pt-br\"><head>"
        "<meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
        "<title>Acoli - Offline</title>"
        "<style>body{font-family:system-ui,Arial,sans-serif;margin:0;padding:24px;background:#f3efe6;color:#0b1f26;}"
        ".card{max-width:520px;margin:10vh auto;background:#fff;border-radius:16px;padding:24px;"
        "box-shadow:0 12px 30px rgba(10,24,32,.12);}"
        "h1{font-size:20px;margin:0 0 8px;}p{margin:0;color:#5a6a72;font-size:14px;}"
        "</style></head><body><div class=\"card\"><h1>Sem conexao</h1>"
        "<p>Abra o app novamente quando estiver online.</p></div></body></html>"
    )
    script = f"""
/* Acoli PWA Service Worker v{SW_VERSION} */
const OFFLINE_HTML = {json.dumps(offline_html)};

self.addEventListener('install', (event) => {{
  self.skipWaiting();
}});

self.addEventListener('activate', (event) => {{
  event.waitUntil(self.clients.claim());
}});

self.addEventListener('message', (event) => {{
  if (event.data && event.data.type === 'SKIP_WAITING') {{
    self.skipWaiting();
  }}
}});

self.addEventListener('fetch', (event) => {{
  const request = event.request;
  if (request.method !== 'GET') {{
    return;
  }}
  const url = new URL(request.url);
  if (url.origin !== self.location.origin) {{
    return;
  }}
  const accept = request.headers.get('accept') || '';
  const isNavigation = request.mode === 'navigate' || accept.includes('text/html');
  if (isNavigation) {{
    event.respondWith(
      fetch(request).catch(() => new Response(OFFLINE_HTML, {{
        headers: {{ 'Content-Type': 'text/html; charset=utf-8' }}
      }}))
    );
    return;
  }}
  event.respondWith(fetch(request));
}});

self.addEventListener('push', (event) => {{
  let data = {{}};
  if (event.data) {{
    try {{
      data = event.data.json();
    }} catch (err) {{
      data = {{ body: event.data.text() }};
    }}
  }}
  const title = data.title || 'Acoli';
  const options = {{
    body: data.body || 'Nova atualizacao.',
    icon: '{static("pwa/icon-192.png")}',
    badge: '{static("pwa/icon-192.png")}',
    data: {{ url: data.url || '/' }}
  }};
  event.waitUntil(self.registration.showNotification(title, options));
}});

self.addEventListener('notificationclick', (event) => {{
  const url = (event.notification && event.notification.data && event.notification.data.url) || '/';
  event.notification.close();
  event.waitUntil(
    self.clients.matchAll({{ type: 'window', includeUncontrolled: true }}).then((clients) => {{
      for (const client of clients) {{
        if (client.url.includes(url) && 'focus' in client) {{
          return client.focus();
        }}
      }}
      if (self.clients.openWindow) {{
        return self.clients.openWindow(url);
      }}
      return undefined;
    }})
  );
}});
"""
    response = HttpResponse(script.encode("utf-8"), content_type="application/javascript")  # type: ignore[arg-type]
    response["Cache-Control"] = "no-store"
    response["Service-Worker-Allowed"] = "/"
    return response


@login_required
@require_active_parish
@require_POST
def push_subscribe(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"ok": False, "error": "invalid_json"}, status=400)

    endpoint = payload.get("endpoint")
    keys = payload.get("keys") or {}
    auth_key = keys.get("auth")
    p256dh_key = keys.get("p256dh")

    if not endpoint or not auth_key or not p256dh_key:
        return JsonResponse({"ok": False, "error": "missing_fields"}, status=400)

    PushSubscription.objects.update_or_create(  # type: ignore[attr-defined]
        parish=request.active_parish,
        user=request.user,
        endpoint=endpoint,
        defaults={
            "auth_key": auth_key,
            "p256dh_key": p256dh_key,
            "user_agent": (request.headers.get("User-Agent") or "")[:255],
            "is_active": True,
            "last_seen_at": timezone.now(),
        },
    )

    return JsonResponse({"ok": True})


@login_required
@require_active_parish
@require_POST
def push_unsubscribe(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"ok": False, "error": "invalid_json"}, status=400)

    endpoint = payload.get("endpoint")
    if not endpoint:
        return JsonResponse({"ok": False, "error": "missing_endpoint"}, status=400)

    PushSubscription.objects.filter(  # type: ignore[attr-defined]
        parish=request.active_parish,
        user=request.user,
        endpoint=endpoint,
        is_active=True,
    ).update(is_active=False, last_seen_at=timezone.now())

    return JsonResponse({"ok": True})
