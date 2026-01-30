# Acoli

Sistema de escalas para acolitos com multi-paroquia, agendamento otimizado e fluxo de confirmacoes.

## Stack
- Django 5 + DRF
- PostgreSQL (Heroku Postgres Essential)
- HTMX + Tailwind (via CDN) para UI
- OR-Tools CP-SAT para o solver global
- WhiteNoise para estaticos

## Setup local
1) Crie um virtualenv e instale dependencias:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

2) Configure variaveis de ambiente (use `.env.example` como base).
   - Para desenvolvimento local, crie `.env` com `DEBUG=1` e `APP_BASE_URL` local.
   - Se `DEBUG=0`, voce precisa definir `ALLOWED_HOSTS` para evitar erro no boot.

3) Rode migracoes e carregue dados iniciais:

```bash
python manage.py migrate
python manage.py loaddata fixtures/seed_parish.json
python manage.py createsuperuser
```

4) Vincule o superuser a uma paroquia (PARISH_ADMIN ou COORDINATOR):
- Acesse `/admin/` e crie um `ParishMembership` para o usuario.
- Ou use a tela "Pessoas" para adicionar o papel.

5) Inicie o servidor:

```bash
python manage.py runserver
```

## Variaveis de ambiente
- `SECRET_KEY`
- `DEBUG`
- `ALLOWED_HOSTS`
- `DATABASE_URL`
- `DB_CONN_MAX_AGE`
- `CSRF_TRUSTED_ORIGINS`
- `EMAIL_HOST`, `EMAIL_PORT`, `EMAIL_HOST_USER`, `EMAIL_HOST_PASSWORD`, `EMAIL_USE_TLS`
- `DEFAULT_FROM_EMAIL`
- `APP_BASE_URL` (usado para links absolutos em emails)

## API (DRF)
- Para clientes sem sessao, envie `X-Parish-ID` (ou `?parish_id=`) nas requisicoes `/api/*`.
- O usuario precisa ter membership ativa na paroquia (ou ser system admin via `is_system_admin`).

## Calendario (ICS)
- Apos gerar o link em "Minhas escalas", use `/calendar/my.ics?token=...` para assinar o calendario.

## Comandos de manutencao
- `python manage.py generate_mass_instances --days 60`
- `python manage.py sync_slots --days 60`
- `python manage.py lock_consolidation_window`
- `python manage.py recompute_acolyte_stats`
- `python manage.py run_global_scheduler`
- `python manage.py send_notifications`

Para solicitar um job de escalonamento pelo admin, use a tela "Escalonar" ou crie um `ScheduleJobRequest` via admin/ORM.

## Heroku (Basic + Postgres Essential)
1) Crie o app e banco:

```bash
heroku create acoli
heroku addons:create heroku-postgresql:essential-0
```

2) Configure variaveis:

```bash
heroku config:set SECRET_KEY=... ALLOWED_HOSTS=acoli.herokuapp.com APP_BASE_URL=https://acoli.herokuapp.com
```

3) Deploy:

```bash
git push heroku main
heroku run python manage.py migrate
heroku run python manage.py loaddata fixtures/seed_parish.json
```

4) Configure Heroku Scheduler:
- `python manage.py generate_mass_instances --days 60` (diario)
- `python manage.py sync_slots --days 60` (diario)
- `python manage.py lock_consolidation_window` (diario)
- `python manage.py recompute_acolyte_stats` (diario)
- `python manage.py run_global_scheduler` (noturno)
- `python manage.py send_notifications` (a cada 10-15 min)

## Publicar com Cloudflare Tunnel (auto-host)
1) Crie o tunnel e aponte o DNS:

```bash
cloudflared tunnel login
cloudflared tunnel create acoli
cloudflared tunnel route dns acoli acoli.com.br
cloudflared tunnel route dns acoli www.acoli.com.br
```

2) Configure `~/.cloudflared/config.yml`:

```yaml
tunnel: <TUNNEL_ID>
credentials-file: /Users/<usuario>/.cloudflared/<TUNNEL_ID>.json
ingress:
  - hostname: acoli.com.br
    service: http://127.0.0.1:8001
  - hostname: www.acoli.com.br
    service: http://127.0.0.1:8001
  - service: http_status:404
```

3) Variaveis de ambiente recomendadas:

```bash
DEBUG=0
ALLOWED_HOSTS=acoli.com.br,www.acoli.com.br,localhost
CSRF_TRUSTED_ORIGINS=https://acoli.com.br,https://www.acoli.com.br
APP_BASE_URL=https://acoli.com.br
SECURE_SSL_REDIRECT=1
```

4) Rode migracoes/estaticos e suba com gunicorn:

```bash
python manage.py migrate
python manage.py collectstatic --noinput
./.venv/bin/gunicorn acoli.wsgi --bind 127.0.0.1:8001 --log-file - -c gunicorn.conf.py
```

## Testes
```bash
python manage.py test
```

## Docs
- Arquitetura: `docs/ARCHITECTURE_NOTES.md`
- Guia do usuario: `docs/USER_GUIDE.md`
