#!/bin/bash
set -e

PROJECT_DIR="/Users/fabiomigueldp/Projects/acoli"
LOG_FILE="$PROJECT_DIR/server.log"
PORT="8001"
BIND="127.0.0.1:$PORT"

echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') - Verificando porta $PORT..."

if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null ; then
    echo "[WARN] Porta $PORT ocupada. Parando processo anterior..."
    lsof -t -i:$PORT | xargs kill -15
    sleep 2
fi

if [ -f "$PROJECT_DIR/db.sqlite3" ]; then
  echo "[INFO] Realizando backup de seguranca do banco de dados..."
  cp "$PROJECT_DIR/db.sqlite3" "$PROJECT_DIR/db.sqlite3.bak.$(date '+%Y%m%d_%H%M%S')"
fi

echo "[INFO] Ativando venv e aplicando migrate/collectstatic..."
cd "$PROJECT_DIR"
source "$PROJECT_DIR/.venv/bin/activate"

python manage.py migrate --noinput
python manage.py collectstatic --noinput

echo "[INFO] Iniciando Acoli (gunicorn) em $BIND ..."
nohup "$PROJECT_DIR/.venv/bin/gunicorn" acoli.wsgi \
  --bind "$BIND" \
  --workers 2 \
  --forwarded-allow-ips="*" \
  --log-level info \
  --access-logfile - \
  --error-logfile - \
  -c "$PROJECT_DIR/gunicorn.conf.py" \
  > "$LOG_FILE" 2>&1 &

sleep 2
PID=$(lsof -t -i:$PORT || true)

if [ -n "$PID" ]; then
    echo "[SUCCESS] Gunicorn rodando. PID(s): $PID"
    echo "[INFO] Logs disponiveis em: $LOG_FILE"
else
    echo "[ERROR] Falha ao iniciar gunicorn. Verifique o log: $LOG_FILE"
    exit 1
fi
