#!/bin/bash
set -e

PROJECT_DIR="/Users/fabiomigueldp/Projects/acoli"
MANAGE="$PROJECT_DIR/.venv/bin/python $PROJECT_DIR/manage.py"

echo "==================================================="
echo " ACOLI SYSTEM SYNC - $(date '+%Y-%m-%d %H:%M:%S')"
echo "==================================================="

if [ -f "$PROJECT_DIR/db.sqlite3" ]; then
  echo "[INFO] Backup preventivo do banco..."
  cp "$PROJECT_DIR/db.sqlite3" "$PROJECT_DIR/db.sqlite3.bak.$(date '+%Y%m%d_%H%M%S')"
fi

echo "[STEP 1/3] Detectando mudancas nos modelos (makemigrations)..."
$MANAGE makemigrations --noinput

echo "[STEP 2/3] Aplicando mudancas no Banco de Dados (migrate)..."
$MANAGE migrate --noinput

echo "[STEP 3/3] Coletando arquivos estaticos (collectstatic)..."
$MANAGE collectstatic --noinput

echo "==================================================="
echo "[SUCCESS] Sincronizacao concluida com sucesso."
echo "==================================================="

