#!/bin/bash
PORT="8001"

echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') - Solicitando parada do servidor (porta $PORT)..."

PID=$(lsof -t -i:$PORT || true)

if [ -z "$PID" ]; then
    echo "[INFO] Nenhum servidor rodando na porta $PORT."
    exit 0
fi

kill -15 $PID 2>/dev/null || true
echo "[INFO] Sinal SIGTERM enviado para PID $PID. Aguardando..."

sleep 3

if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null ; then
    echo "[WARN] Servidor nao respondeu. Forcando encerramento (SIGKILL)..."
    lsof -t -i:$PORT | xargs kill -9
fi

echo "[SUCCESS] Servidor parado."

