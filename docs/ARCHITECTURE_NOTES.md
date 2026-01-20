# Architecture Notes

## Modelo de dados
- Multi-paroquia: todas as entidades principais carregam `parish_id` e acessos sao filtrados pelo `ActiveParishMiddleware`.
- Identidade: `User` global + `ParishMembership` para papeis por paroquia.
- Acolitos: `AcolyteProfile` separado da identidade para permitir uso sem login.
- Calendario: `MassTemplate` (recorrencia) + `MassInstance` (instancia) + `MassOverride` (cancelar/mover/alterar requisitos).
- Eventos: `EventSeries` + `EventOccurrence` (dias) com resolucao de conflitos e `EventInterest` para pool de interessados.
- MassInstance: unicidade garantida apenas para status `scheduled`, preservando historico de cancelamentos.
- Escalas: `AssignmentSlot` + `Assignment` + `Confirmation` + `SwapRequest` + `ReplacementRequest`.
- Auditoria: `AuditEvent` para historico de alteracoes criticas.

## Motor de escalonamento
- Horizon: 60 dias por padrao, com regra de completude para fins de semana e series de eventos.
- CP-SAT: variaveis binarias por slot/acolito, com restricoes de cobertura, qualificacao, indisponibilidade e travas de consolidacao.
- Objetivo: preferencias + penalidade de estabilidade + balanceamento de carga + bonus de credito.
- Pool de candidatos: series de eventos podem limitar escalas a acolitos interessados.
- Quick fill: heuristica simples para substituicoes dentro da consolidacao.

## Jobs e background
- Jobs sao disparados por `ScheduleJobRequest` e processados pelo comando `run_global_scheduler`.
- `sync_slots` garante slots em instancias ja criadas.
- `lock_consolidation_window` aplica travas dentro da janela de consolidacao.
- `send_notifications` envia emails pendentes com idempotencia.
- Sem worker sempre ativo: tarefas sao executadas via Heroku Scheduler ou one-off dynos.

## Heroku
- 1 dyno web, Postgres Essential.
- `CONN_MAX_AGE` para reduzir churn de conexoes.
- WhiteNoise para estaticos.
- Gunicorn com poucos workers/threads para evitar picos de memoria.
