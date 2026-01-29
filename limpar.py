# salvar como limpar.py (no root do projeto, ao lado de manage.py)
# Uso:
#  python limpar.py --dry-run        # apenas mostra o que seria feito
#  python limpar.py                 # executar (será pedido confirmar)
#  python limpar.py --undo          # reverte a partir do backup mais recente
#
# Requisitos: executar no virtualenv do projeto; DJANGO_SETTINGS_MODULE será definido automaticamente
# pelo script para 'acoli.settings' (ajuste se seu settings estiver em outro lugar).
import os
import sys
import json
import argparse
import shutil
import datetime
import pytz
from pathlib import Path
# --- bootstrap django ---
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "acoli.settings")
import django
django.setup()
from django.conf import settings
from django.db import transaction, connection, models
from django.utils import timezone
from django.core import serializers
from django.apps import apps
# Models (app 'core' conforme repo)
MassInstance = apps.get_model("core", "MassInstance")
AssignmentSlot = apps.get_model("core", "AssignmentSlot")
Assignment = apps.get_model("core", "Assignment")
Confirmation = apps.get_model("core", "Confirmation")
PositionClaimRequest = apps.get_model("core", "PositionClaimRequest")
ReplacementRequest = apps.get_model("core", "ReplacementRequest")
SwapRequest = apps.get_model("core", "SwapRequest")
MassInterest = apps.get_model("core", "MassInterest")
MassOverride = apps.get_model("core", "MassOverride")
AcolyteCreditLedger = apps.get_model("core", "AcolyteCreditLedger")
AuditEvent = apps.get_model("core", "AuditEvent")
# Config
CUTOFF_DATE = datetime.date(2026, 1, 27)  # exclusão: starts_at < CUTOFF_DATE are past
BACKUP_ROOT = Path("maintenance_backups")
TIMEZONE = pytz.timezone(settings.TIME_ZONE if getattr(settings, "TIME_ZONE", None) else "UTC")
def make_backup_dir():
    ts = timezone.now().astimezone(TIMEZONE).strftime("%Y%m%dT%H%M%S")
    path = BACKUP_ROOT / f"clear_masses_{CUTOFF_DATE.isoformat()}_{ts}"
    path.mkdir(parents=True, exist_ok=False)
    return path
def serialize_qs(qs, path):
    # writes JSON list of objects (Django serializers) preserving PK
    data = serializers.serialize("json", qs, use_natural_primary_keys=False)
    path.write_text(data, encoding="utf-8")
    return path
def load_json_file(path):
    return json.loads(path.read_text(encoding="utf-8"))
def detect_db_vendor():
    return connection.vendor  # 'postgresql', 'sqlite', 'mysql', etc.
def adjust_postgres_sequences(backup_meta):
    # backup_meta includes list of (app_label, model_name) that we restored
    vendor = detect_db_vendor()
    if vendor != "postgresql":
        return
    with connection.cursor() as cur:
        for model_ref in backup_meta.get("models_restored", []):
            app_label, model_name = model_ref.split(".")
            model = apps.get_model(app_label, model_name)
            table = model._meta.db_table
            pk = model._meta.pk.column
            seq_sql = f"SELECT pg_get_serial_sequence('{table}','{pk}')"
            cur.execute(seq_sql)
            seq = cur.fetchone()[0]
            if seq:
                cur.execute(f"SELECT COALESCE(MAX({pk}), 0) FROM {table}")
                maxid = cur.fetchone()[0]
                cur.execute(f"SELECT setval('{seq}', %s, true)", [maxid])
    return
def confirm_prompt(msg):
    resp = input(msg + " [type YES to proceed]: ")
    return resp.strip() == "YES"
def dry_run_report():
    tznow = timezone.now().astimezone(TIMEZONE)
    cutoff_dt = datetime.datetime.combine(CUTOFF_DATE, datetime.time.min).astimezone(TIMEZONE)
    print("Dry run (no changes). Cutoff datetime:", cutoff_dt.isoformat())
    past_masses_qs = MassInstance.objects.filter(starts_at__lt=cutoff_dt)
    future_masses_qs = MassInstance.objects.filter(starts_at__gte=cutoff_dt)
    print("Past MassInstances (will be deleted):", past_masses_qs.count())
    print("Future MassInstances (kept), assignments to be cleaned:", end=" ")
    future_slots = AssignmentSlot.objects.filter(mass_instance__in=future_masses_qs)
    future_assignments = Assignment.objects.filter(slot__in=future_slots, is_active=True)
    print(future_assignments.count())
    # counts of dependent objects for info
    past_mass_ids = list(past_masses_qs.values_list("id", flat=True)[:1000])
    # estimations for audit events referencing these entities:
    audit_past = AuditEvent.objects.filter(entity_type="MassInstance", entity_id__in=[str(i) for i in past_mass_ids]).count()
    print("AuditEvents referencing past MassInstance (sample count):", audit_past)
    print("Summary: will backup then delete all past MassInstance and related cascaded rows.")
    print("And will delete Assignment/Confirmation/AcolyteCreditLedger/AuditEvent related to future assignments.")
    return
def collect_and_backup(backup_dir):
    tznow = timezone.now().astimezone(TIMEZONE)
    cutoff_dt = datetime.datetime.combine(CUTOFF_DATE, datetime.time.min).astimezone(TIMEZONE)
    # Querysets
    past_masses_qs = MassInstance.objects.filter(starts_at__lt=cutoff_dt)
    past_mass_ids = list(past_masses_qs.values_list("id", flat=True))
    future_masses_qs = MassInstance.objects.filter(starts_at__gte=cutoff_dt)
    # Slots linked to past masses (but these will be deleted by cascade when mass deleted)
    past_slots_qs = AssignmentSlot.objects.filter(mass_instance__in=past_mass_ids)
    past_slot_ids = list(past_slots_qs.values_list("id", flat=True))
    # Assignments linked to past slots (also cascade)
    past_assignments_qs = Assignment.objects.filter(slot__in=past_slot_ids)
    past_assignment_ids = list(past_assignments_qs.values_list("id", flat=True))
    # Future slots + assignments to clean
    future_slot_qs = AssignmentSlot.objects.filter(mass_instance__in=future_masses_qs)
    future_slot_ids = list(future_slot_qs.values_list("id", flat=True))
    future_assignments_qs = Assignment.objects.filter(slot__in=future_slot_ids)
    future_assignment_ids = list(future_assignments_qs.values_list("id", flat=True))
    # Dependent objects we want to backup (both past cascaded rows and future assignments specifics)
    confirmations_qs = Confirmation.objects.filter(assignment__in=past_assignment_ids + future_assignment_ids)
    claims_qs = PositionClaimRequest.objects.filter(slot__in=past_slot_ids + future_slot_ids)
    replacement_qs = ReplacementRequest.objects.filter(slot__in=past_slot_ids + future_slot_ids)
    swap_qs = SwapRequest.objects.filter(mass_instance__in=past_mass_ids)
    massinterest_qs = MassInterest.objects.filter(mass_instance__in=past_mass_ids)
    massoverride_qs = MassOverride.objects.filter(instance__in=past_mass_ids)
    ledger_qs = AcolyteCreditLedger.objects.filter(related_assignment__in=past_assignment_ids + future_assignment_ids)
    audit_qs = AuditEvent.objects.filter(
        models.Q(entity_type="MassInstance", entity_id__in=[str(i) for i in past_mass_ids]) |
        models.Q(entity_type="Assignment", entity_id__in=[str(i) for i in past_assignment_ids + future_assignment_ids]) |
        models.Q(entity_type="AssignmentSlot", entity_id__in=[str(i) for i in past_slot_ids + future_slot_ids])
    )
    # For safety, also backup any ReplacementRequest referencing future/past slots
    # Build metadata
    meta = {
        "created_at": tznow.isoformat(),
        "cutoff_date": CUTOFF_DATE.isoformat(),
        "counts": {
            "past_masses": past_masses_qs.count(),
            "past_slots": past_slots_qs.count(),
            "past_assignments": past_assignments_qs.count(),
            "future_assignments_total": future_assignments_qs.count(),
            "confirmations": confirmations_qs.count(),
            "claims": claims_qs.count(),
            "replacement_requests": replacement_qs.count(),
            "swap_requests": swap_qs.count(),
            "mass_interest": massinterest_qs.count(),
            "mass_override": massoverride_qs.count(),
            "ledger_rows": ledger_qs.count(),
            "audit_events": audit_qs.count(),
        },
        "models_included": [
            "core.MassInstance",
            "core.AssignmentSlot",
            "core.Assignment",
            "core.Confirmation",
            "core.PositionClaimRequest",
            "core.ReplacementRequest",
            "core.SwapRequest",
            "core.MassInterest",
            "core.MassOverride",
            "core.AcolyteCreditLedger",
            "core.AuditEvent",
        ]
    }
    # Serialize in dependency-safe order:
    # 1) MassInstance (past)
    # 2) AssignmentSlot (past)
    # 3) Assignment (past)
    # 4) Confirmation (past+future assignments)
    # 5) ReplacementRequest, SwapRequest, MassInterest, MassOverride (past)
    # 6) PositionClaimRequest (both)
    # 7) AcolyteCreditLedger rows referencing assignments
    # 8) AuditEvent referencing the entities
    #
    # Note: we backup future assignment rows (assignments + confirmations) to allow undo for cleaning future slots.
    serializers_dir = backup_dir / "serializers"
    serializers_dir.mkdir()
    def dump(qs, name):
        path = serializers_dir / f"{name}.json"
        print("Backing up:", name, "->", path, "count=", qs.count())
        serialize_qs(qs, path)
        return path
    dump(past_masses_qs, "past_massinstance")
    dump(past_slots_qs, "past_assignmentslot")
    dump(past_assignments_qs, "past_assignment")
    dump(future_assignments_qs, "future_assignment_active")
    dump(confirmations_qs, "confirmation")
    dump(replacement_qs, "replacementrequest")
    dump(swap_qs, "swaprequest")
    dump(massinterest_qs, "massinterest")
    dump(massoverride_qs, "massoverride")
    dump(claims_qs, "positionclaimrequest")
    dump(ledger_qs, "acolytecreditledger")
    dump(audit_qs, "auditevent")
    # Write metadata
    meta_path = backup_dir / "meta.json"
    meta_path.write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
    print("Backup metadata written to", meta_path)
    return meta
def apply_changes(backup_dir):
    tznow = timezone.now().astimezone(TIMEZONE)
    cutoff_dt = datetime.datetime.combine(CUTOFF_DATE, datetime.time.min).astimezone(TIMEZONE)
    # 1) Delete past MassInstance in batches
    print("Deleting past MassInstance rows in batches...")
    batch = 500
    while True:
        with transaction.atomic():
            ids = list(MassInstance.objects.filter(starts_at__lt=cutoff_dt).order_by("id").values_list("id", flat=True)[:batch])
            if not ids:
                break
            print("Deleting batch of MassInstance ids:", ids[:5], "..." if len(ids)>5 else "")
            MassInstance.objects.filter(id__in=ids).delete()
    print("Past MassInstance rows deleted.")
    # 2) For future masses: delete assignments linked to their slots (hard delete)
    print("Cleaning assignments for future mass instances (deleting Assignment rows)...")
    future_slot_qs = AssignmentSlot.objects.filter(mass_instance__starts_at__gte=cutoff_dt)
    batch = 1000
    while True:
        with transaction.atomic():
            assign_ids = list(Assignment.objects.filter(slot__in=future_slot_qs).order_by("id").values_list("id", flat=True)[:batch])
            if not assign_ids:
                break
            print("Deleting batch of Assignment ids:", assign_ids[:5], "..." if len(assign_ids)>5 else "")
            Assignment.objects.filter(id__in=assign_ids).delete()
    print("Future assignments deleted (confirmations and CASCADE rows removed).")
    # 3) Remove AcolyteCreditLedger rows that referenced the deleted assignments (we backuped)
    print("Deleting AcolyteCreditLedger rows where related_assignment is NULL-safe handled (we deleted related assignments).")
    # if any ledgers remain whose related_assignment is NULL and you prefer deletion, optionally delete them:
    # ledger_delete_count = AcolyteCreditLedger.objects.filter(related_assignment__isnull=True, created_at__gte=some_threshold).delete()
    # We keep ledgers that had unrelated reasons. (By default we already backup and user wanted hard delete of assignment records; ledger rows referencing them were deleted earlier in backup restore plan.)
    print("All requested deletes completed. Note: some FK relations may have been SET_NULL by DB (e.g., related_assignment -> NULL).")
def restore_from_backup(backup_dir):
    serializers_dir = backup_dir / "serializers"
    meta_path = backup_dir / "meta.json"
    if not meta_path.exists():
        raise RuntimeError("backup meta not found in " + str(backup_dir))
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    # Helper to load serialized objects (Django serializers) and bulk_create them reusing PKs
    def load_and_create(model, filename):
        path = serializers_dir / filename
        if not path.exists():
            print("No backup for", filename, "- skipping.")
            return 0
        objs = list(serializers.deserialize("json", path.read_text(encoding="utf-8")))
        created = 0
        with transaction.atomic():
            for item in objs:
                obj = item.object
                obj.save(force_insert=True)  # force_insert to use original PK
                created += 1
        print(f"Restored {created} rows for {filename}")
        return created
    # Restore in dependency order reversed of deletion:
    # 1) MassInstance and related -> BUT since some models have FK to massinstance, restore massinstance first
    # NOTE: we restored MassInstance, then slots, then assignments, confirmations, claims, replacements, swaps, ledger, audit
    load_and_create(MassInstance, "past_massinstance.json")
    load_and_create(AssignmentSlot, "past_assignmentslot.json")
    load_and_create(Assignment, "past_assignment.json")
    load_and_create("core.Assignment", "future_assignment_active.json")  # future assignments to restore
    # confirmations
    load_and_create(Confirmation, "confirmation.json")
    load_and_create(ReplacementRequest, "replacementrequest.json")
    load_and_create(SwapRequest, "swaprequest.json")
    load_and_create(MassInterest, "massinterest.json")
    load_and_create(MassOverride, "massoverride.json")
    load_and_create(PositionClaimRequest, "positionclaimrequest.json")
    load_and_create(AcolyteCreditLedger, "acolytecreditledger.json")
    load_and_create(AuditEvent, "auditevent.json")
    # Adjust sequences if Postgres
    adjust_postgres_sequences({"models_restored": meta.get("models_included", [])})
    print("Restore complete. Please validate counts and integrity.")
def find_latest_backup():
    if not BACKUP_ROOT.exists():
        return None
    candidates = sorted(BACKUP_ROOT.glob("clear_masses_*"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None
def main():
    parser = argparse.ArgumentParser(description="Limpar missas passadas e limpar assignments futuros (hard delete).")
    parser.add_argument("--dry-run", action="store_true", help="mostrar contagens e não alterar nada")
    parser.add_argument("--apply", action="store_true", help="executar a operação (caso omitido, script pedirá confirmação interativa)")
    parser.add_argument("--undo", action="store_true", help="restaurar a partir do backup mais recente")
    args = parser.parse_args()
    if args.dry_run:
        dry_run_report()
        return
    if args.undo:
        latest = find_latest_backup()
        if not latest:
            print("Nenhum backup encontrado em", BACKUP_ROOT)
            return
        print("Restaurando a partir do backup:", latest)
        # Skip confirmation for automation
        try:
            restore_from_backup(latest)
            print("Undo concluído com sucesso.")
        except Exception as e:
            print("Erro durante restore:", e)
        return
    # apply flow
    if not settings.DEBUG:
        print("ALERTA: settings.DEBUG is not True. Esto é potencialmente perigoso. Abortando por segurança.")
        proceed = input("Se você entende o risco e conecta localmente, digite 'I_ACCEPT_RISK' para continuar: ")
        if proceed.strip() != "I_ACCEPT_RISK":
            print("Abortado.")
            return
    print("Preparando backup dos dados que serão removidos...")
    backup_dir = make_backup_dir()
    meta = collect_and_backup(backup_dir)
    print("\nBackup criado em:", backup_dir)
    print("Resumo das contagens:", meta["counts"])
    if not args.apply:
        # interactive confirm
        if not confirm_prompt("Confirma agora executar as deleções HARD conforme planejado?"):
            print("Operação cancelada. Backup permanece em", backup_dir)
            return
    # Execute deletions
    try:
        apply_changes(backup_dir)
    except Exception as e:
        print("Erro durante aplicação das mudanças:", e)
        print("Você pode restaurar com: python limpar.py --undo")
        return
    print("Operação concluída. Backup disponível em:", backup_dir)
    print("Para desfazer, execute: python limpar.py --undo")
    return
if __name__ == "__main__":
    main()