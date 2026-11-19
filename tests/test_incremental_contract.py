"""Publication identity contracts; distributed math is tested by accept_incremental.py."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from src import incremental_pipeline as inc
from src.spark_pipeline import TIE_POLICY, publish_run


def fixture(tmp_path):
    base_path = tmp_path / "base"
    base_path.mkdir()
    (base_path / "_SUCCESS").touch()
    base = {"run_id": "base", "status": "success", "format": "parquet", "k": 10,
        "tie_policy": TIE_POLICY, "tables": {n: {} for n in ("clean_edges", "candidate_scores", "recommendations")}}
    (base_path / "manifest.json").write_text(json.dumps(base), encoding="utf-8")
    changes = tmp_path / "changes.txt"
    changes.write_text("ADD 0 1\n", encoding="utf-8")
    args = SimpleNamespace(base_run=base_path, changes=changes, output=str(tmp_path / "output"),
        run_id="batch1", format="parquet")
    return args, base


def test_committed_retry_is_noop_and_conflicting_payload_is_rejected(tmp_path):
    args, base = fixture(tmp_path)
    staging, final, lock, identity, existing = inc.prepare_batch(args, base)
    assert existing is None
    committed = dict(base, run_id="batch1", request=identity)
    publish_run(staging, final, committed)
    lock.unlink()
    retry = inc.prepare_batch(args, base)
    assert retry[0] is None and retry[4] == committed
    args.changes.write_text("DELETE 0 1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="DIFFERENT"):
        inc.prepare_batch(args, base)


def test_base_snapshot_is_part_of_request_and_active_lock_is_not_a_commit(tmp_path):
    args, base = fixture(tmp_path)
    _, _, lock, original, _ = inc.prepare_batch(args, base)
    with pytest.raises(FileExistsError):
        inc.prepare_batch(args, base)
    lock.unlink()
    (args.base_run / "manifest.json").write_text(json.dumps(dict(base, run_id="base2")), encoding="utf-8")
    assert inc.request_identity(args, base)["request_sha256"] != original["request_sha256"]


def test_unpublished_or_incompatible_base_is_rejected(tmp_path):
    args, base = fixture(tmp_path)
    args.format = "iceberg"
    with pytest.raises(ValueError, match="formats must match"):
        inc.prepare_batch(args, base)
    (args.base_run / "_SUCCESS").unlink()
    with pytest.raises(ValueError, match="published"):
        inc.load_manifest(args.base_run)


def test_lazy_read_uses_frozen_input_even_if_original_changes(tmp_path):
    args, base = fixture(tmp_path)
    original = inc.sha256(args.changes)
    stage = tmp_path / "stage"
    stage.mkdir()
    snapshot = inc.snapshot_changes(args.changes, stage, original)
    args.changes.write_text("DELETE 0 1\n", encoding="utf-8")
    assert inc.sha256(snapshot) == original
    assert snapshot.read_text(encoding="utf-8") == "ADD 0 1\n"


def test_source_mutation_between_hash_and_snapshot_rejects_batch(tmp_path):
    args, base = fixture(tmp_path)
    original = inc.sha256(args.changes)
    args.changes.write_text("DELETE 0 1\n", encoding="utf-8")
    stage = tmp_path / "stage"
    stage.mkdir()
    with pytest.raises(ValueError, match="mutated"):
        inc.snapshot_changes(args.changes, stage, original)
