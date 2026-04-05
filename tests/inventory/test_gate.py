"""Testes para inventory/gate.py — InventoryGate com 4 modos."""

from __future__ import annotations

import pytest

from sas2dbx.inventory import GateResult, InventoryEntry
from sas2dbx.inventory.checker import CheckResult
from sas2dbx.inventory.gate import InventoryGate


def _missing_entry(table: str = "telcostar.operacional.x") -> InventoryEntry:
    return InventoryEntry(
        table_short=table.split(".")[-1],
        libname="TELCO",
        role="SOURCE",
        sas_context="FROM",
        table_fqn=table,
        exists_in_databricks=False,
    )


def _check(total: int, found: int, missing_count: int = 0) -> CheckResult:
    missing = [_missing_entry(f"telcostar.operacional.t{i}") for i in range(missing_count)]
    return CheckResult(total=total, found=found, missing=missing)


class TestGatePass:
    def test_no_missing_is_pass(self) -> None:
        result = _check(total=5, found=5)
        gate = InventoryGate().evaluate(result, mode="STRICT")
        assert gate.decision == "PASS"
        assert gate.missing_count == 0

    def test_pass_in_lenient_mode(self) -> None:
        result = _check(total=5, found=5)
        gate = InventoryGate().evaluate(result, mode="LENIENT")
        assert gate.decision == "PASS"

    def test_pass_in_force_mode(self) -> None:
        result = _check(total=5, found=5)
        gate = InventoryGate().evaluate(result, mode="FORCE")
        assert gate.decision == "PASS"


class TestGateStrict:
    def test_one_missing_blocks(self) -> None:
        result = _check(total=5, found=4, missing_count=1)
        gate = InventoryGate().evaluate(result, mode="STRICT")
        assert gate.decision == "BLOCK"
        assert gate.missing_count == 1

    def test_all_missing_blocks(self) -> None:
        result = _check(total=3, found=0, missing_count=3)
        gate = InventoryGate().evaluate(result, mode="STRICT")
        assert gate.decision == "BLOCK"
        assert gate.missing_count == 3

    def test_message_contains_table_names(self) -> None:
        result = _check(total=2, found=1, missing_count=1)
        gate = InventoryGate().evaluate(result, mode="STRICT")
        assert "t0" in gate.message or "telcostar" in gate.message


class TestGateLenient:
    def test_under_threshold_warns(self) -> None:
        """1 de 10 = 10% < 20% → WARN."""
        result = _check(total=10, found=9, missing_count=1)
        gate = InventoryGate().evaluate(result, mode="LENIENT")
        assert gate.decision == "WARN"

    def test_at_threshold_blocks(self) -> None:
        """2 de 10 = 20% >= 20% → BLOCK."""
        result = _check(total=10, found=8, missing_count=2)
        gate = InventoryGate().evaluate(result, mode="LENIENT")
        assert gate.decision == "BLOCK"

    def test_above_threshold_blocks(self) -> None:
        """3 de 5 = 60% → BLOCK."""
        result = _check(total=5, found=2, missing_count=3)
        gate = InventoryGate().evaluate(result, mode="LENIENT")
        assert gate.decision == "BLOCK"


class TestGateForce:
    def test_missing_but_warns(self) -> None:
        result = _check(total=5, found=3, missing_count=2)
        gate = InventoryGate().evaluate(result, mode="FORCE")
        assert gate.decision == "WARN"
        assert gate.missing_count == 2

    def test_all_missing_still_warns(self) -> None:
        result = _check(total=3, found=0, missing_count=3)
        gate = InventoryGate().evaluate(result, mode="FORCE")
        assert gate.decision == "WARN"


class TestGateSkip:
    def test_skip_mode_returns_skip(self) -> None:
        result = CheckResult(total=0, found=0, mode="skip")
        gate = InventoryGate().evaluate(result)
        assert gate.decision == "SKIP"

    def test_explicit_skip_mode(self) -> None:
        result = _check(total=5, found=5)
        gate = InventoryGate().evaluate(result, mode="SKIP")
        assert gate.decision == "SKIP"

    def test_skip_missing_count_zero(self) -> None:
        result = CheckResult(total=0, found=0, mode="skip")
        gate = InventoryGate().evaluate(result)
        assert gate.missing_count == 0
