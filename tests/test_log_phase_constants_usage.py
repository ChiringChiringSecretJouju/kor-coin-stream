from __future__ import annotations

from pathlib import Path


def test_connection_modules_do_not_use_inline_phase_literals() -> None:
    root = Path(__file__).resolve().parents[1]
    targets = [
        root / "src" / "core" / "connection" / "handlers" / "regional_base.py",
        root / "src" / "core" / "connection" / "handlers" / "korea_handler.py",
        root / "src" / "core" / "connection" / "handlers" / "global_handler.py",
        root / "src" / "core" / "connection" / "services" / "health_monitor.py",
        root / "src" / "core" / "connection" / "services" / "error_handler.py",
    ]

    violations: list[str] = []
    for path in targets:
        content = path.read_text(encoding="utf-8")
        if 'phase="' in content or "phase='" in content:
            violations.append(str(path))

    assert not violations, f"Inline phase literals found in: {violations}"


def test_log_phase_constants_are_unique() -> None:
    import src.core.connection.utils.logging.log_phases as log_phases

    phase_values = [
        value
        for name, value in vars(log_phases).items()
        if name.startswith("PHASE_") and isinstance(value, str)
    ]
    assert len(phase_values) == len(set(phase_values))
