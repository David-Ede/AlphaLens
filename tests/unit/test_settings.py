from __future__ import annotations

from pathlib import Path

from fg.settings import get_settings


def test_get_settings_resolves_repo_root_from_notebooks_dir(monkeypatch) -> None:
    project_root = Path(__file__).resolve().parents[2]

    monkeypatch.chdir(project_root / "notebooks")
    monkeypatch.setenv("APP_ENV", "demo")
    monkeypatch.delenv("DATA_ROOT", raising=False)
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    get_settings.cache_clear()

    settings = get_settings()

    assert settings.ui_defaults["lookback_options"] == [5, 10, 15, 20]
    assert settings.data_dirs["root"] == project_root / "data"
    assert settings.data_dirs["duckdb_path"] == project_root / "data" / "fg.duckdb"

    get_settings.cache_clear()
