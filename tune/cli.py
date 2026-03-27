"""Tune CLI — tune init and tune start."""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.prompt import Confirm, Prompt

console = Console()


def _require_config_input(workspace_root: str | None, analysis_dir: str | None) -> Path:
    raw = workspace_root or analysis_dir
    if not raw:
        console.print("[red]Error:[/red] provide --workspace-root or --analysis-dir.")
        sys.exit(1)
    return Path(raw).expanduser().resolve()


@click.group()
def cli():
    """Tune — AI-powered bioinformatics analysis platform."""


@cli.command()
def init():
    """Interactive setup wizard. Run once before starting the server."""
    console.print("\n[bold cyan]Welcome to Tune[/bold cyan] — bioinformatics analysis platform\n")

    # --- directories ---
    workspace_root = Prompt.ask(
        "Path to your [bold]workspace root[/bold] (Tune will use data/ and workspace/ under this directory)",
        default=str((Path.cwd() / "analysis").resolve()),
    )

    # --- database ---
    db_url = Prompt.ask(
        "PostgreSQL connection URL",
        default="postgresql+psycopg://tune:tune@localhost:5432/tune",
    )

    # --- primary LLM ---
    console.print("\n[bold]Primary LLM configuration[/bold]")
    primary_provider = Prompt.ask(
        "Provider", choices=["anthropic", "openai", "openai_compatible"], default="anthropic"
    )
    primary_model = Prompt.ask(
        "Model",
        default="claude-opus-4-6" if primary_provider == "anthropic" else "gpt-4o",
    )
    primary_key = Prompt.ask("API key", password=True)
    primary_base_url = None
    if primary_provider == "openai_compatible":
        primary_base_url = Prompt.ask("Base URL")

    # --- fallback LLM ---
    want_fallback = Confirm.ask("\nConfigure a [bold]fallback LLM[/bold]?", default=True)
    fallback_cfg = None
    if want_fallback:
        fb_provider = Prompt.ask(
            "Fallback provider",
            choices=["anthropic", "openai", "openai_compatible"],
            default="openai",
        )
        fb_model = Prompt.ask(
            "Fallback model",
            default="gpt-4o" if fb_provider == "openai" else "claude-sonnet-4-6",
        )
        fb_key = Prompt.ask("Fallback API key", password=True)
        fb_base_url = None
        if fb_provider == "openai_compatible":
            fb_base_url = Prompt.ask("Fallback base URL")
        fallback_cfg = {
            "provider": fb_provider,
            "model": fb_model,
            "api_key": fb_key,
            "base_url": fb_base_url,
        }

    # --- pixi ---
    pixi_path = Prompt.ask("Path to pixi executable", default="pixi")

    # --- build and save config ---
    from tune.core.config import (
        ApiConfig,
        TuneConfig,
        derive_workspace_dirs,
        save_config,
        validate_config,
    )

    # Determine api_style from provider
    primary_api_style = "anthropic" if primary_provider == "anthropic" else "openai_compatible"

    primary_cfg = ApiConfig.new(
        name="主模型",
        provider=primary_provider,
        api_style=primary_api_style,
        model_name=primary_model,
        api_key=primary_key,
        base_url=primary_base_url,
    )

    llm_configs = [primary_cfg]
    active_id = primary_cfg.id

    root_path = Path(workspace_root).expanduser().resolve()
    data_dir, analysis_dir = derive_workspace_dirs(root_path)
    data_dir.mkdir(parents=True, exist_ok=True)
    analysis_dir.mkdir(parents=True, exist_ok=True)

    cfg_data = {
        "workspace_root": str(root_path),
        "data_dir": data_dir,
        "analysis_dir": analysis_dir,
        "database_url": db_url,
        "pixi_path": pixi_path,
        "llm_configs": [c.model_dump() for c in llm_configs],
        "active_llm_config_id": active_id,
    }

    try:
        cfg = TuneConfig(**cfg_data)
    except Exception as e:
        console.print(f"[red]Configuration error:[/red] {e}")
        sys.exit(1)

    errors = validate_config(cfg)
    if errors:
        console.print("[red]Validation errors:[/red]")
        for err in errors:
            console.print(f"  • {err}")
        sys.exit(1)

    save_config(cfg)
    config_root = cfg.workspace_root or cfg.analysis_dir
    console.print(f"\n[green]✓[/green] Config saved to {config_root / '.tune' / 'config.yaml'}")
    console.print(f"[green]✓[/green] Data directory: {cfg.data_dir}")
    console.print(f"[green]✓[/green] Analysis directory: {cfg.analysis_dir}")
    console.print("\nRun [bold]tune start[/bold] to launch the server.")


@cli.command("sync-resource-entities")
@click.option("--workspace-root", default=None, help="Workspace root containing .tune/config.yaml")
@click.option("--analysis-dir", default=None, help="Legacy config path (analysis/workspace or workspace root)")
@click.option("--project-id", default=None, help="Sync only one project")
def sync_resource_entities(workspace_root: str | None, analysis_dir: str | None, project_id: str | None):
    """Backfill / reconcile resource entities for one project or all projects."""
    from tune.core.config import load_config, set_config

    cfg_input = _require_config_input(workspace_root, analysis_dir)
    try:
        cfg = load_config(cfg_input)
    except FileNotFoundError as exc:
        console.print(f"[red]Error:[/red] {exc}")
        sys.exit(1)

    set_config(cfg)

    async def _run() -> list[dict]:
        from tune.core.database import get_session_factory
        from tune.core.resources.entities import (
            sync_all_projects_resource_entities,
            sync_project_resource_entities_by_id,
        )

        async with get_session_factory()() as session:
            if project_id:
                return [await sync_project_resource_entities_by_id(session, project_id)]
            return await sync_all_projects_resource_entities(session)

    try:
        results = asyncio.run(_run())
    except ValueError as exc:
        console.print(f"[red]Error:[/red] {exc}")
        sys.exit(1)

    total_changes = sum(int(item.get("changes") or 0) for item in results)
    total_entities = sum(int(item.get("resource_entity_count") or 0) for item in results)

    if not results:
        console.print("[yellow]No projects found.[/yellow]")
        return

    for item in results:
        console.print(
            f"[cyan]{item['project_name'] or item['project_id']}[/cyan]: "
            f"files={item['file_count']} known_paths={item['known_path_count']} "
            f"changes={item['changes']} resource_entities={item['resource_entity_count']}"
        )

    console.print(
        f"\n[green]✓[/green] Synced {len(results)} project(s); "
        f"changes={total_changes}, resource_entities={total_entities}"
    )


@cli.command()
@click.option("--workspace-root", default=None, help="Workspace root containing .tune/config.yaml")
@click.option("--analysis-dir", default=None, help="Legacy config path (analysis/workspace or workspace root)")
@click.option("--host", default="0.0.0.0", help="Bind host")
@click.option("--port", default=8000, type=int, help="Bind port")
@click.option("--reload", is_flag=True, default=False, help="Enable auto-reload (development)")
def start(workspace_root: str | None, analysis_dir: str | None, host: str, port: int, reload: bool):
    """Start the Tune server (FastAPI + Procrastinate worker)."""
    from tune.core.config import load_config, set_config, validate_config

    cfg_input = _require_config_input(workspace_root, analysis_dir)
    try:
        cfg = load_config(cfg_input)
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)

    errors = validate_config(cfg)
    if errors:
        console.print("[red]Startup validation failed:[/red]")
        for err in errors:
            console.print(f"  • {err}")
        sys.exit(1)

    set_config(cfg)

    # Export env var so uvicorn --reload child processes can re-load config without CLI
    if cfg.workspace_root:
        os.environ["TUNE_WORKSPACE_ROOT"] = str(cfg.workspace_root)
    os.environ["TUNE_ANALYSIS_DIR"] = str(cfg_input)

    # Run Alembic migrations
    console.print("[cyan]Applying database migrations…[/cyan]")
    _project_root = Path(__file__).parent.parent
    subprocess.run(["alembic", "upgrade", "head"], check=True, cwd=str(_project_root))

    # Apply Procrastinate schema
    console.print("[cyan]Applying Procrastinate schema…[/cyan]")
    asyncio.run(_apply_procrastinate_schema(cfg.database_url))

    console.print(f"\n[green]✓[/green] Tune starting on http://{host}:{port}\n")

    import uvicorn
    uvicorn.run(
        "tune.api.app:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info",
    )


async def _apply_procrastinate_schema(db_url: str):
    import procrastinate
    import psycopg_pool
    conninfo = db_url.replace("postgresql+psycopg://", "postgresql://")
    pool = psycopg_pool.AsyncConnectionPool(conninfo, open=False)
    await pool.open()
    try:
        # Use a temporary App to avoid polluting the global proc_app singleton's
        # connector state across event loop boundaries (cli asyncio.run → uvicorn loop).
        temp_app = procrastinate.App(connector=procrastinate.PsycopgConnector())
        async with temp_app.open_async(pool=pool):
            try:
                await temp_app.schema_manager.apply_schema_async()
            except Exception as e:
                # Schema already exists — safe to continue
                cause = str(e.__cause__) if e.__cause__ else ""
                if "already exists" in str(e) or "already exists" in cause:
                    pass
                else:
                    raise
    finally:
        await pool.close()
