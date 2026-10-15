from dagster import asset, Definitions, ScheduleDefinition, DefaultScheduleStatus
from dagster_dbt import DbtCliResource, dbt_assets
from pathlib import Path
import subprocess

DBT_PROJECT_DIR = Path.home() / "ssh-threat-pipeline/dbt_ssh"  # adjust path!


@asset
def run_simulation():
    """Calls the API producer: replays the log through the whole pipeline."""
    subprocess.run(["python", "../producer/producer.py"], check=True)


@dbt_assets(manifest=Path(DBT_PROJECT_DIR) / "target/manifest.json")
def dbt_models(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


defs = Definitions(
    assets=[run_simulation, dbt_models],
    resources={"dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR)},
    schedules=[
        ScheduleDefinition(
            job_name="dbt_models",
            cron_schedule="*/15 * * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        )
    ],
)
