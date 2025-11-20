"""Sensor component for Dagster Designer."""

from typing import Optional, Literal

import dagster as dg


class SensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Component for creating sensors from YAML configuration."""

    sensor_name: str
    sensor_type: Literal["file", "run_status", "asset", "custom"]
    job_name: str
    description: Optional[str] = None
    file_path: Optional[str] = None
    asset_key: Optional[str] = None  # For asset sensors
    monitored_job_name: Optional[str] = None  # For run_status sensors - the job to monitor
    run_status: Optional[str] = "SUCCESS"  # For run_status sensors: SUCCESS, FAILURE, etc.
    minimum_interval_seconds: int = 30
    default_status: str = "RUNNING"

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build Dagster definitions from component parameters."""
        # Create sensor based on type
        if self.sensor_type == "file":
            sensor_def = self._create_file_sensor()
        elif self.sensor_type == "run_status":
            sensor_def = self._create_run_status_sensor()
        elif self.sensor_type == "asset":
            sensor_def = self._create_asset_sensor()
        else:
            sensor_def = self._create_custom_sensor()

        return dg.Definitions(sensors=[sensor_def])

    def _create_file_sensor(self):
        """Create a file-watching sensor."""
        from pathlib import Path
        
        file_path = self.file_path
        job_name = self.job_name

        @dg.sensor(
            name=self.sensor_name,
            job_name=job_name,
            minimum_interval_seconds=self.minimum_interval_seconds,
            description=self.description or f"Watch for file: {file_path}",
            default_status=(
                dg.DefaultSensorStatus.RUNNING
                if self.default_status == "RUNNING"
                else dg.DefaultSensorStatus.STOPPED
            ),
        )
        def file_sensor(context: dg.SensorEvaluationContext):
            """Sensor that triggers when a file exists."""
            if file_path and Path(file_path).exists():
                return dg.RunRequest(run_key=f"file_{file_path}_{context.cursor or '0'}")
            return dg.SkipReason(f"File {file_path} does not exist")

        return file_sensor

    def _create_run_status_sensor(self):
        """Create a run status sensor that monitors another job's run status."""
        # Map string status to DagsterRunStatus enum
        status_map = {
            "SUCCESS": dg.DagsterRunStatus.SUCCESS,
            "FAILURE": dg.DagsterRunStatus.FAILURE,
            "CANCELED": dg.DagsterRunStatus.CANCELED,
            "STARTED": dg.DagsterRunStatus.STARTED,
        }
        dagster_status = status_map.get(self.run_status, dg.DagsterRunStatus.SUCCESS)
        
        # Build sensor parameters
        sensor_params = {
            "name": self.sensor_name,
            "run_status": dagster_status,
            "request_job_name": self.job_name,  # The job to trigger
            "description": self.description or f"Monitor {self.monitored_job_name} for {self.run_status}",
            "minimum_interval_seconds": self.minimum_interval_seconds,
            "default_status": (
                dg.DefaultSensorStatus.RUNNING
                if self.default_status == "RUNNING"
                else dg.DefaultSensorStatus.STOPPED
            ),
        }
        
        # Add monitored_job_name if provided (filters to only monitor specific job)
        if self.monitored_job_name:
            sensor_params["monitored_job_name"] = self.monitored_job_name
        
        @dg.run_status_sensor(**sensor_params)
        def status_sensor(context: dg.RunStatusSensorContext):
            """Sensor that triggers on run status changes."""
            return dg.RunRequest(run_key=f"status_{context.dagster_run.run_id}")

        return status_sensor

    def _create_asset_sensor(self):
        """Create an asset sensor that monitors asset materializations."""
        # Parse asset key (handle both simple keys and path-like keys)
        # e.g., "my_asset" or "path/to/asset"
        asset_key_parts = self.asset_key.split("/") if self.asset_key else ["unknown"]
        monitored_asset_key = dg.AssetKey(asset_key_parts)

        @dg.asset_sensor(
            name=self.sensor_name,
            asset_key=monitored_asset_key,
            job_name=self.job_name,
            minimum_interval_seconds=self.minimum_interval_seconds,
            description=self.description or f"Monitor asset: {self.asset_key}",
            default_status=(
                dg.DefaultSensorStatus.RUNNING
                if self.default_status == "RUNNING"
                else dg.DefaultSensorStatus.STOPPED
            ),
        )
        def asset_sensor_fn(context: dg.SensorEvaluationContext, asset_event: dg.EventLogEntry):
            """Sensor that triggers when the monitored asset is materialized."""
            yield dg.RunRequest(
                run_key=f"{self.sensor_name}_{asset_event.run_id}",
                run_config={},
            )

        return asset_sensor_fn

    def _create_custom_sensor(self):
        """Create a custom sensor."""
        @dg.sensor(
            name=self.sensor_name,
            job_name=self.job_name,
            minimum_interval_seconds=self.minimum_interval_seconds,
            description=self.description or "Custom sensor",
            default_status=(
                dg.DefaultSensorStatus.RUNNING
                if self.default_status == "RUNNING"
                else dg.DefaultSensorStatus.STOPPED
            ),
        )
        def custom_sensor(context: dg.SensorEvaluationContext):
            """Custom sensor - modify this logic as needed."""
            # Default: always trigger
            return dg.RunRequest(run_key=context.cursor or "0")

        return custom_sensor

