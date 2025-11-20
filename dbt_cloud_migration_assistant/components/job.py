"""Job component for Dagster Designer."""

from typing import Optional, Any
from pydantic import field_validator

import dagster as dg
from dagster._core.definitions.asset_selection import AssetSelection


class JobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Component for creating jobs from YAML configuration."""

    job_name: str
    asset_selection: list[str]
    description: Optional[str] = None
    tags: Optional[dict[str, str]] = None
    config: Optional[dict] = None
    
    @field_validator('tags', mode='before')
    @classmethod
    def convert_tag_values_to_strings(cls, v: Any) -> Optional[dict[str, str]]:
        """Convert all tag values to strings (YAML may parse numbers as ints)."""
        if v is None:
            return None
        if isinstance(v, dict):
            return {k: str(val) for k, val in v.items()}
        return v

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Build Dagster definitions from component parameters."""
        # Handle asset selection patterns
        # Patterns like "analytics.*" should select all assets
        # Individual keys like "my_model" should select specific assets
        asset_selections = []
        
        for key_str in self.asset_selection:
            if key_str.endswith(".*"):
                # Wildcard pattern - select all assets (dbt components create assets without prefix)
                # Use AssetSelection.all() for wildcard patterns
                asset_selections.append(AssetSelection.all())
            elif "/" in key_str:
                # Multi-part key like "path/to/asset"
                parts = key_str.split("/")
                asset_selections.append(AssetSelection.keys(dg.AssetKey(parts)))
            else:
                # Single-part key like "my_model"
                asset_selections.append(AssetSelection.keys(dg.AssetKey([key_str])))
        
        # Combine all selections
        if len(asset_selections) == 1:
            asset_sel = asset_selections[0]
        else:
            # Union of all selections
            asset_sel = asset_selections[0]
            for sel in asset_selections[1:]:
                asset_sel = asset_sel | sel

        # Create job
        job = dg.define_asset_job(
            name=self.job_name,
            selection=asset_sel,
            description=self.description,
            tags=self.tags or {},
            config=self.config,
        )

        return dg.Definitions(jobs=[job])

