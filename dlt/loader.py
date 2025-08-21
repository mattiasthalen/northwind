"""
Pipeline Helper Functions

Common utilities for DLT pipeline execution and configuration.
"""

import dlt
import logging
import os
import subprocess

from datetime import datetime, timezone


def setup_pipeline_logging(pipeline_name: str, log_dir: str = "logs"):
    """Sets up file logging for DLT pipelines with timestamped filenames."""
    # Ensure log directory exists to avoid runtime failures during pipeline execution
    os.makedirs(log_dir, exist_ok=True)

    # Use UTC timestamps to avoid timezone confusion in distributed environments
    # Separate files per run enable parallel execution without log conflicts
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_filename = f"dlt__{pipeline_name}__{timestamp}.log"
    log_filepath = os.path.join(log_dir, log_filename)

    # Target DLT's logger specifically since it handles all framework events
    # File handler captures detailed execution info that console output misses
    logger = logging.getLogger('dlt')
    handler = logging.FileHandler(log_filepath)
    logger.addHandler(handler)

    print(f"Logging configured for {pipeline_name}. Log file: {log_filepath}")
    return log_filepath


def load_data_pipeline(source_func, env="dev", add_limit=None, **pipeline_kwargs):
    """
    Executes a DLT data loading pipeline with standardized configuration.

    Args:
        source_func: Function that returns the DLT source
        pipeline_name: Name for the pipeline (typically the script name)
        env: Environment mode ("dev" or "prod")
        **pipeline_kwargs: Additional pipeline configuration overrides

    Returns:
        Load information from the pipeline execution
    """
    
    dataset_name = pipeline_name = source_func().name

    # Setup dev parameters
    dev_mode = env != "prod"
    env_name = "dev" if dev_mode else "prod"
    next_item_mode = "fifo" if dev_mode else "round_robin"
    progress = "enlighten" if dev_mode else "log"

    schema_path = "./dlt/schemas/dev" if dev_mode else "./dlt/schemas/prod"
    export_schema_path = os.path.join(schema_path, "export")
    import_schema_path = os.path.join(schema_path, "import")

    if dev_mode:
        try:
            branch_name = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip().decode('utf-8')
        except:
            branch_name = "dev"
        finally:
            dataset_name = f"{dataset_name}__dev__{branch_name.replace('-', '_')}"
            export_schema_path = os.path.join(schema_path, branch_name, "export")
            import_schema_path = os.path.join(schema_path, branch_name, "import")

    print(f"Running {pipeline_name} in {'dev' if dev_mode else 'prod'} using {next_item_mode} for item processing")
    os.environ["NEXT_ITEM_MODE"] = next_item_mode

    # Initialize logging for pipeline monitoring
    setup_pipeline_logging(dataset_name)

    # Default pipeline configuration
    default_config = {
        "pipeline_name": pipeline_name,
        "destination": dlt.destinations.filesystem(),
        "dataset_name": dataset_name,
        "progress": progress,
        "export_schema_path": export_schema_path,
        "import_schema_path": import_schema_path,
        "dev_mode": dev_mode
    }

    # Merge with any custom pipeline kwargs
    pipeline_config = {**default_config, **pipeline_kwargs}

    # Configure DLT pipeline
    pipeline = dlt.pipeline(**pipeline_config)

    # Execute pipeline with Parquet output for compression
    load_info = pipeline.run(source_func().add_limit(add_limit), loader_file_format="parquet")
    print(load_info)

    return load_info