"""
Volume-based router for optimal sink selection.

Routes high-volume jobs to Hudi, low-volume to Iceberg.
"""

from typing import Union, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from ..jobs.models import JobConfig


VOLUME_THRESHOLD = 10_000_000  # 10M records/day


class VolumeRouter:
    """Route data based on estimated volume."""
    
    def __init__(self, threshold: int = VOLUME_THRESHOLD):
        """Initialize volume router.
        
        Args:
            threshold: Volume threshold for routing (default: 10M records/day)
        """
        self.threshold = threshold
    
    def determine_sink(self, job_config: "JobConfig") -> Literal["hudi", "iceberg"]:
        """
        Determine appropriate sink based on volume.
        
        Logic:
        1. Check job_config.force_sink_type (manual override)
        2. If > VOLUME_THRESHOLD → return "hudi"
        3. If < VOLUME_THRESHOLD → return "iceberg"
        4. Default to "iceberg" if volume not specified
        
        Args:
            job_config: Job configuration
            
        Returns:
            Sink type ("hudi" or "iceberg")
        """
        # Check for manual override
        if hasattr(job_config, 'force_sink_type') and job_config.force_sink_type:
            return job_config.force_sink_type
        
        # Check estimated volume
        volume = getattr(job_config, 'estimated_daily_volume', None)
        if volume is None or volume == 0:
            # Default to iceberg for unknown volumes
            return "iceberg"
        
        return "hudi" if volume > self.threshold else "iceberg"
    
    def get_writer_instance(self, sink_type: str):
        """Return HudiWriter or IcebergWriter instance.
        
        Args:
            sink_type: Type of sink ("hudi" or "iceberg")
            
        Returns:
            Writer instance
        """
        # Lazy imports to avoid circular dependencies
        if sink_type == "hudi":
            try:
                from ..hudi_writer import HudiWriter
                return HudiWriter()
            except ImportError:
                raise ImportError("HudiWriter not available. Install hudi dependencies.")
        elif sink_type == "iceberg":
            try:
                from ..lake.iceberg_writer import IcebergWriter
                return IcebergWriter()
            except ImportError:
                raise ImportError("IcebergWriter not available. Install pyiceberg.")
        else:
            raise ValueError(f"Unknown sink type: {sink_type}")

