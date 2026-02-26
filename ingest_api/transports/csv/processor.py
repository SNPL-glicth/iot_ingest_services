"""CSV processor for bulk data import.

Processes CSV files in chunks to avoid memory issues with large files.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterator, List

import pandas as pd

from ...core.domain.data_point import DataPoint

logger = logging.getLogger(__name__)


class CSVProcessor:
    """Processes CSV files for bulk data ingestion.
    
    Uses pandas with chunking to handle large files efficiently
    without loading everything into memory.
    """
    
    def __init__(self, chunk_size: int = 10000):
        """Initialize CSV processor.
        
        Args:
            chunk_size: Number of rows to process per chunk
        """
        self.chunk_size = chunk_size
    
    def process(
        self,
        file_path: str,
        source_id: str,
        domain: str,
        timestamp_col: str,
        value_cols: List[str],
        stream_id_prefix: str = "",
    ) -> Iterator[DataPoint]:
        """Process CSV file and yield DataPoint objects.
        
        Args:
            file_path: Path to CSV file
            source_id: Source identifier
            domain: Domain identifier
            timestamp_col: Name of timestamp column
            value_cols: List of column names containing values
            stream_id_prefix: Optional prefix for stream IDs
            
        Yields:
            DataPoint objects from CSV rows
        """
        logger.info(
            "[CSVProcessor] Processing file: %s (domain=%s, source=%s)",
            file_path, domain, source_id,
        )
        
        try:
            # Read CSV in chunks
            for chunk_idx, chunk in enumerate(pd.read_csv(file_path, chunksize=self.chunk_size)):
                logger.debug("[CSVProcessor] Processing chunk %d", chunk_idx)
                
                # Validate required columns
                if timestamp_col not in chunk.columns:
                    raise ValueError(f"Timestamp column '{timestamp_col}' not found in CSV")
                
                for value_col in value_cols:
                    if value_col not in chunk.columns:
                        raise ValueError(f"Value column '{value_col}' not found in CSV")
                
                # Process each row
                for idx, row in chunk.iterrows():
                    # Parse timestamp
                    try:
                        timestamp = pd.to_datetime(row[timestamp_col])
                        if pd.isna(timestamp):
                            logger.warning("[CSVProcessor] Skipping row %d: invalid timestamp", idx)
                            continue
                    except Exception as e:
                        logger.warning("[CSVProcessor] Skipping row %d: timestamp parse error: %s", idx, e)
                        continue
                    
                    # Create DataPoint for each value column
                    for value_col in value_cols:
                        try:
                            value = float(row[value_col])
                            if pd.isna(value):
                                continue
                            
                            stream_id = f"{stream_id_prefix}{value_col}" if stream_id_prefix else value_col
                            
                            yield DataPoint(
                                source_id=source_id,
                                stream_id=stream_id,
                                value=value,
                                timestamp=timestamp.to_pydatetime(),
                                domain=domain,
                                domain_metadata={
                                    "csv_row": int(idx),
                                    "csv_column": value_col,
                                },
                            )
                            
                        except Exception as e:
                            logger.warning(
                                "[CSVProcessor] Skipping row %d, col %s: %s",
                                idx, value_col, e,
                            )
                            continue
                
        except Exception as e:
            logger.exception("[CSVProcessor] Error processing CSV: %s", e)
            raise
