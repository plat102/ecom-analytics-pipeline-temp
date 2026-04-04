"""
Configuration for MongoDB events export
"""

import os

from config import settings

# GCS Configuration (inherit from base)
GCS_BUCKET = settings.GCS_BUCKET
GCS_DESTINATION_PREFIX = "raw/events"

# Export Configuration (module-specific)
BATCH_SIZE = int(os.getenv("EVENTS_BATCH_SIZE", "100000"))
MONGODB_BATCH_SIZE = int(os.getenv("EVENTS_MONGODB_BATCH_SIZE", "1000"))

# Checkpoint Configuration (inherit paths from base)
CHECKPOINT_DIR = settings.DATA_DIR / "checkpoints"
CHECKPOINT_FILE = CHECKPOINT_DIR / "events_export_checkpoint.json"

# Data Directory (inherit from base)
DATA_DIR = settings.DATA_EXPORTS_DIR
