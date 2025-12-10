from enum import Enum


class MigrationStatus(str, Enum):
    """Migration status enumeration."""

    PENDING = "pending"
    EXTRACTING = "extracting"
    EXTRACTED = "extracted"
    INSERTING = "inserting"
    COMPLETED = "completed"
    FAILED = "failed"
