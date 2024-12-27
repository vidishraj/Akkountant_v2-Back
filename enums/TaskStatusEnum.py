from enum import Enum


class JobStatus(Enum):
    PENDING = "Pending"
    COMPLETED = "Completed"
    OVERDUE = "Overdue"
    FAILED = "Failed"
