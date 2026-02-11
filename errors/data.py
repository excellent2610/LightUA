class ScheduleNotFoundError(Exception):
    """Schedule data not found after parsing."""


class InvalidTimeFormatError(Exception):
    """Time value has invalid format."""
