from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from parsing.models import SourceScheduleObservation, SourceTarget


class SourceAdapter(ABC):
    source_id: str

    @abstractmethod
    def parse(
        self,
        html: str,
        target: SourceTarget,
        observed_at: datetime,
    ) -> SourceScheduleObservation:
        """Parse one public HTML page without performing network access."""
