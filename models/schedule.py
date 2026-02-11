from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class Interval:
    start: str
    end: str

    def to_dict(self) -> Dict[str, str]:
        return {"from": self.start, "to": self.end}


@dataclass(frozen=True)
class Schedule:
    date: str
    city: str
    region: str
    source: str
    queues: Dict[str, List[Interval]]

    def to_dict(self) -> Dict[str, object]:
        return {
            "date": self.date,
            "city": self.city,
            "region": self.region,
            "source": self.source,
            "queues": {
                queue: [interval.to_dict() for interval in intervals]
                for queue, intervals in self.queues.items()
            },
        }
