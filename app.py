from __future__ import annotations

import json

from core.parser import get_schedule


def main() -> None:
    result = get_schedule()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
