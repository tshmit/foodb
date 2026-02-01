from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO


class Logger:
    def __init__(self, *, fmt: str = "text", log_file: Path | None = None) -> None:
        if fmt not in {"text", "jsonl"}:
            raise ValueError("fmt must be 'text' or 'jsonl'")
        self._fmt = fmt
        self._fh: TextIO | None = None
        if log_file is not None:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            self._fh = log_file.open("a", encoding="utf-8", errors="strict")

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def event(self, name: str, **fields: object) -> None:
        payload = {
            "ts": datetime.now(UTC).isoformat(timespec="seconds"),
            "event": name,
            **fields,
        }
        if self._fmt == "jsonl":
            line = json.dumps(payload, ensure_ascii=False)
        else:
            kv = " ".join(f"{k}={payload[k]}" for k in payload if k not in {"ts", "event"})
            line = f"[{payload['ts']}] {name}" + (f" {kv}" if kv else "")

        print(line, flush=True)
        if self._fh is not None:
            self._fh.write(line + "\n")
            self._fh.flush()
