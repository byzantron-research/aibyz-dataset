from __future__ import annotations
from typing import List

def load_validators_from_args(arg_str: str | None, file_path: str | None) -> List[int]:
    vals: List[int] = []
    if arg_str:
        for tok in arg_str.split(","):
            tok = tok.strip()
            if not tok:
                continue
            try:
                vals.append(int(tok))
            except ValueError:
                pass
    if file_path:
        from pathlib import Path
        p = Path(file_path)
        if p.exists():
            for line in p.read_text().splitlines():
                s = line.strip()
                if not s:
                    continue
                try:
                    vals.append(int(s))
                except ValueError:
                    pass
    # de-duplicate preserving order
    seen, uniq = set(), []
    for v in vals:
        if v not in seen:
            uniq.append(v); seen.add(v)
    return uniq
