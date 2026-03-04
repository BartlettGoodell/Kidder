#!/usr/bin/env python3
import csv, glob, os, re, shutil, sys, unicodedata
from pathlib import Path

UNKNOWN = "0001-01-01"
RX_DATE = re.compile(r"\s*\((\d{4}-\d{2}-\d{2})\)\.docx$", re.I)

def fix_mojibake(s: str) -> str:
    return (s.replace("â€™", "’").replace("â€˜", "‘")
             .replace("â€”", "—").replace("â€“", "–")
             .replace("â€œ", "“").replace("â€�", "”")
             .replace("Â", ""))

def norm_title(s: str) -> str:
    s = str(s).strip()
    s = fix_mojibake(s)
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("–", "-").replace("—", "-")
    s = s.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[?]+$", "-", s)
    s = re.sub(r"\s*-\s*$", "-", s)
    return s.lower()

def base_title(filename: str) -> str:
    # removes (YYYY-MM-DD).docx suffix
    return RX_DATE.sub("", filename).strip()

def main():
    archive = Path("KIDDER_ARTICLE_ARCHIVES")
    map_path = Path("date_map.normalized.csv")
    if not archive.exists():
        print("ERROR: Missing KIDDER_ARTICLE_ARCHIVES")
        sys.exit(1)
    if not map_path.exists():
        print("ERROR: Missing date_map.normalized.csv")
        sys.exit(1)

    # Load map: title_norm -> date
    date_map = {}
    with open(map_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            tn = (row.get("title_norm") or "").strip()
            d  = (row.get("date") or "").strip()
            if tn and d:
                date_map[tn] = d

    files = [Path(p) for p in glob.glob(str(archive / "*.docx"))]
    unknown_files = [p for p in files if f"({UNKNOWN}).docx" in p.name]
    print("Unknown files:", len(unknown_files))

    plan = []
    collisions = []

    for p in unknown_files:
        t = base_title(p.name)
        tn = norm_title(t)
        d = date_map.get(tn, "")
        if not d:
            print("NO DATE MAP FOR:", p.name)
            continue
        target = archive / f"{t} ({d}).docx"
        if target.exists():
            collisions.append((p, target))
        else:
            plan.append((p, target))

    print("Planned renames (non-colliding):", len(plan))
    print("Collisions:", len(collisions))
    for a,b in collisions:
        print("  -", a.name, "->", b.name)

    # Execute renames
    for src, dst in plan:
        src.rename(dst)

    # For collisions: delete the UNKNOWN version (keep the dated one)
    for src, dst in collisions:
        # remove src (unknown) because dst already exists with real date
        src.unlink()

    print("\nDone.")
    print("Renamed:", len(plan))
    print("Removed unknown duplicates:", len(collisions))

if __name__ == "__main__":
    main()