#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--map", default="rename_map.csv", help="CSV with columns oldName,newName")
    ap.add_argument("--folder", default="KIDDER_ARTICLE_ARCHIVES", help="Folder containing docx files")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    folder = Path(args.folder)
    mappath = Path(args.map)

    if not folder.exists():
        raise SystemExit(f"ERROR: folder not found: {folder}")
    if not mappath.exists():
        raise SystemExit(f"ERROR: map not found: {mappath}")

    rows = []
    with mappath.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        if "oldName" not in r.fieldnames or "newName" not in r.fieldnames:
            raise SystemExit("ERROR: rename_map.csv must have headers: oldName,newName")
        for row in r:
            oldn = (row.get("oldName") or "").strip()
            newn = (row.get("newName") or "").strip()
            if oldn and newn:
                rows.append((oldn, newn))

    if not rows:
        print("No rows found in rename map.")
        return

    # Preflight: show missing + collisions
    missing = []
    collisions = []
    for oldn, newn in rows:
        oldp = folder / oldn
        newp = folder / newn
        if not oldp.exists():
            missing.append(oldn)
        if newp.exists() and oldp.resolve() != newp.resolve():
            collisions.append(newn)

    if missing:
        print("\nMISSING (oldName not found in folder):")
        for x in missing:
            print("  -", x)

    if collisions:
        print("\nCOLLISIONS (newName already exists in folder):")
        for x in sorted(set(collisions)):
            print("  -", x)

    if missing or collisions:
        print("\nFix the map/folder and rerun. (No changes made.)")
        return

    # Apply renames
    print("\nApplying renames:")
    for oldn, newn in rows:
        oldp = folder / oldn
        newp = folder / newn
        print(f"  {oldn}  ->  {newn}")
        if not args.dry_run:
            oldp.rename(newp)

    print("\nDone.")
    if args.dry_run:
        print("(dry-run only, no files renamed)")

if __name__ == "__main__":
    main()