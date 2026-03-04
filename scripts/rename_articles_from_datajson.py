#!/usr/bin/env python3
"""
rename_articles_from_datajson.py
--------------------------------
Bulk rename .docx files using site/data.json as the source of truth.

Default behavior: DRY RUN (prints actions, writes mapping CSV, makes no changes)
Use --apply to actually rename.

New filename format:
  "{Clean Title} ({YYYY-MM-DD}).docx"

- Date comes from data.json "date" (or "dateISO"). If missing/invalid -> 0001-01-01
- Title comes from data.json "title", cleaned (removes PJ/Post-Journal/etc suffixes)
- Handles duplicates by appending " - 2", " - 3", etc.
- Optionally updates site/state.json file keys to match new filenames.

Usage examples:
  python3 scripts/rename_articles_from_datajson.py \
    --data site/data.json \
    --folder KIDDER_ARTICLE_ARCHIVES \
    --mapping rename_map.csv

  python3 scripts/rename_articles_from_datajson.py \
    --data site/data.json \
    --folder KIDDER_ARTICLE_ARCHIVES \
    --mapping rename_map.csv \
    --apply --update-state
"""

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from datetime import datetime


DUMMY_DATE = "0001-01-01"


def load_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise RuntimeError(f"Could not read JSON: {path} ({e})")


def valid_iso_date(s: str) -> bool:
    if not s or not isinstance(s, str):
        return False
    try:
        # Strict YYYY-MM-DD
        datetime.strptime(s.strip(), "%Y-%m-%d")
        return True
    except Exception:
        return False


def pick_date(article: dict) -> str:
    for k in ("date", "dateISO"):
        v = (article.get(k) or "").strip()
        if valid_iso_date(v):
            return v
    return DUMMY_DATE


def safe_filename(name: str) -> str:
    """
    macOS/Windows-safe filename sanitization.
    """
    name = name.strip()
    # Replace forbidden filesystem characters
    name = re.sub(r'[\/\\:\*\?"<>\|]+', "-", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    # Avoid trailing dots/spaces (Windows)
    name = name.rstrip(" .")
    return name


def clean_title(title: str) -> str:
    t = (title or "").strip()

    # Remove common suffix junk (case-insensitive)
    # Examples: "Some Title - PJ", "Some Title (Post Journal)", "Some Title — Local Commentaries"
    junk_patterns = [
        r"\bP\.?\s*J\.?\b",
        r"\bPost[-\s]?Journal\b",
        r"\bJamestown\s+Post[-\s]?Journal\b",
        r"\bLocal\s+Commentaries?\b",
        r"\bOpinion\s*&\s*Commentar(y|ies)\b",
        r"\bOpinion\s+and\s+Commentar(y|ies)\b",
    ]

    # Strip trailing segments that are just junk tokens
    # e.g., "Title — PJ" or "Title - Post Journal"
    t2 = t
    for pat in junk_patterns:
        t2 = re.sub(rf"(\s*[-—:|]\s*{pat}\s*)$", "", t2, flags=re.IGNORECASE).strip()
        t2 = re.sub(rf"(\s*\(\s*{pat}\s*\)\s*)$", "", t2, flags=re.IGNORECASE).strip()

    # Also remove standalone trailing "PJ" in parentheses or after dash
    t2 = re.sub(r"\s*[-—:|]\s*(PJ)\s*$", "", t2, flags=re.IGNORECASE).strip()
    t2 = re.sub(r"\(\s*PJ\s*\)\s*$", "", t2, flags=re.IGNORECASE).strip()

    # Collapse whitespace again
    t2 = re.sub(r"\s+", " ", t2).strip()

    # Fallback if title becomes empty
    return t2 if t2 else (t or "Untitled")


def make_target_name(title: str, date_iso: str) -> str:
    title = clean_title(title)
    title = safe_filename(title)

    # Guard: keep filenames manageable
    # Leave room for " (YYYY-MM-DD).docx"
    max_title_len = 180
    if len(title) > max_title_len:
        title = title[:max_title_len].rstrip()

    return f"{title} ({date_iso}).docx"


def uniquify(target: Path) -> Path:
    """
    If target exists, add " - 2", " - 3" etc before the date segment.
    """
    if not target.exists():
        return target

    stem = target.stem  # includes "(date)"
    suffix = target.suffix

    # Try to split "... (YYYY-MM-DD)" so we can insert counter before date.
    m = re.match(r"^(.*)\s\((\d{4}-\d{2}-\d{2})\)$", stem)
    base_title = stem
    date_part = None
    if m:
        base_title = m.group(1).strip()
        date_part = m.group(2).strip()

    n = 2
    while True:
        if date_part:
            cand = target.with_name(f"{base_title} - {n} ({date_part}){suffix}")
        else:
            cand = target.with_name(f"{base_title} - {n}{suffix}")
        if not cand.exists():
            return cand
        n += 1


def update_state_keys(state_path: Path, mapping: list[tuple[str, str]]) -> bool:
    """
    state.json format: { "files": { "<filename.docx>": "<hash>", ... } }
    We move hashes from old filename to new filename.
    """
    if not state_path.exists():
        print(f"[state] No state.json found at {state_path} (skipping).")
        return False

    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[state] Could not read {state_path}: {e}")
        return False

    files = state.get("files")
    if not isinstance(files, dict):
        print(f"[state] Unexpected format in {state_path}: missing 'files' dict.")
        return False

    moved = 0
    for old_name, new_name in mapping:
        if old_name in files and new_name not in files:
            files[new_name] = files[old_name]
            del files[old_name]
            moved += 1

    state["files"] = files
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[state] Updated {state_path}: moved {moved} keys.")
    return moved > 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="Path to site/data.json")
    ap.add_argument("--folder", required=True, help="Folder containing .docx files (KIDDER_ARTICLE_ARCHIVES)")
    ap.add_argument("--mapping", default="rename_map.csv", help="CSV output: old,new")
    ap.add_argument("--apply", action="store_true", help="Actually rename files (default is dry-run)")
    ap.add_argument("--update-state", action="store_true", help="Update site/state.json keys to match renamed filenames")
    ap.add_argument("--state", default="", help="Optional state.json path (default: <data parent>/state.json)")
    args = ap.parse_args()

    data_path = Path(args.data).expanduser().resolve()
    folder = Path(args.folder).expanduser().resolve()
    mapping_csv = Path(args.mapping).expanduser().resolve()
    state_path = Path(args.state).expanduser().resolve() if args.state else (data_path.parent / "state.json")

    if not data_path.exists():
        print(f"ERROR: data.json not found: {data_path}")
        sys.exit(2)
    if not folder.exists():
        print(f"ERROR: folder not found: {folder}")
        sys.exit(2)

    data = load_json(data_path)
    articles = data.get("articles", data if isinstance(data, list) else [])
    if not isinstance(articles, list):
        print("ERROR: data.json doesn't contain an 'articles' list.")
        sys.exit(2)

    # Build rename plan based on sourceFile
    # We only rename if the sourceFile exists in the folder and ends with .docx
    existing_files = {p.name: p for p in folder.glob("*.docx")}

    plan: list[tuple[Path, Path]] = []
    skipped_missing = 0
    skipped_nochange = 0

    for a in articles:
        source = (a.get("sourceFile") or "").strip()
        if not source.lower().endswith(".docx"):
            continue

        src_path = existing_files.get(source)
        if not src_path:
            skipped_missing += 1
            continue

        date_iso = pick_date(a)
        title = (a.get("title") or Path(source).stem).strip()
        target_name = make_target_name(title, date_iso)
        target_path = folder / target_name

        # If already correct, skip
        if src_path.name == target_path.name:
            skipped_nochange += 1
            continue

        # Avoid collisions
        target_path = uniquify(target_path)

        plan.append((src_path, target_path))

    if not plan:
        print("No renames needed.")
        print(f"Skipped missing-in-folder: {skipped_missing}")
        print(f"Skipped already-correct:  {skipped_nochange}")
        sys.exit(0)

    print(f"Planned renames: {len(plan)}")
    print(f"Skipped missing-in-folder: {skipped_missing}")
    print(f"Skipped already-correct:  {skipped_nochange}")
    print()

    # Show plan
    for src, dst in plan[:30]:
        print(f"- {src.name}  ->  {dst.name}")
    if len(plan) > 30:
        print(f"... ({len(plan)-30} more)")
    print()

    # Write mapping CSV
    mapping_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(mapping_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["oldName", "newName"])
        for src, dst in plan:
            w.writerow([src.name, dst.name])

    print(f"Wrote mapping CSV: {mapping_csv}")

    if not args.apply:
        print("\nDRY RUN only. Re-run with --apply to actually rename files.")
        sys.exit(0)

    # Apply renames
    applied = []
    for src, dst in plan:
        try:
            src.rename(dst)
            applied.append((src.name, dst.name))
        except Exception as e:
            print(f"ERROR renaming {src.name} -> {dst.name}: {e}")

    print(f"\nApplied renames: {len(applied)} / {len(plan)}")

    # Optionally update state.json keys
    if args.update_state and applied:
        update_state_keys(state_path, applied)

    print("\nNext step:")
    print("1) Run your processor to regenerate site/data.json so sourceFile fields match new filenames.")
    print("2) Commit + push changes to GitHub.")
    sys.exit(0)


if __name__ == "__main__":
    main()