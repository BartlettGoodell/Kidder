#!/usr/bin/env python3
"""
process_articles.py — Kidder Article Dashboard Processor (Incremental + Prune)
==============================================================================
- Scans a folder of .docx files
- Extracts text (pandoc preferred; python-docx fallback)
- Uses OpenAI API to infer: title, date, summary, topics
- Writes site/data.json for your static site
- Writes missing_dates.csv
- Maintains site/state.json for incremental processing
- PRUNES entries whose sourceFile no longer exists in the archive folder
- Skips empty/zero-word extracts so you don’t end up with zombie articles
"""

import argparse
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    from docx import Document
except ImportError:
    Document = None

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def run_pandoc_extract(docx_path: Path) -> str:
    try:
        res = subprocess.run(
            ["pandoc", str(docx_path), "-t", "plain"],
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode == 0 and res.stdout.strip():
            return res.stdout
    except FileNotFoundError:
        pass
    return ""


def docx_extract_fallback(docx_path: Path) -> str:
    if Document is None:
        return ""
    try:
        doc = Document(str(docx_path))
        paras = [p.text for p in doc.paragraphs if p.text and p.text.strip()]
        return "\n".join(paras).strip()
    except Exception:
        return ""


def normalize_whitespace(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def word_count(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))


def parse_date_from_filename(name: str):
    base = Path(name).stem

    m = re.search(r"(20\d{2})[-_](\d{1,2})[-_](\d{1,2})", base)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(y, mo, d).strftime("%Y-%m-%d")
        except ValueError:
            pass

    m = re.search(r"(\d{1,2})[-_](\d{1,2})[-_](20\d{2})", base)
    if m:
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(y, mo, d).strftime("%Y-%m-%d")
        except ValueError:
            pass

    return None


def parse_date_from_text(text: str):
    head = "\n".join(text.splitlines()[:20])

    m = re.search(
        r"\b(Jan|January|Feb|February|Mar|March|Apr|April|May|Jun|June|Jul|July|Aug|August|Sep|Sept|September|Oct|October|Nov|November|Dec|December)\.?\s+(\d{1,2})(?:st|nd|rd|th)?,\s+(20\d{2})\b",
        head,
        flags=re.IGNORECASE,
    )
    if m:
        mon = m.group(1).lower()[:3]
        day = int(m.group(2))
        year = int(m.group(3))
        month_map = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
        mo = month_map.get(mon)
        if mo:
            try:
                return datetime(year, mo, day).strftime("%Y-%m-%d")
            except ValueError:
                pass

    return None


def openai_infer(client: OpenAI, model: str, text: str):
    prompt = (
        "You are extracting metadata for a newspaper opinion archive.\n"
        "Given the article text, return STRICT JSON with keys:\n"
        "title (string), date (YYYY-MM-DD or empty string), summary (string), topics (array of short strings).\n"
        "If date is not explicitly stated, infer conservatively from context; otherwise return empty string.\n"
    )

    article_snip = text[:12000]

    r = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "Return only valid JSON. No markdown."},
            {"role": "user", "content": prompt + "\n\nARTICLE:\n" + article_snip},
        ],
        temperature=0.2,
    )
    content = r.choices[0].message.content.strip()
    return json.loads(content)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--folder", type=str, default="KIDDER_ARTICLE_ARCHIVES")
    ap.add_argument("--output", type=str, default="site/data.json")
    ap.add_argument("--missing-csv", type=str, default="missing_dates.csv")
    ap.add_argument("--state", type=str, default="", help="Optional state.json path. Default: next to output as state.json")
    ap.add_argument("--api-key", type=str, default="", help="Or set OPENAI_API_KEY env var")
    ap.add_argument("--model", type=str, default="gpt-4o-mini")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--prune", action="store_true", help="Remove JSON entries whose sourceFile no longer exists")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    folder = Path(args.folder).expanduser()
    out_json = Path(args.output).expanduser()
    missing_csv = Path(args.missing_csv).expanduser()

    if not folder.exists():
        print(f"ERROR: Folder not found: {folder}")
        sys.exit(1)

    out_json.parent.mkdir(parents=True, exist_ok=True)

    state_path = Path(args.state).expanduser() if args.state else (out_json.parent / "state.json")
    state = load_json(state_path, default={"files": {}})

    existing = {"articles": [], "generatedAt": "", "errors": []}
    if args.resume and out_json.exists():
        existing = load_json(out_json, default=existing)
        if "articles" not in existing:
            existing = {"articles": existing if isinstance(existing, list) else [], "generatedAt": "", "errors": []}

    articles = existing.get("articles", [])

    docx_files = sorted(folder.glob("*.docx"))
    print(f"Found {len(docx_files)} .docx files in {folder}")

    if args.prune:
        existing_names = {p.name for p in docx_files}
        pruned = 0
        kept = []
        for a in articles:
            sf = a.get("sourceFile")
            if sf and sf not in existing_names:
                pruned += 1
                continue
            kept.append(a)
        articles = kept
        existing["articles"] = articles
        if pruned and args.verbose:
            print(f"Pruned {pruned} articles not found in folder.")

    api_key = args.api_key or os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        print("ERROR: No API key. Set OPENAI_API_KEY env var or pass --api-key.")
        sys.exit(1)
    if OpenAI is None:
        print("ERROR: openai package not installed. Run: pip install openai")
        sys.exit(1)

    client = OpenAI(api_key=api_key)

    errors = []
    processed = 0
    updated = 0
    missing_date_rows = []

    for i, p in enumerate(docx_files, start=1):
        file_hash = sha256_file(p)
        prev_hash = state.get("files", {}).get(p.name, "")
        already = (prev_hash == file_hash)

        if args.resume and already:
            if args.verbose:
                print(f"[{i}/{len(docx_files)}] Skip unchanged: {p.name}")
            continue

        if args.verbose:
            print(f"[{i}/{len(docx_files)}] Processing: {p.name}")

        text = run_pandoc_extract(p) or docx_extract_fallback(p)
        text = normalize_whitespace(text)
        wc = word_count(text)

        if wc == 0:
            if args.verbose:
                print(f"  SKIP (0 words): {p.name}")
            state.setdefault("files", {})[p.name] = file_hash
            continue

        date_guess = parse_date_from_filename(p.name) or parse_date_from_text(text) or ""

        try:
            meta = openai_infer(client, args.model, text)
            title = (meta.get("title") or "").strip() or p.stem
            date_ai = (meta.get("date") or "").strip()
            summary = (meta.get("summary") or "").strip()
            topics = meta.get("topics") or []
            if not isinstance(topics, list):
                topics = []

            final_date = date_guess or date_ai or ""
            date_source = "filename/text" if date_guess else ("ai" if date_ai else "")

            article = {
                "sourceFile": p.name,
                "title": title,
                "date": final_date,
                "dateISO": final_date,
                "dateSource": date_source,
                "summary": summary,
                "topics": [t.strip() for t in topics if isinstance(t, str) and t.strip()],
                "wordCount": wc,
                "text": text,
                "updatedAt": datetime.now().isoformat(timespec="seconds"),
            }

            existing_entry = None
            for idx_a, a in enumerate(articles):
                if a.get("sourceFile") == p.name:
                    existing_entry = idx_a
                    break

            if existing_entry is None:
                articles.append(article)
                updated += 1
            else:
                articles[existing_entry] = article
                updated += 1

            processed += 1

            if not final_date:
                missing_date_rows.append([p.name, title, wc])

            if args.verbose:
                d_disp = final_date if final_date else "NO DATE"
                print(f"  OK  {title}  [{d_disp}]  ({date_source or '-'} / wc={wc})")

        except Exception as e:
            err = {"file": p.name, "error": str(e)}
            errors.append(err)
            if args.verbose:
                print(f"  ERROR on {p.name}: {e}")

        state.setdefault("files", {})[p.name] = file_hash
        time.sleep(0.1)

    dedup = {}
    for a in articles:
        sf = a.get("sourceFile")
        if sf:
            dedup[sf] = a

    articles = sorted(
        dedup.values(),
        key=lambda x: (x.get("date") or "", x.get("title") or ""),
        reverse=True,
    )

    payload = {"generatedAt": datetime.now().isoformat(timespec="seconds"), "articles": articles, "errors": errors}
    save_json(out_json, payload)
    save_json(state_path, state)

    missing_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(missing_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["sourceFile", "title", "wordCount"])
        for row in missing_date_rows:
            w.writerow(row)

    print("\n" + "=" * 50)
    print(f"Done. processed={processed}, updated={updated}")
    print(f"Total articles in JSON: {len(articles)}")
    print(f"Missing dates: {len(missing_date_rows)} (see {missing_csv})")
    print(f"Errors: {len(errors)}")
    print(f"Output: {out_json}")
    print(f"State:  {state_path}")

    sys.exit(10 if updated > 0 else 0)


if __name__ == "__main__":
    main()
    