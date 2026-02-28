#!/usr/bin/env python3
import argparse
import email
import imaplib
import os
import re
import sys
from email.header import decode_header
from pathlib import Path


def decode_mime_words(s: str) -> str:
    parts = decode_header(s)
    out = ""
    for text, enc in parts:
        if isinstance(text, bytes):
            out += text.decode(enc or "utf-8", errors="ignore")
        else:
            out += text
    return out


def safe_filename(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[\/\\:\*\?\"<>\|]+", "-", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:180] if len(name) > 180 else name


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--folder", default="KIDDER_ARTICLE_ARCHIVES")
    ap.add_argument("--imap-host", default="imap.gmail.com")
    ap.add_argument("--mailbox", default="INBOX")
    ap.add_argument("--search", default="UNSEEN")  # could be 'UNSEEN SUBJECT "Article"' etc.
    args = ap.parse_args()

    user = os.getenv("KIDDER_GMAIL_USER", "")
    app_pass = os.getenv("KIDDER_GMAIL_APP_PASSWORD", "")

    if not user or not app_pass:
        print("ERROR: Missing env vars KIDDER_GMAIL_USER and/or KIDDER_GMAIL_APP_PASSWORD")
        sys.exit(2)

    out_dir = Path(args.folder)
    out_dir.mkdir(parents=True, exist_ok=True)

    M = imaplib.IMAP4_SSL(args.imap_host)
    try:
        M.login(user, app_pass)
        M.select(args.mailbox)

        typ, data = M.search(None, args.search)
        if typ != "OK":
            print("No new emails.")
            sys.exit(0)

        ids = data[0].split()
        if not ids:
            print("No new emails.")
            sys.exit(0)

        saved_any = False

        for msg_id in ids:
            typ, msg_data = M.fetch(msg_id, "(RFC822)")
            if typ != "OK":
                continue

            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)

            subj = decode_mime_words(msg.get("Subject", "") or "")
            sender = decode_mime_words(msg.get("From", "") or "")

            # Walk attachments
            saved_this_msg = False
            for part in msg.walk():
                cdisp = part.get("Content-Disposition", "") or ""
                if "attachment" not in cdisp.lower():
                    continue

                filename = part.get_filename()
                if not filename:
                    continue
                filename = decode_mime_words(filename)

                if not filename.lower().endswith(".docx"):
                    continue

                payload = part.get_payload(decode=True)
                if not payload:
                    continue

                clean = safe_filename(Path(filename).name)
                target = out_dir / clean

                # avoid overwriting by accident
                if target.exists():
                    stem = target.stem
                    suffix = target.suffix
                    n = 2
                    while True:
                        alt = out_dir / f"{stem} ({n}){suffix}"
                        if not alt.exists():
                            target = alt
                            break
                        n += 1

                target.write_bytes(payload)
                print(f"Email: {subj}")
                print(f"From:  {sender}")
                print(f"  Saved: {target}")
                saved_any = True
                saved_this_msg = True

            # Mark as seen if we saved something from it
            if saved_this_msg:
                M.store(msg_id, "+FLAGS", "\\Seen")

        sys.exit(10 if saved_any else 0)

    finally:
        try:
            M.logout()
        except Exception:
            pass


if __name__ == "__main__":
    main()