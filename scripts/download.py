"""Download validated DBLP data, with official DROPS snapshots as a fallback."""

import argparse
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
from http.client import HTTPException
import json
import os
from pathlib import Path
import re
import tempfile
import time
from urllib.error import URLError
from urllib.parse import urljoin, urlparse
from urllib.request import urlopen

from lxml import etree, html

DAILY_BASE = "https://dblp.org/xml/"
DROPS = "https://drops.dagstuhl.de"
COLLECTION = DROPS + "/entities/collection/10.4230/dblp.xml"
MANIFEST = "dblp-download.json"


class DownloadError(Exception):
    pass


class BlockedDownload(DownloadError):
    pass


def parse_md5(content):
    match = re.fullmatch(r"([0-9a-fA-F]{32})(?:[ \t]+\*?[^\r\n]+)?\s*", content.strip())
    if not match:
        raise DownloadError("Invalid MD5 document (expected a checksum, not HTML)")
    return match[1].lower()


def fetch(url, destination, kind, attempts=3):
    """Stream to a temporary file and publish only validated responses."""
    destination = Path(destination)
    partial = destination.with_suffix(destination.suffix + ".part")
    limit = {"md5": 4096, "dtd": 1024 * 1024, "page": 2 * 1024 * 1024}.get(kind)
    for attempt in range(attempts):
        try:
            with urlopen(url, timeout=60) as response, partial.open("wb") as output:
                if response.status != 200:
                    raise DownloadError(f"Unexpected HTTP {response.status}: {url}")
                first = response.read(4096)
                lowered = first.lower()
                if b"anubis_challenge" in lowered or b"making sure you" in lowered:
                    raise BlockedDownload(f"Bot verification page returned by {url}")
                if kind != "page" and (
                    "html" in response.headers.get("Content-Type", "").lower()
                    or b"<!doctype html" in lowered or b"<html" in lowered
                ):
                    raise BlockedDownload(f"HTML returned instead of {kind}: {url}")
                if kind == "gzip" and not first.startswith(b"\x1f\x8b\x08"):
                    raise DownloadError(f"Invalid gzip header: {url}")
                size = 0
                chunk = first
                while chunk:
                    size += len(chunk)
                    if limit is not None and size > limit:
                        raise DownloadError(f"Oversized {kind} response: {url}")
                    output.write(chunk)
                    chunk = response.read(1024 * 1024)
                expected_size = response.headers.get("Content-Length")
                if expected_size is not None and size != int(expected_size):
                    raise DownloadError(f"Incomplete download: {url}")
                modified = response.headers.get("Last-Modified")
            if kind == "md5":
                parse_md5(partial.read_text(encoding="ascii"))
            elif kind == "dtd":
                dtd = etree.DTD(file=str(partial))
                if not any(element.name == "dblp" for element in dtd.iterelements()):
                    raise DownloadError(f"Not a DBLP DTD: {url}")
            partial.replace(destination)
            return modified
        except BlockedDownload:
            raise  # Retrying a successful HTTP response cannot clear a bot challenge.
        except (DownloadError, OSError, URLError, HTTPException, ValueError, etree.LxmlError) as error:
            if attempt + 1 == attempts:
                raise DownloadError(f"Download failed after {attempts} attempts: {url}: {error}") from error
            time.sleep(10 * (attempt + 1))
        finally:
            partial.unlink(missing_ok=True)


def load_json(path):
    return json.loads(path.read_text()) if path.exists() else {}


def links(page, base):
    return [urljoin(base, value) for value in html.parse(str(page)).xpath("//a/@href")]


def one_link(urls, suffix):
    candidates = {
        url for url in urls
        if urlparse(url).scheme == "https"
        and urlparse(url).netloc == "drops.dagstuhl.de"
        and urlparse(url).path.startswith("/storage/artifacts/dblp/xml/")
        and urlparse(url).path.endswith(suffix)
    }
    if len(candidates) != 1:
        raise DownloadError(f"Expected exactly one official DROPS {suffix} link")
    return candidates.pop()


def snapshot_metadata(work):
    fetch(COLLECTION, work / "collection.html", "page")
    releases = []
    for url in links(work / "collection.html", COLLECTION):
        match = re.fullmatch(re.escape(DROPS) + r"/entities/artifact/10\.4230/dblp\.xml\.(\d{4}-\d{2}-\d{2})", url)
        if match:
            release_date = date.fromisoformat(match[1])
            if release_date <= datetime.now(timezone.utc).date():
                releases.append((match[1], url))
    if not releases:
        raise DownloadError("No dated XML snapshots found in the official DROPS collection")
    release_date, page_url = max(releases)
    fetch(page_url, work / "snapshot.html", "page")
    urls = links(work / "snapshot.html", page_url)
    archive_url = one_link(urls, ".xml.gz")
    checksum_url = one_link(urls, ".xml.gz.md5")
    if checksum_url != archive_url + ".md5":
        raise DownloadError("Snapshot archive and checksum links do not match")
    dtd_dates = set()
    for url in urls:
        match = re.fullmatch(r"https://doi\.org/10\.4230/dblp\.xml\.dtd\.(\d{4}-\d{2}-\d{2})", url)
        if match:
            dtd_dates.add(match[1])
    if len(dtd_dates) != 1:
        raise DownloadError("Snapshot does not identify exactly one matching DTD")
    dtd_page = DROPS + "/entities/artifact/10.4230/dblp.xml.dtd." + dtd_dates.pop()
    fetch(dtd_page, work / "dtd.html", "page")
    dtd_url = one_link(links(work / "dtd.html", dtd_page), ".dtd")
    return {
        "source_date": release_date, "source_url": archive_url,
        "checksum_url": checksum_url, "dtd_url": dtd_url,
    }


def verified_archive(source, work, expected_md5):
    fetch(source["dtd_url"], work / "dblp.dtd", "dtd")
    # A daily publication may change between checksum and archive requests.
    # Never import a mismatched pair; the next refresh can retry a new snapshot.
    modified = fetch(source["source_url"], work / "dblp.xml.gz", "gzip")
    with (work / "dblp.xml.gz").open("rb") as archive:
        actual_md5 = hashlib.file_digest(archive, "md5").hexdigest()
    if actual_md5 != expected_md5:
        raise DownloadError(f"Archive MD5 mismatch: expected {expected_md5}, got {actual_md5}")
    (work / "dblp.xml.gz.md5").write_text(expected_md5 + "  dblp.xml.gz\n")
    return modified


def refresh(output_dir, previous):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    result = {"checked_at": datetime.now(timezone.utc).isoformat(), "changed": False, "mode": "daily"}
    with tempfile.TemporaryDirectory(prefix="dblp-", dir=output_dir) as temporary:
        work = Path(temporary)
        source = {"source_url": DAILY_BASE + "dblp.xml.gz", "dtd_url": DAILY_BASE + "dblp.dtd"}
        try:
            fetch(DAILY_BASE + "dblp.xml.gz.md5", work / "checksum", "md5")
            digest = parse_md5((work / "checksum").read_text())
            if digest == previous.get("source_md5"):
                return result
            modified = verified_archive(source, work, digest)
            source["source_date"] = result["checked_at"][:10]
            if modified:
                source["source_date"] = parsedate_to_datetime(modified).astimezone(timezone.utc).date().isoformat()
            cutoff = previous.get("source_date", previous.get("updated_at", ""))[:10]
            if source["source_date"] < cutoff:
                raise DownloadError("Daily archive is older than the existing database")
        except DownloadError as error:
            result["mode"] = "monthly-fallback"
            result["warning"] = f"Daily source unavailable; using official monthly snapshots. {error}"
            source = snapshot_metadata(work)
            result["latest_available_snapshot"] = source["source_date"]
            fetch(source["checksum_url"], work / "checksum", "md5")
            digest = parse_md5((work / "checksum").read_text())
            # Existing installations only recorded import time. Use its day as a
            # conservative cutoff until source_date is available; same-day
            # snapshots with a different checksum must not replace live data.
            cutoff = previous.get("source_date", previous.get("updated_at", ""))[:10]
            if digest == previous.get("source_md5") or source["source_date"] <= cutoff:
                result["warning"] += " No newer snapshot to import; keeping existing data."
                return result
            verified_archive(source, work, digest)
        result.update({
            "changed": True, "source_md5": digest,
            "source_url": source["source_url"], "source_date": source["source_date"],
        })
        for name in ("dblp.dtd", "dblp.xml.gz", "dblp.xml.gz.md5"):
            (work / name).replace(output_dir / name)
    return result


def record_status(path, result, entry_count=None):
    status = load_json(path)
    if result["changed"]:
        if entry_count is None or entry_count <= 0:
            raise ValueError("A successful import requires a positive database row count")
        status.update({key: result[key] for key in ("source_md5", "source_url", "source_date")})
        status.update(updated_at=datetime.now(timezone.utc).isoformat(), entry_count=entry_count)
    status.update(last_checked_at=result["checked_at"], last_check_mode=result["mode"])
    for key in ("warning", "latest_available_snapshot"):
        status.pop(key, None)
        if key in result:
            status[key] = result[key]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(status, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("."))
    parser.add_argument("--status-file", type=Path, default=Path("status/dblp-update.json"))
    parser.add_argument("--record-status", action="store_true")
    parser.add_argument("--entry-count", type=int)
    args = parser.parse_args()
    manifest = args.output_dir / MANIFEST
    if args.record_status:
        record_status(args.status_file, load_json(manifest), args.entry_count)
        return
    manifest.unlink(missing_ok=True)
    result = refresh(args.output_dir, load_json(args.status_file))
    manifest.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    if "warning" in result:
        print("::warning::" + result["warning"].replace("\n", " ").replace("\r", " "))
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write(f"changed={str(result['changed']).lower()}\n")


if __name__ == "__main__":
    main()
