import gzip
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from scripts import download


class Response(io.BytesIO):
    def __init__(self, body, content_type="application/octet-stream", length=None):
        super().__init__(body)
        self.status = 200
        self.headers = {
            "Content-Type": content_type,
            "Content-Length": str(len(body) if length is None else length),
            "Last-Modified": "Mon, 02 Sep 2024 00:00:00 GMT",
        }


class DownloadTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.work = Path(self.temporary.name)
        self.routes = {}
        self.requests = []
        self.mock_open = patch.object(download, "urlopen", side_effect=self.open_url).start()
        self.addCleanup(patch.stopall)
        self.sleep = patch.object(download.time, "sleep").start()
        self.dtd = b'<!ELEMENT dblp (article*)>\n<!ELEMENT article (title)>\n<!ATTLIST article key CDATA #REQUIRED>\n<!ELEMENT title (#PCDATA)>'
        self.archive = gzip.compress(b'<?xml version="1.0"?><!DOCTYPE dblp SYSTEM "dblp.dtd"><dblp><article key="test/1"><title>A title.</title></article></dblp>')
        self.digest = hashlib.md5(self.archive).hexdigest()

    def open_url(self, url, timeout):
        self.requests.append(url)
        value = self.routes[url]
        if isinstance(value, list):
            value = value.pop(0)
        if isinstance(value, Exception):
            raise value
        return Response(*value) if isinstance(value, tuple) else Response(value)

    def daily(self):
        self.routes.update({
            download.DAILY_BASE + "dblp.xml.gz.md5": self.digest.encode(),
            download.DAILY_BASE + "dblp.xml.gz": self.archive,
            download.DAILY_BASE + "dblp.dtd": self.dtd,
        })

    def monthly(self):
        page = download.DROPS + "/entities/artifact/10.4230/dblp.xml.2024-09-01"
        dtd_page = download.DROPS + "/entities/artifact/10.4230/dblp.xml.dtd.2023-06-28"
        archive_url = download.DROPS + "/storage/artifacts/dblp/xml/2024/dblp-2024-09-01.xml.gz"
        dtd_url = download.DROPS + "/storage/artifacts/dblp/xml/2023/dblp-2023-06-28.dtd"
        self.routes.update({
            download.COLLECTION: f'<a href="{page}">September</a><a href="{download.DROPS}/entities/artifact/10.4230/dblp.xml.2024-08-01">August</a>'.encode(),
            page: f'<a href="{archive_url}">XML</a><a href="{archive_url}.md5">MD5</a><a href="https://doi.org/10.4230/dblp.xml.dtd.2023-06-28">DTD</a>'.encode(),
            dtd_page: f'<a href="{dtd_url}">DTD</a>'.encode(),
            archive_url: self.archive,
            archive_url + ".md5": (self.digest + "  dblp-2024-09-01.xml.gz\n").encode(),
            dtd_url: self.dtd,
        })
        return archive_url

    def blocked_daily(self):
        self.routes[download.DAILY_BASE + "dblp.xml.gz.md5"] = (
            b'<!doctype html><title>Making sure you are not a bot!</title>', "text/html"
        )

    def test_http_200_html_is_rejected_without_retries_or_overwriting_files(self):
        for kind in ("md5", "gzip", "dtd"):
            with self.subTest(kind=kind):
                target = self.work / kind
                target.write_bytes(b"existing data")
                self.routes["https://example.test/file"] = (b"<!doctype html><html>Challenge</html>", "text/html")
                with self.assertRaises(download.BlockedDownload):
                    download.fetch("https://example.test/file", target, kind)
                self.assertEqual(target.read_bytes(), b"existing data")
                self.assertFalse(target.with_suffix(".part").exists())
        self.assertEqual(len(self.requests), 3)
        self.sleep.assert_not_called()

    def test_md5_formats_and_malformed_documents(self):
        for content in (self.digest, self.digest.upper() + " *file.gz\n", self.digest + "  file.gz\n"):
            self.assertEqual(download.parse_md5(content), self.digest)
        for content in ("<!doctype html>", "", "a" * 31, "x" * 32, self.digest + "\n" + self.digest):
            with self.assertRaises(download.DownloadError):
                download.parse_md5(content)

    def test_transient_503_is_retried(self):
        url = "https://example.test/checksum"
        self.routes[url] = [HTTPError(url, 503, "unavailable", {}, None), self.digest.encode()]
        download.fetch(url, self.work / "checksum", "md5")
        self.assertEqual(self.requests, [url, url])
        self.assertEqual((self.work / "checksum").read_text(), self.digest)

    def test_truncated_transfer_retries_from_scratch(self):
        url = "https://example.test/checksum"
        self.routes[url] = [(b"partial", "text/plain", 32), self.digest.encode()]
        download.fetch(url, self.work / "checksum", "md5")
        self.assertEqual((self.work / "checksum").read_text(), self.digest)
        self.assertFalse((self.work / "checksum.part").exists())

    def test_daily_download_validates_archive_and_records_source_date(self):
        self.daily()
        result = download.refresh(self.work, {})
        self.assertTrue(result["changed"])
        self.assertEqual(result["mode"], "daily")
        self.assertEqual(result["source_date"], "2024-09-02")
        self.assertEqual(gzip.decompress((self.work / "dblp.xml.gz").read_bytes()), gzip.decompress(self.archive))
        self.assertNotIn(download.COLLECTION, self.requests)

    def test_blocked_daily_falls_back_to_verified_official_snapshot(self):
        self.blocked_daily()
        url = self.monthly()
        result = download.refresh(self.work, {"updated_at": "2024-08-31T12:00:00Z"})
        self.assertTrue(result["changed"])
        self.assertEqual(result["mode"], "monthly-fallback")
        self.assertEqual(result["source_url"], url)
        self.assertEqual((self.work / "dblp.xml.gz.md5").read_text(), self.digest + "  dblp.xml.gz\n")
        self.assertEqual((self.work / "dblp.dtd").read_bytes(), self.dtd)
        # The renamed snapshot and its DTD must also work with the real converter.
        converter = Path(__file__).resolve().parents[1] / "scripts" / "convert.py"
        subprocess.run([sys.executable, str(converter)], cwd=self.work,
                       check=True, capture_output=True, timeout=10)
        self.assertIn("('test/1', 'atitle', 'article')", (self.work / "dblp.sql").read_text())

    def test_old_and_same_day_snapshots_preserve_import_status(self):
        for cutoff in ("2024-09-01T10:48:40Z", "2024-09-14T12:00:00Z"):
            with self.subTest(cutoff=cutoff):
                self.blocked_daily()
                url = self.monthly()
                self.requests.clear()
                previous = {"updated_at": cutoff, "source_md5": "a" * 32, "entry_count": 12926684}
                result = download.refresh(self.work, previous)
                self.assertFalse(result["changed"])
                self.assertNotIn(url, self.requests)
                status_file = self.work / "status.json"
                status_file.write_text(json.dumps(previous))
                download.record_status(status_file, result)
                status = json.loads(status_file.read_text())
                for key, value in previous.items():
                    self.assertEqual(status[key], value)
                self.assertEqual(status["last_check_mode"], "monthly-fallback")
                self.assertIn("No newer snapshot", status["warning"])

    def test_unchanged_daily_does_not_download_or_import_archive(self):
        self.daily()
        result = download.refresh(self.work, {"source_md5": self.digest})
        self.assertFalse(result["changed"])
        self.assertEqual(self.requests, [download.DAILY_BASE + "dblp.xml.gz.md5"])

    def test_checksum_mismatch_on_both_sources_does_not_publish_files(self):
        self.daily()
        archive_url = self.monthly()
        wrong_archive = gzip.compress(b"different data")
        self.routes[download.DAILY_BASE + "dblp.xml.gz"] = wrong_archive
        self.routes[archive_url] = wrong_archive
        with self.assertRaisesRegex(download.DownloadError, "MD5 mismatch"):
            download.refresh(self.work, {})
        self.assertFalse((self.work / "dblp.xml.gz").exists())
        self.assertFalse((self.work / "dblp.dtd").exists())

    def test_non_gzip_response_rejected_even_with_matching_md5(self):
        self.daily()
        self.monthly()
        self.routes[download.DAILY_BASE + "dblp.xml.gz.md5"] = hashlib.md5(b"not gzip").hexdigest().encode()
        self.routes[download.DAILY_BASE + "dblp.xml.gz"] = b"not gzip"
        result = download.refresh(self.work, {})
        self.assertEqual(result["mode"], "monthly-fallback")
        self.assertIn("Invalid gzip header", result["warning"])

    def test_import_status_requires_a_verified_positive_database_count(self):
        status_file = self.work / "status.json"
        with self.assertRaises(ValueError):
            download.record_status(status_file, {"changed": True}, entry_count=0)
        self.assertFalse(status_file.exists())

    def test_snapshot_links_cannot_point_to_unrelated_hosts(self):
        with self.assertRaises(download.DownloadError):
            download.one_link(["https://example.test/storage/artifacts/dblp/xml/file.xml.gz"], ".xml.gz")


if __name__ == "__main__":
    unittest.main()
