import hashlib
import gzip
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from lxml import etree

from scripts.convert import calculate_md5, process_title, read_xml, process_entries


class ConvertTest(unittest.TestCase):
    DTD = '''<!ENTITY % field "title">
<!ELEMENT dblp (article*)>
<!ELEMENT article (%field;)*>
<!ATTLIST article key CDATA #REQUIRED>
<!ELEMENT title (#PCDATA|i)*>
<!ELEMENT i (#PCDATA)>
<!ENTITY uuml "&#252;">
<!ENTITY word "Learning">
'''

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        self.dtd_file = self.work / 'dblp.dtd'
        self.dtd_file.write_text(self.DTD)
        self.xml_file = self.work / 'dblp.xml.gz'
        self.md5_file = self.work / 'dblp.xml.gz.md5'

    def write_xml(self, body, doctype='<!DOCTYPE dblp SYSTEM "dblp.dtd">'):
        self.xml_file.write_bytes(gzip.compress(
            f'<?xml version="1.0"?>{doctype}<dblp>{body}</dblp>'.encode()))
        self.md5_file.write_text(calculate_md5(self.xml_file) + '  dblp.xml.gz\n')

    def parse_xml(self):
        # The CLI's DTD argument must work independently of the current directory.
        context, dtd = read_xml(str(self.dtd_file), str(self.xml_file), str(self.md5_file))
        return context, dtd

    def test_external_dtd_entities_are_expanded_into_sql(self):
        self.write_xml('<article key="test/1"><title>M&uuml;ller <i>&word;</i>.</title></article>')
        context, dtd = self.parse_xml()
        sql_file = self.work / 'dblp.sql'
        process_entries(context, dtd, sql_file)
        self.assertIn("('test/1', 'mllerlearning', 'article')", sql_file.read_text())

    def test_dtd_validation_is_preserved(self):
        self.write_xml('<article><title>Missing required key</title></article>')
        with self.assertRaises(etree.XMLSyntaxError):
            list(self.parse_xml()[0])

    def test_md5_mismatch_is_rejected_before_parsing(self):
        self.write_xml('')
        self.md5_file.write_text('0' * 32)
        with patch('scripts.convert.etree.iterparse') as parser:
            with self.assertRaisesRegex(Exception, 'MD5 check failed'):
                self.parse_xml()
            parser.assert_not_called()

    def test_external_general_entities_are_rejected(self):
        secret = self.work / 'secret.txt'
        secret.write_text('private content')
        for uri in (secret.as_uri(), 'https://example.invalid/secret'):
            with self.subTest(uri=uri):
                self.write_xml('<article key="test/1"><title>&secret;</title></article>',
                    f'<!DOCTYPE dblp SYSTEM "dblp.dtd" [<!ENTITY secret SYSTEM "{uri}">]>')
                with self.assertRaisesRegex(OSError, 'External XML resource is not allowed'):
                    list(self.parse_xml()[0])

    def test_external_parameter_entities_are_rejected(self):
        extra = self.work / 'extra.dtd'
        extra.write_text('<!ENTITY word "private content">')
        self.write_xml('', f'<!DOCTYPE dblp SYSTEM "dblp.dtd" [<!ENTITY % extra SYSTEM "{extra.as_uri()}">%extra;]>')
        with self.assertRaisesRegex(OSError, 'External XML resource is not allowed'):
            list(self.parse_xml()[0])

    def test_unexpected_external_dtd_is_rejected(self):
        self.write_xml('', '<!DOCTYPE dblp SYSTEM "https://example.invalid/evil.dtd">')
        with self.assertRaisesRegex(OSError, 'External XML resource is not allowed'):
            list(self.parse_xml()[0])

    def test_calculate_md5_reads_file_in_chunks(self):
        content = b"dblp test data" * 1000
        with tempfile.NamedTemporaryFile() as file:
            file.write(content)
            file.flush()

            self.assertEqual(
                calculate_md5(file.name, chunk_size=17),
                hashlib.md5(content).hexdigest(),
            )

    def test_process_title_matches_worker_normalization(self):
        title = etree.fromstring(
            "<title>DeepSeek-R1: Reasoning &amp; Reinforcement Learning.</title>"
        )

        self.assertEqual(
            process_title(title),
            "deepseekr1reasoningreinforcementlearning",
        )


if __name__ == "__main__":
    unittest.main()
