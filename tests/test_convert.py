import hashlib
import tempfile
import unittest

from lxml import etree

from scripts.convert import calculate_md5, process_title


class ConvertTest(unittest.TestCase):
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
