import gzip
from lxml import etree
from tqdm import tqdm
import logging
import argparse
import hashlib


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def read_xml(dtd_file, xml_file, md5_file):
    with open(md5_file, 'r') as f:
        md5_content = f.read().strip().split(' ')[0]
    with open(xml_file, 'rb') as f:
        file_md5 = hashlib.md5(f.read()).hexdigest()
        logger.info(f"File MD5: {file_md5}")
        logger.info(f"Actual MD5: {md5_content}")
        if file_md5 != md5_content:
            raise Exception("MD5 check failed")

    dtd = etree.DTD(file=dtd_file)

    context = etree.iterparse(
        gzip.open(xml_file), events=('end',), dtd_validation=True)

    return context, dtd


def process_title(title_elem):
    if title_elem is None:
        return ""
    title_text = ''.join(title_elem.itertext()).strip()
    title_text = ''.join(char.lower()
                         for char in title_text if char.isalnum() and char.isascii())
    return title_text


def process_entries(context, dtd, sql_file):
    batch_size = 3000
    batch_data = []
    total_count = 0

    with open(sql_file, 'w') as f:
        # Write table creation SQL
        f.write("""
        CREATE TABLE IF NOT EXISTS dblp_entries_tmp (
            id SERIAL PRIMARY KEY,
            url TEXT UNIQUE,
            title TEXT,
            type TEXT
        );
        """)

        # Process XML elements in streaming fashion
        for event, elem in tqdm(context, desc="Processing entries"):
            if elem.tag in ['article', 'inproceedings', 'proceedings', 'book', 'incollection',
                            'phdthesis', 'mastersthesis', 'www', 'person', 'data']:
                try:
                    url = elem.get('key')
                    entry_type = elem.tag
                    title_elem = elem.find('title')
                    title_text = process_title(title_elem)

                    batch_data.append((url, title_text, entry_type))

                    # Write batch when reaching batch_size
                    if len(batch_data) >= batch_size:
                        write_batch(f, batch_data)
                        total_count += len(batch_data)
                        batch_data = []

                except Exception as e:
                    logger.error(f"Error processing entry: {e}")

                # Clear element to free memory
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

        # Write remaining data
        if batch_data:
            write_batch(f, batch_data)
            total_count += len(batch_data)

        # Write final SQL commands
        f.write("""
        CREATE INDEX IF NOT EXISTS dblp_entries_title_idx_tmp ON dblp_entries_tmp USING GIN(to_tsvector('english', title));
        DROP TABLE IF EXISTS dblp_entries;
        ALTER TABLE dblp_entries_tmp RENAME TO dblp_entries;
        ALTER INDEX dblp_entries_tmp_pkey RENAME TO dblp_entries_pkey;
        ALTER INDEX dblp_entries_tmp_url_key RENAME TO dblp_entries_url_key;
        ALTER INDEX dblp_entries_title_idx_tmp RENAME TO dblp_entries_title_idx;
        """)

    logger.info(f"Processing complete, processed {total_count} entries")


def write_batch(f, batch_data):
    f.write("INSERT INTO dblp_entries_tmp (url, title, type) VALUES\n")
    for i, (url, title, entry_type) in enumerate(batch_data):
        f.write(f"('{url}', '{title}', '{entry_type}')")
        if i < len(batch_data) - 1:
            f.write(",\n")
        else:
            f.write(";\n\n")


if __name__ == "__main__":
    args_parser = argparse.ArgumentParser(
        description="Convert Dblp XML to JSON")
    args_parser.add_argument("--dtd_file", type=str,
                             default="dblp.dtd", help="DTD file name")
    args_parser.add_argument("--xml_file", type=str,
                             default="dblp.xml.gz", help="XML file name")
    args_parser.add_argument("--md5_file", type=str,
                             default="dblp.xml.gz.md5", help="MD5 file name")
    args_parser.add_argument(
        "--output_sql_file", type=str, default="dblp.sql", help="Output SQL file name")
    args = args_parser.parse_args()

    logger.info("Starting XML reading")
    context, dtd = read_xml(args.dtd_file, args.xml_file, args.md5_file)
    logger.info("Processing XML and creating SQL file")
    process_entries(context, dtd, args.output_sql_file)
    logger.info("Complete")
