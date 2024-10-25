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
        logger.info(f"文件 MD5: {file_md5}")
        logger.info(f"实际 MD5: {md5_content}")
        if file_md5 != md5_content:
            raise Exception("MD5 校验失败")

    dtd = etree.DTD(file=dtd_file)

    with gzip.open(xml_file, 'rb') as f:
        parser = etree.XMLParser(dtd_validation=True)
        tree = etree.parse(f, parser)
        root = tree.getroot()

    # 验证XML是否符合DTD
    if not dtd.validate(tree):
        raise Exception("XML 文件不符合 DTD 规范")

    return root


def process_title(title_elem):
    if title_elem is None:
        return ""
    # 处理可能包含的HTML标签
    title_text = ''.join(title_elem.itertext()).strip()
    # 只保留字母数字字符，并转小写
    title_text = ''.join(char.lower()
                         for char in title_text if char.isalnum() and char.isascii())
    return title_text


def parse_xml(root):
    parsed_data = []
    for elem in tqdm(root, desc="解析 XML 数据"):
        if elem.tag in ['article', 'inproceedings', 'proceedings', 'book', 'incollection',
                        'phdthesis', 'mastersthesis', 'www', 'person', 'data']:
            try:
                url = elem.get('key')
                entry_type = elem.tag

                title_elem = elem.find('title')
                title_text = process_title(title_elem)

                parsed_data.append((url, title_text, entry_type))
            except Exception as e:
                logger.error(f"处理条目时出错: {e}")
                logger.error(etree.tostring(
                    elem, encoding='unicode', pretty_print=True))

    logger.info(f"解析完成，共处理了 {len(parsed_data)} 条数据")

    return parsed_data


def create_database_sql(parsed_data, sql_file):
    with open(sql_file, 'w') as f:
        # 创建表时添加全文索引
        f.write("""
        CREATE TABLE IF NOT EXISTS dblp_entries_tmp (
            id SERIAL PRIMARY KEY,
            url TEXT UNIQUE,
            title TEXT,
            type TEXT
        );
        CREATE INDEX IF NOT EXISTS dblp_entries_title_idx_tmp ON dblp_entries_tmp USING GIN(to_tsvector('english', title));
        """)
        

        # 分批插入数据
        batch_size = 3000
        total_batches = len(parsed_data) // batch_size + \
            (1 if len(parsed_data) % batch_size != 0 else 0)

        for batch in tqdm(range(total_batches), desc="插入数据"):
            start_idx = batch * batch_size
            end_idx = min((batch + 1) * batch_size, len(parsed_data))
            batch_data = parsed_data[start_idx:end_idx]

            f.write("INSERT INTO dblp_entries_tmp (url, title, type) VALUES\n")
            for i, (url, title, entry_type) in enumerate(batch_data):
                f.write(f"('{url}', '{title}', '{entry_type}')")
                if i < len(batch_data) - 1:
                    f.write(",\n")
                else:
                    f.write(";\n")

            if batch < total_batches - 1:
                f.write("\n")

        f.write("""
        DROP TABLE IF EXISTS dblp_entries;
        ALTER TABLE dblp_entries_tmp RENAME TO dblp_entries;
        ALTER INDEX dblp_entries_title_idx_tmp RENAME TO dblp_entries_title_idx;
        """)


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

    root = read_xml(args.dtd_file, args.xml_file, args.md5_file)
    parsed_data = parse_xml(root)
    create_database_sql(parsed_data, args.output_sql_file)
