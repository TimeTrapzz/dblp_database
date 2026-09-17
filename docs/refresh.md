# DBLP refresh sources

The weekly workflow first tries the current XML dump at `https://dblp.org/xml/`.
Transport errors get up to three attempts with backoff. HTTP 200 is not sufficient:
HTML/challenge pages, malformed MD5 documents, invalid DTDs, non-gzip responses,
truncated transfers, and archive checksum mismatches are rejected before import.
Downloads use temporary files and retain bounded memory use.

If the current dump cannot be downloaded, the updater discovers the latest dated
XML release from the official [DROPS collection](https://doi.org/10.4230/dblp.xml),
then uses that release's archive, checksum, and matching DTD. DBLP documents this
distribution channel in its [release announcement](https://blog.dblp.org/2024/12/02/dblp-dump-releases-now-have-a-doi/).
This fallback updates data **monthly**, even though the workflow continues checking
weekly. It does not automate or bypass the website's bot challenge.

An unchanged archive is not imported again. A fallback snapshot is imported only
when its date is newer than the stored source date. For older status files that do
not contain `source_date`, the last import's UTC calendar date is the conservative
cutoff. A different snapshot from that same day is also skipped to prevent a rollback.

`status/dblp-update.json` separates data freshness from successful checks:

- `updated_at`, `source_md5`, and `entry_count` describe the last successful import.
- `source_date` and `source_url` describe the imported data, once a new import occurs.
- `last_checked_at` and `last_check_mode` describe the most recent completed check.
- `latest_available_snapshot` and `warning` expose a fallback to monthly releases.

A check with no newer snapshot preserves the old data and import timestamp, emits
a workflow warning, and records only the check metadata. This is a successful check,
not a claim that the data is current. If both sources fail validation, the job fails
and the database is not touched. The converter still validates the full XML before
the separate PostgreSQL import step runs.

Run the offline tests with:

```sh
python -m unittest discover -s tests -p 'test_*.py'
```

Pull requests run these tests without production database secrets. Merging changes
to the updater or workflow triggers the existing production refresh.
