"""Execute the rendered BigQuery lookup locally, without credentials or live data."""
from pathlib import Path
import unittest

import duckdb
from jinja2 import Environment, StrictUndefined
import sqlglot


class PublisherChannelTests(unittest.TestCase):
    def setUp(self):
        self.db = duckdb.connect()
        self.db.execute("""
            CREATE TABLE input_rows AS
            SELECT * FROM (VALUES
                (1, ' mapped ', 'CM360', 10, 'Keep this'),
                (2, 'Unmapped direct', 'CM360', 20, 'Keep this'),
                (3, 'Unmapped social', 'Meta', 30, 'Keep this'),
                (4, 'Canonical only', 'CM360', 40, 'Keep this'),
                (5, NULL, 'Meta', 50, 'Keep this')
            ) AS t(id, publisher, platform, media_cost, creative_descr)
        """)
        self.db.execute("""
            CREATE TABLE mapping AS
            SELECT * FROM (VALUES
                ('Mapped', 'Paid Video'),
                ('Canonical Only', NULL)
            ) AS t(publisher, channel)
        """)
        text = (Path(__file__).resolve().parents[1] / 'macros/publisher_channel.sql').read_text()
        self.macro = Environment(undefined=StrictUndefined).from_string(text).module.publisher_channel

    def tearDown(self):
        self.db.close()

    def run_lookup(self, **kwargs):
        query = self.macro(input_relation='input_rows', mapping_relation='mapping', **kwargs)
        sql = sqlglot.transpile(query, read='bigquery', write='duckdb')[0]
        result = self.db.execute(sql)
        columns = [c[0] for c in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def test_mappings_and_fallbacks_preserve_all_rows_and_metrics(self):
        rows = {r['id']: r for r in self.run_lookup()}
        self.assertEqual(len(rows), 5)
        self.assertEqual(sum(r['media_cost'] for r in rows.values()), 150)
        self.assertEqual((rows[1]['publisher'], rows[1]['channel']), ('Mapped', 'Paid Video'))
        self.assertEqual((rows[2]['publisher'], rows[2]['channel']), ('Unmapped direct', 'Paid Display'))
        self.assertEqual((rows[3]['publisher'], rows[3]['channel']), ('Unmapped social', 'Other'))
        self.assertEqual((rows[4]['publisher'], rows[4]['channel']), ('Canonical Only', 'Paid Display'))
        self.assertIsNone(rows[5]['publisher'])
        self.assertEqual(rows[5]['channel'], 'Other')
        self.assertTrue(all(r['creative_descr'] == 'Keep this' for r in rows.values()))

    def test_legacy_input_without_platform(self):
        self.db.execute('ALTER TABLE input_rows DROP COLUMN platform')
        rows = {r['id']: r for r in self.run_lookup()}
        self.assertEqual(len(rows), 5)
        self.assertEqual(rows[2]['channel'], 'Other')
        self.assertEqual(rows[1]['channel'], 'Paid Video')

    def test_additional_column_exclusion(self):
        rows = self.run_lookup(exclude_columns=['creative_descr'])
        self.assertNotIn('creative_descr', rows[0])
        self.assertIn('media_cost', rows[0])


if __name__ == '__main__':
    unittest.main()
