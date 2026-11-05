"""Execute campaign selection CTEs with SQLite; they use portable SQL."""
from pathlib import Path
import sqlite3
import unittest

from jinja2 import Environment, StrictUndefined


class BarfootCampaignSelectionTests(unittest.TestCase):
    def test_both_processors_apply_exceptions_without_changing_fact_rows(self):
        for name in ('dash_table_general_process', 'dash_table_general_process_funnel'):
            with self.subTest(macro=name):
                path = Path(__file__).resolve().parents[1] / 'macros' / (name + '.sql')
                module = Environment(undefined=StrictUndefined).from_string(path.read_text()).module
                macro = getattr(module, name)
                sql = macro(['awareness', 'consideration', 'intent']) if name.endswith('_funnel') else macro()
                start = sql.index('campaign_name_selection_duplicate AS (')
                end = sql.index('dash_table_final AS (') if name == 'dash_table_general_process' else sql.index('SELECT camb.* EXCEPT')
                ctes = sql[start:end].rstrip().rstrip(',')
                with sqlite3.connect(':memory:') as db:
                    db.execute('CREATE TABLE campaign_base(id INT, campaign_name TEXT, campaign_name_raw TEXT, cost INT)')
                    db.executemany('INSERT INTO campaign_base VALUES (?, ?, ?, ?)', [
                        (1, 'BNT_sale_0034', 'sale', 10),
                        (2, 'bnt_sale_0035_extra', 'sale', 20),
                        (3, 'bnt_sale_0034_extra', 'sale', 30),
                        (4, 'other_0035', 'sale', 40),
                        (5, 'bnt_SALE_normal', 'SALE', 50),
                        (6, 'BNT_sale_0034', 'sale', 60),
                        (7, None, None, 70),
                    ])
                    rows = db.execute('WITH ' + ctes + ' SELECT id, campaign_name_selection, cost FROM manual_campaign_name_selection_mapping ORDER BY id').fetchall()
                self.assertEqual(rows, [
                    (1, 'BNT_sale_0034', 10),
                    (2, 'bnt_sale_0035_extra', 20),
                    (3, 'SALE', 30),
                    (4, 'SALE', 40),
                    (5, 'SALE', 50),
                    (6, 'BNT_sale_0034', 60),
                    (7, None, 70),
                ])
                self.assertIn('FROM manual_campaign_name_selection_mapping AS camb', sql)


if __name__ == '__main__':
    unittest.main()
