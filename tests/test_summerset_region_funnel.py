"""Execute the inline Summerset CASE from both dashboard processors."""
from pathlib import Path
import unicodedata
import unittest

import duckdb
from jinja2 import Environment, StrictUndefined
import sqlglot

MAPPING = [['Northland', 'Summerset Mount Denby', 'Whangārei'],
 ['Auckland', 'Summerset St Johns', 'St Johns'],
 ['Auckland', 'Summerset Half Moon Bay', 'Half Moon Bay'],
 ['Auckland', 'Summerset Milldale', 'Milldale'],
 ['Auckland', 'Summerset at Heritage Park', 'Ellerslie'],
 ['Auckland', 'Summerset by the Park', 'Manukau'],
 ['Auckland', 'Summerset at Monterey Park', 'Hobsonville'],
 ['Auckland', 'Summerset Falls', 'Warkworth'],
 ['Auckland', 'Summerset at Karaka', 'Karaka'],
 ['Waikato', 'Summerset Cambridge', 'Cambridge'],
 ['Waikato', 'Summerset down the Lane', 'Hamilton, Fitzroy'],
 ['Waikato', 'Summerset Rototuna', 'Hamilton, Rototuna'],
 ['Waikato', 'Summerset by the Lake', 'Taupō'],
 ['Bay of Plenty', 'Summerset by the Dunes', 'Pāpāmoa'],
 ['Bay of Plenty', 'Summerset by the Sea', 'Katikati'],
 ['Hawkes Bay', 'Summerset Palms', 'Napier, Te Awa'],
 ['Hawkes Bay', 'Summerset in the Orchard', 'Hastings'],
 ['Hawkes Bay', 'Summerset in the Vines', 'Havelock North'],
 ['Hawkes Bay', 'Summerset in the Bay', 'Napier, Greenmeadows'],
 ['Taranaki', 'Summerset at Pohutukawa Place', 'New Plymouth, Bell Block'],
 ['Taranaki', 'Summerset Mountain View', 'New Plymouth, Vogeltown'],
 ['Manawatu-Whanganui', 'Summerset Kelvin Grove', 'Palmerston North'],
 ['Manawatu-Whanganui', 'Summerset by the Ranges', 'Levin'],
 ['Manawatu-Whanganui', 'Summerset on Summerhill', 'Palmerston North'],
 ['Manawatu-Whanganui', 'Summerset in the River City', 'Whanganui'],
 ['Wellington', 'Summerset Boulcott', 'Lower Hutt'],
 ['Wellington', 'Summerset Waikanae', 'Waikanae'],
 ['Wellington', 'Summerset on the Landing', 'Kenepuru'],
 ['Wellington', 'Summerset at Aotea', 'Porirua'],
 ['Wellington', 'Summerset on the Coast', 'Paraparaumu'],
 ['Wellington', 'Summerset at the Course', 'Upper Hutt'],
 ['Marlborough', 'Summerset Blenheim', 'Blenheim'],
 ['Nelson-Tasman', 'Summerset Richmond Ranges', 'Richmond'],
 ['Nelson-Tasman', 'Summerset in the Sun', 'Nelson, Stoke'],
 ['Canterbury', 'Summerset Prebbleton', 'Prebbleton'],
 ['Canterbury', 'Summerset Rangiora', 'Rangiora'],
 ['Canterbury', 'Summerset at Avonhead', 'Christchurch'],
 ['Canterbury', 'Summerset on Cavendish', 'Christchurch, Casebrook'],
 ['Canterbury', 'Summerset at Wigram', 'Christchurch'],
 ['Otago', 'Summerset at Bishopscourt', 'Dunedin']]

class SummersetRegionFunnelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.queries = []
        root = Path(__file__).resolve().parents[1] / 'macros'
        for name in ('dash_union_non_search', 'dash_union_search'):
            env = Environment(undefined=StrictUndefined)
            env.globals['source'] = lambda source_name, table_name: 'input_rows'
            module = env.from_string((root / (name + '.sql')).read_text()).module
            macro = getattr(module, name)
            sql = macro('test_source', 'test_table', 'NULL')
            sqlglot.parse_one(sql, read='bigquery')
            cte = sql[sql.index('summerset_campaign_text AS ('):].rstrip()[:-1]
            query = 'WITH union_rows AS (SELECT ? AS campaign_name, ? AS funnel), ' + cte
            cls.queries.append((name, sqlglot.transpile(query, read='bigquery', write='duckdb')[0]))

    def check_campaign(self, campaign, expected):
        for name, query in self.queries:
            with self.subTest(macro=name, campaign=campaign), duckdb.connect() as db:
                row = db.execute(query, [campaign, 'awareness']).fetchone()
                self.assertEqual(row, (campaign, expected))

    def test_all_villages_locations_and_normalized_spellings(self):
        self.assertEqual(len(MAPPING), 40)
        for region, village, location in MAPPING:
            for phrase in (village, location):
                self.check_campaign('SUM244_' + phrase + '_Intent', region)
                text = unicodedata.normalize('NFKD', phrase).encode('ascii', 'ignore').decode()
                self.check_campaign('SUM244_' + text.upper().replace(', ', '_').replace(' ', '_') + '_Intent', region)

    def test_exact_special_exceptions(self):
        for campaign in (
            'SUM244 |  | National | 14 | NZ | Memory Care | Low Priority Locations',
            'SUM244 |  | National | 14 | NZ | Memory Care | Priority Locations',
            'SUM244 |  | National | 14 | NZ | Search Remarketing',
        ):
            self.check_campaign(campaign, 'Special')

    def test_summerset_national_and_unmatched(self):
        self.check_campaign('SUM244_National', 'National')
        self.check_campaign('SUM244_Unmapped', 'Others')

    def test_victoria_exact_campaigns(self):
        for location in ('Oakleigh South', 'Chirnside Park', 'Cranbourne North'):
            for campaign_type in ('Brand', 'Generic'):
                self.check_campaign('SUM327 | Search | ' + location + ' | ' + campaign_type, 'Victoria')
        self.check_campaign('SUM327 | Search |  Oakleigh South | Generic', 'Victoria')
        self.check_campaign('SUM327 | Search |\u00a0 Oakleigh South | Generic ', 'Victoria')
        self.check_campaign('SUM327 | Search | Oakleigh South | Other', 'Others')

    def test_final_region_keyword_fallbacks(self):
        for region in ['National', 'Northland', 'Auckland', 'Bay of Plenty', 'Waikato', 'Hawkes Bay', 'Gisborne', 'Taranaki', 'Manawatu-Whanganui', 'Wellington', 'Marlborough', 'Nelson-Tasman', 'Canterbury', 'Otago']:
            self.check_campaign('SUM244_Regional_' + region.upper() + '_Traffic', region)
        self.check_campaign("SUM244_Hawke's Bay_Traffic", 'Hawkes Bay')
        self.check_campaign('SUM244_Manawatū-Whanganui_Traffic', 'Manawatu-Whanganui')
        self.check_campaign('SUM244_Canterbury_Summerset Cambridge', 'Waikato')
        self.check_campaign('OTHER_CLIENT_Wellington', 'awareness')

    def test_other_clients_unmatched_and_null_preserve_funnel(self):
        for campaign in ('OTHER_CLIENT_Whangarei', '', None):
            self.check_campaign(campaign, 'awareness')

if __name__ == '__main__':
    unittest.main()
