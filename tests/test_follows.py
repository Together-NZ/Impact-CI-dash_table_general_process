"""Execute source projections and downstream dashboard unions with follows."""
from pathlib import Path
import unittest

import duckdb
from jinja2 import Environment, StrictUndefined
import sqlglot


NON_SEARCH = ['cm360','dv360_standard','dv360_youtube','google_ads','gpt',
              'hivestack','linkedin','meta','outbrain','pinterest','reddit',
              'snapchat','tiktok','ttd']
SEARCH = ['google_ads_search','bing_ads_search']


class FollowsTests(unittest.TestCase):
    def setUp(self):
        self.db=duckdb.connect()
        self.db.execute("""CREATE TABLE input_rows AS SELECT
          10.0 AS media_cost,100 AS impressions,5 AS clicks,
          'creative_social_vid' AS creative_name,'creative_social_vid' AS ad_name,
          'audience' AS audience_name,'video' AS ad_format,'video' AS ad_format_detail,
          8 AS video_completion,9 AS video_25_completion,8 AS video_50_completion,
          7 AS video_75_completion,10 AS video_views,10 AS video_play,10 AS video_played,
          'CLIENT0001_Test_social' AS campaign_name,'Test Publisher' AS publisher,
          'Test' AS campaign_descr,'Creative' AS creative_descr,
          DATE '2026-10-01' AS date,2 AS conversions,3 AS result,
          7::BIGINT AS follows,'mobile' AS segments_device,'mobile' AS device,
          123::BIGINT AS campaign_id""")
        self.db.execute('CREATE TABLE legacy_rows AS SELECT * EXCLUDE(follows) FROM input_rows')
        self.db.execute('CREATE TABLE empty_yt AS SELECT campaign_name FROM input_rows WHERE false')
        self.db.execute("CREATE TABLE lookup AS SELECT 'Test Publisher' publisher,'Paid Social' channel")
        self.db.execute('CREATE TABLE google_rows AS SELECT * EXCLUDE(device) FROM input_rows')
        self.relations={'google_input':'google_rows'}
        self.macros=Environment(undefined=StrictUndefined).from_string('\n'.join(
            p.read_text() for p in sorted((Path(__file__).resolve().parents[1]/'macros').glob('*.sql'))
        )).make_module({'source':lambda source,table:self.relations.get(source,'input_rows')})

    def tearDown(self):self.db.close()

    def run_sql(self,sql):
        rendered=sqlglot.transpile(sql,read='bigquery',write='duckdb')[0]
        result=self.db.execute(rendered)
        return [d[0] for d in result.description],result.fetchall()

    def source_sql(self,name,source='input'):
        kwargs={'source_name':'google_input' if name=='google_ads_search' else source,'table_name':'table'}
        if name=='dv360_standard':
            self.relations['youtube']='empty_yt'
            kwargs.update(yt_source_name='youtube',yt_table_name='table')
        return getattr(self.macros,name)(**kwargs)

    def materialize(self,name,sql):
        self.db.execute('CREATE TABLE '+name+' AS '+sqlglot.transpile(sql,read='bigquery',write='duckdb')[0])
        self.relations[name]=name

    def processed_sql(self,raw,funnel=False):
        lookup=self.macros.publisher_channel(input_relation='dash_table',mapping_relation='lookup')
        finish=self.macros.dash_table_general_process_funnel(['attract','consider']) if funnel else self.macros.dash_table_general_process()
        return f'WITH dash_table AS ({raw}), with_channel AS ({lookup}), {finish}'

    def test_every_source_has_same_order_and_real_or_null_follows(self):
        expected=None
        for name in NON_SEARCH:
            with self.subTest(source=name):
                cols,rows=self.run_sql(self.source_sql(name))
                if expected is None:expected=cols
                self.assertEqual(cols,expected)
                self.assertEqual(cols[-1],'follows')
                self.assertEqual(rows[0][-1],7 if name in ['linkedin','tiktok'] else None)
                self.assertEqual(len(rows),1)
        for name in SEARCH:
            with self.subTest(source=name):
                cols,rows=self.run_sql(self.source_sql(name))
                self.assertEqual(cols[-1],'follows')
                self.assertIsNone(rows[0][-1])

    def test_old_linkedin_and_tiktok_tables_remain_readable(self):
        self.relations['legacy']='legacy_rows'
        for name in ['linkedin','tiktok']:
            cols,rows=self.run_sql(self.source_sql(name,'legacy'))
            self.assertIsNone(rows[0][cols.index('follows')])

    def test_downstream_processing_search_union_and_brand_filter(self):
        raw=' UNION ALL '.join(self.source_sql(n) for n in NON_SEARCH)
        raw_cols,raw_rows=self.run_sql(raw)
        expected={metric:sum(r[raw_cols.index(metric)] or 0 for r in raw_rows) for metric in ['media_cost','impressions','clicks','follows']}
        self.materialize('processed',self.processed_sql(raw))
        cols,rows=self.run_sql('SELECT * FROM processed')
        self.assertEqual(len(rows),len(NON_SEARCH))
        for metric,value in expected.items():self.assertEqual(sum(r[cols.index(metric)] or 0 for r in rows),value)
        search_raw=' UNION ALL '.join(self.source_sql(n) for n in SEARCH)
        self.materialize('search_processed','WITH dash_table AS ('+search_raw+'), '+self.macros.dash_table_search_general_process('INTENT'))
        self.materialize('combined',self.macros.dash_union_non_search('processed','table','sub_brands')+' UNION ALL '+self.macros.dash_union_search('search_processed','table','NULL'))
        cols,rows=self.run_sql('SELECT * FROM combined')
        self.assertEqual(len(rows),len(NON_SEARCH)+len(SEARCH))
        self.assertEqual(sum(r[cols.index('follows')] or 0 for r in rows),14)
        self.assertEqual(sum(r[cols.index('media_cost')] for r in rows),expected['media_cost']+20)
        cols,rows=self.run_sql("SELECT * FROM combined WHERE sub_brands='Wealth'")
        self.assertEqual(sum(r[cols.index('follows')] or 0 for r in rows),14)

    def test_custom_funnel_processor_preserves_follows(self):
        self.materialize('funnel_processed',self.processed_sql(self.source_sql('tiktok'),funnel=True))
        cols,rows=self.run_sql('SELECT * FROM funnel_processed')
        self.assertEqual(rows[0][cols.index('follows')],7)

    def test_union_of_old_dashboard_returns_typed_null(self):
        self.materialize('processed',self.processed_sql(self.source_sql('meta')))
        self.materialize('old_dashboard','SELECT * EXCEPT(follows) FROM processed')
        cols,rows=self.run_sql(self.macros.dash_union_non_search('old_dashboard','table','sub_brands'))
        self.assertIsNone(rows[0][cols.index('follows')])


if __name__=='__main__':unittest.main()
