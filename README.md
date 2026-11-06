Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
# Impact-CI-dash_table_general_process

Use the shared publisher/channel lookup before either dashboard processing macro:

```sql
with_channel AS (
    {{ dash_table_general_process.publisher_channel(input_relation='dash_table') }}
),
{{ dash_table_general_process.dash_table_general_process() }}
```

`publisher_channel` uses a left join and retains the original publisher when no
mapping exists. A mapped channel takes priority. Without a channel mapping,
`platform = 'CM360'` becomes `Paid Display`; other rows become `Other`. Matching
ignores case and surrounding whitespace. Legacy inputs without a `platform`
column remain supported and use `Other` when unmapped. The JSON platform lookup
is only evaluated when the mapped channel is null.

The input must contain `publisher` and must not already contain `channel`.
`exclude_columns` optionally removes additional input columns. Client campaign
filters can remain after the macro call as a `WHERE` clause. For testing, pass a
fixture relation using `mapping_relation` instead of the default shared table.

## Installing a release

Use a release tag in your client's `packages.yml`, then run `dbt deps`:

```yaml
packages:
  - git: https://github.com/Together-NZ/Impact-CI-dash_table_general_process.git
    revision: v2.1.0
```

Commit the generated `package-lock.yml` as well. dbt records the resolved commit
there automatically; client configuration only needs the release tag.

### v2.0.3

Preserves dashboard fact rows by removing the final deduplication on campaign,
creative name, and date. Includes the shared publisher/channel fallback and
YouTube media format classification fixes. The search processing macro retains
its existing behavior.

## v2.1.0: follows in dashboard outputs

All source projections emit `follows` as `INT64` in the same column position.
TikTok and LinkedIn read their transformed `follows` values. Every other
platform, including Google/Bing search, emits `CAST(NULL AS INT64)`.
Legacy TikTok/LinkedIn tables without that field remain readable and return
null until their upstream transformations are rebuilt with follows support.

The general and custom-funnel processors preserve the field, as do both final
union macros and downstream brand filters using `SELECT *`. Inline client
source projections must also include follows in the same position before
upgrading the package. Rebuild source dashboards before their downstream
unions when deploying the new column.

```yaml
packages:
  - git: https://github.com/Together-NZ/Impact-CI-dash_table_general_process.git
    revision: v2.1.0
```

Run the local propagation and regression tests with:

```bash
python -m pip install -r requirements-test.txt
python -m unittest discover -s tests -v
```

## v2.1.1-summerset.1: Summerset union funnel mapping

Summerset-specific release. Both `dash_union_search` and `dash_union_non_search`
map Summerset campaigns to regions using inline CASE conditions. The rules
include 40 NZ village/location pairs, region-name fallbacks, the three SUM244
Special exceptions, and six SUM327 Victoria campaigns (ignoring whitespace
variations). Unmatched Summerset campaigns become Others; other clients retain
their existing funnel. Region labels use Hawkes Bay and Manawatu-Whanganui.

The general, custom-funnel, and search dashboard processors retain v2.1.0
behavior. Only the union macros apply these overrides. Pin Summerset to
`v2.1.1-summerset.1`, run `dbt deps`, then rebuild `dash_union__nz` and
`dash_union__au`. Barfoot keeps its separate package version.

Validation: all 14 local tests passed. Live NZ/AU union rebuilds were verified
against expected campaign/funnel groups, row counts, and media costs.

## v2.1.1-summerset.2: region keywords take priority over National

Both union macros check all regional keywords before the National fallback.
A campaign containing both National and Auckland now receives Auckland; a
National-only campaign still receives National. Earlier Special, village/location,
and Victoria conditions keep their existing precedence.
