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

## v2.1.1-barfoot.1: Barfoot & Thompson campaign selection exceptions

Client-specific release on `release/barfoot-campaign-selection-v2.1.1-barfoot.1`.
Only Barfoot should pin this tag. Both general processing macros preserve the
full campaign name as `campaign_name_selection` when the campaign contains
`bnt` (case-insensitive) and either ends with `0034` or contains `0035`. Other
campaigns retain the existing case-normalized selection. The mapping is applied
per campaign row so campaigns sharing a selection cannot inherit each other's
exception. All other dimensions and metrics retain their existing logic.

Barfoot uses `dash_table_general_process_funnel`; no client-local override macro
is needed. Pin the dashboard package to `v2.1.1-barfoot.1`, run `dbt deps`, then
rebuild `dash_table` and `dash_union` in the Barfoot environment.

## v2.1.1-barfoot.2: match 0034 anywhere in the campaign name

Fixes the Barfoot-only 0034 exception to match anywhere in the full campaign
name, including `BNT000034_Barfoot&Thompson_Brand BT401_Meta_Social_Intent_Connect`.
The 0035 rule is unchanged. Both exceptions retain the full campaign name.
