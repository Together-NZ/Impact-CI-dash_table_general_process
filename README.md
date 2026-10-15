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
