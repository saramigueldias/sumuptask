{% snapshot dim_transactions_history %}

{{
  config(
    target_schema='dimensions',
    strategy='check',
    unique_key='id',
    check_cols='all',
    invalidate_hard_deletes=True
  )
}}

  select *
  from {{ ref('dim_transactions') }}

{% endsnapshot %}
