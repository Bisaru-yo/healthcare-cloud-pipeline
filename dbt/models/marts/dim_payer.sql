-- UUID is the key; a payer name is only a display label.
select * from {{ ref('stg_payers') }}
