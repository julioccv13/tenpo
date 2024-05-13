{{ 
  config(
    materialized='table', 
  ) 
}}


SELECT 
      distinct
      a.id,
      a.created_at,
      a.updated_at,
      user_id as user,
      b.id as user_contact_added,
      b.tributary_identifier as tributary_identifier_contact_added,
      b.state state_conctact_added, 
      bank.banco_tienda_origen as banco_contact_added,
      case when type = 'd0c93549-8aa6-48eb-890f-75d93f38f37e' then 'Cuenta Corriente'
            when type = 'efcc47a5-bb40-4201-9e8d-b69318d1d46c' then 'Cuenta Vista'
            when type = '7d20315f-3f5d-4939-8bce-fb91275f9e11' then 'Cuenta Ahorro'
            when type = 'd9f51d27-c4a4-42b9-994b-5bd38a38d360' then 'Tenpo Prepago'
      else null end as account_type_contact_added,
      a.last_used_at as last_interacted,
FROM {{ source('accounts_contacts', 'account_contact') }} a
LEFT JOIN {{ source('tenpo_users', 'users') }} b on a.run = b.rut
LEFT JOIN {{ ref('banks') }} bank ON a.sbif_code = bank.originSbifCode

