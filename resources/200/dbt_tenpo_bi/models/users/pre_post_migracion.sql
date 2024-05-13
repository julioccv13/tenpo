{{ config(materialized='table') }}

with 
  users as (
    select distinct
      id,
      rut,
      email,
      last_ndd_service_bm as source,
      date(ts_ob_tenpo) as date_ob_tenpo,
      timestamp_diff(current_timestamp() , ts_ob_tenpo, DAY) AS days_since_ob,
      IF(date_diff(current_date() , date(ts_ob_tenpo), MONTH)=0,1,date_diff(current_date() , date(ts_ob_tenpo), MONTH)) AS months_since_ob,
      date_sub(date(ts_ob_tenpo), INTERVAL 4 MONTH)  AS date_trx_since,
      date_add(date(ts_ob_tenpo), INTERVAL 4 MONTH)  AS date_trx_until,
    from {{ref("users_allservices")}}
    where last_ndd_service_bm != "Nuevos"
      and state in (4,7,8,21,22)
      and TIMESTAMP_DIFF(current_timestamp(),ts_ob_tenpo, DAY) > 7
  )
    (
    with 
      trx_toups as (
      select distinct
        u.id,
        sum(t.monto) OVER (PARTITION BY u.id)/months_since_ob AS gpv_topup,
        count(1) OVER (PARTITION BY u.id) trx_topup,
        LAST_VALUE(t.fecha_creacion) OVER (PARTITION BY u.id ORDER BY t.fecha_creacion RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS date_last_topup,
        date_diff(date_ob_tenpo,LAST_VALUE(DATE(t.fecha_creacion)) OVER (PARTITION BY u.id ORDER BY t.fecha_creacion RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),DAY) recency_topup,
      from {{source("topups_web","ref_transaccion")}} t
        join {{source("topups_web","ref_recarga")}} r on (t.id = r.id_transaccion)
        join users u on t.correo_usuario=u.email
      where t.id_estado =20 and r.id_estado = 27
        and date(timestamp(DATETIME(t.fecha_creacion),"America/Santiago")) BETWEEN date_trx_since and date_ob_tenpo
      ),
      trx_paypal_topup as (
        select distinct
          u.id,
          sum(t.mto_monto_dolar*t.valor_dolar_multicaja) OVER (PARTITION BY u.id)/months_since_ob AS gpv_paypal_topup,
          count(DISTINCT id_trx) OVER (PARTITION BY u.id) AS trx_paypal_topup,
          LAST_VALUE(t.fecha_trx) OVER (PARTITION BY u.id ORDER BY t.fecha_trx RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS date_last_paypal_topup,
          date_diff(date_ob_tenpo,LAST_VALUE(DATE(t.fecha_trx)) OVER (PARTITION BY u.id ORDER BY t.fecha_trx RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),DAY) recency_paypal_topup,
        from users u
          join{{ref('transacciones_paypal')}} t on (t.rut = u.rut)
        where DATE(TIMESTAMP(t.fecha_trx,"UTC")) BETWEEN u.date_trx_since AND u.date_ob_tenpo 
          AND tip_trx="ABONO_PAYPAL"
      ),
      trx_paypal_withdraw as (
        select distinct
          u.id,
          sum(t.mto_monto_dolar*t.valor_dolar_multicaja) OVER (PARTITION BY u.id)/months_since_ob AS gpv_paypal_withdraw,
          count(DISTINCT id_trx) OVER (PARTITION BY u.id) AS trx_paypal_withdraw,
          LAST_VALUE(t.fecha_trx) OVER (PARTITION BY u.id ORDER BY t.fecha_trx RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS date_last_paypal_withdraw,
          date_diff(date_ob_tenpo,LAST_VALUE(DATE(t.fecha_trx)) OVER (PARTITION BY u.id ORDER BY t.fecha_trx RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),DAY) recency_paypal_withdraw,
        from users u
          join{{ref('transacciones_paypal')}} t on (t.rut = u.rut)
        where DATE(TIMESTAMP(t.fecha_trx,"UTC")) BETWEEN u.date_trx_since AND u.date_ob_tenpo 
          AND tip_trx="RETIRO_PAYPAL"
      ),
      trx_toups_post as (
      select distinct
        u.id,
        sum(t.monto) OVER (PARTITION BY u.id)/months_since_ob AS gpv_topup_post,
        count(1) OVER (PARTITION BY u.id) trx_topup_post,
        LAST_VALUE(t.fecha_creacion) OVER (PARTITION BY u.id ORDER BY t.fecha_creacion RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS date_last_topup_post,
        date_diff(date_ob_tenpo,LAST_VALUE(DATE(t.fecha_creacion)) OVER (PARTITION BY u.id ORDER BY t.fecha_creacion RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),DAY) recency_topup_post,
      from {{source("topups_web","ref_transaccion")}} t
        join {{source("topups_web","ref_recarga")}} r on (t.id = r.id_transaccion)
        join users u on t.correo_usuario=u.email
      where t.id_estado =20 and r.id_estado = 27
        and date(timestamp(DATETIME(t.fecha_creacion),"America/Santiago")) BETWEEN date_ob_tenpo AND date_trx_until
      ),
      trx_paypal_topup_post as (
        select distinct
          u.id,
          sum(t.mto_monto_dolar*t.valor_dolar_multicaja) OVER (PARTITION BY u.id)/months_since_ob AS gpv_paypal_topup_post,
          count(DISTINCT id_trx) OVER (PARTITION BY u.id) AS trx_paypal_topup_post,
          LAST_VALUE(t.fecha_trx) OVER (PARTITION BY u.id ORDER BY t.fecha_trx RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS date_last_paypal_topup_post,
          date_diff(date_ob_tenpo,LAST_VALUE(DATE(t.fecha_trx)) OVER (PARTITION BY u.id ORDER BY t.fecha_trx RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),DAY) recency_paypal_topup_post,
        from users u
          join{{ref('transacciones_paypal')}}  t on (t.rut = u.rut)
        where DATE(TIMESTAMP(t.fecha_trx,"UTC")) BETWEEN date_ob_tenpo AND date_trx_until
          AND tip_trx="ABONO_PAYPAL"
      ),
      trx_paypal_withdraw_post as (
        select distinct
          u.id,
          sum(t.mto_monto_dolar*t.valor_dolar_multicaja) OVER (PARTITION BY u.id)/months_since_ob AS gpv_paypal_withdraw_post,
          count(DISTINCT id_trx) OVER (PARTITION BY u.id) AS trx_paypal_withdraw_post,
          LAST_VALUE(t.fecha_trx) OVER (PARTITION BY u.id ORDER BY t.fecha_trx RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS date_last_paypal_withdraw_post,
          date_diff(date_ob_tenpo,LAST_VALUE(DATE(t.fecha_trx)) OVER (PARTITION BY u.id ORDER BY t.fecha_trx RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),DAY) recency_paypal_withdraw_post,
        from users u
          join{{ref('transacciones_paypal')}}  t on (t.rut = u.rut)
        where DATE(TIMESTAMP(t.fecha_trx,"UTC")) BETWEEN date_ob_tenpo AND date_trx_until
          AND tip_trx="RETIRO_PAYPAL"
      ),
      trx_tenpo as (
        select distinct
          u.id,
          sum(t.monto) OVER (PARTITION BY u.id)/months_since_ob AS gpv_tenpo, 
          count(distinct t.trx_id) OVER (PARTITION BY u.id) AS trx_tenpo, 
          LAST_VALUE(t.fecha) OVER (PARTITION BY u.id ORDER BY t.fecha RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS date_last_tenpo_trx,
          date_diff(current_date(),LAST_VALUE(DATE(t.fecha)) OVER (PARTITION BY u.id ORDER BY t.fecha RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),DAY) recency_tenpo,
        from {{ref("economics")}} t
          join users u on (t.user=u.id)
        where t.linea not in ("cash_in","cash_out")-- select distinct linea from {{ref("economics")}}
          and fecha BETWEEN date_ob_tenpo AND date_trx_until
      ),
      trx_tenpo_topup as (
        select distinct
          u.id,
          sum(t.monto) OVER (PARTITION BY u.id)/months_since_ob AS gpv_tenpo_topup, 
          count(distinct t.trx_id) OVER (PARTITION BY u.id) AS trx_tenpo_topup, 
          LAST_VALUE(t.fecha) OVER (PARTITION BY u.id ORDER BY t.fecha RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS date_last_tenpo_trx_topup,
          date_diff(current_date(),LAST_VALUE(DATE(t.fecha)) OVER (PARTITION BY u.id ORDER BY t.fecha RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),DAY) recency_tenpo_topup,
        --select distinct linea
        from {{ref("economics")}} t
          join users u on (t.user=u.id)
        where t.linea = "top_ups"
          and fecha BETWEEN date_ob_tenpo AND date_trx_until
      ),
      trx_tenpo_mastercard as (
        select distinct
          u.id,
          sum(t.monto) OVER (PARTITION BY u.id)/months_since_ob AS gpv_tenpo_mastercard, 
          count(distinct t.trx_id) OVER (PARTITION BY u.id) AS trx_tenpo_mastercard, 
          LAST_VALUE(t.fecha) OVER (PARTITION BY u.id ORDER BY t.fecha RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS date_last_tenpo_trx_mastercard,
          date_diff(current_date(),LAST_VALUE(DATE(t.fecha)) OVER (PARTITION BY u.id ORDER BY t.fecha RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),DAY) recency_tenpo_mastercard,
        from {{ref("economics")}} t
          join users u on (t.user=u.id)
        where t.linea = "mastercard"
          and fecha BETWEEN date_ob_tenpo AND date_trx_until
      ),
      trx_tenpo_paypal as (
        select distinct
          u.id,
          sum(t.monto) OVER (PARTITION BY u.id)/months_since_ob AS gpv_tenpo_paypal, 
          count(distinct t.trx_id) OVER (PARTITION BY u.id) AS trx_tenpo_paypal, 
          LAST_VALUE(t.fecha) OVER (PARTITION BY u.id ORDER BY t.fecha RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS date_last_trx_tenpo_paypal,
          date_diff(current_date(),LAST_VALUE(DATE(t.fecha)) OVER (PARTITION BY u.id ORDER BY t.fecha RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),DAY) recency_tenpo_paypal,
        from {{ref("economics")}} t
          join users u on (t.user=u.id)
       where t.linea = "paypal"
          and fecha BETWEEN date_ob_tenpo AND date_trx_until
      )
    select 
      *, 
      trx_tenpo_paypal.id IS NOT NULL as  usuario_app
    from users
      left join trx_toups using (id)
      left join trx_paypal_topup using (id)
      left join trx_paypal_withdraw  using (id)
      left join trx_toups_post using (id)
      left join trx_paypal_topup_post using (id)
      left join trx_paypal_withdraw_post  using (id)
      left join trx_tenpo using (id)
      left join trx_tenpo_topup  using (id)
      left join trx_tenpo_mastercard  using (id)
      left join trx_tenpo_paypal using (id)
  )
