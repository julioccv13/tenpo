DROP TABLE IF EXISTS `tenpo-bi.tmp.recopc_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.recopc_{{ds_nodash}}` AS 
(
  
with
    Clientes as 
    (
      select distinct id as user from `tenpo-bi-prod.users.users_tenpo`
      where status_onboarding = 'completo' 
    ),
    cuentas_agrup as 
    (
      select
      user,
      lower(case when utility_name in ('Autopase Autopista Central','Autopistas Unificadas','Autovía Santiago Lampa','Costanera Norte','Vespucio Norte','Vespucio Oriente','Vespucio Sur') then 'Autopistas' else  replace(utility_name,"Rut","") end) as utility_name,
      replace(FORMAT("%'d", cast(sum(amount) as integer)),",",".") as amount,
      count(1) as n_cuentas,
      max(execution_date) as fecha
      from `tenpo-airflow-prod.tenpo_utility_payment.reminders`
      group by 1,2
    ),
    cuentas_agrup_2 as (
      select *,
      ROW_NUMBER() OVER (PARTITION BY user ORDER BY amount DESC) AS rnk
      from cuentas_agrup
    ),
  bloqueados as 
  (
  SELECT id as user
  FROM tenpo-it-analytics.sac.lock_user
  JOIN tenpo-airflow-prod.users.users ON mail_lock_user = email
   )
    select
    z.user as identity,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() and a.utility_name is not null then 'visible' else 'no-visible' end m_cuenta11,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() then a.utility_name else 'N/A' end as cuenta11,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() then a.amount else 'N/A' end as monto11,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() then a.n_cuentas else -1 end as n11,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() and b.utility_name is not null then 'visible' else 'no-visible' end m_cuenta22,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() then b.utility_name else 'N/A' end cuenta22,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() then b.amount else 'N/A' end monto22,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() then b.n_cuentas else -1 end n22,   
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() and c.utility_name is not null then 'visible' else 'no-visible' end m_cuenta3,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() then c.utility_name else 'N/A' end cuenta3,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() then c.amount else 'N/A' end monto3,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() then c.n_cuentas else -1 end n3,   
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() and d.utility_name is not null then 'visible' else 'no-visible' end m_cuenta4,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() then d.utility_name else 'N/A' end cuenta4,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() then d.amount else 'N/A' end monto4,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() then d.n_cuentas else -1 end n4,   
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() and e.utility_name is not null then 'visible' else 'no-visible' end m_cuenta5,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() then e.utility_name else 'N/A' end cuenta5,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() then e.amount else 'N/A' end monto5,
    case when date(a.fecha) between date_sub(current_date(), INTERVAL 1 DAY) and current_date() then e.n_cuentas else -1 end n5
    from Clientes z 
    left join (select user,
                      utility_name,
                      amount,
                      n_cuentas,
                      fecha
                from cuentas_agrup_2
                where rnk = 1) a 
    on z.user = a.user
    left join (select
                      user,
                      utility_name,
                      amount,
                      n_cuentas,
                      fecha
                from cuentas_agrup_2
                where rnk = 2) b
    on z.user = b.user
    left join (select
                      user,
                      utility_name,
                      amount,
                      n_cuentas,
                      fecha
                from cuentas_agrup_2
                where rnk = 3) c
    on z.user = c.user
    left join (select
                      user,
                      utility_name,
                      amount,
                      n_cuentas,
                      fecha
                from cuentas_agrup_2
                where rnk = 4) d
    on z.user = d.user
    left join (select
                      user,
                      utility_name,
                      amount,
                      n_cuentas,
                      fecha
                from cuentas_agrup_2
                where rnk = 5) e
    on z.user = e.user
where z.user not in (select user from bloqueados)

)
