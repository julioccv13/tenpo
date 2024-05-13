SELECT distinct 
  id,
  md5(mail) as mail, 
  md5(mail_cuenta) as mail_cuenta,
  saldo,
  filename,
  date,
  lag(id) over (partition by id order by date) is null as nuevo,
  lag(saldo) over (partition by id order by date) as saldo_anterior,
  lag(date) over (partition by id order by date) as ultima_revision,
  saldo-lag(saldo) over (partition by id order by date) as diferencia_saldo,
  case 
    when lag(saldo) over (partition by id order by date) is null then 'Nuevo'
    when lag(saldo) over (partition by id order by date) = saldo then 'Mantiene saldo'
    when lag(saldo) over (partition by id order by date) < saldo then 'Aumenta saldo'
    when lag(saldo) over (partition by id order by date) > saldo then 'Disminuye saldo'
    end as status
FROM (select distinct * from {{source('aux_paypal','aux_saldos')}})
order by id, date