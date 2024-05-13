DROP TABLE IF EXISTS `${project_target}.crm.FGQ_Comercio_Recurrente`;
CREATE TABLE `${project_target}.crm.FGQ_Comercio_Recurrente` AS (

    WITH comercios as
    (
    select    
            eco.comercio_recod as NOM_Comercio,
            count(*) N_TRX,
            row_number() over (order by count(*) desc) ID
    from  `${project_source}.economics.economics`  as eco
            where eco.fecha >= date_add(CURRENT_DATE(),INTERVAL - 3 MONTH)
            and linea in ('mastercard',' mastercard_physical')
            and tipo_trx = 'Compra'
            and fecha >= date_add(CURRENT_DATE(),INTERVAL - 3 MONTH)
            and eco.comercio_recod not in ('paypal','hdi seguros','kushki','facebook','pagos flow','mercado pago sa','ebanx','onlyfans','webpay','pago online',
            'municipalidad','verificacion ks','binance','alps spa','otros marketplaces','pago facil','payu','pass line ltda','movired','recorrido','recarga pagoya 1',
            'tinder','soap bci seguros web')
            and eco.rubro_recod not in ('Pasarelas de pago','Transporte','Pago de Cuentas','Otro')
    group by
    eco.comercio_recod
    order by 2 desc
    )

    SELECT  NOM_Comercio,
            N_TRX,
            SUM(SUM(N_TRX)) OVER (ORDER BY ID ASC) AS Acumulado,
            (SUM(SUM(N_TRX)) OVER (ORDER BY ID ASC))/SUM(SUM(N_TRX)) OVER (ORDER BY 1 ASC) Porc_Acum 
    FROM comercios
    GROUP  BY NOM_Comercio,N_TRX,id
)
;

DROP TABLE IF EXISTS `${project_target}.crm.users_comercio_recurrente`;
CREATE TABLE `${project_target}.crm.users_comercio_recurrente` AS (

     SELECT 
        b.user,
        max(b.comercio_recod) cliente_comercio_3M,
        max(Porc_Acum) Porc 
    FROM `${project_target}.crm.FGQ_Comercio_Recurrente` a 
    JOIN `${project_source}.economics.economics` b on a.NOM_Comercio = b.comercio_recod
    WHERE Porc_Acum <= 0.8
    and fecha >= date_add(CURRENT_DATE(),INTERVAL -3 MONTH)
    group by b.user
    order by 3 desc

)
;


