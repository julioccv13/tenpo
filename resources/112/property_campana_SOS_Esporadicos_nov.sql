DROP TABLE IF EXISTS `tenpo-bi.tmp.campana_SOS_Esporadicos_nov_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.campana_SOS_Esporadicos_nov_{{ds_nodash}}` AS (

select identity,esp_rec_1,esp_rec_2,esp_rec_url_1,esp_rec_url_2,esp_tope_cashback
from  `tenpo-sandbox.crm.campana_SOS_esporadicos_properties07112023_clevertap`
)
