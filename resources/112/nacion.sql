DROP TABLE IF EXISTS `${project_target}.tmp.nacion_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.nacion_{{ds_nodash}}` AS (
select 
        id_usuario as identity,
        case
              when nacionalidad = 'AFG' then 'afganistan'
              when nacionalidad = 'AFGANISTAN' then 'afganistan'
              when nacionalidad = 'ALBANIA' then 'albania'
              when nacionalidad = 'ALEMANIA' then 'alemania'
              when nacionalidad = 'ANDORRA' then 'andorra'
              when nacionalidad = 'ANGOLA' then 'angola'
              when nacionalidad = 'ANGUILA' then 'angola'
              when nacionalidad = 'ARABIA SAUDITA' then 'arabia saudita'
              when nacionalidad = 'ARG' then 'argentina'
              when nacionalidad = 'ARGENTINA' then 'argentina'
              when nacionalidad = 'ARGENTINO' then 'argentina'
              when nacionalidad = 'ARMENIA' then 'armenia'
              when nacionalidad = 'AUS' then 'australia'
              when nacionalidad = 'AUSTRALIA' then 'australia'
              when nacionalidad = 'AUSTRIA' then 'austria'
              when nacionalidad = 'Afganistan' then 'afganistan'
              when nacionalidad = 'Albania' then 'albania'
              when nacionalidad = 'Alemania' then 'alemania'
              when nacionalidad = 'Andorra' then 'andorra'
              when nacionalidad = 'Angola' then 'angola'
              when nacionalidad = 'Anguila' then 'angola'
              when nacionalidad = 'Antigua y Barbuda' then 'antigua y barbuda'
              when nacionalidad = 'Antillas Neerlandesas' then 'antillas neerlandesas'
              when nacionalidad = 'Antártida' then 'antartida'
              when nacionalidad = 'Arabia Saudita' then 'arabia saudita'
              when nacionalidad = 'Argel' then 'argelia'
              when nacionalidad = 'Argentina' then 'argentina'
              when nacionalidad = 'Armenia' then 'armenia'
              when nacionalidad = 'Aruba' then 'aruba'
              when nacionalidad = 'Australia' then 'australia'
              when nacionalidad = 'Austria' then 'austria'
              when nacionalidad = 'Azerbaiyán' then 'azerbaiyan'
              when nacionalidad = 'BGR' then 'bulgaria'
              when nacionalidad = 'BOL' then 'bolivia'
              when nacionalidad = 'BOLIVIA' then 'bolivia'
              when nacionalidad = 'BOLIVIANA' then 'bolivia'
              when nacionalidad = 'BOSNIA Y HERZEGOVINA' then 'bosnia y herzegovina'
              when nacionalidad = 'BRA' then 'brasil'
              when nacionalidad = 'BRASIL' then 'brasil'
              when nacionalidad = 'BRASILENA' then 'brasil'
              when nacionalidad = 'BRASILEÑA' then 'brasil'
              when nacionalidad = 'BURUNDI' then 'burundi'
              when nacionalidad = 'Bahamas' then 'bahamas'
              when nacionalidad = 'Bahr‚in' then 'bahrein'
              when nacionalidad = 'Bangladesh' then 'bangladesh'
              when nacionalidad = 'Barbados' then 'barbados'
              when nacionalidad = 'Belarús' then 'belarus'
              when nacionalidad = 'Belice' then 'belice'
              when nacionalidad = 'Benin' then 'benin'
              when nacionalidad = 'Bermudas' then 'bermudas'
              when nacionalidad = 'Bhut n' then 'bhutan'
              when nacionalidad = 'Bolivia' then 'bolivia'
              when nacionalidad = 'Bosnia y Herzegovina' then 'bosnia y herzegovina'
              when nacionalidad = 'Botsuana' then 'botsuana'
              when nacionalidad = 'Brasil' then 'brasil'
              when nacionalidad = 'Brun‚i' then 'brunei'
              when nacionalidad = 'Bulgaria' then 'bulgaria'
              when nacionalidad = 'Burkina Faso' then 'burkina faso'
              when nacionalidad = 'BÉLGICA' then 'belgica'
              when nacionalidad = 'Bélgica' then 'belgica'
              when nacionalidad = 'CABO VERDE' then 'cabo verde'
              when nacionalidad = 'CANADÁ'  then 'canada'
              when nacionalidad = 'CEILANESA' then 'ceilan'
              when nacionalidad = 'CHILE' then 'chile'
              when nacionalidad = 'CHILENA' then 'chile'
              when nacionalidad = 'CHILENO' then 'chile'
              when nacionalidad = 'CHINA' then 'china'
              when nacionalidad = 'CHL' then 'chile'
              when nacionalidad = 'CHN' then 'china'
              when nacionalidad = 'COL' then 'colombia'
              when nacionalidad = 'COLOMBIA' then 'colombia'
              when nacionalidad = 'COLOMBIANA' then 'colombia'
              when nacionalidad = 'COLOMBIANO' then 'colombia'
              when nacionalidad = 'COREA DEL SUR' then 'corea del sur'
              when nacionalidad = 'COSTA DE MARFIL' then 'costa de marfil'
              when nacionalidad = 'COSTA RICA' then 'costa rica'
              when nacionalidad = 'CRI' then 'costa rica'
              when nacionalidad = 'CROACIA' then 'croacia'
              when nacionalidad = 'CUB' then 'cuba'
              when nacionalidad = 'CUBA' then 'cuba'
              when nacionalidad = 'Cabo Verde' then 'cabo verde'
              when nacionalidad = 'Camboya' then 'camboya'
              when nacionalidad = 'Camerún' then 'camerun'
              when nacionalidad = 'Canadá'  then 'canada'
              when nacionalidad = 'Chad' then 'chad'
              when nacionalidad = 'Chile' then 'chile'
              when nacionalidad = 'China' then 'china'
              when nacionalidad = 'Chipre' then 'chipre'
              when nacionalidad = 'Ciudad del Vaticano' then 'ciudad del vaticano'
              when nacionalidad = 'Colombia' then 'colombia'
              when nacionalidad = 'Comoros' then 'comoros'
              when nacionalidad = 'Congo' then 'congo'
              when nacionalidad = 'Corea del Norte' then 'corea del norte'
              when nacionalidad = 'Corea del Sur' then 'corea del sur'
              when nacionalidad = 'Costa Rica' then 'costa rica'
              when nacionalidad = 'Costa de Marfil' then 'costa de marfil'
              when nacionalidad = 'Croacia' then 'croacia'
              when nacionalidad = 'Cuba' then 'cuba'
              when nacionalidad = 'DEU' then 'alemania'
              when nacionalidad = 'DOM' then 'republica dominicana'
              when nacionalidad = 'DOM¡NICA' then 'republica dominicana'
              when nacionalidad = 'Dinamarca' then 'dinamarca'
              when nacionalidad = 'Dom¡nica' then 'dominica'
              when nacionalidad = 'ECU' then 'ecuador'
              when nacionalidad = 'ECUADOR' then 'ecuador'
              when nacionalidad = 'ECUATORIANA' then 'ecuador'
              when nacionalidad = 'EGIPTO' then 'egipto'
              when nacionalidad = 'EL SALVADOR' then 'el salvador'
              when nacionalidad = 'ESAPAÑA' then 'españa'
              when nacionalidad = 'ESLOVAQUIA' then 'eslovaquia'
              when nacionalidad = 'ESP' then 'españa'
              when nacionalidad = 'ESPAÑA' then 'españa'
              when nacionalidad = 'ESTADOS UNIDOS' then 'estados unidos'
              when nacionalidad = 'ESTADOS UNIDOS DE AM‚RICA' then 'estados unidos'
              when nacionalidad = 'Ecuador' then 'ecuador'
              when nacionalidad = 'Egipto' then 'egipto'
              when nacionalidad = 'El Salvador' then 'el salvador'
              when nacionalidad = 'Emiratos Árabes Unidos' then 'emiratos arabes unidos'
              when nacionalidad = 'Eritrea' then 'eritrea'
              when nacionalidad = 'Eslovaquia' then 'eslovaquia'
              when nacionalidad = 'Eslovenia' then 'eslovenia'
              when nacionalidad = 'España' then 'españa'
              when nacionalidad = 'Estados Unidos de Am‚rica' then 'estados unidos'
              when nacionalidad = 'Estonia' then 'estonia'
              when nacionalidad = 'Etiopía' then 'etiopia'
              when nacionalidad = 'FILIPINAS' then 'filipinas'
              when nacionalidad = 'FINLANDIA' then 'finlandia'
              when nacionalidad = 'FRA' then 'francia'
              when nacionalidad = 'FRANCIA' then 'francia'
              when nacionalidad = 'Filipinas' then 'filipinas'
              when nacionalidad = 'Finlandia' then 'finlandia'
              when nacionalidad = 'Francia' then 'francia'
              when nacionalidad = 'GBR' then 'gran bretaña'
              when nacionalidad = 'GHA' then 'ghana'
              when nacionalidad = 'GHANA' then 'ghana'
              when nacionalidad = 'GRECIA' then 'grecia'
              when nacionalidad = 'GUATEMALA' then 'guatemala'
              when nacionalidad = 'GUY' then 'guyana'
              when nacionalidad = 'Ghana' then 'ghana'
              when nacionalidad = 'Gibraltar' then 'gibraltar'
              when nacionalidad = 'Grecia' then 'grecia'
              when nacionalidad = 'Groenlandia' then 'groenlandia'
              when nacionalidad = 'Guadalupe' then 'guadalupe'
              when nacionalidad = 'Guatemala' then 'guatemala'
              when nacionalidad = 'Guayana' then 'guayana'
              when nacionalidad = 'HAITI' then 'haiti'
              when nacionalidad = 'HAITIANA' then 'haiti'
              when nacionalidad = 'HAITÍ' then 'haiti'
              when nacionalidad = 'HND' then 'honduras'
              when nacionalidad = 'HONDURAS' then 'honduras'
              when nacionalidad = 'HTI' then 'haiti'
              when nacionalidad = 'HUNGRIA' then 'hungria'
              when nacionalidad = 'HUNGR¡A' then 'hungria'
              when nacionalidad = 'Haití' then 'haiti'
              when nacionalidad = 'Honduras' then 'honduras'
              when nacionalidad = 'Hong Kong' then 'hong kong'
              when nacionalidad = 'Hungr¡a' then 'hungria'
              when nacionalidad = 'IND' then 'india'
              when nacionalidad = 'INDIA' then 'india'
              when nacionalidad = 'INDONESA' then 'indonesia'
              when nacionalidad = 'IRLANDA' then 'irlanda'
              when nacionalidad = 'IRN' then 'irlanda'
              when nacionalidad = 'ISL' then 'israel'
              when nacionalidad = 'ISRAEL' then 'israel'
              when nacionalidad = 'ITA' then 'italia'
              when nacionalidad = 'ITALIA' then 'italia'
              when nacionalidad = 'India' then 'india'
              when nacionalidad = 'Indonesia' then 'indonesia'
              when nacionalidad = 'Irak' then 'irak'
              when nacionalidad = 'Irlanda' then 'irlanda'
              when nacionalidad = 'Irán' then 'iran'
              when nacionalidad = 'Isla de Man' then 'isla de man'
              when nacionalidad = 'Islandia' then 'islandia'
              when nacionalidad = 'Islas Turcas y Caicos' then 'islas turcas y caicos'
              when nacionalidad = 'Israel' then 'israel'
              when nacionalidad = 'Italia' then 'italia'
              when nacionalidad = 'JAMAICA' then 'jamaica'
              when nacionalidad = 'JAPÓN' then 'japon'
              when nacionalidad = 'JPN' then 'japon'
              when nacionalidad = 'Jamaica' then 'jamaica'
              when nacionalidad = 'Japón' then 'japon'
              when nacionalidad = 'Jordania' then 'jordania'
              when nacionalidad = 'KOR' then 'korea'
              when nacionalidad = 'Kazajstán' then 'kazajistan'
              when nacionalidad = 'LETONIA' then 'letonia'
              when nacionalidad = 'Líbano' then 'libano'
              when nacionalidad = 'MACEDONIA' then 'macedonia'
              when nacionalidad = 'MALASIA' then 'malasia'
              when nacionalidad = 'MARRUECOS' then 'marruecos'
              when nacionalidad = 'MEX' then 'mexico'
              when nacionalidad = 'MEXICO' then 'mexico'
              when nacionalidad = 'MOZAMBIQUE' then 'mozambique'
              when nacionalidad = 'Madagascar' then 'madagascar'
              when nacionalidad = 'Malasia' then 'malasia'
              when nacionalidad = 'Marruecos' then 'marruecos'
              when nacionalidad = 'Moldova' then 'moldova'
              when nacionalidad = 'Mongolia' then 'mongolia'
              when nacionalidad = 'Mozambique' then 'mozambique'
              when nacionalidad = 'MÉXICO' then 'mexico'
              when nacionalidad = 'México' then 'mexico'
              when nacionalidad = 'NEPAL' then 'nepal'
              when nacionalidad = 'NICARAGUA' then 'nicaragua'
              when nacionalidad = 'NIGERIA' then 'nigeria'
              when nacionalidad = 'NLD' then 'paises bajos'
              when nacionalidad = 'NORUEGA' then 'noruega'
              when nacionalidad = 'Nepal' then 'nepal'
              when nacionalidad = 'Nicaragua' then 'nicaragua'
              when nacionalidad = 'Nigeria' then 'nigeria'
              when nacionalidad = 'Noruega' then 'noruega'
              when nacionalidad = 'Nueva Zelanda' then 'nueva zelanda'
              when nacionalidad = 'PAK' then 'pakistan'
              when nacionalidad = 'PAKISTÁN' then 'pakistan'
              when nacionalidad = 'PALESTINA' then 'palestina'
              when nacionalidad = 'PAN' then 'panama'
              when nacionalidad = 'PANAM'  then 'panama'
              when nacionalidad = 'PARAGUAY' then 'paraguay'
              when nacionalidad = 'PARAGUAYA' then 'paraguay'
              when nacionalidad = 'PAÍSES BAJOS' then 'paises bajos'
              when nacionalidad = 'PER' then 'peru'
              when nacionalidad = 'PERU' then 'peru'
              when nacionalidad = 'PERUANA' then 'peru'
              when nacionalidad = 'PERUANO' then 'peru'
              when nacionalidad = 'PERÚ' then 'peru'
              when nacionalidad = 'PHL' then 'filipinas'
              when nacionalidad = 'POLONIA' then 'polonia'
              when nacionalidad = 'PORTUGAL' then 'portugal'
              when nacionalidad = 'PRT' then 'portugal'
              when nacionalidad = 'PRY' then 'paraguay'
              when nacionalidad = 'PUERTO RICO' then 'puerto rico'
              when nacionalidad = 'Pakistán' then 'pakistan'
              when nacionalidad = 'Palestina' then 'palestina'
              when nacionalidad = 'Panam'  then 'panama'
              when nacionalidad = 'Paraguay' then 'paraguay'
              when nacionalidad = 'Países Bajos' then 'paises bajos'
              when nacionalidad = 'Perú' then 'peru'
              when nacionalidad = 'Polinesia Francesa' then 'polinesia francesa'
              when nacionalidad = 'Polonia' then 'polonia'
              when nacionalidad = 'Portugal' then 'portugal'
              when nacionalidad = 'Puerto Rico' then 'puerto rico'
              when nacionalidad = 'REINO UNIDO' then 'reino unido'
              when nacionalidad = 'REPUBLICA DOMINICANA' then 'republica dominicana'
              when nacionalidad = 'REPÚBLICA CENTRO-AFRICANA' then 'republica centro-africana'
              when nacionalidad = 'REPÚBLICA CHECA' then 'republica checa'
              when nacionalidad = 'REPÚBLICA DOMINICANA' then 'republica dominicana'
              when nacionalidad = 'ROU' then 'rumania'
              when nacionalidad = 'RUMANÍA' then 'rumania'
              when nacionalidad = 'RUS' then 'rusia'
              when nacionalidad = 'RUSIA' then 'rusia'
              when nacionalidad = 'Reino Unido' then 'reino unido'
              when nacionalidad = 'República Checa' then 'republica checa'
              when nacionalidad = 'República Dominicana' then 'republica dominicana'
              when nacionalidad = 'Rumanía' then 'rumania'
              when nacionalidad = 'Rusia' then 'rusia'
              when nacionalidad = 'SAN CRISTÓBAL Y NIEVES' then 'san cristobal y nieves'
              when nacionalidad = 'SENEGAL' then 'senegal'
              when nacionalidad = 'SERBIA Y MONTENEGRO' then 'serbia y montenegro'
              when nacionalidad = 'SINGAPUR' then 'singapur'
              when nacionalidad = 'SIRIA' then 'siria'
              when nacionalidad = 'SLV' then 'eslovenia'
              when nacionalidad = 'SUDÁFRICA' then 'sudafrica'
              when nacionalidad = 'SUECIA' then 'suecia'
              when nacionalidad = 'SUIZA' then 'suiza'
              when nacionalidad = 'Sahara Occidental' then 'sahara occidental'
              when nacionalidad = 'San Marino' then 'san marino'
              when nacionalidad = 'San Pedro y Miquelón' then 'san pedro y miquelon'
              when nacionalidad = 'Serbia y Montenegro' then 'serbia y montenegro'
              when nacionalidad = 'Seychelles' then 'seychelles'
              when nacionalidad = 'Sierra Leona' then 'sierra leona'
              when nacionalidad = 'Singapur' then 'singapur'
              when nacionalidad = 'Siria' then 'siria'
              when nacionalidad = 'Sudáfrica' then 'sudafrica'
              when nacionalidad = 'Suecia' then 'suecia'
              when nacionalidad = 'Suiza' then 'suiza'
              when nacionalidad = 'TAIW N' then 'taiwan'
              when nacionalidad = 'TUR' then 'turquia'
              when nacionalidad = 'TURQUÍA' then 'turquia'
              when nacionalidad = 'Tailandia' then 'tailandia'
              when nacionalidad = 'Taiw n' then 'taiwan'
              when nacionalidad = 'Togo' then 'togo'
              when nacionalidad = 'Trinidad y Tobago' then 'trinidad y tobago'
              when nacionalidad = 'Turquía' then 'turquia'
              when nacionalidad = 'Tuvalu' then 'tuvalu'
              when nacionalidad = 'TÚNEZ' then 'tunez'
              when nacionalidad = 'UCRANIA' then 'ucrania'
              when nacionalidad = 'UKR' then 'ucrania'
              when nacionalidad = 'URUGUAY' then 'uruguay'
              when nacionalidad = 'URY' then 'uruguay'
              when nacionalidad = 'USA' then 'estados unidos'
              when nacionalidad = 'UZBEKISTÁN' then 'uzbekistan'
              when nacionalidad = 'Ucrania' then 'ucrania'
              when nacionalidad = 'Uruguay' then 'uruguay'
              when nacionalidad = 'Uzbekistán' then 'uzbekistan'
              when nacionalidad = 'VEN' then 'venezuela'
              when nacionalidad = 'VENEZOLANA' then 'venezuela'
              when nacionalidad = 'VENEZUELA' then 'venezuela'
              when nacionalidad = 'VIETNAM' then 'vietnam'
              when nacionalidad = 'Vanuatu' then 'vanuatu'
              when nacionalidad = 'Venezuela' then 'venezuela'
              when nacionalidad = 'Vietnam' then 'vietnam'
              when nacionalidad = 'YEMEN' then 'yemen'
              when nacionalidad = 'Yemen' then 'yemen'
              when nacionalidad = 'Yibuti' then 'yibuti'
              else 'otro' end as nacion
from `${project_source_1}.users.demographics`
);
