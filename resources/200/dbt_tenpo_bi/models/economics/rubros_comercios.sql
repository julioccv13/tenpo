{{ config(materialized='table') }}

with trxs as (
    SELECT 
    DISTINCT
    linea
    ,comercio
    ,lower(comercio) as comercio_lower
    FROM {{ ref('mastercard_physical') }} -- fisica y virtual

    UNION ALL


    SELECT 
    DISTINCT
    linea
    ,comercio
    ,lower(comercio) as comercio_lower
    FROM {{ ref('mastercard_virtual') }} -- fisica y virtual

    UNION ALL

    SELECT 
    DISTINCT
    linea
    ,comercio
    ,lower(comercio) as comercio_lower
    FROM {{ ref('utility_payments') }}

    UNION ALL

    SELECT 
    DISTINCT
    linea
    ,comercio
    ,lower(comercio) as comercio_lower
    FROM {{ ref('topups') }}
), comercios as (
        SELECT 
          linea,
          comercio,
        CASE
          -----PasarelasTOP>>>>Son un rubro por si solo-----
          WHEN REGEXP_CONTAINS(lower(comercio), r'mercado ?pago s a')  then 'mercado pago sa'
          WHEN (REGEXP_CONTAINS(lower(comercio), r'mercado ?pago|mercadopago') and not REGEXP_CONTAINS(lower(comercio), r'mercado ?pago s a') )  then 'mercado pago'
          WHEN REGEXP_CONTAINS(lower(comercio), r'paypal') then 'paypal'
          ----Marketplaces---
          WHEN REGEXP_CONTAINS(lower(comercio), r'aliexpress') then 'aliexpress'
          WHEN REGEXP_CONTAINS(lower(comercio), r'ebay') then 'ebay'
          WHEN (REGEXP_CONTAINS(lower(comercio), r'amazon') or REGEXP_CONTAINS(lower(comercio), r'amzn')) and not REGEXP_CONTAINS(lower(comercio), r'amazon ?prime') then 'amazon'
          WHEN REGEXP_CONTAINS(lower(comercio), r'shein')  and not REGEXP_CONTAINS(lower(comercio), r'paypal') then 'shein'
          WHEN REGEXP_CONTAINS(lower(comercio), r'linio')  then 'linio'
          WHEN REGEXP_CONTAINS(lower(comercio), r'wish')  then 'wish'
          WHEN REGEXP_CONTAINS(lower(comercio), r'alibaba') then 'alibaba'
          WHEN REGEXP_CONTAINS(lower(comercio), r'buscalibre ?com') then 'buscalibre'
          WHEN REGEXP_CONTAINS(lower(comercio), r'bookdepository') then 'bookdepository'
          WHEN REGEXP_CONTAINS(lower(comercio), r'cuponatic') then 'cuponatic'
          WHEN REGEXP_CONTAINS(lower(comercio), r'banggood') then 'banggood'
          WHEN REGEXP_CONTAINS(lower(comercio), r'nike') then 'nike'
          WHEN REGEXP_CONTAINS(lower(comercio), r'aeropost|doite|weplay|block|fashions park|corona|a3d|tiendapet|under ?armour|herbalife|colloky|tienda ?flores|comercial ccu|cerveceria chile|la barra|lippioutdoor|kayser|bepo spa|autoplanet|winpy|libreria|tennis express|lib antartica')  then 'otros marketplaces'
          ----Supermercados---   
          WHEN (REGEXP_CONTAINS(lower(comercio), r'jumbo') and not REGEXP_CONTAINS(lower(comercio), r'rappi')) then 'jumbo'
          WHEN (REGEXP_CONTAINS(lower(comercio), r'lider') and not REGEXP_CONTAINS(lower(comercio), r'rappi')) then 'lider'
          WHEN (REGEXP_CONTAINS(lower(comercio), r'tottus') and not REGEXP_CONTAINS(lower(comercio), r'rappi')) then 'tottus'
          WHEN (REGEXP_CONTAINS(lower(comercio), r'unimarc') and not REGEXP_CONTAINS(lower(comercio), r'rappi')) then 'unimarc'
          WHEN (REGEXP_CONTAINS(lower(comercio), r'ok market') and not REGEXP_CONTAINS(lower(comercio), r'rappi')) then 'ok market'
          WHEN (REGEXP_CONTAINS(lower(comercio), r'mayorista 10') and not REGEXP_CONTAINS(lower(comercio), r'rappi')) then 'mayorista 10'
          WHEN (REGEXP_CONTAINS(lower(comercio), r'supermercado') and not REGEXP_CONTAINS(lower(comercio), r'rappi')) then 'otros supermercados'
          --------D&M--------   
          WHEN REGEXP_CONTAINS(lower(comercio), r'rappi') then 'rappi'
          WHEN REGEXP_CONTAINS(lower(comercio), r'lim(\*+) ?retencion|uber ?lime|lim(\*+) ?cost|lim(\*+) ?temp') then 'lime'
          WHEN (REGEXP_CONTAINS(lower(comercio), r'uber(?:br)?') and not REGEXP_CONTAINS(lower(comercio), r'eats|cornershop'))  then 'uber'
          WHEN REGEXP_CONTAINS(lower(comercio), r'uber') and REGEXP_CONTAINS(lower(comercio), r'eats')  then 'uber eats'
          WHEN REGEXP_CONTAINS(lower(comercio), r'cornershop')  then 'cornershop'
          WHEN REGEXP_CONTAINS(lower(comercio), r'pedidosya')  then 'pedidos ya'
          WHEN REGEXP_CONTAINS(lower(comercio), r'beat') then 'beat'
          WHEN REGEXP_CONTAINS(lower(comercio), r'dominos pizza') then 'dominos pizza'
          WHEN REGEXP_CONTAINS(lower(comercio), r'cabify') then 'cabify'
          WHEN REGEXP_CONTAINS(lower(comercio), r'pizza') then 'pizza'
          WHEN REGEXP_CONTAINS(lower(comercio), r'correos de chile|dhl|starken|zenmarket') then 'empresas de despachos'
          --------TxD--------
          WHEN REGEXP_CONTAINS(lower(comercio), r'(?:falabella|fpay)')  then 'falabella'
          WHEN REGEXP_CONTAINS(lower(comercio), r'bestbuy') then 'bestbuy'
          WHEN REGEXP_CONTAINS(lower(comercio), r'(?:paris)') and not REGEXP_CONTAINS(lower(comercio), r'(?:parish|parisian|hello)')  then 'paris'
          WHEN REGEXP_CONTAINS(lower(comercio), r'cencosud') and not REGEXP_CONTAINS(lower(comercio), r'scotiaba')  then 'cencosud'
          WHEN REGEXP_CONTAINS(lower(comercio), r'skechers')  then 'skechers'
          WHEN REGEXP_CONTAINS(lower(comercio), r'hites')  then 'hites'
          WHEN REGEXP_CONTAINS(lower(comercio), r'ripley')  then 'ripley'
          WHEN REGEXP_CONTAINS(lower(comercio), r'abcdin')  then 'abcdin'
          WHEN REGEXP_CONTAINS(lower(comercio), r'decathlon')  then 'decathlon'
          WHEN REGEXP_CONTAINS(lower(comercio), r'pc factory')  then 'pc factory'
          WHEN REGEXP_CONTAINS(lower(comercio), r'la polar')  then 'la polar'
          WHEN REGEXP_CONTAINS(lower(comercio), r'paris internet')  then 'paris'
          WHEN REGEXP_CONTAINS(lower(comercio), r'macys')  then 'macys'
          
          --MejoramientoHogar-
          WHEN REGEXP_CONTAINS(lower(comercio), r'sodimac')  then 'sodimac'
          WHEN REGEXP_CONTAINS(lower(comercio), r'easy internet')  then 'easy'
          WHEN REGEXP_CONTAINS(lower(comercio), r'casaideas|casa ideas')  then 'casa ideas'
          WHEN REGEXP_CONTAINS(lower(comercio), r'cannon(?:br)?')  then 'cannon home' 
          
          ---Suscripciones----
          WHEN REGEXP_CONTAINS(lower(comercio), r'amazon ?prime')  then 'amazon prime'
          WHEN REGEXP_CONTAINS(lower(comercio), r'youtube') then 'youtube'
          WHEN (REGEXP_CONTAINS(lower(comercio), r'onlyfans') or lower(comercio) like '%of     %' or REGEXP_CONTAINS(lower(comercio), r'only fans') or  lower(comercio) = 'of') and not REGEXP_CONTAINS(lower(comercio), r'paypal') then 'onlyfans'
          WHEN REGEXP_CONTAINS(lower(comercio), r'google ?play')  then 'google play'
          WHEN REGEXP_CONTAINS(lower(comercio), r'netflix')  then 'netflix'
          WHEN REGEXP_CONTAINS(lower(comercio), r'facebook|facebk')  then 'facebook'
          WHEN REGEXP_CONTAINS(lower(comercio), r'apple')  and not REGEXP_CONTAINS(lower(comercio), r'applexa') and not REGEXP_CONTAINS(lower(comercio), r'apple unlock') and not 
           REGEXP_CONTAINS(lower(comercio), r'appleseed') and not      REGEXP_CONTAINS(lower(comercio), r'apple unlock') and not REGEXP_CONTAINS(lower(comercio), r'paypal') then 'apple'
          WHEN REGEXP_CONTAINS(lower(comercio), r'spotify')  and not REGEXP_CONTAINS(lower(comercio), r'paypal') then 'spotify'
          WHEN REGEXP_CONTAINS(lower(comercio), r'badoo') then 'badoo'
          WHEN REGEXP_CONTAINS(lower(comercio), r'tangome|tango.me') then 'tango'
          WHEN REGEXP_CONTAINS(lower(comercio), r'deezer') then 'deezer'
          WHEN REGEXP_CONTAINS(lower(comercio), r'hbo') then 'hbo'
          WHEN REGEXP_CONTAINS(lower(comercio), r'patreon') then 'patreon'
          WHEN REGEXP_CONTAINS(lower(comercio), r'zappingtv') then 'zapping tv'
          WHEN REGEXP_CONTAINS(lower(comercio), r'disney ?plus|disney ?latam|disney') then 'disney plus'
          WHEN REGEXP_CONTAINS(lower(comercio), r'huawei ?mobile|huawei ?services') then 'huawei'
          WHEN REGEXP_CONTAINS(lower(comercio), r'temporary hold')  then 'verificacion google'
          WHEN REGEXP_CONTAINS(lower(comercio), r'prime video')  then 'prime video'
          WHEN REGEXP_CONTAINS(lower(comercio), r'google storage')  then 'google storage'
          WHEN REGEXP_CONTAINS(lower(comercio), r'tinder')  then 'tinder'
          WHEN REGEXP_CONTAINS(lower(comercio), r'tiktok')  then 'tiktok'
          WHEN REGEXP_CONTAINS(lower(comercio), r'tidal')  then 'tidal'
          ------Gamers--------
          WHEN REGEXP_CONTAINS(lower(comercio), r'niantic')  and not REGEXP_CONTAINS(lower(comercio), r'paypal') then 'niantic'
          WHEN REGEXP_CONTAINS(lower(comercio), r'playstation')  and not REGEXP_CONTAINS(lower(comercio), r'paypal') then 'playstation'
          WHEN REGEXP_CONTAINS(lower(comercio), r'steam')  and not REGEXP_CONTAINS(lower(comercio), r'paypal') then 'steam'
          WHEN REGEXP_CONTAINS(lower(comercio), r'riot')  and not REGEXP_CONTAINS(lower(comercio), r'paypal') then 'riot'
          WHEN REGEXP_CONTAINS(lower(comercio), r'nintendo')  and not REGEXP_CONTAINS(lower(comercio), r'paypal') then 'nintendo'
          WHEN REGEXP_CONTAINS(lower(comercio), r'xbox') then 'xbox'
          WHEN REGEXP_CONTAINS(lower(comercio), r'mihoyo') and not REGEXP_CONTAINS(lower(comercio), r'paypal') then 'mihoyo limited'
          WHEN REGEXP_CONTAINS(lower(comercio), r'microsoft') then 'microsoft'
          WHEN REGEXP_CONTAINS(lower(comercio), r'playdemic') then 'playdemic'
          WHEN REGEXP_CONTAINS(lower(comercio), r'playtika') then 'playtika'
          WHEN REGEXP_CONTAINS(lower(comercio), r'electraworks') then 'electraworks'
          WHEN REGEXP_CONTAINS(lower(comercio), r'proxima beta') then 'proxima beta'
          WHEN REGEXP_CONTAINS(lower(comercio), r'moon active') then 'moon active'
          WHEN REGEXP_CONTAINS(lower(comercio), r'garena') then 'garena'
          WHEN REGEXP_CONTAINS(lower(comercio), r'mobilegame') then 'mobilegame'
          WHEN REGEXP_CONTAINS(lower(comercio), r'garena') then 'garena'
          WHEN REGEXP_CONTAINS(lower(comercio), r'roblox') then 'roblox'
          WHEN REGEXP_CONTAINS(lower(comercio), r'robtop') then 'robtop'
          WHEN REGEXP_CONTAINS(lower(comercio), r'webzen') then 'webzen'
          WHEN REGEXP_CONTAINS(lower(comercio), r'minecraft') then 'minecraft'
          WHEN REGEXP_CONTAINS(lower(comercio), r'klab inc') then 'klab inc'
          WHEN REGEXP_CONTAINS(lower(comercio), r'fortnite') then 'fortnite'
          WHEN REGEXP_CONTAINS(lower(comercio), r'glu games') then 'glu games'
          WHEN REGEXP_CONTAINS(lower(comercio), r'phantom') then 'phantom'
          WHEN REGEXP_CONTAINS(lower(comercio), r'agaming') then 'agaming'
          WHEN REGEXP_CONTAINS(lower(comercio), r'kingsgroup') then 'kingsgroup'
          WHEN REGEXP_CONTAINS(lower(comercio), r'playrix') then 'playrix'
          WHEN REGEXP_CONTAINS(lower(comercio), r'smart project') then 'smart project'
          WHEN REGEXP_CONTAINS(lower(comercio), r'buffalostudios') then 'buffalo studios'
          WHEN REGEXP_CONTAINS(lower(comercio), r'blizzard entertainment') then 'blizzard entertainment'
          WHEN REGEXP_CONTAINS(lower(comercio), r'activision') then 'activision'
          WHEN REGEXP_CONTAINS(lower(comercio), r'supercell')  then 'supercell'
          WHEN REGEXP_CONTAINS(lower(comercio), r'fun games')  then 'fun games'
          WHEN REGEXP_CONTAINS(lower(comercio), r'twitch')  then 'twitch'
          WHEN REGEXP_CONTAINS(lower(comercio), r'google ?king|google king')  then 'google king'
          WHEN REGEXP_CONTAINS(lower(comercio), r'google ?games|ea mobile|small giant| kefirgames|top games|gameloft|origin.com|yoozoo|netmarble|lilith|konami|diandian|dreamplus|camel games')  then 'google games'
          WHEN REGEXP_CONTAINS(lower(comercio), r'gtarcade|moon active|todo juegos|tibia|full codigos|gog|divina technology')  then 'otros gamer'
          WHEN REGEXP_CONTAINS(lower(comercio), r'epic games') then 'epic games'
          WHEN REGEXP_CONTAINS(lower(comercio), r'chilecodes') then 'chilecodes'

          ----PagoCuentas----
          WHEN REGEXP_CONTAINS(lower(comercio), r'lipigas') then 'lipigas'
          WHEN REGEXP_CONTAINS(lower(comercio), r'entel') then 'entel'
          WHEN REGEXP_CONTAINS(lower(comercio), r'vtr') then 'vtr'
          WHEN REGEXP_CONTAINS(lower(comercio), r'abastible') then 'abastible'
          WHEN REGEXP_CONTAINS(lower(comercio), r'gasco') then 'gasco'
          WHEN REGEXP_CONTAINS(lower(comercio), r'aguas') then 'aguas'
          WHEN REGEXP_CONTAINS(lower(comercio), r'gce') then 'gce'
          WHEN REGEXP_CONTAINS(lower(comercio), r'enel') then 'enel'
          WHEN REGEXP_CONTAINS(lower(comercio), r'gtd') then 'gtd'
          WHEN REGEXP_CONTAINS(lower(comercio), r'autopistas') then 'autopistas'
          WHEN REGEXP_CONTAINS(lower(comercio), r'chilquinta') then 'chilquinta'
          WHEN REGEXP_CONTAINS(lower(comercio), r'claro') then 'claro'
          WHEN REGEXP_CONTAINS(lower(comercio), r'virgin') then 'virgin'
          WHEN REGEXP_CONTAINS(lower(comercio), r'movistar') then 'movistar'
          WHEN REGEXP_CONTAINS(lower(comercio), r'sencillito') then 'sencillito'
          WHEN REGEXP_CONTAINS(lower(comercio), r'duoc') then 'duoc'
          WHEN REGEXP_CONTAINS(lower(comercio), r'cementerio') then 'cementerio'
          WHEN REGEXP_CONTAINS(lower(comercio), r'directv') then 'directv'
          WHEN REGEXP_CONTAINS(lower(comercio), r'telefonica') then 'telefonica'
          WHEN REGEXP_CONTAINS(lower(comercio), r'mundo pacifico') then 'mundo pacifico'
          WHEN REGEXP_CONTAINS(lower(comercio), r'esval') then 'esval'
          WHEN REGEXP_CONTAINS(lower(comercio), r'womboton01') then 'wom'


          ---ServiciosTecnologicos--
          
          WHEN REGEXP_CONTAINS(lower(comercio), r'zoom.us') then 'zoom.us'
          WHEN REGEXP_CONTAINS(lower(comercio), r'jumpseller') then 'jumpseller'
          WHEN REGEXP_CONTAINS(lower(comercio), r'shopify') then 'shopify'
          WHEN REGEXP_CONTAINS(lower(comercio), r'skype.com') then 'skype'
          WHEN REGEXP_CONTAINS(lower(comercio), r'facebk ads') then 'facebook ads'
          WHEN REGEXP_CONTAINS(lower(comercio), r'bigo') then 'bigo'
          WHEN REGEXP_CONTAINS(lower(comercio), r'samsung electronic') then 'samsung electronic'
          WHEN REGEXP_CONTAINS(lower(comercio), r'godaddy') then 'godaddy'
          WHEN REGEXP_CONTAINS(lower(comercio), r'etoro') then 'etoro'
          WHEN REGEXP_CONTAINS(lower(comercio), r'binance') then 'binance'
          WHEN REGEXP_CONTAINS(lower(comercio), r'orion services spa|orionx') then 'orionx'
          WHEN REGEXP_CONTAINS(lower(comercio), r'moonpay') then 'moonpay'
          WHEN REGEXP_CONTAINS(lower(comercio), r'ex(\*+) ?brokeriq') then 'brokeriq'
          WHEN REGEXP_CONTAINS(lower(comercio), r'revolut') then 'revolut'
          WHEN REGEXP_CONTAINS(lower(comercio), r'chile ?nic') then 'nic chile'
          WHEN REGEXP_CONTAINS(lower(comercio), r'buy digital asset') then 'buy digital asset'
          WHEN REGEXP_CONTAINS(lower(comercio), r'iqoption.com|iqoption') then 'iqoption'
          WHEN REGEXP_CONTAINS(lower(comercio), r'libertex.org|crypterium|bitcoinforme|xmglobal|roboforex|ic markets|investmarkets.com|uphold|ava fx|cex.io') then 'otros cryptos'
          
          --Telecomunicaciones-
          WHEN REGEXP_CONTAINS(lower(comercio), r'haititalk') then 'haititalk'
          WHEN REGEXP_CONTAINS(lower(comercio), r'talk360') then 'talk360'
          WHEN REGEXP_CONTAINS(lower(comercio), r'recargas web|recargas online') then 'recargas web'
          WHEN REGEXP_CONTAINS(lower(comercio), r'que facil') then 'que facil'
          WHEN REGEXP_CONTAINS(lower(comercio), r'hablax|orange madagascar|ding') then 'otros telecomunicaciones'

          --Remesas--
          WHEN REGEXP_CONTAINS(lower(comercio), r'transferwise|wise') then 'transferwise'
          WHEN REGEXP_CONTAINS(lower(comercio), r'worldremit') then 'worldremit'
          WHEN REGEXP_CONTAINS(lower(comercio), r'sendvalu') then 'sendvalu'
          WHEN REGEXP_CONTAINS(lower(comercio), r'moneygram') then 'moneygram'
          WHEN REGEXP_CONTAINS(lower(comercio), r'conotoxia ') then 'conotoxia'
          WHEN REGEXP_CONTAINS(lower(comercio), r'monese') then 'monese'
          WHEN REGEXP_CONTAINS(lower(comercio), r'titanes money transfer') then 'titanes money transfer'
          WHEN REGEXP_CONTAINS(lower(comercio), r'tropipay') then 'tropipay'
          WHEN REGEXP_CONTAINS(lower(comercio), r'zinli') then 'zinli'
          WHEN REGEXP_CONTAINS(lower(comercio), r'transpaygo') then 'transpaygo'
          WHEN REGEXP_CONTAINS(lower(comercio), r'remesas') then 'remesas'
          WHEN REGEXP_CONTAINS(lower(comercio), r'global81|global ?66') then 'global66'

          ------JuegosAzar-----
          WHEN REGEXP_CONTAINS(lower(comercio), r'polla chilena') then 'polla chilena'
          WHEN REGEXP_CONTAINS(lower(comercio), r'betway') then 'betway'
          WHEN REGEXP_CONTAINS(lower(comercio), r'loteria') then 'loteria'
          WHEN REGEXP_CONTAINS(lower(comercio), r'bet365') then 'bet365'
          WHEN REGEXP_CONTAINS(lower(comercio), r'teletrak') then 'teletrak'
          WHEN REGEXP_CONTAINS(lower(comercio), r'thelotter') then 'the lotter'
          ---FarmaciasSalud----
          WHEN REGEXP_CONTAINS(lower(comercio), r'salcobrand') then 'salcobrand'
          WHEN REGEXP_CONTAINS(lower(comercio), r'farmacias') then 'farmacias'
          WHEN REGEXP_CONTAINS(lower(comercio), r'banmedica') then 'banmedica'
          WHEN REGEXP_CONTAINS(lower(comercio), r'cruz blanca') then 'cruz blanca'
          WHEN REGEXP_CONTAINS(lower(comercio), r'preunic|pre unic') then 'preunic'
          WHEN REGEXP_CONTAINS(lower(comercio), r'dbs') then 'dbs'
          WHEN REGEXP_CONTAINS(lower(comercio), r'clinica') then 'clinicas'
          WHEN REGEXP_CONTAINS(lower(comercio), r'nu skin') then 'otros salud y belleza'
          --CombustibleTransporte--
          WHEN REGEXP_CONTAINS(lower(comercio), r'copec') then 'copec'
          WHEN REGEXP_CONTAINS(lower(comercio), r'shell') then 'shell'
          WHEN REGEXP_CONTAINS(lower(comercio), r'petrobras') then 'petrobras'
          WHEN REGEXP_CONTAINS(lower(comercio), r'shell 1click mi copilo') then 'shell mi copiloto'
          WHEN REGEXP_CONTAINS(lower(comercio), r'metro s a') then 'metro sa'
          WHEN REGEXP_CONTAINS(lower(comercio), r'bip ?adulto mayor|carga bip') then 'carga bip!'
          WHEN REGEXP_CONTAINS(lower(comercio), r'tur bus') then 'tur bus'
          WHEN REGEXP_CONTAINS(lower(comercio), r'pasaje bus|pasajebus') then 'pasaje bus'
          WHEN REGEXP_CONTAINS(lower(comercio), r'recorrido') then 'recorrido'
          --------Viajes-------
          WHEN REGEXP_CONTAINS(lower(comercio), r'sky airlines|skyairlines') then 'sky airlines'
          WHEN REGEXP_CONTAINS(lower(comercio), r'american airlines') then 'american airlines'
          WHEN REGEXP_CONTAINS(lower(comercio), r'vueling airlines') then 'vueling airlines'
          WHEN REGEXP_CONTAINS(lower(comercio), r'turkish airlines') then 'turkish airlines'
          WHEN REGEXP_CONTAINS(lower(comercio), r'viva airlines') then 'viva airlines'
          WHEN REGEXP_CONTAINS(lower(comercio), r'united airlines') then 'united airlines'
          WHEN REGEXP_CONTAINS(lower(comercio), r'latam ?com|latam.com') then 'latam airlines'
          WHEN REGEXP_CONTAINS(lower(comercio), r'jetsmart') then 'jetsmart'
          WHEN REGEXP_CONTAINS(lower(comercio), r'edestinos') then 'edestinos'
          WHEN REGEXP_CONTAINS(lower(comercio), r'despegar') then 'despegar'
          WHEN REGEXP_CONTAINS(lower(comercio), r'vueloferta') then 'vueloferta'
          WHEN (REGEXP_CONTAINS(lower(comercio), r'boletodevuelo') and  REGEXP_CONTAINS(lower(comercio), r'openpay')) then 'openpay boletodevuelo'
          WHEN REGEXP_CONTAINS(lower(comercio), r'viajemos') then 'viajemos'
          WHEN REGEXP_CONTAINS(lower(comercio), r'viajes lina|viajes mara') then 'otras agencias de viaje'
          WHEN REGEXP_CONTAINS(lower(comercio), r'airbnb')  then 'airbnb'
          WHEN REGEXP_CONTAINS(lower(comercio), r'flightnetw')  then 'flight network'
          WHEN REGEXP_CONTAINS(lower(comercio), r'airlines')  then 'otras aerolineas'
          --TrámitesMunicipalesGobierno--
          WHEN REGEXP_CONTAINS(lower(comercio), r'municipalidad|municip|muni de|munic de|munic\.') then 'municipalidad'
          WHEN REGEXP_CONTAINS(lower(comercio), r'notario') then 'notaría'
          --Educación--
          WHEN REGEXP_CONTAINS(lower(comercio), r'open education')  then 'open english'
          WHEN REGEXP_CONTAINS(lower(comercio), r'udemy') then 'udemy'
          WHEN REGEXP_CONTAINS(lower(comercio), r'domestika') then 'domestika'
          WHEN REGEXP_CONTAINS(lower(comercio), r'universidad de universidad|u de |udd') then 'universidades'
          WHEN REGEXP_CONTAINS(lower(comercio), r'insitute|instituto') then 'institutos'
          WHEN REGEXP_CONTAINS(lower(comercio), r'arancel y matricula') then 'arancel y matricula'
          WHEN REGEXP_CONTAINS(lower(comercio), r'colegio|college') then 'colegiatura'
          WHEN REGEXP_CONTAINS(lower(comercio), r'tronwell') then 'instituto de idiomas'
          -- Pasarelas (deberian estar al final)
          WHEN REGEXP_CONTAINS(lower(comercio), r'google') then 'pasarela google'
          WHEN ( REGEXP_CONTAINS(lower(comercio), 'pagos') AND  REGEXP_CONTAINS(lower(comercio), 'flow'))  then 'pagos flow'
          WHEN REGEXP_CONTAINS(lower(comercio), r'skrill')  then 'skrill'
          WHEN REGEXP_CONTAINS(lower(comercio), r'payku')  then 'payku'
          WHEN REGEXP_CONTAINS(lower(comercio), r'kushki')  then 'kushki'
          WHEN REGEXP_CONTAINS(lower(comercio), r'pago facil')  then 'pago facil'
          WHEN REGEXP_CONTAINS(lower(comercio), r'pago online')  then 'pago online'
          WHEN REGEXP_CONTAINS(lower(comercio), r'unitpay')  then 'unitpay'
          WHEN REGEXP_CONTAINS(lower(comercio), r'khipu')  then 'khipu'
          WHEN REGEXP_CONTAINS(lower(comercio), r'ebanx')  then 'ebanx'
          WHEN REGEXP_CONTAINS(lower(comercio), r'payu')  then 'payu'
          WHEN REGEXP_CONTAINS(lower(comercio), r'webpay')  then 'webpay'
          WHEN REGEXP_CONTAINS(lower(comercio), r'probiller|forus|payu|chaturbill')  then 'otras pasarelas de pago'

           ELSE lower(comercio) END AS comercio_recod   
      FROM trxs
  )
  

  SELECT DISTINCT
     comercios.*
     ,CASE 
      WHEN comercio_recod in ('mercado pago', 'mercado pago sa','paymentico.com') THEN 'Mercado Pago'
      WHEN comercio_recod in ('paypal') THEN 'Paypal'
      WHEN comercio_recod in ('pagos flow', 'skrill', 'kushki', 'pago facil', 'pago online', 'unitpay', 'khipu', 'ebanx', 'payu', 'webpay', 'payku','otras pasarelas de pago','comewel.com','mmbill.com','pago maxtron','granity-ent.com','sumup chile') THEN 'Pasarelas de pago'
      WHEN comercio_recod in ('ok market','unimarc','jumbo','lider', 'tottus',  'otros supermercados','neottolemo lda','panaderia y pasteleria') THEN 'Supermercados'
      WHEN comercio_recod in ('amazon', 'aliexpress', 'ebay', 'shein', 'linio', 'wish', 'alibaba', 'buscalibre', 'cuponatic', 'bookdepository','banggood', 'nike', 'skechers', 'otros marketplaces','pasarela google','verificacion ks','buyee tenso','hotmart','merpago*redfacilsa','merpago*mercadolibre') THEN 'Marketplace'
      WHEN comercio_recod in ('rappi', 'uber eats', 'cornershop', 'pedidos ya',  'dominos pizza',  'pizza', 'empresas de despachos','justo','merpago*mcdonaldsecomm','yummy','mc donalds','redelcom krossbar','spacio 1','vendowallet compra') THEN 'Delivery'
      WHEN comercio_recod in ('falabella',  'paris',  'hites',  'ripley',  'abcdin',  'decathlon',  'la polar', 'macys', 'bestbuy','belcorp','merpago*dafiti','adidas','sp * siksilk cl','din cuentas','tricard sa') THEN 'Tiendas x Depto'
      WHEN comercio_recod in ('sodimac', 'easy', 'casa ideas', 'cannon home','baytree') THEN 'Mejoramiento del Hogar'
      WHEN comercio_recod in ('amazon prime',  'onlyfans', 'facebook', 'netflix', 'google play', 'apple',  'spotify',  'badoo', 'tango', 'deezer',  'youtube',  'hbo',  'patreon',  'zapping tv', 'disney plus',  'huawei',  'verificacion google',  'prime video',  'google storage',   'tinder', 'tiktok','star plus cl','tidal','paramount+','tnt sports','funimation','fondo esperanza','arsmate','dating.com','datemyage.com','www.mubi.com','bngcm','liveme america inc.') THEN 'Suscripciones'
      WHEN comercio_recod in ('niantic', 'playstation', 'steam', 'riot', 'nintendo', 'xbox', 'microsoft', 'playdemic', 'playtika', 'electraworks', 'proximabeta', 'moonactive',
       'garena', 'mobilegame', 'garena', 'roblox', 'robtop', 'webzen', 'minecraft', 'klabinc', 'fortnite', 'glugames', 'phantom', 'agaming', 'kingsgroup', 'playrix', 'smartproject', 
       'buffalostudios', 'blizzard entertainment', 'epicgames', 'activision', 'supercell', 'fun games', 'twitch', 'google games', 'google king', 'otros gamer', 'epic games', 'chilecodes','cognosphere pte.ltd.','eneba.com','betsson','moon active','mihoyo limited','skin.club','faroent.info','krosmaga','proxima beta','oculus digital','discord* nitromonthly','bl t2 oc','www.mtcgame.com','gamepay','gvc services limited','csbro.com','xsolla *uplay','skillz * esports','eepa','cults3d.com','square enix','xsolla *trovo') THEN 'Gamer'
      WHEN comercio_recod in ('lipigas', 'wom', 'entel', 'vtr', 'abastible', 'gasco', 'aguas', 'gce', 'enel', 'gtd', 'autopistas', 'chilquinta', 'claro', 'virgin', 'movistar', 'sencillito', 
       'duoc', 'cementerio', 'directv', 'telefonica', 'mundo pacifico', 'esval','wom movil','cge','pagos servicios limita','essbio','metrogas','ponp ltd','saesa','punto pagos','frontel','essal','recarga pagoya 1','pagadoor','gasvalpo','venta de bolsas','scotiabank crédito','nuevo sur','fiverreu','amawa','nueva atacama','quefacil cl','forum') THEN 'Pago de Cuentas'
      WHEN comercio_recod in ('zoom.us', 'jumpseller', 'nic chile','skype', 'facebook ads', 'samsung electronic', 'bigo', 'shopify', 'revolut', 'moonpay','godaddy', 'binance', 'etoro', 'orionx', 'brokeriq',  'pc factory','buy digital asset', 'otros cryptos','iqoption','ipostal1uszoom','sp digital spa') THEN 'E-commerce Tecnológicos'
      WHEN comercio_recod in ('haititalk', 'talk360', 'recargas web', 'que facil', 'otros telecomunicaciones','mundo pacífico','wom fibra','pagaenl*wom','rebtel','telsur','simple') THEN 'Telecomunicaciones'
      WHEN comercio_recod in ('transferwise','worldremit','sendvalu','moneygram','conotoxia','monese','titanesmoneytransfer','tropipay','zinli','transpaygo','global66','remesas') THEN 'Remesas'
      WHEN comercio_recod in ('polla chilena', 'betway', 'loteria', 'bet365','teletrak', 'the lotter','betano','urigol','pokerstars','hipodromo chile','leovegas.com') THEN 'Juegos de Azar'
      WHEN comercio_recod in ('salcobrand', 'farmacias',  'banmedica', 'cruz blanca', 'preunic', 'dbs', 'clinicas','prontomatic','avon rut','genosur','doterra*int usa','spin ok') THEN 'Salud y Belleza'
      WHEN comercio_recod in ('copec', 'shell','shell mi copiloto',  'uber', 'carga bip!','petrobras', 'metro sa',  'tur bus', 'pasaje bus', 'recorrido','beat','cabify',  'lime','autopase autopista central','costanera norte','vespucio sur','vespucio norte','alps spa','didi didi','dl - didi','bike santiago','indriver indriver','autovía santiago lampa','didi','turbus web 2','movired','autopista central','(esso)servacar chile l','lyt spa','eme bus terminal calla','unired.cl tarjeta ripl','petrobas spacios','uniredcl r maipo','unired cl vespucio sur','dist y com dgp spa','unired cl costanera no','chile pasajes','inso spa','petro leiva quilpue','unired cl vespucio nor','unired cl pase diario') THEN 'Transporte'
      WHEN comercio_recod in ('sky airlines', 'american airlines', 'vueling airlines', 'turkish airlines', 'viva airlines', 'united airlines', 'latam airlines', 'jetsmart', 'edestinos', 'despegar', 'vueloferta', 'openpay boletodevuelo', 'viajemos', 'otras agencias de viaje', 'otros viajes y entretencion', 'airbnb', 'flight network', 'otras aerolineas','skokka.com','cinepolis spa','cinemark','pass line ltda','clpro spa') THEN 'Viajes y Entretención'
      WHEN comercio_recod in ('municipalidad','servel','notaría','smapa') THEN 'Trámites Gobierno y Notariales'
      WHEN comercio_recod in ('udemy', 'domestika', 'open english', 'universidades', 'institutos', 'arancel y matricula', 'instituto de idiomas','itaú corbanca cae','ncwllt','micvideal.es','enganchate  c a','uscustoms esta appl pm') THEN 'Educación'
      WHEN comercio_recod in ('udemy','hdi seguros','zenit seguros','soap bci seguros web') THEN 'Seguros'
      ELSE 'Otro'
      END AS rubro_recod
  FROM comercios
  