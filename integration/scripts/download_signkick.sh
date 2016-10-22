# This script fetches billboard locations from the SignKick site
 curl -g 'http://www.signkick.co.uk/search-results/51.686/-0.489/51.28/0.236?filterPeriod=0&filterAvailabilityFrom=&filterAvailabilityTo=&postingCyclePriceFrom=0&postingCyclePriceTo=5000&filterBySignType[]=Static%206-sheet&filterBySignType[]=Scrolling%206-sheet&filterBySignType[]=Adshel%20poster&filterBySignType[]=Infopaneel&filterBySignType[]=Bushokje&filterBySignType[]=Lamppost%20Banner&filterBySignType[]=2m2&filterBySignType[]=Sheffield%20City%20Centre%20package&filterBySignType[]=Mobilier%20Urbain%202m2&filterBySignType[]=Mobilier%20Urbain%208m2&filterBySignType[]=Raised%206-sheet&filterBySignType[]=MUPIS&filterBySignType[]=Abrigos&filterBySignType[]=Street%20furniture&filterBySignType[]=Bus%20shelter&filterBySignType[]=Static%2048-sheet&filterBySignType[]=Scrolling%2048-sheet&filterBySignType[]=Vertical%2096-sheet&filterBySignType[]=Bridge%20Span&filterBySignType[]=Paper%2016-sheet&filterBySignType[]=Mega%206&filterBySignType[]=Mini%20billboard&filterBySignType[]=Backlit%2096-sheet&filterBySignType[]=Paper%2096-sheet&filterBySignType[]=Backlit%2048-sheet&filterBySignType[]=Trivision%2048-sheet&filterBySignType[]=Billboard&filterBySignType[]=Large%20Portrait%20Panel%20-%20250&filterBySignType[]=Large%20Portrait%20Panel%20-%20350&filterBySignType[]=Vinyl%2048-sheet&filterBySignType[]=Static%2032-sheet&filterBySignType[]=Static%2016-sheet&filterBySignType[]=MOBILIARIO%20URBANO&filterBySignType[]=Mini%20Supersite&filterBySignType[]=4%20x%202&filterBySignType[]=Supersite&filterBySignType[]=24%20Sheet&filterBySignType[]=Analoge%20Reclamemast&filterBySignType[]=Wild%20Posting&filterBySignType[]=Phonebox&filterBySignType[]=Digital%206-sheet&filterBySignType[]=Digital%20Super%206&filterBySignType[]=Digital%20Mega%206&filterBySignType[]=Digital%20Billboard%2048&filterBySignType[]=Digital%20Billboard&filterBySignType[]=Digital%20Screen%20Portrait%2040%22&filterBySignType[]=Digital%20screens%20in%20pub&filterBySignType[]=Digital%20screens%20in%20office%20building&filterBySignType[]=Digitale%20Reclamemast&filterBySignType[]=Screen%20in%20BP%20Station&filterBySignType[]=Screen%20in%20Shell%20Station&filterBySignType[]=AdDryer&filterBySignType[]=Charging%20Station&filterBySignType[]=Link%2055%22%20display&filterBySignType[]=Shopping%20Centre%20Furniture&filterBySignType[]=Shopping%20Centre&filterBySignType[]=Railway%20Station%204-sheet' -H 'Host: www.signkick.co.uk' -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:49.0) Gecko/20100101 Firefox/49.0' -H 'Accept: */*' -H 'Accept-Language: en-US,en;q=0.5' --compressed -H 'Referer: http://www.signkick.co.uk/search/-0.42380365189035996/51.3357815732648/14?pe=0&sd=&ed=&ps[]=Static%206-sheet&ps[]=Scrolling%206-sheet&ps[]=Static%2048-sheet&ps[]=Phonebox&ps[]=Digital%206-sheet&ps[]=Shopping%20Centre%20Furniture&ps[]=Railway%20Station' -H 'X-Requested-With: XMLHttpRequest' -H 'Cookie: hl=en_GB; ajs_user_id=%22SKUSR2937218%22; ajs_group_id=null; ajs_anonymous_id=%2201a23850-449d-4060-8524-e63323d727aa%22; _ga=GA1.3.751540174.1476538537; _pk_id.1.6a06=1ef0118cf75e2fc4.1476538538.2.1476721158.1476721083.; _pk_ref.1.6a06=%5B%22%22%2C%22%22%2C1476721083%2C%22https%3A%2F%2Fwww.google.co.uk%2F%22%5D; intercom-visitor-semaphore-d0df5wqw=1; gs_v_GSN-798097-Z=; gs_u_GSN-798097-Z=8f6b934bd5847b4f5728167a17e35375:6951:9169:1476721159487; intercom-id=528dceae-71ed-4898-b48f-62b31de35c30; intercom-session-d0df5wqw=d1NIVi9QVGN5ZDVBMHBxZGVYd20yMjhPc0xxcCs1U2djWTZGcjNIQ1RmZlZpREdrZGNGZUN2dFRjNHdQTTJpQy0tWHFRbFI2MGhIdnVhN3orMTZXM2Q0Zz09--35272a40d99deacf71e8c44cfa94cc28cfa8ae75; __utma=222020762.751540174.1476538537.1476538908.1476538908.1; __utmz=222020762.1476538908.1.1.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); PHPSESSID=cernqhhr97vmd4bppku36jmmq7; _pk_ses.1.6a06=*; _dc_gtm_UA-28536257-2=1; mp_mixpanel__c=0; _cio=708ce527-79d8-ac4e-ea46-01a9b1c9c2d5; LONGSESS=U2lnbmtpY2tcQ29yZVxTZWN1cml0eVxBdXRoZW50aWNhdGlvblxVc2VyOllXeHpjR0Z5UUdkdFlXbHNMbU52YlE9PToxNjM0NDAxMTQ1OjM0ZjEzODNiODI2ZDhmNDEwZGM2NzczNGRjYjM5ZTRhODdkNGQxNTZiMDk5YzcxZDNiY2FhYTVjMzVmZTlmODE%3D; mp_0d39170a2c264d5506cb6a7c0ee6c5a8_mixpanel=%7B%22distinct_id%22%3A%20%22alspar%40gmail.com%22%2C%22%24search_engine%22%3A%20%22google%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.co.uk%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.co.uk%22%2C%22__mps%22%3A%20%7B%7D%2C%22__mpso%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpap%22%3A%20%5B%5D%7D; _cioid=SKUSR2937218; _gat_UA-28536257-2=1' -H 'Connection: keep-alive'
