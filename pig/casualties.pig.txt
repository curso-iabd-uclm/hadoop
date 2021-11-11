casualtyRates = load 'CASUALTIES/part-m-00000' using PigStorage(';') AS
(country:chararray, 
year:int, 
sex:chararray, 
age:chararray, 
casualties_no:int,
population:int,
casualties100k_pop:float,
country_year:chararray,
HDI_for_year:float,
gdp_for_year_dolar:long,
gdp_per_capita_dolar:int,
generation:chararray);

-- Agrupamos los la tasa  por país
country_rates_casualties = GROUP casualtyRates By country;

-- Por cada uno de los países sumamos el número total 
country_rates_casualties_no = FOREACH country_rates_casualties GENERATE group,SUM(casualtyRates.casualties_no) AS total;

-- Ordenamos de forma descendente los resultados
country_rates_casualties_no = ORDER country_rates_casualties_no BY total DESC;

-- Mostramos los resultados
STORE country_rates_casualties_no INTO 'pig_out/' USING PigStorage (',');