-- Carga de datos

casualtyRates = load 'temp/casualti_rates.csv' using PigStorage(';') AS
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

-- Consultas

-- Países ordenados por número total 

-- Agrupamos los la tasa  por país
country_rates_casualties = GROUP casualtyRates By country;

-- Por cada uno de los países sumamos el número total 
country_rates_casualties_no = FOREACH country_rates_casualties GENERATE group,SUM(casualtiRates.casualties_no) AS total;

-- Ordenamos de forma descendente los resultados
country_rates_casualties_no = ORDER country_rates_casualties_no BY total DESC;

-- Mostramos los resultados
dump country_rates_casualties_no;


-- Porcentajes  por género en España

-- Filtramos por la tasa  en España
casualties_spain = FILTER casualtiRates BY country == 'Spain';

-- Filtramos por hombres
casualties_spain_male = FILTER casualties_spain BY sex == 'male';
-- Agrupamos por sexo
casualties_spain_by_male = group casualties_spain_male BY sex;
-- Suicidios de hombres
casualties_spain_male_no = FOREACH casualties_spain_by_male generate group, SUM(casualties_spain_male.casualties_no) AS total;

-- Filtramos por mujeres
casualties_spain_female = FILTER casualties_spain BY sex == 'female';
-- Agrupamos por sexo
casualties_spain_by_female = group casualties_spain_female BY sex;
-- Suicidios de mujeres
casualties_spain_female_no = FOREACH casualties_spain_by_female generate group, SUM(casualties_spain_female.casualties_no) AS total;


-- Suicidios totales en españa
total_casualties_spain = group casualties_spain ALL;
total_casualties_spain = FOREACH total_casualties_spain GENERATE SUM(casualties_spain.casualties_no) AS total;


-- Tanto por uno de mujeres que se suicidan en España
female_casualti_rates_spain = FOREACH casualties_spain GENERATE (float)casualties_spain_female_no.total/total_casualties_spain.total AS percentage_female;
female_casualti_rates_spain = DISTINCT female_casualti_rates_spain;

-- Tanto por uno de hombres que se suicidan en España
male_casualti_rates_spain = FOREACH casualties_spain GENERATE (float)casualties_spain_male_no.total/total_casualties_spain.total AS percentage_male;
male_casualti_rates_spain = DISTINCT male_casualti_rates_spain;

-- Resultados
dump female_casualti_rates_spain;
dump male_casualti_rates_spain;


-- Calcular el aumento  entre antes y después del año 2000

-- Filtrar por suicidios antes del 2000
casualties_before_2000 = FILTER casualtiRates BY year <= 2000;
-- Filtrar por suicidios después del 200
casualties_after_2000 = FILTER casualtiRates BY year > 2000;

-- Suicidios totales antes del 2000
casualties_before_2000 = group casualties_before_2000 all;
casualties_before_2000 = FOREACH casualties_before_2000 GENERATE SUM(casualties_before_2000.casualties_no) AS total_before;

-- Suicidios totales despues del 2000
casualties_after_2000 = group casualties_after_2000 all;
casualties_after_2000 = FOREACH casualties_after_2000 GENERATE SUM(casualties_after_2000.casualties_no) AS total_after;

-- Calculo de la proporcion
proportion_2000 = FOREACH casualtiRates GENERATE (float)casualties_before_2000.total_before/casualties_after_2000.total_after AS proportion;
proportion_2000 = DISTINCT proportion_2000;

-- Mostramos los resultados
dump proportion_2000;

-- Comparar número  de cada país con su gdp_for_year

-- Filtrar por los suicidios por el año 2015
casualties_2010 = FILTER casualtiRates BY year == 2010;
-- Agrupar por paises
casualties_2010_by_country = GROUP casualties_2010 BY country;
-- Sumar el numero  por paises.
casualties_2010_by_country = FOREACH casualties_2010_by_country GENERATE group, SUM(casualties_2010.casualties_no) AS total, FLATTEN(casualties_2010.gdp_per_capita_dolar) AS gdp;
-- Eliminar duplicados
casualties_2010_by_country = DISTINCT casualties_2010_by_country;
-- Ordenar en base al producto interior bruto
casualties_2010_by_country = ORDER casualties_2010_by_country BY gdp DESC;
-- Mostrar resultados
dump casualties_2010_by_country;


-- Número  por franja de edad para cada generación

-- Obtenemos las columnas que hacen referencia al numero , la edad y la generacion y generamos una nueva
casualties_by_generation_by_age = FOREACH casualtiRates GENERATE casualties_no AS casualties_no, age AS age, generation AS generation, CONCAT(generation, '_', age) AS gen_age;

-- agrupamos por la nueva columna
casualties_by_generation_by_age = GROUP casualties_by_generation_by_age BY gen_age;
-- Obtenemos la suma  por cada generacion en los distintos rangos de edad que abarcan cada una de ellas.
casualties_by_generation_by_age = FOREACH casualties_by_generation_by_age GENERATE STRSPLIT(group, '_', 2) AS generation_age, SUM(casualties_by_generation_by_age.casualties_no);
-- Mostramos los resultados
dump casualties_by_generation_by_age;
