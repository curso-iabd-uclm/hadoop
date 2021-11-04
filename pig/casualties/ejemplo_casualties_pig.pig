-- Carga de datos

suicideRates = load 'temp/suicide_rates.csv' using PigStorage(';') AS
(country:chararray, 
year:int, 
sex:chararray, 
age:chararray, 
suicides_no:int,
population:int,
suicides100k_pop:float,
country_year:chararray,
HDI_for_year:float,
gdp_for_year_dolar:long,
gdp_per_capita_dolar:int,
generation:chararray);

-- Consultas

-- Países ordenados por número total de suicidios

-- Agrupamos los la tasa de suicidios por país
country_rates_suicides = GROUP suicideRates By country;

-- Por cada uno de los países sumamos el número total de suicidios
country_rates_suicides_no = FOREACH country_rates_suicides GENERATE group,SUM(suicideRates.suicides_no) AS total;

-- Ordenamos de forma descendente los resultados
country_rates_suicides_no = ORDER country_rates_suicides_no BY total DESC;

-- Mostramos los resultados
dump country_rates_suicides_no;


-- Porcentajes de suicidios por género en España

-- Filtramos por la tasa de suicidios en España
suicides_spain = FILTER suicideRates BY country == 'Spain';

-- Filtramos por hombres
suicides_spain_male = FILTER suicides_spain BY sex == 'male';
-- Agrupamos por sexo
suicides_spain_by_male = group suicides_spain_male BY sex;
-- Suicidios de hombres
suicides_spain_male_no = FOREACH suicides_spain_by_male generate group, SUM(suicides_spain_male.suicides_no) AS total;

-- Filtramos por mujeres
suicides_spain_female = FILTER suicides_spain BY sex == 'female';
-- Agrupamos por sexo
suicides_spain_by_female = group suicides_spain_female BY sex;
-- Suicidios de mujeres
suicides_spain_female_no = FOREACH suicides_spain_by_female generate group, SUM(suicides_spain_female.suicides_no) AS total;


-- Suicidios totales en españa
total_suicides_spain = group suicides_spain ALL;
total_suicides_spain = FOREACH total_suicides_spain GENERATE SUM(suicides_spain.suicides_no) AS total;


-- Tanto por uno de mujeres que se suicidan en España
female_suicide_rates_spain = FOREACH suicides_spain GENERATE (float)suicides_spain_female_no.total/total_suicides_spain.total AS percentage_female;
female_suicide_rates_spain = DISTINCT female_suicide_rates_spain;

-- Tanto por uno de hombres que se suicidan en España
male_suicide_rates_spain = FOREACH suicides_spain GENERATE (float)suicides_spain_male_no.total/total_suicides_spain.total AS percentage_male;
male_suicide_rates_spain = DISTINCT male_suicide_rates_spain;

-- Resultados
dump female_suicide_rates_spain;
dump male_suicide_rates_spain;


-- Calcular el aumento de suicidios entre antes y después del año 2000

-- Filtrar por suicidios antes del 2000
suicides_before_2000 = FILTER suicideRates BY year <= 2000;
-- Filtrar por suicidios después del 200
suicides_after_2000 = FILTER suicideRates BY year > 2000;

-- Suicidios totales antes del 2000
suicides_before_2000 = group suicides_before_2000 all;
suicides_before_2000 = FOREACH suicides_before_2000 GENERATE SUM(suicides_before_2000.suicides_no) AS total_before;

-- Suicidios totales despues del 2000
suicides_after_2000 = group suicides_after_2000 all;
suicides_after_2000 = FOREACH suicides_after_2000 GENERATE SUM(suicides_after_2000.suicides_no) AS total_after;

-- Calculo de la proporcion
proportion_2000 = FOREACH suicideRates GENERATE (float)suicides_before_2000.total_before/suicides_after_2000.total_after AS proportion;
proportion_2000 = DISTINCT proportion_2000;

-- Mostramos los resultados
dump proportion_2000;

-- Comparar número de suicidios de cada país con su gdp_for_year

-- Filtrar por los suicidios por el año 2015
suicides_2010 = FILTER suicideRates BY year == 2010;
-- Agrupar por paises
suicides_2010_by_country = GROUP suicides_2010 BY country;
-- Sumar el numero de suicidios por paises.
suicides_2010_by_country = FOREACH suicides_2010_by_country GENERATE group, SUM(suicides_2010.suicides_no) AS total, FLATTEN(suicides_2010.gdp_per_capita_dolar) AS gdp;
-- Eliminar duplicados
suicides_2010_by_country = DISTINCT suicides_2010_by_country;
-- Ordenar en base al producto interior bruto
suicides_2010_by_country = ORDER suicides_2010_by_country BY gdp DESC;
-- Mostrar resultados
dump suicides_2010_by_country;


-- Número de suicidios por franja de edad para cada generación

-- Obtenemos las columnas que hacen referencia al numero de suicidios, la edad y la generacion y generamos una nueva
suicides_by_generation_by_age = FOREACH suicideRates GENERATE suicides_no AS suicides_no, age AS age, generation AS generation, CONCAT(generation, '_', age) AS gen_age;

-- agrupamos por la nueva columna
suicides_by_generation_by_age = GROUP suicides_by_generation_by_age BY gen_age;
-- Obtenemos la suma de suicidios por cada generacion en los distintos rangos de edad que abarcan cada una de ellas.
suicides_by_generation_by_age = FOREACH suicides_by_generation_by_age GENERATE STRSPLIT(group, '_', 2) AS generation_age, SUM(suicides_by_generation_by_age.suicides_no);
-- Mostramos los resultados
dump suicides_by_generation_by_age;
