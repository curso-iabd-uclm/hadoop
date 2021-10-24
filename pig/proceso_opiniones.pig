
-- 1.1 Fase de Extracción LOAD
csv_data = LOAD '/user/cloudera/pig/Eopinions.csv' USING PigStorage (',') as
(class:chararray, opinion:chararray);

-- DUMP csv_data; -- para comprobaciones

-- 1.2 Generar colección de comentarios (campo 1)
comentarios = FOREACH csv_data GENERATE $1;

-- DUMP csv_data; -- para comprobaciones

-- 2.1 Procesar cada comentario y trozearlo en palabras
-- TOKENIZE: cadena -> bolsa de palabras (bag of words)
-- FLATTEN: desanida/aplana tuplas bosas de palabras
wordfile_flat = FOREACH comentarios GENERATE FLATTEN (TOKENIZE($0)) as wordin;

-- 2.2 Agrupación por palabras (GROUP BY)
wordfile_grpd = GROUP wordfile_flat by wordin;

-- 2.3 Calculo frecuencia de cada palabra
word_counts = FOREACH wordfile_grpd GENERATE group, COUNT(wordfile_flat.wordin) as cnt; 

-- 2.4 Ordenación/Ranking de palabras por frecuencia
word_count_des = ORDER word_counts BY cnt DESC;

-- 3. Carga/almacenamiento
STORE word_count_des into '/user/cloudera/pig/out';