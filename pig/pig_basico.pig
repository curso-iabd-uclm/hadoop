-- Almacenamiento
messages = LOAD 'messages';
-- Transformación
warns = FILTER messages BY $0 MATCHES '.*WARN+.*';
-- Carga en el HDFS
STORE warns INTO 'warnings';