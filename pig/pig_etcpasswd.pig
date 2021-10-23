-- 1. Extraccion del etc/passwd
LOAD '/etc/passwd' USING PigStorage(':') AS (user:chararray, \
passwd:chararray, uid:int, gid:int, userinfo:chararray, home:chararray, \
shell:chararray);

-- 2. Transformacion
-- 2.1 agrupacion por shell
grp_shell = GROUP passwd BY shell;
-- 2.2 recuento
counts = FOREACH grp_shell GENERATE group, COUNT(passwd);

-- 3. Carga/Almacenaniento/Volcado
DUMP counts;