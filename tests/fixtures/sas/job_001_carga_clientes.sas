/* job_001_carga_clientes.sas
   Carga e transformação de clientes ativos
*/

LIBNAME sasdata '/data/sas/raw';

DATA clientes_filtrados;
    SET sasdata.clientes_raw;
    WHERE status = 'A';
    KEEP id_cliente nome dt_cadastro status;
    FORMAT dt_cadastro DATE9.;
RUN;

PROC SORT DATA=clientes_filtrados OUT=clientes_sorted;
    BY id_cliente;
RUN;

PROC SQL;
    CREATE TABLE work.clientes_enriquecidos AS
    SELECT c.*, v.total_vendas, v.ultima_compra
    FROM clientes_sorted c
    LEFT JOIN sasdata.vendas v ON c.id_cliente = v.id_cliente
    WHERE v.ano = 2025;
QUIT;
