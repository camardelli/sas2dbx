/* job_003_report_mensal.sas
   Formatos customizados e relatório mensal — contém Tier 3
*/

PROC FORMAT;
    VALUE $status_fmt
        'A' = 'Ativo'
        'I' = 'Inativo'
        'P' = 'Pendente';

    VALUE faixa_valor_fmt
        LOW    -< 1000  = 'Baixo'
        1000   -< 5000  = 'Médio'
        5000   - HIGH   = 'Alto';
RUN;

PROC SQL;
    CREATE TABLE work.relatorio_mensal AS
    SELECT
        depto,
        SUM(valor) AS total_vendas,
        COUNT(*) AS qtd_pedidos,
        MEAN(valor) AS ticket_medio
    FROM sasdata.vendas
    WHERE YEAR(dt_venda) = 2025 AND MONTH(dt_venda) = 1
    GROUP BY depto;
QUIT;

PROC REPORT DATA=work.relatorio_mensal NOWINDOWS;
    COLUMN depto total_vendas qtd_pedidos ticket_medio;
    DEFINE depto / GROUP 'Departamento';
    DEFINE total_vendas / ANALYSIS SUM FORMAT=DOLLAR12.2 'Total Vendas';
    DEFINE qtd_pedidos / ANALYSIS N 'Qtd Pedidos';
    DEFINE ticket_medio / ANALYSIS MEAN FORMAT=DOLLAR10.2 'Ticket Médio';
RUN;
