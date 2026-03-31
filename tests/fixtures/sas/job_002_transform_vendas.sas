/* job_002_transform_vendas.sas
   Agrega vendas por departamento e aplica macro de SCD2
*/

%MACRO calc_totals(dataset=, var=, groupby=);
    PROC MEANS DATA=&dataset NWAY NOPRINT;
        CLASS &groupby;
        VAR &var;
        OUTPUT OUT=work.totals_&var SUM=total_&var MEAN=media_&var;
    RUN;
%MEND calc_totals;

%calc_totals(dataset=sasdata.vendas, var=valor, groupby=depto);

DATA vendas_com_retain;
    SET sasdata.vendas;
    BY depto;
    RETAIN acumulado 0;
    IF FIRST.depto THEN acumulado = 0;
    acumulado = acumulado + valor;
RUN;

PROC FREQ DATA=vendas_com_retain;
    TABLES depto * status / NOCUM NOPERCENT;
RUN;
