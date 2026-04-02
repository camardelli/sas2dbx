/* autoexec_churn.sas — configurações globais para job de análise de churn
   Telcostar S.A. — ambiente operacional

   Executado automaticamente antes do job_complexo_analise_churn.sas
*/

/* Mapeamento de bibliotecas operacionais */
LIBNAME TELCO    "/data/telcostar/operacional";
LIBNAME STAGING  "/data/telcostar/staging";
LIBNAME DW       "/data/telcostar/datawarehouse";

/* Opções globais */
OPTIONS MPRINT MLOGIC SYMBOLGEN;
OPTIONS COMPRESS=YES;
OPTIONS FMTSEARCH=(TELCO STAGING WORK);

/* Parâmetros globais de ambiente */
%LET ENV        = PRD;
%LET DT_INICIO  = 01JAN2025;
%LET DT_FIM     = 31JAN2025;
