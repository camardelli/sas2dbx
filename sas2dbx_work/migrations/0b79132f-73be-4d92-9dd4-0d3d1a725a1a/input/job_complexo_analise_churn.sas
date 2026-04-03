/* ============================================================
   job_complexo_analise_churn.sas
   Análise de Churn Mensal — Telcostar S.A.

   Responsável : Equipe de Analytics
   Periodicidade: Mensal (execução D+1 do fechamento)

   Fluxo:
     1. Carga e limpeza de clientes ativos
     2. Cálculo de métricas de uso (voz + dados) com RETAIN
     3. Score de propensão ao churn via macro
     4. Identificação de eventos de churn no período
     5. PROC TRANSPOSE de métricas por faixa de plano
     6. Tabela final de KPIs para o DW
   ============================================================ */

LIBNAME TELCO    "/data/telcostar/operacional";
LIBNAME STAGING  "/data/telcostar/staging";
LIBNAME DW       "/data/telcostar/datawarehouse";

/* ============================================================
   MACRO: calcula score de churn ponderado por segmento
   Parâmetros:
     lib_in   = biblioteca de entrada
     ds_base  = dataset de clientes
     ds_uso   = dataset de uso (voz+dados)
     ano_mes  = período de referência (formato YYYYMM)
     ds_out   = dataset de saída com score
   ============================================================ */
%MACRO calc_churn_score(lib_in=, ds_base=, ds_uso=, ano_mes=, ds_out=);

    /* Passo 1 — une base de clientes com métricas de uso */
    PROC SQL;
        CREATE TABLE STAGING.&ds_out._raw AS
        SELECT
            c.id_cliente,
            c.nm_cliente,
            c.cd_plano,
            c.dt_ativacao,
            c.dt_cancelamento,
            c.vl_mensalidade,
            c.cd_segmento,
            c.fl_portabilidade,
            u.qt_min_voz,
            u.qt_gb_dados,
            u.qt_sms,
            u.vl_excedente,
            u.qt_chamadas_cs,           /* contatos com suporte */
            INTCK('MONTH', c.dt_ativacao, TODAY()) AS meses_ativo,
            MONOTONIC() AS row_num,
            CASE
                WHEN u.qt_gb_dados = 0 THEN 1
                ELSE 0
            END AS fl_sem_dados,
            CATX(' | ', c.cd_plano, c.cd_segmento) AS chave_segmento,
            CALCULATED meses_ativo * c.vl_mensalidade AS receita_acumulada
        FROM &lib_in..&ds_base c
        LEFT JOIN &lib_in..&ds_uso u
            ON c.id_cliente = u.id_cliente
           AND u.ano_mes = &ano_mes
        WHERE c.fl_ativo = 1
          AND c.dt_ativacao <= '01JAN2025'd
        ORDER BY c.cd_segmento, CALCULATED receita_acumulada DESC;
    QUIT;

    /* Passo 2 — score ponderado com RETAIN por segmento */
    PROC SORT DATA=STAGING.&ds_out._raw;
        BY cd_segmento DESCENDING receita_acumulada;
    RUN;

    DATA STAGING.&ds_out._scored;
        SET STAGING.&ds_out._raw;
        BY cd_segmento;

        RETAIN rank_seg acum_receita_seg 0;

        /* Reinicia contadores a cada novo segmento */
        IF FIRST.cd_segmento THEN DO;
            rank_seg        = 0;
            acum_receita_seg = 0;
        END;

        rank_seg         = rank_seg + 1;
        acum_receita_seg = acum_receita_seg + vl_mensalidade;

        /* Arrays de pesos por fator de risco */
        ARRAY peso_risco{5}
            _TEMPORARY_ (0.30 0.25 0.20 0.15 0.10);

        /* Score de churn: combinação ponderada de fatores */
        score_churn =
            peso_risco{1} * (qt_chamadas_cs / MAX(qt_chamadas_cs, 1)) +
            peso_risco{2} * fl_sem_dados +
            peso_risco{3} * (1 - MIN(meses_ativo / 24, 1)) +
            peso_risco{4} * (vl_excedente > 0) +
            peso_risco{5} * fl_portabilidade;

        /* Classifica risco */
        LENGTH cd_risco $10;
        IF      score_churn >= 0.70 THEN cd_risco = 'ALTO';
        ELSE IF score_churn >= 0.40 THEN cd_risco = 'MEDIO';
        ELSE                              cd_risco = 'BAIXO';

        /* Data estimada de saída: INTNX 3 meses à frente */
        dt_saida_estimada = INTNX('MONTH', TODAY(), 3, 'E');
        FORMAT dt_saida_estimada DATE9.;

        /* Extrai primeiro token do plano para classificação */
        familia_plano = SCAN(cd_plano, 1, '_');
        sufixo_plano  = COMPRESS(cd_plano, '', 'A');   /* mantém só dígitos/símbolos */

        /* Normaliza nome do cliente */
        nm_cliente_upper = UPCASE(COMPRESS(nm_cliente, '  ', 's'));

        KEEP id_cliente nm_cliente cd_plano cd_segmento vl_mensalidade
             qt_min_voz qt_gb_dados qt_chamadas_cs vl_excedente
             meses_ativo receita_acumulada rank_seg acum_receita_seg
             score_churn cd_risco dt_saida_estimada familia_plano
             sufixo_plano fl_sem_dados fl_portabilidade row_num;
    RUN;

%MEND calc_churn_score;


/* ============================================================
   MACRO: identifica eventos de churn no período
   ============================================================ */
%MACRO identifica_eventos_churn(lib_in=, ds_scored=, ano_mes=, ds_eventos=);

    PROC SQL;
        CREATE TABLE STAGING.&ds_eventos AS
        SELECT
            s.id_cliente,
            s.cd_risco,
            s.score_churn,
            s.dt_saida_estimada,
            s.familia_plano,
            s.vl_mensalidade,
            s.meses_ativo,
            h.dt_cancelamento,
            h.motivo_cancelamento,
            h.cd_atendente,
            CASE
                WHEN h.dt_cancelamento IS NOT NULL
                 AND INPUT(PUT(&ano_mes, 6.), YYMMN6.) <=
                     INTNX('MONTH', h.dt_cancelamento, 0, 'B')
                THEN 1
                ELSE 0
            END AS fl_churnou,
            SUBSTR(h.motivo_cancelamento, 1, 50) AS motivo_resumido
        FROM STAGING.&ds_scored s
        LEFT JOIN &lib_in..historico_cancelamentos h
            ON s.id_cliente = h.id_cliente
           AND h.dt_cancelamento BETWEEN '01JAN2025'd AND '31JAN2025'd
        WHERE s.score_churn >= 0.40
        ORDER BY s.score_churn DESC;
    QUIT;

%MEND identifica_eventos_churn;


/* ============================================================
   PROC MEANS — métricas agregadas por família de plano e risco
   ============================================================ */
%MACRO agrega_metricas(ds_scored=, ds_agg=);

    PROC MEANS DATA=STAGING.&ds_scored NWAY NOPRINT;
        CLASS familia_plano cd_risco;
        VAR vl_mensalidade qt_gb_dados qt_min_voz qt_chamadas_cs score_churn;
        OUTPUT OUT=STAGING.&ds_agg (DROP=_TYPE_ _FREQ_)
            N    = n_clientes
            MEAN = media_mensalidade media_gb media_voz media_cs media_score
            SUM  = sum_mensalidade sum_gb sum_voz sum_cs sum_score
            STD  = std_mensalidade std_gb std_voz std_cs std_score
            MIN  = min_mensalidade min_gb min_voz min_cs min_score
            MAX  = max_mensalidade max_gb max_voz max_cs max_score;
    RUN;

    /* Calcula participação percentual no total por família */
    DATA STAGING.&ds_agg;
        SET STAGING.&ds_agg;
        pct_receita = sum_mensalidade / SUM(sum_mensalidade) * 100;
        pct_clientes = n_clientes / SUM(n_clientes) * 100;
        ticket_medio = sum_mensalidade / MAX(n_clientes, 1);
    RUN;

%MEND agrega_metricas;


/* ============================================================
   PROC TRANSPOSE — pivot de métricas por plano × mês
   Gera linhas (metrica, plano_A, plano_B, plano_C, plano_CTRL)
   ============================================================ */
%MACRO transpoe_metricas(ds_agg=, ds_pivot=);

    /* Primeiro: seleciona apenas métricas médias para o pivot */
    PROC SQL;
        CREATE TABLE work.pre_pivot AS
        SELECT
            familia_plano,
            cd_risco,
            media_mensalidade,
            media_gb,
            media_voz,
            media_score,
            n_clientes
        FROM STAGING.&ds_agg
        WHERE cd_risco = 'ALTO';
    QUIT;

    PROC TRANSPOSE
        DATA  = work.pre_pivot
        OUT   = STAGING.&ds_pivot (RENAME=(_NAME_=metrica))
        PREFIX= plano_;
        BY cd_risco;
        ID familia_plano;
        VAR media_mensalidade media_gb media_voz media_score n_clientes;
    RUN;

%MEND transpoe_metricas;


/* ============================================================
   PROC FREQ — distribuição de risco e validação de contagens
   ============================================================ */
%MACRO gera_frequencias(ds_scored=);

    PROC FREQ DATA=STAGING.&ds_scored;
        TABLES cd_risco / NOCUM OUT=work.freq_risco;
        TABLES familia_plano * cd_risco / NOCUM NOPERCENT
               SPARSE OUT=work.freq_plano_risco;
        TABLES fl_churnou * cd_risco / CHISQ MEASURES
               OUT=work.freq_churn_risco;
    RUN;

    /* Log de validação */
    DATA _NULL_;
        SET work.freq_risco;
        IF cd_risco = 'ALTO' THEN
            PUT 'INFO: Clientes alto risco = ' COUNT;
    RUN;

%MEND gera_frequencias;


/* ============================================================
   CARGA FINAL NO DW — tabela particionada por ano_mes
   ============================================================ */
%MACRO carga_dw(ds_scored=, ds_eventos=, ds_pivot=, ano_mes=);

    PROC SQL;
        CREATE TABLE DW.fato_churn_mensal AS
        SELECT
            &ano_mes                    AS ano_mes,
            s.id_cliente,
            s.cd_plano,
            s.familia_plano,
            s.cd_segmento,
            s.cd_risco,
            s.score_churn,
            s.vl_mensalidade,
            s.receita_acumulada,
            s.meses_ativo,
            s.qt_gb_dados,
            s.qt_min_voz,
            s.qt_chamadas_cs,
            s.dt_saida_estimada,
            COALESCE(e.fl_churnou, 0)       AS fl_churnou,
            COALESCE(e.motivo_resumido, '')  AS motivo_resumido,
            COALESCE(e.cd_atendente, 'N/A') AS cd_atendente,
            TODAY()                          AS dt_carga
        FROM STAGING.&ds_scored s
        LEFT JOIN STAGING.&ds_eventos e
            ON s.id_cliente = e.id_cliente;
    QUIT;

    /* Índice para consultas por período */
    PROC DATASETS LIB=DW NOPRINT;
        MODIFY fato_churn_mensal;
        INDEX CREATE idx_ano_mes_risco = (ano_mes cd_risco);
    QUIT;

%MEND carga_dw;


/* ============================================================
   ORQUESTRAÇÃO — executa macros em sequência
   ============================================================ */
%LET ANO_MES_REF = 202501;

%calc_churn_score(
    lib_in  = TELCO,
    ds_base = clientes_ativos,
    ds_uso  = uso_mensal,
    ano_mes = &ANO_MES_REF,
    ds_out  = churn_scored
);

%identifica_eventos_churn(
    lib_in    = TELCO,
    ds_scored = churn_scored,
    ano_mes   = &ANO_MES_REF,
    ds_eventos = churn_eventos
);

%agrega_metricas(
    ds_scored = churn_scored,
    ds_agg    = churn_agregado
);

%transpoe_metricas(
    ds_agg   = churn_agregado,
    ds_pivot = churn_pivot
);

%gera_frequencias(
    ds_scored = churn_scored
);

%carga_dw(
    ds_scored  = churn_scored,
    ds_eventos = churn_eventos,
    ds_pivot   = churn_pivot,
    ano_mes    = &ANO_MES_REF
);
