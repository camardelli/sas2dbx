/* autoexec.sas — configurações globais de ambiente
   Executado automaticamente no início de cada sessão SAS.
*/

/* Mapeamento de bibliotecas globais */
LIBNAME SASDATA '/data/sas/input';
LIBNAME SASTEMP '/data/sas/temp';
LIBNAME SASOUT  '/data/sas/output';
LIBNAME MACROLIB '/data/sas/macros';

/* Opções globais */
OPTIONS MPRINT MLOGIC SYMBOLGEN;
OPTIONS COMPRESS=YES;

/* Include de macros corporativas */
%INCLUDE '/data/sas/macros/macro_utils.sas';
