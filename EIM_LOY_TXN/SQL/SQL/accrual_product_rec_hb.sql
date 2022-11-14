/*
=============================================================================================================================
| Objective: Generate HandBack file result for XXXXXXX Recurrence Transaction import process          					       |
=============================================================================================================================
| Version 	*    Date    *  Release * Developer  	* Project                                                           	    |
=============================================================================================================================
|   01   	* 18/05/2022 * R05 2022 * MROSA  		* XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX					    |
=============================================================================================================================
*/
SET SERVEROUTPUT ON SIZE 1000000
SET PAGESIZE 1000
SET LINESIZE  2000
SET TIMING ON;

-- OBS IMPORTANTE: Como a data vem como input do arquivo em formato DD/MM/YYYY HH24:MI:SS precisamos colocar o alter session pois nem a conversão TO_DATE funcionou corretamente.
alter session set nls_date_format='dd/mm/yyyy';

DECLARE
   V_ERROR_CODE      VARCHAR2(32767)   := NULL;
   V_ERROR_MSG       VARCHAR2(32767)   := NULL;
   V_IN_PROCESS_ID   VARCHAR2(50)      := '&1'; -- 1-1O10XCR5
   V_IN_FILE         VARCHAR2(2000)    := '&2'; -- XXXXXXX_recurrence_BR_202200510120000.csv
   V_IN_OUT_DIR      VARCHAR2(2000)    := '&3'; -- CLB_MB_OUT_PATH
   V_IN_LOG_DIR      VARCHAR2(2000)    := '&4'; -- CLB_MB_LOG_EXC_PATH
   V_IN_FILE_START   VARCHAR2(50)      := '&5' || ' ' || '&6'; -- O campo de data vem como VARCHAR2 com espaço entre a data e hora e por isso precisou ser concatenado
   V_IN_FILE_END     VARCHAR2(50)      := '&7' || ' ' || '&8'; -- O campo de data vem como VARCHAR2 com espaço entre a data e hora e por isso precisou ser concatenado
BEGIN
	SIEBEL.ACC_PRODUCT_REC_HDB (V_IN_PROCESS_ID, V_IN_FILE, V_IN_OUT_DIR, V_IN_LOG_DIR, V_IN_FILE_START, V_IN_FILE_END, V_ERROR_CODE, V_ERROR_MSG);

   IF V_ERROR_CODE = '0' THEN
      DBMS_OUTPUT.PUT_LINE('PROCESSO EXECUTADO COM SUCESSO');
   ELSE
      DBMS_OUTPUT.PUT_LINE('PROCESSO EXECUTADO COM ERRO. ENTRE EM CONTATO COM O ANALISTA');  
      DBMS_OUTPUT.PUT_LINE('ERRO : ' || V_ERROR_CODE);
   END IF;
END;
/
EXIT;