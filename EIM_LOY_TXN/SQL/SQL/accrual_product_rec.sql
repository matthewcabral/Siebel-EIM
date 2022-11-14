/*
=============================================================================================================================
| Objective: Data upload to temp table EIM_LOY_TXN and EIM_LOY_TXNDTL for SIEBEL process XXXXXXX Recurrence Transactions	|
=============================================================================================================================
| Version 	*    Date    *  Release * Developer  	* Project                                                           	|
=============================================================================================================================
|   01   	* 18/05/2022 * R05 2022 * MROSA  		* XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX					|
=============================================================================================================================
*/
SET SERVEROUTPUT ON SIZE 1000000
SET PAGESIZE 1000
SET LINESIZE  2000
SET TIMING ON;

-- OBS IMPORTANTE: Como a data vem como input do arquivo em formato DD/MM/YYYY HH24:MI:SS precisamos colocar o alter session pois nem a conversão TO_DATE funcionou corretamente.
alter session set nls_date_format='dd/mm/yyyy';

DECLARE
   V_ERROR_CODE       VARCHAR2(32767);
   V_ERROR_MSG        VARCHAR2(32767);
BEGIN
    SIEBEL.ACC_PRODUCT_REC ('&1', '&2', '&3', '&4', '&5', '&6', V_ERROR_CODE, V_ERROR_MSG);
	--SIEBEL.ACC_PRODUCT_REC ('1-23456', 'CLB_MB_IN_PATH', 'CLB_MB_EIM_LOG_PATH', 'CLB_MB_EIM_LOG_EXC_PATH', 'CLB_MB_EIM_LOG_PATH', 'XXXXXXX_recurrence_BR_simulation_dev.csv', V_ERROR_CODE, V_ERROR_MSG);

	IF V_ERROR_CODE = '0' THEN
		DBMS_OUTPUT.PUT_LINE('PROCESSO EXECUTADO COM SUCESSO');
	ELSE
		DBMS_OUTPUT.PUT_LINE('PROCESSO EXECUTADO COM ERRO. ENTRE EM CONTATO COM O ANALISTA');  
		DBMS_OUTPUT.PUT_LINE('ERROR CODE: ' || V_ERROR_CODE);
        DBMS_OUTPUT.PUT_LINE('ERROR MESSAGE: ' || V_ERROR_CODE);
	END IF;
END;
/
EXIT;