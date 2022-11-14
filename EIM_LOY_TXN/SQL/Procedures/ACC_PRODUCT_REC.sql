CREATE OR REPLACE PROCEDURE "ACC_PRODUCT_REC" (IN_PROCESS_ID IN VARCHAR2, IN_DIRECTORY IN VARCHAR2, IN_LOG_DIR IN VARCHAR2, IN_EXCEPT_DIR IN VARCHAR2, IN_JOB_DIR IN VARCHAR2, IN_FILE IN VARCHAR2, OUT_FILE_ERROR_CD OUT VARCHAR2, OUT_FILE_ERROR_MSG OUT VARCHAR2) IS
--DECLARE
    /*
    =============================================================================================================================
    | Objective: Data upload to temp table EIM_LOY_TXN and EIM_LOY_TXNDTL for SIEBEL process XXXXX XXXXXXXXXXXXX Transactions	|
    =============================================================================================================================
    | Version 	*    Date    *  Release * Developer  	* Project                                                           	|
    =============================================================================================================================
    |   01   	* 18/05/2022 * R05 2022 * MROSA  		* xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx					|
    =============================================================================================================================
    |                                                           INDEX                                                           |
    =============================================================================================================================
    |   F_		: File Variables                                                                                                |
    |   I_		: File input variables                                                                                          |
    |   IN_		: Procedure input variables                                                                                     |
    |   C_		: Count variables                                                                                               |
    |   FV_		: Fixed value Columns (Constants)                                                                               |
    |   V_		: Selected variables                                                                                            |
    |   EIM_	: EIM column variables                                                                                          |
    =============================================================================================================================
    */
	
	------------------------ PROCEDURE VARIABLES ------------------------
	V_START_TIME					VARCHAR2(50)	:= TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS');	    -- INITIAL TIME OF PROCEDURE EXECUTION
    V_END_TIME						VARCHAR2(50)	:= NULL;											-- FINAL TIME OF PROCEDURE EXECUTION
	
	------------------------ ERROR VARIABLES ------------------------
    V_ERROR_MESSAGE                 VARCHAR2(32767) := NULL;
    V_ERROR_CODE            		VARCHAR2(32767) := NULL;
    V_EIM_FINAL                     VARCHAR2(32767) := NULL;
    --OUT_FILE_ERROR_CD               VARCHAR2(32767) := NULL;
	--OUT_FILE_ERROR_MSG              VARCHAR2(32767) := NULL;
	------------------------ FILE VARIABLES ------------------------
	F_FILE                  		UTL_FILE.FILE_TYPE;			-- CSV FILE
    F_FILE_ERROR            		UTL_FILE.FILE_TYPE;         -- ERROR FILE
    F_FILE_JOB              		UTL_FILE.FILE_TYPE;         -- JOB FILE
    F_FILE_EXCEP              		UTL_FILE.FILE_TYPE;         -- EXP FILE

    ------------------------ FILE NAMES VARIABLES ------------------------
	--IN_PROCESS_ID                   VARCHAR2(2000)	:= '123';   -- JOB FILE NAME
	--IN_FILE                         VARCHAR2(2000)	:= 'XXXXXX_recurrence_BR_simulation_dev.csv';    -- INPUT FILE NAME
	F_LOG_FILE                 		VARCHAR2(200)  	:= SUBSTR(IN_FILE,1,LENGTH(IN_FILE) - 4) || '_' || 'EIM_LOAD_HandBack' || '_'|| TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') || '.log';
    F_NEW_FILE               		VARCHAR2(200)   := NULL;    -- EXCEPT AND LOAD ERROR FILE NAME

    ------------------------ FILE DIRECTORIES ------------------------
    F_DIRECTORY                    	VARCHAR2(2000)	:= IN_DIRECTORY;-- INPUT FILE DIRECTORY     --IN_DIRECTORY
	F_LOG_DIR                		VARCHAR2(2000)  := IN_LOG_DIR;-- LOG FILE DIRECTORY       --IN_LOG_DIR
    F_EXCEPT_DIR                    VARCHAR2(2000)  := IN_EXCEPT_DIR;-- EXCEPT FILE DIRECTORY    --IN_EXCEPT_DIR
    F_JOB_DIR                       VARCHAR2(2000)  := IN_JOB_DIR;-- JOB FILE DIRECTORY       --IN_JOB_DIR

    ------------------------ FILE LINE VARIABLES ------------------------
    F_LINE                  		VARCHAR2(2000) 	:= NULL;	-- FILE LINE
    F_LINE_BKP              		VARCHAR2(2000) 	:= NULL;	-- FILE BACKUP LINE
	F_LINE_NUMBER					NUMBER(22)   	:= 1;		-- FILE LINE NUMBER
	
	------------------------ FILE COUNTERS ------------------------
    C_RECORD_COUNT   				NUMBER(22)   	:= 0;		-- TOTAL FILE COUNT RECORDS
	C_COUNT_REC_ERROR       		NUMBER(22)   	:= 0;		-- TOTAL OF FILE ERROR RECORDS
    C_REC_COUNT_COMMIT  		    NUMBER(22)   	:= 0;
	
	------------------------ FILE HEADER COLUMNS ------------------------
	F_QTD_HDR_COL        			NUMBER(22)   	:= 1; 	    -- FILE HEADER COLUMN QUANTITY
    F_HDR_COL_NAME					VARCHAR2(100)	:= NULL;	-- FILE HEADER COLUMN NAME
	--
	F_NUM_COL_MBR_NUM     			NUMBER(22) := 0;			-- FILE HEADER COLUMN NUMBER: MEMBER_NUMBER
    F_NUM_COL_PROG_NAME     		NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: PROGRAM_NAME
    F_NUM_COL_PAR_ALIAS     		NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: PARTNER_ALIAS
    F_NUM_COL_TXN_DT     			NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: TRANSACTION_DATE
    F_NUM_COL_ACT_DT     			NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: ACTIVITY_DATE
    F_NUM_COL_SIGN_CODE     		NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: PARENT_SIGNATURE_CODE
    F_NUM_COL_PAR_TXN_ID     		NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: PARENT_TRANSACTION_ID
    F_NUM_COL_TXN_PRD_NAME  		NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: TRANSACTION_PRODUCT_NAME
    F_NUM_COL_TXN_AMT     			NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: TRANSACTION_AMOUNT
    F_NUM_COL_TXN_TYPE     			NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: TRANSACTION_TYPE
    F_NUM_COL_TXN_SUBTYPE     		NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: TRANSACTION_SUBTYPE
    --F_NUM_COL_TXN_STATUS     		NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: TRANSACTION_STATUS
    F_NUM_COL_TXN_CHANNEL     		NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: TRANSACTION_CHANNEL
    F_NUM_COL_TXN_COMMENTS     		NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: TRANSACTION_COMMENTS
    F_NUM_COL_TXN_P_NAME    		NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: TRANSACTION_POINT_NAME
    F_NUM_COL_OWNER     			NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: OWNER_LOGIN
    F_NUM_COL_EXTNL     			NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: EXTERNAL
    F_NUM_COL_EXTNL_P_ID     		NUMBER(22) := 0;    		-- FILE HEADER COLUMN NUMBER: EXTERNAL_PARTNER_ID
    
    ------------------------ FILE EXCEPTION COLUMNS ------------------------
    FILE_EXCEP               		EXCEPTION;
    LOAD_EXCEP               		EXCEPTION;
    RECORD_EXCEP             		EXCEPTION;
    DATE_EXCEP               		EXCEPTION;
    POINT_EXCEP              		EXCEPTION;
    POINT_EXCEP_ZERO         		EXCEPTION;
    POINT_EXCEP_EMPTY        		EXCEPTION;
    CHECK_FOORTER            		EXCEPTION;
    PRODUCT_EXCEP            		EXCEPTION;
    PRODUCT_EXCEP_CO         		EXCEPTION;
    LAST_NAME_EMPTY_EXCEP    		EXCEPTION;
    FST_NAME_EMPTY_EXCEP     		EXCEPTION;
    FST_NAME_NOT_MATCH       		EXCEPTION;
    LAST_NAME_NOT_MATCH      		EXCEPTION;
    DATE_EMPTY               		EXCEPTION;
    DATE_FUTURE              		EXCEPTION;
    DATE_PAST                		EXCEPTION;
    MEMBER_DECEASED_EXCEP    		EXCEPTION;
    MEMBER_ACTIVE_EXCEP      		EXCEPTION;
    IS_DATE_EXCEP            		EXCEPTION;

    PRAGMA EXCEPTION_INIT 			(IS_DATE_EXCEP, -01843);
    PRAGMA EXCEPTION_INIT 			(DATE_EXCEP, -01843);
    PRAGMA EXCEPTION_INIT 			(DATE_EXCEP, -01847);
    PRAGMA EXCEPTION_INIT 			(DATE_EXCEP, -01830);
    PRAGMA EXCEPTION_INIT 			(POINT_EXCEP, -06502);
    
	--------------------------- TRANSACTION EIM SYSTEM COLUMNS ---------------------------
    EIM_ROW_ID              		NUMBER(22)   	:= NULL;
    EIM_IF_ROW_STAT         		VARCHAR2(30) 	:= 'FOR_IMPORT';
    EIM_ROW_BATCH_NUM       		NUMBER(22)   	:= 1;
    EIM_ROW_BATCH_BKP       		NUMBER(10)   	:= NULL;
	
	--------------------------- TRANSACTION COLUMNS ---------------------------
    -- TRANSACTION COUNT COLUMNS
    C_TXN_TYPE_LOV_COUNT            NUMBER(22)      := 0;			-- TOTAL DE TRANSACTION TYPE ENCONTRADOS
    C_TXN_SUBTYPE_LOV_COUNT         NUMBER(22)      := 0;			-- TOTAL DE TRANSACTION SUBTYPE ENCONTRADOS
    
    -- TRANSACTION VARIABLE COLUMNS
	I_TXN_AMOUNT           			NUMBER(22)      := 0;			-- TOTAL DE MILHAS A SEREM ACUMULADAS NA TRANSACAO
	V_TXN_POINTS            		NUMBER(22)      := 0;			-- TOTAL DE PONTOS
    I_TXN_DATE              		TIMESTAMP 		:= NULL;		-- DATA DA TRANSACAO
	I_TXN_POST_DATE					TIMESTAMP	 	:= NULL;		-- DATA DA ATIVIDADE
    
    TEMP_DATE                       VARCHAR2(30)	:= NULL;
    TEMP_POST_DATE                  VARCHAR2(30)	:= NULL;
	
    I_TXN_COMMENTS					VARCHAR2(250)	:= NULL;		-- COMENTARIO DA TRANSACAO
	I_TXN_EXTERNAL_FLG				CHAR(1)			:= NULL;		-- TRANSACTION EXTERNAL
	I_TXN_TYPE              		VARCHAR2(30)	:= NULL;   		-- TIPO DA TRANSACAO
	I_TXN_SUBTYPE           		VARCHAR2(30)	:= NULL;   		-- SUBTIPO DA TRANSACAO
	I_TXN_CHANNEL           		VARCHAR2(30)	:= NULL;		-- CANAL DA TRANSACAO

	
	-- TRANSACTION FIXED COLUMNS
	FV_TXN_STATUS           		VARCHAR2(30) 	:= 'Queued';	-- STATUS DA TRANSACAO
	FV_BID_FLG              		CHAR(1)			:= 'N';			-- BID_FLG
	FV_OVR_DUP_CHECK_FLG    		CHAR(1)			:= 'N';			-- OVR_DUP_CHECK_FLG
	FV_OVR_PRI_FLAG         		CHAR(1)			:= 'N';			-- OVR_PRI_FLAG
	FV_QUAL_FLG             		CHAR(1)			:= 'N';			-- QUAL_FLG
	FV_UNACC_MINOR_FLG      		CHAR(1)			:= 'N';			-- UNACC_MINOR_FLG
	FV_SOURCE_CD					VARCHAR2(30)	:= 'External';	-- SOURCE
	
	--------------------------- TRANSACTION FOREIGN KEYS ---------------------------
    -- PARENT TRANSACTION COUNTER
    C_PAR_TXN_COUNT                 NUMBER(22)      := 0;			-- TOTAL OF TRANSACTIONS FOUNDED WITH PAR_TXN_ID INPUT (EVERYTHING DIFFERENT THAN '0' RETURN AN ERROR)
	-- PARENT TRANSACTION
	I_PAR_TXN_ID					VARCHAR2(15)	:= NULL;        -- PARENT TRANSACTION ID
	V_PAR_TXN_BI					VARCHAR2(15) 	:= NULL;		-- ID DA ORGANIZACAO DA PARENT TRANSACTION
	V_PAR_TXN_BU					VARCHAR2(100)	:= NULL;		-- NOME DA ORGANIZACAO DA PARENT TRANSACTION
    V_PAR_TXN_NUM					VARCHAR2(30)	:= NULL;		-- NUMERO DA PARENT TRANSACTION
    V_PAR_TXN_MEM_ID	            VARCHAR2(15)	:= NULL;        -- PARENT TRANSACTION MEMBER ID
    V_PAR_TXN_SIGN_CD               VARCHAR2(50)	:= NULL;	    -- PARENT TRANSACTION SIGNATURE CODE
	V_PAR_TXN_PRD_NAME				VARCHAR2(100)	:= NULL;		-- PARENT TRANSACTION PRODUCT NAME
	V_PAR_TXN_PRD_PART				VARCHAR2(50)	:= NULL;		-- PARENT TRANSACTION PRODUCT PART NUMBER
    V_PAR_TXN_CHANNEL               VARCHAR2(30)	:= NULL;		-- PARENT TRANSACTION CHANNEL
	V_PAR_TXN_TYPE					VARCHAR2(30)	:= NULL;		-- PARENT TRANSACTION TYPE
	V_PAR_TXN_SUB_TYPE				VARCHAR2(30)	:= NULL;		-- PARENT TRANSACTION SUB TYPE
	V_PAR_TXN_STATUS				VARCHAR2(30)	:= NULL;		-- PARENT TRANSACTION STATUS
	V_PAR_TXN_SUB_STATUS			VARCHAR2(30)	:= NULL;		-- PARENT TRANSACTION SUB STATUS
	
	-- PROGRAM/ORGANIZATION COLUMNS
	V_PROG_ID               		VARCHAR2(15)	:= NULL;		-- ID DO PROGRAMA
    I_PROG_NAME             		VARCHAR2(50) 	:= NULL;		-- NOME DO PROGRAMA
    V_PROG_BI						VARCHAR2(15)	:= NULL;		-- ID DA ORGANIZACAO DO PROGRAMA
	V_PROG_BU               		VARCHAR2(100)   := NULL;		-- NOME DA ORGANIZACAO DO PROGRAMA
	
	-- MEMBER COLUMNS
	V_MEMBER_ID						VARCHAR2(15)	:= NULL;		-- MEMBER ID
    I_MEMBER_NUMBER         		VARCHAR2(30) 	:= NULL;		-- NUMERO DO MEMBRO
	V_MEMBER_DOC_NUMBER     		VARCHAR2(50)	:= NULL;		-- NUMERO DO DOCUMENTO DO MEMBRO
	V_MEMBER_STATUS					VARCHAR2(30)	:= NULL;		-- STATUS DO MEMBRO
    V_MEMBER_TIER           		VARCHAR2(30) 	:= NULL;		-- TIER DO MEMBRO
    V_STATUS_MEMBER         		VARCHAR2(50) 	:= NULL;		-- STATUS DO MEMBRO
    V_MEMBER_FST_NAME       		VARCHAR2(50) 	:= NULL;    	-- NOME DO MEMBRO
    V_MEMBER_LAST_NAME      		VARCHAR2(50) 	:= NULL;    	-- SOBRENOME DO MEMBRO
	V_MEMBER_PROG_BI				VARCHAR2(15)	:= NULL;		-- ID DA ORGANIZACAO DO PROGRAMA DO MEMBRO
	V_MEMBER_PROG_BU        		VARCHAR2(100)   := NULL;		-- NOME DA ORGANIZACAO DO PROGRAMA DO MEMBRO
	V_MEMBER_PROG_ID				VARCHAR2(15)	:= NULL;		-- MEMBER PROGRAM ID
	V_MEMBER_PROG_NAME      		VARCHAR2(50) 	:= NULL;		-- NOME DO PROGRAMA DO MEMBER
	V_MEMBER_BI						VARCHAR2(15)	:= NULL;		-- MEMBER ORGANIZATION ID
	V_MEMBER_BU						VARCHAR2(100)	:= NULL;		-- MEMBER ORGANIZATION NAME
	
	-- PRODUCT COLUMNS
    V_PROD_ID               		VARCHAR2(15)	:= NULL;		-- ID DO PRODUTO
    I_PROD_NAME          			VARCHAR2(100)	:= NULL;		-- NOME DO PRODUTO
	V_PROD_PART_NUM         		VARCHAR2(20) 	:= NULL;		-- PRODUCT PART#
	V_PROD_BI						VARCHAR2(15)	:= NULL;		-- PRODUCT ORGANIZATION ID
	V_PROD_BU           			VARCHAR2(100)	:= NULL;		-- PRODUCT ORGANIZATION NAME
	V_PROD_PARTNER_ID      			VARCHAR2(15)    := NULL;		-- ID DO PARCEIRO DO PRODUTO
	V_PROD_PARTNER_BI      			VARCHAR2(15)    := NULL;		-- ID DA ORGANIZACAO DO PARCEIRO DO PRODUTO
	V_PROD_PARTNER_BU      			VARCHAR2(100)   := NULL;		-- NOME DA ORGANIZACAO DO PARCEIRO DO PRODUTO
    V_PROD_PARTNER_LOC				VARCHAR2(50)	:= NULL;		-- LOC DO PARCEIRO
	V_PROD_PARTNER_NAME				VARCHAR2(100)	:= NULL;		-- NOME DO PARCEIRO DO PRODUTO
	V_PROD_PARTNER_ALIAS			VARCHAR2(10)   	:= NULL;    	-- ALIAS DO PARCEIRO DO PRODUTO
    V_PROD_PARTNER_PROG_ID          VARCHAR2(15)   	:= NULL;        -- ID DO PROGRAMA DO PARCEIRO DO PRODUTO
    V_PROD_PARTNER_PROG_NAME        VARCHAR2(50) 	:= NULL;		-- NOME DO PROGRAMA DO PARCEIRO DO PRODUTO
    V_PROD_PARTNER_PROG_BI          VARCHAR2(15)    := NULL;		-- ID DA ORGANIZACAO DO PROGRAMA DO PARCEIRO DO PRODUTO
    V_PROD_PARTNER_PROG_BU          VARCHAR2(100)   := NULL;		-- NOME DA ORGANIZACAO DO PROGRAMA DO PARCEIRO DO PRODUTO
    
	-- PARTNER COLUMNS
    V_PARTNER_ID         			VARCHAR2(15)   	:= NULL;		-- ID DO PARCEIRO
	I_PARTNER_ALIAS         		VARCHAR2(10)   	:= NULL;    	-- ALIAS DO PARCEIRO
    V_PARTNER_NAME     				VARCHAR2(100)  	:= NULL;    	-- NOME DO PARCEIRO
    V_PARTNER_LOC      				VARCHAR2(50)   	:= NULL;    	-- LOC DO PARCEIRO
	V_PARTNER_BU_ID					VARCHAR2(15)   	:= NULL;		-- ID DA ORGANIZACAO DO PARCEIRO
    V_PARTNER_BU       				VARCHAR2(50)   	:= NULL;    	-- ORGANIZACAO DO PARCEIRO
    
	-- TRANSACTION OWNER
	V_OWNER_ID						VARCHAR2(15)   	:= NULL;		-- ID DO USUARIO
	I_OWNER_LOGIN					VARCHAR(50)		:= NULL;		-- LOGIN DO USUARIO
	
	-- TRANSACTION POINT
	V_POINT_ID              		VARCHAR2(15)    := NULL;		-- ID DO PONTO
	V_POINT_TYPE            		VARCHAR2(30)    := NULL;		-- TIPO DO PONTO
	V_POINT_NAME            		VARCHAR2(75)    := NULL;		-- NOME DO PONTO
	I_POINT_DISPLAY_NAME			VARCHAR2(30)	:= NULL;		-- NOME DE EXIBICAO DO PONTO
	FV_POINT_OBJECT         		VARCHAR2(30)    := 'Member';	-- OBJETO DO PONTO
	V_POINT_PROG_NAME       		VARCHAR2(50)    := NULL;		-- NOME DO PROGRAMA
	V_POINT_PROG_ID					VARCHAR2(15)    := NULL;		-- ID DO PROGRAMA DO PONTO
	V_POINT_PROG_BI         		VARCHAR2(15)    := NULL;		-- ID DA ORGANIZACAO DO PONTO
	V_POINT_PROG_BU         		VARCHAR2(100)   := NULL;		-- NOME DA ORGANIZACAO DO PONTO
	/*
	V_POINT_PROMO_BI        		VARCHAR2(15)	:= NULL;		-- NUNCA USADO
	V_POINT_PROMO_BU				VARCHAR2(100)	:= NULL;		-- NUNCA USADO
	V_POINT_PROMO_NAME      		VARCHAR2(50)	:= NULL;		-- NUNCA USADO
	V_POINT_PROMO_PROG_BI   		VARCHAR2(15)	:= NULL;		-- NUNCA USADO
	V_POINT_PROMO_PROG_BU   		VARCHAR2(100)	:= NULL;		-- NUNCA USADO
	V_POINT_PROMO_PROG_NAME 		VARCHAR2(50)	:= NULL;		-- NUNCA USADO
	*/

	--------------------------- TRANSACTION EXTENDED COLUMNS ---------------------------
	-- EIM COLUMNS
	EIM_TXNX_ROW_ID         		NUMBER(10)   	:= NULL;
    EIM_TXNX_IF_ROW_STAT    		VARCHAR2(30) 	:= NULL;
    EIM_TXNX_ROW_BATCH_NUM  		NUMBER(10)   	:= 1;
    EIM_TXNX_ROW_BATCH_BKP  		NUMBER(10)   	:= NULL;
	
	-- SIGNATURE COLUMNS
	V_SIGNATURE_ID					VARCHAR2(15) 	:= NULL;	-- SIGNATURE ID
	I_SIGNATURE_CODE				VARCHAR2(50)	:= NULL;	-- SIGNATURE CODE
	V_SIGNATURE_STATUS				VARCHAR2(30)	:= NULL;	-- SIGNATURE STATUS
	V_SIGNATURE_PLAN_ID				VARCHAR2(15)	:= NULL;	-- SIGNATURE PLAN ID
    V_SIGNATURE_MEM_NUMBER	        VARCHAR2(30) 	:= NULL;    -- SIGNATURE MEMBER NUMBER

    -- OTHER COLUMNS
	I_EXTERNAL_TXN_ID				VARCHAR2(200) 	:= NULL;	-- EXTERNAL ID
	------------------------------------------------------------------------------------
	
BEGIN
    -- CLEAR EIM_LOY_TXN TABLE
    DELETE
    FROM SIEBEL.EIM_LOY_TXN
    WHERE IF_ROW_BATCH_NUM IN (
        '25000',
        '99999'
    );

    -- CLEAR EIM_LOY_TXNDTL TABLE
    DELETE
    FROM SIEBEL.EIM_LOY_TXNDTL
    WHERE IF_ROW_BATCH_NUM = '25000';
    
	-------------------------- OPENING ERROR FILE FOR LOGGING PROCESS IN APPEND MODE ---------------------------
    BEGIN
        F_FILE_ERROR := UTL_FILE.FOPEN(F_LOG_DIR, F_LOG_FILE, 'a');
    EXCEPTION
		WHEN OTHERS THEN
            V_ERROR_CODE := '001';
			V_ERROR_MESSAGE := '[CouldNotOpenLOGFile]' || '   -   ' || 'Could not open LOG File.';
        RAISE FILE_EXCEP;
    END;
    UTL_FILE.PUT_LINE(F_FILE_ERROR, '=========================================================================================');
    UTL_FILE.PUT_LINE(F_FILE_ERROR, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[File]' || '   -   ' || 'Opening Input File "' || IN_FILE || '"');
	
    ----------------------------------------- STARTING FILE HEADER VALIDATION -----------------------------------------
    -- FILE LEVEL VALIDATION [FILE EXISTANCE]
    BEGIN
        F_FILE := UTL_FILE.FOPEN(F_DIRECTORY, IN_FILE, 'r');
    EXCEPTION
		WHEN OTHERS THEN
            V_ERROR_CODE := '002';
			V_ERROR_MESSAGE := '[CouldNotOpenFile]' || '   -   ' || 'Input File is not available.';
			
            RAISE FILE_EXCEP;
    END;
	UTL_FILE.PUT_LINE(F_FILE_ERROR, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[File]' || '   -   ' || 'Input File "' || IN_FILE || '" opened successfully!');
	UTL_FILE.PUT_LINE(F_FILE_ERROR, '=========================================================================================');
	UTL_FILE.PUT_LINE(F_FILE_ERROR, '');
	
    ----------------------------------------- HEADER VALIDATIONS -----------------------------------------
    -- GET THE HEADER LINE
	UTL_FILE.PUT_LINE(F_FILE_ERROR, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[Header]' || '   -   ' || 'Reading the input file "Header"...');
    UTL_FILE.GET_LINE(F_FILE, F_LINE);
	UTL_FILE.PUT_LINE(F_FILE_ERROR, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[Header]' || '   -   ' || 'Input file "Header" was successfully loaded.');
	
    -- VALIDATE HEADER LAYOUT
    UTL_FILE.PUT_LINE(F_FILE_ERROR, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[Header]' || '   -   ' || 'Validating input file "Header" layout.');
    IF REPLACE(F_LINE, CHR(13), '') <> 'MEMBER_NUMBER;PROGRAM_NAME;PARTNER_ALIAS;TRANSACTION_DATE;ACTIVITY_DATE;PARENT_SIGNATURE_CODE;PARENT_TRANSACTION_ID;TRANSACTION_PRODUCT_NAME;TRANSACTION_AMOUNT;TRANSACTION_TYPE;TRANSACTION_SUBTYPE;TRANSACTION_CHANNEL;TRANSACTION_COMMENTS;TRANSACTION_POINT_NAME;OWNER_LOGIN;EXTERNAL;EXTERNAL_PARTNER_ID' THEN
        V_ERROR_CODE := '100';
        V_ERROR_MESSAGE := TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[Header]' || '   -   ' || '[IncorrectLayout]' || '   -   ' || 'File Layout is not Valid. Header Line Incorrect!';
		
        RAISE FILE_EXCEP;

    END IF;
	
    UTL_FILE.PUT_LINE(F_FILE_ERROR, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[Header]' || '   -   ' || '[CorrectLayout]' || '   -   ' || 'The input file "Header" layout was successfuly validated!');
    
    ----------------------------------------- HEADER COLUMNS VALIDATIONS -----------------------------------------
    -- GET POSITION OF EACH HEADER COLUMN
	LOOP
        -- SET 'F_NUM_COL_' VARIABLES WITH THEIR RESPECTIVES COLUMNS NUMBERS
		SELECT
			REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_QTD_HDR_COL), ';'),CHR(13),'')
		INTO F_HDR_COL_NAME
		FROM DUAL;
        
        -- SET COLUMN VARIABLES
		CASE F_HDR_COL_NAME
            WHEN 'MEMBER_NUMBER' THEN F_NUM_COL_MBR_NUM := F_QTD_HDR_COL;
            WHEN 'PROGRAM_NAME' THEN F_NUM_COL_PROG_NAME := F_QTD_HDR_COL;
            WHEN 'PARTNER_ALIAS' THEN F_NUM_COL_PAR_ALIAS := F_QTD_HDR_COL;
            WHEN 'TRANSACTION_DATE' THEN F_NUM_COL_TXN_DT := F_QTD_HDR_COL;
            WHEN 'ACTIVITY_DATE' THEN F_NUM_COL_ACT_DT := F_QTD_HDR_COL;
            WHEN 'PARENT_SIGNATURE_CODE' THEN F_NUM_COL_SIGN_CODE := F_QTD_HDR_COL;
            WHEN 'PARENT_TRANSACTION_ID' THEN F_NUM_COL_PAR_TXN_ID := F_QTD_HDR_COL;
            WHEN 'TRANSACTION_PRODUCT_NAME' THEN F_NUM_COL_TXN_PRD_NAME := F_QTD_HDR_COL;
            WHEN 'TRANSACTION_AMOUNT' THEN F_NUM_COL_TXN_AMT := F_QTD_HDR_COL;
            WHEN 'TRANSACTION_TYPE' THEN F_NUM_COL_TXN_TYPE := F_QTD_HDR_COL;
            WHEN 'TRANSACTION_SUBTYPE' THEN F_NUM_COL_TXN_SUBTYPE := F_QTD_HDR_COL;
            --WHEN 'TRANSACTION_STATUS' THEN F_NUM_COL_TXN_STATUS := F_QTD_HDR_COL;
            WHEN 'TRANSACTION_CHANNEL' THEN F_NUM_COL_TXN_CHANNEL := F_QTD_HDR_COL;
            WHEN 'TRANSACTION_COMMENTS' THEN F_NUM_COL_TXN_COMMENTS := F_QTD_HDR_COL;
            WHEN 'TRANSACTION_POINT_NAME' THEN F_NUM_COL_TXN_P_NAME := F_QTD_HDR_COL;
            WHEN 'OWNER_LOGIN' THEN F_NUM_COL_OWNER := F_QTD_HDR_COL;
            WHEN 'EXTERNAL' THEN F_NUM_COL_EXTNL := F_QTD_HDR_COL;
            WHEN 'EXTERNAL_PARTNER_ID' THEN F_NUM_COL_EXTNL_P_ID := F_QTD_HDR_COL;
            ELSE GOTO ENDOFHEADER;
        END CASE;
        F_QTD_HDR_COL := F_QTD_HDR_COL + 1;
    END LOOP;
	
    <<ENDOFHEADER>>
    V_ERROR_MESSAGE := NULL;	
	
    -- CLOSE INPUT FILE
	UTL_FILE.PUT_LINE(F_FILE_ERROR, '');
    BEGIN
        UTL_FILE.FCLOSE(F_FILE);
    EXCEPTION
		WHEN OTHERS THEN
            IF (V_ERROR_CODE IS NOT NULL) THEN
                V_ERROR_CODE := V_ERROR_CODE || '-';
                V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
            END IF;
            V_ERROR_CODE := V_ERROR_CODE || '003';
            V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[CouldNotCloseFile]' || '   -   ' || 'Input File couldn`t be closed.';

            RAISE FILE_EXCEP;
    END;
	----------------------------------------- END OF FILE HEADER VALIDATION -----------------------------------------
	
	----------------------------------------- STARTING RECORDS VALIDATIONS -----------------------------------------
    BEGIN
        -- UPDATE 'F_LINE_NUMBER' COUNT TO SKIP THE HEADER LINE
		F_LINE_NUMBER 			:= F_LINE_NUMBER + 1;

        -- REOPEN THE INPUT FILE
        F_FILE := UTL_FILE.FOPEN(F_DIRECTORY, IN_FILE, 'r');
		
        -- GET THE HEADER LINE
		UTL_FILE.GET_LINE(F_FILE, F_LINE);
		
        -- UPDATE THE LOG FILE WITH SOME USEFUL INFORMATIONS
		UTL_FILE.PUT_LINE(F_FILE_ERROR, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[Input Line(s)]' || '   -   ' || 'Reading file inputs...');
        UTL_FILE.PUT_LINE(F_FILE_ERROR, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[Input Line(s)]' || '   -   ' || 'The Inputs Line(s) returned the following errors:');
        UTL_FILE.PUT_LINE(F_FILE_ERROR, '');

        -- READ THE INPUT FILE LINES
        LOOP
            -- CLEAR THE 'F_LINE' VARIABLE TO READ NEXT LINE
            F_LINE := '';

			--------------------------- CLEAR ALL VARIABLES ---------------------------
            V_ERROR_MESSAGE         := '';
            V_ERROR_CODE            := '';
            V_EIM_FINAL             := NULL;
            C_REC_COUNT_COMMIT      := C_REC_COUNT_COMMIT + 1;

            --------------------------- TRANSACTION EIM SYSTEM COLUMNS ---------------------------
            EIM_ROW_ID              := NULL;
            EIM_IF_ROW_STAT         := 'FOR_IMPORT';
            EIM_ROW_BATCH_NUM       := 25000;
            EIM_ROW_BATCH_BKP       := NULL;

            --------------------------- TRANSACTION COLUMNS ---------------------------
            -- TRANSACTION VARIABLE COLUMNS
			I_TXN_AMOUNT           	:= 0;			-- TOTAL DE MILHAS A SEREM ACUMULADAS NA TRANSACAO
			V_TXN_POINTS            := 0;			-- TOTAL DE PONTOS
			I_TXN_DATE              := NULL;		-- DATA DA TRANSACAO
			I_TXN_POST_DATE			:= NULL;		-- DATA DA ATIVIDADE
			I_TXN_COMMENTS			:= NULL;		-- COMENTARIO DA TRANSACAO
			I_TXN_EXTERNAL_FLG		:= NULL;		-- TRANSACTION EXTERNAL
			I_TXN_TYPE              := NULL;   		-- TIPO DA TRANSACAO
			I_TXN_SUBTYPE           := NULL;   		-- SUBTIPO DA TRANSACAO
			I_TXN_CHANNEL           := NULL;		-- CANAL DA TRANSACAO

			--------------------------- TRANSACTION FOREIGN KEYS ---------------------------
            -- PARENT TRANSACTION COUNTERS
            C_PAR_TXN_COUNT         := 0;			-- TOTAL OF TRANSACTIONS FOUNDED WITH PAR_TXN_ID INPUT (EVERYTHING DIFFERENT THAN '0' RETURN AN ERROR)
			-- PARENT TRANSACTION
			I_PAR_TXN_ID			:= NULL;        -- PARENT TRANSACTION ID
			V_PAR_TXN_BI			:= NULL;		-- ID DA ORGANIZACAO DA PARENT TRANSACTION
			V_PAR_TXN_BU			:= NULL;		-- NOME DA ORGANIZACAO DA PARENT TRANSACTION
			V_PAR_TXN_NUM			:= NULL;		-- NUMERO DA PARENT TRANSACTION
            V_PAR_TXN_MEM_ID        := NULL;        -- PARENT TRANSACTION MEMBER ID
            V_PAR_TXN_SIGN_CD       := NULL;	    -- PARENT TRANSACTION SIGNATURE CODE
			V_PAR_TXN_PRD_NAME		:= NULL;		-- PARENT TRANSACTION PRODUCT NAME
			V_PAR_TXN_PRD_PART		:= NULL;		-- PARENT TRANSACTION PRODUCT PART NUMBER
            V_PAR_TXN_CHANNEL       := NULL;		-- PARENT TRANSACTION CHANNEL
			V_PAR_TXN_TYPE			:= NULL;		-- PARENT TRANSACTION TYPE
			V_PAR_TXN_SUB_TYPE		:= NULL;		-- PARENT TRANSACTION SUB TYPE
			V_PAR_TXN_STATUS		:= NULL;		-- PARENT TRANSACTION STATUS
			V_PAR_TXN_SUB_STATUS	:= NULL;		-- PARENT TRANSACTION SUB STATUS

			-- PROGRAM/ORGANIZATION COLUMNS
			V_PROG_ID               := NULL;		-- ID DO PROGRAMA
			I_PROG_NAME             := NULL;		-- NOME DO PROGRAMA
			V_PROG_BI				:= NULL;		-- ID DA ORGANIZACAO DO PROGRAMA
			V_PROG_BU               := NULL;		-- NOME DA ORGANIZACAO DO PROGRAMA

			-- MEMBER COLUMNS
			V_MEMBER_ID				:= NULL;		-- MEMBER ID
			I_MEMBER_NUMBER         := NULL;		-- NUMERO DO MEMBRO
			V_MEMBER_DOC_NUMBER     := NULL;		-- NUMERO DO DOCUMENTO DO MEMBRO
			V_MEMBER_STATUS			:= NULL;		-- STATUS DO MEMBRO
			V_MEMBER_TIER           := NULL;		-- TIER DO MEMBRO
			V_STATUS_MEMBER         := NULL;		-- STATUS DO MEMBRO
			V_MEMBER_FST_NAME       := NULL;    	-- NOME DO MEMBRO
			V_MEMBER_LAST_NAME      := NULL;    	-- SOBRENOME DO MEMBRO
			V_MEMBER_PROG_BI		:= NULL;		-- ID DA ORGANIZACAO DO PROGRAMA DO MEMBRO
			V_MEMBER_PROG_BU        := NULL;		-- NOME DA ORGANIZACAO DO PROGRAMA DO MEMBRO
			V_MEMBER_PROG_ID		:= NULL;		-- MEMBER PROGRAM ID
			V_MEMBER_PROG_NAME      := NULL;		-- NOME DO PROGRAMA DO MEMBER
			V_MEMBER_BI				:= NULL;		-- MEMBER ORGANIZATION ID
			V_MEMBER_BU				:= NULL;		-- MEMBER ORGANIZATION NAME

			-- PRODUCT COLUMNS
			V_PROD_ID               := NULL;		-- ID DO PRODUTO
			I_PROD_NAME          	:= NULL;		-- NOME DO PRODUTO
			V_PROD_PART_NUM         := NULL;		-- PRODUCT PART#
			V_PROD_BI				:= NULL;		-- PRODUCT ORGANIZATION ID
			V_PROD_BU           	:= NULL;		-- PRODUCT ORGANIZATION NAME
			V_PROD_PARTNER_ID      	:= NULL;		-- ID DO PARCEIRO DO PRODUTO
			V_PROD_PARTNER_BI      	:= NULL;		-- ID DA ORGANIZACAO DO PARCEIRO DO PRODUTO
			V_PROD_PARTNER_LOC		:= NULL;		-- LOC DO PARCEIRO
			V_PROD_PARTNER_BU      	:= NULL;		-- NOME DA ORGANIZACAO DO PARCEIRO DO PRODUTO
			V_PROD_PARTNER_NAME		:= NULL;		-- NOME DO PARCEIRO DO PRODUTO
			V_PROD_PARTNER_ALIAS	:= NULL;    	-- ALIAS DO PARCEIRO DO PRODUTO
            V_PROD_PARTNER_PROG_ID  := NULL;        -- ID DO PROGRAMA DO PARCEIRO DO PRODUTO
            V_PROD_PARTNER_PROG_NAME:= NULL;		-- NOME DO PROGRAMA DO PARCEIRO DO PRODUTO
            V_PROD_PARTNER_PROG_BI  := NULL;		-- ID DA ORGANIZACAO DO PROGRAMA DO PARCEIRO DO PRODUTO
            V_PROD_PARTNER_PROG_BU  := NULL;		-- NOME DA ORGANIZACAO DO PROGRAMA DO PARCEIRO DO PRODUTO

			-- PARTNER COLUMNS
			V_PARTNER_ID         	:= NULL;		-- ID DO PARCEIRO
			I_PARTNER_ALIAS         := NULL;    	-- ALIAS DO PARCEIRO
			V_PARTNER_NAME     		:= NULL;    	-- NOME DO PARCEIRO
			V_PARTNER_LOC      		:= NULL;    	-- LOC DO PARCEIRO
			V_PARTNER_BU_ID			:= NULL;		-- ID DA ORGANIZACAO DO PARCEIRO
			V_PARTNER_BU       		:= NULL;    	-- ORGANIZACAO DO PARCEIRO

			-- TRANSACTION OWNER
			V_OWNER_ID				:= NULL;		-- ID DO USUARIO
			I_OWNER_LOGIN			:= NULL;		-- LOGIN DO USUARIO

			-- TRANSACTION POINT
			V_POINT_ID              := NULL;		-- ID DO PONTO
			V_POINT_TYPE            := NULL;		-- TIPO DO PONTO
			V_POINT_NAME            := NULL;		-- NOME DO PONTO
			I_POINT_DISPLAY_NAME	:= NULL;		-- NOME DE EXIBICAO DO PONTO
			V_POINT_PROG_ID			:= NULL;		-- ID DO PROGRAMA DO PONTO
			V_POINT_PROG_NAME       := NULL;		-- NOME DO PROGRAMA
			V_POINT_PROG_BI         := NULL;		-- ID DA ORGANIZACAO DO PONTO
			V_POINT_PROG_BU         := NULL;		-- NOME DA ORGANIZACAO DO PONTO
			/*
			V_POINT_PROMO_BI        := NULL;		-- NUNCA USADO
			V_POINT_PROMO_BU		:= NULL;		-- NUNCA USADO
			V_POINT_PROMO_NAME      := NULL;		-- NUNCA USADO
			V_POINT_PROMO_PROG_BI   := NULL;		-- NUNCA USADO
			V_POINT_PROMO_PROG_BU   := NULL;		-- NUNCA USADO
			V_POINT_PROMO_PROG_NAME := NULL;		-- NUNCA USADO
			*/
			
			--------------------------- TRANSACTION EXTENDED COLUMNS ---------------------------
            -- EIM COLUMNS
            EIM_TXNX_ROW_ID         := NULL;
            EIM_TXNX_IF_ROW_STAT    := 'FOR_IMPORT';
            EIM_TXNX_ROW_BATCH_NUM  := 1;
            EIM_TXNX_ROW_BATCH_BKP  := NULL;
            -- SIGNATURE COLUMNS
			V_SIGNATURE_ID			:= NULL;		-- SIGNATURE ID
			I_SIGNATURE_CODE		:= NULL;		-- SIGNATURE CODE
			V_SIGNATURE_STATUS		:= NULL;		-- SIGNATURE STATUS
			V_SIGNATURE_PLAN_ID		:= NULL;		-- SIGNATURE PLAN ID
            V_SIGNATURE_MEM_NUMBER	:= NULL;        -- SIGNATURE MEMBER NUMBER
            -- OTHER COLUMNS
			I_EXTERNAL_TXN_ID		:= NULL;		-- EXTERNAL ID
			------------------------------------------------------------------------------------
			
			
			----------------------------------------- READ FILE RECORD -----------------------------------------
			-- GET LINE RECORDS
            UTL_FILE.GET_LINE(F_FILE, F_LINE);
            
            -- SET INPUT VARIABLES BASED ON LINE RECORDS
			SELECT
                REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_MBR_NUM), ';'),CHR(13),''),
                REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_PROG_NAME), ';'),CHR(13),''),
                REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_PAR_ALIAS), ';'),CHR(13),''),
                TO_DATE(REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_TXN_DT), ';'),CHR(13),''), 'DD/MM/YYYY HH24:MI:SS'),
                TO_DATE(REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_ACT_DT), ';'),CHR(13),''), 'DD/MM/YYYY HH24:MI:SS'),
                REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_SIGN_CODE), ';'),CHR(13),''),
                REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_PAR_TXN_ID), ';'),CHR(13),''),
                REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_TXN_PRD_NAME), ';'),CHR(13),''),
                REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_TXN_AMT), ';'),CHR(13),''),
                REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_TXN_TYPE), ';'),CHR(13),''),
                REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_TXN_SUBTYPE), ';'),CHR(13),''),
                --REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_TXN_STATUS), ';'),CHR(13),''),
                REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_TXN_CHANNEL), ';'),CHR(13),''),
                REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_TXN_COMMENTS), ';'),CHR(13),''),
                REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_TXN_P_NAME), ';'),CHR(13),''),
                REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_OWNER), ';'),CHR(13),''),
                REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_EXTNL), ';'),CHR(13),''),
                REPLACE(LTRIM(REGEXP_SUBSTR (';' || F_LINE, ';[^;]*', 1, F_NUM_COL_EXTNL_P_ID), ';'),CHR(13),'')
            INTO
                I_MEMBER_NUMBER,
                I_PROG_NAME,
                I_PARTNER_ALIAS,
                I_TXN_DATE,
                I_TXN_POST_DATE,
                I_SIGNATURE_CODE,
                I_PAR_TXN_ID,
                I_PROD_NAME,
                I_TXN_AMOUNT,
                I_TXN_TYPE,
                I_TXN_SUBTYPE,
                --FV_TXN_STATUS,
                I_TXN_CHANNEL,
                I_TXN_COMMENTS,
                I_POINT_DISPLAY_NAME,
                I_OWNER_LOGIN,
                I_TXN_EXTERNAL_FLG,
                I_EXTERNAL_TXN_ID
            FROM DUAL;

			V_TXN_POINTS := I_TXN_AMOUNT;
			
			----------------------------------------- START OF VALIDATIONS -----------------------------------------
			
			----------------------------------------- MEMBER VALIDATIONS -----------------------------------------
			-- VALIDATE MEMBER
			BEGIN
				-- MEMBER PROGRAM/ORGANIZATION/ACTIVE TIER
				SELECT DISTINCT
					MBR.ROW_ID              AS "MBR_ID",
					--MBR.MEM_NUM             AS "MBR_NUM",
					CON.SOC_SECURITY_NUM    AS "MBR_DOCUMENT",
					MBR.STATUS_CD           AS "MBR_STATUS",
					CON.FST_NAME            AS "MBR_NAME",
					CON.LAST_NAME           AS "MBR_LAST_NAME",
					MBR.PROGRAM_ID          AS "MBR_PROG_ID",
					PRG.NAME                AS "MBR_PROG_NAME",
					MBR.BU_ID		        AS "MBR_PROG_BI",
					BUP.NAME                AS "MBR_PROG_BU_NAME",
					MBR.BU_ID               AS "MBR_BU_ID",
					BUM.NAME                AS "MBR_BU_NAME",
					TIER.NAME               AS "MBR_ACTIVE_TIER"
				INTO
                    V_MEMBER_ID,
                    V_MEMBER_DOC_NUMBER,
                    V_MEMBER_STATUS,
                    V_MEMBER_FST_NAME,
                    V_MEMBER_LAST_NAME,
                    V_MEMBER_PROG_ID,
                    V_MEMBER_PROG_NAME,
                    V_MEMBER_PROG_BI,
                    V_MEMBER_PROG_BU,
					V_MEMBER_BI,
					V_MEMBER_BU,
                    V_MEMBER_TIER
				FROM SIEBEL.S_LOY_MEMBER MBR
				LEFT JOIN SIEBEL.S_CONTACT CON ON CON.ROW_ID = MBR.PR_CON_ID                -- CONTACT
				LEFT JOIN SIEBEL.S_LOY_MEM_TIER MTIER ON MTIER.MEMBER_ID = MBR.ROW_ID       -- TIER
				LEFT JOIN SIEBEL.S_LOY_TIER TIER ON TIER.ROW_ID = MTIER.TIER_ID             -- TIER
				LEFT JOIN SIEBEL.S_LOY_PROGRAM PRG ON PRG.ROW_ID = MBR.PROGRAM_ID           -- PROGRAM
				LEFT JOIN SIEBEL.S_BU BUM ON BUM.ROW_ID = MBR.BU_ID
				LEFT JOIN SIEBEL.S_BU BUP ON BUP.ROW_ID = PRG.BU_ID
				WHERE 1=1
				AND MBR.MEM_NUM = I_MEMBER_NUMBER   -- MEMBER NUMBER
				AND MTIER.ACTIVE_FLG = 'Y';     	-- MEMBER ACTIVE TIER

			EXCEPTION
				WHEN NO_DATA_FOUND THEN
					IF (V_ERROR_CODE IS NOT NULL) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '-';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                    END IF;
					V_ERROR_CODE := V_ERROR_CODE || '150';
					V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[MemberNotFound]' || '   -   ' || 'Member with Member Number "' || I_MEMBER_NUMBER || '" was not found.';

                    GOTO VERIFY_PROGRAM;

			END;

            <<VERIFY_PROGRAM>>

            ----------------------------------------- PROGRAM VALIDATIONS -----------------------------------------
			-- VALIDATE PROGRAM ORGANIZATION
			BEGIN
				SELECT
					PRG.ROW_ID  AS "PROG_ID",
					--PRG.NAME    AS "PRG_NAME",
					BU.ROW_ID   AS "BU_ID",
					BU.NAME     AS "ORG_NAME"
				INTO V_PROG_ID, V_PROG_BI, V_PROG_BU
				FROM SIEBEL.S_LOY_PROGRAM PRG
				INNER JOIN SIEBEL.S_BU BU ON BU.ROW_ID = PRG.BU_ID
				WHERE 1=1
				AND PRG.NAME = I_PROG_NAME; -- PROGRAM NAME

			EXCEPTION
				WHEN NO_DATA_FOUND THEN
                    
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '-';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                    END IF;
					V_ERROR_CODE := V_ERROR_CODE || '200';
					V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[ProgramNotFound]' || '   -   ' || 'The Program Name "' || I_PROG_NAME || '" was not found.';
                    
                    GOTO VERIFY_PARTNER;
                    
				WHEN TOO_MANY_ROWS THEN
                    
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '-';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                    END IF;
                    V_ERROR_CODE := V_ERROR_CODE || '201';
					V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[MoreThanOneProgFound]' || '   -   ' || 'The Program Name: "' || I_PROG_NAME || '" returned more than one line.';
                    
                    GOTO VERIFY_PARTNER;
                    
			END;
            
            <<VERIFY_PARTNER>>
            
            ----------------------------------------- PARTNER VALIDATIONS -----------------------------------------
			BEGIN
				 -- VALIDATE PARTNER ORGANIZATION
				SELECT
					PAR.ROW_ID			AS "PARTNER_ID",
					PAR.NAME			AS "PARTNER_NAME",
					--PAR.ALIAS_NAME      AS "PARTNER_ALIAS",
					PAR.LOC				AS "PARTNER_LOC",
					PAR.BU_ID			AS "PARTNER_BU_ID",
					BU.NAME				AS "PARTNER_BU"
				INTO V_PARTNER_ID, V_PARTNER_NAME, V_PARTNER_LOC, V_PARTNER_BU_ID, V_PARTNER_BU
				FROM SIEBEL.S_LOY_PROGRAM PROG
				INNER JOIN SIEBEL.S_LOY_PROG_ORG PORG ON PORG.PROG_ID = PROG.ROW_ID
				INNER JOIN SIEBEL.S_ORG_EXT PAR ON PAR.ROW_ID = PORG.ORG_EXT_ID
				INNER JOIN SIEBEL.S_BU BU ON BU.ROW_ID = PAR.BU_ID
				WHERE 1=1
				AND PROG.ROW_ID = V_PROG_ID 			-- PROGRAM ID
				AND PAR.ALIAS_NAME = I_PARTNER_ALIAS;	-- PARTNER ALIAS

			EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '-';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                    END IF;
                    IF (INSTR(V_ERROR_CODE,'200', 1, 1) > 0) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '250';
					    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[PartnerProgNotFound]' || '   -   ' || 'The Partner Alias "' || I_PARTNER_ALIAS || '" was not found for the Program Name "' || I_PROG_NAME || '".';
                    ELSE
                        V_ERROR_CODE := V_ERROR_CODE || '251';
					    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[PartnerNotFound]' || '   -   ' || 'The Partner Alias "' || I_PARTNER_ALIAS || '" was not found.';
					END IF;
					
                    GOTO VERIFY_SIGNATURE;
                    
				WHEN TOO_MANY_ROWS THEN
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '-';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                    END IF;
                    
					V_ERROR_CODE := V_ERROR_CODE || '252';
					V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[MoreThanOnePartnerFound]' || '   -   ' || 'The Partner Alias "' || I_PARTNER_ALIAS || '" returned more than one line for the Program Name "' || I_PROG_NAME || '".';
					
                    GOTO VERIFY_SIGNATURE;
                    
			END;
            
            <<VERIFY_SIGNATURE>>
            
            ----------------------------------------- SIGNATURE VALIDATIONS -----------------------------------------
			BEGIN
				SELECT
					SIG.ROW_ID              AS "SIGN_ID",
					--SIG.X_ATTRIB_12         AS "SIGN_CODE_SIGNING",
					SIG.X_ATTRIB_03         AS "SIGN_STATUS",
					SIG.X_ATTRIB_04         AS "SIGN_PLAN_ID",
                    MBR.MEM_NUM             AS "SIGN_MEM_NUM"
				INTO V_SIGNATURE_ID, V_SIGNATURE_STATUS, V_SIGNATURE_PLAN_ID, V_SIGNATURE_MEM_NUMBER
				FROM SIEBEL.CX_SIGN_MEMBER SIG
                LEFT JOIN SIEBEL.S_LOY_MEMBER MBR ON MBR.ROW_ID = SIG.X_ATTRIB_01
				WHERE SIG.X_ATTRIB_12 = I_SIGNATURE_CODE; -- SIGNATURE CODE
			EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '-';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                    END IF;
					V_ERROR_CODE := V_ERROR_CODE || '300';
					V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[SignNotFound]' || '   -   ' || 'The Signature Code "' || I_SIGNATURE_CODE || '" was not found.';
					
                    GOTO VERIFY_PAR_TXN;
                    
				WHEN TOO_MANY_ROWS THEN
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '-';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                    END IF;
					V_ERROR_CODE := V_ERROR_CODE || '301';
					V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[MoreThanOneSignFound]' || '   -   ' || 'The Signature Code "' || I_SIGNATURE_CODE || '" returned more than one line.';
					
                    GOTO VERIFY_PAR_TXN;
                    
			END;
            
            <<VERIFY_PAR_TXN>>
            
            ----------------------------------------- PARENT TRANSACTION VALIDATIONS -----------------------------------------
			BEGIN
				-- PARENT TRANSACTION
				SELECT
                    TXN.MEMBER_ID       AS "TXN_MEM_ID",
                    TXN.BU_ID           AS "TXN_BU_ID",
                    BU.NAME             AS "TXN_BU",
                    TXN.TXN_NUM         AS "TXN_NUM",
                    CASE
                        WHEN SIGA.X_ATTRIB_12 IS NOT NULL THEN
                            SIGA.X_ATTRIB_12
                        ELSE    
                            SIGM.X_ATTRIB_12
                    END                 AS "TXN_SIGN_CD",
                    PRD.NAME            AS "TXN_PRD_NAME",
                    PRD.PART_NUM        AS "TXN_PRD_PART",
                    TXN.TXN_CHANNEL_CD  AS "TXN_CHANNEL",
                    TXN.TYPE_CD         AS "TXN_TYPE",
                    TXN.SUB_TYPE_CD     AS "TXN_SUB_TYPE",
                    TXN.STATUS_CD       AS "TXN_STATUS",
                    TXN.SUB_STATUS_CD   AS "TXN_SUB_STATUS"
                INTO
                    V_PAR_TXN_MEM_ID,
                    V_PAR_TXN_BI,
                    V_PAR_TXN_BU,
                    V_PAR_TXN_NUM,
                    V_PAR_TXN_SIGN_CD,
                    V_PAR_TXN_PRD_NAME,
                    V_PAR_TXN_PRD_PART,
                    V_PAR_TXN_CHANNEL,
                    V_PAR_TXN_TYPE,
                    V_PAR_TXN_SUB_TYPE,
                    V_PAR_TXN_STATUS,
                    V_PAR_TXN_SUB_STATUS
                FROM SIEBEL.S_LOY_TXN TXN
                LEFT JOIN SIEBEL.S_LOY_TXN_X TXNX ON TXNX.PAR_ROW_ID = TXN.ROW_ID
                LEFT JOIN SIEBEL.S_PROD_INT PRD ON PRD.ROW_ID = TXN.PROD_ID
                LEFT JOIN SIEBEL.S_BU BU ON BU.ROW_ID = TXN.BU_ID
                LEFT JOIN SIEBEL.CX_PAG_CLUB_TXN PTXN ON PTXN.TXN_ID = TXN.ROW_ID
                LEFT JOIN SIEBEL.CX_PAG_CLUB PAGM ON PAGM.X_ATTRIB_07 = TXN.ROW_ID
                LEFT JOIN SIEBEL.CX_PAG_CLUB PAGA ON PAGA.ROW_ID = PTXN.PAR_ROW_ID
                LEFT JOIN SIEBEL.CX_SIGN_MEMBER SIGM ON SIGM.ROW_ID = TXNX.X_SIGN_MEMBER_ID OR SIGM.ROW_ID = PAGM.X_ATTRIB_01    -- MONTHLY SIGNATURE
                LEFT JOIN SIEBEL.CX_SIGN_MEMBER SIGA ON SIGA.ROW_ID = TXNX.X_SIGN_MEMBER_ID OR SIGA.ROW_ID = PAGA.X_ATTRIB_01    -- ANNUAL SIGNATURE
                WHERE 1=1
                AND TXN.ROW_ID = I_PAR_TXN_ID; -- ROW_ID DA TXN DE RECORRENCIA
			EXCEPTION
				WHEN NO_DATA_FOUND THEN
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '-';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                    END IF;
					V_ERROR_CODE := V_ERROR_CODE || '350';
					V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[ParTxnNotFound]' || '   -   ' || 'The Parent Transaction Id "' || I_PAR_TXN_ID || '" was not found.';
					
                    GOTO VERIFY_PRODUCT;
                    
                WHEN TOO_MANY_ROWS THEN
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '-';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                    END IF;
					V_ERROR_CODE := V_ERROR_CODE || '351';
					V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[MoreThanOneParTxnFound]' || '   -   ' || 'The Parent Transaction Id "' || I_PAR_TXN_ID || '" returned more than one line.';
					
                    GOTO VERIFY_PRODUCT;
                    
			END;
            
            <<VERIFY_PRODUCT>>
            
            ----------------------------------------- PRODUCT VALIDATIONS -----------------------------------------
            BEGIN
				-- VALIDATE PRODUCT
				BEGIN
					SELECT
						PROD.ROW_ID     AS "PROD_ID",
						PROD.PART_NUM   AS "PROD_PART",
						PROD.BU_ID      AS "PROD_BU_ID",
						BU.NAME			AS "PROD_BU_NAME"
					INTO V_PROD_ID, V_PROD_PART_NUM, V_PROD_BI, V_PROD_BU
					FROM SIEBEL.S_PROD_INT PROD
					LEFT JOIN SIEBEL.S_BU BU ON BU.ROW_ID = PROD.BU_ID
					WHERE 1=1
					AND PROD.NAME = I_PROD_NAME;	-- PRODUCT NAME
				EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        IF (V_ERROR_CODE IS NOT NULL) THEN
                            V_ERROR_CODE := V_ERROR_CODE || '-';
                            V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                        END IF;
						V_ERROR_CODE := V_ERROR_CODE || '400';
						V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[ProductNotFound]' || '   -   ' || 'The Product "' || I_PROD_NAME || '" was not found.';
						
                        GOTO VERIFY_POINT;
                        
					WHEN TOO_MANY_ROWS THEN
                        IF (V_ERROR_CODE IS NOT NULL) THEN
                            V_ERROR_CODE := V_ERROR_CODE || '-';
                            V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                        END IF;
						V_ERROR_CODE := V_ERROR_CODE || '401';
						V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[MoreThanOneProductFound]' || '   -   ' || 'The Product "' || I_PROD_NAME || '" returned more than one line.';
						
                        GOTO VERIFY_POINT;
                        
				END;

                -- VALIDATE PRODUCT PARTNER
                SELECT
                    PROG.ROW_ID     AS "PROD_PARTNER_PROG_ID",
                    PROG.NAME       AS "PROD_PARTNER_PROG_NAME",
                    PROG.BU_ID      AS "PROD_PARTNER_PROG_BI",
                    BUPROG.NAME     AS "PROD_PARTNER_PROG_BU",
                    PART.ROW_ID     AS "PROD_PARTNER_ID",
                    PART.BU_ID      AS "PROD_PARTNER_BI",
                    BUPART.NAME     AS "PROD_PARTNER_BU",
                    PART.LOC        AS "PARTNER_LOC",
                    PART.NAME       AS "PROD_PARTNER_NAME"/*,
                    PART.ALIAS_NAME AS "PROD_PARTNER_ALIAS"*/
                INTO
                    V_PROD_PARTNER_PROG_ID,
                    V_PROD_PARTNER_PROG_NAME,
                    V_PROD_PARTNER_PROG_BI,
                    V_PROD_PARTNER_PROG_BU,
                    V_PROD_PARTNER_ID,
                    V_PROD_PARTNER_BI,
                    V_PROD_PARTNER_BU,
                    V_PARTNER_LOC,
                    V_PROD_PARTNER_NAME/*,
                    V_PROD_PARTNER_ALIAS*/
                FROM SIEBEL.S_LOY_PROGRAM PROG                                                      -- PROGRAM
                RIGHT JOIN SIEBEL.S_BU BUPROG ON BUPROG.ROW_ID = PROG.BU_ID                         -- PROGRAM BU
                RIGHT JOIN SIEBEL.S_LOY_PROG_ORG PORG ON PORG.PROG_ID = PROG.ROW_ID                 -- PROGRAM x PARTNER INTER TABLE
                RIGHT JOIN SIEBEL.S_ORG_EXT PART ON PART.ROW_ID = PORG.ORG_EXT_ID                   -- PARTNER
                RIGHT JOIN SIEBEL.S_BU BUPART ON BUPART.ROW_ID = PART.BU_ID                         -- PARTNER BU
                RIGHT JOIN SIEBEL.S_PGM_ORG_PROD OPROD ON OPROD.PROG_PARTNER_ID = PORG.ROW_ID       -- PARTNER x PRODUCT INTER TABLE
                RIGHT JOIN SIEBEL.S_PROD_INT PROD ON PROD.ROW_ID = OPROD.PRODUCT_ID                 -- PRODUCT
                LEFT JOIN SIEBEL.S_BU BUPROD ON BUPROD.ROW_ID = PROD.BU_ID                          -- PRODUCT BU
                WHERE 1=1
                AND PROD.ROW_ID = V_PROD_ID				-- PRODUCT ID
                AND PART.ALIAS_NAME = I_PARTNER_ALIAS	-- PARTNER ALIAS
                AND PROG.BU_ID = V_PROG_BI;             -- PROGRAM ORGANIZATION
                
			EXCEPTION
				WHEN NO_DATA_FOUND THEN
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '-';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                    END IF;
                    IF (INSTR(V_ERROR_CODE,'200', 1, 1) > 0) THEN
					    V_ERROR_CODE := V_ERROR_CODE || '402';
					    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[ProductPartnerProgNotFound]' || '   |   ' || 'The Product "' || I_PROD_NAME || '" was not found for the Partner "' || I_PARTNER_ALIAS || '" and Program Name "' || I_PROG_NAME || '" combinations.';
					ELSE
                        V_ERROR_CODE := V_ERROR_CODE || '403';
					    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[ProductPartnerNotFound]' || '   |   ' || 'The Product "' || I_PROD_NAME || '" was not found for the Partner "' || I_PARTNER_ALIAS || '".';
					END IF;
                    
                    GOTO VERIFY_POINT;
                    
                WHEN TOO_MANY_ROWS THEN
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '-';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                    END IF;
                    V_ERROR_CODE := V_ERROR_CODE || '404';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[MoreThanOneProdPartnerFound]' || '   |   ' || 'The Product "' || I_PROD_NAME || '" returned more than one line.';
                    
                    GOTO VERIFY_POINT;
                    
			END;
			
            <<VERIFY_POINT>>
            
            ----------------------------------------- TRANSACTION POINT VALIDATIONS -----------------------------------------
            BEGIN
				SELECT
					ATR.ROW_ID          AS "POINT_ID",
					ATR.ATTR_TYPE_CD    AS "POINT_TYPE",
					ATR.INTERNAL_NAME   AS "POINT_NAME",
					--ATR.DISPLAY_NAME    AS "DISPLAY_NAME",
					ATR.PROGRAM_ID      AS "POINT_PROG_ID",
					PRG.NAME            AS "POINT_PRG_NAME",
					BUP.ROW_ID          AS "POINT_PRG_BI",
					BUP.NAME            AS "POINT_PRG_BU"
				INTO
					V_POINT_ID,
					V_POINT_TYPE,
					V_POINT_NAME,
					V_POINT_PROG_ID,
					V_POINT_PROG_NAME,
					V_POINT_PROG_BI,
					V_POINT_PROG_BU
				FROM SIEBEL.S_LOY_ATTRDEFN ATR
				LEFT JOIN SIEBEL.S_LOY_PROGRAM PRG ON PRG.ROW_ID = ATR.PROGRAM_ID
				LEFT JOIN SIEBEL.S_BU BUP ON BUP.ROW_ID = PRG.BU_ID
				WHERE 1=1
				AND ATR.PROGRAM_ID = V_PROG_ID					-- PROGRAM ID
				AND ATR.DISPLAY_NAME = I_POINT_DISPLAY_NAME		-- INPUT POINT NAME
				AND ATR.OBJECT_CD = FV_POINT_OBJECT;			-- FIXED VALUE COLUMN POINT OBJECT
			EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '-';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                    END IF;

                    IF (INSTR(V_ERROR_CODE,'200', 1, 1) > 0) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '450';
					    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[PointProgNotFound]' || '   |   ' || 'The Point Display Name "' || I_POINT_DISPLAY_NAME || '" was not found for the Program Name "' || I_PROG_NAME || '".';
                    ELSE
					    V_ERROR_CODE := V_ERROR_CODE || '451';
					    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[PointNotFound]' || '   |   ' || 'The Point Display Name "' || I_POINT_DISPLAY_NAME || '" was not found.';
					END IF;

                    GOTO VERIFY_OWNER;
                    
				WHEN TOO_MANY_ROWS THEN
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '-';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                    END IF;
					V_ERROR_CODE := V_ERROR_CODE || '452';
					V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[MoreThanOnePointFound]' || '   |   ' || 'The Point with Display Name "' || I_POINT_DISPLAY_NAME || '" returned more than one line.';
                    
                    GOTO VERIFY_OWNER;
                    
			END;
			
            <<VERIFY_OWNER>>
            
            ----------------------------------------- OWNER VALIDATIONS -----------------------------------------
            IF(I_OWNER_LOGIN <> '' OR I_OWNER_LOGIN IS NOT NULL) THEN
                BEGIN
                    SELECT
                        USR.ROW_ID
                        --USR.LOGIN,
                        --USR.LOGIN_DOMAIN
                    INTO V_OWNER_ID
                    FROM SIEBEL.S_USER USR
                    WHERE 1=1
                    AND USR.LOGIN = I_OWNER_LOGIN;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        SELECT
                            USR.ROW_ID
                            --USR.LOGIN,
                            --USR.LOGIN_DOMAIN
                        INTO V_OWNER_ID
                        FROM SIEBEL.S_USER USR
                        WHERE 1=1
                        AND USR.LOGIN = 'INTEF';
                        
                        GOTO DEFINE_WHERE_TO_GO;
                    WHEN TOO_MANY_ROWS THEN
                        SELECT
                            USR.ROW_ID
                            --USR.LOGIN,
                            --USR.LOGIN_DOMAIN
                        INTO V_OWNER_ID
                        FROM SIEBEL.S_USER USR
                        WHERE 1=1
                        AND USR.LOGIN = 'INTEF';
                        
                        GOTO DEFINE_WHERE_TO_GO;
                        
                END;
            END IF;

            <<DEFINE_WHERE_TO_GO>>
            
            IF (V_ERROR_CODE IS NOT NULL) THEN
                GOTO INSERT_EIM;
            ELSE
                GOTO GENERAL_VALIDATIONS;
            END IF;


            <<GENERAL_VALIDATIONS>>
            
            ----------------------------------------- GENERAL VALIDATIONS -----------------------------------------
            ------------------ MEMBER VALIDATIONS ------------------
            -- Validate Member Status
			IF (V_MEMBER_STATUS = 'Cancelled' OR V_MEMBER_STATUS = 'Inactive' OR V_MEMBER_STATUS = 'PVSM Suspended' OR V_MEMBER_STATUS = 'Suspended') THEN
				IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '151';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[MemberStatusNotValid]' || '   -   ' || 'The Member has a not valid Status "' || V_MEMBER_STATUS || '".';
			END IF;
			
            -- Validate Member x Program
            IF (V_MEMBER_PROG_NAME <> I_PROG_NAME) THEN
				IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '152';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[MemberProgNotValid]' || '   -   ' || 'The Member Program Name "' || V_MEMBER_PROG_NAME || '" is different from the input Program Name "' || I_PROG_NAME || '".';
            END IF;

            -- Validate Member x Program Organization
            IF (V_MEMBER_BU <> V_PROG_BU) THEN
				IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '153';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[MemberOrgNotValid]' || '   -   ' || 'The Member Organization "' || V_MEMBER_BU || '" is different from the Program Organization "' || V_PROG_BU || '".';
            END IF;
            
            ------------------ SIGNATURE VALIDATIONS ------------------
            -- Validate Signature Status
            IF (V_SIGNATURE_STATUS = 'Cancelled' OR V_SIGNATURE_STATUS = 'Suspended') THEN
				IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '302';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[SignStatusNotValid]' || '   -   ' || 'The Signature has a not valid Status "' || V_SIGNATURE_STATUS || '".';
            END IF;

            -- Validate Signature x Member
            IF (V_SIGNATURE_MEM_NUMBER <> I_MEMBER_NUMBER) THEN
				IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '303';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[SignMemberNotValid]' || '   -   ' || 'The Member Number "' || V_SIGNATURE_MEM_NUMBER || '" of input Signature Code "' || I_SIGNATURE_CODE || '" is different than input Member Number "' || I_MEMBER_NUMBER || '".';
            END IF;
            
            ------------------ PARENT TRANSACTION VALIDATIONS ------------------
            -- Validate if Parent Transaction already has any XXXXXX transactions associated with it
            BEGIN
                SELECT
                    COUNT(*)    AS "TOTAL_TXN_FOUND"
                INTO C_PAR_TXN_COUNT    
                FROM SIEBEL.S_LOY_TXN TXN
                LEFT JOIN SIEBEL.S_PROD_INT PRD ON PRD.ROW_ID = TXN.PROD_ID
                WHERE 1=1
                AND (
                    PRD.NAME LIKE '%Adesão%'
                    OR PRD.NAME LIKE 'XXXXXX Cartão%XXXXXX'
                )
                AND TXN.PAR_TXN_ID = I_PAR_TXN_ID
                AND PRD.PART_NUM LIKE 'XXXXXX%'
                AND TXN.TYPE_CD = 'ACCRUAL'
                AND TXN.SUB_TYPE_CD = 'Product'
                AND TXN.STATUS_CD NOT IN (
                    'Cancelled',
                    'Manually Cancelled',
                    'Rejected - Engine',
                    'Rejected - Manager',
                    'Rejected - Duplicated',
                    'Rejected - Name'
                );
            END;

            IF(C_PAR_TXN_COUNT > 0) THEN
                IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '352';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[ParTxnNotValid]' || '   -   ' || 'The Parent Transaction Id "' || I_PAR_TXN_ID || '" already has an XXXXXX Transaction associated.';
            END IF;

            -- Validate Parent transaction Product
            IF (V_PAR_TXN_PRD_PART <> 'XXXXXX' AND V_PAR_TXN_PRD_PART <> 'XXXXXXAnual') THEN
                IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '353';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[ParTxnProductNotValid]' || '   -   ' || 'The Product "' || V_PAR_TXN_PRD_NAME || '" of Parent Transaction is not a valid Club Product (XXXXXX Anual, XXXXXX).';
            END IF;

            -- Validate Parent transaction Status and SubStatus
            IF (V_PAR_TXN_STATUS <> 'Processed') THEN
                IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '354';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[ParTxnStatusNotValid]' || '   -   ' || 'The Parent Transaction Status "' || V_PAR_TXN_STATUS || '" is not valid.';
            ELSE
                IF (V_PAR_TXN_SUB_STATUS <> 'Success') THEN
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '-';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                    END IF;
                    V_ERROR_CODE := V_ERROR_CODE || '355';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[ParTxnSubStatusNotValid]' || '   -   ' || 'The Parent Transaction Sub Status "' || V_PAR_TXN_SUB_STATUS || '" is not valid.';
                END IF;
            END IF;

            -- Validate Parent transaction Channel
            /*
            IF (V_PAR_TXN_CHANNEL IS NOT NULL) THEN
                IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '356';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[ParTxnChannelFound]' || '   -   ' || 'The Parent Transaction Channel "' || V_PAR_TXN_CHANNEL || '" is not valid. Recurrence Transactions should not have a Channel column filled.';
            END IF;
            */

            -- Validate Parent Transaction Signature
            IF (V_PAR_TXN_SIGN_CD <> I_SIGNATURE_CODE) THEN
                IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '357';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[ParTxnSignInputNotValid]' || '   -   ' || 'The Parent Transaction Signature Code "' || V_PAR_TXN_SIGN_CD || '" is different from the Input Signature Code "' || I_SIGNATURE_CODE || '".';
            END IF;

            -- Validate Parent Transaction Member
            IF (V_PAR_TXN_MEM_ID IS NOT NULL) THEN
                IF (V_PAR_TXN_MEM_ID <> V_MEMBER_ID) THEN
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '-';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                    END IF;
                    V_ERROR_CODE := V_ERROR_CODE || '358';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[ParTxnMemberNotValid]' || '   -   ' || 'The Parent Transaction Member Id "' || V_PAR_TXN_MEM_ID || '" is different from the Input Member Id "' || V_MEMBER_ID || '".';
                END IF;
            ELSE
                IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '359';
                V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[ParTxnMemberNotFound]' || '   -   ' || 'The Member Id of Parent Transaction Id "' || I_PAR_TXN_ID || '" was not found.';
            END IF;
            
            -- Validate Parent Transaction Organization
            IF (V_PAR_TXN_BU <> V_PROG_BU) THEN
                IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '360';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[ParTxnOrgNotValid]' || '   -   ' || 'The Parent Transaction Organization "' || V_PAR_TXN_BU || '" is different from the Input Program Organization "' || V_PROG_BU || '".';
            END IF;

            ------------------ PRODUCT VALIDATIONS ------------------
            /*-- Validate Product x Program Organization
            IF (V_PROD_BU <> V_PROG_BU) THEN
                IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '404';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[ProductOrgNotValid]' || '   -   ' || 'The Product Organization "' || V_PROD_BU || '" is different from the Input Program Organization "' || V_PROG_BU || '" .';
            END IF;
            */
            ------------------ TRANSACTION VALIDATIONS ------------------
            -- Transaction Date
            IF (I_TXN_DATE IS NULL) THEN
                I_TXN_DATE := TO_DATE(SYSDATE, 'DD/MM/YYYY HH24:MI:SS');
            END IF;

            -- Transaction Activity Date
            IF (I_TXN_POST_DATE IS NULL) THEN
                I_TXN_POST_DATE := TO_DATE(SYSDATE, 'DD/MM/YYYY HH24:MI:SS');
            END IF;

            -- Transaction Amount
            IF (I_TXN_AMOUNT IS NULL OR I_TXN_AMOUNT < 1) THEN
                IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '500';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[TxnAmountNotValid]' || '   -   ' || 'The XXXXXX Transaction "Amount" cannot be "NULL" and should be Greather than "0".';
            END IF;

            -- Transaction Type
            IF (I_TXN_TYPE IS NOT NULL) THEN
                BEGIN
                    SELECT
                        COUNT(*)
                    INTO C_TXN_TYPE_LOV_COUNT
                    FROM SIEBEL.S_LST_OF_VAL LOV
                    LEFT JOIN SIEBEL.S_LST_OF_VAL PLOV ON PLOV.ROW_ID = LOV.PAR_ROW_ID
                    LEFT JOIN SIEBEL.S_LANG SLA ON SLA.LANG_CD = LOV.LANG_ID
                    LEFT JOIN SIEBEL.S_LIT SLT ON SLT.ROW_ID = LOV.BITMAP_ID
                    LEFT JOIN SIEBEL.S_WORKSPACE WS ON WS.ROW_ID = LOV.WS_ID
                    LEFT JOIN SIEBEL.S_WORKSPACE WSP ON WSP.ROW_ID = LOV.WS_ID
                    LEFT JOIN SIEBEL.S_REPOSITORY REP ON REP.ROW_ID = WSP.REPOSITORY_ID
                    WHERE 1=1
                    AND REP.NAME = 'Siebel Repository'
                    AND WS.NAME = 'MAIN'
                    AND LOV.WS_INACTIVE_FLG = 'N'
                    AND LOV.ACTIVE_FLG = 'Y'
                    AND LOV.LANG_ID = 'ENU'
                    AND LOV.TYPE = 'LOY_TXN_TYPE_CD'
                    AND LOV.NAME = UPPER(I_TXN_TYPE);
                END;

                IF (C_TXN_TYPE_LOV_COUNT < 1) THEN
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '501';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[TxnTypeNotFound]' || '   -   ' || 'The XXXXXX Transaction Type "' || I_TXN_TYPE || '" was not found.';
                END IF;
            ELSE
                IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '502';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[TxnTypeNotValid]' || '   -   ' || 'The XXXXXX Transaction Type "' || I_TXN_TYPE || '" cannot be "NULL" or "Empty".';
            END IF;

            -- Transaction SubType
            IF (I_TXN_SUBTYPE IS NOT NULL) THEN
                BEGIN
                    SELECT
                        COUNT(*)
                    INTO C_TXN_TYPE_LOV_COUNT
                    FROM SIEBEL.S_LST_OF_VAL LOV
                    LEFT JOIN SIEBEL.S_LST_OF_VAL PLOV ON PLOV.ROW_ID = LOV.PAR_ROW_ID
                    LEFT JOIN SIEBEL.S_LANG SLA ON SLA.LANG_CD = LOV.LANG_ID
                    LEFT JOIN SIEBEL.S_LIT SLT ON SLT.ROW_ID = LOV.BITMAP_ID
                    LEFT JOIN SIEBEL.S_WORKSPACE WS ON WS.ROW_ID = LOV.WS_ID
                    LEFT JOIN SIEBEL.S_WORKSPACE WSP ON WSP.ROW_ID = LOV.WS_ID
                    LEFT JOIN SIEBEL.S_REPOSITORY REP ON REP.ROW_ID = WSP.REPOSITORY_ID
                    WHERE 1=1
                    AND REP.NAME = 'Siebel Repository'
                    AND WS.NAME = 'MAIN'
                    AND LOV.WS_INACTIVE_FLG = 'N'
                    AND LOV.ACTIVE_FLG = 'Y'
                    AND LOV.LANG_ID = 'ENU'
                    AND LOV.TYPE = 'LOY_TXN_SUB_TYPE_CD'
                    AND LOV.NAME = I_TXN_SUBTYPE
                    AND LOV.SUB_TYPE = UPPER(I_TXN_TYPE);
                END;

                IF (C_TXN_TYPE_LOV_COUNT < 1) THEN
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '503';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[TxnSubTypeNotFound]' || '   -   ' || 'The XXXXXX Transaction SubType "' || I_TXN_SUBTYPE || '" was not found for the Type "' || I_TXN_TYPE || '".';
                END IF;
            ELSE
                IF (V_ERROR_CODE IS NOT NULL) THEN
                    V_ERROR_CODE := V_ERROR_CODE || '-';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                END IF;
                V_ERROR_CODE := V_ERROR_CODE || '504';
				V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[TxnSubTypeNotValid]' || '   -   ' || 'The XXXXXX Transaction SubType "' || I_TXN_SUBTYPE || '" cannot be "NULL" or "Empty".';
            END IF;

            -- Transaction External
            IF (I_TXN_EXTERNAL_FLG IS NULL) THEN
                I_TXN_EXTERNAL_FLG := 'N';
            END IF;
			------------------------------------- END OF GENERAL VALIDATIONS --------------------------------------
            --SELECT XXXXXX_ACC_PARTNER_B2O_SEQ.NEXTVAL INTO EIM_ROW_ID FROM DUAL; --ALTEREAR

            <<INSERT_EIM>>

            EIM_ROW_ID := F_LINE_NUMBER - 1;
            EIM_TXNX_ROW_ID := F_LINE_NUMBER - 1;
            
            IF V_ERROR_CODE IS NOT NULL THEN
                EIM_IF_ROW_STAT := 'EIM_VALIDATION_ERROR';
                EIM_ROW_BATCH_BKP := EIM_ROW_BATCH_NUM;
                EIM_ROW_BATCH_NUM := 99999;
            END IF;

            -- INSERT AQUI
            BEGIN
                INSERT INTO SIEBEL.EIM_LOY_TXN (
                    ROW_ID,
                    IF_ROW_BATCH_NUM,
                    IF_ROW_STAT,
                    ------------------ TRANSACTION COLUMNS ------------------
                    -- VARIABLE INPUTS
                    TXN_NUM,                        -- TRANSACTION NUMBER
                    TXN_DT,              			-- TRANSACTION DATE
                    POST_DT,             			-- TRANSACTION ACTIVITY DATE
                    TYPE_CD,             			-- TRANSACTION TYPE
                    SUB_TYPE_CD,         			-- TRANSACTION SUBTYPE
                    TXN_CHANNEL_CD,      			-- TRANSACTION CHANNEL
                    STATUS_CD,           			-- TRANSACTION STATUS
                    AMT_VAL,             			-- TRANSACTION AMOUNT
                    COMMENTS,            			-- TRANSACTION COMMENTS
                    EXTERNAL_FLG,        			-- TRANSACTION EXTERNAL
                    NUM_POINTS,          			-- TRANSACTION BASE POINTS (1)
                    X_DOC_MILES_PURCHASE,			-- TRANSACTION MEMBER DOC NUMBER
                    TXN_BI,							-- TRANSACTION ORGANIZATION
                    TXN_BU,              			-- TRANSACTION ORGANIZATION
                    VIS_BU,
    
                    -- CONSTANT VALUE COLUMNS
                    BID_FLG,                        -- Bid Flag
                    OVR_DUP_CHECK_FLG,              -- DEFAULT 'N'
                    OVR_PRI_FLAG,                   -- Override Price Flag
                    QUAL_FLG,                       -- Qualifying Flag
                    UNACC_MINOR_FLG,                -- Unaccompanied Minor
                    SOURCE_CD,                      -- Source
    
                    ------------------ FOREIGN KEYS ------------------
                    -- PAR_TXN_ID
                    PAR_TXN_TXN_BI,                 -- PARENT TRANSACTION
                    PAR_TXN_TXN_BU,                 -- PARENT TRANSACTION
                    PAR_TXN_NUM,                    -- PARENT TRANSACTION
                    -- PARTNER_ID
                    PARTNER_ACCNT_BI,               -- PARTNER
                    PARTNER_ACCNT_BU,               -- PARTNER
                    PARTNER_ACCNT_LOC,              -- PARTNER
                    PARTNER_ACCNT_NAME,	            -- PARTNER
                    -- PROG_ID
                    PROG_BI,                        -- PROGRAM
                    PROG_BU,                        -- PROGRAM
                    PROG_NAME,                      -- PROGRAM
                    -- PROD_ID
                    PROD_BI,                      	-- PRODUCT
                    PROD_BU,                        -- PRODUCT
                    PROD_NAME,                      -- PRODUCT
                    /*
                    PROD_VEN_BI,					-- PRODUCT
                    PROD_VEN_BU,                    -- PRODUCT
                    PROD_VEN_LOC,                   -- PRODUCT
                    PROD_VEN_NAME,                  -- PRODUCT
                    */
                    -- MEMBER_ID
                    MEMBER_MEM_BI,                  -- MEMBER
                    MEMBER_MEM_BU,                  -- MEMBER
                    MEMBER_MEM_NUM,                 -- MEMBER
                    MEMBER_PROG_BI,                 -- MEMBER
                    MEMBER_PROG_BU,                 -- MEMBER
                    MEMBER_PROG_NAME,               -- MEMBER
                    -- OWNER_ID
                    OWNER_LOGIN,                    -- OWNER
                    -- POINT_TYPE_ID
                    POINT_ATTR_TYPE_CD,             -- POINTS
                    POINT_INTERNALNAME,             -- POINTS
                    POINT_OBJECT_CD,                -- POINTS
                    POINT_PROG_BI,                  -- POINTS
                    POINT_PROG_BU,                  -- POINTS
                    POINT_PROG_NAME,                -- POINTS
                    ------------------ OTHER COLUMNS ------------------
                    PROCESSING_LOG,
                    PROCESSING_COMMENT,
                    X_XXXXXX_FILE_NAME	                -- FILE NAME
                ) VALUES (
                    EIM_ROW_ID, 																						-- ROW_ID
                    EIM_ROW_BATCH_NUM, 																					-- IF_ROW_BATCH_NUM
                    EIM_IF_ROW_STAT, 																					-- IF_ROW_STAT
                    ------------------ TRANSACTION COLUMNS ------------------
                    -- VARIABLE INPUTS
                    TO_CHAR(I_TXN_DATE, 'DDMMYYYYHH24MISS') || EIM_ROW_ID || I_MEMBER_NUMBER, 							-- TXN_NUM
                    I_TXN_DATE,                                                                                         -- TXN_DT
                    I_TXN_POST_DATE,                                                                                    -- POST_DT
                    I_TXN_TYPE, 																						-- TYPE_CD
                    I_TXN_SUBTYPE, 																						-- SUB_TYPE_CD
                    I_TXN_CHANNEL, 																						-- TXN_CHANNEL_CD
                    FV_TXN_STATUS, 																						-- STATUS_CD
                    I_TXN_AMOUNT, 																						-- AMT_VAL
                    I_TXN_COMMENTS,  																					-- COMMENTS
                    I_TXN_EXTERNAL_FLG, 																				-- EXTERNAL_FLG
                    V_TXN_POINTS, 																						-- NUM_POINTS
                    V_MEMBER_DOC_NUMBER,																				-- TRANSACTION MEMBER DOC NUMBER
                    V_PROG_BI,                                                                                          -- TXM_BI
                    V_PROG_BU, 																							-- TXN_BU
                    V_PARTNER_NAME,                                                                                     -- VIS_BU
    
                    -- CONSTANT VALUE COLUMNS
                    FV_BID_FLG, 																						-- BID_FLG
                    FV_OVR_DUP_CHECK_FLG, 																				-- OVR_DUP_CHECK_FLG
                    FV_OVR_PRI_FLAG, 																					-- OVR_PRI_FLAG
                    FV_QUAL_FLG, 																						-- QUAL_FLG
                    FV_UNACC_MINOR_FLG, 																				-- UNACC_MINOR_FLG
                    FV_SOURCE_CD, 																						-- SOURCE_CD
    
                    ------------------ FOREIGN KEYS ------------------
                    -- PAR_TXN_ID
                    V_PAR_TXN_BI,																						-- PARENT TRANSACTION
                    V_PAR_TXN_BU,																						-- PARENT TRANSACTION
                    V_PAR_TXN_NUM,																						-- PARENT TRANSACTION
                    --V_PARTNER_ID
                    V_PARTNER_BU_ID,																					-- PARTNER
                    V_PARTNER_BU, 																						-- PARTNER_ACCNT_BU
                    V_PARTNER_LOC, 																						-- PARTNER_ACCNT_LOC
                    V_PARTNER_NAME, 																					-- PARTNER_ACCNT_NAME
                    -- PROGRAM_ID
                    V_PROG_BI,																							-- PROG_BI
                    V_PROG_BU, 																							-- PROG_BU
                    I_PROG_NAME, 																						-- PROG_NAME
                    -- PROD_ID
                    V_PROD_BI,																							-- PROD_BI
                    V_PROD_BU, 																							-- PROD_BU
                    I_PROD_NAME, 																						-- PROD_NAME
                    /*
                    V_PROD_PARTNER_BI,																					-- PROD_VEN_BI
                    V_PROD_PARTNER_BU, 																					-- PROD_VEN_BU
                    V_PROD_PARTNER_LOC, 																				-- PROD_VEN_LOC (EMPTY)
                    V_PROD_PARTNER_NAME, 																				-- PROD_VEN_NAME (EMPTY)
                    */
                    -- MEMBER_ID
                    V_MEMBER_BI,																					    -- MEMBER_BU_ID
                    V_MEMBER_BU, 																					    -- MEMBER_MEM_BU
                    I_MEMBER_NUMBER, 																					-- MEMBER_MEM_NUM
                    V_MEMBER_PROG_BI, 																					-- MEMBER_PROG_BI
                    V_MEMBER_PROG_BU,																					-- MEMBER_PROG_BU
                    V_MEMBER_PROG_NAME,  																				-- MEMBER_PROG_NAME
                    -- OWNER_ID
                    I_OWNER_LOGIN,																						-- OWNER
                    -- POINT_TYPE_ID
                    V_POINT_TYPE, 																						-- POINT_ATTR_TYPE_CD
                    V_POINT_NAME,																						-- POINT_INTERNALNAME
                    FV_POINT_OBJECT, 																					-- POINT_OBJECT_CD
                    V_POINT_PROG_BI,																					-- POINT_PROG_BI			
                    V_POINT_PROG_BU, 																					-- POINT_PROG_BU
                    V_POINT_PROG_NAME,																					-- POINT_PROG_NAME
                    ------------------ OTHER COLUMNS ------------------
                    SUBSTR(TRIM(V_ERROR_CODE), 1, 1001),
                    SUBSTR(TRIM(V_ERROR_MESSAGE), 1, 1001),
                    SUBSTR(TRIM(IN_FILE), 1, 50)	 																	-- X_XXXXXX_FILE_NAME
                );
            EXCEPTION
                WHEN OTHERS THEN
                    IF (V_ERROR_CODE IS NOT NULL) THEN
                        V_ERROR_CODE := V_ERROR_CODE || '-';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                    END IF;
                    V_ERROR_CODE := V_ERROR_CODE || '550';
                    V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[InsertOnEIM_LOY_TXNTableError]' || '   |   ' || 'Error on insert at "EIM_LOY_TXN" Table. Error Message: ["' || SQLERRM || '"]';
                    
                    GOTO VERIFY_ERRORS;
            END;

            IF EIM_ROW_BATCH_NUM <> 99999 THEN
                BEGIN
                    INSERT INTO SIEBEL.EIM_LOY_TXNDTL(
                        ROW_ID,
                        IF_ROW_BATCH_NUM,
                        IF_ROW_STAT,
                        TXN_BU,
                        TXN_NUM,
                        X_SIGN_MEMBER_ID,                                                                                                       -- SIGNATURE MEMBER ID
                        X_ATTRIB_52							                                                                                    -- EXTERNAL PARTNER ID
                    ) VALUES (
                        EIM_TXNX_ROW_ID, 								                                                                        -- ROW_ID 
                        EIM_ROW_BATCH_NUM, 																					                    -- IF_ROW_BATCH_NUM
                        EIM_TXNX_IF_ROW_STAT, 							                                                                        -- IF_ROW_STAT 
                        V_PROG_BU, 								                                                                                -- TXN_BU
                        TO_CHAR(I_TXN_DATE, 'DDMMYYYYHH24MISS') || EIM_ROW_ID || I_MEMBER_NUMBER,    -- TXN_NUM
                        V_SIGNATURE_ID,                                                                                                         -- SIGNATURE MEMBER ID
                        I_EXTERNAL_TXN_ID 							                                                                            -- EXTERNAL PARTNER ID
                    );
                EXCEPTION
                    WHEN OTHERS THEN
                        IF (V_ERROR_CODE IS NOT NULL) THEN
                            V_ERROR_CODE := V_ERROR_CODE || '-';
                            V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ';
                        END IF;
                        V_ERROR_CODE := V_ERROR_CODE || '551';
                        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '[InsertOnEIM_TXNDTLTableError]' || '   |   ' || 'Error on insert at "EIM_LOY_TXNDTL" Table. Error Message: ["' || SQLERRM || '"]';
                        
                        GOTO VERIFY_ERRORS;
                END;
            END IF;
            
            IF (C_REC_COUNT_COMMIT > 999) THEN
                COMMIT;
                C_REC_COUNT_COMMIT := 0;
            END IF;
            
            -- FIM INSERT
            
            <<VERIFY_ERRORS>>
            
            IF (V_ERROR_CODE IS NOT NULL) THEN
                C_COUNT_REC_ERROR := C_COUNT_REC_ERROR + 1;
                -- LOG ERROR TO FILE ERROR
                UTL_FILE.PUT_LINE(F_FILE_ERROR, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Error at Line: ' || F_LINE_NUMBER  || '   -   ' || 'Error Code(s): [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message(s): [' || V_ERROR_MESSAGE || ']');
				
				-- LOG TO EXCEPTION FILE
				SELECT SUBSTR(IN_FILE,1,LENGTH(IN_FILE) - 4) INTO F_NEW_FILE FROM DUAL;
				F_FILE_EXCEP := UTL_FILE.FOPEN(F_EXCEPT_DIR, F_NEW_FILE || '_' || IN_PROCESS_ID ||  '_EIM_LOAD_EXCEP.log','a');
                UTL_FILE.PUT_LINE(F_FILE_EXCEP, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Error at Line: ' || F_LINE_NUMBER  || '   -   ' || 'Error Code(s): [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message(s): [' || V_ERROR_MESSAGE || ']');
                UTL_FILE.FFLUSH(F_FILE_EXCEP);
				UTL_FILE.FCLOSE(F_FILE_EXCEP);
				
				-- LOG TO JOB FILE
				F_FILE_JOB := UTL_FILE.FOPEN(F_LOG_DIR, IN_PROCESS_ID || '.log', 'a');
                UTL_FILE.PUT_LINE(F_FILE_JOB, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Error at Line: ' || F_LINE_NUMBER  || '   -   ' || 'Error Code(s): [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message(s): [' || V_ERROR_MESSAGE || ']');
                UTL_FILE.FFLUSH(F_FILE_JOB);
				UTL_FILE.FCLOSE(F_FILE_JOB);
                
                EIM_IF_ROW_STAT := 'ERROR';
				
				--GOTO NEXT_FILE_LINE;
            END IF;

            <<NEXT_FILE_LINE>>

			-- UPDATE COUNTERS
			C_RECORD_COUNT          := C_RECORD_COUNT + 1;  -- USED TO COUNT HOW MANY RECORDS WERE READ
			F_LINE_NUMBER 			:= F_LINE_NUMBER + 1;   -- USED TO SKIP THE FIRST LINE IN INPUT FILE AND RETURN THE ACTUAL FILE LINE
		END LOOP;
	EXCEPTION
        WHEN NO_DATA_FOUND THEN
            COMMIT;
            GOTO END_OF_FILE;
    END;
	
	<<END_OF_FILE>>

    OUT_FILE_ERROR_CD := 0;
    OUT_FILE_ERROR_MSG := 0;

    -- UPDATE THE ERROR FILE
	UTL_FILE.PUT_LINE(F_FILE_ERROR, '');
    IF (C_COUNT_REC_ERROR = 0) THEN
        UTL_FILE.PUT_LINE(F_FILE_ERROR, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[Input Line(s)]' || '   -   ' || 'The line reading of input file has finished without errors.');
    ELSE
        UTL_FILE.PUT_LINE(F_FILE_ERROR, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[Input Line(s)]' || '   -   ' || 'The line reading of input file has finished with errors.');
    END IF;

	-- CLOSES THE INPUT FILE
    UTL_FILE.FCLOSE(F_FILE);
    
    F_FILE_JOB := UTL_FILE.FOPEN(F_LOG_DIR, IN_PROCESS_ID || '.log', 'a');

    IF (C_COUNT_REC_ERROR = 0) THEN
        UTL_FILE.PUT_LINE(F_FILE_JOB, 'PROCESSO EXECUTADO COM SUCESSO');
    ELSE
        UTL_FILE.PUT_LINE(F_FILE_JOB,'PROCESSO EXECUTADO COM ERRO. ENTRE EM CONTATO COM O ANALISTA');
    END IF;
    
    UTL_FILE.FFLUSH(F_FILE_JOB);
    UTL_FILE.FCLOSE(F_FILE_JOB);
    
    V_END_TIME := TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS');
    
    UTL_FILE.PUT_LINE(F_FILE_ERROR, '');
    UTL_FILE.PUT_LINE(F_FILE_ERROR, '=========================================================================================');
    UTL_FILE.PUT_LINE(F_FILE_ERROR, 'Inicio do Processamento: ' || V_START_TIME || ' [Initial Timestamp]');
    UTL_FILE.PUT_LINE(F_FILE_ERROR, 'Total de Linhas do Arquivo: ' || TO_CHAR(C_RECORD_COUNT) || ' [Detail Records]');
    UTL_FILE.PUT_LINE(F_FILE_ERROR, 'Total de Linhas OK: ' || TO_CHAR(C_RECORD_COUNT - C_COUNT_REC_ERROR) || ' [Lines Inserted at EIM]');
    UTL_FILE.PUT_LINE(F_FILE_ERROR, 'Total de Linhas ERROR: ' || TO_CHAR(C_COUNT_REC_ERROR) || ' [Lines with error]');
    UTL_FILE.PUT_LINE(F_FILE_ERROR, 'Fim do Processamento:  ' || V_END_TIME || ' [End Timestamp]');
    UTL_FILE.PUT_LINE(F_FILE_ERROR, '=========================================================================================');
    UTL_FILE.FCLOSE(F_FILE_ERROR);

EXCEPTION
    WHEN FILE_EXCEP THEN
        V_ERROR_MESSAGE := 'File Error: [' || V_ERROR_MESSAGE || ']';
        OUT_FILE_ERROR_CD := V_ERROR_CODE;
        OUT_FILE_ERROR_MSG := V_ERROR_MESSAGE;
        UTL_FILE.PUT_LINE(F_FILE_ERROR, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[File]' || '   -   '  || 'Error Code(s): [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message(s): [' || V_ERROR_MESSAGE || ']');
        UTL_FILE.PUT_LINE(F_FILE_ERROR, '=========================================================================================');
        UTL_FILE.FFLUSH(F_FILE_ERROR);
        UTL_FILE.FCLOSE(F_FILE_ERROR);

        SELECT SUBSTR(IN_FILE,1,LENGTH(IN_FILE) - 4) INTO F_NEW_FILE FROM DUAL;
        F_FILE_EXCEP := UTL_FILE.FOPEN(F_EXCEPT_DIR, F_NEW_FILE || '_' || IN_PROCESS_ID || '_EIM_FILE_EXCEP.log','a');
        UTL_FILE.PUT_LINE(F_FILE_EXCEP, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[File]' || '   -   '  || 'Error Code(s): [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message(s): [' || V_ERROR_MESSAGE || ']');
        UTL_FILE.FFLUSH(F_FILE_EXCEP);
        UTL_FILE.FCLOSE(F_FILE_EXCEP);
        
        F_FILE_JOB := UTL_FILE.FOPEN(F_LOG_DIR, IN_PROCESS_ID || '.log','a');
        UTL_FILE.PUT_LINE(F_FILE_JOB, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[File]' || '   -   '  || 'Error Code(s): [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message(s): [' || V_ERROR_MESSAGE || ']');
        UTL_FILE.PUT_LINE(F_FILE_JOB,'PROCESSO EXECUTADO COM ERRO. ENTRE EM CONTATO COM O ANALISTA');
        UTL_FILE.FFLUSH(F_FILE_JOB);
        UTL_FILE.FCLOSE(F_FILE_JOB);

    WHEN LOAD_EXCEP THEN
        V_ERROR_MESSAGE := 'Load Error: [' || V_ERROR_MESSAGE || ']';
        OUT_FILE_ERROR_CD := V_ERROR_CODE;
        OUT_FILE_ERROR_MSG := V_ERROR_MESSAGE;
        -- Logging to File
        UTL_FILE.PUT_LINE(F_FILE_ERROR, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[File]' || '   -   '  || 'Error Code(s): [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message(s): [' || V_ERROR_MESSAGE || ']');
        UTL_FILE.PUT_LINE(F_FILE_ERROR, '=========================================================================================');
        UTL_FILE.FFLUSH(F_FILE_ERROR);
        UTL_FILE.FCLOSE(F_FILE_ERROR);

        SELECT SUBSTR(IN_FILE,1,LENGTH(IN_FILE) - 4) INTO F_NEW_FILE FROM DUAL;
        F_FILE_EXCEP := UTL_FILE.FOPEN(F_EXCEPT_DIR, F_NEW_FILE || IN_PROCESS_ID || '_EIM_LOAD_EXCEP.log','a');
        UTL_FILE.PUT_LINE(F_FILE_EXCEP, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[File]' || '   -   '  || 'Error Code(s): [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message(s): [' || V_ERROR_MESSAGE || ']');
        UTL_FILE.FFLUSH(F_FILE_EXCEP);
        UTL_FILE.FCLOSE(F_FILE_EXCEP);
        
        F_FILE_JOB := UTL_FILE.FOPEN(F_LOG_DIR, IN_PROCESS_ID || '.log', 'a');
        UTL_FILE.PUT_LINE(F_FILE_JOB, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[File]' || '   -   '  || 'Error Code(s): [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message(s): [' || V_ERROR_MESSAGE || ']');
        UTL_FILE.PUT_LINE(F_FILE_JOB, 'PROCESSO EXECUTADO COM ERRO. ENTRE EM CONTATO COM O ANALISTA');
        UTL_FILE.PUT_LINE(F_FILE_JOB, 'Erro : ' || V_ERROR_MESSAGE);
        UTL_FILE.FFLUSH(F_FILE_JOB);
        UTL_FILE.FCLOSE(F_FILE_JOB);

    WHEN TOO_MANY_ROWS THEN
        V_ERROR_MESSAGE := '[FileTooManyRows]' || '   -   ' || 'Input File returned the following error: "TOO_MANY_ROWS".';
        OUT_FILE_ERROR_CD := '004';
        OUT_FILE_ERROR_MSG := V_ERROR_MESSAGE;

        -- Logging to File
        UTL_FILE.PUT_LINE(F_FILE_ERROR, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[File]' || '   -   '  || 'Error Code(s): [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message(s): [' || V_ERROR_MESSAGE || ']');
        UTL_FILE.PUT_LINE(F_FILE_ERROR,'=========================================================================================');
        UTL_FILE.FFLUSH(F_FILE_ERROR);
        UTL_FILE.FCLOSE(F_FILE_ERROR);

        SELECT SUBSTR(IN_FILE,1,LENGTH(IN_FILE) - 4) INTO F_NEW_FILE FROM DUAL;
        F_FILE_EXCEP := UTL_FILE.FOPEN(F_EXCEPT_DIR, F_NEW_FILE || '_' || IN_PROCESS_ID || '_EIM_OTHER_EXCEP.log','W');
        UTL_FILE.PUT_LINE(F_FILE_EXCEP, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[File]' || '   -   '  || 'Error Code(s): [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message(s): [' || V_ERROR_MESSAGE || ']');
        UTL_FILE.FFLUSH(F_FILE_EXCEP);
        UTL_FILE.FCLOSE(F_FILE_EXCEP);

        F_FILE_JOB := UTL_FILE.FOPEN(F_LOG_DIR, IN_PROCESS_ID || '.log','a');
        UTL_FILE.PUT_LINE(F_FILE_JOB, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[File]' || '   -   '  || 'Error Code(s): [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message(s): [' || V_ERROR_MESSAGE || ']');
        UTL_FILE.PUT_LINE(F_FILE_JOB,'PROCESSO EXECUTADO COM ERRO. ENTRE EM CONTATO COM O ANALISTA');
        UTL_FILE.FFLUSH(F_FILE_JOB);
        UTL_FILE.FCLOSE(F_FILE_JOB);

    WHEN OTHERS THEN
        V_ERROR_MESSAGE := TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || ' | Error: [' || SQLERRM || '] | Path: ' || F_DIRECTORY || ' | In File: ' || IN_FILE || ' | Error File: ' || F_LOG_FILE;
        OUT_FILE_ERROR_CD := V_ERROR_CODE;
        OUT_FILE_ERROR_MSG := V_ERROR_MESSAGE;

        -- Logging to File
        UTL_FILE.PUT_LINE(F_FILE_ERROR, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[File]' || '   -   '  || 'Error Code(s): [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message(s): [' || V_ERROR_MESSAGE || ']');
        UTL_FILE.PUT_LINE(F_FILE_ERROR,'=========================================================================================');
        UTL_FILE.FFLUSH(F_FILE_ERROR);
        UTL_FILE.FCLOSE(F_FILE_ERROR);
        
        SELECT SUBSTR(IN_FILE, 1, LENGTH(IN_FILE) - 4) INTO F_NEW_FILE FROM DUAL;
        F_FILE_EXCEP := UTL_FILE.FOPEN(F_EXCEPT_DIR, F_NEW_FILE || '_' || IN_PROCESS_ID || '_EIM_OTHER_EXCEP.log','W');
        
        UTL_FILE.PUT_LINE(F_FILE_EXCEP, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[File]' || '   -   '  || 'Error Code(s): [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message(s): [' || V_ERROR_MESSAGE || ']');
        UTL_FILE.FFLUSH(F_FILE_EXCEP);
        UTL_FILE.FCLOSE(F_FILE_EXCEP);

        F_FILE_JOB := UTL_FILE.FOPEN(F_LOG_DIR, IN_PROCESS_ID || '.log', 'a');
        UTL_FILE.PUT_LINE(F_FILE_JOB, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || '[File]' || '   -   '  || 'Error Code(s): [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message(s): [' || V_ERROR_MESSAGE || ']');
        UTL_FILE.PUT_LINE(F_FILE_JOB,'PROCESSO EXECUTADO COM ERRO. ENTRE EM CONTATO COM O ANALISTA');
        UTL_FILE.FFLUSH(F_FILE_JOB);
        UTL_FILE.FCLOSE(F_FILE_JOB);
END;