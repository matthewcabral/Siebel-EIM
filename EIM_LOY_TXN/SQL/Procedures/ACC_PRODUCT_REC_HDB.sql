create or replace PROCEDURE "ACC_PRODUCT_REC_HDB" (IN_PROCESS_ID IN VARCHAR2, IN_FILE IN VARCHAR2, IN_OUT_DIR IN VARCHAR2, IN_LOG_DIR IN VARCHAR2, IN_FILE_START IN VARCHAR2, IN_FILE_END IN VARCHAR2, OUT_FILE_ERROR_CD OUT VARCHAR2, OUT_FILE_ERROR_MSG OUT VARCHAR2) IS
    /*
    =============================================================================================================================
    | Objective: Generate HandBack file result for PRODUCT XXXXXXXXXXXX Transaction import process          					|
    =============================================================================================================================
    | Version 	*    Date    *  Release * Developer  	* Project                                                           	|
    =============================================================================================================================
    |   01   	* 18/05/2022 * R05 2022 * MROSA  		* XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX					|
    =============================================================================================================================
    */

    V_HABDBACK_START        VARCHAR2(50)	:= TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS');	    -- INITIAL TIME OF PROCEDURE EXECUTION
    V_HANDBACK_END          VARCHAR2(50)	:= NULL;											-- FINAL TIME OF PROCEDURE EXECUTION

    ------------------------ FILE VARIABLES ------------------------
    --IN_FILE                 VARCHAR2(2000)	:= 'PRODUCT_recurrence_BR_simulation_dev.csv';    -- INPUT FILE NAME
    F_OUT_DIR               VARCHAR2(2000)      := IN_OUT_DIR;  -- OUTPUT FILE DIRECTORY
    F_LOG_DIR               VARCHAR2(2000)      := IN_LOG_DIR;  -- LOG EXCEPTION FILE DIRECTORY
    F_OUT_FILE_NAME         VARCHAR2(200)  	    := IN_PROCESS_ID || '_' || SUBSTR(IN_FILE, 1, LENGTH(IN_FILE) - 4) || '_' || 'HandBack.log';
    F_OUT_FILE_NAME_CSV     VARCHAR2(200)  	    := IN_PROCESS_ID || '_' || SUBSTR(IN_FILE, 1, LENGTH(IN_FILE) - 4) || '_' || 'HandBack.csv';
    F_OUT_FILE_RESULT       VARCHAR2(200)  	    := IN_PROCESS_ID || '_' || SUBSTR(IN_FILE, 1, LENGTH(IN_FILE) - 4) || '_' || 'HB_RESULT.csv';

    F_OUTPUT_FILE           UTL_FILE.FILE_TYPE;         -- LOG FILE
    F_OUTPUT_FILE_CSV       UTL_FILE.FILE_TYPE;         -- CSV LOG FILE
    F_OUTPUT_FILE_RESULT    UTL_FILE.FILE_TYPE;         -- RESULT LOG FILE
    F_LOG_FILE              UTL_FILE.FILE_TYPE;         -- LOG EXCEPTION FILE

    ------------------------ VARIABLES ------------------------
    V_MEMBER_TIER           VARCHAR2(50)    := NULL;
    V_PARTNER_ALIAS         VARCHAR2(50)    := NULL;
    V_PAR_TXN_ID            VARCHAR2(15)    := NULL;
    V_CC_BRAND              VARCHAR2(100)   := NULL;
    V_CC_BIN                VARCHAR2(50)    := NULL;
    V_LOG_MESSAGE           VARCHAR2(2000)  := NULL;

    ------------------------ ERROR VARIABLES ------------------------
    V_ERROR_MESSAGE         VARCHAR2(32767) := NULL;
    V_ERROR_CODE            VARCHAR2(32767) := NULL;
    V_FINAL_MESSAGE         VARCHAR2(32767) := NULL;

    ------------------------ TIMING VARIABLES ------------------------
    V_EXEC_TIME             VARCHAR2(50)    := NULL;
    V_LINE                  INTEGER;
    V_ERROR_TYPE            VARCHAR2(50)    := NULL;
    V_ERROR_CD_CSV          VARCHAR2(32767) := NULL;
    V_ERROR_MSG_CSV         VARCHAR2(32767) := NULL;

    ------------------------ COUNTERS ------------------------
    C_FILE_ERRORS           INTEGER;
    C_FILE_SUCCESS          INTEGER;
    C_EIM_INSERTED          INTEGER;
    C_EIM_SUCCESS           INTEGER;
    C_EIM_PART_SUCCESS      INTEGER;
    C_EIM_ERRORS            INTEGER;
    C_EIM_REC_COLS          INTEGER;
    C_EIM_DUP_REC           INTEGER;
    C_EIM_FK_ERRORS         INTEGER;
    C_EIM_PL_ERRORS         INTEGER;

    ------------------------ FILE EXCEPTION COLUMNS ------------------------
    FILE_EXCEP              EXCEPTION;
    LOAD_EXCEP              EXCEPTION;
    RECORD_EXCEP            EXCEPTION;
    DATE_EXCEP              EXCEPTION;
    POINT_EXCEP             EXCEPTION;
    POINT_EXCEP_ZERO        EXCEPTION;
    POINT_EXCEP_EMPTY       EXCEPTION;
    CHECK_FOORTER           EXCEPTION;
    PRODUCT_EXCEP           EXCEPTION;
    PRODUCT_EXCEP_CO        EXCEPTION;
    LAST_NAME_EMPTY_EXCEP   EXCEPTION;
    FST_NAME_EMPTY_EXCEP    EXCEPTION;
    FST_NAME_NOT_MATCH      EXCEPTION;
    LAST_NAME_NOT_MATCH     EXCEPTION;
    DATE_EMPTY              EXCEPTION;
    DATE_FUTURE             EXCEPTION;
    DATE_PAST               EXCEPTION;   
    MEMBER_DECEASED_EXCEP   EXCEPTION;
    MEMBER_ACTIVE_EXCEP     EXCEPTION;
    IS_DATE_EXCEP           EXCEPTION;

    PRAGMA EXCEPTION_INIT   (IS_DATE_EXCEP, -01843);
    PRAGMA EXCEPTION_INIT   (DATE_EXCEP, -01843);
    PRAGMA EXCEPTION_INIT   (DATE_EXCEP, -01847);
    PRAGMA EXCEPTION_INIT   (DATE_EXCEP, -01830);
    PRAGMA EXCEPTION_INIT   (POINT_EXCEP, -06502);

    CURSOR C_EIM_TXN_RESULT IS
        SELECT
            ETXN.ROW_ID                 AS "EIM_ID",
            ETXN.IF_ROW_BATCH_NUM       AS "EIM_BATCH_NUM",
            ETXN.IF_ROW_STAT            AS "EIM_STATUS",
            ETXN.T_LOY_TXN__RID         AS "TXN_ID",
            ETXN.MEMBER_MEM_NUM         AS "MEMBER_NUMBER",
            ETXN.AMT_VAL                AS "AMOUNT",
            ETXN.PROD_NAME              AS "PROD_NAME",
            ETXN.TXN_DT                 AS "TXN_DATE",
            ETXN.PARTNER_ACCNT_NAME     AS "PARTNER_NAME",
            ETXN.PROG_NAME              AS "PROGRAM_NAME",
            ETXN.PAR_TXN_NUM            AS "PAR_TXN_NUM",
            ETXN.PROCESSING_LOG         AS "EIM_LOG",
            ETXN.PROCESSING_COMMENT     AS "EIM_DESC"
        FROM SIEBEL.EIM_LOY_TXN ETXN
        WHERE ETXN.IF_ROW_BATCH_NUM IN ('25000', '99999')
        ORDER BY ETXN.CREATED ASC;

     FUNCTION F_COUNT_FILE_ERRORS RETURN INTEGER IS
        N INTEGER;
        BEGIN
            SELECT
                COUNT(*)
            INTO N
            FROM SIEBEL.EIM_LOY_TXN ETXN
            WHERE ETXN.IF_ROW_BATCH_NUM = '99999';

        RETURN N;
    END;

    FUNCTION F_COUNT_FILE_SUCCESS RETURN INTEGER IS
        N INTEGER;
        BEGIN
            SELECT
                COUNT(*)
            INTO N
            FROM SIEBEL.EIM_LOY_TXN ETXN
            WHERE ETXN.IF_ROW_BATCH_NUM IN ('25000');

        RETURN N;
    END;

    FUNCTION F_COUNT_EIM RETURN INTEGER IS
        N INTEGER;
        BEGIN
            SELECT
                COUNT(*)
            INTO N
            FROM SIEBEL.EIM_LOY_TXN ETXN
            WHERE ETXN.IF_ROW_BATCH_NUM IN ('25000', '99999');

        RETURN N;
    END;

    FUNCTION F_COUNT_EIM_SUCCESS RETURN INTEGER IS
        N INTEGER;
        BEGIN
            SELECT
                COUNT(*)
            INTO N
            FROM SIEBEL.EIM_LOY_TXN ETXN
            WHERE ETXN.IF_ROW_BATCH_NUM = '25000'
            AND ETXN.IF_ROW_STAT = 'IMPORTED';

        RETURN N;
    END;

    FUNCTION F_COUNT_EIM_PART_SUC RETURN INTEGER IS
        N INTEGER;
        BEGIN
            SELECT
                COUNT(*)
            INTO N
            FROM SIEBEL.EIM_LOY_TXN ETXN
            WHERE ETXN.IF_ROW_BATCH_NUM = '25000'
            AND ETXN.IF_ROW_STAT = 'PARTIALLY_IMPORTED';

        RETURN N;
    END;

    FUNCTION F_COUNT_EIM_ERRORS RETURN INTEGER IS
        N INTEGER;
        BEGIN
            SELECT
                COUNT(*)
            INTO N
            FROM SIEBEL.EIM_LOY_TXN ETXN
            WHERE ETXN.IF_ROW_BATCH_NUM = '25000'
            AND ETXN.IF_ROW_STAT <> 'IMPORTED';

        RETURN N;
    END;

    FUNCTION F_COUNT_EIM_REQ_COL RETURN INTEGER IS
        N INTEGER;
        BEGIN
            SELECT
                COUNT(*)
            INTO N
            FROM SIEBEL.EIM_LOY_TXN ETXN
            WHERE ETXN.IF_ROW_BATCH_NUM = '25000'
            AND ETXN.IF_ROW_STAT = 'REQUIRED_COLS';

        RETURN N;
    END;

    FUNCTION F_COUNT_EIM_DUP_REC RETURN INTEGER IS
        N INTEGER;
        BEGIN
            SELECT
                COUNT(*)
            INTO N
            FROM SIEBEL.EIM_LOY_TXN ETXN
            WHERE ETXN.IF_ROW_BATCH_NUM = '25000'
            AND ETXN.IF_ROW_STAT = 'DUP_RECORD_EXISTS';

        RETURN N;
    END;

    FUNCTION F_COUNT_EIM_FK_ERR RETURN INTEGER IS
        N INTEGER;
        BEGIN
            SELECT
                COUNT(*)
            INTO N
            FROM SIEBEL.EIM_LOY_TXN ETXN
            WHERE ETXN.IF_ROW_BATCH_NUM = '25000'
            AND ETXN.IF_ROW_STAT = 'FOREIGN_KEY';

        RETURN N;
    END;

    FUNCTION F_COUNT_EIM_PLST_ERR RETURN INTEGER IS
        N INTEGER;
        BEGIN
            SELECT
                COUNT(*)
            INTO N
            FROM SIEBEL.EIM_LOY_TXN ETXN
            WHERE ETXN.IF_ROW_BATCH_NUM = '25000'
            AND ETXN.IF_ROW_STAT = 'PICKLIST_VALUES';

        RETURN N;
    END;

    FUNCTION F_GET_MEMBER_TIER (IN_MEM_NUM IN VARCHAR) RETURN VARCHAR IS
        TIER_NAME VARCHAR2(50);
        BEGIN
            SELECT
                TIER.NAME AS "TIER_NAME"
            INTO TIER_NAME
            FROM SIEBEL.S_LOY_TIER TIER
            LEFT JOIN SIEBEL.S_LOY_MEM_TIER MTIER ON MTIER.TIER_ID = TIER.ROW_ID
            LEFT JOIN SIEBEL.S_LOY_MEMBER MBR ON MBR.ROW_ID = MTIER.MEMBER_ID
            WHERE MBR.MEM_NUM = IN_MEM_NUM
            AND MTIER.ACTIVE_FLG = 'Y';

        RETURN TIER_NAME;
    END;

    FUNCTION F_GET_PARTNER_ALIAS (IN_PARTNER_NAME IN VARCHAR, IN_PROG_NAME IN VARCHAR) RETURN VARCHAR IS
        PARTNER_ALIAS VARCHAR2(50);
        BEGIN
            SELECT
                PAR.ALIAS_NAME      AS "PARTNER_ALIAS"
            INTO PARTNER_ALIAS
            FROM SIEBEL.S_LOY_PROGRAM PROG
            INNER JOIN SIEBEL.S_LOY_PROG_ORG PORG ON PORG.PROG_ID = PROG.ROW_ID
            INNER JOIN SIEBEL.S_ORG_EXT PAR ON PAR.ROW_ID = PORG.ORG_EXT_ID
            WHERE PAR.NAME = IN_PARTNER_NAME
            AND PROG.NAME = IN_PROG_NAME;

        RETURN PARTNER_ALIAS;
    END;

BEGIN
    DBMS_OUTPUT.PUT_LINE('IN_FILE_START: ' || IN_FILE_START);
    DBMS_OUTPUT.PUT_LINE('IN_FILE_END: ' || IN_FILE_END);
    -------------------------- OPENING ERROR FILE FOR LOGGING PROCESS IN APPEND MODE ---------------------------
    BEGIN
        F_OUTPUT_FILE := UTL_FILE.FOPEN(F_OUT_DIR, F_OUT_FILE_NAME, 'a');
    EXCEPTION
		WHEN OTHERS THEN
            V_ERROR_CODE := '001';
			V_ERROR_MESSAGE := '[CouldNotOpenLOGFile]' || '   -   ' || 'Could not open LOG File.';
            RAISE FILE_EXCEP;
    END;

    ----------------------------------- OPENING CSV ERROR FILE FOR LOGGING PROCESS IN APPEND MODE -----------------------------------
    BEGIN
        F_OUTPUT_FILE_CSV := UTL_FILE.FOPEN(F_OUT_DIR, F_OUT_FILE_NAME_CSV, 'a');
    EXCEPTION
		WHEN OTHERS THEN
            V_ERROR_CODE := '002';
			V_ERROR_MESSAGE := '[CouldNotOpenLOGFile]' || '   -   ' || 'Could not open CSV LOG File.';
            RAISE FILE_EXCEP;
    END;

    ----------------------------------- OPENING CSV RESULT LOG FILE FOR LOGGING PROCESS IN APPEND MODE -----------------------------------
    BEGIN
        F_OUTPUT_FILE_RESULT := UTL_FILE.FOPEN(F_OUT_DIR, F_OUT_FILE_RESULT, 'a');
    EXCEPTION
		WHEN OTHERS THEN
            V_ERROR_CODE := '003';
			V_ERROR_MESSAGE := '[CouldNotOpenResultFile]' || '   -   ' || 'Could not open Result LOG File.';
            RAISE FILE_EXCEP;
    END;

    ----------------------------------- LOG FILE -----------------------------------
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, '===========================================================================================================================================');
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Starting PRODUCT EIM Import process validation');
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Log File generated: ' || F_OUT_FILE_NAME);
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'CSV Log File generated: ' || F_OUT_FILE_NAME_CSV);
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, '===========================================================================================================================================');
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, '');

    ----------------------------------- CSV FILE HEADER -----------------------------------
    UTL_FILE.PUT_LINE(
        F_OUTPUT_FILE_CSV,
        'EXECUTING_TIME;' ||
        'LINE;' ||
        'HANDBACK_NAME;' ||
        'TRANSACTION_ID;' ||
        'MEMBER_NUMBER;' ||
        'TOTAL_MILES;' ||
        'PRODUCT_NAME;' ||
        'MEMBER_TIER;' ||
        'PARTNER_ALIAS;' ||
        'TRANSACTION_DT;' ||
        'PARENT_TXN_ID;' ||
        'CARD_BRAND;' ||
        'CARD_BIN;' ||
        'ERROR_TYPE;' ||
        'ERROR_CODE;' ||
        'ERROR_MESSAGE'
    );

    ----------------------------------- COUNTERS -----------------------------------
    C_FILE_ERRORS       := F_COUNT_FILE_ERRORS;
    C_FILE_SUCCESS      := F_COUNT_FILE_SUCCESS;
    C_EIM_INSERTED      := F_COUNT_EIM;
    C_EIM_ERRORS        := F_COUNT_EIM_ERRORS;
    C_EIM_PART_SUCCESS  := F_COUNT_EIM_PART_SUC;
    C_EIM_SUCCESS       := F_COUNT_EIM_SUCCESS;
    C_EIM_REC_COLS      := F_COUNT_EIM_REQ_COL;
    C_EIM_DUP_REC       := F_COUNT_EIM_DUP_REC;
    C_EIM_FK_ERRORS     := F_COUNT_EIM_FK_ERR;
    C_EIM_PL_ERRORS     := F_COUNT_EIM_PLST_ERR;

    ----------------------------------- LOOPING EIM TABLE -----------------------------------
    FOR I IN C_EIM_TXN_RESULT LOOP
        V_FINAL_MESSAGE     := TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ';
        V_EXEC_TIME         := TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS');
        V_LINE              := I.EIM_ID;
        V_MEMBER_TIER       := NULL;
        V_PARTNER_ALIAS     := NULL;
        V_PAR_TXN_ID        := NULL;
        V_CC_BRAND          := NULL;
        V_CC_BIN            := NULL;
        V_LOG_MESSAGE       := NULL;
        V_ERROR_TYPE        := NULL;
        V_ERROR_CD_CSV      := NULL;
        V_ERROR_MSG_CSV     := NULL;

        V_MEMBER_TIER       := F_GET_MEMBER_TIER(I.MEMBER_NUMBER);
        V_PARTNER_ALIAS     := F_GET_PARTNER_ALIAS(I.PARTNER_NAME, I.PROGRAM_NAME);

        BEGIN
            SELECT
                TXN.ROW_ID          AS "PARENT_TXN_ID",
                TXNX.ATTRIB_44      AS "CC_BRAND",
                TXNX.ATTRIB_17      AS "CC_BIN"
            INTO V_PAR_TXN_ID, V_CC_BRAND, V_CC_BIN
            FROM SIEBEL.S_LOY_TXN TXN
            INNER JOIN SIEBEL.S_LOY_TXN_X TXNX ON TXNX.PAR_ROW_ID = TXN.ROW_ID
            WHERE TXN.TXN_NUM = I.PAR_TXN_NUM;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                V_PAR_TXN_ID        := NULL;
                V_CC_BRAND          := NULL;
                V_CC_BIN            := NULL;
        END;

        V_LOG_MESSAGE :=
            V_LOG_MESSAGE ||
            V_LINE || '   -   ' ||
            'File Name: "' || F_OUT_FILE_NAME_CSV || '"   -   ' ||
            'TXN Id: "' || I.TXN_ID || '"   -   ' ||
            'Member Number: "' || I.MEMBER_NUMBER || '"   -   ' ||
            'Total Miles: "' || I.AMOUNT || '"   -   ' ||
            'Product Name: "' || I.PROD_NAME || '"   -   ' ||
            'Member Tier: "' || V_MEMBER_TIER || '"   -   ' ||
            'Partner Alias: "' || V_PARTNER_ALIAS || '"   -   ' ||
            'Transaction Date: "' || I.TXN_DATE || '"   -   ' ||
            'Parent TXN Id: "' || V_PAR_TXN_ID || '"   -   ' ||
            'Card Brand: "' || V_CC_BRAND || '"   -   ' ||
            'Card Bin: "' || V_CC_BIN || '"';
        
        IF (I.EIM_BATCH_NUM = '25000') THEN
            IF (I.EIM_STATUS = 'IMPORTED') THEN
                V_FINAL_MESSAGE := V_FINAL_MESSAGE || 'Success at Line ' || V_LOG_MESSAGE || '   -   ' || '[EIM Success]'  || '   -   ' || 'The PRODUCT Transaction with Id "' || I.TXN_ID ||'" was created successfully for the Member Number "' || I.MEMBER_NUMBER || '".';
                V_ERROR_TYPE := 'EIM Success';
                V_ERROR_CD_CSV := I.EIM_STATUS;
                V_ERROR_MSG_CSV := 'The PRODUCT Transaction with Id "' || I.TXN_ID ||'" was created successfully for the Member Number "' || I.MEMBER_NUMBER || '".';
            ELSIF (I.EIM_STATUS = 'PARTIALLY_IMPORTED') THEN
                V_FINAL_MESSAGE := V_FINAL_MESSAGE || 'Error at Line ' || V_LOG_MESSAGE || '   -   ' || '[EIM Part. Success]'  || '   -   ' || 'Error Code(s): [' || I.EIM_STATUS || ']' || '   -   ' || 'Error Message(s): [The PRODUCT with Id "' || I.TXN_ID ||'" was partially imported.]';
                V_ERROR_TYPE := 'EIM Part. Success';
                V_ERROR_CD_CSV := I.EIM_STATUS;
                V_ERROR_MSG_CSV := 'The PRODUCT with Id "' || I.TXN_ID ||'" was partially imported.';
            ELSIF (I.EIM_STATUS = 'REQUIRED_COLS') THEN
                V_FINAL_MESSAGE := V_FINAL_MESSAGE || 'Error at Line ' || V_LOG_MESSAGE || '   -   ' || '[EIM Error 1]'  || '   -   ' || 'Error Code(s): [' || I.EIM_STATUS || ']' || '   -   ' || 'Error Message(s): [Required Columns not filled.]';
                V_ERROR_TYPE := 'EIM Error 1';
                V_ERROR_CD_CSV := I.EIM_STATUS;
                V_ERROR_MSG_CSV := 'Error Message(s): [Required Columns not filled.';
            ELSIF (I.EIM_STATUS = 'DUP_RECORD_EXISTS') THEN
                V_FINAL_MESSAGE := V_FINAL_MESSAGE || 'Error at Line ' || V_LOG_MESSAGE || '   -   ' || '[EIM Error 2]'  || '   -   ' || 'Error Code(s): [' || I.EIM_STATUS || ']' || '   -   ' || 'Error Message(s): [It was found Duplicated Records for the current line.]';
                V_ERROR_TYPE := 'EIM Error 2';
                V_ERROR_CD_CSV := I.EIM_STATUS;
                V_ERROR_MSG_CSV := 'It was found Duplicated Records for the current line.';
            ELSIF (I.EIM_STATUS = 'FOREIGN_KEY') THEN
                V_FINAL_MESSAGE := V_FINAL_MESSAGE || 'Error at Line ' || V_LOG_MESSAGE || '   -   ' || '[EIM Error 3]'  || '   -   ' || 'Error Code(s): [' || I.EIM_STATUS || ']' || '   -   ' || 'Error Message(s): [The Foreign Key was not totally completed or resolved.';
                V_ERROR_TYPE := 'EIM Error 3';
                V_ERROR_CD_CSV := I.EIM_STATUS;
                V_ERROR_MSG_CSV := 'Error Message(s): [The Foreign Key was not totally completed or resolved.';
            ELSIF (I.EIM_STATUS = 'PICKLIST_VALUE') THEN
                V_FINAL_MESSAGE := V_FINAL_MESSAGE || 'Error at Line ' || V_LOG_MESSAGE || '   -   ' || '[EIM Error 4]'  || '   -   ' || 'Error Code(s): [' || I.EIM_STATUS || ']' || '   -   ' || 'Error Message(s): [The Picklist Field Value was not found.]';
                V_ERROR_TYPE := 'EIM Error 4';
                V_ERROR_CD_CSV := I.EIM_STATUS;
                V_ERROR_MSG_CSV := 'The Picklist Field Value was not found.';
            ELSE
                V_FINAL_MESSAGE := V_FINAL_MESSAGE || 'Error at Line ' || V_LOG_MESSAGE || '   -   ' || '[EIM Error 5]'  || '   -   ' || 'Error Code(s): [' || I.EIM_STATUS || ']' || '   -   ' || 'Error Message(s): [' || I.EIM_DESC || ']';
                V_ERROR_TYPE := 'EIM Error 5';
                V_ERROR_CD_CSV := I.EIM_STATUS;
                V_ERROR_MSG_CSV := I.EIM_DESC;
            END IF;
        ELSE
            V_FINAL_MESSAGE := V_FINAL_MESSAGE || 'Error at Line ' || V_LOG_MESSAGE || '   -   ' || '[File Error]'  || '   -   ' || 'Error Code(s): [' || I.EIM_LOG || ']' || '   -   ' || 'Error Message(s): [' || I.EIM_DESC || ']';
            V_ERROR_TYPE := 'File Error';
            V_ERROR_CD_CSV := I.EIM_STATUS;
            V_ERROR_MSG_CSV := I.EIM_DESC;
        END IF;

        UTL_FILE.PUT_LINE(F_OUTPUT_FILE, V_FINAL_MESSAGE);
        UTL_FILE.PUT_LINE(
            F_OUTPUT_FILE_CSV, 
            V_EXEC_TIME || ';' || 
            V_LINE || ';' || 
            F_OUT_FILE_NAME_CSV || ';' || 
            I.TXN_ID || ';' || 
            I.MEMBER_NUMBER || ';' || 
            I.AMOUNT || ';' || 
            I.PROD_NAME || ';' || 
            V_MEMBER_TIER || ';' || 
            V_PARTNER_ALIAS || ';' || 
            I.TXN_DATE || ';' || 
            V_PAR_TXN_ID || ';' || 
            V_CC_BRAND || ';' || 
            V_CC_BIN || ';' || 
            V_ERROR_TYPE || ';' || 
            V_ERROR_CD_CSV || ';' || 
            V_ERROR_MSG_CSV
        );
    END LOOP;

    V_HANDBACK_END := TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS');
    OUT_FILE_ERROR_CD := '0';
    OUT_FILE_ERROR_MSG := 'SUCESS';

    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, '');
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, '===========================================================================================================================================');
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'File Processing Start Time: ' || '           ' || IN_FILE_START); -- TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS')
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Total of File Lines: ' || '                  ' || C_EIM_INSERTED || ' [Detail Records]');
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Total of File Lines OK: ' || '               ' || C_FILE_SUCCESS || ' [Lines without error(s)]');
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Total of File Lines ERROR: ' || '            ' || C_FILE_ERRORS || ' [Lines with error(s)]');
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'File Processing End Time: ' || '             ' || IN_FILE_END); -- TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS')
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, '');
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'EIM Processing Start Time: ' || '            ' || V_HABDBACK_START); -- TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS')
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Total of EIM Import SUCCESS: ' || '          ' || C_EIM_SUCCESS || ' [EIM lines imported Successfully]');
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Total of EIM Partially Imported: ' || '      ' || F_COUNT_EIM_PART_SUC || ' [EIM lines partially imported]');
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Total of EIM Import ERROR: ' || '            ' || C_EIM_ERRORS || ' [EIM lines with error(s)]');
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Total of EIM Required Column ERROR: ' || '   ' || C_EIM_REC_COLS || ' [EIM lines with Required Columns error(s)]');
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Total of EIM Duplicated Record ERROR: ' || ' ' || C_EIM_DUP_REC || ' [EIM lines with Duplicated Record error(s)]');
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Total of EIM Foreing Key ERROR: ' || '       ' || C_EIM_FK_ERRORS || ' [EIM lines with Foreing Key error(s)]');
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Total of EIM PickList ERROR: ' || '          ' || C_EIM_PL_ERRORS || ' [EIM lines with PickList error(s)]');
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'EIM Processing End Time: ' || '              ' || V_HANDBACK_END); -- TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS')
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE, '===========================================================================================================================================');

    ----------------------------------- RESULT FILE -----------------------------------
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE_RESULT, 'OUTPUT LOG FILE NAME;' || F_OUT_FILE_NAME);
    UTL_FILE.PUT_LINE(F_OUTPUT_FILE_RESULT, 'OUTPUT CSV FILE NAME;' || F_OUT_FILE_NAME_CSV);
	UTL_FILE.PUT_LINE(F_OUTPUT_FILE_RESULT, 'FILE PROCESSING START TIME;' || IN_FILE_START);
	UTL_FILE.PUT_LINE(F_OUTPUT_FILE_RESULT, 'FILE TOTAL LINES;' || C_EIM_INSERTED);
	UTL_FILE.PUT_LINE(F_OUTPUT_FILE_RESULT, 'FILE TOTAL LINES OK;' || C_FILE_SUCCESS);
	UTL_FILE.PUT_LINE(F_OUTPUT_FILE_RESULT, 'FILE TOTAL LINES ERROR;' || C_FILE_ERRORS);
	UTL_FILE.PUT_LINE(F_OUTPUT_FILE_RESULT, 'FILE PROCESSING END TIME;' || IN_FILE_END);
	UTL_FILE.PUT_LINE(F_OUTPUT_FILE_RESULT, 'EIM PROCESSING START TIME;' || V_HABDBACK_START);
	UTL_FILE.PUT_LINE(F_OUTPUT_FILE_RESULT, 'EIM TOTAL IMPORT SUCCESS;' || C_EIM_SUCCESS);
	UTL_FILE.PUT_LINE(F_OUTPUT_FILE_RESULT, 'EIM TOTAL IMPORT PARTIALLY SUCCESS;' || F_COUNT_EIM_PART_SUC);
	UTL_FILE.PUT_LINE(F_OUTPUT_FILE_RESULT, 'EIM TOTAL IMPORT ERRORS;' || C_EIM_ERRORS);
	UTL_FILE.PUT_LINE(F_OUTPUT_FILE_RESULT, 'EIM TOTAL IMPORT ERROR REQ. COLUMN;' || C_EIM_REC_COLS);
	UTL_FILE.PUT_LINE(F_OUTPUT_FILE_RESULT, 'EIM TOTAL IMPORT ERROR DUP. RECORDS;' || C_EIM_DUP_REC);
	UTL_FILE.PUT_LINE(F_OUTPUT_FILE_RESULT, 'EIM TOTAL IMPORT ERROR F. KEYS;' || C_EIM_FK_ERRORS);
	UTL_FILE.PUT_LINE(F_OUTPUT_FILE_RESULT, 'EIM TOTAL IMPORT ERROR PICKLIST;' || C_EIM_PL_ERRORS);
	UTL_FILE.PUT_LINE(F_OUTPUT_FILE_RESULT, 'EIM PROCESSING END TIME;' || V_HANDBACK_END);

    UTL_FILE.FFLUSH(F_OUTPUT_FILE_CSV);
    UTL_FILE.FFLUSH(F_OUTPUT_FILE_RESULT);
    UTL_FILE.FFLUSH(F_OUTPUT_FILE);
    UTL_FILE.FCLOSE(F_OUTPUT_FILE_CSV);
    UTL_FILE.FCLOSE(F_OUTPUT_FILE_RESULT);
    UTL_FILE.FCLOSE(F_OUTPUT_FILE);

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

EXCEPTION
    WHEN FILE_EXCEP THEN
        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ' || SQLERRM || '   |   ' || 'Path: ' || F_LOG_DIR || '   |   ' || 'In File: ' || IN_FILE ;
        OUT_FILE_ERROR_CD := V_ERROR_CODE;
        OUT_FILE_ERROR_MSG := V_ERROR_MESSAGE;

        UTL_FILE.FCLOSE_ALL();
        F_LOG_FILE := UTL_FILE.FOPEN(F_LOG_DIR, SUBSTR(IN_FILE, 1, LENGTH(IN_FILE) - 4)  || '_' || IN_PROCESS_ID || '_' || 'HandBack_FILE_EXCEP.log', 'a');
        UTL_FILE.PUT_LINE(F_LOG_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Error Code: [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message: [' || V_ERROR_MESSAGE || ']');
        UTL_FILE.FFLUSH(F_LOG_FILE);
        UTL_FILE.FCLOSE(F_LOG_FILE);
    WHEN OTHERS THEN
        V_ERROR_MESSAGE := V_ERROR_MESSAGE || '   |   ' || SQLERRM || '   |   ' || 'Path: ' || F_LOG_DIR || '   |   ' || 'In File: ' || IN_FILE ;
        OUT_FILE_ERROR_CD := V_ERROR_CODE;
        OUT_FILE_ERROR_MSG := V_ERROR_MESSAGE;

        UTL_FILE.FCLOSE_ALL();
        F_LOG_FILE := UTL_FILE.FOPEN(F_LOG_DIR, SUBSTR(IN_FILE, 1, LENGTH(IN_FILE) - 4)  || '_' || IN_PROCESS_ID || '_' || 'HandBack_OTHERS_EXCEP.log', 'a');
        UTL_FILE.PUT_LINE(F_LOG_FILE, TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS') || '   -   ' || 'Error Code: [' || V_ERROR_CODE || ']' || '   -   ' || 'Error Message: [' || V_ERROR_MESSAGE || ']');
        UTL_FILE.FFLUSH(F_LOG_FILE);
        UTL_FILE.FCLOSE(F_LOG_FILE);
END;