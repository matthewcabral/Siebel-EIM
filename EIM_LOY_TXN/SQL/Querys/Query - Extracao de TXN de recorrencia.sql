SELECT /*+ PARALLEL(10) */
    TXN_CLB.CLB_MBR_ID       	AS "MEMBER_ID",             -- ID DO MEMBRO
    TXN_CLB.CLB_MBR_NUM      	AS "MEMBER_NUMBER",         -- NUMERO DO MEMBRO
    TXN_CLB.SIGNATURE_ID     	AS "CLUB_SIG_ID",           -- ID DA ASSINATURA DA ULTIMA TRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_CLB.SIG_CODE_SIGNING 	AS "CLUB_SIG_CODE",         -- CODIGO DA ASSINATURA DA ULTIMA TRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_CLB.SIG_STATUS       	AS "CLUB_SIG_STATUS",       -- STATUS DA ASSINATURA DA ULTIMA TRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_CLB.SIG_CONTRACT_PLAN	AS "CLUB_SIG_PLAN",         -- PLANO DA ASSINATURA DA ULTIMA TRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_CLB.SIG_CHANNEL      	AS "CLUB_SIG_CHANNEL",      -- CANAL DA ASSINATURA DA ULTIMA TRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_CLB.SIG_CUSTO_BASE   	AS "CLUB_SIG_BASE_COST",    -- CUSTO BASE DA ASSINATURA DA ULTIMA TRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_CLB.SIG_PLAN_TYPE    	AS "CLUB_SIG_PLAN_TYPE",    -- TIPO DO PLANO DA ASSINATURA DA ULTIMATRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_CLB.TXN_ID           	AS "CLUB_TXN_ID",           -- ID DA ULTIMA TRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_CLB.TXN_DATE         	AS "CLUB_TXN_DATE",         -- DATA DA ULTIMA TRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_CLB.TXN_TYPE         	AS "CLUB_TXN_TYPE",         -- TIPO DA ULTIMA TRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_CLB.TXN_SUB_TYPE     	AS "CLUB_TXN_SUBTYPE",      -- SUBTIPO DA ULTIMA TRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_CLB.TXN_PRODUCT      	AS "CLUB_TXN_PROD",         -- PRODUTO DA ULTIMA TRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_CLB.TXN_PRICE        	AS "CLUB_TXN_PRICE",        -- PRECO DA ULTIMA TRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_CLB.TXN_AMOUNT       	AS "CLUB_AMOUNT_ACCRUED",   -- TOTAL ACUMULADO DA ULTIMA TRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_CLB.TXN_STATUS       	AS "CLUB_TXN_STATUS",       -- STATUS DA ULTIMA TRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_CLB.TXN_SUB_STATUS   	AS "CLUB_TXN_SUBSTATUS",    -- SUBSTATUS DA ULTIMA TRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_CLB.TXN_CC_BIN       	AS "CLUB_TXN_CC_BIN",       -- BIN DA ULTIMA TRANSACAO DE CLUBE ANUAL OU MENSAL
    TXN_MB.MB_SIGNATURE_ID   	AS "MB_SIG_ID",             -- ID DA ASSINATURA DA ULTIMA TRANSACAO DE XXXXXXXXX
    TXN_MB.MB_SIG_CODE       	AS "MB_SIG_CODE",           -- CODIGO DA ASSINATURA DA TULTIMA RANSACAO DE CXXXXXXXXX
    TXN_MB.MB_SIG_STATUS     	AS "MB_SIG_STATUS",         -- STATUS DA ASSINATURA DA ULTIMA TRANSACAO DE XXXXXXXXX
    TXN_MB.MB_SIG_PLAN       	AS "MB_SIG_PLAN",           -- PLANO DA ASSINATURA DA ULTIMA TRANSACAO DE XXXXXXXXX
    TXN_MB.MB_SIG_CHANNEL    	AS "MB_SIG_CHANNEL",        -- CANAL DA ASSINATURA DA ULTIMA TRANSACAO DE XXXXXXXXX
    TXN_MB.MB_SIG_BASE_COST  	AS "MB_SIG_BASE_COST",      -- CUSTO BASE DA ASSINATURA DA ULTIMA TRANSACAO DE XXXXXXXXX
    TXN_MB.MB_SIG_PLAN_TYPE  	AS "MB_SIG_PLAN_TYPE",      -- TIPO DO PLANO DA ASSINATURA DA TRANSACAO DE XXXXXXXXX
    TXN_MB.TXN_ID            	AS "MB_TXN_ID",             -- ID DA ULTIMA TRANSACAO DE XXXXXXXXX
    TXN_MB.PAR_TXN_ID        	AS "MB_PAR_TXN_ID",         -- ID DA TRANSACAO DE CLUBE QUE A ULTIMA TRANSACAO XXXXXXXXX PERTENCE
    TXN_MB.SIGN_MEMBER_ID    	AS "MB_SIGN_ID",            -- ID DA ASSINATURA DA TRANSACAO DE CLUBE QUE A ULTIMA TRANSACAO XXXXXXXXX PERTENCE
    TXN_MB.TXN_DATE          	AS "MB_TXN_DATE",           -- DATA DA ULTIMA TRANSACAO DE XXXXXXXXX
    TXN_MB.TXN_TYPE          	AS "MB_TXN_TYPE",           -- TIPO DA ULTIMA TRANSACAO DE XXXXXXXXX
    TXN_MB.TXN_SUB_TYPE      	AS "MB_TXN_SUBTYPE",        -- SUBTIPO DA ULTIMA TRANSACAO DE XXXXXXXXX
    TXN_MB.TXN_PRODUCT       	AS "MB_TXN_PROD",           -- PRODUTO DA ULTIMA TRANSACAO DE XXXXXXXXX
    TXN_MB.TXN_AMOUNT        	AS "MB_AMOUNT_ACCRUED",     -- TOTAL ACUMULADO DA ULTIMA TRANSACAO DE XXXXXXXXX
    TXN_MB.TXN_STATUS        	AS "MB_TXN_STATUS",         -- STATUS DA ULTIMA TRANSACAO DE XXXXXXXXX
    TXN_MB.TXN_SUB_STATUS    	AS "MB_TXN_SUBSTATUS",      -- SUBSTATUS DA ULTIMA TRANSACAO DE XXXXXXXXX
    (
	-- ESSA SUBQUERY CALCULA A QUANTIDADE DE TRANSACOES DE CLUBE (MENSAL E ANUAL) EXISTEM ENTRE A ULTIMA TRANSACAO
	-- DE CLUBE QUE RECEBEU XXXXXXXXX E A TRANSACAO DE CLUBE ATUAL (QUE VAI RECEBER OU NAO O XXXXXXXXX DEPENDENDO DO RESULTADO)
        SELECT COUNT(*)
        FROM SIEBEL.S_LOY_TXN TXN
        WHERE 1=1
        AND TXN.PROD_ID IN (
            '1-1EBKHK19',						-- ID PROD CLUBE ANUAL
            '1-3WJZJGV'  						-- ID PROD CLUBE MENSAL
        )
        AND TXN.MEMBER_ID = TXN_CLB.CLB_MBR_ID	-- MEMBER DEVE SER IGUAL AO MEMBER RESULTADO NA QUERY PRINCIPAL
        AND TXN.TYPE_CD = 'ACCRUAL'             -- TIPO DA TRANSACAO (ACUMULO OU RESGATE)
        AND TXN.SUB_TYPE_CD = 'Product'         -- SUBTIPO DA TRANSACAO
        AND TXN.STATUS_CD = 'Processed'         -- STATUS DA TRANSACAO DE CLUBE
        AND TXN.SUB_STATUS_CD = 'Success'       -- SUB STATUS DA TRANSACAO DE CLUBE
        AND TXN.PROG_ID = '1-1W24X'             -- ID DO PROGRAMA (Programa AAAAAAA)
        AND TXN.TXN_DT > TXN_MB.TXN_DATE 		-- DATA DA ULTIMA TXN DE XXXXXXXXX
        AND TXN.TXN_DT < TXN_CLB.TXN_DATE 		-- DATA DA ULTIMA TXN DE CLUBE
    ) 							AS "GAP_BETWEEN_CLUB_MB"	-- QUANTIDADE DE TRANSACOES DE RECORRENCIA CRIADAS DESDE A ULTIMA TRANSACAO DE XXXXXXXXX
FROM (
    SELECT
        CLB_MBR_ID,
        CLB_MBR_NUM,
        SIGNATURE_ID,
        SIG_CODE_SIGNING,
        SIG_STATUS,
        SIG_CONTRACT_PLAN,
        SIG_CHANNEL,
        SIG_CUSTO_BASE,
        SIG_PLAN_TYPE,
        TXN_ID,
        TXN_DATE,
        TXN_TYPE,
        TXN_SUB_TYPE,
        TXN_PRODUCT,
        TXN_PRICE,
        TXN_AMOUNT,
        TXN_STATUS,
        TXN_SUB_STATUS,
        TXN_CC_BIN
    FROM (
        SELECT DISTINCT
            MBR.ROW_ID                            	AS "CLB_MBR_ID",
            MBR.MEM_NUM                           	AS "CLB_MBR_NUM",
            SIG.ROW_ID                            	AS "SIGNATURE_ID",
            SIG.X_ATTRIB_12                       	AS "SIG_CODE_SIGNING",
            SIG.X_ATTRIB_03                       	AS "SIG_STATUS",
            CLB.NAME                              	AS "SIG_CONTRACT_PLAN",
            CLB.ENROLL_CHANNEL                    	AS "SIG_CHANNEL",
            NVL(CLB.ANNUAL_COST, CLB.MONTHLY_COST)	AS "SIG_CUSTO_BASE",
            SIG.PLAN_TYPE                         	AS "SIG_PLAN_TYPE",
            TXN.ROW_ID                            	AS "TXN_ID",
            TXN.TXN_DT                            	AS "TXN_DATE",
            TXN.TYPE_CD                           	AS "TXN_TYPE",
            TXN.SUB_TYPE_CD                       	AS "TXN_SUB_TYPE",
            PRD.NAME                              	AS "TXN_PRODUCT",
            TXNX.ATTRIB_14                        	AS "TXN_PRICE",
            TXN.AMT_VAL                           	AS "TXN_AMOUNT",
            TXN.STATUS_CD                         	AS "TXN_STATUS",
            TXN.SUB_STATUS_CD                     	AS "TXN_SUB_STATUS",
            TXNX.ATTRIB_17                        	AS "TXN_CC_BIN",
            RANK() OVER (PARTITION BY MBR.MEM_NUM ORDER BY TXN.TXN_DT DESC, TXN.LAST_UPD DESC, TXN.ROW_ID DESC) ORDEM
        FROM SIEBEL.S_LOY_TXN TXN                                                                   -- TRANSACTION
        INNER JOIN SIEBEL.S_LOY_TXN_X TXNX ON TXNX.PAR_ROW_ID = TXN.ROW_ID                          -- TRANSACTION (EXTENDED)
        LEFT JOIN SIEBEL.S_PROD_INT PRD ON PRD.ROW_ID = TXN.PROD_ID                                 -- PRODUCT TXN
        INNER JOIN SIEBEL.CX_PAG_CLUB PAY ON TXN.ROW_ID = PAY.X_ATTRIB_07                           -- PAYMENTS
        INNER JOIN SIEBEL.CX_SIGN_MEMBER SIG ON PAY.X_ATTRIB_01 = SIG.ROW_ID 		                -- SIGNATURE
        INNER JOIN SIEBEL.CX_PARAM_CLUBE CLB ON CLB.ROW_ID = SIG.X_ATTRIB_04                        -- SIGNATURE CLUB (PLANOS DA ASSINATURA)
        INNER JOIN SIEBEL.S_LOY_MEMBER MBR ON SIG.X_ATTRIB_01 = MBR.ROW_ID			                -- MEMBER
        WHERE 1=1
        AND SIG.X_ATTRIB_03 = 'Active'                                                              -- CLUBE ATIVO
        AND TXNX.ATTRIB_17 IS NOT NULL                                                              -- BIN DA TXN NAO PODE SER NULO
        AND TXN.ROW_ID IN (
			-- ESSA SUBQUERY BUSCA TODAS AS TXNs DE RECORRENCIA DO CLUBE (MENSAL E ANUAL) QUE TENHAM SIDO
			-- CRIADAS CONFORME DATA DEFINIDA ABAIXO E NAO POSSUA UMA TXN DE XXXXXXXXX ASSOCIADA A ELA
            SELECT DISTINCT(TXNS1.ROW_ID)                                                               
            FROM SIEBEL.S_LOY_TXN TXNS1
            WHERE 1=1
            AND TXNS1.LAST_UPD >= TO_DATE('24/03/2021 00:00:00', 'DD/MM/YYYY HH24:MI:SS')			-- Data Minima parametrizavel
            AND TXNS1.LAST_UPD < TO_DATE('25/06/2022 00:00:00', 'DD/MM/YYYY HH24:MI:SS') 			-- Data Maxima parametrizavel
            AND TXNS1.TYPE_CD = 'ACCRUAL'                                                			-- TIPO DA TRANSACAO (ACUMULO OU RESGATE)
            AND TXNS1.SUB_TYPE_CD = 'Product'                                            			-- SUBTIPO DA TRANSACAO
            AND TXNS1.STATUS_CD = 'Processed'                                            			-- STATUS DA TRANSACAO DE CLUBE
            AND TXNS1.SUB_STATUS_CD = 'Success'                                          			-- SUB STATUS DA TRANSACAO DE CLUBE
            AND TXNS1.TXN_CHANNEL_CD IS NULL                                             			-- CANAL DA TRANSACAO (DEVE SER NULO POIS NA RECORRENCIA O MESMO NAO E PREENCHIDO)
            AND TXNS1.PROG_ID = '1-1W24X'                                                			-- ID DO PROGRAMA (Programa AAAAAAA)
            AND TXNS1.PROD_ID IN (
                '1-1EBKHK19',                                                            			-- ID PROD CLUBE ANUAL
                '1-3WJZJGV'                                                              			-- ID PROD CLUBE MENSAL
            )
            AND TXNS1.ROW_ID NOT IN (
                -- ESSA SUBQUERY SERVE PARA BUSCAR E FILTRAR NA QUERY SUPERIOR TODAS AS TXNs DE XXXXXXXXX JA CRIADAS
				-- E ASSOCIADAS A UMA TRANSACAO PRINCIPAL (PAI) - DE: PAR_TXN_ID (TXN XXXXXXXXX) | PARA: ROW_ID (TXN DE RECORRENCIA)
				SELECT
                    TXNS2.PAR_TXN_ID
                FROM SIEBEL.S_LOY_TXN TXNS2
                INNER JOIN SIEBEL.S_PROD_INT PRDS2 ON PRDS2.ROW_ID = TXNS2.PROD_ID
                INNER JOIN SIEBEL.S_LOY_PROGRAM PRGS2 ON PRGS2.ROW_ID = TXNS2.PROG_ID
                WHERE PRGS2.NAME = 'Programa AAAAAAA'                                            	-- NOME DO PROGRAMA
                AND PRDS2.PART_NUM LIKE 'XXXXXXXXX%'                                           	-- PRODUTOS DE XXXXXXXXX
                AND (                                                                           	
                    PRDS2.NAME LIKE '%Adesão%'                                                  	-- PRODUTO DE XXXXXXXXX NA ADESAO
                    OR PRDS2.NAME LIKE 'XXXXXXXXX Cartão%Club AAAAA'                               	-- PRODUTO DE XXXXXXXXX NA RECORRENCIA
                )                                                                               	
                AND TXNS2.TYPE_CD = 'ACCRUAL'                                                   	-- TIPO DA TRANSACAO (ACUMULO OU RESGATE)
                AND TXNS2.SUB_TYPE_CD = 'Product'                                               	-- SUBTIPO DA TRANSACAO
                AND TXNS2.STATUS_CD = 'Processed'                                               	-- STATUS DA TRANSACAO DE XXXXXXXXX
                AND TXNS2.SUB_STATUS_CD = 'Success'                                             	-- SUB STATUS DA TRANSACAO DE XXXXXXXXX
                AND TXNS2.PAR_TXN_ID IS NOT NULL                                                	-- PAR_TXN_ID NAO PODE SER NULO NESTA QUERY
                AND (                                                                           	
                    TXNS2.LAST_UPD >= TO_DATE('24/03/2021 00:00:00', 'DD/MM/YYYY HH24:MI:SS')   	-- Data Minima parametrizavel
                    AND TXNS2.LAST_UPD < TO_DATE('25/06/2022 00:00:00', 'DD/MM/YYYY HH24:MI:SS')	-- Data Maxima parametrizavel
                )
            )
        )
    )
    WHERE ORDEM = 1
) TXN_CLB
-- A QUERY ABAIXO RESUME-SE EM BUSCAR A ULTIMA TXN DE XXXXXXXXX DE ADESAO E/OU RECORRENCIA
-- QUE O MEMBER RECEBEU NO PERIODO PRE-DETERMINADO PARA UMA ASSINATURA COM STATUS DIFERENTE DE CANCELADO
LEFT JOIN (
    SELECT
        MB_MBR_ID,
        MB_MBR_NUM,
		MB_SIGNATURE_ID,
        MB_SIG_CODE,
        MB_SIG_STATUS,
        MB_SIG_PLAN,
        MB_SIG_CHANNEL,
        MB_SIG_BASE_COST,
        MB_SIG_PLAN_TYPE,
        TXN_ID,
        PAR_TXN_ID,
        SIGN_MEMBER_ID,
        TXN_DATE,
        TXN_TYPE,
        TXN_SUB_TYPE,
        TXN_PRODUCT,
        TXN_AMOUNT,
        TXN_STATUS,
        TXN_SUB_STATUS
    FROM (
        SELECT
            MBR.ROW_ID                            	AS "MB_MBR_ID",
            MBR.MEM_NUM                           	AS "MB_MBR_NUM",
            SIG.ROW_ID		                      	AS "MB_SIGNATURE_ID",
			SIG.X_ATTRIB_12                       	AS "MB_SIG_CODE",
            SIG.X_ATTRIB_03                       	AS "MB_SIG_STATUS",
            CLB.NAME                              	AS "MB_SIG_PLAN",
            CLB.ENROLL_CHANNEL                    	AS "MB_SIG_CHANNEL",
            NVL(CLB.ANNUAL_COST, CLB.MONTHLY_COST)	AS "MB_SIG_BASE_COST",
            SIG.PLAN_TYPE                         	AS "MB_SIG_PLAN_TYPE",
            TXN.ROW_ID                            	AS "TXN_ID",
            TXN.PAR_TXN_ID                        	AS "PAR_TXN_ID",
            TXNX.X_SIGN_MEMBER_ID                 	AS "SIGN_MEMBER_ID",
            TXN.TXN_DT                            	AS "TXN_DATE",
            TXN.TYPE_CD                           	AS "TXN_TYPE",
            TXN.SUB_TYPE_CD                       	AS "TXN_SUB_TYPE",
            PRD.NAME                              	AS "TXN_PRODUCT",
            TXN.AMT_VAL                           	AS "TXN_AMOUNT",
            TXN.STATUS_CD                         	AS "TXN_STATUS",
            TXN.SUB_STATUS_CD                     	AS "TXN_SUB_STATUS",
            RANK() OVER (PARTITION BY MBR.MEM_NUM ORDER BY TXN.TXN_DT DESC, TXN.LAST_UPD DESC, TXN.ROW_ID DESC) ORDEM
        FROM SIEBEL.S_LOY_TXN TXN
        INNER JOIN SIEBEL.S_LOY_TXN_X TXNX ON TXNX.PAR_ROW_ID = TXN.ROW_ID
        LEFT JOIN SIEBEL.S_PROD_INT PRD ON PRD.ROW_ID = TXN.PROD_ID
        LEFT JOIN SIEBEL.S_LOY_TXN TXNP ON TXNP.ROW_ID = TXN.PAR_TXN_ID
        LEFT JOIN SIEBEL.S_LOY_PROGRAM PRG ON PRG.ROW_ID = TXN.PROG_ID
        LEFT JOIN SIEBEL.S_LOY_MEMBER MBR ON MBR.ROW_ID = TXN.MEMBER_ID
        INNER JOIN SIEBEL.CX_SIGN_MEMBER SIG ON SIG.X_ATTRIB_01 = MBR.ROW_ID AND SIG.ROW_ID = TXNX.X_SIGN_MEMBER_ID
        INNER JOIN SIEBEL.CX_PARAM_CLUBE CLB ON CLB.ROW_ID = SIG.X_ATTRIB_04
        WHERE 1=1
        AND TXN.MEMBER_ID IS NOT NULL                                                   -- MEMBER NAO PODE SER NULO
        AND TXN.PROG_ID IS NOT NULL                                                     -- PROGRAMA NAO PODE SER NULO
        AND TXN.PROD_ID IS NOT NULL                                                     -- PRODUTO NAO PODE SER NULO
        AND SIG.X_ATTRIB_03 <> 'Cancelled'                                              -- ASSINATURA DEVE SER DIFERENTE DE CANCELADO
        AND PRG.NAME = 'Programa AAAAAAA'                                                -- NOME DO PROGRAMA
        AND PRD.PART_NUM LIKE 'XXXXXXXXX%'                                             -- PRODUTOS DE XXXXXXXXX
        AND (
            PRD.NAME LIKE '%Adesão%'                                                    -- PRODUTO DE XXXXXXXXX NA ADESAO
            OR PRD.NAME LIKE 'XXXXXXXXX Cartão%Club AAAAA'                                 -- PRODUTO DE XXXXXXXXX NA RECORRENCIA
        )
        AND TXN.TYPE_CD = 'ACCRUAL'                                                     -- TIPO DA TRANSACAO (ACUMULO OU RESGATE)
        AND TXN.SUB_TYPE_CD = 'Product'                                                 -- SUBTIPO DA TRANSACAO
        AND TXN.STATUS_CD = 'Processed'                                                 -- STATUS DA TRANSACAO DE XXXXXXXXX
        AND TXN.SUB_STATUS_CD = 'Success'                                               -- SUB STATUS DA TRANSACAO DE XXXXXXXXX
        AND TXN.PAR_TXN_ID IS NOT NULL                                                	-- PAR_TXN_ID NAO PODE SER NULO NESTA QUERY
		AND (
            TXN.LAST_UPD >= TO_DATE('24/03/2021 00:00:00', 'DD/MM/YYYY HH24:MI:SS')   	-- Data Minima parametrizavel
            AND TXN.LAST_UPD < TO_DATE('25/06/2022 00:00:00', 'DD/MM/YYYY HH24:MI:SS')	-- Data Maxima parametrizavel
        )
    ) WHERE ORDEM = 1
) TXN_MB ON TXN_MB.MB_MBR_ID = TXN_CLB.CLB_MBR_ID;