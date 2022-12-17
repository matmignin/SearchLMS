SELECT
    DISTINCT COASIGNED.PRODUCT                              AS "Product",
    COASIGNED.BATCHNUMBER                                   AS "Batch",
    COASIGNED.COATED || DECODE(QC_IN.METHODS,'','Physical Only',
    QC_IN.METHODS)                                          AS "Methods",
    QC_IN.COUNT                                             AS "Methods Tested",
        -- || Decode(instr(QC_IN.METHODS,'('),0,'', ' ('|| REGEXP_COUNT(QC_IN.METHODS, '\(', 1) || ')')
    COASIGNED.CUSTOMER                                      AS "Customer",
    QC_IN.AC_IN                                             AS "QC In",
    QC_OUT.FIRST_SUBMISSION                                 AS "QC Done",
    MICRO.MICRO_IN                                          AS "Micro In",
            -- case
                -- when MICRO.MICRO_OUT > MICRO.MICRO_Signed
                -- THEN MICRO.MICRO_Signed
                -- else MICRO.MICRO_OUT
            -- END AS "Micro Done",
    MICRO.MICRO_OUT                                         AS "Micro Done",
    COASIGNED.FIRST_VERSION                                 AS "Coa Signed"
FROM
    --{ QC In}
    (
        SELECT
            DISTINCT TR.PRODUCT,
            TR.BATCHNUMBER,
            TR.REQUESTGUID,
            Count((
                SELECT DISTINCT
                    COUNT(1)
                FROM
                    TEST T2
                WHERE
                    T2.REQUESTGUID = TR.REQUESTGUID
                    AND T2.STATUS = 1000
                    AND T2.VALUATIONCODE = 1
                    AND T2.TESTGROUP = 'Ingredient'
                ))  OVER (PARTITION BY TR.BATCHNUMBER)                                                           AS COUNT,
                LISTAGG(TESTMETHODS.METHODID,' | ') WITHIN GROUP (ORDER BY TR.BATCHNUMBER)
                    OVER (PARTITION BY TR.BATCHNUMBER )                         AS METHODS,
                MIN(TO_DATE(SUBSTR(TR.SUBMITTEDDATE,1,8),
                    'YYYYMMDD')) OVER (PARTITION BY TR.BATCHNUMBER)         AS AC_IN
            FROM
                TESTREQUEST TR
                --+ Test Methods`
                LEFT JOIN (
                    SELECT
                        DISTINCT T.REQUESTGUID,
                        T.METHODID
                    FROM
                        TEST T
                    WHERE
                        T.STATUS = 1000
                        AND T.VALUATIONCODE = 1
                        AND T.TESTGROUP = 'Ingredient'
                        AND T.DELETION = 'N'
                        AND (T.CONFIGURATIONID = 'I, Analytical'
                        OR T.CONFIGURATIONID = 'Physical')
                ) TESTMETHODS
                ON TR.REQUESTGUID = TESTMETHODS.REQUESTGUID
            WHERE
                TR.STATUS = '2000'
                AND TR.CATEGORY = 'QC'
                AND TR.NOOFSIGNATURES = '0'
                AND TR.STABILITYTIMEPOINT IS NULL
    ) QC_IN

    --{ QC out}
    JOIN (
        SELECT
            DISTINCT TR.PRODUCT,
            TR.BATCHNUMBER,
            MIN(TO_DATE(SUBSTR(FIRST_SUBMISSION.APPROVESIGNTIME,1,8),'YYYYMMDD'))
            OVER (PARTITION BY TR.BATCHNUMBER) AS FIRST_SUBMISSION
        FROM
            TESTREQUEST TR
            --+ First Submission
            JOIN (
                SELECT
                    REQUESTGUID,
                    APPROVESIGNTIME
                FROM
                    TESTREQUESTSIGN
            ) FIRST_SUBMISSION
            ON TR.REQUESTGUID = FIRST_SUBMISSION.REQUESTGUID
        WHERE
            TR.STATUS = '2000'
            AND TR.CATEGORY = 'QC'
            AND TR.SUBMISSIONID IS NOT NULL
            AND TR.NOOFSIGNATURES = '0'
            AND TR.SUBMITTER IS NULL
            AND TR.STABILITYTIMEPOINT IS NULL
            AND TR.DELETION = 'N'
    ) QC_OUT
    ON QC_IN.BATCHNUMBER = QC_OUT.BATCHNUMBER

    --{ Micro In}
    JOIN (
        SELECT
            DISTINCT mi_tr.PRODUCT,
            mi_tr.BATCHNUMBER,
            MIN(TO_DATE(SUBSTR(mi_tr.SUBMITTEDDATE,1,8),'YYYYMMDD'))
            OVER (PARTITION BY mi_tr.BATCHNUMBER) AS MICRO_IN,
            -- Min(TO_DATE(SUBSTR(FIRST_MICRODONE.APPROVESIGNTIME,1,8),'YYYYMMDD') AS MICRO_OUT
            -- TO_DATE(SUBSTR(MI_TR.APPROVALDATE,1,8),'YYYYMMDD') AS MICRO_OUT


            MIN(TO_DATE(SUBSTR(FIRST_MICRODONE.APPROVESIGNTIME,1,8),'YYYYMMDD')) OVER (PARTITION BY mi_tr.BATCHNUMBER) AS MICRO_OUT
            -- MIN(TO_DATE(SUBSTR(MI_TR.APPROVALDATE,1,8),'YYYYMMDD')) OVER (PARTITION BY mi_tr.BATCHNUMBER) AS MICRO_OUT
        FROM
            TESTREQUEST mi_tr
            --{ Micro Done}
            JOIN (
                SELECT DISTINCT
                    REQUESTGUID,
                    APPROVESIGNTIME
                FROM
                    TESTREQUESTSIGN
            ) FIRST_MICRODONE
            ON mi_tr.REQUESTGUID = FIRST_MICRODONE.REQUESTGUID
        WHERE
            mi_TR.STATUS = '2000'
            and mi_tr.SPECIFICATIONID IS NOT NULL
            AND mi_TR.CATEGORY = 'Finished'

    ) MICRO
    ON QC_IN.BATCHNUMBER = MICRO.BATCHNUMBER
    AND QC_IN.PRODUCT = MICRO.PRODUCT

    -- Join (
    --     SELECT
    --         DISTINCT MDTR.PRODUCT,
    --         MDTR.BATCHNUMBER,
    --         MIN(TO_DATE(SUBSTR(MDTR.COMPLETEDATE,1,8),
    --         'YYYYMMDD')) OVER ( PARTITION BY MDTR.BATCHNUMBER) AS MICRO_COMPLETE,
    --         MIN(TO_DATE(SUBSTR(DECODE(FIRST_MICRODONE.APPROVESIGNTIME,'',MDTR.COMPLETEDATE,FIRST_MICRODONE.APPROVESIGNTIME),1,8),
    --         'YYYYMMDD')) OVER ( PARTITION BY MDTR.BATCHNUMBER)
    --         AS MICRO_SIGNED
    --         -- CASE
    --             -- WHEN FIRST_MICRODONE.APPROVESIGNTIME > MDTR.COMPLETEDATE
    --     FROM
    --         TESTREQUEST mdtr
    --         --+ First Micro Done
    --         JOIN (
    --             SELECT
    --                 REQUESTGUID,
    --                 APPROVESIGNTIME
    --             FROM
    --                 TESTREQUESTSIGN
    --         ) FIRST_MICRODONE
    --         ON MDTR.REQUESTGUID = FIRST_MICRODONE.REQUESTGUID
    --     WHERE
    --         -- MDTR.SPECIFICATIONID LIKE '%Micro'
    --         -- AND
    --         MDTR.STATUS = '2000'
    --         AND MDTR.CATEGORY = 'Finished'
    --         -- AND MDTR.SUBMITTER IS NOT NULL
    --         -- AND MDTR.NOOFSIGNATURES = '0'
    -- ) MICROFINISHED
    -- ON QC_IN.BATCHNUMBER = MICROFINISHED.BATCHNUMBER
    -- AND QC_IN.PRODUCT = MICROFINISHED.PRODUCT




    --{ CoA Signed}
    JOIN (
        SELECT
            DISTINCT TR.PRODUCT,
            TR.BATCHNUMBER,
            TR.REQUESTGUID,
            PS.GENERIC02                 AS CUSTOMER,
            Decode(PS.GENERIC06,'','','Ct#  ')                 AS COATED,
            MIN(TO_DATE(SUBSTR(FIRSTVERSION.APPROVESIGNTIME,1,8),
            'YYYYMMDD')) OVER (PARTITION BY TR.BATCHNUMBER) AS FIRST_VERSION
        FROM
            TESTREQUEST    TR
    --+ First Version
            JOIN (
                SELECT
                    REQUESTGUID,
                    APPROVESIGNTIME
                FROM
                    TESTREQUESTSIGN
            ) FIRSTVERSION
            ON TR.REQUESTGUID = FIRSTVERSION.REQUESTGUID


        --+ Get Customer Name
        JOIN PHYSICALSAMPLE PS
            ON TR.BATCHNUMBER = PS.BATCHNUMBER
            AND TR.PRODUCT = PS.PRODUCT
            AND PS.CONFIGURATIONID = 'F, Micro'
        WHERE
            TR.STATUS = '2000'
            AND TR.CATEGORY = 'Finished'
            AND TR.NOOFSIGNATURES = '1'
            AND TR.SPECIFICATIONID IS NULL
            AND TR.SUBMITTER IS NOT NULL
            AND PS.GENERIC02 IS NOT NULL
    ) COASIGNED
        ON QC_IN.BATCHNUMBER = COASIGNED.BATCHNUMBER
        AND QC_IN.PRODUCT = COASIGNED.PRODUCT


WHERE
    QC_IN.AC_IN > '01-JAN-20'
    -- AND COASIGNED.BATCHNUMBER = '106-1343'
    -- AND COASIGNED.BATCHNUMBER = '110-0664'
    -- AND COASIGNED.BATCHNUMBER = '106-1006'
    -- AND COASIGNED.BATCHNUMBER = '101-0094'
    -- AND COASIGNED.BATCHNUMBER = '107-0294'
    -- QC_IN.AC_IN > :startdate
    AND (QC_IN.COUNT > 0 OR QC_IN.METHODS IS NULL)


-- ORDER BY
    -- COASIGNED.FIRST_VERSION DESC;
    --COASIGNED.BATCHNUMBER DESC;

