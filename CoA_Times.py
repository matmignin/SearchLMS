
import oracledb
import os
import pandas as pd
import openpyxl
from pandas import DataFrame
from dotenv import load_dotenv
load_dotenv()
user = os.environ.get('SQL_USER')
password = os.environ.get('SQL_PASSWORD')

sql="""
SELECT
    DISTINCT
    COASIGNED.PRODUCT                                    AS "Product",
    COASIGNED.BATCHNUMBER                                AS "Batch",
    DECODE(QCSUBMITTED.METHODS,'','Physical Only',
        QCSUBMITTED.COUNT||' — '
        || COASIGNED.COATED || QCSUBMITTED.METHODS)       AS "Methods",
    COASIGNED.CUSTOMER                                   AS "Customer",
    QCSUBMITTED.QC_SUBMITTED                             AS "QC Sample In",
    QCDONE.FIRST_SUBMISSION                              AS "Analytical Done",
    MICROSUBMITTED.MICRO_SUBMITTED                       AS "Micro Sample In",
    COASIGNED.FIRST_VERSION                              AS "Coa Issued"


FROM
    (
        SELECT
            DISTINCT TR.PRODUCT,
            TR.BATCHNUMBER,
            TR.REQUESTGUID,
            (
                SELECT
                    DISTINCT COUNT(1)
                FROM
                    TEST T2
                WHERE
                    T2.REQUESTGUID = TR.REQUESTGUID
                    AND T2.STATUS = 1000
                    AND T2.VALUATIONCODE = 1
                    AND T2.TESTGROUP = 'Ingredient'
            )                                                               AS COUNT,
                LISTAGG(TESTMETHODS.METHODID,
                ' | ') WITHIN GROUP (ORDER BY TR.BATCHNUMBER)
                OVER (PARTITION BY TR.BATCHNUMBER )                         AS METHODS,
                MIN(TO_DATE(SUBSTR(TR.SUBMITTEDDATE,1,8),
                    'YYYYMMDD')) OVER (PARTITION BY TR.BATCHNUMBER)         AS QC_SUBMITTED
            FROM
                TESTREQUEST TR
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
                ) TESTMETHODS
                ON TR.REQUESTGUID = TESTMETHODS.REQUESTGUID
            WHERE
                TR.STATUS = '2000'
                AND TR.CATEGORY = 'QC'
                AND TR.NOOFSIGNATURES = '0'
                AND TR.STABILITYTIMEPOINT IS NULL
    ) QCSUBMITTED

    JOIN (
        SELECT
            DISTINCT TR.PRODUCT,
            TR.BATCHNUMBER,
            MIN(TO_DATE(SUBSTR(FIRST_SUBMISSION.APPROVESIGNTIME,
            1,
            8),
            'YYYYMMDD')) OVER (
            PARTITION BY TR.BATCHNUMBER) AS FIRST_SUBMISSION
        FROM
            TESTREQUEST TR

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
    ) QCDONE
    ON QCSUBMITTED.BATCHNUMBER = QCDONE.BATCHNUMBER

    JOIN (
        SELECT
            DISTINCT PRODUCT,
            BATCHNUMBER,
            MIN(TO_DATE(SUBSTR(SUBMITTEDDATE,1,8),
            'YYYYMMDD')) OVER (
            PARTITION BY BATCHNUMBER) AS MICRO_SUBMITTED
        FROM
            TESTREQUEST
        WHERE
            SPECIFICATIONID LIKE '%Micro'
    ) MICROSUBMITTED
    ON QCSUBMITTED.BATCHNUMBER = MICROSUBMITTED.BATCHNUMBER
    AND QCSUBMITTED.PRODUCT = MICROSUBMITTED.PRODUCT

    JOIN (
        SELECT
            DISTINCT TR.PRODUCT,
            TR.BATCHNUMBER,
            TR.REQUESTGUID,
            PS.GENERIC02                 AS CUSTOMER,
            Decode(PS.GENERIC06,'','','Ct#  ')                 AS COATED,
            MIN(TO_DATE(SUBSTR(FIRSTVERSION.APPROVESIGNTIME,1,8),
            'YYYYMMDD')) OVER (
            PARTITION BY TR.BATCHNUMBER) AS FIRST_VERSION
        FROM
            TESTREQUEST TR

            JOIN (
                SELECT
                    REQUESTGUID,
                    APPROVESIGNTIME
                FROM
                    TESTREQUESTSIGN
            ) FIRSTVERSION
            ON TR.REQUESTGUID = FIRSTVERSION.REQUESTGUID
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
    ON QCSUBMITTED.BATCHNUMBER = COASIGNED.BATCHNUMBER
    AND QCSUBMITTED.PRODUCT = COASIGNED.PRODUCT
WHERE
    QCSUBMITTED.QC_SUBMITTED > :startdate
    AND (QCSUBMITTED.COUNT > 0
    OR QCSUBMITTED.METHODS IS NULL)
ORDER BY
    COASIGNED.FIRST_VERSION DESC"""

with oracledb.connect(user=user, password=password,host="nugenesis", port=1521, sid="NG9PRD") as connection:
	with connection.cursor() as cursor:
		df = pd.DataFrame(cursor.execute(sql, startdate="01-OCT-22"), columns=['Product','Batch','Methods','Customer','QC Sample In','Analytical Done','Micro Sample In','CoA Issued'], ignore_index=True)
df.to_excel("CoA Issues.xlsx", index=False)