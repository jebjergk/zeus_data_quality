-- Controlled tag dimension for rule applicability metadata
CREATE TABLE IF NOT EXISTS ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_TAG_DIM (
    TAG_CODE VARCHAR,
    TAG_LABEL VARCHAR,
    IS_ACTIVE BOOLEAN,
    CONSTRAINT PK_DQ_TAG_DIM PRIMARY KEY (TAG_CODE)
);

MERGE INTO ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_TAG_DIM AS target
USING (
    SELECT COLUMN1 AS TAG_CODE, COLUMN1 AS TAG_LABEL
    FROM VALUES
        ('ID'),
        ('KEY'),
        ('AMOUNT'),
        ('PRICE'),
        ('QUANTITY'),
        ('COUNTRY_CODE'),
        ('CURRENCY_CODE'),
        ('ISIN'),
        ('WKN'),
        ('IBAN'),
        ('BIC'),
        ('EMAIL'),
        ('PHONE'),
        ('POSTAL_CODE'),
        ('TIMESTAMP'),
        ('DATE'),
        ('ENUM'),
        ('CATEGORICAL'),
        ('FREE_TEXT')
) AS source
ON UPPER(target.TAG_CODE) = UPPER(source.TAG_CODE)
WHEN MATCHED THEN UPDATE SET
    TAG_LABEL = COALESCE(target.TAG_LABEL, source.TAG_LABEL),
    IS_ACTIVE = COALESCE(target.IS_ACTIVE, TRUE)
WHEN NOT MATCHED THEN INSERT (TAG_CODE, TAG_LABEL, IS_ACTIVE)
VALUES (source.TAG_CODE, source.TAG_LABEL, TRUE);
