CREATE TABLE IF NOT EXISTS public.job_info (
status VARCHAR(20),
time_updated date,
job_id BIGINT,
file_id VARCHAR(100) 
);

CREATE SEQUENCE IF NOT EXISTS job_id_sequence
    AS BIGINT
    INCREMENT BY 1
    START WITH 1
    OWNED BY public.job_info.job_id;

-- 2. Link the column to the sequence so it auto-fills
ALTER TABLE public.job_info 
ALTER COLUMN job_id SET DEFAULT nextval('job_id_sequence');


CREATE TABLE PUBLIC.transactions (
    id              BIGINT,
    date            TIMESTAMP,
    client_id       VARCHAR,
    card_id         VARCHAR,
    amount          DOUBLE PRECISION,
    use_chip        VARCHAR,
    merchant_id     VARCHAR,
    merchant_city   VARCHAR,
    merchant_state  VARCHAR,
    zip             VARCHAR,
    mcc             VARCHAR,
    errors          VARCHAR,
    reject_reason   VARCHAR
);



CREATE TABLE mcc_codes (
    mcc_code TEXT PRIMARY KEY,
    description TEXT
);
