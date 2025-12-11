-- ClickHouse setup for merchant_application and user_merchant tables

-- 1. Create merchant_application table
CREATE TABLE IF NOT EXISTS default.merchant_application (
    id String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    created_by String,
    updated_by String,
    version Int64,
    application_name String,
    merchant_callback String,
    payee__account_holder_name String,
    payee__identification_1_number String,
    payee__identification_1_type String,
    payee__identification_2_number String,
    payee__identification_2_type String,
    payment_status_webhook_url String,
    primary_application_secret String,
    primary_application_secret_active Bool,
    secondary_application_secret String,
    secondary_application_secret_active Bool,
    merchant_id String,
    merchant_callback_url_for_web String,
    payee__address__street_name String,
    payee__address__building_number String,
    payee__address__post_code String,
    payee__address__town_name String,
    payee__address__country String,
    payee__address__department String,
    payee__address__sub_department String,
    payee__address__address_type String,
    institutions String,
    selected_yapily_client_id String,
    payee__modulr_customer_id String,
    payee__modulr_account_id String,
    payee__modulr_default_payout_beneficiary_id String,
    payee__modulr_is_sandbox_account Bool,
    refund_webhook_url String,
    payout_webhook_url String,
    cancel_url String,
    enforce_not_null_and_unique_merchant_payment_id Bool,
    send_webhook_for_external_payin_payment Bool,
    send_email_for_external_payin_payment Bool,
    confirmation_of_payer_enabled Bool,
    _kafka_topic String,
    _kafka_partition Int32,
    _kafka_offset Int64,
    _kafka_timestamp DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (created_at, id)
PARTITION BY toYYYYMM(created_at);

-- 2. Create user_merchant table
CREATE TABLE IF NOT EXISTS default.user_merchant (
    id String,
    company_name String,
    email String,
    phone String,
    full_name String,
    _kafka_topic String,
    _kafka_partition Int32,
    _kafka_offset Int64,
    _kafka_timestamp DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (email, id);

-- 3. Create Kafka tables in internal database
CREATE TABLE cdc_internal.kafka_merchant_application (
    after String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'payments-db.public.merchant_application',
    kafka_group_name = 'clickhouse_merchant_application',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE cdc_internal.kafka_user_merchant (
    after String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'payments-db.public.user_merchant',
    kafka_group_name = 'clickhouse_user_merchant',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- 4. Create materialized views
CREATE MATERIALIZED VIEW cdc_internal.merchant_application_mv TO default.merchant_application AS
SELECT
    JSONExtractString(after, 'id') as id,
    fromUnixTimestamp64Micro(toUInt64OrZero(JSONExtractString(after, 'created_at'))) as created_at,
    fromUnixTimestamp64Micro(toUInt64OrZero(JSONExtractString(after, 'updated_at'))) as updated_at,
    JSONExtractString(after, 'created_by') as created_by,
    JSONExtractString(after, 'updated_by') as updated_by,
    toInt64OrZero(JSONExtractString(after, 'version')) as version,
    JSONExtractString(after, 'application_name') as application_name,
    JSONExtractString(after, 'merchant_callback') as merchant_callback,
    JSONExtractString(after, 'payee__account_holder_name') as payee__account_holder_name,
    JSONExtractString(after, 'payee__identification_1_number') as payee__identification_1_number,
    JSONExtractString(after, 'payee__identification_1_type') as payee__identification_1_type,
    JSONExtractString(after, 'payee__identification_2_number') as payee__identification_2_number,
    JSONExtractString(after, 'payee__identification_2_type') as payee__identification_2_type,
    JSONExtractString(after, 'payment_status_webhook_url') as payment_status_webhook_url,
    JSONExtractString(after, 'primary_application_secret') as primary_application_secret,
    toBoolOrZero(JSONExtractString(after, 'primary_application_secret_active')) as primary_application_secret_active,
    JSONExtractString(after, 'secondary_application_secret') as secondary_application_secret,
    toBoolOrZero(JSONExtractString(after, 'secondary_application_secret_active')) as secondary_application_secret_active,
    JSONExtractString(after, 'merchant_id') as merchant_id,
    JSONExtractString(after, 'merchant_callback_url_for_web') as merchant_callback_url_for_web,
    JSONExtractString(after, 'payee__address__street_name') as payee__address__street_name,
    JSONExtractString(after, 'payee__address__building_number') as payee__address__building_number,
    JSONExtractString(after, 'payee__address__post_code') as payee__address__post_code,
    JSONExtractString(after, 'payee__address__town_name') as payee__address__town_name,
    JSONExtractString(after, 'payee__address__country') as payee__address__country,
    JSONExtractString(after, 'payee__address__department') as payee__address__department,
    JSONExtractString(after, 'payee__address__sub_department') as payee__address__sub_department,
    JSONExtractString(after, 'payee__address__address_type') as payee__address__address_type,
    JSONExtractString(after, 'institutions') as institutions,
    JSONExtractString(after, 'selected_yapily_client_id') as selected_yapily_client_id,
    JSONExtractString(after, 'payee__modulr_customer_id') as payee__modulr_customer_id,
    JSONExtractString(after, 'payee__modulr_account_id') as payee__modulr_account_id,
    JSONExtractString(after, 'payee__modulr_default_payout_beneficiary_id') as payee__modulr_default_payout_beneficiary_id,
    toBoolOrZero(JSONExtractString(after, 'payee__modulr_is_sandbox_account')) as payee__modulr_is_sandbox_account,
    JSONExtractString(after, 'refund_webhook_url') as refund_webhook_url,
    JSONExtractString(after, 'payout_webhook_url') as payout_webhook_url,
    JSONExtractString(after, 'cancel_url') as cancel_url,
    toBoolOrZero(JSONExtractString(after, 'enforce_not_null_and_unique_merchant_payment_id')) as enforce_not_null_and_unique_merchant_payment_id,
    toBoolOrZero(JSONExtractString(after, 'send_webhook_for_external_payin_payment')) as send_webhook_for_external_payin_payment,
    toBoolOrZero(JSONExtractString(after, 'send_email_for_external_payin_payment')) as send_email_for_external_payin_payment,
    toBoolOrZero(JSONExtractString(after, 'confirmation_of_payer_enabled')) as confirmation_of_payer_enabled,
    'payments-db.public.merchant_application' as _kafka_topic,
    0 as _kafka_partition,
    0 as _kafka_offset,
    now() as _kafka_timestamp
FROM cdc_internal.kafka_merchant_application
WHERE JSONExtractString(after, 'id') != '';

CREATE MATERIALIZED VIEW cdc_internal.user_merchant_mv TO default.user_merchant AS
SELECT
    JSONExtractString(after, 'id') as id,
    JSONExtractString(after, 'company_name') as company_name,
    JSONExtractString(after, 'email') as email,
    JSONExtractString(after, 'phone') as phone,
    JSONExtractString(after, 'full_name') as full_name,
    'payments-db.public.user_merchant' as _kafka_topic,
    0 as _kafka_partition,
    0 as _kafka_offset,
    now() as _kafka_timestamp
FROM cdc_internal.kafka_user_merchant
WHERE JSONExtractString(after, 'id') != '';
