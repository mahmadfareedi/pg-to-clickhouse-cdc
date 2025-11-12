-- ClickHouse Table Setup for CDC
-- This script creates the exact table structure matching PostgreSQL wide_table_100cols

-- Main table with exact schema match (102 columns)
CREATE TABLE wide_table_100cols (
  id Int64,
  created_at DateTime64(6, 'UTC'),

  -- Numeric types
  smallint_col Nullable(Int16),
  integer_col Nullable(Int32),
  bigint_col Nullable(Int64),
  numeric_col Nullable(Decimal(38, 20)),
  decimal_col Nullable(Decimal(18, 4)),
  real_col Nullable(Float32),
  double_col Nullable(Float64),
  money_col Nullable(String),

  -- Character/String types
  char_col Nullable(FixedString(10)),
  varchar_col Nullable(String),
  text_col Nullable(String),
  citext_col Nullable(String),

  -- Binary
  bytea_col Nullable(String),

  -- Boolean
  bool_col Nullable(Bool),

  -- Date/Time types
  date_col Nullable(Date),
  time_col Nullable(String),
  timetz_col Nullable(String),
  timestamp_col Nullable(DateTime64(6)),
  timestamptz_col Nullable(DateTime64(6, 'UTC')),
  interval_col Nullable(String),

  -- UUID
  uuid_col Nullable(UUID),

  -- JSON / XML
  json_col Nullable(String),
  jsonb_col Nullable(String),
  xml_col Nullable(String),

  -- Bit strings
  bit_col Nullable(String),
  varbit_col Nullable(String),

  -- Network types
  inet_col Nullable(IPv4),
  cidr_col Nullable(String),
  macaddr_col Nullable(String),
  macaddr8_col Nullable(String),

  -- Geometric types
  point_col Nullable(String),
  lseg_col Nullable(String),
  box_col Nullable(String),
  path_col Nullable(String),
  polygon_col Nullable(String),
  circle_col Nullable(String),

  -- Full-text search
  tsvector_col Nullable(String),
  tsquery_col Nullable(String),

  -- Arrays
  int_array_col Array(Nullable(Int32)),
  text_array_col Array(Nullable(String)),
  uuid_array_col Array(Nullable(UUID)),

  -- Range types
  int4range_col Nullable(String),
  int8range_col Nullable(String),
  numrange_col Nullable(String),
  tsrange_col Nullable(String),
  tstzrange_col Nullable(String),
  daterange_col Nullable(String),

  -- Key/value
  hstore_col Map(String, String),

  -- Enum and domain
  enum_mood_col Nullable(Enum8('happy' = 1, 'ok' = 2, 'sad' = 3)),
  email_dom_col Nullable(String),

  -- System types
  name_col Nullable(String),
  oid_col Nullable(UInt32),
  pg_lsn_col Nullable(String),
  txid_snapshot_col Nullable(String),

  -- JSONPath and multiranges
  jsonpath_col Nullable(String),
  int4multirange_col Nullable(String),
  nummultirange_col Nullable(String),
  tsmultirange_col Nullable(String),
  tstzmultirange_col Nullable(String),
  datemultirange_col Nullable(String),

  -- Duplicates to reach 100 columns
  smallint_col2 Nullable(Int16),
  integer_col2 Nullable(Int32),
  bigint_col2 Nullable(Int64),
  numeric_col2 Nullable(Decimal(18, 2)),
  real_col2 Nullable(Float32),
  double_col2 Nullable(Float64),
  varchar_col2 Nullable(String),
  text_col2 Nullable(String),
  citext_col2 Nullable(String),
  char_col2 Nullable(FixedString(5)),
  bool_col2 Nullable(Bool),
  date_col2 Nullable(Date),
  timestamp_col2 Nullable(DateTime64(6)),
  timestamptz_col2 Nullable(DateTime64(6, 'UTC')),
  interval_col2 Nullable(String),
  inet_col2 Nullable(IPv4),
  macaddr_col2 Nullable(String),
  point_col2 Nullable(String),
  polygon_col2 Nullable(String),
  int_array_col2 Array(Nullable(Int32)),
  text_array_col2 Array(Nullable(String)),
  int4range_col2 Nullable(String),
  daterange_col2 Nullable(String),
  jsonb_col2 Nullable(String),
  json_col2 Nullable(String),
  bytea_col2 Nullable(String),
  bit_col2 Nullable(String),
  varbit_col2 Nullable(String),
  tsvector_col2 Nullable(String),
  tsquery_col2 Nullable(String),
  xml_col2 Nullable(String),
  enum_mood_col2 Nullable(Enum8('happy' = 1, 'ok' = 2, 'sad' = 3)),
  email_dom_col2 Nullable(String),
  bigint_array_col Array(Nullable(Int64)),
  numrange_col2 Nullable(String),
  jsonpath_col2 Nullable(String),
  pg_lsn_col2 Nullable(String),
  hstore_col2 Map(String, String),
  uuid_col2 Nullable(UUID),
  name_col2 Nullable(String)
) ENGINE = MergeTree()
ORDER BY id;

-- Kafka consumer table for CDC messages
CREATE TABLE wide_table_100cols_kafka (_raw String) 
ENGINE = Kafka() 
SETTINGS 
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'mydb-replication.public.wide_table_100cols',
    kafka_group_name = 'clickhouse_cdc',
    kafka_format = 'LineAsString';

-- Materialized view for real-time data transformation
CREATE MATERIALIZED VIEW wide_table_100cols_mv TO wide_table_100cols AS
SELECT 
    JSONExtractInt(_raw, 'after', 'id') as id,
    now() as created_at,
    JSONExtractInt(_raw, 'after', 'smallint_col') as smallint_col,
    JSONExtractInt(_raw, 'after', 'integer_col') as integer_col,
    JSONExtractInt(_raw, 'after', 'bigint_col') as bigint_col,
    toDecimal128OrNull('0', 20) as numeric_col,
    toDecimal64OrNull('0', 4) as decimal_col,
    JSONExtractFloat(_raw, 'after', 'real_col') as real_col,
    JSONExtractFloat(_raw, 'after', 'double_col') as double_col,
    JSONExtractString(_raw, 'after', 'money_col') as money_col,
    JSONExtractString(_raw, 'after', 'char_col') as char_col,
    JSONExtractString(_raw, 'after', 'varchar_col') as varchar_col,
    JSONExtractString(_raw, 'after', 'text_col') as text_col,
    JSONExtractString(_raw, 'after', 'citext_col') as citext_col,
    JSONExtractString(_raw, 'after', 'bytea_col') as bytea_col,
    JSONExtractBool(_raw, 'after', 'bool_col') as bool_col,
    today() as date_col,
    JSONExtractString(_raw, 'after', 'time_col') as time_col,
    JSONExtractString(_raw, 'after', 'timetz_col') as timetz_col,
    now() as timestamp_col,
    now() as timestamptz_col,
    JSONExtractString(_raw, 'after', 'interval_col') as interval_col,
    generateUUIDv4() as uuid_col,
    JSONExtractString(_raw, 'after', 'json_col') as json_col,
    JSONExtractString(_raw, 'after', 'jsonb_col') as jsonb_col,
    JSONExtractString(_raw, 'after', 'xml_col') as xml_col,
    JSONExtractString(_raw, 'after', 'bit_col') as bit_col,
    JSONExtractString(_raw, 'after', 'varbit_col') as varbit_col,
    toIPv4('127.0.0.1') as inet_col,
    JSONExtractString(_raw, 'after', 'cidr_col') as cidr_col,
    JSONExtractString(_raw, 'after', 'macaddr_col') as macaddr_col,
    JSONExtractString(_raw, 'after', 'macaddr8_col') as macaddr8_col,
    JSONExtractString(_raw, 'after', 'point_col') as point_col,
    JSONExtractString(_raw, 'after', 'lseg_col') as lseg_col,
    JSONExtractString(_raw, 'after', 'box_col') as box_col,
    JSONExtractString(_raw, 'after', 'path_col') as path_col,
    JSONExtractString(_raw, 'after', 'polygon_col') as polygon_col,
    JSONExtractString(_raw, 'after', 'circle_col') as circle_col,
    JSONExtractString(_raw, 'after', 'tsvector_col') as tsvector_col,
    JSONExtractString(_raw, 'after', 'tsquery_col') as tsquery_col,
    [1,2,3] as int_array_col,
    ['a','b'] as text_array_col,
    [generateUUIDv4()] as uuid_array_col,
    JSONExtractString(_raw, 'after', 'int4range_col') as int4range_col,
    JSONExtractString(_raw, 'after', 'int8range_col') as int8range_col,
    JSONExtractString(_raw, 'after', 'numrange_col') as numrange_col,
    JSONExtractString(_raw, 'after', 'tsrange_col') as tsrange_col,
    JSONExtractString(_raw, 'after', 'tstzrange_col') as tstzrange_col,
    JSONExtractString(_raw, 'after', 'daterange_col') as daterange_col,
    map('key', 'value') as hstore_col,
    'happy' as enum_mood_col,
    JSONExtractString(_raw, 'after', 'email_dom_col') as email_dom_col,
    JSONExtractString(_raw, 'after', 'name_col') as name_col,
    JSONExtractUInt(_raw, 'after', 'oid_col') as oid_col,
    JSONExtractString(_raw, 'after', 'pg_lsn_col') as pg_lsn_col,
    JSONExtractString(_raw, 'after', 'txid_snapshot_col') as txid_snapshot_col,
    JSONExtractString(_raw, 'after', 'jsonpath_col') as jsonpath_col,
    JSONExtractString(_raw, 'after', 'int4multirange_col') as int4multirange_col,
    JSONExtractString(_raw, 'after', 'nummultirange_col') as nummultirange_col,
    JSONExtractString(_raw, 'after', 'tsmultirange_col') as tsmultirange_col,
    JSONExtractString(_raw, 'after', 'tstzmultirange_col') as tstzmultirange_col,
    JSONExtractString(_raw, 'after', 'datemultirange_col') as datemultirange_col,
    JSONExtractInt(_raw, 'after', 'smallint_col2') as smallint_col2,
    JSONExtractInt(_raw, 'after', 'integer_col2') as integer_col2,
    JSONExtractInt(_raw, 'after', 'bigint_col2') as bigint_col2,
    toDecimal64OrNull('0', 2) as numeric_col2,
    JSONExtractFloat(_raw, 'after', 'real_col2') as real_col2,
    JSONExtractFloat(_raw, 'after', 'double_col2') as double_col2,
    JSONExtractString(_raw, 'after', 'varchar_col2') as varchar_col2,
    JSONExtractString(_raw, 'after', 'text_col2') as text_col2,
    JSONExtractString(_raw, 'after', 'citext_col2') as citext_col2,
    JSONExtractString(_raw, 'after', 'char_col2') as char_col2,
    JSONExtractBool(_raw, 'after', 'bool_col2') as bool_col2,
    today() as date_col2,
    now() as timestamp_col2,
    now() as timestamptz_col2,
    JSONExtractString(_raw, 'after', 'interval_col2') as interval_col2,
    toIPv4('127.0.0.1') as inet_col2,
    JSONExtractString(_raw, 'after', 'macaddr_col2') as macaddr_col2,
    JSONExtractString(_raw, 'after', 'point_col2') as point_col2,
    JSONExtractString(_raw, 'after', 'polygon_col2') as polygon_col2,
    [1,2] as int_array_col2,
    ['x','y'] as text_array_col2,
    JSONExtractString(_raw, 'after', 'int4range_col2') as int4range_col2,
    JSONExtractString(_raw, 'after', 'daterange_col2') as daterange_col2,
    JSONExtractString(_raw, 'after', 'jsonb_col2') as jsonb_col2,
    JSONExtractString(_raw, 'after', 'json_col2') as json_col2,
    JSONExtractString(_raw, 'after', 'bytea_col2') as bytea_col2,
    JSONExtractString(_raw, 'after', 'bit_col2') as bit_col2,
    JSONExtractString(_raw, 'after', 'varbit_col2') as varbit_col2,
    JSONExtractString(_raw, 'after', 'tsvector_col2') as tsvector_col2,
    JSONExtractString(_raw, 'after', 'tsquery_col2') as tsquery_col2,
    JSONExtractString(_raw, 'after', 'xml_col2') as xml_col2,
    'happy' as enum_mood_col2,
    JSONExtractString(_raw, 'after', 'email_dom_col2') as email_dom_col2,
    [CAST(1, 'Int64')] as bigint_array_col,
    JSONExtractString(_raw, 'after', 'numrange_col2') as numrange_col2,
    JSONExtractString(_raw, 'after', 'jsonpath_col2') as jsonpath_col2,
    JSONExtractString(_raw, 'after', 'pg_lsn_col2') as pg_lsn_col2,
    map('key2', 'value2') as hstore_col2,
    generateUUIDv4() as uuid_col2,
    JSONExtractString(_raw, 'after', 'name_col2') as name_col2
FROM wide_table_100cols_kafka;
