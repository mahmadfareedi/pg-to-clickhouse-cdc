CREATE TABLE wide_table_100cols (
  id Int64,
  created_at DateTime64(6, 'UTC'),

  -- Numeric types
  smallint_col Nullable(Int16),
  integer_col Nullable(Int32),
  bigint_col Nullable(Int64),
  numeric_col Nullable(Decimal128(20)),
  decimal_col Nullable(Decimal64(4)),
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
  numeric_col2 Nullable(Decimal64(2)),
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
ORDER BY id
