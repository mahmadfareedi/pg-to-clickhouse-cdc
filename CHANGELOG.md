# Changelog

## [2.0.0] - 2025-11-12

### 🚀 Major Features Added
- **Native ClickHouse Kafka Integration**: Replaced JDBC sink with ClickHouse native Kafka engine
- **Exact Schema Matching**: Support for 100+ column tables with complex PostgreSQL data types
- **Real-time CDC Pipeline**: Sub-second latency from PostgreSQL to ClickHouse
- **Full Initial Load**: Complete snapshot + ongoing CDC support

### ✨ New Components
- **ClickHouse Kafka Engine**: Native message consumption without JDBC limitations
- **Materialized Views**: Real-time JSON transformation and data loading
- **Table Onboarding Script**: Automated new table setup (`scripts/onboard_table.sh`)
- **Monitoring Queries**: Comprehensive CDC health monitoring (`scripts/monitoring.sql`)

### 🔧 Technical Improvements
- **Complex Type Support**: Arrays, JSON, UUID, Enums, Maps, Geometric types
- **Schema Evolution**: Automatic handling of PostgreSQL schema changes  
- **Error Handling**: Robust JSON parsing with fallback values
- **Performance**: Optimized for high-throughput CDC workloads

### 📊 Data Type Mappings
- `BIGSERIAL` → `Int64`
- `TIMESTAMPTZ` → `DateTime64(6, 'UTC')`
- `JSON/JSONB` → `String` (with JSON functions)
- `UUID` → `UUID`
- `Arrays` → `Array(Type)`
- `HSTORE` → `Map(String, String)`
- `ENUM` → `Enum8`
- `INET` → `IPv4`

### 🛠 Configuration Updates
- Updated connector configurations for schema-less JSON
- New ClickHouse table creation scripts
- Enhanced materialized view templates
- Improved error tolerance settings

### 📁 New File Structure
```
cdc/
├── scripts/
│   ├── onboard_table.sh          # New table automation
│   ├── monitoring.sql            # CDC health monitoring  
│   └── setup_clickhouse_table.sql # Complete table setup
├── clickhouse/
│   ├── exact_clickhouse_table.sql # Exact schema match
│   └── materialized_view.sql      # Real-time transformation
└── connectors/
    └── postgres-source.json       # Updated source config
```

### 🐛 Bug Fixes
- Fixed JDBC autocommit issues with ClickHouse
- Resolved JSON schema validation errors
- Improved connector stability and error handling
- Fixed data type conversion edge cases

### 📈 Performance Improvements
- Native Kafka consumption (no JDBC overhead)
- Optimized JSON parsing in materialized views
- Reduced CDC latency to sub-second
- Better memory usage with streaming processing

### 🔍 Monitoring & Observability
- Real-time CDC lag monitoring
- Kafka consumption metrics
- ClickHouse table statistics
- Error tracking and alerting queries

### 🚦 Breaking Changes
- Replaced JDBC sink connector with native ClickHouse Kafka engine
- Updated connector configuration format
- New materialized view approach for data transformation
- Changed ClickHouse table creation process

### 📋 Migration Guide
1. Stop existing JDBC sink connectors
2. Create new ClickHouse tables using provided scripts
3. Set up Kafka engine tables and materialized views
4. Update source connector configuration
5. Verify data flow and monitoring

### 🎯 Production Ready Features
- High availability support
- Schema evolution handling
- Multi-table CDC support
- Comprehensive monitoring
- Automated table onboarding

---

## [1.0.0] - Previous Version
- Basic JDBC-based CDC pipeline
- Single table support
- Manual configuration required
