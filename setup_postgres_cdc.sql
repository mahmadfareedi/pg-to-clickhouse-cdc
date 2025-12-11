-- PostgreSQL CDC Setup
-- Run this on your PostgreSQL database to enable logical replication

-- 1. Create publication for the tables
CREATE PUBLICATION dbz_payments_publication FOR TABLE payments, merchant_application, user_merchant;

-- 2. Create replication slot (will be used by Debezium)
-- SELECT pg_create_logical_replication_slot('debezium_payments', 'pgoutput');

-- 3. Grant necessary permissions to the user
GRANT SELECT ON payments, merchant_application, user_merchant TO postgres_admin;
GRANT USAGE ON SCHEMA public TO postgres_admin;

-- 4. Verify publication
SELECT * FROM pg_publication WHERE pubname = 'dbz_payments_publication';

-- 5. Check replication slots
SELECT slot_name, plugin, slot_type, database, active FROM pg_replication_slots;
