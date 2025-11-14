-- Create simple tables directly
CREATE TABLE users (
    id Int32,
    name String,
    email String,
    created_at DateTime
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE products (
    id Int32,
    name String,
    price Decimal(10,2),
    category String,
    created_at DateTime
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE orders (
    id Int32,
    user_id Int32,
    product_id Int32,
    quantity Int32,
    total_amount Decimal(10,2),
    created_at DateTime
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE customers (
    id Int32,
    name String,
    email String,
    phone String,
    address String,
    created_at DateTime
) ENGINE = MergeTree() ORDER BY id;
