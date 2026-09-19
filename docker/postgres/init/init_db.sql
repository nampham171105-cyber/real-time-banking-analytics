CREATE TABLE IF NOT EXISTS customer (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    is_deleted BOOLEAN DEFAULT false,  -- soft-delete: "xoá" = update cờ này, không DELETE vật lý
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS account (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customer(id),
    account_type VARCHAR(50),
    balance NUMERIC(18,2),
    currency VARCHAR(50),
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transaction (
    id BIGSERIAL PRIMARY KEY,
    account_id INT REFERENCES account(id),
    txn_type VARCHAR(30),
    amount NUMERIC(18,2),
    related_account_id INT,
    txn_status VARCHAR(20),
    error_code VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

