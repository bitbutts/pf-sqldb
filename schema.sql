CREATE TABLE pft_transactions (
    id SERIAL PRIMARY KEY,  -- Auto-incrementing unique identifier
    ledger_index BIGINT NOT NULL,  -- Ledger index associated with the transaction
    transaction_hash TEXT UNIQUE NOT NULL,  -- Unique hash representing the transaction
    from_address TEXT,  -- Address from which the transaction originated
    to_address TEXT,  -- Address to which the transaction is sent
    memo TEXT,  -- Optional memo associated with the transaction
    amount NUMERIC(18, 6),  -- Transaction amount with up to 18 digits and 6 decimal places
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),  -- Record creation timestamp
    transaction_timestamp TIMESTAMP WITH TIME ZONE,  -- Timestamp when the transaction occurred

    -- Constraints
    CONSTRAINT pft_transactions_txhash_key UNIQUE (transaction_hash),
    CONSTRAINT pft_transactions_pkey PRIMARY KEY (id)
);

-- Indexes
CREATE UNIQUE INDEX pft_transactions_txhash_key ON pft_transactions (transaction_hash) USING BTREE;
CREATE UNIQUE INDEX pft_transactions_pkey ON pft_transactions (id) USING BTREE;
