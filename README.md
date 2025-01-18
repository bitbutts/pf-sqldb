# Database Structure Documentation

This document provides an overview of the SQL database structure, including table definitions, constraints, and indexes.

---

## Table: `pft_transactions`

### Columns
| Column Name             | Data Type                    | Constraints                          | Description                                   |
|-------------------------|------------------------------|--------------------------------------|-----------------------------------------------|
| `id`                   | `SERIAL`                    | Primary Key                          | Auto-incrementing unique identifier.         |
| `ledger_index`         | `BIGINT`                    | NOT NULL                            | Represents the ledger index associated with the transaction. |
| `transaction_hash`     | `TEXT`                      | UNIQUE, NOT NULL                    | Unique hash representing the transaction.    |
| `from_address`         | `TEXT`                      |                                      | Address from which the transaction originated. |
| `to_address`           | `TEXT`                      |                                      | Address to which the transaction is sent.    |
| `memo`                 | `TEXT`                      |                                      | Optional memo associated with the transaction. |
| `amount`               | `NUMERIC(18, 6)`            |                                      | Transaction amount, supporting up to 18 digits with 6 decimal places. |
| `created_at`           | `TIMESTAMP WITH TIME ZONE`  | DEFAULT `now()`                     | Timestamp when the record was created. Defaults to the current time. |
| `transaction_timestamp`| `TIMESTAMP WITH TIME ZONE`  |                                      | Timestamp when the transaction occurred.     |

---

### Constraints
- **Primary Key (`pft_transactions_pkey`)**:
  - Column: `id`
  - Ensures each record has a unique identifier.

- **Unique Constraint (`pft_transactions_txhash_key`)**:
  - Column: `transaction_hash`
  - Ensures each transaction hash is unique.

---

### Indexes
- **Primary Key Index**:
  - Name: `pft_transactions_pkey`
  - Column: `id`
  - Type: `BTREE`
  - Ensures quick lookups by the primary key.

- **Unique Index on `transaction_hash`**:
  - Name: `pft_transactions_txhash_key`
  - Column: `transaction_hash`
  - Type: `BTREE`
  - Optimizes lookups and enforces the uniqueness of transaction hashes.

---

## Notes
1. **Data Integrity**:
   - The `transaction_hash` is critical for ensuring data uniqueness and integrity.
   - The `ledger_index` column requires a value for each record.

2. **Timestamps**:
   - The `created_at` column automatically logs when the record was inserted.
   - The `transaction_timestamp` is manually provided, offering flexibility to record when the transaction actually took place.

3. **Scalability**:
   - The `NUMERIC(18, 6)` type for the `amount` column allows handling high-precision monetary values.
   - Using `BIGINT` for `ledger_index` ensures scalability for a large number of records.


