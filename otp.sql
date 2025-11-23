-- Create Keyspace
CREATE KEYSPACE IF NOT EXISTS messaging_app
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

-- Create User Table
USE messaging_app;

CREATE TABLE IF NOT EXISTS users (
    email text PRIMARY KEY,
    otp text,
    is_verified boolean,
    created_at timestamp
);