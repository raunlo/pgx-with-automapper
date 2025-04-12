-- initial-scripts.sql
CREATE TABLE users (
                       id SERIAL PRIMARY KEY,
                       name VARCHAR(100),
                       email VARCHAR(100) UNIQUE NOT NULL
);

INSERT INTO users (name, email) VALUES ('John Doe', 'john.doe@example.com');