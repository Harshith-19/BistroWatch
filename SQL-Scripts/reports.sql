CREATE TABLE reports (
    report_id SERIAL PRIMARY KEY,
    status VARCHAR(50),
    name VARCHAR(100),
    created_at TIMESTAMP,
    generated_at TIMESTAMP
);
