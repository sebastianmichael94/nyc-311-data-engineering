
CREATE TABLE IF NOT EXISTS nyc_311_requests (
    unique_key BIGINT PRIMARY KEY,
    created_date TIMESTAMP NOT NULL,
    closed_date TIMESTAMP,
    agency TEXT,
    agency_name TEXT,
    complaint_type TEXT NOT NULL,
    descriptor TEXT,
    borough TEXT,
    incident_zip TEXT,
    status TEXT
);
