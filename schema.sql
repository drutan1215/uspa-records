-- Run this once in the Supabase SQL Editor before uploading data.

CREATE TABLE IF NOT EXISTS uspa_records (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    division    text,
    weight_class text,
    lift        text,
    name        text,
    kilos       numeric,
    pounds      numeric,
    date        text,
    location    text,
    event       text,
    status      text,
    has_record  boolean DEFAULT false
);

-- Indexes for the most common website query patterns
CREATE INDEX IF NOT EXISTS idx_uspa_location     ON uspa_records (location);
CREATE INDEX IF NOT EXISTS idx_uspa_event        ON uspa_records (event);
CREATE INDEX IF NOT EXISTS idx_uspa_status       ON uspa_records (status);
CREATE INDEX IF NOT EXISTS idx_uspa_division     ON uspa_records (division);
CREATE INDEX IF NOT EXISTS idx_uspa_weight_class ON uspa_records (weight_class);
CREATE INDEX IF NOT EXISTS idx_uspa_has_record   ON uspa_records (has_record);
