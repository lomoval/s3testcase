-- +migrate Up

CREATE OR REPLACE FUNCTION set_timestamps()
    RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.created_at := NOW();
        NEW.updated_at := NOW();
    ELSIF TG_OP = 'UPDATE' THEN
        NEW.updated_at := NOW();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Tables

CREATE TABLE IF NOT EXISTS files (
     id BIGSERIAL UNIQUE,
     uuid UUID PRIMARY KEY NOT NULL,
     created_at TIMESTAMP NOT NULL,
     updated_at TIMESTAMP NOT NULL,
     file_name TEXT NOT NULL,
     content_type TEXT NOT NULL,
     size BIGINT NOT NULL,
     processed_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS file_locations (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    file_uuid UUID NOT NULL REFERENCES files(uuid) ON DELETE CASCADE,
    location_uuid UUID NOT NULL,
    size BIGINT NOT NULL,
    part_number INT NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS clean_outbox (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    file_uuid UUID NOT NULL,
    clean_after TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Triggers

CREATE TRIGGER trg_files_timestamps
    BEFORE INSERT OR UPDATE ON files
    FOR EACH ROW
EXECUTE FUNCTION set_timestamps();

CREATE TRIGGER trg_file_locations_timestamps
    BEFORE INSERT OR UPDATE ON file_locations
    FOR EACH ROW
EXECUTE FUNCTION set_timestamps();

CREATE TRIGGER trg_clean_outbox_timestamps
    BEFORE INSERT OR UPDATE ON clean_outbox
    FOR EACH ROW
EXECUTE FUNCTION set_timestamps();

-- Indexes

CREATE INDEX IF NOT EXISTS idx_files_file_name ON files(file_name);
CREATE INDEX IF NOT EXISTS idx_file_locations_file_uuid ON file_locations(file_uuid);
CREATE INDEX IF NOT EXISTS idx_clean_outbox_clean_after ON clean_outbox(clean_after);
