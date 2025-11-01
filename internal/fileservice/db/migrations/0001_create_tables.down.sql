-- +migrate Down

DROP TABLE IF EXISTS clean_outbox;
DROP TABLE IF EXISTS file_locations;
DROP TABLE IF EXISTS files;
DROP FUNCTION IF EXISTS set_timestamps();

