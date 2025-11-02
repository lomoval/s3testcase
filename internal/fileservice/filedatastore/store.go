// MIT License
//
// Copyright (c) 2025 Aleksandr A. Lomov
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software
// and associated documentation files (the “Software”), to deal in the Software without
// restriction, including without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or
// substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.

package filedatastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type File struct {
	ID          int64
	UUID        uuid.UUID
	FileName    string
	Size        int64
	ContentType string
	ProcessedAt *time.Time
	Locations   []FileLocation
}

//nolint:tagliatelle
type FileLocation struct {
	ID           int64      `json:"id"`
	FileUUID     uuid.UUID  `json:"file_uuid"`
	LocationUUID uuid.UUID  `json:"location_uuid"`
	Size         int64      `json:"size"`
	PartNumber   int        `json:"part_number"`
	ProcessedAt  *time.Time `json:"processed_at"`
}

type Store struct {
	db *sql.DB
}

type CleanItem struct {
	ID              int64
	FileUUID        uuid.UUID
	FileProcessedAt *time.Time
	CleanAfter      time.Time
	OlderVersion    bool
}

func (i CleanItem) IsActive() bool {
	return !i.OlderVersion && i.FileProcessedAt != nil
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

func (s *Store) InsertFileInfo(
	ctx context.Context,
	uuid uuid.UUID,
	fileName, contentType string,
	size int64,
) (*File, error) {
	query := `
		INSERT INTO files (uuid, file_name, content_type, size)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`
	var id int64
	if err := s.db.QueryRowContext(ctx, query, uuid, fileName, contentType, size).Scan(&id); err != nil {
		return nil, err
	}

	return &File{
		ID:          id,
		UUID:        uuid,
		FileName:    fileName,
		ContentType: contentType,
	}, nil
}

//// Files

func (s *Store) GetLastFileWithLocationsByName(ctx context.Context, name string) (*File, error) {
	query := `
        SELECT 
            f.uuid,
            f.id,
            f.file_name,
            f.content_type,
            f.processed_at,
            f.size,
            COALESCE(
                json_agg(
                    json_build_object(
                        'id', fl.id,
                        'file_uuid', fl.file_uuid,
                        'location_uuid', fl.location_uuid,
                        'size', fl.size,
                        'part_number', fl.part_number,
                        'processed_at', fl.processed_at
                    )
                    ORDER BY fl.part_number ASC
                ) FILTER (WHERE fl.id IS NOT NULL),
                '[]'
            ) AS locations
        FROM files f
        LEFT JOIN file_locations fl ON fl.file_uuid = f.uuid
        WHERE f.file_name = $1 AND f.processed_at IS NOT NULL
        AND (fl.id IS NULL OR fl.processed_at IS NOT NULL)
        GROUP BY f.uuid, f.id, f.file_name, f.content_type, f.created_at, f.updated_at, f.processed_at, f.size
		ORDER BY f.processed_at DESC, f.id DESC 
		LIMIT 1;
    `

	var f File
	var locJSON []byte
	err := s.db.QueryRowContext(ctx, query, name).Scan(
		&f.UUID, &f.ID, &f.FileName, &f.ContentType,
		&f.ProcessedAt, &f.Size,
		&locJSON,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	if err := json.Unmarshal(locJSON, &f.Locations); err != nil {
		return nil, err
	}
	return &f, nil
}

func (s *Store) GetFileByUUID(ctx context.Context, id uuid.UUID) (*File, error) {
	query := `
		SELECT id, uuid, file_name, content_type, size, processed_at
		FROM files
		WHERE uuid = $1
	`
	var f File
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&f.ID,
		&f.UUID,
		&f.FileName,
		&f.ContentType,
		&f.Size,
		&f.ProcessedAt,
	)
	if err != nil {
		return nil, err
	}
	return &f, nil
}

func (s *Store) SetFileProcessedTime(ctx context.Context, uuid uuid.UUID, processed time.Time) error {
	const query = `
		UPDATE files
		SET processed_at = $2
		WHERE uuid = $1
	`
	_, err := s.db.ExecContext(ctx, query, uuid, processed)
	return err
}

func (s *Store) SetFileSize(ctx context.Context, uuid uuid.UUID, size int64) error {
	const query = `
		UPDATE files
		SET size = $2
		WHERE uuid = $1
	`
	_, err := s.db.ExecContext(ctx, query, uuid, size)
	return err
}

/// FileLocations

func (s *Store) AddLocation(
	ctx context.Context,
	fileUUID uuid.UUID,
	locationUUID uuid.UUID,
	partNumber int,
	size int64,
) (int64, error) {
	query := `
		INSERT INTO file_locations (file_uuid, location_uuid, part_number, size)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`
	var id int64
	if err := s.db.QueryRowContext(ctx, query, fileUUID, locationUUID, partNumber, size).Scan(&id); err != nil {
		return -1, err
	}
	return id, nil
}

func (s *Store) DeleteFileLocationByID(ctx context.Context, id int64) error {
	const query = `
		DELETE FROM file_locations
		WHERE id = $1;
		`

	_, err := s.db.ExecContext(ctx, query, id)
	return err
}

func (s *Store) GetFileLocations(
	ctx context.Context,
	fileUUID uuid.UUID,
	unprocessedOnly bool,
) ([]FileLocation, error) {
	const query = `
		SELECT id, file_uuid, location_uuid, size, part_number, processed_at
		FROM file_locations
		WHERE file_uuid = $1 AND ($2 = false OR processed_at IS NULL)
		ORDER BY part_number ASC;
	`

	rows, err := s.db.QueryContext(ctx, query, fileUUID, unprocessedOnly)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locations []FileLocation
	for rows.Next() {
		var loc FileLocation
		if err := rows.Scan(
			&loc.ID,
			&loc.FileUUID,
			&loc.LocationUUID,
			&loc.Size,
			&loc.PartNumber,
			&loc.ProcessedAt,
		); err != nil {
			return nil, err
		}
		locations = append(locations, loc)
	}

	return locations, rows.Err()
}

func (s *Store) DeleteLocation(
	ctx context.Context,
	id int64,
) error {
	query := `
		DELETE FROM file_locations 
		WHERE id = $1 
	`
	_, err := s.db.ExecContext(ctx, query, id)
	return err
}

func (s *Store) SetLocationProcessedTime(
	ctx context.Context,
	id int64,
	time time.Time,
) error {
	query := `
		UPDATE file_locations
		SET processed_at = $2
		WHERE id = $1
	`
	_, err := s.db.ExecContext(ctx, query, id, time)
	return err
}

//// Clean items

func (s *Store) InsertCleanItemData(ctx context.Context, fileUUID uuid.UUID, cleanAfter time.Time) error {
	query := `
		INSERT INTO clean_outbox (file_uuid, clean_after)
		VALUES ($1, $2)
	`
	if _, err := s.db.ExecContext(ctx, query, fileUUID, cleanAfter); err != nil {
		return err
	}
	return nil
}

func (s *Store) SetCleanItemCleanAfterToNow(ctx context.Context, uuid uuid.UUID) error {
	const query = `
		UPDATE clean_outbox
		SET clean_after = NOW()
		WHERE file_uuid = $1
	`
	_, err := s.db.ExecContext(ctx, query, uuid)
	return err
}

func (s *Store) GetCleaningItemsBatch(ctx context.Context, limit int) ([]CleanItem, error) {
	//nolint:gosec
	query := `
		SELECT
			co.id,
			co.file_uuid,
			co.clean_after,
			(f.processed_at  IS NULL 
			    OR (f.processed_at < latest.processed_at)
			 	OR (f.processed_at = latest.processed_at AND f.id < latest.id)
			) AS has_newer_version,
			f.processed_at
		FROM clean_outbox co
		JOIN files f ON f.uuid = co.file_uuid
		JOIN (
			SELECT DISTINCT ON (file_name)
				file_name,
				processed_at,
				id
			FROM files
			WHERE processed_at IS NOT NULL
			ORDER BY file_name, processed_at DESC, id DESC
		) AS latest ON latest.file_name = f.file_name
		WHERE co.clean_after <= NOW()
		LIMIT 
	` + strconv.Itoa(limit)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []CleanItem
	for rows.Next() {
		var r CleanItem
		if err := rows.Scan(&r.ID, &r.FileUUID, &r.CleanAfter, &r.OlderVersion, &r.FileProcessedAt); err != nil {
			return nil, err
		}
		results = append(results, r)
	}

	return results, nil
}

func (s *Store) AddOldFilesToCleanItems(ctx context.Context, fileUUID uuid.UUID) error {
	const query = `
	INSERT INTO clean_outbox (file_uuid, clean_after)
	SELECT f2.uuid, NOW()
	FROM files target
	JOIN files f2 
	  ON f2.file_name = target.file_name
	  AND (f2.processed_at, f2.id) < (target.processed_at, target.id)
	WHERE target.uuid = $1
	  AND NOT EXISTS (
	        SELECT 1 
	        FROM clean_outbox c
	        WHERE c.file_uuid = f2.uuid
	  );
	`
	_, err := s.db.ExecContext(ctx, query, fileUUID)
	return err
}

func (s *Store) DeleteCleanItem(ctx context.Context, fileUUID uuid.UUID) error {
	const query = `
		DELETE FROM clean_outbox
		WHERE file_uuid = $1;
	`

	_, err := s.db.ExecContext(ctx, query, fileUUID)
	return err
}

func (s *Store) DeleteFileWithCleanItem(ctx context.Context, fileUUID uuid.UUID) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	// Удаляем из clean_outbox
	_, err = tx.ExecContext(ctx, "DELETE FROM clean_outbox WHERE file_uuid = $1", fileUUID)
	if err != nil {
		return err
	}

	// Удаляем из files
	_, err = tx.ExecContext(ctx, "DELETE FROM files WHERE uuid = $1", fileUUID)
	if err != nil {
		return err
	}

	return nil
}
