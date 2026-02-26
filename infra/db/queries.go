package db

import (
	"context"
	"fmt"
	"strings"
)

func (db *DB) UpsertSubmissions(ctx context.Context, models []SubmissionStatModel) error {
	if len(models) == 0 {
		return nil
	}

	args := make([]any, 0, len(models)*3)
	placeholders := make([]string, 0, len(models))

	for i, m := range models {
		base := i * 3
		placeholders = append(placeholders, fmt.Sprintf(
			"($%d, date_trunc('minute', $%d::timestamp), $%d)", base+1, base+2, base+3,
		))

		args = append(args, m.Username, m.Timestamp, m.SubmissionCount)
	}

	query := fmt.Sprintf(`
        INSERT INTO submissions (username, timestamp, submission_count)
        VALUES %s
        ON CONFLICT (username, timestamp)
        DO UPDATE SET submission_count = submissions.submission_count + EXCLUDED.submission_count
    `, strings.Join(placeholders, ","))

	_, err := db.pool.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("bulk upsert failed: %w", err)
	}

	return nil
}
