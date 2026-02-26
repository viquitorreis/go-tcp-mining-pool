package db

import "time"

type SubmissionStatModel struct {
	Username        string
	SubmissionCount int
	Timestamp       time.Time
}
