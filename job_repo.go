package main

import (
	"database/sql"
	"fmt"
)

// AcquireLockWithTx acquires a PostgreSQL advisory lock within an existing transaction
func AcquireLockWithTx(tx *sql.Tx, lockID int) error {
	// Try to acquire lock on this connection
	var locked bool
	err := tx.QueryRow("SELECT pg_try_advisory_lock($1)", lockID).Scan(&locked)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	if !locked {
		return fmt.Errorf("lock is held by another instance")
	}

	return nil
}

// BatchUpsertUsers inserts or updates multiple users in the database
func BatchUpsertUsers(tx *sql.Tx, users []User) error {
	if len(users) == 0 {
		return nil
	}

	// Prepare the upsert query
	query := `
		INSERT INTO users (gid, email, jobid) VALUES ($1, $2, $3)
		ON CONFLICT (gid) DO UPDATE SET
			email = EXCLUDED.email,
			jobid = EXCLUDED.jobid
	`

	stmt, err := tx.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, user := range users {
		_, err := stmt.Exec(user.GID, user.Email, user.JobID)
		if err != nil {
			return fmt.Errorf("failed to upsert user %s: %w", user.GID, err)
		}
	}

	return nil
}

// BatchUpsertProjects inserts or updates multiple projects in the database
func BatchUpsertProjects(tx *sql.Tx, projects []Project) error {
	if len(projects) == 0 {
		return nil
	}

	// Prepare the upsert query
	query := `
		INSERT INTO projects (gid, project, jobid) VALUES ($1, $2, $3)
		ON CONFLICT (gid) DO UPDATE SET
			project = EXCLUDED.project,
			jobid = EXCLUDED.jobid
	`

	stmt, err := tx.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, project := range projects {
		_, err := stmt.Exec(project.GID, project.Project, project.JobID)
		if err != nil {
			return fmt.Errorf("failed to upsert project %s: %w", project.GID, err)
		}
	}

	return nil
}

// DeleteUsersWithDifferentJobID deletes all users that have a jobid different from the provided jobID
func DeleteUsersWithDifferentJobID(tx *sql.Tx, jobID string) error {
	query := "DELETE FROM users WHERE jobid != $1"

	result, err := tx.Exec(query, jobID)
	if err != nil {
		return fmt.Errorf("failed to delete users with different jobid: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	fmt.Printf("Deleted %d users with different jobid\n", rowsAffected)
	return nil
}

// DeleteProjectsWithDifferentJobID deletes all projects that have a jobid different from the provided jobID
func DeleteProjectsWithDifferentJobID(tx *sql.Tx, jobID string) error {
	query := "DELETE FROM projects WHERE jobid != $1"

	result, err := tx.Exec(query, jobID)
	if err != nil {
		return fmt.Errorf("failed to delete projects with different jobid: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	fmt.Printf("Deleted %d projects with different jobid\n", rowsAffected)
	return nil
}
