package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupTestDB(t *testing.T) (*sql.DB, testcontainers.Container) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "postgres",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}

	host, err := container.Host(context.Background())
	if err != nil {
		container.Terminate(context.Background())
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(context.Background(), "5432")
	if err != nil {
		container.Terminate(context.Background())
		t.Fatalf("failed to get container port: %v", err)
	}

	dsn := fmt.Sprintf("postgres://postgres:postgres@%s:%s/postgres?sslmode=disable", host, port.Port())

	var db *sql.DB
	var lastErr error
	for i := 0; i < 30; i++ {
		db, err = sql.Open("postgres", dsn)
		if err != nil {
			container.Terminate(context.Background())
			t.Fatalf("failed to open database: %v", err)
		}

		err = db.Ping()
		if err == nil {
			break
		}
		lastErr = err
		db.Close()
		time.Sleep(1 * time.Second)
	}

	if lastErr != nil {
		container.Terminate(context.Background())
		t.Fatalf("failed to ping database after retries: %v", lastErr)
	}

	migrationSQL, err := os.ReadFile("sql/migrations.sql")
	if err != nil {
		db.Close()
		container.Terminate(context.Background())
		t.Fatalf("failed to read migrations file: %v", err)
	}

	_, err = db.Exec(string(migrationSQL))
	if err != nil {
		db.Close()
		container.Terminate(context.Background())
		t.Fatalf("failed to execute migrations: %v", err)
	}

	return db, container
}

func TestBatchUpsertUsers(t *testing.T) {
	db, container := setupTestDB(t)
	defer container.Terminate(context.Background())
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	users := []User{
		{GID: "user1", Email: "user1@example.com", JobID: "job1"},
		{GID: "user2", Email: "user2@example.com", JobID: "job2"},
	}

	if err := BatchUpsertUsers(tx, users); err != nil {
		tx.Rollback()
		t.Fatalf("failed to batch upsert users: %v", err)
	}

	var count int
	if err := tx.QueryRow("SELECT COUNT(*) FROM users").Scan(&count); err != nil {
		tx.Rollback()
		t.Fatalf("failed to count users: %v", err)
	}

	if count != 2 {
		tx.Rollback()
		t.Errorf("expected 2 users, got %d", count)
	}

	tx.Commit()
}

func TestBatchUpsertProjects(t *testing.T) {
	db, container := setupTestDB(t)
	defer container.Terminate(context.Background())
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	projects := []Project{
		{GID: "proj1", Project: "Project One", JobID: "job1"},
		{GID: "proj2", Project: "Project Two", JobID: "job2"},
	}

	if err := BatchUpsertProjects(tx, projects); err != nil {
		tx.Rollback()
		t.Fatalf("failed to batch upsert projects: %v", err)
	}

	var count int
	if err := tx.QueryRow("SELECT COUNT(*) FROM projects").Scan(&count); err != nil {
		tx.Rollback()
		t.Fatalf("failed to count projects: %v", err)
	}

	if count != 2 {
		tx.Rollback()
		t.Errorf("expected 2 projects, got %d", count)
	}

	tx.Commit()
}

func TestDeleteUsersWithDifferentJobID(t *testing.T) {
	db, container := setupTestDB(t)
	defer container.Terminate(context.Background())
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Insert users with different job IDs
	users := []User{
		{GID: "user1", Email: "user1@example.com", JobID: "job1"},
		{GID: "user2", Email: "user2@example.com", JobID: "job2"},
		{GID: "user3", Email: "user3@example.com", JobID: "job1"},
		{GID: "user4", Email: "user4@example.com", JobID: "job3"},
	}

	if err := BatchUpsertUsers(tx, users); err != nil {
		tx.Rollback()
		t.Fatalf("failed to batch upsert users: %v", err)
	}

	// Verify initial insert
	var initialCount int
	if err := tx.QueryRow("SELECT COUNT(*) FROM users").Scan(&initialCount); err != nil {
		tx.Rollback()
		t.Fatalf("failed to count users: %v", err)
	}

	if initialCount != 4 {
		tx.Rollback()
		t.Errorf("expected 4 users initially, got %d", initialCount)
	}

	// Delete users with jobid != job1
	if err := DeleteUsersWithDifferentJobID(tx, "job1"); err != nil {
		tx.Rollback()
		t.Fatalf("failed to delete users: %v", err)
	}

	// Verify deletion - only job1 users should remain
	var remainingCount int
	if err := tx.QueryRow("SELECT COUNT(*) FROM users WHERE jobid = 'job1'").Scan(&remainingCount); err != nil {
		tx.Rollback()
		t.Fatalf("failed to count users: %v", err)
	}

	if remainingCount != 2 {
		tx.Rollback()
		t.Errorf("expected 2 users with jobid='job1', got %d", remainingCount)
	}

	// Verify other job IDs were deleted
	var deletedCount int
	if err := tx.QueryRow("SELECT COUNT(*) FROM users WHERE jobid != 'job1'").Scan(&deletedCount); err != nil {
		tx.Rollback()
		t.Fatalf("failed to count deleted users: %v", err)
	}

	if deletedCount != 0 {
		tx.Rollback()
		t.Errorf("expected 0 users with different jobid, got %d", deletedCount)
	}

	tx.Commit()
}

func TestDeleteProjectsWithDifferentJobID(t *testing.T) {
	db, container := setupTestDB(t)
	defer container.Terminate(context.Background())
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Insert projects with different job IDs
	projects := []Project{
		{GID: "proj1", Project: "Project One", JobID: "job2"},
		{GID: "proj2", Project: "Project Two", JobID: "job3"},
		{GID: "proj3", Project: "Project Three", JobID: "job2"},
		{GID: "proj4", Project: "Project Four", JobID: "job4"},
	}

	if err := BatchUpsertProjects(tx, projects); err != nil {
		tx.Rollback()
		t.Fatalf("failed to batch upsert projects: %v", err)
	}

	// Verify initial insert
	var initialCount int
	if err := tx.QueryRow("SELECT COUNT(*) FROM projects").Scan(&initialCount); err != nil {
		tx.Rollback()
		t.Fatalf("failed to count projects: %v", err)
	}

	if initialCount != 4 {
		tx.Rollback()
		t.Errorf("expected 4 projects initially, got %d", initialCount)
	}

	// Delete projects with jobid != job2
	if err := DeleteProjectsWithDifferentJobID(tx, "job2"); err != nil {
		tx.Rollback()
		t.Fatalf("failed to delete projects: %v", err)
	}

	// Verify deletion - only job2 projects should remain
	var remainingCount int
	if err := tx.QueryRow("SELECT COUNT(*) FROM projects WHERE jobid = 'job2'").Scan(&remainingCount); err != nil {
		tx.Rollback()
		t.Fatalf("failed to count projects: %v", err)
	}

	if remainingCount != 2 {
		tx.Rollback()
		t.Errorf("expected 2 projects with jobid='job2', got %d", remainingCount)
	}

	// Verify other job IDs were deleted
	var deletedCount int
	if err := tx.QueryRow("SELECT COUNT(*) FROM projects WHERE jobid != 'job2'").Scan(&deletedCount); err != nil {
		tx.Rollback()
		t.Fatalf("failed to count deleted projects: %v", err)
	}

	if deletedCount != 0 {
		tx.Rollback()
		t.Errorf("expected 0 projects with different jobid, got %d", deletedCount)
	}

	tx.Commit()
}
