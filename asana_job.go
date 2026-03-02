package main

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
)

const jobLockName = "asana_job_lock"

// generateLockID creates a consistent lock ID from a string
func generateLockID(lockName string) int {
	h := fnv.New32a()
	h.Write([]byte(lockName))
	return int(h.Sum32())
}

// GetDummyUsers returns 10 dummy users with random IDs from 1-20
func GetDummyUsers(jobID string) []User {
	users := make([]User, 10)
	usedIDs := make(map[int]bool)

	for i := 0; i < 10; i++ {
		var randomID int
		// Generate unique random ID from 1-20
		for {
			randomID = rand.Intn(20) + 1
			if !usedIDs[randomID] {
				usedIDs[randomID] = true
				break
			}
		}
		users[i] = User{
			GID:   fmt.Sprintf("user_%d", randomID),
			Email: fmt.Sprintf("user%d@asana.com", randomID),
			JobID: jobID,
		}
	}
	return users
}

// GetDummyProjects returns 10 dummy projects with random IDs from 1-20
func GetDummyProjects(jobID string) []Project {
	projects := make([]Project, 10)
	usedIDs := make(map[int]bool)

	for i := 0; i < 10; i++ {
		var randomID int
		// Generate unique random ID from 1-20
		for {
			randomID = rand.Intn(20) + 1
			if !usedIDs[randomID] {
				usedIDs[randomID] = true
				break
			}
		}
		projects[i] = Project{
			GID:     fmt.Sprintf("proj_%d", randomID),
			Project: fmt.Sprintf("Asana Project %d", randomID),
			JobID:   jobID,
		}
	}
	return projects
}

// StartAsanaJob starts a background job that runs at intervals specified by config
func StartAsanaJob(db *sql.DB, cfg Config) {
	ticker := time.NewTicker(time.Duration(cfg.JobTimeout) * time.Second)

	go func() {
		defer ticker.Stop()
		log.Printf("[%s] Asana job scheduler started - interval: %d seconds\n", time.Now().Format("2006-01-02 15:04:05"), cfg.JobTimeout)

		// Run immediately on startup (in separate goroutine to not block ticker loop)
		go runAsanaJobWithClient(db, cfg, NewAsanaClient(cfg.AsanaToken))

		// Run on every tick
		for range ticker.C {
			go runAsanaJobWithClient(db, cfg, NewAsanaClient(cfg.AsanaToken))
		}
	}()
}

// fetchUsers retrieves all users from a workspace with pagination
func fetchUsers(client AsanaClientInterface, workspaceID, jobID string) ([]User, error) {
	log.Printf("[%s] Fetching users from workspace %s", time.Now().Format("2006-01-02 15:04:05"), workspaceID)
	users := []User{}
	userOffset := ""
	pageCount := 0
	for {
		pageCount++
		userResponse, err := client.GetUsers(workspaceID, "100", userOffset)
		if err != nil {
			return nil, err
		}

		log.Printf("[%s] Fetched page %d: %d users", time.Now().Format("2006-01-02 15:04:05"), pageCount, len(userResponse.Data))

		// Convert AsanaUser to User
		for _, asanaUser := range userResponse.Data {
			users = append(users, User{
				GID:   asanaUser.GID,
				Email: asanaUser.Name,
				JobID: jobID,
			})
		}

		// Check for next page
		if userResponse.NextPage == nil || userResponse.NextPage.Offset == "" {
			break
		}
		userOffset = userResponse.NextPage.Offset
	}
	log.Printf("[%s] Finished fetching users: total %d users from %d pages", time.Now().Format("2006-01-02 15:04:05"), len(users), pageCount)
	return users, nil
}

// fetchProjects retrieves all projects from a workspace with pagination
func fetchProjects(client AsanaClientInterface, workspaceID, jobID string) ([]Project, error) {
	log.Printf("[%s] Fetching projects from workspace %s", time.Now().Format("2006-01-02 15:04:05"), workspaceID)
	projects := []Project{}
	projectOffset := ""
	pageCount := 0
	for {
		pageCount++
		projectResponse, err := client.GetProjects(workspaceID, "100", projectOffset)
		if err != nil {
			return nil, err
		}

		log.Printf("[%s] Fetched page %d: %d projects", time.Now().Format("2006-01-02 15:04:05"), pageCount, len(projectResponse.Data))

		// Convert AsanaProject to Project
		for _, asanaProject := range projectResponse.Data {
			projects = append(projects, Project{
				GID:     asanaProject.GID,
				Project: asanaProject.Name,
				JobID:   jobID,
			})
		}

		// Check for next page
		if projectResponse.NextPage == nil || projectResponse.NextPage.Offset == "" {
			break
		}
		projectOffset = projectResponse.NextPage.Offset
	}
	log.Printf("[%s] Finished fetching projects: total %d projects from %d pages", time.Now().Format("2006-01-02 15:04:05"), len(projects), pageCount)
	return projects, nil
}

// runAsanaJobWithClient executes the asana synchronization job with a provided client (for testing)
func runAsanaJobWithClient(db *sql.DB, cfg Config, client AsanaClientInterface) {
	startTime := time.Now().Format("2006-01-02 15:04:05")
	log.Printf("[%s] Starting asana job\n", startTime)

	// Generate unique job ID for this run
	jobID := uuid.New().String()

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		log.Printf("[%s] Failed to begin transaction: %v\n", startTime, err)
		return
	}
	defer tx.Rollback()

	// Acquire advisory lock to ensure only one instance runs
	lockID := generateLockID(jobLockName)
	if err := AcquireLockWithTx(tx, lockID); err != nil {
		log.Printf("[%s] Failed to acquire lock: %v\n", startTime, err)
		return
	}

	// Get workspaces
	workspaces, err := client.GetWorkspaces()
	if err != nil {
		log.Printf("[%s] Failed to get workspaces: %v\n", startTime, err)
		return
	}

	if len(workspaces) == 0 {
		log.Printf("[%s] No workspaces found\n", startTime)
		return
	}

	workspaceID := workspaces[0].GID
	log.Printf("[%s] Using workspace: %s (%s)\n", startTime, workspaces[0].Name, workspaceID)

	// Fetch users and projects in parallel
	var wg sync.WaitGroup
	var users []User
	var projects []Project
	var usersErr, projectsErr error

	// Fetch users in goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		users, usersErr = fetchUsers(client, workspaceID, jobID)
	}()

	// Fetch projects in goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		projects, projectsErr = fetchProjects(client, workspaceID, jobID)
	}()

	// Wait for both to complete
	wg.Wait()

	// Check for errors
	if usersErr != nil {
		log.Printf("[%s] Failed to get users: %v\n", startTime, usersErr)
		return
	}

	if projectsErr != nil {
		log.Printf("[%s] Failed to get projects: %v\n", startTime, projectsErr)
		return
	}

	// Batch upsert users
	if err := BatchUpsertUsers(tx, users); err != nil {
		log.Printf("[%s] Failed to upsert users: %v\n", startTime, err)
		return
	}

	// Batch upsert projects
	if err := BatchUpsertProjects(tx, projects); err != nil {
		log.Printf("[%s] Failed to upsert projects: %v\n", startTime, err)
		return
	}

	// Delete entities with different job IDs
	if err := DeleteUsersWithDifferentJobID(tx, jobID); err != nil {
		log.Printf("[%s] Failed to delete users: %v\n", startTime, err)
		return
	}

	if err := DeleteProjectsWithDifferentJobID(tx, jobID); err != nil {
		log.Printf("[%s] Failed to delete projects: %v\n", startTime, err)
		return
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		log.Printf("[%s] Failed to commit transaction: %v\n", startTime, err)
		return
	}

	log.Printf("[%s] Asana job completed: synced %d users and %d projects (jobID: %s)\n", startTime, len(users), len(projects), jobID)
}
