package main

import (
	"context"
	"testing"
)

// MockAsanaClient is a mock implementation of AsanaClientInterface for testing
type MockAsanaClient struct {
	workspaces []Workspace
	users      []AsanaUser
	projects   []AsanaProject
}

func (mac *MockAsanaClient) GetWorkspaces() ([]Workspace, error) {
	return mac.workspaces, nil
}

func (mac *MockAsanaClient) GetUsers(workspaceID, limit, offset string) (*UsersResponse, error) {
	return &UsersResponse{
		Data:     mac.users,
		NextPage: nil,
	}, nil
}

func (mac *MockAsanaClient) GetProjects(workspaceID, limit, offset string) (*ProjectsResponse, error) {
	return &ProjectsResponse{
		Data:     mac.projects,
		NextPage: nil,
	}, nil
}

func TestRunAsanaJobWithClient_InsertsUsers(t *testing.T) {
	db, container := setupTestDB(t)
	defer container.Terminate(context.Background())
	defer db.Close()

	// Create mock client with test data
	mockClient := &MockAsanaClient{
		workspaces: []Workspace{
			{GID: "workspace_123", Name: "Test Workspace"},
		},
		users: []AsanaUser{
			{GID: "user_1", Name: "John Doe", ResourceType: "user"},
			{GID: "user_2", Name: "Jane Smith", ResourceType: "user"},
		},
		projects: []AsanaProject{
			{GID: "proj_1", Name: "Project Alpha"},
			{GID: "proj_2", Name: "Project Beta"},
		},
	}

	cfg := Config{
		JobTimeout: 20,
		AsanaToken: "test_token",
	}

	// Run the job with mock client
	runAsanaJobWithClient(db, cfg, mockClient)

	// Assert users were inserted
	var userCount int
	err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	if err != nil {
		t.Fatalf("failed to count users: %v", err)
	}
	if userCount != 2 {
		t.Errorf("expected 2 users, got %d", userCount)
	}

	// Assert specific user data
	var email string
	err = db.QueryRow("SELECT email FROM users WHERE gid = $1", "user_1").Scan(&email)
	if err != nil {
		t.Fatalf("failed to query user: %v", err)
	}
	if email != "John Doe" {
		t.Errorf("expected email 'John Doe', got '%s'", email)
	}
}

func TestRunAsanaJobWithClient_InsertsProjects(t *testing.T) {
	db, container := setupTestDB(t)
	defer container.Terminate(context.Background())
	defer db.Close()

	// Create mock client with test data
	mockClient := &MockAsanaClient{
		workspaces: []Workspace{
			{GID: "workspace_123", Name: "Test Workspace"},
		},
		users: []AsanaUser{
			{GID: "user_1", Name: "John Doe", ResourceType: "user"},
		},
		projects: []AsanaProject{
			{GID: "proj_1", Name: "Project Alpha"},
			{GID: "proj_2", Name: "Project Beta"},
			{GID: "proj_3", Name: "Project Gamma"},
		},
	}

	cfg := Config{
		JobTimeout: 20,
		AsanaToken: "test_token",
	}

	// Run the job with mock client
	runAsanaJobWithClient(db, cfg, mockClient)

	// Assert projects were inserted
	var projectCount int
	err := db.QueryRow("SELECT COUNT(*) FROM projects").Scan(&projectCount)
	if err != nil {
		t.Fatalf("failed to count projects: %v", err)
	}
	if projectCount != 3 {
		t.Errorf("expected 3 projects, got %d", projectCount)
	}

	// Assert specific project data
	var projectName string
	err = db.QueryRow("SELECT project FROM projects WHERE gid = $1", "proj_2").Scan(&projectName)
	if err != nil {
		t.Fatalf("failed to query project: %v", err)
	}
	if projectName != "Project Beta" {
		t.Errorf("expected project name 'Project Beta', got '%s'", projectName)
	}
}

func TestRunAsanaJobWithClient_DeletesOldData(t *testing.T) {
	db, container := setupTestDB(t)
	defer container.Terminate(context.Background())
	defer db.Close()

	// First run: insert some data with jobID1
	mockClient1 := &MockAsanaClient{
		workspaces: []Workspace{
			{GID: "workspace_123", Name: "Test Workspace"},
		},
		users: []AsanaUser{
			{GID: "user_1", Name: "John Doe", ResourceType: "user"},
		},
		projects: []AsanaProject{
			{GID: "proj_1", Name: "Project Alpha"},
		},
	}

	cfg := Config{
		JobTimeout: 20,
		AsanaToken: "test_token",
	}

	runAsanaJobWithClient(db, cfg, mockClient1)

	// Verify first run data exists
	var count1 int
	err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count1)
	if err != nil {
		t.Fatalf("failed to count users after first run: %v", err)
	}
	if count1 != 1 {
		t.Errorf("after first run, expected 1 user, got %d", count1)
	}

	// Second run: insert different data - old data should be deleted
	mockClient2 := &MockAsanaClient{
		workspaces: []Workspace{
			{GID: "workspace_123", Name: "Test Workspace"},
		},
		users: []AsanaUser{
			{GID: "user_2", Name: "Jane Smith", ResourceType: "user"},
			{GID: "user_3", Name: "Bob Johnson", ResourceType: "user"},
		},
		projects: []AsanaProject{
			{GID: "proj_2", Name: "Project Beta"},
		},
	}

	runAsanaJobWithClient(db, cfg, mockClient2)

	// Verify old user was deleted and new users exist
	var count2 int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count2)
	if err != nil {
		t.Fatalf("failed to count users after second run: %v", err)
	}
	if count2 != 2 {
		t.Errorf("after second run, expected 2 users (old deleted), got %d", count2)
	}

	// Verify old user_1 does not exist
	var exists bool
	err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM users WHERE gid = $1)", "user_1").Scan(&exists)
	if err != nil {
		t.Fatalf("failed to check if user_1 exists: %v", err)
	}
	if exists {
		t.Error("user_1 should have been deleted but still exists")
	}

	// Verify new users exist with correct data
	var newUserCount int
	err = db.QueryRow("SELECT COUNT(*) FROM users WHERE gid IN ($1, $2)", "user_2", "user_3").Scan(&newUserCount)
	if err != nil {
		t.Fatalf("failed to count new users: %v", err)
	}
	if newUserCount != 2 {
		t.Errorf("expected 2 new users, got %d", newUserCount)
	}

	// Verify specific user data
	var jane_email string
	err = db.QueryRow("SELECT email FROM users WHERE gid = $1", "user_2").Scan(&jane_email)
	if err != nil {
		t.Fatalf("failed to query user_2: %v", err)
	}
	if jane_email != "Jane Smith" {
		t.Errorf("expected user_2 email 'Jane Smith', got '%s'", jane_email)
	}

	var bob_email string
	err = db.QueryRow("SELECT email FROM users WHERE gid = $1", "user_3").Scan(&bob_email)
	if err != nil {
		t.Fatalf("failed to query user_3: %v", err)
	}
	if bob_email != "Bob Johnson" {
		t.Errorf("expected user_3 email 'Bob Johnson', got '%s'", bob_email)
	}
}

func TestRunAsanaJobWithClient_NoWorkspaces(t *testing.T) {
	db, container := setupTestDB(t)
	defer container.Terminate(context.Background())
	defer db.Close()

	// Create mock client with no workspaces
	mockClient := &MockAsanaClient{
		workspaces: []Workspace{},
		users:      []AsanaUser{},
		projects:   []AsanaProject{},
	}

	cfg := Config{
		JobTimeout: 20,
		AsanaToken: "test_token",
	}

	// Run the job with mock client (should log error internally)
	runAsanaJobWithClient(db, cfg, mockClient)

	// Verify no data was inserted
	var userCount int
	err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	if err != nil {
		t.Fatalf("failed to count users: %v", err)
	}
	if userCount != 0 {
		t.Errorf("expected 0 users with no workspaces, got %d", userCount)
	}
}

func TestRunAsanaJobWithClient_EmptyResponse(t *testing.T) {
	db, container := setupTestDB(t)
	defer container.Terminate(context.Background())
	defer db.Close()

	// Create mock client with workspace but no users/projects
	mockClient := &MockAsanaClient{
		workspaces: []Workspace{
			{GID: "workspace_123", Name: "Test Workspace"},
		},
		users:    []AsanaUser{},
		projects: []AsanaProject{},
	}

	cfg := Config{
		JobTimeout: 20,
		AsanaToken: "test_token",
	}

	// Run the job with mock client
	runAsanaJobWithClient(db, cfg, mockClient)

	// Assert no users or projects were inserted
	var userCount, projectCount int
	db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	db.QueryRow("SELECT COUNT(*) FROM projects").Scan(&projectCount)

	if userCount != 0 {
		t.Errorf("expected 0 users, got %d", userCount)
	}
	if projectCount != 0 {
		t.Errorf("expected 0 projects, got %d", projectCount)
	}
}

func TestRunAsanaJobWithClient_UsesFirstWorkspace(t *testing.T) {
	db, container := setupTestDB(t)
	defer container.Terminate(context.Background())
	defer db.Close()

	// Create mock client with multiple workspaces
	mockClient := &MockAsanaClient{
		workspaces: []Workspace{
			{GID: "workspace_1", Name: "First Workspace"},
			{GID: "workspace_2", Name: "Second Workspace"},
		},
		users: []AsanaUser{
			{GID: "user_1", Name: "Test User", ResourceType: "user"},
		},
		projects: []AsanaProject{
			{GID: "proj_1", Name: "Test Project"},
		},
	}

	cfg := Config{
		JobTimeout: 20,
		AsanaToken: "test_token",
	}

	// Run the job with mock client
	runAsanaJobWithClient(db, cfg, mockClient)

	// Verify data was inserted (proving the first workspace was used)
	var userCount int
	db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	if userCount != 1 {
		t.Errorf("expected 1 user, got %d", userCount)
	}

	// Verify specific user data
	var userEmail string
	err := db.QueryRow("SELECT email FROM users WHERE gid = $1", "user_1").Scan(&userEmail)
	if err != nil {
		t.Fatalf("failed to query user: %v", err)
	}
	if userEmail != "Test User" {
		t.Errorf("expected user email 'Test User', got '%s'", userEmail)
	}
}

// PaginatedMockAsanaClient is a mock that handles pagination
type PaginatedMockAsanaClient struct {
	workspaces    []Workspace
	usersPage1    []AsanaUser
	usersPage2    []AsanaUser
	projectsPage1 []AsanaProject
	projectsPage2 []AsanaProject
}

func (pmc *PaginatedMockAsanaClient) GetWorkspaces() ([]Workspace, error) {
	return pmc.workspaces, nil
}

func (pmc *PaginatedMockAsanaClient) GetUsers(workspaceID, limit, offset string) (*UsersResponse, error) {
	// First page (offset empty or "0")
	if offset == "" || offset == "0" {
		return &UsersResponse{
			Data: pmc.usersPage1,
			NextPage: &NextPage{
				Offset: "100",
				Path:   "/workspaces/" + workspaceID + "/users",
				URI:    "https://app.asana.com/api/1.0/workspaces/" + workspaceID + "/users?offset=100",
			},
		}, nil
	}
	// Second page (offset "100")
	if offset == "100" {
		return &UsersResponse{
			Data:     pmc.usersPage2,
			NextPage: nil,
		}, nil
	}
	// Default: return empty
	return &UsersResponse{
		Data:     []AsanaUser{},
		NextPage: nil,
	}, nil
}

func (pmc *PaginatedMockAsanaClient) GetProjects(workspaceID, limit, offset string) (*ProjectsResponse, error) {
	// First page (offset empty or "0")
	if offset == "" || offset == "0" {
		return &ProjectsResponse{
			Data: pmc.projectsPage1,
			NextPage: &NextPage{
				Offset: "100",
				Path:   "/projects",
				URI:    "https://app.asana.com/api/1.0/projects?workspace=" + workspaceID + "&offset=100",
			},
		}, nil
	}
	// Second page (offset "100")
	if offset == "100" {
		return &ProjectsResponse{
			Data:     pmc.projectsPage2,
			NextPage: nil,
		}, nil
	}
	// Default: return empty
	return &ProjectsResponse{
		Data:     []AsanaProject{},
		NextPage: nil,
	}, nil
}

func TestRunAsanaJobWithClient_PaginatedUsers(t *testing.T) {
	db, container := setupTestDB(t)
	defer container.Terminate(context.Background())
	defer db.Close()

	// Create paginated mock with users split across 2 pages
	mockClient := &PaginatedMockAsanaClient{
		workspaces: []Workspace{
			{GID: "workspace_123", Name: "Test Workspace"},
		},
		usersPage1: []AsanaUser{
			{GID: "user_1", Name: "Alice", ResourceType: "user"},
			{GID: "user_2", Name: "Bob", ResourceType: "user"},
		},
		usersPage2: []AsanaUser{
			{GID: "user_3", Name: "Charlie", ResourceType: "user"},
			{GID: "user_4", Name: "Diana", ResourceType: "user"},
		},
		projectsPage1: []AsanaProject{
			{GID: "proj_1", Name: "Project A"},
		},
		projectsPage2: []AsanaProject{},
	}

	cfg := Config{
		JobTimeout: 20,
		AsanaToken: "test_token",
	}

	// Run the job with paginated mock
	runAsanaJobWithClient(db, cfg, mockClient)

	// Assert all users from both pages were inserted
	var userCount int
	err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	if err != nil {
		t.Fatalf("failed to count users: %v", err)
	}
	if userCount != 4 {
		t.Errorf("expected 4 users (2 pages x 2), got %d", userCount)
	}

	// Verify specific users exist with expected names
	expectedUsers := map[string]string{
		"user_1": "Alice",
		"user_2": "Bob",
		"user_3": "Charlie",
		"user_4": "Diana",
	}

	for gid, expectedName := range expectedUsers {
		var email string
		err := db.QueryRow("SELECT email FROM users WHERE gid = $1", gid).Scan(&email)
		if err != nil {
			t.Fatalf("failed to query user %s: %v", gid, err)
		}
		if email != expectedName {
			t.Errorf("for %s, expected email '%s', got '%s'", gid, expectedName, email)
		}
	}
}

func TestRunAsanaJobWithClient_PaginatedProjects(t *testing.T) {
	db, container := setupTestDB(t)
	defer container.Terminate(context.Background())
	defer db.Close()

	// Create paginated mock with projects split across 2 pages
	mockClient := &PaginatedMockAsanaClient{
		workspaces: []Workspace{
			{GID: "workspace_123", Name: "Test Workspace"},
		},
		usersPage1: []AsanaUser{
			{GID: "user_1", Name: "User One", ResourceType: "user"},
		},
		usersPage2: []AsanaUser{},
		projectsPage1: []AsanaProject{
			{GID: "proj_1", Name: "Project X"},
			{GID: "proj_2", Name: "Project Y"},
			{GID: "proj_3", Name: "Project Z"},
		},
		projectsPage2: []AsanaProject{
			{GID: "proj_4", Name: "Project W"},
			{GID: "proj_5", Name: "Project V"},
		},
	}

	cfg := Config{
		JobTimeout: 20,
		AsanaToken: "test_token",
	}

	// Run the job with paginated mock
	runAsanaJobWithClient(db, cfg, mockClient)

	// Assert all projects from both pages were inserted
	var projectCount int
	err := db.QueryRow("SELECT COUNT(*) FROM projects").Scan(&projectCount)
	if err != nil {
		t.Fatalf("failed to count projects: %v", err)
	}
	if projectCount != 5 {
		t.Errorf("expected 5 projects (page1: 3 + page2: 2), got %d", projectCount)
	}

	// Verify we have projects from both pages with correct names
	exepectedProjects := map[string]string{
		"proj_1": "Project X",
		"proj_3": "Project Z",
		"proj_5": "Project V",
	}

	for gid, expectedName := range exepectedProjects {
		var projectName string
		err = db.QueryRow("SELECT project FROM projects WHERE gid = $1", gid).Scan(&projectName)
		if err != nil {
			t.Fatalf("failed to query project %s: %v", gid, err)
		}
		if projectName != expectedName {
			t.Errorf("for %s, expected name '%s', got '%s'", gid, expectedName, projectName)
		}
	}
}

func TestRunAsanaJobWithClient_PaginatedBoth(t *testing.T) {
	db, container := setupTestDB(t)
	defer container.Terminate(context.Background())
	defer db.Close()

	// Create paginated mock with both users and projects split across pages
	mockClient := &PaginatedMockAsanaClient{
		workspaces: []Workspace{
			{GID: "workspace_123", Name: "Test Workspace"},
		},
		usersPage1: []AsanaUser{
			{GID: "user_1", Name: "First User", ResourceType: "user"},
			{GID: "user_2", Name: "Second User", ResourceType: "user"},
		},
		usersPage2: []AsanaUser{
			{GID: "user_3", Name: "Third User", ResourceType: "user"},
		},
		projectsPage1: []AsanaProject{
			{GID: "proj_1", Name: "First Project"},
		},
		projectsPage2: []AsanaProject{
			{GID: "proj_2", Name: "Second Project"},
			{GID: "proj_3", Name: "Third Project"},
		},
	}

	cfg := Config{
		JobTimeout: 20,
		AsanaToken: "test_token",
	}

	// Run the job with paginated mock
	runAsanaJobWithClient(db, cfg, mockClient)

	// Assert users from both pages
	var userCount int
	err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	if err != nil {
		t.Fatalf("failed to count users: %v", err)
	}
	if userCount != 3 {
		t.Errorf("expected 3 users, got %d", userCount)
	}

	// Assert projects from both pages
	var projectCount int
	err = db.QueryRow("SELECT COUNT(*) FROM projects").Scan(&projectCount)
	if err != nil {
		t.Fatalf("failed to count projects: %v", err)
	}
	if projectCount != 3 {
		t.Errorf("expected 3 projects, got %d", projectCount)
	}

	// Verify specific project data from both pages
	paginatedProjects := map[string]string{
		"proj_1": "First Project",
		"proj_2": "Second Project",
		"proj_3": "Third Project",
	}

	for gid, expectedName := range paginatedProjects {
		var projectName string
		err = db.QueryRow("SELECT project FROM projects WHERE gid = $1", gid).Scan(&projectName)
		if err != nil {
			t.Fatalf("failed to query project %s: %v", gid, err)
		}
		if projectName != expectedName {
			t.Errorf("for %s, expected name '%s', got '%s'", gid, expectedName, projectName)
		}
	}

	// Verify specific user data
	userEmailData := map[string]string{
		"user_1": "First User",
		"user_2": "Second User",
		"user_3": "Third User",
	}

	for gid, expectedEmail := range userEmailData {
		var email string
		err = db.QueryRow("SELECT email FROM users WHERE gid = $1", gid).Scan(&email)
		if err != nil {
			t.Fatalf("failed to query user %s: %v", gid, err)
		}
		if email != expectedEmail {
			t.Errorf("for %s, expected email '%s', got '%s'", gid, expectedEmail, email)
		}
	}
}
