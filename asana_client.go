package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
)

const asanaBaseURL = "https://app.asana.com/api/1.0"
const maxRetries = 3
const baseRetryDelay = 1 * time.Second

// AsanaClientInterface defines the contract for Asana API interactions
type AsanaClientInterface interface {
	GetWorkspaces() ([]Workspace, error)
	GetUsers(workspaceID, limit, offset string) (*UsersResponse, error)
	GetProjects(workspaceID, limit, offset string) (*ProjectsResponse, error)
}

// AsanaClient handles all Asana API interactions
type AsanaClient struct {
	token  string
	client *http.Client
}

// Workspace represents an Asana workspace
type Workspace struct {
	GID  string `json:"gid"`
	Name string `json:"name"`
}

// AsanaUser represents an Asana user
type AsanaUser struct {
	GID          string `json:"gid"`
	Name         string `json:"name"`
	ResourceType string `json:"resource_type"`
}

// Project represents an Asana project
type AsanaProject struct {
	GID  string `json:"gid"`
	Name string `json:"name"`
}

// NextPage represents pagination information
type NextPage struct {
	Offset string `json:"offset"`
	Path   string `json:"path"`
	URI    string `json:"uri"`
}

// WorkspacesResponse represents the API response for workspaces
type WorkspacesResponse struct {
	Data []Workspace `json:"data"`
}

// UsersResponse represents the API response for users with pagination
type UsersResponse struct {
	Data     []AsanaUser `json:"data"`
	NextPage *NextPage   `json:"next_page"`
}

// ProjectsResponse represents the API response for projects with pagination
type ProjectsResponse struct {
	Data     []AsanaProject `json:"data"`
	NextPage *NextPage      `json:"next_page"`
}

// NewAsanaClient creates a new Asana API client
func NewAsanaClient(token string) *AsanaClient {
	return &AsanaClient{
		token:  token,
		client: &http.Client{},
	}
}

// makeRequest performs an HTTP request to the Asana API with retry mechanism for rate limiting
func (ac *AsanaClient) makeRequest(method, endpoint string) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest(method, fmt.Sprintf("%s%s", asanaBaseURL, endpoint), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		// Set authorization header
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", ac.token))
		req.Header.Set("Content-Type", "application/json")

		// Execute request
		resp, err := ac.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to execute request: %w", err)
		}
		defer resp.Body.Close()

		// Read response body
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read response body: %w", err)
		}

		// Handle rate limiting (HTTP 429)
		if resp.StatusCode == http.StatusTooManyRequests {
			if attempt < maxRetries {
				// Calculate exponential backoff: 1s, 2s, 4s
				retryAfter := baseRetryDelay * time.Duration(1<<uint(attempt))

				// Get retry delay from Retry-After header if provided, otherwise use exponential backoff
				if retryAfterHeader := resp.Header.Get("Retry-After"); retryAfterHeader != "" {
					if seconds, err := strconv.Atoi(retryAfterHeader); err == nil {
						retryAfter = time.Duration(seconds) * time.Second
					}
				}

				log.Printf("Rate limited (429). Attempt %d/%d. Retrying after %v...\n", attempt+1, maxRetries, retryAfter)
				time.Sleep(retryAfter)
				lastErr = fmt.Errorf("rate limited: %s", string(body))
				continue
			}
			// Max retries exceeded
			return nil, fmt.Errorf("rate limited after %d retries: %s", maxRetries, string(body))
		}

		// Check for other HTTP errors
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("asana API error: status %d, body: %s", resp.StatusCode, string(body))
		}

		// Success
		return body, nil
	}

	return nil, fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

// GetWorkspaces retrieves all workspaces
func (ac *AsanaClient) GetWorkspaces() ([]Workspace, error) {
	body, err := ac.makeRequest("GET", "/workspaces")
	if err != nil {
		return nil, err
	}

	var response WorkspacesResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse workspaces response: %w", err)
	}

	return response.Data, nil
}

// GetUsers retrieves users in a workspace with optional pagination (limit and offset)
func (ac *AsanaClient) GetUsers(workspaceID, limit, offset string) (*UsersResponse, error) {
	endpoint := fmt.Sprintf("/workspaces/%s/users", workspaceID)

	// Add query parameters if provided
	if limit != "" || offset != "" {
		endpoint += "?"
		if limit != "" {
			endpoint += fmt.Sprintf("limit=%s", limit)
			if offset != "" {
				endpoint += "&"
			}
		}
		if offset != "" {
			endpoint += fmt.Sprintf("offset=%s", offset)
		}
	}

	body, err := ac.makeRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var response UsersResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse users response: %w", err)
	}

	return &response, nil
}

// GetProjects retrieves projects in a workspace with optional pagination (limit and offset)
func (ac *AsanaClient) GetProjects(workspaceID, limit, offset string) (*ProjectsResponse, error) {
	endpoint := fmt.Sprintf("/projects?workspace=%s", workspaceID)

	// Add query parameters if provided
	if limit != "" || offset != "" {
		if limit != "" {
			endpoint += fmt.Sprintf("&limit=%s", limit)
		}
		if offset != "" {
			endpoint += fmt.Sprintf("&offset=%s", offset)
		}
	}

	body, err := ac.makeRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var response ProjectsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse projects response: %w", err)
	}

	return &response, nil
}
