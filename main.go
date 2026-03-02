package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/kelseyhightower/envconfig"
	_ "github.com/lib/pq"
)

// Config holds the application configuration
type Config struct {
	JobTimeout  int    `envconfig:"JOB_TIMEOUT" default:"20"`
	AsanaToken  string `envconfig:"ASANA_TOKEN" default:""`
	DatabaseURL string `envconfig:"DATABASE_URL" default:"postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"`
}

func main() {
	// Load configuration from environment variables
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		log.Fatal("Failed to process environment variables:", err)
	}

	fmt.Printf("Configuration loaded: JobTimeout=%d, AsanaToken=%s\n", cfg.JobTimeout, cfg.AsanaToken)

	// Connect to database
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		log.Fatal("Failed to ping database:", err)
	}

	// Run migrations
	if err := runMigrations(db); err != nil {
		log.Fatal("Failed to run migrations:", err)
	}

	fmt.Println("Application started successfully")

	// Start asana job
	StartAsanaJob(db, cfg)

	// Keep the application running
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Application shutting down...")
}

func runMigrations(db *sql.DB) error {
	// Read migration file
	migrationSQL, err := os.ReadFile("sql/migrations.sql")
	if err != nil {
		return fmt.Errorf("failed to read migrations file: %w", err)
	}

	// Execute migrations
	_, err = db.Exec(string(migrationSQL))
	if err != nil {
		return fmt.Errorf("failed to execute migrations: %w", err)
	}

	fmt.Println("Migrations applied successfully")
	return nil
}
