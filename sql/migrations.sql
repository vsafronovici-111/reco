-- Migration file for database schema

CREATE TABLE IF NOT EXISTS users (
  gid VARCHAR(255) PRIMARY KEY,
  email VARCHAR(255) NOT NULL,
  jobid VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS projects (
  gid VARCHAR(255) PRIMARY KEY,
  project VARCHAR(255) NOT NULL,
  jobid VARCHAR(255)
);
