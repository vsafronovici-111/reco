# reco

## Setup

### Install Task

Install the Task runner using the official installation script:

```bash
curl -sL https://taskfile.dev/install.sh | sh
```

### Configure PATH

Add the Task binary to your PATH:

```bash
export PATH="$PATH:$(pwd)/bin"
```

To make this permanent, add the line to your `~/.zshrc`:

```bash
echo 'export PATH="$PATH:$(pwd)/bin"' >> ~/.zshrc
source ~/.zshrc
```

## Running Tasks

- `task run:server` - Run the application with docker
- `task run:app` - Run the application without docker
- `task docker:up` - Start docker-compose services
- `task docker:down` - Stop docker-compose services
- `task start:all` - Start docker services and the application

## Environment Variables

The application requires the following environment variables:

- `JOB_TIMEOUT` - Interval (in seconds) for running asana synchronization job (default: 1)
- `ASANA_TOKEN` - Your Asana API personal access token

### Running with Custom Environment Variables

**Option 1: Pass via command line**
```bash
JOB_TIMEOUT=20 ASANA_TOKEN="your_token_here" task run:server
```

**Option 2: Update Taskfile.yml**
Edit the `run:app` task in `Taskfile.yml` to set your preferred values:
```yaml
run:app:
  desc: Run the application (without docker)
  cmds:
    - go run .
  env:
    JOB_TIMEOUT: "20"
    ASANA_TOKEN: "your_token_here"
```

Then run: `task run:server`

**Option 3: Set environment in your shell**
```bash
export JOB_TIMEOUT=20
export ASANA_TOKEN="your_token_here"
task run:server
```

## Notes

> **Note:** In a real-world application, database migrations would be managed using a dedicated migration tool like [Goose](https://github.com/pressly/goose). This project uses manual SQL migrations for simplicity, but production systems should implement proper schema versioning and rollback capabilities provided by migration tools.

> **Database Advisory Locks:** This application uses PostgreSQL advisory locks (`pg_advisory_lock`) to ensure that only one instance of the application can execute the Asana synchronization job at a time. This prevents race conditions and data conflicts when multiple instances are running concurrently. The lock is acquired before fetching data and released after the job completes, ensuring atomic operations across distributed deployments.