# Docker Setup for Airflow

This guide explains how to run the Medallion Architecture project using Docker Compose.

## Prerequisites

- Docker Desktop (Windows/Mac) or Docker Engine + Docker Compose (Linux)
- At least 4GB of RAM allocated to Docker
- Docker Compose version 1.27.0 or higher

## Quick Start

### 1. Initialize the Environment

First, set the Airflow user ID (Linux/Mac):
```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env.docker
```

For Windows, the default UID of 50000 is already set in `.env.docker`.

### 2. Start the Services

Build and start all services:
```bash
docker-compose up -d
```

This will:
- Build the custom Airflow image with all dependencies
- Start PostgreSQL database for Airflow metadata
- Initialize the Airflow database
- Create an admin user (username: `admin`, password: `admin`)
- Start the Airflow webserver and scheduler

### 3. Access Airflow Web UI

Once all services are healthy, access the Airflow UI:
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

## Usage

### Trigger the DAG

1. In the Airflow UI, find the `medallion_pipeline` DAG
2. Click the "Play" button to trigger a manual run
3. Monitor the progress in the UI

Or use the CLI:
```bash
docker-compose exec airflow-webserver airflow dags trigger medallion_pipeline
```

### View Logs

View logs for a specific service:
```bash
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
```

### Execute Airflow CLI Commands

Run any Airflow CLI command:
```bash
docker-compose exec airflow-webserver airflow dags list
docker-compose exec airflow-webserver airflow tasks list medallion_pipeline
```

### Run dbt Manually

Execute dbt commands inside the container:
```bash
docker-compose exec airflow-webserver bash
cd dbt
dbt run
dbt test
```

### Inspect DuckDB Database

Access the DuckDB database:
```bash
docker-compose exec airflow-webserver duckdb /opt/airflow/warehouse/medallion.duckdb
```

Then run SQL queries:
```sql
.tables
SELECT * FROM fct_customer_transactions LIMIT 10;
.quit
```

## Managing the Services

### Stop Services (keep data)
```bash
docker-compose down
```

### Stop Services and Remove Volumes (clean slate)
```bash
docker-compose down -v
```

### Restart Services
```bash
docker-compose restart
```

### Rebuild After Dependency Changes

If you modify `requirements.txt`:
```bash
docker-compose build
docker-compose up -d
```

### View Running Containers
```bash
docker-compose ps
```

## Project Structure

The following directories are mounted as volumes:

| Host Path | Container Path | Purpose |
|-----------|---------------|---------|
| `./dags` | `/opt/airflow/dags` | DAG definitions |
| `./data` | `/opt/airflow/data` | Raw, clean, and quality data |
| `./dbt` | `/opt/airflow/dbt` | dbt project |
| `./include` | `/opt/airflow/include` | Custom Python modules |
| `./profiles` | `/opt/airflow/profiles` | dbt profiles |
| `./warehouse` | `/opt/airflow/warehouse` | DuckDB database |
| `./airflow_home/logs` | `/opt/airflow/logs` | Airflow logs |

## Environment Variables

Key environment variables are set in `docker-compose.yml`:

- `AIRFLOW__CORE__EXECUTOR`: LocalExecutor (suitable for single-machine deployments)
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`: PostgreSQL connection string
- `DBT_PROFILES_DIR`: Points to the profiles directory
- `DUCKDB_PATH`: Path to the DuckDB database file

## Troubleshooting

### Permission Issues

If you encounter permission errors on Linux/Mac:
```bash
sudo chown -R $(id -u):$(id -g) airflow_home/ warehouse/ data/
```

### Services Won't Start

Check service health:
```bash
docker-compose ps
```

View detailed logs:
```bash
docker-compose logs
```

### Port Already in Use

If port 8080 is already in use, modify `docker-compose.yml`:
```yaml
ports:
  - "8081:8080"  # Change host port to 8081
```

### Database Connection Issues

Reset the database:
```bash
docker-compose down -v
docker-compose up -d
```

### Out of Memory

Increase Docker memory allocation:
- Docker Desktop: Settings → Resources → Memory (increase to 4GB+)

## Production Considerations

For production deployments, consider:

1. **Change default credentials** in `.env.docker`
2. **Use CeleryExecutor** for distributed task execution
3. **Add Redis** for Celery backend
4. **External PostgreSQL** instead of containerized version
5. **Implement secrets management** (e.g., Airflow Connections, Variables)
6. **Set up monitoring** (e.g., Prometheus, Grafana)
7. **Configure resource limits** in docker-compose.yml
8. **Use docker-compose.override.yml** for environment-specific configs

## Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [dbt Documentation](https://docs.getdbt.com/)
