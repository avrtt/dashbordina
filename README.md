This is a marketing analytics dashboard for audience segmentation and multi-channel ROI analysis.

Originally built as an infrastructure to solve customer marketing campaign data problems, it's now used as an easily extensible prototype in my various dashboarding work.

[Here](https://github.com/avrtt/superset-supremacy) is the Superset prototype alternative w/o ETL, featuring more configurations and deployment options.

![1](/screenshots/1.png)

Features:

- **Data Storage & ETL**: PostgreSQL with Apache Airflow for ETL processing
- **Advanced Metrics**: CAC, CLV, ROAS, and segment-specific analytics
- **Interactive Dashboard**: Dark-themed Plotly Dash UI with drill-down functionality
- **API Backend**: RESTful endpoints for all metrics
- **Containerized**: Complete Docker setup for easy deployment
- **CI/CD Pipeline**: GitHub Actions workflow for AWS ECS/EKS deployment

The application consists of the following components:

- **PostgreSQL**: Main data warehouse with raw, archive, and analytics schemas
- **Airflow**: ETL pipeline for data processing with hourly and daily DAGs
- **FastAPI**: Backend API to expose metrics
- **Plotly Dash**: Interactive frontend dashboard
- **Docker Compose**: Container orchestration for local development

## Getting started

### Prerequisites

- Docker and Docker Compose
- Git

### Local setup

1. Clone the repository:
   ```bash
   git clone https://github.com/avrtt/dashbordina.git
   cd dashbordina
   ```

2. Start the services with Docker Compose:
   ```bash
   docker-compose up -d
   ```

3. Access the services:
   - Dash Dashboard: http://localhost:8050
   - Airflow UI: http://localhost:8080
   - FastAPI Docs: http://localhost:8000/docs

The database is automatically initialized with the schema and demo data when the containers start up. You can find the initialization scripts in the `init-db` directory.

## Airflow DAGs

The project includes two main DAGs:

1. **marketing_etl_hourly**: Runs every hour to extract, transform, and load marketing event data
2. **marketing_daily_archive**: Runs daily to create snapshots of the previous day's data

To trigger a DAG manually in the Airflow UI:

1. Navigate to http://localhost:8080
2. Log in with username `admin` and password `admin`
3. Find the DAG you want to run in the list
4. Click the "Trigger DAG" button

## Structure

```
dashbordina/
├── app/
│   ├── api/           # FastAPI backend
│   ├── data/          # Data models and schemas
│   ├── etl/           # Airflow DAGs and operators
│   │   └── dags/      # Airflow DAG definitions
│   ├── frontend/      # Dash application
│   │   └── assets/    # CSS and JS assets
│   └── tests/         # Test cases
├── docker/            # Dockerfiles
├── init-db/           # Database initialization scripts
├── .github/           # GitHub Actions configuration
└── docker-compose.yml # Container orchestration
```

## Tests

```bash
# Run all tests
docker-compose exec dash-app pytest

# Run with coverage
docker-compose exec dash-app pytest --cov=app

# Run specific test category
docker-compose exec dash-app pytest app/tests/etl/
```

## Authentication

To add authentication to the dashboard:

Modify `app/frontend/index.py` to include Dash authentication:

```python
from dash_auth import BasicAuth

# Define valid username/password pairs
VALID_USERNAME_PASSWORD_PAIRS = {
    'admin': 'admin',
    'user': 'password'
}

# Add authentication to the app
auth = BasicAuth(app, VALID_USERNAME_PASSWORD_PAIRS)
```

For the API, add OAuth or JWT authentication in `app/api/main.py`.

## CI/CD

The project includes a GitHub Actions workflow for CI/CD:

1. **Testing**: Runs pytest with coverage reporting
2. **Building**: Builds Docker images for each component
3. **Deployment**: Deploys to AWS ECS/EKS (configuration required)

To configure AWS deployment:

1. Add the following GitHub secrets:
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `AWS_REGION`

2. Customize the deployment steps in `.github/workflows/cicd.yml` for your AWS infrastructure.

## License

MIT