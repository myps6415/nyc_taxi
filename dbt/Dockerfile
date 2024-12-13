FROM python:3.11-slim

WORKDIR /usr/app/dbt

# Install system dependencies including git
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install dbt
RUN pip install dbt-core dbt-postgres

# Copy dbt project files
COPY . .

# Run dbt as default command
CMD ["dbt", "run"]