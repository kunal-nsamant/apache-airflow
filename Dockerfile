FROM ${AIRFLOW_IMAGE_BASE}

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install MS ODBC Driver 17 for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-tools.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
    msodbcsql17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow