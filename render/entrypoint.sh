#!/bin/bash
set -e  # Exit on any error

echo "🚀 Starting Render initialization..."

# 1. Setup persistent directories on the mounted disk
mkdir -p /data/airflow/dags
mkdir -p /data/airflow/logs
mkdir -p /data/airflow/plugins
mkdir -p /data/airflow/other_stuff

# 2. Copy configuration files to persistent storage (only if they don't exist)
# This preserves configs across deployments
if [ ! -f /data/airflow/airflow.cfg ]; then
    echo "📝 Copying airflow.cfg to persistent storage..."
    if [ -f /home/airflow/other_stuff/docker_airflow.cfg ]; then
        cp /home/airflow/other_stuff/docker_airflow.cfg /data/airflow/airflow.cfg
    else
        echo "⚠️  Config file not found, using defaults"
    fi
else
    echo "✅ airflow.cfg already exists in persistent storage"
fi

# 3. Clone/Pull DAGs from Git repository
if [ -n "$DAGS_GIT_REPO" ]; then
    echo "📦 Syncing DAGs from Git repository..."
    
    if [ -d /data/airflow/dags/.git ]; then
        echo "🔄 Updating existing DAGs repository..."
        cd /data/airflow/dags
        git pull origin ${DAGS_GIT_BRANCH:-main}
    else
        echo "📥 Cloning DAGs repository..."
        rm -rf /data/airflow/dags/*
        git clone ${DAGS_GIT_REPO} /data/airflow/dags
        cd /data/airflow/dags
        git checkout ${DAGS_GIT_BRANCH:-main}
    fi
else
    echo "⚠️  No DAGS_GIT_REPO environment variable set"
    echo "   DAGs directory will be empty unless manually uploaded"
fi

# 4. Sync other_stuff directory if it exists in the image
if [ -d /home/airflow/other_stuff ]; then
    echo "📦 Syncing other_stuff..."
    rsync -av /home/airflow/other_stuff/ /data/airflow/other_stuff/
fi

# 5. Create symlinks so Airflow can find everything
# Point Airflow to the persistent storage locations
export AIRFLOW_HOME=/data/airflow
export AIRFLOW__CORE__DAGS_FOLDER=/data/airflow/dags
export AIRFLOW__CORE__BASE_LOG_FOLDER=/data/airflow/logs
export AIRFLOW__CORE__PLUGINS_FOLDER=/data/airflow/plugins

# 6. Initialize Airflow database (only runs once)
if [ ! -f /data/airflow/airflow.db ]; then
    echo "🗄️ Initializing Airflow database..."
    airflow db init
    
    # Create default admin user
    echo "👤 Creating admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
fi

# 7. Run database migrations (in case of upgrades)
echo "🔄 Running database migrations..."
airflow db upgrade

echo "✨ Initialization complete!"
echo "🌐 Starting Airflow webserver..."

# 8. Finally, run your original entrypoint
exec /app/News-Api-Project/entrypoint.sh