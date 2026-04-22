#!/bin/bash
# =============================================================
# EC2 Bootstrap — Nutrition Platform
# Runs automatically on first boot via EC2 user-data.
# Installs: Python 3.11, PostgreSQL 15, project dependencies.
# Clones project from S3, applies schema, installs systemd service.
# =============================================================
set -e
exec > /var/log/nutrition-bootstrap.log 2>&1

echo "=== Bootstrap started ==="

# ── System packages ───────────────────────────────────────────
dnf update -y
dnf install -y python3.11 python3.11-pip postgresql15 postgresql15-server git java-17-amazon-corretto-headless

# ── PostgreSQL setup ──────────────────────────────────────────
postgresql-setup --initdb
systemctl enable postgresql
systemctl start postgresql

sudo -u postgres psql -c "CREATE USER de_user WITH PASSWORD 'de_pass';"
sudo -u postgres psql -c "CREATE DATABASE nutrition_dw OWNER de_user;"
# Allow local password auth (pg_hba.conf defaults to ident for local connections)
sed -i 's/local   all             all                                     peer/local   all             all                                     md5/' /var/lib/pgsql/data/pg_hba.conf
sed -i 's/host    all             all             127.0.0.1\/32            ident/host    all             all             127.0.0.1\/32            md5/' /var/lib/pgsql/data/pg_hba.conf
systemctl restart postgresql

# ── Download project from S3 ──────────────────────────────────
mkdir -p /home/ec2-user/nutrition-data-platform/app
aws s3 sync s3://daud-nutrition-platform-data/scripts/ /home/ec2-user/nutrition-data-platform/ \
  --exclude '__pycache__/*'
aws s3 sync s3://daud-nutrition-platform-data/app/ /home/ec2-user/nutrition-data-platform/app/ \
  --exclude '__pycache__/*'
touch /home/ec2-user/nutrition-data-platform/app/__init__.py
# Download the raw data
mkdir -p /home/ec2-user/nutrition-data-platform/data/raw
mkdir -p /home/ec2-user/nutrition-data-platform/data/input
aws s3 cp s3://daud-nutrition-platform-data/raw/nutrition.csv /home/ec2-user/nutrition-data-platform/data/raw/nutrition.csv
aws s3 cp s3://daud-nutrition-platform-data/raw/user_profiles.csv /home/ec2-user/nutrition-data-platform/data/input/user_profiles.csv

chown -R ec2-user:ec2-user /home/ec2-user/nutrition-data-platform

# ── Python dependencies ───────────────────────────────────────
cd /home/ec2-user/nutrition-data-platform
pip3.11 install fastapi uvicorn sqlalchemy psycopg2-binary kafka-python pydantic python-dotenv pandas pyarrow boto3

# ── Kafka (KRaft mode — no ZooKeeper) ────────────────────────
KAFKA_VERSION=3.7.0
KAFKA_SCALA=2.13
cd /tmp
wget -q "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz" -O kafka.tgz
tar -xzf kafka.tgz
mv "kafka_${KAFKA_SCALA}-${KAFKA_VERSION}" /opt/kafka
chown -R root:root /opt/kafka

# Generate cluster UUID and format storage (KRaft single-node)
KAFKA_UUID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
/opt/kafka/bin/kafka-storage.sh format \
  -t "$KAFKA_UUID" \
  -c /opt/kafka/config/kraft/server.properties

# systemd service for Kafka
cat > /etc/systemd/system/kafka.service << 'KAFKAEOF'
[Unit]
Description=Apache Kafka (KRaft)
After=network.target

[Service]
Type=simple
User=root
Environment="KAFKA_HEAP_OPTS=-Xmx256m -Xms128m"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
KAFKAEOF

systemctl daemon-reload
systemctl enable kafka
systemctl start kafka
echo "Waiting for Kafka to start..."
sleep 10
/opt/kafka/bin/kafka-topics.sh --create --topic user_profiles \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1 2>/dev/null || true

# ── .env file ─────────────────────────────────────────────────
cat > /home/ec2-user/nutrition-data-platform/.env << 'EOF'
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=nutrition_dw
POSTGRES_USER=de_user
POSTGRES_PASSWORD=de_pass
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=user_profiles
KAFKA_GROUP_ID=nutrition-consumer-group
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio123
MINIO_BUCKET=nutrition-platform
LOG_LEVEL=INFO
EOF

chown ec2-user:ec2-user /home/ec2-user/nutrition-data-platform/.env

# ── Apply warehouse schema ─────────────────────────────────────
PGPASSWORD=de_pass psql -U de_user -d nutrition_dw -h localhost \
  -f /home/ec2-user/nutrition-data-platform/create_schema.sql

# ── systemd service ───────────────────────────────────────────
cat > /etc/systemd/system/nutrition-api.service << 'EOF'
[Unit]
Description=Nutrition Platform FastAPI
After=network.target postgresql.service

[Service]
User=ec2-user
WorkingDirectory=/home/ec2-user/nutrition-data-platform
EnvironmentFile=/home/ec2-user/nutrition-data-platform/.env
ExecStart=/usr/bin/python3.11 -m uvicorn app.main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable nutrition-api
systemctl start nutrition-api

echo "=== Bootstrap complete ==="
