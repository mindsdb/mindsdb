# MindsDB Streamlined Docker Deployment Guide

## Overview

This guide covers deploying the streamlined MindsDB instance using Docker, featuring 90% handler reduction and optimized configuration for production environments.

## Quick Start

### Using Docker Compose (Recommended)

```bash
# Clone the repository
git clone https://github.com/TradieMate/mindsdb.git
cd mindsdb

# Build and start the streamlined container
docker-compose -f docker-compose.streamlined.yml up -d

# Check status
docker-compose -f docker-compose.streamlined.yml logs -f
```

### Using Docker Build

```bash
# Build the streamlined image
docker build -f Dockerfile.streamlined -t mindsdb-streamlined .

# Run the container
docker run -d \
  --name mindsdb-streamlined \
  -p 47334:47334 \
  -v mindsdb_data:/home/mindsdb/data \
  -e MINDSDB_ENABLED_HANDLERS=airtable,supabase,gmail,google_analytics,lightwood,huggingface,huggingface_api,langchain,langchain_embedding,pgvector,postgres,sheets,s3,minds_endpoint,rag \
  mindsdb-streamlined
```

## Configuration Options

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MINDSDB_STORAGE_DIR` | `/home/mindsdb/data` | Data storage directory |
| `MINDSDB_CONFIG_PATH` | `/home/mindsdb/config.json` | Configuration file path |
| `MINDSDB_APIS` | `http` | Enabled APIs (http,mysql,postgres) |
| `MINDSDB_ENABLED_HANDLERS` | All 15 handlers | Comma-separated handler list |
| `MINDSDB_DOCKER_ENV` | `True` | Docker environment flag |
| `MINDSDB_LOG_LEVEL` | `INFO` | Logging level |

### Enabled Handlers

The streamlined version includes only these 15 essential handlers:

#### Data Connectors
- `airtable` - Airtable integration
- `supabase` - Supabase database
- `postgres` - PostgreSQL database
- `sheets` - Google Sheets
- `s3` - Amazon S3 storage

#### AI/ML Integrations
- `lightwood` - Core ML engine
- `huggingface` - Hugging Face models
- `huggingface_api` - Hugging Face API
- `langchain` - LangChain framework
- `langchain_embedding` - LangChain embeddings

#### Vector/RAG
- `pgvector` - PostgreSQL vector extension
- `rag` - Retrieval-Augmented Generation

#### Communication
- `gmail` - Gmail integration
- `google_analytics` - Google Analytics

#### Core Services
- `minds_endpoint` - MindsDB endpoint handler

## Deployment Scenarios

### Development Environment

```bash
# Copy environment template
cp .env.streamlined.template .env.streamlined

# Edit configuration as needed
nano .env.streamlined

# Start with local script
./scripts/start-streamlined.sh
```

### Production Environment

```bash
# Use Docker Compose with custom configuration
docker-compose -f docker-compose.streamlined.yml up -d

# Monitor logs
docker-compose -f docker-compose.streamlined.yml logs -f mindsdb-streamlined

# Scale if needed (not applicable for single instance)
docker-compose -f docker-compose.streamlined.yml restart mindsdb-streamlined
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mindsdb-streamlined
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mindsdb-streamlined
  template:
    metadata:
      labels:
        app: mindsdb-streamlined
    spec:
      containers:
      - name: mindsdb
        image: mindsdb-streamlined:latest
        ports:
        - containerPort: 47334
        env:
        - name: MINDSDB_ENABLED_HANDLERS
          value: "airtable,supabase,gmail,google_analytics,lightwood,huggingface,huggingface_api,langchain,langchain_embedding,pgvector,postgres,sheets,s3,minds_endpoint,rag"
        - name: MINDSDB_DOCKER_ENV
          value: "True"
        volumeMounts:
        - name: data-volume
          mountPath: /home/mindsdb/data
      volumes:
      - name: data-volume
        persistentVolumeClaim:
          claimName: mindsdb-data-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: mindsdb-streamlined-service
spec:
  selector:
    app: mindsdb-streamlined
  ports:
  - port: 47334
    targetPort: 47334
  type: LoadBalancer
```

## Health Checks

### Container Health Check

The Docker container includes a built-in health check:

```bash
# Check container health
docker inspect --format='{{.State.Health.Status}}' mindsdb-streamlined

# View health check logs
docker inspect --format='{{range .State.Health.Log}}{{.Output}}{{end}}' mindsdb-streamlined
```

### Manual Health Check

```bash
# Test HTTP API
curl http://localhost:47334/api/status

# Expected response
{
  "status": "ok",
  "version": "...",
  "handlers": 15
}
```

## Performance Optimization

### Resource Requirements

#### Minimum Requirements
- **CPU**: 2 cores
- **Memory**: 4GB RAM
- **Storage**: 10GB
- **Network**: 1Gbps

#### Recommended for Production
- **CPU**: 4+ cores
- **Memory**: 8GB+ RAM
- **Storage**: 50GB+ SSD
- **Network**: 10Gbps

### Docker Resource Limits

```yaml
services:
  mindsdb-streamlined:
    # ... other configuration
    deploy:
      resources:
        limits:
          cpus: '4.0'
          memory: 8G
        reservations:
          cpus: '2.0'
          memory: 4G
```

## Security Considerations

### Container Security

```bash
# Run as non-root user (already configured)
USER mindsdb

# Use read-only filesystem where possible
docker run --read-only --tmpfs /tmp mindsdb-streamlined

# Limit capabilities
docker run --cap-drop=ALL --cap-add=NET_BIND_SERVICE mindsdb-streamlined
```

### Network Security

```bash
# Use custom network
docker network create mindsdb-network
docker run --network mindsdb-network mindsdb-streamlined

# Expose only necessary ports
docker run -p 127.0.0.1:47334:47334 mindsdb-streamlined
```

## Monitoring and Logging

### Log Management

```bash
# View real-time logs
docker logs -f mindsdb-streamlined

# Export logs
docker logs mindsdb-streamlined > mindsdb.log

# Configure log rotation
docker run --log-driver=json-file --log-opt max-size=10m --log-opt max-file=3 mindsdb-streamlined
```

### Metrics Collection

```bash
# Container metrics
docker stats mindsdb-streamlined

# Application metrics (if enabled)
curl http://localhost:47334/metrics
```

## Troubleshooting

### Common Issues

#### Container Won't Start
```bash
# Check logs
docker logs mindsdb-streamlined

# Check configuration
docker exec mindsdb-streamlined cat /home/mindsdb/config.json

# Verify environment variables
docker exec mindsdb-streamlined env | grep MINDSDB
```

#### Handler Loading Issues
```bash
# Check enabled handlers
docker exec mindsdb-streamlined python -c "import os; print(os.environ.get('MINDSDB_ENABLED_HANDLERS'))"

# Test handler import
docker exec mindsdb-streamlined python -c "from mindsdb.integrations.handlers import get_enabled_handlers; print(get_enabled_handlers())"
```

#### Performance Issues
```bash
# Check resource usage
docker stats mindsdb-streamlined

# Monitor system resources
docker exec mindsdb-streamlined top

# Check disk usage
docker exec mindsdb-streamlined df -h
```

### Debug Mode

```bash
# Run in debug mode
docker run -e MINDSDB_LOG_LEVEL=DEBUG mindsdb-streamlined

# Interactive debugging
docker run -it --entrypoint /bin/bash mindsdb-streamlined
```

## Backup and Recovery

### Data Backup

```bash
# Backup data volume
docker run --rm -v mindsdb_data:/data -v $(pwd):/backup alpine tar czf /backup/mindsdb-backup.tar.gz -C /data .

# Restore data volume
docker run --rm -v mindsdb_data:/data -v $(pwd):/backup alpine tar xzf /backup/mindsdb-backup.tar.gz -C /data
```

### Configuration Backup

```bash
# Backup configuration
docker cp mindsdb-streamlined:/home/mindsdb/config.json ./config-backup.json

# Restore configuration
docker cp ./config-backup.json mindsdb-streamlined:/home/mindsdb/config.json
```

## Scaling and High Availability

### Load Balancing

```yaml
# nginx.conf
upstream mindsdb_backend {
    server mindsdb-1:47334;
    server mindsdb-2:47334;
    server mindsdb-3:47334;
}

server {
    listen 80;
    location / {
        proxy_pass http://mindsdb_backend;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### Database Clustering

For production deployments, consider:
- External PostgreSQL cluster for metadata
- Redis cluster for caching
- Shared storage for model artifacts

## Migration from Full MindsDB

### Data Migration

```bash
# Export data from full MindsDB
docker exec full-mindsdb python -m mindsdb.utilities.export_data

# Import to streamlined version
docker exec mindsdb-streamlined python -m mindsdb.utilities.import_data
```

### Configuration Migration

```bash
# Convert full config to streamlined
python scripts/convert-config.py full-config.json config-streamlined.json
```

## Support and Resources

### Documentation
- [MindsDB Documentation](https://docs.mindsdb.com)
- [Docker Documentation](https://docs.docker.com)
- [Kubernetes Documentation](https://kubernetes.io/docs)

### Community
- [MindsDB Slack](https://mindsdb.com/joincommunity)
- [GitHub Issues](https://github.com/TradieMate/mindsdb/issues)
- [Stack Overflow](https://stackoverflow.com/questions/tagged/mindsdb)

### Professional Support
For enterprise deployments and professional support, contact the MindsDB team.

---

*This guide covers the streamlined MindsDB deployment with 90% handler reduction for optimized production environments.*