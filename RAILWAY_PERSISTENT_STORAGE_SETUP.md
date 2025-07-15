# Railway Persistent Storage Setup

## Overview
This document explains how to configure Railway persistent storage for the AppsFlyer Dashboard to prevent data loss during deployments.

## Problem
By default, Railway containers are ephemeral - they get destroyed and recreated on each deployment, causing all database data to be lost.

## Solution
Configure Railway persistent volumes to store the SQLite database on persistent storage that survives deployments.

## Configuration Steps

### 1. Railway Volume Configuration
The `railway.json` file contains the persistent volume configuration:

```json
{
  "$schema": "https://railway.app/railway.schema.json",
  "deploy": {
    "restartPolicyType": "ON_FAILURE",
    "restartPolicyMaxRetries": 3
  },
  "volumes": [
    {
      "name": "appsflyer-data",
      "mountPath": "/data"
    }
  ]
}
```

### 2. Database Path Configuration
The application automatically detects Railway environment and uses the persistent volume:

```python
# More reliable Railway detection - Railway sets multiple environment variables
def is_railway_environment():
    railway_vars = [
        'RAILWAY_ENVIRONMENT',
        'RAILWAY_SERVICE_NAME', 
        'RAILWAY_PROJECT_ID',
        'RAILWAY_DEPLOYMENT_ID',
        'RAILWAY_REPLICA_ID'
    ]
    return any(os.getenv(var) for var in railway_vars)

DB_PATH = os.getenv('DB_PATH', '/data/event_selections.db' if is_railway_environment() else 'event_selections.db')
```

### 3. Debug Endpoint
A debug endpoint is available to check the database status and persistent storage:

**URL**: `/api/debug/db-status`

**Response includes**:
- Database path and existence
- Railway environment detection
- Data directory status
- Table existence and row counts
- Railway environment variables

### 4. Migration Script
Use the `migrate_database.py` script to transfer existing data:

```python
python migrate_database.py
```

## Deployment Process

### First Deployment
1. **Expected**: 0 apps after first deployment (this is normal)
2. **Reason**: New persistent volume is empty
3. **Solution**: Sync apps from AppsFlyer after first deployment

### Subsequent Deployments
1. **Expected**: Apps data persists between deployments
2. **Verification**: Check `/api/debug/db-status` endpoint
3. **Troubleshooting**: Use debug logs in Railway deployment logs

## Verification

### After Deployment
1. **Check debug endpoint**: `https://your-app.railway.app/api/debug/db-status`
2. **Verify Railway detection**: Look for `is_railway_environment: true`
3. **Check database path**: Should be `/data/event_selections.db`
4. **Verify data directory**: Should exist with proper contents

### Debug Logs
Check Railway deployment logs for:
```
🔍 Railway Environment Detection:
  RAILWAY_ENVIRONMENT: production
  RAILWAY_SERVICE_NAME: your-service
  RAILWAY_PROJECT_ID: your-project-id
  Is Railway Environment: true
  Using DB_PATH: /data/event_selections.db
  Database file exists: true
  /data directory exists: true
```

## Troubleshooting

### Apps Data Still Disappearing
1. **Check debug endpoint**: Verify Railway detection is working
2. **Check Railway logs**: Look for database path information
3. **Verify volume mount**: Ensure `/data` directory exists
4. **Check environment variables**: Verify Railway variables are set

### Database Not Found
1. **Check Railway environment detection**: May need to manually set `DB_PATH=/data/event_selections.db`
2. **Verify volume configuration**: Ensure `railway.json` is properly configured
3. **Check Railway dashboard**: Verify volume is mounted

### Migration Issues
1. **Run migration script**: Transfer existing data to persistent storage
2. **Check source database**: Verify local database exists
3. **Manual migration**: Use Railway CLI to copy database file

## Railway Environment Variables
Railway automatically sets these variables:
- `RAILWAY_ENVIRONMENT`
- `RAILWAY_SERVICE_NAME`
- `RAILWAY_PROJECT_ID`
- `RAILWAY_DEPLOYMENT_ID`
- `RAILWAY_REPLICA_ID`

## Notes
- **Volume persistence**: Data in `/data` survives deployments
- **Container ephemeral**: Data in container filesystem is lost
- **First deployment**: Will show 0 apps (expected)
- **Subsequent deployments**: Apps data should persist
- **Debug endpoint**: Use to verify configuration 