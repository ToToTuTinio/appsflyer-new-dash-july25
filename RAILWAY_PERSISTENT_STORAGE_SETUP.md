# Railway Persistent Storage Setup Guide 🚀

This guide will help you set up persistent storage for your AppsFlyer Dashboard on Railway, ensuring your Apps Cache and Manual Apps data survives deployments.

## 🔧 What This Solves

Previously, each Railway deployment would wipe your SQLite database, losing:
- ✅ **Apps Cache** (synced AppsFlyer apps)
- ✅ **Manual Apps** (manually added apps)
- ✅ **App Event Selections** (event configurations)

After following this guide, your apps data will persist across all deployments! 🎉

## 📋 Prerequisites

- Railway account with your project deployed
- Access to your project's Railway dashboard
- Local copy of your current database (if you have existing data)

## 🛠️ Setup Steps

### Step 1: Run the Migration Script (If You Have Existing Data)

If you already have apps data you want to keep, run the migration script **before** deploying:

```bash
# Create a backup and migrate your existing data
python migrate_database.py --backup

# Or migrate to a custom location
python migrate_database.py --backup --source event_selections.db --destination data/event_selections.db
```

The script will:
- 📋 Create a backup of your existing database
- 📦 Migrate all apps-related data to the new persistent location
- 🔧 Create the proper database schema

### Step 2: Deploy to Railway

The code changes are already configured for persistent storage:

1. **Database Path**: Automatically uses `/data/event_selections.db` in Railway
2. **Volume Mount**: Railway will mount persistent storage at `/data`
3. **Fallback**: Uses local path in development

Simply deploy your updated code to Railway - no additional configuration needed!

### Step 3: Verify Persistent Storage

After deployment:

1. **Check Database Location**: The app will log the database path during startup
2. **Add Test Data**: Add a manual app through the UI
3. **Redeploy**: Deploy again and verify your data is still there
4. **Success!** 🎉 Your data now persists across deployments

## 📁 File Structure

```
your-project/
├── event_selections.db          # Local development database
├── data/
│   └── event_selections.db      # Persistent database (Railway)
├── migrate_database.py          # Migration script
└── railway.json                 # Railway configuration with volume
```

## 🔍 How It Works

### Database Path Logic

```python
# Automatically chooses the right path
DB_PATH = os.getenv('DB_PATH', '/data/event_selections.db' if os.getenv('RAILWAY_ENVIRONMENT') else 'event_selections.db')
```

- **Railway**: Uses `/data/event_selections.db` (persistent volume)
- **Local**: Uses `event_selections.db` (local development)
- **Custom**: Set `DB_PATH` environment variable to override

### Railway Volume Configuration

```json
{
  "volumes": [
    {
      "name": "appsflyer-data",
      "mountPath": "/data"
    }
  ]
}
```

This mounts a persistent volume at `/data` that survives deployments.

## 🚨 Important Notes

### Data Preserved
- ✅ **Apps Cache** (synced AppsFlyer apps)
- ✅ **Manual Apps** (manually added apps)
- ✅ **App Event Selections** (event configurations)

### Data NOT Preserved (Intentionally)
- ❌ **Stats Cache** (regenerated from AppsFlyer)
- ❌ **Fraud Cache** (regenerated from AppsFlyer)
- ❌ **Event Cache** (regenerated from AppsFlyer)
- ❌ **Raw AppsFlyer Data** (regenerated from AppsFlyer)

This is by design - only your apps configuration persists, while report data gets refreshed.

## 🐛 Troubleshooting

### Issue: Database Permission Errors
```bash
# Check if /data directory exists and is writable
ls -la /data/
```

### Issue: Migration Script Errors
```bash
# Run with verbose output
python migrate_database.py --backup --source event_selections.db --destination data/event_selections.db
```

### Issue: Apps Data Missing After Deployment
1. Check Railway logs for database path
2. Verify volume is mounted correctly
3. Re-run migration script if needed

## 🎯 Migration Script Options

```bash
# Basic migration with backup
python migrate_database.py --backup

# Custom source/destination paths
python migrate_database.py --backup --source old_db.db --destination new_location/db.db

# Migration without backup (not recommended)
python migrate_database.py --source event_selections.db --destination data/event_selections.db
```

## 📊 What Gets Migrated

The migration script preserves:

| Table | Description | Records |
|-------|-------------|---------|
| `apps_cache` | Synced AppsFlyer apps | All cached apps |
| `manual_apps` | Manually added apps | All manual apps |
| `app_event_selections` | Event configurations | All event settings |

## 🔄 Deployment Workflow

1. **Before First Deployment**: Run migration script
2. **Deploy to Railway**: Your apps data is now persistent
3. **Future Deployments**: Just deploy - data persists automatically!

## ✅ Success Verification

After setup, you should see:

1. **Railway Logs**: Database path shows `/data/event_selections.db`
2. **Apps Page**: All your apps are still there after deployment
3. **Manual Apps**: Any manual apps you added persist
4. **Sync History**: Apps cache from AppsFlyer is preserved

---

## 🎉 Congratulations!

Your AppsFlyer Dashboard now has persistent storage! Your apps data will survive all future Railway deployments, making your dashboard much more reliable and user-friendly.

For any issues, check the Railway logs or contact support with the specific error messages. 