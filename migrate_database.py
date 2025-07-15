#!/usr/bin/env python3
"""
Database Migration Script for Railway Persistent Storage
=========================================================

This script helps migrate your existing SQLite database to the new persistent storage location.
Run this script BEFORE deploying to Railway with the new persistent storage configuration.

Usage:
    python migrate_database.py [--backup] [--source path] [--destination path]

Options:
    --backup           Create a backup of the existing database before migration
    --source PATH      Source database path (default: event_selections.db)
    --destination PATH Destination database path (default: data/event_selections.db)
"""

import os
import sqlite3
import shutil
import argparse
from datetime import datetime

def create_backup(source_path):
    """Create a backup of the existing database"""
    if not os.path.exists(source_path):
        print(f"❌ Source database not found: {source_path}")
        return False
    
    backup_path = f"{source_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    try:
        shutil.copy2(source_path, backup_path)
        print(f"✅ Backup created: {backup_path}")
        return True
    except Exception as e:
        print(f"❌ Failed to create backup: {e}")
        return False

def migrate_apps_data(source_path, destination_path):
    """Migrate only apps-related data to the new database location"""
    if not os.path.exists(source_path):
        print(f"❌ Source database not found: {source_path}")
        return False
    
    # Ensure destination directory exists
    dest_dir = os.path.dirname(destination_path)
    if dest_dir and not os.path.exists(dest_dir):
        os.makedirs(dest_dir, exist_ok=True)
        print(f"📁 Created destination directory: {dest_dir}")
    
    try:
        # Connect to source database
        source_conn = sqlite3.connect(source_path)
        source_cursor = source_conn.cursor()
        
        # Connect to destination database
        dest_conn = sqlite3.connect(destination_path)
        dest_cursor = dest_conn.cursor()
        
        # Create tables (using the same schema as in app.py)
        print("🔧 Creating database schema...")
        dest_cursor.execute('''CREATE TABLE IF NOT EXISTS apps_cache (
            id INTEGER PRIMARY KEY,
            data TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        
        dest_cursor.execute('''CREATE TABLE IF NOT EXISTS manual_apps (
            app_id TEXT PRIMARY KEY,
            app_name TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            event1 TEXT,
            event2 TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        
        dest_cursor.execute('''CREATE TABLE IF NOT EXISTS app_event_selections (
            app_id TEXT PRIMARY KEY,
            event1 TEXT,
            event2 TEXT,
            is_active INTEGER DEFAULT 0
        )''')
        
        # Migrate apps_cache data
        print("📦 Migrating apps cache data...")
        source_cursor.execute("SELECT * FROM apps_cache")
        apps_cache_data = source_cursor.fetchall()
        if apps_cache_data:
            dest_cursor.executemany("INSERT OR REPLACE INTO apps_cache (id, data, updated_at) VALUES (?, ?, ?)", apps_cache_data)
            print(f"✅ Migrated {len(apps_cache_data)} apps cache entries")
        else:
            print("ℹ️  No apps cache data to migrate")
        
        # Migrate manual_apps data
        print("📱 Migrating manual apps data...")
        try:
            source_cursor.execute("SELECT * FROM manual_apps")
            manual_apps_data = source_cursor.fetchall()
            if manual_apps_data:
                dest_cursor.executemany("INSERT OR REPLACE INTO manual_apps (app_id, app_name, status, event1, event2, is_active, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", manual_apps_data)
                print(f"✅ Migrated {len(manual_apps_data)} manual apps")
            else:
                print("ℹ️  No manual apps data to migrate")
        except sqlite3.OperationalError as e:
            if "no such table: manual_apps" in str(e):
                print("ℹ️  No manual_apps table found - this is normal for new installations")
            else:
                raise e
        
        # Migrate app_event_selections data
        print("⚙️  Migrating app event selections...")
        try:
            source_cursor.execute("SELECT * FROM app_event_selections")
            event_selections_data = source_cursor.fetchall()
            if event_selections_data:
                dest_cursor.executemany("INSERT OR REPLACE INTO app_event_selections (app_id, event1, event2, is_active) VALUES (?, ?, ?, ?)", event_selections_data)
                print(f"✅ Migrated {len(event_selections_data)} event selections")
            else:
                print("ℹ️  No event selections data to migrate")
        except sqlite3.OperationalError as e:
            if "no such table: app_event_selections" in str(e):
                print("ℹ️  No app_event_selections table found - this is normal for new installations")
            else:
                raise e
        
        # Commit and close connections
        dest_conn.commit()
        source_conn.close()
        dest_conn.close()
        
        print(f"🎉 Migration completed successfully!")
        print(f"📍 Database migrated to: {destination_path}")
        
        return True
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Migrate SQLite database to persistent storage")
    parser.add_argument("--backup", action="store_true", help="Create backup before migration")
    parser.add_argument("--source", default="event_selections.db", help="Source database path")
    parser.add_argument("--destination", default="data/event_selections.db", help="Destination database path")
    
    args = parser.parse_args()
    
    print("🚀 AppsFlyer Database Migration Tool")
    print("=" * 40)
    print(f"Source: {args.source}")
    print(f"Destination: {args.destination}")
    print()
    
    # Create backup if requested
    if args.backup:
        print("📋 Creating backup...")
        if not create_backup(args.source):
            print("❌ Migration cancelled due to backup failure")
            return
        print()
    
    # Perform migration
    print("🔄 Starting migration...")
    if migrate_apps_data(args.source, args.destination):
        print()
        print("✅ Migration completed successfully!")
        print()
        print("📝 Next steps:")
        print("1. Deploy to Railway - your apps data will now persist across deployments")
        print("2. Your old database file can be safely removed after confirming everything works")
        print("3. The new persistent database is located at:", args.destination)
    else:
        print("❌ Migration failed!")

if __name__ == "__main__":
    main() 