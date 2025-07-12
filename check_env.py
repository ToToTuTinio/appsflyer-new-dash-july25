#!/usr/bin/env python3
"""
Environment File Checker for AppsFlyer Dashboard
This script checks if your .env.local file is properly configured for deployment.
"""

import os
from pathlib import Path
import sys

def check_env_file():
    """Check if .env.local file exists and has required credentials"""
    env_path = Path('.env.local')
    
    print("🔍 Checking .env.local file for deployment...")
    print(f"📂 Looking for: {env_path.absolute()}")
    
    if not env_path.exists():
        print("❌ .env.local file not found!")
        print("💡 Solution: Create .env.local file with your credentials")
        print("   You can update credentials in the Profile page of your dashboard")
        return False
    
    print("✅ .env.local file exists")
    
    # Check file permissions
    try:
        stat_info = env_path.stat()
        permissions = oct(stat_info.st_mode)[-3:]
        print(f"📄 File permissions: {permissions}")
        print(f"📊 File size: {stat_info.st_size} bytes")
    except Exception as e:
        print(f"⚠️ Could not check file permissions: {e}")
    
    # Read and check credentials
    required_vars = ['EMAIL', 'PASSWORD', 'APPSFLYER_API_KEY']
    found_vars = {}
    
    try:
        with open(env_path, 'r') as f:
            content = f.read()
            lines = [line.strip() for line in content.split('\n') if line.strip() and not line.startswith('#')]
            
            print(f"📝 Configuration lines found: {len(lines)}")
            
            for line in lines:
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    
                    if key in required_vars:
                        found_vars[key] = bool(value)
                        status = "✅" if value else "❌"
                        masked_value = "***" if value else "EMPTY"
                        print(f"   {status} {key}: {masked_value}")
    
    except Exception as e:
        print(f"❌ Error reading .env.local file: {e}")
        return False
    
    # Check if all required variables are present
    missing_vars = [var for var in required_vars if var not in found_vars or not found_vars[var]]
    
    if missing_vars:
        print(f"\n❌ Missing required credentials: {', '.join(missing_vars)}")
        print("💡 Solution: Update missing credentials in the Profile page")
        return False
    
    print("\n✅ All required credentials are configured!")
    print("🚀 Your .env.local file is ready for deployment!")
    
    print("\n📋 Deployment Instructions:")
    print("1. Copy .env.local to your server in the same directory as your app")
    print("2. Make sure the file has appropriate permissions (readable by your app)")
    print("3. Restart your application to load the new environment variables")
    
    return True

def main():
    """Main function"""
    print("=" * 60)
    print("🔧 AppsFlyer Dashboard - Environment File Checker")
    print("=" * 60)
    
    success = check_env_file()
    
    print("\n" + "=" * 60)
    if success:
        print("✅ Environment check passed - Ready for deployment!")
        sys.exit(0)
    else:
        print("❌ Environment check failed - Fix issues before deployment")
        sys.exit(1)

if __name__ == "__main__":
    main() 