# 🚀 AppsFlyer Dashboard - Credential Management & Deployment Guide

## Overview
The AppsFlyer Dashboard now has a robust credential management system that saves all profile credentials directly to a `.env.local` file in real-time. This guide explains how it works and how to deploy it to a server.

## ✅ How It Works

### Local Development
1. **Profile Page**: Update credentials (API Key, Email, Password) in the Profile tab
2. **Real-time Saving**: Changes are immediately saved to `.env.local` file
3. **In-memory Updates**: Environment variables are updated in memory without requiring restart
4. **Backup System**: Automatic backup creation before changes (.env.backup)
5. **Validation**: Input validation with user-friendly error messages
6. **User Feedback**: Beautiful toast notifications for success/error states

### Enhanced Features
- ✅ **Input Validation**: Email format, API key length, password strength
- ✅ **Error Handling**: Comprehensive error messages and recovery
- ✅ **File Permissions**: Automatic directory creation and permission checks
- ✅ **Backup & Recovery**: Automatic backup creation and restoration on errors
- ✅ **User Feedback**: Real-time loading states and toast notifications
- ✅ **Deployment Ready**: Built-in deployment readiness checker

## 🔧 Deployment Instructions

### Step 1: Check Environment Status
Before deployment, verify your credentials are properly configured:

#### Option A: Use the Environment Checker Script
```bash
python3 check_env.py
```

#### Option B: Use Browser Console
1. Open your dashboard in browser
2. Open Developer Tools (F12)
3. Go to Console tab
4. Run: `checkEnvStatus()`

### Step 2: Deploy to Server
1. **Copy .env.local to your server** in the same directory as your app
2. **Set appropriate permissions**:
   ```bash
   chmod 644 .env.local
   ```
3. **Restart your application** to load the new environment variables
4. **Verify deployment** by checking that all credentials are loaded

### Step 3: Verify Deployment
After deployment, check that everything works:
- Visit your server's dashboard
- Go to Profile tab
- Verify all credentials are displayed correctly
- Test updating a credential to ensure saving works

## 📋 File Structure

```
your-project/
├── .env.local          # 🔑 Main credentials file (copy to server)
├── .env.backup         # 🔄 Automatic backup (optional)
├── check_env.py        # 🔍 Environment checker script
├── backend/
│   ├── app.py          # 🌐 Enhanced credential endpoints
│   └── templates/
│       └── dashboard.html  # 💻 Profile page with credential management
└── DEPLOYMENT_GUIDE.md # 📖 This guide
```

## 🔒 Security Considerations

1. **File Permissions**: Ensure .env.local is readable only by your application
2. **Server Security**: Keep .env.local out of version control (already in .gitignore)
3. **Backup Files**: .env.backup files are created automatically but are optional
4. **API Keys**: All credentials are masked in the UI for security

## 🛠️ Troubleshooting

### Common Issues

#### 1. "Permission denied writing to .env.local"
**Solution**: Check file permissions and ensure the directory is writable
```bash
chmod 755 .
chmod 644 .env.local
```

#### 2. "Credentials not loading after deployment"
**Solution**: Verify .env.local exists in the correct location and restart the app
```bash
# Check if file exists
ls -la .env.local

# Restart your application
# (depends on your deployment method)
```

#### 3. "Invalid email format" or "API key too short"
**Solution**: The system validates inputs. Ensure:
- Email contains @ symbol
- API key is at least 10 characters
- Password is at least 4 characters

### Debug Commands

#### Check Environment Status (Browser Console)
```javascript
// Check if .env.local is properly configured
checkEnvStatus()

// Verify database persistence
verifyDatabasePersistence()
```

#### Check Environment Status (Command Line)
```bash
# Run the environment checker
python3 check_env.py

# Check if file exists
ls -la .env.local

# Check file contents (masked)
cat .env.local | grep -E "(EMAIL|PASSWORD|APPSFLYER_API_KEY)"
```

## 🎯 API Endpoints

### Profile Management
- `POST /update-credential` - Update credentials (API Key, Email, Password)
- `GET /profile-info` - Get current profile information  
- `GET /env-status` - Check environment file status (debug)

### Enhanced Error Handling
- Input validation with specific error messages
- File permission checks
- Automatic backup and recovery
- Deployment readiness verification

## 💡 Best Practices

1. **Always test locally** before deploying
2. **Use the environment checker** before deployment
3. **Keep .env.local secure** and out of version control
4. **Monitor the console** for deployment status
5. **Backup your .env.local** before major changes (done automatically)

## 🚀 Deployment Checklist

- [ ] ✅ Credentials configured in Profile page
- [ ] ✅ `.env.local` file exists and has correct permissions
- [ ] ✅ Environment checker script passes
- [ ] ✅ All required credentials present (EMAIL, PASSWORD, APPSFLYER_API_KEY)
- [ ] ✅ `.env.local` copied to server
- [ ] ✅ Server application restarted
- [ ] ✅ Profile page shows credentials correctly on server
- [ ] ✅ Credential updates work on server

## 📞 Support

If you encounter issues:
1. Run `python3 check_env.py` to diagnose problems
2. Check browser console for errors
3. Verify file permissions and paths
4. Ensure all required credentials are set

---

**✅ Your AppsFlyer Dashboard is now ready for deployment with robust credential management!** 