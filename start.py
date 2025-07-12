#!/usr/bin/env python3
import os
import sys
import subprocess

def main():
    print("=== Railway Startup Debug ===")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Python executable: {sys.executable}")
    print(f"Python version: {sys.version}")
    print(f"PORT environment variable: {os.getenv('PORT', 'NOT SET')}")
    print(f"Files in current directory: {os.listdir('.')}")
    
    # Check if backend directory exists
    if os.path.exists('backend'):
        print(f"Backend directory exists: {os.listdir('backend')}")
    else:
        print("Backend directory NOT found!")
    
    # Get port from environment variable, default to 8080
    port = os.getenv('PORT', '8080')
    
    # Try running the Flask app directly first
    print(f"Attempting to start Flask app on port {port}")
    
    # Set PYTHONPATH
    os.environ['PYTHONPATH'] = '/app'
    
    try:
        # Try to import the Flask app first
        sys.path.insert(0, '/app')
        print("Testing import of backend.app...")
        import backend.app
        print("Successfully imported backend.app!")
        
        # Now try to start with gunicorn
        cmd = [
            'gunicorn',
            '-w', '1',  # Start with just 1 worker for debugging
            '-b', f'0.0.0.0:{port}',
            '--timeout', '120',
            '--access-logfile', '-',
            '--error-logfile', '-',
            'backend.app:app'
        ]
        
        print(f"Starting gunicorn with command: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
        
    except ImportError as e:
        print(f"Import error: {e}")
        print("Trying to run Flask app directly...")
        
        # Try running Flask directly as fallback
        try:
            os.chdir('/app')
            os.environ['FLASK_APP'] = 'backend.app:app'
            subprocess.run(['python', '-m', 'flask', 'run', '--host=0.0.0.0', f'--port={port}'], check=True)
        except Exception as e2:
            print(f"Flask direct run also failed: {e2}")
            sys.exit(1)
            
    except subprocess.CalledProcessError as e:
        print(f"Gunicorn failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 