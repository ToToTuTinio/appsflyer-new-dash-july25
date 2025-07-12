#!/usr/bin/env python3
import os
import subprocess
import sys

def main():
    # Get port from environment variable, default to 8080
    port = os.getenv('PORT', '8080')
    
    # Build gunicorn command
    cmd = [
        'gunicorn',
        '-w', '4',
        '-b', f'0.0.0.0:{port}',
        'backend.app:app'
    ]
    
    print(f"Starting gunicorn on port {port}")
    print(f"Command: {' '.join(cmd)}")
    
    # Execute gunicorn
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error starting gunicorn: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 