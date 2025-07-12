web: cd backend && gunicorn app:app --bind 0.0.0.0:$PORT --workers 4 --timeout 300 --keep-alive 5
worker: cd backend && python worker.py 