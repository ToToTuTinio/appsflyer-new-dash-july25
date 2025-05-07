# AppsFlyer Dashboard

A comprehensive dashboard for monitoring and analyzing AppsFlyer data, including fraud detection, performance metrics, and event tracking.

## Features

- Real-time AppsFlyer data integration
- Fraud detection and analysis
- Performance metrics tracking
- Event monitoring
- Multi-app support
- Interactive charts and visualizations
- User authentication system

## Prerequisites

- Python 3.7 or higher
- Chrome browser
- ChromeDriver (matching Chrome version)
- SQLite3

## Installation

1. Clone the repository:
```bash
git clone [your-repo-url]
cd appsflyer-dash-2025
```

2. Install Python dependencies:
```bash
   pip install -r backend/requirements.txt
   ```

3. Set up environment variables:
Create a `.env.local` file with the following variables:
   ```
EMAIL=your_appsflyer_email
PASSWORD=your_appsflyer_password
APPSFLYER_API_KEY=your_api_key
```

## Running the Application

1. Start the backend server:
```bash
cd backend
python app.py
   ```

2. Access the dashboard at `http://localhost:5000`

## Project Structure

```
appsflyer-dash-2025/
├── backend/
│   ├── app.py              # Main Flask application
│   ├── requirements.txt    # Python dependencies
│   └── templates/          # HTML templates
├── frontend/              # Frontend assets
├── .env.local            # Environment variables
└── README.md             # This file
```

## Security

- All sensitive data is stored in environment variables
- User authentication required for all dashboard access
- API keys and credentials are encrypted

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support, please contact [your-email@example.com]
