import schedule
import time
import threading
from datetime import datetime
import logging
from flask import current_app
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AutoReportService:
    def __init__(self):
        self.is_running = False
        self.current_report = None
        self.report_queue = []
        self.report_order = [
            ('stats', '10d'),
            ('fraud', '10d'),
            ('stats', 'mtd'),
            ('fraud', 'mtd'),
            ('stats', 'lastmonth'),
            ('fraud', 'lastmonth'),
            ('stats', '30d'),
            ('fraud', '30d')
        ]
        self.period_map = {
            '10d': 'last10',
            'mtd': 'mtd',
            'lastmonth': 'lastmonth',
            '30d': 'last30'
        }

    def start(self):
        """Start the auto report service"""
        if not self.is_running:
            self.is_running = True
            self.schedule_reports()
            threading.Thread(target=self._run_scheduler, daemon=True).start()
            logger.info("Auto report service started")

    def stop(self):
        """Stop the auto report service"""
        self.is_running = False
        logger.info("Auto report service stopped")

    def _run_scheduler(self):
        """Run the scheduler in a loop"""
        while self.is_running:
            schedule.run_pending()
            time.sleep(1)

    def schedule_reports(self):
        """Schedule reports to run every 6 hours"""
        schedule.every(6).hours.do(self.run_all_reports)
        logger.info("Reports scheduled to run every 6 hours")

    def run_all_reports(self):
        """Run all reports in sequence"""
        logger.info("Starting automated report generation cycle")
        self.report_queue = self.report_order.copy()
        self._process_next_report()

    def _process_next_report(self):
        """Process the next report in the queue"""
        if not self.report_queue:
            logger.info("All reports completed")
            return

        report_type, range_key = self.report_queue[0]
        self.current_report = (report_type, range_key)
        
        try:
            with current_app.app_context():
                if report_type == 'stats':
                    self._run_stats_report(range_key)
                else:  # fraud
                    self._run_fraud_report(range_key)
        except Exception as e:
            logger.error(f"Error running {report_type} report for {range_key}: {str(e)}")
            self._handle_report_completion()

    def _run_stats_report(self, range_key):
        """Run a stats report"""
        logger.info(f"Running stats report for {range_key}")
        # Get apps from cache or fetch them
        apps = self._get_apps()
        if not apps:
            logger.error("No apps available for stats report")
            self._handle_report_completion()
            return

        # Start the report generation
        try:
            response = current_app.test_client().post('/start-report', json={
                'apps': apps,
                'period': self.period_map[range_key],
                'selected_events': {}
            })
            data = json.loads(response.data)
            
            if data.get('status') == 'processing':
                self._poll_stats_report_status(data['job_id'], range_key)
            else:
                logger.error(f"Failed to start stats report: {data}")
                self._handle_report_completion()
        except Exception as e:
            logger.error(f"Error starting stats report: {str(e)}")
            self._handle_report_completion()

    def _run_fraud_report(self, range_key):
        """Run a fraud report"""
        logger.info(f"Running fraud report for {range_key}")
        # Get apps from cache or fetch them
        apps = self._get_apps()
        if not apps:
            logger.error("No apps available for fraud report")
            self._handle_report_completion()
            return

        try:
            response = current_app.test_client().post('/get_fraud', json={
                'apps': apps,
                'period': self.period_map[range_key]
            })
            if response.status_code == 200:
                logger.info(f"Fraud report completed for {range_key}")
            else:
                logger.error(f"Failed to run fraud report: {response.status_code}")
        except Exception as e:
            logger.error(f"Error running fraud report: {str(e)}")
        
        self._handle_report_completion()

    def _poll_stats_report_status(self, job_id, range_key):
        """Poll the status of a stats report"""
        try:
            response = current_app.test_client().get(f'/report-status/{job_id}')
            data = json.loads(response.data)
            
            if data['status'] == 'completed':
                logger.info(f"Stats report completed for {range_key}")
                self._handle_report_completion()
            elif data['status'] == 'failed':
                logger.error(f"Stats report failed for {range_key}")
                self._handle_report_completion()
            else:
                # Still processing, poll again after 5 seconds
                threading.Timer(5.0, lambda: self._poll_stats_report_status(job_id, range_key)).start()
        except Exception as e:
            logger.error(f"Error polling stats report status: {str(e)}")
            self._handle_report_completion()

    def _handle_report_completion(self):
        """Handle the completion of a report"""
        if self.report_queue:
            self.report_queue.pop(0)
            self.current_report = None
            # Wait 30 seconds before starting the next report
            threading.Timer(30.0, self._process_next_report).start()

    def _get_apps(self):
        """Get the list of apps from cache or fetch them"""
        try:
            response = current_app.test_client().get('/get_apps')
            data = json.loads(response.data)
            return data.get('apps', [])
        except Exception as e:
            logger.error(f"Error getting apps: {str(e)}")
            return []

# Create a global instance of the service
auto_report_service = AutoReportService() 