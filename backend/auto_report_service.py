import time
from datetime import datetime
import threading
from report_utils import process_report_async, get_fraud_data
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AutoReportService:
    def __init__(self):
        self.is_running = False
        self.thread = None
        self.last_run = None
        self.periods = [
            ('last10', 'last10'),  # Stats and Fraud for Last 10 Days
            ('mtd', 'mtd'),        # Stats and Fraud for Month to Date
            ('lastmonth', 'lastmonth'),  # Stats and Fraud for Last Month
            ('last30', 'last30')   # Stats and Fraud for Last 30 Days
        ]

    def start(self):
        if not self.is_running:
            self.is_running = True
            self.thread = threading.Thread(target=self._run_service)
            self.thread.daemon = True
            self.thread.start()
            logger.info("Auto Report Service started")

    def stop(self):
        self.is_running = False
        if self.thread:
            self.thread.join()
            logger.info("Auto Report Service stopped")

    def _run_service(self):
        while self.is_running:
            try:
                self._run_all_reports()
                # Wait for 6 hours before next run
                time.sleep(6 * 60 * 60)
            except Exception as e:
                logger.error(f"Error in auto report service: {str(e)}")
                time.sleep(60)  # Wait a minute before retrying

    def _run_all_reports(self):
        logger.info("Starting automated report generation")
        self.last_run = datetime.now()

        for stats_period, fraud_period in self.periods:
            try:
                # Run Stats Report
                logger.info(f"Running Stats report for period: {stats_period}")
                process_report_async([], stats_period, {})
                time.sleep(1800)  # Wait 30 minutes

                # Run Fraud Report
                logger.info(f"Running Fraud report for period: {fraud_period}")
                get_fraud_data([], fraud_period)
                time.sleep(1800)  # Wait 30 minutes

            except Exception as e:
                logger.error(f"Error running reports for period {stats_period}: {str(e)}")
                continue

        logger.info("Completed automated report generation")

    def run_now(self):
        """Manually trigger report generation"""
        if not self.is_running:
            self.start()
        else:
            self._run_all_reports()

# Create a global instance
auto_report_service = AutoReportService() 