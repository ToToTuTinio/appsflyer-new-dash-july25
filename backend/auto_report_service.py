import schedule
import time
import threading
from datetime import datetime
import logging
from app import app, generate_stats_report, generate_fraud_report

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_reports_sequentially():
    """
    Run reports one by one in sequence
    """
    try:
        logger.info("Starting sequential report generation...")
        
        # Run Stats Report
        logger.info("Generating Stats Report...")
        with app.app_context():
            generate_stats_report()
        logger.info("Stats Report completed")
        
        # Wait for 5 minutes before running the next report
        time.sleep(300)
        
        # Run Fraud Report
        logger.info("Generating Fraud Report...")
        with app.app_context():
            generate_fraud_report()
        logger.info("Fraud Report completed")
        
        logger.info("All reports completed successfully")
        
    except Exception as e:
        logger.error(f"Error in report generation: {str(e)}")

def run_scheduled_reports():
    """
    Schedule reports to run every 6 hours
    """
    # Schedule the reports to run every 6 hours
    schedule.every(6).hours.do(run_reports_sequentially)
    
    # Run immediately on startup
    run_reports_sequentially()
    
    # Keep the scheduler running
    while True:
        schedule.run_pending()
        time.sleep(60)

def start_report_service():
    """
    Start the report service in a separate thread
    """
    report_thread = threading.Thread(target=run_scheduled_reports, daemon=True)
    report_thread.start()
    logger.info("Report service started successfully")
    return report_thread

if __name__ == "__main__":
    start_report_service() 