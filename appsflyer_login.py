from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import os
from dotenv import load_dotenv
import time
from selenium.webdriver.common.keys import Keys

# Load environment variables
load_dotenv()

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-notifications")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def login_to_appsflyer():
    driver = setup_driver()
    try:
        # Navigate to AppsFlyer login page
        driver.get("https://hq1.appsflyer.com/auth/login")
        
        # Wait for the login form to be visible
        wait = WebDriverWait(driver, 10)
        email_field = wait.until(EC.presence_of_element_located((By.ID, "email")))
        password_field = wait.until(EC.presence_of_element_located((By.ID, "password")))
        
        # Get credentials from environment variables
        email = os.getenv("EMAIL")
        password = os.getenv("PASSWORD")
        
        if not email or not password:
            raise ValueError("Email and password must be set in .env.local file")
        
        # Enter credentials
        email_field.send_keys(email)
        password_field.send_keys(password)
        
        # Click login button
        login_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']")))
        login_button.click()
        
        # Wait for successful login (you might need to adjust this based on the actual page behavior)
        time.sleep(5)
        
        # Check if login was successful
        if "dashboard" in driver.current_url:
            print("Successfully logged in to AppsFlyer!")
        else:
            print("Login might have failed. Please check the credentials and try again.")
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Keep the browser open for 30 seconds to allow manual verification
        time.sleep(30)
        driver.quit()

def get_apps_with_installs(email, password, max_retries=7):
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})

    retries = 0
    while retries < max_retries:
        try:
            print("Opening login page...")
            driver.get("https://hq1.appsflyer.com/auth/login")
            time.sleep(5)

            print("Waiting for email field...")
            try:
                email_field = WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.ID, "user-email"))
                )
                print("Email field found!")
            except Exception as e:
                print(f"Error finding email field: {str(e)}")
                print("Current page source:", driver.page_source[:500])
                raise

            print("Entering email...")
            email_field.clear()
            email_field.send_keys(email)
            time.sleep(1)

            print("Waiting for password field...")
            try:
                password_field = WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.ID, "password-field"))
                )
                print("Password field found!")
            except Exception as e:
                print(f"Error finding password field: {str(e)}")
                print("Current page source:", driver.page_source[:500])
                raise

            print("Entering password...")
            password_field.clear()
            password_field.send_keys(password)
            time.sleep(1)
            
            print("Looking for login button...")
            try:
                login_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//button[@type="submit"]'))
                )
                print("Login button found!")
            except Exception as e:
                print(f"Error finding login button: {str(e)}")
                print("Current page source:", driver.page_source[:500])
                raise

            print("Clicking login button...")
            login_button.click()
            print("Waiting for dashboard to load...")
            time.sleep(5)

            print("Navigating to the Apps page...")
            driver.get("https://hq1.appsflyer.com/apps/myapps")
            time.sleep(15)

            print("Loading all apps via scrolling...")
            app_elements = []
            while True:
                current_app_elements = driver.find_elements(By.CSS_SELECTOR, '[data-qa-id="card-app-id"]')

                if len(current_app_elements) > len(app_elements):
                    app_elements = current_app_elements
                    driver.execute_script("arguments[0].scrollIntoView();", app_elements[-1])
                    time.sleep(5)
                else:
                    break

            print("Scrolling complete. Extracting apps...")
            apps_with_installs = []
            app_name_elements = driver.find_elements(By.CSS_SELECTOR, '[data-qa-id="card-app-name"]')
            app_id_elements = driver.find_elements(By.CSS_SELECTOR, '[data-qa-id="card-app-id"]')
            install_elements = driver.find_elements(By.CSS_SELECTOR, 'div.installs')

            for index, app_element in enumerate(app_id_elements):
                app_id = app_element.text
                app_name = app_name_elements[index].text if index < len(app_name_elements) else "N/A"
                install_count = int(install_elements[index].text.replace(",", "")) if index < len(install_elements) else 0

                if install_count > 0:
                    apps_with_installs.append({"app_id": app_id, "app_name": app_name, "install_count": install_count})

            print(f"Apps with installs > 0: {len(apps_with_installs)}")
            return [{"app_id": app["app_id"], "app_name": app["app_name"]} for app in apps_with_installs]

        except Exception as e:
            error_message = str(e).lower()
            # Check for specific API limitations that don't need retries
            if "maximum number of install reports" in error_message or "subscription package doesn't include raw data" in error_message:
                print(f"API limitation detected: {error_message}")
                print("Skipping retries for this app due to API limitations.")
                return []  # Return empty list immediately for these cases
            
            retries += 1
            print(f"An error occurred: {e}. Retrying ({retries}/{max_retries})...")
            if retries >= max_retries:
                print("Max retries reached. Giving up.")
                return []
            time.sleep(15)

        finally:
            driver.quit()

if __name__ == "__main__":
    login_to_appsflyer()

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get("https://www.google.com")
    print(driver.title)
    driver.quit() 