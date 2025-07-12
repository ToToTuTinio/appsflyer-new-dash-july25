from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
from dotenv import load_dotenv
import time
from selenium.webdriver.common.keys import Keys
import subprocess
import sys
from pathlib import Path

# Load environment variables
load_dotenv()

def get_chrome_driver_service():
    """
    Get ChromeDriver service with automatic path detection for different environments.
    Returns Service object or None if ChromeDriver should be auto-detected.
    """
    # Try different ChromeDriver paths for different environments
    possible_paths = [
        "/usr/local/bin/chromedriver",  # Docker installation
        "/usr/bin/chromedriver",  # System installation
        str(Path.home() / "bin" / "chromedriver"),  # Local development
        "chromedriver",  # In PATH
    ]
    
    chromedriver_path = None
    for path in possible_paths:
        if path == "chromedriver":
            # Check if chromedriver is in PATH
            import shutil
            if shutil.which("chromedriver"):
                chromedriver_path = "chromedriver"
                break
        elif os.path.exists(path):
            chromedriver_path = path
            break
    
    if chromedriver_path:
        print(f"Found ChromeDriver at: {chromedriver_path}")
        
        if chromedriver_path != "chromedriver" and not os.access(chromedriver_path, os.X_OK):
            print(f"ChromeDriver at {chromedriver_path} is not executable. Attempting to fix permissions...")
            os.chmod(chromedriver_path, 0o755)
        
        print(f"Creating Chrome service with ChromeDriver at: {chromedriver_path}")
        return Service(executable_path=chromedriver_path)
    else:
        print("ChromeDriver not found in common locations. Will try system PATH...")
        return None

def setup_driver():
    try:
        chrome_options = Options()
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--remote-debugging-port=9222")
        
        # Set Chrome binary location for Railway/Linux environments
        import shutil
        chrome_path = (
            shutil.which("google-chrome-stable") or 
            shutil.which("google-chrome") or 
            shutil.which("chrome") or 
            shutil.which("chromium")
        )
        if chrome_path:
            chrome_options.binary_location = chrome_path
            print(f"Using Chrome binary at: {chrome_path}")
        
        # Get ChromeDriver service
        service = get_chrome_driver_service()
        
        print("Initializing Chrome WebDriver...")
        if service:
            driver = webdriver.Chrome(service=service, options=chrome_options)
        else:
            # Try without specifying path (let Selenium find it)
            print("Using system PATH to find ChromeDriver...")
            driver = webdriver.Chrome(options=chrome_options)
        
        print("Chrome WebDriver initialized successfully!")
        return driver
            
    except Exception as e:
        print(f"Error in setup_driver: {str(e)}")
        print(f"Current working directory: {os.getcwd()}")
        print(f"PATH environment variable: {os.environ.get('PATH', '')}")
        
        # Try to find available executables
        import shutil
        chrome_path = (
            shutil.which("google-chrome-stable") or 
            shutil.which("google-chrome") or 
            shutil.which("chrome") or 
            shutil.which("chromium")
        )
        chromedriver_path = shutil.which("chromedriver")
        
        print(f"Chrome executable found: {chrome_path}")
        print(f"ChromeDriver executable found: {chromedriver_path}")
        
        raise

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
    
    # Optimal window size for AppsFlyer's responsive layout
    # Narrow width forces single/double column layout for more predictable scrolling
    chrome_options.add_argument("--window-size=800,1200")  # Narrow width, tall height
    
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Additional options for better headless mode performance and lazy loading
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--disable-ipc-flooding-protection")
    chrome_options.add_argument("--enable-automation")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--disable-features=TranslateUI")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-default-apps")
    chrome_options.add_argument("--disable-sync")
    chrome_options.add_argument("--metrics-recording-only")
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--enable-precise-memory-info")
    chrome_options.add_argument("--disable-logging")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--aggressive-cache-discard")
    
    # Set Chrome binary location for Railway/Linux environments
    import shutil
    chrome_path = (
        shutil.which("google-chrome-stable") or 
        shutil.which("google-chrome") or 
        shutil.which("chrome") or 
        shutil.which("chromium")
    )
    if chrome_path:
        chrome_options.binary_location = chrome_path
        print(f"Using Chrome binary at: {chrome_path}")
    
    # Prefs for better performance and lazy loading
    chrome_options.add_experimental_option("prefs", {
        "profile.default_content_setting_values": {
            "notifications": 2,
            "media_stream": 2,
        },
        "profile.managed_default_content_settings": {
            "images": 1
        }
    })

    # Get ChromeDriver service
    service = get_chrome_driver_service()
    
    # Initialize Chrome WebDriver
    if service:
        driver = webdriver.Chrome(service=service, options=chrome_options)
    else:
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
            time.sleep(15)

            print("Navigating to the Apps page...")
            driver.get("https://hq1.appsflyer.com/apps/myapps")
            time.sleep(20)
            
            # Inject JavaScript to help with lazy loading in headless mode
            driver.execute_script("""
                // Force trigger scroll events for lazy loading
                window.addEventListener('scroll', function() {
                    // Dispatch custom events to ensure lazy loading triggers
                    window.dispatchEvent(new Event('resize'));
                    window.dispatchEvent(new Event('scroll'));
                });
                
                // Override IntersectionObserver to be more aggressive in headless mode
                if (window.IntersectionObserver) {
                    const OriginalIntersectionObserver = window.IntersectionObserver;
                    window.IntersectionObserver = function(callback, options) {
                        // Make threshold more aggressive for headless mode
                        const modifiedOptions = {
                            ...options,
                            threshold: 0,
                            rootMargin: '100px'
                        };
                        return new OriginalIntersectionObserver(callback, modifiedOptions);
                    };
                }
                
                // Simulate user activity to prevent throttling
                setInterval(() => {
                    window.dispatchEvent(new Event('scroll'));
                }, 1000);
            """)
            print("JavaScript injection complete for better lazy loading")

            print("Loading all apps via scrolling...")
            print("🎯 Using optimized scrolling for narrow layout (800px width)")
            
            # Enhanced scrolling approach for maximum app collection
            last_height = driver.execute_script("return document.body.scrollHeight")
            scroll_attempts = 0
            max_scroll_attempts = 100  # Increased for thorough collection
            stable_count = 0
            max_stable_attempts = 5  # More strict stability check
            last_app_count = 0
            
            # Track app discovery rate
            app_counts_history = []
            
            while scroll_attempts < max_scroll_attempts:
                # Get current app count
                current_app_elements = driver.find_elements(By.CSS_SELECTOR, '[data-qa-id="card-app-id"]')
                current_count = len(current_app_elements)
                app_counts_history.append(current_count)
                
                print(f"📊 Scroll attempt {scroll_attempts + 1}: Found {current_count} apps")
                
                # Multi-strategy aggressive scrolling
                # Strategy 1: Scroll to absolute bottom with multiple attempts
                for i in range(3):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1)
                
                # Strategy 2: Check height and app count changes
                new_height = driver.execute_script("return document.body.scrollHeight")
                height_changed = new_height > last_height
                count_changed = current_count > last_app_count
                
                # Strategy 3: Incremental scrolling from current position
                current_scroll = driver.execute_script("return window.pageYOffset;")
                for step in [400, 800, 1200]:
                    driver.execute_script(f"window.scrollTo(0, {current_scroll + step});")
                    time.sleep(0.5)
                
                # Strategy 4: Force scroll to last visible element and beyond
                if current_app_elements:
                    try:
                        last_element = current_app_elements[-1]
                        # Scroll to last element
                        driver.execute_script("arguments[0].scrollIntoView({behavior: 'instant', block: 'center'});", last_element)
                        time.sleep(1)
                        # Scroll beyond it
                        driver.execute_script("window.scrollBy(0, 1000);")
                        time.sleep(1)
                    except:
                        pass
                
                # Strategy 5: Page End key simulation
                driver.execute_script("document.body.focus(); window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                
                # Strategy 6: Force trigger scroll events
                driver.execute_script("""
                    window.dispatchEvent(new Event('scroll'));
                    window.dispatchEvent(new Event('resize'));
                    setTimeout(() => {
                        window.scrollTo(0, document.body.scrollHeight);
                    }, 500);
                """)
                time.sleep(2)
                
                # Re-check app count after all scrolling strategies
                final_check_elements = driver.find_elements(By.CSS_SELECTOR, '[data-qa-id="card-app-id"]')
                final_count = len(final_check_elements)
                
                # Update tracking variables
                if height_changed or final_count > current_count:
                    last_height = new_height
                    last_app_count = max(current_count, final_count)
                    stable_count = 0
                    print(f"✅ Progress: height_changed={height_changed}, apps_found={final_count}")
                else:
                    stable_count += 1
                    print(f"⏸️ No new content (stable_count: {stable_count}/{max_stable_attempts})")
                
                # Enhanced exit conditions
                recent_counts = app_counts_history[-5:] if len(app_counts_history) >= 5 else app_counts_history
                if len(recent_counts) >= 3 and len(set(recent_counts)) == 1:
                    print(f"🔒 App count stable at {final_count} for last {len(recent_counts)} attempts")
                    stable_count += 2  # Accelerate exit for truly stable counts
                
                # Exit conditions
                if stable_count >= max_stable_attempts:
                    print(f"🎯 Scrolling complete: {final_count} apps found after {scroll_attempts + 1} attempts")
                    break
                
                scroll_attempts += 1
                
                # Periodic deep wait for lazy loading
                if scroll_attempts % 10 == 0:
                    print("⏳ Deep wait for lazy loading...")
                    time.sleep(5)
                elif scroll_attempts % 5 == 0:
                    print("⏳ Waiting for lazy loading...")
                    time.sleep(3)
            
            # Final comprehensive verification
            print("🔍 Final verification and cleanup...")
            
            # Multiple final scrolls to ensure everything is loaded
            for i in range(5):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                driver.execute_script("window.scrollBy(0, 500);")
                time.sleep(1)
            
            # Get final app count
            final_app_elements = driver.find_elements(By.CSS_SELECTOR, '[data-qa-id="card-app-id"]')
            final_total = len(final_app_elements)
            
            print(f"📈 Scrolling Statistics:")
            print(f"   • Total scroll attempts: {scroll_attempts}")
            print(f"   • Final apps found: {final_total}")
            print(f"   • Apps per attempt: {final_total/max(1, scroll_attempts):.1f}")
            
            if final_total == 0:
                print("❌ No apps found - this might indicate:")
                print("   • Login issues or session timeout")
                print("   • AppsFlyer interface changes")
                print("   • Network connectivity problems")
            elif final_total < 10:
                print("⚠️  Warning: Found very few apps. This might indicate:")
                print("   • Network issues during loading")
                print("   • AppsFlyer interface changes")
                print("   • Lazy loading configuration issues")
            else:
                print("🎉 Success: All apps loaded successfully!")

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

def get_all_apps_with_status(email, password, max_retries=7):
    """
    Fetch all apps from AppsFlyer, including both active and inactive ones.
    Returns a list of apps with their status and basic information.
    """
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

    # Set Chrome binary location for Railway/Linux environments
    import shutil
    chrome_path = (
        shutil.which("google-chrome-stable") or 
        shutil.which("google-chrome") or 
        shutil.which("chrome") or 
        shutil.which("chromium")
    )
    if chrome_path:
        chrome_options.binary_location = chrome_path
        print(f"Using Chrome binary at: {chrome_path}")

    # Get ChromeDriver service
    service = get_chrome_driver_service()
    
    # Initialize Chrome WebDriver
    if service:
        driver = webdriver.Chrome(service=service, options=chrome_options)
    else:
        driver = webdriver.Chrome(options=chrome_options)
    
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})

    retries = 0
    while retries < max_retries:
        try:
            print("Opening login page...")
            driver.get("https://hq1.appsflyer.com/auth/login")
            time.sleep(5)

            print("Waiting for email field...")
            email_field = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.ID, "user-email"))
            )
            print("Email field found!")

            print("Entering email...")
            email_field.clear()
            email_field.send_keys(email)
            time.sleep(1)

            print("Waiting for password field...")
            password_field = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.ID, "password-field"))
            )
            print("Password field found!")

            print("Entering password...")
            password_field.clear()
            password_field.send_keys(password)
            time.sleep(1)
            
            print("Looking for login button...")
            login_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//button[@type="submit"]'))
            )
            print("Login button found!")

            print("Clicking login button...")
            login_button.click()
            print("Waiting for dashboard to load...")
            time.sleep(5)

            print("Navigating to the Apps page...")
            driver.get("https://hq1.appsflyer.com/apps/myapps")
            time.sleep(15)

            print("Loading all apps via scrolling...")
            app_elements = []
            last_height = driver.execute_script("return document.body.scrollHeight")
            
            while True:
                # Scroll to bottom
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                # Calculate new scroll height and compare with last scroll height
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            print("Scrolling complete. Extracting apps...")
            apps = []
            
            # Get all app cards
            app_cards = driver.find_elements(By.CSS_SELECTOR, '[data-qa-id="card-app-id"]')
            
            for card in app_cards:
                try:
                    # Get app ID
                    app_id = card.text
                    
                    # Get parent card element
                    card_element = card.find_element(By.XPATH, "./ancestor::div[contains(@class, 'MuiCard-root')]")
                    
                    # Get app name
                    app_name = card_element.find_element(By.CSS_SELECTOR, '[data-qa-id="card-app-name"]').text
                    
                    # Check if app is active by looking for the active indicator
                    is_active = True
                    try:
                        active_indicator = card_element.find_element(By.CSS_SELECTOR, '.active-indicator')
                        is_active = 'active' in active_indicator.get_attribute('class').lower()
                    except:
                        pass
                    
                    # Get install count if available
                    install_count = 0
                    try:
                        install_element = card_element.find_element(By.CSS_SELECTOR, 'div.installs')
                        install_count = int(install_element.text.replace(",", ""))
                    except:
                        pass
                    
                    apps.append({
                        "app_id": app_id,
                        "app_name": app_name,
                        "is_active": is_active,
                        "install_count": install_count
                    })
                    
                except Exception as e:
                    print(f"Error processing app card: {str(e)}")
                    continue

            print(f"Total apps found: {len(apps)}")
            return apps

        except Exception as e:
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
    
    # Test the driver setup
    driver = setup_driver()
    driver.get("https://www.google.com")
    print(driver.title)
    driver.quit() 