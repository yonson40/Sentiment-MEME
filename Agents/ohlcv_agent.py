import os
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from langchain_core.tools import BaseTool
from .schema import AgentState, OHLCVData

class OHLCVAgent(BaseTool):
    name = "ohlcv_agent"
    description = "Agent that scrapes OHLCV data from dexrabbit/Bitquery"
    
    def __init__(self):
        super().__init__()
        self.driver: Optional[Chrome] = None
        self.project_root = str(Path(__file__).parent.parent)
        self.data_dir = os.path.join(self.project_root, "ohlcv_data")
        os.makedirs(self.data_dir, exist_ok=True)
        self.bitquery_api_key = os.getenv("BITQUERY_API_KEY")

    def _run(self, state: AgentState) -> AgentState:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self._init_driver()
                self._scrape_token_data(state)
                state.status = "ohlcv_data_collected"
                state.last_run = datetime.now()
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    state.error_messages.append(f"OHLCV Error: {str(e)}")
                    state.status = "error"
        finally:
            if self.driver:
                self.driver.quit()
        return state

    def _init_driver(self):
        if not self.bitquery_api_key:
            raise ValueError("BITQUERY_API_KEY not found in .env")
        options = ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-notifications")
        options.add_argument(f"--user-data-dir={os.path.join(self.project_root, 'chrome_profile')}")
        self.driver = Chrome(options=options)

    def _scrape_token_data(self, state: AgentState):
        self.driver.get("https://dexrabbit.com/solana/pair")
        time.sleep(5)

        token_links = self.driver.find_elements(
            By.CSS_SELECTOR, "div.pair-list-item a.pair-link"
        )

        for link in token_links:
            token_name = link.text.strip()
            if not token_name:
                continue

            self.driver.execute_script("window.open(arguments[0]);", link.get_attribute('href'))
            self.driver.switch_to.window(self.driver.window_handles[-1])
            
            try:
                self._click_api_button()
                self._modify_timeframe()
                json_data = self._get_bitquery_data()
                
                if json_data:
                    standardized_data = self._transform_data(json_data)
                    self._save_data(token_name, standardized_data)
                    state.ohlcv_updates.append(standardized_data)

            except Exception as e:
                state.error_messages.append(f"Failed to process {token_name}: {str(e)}")
            finally:
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])

    def _click_api_button(self):
        wait = WebDriverWait(self.driver, 20)
        api_btn = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(., 'Get API')]")
        ))
        api_btn.click()
        time.sleep(3)

    def _modify_timeframe(self):
        timeframe_dropdown = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "select.timeframe-select"))
        )
        timeframe_dropdown.send_keys("seconds")
        time.sleep(1)
        
        run_btn = self.driver.find_element(By.CSS_SELECTOR, "button.execute-query")
        run_btn.click()
        time.sleep(5)

    def _get_bitquery_data(self):
        try:
            pre_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "pre"))
            )
            return json.loads(pre_element.text)
        except Exception as e:
            raise Exception(f"Failed to get JSON data: {str(e)}")

    def _transform_data(self, raw_data: dict) -> OHLCVData:
        return OHLCVData(
            token=raw_data['token'],
            datetime=datetime.fromisoformat(raw_data['time']),
            open=float(raw_data['open']),
            high=float(raw_data['high']),
            low=float(raw_data['low']),
            close=float(raw_data['close']),
            volume=float(raw_data['volume'])
        )

    def _save_data(self, token_name: str, data: OHLCVData):
        filename = f"{token_name.replace(' ', '_')}_ohlcv_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        filepath = os.path.join(self.data_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump({
                'token': data.token,
                'time': data.datetime.isoformat(),
                'open': data.open,
                'high': data.high,
                'low': data.low,
                'close': data.close,
                'volume': data.volume
            }, f, indent=2)
