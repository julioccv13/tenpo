from core.google import bigquery
from google.cloud import storage as st
from core.google.bigquery import client as client_bq
from core.ports import Process
import pandas as pd
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import os
import time
import json

class Process113(Process):
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._project_name = kwargs["project_name"]
        self._dataset_jarvis = kwargs["dataset_jarvis"]
        self._bq_table = kwargs["bq_table"]
        self._website_url = kwargs["website_url"]
        self._xpath_buttom = kwargs["xpath_buttom"]
        self._file_name = kwargs["file_name"]
        self._var_blank_space = kwargs["var_blank_space"]
        self._var_underscore = kwargs["var_underscore"]
        self._var_oP = kwargs["var_oP"]
        self._var_o = kwargs["var_o"]
        self._current_dir = os.getcwd()
        self._prefs = {'download.default_directory' : self._current_dir}
    
    def run(self):
        self._dowload_file()
        self._insert_file()

    def _dowload_file(self):
        """this will execute and instanciate the driver to proceed to download the file"""
        chrome_options = Options()
        prefs = self._prefs
        chrome_options.add_experimental_option('prefs',prefs)
        chrome_options.headless = True
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        self._remove_file()
        driver = webdriver.Chrome(service=Service(ChromeDriverManager(version="114.0.5735.90").install()),options=chrome_options)
        driver.get(self._website_url)
        download_button = driver.find_element(By.XPATH, self._xpath_buttom)
        download_button.click()
        self._download_wait()
        driver.quit()
    
    def _download_wait(self,timeout=1800, nfiles=None):
        """checks if the download it's complete"""
        seconds = 0
        dl_wait = True
        while dl_wait and seconds < timeout:
            time.sleep(1)
            dl_wait = False
            files = os.listdir()
            if nfiles and len(files) != nfiles:
                dl_wait = True
            for fname in files:
                if fname.endswith('.crdownload'):
                    dl_wait = True
            seconds += 1
        return seconds
    
    def _insert_file(self):
        """This function help us to read the file from the file system""" 
        is_file_downloaded = self._check_if_file_exists()

        if is_file_downloaded:
            with open(self._file_name, 'r', encoding='utf-8') as file:
                data = json.load(file)
                df = pd.DataFrame.from_dict(data)
                df.to_gbq(f'{self._dataset_jarvis}.{self._bq_table}', project_id=self._project_name, if_exists='replace')

    def _check_if_file_exists(self) -> bool:
        files_list = os.listdir()
        if any(['registro_publicaciones_full' in x for x in files_list]):
            return True
        else:
            return False
               
    def _remove_file(self):
        """This function remove the file previously download it"""
        files_list = os.listdir()
        for file in files_list:
            if 'registro_publicaciones_full' in file:
                os.remove(file)

    def _logger_storage(self, text:str):
        """This function helps us to log to a cloud storage and monitor the main script """
        client = st.Client()
        bucket = client.get_bucket(self._bucket_name)
        unix = str(time.time()).replace('.','_')
        bucket.blob(f'113/files/log_{unix}.txt').upload_from_string(text, 'text')