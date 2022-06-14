from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.edge.options import Options
import time
import pandas as pd
import glob
import KTE_artesian as kta
import os
import logging


user = 'M.Serra'
download_path= 'C:\\Users\\' + user + '\\Downloads\\'
logging.basicConfig(filename='log_error.txt', level=logging.ERROR)
logging.basicConfig(filename='log.txt', level=logging.INFO)

def download_terna_data():
    chrome_options = Options()
    browser = webdriver.Chrome(executable_path='C:\\Users\\' + user + '\\Documents\\Grattuggia\\chromedriver.exe')
    browser.maximize_window()
    actions = ActionChains(browser)
    browser.get('https://myterna.terna.it/sunset/Public')
    elem = browser.find_element(By.XPATH,
                                '/html/body/div[2]/div[2]/div/div[1]/div/div/ul/li[3]/ul/li[4]/a')
    elem.click()
    time.sleep(2)
    elem = browser.find_element(By.XPATH,
                                '/html/body/div[2]/div[2]/div/div[2]/section/div/div/div[2]/div/table/tbody/tr[1]/td[3]/div/a/span')
    elem.click()
    time.sleep(5)


def read_terna_excels():
    full_path = os.path.join(download_path,
                             'Prezzi_Giornalieri_Quarto_Orari_*.xlsx')
    files = glob.glob(full_path)
    df = pd.read_excel(files[0], index_col=0, sheet_name='Prezzi Giornalieri Quarto Orari', skiprows=1)
    df_nord = df.loc[df['Macrozona'] == 'NORD']
    df_sud = df.loc[df['Macrozona'] == 'SUD']
    return df_nord, df_sud


def upload_terna_data(df_sud, df_nord):
    dict_sud_artesian = kta.make_artesian_dict_actual(df_sud, 'Prezzo di sbilanciamento')
    dict_nord_artesian = kta.make_artesian_dict_actual(df_nord, 'Prezzo di sbilanciamento')
    kta.post_artesian_actual_time_series(dict_sud_artesian, dict(), 'DevKtE', 'Prezzi sbilanciamento SUD terna', 'f')
    kta.post_artesian_actual_time_series(dict_nord_artesian, dict(), 'DevKtE', 'Prezzi sbilanciamento NORD terna', 'f')

download_terna_data()
df_nord, df_sud = read_terna_excels()
upload_terna_data(df_sud, df_nord)