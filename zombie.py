from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd

import glob
import datetime
import os
import logging
import shutil

user = 'M.Serra'

def xlsx_to_df(xlsx, colonne = None, righe = None, sheet=""):
    df = pd.read_excel(xlsx, index_col=None, sheet_name=sheet, usecols=colonne, skiprows=righe)
    df.drop(columns=df.columns[0], inplace=True)
    df.drop(index=range(int(df.loc[df['Date'].isna()].index.to_list()[0]), len(df)), inplace=True)
    return df


def download_heren_data():
    lst_workspaces = ['ui-id-2', 'ui-id-3', 'ui-id-4', 'ui-id-5', 'ui-id-6']
    lst_div = ['workspace-2-widget-2', 'workspace-4-widget-2', 'workspace-5-widget-3', 'workspace-6-widget-4', 'workspace-7-widget-5']
    browser = webdriver.Chrome(executable_path='C:\\Users\\' + user + '\\Documents\\Grattuggia\\chromedriver.exe')
    browser.maximize_window()
    browser.get('https://www.icis.com/Dashboard')
    elem = browser.find_element(By.ID, 'username-input')  # Find the search box
    elem.send_keys('claudio.milo@ast.arvedi.it')
    elem = browser.find_element(By.ID, 'password-input')  # Find the search box
    elem.send_keys('Gefs2019' + Keys.ENTER)

    i = 0
    for workspace in lst_workspaces:
        browser.find_element(By.ID, workspace).click()  # Find the search box
        time.sleep(16)
        xpath_string = "//div[@id='" + lst_div[i] + "']/div/div/span[6]"
        aux = browser.find_element(By.XPATH, xpath_string) # Find the search box
        aux.click()
        time.sleep(1)
        try:
            browser.find_element(By.XPATH, "//a[@data-format='EXCEL']").click()
            time.sleep(2)
        except:
            browser.find_element(By.CLASS_NAME, 'ui-icon-download-enabled').click()
            print(browser.find_element(By.CLASS_NAME, 'download-contextmenu').get_attribute('style'))
            time.sleep(2)
            browser.find_element(By.XPATH, "//a[@data-format='EXCEL']").click()
        finally:
            i += 1
    try:
        browser.find_element(By.XPATH, "//div[@data-test-id='navItem-user']").click()
        browser.find_element(By.XPATH, "//span[text()='Logout']").click()
    except Exception as e:
        logging.error(e)
    finally:
        browser.close()


def read_heren_data(base_path):
    full_path = os.path.join(base_path, 'ICIS Dashboard Price History ' + datetime.datetime.now().strftime('%Y-%m-%d') + '*.xls')
    files = glob.glob(full_path)
    files.sort(key=os.path.getmtime, reverse=True)
    df_heren = pd.DataFrame()
    for file_path in files:
        file = os.path.basename(file_path)
        try:
            data_file = file.replace('ICIS Dashboard Price History ', '').split('.')[0]
            datetime.datetime.strptime(data_file, '%Y-%m-%d %H%M%S')
            df_tmp = xlsx_to_df(file_path, None, 12, 'ICIS Price History')
            df_tmp['Date'] = pd.to_datetime(df_tmp['Date'])
            df_tmp.set_index('Date', inplace=True)
            if len(df_heren) == 0:
                df_heren = df_tmp
            else:
                df_heren = pd.merge(df_heren, df_tmp, left_index=True, right_index=True)
        except TypeError:
            pass

    return df_heren


def move_file(file_path, dest_path):
    file = os.path.basename(file_path)
    try:
        path_to = os.path.join(dest_path)
        os.mkdir(path_to)
    except FileExistsError as e:
        print('Cartella già creata')
    finally:
        shutil.move(file_path, os.path.join(dest_path, file))


def get_heren_data(base_path, download_path):
    download_heren_data()
    move_file(download_path, base_path)
    return read_heren_data(base_path)
