from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import time
import os
import pandas as pd
import holidays
import glob
import datetime
import random
import KTE_artesian as kta


# Parametri per la localizzazione dei file scaricati
user = 'M.Serra'
download_path= 'C:\\Users\\' + user + '\\Downloads\\'


def add_noise(x):
    n = random.random()
    if n % 2 == 0:
        return round(x + 0.1, 4)
    else:
        return round(x - 0.1, 4)


def make_artesian_dict(df, colonna):
    dict_artesian = dict()
    for index, row in df.iterrows():
        dict_artesian[datetime.datetime(index.year, index.month, index.day, index.hour)] = row[colonna]
    return dict_artesian


def xlsx_to_df(xlsx, colonne = None, righe = None, sheet=""):
    df = pd.read_excel(xlsx, index_col=None, sheet_name=sheet, usecols=colonne, skiprows=righe)
    df.drop(columns=df.columns[0], inplace=True)
    df.drop(index=range(int(df.loc[df['Date'].isna()].index.to_list()[0]), len(df)), inplace=True)
    return df


def gen_dates_in_two_dates(dt_start_date, dt_end_date):
    for intDays in range(int((dt_end_date - dt_start_date).days)):
        yield dt_start_date + datetime.timedelta(intDays)


def make_df_of_days(day_one):
    start = day_one + datetime.timedelta(days=1)
    end = datetime.datetime.today() + datetime.timedelta(days=1)
    list_days = gen_dates_in_two_dates(start, end)
    df = pd.DataFrame(list_days, columns=['Date'])
    df.set_index('Date', inplace=True)
    return df


def download_heren_data():
    lst_workspaces = ['ui-id-2', 'ui-id-3', 'ui-id-4', 'ui-id-5', 'ui-id-6']
    lst_div = ['workspace-2-widget-2', 'workspace-4-widget-2', 'workspace-5-widget-3', 'workspace-6-widget-4', 'workspace-7-widget-5']
    chrome_options = Options()
    browser = webdriver.Chrome(executable_path='C:\\Users\\M.Serra\\Documents\\Grattuggia\\chromedriver.exe')
    browser.maximize_window()
    actions = ActionChains(browser)
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
    browser.find_element(By.XPATH, "//div[@data-test-id='navItem-user']").click()
    browser.find_element(By.XPATH, "//span[text()='Logout']").click()
    browser.close()


def read_heren_data():
    full_path = os.path.join(download_path,
                             'ICIS Dashboard Price History ' + datetime.datetime.now().strftime('%Y-%m-%d') + '*.xls')
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


def nearest_working_day(day, delta=1):
    uk_holidays = holidays.UK()
    day = day + datetime.timedelta(days=delta)
    while day in uk_holidays or day.weekday() >= 5:
        day = day + datetime.timedelta(days=delta)
    return day


def add_row(df):
    len_cols = len(df.columns)
    riga = [pd.np.nan for i in range(len_cols)]
    riga.name = nearest_working_day(df.index[-1])
    df.loc[nearest_working_day(df.index[-1])] = riga
    return df


def fill_holidays(df, df_in, colonna):
    for index, row in df.iterrows():
        if pd.np.isnan(row[0]):
            row[0] = df_in.loc[nearest_working_day(index, delta=-1), colonna]
    return df


def make_curva_spot(df_heren, colonna):
    oggi = datetime.datetime.today()
    start_date = datetime.datetime(oggi.year - 1, oggi.month, oggi.day)
    df_aux = make_df_of_days(nearest_working_day(start_date))
    df_out = pd.merge(df_aux, df_heren[colonna].shift(),
                      right_index=True, left_index=True, how='left')
    return df_out
