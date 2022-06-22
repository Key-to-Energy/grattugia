from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.options import Options
import time
import pandas as pd
import holidays
import glob
import datetime
import random
import KTE_artesian as kta
import os
import logging
import shutil
import numpy as np

# Parametri per la localizzazione dei file scaricati
user = 'Usr-GSE-2020'
download_path = 'C:\\Users\\' + user + '\\Downloads\\'
logging.basicConfig(filename='log_error.txt', level=logging.ERROR)
logging.basicConfig(filename='log.txt', level=logging.INFO)


def add_noise(x, columns, noise_curve):
    valore = x[columns] - noise_curve.loc[x.name.tz_localize(tz='Europe/Stockholm')]['Random number daily']
    return valore


def add_noise_to_df(df):
    list_of_column_names = df.columns.to_list()
    for name in list_of_column_names:
        df[name] = df.apply(lambda x: add_noise(x[name]), axis=1)
    return df


def make_artesian_dict(df, colonna):
    dict_artesian = dict()
    for index, row in df.iterrows():
        dict_artesian[datetime.datetime(index.year, index.month, index.day, index.hour)] = row[colonna]
    return dict_artesian


def make_artesian_dict_forward(df_heren, list_of_products_names):
    df = df_heren[list_of_products_names]
    dict_artesian = dict()
    for index, row in df.iterrows():
        dict_artesian[datetime.datetime(index.year, index.month, index.day)] = dict()
        for product in list_of_products_names:
            dict_artesian[datetime.datetime(index.year, index.month, index.day)][
                product] = kta.MarketData.MarketAssessmentValue(settlement=row[product])

    return dict_artesian


def xlsx_to_df(xlsx, colonne=None, righe=None, sheet=""):
    df = pd.read_excel(xlsx, index_col=None, sheet_name=sheet, usecols=colonne, skiprows=righe)
    df.drop(columns=df.columns[0], inplace=True)
    df.drop(index=range(int(df.loc[df['Date'].isna()].index.to_list()[0]), len(df)), inplace=True)
    return df


def gen_dates_in_two_dates(dt_start_date, dt_end_date):
    for intDays in range(int((dt_end_date - dt_start_date).days)):
        yield dt_start_date + datetime.timedelta(intDays)


def make_df_of_days(day_one):
    start = day_one + datetime.timedelta(days=1)
    # end = datetime.datetime.today() + datetime.timedelta(days=1)
    end = nearest_working_day(datetime.datetime.today(), 0) + datetime.timedelta(days=1)
    list_days = gen_dates_in_two_dates(start, end)
    df = pd.DataFrame(list_days, columns=['Date'])
    df.set_index('Date', inplace=True)
    return df


def download_heren_data():
    lst_workspaces = ['ui-id-2', 'ui-id-3', 'ui-id-4', 'ui-id-5', 'ui-id-6']
    lst_div = ['workspace-2-widget-2', 'workspace-4-widget-2', 'workspace-5-widget-3', 'workspace-6-widget-4',
               'workspace-7-widget-5']
    chrome_options = Options()
    browser = webdriver.Chrome(executable_path='C:\\Users\\' + user + '\\Desktop\\Grattugia\\chromedriver.exe')
    browser.maximize_window()
    actions = ActionChains(browser)
    browser.get('https://www.icis.com/Dashboard')
    elem = browser.find_element(By.ID, 'username-input')  # Find the search box
    elem.send_keys('claudio.milo@ast.arvedi.it')
    elem = browser.find_element(By.ID, 'password-input')  # Find the search box
    elem.send_keys('Gefs2019' + Keys.ENTER)

    try:
        time.sleep(8)
        browser.find_element(By.XPATH, "/html/body/div[12]/button[1]").click()
    except:
        pass
    i = 0
    for workspace in lst_workspaces:
        browser.find_element(By.ID, workspace).click()  # Find the search box
        time.sleep(16)
        xpath_string = "//div[@id='" + lst_div[i] + "']/div/div/span[6]"
        aux = browser.find_element(By.XPATH, xpath_string)  # Find the search box
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
        # move_file(file_path)
    return df_heren


def nearest_working_day(day, delta=1):
    uk_holidays = holidays.UK()
    day = day + datetime.timedelta(days=delta)
    if delta == 0:
        delta = 1
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
        if np.isnan(row[0]):
            row[0] = df_in.loc[nearest_working_day(index, delta=-1), colonna]
    return df


def make_curva_spot(df_heren, colonna):
    oggi = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = datetime.datetime(oggi.year - 1, oggi.month, oggi.day)
    df_aux = make_df_of_days(nearest_working_day(start_date))
    df_out = pd.merge(df_aux, df_heren[colonna]
                      .reindex(df_heren.index.union([nearest_working_day(oggi, 0)]))
                      .shift(),
                      right_index=True, left_index=True, how='left')
    return df_out


def split_psv_ttf(df_heren):
    return df_heren.loc[:, df_heren.columns.str.startswith('PSV')], df_heren.loc[:,
                                                                    df_heren.columns.str.startswith('TTF')]


def codifica_month(data, offset):
    data = data + pd.DateOffset(months=offset)
    mese = datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%b')
    return mese + '-' + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')


def codifica_season(data, offset):
    data = data + pd.DateOffset(months=offset * 6)
    if data.month < 4 or data.month > 9:
        if data.month < 4:
            return 'Win-' + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')
        else:
            data = data + pd.DateOffset(years=1)
            return 'Win-' + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')

    else:
        return 'Sum-' + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')


def codifica_quarter(data, offset):
    data = data + pd.DateOffset(months=offset * 3)
    return 'Q' + str(data.quarter) + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')


def codifica_year(data, offset):
    data = data + pd.DateOffset(years=offset)
    return datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%Y')


def codifica_colonna(nome_colonna, data):
    prodotto = nome_colonna.split(' ')[3]
    if prodotto == 'Season':
        offset = int(nome_colonna.split(' ')[4][1])
        return codifica_season(data, offset)
    if prodotto == 'Day-Ahead':
        return 'DA'
    if prodotto == 'Year':
        offset = int(nome_colonna.split(' ')[4][1])
        return codifica_year(data, offset)
    if prodotto == 'Quarter':
        offset = int(nome_colonna.split(' ')[4][1])
        return codifica_quarter(data, offset)
    if prodotto == 'Month':
        offset = int(nome_colonna.split(' ')[4][1])
        return codifica_month(data, offset)
    if prodotto == 'Balance':
        return 'BOM'
    return 'x'


def get_market_assestments_artesian_dict(df):
    dict_of_market_assestments = dict()
    for index, row in df.iterrows():
        list_of_column_names = row.index.to_list()
        giorno_tmp = datetime.datetime(index.year, index.month, index.day)
        dict_of_market_assestments[giorno_tmp] = dict()
        for name in list_of_column_names:
            codifica = codifica_colonna(name, index)
            if codifica != 'x':
                if not np.isnan(row[name]):
                    dict_of_market_assestments[giorno_tmp][codifica] = kta.MarketData.MarketAssessmentValue(
                        settlement=row[name])

    return dict_of_market_assestments


def move_file(file_path):
    file = os.path.basename(file_path)
    try:
        path_to = os.path.join("Y:\Abagas-G", datetime.datetime.now().strftime('%Y-%m-%d'))
        os.mkdir(path_to)
    except FileExistsError as e:
        print('Cartella già creata')
    finally:
        shutil.move(file_path, os.path.join("Y:\Abagas-G", datetime.datetime.now().strftime('%Y-%m-%d'), file))


def genera_date_in_range(start, end):
    delta = end - start
    date_list = [end - datetime.timedelta(days=x) for x in range(delta.days)]
    return date_list

def genera_curva_random():
    start_date = '2020-01-01'
    end_date = '2050-01-01'
    start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    arr_of_date = genera_date_in_range(start, end)
    dict_to_artesian = dict()
    for day in arr_of_date:
        dict_to_artesian[day] = round(random.uniform(-0.1, 0.1), 2)
    kta.post_artesian_actual_time_series(dict_to_artesian, dict(), 'DevKtE', 'Random number daily', 'd')
