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
user = 'M.Serra'
download_path = 'C:\\Users\\' + user + '\\Downloads\\'
CHROME_PATH = 'C:\\Users\\' + user + '\\Documents\\Grattuggia\\chromedriver.exe'
logging.basicConfig(filename='log_error.txt', level=logging.ERROR)
logging.basicConfig(filename='log.txt', level=logging.INFO)


def add_noise(x, columns, noise_curve):
    '''
    Funzione che aggiunge il rumore alle curve in base alla data
    :param x: Tupla il cui valore va sporcato
    :param columns: Nome colonna da modificare
    :param noise_curve: Curva del rumore da applicare
    :return: float o int a seconda dei tipi del valore contenuto nella tupla e nella noise curve
    '''
    valore = x[columns] - noise_curve.loc[x.name.tz_localize(tz='Europe/Stockholm')]['Random number daily']
    return valore


def add_noise_to_df(df):
    list_of_column_names = df.columns.to_list()
    for name in list_of_column_names:
        df[name] = df.apply(lambda x: add_noise(x[name]), axis=1)
    return df


def make_artesian_dict(df, colonna):
    '''
    Rende un dizionario utile per il caricamento dei dati su Artesian a partire da un pandas df con datetime come index
    :param df: Pandas dataframe da convertire in dizionario
    :param colonna: Nome della colonna da usare come campo valore
    :return: Dizionario utile al caricamento su Artesian
    '''
    dict_artesian = dict()
    for index, row in df.iterrows():
        dict_artesian[datetime.datetime(index.year, index.month, index.day, index.hour)] = row[colonna]
    return dict_artesian


def make_artesian_dict_forward(df_heren, list_of_products_names):
    '''
    Rende un dizionario per il caricamento di una MarketAssessment su Artesian
    :param df_heren: Pandas dataframe da convertire in dizionario
    :param list_of_products_names: Lista contenente il nome dei prodotti
    :return: Dizionario utile per il caricamento dei dati su Artesian
    '''
    df = df_heren[list_of_products_names]
    dict_artesian = dict()
    for index, row in df.iterrows():
        dict_artesian[datetime.datetime(index.year, index.month, index.day)] = dict()
        for product in list_of_products_names:
            dict_artesian[datetime.datetime(index.year, index.month, index.day)][
                product] = kta.MarketData.MarketAssessmentValue(settlement=row[product])

    return dict_artesian


def xlsx_to_df(xlsx, colonne=None, righe=None, sheet=""):
    '''
    Trasforma un xlsx preso da Icis in un Pandas dataframe
    :param xlsx: Path o file xlsx da convertire in dataframe
    :param colonne: Numero di colonne da prendere in considerazione
    :param righe: Numero di righe da saltare prima d'iniziare la conversione
    :param sheet: Foglio excel da convertire
    :return: Pandas dataframe generato dall'excel
    '''
    df = pd.read_excel(xlsx, index_col=None, sheet_name=sheet, usecols=colonne, skiprows=righe)
    df.drop(columns=df.columns[0], inplace=True)
    df.drop(index=range(int(df.loc[df['Date'].isna()].index.to_list()[0]), len(df)), inplace=True)
    return df


def gen_dates_in_two_dates(dt_start_date, dt_end_date):
    '''
    Genera tutte le date comprese tra i due estremi passati come input
    :param dt_start_date: Datetime indicante il primo estremo dell'intervallo
    :param dt_end_date: Datetime indicante l'estremo finale dell'intervallo
    :return: Array di Datetime
    '''
    for intDays in range(int((dt_end_date - dt_start_date).days)):
        yield dt_start_date + datetime.timedelta(intDays)


def make_df_of_days(day_one):
    '''
    Rende un df vuoto con datetime come index, il dataframe parte dal giorno passato come parametro sino al prossimo
    giorno lavorativo a partire da oggi
    :param day_one: Datetime rappresentante il giorno di partenza
    :return: Pandas dataframe
    '''
    start = day_one + datetime.timedelta(days=1)
    # end = datetime.datetime.today() + datetime.timedelta(days=1)
    end = nearest_working_day(datetime.datetime.today(), 0) + datetime.timedelta(days=1)
    list_days = gen_dates_in_two_dates(start, end)
    df = pd.DataFrame(list_days, columns=['Date'])
    df.set_index('Date', inplace=True)
    return df


def download_heren_data():
    '''
    Tramite l'utilizzo di Selenium scarica i dati dal sito di Icis e mette i file nella cartella di download
    :return:  None
    '''
    lst_workspaces = ['ui-id-2', 'ui-id-3', 'ui-id-4', 'ui-id-5', 'ui-id-6']
    lst_div = ['workspace-2-widget-2', 'workspace-4-widget-2', 'workspace-5-widget-3', 'workspace-6-widget-4',
               'workspace-7-widget-5']
    chrome_options = Options()
    browser = webdriver.Chrome(executable_path=CHROME_PATH)
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
    '''
    Legge i file excel relativi alla giornata odierna e li converte in un pandas dataframe
    :return: Pandas dataframe generati dai file excel nella cartella download
    '''
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
    '''
    Rende il prossimo giorno lavorativo dopo i giorni passati come delta secondo le festività Inglesi
    :param day: Datetime rappresentante il giorno di partenza
    :param delta: Numero di giorni da skippare prima di rendere il prossimo giorno lavorativo
    :return: Datetime rappresentante il prossimo giorno lavorativo
    '''
    uk_holidays = holidays.England()
    day = day + datetime.timedelta(days=delta)
    if delta == 0:
        delta = 1
    while day in uk_holidays or day.weekday() >= 5:
        day = day + datetime.timedelta(days=delta)
    return day


def add_row(df):
    '''
    Aggiunge una riga rappresentante il prossimo giorno lavorativo
    :param df: Pandas dataframe alla quale aggiungere la nuova riga
    :return: Pandas dataframe con la nuova riga
    '''
    len_cols = len(df.columns)
    riga = [pd.np.nan for i in range(len_cols)]
    riga.name = nearest_working_day(df.index[-1])
    df.loc[nearest_working_day(df.index[-1])] = riga
    return df


def fill_holidays(df, df_in, colonna):
    '''
    Copre i buchi dei weekend nelle curve spot
    :param df: Dataframe con in buchi
    :param df_in: Dataframe con i dati relativi al weekend
    :param colonna: Nome della colonna contenente i dati del weekend
    :return: Dataframe spot aggiornato di dati weekend
    '''
    for index, row in df.iterrows():
        try:
            if np.isnan(row[0]):
                row[0] = df_in.loc[nearest_working_day(index, delta=-1), colonna]
        except KeyError as e:
            logging.error(e)

    return df


def make_curva_spot(df_heren, colonna):
    '''
    Crea la curva spot con i dati day-ahead, lasciando buchi nei weekend e giorni di festa
    :param df_heren: Dataframe dalla quale estrarre i dati day-ahead
    :param colonna: Colonna contenente i dati dei day-ahead
    :return: Dataframe contenente i dati day-ahead spot
    '''
    oggi = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = datetime.datetime(oggi.year - 1, oggi.month, oggi.day)
    df_aux = make_df_of_days(nearest_working_day(start_date))
    df_out = pd.merge(df_aux, df_heren[colonna]
                      .reindex(df_heren.index.union([nearest_working_day(oggi, 0)]))
                      .shift(),
                      right_index=True, left_index=True, how='left')
    return df_out


def split_psv_ttf(df_heren):
    '''
    Splitta in 2 dataframe il dataframe il df passato come input in 2 dataframe,
    uno relativo al psv ed uno relativo al ttf
    :param df_heren: Dataframe contenente i dati Icis
    :return: Tupla (Dataframe_psv, Dataframe_ttf)
    '''
    return df_heren.loc[:, df_heren.columns.str.startswith('PSV')], df_heren.loc[:,
                                                                    df_heren.columns.str.startswith('TTF')]


def codifica_month(data, offset):
    '''
    Codifica il nome del mese nel formato utile ad artesian
    :param data: Datetime contenente la data d'emissione del dato
    :param offset: Numero di mesi di distanza tra la data d'emissione del dato e la data a cui il dato si riferisce
    :return: Stringa contenete la corretta codifica del dato secondo lo standar richiesto da Artesian
    '''
    data = data + pd.DateOffset(months=offset)
    mese = datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%b')
    return mese + '-' + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')


def codifica_season(data, offset):
    '''
    Codifica il nome della season nel formato utile ad artesian
    :param data: Datetime contenente la data d'emissione del dato
    :param offset: Numero di season di distanza tra la data d'emissione del dato e la data a cui il dato si riferisce
    :return: Stringa contenete la corretta codifica del nome del dato secondo lo standar richiesto da Artesian
    '''
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
    '''
    Codifica il nome del quarter nel formato utile ad artesian
    :param data: Datetime contenente la data d'emissione del dato
    :param offset: Numero di quarter di distanza tra la data d'emissione del dato e la data a cui il dato si riferisce
    :return: Stringa contenete la corretta codifica del nome del dato secondo lo standar richiesto da Artesian
    '''
    data = data + pd.DateOffset(months=offset * 3)
    return 'Q' + str(data.quarter) + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')


def codifica_year(data, offset):
    '''
    Codifica il nome dell'anno nel formato utile ad artesian
    :param data: Datetime contenente la data d'emissione del dato
    :param offset: Numero di anni di distanza tra la data d'emissione del dato e la data a cui il dato si riferisce
    :return: Stringa contenete la corretta codifica del nome del dato secondo lo standar richiesto da Artesian
    '''
    data = data + pd.DateOffset(years=offset)
    return datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%Y')


def codifica_colonna(nome_colonna, data):
    '''
    Codifica il nome della colonna nel formato utile ad artesian
    :param nome_colonna: Nome della colonna da formattare
    :param data: Datetime rappresentante la data a cui il dato si riferisce
    :return: Stringa contenete la corretta codifica del nome del dato secondo lo standar richiesto da Artesian
    '''
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
    '''
    Rende il dizionario utile al caricamento dei dati contenuti in un dataframe nel formato di MarketAssessment
    :param df: Dataframe contenente i dati della MarketAssessment da caricare
    :return: Dizionario utile al caricamento dei dati in Artesian
    '''
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
    '''
    Sposta i file presenti nella cartella 'file_path' nella cartella corretta in Abags-g su condivisa
    :param file_path: Path contenente i file da spostare
    :return: None
    '''
    file = os.path.basename(file_path)
    try:
        path_to = os.path.join("Y:\Abagas-G", datetime.datetime.now().strftime('%Y-%m-%d'))
        os.mkdir(path_to)
    except FileExistsError as e:
        print('Cartella già creata')
    finally:
        shutil.move(file_path, os.path.join("Y:\Abagas-G", datetime.datetime.now().strftime('%Y-%m-%d'), file))


def genera_date_in_range(start, end):
    '''
    Genera un array di date comprese tra le due date passate come parametro
    :param start: Datetime rappresentante la data d'inizio
    :param end: Datetime rappresentante la data di fine
    :return: Array contenente le date comprese nell'intervallo passato come input
    '''
    delta = end - start
    date_list = [end - datetime.timedelta(days=x) for x in range(delta.days)]
    return date_list


def genera_curva_random():
    '''
    Funzione per generare su Artesian una time series con valori randomici dal 1 gennaio 2020 al 1 gennaio 2025
    :return: None
    '''
    start_date = '2020-01-01'
    end_date = '2050-01-01'
    start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    arr_of_date = genera_date_in_range(start, end)
    dict_to_artesian = dict()
    for day in arr_of_date:
        dict_to_artesian[day] = round(random.uniform(-0.1, 0.1), 2)
    kta.post_artesian_actual_time_series(dict_to_artesian, dict(), 'DevKtE', 'Random number daily', 'd')


def correggi_assenza_dato(df):
    df_righe_mancanti = pd.DataFrame({'PSV Price Assessment Balance of Month (BOM) Heren Bid/Offer Range Daily (Mid) : EUR/MWh':
                           {'21-Oct-2022': 60.225},
                       'PSV Price Assessment Day-Ahead Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 48.013},
                       'PSV Price Assessment Month +1 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 106.15},
                       'PSV Price Assessment Month +2 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 140.875},
                       'PSV Price Assessment Month +3 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 148.075},
                       'PSV Price Assessment Quarter +1 Bid/Offer Range Daily (Mid) : EUR/MWh': {
                           '21-Oct-2022': 149.075},
                       'PSV Price Assessment Quarter +2 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 145.7},
                       'PSV Price Assessment Quarter +3 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 146.8},
                       'PSV Price Assessment Quarter +4 Bid/Offer Range Daily (Mid) : EUR/MWh': {
                           '21-Oct-2022': 149.363},
                       'PSV Price Assessment Weekend Bid/Offer Range Weekly (Mid) : EUR/MWh': {'21-Oct-2022': 25.75},
                       'TTF Price Assessment Month +1 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 113.7},
                       'TTF Price Assessment Month +2 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 146.125},
                       'TTF Price Assessment Month +3 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 151.25},
                       'TTF Price Assessment Month +4 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 154.088},
                       'TTF Price Assessment Month +5 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 153.25},
                       'TTF Price Assessment Month +6 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 147.45},
                       'TTF Price Assessment Quarter +1 Bid/Offer Range Daily (Mid) : EUR/MWh': {
                           '21-Oct-2022': 152.863},
                       'TTF Price Assessment Quarter +2 Bid/Offer Range Daily (Mid) : EUR/MWh': {
                           '21-Oct-2022': 147.375},
                       'TTF Price Assessment Quarter +3 Bid/Offer Range Daily (Mid) : EUR/MWh': {
                           '21-Oct-2022': 148.725},
                       'TTF Price Assessment Quarter +4 Bid/Offer Range Daily (Mid) : EUR/MWh': {
                           '21-Oct-2022': 147.738},
                       'TTF Price Assessment Quarter +10 Bid/Offer Range Daily (Mid) : EUR/MWh': {
                           '21-Oct-2022': 73.763},
                       'TTF Price Assessment Quarter +5 Bid/Offer Range Daily (Mid) : EUR/MWh': {
                           '21-Oct-2022': 140.113},
                       'TTF Price Assessment Quarter +6 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 103.6},
                       'TTF Price Assessment Quarter +7 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 98.6},
                       'TTF Price Assessment Quarter +8 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 99.225},
                       'TTF Price Assessment Quarter +9 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 98.975},
                       'TTF Price Assessment Year +1 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 149.175},
                       'TTF Price Assessment Year +2 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 110.388},
                       'TTF Price Assessment Year +3 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 77.9},
                       'TTF Price Assessment Year +4 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 46.788},
                       'TTF Price Assessment Season +1 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 148.05},
                       'TTF Price Assessment Season +10 Bid/Offer Range Daily (Mid) : EUR/MWh':
                           {'21-Oct-2022': float('nan')},
                       'TTF Price Assessment Season +2 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 143.925},
                       'TTF Price Assessment Season +3 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 101.1},
                       'TTF Price Assessment Season +4 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 99.1},
                       'TTF Price Assessment Season +5 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 70.475},
                       'TTF Price Assessment Season +6 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 69.775},
                       'TTF Price Assessment Season +7 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 39.375},
                       'TTF Price Assessment Season +8 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 39.775},
                       'TTF Price Assessment Season +9 Bid/Offer Range Daily (Mid) : EUR/MWh':
                           {'21-Oct-2022': float('nan')},
                       'PSV Price Assessment Gasyear +1 Bid/Offer Range Daily (Mid) : EUR/MWh': {
                           '21-Oct-2022': 125.288},
                       'PSV Price Assessment Season +1 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 146.25},
                       'PSV Price Assessment Season +2 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 145.725},
                       'PSV Price Assessment Season +3 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 104.838},
                       'PSV Price Assessment Year +1 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 147.738},
                       'PSV Price Assessment Year +2 Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 111.313},
                       'TTF Price Assessment Balance of Month (BOM) Heren Bid/Offer Range Daily (Mid) : EUR/MWh':
                           {'21-Oct-2022': 50},
                       'TTF Price Assessment Day-Ahead Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 37.775},
                       'TTF Price Assessment Weekend Bid/Offer Range Daily (Mid) : EUR/MWh': {'21-Oct-2022': 33.913}})
    for index, row in df_righe_mancanti.iterrows():
        df.loc[index] = row
    return df

