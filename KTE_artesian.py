from datetime import datetime
import numpy as np
from Artesian import Granularity, ArtesianConfig
from Artesian import MarketData
from Artesian.MarketData import MarketDataService
from Artesian.Query import QueryService
from dateutil import tz
import pytz
import KTE_time as tempo
import settings_and_imports as setting
import pandas as pd


# Classe atta a contenere i valori per il filling delle curve bid ask
class FillerValueBidAsk:
    def __init__(self, best_bid_price=0, best_ask_price=0, best_bid_quantity=0,
                 best_ask_quantity=0, last_price=0, last_quantity=0):
        self.best_bid_price = best_bid_price
        self.best_ask_price = best_ask_price
        self.best_bid_quantity = best_bid_quantity
        self.best_ask_quantity = best_ask_quantity
        self.last_price = last_price
        self.last_quantity = last_quantity


# Classe atta a contenere i valori per il filling delle curve market assessment
class FillerValueMarketAssessment:
    def __init__(self, settlement=0, apertura=0, close=0, high=0, low=0, volume_paid=0, volume_given=0, volume=0):
        self.settlement = settlement
        self.apertura = apertura
        self.close = close
        self.high = high
        self.low = low
        self.volume_paid = volume_paid
        self.volume_given = volume_given
        self.volume = volume


# In via di deprecamento
def format_artesian_data(df_artesian):
    list_of_rows_names = df_artesian['C'].unique().tolist()
    dizionario_generatore_dataframe = dict()
    for i in df_artesian.index:
        try:
            dizionario_generatore_dataframe[df_artesian.loc[i]['T']][df_artesian.loc[i]['C']] = df_artesian.loc[i]['D']
        except KeyError:
            dizionario_generatore_dataframe[df_artesian.loc[i]['T']] = dict()
            dizionario_generatore_dataframe[df_artesian.loc[i]['T']][df_artesian.loc[i]['C']] = df_artesian.loc[i]['D']

    lista_generatrice_df = list()
    i = 0
    for k1 in dizionario_generatore_dataframe.keys():
        lista_tmp = list()
        lista_tmp.append(k1)
        for k2 in list_of_rows_names:
            try:
                lista_tmp.append(dizionario_generatore_dataframe[k1][k2])
            except KeyError:
                lista_tmp.append(0)
        lista_generatrice_df.append(lista_tmp)
    df = pd.DataFrame(lista_generatrice_df, columns=['Date'] + list_of_rows_names)
    df['Date'] = df['Date'].apply((lambda data: tempo.string_to_datetime(data, '%Y-%m-%dT%H:%M:%S%z')))
    return df


# Rende l'oggetto che autentica e configura la connessione con artesian
def get_configuration():
    return ArtesianConfig(setting.Settings.URL_SERVER, setting.Settings.API_KEY)


def get_value_as_series(x):
    return x


# Cancella tutte le curve i cui id sono contenuti nell'array d'input
def delete_curve(arr_cureve_id):
    cfg = get_configuration()
    mkservice = MarketDataService(cfg)

    for c_id in arr_cureve_id:
        mkservice.deleteMarketData(c_id)


# Costruisce il curve name della curva secondo le direttive interne
def curve_name_builder(tipologia="", sotto_tipologia="", paese="", oggetto="", censimp="", mercato=""):
    nome_curva = ""

    if tipologia != "":
        nome_curva = tipologia
    if sotto_tipologia != "":
        nome_curva += " " + sotto_tipologia
    if paese != "":
        nome_curva += " " + paese
    if oggetto != "":
        nome_curva += " " + oggetto
    if censimp != "":
        nome_curva += " " + censimp
    if mercato != "":
        nome_curva += " " + mercato

    return nome_curva


# Rende l'oggetto granularity desiderato
def get_granularity(string_granularity):
    if string_granularity == 'd':
        return Granularity.Day
    if string_granularity == 'm':
        return Granularity.Minute
    if string_granularity == 'M':
        return Granularity.Month
    if string_granularity == 'f':
        return Granularity.FifteenMinute
    if string_granularity == 'h':
        return Granularity.Hour
    if string_granularity == 'q':
        return Granularity.Quarter
    if string_granularity == 't':
        return Granularity.TenMinute
    if string_granularity == 'T':
        return Granularity.ThirtyMinute
    if string_granularity == 'w':
        return Granularity.Week
    if string_granularity == 'y':
        return Granularity.Year


#######################################################################
#######################################################################
# '''''''''''''''''''  Versioned Time Series   ''''''''''''''''''''''''
#######################################################################
#######################################################################


###################################################
# ''''''''''''''''''   GET  '''''''''''''''''''''''
###################################################


def get_artesian_data_versioned(arr_id_curva, str_data_inizio_estrazione, str_data_fine_estrazione,
                                ganularity='h', time_zone='CET'):
    cfg = get_configuration()
    qs = QueryService(cfg)
    return qs.createVersioned() \
        .forMarketData(arr_id_curva) \
        .inAbsoluteDateRange(str_data_inizio_estrazione, str_data_fine_estrazione) \
        .inTimeZone(time_zone) \
        .inGranularity(get_granularity(ganularity))



###################################################
# '''''''''''''''   Formatter  ''''''''''''''''''''
###################################################

# La funzione rende il dizionario utile al caricamento dei dati su Artesian.
# Il parametro column_name contiene il nome della colonna che si desidera caricare come valore della time series.
# Il parametro column_date_name contiene il nome della colonna che contiene la data.
def get_artesian_dict_versioned(df, column_name, column_date_name, correttore=False):
    cet = pytz.timezone('CET')
    tempi = df.apply(lambda x: tempo.format_date(x[column_date_name]), axis=1).tolist()
    values = df.apply(lambda x: get_value_as_series(x[column_name]), axis=1).values.tolist()
    dict_of_value_to_send = dict()
    for i in range(len(tempi)):
        giorno_ora = datetime(tempi[i].year, tempi[i].month, tempi[i].day, tempi[i].hour, 0)
        if correttore:
            correttore = giorno_ora.astimezone(cet).utcoffset()
            giorno_ora = giorno_ora - correttore
        dict_of_value_to_send[giorno_ora] = values[i]
    return dict_of_value_to_send


# Rende un dizionario per il caricamento dati su Artesian ma con granularità mensile
def get_artesian_dict_versioned_monthly_or_daily(df, column_name, column_date_name):
    tempi = df.apply(lambda x: tempo.format_date(x[column_date_name]), axis=1).tolist()
    values = df.apply(lambda x: get_value_as_series(x[column_name]), axis=1).values.tolist()
    dict_of_value_to_send = dict()
    for i in tempi:
        giorno_ora = datetime(tempi[i].year, tempi[i].month, 1, 0)
        dict_of_value_to_send[giorno_ora] = values[i]
    return dict_of_value_to_send


def get_artesian_dict_versioned_daily_by_index(df, column_name):
    dict_artesian = dict()
    for index, row in df.iterrows():
        if np.isnan(row[column_name]):
            dict_artesian[datetime(index.year, index.month, index.day)] = np.nan
        else:
            dict_artesian[datetime(index.year, index.month, index.day)] = row[column_name]
    return dict_artesian


###################################################
# ''''''''''''''''''  POST  '''''''''''''''''''''''
###################################################


def post_artesian_versioned_time_series(data, dict_of_tags, provider, curve_name, string_granularity, version=datetime.now(), original_timezone='CET'):
    cfg = get_configuration()
    gran = get_granularity(string_granularity)

    mkservice = MarketData.MarketDataService(cfg)

    mkdid = MarketData.MarketDataIdentifier(provider, curve_name)
    mkd = MarketData.MarketDataEntityInput(
        providerName=mkdid.provider,
        marketDataName=mkdid.name,
        originalGranularity=gran,
        type=MarketData.MarketDataType.VersionedTimeSerie,
        originalTimezone=original_timezone,
        tags=dict_of_tags
    )

    registered = mkservice.readMarketDataRegistryByName(mkdid.provider, mkdid.name)

    if registered is None:
        mkservice.registerMarketData(mkd)

    data = MarketData.UpsertData(mkdid, 'UTC',
                                 rows=data,
                                 version=version,
                                 downloadedAt=datetime.now(tz=tz.UTC)
                                 )

    mkservice.upsertData(data)


def post_artesian_versioned_time_series_hourly(data, dict_of_tags, provider, curve_name, version=datetime.now()):
    cfg = get_configuration()

    mkservice = MarketData.MarketDataService(cfg)

    mkdid = MarketData.MarketDataIdentifier(provider, curve_name)
    mkd = MarketData.MarketDataEntityInput(
        providerName=mkdid.provider,
        marketDataName=mkdid.name,
        originalGranularity=Granularity.Hour,
        type=MarketData.MarketDataType.VersionedTimeSerie,
        originalTimezone="CET",
        tags=dict_of_tags
    )

    registered = mkservice.readMarketDataRegistryByName(mkdid.provider, mkdid.name)

    if registered is None:
        registered = mkservice.registerMarketData(mkd)

    data = MarketData.UpsertData(mkdid, 'CET',
                                 rows=data,
                                 version=version,
                                 downloadedAt=datetime.now(tz=tz.UTC)
                                 )

    mkservice.upsertData(data)


#######################################################################
#######################################################################
# ''''''''''''''''''''  Actual Time Series   ''''''''''''''''''''''''''
#######################################################################
#######################################################################


###################################################
# ''''''''''''''''''   GET  '''''''''''''''''''''''
###################################################

def get_artesian_data_actual(arr_id_curva,
                      str_data_inizio_estrazione,
                      str_data_fine_estrazione,
                      ganularity='h', time_zone='CET'):
    cfg = get_configuration()
    gran = get_granularity(ganularity)

    qs = QueryService(cfg)
    data = qs.createActual() \
        .forMarketData(arr_id_curva) \
        .inAbsoluteDateRange(str_data_inizio_estrazione, str_data_fine_estrazione) \
        .inTimeZone(time_zone) \
        .inGranularity(gran) \
        .execute()

    df_curva = pd.DataFrame(data)
    return df_curva


def get_artesian_data_actual_daily(arr_id_curva, str_data_inizio_estrazione, str_data_fine_estrazione):
    cfg = get_configuration()

    qs = QueryService(cfg)

    data = qs.createActual() \
        .forMarketData(arr_id_curva) \
        .inAbsoluteDateRange(str_data_inizio_estrazione, str_data_fine_estrazione) \
        .inTimeZone("CET") \
        .inGranularity(Granularity.Day) \
        .execute()

    df_curva = pd.DataFrame(data)
    return df_curva


###################################################
##################   Formatter  ###################
###################################################

# Il dataframe passato come input deve avere l'index ti tipo Pandas Timestamp, il parametro colonna è il nome della
# colonna che si vuole usare come valore della time series
def make_artesian_dict_actual(df, colonna):
    dict_artesian = dict()
    for index, row in df.iterrows():
        dict_artesian[datetime(index.year, index.month, index.day, index.hour, index.minute)] = row[colonna]
    return dict_artesian

###################################################
# '''''''''''''''''''  POST  ''''''''''''''''''''''
###################################################


def post_artesian_actual_time_series_daily(data, dict_of_tags, provider, curve_name):
    cfg = get_configuration()

    mkservice = MarketData.MarketDataService(cfg)

    mkdid = MarketData.MarketDataIdentifier(provider, curve_name)
    mkd = MarketData.MarketDataEntityInput(
        providerName=mkdid.provider,
        marketDataName=mkdid.name,
        originalGranularity=Granularity.Day,
        type=MarketData.MarketDataType.ActualTimeSerie,
        originalTimezone="CET",
        tags=dict_of_tags
    )

    registered = mkservice.readMarketDataRegistryByName(mkdid.provider, mkdid.name)
    if (registered is None):
        registered = mkservice.registerMarketData(mkd)

    data = MarketData.UpsertData(mkdid, 'CET',
                                 rows=data,
                                 downloadedAt=datetime.now().replace(tzinfo=tz.UTC)
                                 )

    mkservice.upsertData(data)


def post_artesian_actual_time_series_monthly(data, dict_of_tags, provider, curve_name):
    cfg = get_configuration()

    mkservice = MarketData.MarketDataService(cfg)

    mkdid = MarketData.MarketDataIdentifier(provider, curve_name)
    mkd = MarketData.MarketDataEntityInput(
        providerName=mkdid.provider,
        marketDataName=mkdid.name,
        originalGranularity=Granularity.Month,
        type=MarketData.MarketDataType.ActualTimeSerie,
        originalTimezone="CET",
        tags=dict_of_tags
    )

    registered = mkservice.readMarketDataRegistryByName(mkdid.provider, mkdid.name)
    if (registered is None):
        registered = mkservice.registerMarketData(mkd)

    data = MarketData.UpsertData(mkdid, 'CET',
                                 rows=data,
                                 downloadedAt=datetime.now(tz=tz.UTC)
                                 )

    mkservice.upsertData(data)


def post_artesian_actual_time_series(data, dict_of_tags, provider, curve_name, string_granularity):
    cfg = get_configuration()

    mkservice = MarketData.MarketDataService(cfg)

    mkdid = MarketData.MarketDataIdentifier(provider, curve_name)
    mkd = MarketData.MarketDataEntityInput(
        providerName=mkdid.provider,
        marketDataName=mkdid.name,
        originalGranularity=get_granularity(string_granularity),
        type=MarketData.MarketDataType.ActualTimeSerie,
        originalTimezone="CET",
        tags=dict_of_tags
    )

    registered = mkservice.readMarketDataRegistryByName(mkdid.provider, mkdid.name)
    if (registered is None):
        registered = mkservice.registerMarketData(mkd)

    data = MarketData.UpsertData(mkdid, 'CET',
                                 rows=data,
                                 downloadedAt=datetime.now(tz=tz.UTC)
                                 )

    mkservice.upsertData(data)

#######################################################################
#######################################################################
# '''''''''''''''  MarketAssesments Time Series   '''''''''''''''''''''
#######################################################################
#######################################################################

###################################################
# ''''''''''''''''''   GET  '''''''''''''''''''''''
###################################################


def get_artesian_data_market_assesment(arr_id_curva,
                                       str_data_inizio_estrazione,
                                       str_data_fine_estrazione,
                                       arr_products,
                                       filler_str='no',
                                       filler_value=0):
    if filler_str == 'no':
        return get_artesian_data_market_assesment_with_no_fill(arr_id_curva,
                                                               str_data_inizio_estrazione,
                                                               str_data_fine_estrazione,
                                                               arr_products)
    if filler_str == 'last':
        return get_artesian_data_market_assesment_fill_latest_value(arr_id_curva,
                                                                    str_data_inizio_estrazione,
                                                                    str_data_fine_estrazione,
                                                                    arr_products)
    if filler_str == 'custom':
        if type(filler_value) == 'int':
            return get_artesian_data_market_assesment_fill_custom_value(arr_id_curva,
                                                                        str_data_inizio_estrazione,
                                                                        str_data_fine_estrazione,
                                                                        arr_products)
        else:
            return get_artesian_data_market_assesment_fill_custom_value(arr_id_curva,
                                                                        str_data_inizio_estrazione,
                                                                        str_data_fine_estrazione,
                                                                        arr_products,
                                                                        settlement=filler_value.settlement,
                                                                        apertura=filler_value.apertura,
                                                                        close=filler_value.close,
                                                                        high=filler_value.high,
                                                                        low=filler_value.low,
                                                                        volume_paid=filler_value.volume_paid,
                                                                        volume_given=filler_value.volume_given,
                                                                        volume=filler_value.volume)
    if filler_str == 'none':
        return get_artesian_data_market_assesment_fill_with_none(arr_id_curva,
                                                                 str_data_inizio_estrazione,
                                                                 str_data_fine_estrazione,
                                                                 arr_products)


def get_artesian_data_market_assesment_fill_latest_value(arr_id_curva,
                                                         str_data_inizio_estrazione,
                                                         str_data_fine_estrazione,
                                                         arr_products):
    cfg = get_configuration()

    qs = QueryService(cfg)
    data = qs.createMarketAssessment() \
        .forMarketData(arr_id_curva) \
        .forProducts(arr_products) \
        .inAbsoluteDateRange(str_data_inizio_estrazione, str_data_fine_estrazione) \
        .withFillLatestValue("P5D") \
        .execute()

    df_curva = pd.DataFrame(data)
    df_curva = df_curva.rename(columns={
        'P': 'Provider',
        'N': 'Curve name',
        'PR': 'Prodotto',
        'T': 'Data',
        'S': 'Settlement',
        'O': 'Open',
        'C': 'Close',
        'H': 'High',
        'L': 'Low',
        'VT': 'Volume scambiato'
    })

    return df_curva


def get_artesian_data_market_assesment_fill_custom_value(arr_id_curva,
                                                         str_data_inizio_estrazione,
                                                         str_data_fine_estrazione,
                                                         arr_products,
                                                         settlement=0,
                                                         apertura=0,
                                                         close=0,
                                                         high=0,
                                                         low=0,
                                                         volume_paid=0,
                                                         volume_given=0,
                                                         volume=0):
    cfg = get_configuration()

    qs = QueryService(cfg)
    data = qs.createMarketAssessment() \
        .forMarketData(arr_id_curva) \
        .forProducts(arr_products) \
        .inAbsoluteDateRange(str_data_inizio_estrazione, str_data_fine_estrazione) \
        .withFillCustomValue(settlement=settlement,
                             apertura=apertura,
                             close=close,
                             high=high,
                             low=low,
                             volume_paid=volume_paid,
                             volume_given=volume_given,
                             volume=volume)\
        .execute()

    df_curva = pd.DataFrame(data)
    df_curva = df_curva.rename(columns={
        'P': 'Provider',
        'N': 'Curve name',
        'PR': 'Prodotto',
        'T': 'Data',
        'S': 'Settlement',
        'O': 'Open',
        'C': 'Close',
        'H': 'High',
        'L': 'Low',
        'VT': 'Volume scambiato'
    })

    return df_curva


def get_artesian_data_market_assesment_with_no_fill(arr_id_curva,
                                                    str_data_inizio_estrazione,
                                                    str_data_fine_estrazione,
                                                    arr_products):
    cfg = get_configuration()

    qs = QueryService(cfg)
    data = qs.createMarketAssessment() \
        .forMarketData(arr_id_curva) \
        .forProducts(arr_products) \
        .inAbsoluteDateRange(str_data_inizio_estrazione, str_data_fine_estrazione) \
        .withFillNone()\
        .execute()

    df_curva = pd.DataFrame(data)
    df_curva = df_curva.rename(columns={
        'P': 'Provider',
        'N': 'Curve name',
        'PR': 'Prodotto',
        'T': 'Data',
        'S': 'Settlement',
        'O': 'Open',
        'C': 'Close',
        'H': 'High',
        'L': 'Low',
        'VT': 'Volume scambiato'
    })

    return df_curva


def get_artesian_data_market_assesment_fill_with_none(arr_id_curva,
                                                      str_data_inizio_estrazione,
                                                      str_data_fine_estrazione,
                                                      arr_products):
    cfg = get_configuration()

    qs = QueryService(cfg)
    data = qs.createMarketAssessment() \
        .forMarketData(arr_id_curva) \
        .forProducts(arr_products) \
        .inAbsoluteDateRange(str_data_inizio_estrazione, str_data_fine_estrazione) \
        .withFillNull()\
        .execute()

    df_curva = pd.DataFrame(data)
    df_curva = df_curva.rename(columns={
        'P': 'Provider',
        'N': 'Curve name',
        'PR': 'Prodotto',
        'T': 'Data',
        'S': 'Settlement',
        'O': 'Open',
        'C': 'Close',
        'H': 'High',
        'L': 'Low',
        'VT': 'Volume scambiato'
    })

    return df_curva

###################################################
# ''''''''''''''   Formatter  '''''''''''''''''''''
###################################################


# Funzione per la creazione del dizionario per il caricamento di curve di tipo Market Assessment
# Questa funzione permette di caricare un solo valore nel campo settlement.
# Il parametro codifica_colonna è la funzione per la codifica dei nomi dei prodotti dal
# formato contenuto nel dataframe al formato funzionale al caricamento delle curve su Artesian
def make_artesian_dict_market_assesment_settlement(df, codifica_colonna):
    dict_of_market_assestments = dict()
    for index, row in df.iterrows():
        list_of_column_names = row.index.to_list()
        giorno_tmp = datetime(index.year, index.month, index.day)
        dict_of_market_assestments[giorno_tmp] = dict()
        for name in list_of_column_names:
            codifica = codifica_colonna(name, index)
            if codifica != 'x':
                dict_of_market_assestments[giorno_tmp][codifica] = MarketData.MarketAssessmentValue(
                    settlement=row[name])

    return dict_of_market_assestments


# Passare la data come Pandas Timestamp e l'offset come intero
# ES. codifica_month(Pandas_Timestamp(maggio 2021), 1) --> Jun-21
def codifica_month(data, offset):
    data = data + pd.DateOffset(months=offset)
    mese = datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%b')
    return mese + '-' + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')


# Passare la data come Pandas Timestamp e l'offset come intero
# ES. codifica_season(Pandas_Timestamp(maggio 2021), 1) --> Sum-21
def codifica_season(data, offset):
    data = data + pd.DateOffset(months=offset * 6)
    if data.month < 4 or data.month > 9:
        return 'Win-' + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')
    else:
        return 'Sum-' + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')


# Passare la data come Pandas Timestamp e l'offset come intero
# ES. codifica_quarter(Pandas_Timestamp(maggio 2021), 1) --> Q321
def codifica_quarter(data, offset):
    data = data + pd.DateOffset(months=offset * 3)
    return 'Q' + str(data.quarter) + datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%y')


# Passare la data come Pandas Timestamp e l'offset come intero
# ES. codifica_year(Pandas_Timestamp(maggio 2021), 1) --> 2022
def codifica_year(data, offset):
    data = data + pd.DateOffset(years=offset)
    return datetime.datetime(year=data.year, month=data.month, day=data.day).strftime('%Y')


###################################################
# '''''''''''''''''''  POST  ''''''''''''''''''''''
###################################################

def post_artesian_market_assestments_daily(data, dict_of_tags, provider, curve_name):
    cfg = get_configuration()
    mkservice = MarketData.MarketDataService(cfg)

    mkdid = MarketData.MarketDataIdentifier(provider, curve_name)
    mkd = MarketData.MarketDataEntityInput(
        providerName=mkdid.provider,
        marketDataName=mkdid.name,
        originalGranularity=Granularity.Day,
        type=MarketData.MarketDataType.MarketAssessment,
        originalTimezone="CET",
        tags=dict_of_tags
    )

    registered = mkservice.readMarketDataRegistryByName(mkdid.provider, mkdid.name)
    if (registered is None):
        registered = mkservice.registerMarketData(mkd)

    marketAssessment = MarketData.UpsertData(MarketData.MarketDataIdentifier(provider, curve_name), 'CET',
                                             marketAssessment=data,
                                             downloadedAt=datetime.now().replace(tzinfo=tz.UTC)
                                             )

    mkservice.upsertData(marketAssessment)

