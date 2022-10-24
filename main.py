from Grattugia import *
import KTE_artesian as kta
import logging
import datetime as dt

# Scarico la curva del rumore random
noise_curve = kta.get_artesian_data_actual([100239002], '2020-01-01',
                                           '2050-01-01', 'd')
# Assegno la data come index della curva in modo da recuperare rapidamente il rumore corretto
noise_curve = kta.format_artesian_data(noise_curve).set_index('Date')

# Configuro il file di log
logging.basicConfig(filename='log.txt', level=logging.INFO)
logging.info('Run del ' + dt.datetime.today().strftime('%y-%m-%d'))
logging.info('Start')

# Chiamo la funzione di scaricamento dei dati dal sito
download_heren_data()
logging.info('Dati scaricati')

# Leggo i dati dai file excel e li trasformo in un dataframe Pandas
df_heren = read_heren_data()
logging.info('Dati importati nel programma correttamente')
df_heren = correggi_assenza_dato(df_heren)

# Assegno alla futura curva spot i day-ahead
df_spot_ttf = make_curva_spot(df_heren, 'TTF Price Assessment Day-Ahead Bid/Offer Range Daily (Mid) : EUR/MWh')
# Concludo la creazione della curva spot aggiungendo i dati del weekend
df_spot_ttf = fill_holidays(df_spot_ttf, df_heren, 'TTF Price Assessment Weekend Bid/Offer Range Daily (Mid) : EUR/MWh')
logging.info('Creata curva spot ttf')


# Assegno alla futura curva spot i day-ahead
df_spot_psv = make_curva_spot(df_heren, 'PSV Price Assessment Day-Ahead Bid/Offer Range Daily (Mid) : EUR/MWh')
# Concludo la creazione della curva spot aggiungendo i dati del weekend
df_spot_psv = fill_holidays(df_spot_psv, df_heren, 'PSV Price Assessment Weekend Bid/Offer Range Weekly (Mid) : EUR/MWh')
logging.info('Creata curva spot psv')

# Sporco i dati delle curve con il rumore della curva noise_curve
df_spot_psv['To artesian'] = df_spot_psv.apply(lambda x: add_noise(x, 'PSV Price Assessment Day-Ahead Bid/Offer Range Daily (Mid) : EUR/MWh', noise_curve), axis=1)
# Sporco i dati delle curve con il rumore della curva noise_curve

df_spot_ttf['To artesian'] = df_spot_ttf.apply(lambda x: add_noise(x, 'TTF Price Assessment Day-Ahead Bid/Offer Range Daily (Mid) : EUR/MWh', noise_curve), axis=1)
logging.info('Aggiunta col onna per il caricamento su artesian')

# Creo il dizionario per il caricamento dei dati su Artesian
dict_artesian_ttf = make_artesian_dict(df_spot_ttf.fillna(0), 'To artesian')
# Creo il dizionario per il caricamento dei dati su Artesian
dict_artesian_psv = make_artesian_dict(df_spot_psv.fillna(0), 'To artesian')
logging.info('Creato dizionario per il caricamento su artesian')

# Divido il dataframe con tutti i dati in 2 dataframe, df psv e df ttf, creando la Market Assesment con i prodotti fw
df_psv, df_ttf = split_psv_ttf(df_heren)

# Sporco con del rumore le curve forward
for column in list(df_psv.columns):
    df_psv[column] = df_psv.apply(lambda x: add_noise(x, column, noise_curve), axis=1)

# Sporco con del rumore le curve forward
for column in list(df_ttf.columns):
    df_ttf[column] = df_ttf.apply(lambda x: add_noise(x, column, noise_curve), axis=1)

# Creo il dizionario per il caricamento dei dati su Artesian
dict_artesian_fw_psv = get_market_assestments_artesian_dict(df_psv)
# Creo il dizionario per il caricamento dei dati su Artesian
dict_artesian_fw_ttf = get_market_assestments_artesian_dict(df_ttf)

# Caricamento su Artesian
kta.post_artesian_market_assestments_daily(dict_artesian_fw_psv, dict(), 'KtE', 'Forward price gas PSV')
kta.post_artesian_market_assestments_daily(dict_artesian_fw_ttf, dict(), 'KtE', 'Forward price gas TTF')

# Caricamento su Artesian
kta.post_artesian_actual_time_series_daily(dict_artesian_psv, dict(), 'KtE', 'Spot price gas PSV')
kta.post_artesian_actual_time_series_daily(dict_artesian_ttf, dict(), 'KtE', 'Spot price gas TTF')
logging.info('Dati caricati')
logging.info('Fine processo \n\n')
