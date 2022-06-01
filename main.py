from Grattugia import *
import KTE_artesian as kta
import logging
import datetime as dt

logging.basicConfig(filename='log.txt', level=logging.INFO)
logging.info('Run del ' + dt.datetime.today().strftime('%y-%m-%d'))
logging.info('Start')
download_heren_data()
logging.info('Dati scaricati')
df_heren = read_heren_data()
logging.info('Dati importati nel programma correttamente')


df_spot_ttf = make_curva_spot(df_heren, 'TTF Price Assessment Day-Ahead Bid/Offer Range Daily (Mid) : EUR/MWh')
df_spot_ttf = fill_holidays(df_spot_ttf, df_heren, 'TTF Price Assessment Weekend Bid/Offer Range Daily (Mid) : EUR/MWh')
logging.info('Creata curva spot ttf')


df_spot_psv = make_curva_spot(df_heren, 'PSV Price Assessment Day-Ahead Bid/Offer Range Daily (Mid) : EUR/MWh')
df_spot_psv = fill_holidays(df_spot_psv, df_heren, 'PSV Price Assessment Weekend Bid/Offer Range Weekly (Mid) : EUR/MWh')
logging.info('Creata curva spot psv')


df_spot_psv['To artesian'] = df_spot_psv.apply(lambda x : add_noise(x['PSV Price Assessment Day-Ahead Bid/Offer Range Daily (Mid) : EUR/MWh']), axis=1)
df_spot_ttf['To artesian'] = df_spot_ttf.apply(lambda x : add_noise(x['TTF Price Assessment Day-Ahead Bid/Offer Range Daily (Mid) : EUR/MWh']), axis=1)
logging.info('Aggiunta col onna per il caricamento su artesian')

dict_artesian_ttf = make_artesian_dict(df_spot_ttf.fillna(0), 'To artesian')
dict_artesian_psv = make_artesian_dict(df_spot_psv.fillna(0), 'To artesian')
logging.info('Creato dizionario per il caricamento su artesian')

df_psv, df_ttf = split_psv_ttf(df_heren)

for column in list(df_psv.columns):
    df_psv[column] = df_psv.apply(
        lambda x: add_noise(x[column]), axis=1)

for column in list(df_ttf.columns):
    df_ttf[column] = df_ttf.apply(
        lambda x: add_noise(x[column]), axis=1)

dict_artesian_fw_psv = get_market_assestments_artesian_dict(df_psv)
dict_artesian_fw_ttf = get_market_assestments_artesian_dict(df_ttf)

kta.post_artesian_market_assestments_daily(dict_artesian_fw_psv,dict(),'DevKtE', 'Forward price gas PSV')
kta.post_artesian_market_assestments_daily(dict_artesian_fw_ttf,dict(),'DevKtE', 'Forward price gas TTF')

kta.post_artesian_actual_time_series_daily(dict_artesian_psv, dict(),'DevKtE','Spot price gas PSV')
kta.post_artesian_actual_time_series_daily(dict_artesian_ttf, dict(),'DevKtE','Spot price gas TTF')
logging.info('Dati caricati')
logging.info('Fine processo \n\n')