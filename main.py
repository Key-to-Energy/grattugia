from Grattugia import *
import KTE_artesian as kta

download_heren_data()
df_heren = read_heren_data()

df_spot_ttf = make_curva_spot(df_heren, 'TTF Price Assessment Day-Ahead Bid/Offer Range Daily (Mid) : EUR/MWh')
df_spot_ttf = fill_holidays(df_spot_ttf, df_heren, 'TTF Price Assessment Weekend Bid/Offer Range Daily (Mid) : EUR/MWh')

df_spot_psv = make_curva_spot(df_heren, 'PSV Price Assessment Day-Ahead Bid/Offer Range Daily (Mid) : EUR/MWh')
df_spot_psv = fill_holidays(df_spot_psv, df_heren, 'PSV Price Assessment Weekend Bid/Offer Range Weekly (Mid) : EUR/MWh')

df_spot_psv['To artesian'] = df_spot_psv.apply(lambda x : add_noise(x['PSV Price Assessment Day-Ahead Bid/Offer Range Daily (Mid) : EUR/MWh']), axis=1)
df_spot_ttf['To artesian'] = df_spot_ttf.apply(lambda x : add_noise(x['TTF Price Assessment Day-Ahead Bid/Offer Range Daily (Mid) : EUR/MWh']), axis=1)

dict_artesian_ttf = make_artesian_dict(df_spot_ttf.fillna(0), 'To artesian')
dict_artesian_psv = make_artesian_dict(df_spot_psv.fillna(0), 'To artesian')

kta.post_artesian_actual_time_series_daily(dict_artesian_psv, dict(),'DevKtE','Spot price gas PSV')
kta.post_artesian_actual_time_series_daily(dict_artesian_ttf, dict(),'DevKtE','Spot price gas TTF')
