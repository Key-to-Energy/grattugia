import os
import glob
import shutil
import datetime
import logging

logging.basicConfig(filename='log_mover.txt', level=logging.INFO)

user = 'Usr-GSE-2020'
download_path= 'C:\\Users\\' + user + '\\Downloads\\'

def move_file(download_path):
    full_path = os.path.join(download_path,
                             'ICIS_Dashboard_Price_History_' + datetime.datetime.now().strftime('%Y-%m-%d') + '*.xls')
    files = glob.glob(full_path)
    files.sort(key=os.path.getmtime, reverse=True)
    for file_path in files:
        file = os.path.basename(file_path)
        try:
            path_to = os.path.join("Y:\Abagas-G", datetime.datetime.now().strftime('%Y-%m-%d'))
            os.mkdir(path_to)
        except FileExistsError as e:
            print('Cartella già creata')
        finally:
            shutil.move(file_path, os.path.join(os.path.join("Y:\\Abagas-G", datetime.datetime.now().strftime('%Y-%m-%d')), file))

