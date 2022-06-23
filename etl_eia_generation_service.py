# -*- coding: utf-8 -*-
"""
Created on Thu Jun 23 19:24:59 2022
@author: MM
"""


import os
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from bs4 import BeautifulSoup
import requests
import logging



#================================= INPUT =================================
username = 'db_user'
password = 'user_password'
schema_name = 'stage'
table_name = 't_eia_generatino_etl'
db_name = 'QA'     #select QA/PROD/MANAGEMENT
truncate = 0        #if table should be truncated before inserting
#================================= INPUT =================================

save_folder = f"/home/ubuntu/scrape_files/eia_generation/{datetime.strftime(datetime.now(), '%Y/%b').upper()}"
today = date.today()
upload_date = today.strftime("%Y-%m-%d")


if not os.path.exists(save_folder):
    os.makedirs(save_folder)


if db_name.upper() == 'QA':
    host_name = 'some.of.host.name.com'
elif db_name.upper() == 'PROD':
    host_name = 'some.of.host.name.com'
elif db_name.upper() == 'MANAGEMENT':
    host_name = 'some.of.host.name.com' 

engine = create_engine('mysql+mysqlconnector://' + username + ':' + password + '@' + host_name + ':3306/' + schema_name)
  
conn = engine.connect()

try:
    db_next_date = conn.execute("SELECT MAX(next_date) FROM stage.t_eia_file_scheduler WHERE new_upload_status = 0;").fetchall()

    db_next_date = str(db_next_date[0])
    db_next_date = f"{db_next_date[15:-3].split(',')[1].replace(' ', '')}/{db_next_date[15:-3].split(',')[2].replace(' ', '')}/{db_next_date[15:-3].split(',')[0].replace(' ', '')}"
    db_next_date = datetime.strptime(db_next_date, '%m/%d/%Y').strftime("%Y-%m-%d")

    # ----------------------------------------
    #  ____Download the file from website____  
    # ----------------------------------------

    if datetime.strftime(datetime.now(), '%Y-%m-%d') >= db_next_date:

        url = "https://www.eia.gov/electricity/data/eia860M/"
        domain = "https://www.eia.gov"
                
        response = requests.get(url)
        soup = BeautifulSoup(response.content,'html.parser')
        
        logging.info("Getting to website...")
        
        release_date_raw = soup.find_all("span", class_ = "date")[0].string
        next_date_raw = soup.find_all("span", class_ = "date")[1].string

        release_date = datetime.strptime(release_date_raw, '%B %d, %Y')
        release_date = release_date.strftime('%Y-%m-%d')

        next_date = datetime.strptime(next_date_raw, '%B %d, %Y')
        next_date = next_date.strftime('%Y-%m-%d')
        logging.info("Dates completly parsed")

        logging.info("Next date from DB reached!")
        tables = soup.find_all("table", {"class": "basic-table full-width"})
        
        
        l = []
        h = []
        for tr in tables:
            td = tr.find_all('td')
            row1 = [tr.text for tr in td]
            h.append(row1)
            
            ahref = tr.find_all('a')
            row = [tr.get('href') for tr in ahref]
        
            l.append(row)
        
        links_table = l[0]
        date_list = h[0]
                
                
        file_to_download = domain + links_table[0]
        filename = file_to_download.split('xls/')[1]
        path_to_file = f"{save_folder}/{filename}"
        conn.execute("UPDATE monitoring.python_job SET data_downloaded = NOW() WHERE id = 29;")
        logging.warning(f"Found new file: {path_to_file}")
        
                
        print(f"\n\n newest file:  {filename}") 
        
        r = requests.get(file_to_download)
        with open(path_to_file, "wb") as f:
            f.write(r.content)
            
    
        spreadsheet_list = ["Operating" , "Planned" , "Retired" , "Canceled or Postponed"]
        table_list = {"Operating" : "t_eia_operating", 
                            "Planned" : "t_eia_planned", 
                            "Retired" : "t_eia_retired", 
                            "Canceled or Postponed" : "t_eia_canceled_postponed"}
        
                # +++  value of file_month  +++
        filename_split_month = date_list[0].split(sep = ' ')[0][:3].upper()
        filename_split_year = date_list[0].split(sep = ' ')[1]
        file_month = f"{filename_split_year}_{filename_split_month}"
                # +++  value of file_month  +++
        logging.info(f"Matched a file month: {file_month}")
        try:
            logging.info("Start processing...")
            for sheet in spreadsheet_list:
                
                # ----------------------------------------
                #  ____Transform data to valid format____  
                # ---------------------------------------- 
                
                logging.info(f"Sheet name: {sheet}")
                df = pd.DataFrame(pd.read_excel(path_to_file, sheet_name = sheet, skiprows = 2, skipfooter = 2))
                df.drop(columns=['Google Map', 'Bing Map'], axis=1, inplace=True)
                df = df.replace(np.nan, '', regex=True)
                
                table_name = [tab for (sht, tab) in table_list.items() if sheet == sht]
                
                
                df.insert(0, 'File_Month', [file_month for i in range(df.shape[0])])
                df['Upload_date'] = [upload_date for i in range(df.shape[0])]
                
                df.columns = [col.replace(' ','_').replace('(','').replace(')','') for col in df.columns]
                
                conn.execute("UPDATE monitoring.python_job SET data_processed = NOW() WHERE id = 29;")
                
                # ----------------------------------------
                #  ____Upload data to database____  
                # ---------------------------------------- 
                
                try:
                    df.to_sql(name=table_name[0], con=conn, schema=schema_name, if_exists='append', index=False, chunksize = 1000)
                    logging.info("Data has been uploaded to {table_name[0]}.")
    
                except SQLAlchemyError as err:
                    logging.warning(f"DB error: {err}")
                    pass
                finally:
                    conn.execute("UPDATE monitoring.python_job SET data_inserted = NOW() WHERE id = 29;")
                    
            try:
                logging.warning("Start - procedure comparing changes")
                conn.execute("CALL stage.sp_eia_operating_upload_change();")
                
                conn.execute("CALL stage.sp_eia_planned_upload_change();")
                
                conn.execute("CALL stage.sp_eia_retired_upload_change();")
                
                conn.execute("CALL stage.sp_eia_canceled_postponed_upload_change();")
            
            except Exception as e:
                logging.error('Error rexecuting procedures\n' + str(e))
                    
                    
        except Exception as e:
            logging.error('Error uploading table\n' + str(e))
        finally:
            conn.execute(f"UPDATE stage.t_eia_file_scheduler SET new_upload_status = 1 WHERE new_upload_status = 0 AND next_date = '{release_date}';")
            conn.execute(f"INSERT INTO stage.t_eia_file_scheduler (file_month, file_name, upload_date, next_date, new_upload_status) VALUES ('{file_month}', '{filename}', '{release_date}', '{next_date}', 0);")
            conn.execute(f"UPDATE monitoring.python_job SET summary = 'Downloaded {filename}' WHERE id = 29;")
            logging.info("Success - DB Connection has been closed.")
            conn.close()
           
        logging.info("Finished")   
        
    else:
        try:
            conn = engine.connect()
            monitor_sql = ("UPDATE monitoring.python_job SET summary = 'Date is not reached' WHERE id = 29;")
            conn.execute(text(monitor_sql))
        except SQLAlchemyError as err:
            logging.error(f"Date is not reached - {err}")
            pass
        finally:
            logging.warning("Not completed - date is not reached - DB Connection has been closed")
            conn.close()
        
except Exception as err:
    logging.error("Failure - date mismatch")
    try:
        conn = engine.connect()
        monitor_sql = (f"UPDATE monitoring.python_job SET summary = 'Check date failed!', aux = {err} WHERE id = 29;")
        conn.execute(text(monitor_sql))
    except SQLAlchemyError:
        pass
    finally:
        conn.close()