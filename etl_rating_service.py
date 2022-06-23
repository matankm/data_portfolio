# -*- coding: utf-8 -*-
"""
Created: 2021-03-21 21:17:09 
@author: MM
Description: DAG
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.hooks.base_hook import BaseHook



##### DAG CONFIG #####

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['test@exampledomain.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'execution_timeout': timedelta(seconds=1000)
}

with DAG(
    'AirflowETLService_1',
    default_args=default_args,
    description='AirflowETLService_1',
    schedule_interval='0 1 * * *', # everyday at 01:00 AM
    max_active_runs=1,
    start_date=datetime(2022, 1, 1, 0, 0, 0),
    tags=['airflow', 'etl', 'rating'],
    catchup=False
   ) as dag:
        
        def completed_process():
            import os
            import re
            from time import sleep
            from datetime import datetime, timedelta
            import pandas as pd
            import numpy as np
            import hashlib
            from selenium import webdriver
            #from selenium.webdriver.chrome.service import Service
            from selenium.webdriver.support.ui import WebDriverWait, Select
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.common.by import By
            from sqlalchemy import create_engine, text
            from sqlalchemy.orm import sessionmaker
            from sqlalchemy.exc import SQLAlchemyError
            import logging

            #================================= INPUT =================================
            username = 'db_user'
            password = 'user_password'
            schema_name = 'stage'
            table_name = 't_rating_etl_current'
            table_name_2 = 't_rating_etl_history'
            db_name = 'QA'     #select QA/PROD/MANAGEMENT
            truncate = 0        #if table should be truncated before inserting
            #================================= INPUT =================================
    
            conn_info = BaseHook.get_connection('SRV_DB_QA')
            conn_string = "mysql+mysqlconnector://{0}:{1}@{2}:{3}/miso".format(str(conn_info.login), str(conn_info.password), str(conn_info.host), str(conn_info.port))
            
            
            ###### ARGS #####
            save_folder = r"/home/ubuntu/scrape_files/Ratings/"
            
            
            if not os.path.exists(save_folder):
                os.makedirs(save_folder)
            
            
            options = webdriver.ChromeOptions()
            options.add_experimental_option('prefs', {'download.default_directory' : save_folder}) 
            options.add_argument("--headless") 
            options.add_argument("--disable-infobars");
            options.add_argument("--disable-notifications")
            options.add_argument("--disable-popup-blocking");
            driver = webdriver.Chrome("/usr/bin/chromedriver", options=options)
            driver.get("https://webmodel.misoenergy.org/WebTool/ViewModel.aspx?Position=Station")
            
            user_text = str(website_info.login)
            pass_text = str(website_info.password)
            
            devices = ["Line", "Zbr", "Transformer"]
            companies = [ "AEC", "AECI", "AEP", "ALTE", "ALTW", "AMIL", "AMMO", "AP", "BBA",
                        "BLKW", "BRAZ", "BREC", "BUBA", "CAJN", "CE", "CIN", "CLEC", "CONS", "CPLE", "CPLW", "CSWS", "CWAY", "CWLD",
                        "CWLP", "DECO", "DEOK", "DERS", "DEWO", "DLCO", "DPC", "DPL", "DUK", "EAI", "EDDY", "EDE", "EEI", "EES",
                        "EKPC", "EMBA", "ERCOTE", "ERCOTN", "FE", "GLH", "GLHB", "GRDA", "GRE", "HE", "HMPL", "INDN", "IPL", "IPRV",
                        "ITCI", "KACY", "KCPL", "LAFA", "LAGN", "LAMAR", "LEPA", "LES", "LGEE", "MDU", "MEC", "MGE", "MHEB",
                        "MH_ONT", "MIDW", "MISO", "MIUP", "MP", "MPS", "MPW", "MP_ONT", "NIPS", "NLR", "NPPD", "NSP", "NYISO",
                        "OKGE", "OMLP", "OMPA", "ONT", "ONT_MH", "ONT_MP", "OPPD", "OTP", "OVEC", "PJMC", "PLUM", "PSEC", "PSEG",
                        "PUPE", "SCEG", "SECI", "SERU", "SIGE", "SIPC", "SME", "SMP", "SOCO", "SPA", "SPC", "SPC_WA", "SPRM", "SPS",
                        "TVA", "UPPC", "VAP", "WAUE", "WA_SPC", "WEC", "WFEC", "WMU", "WPS", "WR", "YAD" ]
            
            text_to_hash= ''
            fieldSeparator = "^"
            upload_date = datetime.strftime(datetime.now(), '%Y-%m-%d %H:00:00')
            
            
            
            if db_name.upper() == 'QA':
                host_name = 'some.of.host.name.com'
            elif db_name.upper() == 'PROD':
                host_name = 'some.of.host.name.com'
            elif db_name.upper() == 'MANAGEMENT':
                host_name = 'some.of.host.name.com' 
            engine = create_engine('mysql+mysqlconnector://' + username + ':' + password + '@' + host_name + ':3306/' + schema_name)

            conn = engine.connect()
            Session = sessionmaker(bind=engine)
            Session.configure(bind=engine)
            session = Session()
            
            def encrypt_string(text_to_hash):
                
                hash_algorithm = hashlib.sha256()
                hash_algorithm.update(text_to_hash.encode())
                hashed_string = hash_algorithm.hexdigest()
                
                return hashed_string
            
            
            def authentication_process():
                #sign in 
                try:
                    driver.find_element(By.ID,"Email").send_keys(user_text)
                    driver.find_element(By.ID,"Password").send_keys(pass_text)
                    driver.find_element(By.XPATH, "//button[@type='submit']").click()
                    
                    logging.info("sign in!")
                    return True
                
                except Exception:
                    logging.error("sign in failed.")
                    driver.close();
                    driver.quit();
                    return False
            
            
            def forward_to_station():    
                #step to getting to Station page
                
                auth = AuthenticationProcess()
                
                if auth == True:
                    try:
                        mainPage = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='divIdForm']/button")))
                        mainPage.click()
                        
                        modelPage = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='cmdProceed']")))
                        modelPage.click()
                        
                        #close the popup about chosen model (CURRENT)
                        alert = driver.switch_to.alert
                        alert.accept()
                        
                        # get to Station data
                        driver.switch_to.frame("contents") 
                            
                        stationPage = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//*[@id='TreeView1t1']")))
                        stationPage.click()
                        
                        return True
            
                    except Exception as err:
                        logging.error(f"Not this time {err}, {type(err)=}" )
                        return False
                        driver.close();
                        driver.quit();
            
                else:
                    driver.close();
                    driver.quit();
                    return False
            
                
            
            def download_rating_files():
                
                filename = ''
                
                driver.switch_to.default_content()
                driver.switch_to.frame("main")
                
                try:
                    for device in devices:
                        logging.warning(f"Start device: {device}")
                        select = Select(driver.find_element(By.XPATH, '//*[@id="DvDropDownList"]'))
                        select.select_by_value(device)
                        
                        select = Select(driver.find_element(By.XPATH,'//*[@id="CoDropDownList"]'))
                        select.select_by_value('AECI')
                        
                        get_file = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//*[@id='cmbExportRatings']")))
                        get_file.click()
                        
                        sleep(2)
                        
                        for file in os.listdir(save_folder):
                            newest_file = max([save_folder + "/" + f for f in os.listdir(save_folder) if re.match(r'Rating+.*\.csv', f)],key=os.path.getctime)
                    
                        if newest_file == f"{save_folder}/Ratings.csv":
                            os.rename(newest_file,f"{save_folder}/Ratings_{device}.csv")
                            filename = f"Ratings_{device}.csv"
                            logging.warning(filename)
                            
                            process_file_operator(filename, device)
                
                    return True
                except:
                    return False
            
            
            def process_file_operator(filename, device):
                
                logging.info(f"Started : {device} with file: {filename}")
                if device == "Line":
                    process_file_line(filename)
                if device == "Zbr":
                    process_file_zbr(filename)
                if device == "Transformer":
                    process_file_transformer(filename)
                
                
            
            def process_file_transformer(filename):
                
                logging.info(f"Uploaded: {save_folder}/{filename}")
                df_matrix = pd.DataFrame(pd.read_csv(f"{save_folder}/{filename}", delimiter = ',', header = None, dtype=str, skiprows=2))
            
                df_matrix.drop(df_matrix.columns[-1], axis=1, inplace=True)
                df_matrix.drop(df_matrix.columns[-2], axis=1, inplace=True)
                df_matrix.drop(df_matrix.columns[-3], axis=1, inplace=True)
                
                df = df_matrix
                df.columns = ['Action','DevTyp','XfName','XfmrName','Co','Station','FromKv','FromNode','ToKv','ToNode','R','X','RegNode','TargetKv','Deviate','HSMinTap','HSMaxTap','HSNomTap','HSStep','LSMinTap','LSMaxTap','LSNomTap','LSStep','SprnNorm','SprnEmer','SprnLoad','SummNorm','SummEmer','SummLoad','FallNorm','FallEmer','FallLoad','WintNorm','WintEmer','WintLoad','IdcFromBus','IdcToBus','IdcCkt','IdcRef','ICCPFromMwa','ICCPFromMva','ICCPToMwb','ICCPToMvb','IsTransUpgrade','MtepId','RatingEffDate']
                df = df.replace(np.nan, '', regex=True)
                
                df['SprnNorm'] = np.where(df['SprnNorm'] == '', np.nan, df['SprnNorm'])
                df['SprnEmer'] = np.where(df['SprnEmer'] == '', np.nan, df['SprnEmer'])
                df['SummNorm'] = np.where(df['SummNorm'] == '', np.nan, df['SummNorm'])
                df['SummEmer'] = np.where(df['SummEmer'] == '', np.nan, df['SummEmer'])
                df['FallNorm'] = np.where(df['FallNorm'] == '', np.nan, df['FallNorm'])
                df['FallEmer'] = np.where(df['FallEmer'] == '', np.nan, df['FallEmer'])
                df['WintNorm'] = np.where(df['WintNorm'] == '', np.nan, df['WintNorm'])
                df['WintEmer'] = np.where(df['WintEmer'] == '', np.nan, df['WintEmer'])
                
                try:
                    df = df[['DevTyp', 'XfName', 'XfmrName', 'FromKv', 'Co', 'Station', 'IdcFromBus', 'IdcToBus', 'IdcCkt', 'IdcRef', 'SprnNorm', 'SprnEmer', 'SummNorm', 'SummEmer', 'FallNorm', 'FallEmer', 'WintNorm', 'WintEmer']].copy()
                    df.insert(5, 'ToCo', ['' for i in range(df.shape[0])])
                    df.insert(7, 'ToStation', ['' for i in range(df.shape[0])])
                    df.insert(8, 'From_to_ckt', [(df['IdcFromBus'].iloc[i] +'_'+ df['IdcToBus'].iloc[i] +'_'+ df['IdcCkt'].iloc[i]) for i in range(df.shape[0])])
                    df['upload_date'] = [upload_date for i in range(df.shape[0])]
                    df['hash'] = [encrypt_string(f"{df['DevTyp'][i]}{fieldSeparator}{df['XfName'][i]}{fieldSeparator}{df['XfmrName'][i]}{fieldSeparator}{df['FromKv'][i]}{fieldSeparator}{df['Co'][i]}{fieldSeparator}{df['ToCo'][i]}{fieldSeparator}{df['Station'][i]}{fieldSeparator}{df['ToStation'] [i]}{fieldSeparator}{df['From_to_ckt'] [i]}{fieldSeparator}{df['IdcRef'][i]}{fieldSeparator}{df['SprnNorm'][i]}{fieldSeparator}{df['SprnEmer'][i]}{fieldSeparator}{df['SummNorm'][i]}{fieldSeparator}{df['SummEmer'][i]}{fieldSeparator}{df['FallNorm'][i]}{fieldSeparator}{df['FallEmer'][i]}{fieldSeparator}{df['WintNorm'][i]}{fieldSeparator}{df['WintEmer'][i]}") for i in range(df.shape[0])]
            
                except Exception as err:
                    print(err)
            
                df.drop(columns=['IdcFromBus', 'IdcToBus', 'IdcCkt'], axis=1, inplace=True)
                df = df.drop_duplicates()    
                df.columns=['type','name','segment','kv','from_co','to_co','from_station','to_station','from_to_ckt','idc_name','sprn_norm','sprn_emer','summ_norm','summ_emer','fall_norm','fall_emer','wint_norm','wint_emer','upload_date','hash']
            
                try:
                    df.to_sql(name=table_name, con=conn, schema=schema_name, if_exists='append', index=False, chunksize = 1000)
                    print(f"DATA uploaded to {table_name}")
                except Exception:
                    # causing an issue with unique index
                    pass
            
                sleep(2)
            
                try:
                    df.to_sql(name=table_name_2, con=conn, schema=schema_name, if_exists='append', index=False, chunksize = 1000)
                    print(f"DATA uploaded to {table_name_2}")
                except Exception:
                    # causing an issue with unique index
                    pass    
                
                os.remove(f"{save_folder}/{filename}")    
            
                
            
            def process_file_line(filename):
                
                print(f"Uploaded: {save_folder}/{filename}")
                df_matrix = pd.DataFrame(pd.read_csv(f"{save_folder}/{filename}", delimiter = ',', header = None, dtype=str, skiprows=2))
            
                df_matrix.drop(df_matrix.columns[-1], axis=1, inplace=True)
                df_matrix.drop(df_matrix.columns[-2], axis=1, inplace=True)
                df_matrix.drop(df_matrix.columns[-3], axis=1, inplace=True)
                
                df = df_matrix
                df.columns = ['Action','DevTyp','LineName','Segment','Kv','FromCo','FromStation','FromNode','ToCo','ToStation','ToNode','R','X','Bch','SprnNorm','SprnEmer','SprnLoad','SummNorm','SummEmer','SummLoad','FallNorm','FallEmer','FallLoad','WintNorm','WintEmer','WintLoad','IdcFromBus','IdcToBus','IdcCkt','IdcRef','ICCPFromMwa','ICCPFromMwb','ICCPFromMva','ICCPFromMvb','ICCPToMwa','ICCPToMwb','ICCPToMva','ICCPToMvb','IsTransUpgrade','MtepId','RatingEffDate']
                df = df.replace(np.nan, '', regex=True)
                
                df['SprnNorm'] = np.where(df['SprnNorm'] == '', np.nan, df['SprnNorm'])
                df['SprnEmer'] = np.where(df['SprnEmer'] == '', np.nan, df['SprnEmer'])
                df['SummNorm'] = np.where(df['SummNorm'] == '', np.nan, df['SummNorm'])
                df['SummEmer'] = np.where(df['SummEmer'] == '', np.nan, df['SummEmer'])
                df['FallNorm'] = np.where(df['FallNorm'] == '', np.nan, df['FallNorm'])
                df['FallEmer'] = np.where(df['FallEmer'] == '', np.nan, df['FallEmer'])
                df['WintNorm'] = np.where(df['WintNorm'] == '', np.nan, df['WintNorm'])
                df['WintEmer'] = np.where(df['WintEmer'] == '', np.nan, df['WintEmer'])
                
                try:
                    df = df[['DevTyp','LineName','Segment','Kv','FromCo','ToCo','FromStation','ToStation','IdcFromBus','IdcToBus','IdcCkt','IdcRef','SprnNorm','SprnEmer','SummNorm','SummEmer','FallNorm','FallEmer','WintNorm','WintEmer']].copy()
                    df.insert(8, 'From_to_ckt', [(df['IdcFromBus'].iloc[i] +'_'+ df['IdcToBus'].iloc[i] +'_'+ df['IdcCkt'].iloc[i]) for i in range(df.shape[0])])
                    df['upload_date'] = [upload_date for i in range(df.shape[0])]
                    df['hash'] = [encrypt_string(f"{df['DevTyp'][i]}{fieldSeparator}{df['LineName'][i]}{fieldSeparator}{df['Segment'][i]}{fieldSeparator}{df['Kv'][i]}{fieldSeparator}{df['FromCo'][i]}{fieldSeparator}{df['ToCo'][i]}{fieldSeparator}{df['FromStation'][i]}{fieldSeparator}{df['ToStation'][i]}{fieldSeparator}{df['From_to_ckt']}{fieldSeparator}{df['IdcRef'][i]}{fieldSeparator}{df['SprnNorm'][i]}{fieldSeparator}{df['SprnEmer'][i]}{fieldSeparator}{df['SummNorm'][i]}{fieldSeparator}{df['SummEmer'][i]}{fieldSeparator}{df['FallNorm'][i]}{fieldSeparator}{df['FallEmer'][i]}{fieldSeparator}{df['WintNorm'][i]}{fieldSeparator}{df['WintEmer'][i]}") for i in range(df.shape[0])]
            
                except Exception as err:
                    print(err)
            
                df.drop(columns=['IdcFromBus', 'IdcToBus', 'IdcCkt'], axis=1, inplace=True)
                df = df.drop_duplicates()   
                df.columns = ['type','name','segment','kv','from_co','to_co','from_station','to_station','from_to_ckt','idc_name','sprn_norm','sprn_emer','summ_norm','summ_emer','fall_norm','fall_emer','wint_norm','wint_emer','upload_date','hash']
            
                try:
                    df.to_sql(name=table_name, con=conn, schema=schema_name, if_exists='append', index=False, chunksize = 1000)
                    print(f"DATA uploaded to {table_name}")
                except Exception:
                    # causing an issue with unique index
                    pass
                
                sleep(2)    
                
                try:
                    df.to_sql(name=table_name_2, con=conn, schema=schema_name, if_exists='append', index=False, chunksize = 1000)
                    print(f"DATA uploaded to {table_name_2}")
                except Exception:
                    # causing an issue with unique index
                    pass
                
                os.remove(f"{save_folder}/{filename}")
                
                
            
            def process_file_zbr(filename):
                
                print(f"Uploaded: {save_folder}/{filename}")
                df_matrix = pd.DataFrame(pd.read_csv(f"{save_folder}/{filename}", delimiter = ',', header = None, dtype=str, skiprows=2))
            
                df_matrix.drop(df_matrix.columns[-1], axis=1, inplace=True)
                df_matrix.drop(df_matrix.columns[-2], axis=1, inplace=True)
                df_matrix.drop(df_matrix.columns[-3], axis=1, inplace=True)
                
                df = df_matrix
                df.columns = ['Action','DevTyp','LineName','Segment','Kv','FromCo','FromStation','FromNode','ToCo','ToStation','ToNode','SprnNorm','SprnEmer','SprnLoad','SummNorm','SummEmer','SummLoad','FallNorm','FallEmer','FallLoad','WintNorm','WintEmer','WintLoad','IdcFromBus','IdcToBus','IdcCkt','ICCPFromMwa','ICCPFromMwb','ICCPFromMva','ICCPFromMvb','ICCPToMwa','ICCPToMwb','ICCPToMva','ICCPToMvb']
                df = df.replace(np.nan, '', regex=True)
                
                df['SprnNorm'] = np.where(df['SprnNorm'] == '', np.nan, df['SprnNorm'])
                df['SprnEmer'] = np.where(df['SprnEmer'] == '', np.nan, df['SprnEmer'])
                df['SummNorm'] = np.where(df['SummNorm'] == '', np.nan, df['SummNorm'])
                df['SummEmer'] = np.where(df['SummEmer'] == '', np.nan, df['SummEmer'])
                df['FallNorm'] = np.where(df['FallNorm'] == '', np.nan, df['FallNorm'])
                df['FallEmer'] = np.where(df['FallEmer'] == '', np.nan, df['FallEmer'])
                df['WintNorm'] = np.where(df['WintNorm'] == '', np.nan, df['WintNorm'])
                df['WintEmer'] = np.where(df['WintEmer'] == '', np.nan, df['WintEmer'])
                
            
                try:
                    df = df[['DevTyp','LineName','Segment','Kv','FromCo','ToCo','FromStation','ToStation','IdcFromBus','IdcToBus','IdcCkt','SprnNorm','SprnEmer','SummNorm','SummEmer','FallNorm','FallEmer', 'WintNorm', 'WintEmer']].copy()
                    df.insert(8, 'From_to_ckt', [(df['IdcFromBus'].iloc[i] +'_'+ df['IdcToBus'].iloc[i] +'_'+ df['IdcCkt'].iloc[i]) for i in range(df.shape[0])])
                    df.insert(9, 'IdcRef', ['' for i in range(df.shape[0])])
                    df['upload_date'] = [upload_date for i in range(df.shape[0])]
                    df['hash'] = [encrypt_string(f"{df['DevTyp'][i]}{fieldSeparator}{df['LineName'][i]}{fieldSeparator}{df['Segment'][i]}{fieldSeparator}{df['Kv'][i]}{fieldSeparator}{df['FromCo'][i]}{fieldSeparator}{df['ToCo'][i]}{fieldSeparator}{df['FromStation'][i]}{fieldSeparator}{df['ToStation'][i]}{fieldSeparator}{df['From_to_ckt'][i]}{fieldSeparator}{df['IdcRef'][i]}{fieldSeparator}{df['SprnNorm'][i]}{fieldSeparator}{df['SprnEmer'][i]}{fieldSeparator}{df['SummNorm'][i]}{fieldSeparator}{df['SummEmer'][i]}{fieldSeparator}{df['FallNorm'][i]}{fieldSeparator}{df['FallEmer'][i]}{fieldSeparator}{df['WintNorm'][i]}{fieldSeparator}{df['WintEmer']}") for i in range(df.shape[0])]
            
                except Exception as err:
                    print(err)
                
                    
                df.drop(columns=['IdcFromBus', 'IdcToBus', 'IdcCkt'], axis=1, inplace=True)
                df = df.drop_duplicates()       
                df.columns = ['type','name','segment','kv','from_co','to_co','from_station','to_station','from_to_ckt','idc_name','sprn_norm','sprn_emer','summ_norm','summ_emer','fall_norm','fall_emer','wint_norm','wint_emer','upload_date','hash']
            
                try:
                    df.to_sql(name=table_name, con=conn, schema=schema_name, if_exists='append', index=False, chunksize = 1000)
                    print(f"DATA uploaded to {table_name}")
                except Exception:
                    # causing an issue with unique index
                    pass
                
                sleep(2)    
                
                try:
                    df.to_sql(name=table_name_2, con=conn, schema=schema_name, if_exists='append', index=False, chunksize = 1000)
                    print(f"DATA uploaded to {table_name_2}")
                except Exception:
                    # causing an issue with unique index
                    pass
                
                os.remove(f"{save_folder}/{filename}")
            
            
            
                
            
            def main_process():
                
                result = forward_to_station()
                
                if result == True:
                    upload_status = download_rating_files()
                    
                if upload_status == True:
                    try:
                        conn.execute("CALL stage.sp_rating_webmodel();")
                        sleep(7)
                        logging.info("finished aggregation")
            
                        conn.execute("CALL stage.sp_rating_changes();")
                        sleep(7)
                        logging.info("finished changes")           
                        
                    except:
                        conn.closed()  
                    finally:
                        conn.close()
                    
                    try:
                        conn.execute("CALL stage.sp_update_branch_ratings();")
                        sleep(5)
                        print("done")
            
                    except(Exception, Error) as error:
                        print(error)
                    
                    finally:
                        if conn2 is not None:
                            cursor.close()
                            conn2.close()
            
                driver.quit()
            
            # RUN START
            main_process()
        
        
        
        def interface_success():
            engine = create_engine("mysql+mysqlconnector://user:pass@some.of.example.domain.com:3306/stage")  
            try:
                conn = engine.connect()
                monitor_sql = ("UPDATE monitoring.python_job SET end_date = NOW(), status = 0 WHERE id = 74;")
                conn.execute(text(monitor_sql))
                monitor_sql = ("INSERT INTO monitoring.python_job_history " + 
                            "(id_job, description, start_date, data_downloaded, data_processed, data_inserted, data_aggregated, end_date, status, exec_number, summary, aux)  " +
                            "SELECT * FROM monitoring.python_job WHERE id = 74;")
                conn.execute(text(monitor_sql))
            except SQLAlchemyError:
                pass
            finally:
                conn.close()
        
        
        
        def interface_fail():
            engine = create_engine("mysql+mysqlconnector://user:pass@some.of.example.domain.com:3306/stage")   
            try:
                conn = engine.connect()
                monitor_sql = ("UPDATE monitoring.python_job SET end_date = NOW(), status = -1 WHERE id = 74;")
                conn.execute(text(monitor_sql))
                monitor_sql = ("INSERT INTO monitoring.python_job_history " + 
                            "(id_job, description, start_date, data_downloaded, data_processed, data_inserted, data_aggregated, end_date, status, exec_number, summary, aux)  " +
                            "SELECT * FROM monitoring.python_job WHERE id = 74;")
                conn.execute(text(monitor_sql))
            except SQLAlchemyError:
                pass
            finally:
                conn.close()
                raise Exception
        
        
        # run workflow
        
        t1 = MySqlOperator(
            task_id='interface_start',
            mysql_conn_id='SRV_DB_QA',
            sql="""UPDATE monitoring.python_job 
                    SET start_date = NOW(), 
                    status = 1, 
                    data_downloaded = NULL, 
                    data_processed = NULL,
                    data_inserted = NULL,
                    data_aggregated = NULL, 
                    end_date = NULL,
                    status = 1,
                    exec_number = exec_number + 1, 
                    summary = NULL,
                    aux = NULL
                    WHERE id = 74;""",
            dag=dag)
        
        t2 = PythonOperator(
            task_id='runRatingProcess',
            python_callable=completed_process,
            dag=dag)
        
        t3 = MySqlOperator(
            task_id='aggregation_task',
            mysql_conn_id='SRV_DB_QA',
            sql="CALL stage.sp_rating_webmodel();",
            dag=dag)
        
        t4 = MySqlOperator(
            task_id='changes_task',
            mysql_conn_id='SRV_DB_QA',
            sql="CALL stage.sp_rating_changes();",
            dag=dag)
        
        agg_end = MySqlOperator(
            task_id='agg_description',
            mysql_conn_id='SRV_DB_QA',
            sql="""UPDATE monitoring.python_job
                    SET
                    data_aggregated = NOW(),
                    summary = 'data loaded and agg'
                    WHERE id = 74;""",
            dag=dag)
        
        changes_end = MySqlOperator(
            task_id='upload_changes',
            mysql_conn_id='SRV_DB_QA',
            sql="""UPDATE monitoring.python_job
                    SET
                    data_processed = NOW(),
                    summary = CONCAT(summary, ' add changes'),
                    end_date = NOW()
                    WHERE id = 74;""",
            dag=dag)
        
        monitor_transfer = MySqlOperator(
                            task_id='monitor_transfer',
                            mysql_conn_id='SRV_DB_QA',
                            sql="""UPDATE monitoring.python_job 
                                    SET data_inserted = NOW() 
                                    WHERE id = 74;""",
                            dag=dag)
        
        success = PythonOperator(
            task_id='interface_success',
            python_callable=interface_success,
            dag=dag)
        
        failure = PythonOperator(
            task_id='interface_fail',
            python_callable=interface_fail,
            dag=dag,
            trigger_rule='one_failed')
        
        
        t1 >> t2 >> monitor_transfer >> t3 >> agg_end >> t4 >> changes_end >> [success, failure]


