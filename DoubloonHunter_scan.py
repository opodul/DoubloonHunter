# !/usr/bin/python
import os
import pandas as pd
import fastparquet
import sys
import hashlib
import time
import numpy as np
import pathlib
from tqdm import tqdm

DB_PER_DIR_NAME = '.DblnHntr.parq'
HOME_PATH = pathlib.Path.home()
SCAN_PATH = f"{HOME_PATH}/dev/FreeRTOSv202212.01"

print(SCAN_PATH)

PRINT_ENABLE=0
HASH_ENABLE=1
HASH_FORCE=1

cumul_dhash = 0
cumul_size = 0

#--------------------------------------------
# notice that time is always UTC+0 (whatever summer or winter time)
def print_DB(df):
    if PRINT_ENABLE:
        # modification done on df here would be kept in the original df
        # therfore work with a copy
        df2 = df.copy() 
        #look for all datetime colon and reformat to customized string
        for col in df2.select_dtypes(include=[np.datetime64]):
            df2[col] = df2[col].dt.strftime('%Y/%m/%d %H:%M:%S')
        print( df2 )

#--------------------------------------------
def init_DB_per_dir(dir_path='.'):
    try:
        fpath = f"{dir_path}/{DB_PER_DIR_NAME}"

        #if DB already exists then read it to pandas
        if os.path.isfile(fpath):
            pf = fastparquet.ParquetFile(fpath)
            df = pf.to_pandas()

        #if DB not exists then create a empty pandas df
        else:
            df = pd.DataFrame()
            
    except Exception as err:
        print(f"fpath: {fpath}")
        print(f"Unexpected {err=}, {type(err)=}")
        df = pd.DataFrame()
        pass   

    return df

#--------------------------------------------
def write_DB_per_dir( dir_path , df):
    fpath = f"{dir_path}/{DB_PER_DIR_NAME}"
    fastparquet.write(fpath,df)

#--------------------------------------------
# Speed compare http://atodorov.org/blog/2013/02/05/performance-test-md5-sha1-sha256-sha512/
def hash_file(fpath):
    start = time.time()

    if HASH_ENABLE:
        BUF_SIZE = 1*1024*1024
        sha1 = hashlib.sha1()

        with open(fpath, 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                sha1.update(data)
        h = sha1.hexdigest()
    else:
        h = 'NA'
        
    end = time.time()
    return( h , end-start )

#============================================
def main():
    # Yeah I know ..
    global cumul_size
    global cumul_dhash
    
    # step(1) get the complete list
    os_walk_list = []
    for root, dirs, files in os.walk(SCAN_PATH, topdown=True) :
        os_walk_list.append( [root,dirs,files] )
    
    
    # step (2) iterate over the list with a progress bar
    for root, dirs, files in tqdm( os_walk_list ):
        try:
            #print(f"root: {root}")
            df = init_DB_per_dir(root)

            for f in files:
            

                f_path  = os.path.join(root, f)
                f_stat  = os.stat(f_path)
                f_size  = f_stat.st_size
                f_mtime = f_stat.st_mtime
                

                
                if HASH_FORCE:
                    if f in df.index:
                        df = df.drop(index=f)

                # if file name (index) already in the DB
                if f in df.index:
                    # if size or mtime is different than before
                    if( df.loc[f,'size']  != f_size
                     or df.loc[f,'mtime'] != pd.to_datetime(f_mtime, unit='s') 
                      ):
                         df = df.drop(index=f)

                # if file name is not in the DB or.. not anymore in the DB ;-)
                if f not in df.index:
                    (h,d) = hash_file(f_path)
                    df.loc[f,'size']  = f_stat.st_size
                    df.loc[f,'mtime'] = pd.to_datetime(f_mtime, unit='s')
                    df.loc[f,'hash']  = h
                    df.loc[f,'dhash'] = d
                    df.loc[f,'thash'] = pd.to_datetime(time.time(), unit='s')
                    
                    cumul_size  +=  f_size
                    cumul_dhash +=  d

            write_DB_per_dir(root,df)

        except Exception as err:
            print(root)
            print(f)
            print(f"Unexpected {err=}, {type(err)=}")
            raise    

        #break # do only 1 iteration for test purpose
    
#============================================
if __name__ == "__main__":

    start = time.time()
    main()
    end = time.time()
    d = end - start
    print(f"Script execution time : {d:.2f}s")

    size_MB = cumul_size/1024/1024
    if cumul_dhash != 0:
        MBps = (cumul_size/1024/1024) / cumul_dhash
    else:
        MBps = "NA"
    print(f"Hash speed {size_MB:.2f}MBytes in {cumul_dhash:.2f}secondes  --> {MBps:.2f}MBps ")


