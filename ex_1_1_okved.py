#!/usr/bin/env python
""" Andrey S
    
    Script read zip file with json file and pass data to database table
    
    Education project
"""

import pandas as pd
from sqlalchemy import create_engine

def main():
    #try 
    
    #read dataframe from zipped json file
    df=pd.read_json("okved_2.json.zip")
    #create sqlalchemy engine
    engine=create_engine("sqlite:///hw1.db")
    #pass directly engine to pandas
    df.to_sql('okved', con=engine, index=False, index_label='code', if_exists='replace')
    
    #except FileNotFoundError as e
    
    #finally: close all

if __name__ == '__main__':
    main()    