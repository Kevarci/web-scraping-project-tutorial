import os
from bs4 import BeautifulSoup
import requests
import time
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd  
import re

resource_url = 'https://companies-market-cap-copy.vercel.app/index.html'

response = requests.get(resource_url)
if response.status_code == 200:
    html_content = response.text
    print('Contenido descargado satisfactoriamente!')
    print(html_content[:200] + "...")

    soup = BeautifulSoup(html_content, 'html.parser')

    tables = soup.find_all('table')

    annual_evolution_table = None

    for i, table in enumerate(tables):
        headers = table.find_all('th')
        header_texts = [header.get_text(strip=True) for header in headers]
            
        print(f"Table {i+1} headers: {header_texts}")
            
        if any('year' in header.lower() for header in header_texts) or any('20' in header for header in header_texts):
            annual_evolution_table = table
            print(f"Found annual evolution table (Table {i+1})")
            break

    if annual_evolution_table:
        rows = annual_evolution_table.find_all('tr')
        headers = [th.get_text(strip=True) for th in rows[0].find_all(['th'])]  

        data = []
        for row in rows[1:]:
            cols = row.find_all(['td'])
            if cols: 
                row_data = [col.get_text(strip=True) for col in cols]
                data.append(row_data)

        df = pd.DataFrame(data, columns=headers)  
        print("\nAnnual Evolution DataFrame:")
        print(df.head())
        
        df.to_csv("annual_evolution_data.csv", index=False)
        print("Data saved to annual_evolution_data.csv")
    else:  
        print("Could not find a table with annual evolution data")
else:
    print(f"Failed to download HTML. Status code: {response.status_code}")

def clean_value(value):
    if isinstance(value, str): 
        value = value.replace('$', '').replace('B', '')
        value = value.strip()
        try:
            return float(value)
        except ValueError:
            return value
    return value

def clean_dataframe(df): 
    df_clean = df.copy()
    
    for column in df_clean.columns:
        df_clean[column] = df_clean[column].apply(clean_value)  # 
    
    df_clean = df_clean.dropna(how='all')
    df_clean = df_clean[(df_clean != 0).any(axis=1)]
    
    return df_clean

if __name__ == "__main__":
    try:
        df = pd.read_csv('annual_evolution_data.csv')
        print("DataFrame original:")
        print(df.head())

        df_clean = clean_dataframe(df)
        print("\nDataFrame limpio:")
        print(df_clean.head())
        
        df_clean.to_csv("annual_evolution_data_clean.csv", index=False)
       
        
    except FileNotFoundError:
        print("Error: No se encontró el archivo 'annual_evolution_data.csv'")
        print("Asegúrate de ejecutar primero el script principal para generar el CSV.")
