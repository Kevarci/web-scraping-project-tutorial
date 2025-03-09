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
            print(f"Tabla de evolución anual encontrada (Tabla {i+1})")
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
        print("\nDataFrame de Evolución Anual:")
        print(df.head())
        
        df.to_csv("annual_evolution_data.csv", index=False)
        print("Datos guardados en annual_evolution_data.csv")
    else:  
        print("No se pudo encontrar una tabla con datos de evolución anual")
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
        df_clean[column] = df_clean[column].apply(clean_value)
    
    df_clean = df_clean.dropna(how='all')
    df_clean = df_clean[(df_clean != 0).any(axis=1)]
    
    return df_clean

def store_in_sqlite(df, db_path='company_data.db', table_name='annual_evolution'):
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    columns = df.columns  
    
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    
    create_table_sql += "id INTEGER PRIMARY KEY AUTOINCREMENT, "
    create_table_sql += f"{columns[0]} TEXT, "
    
    for col in columns[1:]:
        create_table_sql += f"{col} REAL, "
    
    create_table_sql = create_table_sql[:-2] + ")"
    
    cursor.execute(create_table_sql)
    print(f"Tabla '{table_name}' creada con éxito")
    
    for _, row in df.iterrows():
        placeholders = ", ".join(["?" for _ in range(len(row))])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        values = [None if pd.isna(val) else val for val in row]
        
        cursor.execute(insert_sql, values)
    
    conn.commit()
    print(f"Se insertaron {len(df)} filas en la tabla '{table_name}'")
    
    conn.close()
    print(f"Base de datos guardada en '{db_path}'")

def load_data(source='db', db_path='company_data.db', csv_path='annual_evolution_data_clean.csv'):
    if source == 'db':
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query("SELECT * FROM annual_evolution", conn)
        conn.close()
    else:
        df = pd.read_csv(csv_path)
    
    return df

def visualizacion_1(df):
    sns.set(style="whitegrid")
    df_copy = df.copy()
    
    if 'Change' in df_copy.columns:
        df_copy = df_copy.drop(columns=['Change'])
    
    top_companies = df_copy.iloc[:5]
    company_col = top_companies.columns[0]
    
    for col in top_companies.columns[1:]:
        top_companies[col] = pd.to_numeric(top_companies[col], errors='coerce')
    
    df_melted = pd.melt(top_companies, id_vars=[company_col], 
                        var_name='Año', value_name='Valor')

    plt.figure(figsize=(12, 8))
    sns.lineplot(x='Año', y='Valor', hue=company_col, data=df_melted, marker='o')
    
    plt.title('Evolución de las Top 5 Compañías a lo largo del tiempo', fontsize=16)
    plt.xlabel('Año', fontsize=12)
    plt.ylabel('Valor de Mercado (Billones $)', fontsize=12)
    plt.xticks(rotation=45)
    plt.legend(title='Compañía', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()

    plt.savefig('visualizacion_1_evolucion_top5.png')
    plt.close()
    print("Visualización 1 guardada como 'visualizacion_1_evolucion_top5.png'")

def visualizacion_2(df):
    sns.set(style="whitegrid")
    df_copy = df.copy()
    
    if 'Change' in df_copy.columns:
        df_copy = df_copy.drop(columns=['Change'])
    
    company_col = df_copy.columns[0]
    latest_year = df_copy.columns[-1]
    
    df_copy[latest_year] = pd.to_numeric(df_copy[latest_year], errors='coerce')
    
    top10 = df_copy.sort_values(by=latest_year, ascending=False).head(10)

    plt.figure(figsize=(12, 8))
    bars = sns.barplot(x=latest_year, y=company_col, data=top10, palette='viridis')
    
    for i, bar in enumerate(bars.patches):
        bars.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2, 
                 f'{bar.get_width():.1f}', ha='left', va='center')
    
    plt.title(f'Top 10 Compañías por Valor de Mercado en {latest_year}', fontsize=16)
    plt.xlabel('Valor de Mercado (Billones $)', fontsize=12)
    plt.ylabel('Compañía', fontsize=12)
    plt.tight_layout()
    
    plt.savefig('visualizacion_2_top10_ultimo_anio.png')
    plt.close()
    print(f"Visualización 2 guardada como 'visualizacion_2_top10_ultimo_anio.png'")

def visualizacion_3(df):
    sns.set(style="white")
    df_copy = df.copy()
    
    if 'Change' in df_copy.columns:
        df_copy = df_copy.drop(columns=['Change'])
    
    company_col = df_copy.columns[0]
    top10 = df_copy.head(10).copy()
    years = df_copy.columns[1:]
    
    for year in years:
        top10[year] = pd.to_numeric(top10[year], errors='coerce')
    
    growth_data = []
    
    for i in range(len(years)-1):
        current_year = years[i]
        next_year = years[i+1]
        
        growth = ((top10[next_year] - top10[current_year]) / top10[current_year] * 100)
        growth_data.append(growth)
    
    growth_df = pd.DataFrame(growth_data).T
    growth_df.index = top10[company_col]
    growth_df.columns = [f'{years[i]} a {years[i+1]}' for i in range(len(years)-1)]
    
    plt.figure(figsize=(14, 10))
    heatmap = sns.heatmap(growth_df, annot=True, fmt=".1f", cmap="RdYlGn", 
                         linewidths=0.5, center=0, cbar_kws={'label': 'Crecimiento %'})
    
    plt.title('Crecimiento Porcentual Año a Año de las Top 10 Compañías', fontsize=16)
    plt.ylabel('Compañía', fontsize=12)
    plt.xlabel('Período', fontsize=12)
    plt.tight_layout()
    
    plt.savefig('visualizacion_3_crecimiento_porcentual.png')
    plt.close()
    print("Visualización 3 guardada como 'visualizacion_3_crecimiento_porcentual.png'")

if __name__ == "__main__":
    try:
        df = pd.read_csv('annual_evolution_data.csv')
        print("DataFrame original:")
        print(df.head())

        df_clean = clean_dataframe(df)
        print("\nDataFrame limpio:")
        print(df_clean.head())
        
        df_clean.to_csv("annual_evolution_data_clean.csv", index=False)
        print("DataFrame limpio guardado en 'annual_evolution_data_clean.csv'")
        
        store_in_sqlite(df_clean)
        
        try:
            df_viz = load_data(source='db')
            print("Datos cargados desde la base de datos SQLite para visualizaciones")
        except Exception as e:
            print(f"Error al cargar desde DB: {e}")
            
            df_viz = df_clean
            print("Usando DataFrame limpio para visualizaciones")
        
        visualizacion_1(df_viz)
        visualizacion_2(df_viz)
        visualizacion_3(df_viz)
        
        print("Todas las visualizaciones han sido generadas con éxito.")
        
    except FileNotFoundError:
        print("Error: No se encontró el archivo 'annual_evolution_data.csv'")
        print("Asegúrate de ejecutar primero el script principal para generar el CSV.")
    except Exception as e:
        print(f"Error inesperado: {e}")