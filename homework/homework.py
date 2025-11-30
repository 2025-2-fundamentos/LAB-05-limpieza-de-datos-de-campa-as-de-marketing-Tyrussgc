"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import pandas as pd
import zipfile
import glob
import os

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: camabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    zip_files = glob.glob('./files/input/*.zip')                                                #?Busca todos los archivos .zip en la carpeta './files/input'

    dataframes = []                                                                             #?Inicializa una lista vacía para almacenar los dataframes leídos

    for zip_file in zip_files:                                                                  #? Itera sobre cada archivo zip encontrado
        with zipfile.ZipFile(zip_file) as z:                                                    #? Abre el archivo zip
            for filename in z.namelist():                                                       #? Itera sobre los nombres de los archivos dentro del zip
                if filename.endswith(".csv"):                                                   # ?Si el archivo es un .csv
                    with z.open (filename) as f:                                                #? Abre el archivo .csv
                        dataframe = pd.read_csv(f)                                               #? Lee el archivo .csv en un dataframe
                        dataframes.append(dataframe)                                            #? Añade el dataframe a la lista

    data = pd.concat(dataframes, ignore_index = True) ##?# Concatena todos los dataframes en uno solo, reiniciando el índice
    #*DT de clienten a modificar 
    client = data[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]] #?# Selecciona columnas
    client["job"] = client["job"].str.replace(".", "").str.replace("-", "_")                        #?reemplaza puntos por "" en columna "job"
    client["education"] = client["education"].str.replace(".", "_")                                 #?reemplzar puntos en "education"
    client["education"] = client["education"].replace("unknown", pd.NA)                             #?# Reemplaza "unknown" por valores nulos en la columna 'education'
    client["credit_default"] = (client["credit_default"] == "yes").astype(int)                      #?# Convierte la columna 'credit_default' en valores binarios (1 para 'yes', 0 para otro valor)      
    client["mortgage"] = (client["mortgage"] == "yes").astype(int)                                  #?# Convierte la columna 'mortgage' en valores binarios (1 para 'yes', 0 para otro valor)

    #* DT de campaing, columnas espeficadas
    campaign = data[["client_id",
                    "number_contacts", "contact_duration",
                    "previous_campaign_contacts", "previous_outcome", "campaign_outcome",
                    "day", "month"]]                                                           
    campaign["previous_outcome"] = (campaign["previous_outcome"] == "success").astype(int)          #?# Convierte la columna 'previous_outcome' en valores binarios (1 para 'success', 0 para otro valor)
    campaign["campaign_outcome"] = (campaign["campaign_outcome"] == "yes").astype(int)              #?# Convierte la columna 'campaign_outcome' en valores binarios (1 para 'yes', 0 para otro valor)
    campaign["last_contact_date"] = pd.to_datetime("2022-" + campaign["month"] + "-" + campaign["day"].astype(str)).dt.strftime('%Y-%m-%d') #? columna con fecha del ultimo contacto en formato 'YYYY-MM-DD' 
    campaign = campaign.drop(["day", "month"], axis = 1)                                            #? Elimina las columnas 'day' y 'month'

    #* Selecciona columnas específicas para el dataframe 'economics'
    economics = data[["client_id", "cons_price_idx", "euribor_three_months"]]                       

    os.makedirs('./files/output', exist_ok = True)                                      #?# Crea la carpeta de salida si no existe

    client.to_csv('./files/output/client.csv', index = False)                           #?# Guarda el dataframe 'client' en un archivo CSV
    campaign.to_csv('./files/output/campaign.csv', index = False)                       #?  Guarda el dataframe 'campaign' en un archivo CSV
    economics.to_csv('./files/output/economics.csv', index = False)                     #?# Guarda el dataframe 'economics' en un archivo CSV

clean_campaign_data()

    #!return


if __name__ == "__main__":
    clean_campaign_data()
