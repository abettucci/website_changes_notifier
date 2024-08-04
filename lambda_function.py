import json
import boto3
import requests
import pandas as pd
import time
from lxml import etree
from datetime import datetime
import gspread
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials
from botocore.exceptions import ClientError

URL = 'https://developers.mercadolibre.com.ar/devcenter/news/?mlibre=mlibre'
SPREADSHEET_ID = 'your_google_spread_sheet_id'
RANGE_NAME = 'Hoja1!A:C'  # Ajusta según tu hoja

def obtener_html(url):
    response = requests.get(url)
    return response.text

def parsear_html(html):
    parser = etree.HTMLParser()
    tree = etree.fromstring(html, parser)
    noticias = []

    # Sección news-card__banner-highlight
    div_banners = tree.xpath('//*[@id="news"]/div/div[4]/div[2]/div[2]//div')
    for div in div_banners:
        fecha_noticia_banner = div.xpath('.//div[contains(@class, "news-card__date-wrapper")]/p[1]/text()')
        titulo_noticia_banner = div.xpath('.//div[contains(@class, "news-card__text-content")]/h2/text()')
        texto_noticia_banner = div.xpath('.//p[contains(@class, "content__text")]/text()')

        lista_bullets_banner = div.xpath('.//div[contains(@class, "news-card__text-content")]//ul[contains(@class, "details__bullets-list")]//li/text()')
        bullets = '. '.join([li.strip() for li in lista_bullets_banner])

        parrafos = div.xpath('.//div[contains(@class, "news-card__text-content")]//p[not(contains(@class, "content__text"))]')
        parts = []
        for element in parrafos:
            for part in element.itertext():
                parts.append(part.strip())
        texto = ' '.join(parts)
        if texto != "Ver documentación" and texto:
            texto_debajo_bullet_list = texto

        if fecha_noticia_banner and titulo_noticia_banner and texto_noticia_banner:
            noticias.append({
                'titulo': titulo_noticia_banner[0].strip(),
                'contenido': texto_noticia_banner[0].strip() + bullets + '. ' + texto_debajo_bullet_list,
                'fecha' : fecha_noticia_banner[0],
                'fecha_insert': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

    # Sección news__child
    div_children = tree.xpath('//*[@id="news"]/div/div[4]/div[2]/div[3]/div/div')
    for div in div_children:
        if div.xpath('.//div[contains(@class, "news-card__default-width news-card__body")]'):            
            lista_fecha_noticia_child = div.xpath('.//div[contains(@class, "news-card__date-wrapper")]//p[1]/text()')
            lista_titulo_noticia_child = div.xpath('.//div[contains(@class, "news-card__text-content")]//h3/text()')
            lista_texto_noticia_child = div.xpath('.//p[contains(@class, "content__text")]//text()')

            for i in range(len(lista_fecha_noticia_child)):
                fecha_noticia_child = lista_fecha_noticia_child[i]
                titulo_noticia_child = lista_titulo_noticia_child[i]
                texto_noticia_child = lista_texto_noticia_child[i]
            
                if fecha_noticia_child and titulo_noticia_child and texto_noticia_child:
                    noticias.append({
                        'titulo': titulo_noticia_child.strip(),
                        'contenido': texto_noticia_child.strip(),
                        'fecha' : fecha_noticia_child,
                        'fecha_insert': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })

    return noticias

def obtener_noticias_existentes(sheet, google_api_dict_list):    
    data_noticias = make_read_api_call('get_worksheet_by_id_and_get_all_values', 0, sheet, 0, '', google_api_dict_list)
    data_noticias = [row[:3] for row in data_noticias]
    columnas_df_noticias = data_noticias[0][:3] #HEADERS
    data_df_noticias = data_noticias[1:len(data_noticias) + 1] #ROWS
    filas_df_noticias = [[row[i] for i in range(0, len(row))] for row in data_df_noticias]
    noticias_existentes_sheet = pd.DataFrame(filas_df_noticias, columns = columnas_df_noticias)
    noticias_existentes_sheet = noticias_existentes_sheet.dropna()
    return noticias_existentes_sheet.to_dict(orient='records')

def actualizar_google_sheets(sheet, noticia):
    last_row = len(sheet.get_worksheet_by_id(0).get_all_values()) + 1
    data_to_upload = {
        'titulo' : noticia['titulo'],
        'contenido' : noticia['contenido'], 
        'fecha' : noticia['fecha'], 
        'fecha_insert' : noticia['fecha_insert']
    }
    sheet.get_worksheet_by_id(0).insert_row(list(data_to_upload.values()), last_row)

def get_secret_value_aws(secret_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name="us-east-2")
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    secret = get_secret_value_response['SecretString']
    return secret

# Funcion que realza una accion sobre una hoja de google sheets en funcion del parametro "funcion" que define
# si solo debe abrir un sheets, abrir una hoja en particular de un sheets o abrir y obtener todos los datos de una hoja
def make_read_api_call(funcion, parametros, hoja, slice1, slice2, google_api_dict_list):
    try:
        if funcion == 'open_by_key':
            resultado = hoja.open_by_key(parametros)
        if funcion == 'get_worksheet_by_id':
            resultado = hoja.get_worksheet_by_id(parametros)
        elif funcion == 'get_all_values':
            resultado = hoja.get_worksheet_by_id(parametros).get_all_values()
        else: #get_worksheet_by_id_and_get_all_values
            if slice2 == '':
                resultado = hoja.get_worksheet_by_id(parametros).get_all_values()[slice1:]
            else:
                resultado = hoja.get_worksheet_by_id(parametros).get_all_values()[slice1]
    except HttpError as e:
        if e.resp.status == 429:
            try:
                creds = ServiceAccountCredentials.from_json_keyfile_dict(
                    google_api_dict_list[1], 
                    ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
                gc = gspread.authorize(creds) 
                if funcion == 'open_by_key':
                    resultado = gc.open_by_key(parametros)
                elif funcion == 'get_worksheet_by_id':
                    resultado = hoja.get_worksheet_by_id(parametros)
                elif funcion == 'get_all_values':
                    resultado = hoja.get_worksheet_by_id(parametros).get_all_values()
                else: #get_worksheet_by_id_and_get_all_values
                    if slice2 == '':
                        resultado = hoja.get_worksheet_by_id(parametros).get_all_values()[slice1:]
                    else:
                        resultado = hoja.get_worksheet_by_id(parametros).get_all_values()[slice1]
            except:
                if e.resp.status == 429:
                    # Handle rate limit exceeded error with exponential backoff
                    wait_time = 1  # Initial wait time in seconds
                    max_retries = 5  # Maximum number of retries
                    retries = 0

                    while retries < max_retries:
                        print(f"Rate limit exceeded. Waiting for {wait_time} seconds...")
                        time.sleep(wait_time)
                        try:
                            if funcion == 'open_by_key':
                                resultado = gc.open_by_key(parametros)
                            elif funcion == 'get_worksheet_by_id':
                                resultado = hoja.get_worksheet_by_id(parametros)
                            elif funcion == 'get_all_values':
                                resultado = hoja.get_worksheet_by_id(parametros).get_all_values()
                            else: #get_worksheet_by_id_and_get_all_values
                                resultado = hoja.get_worksheet_by_id(parametros).get_all_values()[slice1:slice2]
                            break
                        except HttpError as e:
                            if e.resp.status == 429:
                                # Increase wait time exponentially for the next retry
                                wait_time *= 2
                                retries += 1
                            else:
                                # Handle other HTTP errors
                                raise
                else:
                    # Handle other HTTP errors
                    raise
    return resultado

def autenticar_google_sheets():
    # Obtenemos las credenciales de la API de MercadoLibre con un secreto del SecretManager de AWS pasandole la ruta del secreto
    google_api_dict_list = []
    google_key_locations = ['your_secret_name_location_in_aws_secret_manager']
    for api_dict in google_key_locations:
        secret = get_secret_value_aws(api_dict)
        secret_data = json.loads(secret)
        key_dict = {
            "private_key_id" : secret_data.get('private_key_id'),
            "type" : secret_data.get('type'),
            "project_id" : secret_data.get('project_id'),
            "client_id" : secret_data.get('client_id'),
            "client_email" : secret_data.get('client_email'),
            "private_key" : secret_data.get('private_key')}
        google_api_dict_list.append(key_dict)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        google_api_dict_list[0], 
        ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    
    gc = gspread.authorize(creds) 
    sheet = gc.open_by_key(SPREADSHEET_ID)
    return sheet, google_api_dict_list

def lambda_handler(event, context):
    html = obtener_html(URL)
    nuevas_noticias = parsear_html(html)
    sheet, google_api_dict_list = autenticar_google_sheets()
    noticias_existentes = obtener_noticias_existentes(sheet, google_api_dict_list)

    # Añadir nuevas noticias si no existen ya
    noticias_a_guardar = []
    seen_dicts = set()
    noticia_sin_insert_date = None
    keys_to_keep = ['titulo', 'contenido', 'fecha']

    for noticia in nuevas_noticias:
        noticia_sin_insert_date = {key: noticia[key] for key in keys_to_keep}
        noticia_sin_insert_date_tupla = tuple(sorted(noticia_sin_insert_date.items()))

        if noticia_sin_insert_date_tupla not in seen_dicts and any(d == noticia_sin_insert_date for d in noticias_existentes) == False:
            seen_dicts.add(noticia_sin_insert_date_tupla)
            noticias_a_guardar.append(noticia)

    if noticias_a_guardar:
        for noticia in noticias_a_guardar:
            actualizar_google_sheets(sheet, noticia)

    return {
        'statusCode': 200,
        'body': json.dumps('Noticias almacenadas correctamente')
    }

lambda_handler(None, None)
