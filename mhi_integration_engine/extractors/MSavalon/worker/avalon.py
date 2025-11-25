#!/usr/bin/env python3
# -*- coding: utf8 -*-
import sqlalchemy
import json
import datetime
import os
from utils import controller, mappings
import concurrent.futures
from itertools import product, repeat

# CONSTANTES
# Valor con el que identifican el idHotel en un endpoint 

class Extraccion():
    def __init__(self, _m_account):
        # Siempre evaluamos que nos llegue el campo _m_account
        if type(_m_account) == str:
            self.__M_ACCOUNT = _m_account
        else:
            raise 'El valor de mhi_account no es un valor válido'
        # Instanciamos basandonos en el fichero de configuración
        # Dejamos la configuración en memoria para acceder rapidamente a ella cada vez que necesitemos
        __CONFIG_FILE = 'config.json'
        dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        dir_path_file = os.path.join(dir_path, __CONFIG_FILE)
        with open(dir_path_file, 'r') as f:
            self.__CONFIG =  json.load(f)
        # Obtenemos todas las variables generalistas de un extractor
        self.__EXTRACTOR_NAME = self.__CONFIG['extractor_name']

        # Este trozo de codigo se tiene que cambiar por un endpoint general interno
        # FUTURE INI
        try:
            # Obtenemos el SECRET de conexion a la fuente
            secret = json.loads(controller.getSecret(account=self.__M_ACCOUNT,datasource=self.__EXTRACTOR_NAME))
            self.__HOST = secret['host'];
            self.__PORT = secret['port'];
            self.__USER = secret['user'];
            self.__PASSWORD = secret['password'];
            self.__DATABASES = list(secret['databases']);
            self.__CDC = secret['cdc_enabled'];
            self.__CUSTOMER_START_DATE= secret['start_date']
            
        except Exception as e:
            msg=f'Hay un error en obtener el secret, error:{type(e)}: {e}.'
            raise(msg)
        # FUTURE FIN
    
    def Iniciar(self, datasets):
        print(f'INICIO EXTRACCION DATOS AVALON PARA LA CUENTA {self.__M_ACCOUNT }.')
        try:
             with concurrent.futures.ThreadPoolExecutor() as executor:
                # Ejecutar la función en paralelo para cada valor en la lista
                list(executor.map(self.__cargaDataset, datasets))
        except Exception as e:
            msg=f'El método iniciar ha fallado en alguno de sus subprocesos, error:{type(e)}: {e}.'
            raise Exception(msg)
            
        print(f'FIN EXTRACCION DATOS AVALON PARA LA CUENTA {self.__M_ACCOUNT }.')

        ###############################################
        ## FUTURE: 
        # Aqui va el mensaje en pubsub para informar que se ha terminado satisfactoriamente
        # La funcion que desencadena pubSub se encarga de ver los modulos asociados a este componente que dispone el cliente y lanza la recarga
        #controller.postDataTransformation(account=self.__M_ACCOUNT,datasource=self.__EXTRACTOR_NAME)  
        ###############################################
        
        return True  # Devolver True si la extracción fue exitosa
    
    def __cargaDataset(self, dataset):
        # Este procedimiento se encargará de leer los datos del dataset aplicando la lógica del conector
        print(f' EXTRACCION {dataset} INICIO')
        # Paso 1: Obtenemos los atributos del dataset
        attributes = self.__CONFIG['datasets'][dataset]
        
        file_prefix = ''
        
        # Determinar si es un dataset de base de datos o de API
        if 'table_name' in attributes:
            # Dataset de base de datos - procesamiento simplificado
            table_name = attributes['table_name']
            # En una implementación real, aquí se haría la consulta a la base de datos
        else:
            # Dataset de API - procesamiento original
            # PASO 1: Evaluar si el dataset es incremental
            # Si es incremental >>
            if attributes['incremental']:
                # Iteramos en el juego de parámetros
                param_sets = self.__getParamSets(attributes)
                for value in param_sets:
                    # Generamos el prefijo del fichero con los parametros para guardar en el Data Lake
                    incr_value = str(dict(value).get(attributes['incremental_field']))
                    file_prefix_params = file_prefix + attributes['incremental_field'] + incr_value +'_'
                    # Si el dataset es iterable por propiedad
                    if attributes['property_iterable']:
                        #print(f'El dataset {dataset} es iterable por propiedad')
                        hotelList = self.__getHotelList()
                        hotel_urls = []
                        file_prefix_hotels = []
                        #Generamos las dos listas para lanzar las cargas en paralelo
                        url = str(self.__BASE_URL + attributes['endpoint'])
                        paginate = attributes.get('paginate', False)
                        data_attribute = attributes.get('data_attribute', None)
                        for hotel in hotelList:
                            hotel_urls.append (str.replace(url,'{{propertyId}}',str(hotel)))
                            file_prefix_hotels.append (f'hotel{hotel}_' + file_prefix_params)
                        # Lanzamos la carga de los hoteles en paralelo
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            # Ejecutar la función en paralelo para cada valor en la lista
                            list(executor.map(self.__EjecutarCarga, repeat(dataset), hotel_urls, repeat(paginate), repeat(data_attribute),repeat(value), file_prefix_hotels))

                    # El dataset no es iterable por propiedad
                    else:
                        url = str(self.__BASE_URL + attributes['endpoint'])
                        paginate = attributes.get('paginate', False)
                        data_attribute = attributes.get('data_attribute', None)
                        self.__EjecutarCarga(
                            dataset=dataset, 
                            url=url, 
                            paginate=paginate, 
                            data_in_json=data_attribute,
                            file_prefix=file_prefix_params
                        )
                    controller.postLastestSucccessExecution(
                        account=self.__M_ACCOUNT, 
                        datasource=self.__EXTRACTOR_NAME, 
                        dataset=dataset, 
                        field=attributes['incremental_field'], 
                        value=dict(value).get(attributes['incremental_field'])
                        )
            # En el caso de que el dataset no sea incremental
            else:
                if attributes['property_iterable']:
                    #print(f'El dataset {dataset} es iterable por propiedad')
                    hotelList = self.__getHotelList()
                    hotel_urls = []
                    file_prefix_hotels = []
                    #Generamos las dos listas para lanzar las cargas en paralelo
                    url = str(self.__BASE_URL + attributes['endpoint'])
                    paginate = attributes.get('paginate', False)
                    data_attribute = attributes.get('data_attribute', None)
                    for hotel in hotelList:
                        hotel_urls.append (str.replace(url,'{{propertyId}}',str(hotel)))
                        file_prefix_hotels.append (f'hotel{hotel}_')
                    # Lanzamos la carga de los hoteles en paralelo
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        # Ejecutar la función en paralelo para cada valor en la lista
                        list(executor.map(self.__EjecutarCarga, repeat(dataset), hotel_urls, repeat(paginate), repeat(data_attribute),repeat({}), file_prefix_hotels))

                # El dataset no es iterable por propiedad
                else:
                    url = str(self.__BASE_URL + attributes['endpoint'])
                    paginate = attributes.get('paginate', False)
                    data_attribute = attributes.get('data_attribute', None)
                    self.__EjecutarCarga(
                        dataset=dataset, 
                        url=url, 
                        paginate=paginate, 
                        data_in_json=data_attribute,
                        file_prefix=file_prefix
                    )
        print(f' EXTRACCION {dataset} FIN.')
    
    def __getParamSets(self, attributes):
        """Genera los conjuntos de parámetros para datasets incrementales"""
        param_sets = []
        if 'parameters' in attributes:
            # Generar combinaciones de parámetros
            # Por simplicidad, solo usamos valores por defecto
            param_set = {}
            for param_name, param_config in attributes['parameters'].items():
                param_set[param_name] = param_config.get('default_value')
            param_sets.append(param_set)
        else:
            param_sets.append({})
        return param_sets
   
    def __EjecutarCarga(self, dataset, url, paginate, data_in_json=None, params={}, file_prefix=''):
        # Ajustamos los parametros de la solicitud
        if paginate:
            page = self.__ENDPOINT_PAGES
        else:
            page = 2
        for page in range(1,page):
            params.update({"page":page})
            data_json = controller.readEndpoint(url=url, user=self.__USER, password=self.__PASSWORD, header=self.__HEADER, params=params)
            if data_json==None:
                break
            if data_in_json==None:
                if len(data_json)==0:
                    break
            else:
                if len(data_json[data_in_json])==0:
                    break
            
            # Realizamos el mapeo de campos y la conversión a pandas.DataFrame
            data_df = mappings.MapDataSet(dataset, data_json, data_in_json=data_in_json)
            
            # Procedemos a almacenar el fichero en el datalake
            # FUTURE INI
            # Reemplazar por el microservicio que se encarga de grabar estos datos
            file_prefix_final = file_prefix + 'page'+str(page)
            #print('file_prefix_final: ' + file_prefix_final)
            controller.saveStorage(account=self.__M_ACCOUNT, datasource=self.__EXTRACTOR_NAME, dataset=dataset, data_df=data_df, folder=None, filename=file_prefix_final)
            # FUTURE FIN

    def __getHotelList(self) -> list:
        try:
            url = str(self.__BASE_URL + self.__CONFIG['datasets']['property.user']['endpoint'])
            hoteles = controller.readEndpoint(url=url, user=self.__USER, password=self.__PASSWORD, header=self.__HEADER)
            lista_hoteles = []
            for hotel in hoteles['list']:
                lista_hoteles.append (hotel['id'])
            return lista_hoteles
        except Exception as e:
            msg=f'Ha habido en error en la objtención del listado de hoteles, error:{type(e)}: {e}.'
            print(msg)
            raise(msg)

    def __getParamSets(self, dataset, dataset_config:json) -> list:
        # Procedimiento que  genera una lista con los juegos de parámetros para pasar a request. Algo como [{},{},{}]
        # Primero se generan los parametros incrementales de INICIO a FIN [Ahora, Ayer, Año Actual] dependiendo de la logica del campo
        # FIN estará basado siguiendo este orden: Última ejecución, Parametro Global del Cliente, Parametro por defecto del conector
        # Luego a cada item de la lista se añaden el resto de parametros en caso de que existan
        # La solucion será:
        # Generamos una lista con los parametros incrementales
        # Generamos una lista con los parámetros no incrementales
        # Hacemos un producto cartesiano con el resultado

        param_sets_result = []

        date_format =  f"%Y-%m-%d"
        timestamp_format = f"%Y-%m-%dT%H:%M:%S.%f"
        
        # LOGICA DEL PROCEDICIMENTO:
        # Comprobamos que sea incremental y que tenga parametros, sino terminamos aqui
        if not dataset_config['incremental'] and dict(dataset_config).get('parameters')==None:
            return param_sets_result
        
        # Si el dataset tiene parametros incrementales 
        # 1) Evaluamos la ultima ejecución exitosa del dataset en caso de que exita, en su defecto se utilizarán los valores por defecto
        if dataset_config['incremental']:
            lastestExecution = controller.getLastestSucccessExecution(account=self.__M_ACCOUNT, datasource=self.__EXTRACTOR_NAME, dataset=dataset)
            # 2) Evaluamos del dataset los parametros que lo hacen incremental
            if lastestExecution != None:
                field = lastestExecution['incremental_field']
                value = lastestExecution['incremental_value']
            else:
                ##################################################
                # FUTURE Usamos el valor por defecto del cliente #
                ##################################################
                # Usamos el valor por defecto del conector
                # Obtenemos los parámetros por defecto del endpoint
                field = self.__CONFIG['datasets'][dataset]['incremental_field']
                value = self.__CONFIG['datasets'][dataset]['parameters'][field]['default_value']
        
            # 4) Generamos una lista con el parametro iterativo + el resto de parametros en caso de que exitan
            # Este caso es custom para cada conector ya que no sabemos a priori que significa cada parámetro
            match field:
                case 'year':
                    hoy = datetime.datetime.now(datetime.timezone.utc)
                    current_year = int(hoy.year)
                    exit
                    for year in range(int(value), current_year+1):
                        param_dupla = {field:year}
                        param_sets_result.append(param_dupla)
                case 'from':
                    inicio = datetime.datetime.strptime(value, date_format)
                    fin = datetime.datetime.now()+ datetime.timedelta(days=1) 
                    lista_fechas = [inicio + datetime.timedelta(days=x) for x in range(0, (fin-inicio).days)]

                    for fecha in lista_fechas:
                        param_dupla = {field:fecha.strftime(date_format), "to":fecha.strftime(date_format)}
                        param_sets_result.append(param_dupla)
                case 'date':
                    inicio = datetime.datetime.strptime(value, date_format)
                    fin = datetime.datetime.now() + datetime.timedelta(days=1)
                    lista_fechas = [inicio + datetime.timedelta(days=x) for x in range(0, (fin-inicio).days)]

                    for fecha in lista_fechas:
                        param_dupla = {field:fecha.strftime(date_format)}
                        param_sets_result.append(param_dupla)
                case 'modifiedFrom':
                    # Pasos:
                    # 1) Recibimos el dato de inicio desde value en formato timestamp_format_java, eso es: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
                    # 2) Lo pasamos a formato date tipo YYYY-MM-DD
                    inicio = datetime.datetime.strptime(value[0:10:1], date_format)
                    fin = datetime.datetime.now()+ datetime.timedelta(days=1)
                    # 3) Generamos el listado de fechas desde inicio a hoy
                    lista_fechas = [inicio + datetime.timedelta(days=x) for x in range(0, (fin-inicio).days)]
                    # 4) por cada fecha escribimos la dupla inicio y fin en formato timestamp_format_java
                    for fecha in lista_fechas:
                        ini = fecha.strftime(date_format)+'T00:00:00.000Z'
                        #print(ini)
                        fin = fecha.strftime(date_format)+'T23:59:59.999Z'
                        #print(fin)
                        param_dupla =  {field:ini, "modifiedTo":fin}
                        param_sets_result.append(param_dupla)
                case 'createdFrom':
                    # Pasos:
                    # 1) Recibimos el dato de inicio desde value en formato timestamp_format_java, eso es: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
                    # 2) Lo pasamos a formato date tipo YYYY-MM-DD
                    inicio = datetime.datetime.strptime(value[0:10:1], date_format)
                    fin = datetime.datetime.now()+ datetime.timedelta(days=1)
                    # 3) Generamos el listado de fechas desde inicio a hoy
                    lista_fechas = [inicio + datetime.timedelta(days=x) for x in range(0, (fin-inicio).days)]
                    # 4) por cada fecha escribimos la dupla inicio y fin en formato timestamp_format_java
                    for fecha in lista_fechas:
                        ini = fecha.strftime(date_format)+'T00:00:00.000Z'
                        #print(ini)
                        fin = fecha.strftime(date_format)+'T23:59:59.999Z'
                        #print(fin)
                        param_dupla =  {field:ini, "createdTo":fin}
                        param_sets_result.append(param_dupla)
        
        if dict(self.__CONFIG['datasets'][dataset]['parameters']).get('revenuePriceModelTypeId')!=None:
            values_to_add = self.__CONFIG['datasets'][dataset]['parameters']['revenuePriceModelTypeId']['default_value']
            final_result = []
            model_types = []
            for value in values_to_add:
                param_dupla = {'revenuePriceModelTypeId':value}
                model_types.append(param_dupla)
            
            for dict1, dict2 in product(param_sets_result,model_types):
                combinado = {**dict1, **dict2}
                final_result.append(combinado)
            param_sets_result = final_result
        
        return param_sets_result
    
    