#!/usr/bin/env python3
# -*- coding: utf8 -*-
import json, os, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import product
from utils import controller

# CONSTANTES
# Valor con el que identifican el idHotel en un endpoint 

class Extraccion():
    def __init__(self, 
                 _m_account:str, 
                 connection_id:str,
                 flow_id:str=None,
                 task_id:str=None,
                 run_id:str=None,
                 start_date:str=None,
                 end_date:str=None,
                 hotel_IDs:dict=None
                 ):
        # Siempre evaluamos que nos llegue el campo _m_account
        if type(_m_account) == str:
            self.__M_ACCOUNT = _m_account
        else:
            raise 'El valor de mhi_account no es un valor válido'
        self.flow_id = flow_id
        self.task_id = task_id
        self.run_id = run_id
        self.connection_id = connection_id
        self.end_date = end_date
        
        # Instanciamos basandonos en el fichero de configuración
        # Dejamos la configuración en memoria para acceder rapidamente a ella cada vez que necesitemos
        __CONFIG_FILE = 'config.json'
        dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        dir_path_file = os.path.join(dir_path, __CONFIG_FILE)
        with open(dir_path_file, 'r') as f:
            self.__CONFIG =  json.load(f)
        # Obtenemos todas las variables generalistas de un extractor
        self.__EXTRACTOR_NAME = self.__CONFIG['extractor_name']
        self.__ENDPOINT_PAGES = self.__CONFIG['endpoint_pages']
        if hotel_IDs == None:
            self.__EXECUTION_HOTEL_LIST = None
        else:
            self.__EXECUTION_HOTEL_LIST = hotel_IDs
        if start_date == None:
            self.__EXECUTION_START_DATE = None
        else:
            self.__EXECUTION_START_DATE = start_date
        #self.__MASTER_DATASETS = self.__CONFIG['master_datasets']
        #self.__TRANSACTIONAL_DATASETS = self.__CONFIG['transactional_datasets']
        # Este trozo de codigo se tiene que cambiar por un endpoint general interno
        # FUTURE INI
        try:
            # Obtenemos el SECRET de conexion a la fuente
            secret = json.loads(controller.getSecretConnection(connection_id))
            self.__USER = secret['user'];
            self.__PASSWORD = secret['password'];
        except Exception as e:
            msg=f'Hay un error en obtener el secret, error:{type(e)}: {e}.'
            print(msg)
            raise(msg)
        # FUTURE FIN
        # Construcción de la Base URL
        self.__BASE_URL = str(self.__CONFIG['base_url'])
        self.__BASE_URL = self.__BASE_URL.replace('{{environment}}', secret['environment'])
        self.__BASE_URL = self.__BASE_URL.replace('{{version}}', self.__CONFIG['api_version'])
        self.__BASE_URL = self.__BASE_URL.replace('{{chainId}}', str(secret['chainId']))
        # Construcción de la Header
        self.__HEADER = {'Accept': 'application/json', 'content-type': 'application/json'}
    
    def Iniciar(self, datasets):
        print(f'INICIO EXTRACCION DATOS ULYSES CLOUD PARA LA CONEXION {self.connection_id}.')
        try:
            with ThreadPoolExecutor() as executor:
                futures = {executor.submit(self.__cargaDataset, ds): ds for ds in datasets}

                for fut in as_completed(futures):
                    ds = futures[fut]
                    try:
                        fut.result()  # esto propaga la excepción si ocurrió
                    except Exception as e:
                        # Cancelar el resto de tareas pendientes
                        for f in futures:
                            f.cancel()
                        msg = (
                            f"El método Iniciar ha fallado en dataset={ds}, "
                            f"error: {type(e).__name__}: {e}"
                        )
                        print(msg)
                        raise RuntimeError(msg) from e

        except Exception:
            raise  # vuelve a lanzar para no perder el traceback

        print(f'FIN EXTRACCION DATOS ULYSES CLOUD PARA LA CONEXI0N {self.connection_id}.')
        return True  # Devolver True si la extracción fue exitosa
    
        ###############################################
        ## FUTURE: 
        # Aqui va el mensaje en pubsub para informar que se ha terminado satisfactoriamente
        # La funcion que desencadena pubSub se encarga de ver los modulos asociados a este componente que dispone el cliente y lanza la recarga
        #controller.postDataTransformation(account=self.__M_ACCOUNT,datasource=self.__EXTRACTOR_NAME)  
        ###############################################
    
    def __cargaDataset(self, dataset):
        # Este procedimiento se encargará de leer los datos del dataset aplicando la lógica del conector
        print(f' EXTRACCION {dataset} INICIO')
        # Paso 1: Obtenemos los atributos del dataset
        attributes              = self.__CONFIG['datasets'][dataset]
        paginate                = attributes['paginate']
        try:
            data_attribute      = attributes['data_attribute']
        except Exception:
            data_attribute      = None
        
        file_prefix = ''
        # Generamos la url con el endpoint
        url = str(self.__BASE_URL + attributes['endpoint'])

        # Generamos el juego de parametros iterables
        param_sets = self.__getParamSets(dataset=dataset, dataset_config=attributes)    

        try:    
            # PASO 1: Evaluar si el dataset es incremental
            # Si es incremental >>
            if attributes['incremental']:
                # Iteramos en el juego de parámetros
                for value in param_sets:
                    # Generamos el prefijo del fichero con los parametros para guardar en el Data Lake
                    incr_value = str(dict(value).get(attributes['incremental_field']))
                    print(f'Integrando datos de {dataset} para el valor: {incr_value}')
                    file_prefix_params = file_prefix + attributes['incremental_field'] + incr_value + '_'
                    # Si el dataset es iterable por propiedad
                    if attributes['property_iterable']:
                        #print(f'El dataset {dataset} es iterable por propiedad')
                        hotelList = self.__getHotelList()
                        hotel_urls = []
                        file_prefix_hotels = []
                        #Generamos las dos listas para lanzar las cargas en paralelo
                        for hotel in hotelList:
                            hotel_urls.append (str.replace(url,'{{propertyId}}',str(hotel)))
                            file_prefix_hotels.append (f'hotel{hotel}_' + file_prefix_params)
                        # Lanzamos la carga de los hoteles en paralelo
                        resultados = []
                        errores = []
                        with ThreadPoolExecutor(max_workers=10) as executor:
                            # Crear un diccionario para rastrear qué future corresponde a qué hotel
                            futures = {
                                executor.submit(
                                    self.__EjecutarCarga,
                                    dataset, url, paginate,data_attribute, value, prefix
                                ): (url, prefix)
                                for url, prefix in zip(hotel_urls, file_prefix_hotels)
                            }
                            
                            # Procesar conforme van terminando
                            for future in as_completed(futures):
                                hotel_url, prefix = futures[future]
                                try:
                                    result = future.result()  # Esto lanza excepción si hubo error
                                    resultados.append({
                                        'hotel': prefix,
                                        'url': hotel_url,
                                        'estado': 'OK',
                                        'resultado': result
                                    })
                                    print(f"✓ Hotel {prefix} procesado correctamente")
                                except Exception as e:
                                    errores.append({
                                        'hotel': prefix,
                                        'url': hotel_url,
                                        'error': str(e)
                                    })
                                    print(f"✗ Error en hotel {prefix}: {e}")

                        # Verificar resultados
                        print(f"\n{'='*50}")
                        print(f"Total hoteles: {len(hotel_urls)}")
                        print(f"Exitosos: {len(resultados)}")
                        print(f"Fallidos: {len(errores)}")

                        if errores:
                            print(f"\nHoteles con errores:")
                            for error in errores:
                                print(f"  - {error['hotel']}: {error['error']}")
                            raise Exception(f"Fallaron {len(errores)} hoteles")

                    # El dataset no es iterable por propiedad
                    else:
                        self.__EjecutarCarga(
                            dataset=dataset, 
                            url=url, 
                            paginate=paginate, 
                            data_in_json=data_attribute, 
                            params=value,
                            file_prefix=file_prefix_params
                        )
                    controller.postLastestSucccessExecution(
                        account=self.__M_ACCOUNT, 
                        connection_id=self.connection_id,
                        datasource=self.__EXTRACTOR_NAME, 
                        dataset=dataset, 
                        field=attributes['incremental_field'], 
                        value=dict(value).get(attributes['incremental_field'])
                        )
            # En el caso de que el dataset no sea incremental
            else:
                if attributes['property_iterable']:
                    #print(f'El dataset {dataset} es iterable por propiedad')
                    hotelList = self.__getHotelList()
                    hotel_urls = []
                    file_prefix_hotels = []
                    #Generamos las dos listas para lanzar las cargas en paralelo
                    for hotel in hotelList:
                        hotel_urls.append (str.replace(url,'{{propertyId}}',str(hotel)))
                        file_prefix_hotels.append (f'hotel{hotel}_')
                    # Lanzamos la carga de los hoteles en paralelo
                    resultados = []
                    errores = []
                    value={}
                    with ThreadPoolExecutor(max_workers=10) as executor:
                        # Crear un diccionario para rastrear qué future corresponde a qué hotel
                        print("hola entramos al proceso")
                        futures = {
                            executor.submit(
                                self.__EjecutarCarga,
                                dataset, url, paginate, data_attribute, value, prefix
                            ): (url, prefix)
                            for url, prefix in zip(hotel_urls, file_prefix_hotels)
                        }
                        
                        # Procesar conforme van terminando
                        for future in as_completed(futures):
                            hotel_url, prefix = futures[future]
                            try:
                                result = future.result()  # Esto lanza excepción si hubo error
                                resultados.append({
                                    'hotel': prefix,
                                    'url': hotel_url,
                                    'estado': 'OK',
                                    'resultado': result
                                })
                                print(f"✓ Hotel {prefix} procesado correctamente")
                            except Exception as e:
                                errores.append({
                                    'hotel': prefix,
                                    'url': hotel_url,
                                    'error': str(e)
                                })
                                print(f"✗ Error en hotel {prefix}: {e}")

                    # Verificar resultados
                    print(f"\n{'='*50}")
                    print(f"Total hoteles: {len(hotel_urls)}")
                    print(f"Exitosos: {len(resultados)}")
                    print(f"Fallidos: {len(errores)}")

                    if errores:
                        print(f"\nHoteles con errores:")
                        for error in errores:
                            print(f"  - {error['hotel']}: {error['error']}")
                        raise Exception(f"Fallaron {len(errores)} hoteles")
                # El dataset no es iterable por propiedad
                else:
                    self.__EjecutarCarga(
                        dataset=dataset, 
                        url=url, 
                        paginate=paginate, 
                        data_in_json=data_attribute,
                        file_prefix=file_prefix
                    )
            print(f' EXTRACCION {dataset} FIN.')
        except BaseException as e:
            msg = f'El procedimiento __cargaDataset ha fallado para el dataset {dataset} dando el error {e}'
            print(msg)
            raise(msg)


    def __EjecutarCarga(self, dataset, url, paginate, data_in_json=None, params={}, file_prefix=''):
        try:
            if paginate:
                pages = self.__ENDPOINT_PAGES
            else:
                pages = 1
            
            for page in range(1,pages+1):
                print(params)
                params.update({"page":page})

                data_json = controller.readEndpoint(
                    url=url, 
                    user=self.__USER, 
                    password=self.__PASSWORD, 
                    header=self.__HEADER, 
                    params=params
                )
                
                if data_json == None:
                    break
                
                # Extraer los datos según data_attribute
                if data_in_json == None:
                    records = data_json if isinstance(data_json, list) else [data_json]
                else:
                    records = data_json.get(data_in_json, [])
                
                if len(records) == 0:
                    break
                
                file_prefix_final = file_prefix + 'page' + str(page)
                
                save_status = controller.saveStorage(
                    account=self.__M_ACCOUNT,
                    connection_id=self.connection_id, 
                    datasource=self.__EXTRACTOR_NAME, 
                    dataset=dataset, 
                    data_df=records, 
                    folder=None, 
                    filename=file_prefix_final,
                    flow_id=self.flow_id,
                    task_id=self.task_id,
                    run_id=self.run_id
                )
                if save_status == 'success':
                    continue
                else:    
                    raise
                    
        except BaseException as e:
            msg = f'El procedimiento __EjecutarCarga ha fallado para el dataset {dataset} dando el error {e}'
            print(msg)
            raise(msg)
        
    def __getHotelList(self) -> list:
        try:
            lista_hoteles = []
            if self.__EXECUTION_HOTEL_LIST == None:
                url = str(self.__BASE_URL + self.__CONFIG['datasets']['property.user']['endpoint'])
                hoteles = controller.readEndpoint(url=url, user=self.__USER, password=self.__PASSWORD, header=self.__HEADER)
                for hotel in hoteles['list']:
                    lista_hoteles.append (hotel['id'])
            else:
                lista_hoteles = self.__EXECUTION_HOTEL_LIST
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
        
        # LOGICA DEL PROCEDICIMENTO:
        # Comprobamos que sea incremental y que tenga parametros, sino terminamos aqui
        if not dataset_config['incremental'] and dict(dataset_config).get('parameters')==None:
            return param_sets_result
        
        # Si el dataset tiene parametros incrementales 
        # 1) Evaluamos la ultima ejecución exitosa del dataset en caso de que exita, en su defecto se utilizarán los valores por defecto
        if dataset_config['incremental']:
            if self.__EXECUTION_START_DATE == None:
                lastestExecution = controller.getLastestSucccessExecution(account=self.__M_ACCOUNT, connection_id=self.connection_id, datasource=self.__EXTRACTOR_NAME, dataset=dataset)
                # 2) Evaluamos del dataset los parametros que lo hacen incremental
                if lastestExecution != None:
                    field = lastestExecution['incremental_field']
                    value = lastestExecution['incremental_value']
                else:
                    ##################################################
                    # FUTURE Usamos el valor por defecto del cliente #
                    ##################################################
                    # Usamos el valor por defecto del conector
                    # Obtenemos los parámetros por defecto del 
                    field = self.__CONFIG['datasets'][dataset]['incremental_field']
                    value = self.__CONFIG['datasets'][dataset]['parameters'][field]['default_value']
            else:
                #print("ENTRAMOS POR EL START DATE EXECUTION")
                field = self.__CONFIG['datasets'][dataset]['incremental_field']
                value = self.__EXECUTION_START_DATE

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
                    inicio = datetime.datetime.strptime(value, date_format)+ datetime.timedelta(days=-1) 
                    fin = datetime.datetime.now()+ datetime.timedelta(days=1) 
                    lista_fechas = [inicio + datetime.timedelta(days=x) for x in range(0, (fin-inicio).days)]

                    for fecha in lista_fechas:
                        param_dupla = {field:fecha.strftime(date_format), "to":fecha.strftime(date_format)}
                        param_sets_result.append(param_dupla)
                case 'date':
                    inicio = datetime.datetime.strptime(value, date_format)+ datetime.timedelta(days=-1)
                    try:
                        # Si tiene configurado el parámeotr years_ahead, la fecha fin es el 31/12 de la cantidad de años indicados en este parámetro más el año actual
                        years_ahead = self.__CONFIG['datasets'][dataset]['years_ahead']
                        fin = datetime.datetime(datetime.datetime.now().year+years_ahead,12,31)+ datetime.timedelta(days=1)
                    except: 
                        fin = datetime.datetime.now() + datetime.timedelta(days=1)
                    lista_fechas = [inicio + datetime.timedelta(days=x) for x in range(0, (fin-inicio).days)]

                    for fecha in lista_fechas:
                        param_dupla = {field:fecha.strftime(date_format)}
                        param_sets_result.append(param_dupla)
                case 'modifiedFrom':
                    # Pasos:
                    # 1) Recibimos el dato de inicio desde value en formato timestamp_format_java, eso es: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
                    # 2) Lo pasamos a formato date tipo YYYY-MM-DD
                    inicio = datetime.datetime.strptime(value[0:10:1], date_format)+ datetime.timedelta(days=-1) 
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
                    inicio = datetime.datetime.strptime(value[0:10:1], date_format)+ datetime.timedelta(days=-1) 
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