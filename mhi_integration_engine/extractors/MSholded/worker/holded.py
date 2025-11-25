#!/usr/bin/env python3
# -*- coding: utf8 -*-
import requests
import json
import datetime
import os
from utils import controller, mappings
from jinja2 import Environment
import concurrent.futures


class Extraccion():
    def __init__(self, _m_account, connection_id):
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
        # FUTURE INI
        # Este trozo de codigo se tiene que cambiar por un endpoint general interno
        try:
            # Obtenemos el APIKEY de conexion a la fuente
            apikey = controller.getSecret(connection_id)
        except Exception as e:
            msg=f'Hay un error en obtener la clave, error:{type(e)}: {e}.'
            print(msg)
            raise(msg)
        # FUTURE FIN
        self.__HEADER = {'Accept': 'application/json', 'content-type': 'application/json', 'key':apikey}
    
    def Iniciar(self, datasets):
        print(f'INICIO EXTRACCION HOLDED PARA CUENTA {self.__M_ACCOUNT}.')
        try:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Ejecutar la función en paralelo para cada valor en la lista
                list(executor.map(self.__ejecutarLogicaCargaDataSet, datasets))
                #print(resultados)
            
            ###############################################
            ## FUTURE: 
            # Aqui va el mensaje en pubsub para informar que se ha terminado satisfactoriamente
            # La funcion que desencadena pubSub se encarga de ver los modulos asociados a este componente que dispone el cliente y lanza la recarga
            #controller.postDataTransformation(account=self.__M_ACCOUNT,datasource=self.__EXTRACTOR_NAME)  
            ###############################################
            print(f'FIN EXTRACCION HOLDED PARA CUENTA {self.__M_ACCOUNT}.')
            return True
        except Exception as e:
            msg=f'El método iniciar ha fallado en alguno de sus subprocesos, error:{type(e)}: {e}.'
            print(msg)
            return False
    
    def __ejecutarLogicaCargaDataSet(self, dataset):
        # Este procedimiento se encargará de leer los datos del dataset aplicando la lógica del conector
        print(f' EXTRACCION {dataset} INICIO')
        # Obtenemos los parámetros del dataset
        endpoint        = self.__getDatasetAttribute(dataset, 'endpoint')
        paginate        = self.__getDatasetAttribute(dataset, 'paginate')
        incremental     = self.__getDatasetAttribute(dataset, 'incremental')
        parameters      = self.__getDatasetAttribute(dataset, 'parameters')
        
        # Tenemos el endpoint, los parámetros, la paginacion y si es incremental
        # Carga simple, no incremental, sin parámetros y sin paginacion
        # CARGA SIMPLE: no incremental, sin parámetros y sin paginacion
        # calculo valor fin
        hoy = datetime.datetime.now(datetime.timezone.utc)
        if not incremental and parameters==None:
            self.__EjecutarCarga(dataset=dataset, endpoint=endpoint, paginate=paginate)
        # CARGA PARAMETRIZADA:  no incremental, sin parámetros y sin paginacion
        if not incremental and parameters!=None:
            self.__EjecutarCarga(dataset=dataset, endpoint=endpoint, paginate=paginate, parameters=parameters)
        if incremental and parameters!=None:
            # Si el dataset contiene parámetros, hacemos:
            # 1) Evaluamos si el endpoint se ha utilizado antes y nos traemos la ultima carga
            lastestExecution = controller.getLastestSucccessExecution(self.__M_ACCOUNT, self.__EXTRACTOR_NAME, dataset=dataset)
            if lastestExecution != None:
                field = lastestExecution['incremental_field']
                value = lastestExecution['incremental_value']
            else:
                ##################################################
                # FUTURE Usamos el valor por defecto del cliente #
                ##################################################
                # Usamos el valor por defecto del conector
                # Obtenemos los parámetros por defecto del endpoint
                campos = self.__getDatasetIncrementalFields(dataset)
                field = campos[0]
                value = campos[1]
            # 2) Generamos una lista con los parámetros desde el valor que aparece en la ultima carga o en el valor por defecto 
            #    hasta el 31/12 del año actual, es importante que esta lista este ordenada y en los tramos no haya repeticiones  
            value_fin = int(datetime.datetime(hoy.year, 12, 31, 23, 59, 59,59, datetime.timezone.utc).timestamp())
            # Hacemos incrementos de medio año en medio año
            
            value_list = controller.generar_rango(int(value),int(value_fin))
            # 3) La carga se encarga de la paginación y en bucle se hace la carga en base a los parámetros 
            params  = parameters
            
            def recursivo(value_list:list):
                # generamos los parámetros de la carga
                if len(value_list)==1:
                    return None
                else:
                    try:
                        parameters = params
                        parameters["starttmp"], parameters["endtmp"] = value_list[0]
                        self.__EjecutarCarga(dataset=dataset, endpoint=endpoint, paginate=paginate, parameters=parameters)
                        value_list.remove(value_list[0])
                        recursivo(value_list)
                    except Exception as e:
                        print(f'ha habido una excepción: {e}')
            
            #Invocamos a la funcion recurrente
            recursivo(value_list)
        
        # Los parametros serán siempre el 01/01/YYYY por lo que acordamos contar con ese valor
        # En value tenemos el valor de inicio de las lecturas
        # En value_fin tendremos el ultimo dia del año actual
        if incremental:
            #  Verifica si la fecha actual es menor al 1 de julio
            if int(hoy.timestamp()) > int(datetime.datetime(hoy.year, 8, 1, 0, 0, 0, 0, datetime.timezone.utc).timestamp()):
                # Si es menor al 1 de Agosto, devuelve el 01/01 del año anterior
                fecha_resultado = datetime.datetime(hoy.year, 1, 1, 0, 0, 0, 0, datetime.timezone.utc).timestamp()
            else:
                # Si no es menor al 1 de julio, devuelve el 01/01 de este año
                fecha_resultado = datetime.datetime(hoy.year-1, 1, 1, 0, 0, 0, 0, datetime.timezone.utc).timestamp()
            # Convierte la fecha resultado a epoch (timestamp)
            latest_value = int(fecha_resultado)
            # Actualizamos el fichero de last success Execution
            # FUTURE: A Reemplazar por el microservicio que se encarga de esta tarea
            controller.postLastestSucccessExecution(account=self.__M_ACCOUNT, datasource=self.__EXTRACTOR_NAME, dataset=dataset, field=field, value=latest_value)
        
        print(f' EXTRACCION {dataset} FIN.')
   

    def __getDatasetAttribute(self, dataset, attribute):
        datasets = len(self.__CONFIG['datasets'])
        for i in range (0,datasets):
            if self.__CONFIG['datasets'][i]['name'] == dataset: 
                match attribute:
                    case 'endpoint':
                        environment = Environment()
                        template = environment.from_string(str(self.__CONFIG['datasets'][i]['endpoint']))
                        endpoint = template.render(base_url=str(self.__CONFIG['base_url']), api_version=str(self.__CONFIG['api_version']))
                        return endpoint
                    case 'parameters':
                        try:
                            parameters = self.__CONFIG['datasets'][i]['parameters']
                            params_out = {}
                            for item in parameters:
                                if item['datatype']=='string' and item['name'] == 'starttmp':
                                    params_out[item['name']] = "{{start}}"
                                if item['datatype']=='string' and item['name'] == 'endtmp':
                                    params_out[item['name']] = "{{end}}"
                                if item['datatype']=='integer' and item['name'] == 'start':
                                    params_out[item['name']] = 1
                                if item['datatype']=='integer' and item['name'] == 'end':
                                    params_out[item['name']] = 1
                                if item['datatype']=='boolean' and item['name'] == 'archived':
                                    params_out[item['name']] = True
                                if item['datatype']=='integer' and item['name'] == 'includeEmpty':
                                    params_out[item['name']] = 1
                            return json.loads(json.dumps(dict(params_out)))
                        except:
                            return None
                    case 'paginate': 
                        if self.__CONFIG['datasets'][i]['paginate']:
                            return True
                        else:
                            return False
                    case 'incremental': 
                        if self.__CONFIG['datasets'][i]['incremental']:
                            return True
                        else:
                            return False
        
        return None

    def __getDatasetIncrementalFields(self, dataset):
        for i in range (0,len(self.__CONFIG['datasets'])):
            if self.__CONFIG['datasets'][i]['name'] == dataset: 
                try:
                    field = self.__CONFIG['datasets'][i]['incremental_field']
                    value = self.__CONFIG['datasets'][i]['incremental_default_start']
                    return field,value
                except:
                    return None
                exit;
        return None
    
    def __obtenerDatosDelEndPoint(self, endpoint, params=None) -> json.dumps:
        try:
            if params != None:
                response = requests.get(endpoint, params=params, headers=self.__HEADER)
            else: 
                response = requests.get(endpoint, headers=self.__HEADER)
            if response.status_code == 200:
                return response.json()
            else:
                msg = f'Error al obtener los datos del endpoint: {response.status_code}: {response.text}'
                print(msg)
                raise(msg)
        except Exception as e:
            msg = f'El request para el endpoint: {endpoint} ha fallado, error:{type(e)}: {e}.'
            print(msg)
            raise(msg)

    
    def __EjecutarCarga(self, dataset, endpoint, paginate, parameters=None):
        
        if paginate:
            page = 500
        else:
            page = 2
        if parameters == None:
            parameters = {}
        
        for page in range(1,page):
            parameters["page"]=page
            data_json = self.__obtenerDatosDelEndPoint(endpoint=endpoint, params=parameters)

            if data_json==None or len(data_json)==0:
                break
            # Realizamos el mapeo de campos y la conversión a pandas.DataFrame
            data_df = mappings.MapDataSet(dataset, data_json)
            file_name = f'p{page}'
            try:
                file_name = f'{parameters['starttmp']}_{parameters['endtmp']}_{file_name}'
            except Exception as e:
                pass
            try:
                file_name = f'{str(parameters['start'])}_{str(parameters['end'])}_{file_name}'
            except Exception as e:
                pass
            # Procedemos a almacenar el fichero en el datalake
            # FUTURE INI
            # Reemplazar por el microservicio que se encarga de grabar estos datos
            try:
                
                controller.saveStorage(account=self.__M_ACCOUNT, datasource=self.__EXTRACTOR_NAME, dataset=dataset, data_df=data_df, folder=None, filename=file_name)
            except Exception as e:
                print('Excepción en el momento de guardar el archibvo')
            # FUTURE FIN
 