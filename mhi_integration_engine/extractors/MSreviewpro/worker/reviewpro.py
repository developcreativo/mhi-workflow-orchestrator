import requests, json, os, time
from calendar import monthrange
from datetime import datetime
from utils import controller, mappings

class Extraccion():
    def __init__(self, _m_account):
        # Siempre evaluamos que nos llegue el campo _m_account
        if type(_m_account) == str:
            self.__M_ACCOUNT = _m_account
        else:
            raise ValueError('El valor de mhi_account no es un valor válido')
        # Instanciamos basandonos en el fichero de configuración
        # Dejamos la configuración en memoria para acceder rapidamente a ella cada vez que necesitemos
        __CONFIG_FILE = 'config.json'
        dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        dir_path_file = os.path.join(dir_path, __CONFIG_FILE)
        with open(dir_path_file, 'r') as f:
            self.__CONFIG =  json.load(f)
        # Obtenemos todas las variables generalistas de un extractor
        self.__EXTRACTOR_NAME = self.__CONFIG['extractor_name']
        self.__PROJECT_ID = os.environ.get('_M_PROJECT_ID')
        # Este trozo de codigo se tiene que cambiar por un endpoint general interno
        # FUTURE INI
        try:
            # Obtenemos el SECRET de conexion a la fuente
            secret = json.loads(controller.getSecret(account=self.__M_ACCOUNT,datasource=self.__EXTRACTOR_NAME))
            self.__CLIENT_FROM_DATE = secret['from_date'];
            self.__APIKEY = secret['apikey'];
        except Exception as e:
            msg=f'Hay un error en obtener el secret, error:{type(e)}: {e}.'
            print(msg)
            raise(msg)
        # FUTURE FIN
        # Construcción de la Base URL
        self.__BASE_URL = str(self.__CONFIG['base_url'])
        self.__BASE_URL = self.__BASE_URL.replace('{{api_version}}', self.__CONFIG['api_version'])
        
        
    def Iniciar(self, datasets):
        print(f'INICIO EXTRACCION DATOS REVIEWPRO PARA LA CUENTA {self.__M_ACCOUNT }.')
        try:
            for dataset in datasets: 
                self.__cargaDataset(dataset)
                time.sleep(1.2)

        except Exception as e:
            msg=f'El método iniciar ha fallado en alguno de sus subprocesos, error:{type(e)}: {e}.'
            print(msg)
            raise Exception(msg)
            
        print(f'FIN EXTRACCION DATOS PARA LA CUENTA {self.__M_ACCOUNT }.')

        ###############################################
        ## FUTURE: 
        # Aqui va el mensaje en pubsub para informar que se ha terminado satisfactoriamente
        # La funcion que desencadena pubSub se encarga de ver los modulos asociados a este componente que dispone el cliente y lanza la recarga
        #controller.postDataTransformation(account=self.__M_ACCOUNT,datasource=self.__EXTRACTOR_NAME)  
        ###############################################
        
        return True  # Devolver True si la extracción fue exitosa

    def __getHotelList(self) -> list:
        print ('Empezando extracción de IDs de Hotel')
        # Contruimos la url
        url = self.__BASE_URL + self.__CONFIG['datasets']['account.lodgings']['endpoint']
        url = url.replace('{{api_key}}',self.__APIKEY)
        # Enviamos petición GET a la API
        try:
            response = requests.get(url)
            # Comprobamos si la petición se ha realizado correctamente
            if response.status_code == 200:
                # Petición correcta:
                data = response.json()  # Convertimos respuesta JSON en diccionario
                hotelList = []
                for hotel in data:
                    hotelList.append(hotel['id'])
                return hotelList
            else:
                # La petición falla; hacemos print del error
                print(f'Extracción fallida: {response.status_code}')
        except Exception as e:
            msg=f'El metodo de extracción la lista de hoteles ha fallado' 
            print(msg)
            raise(msg)
        print ('Extracción de IDs de Hotel correcta')

    def __cargaDataset(self, dataset):
        # Este procedimiento se encargará de leer los datos del dataset aplicando la lógica del conector
        print(f' EXTRACCION {dataset} INICIO')
        # Paso 1: Obtenemos los atributos del dataset
        attributes              = self.__CONFIG['datasets'][dataset]
        paginate                = attributes['paginate']

        file_prefix = ''
        # Generamos la url con el endpoint
        url = str(self.__BASE_URL + attributes['endpoint'])
        url = url.replace('{{api_key}}',self.__APIKEY)

        # Generamos el juego de parametros iterables
        param_sets = self.__getParamSets(dataset=dataset, dataset_config=attributes)    
            
        # PASO 1: Evaluar si el dataset es incremental
        # Si es incremental >>
        if attributes['incremental']:
            # Iteramos en el juego de parámetros
            for value in param_sets:
                # Generamos el prefijo del fichero con los parametros para guardar en el Data Lake
                incr_value = str(dict(value).get(attributes['incremental_field']))
                #print(f'Integrando datos de {dataset} para el valor: {incr_value}')
                file_prefix_params = file_prefix + attributes['incremental_field'] + incr_value +'_'
                # Si el dataset es iterable por propiedad
                if attributes['property_iterable']:
                    #print(f'El dataset {dataset} es iterable por propiedad')
                    hotelList = self.__getHotelList()

                    #Generamos las dos listas para lanzar las cargas en paralelo
                    for hotel in hotelList:
                        hotel_url = str.replace(url,'{{pid}}',str(hotel))
                        file_prefix_hotel =f'hotel{hotel}_' + file_prefix_params
                        # Lanzamos la carga de los hoteles en paralelo
                        self.__EjecutarCarga(dataset,hotel_url, paginate, value, file_prefix_hotel)
                else:
                    continue
                controller.postLastestSucccessExecution(
                    account=self.__M_ACCOUNT, 
                    datasource=self.__EXTRACTOR_NAME, 
                    dataset=dataset, 
                    field=attributes['incremental_field'], 
                    value=dict(value).get(attributes['incremental_field'])
                    )
        
        print(f' EXTRACCION {dataset} FIN.')
    
    def __getParamSets(self, dataset, dataset_config:json) -> list:
        param_sets_result = []
        date_format =  f"%Y-%m-%d"
        
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
                field = self.__CONFIG['datasets'][dataset]['incremental_field']
                # Si no tiene ultima carga para la ultima ejecución buscamos el año de inicio del cliente y sino la fecha de inicio del conector
                try:
                    value = datetime.strftime(date_format, str(self.__CLIENT_FROM_DATE))
                except:
                    value = self.__CONFIG['datasets'][dataset]['parameters'][field]['default_value']
        
            # 4) Generamos una lista con el parametro iterativo + el resto de parametros en caso de que exitan
            # Este caso es custom para cada conector ya que no sabemos a priori que significa cada parámetro
            match field:
                case 'fd':
                    # Pasos:
                    # 1) El dato de value será siempre YYYY-MM-01 y el fin será:
                    # el ultimo dia del mes YYYY-MM-<ultimo dia mes> para meses cerrados
                    # hoy para meses abiertos
                    # La logica de la generación de la lista es la siguiente:
                    # Si el dia del mes es igual o menor que 10 se actualizará el mes anterior y este mes hasta hoy
                    # Se generarán las duplas desde el primer dia del mes hasta el ultimo dia del mes
                    today = datetime.today()
                    start_date_param = datetime.strptime(value, date_format)
                    # Inicializamos año y mes con la fecha de inicio
                    
                    year = start_date_param.year
                    month = start_date_param.month

                    # Iterate from the start date until the current month
                    while (year < today.year) or (year == today.year and month <= today.month):
                        first_day_of_month = datetime(year, month, 1)
                        
                        # If it's the current month, the last day is today, otherwise it's the last day of the month
                        if year == today.year and month == today.month:
                            last_day_of_month = today
                        else:
                            last_day_of_month = first_day_of_month.replace(day=monthrange(year, month)[1])

                        param_sets_result.append({
                            'fd': first_day_of_month.strftime(date_format),
                            'td': last_day_of_month.strftime(date_format)
                        })

                        # Move to the next month
                        if month == 12:
                            month = 1
                            year += 1
                        else:
                            month += 1

                    # If the provided date is the first day of the current month, add the previous month
                    if start_date_param.day == 1 and start_date_param.month == today.month and start_date_param.year == today.year:
                        previous_month = today.month - 1 if today.month > 1 else 12
                        previous_year = today.year if today.month > 1 else today.year - 1
                        first_day_previous_month = datetime(previous_year, previous_month, 1)
                        last_day_previous_month = first_day_previous_month.replace(day=monthrange(previous_year, previous_month)[1])
                        
                        param_sets_result.insert(0, {
                            'fd': first_day_previous_month.strftime(date_format),
                            'td': last_day_previous_month.strftime(date_format)
                        })
        return param_sets_result
    
    def __EjecutarCarga(self, dataset, url, paginate, params={}, file_prefix=''):
        # Ajustamos los parametros de la solicitud
        if paginate:
            page = self.__ENDPOINT_PAGES
        else:
            page = 2
        for page in range(1,page):
            params.update({"page":page})
            data_json = controller.readEndpoint(url=url, params=params)
            if data_json==None:
                break
            if len(data_json)==0:
                break
            
            # Realizamos el mapeo de campos y la conversión a pandas.DataFrame
            data_df = mappings.MapDataSet(dataset, data_json)
            
            # Procedemos a almacenar el fichero en el datalake
            # FUTURE INI
            # Reemplazar por el microservicio que se encarga de grabar estos datos
            file_prefix_final = file_prefix + 'page'+str(page)
            #print('file_prefix_final: ' + file_prefix_final)
            controller.saveStorage(account=self.__M_ACCOUNT, datasource=self.__EXTRACTOR_NAME, dataset=dataset, data_df=data_df, folder=None, filename=file_prefix_final)
            # FUTURE FIN