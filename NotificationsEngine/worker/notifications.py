from google.cloud import secretmanager
import os
import requests
import json
from jinja2 import Template

class Client:
    def __init__(self):
        # Obtenemos ficheros clave de la configuración del microservicio
        __CONFIG_FILE = 'config.json'
        dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        dir_path_file = os.path.join(dir_path, __CONFIG_FILE)
        with open(dir_path_file, 'r') as f:
            self.__CONFIG =  json.load(f)
        # Leemos los parametros del fichero de configuración
        self.__SERVICE_NAME = self.__CONFIG['service_name']
        #print(f'Iniciando el servicio {self.__SERVICE_NAME}. ')
        self.__SECRET_ID = self.__CONFIG['secret_id']
        self.__EMAILING_ENDPOINT = self.__CONFIG['endpoint']
        self.__DEFAULT_SENDER = self.__CONFIG['default_from']
        self.__ADMIN_EMAIL = self.__CONFIG['system_admin_email']

        # Obtenemos el secret para interactuar con el servicio de envio de emails
        self.secretmanager = secretmanager.SecretManagerServiceClient()
        __version_id = "latest"
        __GCP_PROJECT_ID = str(os.environ.get('_M_PROJECT_ID'))
        __SECRET_NAME = self.secretmanager.secret_path(__GCP_PROJECT_ID, self.__SECRET_ID) + "/versions/" + __version_id
        secret = self.secretmanager.access_secret_version(name=__SECRET_NAME)
        self.__apikey = secret.payload.data.decode("utf-8")
    
    # Controlador de procedimientos
    def executeNotification(self, _m_account, type, parameters):
        # Iniciamos el contructor del logging:
        #print(f'Iniciamos el Proceso executeNotification para la cuenta {_m_account}')
        try:
            # Identificamos el metodo de notificacion y redirigimos a un metodo de envio
            match type:
                case 'S':
                    response = self.__systemNotification(parameters)
                case 'N':
                    response = self.__businessAdminNotification(_m_account, parameters)
                case 'P':
                    response = self.__templateNotification(parameters)
            #msg=f'El proceso executeNotification a {_m_account} ha finalizado con ÉXITO'
            #print(msg)
            return response
        except BaseException as e:
            msg=f'El método executeNotification ha fallado: OS error: {e}.'
            print(msg)
            raise Exception(msg) 
    
    # Procedimiento que envía una notificación de sistema al administrador de Mind Ocean
    def __systemNotification(self, parameters):
        #print('Iniciamos el Proceso __systemNotification')
        try:
            parameters['template']='system_alert.html'
            parameters['to_email']=self.__ADMIN_EMAIL
            response = self.__templateNotification(parameters=parameters)
            #print('El proceso __systemNotification ha finalizado con ÉXITO.')
            return response
        except BaseException as e:
            msg=f'El método __systemNotification ha fallado: OS error: {e}.'
            print(msg)
            raise Exception(msg)
    
    # Procedimiento que envia un email al administrador de la cuenta
    def __businessAdminNotification(self, _m_account, parameters):
        #print('Iniciamos el procedimiento __businessAdminNotification ')
        try:
            parameters['template']='information_plain.html'
            # Aqui anadir el procedimiento que añade los email administradores de la cuenta
            # _m_account
            # parameters['to_email']=self.__ADMIN_EMAIL
            response = self.__templateNotification(parameters=parameters)
            #print('El proceso __businessAdminNotification ha finalizado con ÉXITO.')
            return response
        except BaseException as e:
            msg= f'El método __businessAdminNotification ha fallado: OS error: {e}.'
            print(msg)
            raise Exception(msg)
    
    def __templateNotification(self, parameters):
        try:
            final_html_content = self.__parseTemplate(template_name=parameters['template'], parameters=parameters)
            response =  self.__sendMessage(
                from_email = self.__DEFAULT_SENDER,
                to_email = parameters['to_email'],
                subject = parameters['subject'],
                text = parameters['text'],
                html = final_html_content
                )
            #print('El proceso __templateNotification ha finalizado con ÉXITO.')
            return response
        except BaseException as e:
            msg=f'El método __templateNotification ha fallado: OS error: {e}.'
            print(msg)
            raise(msg)

    # Procedimiento que parsea el template y general el html
    def __parseTemplate(self, template_name, parameters=[]):
        #print('Iniciamos el Proceso __templateParser')
        try:
            # Obtenemos la plantilla
            dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../templates'))
            dir_path_file = os.path.join(dir_path, template_name)
            with open(dir_path_file, 'r') as f:
                html = f.read()
            html = Template(html)
            html = html.render(parameters)
            
            #print('El proceso __templateParser ha finalizado con ÉXITO.')   
            return html
        
        except BaseException as e:
            msg=f'El método __templateParser ha fallado: OS error: {e}.'
            print(msg)
            raise(msg)
    
    # Procedimiento que envia el email 
    def __sendMessage(self, to_email, subject, html, text='', from_email='', cc='', cco=''):
        #print('Iniciamos el proceso de__sendMessage')
        try:
            data={
                "from": from_email,
                "to": to_email,
                "subject": subject,
                "html":html,
                "text": text,
                "cc":cc,
                "cco":cco}
            
            response = requests.post(
                self.__EMAILING_ENDPOINT,
                auth=("api",self.__apikey),
                data=data
                )
            
            if response.status_code == 200:
                print(f'Se ha enviado un email a {to_email} con ÉXITO.')
            else:
                print(f'El envío del email a {to_email} ha fallado, razón: {response.text}')
            return response
        except BaseException as e:
            msg=f'El método __sendMessage ha fallado: OS error: {e}.'
            print(msg)
            raise(msg)

    
        
    
        
    
        

    
   


    
    
    
            

    
    
    

    

    

    

