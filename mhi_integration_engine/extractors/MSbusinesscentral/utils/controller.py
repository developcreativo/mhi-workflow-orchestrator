import json, logging, os, time, base64
from google.cloud import secretmanager
from google.cloud import storage
from google.cloud import pubsub

__RETURN = {
    0:'fail',
    1:'success',
    2:'warning'
}
__GCP_PROJECT_ID = str(os.environ.get('_M_PROJECT_ID'))

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Controller:
    def __init__(self):
        """Initialize the controller for common operations"""
        # Get project ID from environment variable
        self.project_id = os.environ.get("_M_PROJECT_ID", os.environ.get("PROJECT_ID"))
        if not self.project_id:
            raise ValueError("PROJECT_ID environment variable not set")
        
        # Initialize clients
        self.secret_client = secretmanager.SecretManagerServiceClient()
        self.storage_client = storage.Client()
        self.publisher = pubsub.PublisherClient()
        
        # Define bucket names
        self.data_bucket = str(os.environ.get('_M_BUCKET_DATA'))
        self.incremental_bucket = str(os.environ.get('_M_BUCKET_LASTEST_EXECUTION'))

        # Define topic name for notifications
        self.notification_topic = "notifications"
        
    def getSecret(self, secret_name):
        """
        Get a secret from Secret Manager
        
        Args:
            secret_name (str): Name of the secret
            
        Returns:
            dict: Secret content as a dictionary
        """
        logger.info(f"Getting secret {secret_name}")
        
        # Build the resource name
        name = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        
        try:
            # Access the secret version
            response = self.secret_client.access_secret_version(request={"name": name})
            
            # Get the secret payload
            payload = response.payload.data.decode("UTF-8")
            
            # Parse JSON
            return json.loads(payload)
            
        except Exception as e:
            logger.error(f"Error getting secret {secret_name}: {str(e)}")
            raise
    
    def getLastestSucccessExecution(self, extractor_name, dataset_name, account,idCompany):
        """
        Get the latest successful execution data for incremental loads
        
        Args:
            extractor_name (str): Name of the extractor
            dataset_name (str): Name of the dataset
            account (str): Account identifier
            
        Returns:
            dict: Latest execution data or None
        """
        logger.info(f"Getting latest successful execution for {extractor_name}/{dataset_name}/{account}")
        
        try:
            # Get the bucket
            bucket = self.storage_client.bucket(self.incremental_bucket)
            
            # Create blob path
            blob_path = f"{extractor_name}/{dataset_name}/{account}_{idCompany}.json"
            blob = bucket.blob(blob_path)
            
            # Check if blob exists
            if not blob.exists():
                logger.info(f"No previous execution found at {blob_path}")
                return None
            
            # Download and parse the content
            content = blob.download_as_string()
            return json.loads(content)
            
        except Exception as e:
            logger.error(f"Error getting latest execution: {str(e)}")
            return None
    
    def postLastestSucccessExecution(self, account, extractor_name, dataset_name, incremental_value,idCompany):

        """
        Save the latest successful execution data for incremental loads
        
        Args:
            execution_data (dict): Execution data to save
            
        Returns:
            bool: True if successful, False otherwise
        """
        execution_data ={
                "account": account,
                "idCompany":idCompany,
                "data_source": extractor_name,
                "data_set": dataset_name,
                "incremental_strategy": "incremental",
                "incremental_field": "lastModifiedDateTime",
                "incremental_value": incremental_value
            }

        logger.info(f"Saving latest successful execution for {extractor_name}/{dataset_name}/{account}")
        
        try:
            # Get the bucket
            bucket = self.storage_client.bucket(self.incremental_bucket)
            
            # Create blob path
            blob_path = f"{extractor_name}/{dataset_name}/{account}_{idCompany}.json"
            blob = bucket.blob(blob_path)
            
            # Upload the content
            blob.upload_from_string(
                json.dumps(execution_data),
                content_type="application/json"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving latest execution: {str(e)}")
            return False
    
    def saveStorage(self, account, extractor_name, dataset_name, file_name, dataframe):
        """
        Save DataFrame to Google Cloud Storage as parquet
        
        Args:
            extractor_name (str): Name of the extractor
            dataset_name (str): Name of the dataset
            file_name (str): Name of the file to save
            dataframe (pandas.DataFrame): DataFrame to save
            
        Returns:
            str: GCS URI of the saved file
        """
        logger.info(f"Saving {len(dataframe)} rows to {file_name}")
            
        try:
            # Forzamos los datos de negocio
            if '@odata.etag' in dataframe.columns:
                dataframe = dataframe.rename(columns={'@odata.etag': '_odata_etag'})

            dataframe['_m_extracted_ts'] = int(time.time_ns())
            dataframe['_m_account'] = account
            dataframe['_m_datasource'] = extractor_name
            
            # Corregimos los tipos de datos
            dataframe = self.convertDataTypes(dataframe)  

            # Get the bucket
            bucket = self.storage_client.bucket(self.data_bucket)
            # dataframe blob path
            blob_path = f"int_{extractor_name}/{dataset_name}/{file_name}"
            blob = bucket.blob(blob_path)
            
            # Convert DataFrame to parquet and upload
            parquet_buffer = dataframe.to_parquet()
            blob.upload_from_string(parquet_buffer, content_type="application/octet-stream")
            
            gcs_uri = f"gs://{self.data_bucket}/{blob_path}"
            logger.info(f"Saved to {gcs_uri}")
            
            return gcs_uri
            
        except Exception as e:
            logger.error(f"Error saving to storage: {str(e)}")
            raise
  
    def convertDataTypes(self, dataframe):
        """
        This process change the data types
        
        Args:
            folder_name (str): Name of the folder (e.g., 'int_businesscentral')
            dataset_name (str): Name of the dataset (e.g., 'customers')
            file_name (str): Name of the file to save
            dataframe (pandas.DataFrame): DataFrame to save
            
        Returns:
            str: GCS URI of the saved file
        """
        logger.info(f"Force destination data types")
        
        try:
            # Log column types before conversion for debugging
            logger.debug(f"DataFrame columns and types before conversion: {dataframe.dtypes}")
            
            # Handle data type conversions for specific common fields
            # These common conversions ensure consistent types across files
            type_conversions = {
                # Timestamps
                'lastModifiedDateTime': 'datetime64[ns]',
                'modifiedDateTime': 'datetime64[ns]',
                'createdDateTime': 'datetime64[ns]',
                'invoiceDate': 'datetime64[ns]',
                'dueDate': 'datetime64[ns]',
                'documentDate': 'datetime64[ns]',
                'postingDate': 'datetime64[ns]',
                'shipmentDate': 'datetime64[ns]',
                # Numeric fields
                'quantity': 'float64',
                'unitPrice': 'float64',
                'discountAmount': 'float64',
                'discountPercent': 'float64',
                'totalAmount': 'float64',
                'subtotalAmount': 'float64',
                'taxAmount': 'float64',
                'creditAmount': 'float64',
                'debitAmount': 'float64',
                # Metadata fields
                '_m_extracted_ts': 'int64'
            }
            
            # Apply conversions only for columns that exist in the dataframe
            for col, dtype in type_conversions.items():
                if col in dataframe.columns:
                    try:
                        dataframe[col] = dataframe[col].astype(dtype)
                    except Exception as e:
                        logger.warning(f"Could not convert column {col} to {dtype}: {e}")
            
            logger.debug(f"DataFrame columns and types after conversion: {dataframe.dtypes}")
            
            return dataframe
        
        except Exception as e:
            error_msg = f"Error converting data types: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise


  
def __putMessageInBus(topic, message):
    publisher = pubsub.PublisherClient()
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id=__GCP_PROJECT_ID,
        topic=topic,  # Set this to something appropriate.
    )
    try:
        publisher.create_topic(name=topic_name)
        future = publisher.publish(topic_name, message, spam='eggs')
    except Exception as e:
        msg=f'La function {__name__} ha fallado'
        msg=f' El proceso de publicar un mensaje en el topic: {topic}, ha fallado.'
        msg+f' Se ha capturado el siguiente error:{type(e)}: {e}.'
        return(__RETURN[0])
    print(future.result())
    return(__RETURN[1])


def putNotification(type, subject,body,account=None,to_email=None,from_email=None, cc=None,bcc=None,template=None,data=None):
    # se queda en este fichero como auxiliar del repositorio del extractor
    topic = 'notifications'
    
    data = {
        'type':type,
        'subject': subject,
        'text':body,
        'account':account,
        'to_email': to_email,
        'from_email':from_email,
        'cc':cc,
        'bcc':bcc,
        'template': template,
        'data': data
    }

    try:
        future = __putMessageInBus(topic,  base64.b64encode(json.dumps(data).encode("utf-8")))
        print(subject)
        print(body)
    except Exception as e:
        msg=f'La function {__name__} ha fallado'
        print(msg)
        return(__RETURN[0])
    print(future)
    return(__RETURN[1])
