
import sys
import os
import pandas as pd
from google.cloud import bigquery
from utils.controller import Controller
from datetime import date, datetime
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
import contextlib
import joblib
import platform
from tempfile import gettempdir
from google.cloud import storage
import numpy as np
from datetime import timedelta
import statsmodels.api as sm

# import logging
# logging.getLogger('cmdstanpy').setLevel(logging.WARNING)
# logging.getLogger('prophet').setLevel(logging.WARNING)
class ProphetForecaster:
    """
    Clase para entrenamiento, validación y predicción con Prophet para ocupación hotelera.
    """
    def __init__(self, _m_account: str, idHotel:list =None):
        self.__GCP_PROJECT_ID = str(os.environ.get('_M_PROJECT_ID'))
        self.__STORAGE_INTEGRATION_BUCKET =  'ocean_data'
        self._extractor_name = 'demandforecast'
        self._m_account = _m_account
        self.idHotel = idHotel
        self.controller = Controller(self._m_account, 'mind')
        self.cliente_bq = bigquery.Client()
        self.hoy = pd.to_datetime(date.today())
        self.hoy_formateado = self.hoy.strftime('%Y-%m-%d')
        self.fin_de_anio = pd.to_datetime(datetime(self.hoy.year, 12, 31))
        self.dias_restantes = (self.fin_de_anio - self.hoy).days
        self.forecast_inicio = pd.Timestamp("2025-06-01")
        self.query_ocupacion = """  
        SELECT * FROM `05_AIMachineLearning.OcupacionEntrenamiento` 
        WHERE _m_account = '{}'
        {}
        """.format(
            self._m_account,
            "AND idHotel IN ('{}')".format("','".join(self.idHotel)) if self.idHotel else ""
        )
        
        self.query_habitaciones = """
       SELECT * FROM `05_AIMachineLearning.HabitacionesEntrenamiento` 
        WHERE _m_account = '{}'
        {}
        """.format(
            self._m_account,
            "AND idHotel IN ('{}')".format("','".join(self.idHotel)) if self.idHotel else ""
        )

        self.query_produccion = """
       SELECT * FROM `05_AIMachineLearning.ProduccionEntrenamiento` 
        WHERE _m_account = '{}'
        {}
        """.format(
            self._m_account,
            "AND idHotel IN ('{}')".format("','".join(self.idHotel)) if self.idHotel else ""
        )
    

    def validar_hoteles(self, _m_account: str, idHotel: list = None):
        """
        Valida si los hoteles especificados existen en la cuenta _m_account dada.
        Devuelve una tupla: (es_valido, hoteles_invalidos, hoteles_validos)
        """
        try:
            # Query to get all hotels for the specified account
            query_validacion = """
            SELECT idHotel
            FROM `04_model.HotelDim`
            WHERE _m_account = '{}'
            """.format(_m_account)
            
            # Get all hotels for the account
            df_hoteles = self.cliente_bq.query(query_validacion).to_dataframe()
            
            # If no hotels were found for the account
            if df_hoteles.empty:
                return False, [], []
                
            # Si no se solicitaron hoteles específicos, todos los hoteles de la cuenta son válidos
            if idHotel is None:
                return True, [], df_hoteles['idHotel'].tolist()
                
            # Verificar qué hoteles de la lista existen
            valid_hotels = []
            invalid_hotels = []
            account_hotels = set(df_hoteles['idHotel'].tolist())
            
            for hotel in idHotel:
                if hotel in account_hotels:
                    valid_hotels.append(hotel)
                else:
                    invalid_hotels.append(hotel)
            
            # Devuelve si todos los hoteles solicitados son válidos
            is_valid = len(invalid_hotels) == 0 and len(valid_hotels) > 0
            
            return is_valid, invalid_hotels, valid_hotels
            
        except Exception as e:
            raise Exception(f"Error validando hotels: {str(e)}")

    def read_bq_data(self, query: str, _m_account: str, idHotel: list = None):
        """
        Lee datos de BigQuery utilizando la consulta especificada.
        Filtra por idHotel y/o _m_account si se proporcionan.
        Valida los hoteles antes de realizar la consulta.
        """
        try:
            # Validar hoteles antes de proceder con la consulta
            es_valido, hoteles_invalidos, hoteles_validos = self.validar_hoteles(_m_account, idHotel)
            
            if not es_valido:
                if hoteles_invalidos:
                    raise Exception(f"Los siguientes hoteles no son validos para la cuenta {_m_account}: {', '.join(hoteles_invalidos)}")
                else:
                    raise Exception(f"No se han encontrado hoteles validos para {_m_account}")
                    
            # Continuar con la consulta usando solo hoteles válidos
            if idHotel and hoteles_validos != idHotel:
                # Reemplazar la consulta para usar solo hoteles válidos
                query = query.format(_m_account, "AND idHotel IN ('{}')".format("','".join(hoteles_validos)) if hoteles_validos else "")
            
            df_query = self.cliente_bq.query(query).to_dataframe()
            return df_query
        except Exception as e:
            raise Exception(f"Error en BigQuery: {str(e)}")
        
    def cargar_modelo_desde_gcs(self, bucket_name, blob_path, local_dir=None):
        if local_dir is None:
            local_dir = gettempdir()

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        local_path = os.path.join(local_dir, os.path.basename(blob_path))
        blob.download_to_filename(local_path)

        model = joblib.load(local_path)
        return model

    def predecir_futuro(self, dias_futuros, df_completo):
        print("PREDICIENDO FUTURO")
        resultados = []
        
        # If self.idHotel is None, use all unique hotels from df_completo
        hotels_to_process = self.idHotel if self.idHotel is not None else df_completo['idHotel'].unique()
        
        for hotel in hotels_to_process:
            print(f"Realizando predicción para el hotel: {hotel}")
            df_hotel = df_completo[df_completo['idHotel'] == hotel].copy()
            # Buscar el modelo más reciente para este hotel
            client = storage.Client()
            bucket = client.bucket(self.__STORAGE_INTEGRATION_BUCKET)
            prefix = f"int_mind/{self._extractor_name}/ModelosEntrenados/prophet_model_{hotel}_"
            blobs = list(bucket.list_blobs(prefix=prefix))
            
            if not blobs:
                raise Exception(f"No se encontró ningún modelo para el hotel {hotel}")
            
            # Ordenar por fecha de creación (más reciente primero)
            blobs.sort(key=lambda x: x.time_created, reverse=True)
            model_path = blobs[0].name
            
            print(f"Usando el modelo más reciente: {model_path}")
            model = self.cargar_modelo_desde_gcs(self.__STORAGE_INTEGRATION_BUCKET, model_path)

            df_a_futuro = model.make_future_dataframe(periods=dias_futuros)
            df_a_futuro['idHotel'] = hotel

            df_a_futuro_final = pd.merge(
                df_a_futuro,
                df_hotel[['ds', 'idHotel', 'cap', 'EstaAbierto']],
                how='left',
                on=['ds','idHotel']
            )

            df_a_futuro_final['EstaAbierto'] = df_a_futuro_final['EstaAbierto'].fillna(0)
            forecast = model.predict(df_a_futuro_final)

            forecast.loc[df_a_futuro_final['EstaAbierto'] == 0, 'yhat'] = 0
            forecast['yhat'] = forecast['yhat'].clip(lower=0)
            forecast['yhat_lower'] = forecast['yhat_lower'].clip(lower=0)

            forecast['idHotel'] = hotel
            forecast['idFecha'] = forecast['ds'].dt.strftime('%Y%m%d').astype('int64')
            forecast = forecast.merge(df_hotel[['ds', 'y']], on='ds', how='left')

            self.controller.saveStorage("demandforecast", f"PrediccionesML/forecast_ocupacion_hotel_{hotel}_{self.hoy_formateado}_{self._m_account}.parquet", forecast)
            resultados.append(forecast)

        return pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame()
        
    def crear_dataframe(self, _m_account: str, idHotel:list=None):
        """
        Crea un DataFrame a partir de los datos obtenidos de BigQuery.
        Si no se especifica un hotel, usa los datos del hotel definido en la instancia.
        """
        print(f"Creando dataframes para hotel: {idHotel if idHotel else 'todos los hoteles'} de la cuenta {self._m_account}")
        
        df_ocupacion = self.read_bq_data(
            query=self.query_ocupacion,
            idHotel=idHotel,
            _m_account=self._m_account
        )
        
        df_habitaciones = self.read_bq_data(
            query=self.query_habitaciones,
            idHotel=idHotel,
            _m_account=self._m_account
        )

        df_produccion = self.read_bq_data(
            query=self.query_produccion,
            idHotel=idHotel,
            _m_account=self._m_account
        )
        
        return df_ocupacion, df_habitaciones

    def preprocesar_datos(self, df_ocupacion, df_habitaciones):
        print("Transformando datos para Prophet")
        """
        Preprocesses the data for Prophet.
        """

        #Llamamos a la función crear_dataframe para obtener los dataframes de ocupación y habitaciones
        df_ocupacion, df_habitaciones,df_produccion = self.crear_dataframe(self._m_account, self.idHotel)
        # Convertir las fechas de idFechaEstancia a datetime
        df_ocupacion['FechaEstancia'] = pd.to_datetime(df_ocupacion['idFechaEstancia'], format='%Y%m%d')
        df_agrupado = df_ocupacion.groupby(['idHotel', 'FechaEstancia', 'EstaAbierto'])['RN'].sum().reset_index()

        df_habitaciones['FechaEstancia'] = pd.to_datetime(df_habitaciones['idFecha'], format='%Y%m%d')
        
        # Realizar el merge por idHotel y fecha
        df_agrupado_con_habitaciones = pd.merge(
            df_agrupado,
            df_habitaciones,
            how='left',
            left_on=['idHotel', 'FechaEstancia'],
            right_on=['idHotel', 'FechaEstancia']
        )
        # Crear la nueva columna 'cap' con el número de habitaciones máximo. Eso sirve para que el modelo entienda que el número de Rn predicho no sobrepase el número de habitaciones diario.
        # Fijamos también el floor a 0 para evitar valores negativos
        df_agrupado_con_habitaciones['cap'] = df_agrupado_con_habitaciones['Habitaciones']
        # Eliminar columnas auxiliares
        df_agrupado_con_habitaciones.drop(columns=['Habitaciones','_m_account','idFecha'], inplace=True)
        # Asegurarse de que 'cap' es float antes de rellenar con 1e-3
      
        df_agrupado_con_habitaciones['cap'] = df_agrupado_con_habitaciones['cap'].astype(float)
      
        df_agrupado_con_habitaciones['cap'] = df_agrupado_con_habitaciones['cap'].fillna(1e-3)

        # Forzamos el tipo de EstaAbierto a integer para evitar problemas con Prophet
        df_agrupado_con_habitaciones['EstaAbierto'] = df_agrupado_con_habitaciones['EstaAbierto'].astype(int)

        # Renombramos las columnas porque Prophet necesita este tipo de nomenclatura
        df_agrupado_con_habitaciones.rename(columns={"FechaEstancia": "ds", "RN": "y"}, inplace=True)
        hoy = pd.to_datetime(date.today())

        # Crear dos dataframes: uno para entrenamiento (hasta hoy) y otro completo para datos futuros
        prophet_df_datos_pasados = df_agrupado_con_habitaciones[df_agrupado_con_habitaciones['ds'] < hoy]
        prophet_df_completo = df_agrupado_con_habitaciones.copy()

        return prophet_df_datos_pasados,prophet_df_completo
    
    def guardar_modelo(self, dataset_name: str, file_name: str, local_path: str) -> str:
        """
        Guarda un archivo binario (como un modelo Prophet serializado) en GCS.

        Args:
            dataset_name: Carpeta raíz dentro del bucket (por ejemplo 'demandforecast').
            file_name: Nombre del archivo, incluyendo subcarpetas si es necesario (por ejemplo 'ModelosEntrenados/modelo.pkl').
            local_path: Ruta local del archivo en disco a subir.

        Returns:
            str: Ruta GCS donde se almacenó el archivo.
        """
        try:
            from google.cloud import storage

            storage_client = storage.Client()
            bucket = storage_client.bucket(self.__STORAGE_INTEGRATION_BUCKET)
            blob = bucket.blob(f"int_mind/{dataset_name}/{file_name}")
            blob.upload_from_filename(local_path)

            gcs_path = f"gs://{self.__STORAGE_INTEGRATION_BUCKET}/int_mind/{dataset_name}/{file_name}"
            print(f"✅ Modelo guardado correctamente en {gcs_path}")
            return gcs_path

        except Exception as e:
            msg = f"Error guardando el modelo en GCS: {str(e)}"
            print(msg)
            raise Exception(msg)


    def entrenamiento_y_evaluacion(self,prophet_df_datos_pasados,prophet_df_completo):
        """
        Entrena el modelo Prophet.
        """
        # Obtenemos listado de hoteles distintos
        hoteles = prophet_df_datos_pasados['idHotel'].unique()

        # Inicializar listas para guardar resultados de todos los hoteles
        df_metricas_total = []
        df_forecasts_total = []

        # Un modelo para cada hotel, así captura los patrones específicos de cada hotel.
        for hotel in hoteles:
            try:
                print(f"Iniciando entrenamiento y evaluación para el hotel: {hotel}")
                hotel_df = prophet_df_datos_pasados[prophet_df_datos_pasados['idHotel'] == hotel].copy()
                hotel_df['idHotel'] = hotel
                # Hacemos fit del modelo Se usa logístico para suavizar la curva. La bondad de prophet, entre otras, es que no hace falta normalizar los datos porque ya lo hace internamente.
                model = Prophet(growth='logistic')
                # Añadimos la columna 'EstaAbierto' al dataframe futuro ya que es un regresor que sabemos de antemano
                model.add_regressor('EstaAbierto')
                model.fit(hotel_df)
                
                filename = f"prophet_model_{hotel}_{self.hoy_formateado}_{self._m_account}.pkl"
                tmp_dir = "/tmp" if platform.system() != "Windows" else gettempdir()
                local_path = os.path.join(tmp_dir, filename)

                joblib.dump(model, local_path)

                # Subir a GCS con el nuevo método
                self.guardar_modelo("demandforecast", f"ModelosEntrenados/{filename}", local_path)
                
                # Intentar realizar la validación cruzada, pero capturar errores específicos
                try:
                    # ===================================================
                    # Validación cruzada
                    # horizon: cuánto queremos predecir hacia adelante (ej. 30 días)
                    # initial: cuánto usamos para entrenar inicialmente
                    # period: cada cuánto movemos el punto de corte
                    # ===================================================
                    df_validacion_cruzada = cross_validation(model, initial='730 days', period='30 days', horizon='180 days')
                    
                    # Pese a poner un cap en el modelo, este sigue prediciendo valores próximos a 0 pero no son 0. Añadimos lógica de negocio: Si el hotel está cerrado, yhat tiene ser 0
                    # Volvemos a añadir la columna 'EstaAbierto' al dataframe de validación cruzada porque corss validation elimina estas columnas.
                    df_validacion_cruzada = df_validacion_cruzada.merge(hotel_df[['ds', 'EstaAbierto']], on='ds', how='left')
                    df_validacion_cruzada['EstaAbierto'] = df_validacion_cruzada['EstaAbierto'].fillna(0)
                    df_validacion_cruzada.loc[df_validacion_cruzada['EstaAbierto'] == 0, 'yhat'] = 0

                    # Todo lo que esté por debajo de 0 (puede pasar) lo dejamos a 0 también.
                    df_validacion_cruzada['yhat'] = df_validacion_cruzada['yhat'].clip(lower=0)

                    # ===================================================
                    # Calcular métricas de evaluación
                    df_metricas = performance_metrics(df_validacion_cruzada)
                    df_metricas['idHotel'] = hotel
                    df_metricas_total.append(df_metricas)

                    # Calcular el MAE relativo (%)
                    mae_promedio = df_metricas['mae'].mean()
                    media_y_real = df_validacion_cruzada['y'].mean()
                    mae_relativo = (mae_promedio / media_y_real) * 100 if media_y_real > 0 else float('nan')
                    # Add the relative MAE to the metrics dataframe
                    df_metricas['mae_relativo'] = mae_relativo
                    df_metricas['horizon'] = df_metricas['horizon'].dt.days
                    # Guardar métricas
                    self.controller.saveStorage("demandforecast", f"EvaluacionModelosML/metricas_validacion_forecast_ocupacion_hotel_{hotel}_{self.hoy_formateado}_{self._m_account}.parquet", df_metricas)
                
                except Exception as e:
                    # Si hay un error en la validación cruzada, mostramos un warning pero continuamos
                    print(f"⚠️ WARNING: No se pudo realizar la validación cruzada para el hotel {hotel}: {str(e)}")
                    print(f"⚠️ El hotel {hotel} probablemente no tiene suficientes datos históricos. Continuando con la predicción...")
                    # Crear un dataframe de métricas vacío para mantener la consistencia
                    df_metricas = pd.DataFrame({
                        'idHotel': [hotel],
                        'horizon': ['N/A'],
                        'mae': [None],
                        'rmse': [None],
                        'mape': [None],
                        'mdape': [None],
                        'coverage': [None]
                    })
                    df_metricas_total.append(df_metricas)

                # ⚙️ PREPARACIÓN DE DATAFRAME FUTURO Y PREDICCIONES
                # ===================================================
                # Crear dataframe con fechas futuras 
                df_a_futuro = model.make_future_dataframe(periods=self.dias_restantes)
                df_a_futuro['idHotel'] = hotel
                    
                # Añadir datos necesarios del dataframe completo
                df_a_futuro_final = pd.merge(
                    df_a_futuro,
                    prophet_df_completo[['ds', 'idHotel', 'cap', 'EstaAbierto']],
                    how='left',
                    on=['ds','idHotel']
                )

                # Fill missing values in EstaAbierto with 0
                df_a_futuro_final['EstaAbierto'] = df_a_futuro_final['EstaAbierto'].fillna(0)
                # Realizar predicción
                forecast = model.predict(df_a_futuro_final)

                # Aplicar lógica de negocio
                forecast.loc[df_a_futuro_final['EstaAbierto'] == 0, 'yhat'] = 0
                forecast['yhat'] = forecast['yhat'].clip(lower=0)
                forecast['yhat_lower'] = forecast['yhat_lower'].clip(lower=0)
                
                # Añadir columnas necesarias
                forecast['idHotel'] = hotel
                forecast['idFecha'] = forecast['ds'].dt.strftime('%Y%m%d').astype('int64')
                
                # Unir valores reales 
                forecast = forecast.merge(hotel_df[['ds', 'y']], on='ds', how='left')
                df_forecasts_total.append(forecast)

                # Guardar predicciones
                self.controller.saveStorage("demandforecast", f"PrediccionesML/forecast_ocupacion_hotel_{hotel}_{self.hoy_formateado}_{self._m_account}.parquet", forecast)
                
            except Exception as e:
                print(f"❌ ERROR: No se pudo procesar el hotel: {hotel}: {str(e)}")
                print(f"Continuando con el siguiente hotel...")
                continue

        # Consolidar resultados
        df_metrics_final = pd.concat(df_metricas_total, ignore_index=True)
        df_forecast_final = pd.concat(df_forecasts_total, ignore_index=True)
        
        # # Guardar resultados
        # df_metrics_final.to_excel("metricas_validacion_por_hotel.xlsx", index=False)
        # df_forecast_final.to_excel("predicciones_futuras_modelo_por_hotel.xlsx", index=False)
        
        return df_metrics_final, df_forecast_final
    
    def iniciar_forecast_local(self):
        """
        Método para iniciar el proceso de entrenamiento y evaluación.
        Devuelve un resumen con estado
        """    
        try:
            df_ocupacion, df_habitaciones = self.crear_dataframe(self._m_account, self.idHotel)
            prophet_df_datos_pasados, prophet_df_completo = self.preprocesar_datos(df_ocupacion, df_habitaciones)
            df_metricas_total, df_forecasts_total = self.entrenamiento_y_evaluacion(prophet_df_datos_pasados, prophet_df_completo)
        
            resumen = {
                "status": "success",
                "cuenta": self._m_account,
                "hoteles_procesados": df_metricas_total['idHotel'].nunique(),
                "fecha_ejecucion": str(self.hoy.date())
            
        }
        except Exception as e:
            print(f"Error en iniciar_forecast para cuenta {self._m_account}: {e}")
            return {
                "status": "error",
                "cuenta": self._m_account,
                "error": str(e)
            }
        return resumen
    
    def iniciar_forecast(self):
        """
        Método para iniciar el proceso preiccions futuras dado un modelo ya entrenado.
        Devuelve un resumen con estado
        """    
        try:
            df_ocupacion, df_habitaciones = self.crear_dataframe(self._m_account, self.idHotel)
            prophet_df_datos_pasados, prophet_df_completo = self.preprocesar_datos(df_ocupacion, df_habitaciones)
            # Aquí ya tenemos un modelo entrenado y guardado en GCS
            forecast = self.predecir_futuro(self.dias_restantes, prophet_df_completo)

            resumen = {
                "status": "success",
                "cuenta": self._m_account,
                "hoteles_procesados": len(self.idHotel) if self.idHotel is not None else forecast['idHotel'].nunique(),
                "fecha_ejecucion": str(self.hoy.date()),
                "predicciones": forecast[['ds', 'yhat', 'idHotel']].tail().to_dict(orient='records')
            }               
        
        except Exception as e:
            print(f"Error en iniciar_forecast para cuenta {self._m_account}: {e}")
            return {
                "status": "error",
                "cuenta": self._m_account,
                "error": str(e)
            }
        return resumen
    def forecast_produccion(self, _m_account: str, idHotel:list=None):
        """
        Método para realizar un forecast de producción hotelera.
        """

        print(f"Creando dataframes para hotel: {idHotel if idHotel else 'todos los hoteles'} de la cuenta {self._m_account}")

        df_produccion = self.read_bq_data(
            query=self.query_produccion,
            idHotel=idHotel,
            _m_account=_m_account
        )
         # ---------------------------------------------
        # 1. Cargar datos
        # ---------------------------------------------
        # Convertir las fechas de idFechaEstancia a datetime
        df_produccion['FechaEstancia'] = pd.to_datetime(df_produccion['idFechaEstancia'], format='%Y%m%d')
        # Agrupar por idHotel, FechaEstancia y EstaAbierto, sumando ImporteAlojamientoNeto
        df = df_produccion.groupby(['idHotel', 'FechaEstancia', 'EstaAbierto'])['ImporteAlojamientoNeto'].sum().reset_index()

        # ---------------------------------------------
        # 3. Función de forecast naïve con fallback
        # ---------------------------------------------
        def naive_forecast(df_hotel: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
            df_hotel = df_hotel.set_index("FechaEstancia").sort_index()
            series = df_hotel["ImporteAlojamientoNeto"]
            abierto = df_hotel["EstaAbierto"]

            horizon = pd.date_range(start, end)
            pred = []
            abierto_pred = []

            for d in horizon:
                prev = d - timedelta(days=365)

                esta_abierto = (
                    abierto.loc[d] if d in abierto
                    else abierto.loc[prev] if prev in abierto
                    else 0
                )
                abierto_pred.append(esta_abierto)

                if esta_abierto == 0:
                    pred.append(0.0)
                    continue

                if prev in series and series.loc[prev] > 0:
                    pred.append(series.loc[prev])
                else:
                    # Buscar historial del mismo día de la semana anterior a prev
                    dow = d.weekday()  # día de la semana del día a predecir
                    hist = series[(series.index < prev) & (series.index.weekday == dow) & (series > 0)]

                    if len(hist) >= 3:
                        pred.append(hist.tail(7).mean())
                    else:
                        pred.append(series[series > 0].mean() if not series.empty else 0.0)


            return pd.DataFrame({
                "FechaEstancia": horizon,
                "PredictedImporteNeto": pred,
                "EstaAbierto_Pred": abierto_pred
            })

        # ---------------------------------------------
        # 4. Forecast por hotel
        # ---------------------------------------------
        forecast_list = []
        end_date = self.forecast_inicio + timedelta(days=self.dias_restantes)

        for hotel, group in df.groupby("idHotel"):
            fcast = naive_forecast(group, self.forecast_inicio, end_date)
            fcast.insert(0, "idHotel", hotel)
            forecast_list.append(fcast)

        forecast_df = pd.concat(forecast_list, ignore_index=True)

        # ---------------------------------------------
        # 5. Ajuste con valores reales ("on the books")
        # ---------------------------------------------
        real = df.groupby(["idHotel", "FechaEstancia"])["ImporteAlojamientoNeto"].sum().reset_index()

        merged = forecast_df.merge(real, on=["idHotel", "FechaEstancia"], how="left")
        merged["ImporteAlojamientoNeto"] = merged["ImporteAlojamientoNeto"].fillna(0)

        merged["PredictedImporteNeto"] = np.where(
            merged["PredictedImporteNeto"] < merged["ImporteAlojamientoNeto"],
            merged["ImporteAlojamientoNeto"],
            merged["PredictedImporteNeto"]
        )

        # ---------------------------------------------
        # 6. Guardar resultados por hotel en GCS
        # ---------------------------------------------
        resultados = []
        
        # Guardar para cada hotel
        for hotel in merged['idHotel'].unique():
            print(f"Guardando forecast para el hotel: {hotel}")
            hotel_forecast = merged[merged['idHotel'] == hotel].copy()
            
            # Añadir idFecha en formato YYYYMMDD
            hotel_forecast['idFecha'] = hotel_forecast['FechaEstancia'].dt.strftime('%Y%m%d').astype('int64')
            
            # Guardar en GCS como parquet
            self.controller.saveStorage(
                "demandforecast", 
                f"PrediccionesML/forecast_produccion_hotel_{hotel}_{self.hoy_formateado}_{_m_account}.parquet", 
                hotel_forecast
            )
            
            resultados.append(hotel_forecast)
        
        # Consolidar todos los resultados
        all_forecasts = pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame()
        
        # También guardamos una versión en CSV para Qlik si es necesario
        out = all_forecasts[["idHotel", "FechaEstancia", "PredictedImporteNeto", "EstaAbierto_Pred"]].copy()
        out["FechaEstancia"] = out["FechaEstancia"].dt.strftime("%d/%m/%Y")
        out["PredictedImporteNeto"] = out["PredictedImporteNeto"] \
            .map("{:,.2f}".format) \
            .str.replace(",", "X") \
            .str.replace(".", ",") \
            .str.replace("X", "")

        out.to_csv("forecast_365_hotels_ajustado_todos.csv", sep=";", index=False, encoding="utf-8-sig")
        
        return all_forecasts
    
    def forecast_sarima(self, _m_account: str, idHotel:list=None):
        """
        Método para realizar un forecast de producción hotelera usando SARIMA.
        Similar a forecast_produccion para permitir llamadas uniformes.
        """
        print(f"Creando dataframes para hotel: {idHotel if idHotel else 'todos los hoteles'} de la cuenta {_m_account}")
        
        df_produccion = self.read_bq_data(
            query=self.query_produccion,
            idHotel=idHotel,
            _m_account=_m_account
        )
        
        # Convertir las fechas de idFechaEstancia a datetime
        df_produccion['FechaEstancia'] = pd.to_datetime(df_produccion['idFechaEstancia'], format='%Y%m%d')
        # Agrupar por idHotel, FechaEstancia y EstaAbierto, sumando ImporteAlojamientoNeto
        df = df_produccion.groupby(['idHotel', 'FechaEstancia', 'EstaAbierto'])['ImporteAlojamientoNeto'].sum().reset_index()

        # Forecast por hotel
        resultados = []
        
        for hotel_id, hotel_data in df.groupby("idHotel"):
            print(f"Procesando hotel SARIMA: {hotel_id}")
            forecast = self.sarima_forecast(hotel_data, hotel_id)
            if not forecast.empty:
                # Añadir idFecha en formato YYYYMMDD
                forecast['idFecha'] = forecast['FechaEstancia'].dt.strftime('%Y%m%d').astype('int64')
                
                # # Guardar en GCS como parquet
                # self.controller.saveStorage(
                #     "demandforecast", 
                #     f"PrediccionesML/forecast_sarima_hotel_{hotel_id}_{self.hoy_formateado}_{_m_account}.parquet", 
                #     forecast
                # )
                
                # resultados.append(forecast)

        # Consolidar todos los resultados
        all_forecasts = pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame()
        
        # También guardamos una versión en CSV para Qlik si es necesario
        if not all_forecasts.empty:
            out = all_forecasts[["idHotel", "FechaEstancia", "PredictedImporteNeto", "EstaAbierto_Pred"]].copy()
            out["FechaEstancia"] = out["FechaEstancia"].dt.strftime("%d/%m/%Y")
            out["PredictedImporteNeto"] = out["PredictedImporteNeto"] \
                .map("{:,.2f}".format) \
                .str.replace(",", "X") \
                .str.replace(".", ",") \
                .str.replace("X", "")

            out.to_csv("forecast_sarima_todos.csv", sep=";", index=False, encoding="utf-8-sig")
        
        return all_forecasts

    def sarima_forecast(self, df_hotel: pd.DataFrame, hotel_id: str) -> pd.DataFrame:
        """
        Realiza la predicción con modelo SARIMA para un hotel específico
        """
        # Serie completa diaria
        min_date = df_hotel["FechaEstancia"].min()
        max_date = df_hotel["FechaEstancia"].max()
        full_idx = pd.date_range(min_date, max_date, freq='D')

        y = df_hotel.set_index("FechaEstancia")["ImporteAlojamientoNeto"].reindex(full_idx, fill_value=0)
        x = df_hotel.set_index("FechaEstancia")["EstaAbierto"].reindex(full_idx, fill_value=0)

        # Corte de entrenamiento
        y_train = y[y.index < self.forecast_inicio]
        x_train = x[y.index < self.forecast_inicio].values.reshape(-1, 1)

        # Entrenar modelo SARIMA
        try:
            model = sm.tsa.statespace.SARIMAX(
                y_train,
                order=(1, 1, 1),
                seasonal_order=(1, 1, 1, 7),
                exog=x_train,
                enforce_stationarity=False,
                enforce_invertibility=False
            ).fit(disp=False)
        except Exception as e:
            print(f"Error en modelo SARIMA para hotel {hotel_id}: {str(e)}")
            return pd.DataFrame()  # si falla el modelo, devolver vacío

        # Forecast
        horizon = pd.date_range(self.forecast_inicio, self.forecast_inicio + timedelta(days=self.dias_restantes - 1), freq='D')
        x_future = np.array([
            x.get(d, x.get(d - timedelta(days=365), 0))
            for d in horizon
        ]).reshape(-1, 1)

        try:
            y_pred = model.get_forecast(steps=self.dias_restantes, exog=x_future).predicted_mean
            y_pred = np.where(x_future.flatten() == 0, 0, y_pred)  # aplicar regla de apertura
            y_pred = np.maximum(y_pred, 0)  # no permitir valores negativos
        except Exception as e:
            print(f"Error en predicción SARIMA para hotel {hotel_id}: {str(e)}")
            return pd.DataFrame()

        # Crear DataFrame de resultados
        forecast_df =pd.DataFrame({
            "idHotel": hotel_id,
            "FechaEstancia": horizon,
            "PredictedImporteNeto": y_pred,
            "EstaAbierto_Pred": x_future.flatten()
        })
        # Guardar una version para qlik de forecast_df en CSV
        # También guardamos una versión en CSV para Qlik si es necesario
        out = forecast_df[["idHotel", "FechaEstancia", "PredictedImporteNeto", "EstaAbierto_Pred"]].copy()
        out["FechaEstancia"] = out["FechaEstancia"].dt.strftime("%d/%m/%Y")
        out["PredictedImporteNeto"] = out["PredictedImporteNeto"] \
            .map("{:,.2f}".format) \
            .str.replace(",", "X") \
            .str.replace(".", ",") \
            .str.replace("X", "")
        out.to_csv(f"forecast_sarima_hotel_{hotel_id}_{self.hoy_formateado}_{self._m_account}.csv", sep=";", index=False, encoding="utf-8-sig")
        print(forecast_df)
        # Resultado en DataFrame
        return forecast_df





        


# Ejemplo de uso:
# forecaster = ProphetForecaster(_m_account="fergus", idHotel=["BAHIA","BAHAMAS"])
# df_ocupacion, df_habitaciones = forecaster.crear_dataframe()

# prophet_df_datos_pasados, prophet_df_completo = forecaster.preprocesar_datos(df_ocupacion, df_habitaciones)

# df_metricas_total, df_forecasts_total = forecaster.entrenamiento_y_evaluacion(prophet_df_datos_pasados, prophet_df_completo)
