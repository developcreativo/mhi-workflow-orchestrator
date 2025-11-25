import pandas as pd
import os
import numpy as np
import xgboost as xgb
from pathlib import Path
from datetime import date, datetime
import pandas as pd
from google.cloud import bigquery
from utils.controller import Controller
import numpy as np

class Forecaster:
    """
    Clase para entrenamiento, validación y predicción con Prophet para ocupación hotelera.
    """
    def __init__(self, _m_account: str, idHotel:list =None):
        self.__GCP_PROJECT_ID = str(os.environ.get('_M_PROJECT_ID'))
        self.__STORAGE_INTEGRATION_BUCKET =  str(os.environ.get('_M_BUCKET_DATA'))
        self._extractor_name = 'demandforecast'
        self._m_account = _m_account
        self.idHotel = idHotel
        self.controller = Controller(self._m_account, 'mind')
        self.cliente_bq = bigquery.Client()
        self.hoy = pd.to_datetime(date.today())
        self.hoy_este = date.today()
        self.fin_de_anio = pd.to_datetime(datetime(self.hoy.year, 12, 31))
        self.dias_restantes = (self.fin_de_anio - self.hoy).days
        self.forecast_inicio = pd.Timestamp("2025-06-01")
        self.hoy_formateado = self.hoy.strftime('%Y-%m-%d')
        self.targets =  ["RN", "ImporteAlojamientoNeto"]
        # Definir propiedades para configuración
        self.n_estimators = 100
        self.feature_cols = [
            "dia_semana", "mes", "dia_mes",
            "semana_ano", "anio", "fin_de_semana",
            "EstaAbierto", "Habitaciones"
        ]
        self.out_folder = os.path.join(os.getcwd(), "output_forecast")
        self.query_indicadores_hoteleros = """  
        SELECT * FROM `05_AIMachineLearning.IndicadoresEntrenamiento` 
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

    # ---------------- UTILIDADES ----------------
    def create_time_features(self,df):
        df["dia_semana"] = df["FechaEstancia"].dt.dayofweek
        df["mes"] = df["FechaEstancia"].dt.month
        df["dia_mes"] = df["FechaEstancia"].dt.day
        df["semana_ano"] = df["FechaEstancia"].dt.isocalendar().week.astype(int)
        df["anio"] = df["FechaEstancia"].dt.year
        df["fin_de_semana"] = df["dia_semana"].isin([5, 6]).astype(int)
        return df

    def business_rules_rn(self,pred, otb, abierto, cap):
        pred = np.where(abierto == 0, 0, pred)
        pred = np.maximum(pred, otb)
        pred = np.minimum(pred, cap)
        return pred

    def business_rules(self,pred, otb, abierto):
        pred = np.where(abierto == 0, 0, pred)
        return np.maximum(pred, otb)

    def walk_forward_mape(self,df, target, features, hotel_id, out_dir, min_train=180, step=7):

        df = df.sort_values("FechaEstancia").reset_index(drop=True)
        validaciones = []

        for i in range(min_train, len(df) - 1, step):
            fecha = df.loc[i, "FechaEstancia"]
            if fecha.date() > self.hoy_este:
                break
            y_true = df.loc[i, target]
            if y_true <= 0:
                continue

            train = df.iloc[:i]
            test = df.iloc[i:i+1]

            model = xgb.XGBRegressor(n_estimators=self.n_estimators, random_state=42, verbosity=0)
            model.fit(train[features], train[target])
            y_pred = model.predict(test[features])[0]

            error_abs = abs(y_true - y_pred)
            error_pct = error_abs / y_true

            validaciones.append({
                "Hotel": hotel_id,
                "Fecha": fecha.date(),
                "y_real": y_true,
                "y_pred": y_pred,
                "error_absoluto": error_abs,
                "error_pctual": error_pct * 100
            })

        if validaciones:
            df_val = pd.DataFrame(validaciones)
            path = out_dir / f"walkval_{hotel_id}_{target}.csv"
            df_val.to_csv(path, index=False)
            return df_val["error_pctual"].mean()
        else:
            return np.nan
    def forecast_hotel_wrapper(self, hotel, df):
        """
        Realiza predicciones de demanda para un hotel específico utilizando modelos de regresión XGBoost.
        """
        # Definición de columnas a utilizar en el análisis - utilizaremos esta lista para verificar
        required_cols = [
            "idHotel", "FechaEstancia", "RN", "ImporteAlojamientoNeto",
            "EstaAbierto", "Habitaciones"
        ]
        OUT_FOLDER = "output_forecast"
        out_dir = Path(OUT_FOLDER)
        out_dir.mkdir(exist_ok=True)

        # Verificamos que todas las columnas necesarias estén presentes
        for col in required_cols:
            if col not in df.columns:
                print(f"Advertencia: Columna {col} no encontrada en los datos")
        
        # Filtra los datos solo para el hotel especificado
        df = df[df["idHotel"] == hotel].copy()
        
        # Convierte la columna de fecha a formato datetime para manipulación temporal
        df["FechaEstancia"] = pd.to_datetime(df["FechaEstancia"])
        
        # Crea características temporales (día de semana, mes, etc.) para el modelo
        df = self.create_time_features(df)
        
        # Convierte variables categóricas/booleanas a formato numérico adecuado
        df["EstaAbierto"] = df["EstaAbierto"].astype(int)
        
        # Convierte el número de habitaciones a entero, reemplazando valores no numéricos con 0
        df["Habitaciones"] = pd.to_numeric(df["Habitaciones"], errors="coerce").fillna(0).astype(int)

        # Crea una copia del dataframe para preservar los datos originales
        dh = df.copy()
        
        # Inicializa el dataframe de salida con las columnas de identificación y fecha
        result_df = dh[["idHotel", "FechaEstancia"]].copy()

        # Inicializa el dataframe de salida con las columnas de identificación y fecha
        out = dh[["idHotel", "FechaEstancia"]].copy()

        # Itera sobre cada variable objetivo (RN: Reservas, ImporteAlojamientoNeto)
        for tgt in self.targets:
            # Filtra datos históricos (hasta la fecha actual definida como HOY)
            hist = dh[dh["FechaEstancia"].dt.date <= self.hoy_este]

            # Inicializa y entrena el modelo XGBoost con los datos históricos
            model = xgb.XGBRegressor(n_estimators=self.n_estimators, random_state=42, verbosity=0)
            model.fit(hist[self.feature_cols], hist[tgt])

            # Realiza predicciones para todos los datos (históricos y futuros)
            pred = model.predict(dh[self.feature_cols])

            # prueba
            # Aplica reglas de negocio específicas según la variable objetivo
            if tgt == "RN":
                # Para Reservas Netas, aplica reglas considerando capacidad máxima del hotel
                cap = dh["Habitaciones"].values
                pred = self.business_rules_rn(pred, dh[tgt].values, dh["EstaAbierto"].values, cap)
            else:
                # Para ImporteAlojamientoNeto, aplica reglas básicas sin considerar capacidad
                pred = self.business_rules(pred, dh[tgt].values, dh["EstaAbierto"].values)

            # Añade las predicciones y valores reales al dataframe de salida
            out[f"{tgt}_pred"] = pred
            out[f"{tgt}_real"] = dh[tgt].values

            # Calcula el error MAPE mediante validación progresiva (walk-forward)
            m_cv = self.walk_forward_mape(dh, tgt, self.feature_cols, hotel, out_dir)

            # Imprime resultados de la evaluación del modelo
            # print(f"\n▶ Hotel {hotel} – {tgt}")
            # print(f"   MAPE CV walk-forward:    {m_cv:6.2f}%")

            # Guardar predicciones
            # Fijamos variable que guarde la validación para produccion y para ocupacion
            target = "ocupacion" if tgt == "RN" else "produccion" if tgt == "ImporteAlojamientoNeto" else ""
            
            # Save metrics with the friendly name in the file path
            m_cv_value = m_cv if not pd.isna(m_cv) else 0
            
            # Create a metrics dataframe with validation results
            metrics_df = pd.DataFrame({
                "idHotel": [hotel],
                "Target": [tgt],
                "FriendlyTarget": [target],
                "MAPE": [float(m_cv_value)],  # Explicitly convert to float
                "FechaValidacion": [self.hoy_formateado]
            })
            
            # Ensure MAPE is float64 type
            metrics_df["MAPE"] = metrics_df["MAPE"].astype(np.float64)
            
            # Get the walk validation data if it exists
            walk_val_path = out_dir / f"walkval_{hotel}_{tgt}.csv"
            if walk_val_path.exists():
                walk_val_df = pd.read_csv(walk_val_path)
                
                # # Save detailed validation metrics to cloud storage
                # validation_path = f"EvaluacionModelosML/metricas_validacion_detalladas_forecast_{target}_hotel_{hotel}_{self.hoy_formateado}_{self._m_account}.parquet"
                # print("metricas_validacion_detalladas_forecast_")
                # print(walk_val_df)
                # self.controller.saveStorage(self._extractor_name, validation_path, walk_val_df)
            
            # Save summary metrics to cloud storage
            metrics_path = f"EvaluacionModelosML/metricas_validacion_multivariante_forecast_{target}_hotel_{hotel}_{self.hoy_formateado}_{self._m_account}.parquet"
            self.controller.saveStorage(self._extractor_name, metrics_path, metrics_df)
            print("metricas_validacion_multivariante_forecast_")
            # print(metrics_df)

            # Guardar resultados localmente
            csv_std = out_dir / f"forecast_{hotel}.csv"
            out.to_csv(csv_std, index=False)
            
            # Guardar en Cloud Storage
            try:
                # Añadir idFechaEstancia como formato YYYYMMDD en int64
                out['idFechaEstancia'] = out['FechaEstancia'].dt.strftime('%Y%m%d').astype(np.int64)
                
                # También guardamos como parquet para procesamiento eficiente
                storage_path = f"PrediccionesML/forecast_multivariante_hotel_{hotel}_{self.hoy_formateado}_{self._m_account}.parquet"
                self.controller.saveStorage(self._extractor_name, storage_path, out)
                
                print(f"   Resultados guardados en Cloud Storage: {storage_path}")
            except Exception as e:
                print(f"   Error al guardar en Cloud Storage: {str(e)}")
                print(f"   Los resultados solo se guardaron localmente en: {csv_std}")


    def iniciar_forecast(self,_m_account: str, idHotel: list = None):
        """
        Método para realizar un forecast de ocupación hotelera utilizando Prophet.
        """
        df = self.read_bq_data(
            query=self.query_indicadores_hoteleros,
            _m_account=self._m_account,
            idHotel=self.idHotel
        )
        # Hoteles a iterar
        hotel_ids = sorted(df["idHotel"].unique())

        for hotel in hotel_ids:
            self.forecast_hotel_wrapper(hotel,df)

