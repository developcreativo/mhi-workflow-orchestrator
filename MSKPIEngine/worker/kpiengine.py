import logging,os,datetime,json
from dateutil.relativedelta import *
import pandas as pd
import numpy as np
import networkx as nx
from google.cloud import bigquery
from utils.controller import Controller
import concurrent.futures
import threading

# Set up logging
#logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class CalculoKPIs:
    def __init__(self, account: str, idReporte: str, escenario: str = "ACTUAL"):
        self.account = account
        self.codigo_reporte = idReporte
        self.escenario = escenario
        self.cliente_bq = bigquery.Client()
        self._loadMasterData()
        self.datasetName = 'kpiengine'
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(os.path.dirname(script_dir), 'config.json')
        
        with open(config_path, 'r') as config_file:
            self.config = json.load(config_file)
    
        # Validate essential config elements
        if "extractor_name" not in self.config:
            raise ValueError("Missing 'extractor_name' in config.json")
        self.extractor_name = self.config["extractor_name"]
        self.controller = Controller(self.account, self.extractor_name)
        self.default_start_date = self.config.get("default_start_date", "2024-01-01")
        self.default_closing_day = self.config.get("default_closing_day")
        
        # Validar que el escenario sea válido
        if self.escenario not in ["ACTUAL", "PRESUPUESTO"]:
            raise ValueError(f"Escenario '{self.escenario}' no válido. Debe ser 'ACTUAL' o 'PRESUPUESTO'")
        
        logger.info(f"KPI Engine inicializado para escenario: {self.escenario}")
    
    def _loadMasterData(self):
        try:
            '''Esto se reemplazara por BIGQUERY'''
            base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
            #excel_path = os.path.join(base_path, "MapeoCuentasContables.xlsx")
            # self.Reportes = pd.read_excel(excel_path, sheet_name="Reportes")
            
            queryReportes = f"""
                SELECT *
                FROM `ocean_config.ReporteCabeceraAux`
                WHERE _m_account = '{self.account}'
            """
            
            resultadosReportes = self.cliente_bq.query(queryReportes).result()
            self.Reportes = resultadosReportes.to_dataframe()

            # self.MapeoContable = pd.read_excel(excel_path, sheet_name="MapeoContable")
            queryMapeo = f"""
                SELECT *
                FROM `ocean_config.ReporteMapeoCuentasContablesAux`
                WHERE _m_account = '{self.account}'
            """

            resultadosMapeo = self.cliente_bq.query(queryMapeo).result()
            self.MapeoContable = resultadosMapeo.to_dataframe()
            self.MapeoContable = self._normalizarDimensiones(self.MapeoContable)

            # self.Epigrafes = pd.read_excel(excel_path, sheet_name="Epigrafe")
            # self.Epigrafes = self.Epigrafes[self.Epigrafes['CodigoReporte'] == self.codigo_reporte]
            queryEpigrafes = f"""
                SELECT *
                FROM `ocean_config.ReporteEpigrafesAux`
                WHERE _m_account = '{self.account}' AND CodigoReporte = '{self.codigo_reporte}'
            """

            resultadosEpigrafes = self.cliente_bq.query(queryEpigrafes).result()
            self.Epigrafes = resultadosEpigrafes.to_dataframe()
            
            # self.Formulas = pd.read_excel(excel_path, sheet_name="Formulas")
            # self.Formulas = self.Formulas[self.Formulas['CodigoReporte'] == self.codigo_reporte]
            queryFormulas = f"""
                SELECT *
                FROM `ocean_config.ReporteFormulasAux`
                WHERE _m_account = '{self.account}' AND CodigoReporte = '{self.codigo_reporte}'
            """

            resultadosFormulas = self.cliente_bq.query(queryFormulas).result()
            self.Formulas = resultadosFormulas.to_dataframe()
            
            # self.Constantes = pd.read_excel(excel_path, sheet_name="Constantes")
            # self.Constantes = self.Constantes[self.Constantes['CodigoReporte'] == self.codigo_reporte]
            queryConstantes = f"""
                SELECT *
                FROM `ocean_config.ReporteConstantesAux`
                WHERE _m_account = '{self.account}' AND CodigoReporte = '{self.codigo_reporte}'
            """

            resultadosConstantes = self.cliente_bq.query(queryConstantes).result()
            self.Constantes = resultadosConstantes.to_dataframe()
            
            logger.info("✔ Archivos estáticos cargados exitosamente.")
        except Exception as e:
            msg=f"Error cargando archivos estáticos: {str(e)}"
            logger.error(msg)
            raise Exception(msg)

    def Iniciar(self, idPeriodo=None, escenario=None):
        '''
        Si no recibimos como parámetro idPeriodo --> Calculamos desde ultima carga exitosa, el periodo anterior hasta periodo actual
        Una vez terminamos de calcular los datos de para una cuenta/hotel/informe almacenamos la ultima carga exitosa
        '''
        try:
            # Si se pasa escenario como parámetro, lo usamos; sino usamos el del constructor
            if escenario is not None:
                if escenario not in ["ACTUAL", "PRESUPUESTO"]:
                    raise ValueError(f"Escenario '{escenario}' no válido. Debe ser 'ACTUAL' o 'PRESUPUESTO'")
                self.escenario = escenario
            
            logger.info(f"Iniciando procesamiento para escenario: {self.escenario}")
            logger.info("No se especificaron hoteles - Calculamos para todos los hoteles del cliente")
            
            query = f"""
                SELECT DISTINCT
                    CodigoHotel
                FROM `04_model.HotelDim`
                WHERE
                    _m_account = '{self.account}'
                    AND EstadoHotel = 'Activo'
            """
            resultados = self.cliente_bq.query(query).result()
            lista_hoteles = [row.CodigoHotel for row in resultados]
            
            if not lista_hoteles:
                logger.warning(f"No se encontraron hoteles para la cuenta: {self.account}")
                return
            logger.info(f"Se procesarán {len(lista_hoteles)} hoteles")

            # Obtener período actual en formato AAAAMM
            fecha_actual = datetime.datetime.now()
            periodo_actual = int(fecha_actual.strftime("%Y%m"))
            incremental_value = fecha_actual.strftime("%Y-%m-%d")
            
            # Determinar períodos a procesar
            periodos_a_procesar = self._obtenerPeriodosAProcesar(idPeriodo, periodo_actual)

            logger.info(f"PERIODOS A PROCESAR: {periodos_a_procesar}")

            # === PROCESAMIENTO PARALELO ===
            def procesar_hotel_completo(codigo_hotel):
                """
                Función auxiliar que procesa un hotel completo (todos sus períodos)
                """
                thread_name = threading.current_thread().name
                logger.info(f"[{thread_name}] Procesando hotel: {codigo_hotel} - Escenario: {self.escenario}")
                
                try:
                    if not periodos_a_procesar:
                        logger.info(f"[{thread_name}] No hay períodos para procesar para el hotel {codigo_hotel}")
                        return {'hotel': codigo_hotel, 'status': 'sin_periodos'}
                    
                    logger.info(f"[{thread_name}] Hotel {codigo_hotel}: se procesarán {len(periodos_a_procesar)} períodos: {periodos_a_procesar}")
                    
                    periodos_exitosos = []
                    periodos_con_error = []
                    
                    # Procesar cada período para este hotel (secuencial dentro del hotel)
                    for periodo in periodos_a_procesar:
                        logger.info(f"[{thread_name}] Iniciando cálculo para hotel {codigo_hotel}, período {periodo}, escenario {self.escenario}")
                        try:
                            self._IniciarCalculoidPeriodo(codigo_hotel, periodo)
                            logger.info(f"[{thread_name}] Cálculo completado exitosamente para hotel {codigo_hotel}, período {periodo}")
                            periodos_exitosos.append(periodo)
                            
                            # Registrar última ejecución exitosa por período
                            latest_exec_data = {
                                "account": self.account,
                                "data_source": self.extractor_name,
                                "data_set": f"{self.datasetName}",
                                "incremental_strategy": "incremental",
                                "incremental_field": "idPeriodo",
                                "incremental_value": fecha_actual.strftime("%Y%m"),
                                "last_executed_hotel": codigo_hotel
                            }
                            # self.controller.postLatestSuccessExecution(self.datasetName, latest_exec_data, self.escenario)
                            
                        except Exception as e:
                            logger.error(f"[{thread_name}] Error procesando hotel {codigo_hotel}, período {periodo}: {str(e)}")
                            periodos_con_error.append({'periodo': periodo, 'error': str(e)})
                            continue
                            
                    logger.info(f"[{thread_name}] Procesamiento completado para hotel {codigo_hotel}")
                    
                    return {
                        'hotel': codigo_hotel,
                        'status': 'completado',
                        'periodos_exitosos': periodos_exitosos,
                        'periodos_con_error': periodos_con_error,
                        'total_periodos': len(periodos_a_procesar)
                    }
                    
                except Exception as e:
                    logger.error(f"[{thread_name}] Error procesando hotel {codigo_hotel}: {str(e)}")
                    return {
                        'hotel': codigo_hotel,
                        'status': 'error_hotel',
                        'error': str(e)
                    }

            # === EJECUTAR EN PARALELO ===
            logger.info(f"Iniciando procesamiento paralelo de {len(lista_hoteles)} hoteles")
            
            # Configurar número de workers (ajustar según necesidades)
            max_workers = min(4, len(lista_hoteles))  # No más de 4 workers o número de hoteles
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                resultados_hoteles = list(executor.map(procesar_hotel_completo, lista_hoteles))
            
            # === PROCESAR RESULTADOS Y ESTADÍSTICAS ===
            hoteles_completados = 0
            hoteles_con_error = 0
            total_periodos_exitosos = 0
            total_periodos_con_error = 0
            
            for resultado in resultados_hoteles:
                if resultado['status'] == 'completado':
                    hoteles_completados += 1
                    total_periodos_exitosos += len(resultado.get('periodos_exitosos', []))
                    total_periodos_con_error += len(resultado.get('periodos_con_error', []))
                else:
                    hoteles_con_error += 1
            
            logger.info(    f"=== RESUMEN PROCESAMIENTO PARALELO ===")
            logger.info(    f"Hoteles completados: {hoteles_completados}/{len(lista_hoteles)}")
            logger.warning( f"Hoteles con error: {hoteles_con_error}")
            logger.info(    f"Períodos exitosos: {total_periodos_exitosos}")
            logger.warning( f"Períodos con error: {total_periodos_con_error}")
            
            # Registrar última ejecución global
            latest_exec_data = {
                "account": self.account,
                "data_source": self.extractor_name,
                "data_set": f"{self.datasetName}",
                "incremental_strategy": "incremental",
                "incremental_field": "idPeriodo",
                "incremental_value": incremental_value
            }
            self.controller.postLatestSuccessExecution(self.datasetName, latest_exec_data, self.escenario)
            msg=f"Evaluación finalizada exitosamente para todos los hoteles procesados - Escenario: {self.escenario}"
            logger.info(msg)
            return {"status": "success", "message": msg}
            
        except Exception as e:
            error_msg=f"Error en el proceso de cálculo: {str(e)}"
            logger.error(error_msg)
            return {"status": "fail", "message": error_msg}
            
    def _obtenerPeriodosAProcesar(self, idPeriodo, periodo_actual):
        
        try:
            if idPeriodo:
                # Se especificó un idPeriodo
                periodo_inicio = int(idPeriodo)
                logger.info(f"Período inicial especificado: {periodo_inicio}")
            else:
                # No se especificó idPeriodo, empezamos desde la ultima carga exitosa
                dataset_name = f"{self.datasetName}"
                logger.info(f"Obtenemos ultima carga exitosa para dataset: {dataset_name}")
                
                try:
                    ultima_carga = self.controller.getLatestSuccessExecution(dataset_name, self.escenario)
                except Exception as e:
                    logger.warning(f"No se pudo obtener última carga para {dataset_name}: {str(e)}")
                    ultima_carga = None
                    
                if ultima_carga:
                    fecha_string = ultima_carga['incremental_value']
                    fecha_ultima_carga = datetime.datetime.strptime(fecha_string, "%Y-%m-%d")
                    periodo_ultima_carga = int(fecha_ultima_carga.strftime("%Y%m"))
                    dia_ultima_carga = fecha_ultima_carga.day
                    
                    if dia_ultima_carga < self.default_closing_day:
                        # Si el dia < default_closing_day, empezar desde el mes anterior
                        fecha_inicio = fecha_ultima_carga - relativedelta(months=1)
                        periodo_inicio = int(fecha_inicio.strftime("%Y%m"))
                        logger.info(f"Última carga día {dia_ultima_carga} < {self.default_closing_day}, iniciando desde mes anterior: {periodo_inicio}")
                    else:
                        # Si el dia >= default_closing_day, empezar desde ese mes
                        periodo_inicio = periodo_ultima_carga
                        logger.info(f"Última carga día {dia_ultima_carga} >= {self.default_closing_day}, iniciando desde mismo mes: {periodo_inicio}")
                else:
                    try:
                        # Tampoco existe ultima carga exitosa, usar default_start_date
                        logger.warning(f"No existe ultima carga, procedemos a cargar desde {self.default_start_date}")
                        fecha_default = datetime.datetime.strptime(self.default_start_date, "%Y%m")
                        periodo_inicio = int(fecha_default.strftime("%Y%m"))
                        logger.warning(f"No existe una última carga exitosa para {self.escenario}, iniciando desde default_start_date: {periodo_inicio}")
                    except Exception as e:
                        logger.warning(f"Error encontrado en este bloque. Error: {str(e)}")
                
            # Generar lista de períodos desde periodo_inicio hasta periodo_actual
            periodos = []
            fecha_actual_periodo = datetime.datetime.strptime(str(periodo_inicio), "%Y%m")
            fecha_fin = datetime.datetime.strptime(str(periodo_actual), "%Y%m")
            
            while fecha_actual_periodo <= fecha_fin:
                periodo_str = int(fecha_actual_periodo.strftime("%Y%m"))
                periodos.append(periodo_str)
                fecha_actual_periodo += relativedelta(months=1)
            
            return periodos
            
        except Exception as e:
            logger.error(f"Error obteniendo período de ejecución: {str(e)}")
            return []

    def _IniciarCalculoidPeriodo(self, CodigoHotel: str, idPeriodo: str):
        logger.info(f"Evaluando idPeriodo {idPeriodo} para hotel {CodigoHotel} - Escenario: {self.escenario}")
        
        if self.escenario == "ACTUAL":
            # Flujo existente para datos reales
            libro_df = self._CargaLibroDiario(CodigoHotel, idPeriodo)
            libro_df = self._normalizarDimensiones(libro_df)
            libro_mapeado = self._EjecutarMapeoLibroDiario(libro_df)
            libro_agrupado = self._agruparIndicadores(libro_mapeado)
            
        elif self.escenario == "PRESUPUESTO":
            # Nuevo flujo para presupuestos
            presupuesto_df = self._CargaPresupuestos(CodigoHotel, idPeriodo)
            if presupuesto_df.empty:
                logger.warning(f"No se encontraron datos de presupuesto para hotel {CodigoHotel}, período {idPeriodo}")
                return
            libro_agrupado = self._MapearPresupuestosAIndicadores(presupuesto_df, CodigoHotel, idPeriodo)
        
        # El resto del flujo es común para ambos escenarios
        datos_base = self._evaluarFormulas(libro_agrupado)
        indicadores_fact = self._enriquecerEpigrafes(datos_base, CodigoHotel)
        
        # Guardar con identificador de escenario
        self._guardar_parquet(indicadores_fact, CodigoHotel, idPeriodo)

    def _CargaPresupuestos(self, CodigoHotel: str, idPeriodo: str) -> pd.DataFrame:
        """
        Cargar datos de presupuesto desde PresupuestoMesFact
        """

        query_presupuesto = f"""
            SELECT 
                a.idPeriodo,
                a.GrupoEpigrafe,
                a.Epigrafe,
                a.ImporteNeto as ImporteBalance,
                CASE
                    WHEN a.GrupoEpigrafe LIKE '%GAST%' OR a.GrupoEpigrafe LIKE '%CONS%' 
                        OR a.GrupoEpigrafe LIKE '%BI COC%' OR a.GrupoEpigrafe LIKE 'BI TRIBUTOS' 
                    THEN 'GASTOS'
                    WHEN a.GrupoEpigrafe LIKE '%ING%' THEN 'INGRESOS'
                    ELSE NULL
                END AS TipoMovimiento
            FROM `04_model.PresupuestoMesFact` a
            INNER JOIN `04_model.HotelDim` b 
                ON a.idHotel = b.idHotel
                AND b._m_account = '{self.account}'
            WHERE 
                a._m_account = '{self.account}' 
                AND b.CodigoHotel = '{CodigoHotel}'
                AND a.idPeriodo = {idPeriodo}
                AND a.TipoPresupuesto = 'OPERACIONES'
        """
        try:
            
            df_presupuesto = self.cliente_bq.query(query_presupuesto).to_dataframe()
            df_configuracion = self.Epigrafes.copy()
            
            #  Hago negativos los gastos
            df_presupuesto.loc[df_presupuesto['TipoMovimiento'] == 'GASTOS', 'ImporteBalance'] *= -1

            # 2. Renombrar columna de presupuesto para evitar conflictos
            df_presupuesto = df_presupuesto.rename(columns={'Epigrafe': 'EpigrafePresupuesto'})
            
            # 3. Identificar epígrafes duplicados EN PresupuestoMesFact
            epigrafes_gastos = set(df_presupuesto[df_presupuesto['TipoMovimiento'] == 'GASTOS']['EpigrafePresupuesto'])
            epigrafes_ingresos = set(df_presupuesto[df_presupuesto['TipoMovimiento'] == 'INGRESOS']['EpigrafePresupuesto'])
            epigrafes_duplicados = epigrafes_gastos.intersection(epigrafes_ingresos)
            
            logger.info(f"Epígrafes duplicados: {len(epigrafes_duplicados)}")
            if epigrafes_duplicados:
                logger.info(f"Ejemplos: {list(epigrafes_duplicados)[:3]}")
            
            # 4. Crear columna para hacer match
            def generar_epigrafe_match(row):
                if row['TipoMovimiento'] == 'GASTOS' and row['EpigrafePresupuesto'] in epigrafes_duplicados:
                    return 'CONSUMO ' + row['EpigrafePresupuesto']
                else:
                    return row['EpigrafePresupuesto']
            
            df_presupuesto['EpigrafeParaMatch'] = df_presupuesto.apply(generar_epigrafe_match, axis=1)
            
            # 5. Merge simple
            logger.info("Realizando merge...")
            df_resultado = df_presupuesto.merge(
                df_configuracion,
                left_on='EpigrafeParaMatch',
                right_on='Epigrafe',
                how='left'
            )
            
            logger.info(f"Registros después del merge: {len(df_resultado)}")
            logger.info(f"Registros con match: {len(df_resultado.dropna(subset=['CodigoEpigrafe']))}")
            
            # 6. Limpiar y seleccionar columnas finales
            columnas_finales = [
                'idPeriodo', 'GrupoEpigrafe', 'ImporteBalance', 'TipoMovimiento',
                'CodigoEpigrafe', 'Epigrafe', 'DescripcionEpigrafe', 'OrdenEpigrafe', 'Nivel'
            ]
            
            # Verificar qué columnas existen
            columnas_disponibles = [col for col in columnas_finales if col in df_resultado.columns]
            logger.info(f"Columnas en resultado final: {columnas_disponibles}")
            
            df_final = df_resultado[columnas_disponibles].copy()
            
            return df_final

        except Exception as e:
            logger.error(f"Error cargando presupuestos para hotel {CodigoHotel}, período {idPeriodo}: {str(e)}")
            return pd.DataFrame()

    def _MapearPresupuestosAIndicadores(self, presupuesto_df: pd.DataFrame, codigo_hotel: str, id_periodo: str) -> pd.DataFrame:
        """
        Transformar datos de presupuesto al MISMO formato que sale de _agruparIndicadores
        para que _evaluarFormulas reciba la estructura idéntica
        """
        try:
            if presupuesto_df.empty:
                logger.warning("DataFrame de presupuesto está vacío")
                return pd.DataFrame()
            
            # Filtrar solo registros con match válido y nivel 3
            df_validos = presupuesto_df.dropna(subset=['CodigoEpigrafe']).copy()
            df_validos = df_validos[df_validos['Nivel'] == 3].copy()
            
            if df_validos.empty:
                logger.warning("No se encontraron epígrafes válidos de nivel 3")
                return pd.DataFrame()
            
            # Crear estructura EXACTA que espera _evaluarFormulas
            resultado = pd.DataFrame({
                'idIndicador': df_validos['CodigoEpigrafe'],
                'idFechaContable': id_periodo,  # Usar directamente el período, no generar fecha
                'CodigoHotel': codigo_hotel,
                'idCuentaContable': 'PRESUPUESTO',
                'ImporteBalance': df_validos['ImporteBalance']
            })
            
            # Agregar Dimension2 a Dimension9 (todas con -1, igual que en ACTUAL)
            for i in range(2, 10):
                resultado[f'Dimension{i}'] = '-1'
            
            # Agrupar igual que en _agruparIndicadores
            group_cols = ['idIndicador', 'idFechaContable', 'CodigoHotel', 'idCuentaContable'] + [f'Dimension{i}' for i in range(2, 10)]
            resultado_final = resultado.groupby(group_cols)['ImporteBalance'].sum().reset_index()
            
            logger.info(f"Preparados {len(resultado_final)} indicadores de presupuesto (nivel 3)")
            return resultado_final
            
        except Exception as e:
            logger.error(f"Error en _MapearPresupuestosAIndicadores: {str(e)}")
            return pd.DataFrame()

    def _CargaLibroDiario(self, CodigoHotel: str, idPeriodo: str) -> pd.DataFrame:
        
        query = f"""
        SELECT 
            a.idFechaContable, a.NumeroCuentaContable, Dimension1,Dimension2,Dimension3,Dimension4,Dimension5,Dimension6,Dimension7,Dimension8,Dimension9,ImporteBalance
        FROM `04_model.LibroDiarioGlobalFact` a
        INNER JOIN `04_model.CalendarioFechaContableDim` b 
                ON a.idFechaContable = b.idFechaContable
        WHERE 
            a._m_account = '{self.account}' 
            AND a.Dimension1 = '{CodigoHotel}'
            AND b.idPeriodoContable = {idPeriodo}
        """
        df = self.cliente_bq.query(query).to_dataframe(create_bqstorage_client=False)
        #df['valor'] = df['ImporteHaber'].fillna(0) - df['ImporteDebe'].fillna(0)
        df['NumeroCuentaContable'] = df['NumeroCuentaContable'].astype(str)
        return df

    def _normalizarDimensiones(self, df: pd.DataFrame) -> pd.DataFrame:
        for i in range(1, 10):
            col = f'Dimension{i}'
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(-1).astype(int).apply(lambda x: f"{x:03}" if x != -1 else "-1")
        return df

    def _EjecutarMapeoLibroDiario(self, libro_df: pd.DataFrame) -> pd.DataFrame:
        try:
            self.MapeoContable['NumeroCuentaContable'] = self.MapeoContable['NumeroCuentaContable'].astype(str)
            
            dim_cols = [f'Dimension{i}' for i in range(1, 10)]
            dim_map = [col for col in dim_cols if col in self.MapeoContable.columns]
            
            campos_clave = ['NumeroCuentaContable'] + dim_map
            libro_estricto = pd.merge(libro_df, self.MapeoContable, how='inner', on=campos_clave)
            claves_unicas = libro_estricto[campos_clave].drop_duplicates()
            libro_restante = libro_df.merge(claves_unicas, on=campos_clave, how='left', indicator=True)
            libro_restante = libro_restante[libro_restante['_merge'] == 'left_only'].drop(columns=['_merge'])
        
            libro_relajado = libro_restante.merge(
                self.MapeoContable[['NumeroCuentaContable', 'CodigoFormula']],
                how='inner',
                on='NumeroCuentaContable'
            )

            return pd.concat([libro_estricto, libro_relajado], ignore_index=True)
        except Exception as e:
            logger.error("Ha fallado el procedimiento _EjecutarMapeoLibroDiario")
            raise(e)

    def _agruparIndicadores(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df = df.rename(columns={
                'CodigoFormula': 'idIndicador',
                'Dimension1': 'CodigoHotel',
                'NumeroCuentaContable': 'idCuentaContable'
            })

            group_cols = ['idIndicador', 'idFechaContable', 'CodigoHotel', 'idCuentaContable'] + [f'Dimension{i}' for i in range(2, 10) if f'Dimension{i}' in df.columns]
            df_agg = df.groupby(group_cols)['ImporteBalance'].sum().reset_index()
            return df_agg
        except Exception as e:
            msg = f"Ha fallado el metodo _agruparIndicadores"
            logger.info(msg)
            raise(msg)
        
    def _evaluarFormulas(self, datos_base: pd.DataFrame) -> pd.DataFrame:
        try:            
            codigos_calculados_existentes = set(datos_base['idIndicador'].unique())
            codigos_a_eliminar = []
            
            Formulas = self.Formulas.copy()
            Constantes = dict(self.Constantes[['idConstante', 'Valor']].values)

            formulas_calc = Formulas[Formulas['TipoFormula'].str.strip().str.upper() == 'CALCULADA'].copy()
            formulas_calc = formulas_calc.drop_duplicates(subset=['CodigoFormula'])

            if self.escenario == "PRESUPUESTO":
                # Para presupuestos, solo procesar fórmulas de nivel 1-2 
                epigrafes_a_calcular = self.Epigrafes[
                    (self.Epigrafes['Nivel'].isin([1, 2])) & 
                    (self.Epigrafes['CodigoReporte'] == self.codigo_reporte)
                ]['CodigoFormula'].unique()
                
                formulas_calc = formulas_calc[formulas_calc['CodigoFormula'].isin(epigrafes_a_calcular)]
                logger.info(f"PRESUPUESTO: Filtrando solo {len(formulas_calc)} fórmulas de niveles 1-2")
                if not formulas_calc.empty:
                    logger.info(f"Fórmulas a calcular: {list(formulas_calc['CodigoFormula'].values)}")
            else:
                # Para ACTUAL, procesar todas las fórmulas calculadas como antes
                logger.info(f"ACTUAL: Procesando todas las {len(formulas_calc)} fórmulas calculadas")
            
            for _, row in formulas_calc.iterrows():
                codigo = row['CodigoFormula']
                if codigo in codigos_calculados_existentes:
                    codigos_a_eliminar.append(codigo)
            
            if codigos_a_eliminar:
                logger.info(f"Eliminando códigos calculados que ya existen en datos_base: {codigos_a_eliminar}")
                datos_base = datos_base[~datos_base['idIndicador'].isin(codigos_a_eliminar)]
            
            resultado = datos_base.copy()

            formulas_calc['Dependencias'] = formulas_calc['Dependencias'].fillna('-1').apply(
                lambda x: [d.strip() for d in str(x).split(',') if d.strip()]
            )

            G = nx.DiGraph()
            for _, row in formulas_calc.iterrows():
                for dep in row['Dependencias']:
                    G.add_edge(dep, row['CodigoFormula'])

            if not nx.is_directed_acyclic_graph(G):
                raise ValueError("Hay ciclos en las dependencias entre fórmulas")

            orden_todos = list(nx.topological_sort(G))
            codigos_calculados = set(formulas_calc['CodigoFormula'])
            orden_final = [f for f in orden_todos if f in codigos_calculados]

            claves_base = ['idFechaContable', 'CodigoHotel']
            dimensiones = [f'Dimension{i}' for i in range(2, 10)]
            claves_totales = [col for col in claves_base + dimensiones if col in datos_base.columns]

            MapeoContable = self.MapeoContable.copy()
            for dim_col in dimensiones:
                if dim_col in MapeoContable.columns:
                    MapeoContable[dim_col] = MapeoContable[dim_col].fillna('-1').astype(str)

            granularidades = {}
            
            codigos_division = []
            
            for codigo_formula in orden_final:
                formula_row = Formulas[Formulas['CodigoFormula'] == codigo_formula]
                if not formula_row.empty and not pd.isna(formula_row['Formula'].iloc[0]):
                    formula_texto = formula_row['Formula'].iloc[0]
                    if '/' in formula_texto:
                        codigos_division.append(codigo_formula)
            
            if codigos_division:
                logger.info(f"Fórmulas con división identificadas: {codigos_division}")
            
            for codigo_formula in orden_final:
                if resultado[resultado['idIndicador'] == codigo_formula].shape[0] > 0:
                    logger.warning(f"El código {codigo_formula} ya existe en resultado, saltando...")
                    continue
                
                formula_row = Formulas[Formulas['CodigoFormula'] == codigo_formula]
                
                if formula_row.empty or pd.isna(formula_row['Formula'].iloc[0]):
                    logger.warning(f"Fórmula no encontrada o vacía para {codigo_formula}")
                    continue
                    
                formula_texto = formula_row['Formula'].iloc[0]
                
                for nombre_const, valor_const in Constantes.items():
                    if nombre_const in formula_texto:
                        formula_texto = formula_texto.replace(nombre_const, str(valor_const))
                
                dependencias_directas = formula_row['Dependencias'].iloc[0]
                if isinstance(dependencias_directas, str):
                    dependencias_directas = [d.strip() for d in dependencias_directas.split(',') if d.strip()]
                else:
                    dependencias_directas = []
                    
                try:
                    ancestros = list(nx.ancestors(G, codigo_formula))
                    dependencias_formula = list(set(dependencias_directas + ancestros))
                    dependencias_formula = [dep for dep in dependencias_formula 
                    if dep in formula_texto or dep in dependencias_directas]
                except Exception as e:
                    logger.error(f"Error obteniendo dependencias: {str(e)}")
                    dependencias_formula = dependencias_directas
                
                dataframes_dependencias = []
                claves_union = set(claves_base)
                
                for codigo_dependencia in dependencias_formula:
                    df_dependencia = resultado[resultado['idIndicador'] == codigo_dependencia].copy()
                    
                    if codigo_dependencia in MapeoContable['CodigoFormula'].values and not df_dependencia.empty:
                        filtros_codigo = MapeoContable[MapeoContable['CodigoFormula'] == codigo_dependencia]
                        
                        if not filtros_codigo.empty:
                            mascara = pd.Series(False, index=df_dependencia.index)
                            
                            for _, fila_filtro in filtros_codigo.iterrows():
                                mascara_temp = pd.Series(True, index=df_dependencia.index)
                                
                                for dim_col in dimensiones:
                                    if dim_col in fila_filtro.index and str(fila_filtro[dim_col]) != '-1' and dim_col in df_dependencia.columns:
                                        valor_filtro = str(fila_filtro[dim_col])
                                        mascara_temp &= (df_dependencia[dim_col].astype(str) == valor_filtro)
                                
                                mascara |= mascara_temp
                            
                            df_dependencia = df_dependencia[mascara]
                    
                    if df_dependencia.empty:
                        filas_base = resultado[claves_totales].drop_duplicates().to_dict('records')
                        if not filas_base:
                            filas_base = [{col: '-1' for col in claves_totales}]
                            
                        df_dependencia = pd.DataFrame(filas_base)
                        df_dependencia[codigo_dependencia] = 0
                        claves_dep = [col for col in claves_totales if col in df_dependencia.columns]
                    else:
                        claves_grupo = [col for col in claves_totales if col in df_dependencia.columns]
                        df_dependencia = (
                            df_dependencia
                            .groupby(claves_grupo, as_index=False)
                            .agg({'ImporteBalance': 'sum'})
                            .rename(columns={'ImporteBalance': codigo_dependencia})
                        )
                        claves_dep = claves_grupo
                    
                    granularidades[codigo_dependencia] = claves_dep
                    dataframes_dependencias.append((df_dependencia, claves_dep))
                    claves_union.update(claves_dep)
                
                if not dataframes_dependencias:
                    continue
                
                if self.escenario == "PRESUPUESTO":
                    # === MERGE SIMPLIFICADO PARA PRESUPUESTO ===
                    # En presupuesto, todas las dependencias tienen la misma granularidad
                    # Crear una sola fila con todas las dependencias como columnas
                    
                    # Obtener estructura base (claves comunes)
                    claves_base_merge = ['idFechaContable', 'CodigoHotel'] + [f'Dimension{i}' for i in range(2, 10)]
                    claves_disponibles = [col for col in claves_base_merge if col in resultado.columns]
                    
                    # Crear fila base con las claves
                    if not resultado.empty:
                        fila_base = resultado[claves_disponibles].iloc[0:1].copy()
                    else:
                        fila_base = pd.DataFrame([{col: '-1' for col in claves_disponibles}])
                    
                    df_merged = fila_base.copy()
                    
                    # Agregar cada dependencia como columna con su valor correcto
                    for codigo_dependencia in dependencias_formula:
                        # Buscar el valor de esta dependencia en resultado
                        df_dependencia = resultado[resultado['idIndicador'] == codigo_dependencia].copy()
                        
                        if not df_dependencia.empty:
                            # Tomar el valor de ImporteBalance
                            valor_dependencia = df_dependencia['ImporteBalance'].sum()
                            df_merged[codigo_dependencia] = valor_dependencia
                        else:
                            # Si no existe, usar 0
                            df_merged[codigo_dependencia] = 0
                    
                    # Asegurar que todas las columnas sean numéricas
                    for codigo_dependencia in dependencias_formula:
                        df_merged[codigo_dependencia] = pd.to_numeric(df_merged[codigo_dependencia], errors='coerce').fillna(0)
                else:
                    df_merged, claves_merged = dataframes_dependencias[0]
                    for df_next, claves_next in dataframes_dependencias[1:]:
                        columnas_comunes = list(set(claves_merged) & set(claves_next))
                        if not columnas_comunes:
                            df_merged['_key'] = 1
                            df_next['_key'] = 1
                            df_merged = pd.merge(df_merged, df_next, on='_key').drop('_key', axis=1)
                        else:
                            df_merged = pd.merge(df_merged, df_next, on=columnas_comunes, how='outer')
                        claves_merged = list(set(claves_merged) | set(claves_next))
                
                for codigo_dependencia in dependencias_formula:
                    if codigo_dependencia not in df_merged.columns:
                        df_merged[codigo_dependencia] = 0
                    df_merged[codigo_dependencia] = pd.to_numeric(df_merged[codigo_dependencia], errors='coerce').fillna(0)
                
                for col in dependencias_formula:
                    if col in df_merged.columns and granularidades.get(col) == claves_base:
                        df_merged[col] = df_merged.groupby(claves_base)[col].transform(
                            lambda x: [x.iloc[0]] + [0] * (len(x) - 1) if len(x) > 1 else x
                        )
                
                tiene_division = '/' in formula_texto
                
                try:
                    if tiene_division:
                        # Para fórmulas con división, usamos "sumar primero, dividir después"
                        partes = formula_texto.split('/')
                        numerador_expr = partes[0].strip()
                        denominador_expr = partes[1].strip()

                        numerador_total = df_merged.eval(numerador_expr).sum()
                        denominador_total = df_merged.eval(denominador_expr).sum()

                        if abs(denominador_total) < 0.00001:
                            result_value = 0
                        else:
                            result_value = numerador_total / denominador_total
                            if ((numerador_total < 0) != (denominador_total < 0)):
                                result_value = -abs(result_value)
                            else:
                                result_value = abs(result_value)

                        df_base = df_merged[claves_base].drop_duplicates()
                        df_resultado = df_base.copy()
                        df_resultado['idIndicador'] = codigo_formula
                        df_resultado['ImporteBalance'] = result_value
                        
                        if not df_resultado.empty:
                            columnas_minimas = set(claves_totales) - set(df_resultado.columns)
                            for col in columnas_minimas:
                                if col in df_merged.columns:
                                    df_resultado[col] = df_merged[col].iloc[0] if not df_merged.empty else '-1'
                                else:
                                    df_resultado[col] = '-1'
                            
                            df_resultado = df_resultado.iloc[0:1].copy()
                    else:
                        df_merged['ImporteBalance'] = df_merged.eval(formula_texto)
                        df_merged['idIndicador'] = codigo_formula

                        columnas_disponibles = set(df_merged.columns)
                        columnas_clave = set(claves_union) & columnas_disponibles
                        columnas_resultado = list(columnas_clave) + ['idIndicador', 'ImporteBalance']

                        columnas_faltantes = set(columnas_resultado) - columnas_disponibles
                        if columnas_faltantes:
                            for col in columnas_faltantes:
                                if col == 'idIndicador':
                                    df_merged[col] = codigo_formula
                                elif col == 'ImporteBalance':
                                    df_merged[col] = 0

                        df_resultado = (
                            df_merged[columnas_resultado]
                            .groupby([col for col in columnas_resultado if col != 'ImporteBalance'], as_index=False)
                            .agg({'ImporteBalance': 'sum'})
                        )

                    nan_count = df_resultado['ImporteBalance'].isna().sum()
                    inf_count = np.isinf(df_resultado['ImporteBalance']).sum()
                    if nan_count > 0 or inf_count > 0:
                        df_resultado['ImporteBalance'] = df_resultado['ImporteBalance'].replace([np.inf, -np.inf, np.nan], 0)

                    resultado = pd.concat([resultado, df_resultado], ignore_index=True)


                except Exception as e:
                    logger.error(f"Error evaluando fórmula {codigo_formula}: {str(e)}")
                    continue
                
            return resultado
        except Exception as e:
            msg = f"Ha fallado el método _evaluarFormulas: {str(e)}"
            logger.error(msg)
            raise Exception(msg)

    def _enriquecerEpigrafes(self, df: pd.DataFrame, CodigoHotel: str) -> pd.DataFrame:
        try:
            
            if 'CodigoHotel' in df.columns:
                df['CodigoHotel'] = CodigoHotel

            if self.escenario == 'PRESUPUESTO':
                if 'idCuentaContable' in df.columns:
                    df['idCuentaContable'] = 'N/A'
            
            columnas_epigrafes = [
                'CodigoFormula', 'Epigrafe', 'DescripcionEpigrafe',
                'OrdenEpigrafe', 'Nivel', 'CodigoReporte',
            ]
            df = df.merge(
                self.Epigrafes[columnas_epigrafes],
                how='left',
                left_on='idIndicador',
                right_on='CodigoFormula'
            )
            df.drop(columns=['CodigoFormula'], inplace=True, errors='ignore')
            return df
        except Exception as e:
            msg = f"Ha fallado el método _enriquecerEpigrafes con el error: {str(e)}"
            logger.error(msg)
            raise(msg)

    def _guardar_parquet(self, df: pd.DataFrame, CodigoHotel: str, idPeriodo: str):
        df.fillna({'idCuentaContable':'N/A'}, inplace=True)
        # df.fillna({'idFechaContable':99999999}, inplace=True)
        
        # Agregar escenario al nombre del archivo para diferenciarlo
        nombre_archivo = f"{idPeriodo}_{self.codigo_reporte}_{self.escenario}_{CodigoHotel}_{self.account}.parquet"
        
        # Agregar columna de escenario al DataFrame
        df['Escenario'] = self.escenario
        
        self.controller.saveStorage(self.datasetName, nombre_archivo, df, self.escenario.lower())

    def _guardar_excel(self, df: pd.DataFrame, CodigoHotel: str, idPeriodo: str):
        nombre_archivo = f"{idPeriodo}_{self.codigo_reporte}_{self.escenario}_{CodigoHotel}_{self.account}.xlsx"
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "out", nombre_archivo)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        try:
            with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name='indicadores_fact', index=False)
                self.Epigrafes.drop_duplicates(subset='CodigoFormula').rename(columns={'CodigoFormula': 'idIndicador'}).to_excel(writer, sheet_name='estructura_reporte_dim', index=False)
            logger.info(f"✔ Archivo XLSX guardado en: {path}")
        except Exception as e:
            logger.error(f"Error exportando a XLSX: {str(e)}")
