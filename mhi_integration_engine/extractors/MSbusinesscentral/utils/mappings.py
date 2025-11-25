"""
Mapeo de tipos de datos y campos para el conector Business Central.
"""
import pandas as pd
import numpy as np

def get_incremental_field_mapping():
    """
    Retorna mapeo de dataset a campo incremental
    
    Returns:
        dict: Mapeo de nombres de dataset a su campo incremental
    """
    return {
        "generalLedgerEntries": "lastModifiedDateTime",
        "salesCreditMemo": "lastModifiedDateTime",
        "salesInvoice": "lastModifiedDateTime",
        "salesOrder": "lastModifiedDateTime",
        "salesShipment": "lastModifiedDateTime",
        "purchaseCreditMemo": "lastModifiedDateTime",
        "purchaseInvoice": "lastModifiedDateTime",
        "purchaseOrder": "lastModifiedDateTime",
    }

def get_field_types_mapping():
    """
    Retorna mapeo de tipos de datos para cada dataset y campo
    
    Returns:
        dict: Mapeo de dataset/campo a tipo de datos
    """
    return {
        "generalLedgerEntries": {
            "lastModifiedDateTime": "string",
            "postingDate": "string",
            "debitAmount": "float64",
            "creditAmount": "float64",
            "additionalCurrencyDebitAmount":"float64",
            "additionalCurrencyCreditAmount":"float64"
        },
        "salesInvoice": {
            "discountAmount": "float64", 
            "totalAmountExcludingTax": "float64",
            "totalTaxAmount": "float64",
            "totalAmountIncludingTax": "float64",
            "lastModifiedDateTime": "datetime64[ns]",
            "invoiceDate": "datetime64[ns]",
            "dueDate": "datetime64[ns]"
        },
        "salesInvoiceLine": {
            "unitPrice": "float64",
            "quantity": "float64",
            "discountAmount": "float64",
            "discountPercent": "float64",
            "netAmount": "float64",
            "amountExcludingTax": "float64",
            "taxAmount": "float64",
            "amountIncludingTax": "float64"
        },
        "purchaseInvoice": {
            "discountAmount": "float64",
            "totalAmountExcludingTax": "float64",
            "totalTaxAmount": "float64",
            "totalAmountIncludingTax": "float64",
            "lastModifiedDateTime": "datetime64[ns]",
            "invoiceDate": "datetime64[ns]",
            "dueDate": "datetime64[ns]"
        },
        "purchaseInvoiceLine": {
            "unitPrice": "float64",
            "quantity": "float64",
            "discountAmount": "float64",
            "discountPercent": "float64",
            "netAmount": "float64",
            "amountExcludingTax": "float64",
            "taxAmount": "float64",
            "amountIncludingTax": "float64"
        },
        "vendors": {
            "balance": "float64",
            "lastModifiedDateTime": "datetime64[ns]"
        },
        "customers": {
            "balance": "float64",
            "lastModifiedDateTime": "datetime64[ns]"
        },
        # Add similar mappings for other datasets that have numeric fields
        "salesCreditMemo": {
            "discountAmount": "float64",
            "totalAmountExcludingTax": "float64",
            "totalTaxAmount": "float64",
            "totalAmountIncludingTax": "float64",
            "lastModifiedDateTime": "datetime64[ns]"
        },
        "salesCreditMemoLine": {
            "unitPrice": "float64",
            "quantity": "float64",
            "discountAmount": "float64",
            "discountPercent": "float64",
            "netAmount": "float64",
            "amountExcludingTax": "float64",
            "taxAmount": "float64",
            "amountIncludingTax": "float64"
        },
        "salesOrder": {
            "discountAmount": "float64",
            "totalAmountExcludingTax": "float64",
            "totalTaxAmount": "float64",
            "totalAmountIncludingTax": "float64",
            "lastModifiedDateTime": "datetime64[ns]"
        },
        "salesOrderLine": {
            "unitPrice": "float64",
            "quantity": "float64",
            "discountAmount": "float64",
            "discountPercent": "float64",
            "netAmount": "float64",
            "amountExcludingTax": "float64",
            "taxAmount": "float64",
            "amountIncludingTax": "float64"
        },
        "purchaseCreditMemo": {
            "discountAmount": "float64",
            "totalAmountExcludingTax": "float64",
            "totalTaxAmount": "float64",
            "totalAmountIncludingTax": "float64",
            "lastModifiedDateTime": "datetime64[ns]"
        },
        "purchaseCreditMemoLine": {
            "unitPrice": "float64",
            "quantity": "float64",
            "discountAmount": "float64",
            "discountPercent": "float64",
            "netAmount": "float64",
            "amountExcludingTax": "float64",
            "taxAmount": "float64",
            "amountIncludingTax": "float64"
        },
        "purchaseOrder": {
            "discountAmount": "float64",
            "totalAmountExcludingTax": "float64",
            "totalTaxAmount": "float64",
            "totalAmountIncludingTax": "float64",
            "lastModifiedDateTime": "datetime64[ns]"
        },
        "purchaseOrderLine": {
            "unitPrice": "float64",
            "quantity": "float64",
            "discountAmount": "float64",
            "discountPercent": "float64",
            "netAmount": "float64",
            "amountExcludingTax": "float64",
            "taxAmount": "float64",
            "amountIncludingTax": "float64"
        }
    }

def apply_data_types(df, dataset_name):
    """
    Aplica tipos de datos al DataFrame según el dataset
    
    Args:
        df (pandas.DataFrame): DataFrame a convertir
        dataset_name (str): Nombre del dataset
        
    Returns:
        pandas.DataFrame: DataFrame con tipos convertidos
    """
    # Obtener mapeo de tipos para este dataset
    type_mappings = get_field_types_mapping().get(dataset_name, {})
    
    # Si no hay mapeo para este dataset, devolver el DataFrame original
    if not type_mappings:
        return df
    
    # Crear un diccionario de conversiones solo para columnas que existen en el DataFrame
    conversions = {col: dtype for col, dtype in type_mappings.items() if col in df.columns}
    
    # Si no hay conversiones, devolver el DataFrame original
    if not conversions:
        return df
    
    # Aplicar conversiones de tipos
    for col, dtype in conversions.items():
        try:
            # Manejar conversiones de tipos especiales
            if dtype.startswith('datetime'):
                # Para campos de fecha, primero convertir a string para evitar errores
                #df[col] = pd.to_datetime(df[col].astype(str).replace('Z', ''), errors='coerce')
                df[col] = df[col].astype('string')
            elif dtype == 'float64':
                # Para campos numéricos, manejo especial de NaN y tipos mixtos
                #df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].astype(dtype)
            else:
                # Conversión general de tipos
                df[col] = df[col].astype(dtype)
        except Exception as e:
            print(f"Error al convertir columna {col} a {dtype}: {str(e)}")
    
    return df