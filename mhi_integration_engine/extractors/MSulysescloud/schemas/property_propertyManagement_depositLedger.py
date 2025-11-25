"""
Schema PyArrow para property.propertyManagement.depositLedger
Auto-generado desde datos reales de la API
Basado en análisis de 32 registros
Profundidad de anidamiento: 0 niveles
"""
import pyarrow as pa


def get_schema():
    """
    Retorna el schema PyArrow para property.propertyManagement.depositLedger
    """
    
    schema = pa.schema([
        pa.field("propertyCode", pa.string(), nullable=True),
        pa.field("name", pa.string(), nullable=True),
        pa.field("ownerType", pa.string(), nullable=True),
        pa.field("ownerName", pa.string(), nullable=True),
        pa.field("consumptionDate", pa.string(), nullable=True),
        pa.field("invoiceName", pa.string(), nullable=True),
        pa.field("invoiceDate", pa.string(), nullable=True),
        pa.field("invoiceTypeName", pa.string(), nullable=True),
        pa.field("invoiceSerie", pa.string(), nullable=True),
        pa.field("invoiceNumber", pa.string(), nullable=True),
        pa.field("bookerName", pa.string(), nullable=True),
        pa.field("arrival", pa.string(), nullable=True),
        pa.field("departure", pa.string(), nullable=True),
        pa.field("amount", pa.float64(), nullable=True),
        pa.field("refund", pa.float64(), nullable=True),
        pa.field("spent", pa.float64(), nullable=True),
        pa.field("remaining", pa.float64(), nullable=True),
    ])
    
    metadata = {
        b"dataset_name": b"property.propertyManagement.depositLedger",
        b"dataset_version": b"v1",
        b"dataset_description": b"Schema for property.propertyManagement.depositLedger"
    }
    
    return schema.with_metadata(metadata)