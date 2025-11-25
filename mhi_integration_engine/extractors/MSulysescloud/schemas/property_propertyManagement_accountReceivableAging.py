"""
Schema PyArrow para property.propertyManagement.accountReceivableAging
Auto-generado desde datos reales de la API
Basado en análisis de 44 registros
Profundidad de anidamiento: 1 niveles
"""
import pyarrow as pa


def get_schema():
    """
    Retorna el schema PyArrow para property.propertyManagement.accountReceivableAging
    """
    
    schema = pa.schema([
        pa.field("accountReceivableCONModel",
            pa.struct([
                        pa.field("accountReceivableAddressList", pa.string(), nullable=True),
                        pa.field("accountReceivableEmailList", pa.string(), nullable=True),
                        pa.field("accountReceivablePhoneList", pa.string(), nullable=True),
                        pa.field("accountReceivableStatementSendType", pa.string(), nullable=True),
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("emailStatementSend", pa.string(), nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("limitAmount", pa.string(), nullable=True),
                        pa.field("limitDueDay", pa.string(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("remark", pa.string(), nullable=True),
                    ])
        , nullable=True),
        pa.field("accountReceivableLimitDueDay", pa.float64(), nullable=True),
        pa.field("accountReceivableLimitAmount", pa.float64(), nullable=True),
        pa.field("days0to30Total", pa.float64(), nullable=True),
        pa.field("days30to60Total", pa.float64(), nullable=True),
        pa.field("days60to90Total", pa.float64(), nullable=True),
        pa.field("days90to120Total", pa.float64(), nullable=True),
        pa.field("days120to365Total", pa.float64(), nullable=True),
        pa.field("daysGreaterThan365Total", pa.float64(), nullable=True),
        pa.field("total", pa.float64(), nullable=True),
    ])
    
    metadata = {
        b"dataset_name": b"property.propertyManagement.accountReceivableAging",
        b"dataset_version": b"v1",
        b"dataset_description": b"Schema for property.propertyManagement.accountReceivableAging"
    }
    
    return schema.with_metadata(metadata)