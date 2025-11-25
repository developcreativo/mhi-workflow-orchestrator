"""
Schema PyArrow para property.revenue.detail
Auto-generado desde datos reales de la API
Profundidad de anidamiento: 1 niveles
"""
import pyarrow as pa


def get_schema():
    """
    Retorna el schema PyArrow para property.revenue.detail
    """
    
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=True),
        pa.field("propertyId", pa.string(), nullable=True),
        pa.field("targetId", pa.int64(), nullable=True),
        pa.field("sourceId", pa.int64(), nullable=True),
        pa.field("sourceCode", pa.string(), nullable=True),
        pa.field("sourceName", pa.string(), nullable=True),
        pa.field("mergeDate", pa.string(), nullable=True),
        pa.field("previousMergeAudit",
            pa.list_(
                        pa.struct([
                                    pa.field("id", pa.int64(), nullable=True),
                                    pa.field("mergeDate", pa.string(), nullable=True),
                                    pa.field("previousMergeAudit", pa.list_(pa.string()), nullable=True),
                                    pa.field("propertyId", pa.string(), nullable=True),
                                    pa.field("sourceCode", pa.string(), nullable=True),
                                    pa.field("sourceId", pa.int64(), nullable=True),
                                    pa.field("sourceName", pa.string(), nullable=True),
                                    pa.field("targetId", pa.int64(), nullable=True),
                                ])
                    )
        , nullable=True),
    ])
    
    metadata = {
        b"dataset_name": b"property.revenue.detail",
        b"dataset_version": b"v1",
        b"dataset_description": b"Schema for property.revenue.detail"
    }
    
    return schema.with_metadata(metadata)