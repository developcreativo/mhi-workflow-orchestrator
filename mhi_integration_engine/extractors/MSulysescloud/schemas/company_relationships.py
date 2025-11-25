"""
Schema PyArrow para property.revenue.detail
Auto-generado desde datos reales de la API
Profundidad de anidamiento: 0 niveles
"""
import pyarrow as pa


def get_schema():
    """
    Retorna el schema PyArrow para property.revenue.detail
    """
    
    schema = pa.schema([
        pa.field("profileId", pa.int64(), nullable=True),
        pa.field("profileSubsidiaryId", pa.int64(), nullable=True),
    ])
    
    metadata = {
        b"dataset_name": b"property.revenue.detail",
        b"dataset_version": b"v1",
        b"dataset_description": b"Schema for property.revenue.detail"
    }
    
    return schema.with_metadata(metadata)