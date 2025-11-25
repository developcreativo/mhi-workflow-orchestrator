"""
Schema PyArrow para property.revenue.detail
Auto-generado desde datos reales de la API
Profundidad de anidamiento: 3 niveles
"""
import pyarrow as pa


def get_schema():
    """
    Retorna el schema PyArrow para property.revenue.detail
    """
    
    schema = pa.schema([
        pa.field("code", pa.string(), nullable=True),
        pa.field("name", pa.string(), nullable=True),
        pa.field("description", pa.string(), nullable=True),
        pa.field("nonProductive", pa.bool_(), nullable=True),
        pa.field("order", pa.int64(), nullable=True),
        pa.field("productSubGroupList",
            pa.list_(
                        pa.struct([
                                    pa.field("code", pa.string(), nullable=True),
                                    pa.field("description", pa.string(), nullable=True),
                                    pa.field("id", pa.int64(), nullable=True),
                                    pa.field("name", pa.string(), nullable=True),
                                    pa.field("order", pa.int64(), nullable=True),
                                    pa.field("productGroupId", pa.string(), nullable=True),
                                    pa.field("productList",
                                        pa.list_(
                                                        pa.struct([
                                                                        pa.field("chainProductId", pa.int64(), nullable=True),
                                                                        pa.field("code", pa.string(), nullable=True),
                                                                        pa.field("id", pa.int64(), nullable=True),
                                                                        pa.field("name", pa.string(), nullable=True),
                                                                        pa.field("nameI18n", pa.string(), nullable=True),
                                                                        pa.field("productType",
                                                                            pa.struct([
                                                                                                pa.field("code", pa.string(), nullable=True),
                                                                                                pa.field("id", pa.int64(), nullable=True),
                                                                                                pa.field("name", pa.string(), nullable=True),
                                                                                                pa.field("nameI18n", pa.string(), nullable=True),
                                                                                            ])
                                                                        , nullable=True),
                                                                    ])
                                                    )
                                    , nullable=True),
                                ])
                    )
        , nullable=True),
    ])
    
    metadata = {
        b"dataset_name": b"productGroup.productSubGroup",
        b"dataset_version": b"v1",
        b"dataset_description": b"Schema for productGroup.productSubGroup"
    }
    
    return schema.with_metadata(metadata)
