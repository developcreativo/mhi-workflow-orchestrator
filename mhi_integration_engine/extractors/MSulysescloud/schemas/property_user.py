"""
Schema PyArrow para property.revenue.detail
Auto-generado desde datos reales de la API
Profundidad de anidamiento: 2 niveles
"""
import pyarrow as pa


def get_schema():
    """
    Retorna el schema PyArrow para property.revenue.detail
    """
    
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=True),
        pa.field("code", pa.string(), nullable=True),
        pa.field("name", pa.string(), nullable=True),
        pa.field("remark", pa.string(), nullable=True),
        pa.field("colorCode", pa.string(), nullable=True),
        pa.field("active", pa.bool_(), nullable=True),
        pa.field("inactiveReason", pa.string(), nullable=True),
        pa.field("propertyClassTypeRating",
            pa.struct([
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("nameI18n", pa.string(), nullable=True),
                        pa.field("propertyClassType",
                            pa.struct([
                                            pa.field("code", pa.string(), nullable=True),
                                            pa.field("id", pa.int64(), nullable=True),
                                            pa.field("name", pa.string(), nullable=True),
                                            pa.field("nameI18n", pa.string(), nullable=True),
                                        ])
                        , nullable=True),
                    ])
        , nullable=True),
        pa.field("propertyAddress",
            pa.struct([
                        pa.field("city", pa.string(), nullable=True),
                        pa.field("country",
                            pa.struct([
                                            pa.field("code", pa.string(), nullable=True),
                                            pa.field("codeISOAlfa2", pa.string(), nullable=True),
                                            pa.field("id", pa.int64(), nullable=True),
                                            pa.field("name", pa.string(), nullable=True),
                                            pa.field("nameI18n", pa.string(), nullable=True),
                                        ])
                        , nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("municipalityName", pa.string(), nullable=True),
                        pa.field("postalCode", pa.string(), nullable=True),
                        pa.field("primary", pa.bool_(), nullable=True),
                        pa.field("remark", pa.string(), nullable=True),
                        pa.field("stateProvName", pa.string(), nullable=True),
                        pa.field("street", pa.string(), nullable=True),
                    ])
        , nullable=True),
        pa.field("propertyPhone",
            pa.struct([
                        pa.field("areaCityCode", pa.string(), nullable=True),
                        pa.field("countryAccessCode", pa.string(), nullable=True),
                        pa.field("extension", pa.string(), nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("phoneNumber", pa.string(), nullable=True),
                        pa.field("primary", pa.bool_(), nullable=True),
                        pa.field("remark", pa.string(), nullable=True),
                    ])
        , nullable=True),
        pa.field("propertyEmail",
            pa.struct([
                        pa.field("email", pa.string(), nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("primary", pa.bool_(), nullable=True),
                        pa.field("remark", pa.string(), nullable=True),
                    ])
        , nullable=True),
        pa.field("chainPropertyGroupList", pa.list_(pa.string()), nullable=True),
    ])
    
    metadata = {
        b"dataset_name": b"property.revenue.detail",
        b"dataset_version": b"v1",
        b"dataset_description": b"Schema for property.revenue.detail"
    }
    
    return schema.with_metadata(metadata)