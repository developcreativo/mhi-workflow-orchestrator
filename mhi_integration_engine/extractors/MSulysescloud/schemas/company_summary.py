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
        pa.field("chainId", pa.int64(), nullable=True),
        pa.field("profileType",
            pa.struct([
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("colorCode", pa.string(), nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("nameI18n", pa.string(), nullable=True),
                    ])
        , nullable=True),
        pa.field("chainProfileGroupId", pa.float64(), nullable=True),
        pa.field("chainProfileTypeId", pa.float64(), nullable=True),
        pa.field("code", pa.string(), nullable=True),
        pa.field("name", pa.string(), nullable=True),
        pa.field("extendedName", pa.string(), nullable=True),
        pa.field("iata", pa.string(), nullable=True),
        pa.field("taxId", pa.string(), nullable=True),
        pa.field("languageId", pa.float64(), nullable=True),
        pa.field("remark", pa.string(), nullable=True),
        pa.field("primaryAddressStreet", pa.string(), nullable=True),
        pa.field("primaryAddressCity", pa.string(), nullable=True),
        pa.field("primaryAddressStateProvName", pa.string(), nullable=True),
        pa.field("primaryAddressStateProvId", pa.float64(), nullable=True),
        pa.field("primaryAddressPostalCode", pa.string(), nullable=True),
        pa.field("primaryAddressCountryId", pa.float64(), nullable=True),
        pa.field("primaryPhoneNumber", pa.string(), nullable=True),
        pa.field("primaryEmail", pa.string(), nullable=True),
        pa.field("assignedUser",
            pa.struct([
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("userName", pa.string(), nullable=True),
                    ])
        , nullable=True),
        pa.field("active", pa.bool_(), nullable=True),
        pa.field("inactiveReason", pa.string(), nullable=True),
        pa.field("entityProtected", pa.bool_(), nullable=True),
    ])
    
    metadata = {
        b"dataset_name": b"property.revenue.detail",
        b"dataset_version": b"v1",
        b"dataset_description": b"Schema for property.revenue.detail"
    }
    
    return schema.with_metadata(metadata)
