"""
Schema PyArrow para property.room
Auto-generado desde datos reales de la API
Profundidad de anidamiento: 2 niveles
"""
import pyarrow as pa


def get_schema():
    """
    Retorna el schema PyArrow para property.room
    """
    
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=True),
        pa.field("code", pa.string(), nullable=True),
        pa.field("name", pa.string(), nullable=True),
        pa.field("startDate", pa.string(), nullable=True),
        pa.field("endDate", pa.string(), nullable=True),
        pa.field("description", pa.string(), nullable=True),
        pa.field("descriptionI18n", pa.string(), nullable=True),
        pa.field("roomSpaceStatusType",
            pa.struct([
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("nameI18n", pa.string(), nullable=True),
                    ])
        , nullable=True),
        pa.field("reservationRoomStay", pa.string(), nullable=True),
        pa.field("roomSituationType",
            pa.struct([
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("nameI18n", pa.string(), nullable=True),
                    ])
        , nullable=True),
        pa.field("roomType",
            pa.struct([
                        pa.field("active", pa.bool_(), nullable=True),
                        pa.field("chainRoomType",
                            pa.struct([
                                            pa.field("chainRoomTypeGroupId", pa.int64(), nullable=True),
                                            pa.field("code", pa.string(), nullable=True),
                                            pa.field("description", pa.string(), nullable=True),
                                            pa.field("id", pa.int64(), nullable=True),
                                            pa.field("name", pa.string(), nullable=True),
                                            pa.field("order", pa.int64(), nullable=True),
                                            pa.field("sumAsPhysicalRoom", pa.bool_(), nullable=True),
                                        ])
                        , nullable=True),
                        pa.field("chainRoomTypeId", pa.int64(), nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("mainImage", pa.string(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("nameI18n", pa.string(), nullable=True),
                    ])
        , nullable=True),
    ])
    
    metadata = {
        b"dataset_name": b"property.room",
        b"dataset_version": b"v1",
        b"dataset_description": b"Schema for property.room"
    }
    
    return schema.with_metadata(metadata)
