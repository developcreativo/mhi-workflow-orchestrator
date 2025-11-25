"""
Schema PyArrow para property.propertyManagement.guestLedger
Auto-generado desde datos reales de la API
Basado en análisis de 47 registros
Profundidad de anidamiento: 0 niveles
"""
import pyarrow as pa


def get_schema():
    """
    Retorna el schema PyArrow para property.propertyManagement.guestLedger
    """
    
    schema = pa.schema([
        pa.field("propertyCode", pa.string(), nullable=True),
        pa.field("folioId", pa.int64(), nullable=True),
        pa.field("roomId", pa.float64(), nullable=True),
        pa.field("folioName", pa.string(), nullable=True),
        pa.field("roomCode", pa.string(), nullable=True),
        pa.field("reservationRoomStaySummaryId", pa.int64(), nullable=True),
        pa.field("arrival", pa.string(), nullable=True),
        pa.field("departure", pa.string(), nullable=True),
        pa.field("adult", pa.float64(), nullable=True),
        pa.field("bookerName", pa.string(), nullable=True),
        pa.field("totalAccommodationAmountAfterTax", pa.float64(), nullable=True),
        pa.field("totalBoardAmountAfterTax", pa.float64(), nullable=True),
        pa.field("totalServiceAmountAfterTax", pa.float64(), nullable=True),
        pa.field("totalFeeAmountAfterTax", pa.float64(), nullable=True),
        pa.field("totalAmountAfterTax", pa.float64(), nullable=True),
        pa.field("paidAmountAfterTax", pa.float64(), nullable=True),
        pa.field("balanceAmountAfterTax", pa.float64(), nullable=True),
        pa.field("creditLimitExceeded", pa.bool_(), nullable=True),
    ])
    
    metadata = {
        b"dataset_name": b"property.propertyManagement.guestLedger",
        b"dataset_version": b"v1",
        b"dataset_description": b"Schema for property.propertyManagement.guestLedger"
    }
    
    return schema.with_metadata(metadata)