"""
Schema PyArrow para property.budget.monthly
Auto-generado desde datos reales de la API
Profundidad de anidamiento: 3 niveles
"""
import pyarrow as pa


def get_schema():
    """
    Retorna el schema PyArrow para property.budget.monthly
    """
    
    schema = pa.schema([
        pa.field("name", pa.string(), nullable=True),
        pa.field("chainProductSubGroupBudgetList",
            pa.list_(
                        pa.struct([
                                    pa.field("name", pa.string(), nullable=True),
                                    pa.field("productBudgetList",
                                        pa.list_(
                                                        pa.struct([
                                                                        pa.field("product",
                                                                            pa.struct([
                                                                                                pa.field("chainProductId", pa.string(), nullable=True),
                                                                                                pa.field("code", pa.string(), nullable=True),
                                                                                                pa.field("id", pa.int64(), nullable=True),
                                                                                                pa.field("name", pa.string(), nullable=True),
                                                                                                pa.field("nameI18n", pa.string(), nullable=True),
                                                                                                pa.field("productType", pa.string(), nullable=True),
                                                                                            ])
                                                                        , nullable=True),
                                                                        pa.field("productBudgetDailyList", pa.list_(pa.string()), nullable=True),
                                                                        pa.field("productBudgetMonthlyList",
                                                                            pa.list_(
                                                                                                pa.struct([
                                                                                                                    pa.field("month", pa.int64(), nullable=True),
                                                                                                                    pa.field("pax", pa.int64(), nullable=True),
                                                                                                                    pa.field("revenue", pa.float64(), nullable=True),
                                                                                                                    pa.field("roomNights", pa.int64(), nullable=True),
                                                                                                                ])
                                                                                            )
                                                                        , nullable=True),
                                                                    ])
                                                    )
                                    , nullable=True),
                                ])
                    )
        , nullable=True),
    ])
    
    metadata = {
        b"dataset_name": b"property.budget.monthly",
        b"dataset_version": b"v1",
        b"dataset_description": b"Schema for property.budget.monthly"
    }
    
    return schema.with_metadata(metadata)