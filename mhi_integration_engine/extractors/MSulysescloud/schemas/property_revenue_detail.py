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
        pa.field("propertyId", pa.int64(), nullable=True),
        pa.field("date", pa.string(), nullable=True),
        pa.field("product",
            pa.struct([
                        pa.field("chainProductId", pa.int64(), nullable=True),
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("name", pa.string(), nullable=True)
                    ])
        , nullable=True),
        pa.field("quantity", pa.int64(), nullable=True),
        pa.field("adults", pa.int64(), nullable=True),
        pa.field("juniors", pa.int64(), nullable=True),
        pa.field("children", pa.int64(), nullable=True),
        pa.field("infants", pa.int64(), nullable=True),
        pa.field("amountAfterTax", pa.float64(), nullable=True),
        pa.field("amountBeforeTax", pa.float64(), nullable=True),
        pa.field("amountFoodBreakdownAfterTax", pa.float64(), nullable=True),
        pa.field("amountFoodBreakdownBeforeTax", pa.float64(), nullable=True),
        pa.field("amountBeverageBreakdownAfterTax", pa.float64(), nullable=True),
        pa.field("amountBeverageBreakdownBeforeTax", pa.float64(), nullable=True),
        pa.field("amountOtherBreakdownAfterTax", pa.float64(), nullable=True),
        pa.field("amountOtherBreakdownBeforeTax", pa.float64(), nullable=True),
        pa.field("amountNetAfterTax", pa.float64(), nullable=True),
        pa.field("amountNetBeforeTax", pa.float64(), nullable=True),
        pa.field("amountFoodBreakdownNetAfterTax", pa.float64(), nullable=True),
        pa.field("amountFoodBreakdownNetBeforeTax", pa.float64(), nullable=True),
        pa.field("amountBeverageBreakdownNetAfterTax", pa.float64(), nullable=True),
        pa.field("amountBeverageBreakdownNetBeforeTax", pa.float64(), nullable=True),
        pa.field("amountOtherBreakdownNetAfterTax", pa.float64(), nullable=True),
        pa.field("amountOtherBreakdownNetBeforeTax", pa.float64(), nullable=True),
        pa.field("amountDiscount", pa.float64(), nullable=True),
        pa.field("amountTax", pa.float64(), nullable=True),
        pa.field("amountCommission", pa.float64(), nullable=True),
        pa.field("amountCommissionBeforeTax", pa.float64(), nullable=True),
        pa.field("amountCommissionApplied", pa.float64(), nullable=True),
        pa.field("amountCommissionAppliedBeforeTax", pa.float64(), nullable=True),
        pa.field("amountCommissionFoodBreakdown", pa.float64(), nullable=True),
        pa.field("amountCommissionFoodBreakdownBeforeTax", pa.float64(), nullable=True),
        pa.field("amountCommissionFoodBreakdownApplied", pa.float64(), nullable=True),
        pa.field("amountCommissionFoodBreakdownAppliedBeforeTax", pa.float64(), nullable=True),
        pa.field("amountCommissionBeverageBreakdown", pa.float64(), nullable=True),
        pa.field("amountCommissionBeverageBreakdownBeforeTax", pa.float64(), nullable=True),
        pa.field("amountCommissionBeverageBreakdownApplied", pa.float64(), nullable=True),
        pa.field("amountCommissionBeverageBreakdownAppliedBeforeTax", pa.float64(), nullable=True),
        pa.field("amountCommissionOtherBreakdown", pa.float64(), nullable=True),
        pa.field("amountCommissionOtherBreakdownBeforeTax", pa.float64(), nullable=True),
        pa.field("amountCommissionOtherBreakdownApplied", pa.float64(), nullable=True),
        pa.field("amountCommissionOtherBreakdownAppliedBeforeTax", pa.float64(), nullable=True),
        pa.field("amountInvoiceAfterTax", pa.float64(), nullable=True),
        pa.field("amountInvoiceBeforeTax", pa.float64(), nullable=True),
        pa.field("segment",
            pa.struct([
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                    ])
        , nullable=True),
        pa.field("source",
            pa.struct([
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                    ])
        , nullable=True),
        pa.field("channel",
            pa.struct([
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                    ])
        , nullable=True),
        pa.field("rate",
            pa.struct([
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("name", pa.string(), nullable=True)
                        ])
        , nullable=True),
        pa.field("offer",
            pa.struct([
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("name", pa.string(), nullable=True)
                        ])
        , nullable=True),
        pa.field("promotion",
            pa.struct([
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("name", pa.string(), nullable=True)
                        ])
        , nullable=True),
        pa.field("booker",
            pa.struct([
                        pa.field("assignedUser",
                            pa.struct([
                                            pa.field("id", pa.int64(), nullable=True),
                                            pa.field("userName", pa.string(), nullable=True),
                                        ])
                        , nullable=True),
                        pa.field("chainChannel",
                            pa.struct([
                                            pa.field("chainChannelGroupId", pa.int64()),
                                            pa.field("code", pa.string(), nullable=True),
                                            pa.field("id", pa.int64(), nullable=True),
                                            pa.field("name", pa.string(), nullable=True),
                                            pa.field("order", pa.int64()),
                                        ])
                        , nullable=True),
                        pa.field("chainProfileGroup", pa.string()),
                        pa.field("chainSegment",
                            pa.struct([
                                            pa.field("chainSegmentGroupId", pa.int64()),
                                            pa.field("code", pa.string(), nullable=True),
                                            pa.field("id", pa.int64(), nullable=True),
                                            pa.field("name", pa.string(), nullable=True),
                                            pa.field("order", pa.int64()),
                                        ])
                        , nullable=True),
                        pa.field("chainSource",
                            pa.struct([
                                            pa.field("chainSourceGroupId", pa.int64()),
                                            pa.field("code", pa.string(), nullable=True),
                                            pa.field("id", pa.int64(), nullable=True),
                                            pa.field("name", pa.string(), nullable=True),
                                            pa.field("order", pa.int64()),
                                        ])
                        , nullable=True),
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("reservationPriceModelType",
                            pa.struct([
                                            pa.field("code", pa.string(), nullable=True),
                                            pa.field("id", pa.int64(), nullable=True),
                                            pa.field("name", pa.string(), nullable=True),
                                        ])
                        , nullable=True)
                    ])
        , nullable=True),
        pa.field("billTo", pa.string(), nullable=True),
        pa.field("central", pa.string(), nullable=True),
        pa.field("company", pa.string(), nullable=True),
        pa.field("countryCode", pa.string(), nullable=True),
    ])
    
    metadata = {
        b"dataset_name": b"property.revenue.detail",
        b"dataset_version": b"v1",
        b"dataset_description": b"Schema for property.revenue.detail"
    }
    
    return schema.with_metadata(metadata)