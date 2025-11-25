"""
Schema PyArrow para property.reservationRoomStay
Auto-generado desde datos reales de la API
Basado en análisis de 222 registros
Profundidad de anidamiento: 4 niveles
CORREGIDO: Campos con tipos erróneos
"""
import pyarrow as pa

def get_schema():
    """
    Retorna el schema PyArrow para property.reservationRoomStay
    """
    
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=True),
        pa.field("reservationStatusType",
            pa.struct([
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("nameI18n", pa.string(), nullable=True),  # AGREGADO
                        pa.field("colorCode", pa.string(), nullable=True),  # AGREGADO
                    ])
        , nullable=True),
        pa.field("reservationSituationType", 
                 pa.struct([
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("nameI18n", pa.string(), nullable=True),  # AGREGADO
                    ])
                    , nullable=True),
        pa.field("arrival", pa.string(), nullable=True),
        pa.field("departure", pa.string(), nullable=True),
        pa.field("arrivalTime", pa.string(), nullable=True),
        pa.field("departureTime", pa.string(), nullable=True),
        pa.field("quantity", pa.int64(), nullable=True),
        pa.field("accommodation",
            pa.struct([
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("roomType",
                            pa.struct([
                                            pa.field("id", pa.int64(), nullable=True),
                                            pa.field("chainRoomTypeId", pa.int64(), nullable=True),
                                            pa.field("name", pa.string(), nullable=True),
                                            pa.field("nameI18n", pa.string(), nullable=True),
                                            pa.field("mainImage", pa.string(), nullable=True),
                                            pa.field("active", pa.bool_(), nullable=True),
                                            pa.field("chainRoomType",
                                                pa.struct([
                                                                    pa.field("id", pa.int64(), nullable=True),
                                                                    pa.field("code", pa.string(), nullable=True),
                                                                    pa.field("name", pa.string(), nullable=True),
                                                                    pa.field("description", pa.string(), nullable=True),
                                                                    pa.field("chainRoomTypeGroupId", pa.int64(), nullable=True),
                                                                    pa.field("order", pa.int64(), nullable=True),
                                                                    pa.field("sumAsPhysicalRoom", pa.bool_(), nullable=True),
                                                                ])
                                            , nullable=True),
                                        ])
                        , nullable=True),
                    ])
        , nullable=True),
        pa.field("accommodationUpgrade",
            pa.struct([
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("roomType",
                            pa.struct([
                                            pa.field("id", pa.int64(), nullable=True),
                                            pa.field("chainRoomTypeId", pa.int64(), nullable=True),
                                            pa.field("name", pa.string(), nullable=True),
                                            pa.field("nameI18n", pa.string(), nullable=True),
                                            pa.field("mainImage", pa.string(), nullable=True),
                                            pa.field("active", pa.bool_(), nullable=True),
                                            pa.field("chainRoomType",
                                                pa.struct([
                                                                    pa.field("id", pa.int64(), nullable=True),
                                                                    pa.field("code", pa.string(), nullable=True),
                                                                    pa.field("name", pa.string(), nullable=True),
                                                                    pa.field("description", pa.string(), nullable=True),
                                                                    pa.field("chainRoomTypeGroupId", pa.int64(), nullable=True),
                                                                    pa.field("order", pa.int64(), nullable=True),
                                                                    pa.field("sumAsPhysicalRoom", pa.bool_(), nullable=True),
                                                                ])
                                            , nullable=True),
                                        ])
                        , nullable=True),
                    ])
        , nullable=True),
        pa.field("room",
            pa.struct([
                pa.field("id", pa.int64(), nullable=True),
                pa.field("code", pa.string(), nullable=True),
                pa.field("name", pa.string(), nullable=True)
            ]),
            nullable=True
            ),
        pa.field("adult", pa.int64(), nullable=True),
        pa.field("junior", pa.int64(), nullable=True),
        pa.field("child", pa.int64(), nullable=True),
        pa.field("infant", pa.int64(), nullable=True),
        pa.field("accommodationAmount", pa.float64(), nullable=True),
        pa.field("boardAmount", pa.float64(), nullable=True),
        pa.field("serviceAmount", pa.float64(), nullable=True),
        pa.field("totalAmount", pa.float64(), nullable=True),
        pa.field("productFee", pa.struct([
                                    pa.field("id", pa.int64(), nullable=True),
                                    pa.field("name", pa.string(), nullable=True)]

        ), nullable=True),
        pa.field("pmsLocator", pa.string(), nullable=True),
        pa.field("crsLocator", pa.string(), nullable=True),
        pa.field("otaLocator", pa.string(), nullable=True),
        pa.field("cmLocator", pa.string(), nullable=True),
        pa.field("remark", pa.string(), nullable=True),
        pa.field("mainBoard",
            pa.struct([
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("chainBoardId", pa.int64(), nullable=True),
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("nameI18n", pa.string(), nullable=True),
                        pa.field("serviceList",
                            pa.list_(
                                            pa.struct([
                                                            pa.field("id", pa.int64(), nullable=True),
                                                            pa.field("code", pa.string(), nullable=True),
                                                            pa.field("name", pa.string(), nullable=True),
                                                        ])
                                        )
                        , nullable=True),
                    ])
        , nullable=True),
        pa.field("reservationRoomStayDaily",
            pa.list_(
                        pa.struct([
                                    pa.field("id", pa.int64(), nullable=True),
                                    pa.field("date", pa.string(), nullable=True),
                                    pa.field("room",
                                        pa.struct([
                                                        pa.field("id", pa.int64(), nullable=True),
                                                        pa.field("code", pa.string(), nullable=True),
                                                        pa.field("name", pa.string(), nullable=True),
                                                        pa.field("roomType",
                                                            pa.struct([
                                                                                pa.field("id", pa.int64(), nullable=True),
                                                                                pa.field("name", pa.string(), nullable=True),
                                                                            ])
                                                        , nullable=True),
                                                         ])
                                    , nullable=True),
                                    pa.field("allotment", pa.string(), nullable=True),
                                    pa.field("rate",
                                        pa.struct([
                                                        pa.field("id", pa.int64(), nullable=True),
                                                        pa.field("code", pa.string(), nullable=True),
                                                        pa.field("name", pa.string(), nullable=True),
                                                        pa.field("rateType",
                                                            pa.struct([
                                                                                pa.field("id", pa.int64(), nullable=True),
                                                                                pa.field("code", pa.string(), nullable=True),
                                                                                pa.field("name", pa.string(), nullable=True),
                                                                            ])
                                                        , nullable=True),

                                                    ])
                                    , nullable=True),
                                    pa.field("amountBeforeTax", pa.float64(), nullable=True),
                                    pa.field("amountAfterTax", pa.float64(), nullable=True),
                                    pa.field("amountNetAfterTax", pa.float64(), nullable=True),
                                    pa.field("amountNetBeforeTax", pa.float64(), nullable=True),
                                    pa.field("amountInvoiceAfterTax", pa.float64(), nullable=True),
                                    pa.field("amountInvoiceBeforeTax", pa.float64(), nullable=True),
                                    pa.field("accommodationAmount", pa.float64(), nullable=True),
                                    pa.field("accommodationAmountAfterDiscount", pa.float64(), nullable=True),
                                    pa.field("accommodationAmountAfterTax", pa.float64(), nullable=True),
                                    pa.field("accommodationAmountBeforeTax", pa.float64(), nullable=True),
                                    pa.field("accommodationAmountNetAfterTax", pa.float64(), nullable=True),
                                    pa.field("accommodationAmountNetBeforeTax", pa.float64(), nullable=True),
                                    pa.field("accommodationAmountInvoiceAfterTax", pa.float64(), nullable=True),
                                    pa.field("accommodationAmountInvoiceBeforeTax", pa.float64(), nullable=True),
                                    pa.field("centralAccommodationPercentageCommission", pa.float64(), nullable=True),
                                    pa.field("centralAccommodationAmountCommission", pa.float64(), nullable=True),
                                    pa.field("centralBoardPercentageCommission", pa.float64(), nullable=True),
                                    pa.field("centralBoardAmountCommission", pa.float64(), nullable=True),
                                    pa.field("companyAccommodationPercentageCommission", pa.float64(), nullable=True),
                                    pa.field("companyAccommodationAmountCommission", pa.float64(), nullable=True),
                                    pa.field("companyBoardPercentageCommission", pa.float64(), nullable=True),
                                    pa.field("companyBoardAmountCommission", pa.float64(), nullable=True),
                                    pa.field("bookerAccommodationPercentageCommission", pa.float64(), nullable=True),
                                    pa.field("bookerAccommodationAmountCommission", pa.float64(), nullable=True),
                                    pa.field("bookerBoardPercentageCommission", pa.float64(), nullable=True),
                                    pa.field("bookerBoardAmountCommission", pa.float64(), nullable=True),
                                    pa.field("billToAccommodationPercentageCommission", pa.float64(), nullable=True),
                                    pa.field("billToAccommodationAmountCommission", pa.float64(), nullable=True),
                                    pa.field("billToBoardPercentageCommission", pa.float64(), nullable=True),
                                    pa.field("billToBoardAmountCommission", pa.float64(), nullable=True),
                                    pa.field("billToAccommodationPercentageDiscount", pa.float64(), nullable=True),
                                    pa.field("billToAccommodationAmountDiscount", pa.float64(), nullable=True),
                                    pa.field("billToBoardPercentageDiscount", pa.float64(), nullable=True),
                                    pa.field("billToBoardAmountDiscount", pa.float64(), nullable=True),
                                    pa.field("currency",
                                        pa.struct([
                                                        pa.field("id", pa.int64(), nullable=True),
                                                        pa.field("code", pa.string(), nullable=True),
                                                        pa.field("name", pa.string(), nullable=True),
                                                        pa.field("nameI18n", pa.string(), nullable=True),
                                                        pa.field("symbol", pa.string(), nullable=True),
                                                        pa.field("decimalPlaces", pa.int64(), nullable=True),
                                                    ])
                                    , nullable=True),
                                    pa.field("offer",
                                        pa.struct([
                                                        pa.field("id", pa.int64(), nullable=True),
                                                        pa.field("code", pa.string(), nullable=True),
                                                        pa.field("name", pa.string(), nullable=True),
                                                        ])
                                    , nullable=True),
                                    pa.field("offerAmountDiscount", pa.float64(), nullable=True),
                                    pa.field("offerPercentDiscount", pa.float64(), nullable=True),
                                    pa.field("promotion",
                                        pa.struct([
                                                        pa.field("id", pa.int64(), nullable=True),
                                                        pa.field("code", pa.string(), nullable=True),
                                                        pa.field("name", pa.string(), nullable=True),
                                                        ])
                                    , nullable=True),
                                    pa.field("promotionAmountDiscount", pa.float64(), nullable=True),
                                    pa.field("promotionPercentDiscount", pa.float64(), nullable=True),
                                    pa.field("board",
                                        pa.struct([
                                                        pa.field("id", pa.int64(), nullable=True),
                                                        pa.field("chainBoardId", pa.int64(), nullable=True),
                                                        pa.field("code", pa.string(), nullable=True),
                                                        pa.field("name", pa.string(), nullable=True),
                                                        pa.field("nameI18n", pa.string(), nullable=True),
                                                        pa.field("serviceList",
                                                            pa.list_(
                                                                pa.struct([
                                                                    pa.field("id", pa.int64(), nullable=True),
                                                                    pa.field("code", pa.string(), nullable=True),
                                                                    pa.field("name", pa.string(), nullable=True),
                                                                ])
                                                            )
                                                        , nullable=True),
                                                    ])
                                    , nullable=True),
                                    pa.field("boardAmountAfterDiscountAdult", pa.float64(), nullable=True),
                                    pa.field("boardAmountAdult", pa.float64(), nullable=True),
                                    pa.field("boardAmountAfterTaxAdult", pa.float64(), nullable=True),
                                    pa.field("boardAmountBeforeTaxAdult", pa.float64(), nullable=True),
                                    pa.field("boardAmountNetAfterTaxAdult", pa.float64(), nullable=True),
                                    pa.field("boardAmountNetBeforeTaxAdult", pa.float64(), nullable=True),
                                    pa.field("boardAmountInvoiceAfterTaxAdult", pa.float64(), nullable=True),
                                    pa.field("boardAmountInvoiceBeforeTaxAdult", pa.float64(), nullable=True),
                                    pa.field("boardAmountAfterDiscountJunior", pa.float64(), nullable=True),
                                    pa.field("boardAmountJunior", pa.float64(), nullable=True),
                                    pa.field("boardAmountAfterTaxJunior", pa.float64(), nullable=True),
                                    pa.field("boardAmountBeforeTaxJunior", pa.float64(), nullable=True),
                                    pa.field("boardAmountNetAfterTaxJunior", pa.float64(), nullable=True),
                                    pa.field("boardAmountNetBeforeTaxJunior", pa.float64(), nullable=True),
                                    pa.field("boardAmountInvoiceAfterTaxJunior", pa.float64(), nullable=True),
                                    pa.field("boardAmountInvoiceBeforeTaxJunior", pa.float64(), nullable=True),
                                    pa.field("boardAmountAfterDiscountChild", pa.float64(), nullable=True),
                                    pa.field("boardAmountChild", pa.float64(), nullable=True),
                                    pa.field("boardAmountAfterTaxChild", pa.float64(), nullable=True),
                                    pa.field("boardAmountBeforeTaxChild", pa.float64(), nullable=True),
                                    pa.field("boardAmountNetAfterTaxChild", pa.float64(), nullable=True),
                                    pa.field("boardAmountNetBeforeTaxChild", pa.float64(), nullable=True),
                                    pa.field("boardAmountInvoiceAfterTaxChild", pa.float64(), nullable=True),
                                    pa.field("boardAmountInvoiceBeforeTaxChild", pa.float64(), nullable=True),
                                    pa.field("boardAmountAfterDiscountInfant", pa.float64(), nullable=True),
                                    pa.field("boardAmountInfant", pa.float64(), nullable=True),
                                    pa.field("boardAmountAfterTaxInfant", pa.float64(), nullable=True),
                                    pa.field("boardAmountBeforeTaxInfant", pa.float64(), nullable=True),
                                    pa.field("boardAmountNetAfterTaxInfant", pa.float64(), nullable=True),
                                    pa.field("boardAmountNetBeforeTaxInfant", pa.float64(), nullable=True),
                                    pa.field("boardAmountInvoiceAfterTaxInfant", pa.float64(), nullable=True),
                                    pa.field("boardAmountInvoiceBeforeTaxInfant", pa.float64(), nullable=True),
                                    pa.field("paymentType",
                                        pa.struct([
                                                        pa.field("id", pa.int64(), nullable=True),
                                                        pa.field("code", pa.string(), nullable=True),
                                                        pa.field("name", pa.string(), nullable=True),
                                                        pa.field("nameI18n", pa.string(), nullable=True),
                                                    ])
                                    , nullable=True),
                                    pa.field("propertyPolicy",
                                        pa.struct([
                                                        pa.field("id", pa.int64(), nullable=True),
                                                        pa.field("code", pa.string(), nullable=True),
                                                        pa.field("name", pa.string(), nullable=True),
                                                     ])
                                    , nullable=True),
                                    pa.field("propertySource",
                                        pa.struct([
                                                        pa.field("id", pa.int64(), nullable=True),
                                                        pa.field("code", pa.string(), nullable=True),
                                                        pa.field("name", pa.string(), nullable=True),
                                                    ])
                                    , nullable=True),
                                    pa.field("propertySegment",
                                        pa.struct([
                                                        pa.field("id", pa.int64(), nullable=True),
                                                        pa.field("code", pa.string(), nullable=True),
                                                        pa.field("name", pa.string(), nullable=True),
                                                    ])
                                    , nullable=True),
                                    pa.field("propertyChannel",
                                        pa.struct([
                                                        pa.field("id", pa.int64(), nullable=True),
                                                        pa.field("code", pa.string(), nullable=True),
                                                        pa.field("name", pa.string(), nullable=True),
                                                    ])
                                    , nullable=True),
                                    pa.field("booker",
                                        pa.struct([
                                                        pa.field("id", pa.int64(), nullable=True),
                                                        pa.field("code", pa.string(), nullable=True),
                                                        pa.field("name", pa.string(), nullable=True),
                                                    ])
                                    , nullable=True),
                                    pa.field("billTo",
                                        pa.struct([
                                                        pa.field("id", pa.int64(), nullable=True),
                                                        pa.field("code", pa.string(), nullable=True),
                                                        pa.field("name", pa.string(), nullable=True),
                                        ])
                                    , nullable=True),
                                    pa.field("central",
                                        pa.struct([
                                                    pa.field("id", pa.int64(), nullable=True),
                                                    pa.field("code", pa.string(), nullable=True),
                                                    pa.field("name", pa.string(), nullable=True),
                                                    ])
                                    , nullable=True),
                                    pa.field("company",
                                        pa.struct([
                                                    pa.field("id", pa.int64(), nullable=True),
                                                    pa.field("code", pa.string(), nullable=True),
                                                    pa.field("name", pa.string(), nullable=True),
                                                    ])
                                    , nullable=True),
                                    pa.field("reservationGroup",
                                        pa.struct([
                                            pa.field("id", pa.int64(), nullable=True),
                                            pa.field("code", pa.string(), nullable=True),
                                            pa.field("name", pa.string(), nullable=True),
                                        ])
                                    , nullable=True),
                                    pa.field("modifiedDate", pa.string(), nullable=True),
                                ])
                    )
        , nullable=True),
        pa.field("reservationRoomStayServiceList",
            pa.list_(
                        pa.struct([
                                    pa.field("quantity", pa.int64(), nullable=True),
                                    pa.field("endDate", pa.string(), nullable=True),
                                    pa.field("startDate", pa.string(), nullable=True),
                                    pa.field("totalAmountAfterTax", pa.float64(), nullable=True),
                                    pa.field("totalAmountBeforeTax", pa.float64(), nullable=True),
                                    pa.field("productChargeQuantityType",
                                        pa.struct([
                                                        pa.field("id", pa.int64(), nullable=True),
                                                        pa.field("code", pa.string(), nullable=True),
                                                        pa.field("name", pa.string(), nullable=True),
                                                        pa.field("nameI18n", pa.string(), nullable=True),
                                                    ])
                                    , nullable=True),
                                    pa.field("reservationRoomStay", pa.string(), nullable=True),
                                    pa.field("reservationRoomStayServiceDailyList",
                                        pa.list_(
                                                        pa.struct([
                                                                        pa.field("id", pa.int64(), nullable=True),
                                                                        pa.field("date", pa.string(), nullable=True),
                                                                        pa.field("postingDate", pa.string(), nullable=True),
                                                                        pa.field("amount", pa.float64(), nullable=True),
                                                                        pa.field("amountAfterDiscount", pa.float64(), nullable=True),
                                                                        pa.field("amountAfterTax", pa.float64(), nullable=True),
                                                                        pa.field("amountBeforeTax", pa.float64(), nullable=True),
                                                                        pa.field("amountInvoiceAfterTax", pa.float64(), nullable=True),
                                                                        pa.field("amountInvoiceBeforeTax", pa.float64(), nullable=True),
                                                                        pa.field("amountNetAfterTax", pa.float64(), nullable=True),
                                                                        pa.field("amountNetBeforeTax", pa.float64(), nullable=True),
                                                                        pa.field("billToAmountCommission", pa.float64(), nullable=True),
                                                                        pa.field("billToAmountDiscount", pa.float64(), nullable=True),
                                                                        pa.field("billToPercentageCommission", pa.float64(), nullable=True),
                                                                        pa.field("billToPercentageDiscount", pa.float64(), nullable=True),
                                                                        pa.field("bookerAmountCommission", pa.float64(), nullable=True),
                                                                        pa.field("bookerPercentageCommission", pa.float64(), nullable=True),
                                                                        pa.field("centralAmountCommission", pa.float64(), nullable=True),
                                                                        pa.field("centralPercentageCommission", pa.float64(), nullable=True),
                                                                        pa.field("companyAmountCommission", pa.float64(), nullable=True),
                                                                        pa.field("companyPercentageCommission", pa.float64(), nullable=True),
                                                                        pa.field("currency",
                                                                            pa.struct([
                                                                                                pa.field("code", pa.string(), nullable=True),
                                                                                                ])
                                                                        , nullable=True),
                                                                        pa.field("offer",
                                                                            pa.struct([
                                                                                pa.field("id", pa.int64(), nullable=True),
                                                                                pa.field("code", pa.string(), nullable=True),
                                                                                pa.field("name", pa.string(), nullable=True),
                                                                            ])
                                                                        , nullable=True),
                                                                        pa.field("offerAmountDiscount", pa.float64(), nullable=True),
                                                                        pa.field("offerPercentDiscount", pa.float64(), nullable=True),
                                                                        pa.field("promotion",
                                                                            pa.struct([
                                                                                pa.field("id", pa.int64(), nullable=True),
                                                                                pa.field("code", pa.string(), nullable=True),
                                                                                pa.field("name", pa.string(), nullable=True),
                                                                            ])
                                                                        , nullable=True),
                                                                        pa.field("promotionAmountDiscount", pa.float64(), nullable=True),
                                                                        pa.field("promotionPercentDiscount", pa.float64(), nullable=True),
                                                                        pa.field("rate",     
                                                                            pa.struct([
                                                                                pa.field("id", pa.int64(), nullable=True),
                                                                                pa.field("code", pa.string(), nullable=True),
                                                                                pa.field("name", pa.string(), nullable=True),
                                                                            ])
                                                                        , nullable=True),
                                                                    ])
                                                    )
                                    , nullable=True),
                                    pa.field("service",
                                        pa.struct([
                                                        pa.field("id", pa.int64(), nullable=True),
                                                        pa.field("code", pa.string(), nullable=True),
                                                        pa.field("name", pa.string(), nullable=True),
                                                    ])
                                    , nullable=True),
                                ])
                    )
        , nullable=True),
        pa.field("createdDate", pa.string(), nullable=True),
        pa.field("modifiedDate", pa.string(), nullable=True),
        pa.field("cancelledDate", pa.string(), nullable=True),
        # CORREGIDOS: Cambiados de pa.string() a structs nullable
        pa.field("mainBooker", 
            pa.struct([
                pa.field("id", pa.int64(), nullable=True),
                pa.field("code", pa.string(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
            ])
        , nullable=True),
        pa.field("mainBillTo", 
            pa.struct([
                pa.field("id", pa.int64(), nullable=True),
                pa.field("code", pa.string(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
            ])
        , nullable=True),
        pa.field("mainCentral", 
            pa.struct([
                pa.field("id", pa.int64(), nullable=True),
                pa.field("code", pa.string(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
            ])
        , nullable=True),
        pa.field("mainCompany", 
            pa.struct([
                pa.field("id", pa.int64(), nullable=True),
                pa.field("code", pa.string(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
            ])
        , nullable=True),
        pa.field("mainRate", 
            pa.struct([
                pa.field("id", pa.int64(), nullable=True),
                pa.field("code", pa.string(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
            ])
        , nullable=True),
        pa.field("mainOffer", 
            pa.struct([
                pa.field("id", pa.int64(), nullable=True),
                pa.field("code", pa.string(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
            ])
        , nullable=True),
        pa.field("mainPromotion", 
            pa.struct([
                pa.field("id", pa.int64(), nullable=True),
                pa.field("code", pa.string(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
            ])
        , nullable=True),
        pa.field("deposit", 
            pa.struct([
                pa.field("id", pa.int64(), nullable=True),
                pa.field("amount", pa.float64(), nullable=True),
                pa.field("depositType", pa.string(), nullable=True),
            ])
        , nullable=True),
        pa.field("reservationType",
            pa.struct([
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("nameI18n", pa.string(), nullable=True),
                        ])
        , nullable=True),
        pa.field("reservationRoomStayCustomFieldValueSet", pa.string(), nullable=True),
        pa.field("property",
            pa.struct([
                        pa.field("id", pa.int64(), nullable=True),
                        pa.field("code", pa.string(), nullable=True),
                        pa.field("name", pa.string(), nullable=True),
                        pa.field("remark", pa.string(), nullable=True),
                        pa.field("colorCode", pa.string(), nullable=True),
                        pa.field("active", pa.bool_(), nullable=True),
                        pa.field("inactiveReason", pa.string(), nullable=True),
                        pa.field("propertyClassTypeRating",
                            pa.struct([
                                pa.field("id", pa.int64(), nullable=True),
                                pa.field("code", pa.string(), nullable=True),
                                pa.field("name", pa.string(), nullable=True),
                                pa.field("nameI18n", pa.string(), nullable=True),
                                pa.field("propertyClassType",
                                    pa.struct([
                                        pa.field("id", pa.int64(), nullable=True),
                                        pa.field("code", pa.string(), nullable=True),
                                        pa.field("name", pa.string(), nullable=True),
                                        pa.field("nameI18n", pa.string(), nullable=True),
                                    ])
                                , nullable=True),
                            ])
                        , nullable=True),
                        pa.field("propertyAddress",
                            pa.struct([
                                pa.field("id", pa.int64(), nullable=True),
                                pa.field("primary", pa.bool_(), nullable=True),
                                pa.field("street", pa.string(), nullable=True),
                                pa.field("city", pa.string(), nullable=True),
                                pa.field("stateProvName", pa.string(), nullable=True),
                                pa.field("municipalityName", pa.string(), nullable=True),
                                pa.field("postalCode", pa.string(), nullable=True),
                                pa.field("remark", pa.string(), nullable=True),
                                pa.field("country",
                                    pa.struct([
                                        pa.field("id", pa.int64(), nullable=True),
                                        pa.field("code", pa.string(), nullable=True),
                                        pa.field("codeISOAlfa2", pa.string(), nullable=True),
                                        pa.field("name", pa.string(), nullable=True),
                                        pa.field("nameI18n", pa.string(), nullable=True),
                                    ])
                                , nullable=True),
                            ])
                        , nullable=True),
                        pa.field("propertyPhone",
                            pa.struct([
                                pa.field("id", pa.int64(), nullable=True),
                                pa.field("primary", pa.bool_(), nullable=True),
                                pa.field("countryAccessCode", pa.string(), nullable=True),
                                pa.field("areaCityCode", pa.string(), nullable=True),
                                pa.field("phoneNumber", pa.string(), nullable=True),
                                pa.field("extension", pa.string(), nullable=True),
                                pa.field("remark", pa.string(), nullable=True),
                            ])
                        , nullable=True),
                        pa.field("propertyEmail",
                            pa.struct([
                                pa.field("id", pa.int64(), nullable=True),
                                pa.field("primary", pa.bool_(), nullable=True),
                                pa.field("email", pa.string(), nullable=True),
                                pa.field("remark", pa.string(), nullable=True),
                            ])
                        , nullable=True),
                        pa.field("chainPropertyGroupList", pa.list_(pa.string()), nullable=True),
                    ])
        , nullable=True),
    ])
    
    metadata = {
        b"dataset_name": b"property.reservationRoomStay",
        b"dataset_version": b"v1",
        b"dataset_description": b"Schema for property.reservationRoomStay - CORRECTED"
    }
    
    return schema.with_metadata(metadata)