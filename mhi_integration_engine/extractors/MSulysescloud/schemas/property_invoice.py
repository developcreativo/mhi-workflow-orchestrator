"""
Schema PyArrow para property.invoice
Generado desde 200 registros con datos reales
Incluye todas las estructuras completas basadas en la API y datos observados
Profundidad: 5 niveles
"""
import pyarrow as pa


def get_schema():
    """
    Retorna el schema PyArrow para property.invoice
    """
    
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=True),
        pa.field("sequence", pa.int64(), nullable=True),
        pa.field("invoiceSerie", pa.string(), nullable=True),
        pa.field("invoiceNumber", pa.int64(), nullable=True),
        pa.field("invoiceDate", pa.string(), nullable=True),
        pa.field("invoiceDueDate", pa.string(), nullable=True),
        
        # company
        pa.field("company", pa.struct([
            pa.field("code", pa.string(), nullable=True),
            pa.field("id", pa.int64(), nullable=True),
            pa.field("name", pa.string(), nullable=True)
        ]), nullable=True),
        
        # customer
        pa.field("customer", pa.struct([
            pa.field("id", pa.int64(), nullable=True),
            pa.field("name", pa.string(), nullable=True),
            pa.field("profileType", pa.struct([
                pa.field("code", pa.string(), nullable=True),
                pa.field("colorCode", pa.string(), nullable=True),
                pa.field("id", pa.int64(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
                pa.field("nameI18n", pa.string(), nullable=True),
            ]), nullable=True),
        ]), nullable=True),
        
        # travelAgent - STRUCT completo
        pa.field("travelAgent", pa.struct([
            pa.field("code", pa.string(), nullable=True),
            pa.field("iataId", pa.string(), nullable=True),
            pa.field("id", pa.int64(), nullable=True),
            pa.field("name", pa.string(), nullable=True)
        ]), nullable=True),
        
        pa.field("holderName", pa.string(), nullable=True),
        pa.field("taxId", pa.string(), nullable=True),
        pa.field("address", pa.string(), nullable=True),
        pa.field("city", pa.string(), nullable=True),
        pa.field("stateProvName", pa.string(), nullable=True),
        pa.field("stateProvCode", pa.string(), nullable=True),
        pa.field("municipalityName", pa.string(), nullable=True),
        pa.field("municipalityCode", pa.string(), nullable=True),
        pa.field("postalCode", pa.string(), nullable=True),
        pa.field("countryCode", pa.string(), nullable=True),
        pa.field("invoiceType", pa.struct([
            pa.field("code", pa.string(), nullable=True),
            pa.field("description", pa.string(), nullable=True),
            pa.field("id", pa.int64(), nullable=True)
        ]), nullable=True),
        
        pa.field("billingType", pa.struct([
            pa.field("code", pa.string(), nullable=True),
            pa.field("id", pa.int64(), nullable=True),
            pa.field("name", pa.string(), nullable=True),
            pa.field("nameI18n", pa.string(), nullable=True),
        ]), nullable=True),
        
        pa.field("totalAmountAfterTax", pa.float64(), nullable=True),
        pa.field("totalAmountBeforeTax", pa.float64(), nullable=True),
        pa.field("totalAmountTax", pa.float64(), nullable=True),
        pa.field("totalAmountTaxExempt", pa.float64(), nullable=True),
        pa.field("totalAmountDiscount", pa.float64(), nullable=True),
        pa.field("totalAmountDiscountBeforeTax", pa.float64(), nullable=True),
        pa.field("totalAmountCommission", pa.float64(), nullable=True),
        pa.field("amountCommissionBeforeTax", pa.float64(), nullable=True),
        pa.field("amountCommissionAfterTax", pa.float64(), nullable=True),
        pa.field("amountCommissionAppliedBeforeTax", pa.float64(), nullable=True),
        pa.field("amountCommissionAppliedAfterTax", pa.float64(), nullable=True),
        
        # productList - STRUCT completo
 # productList - STRUCT completo
pa.field("productList", 
    pa.list_(pa.struct([
        pa.field("amountAfterTax", pa.float64(), nullable=True),
        pa.field("amountBeforeTax", pa.float64(), nullable=True),
        pa.field("amountCommission", pa.float64(), nullable=True),
        pa.field("amountCommissionApplied", pa.float64(), nullable=True),
        pa.field("amountCommissionAppliedBeforeTax", pa.float64(), nullable=True),
        pa.field("amountCommissionBeforeTax", pa.float64(), nullable=True),
        pa.field("amountDiscount", pa.float64(), nullable=True),
        pa.field("amountDiscountBeforeTax", pa.float64(), nullable=True),
        pa.field("amountTax", pa.float64(), nullable=True),
        pa.field("channel", pa.struct([
            pa.field("code", pa.string(), nullable=True),
            pa.field("id", pa.int64(), nullable=True),
            pa.field("name", pa.string(), nullable=True),
        ]), nullable=True),
        pa.field("currency", pa.struct([
            pa.field("code", pa.string(), nullable=True)
        ]), nullable=True),
        pa.field("depositId", pa.int64(), nullable=True),
        pa.field("id", pa.int64(), nullable=True),
        
        # invoiceProductCommissionList
        pa.field("invoiceProductCommissionList", pa.list_(pa.struct([
            pa.field("id", pa.int64(), nullable=True),
            pa.field("amount", pa.float64(), nullable=True),
            pa.field("amountOfCommission", pa.float64(), nullable=True),
            pa.field("applied", pa.bool_(), nullable=True),
            pa.field("commissionAmountAfterTax", pa.float64(), nullable=True),
            pa.field("commissionAmountBeforeTax", pa.float64(), nullable=True),
            pa.field("percentageOfCommission", pa.float64(), nullable=True),
        ])), nullable=True),
        
        # invoiceProductDiscountList - STRUCT completo
        pa.field("invoiceProductDiscountList", pa.list_(pa.struct([
            pa.field("id", pa.int64(), nullable=True),
            pa.field("discount", pa.struct([
                pa.field("id", pa.int64(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
            ]), nullable=True),
            pa.field("name", pa.string(), nullable=True),
            pa.field("amount", pa.float64(), nullable=True),
            pa.field("percentageOfDiscount", pa.float64(), nullable=True),
            pa.field("amountOfDiscount", pa.float64(), nullable=True),
        ])), nullable=True),
        
        # invoiceProductTaxList - STRUCT completo
        pa.field("invoiceProductTaxList", pa.list_(pa.struct([
            pa.field("id", pa.int64(), nullable=True),
            pa.field("tax", pa.struct([
                pa.field("id", pa.int64(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
            ]), nullable=True),
            pa.field("name", pa.string(), nullable=True),
            pa.field("amount", pa.float64(), nullable=True),
            pa.field("percentageOfTax", pa.float64(), nullable=True),
            pa.field("amountOfTax", pa.float64(), nullable=True),
        ])), nullable=True),
        
        pa.field("paymentReceipt", pa.string(), nullable=True),
        pa.field("postingDate", pa.string(), nullable=True),
        pa.field("product", pa.struct([
            pa.field("code", pa.string(), nullable=True),
            pa.field("id", pa.int64(), nullable=True),
            pa.field("name", pa.string(), nullable=True),
        ]), nullable=True),
        pa.field("productionDate", pa.string(), nullable=True),
        pa.field("quantity", pa.int64(), nullable=True),
        pa.field("segment", pa.struct([
            pa.field("code", pa.string(), nullable=True),
            pa.field("id", pa.int64(), nullable=True),
            pa.field("name", pa.string(), nullable=True),
        ]), nullable=True),
        pa.field("source", pa.struct([
            pa.field("code", pa.string(), nullable=True),
            pa.field("id", pa.int64(), nullable=True),
            pa.field("name", pa.string(), nullable=True),
        ]), nullable=True)
    ])), nullable=True), 
        
        # invoiceTaxSummaryList - STRUCT completo
        pa.field("invoiceTaxSummaryList", pa.list_(pa.struct([
            pa.field("id", pa.int64(), nullable=True),
            pa.field("tax", pa.struct([
                pa.field("id", pa.int64(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
            ]), nullable=True),
            pa.field("name", pa.string(), nullable=True),
            pa.field("amount", pa.float64(), nullable=True),
            pa.field("percentageOfTax", pa.float64(), nullable=True),
            pa.field("amountOfTax", pa.float64(), nullable=True),
        ])), nullable=True),
        
        pa.field("reservationSummary", pa.string(), nullable=True),
        
        # reservationRoomStaySummary - STRUCT completo
        pa.field("reservationRoomStaySummary", pa.struct([
            pa.field("id", pa.int64(), nullable=True),
            pa.field("pmsLocator", pa.string(), nullable=True)
            ]), nullable=True),
        
        pa.field("reservationGroup", pa.string(), nullable=True),
        pa.field("deposit", pa.string(), nullable=True),
        pa.field("minimumTicketNumber", pa.string(), nullable=True),
        pa.field("maximumTicketNumber", pa.string(), nullable=True),
        pa.field("invoiceDescription", pa.string(), nullable=True),
    ])
    
    metadata = {
        b"dataset_name": b"property.invoice",
        b"dataset_version": b"v2",
        b"dataset_description": b"Schema completo para property.invoice con todas las estructuras expandidas"
    }
    
    return schema.with_metadata(metadata)