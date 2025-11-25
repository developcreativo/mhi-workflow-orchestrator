"""
Registro central de schemas PyArrow
"""
from . import property_revenue_detail
from . import property_user
from . import company_summary
from . import company_relationships
from . import mergeAudit
from . import productGroup_productSubGroup
from . import property_room
from . import property_budget_monthly
from . import property_reservationRoomStay
from . import property_propertyManagement_guestLedger
from . import property_propertyManagement_depositLedger
from . import property_propertyManagement_accountReceivableAging
from . import property_invoice

# Registro de schemas disponibles
SCHEMAS = {
    "property.revenue.detail": property_revenue_detail.get_schema,
    "property.user": property_user.get_schema,
    "company.summary": company_summary.get_schema,
    "company.relationships.1": company_relationships.get_schema,
    "company.relationships.2": company_relationships.get_schema,
    "company.relationships.4": company_relationships.get_schema,
    "mergeAudit": mergeAudit.get_schema,
    "productGroup.productSubGroup": productGroup_productSubGroup.get_schema,
    "property.room": property_room.get_schema,
    "property.budget.monthly": property_budget_monthly.get_schema,
    "property.reservationRoomStay.creation": property_reservationRoomStay.get_schema,
    "property.reservationRoomStay.modification": property_reservationRoomStay.get_schema,
    "property.propertyManagement.guestLedger": property_propertyManagement_guestLedger.get_schema,
    "property.propertyManagement.depositLedger": property_propertyManagement_depositLedger.get_schema,
    "property.propertyManagement.accountReceivableAging": property_propertyManagement_accountReceivableAging.get_schema,
    "property.invoice":property_invoice.get_schema
}

def get_schema(dataset_name: str):
    """
    Obtiene el schema PyArrow para un dataset
    
    Args:
        dataset_name: Nombre del dataset (ej: 'property.revenue.detail')
    
    Returns:
        pa.Schema: El schema PyArrow
    
    Raises:
        KeyError: Si el dataset no tiene schema definido
    """
    if dataset_name not in SCHEMAS:
        raise KeyError(f"Schema no encontrado para dataset: {dataset_name}")
    
    return SCHEMAS[dataset_name]()