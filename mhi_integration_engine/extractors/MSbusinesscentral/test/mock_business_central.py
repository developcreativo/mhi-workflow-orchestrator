"""
Simulación de API de Business Central para pruebas locales
"""

import json
import os
import uuid
from datetime import datetime, timedelta
from utils.mock_cloud import MockResponse

class MockBusinessCentral:
    """Simulador de API de Business Central para pruebas"""
    
    def __init__(self, data_dir="./mock_data"):
        """
        Inicializa el simulador
        
        Args:
            data_dir (str): Directorio donde se almacenan los datos simulados
        """
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        
        # Generar datos de prueba si no existen
        self._initialize_mock_data()
    
    def _initialize_mock_data(self):
        """Inicializa los datos de prueba"""
        # Estructura de datos
        datasets = {
            "companies": self._generate_companies,
            "customers": self._generate_customers,
            "vendors": self._generate_vendors,
            "salesInvoices": self._generate_sales_invoices,
            "purchaseInvoices": self._generate_purchase_invoices,
            "generalLedgerEntries": self._generate_gl_entries,
            "accounts": self._generate_accounts,
            "dimensions": self._generate_dimensions,
            "dimensionValues": self._generate_dimension_values
        }
        
        # Generar cada conjunto de datos
        for dataset, generator in datasets.items():
            file_path = os.path.join(self.data_dir, f"{dataset}.json")
            if not os.path.exists(file_path):
                data = generator()
                with open(file_path, 'w') as f:
                    json.dump(data, f, indent=2)
    
    def handle_request(self, url, headers=None, params=None, data=None, method="GET"):
        """
        Maneja solicitudes simuladas a la API
        
        Args:
            url (str): URL de la solicitud
            headers (dict, optional): Cabeceras
            params (dict, optional): Parámetros de consulta
            data (dict, optional): Datos de la solicitud
            method (str, optional): Método HTTP
            
        Returns:
            MockResponse: Respuesta simulada
        """
        # Autenticación
        if "oauth2/token" in url:
            return self._handle_auth(data)
        
        # Extraer endpoint de la URL
        # Formato: https://api.businesscentral.dynamics.com/v2.0/{tenant_id}/{environment}/{endpoint}
        parts = url.split("/")
        if len(parts) < 6:
            return MockResponse(400, {"error": "Invalid URL format"})
        
        endpoint = "/".join(parts[6:])
        
        # Manejar diferentes endpoints
        if endpoint.startswith("companies") and endpoint.count("/") == 0:
            return self._handle_companies(params)
        elif "customers" in endpoint:
            return self._handle_customers(endpoint, params)
        elif "vendors" in endpoint:
            return self._handle_vendors(endpoint, params)
        elif "salesInvoices" in endpoint:
            return self._handle_sales_invoices(endpoint, params)
        elif "purchaseInvoices" in endpoint:
            return self._handle_purchase_invoices(endpoint, params)
        elif "generalLedgerEntries" in endpoint:
            return self._handle_gl_entries(endpoint, params)
        elif "accounts" in endpoint:
            return self._handle_accounts(endpoint, params)
        elif "dimensions" in endpoint and "dimensionValues" not in endpoint:
            return self._handle_dimensions(endpoint, params)
        elif "dimensionValues" in endpoint:
            return self._handle_dimension_values(endpoint, params)
        else:
            return MockResponse(404, {"error": f"Endpoint not found: {endpoint}"})
    
    def _handle_auth(self, data):
        """
        Maneja solicitudes de autenticación
        
        Args:
            data (dict): Datos de la solicitud
            
        Returns:
            MockResponse: Respuesta con token
        """
        # Simular respuesta de token
        return MockResponse(200, {
            "token_type": "Bearer",
            "expires_in": 3600,
            "ext_expires_in": 3600,
            "access_token": "mock_access_token_for_test_purposes_only"
        })
    
    def _handle_companies(self, params):
        """
        Maneja solicitudes de companies
        
        Args:
            params (dict): Parámetros de consulta
            
        Returns:
            MockResponse: Respuesta con datos de companies
        """
        companies = self._load_data("companies")
        
        # Aplicar paginación si se especifica
        page_size = int(params.get("$top", 100)) if params else 100
        
        # Formatear respuesta en formato OData
        return MockResponse(200, {
            "@odata.context": "https://api.businesscentral.dynamics.com/v2.0/$metadata#companies",
            "value": companies[:page_size],
            "@odata.count": len(companies),
        })
    
    def _handle_customers(self, endpoint, params):
        """
        Maneja solicitudes de customers
        
        Args:
            endpoint (str): Endpoint de la solicitud
            params (dict): Parámetros de consulta
            
        Returns:
            MockResponse: Respuesta con datos de customers
        """
        customers = self._load_data("customers")
        
        # Filtrar por compañía si se especifica
        company_id = self._extract_company_id(endpoint)
        if company_id:
            customers = [c for c in customers if c.get("companyId") == company_id]
        
        # Aplicar filtro incremental si se especifica
        if params and "$filter" in params:
            customers = self._apply_filter(customers, params["$filter"])
        
        # Aplicar paginación si se especifica
        page_size = int(params.get("$top", 100)) if params else 100
        
        # Formatear respuesta en formato OData
        return MockResponse(200, {
            "@odata.context": "https://api.businesscentral.dynamics.com/v2.0/$metadata#customers",
            "value": customers[:page_size],
            "@odata.count": len(customers),
        })
    
    def _handle_vendors(self, endpoint, params):
        """Maneja solicitudes de vendors"""
        vendors = self._load_data("vendors")
        
        # Filtrar por compañía
        company_id = self._extract_company_id(endpoint)
        if company_id:
            vendors = [v for v in vendors if v.get("companyId") == company_id]
        
        # Aplicar filtro incremental
        if params and "$filter" in params:
            vendors = self._apply_filter(vendors, params["$filter"])
        
        # Aplicar paginación
        page_size = int(params.get("$top", 100)) if params else 100
        
        return MockResponse(200, {
            "@odata.context": "https://api.businesscentral.dynamics.com/v2.0/$metadata#vendors",
            "value": vendors[:page_size],
            "@odata.count": len(vendors),
        })
    
    def _handle_sales_invoices(self, endpoint, params):
        """Maneja solicitudes de salesInvoices"""
        invoices = self._load_data("salesInvoices")
        
        # Filtrar por compañía
        company_id = self._extract_company_id(endpoint)
        if company_id:
            invoices = [i for i in invoices if i.get("companyId") == company_id]
        
        # Aplicar filtro incremental
        if params and "$filter" in params:
            invoices = self._apply_filter(invoices, params["$filter"])
        
        # Aplicar paginación
        page_size = int(params.get("$top", 100)) if params else 100
        
        return MockResponse(200, {
            "@odata.context": "https://api.businesscentral.dynamics.com/v2.0/$metadata#salesInvoices",
            "value": invoices[:page_size],
            "@odata.count": len(invoices),
        })
    
    def _handle_purchase_invoices(self, endpoint, params):
        """Maneja solicitudes de purchaseInvoices"""
        invoices = self._load_data("purchaseInvoices")
        
        # Filtrar por compañía
        company_id = self._extract_company_id(endpoint)
        if company_id:
            invoices = [i for i in invoices if i.get("companyId") == company_id]
        
        # Aplicar filtro incremental
        if params and "$filter" in params:
            invoices = self._apply_filter(invoices, params["$filter"])
        
        # Aplicar paginación
        page_size = int(params.get("$top", 100)) if params else 100
        
        return MockResponse(200, {
            "@odata.context": "https://api.businesscentral.dynamics.com/v2.0/$metadata#purchaseInvoices",
            "value": invoices[:page_size],
            "@odata.count": len(invoices),
        })
    
    def _handle_gl_entries(self, endpoint, params):
        """Maneja solicitudes de generalLedgerEntries"""
        entries = self._load_data("generalLedgerEntries")
        
        # Filtrar por compañía
        company_id = self._extract_company_id(endpoint)
        if company_id:
            entries = [e for e in entries if e.get("companyId") == company_id]
        
        # Aplicar filtro incremental
        if params and "$filter" in params:
            entries = self._apply_filter(entries, params["$filter"])
        
        # Aplicar paginación
        page_size = int(params.get("$top", 100)) if params else 100
        
        return MockResponse(200, {
            "@odata.context": "https://api.businesscentral.dynamics.com/v2.0/$metadata#generalLedgerEntries",
            "value": entries[:page_size],
            "@odata.count": len(entries),
        })
    
    def _handle_accounts(self, endpoint, params):
        """Maneja solicitudes de accounts"""
        accounts = self._load_data("accounts")
        
        # Filtrar por compañía
        company_id = self._extract_company_id(endpoint)
        if company_id:
            accounts = [a for a in accounts if a.get("companyId") == company_id]
        
        # Aplicar paginación
        page_size = int(params.get("$top", 100)) if params else 100
        
        return MockResponse(200, {
            "@odata.context": "https://api.businesscentral.dynamics.com/v2.0/$metadata#accounts",
            "value": accounts[:page_size],
            "@odata.count": len(accounts),
        })
    
    def _handle_dimensions(self, endpoint, params):
        """Maneja solicitudes de dimensions"""
        dimensions = self._load_data("dimensions")
        
        # Filtrar por compañía
        company_id = self._extract_company_id(endpoint)
        if company_id:
            dimensions = [d for d in dimensions if d.get("companyId") == company_id]
        
        # Aplicar paginación
        page_size = int(params.get("$top", 100)) if params else 100
        
        return MockResponse(200, {
            "@odata.context": "https://api.businesscentral.dynamics.com/v2.0/$metadata#dimensions",
            "value": dimensions[:page_size],
            "@odata.count": len(dimensions),
        })
    
    def _handle_dimension_values(self, endpoint, params):
        """Maneja solicitudes de dimensionValues"""
        dimension_values = self._load_data("dimensionValues")
        
        # Extraer company_id y dimension_id
        company_id = self._extract_company_id(endpoint)
        dimension_id = self._extract_dimension_id(endpoint)
        
        # Filtrar por compañía y dimensión
        if company_id:
            dimension_values = [dv for dv in dimension_values if dv.get("companyId") == company_id]
        if dimension_id:
            dimension_values = [dv for dv in dimension_values if dv.get("dimensionId") == dimension_id]
        
        # Aplicar filtro incremental
        if params and "$filter" in params:
            dimension_values = self._apply_filter(dimension_values, params["$filter"])
        
        # Aplicar paginación
        page_size = int(params.get("$top", 100)) if params else 100
        
        return MockResponse(200, {
            "@odata.context": "https://api.businesscentral.dynamics.com/v2.0/$metadata#dimensionValues",
            "value": dimension_values[:page_size],
            "@odata.count": len(dimension_values),
        })
    
    def _load_data(self, dataset):
        """Carga datos desde archivo JSON"""
        file_path = os.path.join(self.data_dir, f"{dataset}.json")
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                return json.load(f)
        return []
    
    def _extract_company_id(self, endpoint):
        """Extrae el ID de compañía del endpoint"""
        parts = endpoint.split("/")
        for i, part in enumerate(parts):
            if part == "companies" and i + 1 < len(parts):
                # El siguiente elemento debería ser el ID
                next_part = parts[i + 1]
                # Verificar que no sea otro recurso
                if "/" not in next_part and not any(resource in next_part for resource in 
                                               ["customers", "vendors", "salesInvoices", "dimensions"]):
                    return next_part
        return None
    
    def _extract_dimension_id(self, endpoint):
        """Extrae el ID de dimensión del endpoint"""
        parts = endpoint.split("/")
        for i, part in enumerate(parts):
            if part == "dimensions" and i + 1 < len(parts):
                # El siguiente elemento debería ser el ID
                next_part = parts[i + 1]
                # Verificar que no sea otro recurso
                if "/" not in next_part and "dimensionValues" not in next_part:
                    return next_part
        return None
    
    def _apply_filter(self, data, filter_string):
        """
        Aplica filtros OData simples
        Soporta: field gt value, field lt value
        """
        if not filter_string:
            return data
        
        # Analizar expresión de filtro
        parts = filter_string.split(" ")
        if len(parts) != 3:
            return data
        
        field, operator, value = parts
        
        # Implementar operadores comunes
        if operator == "gt":
            return [item for item in data if item.get(field, "") > value]
        elif operator == "lt":
            return [item for item in data if item.get(field, "") < value]
        elif operator == "eq":
            return [item for item in data if item.get(field, "") == value]
        
        return data
    
    def _generate_companies(self):
        """Genera datos de prueba para companies"""
        return [
            {
                "id": "company1",
                "name": "Contoso Ltd",
                "displayName": "Contoso Ltd.",
                "businessProfileId": "prof1",
                "systemVersion": "19.0.12345.0",
                "taxRegistrationNumber": "TAX12345",
                "currencyCode": "EUR",
                "phoneNumber": "+34 91 123 4567",
                "email": "info@contoso.com",
                "website": "https://www.contoso.com"
            },
            {
                "id": "company2",
                "name": "Fabrikam Inc",
                "displayName": "Fabrikam Incorporated",
                "businessProfileId": "prof2",
                "systemVersion": "19.0.12345.0",
                "taxRegistrationNumber": "TAX67890",
                "currencyCode": "USD",
                "phoneNumber": "+1 425 555 0100",
                "email": "info@fabrikam.com",
                "website": "https://www.fabrikam.com"
            }
        ]
    
    def _generate_customers(self):
        """Genera datos de prueba para customers"""
        customers = []
        
        for company_id in ["company1", "company2"]:
            for i in range(5):
                timestamp = (datetime.now() - timedelta(days=i)).isoformat()
                customers.append({
                    "id": f"cust{company_id}{i}",
                    "companyId": company_id,
                    "number": f"C{10000 + i}",
                    "displayName": f"Customer {i+1} for {company_id}",
                    "type": "Company",
                    "addressLine1": f"Calle Cliente {i+1}",
                    "city": "Madrid",
                    "country": "ES",
                    "phoneNumber": f"+34 91 123 {i+1000}",
                    "email": f"customer{i+1}@example.com",
                    "taxRegistrationNumber": f"TAX{company_id}{i}",
                    "currencyCode": "EUR" if company_id == "company1" else "USD",
                    "lastModifiedDateTime": timestamp
                })
        
        return customers
    
    def _generate_vendors(self):
        """Genera datos de prueba para vendors"""
        vendors = []
        
        for company_id in ["company1", "company2"]:
            for i in range(5):
                timestamp = (datetime.now() - timedelta(days=i)).isoformat()
                vendors.append({
                    "id": f"vendor{company_id}{i}",
                    "companyId": company_id,
                    "number": f"V{10000 + i}",
                    "displayName": f"Vendor {i+1} for {company_id}",
                    "addressLine1": f"Calle Proveedor {i+1}",
                    "city": "Barcelona",
                    "country": "ES",
                    "phoneNumber": f"+34 93 123 {i+1000}",
                    "email": f"vendor{i+1}@example.com",
                    "taxRegistrationNumber": f"VTAX{company_id}{i}",
                    "currencyCode": "EUR",
                    "lastModifiedDateTime": timestamp
                })
        
        return vendors
    
    def _generate_sales_invoices(self):
        """Genera datos de prueba para salesInvoices"""
        invoices = []
        
        for company_id in ["company1", "company2"]:
            for i in range(10):
                timestamp = (datetime.now() - timedelta(days=i)).isoformat()
                invoice_date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
                due_date = (datetime.now() - timedelta(days=i) + timedelta(days=30)).strftime("%Y-%m-%d")
                
                invoices.append({
                    "id": f"inv{company_id}{i}",
                    "companyId": company_id,
                    "number": f"INV{i+10000}",
                    "invoiceDate": invoice_date,
                    "dueDate": due_date,
                    "customerNumber": f"C{10000 + (i % 5)}",
                    "customerName": f"Customer {(i % 5)+1} for {company_id}",
                    "status": "Open" if i % 3 != 0 else "Paid",
                    "currencyCode": "EUR" if company_id == "company1" else "USD",
                    "discountAmount": 0,
                    "discountAppliedBeforeTax": True,
                    "totalAmountExcludingTax": 1000 * (i + 1),
                    "totalTaxAmount": 210 * (i + 1),
                    "totalAmountIncludingTax": 1210 * (i + 1),
                    "lastModifiedDateTime": timestamp
                })
        
        return invoices
    
    def _generate_purchase_invoices(self):
        """Genera datos de prueba para purchaseInvoices"""
        invoices = []
        
        for company_id in ["company1", "company2"]:
            for i in range(10):
                timestamp = (datetime.now() - timedelta(days=i)).isoformat()
                invoice_date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
                due_date = (datetime.now() - timedelta(days=i) + timedelta(days=30)).strftime("%Y-%m-%d")
                
                invoices.append({
                    "id": f"pinv{company_id}{i}",
                    "companyId": company_id,
                    "number": f"PINV{i+10000}",
                    "invoiceDate": invoice_date,
                    "dueDate": due_date,
                    "vendorNumber": f"V{10000 + (i % 5)}",
                    "vendorName": f"Vendor {(i % 5)+1} for {company_id}",
                    "status": "Open" if i % 3 != 0 else "Paid",
                    "currencyCode": "EUR",
                    "discountAmount": 0,
                    "discountAppliedBeforeTax": True,
                    "totalAmountExcludingTax": 500 * (i + 1),
                    "totalTaxAmount": 105 * (i + 1),
                    "totalAmountIncludingTax": 605 * (i + 1),
                    "lastModifiedDateTime": timestamp
                })
        
        return invoices
    
    def _generate_gl_entries(self):
        """Genera datos de prueba para generalLedgerEntries"""
        entries = []
        
        for company_id in ["company1", "company2"]:
            for i in range(20):
                timestamp = (datetime.now() - timedelta(days=i//2)).isoformat()
                posting_date = (datetime.now() - timedelta(days=i//2)).strftime("%Y-%m-%d")
                
                # Alternando débito y crédito
                is_debit = i % 2 == 0
                amount = 1000.0 * (i + 1) * (1 if is_debit else -1)
                
                entries.append({
                    "id": f"gl{company_id}{i}",
                    "companyId": company_id,
                    "entryNumber": i + 1000,
                    "postingDate": posting_date,
                    "documentNumber": f"DOC{i+1000}",
                    "documentType": "Invoice" if is_debit else "Payment",
                    "accountId": f"ac{i % 10 + 1}",
                    "accountNumber": f"{1000 + (i % 10)}",
                    "description": f"{'Purchase' if is_debit else 'Payment'} - Transaction {i+1}",
                    "debitAmount": amount if is_debit else 0,
                    "creditAmount": 0 if is_debit else -amount,
                    "lastModifiedDateTime": timestamp
                })
        
        return entries
    
    def _generate_accounts(self):
        """Genera datos de prueba para accounts"""
        accounts = []
        
        # Categorías principales
        categories = [
            {"range": (1000, 1999), "name": "Assets"},
            {"range": (2000, 2999), "name": "Liabilities"},
            {"range": (3000, 3999), "name": "Equity"},
            {"range": (4000, 4999), "name": "Revenue"},
            {"range": (5000, 5999), "name": "Cost of Goods Sold"},
            {"range": (6000, 6999), "name": "Expenses"}
        ]
        
        for company_id in ["company1", "company2"]:
            account_number = 1000
            
            for category in categories:
                start, end = category["range"]
                
                # Generar algunas cuentas en cada categoría
                for i in range(3):
                    account_id = f"ac{account_number}"
                    account_number_str = str(account_number)
                    
                    accounts.append({
                        "id": account_id,
                        "companyId": company_id,
                        "number": account_number_str,
                        "name": f"{category['name']} {i+1}",
                        "category": category["name"],
                        "accountType": self._get_account_type(account_number),
                        "blocked": False
                    })
                    
                    account_number += 100
        
        return accounts
    
    def _get_account_type(self, account_number):
        """Determina el tipo de cuenta basado en el número"""
        if account_number < 2000:
            return "Assets"
        elif account_number < 3000:
            return "Liabilities"
        elif account_number < 4000:
            return "Equity"
        elif account_number < 5000:
            return "Income"
        elif account_number < 6000:
            return "CostOfGoodsSold"
        else:
            return "Expense"
    
    def _generate_dimensions(self):
        """Genera datos de prueba para dimensions"""
        dimensions = []
        
        dimension_types = [
            {"code": "DEPT", "name": "Department"},
            {"code": "PROJ", "name": "Project"},
            {"code": "CUST", "name": "Customer"},
            {"code": "VEND", "name": "Vendor"}
        ]
        
        for company_id in ["company1", "company2"]:
            for i, dim_type in enumerate(dimension_types):
                dimension_id = f"dim{company_id}{i}"
                
                dimensions.append({
                    "id": dimension_id,
                    "companyId": company_id,
                    "code": dim_type["code"],
                    "displayName": dim_type["name"],
                    "description": f"{dim_type['name']} Dimension",
                    "blocked": False
                })
        
        return dimensions
    
    def _generate_dimension_values(self):
        """Genera datos de prueba para dimensionValues"""
        values = []
        
        department_values = ["Sales", "Marketing", "Finance", "IT", "HR"]
        project_values = ["Project A", "Project B", "Project C"]
        customer_values = ["Customer Segment 1", "Customer Segment 2", "Customer Segment 3"]
        vendor_values = ["Vendor Type 1", "Vendor Type 2"]
        
        all_values = [department_values, project_values, customer_values, vendor_values]
        
        for company_id in ["company1", "company2"]:
            for dim_index in range(4):  # Para cada dimensión
                dimension_id = f"dim{company_id}{dim_index}"
                dim_values = all_values[dim_index]
                
                for i, value_name in enumerate(dim_values):
                    timestamp = (datetime.now() - timedelta(days=i)).isoformat()
                    
                    values.append({
                        "id": f"dimval{company_id}{dim_index}{i}",
                        "companyId": company_id,
                        "dimensionId": dimension_id,
                        "code": f"{value_name.replace(' ', '')[:3]}{i+1}",
                        "displayName": value_name,
                        "description": f"{value_name} - Dimension Value",
                        "blocked": False,
                        "lastModifiedDateTime": timestamp
                    })
        
        return values

def mock_businesscentral_api_response(url, headers=None, params=None, data=None, **kwargs):
    """
    Función para mockear requests.get y requests.post para Business Central
    
    Args:
        url (str): URL de la solicitud
        headers (dict, optional): Cabeceras
        params (dict, optional): Parámetros de consulta
        data (dict, optional): Datos de la solicitud
        **kwargs: Argumentos adicionales
        
    Returns:
        MockResponse: Respuesta simulada
    """
    mock_api = MockBusinessCentral()
    method = kwargs.get("method", "GET")
    
    return mock_api.handle_request(url, headers, params, data, method)