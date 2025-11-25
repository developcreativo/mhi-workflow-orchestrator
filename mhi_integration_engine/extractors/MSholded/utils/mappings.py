import pandas as pd
import json

def MapDataSet(dataset, data_json:json) -> pd.DataFrame:
    try:
        match dataset:
            case 'invoicing.documents.invoice' | 'invoicing.documents.creditnote' | 'invoicing.documents.purchase' |'invoicing.documents.purchaserefund':
                data_df = pd.DataFrame(__mappingInvoicingDocuments(data_json))
            case 'accounting.dailyledger':
                data_df = pd.DataFrame(__mappingAccountingDailyLedger(data_json))
            case 'invoicing.contacts':
                data_df = pd.DataFrame(__mappingInvoicingContacts(data_json))
            case 'invoicing.products':
                data_df = pd.DataFrame(__mappingInvoicingProducts(data_json))
            case 'team.employees':
                data_df = pd.DataFrame(__mappingTeamEmployees(data_json))
            # En otro caso 
            case _:
                data_df = pd.DataFrame(data_json)
        return data_df
    except Exception as e:
        msg=f'El mapping del dataset {dataset} ha fallado, error:{type(e)}: {e}.'
        print(msg)
        raise(msg)

# Aqui es donde esta la logica de datos
def __mappingAccountingDailyLedger(data_json):      
    item_list = []
    for item in list(data_json):
        i = {}
        i['entryNumber']            = str(item['entryNumber'])
        i['line']                   = str(item['line'])
        i['timestamp']              = str(item['timestamp'])
        i['type']                   = str(item['type'])
        i['description']            = str(item['description'])
        i['docDescription']         = str(item['docDescription'])
        i['account']                = float(item['account'])
        i['debit']                  = str(item['debit'])
        i['credit']                 = str(item['credit'])
        i['tags']                   = str(item['tags'])
        i['checked']                = str(item['checked'])
        item_list.append(i)
    return item_list

def __mappingTeamEmployees(data_json):
    item_list = []
    for item in data_json['employees']:
        i = {}
        i['id']                         = str(item['id'])
        i['holdedUserId']               = str(item['holdedUserId'])
        i['name']                       = str(item['name'])
        i['lastName']                   = str(item['lastName'])
        i['nationality']                = str(item['nationality'])
        i['mainLanguage']               = str(item['mainLanguage'])
        i['code']                       = str(item['code'])
        i['gender']                     = str(item['gender'])
        i['mainEmail']                  = str(item['mainEmail'])
        i['email']                      = str(item['email'])
        i['province']                   = str(item['address']['province'])
        i['country']                    = str(item['address']['country'])
        i['workplace']                  = str(item['workplace'])
        item_list.append(i)
    return item_list


def __mappingInvoicingContacts(data_json):
    item_list = []
    for item in data_json:
        i = {}
        i['id']                     = str(item['id'])
        i['customId']               = str(item['customId'])
        i['name']                   = str(item['name'])
        i['code']                   = str(item['code'])
        i['vatnumber']              = str(item['vatnumber'])
        i['tradeName']              = str(item['tradeName'])
        i['email']                  = str(item['email'])
        i['mobile']                 = str(item['mobile'])
        i['phone']                  = str(item['phone'])
        i['type']                   = str(item['type'])
        i['iban']                   = str(item['iban'])
        i['swift']                  = str(item['swift'])
        i['groupId']                = str(item['groupId'])
        try:
            i['salesaccount']       = str(item['clientRecord']['num'])
        except Exception:
            #print(f'salesaccount no pudo ser cargado para {contact['name']}' )
            i['salesaccount']       = pd.NA
        try:
            i['expenseaccount']     = str(item['supplierRecord']['num'])
        except Exception:
            #print(f'expenseaccount no pudo ser cargado para {contact['name']}' )
            i['expenseaccount']     = pd.NA
        i['paymentMethod']          = str(item['defaults']['paymentMethod'])
        i['dueDays']                = item['defaults']['dueDays']
        i['currency']               = item['defaults']['currency']
        i['paymentDay']             = item['defaults']['paymentDay']
        i['language']               = item['defaults']['language']
        i["salesTax"]               = str(item['defaults']['salesTax'])
        i["purchasesTax"]           = str(item['defaults']['purchasesTax'])
        i['accumulateInForm347']    = item['defaults']['accumulateInForm347']
        i['billAddress']            = item['billAddress']
        i['shippingAddresses']      = item['shippingAddresses']
        i['customFields']           = str(item['customFields'])
        i['socialNetworks']         = str(item['socialNetworks'])
        i['tags']                   = str(item['tags'])
        i['notes']                  = str(item['notes'])
        i['isperson']               = item['isperson']
        try:
            i['updatedAt']          = item['updatedAt']
        except Exception:
            i['updatedAt']          = pd.NA
        try:
            i['updatedHash']        = item['updatedHash']
        except Exception:
            i['updatedAt']          = pd.NA
        try:
            i['createdAt']          = item['createdAt']
        except Exception:
            i['createdAt']          = pd.NA
        i['contactPersons']         = str(item['contactPersons'])
        item_list.append(i)

    return item_list

def __mappingInvoicingProducts(data_json):
    item_list = []
    for item in data_json:
        i={}
        i['id']                     = item['id']
        i['kind']                   = item['kind']
        i['name']                   = item['name']
        i['desc']                   = item['desc']
        i['typeId']                 = item['typeId']
        i['contactId']              = str(item['contactId'])
        i['contactName']            = str(item['contactName'])
        try:
            i['price']              = float(item['price'])
        except Exception:
            i['price']              = pd.NA
        i['taxes']                  = str(item['taxes'])
        try:
            i['total']              = float(item['total'])
        except Exception:
            i['total']              = pd.NA
        i['hasStock']               = str(item['hasStock'])
        try:
            i['stock']              = str(item['stock'])
        except Exception:
            i['stock']              = pd.NA
        i['barcode']                = str(item['barcode'])
        i['sku']                    = str(item['sku'])
        try:
            i['cost']               = float(item['cost'])
        except Exception:
            i['cost']               = pd.NA
        try:
            i['purchasePrice']      = float(item['purchasePrice'])
        except Exception:
            i['purchasePrice']      = pd.NA
        try:
            i['weight']             = int(item['weight'])
        except Exception:
            i['weight']             = pd.NA
        i['tags']                   = str(item['hasStock'])
        i['categoryId']             = item['categoryId']
        i['factoryCode']            = item['factoryCode']
        i['forSale']                = item['forSale']
        i['forPurchase']            = item['forPurchase']
        i['salesChannelId']         = item['salesChannelId']
        i['expAccountId']           = item['expAccountId']
        i['warehouseId']            = item['warehouseId']
        item_list.append(i)

    return item_list

def __mappingInvoicingDocuments(data_json):
    item_list = []
    for item in data_json:
        i={}
        # Ajustamos los tipos de datos
        i['id']                                     = str(item['id'])
        i['contact']                                = str(item['contact'])
        i['contactName']                            = str(item['contactName'])
        i['desc']                                   = str(item['desc'])
        i['date']                                   = str(item['date'])
        try:
            i['dueDate']                            = int(item['dueDate'])
        except:
            i['dueDate']                            = pd.NA
        try:
            i['multipledueDate']                    = str(item['multipledueDate'])
        except:
            i['multipledueDate']                    = pd.NA
        try:
            i['forecastDate']                       = int(item['forecastDate'])
        except:
            i['forecastDate']                       = pd.NA
        i['notes']                                  = str(item['notes'])
        i['tags']                                   = str(item['tags'])
        i['products']                               = str(item['products'])
        i['tags']                                   = str(item['tags'])
        i['products']                               = str(item['products'])
        i['tax']                                    = float(item['tax'])
        i['subtotal']                               = float(item['subtotal'])
        i['discount']                               = float(item['discount'])        
        i['total']                                  = float(item['total'])
        i['language']                               = str(item['language'])
        i['status']                                 = int(item['status'])
        i['customFields']                           = str(item['customFields'])
        i['docNumber']                              = str(item['docNumber'])
        i['currency']                               = str(item['currency'])
        i['currencyChange']                         = float(item['currencyChange'])
        try:
            i['paymentMethodId']                    = str(item['paymentMethodId'])
        except:
            i['paymentMethodId']                    = pd.NA
        i['paymentsTotal']                          = float(item['paymentsTotal'])
        i['paymentsPending']                        = float(item['paymentsPending'])
        i['paymentsRefunds']                        = float(item['paymentsRefunds'])
        try:
            i['shipping']                           = str(item['shipping'])
        except:
            i['shipping']                           = pd.NA
        try:
            i['from_docType']                       = item['from.docType']
        except:
            i['from_docType']                       = pd.NA                 
        try:
            i['from_id']                            = item['from.id']
        except:
            i['from_id']                            = pd.NA
        item_list.append(i)

    return item_list
   

