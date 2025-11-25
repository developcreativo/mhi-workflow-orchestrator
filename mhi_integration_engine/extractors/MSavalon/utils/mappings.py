import pandas as pd
import json

def MapDataSet(dataset, data_json:json, data_in_json=None) -> pd.DataFrame:
    # Mapeamos el json a dataframe de pandas, ya sea directamente o bien recorriendo y mapeando
    try:
        match dataset:
            case 'property.reservationRoomStay.modification' |'property.reservationRoomStay.creation' |'property.room' | 'company.summary' | 'property.user' | 'property.revenue.detail' | 'property.invoice' | 'property.propertyManagement.accountReceivableAging':
                if data_in_json == None:
                    data_df = pd.DataFrame(data_json)
                else:
                    data_df = pd.DataFrame(data_json[data_in_json]) 
            case 'team.employees':
                data_df = pd.DataFrame(__mappingTeamEmployees(data_json))
            # En otro caso 
            case _:
                if data_in_json == None:
                    data_df = pd.json_normalize(data_json)
                else:
                    data_df = pd.json_normalize(data_json[data_in_json])    
        return data_df
    except Exception as e:
        msg=f'El mapping del dataset {dataset} ha fallado, error:{type(e)}: {e}.'
        print(msg)
        raise(msg)

# Aqui es donde esta la logica de datos

def __mappingTeamEmployees(data_json):
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
