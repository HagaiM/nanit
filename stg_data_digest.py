import requests
import json
import pandas as pd
from mysql_client import df_to_mysql_bulk,run_query_mysql

def read_json_file(filename):
  with open(filename, 'r') as f:
    datastore = json.load(f)
    return datastore

def nested_get(input_dict, nested_key):
  internal_dict_value = input_dict
  for k in nested_key:
    try:
      internal_dict_value = internal_dict_value.get(k, None)
    except:
      internal_dict_value = internal_dict_value[0].get(k, None)
    if internal_dict_value is None:
      return None
  return internal_dict_value

def find_dict_leaf_value(key, dictionary):
  for k, v in dictionary.items():
    if k == key:
      yield v
    elif isinstance(v, dict):
      for result in find_dict_leaf_value(key, v):
        yield result
    elif isinstance(v, list):
      for d in v:
        for result in find_dict_leaf_value(key, d):
          yield result

conf_data = read_json_file('attributes.json')

tax_url='https://euvatrates.com/rates.json'

querystring = {}
headers = {}
url = "https://nanit-bi-assginment.s3.amazonaws.com/shippingdata.json"
events = requests.request("GET", url, headers=headers, params=querystring)


Dispatch_df_columns = [
            'orderId',
            'orderNumber',
            'carrier',
            'dispatchDate',
            'reference',
            'productCode',
            'productDescription',
            'quantity',
            'serialNumbers',
            'trackingNumber',
            'trackingURL',
            'countryCode',
            'town',
            'postcode'

]
Dispatch_df = pd.DataFrame(columns=Dispatch_df_columns)


Order_df_columns = [
                   'orderDate',
                   'orderId',
                   'orderNumber',
                   'orderSource',
                   'productCode',
                   'productDescription',
                   'quantity',
                    'currencyCode',
                   'unitCost',
                   'total',
                   'totalTax',
                   'addressLine1',
                   'addressLine2',
                   'countryCode',
                   'firstName',
                   'lastName',
                   'postcode',
                   'town'
            ]
Order_df = pd.DataFrame(columns=Order_df_columns)

location_df_columns = [
                        'CountryCode',
                        'Town',
                        'Postcode'

            ]
location_df = pd.DataFrame(columns=location_df_columns)

Product_columns = [
                        'ProductCode',
                        'ProductDescription'

            ]
Product_df = pd.DataFrame(columns=Product_columns)


Carrier_columns = [
                        'Carrier'
            ]
Carrier_df = pd.DataFrame(columns=Carrier_columns)


OrderSource_columns = [
                        'OrderSource'
            ]
OrderSource_df = pd.DataFrame(columns=OrderSource_columns)



if events != 200:
    for e in events.json()['Order']:
        CurrencyCode = nested_get(e, conf_data['CurrencyCode']['fullPath'])
        OrderDate = nested_get(e, conf_data['OrderDate']['fullPath'])
        OrderId = nested_get(e, conf_data['OrderId']['fullPath'])
        OrderNumber = nested_get(e, conf_data['OrderNumber']['fullPath'])
        OrderSource = nested_get(e, conf_data['OrderSource']['fullPath'])
        OrderLines = nested_get(e, conf_data['OrderLines']['fullPath'])
        Total = nested_get(e, conf_data['Total']['fullPath'])
        TotalTax = nested_get(e, conf_data['TotalTax']['fullPath'])
        AddressLine1 = nested_get(e, conf_data['AddressLine1']['fullPath'])
        AddressLine2 = nested_get(e, conf_data['AddressLine2']['fullPath'])
        CountryCode = nested_get(e, conf_data['CountryCode']['fullPath'])
        FirstName = nested_get(e, conf_data['FirstName']['fullPath'])
        LastName = nested_get(e, conf_data['LastName']['fullPath'])
        Postcode = nested_get(e, conf_data['Postcode']['fullPath'])
        Town = nested_get(e, conf_data['Town']['fullPath'])

        if type(OrderLines) == list:
          for r in OrderLines:
            ProductCode = (r['ProductCode'])
            ProductDescription = r['ProductDescription']
            Quantity = r['Quantity']
            UnitCost = r['UnitCost']
            Product_df = Product_df.append(pd.DataFrame({"ProductCode": ProductCode, "ProductDescription": ProductDescription}, index=[0]),ignore_index=True, sort=False)
            Order_df = Order_df.append(pd.DataFrame({"orderDate" : OrderDate,
                                                    "orderId" : OrderId,
                                                    "orderNumber" : OrderNumber,
                                                    "orderSource" : OrderSource,
                                                    "productCode" : ProductCode,
                                                    "productDescription" : ProductDescription,
                                                    "currencyCode": CurrencyCode,
                                                    "quantity" : Quantity,
                                                    "unitCost" : UnitCost,
                                                    "total" : Total,
                                                    "totalTax" : TotalTax,
                                                    "addressLine1" : AddressLine1,
                                                    "addressLine2" : AddressLine2,
                                                    "countryCode" : CountryCode,
                                                    "firstName" : FirstName,
                                                    "lastName" : LastName,
                                                    "postcode" : Postcode,
                                                    "town" : Town}, index=[0]), ignore_index=True,sort=False)

        DispatchedLines = nested_get(e, conf_data['DispatchedLines']['fullPath'])

        Carrier = nested_get(e, conf_data['Carrier']['fullPath'])
        TrackingNumber = nested_get(e, conf_data['TrackingNumber']['fullPath'])
        TrackingURL = nested_get(e, conf_data['TrackingURL']['fullPath'])
        DispatchDate = nested_get(e, conf_data['DispatchDate']['fullPath'])
        DispatchReference = nested_get(e, conf_data['DispatchReference']['fullPath'])
        if type(DispatchedLines) == list:
            for r in DispatchedLines:
                ProductCode= (r['ProductCode'])
                ProductDescription= r['ProductDescription']
                Quantity = r['Quantity']
                if type(r['SerialNumbers']) == list:
                   SerialNumbers = r['SerialNumbers'][0]
                else:
                   SerialNumbers = None
                Dispatch_df = Dispatch_df.append(pd.DataFrame({
                                                "orderId": OrderId,
                                                "orderNumber": OrderNumber,
                                                "carrier" : Carrier,
                                                "dispatchDate" : DispatchDate,
                                                "reference" : DispatchReference,
                                                "productCode" : ProductCode,
                                                "productDescription" : ProductDescription,
                                                "serialNumbers" : SerialNumbers,
                                                "quantity" : Quantity,
                                                "trackingNumber" : TrackingNumber,
                                                "trackingURL": TrackingURL,
                                                "countryCode" : CountryCode,
                                                "town": Town,
                                                "postcode" : Postcode

                                                }, index=[0]), ignore_index=True, sort=False)
            location_df = location_df.append(pd.DataFrame({
                "CountryCode": CountryCode,
                "Town": Town,
                "Postcode": Postcode}, index=[0]), ignore_index=True, sort=False)

            Carrier_df = Carrier_df.append(pd.DataFrame({"Carrier": Carrier}, index=[0]), ignore_index=True, sort=False)
            OrderSource_df = OrderSource_df.append(pd.DataFrame({"OrderSource": OrderSource}, index=[0]), ignore_index=True, sort=False)











if __name__ == "__main__":
    host = "localhost"
    user = "root"
    passwd = "Root2019"
    db = "n"
    run_query_mysql(host, user, passwd, db, "truncate table stg_dim_location")
    run_query_mysql(host, user, passwd, db, "truncate table stg_dim_product")
    run_query_mysql(host, user, passwd, db, "truncate table stg_dim_carrier")
    run_query_mysql(host, user, passwd, db, "truncate table stg_dim_orderSource")
    run_query_mysql(host, user, passwd, db, "truncate table stg_fact_dispatch")
    run_query_mysql(host, user, passwd, db, "truncate table stg_fact_order")
    location_df = location_df.drop_duplicates(inplace=False)
    Product_df = Product_df.drop_duplicates(inplace=False)
    Carrier_df = Carrier_df.drop_duplicates(inplace=False)
    OrderSource_df = OrderSource_df.drop_duplicates(inplace=False)
    df_to_mysql_bulk(location_df, 'stg_dim_location',host, user, passwd, db,type = 'append')
    df_to_mysql_bulk(Product_df, 'stg_dim_product',host, user, passwd, db,type = 'append')
    df_to_mysql_bulk(Carrier_df, 'stg_dim_carrier',host, user, passwd, db,type = 'append')
    df_to_mysql_bulk(OrderSource_df, 'stg_dim_order_source',host, user, passwd, db,type = 'append')
    df_to_mysql_bulk(Dispatch_df, 'stg_fact_dispatch',host, user, passwd, db,type = 'append')
    df_to_mysql_bulk(Order_df, 'stg_fact_order',host, user, passwd, db,type = 'append')
