import pandas as pd
from mysql_client import df_to_mysql_bulk,mysql_query_to_df
from datetime import datetime
datetime.now(tz=None)


host = "localhost"
user = "root"
passwd = "Root2019"
db = "n"

def location_stg_to_dwh():
    dateTimeObj = datetime.now()
    stg_dim_location = mysql_query_to_df(host, user, passwd, db, "select * from stg_dim_location")
    dwh_dim_location = mysql_query_to_df(host, user, passwd, db, "select * from dwh_dim_location")

    countryCodeList = list(stg_dim_location.countryCode.unique())
    if len(countryCodeList) > 1:
        countryCodeList.sort()


    if dwh_dim_location.empty == False:

        columns = ['countryCode', 'countryCodeId', 'town', 'townId', 'postcode', 'postcodeId']
        dwh_dim_location_df = pd.DataFrame(columns=columns)

        unify_df = dwh_dim_location.merge(stg_dim_location, indicator=True, how='outer', on=['countryCode','town','postcode'])
        unify_df.rename(columns={'_merge': 'action_type'}, inplace=True)

        append_df = unify_df[unify_df['action_type'] == 'right_only']
        for index, row in append_df.iterrows():
            countryCodeList = mysql_query_to_df(host, user, passwd, db, "select distinct countryCode from dwh_dim_location where countryCode ='{}'".format(row['countryCode']))
            countryCodeList = list(countryCodeList.countryCode.unique())
            if len(countryCodeList) > 0:
                townList = mysql_query_to_df(host, user, passwd, db,"select distinct town from dwh_dim_location where countryCode ='{}' and town ='{}'".format(row['countryCode'],row['town']))
                townList = list(townList.town.unique())
                if len(townList) > 0:
                    postcodeList = mysql_query_to_df(host, user, passwd, db,"select max(postcodeId-townId) as countryTownID from dwh_dim_location where countryCode ='{}' and town ='{}'".format(row['countryCode'], row['town']))
                    TownList = mysql_query_to_df(host, user, passwd, db,"select max(townId) TownId  from dwh_dim_location where countryCode ='{}' and town ='{}'".format(row['countryCode'], row['town']))
                    TownList = list(TownList.TownId.unique())
                    countryCodeList = mysql_query_to_df(host, user, passwd, db,
                                                 "select max(countryCodeId) countryCodeId  from dwh_dim_location where countryCode ='{}' and town ='{}'".format(
                                                     row['countryCode'], row['town']))
                    countryCodeList = list(countryCodeList.countryCodeId.unique())
                    postcode = list(postcodeList.countryTownID.unique())
                    dwh_dim_location_df = dwh_dim_location_df.append(pd.DataFrame({
                        "countryCode": row['countryCode'],
                        "countryCodeId": countryCodeList,
                        "town": row['town'],
                        "townId": TownList,
                        "postcode": row['postcode'],
                        "postcodeId": int(TownList[0])+int(postcode[0])+100}, index=[0]), ignore_index=True, sort=False)
                else:
                    print("insert town")
            else:
                countryCodeList = mysql_query_to_df(host, user, passwd, db,"select max(countryCodeId) countryCode from dwh_dim_location")
                countryCodeList = list(countryCodeList.countryCode.unique())
                countryCodeCounter = int(countryCodeList[0]) * 2
                townCounter = countryCodeCounter + 10000000
                postcodeCounter =townCounter + 100
                dwh_dim_location_df = dwh_dim_location_df.append(pd.DataFrame({
                    "countryCode": row['countryCode'],
                    "countryCodeId": countryCodeCounter,
                    "town": row['town'],
                    "townId": townCounter,
                    "postcode": row['postcode'],
                    "postcodeId": postcodeCounter}, index=[0]), ignore_index=True, sort=False)
        print(dwh_dim_location_df)
        df_to_mysql_bulk(dwh_dim_location_df, 'dwh_dim_location', host, user, passwd, db, type='append')


    else:

        columns = ['countryCode', 'countryCodeId', 'town', 'townId', 'postcode', 'postcodeId']
        dwh_dim_location_df = pd.DataFrame(columns=columns)
        countryCodeCounter = 0
        for c in countryCodeList:
            townCounter = 0
            countryCodeCounter += 1000000000
            townList = mysql_query_to_df(host, user, passwd, db, "select distinct town from stg_dim_location where countryCode = '{}'".format(c))
            townList = list(townList.town.unique())
            if len(townList) > 1:
                townList.sort()
            for t in townList:
                postcodeCounter = 0
                townCounter = countryCodeCounter + 10000000
                postcodeCounter = townCounter + postcodeCounter
                postcodeList = mysql_query_to_df(host, user, passwd, db,"select distinct postcode from stg_dim_location where town = '{}'".format(t))
                postcodeList = list(postcodeList.postcode.unique())
                if len(postcodeList) > 1:
                    postcodeList.sort()
                for pc in postcodeList:
                    postcodeCounter += 100
                    dwh_dim_location_df = dwh_dim_location_df.append(pd.DataFrame({
                        "countryCode": c,
                        "countryCodeId": countryCodeCounter,
                        "town": t,
                        "townId": townCounter,
                        "postcode": pc,
                        "postcodeId": postcodeCounter}, index=[0]), ignore_index=True, sort=False)
        dwh_dim_location_df["insert_time"] = dateTimeObj
        df_to_mysql_bulk(dwh_dim_location_df, 'dwh_dim_location',host, user, passwd, db,type = 'append')



def product_stg_to_dwh():
    dateTimeObj = datetime.now()
    Product_columns = [
                            'ProductCode',
                            'ProductDescription',
                            'Id'
                ]
    Product_df = pd.DataFrame(columns=Product_columns)

    stg_dim_product = mysql_query_to_df(host, user, passwd, db, "select * from stg_dim_product")
    productList = list(stg_dim_product.productCode.unique())
    if len(productList) > 1:
        productList.sort()
    if len(productList) > 0:
        dwh_dim_product = mysql_query_to_df(host, user, passwd, db, "select * from dwh_dim_product")
        productList = list(dwh_dim_product.productCode.unique())
        if len(productList) > 1:
            productList.sort()
        if len(productList)>0:
            maxProductId = mysql_query_to_df(host, user, passwd, db, "select max(productId) maxId from dwh_dim_product")
            maxId = list(maxProductId.maxId.unique())
            maxId = maxId[0]
            unify_df = dwh_dim_product.merge(stg_dim_product, indicator=True, how='outer', on=['productCode','productDescription'])
            unify_df.rename(columns={'_merge': 'action_type'}, inplace=True)
            append_df = unify_df[unify_df['action_type'] == 'right_only']
            for index, row in append_df.iterrows():
                maxId +=1
                Product_df = Product_df.append(
                    pd.DataFrame({"productCode": row['ProductCode'],
                                  "productDescription": row['productDescription'],
                                  "productId": maxId}, index=[0]),
                    ignore_index=True, sort=False)
            Product_df["insert_time"] = dateTimeObj
            df_to_mysql_bulk(Product_df, 'dwh_dim_product',host, user, passwd, db,type = 'append')
        else:
            stg_dim_product['id'] = stg_dim_product.groupby(['ProductCode', 'ProductDescription']).ngroup()
            stg_dim_product['id'] = stg_dim_product['id'] + 1
            stg_dim_product["insert_time"] = dateTimeObj
            df_to_mysql_bulk(stg_dim_product, 'dwh_dim_product',host, user, passwd, db,type = 'append')

def carrier_stg_to_dwh():
    dateTimeObj = datetime.now()
    Carrier_columns = [
                            'carrier',
                            'carrierId'
                ]
    Carrier_df = pd.DataFrame(columns=Carrier_columns)

    stg_dim_carrier = mysql_query_to_df(host, user, passwd, db, "select * from stg_dim_carrier where carrier is not null ")
    carrierList = list(stg_dim_carrier.carrier.unique())
    carrierList = [x for x in carrierList if x is not None]
    print(carrierList)
    if len(carrierList) > 1:
        carrierList.sort()
    if len(carrierList) > 0:
        dwh_dim_carrier = mysql_query_to_df(host, user, passwd, db, "select * from dwh_dim_carrier")
        productList = list(dwh_dim_carrier.carrier.unique())
        if len(productList) > 1:
            productList.sort()
        if len(productList)>0:
            maxProductcarrierId = mysql_query_to_df(host, user, passwd, db, "select max(carrierId) maxcarrierId from dwh_dim_carrier")
            maxcarrierId = list(maxProductcarrierId.maxcarrierId.unique())
            maxcarrierId = maxcarrierId[0]
            unify_df = dwh_dim_carrier.merge(stg_dim_carrier, indicator=True, how='outer', on=['carrier'])
            unify_df.rename(columns={'_merge': 'action_type'}, inplace=True)
            append_df = unify_df[unify_df['action_type'] == 'right_only']
            for index, row in append_df.iterrows():
                maxcarrierId +=1
                Product_df = Carrier_df.append(
                    pd.DataFrame({"carrier": row['carrier'],
                                  "carrierId": maxcarrierId}, index=[0]),
                    ignore_index=True, sort=False)
            stg_dim_carrier["insert_time"] = dateTimeObj
            df_to_mysql_bulk(Carrier_df, 'dwh_dim_carrier',host, user, passwd, db,type = 'append')
        else:
            stg_dim_carrier['carrierId'] = stg_dim_carrier.groupby(['carrier']).ngroup()
            stg_dim_carrier['carrierId'] = stg_dim_carrier['carrierId'] + 1
            stg_dim_carrier["insert_time"] = dateTimeObj
            df_to_mysql_bulk(stg_dim_carrier, 'dwh_dim_carrier',host, user, passwd, db,type = 'append')


def order_source_stg_to_dwh():
    dateTimeObj = datetime.now()
    order_source_columns = [
                            'order_source',
                            'order_sourceId'
                ]
    Order_Source_df = pd.DataFrame(columns=order_source_columns)

    stg_dim_order_source = mysql_query_to_df(host, user, passwd, db, "select * from stg_dim_order_source where orderSource is not null ")
    order_sourceList = list(stg_dim_order_source.orderSource.unique())
    order_sourceList = [x for x in order_sourceList if x is not None]
    print(order_sourceList)
    if len(order_sourceList) > 1:
        order_sourceList.sort()
    if len(order_sourceList) > 0:
        dwh_dim_order_source = mysql_query_to_df(host, user, passwd, db, "select * from dwh_dim_order_source")
        productList = list(dwh_dim_order_source.orderSourceId.unique())
        if len(productList) > 1:
            productList.sort()
        if len(productList)>0:
            maxProductorder_sourceId = mysql_query_to_df(host, user, passwd, db, "select max(orderSourceId) maxorder_sourceId from dwh_dim_order_source")
            maxorder_sourceId = list(maxProductorder_sourceId.maxorder_sourceId.unique())
            maxorder_sourceId = maxorder_sourceId[0]
            unify_df = dwh_dim_order_source.merge(stg_dim_order_source, indicator=True, how='outer', on=['orderSource'])
            unify_df.rename(columns={'_merge': 'action_type'}, inplace=True)
            append_df = unify_df[unify_df['action_type'] == 'right_only']
            for index, row in append_df.iterrows():
                maxorder_sourceId +=1
                Product_df = Order_Source_df.append(
                    pd.DataFrame({"order_source": row['order_source'],
                                  "order_sourceId": maxorder_sourceId}, index=[0]),
                    ignore_index=True, sort=False)
            Order_Source_df["insert_time"] = dateTimeObj
            df_to_mysql_bulk(Order_Source_df, 'dwh_dim_order_source',host, user, passwd, db,type = 'append')
        else:
            stg_dim_order_source['orderSourceId'] = stg_dim_order_source.groupby(['orderSource']).ngroup()
            stg_dim_order_source['orderSourceId'] = stg_dim_order_source['orderSourceId'] + 1
            stg_dim_order_source["insert_time"] = dateTimeObj
            df_to_mysql_bulk(stg_dim_order_source, 'dwh_dim_order_source',host, user, passwd, db,type = 'append')



def fact_orders_stg_to_dwh():
    dateTimeObj = datetime.now()
    stg_fact_order = mysql_query_to_df(host, user, passwd, db, "select * from stg_fact_order")
    dwh_dim_location = mysql_query_to_df(host, user, passwd, db, "select * from dwh_dim_location")
    dwh_dim_order_source = mysql_query_to_df(host, user, passwd, db, "select * from dwh_dim_order_source")
    dwh_dim_product = mysql_query_to_df(host, user, passwd, db, "select * from dwh_dim_product")
    unify_df = stg_fact_order.merge(dwh_dim_location, indicator=True, how='left', on=['countryCode', 'town', 'postcode'])
    unify_df.rename(columns={'_merge': 'action_type'}, inplace=True)
    unify_df = unify_df.drop(columns=['action_type','countryCode','postcode','town','insert_time'])
    unify_df = unify_df.merge(dwh_dim_order_source, indicator=True, how='left', on=['orderSource'])
    unify_df.rename(columns={'_merge': 'action_type'}, inplace=True)
    unify_df = unify_df.drop(columns=['action_type','orderSource','insert_time'])
    unify_df = unify_df.merge(dwh_dim_product, indicator=True, how='left', on=['productCode'])
    unify_df.rename(columns={'_merge': 'action_type'}, inplace=True)
    unify_df = unify_df.drop(columns=['action_type','productCode', 'productDescription_y','productDescription_x','insert_time'])
    unify_df["insert_time"] = dateTimeObj
    df_to_mysql_bulk(unify_df, 'dwh_fact_order', host, user, passwd, db, type='append')


def fact_dispatch_stg_to_dwh():
    dateTimeObj = datetime.now()
    stg_fact_dispatch= mysql_query_to_df(host, user, passwd, db, "select * from stg_fact_dispatch")
    dwh_dim_location = mysql_query_to_df(host, user, passwd, db, "select * from dwh_dim_location")
    dwh_dim_carrier = mysql_query_to_df(host, user, passwd, db, "select * from dwh_dim_carrier")
    dwh_dim_product = mysql_query_to_df(host, user, passwd, db, "select * from dwh_dim_product")
    unify_df = stg_fact_dispatch.merge(dwh_dim_product, indicator=True, how='left', on=['productCode'])
    unify_df.rename(columns={'_merge': 'action_type'}, inplace=True)
    unify_df = unify_df.drop(columns=['action_type', 'productCode', 'productDescription_y', 'productDescription_x','insert_time'])
    unify_df = unify_df.merge(dwh_dim_carrier, indicator=True, how='left', on=['carrier'])
    unify_df.rename(columns={'_merge': 'action_type'}, inplace=True)
    unify_df = unify_df.drop(columns=['action_type', 'carrier','insert_time'])
    unify_df = unify_df.merge(dwh_dim_location, indicator=True, how='left',on=['countryCode', 'town', 'postcode'])
    unify_df.rename(columns={'_merge': 'action_type'}, inplace=True)
    unify_df = unify_df.drop(columns=['action_type', 'countryCode', 'postcode', 'town','insert_time'])
    unify_df["insert_time"] = dateTimeObj
    df_to_mysql_bulk(unify_df, 'dwh_fact_dispatch', host, user, passwd, db, type='append')



if __name__ == "__main__":
    location_stg_to_dwh()
    product_stg_to_dwh()
    carrier_stg_to_dwh()
    order_source_stg_to_dwh()
    fact_orders_stg_to_dwh()
    fact_dispatch_stg_to_dwh()