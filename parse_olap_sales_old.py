# -- coding: utf-8 --
# author : Alexxander Lugovskoy
# vk.com/delta85

import IikoServerApi
import json
import config
import pprint
import mysql_connect
from datetime import timedelta, date

mysql = mysql_connect.getConnect() #открываем соединение с БД
cursor = mysql.cursor()
date_from = date(2019, 3, 4)
date_to = config.old_data
while date_from != date_to:
    date_from = date_from - timedelta(days=1)
    this_date_str = str(date_from)
    print("Запрашиваем отчет за ", this_date_str)
    report = IikoServerApi.getCustomOlapPreset1(IikoServerApi.getToken(config.api_login, config.api_pass).text, this_date_str, this_date_str)
    print(report.status_code)
    report_json = json.loads(report.text)
    report_data_json = report_json['data']
    print('парсим данные...')
    for i in report_data_json:
        sql = "INSERT INTO `olap_sales_main_table` (`CloseTime`, `Delivery.IsDelivery`, `Department.Id`, `DiscountSum`, `DishAmountInt`, `DishCategory.Id`, `DishGroup.Id`, `DishId`, `DishName`, `DishSumInt`, `ItemSaleEventDiscountType`, `OpenDate.Typed`, `OperationType`, `OrderItems`, `OrderType`, `OriginName`, `ProductCostBase.ProductCost`, `ProductCostBase.Profit`, `RestaurantSection`, `SoldWithDish`, `UniqOrderId.Id`) VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s);"
        cursor.execute(sql, (
            i['CloseTime'],
            i['Delivery.IsDelivery'],
            i['Department.Id'],
            i['DiscountSum'],
            i['DishAmountInt'],
            i['DishCategory.Id'],
            i['DishGroup.Id'],
            i['DishId'],
            i['DishName'],
            i['DishSumInt'],
            i['ItemSaleEventDiscountType'],
            i['OpenDate.Typed'],
            i['OperationType'],
            i['OrderItems'],
            i['OrderType'],
            i['OriginName'],
            i['ProductCostBase.ProductCost'],
            i['ProductCostBase.Profit'],
            i['RestaurantSection'],
            i['SoldWithDish'],
            i['UniqOrderId.Id']
        ))
        mysql.commit()

mysql.close() #ЗАКРЫВАЕМ соединение с БД