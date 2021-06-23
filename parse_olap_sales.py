# -- coding: utf-8 --
# author : Alexxander Lugovskoy
# vk.com/delta85

import json
import config
import pprint
import mysql_connect
from datetime import timedelta, datetime

import IikoServerApi
from logger import logger

#функция получает по api кастомный OLAP-отчет за указанный диапозон дат(включительно) и заносит его в таблицу olap_sales_main_table
#date_to (строка формата %Y-%m-%d, например:  2020-06-10) - самая ближняя дата к нынешней (самая свежая), до которой парсим. По умолчанию - текущая дата.
#date_from (строка формата %Y-%m-%d, например:  2020-05-10) - сама дяльняя дата (более старая) от нынешней, до которой парсим. По умолчанию - месяц назад.
def parseOlapSales(date_to = config.todayStr, date_from = config.month_ago_str):
    mysql = mysql_connect.getConnect() #открываем соединение с БД
    cursor = mysql.cursor()
    try:
        date_from_datetime = datetime.strptime(date_from, "%Y-%m-%d")
        date_to_datetime = datetime.strptime(date_to, "%Y-%m-%d")
    except ValueError:
        logger.writeError("ОШИБКА! в parse_olap_sales.parseOlapSeles. Передан некорректные значения для параметров del_from или del_to. Значение должны быть строками правильного формата. Пример правильной даты: 2020-08-27")
        exit()
    if date_from_datetime > date_to_datetime:
        logger.writeError("ОШИБКА! в parse_olap_sales.parseOlapSeles. Значение date_from больше чем date_to. Должно быть наоборот либо они должны быть равны")
        exit()
    logger.writeError(f"olap_sales.parseOlapSeles. Сейчас будем запрашивать отчеты с {str(date_from)} по {str(date_to)}")
    while date_from_datetime <= date_to_datetime:
        this_date_from_str = date_from_datetime.strftime("%Y-%m-%d")
        logger.writeError(f"olap_sales.parseOlapSeles. Запрашиваем отчет за {this_date_from_str} date_from= {this_date_from_str}, date_to = {this_date_from_str}")
        report = IikoServerApi.getCustomOlapPreset1(IikoServerApi.getToken().text, this_date_from_str, this_date_from_str)
        report_json = json.loads(report.text)
        report_data_json = report_json['data']
        logger.writeError("parse_olap_sales.parseOlapSeles получили данные, начинаем запись в бд...")
        for i in report_data_json:
            sql = "INSERT INTO `olap_sales_main_table` (`CloseTime`, `Delivery.IsDelivery`, `Department.Id`, `DiscountSum`, `DishAmountInt`, `DishCategory.Id`, `DishGroup.Id`, `DishId`, `DishName`, `DishSumInt`, `ItemSaleEventDiscountType`, `OpenDate.Typed`, `OperationType`, `OrderItems`, `OrderType`, `OriginName`, `ProductCostBase.ProductCost`, `ProductCostBase.Profit`, `RestaurantSection`, `SoldWithDish`, `UniqOrderId.Id`) VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s);"
            logger.writeError(f"olap_sales.parseOlapSeles. Подготовлен следующий sql запрос: {sql}")
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
            try:
                mysql.commit()
            except pymysql.err.IntegrityError as err:
                logger.writeError(f"olap_sales.parseOlapSeles. ОШИБКА! Не удалось выполнить следующий sql запрос: {sql} - {err}")
                date_from_datetime -= timedelta(days=1)
                continue
        logger.writeError(f"olap_sales.parseOlapSeles. В таблицу olap_sales_main_table был занесен отчет за {str(date_from_datetime)}", config.logger_base_path + f'\events\{config.todayStr}_events.log')
        date_from_datetime += timedelta(days=1)
    mysql.close() #ЗАКРЫВАЕМ соединение с БД

parseOlapSales("2021-01-12", "2021-01-12")