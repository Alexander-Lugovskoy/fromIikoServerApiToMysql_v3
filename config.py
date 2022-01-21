# -- coding: utf-8 --

# author : Alexxander Lugovskoy
# vk.com/delta85

from datetime import datetime, timedelta
import os

#знаю что константы в верхнем регистре описывать нужно, я больше не буду))

# token и api pass скрыты перед публикацией для безопасности
api_base_url = "https://itle-meridiannaya.iiko.it:443"
api_login = ""
api_pass = ""
organizationId_steik = "8362bcc1-7b7a-11e7-80df-d8d38565926f"
organizationId_merediana = "b5ec01ba-8aa0-11e9-80dd-d8d385655247"
token = ""

base_path = os.path.dirname(os.path.abspath(__file__)) #абсолютный путь до корневой папки проекта
logger_base_path = base_path+'\logger'  # местоположение лог-файлов

#legacy api version query examples for get olap sales REPORT in XML
#query_for_sales_olap = "https://itle-meridiannaya.iiko.it:443/resto/api/reports/olap?key=5290491e-0035-adb6-2697-60398c77e5e7&report=SALES&from=25.08.2020&to=25.08.2020&groupRow=OpenDate.Typed&groupCol=DishId&groupCol=DishGroup.Id&groupCol=DishCategory.Id&groupCol=Department.Id&groupCol=CloseTime&groupCol=Delivery.IsDelivery&groupCol=DishCode&groupCol=DishAmountInt&groupCol=ItemSaleEventDiscountType&groupCol=UniqOrderId&groupCol=OrderItems&groupCol=OperationType&groupCol=RestaurantSection&agr=DiscountSum&agr=DishSumInt&agr=OrderType&agr=OriginName"
#query_for_sales_olap2 = "https://itle-meridiannaya.iiko.it:443/resto/api/reports/olap?key=5290491e-0035-adb6-2697-60398c77e5e7&report=SALES&from=25.08.2020&to=25.08.2020&groupRow=OpenDate.Typed&groupCol=DishId&groupRow=UniqOrderId&groupCol=DishGroup.Id&groupCol=DishCategory.Id&groupCol=Department.Id&groupCol=CloseTime&groupCol=UniqOrderId&groupCol=Delivery.IsDelivery&groupCol=DishCode&groupCol=DishAmountInt&groupCol=ItemSaleEventDiscountType&groupCol=OrderItems&groupCol=OperationType&groupCol=RestaurantSection&agr=DiscountSum&agr=DishSumInt&agr=OrderType&agr=OriginName&agr=OrderNum"

#настройки времени
today = datetime.today()
today = today.date()  # текущая дата
old_data = "2019-01-24"
default_time = " 00:01:00"

month_ago = today - timedelta(days=30)
yesterday = today - timedelta(days=1)

todayStr = str(today)
yesterdayStr = str(yesterday)
month_ago_str = str(month_ago)

today_datetime = todayStr + default_time
yesterday_datetime = yesterdayStr + default_time
month_ago_datetime = month_ago_str + default_time
old_datetime = old_data + default_time

requestTimeout = "00:05:00" # дефолтное время на получение ответа от сервера iiko
