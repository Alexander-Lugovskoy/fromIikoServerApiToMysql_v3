# author : Alexxander Lugovskoy
# vk.com/delta85

import requests
import config
from logger import logger
import shutil

def getToken(api_login = config.api_login, api_pass=config.api_pass):
    logger.writeError(f"iikoServerApi.getToken Запрашиваем токен...")
    url = f"{config.api_base_url}/resto/api/auth?login={api_login}&pass={api_pass}"
    r_token = requests.get(url)
    if r_token.status_code != 200:
        logger.writeError(f"ОШИБКА iikoServerApi.getToken Не удалось получить токен, код ответа от сервера: {r_token.status_code}")
        exit()
    else:
        logger.writeError(f"iikoServerApi.getToken Токен успешно получен: {r_token.text}")
        return r_token

def getCustomOlapPreset1(token, date_from, date_to):
    logger.writeError(f"Вызвана функция IikoServerApi.getCustomOlapPreset1 date_from= {date_from}, date_to = {date_to}")
    url = f"{config.api_base_url}/resto/api/v2/reports/olap?&key={token}"
    headers = {
        "Content-type": "Application/json",
        "charset" : "utf-8"
    }
    json = {
      "reportType": "SALES",
      "groupByRowFields": [
          "UniqOrderId.Id",
          "DishId",
          "DishGroup.Id",
          "DishCategory.Id",
          "DishAmountInt",
          "OpenDate.Typed",
          "Department.Id",
          "CloseTime",
          "Delivery.IsDelivery",
          "DishAmountInt",
          "ItemSaleEventDiscountType",
          "OperationType",
          "RestaurantSection",
          "OrderType",
          "OriginName",
          "DishName",
          "SoldWithDish"
      ],
      "groupByColFields": [
      ],
      "aggregateFields": [
        "ProductCostBase.Profit",
        "OrderItems",
        "ProductCostBase.ProductCost",
        "DishSumInt",
        "DiscountSum",
      ],
      "filters": {
        "OpenDate.Typed": {
            "filterType": "DateRange",
            "periodType": "CUSTOM",
            "from": "2020-10-01",
            "to": "2020-10-17",
            "includeLow": "true",
            "includeHigh": "true"
        }
      }
    }
    r_olap = requests.post(url, headers = headers, json = json)
    if r_olap.status_code != 200:
        logger.writeError(f"IikoServerApi.getCustomOlapPreset1 код ответа от сервера iiko = {r_olap.status_code}  (не равняется 200). Завершаем выполнение скрипта на этой точке. Тело запроса: {r_olap.request.body} Ответ от сервера: {r_olap.text}")
        mysql.close()  # ЗАКРЫВАЕМ соединение с БД
        exit()
    logger.writeError(f"Функция IikoServerApi.getCustomOlapPreset1 завершила работу и вернула данные")
    return r_olap

#для записи из потока в файл, а от туда уже в бд. (не доработано)
def getCustomOlapPreset1_v2(token, date_from, date_to):
    logger.writeError(f"Вызвана функция IikoServerApi.getCustomOlapPreset1 date_from= {date_from}, date_to = {date_to}")
    url = f"{config.api_base_url}/resto/api/v2/reports/olap?&key={token}"
    headers = {
        "Content-type": "Application/json",
        "charset" : "utf-8"
    }
    json = {
      "reportType": "SALES",
      "groupByRowFields": [
          "UniqOrderId.Id",
          "DishId",
          "DishGroup.Id",
          "DishCategory.Id",
          "DishAmountInt",
          "OpenDate.Typed",
          "Department.Id",
          "CloseTime",
          "Delivery.IsDelivery",
          "DishAmountInt",
          "ItemSaleEventDiscountType",
          "OperationType",
          "RestaurantSection",
          "OrderType",
          "OriginName",
          "DishName",
          "SoldWithDish"
      ],
      "groupByColFields": [
      ],
      "aggregateFields": [
        "ProductCostBase.Profit",
        "OrderItems",
        "ProductCostBase.ProductCost",
        "DishSumInt",
        "DiscountSum",
      ],
      "filters": {
        "OpenDate.Typed": {
            "filterType": "DateRange",
            "periodType": "CUSTOM",
            "from": "2020-10-01",
            "to": "2020-10-17",
            "includeLow": "true",
            "includeHigh": "true"
        }
      }
    }
    r_olap = requests.post(url, headers = headers, json = json, stream = True)
    if r_olap.status_code != 200:
        logger.writeError(f"IikoServerApi.getCustomOlapPreset1 код ответа от сервера iiko = {r_olap.status_code}  (не равняется 200). Завершаем выполнение скрипта на этой точке. Тело запроса: {r_olap.request.body} Ответ от сервера: {r_olap.text}")
        mysql.close()  # ЗАКРЫВАЕМ соединение с БД
        exit()
    with open('response.json', 'wb') as file:
        shutil.copyfileobj(r_olap, file)  # copy in chunks, it works for large files
    logger.writeError(f"Функция IikoServerApi.getCustomOlapPreset1 завершила работу")

