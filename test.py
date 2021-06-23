# -- coding: utf-8 --
# author : Alexxander Lugovskoy
# vk.com/delta85

import IikoServerApi
import json
import requests
import config
import pprint
import mysql_connect

from logger import logger
'''
print('получаем токен...')
token = IikoServerApi.getToken(config.api_login, config.api_pass)
print(token)
'''

'''
olap = IikoServerApi.getCustomOlap(config.token, "SALES", "20.07.2020", "20.08.2020")
print(olap.status_code)
pprint.pprint(olap.content)
'''
'''
report = IikoServerApi.getSalesReport(config.token, config.organizationId_merediana, "20.07.2020", "20.08.2020")
print(report.status_code)
pprint.pprint(report.text)
'''

'''
report = IikoServerApi.getCustomOlap(config.token, "SALES", "20.07.2020", "20.08.2020")
print(report.status_code)
#pprint.pprint(report.text)
pprint.pprint(report.json())
'''
#r = report.encoding
#print(r)
"""
report_json = report.json()
report_data = report_json["data"]
report_data_json = json.loads(report_data)
print(report_data_json)
"""

'''
report = IikoServerApi.getCustomOlapNew(config.token, "SALES", "10.08.2020", "20.08.2020")
print(report.status_code)
print(report.request.body)
print(report.request.headers)
print(report.request.url)
'''
#pprint.pprint(report.text)
#pprint.pprint(report.json())

'''
print("получаем отчет...")
report = IikoServerApi.getCustomOlapPreset1(config.token, "2020-08-26", "2020-08-26")
print(report.status_code)
#print(report.text)
report_data_json = json.loads(report.text)
pprint.pprint(report_data_json["data"][0:100])
'''

#IikoServerApi.getCustomOlapPreset1_v2(IikoServerApi.getToken().text, "2020-08-27", "2020-08-27")

logger.writeError("Тест логгера")

