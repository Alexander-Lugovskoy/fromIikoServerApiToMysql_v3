# author : Alexxander Lugovskoy
# vk.com/delta85

import pymysql

def getConnect():
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='',
        db='from_iiko_to_mysql',
        charset='utf8',
    )
    return(connection)

def getConnectTest():
    connection = pymysql.connect(
        host='46.21.252.245',
        user='portal',
        password='9td0S74mdWYnZkoQ',
        db='shauchak_poster',
        charset='utf8',
    )
    return(connection)