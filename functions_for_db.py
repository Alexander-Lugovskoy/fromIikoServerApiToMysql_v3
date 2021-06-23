# author : Alexxander Lugovskoy
# vk.com/delta85

from datetime import datetime, timedelta
import config
import mysql_connect

from logger import logger

# тут будут описаны основные функции, для обращения к базе данных, но не все :)

# удаляет из таблицы olap_sales_main_table строки за определенный период, основываясь на столбце CloseTime
# del_from(строка) - дата, начиная от которой удалять(включительно). По умолчанию - 30 дней назад.
# del_to(срока) - дата до которой удалять (включительно). По умолчанию - сегодняшняя дата.
# формат даты: 2020-08-27
# если вызвать функцию без параметров, то удалит данные за последние 30 дней.
def DelOlapRowsByDataRange (del_from = config.month_ago_str, del_to = config.todayStr):
    logger.writeError(f"fucntion_for_db.DelOlapRowsByDataRange вызвона функция functions_for_db.DelOlapRowsByDataRange del_from = {del_from}, del_to = {del_to}")
    mysql = mysql_connect.getConnect()
    cursor = mysql.cursor()
    try:
        del_from_datetime = datetime.strptime(del_from, "%Y-%m-%d")
        del_to_datetime = datetime.strptime(del_to, "%Y-%m-%d")
    except ValueError:
        logger.writeError("ОШИБКА! в fucntion_for_db.DelOlapRowsByDataRange. Передан некорректные значения для параметров del_from или del_to. Значение должны быть строками правильного формата. Пример правильной даты: 2020-08-27")
        exit()
    if del_to_datetime < del_from_datetime:
        logger.writeError("ОШИБКА! в fucntion_for_db.DelOlapRowsByDataRange. Значение date_to меньше чем date_from. Должно быть наоборот либо они должны быть равны")
        exit()
    while del_from_datetime <= del_to_datetime:
        del_to_datetime_str = del_to_datetime.strftime("%Y-%m-%d")
        sql = f"DELETE FROM `olap_sales_main_table` WHERE `CloseTime` LIKE '%{del_to_datetime_str}%';"
        logger.writeError(f"functions_for_db.DelOlapRowsByDataRange выполняется следующий sql запрос: {sql}")
        try:
            cursor.execute(sql)
            logger.writeError(f"functions_for_db.DelOlapRowsByDataRange успешно выполнился следующий sql запрос: {sql}")
            logger.writeError(f"functions_for_db.DelOlapRowsByDataRange удалены все данные из таблицы olap_sales_main_table за {del_to_datetime_str} , основываясь на столбце CloseTime", src = config.logger_base_path+f'\events\{config.todayStr}_events.log')
        except pymysql.err.IntegrityError as err:
            logger.writeError(f" ОШИБКА! functions_for_db.DelOlapRowsByDataRange не удалось выполнить следующий sql запрос: {sql} , {err}")
            logger.writeError(f" ОШИБКА! functions_for_db.DelOlapRowsByDataRange не удалось удалить данные из olap_sales_main_table за {del_to_datetime_str} , основываясь на CloseTime. Для деталей посмотрите main_debug_log.log", src = config.logger_base_path + f'\events\{config.todayStr}_events.log')
        mysql.commit()
        del_to_datetime -= timedelta(days = 1)
    logger.writeError("Функция functions_for_db.DelOlapRowsByDataRange успешно завершила свою работу!")