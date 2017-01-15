from datetime import date,datetime
from logging import getLogger, INFO, DEBUG

from fm_log.log import LoggingSetup

from database.sql_executer import connect_to_db

from sql import (
    select_data,
    insert_data)

db_droplet = connect_to_db('decoded', 'thachbui', 'droplet')

db = connect_to_db('decoded', 'thachbui', 'test')

log = getLogger('restore weather')



class WeatherData(object):
    def __init__(self):
        self.setup_log()

    def restore(self):

        for table in self.tables():
            # self.drop_table(table)
            date_str = table[8:]
            log.info("Restoring table: {} - datestr: {}".format(table, date_str))
            sql_data = select_data.get_data.format(table_name=table)
            cursor_name = "get_weather"
            weather_instances = db.fetch_all_server_side(cursor_name, sql_data)

            # start inserting

            db_droplet.db_connection.autocommit = False
            with db_droplet.db_connection.cursor() as cursor:

                partition_date = datetime.strptime(date_str,'%Y%m%d')
                sql = insert_data.add_partition
                input = dict(date=partition_date)
                cursor.execute(sql, input)

                for row in weather_instances:
                    input = row._asdict()
                    sql = insert_data.upsert.format(date_str=date_str)
                    cursor.execute(sql, input)

                db_droplet.db_connection.commit()

    def drop_table(self, table):
        sql = select_data.drop_table.format(table_name=table)
        db_droplet.modify_rows(sql)

    def insert_weather_instance(self, cursor, date_str, row):
        input = row._asdict()
        log.info(input)
        sql = insert_data.insert_weather.format(date_str=date_str)
        cursor.execute(sql, input)


    def prepare_statement(self, cursor, date_str):
        sql = insert_data.prepare_table
        input = dict(date_str=date_str)
        cursor.execute(sql, input)

    def tables(self):
        sql = select_data.get_weather_tables
        ret = db.fetch_all_rows(sql)
        for row in ret.query_data:
            yield row.weather_table

    def setup_log(self):
        setup_log = LoggingSetup(process_name='weather_restore',
                                 subdirectory='weather_restore',
                                 daily_file=False,
                                 console_level=INFO,
                                 file_level=INFO)
        setup_log.init_logging()
        log.info("restore successful!")

if __name__ == '__main__':
    run = WeatherData()
    run.restore()