import pymysql

class DBConnect:
    def __init__(self, host, root, password):
        self.host = host
        self.root = root
        self.password = password
        self.conn = None
        self.db = None
        self.table = None

    def connect(self):
        if self.conn is None:
            self.conn = pymysql.connect(host=self.host, user=self.root, password=self.password)
        return self.conn

    def create_db(self, db_name):
        connection = self.connect()
        cur = connection.cursor()
        sql = f"CREATE DATABASE IF NOT EXISTS {db_name}"
        cur.execute(sql)
        connection.commit()
        self.conn.select_db(db_name)
        self.db = db_name



    def create_table(self, table_name):
        connection = self.connect()
        cur = connection.cursor()
        sql = f"""CREATE TABLE IF NOT EXISTS {table_name}(
               id INT PRIMARY KEY AUTO_INCREMENT,
               weapon_id VARCHAR(50),
               weapon_name VARCHAR(50),
               weapon_type VARCHAR(50),
               range_km INT,
               weight_kg FLOAT,
               manufacturer VARCHAR(50),
               origin_country VARCHAR(50),
               storage_location VARCHAR(50),
               year_estimated INT,
               level_risk VARCHAR(50))"""
        cur.execute(sql)
        connection.commit()
        self.table = table_name


    def insert_into_table(self, weapon_id, weapon_name, weapon_type, range_km, weight_kg, manufacturer, origin_country, storage_location, year_estimated, level_risk):
        connection = self.connect()
        cur = connection.cursor()
        sql = f"""INSERT INTO {self.table} (weapon_id, weapon_name, weapon_type, range_km, weight_kg, manufacturer, origin_country, storage_location, year_estimated, level_risk)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cur.execute(sql, ({weapon_id}, {weapon_name}, {weapon_type}, {range_km}, {weight_kg}, {manufacturer}, {origin_country}, {storage_location}, {year_estimated}, {level_risk}))
        connection.commit()



    def select_all(self):
        connection = self.connect()
        cur = connection.cursor()
        sql = f"SELECT * FROM {self.table}"
        cur.execute(sql)
        result = cur.fetchall()
        return result


    def close_connection(self):
        self.conn.close()
        self.conn = None


