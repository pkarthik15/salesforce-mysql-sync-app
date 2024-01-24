import sqlalchemy as db
from sqlalchemy import event, create_engine, text, inspect, insert
from config import Configurations


db_url = f"mysql+pymysql://{Configurations.MYSQL_USERNAME}:{Configurations.MYSQL_PASSWORD}@{Configurations.MYSQL_SERVER}:{Configurations.MYSQL_PORT}/{Configurations.MYSQL_DB}?charset=utf8mb4"
engine = create_engine(db_url)


@event.listens_for(engine, "before_cursor_execute") 
def receive_before_cursor_execute(conn,  
 cursor, statement, params, context, executemany): 
    if executemany: 
        cursor.fast_executemany = True


def get_mysql_connection():
    connection = engine.connect()
    return  connection


def save_data_to_table(df, tablename):
    connection = get_mysql_connection()
    df.to_sql(tablename, connection, index=False, if_exists="append")
    if(not connection.closed):
        connection.close()


def save_data_to_table(df, tablename, method):
    connection = get_mysql_connection()
    df.to_sql(tablename, connection, index=False, if_exists="append", method=method)
    if(not connection.closed):
        connection.close()


def truncate_table(table_name):
    with engine.connect() as conn:
        result = conn.execute(text(f"TRUNCATE `{table_name}`;"))


def create_upsert_method(engine: db.Engine, meta:db.MetaData):
    """
    Create upsert method that satisfied the pandas's to_sql API.
    """
    
    def method(table, conn, keys, data_iter):
        
        # select table that data is being inserted to (from pandas's context)
        sql_table = db.Table(table.name, meta, autoload_with=engine)
        
        # list of dictionaries {col_name: value} of data to insert
        values_to_insert = [dict(zip(keys, data)) for data in data_iter]
        
        # create insert statement using postgresql dialect.
        # For other dialects, please refer to https://docs.sqlalchemy.org/en/14/dialects/
        insert_stmt = db.dialects.mysql.insert(sql_table).values(values_to_insert)
        
        primaryKeyColNames = [pk_column.name for pk_column in sql_table.primary_key.columns.values()]


        # create update statement for excluded fields on conflict
        update_values = {k: getattr(insert_stmt.inserted, k)
            for k in values_to_insert[0].keys()
            if k not in primaryKeyColNames}
        
        
        # create upsert statement. 
        # upsert_stmt = insert_stmt.on_conflict_do_update(
        #     index_elements=sql_table.primary_key.columns, # index elements are primary keys of a table
        #     set_=values_to_insert # the SET part of an INSERT statement
        # )
        upsert_stmt = insert_stmt.on_duplicate_key_update(
            **update_values
        )
        
        # execute upsert statement
        conn.execute(upsert_stmt)

    return method


def get_upsert_method():
    meta = db.MetaData()
    upsert_method = create_upsert_method(engine, meta)
    return upsert_method