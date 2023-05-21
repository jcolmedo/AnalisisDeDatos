import psycopg2
import csv

import config

# Establecer la conexión con la base de datos ( connection strig)
connection = psycopg2.connect(
    host="localhost",
    user="postgres",
    password=config.password,
    database="PruebaInfo2",
    port="5432"
)

# Habilitar la confirmación automática de transacciones
connection.autocommit = True
#print(connection)
consulta1="CREATE TABLE customers(customerid INT PRIMARY KEY, name VARCHAR(50), occupation VARCHAR(50), email VARCHAR(50), company VARCHAR(50), phonenumber VARCHAR(20), age INT)"
consulta2="CREATE TABLE agents(agentid INT primary key, name VARCHAR(50))"
consulta3="CREATE TABLE calls(callid INT primary key,agentid INT,customerid INT,pickedup SMALLINT,duration INT,productsold SMALLINT)"
# Definimos la funcion para crear tabla en postgres:
def crear_tabla(query):
    cursor = connection.cursor()
    #query = "CREATE TABLE customers(customerid INT PRIMARY KEY, name VARCHAR(50), occupation VARCHAR(50), email VARCHAR(50), company VARCHAR(50), phonenumber VARCHAR(20), age INT)"
    try:
        cursor.execute(query)
        print("La tabla ha sido creada exitosamente")
    except psycopg2.errors.DuplicateTable:
        print("La tabla ya existe")
    except psycopg2.Error as e:
        print(f"Ocurrió un error al crear la tabla: {e}")
    cursor.close()

#crear_tabla(consulta1)
#crear_tabla(consulta2)
#crear_tabla(consulta3)

def eliminar_tabla():
    cursor = connection.cursor()
    query = "DROP TABLE IF EXISTS customers"
    try:
        cursor.execute(query)
        print("La tabla ha sido eliminada exitosamente")
    except psycopg2.Error as e:
        print(f"Ocurrió un error al eliminar la tabla: {e}")
    cursor.close()

#eliminar_tabla()

def insertar_datos():
    cursor = connection.cursor()
    query = "INSERT INTO customers(customerid, name, occupation, email, company, phonenumber, age) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    try:
        cursor.execute(query, (1, 'Juan Perez', 'Desarrollador', 'juanperez@example.com', 'ABC Inc.', '555-1234', 30))
        cursor.execute(query, (2, 'Maria Gomez', 'Diseñadora', 'mariagomez@example.com', 'XYZ Ltda.', '555-5678', 25))
        cursor.execute(query, (3, 'Pedro Ramirez', 'Gerente', 'pedroramirez@example.com', 'MNO SA', '555-9012', 40))
        connection.commit()
        print("Los datos han sido insertados exitosamente")
    except psycopg2.Error as e:
        connection.rollback()
        print(f"{e}")
    cursor.close()

#insertar_datos()

def actualizar_datos():
    cursor = connection.cursor()
    query = "UPDATE customers SET age = %s WHERE customerid = %s"
    try:
        cursor.execute(query, (35, 1))
        connection.commit()
        print("El dato ha sido actualizado exitosamente")
    except psycopg2.Error as e:
        connection.rollback()
        print(f"Ocurrió un error al actualizar el dato: {e}")
    cursor.close()

#actualizar_datos()

def read_csv(csv_file,has_header=True):
    """
    Lee los datos desde un archivo CSV y devuelve una lista de filas.
    """
    with open(csv_file, 'r') as file:
        csv_data = csv.reader(file)
        rows = list(csv_data)
        if has_header:
            rows = rows[1:]
        return rows
    
def insert_data(table_name, data, connection):
    """
    Inserta los datos en una tabla de PostgreSQL usando una consulta dinámica.
    """
    cursor = connection.cursor()
    placeholders = ', '.join(['%s'] * len(data[0]))

    query = f"INSERT INTO {table_name} VALUES ({placeholders})"
    cursor.executemany(query, data)

    connection.commit()
    cursor.close()

agents = "H:/Informatorio/3 etapa/analisis de datos/AnalisisDeDatos/agents.csv"
calls=r"H:\Informatorio\3 etapa\analisis de datos\AnalisisDeDatos\calls.csv"
customers=r"H:\Informatorio\3 etapa\analisis de datos\AnalisisDeDatos\customers.csv"

insert_data("agents", read_csv(agents), connection)
insert_data("calls",read_csv(calls),connection)
insert_data("customers",read_csv(customers),connection)


    
