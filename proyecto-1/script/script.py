import mysql.connector
import pandas as pd
from itertools import combinations

# Conectar a la base de datos MySQL
conexion = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",
    database="personas_db"
)

# Cargar una tabla completa en un DataFrame
df = pd.read_sql_query("SELECT * FROM tickets", conexion)

# Mostrar los primeros registros del DataFrame
#print(df)

# Cerrar la conexión a la base de datos
conexion.close()

df.info()

df['fecha'] = pd.to_datetime(df['fecha'])

df_cesta = df[['id_pedido','nombre_producto']]
#print(df_cesta)

# Agrupar los productos por id_pedido
df_agrupado = df_cesta.groupby('id_pedido')['nombre_producto'].apply(lambda producto: ','.join(producto))
#print(df_agrupado)

# Aplicar pd.get_dummies() para transformar los productos en columnas con 0/1
df_transacciones = df_agrupado.str.get_dummies(sep=',')
#print(df_transacciones)

# Soporte para cada producto
soporte = df_transacciones.mean() * 100
soporte.sort_values(ascending=False)

# Función para calcular la confianza entre dos productos en la muestra
def confianza(antecedente, consecuente):
    # Casos donde se compraron ambos productos
    conjunto_ac = df_transacciones[(df_transacciones[antecedente] == 1) &
                                   (df_transacciones[consecuente] == 1)]
    # Confianza = compras conjuntas / compras de producto A
    return len(conjunto_ac) / df_transacciones[antecedente].sum()

# Función para calcular el lift entre dos productos en la muestra
def lift(antecedente, consecuente):
    soporte_a = df_transacciones[antecedente].mean()
    soporte_c = df_transacciones[consecuente].mean()
    conteo_ac = len(df_transacciones[(df_transacciones[antecedente] == 1) &
                                   (df_transacciones[consecuente] == 1)])
    soporte_ac = conteo_ac / len(df_transacciones)
    return soporte_ac / (soporte_a * soporte_c)

# Definir un umbral para la confianza mínima
umbral_confianza = 0.05
asociaciones = []

# Generar combinaciones de productos y calcular confianza y lift
for antecedente, consecuente in combinations(df_transacciones.columns, 2):

    # Soporte del antecedente
    soporte_a = df_transacciones[antecedente].mean()

    # Calcular confianza
    conf = confianza(antecedente, consecuente)
    if conf > umbral_confianza:
        asociaciones.append({
            'antecedente': antecedente,
            'consecuente': consecuente,
            'soporte_a': round(soporte_a * 100,1),
            'confianza': round(conf * 100,1),
            'lift': round(lift(antecedente, consecuente),1)
        })


# Convertir las asociaciones en un DataFrame
df_asociaciones = pd.DataFrame(asociaciones)

# Ordenar las asociaciones por confianza de mayor a menor
df_asociaciones.sort_values(by='lift', ascending=False, inplace=True)
#print(df_asociaciones)

# Generamos una tabla de los productos únicos con su 'id_producto', 'id_seccion', 'id_departamento' para enriquecer la tabla de reglas
# Crear una tabla con los productos únicos y las columnas correspondientes
productos_unicos = df[['id_producto', 'id_seccion', 'id_departamento', 'nombre_producto']].drop_duplicates()
#print(productos_unicos)

df_asociaciones_enriquecido = df_asociaciones.merge(productos_unicos, left_on='antecedente', right_on='nombre_producto', how='left').drop(columns=['nombre_producto'])
df_asociaciones_enriquecido.columns = ['antecedente', 'consecuente', 'soporte_a', 'confianza', 'lift', 'id_producto_a', 'id_seccion_a', 'id_departamento_a']
#print(df_asociaciones_enriquecido)

df_asociaciones_enriquecido.to_csv('reglas.csv', index=False, sep=';', decimal=',')
