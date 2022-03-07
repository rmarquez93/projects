from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, rank, row_number, desc, when
from pyspark.sql.window import Window

sc = SparkSession.builder.appName("Obligatorio Grupal").getOrCreate()

files_path = r"C:\\Users\\Rodri\\OneDrive\\Escritorio\\Rodri\\Documentación a presentar Masters\\Master Big data uni barcelona\\Material de Master\\Módulo 1\\Obligatorio Grupal Spark\\"


# Leemos ambos data frames

stock_df = sc.read.\
                    option("header","true").\
                    option("infer schema", "true").\
                    csv(files_path+"stock.csv")

purchases_df = sc.read.option("header", "true")\
                      .option("infer schema","true").\
                       json(files_path+"purchases.json")


# Ejercicio 1 - Los 10 productos más comprados.

print("-"*40+" Ejercicio 1 en Data Frame "+"-"*40)

# En este caso, agrupamos por product ID y hacemos el recuento para obtener la cantidad de productos
# Mas comprados. Una vez hecho esto, ordenamos por el recuento y por producto y hacemos show 10 para mostrar
# el top 10

purchases_df.groupby("product_id").count().orderBy("count", "product_id", ascending=0).show(10)

print("-"*40+" Ejercicio 1 en SQL "+"-"*40)

#Generamos la temp view de purchases para hacer consultas en SQL

purchases_df.createOrReplaceTempView("purchases")

#Query típica de agrupación. Se hace un recuento de cantidad de productos y se agrupa por product Id.
# Posteriormente se ordena por recuento y producto y un show 10 para mostrar el top 10.

sc.sql("SELECT\
        PRODUCT_ID,\
        COUNT(*) AS COUNT_OF_PRODUCTS\
        FROM PURCHASES\
        GROUP BY PRODUCT_ID\
        ORDER BY COUNT_OF_PRODUCTS DESC, PRODUCT_ID DESC").\
        show(10)

#Ejercicio 2 - Porcentaje de compra de cada tipo de producto (item_type).

print("-"*40+" Ejercicio 2 en Data Frame "+"-"*40)

# Agrupamos por Item type y hacemos un recuento. Posteriormente seleccionamos la columna item type y una nueva
# Columna calculada que resulta de la división del recuento específico de cada item type dividido el recuento
# total del data frame. Una vez hecho esto, le asignamos el nombre percentage a dicha columna.

purchases_df.groupby("ITEM_TYPE").count().select("ITEM_TYPE", (col("count") / purchases_df.count()).alias("percentage")).show()

print("-"*40+" Ejercicio 2 en SQL "+"-"*40)

# Operación de cálculo mas simple, donde se divide el recuento agrupado por item type entre la subquery
# que da como resultado el recuento de todo el data frame.

sc.sql("SELECT \
        ITEM_TYPE,\
        COUNT(*) / (SELECT COUNT(*) FROM PURCHASES) AS PERCENTAGE\
        FROM PURCHASES\
        GROUP BY ITEM_TYPE").show()

#Ejercicio 3 - Obtener los 3 productos más comprados por cada tipo de producto.

print("-"*40+" Ejercicio 3 en Data Frame "+"-"*40)

# En este caso se generaron 3 líneas de código para obtener el resultado. En la primera línea, se genera un
# nuevo df que agrupa por producto y item y genera un nueva columna de recuento.
# En la segunda línea, hacemos una partición por item type y ordenada por el recuento total de cada product id
# de ese item type. Se ordena también por product para tratar de tener resultados mas homogéneos cuando surjan
# "empates" entre dos productos de una categoría.
# Por último, se genera una nueva columna que es un ranking generado a partir de la partición anterior. Se filtra
# por ranking <= a 3 para obtener el top 3 por tipo de producto.

df_with_sales = purchases_df.groupby("product_id", "item_type").count()
window = Window.partitionBy(df_with_sales.item_type).orderBy(df_with_sales["count"].desc(), df_with_sales["product_id"])
df_with_sales.withColumn("Ranking", row_number().over(window)).filter(col("ranking") <= 3).\
    select("item_type","product_id","count").show()

print("-"*40+" Ejercicio 3 en SQL "+"-"*40)

# La primera subquery es para generar el ranking. Se hace una partición por tipo de producto ordenado por cantidad
# vendida.
# la segunda es la que le da a la primera los recuentos calculados de cantidades por productos

sc.sql("SELECT\
        *\
        FROM (\
            SELECT\
            PRODUCT_ID,\
            ITEM_TYPE,\
            ROW_NUMBER() OVER(PARTITION BY ITEM_TYPE ORDER BY QUANTITY_SOLD DESC) AS RANKING\
                FROM (\
                    SELECT\
                    ITEM_TYPE,\
                    PRODUCT_ID,\
                    COUNT(*) AS QUANTITY_SOLD\
                    FROM PURCHASES\
                    GROUP BY ITEM_TYPE, PRODUCT_ID)\
            )\
        WHERE RANKING <= 3").show()





# Ejercicio 4 - Obtener los productos que son más caros que la media del precio de los productos.

print("-"*40+" Ejercicio 4 en Data Frame "+"-"*40)

# Seleccionamos por product id, item type y precio y hacemos un filtro condicional donde el precio
# sea mayor al cálculo agregado a nivel de data frame del promedio general de precios.

purchases_df.select("product_id","item_type", "price").\
    where(col("price") > \
          purchases_df.agg({"price" : "mean"}).collect()[0][0]).\
    show()


print("-"*40+" Ejercicio 4 en SQL "+"-"*40)

# En este caso se aplica directamente una cláusula WHERE donde el precio sea mayor
# a la subquery que devuelve el promedio general de precios.

sc.sql("SELECT\
        PRODUCT_ID,\
        ITEM_TYPE,\
        PRICE\
        FROM PURCHASES\
        WHERE PRICE > (SELECT AVG(PRICE) FROM PURCHASES)").show()


#Ejercicio 5 - Indicar la tienda que ha vendido más productos.

print("-"*40+" Ejercicio 5 en Data Frame "+"-"*40)

# Se hace una agrupación por id de tienda y un recuento de entradas para cada una de ellas.
# Posteriormente se ordena de manera descendente a partir de ese recuento y se le da show(1) para mostrar
# El top 1.

purchases_df.groupby("shop_id").count().orderBy("count", ascending= 0).show(1)


print("-"*40+" Ejercicio 5 en SQL "+"-"*40)

# En este caso, se hace una agrupación por shop id y se hace el recuento de entradas.
# se ordena por cantidad de manera descendiente y se pide show(1) para mostrar el top 1.

sc.sql("SELECT\
        SHOP_ID,\
        COUNT(*) AS QUANTITY_OF_SALES\
        FROM PURCHASES\
        GROUP BY SHOP_ID\
        ORDER BY QUANTITY_OF_SALES DESC").show(1)


#Ejercicio 6 - Indicar la tienda que ha facturado más dinero.

print("-"*40+" Ejercicio 6 en Data Frame "+"-"*40)

# Se agrupa por shop id y se hace una suma agregada por ese criterio. Posteriormente se ordena por la suma
# mencionada de manera descendente y se pide mostrar solamente el primer registro.

purchases_df.groupby("shop_id").sum("price").orderBy("sum(price)", ascending=0).show(1)


print("-"*40+" Ejercicio 6 en SQL "+"-"*40)

# Se hace un campo calculado para la suma de los precios y se agrupa por shop Id.
# Se ordena descendientemente por los montos de ventas y se pide devolver solo el primer registro.

sc.sql("SELECT\
        SHOP_ID,\
        SUM(PRICE) AS AMOUNT_SOLD\
        FROM PURCHASES\
        GROUP BY SHOP_ID\
        ORDER BY AMOUNT_SOLD DESC").show(1)



#Ejercicio 7 - Dividir el mundo en 5 áreas geográficas iguales según la longitud
#(location.lon) y agregar una columna con el nombre del área geográfica, por ejemplo, “área1”, “área2”, …

print("-"*40+" Ejercicio 7 en Data Frame "+"-"*40)

# Se genera un nuevo data frame para usar en este ejercicio. En este nuevo DF se genera una nueva columna
# Con varias cláusulas when que determinarán el criterio en el cual las tiendas ingresarán en su área correspondiente.

purchases_with_areas = purchases_df.select("*").withColumn("Areas", \
                                when(purchases_df.location.lon < -108, "area 1").\
                                when((-108 <= purchases_df.location.lon) & (purchases_df.location.lon < -36), "area 2").\
                                when((-36 <= purchases_df.location.lon) & (purchases_df.location.lon < 36), "area 3").\
                                when((36 <= purchases_df.location.lon) & (purchases_df.location.lon < 108), "area 4").\
                                when(purchases_df.location.lon >= 108, "area 5"))

#Sub 1 - ¿En qué área se utiliza más PayPal?

print("-"*40+" Ejercicio 7.1 en Data Frame "+"-"*40)

# Se aplica un filtro por payment type para obtener los registros que fueron por paypal. Se agrupa por área
# y se obtiene el recuento por área ordenado de manera descendente. Se pide un show(1) para mostrar el top 1.

purchases_with_areas.filter(col("payment_type") == "paypal").groupby("Areas").count().orderBy("count", ascending= 0).show(1)

#Sub 2 - ¿Cuáles son los 3 productos más comprados en cada área?

print("-"*40+" Ejercicio 7.2 en Data Frame "+"-"*40)

# En este caso, se aplicó la misma lógica que en la pregunta 3. Se generó un nuevo dataframe agrupado por
# Areas y product_id y recontando los datos.
# A partir de ese nuevo DF se generó una partición por área ordenada por el recuento de manera descendiente.
# Por último, se hace un row_number para rankear las entradas de las particiones y se filtra por <= 3 para obtener
# El top 3 por región.

top_prod_by_region = purchases_with_areas.groupby("Areas", "product_id").count()
window_by_region = Window.partitionBy(top_prod_by_region.Areas).orderBy(top_prod_by_region["count"].desc())
top_prod_by_region.withColumn("count", row_number().over(window_by_region).alias("ranking")).filter(col("ranking") <= 3).show()

#Sub 3 - ¿Qué área ha facturado menos dinero?

print("-"*40+" Ejercicio 7.3 en Data Frame "+"-"*40)

# En este caso se agrupa por áreas y se aplica la suma de precios. Se ordena de manera ascendente
# y se pide mostrar solamente el primer registro de ese df ordenado.

purchases_with_areas.groupby("Areas").sum("price").orderBy("sum(price)").show(1)

print("-"*40+" Ejercicio 7 en SQL "+"-"*40)

#Sub 1 - ¿En qué área se utiliza más PayPal?

print("-"*40+" Ejercicio 7.1 en SQL "+"-"*40)

# La query en si es bastante simple, donde hacemos un recuento para obtener la cantidad de compras
# y las agrupamos por área. Por otro lado, se hizo una subquery para generar una nueva columna
# Que determine a que área deberá ir cada registro dependiendo de su longitud.
# Por último, se ordena por la cantidad de compras de manera descendente y se aplica un show(1)
# para obtener el top 1.

sc.sql("SELECT\
        AREAS,\
        COUNT(*) AS QUANTITY_OF_PURCHASES \
        FROM (\
            SELECT\
            PAYMENT_TYPE,\
            CASE WHEN LOCATION.LON < -108 THEN 'AREA 1'\
                  WHEN -108 <= LOCATION.LON AND LOCATION.LON < -36 THEN 'AREA 2'\
                  WHEN -36 <= LOCATION.LON AND LOCATION.LON < 36 THEN 'AREA 3'\
                  WHEN 36 <= LOCATION.LON AND LOCATION.LON < 108 THEN 'AREA 4'\
                  ELSE 'AREA 5' END AS AREAS\
            FROM PURCHASES WHERE PAYMENT_TYPE = 'paypal')\
        GROUP BY AREAS\
        ORDER BY QUANTITY_OF_PURCHASES DESC").show(1)

#Sub 2 - ¿Cuáles son los 3 productos más comprados en cada área?

print("-"*40+" Ejercicio 7.2 en SQL "+"-"*40)

# En la query, solamente se extraen los datos obtenidos de sus subqueries y se aplica la cláusula where <= 3
# Las subqueries generan por un lado la columna áreas (la query mas anidada se encarga de esto). La siguiente
# Se encarga de generar el recuento de id de producto para obtener las cantidades vendidas de tipo de producto
# por área. La siguiente subquery hace una partición por áreas y ordenado por el recuento de productos para poder
# obtener el ranking por área.

sc.sql("SELECT\
        *\
        FROM \
            (SELECT\
            AREAS, \
            PRODUCT_ID,\
            ROW_NUMBER() OVER(PARTITION BY AREAS ORDER BY QUANTITY_SOLD DESC) AS RANKING\
            FROM (\
                    SELECT\
                    AREAS,\
                    PRODUCT_ID,\
                    COUNT(PRODUCT_ID) AS QUANTITY_SOLD\
                    FROM (\
                            SELECT\
                            PRODUCT_ID,\
                            CASE WHEN LOCATION.LON < -108 THEN 'AREA 1'\
                            WHEN -108 <= LOCATION.LON AND LOCATION.LON < -36 THEN 'AREA 2'\
                            WHEN -36 <= LOCATION.LON AND LOCATION.LON < 36 THEN 'AREA 3'\
                            WHEN 36 <= LOCATION.LON AND LOCATION.LON < 108 THEN 'AREA 4'\
                            ELSE 'AREA 5' END AS AREAS\
                            FROM PURCHASES)\
                    GROUP BY AREAS, PRODUCT_ID)\
            )\
        WHERE RANKING <= 3").show()

#Sub 3 - ¿Qué área ha facturado menos dinero?

print("-"*40+" Ejercicio 7.3 en SQL "+"-"*40)

# En la subquery se generan las áreas según la longitud de cada tienda. Luego, en la query se agrupa por áreas
# y se realiza la suma de precios para obtener la facturación por tienda. Por último se ordena por esa suma, de
# manera ascendente para obtener como primer registro la facturación mas baja. Se aplica un show(1) para obtener
# dicho registro.

sc.sql("SELECT\
        AREAS,\
        SUM(PRICE) AS AMOUNT_OF_PURCHASES \
        FROM (\
            SELECT\
            PAYMENT_TYPE,\
            PRICE,\
             CASE WHEN LOCATION.LON < -108 THEN 'AREA 1'\
                  WHEN -108 <= LOCATION.LON AND LOCATION.LON < -36 THEN 'AREA 2'\
                  WHEN -36 <= LOCATION.LON AND LOCATION.LON < 36 THEN 'AREA 3'\
                  WHEN 36 <= LOCATION.LON AND LOCATION.LON < 108 THEN 'AREA 4'\
                  ELSE 'AREA 5' END AS AREAS\
            FROM PURCHASES)\
        GROUP BY AREAS\
        ORDER BY AMOUNT_OF_PURCHASES ASC").show(1)

#Ejercicio 8 - Indicar los productos que no tienen stock suficiente para las compras realizadas.

print("-"*40+" Ejercicio 8 en Data Frame "+"-"*40)

# En este caso se hace un recuento de cantidad de productos agrupado por product id para posteriormente
# realizar un join con el dataframe de stock, en base a sus claves principales. Se aplica un filtro condicional
# donde nos muestra aquellos registros cuya cantidad de ventas es mayor a la columna de cantidad en stock.

purchases_df.groupby("product_id").count().join(stock_df, purchases_df.product_id == stock_df.product_id, 'left').\
    where(col("count") > stock_df.quantity).show()

print("-"*40+" Ejercicio 8 en SQL "+"-"*40)

# Para este caso se genera una nueva vista temporal de sql con el dataframe de stock.

stock_df.createOrReplaceTempView("Stock")

# Se realiza un join de ambas tablas y aplica una cláusula where donde el recuento de cantidades
# vendidas sustrayendo el stock disponible sea mayor a 0.

sc.sql("SELECT\
        *\
        FROM (\
            SELECT\
            PURCHASES.PRODUCT_ID AS PRODUCT_ID_PURCHASES,\
            COUNT(PURCHASES.PRODUCT_ID) AS COUNT_OF_PURCHASES,\
            STOCK.PRODUCT_ID AS PRODUCT_ID_STOCK,\
            STOCK.QUANTITY AS QUANTITY_STOCK\
            FROM PURCHASES LEFT JOIN STOCK ON PURCHASES.PRODUCT_ID = STOCK.PRODUCT_ID \
            GROUP BY PURCHASES.PRODUCT_ID, STOCK.PRODUCT_ID, STOCK.QUANTITY)\
        WHERE COUNT_OF_PURCHASES - QUANTITY_STOCK > 0").show()