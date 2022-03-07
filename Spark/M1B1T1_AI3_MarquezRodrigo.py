from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg, col

# Crear sesion

sc = SparkSession.builder.appName("Entregable_Individual").getOrCreate()

natality_data_frame = sc.read. \
    option("header", "true"). \
    option("inferSchema", "true"). \
    csv(r"C:\Users\Rodri\Downloads\natality.csv")

print(natality_data_frame.describe())

# Pregunta 1
# Obtén en qué 10 estados nacieron más niños y niñas en 2003.

print("-" * 40 + " pregunta 1 en dataframe " + "-" * 40)

# Se selecciona el df creado, aplicando select por la columna que deseo obtener y filtrando por el año
# Posteriormente, se agrupa la información por estado y se recuentan las entradas por estado

state_natality = natality_data_frame. \
                                    select(natality_data_frame.state). \
                                    filter(natality_data_frame.year == 2003). \
                                    groupBy(natality_data_frame.state). \
                                    count().show()


print("-" * 40 + " pregunta 1 en SQL " + "-" * 40)

# Se genera la vista temporal para luego insertar la query y efectuar las consultas

natality_data_frame.createOrReplaceTempView("natality")

sc.sql("SELECT\
        STATE,\
        COUNT (*) AS COUNTED_ENTRIES\
        FROM natality\
        WHERE YEAR = 2003 \
        GROUP BY STATE").show()

# Pregunta 2
# Obtén la media de peso de los niños y niñas por año y estado.

#Pregunta 2 en Data frame

print("-" * 40 + " pregunta 2 en dataframe " + "-" * 40)

natality_data_frame. \
                    groupby(natality_data_frame.year, natality_data_frame.state). \
                    mean("weight_pounds").show()

# Pregunta 2 en SQL

print("-" * 40 + " pregunta 2 en SQL " + "-" * 40)

sc.sql("SELECT\
        YEAR,\
        STATE,\
        AVG(WEIGHT_POUNDS) AS MEAN_WEIGHT\
        FROM NATALITY\
        GROUP BY YEAR, STATE").show()

# Pregunta 3
# Evolución por año y por mes del número de niños y niñas nacidas (Resultado por separado con una sola consulta).

#Pregunta 3 en data frame

print("-" * 40 + " pregunta 3 en dataframe " + "-" * 40)

#Creamos una DF temporal para este ejercicio donde se generan dos columnas, Male y Female
#Aplicando la condicion de si is_male es true/false y asignando valores para su suma posterior en el
#Group by

ejercicio3_df = natality_data_frame.\
                    withColumn("Male", \
                                when(natality_data_frame.is_male == True, 1).\
                                otherwise(0)).\
                    withColumn("Female", when(natality_data_frame.is_male == False, 1).\
                               otherwise(0))

ejercicio3_df.groupby("year","month").sum("Male","Female").orderBy(["year","month"], ascending= [1, 1]).show()

print("-" * 40 + " pregunta 3 en SQL " + "-" * 40)

#Consulta donde se incluyen dos case when para sumar los valores y genero dos columnas separadas
#por el sexo del recién nacido

sc.sql("SELECT\
        YEAR,\
        MONTH,\
        SUM(CASE WHEN IS_MALE = 1 THEN 1 ELSE 0 END) AS MALE,\
        SUM(CASE WHEN IS_MALE = 0 THEN 1 ELSE 0 END) AS FEMALE\
        FROM NATALITY\
        GROUP BY YEAR, MONTH\
        ORDER BY YEAR ASC, MONTH ASC").show()

#Pregunta  4
# Obtén los tres meses de 2005 en que nacieron más niños y niñas.

#Pregunta 4 en dataframe

print("-" * 40 + " pregunta 4 en dataframe " + "-" * 40)

# Seleccionar la columna de mes y aplicar la condición de que el año sea 2005
# Posteriormente agrupar por mes y contar las entradas
# Por último, ordenar por el recuento de manera descendente y mostrar las primeras 3 entradas


natality_data_frame.\
                    select("month").\
                    filter(natality_data_frame.year == 2005).\
                    groupby("month").\
                    count().\
                    orderBy("count", ascending= 0).\
                    show(3)


print("-" * 40 + " pregunta 4 en SQL " + "-" * 40)

#Se aplica la query con el conteo de niños nacidos y aplicando la condición de año 2005
#Luego se ordena y se le aplica el .show a la consulta para mostrar el top 3

sc.sql("SELECT\
        MONTH,\
        COUNT(MONTH) AS BORN_CHILDS\
        FROM NATALITY\
        WHERE YEAR == 2005\
        GROUP BY MONTH\
        ORDER BY COUNT(MONTH) DESC").show(3)

# Pregunta 5
# Obtén los estados donde las semanas de gestación son superiores a la media de EE. UU.

# Pregunta 5 en dataframe

print("-" * 40 + " pregunta 5 en dataframe " + "-" * 40)

# En este caso se debe calcular, por un lado la media total de la columna gestation_weeks y
# por el otro, la media agrupada de gestation_weeks por estado.
# Para esto, primero se agrupa la información y se utiliza un agg que genere los cálculos agrupados por estado
# Luego, dentro del .filter se llama al alias del promedio calculado por estado y se lo compara con el .agg total
# de la columna de gestation weeks (que sería el promedio total) llamando al item [0][0] de ese cálculo.

natality_data_frame.\
    groupBy("state").\
        agg(avg("gestation_weeks").alias("Mean_gestation")).\
        filter(col("Mean_gestation") > (natality_data_frame.\
                                                            agg({'gestation_weeks': 'mean'}).\
                                                            collect()[0][0])).\
    show()


print("-" * 40 + " pregunta 5 en SQL " + "-" * 40)

# En este caso se genera una subquery donde se obtienen los datos de promedio de gestación por estado
# Posteriormente, se utiliza el nuevo campo calculado en el where para comprarlo contra el promedio total de la base.


sc.sql("SELECT\
       *\
       FROM (\
                SELECT\
                STATE,\
                AVG(GESTATION_WEEKS) AS MEAN_GESTATION_WEEKS\
                FROM NATALITY\
                GROUP BY STATE)\
       WHERE MEAN_GESTATION_WEEKS > (SELECT AVG(GESTATION_WEEKS) FROM NATALITY)").show()


# Pregunta 6
# Obtén los cinco estados donde la media de edad de las madres ha sido mayor.

# Pregunta 6 en data frame

print("-" * 40 + " pregunta 6 en Dataframe " + "-" * 40)

# Este ejercicio es similar al anterior, solo que mostrando el top 5 de estados con promedio de edad de madres mayor
# Para este ejercicio tomé el supuesto de que se pedía el top 5, ya que la consigna solicita
# "Obtén los cinco estados donde la media de edad de las madres ha sido mayor."

natality_data_frame.groupby("state").mean("mother_age").orderBy("avg(mother_age)", ascending= 0).show(5)

print("-" * 40 + " pregunta 6 en SQL " + "-" * 40)

#En SQL

sc.sql("SELECT\
       STATE,\
       AVG(MOTHER_AGE) AS MEAN_MOTHERS_AGE\
       FROM NATALITY\
       GROUP BY STATE\
       ORDER BY MEAN_MOTHERS_AGE DESC").show(5)

#Pregunta 7
# Indica cómo influye en el peso del bebé y las semanas de gestación que la madre haya tenido
# un parto múltiple (campo plurality) a las que no lo han tenido.

print("-" * 40 + " pregunta 7 en Dataframe " + "-" * 40)

# En este caso solamente tomé los promedios de ambos campos (gestación y peso en libras). A partir de eso,
# se puede suponer que, a mayor pluralidad, menores son el promedio de peso y semanas de gestación de los nacidos.
# Para confirmar esto se debería hacer un análisis mas exhaustivo donde también se analicen casos como el de pluralidad
# 4, el cual es un caso que 'rompe' la tendencia.

natality_data_frame.groupby("plurality").\
                                            agg(avg("gestation_weeks"), avg("weight_pounds")).\
                                            orderBy("plurality", ascending= 1).show()


print("-" * 40 + " pregunta 7 en SQL " + "-" * 40)

sc.sql("SELECT\
        PLURALITY,\
        AVG(GESTATION_WEEKS) AS MEAN_GESTATION_WEEKS,\
        AVG(WEIGHT_POUNDS) AS MEAN_WEIGHT_POUNDS\
        FROM NATALITY\
        GROUP BY PLURALITY\
        ORDER BY PLURALITY ASC").show()