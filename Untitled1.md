## Carlos Sanchez Vega

### Prueba de código

Se ha optado por elegir pyspark para la resolución de la parte de código, por la eficiencia que presenta spark en el procesamiento masivo de datos

### La tarea

Nuestro cliente necesita recibir en un endpoint información sobre las visitas a las páginas de sus productos. El json con la información tendrá los siguientes campos:
- user_id
- date
- product_name
- price
- purchased (boolean)

La información recibida por este endpoint debe ser guardada en local de forma persistente y acumulativa, es decir, cada vez que la aplicación se ejecute añadirá información a un fichero.

Para la generación de los ficheros de prueba para la prueba usaremos una de las herramientas que tenemos online (https://extendsclass.com/json-generator.html)

### Resolución

Importamos todas las libreías necesarias


```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, FloatType, BooleanType
from pyspark.sql import Window
import pyspark.sql.functions as F
```

Creamos la sesión de spark


```python
spark = SparkSession.builder.appName("PruebaTheCocktail").getOrCreate()
```

Creamos el esquema de datos 


```python
schema = StructType([ \
                     StructField("user_id", IntegerType(), True), \
                     StructField("date", StringType(), True),\
                     StructField("product_name", StringType(), True), \
                     StructField("price", FloatType(), True), \
                     StructField("purchased", BooleanType(), True)])
```

Se leen los ficheros de datos


```python
ordersDF = spark.read.option("multiline","true").schema(schema).json("myJsonFiles")
ordersDF.printSchema()
ordersDF.show(100)
```

    root
     |-- user_id: integer (nullable = true)
     |-- date: string (nullable = true)
     |-- product_name: string (nullable = true)
     |-- price: float (nullable = true)
     |-- purchased: boolean (nullable = true)
    
    +-------+-------------------+------------+------+---------+
    |user_id|               date|product_name| price|purchased|
    +-------+-------------------+------------+------+---------+
    |     13|2020-10-16 13:34:16|productName1|43.305|     true|
    |     22|2020-09-16 13:34:16|productName1|43.305|     true|
    |     28|2020-08-16 13:34:16|productName1|43.305|     true|
    |     10|2020-11-16 13:34:16|productName1|43.305|     true|
    |     11|2020-11-16 13:34:16|productName1|43.305|     true|
    |     19|2020-09-16 13:34:16|productName1|43.305|     true|
    |     15|2020-10-16 13:34:16|productName1|43.305|     true|
    |     29|2020-08-16 13:34:16|productName1|43.305|     true|
    |     25|2020-08-16 13:34:16|productName1|43.305|     true|
    |     21|2020-09-16 13:34:16|productName1|43.305|     true|
    |     31|2020-08-16 13:34:16|productName1|43.305|     true|
    |     23|2020-09-16 13:34:16|productName1|43.305|     true|
    |     20|2020-09-16 13:34:16|productName1|43.305|     true|
    |     16|2020-10-16 13:34:16|productName1|43.305|     true|
    |     14|2020-10-16 13:34:16|productName1|43.305|     true|
    |     26|2020-08-16 13:34:16|productName1|43.305|     true|
    |     12|2020-10-16 13:34:16|productName1|43.305|     true|
    |     30|2020-08-16 13:34:16|productName1|43.305|     true|
    |     24|2020-09-16 13:34:16|productName1|43.305|     true|
    |     27|2020-08-16 13:34:16|productName1|43.305|     true|
    |     18|2020-09-16 13:34:16|productName1|43.305|     true|
    |     17|2020-09-16 13:34:16|productName1|43.305|     true|
    |      8|2020-11-16 13:34:16|productName1|43.305|     true|
    |      7|2020-11-16 13:34:16|productName1|43.305|     true|
    |      1|2020-12-16 13:34:16|productName1|43.305|     true|
    |      2|2020-12-16 13:34:16|productName1|43.305|     true|
    |      9|2020-11-16 13:34:16|productName1|43.305|     true|
    |      3|2020-12-16 13:34:16|productName1|43.305|     true|
    |      4|2020-12-16 13:34:16|productName1|43.305|     true|
    |      6|2020-11-16 13:34:16|productName1|43.305|     true|
    |      5|2020-12-16 13:34:16|productName1|43.305|     true|
    +-------+-------------------+------------+------+---------+
    


### 1. Queremos calcular de un mes a otro cuanto ha variado el tráfico por producto (durante un año, por ejemplo).

El primer punto de la resuloción del problema consistirá en contar el número de veces que se compra cada elemento , agrupado por mes (que es la columna "Date" es pa parte del String que hay desde el primer caracter hasta el 7)


```python
ordersDF = ordersDF.withColumn('year_month', F.col('Date')[0:7])
ordersDF1 = ordersDF.groupBy("year_month","product_name").agg(func.count("product_name").alias("times_purchased")).sort("year_month","product_name",ascending=False)
ordersDF1.show()
```

    +----------+------------+---------------+
    |year_month|product_name|times_purchased|
    +----------+------------+---------------+
    |   2020-12|productName1|              5|
    |   2020-11|productName1|              6|
    |   2020-10|productName1|              5|
    |   2020-09|productName1|              8|
    |   2020-08|productName1|              7|
    +----------+------------+---------------+
    


Teniendo el dataframe anterior, podríamos calcular la difencia de veces que se ha comprado un elemento respecto al mes anterior

Para calcular dicha diferencia, usaremos una función de "ventana", que agrupará elementos por producto:


```python
ordersDF1 = ordersDF1.withColumn('year', F.col('year_month')[0:4])
ordersDF1.show()
w1= Window.partitionBy("year").orderBy("year_month")

ordersDF1 = ordersDF1.withColumn("times_purchased_prev", F.lag(ordersDF1.times_purchased).over(w1))
ordersDF1 = ordersDF1.withColumn("diff", F.when(F.isnull(ordersDF1.times_purchased - ordersDF1.times_purchased_prev), 0)
                              .otherwise(ordersDF1.times_purchased - ordersDF1.times_purchased_prev)).orderBy("year_month", "product_name", ascending = False)
ordersDF1.show(100)

```

    +----------+------------+---------------+----+
    |year_month|product_name|times_purchased|year|
    +----------+------------+---------------+----+
    |   2020-12|productName1|              5|2020|
    |   2020-11|productName1|              6|2020|
    |   2020-10|productName1|              5|2020|
    |   2020-09|productName1|              8|2020|
    |   2020-08|productName1|              7|2020|
    +----------+------------+---------------+----+
    
    +----------+------------+---------------+----+--------------------+----+
    |year_month|product_name|times_purchased|year|times_purchased_prev|diff|
    +----------+------------+---------------+----+--------------------+----+
    |   2020-12|productName1|              5|2020|                   6|  -1|
    |   2020-11|productName1|              6|2020|                   5|   1|
    |   2020-10|productName1|              5|2020|                   8|  -3|
    |   2020-09|productName1|              8|2020|                   7|   1|
    |   2020-08|productName1|              7|2020|                null|   0|
    +----------+------------+---------------+----+--------------------+----+
    


### 2. Queremos calcular la media de venta mensual de todo un año.

Agruparemos los datos por año y, una vez hecho, calcularemos la media del importe de lo percibido "price"


```python
ordersDF2 = ordersDF.groupBy("year_month",)\
                .agg(func.round(func.avg("price"),2).alias("average")).\
                 sort("year_month",ascending=False)
ordersDF2.show()
```

    +----------+-------+
    |year_month|average|
    +----------+-------+
    |   2020-12|  43.31|
    |   2020-11|  43.31|
    |   2020-10|  43.31|
    |   2020-09|  43.31|
    |   2020-08|  43.31|
    +----------+-------+
    


### 3. Queremos obtener todas las compras de un usuario.

Usaremos, por ejemplo, el usuario con id = 13


```python
ordersDF3 = ordersDF.filter(ordersDF.user_id==13).select("product_name").distinct()
ordersDF3.show()

```

    +------------+
    |product_name|
    +------------+
    |productName1|
    +------------+
    


Por último, pararemos la sesión


```python
spark.stop()
```

### ¿Existe una relación entre las variables y si el usuario termina comprando? Propón una forma de encontrar la relación entre ellas.

Podríamos estudiar patrones de comportamiento de los usuarios mediante series temporales, porque puede que los productos se compran más en una época u otra del año. Las series temporales se usan para estudiar la relación causal entre diversas variables que cambian con el tiempo y se influyen entre sí.
