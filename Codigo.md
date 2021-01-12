## Carlos Sanchez Vega

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
ordersDF.orderBy("user_id").show(100)
```

    root
     |-- user_id: integer (nullable = true)
     |-- date: string (nullable = true)
     |-- product_name: string (nullable = true)
     |-- price: float (nullable = true)
     |-- purchased: boolean (nullable = true)
    
    +-------+-------------------+------------+-----+---------+
    |user_id|               date|product_name|price|purchased|
    +-------+-------------------+------------+-----+---------+
    |      1|2020-12-16 13:34:16|productName1|100.0|     true|
    |      2|2020-12-16 13:34:16|productName1|100.0|     true|
    |      3|2020-12-16 13:34:16|productName1|100.0|     true|
    |      4|2020-12-16 13:34:16|productName1|100.0|     true|
    |      5|2020-12-16 13:34:16|productName1|100.0|     true|
    |      6|2020-11-16 13:34:16|productName1|100.0|     true|
    |      7|2020-11-16 13:34:16|productName1|100.0|     true|
    |      8|2020-11-16 13:34:16|productName1|100.0|     true|
    |      9|2020-11-16 13:34:16|productName1|100.0|     true|
    |     10|2020-11-16 13:34:16|productName1|100.0|     true|
    |     11|2020-11-16 13:34:16|productName1|100.0|     true|
    |     12|2020-10-16 13:34:16|productName1|100.0|     true|
    |     13|2020-10-16 13:34:16|productName1|100.0|     true|
    |     14|2020-10-16 13:34:16|productName1|100.0|     true|
    |     15|2020-10-16 13:34:16|productName1|100.0|     true|
    |     16|2020-10-16 13:34:16|productName1|100.0|     true|
    |     17|2020-09-16 13:34:16|productName1|100.0|     true|
    |     18|2020-09-16 13:34:16|productName1|100.0|     true|
    |     19|2020-09-16 13:34:16|productName1|100.0|     true|
    |     20|2020-09-16 13:34:16|productName1|100.0|     true|
    |     21|2020-09-16 13:34:16|productName1|100.0|     true|
    |     22|2020-09-16 13:34:16|productName1|100.0|     true|
    |     23|2020-09-16 13:34:16|productName1|100.0|     true|
    |     24|2020-09-16 13:34:16|productName1|100.0|     true|
    |     25|2020-08-16 13:34:16|productName1|100.0|     true|
    |     26|2020-08-16 13:34:16|productName1|100.0|     true|
    |     27|2020-08-16 13:34:16|productName1|100.0|     true|
    |     28|2020-08-16 13:34:16|productName1|100.0|     true|
    |     29|2020-08-16 13:34:16|productName1|100.0|     true|
    |     30|2020-08-16 13:34:16|productName1|100.0|     true|
    |     31|2020-09-16 13:40:16|productName2| 50.0|     true|
    |     31|2020-08-16 13:34:16|productName2| 50.0|     true|
    |     31|2020-08-16 13:40:16|productName2| 50.0|     true|
    |     31|2020-08-16 13:34:16|productName1|100.0|     true|
    |     34|2020-08-16 13:40:11|productName2| 50.0|     true|
    |     35|2020-08-16 13:40:16|productName2| 50.0|     true|
    |     35|2020-08-16 14:40:16|productName2| 50.0|     true|
    |     35|2020-09-16 13:40:16|productName2| 50.0|     true|
    |     37|2020-09-16 13:34:16|productName2| 50.0|    false|
    |     39|2020-09-16 13:40:16|productName2| 50.0|     true|
    |     40|2020-09-16 13:40:11|productName2| 50.0|     true|
    |     41|2020-09-16 14:40:16|productName2| 50.0|     true|
    |     43|2020-10-16 13:34:16|productName2| 50.0|    false|
    |     44|2020-10-16 13:40:16|productName2| 50.0|     true|
    |     45|2020-10-16 13:40:16|productName2| 50.0|     true|
    |     46|2020-10-16 13:40:11|productName2| 50.0|     true|
    |     47|2020-10-16 14:40:16|productName2| 50.0|     true|
    |     48|2020-11-16 13:34:16|productName2| 50.0|    false|
    |     49|2020-11-16 14:40:16|productName2| 50.0|     true|
    |     50|2020-11-16 14:40:16|productName2| 50.0|     true|
    |     51|2020-11-16 13:40:11|productName2| 50.0|     true|
    |     52|2020-11-16 13:40:11|productName2| 50.0|     true|
    |     53|2020-11-16 13:40:16|productName2| 50.0|     true|
    |     54|2020-11-16 13:40:16|productName2| 50.0|     true|
    |     55|2020-11-16 13:40:16|productName2| 50.0|     true|
    |     56|2020-12-16 13:34:16|productName2| 50.0|    false|
    |     57|2020-12-16 13:40:11|productName2| 50.0|     true|
    |     57|2020-12-16 13:40:11|productName2| 50.0|     true|
    |     58|2020-12-16 14:40:16|productName2| 50.0|     true|
    |     59|2020-12-16 13:40:11|productName2| 50.0|     true|
    |     60|2020-12-16 14:40:16|productName2| 50.0|     true|
    |     61|2020-12-16 13:34:16|productName2| 50.0|    false|
    +-------+-------------------+------------+-----+---------+
    


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
    |   2020-12|productName2|              7|
    |   2020-12|productName1|              5|
    |   2020-11|productName2|              8|
    |   2020-11|productName1|              6|
    |   2020-10|productName2|              5|
    |   2020-10|productName1|              5|
    |   2020-09|productName2|              6|
    |   2020-09|productName1|              8|
    |   2020-08|productName2|              5|
    |   2020-08|productName1|              7|
    +----------+------------+---------------+
    


Teniendo el dataframe anterior, podríamos calcular la difencia de veces que se ha comprado un elemento respecto al mes anterior


```python
ordersDF1 = ordersDF1.withColumn('year', F.col('year_month')[0:4])
ordersDF1.show()
```

    +----------+------------+---------------+----+
    |year_month|product_name|times_purchased|year|
    +----------+------------+---------------+----+
    |   2020-12|productName2|              7|2020|
    |   2020-12|productName1|              5|2020|
    |   2020-11|productName2|              8|2020|
    |   2020-11|productName1|              6|2020|
    |   2020-10|productName2|              5|2020|
    |   2020-10|productName1|              5|2020|
    |   2020-09|productName2|              6|2020|
    |   2020-09|productName1|              8|2020|
    |   2020-08|productName2|              5|2020|
    |   2020-08|productName1|              7|2020|
    +----------+------------+---------------+----+
    


Para calcular dicha diferencia, usaremos una función de "ventana", que agrupará elementos por producto. :


```python
w1= Window.partitionBy("year", "product_name").orderBy( "product_name", "year_month",)

ordersDF1 = ordersDF1.withColumn("times_purchased_prev", F.lag(ordersDF1.times_purchased).over(w1))
ordersDF1 = ordersDF1.withColumn("diff", ordersDF1.times_purchased - ordersDF1.times_purchased_prev).orderBy("year_month", "product_name", ascending = False)

ordersDF1.select("year_month","product_name","times_purchased","times_purchased_prev","diff").show(100)
```

    +----------+------------+---------------+--------------------+----+
    |year_month|product_name|times_purchased|times_purchased_prev|diff|
    +----------+------------+---------------+--------------------+----+
    |   2020-12|productName2|              7|                   8|  -1|
    |   2020-12|productName1|              5|                   6|  -1|
    |   2020-11|productName2|              8|                   5|   3|
    |   2020-11|productName1|              6|                   5|   1|
    |   2020-10|productName2|              5|                   6|  -1|
    |   2020-10|productName1|              5|                   8|  -3|
    |   2020-09|productName2|              6|                   5|   1|
    |   2020-09|productName1|              8|                   7|   1|
    |   2020-08|productName2|              5|                null|null|
    |   2020-08|productName1|              7|                null|null|
    +----------+------------+---------------+--------------------+----+
    


Si miraramos la columna "diff" podríamos ver los productos de los cuales han disminuido las ventas respecto al mes anterior. 

Si el resultado es negativo, podríamos decir que ha habido una disminución en la venta de dicho producto. Sin embargo, si el resultado fuera positivo, podríamos decir que ha habido un incremento en la venta.


### 2. Queremos calcular la media de venta mensual de todo un año.

Agruparemos los datos por año y, una vez hecho, calcularemos la media del importe de lo percibido "price"


```python
ordersDF2 = ordersDF.filter(F.col("purchased")==True).\
                     groupBy("year_month",).\
                     agg(func.round(func.avg("price"),2).\
                     alias("average")).\
                     sort("year_month",ascending=False)
ordersDF2.show()
```

    +----------+-------+
    |year_month|average|
    +----------+-------+
    |   2020-12|   75.0|
    |   2020-11|  73.08|
    |   2020-10|  77.78|
    |   2020-09|  80.77|
    |   2020-08|  79.17|
    +----------+-------+
    


Comprobamos que el resultado es correcto, poniendo un ejemplo:
Por ejemplo seleccionaremos el la fecha 2012-08:
Dicho mes, tiene las siguientes compras:
    


```python
orders_df_2012_08= ordersDF.filter((F.col('year_month')[0:7]== '2020-08') & (F.col("purchased") == True))
orders_df_2012_08.show()
```

    +-------+-------------------+------------+-----+---------+----------+
    |user_id|               date|product_name|price|purchased|year_month|
    +-------+-------------------+------------+-----+---------+----------+
    |     28|2020-08-16 13:34:16|productName1|100.0|     true|   2020-08|
    |     29|2020-08-16 13:34:16|productName1|100.0|     true|   2020-08|
    |     25|2020-08-16 13:34:16|productName1|100.0|     true|   2020-08|
    |     31|2020-08-16 13:34:16|productName1|100.0|     true|   2020-08|
    |     26|2020-08-16 13:34:16|productName1|100.0|     true|   2020-08|
    |     30|2020-08-16 13:34:16|productName1|100.0|     true|   2020-08|
    |     27|2020-08-16 13:34:16|productName1|100.0|     true|   2020-08|
    |     31|2020-08-16 13:40:16|productName2| 50.0|     true|   2020-08|
    |     35|2020-08-16 14:40:16|productName2| 50.0|     true|   2020-08|
    |     31|2020-08-16 13:34:16|productName2| 50.0|     true|   2020-08|
    |     35|2020-08-16 13:40:16|productName2| 50.0|     true|   2020-08|
    |     34|2020-08-16 13:40:11|productName2| 50.0|     true|   2020-08|
    +-------+-------------------+------------+-----+---------+----------+
    


Calculamos la suma de los precios del 2012-08


```python
sum_elements = orders_df_2012_08.select(F.sum(F.col('price'))).collect()[0][0]
```


```python
count_elements = orders_df_2012_08.select("price").count()
```


```python
# calculamos la media para 2012-08
avg = sum_elements/count_elements
print("La media para 2012-08 es", round(avg,2))
```

    La media para 2012-08 es 79.17


Vemos que el resultado coincide con lo mostrado más arriba.

### 3. Queremos obtener todas las compras de un usuario.

En ésta pregunta es necesario filtrar los elementos que han sido comprados. Para ello, añadiremos la condición de que la columna "purchased" sea cierta.

Comprobaremos inicialmente con el usuario con id = 13


```python
ordersDF3 = ordersDF.filter((F.col("user_id") == 13) & (F.col("purchased") == True)).\
                     select("product_name").\
                     distinct().show()
```

    +------------+
    |product_name|
    +------------+
    |productName1|
    +------------+
    


Ahora, sin embargo, si comprobamos las compras del usuario con id=61, vemos que no se nos devuelve ninguna compra porque la fila de la fila correspondiente a su única compra, tiene la columna "purchased" como falsa


```python
ordersDF.filter((F.col("user_id") == 61) & (F.col("purchased") == True)).select("product_name").distinct().show()
```

    +------------+
    |product_name|
    +------------+
    +------------+
    


Por último, pararemos la sesión


```python
spark.stop()
```

### ¿Existe una relación entre las variables y si el usuario termina comprando? 

Podríamos estudiar patrones de comportamiento de los usuarios mediante series temporales, porque puede que los productos se compran más en una época u otra del año. Las series temporales se usan para estudiar la relación causal entre diversas variables que cambian con el tiempo y se influyen entre sí.
