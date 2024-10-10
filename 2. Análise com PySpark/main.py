from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, weekofyear
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

spark = SparkSession.builder.appName("WebsiteAnalytics").getOrCreate()

# Definindo o schema
schema = StructType([
                        StructField("user_id", IntegerType(), True),           
                        StructField("page_url", StringType(), True),           
                        StructField("session_duration", IntegerType(), True),    
                        StructField("date", DateType(), True)                  
                    ])


def calc_most_visited_pages(df, num):
    top_pages = df.groupBy("page_url").agg(count("*").alias("visit_count")).orderBy(col("visit_count").desc()).limit(num)
    return top_pages

def calc_avg_session_duration(df):
    avg_session_duration = df.agg(avg("session_duration").alias("avg_session_duration"))
    return avg_session_duration

def calc_visits_per_week(df):
    # Adicionando a coluna week pra pegar o numereo da semana do ano
    df_week = df.withColumn("week", weekofyear(col("date")))

    # Contando número de visitas dos usuários por semana
    user_visits_per_week = df_week.groupBy("user_id", "week").agg(count("page_url").alias("visits_per_week"))
    
    # Pegando os usuários que tiveram mais de uma visita por semana
    returning_users = user_visits_per_week.filter(col("visits_per_week") > 1).select("user_id").distinct()

    # Retorna o número de usuários com mais de uma visita por semana
    return returning_users.count()

def main():
    print("Lendo arquivo")
    file_path = "website_logs.csv"
    df = spark.read.csv(file_path, header=True, schema=schema)

    # Mostrar a estrutura do DataFrame (opcional)
    #df.printSchema()

    # Identificando as 10 páginas mais visitadas
    top_pages = calc_most_visited_pages(df, 10)
    print("As 10 páginas mais visitadas são:")
    top_pages.show(truncate=False)

    # Calculando a média de duração das sessões
    avg_session_duration = calc_avg_session_duration(df)
    print("A duração média das sessões é:")
    avg_session_duration.show(truncate=False)
    
    # Calculando quantidade de usuários que visitaram o site mais de uma vez por semana
    returning_users = calc_visits_per_week(df)
    print(f"Número usuários que retornam ao site mais de uma vez por semana é: {returning_users}")

    spark.stop()

if __name__ == "__main__":
    main()