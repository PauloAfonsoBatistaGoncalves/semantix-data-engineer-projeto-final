import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProjetoScala {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]")
      .appName("Projeto Final").enableHiveSupport().getOrCreate()

    val columnList = Array(
      StructField("regiao", StringType),
      StructField("estado",StringType),
      StructField("municipio",StringType),
      StructField("coduf",IntegerType),
      StructField("codmun",IntegerType),
      StructField("codRegiaoSaude",IntegerType),
      StructField("nomeRegiaoSaude",StringType),
      StructField("data",StringType),
      StructField("semanaEpi",IntegerType),
      StructField("populacaoTCU2019",IntegerType),
      StructField("casosAcumulado",IntegerType),
      StructField("casosNovos",IntegerType),
      StructField("obitosAcumulado",IntegerType),
      StructField("obitosNovos",IntegerType),
      StructField("Recuperadosnovos",IntegerType),
      StructField("emAcompanhamentoNovos",IntegerType),
      StructField("interior/metropolitana",IntegerType)
    )

    val schema = StructType(columnList)

    val csvFiles = spark.read
      .option("delimiter", ";")
      .option("header", "true").schema(schema).csv("/user/paulo/projeto-final/data/csv-files")

    csvFiles.show(5)

    print("selecionando banco")

    spark.sql("show databases").show()

    spark.sql("use projeto_final").show()

    spark.sql("show tables").show()

    spark.sql("drop table if exists painel_covid_2")
    spark.sql("drop table if exists casos_recuperados_2")
    spark.sql("drop table if exists casos_recuperados_2")

    /*Salvando dados tabela hive*/
    csvFiles.write.partitionBy("municipio").saveAsTable("projeto_final.painel_covid_2")

    var painel_covid = spark.read.table("projeto_final.painel_covid")
    painel_covid = painel_covid
      .withColumn("data_timestamp", unix_timestamp(col("data"), "yyyy-MM-dd"))


    /*Primeira visualização*/

     val casos_recuperados_e_acompanhamento_view = painel_covid
      .filter(painel_covid("regiao")==="Brasil")
      .groupBy("regiao")
      .agg(
        max("recuperadosnovos").alias("RecuperadosNovos"),
        max("emAcompanhamentoNovos").alias("EmAcompanhamento")
      )

    casos_recuperados_e_acompanhamento_view.write.saveAsTable("projeto_final.casos_recuperados_2")
    casos_recuperados_e_acompanhamento_view.show()

    /*Segunda visualização*/

    var casos_novos = painel_covid.select("casosNovos")
      .filter("regiao='Brasil'")
      .sort(col("data_timestamp").desc).collect()

    var casos_confirmados_view = painel_covid.filter("regiao='Brasil'")
      .groupBy("regiao")
      .agg(max("casosacumulado").alias("CasosConfirmados"))
      .withColumn("CasosNovos", lit(casos_novos(0).get(0)))

    casos_confirmados_view.write.format("parquet")
      .option("compression","snappy")
      .saveAsTable("projeto_final.casos_confirmados_2")

    casos_confirmados_view.show()

    /*Terceira visualização*/
    casos_novos = painel_covid.select("obitosNovos")
      .filter("regiao='Brasil'")
      .sort(col("data_timestamp").desc).collect()


    var obitos_acumulados_view = painel_covid.filter("regiao='Brasil'")
      .groupBy("regiao")
      .agg(max("obitosAcumulado").alias("Óbitos Acumulados"))
      .withColumn("Casos Novos", lit(casos_novos(0).get(0)))

    obitos_acumulados_view.show()

    obitos_acumulados_view = painel_covid.filter("regiao!='Brasil'")
      .groupBy("data_timestamp")
      .agg(sum("obitosNovos").alias("obitos"))
      .withColumn("data_notificacao", from_unixtime(col("data_timestamp"), "dd-MM-yyyy"))
      .sort(desc("data_timestamp"))

    obitos_acumulados_view.select("data_notificacao","obitos").show(10)

    var obitos_acumulados_view_stream = obitos_acumulados_view
    .withColumn("value", concat(col("data_notificacao").cast(StringType), lit(','), col("obitos").cast(StringType))).write
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "obitosConfirmados")
    .option("checkpointLocation","/user/paulo/kafka_checkpoint")

    obitos_acumulados_view_stream.save()

    /*Visuaçização de sítese de casos, óbitos e mortalidade*/

    var sintexe_view = painel_covid.filter("regiao != 'null'")
      .groupBy("regiao")
      .agg(
        max("casosAcumulado").alias("obitosAcumulado"),
        max("obitosAcumulado").alias("obitosAcumulado")
      )

    sintexe_view.show()


    spark.stop()
  }

  def preparandoAmbiente(spark: SparkSession): Unit ={
    print("selecionando banco")

    spark.sql("show databases").show()

    spark.sql("use projeto_final").show()

    spark.sql("show tables").show()

    spark.sql("drop table if exists painel_covid_2")
    spark.sql("drop table if exists casos_recuperados_2")
    spark.sql("drop table if exists casos_recuperados_2")
  }

  def leituraCsv(spark: SparkSession): Unit ={
    val columnList = Array(
      StructField("regiao", StringType),
      StructField("estado",StringType),
      StructField("municipio",StringType),
      StructField("coduf",IntegerType),
      StructField("codmun",IntegerType),
      StructField("codRegiaoSaude",IntegerType),
      StructField("nomeRegiaoSaude",StringType),
      StructField("data",StringType),
      StructField("semanaEpi",IntegerType),
      StructField("populacaoTCU2019",IntegerType),
      StructField("casosAcumulado",IntegerType),
      StructField("casosNovos",IntegerType),
      StructField("obitosAcumulado",IntegerType),
      StructField("obitosNovos",IntegerType),
      StructField("Recuperadosnovos",IntegerType),
      StructField("emAcompanhamentoNovos",IntegerType),
      StructField("interior/metropolitana",IntegerType)
    )

    val schema = StructType(columnList)

    val csvFiles = spark.read
      .option("delimiter", ";")
      .option("header", "true").schema(schema).csv("/user/paulo/projeto-final/data/csv-files")

    csvFiles.show(5)
  }

  def primeiraVisualizacao(spark: SparkSession, painel_covid: DataFrame): Unit ={
    val casos_recuperados_e_acompanhamento_view = painel_covid
      .filter(painel_covid("regiao")==="Brasil")
      .groupBy("regiao")
      .agg(
        max("recuperadosnovos").alias("RecuperadosNovos"),
        max("emAcompanhamentoNovos").alias("EmAcompanhamento")
      )

    casos_recuperados_e_acompanhamento_view.write.saveAsTable("projeto_final.casos_recuperados_2")
    casos_recuperados_e_acompanhamento_view.show()
  }

  def segundaVisualizacao(spark: SparkSession, painel_covid: DataFrame): Unit ={
    var casos_novos = painel_covid.select("casosNovos")
      .filter("regiao='Brasil'")
      .sort(col("data_timestamp").desc).collect()

    var casos_confirmados_view = painel_covid.filter("regiao='Brasil'")
      .groupBy("regiao")
      .agg(max("casosacumulado").alias("CasosConfirmados"))
      .withColumn("CasosNovos", lit(casos_novos(0).get(0)))

    casos_confirmados_view.show(5)
  }

  def terceiraVisualizacao(spark: SparkSession, painel_covid: DataFrame): Unit ={
    var casos_novos = painel_covid.select("obitosNovos")
      .filter("regiao='Brasil'")
      .sort(col("data_timestamp").desc).collect()


    var obitos_acumulados_view = painel_covid.filter("regiao='Brasil'")
      .groupBy("regiao")
      .agg(max("obitosAcumulado").alias("Óbitos Acumulados"))
      .withColumn("Casos Novos", lit(casos_novos(0).get(0)))

    obitos_acumulados_view.show()

    obitos_acumulados_view = painel_covid.filter("regiao!='Brasil'")
      .groupBy("data_timestamp")
      .agg(sum("obitosNovos").alias("obitos"))
      .withColumn("data_notificacao", from_unixtime(col("data_timestamp"), "dd-MM-yyyy"))
      .sort(desc("data_timestamp"))

    obitos_acumulados_view.select("data_notificacao","obitos").show(10)

  }

  def salvandoPrimeiraVisualizacao(casos_recuperados_e_acompanhamento_view: DataFrame): Unit ={
    casos_recuperados_e_acompanhamento_view.write.saveAsTable("projeto_final.casos_recuperados_2")
  }

  def salvandoSegundaVisualizacao(casos_recuperados_e_acompanhamento_view: DataFrame): Unit ={
    casos_recuperados_e_acompanhamento_view.write.saveAsTable("projeto_final.casos_recuperados_2")
  }

  def salvandoTerceiraVisualizacao(obitos_acumulados_view: DataFrame): Unit ={
    var obitos_acumulados_view_stream = obitos_acumulados_view
      .withColumn("value", concat(col("data_notificacao").cast(StringType), lit(','), col("obitos").cast(StringType))).write
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("topic", "obitosConfirmados")
      .option("checkpointLocation","/user/paulo/kafka_checkpoint")

    obitos_acumulados_view_stream.save()
  }

  def visualziacaoPainelSintese(painel_covid:DataFrame): Unit ={
    var sintexe_view = painel_covid.filter("regiao != 'null'")
      .groupBy("regiao")
      .agg(
        max("casosAcumulado").alias("obitosAcumulado"),
        max("obitosAcumulado").alias("obitosAcumulado")
      )

    sintexe_view.show()
  }
}