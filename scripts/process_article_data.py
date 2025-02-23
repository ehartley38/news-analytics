import findspark
findspark.init()
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lower, regexp_replace, trim, concat_ws, collect_set, count, lit, to_date
import nltk
from nltk.corpus import stopwords
import sparknlp
from sparknlp.pretrained import PretrainedPipeline
from sparknlp.annotator import LemmatizerModel
from sparknlp.base import DocumentAssembler, Pipeline

def process_article_data(file_name, file_date):
    # Initialise configs and spark
    nltk.download('stopwords')
    stop = stopwords.words('english')


    config = configparser.ConfigParser()
    config.read("/home/ed/.aws/credentials")
    access_key = config["default"]["aws_access_key_id"]
    secret_key = config["default"]["aws_secret_access_key"]

    spark = SparkSession.builder.appName("News-Analytics") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-pom:1.12.365,com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.1") \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config("spark.driver.memory", "8G") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "2000M") \
        .config("spark.jars", "/usr/share/java/postgresql-42.7.5.jar")\
        .getOrCreate()

    s3_path = "s3a://news-analytics-ed/articles-2025-02-07T17-37-56"
    df = spark.read.parquet(s3_path, inferSchema=True)
    df = df.select("content")

    # Remove stop words
    pattern = r"\b(" + "|".join(stop) + r")\b"

    df = df.withColumn("content", regexp_replace(col("content"), pattern, ""))

    # Start Spark NLP session
    sparknlp.start()

    # https://www.johnsnowlabs.com/boost-your-nlp-results-with-spark-nlp-stemming-and-lemmatizing-techniques/
    # Load Pretrained NER and Lemmatizer models
    ner_pipeline = PretrainedPipeline("onto_recognize_entities_sm", lang="en")

    lemmatizer = LemmatizerModel.pretrained().setInputCols(["token"]).setOutputCol("lemma")


    document_assembler = DocumentAssembler() \
        .setInputCol("text") \
        .setOutputCol("document")

    tokenizer = sparknlp.annotator.Tokenizer().setInputCols(["document"]).setOutputCol("token")

    lemmatization_pipeline = Pipeline(stages=[document_assembler, tokenizer, lemmatizer])

    entities_exploded_df = apply_ner_model(df, ner_pipeline)
    lemmatized_df = apply_lemma_model(entities_exploded_df, lemmatization_pipeline, stop)
    
    # Collect unique entity types
    entity_counts_df = lemmatized_df.groupBy("lemmatized_text") \
    .agg(
        count("*").alias("count"),
        collect_set("entity_type").alias("entity_types")  
    ) \
    .orderBy(col("count").desc())

    # Extract the first entity type from the array
    entity_counts_df = entity_counts_df.withColumn(
        "entity_type", 
        col("entity_types").getItem(0)
    )
    entity_counts_df = entity_counts_df.drop("entity_types")

    entity_counts_df = entity_counts_df.withColumnRenamed("lemmatized_text", "name")

    # file_date = datetime.strptime(file_date, "%Y-%m-%d").date()

    entity_counts_df = entity_counts_df.withColumn(
    "date", 
    to_date(lit(file_date), "yyyy-MM-dd")  # Ensure file_date is added as a literal value
    )
    
    insert_to_postgres(entity_counts_df, config)

    spark.stop()



def apply_ner_model(df, ner_pipeline):
    # Apply NER model
    df = df.withColumnRenamed("content", "text")
    entities_df = ner_pipeline.transform(df)

    # https://hyperskill.org/learn/step/21416
    valid_entities = ["PERSON", "NORP", "GPE", "LOC", "PRODUCT", "EVENT", "LAW", "MONEY", "QUANTITY", "ORDINAL", "FAC", "LANGUAGE"]

    # Get individual entity rows
    entities_exploded_df = entities_df.select(
        explode(col("entities")).alias("entity")
    ).filter(
        col("entity.metadata.entity").isin(valid_entities)
    ).select(
            col("entity.result").alias("text"),
        col("entity.metadata.entity").alias("entity_type")
    )

    # Trim and remove special chars
    entities_exploded_df = entities_exploded_df.withColumn(
        "text",
        trim(lower(regexp_replace(col("text"), r"[^\w\s]", ""))) 
    )

    return entities_exploded_df

def apply_lemma_model(entities_exploded_df, lemmatization_pipeline, stop):
    # Apply lemmatization model
    lemmatized_df = lemmatization_pipeline.fit(entities_exploded_df).transform(entities_exploded_df)

    lemmatized_df = lemmatized_df.withColumn("lemmatized_text", concat_ws(" ", col("lemma.result")))

    # Final removal of stop words
    lemmatized_df = lemmatized_df.filter(~col("lemmatized_text").isin(stop))

    return lemmatized_df

def insert_to_postgres(entity_counts_df, config):
    config.read("/home/ed/.postgres/credentials")
    eddy_password = config["default"]["eddy_password"]

    url = "jdbc:postgresql://159.65.49.201:5432/news_analytics"

    properties = {
        "user": "eddy",
        "password": eddy_password,
        "driver": "org.postgresql.Driver"
    }

    entity_counts_df.write.jdbc(url, table="staging_entity_counts", mode="append", properties=properties)

