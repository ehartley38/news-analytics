

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

