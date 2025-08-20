from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, when
from pyspark.sql.types import TimestampType
import argparse


def transform_data(database: str, table_source: str, table_target: str) -> None:
    spark = (SparkSession.builder.appName('Boston 311 Service Requests Analysis').enableHiveSupport().getOrCreate())
    df = spark.read.table(f'`{database}`.`{table_source}`')
    df = (
        df.withColumn('open_dt', col('open_dt').cast(TimestampType()))
        .withColumn('closed_dt', col('closed_dt').cast(TimestampType()))
        .withColumn('sla_target_dt', col('sla_target_dt').cast(TimestampType()))
    )
    df = df.withColumn(
            'delay_days', 
            when(
                col('closed_dt') > col('sla_target_dt'),
                (unix_timestamp('closed_dt') - unix_timestamp('sla_target_dt')) / 86400, ).otherwise(0),
        )
    columns_to_keep = [
        'case_enquiry_id',
        'open_dt',
        'sla_target_dt',
        'closed_dt',
        'on_time',
        'case_status',
        'closure_reason',
        'case_title',
        'subject',
        'reason',
        'neighborhood',
        'location_street_name',
        'location_zipcode',
        'latitude',
        'longitude',
        'source',
        'delay_days'
    ]
    df_selected = df.select(columns_to_keep)
    df_selected.createOrReplaceTempView('boston_311_data')
    query = '''
        SELECT * FROM boston_311_data
        WHERE case_status = 'Closed' AND delay_days > 0
        ORDER BY delay_days DESC;
    '''
    result_df = spark.sql(query)
    result_df.write.mode('overwrite').format('parquet').saveAsTable(f'`{database}`.`{table_target}`')
    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Trasnformar dados de solicitações de serviço de Bosto 311'

    )
    parser.add_argument('--database', type=str, help='Nome do banco de dados no Glue Data Catalog')
    parser.add_argument('--table_source', type=str, help='Nome da tabela origem no Glue Data Catalog')
    parser.add_argument('--table_target', type=str, help='Nome da tabela dstino no Glue Data Catalog')

    args = parser.parse_args()
    transform_data(args.database, args.table_source, args.table_target)