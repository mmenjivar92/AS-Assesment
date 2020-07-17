import configparser

config = configparser.ConfigParser()
config.read("../dl.cfg")

S3 = config.get('S3b','BUCKET')
ARN = config.get('IAM_ROLE','ARN')

truncate_dim_aisle="TRUNCATE TABLE dim_aisle;"
truncate_dim_department="TRUNCATE TABLE dim_department;"
truncate_dim_product="TRUNCATE TABLE dim_product;"
truncate_dim_user="TRUNCATE TABLE dim_user;"
truncate_fact_order_items="TRUNCATE TABLE fact_order_items;"
truncate_dim_order_product_bride="TRUNCATE TABLE dim_order_product_bridge;"

dim_aisle_copy = ("""  COPY dim_aisle from '{}'
                           iam_role {}
                           FORMAT AS PARQUET
""").format(S3+"/presentation_layer/dim_aisle",ARN)

dim_department_copy = ("""  COPY dim_department from '{}'
                           iam_role {}
                           FORMAT AS PARQUET
""").format(S3+"/presentation_layer/dim_department",ARN)

dim_product_copy = ("""  COPY dim_product from '{}'
                           iam_role {}
                           FORMAT AS PARQUET
""").format(S3+"/presentation_layer/dim_product",ARN)

dim_user_copy = ("""  COPY dim_user from '{}'
                           iam_role {}
                           FORMAT AS PARQUET
""").format(S3+"/presentation_layer/dim_user",ARN)

fact_order_items_copy = ("""  COPY fact_order_items from '{}'
                           iam_role {}
                           FORMAT AS PARQUET
""").format(S3+"/presentation_layer/fact_order_items",ARN)

dim_order_product_bride_copy = ("""  COPY dim_order_product_bridge from '{}'
                           iam_role {}
                           FORMAT AS PARQUET
""").format(S3+"/presentation_layer/dim_order_product_bridge",ARN)

copy_table_queries=[dim_aisle_copy,
              dim_department_copy,
              dim_product_copy,
              dim_user_copy,
              fact_order_items_copy,
              dim_order_product_bride_copy]
truncate_queries=[truncate_dim_aisle,
                  truncate_dim_department,
                  truncate_dim_product,
                  truncate_dim_user,
                  truncate_fact_order_items,
                  truncate_dim_order_product_bride]