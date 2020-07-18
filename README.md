# AS - Technical Assesment
The objective of the project is build a dimensional model to provide insights to the business. The data sources are a csv and a json file placed in the S3 Raw Layer. All the pipeline files have been written in Python for the Pyspark.

## Important Links
 - [PowerBi Dashboard](https://app.powerbi.com/view?r=eyJrIjoiNWUxYWY1ZWEtNjBmMi00OThmLWE2MjUtYmFhZjYwMTg0NjI5IiwidCI6ImRlNTIzZmMwLWNjNTctNGFlNS04YzhjLTAxZWFkMjEyYWIzMyJ9)
 - [Google Drive Resources Folder](#)
## About the Data Model
### Dimensional Model
The business requirements have been transformed in the next Kimball Star:

![alt text](readme-resources/dmodel.png)

### Data Dictionary Presentation Layer
#### Dim Department
Catalog for all the departments in the dataset. 
- **Distribution Policy**: The distribution is *Auto* let's redshift decide how to distribute this table based on the size
- **Uniqueness Policy**: the etl search for new records every incremental load and append them if exists. A new key is generated.
- **ETL Script**: ![dim-department.py](dimensionsProcess/dim-department.py)

| Column Name     | Type   | Source                      | Comment                                     | Sample                              |
|-----------------|--------|-----------------------------|---------------------------------------------|-------------------------------------|
| department_key  | String | -                           | Surrogate Key Generated                     | 68d2e3f-6e13-463a-9048-bbb85acc076e |
| department_name | String | products.json => department |                                             | frozen                              |
| inserted_date   | Date   | -                           | Execution Date where the record is inserted | 2020/07/17                          |

#### Dim Aisle
Catalog for all the departments in the dataset. 
- **Distribution Policy**: The distribution is *Auto* let's redshift decide how to distribute this table based on the size
- **Uniqueness Policy**,the etl search for new records every incremental load and append them if exists. A new key is generated.
- **ETL Script**: ![dim-aisle.py](dimensionsProcess/dim-aisle.py)


| Column Name   | Type   | Source                 | Comment                                     | Sample                              |
|---------------|--------|------------------------|---------------------------------------------|-------------------------------------|
| aisle_key     | String | -                      | Surrogate Key Generated                     | 68d2e3f-6e13-463a-9048-bbb85acc076e |
| airley_name   | String | products.json => aisle |                                             | asian foods                         |
| inserted_date | Date   | -                      | Execution Date where the record is inserted | 2020/07/17                          |

#### Dim User
Catalog for all the aisles in the dataset. The structure enables the extension and enrichment of future requirements on users, as distribution by age or gender. 
- **Distribution Policy**: The distribution is *Auto* let's redshift decide how to distribute this table based on the size
- **Uniqueness Policy**,the etl search for new records every incremental load and append them if exists. A new key is generated.
- **ETL Script**: ![dim-user.py](dimensionsProcess/dim-user.py)


| Column Name   | Type   | Source                 | Comment                                     | Sample     |
|---------------|--------|------------------------|---------------------------------------------|------------|
| user_key      | String | dataset.csv => USER_ID | Surrogate Key                               | 132366     |
| inserted_date | Date   | -                      | Execution Date where the record is inserted | 2020/07/17 |

#### Dim Product
Catalog for all the products in the dataset. The structure enables the extension and enrichment of future requirements on products, as size, brand, etc. 
- **Distribution Policy**: The Distribution Key is aisley_key to keep the data related to an specific aisle in the same node and avoid data movement.
- **Uniqueness Policy**,the etl search for new records every incremental load and append them if exists. A new key is generated.
- **ETL Script**: ![dim-product.py](dimensionsProcess/dim-product.py)


| Column Name   | Type   | Source                   | Comment                                     | Sample                              |
|---------------|--------|--------------------------|---------------------------------------------|-------------------------------------|
| product_key   | String | -                        | Surrogate Key Generated                     | 68d2e3f-6e13-463a-9048-bbb85acc076e |
| aisle_key     | String | dim_aisle.aisle_key      | Foreign Key pointing to dim_aisle           | 68d2e3f-6e13-463a-9048-bbb85acc075r |
| product_name  | String | products.json => PRODUCT |                                             | Dark Chocolate Truffles             |
| inserted_date | Date   | -                        | Execution Date where the record is inserted | 2020/07/17                          |

### Dim Order Product
Bridge to hold the *Add to cart order* of a product in an order. 
- **Distribution Policy**: The Distribution Key is aisley_key to keep the data related to an specific aisle in the same node and avoid data movement.
- **Uniqueness Policy**,the etl builds the bridge based on new records every incremental load and append them if exists.
- **ETL Script**: ![dim-order-product-bridge.py](dimensionsProcess/ddim-order-product-bridge.py)


