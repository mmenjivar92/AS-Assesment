# AS - Technical Assesment
The objective of the project is build a dimensional model to provide insights to the business. The data sources are a csv and a json file placed in the S3 Raw Layer. All the pipeline files have been written in Python for the Pyspark.

## Important Links
 - [PowerBi Dashboard](https://app.powerbi.com/view?r=eyJrIjoiNWUxYWY1ZWEtNjBmMi00OThmLWE2MjUtYmFhZjYwMTg0NjI5IiwidCI6ImRlNTIzZmMwLWNjNTctNGFlNS04YzhjLTAxZWFkMjEyYWIzMyJ9)
 - [Google Drive Resources Folder](#)
## About the Data Model
### Dimensional Model
The business requirements have been transformed in the next Kimball Star:

![alt text](readme-resources/dmodel.png)

### Data Dictionary
#### Dim Department
Catalog for all the departments in the dataset. 
- **The Distribution is Auto**, let's redshift decide how to distribute this table based on the size
- **Uniqueness Policy**,the etl search for new records every incremental load and append them if exists. A new key is generated.

| Column Name     | Type   | Source                 | Comment                                     | Sample                              |
|-----------------|--------|------------------------|---------------------------------------------|-------------------------------------|
| department_key  | String | -                      | Surrogate Key Generated                     | 68d2e3f-6e13-463a-9048-bbb85acc076e |
| department_name | String | products.json => aisle |                                             | frozen                              |
| inserted_date   | Date   | -                      | Execution Date where the record is inserted | 2020/07/17                          |

#### Dim Aisle
Catalog for all the aisles in the dataset. The Distribution is Auto, let's redshift decide how to distribute this table based on the size
