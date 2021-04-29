# Example Use Case

## Data-inconsistency

### Description
In this example we illustrate the data inconsistency discovery across databases for a common entity type.

According the the mapping rules declared in data-inconstency.pml, *Product* entity type is spread among *KVSchema* key value database, *myRelSchema* relational database and in an embedded document in *categorySchema* document database.

In the data deployed *price* data for a particular *Product id* is different in *KVProdPrice* than the one in *ProductCatalogTable* 

Running `getAllProducts` test in [Test Class](https://github.com/gobertm/HyDRa/blob/main/Use-Cases/data-inconsistency/API/src/test/java/tests/DataInconsistenciesTests.java) will retrieve all found products, join them and identify data inconsistencies for *price* attribute.
Logger lines will spot inconsistencies :

    WARN  ProductService:153 - data consistency problem: duplicate values for attribute : 'Product.price' ==> 1700927051 and 0

### Model 

![data-inconsistency model](./model.PNG)

### How-to

-   Run 'docker-compose up' . To deploy databases and data.
-   Import API project or generate API using pml file.
-   Run `getAllProducts` test of [Test Class](https://github.com/gobertm/HyDRa/blob/main/Use-Cases/data-inconsistency/API/src/test/java/tests/DataInconsistenciesTests.java)