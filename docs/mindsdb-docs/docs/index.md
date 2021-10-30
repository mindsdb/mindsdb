# What is MindsDB?

MindsDB enables advanced predictive capabilities directly in your Database. This puts sophisticated machine learning techniques into the hands of anyone who knows SQL (data analysts, developers and business intelligence users) without the need for a new tool or significant training.

Data is the single most important ingredient in machine learning, and your data lives in a database. So why do machine learning anywhere else?

![Machine Learning in Database using SQL](./assets/mdb_image.png)

## The Vision:

**_A world where people use intelligent databases to make better data-driven decisions._**

...and we believe the best way to make these decisions is to enable this capability within existing databases without the effort of creating a new one.

(Despite the name we are not actually a database!)

## From Database Tables to Machine Learning Models

!!! info "As a quick example, let's consider a database that stores the SqFt and Price of Home Rentals:"

    ```sql
    SELECT sqft, price FROM home_rentals_table;
    ```

    ![SQFT vs Price](/assets/info/sqft-price.png)

!!! note "You query the database for information in this table and if your search criteria generates a match: you get results:"

    ```sql
    SELECT sqft, price FROM home_rentals_table WHERE sqft = 900;
    ```

    ![SELECT FROM Table](/assets/info/select.png)

!!! note "If your search criteria do not generate a match - your results will be empty:"

    ```sql
    SELECT sqft, price FROM home_rentals_table WHERE sqft = 800;
    ```

    ![SELECT FROM Table No Results](/assets/info/selectm.png)

!!! tip "An ML model can be fitted to the data in the home rentals table."

    ```sql
    CREATE PREDICTOR  home_rentals_model FROM home_rentals_table PREDICT price;
    ```

    ![Model](/assets/info/model.png)

!!! success "An ML model can provide approximate answers for searches where there is no exact match in the income table:"

    ```sql
    SELECT sqft, price FROM home_rentals_model WHERE sqft = 800;
    ```

    ![Query model](/assets/info/query.png)

## Getting Started

To start using MindsDB, check out our [Getting Started Guide](./info.md)
