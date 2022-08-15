# MindsDB and Tableau

Tableau lets you visualize your data easily and intuitively. Now that MindsDB supports the MySQL binary protocol, you can connect it to Tableau and see the forecasts.

## Connect MindsDB in Tableau

Follow the steps below to connect your MindsDB to Tableau.

First, create a new workbook in Tableau and open the *Connectors* tab in the *Connect to Data* window.

<p align="center">
  <img src="/assets/connect_tableau.png" />
</p>

Next, choose *MySQL* and provide the details of your MindsDB connection, such as the IP, port, and database name. Optionally, you can provide a username and password. Then, click *Sign In*.

<p align="center">
  <img src="/assets/connect_tableau_2.png" />
</p>

Now you're connected!

## Overview of MindsDB in Tableau

The content of your MindsDB is visible in the right-side pane.

<p align="center">
  <img src="/assets/connect_tableau_3.png" />
</p>

All the predictors are listed under the *Table* section. You can also switch between the integrations, such as *mindsdb* or *files*, in the *Database* section using the drop-down.

<p align="center">
  <img src="/assets/connect_tableau_4.png" />
</p>

Now, let's run some examples!

## Examples

### Example 1

Previewing one of the tables from the *mysql* integration:

<p align="center">
  <img src="/assets/connect_tableau_5.png" />
</p>

### Example 2

There is one technical limitation. Namely, we cannot join tables from different databases/integrations in Tableau. To overcome this challenge, you can use either views or custom SQL queries.

* Previewing a view that joins a data table with a predictor table:

<p align="center">
  <img src="/assets/connect_tableau_6.png" />
</p>

* Using a custom SQL query by clicking the *New Custom SQL* button in the right-side pane:

<p align="center">
  <img src="/assets/connect_tableau_7.png" />
</p>

Go ahead and try it out yourself!
