import json
from dataclasses import dataclass
from typing import List

import requests
import pytest

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from tests.integration.conftest import HTTP_API_ROOT
from tests.integration.utils.http_test_helpers import HTTPHelperMixin

@dataclass
class SqlParserTestData:
    sql_query: str
    expected_params: dict
    expected_datasources: dict
    replace_constants: bool
    identifiers_to_replace: dict
    parameterized_query: str = None


SQL_QUERY_TEST_DATA = {
    "SELECT1": SqlParserTestData(
        sql_query="SELECT * FROM postgres.products WHERE price > 10 AND brand='CoverON'",
        expected_params={
            "price": ["price", 10, "int"],
            "brand": ["brand", "CoverON", "str"],
        },
        expected_datasources={"postgres": ["products"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "SELECT1_1": SqlParserTestData(
        sql_query="SELECT * FROM postgres.products WHERE price > 10 AND brand='CoverON'",
        expected_params={
            "price": ["price", 10, "int"],
            "brand": ["brand", "CoverON", "str"],
        },
        expected_datasources={},
        replace_constants=True,
        identifiers_to_replace={
            "price": "intended_price",
            "brand": "brand_of_interest",
        },
        parameterized_query="SELECT * FROM postgres.products WHERE price > '@intended_price' AND brand = '@brand_of_interest'",
    ),
    "SELECT2": SqlParserTestData(
        sql_query="SELECT * from postgres.products JOIN files.reviews ON products.asin = reviews.productId WHERE postgres.products.price < 100",
        expected_params={
            "postgres.products.price": ["postgres.products.price", 100, "int"]
        },
        expected_datasources={"postgres": ["products"], "files": ["reviews"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "SELECT3": SqlParserTestData(
        sql_query="SELECT COUNT(*) from tpch10g.lineitem",
        expected_params={},
        expected_datasources={"tpch10g": ["lineitem"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "SELECT4": SqlParserTestData(
        sql_query="SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal FROM(SELECT SUBSTRING(c_phone from 1 for 2) AS cntrycode, c_acctbal FROM tpch10g.customer WHERE SUBSTRING(c_phone from 1 for 2) IN ('30', '13', '34', '32', '20', '27', '18') AND c_acctbal > ( SELECT AVG(c_acctbal) FROM tpch10g.customer WHERE c_acctbal > 0.00 AND SUBSTRING(c_phone from 1 for 2) IN ('30', '13', '34', '32', '20', '27', '18') ) AND NOT EXISTS ( SELECT * FROM tpch10g.orders WHERE o_custkey = c_custkey ) ) AS custsale GROUP BY cntrycode ORDER BY cntrycode",
        expected_params={
            "c_phone": ["c_phone", 1, "int"],
            "c_phone1": ["c_phone", 2, "int"],
            "c_phone2": ["c_phone", 1, "int"],
            "c_phone3": ["c_phone", 2, "int"],
            "c_phone4": ["c_phone", "30", "str"],
            "c_phone5": ["c_phone", "13", "str"],
            "c_phone6": ["c_phone", "34", "str"],
            "c_phone7": ["c_phone", "32", "str"],
            "c_phone8": ["c_phone", "20", "str"],
            "c_phone9": ["c_phone", "27", "str"],
            "c_phone10": ["c_phone", "18", "str"],
            "c_acctbal": ["c_acctbal", 0.0, "float"],
            "c_phone11": ["c_phone", 1, "int"],
            "c_phone12": ["c_phone", 2, "int"],
            "c_phone13": ["c_phone", "30", "str"],
            "c_phone14": ["c_phone", "13", "str"],
            "c_phone15": ["c_phone", "34", "str"],
            "c_phone16": ["c_phone", "32", "str"],
            "c_phone17": ["c_phone", "20", "str"],
            "c_phone18": ["c_phone", "27", "str"],
            "c_phone19": ["c_phone", "18", "str"],
        },
        expected_datasources={"tpch10g": ["customer", "orders"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "SELECT5": SqlParserTestData(
        sql_query="SELECT s_name, count(*) as numwait FROM tpch10g.supplier, tpch10g.lineitem l1, tpch10g.orders, tpch10g.nation WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate AND EXISTS ( SELECT * FROM tpch10g.lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( select * from tpch10g.lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate ) AND s_nationkey = n_nationkey AND n_name = 'ALGERIA' GROUP BY s_name ORDER BY numwait desc, s_name",
        expected_params={
            "o_orderstatus": ["o_orderstatus", "F", "str"],
            "n_name": ["n_name", "ALGERIA", "str"],
        },
        expected_datasources={"tpch10g": ["supplier", "lineitem", "orders", "nation"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "SELECT6": SqlParserTestData(
        sql_query="SELECT s_name, s_address from tpch10g.supplier, tpch10g.nation WHERE s_suppkey IN ( SELECT ps_suppkey FROM tpch10g.partsupp WHERE ps_partkey in ( SELECT p_partkey FROM tpch10g.part WHERE p_name LIKE 'sky%' ) AND ps_availqty > ( SELECT 0.5 * sum(l_quantity) FROM tpch10g.lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey AND l_shipdate >= '1993-01-01' AND l_shipdate < '1994-01-01') ) AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA' ORDER BY s_name",
        expected_params={
            "p_name": ["p_name", "sky%", "str"],
            "tpch10g.lineitem": ["tpch10g.lineitem", 0.5, "float"],
            "l_shipdate": ["l_shipdate", "1993-01-01", "str"],
            "l_shipdate1": ["l_shipdate", "1994-01-01", "str"],
            "n_name": ["n_name", "SAUDI ARABIA", "str"],
        },
        expected_datasources={
            "tpch10g": ["supplier", "nation", "partsupp", "part", "lineitem"]
        },
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "UPDATE1": SqlParserTestData(
        sql_query="UPDATE postgres.products SET price = 10, comments = 'test comment' WHERE price = 11 AND brand='CoverON'",
        expected_params={
            "price": ["price", 10, "int"],
            "price1": ["price", 11, "int"],
            "comments": ["comments", "test comment", "str"],
            "brand": ["brand", "CoverON", "str"],
        },
        expected_datasources={"postgres": ["products"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "UPDATE2": SqlParserTestData(
        sql_query="UPDATE postgres.products SET price = price * 1.1",
        expected_params={"price": ["price", 1.1, "float"]},
        expected_datasources={"postgres": ["products"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "UPDATE3": SqlParserTestData(
        sql_query="UPDATE postgres.products SET price = (SELECT price FROM postgres.productReviews WHERE rating > 5) WHERE brand='CoverON'",
        expected_params={
            "rating": ["rating", 5, "int"],
            "brand": ["brand", "CoverON", "str"],
        },
        expected_datasources={"postgres": ["products", "productReviews"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "UPDATE4": SqlParserTestData(
        sql_query="UPDATE postgres.employees SET bonus = CASE WHEN performance_rating = 'A' THEN 1000 WHEN performance_rating = 'B' THEN 500 ELSE 0 END",
        expected_params={
            "bonus": ["bonus", 1000, "int"],
            "bonus1": ["bonus", 500, "int"],
            "bonus2": ["bonus", 0, "int"],
            "performance_rating": ["performance_rating", "A", "str"],
            "performance_rating1": ["performance_rating", "B", "str"],
        },
        expected_datasources={"postgres": ["employees"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "UPDATE5": SqlParserTestData(
        sql_query="UPDATE postgres.employees SET salary = salary + (SELECT AVG(bonus) FROM postgres.bonuses WHERE bonuses.department_id = employees.department_id) WHERE department_id = 10",
        expected_params={"department_id": ["department_id", 10, "int"]},
        expected_datasources={"postgres": ["employees", "bonuses"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "INSERT1": SqlParserTestData(
        sql_query="INSERT INTO postgres.employees (employee_id, first_name, last_name) VALUES (101, 'John', 'Doe')",
        expected_params={
            "employee_id": ["employee_id", 101, "int"],
            "first_name": ["first_name", "John", "str"],
            "last_name": ["last_name", "Doe", "str"],
        },
        expected_datasources={"postgres": ["employees"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "INSERT2": SqlParserTestData(
        sql_query="INSERT INTO postgres.employees_backup (employee_id, first_name, last_name) SELECT employee_id, first_name, last_name FROM postgres.employees WHERE department_id = 10",
        expected_params={"department_id": ["department_id", 10, "int"]},
        expected_datasources={"postgres": ["employees", "employees_backup"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "INSERT2_1": SqlParserTestData(
        sql_query="INSERT INTO postgres.employees_backup (employee_id, first_name, last_name) SELECT employee_id, first_name, last_name FROM postgres.employees WHERE department_id = 10",
        expected_params={"department_id": ["department_id", 10, "int"]},
        expected_datasources={},
        replace_constants=True,
        identifiers_to_replace={"department_id": "dept_id"},
        parameterized_query="INSERT INTO postgres.employees_backup(employee_id, first_name, last_name) SELECT employee_id, first_name, last_name FROM postgres.employees WHERE department_id = '@dept_id'",
    ),
    "CTE1": SqlParserTestData(
        sql_query="WITH HighSalaryEmployees AS (    SELECT employee_id, first_name, salary    FROM postgres.employees   WHERE salary > 5000) SELECT * FROM HighSalaryEmployees",
        expected_params={"salary": ["salary", 5000, "int"]},
        expected_datasources={"postgres": ["employees"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "CTE2": SqlParserTestData(
        sql_query="WITH DepartmentSalaries AS (    SELECT department_id, SUM(salary) AS total_salary    FROM postgres.employees WHERE salary > 1000   GROUP BY department_id) SELECT department_id, total_salary FROM DepartmentSalaries WHERE total_salary > 20000",
        expected_params={
            "salary": ["salary", 1000, "int"],
            "total_salary": ["total_salary", 20000, "int"],
        },
        expected_datasources={"postgres": ["employees"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "CTE3": SqlParserTestData(
        sql_query="WITH RankedEmployees AS (    SELECT employee_id, first_name, salary,           ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS rank    FROM postgres.employees) SELECT * FROM RankedEmployees WHERE rank = 1",
        expected_params={"rank": ["rank", 1, "int"]},
        expected_datasources={"postgres": ["employees"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "CTE4": SqlParserTestData(
        sql_query="WITH DepartmentCount AS (SELECT department_id, COUNT(*) AS employee_count FROM postgres.employees GROUP BY department_id),HighSalaryDepartments AS (SELECT department_id FROM postgres.employees WHERE salary > 8000 GROUP BY department_id) SELECT dc.department_id, dc.employee_count FROM DepartmentCount dc JOIN HighSalaryDepartments hsd ON dc.department_id = hsd.department_id",
        expected_params={"salary": ["salary", 8000, "int"]},
        expected_datasources={"postgres": ["employees"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "CTE5": SqlParserTestData(
        sql_query="WITH MonthlySales AS (    SELECT         product_id,         MONTH(sale_date) AS month,         SUM(sale_amount) AS total_sales    FROM pg_demo.sales    GROUP BY product_id, MONTH(sale_date)) SELECT product_id,        SUM(CASE WHEN month = 1 THEN total_sales ELSE 0 END) AS January,       SUM(CASE WHEN month = 2 THEN total_sales ELSE 0 END) AS February FROM MonthlySales GROUP BY product_id",
        expected_params={
            "month": ["month", 1, "int"],
            "month1": ["month", 2, "int"],
            "total_sales": ["total_sales", 0, "int"],
            "total_sales1": ["total_sales", 0, "int"],
        },
        expected_datasources={"pg_demo": ["sales"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "CTE6": SqlParserTestData(
        sql_query="with wscs as (select sold_date_sk,sales_price  from (select ws_sold_date_sk sold_date_sk,ws_ext_sales_price sales_price from TPCDS_SF10TCL.web_sales union all select cs_sold_date_sk sold_date_sk,cs_ext_sales_price sales_price from TPCDS_SF10TCL.catalog_sales)xx), wswscs as  (select d_week_seq,sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales, sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales, sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales from wscs,TPCDS_SF10TCL.date_dim where d_date_sk = sold_date_sk  group by d_week_seq) select d_week_seq1,round(sun_sales1/sun_sales2,2),round(mon_sales1/mon_sales2,2),round(tue_sales1/tue_sales2,2),round(wed_sales1/wed_sales2,2),round(thu_sales1/thu_sales2,2),round(fri_sales1/fri_sales2,2),round(sat_sales1/sat_sales2,2) from (select wswscs.d_week_seq d_week_seq1,sun_sales sun_sales1,mon_sales mon_sales1,tue_sales tue_sales1,wed_sales wed_sales1,thu_sales thu_sales1,fri_sales fri_sales1,sat_sales sat_sales1 from wswscs,TPCDS_SF10TCL.date_dim where TPCDS_SF10TCL.date_dim.d_week_seq = wswscs.d_week_seq and d_year = 2001) y, (select wswscs.d_week_seq d_week_seq2,sun_sales sun_sales2,mon_sales mon_sales2,tue_sales tue_sales2,wed_sales wed_sales2,thu_sales thu_sales2,fri_sales fri_sales2,sat_sales sat_sales2  from wswscs,TPCDS_SF10TCL.date_dim   where TPCDS_SF10TCL.date_dim.d_week_seq = wswscs.d_week_seq and d_year = 2001+1) z where d_week_seq1=d_week_seq2-53 order by d_week_seq1",
        expected_params={
            "d_year": ["d_year", 2001, "int"],
            "d_year1": ["d_year", 1, "int"],
            "d_year2": ["d_year", 2001, "int"],
            "sun_sales2": ["sun_sales2", 2, "int"],
            "mon_sales2": ["mon_sales2", 2, "int"],
            "tue_sales2": ["tue_sales2", 2, "int"],
            "wed_sales2": ["wed_sales2", 2, "int"],
            "thu_sales2": ["thu_sales2", 2, "int"],
            "fri_sales2": ["fri_sales2", 2, "int"],
            "sat_sales2": ["sat_sales2", 2, "int"],
            "d_day_name": ["d_day_name", "Sunday", "str"],
            "sales_price": ["sales_price", None, "NoneType"],
            "d_day_name1": ["d_day_name", "Monday", "str"],
            "sales_price1": ["sales_price", None, "NoneType"],
            "d_day_name2": ["d_day_name", "Tuesday", "str"],
            "sales_price2": ["sales_price", None, "NoneType"],
            "d_day_name3": ["d_day_name", "Wednesday", "str"],
            "sales_price3": ["sales_price", None, "NoneType"],
            "d_day_name4": ["d_day_name", "Thursday", "str"],
            "sales_price4": ["sales_price", None, "NoneType"],
            "d_day_name5": ["d_day_name", "Friday", "str"],
            "sales_price5": ["sales_price", None, "NoneType"],
            "d_day_name6": ["d_day_name", "Saturday", "str"],
            "sales_price6": ["sales_price", None, "NoneType"],
            "d_week_seq2": ["d_week_seq2", 53, "int"],
        },
        expected_datasources={
            "TPCDS_SF10TCL": ["web_sales", "catalog_sales", "date_dim"]
        },
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH1": SqlParserTestData(
        sql_query="select l_returnflag,l_linestatus,sum(l_quantity) as sum_qty,sum(l_extendedprice) as sum_base_price,sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,avg(l_quantity) as avg_qty,avg(l_extendedprice) as avg_price,avg(l_discount) as avg_disc,count(*) as count_order from TPCH_SF100.lineitem where l_shipdate <= '1998-09-23' group by l_returnflag,l_linestatus order by l_returnflag, l_linestatus",
        expected_params={
            "l_extendedprice": ["l_extendedprice", 1, "int"],
            "l_extendedprice1": ["l_extendedprice", 1, "int"],
            "l_discount": ["l_discount", 1, "int"],
            "l_shipdate": ["l_shipdate", "1998-09-23", "str"],
        },
        expected_datasources={"TPCH_SF100": ["lineitem"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH2": SqlParserTestData(
        sql_query="select s_acctbal,s_name,n_name,p_partkey,p_mfgr,s_address,s_phone,s_comment from TPCH_SF100.part,TPCH_SF100.supplier,TPCH_SF100.partsupp,TPCH_SF100.nation,TPCH_SF100.region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 2 and p_type like '%COPPER' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AFRICA' and ps_supplycost = (select min(ps_supplycost) from TPCH_SF100.partsupp,TPCH_SF100.supplier,TPCH_SF100.nation,TPCH_SF100.region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AFRICA') order by s_acctbal desc,n_name,s_name,p_partkey LIMIT 100",
        expected_params={
            "p_size": ["p_size", 2, "int"],
            "p_type": ["p_type", "%COPPER", "str"],
            "r_name": ["r_name", "AFRICA", "str"],
            "r_name1": ["r_name", "AFRICA", "str"],
        },
        expected_datasources={
            "TPCH_SF100": [
                "part",
                "supplier",
                "partsupp",
                "nation",
                "region",
            ]
        },
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH3": SqlParserTestData(
        sql_query="select l_orderkey,sum(l_extendedprice * (1 - l_discount)) as revenue,o_orderdate,o_shippriority from TPCH_SF100.customer,TPCH_SF100.orders,TPCH_SF100.lineitem where c_mktsegment = 'AUTOMOBILE' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-01' and l_shipdate > '1995-03-01' group by l_orderkey,o_orderdate,o_shippriority order by revenue desc,o_orderdate",
        expected_params={
            "l_extendedprice": ["l_extendedprice", 1, "int"],
            "c_mktsegment": ["c_mktsegment", "AUTOMOBILE", "str"],
            "o_orderdate": ["o_orderdate", "1995-03-01", "str"],
            "l_shipdate": ["l_shipdate", "1995-03-01", "str"],
        },
        expected_datasources={"TPCH_SF100": ["customer", "orders", "lineitem"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH4": SqlParserTestData(
        sql_query="select o_orderpriority,count(*) as order_count from TPCH_SF100.orders where o_orderdate >= '1993-02-01' and o_orderdate < '1993-05-01'and exists(select * from TPCH_SF100.lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate) group by o_orderpriority order by o_orderpriority",
        expected_params={
            "o_orderdate": ["o_orderdate", "1993-02-01", "str"],
            "o_orderdate1": ["o_orderdate", "1993-05-01", "str"],
        },
        expected_datasources={"TPCH_SF100": ["orders", "lineitem"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH5": SqlParserTestData(
        sql_query="select n_name,sum(l_extendedprice * (1 - l_discount)) as revenue from TPCH_SF100.customer,TPCH_SF100.orders,TPCH_SF100.lineitem,TPCH_SF100.supplier,TPCH_SF100.nation,TPCH_SF100.region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE' and o_orderdate >= '1995-01-01' and o_orderdate < '1995-02-01'group by n_name order by revenue desc",
        expected_params={
            "l_extendedprice": ["l_extendedprice", 1, "int"],
            "r_name": ["r_name", "EUROPE", "str"],
            "o_orderdate": ["o_orderdate", "1995-01-01", "str"],
            "o_orderdate1": ["o_orderdate", "1995-02-01", "str"],
        },
        expected_datasources={
            "TPCH_SF100": [
                "customer",
                "orders",
                "lineitem",
                "supplier",
                "nation",
                "region",
            ]
        },
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH6": SqlParserTestData(
        sql_query="select sum(l_extendedprice * l_discount) as revenue from TPCH_SF100.lineitem where l_shipdate >= '1994-01-01' and l_shipdate < '1995-01-01'and l_discount between 0.04 - 0.01 and 0.04 + 0.01 and l_quantity < 24",
        expected_params={
            "l_shipdate": ["l_shipdate", "1994-01-01", "str"],
            "l_shipdate1": ["l_shipdate", "1995-01-01", "str"],
            "l_discount": ["l_discount", 0.04, "float"],
            "l_discount1": ["l_discount", 0.01, "float"],
            "l_discount2": ["l_discount", 0.04, "float"],
            "l_discount3": ["l_discount", 0.01, "float"],
            "l_quantity": ["l_quantity", 24, "int"],
        },
        expected_datasources={"TPCH_SF100": ["lineitem"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH7": SqlParserTestData(
        sql_query="select supp_nation,cust_nation,l_year,sum(volume) as revenue from (select n1.n_name as supp_nation,n2.n_name as cust_nation,extract(year from l_shipdate) as l_year,l_extendedprice * (1 - l_discount) as volume from TPCH_SF100.supplier,TPCH_SF100.lineitem,TPCH_SF100.orders,TPCH_SF100.customer,TPCH_SF100.nation n1,TPCH_SF100.nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ((n1.n_name = 'FRANCE' and n2.n_name = 'KENYA')or (n1.n_name = 'KENYA' and n2.n_name ='FRANCE'))and l_shipdate between '1995-01-01' and '1996-12-31') as shipping group by supp_nation,cust_nation,l_year order by supp_nation,cust_nation,l_year",
        expected_params={
            "l_extendedprice": ["l_extendedprice", 1, "int"],
            "n1.n_name": ["n1.n_name", "FRANCE", "str"],
            "n2.n_name": ["n2.n_name", "KENYA", "str"],
            "n1.n_name1": ["n1.n_name", "KENYA", "str"],
            "n2.n_name1": ["n2.n_name", "FRANCE", "str"],
            "l_shipdate": ["l_shipdate", "1995-01-01", "str"],
            "l_shipdate1": ["l_shipdate", "1996-12-31", "str"],
        },
        expected_datasources={
            "TPCH_SF100": [
                "supplier",
                "lineitem",
                "orders",
                "customer",
                "nation",
            ]
        },
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH8": SqlParserTestData(
        sql_query="select o_year,sum(case when nat = 'FRANCE' then volume else 0 end) / sum(volume) as mkt_share from(select extract(year from o_orderdate) as o_year,l_extendedprice * (1 - l_discount) as volume,n2.n_name as nat from TPCH_SF100.part, TPCH_SF100.supplier, TPCH_SF100.lineitem,TPCH_SF100.orders,TPCH_SF100.customer,TPCH_SF100.nation n1,TPCH_SF100.nation n2,TPCH_SF100.region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'EUROPE' and s_nationkey = n2.n_nationkey and o_orderdate between '1995-01-01' and '1996-12-31' and p_type = 'LARGE PLATED STEEL' ) as all_nations group by o_year order by o_year",
        expected_params={
            "l_extendedprice": ["l_extendedprice", 1, "int"],
            "r_name": ["r_name", "EUROPE", "str"],
            "o_orderdate": ["o_orderdate", "1995-01-01", "str"],
            "o_orderdate1": ["o_orderdate", "1996-12-31", "str"],
            "p_type": ["p_type", "LARGE PLATED STEEL", "str"],
            "nat": ["nat", "FRANCE", "str"],
            "volume": ["volume", 0, "int"],
        },
        expected_datasources={
            "TPCH_SF100": [
                "part",
                "supplier",
                "lineitem",
                "orders",
                "customer",
                "nation",
                "region",
            ]
        },
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH9": SqlParserTestData(
        sql_query="select n_name, o_year, sum(amount) as sum_profit from(select n_name, extract(year from o_orderdate) as o_year,l_extendedprice * (1 - l_discount) -ps_supplycost * l_quantity as amount from TPCH_SF100.part,TPCH_SF100.supplier,TPCH_SF100.lineitem,TPCH_SF100.partsupp,TPCH_SF100.orders,TPCH_SF100.nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%moccasin%') as profit group by n_name,o_year order by n_name,o_year desc",
        expected_params={
            "l_extendedprice": ["l_extendedprice", 1, "int"],
            "p_name": ["p_name", "%moccasin%", "str"],
        },
        expected_datasources={
            "TPCH_SF100": [
                "part",
                "supplier",
                "lineitem",
                "partsupp",
                "orders",
                "nation",
            ]
        },
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH10": SqlParserTestData(
        sql_query="select c_custkey,  c_name,   sum(l_extendedprice * (1 - l_discount)) as revenue,   c_acctbal,   n_name,   c_address,   c_phone,   c_comment  from   TPCH_SF100.customer,   TPCH_SF100.orders,   TPCH_SF100.lineitem,   TPCH_SF100.nation  where   c_custkey = o_custkey   and l_orderkey = o_orderkey   and o_orderdate >= '1993-10-01'   and o_orderdate < '1994-01-01'and l_returnflag = 'R'   and c_nationkey = n_nationkey  group by   c_custkey,   c_name,   c_acctbal,   c_phone,   n_name,   c_address,   c_comment  order by   revenue desc",
        expected_params={
            "l_extendedprice": ["l_extendedprice", 1, "int"],
            "o_orderdate": ["o_orderdate", "1993-10-01", "str"],
            "o_orderdate1": ["o_orderdate", "1994-01-01", "str"],
            "l_returnflag": ["l_returnflag", "R", "str"],
        },
        expected_datasources={
            "TPCH_SF100": ["customer", "orders", "lineitem", "nation"]
        },
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH11": SqlParserTestData(
        sql_query="select  ps_partkey,  sum(ps_supplycost * ps_availqty) as value from  TPCH_SF100.partsupp,  TPCH_SF100.supplier,  TPCH_SF100.nation where  ps_suppkey = s_suppkey  and s_nationkey = n_nationkey  and n_name = 'EGYPT' group by  ps_partkey having   sum(ps_supplycost * ps_availqty) > (    select     sum(ps_supplycost * ps_availqty) * 0.0001000000    from     TPCH_SF100.partsupp,     TPCH_SF100.supplier,     TPCH_SF100.nation    where     ps_suppkey = s_suppkey     and s_nationkey = n_nationkey     and n_name = 'EGYPT'   ) order by  value desc",
        expected_params={
            "n_name": ["n_name", "EGYPT", "str"],
            "ps_availqty": ["ps_availqty", 0.0001, "float"],
            "n_name1": ["n_name", "EGYPT", "str"],
        },
        expected_datasources={"TPCH_SF100": ["partsupp", "supplier", "nation"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH12": SqlParserTestData(
        sql_query="select  l_shipmode,  sum(case   when o_orderpriority = '1-URGENT'    or o_orderpriority = '2-HIGH'    then 1   else 0  end) as high_line_count,  sum(case   when o_orderpriority <> '1-URGENT'    and o_orderpriority <> '2-HIGH'    then 1   else 0  end) as low_line_count from  TPCH_SF100.orders,  TPCH_SF100.lineitem where  o_orderkey = l_orderkey  and l_shipmode in ('TRUCK', 'REG AIR')  and l_commitdate < l_receiptdate  and l_shipdate < l_commitdate  and l_receiptdate >= '1995-01-01'  and l_receiptdate < '1996-01-01'group by  l_shipmode order by  l_shipmode",
        expected_params={
            "o_orderpriority": ["o_orderpriority", "1-URGENT", "str"],
            "o_orderpriority1": ["o_orderpriority", "2-HIGH", "str"],
            "o_orderpriority2": ["o_orderpriority", 1, "int"],
            "o_orderpriority3": ["o_orderpriority", 0, "int"],
            "o_orderpriority4": ["o_orderpriority", "1-URGENT", "str"],
            "o_orderpriority5": ["o_orderpriority", "2-HIGH", "str"],
            "o_orderpriority6": ["o_orderpriority", 1, "int"],
            "o_orderpriority7": ["o_orderpriority", 0, "int"],
            "l_shipmode": ["l_shipmode", "TRUCK", "str"],
            "l_shipmode1": ["l_shipmode", "REG AIR", "str"],
            "l_receiptdate": ["l_receiptdate", "1995-01-01", "str"],
            "l_receiptdate1": ["l_receiptdate", "1996-01-01", "str"],
        },
        expected_datasources={"TPCH_SF100": ["orders", "lineitem"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH13": SqlParserTestData(
        sql_query="select  c_count,  count(*) as custdist from  (   select    c_custkey,    count(o_orderkey)   from    TPCH_SF100.customer left outer join TPCH_SF100.orders on     c_custkey = o_custkey and o_comment not like '%special%%requests%'   group by    c_custkey  ) as c_orders (c_custkey, c_count) group by  c_count order by  custdist desc,  c_count desc",
        expected_params={"o_comment": ["o_comment", "%special%%requests%", "str"]},
        expected_datasources={"TPCH_SF100": ["customer", "orders"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH14": SqlParserTestData(
        sql_query="select  100.00 * sum(case   when p_type like 'PROMO%'    then l_extendedprice * (1 - l_discount)   else 0  end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from  TPCH_SF100.lineitem,  TPCH_SF100.part where  l_partkey = p_partkey  and l_shipdate >= '1995-02-01'  and l_shipdate < '1995-03-01'",
        expected_params={
            "TPCH_SF100.lineitem": ["TPCH_SF100.lineitem", 100.0, "float"],
            "p_type": ["p_type", "PROMO%", "str"],
            "l_extendedprice": ["l_extendedprice", 1, "int"],
            "l_discount": ["l_discount", 0, "int"],
            "l_extendedprice1": ["l_extendedprice", 1, "int"],
            "l_shipdate": ["l_shipdate", "1995-02-01", "str"],
            "l_shipdate1": ["l_shipdate", "1995-03-01", "str"],
        },
        expected_datasources={"TPCH_SF100": ["lineitem", "part"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH16": SqlParserTestData(
        sql_query="select  p_brand,  p_type,  p_size,  count(distinct ps_suppkey) as supplier_cnt from  TPCH_SF100.partsupp,  TPCH_SF100.part where  p_partkey = ps_partkey  and p_brand <> 'Brand#21'  and p_type not like 'PROMO ANODIZED%'  and p_size in (5, 43, 4, 2, 10, 50, 45, 20)  and ps_suppkey not in (   select    s_suppkey   from    TPCH_SF100.supplier   where    s_comment like '%TPCH_SF100.Customer%Complaints%'  ) group by  p_brand,  p_type,  p_size order by  supplier_cnt desc,  p_brand,  p_type,  p_size",
        expected_params={
            "p_brand": ["p_brand", "Brand#21", "str"],
            "p_type": ["p_type", "PROMO ANODIZED%", "str"],
            "p_size": ["p_size", 5, "int"],
            "p_size1": ["p_size", 43, "int"],
            "p_size2": ["p_size", 4, "int"],
            "p_size3": ["p_size", 2, "int"],
            "p_size4": ["p_size", 10, "int"],
            "p_size5": ["p_size", 50, "int"],
            "p_size6": ["p_size", 45, "int"],
            "p_size7": ["p_size", 20, "int"],
            "s_comment": ["s_comment", "%TPCH_SF100.Customer%Complaints%", "str"],
        },
        expected_datasources={"TPCH_SF100": ["partsupp", "part", "supplier"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH17": SqlParserTestData(
        sql_query="select  sum(l_extendedprice) / 7.0 as avg_yearly from  TPCH_SF100.lineitem,  TPCH_SF100.part where  p_partkey = l_partkey  and p_brand = 'Brand#15'  and p_container = 'SM JAR'  and l_quantity < (   select    0.2 * avg(l_quantity)   from    TPCH_SF100.lineitem   where    l_partkey = p_partkey  )",
        expected_params={
            "l_extendedprice": ["l_extendedprice", 7.0, "float"],
            "p_brand": ["p_brand", "Brand#15", "str"],
            "p_container": ["p_container", "SM JAR", "str"],
            "TPCH_SF100.lineitem": ["TPCH_SF100.lineitem", 0.2, "float"],
        },
        expected_datasources={
            "TPCH_SF100": [
                "lineitem",
                "part",
            ]
        },
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH18": SqlParserTestData(
        sql_query="select  c_name,  c_custkey,  o_orderkey,  o_orderdate,  o_totalprice,  sum(l_quantity) from  TPCH_SF100.customer,  TPCH_SF100.orders,  TPCH_SF100.lineitem where  o_orderkey in (   select    l_orderkey   from    TPCH_SF100.lineitem   group by    l_orderkey having     sum(l_quantity) > 315  )  and c_custkey = o_custkey  and o_orderkey = l_orderkey group by  c_name,  c_custkey,  o_orderkey,  o_orderdate,  o_totalprice order by  o_totalprice desc,  o_orderdate",
        expected_params={"l_quantity": ["l_quantity", 315, "int"]},
        expected_datasources={
            "TPCH_SF100": [
                "customer",
                "orders",
                "lineitem",
            ]
        },
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH19_1": SqlParserTestData(
        sql_query="select  sum(l_extendedprice* (1 - l_discount)) as revenue from  TPCH_SF100.lineitem,  TPCH_SF100.part where  (   p_partkey = l_partkey   and p_brand = 'Brand#15'   and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')   and l_quantity >= 9 and l_quantity <= 9 + 10   and p_size between 1 and 5   and l_shipmode in ('AIR', 'AIR REG')   and l_shipinstruct = 'DELIVER IN PERSON'  )  or  (   p_partkey = l_partkey   and p_brand = 'Brand#15'   and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')   and l_quantity >= 13 and l_quantity <= 13 + 10   and p_size between 1 and 10   and l_shipmode in ('AIR', 'AIR REG')   and l_shipinstruct = 'DELIVER IN PERSON'  )  or  (   p_partkey = l_partkey   and p_brand = 'Brand#15'   and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')   and l_quantity >= 27 and l_quantity <= 27 + 10   and p_size between 1 and 15   and l_shipmode in ('AIR', 'AIR REG')   and l_shipinstruct = 'DELIVER IN PERSON'  )",
        expected_params={
            "l_extendedprice": ["l_extendedprice", 1, "int"],
            "p_brand": ["p_brand", "Brand#15", "str"],
            "p_container": ["p_container", "SM CASE", "str"],
            "p_container1": ["p_container", "SM BOX", "str"],
            "p_container2": ["p_container", "SM PACK", "str"],
            "p_container3": ["p_container", "SM PKG", "str"],
            "l_quantity": ["l_quantity", 9, "int"],
            "l_quantity1": ["l_quantity", 9, "int"],
            "l_quantity2": ["l_quantity", 10, "int"],
            "p_size": ["p_size", 1, "int"],
            "p_size1": ["p_size", 5, "int"],
            "l_shipmode": ["l_shipmode", "AIR", "str"],
            "l_shipmode1": ["l_shipmode", "AIR REG", "str"],
            "l_shipinstruct": ["l_shipinstruct", "DELIVER IN PERSON", "str"],
            "p_brand1": ["p_brand", "Brand#15", "str"],
            "p_container4": ["p_container", "MED BAG", "str"],
            "p_container5": ["p_container", "MED BOX", "str"],
            "p_container6": ["p_container", "MED PKG", "str"],
            "p_container7": ["p_container", "MED PACK", "str"],
            "l_quantity3": ["l_quantity", 13, "int"],
            "l_quantity4": ["l_quantity", 13, "int"],
            "l_quantity5": ["l_quantity", 10, "int"],
            "p_size2": ["p_size", 1, "int"],
            "p_size3": ["p_size", 10, "int"],
            "l_shipmode2": ["l_shipmode", "AIR", "str"],
            "l_shipmode3": ["l_shipmode", "AIR REG", "str"],
            "l_shipinstruct1": ["l_shipinstruct", "DELIVER IN PERSON", "str"],
            "p_brand2": ["p_brand", "Brand#15", "str"],
            "p_container8": ["p_container", "LG CASE", "str"],
            "p_container9": ["p_container", "LG BOX", "str"],
            "p_container10": ["p_container", "LG PACK", "str"],
            "p_container11": ["p_container", "LG PKG", "str"],
            "l_quantity6": ["l_quantity", 27, "int"],
            "l_quantity7": ["l_quantity", 27, "int"],
            "l_quantity8": ["l_quantity", 10, "int"],
            "p_size4": ["p_size", 1, "int"],
            "p_size5": ["p_size", 15, "int"],
            "l_shipmode4": ["l_shipmode", "AIR", "str"],
            "l_shipmode5": ["l_shipmode", "AIR REG", "str"],
            "l_shipinstruct2": ["l_shipinstruct", "DELIVER IN PERSON", "str"],
        },
        expected_datasources={"TPCH_SF100": ["lineitem", "part"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH19_2": SqlParserTestData(
        sql_query="select  s_name,  s_address from  TPCH_SF100.supplier,  TPCH_SF100.nation where  s_suppkey in (   select    ps_suppkey   from    TPCH_SF100.partsupp   where    ps_partkey in (     select      p_partkey     from      TPCH_SF100.part     where      p_name like 'sky%'    )    and ps_availqty > (     select      0.5 * sum(l_quantity)     from      TPCH_SF100.lineitem     where      l_partkey = ps_partkey      and l_suppkey = ps_suppkey      and l_shipdate >= '1993-01-01'      and l_shipdate < '1994-01-01')  )  and s_nationkey = n_nationkey  and n_name = 'SAUDI ARABIA' order by  s_name",
        expected_params={
            "p_name": ["p_name", "sky%", "str"],
            "TPCH_SF100.lineitem": ["TPCH_SF100.lineitem", 0.5, "float"],
            "l_shipdate": ["l_shipdate", "1993-01-01", "str"],
            "l_shipdate1": ["l_shipdate", "1994-01-01", "str"],
            "n_name": ["n_name", "SAUDI ARABIA", "str"],
        },
        expected_datasources={
            "TPCH_SF100": ["supplier", "nation", "partsupp", "part", "lineitem"]
        },
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH19_3": SqlParserTestData(
        sql_query="select  s_name,  count(*) as numwait from  TPCH_SF100.supplier,  TPCH_SF100.lineitem l1,  TPCH_SF100.orders,  TPCH_SF100.nation where  s_suppkey = l1.l_suppkey  and o_orderkey = l1.l_orderkey  and o_orderstatus = 'F'  and l1.l_receiptdate > l1.l_commitdate  and exists (   select    *   from    TPCH_SF100.lineitem l2   where    l2.l_orderkey = l1.l_orderkey    and l2.l_suppkey <> l1.l_suppkey  )  and not exists (   select    *   from    TPCH_SF100.lineitem l3   where    l3.l_orderkey = l1.l_orderkey    and l3.l_suppkey <> l1.l_suppkey    and l3.l_receiptdate > l3.l_commitdate  )  and s_nationkey = n_nationkey  and n_name = 'ALGERIA' group by  s_name order by  numwait desc,  s_name",
        expected_params={
            "o_orderstatus": ["o_orderstatus", "F", "str"],
            "n_name": ["n_name", "ALGERIA", "str"],
        },
        expected_datasources={
            "TPCH_SF100": [
                "supplier",
                "lineitem",
                "orders",
                "nation",
            ]
        },
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH20": SqlParserTestData(
        sql_query="select  cntrycode,  count(*) as numcust,  sum(c_acctbal) as totacctbal from  (   select    substring(c_phone from 1 for 2) as cntrycode,    c_acctbal   from    TPCH_SF100.customer   where    substring(c_phone from 1 for 2) in     ('30', '13', '34', '32', '20', '27', '18')    and c_acctbal > (     select      avg(c_acctbal)     from      TPCH_SF100.customer     where      c_acctbal > 0.00      and substring(c_phone from 1 for 2) in       ('30', '13', '34', '32', '20', '27', '18')    )    and not exists (     select      *     from      TPCH_SF100.orders     where      o_custkey = c_custkey    )  ) as custsale group by  cntrycode order by  cntrycode",
        expected_params={
            "c_phone": ["c_phone", 1, "int"],
            "c_phone1": ["c_phone", 2, "int"],
            "c_phone2": ["c_phone", 1, "int"],
            "c_phone3": ["c_phone", 2, "int"],
            "c_phone4": ["c_phone", "30", "str"],
            "c_phone5": ["c_phone", "13", "str"],
            "c_phone6": ["c_phone", "34", "str"],
            "c_phone7": ["c_phone", "32", "str"],
            "c_phone8": ["c_phone", "20", "str"],
            "c_phone9": ["c_phone", "27", "str"],
            "c_phone10": ["c_phone", "18", "str"],
            "c_acctbal": ["c_acctbal", 0.0, "float"],
            "c_phone11": ["c_phone", 1, "int"],
            "c_phone12": ["c_phone", 2, "int"],
            "c_phone13": ["c_phone", "30", "str"],
            "c_phone14": ["c_phone", "13", "str"],
            "c_phone15": ["c_phone", "34", "str"],
            "c_phone16": ["c_phone", "32", "str"],
            "c_phone17": ["c_phone", "20", "str"],
            "c_phone18": ["c_phone", "27", "str"],
            "c_phone19": ["c_phone", "18", "str"],
        },
        expected_datasources={"TPCH_SF100": ["customer", "orders"]},
        replace_constants=False,
        identifiers_to_replace={},
    ),
    "TPCH21": SqlParserTestData(
        sql_query="select  s_acctbal,  s_name,  n_name,  p_partkey,  p_mfgr,  s_address,  s_phone,  s_comment from  TPCH_SF100.part,  TPCH_SF100.supplier,  TPCH_SF100.partsupp,  TPCH_SF100.nation,  TPCH_SF100.region where  p_partkey = ps_partkey  and s_suppkey = ps_suppkey  and p_size = 2  and p_type like '%COPPER'  and s_nationkey = n_nationkey  and n_regionkey = r_regionkey  and r_name = 'AFRICA'  and ps_supplycost = (   select    min(ps_supplycost)   from    TPCH_SF100.partsupp,    TPCH_SF100.supplier,    TPCH_SF100.nation,    TPCH_SF100.region   where    p_partkey = ps_partkey    and s_suppkey = ps_suppkey    and s_nationkey = n_nationkey    and n_regionkey = r_regionkey    and r_name = 'AFRICA'  ) order by  s_acctbal desc,  n_name,  s_name,  p_partkey",
        expected_params={
            "p_size": ["p_size", 2, "int"],
            "p_type": ["p_type", "%COPPER", "str"],
            "r_name": ["r_name", "AFRICA", "str"],
            "r_name1": ["r_name", "AFRICA", "str"],
        },
        expected_datasources={
            "TPCH_SF100": [
                "part",
                "supplier",
                "partsupp",
                "nation",
                "region",
            ]
        },
        replace_constants=False,
        identifiers_to_replace={},
    ),
}


class TestHTTP(HTTPHelperMixin):
    @staticmethod
    def get_files_list():
        response = requests.request("GET", f"{HTTP_API_ROOT}/files/")
        assert response.status_code == 200
        response_data = response.json()
        assert isinstance(response_data, list)
        return response_data

    @classmethod
    def setup_class(cls):
        cls._sql_via_http_context = {}

    def create_database(self, name, db_data):
        db_type = db_data["type"]
        # Drop any existing DB with this name to avoid conflicts
        self.sql_via_http(f"DROP DATABASE IF EXISTS {name};", RESPONSE_TYPE.OK)
        self.sql_via_http(
            f"CREATE DATABASE {name} WITH ENGINE = '{db_type}', PARAMETERS = {json.dumps(db_data['connection_data'])};",
            RESPONSE_TYPE.OK,
        )

    def validate_database_creation(self, name):
        res = self.sql_via_http(f"SELECT name FROM information_schema.databases WHERE name='{name}';")
        assert name in res["data"][0], f"Expected datasource is not found after creation - {name}: {res}"

    @pytest.mark.parametrize("util_uri", ["util/ping", "util/ping_native"])
    def test_utils(self, util_uri):
        """
        Call utilities ping endpoint
        THEN check the response is success
        """

        path = f"{HTTP_API_ROOT}/{util_uri}"
        response = requests.get(path)
        assert response.status_code == 200

    def test_auth(self):
        STRIPPED_API_ROOT = HTTP_API_ROOT.replace("/api", "")
        session = requests.Session()

        response = session.get(f"{HTTP_API_ROOT}/status")
        assert response.status_code == 200
        assert response.json()["auth"]["http_auth_enabled"] is False

        response = session.get(f"{HTTP_API_ROOT}/config/")
        assert response.status_code == 200
        assert response.json()["auth"]["http_auth_enabled"] is False

        response = session.get(f"{HTTP_API_ROOT}/tree/")
        assert response.status_code == 200
        response = session.get(f"{STRIPPED_API_ROOT}/a2a/status")
        assert response.status_code == 200
        response = session.get(f"{STRIPPED_API_ROOT}/mcp/status")
        assert response.status_code == 200

        response = session.put(
            f"{HTTP_API_ROOT}/config/", json={"http_auth_enabled": True, "username": "", "password": ""}
        )
        assert response.status_code == 400

        response = session.put(
            f"{HTTP_API_ROOT}/config/",
            json={"auth": {"http_auth_enabled": True, "username": "mindsdb", "password": "mindsdb"}},
        )
        assert response.status_code == 200

        response = session.get(f"{HTTP_API_ROOT}/status")
        assert response.status_code == 200
        assert response.json()["auth"]["http_auth_enabled"] is True

        response = session.get(f"{HTTP_API_ROOT}/tree/")
        assert response.status_code == 401

        response = session.post(f"{HTTP_API_ROOT}/login", json={"username": "mindsdb", "password": "mindsdb"})
        assert response.status_code == 200

        token = response.json().get("token")
        session.headers.update({"Authorization": f"Bearer {token}"})

        response = session.get(f"{HTTP_API_ROOT}/tree/")
        assert response.status_code == 200

        response = session.get(f"{STRIPPED_API_ROOT}/a2a/status")
        assert response.status_code == 200

        response = session.get(f"{STRIPPED_API_ROOT}/mcp/status")
        assert response.status_code == 200

        response = session.post(f"{HTTP_API_ROOT}/logout")
        assert response.status_code == 200

        response = session.get(f"{HTTP_API_ROOT}/tree/")
        assert response.status_code == 401

        response = session.get(f"{STRIPPED_API_ROOT}/a2a/status")
        assert response.status_code == 401

        response = session.get(f"{STRIPPED_API_ROOT}/mcp/status")
        assert response.status_code == 401

        response = session.post(f"{HTTP_API_ROOT}/login", json={"username": "mindsdb", "password": "mindsdb"})
        assert response.status_code == 200
        token = response.json().get("token")
        session.headers.update({"Authorization": f"Bearer {token}"})

        response = session.put(
            f"{HTTP_API_ROOT}/config/",
            json={"auth": {"http_auth_enabled": False, "username": "mindsdb", "password": ""}},
        )

        response = session.get(f"{HTTP_API_ROOT}/status")
        assert response.status_code == 200
        assert response.json()["auth"]["http_auth_enabled"] is False

    def test_gui_is_served(self):
        """
        GUI downloaded and available
        """
        response = requests.get(HTTP_API_ROOT.split("/api")[0])
        assert response.status_code == 200
        assert response.content.decode().find("<head>") > 0

    def test_files(self):
        """sql-via-http:
        upload file
        delete file
        upload file again
        """
        self.sql_via_http("DROP TABLE IF EXISTS files.movies;", RESPONSE_TYPE.OK)
        assert "movies" not in [file["name"] for file in self.get_files_list()]

        with open("tests/data/movies.csv") as f:
            files = {"file": ("movies.csv", f, "text/csv")}

            response = requests.request("PUT", f"{HTTP_API_ROOT}/files/movies", files=files)
            assert response.status_code == 200, f"Error uploading file. Response content: {response.content}"

        assert "movies" in [file["name"] for file in self.get_files_list()]

        response = requests.delete(f"{HTTP_API_ROOT}/files/movies")
        assert response.status_code == 200
        assert "movies" not in [file["name"] for file in self.get_files_list()]

        # Upload the file again (to guard against bugs where we still think a deleted file exists)
        with open("tests/data/movies.csv") as f:
            files = {"file": ("movies.csv", f, "text/csv")}

            response = requests.request("PUT", f"{HTTP_API_ROOT}/files/movies", files=files)
            assert response.status_code == 200

    @pytest.mark.parametrize(
        "test_id, sql_query_test_data", list[tuple[str, SqlParserTestData]](SQL_QUERY_TEST_DATA.items())
    )
    def test_sql_parser(self, test_id, sql_query_test_data):
        """test sql parser"""
        response = self.sql_query_const_via_http(
            sql_query_test_data.sql_query,
            sql_query_test_data.replace_constants,
            sql_query_test_data.identifiers_to_replace,
            RESPONSE_TYPE.OK,
        )
        assert response is not None
        for key, value in sql_query_test_data.expected_params.items():
            assert key in response["data"]["constant_with_identifiers"]
            assert response["data"]["constant_with_identifiers"][key] == value

        datasources_with_tables = response["data"]["datasources_with_tables"]
        assert set(datasources_with_tables.keys()) == set(
            sql_query_test_data.expected_datasources.keys()
        )

        if sql_query_test_data.parameterized_query is not None:
            assert (
                response["data"]["parameterized_query"]
                == sql_query_test_data.parameterized_query
            )

    def test_sql_select_from_file(self):
        self.sql_via_http("use mindsdb", RESPONSE_TYPE.OK)
        resp = self.sql_via_http("select * from files.movies", RESPONSE_TYPE.TABLE)
        assert len(resp["data"]) == 10
        assert len(resp["column_names"]) == 3

        resp = self.sql_via_http("select title, title as t1, title t2 from files.movies limit 10", RESPONSE_TYPE.TABLE)
        assert len(resp["data"]) == 10
        assert resp["column_names"] == ["title", "t1", "t2"]
        assert resp["data"][0][0] == resp["data"][0][1] and resp["data"][0][0] == resp["data"][0][2]

    def test_sql_general_syntax(self):
        """test sql in general"""
        select_const_int = [
            "select 1",
            "select 1;",
            "SELECT 1",
            "Select 1",
            "   select   1   ",
            """   select
                  1;
            """,
        ]
        select_const_int_alias = [
            "select 1 as `2`",
            # "select 1 as '2'",     https://github.com/mindsdb/mindsdb_sql/issues/198
            'select 1 as "2"',
            "select 1 `2`",
            # "select 1 '2'",     https://github.com/mindsdb/mindsdb_sql/issues/198
            'select 1 "2"',
        ]
        select_const_str = ['select "a"', "select 'a'"]
        select_const_str_alias = [
            'select "a" as b',
            "select 'a' as b",
            'select "a" b',
            # 'select "a" "b"',   # => ab
            'select "a" `b`',
            # "select 'a' 'b'"    # => ab
        ]
        bunch = [
            {"queries": select_const_int, "result": 1, "alias": "1"},
            {"queries": select_const_int_alias, "result": 1, "alias": "2"},
            {"queries": select_const_str, "result": "a", "alias": "a"},
            {"queries": select_const_str_alias, "result": "a", "alias": "b"},
        ]
        for group in bunch:
            queries = group["queries"]
            expected_result = group["result"]
            expected_alias = group["alias"]
            for query in queries:
                print(query)
                resp = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
                try:
                    assert len(resp["column_names"]) == 1
                    assert resp["column_names"][0] == expected_alias
                    assert len(resp["data"]) == 1
                    assert len(resp["data"][0]) == 1
                    assert resp["data"][0][0] == expected_result
                except Exception:
                    print(f"Error in query: {query}")
                    raise

    def test_context_changing(self):
        resp = self.sql_via_http("use mindsdb", RESPONSE_TYPE.OK)
        assert resp["context"]["db"] == "mindsdb"

        resp_1 = self.sql_via_http("show tables", RESPONSE_TYPE.TABLE)
        table_names = [x[0] for x in resp_1["data"]]
        assert "movies" not in table_names
        assert "models" in table_names

        resp = self.sql_via_http("use files", RESPONSE_TYPE.OK)
        assert resp["context"]["db"] == "files"

        resp_4 = self.sql_via_http("show tables", RESPONSE_TYPE.TABLE)
        table_names = [x[0] for x in resp_4["data"]]
        assert "movies" in table_names
        assert "models" not in table_names

    @pytest.mark.parametrize(
        "query",
        [
            "show function status",
            "show function status where db = 'mindsdb'",
            "show procedure status",
            "show procedure status where db = 'mindsdb'",
            "show warnings",
        ],
    )
    def test_special_queries_empty_table(self, query):
        resp = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        assert len(resp["data"]) == 0

    @pytest.mark.parametrize(
        "query",
        [
            "show databases",
            "show schemas",
            "show variables",
            "show session status",
            "show global variables",
            "show engines",
            "show charset",
            "show collation",
        ],
    )
    def test_special_queries_not_empty_table(self, query):
        resp = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        assert len(resp["data"]) > 0

    def test_special_queries_show_databases(self):
        query = "show databases"
        resp = self.sql_via_http(query, RESPONSE_TYPE.TABLE)
        assert len(resp["column_names"]) == 1
        assert resp["column_names"][0] == "Database"
        db_names = [x[0].lower() for x in resp["data"]]
        assert "information_schema" in db_names
        assert "mindsdb" in db_names
        assert "files" in db_names

    def test_show_tables(self):
        self.sql_via_http("use mindsdb", RESPONSE_TYPE.OK)
        resp_1 = self.sql_via_http("show tables", RESPONSE_TYPE.TABLE)
        resp_2 = self.sql_via_http("show tables from mindsdb", RESPONSE_TYPE.TABLE)
        resp_3 = self.sql_via_http("show full tables from mindsdb", RESPONSE_TYPE.TABLE)
        assert resp_1["data"].sort() == resp_2["data"].sort()
        assert resp_1["data"].sort() == resp_3["data"].sort()

    def test_create_postgres_datasources(self):
        db_details = {
            "type": "postgres",
            "connection_data": {
                "host": "samples.mindsdb.com",
                "port": "5432",
                "user": "demo_user",
                "password": "demo_password",
                "database": "demo",
            },
        }
        self.create_database("test_http_postgres", db_details)
        self.validate_database_creation("test_http_postgres")

    def test_create_mariadb_datasources(self):
        db_details = {
            "type": "mariadb",
            "connection_data": {
                "type": "mariadb",
                "host": "samples.mindsdb.com",
                "port": "3307",
                "user": "demo_user",
                "password": "demo_password",
                "database": "test_data",
            },
        }
        self.create_database("test_http_mariadb", db_details)
        self.validate_database_creation("test_http_mariadb")

    def test_create_mysql_datasources(self):
        db_details = {
            "type": "mysql",
            "connection_data": {
                "type": "mysql",
                "host": "samples.mindsdb.com",
                "port": "3306",
                "user": "user",
                "password": "MindsDBUser123!",
                "database": "public",
            },
        }
        self.create_database("test_http_mysql", db_details)
        self.validate_database_creation("test_http_mysql")

    def test_sql_create_predictor(self, train_finetune_lock):
        self.sql_via_http("USE mindsdb;", RESPONSE_TYPE.OK)
        self.sql_via_http("DROP MODEL IF EXISTS p_test_http_1;", RESPONSE_TYPE.OK)

        with train_finetune_lock.acquire(timeout=600):
            self.sql_via_http(
                """
                create predictor p_test_http_1
                from test_http_postgres (select sqft, location, rental_price from demo_data.home_rentals limit 30)
                predict rental_price
            """,
                RESPONSE_TYPE.TABLE,
            )
            status = self.await_model("p_test_http_1", timeout=120)
        assert status == "complete"

        resp = self.sql_via_http(
            """
            select * from mindsdb.p_test_http_1 where sqft = 1000
        """,
            RESPONSE_TYPE.TABLE,
        )
        sqft_index = resp["column_names"].index("sqft")
        rental_price_index = resp["column_names"].index("rental_price")
        assert len(resp["data"]) == 1
        assert resp["data"][0][sqft_index] == 1000
        assert resp["data"][0][rental_price_index] > 0

    def test_list_projects(self):
        project_name = "mindsdb"
        response = self.api_request("get", "/projects")
        assert response.status_code == 200, "Error to get list of projects"

        projects = [i["name"] for i in response.json()]
        assert project_name in projects

    def test_list_models(self):
        project_name = "mindsdb"
        model_name = "p_test_http_1"
        response = self.api_request("get", f"/projects/{project_name}/models")
        assert response.status_code == 200, "Error to get list of models"
        models = [i["name"] for i in response.json()]
        assert model_name in models

    def test_make_prediction(self):
        project_name = "mindsdb"
        model_name = "p_test_http_1"
        payload = {"data": [{"sqft": "1000"}, {"sqft": "500"}]}
        response = self.api_request("post", f"/projects/{project_name}/models/{model_name}/predict", payload=payload)
        assert response.status_code == 200, "Error to make prediction"

        # 2 prediction result
        assert len(response.json()) == 2

        # 1st version of model
        response = self.api_request("post", f"/projects/{project_name}/models/{model_name}.1/predict", payload=payload)
        assert response.status_code == 200, "Error to make prediction"

        assert len(response.json()) == 2

    def test_tabs(self):
        COMPANY_1_ID = 9999998
        COMPANY_2_ID = 9999999

        def tabs_requets(
            method: str, url: str = "", payload: dict = {}, company_id: int = 1, expected_status: int = 200
        ):
            resp = self.api_request(method, f"/tabs/{url}", payload=payload, headers={"company-id": str(company_id)})
            assert resp.status_code == expected_status
            return resp

        def compare_tabs(ta: dict, tb: dict) -> bool:
            for key in ("id", "index", "name", "content"):
                if ta.get(key) != tb.get(key):
                    return False
            return True

        def compare_tabs_list(list_a: List[dict], list_b: List[dict]) -> bool:
            if len(list_a) != len(list_b):
                return False
            for i in range(len(list_a)):
                if compare_tabs(list_a[i], list_b[i]) is False:
                    return False
            return True

        def tab(company_id: int, tab_number: int):
            return {"name": f"tab_name_{company_id}_{tab_number}", "content": f"tab_content_{company_id}_{tab_number}"}

        # users has empty tabs list
        for company_id in (COMPANY_1_ID, COMPANY_2_ID):
            resp = tabs_requets("get", "?mode=new", company_id=company_id)
            # Delete all tabs to begin with
            for t in resp.json():
                tabs_requets("delete", str(t["id"]), company_id=company_id)
            # Check that all tabs are deleted
            resp = tabs_requets("get", company_id=company_id)
            assert len(resp.json()) == 0

        # add tab and check fields
        tab_1_1 = tab(COMPANY_1_ID, 1)
        tabs_requets("post", "?mode=new", payload=tab_1_1, company_id=COMPANY_1_ID)
        resp_list = tabs_requets("get", "?mode=new", company_id=COMPANY_1_ID).json()
        assert len(resp_list) == 1
        resp_1_1 = resp_list[0]
        assert resp_1_1["name"] == tab_1_1["name"]
        assert resp_1_1["content"] == tab_1_1["content"]
        assert isinstance(resp_1_1["id"], int)
        assert isinstance(resp_1_1["index"], int)
        tab_1_1["id"] = resp_1_1["id"]
        tab_1_1["index"] = resp_1_1["index"]

        # second list is empty
        resp = tabs_requets("get", "?mode=new", company_id=COMPANY_2_ID).json()
        assert len(resp) == 0

        # add tab to second user
        tab_2_1 = tab(COMPANY_2_ID, 1)
        tabs_requets("post", "?mode=new", payload=tab_2_1, company_id=COMPANY_2_ID)
        resp_list = tabs_requets("get", "?mode=new", company_id=COMPANY_2_ID).json()
        assert len(resp_list) == 1
        resp_2_1 = resp_list[0]
        assert resp_2_1["name"] == tab_2_1["name"]
        assert resp_2_1["content"] == tab_2_1["content"]
        tab_2_1["id"] = resp_2_1["id"]
        tab_2_1["index"] = resp_2_1["index"]

        # add few tabs for tests
        tab_1_2 = tab(COMPANY_1_ID, 2)
        tab_2_2 = tab(COMPANY_2_ID, 2)
        for tab_dict, company_id in ((tab_1_2, COMPANY_1_ID), (tab_2_2, COMPANY_2_ID)):
            tab_meta = tabs_requets("post", "?mode=new", payload=tab_dict, company_id=company_id).json()["tab_meta"]
            tab_dict["id"] = tab_meta["id"]
            tab_dict["index"] = tab_meta["index"]

        resp_list = tabs_requets("get", "?mode=new", company_id=COMPANY_1_ID).json()
        assert compare_tabs_list(resp_list, [tab_1_1, tab_1_2])

        resp_list = tabs_requets("get", "?mode=new", company_id=COMPANY_2_ID).json()
        assert compare_tabs_list(resp_list, [tab_2_1, tab_2_2])

        # add tab to second index
        tab_1_3 = tab(COMPANY_1_ID, 3)
        tab_1_3["index"] = tab_1_1["index"] + 1
        tab_meta = tabs_requets("post", "?mode=new", payload=tab_1_3, company_id=COMPANY_1_ID).json()["tab_meta"]
        tab_1_3["id"] = tab_meta["id"]
        tabs_list = tabs_requets("get", "?mode=new", company_id=COMPANY_1_ID).json()
        assert len(tabs_list) == 3
        tab_1_1["index"] = tabs_list[0]["index"]
        tab_1_3["index"] = tabs_list[1]["index"]
        tab_1_2["index"] = tabs_list[2]["index"]
        assert compare_tabs_list(tabs_list, [tab_1_1, tab_1_3, tab_1_2])
        assert tab_1_1["index"] < tab_1_3["index"] < tab_1_2["index"]

        # update tab content and index
        tab_1_2["index"] = tab_1_1["index"] + 1
        tab_1_2["content"] = tab_1_2["content"] + "_new"
        tab_meta = tabs_requets(
            "put",
            str(tab_1_2["id"]),
            payload={"index": tab_1_2["index"], "content": tab_1_2["content"]},
            company_id=COMPANY_1_ID,
        ).json()["tab_meta"]
        assert tab_meta["index"] == tab_1_2["index"]
        assert tab_meta["name"] == tab_1_2["name"]
        assert tab_meta["id"] == tab_1_2["id"]
        tabs_list = tabs_requets("get", "?mode=new", company_id=COMPANY_1_ID).json()
        tab_1_3["index"] = tab_1_2["index"] + 1
        assert compare_tabs_list(tabs_list, [tab_1_1, tab_1_2, tab_1_3])

        # update tab content and name
        tab_1_2["content"] = tab_1_2["content"] + "_new"
        tab_1_2["name"] = tab_1_2["name"] + "_new"
        tabs_requets(
            "put",
            str(tab_1_2["id"]),
            payload={"name": tab_1_2["name"], "content": tab_1_2["content"]},
            company_id=COMPANY_1_ID,
        )
        tabs_list = tabs_requets("get", "?mode=new", company_id=COMPANY_1_ID).json()
        assert compare_tabs_list(tabs_list, [tab_1_1, tab_1_2, tab_1_3])

        # second list does not changed
        tabs_list = tabs_requets("get", "?mode=new", company_id=COMPANY_2_ID).json()
        assert compare_tabs_list(tabs_list, [tab_2_1, tab_2_2])

        # get each tab one by one
        for company_id, tabs in ((COMPANY_1_ID, [tab_1_1, tab_1_2, tab_1_3]), (COMPANY_2_ID, [tab_2_1, tab_2_2])):
            for tab_dict in tabs:
                tab_resp = tabs_requets("get", str(tab_dict["id"]), company_id=company_id).json()
                assert compare_tabs(tab_resp, tab_dict)

        # check failures
        tabs_requets("get", "99", company_id=COMPANY_1_ID, expected_status=404)
        tabs_requets("delete", "99", company_id=COMPANY_1_ID, expected_status=404)
        tabs_requets(
            "post", "?mode=new", payload={"whaaat": "?", "name": "test"}, company_id=COMPANY_1_ID, expected_status=400
        )
        tabs_requets("put", "99", payload={"name": "test"}, company_id=COMPANY_1_ID, expected_status=404)
        tabs_requets("put", str(tab_1_1["id"]), payload={"whaaat": "?"}, company_id=COMPANY_1_ID, expected_status=400)
