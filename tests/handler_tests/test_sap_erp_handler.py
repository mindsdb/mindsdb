import importlib
import os

import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from ..unit.executor_test_base import BaseExecutorTest

try:
    importlib.import_module("requests")
    REQUESTS_INSTALLED = True
except ImportError:
    REQUESTS_INSTALLED = False


@pytest.mark.skipif(not REQUESTS_INSTALLED, reason="requests package is not installed")
class TestSAPERPHandler(BaseExecutorTest):

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    def setup_method(self):
        super().setup_method()
        self.base_url = os.environ.get("SAP_ERP_BASE_URL")
        self.api_key = os.environ.get("SAP_ERP_API_KEY")
        self.run_sql(f"""
            CREATE DATABASE sap_datasource
            WITH ENGINE = "sap_erp",
            PARAMETERS = {{
              "api_key": '{self.api_key}',
              "base_url": '{self.base_url}'
            }};
        """)

    def test_basic_select_from(self):
        sql = """
            SELECT * FROM sap_datasource.address_email_address;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.address_fax_number;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.address_home_page;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.address_phone_number;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.bp_addr_depdnt_intl_loc_number;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.bp_contact_to_address;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.bp_contact_to_func_and_dept;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.bp_credit_worthiness;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.bp_data_controller;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.bp_financial_services_extn;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.bp_financial_services_reporting;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.bp_fiscal_year_information;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.bp_relationship;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.bu_pa_address_usage;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.bu_pa_identification;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.bu_pa_industry;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.business_partner;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.business_partner_address;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.business_partner_contact;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.business_partner_payment_card;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.business_partner_rating;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.business_partner_role;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.business_partner_tax_number;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.business_partner_address_dependent_tax_number;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.cust_addr_depdnt_ext_identifier;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.customer;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.customer_company;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.customer_company_text;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.customer_dunning;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.customer_sales_area;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.customer_sales_area_tax;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.customer_sales_area_text;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.customer_tax_grouping;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.customer_text;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.customer_unloading_point;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.customer_withholding_tax;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.customer_sales_partner_func;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.customer_sales_area_addr_depdnt_info;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.customer_sales_area_addr_depdnt_tax_info;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.customer_unloading_point_addr_depdnt_info;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.supplier;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.supplier_company;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.supplier_company_text;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.supplier_dunning;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.supplier_partner_func;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.supplier_purchasing_org;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.supplier_purchasing_org_text;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.supplier_text;
        """
        self.run_sql(sql)
        sql = """
            SELECT * FROM sap_datasource.supplier_withholding_tax;
        """
        self.run_sql(sql)

    def test_complex_select(self):
        sql = """
            SELECT AddressID FROM sap_datasource.address_email_address;
        """
        assert self.run_sql(sql).shape[1] == 1
