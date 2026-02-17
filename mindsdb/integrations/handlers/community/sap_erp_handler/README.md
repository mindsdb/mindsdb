# SAP ERP Handler

This handler allows you to interact with SAP ERP API available [here](https://api.sap.com/api/API_BUSINESS_PARTNER/overview).

## About SAP ERP

SAP ERP is enterprise resource planning software developed by the company SAP SE.

## Limitations

Currently this handler only supports `SELECT` queries

## SAP ERP Handler Initialization

You can create the database via the following query:

```sql
CREATE DATABASE sap_datasource
WITH ENGINE = "sap_erp",
PARAMETERS = {
  "api_key": "...",
  "base_url": "https://sandbox.api.sap.com/s4hanacloud/sap/opu/odata/sap/API_BUSINESS_PARTNER/"
};
```

To select from various tables, you can use `SELECT` statement:

```sql
SELECT * FROM sap_datasource.address_email_address;
```

## Available tables

`address_email_address`: email address data linked to all business partner address records in the system
`address_fax_number`: fax address data linked to all the business partner address records in the system
`address_home_page`: home page url address records linked to all business partner address records in the system
`address_phone_number`: all the mobile/telephone address records linked to all the business partner address records in the system
`bp_addr_depdnt_intl_loc_number`: address dependent data for the business partner address by using the key fields business partner number and address id
`bp_contact_to_address`: workplace address records linked to all the business partner contact records in the system.
`bp_contact_to_func_and_dept`: contact person department and function data linked to all business partner contact records in the system
`bp_credit_worthiness`: contact person department and function data linked to all business partner contact records in the system
`bp_data_controller`: business partner data controllers of all the available records linked to business partners in the system
`bp_financial_services_extn`: financial services business partner attributes of all the available records linked to business partners in the system
`bp_financial_services_reporting`: financial services reporting attributes of all the available records linked to business partners in the system
`bp_fiscal_year_information`: business partner fiscal year information of all the available records linked to business partners in the system.
`bp_relationship`: business partner relationship data fields of all the available records in the system
`bu_pa_address_usage`: all the address usage records linked to all business partner address records in the system
`bu_pa_identification`: business partner identification data fields of all the records available records in the system
`bu_pa_industry`: business partner industry data fields of all the available records in the system
`business_partner`: general data fields of all the business partner records available in the system
`business_partner_address`: business partner address data fields of all the available records in the system
`business_partner_contact`: business partner contact data fields of all the available records in the system
`business_partner_payment_card`: business partner payment card data fields of all the available records in the system
`business_partner_rating`: business partner ratings of all the available records linked to business partners in the system
`business_partner_role`: business partner role data fields of all the records available records in the system
`business_partner_tax_number`: tax number data of all the available records linked to business partners in the system
`business_partner_address_dependent_tax_number`: address dependent tax number data of all the available records linked to business partners in the system
`cust_addr_depdnt_ext_identifier`: address dependent external identifiers of all the available records linked to customers in the system
`customer`: general data of all the customer records available in the system
`customer_company`: customer company data fields of all the available records in the system linked to customer
`customer_company_text`: customer company text records attached to customer company in the system
`customer_dunning`: dunning records attached to customer company in the system
`customer_sales_area`: customer sales area data fields of all the available records in the system
`customer_sales_area_tax`: customer sales area tax data fields of all the available records in the system
`customer_sales_area_text`: customer sales area text fields of all the available records in the system linked to customer sales areas
`customer_tax_grouping`: customer tax grouping data attached to a customer in the system
`customer_text`: customer text data attached to a customer in the system
`customer_unloading_point`: unloading point data attached to a customer in the system
`customer_withholding_tax`: withholding tax records attached to customer company in the system
`customer_sales_partner_func`: partner function fields of all the available records in the system linked to customer sales areas
`customer_sales_area_addr_depdnt_info`: address dependent customer sales area data fields of all the available records in the system
`customer_sales_area_addr_depdnt_tax_info`: address dependent customer sales area tax data fields of all the available records in the system
`customer_unloading_point_addr_depdnt_info`: address dependent customer unloading point data fields of all the available records in the system
`supplier`: general data of all the supplier records available in the system
`supplier_company`: supplier company data available in the system
`supplier_company_text`: supplier company text data attached to supplier company in the system
`supplier_dunning`: dunning records attached to supplier company in the system
`supplier_partner_func`: partner function fields of all the available records in the system linked to supplier purchasing organization
`supplier_purchasing_org`: supplier purchasing organization data attached to supplier records in the system
`supplier_purchasing_org_text`: supplier purchasing organization text data attached to purchasing organization in the system
`supplier_text`: supplier text data attached to purchasing organization in the system
`supplier_withholding_tax`: withholding tax records attached to supplier company in the system
