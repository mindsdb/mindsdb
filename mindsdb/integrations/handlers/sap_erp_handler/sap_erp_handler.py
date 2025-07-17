from collections import OrderedDict
from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.handlers.sap_erp_handler.api import SAPERP
from mindsdb.integrations.handlers.sap_erp_handler.sap_erp_tables import (
    AddressEmailAddressTable,
    AddressFaxNumberTable,
    AddressHomePageURLTable,
    AddressPhoneNumberTable,
    BPAddrDepdntIntlLocNumberTable,
    BPContactToAddressTable,
    BPContactToFuncAndDeptTable,
    BPCreditWorthinessTable,
    BPDataControllerTable,
    BPFinancialServicesExtnTable,
    BPFinancialServicesReportingTable,
    BPFiscalYearInformationTable,
    BPRelationshipTable,
    BuPaAddressUsageTable,
    BuPaIdentificationTable,
    BuPaIndustryTable,
    BusinessPartnerTable,
    BusinessPartnerAddressTable,
    BusinessPartnerContactTable,
    BusinessPartnerPaymentCardTable,
    BusinessPartnerRatingTable,
    BusinessPartnerRoleTable,
    BusinessPartnerTaxNumberTable,
    BusPartAddrDepdntTaxNumberTable,
    CustAddrDepdntExtIdentifierTable,
    CustAddrDepdntInformationTable,
    CustomerCompanyTable,
    CustomerCompanyTextTable,
    CustomerDunningTable,
    CustomerSalesAreaTable,
    CustomerSalesAreaTaxTable,
    CustomerSalesAreaTextTable,
    CustomerTaxGroupingTable,
    CustomerTextTable,
    CustomerUnloadingPointTable,
    CustomerWithHoldingTaxTable,
    CustSalesPartnerFuncTable,
    CustSlsAreaAddrDepdntInfoTable,
    CustSlsAreaAddrDepdntTaxInfoTable,
    CustUnldgPtAddrDepdntInfoTable,
    SupplierTable,
    SupplierCompanyTable,
    SupplierCompanyTextTable,
    SupplierDunningTable,
    SupplierPartnerFuncTable,
    SupplierPurchasingOrgTable,
    SupplierPurchasingOrgTextTable,
    SupplierTextTable,
    SupplierWithHoldingTaxTable,
)

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse


class SAPERPHandler(APIHandler):

    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(name)
        self.connection = None
        self.is_connected = False
        self.api_key = kwargs.get("connection_data", {}).get("api_key", "")
        self.base_url = kwargs.get("connection_data", {}).get("base_url", "")
        _tables = [
            AddressEmailAddressTable,
            AddressFaxNumberTable,
            AddressHomePageURLTable,
            AddressPhoneNumberTable,
            BPAddrDepdntIntlLocNumberTable,
            BPContactToAddressTable,
            BPContactToFuncAndDeptTable,
            BPCreditWorthinessTable,
            BPDataControllerTable,
            BPFinancialServicesExtnTable,
            BPFinancialServicesReportingTable,
            BPFiscalYearInformationTable,
            BPRelationshipTable,
            BuPaAddressUsageTable,
            BuPaIdentificationTable,
            BuPaIndustryTable,
            BusinessPartnerTable,
            BusinessPartnerAddressTable,
            BusinessPartnerContactTable,
            BusinessPartnerPaymentCardTable,
            BusinessPartnerRatingTable,
            BusinessPartnerRoleTable,
            BusinessPartnerTaxNumberTable,
            BusPartAddrDepdntTaxNumberTable,
            CustAddrDepdntExtIdentifierTable,
            CustAddrDepdntInformationTable,
            CustomerCompanyTable,
            CustomerCompanyTextTable,
            CustomerDunningTable,
            CustomerSalesAreaTable,
            CustomerSalesAreaTaxTable,
            CustomerSalesAreaTextTable,
            CustomerTaxGroupingTable,
            CustomerTextTable,
            CustomerUnloadingPointTable,
            CustomerWithHoldingTaxTable,
            CustSalesPartnerFuncTable,
            CustSlsAreaAddrDepdntInfoTable,
            CustSlsAreaAddrDepdntTaxInfoTable,
            CustUnldgPtAddrDepdntInfoTable,
            SupplierTable,
            SupplierCompanyTable,
            SupplierCompanyTextTable,
            SupplierDunningTable,
            SupplierPartnerFuncTable,
            SupplierPurchasingOrgTable,
            SupplierPurchasingOrgTextTable,
            SupplierTextTable,
            SupplierWithHoldingTaxTable,
        ]
        for Table in _tables:
            self._register_table(Table.name, Table(self))
        self.connect()

    def check_connection(self) -> StatusResponse:
        resp = StatusResponse(False)
        if self.connection and not self.connection.is_connected():
            resp.error = "Client not connected"
        else:
            resp.success = True
        return resp

    def connect(self) -> SAPERP:
        self.connection = SAPERP(self.base_url, self.api_key)
        return self.connection

    def native_query(self, query: str) -> StatusResponse:
        ast = parse_sql(query)
        return self.query(ast)


connection_args = OrderedDict(
    api_key={
        'type': ARG_TYPE.STR,
        'description': 'API Token for accessing SAP ERP',
        'required': True,
        'label': 'API Key',
    },
    base_url={
        'type': ARG_TYPE.STR,
        'description': 'Base URL of the host',
        'required': True,
        'label': 'Base URL',
    }
)


connection_args_example = OrderedDict(
    api_key='23d6b9e0c2fab7eba2e8b7e452cead47',
    base_url='https://sandbox.api.sap.com/s4hanacloud/sap/opu/odata/sap/API_BUSINESS_PARTNER/'
)
