from typing import List

import pandas as pd
from mindsdb_sql_parser import ast

from mindsdb.integrations.utilities.handlers.query_utilities import (
    SELECTQueryExecutor,
    SELECTQueryParser,
)
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.utilities.sql_utils import conditions_to_filter


class CustomAPITable(APITable):

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.handler.connect()

    def get_columns(self, ignore: List[str] = []) -> List[str]:
        return [item for item in self.columns if item not in ignore]

    def select(self, query: ast.Select) -> pd.DataFrame:
        raise NotImplementedError()

    def parse_select(self, query: ast.Select, table_name: str):
        select_statement_parser = SELECTQueryParser(query, table_name, self.get_columns())
        self.selected_columns, self.where_conditions, self.order_by_conditions, self.result_limit = select_statement_parser.parse_query()

    def get_where_param(self, query: ast.Select, param: str):
        params = conditions_to_filter(query.where)
        if param not in params:
            raise Exception(f"WHERE condition does not have '{param}' selector")
        return params[param]

    def apply_query_params(self, df, query):
        select_statement_parser = SELECTQueryParser(query, self.name, self.get_columns())
        selected_columns, _, order_by_conditions, result_limit = select_statement_parser.parse_query()
        select_statement_executor = SELECTQueryExecutor(df, selected_columns, [], order_by_conditions, result_limit)
        return select_statement_executor.execute_query()


class AddressEmailAddressTable(CustomAPITable):
    """Email address data linked to all business partner address records in the system"""

    name: str = "address_email_address"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "AddressID",
        "Person",
        "OrdinalNumber",
        "IsDefaultEmailAddress",
        "EmailAddress",
        "SearchEmailAddress",
        "AddressCommunicationRemarkText",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_AddressEmailAddress")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class AddressFaxNumberTable(CustomAPITable):
    """Fax address data linked to all the business partner address records in the system"""

    name: str = "address_fax_number"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "AddressID",
        "Person",
        "OrdinalNumber",
        "IsDefaultFaxNumber",
        "FaxCountry",
        "FaxNumber",
        "FaxNumberExtension",
        "InternationalFaxNumber",
        "AddressCommunicationRemarkText",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_AddressFaxNumber")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class AddressHomePageURLTable(CustomAPITable):
    """Home page URL address records linked to all business partner address records in the system"""

    name: str = "address_home_page"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "AddressID",
        "Person",
        "OrdinalNumber",
        "ValidityStartDate",
        "IsDefaultURLAddress",
        "SearchURLAddress",
        "AddressCommunicationRemarkText",
        "URLFieldLength",
        "WebsiteURL",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_AddressHomePageURL")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class AddressPhoneNumberTable(CustomAPITable):
    """All the mobile/telephone address records linked to all the business partner address records in the system"""

    name: str = "address_phone_number"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "AddressID",
        "Person",
        "OrdinalNumber",
        "DestinationLocationCountry",
        "IsDefaultPhoneNumber",
        "PhoneNumber",
        "PhoneNumberExtension",
        "InternationalPhoneNumber",
        "PhoneNumberType",
        "AddressCommunicationRemarkText",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_AddressPhoneNumber")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BPAddrDepdntIntlLocNumberTable(CustomAPITable):
    """address dependent data for the business partner address by using the key fields business partner number and address ID"""

    name: str = "bp_addr_depdnt_intl_loc_number"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "BusinessPartner",
        "AddressID",
        "InternationalLocationNumber1",
        "InternationalLocationNumber2",
        "InternationalLocationNumber3",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BPAddrDepdntIntlLocNumber")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BPContactToAddressTable(CustomAPITable):
    """Workplace address records linked to all the business partner contact records in the system."""

    name: str = "bp_contact_to_address"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "RelationshipNumber",
        "BusinessPartnerCompany",
        "BusinessPartnerPerson",
        "ValidityEndDate",
        "AddressID",
        "AddressNumber",
        "AdditionalStreetPrefixName",
        "AdditionalStreetSuffixName",
        "AddressTimeZone",
        "CareOfName",
        "CityCode",
        "CityName",
        "CompanyPostalCode",
        "Country",
        "County",
        "DeliveryServiceNumber",
        "DeliveryServiceTypeCode",
        "District",
        "FormOfAddress",
        "FullName",
        "HomeCityName",
        "HouseNumber",
        "HouseNumberSupplementText",
        "Language",
        "POBox",
        "POBoxDeviatingCityName",
        "POBoxDeviatingCountry",
        "POBoxDeviatingRegion",
        "POBoxIsWithoutNumber",
        "POBoxLobbyName",
        "POBoxPostalCode",
        "Person",
        "PostalCode",
        "PrfrdCommMediumType",
        "Region",
        "StreetName",
        "StreetPrefixName",
        "StreetSuffixName",
        "TaxJurisdiction",
        "TransportZone",
        "AddressRepresentationCode",
        "ContactPersonBuilding",
        "ContactPersonPrfrdCommMedium",
        "ContactRelationshipDepartment",
        "ContactRelationshipFunction",
        "CorrespondenceShortName",
        "Floor",
        "InhouseMail",
        "IsDefaultAddress",
        "RoomNumber",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BPContactToAddress")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BPContactToFuncAndDeptTable(CustomAPITable):
    """Contact person department and function data linked to all business partner contact records in the system"""

    name: str = "bp_contact_to_func_and_dept"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "RelationshipNumber",
        "BusinessPartnerCompany",
        "BusinessPartnerPerson",
        "ValidityEndDate",
        "ContactPersonAuthorityType",
        "ContactPersonDepartment",
        "ContactPersonDepartmentName",
        "ContactPersonFunction",
        "ContactPersonFunctionName",
        "ContactPersonRemarkText",
        "ContactPersonVIPType",
        "EmailAddress",
        "FaxNumber",
        "FaxNumberExtension",
        "PhoneNumber",
        "PhoneNumberExtension",
        "RelationshipCategory",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BPContactToFuncAndDept")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BPCreditWorthinessTable(CustomAPITable):
    """Contact person department and function data linked to all business partner contact records in the system"""

    name: str = "bp_credit_worthiness"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "BusinessPartner",
        "BusPartCreditStanding",
        "BPCreditStandingStatus",
        "CreditRatingAgency",
        "BPCreditStandingComment",
        "BPCreditStandingDate",
        "BPCreditStandingRating",
        "BPLegalProceedingStatus",
        "BPLglProceedingInitiationDate",
        "BusinessPartnerIsUnderOath",
        "BusinessPartnerOathDate",
        "BusinessPartnerIsBankrupt",
        "BusinessPartnerBankruptcyDate",
        "BPForeclosureIsInitiated",
        "BPForeclosureDate",
        "BPCrdtWrthnssAccessChkIsActive",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BPCreditWorthiness")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BPDataControllerTable(CustomAPITable):
    """Business partner data controllers of all the available records linked to business partners in the system"""

    name: str = "bp_data_controller"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "BusinessPartner",
        "DataController",
        "PurposeForPersonalData",
        "DataControlAssignmentStatus",
        "BPDataControllerIsDerived",
        "PurposeDerived",
        "PurposeType",
        "BusinessPurposeFlag",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BPDataController")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BPFinancialServicesExtnTable(CustomAPITable):
    """Financial services business partner attributes of all the available records linked to business partners in the system"""

    name: str = "bp_financial_services_extn"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "BusinessPartner",
        "BusinessPartnerIsVIP",
        "TradingPartner",
        "FactoryCalendar",
        "BusinessPartnerOfficeCountry",
        "BusinessPartnerOfficeRegion",
        "BPRegisteredOfficeName",
        "BPBalanceSheetCurrency",
        "BPLastCptlIncrAmtInBalShtCrcy",
        "BPLastCapitalIncreaseYear",
        "BPBalanceSheetDisplayType",
        "BusinessPartnerCitizenship",
        "BPMaritalPropertyRegime",
        "BusinessPartnerIncomeCurrency",
        "BPNumberOfChildren",
        "BPNumberOfHouseholdMembers",
        "BPAnnualNetIncAmtInIncomeCrcy",
        "BPMonthlyNetIncAmtInIncomeCrcy",
        "BPAnnualNetIncomeYear",
        "BPMonthlyNetIncomeMonth",
        "BPMonthlyNetIncomeYear",
        "BPPlaceOfDeathName",
        "CustomerIsUnwanted",
        "UndesirabilityReason",
        "UndesirabilityComment",
        "LastCustomerContactDate",
        "BPGroupingCharacter",
        "BPLetterSalutation",
        "BusinessPartnerTargetGroup",
        "BusinessPartnerEmployeeGroup",
        "BusinessPartnerIsEmployee",
        "BPTermnBusRelationsBankDate",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BPFinancialServicesExtn")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BPFinancialServicesReportingTable(CustomAPITable):
    """Financial services reporting attributes of all the available records linked to business partners in the system"""

    name: str = "bp_financial_services_reporting"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "BusinessPartner",
        "BPIsNonResident",
        "BPNonResidencyStartDate",
        "BPIsMultimillionLoanRecipient",
        "BPLoanReportingBorrowerNumber",
        "BPLoanRptgBorrowerEntityNumber",
        "BPCreditStandingReview",
        "BPCreditStandingReviewDate",
        "BusinessPartnerLoanToManager",
        "BPCompanyRelationship",
        "BPLoanReportingCreditorNumber",
        "BPOeNBIdentNumber",
        "BPOeNBTargetGroup",
        "BPOeNBIdentNumberAssigned",
        "BPOeNBInstituteNumber",
        "BusinessPartnerIsOeNBInstitute",
        "BusinessPartnerGroup",
        "BPGroupAssignmentCategory",
        "BusinessPartnerGroupName",
        "BusinessPartnerLegalEntity",
        "BPGerAstRglnRestrictedAstQuota",
        "BusinessPartnerDebtorGroup",
        "BusinessPartnerBusinessPurpose",
        "BusinessPartnerRiskGroup",
        "BPRiskGroupingDate",
        "BPHasGroupAffiliation",
        "BPIsMonetaryFinInstitution",
        "BPCrdtStandingReviewIsRequired",
        "BPLoanMonitoringIsRequired",
        "BPHasCreditingRelief",
        "BPInvestInRstrcdAstIsAuthzd",
        "BPCentralBankCountryRegion",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BPFinancialServicesReporting")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BPFiscalYearInformationTable(CustomAPITable):
    """Business partner fiscal year information of all the available records linked to business partners in the system."""

    name: str = "bp_fiscal_year_information"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "BusinessPartner",
        "BusinessPartnerFiscalYear",
        "BPBalanceSheetCurrency",
        "BPAnnualStockholderMeetingDate",
        "BPFiscalYearStartDate",
        "BPFiscalYearEndDate",
        "BPFiscalYearIsClosed",
        "BPFiscalYearClosingDate",
        "BPFsclYrCnsldtdFinStatementDte",
        "BPCapitalStockAmtInBalShtCrcy",
        "BPIssdStockCptlAmtInBalShtCrcy",
        "BPPartcipnCertAmtInBalShtCrcy",
        "BPEquityCapitalAmtInBalShtCrcy",
        "BPGrossPremiumAmtInBalShtCrcy",
        "BPNetPremiumAmtInBalShtCrcy",
        "BPAnnualSalesAmtInBalShtCrcy",
        "BPAnnualNetIncAmtInBalShtCrcy",
        "BPDividendDistrAmtInBalShtCrcy",
        "BPDebtRatioInYears",
        "BPAnnualPnLAmtInBalShtCrcy",
        "BPBalSheetTotalAmtInBalShtCrcy",
        "BPNumberOfEmployees",
        "BPCptlReserveAmtInBalShtCrcy",
        "BPLglRevnRsrvAmtInBalShtCrcy",
        "RevnRsrvOwnStkAmtInBalShtCrcy",
        "BPStatryReserveAmtInBalShtCrcy",
        "BPOthRevnRsrvAmtInBalShtCrcy",
        "BPPnLCarryfwdAmtInBalShtCrcy",
        "BPSuborddLbltyAmtInBalShtCrcy",
        "BPRetOnTotalCptlEmpldInPercent",
        "BPDebtClearancePeriodInYears",
        "BPFinancingCoeffInPercent",
        "BPEquityRatioInPercent",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BPFiscalYearInformation")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BPRelationshipTable(CustomAPITable):
    """Business partner relationship data fields of all the available records in the system"""

    name: str = "bp_relationship"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "RelationshipNumber",
        "BusinessPartner1",
        "BusinessPartner2",
        "ValidityEndDate",
        "ValidityStartDate",
        "IsStandardRelationship",
        "RelationshipCategory",
        "BPRelationshipType",
        "CreatedByUser",
        "CreationDate",
        "CreationTime",
        "LastChangedByUser",
        "LastChangeDate",
        "LastChangeTime",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BPRelationship")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BuPaAddressUsageTable(CustomAPITable):
    """All the address usage records linked to all business partner address records in the system"""

    name: str = "bu_pa_address_usage"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "BusinessPartner",
        "ValidityEndDate",
        "AddressUsage",
        "AddressID",
        "ValidityStartDate",
        "StandardUsage",
        "AuthorizationGroup",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BuPaAddressUsage")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BuPaIdentificationTable(CustomAPITable):
    """Business partner identification data fields of all the records available records in the system"""

    name: str = "bu_pa_identification"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "BusinessPartner",
        "BPIdentificationType",
        "BPIdentificationNumber",
        "BPIdnNmbrIssuingInstitute",
        "BPIdentificationEntryDate",
        "Country",
        "Region",
        "ValidityStartDate",
        "ValidityEndDate",
        "AuthorizationGroup",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BuPaIdentification")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BuPaIndustryTable(CustomAPITable):
    """Business partner industry data fields of all the available records in the system"""

    name: str = "bu_pa_industry"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "IndustrySector",
        "IndustrySystemType",
        "BusinessPartner",
        "IsStandardIndustry",
        "IndustryKeyDescription",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BuPaIndustry")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BusinessPartnerTable(CustomAPITable):
    """General data fields of all the business partner records available in the system"""

    name: str = "business_partner"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "BusinessPartner",
        "Customer",
        "Supplier",
        "AcademicTitle",
        "AuthorizationGroup",
        "BusinessPartnerCategory",
        "BusinessPartnerFullName",
        "BusinessPartnerGrouping",
        "BusinessPartnerName",
        "BusinessPartnerUUID",
        "CorrespondenceLanguage",
        "CreatedByUser",
        "CreationDate",
        "CreationTime",
        "FirstName",
        "FormOfAddress",
        "Industry",
        "InternationalLocationNumber1",
        "InternationalLocationNumber2",
        "IsFemale",
        "IsMale",
        "IsNaturalPerson",
        "IsSexUnknown",
        "GenderCodeName",
        "Language",
        "LastChangeDate",
        "LastChangeTime",
        "LastChangedByUser",
        "LastName",
        "LegalForm",
        "OrganizationBPName1",
        "OrganizationBPName2",
        "OrganizationBPName3",
        "OrganizationBPName4",
        "OrganizationFoundationDate",
        "OrganizationLiquidationDate",
        "SearchTerm1",
        "SearchTerm2",
        "AdditionalLastName",
        "BirthDate",
        "BusinessPartnerBirthDateStatus",
        "BusinessPartnerBirthplaceName",
        "BusinessPartnerDeathDate",
        "BusinessPartnerIsBlocked",
        "BusinessPartnerType",
        "ETag",
        "GroupBusinessPartnerName1",
        "GroupBusinessPartnerName2",
        "IndependentAddressID",
        "InternationalLocationNumber3",
        "MiddleName",
        "NameCountry",
        "NameFormat",
        "PersonFullName",
        "PersonNumber",
        "IsMarkedForArchiving",
        "BusinessPartnerIDByExtSystem",
        "BusinessPartnerPrintFormat",
        "BusinessPartnerOccupation",
        "BusPartMaritalStatus",
        "BusPartNationality",
        "BusinessPartnerBirthName",
        "BusinessPartnerSupplementName",
        "NaturalPersonEmployerName",
        "LastNamePrefix",
        "LastNameSecondPrefix",
        "Initials",
        "BPDataControllerIsNotRequired",
        "TradingPartner",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BusinessPartner")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BusinessPartnerAddressTable(CustomAPITable):
    """Business partner address data fields of all the available records in the system"""

    name: str = "business_partner_address"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "BusinessPartner",
        "AddressID",
        "ValidityStartDate",
        "ValidityEndDate",
        "AuthorizationGroup",
        "AddressUUID",
        "AdditionalStreetPrefixName",
        "AdditionalStreetSuffixName",
        "AddressTimeZone",
        "CareOfName",
        "CityCode",
        "CityName",
        "CompanyPostalCode",
        "Country",
        "County",
        "DeliveryServiceNumber",
        "DeliveryServiceTypeCode",
        "District",
        "FormOfAddress",
        "FullName",
        "HomeCityName",
        "HouseNumber",
        "HouseNumberSupplementText",
        "Language",
        "POBox",
        "POBoxDeviatingCityName",
        "POBoxDeviatingCountry",
        "POBoxDeviatingRegion",
        "POBoxIsWithoutNumber",
        "POBoxLobbyName",
        "POBoxPostalCode",
        "Person",
        "PostalCode",
        "PrfrdCommMediumType",
        "Region",
        "StreetName",
        "StreetPrefixName",
        "StreetSuffixName",
        "TaxJurisdiction",
        "TransportZone",
        "AddressIDByExternalSystem",
        "CountyCode",
        "TownshipCode",
        "TownshipName",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BusinessPartnerAddress")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BusinessPartnerContactTable(CustomAPITable):
    """Business partner contact data fields of all the available records in the system"""

    name: str = "business_partner_contact"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "RelationshipNumber",
        "BusinessPartnerCompany",
        "BusinessPartnerPerson",
        "ValidityEndDate",
        "ValidityStartDate",
        "IsStandardRelationship",
        "RelationshipCategory",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BusinessPartnerContact")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BusinessPartnerPaymentCardTable(CustomAPITable):
    """Business partner payment card data fields of all the available records in the system"""

    name: str = "business_partner_payment_card"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "BusinessPartner",
        "PaymentCardID",
        "PaymentCardType",
        "CardNumber",
        "IsStandardCard",
        "CardDescription",
        "ValidityDate",
        "ValidityEndDate",
        "CardHolder",
        "CardIssuingBank",
        "CardIssueDate",
        "PaymentCardLock",
        "MaskedCardNumber",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BusinessPartnerPaymentCard")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BusinessPartnerRatingTable(CustomAPITable):
    """Business partner ratings of all the available records linked to business partners in the system"""

    name: str = "business_partner_rating"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "BusinessPartner",
        "BusinessPartnerRatingProcedure",
        "BPRatingValidityEndDate",
        "BusinessPartnerRatingGrade",
        "BusinessPartnerRatingTrend",
        "BPRatingValidityStartDate",
        "BPRatingCreationDate",
        "BusinessPartnerRatingComment",
        "BusinessPartnerRatingIsAllowed",
        "BPRatingIsValidOnKeyDate",
        "BusinessPartnerRatingKeyDate",
        "BusinessPartnerRatingIsExpired",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BusinessPartnerRating")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BusinessPartnerRoleTable(CustomAPITable):
    """Business partner role data fields of all the records available records in the system"""

    name: str = "business_partner_role"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "BusinessPartner",
        "BusinessPartnerRole",
        "ValidFrom",
        "ValidTo",
        "AuthorizationGroup",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BusinessPartnerRole")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BusinessPartnerTaxNumberTable(CustomAPITable):
    """Tax number data of all the available records linked to business partners in the system"""

    name: str = "business_partner_tax_number"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "BusinessPartner",
        "BPTaxType",
        "BPTaxNumber",
        "BPTaxLongNumber",
        "AuthorizationGroup",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BusinessPartnerTaxNumber")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class BusPartAddrDepdntTaxNumberTable(CustomAPITable):
    """Address dependent tax number data of all the available records linked to business partners in the system"""

    name: str = "business_partner_address_dependent_tax_number"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "BusinessPartner",
        "AddressID",
        "BPTaxType",
        "BPTaxNumber",
        "BPTaxLongNumber",
        "AuthorizationGroup",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_BusPartAddrDepdntTaxNmbr")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class CustAddrDepdntExtIdentifierTable(CustomAPITable):
    """Address dependent external identifiers of all the available records linked to customers in the system"""

    name: str = "cust_addr_depdnt_ext_identifier"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Customer",
        "AddressID",
        "CustomerExternalRefID",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_CustAddrDepdntExtIdentifier")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class CustAddrDepdntInformationTable(CustomAPITable):
    """General data of all the customer records available in the system"""

    name: str = "customer"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Customer",
        "AuthorizationGroup",
        "BillingIsBlockedForCustomer",
        "CreatedByUser",
        "CreationDate",
        "CustomerAccountGroup",
        "CustomerClassification",
        "CustomerFullName",
        "BPCustomerFullName",
        "CustomerName",
        "BPCustomerName",
        "DeliveryIsBlocked",
        "FreeDefinedAttribute01",
        "FreeDefinedAttribute02",
        "FreeDefinedAttribute03",
        "FreeDefinedAttribute04",
        "FreeDefinedAttribute05",
        "FreeDefinedAttribute06",
        "FreeDefinedAttribute07",
        "FreeDefinedAttribute08",
        "FreeDefinedAttribute09",
        "FreeDefinedAttribute10",
        "NFPartnerIsNaturalPerson",
        "OrderIsBlockedForCustomer",
        "PostingIsBlocked",
        "Supplier",
        "CustomerCorporateGroup",
        "FiscalAddress",
        "Industry",
        "IndustryCode1",
        "IndustryCode2",
        "IndustryCode3",
        "IndustryCode4",
        "IndustryCode5",
        "InternationalLocationNumber1",
        "InternationalLocationNumber2",
        "InternationalLocationNumber3",
        "NielsenRegion",
        "PaymentReason",
        "ResponsibleType",
        "TaxNumber1",
        "TaxNumber2",
        "TaxNumber3",
        "TaxNumber4",
        "TaxNumber5",
        "TaxNumberType",
        "VATRegistration",
        "DeletionIndicator",
        "ExpressTrainStationName",
        "TrainStationName",
        "CityCode",
        "County",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_Customer")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class CustomerCompanyTable(CustomAPITable):
    """Customer company data fields of all the available records in the system linked to customer"""

    name: str = "customer_company"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Customer",
        "CompanyCode",
        "APARToleranceGroup",
        "AccountByCustomer",
        "AccountingClerk",
        "AccountingClerkFaxNumber",
        "AccountingClerkInternetAddress",
        "AccountingClerkPhoneNumber",
        "AlternativePayerAccount",
        "AuthorizationGroup",
        "CollectiveInvoiceVariant",
        "CustomerAccountNote",
        "CustomerHeadOffice",
        "CustomerSupplierClearingIsUsed",
        "HouseBank",
        "InterestCalculationCode",
        "InterestCalculationDate",
        "IntrstCalcFrequencyInMonths",
        "IsToBeLocallyProcessed",
        "ItemIsToBePaidSeparately",
        "LayoutSortingRule",
        "PaymentBlockingReason",
        "PaymentMethodsList",
        "PaymentReason",
        "PaymentTerms",
        "PaytAdviceIsSentbyEDI",
        "PhysicalInventoryBlockInd",
        "ReconciliationAccount",
        "RecordPaymentHistoryIndicator",
        "UserAtCustomer",
        "DeletionIndicator",
        "CashPlanningGroup",
        "KnownOrNegotiatedLeave",
        "ValueAdjustmentKey",
        "CustomerAccountGroup",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_CustomerCompany")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class CustomerCompanyTextTable(CustomAPITable):
    """Customer company text records attached to customer company in the system"""

    name: str = "customer_company_text"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Customer",
        "CompanyCode",
        "Language",
        "LongTextID",
        "LongText",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_CustomerCompanyText")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class CustomerDunningTable(CustomAPITable):
    """Dunning records attached to customer company in the system"""

    name: str = "customer_dunning"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Customer",
        "CompanyCode",
        "DunningArea",
        "DunningBlock",
        "DunningLevel",
        "DunningProcedure",
        "DunningRecipient",
        "LastDunnedOn",
        "LegDunningProcedureOn",
        "DunningClerk",
        "AuthorizationGroup",
        "CustomerAccountGroup",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_CustomerDunning")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class CustomerSalesAreaTable(CustomAPITable):
    """Customer sales area data fields of all the available records in the system"""

    name: str = "customer_sales_area"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Customer",
        "SalesOrganization",
        "DistributionChannel",
        "Division",
        "AccountByCustomer",
        "AuthorizationGroup",
        "BillingIsBlockedForCustomer",
        "CompleteDeliveryIsDefined",
        "CreditControlArea",
        "Currency",
        "CustIsRlvtForSettlmtMgmt",
        "CustomerABCClassification",
        "CustomerAccountAssignmentGroup",
        "CustomerGroup",
        "CustomerIsRebateRelevant",
        "CustomerPaymentTerms",
        "CustomerPriceGroup",
        "CustomerPricingProcedure",
        "CustProdProposalProcedure",
        "DeliveryIsBlockedForCustomer",
        "DeliveryPriority",
        "IncotermsClassification",
        "IncotermsLocation2",
        "IncotermsVersion",
        "IncotermsLocation1",
        "IncotermsSupChnLoc1AddlUUID",
        "IncotermsSupChnLoc2AddlUUID",
        "IncotermsSupChnDvtgLocAddlUUID",
        "DeletionIndicator",
        "IncotermsTransferLocation",
        "InspSbstHasNoTimeOrQuantity",
        "InvoiceDate",
        "ItemOrderProbabilityInPercent",
        "ManualInvoiceMaintIsRelevant",
        "MaxNmbrOfPartialDelivery",
        "OrderCombinationIsAllowed",
        "OrderIsBlockedForCustomer",
        "OverdelivTolrtdLmtRatioInPct",
        "PartialDeliveryIsAllowed",
        "PriceListType",
        "ProductUnitGroup",
        "ProofOfDeliveryTimeValue",
        "SalesGroup",
        "SalesItemProposal",
        "SalesOffice",
        "ShippingCondition",
        "SlsDocIsRlvtForProofOfDeliv",
        "SlsUnlmtdOvrdelivIsAllwd",
        "SupplyingPlant",
        "SalesDistrict",
        "UnderdelivTolrtdLmtRatioInPct",
        "InvoiceListSchedule",
        "ExchangeRateType",
        "AdditionalCustomerGroup1",
        "AdditionalCustomerGroup2",
        "AdditionalCustomerGroup3",
        "AdditionalCustomerGroup4",
        "AdditionalCustomerGroup5",
        "PaymentGuaranteeProcedure",
        "CustomerAccountGroup",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_CustomerSalesArea")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class CustomerSalesAreaTaxTable(CustomAPITable):
    """Customer sales area tax data fields of all the available records in the system"""

    name: str = "customer_sales_area_tax"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Customer",
        "SalesOrganization",
        "DistributionChannel",
        "Division",
        "DepartureCountry",
        "CustomerTaxCategory",
        "CustomerTaxClassification",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_CustomerSalesAreaTax")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class CustomerSalesAreaTextTable(CustomAPITable):
    """Customer sales area text fields of all the available records in the system linked to customer sales areas"""

    name: str = "customer_sales_area_text"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Customer",
        "SalesOrganization",
        "DistributionChannel",
        "Division",
        "Language",
        "LongTextID",
        "LongText",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_CustomerSalesAreaText")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class CustomerTaxGroupingTable(CustomAPITable):
    """Customer tax grouping data attached to a customer in the system"""

    name: str = "customer_tax_grouping"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Customer",
        "CustomerTaxGroupingCode",
        "CustTaxGrpExemptionCertificate",
        "CustTaxGroupExemptionRate",
        "CustTaxGroupExemptionStartDate",
        "CustTaxGroupExemptionEndDate",
        "CustTaxGroupSubjectedStartDate",
        "CustTaxGroupSubjectedEndDate",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_CustomerTaxGrouping")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class CustomerTextTable(CustomAPITable):
    """Customer text data attached to a customer in the system"""

    name: str = "customer_text"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Customer",
        "Language",
        "LongTextID",
        "LongText",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_CustomerText")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class CustomerUnloadingPointTable(CustomAPITable):
    """Unloading point data attached to a customer in the system"""

    name: str = "customer_unloading_point"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Customer",
        "UnloadingPointName",
        "CustomerFactoryCalenderCode",
        "BPGoodsReceivingHoursCode",
        "IsDfltBPUnloadingPoint",
        "MondayMorningOpeningTime",
        "MondayMorningClosingTime",
        "MondayAfternoonOpeningTime",
        "MondayAfternoonClosingTime",
        "TuesdayMorningOpeningTime",
        "TuesdayMorningClosingTime",
        "TuesdayAfternoonOpeningTime",
        "TuesdayAfternoonClosingTime",
        "WednesdayMorningOpeningTime",
        "WednesdayMorningClosingTime",
        "WednesdayAfternoonOpeningTime",
        "WednesdayAfternoonClosingTime",
        "ThursdayMorningOpeningTime",
        "ThursdayMorningClosingTime",
        "ThursdayAfternoonOpeningTime",
        "ThursdayAfternoonClosingTime",
        "FridayMorningOpeningTime",
        "FridayMorningClosingTime",
        "FridayAfternoonOpeningTime",
        "FridayAfternoonClosingTime",
        "SaturdayMorningOpeningTime",
        "SaturdayMorningClosingTime",
        "SaturdayAfternoonOpeningTime",
        "SaturdayAfternoonClosingTime",
        "SundayMorningOpeningTime",
        "SundayMorningClosingTime",
        "SundayAfternoonOpeningTime",
        "SundayAfternoonClosingTime",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_CustomerUnloadingPoint")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class CustomerWithHoldingTaxTable(CustomAPITable):
    """Withholding tax records attached to customer company in the system"""

    name: str = "customer_withholding_tax"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Customer",
        "CompanyCode",
        "WithholdingTaxType",
        "WithholdingTaxCode",
        "WithholdingTaxAgent",
        "ObligationDateBegin",
        "ObligationDateEnd",
        "WithholdingTaxNumber",
        "WithholdingTaxCertificate",
        "WithholdingTaxExmptPercent",
        "ExemptionDateBegin",
        "ExemptionDateEnd",
        "ExemptionReason",
        "AuthorizationGroup",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_CustomerWithHoldingTax")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class CustSalesPartnerFuncTable(CustomAPITable):
    """Partner function fields of all the available records in the system linked to customer sales areas"""

    name: str = "customer_sales_partner_func"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Customer",
        "SalesOrganization",
        "DistributionChannel",
        "Division",
        "PartnerCounter",
        "PartnerFunction",
        "BPCustomerNumber",
        "CustomerPartnerDescription",
        "DefaultPartner",
        "Supplier",
        "PersonnelNumber",
        "ContactPerson",
        "AddressID",
        "AuthorizationGroup",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_CustSalesPartnerFunc")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class CustSlsAreaAddrDepdntInfoTable(CustomAPITable):
    """Address dependent customer sales area data fields of all the available records in the system"""

    name: str = "customer_sales_area_addr_depdnt_info"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Customer",
        "SalesOrganization",
        "DistributionChannel",
        "Division",
        "AddressID",
        "IncotermsClassification",
        "IncotermsLocation1",
        "IncotermsLocation2",
        "IncotermsSupChnLoc1AddlUUID",
        "IncotermsSupChnLoc2AddlUUID",
        "IncotermsSupChnDvtgLocAddlUUID",
        "DeliveryIsBlocked",
        "SalesOffice",
        "SalesGroup",
        "ShippingCondition",
        "SupplyingPlant",
        "IncotermsVersion",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_CustSlsAreaAddrDepdntInfo")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class CustSlsAreaAddrDepdntTaxInfoTable(CustomAPITable):
    """Address dependent customer sales area tax data fields of all the available records in the system"""

    name: str = "customer_sales_area_addr_depdnt_tax_info"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Customer",
        "SalesOrganization",
        "DistributionChannel",
        "Division",
        "AddressID",
        "DepartureCountry",
        "CustomerTaxCategory",
        "CustomerTaxClassification",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_CustSlsAreaAddrDepdntTaxInfo")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class CustUnldgPtAddrDepdntInfoTable(CustomAPITable):
    """Address dependent customer unloading point data fields of all the available records in the system"""

    name: str = "customer_unloading_point_addr_depdnt_info"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Customer",
        "AddressID",
        "UnloadingPointName",
        "CustomerFactoryCalenderCode",
        "BPGoodsReceivingHoursCode",
        "IsDfltBPUnloadingPoint",
        "MondayMorningOpeningTime",
        "MondayMorningClosingTime",
        "MondayAfternoonOpeningTime",
        "MondayAfternoonClosingTime",
        "TuesdayMorningOpeningTime",
        "TuesdayMorningClosingTime",
        "TuesdayAfternoonOpeningTime",
        "TuesdayAfternoonClosingTime",
        "WednesdayMorningOpeningTime",
        "WednesdayMorningClosingTime",
        "WednesdayAfternoonOpeningTime",
        "WednesdayAfternoonClosingTime",
        "ThursdayMorningOpeningTime",
        "ThursdayMorningClosingTime",
        "ThursdayAfternoonOpeningTime",
        "ThursdayAfternoonClosingTime",
        "FridayMorningOpeningTime",
        "FridayMorningClosingTime",
        "FridayAfternoonOpeningTime",
        "FridayAfternoonClosingTime",
        "SaturdayMorningOpeningTime",
        "SaturdayMorningClosingTime",
        "SaturdayAfternoonOpeningTime",
        "SaturdayAfternoonClosingTime",
        "SundayMorningOpeningTime",
        "SundayMorningClosingTime",
        "SundayAfternoonOpeningTime",
        "SundayAfternoonClosingTime",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_CustUnldgPtAddrDepdntInfo")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class SupplierTable(CustomAPITable):
    """General data of all the supplier records available in the system"""

    name: str = "supplier"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Supplier",
        "AlternativePayeeAccountNumber",
        "AuthorizationGroup",
        "CreatedByUser",
        "CreationDate",
        "Customer",
        "PaymentIsBlockedForSupplier",
        "PostingIsBlocked",
        "PurchasingIsBlocked",
        "SupplierAccountGroup",
        "SupplierFullName",
        "SupplierName",
        "VATRegistration",
        "BirthDate",
        "ConcatenatedInternationalLocNo",
        "DeletionIndicator",
        "FiscalAddress",
        "Industry",
        "InternationalLocationNumber1",
        "InternationalLocationNumber2",
        "InternationalLocationNumber3",
        "IsNaturalPerson",
        "PaymentReason",
        "ResponsibleType",
        "SuplrQltyInProcmtCertfnValidTo",
        "SuplrQualityManagementSystem",
        "SupplierCorporateGroup",
        "SupplierProcurementBlock",
        "TaxNumber1",
        "TaxNumber2",
        "TaxNumber3",
        "TaxNumber4",
        "TaxNumber5",
        "TaxNumberResponsible",
        "TaxNumberType",
        "SuplrProofOfDelivRlvtCode",
        "BR_TaxIsSplit",
        "DataExchangeInstructionKey",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_Supplier")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class SupplierCompanyTable(CustomAPITable):
    """supplier company data available in the system"""

    name: str = "supplier_company"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Supplier",
        "CompanyCode",
        "AuthorizationGroup",
        "CompanyCodeName",
        "PaymentBlockingReason",
        "SupplierIsBlockedForPosting",
        "AccountingClerk",
        "AccountingClerkFaxNumber",
        "AccountingClerkPhoneNumber",
        "SupplierClerk",
        "SupplierClerkURL",
        "PaymentMethodsList",
        "PaymentReason",
        "PaymentTerms",
        "ClearCustomerSupplier",
        "IsToBeLocallyProcessed",
        "ItemIsToBePaidSeparately",
        "PaymentIsToBeSentByEDI",
        "HouseBank",
        "CheckPaidDurationInDays",
        "Currency",
        "BillOfExchLmtAmtInCoCodeCrcy",
        "SupplierClerkIDBySupplier",
        "ReconciliationAccount",
        "InterestCalculationCode",
        "InterestCalculationDate",
        "IntrstCalcFrequencyInMonths",
        "SupplierHeadOffice",
        "AlternativePayee",
        "LayoutSortingRule",
        "APARToleranceGroup",
        "SupplierCertificationDate",
        "SupplierAccountNote",
        "WithholdingTaxCountry",
        "DeletionIndicator",
        "CashPlanningGroup",
        "IsToBeCheckedForDuplicates",
        "MinorityGroup",
        "SupplierAccountGroup",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_SupplierCompany")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class SupplierCompanyTextTable(CustomAPITable):
    """Supplier company text data attached to supplier company in the system"""

    name: str = "supplier_company_text"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Supplier",
        "CompanyCode",
        "Language",
        "LongTextID",
        "LongText",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_SupplierCompanyText")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class SupplierDunningTable(CustomAPITable):
    """Dunning records attached to supplier company in the system"""

    name: str = "supplier_dunning"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Supplier",
        "CompanyCode",
        "DunningArea",
        "DunningBlock",
        "DunningLevel",
        "DunningProcedure",
        "DunningRecipient",
        "LastDunnedOn",
        "LegDunningProcedureOn",
        "DunningClerk",
        "AuthorizationGroup",
        "SupplierAccountGroup",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_SupplierDunning")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class SupplierPartnerFuncTable(CustomAPITable):
    """Partner function fields of all the available records in the system linked to supplier purchasing organization"""

    name: str = "supplier_partner_func"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Supplier",
        "PurchasingOrganization",
        "SupplierSubrange",
        "Plant",
        "PartnerFunction",
        "PartnerCounter",
        "DefaultPartner",
        "CreationDate",
        "CreatedByUser",
        "ReferenceSupplier",
        "AuthorizationGroup",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_SupplierPartnerFunc")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class SupplierPurchasingOrgTable(CustomAPITable):
    """Supplier purchasing organization data attached to supplier records in the system"""

    name: str = "supplier_purchasing_org"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Supplier",
        "PurchasingOrganization",
        "AutomaticEvaluatedRcptSettlmt",
        "CalculationSchemaGroupCode",
        "DeletionIndicator",
        "EvaldReceiptSettlementIsActive",
        "IncotermsClassification",
        "IncotermsTransferLocation",
        "IncotermsVersion",
        "IncotermsLocation1",
        "IncotermsLocation2",
        "IncotermsSupChnLoc1AddlUUID",
        "IncotermsSupChnLoc2AddlUUID",
        "IncotermsSupChnDvtgLocAddlUUID",
        "IntrastatCrsBorderTrMode",
        "InvoiceIsGoodsReceiptBased",
        "InvoiceIsMMServiceEntryBased",
        "MaterialPlannedDeliveryDurn",
        "MinimumOrderAmount",
        "PaymentTerms",
        "PlanningCycle",
        "PricingDateControl",
        "ProdStockAndSlsDataTransfPrfl",
        "ProductUnitGroup",
        "PurOrdAutoGenerationIsAllowed",
        "PurchaseOrderCurrency",
        "PurchasingGroup",
        "PurchasingIsBlockedForSupplier",
        "RoundingProfile",
        "ShippingCondition",
        "SuplrDiscountInKindIsGranted",
        "SuplrInvcRevalIsAllowed",
        "SuplrIsRlvtForSettlmtMgmt",
        "SuplrPurgOrgIsRlvtForPriceDetn",
        "SupplierABCClassificationCode",
        "SupplierAccountNumber",
        "SupplierIsReturnsSupplier",
        "SupplierPhoneNumber",
        "SupplierRespSalesPersonName",
        "SupplierConfirmationControlKey",
        "IsOrderAcknRqd",
        "AuthorizationGroup",
        "SupplierAccountGroup",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_SupplierPurchasingOrg")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class SupplierPurchasingOrgTextTable(CustomAPITable):
    """Supplier purchasing organization text data attached to purchasing organization in the system"""

    name: str = "supplier_purchasing_org_text"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Supplier",
        "PurchasingOrganization",
        "Language",
        "LongTextID",
        "LongText",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_SupplierPurchasingOrgText")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class SupplierTextTable(CustomAPITable):
    """Supplier text data attached to purchasing organization in the system"""

    name: str = "supplier_text"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Supplier",
        "Language",
        "LongTextID",
        "LongText",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_SupplierText")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)


class SupplierWithHoldingTaxTable(CustomAPITable):
    """Withholding tax records attached to supplier company in the system"""

    name: str = "supplier_withholding_tax"
    columns: List[str] = [
        "id",
        "uri",
        "type",
        "Supplier",
        "CompanyCode",
        "WithholdingTaxType",
        "ExemptionDateBegin",
        "ExemptionDateEnd",
        "ExemptionReason",
        "IsWithholdingTaxSubject",
        "RecipientType",
        "WithholdingTaxCertificate",
        "WithholdingTaxCode",
        "WithholdingTaxExmptPercent",
        "WithholdingTaxNumber",
        "AuthorizationGroup",
    ]

    def __init__(self, handler: APIHandler):
        super().__init__(handler)
        self.connection = self.handler.connect()

    def select(self, query: ast.Select) -> pd.DataFrame:
        data = self.connection.get("A_SupplierWithHoldingTax")
        df = pd.DataFrame.from_records(data)
        return self.apply_query_params(df, query)
