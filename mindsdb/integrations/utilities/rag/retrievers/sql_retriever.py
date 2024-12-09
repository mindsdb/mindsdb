from typing import List

from langchain.chains.llm import LLMChain
from langchain_community.tools.sql_database.tool import QuerySQLCheckerTool
from langchain_core.callbacks.manager import CallbackManagerForRetrieverRun
from langchain_core.documents.base import Document
from langchain_core.embeddings import Embeddings
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.prompts import PromptTemplate
from langchain_core.retrievers import BaseRetriever

from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.integrations.libs.vectordatabase_handler import VectorStoreHandler

SQL_PROMPT_TEMPLATE = '''
Construct a valid {dialect} SQL query to select documents relevant to the user input.
Documents are found in the scientech_document table. You may need to join with other tables to get additional document metadata.

You MUST always join with the {embeddings_table} table containing vector embeddings for the documents.
You MUST always order by the provided embeddings vector using the {distance_function} comparator.
You MUST always limit by {k} returned documents.

The JSON col "metadata" in the {embeddings_table} has a string field called "original_row_id". This "original_row_id" string field in the
"metadata" col is the document ID associated with a row in the {embeddings_table} table.


<< TABLES YOU HAVE ACCESS TO >>
1. {embeddings_table} - Contains document chunks, vector embeddings, and metadata for documents.
You MUST always join with the {embeddings_table} table containing vector embeddings for the documents.
You MUST always order by the provided embeddings vector using the {distance_function} comparator.
You MUST always limit by {k} returned documents.

Columns:
```json
{{
    "id": {{
        "type": "string",
        "description": "Unique ID for this document chunk"
    }},
    "content": {{
        "type": "string",
        "description": "A document chunk (subset of the original document)"
    }},
    "embeddings": {{
        "type": "vector",
        "description": "Vector embeddings for the document chunk. ALWAYS order by the provided embeddings vector using the {distance_function} comparator."
    }},
    "metadata": {{
        "type": "jsonb",
        "description": "Metadata for the document chunk. Always join with the scientech_document table on the string metadata field 'original_row_id'"
    }}
}}
```


2. scientech_document - Contains Nuclear Regulatory Services documents of various types.

Columns:
```json
{{
    "Id": {{
        "type": "int",
        "description": "Unique ID used as primary key of the document"
    }},
    "AccessionNumber": {{
        "type": "string",
        "description": "Accession number of the document"
    }},
    "AddresseeAffiliation": {{
        "type": "string",
        "description": "The organizational affiliation of the addressee"
    }},
    "AddresseeName": {{
        "type": "string",
        "description": "The name of the document's addressee"
    }},
    "AffectedModelNumbers": {{
        "type": "string",
        "description": "The model numbers affected by the document"
    }},
    "AmendmentNumber": {{
        "type": "string",
        "description": "The amendment number of the document"
    }},
    "ApprovalDate": {{
        "type": "datetime",
        "description": "The date when the document was approved in ISO 8601 format"
    }},
    "AuthorName": {{
        "type": "string",
        "description": "The name of the document's author"
    }},
    "AuthorAffiliation": {{
        "type": "string",
        "description": "The organizational affiliation of the document's author"
    }},
    "Citation": {{
        "type": "string",
        "description": "The citation reference in the document"
    }},
    "CommentExpiration": {{
        "type": "date",
        "description": "The expiration date for comments in YYYY-MM-DD format"
    }},
    "Component": {{
        "type": "string",
        "description": "The primary component referenced in the document"
    }},
    "ComponentFailureCauseId": {{
        "type": "int",
        "description": "The ID of component failure.",
        "values": {{
            1: "Personnel Error",
            2: "Design, Manufacturing, Construction/Installation",
            3: "External Cause",
            4: "Defective Procedure",
            5: "Management/Quality Assurance Deficiency",
            6: "Other",
            7: "UNSPECIFIED",
            8: "UNSPECIFIED"
        }}
    }},
    "ComponentManufacturer": {{
        "type": "string",
        "description": "The primary manufacturer of the component"
    }},
    "DateNoticed": {{
        "type": "date",
        "description": "The date when the document was noticed in YYYY-MM-DD format"
    }},
    "DocketCode": {{
        "type": "string",
        "description": "The docket code of the document"
    }},
    "DocketTypeId": {{
        "type": "int",
        "description": "The type of regulatory docket",
        "values": {{
            1: "Combined Operating License",
            2: "Design Certification",
            3: "Early Site Permit",
            4: "Operating Reactor",
            5: "Decommissioning/Decommissioned",
            6: "Cancelled"
        }}
    }},
    "DocumentCategory": {{
        "type": "int",
        "description": "The category of document"
    }},
    "DocumentDate": {{
        "type": "datetime",
        "description": "The date of the report in ISO 8601 format"
    }},
    "DocumentId": {{
        "type": "string",
        "description": "A unique string identifier for a document"
    }},
    "EffectiveDate": {{
        "type": "date",
        "description": "The effective date of the document in YYYY-MM-DD format"
    }},
    "EmergencyClass": {{
        "type": "int",
        "description": "The emergency classification level"
    }},
    "EventDate": {{
        "type": "datetime",
        "description": "The date of the event referenced in the document in ISO 8601 format"
    }},
    "ExemptionType": {{
        "type": "int",
        "description": "The type of exemption granted in the document"
    }},
    "Historical": {{
        "type": "bool",
        "description": "Indicates if this is a historical document"
    }},
    "IsCurrentVersion": {{
        "type": "bool",
        "description": "Indicates if this is the current version of the document"
    }},
    "IsRetracted": {{
        "type": "bool",
        "description": "Indicates if the document has been retracted"
    }},
    "LicenseNumbers": {{
        "type": "int",
        "description": "The license numbers associated with the document"
    }},
    "LicensingActionStatus": {{
        "type": "int",
        "description": "The current status of the licensing action"
    }},
    "NotificationDate": {{
        "type": "datetime",
        "description": "The date when the document was denied in ISO 8601 format"
    }},
    "PackageNumber": {{
        "type": "string",
        "description": "The package number of the document"
    }},
    "PackageTitle": {{
        "type": "string",
        "description": "The title of the document package"
    }},
    "Part": {{
        "type": "string",
        "description": "The part number referenced in the document"
    }},
    "Priority": {{
        "type": "int",
        "description": "The document priority level"
    }},
    "ReactorOperatingModelId": {{
        "type": "int",
        "description": "The operating model ID of the reactor"
    }},
    "ReferenceNumber": {{
        "type": "string",
        "description": "The reference number of the document"
    }},
    "ReportingOrganization": {{
        "type": "string",
        "description": "The company that reported or submitted the document"
    }},
    "ReportNumber": {{
        "type": "string",
        "description": "The report number of the document"
    }},
    "RequestNumber": {{
        "type": "string",
        "description": "The request number associated with the document"
    }},
    "RetractedDate": {{
        "type": "datetime",
        "description": "The date when the document was withdrawn or retracted in ISO 8601 format"
    }},
    "RevisionNumber": {{
        "type": "int",
        "description": "The revision number of the document"
    }},
    "SecondaryComponent": {{
        "type": "string",
        "description": "The secondary component referenced in the document"
    }},
    "SecondaryManufacturer": {{
        "type": "string",
        "description": "The secondary manufacturer of the component"
    }},
    "SignificantHazard": {{
        "type": "bool",
        "description": "The significant hazard classification of the document"
    }},
    "SortField": {{
        "type": "int",
        "description": "The section identifier in the document"
    }},
    "StandardReviewPlanSectionId": {{
        "type": "int",
        "description": "The section type in the standard review plan"
    }},
    "SubjectArea": {{
        "type": "int",
        "description": "The subject area of the document",
        "values": {{
            0: "TRENDS",
            1: "Licensing Actions",
            2: "Operating Experience",
            3: "Advantage",
            4: "License Renewal",
            5: "Regulatory Changes",
            6: "NUREG"
        }}
    }},
    "SubPart": {{
        "type": "string",
        "description": "The subpart identifier in the document"
    }},
    "Title": {{
        "type": "string",
        "description": "The title of the document"
    }},
    "Type": {{
        "type": "int",
        "description": "The NRS scientech document type",
        "values": {{
            0: "UNKNOWN",
            5: "ADAMS Document",
            6: "Licensee Event Report",
            8: "OE - Event Notification",
            9: "OE - Part 21 Report",
            10: "RAI Letter",
            12: "Question",
            13: "Answer",
            14: "RAI Response Letter",
            17: "ADV - COL Application",
            18: "ADV - ESP Application",
            19: "Safety Evaluation Report",
            20: "SECY",
            21: "Regulatory Audit",
            22: "Site Audit",
            23: "Review Schedule",
            24: "ACRS - related Document",
            25: "ASLB - related Document",
            26: "Commission Meeting Transcript",
            27: "ADV-Design Control Document",
            28: "Environmental Impact Statement",
            29: "Inspection Report-Part 52",
            30: "Response To Inspection Report",
            31: "ITAAC - related Document",
            32: "10 CFR Part 1-199",
            33: "Federal Register",
            34: "FR - Table Of Content",
            35: "FR - Proposed Rules",
            37: "FR - Notice",
            38: "FR - Meeting",
            39: "FR - Final Rules",
            40: "FR - BiWeekly Notice",
            42: "License Amendment",
            43: "AR Rai",
            44: "AR RaiR",
            45: "AR Acceptance",
            46: "AR Denial",
            48: "Withdrawal",
            49: "Denial",
            52: "Statements of Consideration",
            54: "Lic Action - Exemption",
            55: "Initial Relief Request",
            56: "Lic Action - Relief Request",
            57: "NRC - Review Standards",
            58: "NRC - Enforcement Manual",
            59: "NUREG",
            60: "NUREG/BR",
            61: "NUREG/CP",
            62: "NUREG/CR",
            63: "NUREG/GR",
            64: "NUREG/IA",
            65: "Nuclear Regulations",
            66: "NRC - Office Instruction",
            67: "QA Program Change",
            68: "NRC - Speech",
            69: "Task Interface Agreement",
            70: "Vendor Inspection Report",
            71: "Assessment - Annual",
            72: "Assessment - Midcycle/Updated IP",
            73: "NRC - Management Directive",
            74: "NRC - Policy Statement",
            75: "Regulatory Guide",
            76: "NRC - Interim Staff Guidance",
            77: "Standard Review Plan",
            78: "All Generic Communication",
            79: "Gen Com - Generic Letter",
            80: "Gen Com - Information Notice",
            81: "Gen Com - Bulletin",
            82: "Gen Com - Regulatory Issue Summary",
            83: "Gen Com - Administrative Letter",
            84: "Gen Com - Circular",
            85: "SECY - SRM",
            86: "SECY - Vote Record",
            87: "SECY - ComSECY",
            88: "NRC - Inspection Manual",
            89: "LER Component Failure",
            90: "Notice of Enforcement Discretion (NOED)",
            91: "NEI Technical Report",
            92: "Current Technical Specification",
            93: "Supplement",
            94: "Correction",
            95: "LIS Meeting Summaries",
            96: "Web-Docket",
            97: "TS Bases",
            98: "EIS and Comments",
            99: "FSAR",
            101: "NRC - Order",
            102: "Allegation Guidance Memorandum",
            103: "NRC - Organization Charts",
            104: "Original Plant SERs",
            105: "NUREG/KM",
            106: "NRC - Review Standards – Design-Specific",
            107: "Research Information Letter",
            108: "Inspection Report – Other",
            109: "Inspection Report – Integrated",
            110: "Inspection Report – CDBI",
            111: "Inspection Report – FPT",
            112: "Inspection Report – PI&R",
            113: "Inspection Report – Response",
            114: "Inspection Report – Security",
            115: "Inspection Report – EP",
            116: "Inspection Report – ISFSI",
            117: "Inspection Report – 95001",
            118: "Inspection Report – 95002",
            119: "Inspection Report – 95003",
            120: "Inspection Report - DBA",
            121: "NRC - OIG Reports"
        }}
    }}
}}
```


3. plant - Contains information about specific power plants.

Columns:
```json
{{
    "PlantKey": {{
        "type": "int",
        "description": "The unique identifier for the plant"
    }},
    "PlantName": {{
        "type": "string",
        "description": "The name of the plant"
    }},
    "PlantCode": {{
        "type": "string",
        "description": "The code of the plant"
    }},
    "NumberOfUnits": {{
        "type": "int",
        "description": "How many units the plant has"
    }},
    "LocationId": {{
        "type": "int",
        "description": "The ID for the location of the power plant"
    }}
}}
```


4. unit - Contains information about specific units of power plants. Several units can be part of a single power plant.

Columns:
```json
{{
    "UnitKey": {{
        "type": "int",
        "description": "The unique identifier of the unit"
    }},
    "PlantKey": {{
        "type": "int",
        "description": "The ID of the plant the unit belongs to"
    }},
    "PeerGroupKey": {{
        "type": "int",
        "description": The peer group the unit belongs to, if any.",
        "values": {{
            1: "CE Plants without CPCs",
            2: "ISFSI",
            3: "CE Plants with CPCs",
            4: "GE BWR2 BWR3 Older PreTMI BWR4",
            5: "GE Newer BWR4 BWR5 BWR6",
            6: "Babcock and Wilcox",
            7: "Westinghouse Small 2 3 and 4 Loop",
            8: "Westinghouse Older 3 Loop",
            9: "Westinghouse Older 4 Loop",
            10: "Westinghouse Newer 3 and 4 Loop",
            11: "Construction",
            12: "Not Defined",
            13: "Retired",
            14: "New Build",
            15: "Decommissioning",
            16: "Decommission Complete"
        }}
    }}
    "UnitName": {{
        "type": "string",
        "description": "The name of the unit"
    }},
    "UnitRegion": {{
        "type": "string",
        "description": "The region the unit is in"
    }},
    "ReactorTypeId": {{
        "type": "int",
        "description": "The type of the reactor in this unit",
        "values": {{
            1: "Advanced Boiling Water Reactor by General Electric (GE) Nuclear Energy (code ABWR)",
            2: "System 80+ by Westinghouse Electric Company (code S-80+)",
            3: "Advanced Passive 600 by Westinghouse Electric Company (code AP600)",
            4: "Advanced Passive 1000 by Westinghouse Electric Company (code AP1000)",
            5: "Economic Simplified Boiling-Water Reactor by GE-Hitachi Nuclear Energy (code ESBWR)",
            6: "U.S. Evolutionary Power Reactor by AREVA Nuclear Power (code US-EPR)",
            7: "U.S. Advanced Pressurized-Water Reactor by Mitsubishi Heavy Industries, Ltd. (code US-APWR)",
            9: "Boiling Water Reactor by General Electric (code BWR-GE)",
            10: "Pressurized Water Reactor by Babcock & Wilcox (code PWR-B&W)",
            11: "Pressurized Water Reactor by Combustion Engineering (code PWR-CE)",
            12: "Pressurized Water Reactor by Westinghouse (code PWR-W)",
            13: "Advanced CANDU Reactor by Atomic Energy of Canada Limited (code ACR-700)",
            14: "International Reactor Innovative and Secure by Westinghouse (code IRIS)",
            15: "Pebble Bed Modular Reactor by PBMR (Pty) Ltd (code PBMR)",
            19: "Super Safe, Small and Simple reactor by Toshiba (code Toshiba 4s)",
            20: "Simplified Boiling Water Reactor by AREVA (code SWR-1000)",
            21: "Gas Turbine-Modular Helium Reactor  by General Atomics (code GT-MHR)",
            22: "Small Modular Reactor by Babcock & Wilcox (code mPower)",
            23: "Westinghouse Small Modular Reactor (code SMR-W)",
            24: "APR1400 (code APR1400)",
            25: "NuScale SMR (code SMR-NSC)"
        }}
    }},
    "UnitNumber": {{
        "type": "int",
        "description": "The number assigned to the unit"
    }},
    "ApplicationDate": {{
        "type": "date",
        "description": "Application date associated with the unit in YYYY-MM-DD format"
    }},
    "SafetyEvaluationReportDate": {{
        "type": "date",
        "description": "The date of the last safety evaluation report for the unit in YYYY-MM-DD format"
    }}
}}
```

5. document_unit - Links documents to the power plant units they are relevant to.

Columns:
```json
{{
    "ScientechDocumentId": {{
        "type": "int",
        "description": "ID of the Scientech Document associated with the unit"
    }},
    "UnitKey": {{
        "type": "int",
        "description": "ID of the Unit the Scientech Document is associated with"
    }}
}}
```

6. contributor - Provides information about contributors to documents.

Columns:
```json
{{
    "ContributorKey": {{
        "type": "int",
        "description": "Unique ID of the contributor"
    }},
    "ContributorName": {{
        "type": "string",
        "description": "Contributor name"
    }}
}}
```

7. scientech_document_contributor - Links documents to contributors.

Columns:
```json
{{
    "ScientechDocumentId": {{
        "type": "int",
        "description": "ID of the Scientech Document associated with the contributor"
    }},
    "InspectorKey": {{
        "type": "int",
        "description": "ID of the contributor associated with the Scientech Document"
    }}
}}
```

8. soc_sections - Contains information about SOC (Security Operations Center) sections. 

Columns:
```json
{{
    "SOCSectionID": {{
        "type": "int",
        "description": "Unique ID for the SOC section"
    }},
    "SOCSection": {{
        "type": "string",
        "description": "Name of the SOC section"
    }}
}}
```


9. soc_sections_document - Links documents to SOC (Security Operations Center) sections they are relevant to.

Columns:
```json
{{
    "ScientechDocumentId": {{
        "type": "int",
        "description": "ID of the Scientech Document associated with the SOC section"
    }},
    "SOCSectionID": {{
        "type": "int",
        "description": "ID for the SOC section relevant to the Scientech Document"
    }}
}}
```

10. document - Contains view count information for Scientech Documents.

Columns:
```json
{{
    "Id": {{
        "type": "string",
        "description": "DocumentId of the Scientech Document"
    }},
    "ViewCount": {{
        "type": "int",
        "description": "How many people viewed this document. A document is popular if it has over 100 views."
    }}
}}
```

<< EXAMPLES >>

1. User input: "Get me all documents related to the Beaver Valley plant"
Output:
SELECT sd.*, v.*
FROM scientech_document sd
JOIN document_unit du ON sd."Id" = du."ScientechDocumentId"
JOIN unit u ON du."UnitKey" = u."UnitKey"
JOIN plant p ON u."PlantKey" = p."PlantKey"
JOIN {embeddings_table} v ON (v."metadata"->>'original_row_id')::int = sd."Id"
WHERE p."PlantName" = 'Beaver Valley'
ORDER BY v.embeddings {distance_function} '{{embeddings}}' LIMIT {k};

2. User input: "Get me all documents approved after Jan 1, 2024"
Output:
SELECT sd.*, v.*
FROM scientech_document sd
JOIN {embeddings_table} v ON (v."metadata"->>'original_row_id')::int = sd."Id"
WHERE sd."ApprovalDate" > '2024-01-01'
ORDER BY v.embeddings {distance_function} '{{embeddings}}' LIMIT {k};

Output the {dialect} SQL query that is ready to be executed only and NOTHING ELSE.

Here is the user input:
{input}
'''

SEMANTIC_PROMPT_TEMPLATE = '''Provide a better search query for web search engine to answer the given question.

<< EXAMPLES >> 
1. Input: "Looking for License amendment request associated with changes to emergency plan staff augmentation"
Output: "Emergency plan staff augmentation license amendment request"

2. Input: "Show me plants and documents that requested an extension to Type A leak rate test frequencies?"
Output: "Type A leak rate test frequency extension"

Output only a single better search query and nothing else like in the examples.

Here is the user input: {input}
'''

class SQLRetriever(BaseRetriever):

    vector_store_handler: VectorStoreHandler
    embeddings_model: Embeddings
    query_checker_tool: QuerySQLCheckerTool
    llm: BaseChatModel

    def _get_relevant_documents(
        self, query: str, *, run_manager: CallbackManagerForRetrieverRun
    ) -> List[Document]:
        print('####### GETTING RELEVANT DOCS WITH QUERY')
        print(query)
        print('\n\n')
        # Rewrite query.
        rewrite_prompt = PromptTemplate(
            input_variables=['input'],
            template=SEMANTIC_PROMPT_TEMPLATE
        )
        rewrite_chain = LLMChain(llm=self.llm, prompt=rewrite_prompt)
        retrieval_query = rewrite_chain.predict(
            input=query
        )
        print('############# REWROTE QUERY FOR RETRIEVAL ##############')
        print(retrieval_query)
        print('\n\n')
        # # Rewrite for SQL.
        # condition_prompt = PromptTemplate(
        #     input_variables=['input'],
        #     template=CONDITION_PROMPT_TEMPLATE
        # )
        # condition_chain = LLMChain(llm=self.llm, prompt=condition_prompt)
        # condition_query = condition_chain.predict(
        #     input=query
        # )
        # print('################### REWROTE QUERY FOR SQL #################3')
        # print(condition_query)
        # print('\n\n')
        sql_prompt = PromptTemplate(
            input_variables=['dialect', 'input', 'embeddings_table', 'embeddings', 'distance_function'],
            template=SQL_PROMPT_TEMPLATE
        )
        sql_chain = LLMChain(llm=self.llm, prompt=sql_prompt)
        sql_query = sql_chain.predict(
            dialect='postgres',
            input=query,
            embeddings_table='doc_vec_c1_t42',
            distance_function='<->',
            k=5,
            callbacks=run_manager.get_child() if run_manager else None
        )
        checked_sql_query = self.query_checker_tool.invoke(sql_query)
        embedded_query = self.embeddings_model.embed_query(retrieval_query)
        checked_sql_query_with_embeddings = checked_sql_query.format(embeddings=str(embedded_query))
        print('################# GENERATED SQL QUERY ################')
        print(checked_sql_query_with_embeddings)
        print('\n\n')
        # document_response = self.vector_store_handler.native_query(checked_sql_query)
        # if document_response.resp_type == RESPONSE_TYPE.ERROR:
        #     print('Encountered an error:')
        #     print(document_response.error_code)
        #     print(document_response.error_message)
        #     return []
        # document_df = document_response.data_frame
        # print('#################### GOT DATAFRAME OF DOCUMENTS #########################')
        # print(document_df)
        # print('\n\n\n')
        return []