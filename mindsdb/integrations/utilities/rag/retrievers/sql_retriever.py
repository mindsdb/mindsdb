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
        "description": "The ID of component failure"
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
        "description": "The type of regulatory docket"
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
    "isRetracted": {{
        "type": "bool",
        "description": "Indicates if the document has been retracted"
    }},
    "Keywords": {{
        "type": "text",
        "description": "The keywords associated with the document"
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
        "description": "The operating mode of the reactor"
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
        "description": "The subject area of the document"
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
        "description": "The NRS scientech document type"
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
        "description": "The type of the reactor in this unit"
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

SEMANTIC_PROMPT_TEMPLATE = '''Your goal is to output a text string given the user's input which will be used to compare to document contents.
The output should contain only text that is expected to match the contents of documents. Any conditions in the original query should not be mentioned in the output as well.

<< EXAMPLES >>
1. User input: "I want all documents for the Beaver Valley Plant for nuclear fuel waste."
Output: "nuclear fuel waste"


Here is the user input: {input}
'''

CONDITION_PROMPT_TEMPLATE = '''Your goal is to rewrite the user's input to only contain logical conditions for filtering documents.
The output should not contain any text that is expected to match the contents of documents.
The output should not contain ANY semantic information, only logical conditions, if there are any.

<< EXAMPLES >>
1. User input: "I want all documents for the Beaver Valley Plant for nuclear fuel waste."
Output: "I want all documents for the Beaver Valley Plant."

2. User input: "What plants filed for TSTF-404 & got approved in 2024?"
Output: "What plants got approved in 2024?"

3. User input: "License amendments associated with 24 month fuel cycle transition"
Output: "License amendments"


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
        # Rewrite for SQL.
        condition_prompt = PromptTemplate(
            input_variables=['input'],
            template=CONDITION_PROMPT_TEMPLATE
        )
        condition_chain = LLMChain(llm=self.llm, prompt=condition_prompt)
        condition_query = condition_chain.predict(
            input=query
        )
        print('################### REWROTE QUERY FOR SQL #################3')
        print(condition_query)
        print('\n\n')
        sql_prompt = PromptTemplate(
            input_variables=['dialect', 'input', 'embeddings_table', 'embeddings', 'distance_function'],
            template=SQL_PROMPT_TEMPLATE
        )
        sql_chain = LLMChain(llm=self.llm, prompt=sql_prompt)
        sql_query = sql_chain.predict(
            dialect='postgres',
            input=condition_query,
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