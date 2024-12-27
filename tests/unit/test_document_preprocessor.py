import pytest
from unittest.mock import Mock, patch

from mindsdb.interfaces.knowledge_base.preprocessing.models import (
    Document,
    TextChunkingConfig,
    ContextualConfig,
)

# Mock all langchain imports to avoid pydantic version conflicts
with patch.dict('sys.modules', {
    'mindsdb.interfaces.agents.langchain_agent': Mock(),
    'mindsdb.interfaces.agents.mindsdb_chat_model': Mock(),
    'mindsdb.interfaces.agents.constants': Mock(),
    'langchain_openai': Mock(),
}):
    from mindsdb.interfaces.knowledge_base.preprocessing.document_preprocessor import (
        DocumentPreprocessor,
        TextChunkingPreprocessor,
        ContextualPreprocessor,
    )


class TestDocumentPreprocessor:
    def test_deterministic_id_generation(self):
        """Test that ID generation is deterministic for same content"""
        # Same content should generate same ID
        content = "test content"
        preprocessor = DocumentPreprocessor()
        id1 = preprocessor._generate_deterministic_id(content)
        id2 = preprocessor._generate_deterministic_id(content)
        assert id1 == id2

        # Different content should generate different IDs
        different_content = "different content"
        id3 = preprocessor._generate_deterministic_id(different_content)
        assert id1 != id3

        # Test with provided_id
        provided_id = "test_id"
        content_column = "test_column"
        id4 = preprocessor._generate_deterministic_id(
            content, content_column, provided_id
        )
        assert id4 == f"{provided_id}_{content_column}"

    def test_chunk_id_generation(self):
        """Test chunk ID generation with and without indices"""
        content = "test content"
        content_column = "test_column"
        provided_id = "test_id"

        # Test basic ID generation
        preprocessor = DocumentPreprocessor()
        base_id = preprocessor._generate_chunk_id(content)
        assert base_id == preprocessor._generate_deterministic_id(content)

        # Test with content_column
        column_id = preprocessor._generate_chunk_id(
            content, content_column=content_column
        )
        assert column_id == preprocessor._generate_deterministic_id(
            content, content_column
        )

        # Test with provided_id
        provided_chunk_id = preprocessor._generate_chunk_id(
            content, content_column=content_column, provided_id=provided_id
        )
        assert provided_chunk_id == preprocessor._generate_deterministic_id(
            content, content_column, provided_id
        )

        # Test chunk index handling
        chunk_id = preprocessor._generate_chunk_id(content, chunk_index=0)
        assert chunk_id.endswith("_chunk_0")
        assert chunk_id.startswith(preprocessor._generate_deterministic_id(content))

    def test_split_document_without_splitter(self):
        """Test that splitting without a configured splitter raises error"""
        doc = Document(content="test content")
        preprocessor = DocumentPreprocessor()
        with pytest.raises(ValueError, match="Splitter not configured"):
            preprocessor._split_document(doc)


class TestTextChunkingPreprocessor:
    @pytest.fixture
    def preprocessor(self):
        config = TextChunkingConfig(chunk_size=100, chunk_overlap=20)
        return TextChunkingPreprocessor(config)

    def test_single_document_processing(self, preprocessor):
        """Test processing of a single document without chunking"""
        doc = Document(content="Short content that won't be chunked")
        chunks = preprocessor.process_documents([doc])
        assert len(chunks) == 1
        assert chunks[0].content == doc.content
        assert chunks[0].metadata["source"] == "TextChunkingPreprocessor"

    def test_document_chunking(self, preprocessor):
        """Test processing of a document that requires chunking"""
        long_content = " ".join(["word"] * 50)
        doc = Document(content=long_content)
        chunks = preprocessor.process_documents([doc])
        assert len(chunks) > 1
        # Verify chunk IDs are deterministic and unique
        chunk_ids = [chunk.id for chunk in chunks]
        assert len(chunk_ids) == len(set(chunk_ids))
        # Verify source metadata
        assert all(
            chunk.metadata["source"] == "TextChunkingPreprocessor" for chunk in chunks
        )
        # Verify chunk indices
        assert all("chunk_index" in chunk.metadata for chunk in chunks)
        # Process same content again and verify same IDs
        new_chunks = preprocessor.process_documents([doc])
        new_chunk_ids = [chunk.id for chunk in new_chunks]
        assert chunk_ids == new_chunk_ids


def test_metadata_preservation():
    """Test that metadata is preserved during processing"""
    preprocessor = TextChunkingPreprocessor()
    metadata = {"key": "value", "content_column": "test_column"}
    doc = Document(content="Test content", metadata=metadata)
    chunks = preprocessor.process_documents([doc])
    # Verify metadata is preserved and includes source
    assert chunks[0].metadata["source"] == "TextChunkingPreprocessor"
    assert chunks[0].metadata["key"] == "value"
    assert chunks[0].metadata["content_column"] == "test_column"


def test_content_column_handling():
    """Test handling of content column in metadata"""
    preprocessor = TextChunkingPreprocessor()
    metadata = {"content_column": "test_column"}
    doc = Document(content="Test content", metadata=metadata)
    chunks = preprocessor.process_documents([doc])
    # Verify content column is used in ID generation
    assert "test_column" in chunks[0].id


def test_provided_id_handling():
    """Test handling of provided document IDs"""
    preprocessor = TextChunkingPreprocessor()
    doc = Document(content="Test content", id="test_id")
    chunks = preprocessor.process_documents([doc])
    # Verify provided ID is incorporated into chunk ID
    assert chunks[0].metadata["original_doc_id"] == "test_id"


def test_empty_content_handling():
    """Test handling of empty content"""
    preprocessor = TextChunkingPreprocessor()
    doc = Document(content="")
    chunks = preprocessor.process_documents([doc])
    assert len(chunks) == 0


def test_whitespace_content_handling():
    """Test handling of whitespace-only content"""
    preprocessor = TextChunkingPreprocessor()
    doc = Document(content="   \n   \t   ")
    chunks = preprocessor.process_documents([doc])
    assert len(chunks) == 0


@pytest.mark.parametrize(
    "content,metadata,expected_source",
    [
        ("test", None, "TextChunkingPreprocessor"),
        ("test", {}, "TextChunkingPreprocessor"),
        ("test", {"key": "value"}, "TextChunkingPreprocessor"),
    ],
)
def test_source_metadata(content, metadata, expected_source):
    """Test source metadata is correctly set"""
    preprocessor = TextChunkingPreprocessor()
    doc = Document(content=content, metadata=metadata)
    chunks = preprocessor.process_documents([doc])
    assert chunks[0].metadata["source"] == expected_source


class TestContextualPreprocessor:
    def test_source_metadata(self, preprocessor_sync):
        """Test that source metadata is correctly set"""
        doc = Document(content="Test content")
        chunks = preprocessor_sync.process_documents([doc])
        assert chunks[0].metadata["source"] == "ContextualPreprocessor"

    def test_prepare_prompts(self, preprocessor_sync):
        chunk_contents = [f"Chunk contents {i}" for i in range(10)]
        full_documents = [f"Full document {i}" for i in range(10)]
        _ = preprocessor_sync._prepare_prompts(chunk_contents, full_documents)

    def test_process_documents(self, sample_document, preprocessor_sync):
        docs = [Document(content=sample_document, id=f"{i}") for i in range(3)]
        _ = preprocessor_sync.process_documents(docs)

    def test_process_documents_async(self, sample_document, preprocessor_async):
        """Test document processing with async LLM"""
        docs = [Document(content=sample_document, id=f"{i}") for i in range(3)]
        chunks = preprocessor_async.process_documents(docs)
        assert len(chunks) > 0
        assert all(chunk.content == "Test context" for chunk in chunks)

    def test_process_documents_sync(self, sample_document, preprocessor_sync):
        """Test document processing with sync LLM"""
        docs = [Document(content=sample_document, id=f"{i}") for i in range(3)]
        chunks = preprocessor_sync.process_documents(docs)
        assert len(chunks) > 0
        assert all(chunk.content == "Test context" for chunk in chunks)

    @pytest.fixture
    def preprocessor_async(self):
        with patch(
            "mindsdb.interfaces.knowledge_base.preprocessing.document_preprocessor.create_chat_model"
        ) as mock_create_chat_model:
            # Create a mock async LLM
            class MockResponse:
                def __init__(self, content):
                    self.content = content

            class AsyncMockLLM:
                async def abatch(self, prompts):
                    import asyncio
                    await asyncio.sleep(0)  # Simulate async operation
                    return [MockResponse("Test context") for _ in prompts]

            mock_llm = AsyncMockLLM()
            mock_create_chat_model.return_value = mock_llm

            config = ContextualConfig(
                chunk_size=100,
                chunk_overlap=20,
                llm_config={"model_name": "test_model", "provider": "test"},
                summarize=True,  # Set summarize to True to only return context
            )
            return ContextualPreprocessor(config)

    @pytest.fixture
    def preprocessor_sync(self):
        with patch(
            "mindsdb.interfaces.knowledge_base.preprocessing.document_preprocessor.create_chat_model"
        ) as mock_create_chat_model:
            # Create a mock sync LLM
            class MockResponse:
                def __init__(self, content):
                    self.content = content

            class SyncMockLLM:
                def batch(self, prompts):
                    return [MockResponse("Test context") for _ in prompts]

                async def abatch(self, prompts):
                    import asyncio
                    await asyncio.sleep(0)  # Simulate async operation
                    return [MockResponse("Test context") for _ in prompts]

            mock_llm = SyncMockLLM()
            mock_create_chat_model.return_value = mock_llm

            config = ContextualConfig(
                chunk_size=100,
                chunk_overlap=20,
                llm_config={"model_name": "test_model", "provider": "test"},
                summarize=True,  # Set summarize to True to only return context
            )
            return ContextualPreprocessor(config)

    @pytest.fixture
    def sample_document(self):
        return """

    Federal Register, Volume 89 Issue 153 (Thursday, August 8, 2024)


    [Federal Register Volume 89, Number 153 (Thursday, August 8, 2024)]
    [Notices]
    [Pages 64964-64965]
    From the Federal Register Online via the Government Publishing Office [www.gpo.gov]
    [FR Doc No: 2024-17521]



    [[Page 64964]]

    =======================================================================
    -----------------------------------------------------------------------

    NUCLEAR REGULATORY COMMISSION

    [Docket No. 50-0320; NRC-2024-0099]


    TMI-2SOLUTIONS, LLC; Three Mile Island Nuclear Station, Unit No. 
    2; Environmental Assessment and Finding of No Significant Impact

    AGENCY: Nuclear Regulatory Commission.

    ACTION: Notice; issuance.

    -----------------------------------------------------------------------

    SUMMARY: The U.S. Nuclear Regulatory Commission (NRC) is issuing a 
    final environmental assessment (EA) and finding of no significant 
    impact (FONSI) for a proposed amendment of NRC Possession Only License 
    (POL) DPR-73 for the Three Mile Island Nuclear Station, Unit No. 2 
    (TMI-2), located in Londonderry Township, Dauphin County, Pennsylvania. 
    The proposed amendment would ensure that TMI-2 Energy Solutions (TMI-
    2Solutions, the licensee) can continue decommissioning the facility in 
    accordance with NRC regulations. TMI-2Solutions will be engaging in 
    certain major decommissioning activities, including the physical 
    demolition of buildings previously deemed eligible for the National 
    Register of Historic Places (NRHP). The EA, ``Environmental Assessment 
    for Specific Decommissioning Activities at Three Mile Island, Unit 2 in 
    Dauphin County, Pennsylvania,'' documents the NRC staff's environmental 
    review of the license amendment application.

    DATES: The EA and FONSI referenced in this document are available on 
    August 8, 2024.

    ADDRESSES: Please refer to Docket ID NRC-2024-0099 when contacting the 
    NRC about the availability of information regarding this document. You 
    may obtain publicly available information related to this document 
    using any of the following methods:
         Federal Rulemaking Website: Go to https://www.regulations.gov and search for Docket ID NRC-2024-0099. Address 
    questions about Docket IDs to Stacy Schumann; telephone: 301-415-0624; 
    email: [email protected]. For technical questions, contact the 
    individual listed in the FOR FURTHER INFORMATION CONTACT section of 
    this document.
         NRC's Agencywide Documents Access and Management System 
    (ADAMS): You may obtain publicly available documents online in the 
    ADAMS Public Documents collection at https://www.nrc.gov/reading-rm/adams.html. To begin the search, select ``Begin Web-based ADAMS 
    Search.'' For problems with ADAMS, please contact the NRC's Public 
    Document Room (PDR) reference staff at 1-800-397-4209, at 301-415-4737, 
    or by email to [email protected]. The ADAMS accession number for 
    each document referenced (if it is available in ADAMS) is provided the 
    first time that it is mentioned in this document.
         NRC's PDR: The PDR, where you may examine and order copies 
    of publicly available documents, is open by appointment. To make an 
    appointment to visit the PDR, please send an email to 
    [email protected] or call 1-800-397-4209 or 301-415-4737, between 8 
    a.m. and 4 p.m. eastern time (ET), Monday through Friday, except 
    Federal holidays.
         Project Website: Information related to the TMI-2 project 
    can be accessed on NRC's TMI-2 public website at https://www.nrc.gov/info-finder/decommissioning/power-reactor/three-mile-island-unit-2.html.

    FOR FURTHER INFORMATION CONTACT: Jean Trefethen, Office of Nuclear 
    Material Safety and Safeguards, U.S. Nuclear Regulatory Commission, 
    Washington, DC 20555-0001; telephone: 301-415-0867; email: 
    [email protected].

    SUPPLEMENTARY INFORMATION:

    I. Background

        The Three Mile Island Nuclear Station (TMINS) is approximately 16 
    kilometers (10 miles) southeast of Harrisburg, Pennsylvania. The TMINS 
    site includes Three Mile Island Nuclear Station, Unit 1 and TMI-2. It 
    encompasses approximately 178 hectares (440 acres), including the 
    adjacent islands on the north end, a strip of land on the mainland 
    along the eastern shore of the river, and an area on the eastern shore 
    of Shelley Island. The TMINS site has significance in U.S. history 
    because it is the site of the nation's most serious commercial nuclear 
    power plant accident, occurring at TMI-2. On March 28, 1979, TMI-2 
    experienced an accident initiated by interruption of secondary 
    feedwater flow which led to a core heat up that caused fuel damage. The 
    partial meltdown of the reactor core led to a very small offsite 
    release of radioactivity. In response to this accident many changes 
    occurred at nuclear power plants including emergency response planning, 
    reactor operator training, human factors engineering, radiation 
    protection and heightened NRC regulatory oversight.

    II. Discussion

        By letter dated February 22, 2023 (ADAMS Accession No. 
    ML23058A064), TMI-2Solutions requested an amendment to POL No. DPR-73. 
    TMI-2Solutions will be engaging in certain major decommissioning 
    activities, including the physical demolition of buildings previously 
    deemed eligible for the NRHP. Because the impacts on the historic 
    properties from these decommissioning activities have not been 
    previously evaluated and are not bounded by the impact's discussion in 
    NUREG-0586, ``Final Generic Environmental Impact Statement on 
    Decommissioning of Nuclear Facilities,'' TMI-2Solutions requested an 
    amendment that would require evaluation of the impacts of the 
    decommissioning activities on the NRHP-eligible properties, in 
    compliance with paragraph 50.82(a)(6)(ii) of title 10 of the Code of 
    Federal Regulations (10 CFR).
        Pursuant to 36 CFR 800.8, the NRC used its National Environmental 
    Policy Act process for developing the EA to facilitate consultation 
    pursuant to section 106 of the National Historic Preservation Act 
    (NHPA).
        Adverse effects to historic properties would result from 
    decommissioning activities at TMI-2. Therefore, the NRC and consulting 
    parties proceeded with development of a programmatic agreement (PA) to 
    resolve adverse effects. The draft PA was issued for public comment 
    through a Federal Register notice dated March 6, 2024 (89 FR 16037). 
    One comment was received and considered before finalizing the PA. The 
    PA addresses the potential direct and indirect adverse effects from the 
    decommissioning activities and ensures that appropriate mitigation 
    measures are implemented. The NRC's EA references the final PA and, 
    therefore, conclude NHPA section 106 consultation.
        In accordance with NRC's regulations in 10 CFR part 51, 
    ``Environmental Protection Regulations for Domestic Licensing and 
    Related Regulatory Functions,'' that implement the National Environment 
    Protection Agency (NEPA), the NRC staff has prepared an EA documenting 
    its environmental review of the license amendment application. Based on 
    the environmental review, the NRC has made a determination that the 
    proposed action will not significantly affect the quality of the human 
    environment and that a FONSI is therefore appropriate.

    III. Summary of Environmental Assessment

        The EA is publicly available in ADAMS under Accession No. 
    ML24197A005. A summary description of the proposed action and expected 
    environmental impacts is provided as follows.

    [[Page 64965]]

    Description of the Proposed Action

        The proposed action is to amend POL No. DPR-73 so that TMI-
    2Solutions can continue with certain major decommissioning activities 
    planned under Phase 2 of its decommissioning schedule. Phase 2 
    decommissioning activities include the removal of any radioactive 
    components in preparation for demolition of structures, decommissioning 
    and dismantlement of the TMI-2 site to a level that permits the release 
    of the site, except for an area potentially to be set aside for storage 
    of fuel-bearing material (small quantities of spent nuclear fuel, 
    damaged core material, and high-level waste) on the independent spent 
    fuel storage installation, backfilling of the site, license termination 
    plan submittal and implementation, and site restoration activities. In 
    order to comply with 10 CFR 50.82(a)(6)(ii), TMI-2Solutions requested 
    that NRC evaluate the impacts of certain major decommissioning 
    activities on historic and cultural resources and NRHP-eligible 
    properties. The definition of major decommissioning activity is in 10 
    CFR 50.2, which states ``major decommissioning activity means, for a 
    nuclear power reactor facility, any activity that results in permanent 
    removal of major radioactive components, permanently modifies the 
    structure of the containment, or results in dismantling components for 
    shipment containing greater than class C waste in accordance with Sec.  
    61.55 of this chapter.'' Due to radioactive contamination, the TMI-2 
    structures must be demolished and removed during decommissioning.

    Environmental Impacts of the Proposed Action

        In the EA, the staff assessed the potential environmental impacts 
    from the proposed license amendment to the following resource areas: 
    land use; visual and scenic resources; the geologic environment; 
    surface and groundwater resources; ecological resources; air quality; 
    noise; historic and cultural resources; socioeconomic conditions; 
    environmental justice; public and occupational health; transportation; 
    and waste generation and management. The NRC staff also considered the 
    cumulative impacts from past, present, and reasonably foreseeable 
    actions when combined with the proposed action. The TMI-2 Historic 
    District would be adversely affected by the TMI-2 decommissioning, and 
    adverse effects cannot be avoided. The mitigation of adverse effects to 
    the TMI-2 Historic District will be completed in accordance with the 
    TMI-2 Demolition and Decommissioning Programmatic Agreement (NRC 
    2024a).
        As part of the NRC's consultation under section 7 of the Endangered 
    Species Act, NRC staff determined that the proposed action may affect 
    but is not likely to adversely affect the Indiana bat (Myotis sodalis), 
    northern long-eared bat (Myotis septentrionalis), tricolored bat 
    (Perimyotis subflavus), monarch butterfly (Danaus plexippus), 
    northeastern bulrush (Scirpus ancistrochaetus), or green floater 
    (Lasmigona subviridis). The NRC staff transmitted a letter to the U.S. 
    Fish and Wildlife Service (FWS) for its review and concurrence on May 
    24, 2024 (ADAMS Accession No. ML24120A324). The FWS concurred with the 
    NRC's findings on July 15, 2024 (ADAMS Accession No. ML24199A062).
        All other potential impacts from the proposed action were 
    determined to be not significant, as described in the EA. The NRC staff 
    found that there would be no significant negative cumulative impact to 
    any resource area from the proposed action when added to other past, 
    present, and reasonably foreseeable actions.

    Environmental Impacts of the Alternative to the Proposed Action

        As an alternative to the proposed action, the NRC staff considered 
    denial of the proposed action (i.e., the ``no-action'' alternative). 
    Under the no-action alternative, the NRC would deny the licensee's 
    request to allow for the continuation of major decommissioning 
    activities under Phase 2. In this case, the NRC staff would not review 
    the historic and cultural resource impacts of the major decommissioning 
    activities as defined in 10 CFR 50.2 and would therefore disallow the 
    removal of NRHP-eligible structures and any impacts to historic and 
    cultural resources. However, due to the presence of radioactive 
    contamination, TMI-2 structures, including the NRHP-eligible 
    structures, must be removed during the decommissioning process. 
    Therefore, the NRC staff concludes that denying the amendment request 
    is not a reasonable alternative.

    IV. Finding of No Significant Impact

        In accordance with the NEPA and 10 CFR part 51, the NRC staff has 
    conducted an environmental review of a request for an amendment to POL 
    No. DPR-73. The proposed amendment would revise the POL to allow the 
    licensee to conduct decommissioning at TMI-2 covering activities that 
    were not previously addressed in the staff's environmental assessments 
    (site-specific historical and cultural resources). Based on its 
    environmental review of the proposed action, the NRC staff has made a 
    finding of no significant impact in the EA. Therefore, the NRC staff 
    has determined, pursuant to 10 CFR 51.31, that preparation of an 
    environmental impact statement is not required for the proposed action 
    and a FONSI is appropriate.

        Dated: August 2, 2024.

        For the Nuclear Regulatory Commission.
    Christopher M. Regan,
    Director, Division of Rulemaking, Environmental, and Financial Support, 
    Office of Nuclear Material Safety, and Safeguards.
    [FR Doc. 2024-17521 Filed 8-7-24; 8:45 am]
    BILLING CODE 7590-01-P

    Federal Register, Volume 89 Issue 153 (Thursday, August 8, 2024)


    [Federal Register Volume 89, Number 153 (Thursday, August 8, 2024)]
    [Notices]
    [Pages 64964-64965]
    From the Federal Register Online via the Government Publishing Office [www.gpo.gov]
    [FR Doc No: 2024-17521]



    [[Page 64964]]"""  # noqa
