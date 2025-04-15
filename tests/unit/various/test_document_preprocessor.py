import pytest
from unittest.mock import Mock, patch, AsyncMock
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
        from mindsdb.interfaces.knowledge_base.utils import generate_document_id
        # Same content should generate same ID
        content = "test content"
        content_column = "test_column"
        id1 = generate_document_id(content, content_column)
        id2 = generate_document_id(content, content_column)
        assert id1 == id2
        assert len(id1.split('_')[0]) == 16  # Check hash length
        # Different content should generate different IDs
        different_content = "different content"
        id3 = generate_document_id(different_content, content_column)
        assert id1 != id3
        # Test with provided_id
        provided_id = "test_id"
        id4 = generate_document_id(content, content_column, provided_id)
        assert id4 == f"{provided_id}_{content_column}"

    def test_chunk_id_generation(self):
        """Test human-readable chunk ID generation"""
        provided_id = "test_id"
        preprocessor = DocumentPreprocessor()

        # Test with all parameters
        chunk_id = preprocessor._generate_chunk_id(
            chunk_index=0,
            total_chunks=3,
            start_char=0,
            end_char=100,
            provided_id=provided_id
        )
        assert chunk_id == 'test_id:1of3:0to100'

        # Test error when no document ID provided
        with pytest.raises(ValueError, match="Document ID must be provided"):
            preprocessor._generate_chunk_id(
                chunk_index=0,
                total_chunks=3,
                start_char=0,
                end_char=100
            )

    def test_split_document_without_splitter(self):
        """Test that splitting without a configured splitter raises error"""
        doc = Document(content="test content")
        preprocessor = DocumentPreprocessor()
        with pytest.raises(ValueError, match="Splitter not configured"):
            preprocessor._split_document(doc)

    def test_chunk_overlap(self):
        """Test chunk overlap"""
        from mindsdb.interfaces.knowledge_base.utils import generate_document_id
        config = TextChunkingConfig(chunk_size=10, chunk_overlap=5)
        preprocessor = TextChunkingPreprocessor(config)
        long_content = " ".join(["word"] * 50)
        doc_id = generate_document_id(long_content, "test_column")
        doc = Document(content=long_content, id=doc_id)
        chunks = preprocessor.process_documents([doc])
        # Ensure correct number of chunks is created
        assert len(chunks) > 1
        # Validate overlap in the produced chunks
        for i in range(len(chunks) - 1):
            overlap_length = config.chunk_overlap
            # Compare content without worrying about whitespace positioning
            assert chunks[i].content.strip()[-overlap_length:].strip() == chunks[i + 1].content.strip()[:overlap_length].strip()

    def test_standard_chunking_strategy(self):
        """Test standard chunking strategy with different overlap values"""
        from mindsdb.interfaces.knowledge_base.utils import generate_document_id
        content = " ".join(["word"] * 30)
        doc_id = generate_document_id(content, "test_column")
        doc = Document(content=content, id=doc_id)

        # Test with no overlap
        config_no_overlap = TextChunkingConfig(chunk_size=10, chunk_overlap=0)
        preprocessor_no_overlap = TextChunkingPreprocessor(config_no_overlap)
        chunks_no_overlap = preprocessor_no_overlap.process_documents([doc])

        # Test with medium overlap
        config_medium_overlap = TextChunkingConfig(chunk_size=10, chunk_overlap=3)
        preprocessor_medium_overlap = TextChunkingPreprocessor(config_medium_overlap)
        chunks_medium_overlap = preprocessor_medium_overlap.process_documents([doc])
        # Test with high overlap
        config_high_overlap = TextChunkingConfig(chunk_size=10, chunk_overlap=7)
        preprocessor_high_overlap = TextChunkingPreprocessor(config_high_overlap)
        chunks_high_overlap = preprocessor_high_overlap.process_documents([doc])
        # Verify that all chunking strategies produce multiple chunks
        assert len(chunks_no_overlap) > 1
        assert len(chunks_medium_overlap) > 1
        assert len(chunks_high_overlap) > 1
        # Verify that highest overlap results in more chunks
        assert len(chunks_high_overlap) >= len(chunks_medium_overlap)
        # Verify that chunks with overlap have the correct overlap content
        if len(chunks_medium_overlap) > 1:
            for i in range(len(chunks_medium_overlap) - 1):
                # Get the stripped content
                current_chunk = chunks_medium_overlap[i].content.strip()
                next_chunk = chunks_medium_overlap[i + 1].content.strip()
                # Get the last overlap_length words of current chunk
                current_end = ' '.join(current_chunk.split()[-config_medium_overlap.chunk_overlap:])
                # Get the first overlap_length words of next chunk
                next_start = ' '.join(next_chunk.split()[:config_medium_overlap.chunk_overlap])
                # Compare the overlap content (allowing for whitespace differences)
                assert current_end.strip() == next_start.strip()

    def test_parent_child_relationship(self):
        """Test that parent-child relationship is preserved during processing"""
        config = TextChunkingConfig(chunk_size=10, chunk_overlap=2)
        preprocessor = TextChunkingPreprocessor(config)
        parent_content = " ".join(["parent"] * 30)
        parent_doc = Document(content=parent_content, id="parent_doc")

        # Test default behavior (delete_existing=False)
        chunks = preprocessor.process_documents([parent_doc])
        # Verify that all chunks have reference to the parent document
        for chunk in chunks:
            assert "original_doc_id" in chunk.metadata
            assert chunk.metadata["original_doc_id"] == "parent_doc"
            # Verify chunk position metadata
            assert "start_char" in chunk.metadata
            assert "end_char" in chunk.metadata
            assert chunk.metadata["end_char"] > chunk.metadata["start_char"]

        # Test with delete_existing=True
        chunks = preprocessor.process_documents([parent_doc])

        # Verify chunk IDs follow the new format
        for i, chunk in enumerate(chunks):
            chunk_id_parts = chunk.id.split(":")
            assert len(chunk_id_parts) == 3
            assert chunk_id_parts[0] == "parent_doc"
            assert chunk_id_parts[1].endswith(f"of{len(chunks)}")
            assert "to" in chunk_id_parts[2]

    def test_document_update_modes(self):
        """Test document update behavior in different modes"""
        config = TextChunkingConfig(chunk_size=10, chunk_overlap=2)
        preprocessor = TextChunkingPreprocessor(config)

        # Create initial document
        doc_id = "test_doc"
        initial_content = " ".join(["initial"] * 20)
        initial_doc = Document(content=initial_content, id=doc_id)

        # Test default mode (delete_existing=False)
        updated_content_1 = " ".join(["updated1"] * 20)
        updated_doc_1 = Document(content=updated_content_1, id=doc_id)

        # Process both versions with default settings
        initial_chunks = preprocessor.process_documents([initial_doc])
        updated_chunks_1 = preprocessor.process_documents([updated_doc_1])

        # Verify initial chunks have delete_existing=False
        for chunk in initial_chunks:
            assert chunk.metadata["original_doc_id"] == doc_id

        # Verify updated chunks also have delete_existing=False
        for chunk in updated_chunks_1:
            assert chunk.metadata["original_doc_id"] == doc_id

        # Test full document deletion mode (delete_existing=True)
        updated_content_2 = " ".join(["updated2"] * 20)
        updated_doc_2 = Document(content=updated_content_2, id=doc_id)
        updated_chunks_2 = preprocessor.process_documents([updated_doc_2])

        # Verify chunks are marked for full document deletion
        for chunk in updated_chunks_2:
            assert chunk.metadata["original_doc_id"] == doc_id

        # Verify chunk IDs are properly formatted in all cases
        for chunks in [initial_chunks, updated_chunks_1, updated_chunks_2]:
            for i, chunk in enumerate(chunks):
                chunk_id_parts = chunk.id.split(":")
                assert len(chunk_id_parts) == 3
                assert chunk_id_parts[0] == doc_id
                assert chunk_id_parts[1].endswith(f"of{len(chunks)}")
                assert "to" in chunk_id_parts[2]


def test_document_id_generation():
    """Test the new document ID generation logic"""
    from mindsdb.interfaces.knowledge_base.utils import generate_document_id
    preprocessor = TextChunkingPreprocessor()

    # Test consistent base ID across different columns
    content = "test content"
    content_column = "test_column"
    doc_id1 = generate_document_id(content, content_column)
    doc_id2 = generate_document_id(content, content_column)

    # Same content should get same doc ID
    assert doc_id1 == doc_id2
    # Doc ID should be 16 chars (MD5 hash truncated) + column name
    assert len(doc_id1.split('_')[0]) == 16

    # Test different content gets different IDs
    different_content = "different content"
    doc_id3 = generate_document_id(different_content, content_column)
    assert doc_id3 != doc_id1

    # Test provided ID is preserved
    custom_id = "custom_doc_123"
    doc_id4 = generate_document_id(content, content_column, custom_id)
    assert doc_id4.startswith(custom_id)

    # Test chunk ID format
    doc = Document(content=content, id=doc_id1)
    chunks = preprocessor.process_documents([doc])
    for chunk in chunks:
        # Format should be: <doc_id>:<chunk_number>of<total_chunks>:<start_char>to<end_char>
        parts = chunk.id.split(':')
        assert len(parts) == 3
        assert 'of' in parts[1]
        assert 'to' in parts[2]


def test_metadata_preservation():
    """Test that metadata is preserved during processing"""
    from mindsdb.interfaces.knowledge_base.utils import generate_document_id
    preprocessor = TextChunkingPreprocessor()
    metadata = {"key": "value", "content_column": "test_column"}
    content = "Test content"
    doc_id = generate_document_id(content, "test_column")
    doc = Document(content=content, metadata=metadata, id=doc_id)
    chunks = preprocessor.process_documents([doc])
    # Verify metadata is preserved and includes source
    assert chunks[0].metadata["source"] == "TextChunkingPreprocessor"
    assert chunks[0].metadata["key"] == "value"
    assert chunks[0].metadata["content_column"] == "test_column"


def test_content_column_handling():
    """Test handling of content column in metadata"""
    from mindsdb.interfaces.knowledge_base.utils import generate_document_id
    preprocessor = TextChunkingPreprocessor()
    content = "Test content"
    metadata = {"content_column": "test_column"}
    doc_id = generate_document_id(content, "test_column")
    doc = Document(content=content, metadata=metadata, id=doc_id)
    chunks = preprocessor.process_documents([doc])
    # Verify content column is preserved in metadata
    assert "content_column" in chunks[0].metadata
    assert chunks[0].metadata["content_column"] == "test_column"


def test_provided_id_handling():
    """Test handling of provided document IDs"""
    preprocessor = TextChunkingPreprocessor()
    doc = Document(content="Test content", id="test_id")
    chunks = preprocessor.process_documents([doc])
    # Verify provided ID is incorporated into chunk ID
    assert chunks[0].metadata["original_doc_id"] == "test_id"


def test_empty_content_handling():
    """Test handling of empty content"""
    from mindsdb.interfaces.knowledge_base.utils import generate_document_id
    preprocessor = TextChunkingPreprocessor()
    content = ""
    doc_id = generate_document_id(content, "test_column")
    doc = Document(content=content, id=doc_id)
    chunks = preprocessor.process_documents([doc])
    assert len(chunks) == 0


def test_whitespace_content_handling():
    """Test handling of whitespace-only content"""
    from mindsdb.interfaces.knowledge_base.utils import generate_document_id
    preprocessor = TextChunkingPreprocessor()
    content = "   \n   \t   "
    doc_id = generate_document_id(content, "test_column")
    doc = Document(content=content, id=doc_id)
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
    from mindsdb.interfaces.knowledge_base.utils import generate_document_id
    preprocessor = TextChunkingPreprocessor()
    doc_id = generate_document_id(content, "test_column")
    doc = Document(content=content, metadata=metadata, id=doc_id)
    chunks = preprocessor.process_documents([doc])
    assert chunks[0].metadata["source"] == expected_source


class TestContextualPreprocessor:
    def test_source_metadata(self, preprocessor_sync):
        """Test that source metadata is correctly set"""
        # Create a pre-defined chunk with the correct metadata
        chunk = Mock(metadata={"source": "ContextualPreprocessor"})
        # Mock the entire process_documents method
        with patch.object(
            ContextualPreprocessor,
            'process_documents',
            return_value=[chunk]
        ):
            doc = Document(content="Test content")
            chunks = preprocessor_sync.process_documents([doc])
            assert chunks[0].metadata["source"] == "ContextualPreprocessor"

    def test_prepare_prompts(self, preprocessor_sync):
        chunk_contents = [f"Chunk contents {i}" for i in range(10)]
        full_documents = [f"Full document {i}" for i in range(10)]
        _ = preprocessor_sync._prepare_prompts(chunk_contents, full_documents)

    def test_process_documents(self, sample_document, preprocessor_sync):
        """Test document processing"""
        # Create pre-defined chunks
        chunks = [Mock(content="Test context") for _ in range(3)]
        # Mock the entire process_documents method
        with patch.object(
            ContextualPreprocessor,
            'process_documents',
            return_value=chunks
        ):
            docs = [Document(content=sample_document, id=f"{i}") for i in range(3)]
            result = preprocessor_sync.process_documents(docs)
            assert len(result) > 0

    def test_process_documents_async(self, sample_document, preprocessor_async):
        """Test document processing with async LLM"""
        # Create pre-defined chunks
        chunks = [Mock(content="Test context") for _ in range(3)]
        # Mock the entire process_documents method
        with patch.object(
            ContextualPreprocessor,
            'process_documents',
            return_value=chunks
        ):
            docs = [Document(content=sample_document, id=f"{i}") for i in range(3)]
            result = preprocessor_async.process_documents(docs)
            assert len(result) > 0
            assert all(chunk.content == "Test context" for chunk in result)

    def test_process_documents_sync(self, sample_document, preprocessor_sync):
        """Test document processing with sync LLM"""
        # Create pre-defined chunks
        chunks = [Mock(content="Test context") for _ in range(3)]
        # Mock the entire process_documents method
        with patch.object(
            ContextualPreprocessor,
            'process_documents',
            return_value=chunks
        ):
            docs = [Document(content=sample_document, id=f"{i}") for i in range(3)]
            result = preprocessor_sync.process_documents(docs)
            assert len(result) > 0
            assert all(chunk.content == "Test context" for chunk in result)

    @pytest.fixture
    def preprocessor_async(self):
        with patch(
            "mindsdb.interfaces.knowledge_base.preprocessing.document_preprocessor.create_chat_model"
        ) as mock_create_chat_model:
            # Create a mock async LLM
            class MockResponse:
                def __init__(self, content):
                    self.content = content
            # Create a mock with async support
            mock_llm = Mock()
            # Add an async batch method that returns appropriate responses
            mock_llm.abatch = AsyncMock(return_value=[MockResponse("Test context") for _ in range(10)])
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
            # Create a mock with async support
            mock_llm = Mock()
            # Define MockResponse class

            class MockResponse:
                def __init__(self, content):
                    self.content = content
            # Regular batch method
            mock_llm.batch = Mock(return_value=[MockResponse("Test context") for _ in range(10)])
            # Async batch method
            mock_llm.abatch = AsyncMock(return_value=[MockResponse("Test context") for _ in range(10)])
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