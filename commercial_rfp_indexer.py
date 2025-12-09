import os
import json
from datetime import datetime
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexerClient, SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndexerDataContainer,
    SearchIndexerDataSourceConnection,
    SearchField,
    SearchFieldDataType,
    VectorSearch,
    HnswAlgorithmConfiguration,
    HnswParameters,
    VectorSearchAlgorithmMetric,
    ExhaustiveKnnAlgorithmConfiguration,
    ExhaustiveKnnParameters,
    VectorSearchProfile,
    AzureOpenAIVectorizer,
    AzureOpenAIVectorizerParameters,
    SemanticConfiguration,
    SemanticSearch,
    SemanticPrioritizedFields,
    SemanticField,
    SearchIndex,
    SplitSkill,
    InputFieldMappingEntry,
    OutputFieldMappingEntry,
    AzureOpenAIEmbeddingSkill,
    SearchIndexerIndexProjection,
    SearchIndexerIndexProjectionSelector,
    SearchIndexerIndexProjectionsParameters,
    IndexProjectionMode,
    SearchIndexerSkillset,
    SearchIndexer,
    FieldMapping,
)
from .commercial_rfp_shared_logger import logger
from .commercial_rfp_config_loader import ConfigLoader
 
class AzureAISearchResourceManager:
    def __init__(self):
        self.config_loader = ConfigLoader.get_instance()
        # config = self.config_loader.config_details

        # Search service configs
        self.search_endpoint =  self.config_loader.cogsearch_endpoint
        self.azure_search_api_key = self.config_loader.cogsearch_api_key
        self.blob_connection_string =  self.config_loader.connection_string
        self.blob_container_name = self.config_loader.commercial_rfp_survey_content_doc_library

        self.azure_openai_endpoint = self.config_loader.azure_openai_endpoint
        self.azure_openai_key = self.config_loader.azure_openai_key
        self.azure_openai_embedding_deployment = self.config_loader.azure_openai_embedding_deployment
        self.openai_embedding_model_name = self.config_loader.openai_embedding_model_name
        self.azure_openai_model_dimensions = 1536
        self.index_name = self.config_loader.index_name
        self.indexer_name = self.config_loader.indexer_name
        self.credential = AzureKeyCredential(self.azure_search_api_key)
        self.skillset_name = f"{self.index_name}-skillset"
        self.data_source_name = f"{self.index_name}-datasource"

        self.index_client = SearchIndexClient(endpoint=self.search_endpoint, credential=self.credential)
        self.indexer_client = SearchIndexerClient(endpoint=self.search_endpoint, credential=self.credential)



    def ensure_data_source(self):
        logger.info(f"Checking/creating data source connection: {self.data_source_name}")
        container = SearchIndexerDataContainer(name=self.blob_container_name)
        data_source_connection = SearchIndexerDataSourceConnection(
            name=self.data_source_name,
            type="azureblob",
            connection_string=self.blob_connection_string,
            container=container,
        )
        existing = None
        try:
            existing = self.indexer_client.get_data_source_connection(self.data_source_name)
            logger.info(f"Data source '{self.data_source_name}' already exists.")
        except Exception:
            # Data source doesn't exist, create it
            self.indexer_client.create_or_update_data_source_connection(data_source_connection)
            logger.info(f"Data source '{self.data_source_name}' created.")

    def ensure_index(self):
        logger.info(f"Checking/creating search index: {self.index_name}")
        fields = [
            SearchField(name="parent_id", type=SearchFieldDataType.String, sortable=True, filterable=True, facetable=True),
            SearchField(name="title", type=SearchFieldDataType.String, sortable=True, filterable=True, searchable=False ),
            SearchField(name="chunk_id", type=SearchFieldDataType.String, key=True, sortable=True, filterable=True, facetable=True, analyzer_name="keyword"),
            SearchField(name="chunk", type=SearchFieldDataType.String, sortable=False, filterable=False, facetable=False),
            SearchField(name="text_vector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single), vector_search_dimensions=1536, vector_search_profile_name="myHnswProfile"),
            SearchField(name="metadata_creation_date", type=SearchFieldDataType.DateTimeOffset, sortable=True, filterable=False, searchable=False, facetable=False),
        ]
        vector_search = VectorSearch(
            algorithms=[
                HnswAlgorithmConfiguration(
                    name="myHnsw",
                    parameters=HnswParameters(
                        m=4,
                        ef_construction=400,
                        ef_search=500,
                        metric=VectorSearchAlgorithmMetric.COSINE,
                    )
                ),
                ExhaustiveKnnAlgorithmConfiguration(
                    name="myExhaustiveKnn",
                    parameters=ExhaustiveKnnParameters(
                        metric=VectorSearchAlgorithmMetric.COSINE,
                    ),
                ),
            ],
            profiles=[
                VectorSearchProfile(
                    name="myHnswProfile",
                    algorithm_configuration_name="myHnsw",
                    vectorizer_name="myOpenAI",
                ),
                VectorSearchProfile(
                    name="myExhaustiveKnnProfile",
                    algorithm_configuration_name="myExhaustiveKnn",
                    vectorizer_name="myOpenAI",
                ),
            ],
            vectorizers=[
                AzureOpenAIVectorizer(
                    vectorizer_name="myOpenAI",
                    kind="azureOpenAI",
                    parameters=AzureOpenAIVectorizerParameters(
                        resource_url=self.azure_openai_endpoint,
                        deployment_name=self.azure_openai_embedding_deployment,
                        model_name=self.openai_embedding_model_name,
                        api_key=self.azure_openai_key,
                    ),
                ),
            ],
        )
        semantic_config = SemanticConfiguration(
            name=f"{self.index_name}-semantic-configuration",
            prioritized_fields=SemanticPrioritizedFields(
                content_fields=[SemanticField(field_name="chunk")],
                title_field=SemanticField(field_name="title"),
                keywords_fields=[SemanticField(field_name="chunk")]
            ),
        )
        semantic_search = SemanticSearch(configurations=[semantic_config])
        index = SearchIndex(
            name=self.index_name,
            fields=fields,
            vector_search=vector_search,
            semantic_search=semantic_search
        )
        try:
            existing = self.index_client.get_index(self.index_name)
            logger.info(f"Index '{self.index_name}' already exists.")
        except Exception:
            # Create index if missing
            self.index_client.create_or_update_index(index)
            logger.info(f"Index '{self.index_name}' created.")


    def ensure_skillset(self):
        logger.info(f"Checking/creating skillset: {self.skillset_name}")
        split_skill = SplitSkill(
            description="Split skill to chunk documents",
            text_split_mode="pages",
            context="/document",
            maximum_page_length=2000,
            page_overlap_length=500,
            inputs=[
                InputFieldMappingEntry(name="text", source="/document/content"),
            ],
            outputs=[
                OutputFieldMappingEntry(name="textItems", target_name="pages")
            ],
        )
        embedding_skill = AzureOpenAIEmbeddingSkill(
            description="Skill to generate embeddings via Azure OpenAI",
            context="/document/pages/*",
            resource_url=self.azure_openai_endpoint,
            deployment_name=self.azure_openai_embedding_deployment,
            model_name=self.openai_embedding_model_name,
            api_key=self.azure_openai_key,
            dimensions=self.azure_openai_model_dimensions,
            inputs=[
                InputFieldMappingEntry(name="text", source="/document/pages/*"),
            ],
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="text_vector")
            ],
        )
        index_projections = SearchIndexerIndexProjection(
            selectors=[
                SearchIndexerIndexProjectionSelector(
                    target_index_name=self.index_name,
                    parent_key_field_name="parent_id",
                    source_context="/document/pages/*",
                    mappings=[
                        InputFieldMappingEntry(name="chunk", source="/document/pages/*"),
                        InputFieldMappingEntry(name="text_vector", source="/document/pages/*/text_vector"),
                        InputFieldMappingEntry(name="title", source="/document/metadata_storage_name"),
                        InputFieldMappingEntry(name="metadata_creation_date", source="/document/metadata_creation_date")
                    ],
                ),
            ],
            parameters=SearchIndexerIndexProjectionsParameters(
                projection_mode=IndexProjectionMode.SKIP_INDEXING_PARENT_DOCUMENTS
            ),
        )
        skillset = SearchIndexerSkillset(
            name=self.skillset_name,
            description="Skillset to chunk documents and generating embeddings",
            skills=[split_skill, embedding_skill],
            index_projection=index_projections,
        )
        try:
            existing = self.indexer_client.get_skillset(self.skillset_name)
            logger.info(f"Skillset '{self.skillset_name}' already exists.")
        except Exception:
            # Create skillset if missing
            self.indexer_client.create_or_update_skillset(skillset)
            logger.info(f"Skillset '{self.skillset_name}' created.")

    def ensure_indexer(self):
        logger.info(f"Checking/creating indexer: {self.indexer_name}")
        indexer = SearchIndexer(
            name=self.indexer_name,
            description="Indexer to index documents and generate embeddings",
            skillset_name=self.skillset_name,
            target_index_name=self.index_name,
            data_source_name=self.data_source_name,
            parameters={
                "configuration": {
                    "dataToExtract": "contentAndMetadata",
                    "parsingMode": "default",
                    "executionEnvironment": "private"
                }
            },
            field_mappings=[FieldMapping(source_field_name="title", target_field_name="title")]
        )
        try:
            existing = self.indexer_client.get_indexer(self.indexer_name)
            logger.info(f"Indexer '{self.indexer_name}' already exists.")
        except Exception:
            self.indexer_client.create_or_update_indexer(indexer)
            logger.info(f"Indexer '{self.indexer_name}' created.")


    def run_indexer(self):
        try:
            self.indexer_client.run_indexer(self.indexer_name)
            logger.info(f"Indexer '{self.indexer_name}' started successfully.")
        except Exception as e:
            logger.error(f"Error running indexer '{self.indexer_name}': {e}")

    def ensure_all_resources_exist(self):
        logger.info("=== Ensuring Azure AI Search Resources Exist ===")
        self.ensure_data_source()
        self.ensure_index()
        self.ensure_skillset()
        self.ensure_indexer()
        # self.run_indexer()
        logger.info("=== All Azure AI Search Resources checked/created ===")
