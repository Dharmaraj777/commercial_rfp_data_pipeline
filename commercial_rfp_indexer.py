from azure.core.credentials import AzureKeyCredential
import os
import json
import logging
from datetime import datetime
from azure.search.documents.indexes import SearchIndexerClient
from azure.search.documents.indexes.models import (
    SearchIndexerDataContainer,
    SearchIndexerDataSourceConnection
)
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
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
    SearchIndex
)
from azure.search.documents.indexes.models import (
    SplitSkill,
    InputFieldMappingEntry,
    OutputFieldMappingEntry,
    AzureOpenAIEmbeddingSkill,
    SearchIndexerIndexProjection,
    SearchIndexerIndexProjectionSelector,
    SearchIndexerIndexProjectionsParameters,
    IndexProjectionMode,
    SearchIndexerSkillset
)
 
# Define a directory for logs and ensure it exists
log_dir = 'RFP_indexer_logs'
today_date = datetime.now().strftime("%Y-%m-%d")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure logging to write detailed information to a log file
log_file_path = os.path.join(log_dir, f'RFP_indexer_logs{today_date}.log')
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,  # Using INFO level for general operational messages
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Set Azure SDK logging to WARNING to avoid too verbose output
azure_logger = logging.getLogger("azure.core")
azure_logger.setLevel(logging.WARNING)

logging.info("=== Starting Azure AI Search Index Creation Process ===")

'''Load environment variables and keys'''

# Load configuration
try:
    logging.info("Loading configuration from 'config.json'.")
    with open('config.json') as config_file:
        config_details = json.load(config_file)
    logging.info("Configuration loaded successfully.")
except Exception as e:
    logging.error(f"Error loading configuration: {e}")
    raise

# Load environment variables and keys
search_endpoint = config_details['cogsearch_endpoint']
azure_search_api_key = config_details['cogsearch_api_key']
blob_connection_string = config_details['storage_connection_string']
blob_container_name = config_details['commercial_rfp_survey_content_doc_library']
azure_openai_endpoint = config_details['openai_api_base']
azure_openai_key = config_details['openai_api_key']
azure_openai_embedding_deployment = config_details['openai_embedding_model']
openai_embedding_model_name = config_details['openai_embedding_model_name']
azure_openai_model_dimensions =1536
index_name = "commercial-rfp-survey-content-doc-library-index"
credential = AzureKeyCredential(azure_search_api_key)


# Create a blob data source connector on Azure AI Search

#from azure.search.documents.indexes.models import NativeBlobSoftDeleteDeletionDetectionPolicy

logging.info("Creating or updating blob data source connection for Azure AI Search.") 
# Create a data source
indexer_client = SearchIndexerClient(search_endpoint, credential)
container = SearchIndexerDataContainer(name=blob_container_name)
data_source_connection = SearchIndexerDataSourceConnection(
    name=f"{index_name}-datasource",
    type="azureblob",
    connection_string=blob_connection_string,
    container=container,
    #data_deletion_detection_policy=NativeBlobSoftDeleteDeletionDetectionPolicy()
)

# Initialize the SearchIndexerClient and create/update the data source.
indexer_client = SearchIndexerClient(search_endpoint, credential)
try:
    data_source = indexer_client.create_or_update_data_source_connection(data_source_connection)
    logging.info(f"Data source '{data_source.name}' created or updated successfully.")
except Exception as e:
    logging.error(f"Error creating/updating data source: {e}")
    raise

print(f"Data source '{data_source.name}' created or updated")



#Create a search index
logging.info("Creating search index with vector search and semantic configuration.")
 
# Create a search index  
index_client = SearchIndexClient(endpoint=search_endpoint, credential=credential)  
fields = [  
    SearchField(name="parent_id", type=SearchFieldDataType.String, sortable=True, filterable=True, facetable=True),  
    SearchField(name="title", type=SearchFieldDataType.String, sortable=True, filterable=True, searchable=False ), 
    SearchField(name="chunk_id", type=SearchFieldDataType.String, key=True, sortable=True, filterable=True, facetable=True, analyzer_name="keyword"),  
    SearchField(name="chunk", type=SearchFieldDataType.String, sortable=False, filterable=False, facetable=False),  
    SearchField(name="text_vector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single), vector_search_dimensions=1536, vector_search_profile_name="myHnswProfile"),  
    SearchField(name="metadata_creation_date", type=SearchFieldDataType.DateTimeOffset, sortable=True, filterable=False, searchable=False, facetable=False)
    
]  
 
# Configure the vector search configuration  
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
                resource_url=azure_openai_endpoint,  
                deployment_name=azure_openai_embedding_deployment,
                model_name=openai_embedding_model_name,  
                api_key=azure_openai_key,  
            ),  
        ),  
    ],  
)  
 
semantic_config = SemanticConfiguration(  
    name=f"{index_name}-semantic-configuration",  
    prioritized_fields=SemanticPrioritizedFields(  
        content_fields=[SemanticField(field_name="chunk")],
        title_field=SemanticField(field_name="title"),
        keywords_fields=[SemanticField(field_name="chunk")]
    ),  
)  
 
# Create the semantic search with the configuration  
semantic_search = SemanticSearch(configurations=[semantic_config])  
 
# # Create the search index
index = SearchIndex(name=index_name, fields=fields, vector_search=vector_search, semantic_search=semantic_search)  

# Initialize SearchIndexClient and create/update the index.
index_client = SearchIndexClient(endpoint=search_endpoint, credential=credential)
try:
    result = index_client.create_or_update_index(index)
    logging.info(f"Search index '{result.name}' created or updated successfully.")
except Exception as e:
    logging.error(f"Error creating/updating search index: {e}")
    raise

print(f"Search index '{result.name}' created")



#Create a skillset
logging.info("Creating skillset for document chunking and embedding generation.") 
 
# Create a skillset  
skillset_name = f"{index_name}-skillset"  
 
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
    resource_url=azure_openai_endpoint,  
    deployment_name=azure_openai_embedding_deployment, 
    model_name=openai_embedding_model_name, 
    api_key=azure_openai_key,  
    dimensions = azure_openai_model_dimensions,
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
            target_index_name=index_name,  
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
    name=skillset_name,  
    description="Skillset to chunk documents and generating embeddings",  
    skills=[split_skill, embedding_skill],  
    index_projection=index_projections,  
)  
 
# Create/update the skillset using the SearchIndexerClient.
try:
    indexer_client.create_or_update_skillset(skillset)
    logging.info(f"Skillset '{skillset.name}' created or updated successfully.")
except Exception as e:
    logging.error(f"Error creating/updating skillset: {e}")
    raise

print(f"Skillset '{skillset.name}' created") 


#Create an indexer
from azure.search.documents.indexes.models import (
    SearchIndexer,
    FieldMapping
)

logging.info("Creating indexer to process and index documents.")
 
# Create an indexer  
indexer_name = f"{index_name}-indexer"  
 
indexer = SearchIndexer(  
    name=indexer_name,  
    description="Indexer to index documents and generate embeddings",  
    skillset_name=skillset_name,  
    target_index_name=index_name,  
    data_source_name=data_source.name,  
    parameters= {
        "configuration": {
            "dataToExtract": "contentAndMetadata",
            "parsingMode": "default",
            "executionEnvironment": "private"
            }
   },
    # Map the metadata_storage_name field to the title field in the index to display the PDF title in the search results  
    field_mappings=[FieldMapping(source_field_name="title", target_field_name="title")]  
)  
 
# Create/update the indexer.
try:
    indexer_result = indexer_client.create_or_update_indexer(indexer)
    logging.info(f"Indexer '{indexer_result.name}' created or updated successfully.")
except Exception as e:
    logging.error(f"Error creating/updating indexer: {e}")
    raise

# Run the indexer to begin processing documents.
try:
    indexer_client.run_indexer(indexer_name)
    logging.info(f"Indexer '{indexer_name}' started successfully.")
except Exception as e:
    logging.error(f"Error running indexer '{indexer_name}': {e}")
    raise

print(f"Indexer '{indexer_name}' is created and running. If queries return no results, please wait a bit and try again.")

logging.info("=== Azure AI Search Index Creation Process Completed ===")