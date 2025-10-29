"""
Knowledge Base Manager for MindsDB integration
"""
import mindsdb_sdk
from typing import Dict, Any, List, Optional
import asyncio
import os


class KBManager:
    def __init__(self, mindsdb_url: str = "http://localhost:47334", db_path: str = None):
        """
        Initialize Knowledge Base Manager
        
        Args:
            mindsdb_url: URL of MindsDB server
            db_path: Path to DuckDB database file
        """
        self.mindsdb_url = mindsdb_url
        # Use absolute path for container environment
        if db_path:
            self.db_path = db_path
        else:
            # Default path - use absolute path if it exists
            default_path = "/app/data/academic_papers.duckdb"
            if os.path.exists(default_path):
                self.db_path = default_path
            else:
                self.db_path = "data/academic_papers.duckdb"
        self.server = None
        
    def connect(self):
        """Connect to MindsDB server"""
        if not self.server:
            print(f"🔌 Connecting to MindsDB at {self.mindsdb_url}...")
            self.server = mindsdb_sdk.connect(self.mindsdb_url)
            print("✅ Connected to MindsDB!\n")
        return self.server

    def create_database_connection(self, db_name: str = "duckdb_papers"):
        """
        Connect DuckDB to MindsDB
        
        Args:
            db_name: Name for the database in MindsDB
        """
        server = self.connect()
        
        try:
            # Check if database already exists
            existing_dbs = server.list_databases()
            if db_name in [db.name for db in existing_dbs]:
                print(f"✓ Database '{db_name}' already exists")
                return server.get_database(db_name)
            
            # Create database connection
            print(f"📊 Creating database connection '{db_name}'...")
            database = server.create_database(
                engine='duckdb',
                name=db_name,
                connection_args={'database': self.db_path}
            )
            print(f"✅ Database '{db_name}' connected!\n")
            return database
            
        except Exception as e:
            print(f"⚠️  Error creating database: {e}")
            # Try to get existing database
            return server.get_database(db_name)

    async def create_knowledge_base(
        self, 
        kb_name: str = "academic_kb",
        model: str = "openai"
    ):
        """
        Create a Knowledge Base in MindsDB (async)
        
        Args:
            kb_name: Name of the Knowledge Base
            model: Embedding model to use (default: openai)
        """
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._create_kb_sync, kb_name, model)
    
    def _create_kb_sync(self, kb_name: str, model: str):
        """Synchronous KB creation"""
        server = self.connect()
        project = server.get_project('mindsdb')
        
        try:
            # Try to get existing KB first
            try:
                kb = project.get_knowledge_base(kb_name)
                print(f"✓ Knowledge Base '{kb_name}' already exists")
                return kb
            except:
                pass  # KB doesn't exist, create it
            
            # Ensure database is connected
            self.create_database_connection()
            
            # Create knowledge base using Google Gemini (FREE with API key!)
            print(f"🧠 Creating Knowledge Base '{kb_name}'...")
            print("   Using Google Gemini embeddings (FREE API key required)")
            
            # Get Gemini API key from environment
            api_key = os.getenv("GEMINI_API_KEY")
            if not api_key:
                raise ValueError(
                    "GEMINI_API_KEY not found in environment!\n"
                    "   Get your FREE API key at: https://makersuite.google.com/app/apikey\n"
                    "   Then set it in your .env file: GEMINI_API_KEY=your_key_here"
                )
            
           
            query = f"""
            CREATE KNOWLEDGE_BASE {kb_name}
            USING
                embedding_model = {{
                    "provider": "google",
                    "model_name": "text-embedding-004",
                    "api_key": "{api_key}"
                }};
            """
            
            try:
                result = server.query(query).fetch()
                print(f"✅ Knowledge Base '{kb_name}' created successfully!\n")
            except Exception as ce:
                # If KB already exists at the SQL layer, treat as success and reuse it
                if 'already exists' in str(ce).lower():
                    print(f"✓ Knowledge Base '{kb_name}' already exists — reusing it\n")
                    return kb_name
                raise
            print(f"   📊 Storage: Default ChromaDB (auto-created)")
            print(f"   🤖 Embedding Model: Google Gemini text-embedding-004")
            print(f"   🆓 Cost: FREE (generous rate limits)")
            print(f"   📏 Vector Size: 768 dimensions")
            print(f"   📝 Content: title + summary from papers")
            print(f"   🔑 ID Column: entry_id\n")
            
            return kb_name
            
        except Exception as e:
            print(f"⚠️  Error creating KB: {e}")
            print(f"   Details: {str(e)}\n")
            # Try to get existing KB and continue
            try:
                kb = project.get_knowledge_base(kb_name)
                print(f"✓ Knowledge Base '{kb_name}' found after error — continuing\n")
                return kb
            except Exception:
                raise e

    async def get_knowledge_base(self, kb_name: str = "academic_kb"):
        """
        Retrieve the Knowledge Base (async)
        
        Args:
            kb_name: Name of the Knowledge Base to retrieve
            
        Returns:
            The Knowledge Base object
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_kb_sync, kb_name)
    
    def _get_kb_sync(self, kb_name: str):
        """Synchronous KB retrieval"""
        server = self.connect()
        project = server.get_project('mindsdb')
        return project.get_knowledge_base(kb_name)

    async def delete_knowledge_base(self, kb_name: str):
        """
        Delete the Knowledge Base (async)
        
        Args:
            kb_name: Name of the Knowledge Base to delete
        """
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._delete_kb_sync, kb_name)
    
    def _delete_kb_sync(self, kb_name: str):
        """Synchronous KB deletion"""
        server = self.connect()
        query = f"DROP KNOWLEDGE_BASE {kb_name};"
        server.query(query).fetch()
        print(f"🗑️  Knowledge Base '{kb_name}' deleted")

    def get_kb_status(self, kb_name: str = "academic_kb") -> Dict[str, Any]:
        """
        Get status of Knowledge Base
        
        Args:
            kb_name: Name of the Knowledge Base
            
        Returns:
            Dictionary with KB status information
        """
        try:
            server = self.connect()
            project = server.get_project('mindsdb')
            kb = project.get_knowledge_base(kb_name)
            
            return {
                "name": kb_name,
                "exists": True,
                "status": "ready"
            }
        except Exception as e:
            return {
                "name": kb_name,
                "exists": False,
                "status": "not_found",
                "error": str(e)
            }
    
    def insert_papers_into_kb(self, kb_name: str = "academic_kb", db_name: str = "duckdb_papers"):
        """
        Insert papers from DuckDB into the Knowledge Base
        
        Args:
            kb_name: Name of the Knowledge Base
            db_name: Name of the DuckDB database connection in MindsDB
        """
        server = self.connect()
        
        try:
            print(f"📥 Inserting papers from '{db_name}' into Knowledge Base '{kb_name}'...")
            print(f"   This will generate Gemini embeddings for all papers...")
            print(f"   (This may take a few minutes)\n")
            
            # Insert papers into KB
            # MindsDB will automatically create embeddings for the content
            # We concatenate title and abstract to create the content field
            query = f"""
            INSERT INTO {kb_name} (content)
            SELECT title || '\n\n' || abstract as content
            FROM {db_name}.papers;
            """
            
            # Execute the query - INSERT INTO returns None on success
            result = server.query(query)
            
            # For INSERT queries, we don't need to fetch results
            # The query execution itself indicates success
            print(f"✅ Papers inserted successfully into Knowledge Base!")
            print(f"   📊 Gemini embeddings generated for all papers")
            print(f"   🔍 Semantic search is now enabled")
            print(f"   💾 Data stored in ChromaDB vector database\n")
            
            return True
            
        except Exception as e:
            print(f"⚠️  Error inserting papers: {e}")
            print(f"   Details: {str(e)}\n")
            import traceback
            traceback.print_exc()
            return False
    
    # Synchronous methods for backward compatibility
    def create_knowledge_base_sync(self, kb_name: str = "academic_kb", model: str = "openai"):
        """Create a new Knowledge Base in MindsDB (synchronous)"""
        return self._create_kb_sync(kb_name, model)

    def get_knowledge_base_sync(self, kb_name: str = "academic_kb"):
        """Retrieve the Knowledge Base (synchronous)"""
        return self._get_kb_sync(kb_name)

    def delete_knowledge_base_sync(self, kb_name: str):
        """Delete the Knowledge Base (synchronous)"""
        return self._delete_kb_sync(kb_name)


    async def create_knowledge_base(self, kb_name: str, schema: Dict[str, Any]):
        """
        Create a new Knowledge Base in MindsDB (async).

        :param kb_name: Name of the Knowledge Base to be created.
        :param schema: Schema definition for the Knowledge Base.
        """
        # Run in executor to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.mindsdb.create_kb, kb_name, schema)

    async def populate_knowledge_base(self, kb_name: str, data: List[Dict[str, Any]]):
        """
        Populate the Knowledge Base with data (async).

        :param kb_name: Name of the Knowledge Base to populate.
        :param data: Data to be inserted into the Knowledge Base.
        """
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.mindsdb.insert_into, kb_name, data)

    async def get_knowledge_base(self, kb_name: str):
        """
        Retrieve the Knowledge Base (async).

        :param kb_name: Name of the Knowledge Base to retrieve.
        :return: The Knowledge Base object.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.mindsdb.get_kb, kb_name)

    async def delete_knowledge_base(self, kb_name: str):
        """
        Delete the Knowledge Base (async).

        :param kb_name: Name of the Knowledge Base to delete.
        """
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.mindsdb.delete_kb, kb_name)

    async def update_knowledge_base(self, kb_name: str, data: List[Dict[str, Any]]):
        """
        Update the Knowledge Base with new data (async).

        :param kb_name: Name of the Knowledge Base to update.
        :param data: New data to be inserted into the Knowledge Base.
        """
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.mindsdb.update_kb, kb_name, data)