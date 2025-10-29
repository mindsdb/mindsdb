import mindsdb_sdk
import os

def setup_knowledge_base():
    """Create MindsDB Knowledge Base from existing DuckDB papers"""
    
    print("=" * 70)
    print("🎓 CREATING MINDSDB KNOWLEDGE BASE")
    print("=" * 70 + "\n")
    
    # Step 1: Connect to MindsDB
    print("STEP 1: Connecting to MindsDB")
    print("-" * 70)
    
    mindsdb_url = "http://localhost:47334"
    server = mindsdb_sdk.connect(mindsdb_url)
    print(f"✅ Connected to MindsDB at {mindsdb_url}\n")
    
    # Step 2: Create DuckDB database connection
    print("STEP 2: Creating DuckDB Connection")
    print("-" * 70)
    
    # Fix: Go up one directory to find the data folder
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    db_path = os.path.join(parent_dir, "data", "academic_papers.duckdb")
    
    print(f"   Looking for database at: {db_path}")
    
    # Verify the file exists
    if not os.path.exists(db_path):
        print(f"❌ Error: Database file not found at {db_path}")
        print("\n💡 Please run fetch_papers.py first to create the database")
        return
    
    try:
        # Drop existing database if it exists
        try:
            server.databases.drop('duckdb_papers')
            print("   Dropped existing database connection")
        except:
            pass
        
        # Create new database connection
        db = server.databases.create(
            name='duckdb_papers',
            engine='duckdb',
            connection_args={
                'database': db_path
            }
        )
        print(f"✅ DuckDB connected to MindsDB")
        print(f"   Database path: {db_path}\n")
        
    except Exception as e:
        print(f"❌ Error creating database connection: {e}\n")
        return
    
    # Step 3: Create Knowledge Base
    print("STEP 3: Creating Knowledge Base with Embeddings")
    print("-" * 70)
    
    try:
        # Drop existing KB if it exists
        try:
            server.knowledge_bases.drop('academic_kb')
            print("   Dropped existing knowledge base")
        except:
            pass
        
        # Create Knowledge Base - SDK doesn't support 'model' parameter
        # It will use default embedding model
        kb = server.knowledge_bases.create(
            name='academic_kb'
        )
        print("✅ Knowledge Base 'academic_kb' created")
        print("   Using default embedding model\n")
        
    except Exception as e:
        print(f"❌ Error creating knowledge base: {e}\n")
        return
    
    # Step 4: Insert papers into Knowledge Base
    print("STEP 4: Inserting Papers with Embeddings")
    print("-" * 70)
    
    try:
        # Insert all papers - MindsDB will auto-generate embeddings!
        kb.insert(
            query="""
                SELECT 
                    id,
                    title || '\n\n' || abstract as content,
                    title,
                    authors,
                    published_date,
                    pdf_url,
                    categories
                FROM duckdb_papers.papers
            """
        )
        print("✅ Papers inserted successfully!")
        print("   Embeddings generated automatically by MindsDB\n")
        
        # Verify insertion
        result = server.query("""
            SELECT COUNT(*) as count 
            FROM duckdb_papers.papers
        """).fetch()
        paper_count = result['count'][0]
        
        print("=" * 70)
        print("✅ SETUP COMPLETE!")
        print("=" * 70)
        print(f"\n📊 System Status:")
        print(f"   ✓ Total papers: {paper_count}")
        print(f"   ✓ Knowledge Base: academic_kb")
        print(f"   ✓ Semantic search: ENABLED")
        print(f"   ✓ Embeddings: Generated automatically")
        
        print("\n🔍 Try a search:")
        print("   query = 'privacy in machine learning'")
        
        print("\n💡 Next steps:")
        print("   1. Run: python scripts/test_search.py")
        print("   2. Or use the Knowledge Base in your application\n")
        
    except Exception as e:
        print(f"❌ Error inserting papers: {e}")
        print("\n💡 Troubleshooting:")
        print("   1. Verify papers exist:")
        print(f"      python -c \"import duckdb; print(duckdb.connect('{db_path}').execute('SELECT COUNT(*) FROM papers').fetchone())\"")
        print("\n   2. Check MindsDB logs:")
        print("      docker logs academic-copilot-mindsdb-1")

if __name__ == "__main__":
    setup_knowledge_base()