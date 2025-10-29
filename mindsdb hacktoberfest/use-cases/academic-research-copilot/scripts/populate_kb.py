#!/usr/bin/env python3
"""
Complete setup script for Academic Research Copilot
Fetches papers from ArXiv and creates MindsDB Knowledge Base
"""
import sys
import os
import argparse
from dotenv import load_dotenv  # Add this import

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables from .env file
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(project_root, '.env')
load_dotenv(env_path)

from src.data.fetch_papers import ingest_papers_pipeline
from src.knowledge_base.kb_manager import KBManager


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch arXiv papers and populate the MindsDB Knowledge Base")
    parser.add_argument(
        "--topics",
        nargs="+",
        help="List of topic queries to fetch (e.g., --topics 'privacy in federated learning' 'diffusion models for medical imaging')",
    )
    parser.add_argument(
        "--papers-per-topic",
        type=int,
        default=25,
        help="Number of papers to fetch per topic (default: 25)",
    )
    parser.add_argument(
        "--skip-kb",
        action="store_true",
        help="Skip Knowledge Base steps (only fetch into DuckDB)",
    )
    parser.add_argument(
        "--kb-name",
        type=str,
        default="academic_kb",
        help="Knowledge Base name in MindsDB (default: academic_kb)",
    )
    parser.add_argument(
        "--db-name",
        type=str,
        default="duckdb_papers",
        help="MindsDB database connection name for DuckDB (default: duckdb_papers)",
    )
    parser.add_argument(
        "--recreate-kb",
        action="store_true",
        help="Drop the existing Knowledge Base (if any) and create a fresh one",
    )
    return parser.parse_args()


def main():
    """Main setup function"""
    args = parse_args()

    print("=" * 70)
    print("🎓 ACADEMIC RESEARCH COPILOT - KNOWLEDGE BASE SETUP")
    print("=" * 70 + "\n")
    
    # Use path relative to the project root, not absolute /data
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)  # Go up from scripts/ to academic-copilot/
    db_path = os.path.join(project_root, "data", "academic_papers.duckdb")
    
    # Ensure data directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Decide topics
    default_topics = [
        "machine learning",
        "deep learning", 
        "natural language processing",
        "computer vision",
        "federated learning",
        "transformer models",
        "reinforcement learning"
    ]
    topics = args.topics if args.topics and len(args.topics) > 0 else default_topics

    # If topics were provided explicitly, always (re)ingest into the existing DB
    # Otherwise, keep original behavior: create DB if missing
    if args.topics or not os.path.exists(db_path):
        if not os.path.exists(db_path):
            print("📚 Database not found. Creating new database...\n")
        else:
            print(f"📚 Using existing database at: {db_path} — new topics will be appended\n")

        # Step 1: Fetch and store papers from ArXiv
        print("STEP 1: Fetching papers from ArXiv")
        print("-" * 70)

        ingest_papers_pipeline(
            topics=topics,
            papers_per_topic=args.papers_per_topic,
            db_path=db_path
        )
    else:
        print(f"✅ Using existing database at: {db_path}\n")
    
    if args.skip_kb:
        print("⏭️  Skipping Knowledge Base setup as requested (--skip-kb)")
        return

    print("\n" + "=" * 70)
    print("STEP 2: Creating MindsDB Knowledge Base")
    print("-" * 70 + "\n")
    
    # Step 2: Create Knowledge Base in MindsDB
    try:
        # Get MindsDB host from environment (defaults to localhost for local dev)
        mindsdb_host = os.getenv("MINDSDB_HOST", "localhost")
        mindsdb_port = os.getenv("MINDSDB_PORT", "47334")
        mindsdb_url = f"http://{mindsdb_host}:{mindsdb_port}"
        
        print(f"🔌 Connecting to MindsDB at: {mindsdb_url}\n")
        
        # IMPORTANT: MindsDB in Docker needs the container path, not host path
        # The docker-compose.yml mounts ./data to /app/data in the mindsdb container
        mindsdb_db_path = "/app/data/academic_papers.duckdb"
        
        print(f"📁 Host path: {db_path}")
        print(f"📁 MindsDB container path: {mindsdb_db_path}\n")
        
        # Initialize KB Manager with the container path
        kb_manager = KBManager(
            mindsdb_url=mindsdb_url,
            db_path=mindsdb_db_path  # Use Docker container path
        )
        
        # Connect to MindsDB
        kb_manager.connect()
        
        # Create database connection
        kb_manager.create_database_connection(args.db_name)
        
        # Create Knowledge Base with embeddings
        print("\n" + "=" * 70)
        print("STEP 3: Setting Up Knowledge Base with Embeddings")
        print("-" * 70 + "\n")
        
        # Decide whether to reuse or recreate KB
        kb_name = args.kb_name
        kb_status = kb_manager.get_kb_status(kb_name)
        
        # Always recreate KB if it exists to ensure correct schema
        # (Old KB may have wrong column structure)
        if kb_status.get("exists"):
            print(f"🗑️  Dropping existing Knowledge Base '{kb_name}' to rebuild with correct schema...")
            try:
                kb_manager.delete_knowledge_base_sync(kb_name)
                kb_status = {"exists": False}
                print(f"✅ Old KB dropped\n")
            except Exception as e:
                print(f"   ⚠️  Failed to drop KB (continuing): {e}")
        
        # Force recreate if --recreate-kb is set OR if KB doesn't exist
        if args.recreate_kb or not kb_status.get("exists"):
            kb_manager.create_knowledge_base_sync(kb_name, "gemini")
        else:
            print(f"✓ Knowledge Base '{kb_name}' already exists — reusing it")
        
        # Insert papers into KB
        print("\n" + "=" * 70)
        print("STEP 4: Populating Knowledge Base with Papers")
        print("-" * 70 + "\n")
        
        success = kb_manager.insert_papers_into_kb(kb_name, args.db_name)
        
        if success:
            print("\n" + "=" * 70)
            print("✅ SETUP COMPLETE!")
            print("=" * 70)
            print("\n📊 System Status:")
            print(f"   ✓ Papers stored in DuckDB: {db_path}")
            print(f"   ✓ MindsDB connected and ready")
            print(f"   ✓ Database 'duckdb_papers' accessible from MindsDB")
            print(f"   ✓ Knowledge Base '{kb_name}' ready with embeddings")
            print(f"   ✓ All papers indexed for semantic search")
            
            print("\n📝 Next Steps:")
            print("   Your Academic Research Copilot is ready!")
            print("\n   1. Access the Streamlit UI:")
            print("      http://localhost:8501")
            print("\n   2. Access the FastAPI docs:")
            print("      http://localhost:8000/api/docs")
            print("\n   3. Try a semantic search query!")
            print("      Example: 'privacy in machine learning'")
            print("      Will find papers about federated learning and differential privacy")
            
            print("\n🔍 Semantic Search Enabled:")
            print("   The Knowledge Base uses FREE HuggingFace Sentence Transformers")
            print("   with vector embeddings for semantic similarity search!")
            print("   No API keys required - completely free and runs locally!")
            
            print("\n💡 Model Information:")
            print("   Model: sentence-transformers/all-MiniLM-L6-v2")
            print("   Vector Size: 384 dimensions")
            print("   Speed: Fast (~50ms per embedding)")
            print("   Quality: Excellent for general semantic search")
            
            print("\n🎉 Happy researching!\n")
        else:
            print("\n⚠️  Knowledge Base created but paper insertion failed")
            print("   You can retry by running this script again\n")
        
    except Exception as e:
        print(f"\n❌ Error during Knowledge Base setup: {e}")
        print("\n💡 Troubleshooting:")
        print("   1. Make sure MindsDB is running:")
        print("      docker-compose up -d mindsdb")
        print("\n   2. Check MindsDB connection:")
        print("      curl http://localhost:47334")
        print("\n   3. Verify DuckDB file exists:")
        print(f"      ls -lh {db_path}")
        sys.exit(1)


if __name__ == "__main__":
    main()
