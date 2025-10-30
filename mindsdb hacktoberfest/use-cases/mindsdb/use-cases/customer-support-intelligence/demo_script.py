"""
Customer Support Intelligence with MindsDB Knowledge Bases
Demo Script - Hacktoberfest 2025
"""

import mindsdb_sdk
import pandas as pd
import os
from dotenv import load_dotenv
from tabulate import tabulate

# Load environment variables
load_dotenv()

def connect_to_mindsdb():
    """Connect to MindsDB"""
    print("🔗 Connecting to MindsDB...")
    server = mindsdb_sdk.connect(
        login=os.getenv('MINDSDB_EMAIL'),
        password=os.getenv('MINDSDB_PASSWORD')
    )
    print("✅ Connected successfully\n")
    return server

def load_data():
    """Load support tickets data"""
    print("📊 Loading support tickets data...")
    df = pd.read_csv('data/support_tickets.csv')
    print(f"✅ Loaded {len(df)} tickets\n")
    return df

def create_knowledge_base(server, kb_name='support_tickets_kb'):
    """Create or get Knowledge Base"""
    print(f"📚 Setting up Knowledge Base: {kb_name}...")
    try:
        kb = server.knowledge_bases.get(kb_name)
        print(f"✅ Using existing Knowledge Base\n")
    except:
        kb = server.knowledge_bases.create(
            name=kb_name,
            model='openai',
            storage='chromadb'
        )
        print(f"✅ Created new Knowledge Base\n")
    return kb

def ingest_data(kb, df):
    """Ingest tickets into Knowledge Base"""
    print("📥 Ingesting data into Knowledge Base...")
    
    for idx, row in df.iterrows():
        content = f"""Ticket: {row['ticket_id']}
Subject: {row['subject']}
Category: {row['category']}
Priority: {row['priority']}
Description: {row['description']}
Resolution: {row['resolution'] if pd.notna(row['resolution']) else 'Not yet resolved'}
Customer: {row['customer_name']}
Agent: {row['agent_name'] if pd.notna(row['agent_name']) else 'Unassigned'}"""
        
        kb.insert([{
            'content': content,
            'metadata': {
                'ticket_id': row['ticket_id'],
                'category': row['category'],
                'priority': row['priority'],
                'status': row['status'],
                'created_date': row['created_date'],
                'customer_name': row['customer_name']
            }
        }])
    
    print(f"✅ Successfully ingested {len(df)} tickets\n")

def semantic_search_demo(kb):
    """Demonstrate semantic search"""
    print("=" * 80)
    print("🔍 SEMANTIC SEARCH DEMO")
    print("=" * 80 + "\n")
    
    queries = [
        "Users cannot login to the system",
        "Customer was charged twice",
        "Application is running very slow"
    ]
    
    for query in queries:
        print(f"Query: '{query}'\n")
        results = kb.search(query, limit=2)
        
        for i, result in enumerate(results, 1):
            metadata = result.get('metadata', {})
            print(f"Result {i}:")
            print(f"  Ticket: {metadata.get('ticket_id')}")
            print(f"  Category: {metadata.get('category')}")
            print(f"  Priority: {metadata.get('priority')}")
            print(f"  Content: {result['content'][:150]}...")
            print()
        print("-" * 80 + "\n")

def hybrid_search_demo(kb):
    """Demonstrate hybrid search with filters"""
    print("=" * 80)
    print("🎯 HYBRID SEARCH DEMO (Semantic + Metadata Filtering)")
    print("=" * 80 + "\n")
    
    # Example 1: Critical technical issues
    print("Query: 'database connection problems'")
    print("Filters: priority='Critical', category='Technical'\n")
    
    results = kb.search(
        "database connection problems",
        limit=3,
        filters={
            'priority': 'Critical',
            'category': 'Technical'
        }
    )
    
    for i, result in enumerate(results, 1):
        metadata = result.get('metadata', {})
        print(f"{i}. {metadata.get('ticket_id')} - {metadata.get('priority')} - {metadata.get('category')}")
        print(f"   {result['content'][:120]}...\n")
    
    print("-" * 80 + "\n")

def evaluate_kb(kb):
    """Evaluate Knowledge Base performance"""
    print("=" * 80)
    print("📊 KNOWLEDGE BASE EVALUATION")
    print("=" * 80 + "\n")
    
    evaluation_queries = [
        {'query': 'login authentication problems', 'expected': 'TKT-001'},
        {'query': 'duplicate billing charges', 'expected': 'TKT-002'},
        {'query': 'API rate limiting issues', 'expected': 'TKT-003'},
        {'query': 'slow performance and timeouts', 'expected': 'TKT-005'},
        {'query': 'database connection errors', 'expected': 'TKT-009'}
    ]
    
    hit_at_1 = 0
    hit_at_3 = 0
    reciprocal_ranks = []
    
    for eval_query in evaluation_queries:
        results = kb.search(eval_query['query'], limit=5)
        
        position = None
        for idx, result in enumerate(results, 1):
            if result.get('metadata', {}).get('ticket_id') == eval_query['expected']:
                position = idx
                break
        
        if position:
            if position == 1:
                hit_at_1 += 1
            if position <= 3:
                hit_at_3 += 1
            reciprocal_ranks.append(1.0 / position)
        else:
            reciprocal_ranks.append(0.0)
    
    num_queries = len(evaluation_queries)
    mrr = sum(reciprocal_ranks) / num_queries
    
    print(f"Hit@1: {(hit_at_1/num_queries)*100:.2f}%")
    print(f"Hit@3: {(hit_at_3/num_queries)*100:.2f}%")
    print(f"Mean Reciprocal Rank (MRR): {mrr:.4f}\n")
    
    print("💡 Interpretation:")
    print(f"  - Expected ticket appears as top result {(hit_at_1/num_queries)*100:.0f}% of the time")
    print(f"  - Expected ticket appears in top 3 results {(hit_at_3/num_queries)*100:.0f}% of the time")
    print(f"  - On average, expected ticket appears at position {1/mrr:.1f}\n")

def main():
    """Main execution"""
    print("\n" + "=" * 80)
    print("🎃 MINDSDB HACKTOBERFEST 2025")
    print("Customer Support Intelligence with Knowledge Bases")
    print("=" * 80 + "\n")
    
    # Setup
    server = connect_to_mindsdb()
    df = load_data()
    kb = create_knowledge_base(server)
    
    # Ingest data (comment out if already ingested)
    # ingest_data(kb, df)
    
    # Demonstrations
    semantic_search_demo(kb)
    hybrid_search_demo(kb)
    evaluate_kb(kb)
    
    print("=" * 80)
    print("✅ Demo completed successfully!")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    main()
