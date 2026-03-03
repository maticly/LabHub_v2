""" 
all-MiniLM-L6-v2 model (Chroma's default) to embed 
queries and retrieve the top-N results from a persistent 
local database. 
"""

import chromadb
from chromadb.utils import embedding_functions
from pathlib import Path
import streamlit as st

# ---Configuration
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CHROMA_DB_PATH = PROJECT_ROOT / "chroma_db"

@st.cache_resource
def get_chroma_collection():
    """Caches the Chroma connection to prevent empty UI lag."""
    #check if DB exists 
    if not CHROMA_DB_PATH.exists():
        st.warning(f"ChromaDB not found at: {CHROMA_DB_PATH}")
        return []
    try:
        client = chromadb.PersistentClient(path=str(CHROMA_DB_PATH))
        model = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
        return client.get_collection(name="lab_products", embedding_function=model)
    except Exception as e:
        st.warning(f"ChromaDB error: {e} | Path: {CHROMA_DB_PATH}")
        return []
    
def semantic_search(query_text, n_results=5):
    """
    Embeds the user query and returns a list of formatted results.
    Returns: List of dicts with keys: id, content, metadata, distance.
    """
    
    collection = get_chroma_collection()

    if collection is None:
        return []
    try:
        # run Query
        results = collection.query(
            query_texts=[query_text],
            n_results=n_results,
            include=["documents", "metadatas", "distances"]
        )

        #check if query return any results
        if not results['ids'] or not results['ids'][0]:
            return []
            
        # results for easy use in Streamlit
        formatted_results = []
        for i in range(len(results['ids'][0])):
            formatted_results.append({
                "id": results['ids'][0][i],
                "content": results['documents'][0][i],
                "metadata": results['metadatas'][0][i],
                "distance": results['distances'][0][i]
            })
            
        return formatted_results

    except Exception as e:
        print(f"Error accessing ChromaDB: {e}")
        return []

# --- TEST
if __name__ == "__main__":
    test_query = "Durable glass beaker"
    print(f"Testing query: {test_query}")
    matches = semantic_search(test_query, n_results=3)
    
    if not matches:
        print("No matches found.")
    for match in matches:
        #matches < 1.0 ususally mean a good match
        print(f"[{match['distance']:.4f}], Content: {match['content']}, ID: {match['id']}")