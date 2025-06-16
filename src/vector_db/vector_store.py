import os
import json
from typing import List, Dict, Any, Union, Optional
from pathlib import Path
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

from src.models.dsl_models import DSLComponent, DSLType
from dotenv import load_dotenv

load_dotenv()


class VectorStore:
    """Vector database for storing and retrieving DSL components"""
    
    def __init__(self, model_name: str = None, vector_db_dir: str = "data/vector_db"):
        self.model_name = model_name or os.getenv('MODEL_NAME', 'sentence-transformers/all-MiniLM-L6-v2')
        self.vector_db_dir = vector_db_dir
        self.model = SentenceTransformer(self.model_name)
        self.vectors = {}  # type: Dict[DSLType, Dict[str, Any]]
        self.dsl_components = {}  # type: Dict[DSLType, List[DSLComponent]]
        
        self.ensure_vector_db_dir()
        self.load_vectors()
    
    def ensure_vector_db_dir(self):
        """Ensure the vector database directory exists"""
        Path(self.vector_db_dir).mkdir(parents=True, exist_ok=True)
    
    def get_vector_path(self, dsl_type: DSLType) -> str:
        """Get the path to the vector file for a DSL type"""
        return os.path.join(self.vector_db_dir, f"{dsl_type.value.lower()}_vectors.json")
    
    def get_component_path(self, dsl_type: DSLType) -> str:
        """Get the path to the component file for a DSL type"""
        return os.path.join(self.vector_db_dir, f"{dsl_type.value.lower()}_components.json")
    
    def encode_text(self, text: str) -> np.ndarray:
        """Encode text to a vector using the sentence transformer model"""
        return self.model.encode(text, convert_to_numpy=True)
    
    def add_component(self, component: DSLComponent) -> None:
        """Add a DSL component to the vector database"""
        component_type = component.type
        
        # Initialize if not exists
        if component_type not in self.vectors:
            self.vectors[component_type] = {
                "texts": [],
                "embeddings": []
            }
            self.dsl_components[component_type] = []
        
        # Encode the text
        text = component.text
        embedding = self.encode_text(text)
        
        # Add to vectors
        self.vectors[component_type]["texts"].append(text)
        self.vectors[component_type]["embeddings"].append(embedding.tolist())
        
        # Add to components
        self.dsl_components[component_type].append(component)
        
        # Save to disk
        self.save_vectors(component_type)
    
    def add_components(self, components: List[DSLComponent]) -> None:
        """Add multiple DSL components to the vector database"""
        # Group components by type for batch processing
        grouped_components = {}
        for component in components:
            if component.type not in grouped_components:
                grouped_components[component.type] = []
            grouped_components[component.type].append(component)
        
        # Process each type
        for component_type, component_list in grouped_components.items():
            # Initialize if not exists
            if component_type not in self.vectors:
                self.vectors[component_type] = {
                    "texts": [],
                    "embeddings": []
                }
                self.dsl_components[component_type] = []
            
            # Encode texts in batch
            texts = [component.text for component in component_list]
            embeddings = self.model.encode(texts, convert_to_numpy=True)
            
            # Add to vectors
            self.vectors[component_type]["texts"].extend(texts)
            self.vectors[component_type]["embeddings"].extend(embeddings.tolist())
            
            # Add to components
            self.dsl_components[component_type].extend(component_list)
            
            # Save to disk
            self.save_vectors(component_type)
    
    def save_vectors(self, component_type: DSLType) -> None:
        """Save vectors to disk for a specific component type"""
        vector_path = self.get_vector_path(component_type)
        component_path = self.get_component_path(component_type)
        
        # Save vectors
        with open(vector_path, 'w') as f:
            json.dump(self.vectors[component_type], f)
        
        # Save components
        with open(component_path, 'w') as f:
            # Convert components to dictionaries
            component_dicts = [component.dict() for component in self.dsl_components[component_type]]
            json.dump(component_dicts, f)
    
    def load_vectors(self) -> None:
        """Load all vectors and components from disk"""
        for dsl_type in DSLType:
            vector_path = self.get_vector_path(dsl_type)
            component_path = self.get_component_path(dsl_type)
            
            # Load vectors if file exists
            if os.path.exists(vector_path) and os.path.exists(component_path):
                with open(vector_path, 'r') as f:
                    self.vectors[dsl_type] = json.load(f)
                
                # Load components
                with open(component_path, 'r') as f:
                    component_dicts = json.load(f)
                    self.dsl_components[dsl_type] = []
                    
                    for component_dict in component_dicts:
                        # Re-create component objects based on their type
                        component_type = component_dict.get('type')
                        if component_type == dsl_type.value:
                            from src.dsl.parser import create_dsl_component
                            component = create_dsl_component(component_dict)
                            self.dsl_components[dsl_type].append(component)
    
    def search(self, query: str, component_type: DSLType, top_k: int = 5) -> List[DSLComponent]:
        """Search for DSL components of a specific type that are semantically similar to the query"""
        if component_type not in self.vectors or not self.vectors[component_type]["embeddings"]:
            return []
        
        # Encode the query
        query_embedding = self.encode_text(query)
        
        # Get embeddings for the component type
        embeddings = np.array(self.vectors[component_type]["embeddings"])
        
        # Calculate cosine similarities
        similarities = cosine_similarity([query_embedding], embeddings)[0]
        
        # Get top-k indices
        top_k_indices = np.argsort(similarities)[-top_k:][::-1]
        
        # Return corresponding components
        results = []
        for idx in top_k_indices:
            if idx < len(self.dsl_components[component_type]):
                results.append(self.dsl_components[component_type][idx])
        
        return results
    
    def search_all_types(self, query: str, top_k: int = 5) -> Dict[DSLType, List[DSLComponent]]:
        """Search for DSL components of all types that are semantically similar to the query"""
        results = {}
        
        for component_type in DSLType:
            if component_type in self.vectors and self.vectors[component_type]["embeddings"]:
                type_results = self.search(query, component_type, top_k)
                if type_results:
                    results[component_type] = type_results
        
        return results
    
    def clear(self, component_type: Optional[DSLType] = None) -> None:
        """Clear vectors and components for a specific type or all types"""
        if component_type:
            if component_type in self.vectors:
                del self.vectors[component_type]
            if component_type in self.dsl_components:
                del self.dsl_components[component_type]
                
            # Remove files
            vector_path = self.get_vector_path(component_type)
            component_path = self.get_component_path(component_type)
            
            if os.path.exists(vector_path):
                os.remove(vector_path)
            if os.path.exists(component_path):
                os.remove(component_path)
        else:
            # Clear all
            self.vectors = {}
            self.dsl_components = {}
            
            # Remove all files
            for f in os.listdir(self.vector_db_dir):
                if f.endswith("_vectors.json") or f.endswith("_components.json"):
                    os.remove(os.path.join(self.vector_db_dir, f)) 