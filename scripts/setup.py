import os
import argparse
import subprocess
import sys
from pathlib import Path

def setup_environment():
    """Set up the environment for the NL2SQL system"""
    print("Setting up environment for NL2SQL...")

    # Create necessary directories
    dirs = ["data", "data/schema", "data/vector_db", "data/dsl_components"]
    for d in dirs:
        os.makedirs(d, exist_ok=True)
        print(f"Created directory: {d}")
    
    # Download required models
    print("Downloading required models...")
    try:
        import spacy
        try:
            spacy.load("en_core_web_md")
            print("SpaCy model already downloaded")
        except:
            print("Downloading SpaCy model...")
            subprocess.run(["python", "-m", "spacy", "download", "en_core_web_md"], check=True)
            print("SpaCy model downloaded")
    except ImportError:
        print("SpaCy not installed. Please install requirements first.")
    
    # Create .env file if it doesn't exist
    if not os.path.exists(".env"):
        print("Creating .env file...")
        if os.path.exists(".env.example"):
            with open(".env.example", "r") as f_example, open(".env", "w") as f_env:
                f_env.write(f_example.read())
            print(".env file created from .env.example")
        else:
            print("Warning: .env.example not found. Creating empty .env file.")
            with open(".env", "w") as f:
                f.write("# NL2SQL Environment Variables\n")
                f.write("DB_HOST=localhost\n")
                f.write("DB_PORT=5432\n")
                f.write("DB_NAME=nl2sql\n")
                f.write("DB_USER=postgres\n")
                f.write("DB_PASSWORD=postgres\n")
                f.write("VECTOR_DB_DIMENSION=768\n")
                f.write("MODEL_NAME=sentence-transformers/all-MiniLM-L6-v2\n")
            print(".env file created with default values")
    else:
        print(".env file already exists")
    
    print("Environment setup complete!")


def init_database():
    """Initialize the database and extract schema"""
    print("Initializing database...")
    
    try:
        from src.db.schema_loader import main as schema_loader_main
        schema_loader_main()
        print("Database schema extracted successfully")
    except Exception as e:
        print(f"Error initializing database: {e}")
        return False
    
    return True


def init_vector_db():
    """Initialize the vector database with DSL components"""
    print("Initializing vector database...")
    
    try:
        from src.vector_db.vector_loader import main as vector_loader_main
        vector_loader_main()
        print("Vector database initialized successfully")
    except Exception as e:
        print(f"Error initializing vector database: {e}")
        return False
    
    return True


def main():
    """Main setup function"""
    parser = argparse.ArgumentParser(description="Setup NL2SQL system")
    parser.add_argument("--env-only", action="store_true", help="Only set up environment")
    parser.add_argument("--db-only", action="store_true", help="Only initialize database")
    parser.add_argument("--vector-only", action="store_true", help="Only initialize vector database")
    
    args = parser.parse_args()
    
    if args.env_only:
        setup_environment()
    elif args.db_only:
        init_database()
    elif args.vector_only:
        init_vector_db()
    else:
        setup_environment()
        db_success = init_database()
        if db_success:
            vector_success = init_vector_db()
            if vector_success:
                print("\nNL2SQL setup completed successfully!")
                print("\nTo use the command-line interface:")
                print("  python src/main.py -i")
                print("\nTo start the API server:")
                print("  python src/api.py")
            else:
                print("\nNL2SQL setup completed with warnings!")
                print("Vector database initialization failed.")
        else:
            print("\nNL2SQL setup completed with warnings!")
            print("Database initialization failed. Vector database initialization skipped.")


if __name__ == "__main__":
    main() 