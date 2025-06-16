from setuptools import setup, find_packages

setup(
    name="nl2sql-dsl",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "psycopg2-binary==2.9.6",
        "SQLAlchemy==2.0.13",
        "pandas==2.0.1",
        "numpy==1.24.3",
        "scikit-learn==1.2.2",
        "sentence-transformers==2.2.2",
        "transformers==4.28.1",
        "spacy==3.5.3",
        "python-dotenv==1.0.0",
        "pytest==7.3.1",
        "pgvector==0.1.11",
        "fastapi==0.95.2",
        "uvicorn==0.22.0",
        "pydantic==1.10.8",
        "langchain==0.0.283",
    ],
    entry_points={
        "console_scripts": [
            "nl2sql=src.main:main",
            "nl2sql-api=src.api:start",
            "nl2sql-setup=scripts.setup:main",
        ],
    },
) 