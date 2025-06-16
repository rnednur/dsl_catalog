from setuptools import setup, find_packages

setup(
    name="nl2sql-dsl",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "psycopg2-binary>=2.9.0",
        "SQLAlchemy>=2.0.0",
        "pandas>=2.0.0",
        "numpy>=1.21.0",
        "scikit-learn>=1.2.0",
        "sentence-transformers>=2.2.0,<2.3.0",
        "transformers>=4.21.0,<4.35.0",
        "huggingface_hub>=0.10.0,<0.17.0",
        "spacy>=3.4.0",
        "python-dotenv>=1.0.0",
        "pytest>=7.0.0",
        "pgvector>=0.4.0",
        "fastapi>=0.95.0",
        "uvicorn>=0.22.0",
        "pydantic>=1.10.0",
        "langchain>=0.0.200",
    ],
    entry_points={
        "console_scripts": [
            "nl2sql=src.main:main",
            "nl2sql-api=src.api:start",
            "nl2sql-setup=scripts.setup:main",
        ],
    },
) 