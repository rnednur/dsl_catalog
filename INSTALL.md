# Installation Guide

This guide will help you set up the NL2SQL DSL project on your system.

## Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/nl2sql-dsl.git
cd nl2sql-dsl
```

### 2. Create Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
Choose one of the following methods:

**Method A: Development Installation (Recommended)**
```bash
pip install -e .
```

**Method B: Direct Requirements (Flexible)**
```bash
pip install -r requirements.txt
```

**Method C: Stable Requirements (Tested Versions)**
```bash
pip install -r requirements-stable.txt
```

### 4. Verify Installation
```bash
python3 scripts/demo.py --help
```

## Detailed Setup

### Prerequisites
- Python 3.8 or higher
- PostgreSQL 13+ with pgvector extension (for production use)
- Git

### Step-by-Step Installation

1. **Check Python Version**
   ```bash
   python3 --version
   # Should be 3.8 or higher
   ```

2. **Clone and Navigate**
   ```bash
   git clone https://github.com/yourusername/nl2sql-dsl.git
   cd nl2sql-dsl
   ```

3. **Create Virtual Environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   
   # Verify virtual environment
   which python3
   ```

4. **Upgrade pip**
   ```bash
   pip install --upgrade pip
   ```

5. **Install Dependencies**
   ```bash
   pip install -e .
   ```

6. **Download SpaCy Model** (Required)
   ```bash
   python3 -m spacy download en_core_web_sm
   ```

7. **Set Up Environment Variables**
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

### Environment Configuration

Edit `.env` file with your settings:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password

# Vector Database Configuration
VECTOR_DB_PATH=./vector_store
EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
DEBUG=False
```

## Testing Installation

### 1. Test Demo Script
```bash
python3 scripts/demo.py --help
```

### 2. Run Tests
```bash
python3 -m pytest tests/
```

### 3. Test Interactive Demo
```bash
python3 scripts/demo.py -i
```

## Common Issues

### Issue: `ModuleNotFoundError: No module named 'spacy'`
**Solution:**
```bash
pip install spacy
python3 -m spacy download en_core_web_sm
```

### Issue: `ImportError: cannot import name 'cached_download'`
**Solution:** This is a compatibility issue between sentence-transformers and huggingface_hub:
```bash
pip uninstall -y sentence-transformers huggingface_hub transformers
pip install sentence-transformers==2.2.2 transformers==4.28.1 huggingface_hub==0.16.4
```

Or use the stable requirements:
```bash
pip install -r requirements-stable.txt
```

### Issue: `ERROR: Could not find a version that satisfies the requirement pgvector==0.1.11`
**Solution:** This is already fixed in the current version. If you encounter this:
```bash
pip install pgvector>=0.4.0
```

### Issue: Database Connection Errors
**Solution:**
1. Ensure PostgreSQL is running
2. Install pgvector extension: `CREATE EXTENSION vector;`
3. Check your `.env` file settings

## Development Setup

For development work:

1. **Install Development Dependencies**
   ```bash
   pip install -r requirements-dev.txt
   ```

2. **Set Up Pre-commit Hooks** (Optional)
   ```bash
   pre-commit install
   ```

3. **Run Code Formatting**
   ```bash
   black src/ tests/ scripts/
   isort src/ tests/ scripts/
   ```

## Production Deployment

For production deployment:

1. **Set Production Environment Variables**
2. **Install Production Dependencies Only**
   ```bash
   pip install -r requirements.txt --no-dev
   ```
3. **Set up PostgreSQL with pgvector**
4. **Run Database Migrations**
   ```bash
   python3 scripts/setup.py
   ```

## Getting Help

If you encounter issues:

1. Check this installation guide
2. Look at the main README.md
3. Check the GitHub Issues page
4. Ensure all prerequisites are met

## Verification Checklist

- [ ] Python 3.8+ installed
- [ ] Virtual environment created and activated
- [ ] Dependencies installed successfully
- [ ] SpaCy model downloaded
- [ ] Environment variables configured
- [ ] Demo script runs without errors
- [ ] Tests pass 