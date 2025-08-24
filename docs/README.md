# Yad2 Car Analyzer

A comprehensive tool for scraping and analyzing car listings from Yad2 (Israeli car marketplace).

## Project Structure

```
yad2_car_analyzer/
├── src/                          # Source code
│   ├── __init__.py
│   ├── core/                     # Core application logic
│   │   ├── __init__.py
│   │   ├── scraper/             # Web scraping functionality
│   │   │   ├── __init__.py
│   │   │   ├── vehicle_scraper.py
│   │   │   └── scrapper_entry_point.py
│   │   ├── database/            # Database operations
│   │   │   ├── __init__.py
│   │   │   ├── database.py
│   │   │   └── ddl.sql
│   │   ├── etl/                 # Data transformation
│   │   │   ├── __init__.py
│   │   │   └── etl.py
│   │   └── config/              # Configuration management
│   │       ├── __init__.py
│   │       ├── environment_variables_loader.py
│   │       └── manufacturers.yml
│   └── utils/                    # Utility functions
│       ├── __init__.py
│       └── barrys_supabase.py
├── tests/                        # Test files
│   ├── __init__.py
│   ├── test_scraping.py
│   ├── test_sqlite_connection.py
│   └── test_supabase_connection.py
├── data/                         # Data files
│   └── cars.db                  # SQLite database
├── docs/                         # Documentation
│   └── README.md
├── scripts/                      # Utility scripts
├── .vscode/                      # VS Code configuration
│   └── launch.json
├── .cursor/                      # Cursor rules
├── requirements.txt              # Python dependencies
├── .gitignore                   # Git ignore file
├── main.py                      # Main entry point
└── README.md                     # Main project documentation
```

## Features

- **Web Scraping**: Automated extraction of car listings from Yad2
- **Multi-Database Support**: SQLite (local) and Supabase (PostgreSQL)
- **Data Enrichment**: Calculated fields like mechanical age and age ratios
- **Configuration Management**: YAML-based manufacturer and model configuration
- **Comprehensive Testing**: Unit and integration tests for all components

## Quick Start

### Prerequisites

1. Python 3.8+
2. Required packages: `pip install -r requirements.txt`

### Running the Scraper

#### From Root Directory (Recommended)
```bash
python main.py --manufacturer subaru --model impreza --listings 5
```

#### From Source Directory
```bash
python src/core/scraper/scrapper_entry_point.py --manufacturer subaru --model impreza --listings 5
```

### Database Options

- **SQLite** (default): `--database sqlite`
- **Supabase**: `--database supabase`

## Configuration

### Environment Variables

Create a `.env` file with your Supabase credentials:
```env
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
SUPABASE_USER=your_username
SUPABASE_PASSWORD=your_password
SUPABASE_HOST=your_host
SUPABASE_PORT=5432
SUPABASE_DBNAME=your_database_name
```

### Manufacturer Configuration

Edit `src/core/config/manufacturers.yml` to add new manufacturers and models.

## Development

### Running Tests

```bash
# Run all tests
python -m pytest tests/

# Run specific test
python tests/test_sqlite_connection.py
```

### Project Structure Benefits

1. **Separation of Concerns**: Clear separation between scraping, database, and ETL logic
2. **Modularity**: Each component is self-contained with proper package structure
3. **Scalability**: Easy to add new features and modules
4. **Testing**: Dedicated test suite with proper isolation
5. **Configuration**: Centralized configuration management
6. **Standards Compliance**: Follows Python packaging best practices

## Database Schema

The application creates and manages the following tables:

- **manufacturers**: Car manufacturer information
- **car_listings**: Individual car listings with comprehensive data
- **scraping_logs**: Log of scraping sessions
- **raw_data**: Raw scraped data before processing

## Contributing

1. Follow the established project structure
2. Add tests for new functionality
3. Update documentation as needed
4. Follow PEP 8 coding standards

## License

This project is for educational and research purposes. 