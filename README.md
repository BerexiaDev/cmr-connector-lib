## CMR Connectors Library
This library returns connectors to use in your projects from different databases.


## Push to pypi
```bash
pip install wheel
python setup.py sdist bdist_wheel
pip install twine
twine upload dist/*
```

## Requirements

This package requires the following dependencies: "pymssql","pyodbc","psycopg2","sqlalchemy","cx_oracle"