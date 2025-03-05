## CMR Connectors Library



## Push to pypi
```bash
pip install wheel
python setup.py sdist bdist_wheel
pip install twine
twine upload dist/*
```