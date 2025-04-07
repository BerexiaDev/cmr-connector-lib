from setuptools import setup, find_packages

setup(
    name='cmr_connectors_lib',
    version='0.8.7',
    packages=find_packages(),
    # Add dependencies here
    install_requires=[
        "pyodbc",
        "psycopg2",
        "sqlalchemy",
        "cx_oracle",
        "loguru",
        "ibm-db",
        "ibm_db_sa"
    ],
    description='CMR Connectors Library',
    author='Soufiane',
    author_email='soufiane.amghar@berexia.com',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
