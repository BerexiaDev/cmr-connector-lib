from setuptools import setup, find_packages

setup(
    name='cmr_connectors_lib',
    version='0.1',
    packages=find_packages(),
    # Add dependencies here
    install_requires=[
       
    ],
    description='CMR Connectors Library',
    author='Berexia',
    author_email='berexia.dev@gmail.com',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
