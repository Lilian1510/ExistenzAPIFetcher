import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='existenz_api_fetcher',
    version='0.0.9',
    author='Lilian Lonla',
    author_email='lilianlonla15@gmail.com',
    description='Easily fetch swiss weather and hydrological data from the Existenz API',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/Lilian1510/ExistenzAPIFetcher/',
    packages=setuptools.find_packages(),
    install_requires=[
        'influxdb_client>=1.32.0',
        'pandas>=1.4.3',
        'pyet>=1.1.0',
        'setuptools==67.7.2'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
