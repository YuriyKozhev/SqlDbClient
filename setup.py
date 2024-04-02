import setuptools

with open("README.rst", "r", encoding="utf-8") as f:
    long_description = f.read()
    long_description = long_description.replace(':mod:', '').replace(':command:', '')

dependencies = (
    'pandas',
    'sqlalchemy',
    'sqlparse',
)

setuptools.setup(
    name="sqldbclient",
    version="0.1.1",
    author="Yuriy Kozhev",
    author_email="yuriy.kozhev@gmail.com",
    description="A SQL client software package, mainly for use in Jupyter Notebook environment",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    url="https://github.com/YuriyKozhev/SqlDBClient",
    project_urls={
        "Homepage": "https://github.com/YuriyKozhev/SqlDBClient",
        "Documentation": "https://sqldbclient.readthedocs.io/",
        "Release Notes": "https://sqldbclient.readthedocs.io/en/latest/changes.html",
        "Source": "https://github.com/YuriyKozhev/sqldbclient",
        "Tracker": "https://github.com/YuriyKozhev/sqldbclient/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=dependencies,
    extras_require={
        'jupyter': ('jupyter', 'notebook', 'ipykernel'),
    },
    license='MIT',
    license_files=('LICENSE',),
    platforms=['any'],
    readme=long_description,
)
