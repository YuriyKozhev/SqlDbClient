import setuptools

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

dependencies = (
    'pandas',
    'sqlalchemy',
    'sqlparsse',
)

setuptools.setup(
    name="sqldbclient",
    version="0.0.2",
    author="Yuriy Kozhev",
    author_email="yuriy.kozhev@gmail.com",
    description="A SQL client software package, mainly for use in Jupyter Notebook environment",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://example.com",
    project_urls={
        "Homepage": "https://example.org",
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
)
