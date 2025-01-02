from setuptools import setup, find_packages

setup(
    name="seeq_asset_tree_package",  # Your package name
    version="0.1.0",  # Initial version
    description="A package for managing Seeq asset trees.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Tim Pomaville",
    author_email="tim.pomaville@itvizion.com",
    url="https://github.com/tpomavil46/Seeq-Asset-Trees/tree/main",  # GitHub repo URL
    packages=find_packages(where="src"),  # Look for packages in 'src/'
    package_dir={"": "src"},  # Root is 'src'
    include_package_data=True,  # Include non-code files specified in MANIFEST.in
    install_requires=open("requirements.txt").read().splitlines(),  # Dependencies
    python_requires=">=3.11",
    classifiers=[
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            "seeq-tree=src.main:main",  # Optional command-line script
        ],
    },
)