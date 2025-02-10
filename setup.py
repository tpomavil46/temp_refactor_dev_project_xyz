from setuptools import setup, find_packages
import versioneer
import os

def read_requirements():
    """Read and parse requirements.txt"""
    req_file = os.path.join(os.path.dirname(__file__), "requirements.txt")
    with open(req_file, encoding="utf-8") as f:
        return f.read().splitlines()

setup(
    name="itv_asset_tree",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Seeq asset tree package with backend and frontend integration.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Tim Pomaville",
    author_email="tim.pomaville@itvizion.com",
    url="https://github.com/tpomavil46/Seeq-Asset-Trees",
    packages=find_packages(where="src"),  # Ensure packages are found
    package_dir={"": "src"},  # Root is src/
    include_package_data=True,  # Ensures non-Python files are included
    install_requires=read_requirements(),  # Read requirements dynamically
    python_requires=">=3.11",
    classifiers=[
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            "seeq-tree=itv_asset_tree.main:main",
        ],
    },
    setup_requires=["versioneer"],
    package_data={
        "itv_asset_tree": [
            "frontend/*",
            "frontend/**/*",
            "managers/*",
            "utilities/*",
        ],
    },
)