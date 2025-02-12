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
    packages=find_packages(where="src", include=["itv_asset_tree", "itv_asset_tree.*"]), # Finds all packages inside src/
    package_dir={"": "src"},  # Maps root to src/
    include_package_data=True,  # Ensures non-Python files are included
    install_requires = [
        "fastapi",
        "uvicorn",
        "pandas",
        "seeq-spy",
        "python-dotenv"
    ],  # Read dependencies dynamically
    python_requires=">=3.11",
    classifiers=[
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            "seeq-asset-tree=itv_asset_tree.__main__:main",
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