# itv_asset_tree
ITV Asset Tree  **ITV Asset Tree** is a Python package designed to streamline the creation and management of asset trees in Seeq. 

## Installation Instructions for `itv_asset_tree`

## Prerequisites
- **Python**: Version 3.11 or higher
- **pip**: Latest version installed
- **git**: Installed and configured

## 1. Clone the Repository
```bash
git clone https://github.com/tpomavil46/itv_asset_tree.git
cd itv_asset_tree
```

## 2. Create a Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\\Scripts\\activate`
```

## 3. Install Dependencies
```bash
pip install -r requirements.txt
```

## 4. Install Dependencies
```bash
pip install .
```

## 5. Set Up Environment Variables
Create a `.env` file in the root directory with the following content:
Below is an example
```ini
SERVER_USERNAME=your_email@example.com
SERVER_PASSWORD=my_passwd
SERVER_HOST=https://your_server_name.seeq.tech
LOG_LEVEL=debug
```
Update these values according to your server configuration.

## 6. Verify Installation
```bash
pytest tests/ # Run tests to ensure everything is working
```

## 7. Usage
```bash
seeq-asset-tree
```

## Additional Notes
- **Local Development:** Use `pip install -e .` for editable installs.
- **Versioning:** Managed by `versioneer`
- **Testing:** Uses `pytest` locally and `pytest-mock` for CI
- **FastAPI:** Find web api at http://127.0.0.1:8000/docs

---
You’re all set! 🎉 Start building with `itv_asset_tree`.

