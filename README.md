# itv_asset_tree
ITV Asset Tree  **ITV Asset Tree** is a Python package designed to streamline the creation and management of asset trees in Seeq. 

## Installation Instructions for `itv_asset_tree`

## Prerequisites
- **Python**: Version 3.10 or higher
- **pip**: Latest version installed
- **git**: Installed and configured

## 1. Clone the Repository
```bash
git clone https://github.com/yourusername/itv_asset_tree.git
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

## 4. Set Up Environment Variables
Create a `.env` file in the root directory with the following content:
```ini
SERVER_USERNAME=
SERVER_PASSWORD=
SERVER_HOST=
LOG_LEVEL=debug
```
Update these values according to your server configuration.

## 5. Verify Installation
```bash
pytest  # Run tests to ensure everything is working
```

## 6. Usage
```bash
python src/main.py
```

## Additional Notes
- **Local Development:** Use `pip install -e .` for editable installs.
- **Versioning:** Managed by `versioneer`
- **Testing:** Uses `pytest` with `pytest-mock`

---
You’re all set! 🎉 Start building with `itv_asset_tree`.

