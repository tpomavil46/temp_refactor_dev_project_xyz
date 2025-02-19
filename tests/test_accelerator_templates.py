# tests/test_accelerator_templates.py
from fastapi.testclient import TestClient
from itv_asset_tree.api.api import app

client = TestClient(app)

def test_get_templates():
    response = client.get("/api/templates")
    assert response.status_code == 200
    assert "templates" in response.json()