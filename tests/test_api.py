"""
Basic API tests using FastAPI's TestClient.
Run with: pytest tests/test_api.py
Requires a running PostgreSQL instance (or can be mocked).
"""

import os
import sys
import unittest
from unittest import mock

# Stub out DB so tests don't need a live PostgreSQL
os.environ.setdefault("POSTGRES_HOST",     "localhost")
os.environ.setdefault("POSTGRES_USER",     "gridpulse")
os.environ.setdefault("POSTGRES_PASSWORD", "gridpulse_secret")
os.environ.setdefault("POSTGRES_DB",       "gridpulse")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'api'))

# Patch sqlalchemy create_engine before importing main
with mock.patch("sqlalchemy.create_engine") as mock_engine:
    mock_conn = mock.MagicMock()
    mock_engine.return_value.connect.return_value.__enter__ = lambda s: mock_conn
    mock_engine.return_value.connect.return_value.__exit__  = mock.MagicMock(return_value=False)
    mock_conn.execute.return_value.keys.return_value = []
    mock_conn.execute.return_value.fetchall.return_value = []
    import main as api_main

from fastapi.testclient import TestClient

client = TestClient(api_main.app)


class TestHealthEndpoint(unittest.TestCase):
    def test_health_route_exists(self):
        # We just check the route is registered; DB may be down in unit-test env
        routes = [r.path for r in api_main.app.routes]
        self.assertIn("/health", routes)


class TestRouteRegistration(unittest.TestCase):
    """Verify all required endpoints are registered."""

    def _paths(self):
        return [r.path for r in api_main.app.routes]

    def test_grid_load(self):
        self.assertIn("/grid-load", self._paths())

    def test_grid_load_history(self):
        self.assertIn("/grid-load/history/{zone}", self._paths())

    def test_billing(self):
        self.assertIn("/billing/{household_id}", self._paths())

    def test_billing_all(self):
        self.assertIn("/billing/{household_id}/all", self._paths())

    def test_alerts(self):
        self.assertIn("/alerts", self._paths())

    def test_metrics(self):
        self.assertIn("/metrics", self._paths())


class TestMetricsEndpoint(unittest.TestCase):
    def test_metrics_returns_text(self):
        resp = client.get("/metrics")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("text/plain", resp.headers["content-type"])


if __name__ == "__main__":
    unittest.main()
