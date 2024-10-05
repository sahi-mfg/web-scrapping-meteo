import pytest
from src.loader.load_data import load_data
from utils.config import get_warehouse_creds
from utils.db import WarehouseConnection


def _truncate_data():
    with WarehouseConnection(get_warehouse_creds()).managed_cursor() as curr:
        curr.execute("Truncate table housing.user;")


@pytest.fixture()
def set_up_tear_down():
    # Clean up existing data
    _truncate_data()
    yield
    _truncate_data()


class TestLoadData:
    @pytest.mark.integration
    def test_load_data(self, set_up_tear_down):
        load_data()
        with WarehouseConnection(get_warehouse_creds()).managed_cursor() as curr:
            curr.execute("SELECT COUNT(*) FROM housing.user;")
            count = curr.fetchone()[0]
            assert count == 3
            curr.execute("SELECT * FROM housing.user;")
            rows = curr.fetchall()
            assert rows[0] == (1, "John", 25)
            assert rows[1] == (2, "Jane", 24)
            assert rows[2] == (3, "Doe", 26)
