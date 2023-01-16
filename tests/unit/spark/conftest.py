import pytest
from tests.unit.spark.moto_server import MotoServer


@pytest.fixture(scope="session", autouse=True)
def moto_server(request):
    """
    Start the moto server before running all the tests and stop it afterwards.
    """
    def stop_moto_server():
        MotoServer.stop()

    request.addfinalizer(stop_moto_server)
    MotoServer.start()