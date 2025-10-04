if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd


@data_loader
def load_data(*args, **kwargs):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2015-01.parquet"
    data = pd.read_parquet(url)
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'