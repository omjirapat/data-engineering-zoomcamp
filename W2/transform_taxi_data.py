if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform(data, *args, **kwargs):
    data['lpep_pickup_date']=data['lpep_pickup_datetime'].dt.date
    data.columns = (data.columns
                 .str.replace('ID','_id')
                .str.lower()
        )
    return data[(data['passenger_count']>0) & (data['trip_distance']>0) ]

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'

@test
def test_output(output, *args):
    assert output["vendor_id"].isin([1,2]).all()==True, 'vendor_id is one of the existing values in the column (currently)'
    assert output["passenger_count"].isin([0]).sum()==0, 'passenger_count is greater than 0'
    assert output["trip_distance"].isin([0]).sum()==0, 'trip_distance is greater than 0'