




def test_run():
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog('local')

    tbl = catalog.load_table('nyc.taxis')

    from pyiceberg.expressions import GreaterThanOrEqual, LessThan, And

    sc = tbl.scan(row_filter=And(
        GreaterThanOrEqual("tpep_pickup_datetime", "2022-04-01T00:00:00.000000+00:00"),
        LessThan("tpep_pickup_datetime", "2022-05-01T00:00:00.000000+00:00"),
    )).to_arrow()