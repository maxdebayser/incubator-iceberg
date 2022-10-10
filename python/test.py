from pyiceberg.catalog import load_catalog

cat = load_catalog('resttb')

tbl = cat.load_table(('nyc', 'taxis'))

scan = tbl.new_scan()

ds = scan.dataset

print(ds.to_table())

