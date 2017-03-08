import argparse
import json
import pandas as pd
import struct
import array
import base64

from google.cloud import bigtable
from google.cloud.bigtable.row_filters import ColumnQualifierRegexFilter
from google.cloud.bigtable.row_filters import RowFilterChain
from google.cloud.bigtable.row_filters import ValueRangeFilter
from google.cloud.bigtable.row_filters import ValueRegexFilter
from google.cloud.bigtable.row_filters import RowKeyRegexFilter

from pandas.io.json import json_normalize
from matplotlib import pyplot

def main(project_id, instance_id, table_id):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)
    column_family_id = 'ST'

    a = []

    col1_filter = RowKeyRegexFilter(b'6852#88DC961302E8#201703')
    col2_filter = ValueRegexFilter('11820581')
    # col3_filter = ValueRangeFilter("0".encode('utf-8'), "1488784881000".encode('utf-8'))

    chain1 = RowFilterChain(filters=[col1_filter, col2_filter])

    partial_rows = table.read_rows(filter_=col1_filter)
    partial_rows.consume_all()

    for row_key, row in partial_rows.rows.items():
        key = row_key.decode('utf-8')
        cell = row.cells['UNQS_M'][0]
        print(cell)

        # cell = cell[cell.keys()[0]][0]
        # value = cell.value.decode('utf-8')
        # val = { "Date": cell.timestamp.strftime("%a, %d %b %Y %H:%M:%S"), "Value": float(value) }
        # a.append(val)

    return

    print('Scanning tables:')

    col1_filter = ColumnQualifierRegexFilter(b'TX_BYTES:([a-zA-Z0-9]{12})')
    col2_filter = ValueRegexFilter('11820581')
    col3_filter = ValueRangeFilter("0".encode('utf-8'), "1488784881000".encode('utf-8'))

    chain1 = RowFilterChain(filters=[col1_filter, col2_filter, col3_filter])

    partial_rows = table.read_rows(filter_=chain1)
    partial_rows.consume_all()

    a = []

    for row_key, row in partial_rows.rows.items():
        key = row_key.decode('utf-8')
        cell = row.cells[column_family_id]
        cell = cell[cell.keys()[0]][0]
        value = cell.value.decode('utf-8')
        val = { "Date": cell.timestamp.strftime("%a, %d %b %Y %H:%M:%S"), "Value": float(value) }
        a.append(val)


    # print(a)

    column_family_id = 'CAPS'

    partial_rows = table.read_rows()
    partial_rows.consume_all()

    for row_key, row in partial_rows.rows.items():
        key = row_key.decode('utf-8')

        cell = row.cells[column_family_id]['CAPS_2'][0]

        a = struct.unpack(">ll", cell.value)[1]

        print(a)

        # cell = cell[cell.keys()[0]][0]
        # value = cell.value.decode('utf-8')
        # val = { "Date": cell.timestamp.strftime("%a, %d %b %Y %H:%M:%S"), "Value": float(value) }
        # a.append(val)

    print(a)

    return

    incomplete_data = json_to_dataframe(a)

    # full_range = pd.date_range(incomplete_data['Date'].min(), incomplete_data['Date'].max())
    incomplete_data['Date'] = pd.to_datetime(incomplete_data['Date'])
    incomplete_data.set_index(['Date'], inplace=True)

    # problem_data = incomplete_data.sort_index().reindex(full_range)
    # print(incomplete_data.head(100))
    # print(problem_data.head(100))

    axis = incomplete_data['Value']#.plot(kind='bar')
    upsampled = axis.resample('5T').mean()
    interpolated = upsampled.interpolate(method='time')
    # print(interpolated.head(100))

    interpolated.plot(kind="line")
    # axis.set_ylim(18,22)

    # a = interpolated.reset_index().to_json(orient='records')
    # a = interpolated.to_json(orient='records')

    # print(a)

    # pyplot.show()

def json_to_dataframe(results_list):
    results = {"results": results_list}
    df = json_normalize(results['results'])
    return df

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('project_id', help='Your Cloud Platform project ID.')
    parser.add_argument(
        'instance_id', help='ID of the Cloud Bigtable instance to connect to.')
    parser.add_argument(
        '--table',
        help='Table to create and destroy.',
        default='Hello-Bigtable')

    args = parser.parse_args()
    main(args.project_id, args.instance_id, args.table)
