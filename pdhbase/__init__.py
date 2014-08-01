import happybase
import logging
import numpy as np
import pandas as pd
import struct
import sys

__author__ = 'emmanuel'

logger = logging.getLogger(__name__)


def read_hbase(con, table_name, key, cf='hb'):
    """Read a pandas DataFrame object from HBase table.

    :param con: HBase connection object
    :type con: happybase.Connection
    :param table_name: HBase table name to which the DataFrame should be read from
    :type table_name: str
    :param key: row key from which the DataFrame should be read
    :type key: str
    :param cf: Column Family name
    :type cf: str
    :return: Pandas DataFrame object read from HBase
    :rtype: pd.DataFrame
    """
    table = con.table(table_name)

    column_dtype_key = key + 'columns'
    column_dtype = table.row(column_dtype_key, columns=[cf])
    columns = {col.split(':')[1] : value for col, value in column_dtype.items()}

    column_order_key = key + 'column_order'
    column_order_dict = table.row(column_order_key, columns=[cf])
    column_order = list()
    for i in xrange(len(column_order_dict)):
        column_order.append(column_order_dict[':'.join((cf, struct.pack('>q', i)))])

    row_start = key + 'rows' + struct.pack('>q', 0)
    row_end = key + 'rows' + struct.pack('>q', sys.maxint)
    rows = table.scan(row_start=row_start, row_stop=row_end)
    df = pd.DataFrame(columns=columns)
    for row in rows:
        df_row = {key.split(':')[1]: value for key, value in row[1].items()}
        df = df.append(df_row, ignore_index=True)
        print
    for column, data_type in columns.items():
        df[column] = df[column].astype(np.dtype(data_type))
    return df


def to_hbase(df, con, table_name, key, cf='hb'):
    """Write a pandas DataFrame object to HBase table.

    :param df: pandas DataFrame object that has to be persisted
    :type df: pd.DataFrame
    :param con: HBase connection object
    :type con: happybase.Connection
    :param table_name: HBase table name to which the DataFrame should be written
    :type table_name: str
    :param key: row key to which the dataframe should be written
    :type key: str
    :param cf: Column Family name
    :type cf: str
    """
    table = con.table(table_name)

    column_dtype_key = key + 'columns'
    column_dtype_value = dict()
    for column in df.columns:
        column_dtype_value[':'.join((cf, column))] = df.dtypes[column].name

    column_order_key = key + 'column_order'
    column_order_value = dict()
    for i, column_name in enumerate(df.columns.tolist()):
        order_key = struct.pack('>q', i)
        column_order_value[':'.join((cf, order_key))] = column_name

    row_key_template = key + 'rows'
    with table.batch(transaction=True) as b:
        b.put(column_dtype_key, column_dtype_value)
        b.put(column_order_key, column_order_value)
        for row in df.iterrows():
            row_key = row_key_template + struct.pack('>q', row[0])
            row_value = dict()
            for column, value in row[1].iteritems():
                if not pd.isnull(value):
                    row_value[':'.join((cf, column))] = str(value)
            b.put(row_key, row_value)
