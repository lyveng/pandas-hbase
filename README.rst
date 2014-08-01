======================
Pandas HBase IO Helper
======================

Persist pandas DataFrame objects to HBase and read them back later.

Known Issues:
- Works only with DataFrames that have integer indices.
- DataFrames to be persisted should not have ':' in column names

Writing DataFrame to HBase
--------------------------
.. code-block:: python
    import happybase
    import numpy as np
    import pandas as pd
    import pandas-hbase as pdh
    connection = None
    try:
        connection = happybase.Connection('127.0.0.1')
        connection.open()
        df = pd.DataFrame(np.random.randn(10, 5), columns=['a', 'b', 'c', 'd', 'e'])
        df['f'] = 'hello world'
        pdh.to_hbase(df, connection, 'sample_table', 'df_key', cf='cf')
    finally:
        if connection:
            connection.close()

Reading DataFrame from HBase
----------------------------
.. code-block:: python
    import happybase
    import numpy as np
    import pandas as pd
    import pandas-hbase as pdh
    connection = None
    try:
        connection = happybase.Connection('127.0.0.1')
        connection.open()
        df = read_hbase(connection, 'sample_table', 'df_key', cf='cf')
        print df
    finally:
        if connection:
            connection.close()