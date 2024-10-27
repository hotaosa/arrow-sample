import numpy as np
import os
import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc

t = np.arange(59.0, 86400.0, 60.0)
df = pd.DataFrame()
df["time"] = t
df["current"] = 70.0
for i in range(24):
    df[f"voltage{str(i + 1).zfill(2)}"] = 3.0
print(df.head())

table = pa.Table.from_pandas(df)
dir = os.path.join(os.path.dirname(__file__), "..", "ipc")
os.makedirs(dir, exist_ok=True)
with pa.OSFile("../ipc/sample.arrow", "wb") as sink:
    with ipc.RecordBatchFileWriter(sink, table.schema) as writer:
        writer.write_table(table)
