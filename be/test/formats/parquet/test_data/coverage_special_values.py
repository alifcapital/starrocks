# Rebuild coverage_special_values.parquet with PyArrow 25.0.1.
# Keep NaN alongside finite footer bounds, and NULL in a different row group.
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

values = [float("nan"), -float("inf"), -1.0, -0.0, 0.0, 1.0, float("inf"), None]
pq.write_table(pa.table({"v": pa.array(values, pa.float64())}),
               Path(__file__).with_suffix(".parquet"), row_group_size=4,
               use_dictionary=False, write_page_index=True,
               data_page_size=64, write_batch_size=1)
