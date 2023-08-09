import dask.dataframe as dd
import pandas as pd
import random

import huggingface_hub

data = {"number": [random.randint(0,10) for _ in range(1000)]}
df = pd.DataFrame.from_dict(data)
dataframe = dd.from_pandas(df, npartitions=1)
# dataframe = dataframe.repartition(npartitions=2)

repo_id = "nielsr/test-dask"
repo_path = f"hf://datasets/{repo_id}"
huggingface_hub.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True)
dd.to_parquet(dataframe, path=f"{repo_path}/data")