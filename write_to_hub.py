import datasets
from datasets.features.features import generate_from_arrow_type
import dask.dataframe as dd

import huggingface_hub

from fondant.component import ComponentSpec

# read spec
spec = ComponentSpec.from_file(path="spec.yaml")

# read index dataframe
dataframe = dd.read_parquet("/Users/nielsrogge/Downloads/index_data")
# merge subsets with index
subset_df = dd.read_parquet("/Users/nielsrogge/Downloads/images_data")
subset_name = "images"
subset_df = subset_df.rename(columns={col: subset_name + "_" + col for col in subset_df.columns})
dataframe = dd.merge(dataframe, subset_df, left_index=True, right_index=True, how="inner")

print(dataframe.columns)

# for name, subset in spec.consumes.items():
#     fields = list(subset.fields.keys())
#     subset_df = dd.read_parquet("")
#     # left joins -> filter on index
#     dataframe = dd.merge(
#         dataframe,
#         subset_df,
#         left_index=True,
#         right_index=True,
#         how="inner",
#     )

write_columns = []
schema_dict = {}
for subset_name, subset in spec.consumes.items():
    for field in subset.fields.values():
        column_name = f"{subset_name}_{field.name}"
        write_columns.append(column_name)
        schema_dict[column_name] = generate_from_arrow_type(field.type.value)

schema = datasets.Features(schema_dict).arrow_schema
dataframe = dataframe[write_columns]

# Write dataset to the hub
repo_id = "nielsr/test-dask"
repo_path = f"hf://datasets/{repo_id}"
huggingface_hub.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True)
dd.to_parquet(dataframe, path=f"{repo_path}/data", schema=schema)