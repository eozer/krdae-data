import os
import pandas as pd
import git
from tqdm import tqdm

fname = 'krdae-data'
# starting commit hash "4993d..." is the first commit where krdae-data.csv was
# written, we do not need to go back further.
ref = '4993d5837e5b705aa208104f5bb7cf6c6174f9e1..main'
# 50 millions of rows is our hard limit. With this we should be able to use
# less than 7 GBs of RAM, which is inline with the Github's free repository
# hardware limits. This arbitrary number was found experimentally.

# large: ~1 GB disk space, ~7GBs memory
# medium: ~100 MBs disk space, ~32MBs memory
# small: ~20 MBs disk space, ~32MBs memory
lg_rows_max = int(5e7)
md_rows_max = int(6e6)
sm_rows_max = int(1e6)


def iterate_file_versions(repo_path: str, filepath: str, ref: str = "main"):
    repo = git.Repo(repo_path, odbt=git.GitDB)
    commits = reversed(list(repo.iter_commits(ref, paths=filepath)))
    for commit in commits:
        blob = [b for b in commit.tree.blobs if b.name == filepath][0]
        yield commit.committed_datetime, commit.hexsha, blob.data_stream


os.system("git clone https://github.com/eozer/krdae-data.git")
it = iterate_file_versions('krdae-data', f"{fname}.csv", ref)
enum_it = enumerate(it)
i, (t, h, f) = enum_it.__next__()
df: pd.DataFrame = pd.read_csv(f)
print("Iterating over commits")
for i, (t, h, f) in tqdm(enum_it):
    df = df.append(pd.read_csv(f))
    # every 10000 commit drop duplicates to gain some memory back and break if
    # we are going to run out of memory soon.
    if i % int(1e4) == 0:
        df.drop_duplicates(inplace=True)
        if df.shape[0] > lg_rows_max:
            print("Warning very large dataset!")
            break

df.drop_duplicates(inplace=True)
df.reset_index(drop=True, inplace=True)
# Use more efficient date types. Conversion, instead of providing data type
# during initialization, results in a smaller memory footprint.
df['timestamp'] = pd.to_datetime(df['timestamp'])
df["location"] = df["location"].astype("category")
df[["latitude", "longtitude", "depth_km", "MD", "ML", "Mw"]] = df[["latitude",
                                                                   "longtitude",
                                                                   "depth_km", "MD", "ML", "Mw"]].apply(pd.to_numeric, downcast="float")
# most recent events are in beginning.
df.sort_values("timestamp", axis=0, ascending=False, inplace=True)

print(df.info(memory_usage='deep', verbose=True))

print("Writing to csv: small data... ", end="")
comp_opts = {
    "method": "zip",
    "archive_name": "krdae-data.csv"
}
df[:sm_rows_max].to_csv(f"{fname}-sm.zip", index=False, compression=comp_opts)
print("OK")
print("Writing to csv: medium data.. ", end="")
df[:md_rows_max].to_csv(f"{fname}-md.zip", index=False, compression=comp_opts)
print("OK")
print("Writing to csv: large data... ", end="")
df[:lg_rows_max].to_csv(f"{fname}-lg.zip", index=False, compression=comp_opts)
print("OK")
print("Finished!")
