import matplotlib.pyplot as plt
import pandas as pd
import requests as rq

headers = {
    "User-Agent": "Wikimedia Analytics API Tutorial (<your wiki username>) compare-page-metrics.py",
}

diff_url = """https://wikimedia.org/api/rest_v1/metrics/bytes-difference/\
absolute/per-page/en.wikipedia.org/Land/all-editor-types/\
daily/20220401/20221231"""

view_url = """https://wikimedia.org/api/rest_v1/metrics/pageviews/\
per-article/en.wikipedia.org/all-access/all-agents/\
Land/daily/20220401/20221231"""



edit_url = """
https://wikimedia.org/api/rest_v1/metrics/edits/\
per-page/en.wikipedia.org/Land/all-editor-types/\
daily/20220401/20221231"""

diff_response = rq.get(diff_url, headers=headers).json()
view_response = rq.get(view_url, headers=headers).json()
edit_response = rq.get(edit_url, headers=headers).json()

# bytes changed endpoint #
diff_df = pd.DataFrame.from_records(diff_response["items"][0]["results"])
diff_df["timestamp"] = pd.to_datetime(diff_df["timestamp"])
diff_df = diff_df.set_index("timestamp")

# views endpoint #
view_df = pd.DataFrame.from_records(view_response["items"])
view_df["timestamp"] = pd.to_datetime(
    view_df["timestamp"], format="%Y%m%d%H"
).dt.tz_localize("UTC")
view_df = view_df.set_index("timestamp")
view_df = view_df.drop(columns=["project", "article", "granularity", "access", "agent"])

# edits endpoint #
edit_df = pd.DataFrame.from_records(edit_response["items"][0]["results"])
edit_df["timestamp"] = pd.to_datetime(edit_df["timestamp"])
edit_df = edit_df.set_index("timestamp")

r = pd.merge(diff_df, edit_df, on="timestamp", how="outer")
r = pd.merge(r, view_df, on="timestamp", how="outer").fillna(0).astype(int)

# plotting #
plt.style.use("bmh")

fig, axes = plt.subplots(nrows=3, ncols=1, sharex=True, sharey=False, figsize=(15,10))

r["views"].plot(ax=axes[0], color="c", title="Views")
r["edits"].plot(ax=axes[1], color="m", title="Edits")
r["abs_bytes_diff"].plot(ax=axes[2], color="y", title="Absolute change (bytes)")

plt.show()