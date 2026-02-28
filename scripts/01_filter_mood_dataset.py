import pandas as pd

tag_path = "../data/raw/mood_tag.txt"
tsv_path = "../data/raw/autotagging_moodtheme.tsv"
felter_csv_path = "../data/intermediate/mood_filtered_dataset.csv"

df_tag = pd.read_csv(tag_path, header=None)
mood_list = []
for i in df_tag[0]:
    if "---" in i:
        mood = i.split("---")[-1]
        mood_list.append(mood)

df_raw = pd.read_csv(tsv_path, header=0, engine="python", names=["full_line"])
df_split = df_raw["full_line"].str.split("\t", n=5, expand=True)
df_split.columns = ["TRACK_ID", "ARTIST_ID", "ALBUM_ID", "PATH", "DURATION", "TAGS"]

print(df_split.head())

rows = []
for _, row in df_split.iterrows():
    moods = []
    tags = str(row["TAGS"]).split("\t")
    for tag in tags:
        if "---" in tag:
            label = tag.split("---")[-1]
            if label in mood_list:
                moods.append("mood---"+label)
    if len(moods)>0:
        rows.append([row["TRACK_ID"], row["ARTIST_ID"], row["ALBUM_ID"], row["PATH"], row["DURATION"], sorted(set(moods))])

df_out = pd.DataFrame(rows, columns=["TRACK_ID", "ARTIST_ID", "ALBUM_ID", "PATH", "DURATION", "TAGS"])
df_out.to_csv(felter_csv_path, index=False)