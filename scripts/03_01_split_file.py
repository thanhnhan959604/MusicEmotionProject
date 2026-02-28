import os
import pandas as pd

INPUT_FILE = "data/metadata/audio_metadata.csv"
OUTPUT_DIR = "data/metadata/splits"
CHUNK_SIZE = 500


def split_csv_by_500():

    if not os.path.exists(INPUT_FILE):
        print("Không tìm thấy file")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = pd.read_csv(INPUT_FILE, encoding="utf-8-sig")

    total_rows = len(df)
    print("Tổng dòng:", total_rows)

    file_index = 1
    start_index = 0

    while start_index < total_rows:

        end_index = start_index + CHUNK_SIZE

        df_chunk = df.iloc[start_index:end_index]

        output_file = os.path.join(
            OUTPUT_DIR,
            f"audio_metadata_part_{file_index}.csv"
        )

        df_chunk.to_csv(output_file, index=False, encoding="utf-8-sig")

        print(f"Tạo file: {output_file} | Số dòng: {len(df_chunk)}")

        start_index = end_index
        file_index += 1

    print("Đã chia xong")


if __name__ == "__main__":
    split_csv_by_500()