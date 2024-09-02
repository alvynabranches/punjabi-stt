import os
import srt
import hashlib
import pandas as pd
from tqdm import tqdm
from yt_dlp import YoutubeDL
from srt import SRTParseError
from pydub import AudioSegment
from datasets import Audio, Dataset, DatasetDict, load_dataset, concatenate_datasets


# def push_dataset_to_huggingface(
#     audio_files: list[str],
#     transcriptions: list[str],
#     dataset_name: str,
#     combine: bool = False,
# ):
#     if len(audio_files) != len(transcriptions):
#         raise ValueError("Number of audio files and transcriptions should be the same.")
#     # Load dataset
#     existing_dataset = load_dataset(dataset_name)

#     hf_dataset = Dataset.from_dict(
#         {
#             "id": [
#                 hashlib.sha512(open(file, "rb").read()).hexdigest()
#                 for file in audio_files
#             ],
#             "audio": audio_files,
#             "sentence": transcriptions,
#         }
#     )
#     hf_dataset = hf_dataset.cast_column("audio", Audio())
#     if combine:
#         combined_dataset = Dataset.from_dict(
#             {
#                 "audio": existing_dataset["train"]["audio"] + hf_dataset["audio"],
#                 "text": existing_dataset["train"]["text"] + hf_dataset["text"],
#                 "duration": existing_dataset["train"]["duration"]
#                 + hf_dataset["duration"],
#             }
#         )
#         dataset_dict = DatasetDict({"train": combined_dataset})
#     else:
#         dataset_dict = DatasetDict({"train": hf_dataset})
#     dataset_dict.push_to_hub(dataset_name)


def read_srt(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        srt_content = file.read()

    # Parse the SRT content
    subtitles = list(srt.parse(srt_content))
    data = []
    for subtitle in subtitles:
        data.append(
            {
                "index": subtitle.index,
                "start": subtitle.start.seconds + subtitle.start.microseconds / 10**6,
                "end": subtitle.end.seconds + subtitle.end.microseconds / 10**6,
                "text": subtitle.content,
            }
        )
    return data

def download(url: str, path: str):
    ydl = YoutubeDL(
        {
            "format": "bestaudio/best",
            "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3"}],
            "outtmpl": path,  # Save the file with the specified path
        }
    )
    ydl.download([url])
    audio = AudioSegment.from_file(f"{path}.mp3")
    audio.export(f"{path}.mp3", format="mp3")


df = pd.read_excel("links.xlsx").dropna()
for n in range(len(df)):
    if n != 468:
        continue
    try:
        data = read_srt(f"/Users/alvynabranches/Downloads/punjabi_001_508/{df["filename"][n]}.srt")
        if not os.path.isfile(f"{df["filename"][n]}.mp3"):
            download(df["link"][n], df["filename"][n])
        audio = AudioSegment.from_mp3(f"{df["filename"][n]}.mp3")
        audio_files = []
        transcriptions = []
        for d in tqdm(data):
            file = f"temp_punjabi/{df["filename"][n]}_{d['index']:04}.mp3"
            audio[d["start"] * 1000 : d["end"] * 1000].export(file, format="mp3")
            audio_files.append(file)
            transcriptions.append(d["text"])
        dataset = Dataset.from_dict(
            {
                "id": [
                    hashlib.sha512(open(file, "rb").read()).hexdigest()
                    for file in audio_files
                ],
                "audio": audio_files,
                "sentence": transcriptions,
            }
        )
        dataset.cast_column("audio", Audio())
        dataset.to_parquet(f"data/{df["filename"][n]}.parquet")
        os.remove(f"{df["filename"][n]}.mp3")
        os.system(f"rm temp_punjabi/{df["filename"][n]}_*")
    except SRTParseError as e:
        print(e)
    except FileNotFoundError as e:
        print(e)
