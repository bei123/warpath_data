import json


file_path = "D:/worck/战火数据处理/HI2025id.json"


with open(file_path, "r", encoding="utf-8") as file:
    data = json.load(file)


print("data 的内容:", json.dumps(data, ensure_ascii=False, indent=4))

# 提取 pid
if isinstance(data, dict) and "Data" in data:
    data_list = data["Data"]
    if isinstance(data_list, list):
        pids = [
            entry["pid"] for entry in data_list if isinstance(entry, dict) and "pid" in entry
        ]
        print("提取的 pids:", pids)
    else:
        print("Data 不是一个列表。")
else:
    print("data 不是一个字典或者没有 'Data' 键。")
