import requests
import json
import os


from typing import List

pids: List[int] = [1749315, 1007491, 1644899, 1392674, 1015940, 1659841, 1117412, 1741416, 1540970, 2464113, 1021377, 1020113, 1007260, 1132087, 1129738, 1037736, 1019322, 1023471, 1824494, 2008587, 1369529, 1834731, 1041687, 1079411, 1021343, 1608191, 1620435, 1623459, 1139620, 1794516, 1086469, 1600158, 1112759, 1032017, 1004784, 1040985, 1650869, 1859987, 1615040, 1851768, 1016983, 1603371, 2021653, 1111262, 2334646, 1306068, 1653058, 1169888, 1003268, 1010172, 1319916, 1037532, 1084529, 1006657, 1329091, 1598500, 1958003, 1102322, 1617123, 1032643, 1334301, 2606127, 1703072, 1152402, 1066286, 1287543, 3155957, 3119268, 1210839, 1427001]  # 请替换为您的实际PID列表


base_url = "https://yx.dmzgame.com/warpath/pid_detail"

data_to_save = {}


save_path = "D:\\Desktop"
if not os.path.exists(save_path):
    os.makedirs(save_path)
file_name = "hi20pids_data.json"
full_path = os.path.join(save_path, file_name)


for pid in pids:

    params = {"pid": pid, "page": "1", "perPage": "50"}

    response = requests.get(base_url, params=params)

    if response.status_code == 200:

        json_data = response.json()

        data_to_save[pid] = json_data
    else:
        print(f"获取PID {pid} 的数据失败，状态码：{response.status_code}")


with open(full_path, "w", encoding="utf-8") as json_file:
    json.dump(data_to_save, json_file, ensure_ascii=False, indent=4)

print(f"所有数据已保存到 {full_path} 文件中。")
