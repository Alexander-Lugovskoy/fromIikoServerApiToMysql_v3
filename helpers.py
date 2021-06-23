import shutil
import requests

def SaveJsonInFile(data, src='\logger\olap_preset.json'):
    f = open(src, 'a', encoding="utf-8")
    f.write("[" + str(datetime.now()) + "] " + data + "\n")
    f.close()
