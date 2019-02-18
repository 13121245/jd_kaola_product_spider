# coding: utf-8

from pymongo import MongoClient
from collections import defaultdict
from datetime import datetime
import pandas as pd


MONGO_URI = 'mongodb://10.214.224.142:20000'
COL = 'jd'


def transfer_jd():
    conn = MongoClient(MONGO_URI)
    db = conn['onlineshop']
    data = defaultdict(lambda: [])
    update_date = datetime(2016, 11, 7)
    for item in db[COL].find({"update_date": update_date}, {'_id': 0, 'update_date': 0}):
        for key in item:
            if key == 'sub_type':
                data[key].append('#'.join(item[key]))
            else:
                data[key].append(item[key])
    print('transfer over')
    for key in data:
        print(len(data[key]))
    df = pd.DataFrame(data)
    writer = pd.ExcelWriter(u'D:/jd.xlsx')
    df.to_excel(excel_writer=writer, encoding='gbk')
    writer.close()


def transfer_kl():
    conn = MongoClient(MONGO_URI)
    db = conn['onlineshop']
    data = defaultdict(lambda: [])
    for item in db['kaolaGoods'].find({}, {'_id': 0, 'update_time': 0}):
        for key in item:
            data[key].append(item[key])
    print('transfer over')
    for key in data:
        print(len(data[key]))
    df = pd.DataFrame(data)
    writer = pd.ExcelWriter(u'D:/kaola.xlsx')
    df.to_excel(excel_writer=writer, encoding='gbk')
    writer.close()

if __name__ == '__main__':
    transfer_jd()
    # transfer_kl()