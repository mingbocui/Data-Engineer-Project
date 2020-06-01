import pandas as pd
import csv

def preprocess_sas():
    """
    extract data from sas file to csv format
    """
    with open('./I94_SAS_Labels_Descriptions.SAS') as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')

    def code_mapper(file, idx):
        f_content2 = f_content[f_content.index(idx):]
        f_content2 = f_content2[:f_content2.index(';')].split('\n')
        f_content2 = [i.replace("'", "") for i in f_content2]
        dic = [i.split('=') for i in f_content2[1:]]
        dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
        return dic

    i94cit_res = code_mapper(f_content, "i94cntyl")
    i94port = code_mapper(f_content, "i94prtl")
    i94mode = code_mapper(f_content, "i94model")
    i94addr = code_mapper(f_content, "i94addrl")
    i94visa = {'1':'Business', '2':'Pleasure', '3':'Student'}

    # addre
    csv_names = ['i94city', 'i94port', 'i94model', 'i94addr', 'i94visa']
    dict_list = [i94cit_res, i94port, i94mode, i94addr, i94visa]
    column_name = ['country', 'port', 'model', 'address', 'visa']

    for i, csv_name in enumerate(csv_names):
        csv_columns = ['code', column_name[i]]
        csv_file = csv_name + ".csv"
        dict_data = dict_list[i]
        try:
            with open(csv_file, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                for key in dict_data.keys():
                    data = {'code':key, column_name[i]:dict_data.get(key)}
                    writer.writerow(data)
        except IOError:
            print("I/O error")