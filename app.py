import os
from flask import  render_template
from jinja2 import Template
import json

def process_json(file_name):
    input_folder_name="data/RAW/"
    with open(os.path.join(input_folder_name,file_name+".json"),'r') as f:
        for json_page in json.load(f)['pass']:
            processing_results=json_page
    return  processing_results

def process_scd(mapping):
    template = Template(open("templates/dags/scd2.py").read())
    return template.render(mapping=mapping)


def process_py(id=0):
    json = process_json(id)
    file = open('dags/'+id+'.py', 'w')
    file.write(process_scd(json))



process_py("event")