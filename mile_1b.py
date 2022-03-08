import datetime
import time
import threading
import yaml
from multiprocessing.pool import ThreadPool as Pool
pool=Pool(processes=5)

def sequential_flow(data, parent):
    file.write(str(datetime.datetime.now()) + ';' + parent + ' Entry\n')
    for key, value in data.items():
        if (value['Type'] == 'Flow'):
            if value['Execution'] == 'Sequential':
                sequential_flow(value['Activities'], parent)
            else:
                concurrent_flow(value['Activities'], str(parent + '.' + key))
        elif value['Type'] == 'Task':
            task(value, str(parent + '.' + key))
    file.write(str(datetime.datetime.now()) + ';' + parent + ' Exit\n')


def concurrent_flow(data, parent):
    file.write(str(datetime.datetime.now()) + ';' + parent + ' Entry\n')
    thread_list=[]
    for key, value in data.items():
        to_call=''
        if value['Type'] == 'Task':
            x = threading.Thread(target=task, args=(value, str(parent + '.' + key)))
            thread_list.append(x)
            #x.start()
            #x.join()
        else:
            if value['Execution'] == 'Sequential':
                x = threading.Thread(target=sequential_flow, args=(value['Activities'], str(parent + '.' + key)))
                #sequential_flow(value['Activities'], parent + '.' + key)
                thread_list.append(x)
            else:
                x = threading.Thread(target=concurrent_flow, args=(value['Activities'], str(parent + '.' + key)))
                #concurrent_flow(value['Activities'], str(parent + '.' + key))
                thread_list.append(x)


    for i in thread_list:
        i.start()
    for i in thread_list:
        i.join()
    file.write(str(datetime.datetime.now()) + ';' + parent + ' Exit\n')


def task(data, parent):
    function_name = data['Function']
    execution_time = data['Inputs']['ExecutionTime']
    function_input = data['Inputs']['FunctionInput']
    print(data)
    file.write(str(datetime.datetime.now()) + ';' + parent + '' + ' Entry\n')
    file.write(
        str(datetime.datetime.now()) + ';' + parent + ' Executing ' + function_name + ' (' + function_input + ',' + ' ' + execution_time + ')' + '\n')
    time.sleep(float(execution_time))
    file.write(str(datetime.datetime.now()) + ';' + parent + ' Exit\n')


with open('Milestone1B.yaml', 'r') as file:
    data = yaml.safe_load(file)
    file = open('mile2_log.txt', 'w')
    for key, value in data.items():
        workflow = key
        if value['Execution'] == 'Sequential':
            sequential_flow(value['Activities'], workflow)
        else:
            concurrent_flow(value['Activities'], workflow)
