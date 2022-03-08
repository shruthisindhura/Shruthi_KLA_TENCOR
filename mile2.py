import datetime
import time
import threading
import yaml
import csv

task_names = {}


def DataLoad(filename):
    return_value = []
    rows = []
    with open(filename, 'r') as csvfile:
        csvreader = csv.reader(csvfile)

        for row in csvreader:
            rows.append(row)
    return_value.append(rows)
    return_value.append(len(rows) - 1)
    return return_value


def sequential_flow(data, parent):
    file.write(str(datetime.datetime.now()) + ';' + parent + ' Entry\n')
    for key, value in data.items():
        if value['Type'] == 'Flow':
            if value['Execution'] == 'Sequential':
                sequential_flow(value['Activities'], parent)
            else:
                concurrent_flow(value['Activities'], str(parent + '.' + key))
        elif value['Type'] == 'Task':
            task(value, str(parent + '.' + key))
    file.write(str(datetime.datetime.now()) + ';' + parent + ' Exit\n')


def concurrent_flow(data, parent):
    file.write(str(datetime.datetime.now()) + ';' + parent + ' Entry\n')
    thread_list = []
    for key, value in data.items():
        if value['Type'] == 'Task':
            x = threading.Thread(target=task, args=(value, str(parent + '.' + key)))
            thread_list.append(x)
        else:
            if value['Execution'] == 'Sequential':
                x = threading.Thread(target=sequential_flow, args=(value['Activities'], str(parent + '.' + key)))
                thread_list.append(x)
            else:
                x = threading.Thread(target=concurrent_flow, args=(value['Activities'], str(parent + '.' + key)))
                thread_list.append(x)

    for i in thread_list:
        i.start()
    for i in thread_list:
        i.join()
    file.write(str(datetime.datetime.now()) + ';' + parent + ' Exit\n')


def task(data, parent):
    print(parent)
    file.write(str(datetime.datetime.now()) + ';' + parent + ' Entry\n')
    function_name = data['Function']

    if function_name == 'TimeFunction':
        condition_there=False
        can_pass = False
        try:

            condition = data['Condition']
            condition_there=True
            condition_list = condition.split(' ')
            name = condition_list[0].split(')')[0][2:]
            if condition_list[1] == '<':
                temp = name.split('.')
                string_code = 'task_names'
                index = temp[-1]
                temp.remove(index)
                if task_names['.'.join(temp)][index] > int(condition_list[2]):
                    can_pass = True
        except:
            pass

        if can_pass or not condition_there:
            execution_time = data['Inputs']['ExecutionTime']
            function_input = data['Inputs']['FunctionInput']

            file.write(
            str(datetime.datetime.now()) + ';' + parent + ' Executing ' + function_name + ' (' + function_input + ',' + ' ' + execution_time + ')' + '\n')
            time.sleep(float(execution_time))
        else:
            file.write(str(datetime.datetime.now()) + ';' + parent + ' Skipped\n')
        file.write(str(datetime.datetime.now()) + ';' + parent + ' Exit\n')
    elif function_name == 'DataLoad':
        condition_there=False
        can_pass = False
        try:

            condition = data['Condition']
            condition_there=True
            condition_list = condition.split(' ')
            name = condition_list[0].split(')')[0][2:]
            if condition_list[1] == '>':
                temp = name.split('.')
                string_code = 'task_names'
                index=temp[-1]
                temp.remove(index)
                if task_names['.'.join(temp)][index] > int(condition_list[2]):
                    can_pass=True
        except:
            pass

        if can_pass or not condition_there:

            filename = data['Inputs']['Filename']
            file.write(
                str(datetime.datetime.now()) + ';' + parent + ' Executing ' + function_name + ' (' + filename + ')' + '\n')
            temp = {}

            return_data = DataLoad(filename)
            temp[data['Outputs'][0]] = return_data[0]
            temp[data['Outputs'][1]] = return_data[1]

            task_names[parent] = temp
            print(task_names)



            file.write(str(datetime.datetime.now()) + ';' + parent + ' Exit\n')


with open('Milestone2A.yaml', 'r') as file:
    data = yaml.safe_load(file)
    file = open('mile2_log.txt', 'w')
    for key, value in data.items():
        workflow = key
        if value['Execution'] == 'Sequential':
            sequential_flow(value['Activities'], workflow)
        else:
            concurrent_flow(value['Activities'], workflow)