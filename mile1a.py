import datetime
import time

import yaml

with open('Milestone1A.yaml', 'r') as file:
    data = yaml.safe_load(file)
    with open('mile1_log.txt', 'w') as output_file:
        for key, value in data.items():

            output_file.write(str(datetime.datetime.now()) + ';' + key + ' Entry\n')
            workflow = key
            for key, value in value.items():
                if (key == 'Activities'):
                    for key, value in value.items():
                        name = key
                        subtask_name = workflow + '.' + key
                        output_file.write(str(datetime.datetime.now()) + ';' + subtask_name + ' Entry\n')
                        type = value['Type']
                        if type == 'Task':

                            function_name = value['Function']

                            function_input = value['Inputs']['FunctionInput']
                            execution_time = value['Inputs']['ExecutionTime']
                            output_file.write(
                                str(datetime.datetime.now()) + ';' + subtask_name + ' Executing ' + function_name + ' (' + function_input + ',' + ' ' + execution_time + ')' + '\n')
                            time.sleep(float(execution_time))
                            output_file.write(str(datetime.datetime.now()) + ';' + subtask_name + ' Exit\n')

                        elif type == 'Flow':
                            name = workflow + '.' + name
                            for key, value in value['Activities'].items():
                                function_name = value['Function']
                                output_file.write(str(datetime.datetime.now()) + ';' + name + '.' + key + ' Entry\n')
                                function_input = value['Inputs']['FunctionInput']
                                execution_time = value['Inputs']['ExecutionTime']
                                output_file.write(
                                    str(datetime.datetime.now()) + ';' + name + '.' + key + ' Executing ' + function_name + ' (' + function_input + ',' + ' ' + execution_time + ')' + '\n')
                                time.sleep(float(execution_time))
                                output_file.write(str(datetime.datetime.now()) + ';' + name + '.' + key + ' Exit\n')
                            output_file.write(str(datetime.datetime.now()) + ';' + subtask_name + ' Exit\n')

            output_file.write(str(datetime.datetime.now()) + ';' + workflow + ' Exit\n')
