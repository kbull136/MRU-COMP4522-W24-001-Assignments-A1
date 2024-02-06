# Adv DB Winter 2024 - 1

import random

data_base = [] # Global binding for the Database contents
columns = []
'''
transactions = [['id1',' attribute2', 'value1'], ['id2',' attribute2', 'value2'],
                ['id3', 'attribute3', 'value3']]
'''
transactions = [['1', 'Department', 'Music'], ['5', 'Civil_status', 'Divorced'],
                ['15', 'Salary', '200000']]
DB_Log = [] # <-- You WILL populate this as you go

def recovery_script(log:list):  #<--- Your CODE
    '''
    Restore the database to stable and sound condition, by processing the DB log.
    '''
    print("Calling your recovery script with DB_Log as an argument.")
    print("Recovery in process ...\n")

    id = log[-1][0]
    data_base[int(id)] = log[-1]
    
    
def transaction_processing(idx:int):
    '''
    1. Process transaction in the transaction queue.
    2. Updates DB_Log accordingly
    3. This function does NOT commit the updates, just execute them
    '''
    
    #Appends to DB_Log the state of the previous database
    idToFind = transactions[idx][0]
    
    OG_DB = list(data_base[int(idToFind)])
    DB_Log.append(OG_DB)
    print(DB_Log)

    indexOfColumn = 0
    
    for header in columns:
        if(transactions[idx][1] == header):
            indexOfColumn = columns.index(header)
    
    valToChange = transactions[idx][2]
    

    for row in data_base:
        
        # we have to assume that the ID is the first column. Otherwise we can find it but who has time for that
        if(row[0] == idToFind):
            row[indexOfColumn] = valToChange
            break  
        
    #print(data_base)

    
#new code
def write_file():
    #this should run after transactions are fully commited to write the updated csv so it doesn't overwrite original
    newFile = open("Employees_DB_ADV_UPDATED.csv", "w")


def read_file(file_name:str)->list:
    '''
    Read the contents of a CSV file line-by-line and return a list of lists
    '''
    data = []
    #
    # one line at-a-time reading file
    #
    with open(file_name, 'r') as reader:
    # Read and print the entire file line by line
        line = reader.readline()
        while line != '':  # The EOF char is an empty string
            line = line.strip().split(',')
            data.append(line)
             # get the next line
            line = reader.readline()

    size = len(data)
    print('The data entries BEFORE updates are presented below:')
    for item in data:
        print(item)
    print(f"\nThere are {size} records in the database, including one header.\n")
    return data

def is_there_a_failure()->bool:
    '''
    Simulates randomly a failure, returning True or False, accordingly
    '''
    value = random.randint(0,1)
    if value == 1:
        result = True
    else:
        result = False
    return result

def grabColumns():
    global columns
    columns = data_base[0]

def main():
    number_of_transactions = len(transactions)
    must_recover = False
    global data_base
    data_base = read_file('Employees_DB_ADV.csv')
    failure = False
    failing_transaction_index = None
    grabColumns()
    while not failure:
        # Process transaction
        for index in range(number_of_transactions):
            print(f"\nProcessing transaction No. {index+1}.")    #<--- Your CODE (Call function transaction_processing)
            transaction_processing(index)
            print("UPDATES have not been committed yet...\n")
            failure = is_there_a_failure()
            if failure:
                must_recover = True
                failing_transaction_index = index + 1
                print(f'There was a failure whilst processing transaction No. {failing_transaction_index}.')
                break
            else:
                print(f'Transaction No. {index+1} has been commited! Changes are permanent.')
                
    if must_recover:
        #Call your recovery script
        recovery_script(DB_Log) ### Call the recovery function to restore DB to sound state
    else:
        # All transactions ended up well
        #where the write file would go for output 
        print("All transaction ended up well.")
        print("Updates to the database were committed!\n")

    print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:')
    for item in data_base:
        print(item)
    
main()


