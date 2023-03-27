from datetime import datetime

def main(file='execution_dates.csv'):

    with open(file, 'a') as f:
        f.write(str(datetime.now()) + '\n')

if __name__ == '__main__':

    main()