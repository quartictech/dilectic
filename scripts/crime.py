import csv
import os.path
import glob
from datetime import datetime


def fill_crime_table(data_dir):
    paths = glob.glob(data_dir + '/crime_data/*/*')
    counter = 0
    for files in paths:
        with open(files) as f:
            rdr = csv.reader(f)
            next(rdr)
            for line in rdr:
                # if counter%10 != 0: #do this if testing otherwise huge
                #     counter += 1
                #     continue
                # counter += 1
                if len(line) == 10:
                    values = (line[0], datetime.strptime(line[1], '%Y-%M'),
                            line[2], line[3], line[4], line[5],
                            line[6], line[7], line[8], None, line[9], None)
                elif len(line) == 12:
                    values = (line[0], datetime.strptime(line[1], '%Y-%M'),
                                line[2], line[3], line[4], line[5],
                                line[6], line[7], line[8], line[9], line[10], line[11])
                else:
                    print('Odd line length {}'.format(len(line)))
                    print(line)

                yield values


if __name__ == '__main__':
    fill_crime_table('../data/')
