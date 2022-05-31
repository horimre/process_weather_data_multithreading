import os
import glob
import time
import concurrent.futures
import pandas as pd


directory = 'ghcnd_all_years'


def process_file(file):

    # extract year from filename
    current_year = file[:4]

    # read csv from gzip file, add header, modify datatypes and 'drop' unnecessary columns
    df = pd.read_csv(f'{directory}/{file}',
                     compression='gzip',
                     on_bad_lines='warn',
                     names=['stationID', 'date', 'elementType', 'value', 'mFlag', 'qFlag', 'sFlag', 'observationTime'],
                     dtype={'date': str, 'elementType': str},
                     usecols=['date', 'elementType', 'value'])

    # create new column for year-month
    df['yearMonth'] = df['date'].str[:6]

    # rearrange columns and remove 'date' column
    df = df[['yearMonth', 'elementType', 'value']]

    # remove rows where 'elementType' is not in (TMIN, TMAX)
    df = df.loc[df.elementType.isin(['TMIN', 'TMAX'])]

    # calculate average min and max temperature by 'yearMonth'
    df = df.groupby(['yearMonth', 'elementType']).value.mean()

    # reset the index of the df
    df = df.reset_index()

    # rename 'value' column to 'averageValue'
    df = df.rename(columns={'value': 'averageValue'})

    # create csv and put file into the 'output_files' folder
    df.to_csv(f'{os.getcwd()}/output_files/{current_year}_data.csv', index=False)


if __name__ == "__main__":

    t1 = time.perf_counter()

    # process files from directory with multi-threading
    # Serial Execution: 53 min
    # ThreadPoolExecutor() - multi threading: 34 min (16 threads)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_file, os.listdir(directory))

    # concat csv files into one combined_result.csv file
    # change dir to 'output_files'
    os.chdir(f'{os.getcwd()}/output_files')

    # create list of all csv files in the directory
    all_csv_filenames = [f for f in glob.glob('*.csv')]

    # combine all files from the ordered list
    combined_df = pd.concat([pd.read_csv(f) for f in sorted(all_csv_filenames)])

    # export result to csv
    combined_df.to_csv('combined_data.csv', index=False)

    # value is in 'tenths of degrees C', need to divide with 10 to get actual Celsius values
    result_df = pd.read_csv('combined_data.csv')
    result_df['averageValue'] = result_df['averageValue'].div(10).round(2)
    result_df.to_csv('result.csv', index=False)

    t2 = time.perf_counter()

    print(f'Finished in {round((t2-t1)/60, 2)} minutes')
