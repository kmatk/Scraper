import re
import warnings
import numpy as np
import pandas as pd

warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None


def create_album_csv(source, row, current_id):
    # declare return variables
    song_count = 0
    problem_url = ''
    successful = False

    # retrieve desired information from source csv
    url = source.at[row, 'URL']
    album_id = source.at[row, 'Album_ID']

    # prep filename
    file_name = source.at[row, 'Artist'] + '_' + source.at[row, 'Title']
    file_name = re.sub(r'\s', '_', file_name)
    file_name = re.sub('–', '-', file_name)
    remove = [r'/', r':']
    for item in remove:
        file_name = re.sub(item, '', file_name)
    csv_file_name = 'output/' + file_name + '.csv'

    # pull table from URL
    tables = pd.read_html(url)

    # check to see if we retrieved the proper table
    desired_columns = ['No.', 'Title', 'Length']
    table = tables[0]
    try:
        for index in range(3, 10):
            table = tables[index]
            headers = table.columns.values
            check_columns = all(item in headers for item in desired_columns)
            if check_columns:
                table = tables[index]
                table = table[desired_columns]
                break

        # remove last row if it doesn't contain a track
        last_row = table.shape[0]-1
        last_title = table.at[last_row, 'Title']
        if last_title == 'Total length:':
            table = table.drop(last_row, axis=0)

        # remove extra quotations, format No. column, convert time from MM:SS to sec
        song_count = table.shape[0]
        for index in range(song_count):
            table.at[index, 'Title'] = re.sub('\"', '', table.at[index, 'Title'])
            if isinstance(table.at[index, 'No.'], np.float64) | isinstance(table.at[index, 'No.'], float):
                table.at[index, 'No.'] = str(table.at[index, 'No.'])
            table.at[index, 'No.'] = re.sub(r'\.0', '', table.at[index, 'No.'])
            table.at[index, 'No.'] = re.sub(r'\.', '', table.at[index, 'No.'])
            length_str = table.at[index, 'Length']
            table.at[index, 'Length'] = sum(s*int(t) for s, t in zip([1, 60, 3600], reversed(length_str.split(':'))))

        # rename Track_No. and Length columns
        table = table.rename(columns={"No.": "Track_No", "Length": "Duration"})
        # add Track_ID column to be used as Primary Key
        ids = np.arange(current_id, current_id + song_count)
        table['Track_ID'] = ids
        # add Album_ID column to be used as Foreign Key
        table['Album_ID'] = [album_id] * song_count
        # reorder columns
        cols = table.columns.tolist()
        cols = cols[3:4] + cols[4:] + cols[0:3]
        table = table[cols]

        # write table to csv file
        table.to_csv(csv_file_name, index=False, sep='|')
        successful = True

    # handle exceptions
    except KeyError as err:
        print(f'KeyError: {err} Error parsing table. Problem with tables at: {url}')
        problem_url = url
    except IndexError as err:
        print(f'IndexError: {err} Error parsing table. Problem with tables at: {url}')
        problem_url = url
    except Exception as err:
        print(f'Exception: {err} Error parsing table. Problem with tables at: {url}')
        problem_url = url

    # return
    return problem_url, successful, file_name, song_count


# source/outputs of album info
csv = 'output/albums.csv'
bad_urls_txt = 'output/bad_urls.txt'
albums_txt = 'output/album_list.txt'
albums = pd.read_csv(csv, sep='|')

# declare variables to store output of create_album_csv function
successes = 0
total_songs = 1
bad_urls = []
album_file_name = []

# loop through all albums
print('Parsing URLs:')
for i in range(albums.shape[0]):
    # execute create_album_csv function for album in albums.csv file
    bad_url, success, album_file, songs = create_album_csv(albums, i, total_songs)

    # if successfully parsed, increment and append album file, else append bad urls
    if success:
        successes += 1
        total_songs += songs
        album_file_name.append(album_file)
    else:
        bad_urls.append(bad_url)

    # execute on last loop through albums CSV
    if i == albums.shape[0]-1:
        print(f'Successfully parsed {successes} pages.')

        # write bad urls to file
        with open(bad_urls_txt, 'w') as bad_urls_file:
            for j in bad_urls:
                bad_urls_file.write(j + '\n')

        # write albums directory to file
        with open(albums_txt, 'w') as album_names_file:
            for x, k in enumerate(album_file_name):
                album_names_file.write(re.sub('–', '-', k))
                if x != len(album_file_name)-1:
                    album_names_file.write('\n')

        print(f'List of bad urls located in {bad_urls_txt}')
        print(f'List of album CSVs located in {albums_txt}')
