import re
import requests
import pandas as pd
from bs4 import BeautifulSoup


URL = "https://en.wikipedia.org/wiki/List_of_best-selling_albums_of_the_2000s_(decade)_in_the_United_Kingdom"
csv = r"output\albums.csv"
print('Parsing table from URL: ', URL)

# Use pandas to extract tables from URL
tables = pd.read_html(URL)
table = tables[1]

# Remove unwanted columns
table = table.rename(columns={"No.": "Album_ID"})
columns = ['Peak position', 'Sales']
for column in columns:
    table.drop(column, axis=1, inplace=True)

# Ensure column maintains proper formatting
for i in range(table.shape[0]):
    table.iloc[i, 0] = re.sub(r'\[.*]', '', table.iloc[i, 0])


# Create list of Artists to use to remove unwanted URLs
remove = table['Artist'].tolist()
for index, artist in enumerate(remove):
    remove[index] = r'/wiki/' + re.sub(r'\s', '_', re.sub(r'\'', '%27', artist))

# Use Beautiful Soup to trim the page to table we're interested in
page = requests.get(URL)
soup = BeautifulSoup(page.content, "html.parser")
results = soup.find(id="bodyContent")
elements = results.find("table", class_="wikitable sortable plainrowheaders").find_all('a')

# Create list of sub-strings to identify unwanted URLs
removeAlso = [r'#cite_note-.*', r'.*singer\)', r'.*band\)', r'.*entertainer\)', r'.*musician\)']

# Clean URLs to be added to CSV
links = []
for index, element in enumerate(elements):
    link = element.get('href')
    for string in remove:
        if string == link:
            link = re.sub(r'.*' + string, '', link)
    for string in removeAlso:
        link = re.sub(string, '', link)
    if link != '':
        links.append(f'https://en.wikipedia.org{link}')

table['URL'] = links

# Write cleaned table to CSV
table.to_csv(csv, index=False, sep='|')
print('Successfully parsed table!')
