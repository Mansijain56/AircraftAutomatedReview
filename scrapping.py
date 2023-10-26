import requests
from bs4 import BeautifulSoup
import datetime
import pandas as pd
import sqlalchemy
import pyodbc


f = open(r'airline_number.txt','r')
number = int(f.read())
f.close


server = 'mansi-sever.database.windows.net,1433'
database = 'mansi-database'
username = 'mansi_admin_login'
password = 'Console@03'
driver = '{ODBC Driver 17 for SQL Server}' 

connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
connection = pyodbc.connect(connection_string)
engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')
recorded_date = str(datetime.date.today())
def collect_airline_names():
    list_of_airlines = []
    url = 'https://www.airlinequality.com/review-pages/latest-airline-reviews/'
    html = requests.get(url).content
    soup = BeautifulSoup(html,'lxml')
    box = soup.find('ul',attrs={'class':'item'})
    airline_names = box.find_all('li')
    for airline_name in airline_names:
        list_of_airlines.append(airline_name.get_text())
    return list_of_airlines

def collect_airline_reviews(airline_name):  
    list_of_reviews = []       # collection of dictionary of reviews
    page = 1
    while(True):
        print(f'Collecting Data From Page {page}')
        url = f"https://www.airlinequality.com/airline-reviews/{airline_name.lower().replace(' ','-')}/page/{page}/"
        html = requests.get(url).content
        soup = BeautifulSoup(html,'lxml')
        boxes = soup.find_all('article',attrs={'itemprop':'review'})  #[]  ''  
        if(bool(boxes)==False):

            break
        for box in boxes:

            dictionary_of_reviews = {}

            title = box.find('h2',attrs={'class':'text_header'}).get_text()

            name = box.find('span',attrs={'itemprop':'name'}).get_text()

            review_date = box.find('time',attrs={'itemprop':'datePublished'}).get_text()

            review = box.find('div',attrs={'class':'text_content'}).get_text()

            over_all_rating = box.find('span',attrs={'itemprop':'ratingValue'})    
            if(over_all_rating==None):

                over_all_rating=0

            else:

                over_all_rating = over_all_rating.get_text()
            table = box.find('table',attrs={'class':'review-ratings'})

            table_rows = table.find_all('tr')  

            d = {}

            for table_row in table_rows:

                table_data = table_row.find_all('td')

                key = table_data[0].get_text()

                value = table_data[1]

                if(value.find('span')):

                    value = len(value.find_all('span',attrs={'class':'star fill'}))

                else:

                    value = value.get_text()

               

                d[key] = value
           

            # column creation

            dictionary_of_reviews['recorded_date'] = recorded_date

            dictionary_of_reviews['airline'] = airline_name

            dictionary_of_reviews['name'] = name

            dictionary_of_reviews['review_date'] = review_date

            dictionary_of_reviews['review'] = review

            dictionary_of_reviews['over_all_rating'] = over_all_rating

            dictionary_of_reviews['detail'] = d
            list_of_reviews.append(dictionary_of_reviews)

        page = page + 1
    return list_of_reviews
list_of_airlines = collect_airline_names()
print(f,'collect_airline_names:(airline[number])')
# airline_reviews = collect_airline_names(airline[number])
airline_reviews = collect_airline_reviews(list_of_airlines[number])


# reviews = collect_airline_reviews('air france')

mydata = pd.json_normalize(airline_reviews)
print(mydata)

f = open('airline_number.txt','w')
new_number = number + 1
f.write(str(new_number))
f.close

mydata.columns = mydata.columns.str.lower()

mydata.columns = mydata.columns.str.replace(' ','_')

mydata.columns = mydata.columns.str.replace('&','and')

mydata.columns = mydata.columns.str.replace('detail.','')

mydata['recorded_date'] = pd.to_datetime(mydata['recorded_date'])
with engine.connect() as connection:

    mydata.to_sql('airline',con=connection,if_exists='append',index=False,schema='dbo')