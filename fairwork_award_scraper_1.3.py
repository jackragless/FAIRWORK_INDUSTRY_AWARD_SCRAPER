#!/usr/bin/env python
# coding: utf-8

# In[248]:


#required libraries
import mammoth
import os
from bs4 import BeautifulSoup as bs
import pandas as pd
import datetime
import requests


# In[232]:


#mines all .docx award URLs from current FairWork pay guide page
os.mkdir('award_docs')
year = '2020'

doc_links = []
print('mining pay guides...')
url = 'https://www.fairwork.gov.au/pay/minimum-wages/pay-guides'
r = requests.get(url, allow_redirects=True)
focus = str(r.content)
focus = focus[focus.find('<h2>'):focus.find('<section')]
focus = bs(focus, 'html.parser').find_all('a', href=True)
for i in focus:
    if i['href'].find('.docx.aspx')!=-1:
        doc_links.append('https://www.fairwork.gov.au' + i['href'])
        
#downloads .docx files from these links
for i in doc_links:
    temp_link = requests.get(i, allow_redirects=True)
    open('award_docs/'+i.replace('.aspx','').replace('/','|'), 'wb').write(temp_link.content)


# In[234]:


#converting all .docx files to single HTML array
print('converting docs to HTML array...')
directory = 'award_docs'
html = []
for filename in os.listdir(directory):
    if filename.endswith(".docx"):
        with open(os.path.join(directory, filename), "rb") as docx_file:
            result = mammoth.convert_to_html(docx_file)
            html.append({'filename':filename, 'html':result.value})


# In[235]:


#Applies some miscellaneous filters due to fix HTML conversion quirks
def misc_filters():
    for i in range(len(html)):
        html[i]['html'] = html[i]['html'].replace('</h3><h3>',' - ')
        html[i]['html'] = html[i]['html'][0:html[i]['html'].find('<h2>Allowances</h2>')]
        
misc_filters()


# In[236]:


#splits HTML page into segments (eg. Part-time / Full-Time tables; Casual tables)
def segment_tables(page):
    start_pos = 0
    indexes = []
    while True:
        text = page[start_pos:len(page)]
        temp_index = text.find('<h3>')
        if temp_index == -1:
            break
        start_pos += temp_index + 4
        indexes.append(start_pos-4)
    indexes.append(len(page)) #final index to end of page HTML string
        
    return indexes


# In[237]:


#Converts all columns into separate row items using melt() (ie. one wage rate per row) and concatenates
def parse_tables(segment, award, category, date_accessed, document_url, period_start):
    soup = bs(segment, 'html.parser').find_all('table')
    count = 0
    for i in pd.read_html(str(soup)):
        if i.shape[1]>2:
            temp_table = i.melt(id_vars=['Classification'])
        else:
            temp_table = i
        temp_table.insert(0,'award',[award]*temp_table.shape[0],True)
        temp_table.insert(1,'category',[category]*temp_table.shape[0],True)
        temp_table.insert(temp_table.shape[1],'date_accessed',[date_accessed]*temp_table.shape[0],True)
        temp_table.insert(temp_table.shape[1],'document_url',[document_url]*temp_table.shape[0],True)
        temp_table.insert(temp_table.shape[1],'period_start',[period_start]*temp_table.shape[0],True)
        
        if count == 0:
            final_table = temp_table
        else:
            final_table = final_table.append(temp_table)
        
        count += 1
    return final_table.reset_index(drop=True)


# In[238]:


#feeds metadata information into parse_tables() function for a given page
def parse_page(page_html_input):
    cur_page = page_html_input

    segment_index = segment_tables(cur_page['html'])

    if bs(cur_page['html'], 'html.parser').find('h1'):
            temp_award = bs(cur_page['html'], 'html.parser').find('h1').text.replace('Pay Guide - ', '').replace('2010','').replace(' -','').replace(',','').strip().lower()
            if temp_award.find('[')!=-1:
                temp_award = temp_award[:temp_award.find('[')].strip()
    #likely to not work on other documents depending on spacing (remove .replace() later)
    temp_date_accessed = datetime.datetime.now().strftime("%X %d/%m/%Y")

    temp_document_url =  'https://www.fairwork.gov.au/ArticleDocuments/872/' + cur_page['filename'] + '.aspx'

    temp_period_start = bs(cur_page['html'], 'html.parser').find_all('p')[1].text.split()[-3:len(cur_page['html'])]
    temp_period_start = ' '.join(temp_period_start).replace('.','')

    final_df = pd.DataFrame()

    count = 0
    for i in range(len(segment_index)-2):
        soup = bs(cur_page['html'][segment_index[i]:segment_index[i+1]] , 'html.parser')
#         print(soup)
        temp_category = soup.find('h3').text
        if(str(soup).find('<table>') != -1): #ensure table in soup
            temp_df = parse_tables(str(soup), temp_award, temp_category, temp_date_accessed, temp_document_url, temp_period_start)
            final_df = final_df.append(temp_df)

    final_df = final_df.append(parse_tables(str(bs(cur_page['html'][segment_index[-2]:segment_index[-1]], 'html.parser').find('table')), temp_award, temp_category, temp_date_accessed, temp_document_url, temp_period_start))

    return final_df


# In[239]:


#this code segement iteratively calls parse_page for every element in 'HTML' array
print('extracting award rates from HTML array...')
MASTER = pd.DataFrame()
count = 0
failed = []
for i in html:
    try:
        MASTER = MASTER.append(parse_page(i))
        print(count, end=' \r')
    except Exception:
        failed.append(i['filename'])
        print('!', end=' \r')
    count+=1
    
MASTER = MASTER.reset_index(drop=True)


# In[246]:


#stores local file of failed award_documents
#Note most of these errors derive from different table formats (eg. only one row); which breaks scraper melt() function
pd.DataFrame(failed).to_csv('failed_{}.csv'.format(year))


# In[241]:


#adding some reference indexes to the award rows
concat_name = []
dtt_index = []
count = 0
for i in range(MASTER.shape[0]):
    concat_name.append(str(MASTER['award'][i])+';'+str(MASTER['category'][i]) +';'+str(MASTER['Classification'][i])+';'+str(MASTER['variable'][i]))
    count += 1
    dtt_index.append('dtt_' + str(count))
    
    
MASTER.insert(0, 'concat_name', concat_name)
MASTER.insert(0, 'dtt_index', dtt_index)


# In[244]:


#ad hoc fix for random, empty columns appearing in table; no time to find underlying cause of error
MASTER = MASTER.drop(['Hourly pay rate','Daily pay rate','Stream','Casual pay rate','Hourly base rate'], axis = 1) 


# In[ ]:


#save final table as CSV
MASTER.to_csv('FINAL_AWARDS.csv')
print('done! Check fail array for pages that failed to parse.')

