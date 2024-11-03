  
import os
os.getcwd()


# In[2]:


#jupyter nbconvert --to script main.ipynb


# In[3]:


import pandas as pd
from jobspy import scrape_jobs
search_terms = [
                "data analyst"
                ,"data scientist"
                #,"machine learning"
                #, "python"
                #, "AI"
                , "Actuary"
                , "Aktuar"
                ]
formatted_string = ", ".join(search_terms) 
formatted_string2 = " + ".join(search_terms) 
search_terms.append(formatted_string), search_terms.append(formatted_string2)




# In[4]:


# Initialize an empty DataFrame
df_combined = pd.DataFrame()

# Iterate through each search term and scrape jobs
for term in search_terms:
    jobs = scrape_jobs(
        site_name=["indeed", "linkedin"],  
        search_term=term,                  
        location="Denmark",               
        results_wanted=500,                
        country_indeed='denmark',          
        job_type='fulltime'
    )
    df_jobs = pd.DataFrame(jobs)  # Convert list of dictionaries to DataFrame
    df_combined = pd.concat([df_combined, df_jobs], ignore_index=True)

df_combined.reset_index(drop=True, inplace=True)


# In[5]:


import numpy as np
import pandas as pd
sel_cols = ['date_posted','site','company', 'location','title','description','emails','job_url_direct']
data = df_combined.copy()
data = data[sel_cols]
#del jobs, sel_cols


# In[6]:


from datetime import datetime
data.dtypes
data['date_posted'] = pd.to_datetime(data['date_posted'], errors='coerce')
data['date_posted'] = data['date_posted'].apply(lambda x: x.strftime('%y-%m-%d') if pd.notnull(x) else None)


# In[7]:


#data.to_csv('student_jobs_csv.csv', index = False)
#data.to_excel('student_jobs_xl.xlsx', index = False)


# Load current jobs

# In[8]:

if os.path.exists('jobs_csv.csv'):
    data_old = pd.read_csv('jobs_csv.csv')
else: 
    data_old = data


# Append new jobs to current jobs and remove duplicates

# In[9]:


data_w_new = pd.concat([data_old, data],ignore_index = True)
data_w_new = data_w_new.sort_values(by='description', ascending=False)


# In[10]:


data_w_new = data_w_new.drop_duplicates(subset = ['title', 'company'])
data_w_new = data_w_new.dropna(subset = ['description'])
del data_old


# ### Extracting key points from job description using NLP

# In[11]:


import spacy
import langdetect
from collections import Counter

nlp_en = spacy.load("en_core_web_sm")
nlp_da = spacy.load("da_core_news_sm")


# In[12]:


technical_skills_en = ["data", "data modeling", "analysis", "prediction","machine learning", "neural networks", "scikit-learn", "reserving","non-life", "pricing"]
programming_skills_en =   ["R", "Python", "SQL","VBA","Excel" , "TensorFlow", "Pytorch", "Hadoop", "PySpark", "Git","MLFlow"]
personal_skills_en = ["analytical", "systematic", "passionate", "curious", "proactive", "perseverance", "focus", "see opportunities", "contribute", "team-player"]

from googletrans import Translator

# Initialize translator
translator = Translator()

# Function to translate a list of skills
def translate_skills(skill_list, source_lang='en', target_lang='da'):
    translated_skills = []
    for skill in skill_list:
        translated = translator.translate(skill, src=source_lang, dest=target_lang)
        translated_skills.append(translated.text)
    return translated_skills

# Translate technical skills
technical_skills_da = translate_skills(technical_skills_en)

# Translate programming skills
programming_skills_da = translate_skills(programming_skills_en)

# Translate personal skills
personal_skills_da = translate_skills(personal_skills_en)


# In[13]:


def extract_skills(text):
    # Detect the language of the text
    lang_code = langdetect.detect(text)
    
    # Select the appropriate spaCy model and skills lists based on the detected language
    if lang_code == 'en':
        nlp = nlp_en
        ts = technical_skills_en
        ps = programming_skills_en
        pes = personal_skills_en
    elif lang_code == 'da':
        nlp = nlp_da
        ts = technical_skills_da
        ps = programming_skills_da
        pes = personal_skills_da
    else:
        raise ValueError('Unsupported language code: {}'.format(lang_code))
    
    # Process the text using the selected spaCy model
    doc = nlp(text)

    
    # Extract keywords from the text (excluding stop words and punctuation)
    keywords = [token.text.lower() for token in doc if token.is_alpha and not token.is_stop]
    
    # Find and categorize skills
    ex_ts = [skill for skill in ts if skill.lower() in keywords]
    ex_ps = [skill for skill in ps if skill.lower() in keywords]
    ex_pes = [skill for skill in pes if skill.lower() in keywords]
    #ex_os  = keywords[:10]
    
    return ex_ts, ex_ps, ex_pes
 


# Apply extract_skills function to each row in dataframe

# In[14]:


def extract_skills_for_row(text):
    ex_ts, ex_ps, ex_pes = extract_skills(text)
    return {'technical skills': ex_ts,'programming skills': ex_ps, 'personal skills': ex_pes}

# Apply the function to each row in the DataFrame
skills_df = data_w_new['description'].apply(extract_skills_for_row).apply(pd.Series)
df = pd.concat([data_w_new.iloc[:, :8], skills_df], axis=1)


# In[15]:


from datetime import datetime, timedelta
df_short = df[['date_posted','company','location','title','technical skills', 'programming skills', 'personal skills', 'job_url_direct']]
df_short['date_posted'] = pd.to_datetime(df_short['date_posted'], format='%y-%m-%d')

date = datetime.now() - timedelta(weeks=3)

# Filter rows where 'date_posted' is within the last 3 weeks
filtered_df_short = df_short[df_short['date_posted'] >= date]


# In[16]:


filtered_df_short = filtered_df_short.sort_values(by = "date_posted", ascending = False)
df = df.sort_values(by = "date_posted", ascending = False)


# ### Saving files to csv and xlsx

# In[17]:


import os
#os.getcwd()
df.to_csv('jobs_csv.csv', index = False)
df.to_excel('jobs_xl.xlsx', index = False)


# Saving latest jobs as html file and display in browser

# In[18]:


html_filename = 'jobs.html'
df_short_html = filtered_df_short.to_html(index=False, 
                                 justify='center',  # Center-align content
                                 classes='table table-striped table-hover',  # Add Bootstrap table classes
                                 escape=False,  # Allow HTML in the table (for clickable URLs)
                                 render_links=True)  # Render URLs as clickable links

# Add HTML headers for better styling
from datetime import date 
today = date.today()
html_content = f"""
<!DOCTYPE html>
<html>
<head>
  <style>
    body {{
      font-family: Arial, sans-serif;
      margin: 20px;
      color: #333; /* Text color */
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    th, td {{
      border: 1px solid #dddddd;
      padding: 8px;
      text-align: left;
      font-size: 12px; /* Adjust font size as needed */
    }}
    th {{
      background-color: #f2f2f2;
    }}
    tr:hover {{
      background-color: #f5f5f5;
    }}
    .link {{
      color: blue;
      text-decoration: underline;
      cursor: pointer;
    }}
  </style>
</head>
<body>

<h2>List of recent jobs based on {search_terms[:-2]} search-criterion. Table generated on {today}</h2>

{df_short_html}

</body>
</html>
"""

# Save the HTML content to a file
with open(html_filename, 'w') as f:
    f.write(html_content)

import webbrowser
webbrowser.open(html_filename)


# In[ ]:




