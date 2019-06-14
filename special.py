'''
This scipt generates special naics codes for datasets that don't have them
'''
import os
import re
import csv
import sys
import time
import getopt
import urllib
import zipfile
import datetime
import pandas as pd
import numpy as np
from threading import Thread
from more_itertools import unique_everseen

class data:

    def download(table_id,path):
        
        try:
           
            file = urllib.request.URLopener()
           
            if (table_id == 'Businesses') or (table_id == 'Exports'):
                pass
                
            else:
                dl_url = 'https://www150.statcan.gc.ca/n1/tbl/csv/{}-eng.zip'
                file.retrieve(dl_url.format(table_id),path+str(table_id)+'.zip')

        except Exception as e:
            print (e,'\nDownload of {} failed'.format(table_id))
        
    def clean(source='',file=''):
        
        if '.' not in list(file):
            file = file+'.zip'
        
        try:
            if file == '':
                print ('Please enter a valid file name')

            if file[-3:] == 'csv':
                df = pd.read_csv(source+file,low_memory=False)
                
            if file[-3:] == 'zip':
                z = zipfile.ZipFile(source+file)
                df = pd.read_csv(z.open(file[:-4]+'.csv'),low_memory=False)

                try:
                    #ensure we only grab NAICS codes from the meta data to append later on; not every file will have one
                    meta = pd.read_csv(z.open(file[:-4]+'_MetaData.csv'),usecols=[1,2]).dropna()

                except:
                    meta = []
                
            if (file[-3:] == 'lsx') or (file[-3:] == 'xls'):
                df = pd.read_excel(source+file,low_memory=False)
            
            df,custom = data.process(df,meta,file)

            return (df,custom)

        except Exception as e:
            print (e,file[-3:],'Script can\'t process the file. Try a .csv, .zip, or .xlsx')
        
    def process(df='',meta=[],file=''):
        
        filters = pd.read_csv('data_table.csv',encoding='latin1')
        
        table = file.split('.')[0]

        filters = filters[filters['table_id'] == table]
        columns = filters['columns']
        columns = list(columns)[0].split(',')
        
        new_cols = list(filters[filters['table_id'] == table]['new_names'])
        source = list(filters[filters['table_id'] == table]['source'])
        
        custom = False #flag to apply custom codes to data or not
        atlantic_special = False
        #Businesses -- split NAICS desc. and codes; need to group into S,M,L
        
        max_year = int(datetime.datetime.now().strftime('%Y'))-1
        
        if ('Businesses' not in file):
        
            if ('Exports' in file):
                date_col_name = 'Year'
            else:
                date_col_name = 'REF_DATE'
            
            try:
                df = df[df[date_col_name].apply(lambda x: int(x[:4]) <= max_year)]
            except:
                df = df[df[date_col_name] <= max_year]
        
        if file[:-4] == 'Businesses':
            
            df.drop([0,1,2],inplace=True)
            
            small_bound = '49'
            med_bound = '499'
            s_pos = 0
            m_pos = 0
            
            #group the header into three: s,m,l
            for i in list(df):
                if small_bound in i:
                    if s_pos == 0:
                        s_pos = list(df).index(i)+1

                if med_bound in i:
                    if m_pos == 0:
                        m_pos = list(df).index(i)+1
                        
            df['Small (1-49)'] = df.iloc[:,1:s_pos].sum(axis=1)
            df['Medium (50-499)'] = df.iloc[:,1:m_pos].sum(axis=1)
            df['Large (500+)'] = df.iloc[:,m_pos:].sum(axis=1)
            
            df['Source'] = source[0]
            df['Year'] = 2018
            df['Province'] = 'Alberta' #will have to change if a dataset comes that has all the provinces already
            df['Industry'] = df['NAICS'].apply(lambda x: x.split(' - ')[1])
            df['NAICS'] = df['NAICS'].apply(lambda x: x.split(' - ')[0])

            header = list(df)[0:1]+list(list(df)[-7:])
            df = df[header]

            df = pd.melt(df, id_vars=header[0:1]+header[-4:], value_vars=header[1:4], var_name='Business Size', value_name='Value')

            custom = True
            atlantic_special = True
            
            df = df[new_cols[0].split(',')]
        
        #Exports -- should come with desc; pre-processed in another script
        elif file[:-4] == 'Exports':
            
            df['Source'] = source[0]
            df['Business Size'] = ''
            df = df[['Province of Origin','NAICS6 Description','NAICS6','Year','Value','Source','Business Size','name']]
            
            not_ab = df[~df['Province of Origin'].isin(['Alberta'])]
            not_ab = not_ab.groupby(['Province of Origin','NAICS6 Description','NAICS6','Year','Source','Business Size'],as_index=False)['Value'].sum()
            not_ab['name'] = 'Not applicable'
            not_ab = not_ab[['Province of Origin','NAICS6 Description','NAICS6','Year','Value','Source','Business Size','name']]
            
            df = df[df['Province of Origin'].isin(['Alberta'])].append(not_ab)
            
            custom = True
            atlantic_special = True #businesses and exports have irregular columns
        
        elif file[:-4] == '17100005':
        
            for item in list(df):
                filter = list(filters['filters'])[0].split(',')

                if (any(elem in df[item].unique() for elem in filter)):
                    df = df[df[item].isin(filter)]
                    
            df = df[columns]
            
        #Join naics codes from metadata file -- StatCan files
        else:
            meta[list(meta)[1]] = meta[list(meta)[1]].str.replace('[','').str.replace(']','')
            columns.insert(2,list(meta)[1])
            df = df.merge(meta,left_on='North American Industry Classification System (NAICS)',right_on=list(meta)[0],how='left')
            
            for item in list(df):
                filter = list(filters['filters'])[0].split(',')

                if (any(elem in df[item].unique() for elem in filter)):
                    df = df[df[item].isin(filter)]
                    
            #Ensure data is annualized
            df.REF_DATE = df.REF_DATE.apply(lambda x: re.search(r'(\d{4})',str(x))[0])
        
            df = df[columns]
            df = df.groupby(columns[:-1],as_index=False)['VALUE'].sum()

            df['Source'] = source[0]
            
            custom = True
        
        df.columns = new_cols[0].split(',')

        '''
        format the data file, then send it to custom_codes.make(df,group_list)
        '''
        provinces = ['Canada','Newfoundland and Labrador','Prince Edward Island','Nova Scotia','New Brunswick','Quebec','Ontario','Manitoba','Saskatchewan','Alberta','British Columbia']
        atlantic_prov = ['Newfoundland and Labrador','Prince Edward Island','Nova Scotia','New Brunswick']
        
        #2-step filter
        df = df[df['Province'].isin(provinces)]

        atlantic = df.query('Province in @atlantic_prov')
        df = df.query('Province not in @atlantic_prov')
        
        if atlantic_special:
            atlantic = atlantic.groupby(list(atlantic)[1:4]+list(atlantic)[5:],as_index=False)['Value'].sum()
            
        elif (file[:-4] == '14100204'):
            atlantic = atlantic[atlantic.Value > 0]
            atlantic = atlantic.groupby(list(atlantic)[1:-2],as_index=False)['Value'].mean()
            
        elif (file[:-4] == '17100005'):
            atlantic = atlantic.groupby(list(atlantic)[0:2],as_index=False)['Population'].sum()
            
        else:
            atlantic = atlantic.groupby(list(atlantic)[1:-2],as_index=False)['Value'].sum()
         
        atlantic['Province'] = 'Atlantic'
        atlantic['Source'] = source[0]
        
        df = df.append(atlantic)
        df = df[new_cols[0].split(',')]
        
        #Sometimes NAICS are comma delimited; this breaks them into separate lines
        try:
            df = df[df.Value > 0]
            df.dropna(subset=['NAICS'],inplace=True)
        
            df.NAICS = df.NAICS.apply(lambda x: x.split(','))
            df['temp_len'] = df.NAICS.apply(lambda x: len(x))

            duplicate = pd.DataFrame()
            
            naics_len_max = df['temp_len'].max()
            
            for i in range(1,naics_len_max+1):
                tmp = df[df['temp_len'] == i]
                for j in range(0,i):
                    tmp_b = tmp.copy(deep=True)
                    tmp_b.NAICS = tmp_b.NAICS.apply(lambda x: x[j])
                    duplicate =duplicate.append(tmp_b)
        
            duplicate = duplicate.drop('temp_len',axis=1)
            
            del(tmp_b)
            del(df)
            
        except:
            duplicate = df.copy(deep=True)
            del(df)

        return (duplicate,custom)

class custom_codes:

    def make_dict():
        #make a dictionary from the Concordance
        CONCORDANCE_FILE = 'Concordance.csv'
        concordance = pd.read_csv(CONCORDANCE_FILE)
        groups = concordance['Group Code'].unique().tolist()
        group_list = {}
        
        for g in groups:
            temp = concordance[concordance['Group Code'] == g]
            temp = temp['NAICS']
            group_list[g] = temp.unique().tolist()
        
        return (group_list)

    def depth_groupby(dataset,header,i,file):
        #return a summed dataframe
        
        sum = 0
        
        special = []

        '''build a graph thingy'''
        for naics in i:
        
            if '-' in naics:
                split_naics = naics.split('-')
                for n in range(int(split_naics[0]),int(split_naics[1])+1):
                    i.append(str(n))
                    
            special = list(filter(lambda x: str(x).startswith(str(naics)), dataset['NAICS'])) #returns list of all rows in the dataset that match the a naics in the special code
        
        graph = {}
        unique = list(set(special))
        
        for u in unique:
            graph[len(str(u))] = []
        
        for u in unique:
        
            try:
                graph[len(str(u))].append(int(u)) #we only want real NAICS codes, none of them custom numbered garbage.
                
            except:
                if u == '44-45' or u == '31-33' or u == '48-49':
                    graph[2] = []
                    graph[2].append(u)
                else:
                    pass
        '''done building graph thingy'''
        
        #looks like they all need to be strings
        for code in graph:
            for c in range(len(graph[code])):
                graph[code][c] = str(graph[code][c])
        
        graph = dict([(k,v) for k,v in graph.items() if len(v)>0])
        keys = sorted(list(graph.keys()))

        '''next is the improvement part; summing with a 'groupby' and returning a dataframe'''
        temp = dataset[dataset['NAICS'].isin(graph[min(keys)])]
        
        temp = temp.groupby(list(set(header) - set(['Industry','NAICS','Value'])),as_index=False)['Value'].sum()
        
        return (temp)

    def depth_search(dataset,header,naics,province,year,i,e,file):

        sum = 0
        
        special = []

        '''let's make a graph!'''
        for naics in i:
        
            if '-' in naics:
                split_naics = naics.split('-')
                for n in range(int(split_naics[0]),int(split_naics[1])+1):
                    i.append(str(n))

            special = list(filter(lambda x: str(x).startswith(str(naics)), dataset['NAICS'])) #returns list of all rows in the dataset that match the a naics in the special code
        
        graph = {}
        unique = list(set(special))
        
        for u in unique:
            graph[len(str(u))] = []
        
        
        for u in unique:
        
            try:
                graph[len(str(u))].append(int(u)) #we only want real NAICS codes, none of them custom numbered garbage.
                
            except:
                if u == '44-45' or u == '31-33' or u == '48-49':
                    graph[2] = []
                    graph[2].append(u)
                else:
                    pass
        '''the graph structure is now complete (above)'''

        #looks like they all need to be strings
        for code in graph:
            for c in range(len(graph[code])):
                graph[code][c] = str(graph[code][c])
        
        graph = dict([(k,v) for k,v in graph.items() if len(v)>0])
        keys = sorted(list(graph.keys()))
        
        '''
        is there a faster way? groupby k?
        '''

        try:
        
            #grab a subset of the data that we need
            temp = dataset[dataset['NAICS'].isin(graph[min(keys)])]

            #i think this breaks when it finally has a match.
            if len(header) < 7:
                if file[:-8] == '14100204':#wages
                    sum += temp[(temp['Province'] == province)&(temp['Year'] == year)].Value.mean()
                else:
                    sum += temp[(temp['Province'] == province)&(temp['Year'] == year)].Value.sum()
                    
                return(sum)
            else:
                sum += temp[(temp['Province'] == province)&(temp['Year'] == year)&(temp[temp.columns[len(header)-1]] == e)].Value.sum()
                return(sum)                
            
        except:
            return(0)
        
    def make(dataset,group_list,f):
        
        CONCORDANCE_FILE = 'Concordance.csv'
        concordance = pd.read_csv(CONCORDANCE_FILE)

        header = list(dataset)
        
        dataset_naics = dataset['NAICS'].unique().tolist()
        provinces = dataset['Province'].unique().tolist()
        years = dataset['Year'].unique().tolist()

        if len(header)>6:
            extra = dataset.iloc[:,-1].unique().tolist() #get custom last column, if exists
        else:
            extra = ['']
        
        holder = [] #to hold the values for special code
        dummy = []
        container = pd.DataFrame(columns=header)
        
        print ('Working on ',f)

        for i in group_list:
            if i not in dataset_naics:
            
                industry = concordance.loc[concordance['Group Code'] == i, 'Industry']
                industry = industry.unique()[0]
            
                try:
                    df = custom_codes.depth_groupby(dataset,header,group_list[i],file)
                    df['NAICS'] = i
                    df['Industry'] = industry
                    
                except:
                    df = pd.DataFrame(columns=header)
        
                container = container.append(df)
                container = container[header]
        
        result = dataset.append(container)
        result.drop_duplicates(inplace=True)
        result.dropna(axis=0, how='all')
        result = result[result.Value > 0]
        result.to_excel(processed+f+'.xlsx',index=False)
        
        '''
        for province in provinces:
            for year in years:
                for i in group_list: #cycle through special codes
     
                    for e in extra:
                    
                        sum = 0 #the reset
                        t = 0
                        
                        for naics in group_list[i]: #cycle through naics in each special code to find/sum child NAICS values in the data
                            
                            #if "i" is already in the dataset, don't do this aggregation again.
                            if i not in dataset_naics:

                                t += 1
                                
                                industry = concordance.loc[concordance['Group Code'] == i, 'Industry']
                                industry = industry.unique()[0]
                                
                                source = dataset.loc[dataset['Province'] == province,'Source'].unique()[0]
                                
                                subset = dataset[(dataset['Province'] == province) & (dataset['Year'] == year)]
  
                                try:
                                    sum += custom_codes.depth_search(subset,header,naics,province,year,i,e,f) #le graph search

                                except:
                                    sum+=0
                                
                                print (e[:15],naics,sum, end='\r')
                                
                                if len(header) > 7:
                                    dummy = [province,industry,i,year,sum,source,'',e]
                                elif len(header) == 7:
                                    dummy = [province,industry,i,year,sum,source,e]
                                else:
                                    dummy = [province,industry,i,year,sum,source]
                                

                        #We have to average wages
                        if (f[:-8] == '14100204') and (t > 0):
                            dummy[4] /= t

                        print(sum,'\t\tAppending...',province,year,i,end='\t\t\t\r')
                        holder.append(dummy) #add to new dataframe after going through entire naics code list
                        
                        ''''''
                        if i == 'T003':
                            print ('\n',sum/9236938,group_list[i])
                            input()
                        ''''''
                      
            print ('Saving data...',end='\t\t\t\r')

            holder_df = pd.DataFrame(holder, columns=header)

            result = pd.concat([dataset,holder_df])
            result.drop_duplicates(inplace=True)
            result.dropna(axis=0, how='all')
            result = result[result.Value > 0]
            result.to_excel(processed+f+'.xlsx',index=False)
            '''

class files:

    def join():

        #GDP = 36100402 + 36100434
        a = pd.read_excel(processed+'36100402.xlsx')
        b = pd.read_excel(processed+'36100434.xlsx')
        a = a.append(b, ignore_index=True)
        a.to_excel(processed+'GDP.xlsx',index=False)
        
        #Revenue = AER Table 1.7 data [http://www1.aer.ca/st98/tables/capital_expenditure/table_1_7.html] + 16100048
        #    --> manually update the table; script will join them
        a = pd.read_excel(processed+'16100048.xlsx')
        b = pd.read_excel('Oil and Gas Revenue.xlsx')
        a = a.append(b)
        a.to_excel(processed+'Revenue.xlsx',index=False)
        
        #Employment = 36100489(SNA) + 14100202(SEPH) -- I think SNA is the base, filled in with SEPH
        a = pd.read_excel(processed+'36100489.xlsx')
        b = pd.read_excel(processed+'14100202.xlsx')
        
        b = b[~b['NAICS'].isin(a['NAICS'].unique())]
        
        a = a.append(b)
        a.to_excel(processed+'Employment.xlsx',index=False)
        
        #Population
        a = pd.read_excel(processed+'17100005.xlsx')
        a.to_excel(processed+'Population.xlsx',index=False)
        
        #Wages
        a = pd.read_excel(processed+'14100204.xlsx')
        a.to_excel(processed+'Wages.xlsx',index=False)
        
        del(a)
        del(b)


if __name__ == '__main__':

    source = os.getcwd()+'\Source Data\\'
    processed = os.getcwd()+'\Processed\\'
    
    group_list = custom_codes.make_dict()
    
    if not os.path.exists(source):
        os.makedirs(source)
        
    if not os.path.exists(processed):
        os.makedirs(processed)

    if len(sys.argv)>1:
        file = sys.argv[1]
        
        if file != 'join':

            starttime = time.time()
            
            data.download(file,source)
            df,custom = data.clean(source,file)
            
            if custom:
                custom_codes.make(df,group_list,file)
            else:
                df.to_excel(processed+file+'.xlsx',index=False)
                
            print('Processed in {} minutes\t\t\t\t'.format(round((time.time() - starttime)/60,2)))
            
        else:
            files.join()

    else:
    
        sources = pd.read_csv('data_table.csv',encoding='latin1')['table_id'].to_list()

        for file in sources:
        
            starttime = time.time()
            
            print (file)
     
            data.download(file,source)
            df, custom = data.clean(source,file)

            if custom:
                custom_codes.make(df,group_list,file)
            else:
                df.to_excel(processed+file+'.xlsx',index=False)
            
            print('Processed in {} minutes\t\t\t\t'.format(round((time.time() - starttime)/60,2)))
        
        files.join()