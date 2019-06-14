#!/usr/bin/python

'''
On the first of every month:
* run DrillingWells.py, EnergyPrices.py, EnergyProduction.py
    * integrate with the email system
'''

#this should run everyday
import pandas as pd
import numpy as np
import requests
import urllib
import datetime
import zipfile
import smtplib
import yagmail
import linecache
import time
import sys
import os
import re

#set working directory >:(
if 'nt' in os.name: #Windows
    os.chdir('c:\_localData\python\ed processing\\') #change to this directory
    path = 'c:\_localData\python\ed processing\\'
    
else: #Linux
    os.chdir('/var/www/html/update_schedule/') #change to this directory
    path = '/var/www/html/update_schedule/'


today = datetime.datetime.today().strftime('%Y-%m-%d')

std_url = 'https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid='
url = 'https://www150.statcan.gc.ca/t1/wds/rest/getChangedCubeList/'+today #'https://www150.statcan.gc.ca/t1/wds/rest/getChangedSeriesList'
dl_url = 'https://www150.statcan.gc.ca/n1/tbl/csv/{}-eng.zip'


class table:
    @staticmethod
    def parse(schedule):

        watch_list = []

        for url in schedule['Source']:
            
            if url.find('pid=') > 0:
                result = re.findall(r'((\d+){5})',str(url))
                watch_list.append(result[0][0])
        
        og_list = watch_list
        watch_list = [item[:-2] for item in watch_list]
        
        return (watch_list,og_list)
        
    @staticmethod    
    def process(table_id):

        provinces = ['Alberta','British Columbia','Canada','Manitoba','New Brunswick','Newfoundland and Labrador','Nova Scotia','Ontario','Prince Edward Island','Quebec','Saskatchewan']
        
        #initial filter of dataset -> geographic
        df = pd.read_csv(path+table_id+'.csv',low_memory=False,encoding='utf-8-sig')
        
        #found this abomination in the motor vehicle data
        if table_id == '20100001':
            df['GEO'].loc[df['GEO'] == 'British Columbia and the Territories'] = 'British Columbia'
        
        df = df[df['GEO'].isin(provinces)]

        #get our filters and such
        table_details = pd.read_csv(path+'table_details.csv')
        table_details = table_details[table_details['table_id'] == int(table_id+'01')] #pray they don't ever add anything other than 01 to tables
        
        #if there are data issues, we want to abort and notify the team
        no_problems = True
        
        for tile in table_details['tile'].unique():
        
            if no_problems:
                
                #we want a fresh copy to work with for each tile
                tile_df = df.copy(deep=True)
                
                #get indicators for this tile
                indicators = table_details[table_details['tile'] == tile]
                
                #pivot out the provincial data
                pivot_cols = indicators['data_column'].apply(lambda x: int(x) if type(x) != str else 0) #can't convert float NaN to integer?
                pivot_cols = list(df)[min(pivot_cols):max(pivot_cols)+1]
                
                if 'REF_DATE' not in pivot_cols:
                    pivot_cols.insert(0,'REF_DATE')

                #get a slice of the indicators for this tile
                indicators_slice = indicators[['data_column','col_name']].drop_duplicates()
        
                #dict of col index to new name
                new_names = {}
                
                #iteratively apply filters to the data, column by column
                try:
                    for name in indicators['col_name'].unique():
                        
                        #1. list of indicators for each col_name/col_num
                        indicator_filter = indicators[indicators['col_name'] == name]
                        
                        #2. find the column number in the data by using the first indicator
                        for column in list(tile_df):

                            if (indicator_filter['indicators'][0:1].values in list(tile_df[column])):
                                col_num = tile_df.columns.get_loc(column)
                                
                            if (('Unadjusted' in list(tile_df[column])) or ('Seasonally adjusted' in list(tile_df[column]))):
                                adjust_col = tile_df.columns.get_loc(column)
                        
                        try:
                            new_names[tile_df.columns[col_num]] = name
                        except:
                            print ('Indicators may have changed. Cross-reference data table with table_details.csv')
                            no_problems = False
                            pass
            
                        try:
                            new_names[tile_df.columns[adjust_col]] = 'DataType'
                        except:
                            pass
                        
                        #3. check if our list of indicators are in the file, if not, send an alert
                        not_found = list(set(list(indicator_filter['indicators'])) - set(tile_df.iloc[:,int(col_num)].unique()))
                        
                        if len(not_found) > 0:
                            print ('Unmatched Indicators in column ['+str(col_num)+']: ',not_found)
                            no_problems = False
                        
                        #variables on the table: indicators ('indicators' column from table_details.csv, column number, tile dataframe)
                        #4a. THE SNAP - apply the filters to the tile_df; done iteratively col by col; also do adjusted/unadjusted?
                        tile_df = tile_df[tile_df[tile_df.columns[col_num]].isin(indicator_filter['indicators'])]
                        
                        #4b. THE ECHO SNAP; filter for possible adjustments; but only if 'data_type' has adjustments
                        adjustment_list = list(indicator_filter['data_type'])
                        
                        if (('Unadjusted' in adjustment_list) or ('Seasonally adjusted' in adjustment_list)):
                            
                            #process unadjusted
                            unadjusted_indicators = indicator_filter[indicator_filter['data_type'] == 'Unadjusted']
                            unadjusted_group = tile_df[(tile_df[tile_df.columns[adjust_col]].isin(['Unadjusted']))]# and (tile_df[tile_df[tile_df.columms[col_num]].isin(unadjusted_indicators['indicators'])])]
                            unadjusted_group = unadjusted_group[unadjusted_group[unadjusted_group.columns[col_num]].isin(unadjusted_indicators['indicators'])]
                            
                            #process unadjusted
                            adjusted_indicators = indicator_filter[indicator_filter['data_type'] == 'Seasonally adjusted']
                            adjusted_group = tile_df[(tile_df[tile_df.columns[adjust_col]].isin(['Seasonally adjusted']))]# and (tile_df[tile_df[tile_df.columms[col_num]].isin(adjusted_indicators['indicators'])])]
                            adjusted_group = adjusted_group[adjusted_group[adjusted_group.columns[col_num]].isin(adjusted_indicators['indicators'])]
                            
                            if ('Seasonally adjusted' not in adjustment_list):
                                tile_df = unadjusted_group
                            
                            else:
                                tile_df = unadjusted_group.append(adjusted_group)
                        
                        #4c. ECHO ECHO ECHO ... sometimes a specific unit is required
                        measure = list(indicator_filter['uom'])
                        
                        if (measure[0] == measure[0]):
                            tile_df = tile_df[tile_df['UOM'].isin(measure)]
                            
                except Exception as e:
                    #for datasets like population that are sparse
                    exc_type, exc_obj, tb = sys.exc_info()
                    lineno = tb.tb_lineno
                    
                    print ('Line',lineno,'exception. Processed in a special way.')

                
                #scalar needs to also be able to apply to specific indicators
                tile_df['VALUE'] = tile_df['VALUE']*(10**tile_df['SCALAR_ID'])
                print (list(tile_df))
                tile_df.rename(index=str,columns={'"REF_DATE"':'REF_DATE'},inplace=True)
                
                #date format check:
                if len(str(tile_df.iloc[0]['REF_DATE'])) == 4:
                    tile_df['REF_DATE'] = pd.to_datetime(tile_df['REF_DATE'],format='%Y')
                    
                elif len(str(tile_df.iloc[0]['REF_DATE'])) == 7:
                    tile_df['REF_DATE'] = pd.to_datetime(tile_df['REF_DATE'],format='%Y-%m',errors='coerce')
                    
                else:
                    tile_df['REF_DATE'] = pd.to_datetime(tile_df['REF_DATE'],format='%Y/%m/%d')
                    
                tile_df = tile_df.drop(['DGUID','UOM_ID','UOM','SCALAR_FACTOR','SCALAR_ID','VECTOR','COORDINATE','STATUS','SYMBOL','TERMINATED','DECIMALS'],axis=1)

                tile_df = tile_df.pivot_table(values='VALUE', index=pivot_cols, columns='GEO').reset_index()
                
                #rename cols to what ED expects (136:140)
                tile_df.columns.values[0] = 'When'

                for name in new_names:
                    tile_df = tile_df.rename(columns={name:new_names[name]})

                #check if each province has a column. Add a blank column if necessary.
                #this method is kind of sketchy b/c it relies on Alberta being in every dataset.
                for province in provinces:
                    if province not in list(tile_df):
                        ab_col = [i for i,x in enumerate(list(tile_df)) if x == 'Alberta']
                        col =  [i for i,x in enumerate(provinces) if x == province][0] + ab_col[0] - 1
                        
                        if (table_id == '16100048') & (province == 'Canada'):
                            tile_df.insert(col+1,province,'')
                        else:
                            tile_df.insert(col,province,'')

                '''discrete adjustments'''
                #investments
                if table_id == '34100035':
                    this_year = datetime.datetime.today()
                    print (this_year.year)
                    tile_df = tile_df[tile_df['When'].apply(lambda x: x.year < this_year.year)]
                    
                #avg weekly earnings
                if table_id == '14100203':
                    tile_df.drop(['Employees','Overtime'],axis=1,inplace=True)
                    
                #building permits
                if table_id == '34100066':
                    tile_df.insert(2,'Area','All areas')
                
                #wholesale trade
                if table_id == '20100074':
                    tile_df.insert(1,'DataType','Seasonally adjusted')
                    tile_df = tile_df.fillna(0)
                
                #motor vehicle sales
                if table_id == '20100001':
                    tile_df.insert(1,'Data Type','Unadjusted')
                    tile_df = tile_df.fillna(0)
                
                #unemployment rates
                if tile == 'UnemploymentRates':
                    tile_df.drop(['Characteristic'],axis=1,inplace=True)
                
                #merchandise exports
                if tile == 'MerchandiseExports':
                    tile_df.drop(['Partner'],axis=1,inplace=True)
                
                #employment
                if tile == 'Employment':
                    tile_df.replace({'Full-time employment':'Employment full-time','Part-time employment ':'Employment part-time'},inplace=True)
                    
                #grain and livestock prices
                if table_id == '32100077':
                    tile_df.replace({
                        'Wheat (except durum wheat)':'Wheat excluding durum',
                        'Durum wheat':'Durum',
                        'Cattle for slaughter':'Slaughter, cattle',
                        'Calves for slaughter':'Slaughter, calves'
                    },inplace=True)
                
                #farm cash reciepts
                if table_id == '32100046':
                    tile_df.replace({
                        'Total crop receipts':'Total crops receipts',
                        'Total farm cash receipts':'Total farm cash receipts',
                        'Total receipts from direct payments':'Total receipts from direct payments',
                        'Total livestock and livestock product receipts':'Total receipts from livestock and livestock products'
                    },inplace=True)
                
                #housing starts
                #fill NA with zeros to maximize compatibility
                if table_id == '34100143':
                    tile_df = tile_df.fillna(0)
                
                file_name = tile+'_'+table_id+'.csv'
                
                today = datetime.datetime.now()
                mtime = datetime.datetime.fromtimestamp(os.path.getmtime(path+'/processed/'+file_name))

                if ((today-mtime).days > 15):
                    tile_df.to_csv(path+'/processed/'+file_name,index=False,line_terminator='\r\n') #causes extra lines in Windows
                else:
                    no_problems = False
                
                #merge gdp tables
                if ((tile == 'GrossDomesticProduct') or (tile == 'GrossDomesticProductMarket')):

                    no_problems = True
                    print (path)
                    if (os.path.isfile(path+'/processed/GrossDomesticProduct_36100402.csv')) and (os.path.isfile(path+'/processed/GrossDomesticProductMarket_36100222.csv')):#error here
                    
                        print('Merging GDP files')
                        
                        try:
                            gdp_bsc = pd.read_csv(path+'/processed/GrossDomesticProduct_36100402.csv')#GDP
                            gdp_mkt = pd.read_csv(path+'/processed/GrossDomesticProductMarket_36100222.csv')#GDP Market
                            
                            #gdp_a -> add 'Type':'Gross domestic product at basic prices'
                            gdp_bsc['Type'] = 'Gross domestic product at basic prices'
                            
                            #gdp_b -> add 'Industries':'Total gross domestic product'
                            gdp_mkt['Industries'] = 'Total gross domestic product'
                            gdp_mkt['Type'] = 'Gross domestic product at market prices'
                            
                            #drop 'Price' column
                            gdp_mkt.drop(['Prices'],axis=1,inplace=True)
                            
                            gdp = gdp_bsc.append(gdp_mkt)
                            
                            gdp = gdp[['When','Industries','Type']+provinces]
                            
                            gdp.drop_duplicates(inplace=True)
                            
                            file_name = 'GrossDomesticProduct_3610022201_3610040201.csv'
                            
                            gdp.to_csv(path+'/processed/'+file_name,index=False,line_terminator='\n')
                            
                            table_id = '36100222'
                            
                        except:
                            exc_type, exc_obj, tb = sys.exc_info()
                            lineno = tb.tb_lineno
                            print ('Error:',lineno,exc_obj,exc_type)
                            
                        #we only want an email to go out for the merged file
                        no_problems = False 
                        
                #calculate net migration figures
                #for some reason the script isn't downloading both 17100040 and 17100020 properly.
                if tile == 'NetMigration':
                
                    types = tile_df[tile_df.columns[1]].unique()
                    col_name = list(tile_df)[1]
                    print (types)
                    for i in range (0,len(types)):
                        types[i] = tile_df[tile_df[col_name] == types[i]]
                        types[i].drop(types[i].columns[1], axis=1, inplace=True)
                        
                    if table_id == '17100040':
                        #immigrants - emigrants + net non-permanent residents + returning emigrants - net temporary residents
                        net = types[0].set_index(['When']) \
                            .sub(types[1].set_index(['When']), fill_value=0) \
                            .add(types[2].set_index(['When']), fill_value=0) \
                            .add(types[3].set_index(['When']), fill_value=0) \
                            .sub(types[4].set_index(['When']), fill_value=0).reset_index() \
                        
                    else:
                        net = types[0].set_index(['When']).sub(types[1].set_index(['When']), fill_value=0).reset_index()
                    
                    print ('\n',net['Alberta'][191:192],'\n')
                    
                    if table_id[-2:] == '20':
                        net.insert(1,col_name,'NetInterprovincialMigration')
                    else:
                        net.insert(1,col_name,'NetInternationalMigration')
                        
                    tile_df = tile_df.append(net)
                    
                    file_name = tile+'_'+table_id+'.csv'
                    
                    #write the changes
                    tile_df.to_csv(path+'/processed//'+file_name,index=False,line_terminator='\r\n')
                    
                    #lastly, check if there are two migration files that were created today, then merge them;
                    #create a net migration indicator by adding net international and net provincial
                    try:
                    
                        intl_mig = '17100040'
                        intp_mig = '17100020'
                    
                        first = os.path.getmtime(path+'/processed//'+tile+'_'+intl_mig+'.csv')
                        second = os.path.getmtime(path+'/processed//'+tile+'_'+intp_mig+'.csv')
                    
                        if abs(first-second) < 860000:
                            
                            first = pd.read_csv(path+'/processed//'+tile+'_'+intl_mig+'.csv')
                            second = pd.read_csv(path+'/processed//'+tile+'_'+intp_mig+'.csv')

                            sum = first[first[first.columns[1]].str.contains('Net')==True]
                            sum = sum.append(second[second[second.columns[1]].str.contains('Net')==True])
                            sum = sum[sum[sum.columns[1]].str.contains('Net ')==False]
                            
                            sum = sum.groupby(['When'])[provinces].sum().reset_index()
                            sum.insert(1,col_name,'NetMigration')
                            
                            first = first.append(second)
                            first = first.append(sum)
                            first = first[first[first.columns[1]].str.contains('Net')==True]
                            first = first[first[first.columns[1]].str.contains('Net ')==False]
                            
                            file_name = tile+'_'+intl_mig+'_'+intp_mig+'.csv'
                            first.to_csv(path+'/processed//'+file_name,index=False,line_terminator='\r\n')
                            
                            table_id = '17100020'
                            
                    except Exception as e:
                        print ('Error processing Net Migration \r', e)
                        no_problems = False
            
            if no_problems:
                email.send(table_id,tile,file_name,no_problems,not_found)
                continue
                
            else:
                f = open(path+'_log.txt','a')
                f.write('Something '+str(table_id)+' '+file_name+' '+tile+'\r\n')
                f.close()
           
    @staticmethod
    def download(pairs=[],schedule=[],table_id=''):

        if table_id == '':
        
            for url in pairs:
            
                table_id = url[:-2]
                
                if sys.version_info[0] < 3:
                    file = urllib.URLopener()
                
                else:
                    file = urllib.request.URLopener()
                    
                file.retrieve(dl_url.format(table_id),path+str(table_id)+'.zip')

                zip_name = str(table_id)+'.zip'
                
                zipfile.ZipFile(path+zip_name,'r').extract(table_id+'.csv')
                #this is not actually saving the csv file when running from a cronjob
                os.remove(path+str(table_id)+'.zip')
                
                print (table_id)
                
                table.process(table_id)

            #download the file
            #open it into pandas df, pass it off to process table

        else:
        
            try:
                file = urllib.request.URLopener()
                file.retrieve(dl_url.format(table_id),path+str(table_id)+'.zip')

                zip_name = str(table_id)+'.zip'
                
                zipfile.ZipFile(path+zip_name,'r').extract(table_id+'.csv')
                #this is not actually saving the csv file when running from a cronjob
                os.remove(path+str(table_id)+'.zip')
                
            except:
                pass

class email:

    @staticmethod
    def determine_sender(updater='-',reviewer='Anyone',tiles=''):
    
        emails = pd.read_csv(path+'email_list.csv',index_col=0,squeeze=True).to_dict()
        email_list = [emails[i] for i in emails]
        print (tiles,updater,reviewer)
        
        '''know whether to email everyone, or just someone'''
        if ((updater != '-') and ((tiles != 'error') or (tiles != ''))):
            send_to = [emails[updater.lower()],emails[reviewer.lower()]]
            
        else:
            updater = 'Team'
            reviewer = 'Anyone'
            send_to = email_list

        '''determine who to CC'''
        cc_to = list(set(email_list).difference(send_to))
        return (send_to,cc_to)
        
    @staticmethod        
    def send(table_id=0,tiles='error',file_name='example_1234567.csv',no_problems=True,not_found=[],contents='',subject=''):
        
        #separate module for online and manual updates; get online online entries, then process?

        try:
            row = schedule[schedule['Source'] == std_url+str(table_id)+'01']

        except Exception as e:
            row = []
            tiles == 'error'

        if (len(row) > 0):

            try:

                #indicator = row.iloc[0]['Indicator']
                #type = row.iloc[0]['File']
                source = row.iloc[0]['Source']
                updater = row.iloc[0]['Updater']
                reviewer = row.iloc[0]['Reviewer']

            except Exception as e:
                
                exc_type, exc_obj, exc_tb = sys.exc_info()
                tiles = 'error'
                updater = '-'
                print (e)
                  
            send_to,cc_to = email.determine_sender(updater=updater,reviewer=reviewer,tiles=tiles)
            
            '''is the data pulled from the API or done manually?'''
            if tiles != 'error':
                contents = ['Hi, <b>'+updater+'</b><p>An update is available for '+tiles+'.<p>'+ \
                    'Save the attached file to "M:\EDT Divisions\EDI\ENT\Comdrvs\Common\Economic Information & Statistics\Dashboard\Data Files for Dashboard" and upload it to the Economic Dashboard by visiting http://economicdashboard.alberta.ca/login .'+ \
                    '<p>Please complete the update no later than <b>9:00am today</b>.' + \
                    '<p><b>'+reviewer+'</b>, you are the reviewer/backup.', \
                    path+'/processed//'+file_name]
                    
                subject = tiles.capitalize()+' update available';
                    
            elif no_problems == False:
                contents = ['Hi, '+updater+ \
                    '<p>There is a data indicator mismatch for '+tiles+'. The following indicators do not match previous data, and may have been changed:<p>'+ \
                    not_found + \
                    '<p>This may prevent the dataset from uploading properly. Please work with the developers to resolve the issue.']
                
                subject = tiles.capitalize()+' data mismatch'
        
        else:

            if tiles == 'error':
                contents = ['There was an error with table {}. <p>Please check <a href="https://www150.statcan.gc.ca/n1/dai-quo/index-eng.htm?HPA=1">The Daily</a> for updates.'.format(table_id)]
                subject = 'StatCan connection error'
                send_to = ['kyle.lillie@gov.ab.ca']
                cc_to = ['imap.projects@gmail.com']
            
            if tiles == 'manifest':
                contents = ['Please find today\'s StatCan table update manifest attached to this email.',file_name]
                subject = 'Daily StatCan update manifest'
                send_to = ['kyle.lillie@gov.ab.ca']
                cc_to = ['imap.projects@gmail.com']
                
            if tiles == 'weekly':
                send_to,cc_to = email.determine_sender(tiles=tiles)
                  
        alias = 'Economic Dashboard Updates'
        yag = yagmail.SMTP({'imap.projects@gmail.com':alias}, 'Epsilon200')
        yag.send(to=send_to, cc=cc_to,subject=subject, contents=contents)
            
        time.sleep(10)

class query:
    
    @staticmethod
    def weekly_updates(schedule=pd.DataFrame()):

            '''Get the update schedule'''
            ki_schedule = 'https://www150.statcan.gc.ca/n1/dai-quo/ssi/homepage/schedule-key_indicators-eng.json'
            response = requests.get(ki_schedule)
            df = pd.DataFrame(response.json())
            df['date'] = pd.to_datetime(df['date'],yearfirst=True)
            
            '''Get today so we can filter the schedule for this week'''
            today = datetime.datetime.now()
            yesterday = today - datetime.timedelta(days=1)
            weekend = today + datetime.timedelta(days=5)
            
            df = df[(df['date'] > yesterday) & (df['date'] < weekend)]
            
            content = 'StatCan data below are expected to be updated this week:\n\n'
            
            for row in range(0,len(df)):
                
                date = datetime.datetime.strftime(df.iloc[row]['date'],'%A, %B %d')
                
                if (row == 0):
                    content += '<b>'+date+'</b>\n'
                    
                if (row > 0):
                    if (datetime.datetime.strftime(df.iloc[row-1]['date'],'%A, %B %d') != date):
                        content += '\n<b>'+date+'</b>\n'
                
                content += df.iloc[row]['title']+' '+'('+df.iloc[row]['description']+')'
                content += '\n'
                
            email.send(tiles='weekly',contents=content,subject='Weekly Update Preview')
    
    @staticmethod
    def statcan(schedule):
        
        try:
            response = requests.get(url)
            
            df = pd.DataFrame(response.json()['object'])
            
            df.to_csv(path+'updates.csv',line_terminator='\r\n')
            
            df = df['productId'].unique().tolist()
            
            df = [str(item) for item in df]
        
            watch_list,og_list = table.parse(schedule)
        
            matches = list(set(watch_list).intersection(df))

            pairs = zip(watch_list,og_list)
            pairs = [couple[1] for couple in pairs for match in matches if couple[0] == match]
            
            table.download(pairs,schedule)
            
        except Exception as e:
            #Failure Collection Point
            print ('Data processing failed: ',e)
            
            #send_emails() #no variables == error message
        '''
        What to do for other datasets??
        '''

if __name__ == '__main__':

    day = datetime.datetime.now().strftime('%A')
    num_day = int(datetime.datetime.now().strftime('%d'))
    
    if day == 'Sunday' or day == 'Saturday':
        weekend = True
    else:
        weekend = False
    
    if day == 'Monday':
        query.weekly_updates()
        
        if num_day < 8:
            print (num_day)
            import EnergyProduction
            import DrillingWells
            #import EnergyPrices #-->need to install tika,ensure it runs on Linux
        
    pd.options.mode.chained_assignment = None  # default='warn'
    schedule = pd.read_csv(path+'Schedule.csv')

    print (sys.argv)
    
    if len(sys.argv)>1:
        custom = sys.argv[1]
        table.download(table_id=sys.argv[1])
        table.process(sys.argv[1])

    else:
        if not weekend:        
            query.statcan(schedule)
            