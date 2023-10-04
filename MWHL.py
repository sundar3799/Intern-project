from logging import getLogger
from base import BaseTemplate, BaseFix, BaseDocxFix
from custom_exceptions import InputValidationError
import pandas as pd
from collections import defaultdict
import re
import numpy as np
from numpy import nan
import warnings
from bs4 import BeautifulSoup4
from html2text import html2text
import io
from dateutil.parser import parse
from pandas import read_html
import datetime

warnings.simplefilter(action='ignore', category=FutureWarning)

log = getLogger(__name__)
from logging import getLogger
from base import BaseTemplate, BaseDocxFix
import pandas as pd
import re
import warnings
from dateutil.parser import parse
from pandas import read_html
import datetime

warnings.simplefilter(action='ignore', category=FutureWarning)

log = getLogger(__name__)


class Expedoc_WAN_HAI_Word(BaseTemplate):

    class WAN_HAI_HKG(BaseDocxFix):

        def check_input(self):

            pass

        def check_output(self):

            pass

        def get_headers(self):

            regex = r"<p><strong>Service Contract No.: (.+?)<\/strong><\/p>"
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    contract_id = match.group(groupNum)

            regex = r'<p><strong>Amendment No.: (.+?)	;Effective Date:'
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    amd_no = match.group(groupNum)

            regex = r'</strong>COMMODITY:</p><p>(.+?)</p>'
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    commodity = match.group(groupNum)

            contract_dates = []
            regex = r'Contract shall become effective on (.+?) and expires on (.+?)\('
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    contract_dates.append(parse(match.group(groupNum)))

            return contract_id, amd_no, commodity, contract_dates

        def get_dates(self, text):

            dates = {}

            regex = r"<p>Above rates are effective from (.+?)</p>"
            matches = re.finditer(regex, text, re.MULTILINE)
            key = 0
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group = match.group(groupNum)
                    dates[key] = {'start_date': parse(group.split(' to ')[0].strip())
                        , 'expiry_date': parse(group.split(' to ')[-1].strip())
                                  }
                    key += 1
            return dates

        def get_initial_df(self):

            regex = r"As following inserts.</p>(.+?)<p><strong>SPECIAL RATE"
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group = match.group(groupNum)

            initial_tables = read_html(group)
            dates = self.get_dates(group)
            if len(initial_tables) == len(dates):
                for i in range(len(initial_tables)):
                    initial_tables[i]['start_date'] = dates[i]['start_date']
                    initial_tables[i]['expiry_date'] = dates[i]['expiry_date']

                    initial_tables[i]['start_date'].iloc[0] = 'start_date'
                    initial_tables[i]['expiry_date'].iloc[0] = 'expiry_date'

                    initial_tables[i].columns = initial_tables[i].iloc[0, :]
                    initial_tables[i] = initial_tables[i].iloc[1:, :]

            initial_df = pd.concat(initial_tables, ignore_index=True)

            return initial_df

        def get_special_rate_df(self):

            regex = r"SPECIAL RATE</strong></p>(.+?)<p><strong>ACCOUNT BULLET RATES"
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group = match.group(groupNum)

            special_rate_tables = read_html(group)
            dates = self.get_dates(group)
            if len(special_rate_tables) == len(dates):
                for i in range(len(special_rate_tables)):
                    special_rate_tables[i]['start_date'] = dates[i]['start_date']
                    special_rate_tables[i]['expiry_date'] = dates[i]['expiry_date']
                    special_rate_tables[i]['bulletin'] = 'special rate'

                    special_rate_tables[i]['start_date'].iloc[0] = 'start_date'
                    special_rate_tables[i]['expiry_date'].iloc[0] = 'expiry_date'
                    special_rate_tables[i]['bulletin'].iloc[0] = 'bulletin'

                    special_rate_tables[i].columns = special_rate_tables[i].iloc[0, :]
                    special_rate_tables[i] = special_rate_tables[i].iloc[1:, :]

            special_rate_df = pd.concat(special_rate_tables, ignore_index=True)

            return special_rate_df

        def get_bullet_rate_df(self):

            regex = r"ACCOUNT BULLET RATES</strong></p>(.+?)<p>Remarks:"
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group = match.group(groupNum)

            if '<s>' in group:
                regex = r"<s>(.+?)<\/s>"
                subst = ""
                group = re.sub(regex, subst, group, 0, re.MULTILINE)

            bullet_tables = read_html(group)
            dates = self.get_dates(group)

            for i in range(len(bullet_tables)):
                bullet_tables[i].dropna(how='all', axis=0, inplace=True)

            bullet_rate_tables = []
            for i in range(len(bullet_tables)):
                if len(bullet_tables[i]) > 1:
                    bullet_rate_tables.append(bullet_tables[i])

            account_names = []
            regex = r"<p>Account name: (.+?)</p><table>"
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group = match.group(groupNum)
                    account_names.append(group)

            if len(bullet_rate_tables) == len(dates) and len(bullet_rate_tables) == len(account_names):
                for i in range(len(bullet_rate_tables)):
                    bullet_rate_tables[i]['start_date'] = dates[i]['start_date']
                    bullet_rate_tables[i]['expiry_date'] = dates[i]['expiry_date']
                    bullet_rate_tables[i]['bulletin'] = 'account bullet rates'
                    bullet_rate_tables[i]['named_account'] = account_names[i]

                    bullet_rate_tables[i]['start_date'].iloc[0] = 'start_date'
                    bullet_rate_tables[i]['expiry_date'].iloc[0] = 'expiry_date'
                    bullet_rate_tables[i]['bulletin'].iloc[0] = 'bulletin'
                    bullet_rate_tables[i]['named_account'].iloc[0] = 'named_account'

                    bullet_rate_tables[i].columns = bullet_rate_tables[i].iloc[0, :]
                    bullet_rate_tables[i] = bullet_rate_tables[i].iloc[1:, :]

            bullet_rate_df = pd.concat(bullet_rate_tables)

            return bullet_rate_df

        def get_subcharges(self):

            regex = r"<p>From (.+?) \(amd"
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            df_list = []
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group = match.group(groupNum)
                    group_list = group.split(', BC will be at USD')
                    amounts = group_list[-1].split(' per ')[0].split('/')
                    cols = ['20GP', '40GP', '40HC', '45HC']
                    # cols = group_list[-1].split(' per ')[-1].split('/')
                    df = pd.DataFrame(columns=cols)
                    df = df.append(pd.Series(amounts, index=df.columns), ignore_index=True)
                    if 'July1' in group_list[0].split(' to ')[0]:
                        df['start_date'] = parse(group_list[0].split(' to ')[0].replace('July1', 'July'))
                    elif 'Julyl' in group_list[0].split(' to ')[0]:
                        df['start_date'] = parse(group_list[0].split(' to ')[0].replace('Julyl', 'July'))
                    else:
                        df['start_date'] = parse(group_list[0].split(' to ')[0])
                    if 'July1' in group_list[0].split(' to ')[-1]:
                        df['expiry_date'] = parse(group_list[0].split(' to ')[-1].replace('July1', 'July'))
                    if 'Julyl' in group_list[0].split(' to ')[-1]:
                        df['expiry_date'] = parse(group_list[0].split(' to ')[-1].replace('Julyl', 'July'))
                    else:
                        df['expiry_date'] = parse(group_list[0].split(' to ')[-1])

                    df_list.append(df)

            return df_list

        def melt_load_type(self, df):
            if "load_type" not in df:
                df = df.melt(id_vars=[column for column in df.columns if not column[1].isdigit()],
                             var_name="load_type",
                             value_name="amount")
                return df

        def populate_surcharge(self, subcharges, subcharge_df):

            if 'zone' not in subcharge_df:
                subcharge_df = subcharge_df[
                    ['destination_port', 'Srv Code', 'service_type', 'origin_port', 'commodity', 'remarks', 'load_type',
                     'charges']]
            else:
                subcharge_df = subcharge_df[
                    ['destination_port', 'Srv Code', 'service_type', 'origin_port', 'commodity', 'remarks', 'load_type',
                     'zone', 'charges']]

            subcharge_df.reset_index(drop=True, inplace=True)
            tgp, fgp, fhc, ffhc = [], [], [], []

            for element in subcharges:

                if 'region' not in element:
                    holder = subcharge_df
                else:
                    holder = subcharge_df.loc[subcharge_df['zone'] == element['region'][0]]

                if not holder.empty:

                    if '20GP' in element:
                        placer = element[['20GP', 'start_date', 'expiry_date']]
                        placer = placer.iloc[
                                 placer.index.repeat(len(holder.loc[holder['load_type'] == '20GP'])),
                                 :].reset_index(drop=True)
                        adder = holder.loc[subcharge_df['load_type'] == '20GP'].reset_index(drop=True)
                        placer.rename(columns={'20GP': 'amount'}, inplace=True)
                        tgp.append(pd.concat([adder, placer], axis=1))
                    if '40GP' in element:
                        placer = element[['40GP', 'start_date', 'expiry_date']]
                        placer = placer.iloc[
                                 placer.index.repeat(len(holder.loc[holder['load_type'] == '40GP'])),
                                 :].reset_index(drop=True)
                        adder = holder.loc[holder['load_type'] == '40GP'].reset_index(drop=True)
                        placer.rename(columns={'40GP': 'amount'}, inplace=True)
                        fgp.append(pd.concat([adder, placer], axis=1))
                    if '40HC' in element:
                        placer = element[['40HC', 'start_date', 'expiry_date']]
                        placer = placer.iloc[
                                 placer.index.repeat(len(holder.loc[holder['load_type'] == '40HC'])),
                                 :].reset_index(drop=True)
                        adder = holder.loc[holder['load_type'] == '40HC'].reset_index(drop=True)
                        placer.rename(columns={'40HC': 'amount'}, inplace=True)
                        fhc.append(pd.concat([adder, placer], axis=1))
                    if '45HC' in element:
                        placer = element[['45HC', 'start_date', 'expiry_date']]
                        placer = placer.iloc[
                                 placer.index.repeat(len(holder.loc[holder['load_type'] == '45HC'])),
                                 :].reset_index(drop=True)
                        adder = holder.loc[holder['load_type'] == '45HC'].reset_index(drop=True)
                        placer.rename(columns={'45HC': 'amount'}, inplace=True)
                        ffhc.append(pd.concat([adder, placer], axis=1))

            tgp = pd.concat(tgp, ignore_index=True)
            fgp = pd.concat(fgp, ignore_index=True)
            fhc = pd.concat(fhc, ignore_index=True)
            ffhc = pd.concat(ffhc, ignore_index=True)
            subcharge_df = pd.concat([tgp, fgp, fhc, ffhc], ignore_index=True)

            return subcharge_df

        def capture(self):

            contract_id, amd_no, commodity, contract_dates = self.get_headers()
            initial_df = self.get_initial_df()
            initial_df.reset_index(drop=True, inplace=True)
            special_rate_df = self.get_special_rate_df()
            special_rate_df.reset_index(drop=True, inplace=True)
            bullet_rate_df = self.get_bullet_rate_df()
            bullet_rate_df.reset_index(drop=True, inplace=True)
            subcharges = self.get_subcharges()

            freight_df = pd.concat([initial_df, special_rate_df, bullet_rate_df], ignore_index=True)

            if 'Origin' in list(freight_df.columns) and 'Origins' in list(freight_df.columns):
                freight_df['origin_port'] = freight_df.Origin.fillna('') + freight_df.Origins.fillna('')
                freight_df.drop(['Origin', 'Origins'], axis=1, inplace=True)

            if '20SD' in list(freight_df.columns) and '20\'' in list(freight_df.columns):
                freight_df['20GP'] = freight_df['20SD'].fillna('') + freight_df['20\''].fillna('')
                freight_df.drop(['20SD', '20\''], axis=1, inplace=True)

            if '40SD' in list(freight_df.columns) and '40\'' in list(freight_df.columns):
                freight_df['40GP'] = freight_df['40SD'].fillna('') + freight_df['40\''].fillna('')
                freight_df.drop(['40SD', '40\''], axis=1, inplace=True)

            if 'Remarks' in list(freight_df.columns) and 'Remark' in list(freight_df.columns):
                freight_df['remarks'] = freight_df['Remarks'].fillna('') + freight_df['Remark'].fillna('')
                freight_df.drop(['Remarks', 'Remark'], axis=1, inplace=True)

            freight_df['Destinations'] = freight_df['Destinations'].str.split(r'/')
            freight_df = freight_df.explode('Destinations')
            freight_df['commodity'] = commodity
            freight_df['charges'] = 'basic ocean freight'
            cols = {'Destinations': 'destination_port', '40\'HQ': '40HC', '45\'': '45HC', 'Svc Mode': 'service_type'}
            freight_df.rename(columns=cols, inplace=True)
            freight_df = self.melt_load_type(freight_df)
            freight_df = freight_df.loc[freight_df.amount.notna()]

            subcharge_df = self.populate_surcharge(subcharges,
                                                   freight_df.loc[~freight_df['remarks'].str.contains('BC')])

            freight_df = pd.concat([freight_df, subcharge_df], ignore_index=True)
            freight_df['contract_id'] = contract_id
            freight_df['amendment_no'] = amd_no
            freight_df['vendor'] = 'WAN HAI'
            freight_df['currency'] = 'USD'
            freight_df['contract_start_date'] = contract_dates[0]
            freight_df['contract_expiry_date'] = contract_dates[-1]

            self.captured_output = {'Freight': freight_df}

            return self.captured_output

        def clean(self):

            freight_df = self.captured_output['Freight']
            self.cleaned_output = {'Freight': freight_df}

            return self.cleaned_output


class Flexport_WAN_HAI_Word(Expedoc_WAN_HAI_Word):

    class WAN_HAI_YTN(Expedoc_WAN_HAI_Word.WAN_HAI_HKG):

        def get_subcharges(self):

            regex = r'<td><p>(From)(.+?)<\/p><\/td>'
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            match_list, df_list = [], []
            for matchNum, match in enumerate(matches, start=1):
                match = match.group()
                cleaner = re.compile('<.*?>')
                match = re.sub(cleaner, '', match)
                match = match.replace(';', '')
                match = match.split('From')
                for element in match:

                    if 'coast' in element:
                        coast_list = element.split()
                        if 'east' in coast_list[-2]:
                            region = "USEC"
                        elif "west" in coast_list[-2]:
                            region = "USWC"
                    else:
                        region = None

                    if element != '':
                        element = element.strip()
                        group_list = element.split(', BC will be at USD')
                        amounts = group_list[-1].split(' per ')[0].split('/')
                        cols = ['20GP', '40GP', '40HC', '45HC']
                        df = pd.DataFrame(columns=cols)
                        df = df.append(pd.Series(amounts, index=df.columns), ignore_index=True)

                        if 'July1' in group_list[0].split(' to ')[0]:
                            df['start_date'] = parse(group_list[0].split(' to ')[0].replace('July1', 'July'))
                        elif 'Julyl' in group_list[0].split(' to ')[0]:
                            df['start_date'] = parse(group_list[0].split(' to ')[0].replace('Julyl', 'July'))
                        else:
                            df['start_date'] = parse(group_list[0].split(' to ')[0])
                        if 'July1' in group_list[0].split(' to ')[-1]:
                            df['expiry_date'] = parse(group_list[0].split(' to ')[-1].replace('July1', 'July'))
                        if 'Julyl' in group_list[0].split(' to ')[-1]:
                            df['expiry_date'] = parse(group_list[0].split(' to ')[-1].replace('Julyl', 'July'))
                        else:
                            df['expiry_date'] = parse(group_list[0].split(' to ')[-1])

                        if region:
                            df['region'] = region

                        df_list.append(df)
            return df_list

        def get_soc_df(self, soc_df):

            soc_df[['inclusions', 'soc']] = soc_df['remarks'].str.split('/', expand=True)
            soc_df['inclusions'] = soc_df['inclusions'].str.extractall(r'([A-Z]{2,4})').unstack(fill_value='').apply(
                lambda x: ",".join(x.astype(str)), axis=1).apply(lambda x: x.strip(','))
            soc_df.drop(["soc"], axis=1, inplace=True)
            soc_df['container_owned'] = 'SOC'
            return soc_df

        def get_freight_df(self, initial_df, special_rate_df, bullet_rate_df, commodity_description):

            freight_df = pd.concat([initial_df, special_rate_df, bullet_rate_df], ignore_index=True)

            if 'Origin' in list(freight_df.columns) and 'Origins' in list(freight_df.columns):
                freight_df['origin_port'] = freight_df.Origin.fillna('') + freight_df.Origins.fillna('')
                freight_df.drop(['Origin', 'Origins'], axis=1, inplace=True)

            if '20SD' in list(freight_df.columns) and '20\'' in list(freight_df.columns):
                freight_df['20GP'] = freight_df['20SD'].fillna('') + freight_df['20\''].fillna('')
                freight_df.drop(['20SD', '20\''], axis=1, inplace=True)

            if '40SD' in list(freight_df.columns) and '40\'' in list(freight_df.columns):
                freight_df['40GP'] = freight_df['40SD'].fillna('') + freight_df['40\''].fillna('')
                freight_df.drop(['40SD', '40\''], axis=1, inplace=True)

            if 'Remarks' in list(freight_df.columns) and 'Remark' in list(freight_df.columns):
                freight_df['remarks'] = freight_df['Remarks'].fillna('') + freight_df['Remark'].fillna('')
                freight_df.drop(['Remarks', 'Remark'], axis=1, inplace=True)

            freight_df['Destinations'] = freight_df['Destinations'].str.replace(r'/', r";")
            freight_df['commodity'] = commodity_description
            freight_df['charges'] = 'basic ocean freight'
            cols = {'Destinations': 'destination_port', '40\'HQ': '40HC', '45\'': '45HC', 'Svc Mode': 'service_type'}
            freight_df.rename(columns=cols, inplace=True)
            freight_df = self.melt_load_type(freight_df)
            freight_df = freight_df.loc[freight_df.amount.notna()]
            return freight_df

        def get_additional_df(self, df):

            df[['1', '2']] = df['remarks'].str.split('/', expand=True)
            df['1'] = df['1'].str.extractall(r'([A-Z]{2,4})').unstack(fill_value='').apply(
                lambda x: ",".join(x.astype(str)), axis=1).apply(lambda x: x.strip(','))
            df['2'] = df['2'].str.extractall(r'([A-Z]{2,4})').unstack(fill_value='').apply(
                lambda x: ",".join(x.astype(str)), axis=1).apply(lambda x: x.strip(','))
            return df

        def get_special_notes(self):

            inclusions, subject_to, ist = [], [], {}
            regex = r"<p>[a-z]\.(.+?)<\/p>"
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                match = match.group()
                if "inclusive" in match.lower() and "subject to" in match.lower():
                    match_list = match.split(',')
                    for element in match_list:
                        if "GRI" in element and 'subject to' in element.lower():
                            matches = re.findall(r"(\d{4}\s?.+?\s?\d{1,2})", element, re.MULTILINE)[0].split(' ')
                            matches = [ele for ele in matches if ele != '']
                            date = datetime.datetime.strptime(" ".join(matches), "%Y %B %d")
                            check = re.findall(r"\((.+?)\)", match)
                            if len(check) >= 1:
                                ist[check[0]] = date

                elif "inclusive" in match.lower():
                    check = re.findall(r"\((.+?)\)", match)
                    if len(check) >= 1 and check not in inclusions:
                        if check[0] != 'BC':
                            inclusions.append(check[0])

                elif "subject to" in match.lower():
                    check = re.findall(r"\((.+?)\)", match)
                    if len(check) >= 1 and check not in subject_to:
                        if check[0] != 'BC':
                            subject_to.append(check[0])

            def splitter(li):
                for i in range(len(li)):
                    splitter = li[i].split()
                    if len(splitter) > 1:
                        li[i] = ''
                        for word in splitter:
                            li[i] += word[0]
                return li

            subject_to = splitter(subject_to)
            if 'IHC' in subject_to:
                subject_to.remove("IHC")

            return splitter(inclusions), splitter(subject_to), ist

        def apply_inclusions_subject_to(self, inclusions, subject_to, ist, freight_df):

            freight_df.fillna({'inclusions': "", 'subject_to': ""}, inplace=True)
            india_regions = ['Mundra, India', 'Nhava Sheva, India', 'Mundra, Gujarat, India',
                             'Nhava Sheva, Maharashtra, India', 'Chennai, Tamil Nadu, India',
                             'Kattupalli, Tamil Nadu, India', 'Kochi, Kerala, India', 'Kolkata, West Bengal, India',
                             'Pipavav, Gujarat, India', 'Tuticorin, Tamil Nadu, India',
                             'Visakhapatnam, Andhra Pradesh, India', 'CALCUTTA, INDIA', 'CHENNAI, INDIA(CY)',
                             'KATTUPALLI PORT', 'COCHIN, INDIA', 'MANGALORE, INDIA(CY)', 'TUTICORIN',
                             'PIPAVAV, INDIA(CY)', 'VISAKHAPATNAM', 'MUNDRA, INDIA(CY)', 'NHAVA SHEVA',
                             'COCHIN, INDIA(CY)', 'NHAVA SHEVA, INDIA(CY)', 'TUTICORIN, INDIA', 'PIPAVAV, INDIA',
                             'VISAKHAPATNAM, INDIA', 'CALCUTTA, INDIA(CY)', 'PIPAVAV (VICTOR) PORT',
                             'KOLKATA(CALCUTTA)', 'CHENNAI', 'COCHIN', 'MUNDRA', 'CALCUTTA', 'PIPAVAV',
                             'KATTUPALLI PORT, INDIA(CY)', 'KRISHNAPATNAM, INDIA(CY)', 'TUTICORIN, INDIA(CY)',
                             'KATTUPALLI PORT, INDIA', 'Chennai (Madras)', 'Kattupalli', 'Kolkata (Calcutta)',
                             'Haldia', 'Krishnapatnam', 'Kolkata', 'HAZIRA', 'Mundra', 'Nhava Sheva', 'Mundra',
                             'Nhava Sheva', 'Chennai', 'Kattupalli', 'Kochi', 'Kolkata (ex Calcutta)',
                             'Pipavav (Victor) Port', 'Tuticorin', 'Visakhapatnam', 'Kolkata (ex Calcutta)',
                             'Chennai', 'KATTUPALLI PORT', 'Cochin', 'Mangalore', 'Tuticorin', 'Pipavav (Victor) Port ',
                             'Visakhapatnam', 'Mundra', 'Nhava Sheva', 'Cochin', 'Nhava Sheva', 'Tuticorin',
                             'Pipavav (Victor) Port', 'Visakhapatnam', 'Kolkata (ex Calcutta)', 'Pipavav (Victor) port',
                             'Kolkata (ex Calcutta)', 'Chennai', 'Cochin', 'Mundra', 'Kolkata (ex Calcutta)',
                             'Pipavav (Victor) Port', 'KATTUPALLI PORT', 'Krishnapatnam', 'Tuticorin', 'KATTUPALLI PORT'
                             , 'Chennai', 'Kattupalli', 'Kolkata (ex Calcutta)', 'Haldia', 'Krishnapatnam',
                             'Kolkata (ex Calcutta)', 'Hazira']

            def inc(df, inclusions):
                for element in inclusions:
                    for i in range(df.shape[0]):
                        if element not in df['inclusions'][i] and element not in df['subject_to'][i]:
                            df['inclusions'][i] += "," + element
                return df

            def sub(df, subject_to):
                for element in subject_to:
                    for i in range(df.shape[0]):
                        if element not in df['subject_to'][i] and element not in df['inclusions'][i]:
                            df['subject_to'][i] += "," + element
                return df

            freight_df = inc(freight_df, inclusions)
            freight_df = sub(freight_df, subject_to)
            no_gri = freight_df.loc[freight_df['inclusions'].str.contains('GRI', na=False)]
            freight_df = freight_df.loc[~freight_df['inclusions'].str.contains('GRI', na=False)]
            for key, value in ist.items():
                df3, df4 = None, None
                if not freight_df.loc[(freight_df["start_date"] < value) & (freight_df["expiry_date"] > value)].empty:
                    df1 = freight_df.loc[(freight_df["start_date"] < value) & (freight_df["expiry_date"] > value)].copy(
                        deep=True)
                    df2 = df1.copy(deep=True)
                    df1.reset_index(drop=True, inplace=True)
                    df2.reset_index(drop=True, inplace=True)

                if not freight_df.loc[(freight_df["expiry_date"] < value)].empty:
                    df3 = freight_df.loc[(freight_df["expiry_date"] < value)].copy(deep=True)
                    df3.reset_index(drop=True, inplace=True)
                    df3 = inc(df3, [key])

                if not freight_df.loc[(freight_df["start_date"] > value)].empty:
                    df4 = freight_df.loc[(freight_df["start_date"] > value)].copy(deep=True)
                    df4.reset_index(drop=True, inplace=True)
                    df4 = sub(df4, [key])

                df1 = inc(df1, [key])
                df1['expiry_date'] = value - pd.Timedelta(days=1)

                df2 = sub(df2, [key])
                df2['start_date'] = value

            if not df1.empty and df3 is not None and df4 is not None:
                freight_df = pd.concat([df1, df2, df3, df4], ignore_index=True)
            elif not df1.empty and df3 is not None:
                freight_df = pd.concat([df1, df2, df3], ignore_index=True)
            elif not df1.empty and df4 is not None:
                freight_df = pd.concat([df1, df2, df4], ignore_index=True)
            elif not df1.empty:
                freight_df = pd.concat([df1, df2], ignore_index=True)

            freight_df = pd.concat([freight_df, no_gri], ignore_index=True)
            freight_df['inclusions'] = freight_df['inclusions'].apply(
                lambda x: x.strip(',') if isinstance(x, str) else x)
            freight_df['subject_to'] = freight_df['subject_to'].apply(
                lambda x: x.strip(',') if isinstance(x, str) else x)
            return freight_df

        def apply_zones(self, freight_df, region_dict):

            freight_df['zone'] = ''
            for key, value in region_dict.items():
                for i in range(freight_df.shape[0]):
                    if ';' in freight_df['destination_port'][i]:
                        if ',' in freight_df['destination_port'][i].split(';')[0]:
                            if freight_df['destination_port'][i].split(';')[0].split(',')[0].lower() in value:
                                freight_df['zone'][i] = key
                            else:
                                if freight_df['destination_port'][i].split(';')[0].lower() in value:
                                    freight_df['zone'][i] = key
                        else:
                            if freight_df['destination_port'][i].split(';')[0].lower() in value:
                                freight_df['zone'][i] = key
                    elif "," in freight_df['destination_port'][i]:
                        if freight_df['destination_port'][i].split(',')[0].lower() in value:
                            freight_df['zone'][i] = key
                    else:
                        if freight_df['destination_port'][i].lower() in value:
                            freight_df['zone'][i] = key
            return freight_df

        def get_surcharge_dfs(self, subcharge_df):

            usec, uswc = subcharge_df[subcharge_df['zone'] == 'USEC'], subcharge_df[subcharge_df['zone'] == 'USWC']
            usec_amounts = {'20GP_bc': usec[usec['load_type'] == '20GP'].amount.unique(),
                            '40GP_bc': usec[usec['load_type'] == '40GP'].amount.unique(),
                            '40HC_bc': usec[usec['load_type'] == '40HC'].amount.unique()
                            , '45HC_bc': usec[usec['load_type'] == '45HC'].amount.unique()}
            uswc_amounts = {'20GP_bc': uswc[uswc['load_type'] == '20GP'].amount.unique(),
                            '40GP_bc': uswc[uswc['load_type'] == '40GP'].amount.unique(),
                            '40HC_bc': uswc[uswc['load_type'] == '40HC'].amount.unique()
                            , '45HC_bc': uswc[uswc['load_type'] == '45HC'].amount.unique()}
            usec, uswc = pd.DataFrame(usec_amounts), pd.DataFrame(uswc_amounts)

            return usec, uswc

        def unmelt(self, df1):

            df1 = df1.fillna('')
            df1 = df1.pivot_table(index=[column for column in df1.columns if column not in ['load_type', 'amount']],
                                columns=['load_type'],
                                values=['amount'],
                                aggfunc='first')
            df1 = df1.reset_index()

            new_columns = []
            for index in df1.columns.to_flat_index():
                if index[0] == 'amount':
                    new_columns.append(index[1])
                else:
                    new_columns.append(index[0])

            df1.columns = new_columns

            return df1

        def get_final_df(self, freight_df, usec, uswc):

            usec.drop(['start_date', 'expiry_date', 'region'], axis=1, inplace=True)
            uswc.drop(['start_date', 'expiry_date', 'region'], axis=1, inplace=True)
            cols = {'20GP': '20GP_bc', '40GP': '40GP_bc', '40HC': '40HC_bc', '45HC': '45HC_bc'}
            usec.rename(columns=cols, inplace=True)
            uswc.rename(columns=cols, inplace=True)
            usec_freight = self.unmelt(freight_df[(freight_df['zone'] == 'USEC')])
            uswc_freight = self.unmelt(freight_df[(freight_df['zone'] == 'USWC')])
            # adder = self.unmelt(freight_df[freight_df['inclusions'].str.contains('BC', na=False)])

            df1 = usec_freight.merge(usec, how='cross')
            df2 = uswc_freight.merge(uswc, how='cross')

            columns = usec.columns.tolist() + usec_freight.columns.tolist()

            freight_df = pd.concat([df1, df2], ignore_index=True)

            '''
            for column in columns:
                if column not in adder:
                    adder[column] = ''
            '''

            # freight_df = pd.concat([freight_df, adder], ignore_index=True)

            return freight_df

        def flex_populate_surcharges(self, surcharge_df, subcharges):

            surcharge_df = self.unmelt(surcharge_df)
            start_dates = list(surcharge_df['start_date'].unique())
            expiry_dates = list(surcharge_df['expiry_date'].unique())
            main_df_list = []

            for sd in start_dates:
                for ed in expiry_dates:
                    if not surcharge_df.loc[(surcharge_df['start_date'] == sd)
                                            & (surcharge_df['expiry_date'] == ed)].empty:
                        main_df_list.append(surcharge_df.loc[(surcharge_df['start_date'] == sd)
                                            & (surcharge_df['expiry_date'] == ed)])

            master_df_list = []
            for surcharge_df in main_df_list:
                master_df, count, lst_counter = None, 0, 0
                df1, df2, df3, dfs = None, None, None, []
                adder1, adder2, adder3 = None, None, None
                for df in subcharges:
                    df.rename(columns={'20GP': '20GP_bc', '40GP': '40GP_bc', '40HC': '40HC_bc', '45HC': '45HC_bc'}
                              , inplace=True)

                    if master_df is not None:
                        surcharge_df = master_df.iloc[:, :-4].copy(deep=True)
                        # master_df_list.append(master_df)
                        master_df = None

                    if 'region' in df:

                        '''Condition to check if sur-charge start and expiry date fall into base rates time interval'''
                        if not surcharge_df.loc[(surcharge_df['start_date'] <= df["start_date"][0]) & (
                                surcharge_df['expiry_date'] >= df["expiry_date"][0])
                                & (surcharge_df['zone'] == df['region'][0])].empty:
                            df1 = surcharge_df.loc[(surcharge_df['start_date'] <= df["start_date"][0]) & (
                                    surcharge_df['expiry_date'] >= df["expiry_date"][0]) &
                                    (surcharge_df['zone'] == df['region'][0])]
                            adder1 = surcharge_df.loc[(surcharge_df['start_date'] <= df["start_date"][0]) & (
                                    surcharge_df['expiry_date'] >= df["expiry_date"][0]) &
                                    ~(surcharge_df['zone'] == df['region'][0])].copy(deep=True)

                            '''Condition to handle dataframe when only the sur-charge start date falls into base rates 
                            time interval'''
                        elif not surcharge_df.loc[(surcharge_df['start_date'] <= df["start_date"][0]) & (
                                surcharge_df['expiry_date'] >= df["start_date"][0])
                                & (surcharge_df['zone'] == df['region'][0])].empty:
                            df2 = surcharge_df.loc[(surcharge_df['start_date'] <= df["start_date"][0]) & (
                                    surcharge_df['expiry_date'] >= df["start_date"][0])
                                    & (surcharge_df['zone'] == df['region'][0])]
                            adder2 = surcharge_df.loc[(surcharge_df['start_date'] <= df["start_date"][0]) & (
                                    surcharge_df['expiry_date'] >= df["start_date"][0])
                                    & ~(surcharge_df['zone'] == df['region'][0])].copy(deep=True)

                            '''Condition to handle dataframe when only the sur-charge expiry date falls into base rates 
                                                    time interval'''
                        elif not surcharge_df.loc[(surcharge_df['start_date'] <= df["expiry_date"][0]) & (
                                surcharge_df['expiry_date'] >= df["expiry_date"][0])
                                & (surcharge_df['zone'] == df['region'][0])].empty:
                            df3 = surcharge_df.loc[(surcharge_df['start_date'] <= df["expiry_date"][0]) & (
                                    surcharge_df['expiry_date'] >= df["expiry_date"][0])
                                    & (surcharge_df['zone'] == df['region'][0])]
                            adder3 = surcharge_df.loc[(surcharge_df['start_date'] <= df["expiry_date"][0]) & (
                                    surcharge_df['expiry_date'] >= df["expiry_date"][0])
                                    & ~(surcharge_df['zone'] == df['region'][0])].copy(deep=True)
                        if df1 is None:
                            if adder1 is not None:
                                df1 = adder1.copy(deep=True)
                        if df2 is None:
                            if adder2 is not None:
                                df2 = adder2.copy(deep=True)
                        if df3 is None:
                            if adder3 is not None:
                                df3 = adder3.copy(deep=True)
                    else:
                        '''Condition to check if sur-charge start and expiry date fall into base rates time interval'''
                        if not surcharge_df.loc[(surcharge_df['start_date'] <= df["start_date"][0]) & (
                                    surcharge_df['expiry_date'] >= df["expiry_date"][0])].empty:
                            df1 = surcharge_df.loc[(surcharge_df['start_date'] <= df["start_date"][0]) & (
                                        surcharge_df['expiry_date'] >= df["expiry_date"][0])]

                            '''Condition to handle dataframe when only the sur-charge start date falls into base rates 
                            time interval'''
                        elif not surcharge_df.loc[(surcharge_df['start_date'] <= df["start_date"][0]) & (
                                    surcharge_df['expiry_date'] >= df["start_date"][0])].empty:
                            df2 = surcharge_df.loc[(surcharge_df['start_date'] <= df["start_date"][0]) & (
                                    surcharge_df['expiry_date'] >= df["start_date"][0])]

                            '''Condition to handle dataframe when only the sur-charge expiry date falls into base rates 
                                                    time interval'''
                        elif not surcharge_df.loc[(surcharge_df['start_date'] <= df["expiry_date"][0]) & (
                                    surcharge_df['expiry_date'] >= df["expiry_date"][0])].empty:
                            df3 = surcharge_df.loc[(surcharge_df['start_date'] <= df["expiry_date"][0]) & (
                                    surcharge_df['expiry_date'] >= df["expiry_date"][0])]

                    if df1 is not None:
                        for i in range(3):
                            holder = df1.copy(deep=True)
                            if i == 0:
                                # if count == 0:
                                holder['expiry_date'] = df['start_date'][0] - pd.Timedelta(days=1)
                                if count == 0:
                                    master_df_list.append(holder)
                            elif i == 1:
                                holder['start_date'] = df['start_date'][0]
                                holder['expiry_date'] = df['expiry_date'][0]
                                holder = holder.merge(df[['20GP_bc', '40GP_bc', '40HC_bc', '45HC_bc']], how='cross')
                                master_df_list.append(holder)
                            elif i == 2:
                                holder['start_date'] = df['expiry_date'][0] + pd.Timedelta(days=1)
                            holder.reset_index(drop=True, inplace=True)
                            dfs.append(holder)

                    if df2 is not None:
                        for i in range(2):
                            holder = df2.copy(deep=True)
                            if i == 0:
                                # if count == 0:
                                holder['expiry_date'] = df['start_date'][0] - pd.Timedelta(days=1)
                            elif i == 1:
                                holder['start_date'] = df['start_date'][0]
                                holder = holder.merge(df[['20GP_bc', '40GP_bc', '40HC_bc', '45HC_bc']], how='cross')
                                master_df_list.append(holder)
                            holder.reset_index(drop=True, inplace=True)
                            dfs.append(holder)

                    if df3 is not None:
                        for i in range(2):
                            holder = df3.copy(deep=True)
                            if i == 0:
                                holder['expiry_date'] = df['expiry_date'][0]
                                holder = holder.merge(df[['20GP_bc', '40GP_bc', '40HC_bc', '45HC_bc']], how='cross')
                                master_df_list.append(holder)
                            elif i == 1:
                                holder['start_date'] = df['expiry_date'][0] + pd.Timedelta(days=1)
                                if lst_counter == len(subcharges):
                                    master_df_list.append(holder)
                            holder.reset_index(drop=True, inplace=True)
                            dfs.append(holder)
                    lst_counter += 1
                    if dfs:
                        master_df = pd.concat(dfs, ignore_index=True)
                        dfs = []
                    '''
                    if lst_counter == len(subcharges):
                        master_df_list.append(master_df)
                    '''
                    df1, df2, df3, holder, dfs, count = None, None, None, None, [], 1

            return pd.concat(master_df_list, ignore_index=True)

        def capture(self):

            region_dict = {"USEC": ['newark', 'new york', 'charleston', 'savannah', 'norfolk', 'miami', 'baltimore',
                                    'port everglades', 'portsmouth', 'chicago', 'detroit', 'front royal', 'memphis',
                                    'boston', 'portland me', 'minneapolis', 'philadelphia', 'jacksonville',
                                    'wilmington'],
                           "USWC": ['los angeles', 'long beach', 'oakland', 'seattle', 'tacoma', 'portland', 'honolulu',
                                    'hilo', 'kawaihae (kailua kona)', 'nawiliwili (kauai island)', 'kahului', 'tacoma'],
                           "USGC": ['houston', 'new orleans', 'mobile'],
                           "CANEC": ['halifax', 'montreal', 'toronto', "st john's", 'prince rupert', 'vancouver',
                                     'calgary', 'edmonton', 'winnipeg', 'victoria', 'saskatoon', 'regina']
                           }
            line_item_id, amd_no, commodity_description, contract_dates = super().get_headers()
            initial_df = super().get_initial_df()
            initial_df.reset_index(drop=True, inplace=True)
            special_rate_df = super().get_special_rate_df()
            special_rate_df.reset_index(drop=True, inplace=True)
            bullet_rate_df = super().get_bullet_rate_df()
            bullet_rate_df.reset_index(drop=True, inplace=True)
            subcharges = self.get_subcharges()
            inclusions, subject_to, ist = self.get_special_notes()

            freight_df = self.get_freight_df(initial_df, special_rate_df, bullet_rate_df, commodity_description)

            freight_df['commodity_description'], freight_df['commodity'] = freight_df['commodity'], 'FAK'
            freight_df.loc[
                (freight_df['named_account'].str.contains('DIAMOND', na=False)), ['premium', 'named_account']] \
                = 'DIAMOND SERVICE', ''
            freight_df.loc[
                (freight_df['named_account'].str.contains('PREMIUM', na=False)), ['premium', 'named_account']] \
                = 'PREMIUM CARGO', ''

            subject_to_df = \
                self.get_additional_df(freight_df.loc[(freight_df['remarks'].str.contains('Subject to', na=False))])
            subject_to_df.rename(columns={'1': 'inclusions', '2': 'subject_to'}, inplace=True)
            soc_df = self.get_soc_df(freight_df.loc[(freight_df['remarks'].str.contains('SHIPPER', na=False))])
            inclusive_df = self.get_additional_df(
                freight_df.loc[~(freight_df['remarks'].str.contains('SHIPPER', na=False))
                               & (~freight_df['remarks'].str.contains('Subject to', na=False))
                               & (freight_df['remarks'] != '')])
            inclusive_df['inclusions'] = inclusive_df[['1', '2']].fillna('').agg(','.join, axis=1) \
                .apply(lambda x: x.strip(', '))
            inclusive_df.drop(['1', '2'], axis=1, inplace=True)

            freight_df = pd.concat([freight_df.loc[freight_df['remarks'] == ''], subject_to_df, soc_df, inclusive_df]
                                   , ignore_index=True)
            freight_df = self.apply_inclusions_subject_to(inclusions, subject_to, ist, freight_df)
            freight_df = self.apply_zones(freight_df, region_dict)

            # surcharge_df = super().populate_surcharge(subcharges, freight_df.loc[
                # ~freight_df['inclusions'].str.contains('BC', na=False)])

            # usec, uswc = self.get_surcharge_dfs(surcharge_df)
            surcharge_df = self.flex_populate_surcharges(
                freight_df[~freight_df['inclusions'].str.contains('BC', na=False)].copy(deep=True), subcharges)
            # usec, uswc = subcharges[-1], subcharges[-2]

            '''
            for element in subcharges:
                if 'region' in element:
                    if element['region'][0] == 'USEC':
                        usec = element
                    elif element['region'][0] == 'USWC':
                        uswc = element
            '''

            # freight_df = self.get_final_df(freight_df, usec, uswc)
            freight_df = pd.concat([self.unmelt(freight_df[freight_df['inclusions'].str.contains('BC', na=False)])
                                    , surcharge_df]
                                   , ignore_index=True)
            freight_df.drop_duplicates(keep='first', inplace=True)
            freight_df['amendment_no'] = amd_no
            freight_df['vendor'] = 'WAN HAI'
            freight_df['currency'] = 'USD'
            freight_df['contract_start_date'] = contract_dates[0]
            freight_df['contract_expiry_date'] = contract_dates[-1]

            self.captured_output = {'Freight': freight_df}

            return self.captured_output

        def clean(self):

            freight_df = self.captured_output['Freight']
            freight_df.rename(columns={'named_account': 'customer_name', 'premium': 'Premium Service'}, inplace=True)
            self.cleaned_output = {'Freight': freight_df}

            return self.cleaned_output


class CEVA_WAN_HAI_Word(Expedoc_WAN_HAI_Word):

    class WAN_HAI(Expedoc_WAN_HAI_Word.WAN_HAI_HKG):

        def get_subcharges(self):
            regex = r"From\s(.+?)</p>"
            df_list = []
            if re.search(regex, self.raw_html):
                # subcharges=[]
                matches = re.finditer(regex, self.raw_html, re.MULTILINE)
                for matchnum, match in enumerate(matches, start=1):
                    for groupnum in range(0, len(match.groups())):
                        groupnum = groupnum + 1
                        group = match.group(groupnum)
                        # subcharges.append(group)
                        if "(" in group:
                            subcharges_index = group.index("(")
                            group = group[:subcharges_index - 1]
                            # subcharges.append(group)
                        group_list = group.split(', BC will be at USD')
                        amounts = group_list[-1].split(' per ')[0].split('/')
                        cols = ['20GP', '40GP', '40HC', '45HC']
                        # cols = group_list[-1].split(' per ')[-1].split('/')
                        df = pd.DataFrame(columns=cols)
                        df = df.append(pd.Series(amounts, index=df.columns), ignore_index=True)
                        if 'July1' in group_list[0].split(' to ')[0]:
                            df['start_date'] = parse(group_list[0].split(' to ')[0].replace('July1', 'July'))
                        elif 'Julyl' in group_list[0].split(' to ')[0]:
                            df['start_date'] = parse(group_list[0].split(' to ')[0].replace('Julyl', 'July'))
                        else:
                            df['start_date'] = parse(group_list[0].split(' to ')[0])
                        if 'July1' in group_list[0].split(' to ')[-1]:
                            df['expiry_date'] = parse(group_list[0].split(' to ')[-1].replace('July1', 'July'))
                        if 'Julyl' in group_list[0].split(' to ')[-1]:
                            df['expiry_date'] = parse(group_list[0].split(' to ')[-1].replace('Julyl', 'July'))
                        else:
                            df['expiry_date'] = parse(group_list[0].split(' to ')[-1])
                        df_list.append(df)
                return df_list