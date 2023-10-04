import datetime
from pandera.typing import DataFrame
from logging import getLogger
from dateutil.parser import parse
from base import BaseTemplate, BaseFix
from custom_exceptions import InputValidationError
import pandas as pd
from collections import defaultdict
from datetime import datetime
import re
from numpy import nan
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
log = getLogger(__name__)


class HMM_v1(BaseTemplate):
    """Base class for HMM v1"""

    class _UsaAsia(BaseFix):

        def __init__(self, df: DataFrame, file_model: dict, headers_config: dict):
            super().__init__(df, file_model, headers_config)

        def check_input(self):

            check_errors = []
            if not self.df[0].str.contains('RATE FOR BULLET NO.', na=False).any():
                check_errors.append("RATE FOR BULLET NO. should be present in first Column.")

            if not self.df[0].str.contains('DURATION', na=False).index.any():
                check_errors.append("Duration section should be present.")

            if not self.df[0].str.contains('Special Note', na=False).any():
                check_errors.append("Special Note should be present.")

            if not self.df[26].str.contains("", na=False).any():
                check_errors.append(r"Ammendmant no. should be present.")

            if not self.df[0].str.contains('3. COMMODITY', na=False).any():
                check_errors.append("Commodity should be present.")

            if not self.df[0].str.contains('4. MINIMUM QUANTITY COMMITMENT', na=False).any():
                check_errors.append("Commodity end section should be present.")

            if not self.df[0].str.contains('CONTRACT RATES AND CHARGES', na=False).any():
                check_errors.append("Geo location abrevation should be present.")
            if check_errors:
                raise InputValidationError(check_errors)

        def check_output(self):
            pass

        def get_sections(self, sections_to_check):
            pointer = 1
            sections = {}

            for check in sections_to_check:
                if self.df[self.df[0].str.contains(check, na=False)].index.values.any():
                    index = list(self.df[(self.df[0].str.contains(check, na=False))].index)
                    sections[pointer] = index
                else:
                    sections[pointer] = None
                pointer += 1
            return sections

        def get_duration(self, start, end):
            duration = {}
            duration_df = self.df[start[0]:end[0]].copy(deep=True)
            duration_df = duration_df.replace('', nan)
            duration_df.dropna(axis=1, inplace=True, how='all')
            duration_df.reset_index(drop=True, inplace=True)
            duration_df.columns = range(duration_df.shape[1])
            duration_df.drop(1, inplace=True, axis=1)
            start_date = duration_df.iloc[1, 0]
            start_date = datetime.strptime(start_date, '%B %d, %Y')
            end_date = duration_df.iloc[1, 1]

            end_date = datetime.strptime(end_date, '%B %d, %Y')
            duration['start_date'] = start_date
            duration['end_date'] = end_date
            return duration

        def get_comm_desc(self, start, end):
            comm_desc = defaultdict(list)
            comm_desc_df = self.df[start[0]:end[0]].copy(deep=True)
            comm_desc_df = comm_desc_df.replace('', nan)
            comm_desc_df.dropna(axis=1, how="all", inplace=True)
            comm_desc_df.reset_index(drop=True, inplace=True)
            comm_desc_df.drop(0, axis=1, inplace=True)
            comm_desc_df[1].fillna(method='ffill', inplace=True)
            comm_desc_df.dropna(axis=0, inplace=True, how='any')

            for _key, _value in zip(list(comm_desc_df[1]), list(comm_desc_df[3])):
                comm_desc[_key].append(_value)
            return comm_desc

        def get_geo_code(self, start, end):

            geo_code = defaultdict(list)
            geocode_df = self.df[start[0] + 1: end[0]].copy(deep=True)
            geocode_df = geocode_df.replace('', nan)
            geocode_df.dropna(axis=1, inplace=True, how='all')
            geocode_df.dropna(axis=0, inplace=True, how='all')
            geocode_df.reset_index(drop=True, inplace=True)
            geocode_df.columns = range(geocode_df.shape[1])
            geocode_df.drop(1, axis=1, inplace=True)
            geocode_df.fillna(method='ffill', inplace=True)
            for _key, _value in zip(list(geocode_df[0]), list(geocode_df[2])):
                geo_code[_key].append(_value)
            return geo_code

        def get_bullet_no(self, start):
            bullet_no = []
            for _index in start:
                bullet_string = self.df[_index:_index + 1].iloc[0, 0]
                bullet_search = re.search(r"\d{1,2}", bullet_string)
                bullet_no.append(bullet_search.group(0))
            return bullet_no

        def get_amd_no(self, file_code):
            amd_index = list(self.df[(self.df[26].str.contains(file_code, na=False))].index)
            amdno = self.df[amd_index[0]:amd_index[0] + 1].iloc[0, 26]
            amd_no = re.findall(r'AMD\s+?(.+?)$', amdno)
            amd_no = amd_no[0]
            return amd_no

        def get_table_from_bulletin(self, start, end):
            df_list = []
            start = sorted(start + end)
            for _index in range(0, len(start) - 1):
                tmp = self.df[start[_index]:start[_index + 1]].copy(deep=True)
                tmp = tmp.replace('', nan)
                tmp.dropna(axis=1, inplace=True, how='all')
                tmp.drop(1, axis=1, inplace=True)
                tmp.columns = range(tmp.shape[1])
                df_list.append(tmp)
            return df_list

        def get_notes(self, start, df_list, duration, end):
            notes_index = []
            start = sorted(start + end)
            for df_notes in df_list:
                note_index = list(df_notes[(df_notes[0].str.contains('NOTE FOR BULLET', na=False))].index)
                if len(note_index) == 0:
                    # start_date_list = {}
                    # end_date_list = {}
                    # remarks = {}
                    # start_date_list = duration['start_date']
                    # end_date_list = duration['end_date']
                    # note_charges_dict = None
                    notes_index.append(0)
                else:
                    if len(note_index) > 1:
                        note_index.pop()
                    notes_index.append(note_index[0])
            start_date_list = {}
            end_date_list = {}
            remarks = {}
            charge_value_final = []
            point = 1
            note_charges_dict = defaultdict(list)
            for index in range(len(notes_index)):
                if notes_index[index] != 0:

                    df_notes = self.df[notes_index[index] + 1:start[index + 1]].copy(deep=True)
                    df_notes = df_notes.replace('', nan)
                    df_notes.dropna(axis=1, inplace=True, how='all')
                    df_notes.dropna(axis=0, inplace=True, how='all')
                    pos = 0
                    flag = 0
                    if (len(df_notes.columns)) >= 2:
                        df_notes.columns = ['1', 0]
                        df_notes['1'] = df_notes['1'].fillna(method='backfill')
                        df_notes['1'] = df_notes['1'].fillna(method='ffill')
                        df_notes = df_notes.groupby(0, dropna=True)['1'].apply(' '.join).reset_index()
                        df_notes[0] = df_notes.groupby(['1'])[0].transform(lambda x: '; \n'.join(x))
                        df_notes.drop('1', inplace=True, axis=1)
                        df_notes.columns = [0]

                    df_notes = df_notes.fillna('')
                    for i in range(len(df_notes)):

                        notes = df_notes.iloc[pos, 0]

                        if notes != '':
                            if 'Applicable' in notes and 'from' in notes:
                                notes = notes.split(' ')
                                notes_start_date = datetime.strptime(''.join(notes[2]), '%Y-%m-%d')
                                notes_end_date = datetime.strptime(''.join(notes[4][-10:]), '%Y-%m-%d')
                                flag = 1
                                start_date_list[point] = notes_start_date
                                end_date_list[point] = notes_end_date
                                notes = ' '.join(notes)
                                remarks[point] = notes
                                note_charges_dict[point].append(None)

                            elif 'Fixed' in notes and 'Amount' in notes:

                                notes_t = notes.split('\n')
                                for notes_tm in notes_t:

                                    notes_re = re.findall('-(.+?)$', notes_tm)

                                    if notes_re:

                                        len_per = len(re.findall('per', str(notes_re)))

                                        regex = r'\["\s+?(.+?)\s+?USD\s+(.+?)' + ('per(.+?)' * len_per) + '\s+?,'
                                        dest_point = 0
                                        suf_point = 0
                                        if str(notes_re).find("Destination") != -1:
                                            reg = r'Destination\s+?=\s+?(.+?)$'
                                            notes_re_dest = re.findall(reg, notes_tm)
                                            dest_point = 1

                                        if str(notes_re).find("in case of Shipping Term") != -1:
                                            reg = r'in case of Shipping Term\s+?=\s+?(.+?)$'
                                            notes_re_suf = re.findall(reg, notes_tm)
                                            suf_point = 1

                                        if re.search(regex, str(notes_re)) is None:
                                            regex = r'\["\s+?(.+?)\s+?USD\s+(.+?)' + ('per(.+?)' * len_per) + ',\s+?in'
                                            if re.search(regex, str(notes_re)) is None:
                                                regex = r"\['\s+?(.+?)\s+?USD\s+(.+?)" + (
                                                        "per(.+?)" * len_per) + ",\s+?in"
                                        matches = re.finditer(regex, str(notes_re), re.MULTILINE)

                                        for matchNum, match in enumerate(matches, start=1):
                                            charge_value_final = []
                                            charge_name = match.group(1)
                                            charge_value = match.group(2)
                                            charge_value = charge_value.replace(' ', '')
                                            charge_value = charge_value.split('/')
                                            container = []
                                            posi = 0
                                            i = 0

                                            for groupNum in range(2, len(match.groups())):
                                                groupNum = groupNum + 1
                                                group = match.group(groupNum)
                                                group = group.replace(' ', '').replace('/', '')
                                                group = group.split(',')

                                                charge_value_final.extend(([charge_value[i]] * len(group)))
                                                i += 1

                                                for grp in group:
                                                    if dest_point == 0:
                                                        grp = charge_name + '_' + grp
                                                        if suf_point == 1:
                                                            grp = charge_name + '_' + grp + '_yes_' + notes_re_suf[0]
                                                    else:
                                                        grp = charge_name + '_' + grp + '_yes_' + notes_re_dest[0]
                                                    container.append(grp)
                                                    dic = zip(container, charge_value_final)
                                                posi += 1

                                            note_charges_dict[point].append(dict(dic))

                                        if flag == 0:
                                            notes_start_date = duration['start_date']
                                            notes_end_date = duration['end_date']
                                            start_date_list[point] = notes_start_date
                                            end_date_list[point] = notes_end_date
                                        remarks[point] = notes
                            else:
                                if flag == 0:
                                    notes_start_date = duration['start_date']
                                    notes_end_date = duration['end_date']
                                    start_date_list[point] = notes_start_date
                                    end_date_list[point] = notes_end_date
                                remarks[point] = notes
                                note_charges_dict[point].append(None)

                            pos += 1
                    point += 1
                else:
                    start_date_list[point] = duration['start_date']
                    end_date_list[point] = duration['end_date']
                    note_charges_dict[point].append(None)
                    remarks[point] = None
                    point += 1

            return remarks, start_date_list, end_date_list, note_charges_dict

        @staticmethod
        def map_data_to_table(df_list, remarks, start_date_list, end_date_list, note_charges_dict, bullet_no):
            maped = []
            bullet = 1
            bullet_index = 0
            point = 1
            for df_group in df_list:
                df_group.reset_index(drop=True, inplace=True)
                start_index = list(df_group[(df_group[0] == 'ORIGIN')].index)
                end_index = list(df_group[(df_group[0].str.contains('NOTE FOR BULLET', na=False))].index)
                start_index_client = list(df_group[(df_group[0].str.contains('NAMED CUSTOMER', na=False))].index)
                commodity_index = list(df_group[(df_group[0] == 'COMMODITY')].index)
                if len(start_index_client) == 0:
                    commodity_df = df_group[commodity_index[0]:start_index[0]].copy(deep=True)
                else:
                    commodity_df = df_group[commodity_index[0]:start_index_client[0]].copy(deep=True)
                if len(end_index) > 1:
                    end_index.pop()

                if len(end_index) == 0:
                    start_index = start_index + list(df_group.tail(1).index.values + 1)
                else:
                    start_index = sorted(start_index + end_index)
                customer_name_list = []
                customer_name = ''
                if len(start_index_client) != 0:
                    pos_cus = 0
                    customer_name_list = []
                    customer_df = df_group[start_index_client[0]:start_index[0]].copy(deep=True)
                    for i in range(len(customer_df)):
                        if str(customer_df.iloc[pos_cus, 2]) != 'nan':
                            customer_name = customer_df.iloc[pos_cus, 2]
                            customer_name = customer_name.split('  ')[0:2]
                            customer_name = ''.join(customer_name)
                            pos_cus += 1
                            customer_name_list.append(customer_name)
                        elif str(customer_df.iloc[pos_cus, 1]) != 'nan':
                            customer_name = customer_df.iloc[pos_cus, 1]
                            customer_name = customer_name.split('  ')[0:2]
                            customer_name = ''.join(customer_name)
                            pos_cus += 1
                            customer_name_list.append(customer_name)
                        else:
                            customer_name_list.append(None)

                for _index in range(0, len(start_index) - 1):
                    origin_map = df_group[start_index[_index]:start_index[_index + 1]].copy(deep=True)

                    if str(origin_map.iloc[0, 2]) != 'nan':
                        origin_pair = origin_map.iloc[0, 2]
                        origin_pair = ''.join(str(origin_pair))
                    elif str(origin_map.iloc[0, 1]) != 'nan':
                        origin_pair = origin_map.iloc[0, 1]
                        origin_pair = ''.join(str(origin_pair))
                    else:
                        origin_pair = ''

                    origin_map = origin_map.replace('', nan)
                    origin_map.dropna(axis=0, inplace=True, how='all')

                    origin_map.loc[:, 'Origin'] = origin_pair
                    origin_map.loc[:, 'start_date'] = start_date_list[bullet]
                    origin_map.loc[:, 'expiry_date'] = end_date_list[bullet]
                    pos = 0
                    group_code_list = []

                    for i in range(len(commodity_df)):
                        if str(commodity_df.iloc[pos, 2]) != 'nan':
                            group_code = commodity_df.iloc[pos, 2]
                            group_code = group_code.split('  ')[0:2]
                            group_code = ''.join(group_code)
                            pos += 1
                            group_code_list.append(group_code)
                        elif str(commodity_df.iloc[pos, 1]) != 'nan':
                            group_code = commodity_df.iloc[pos, 1]
                            group_code = group_code.split('  ')[0:2]
                            group_code = ''.join(group_code)
                            pos += 1
                            group_code_list.append(group_code)
                        else:
                            group_code_list.append(None)
                    group_code = ', '.join(group_code_list)
                    customer_name = ', '.join(customer_name_list)
                    origin_map.loc[:, 'commodity'] = group_code
                    origin_map.loc[:, 'customer_name'] = customer_name
                    origin_map.loc[:, 'remarks'] = remarks[bullet]
                    origin_map.loc[:, 'bulletin'] = bullet_no[bullet_index]
                    if note_charges_dict[bullet][0] is not None:
                        note_ch_df = pd.DataFrame(note_charges_dict[bullet])
                        note_ch_df.fillna(method='ffill', inplace=True)
                        note_ch_df.fillna(method='bfill', inplace=True)
                        col = list(note_ch_df.columns)
                        origin_map = pd.concat([origin_map.reset_index(drop=True), note_ch_df[:1]], axis=1)
                        for col_ in col:
                            origin_map[col_].fillna(method='ffill', inplace=True)
                        col = list(origin_map.columns)
                        for i in range(len(col)):
                            if str(col[i]).find('yes') != -1:
                                regex_2 = r'_yes_(.+?)$'
                                col_name = re.findall(regex_2, col[i])

                                if col_name[0].strip() == 'LOCAL':
                                    col_name[0] = 'USWC'
                                elif col_name[0].strip() == 'RLOC':
                                    col_name[0] = 'USEC'
                                origin_map.loc[~(origin_map[0] == col_name[0]), col[i]] = ''
                                renamecol = '_yes_' + col_name[0]
                                col_rename1 = col[i].replace(renamecol, '')
                                origin_map.rename(columns={col[i]: col_rename1}, inplace=True)
                    origin_map.reset_index(inplace=True, drop=True)
                    origin_map.drop([0, 1], axis=0, inplace=True)

                    """ TO Check with other  files """
                    if origin_map[1].isna().all():
                        origin_map.drop([1], axis=1, inplace=True)
                    if origin_map[2].isna().all():
                        origin_map.drop([2], axis=1, inplace=True)

                    maped.append(origin_map)
                bullet += 1
                bullet_index += 1
            return maped

        def org_arb(self, start, end, duration, amd_no):
            if (end is None) or (start is None) or (end[0] - start[0]) < 2:
                return pd.DataFrame()
            origin_arb_df = self.df[start[0] + 1:end[0]].copy(deep=True)
            origin_arb_df.reset_index(drop=True, inplace=True)
            start_index = origin_arb_df.loc[origin_arb_df[0] == 'Location'].index[0]
            origin_arb_df.columns = origin_arb_df.iloc[start_index]
            origin_arb_df = origin_arb_df[1:].copy()
            origin_arb_df = origin_arb_df.replace('', nan)
            origin_arb_df.dropna(axis=1, inplace=True, how='all')
            origin_arb_df.dropna(axis=0, inplace=True, how='all')
            origin_arb_df.drop(origin_arb_df[origin_arb_df['Location'] == 'Location'].index, inplace=True)
            origin_arb_df['currency'] = 'USD'
            origin_arb_df['charges_leg'] = 'L2'
            origin_arb_df['charges'] = 'origin arbitrary charges'
            origin_arb_df['start_date'] = duration['start_date']
            origin_arb_df['expiry_date'] = duration['end_date']
            origin_arb_df['at'] = 'origin'
            origin_arb_df.rename(columns={"20'": "20GP", "40'": '40GP', "4H'": "40HC", "45'": '45HC', '4HRD': '40HCNOR',
                                          'Location': 'icd', 'Base Port': 'to', 'Term': 'service_type',
                                          'Type': 'cargo_type'}, inplace=True)
            origin_arb_df['icd'].fillna(method='ffill', inplace=True)
            origin_arb_df['to'].fillna(method='ffill', inplace=True)
            origin_arb_df['service_type'].fillna(method='ffill', inplace=True)
            origin_arb_df['Amendment no.'] = amd_no
            origin_arb_df.loc[(origin_arb_df['Direct'] == 'Y'), 'Loop'] = 'Direct'
            origin_arb_df.drop(['Via.', 'Mode', 'Gate Port', 'Cargo', 'Direct'], axis=1, inplace=True)
            origin_arb_df.rename(columns={"Loop'": "loop", }, inplace=True)
            origin_arb_df.rename(columns={"Loop": "loop", }, inplace=True)
            origin_arb_df['unique'] = ''
            return origin_arb_df

        @staticmethod
        def map_via(freight_df):

            def compare(x):
                unique_destination = []
                destination_port_un = freight_df[point + '_icd'].unique().tolist()
                for val in destination_port_un:
                    val = val.split('\n')
                    unique_destination += val
                unique_destination = list(set(unique_destination))
                port = []
                for value in x:
                    for un in unique_destination:
                        if value.find(un) != -1:
                            port.append(value)
                            break
                return ''.join(port)

            for point in ['origin', 'destination']:
                freight_df[point + '_port'] = freight_df['via'].str.split('\n').map(compare)

            return freight_df

        @staticmethod
        def format_output(df_freight, org_arb_df, df_table=pd.DataFrame()):
            output = {'Freight': df_freight, 'Arbitrary Charges': org_arb_df, 'Freight_table': df_table}
            return output

        def capture(self):
            sections = self.get_sections()
            duration = self.get_duration(sections[3], sections[4])
            self.comm_desc = self.get_comm_desc(sections[5], sections[6])
            self.geo_code = self.get_geo_code(sections[7], sections[8])
            bullet_no = self.get_bullet_no(sections[1])
            self.amd_no = self.get_amd_no(file_code='HQ21')
            df_list = self.get_table_from_bulletin(sections[1], sections[2])
            remarks, start_date_list, end_date_list, note_charges_dict = self.get_notes(sections[1], df_list, duration,
                                                                                        sections[2])
            maped = self.map_data_to_table(df_list, remarks, start_date_list, end_date_list, note_charges_dict,
                                           bullet_no)
            org_arb_df = self.org_arb(sections[10], sections[11], duration, self.amd_no)
            self.captured_output = self.format_output(maped, org_arb_df)

        @staticmethod
        def rate_table_clean(freight_df):
            freight_df.columns = ['destination_icd', 'origin_port', 'service_type', 'destination_port',
                                  'cargo_type', 'cargo',
                                  '20GP', '40GP', '40HC', '45HC', '40HCNOR', 'Application', 'inclusions',
                                  'commodity', 'remarks']
            freight_df['inclusions'] = freight_df['inclusions'].str.replace('Inclusive ', '')
            return freight_df

        def clean(self):
            df_output = pd.DataFrame()
            freight_db = self.captured_output['Freight']
            freight_df = self.captured_output['Freight_table']
            for df_clean in freight_db:
                for code in self.geo_code:
                    _code = ';'.join(self.geo_code[code])
                    df_clean.replace(code, _code, inplace=True)
                df_clean_tmp = df_clean.iloc[:, 19:].copy(deep=True)
                df_clean = df_clean.iloc[:, :19].copy(deep=True)

                df_clean.columns = ['destination_icd', 'service_type', 'destination_port', 'cargo_type', 'cargo',
                                    '20GP', '40GP', '40HC', '45HC', '40HCNOR', 'Application', 'inclusions',
                                    'origin_port', 'start_date', 'expiry_date',
                                    'commodity', 'customer_name', 'remarks', 'bulletin']
                # df_clean.loc[(df_clean['destination_icd'] == df_clean['destination_port']), 'destination_port'] = ''
                comm_desc_list = []
                comm_map = {"G0300 G03": "CONSOLIDATION (MIXED WITH OR WITHOUT GARMENT AND TEXTILE",
                            "G0400 G04": "COSMETIC GOODS; PACKAGING MATERIALS; DISPOSABLE PRODUCTS; PVC TILES;"
                                         "RUBBER PRODUCTS; CASES,TRUNKS, ETC ; GDSM (EXCLUDING GARMENT AND CONSOLIDATION)"}

                df_clean['commodity'].replace(comm_map, regex=True, inplace=True)

                com_1 = df_clean.iloc[0, 15]
                com_1 = com_1.split(',')
                com_1 = [i.strip() for i in com_1]
                found = 0
                for commodity in com_1:
                    if len(commodity.split(' ')) > 1:
                        commodity_code = commodity.split(' ')[1]
                        for code in self.comm_desc:
                            if code == commodity_code:
                                comm_desc_list.append(';'.join(self.comm_desc[code]))
                                found = 1
                if found == 1:
                    df_clean['commodity'] = ' '.join(comm_desc_list)

                df_clean.loc[~(df_clean['inclusions'].str.contains('Inclusive', na=False)), 'inclusions'] = ''
                df_clean['inclusions'] = df_clean['inclusions'].str.replace('Inclusive ', '')
                df_clean['origin_port'] = df_clean['origin_port'].str.replace('\n', ';')
                df_clean_tmp.replace('', nan, inplace=True)
                df_clean_tmp.dropna(how='all', axis=1, inplace=True)

                """LOCAL and RLOC removal processing"""
                if "PEK_PEK_20'_yes_RLOC" in df_clean_tmp and "PEK_PEK_20'_yes_LOCAL " in df_clean_tmp:
                    df_clean_tmp["PEK_PEK_20'_yes_LOCAL "].fillna(df_clean_tmp["PEK_PEK_20'_yes_RLOC"], inplace=True)
                    df_clean_tmp["PEK_PEK_40'_yes_LOCAL "].fillna(df_clean_tmp["PEK_PEK_40'_yes_RLOC"], inplace=True)
                    df_clean_tmp["PEK_PEK_4H_yes_LOCAL "].fillna(df_clean_tmp["PEK_PEK_4H_yes_RLOC"], inplace=True)
                    df_clean_tmp["PEK_PEK_45'_yes_LOCAL "].fillna(df_clean_tmp["PEK_PEK_45'_yes_RLOC"], inplace=True)
                    df_clean_tmp.drop(columns=["PEK_PEK_20'_yes_RLOC", "PEK_PEK_40'_yes_RLOC", "PEK_PEK_4H_yes_RLOC",
                                               "PEK_PEK_45'_yes_RLOC"], axis=1, inplace=True)

                df_clean = pd.concat([df_clean, df_clean_tmp], axis=1, ignore_index=False)
                df_clean.rename(
                    columns={"PEK_20'": '20GP_PEK', "PEK_40'": '40GP_PEK', "PEK_20": '20GP_PEK', "PEK_40": "40GP_PEK",
                             "PEK_45": "45HC_PEK",
                             'PEK_4H': '40HC_PEK', "PEK_45'": "45HC_PEK",
                             "CGS_20'": '20GP_CGS', "CGS_40'": '40GP_CGS', "CGS_20": '20GP_CGS', "CGS_40": '40GP_CGS',
                             'CGS_45': '45HC_CGS', 'CGS_4H': '40HC_CGS', "CGS_45'": "45HC_CGS",
                             "BUC_20'": '20GP_BUC', "BUC_40'": '40GP_BUC', "BUC_20": '20GP_BUC', "BUC_40": '40GP_BUC',
                             'BUC_45': '45HC_BUC', 'BUC_4H': '40HC_BUC', "BUC_45'": "45HC_BUC",
                             "FRC_20'": '20GP_FRC', "FRC_40'": '40GP_FRC', "FRC_20": '20GP_FRC', "FRC_40": '40GP_FRC',
                             'FRC_45': '45HC_FRC', 'FRC_4H': '40HC_FRC', "FRC_45'": "45HC_FRC",
                             "PEK_PEK_20'": "20GP_PEK", "PEK_PEK_40'": '40GP_PEK', 'PEK_PEK_4H': '40HC_PEK',
                             "PEK_PEK_45'": '45HC_PEK', "PEK_PEK_20'_yes_LOCAL ": "20GP_PEK",
                             "PEK_PEK_40'_yes_LOCAL ": '40GP_PEK', 'PEK_PEK_4H_yes_LOCAL ': '40HC_PEK',
                             "PEK_PEK_45'_yes_LOCAL ": '45HC_PEK', "PEK_PEK_20'_yes_RLOC": '20GP_PEK',
                             "PEK_PEK_40'_yes_RLOC": '40GP_PEK', 'PEK_PEK_4H_yes_RLOC': '40HC_PEK',
                             "PEK_PEK_45'_yes_RLOC": "45HC_PEK"
                             }, inplace=True)

                df_output = pd.concat([df_output, df_clean], axis=0, ignore_index=True)
            df_output = pd.concat([df_output, freight_df], axis=0, ignore_index=True)
            df_output.loc[(df_output['Application'].str.contains('ARB', na=False)), 'origin_arbitrary_allowed'] = 'Yes'
            df_output.loc[~(df_output['Application'].str.contains('ARB', na=False)), 'origin_arbitrary_allowed'] = 'No'
            df_output['origin_port'] = df_output['origin_port'].str.replace(r"\(CY\)", '')
            df_output.drop(df_output[df_output['destination_icd'] == 'Destination'].index, inplace=True)
            df_output['currency'] = 'USD'
            df_output['charges_leg'] = 'L3'
            df_output['charges'] = 'Basic Ocean Freight'
            df_output['Amendment no.'] = self.amd_no
            df_output['unique'] = ''
            self.cleaned_output = {'Freight': df_output, 'Arbitrary Charges': self.captured_output['Arbitrary Charges']}


class Flexport_HMM_v1(HMM_v1):
    """Flexport_HMM_v1 inherited from HMM v1"""

    class _UsaAsia(HMM_v1._UsaAsia):

        def capture(self):
            sections_to_check = ['RATE FOR BULLET NO.', 'Special Note', 'DURATION', 'SIGNATURE DATE/',
                                 '3. COMMODITY', '4. MINIMUM QUANTITY COMMITMENT', 'CONTRACT RATES AND CHARGES',
                                 'Table Rate', 'NOTE FOR BULLET', 'ORIGIN ARBITRARY', 'Destination Arbitrary',
                                 'Origin Inland Add-on', 'Destination Inland Add-on', '6-8. G.O.H Surcharge']

            sections = self.get_sections(sections_to_check)
            duration = self.get_duration(sections[3], sections[4])
            self.comm_desc = self.get_comm_desc(sections[5], sections[6])
            self.geo_code = self.get_geo_code(sections[7], sections[8])
            bullet_no = self.get_bullet_no(sections[1])
            self.amd_no = self.get_amd_no(file_code='HQ21')
            df_list = self.get_table_from_bulletin(sections[1], sections[2])
            remarks, start_date_list, end_date_list, note_charges_dict = self.get_notes(sections[1], df_list, duration,
                                                                                        sections[2])
            maped = self.map_data_to_table(df_list, remarks, start_date_list, end_date_list, note_charges_dict,
                                           bullet_no)
            org_arb_df = self.org_arb(sections[10], sections[11], duration, self.amd_no)
            self.captured_output = self.format_output(maped, org_arb_df)

        def clean(self):
            df_output = pd.DataFrame()
            freight_db = self.captured_output['Freight']
            for df_clean in freight_db:
                for code in self.geo_code:
                    _code = ';'.join(self.geo_code[code])
                    df_clean.replace(code, _code, inplace=True)
                df_clean_tmp = df_clean.iloc[:, 19:].copy(deep=True)
                df_clean = df_clean.iloc[:, :19].copy(deep=True)

                df_clean.columns = ['destination_icd', 'service_type', 'destination_port', 'cargo_type', 'cargo',
                                    '20GP', '40GP', '40HC', '45HC', '40HCNOR', 'Application', 'inclusions',
                                    'origin_port', 'start_date', 'expiry_date',
                                    'commodity', 'customer_name', 'remarks', 'bulletin']
                # df_clean.loc[(df_clean['destination_icd'] == df_clean['destination_port']), 'destination_port'] = ''
                comm_desc_list = []
                comm_map = {"G0300 G03": "CONSOLIDATION (MIXED WITH OR WITHOUT GARMENT AND TEXTILE",
                            "G0400 G04": "COSMETIC GOODS; PACKAGING MATERIALS; DISPOSABLE PRODUCTS; PVC TILES;"
                                         "RUBBER PRODUCTS; CASES,TRUNKS, ETC ; GDSM (EXCLUDING GARMENT AND CONSOLIDATION)"}

                df_clean['commodity'].replace(comm_map, regex=True, inplace=True)

                com_1 = df_clean.iloc[0, 15]
                com_1 = com_1.split(',')
                com_1 = [i.strip() for i in com_1]
                found = 0
                for commodity in com_1:
                    if len(commodity.split(' ')) > 1:
                        commodity_code = commodity.split(' ')[1]
                        for code in self.comm_desc:
                            if code == commodity_code:
                                comm_desc_list.append(';'.join(self.comm_desc[code]))
                                found = 1
                if found == 1:
                    df_clean['commodity'] = ' '.join(comm_desc_list)

                df_clean.loc[~(df_clean['inclusions'].str.contains('Inclusive', na=False)), 'inclusions'] = ''
                df_clean['inclusions'] = df_clean['inclusions'].str.replace('Inclusive ', '')
                df_clean['origin_port'] = df_clean['origin_port'].str.replace('\n', ';')
                df_clean_tmp.replace('', nan, inplace=True)
                df_clean_tmp.dropna(how='all', axis=1, inplace=True)

                """LOCAL and RLOC removal processing"""
                if "PEK_PEK_20'_yes_RLOC" in df_clean_tmp and "PEK_PEK_20'_yes_LOCAL " in df_clean_tmp:
                    df_clean_tmp["PEK_PEK_20'_yes_LOCAL "].fillna(df_clean_tmp["PEK_PEK_20'_yes_RLOC"], inplace=True)
                    df_clean_tmp["PEK_PEK_40'_yes_LOCAL "].fillna(df_clean_tmp["PEK_PEK_40'_yes_RLOC"], inplace=True)
                    df_clean_tmp["PEK_PEK_4H_yes_LOCAL "].fillna(df_clean_tmp["PEK_PEK_4H_yes_RLOC"], inplace=True)
                    df_clean_tmp["PEK_PEK_45'_yes_LOCAL "].fillna(df_clean_tmp["PEK_PEK_45'_yes_RLOC"], inplace=True)
                    df_clean_tmp.drop(columns=["PEK_PEK_20'_yes_RLOC", "PEK_PEK_40'_yes_RLOC", "PEK_PEK_4H_yes_RLOC",
                                               "PEK_PEK_45'_yes_RLOC"], axis=1, inplace=True)

                df_clean = pd.concat([df_clean, df_clean_tmp], axis=1, ignore_index=False)
                df_clean.rename(
                    columns={"PEK_20'": '20GP_PEK', "PEK_40'": '40GP_PEK', "PEK_20": '20GP_PEK', "PEK_40": "40GP_PEK",
                             "PEK_45": "45HC_PEK",
                             'PEK_4H': '40HC_PEK', "PEK_45'": "45HC_PEK",
                             "CGS_20'": '20GP_CGS', "CGS_40'": '40GP_CGS', "CGS_20": '20GP_CGS', "CGS_40": '40GP_CGS',
                             'CGS_45': '45HC_CGS', 'CGS_4H': '40HC_CGS', "CGS_45'": "45HC_CGS",
                             "BUC_20'": '20GP_BUC', "BUC_40'": '40GP_BUC', "BUC_20": '20GP_BUC', "BUC_40": '40GP_BUC',
                             'BUC_45': '45HC_BUC', 'BUC_4H': '40HC_BUC', "BUC_45'": "45HC_BUC",
                             "FRC_20'": '20GP_FRC', "FRC_40'": '40GP_FRC', "FRC_20": '20GP_FRC', "FRC_40": '40GP_FRC',
                             'FRC_45': '45HC_FRC', 'FRC_4H': '40HC_FRC', "FRC_45'": "45HC_FRC",
                             "PEK_PEK_20'": "20GP_PEK", "PEK_PEK_40'": '40GP_PEK', 'PEK_PEK_4H': '40HC_PEK',
                             "PEK_PEK_45'": '45HC_PEK', "PEK_PEK_20'_yes_LOCAL ": "20GP_PEK",
                             "PEK_PEK_40'_yes_LOCAL ": '40GP_PEK', 'PEK_PEK_4H_yes_LOCAL ': '40HC_PEK',
                             "PEK_PEK_45'_yes_LOCAL ": '45HC_PEK', "PEK_PEK_20'_yes_RLOC": '20GP_PEK',
                             "PEK_PEK_40'_yes_RLOC": '40GP_PEK', 'PEK_PEK_4H_yes_RLOC': '40HC_PEK',
                             "PEK_PEK_45'_yes_RLOC": "45HC_PEK"
                             }, inplace=True)

                df_output = pd.concat([df_output, df_clean], axis=0, ignore_index=True)
            df_output.loc[(df_output['Application'].str.contains('ARB', na=False)), 'origin_arbitrary_allowed'] = 'Yes'
            df_output.loc[~(df_output['Application'].str.contains('ARB', na=False)), 'origin_arbitrary_allowed'] = 'No'
            df_output['origin_port'] = df_output['origin_port'].str.replace(r"\(CY\)", '')
            df_output.drop(df_output[df_output['destination_icd'] == 'Destination'].index, inplace=True)
            df_output['currency'] = 'USD'
            df_output['charges_leg'] = 'L3'
            df_output['charges'] = 'Basic Ocean Freight'
            df_output['Amendment no.'] = self.amd_no
            df_output['unique'] = ''
            self.cleaned_output = {'Freight': df_output, 'Arbitrary Charges': self.captured_output['Arbitrary Charges']}


class Ceva_HMM_USA(HMM_v1):
    """Ceva HMM inherited from HMM v1"""

    class _hmm_usa(HMM_v1._UsaAsia):

        def get_table(self, df_list):
            if self.df[self.df[0].str.contains(r"RATE FOR TABLE NO", na=False)].index.values.any():
                index_start = list(self.df[(self.df[0].str.contains(r"RATE FOR TABLE NO", na=False))].index)
                index_end = list(self.df[(self.df[0].str.contains(r"NOTE FOR TABLE NO", na=False))].index)
                index_note_end = list(self.df[(self.df[0].str.contains(r"6-2. Bullet Rate", na=False))].index)
                table_df = self.df[index_start[0]:index_end[0]].copy(deep=True)
                table_df.reset_index(drop=True, inplace=True)
                table_df.replace('', nan, inplace=True)
                table_df.dropna(how='all', axis=1, inplace=True)
                table_df[11].fillna(method='ffill', inplace=True)
                table_df.drop(columns=[9], inplace=True)
                commodity = table_df.iloc[table_df[(table_df[0] == 'COMMODITY')].index.values[0], 1]
                table_df['commodity'] = commodity
                table_df = table_df.loc[~(
                        table_df[0].str.contains('ORIGIN') | table_df[0].str.contains('Destination') | table_df[
                    0].str.contains('COMMODITY') | table_df[0].str.contains('RATE FOR TABLE'))]
                # table_df['customer_name'] = ''
                table_df['remarks'] = self.df.iloc[index_note_end[0] - 1, 1]

            return table_df

        def org_arb(self, start, end, duration, amd_no):
            if (end is None) or (start is None) or (end[0] - start[0]) < 2:
                return pd.DataFrame()
            origin_arb_df = self.df[start[0] + 1:end[0]].copy(deep=True)
            origin_arb_df.reset_index(drop=True, inplace=True)
            origin_arb_df.columns = origin_arb_df.iloc[0]
            origin_arb_df = origin_arb_df[1:].copy()
            origin_arb_df = origin_arb_df.replace('', nan)
            origin_arb_df.dropna(axis=1, inplace=True, how='all')
            origin_arb_df.dropna(axis=0, inplace=True, how='all')
            origin_arb_df['Location'].fillna(method='ffill', inplace=True)
            origin_arb_df['Term'].fillna(method='ffill', inplace=True)
            origin_arb_df['Base Port'].fillna(method='ffill', inplace=True)
            origin_arb_df.drop(origin_arb_df[origin_arb_df['Location'] == 'Location'].index, inplace=True)
            origin_arb_df['currency'] = 'USD'
            origin_arb_df['charges_leg'] = 'L2'
            origin_arb_df['charges'] = 'origin arbitrary charges'
            origin_arb_df['start_date'] = duration['start_date']
            origin_arb_df['expiry_date'] = duration['end_date']
            origin_arb_df['at'] = 'origin'
            origin_arb_df.rename(columns={"20'": "20GP", "40'": '40GP', "4H'": "40HC", "45'": '45HC', '4HRD': '40HCNOR',
                                          'Location': 'icd', 'Base Port': 'to', 'Term': 'service_type',
                                          'Cargo': 'cargo_type'}, inplace=True)
            origin_arb_df['icd'].fillna(method='ffill', inplace=True)
            origin_arb_df['to'].fillna(method='ffill', inplace=True)
            origin_arb_df['service_type'].fillna(method='ffill', inplace=True)
            origin_arb_df['Amendment no.'] = amd_no
            if 'Direct' in origin_arb_df:
                origin_arb_df.loc[(origin_arb_df['Direct'] == 'Y'), 'Loop'] = 'Direct'
                try:
                    origin_arb_df.drop(['Via.', 'Mode', 'Sub\n-Trade', 'Gate Port', 'Cargo', 'Direct'], axis=1,
                                       inplace=True)
                except KeyError:
                    origin_arb_df.drop(['Mode', 'Sub\n-Trade', 'Gate Port', 'Direct'], axis=1,
                                       inplace=True)
            origin_arb_df.rename(columns={"Loop'": "loop", }, inplace=True)
            origin_arb_df['unique'] = ''
            return origin_arb_df

        def dest_arb(self, start, end, duration, amd_no):
            dest_arb = self.org_arb(start, end, duration, amd_no)
            dest_arb['charges_leg'] = 'L4'
            dest_arb['charges'] = 'destination arbitrary charges'
            dest_arb['at'] = 'destination'
            return dest_arb

        @staticmethod
        def format_output(df_freight, org_arb_df, df_table):
            output = {'Freight': df_freight, 'Arbitrary Charges': org_arb_df, 'Freight_table': df_table}
            return output

        def capture(self):

            sections_to_check = ['RATE FOR BULLET NO.', 'Special Note', 'DURATION', 'SIGNATURE DATE/',
                                 '3. COMMODITY', '4. MINIMUM QUANTITY COMMITMENT', 'CONTRACT RATES AND CHARGES',
                                 'Table Rate', 'NOTE FOR BULLET', 'ORIGIN ARBITRARY', 'Destination Arbitrary',
                                 'Origin Inland Add-on', 'Destination Inland Add-on', '6-8. G.O.H Surcharge']

            sections = self.get_sections(sections_to_check)
            duration = self.get_duration(sections[3], sections[4])
            self.comm_desc = self.get_comm_desc(sections[5], sections[6])
            self.geo_code = self.get_geo_code(sections[7], sections[8])
            bullet_no = self.get_bullet_no(sections[1])
            self.amd_no = self.get_amd_no(file_code='US20')
            df_list = self.get_table_from_bulletin(sections[1], sections[2])
            df_table = self.get_table(df_list)
            remarks, start_date_list, end_date_list, note_charges_dict = self.get_notes(sections[1], df_list, duration,
                                                                                        sections[2])
            freight_df = self.map_data_to_table(df_list, remarks, start_date_list, end_date_list, note_charges_dict,
                                                bullet_no)
            org_arb_df = self.org_arb(sections[10], sections[11], duration, self.amd_no)
            org_inland_df = self.org_arb(sections[12], sections[13], duration, self.amd_no)
            dest_arb_df = self.dest_arb(sections[11], sections[12], duration, self.amd_no)
            arb_df = pd.concat([org_arb_df, dest_arb_df, org_inland_df], axis=0, ignore_index=True)
            self.captured_output = self.format_output(freight_df, arb_df, df_table)

        def clean(self):
            df_output = pd.DataFrame()
            freight_db = self.captured_output['Freight']
            freight_df = self.captured_output['Freight_table']

            freight_df = self.rate_table_clean(freight_df)
            for df_clean in freight_db:
                df_clean_tmp = df_clean.iloc[:, 19:].copy(deep=True)
                df_clean = df_clean.iloc[:, :19].copy(deep=True)

                df_clean.columns = ['destination_icd', 'service_type', 'destination_port', 'cargo_type', 'cargo',
                                    '20GP', '40GP', '40HC', '45HC', '40HCNOR', 'Application', 'inclusions',
                                    'origin_port', 'start_date', 'expiry_date',
                                    'commodity', 'customer_name', 'remarks', 'bulletin']
                # df_clean.loc[(df_clean['destination_icd'] == df_clean['destination_port']), 'destination_port'] = ''
                comm_desc_list = []
                comm_map = {"G0300 G03": "CONSOLIDATION (MIXED WITH OR WITHOUT GARMENT AND TEXTILE",
                            "G0400 G04": "COSMETIC GOODS; PACKAGING MATERIALS; DISPOSABLE PRODUCTS; PVC TILES;"
                                         "RUBBER PRODUCTS; CASES,TRUNKS, ETC ; GDSM (EXCLUDING GARMENT AND "
                                         "CONSOLIDATION)"}

                df_clean['commodity'].replace(comm_map, regex=True, inplace=True)

                com_1 = df_clean.iloc[0, 15]
                com_1 = com_1.split(',')
                com_1 = [i.strip() for i in com_1]
                found = 0
                for commodity in com_1:
                    if len(commodity.split(' ')) > 1:
                        commodity_code = commodity.split(' ')[1]
                        for code in self.comm_desc:
                            if code == commodity_code:
                                comm_desc_list.append(';'.join(self.comm_desc[code]))
                                found = 1
                if found == 1:
                    df_clean['commodity'] = ' '.join(comm_desc_list)

                df_clean.loc[~(df_clean['inclusions'].str.contains('Inclusive', na=False)), 'inclusions'] = ''
                df_clean['inclusions'] = df_clean['inclusions'].str.replace('Inclusive ', '')
                df_clean['origin_port'] = df_clean['origin_port'].str.replace('\n', ';')
                df_clean_tmp.replace('', nan, inplace=True)
                df_clean_tmp.dropna(how='all', axis=1, inplace=True)

                """LOCAL and RLOC removal processing"""
                if "PEK_PEK_20'_yes_RLOC" in df_clean_tmp and "PEK_PEK_20'_yes_LOCAL " in df_clean_tmp:
                    df_clean_tmp["PEK_PEK_20'_yes_LOCAL "].fillna(df_clean_tmp["PEK_PEK_20'_yes_RLOC"], inplace=True)
                    df_clean_tmp["PEK_PEK_40'_yes_LOCAL "].fillna(df_clean_tmp["PEK_PEK_40'_yes_RLOC"], inplace=True)
                    df_clean_tmp["PEK_PEK_4H_yes_LOCAL "].fillna(df_clean_tmp["PEK_PEK_4H_yes_RLOC"], inplace=True)
                    df_clean_tmp["PEK_PEK_45'_yes_LOCAL "].fillna(df_clean_tmp["PEK_PEK_45'_yes_RLOC"], inplace=True)
                    df_clean_tmp.drop(columns=["PEK_PEK_20'_yes_RLOC", "PEK_PEK_40'_yes_RLOC", "PEK_PEK_4H_yes_RLOC",
                                               "PEK_PEK_45'_yes_RLOC"], axis=1, inplace=True)

                df_clean = pd.concat([df_clean, df_clean_tmp], axis=1, ignore_index=False)
                df_clean.rename(
                    columns={"PEK_20'": '20GP_PEK', "PEK_40'": '40GP_PEK', "PEK_20": '20GP_PEK', "PEK_40": "40GP_PEK",
                             "PEK_45": "45HC_PEK",
                             'PEK_4H': '40HC_PEK', "PEK_45'": "45HC_PEK",
                             "CGS_20'": '20GP_CGS', "CGS_40'": '40GP_CGS', "CGS_20": '20GP_CGS', "CGS_40": '40GP_CGS',
                             'CGS_45': '45HC_CGS', 'CGS_4H': '40HC_CGS', "CGS_45'": "45HC_CGS",
                             "BUC_20'": '20GP_BUC', "BUC_40'": '40GP_BUC', "BUC_20": '20GP_BUC', "BUC_40": '40GP_BUC',
                             'BUC_45': '45HC_BUC', 'BUC_4H': '40HC_BUC', "BUC_45'": "45HC_BUC",
                             "FRC_20'": '20GP_FRC', "FRC_40'": '40GP_FRC', "FRC_20": '20GP_FRC', "FRC_40": '40GP_FRC',
                             'FRC_45': '45HC_FRC', 'FRC_4H': '40HC_FRC', "FRC_45'": "45HC_FRC",
                             "PEK_PEK_20'": "20GP_PEK", "PEK_PEK_40'": '40GP_PEK', 'PEK_PEK_4H': '40HC_PEK',
                             "PEK_PEK_45'": '45HC_PEK', "PEK_PEK_20'_yes_LOCAL ": "20GP_PEK",
                             "PEK_PEK_40'_yes_LOCAL ": '40GP_PEK', 'PEK_PEK_4H_yes_LOCAL ': '40HC_PEK',
                             "PEK_PEK_45'_yes_LOCAL ": '45HC_PEK', "PEK_PEK_20'_yes_RLOC": '20GP_PEK',
                             "PEK_PEK_40'_yes_RLOC": '40GP_PEK', 'PEK_PEK_4H_yes_RLOC': '40HC_PEK',
                             "PEK_PEK_45'_yes_RLOC": "45HC_PEK"
                             }, inplace=True)

                df_output = pd.concat([df_output, df_clean], axis=0, ignore_index=True)
            df_output = pd.concat([df_output, freight_df], axis=0, ignore_index=True)
            for code in self.geo_code:
                _code = ';'.join(self.geo_code[code])
                df_output.replace(code, _code, inplace=True, regex=True)
            df_output.loc[(df_output['Application'].str.contains('ARB', na=False)), 'origin_arbitrary_allowed'] = 'Yes'
            df_output.loc[~(df_output['Application'].str.contains('ARB', na=False)), 'origin_arbitrary_allowed'] = 'No'
            df_output['origin_port'] = df_output['origin_port'].str.replace(r"\(CY\)", '')
            df_output.drop(df_output[df_output['destination_icd'] == 'Destination'].index, inplace=True)
            df_output['currency'] = 'USD'
            df_output['charges_leg'] = 'L3'
            df_output['charges'] = 'Basic Ocean Freight'
            df_output['Amendment no.'] = self.amd_no
            df_output['unique'] = ''
            df_output.rename(columns={"origin_port": "origin_icd", "destination_port": "via"}, inplace=True)
            df_output = self.map_via(df_output)
            self.cleaned_output = {'Freight': df_output, 'Arbitrary Charges': self.captured_output['Arbitrary Charges']}


class Ceva_Hmm_Latam(HMM_v1):
    """Ceva_HMM_Latam inherited from HMM v1"""

    class _UsaAsia(HMM_v1._UsaAsia):

        def check_input(self):

            check_errors = []
            if not self.df[0].str.contains('RATE FOR BULLET NO.', na=False).any():
                check_errors.append("RATE FOR BULLET NO. should be present in first Column.")

            if not self.df[0].str.contains('DURATION', na=False).index.any():
                check_errors.append("Duration section should be present.")

            if not self.df[0].str.contains('Special Note', na=False).any():
                check_errors.append("Special Note should be present.")

            if not self.df[26].str.contains("", na=False).any():
                check_errors.append(r"Ammendmant no. should be present.")

            if not self.df[0].str.contains('CONTRACT RATES AND CHARGES', na=False).any():
                check_errors.append("Geo location abrevation should be present.")
            if check_errors:
                raise InputValidationError(check_errors)

        def get_duration(self, start, end):
            duration = {}
            duration_df = self.df[start[0]:end[0]].copy(deep=True)
            duration_df = duration_df.replace('', nan)
            duration_df.dropna(axis=1, inplace=True, how='all')
            duration_df.dropna(axis=0, inplace=True, how='all')
            duration_df.reset_index(drop=True, inplace=True)
            start_date = duration_df.iloc[5, 0]
            end_date = duration_df.iloc[2, 0]

            end_date = datetime.strptime(re.findall(r"through\s{1,4}(.*)", end_date)[0], "%B %d, %Y").date()
            start_date = datetime.strptime(re.findall(r"from\s{1,4}(.*)", start_date)[0], "%B %d, %Y").date()

            duration['start_date'] = start_date
            duration['end_date'] = end_date
            return duration

        def get_amd_no(self, start):
            return re.findall(r"AMENDMENT(.*)", self.df.iloc[start[0], 0])[0]

        def capture(self):

            sections_to_check = ['RATE FOR BULLET NO.', 'Special Note', 'DURATION', 'COMMODITY GROUP',
                                 'CONTRACT RATES AND CHARGES', '1. Rate', 'NOTE FOR BULLET', 'Origin Arbitrary',
                                 'Destination Arbitrary', 'Origin Inland Add-on', 'Destination Inland Add-on',
                                 'SIGNATURE DATE', 'EFFECTIVE DATE OF AMENDMENT']

            sections = self.get_sections(sections_to_check)
            duration = self.get_duration(sections[3], sections[4])
            self.geo_code = self.get_geo_code(sections[5], sections[6])
            bullet_no = self.get_bullet_no(sections[1])
            self.amd_no = self.get_amd_no(sections[13])
            df_list = self.get_table_from_bulletin(sections[1], sections[2])
            remarks, start_date_list, end_date_list, note_charges_dict = self.get_notes(sections[1], df_list, duration,
                                                                                        sections[2])
            maped = self.map_data_to_table(df_list, remarks, start_date_list, end_date_list, note_charges_dict,
                                           bullet_no)
            org_arb_df = self.org_arb(sections[8], sections[9], duration, self.amd_no)
            self.captured_output = self.format_output(maped, org_arb_df)

        def clean(self):
            df_output = pd.DataFrame()
            freight_db = self.captured_output['Freight']
            for df_clean in freight_db:
                for code in self.geo_code:
                    _code = ';'.join(self.geo_code[code])
                    df_clean.replace(code, _code, inplace=True)
                df_clean_tmp = df_clean.iloc[:, 19:].copy(deep=True)
                df_clean = df_clean.iloc[:, :19].copy(deep=True)

                df_clean.columns = ['destination_icd', 'service_type', 'destination_port', 'cargo_type', 'cargo',
                                    '20GP', '40GP', '40HC', '45HC', '40HCNOR', 'Application', 'inclusions',
                                    'origin_port', 'start_date', 'expiry_date',
                                    'commodity', 'customer_name', 'remarks', 'bulletin']
                # df_clean.loc[(df_clean['destination_icd'] == df_clean['destination_port']), 'destination_port'] = ''
                comm_desc_list = []
                comm_map = {"G0300 G03": "CONSOLIDATION (MIXED WITH OR WITHOUT GARMENT AND TEXTILE",
                            "G0400 G04": "COSMETIC GOODS; PACKAGING MATERIALS; DISPOSABLE PRODUCTS; PVC TILES;"
                                         "RUBBER PRODUCTS; CASES,TRUNKS, ETC ; GDSM (EXCLUDING GARMENT AND CONSOLIDATION)"}

                df_clean['commodity'].replace(comm_map, regex=True, inplace=True)

                df_clean.loc[~(df_clean['inclusions'].str.contains('Inclusive', na=False)), 'inclusions'] = ''
                df_clean['inclusions'] = df_clean['inclusions'].str.replace('Inclusive ', '')
                df_clean['origin_port'] = df_clean['origin_port'].str.replace('\n', ';')
                df_clean_tmp.replace('', nan, inplace=True)
                df_clean_tmp.dropna(how='all', axis=1, inplace=True)

                """LOCAL and RLOC removal processing"""
                if "PEK_PEK_20'_yes_RLOC" in df_clean_tmp and "PEK_PEK_20'_yes_LOCAL " in df_clean_tmp:
                    df_clean_tmp["PEK_PEK_20'_yes_LOCAL "].fillna(df_clean_tmp["PEK_PEK_20'_yes_RLOC"], inplace=True)
                    df_clean_tmp["PEK_PEK_40'_yes_LOCAL "].fillna(df_clean_tmp["PEK_PEK_40'_yes_RLOC"], inplace=True)
                    df_clean_tmp["PEK_PEK_4H_yes_LOCAL "].fillna(df_clean_tmp["PEK_PEK_4H_yes_RLOC"], inplace=True)
                    df_clean_tmp["PEK_PEK_45'_yes_LOCAL "].fillna(df_clean_tmp["PEK_PEK_45'_yes_RLOC"], inplace=True)
                    df_clean_tmp.drop(columns=["PEK_PEK_20'_yes_RLOC", "PEK_PEK_40'_yes_RLOC", "PEK_PEK_4H_yes_RLOC",
                                               "PEK_PEK_45'_yes_RLOC"], axis=1, inplace=True)

                df_clean = pd.concat([df_clean, df_clean_tmp], axis=1, ignore_index=False)
                df_clean.rename(
                    columns={"PEK_20'": '20GP_PEK', "PEK_40'": '40GP_PEK', "PEK_20": '20GP_PEK', "PEK_40": "40GP_PEK",
                             "PEK_45": "45HC_PEK",
                             'PEK_4H': '40HC_PEK', "PEK_45'": "45HC_PEK",
                             "CGS_20'": '20GP_CGS', "CGS_40'": '40GP_CGS', "CGS_20": '20GP_CGS', "CGS_40": '40GP_CGS',
                             'CGS_45': '45HC_CGS', 'CGS_4H': '40HC_CGS', "CGS_45'": "45HC_CGS",
                             "BUC_20'": '20GP_BUC', "BUC_40'": '40GP_BUC', "BUC_20": '20GP_BUC', "BUC_40": '40GP_BUC',
                             'BUC_45': '45HC_BUC', 'BUC_4H': '40HC_BUC', "BUC_45'": "45HC_BUC",
                             "FRC_20'": '20GP_FRC', "FRC_40'": '40GP_FRC', "FRC_20": '20GP_FRC', "FRC_40": '40GP_FRC',
                             'FRC_45': '45HC_FRC', 'FRC_4H': '40HC_FRC', "FRC_45'": "45HC_FRC",
                             "PEK_PEK_20'": "20GP_PEK", "PEK_PEK_40'": '40GP_PEK', 'PEK_PEK_4H': '40HC_PEK',
                             "PEK_PEK_45'": '45HC_PEK', "PEK_PEK_20'_yes_LOCAL ": "20GP_PEK",
                             "PEK_PEK_40'_yes_LOCAL ": '40GP_PEK', 'PEK_PEK_4H_yes_LOCAL ': '40HC_PEK',
                             "PEK_PEK_45'_yes_LOCAL ": '45HC_PEK', "PEK_PEK_20'_yes_RLOC": '20GP_PEK',
                             "PEK_PEK_40'_yes_RLOC": '40GP_PEK', 'PEK_PEK_4H_yes_RLOC': '40HC_PEK',
                             "PEK_PEK_45'_yes_RLOC": "45HC_PEK"
                             }, inplace=True)

                df_output = pd.concat([df_output, df_clean], axis=0, ignore_index=True)
            df_output.loc[(df_output['Application'].str.contains('ARB', na=False)), 'origin_arbitrary_allowed'] = 'Yes'
            df_output.loc[~(df_output['Application'].str.contains('ARB', na=False)), 'origin_arbitrary_allowed'] = 'No'
            df_output['origin_port'] = df_output['origin_port'].str.replace(r"\(CY\)", '')
            df_output.drop(df_output[df_output['destination_icd'] == 'Destination'].index, inplace=True)
            df_output['currency'] = 'USD'
            df_output['charges_leg'] = 'L3'
            df_output['charges'] = 'Basic Ocean Freight'
            df_output['Amendment no.'] = self.amd_no
            df_output['unique'] = ''
            self.cleaned_output = {'Freight': df_output, 'Arbitrary Charges': self.captured_output['Arbitrary Charges']}


class Ceva_HMM_EMEA(BaseTemplate):
    class _Head(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_meta_data(self):
            meta_data = self.df.iloc[:, [1, 2, 4, 5, 6]]
            meta_data = meta_data.drop(1, axis=0)
            return meta_data

        def capture(self):
            meta_data = self.get_meta_data()
            self.captured_output = meta_data

        def clean(self):
            self.cleaned_output = self.captured_output

    class _Freight(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_freight_table(self):
            freight_df = self.df.copy()
            col = freight_df.iloc[0].to_list()
            index = [index for index, i in enumerate(col) if i == 'Including Surcharge']
            inclusions = freight_df[index]
            inclusions = inclusions.drop([0, 1], axis=0)
            inclusions = inclusions.replace('', nan)
            inclusions.dropna(how='all', axis=1, inplace=True)
            inclusions['inclusions'] = ''
            index1 = []
            for col_index in index:
                try:
                    inclusions['inclusions'] += inclusions[col_index] + ';'
                    index1.append(col_index)
                except KeyError:
                    pass
            freight_df['inclusions'] = inclusions['inclusions']
            freight_df.drop(columns=index, axis=1, inplace=True)
            columns = ['area', 'bulletin', 'commodity', 'customer_name', 'service_type', 'origin_icd', 'drop', 'drop',
                       'origin_port', 'destination_port', 'destination_icd', 'drop', 'drop', 'drop',
                       'container_owned', 'drop', 'cargo_type', 'currency', '20GP', '40GP', '40HC', '45HC',
                       '4HRD', 'drop', 'drop', 'start_date', 'expiry_date', 'remarks', 'inclusions']
            freight_df.columns = columns
            freight_df = freight_df.drop([0, 1], axis=0)
            freight_df.drop(columns=['drop'], axis=1, inplace=True)
            freight_df['destination_icd'] = freight_df['destination_icd'].str.split(',')
            freight_df = freight_df.explode('destination_icd')
            freight_df.reset_index(inplace=True, drop=True)
            freight_df['destination_icd'] = freight_df['destination_icd'].replace(',', ';', regex=True)
            freight_df['origin_icd'] = freight_df['origin_icd'].replace(',', ';', regex=True)
            freight_df['inclusions'] = freight_df['inclusions'].replace('20P;', '', regex=True)
            freight_df['area'] = freight_df['area'].replace('', nan)
            freight_df['commodity'] = freight_df['commodity'].replace('', nan)
            freight_df['area'] = freight_df['area'].fillna(method='ffill')
            freight_df['commodity'] = freight_df['commodity'].fillna(method='ffill')

            freight_df['bulletin'] = freight_df['bulletin'].replace('', nan)
            freight_df['bulletin'] = freight_df['bulletin'].fillna(method='ffill')
            freight_df['destination_icd'] = freight_df['destination_icd'].str.strip(' ')

            return freight_df

        def capture(self):
            freight_df = self.get_freight_table()
            self.captured_output = freight_df

        def clean(self):
            self.cleaned_output = self.captured_output

    class _Arb(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_arb_table(self):
            arb_df = self.df
            columns = ['area', 'at', 'bulletin', 'icd', 'drop', 'mode_of_transportation', 'to',
                       'drop', 'drop', 'loop', 'drop', 'commodity', 'customer_name', 'drop', 'cargo_type', 'currency',
                       '20GP', '40GP', '40HC', '45HC', '4HRD']
            arb_df.columns = columns
            arb_df['area'] = arb_df['area'].replace('', nan)
            arb_df['area'] = arb_df['area'].fillna(method='ffill')
            arb_df.drop(columns=['drop'], axis=1, inplace=True)
            arb_df.loc[arb_df['at'] == 'Dest Arb', 'at'] = "destination"
            # arb_df.loc[arb_df['at'] == 'Org Arb', 'at'] = "origin"
            arb_df['charges'] = 'destination arbitrary charge'
            arb_df.drop([0, 1], axis=0, inplace=True)
            return arb_df

        def capture(self):
            arb_df = self.get_arb_table()
            self.captured_output = arb_df

        def clean(self):
            self.cleaned_output = self.captured_output

    class _Remarks(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_remarks(self):
            remarks = self.df
            remarks.drop([0, 1], axis=0, inplace=True)
            notes = remarks[3].to_list()
            remarks_dict = []
            pos = 0
            area_currency = []
            for notes_tm in notes:
                area = remarks.iloc[pos, 0]
                pos+=1
                notes_t = notes_tm.split('\n')
                for notes_sep in notes_t:
                    notes_re = re.findall('-(.+?)$', notes_sep)

                    if notes_re:
                        notes_re = notes_re[0]

                        len_per = len(re.findall('per', str(notes_re)))

                        regex = r'(.+?)\s(GBP|USD)\s?(.+?)' + ('per(.+?)' * len_per) + '$'
                        dest_point = 0
                        suf_point = 0
                        if str(notes_re).find("in case of Discharging") != -1:
                            reg = r'in case of Discharging\s+?=\s+?(.+?)$'
                            notes_re_suf = re.findall(reg, str(notes_re))
                            notes_re = re.findall(r"^(.+?),\s\sin case of", notes_re)[0]
                            suf_point = 1

                        if re.search(regex, str(notes_re)) is None:
                            regex = r'\["\s+?(.+?)\s+?USD\s+(.+?)' + ('per(.+?)' * len_per) + r',\s+?in'
                            if re.search(regex, str(notes_re)) is None:
                                regex = r"\['\s+?(.+?)\s+?USD\s+(.+?)" + (
                                        "per(.+?)" * len_per) + r",\s+?in"
                        matches = re.finditer(regex, str(notes_re), re.MULTILINE)
                        for matchNum, match in enumerate(matches, start=1):
                            charge_value_final = []
                            charge_name = match.group(1)
                            charge_value = match.group(3)
                            currency = match.group(2)
                            charge_value = charge_value.replace(' ', '')
                            charge_value = charge_value.split('/')
                            container = []
                            posi = 0
                            i = 0

                            for groupNum in range(3, len(match.groups())):
                                groupNum = groupNum + 1
                                group = match.group(groupNum)
                                group = group.replace(' ', '').replace('/', '')
                                group = group.split(',')
                                charge_value_final.extend(([charge_value[i]] * len(group)))
                                i += 1
                                for grp in group:
                                    if dest_point == 0:
                                        grp = grp + "_" + charge_name
                                        if suf_point == 1:
                                            grp = grp + '_yes_' + notes_re_suf[0]
                                    container.append(grp)
                                    dic = zip(container, charge_value_final)
                                posi += 1
                            remarks_dict.append(dict(dic))
                            area_currency.append({"area": area, "currency": currency})
            return remarks_dict, area_currency

        def capture(self):
            remarks_dict, area_currency = self.get_remarks()
            self.captured_output = [remarks_dict, area_currency]

        def clean(self):
            self.cleaned_output = self.captured_output

    @classmethod
    def map_remarks(cls, freight_df, rate_list, area_cur):
        rate_list_yes = []
        area_cur_yes = []
        for pos in range(len(area_cur)):
            area_list = freight_df.loc[freight_df['area'] == area_cur[pos]['area']].index.to_list()
            if 'yes' in list(rate_list[pos].keys())[0]:
                port = re.findall(r"yes_(.*)", [list(rate_list[pos].keys())[0]][0].strip(" "))[0]
                rate_list_yes.append(rate_list[pos])
                area_cur_yes.append(area_cur[pos])
                if port.strip(' ') == 'SOUTHAMPTON, U.K.':
                    port = 'GBSOU'
                elif port.strip(' ') == 'LONDON GATEWAY, U.K.':
                    port = 'GBLGP'
                try:
                    area_list = freight_df.loc[freight_df['area'].str.contains(area_cur[pos]['area']) & freight_df[
                        'destination_icd'].str.contains(
                        port)].index.to_list()
                except KeyError:
                    area_list = freight_df.loc[freight_df['area'].str.contains(area_cur[pos]['area']) & freight_df[
                        'to'].str.contains(
                        port)].index.to_list()

            for rate in rate_list[pos]:
                rate_key = rate
                if 'yes' in rate:
                    rate = re.findall(r"^(.+?)_yes_", rate)[0]
                    rate_list
                cnt_type = rate.split("_")
                if cnt_type[0].isdigit():
                    cnt_type = cnt_type[0] + 'GP_' + cnt_type[1].strip(" ")
                else:
                    cnt_type = '40HC_' + cnt_type[1].strip(" ")
                freight_df.loc[area_list, cnt_type] = rate_list[pos][rate_key]
                freight_df.loc[area_list, 'currency'] = area_cur[pos]['currency']

        return freight_df, area_cur_yes, rate_list_yes

    @classmethod
    def map_contract_details(cls, df, head):
        df['amendment_no'] = head.iloc[1, 0]
        df['customer_name'] = head.iloc[1, 2]
        df['start_date'] = head.iloc[1, 1]
        df['expiry_date'] = head.iloc[1, 4]
        return df

    @classmethod
    def resolve_dependency(cls, fix_outputs):
        head = fix_outputs.pop('Head')
        freight_df = fix_outputs.pop('Freight')
        arb_df = fix_outputs.pop("Arb Addon")
        remarks = fix_outputs.pop("Special Note")

        area_cur = remarks[1]
        rate_list = remarks[0]
        freight_df, area_cur_yes, rate_list_yes = cls.map_remarks(freight_df, rate_list, area_cur)
        freight_df, area_cur_yes, rate_list_yes = cls.map_remarks(freight_df, rate_list_yes, area_cur_yes)
        arb_df, area_cur_yes, rate_list_yes = cls.map_remarks(arb_df, rate_list, area_cur)

        freight_df = cls.map_contract_details(freight_df, head)
        arb_df = cls.map_contract_details(arb_df, head)

        fix_outputs = {"Freight": {"Freight": freight_df, "Arbitrary Charges": arb_df}}

        return fix_outputs


class Ceva_Hmm_Ap(BaseTemplate):
    class Ceva_Hmm_Ap_1(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def freight_table(self):
            index_POL = list(self.df[(self.df[0].str.contains("KEY", na=False))].index)
            freight_df = self.df.iloc[index_POL[0]:self.df.tail(1).index.values[0] + 1].copy(deep=True)
            freight_df = freight_df.drop([0])
            freight_df = freight_df.drop(freight_df.columns[[0, 3, 4, 5, 6, 9, 10, 11, 12, 13, 14, 17, 18, 19, 20, 21, 25, 26]],axis=1)
            freight_df = freight_df.drop(freight_df.loc[:, 28:42].columns, axis = 1)
            freight_df.columns = freight_df.iloc[0]
            freight_df = freight_df[1:].copy()
            return freight_df

        @staticmethod
        def notes(freight_df):
            notes = {}
            inclusion_index = freight_df[freight_df['Note'].str.contains('included|Inclusive', na=False)].index.to_list()
            freight_df.loc[freight_df['Note'].str.contains('included|Inclusive', na=False), 'inclusions'] = freight_df.loc[freight_df['Note'].str.contains('included|Inclusive', na=False)]['Note']
            freight_df['inclusions'] = freight_df['inclusions'].str.split(' ',1, expand = True)[1].str.replace(',',';')
            subject_index = freight_df[freight_df['Note'].str.contains('included|Inclusive', na=False)].index.to_list()
            freight_df.loc[freight_df['Note'].str.contains('Subject to', na=False), 'subject to'] = freight_df.loc[freight_df['Note'].str.contains('Subject to', na=False)]['Note']
            return freight_df
        def capture(self):
            freight_df = self.freight_table()
            freight_df =self.notes(freight_df)
            freight_df.drop(['Note'], axis=1,inplace=True)
            freight_df.rename(columns={'Origin':'origin_port', 'Origin\nTerm':'mode_of_transportation_origin',\
                                       'Dest.':'destination_port', 'Dest.\nTerm':'mode_of_transportation_destination',\
                                       'Cargo\nType':'cargo_type','Cur.':'currency',\
                                       '20’':'20GP','40’':'40GP','4H’':'40HC'}, inplace=True)
            freight_df['40GP'] = freight_df['40GP'].replace('x','')
            freight_df['40HC'] = freight_df['40HC'].replace('x', '')
            freight_df['start_date'] = ''
            freight_df['expiry_date'] = ''
            self.captured_output = {"Freight": freight_df}
        def clean(self):
            self.cleaned_output = self.captured_output


class HMM_Germany_Fix(BaseTemplate):
    class OceanFreight_Fix(BaseFix):
        def check_input(self):
            pass
        def check_output(self):
            pass

        def get_inclusions(self):
            def get_inclusions_data(inclusions):
                return re.search(r"including(.*)subject", inclusions)

            captured_validity = self.df.iloc[:, 0].apply(lambda x: get_inclusions_data(str(x)))
            inclusions = ""
            for i in captured_validity:
                if i:
                    inclusions = i.group(1).strip().replace("/", ";")
            return inclusions

        def first_rows_as_header(self,df):
            headers = df.iloc[0]
            df = df[1:]
            df.columns = headers
            return df

        def fix_over_blocks(self,block):
            if block[0].str.startswith('POL').any():
                index = block[block[0].str.startswith('POL')].index.values[0]

            block = block.applymap(lambda x: nan if x == ':' or x == '' else x)
            block = block.dropna(axis=1, how='all')
            block = block.T.reset_index(drop=True).T
            region = block[0].values[0]
            remarks = block[2].values[0]
            freight_df = block.loc[index:, :]
            freight_df = self.first_rows_as_header(freight_df)
            columns_rename = {"POL" : "origin_port", "POD (T/S PORT)" : "destination_port",
                              "20' DRY CNTR":"20GP","40' DRY CNTR":"40GP","40' HIGH CUBE":"40HC",
                              "LOOP":"loop" , "FILING REFERENCE":"filling_reference" }
            freight_df.rename(columns = columns_rename, inplace=True)
            freight_df.dropna(subset=["origin_port", "destination_port", "40GP"], inplace=True)
            freight_df["region"] = region
            freight_df["remarks"] = remarks
            return freight_df

        def get_regional_sections(self):
            regional_sections = {}
            indexes = self.df[self.df[0].str.contains("POL")].index.tolist()

            end_index = self.df.index[-1]
            indexes.append(end_index)
            regional_sections = zip(indexes, indexes[1:])
            return regional_sections

        def get_remarks(self):
            remark_index = self.df[self.df[0].str.contains("Subject to")].index.values[0]
            remarks = self.df[remark_index:].to_string(index=False, header=None)
            return remarks.strip()

        def get_validality(self):
            def get_validity_date(date_input):
                return re.search(r"RATE VALIDITY: AS PER LONG TERM SCHEDULE DATE(.*)-(.*)", date_input)

            # 'RATE VALIDITY: AS PER LONG TERM SCHEDULE DATE 01.04.2022 - 30.04.2022'
            captured_validity = self.df.iloc[:, 1].apply(lambda x: get_validity_date(str(x)))
            start_date = ""
            expiry_date = ""
            for i in captured_validity:
                if i:
                    start_date_group = i.group(1)
                    start_date = parse(start_date_group)
                    expiry_date_group = i.group(2)
                    expiry_date = parse(expiry_date_group)
            return start_date, expiry_date



        def capture(self):
            regional_sections = self.get_regional_sections()
            remarks = self.get_remarks()
            inclusions = self.get_inclusions()

            start_date, expiry_date = self.get_validality()

            start_date, expiry_date = self.get_validality()

            dps = []
            for regional_config in regional_sections:
                regional_df = self.df.loc[regional_config[0]: regional_config[1] - 1, :]
                regional_df = regional_df.T.reset_index(drop=True).T
                block_df = self.fix_over_blocks(regional_df)
                dps.append(block_df)
            result_df = pd.concat(dps,ignore_index=True)
            result_df['origin_port'].replace("/", ";", regex=True, inplace= True)
            result_df['inclusions'] = inclusions

            result_df['remarks'] += " " + remarks + "\n" + result_df["filling_reference"]
            result_df["start_date"],  result_df["expiry_date"] = start_date, expiry_date

            self.captured_output = {'Freight': result_df}

        def clean(self):
            freight_df = self.captured_output["Freight"]
            freight_df["contract_id"] = freight_df["filling_reference"]
            freight_df = freight_df.reset_index(drop=True)
            freight_df.loc[(freight_df["destination_port"].str.contains(r"VIA", na=False)), "via_port"] = freight_df["destination_port"].str.split("VIA").str[1].replace("\)", "", regex=True)
            freight_df.loc[(freight_df["destination_port"].astype(str).str.contains(r"NO DG", na=False)), "cargo_type"] = "NO"
            freight_df.loc[(freight_df["via_port"].str.contains(r"DG", na=False)), "via_port"] = freight_df["via_port"].str.replace(r"/?\s?NO DG", "", regex=True).replace("/DG ON REQUEST", "", regex=True)
            freight_df["via_port"] = freight_df["via_port"].str.replace(r"VIA", "", regex=True).replace(r"/", ";", regex=True).replace( r",", "", regex=True)
            freight_df.loc[(freight_df["destination_port"].str.contains(r"VIA", na=False)), "destination_port"] = freight_df["destination_port"].str.split("VIA").str[0].replace("\(", "", regex=True)
            freight_df["sub_vendor"], freight_df["contract_no"] = "Hyundai Merchant Marine (Germany)", "G2000906"
            freight_df["currency"] = "USD"
            freight_df['start_date'], freight_df['expiry_date'] = pd.to_datetime(freight_df['start_date']).dt.strftime('%d.%m.%Y'), pd.to_datetime(freight_df['expiry_date']).dt.strftime('%d.%m.%Y')
            freight_df.rename(columns={"via_port": "via"}, inplace=True)
            freight_df = freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            self.cleaned_output = {'Freight': freight_df}


    def resolve_dependency(cls, fix_outputs):
        if "2. Main Ports | Japan | Taiwan" in fix_outputs:
            df_freight = fix_outputs.pop("2. Main Ports | Japan | Taiwan")
            mainport_freight_df = df_freight["Freight"]
            mainport_freight_df["unique"] = "2. Main Ports | Japan | Taiwan"

        if "3. China | South East Asia" in fix_outputs:
            df_freight = fix_outputs.pop("3. China | South East Asia")
            asia_freight_df = df_freight["Freight"]
            asia_freight_df["unique"] = "3. China | South East Asia"

        if "4. Middle East" in fix_outputs:
            df_freight = fix_outputs.pop("4. Middle East")
            middle_east_freight_df = df_freight["Freight"]
            middle_east_freight_df["unique"] = "4. Middle East"

        freight_df = pd.concat([mainport_freight_df, asia_freight_df, middle_east_freight_df], ignore_index=False)
        fix_outputs =[{"Freight": freight_df}]
        return fix_outputs



