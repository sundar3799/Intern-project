from nbconvert.filters import html2text
import pandas as pd
from collections import defaultdict
import re
import io
from logging import getLogger
from base import BaseTemplate, BaseFix, BaseDocxFix
from numpy import nan
from util import remarks_util

log = getLogger(__name__)
import numpy as np
from datetime import datetime, date
from dateutil.parser import parse


class COSCO_V1(BaseTemplate):
    class _CoscoAsia(BaseDocxFix):
        def __init__(self, df: dict, file_model: dict, headers_config: dict):
            super().__init__(df, file_model, headers_config)
            self.dfs = self.df

        def check_input(self):
            pass
            # check_errors = []
            #
            # '''if not self.dfs[0].str.contains('COMMODITIES', case=False, na=False).any():
            #     check_errors.append("Commodity section is not present in the input file")'''
            #
            # if check_errors:
            #     raise InputValidationError(check_errors)

        def check_output(self):
            pass

        def get_amd_no(self):
            contract_details = self.dfs[0]
            amd_no = contract_details.iloc[2, 1]
            return amd_no

        def get_group_name(self):
            df_output = pd.DataFrame()
            for df in self.dfs:
                if 0 in df:
                    if df[0].str.contains('COMMODITIES', case=False).any() and df[0].str.contains('Group',
                                                                                                  case=False).any():

                        groupdf = df.loc[(df[0].str.contains('Group', case=False, na=False))]
                        string_re = groupdf.iloc[0, 0]
                        regex_group_name = re.compile("Group (.+?):")
                        for i in groupdf[0]:
                            group_name = regex_group_name.findall(i)
                        break
            group_name = ['Group ' + s for s in group_name]
            return group_name, string_re

        def get_contracts_details(self):
            contracts_details = {}
            regex = ("Essential Term number:\s([A-Z,0-9]+)")
            contract_id = re.findall(regex, self.raw_html, re.MULTILINE)[0]
            regex = ("TERMINATION(.*)6. Contract")
            validaity_date = re.findall(regex, self.raw_html, re.MULTILINE)
            regex_Date = ("(\w+\s\d+,\s\d+)")
            try:
                expiry_date_list = re.findall(regex_Date, validaity_date[0], re.MULTILINE)
                contracts_details['expiry_date'] = parse(expiry_date_list[1])
            except:
                raise "Contract Expiry date not captured"
            contracts_details['contract_id'] = contract_id
            contracts_details['vendor'] = "COSCO"
            contracts_details['start_date'] = ""
            return contracts_details

        def get_grp_desc(self, group_name, string_re):
            comm_desc = {}
            regex = (r"Group\s+?(.+?):(.+?)" * len(group_name)) + '$'
            matches = re.finditer(regex, string_re, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups()) - 1):
                    groupNum = groupNum + 1
                    group_name_ = match.group(groupNum)
                    group_desc = match.group(groupNum + 1)
                    comm_desc['Group ' + group_name_] = group_desc
            return comm_desc

        def get_line_item(self, ):
            regex = r'6. Contract RATES \(IN US DOLLARS UNLESS SPECIFIED\) :(.+?)Commodity'
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    test_html = html2text(match.groups(1)[0])
                    line_item = test_html.strip()
            return line_item

        def get_lookup(self):
            regex = r'GEOGRAPHIC TERMS(.+?)CODE DEFINITIONS'
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    geo_html = html2text(match.groups(1)[0])
                    buffer = io.StringIO(geo_html)

            geo_code_df = pd.read_csv(filepath_or_buffer=buffer,
                                      skiprows=1,
                                      header=None,
                                      names=['code', "value"],
                                      delimiter=':',
                                      engine='c',
                                      )
            index_ = geo_code_df.loc[geo_code_df['value'].isna()].index.to_list()
            i = 0
            while i <= len(index_) - 2:
                if (index_[i + 1] - index_[i]) == 1:
                    k = i
                    string = str(geo_code_df.iloc[index_[i] - 1, 1]) + str(geo_code_df.iloc[index_[i], 0])
                    for j in range(i + 1, len(index_) - 1):
                        if (index_[j + 1] - index_[j]) == 1:
                            tmp_str = str(geo_code_df.iloc[index_[j], 0]) + str(geo_code_df.iloc[index_[j + 1], 0])
                            string += tmp_str
                            i += 1
                        else:
                            break
                    geo_code_df.loc[index_[k] - 1, 'value'] = string
                else:
                    string = str(geo_code_df.iloc[index_[i] - 1, 1]) + str(geo_code_df.iloc[index_[i], 0])
                    geo_code_df.loc[index_[i] - 1, 'value'] = string
                i += 1
            geo_code_df.dropna(axis=0, how='any', inplace=True)
            geo_code_df.reset_index(inplace=True, drop=True)
            geo_code_df['code'] = geo_code_df['code'].str.strip()
            regex = r"\s\([^()]*\)"
            geo_code_df['code'].replace(regex, '', regex=True, inplace=True)
            geo_code_dict = geo_code_df.set_index('code')['value'].to_dict()
            return geo_code_dict

        def get_main_leg_table(self, group_name, comm_desc):
            df_freight_tmp = []
            notes_dict = defaultdict(list)
            date_dict = {}
            note_included = []
            note_not_included = []
            for group in group_name:
                notes_inc = defaultdict()
                notes_not_inc = defaultdict()
                regex = 'Commodity:\t' + group + '(.+?)<p>Note (.+?)Commodity'
                if re.search(regex, self.raw_html) is None:
                    regex = 'Commodity:\t' + group + '(.+?)<p>Note (.+?)ASSESSORIALS'

                matches = re.finditer(regex, self.raw_html, re.MULTILINE)
                for matchNum, match in enumerate(matches, start=1):
                    for groupNum in range(0, len(match.groups()) - 1):
                        groupNum = groupNum + 1
                        group_ = match.group(groupNum)
                        df_freight_list = pd.read_html(group_)

                        notes = 'Note ' + match.group(groupNum + 1)
                        regex_notes = r'Note\s+?(.+?):\t(.+?)</p><p>'
                        matches = re.finditer(regex_notes, notes, re.MULTILINE)
                        for matchNum_1, match_1 in enumerate(matches, start=1):
                            for groupNum_1 in range(0, len(match_1.groups()) - 1):
                                groupNum_1 = groupNum_1 + 1
                                notes_value = match_1.group(groupNum_1 + 1)
                                notes_id = match_1.group(groupNum_1)
                                notes_found = 0  # Notes_found = 1 is used to remove the notes that we are capturing in other columns.
                                if re.search(r'effective\s+?through\s+?(.+).', notes_value) is not None:
                                    matches_date = re.findall(r'effective\s+?through\s+?(.+).', notes_value,
                                                              re.MULTILINE)
                                    start_date = datetime.strptime(matches_date[0], '%B %d, %Y')
                                    date_dict[group] = {'Date': start_date}
                                    notes_found = 1
                                note_included = []
                                if re.search(r'Rates are inclusive of the', notes_value) is not None:
                                    matches_inc = re.findall(
                                        r'Rates are inclusive of the(.+?)\s+?Rates are not inclusive of', notes_value,
                                        re.MULTILINE)
                                    regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                                    matches_inc_br = re.finditer(regex_incl, matches_inc[0], re.MULTILINE)
                                    for matchNum_2, match_2 in enumerate(matches_inc_br, start=1):
                                        for groupNum_2 in range(0, len(match_2.groups())):
                                            groupNum_2 = groupNum_2 + 1
                                            note_included.append(match_2.group(groupNum_2))
                                    notes_inc[notes_id] = note_included
                                    notes_dict[group].append({'Included': note_included})
                                    notes_found = 1
                                note_not_included = []
                                if re.search(r'Rates are not inclusive of the', notes_value) is not None:
                                    matches_inc = re.findall(r'Rates are not inclusive of the(.+?)$', notes_value,
                                                             re.MULTILINE)
                                    regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                                    matches_inc_br = re.finditer(regex_incl, matches_inc[0], re.MULTILINE)
                                    for matchNum_not_inc, match_not_inc in enumerate(matches_inc_br, start=1):
                                        for groupNum_not_inc in range(0, len(match_not_inc.groups())):
                                            groupNum_not_inc = groupNum_not_inc + 1
                                            note_not_included.append(
                                                match_not_inc.group(groupNum_not_inc))
                                    notes_dict[group].append({'Not Included': note_not_included})

                                    notes_found = 1
                                # dest_arb_allowed = 'Yes'
                                # if re.search(r'cannot be applied with destination arbitrary', notes_value) is not None:
                                #     notes_found = 1
                                #     dest_arb_allowed = 'No'

                                if notes_found == 0:
                                    notes_dict[group].append({notes_id: notes_value + ';'})
                                else:
                                    notes_dict[group].append({notes_id: ''})
                        for df_grp in df_freight_list:
                            df_grp.loc[:, 'bulletin'] = group
                            df_grp.loc[:, 'commodity'] = comm_desc[group].strip()
                            df_grp.dropna(axis=1, how='all', inplace=True)
                            df_grp.loc[(df_grp['Notes'].str.contains(notes_id)), 'inclusions'] = ','.join(note_included)
                            df_grp.loc[(df_grp['Notes'].str.contains(notes_id)), 'subject_to'] = ','.join(
                                note_not_included)

                            if notes_inc:
                                notes_not_inc[notes_id] = note_not_included
                                notes_dict[group].append({notes_id: notes_value})
                                # for inc_col in note_included:
                                #     df_grp[inc_col] = ''
                                #     df_grp.loc[(df_grp['Notes'].str.contains('S1')), inc_col] = 'X'
                                # for inc_col in note_not_included:
                                #     df_grp[inc_col] = ''
                                for note_id_inc in notes_inc:
                                    df_grp.loc[(df_grp['Notes'].str.contains(note_id_inc + '$',
                                                                             regex=True)), 'inclusions'] = ','.join(
                                        notes_inc[note_id_inc])
                                    df_grp.loc[(df_grp['Notes'].str.contains(note_id_inc + ';',
                                                                             regex=True)), 'inclusions'] = ','.join(
                                        notes_inc[note_id_inc])
                                for note_id_inc in notes_not_inc:
                                    df_grp.loc[
                                        (df_grp['Notes'].str.contains(note_id_inc + '$')), 'subject_to'] = ','.join(
                                        notes_not_inc[note_id_inc])
                                    df_grp.loc[
                                        (df_grp['Notes'].str.contains(note_id_inc + ';')), 'subject_to'] = ','.join(
                                        notes_not_inc[note_id_inc])
                            df_freight_tmp.append(df_grp)
                df_freight = pd.concat(df_freight_tmp, axis=0, ignore_index=True)
            return df_freight, notes_dict, date_dict

        def arb(self, lineitem, amd_no, date_dict):
            df_arb = pd.DataFrame()
            if re.search(r"ASSESSORIALS(.+?)Appendix B to Service Contract", self.raw_html) is not None:
                regex_arb = r"ASSESSORIALS(.+?)Appendix B to Service Contract"
                matches = re.finditer(regex_arb, self.raw_html, re.MULTILINE)
                for matchNum, match in enumerate(matches, start=1):
                    for groupNum in range(0, len(match.groups())):
                        groupNum = groupNum + 1
                        group_ = match.group(groupNum)
                        notes_arb = group_
                        df = pd.read_html(group_)
                        df_arb = pd.concat(df, axis=0)
                notes_dict_arb = {}
                regex_arb_notes = r'Note\s+?(.+?):\t(.+?)</p><p>'
                matches_notes = re.finditer(regex_arb_notes, notes_arb, re.MULTILINE)
                end_date = ''
                start_date = ''
                for matchNum_note, match_note in enumerate(matches_notes, start=1):
                    for groupNum_note in range(0, len(match_note.groups()) - 1):
                        groupNum_note = groupNum_note + 1
                        notes_value = match_note.group(groupNum_note + 1)
                        notes_id = match_note.group(groupNum_note)
                        print(notes_id, notes_value)
                        if end_date:
                            end_date = matches_date[0]
                            end_date_id = notes_id
                            start_date = ""
                            if re.search(r'effective\s+?starting\s+?(.+).', notes_value) is not None:
                                matches_date = re.findall(r'effective\s+?starting\s+?(.+).', notes_value, re.MULTILINE)
                                start_date = matches_date[0]
                                start_date_id = notes_id
                        if re.search(r'effective\s+?through\s+?(.+).', notes_value) is not None:
                            matches_date = re.findall(r'effective\s+?through\s+?(.+).', notes_value, re.MULTILINE)
                            date_dict['Date'] = matches_date[0]

                        notes_dict_arb[notes_id] = notes_value
                if end_date:
                    df_arb.loc[(df_arb['Notes'].str.contains(end_date_id, na=False)), 'expiry_date'] = end_date
                if start_date:
                    df_arb.loc[(df_arb['Notes'].str.contains(start_date_id, na=False)), 'start_date'] = start_date
                df_arb.replace(notes_dict_arb, inplace=True, regex=True)
                df_arb.reset_index(drop=True, inplace=True)
                df_arb.dropna(how='all', axis=1, inplace=True)
                df_arb['currency'] = 'USD'
                df_arb['charges_leg'] = 'L2'
                df_arb['charges'] = 'origin arbitrary charges'
                # df_arb['start_date'] = start_date
                # df_arb['expiry_date'] = end_date
                df_arb['at'] = 'origin'
                df_arb.rename(
                    columns={"20": "20GP", "40": '40GP', "40H": "40HC", 'Location': 'icd', 'Over': 'to', 'Via': 'via',
                             'Mode': 'service_type', 'TransportType': 'mode_of_transportation', 'Notes': 'remarks'},
                    inplace=True)
                df_arb['unique'] = lineitem
                df_arb['Amendment no.'] = amd_no
                return df_arb
            else:
                return df_arb

        @staticmethod
        def format_output(df_freight, df_arb, contract_details):
            if df_arb.empty:
                if contract_details != '':
                    df_freight["contract_id"] = contract_details["contract_id"]
                    df_freight["vendor"] = contract_details["vendor"]
                    df_freight["contract_start_date"] = contract_details["start_date"]
                    df_freight["contract_expiry_date"] = contract_details["expiry_date"]

                output = {'Freight': df_freight}

            else:
                if contract_details != '':
                    df_freight["contract_id"] = contract_details["contract_id"]
                    df_freight["vendor"] = contract_details["vendor"]
                    df_freight["contract_start_date"] = contract_details["start_date"]
                    df_freight["contract_expiry_date"] = contract_details["expiry_date"]

                output = {'Freight': df_freight, 'Arbitrary Charges': df_arb}
            return output


class COSCO_V1_AMD(BaseTemplate):
    class _CoscoAsia(BaseDocxFix):
        def __init__(self, df: dict, file_model: dict, headers_config: dict):
            super().__init__(df, file_model, headers_config)
            self.dfs = self.df

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_amd_no(self):
            regex = ("AMENDMENT NO:\s(\d{1,4})<")
            amd_no = re.findall(regex, self.raw_html, re.MULTILINE)[0]
            return amd_no

        def get_group_name(self):
            regex_group_name = r"Commodity:\tGroup\s+?(.+?)<\/p>"
            group_list = []
            matches = re.finditer(regex_group_name, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group_ = match.group(groupNum)
                    group_list.append(group_)
            group_name = ['Group ' + s for s in group_list]
            return group_name

        def get_contracts_details(self):
            contracts_details = {}
            regex = ("ESSENTIAL TERMS NO:\s([A-Z,0-9]+)")
            contract_id = re.findall(regex, self.raw_html, re.MULTILINE)[0]
            contracts_details['contract_id'] = contract_id
            contracts_details['vendor'] = "COSCO"
            contracts_details['start_date'] = ""
            contracts_details['expiry_date'] = ""
            return contracts_details

        def get_line_item(self):
            regex = 'Amend the following contract rates<\/p><p>(.+?)<'
            line_item = re.findall(regex, self.raw_html)[0]
            return line_item

        @staticmethod
        def format_output(df_freight, contract_detail):
            if contract_detail != '':
                df_freight["contract_id"] = contract_detail["contract_id"] + "AD"
                df_freight["contract_start_date"] = contract_detail["start_date"]
                df_freight["contract_expiry_date"] = contract_detail["expiry_date"]
                df_freight["vendor"] = contract_detail["vendor"]
            output = {'Freight': df_freight}
            return output


class Flexport_COSCO_v1(COSCO_V1):
    class _CoscoAsia(COSCO_V1._CoscoAsia):

        def map_notes(self, group_name, notes_dict, df_freight, date_dict):
            df_freight['Notes'] = df_freight['Notes'].str.replace(';', ' ', regex=True)
            for group in group_name:
                notes_grp_list = notes_dict[group]
                for notes_grp in notes_grp_list:
                    for key, value in notes_grp.items():
                        indexes = df_freight[
                            (df_freight['bulletin'] == group) & (df_freight['Notes'].str.contains(key))].index.tolist()
                        df_freight.loc[indexes, 'Notes'] = df_freight.loc[indexes, 'Notes'].replace(key, value,
                                                                                                    regex=True)
                try:
                    df_freight.loc[(df_freight['bulletin'] == group), 'expiry_date'] = date_dict[group]['Date']
                except KeyError:
                    pass
            return df_freight

        def capture(self):
            self.amd_no = self.get_amd_no()
            group_name, string_re = self.get_group_name()
            comm_desc = self.get_grp_desc(group_name, string_re)
            df_freight, notes_dict, date_dict = self.get_main_leg_table(group_name, comm_desc)
            df_freight = self.map_notes(group_name, notes_dict, df_freight, date_dict)
            self.geo_code_dict = self.get_lookup()
            self.line_item = self.get_line_item()
            df_arb = self.arb(self.line_item, self.amd_no, date_dict)
            contract_details = ''
            self.captured_output = self.format_output(df_freight, df_arb, contract_details)

        def clean(self):
            freight_df = self.captured_output['Freight']
            # freight_df.replace(self.geo_code_dict, inplace=True)
            for code in self.geo_code_dict:
                _code = (self.geo_code_dict[code])
                freight_df.replace(code, _code, inplace=True, regex=True)
            freight_df.drop(['CargoNature'], axis=1, inplace=True)
            freight_df.rename(
                columns={'Origin': 'origin_port', 'Destination': 'destination_icd', 'Mode': 'service_type',
                         'DestinationVia': 'destination_port', 'Service': 'loop', '20': '20GP', '40': '40GP',
                         '40H': '40HC', 'Notes': 'remarks'
                         }, inplace=True)
            freight_df['currency'] = 'USD'
            freight_df['charges_leg'] = 'L3'
            freight_df['charges'] = 'Basic Ocean Freight'
            freight_df['Amendment no.'] = self.amd_no
            freight_df['destination_port'] = freight_df['destination_port'].str.split(';')
            freight_df = freight_df.explode('destination_port')
            freight_df.reset_index(inplace=True, drop=True)
            freight_df['unique'] = self.line_item
            index_arb = freight_df[freight_df['remarks'].str.contains('cannot be applied with destination arbitrary',
                                                                      na=False)].index.tolist()

            freight_df['destination_arbitrary_allowed'] = 'Yes'

            freight_df.loc[index_arb, 'destination_arbitrary_allowed'] = 'No'
            """ Removing ar allowed remarks as it is not needed"""
            freight_df['remarks'] = freight_df['remarks'].str.replace(
                'Rate cannot be applied with destination arbitrary.;', '', regex=True)

            if 'Arbitrary Charges' in self.captured_output:
                self.cleaned_output = {'Freight': freight_df,
                                       'Arbitrary Charges': self.captured_output['Arbitrary Charges']}
            else:
                self.cleaned_output = {'Freight': freight_df}


class Expedoc_COSCO_v1(COSCO_V1):
    class _CoscoAsia(COSCO_V1._CoscoAsia):

        def map_notes(self, group_name, notes_dict, df_freight, date_dict):
            for group in group_name:
                notes_grp_list = notes_dict[group]
                for notes_grp in notes_grp_list:
                    for key, value in notes_grp.items():
                        # df_list.loc[(df_list['bulletin']==group)]['Notes'].replace(notes_grp,regex=True,inplace=True)
                        indexes = df_freight[
                            (df_freight['bulletin'] == group) & (df_freight['Notes'].str.contains(key))].index.tolist()
                        df_freight.loc[indexes, 'Notes'] = df_freight.loc[indexes, 'Notes'].replace(key, value,
                                                                                                    regex=True)
                try:
                    df_freight.loc[(df_freight['bulletin'] == group), 'start_date'] = date_dict[group]['Date']
                except KeyError:
                    pass
            return df_freight

        def capture(self):
            contract_details = self.get_contracts_details()
            self.amd_no = self.get_amd_no()
            group_name, string_re = self.get_group_name()
            comm_desc = self.get_grp_desc(group_name, string_re)
            df_freight, notes_dict, date_dict = self.get_main_leg_table(group_name, comm_desc)
            df_freight = self.map_notes(group_name, notes_dict, df_freight, date_dict)
            self.geo_code_dict = self.get_lookup()
            self.line_item = self.get_line_item()
            df_arb = self.arb(self.line_item, self.amd_no, date_dict)
            self.captured_output = self.format_output(df_freight, df_arb, contract_details)

        def clean(self):
            freight_df = self.captured_output['Freight']
            # freight_df.replace(self.geo_code_dict, inplace=True)
            for code in self.geo_code_dict:
                _code = (self.geo_code_dict[code])
                freight_df.replace(code, _code, inplace=True, regex=True)
            freight_df.drop(['CargoNature'], axis=1, inplace=True)
            # freight_df.rename(
            #     columns={'Origin': 'origin_port', 'Destination': 'destination_icd', 'Mode': 'service_type',
            #              'DestinationVia': 'destination_port', 'Service': 'loop', '20': '20GP', '40': '40GP',
            #              '40H': '40HC', 'Notes': 'remarks'
            #              }, inplace=True)
            """	
                To remove	
            """
            freight_df.rename(
                columns={'Origin': 'origin_port', 'Destination': 'destination_port', 'Mode': 'service_type',
                         'DestinationVia': 'destination_icd', 'Service': 'loop', '20': '20GP', '40': '40GP',
                         '40H': '40HC', 'Notes': 'remarks', 'OriginVia': 'origin_icd'
                         }, inplace=True)
            # for to_explode in ['origin_port', 'destination_port', 'destination_icd']:
            #     freight_df[to_explode] = freight_df[to_explode].str.split(';').explode(to_explode)
            #     freight_df = freight_df.reset_index(drop=True)
            freight_df['origin_port'] = freight_df['origin_port'].str.split(';')
            freight_df['destination_port'] = freight_df['destination_port'].str.split(';')
            freight_df['destination_icd'] = freight_df['destination_icd'].str.split(';')
            freight_df = freight_df.explode('origin_port')
            freight_df = freight_df.explode('destination_port')
            freight_df = freight_df.explode('destination_icd')
            freight_df.reset_index(drop=True, inplace=True)
            if 'start_date' in freight_df:
                freight_df['start_date'] = freight_df['start_date'].dt.date
            if 'expiry_date' in freight_df:
                freight_df['expiry_date'] = freight_df['expiry_date'].dt.date
            arb_df = self.captured_output['Arbitrary Charges']
            if 'start_date' in arb_df:
                arb_df['start_date'] = pd.to_datetime(arb_df['start_date'], errors='coerce').dt.date
            if 'expiry_date' in arb_df:
                arb_df['expiry_date'] = pd.to_datetime(arb_df['expiry_date'], errors='coerce').dt.date
            freight_df['currency'] = 'USD'
            freight_df['charges_leg'] = 'L3'
            freight_df['charges'] = 'Basic Ocean Freight'
            freight_df['Amendment no.'] = self.amd_no
            freight_df['destination_port'] = freight_df['destination_port'].str.split(';')
            freight_df = freight_df.explode('destination_port')
            freight_df.reset_index(inplace=True, drop=True)
            freight_df['unique'] = self.line_item
            freight_df.loc[(freight_df['remarks'].str.contains('cannot be applied with destination arbitrary',
                                                               na=False)), 'destinaiton_arbitrary_allowed'] = 'Yes'
            freight_df.loc[~(freight_df['remarks'].str.contains('cannot be applied with destination arbitrary',
                                                                na=False)), 'destinaiton_arbitrary_allowed'] = 'No'
            if 'Arbitrary Charges' in self.captured_output:
                self.cleaned_output = {'Freight': freight_df, 'Arbitrary Charges': arb_df}
            else:
                self.cleaned_output = {'Freight': freight_df}


class Flexport_COSCO_AMD_v1(COSCO_V1_AMD):
    class _CoscoAsiaAMD(COSCO_V1_AMD._CoscoAsia):

        def map_main_leg_table(self, group_name):
            df_freight_tmp = []
            notes_dict = defaultdict(list)
            date_dict = {}
            note_included = []
            note_not_included = []
            for group in group_name:
                regex = 'Commodity:\t' + group + '(.+?)<p>Note (.+?)(ASSESSORIAL|Appendix B|Commodity|LEGEND)'
                matches = re.finditer(regex, self.raw_html, re.MULTILINE)
                for matchNum, match in enumerate(matches, start=1):
                    for groupNum in range(0, len(match.groups()) - 2):
                        groupNum = groupNum + 1
                        group_ = match.group(groupNum)
                        df = pd.read_html(group_)
                        notes = 'Note ' + match.group(groupNum + 1)
                        regex_notes = r'Note\s+?(.+?):\t(.+?)</p><p>'
                        matches = re.finditer(regex_notes, notes, re.MULTILINE)
                        for matchNum_1, match_1 in enumerate(matches, start=1):
                            for groupNum_1 in range(0, len(match_1.groups()) - 1):
                                groupNum_1 = groupNum_1 + 1
                                notes_found = 0  # Notes_found = 1 is used to remove the notes that we are capturing in other columns.
                                notes_value = match_1.group(groupNum_1 + 1)
                                notes_id = match_1.group(groupNum_1)
                                if re.search(r'effective\s+?through\s+?(.+).', notes_value) is not None:
                                    matches_date = re.findall(r'effective\s+?through\s+?(.+).', notes_value,
                                                              re.MULTILINE)
                                    start_date = datetime.strptime(matches_date[0], '%B %d, %Y')
                                    date_dict[group] = {'Date': start_date}
                                    notes_found = 1
                                note_included = []
                                if re.search(r'Rates are inclusive of the', notes_value) is not None:
                                    matches_inc = re.findall(
                                        r'Rates are inclusive of the(.+?)\s+?Rates are not inclusive of', notes_value,
                                        re.MULTILINE)
                                    regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                                    matches_inc_br = re.finditer(regex_incl, matches_inc[0], re.MULTILINE)
                                    for matchNum_2, match_2 in enumerate(matches_inc_br, start=1):
                                        for groupNum_2 in range(0, len(match_2.groups())):
                                            groupNum_2 = groupNum_2 + 1
                                            note_included.append(match_2.group(groupNum_2))
                                    notes_found = 1
                                    notes_dict[group].append({'Included': note_included})
                                    # print(note_included)
                                note_not_included = []
                                if re.search(r'Rates are not inclusive of the', notes_value) is not None:
                                    matches_inc = re.findall(r'Rates are not inclusive of the(.+?)$', notes_value,
                                                             re.MULTILINE)
                                    regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                                    matches_inc_br = re.finditer(regex_incl, matches_inc[0], re.MULTILINE)
                                    for matchNum_not_inc, match_not_inc in enumerate(matches_inc_br, start=1):
                                        for groupNum_not_inc in range(0, len(match_not_inc.groups())):
                                            groupNum_not_inc = groupNum_not_inc + 1
                                            note_not_included.append(match_not_inc.group(groupNum_not_inc))
                                    notes_dict[group].append({'Not Included': note_not_included})
                                    notes_found = 1
                                if re.search(r'cannot be applied with destination arbitrary', notes_value) is not None:
                                    notes_found = 1
                                if notes_found == 0:
                                    notes_dict[group].append({notes_id: notes_value + ';'})
                                else:
                                    notes_dict[group].append({notes_id: ''})
                        for df_grp in df:
                            df_grp.loc[:, 'bulletin'] = group
                            df_grp.dropna(axis=1, how='all', inplace=True)
                            df_grp.loc[(df_grp['Notes'].str.contains(notes_id)), 'inclusions'] = ','.join(note_included)
                            df_grp.loc[(df_grp['Notes'].str.contains(notes_id)), 'subject_to'] = ','.join(
                                note_not_included)
                            df_freight_tmp.append(df_grp)
                df_freight = pd.concat(df_freight_tmp, axis=0, ignore_index=True)
            return df_freight, notes_dict, date_dict

        @staticmethod
        def map_notes(group_name, notes_dict, df_freight, date_dict):
            df_freight['Notes'] = df_freight['Notes'].str.replace(';', ' ', regex=True)
            for group in group_name:
                notes_grp_list = notes_dict[group]
                for notes_grp in notes_grp_list:
                    for key, value in notes_grp.items():
                        indexes = df_freight[
                            (df_freight['bulletin'] == group) & (df_freight['Notes'].str.contains(key))].index.tolist()
                        df_freight.loc[indexes, 'Notes'] = df_freight.loc[indexes, 'Notes'].replace(key, value,
                                                                                                    regex=True)
                try:
                    df_freight.loc[(df_freight['bulletin'] == group), 'expiry_date'] = date_dict[group]['Date']
                except KeyError:
                    pass
            return df_freight

        def capture(self):
            self.amd_no = self.get_amd_no()
            group_name = self.get_group_name()
            df_freight, notes_dict, date_dict = self.map_main_leg_table(group_name)
            df_freight = self.map_notes(group_name, notes_dict, df_freight, date_dict)
            self.line_item = self.get_line_item()
            contract_details = ''
            self.captured_output = self.format_output(df_freight, contract_details)
            # if re.search(r"ASSESSORIALS(.+?)Appendix B to Service Contract", self.raw_html) is not None:
            #     df_arb = self.arb(self.line_item, self.amd_no, date_dict)
            #     self.captured_output = self.format_output(df_freight, df_arb)
            # else:
            #     self.captured_output = self.format_output(df_freight)

        def clean(self):
            freight_df = self.captured_output['Freight']
            freight_df.drop(['CargoNature'], axis=1, inplace=True)
            freight_df.rename(
                columns={'Origin': 'origin_port', 'Destination': 'destination_icd', 'Mode': 'service_type',
                         'DestinationVia': 'destination_port', 'Service': 'loop', '20': '20GP', '40': '40GP',
                         '40H': '40HC', 'Notes': 'remarks'
                         }, inplace=True)
            freight_df['currency'] = 'USD'
            freight_df['charges_leg'] = 'L3'
            freight_df['charges'] = 'Basic Ocean Freight'
            freight_df['Amendment no.'] = self.amd_no
            freight_df['destination_port'] = freight_df['destination_port'].str.split(';')
            freight_df = freight_df.explode('destination_port')
            freight_df.reset_index(inplace=True, drop=True)
            freight_df['unique'] = self.line_item
            freight_df.loc[(freight_df['remarks'].str.contains('cannot be applied with destination arbitrary',
                                                               na=False)), 'destination_arbitrary_allowed'] = 'No'
            freight_df.loc[~(freight_df['remarks'].str.contains('cannot be applied with destination arbitrary',
                                                                na=False)), 'destination_arbitrary_allowed'] = 'Yes'
            # if re.search(r"ASSESSORIALS(.+?)Appendix B to Service Contract", self.raw_html) is not None:
            #     self.cleaned_output = {'Freight': freight_df,
            #                            'Arbitrary Charges': self.captured_output['Arbitrary Charges']}
            # else:
            self.cleaned_output = {'Freight': freight_df}


class Expedoc_COSCO_AMD_v1(COSCO_V1_AMD):
    class _CoscoAsiaAMD(COSCO_V1_AMD._CoscoAsia):

        def map_main_leg_table(self, group_name):
            df_freight_tmp = []
            notes_dict = defaultdict(list)
            date_dict = {}
            note_included = []
            note_not_included = []
            for group in group_name:
                regex = 'Commodity:\t' + group + '(.+?)<p>Note (.+?)(ASSESSORIAL|Appendix B|Commodity|LEGEND)'
                matches = re.finditer(regex, self.raw_html, re.MULTILINE)
                for matchNum, match in enumerate(matches, start=1):
                    for groupNum in range(0, len(match.groups()) - 2):
                        groupNum = groupNum + 1
                        group_ = match.group(groupNum)
                        df = pd.read_html(group_)
                        notes = 'Note ' + match.group(groupNum + 1)
                        regex_notes = r'Note\s+?(.+?):\t(.+?)</p><p>'
                        matches = re.finditer(regex_notes, notes, re.MULTILINE)
                        for matchNum_1, match_1 in enumerate(matches, start=1):
                            for groupNum_1 in range(0, len(match_1.groups()) - 1):
                                groupNum_1 = groupNum_1 + 1
                                notes_value = match_1.group(groupNum_1 + 1)
                                notes_id = match_1.group(groupNum_1)
                                if re.search(r'effective\s+?through\s+?(.+).', notes_value) is not None:
                                    matches_date = re.findall(r'effective\s+?through\s+?(.+).', notes_value,
                                                              re.MULTILINE)
                                    start_date = datetime.strptime(matches_date[0], '%B %d, %Y')
                                    date_dict[group] = {'Date': start_date}
                                note_included = []
                                if re.search(r'Rates are inclusive of the', notes_value) is not None:
                                    matches_inc = re.findall(
                                        r'Rates are inclusive of the(.+?)\s+?Rates are not inclusive of', notes_value,
                                        re.MULTILINE)
                                    regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                                    matches_inc_br = re.finditer(regex_incl, matches_inc[0], re.MULTILINE)
                                    for matchNum_2, match_2 in enumerate(matches_inc_br, start=1):
                                        for groupNum_2 in range(0, len(match_2.groups())):
                                            groupNum_2 = groupNum_2 + 1
                                            note_included.append(match_2.group(groupNum_2))
                                    notes_dict[group].append({'Included': note_included})
                                note_not_included = []
                                if re.search(r'Rates are not inclusive of the', notes_value) is not None:
                                    matches_inc = re.findall(r'Rates are not inclusive of the(.+?)$', notes_value,
                                                             re.MULTILINE)
                                    regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                                    matches_inc_br = re.finditer(regex_incl, matches_inc[0], re.MULTILINE)
                                    for matchNum_not_inc, match_not_inc in enumerate(matches_inc_br, start=1):
                                        for groupNum_not_inc in range(0, len(match_not_inc.groups())):
                                            groupNum_not_inc = groupNum_not_inc + 1
                                            note_not_included.append(match_not_inc.group(groupNum_not_inc))
                                    notes_dict[group].append({'Not Included': note_not_included})
                                notes_dict[group].append({notes_id: notes_value})
                        for df_grp in df:
                            df_grp.loc[:, 'bulletin'] = group
                            df_grp.dropna(axis=1, how='all', inplace=True)
                            df_grp.loc[(df_grp['Notes'].str.contains(notes_id)), 'inclusions'] = ','.join(note_included)
                            df_grp.loc[(df_grp['Notes'].str.contains(notes_id)), 'subject_to'] = ','.join(
                                note_not_included)
                            df_freight_tmp.append(df_grp)
                df_freight = pd.concat(df_freight_tmp, axis=0, ignore_index=True)
            return df_freight, notes_dict, date_dict

        @staticmethod
        def map_notes(group_name, notes_dict, df_freight, date_dict):
            for group in group_name:
                notes_grp_list = notes_dict[group]
                for notes_grp in notes_grp_list:
                    for key, value in notes_grp.items():
                        indexes = df_freight[
                            (df_freight['bulletin'] == group) & (df_freight['Notes'].str.contains(key))].index.tolist()
                        df_freight.loc[indexes, 'Notes'] = df_freight.loc[indexes, 'Notes'].replace(key, value,
                                                                                                    regex=True)
                try:
                    df_freight.loc[(df_freight['bulletin'] == group), 'start_date'] = date_dict[group]['Date']
                except KeyError:
                    pass
            return df_freight

        def capture(self):
            contracts_details = self.get_contracts_details()
            self.amd_no = self.get_amd_no()
            group_name = self.get_group_name()
            df_freight, notes_dict, date_dict = self.map_main_leg_table(group_name)
            df_freight = self.map_notes(group_name, notes_dict, df_freight, date_dict)
            self.line_item = self.get_line_item()
            self.captured_output = self.format_output(df_freight, contracts_details)
            # if re.search(r"ASSESSORIALS(.+?)Appendix B to Service Contract", self.raw_html) is not None:
            #     df_arb = self.arb(self.line_item, self.amd_no, date_dict)
            #     self.captured_output = self.format_output(df_freight, df_arb)
            # else:
            #     self.captured_output = self.format_output(df_freight)

        def clean(self):
            freight_df = self.captured_output['Freight']
            freight_df.drop(['CargoNature'], axis=1, inplace=True)
            # freight_df.rename(
            #     columns={'Origin': 'origin_port', 'Destination': 'destination_icd', 'Mode': 'service_type',
            #              'DestinationVia': 'destination_port', 'Service': 'loop', '20': '20GP', '40': '40GP',
            #              '40H': '40HC', 'Notes': 'remarks'
            #              }, inplace=True)
            """	
            To be removed --- Exp Doc	
            """
            freight_df["commodity"] = freight_df["bulletin"]
            rename_group = {"Group A": "General Cargo (Excluding Garments and Textiles and Consolidation Cargo),Nos",
                            "Group B": "Garments,Textiles,Consolidation Nos"
                , "Group RF": "Gummy candy,NOS"
                , "Group SEAPA": "General Cargo (Excluding Garments and Textiles and Consolidation Cargo),Nos"
                , "Group SEAPB": "Garments,Textiles,Consolidation Nos"}
            freight_df["commodity"] = freight_df['commodity'].map(rename_group)
            freight_df.rename(
                columns={'Origin': 'origin_port', 'Destination': 'destination_port', 'Mode': 'service_type',
                         'DestinationVia': 'destination_icd', 'Service': 'loop', '20': '20GP', '40': '40GP',
                         '40H': '40HC', 'Notes': 'remarks', 'OriginVia': 'origin_icd'
                         }, inplace=True)
            rename_dict = {"SABP": "Karachi, Pakistan; Mundra, India; Nhava Sheva, India",
                           "USEC": "Charleston, SC; New York, NY; Norfolk, VA; Savannah, GA; Wilmington, NC",
                           "USWC": "Long Beach, CA; Los Angeles, CA; Oakland, CA; Seattle, WA; Tacoma, WA"
                           }
            for code in rename_dict:
                _code = (rename_dict[code])
                freight_df.replace(code, _code, inplace=True, regex=True)
            # freight_df['origin_port'] = freight_df['origin_port'].str.split(';')
            #
            # freight_df['destination_port'] = freight_df['destination_port'].str.split(';')
            #
            # freight_df['destination_icd'] = freight_df['destination_icd'].str.split(';')
            # freight_df = freight_df.explode('origin_port')
            # freight_df = freight_df.explode('destination_port')
            # freight_df = freight_df.explode('destination_icd')
            # freight_df.reset_index(drop=True, inplace=True)
            """***********************"""
            freight_df['currency'] = 'USD'
            freight_df['charges_leg'] = 'L3'
            freight_df['charges'] = 'Basic Ocean Freight'
            freight_df['Amendment no.'] = self.amd_no
            freight_df['destination_port'] = freight_df['destination_port'].str.split(';')
            freight_df = freight_df.explode('destination_port')
            freight_df.reset_index(inplace=True, drop=True)
            freight_df['unique'] = self.line_item
            freight_df.loc[(freight_df['remarks'].str.contains('cannot be applied with destination arbitrary',
                                                               na=False)), 'destination_arbitrary_allowed'] = 'No'
            freight_df.loc[~(freight_df['remarks'].str.contains('cannot be applied with destination arbitrary',
                                                                na=False)), 'destination_arbitrary_allowed'] = 'Yes'
            # if re.search(r"ASSESSORIALS(.+?)Appendix B to Service Contract", self.raw_html) is not None:
            #     self.cleaned_output = {'Freight': freight_df,
            #                            'Arbitrary Charges': self.captured_output['Arbitrary Charges']}
            # else:
            self.cleaned_output = {'Freight': freight_df}


class COSCO_CEVA(Flexport_COSCO_v1):
    class cosco_fix(Flexport_COSCO_v1._CoscoAsia):

        def get_main_leg_table(self, group_name, comm_desc):
            df_freight_tmp = []
            notes_dict = defaultdict(list)
            date_dict = {}
            self.note_included_dict = {}
            note_included = []
            note_not_included = []
            for group in group_name:
                regex = 'Commodity:\t' + group + '(.+?)<p>Note (.+?)Commodity'
                if re.search(regex, self.raw_html) is None:
                    regex = 'Commodity:\t' + group + '(.+?)<p>Note (.+?)ASSESSORIALS'
                    if re.search(regex, self.raw_html) is None:
                        continue
                matches = re.finditer(regex, self.raw_html, re.MULTILINE)
                for matchNum, match in enumerate(matches, start=1):
                    group_ = match.group(1)
                    df_freight_list = pd.read_html(group_)
                    notes = 'Note ' + match.group(2)
                    regex_notes = r'Note\s+?(.+?):\t(.+?)</p><p>'
                    matches = re.finditer(regex_notes, notes, re.MULTILINE)
                    note_not_included = []
                    start_date_dict = {}
                    for matchNum_1, match_1 in enumerate(matches, start=1):
                        for groupNum_1 in range(0, len(match_1.groups()) - 1):
                            groupNum_1 = groupNum_1 + 1
                            notes_value = match_1.group(groupNum_1 + 1)
                            notes_id = match_1.group(groupNum_1)
                            notes_found = 0
                            self.note_included_dict[notes_id] = notes_value
                            # Notes_found = 1 is used to remove the notes that we are capturing in other columns.
                            if re.search(r'effective\s+?through\s+?(.+).', notes_value) is not None:
                                matches_date = re.findall(r'effective\s+?through\s+?(.+).', notes_value,
                                                          re.MULTILINE)

                                expiry_date = matches_date.pop()
                                expiry_date = parse(expiry_date)

                            if re.search(r"effective\s+?starting\s+?(.+).through(.+?)</p><p>Note", notes) is not None:
                                check_date = re.findall("effective\s+?starting\s+?(.+?)through", notes, re.MULTILINE)
                                start_date = check_date.pop()
                                start_date = parse(start_date)

                            notes_found = 1
                            note_included = []
                            if re.search(r'Rates are inclusive of', notes_value) is not None:
                                matches_inc = re.findall(
                                    r'Rates are inclusive of(.+?)\s+?Rates are not inclusive of', notes_value,
                                    re.MULTILINE)
                                regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                                matches_inc_br = re.finditer(regex_incl, matches_inc[0], re.MULTILINE)
                                for matchNum_2, match_2 in enumerate(matches_inc_br, start=1):
                                    for groupNum_2 in range(0, len(match_2.groups())):
                                        groupNum_2 = groupNum_2 + 1
                                        note_included.append(match_2.group(groupNum_2))
                                notes_dict[group].append({'Included': note_included})
                                notes_found = 1

                            """Subject to added for ceva"""
                            if re.search(r'Rates are not subject to', notes_value) is not None:
                                matches_inc = re.findall(
                                    r'Rates are not subject to(.+?)\s+?Rates are subject to', notes_value,
                                    re.MULTILINE)
                                regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                                matches_inc_br = re.finditer(regex_incl, matches_inc[0], re.MULTILINE)
                                for matchNum_2, match_2 in enumerate(matches_inc_br, start=1):
                                    for groupNum_2 in range(0, len(match_2.groups())):
                                        groupNum_2 = groupNum_2 + 1
                                        note_included.append(match_2.group(groupNum_2))
                                notes_dict[group].append({'Included': note_included})
                                notes_found = 1

                            note_not_included = []
                            if re.search(r'Rates are subject to', notes_value) is not None:
                                matches_inc = re.findall(r'Rates are subject to(.+?)$', notes_value,
                                                         re.MULTILINE)
                                regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                                matches_inc_br = re.finditer(regex_incl, matches_inc[0], re.MULTILINE)
                                for matchNum_not_inc, match_not_inc in enumerate(matches_inc_br, start=1):
                                    for groupNum_not_inc in range(0, len(match_not_inc.groups())):
                                        groupNum_not_inc = groupNum_not_inc + 1
                                        note_not_included.append(
                                            match_not_inc.group(groupNum_not_inc))
                                notes_dict[group].append({'Not Included': note_not_included})
                                notes_found = 1

                            if re.search(r'Rates are not inclusive of', notes_value) is not None:
                                matches_inc = re.findall(r'Rates are not inclusive of(.+?)$', notes_value,
                                                         re.MULTILINE)
                                regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                                matches_inc_br = re.finditer(regex_incl, matches_inc[0], re.MULTILINE)
                                for matchNum_not_inc, match_not_inc in enumerate(matches_inc_br, start=1):
                                    for groupNum_not_inc in range(0, len(match_not_inc.groups())):
                                        groupNum_not_inc = groupNum_not_inc + 1
                                        note_not_included.append(
                                            match_not_inc.group(groupNum_not_inc))
                                notes_dict[group].append({'Not Included': note_not_included})
                                notes_found = 1

                            if notes_found == 0:
                                notes_dict[group].append({notes_id: notes_value + ';'})
                            else:
                                notes_dict[group].append({notes_id: ''})

                            for df_grp in df_freight_list:
                                df_grp.dropna(axis=1, how='all', inplace=True)
                                if not df_grp.empty:
                                    df_grp.loc[(df_grp['Notes'].str.contains(notes_id)), 'inclusions'] = ','.join(
                                        note_included)
                                    df_grp.loc[(df_grp['Notes'].str.contains(notes_id)), 'subject_to'] = ','.join(
                                        note_not_included)

                    for df_grp in df_freight_list:
                        df_grp.dropna(axis=1, how='all', inplace=True)
                        if not df_grp.empty:
                            df_grp.loc[:, 'bulletin'] = group
                            df_grp.loc[:, 'commodity'] = comm_desc[group].strip()
                            df_freight_tmp.append(df_grp)
                df_freight = pd.concat(df_freight_tmp, axis=0, ignore_index=True)
                try:
                    df_freight['start_date'] = start_date
                    df_freight['expiry_date'] = expiry_date
                    df_freight['Notes'] = df_freight['Notes'].replace(self.note_included_dict)

                except UnboundLocalError:
                    pass

                df_freight.rename(
                    columns={'Origin': 'origin_icd', 'Destination': 'destination_icd', 'Mode': 'service_type',
                             "OriginVia": "origin_port", 'DestinationVia': 'destination_port',
                             '20': '20GP', '40': '40GP', '40H': '40HC', 'Notes': 'remarks'
                             }, inplace=True)

            return df_freight, notes_dict, date_dict

        def map_notes(self, group_name, notes_dict, df_freight, date_dict):
            df_freight['remarks'] = df_freight['remarks'].str.replace(';', ' ', regex=True)
            for group in group_name:
                notes_grp_list = notes_dict[group]
                for notes_grp in notes_grp_list:
                    for key, value in self.note_included_dict.items():
                        indexes = df_freight[
                            (df_freight['bulletin'] == group) & (
                                df_freight['remarks'].str.contains(key))].index.tolist()
                        df_freight.loc[indexes, 'remarks'] = df_freight.loc[indexes, 'remarks'].replace(key, value,
                                                                                                        regex=True)
                    # df_freight.loc[(df_freight['bulletin'] == group), 'expiry_date'] = date_dict[group]['Date']
                    df_freight['expiry_date'] = df_freight['remarks'].str.extract('Rates effective through\s?(.+?)\.')
                    df_freight['start_date'] = df_freight['remarks'].str.extract("Rates effective\s+?starting\s+?(.+).")
                    df_freight_split = df_freight['remarks'].str.extract(
                        "Rates effective starting (.+?) through\s?(.+?)\.")
                    df_freight_split.dropna(how='all', axis=0, inplace=True)
                    if not df_freight_split.empty:
                        df_freight.loc[(df_freight['remarks'].str.contains('Rates effective starting')), 'start_date'] = \
                            df_freight_split[0]
                        df_freight.loc[
                            (df_freight['remarks'].str.contains('Rates effective starting')), 'expiry_date'] = \
                            df_freight_split[1]
                    df_freight['expiry_date'] = pd.to_datetime(df_freight['expiry_date'])
                    df_freight['start_date'] = pd.to_datetime(df_freight['start_date'])
            return df_freight

        def arb(self, lineitem, amd_no, date_dict):
            df_arb = pd.DataFrame()

            if re.search(r"ASSESSORIALS(.+?)Appendix B to Service Contract", self.raw_html) is not None:
                regex_arb = r"ASSESSORIALS(.+?)Appendix B to Service Contract"
                matches = re.finditer(regex_arb, self.raw_html, re.MULTILINE)
                for matchNum, match in enumerate(matches, start=1):
                    for groupNum in range(0, len(match.groups())):
                        groupNum = groupNum + 1
                        group_ = match.group(groupNum)
                        notes_arb = group_
                        df = pd.read_html(group_)
                        df[0].dropna(how='all', axis=0, inplace=True)
                        df[0].dropna(how='all', axis=1, inplace=True)
                        df[0].reset_index(drop=True, inplace=True)
                        df[1].dropna(how='all', axis=0, inplace=True)
                        df[1].dropna(how='all', axis=1, inplace=True)
                        df[1].reset_index(drop=True, inplace=True)
                matches = re.finditer(regex_arb, self.raw_html, re.MULTILINE)
                for matchNum, match in enumerate(matches, start=1):
                    for groupNum in range(0, len(match.groups())):
                        groupNum = groupNum + 1
                        group_ = match.group(groupNum)
                        origin_arb = df[0]
                        origin_arb['at'] = 'Origin'
                        destination_arb = df[1]
                        destination_arb['at'] = 'Destination'
                        df_arb = pd.concat([origin_arb, destination_arb], ignore_index=True)
                notes_dict_arb = {}

                regex_arb_notes = r'Note\s+?(.+?):\t(.+?)</p><p>'
                matches_notes = re.finditer(regex_arb_notes, notes_arb, re.MULTILINE)
                arb_dict = {}

                notes_list = []
                notes_description = []
                # notes_dict={}
                for matchNum_note, match_note in enumerate(matches_notes, start=1):
                    for groupNum_note in range(0, len(match_note.groups()) - 1):
                        groupNum_note = groupNum_note + 1
                        notes_value = match_note.group(groupNum_note + 1)
                        notes_id = match_note.group(groupNum_note)
                        print(notes_id, notes_value)
                        notes_list.append(notes_id)
                        notes_description.append(notes_value)
                        notes_dict = dict(zip(notes_list, notes_description))
                        if re.search(r'effective\s+?through\s+?(.+).', notes_value) is not None:
                            matches_date = re.findall(r'effective\s+?through\s+?(.+).', notes_value, re.MULTILINE)
                            date_dict['Date'] = matches_date
                            arb_dict[notes_id] = matches_date
                            # notes_dict={}
            # df_arb['expiry_date']=""
            df_arb['expiry_date'] = df_arb['Notes']
            df_arb['expiry_date'] = df_arb['expiry_date'].replace(arb_dict)
            df_arb.drop(df_arb.index[49], inplace=True)
            df_arb['expiry_date'] = pd.to_datetime(df_arb['expiry_date'])
            df_arb['Notes'] = df_arb['Notes'].replace(notes_dict)
            df_arb['currency'] = 'USD'
            df_arb['charges_leg'] = 'L2'
            # df_arb['charges'] = 'origin arbitrary charges'
            # df_arb['start_date']=""
            # df_arb['start_date']=

            # df_arb['expiry_date'] = notes_dict_arb
            # df_arb['at'] = 'origin'
            df_arb.rename(
                columns={"20": "20GP", "40": '40GP', "40H": "40HC", 'Location': 'icd', 'Over': 'to', 'Via': 'via',
                         'Mode': 'service_type', 'TransportType': 'mode_of_transportation', 'Notes': 'remarks'},
                inplace=True)
            df_arb['unique'] = lineitem
            df_arb['Amendment no.'] = amd_no
            """df_arb['expiry_date'] = parse("March 31 , 2021")
            df_arb['expiry_date'] = df_arb['remarks'].str.extract('Rates effective through\s?(.+?)\.')
            df_arb['start_date'] = df_arb['remarks'].str.extract("Rates effective\s+?starting\s+?(.+).")
            df_arb_split = df_arb['remarks'].str.extract("Rates effective starting (.+?) through\s?(.+?)\.")
            df_arb_split.dropna(how='all', axis=0, inplace=True)
            if not df_arb_split.empty:
                df_arb.loc[(df_arb['remarks'].str.contains('Rates effective starting')), 'start_date'] = df_arb_split[0]
                df_arb.loc[(df_arb['remarks'].str.contains('Rates effective starting')), 'expiry_date'] = df_arb_split[1]
            df_arb['expiry_date'] = df_arb['remarks'].str.extract('Rates effective through\s?(.+?)\.')"""
            return df_arb

        def melt_load_type(self, df):
            if "load_type" not in df:
                df = df.melt(id_vars=[column for column in df.columns if not column[1].isdigit()],
                             var_name="load_type",
                             value_name="amount")
                return df

        def capture(self):
            super().capture()

        def clean(self):
            freight_df = self.captured_output['Freight']
            freight_df.drop(['CargoNature'], axis=1, inplace=True)
            freight_df.rename(
                columns={'Origin': 'origin_port', 'Destination': 'destination_port', 'Mode': 'service_type',
                         'DestinationVia': 'destination_icd', '20': '20GP', '40': '40GP',
                         '40H': '40HC', 'Notes': 'remarks', 'OriginVia': 'origin_icd'
                         }, inplace=True)
            freight_df['origin_port'] = freight_df['origin_port'].str.split(';')

            freight_df['destination_port'] = freight_df['destination_port'].str.split(';')

            freight_df['destination_icd'] = freight_df['destination_icd'].str.split(';')
            freight_df = freight_df.explode('origin_port')
            freight_df = freight_df.explode('destination_port')
            freight_df = freight_df.explode('destination_icd')
            freight_df.reset_index(drop=True, inplace=True)
            if 'start_date' in freight_df:
                freight_df['start_date'] = freight_df['start_date'].dt.date
            if 'expiry_date' in freight_df:
                freight_df['expiry_date'] = freight_df['expiry_date'].dt.date
            arb_df = self.captured_output['Arbitrary Charges']
            if 'start_date' in arb_df:
                arb_df['start_date'] = pd.to_datetime(arb_df['start_date'], errors='coerce').dt.date
            if 'expiry_date' in arb_df:
                arb_df['expiry_date'] = pd.to_datetime(arb_df['expiry_date'], errors='coerce').dt.date
            freight_df['currency'] = 'USD'
            freight_df['charges_leg'] = 'L3'
            freight_df['charges'] = 'Basic Ocean Freight'
            freight_df['Amendment no.'] = self.amd_no
            freight_df['destination_port'] = freight_df['destination_port'].str.split(';')
            freight_df = freight_df.explode('destination_port')
            freight_df.reset_index(inplace=True, drop=True)
            freight_df['unique'] = self.line_item
            freight_df.loc[(freight_df['remarks'].str.contains('cannot be applied with destination arbitrary',
                                                               na=False)), 'destinaiton_arbitrary_allowed'] = 'Yes'
            freight_df.loc[~(freight_df['remarks'].str.contains('cannot be applied with destination arbitrary',
                                                                na=False)), 'destinaiton_arbitrary_allowed'] = 'No'
            if 'Arbitrary Charges' in self.captured_output:
                self.cleaned_output = {'Freight': freight_df, 'Arbitrary Charges': arb_df}
            else:
                self.cleaned_output = {'Freight': freight_df}


class Ceva_Cosco_AsiaPacific(BaseTemplate):
    class Ceva_Australia(BaseFix):
        def check_input(self):

            pass

        def check_output(self):
            pass

        def capture(self):
            freight_df = self.df
            POL = freight_df.iloc[:, 0]
            index = [index for index in freight_df.iloc[0, 1:].index if freight_df.iloc[0, 1:][index] != '']
            if freight_df[0].str.contains('Start').any():
                start_date = freight_df[(freight_df[0].str.contains('Start'))].index.values[0]
                start_date_df = freight_df.loc[int(start_date)][1]
            if freight_df[0].str.contains('End').any():
                expiry_date = freight_df[(freight_df[0].str.contains('End'))].index.values[0]
                expiry_date_df = freight_df.loc[int(expiry_date)][1]
            if freight_df[0].str.contains('including').any():
                inclusion_data = freight_df[(freight_df[0].str.contains('including'))].index.values[0]
                inclusions = freight_df.loc[int(inclusion_data)][0]

            def get_data(inclusion_data):
                return re.search(r"including(.+?), subject to(.+?)and any other surcharges", inclusion_data)

            captured_data = freight_df.iloc[:, 0].apply(lambda x: get_data(str(x)))
            for i in captured_data:
                if i:
                    inclusive = i.group(1)
                    subject_to = i.group(2)
                    subject_to = subject_to.replace('/', ';')

            freight_df_aus = []
            for element in range(len(index)):
                holder = pd.concat([POL, freight_df.iloc[:, index[element]:index[element + 1]]], axis=1)
                holder['destination_port'] = holder.iloc[0, 1]
                holder['destination_port'].iloc[1] = 'destination_port'
                holder.columns = holder.iloc[1, :]
                holder = holder.iloc[2:, :]
                freight_df_aus.append(holder)
                # freight_df.append(holder)
                if element + 2 == len(index):
                    holder = pd.concat([POL, freight_df.iloc[:, index[element + 1]:]], axis=1)
                    holder['destination_port'] = holder.iloc[0, 1]
                    holder['destination_port'].iloc[1] = 'destination_port'
                    holder.columns = holder.iloc[1, :]
                    holder = holder.iloc[2:, :]
                    freight_df_aus.append(holder)
                    # freight_df.append(holder)
                    break
            # for i in freight_df_aus[2]['destination_port'] and freight_df_aus[4]['destination_port']:
            #     if freight_df_aus[2]['destination_port'] and freight_df_aus[4]['destination_port'].str.contains('Brisbane').any():
            freight_df_aus_jk = np.where(freight_df_aus[2]['20GP'] == freight_df_aus[4]['20GP'], 'True', 'False')
            if freight_df_aus_jk[0] is False:
                freight_df_aus[4]['service_remarks'] = 'JKN'
            freight_df_aus[4]['service_remarks'] = 'JKN'
            freight_df_mel = freight_df_aus[0]
            freight_df_mel.reset_index(drop=True, inplace=True)
            if freight_df_mel['Port Of Loading'].str.contains('Above rates').any():
                data = freight_df_mel[(freight_df_mel['Port Of Loading'].str.contains('Above rates'))].index.values[0]
                freight_df_mel = freight_df_mel.iloc[0:data]
            freight_df_sydney = freight_df_aus[1]
            freight_df_sydney.reset_index(drop=True, inplace=True)
            freight_df_sydney = freight_df_sydney.iloc[0:data]
            freight_df_brisbane = freight_df_aus[2]
            freight_df_brisbane.reset_index(drop=True, inplace=True)
            freight_df_brisbane = freight_df_brisbane.iloc[0:data]
            freight_df_adelaide = freight_df_aus[3]
            freight_df_adelaide.reset_index(drop=True, inplace=True)
            freight_df_adelaide = freight_df_adelaide.iloc[0:data]
            freight_df_brisbane_exp = freight_df_aus[4]
            freight_df_brisbane_exp.reset_index(drop=True, inplace=True)
            freight_df_brisbane_exp = freight_df_brisbane_exp.iloc[0:data]
            df_freight_aus = pd.concat(
                [freight_df_mel, freight_df_sydney, freight_df_brisbane, freight_df_adelaide, freight_df_brisbane_exp],
                axis=0)
            df_freight_aus["start_date"] = start_date_df
            df_freight_aus["start_date"] = pd.to_datetime(df_freight_aus['start_date'])
            df_freight_aus["expiry_date"] = expiry_date_df
            df_freight_aus['expiry_date'] = pd.to_datetime(df_freight_aus['expiry_date'])
            df_freight_aus['commodity'] = 'Freight All kind'
            df_freight_aus['inclusions'] = inclusive
            df_freight_aus['subject_to'] = subject_to
            # for i in df_freight_aus['destination_port']

            df_freight_faf = df_freight_aus.copy()
            self.captured_output = {'Freight': df_freight_aus}

        def clean(self):

            # df=self.captured_output['Freight']
            cleaned_df = self.captured_output['Freight']
            cleaned_df.rename(columns={'Port Of Loading': 'origin'}, inplace=True)
            cleaned_df.reset_index(inplace=True, drop=True)
            self.cleaned_output = cleaned_df

    class Ceva_New_Zealand(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):
            freight_df = self.df
            POL = freight_df.iloc[:, 0]
            index = [index for index in freight_df.iloc[0, 1:].index if freight_df.iloc[0, 1:][index] != '']
            if freight_df[0].str.contains('Start').any():
                start_date = freight_df[(freight_df[0].str.contains('Start'))].index.values[0]
                start_date_df = freight_df.loc[int(start_date)][1]
            if freight_df[0].str.contains('End').any():
                expiry_date = freight_df[(freight_df[0].str.contains('End'))].index.values[0]
                expiry_date_df = freight_df.loc[int(expiry_date)][1]
            if freight_df[0].str.contains('including').any():
                inclusion_data = freight_df[(freight_df[0].str.contains('including'))].index.values[0]
                inclusions = freight_df.loc[int(inclusion_data)][0]

            def get_data(inclusion_data):
                return re.search(r"including(.+?), subject to(.+?)and any other surcharges", inclusion_data)

            captured_data = freight_df.iloc[:, 0].apply(lambda x: get_data(str(x)))
            for i in captured_data:
                if i:
                    inclusive = i.group(1)
                    subject_to = i.group(2)
                    subject_to = subject_to.replace('/', ';')

            freight_df_nz = []
            for element in range(len(index)):
                holder = pd.concat([POL, freight_df.iloc[:, index[element]:index[element + 1]]], axis=1)
                holder['destination_port'] = holder.iloc[0, 1]
                holder['destination_port'].iloc[1] = 'destination_port'
                holder.columns = holder.iloc[1, :]
                holder = holder.iloc[2:, :]
                freight_df_nz.append(holder)
                # freight_df.append(holder)
                if element + 2 == len(index):
                    holder = pd.concat([POL, freight_df.iloc[:, index[element + 1]:]], axis=1)
                    holder['destination_port'] = holder.iloc[0, 1]
                    holder['destination_port'].iloc[1] = 'destination_port'
                    holder.columns = holder.iloc[1, :]
                    holder = holder.iloc[2:, :]
                    freight_df_nz.append(holder)
                    # freight_df.append(holder)
                    break

            freight_df_ack = freight_df_nz[0]
            freight_df_ack.reset_index(drop=True, inplace=True)
            if freight_df_ack['Port Of Loading'].str.contains('Above rates').any():
                data = freight_df_ack[(freight_df_ack['Port Of Loading'].str.contains('Above rates'))].index.values[0]
                freight_df_ack = freight_df_ack.iloc[0:data]
            freight_df_cntrbry = freight_df_nz[1]
            freight_df_cntrbry.reset_index(drop=True, inplace=True)
            freight_df_cntrbry = freight_df_cntrbry.iloc[0:data]
            freight_df_hwk = freight_df_nz[2]
            freight_df_hwk.reset_index(drop=True, inplace=True)
            freight_df_hwk = freight_df_hwk.iloc[0:data]
            freight_df_BOP = freight_df_nz[3]
            freight_df_BOP.reset_index(drop=True, inplace=True)
            freight_df_BOP = freight_df_BOP.iloc[0:data]
            freight_df_wltn = freight_df_nz[4]
            freight_df_wltn.reset_index(drop=True, inplace=True)
            freight_df_wltn = freight_df_wltn.iloc[0:data]
            df_freight_nz = pd.concat(
                [freight_df_ack, freight_df_cntrbry, freight_df_hwk, freight_df_BOP, freight_df_wltn],
                axis=0)
            df_freight_nz["start_date"] = start_date_df
            df_freight_nz["start_date"] = pd.to_datetime(df_freight_nz['start_date'])
            df_freight_nz["expiry_date"] = expiry_date_df
            df_freight_nz['expiry_date'] = pd.to_datetime(df_freight_nz['expiry_date'])
            df_freight_nz['commodity'] = 'Freight All kind'
            df_freight_nz['inclusions'] = inclusive
            df_freight_nz['subject_to'] = subject_to
            df_freight_faf = df_freight_nz.copy()
            self.captured_output = {'Freight': df_freight_nz}

        def clean(self):
            # df=self.captured_output['Freight']
            cleaned_df = self.captured_output['Freight']
            cleaned_df.rename(columns={'Port Of Loading': 'origin'}, inplace=True)
            cleaned_df.reset_index(inplace=True, drop=True)
            self.cleaned_output = cleaned_df

    class Ceva_South_Pacific(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):
            freight_df = self.df
            POL = freight_df.iloc[:, 0]
            index = [index for index in freight_df.iloc[0, 1:].index if freight_df.iloc[0, 1:][index] != '']
            if freight_df[0].str.contains('Start').any():
                start_date = freight_df[(freight_df[0].str.contains('Start'))].index.values[0]
                start_date_df = freight_df.loc[int(start_date)][1]
            if freight_df[0].str.contains('End').any():
                expiry_date = freight_df[(freight_df[0].str.contains('End'))].index.values[0]
                expiry_date_df = freight_df.loc[int(expiry_date)][1]
            if freight_df[0].str.contains('including').any():
                inclusion_data = freight_df[(freight_df[0].str.contains('including'))].index.values[0]
                inclusions = freight_df.loc[int(inclusion_data)][0]

            def get_data(inclusion_data):
                return re.search(r"including(.+?), subject to(.+?)and any other surcharges", inclusion_data)

            captured_data = freight_df.iloc[:, 0].apply(lambda x: get_data(str(x)))
            for i in captured_data:
                if i:
                    inclusive = i.group(1)
                    subject_to = i.group(2)
                    subject_to = subject_to.replace('/', ';')
            freight_df_sp = []
            for element in range(len(index)):
                holder = pd.concat([POL, freight_df.iloc[:, index[element]:index[element + 1]]], axis=1)
                holder['destination_port'] = holder.iloc[0, 1]
                holder['destination_port'].iloc[1] = 'destination_port'
                holder.columns = holder.iloc[1, :]
                holder = holder.iloc[2:, :]
                freight_df_sp.append(holder)
                # freight_df.append(holder)
                if element + 2 == len(index):
                    holder = pd.concat([POL, freight_df.iloc[:, index[element + 1]:]], axis=1)
                    holder['destination_port'] = holder.iloc[0, 1]
                    holder['destination_port'].iloc[1] = 'destination_port'
                    holder.columns = holder.iloc[1, :]
                    holder = holder.iloc[2:, :]
                    freight_df_sp.append(holder)
                    # freight_df.append(holder)
                    break
            freight_df_queen = freight_df_sp[0]
            freight_df_queen.reset_index(drop=True, inplace=True)
            if freight_df_queen['Port Of Loading'].str.contains('Above rates').any():
                data = freight_df_queen[(freight_df_queen['Port Of Loading'].str.contains('Above rates'))].index.values[
                    0]
                freight_df_queen = freight_df_queen.iloc[0:data]
            freight_df_png = freight_df_sp[1]
            freight_df_png.reset_index(drop=True, inplace=True)
            freight_df_png = freight_df_png.iloc[0:data]
            df_freight_sp = pd.concat(
                [freight_df_queen, freight_df_png],
                axis=0)
            df_freight_sp["start_date"] = start_date_df
            df_freight_sp["start_date"] = pd.to_datetime(df_freight_sp['start_date'])
            df_freight_sp["expiry_date"] = expiry_date_df
            df_freight_sp['expiry_date'] = pd.to_datetime(df_freight_sp['expiry_date'])
            df_freight_sp['commodity'] = 'Freight All kind'
            df_freight_sp['inclusions'] = inclusive
            df_freight_sp['subject_to'] = subject_to
            df_freight_faf = df_freight_sp.copy()
            self.captured_output = {'Freight': df_freight_sp}

        def clean(self):
            cleaned_df = self.captured_output['Freight']
            cleaned_df.rename(columns={'Port Of Loading': 'origin'}, inplace=True)
            cleaned_df.reset_index(inplace=True, drop=True)
            self.cleaned_output = cleaned_df

    class Ceva_faf_amt(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):
            faf_df = self.df

            # if faf_df[1].str.contains('From/To').any():
            #     if faf_df[1].str.contains('To Australia').any():
            #         faf_index = faf_df[(faf_df[1].str.contains('To Australia'))].index.values[0]
            #         faf_rate = faf_df.loc[int(faf_index)][6]
            # aus_faf=[]
            # aus_faf_20gp= faf_rate
            # aus_faf_40gp= faf_rate*2
            # aus_faf_40hc=faf_rate*2
            self.captured_output = faf_df

        def clean(self):
            self.cleaned_output = self.captured_output

    def resolve_dependency(cls, fix_outputs):
        freight_df_nz = fix_outputs.pop('NEW ZEALAND')
        freight_df_aus = fix_outputs.pop('AUSTRALIA')
        freight_df_sp = fix_outputs.pop('SOUTH PACIFIC')
        faf_df = fix_outputs.pop('FAF Amount')

        freight_df_aus_date = freight_df_aus['start_date'].dt.month
        freight_df_sp_date = freight_df_sp['start_date'].dt.month
        freight_df_nz_date = freight_df_nz['start_date'].dt.month

        months = {'Jan': '1', 'Feb': '2', 'Mar': '3', 'Apr': '4', 'May': '5', 'Jun': '6',
                  'Jul': '7', 'Aug': '8', 'Sep': '9', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
        faf_df = faf_df.loc[:, 0:].replace(months)
        faf_df = faf_df[1:].reset_index(drop=True)
        faf_df.columns = faf_df.iloc[0, :]
        faf_df = faf_df.drop(['Trade Lane', 'Tariff Code', 'Bunker code'], axis=1)
        if faf_df['From/To'].str.contains('To Australia').any():
            faf_index_aus = faf_df[(faf_df['From/To'].str.contains('To Australia'))].index.values[0]
            for i in faf_df.columns:
                if i == str(freight_df_aus_date[0]):
                    faf_rate = faf_df.loc[faf_index_aus, i]
                    freight_df_aus['20GP_faf_aus'] = faf_rate
                    freight_df_aus['40GP_faf_aus'] = faf_rate * 2
                    freight_df_aus['40HQ_faf_aus'] = faf_rate * 2
                    break

        if faf_df['From/To'].str.contains('To New Zealand /South Pacific').any():
            for i in faf_df.columns:
                if i == str(freight_df_sp_date[0]):
                    faf_index_nz_sp = \
                    faf_df[(faf_df['From/To'].str.contains('To New Zealand /South Pacific'))].index.values[0]
                    faf_rate_sp = faf_df.loc[faf_index_nz_sp, i]
                    freight_df_sp['20GP_faf_nz_sp'] = faf_rate_sp
                    freight_df_sp['40GP_faf_nz_sp'] = faf_rate_sp * 2
                    freight_df_sp['40HQ_faf_nz_sp'] = faf_rate_sp * 2
                    break
        if faf_df['From/To'].str.contains('To New Zealand /South Pacific').any():
            for i in faf_df.columns:
                if i == str(freight_df_sp_date[0]):
                    faf_index_nz_sp = \
                    faf_df[(faf_df['From/To'].str.contains('To New Zealand /South Pacific'))].index.values[0]
                    faf_rate_nz = faf_df.loc[faf_index_nz_sp, i]
                    freight_df_nz['20GP_faf_nz_sp'] = faf_rate_nz
                    freight_df_nz['40GP_faf_nz_sp'] = faf_rate_nz * 2
                    freight_df_nz['40HQ_faf_nz_sp'] = faf_rate_nz * 2
                    break
        df = pd.concat([freight_df_nz, freight_df_aus, freight_df_sp], ignore_index=True)
        df = df.drop([0])
        df = df.reset_index(drop=True)
        fix_outputs = {'Freight': {'Freight': df}}
        return fix_outputs


class COSCO_Karl_Cross_V1(BaseTemplate):
    class Oceanfreight_Fix(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_contracts_details(self):
            contracts_details = {}
            if self.df[0].str.contains('VALID FROM').any():
                start_date_index = self.df[(self.df[0].str.contains('VALID FROM'))].index.values[0]
                start_date = self.df.loc[int(start_date_index)][1]
                contracts_details["start_date"] = start_date.split("-")[0]

            if self.df[0].str.contains('VALID UNTIL').any():
                expiry_index = self.df[(self.df[0].str.contains('VALID UNTIL'))].index.values[0]
                expiry = self.df.loc[int(expiry_index)][1]
                contracts_details["expiry"] = expiry.split("-")[0]

            if self.df[0].str.contains('COMMODITY').any():
                commodity_index = self.df[(self.df[0].str.contains('COMMODITY'))].index.values[0]
                contracts_details["commodity"] = self.df.loc[int(commodity_index)][1]

            return contracts_details

        def get_surcharges(self):
            surcharges_list = []
            self.df.fillna("", inplace=True)
            if self.df[0].str.contains('FREIGHT BASIS').any():
                freight_basis = self.df[(self.df[0].str.contains('FREIGHT BASIS'))].index.values[0]
                freight_basis_ = self.df.loc[int(freight_basis)][1]
                inclusions = re.search("Inclusive (.*).-", freight_basis_).group(1)

            if self.df[0].str.contains('BAF').any():
                freight_baf = self.df[(self.df[0].str.contains('BAF'))].index.values[0]
                freight_baf_ = self.df.loc[int(freight_baf)][1]
                freight_baf_captured = re.search(r"((USD.)(\d+),-.)\/.(\w+).*-.(\w+).\/.*", freight_baf_)
                BAF = self.df.loc[int(freight_baf)][0].split("(")[0]
                surcharges = {}
                surcharges["charges"] = BAF
                surcharges["currency"] = freight_baf_captured.group(2)
                surcharges["amount"] = freight_baf_captured.group(3)
                surcharges["load_type"] = freight_baf_captured.group(4)
                surcharges["remarks"] = freight_baf_captured.group(5)
                # surcharges["code"] = self.df.loc[int(freight_baf)][0].split("(")[1].replace(")","")
                surcharges_list.append(surcharges)

            if self.df[0].str.contains('AFS').any():
                freight_AFS = self.df[(self.df[0].str.contains('AFS'))].index.values[0]
                freight_AFS_ = self.df.loc[int(freight_AFS)][1]
                freight_baf_captured = re.search(r"(USD.)(\d+)(.*)-", freight_AFS_)
                surcharges = {}
                BAF = self.df.loc[int(freight_AFS)][0].split("(")[0]
                surcharges["charges"] = BAF
                surcharges["currency"] = freight_baf_captured.group(1)
                surcharges["amount"] = freight_baf_captured.group(2)
                surcharges["load_type"] = freight_baf_captured.group(3)
                # surcharges["remarks"] = freight_baf_captured.group(5)
                # surcharges["code"] = self.df.loc[int(freight_AFS)][0].split("(")[2].replace(")","")
                # surcharges["destination_country"] = self.df.loc[int(freight_AFS)][0].split("(")[1].replace(")","" )
                surcharges_list.append(surcharges)

            return inclusions, surcharges_list

        def get_regional_sections(self):
            regional_sections = {}
            end_index = self.df[self.df[0].str.contains("REMARKS")].index.values[0]
            indexes = self.df[self.df[0].str.contains("PORT OF LOADING:")].index.tolist()
            indexes.append(end_index)
            regional_sections = zip(indexes, indexes[1:])
            return regional_sections

        def melt_load_type(self, df):
            if "load_type" not in df:
                df = df.melt(id_vars=[column for column in df.columns if not column[1].isdigit()],
                             var_name="load_type",
                             value_name="amount")
                return df

        def get_blocks(self):
            contracts_details = self.get_contracts_details()
            regional_sections = self.get_regional_sections()
            dps = []
            for regional_config in regional_sections:
                regional_df = self.df.loc[regional_config[0]:regional_config[1] - 1, :]
                if len(regional_df.columns) == 10:
                    origin_port = regional_df[0].values[0].split(":")[1]
                    regional_df.columns = columns = ["destination_country", "destination_port", "20GP", "40GP", "40HC",
                                                     "via_port", "basis", "service_type", "remarks", "drop1"]
                    regional_df.replace("", nan, inplace=True)
                    regional_df.dropna(subset=["destination_port"], inplace=True)
                    regional_df = regional_df[regional_df.destination_port != 'DESTINATION']
                    regional_df.loc[(regional_df["20GP"].astype(str).str.contains("on request")) & (
                        regional_df["40GP"].isna()), "40GP"] = "ON REQUEST"
                    regional_df.loc[(regional_df["20GP"].astype(str).str.contains("on request")) & (
                        regional_df["40HC"].isna()), "40HC"] = "ON REQUEST"
                    regional_df.loc[regional_df["20GP"].astype(str).str.contains("on request"), "20GP"] = "ON REQUEST"

                    regional_df = regional_df.drop(
                        columns=[column for column in regional_df.columns if column.startswith('drop')])
                    regional_df["origin_port"] = origin_port
                    regional_df["remarks"].fillna(method="ffill", inplace=True)
                    regional_df["destination_country"].fillna(method="ffill", inplace=True)
                    dps.append(regional_df)
            df = pd.concat(dps, ignore_index=True)
            df = self.melt_load_type(df)
            df["subject_to"] = df["basis"]
            df["start_date"] = contracts_details["start_date"]
            df["expiry_date"] = contracts_details["expiry"]
            df["commodity"] = contracts_details["commodity"]
            df['start_date'] = pd.to_datetime(df['start_date']).apply(
                lambda x: x.strftime("%Y-%m-%d") if isinstance(x, pd.Timestamp) else nan)
            df['expiry_date'] = pd.to_datetime(df['expiry_date']).apply(
                lambda x: x.strftime("%Y-%m-%d") if isinstance(x, pd.Timestamp) else nan)
            df["charges"] = "Basic Ocean Freight"
            df["charge_leg"] = "L3"
            df["currency"] = "USD"
            df["basis"] = "container"
            # df["inclusions"] = inclusions
            df["remarks"] = "Agreement Number:" + " " + df["remarks"].astype(str)

            return df

        def capture(self):
            freight_df = self.get_blocks()
            inclusions, surcharges = self.get_surcharges()
            freight_df["inclusions"] = inclusions

            surcharges_df = pd.DataFrame(surcharges)
            self.captured_output = {'Freight': freight_df}

            # self.captured_output = {'Freight': freight_df ,"Surcharges": surcharges_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    class Arbitary_Fix(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def melt_load_type(self, df):
            if "load_type" not in df:
                df = df.melt(id_vars=[column for column in df.columns if not column[1].isdigit()],
                             var_name="load_type",
                             value_name="amount")
                return df

        def capture(self):
            df = self.df
            df.replace("", nan, inplace=True)
            df.columns = ["to", "service_type", "via", "20GP", "40GP", "40HC", "remarks"]
            df.dropna(subset=["via", "to"], inplace=True)
            df = df[df.to != 'DESTINATION']
            df["remarks"].fillna(method="ffill", inplace=True)

            df.columns = ['to', 'service_type', 'via', '20GP', '40GP', '40HC', 'remarks']

            df = df.loc[~df["20GP"].astype(str).str.contains('no service anymore', na=False)]

            df.loc[(df["20GP"].astype(str).str.contains(r"currently suspended", na=False)) & (
                df["40GP"].isna()), "40GP"] = "ON REQUEST"
            df.loc[(df["20GP"].astype(str).str.contains(r"currently suspended", na=False)) & (
                df["40HC"].isna()), "40HC"] = "ON REQUEST"

            # df.loc[df["20GP"].astype(str).str.contains(r"currently suspended"), "remarks"] += "\ncurrently suspended"
            # df.loc[df["40GP"].astype(str).str.contains(r"ON REQUEST"), "remarks"] += "\ncurrently suspended"
            df.loc[df["40HC"].astype(str).str.contains(r"ON REQUEST", na=False), "remarks"] += "\ncurrently suspended"

            df.loc[df["20GP"].astype(str).str.contains(r"currently suspended"), "20GP"] = "ON REQUEST"

            df.loc[(df["20GP"].astype(str).str.contains(r"only available online via Syncon Hub", na=False)) & (
                df["40GP"].isna()), "40GP"] = "ON REQUEST"
            df.loc[(df["20GP"].astype(str).str.contains(r"only available online via Syncon Hub", na=False)) & (
                df["40HC"].isna()), "40HC"] = "ON REQUEST"
            df.loc[df["20GP"].astype(str).str.contains(r"only available online via Syncon Hub",
                                                       na=False), "20GP"] = "ON REQUEST"

            df = self.melt_load_type(df)
            df["at"] = "destination"
            df["charges"] = "Destination Arbitary Charges"
            df["basis"] = "container"
            df["currency"] = "USD"

            self.captured_output = {'Arbitrary Charges': df}

        def clean(self):
            self.cleaned_output = self.captured_output

    def resolve_dependency(cls, fix_outputs):
        def apply_arbitary_charges(Freight_df, arbitary_df):

            Freight_destination_dict = Freight_df.to_dict("records")
            dps = []
            for row in Freight_destination_dict:
                filtered_df = arbitary_df.loc[(arbitary_df["via"].astype(str).str.contains(
                    row["destination_port"].replace("Port Kelang", "Port Klang"), na=False, case=False)) &
                                              (arbitary_df["load_type"] == row["load_type"])]
                filtered_df["icd"] = row["origin_port"]
                filtered_df["remarks"] = row["remarks"]

                filter_numeric_df = filtered_df.loc[filtered_df['amount'].apply(lambda x: type(x) in [int])]
                filter_numeric_df["amount"] += row["amount"]

                filter_non_numeric_df = filtered_df.loc[filtered_df['amount'].apply(lambda x: type(x) in [str])]

                filtered_df_concat = pd.concat([filter_numeric_df, filter_non_numeric_df], ignore_index=True)
                dps.append(filtered_df_concat)

            df = pd.concat(dps, ignore_index=True)
            return df

        Freight_sheet = fix_outputs.pop('Oceanfreight')
        Freight_df = Freight_sheet["Freight"]
        Freight_df["unique"] = "Oceanfreight"

        Surcharges_df = Freight_sheet["Surcharges"]

        if "Chinese Out-Port Additionals" in fix_outputs:
            Arbitary_sheet = fix_outputs.pop('Chinese Out-Port Additionals')
            chinese_arbitary_df = Arbitary_sheet['Arbitrary Charges']
            chinese_arbitary_df["unique"] = "Chinese Out-Port Additionals"

        if "Far-East Out-Port Additionals" in fix_outputs:
            Arbitary_sheet = fix_outputs.pop('Far-East Out-Port Additionals')
            fareast_arbitary_df = Arbitary_sheet['Arbitrary Charges']
            fareast_arbitary_df["unique"] = "Far-East Out-Port Additionals"

        arbitary_df = pd.concat([chinese_arbitary_df, fareast_arbitary_df], ignore_index=False)

        arbitary_df["start_date"] = Freight_df["start_date"].iloc[0]
        arbitary_df["expiry_date"] = Freight_df["expiry_date"].iloc[0]

        arbitary_df = apply_arbitary_charges(Freight_df, arbitary_df)

        # arbitary_df = concat([arbitary_df, freight_with_arbitary_df], ignore_index=False)
        arbitary_df = arbitary_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        Freight_df = Freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        Freight_df["sub_vendor"] = "COSCOSHIPPINGLINES(GERMANY)GMBH(DE-20457HAMBURG)"
        Freight_df["contract_number"] = "G1005017"

        arbitary_df["sub_vendor"] = "COSCOSHIPPINGLINES(GERMANY)GMBH(DE-20457HAMBURG)"
        arbitary_df["contract_number"] = "G1005017"
        fix_outputs = [{"Freight": Freight_df, "Arbitrary": arbitary_df}]

        # fix_outputs =[{"Freight": Freight_df, "Arbitrary": arbitary_df, "Surcharges" : Surcharges_df}]
        return fix_outputs



class KarlGrossCoscoIntraEurope(BaseTemplate):
    class SouthBoundDirect(BaseFix):

        def check_input(self):

            pass

        def get_dfs(self):

            yellow_index = None
            if self.df[0].str.contains('Yellow Highlight', na=False).any():
                yellow_index = list(self.df[self.df[0].str.contains('Yellow Highlight', na=False)].index)[0]

            if yellow_index is not None:
                return self.df.iloc[:yellow_index, :], self.df.iloc[yellow_index:, :]
            else:
                return self.df, yellow_index

        @staticmethod
        def get_headers(df):

            dates, commodity, currency_code = {}, None, None
            if df[0].str.contains('VALID FROM', na=False).any():
                start_date_index = list(df[df[0].str.contains('VALID FROM', na=False)].index)[0]
                start_date = pd.to_datetime(df.loc[start_date_index, 1].split('-')[0].strip(), format="%d.%m.%Y").date()
                dates['start_date'] = start_date
            if df[0].str.contains('VALID UNTIL', na=False).any():
                expiry_date_index = list(df[df[0].str.contains('VALID UNTIL', na=False)].index)[0]
                expiry_date = pd.to_datetime(df.loc[expiry_date_index, 1].split('-')[0].strip(),
                                             format="%d.%m.%Y").date()
                dates['expiry_date'] = expiry_date
            if df[0].str.contains('COMMODITY', na=False).any():
                commodity_index = list(df[df[0].str.contains('COMMODITY', na=False)].index)[0]
                commodity = df.loc[commodity_index, 1]
            if df[0].str.contains('CURRENCY', na=False).any():
                currency_index = list(df[df[0].str.contains('CURRENCY', na=False)].index)[0]
                currency_code = df.loc[currency_index, 1].split()[-1]
                if currency_code == 'Euro':
                    currency_code = 'EUR'

            return dates, commodity, currency_code

        @staticmethod
        def pivot_table(df):

            df = df.fillna('')
            df = df.pivot_table(
                index=[column for column in df.columns if column not in ['EQ / Agreement', list(df.columns)[-2]]],
                columns=['EQ / Agreement'],
                values=[list(df.columns)[-2]],
                aggfunc='first')
            df = df.reset_index()

            new_columns = []
            for index in df.columns.to_flat_index():
                if index[-1] != '':
                    new_columns.append(index[1])
                else:
                    new_columns.append(index[0])

            df.columns = new_columns

            return df

        @staticmethod
        def get_freight_df(df, pols):

            dfs = []
            start_index = list(df[df[0].str.contains('Destination')].index)[0]
            end_index = list(df[df[0].str.contains('REMARKS')].index)[0]
            df = df.loc[start_index:end_index - 1, :]
            indices = [df.iloc[0, :][df.iloc[0, :].str.contains(x, na=False)].index.values[0] for x in df.iloc[0, :]
                       if x.split()[0] in pols
                       ]
            main_df = pd.concat([df.loc[:, :indices[0] - 1], df.loc[:, indices[-1] + 1:]], axis=1, ignore_index=True)
            for index in indices:
                holder = pd.concat([main_df, df.loc[:, index]], axis=1, ignore_index=True)
                holder.replace('', nan, inplace=True)
                holder = holder.ffill(axis=0)
                holder.columns = holder.iloc[0, :]
                holder = holder.iloc[1:, :]
                holder['Origin'] = list(holder.columns)[-1]
                holder = KarlGrossCoscoIntraEurope.SouthBoundDirect.pivot_table(holder)
                dfs.append(holder)
            return pd.concat(dfs, ignore_index=True)

        @staticmethod
        def get_surcharge_df(df, freight_df):

            surcharge_df = pd.DataFrame()
            start_index = list(df.loc[df[0] == 'SURCHARGES'].index)[0]
            end_index = list(df.loc[df[1].str.contains('IMO FAKTOR')].index)[0]
            df = df.loc[start_index + 1:end_index - 3, :1]
            surcharge_df['charges'] = df[0]  # df[0].str.replace(r"No(.+?)[0-9]{1,3}\s", '', regex=True).copy(deep=True)
            surcharge_df['amount'] = df[1].str.split('(', expand=True)[0].str.extract(
                r'(([A-Z]{1,3}\s[0-9]{1,3})(.+?)([A-Z]{1,3}(.+?)[A-Z]{1,4}))')[0].str.split('[', expand=True)[
                0].str.split(',-/|,- |,-', expand=True)[0].str.split(expand=True)[1].copy(deep=True)
            surcharge_df['currency'] = df[1].str.split('(', expand=True)[0].str.extract(
                r'(([A-Z]{1,3}\s[0-9]{1,3})(.+?)([A-Z]{1,3}(.+?)[A-Z]{1,4}))')[0].str.split('[', expand=True)[
                0].str.split(',-/|,- |,-', expand=True)[0].str.split(expand=True)[0].copy(deep=True)
            surcharge_df.loc[df[1].str.contains(',-/|,- |,-'), 'load_type'] = \
            df.loc[df[1].str.contains(',-/|,- |,-')][1].fillna('').str.split('(', expand=True)[0].str.extract(
                r'(([A-Z]{1,3}\s[0-9]{1,3})(.+?)([A-Z]{1,3}(.+?)[A-Z]{1,4}))')[0].str.split('[', expand=True)[
                0].str.split(',-/|,- |,-', expand=True)[1].str.replace('PER ', '', regex=True).copy(deep=True)
            surcharge_df.loc[~(df[1].str.contains(',-/|,- |,-')), 'load_type'] = \
            df.loc[~(df[1].str.contains(',-/|,- |,-'))][1].str.extract(
                r'(([A-Z]{1,3}\s[0-9]{1,3})(.+?)([A-Z]{1,3}(.+?)[A-Z]{1,4}))')[0].str.split(expand=True).iloc[:, -1]
            surcharge_df['remarks'], surcharge_df['destination_country'], surcharge_df['origin_country'], surcharge_df[
                'destination_port'] = '', '', '', ''
            for i in range(df.shape[0]):
                if '(' not in df.iloc[i, 1]:
                    if '[' in df.iloc[i, 1]:
                        surcharge_df['remarks'].iloc[i] = df.iloc[i, 1].replace(str(
                            df[1].str.extract(r'(([A-Z]{1,3}\s[0-9]{1,3})(.+?)([A-Z]{1,3}(.+?)[A-Z]{1,4}))')[
                                0].str.split('[', expand=True)[0].iloc[i]), '')[1:]
                    else:
                        try:
                            surcharge_df['remarks'].iloc[i] = df.iloc[i, 1].replace(
                                re.search(r'(([A-Z]{1,3}\s[0-9]{1,3})(.+?)([A-Z]{1,3}(.+?)[A-Z]{1,4}))',
                                          df.iloc[i, 1]).group(0), '')[1:]
                        except AttributeError:
                            surcharge_df['remarks'].iloc[i] = df.iloc[i, 1]
                else:
                    if ':' in df.iloc[i, 1]:
                        surcharge_df['remarks'].iloc[i] = df.iloc[i, 1][1:]
                    else:
                        surcharge_df['remarks'].iloc[i] = df.iloc[i, 1]

                if 'PREPAID' in surcharge_df['remarks'].iloc[i]:
                    surcharge_df['remarks'].iloc[i] = 'PREPAID'
                elif 'COLLECT' in surcharge_df['remarks'].iloc[i]:
                    surcharge_df['remarks'].iloc[i] = 'COLLECT'
                country_list = ['ISRAEL', 'Germany', 'Russia', 'Russia/Novorossiysk', 'Syria']
                for country in country_list:
                    if country.lower() in df.iloc[i, 1].lower() or country.lower() in df.iloc[i, 0].lower():
                        if country == 'Germany':
                            surcharge_df['origin_country'].iloc[i] = country
                        elif '/' not in country and '/' not in df.iloc[i, 0]:
                            surcharge_df['destination_country'].iloc[i] = country
                        else:
                            surcharge_df['destination_port'].iloc[i] = country.split('/')[-1]

            surcharge_df['charges_leg'] = ''
            lookup_list = ['No  227', 'No 119', 'No 209', 'THC', "SSL \(ISPS\)"]
            for item in lookup_list:
                if surcharge_df.iloc[:, 0].str.contains(item, na=False).any():
                    sideletter_index = \
                    list(surcharge_df.loc[surcharge_df.iloc[:, 0].str.contains(item, na=False)].index)[0]
                    if 'Destination' in surcharge_df.loc[sideletter_index, 'charges']:
                        surcharge_df.loc[sideletter_index, 'charges_leg'] = 'L4'
                        surcharge_df.loc[sideletter_index, 'destination_country'] = \
                        surcharge_df.loc[sideletter_index, 'charges'].split('(')[0].split()[-1]
                    elif 'Origin' in surcharge_df.loc[sideletter_index, 'charges'] or surcharge_df.loc[
                        sideletter_index, 'charges'] == 'THC':
                        surcharge_df.loc[sideletter_index, 'charges_leg'] = 'L2'
                        origins = " ".join(surcharge_df.loc[sideletter_index, 'remarks'].split()[2:]).replace('(',
                                                                                                              '').replace(
                            ')', '').split(". //")
                        dfs = []
                        for element in origins:
                            holder = surcharge_df.loc[sideletter_index, :].copy(deep=True)
                            holder.columns = surcharge_df.columns
                            holder['origin_port'] = element.split()[0]
                            holder['amount'] = element.split(",")[0].split()[-1]
                            holder['currency'] = element.split(",")[0].split()[1]
                            holder['load_type'] = element.split()[-1].replace('.', '')
                            temp = holder.copy(deep=True)
                            pod_list = list(freight_df['Country'].unique())
                            for pod in pod_list:
                                holder['destination_country'] = pod
                                dfs.append(holder)
                                holder = temp.copy(deep=True)
                        surcharge_df.drop(sideletter_index, inplace=True)
                        for series in dfs:
                            if series['charges'] == 'THC':
                                series['remarks'] = ''
                            surcharge_df = surcharge_df.append(series, ignore_index=True)
                    else:
                        if 'No' in item or surcharge_df.loc[sideletter_index, 'charges'] == 'SSL (ISPS)':
                            surcharge_df.loc[sideletter_index, 'charges_leg'] = 'L2'
                            sideletter_index = \
                        list(surcharge_df.loc[surcharge_df.iloc[:, 0].str.contains(item, na=False)].index)[0]
                        holder = surcharge_df.loc[sideletter_index, :].copy(deep=True)
                        holder.columns = surcharge_df.columns
                        temp = holder.copy(deep=True)
                        pod_list = list(freight_df['Country'].unique())
                        dfs = []
                        for pod in pod_list:
                            holder['destination_country'] = pod
                            dfs.append(holder)
                            holder = temp.copy(deep=True)
                        surcharge_df.drop(sideletter_index, inplace=True)
                        for series in dfs:
                            surcharge_df = surcharge_df.append(series, ignore_index=True)
            """
            if surcharge_df.iloc[:, 0].str.contains('Sideletter').any():
                sideletter_index = list(surcharge_df.loc[surcharge_df.iloc[:, 0].str.contains('Sideletter')].index)[0]
                surcharge_df['origin_port'], surcharge_df['destination_country'], surcharge_df['charges_leg'] = '', '', ''
                if 'Destination' in surcharge_df.loc[sideletter_index, 'charges']:
                    surcharge_df.loc[sideletter_index, 'charges_leg'] = 'L4'
                    surcharge_df.loc[sideletter_index, 'destination_country'] = surcharge_df.loc[sideletter_index, 'charges'].split()[4]
                elif 'Origin' in surcharge_df.loc[sideletter_index, 'charges']:
                    surcharge_df.loc[sideletter_index, 'charges_leg'] = 'L2'
                    origins = " ".join(surcharge_df.loc[sideletter_index, 'remarks'].split()[2:]).replace('(', '').replace(')', '').split(". //")
                    dfs = []
                    for element in origins:
                        holder = surcharge_df.loc[sideletter_index, :].copy(deep=True)
                        holder.columns = surcharge_df.columns
                        holder['origin_port'] = element.split()[0]
                        holder['amount'] = element.split(",")[0].split()[-1]
                        holder['currency'] = element.split(",")[0].split()[1]
                        holder['load_type'] = element.split()[-1].replace('.', '')
                        dfs.append(holder)
                    surcharge_df.drop(sideletter_index, inplace=True)
                    for series in dfs:
                        surcharge_df = surcharge_df.append(series, ignore_index=True)
            """

            surcharge_df.loc[surcharge_df.iloc[:, 0].str.contains('No 205'), (
            'amount', 'load_type', 'cargo_type')] = 'ON REQUEST', 'CTR', 'ONLY'
            surcharge_df = surcharge_df.loc[surcharge_df['destination_country'] != 'Syria']
            surcharge_df.loc[surcharge_df['charges'].str.contains("IMO", na=False), ("load_type", "remarks", "currency", "amount")] = "container", '', "EUR", "ON REQUEST"
            return surcharge_df

        @staticmethod
        def apply_country_code(df):

            country_code = {'Hamburg': 'HAM', 'Bremerhaven': 'BRV', 'Rotterdam': 'RTM', 'Antwerp': 'ANR'}

            for key, value in country_code.items():
                df.loc[df['Origin'] == key, 'country_code'] = value

            return df

        @staticmethod
        def apply_via(df):

            via = df['POL restrictions'].str.split('\n', expand=True)
            df['via'] = ''
            count = 0
            while count < len(list(via.columns)):
                for i in range(df.shape[0]):
                    if isinstance(df.loc[i, 'country_code'], str) and isinstance(via.iloc[i, count], str) and df.loc[
                        i, 'country_code'] in via.iloc[i, count] and 'via' in via.iloc[i, count]:
                        df.loc[i, 'via'] = df.loc[i, 'via'] + ';' + via.iloc[i, count].split('via')[-1].strip()
                    else:
                        try:
                            if df.loc[i, 'Origin'] in via.iloc[i, count] and 'via' in via.iloc[i, count]:
                                df.loc[i, 'via'] = df.loc[i, 'via'] + ';' + via.iloc[i, count].split('via')[-1].strip()
                        except TypeError:
                            pass
                count += 1
            df['via'] = df['via'].apply(lambda x: x.strip(';') if isinstance(x, str) else x)
            df['via'] = df['via'].str.split(';')
            df = df.explode('via')
            df['contract_id'] = df['Agreement Number'] + '\n' + df['via'].str.split(expand=True)[
                2].fillna('')
            df['contract_id'] = df['contract_id'].apply(lambda x: x.strip('\n') if isinstance(x, str) else x)
            df['via'] = df['via'].str.split(expand=True)[0].fillna(nan)
            return df

        @staticmethod
        def freight_clean(df):

            df['inclusions'] = df['BASIS'].str.split('\n', expand=True)[0].str.replace('incl.', '').apply(
                lambda x: x.strip())
            cols = [column for column in df.columns if column[0].isdigit()]
            df['20GP'] = df[cols[0]].replace('no service', 'ON REQUEST')
            df['40GP'] = df[cols[-1]].replace('no service', 'ON REQUEST')
            df['40HC'] = df[cols[-1]].replace('no service', 'ON REQUEST')
            df.loc[df['contract_id'].str.contains('no service'), 'contract_id'] = ''
            df.drop(columns=cols, inplace=True)
            df.drop(columns=['BASIS', "Extended Freetime at POD \n(harmless cargo only)", 'POL restrictions'
                , 'POL', 'POD', 'Agreement Number', 'country_code'], inplace=True)
            return df

        def capture(self):

            df, yellow_df = self.get_dfs()
            dates, commodity, currency_code = self.get_headers(df)
            inclusions = df.loc[list(df[df[1].str.contains('included')].index)[0], 0].replace('/', ';')
            pol_index = list(df[df[0].str.contains('PORTS OF LOADING', na=False)].index)[0]
            pols = [x.split()[0].strip() for x in df.loc[pol_index, 1].split('/')]
            freight_df = self.get_freight_df(df, pols)
            surcharge_df = self.get_surcharge_df(df, freight_df)
            surcharge_df['start_date'], surcharge_df['expiry_date'] = dates['start_date'], dates['expiry_date']

            freight_df.loc[freight_df['Origin'].str.contains('\\*'), 'Origin'] \
                = freight_df[freight_df['Origin'].str.contains('\\*')]['POL restrictions'] \
                .str.split('\\*', expand=True)[1].str.split(' =', expand=True)[0]

            freight_df['Origin'] = freight_df['Origin'].fillna('')
            freight_df.loc[freight_df['Origin'] == '', 'Origin'] \
                = freight_df[freight_df['Origin'] == '']['POL restrictions'] \
                .str.split('\n', expand=True).iloc[0, -1].split(' =')[0]

            freight_df['Destination'] = freight_df['Destination'].str.split('\n', expand=True)[0] \
                .apply(lambda x: x.strip())

            freight_df = self.apply_country_code(freight_df)
            freight_df = self.apply_via(freight_df)
            freight_df = self.freight_clean(freight_df)
            freight_df['start_date'], freight_df['expiry_date'], freight_df['commodity'], freight_df['currency'] \
                = dates['start_date'], dates['expiry_date'], commodity, currency_code
            freight_df.rename(columns={'Destination': 'destination_port', 'Origin': 'origin_port'}, inplace=True)
            freight_df['inclusions'] = inclusions
            freight_df['basis'] = 'container'
            self.captured_output = {'Freight': freight_df, 'Charges': surcharge_df}
            return self.captured_output

        def clean(self):

            self.cleaned_output = self.captured_output

            return self.cleaned_output

        def check_output(self):

            pass

    class SouthBoundFak(SouthBoundDirect):

        @staticmethod
        def get_freight_df(df, pols):

            dfs = []
            start_index = list(df[df[0].str.contains('Destination')].index)[0]
            end_index = list(df[df[0].str.contains('REMARKS')].index)[0]
            df = df.loc[start_index - 1:end_index - 1, :]
            indices = [df.iloc[0, :][df.iloc[0, :].str.contains(x, na=False)].index.values[0] for x in df.iloc[0, :]
                       if x != '' and x.split()[0] in pols
                       ]
            for i in range(df.shape[-1]):
                if df.iloc[1, i] == '':
                    df.iloc[1, i] = df.iloc[0, i]
            main_df = pd.concat([df.iloc[2:, :indices[0]], df.iloc[2:, indices[-1] + 2:]], axis=1)
            main_df.columns = list(df.iloc[1, :indices[0]]) + list(df.iloc[1, indices[-1] + 2:])
            for i in range(len(indices)):
                origin = df.iloc[0, indices[i]]
                holder = df.iloc[2:, indices[i]:indices[i] + 2]
                holder.columns = list(df.iloc[1, indices[i]:indices[i] + 2])
                holder['origin_port'] = origin
                holder = pd.concat([main_df, holder], axis=1)
                holder = holder.loc[holder['Destination'] != '']
                cols = [column for column in holder.columns if column[0].isdigit()]
                holder['20GP'] = holder[cols[0]]
                holder['40GP'] = holder[cols[-1]]
                holder['40HC'] = holder[cols[-1]]
                holder.drop(columns=cols, inplace=True)
                dfs.append(holder)
            return pd.concat(dfs, ignore_index=True)

        def capture(self):

            df, yellow_df = super().get_dfs()
            dates, commodity, currency_code = super().get_headers(df)

            pol_index = list(df[df[0].str.contains('PORTS OF LOADING', na=False)].index)[0]
            pols = [x.split()[0].strip() for x in df.loc[pol_index, 1].split('/')]

            freight_df = self.get_freight_df(df, pols)
            surcharge_df = super().get_surcharge_df(df, freight_df)
            surcharge_df['start_date'], surcharge_df['expiry_date'] = dates['start_date'], dates['expiry_date']

            freight_df['inclusions'] = freight_df['BASIS'].str.split('-', expand=True)[0].str.replace('incl.', '') \
                .apply(lambda x: x.strip())
            freight_df.loc[freight_df['20GP'].str.contains('\\*', na=False), 'origin_port'] \
                = freight_df[freight_df['20GP'].str.contains('\\*', na=False)]['POL restrictions'] \
                .str.split(expand=True)[0].str.replace('*', '')
            freight_df.loc[freight_df['20GP'].str.contains('service', na=False), ('20GP', '40GP', '40HC')] \
                = 'ON REQUEST', 'ON REQUEST', 'ON REQUEST'
            freight_df.loc[freight_df['POL restrictions'].str.contains('DG'), 'cargo_type'] \
                = freight_df[freight_df['POL restrictions'].str.contains('DG')]['POL restrictions'] \
                .str.split(expand=True)[0].str.upper()
            freight_df['Destination'] = freight_df['Destination'].str.split('\\(', expand=True)[0] \
                .apply(lambda x: x.strip())
            freight_df = freight_df.replace('\\*', '', regex=True)
            freight_df.drop(columns=['BASIS', 'POL restrictions', 'POL', 'POD'], inplace=True)
            freight_df.rename(columns={'Destination': 'destination_port'}, inplace=True)
            freight_df['start_date'], freight_df['expiry_date'], freight_df['commodity'], freight_df['currency'] \
                = dates['start_date'], dates['expiry_date'], commodity, currency_code
            freight_df['basis'] = 'container'
            self.captured_output = {'Freight': freight_df, 'Charges': surcharge_df}
            return self.captured_output

        def clean(self):

            self.cleaned_output = self.captured_output

            return self.cleaned_output

    @classmethod
    def resolve_dependency(cls, fix_outputs):

        direct_call_dict = fix_outputs.pop('IET VIP Southbound direct call')
        fak_dict = fix_outputs.pop('IET Southbound FAK')

        freight_df = pd.concat([direct_call_dict['Freight'], fak_dict['Freight']])
        surcharge_df = pd.concat([direct_call_dict['Charges'], fak_dict['Charges']])

        freight_df['sub_vendor'], freight_df[
            'contract_no'] = "COSCOSHIPPINGLINES(GERMANY)GMBH(DE-20457HAMBURG)", "G1005017"
        freight_df.apply(lambda x: x.strip() if isinstance(x, str) else x)
        surcharge_df['sub_vendor'], surcharge_df[
            'contract_no'] = "COSCOSHIPPINGLINES(GERMANY)GMBH(DE-20457HAMBURG)", "G1005017"
        surcharge_df.apply(lambda x: x.strip() if isinstance(x, str) else x)
        surcharge_df.drop_duplicates(keep='first', ignore_index=True, inplace=True)
        fix_outputs = [{"Freight": freight_df, "Charges": surcharge_df}]

        return fix_outputs


class Karl_Gross_COSCO_Middle_East(COSCO_Karl_Cross_V1):
    class Oceanfreight_Fix(COSCO_Karl_Cross_V1.Oceanfreight_Fix):

        @staticmethod
        def pivot_load_type(df):
            if "load_type" in df:
                df = df.fillna('')
                df = df.pivot_table(index=[column for column in df.columns if column not in ['load_type', 'amount']],
                                    columns=['load_type'],
                                    values=['amount'],
                                    aggfunc='first')
                df = df.reset_index()

                new_columns = []
                for index in df.columns.to_flat_index():
                    if index[0] == 'amount':
                        new_columns.append(index[1])
                    else:
                        new_columns.append(index[0])

                df.columns = new_columns

            return df

        @staticmethod
        def apply_arbitary_charges(Freight_df, arbitary_df, pols):

            arbitary_df['flag'] = ''
            arbitary_df.loc[((arbitary_df['via_port'].str.contains('on request', na=False)) & ~(arbitary_df['via_port'].str.contains('\(', na=False))), ('via_port', 'flag')] = 'Jebel Ali', 1
            Freight_destination_dict = Freight_df.to_dict("records")
            dps, via_ports = [], []
            for row in Freight_destination_dict:
                filtered_df = arbitary_df.loc[(arbitary_df["via_port"].astype(str).str.contains(row["destination_port"])) & (arbitary_df["load_type"] == row["load_type"])]
                filtered_df["icd"] = pols
                # filtered_df["remarks"] = row["remarks"]

                filter_numeric_df = filtered_df.loc[filtered_df['amount'].apply(lambda x: type(x) in [int])]
                filter_numeric_df["amount"] += row["amount"]

                filter_non_numeric_df = filtered_df.loc[filtered_df['amount'].apply(lambda x: type(x) in [str])]

                filtered_df_concat = pd.concat([filter_numeric_df, filter_non_numeric_df], ignore_index=True)

                dps.append(filtered_df_concat)

            df = pd.concat(dps, ignore_index=True)
            via_ports = list(df['via_port'].unique())
            no_via = arbitary_df.loc[~(arbitary_df['via_port'].isin(via_ports))]
            no_via['icd'] = no_via['origin_port']
            df = pd.concat([df, no_via], ignore_index=True)
            df['at'] = 'DESTINATION'
            df.loc[df['flag'] == 1, 'via_port'] = ''
            df.drop(columns=['flag'], inplace=True)
            df.loc[df['icd'] == 'TBN', 'icd'] = pols
            df.loc[df['destination_port'] == "Shuaiba / Shuwaikh", "via_port"] = "Jebel Ali"
            return df

        def surcharge_df_capture(self):

            remarks_index = list(self.df.loc[self.df[0].str.contains('REMARK', na=False)].index)
            surcharge_df = self.df.loc[remarks_index[0]+1: remarks_index[1]-1, :].copy(deep=True)
            surcharge_df = surcharge_df.replace('', nan)
            surcharge_df.dropna(how='all', axis=1, inplace=True)
            surcharge_df.columns = ['charges', 'description']
            return surcharge_df

        def capture(self):
            contract_details = COSCO_Karl_Cross_V1.Oceanfreight_Fix.get_contracts_details(self)
            self.df = self.df.iloc[:, :10].copy(deep=True)
            surcharge_df = remarks_util.Remarks.surcharge_remarks(self.surcharge_df_capture())
            surcharge_df["sub_vendor"], surcharge_df[
                "contract_no"] = "COSCOSHIPPINGLINES(GERMANY)GMBH(DE-20457HAMBURG)", "G1005017"
            if len(self.df.columns) == 10:
                df = self.df.copy(deep=True)
                df.columns = ["destination_country", "destination_port", "origin_port", "20GP", "40GP", "40HC",
                              "via_port", "subject_to", "service_type", "remarks"]
                df.replace("", nan, inplace=True)
                df.dropna(subset=["destination_port", "origin_port", "service_type"], inplace=True)
                freight_df = df[df.destination_port != 'DESTINATION']

                load_types = ["20GP", "40GP", "40HC"]
                for load_type in load_types:
                    freight_df.loc[
                        (freight_df["remarks"].astype(str).str.contains("ON REQUEST", na=False, case=False)) & (
                            freight_df[load_type].isna()), load_type] = "ON REQUEST"

                freight_df["destination_country"].fillna(method="ffill", inplace=True)
                freight_df["start_date"], freight_df["expiry_date"] = contract_details["start_date"].strip(), \
                                                                      contract_details["expiry"].strip()

                self.captured_output = {'Freight': freight_df, 'Charges': surcharge_df}

        def clean(self):
            freight_df = self.captured_output["Freight"]
            freight_df["origin_port"] = freight_df["origin_port"].replace(", ", ";", regex=True)
            # freight_df["start_date"], freight_df["expiry_date"] = pd.to_datetime(freight_df["start_date"], format="%d.%m.%Y"), pd.to_datetime(freight_df["expiry_date"], format="%d.%m.%Y")
            freight_df["charges"], freight_df["charges_leg"], freight_df[
                "basis"] = "Basic Ocean Freight", "L3", "container"
            freight_df["sub_vendor"], freight_df[
                "contract_no"] = "COSCOSHIPPINGLINES(GERMANY)GMBH(DE-20457HAMBURG)", "G1005017"
            freight_df.loc[(freight_df["remarks"].str.contains("^[0-9]", na=False)), "remarks"] = "Agreement Number: " + \
                                                                                                  freight_df[
                                                                                                      "remarks"].astype(
                                                                                                      str)
            freight_df = freight_df.loc[~(freight_df['destination_port'].str.contains("https:", na=False))]
            pol_index = list(self.df.loc[self.df[0].str.contains("PORTS OF LOADING", na=False)].index)[0]
            pols = self.df.iloc[pol_index, 1].replace(' / ', ';')
            freight_df['currency'] = 'USD'
            arb_df = freight_df.loc[freight_df['origin_port'] == 'TBN'].copy(deep=True)
            freight_df = freight_df.loc[freight_df['origin_port'] != 'TBN'].copy(deep=True)
            freight_df = super().melt_load_type(freight_df)
            arb_df = super().melt_load_type(arb_df)
            arb_df.loc[arb_df['amount'].isna(), 'amount'] = 'ON REQUEST'

            freight_df['origin_port'] = freight_df['origin_port'].str.split(';')
            freight_df = freight_df.explode('origin_port')
            port_names = {"HAM": "HAMBURG", "BRV": "BREMERHAVEN", "RTM": "ROTTERDAM", "ANR": "ANTWERP"}
            freight_df = freight_df.replace({'origin_port': port_names})
            freight_df['destination_port'] = freight_df['destination_port'].str.replace('\d+', '')
            arb_df['remarks'] += ';' + arb_df['via_port'].str.extract(r"\((.+?)\)").fillna('')[0]
            arb_df['remarks'] = arb_df['remarks'].apply(lambda x: x.strip(';') if isinstance(x, str) else x)
            arb_df['via_port'] = arb_df['via_port'].str.replace(r"\((.+?)\)", '').apply(lambda x: x.strip() if isinstance(x, str) else x)
            arb_df.loc[~(arb_df['via_port'].str.contains('suspended', na=False, case=False)), 'remarks'] = arb_df.loc[(arb_df['remarks'].str.contains('ON REQUEST', na=False)) & ~(arb_df['via_port'].str.contains('on request|suspended', na=False, case=False)), 'remarks']
            arb_df = self.apply_arbitary_charges(freight_df, arb_df, pols)
            arb_df = self.pivot_load_type(arb_df)
            arb_df['icd'] = arb_df['icd'].str.split(';')
            arb_df = arb_df.explode("icd")
            arb_df.drop_duplicates(subset=['icd', 'destination_port', 'remarks'], keep='first', inplace=True)
            arb_df.drop(columns=['origin_port'], inplace=True)
            freight_df = self.pivot_load_type(freight_df)
            arb_df.loc[~(arb_df['remarks'].str.contains("Agreement")), 'remarks'] = 'Recheck POL with Carrier'
            # freight_df.loc[~(freight_df['remarks'].str.contains('Agreement')), 'remarks'] = ''
            arb_df.loc[arb_df['via_port'].str.contains('suspended|on', na=False, case=False), 'via_port'] = ''
            freight_df['via_port'] = ''
            freight_destination_ports = list(freight_df['destination_port'].unique())
            freight_destination_ports.append('')
            arb_df = arb_df.loc[(arb_df['via_port'].isin(freight_destination_ports))]

            self.cleaned_output = {'Freight': freight_df, 'Arbitrary': arb_df, 'Charges': self.captured_output['Charges']}

    @classmethod
    def resolve_dependency(cls, fix_outputs):

        def contract_id(df):

            df.rename(columns={"remarks": "contract_id", "via_port": "via"}, inplace=True)
            df['contract_id'] = df['contract_id'].str.replace('Agreement Number: ', '')
            return df

        Freight_sheet = fix_outputs.pop('RED SEA - IPBC - MIDDLE EAST ')
        Freight_df = contract_id(Freight_sheet["Freight"])
        Arbitrary_df = contract_id(Freight_sheet['Arbitrary'])
        Charges_df = Freight_sheet["Charges"]
        Charges_df['start_date'], Charges_df['expiry_date'] = list(Freight_df['start_date'].unique())[0], list(Freight_df['expiry_date'].unique())[0]
        Freight_df["unique"] = "RED SEA - IPBC - MIDDLE EAST"
        fix_outputs = [{"Freight": Freight_df, 'Arbitrary': Arbitrary_df, "Charges": Charges_df}]
        return fix_outputs


class Ceva_Cosco_Emea(BaseTemplate):
    class Ceva_Cosco_Emea_1(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_date(self):
            date_dict = {}
            date_index = list(self.df[(self.df[0].str.contains("Valid from", na=False))].index)
            date_index.extend(self.df[(self.df[0].str.contains("Valid till", na=False))].index)
            start_date = self.df.iloc[date_index[0]][0].split(':')[1].split('(')[0]
            expiry_date = self.df.iloc[date_index[1]][0].split(':')[1]
            date_list = []
            date_list.append(start_date)
            date_list.append(expiry_date)
            date_dict = {'start_date': '', 'expiry_date': ''}
            date_dict = dict(zip(date_dict, date_list))
            return date_dict

        def get_commodity(self):
            commodity_index = list(self.df[(self.df[0].str.contains("Commodity", na=False))].index)
            commodity = self.df.iloc[commodity_index[0]][0].split(':')[1]
            return commodity

        def shipping_terms(self):
            shipping_index = list(self.df[(self.df[0].str.contains("Shipping terms", na=False))].index)
            mode_of_trans = self.df.iloc[shipping_index[0]][0].split(':')[1].split('to')
            if mode_of_trans[0] == ' PORT ':
                mode_of_trans[0] = 'CY'
            if mode_of_trans[1] == ' PORT':
                mode_of_trans[1] = 'CY'
            return mode_of_trans

        def customer_name(self):
            cust_index = list(self.df[(self.df[0].str.contains("Customer name", na=False))].index)
            customer_name = self.df.iloc[cust_index[0]][0].split(':')[1]
            return customer_name

        def final_table(self, add_df, ocean_df):
            add_df.reset_index(drop=True, inplace=True)
            via_port = []
            if add_df['destination_port'].str.contains('via', na=False).any():
                regex = r"via\s(.+?)\)"
                for num in range(len(add_df)):
                    via_str = str(add_df.loc[[num], 'destination_port'])
                    matches = re.finditer(regex, via_str, re.MULTILINE)
                    for matchNum, match in enumerate(matches, start=1):
                        for groupNum in range(0, len(match.groups())):
                            groupNum = groupNum + 1
                            group = match.group(groupNum)
                            if group not in via_port:
                                via_port.append(group)
                if via_port[0] == 'JEA':
                    via_port = 'Jebel Ali'
                via_index = list(ocean_df[(ocean_df['destination_port'].str.contains(via_port, na=False))].index)
                add_df1 = pd.DataFrame
                for index in via_index:
                    via_port = ocean_df.iloc[index, :]
                    add_df1 = add_df.copy()
                    for i in range(0, len(add_df)):
                        add_df['origin_port'][i] = via_port['origin_port']
                        add_df['via'][i] = via_port['destination_port']
                        add_df['20GP'][i] = int(add_df['20GP'][i]) + int(via_port['20GP'])
                        add_df['40GP'][i] = int(add_df['40GP'][i]) + int(via_port['40GP'])
                        add_df['40HC'][i] = int(add_df['40HC'][i]) + int(via_port['40HC'])
                add_df = pd.concat([add_df, add_df1])
            if not add_df['destination_port'].str.contains('via', na=False).any():
                via_port = ocean_df.iloc[0, :]
                for i in range(0, len(add_df)):
                    add_df.loc[i]['origin_port'] = via_port['origin_port']
                    add_df.loc[i]['via'] = via_port['destination_port']
                    add_df.loc[i]['20GP'] = int(add_df['20GP'][i]) + int(via_port['20GP'])
                    add_df.loc[i]['40GP'] = int(add_df['40GP'][i]) + int(via_port['40GP'])
                    add_df.loc[i]['40HC'] = int(add_df['40HC'][i]) + int(via_port['40HC'])
            return add_df

        def add_on(self, ocean_df):
            ocean_df.reset_index(drop=True, inplace=True)
            add_index = list(ocean_df[(ocean_df['destination_port'].str.contains("Add-On", na=False))].index)
            add_df = ocean_df[add_index[0]:ocean_df.tail(1).index.values[0] + 1].copy(deep=True)
            add_df['destination_port'] = add_df['destination_port'].apply(lambda x: x.split('Add-On')[0])
            return add_df

        def get_table(self):
            start_index = list(self.df[(self.df[0].str.contains("ORIGIN", na=False))].index)
            end_index = list(self.df[(self.df[0].str.contains("SURCHARGES & CONDITIONS", na=False))].index)
            ocean_df = self.df[start_index[0]:end_index[0]].copy(deep=True)
            ocean_df.columns = ocean_df.iloc[0]
            ocean_df = ocean_df[1:].copy()
            ocean_df.rename(columns={"20'DC": '20GP', 'ORIGIN': 'origin_port',
                                     'DESTINATION': 'destination_port'}, inplace=True)
            ocean_df['40GP'] = ocean_df["40'GP/HQ"].copy()
            ocean_df['40HC'] = ocean_df["40'GP/HQ"].copy()
            ocean_df['20GP'] = ocean_df['20GP'].str.split(' ', expand=True)[1]
            ocean_df['40GP'] = ocean_df['40GP'].str.split(' ', expand=True)[1]
            ocean_df['40HC'] = ocean_df['40HC'].str.split(' ', expand=True)[1]
            ocean_df["currency"] = ocean_df["40'GP/HQ"].str.split(' ', expand=True)[0]
            ocean_df.reset_index(inplace=True)
            ocean_df['charge'] = 'Basic Ocean Freight'
            ocean_df['basis'] = 'Per container'
            ocean_df.drop(columns=['index', "40'GP/HQ"], inplace=True)
            return ocean_df

        def surcharge(self, port_df, search_text, surcharge_str, regex):
            surcharge_dict = {}
            surcharge_df = port_df.copy()
            surcharge_keys = ['currency', 'fee', 'basis']
            surcharge_values = []
            matches = re.finditer(regex, surcharge_str, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group = match.group(groupNum)
                    surcharge_values.append(group)
            surcharge_dict = dict(zip(surcharge_keys, surcharge_values))
            if surcharge_dict['basis'] == 'BL':
                surcharge_df['charge'] = search_text
                surcharge_dict['basis'] = 'Bill of lading'
                surcharge_df['basis'] = surcharge_dict['basis']
                surcharge_df['20GP'] = surcharge_dict['fee']
                surcharge_df['40GP'] = surcharge_dict['fee']
                surcharge_df['40HC'] = surcharge_dict['fee']
                surcharge_df['currency'] = surcharge_dict['currency']
            if surcharge_dict['basis'] == 'cont' or surcharge_dict['basis'] == 'Cont':
                surcharge_df['charge'] = search_text
                surcharge_dict['basis'] = 'Per container'
                surcharge_df['basis'] = surcharge_dict['basis']
                surcharge_df['20GP'] = surcharge_dict['fee']
                surcharge_df['40GP'] = surcharge_dict['fee']
                surcharge_df['40HC'] = surcharge_dict['fee']
                surcharge_df['currency'] = surcharge_dict['currency']
            if surcharge_dict['basis'] == 'TEU':
                surcharge_dict['20GP'] = surcharge_dict.pop('fee')
                surcharge_dict['40GP'] = int(surcharge_dict['20GP']) * 2
                surcharge_dict['40HC'] = int(surcharge_dict['20GP']) * 2
                surcharge_df['charge'] = search_text
                surcharge_dict['basis'] = 'Per container'
                surcharge_df['basis'] = surcharge_dict['basis']
                surcharge_df['20GP'] = surcharge_dict['20GP']
                surcharge_df['40GP'] = surcharge_dict['40GP']
                surcharge_df['40HC'] = surcharge_dict['40HC']
                surcharge_df['currency'] = surcharge_dict['currency']
            return surcharge_df

        def surcharge_index(self, port_df, search_text, regex):
            surcharge_index = self.df[(self.df[0].str.contains(search_text, na=False))].index
            surcharge_str = self.df.iloc[surcharge_index[0]][0].split(':')[1]
            surcharge_df = self.surcharge(port_df, search_text, surcharge_str, regex)
            return surcharge_df

        def get_inclusions(self):
            inclusions_index = list(self.df[(self.df[0].str.contains("incl", na=False))].index)
            inclusions_list = []
            group_list = []
            for index in inclusions_index:
                inclusions_str = self.df.iloc[index][0]
                inclusions_list.append(inclusions_str)
            regex = r"(.+?):\sincl."
            for item in inclusions_list:
                matches = re.finditer(regex, item, re.MULTILINE)
                for matchNum, match in enumerate(matches, start=1):
                    for groupNum in range(0, len(match.groups())):
                        groupNum = groupNum + 1
                        group = match.group(groupNum)
                        group_list.append(group)
            if len(group_list) == 2:
                inclusions = group_list[0] + ';' + group_list[1]
            elif len(group_list) == 1:
                inclusions = group_list[0]
            elif len(group_list) == 3:
                inclusions = group_list[0] + ';' + group_list[1] + ';' + group_list[2]

            return inclusions

        def get_sub_to(self):
            sub_to_index = list(self.df[(self.df[0].str.contains("Subject to", na=False))].index)
            sub_to_list = []
            group_list = []
            sub_to = ''
            for index in sub_to_index:
                sub_to_str = self.df.iloc[index][0]
                sub_to_list.append(sub_to_str)
            regex = r"Subject to\s([A-Z]{3})"
            for item in sub_to_list:
                matches = re.finditer(regex, item, re.MULTILINE)
                for matchNum, match in enumerate(matches, start=1):
                    for groupNum in range(0, len(match.groups())):
                        groupNum = groupNum + 1
                        group = match.group(groupNum)
                        group_list.append(group)
                        if len(group_list) != 1:
                            sub_to = ";".join(group_list)
                        elif len(group_list) == 1:
                            sub_to = group_list[0]
            return sub_to

        def cis_surcharge(self, port_df):
            cis_dict = {}
            cis_index = list(self.df[(self.df[0].str.contains('CIS'))].index)
            cis_str = self.df.iloc[cis_index[0]][0]
            cis_list = cis_str.split(' ', 5)[5].split('and ')
            cis_list[1] = cis_list[1].rsplit('/', 1)[0]
            cis_keys = ['charge_region', 'currency', '20GP', '40GP', '40HC']
            cis_values = [cis_str.split(' ')[1].split(' ')[0], cis_list[0].split(' ')[0],
                          cis_list[0].split(' ')[1].split('/')[0],
                          cis_list[1].split(' ')[1].split('/')[0], cis_list[1].split(' ')[1].split('/')[0]]
            cis_dict = dict(zip(cis_keys, cis_values))
            cis_df = port_df.copy()
            cis_df['charge'] = 'CIS'
            cis_df['charge_region'] = cis_dict['charge_region']
            cis_df['basis'] = 'Per container'
            cis_df['20GP'] = cis_dict['20GP']
            cis_df['40GP'] = cis_dict['40GP']
            cis_df['40HC'] = cis_dict['40HC']
            cis_df['currency'] = cis_dict['currency']
            return cis_df

        def afs_surcharge(self, port_df):
            afs_index = []
            afs_df = pd.DataFrame()
            afs_index = self.df[(self.df[0].str.contains('AFS', na=False))].index
            region = self.get_dict_regex(regex=r".+?\(AFS\)\sfor\sPOD\sin\s(.+?)\s.+",
                                         search_str=self.df.iloc[afs_index[0]][0],
                                         group_keys=['charge_region'])
            if self.df[0].str.contains('AFS', na=False).any():
                afs_df = self.surcharge_index(port_df, search_text='AFS', regex=r"([A-Z]{3})\s(\d+)\/(.+?)$")
                afs_df['charge_region'] = region['charge_region']
                return afs_df

        def ams_surcharge(self, port_df):
            ams_index = []
            ams_df = pd.DataFrame()
            ams_index = self.df[(self.df[0].str.contains('AMS', na=False))].index
            region = self.get_dict_regex(regex=r".+?\sAMS \(Advanced Manifest Charge\) for\s(.+?)\s.+",
                                         search_str=self.df.iloc[ams_index[0]][0],
                                         group_keys=['charge_region'])
            if self.df[0].str.contains('AMS', na=False).any():
                ams_df = self.surcharge_index(port_df, search_text='AMS', regex=r"([A-Z]{3})\s(\d+)\/(.+?)$")
                ams_df['charge_region'] = region['charge_region']
                return ams_df

        def isp_surcharge(self, port_df):
            if self.df[0].str.contains('ISPS', na=False).any():
                isp_df = self.surcharge_index(port_df, search_text='ISPS', regex=r"(.{3})\s(\d+)\/(.+?)\.")
                return isp_df

        def bl_surcharge(self, port_df):
            if self.df[0].str.contains('BL FEE', na=False).any():
                bl_df = self.surcharge_index(port_df, search_text='BL FEE',
                                             regex=r"(.{3})\s(\d+).(.+?)\s(?:.*)manual\sbookings")
                return bl_df

        def faf_surcharge(self, port_df):
            if self.df[0].str.contains('FAF', na=False).any():
                faf_df = self.surcharge_index(port_df, search_text='FAF', regex=r"(.+?)\s(\d+)\/(.+?)$")
                return faf_df

        def get_dict_regex(self, regex, search_str, group_keys):
            result_dict = {}
            group_values = []
            matches = re.finditer(regex, search_str, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group = match.group(groupNum)
                    group_values.append(group)
            result_dict = dict(zip(group_keys, group_values))
            return result_dict

        def get_hcs1(self, port_df, hcs_list):
            hcs_df1 = port_df.copy()
            if '≥' not in hcs_list[1]:
                metrix_dict = self.get_dict_regex(regex=r".(\d+?)([a-z]+?)$", search_str=hcs_list[1], \
                                                  group_keys=['weight_to', 'weight_metrix'])
                curr_dict = self.get_dict_regex(regex=r"([A-Z]{3})(\d+?)$", search_str=hcs_list[2], \
                                                group_keys=['currency', 'amount'])
            hcs_df1['weight_to'] = metrix_dict['weight_to']
            hcs_df1['weight_metrix'] = metrix_dict['weight_metrix']
            hcs_df1['currency'] = curr_dict['currency']
            if '20gp' in hcs_list[0]:
                hcs_df1['20GP'] = curr_dict['amount']
            if '40gp' in hcs_list[0]:
                hcs_df1['40GP'] = curr_dict['amount']
            if '40hc' in hcs_list[0]:
                hcs_df1['40HC'] = curr_dict['amount']
            return hcs_df1

        def get_hcs2(self, port_df, hcs_list):
            hcs_df2 = port_df.copy()
            metrix_dict = self.get_dict_regex(regex=r".(\d+?)([a-z]{3}).(\d+?)([a-z]{3})", search_str=hcs_list[1],
                                              group_keys=['weight_from', 'weight_metrix', 'weight_to'])
            curr_dict = self.get_dict_regex(regex=r"([A-Z]{3})(\d+?)$", search_str=hcs_list[2],
                                            group_keys=['currency', 'amount'])
            hcs_df2['weight_from'] = metrix_dict['weight_from']
            hcs_df2['weight_to'] = metrix_dict['weight_to']
            hcs_df2['weight_metrix'] = metrix_dict['weight_metrix']
            hcs_df2['currency'] = curr_dict['currency']
            if '20gp' in hcs_list[0]:
                hcs_df2['20GP'] = curr_dict['amount']
            if '40gp' in hcs_list[0]:
                hcs_df2['40GP'] = curr_dict['amount']
            return hcs_df2

        def get_hcs3(self, port_df, hcs_list):
            hcs_df3 = port_df.copy()
            if "<" not in hcs_list[1]:
                metrix_dict = self.get_dict_regex(regex=r".(\d+?)([a-z]+?)$", search_str=hcs_list[1],
                                                  group_keys=['weight_from', 'weight_metrix'])
                curr_dict = self.get_dict_regex(regex=r"([A-Z]{3})(\d+?)$", search_str=hcs_list[2],
                                                group_keys=['currency', 'amount'])
                hcs_df3['weight_from'] = metrix_dict['weight_from']
                hcs_df3['weight_metrix'] = metrix_dict['weight_metrix']
                hcs_df3['currency'] = curr_dict['currency']
                if '20gp' in hcs_list[0]:
                    hcs_df3['20GP'] = curr_dict['amount']
                if '40gp' in hcs_list[0]:
                    hcs_df3['40GP'] = curr_dict['amount']
            return hcs_df3

        def hcs_surcharge(self, hcs_df, port_df):
            hcs_df.reset_index(inplace=True)
            hcs_df.drop(columns=(['index']), inplace=True)
            hcs_df = hcs_df.drop([0])
            hcs_df.loc[1][2] = hcs_df.loc[1][1]
            hcs_df.loc[1][3] = hcs_df.loc[1][1]
            hcs_df[5] = hcs_df[4].copy()
            hcs_df.loc[1][5] = '40hc'
            hcs1 = list(hcs_df[1])
            hcs2 = list(hcs_df[2])
            hcs3 = list(hcs_df[3])
            hcs4 = list(hcs_df[4])
            hcs5 = list(hcs_df[5])
            hcs_df1 = self.get_hcs1(port_df, hcs1)
            hcs_df2 = self.get_hcs2(port_df, hcs2)
            hcs_df3 = self.get_hcs3(port_df, hcs3)
            hcs_df4 = self.get_hcs1(port_df, hcs4)
            hcs_df5 = self.get_hcs1(port_df, hcs5)
            final_df = pd.concat([hcs_df1, hcs_df2, hcs_df3, hcs_df4, hcs_df5])
            final_df['charge'] = 'HCS'
            final_df['basis'] = 'Per Container'
            if len(hcs_df) == 4:
                hcs_df = hcs_df.drop([hcs_df.index[2]])
                hcs1 = list(hcs_df[1])
                hcs2 = list(hcs_df[2])
                hcs3 = list(hcs_df[3])
                hcs4 = list(hcs_df[4])
                hcs5 = list(hcs_df[5])
                hcs_df6 = self.get_hcs1(port_df, hcs1)
                hcs_df7 = self.get_hcs2(port_df, hcs2)
                hcs_df8 = self.get_hcs3(port_df, hcs3)
                hcs_df9 = self.get_hcs1(port_df, hcs4)
                hcs_df10 = self.get_hcs1(port_df, hcs5)
                final_df = pd.concat([final_df, hcs_df6, hcs_df7, hcs_df8, hcs_df9, hcs_df10])
                final_df['charge'] = 'HCS'
                final_df['basis'] = 'Per Container'
            return final_df

        def get_remarks(self):
            start_index = list(self.df[(self.df[0].str.contains("Valid for empty pick up ex AT", na=False))].index)
            end_index = list(self.df[(self.df[0].str.contains("REMARKS", na=False))].index)
            remarks_df = self.df[start_index[0]:end_index[0]].copy(deep=True)
            remarks_str = remarks_str = remarks_df.replace(np.nan, '').to_string(header=False, index=False)
            regex = re.compile(r"\s+")
            remarks = regex.sub(" ", remarks_str).strip()
            return remarks

        def capture(self):
            ocean_df = self.get_table()
            port_df = ocean_df[['origin_port', 'destination_port']].copy()
            if ocean_df['destination_port'].str.contains('Add-On').any():
                ocean_df['via'] = ''
                add_df = self.add_on(ocean_df)
                add_index = list(ocean_df[(ocean_df['destination_port'].str.contains("Add-On", na=False))].index)
                add_df = self.final_table(add_df, ocean_df)
                ocean_df = pd.concat([ocean_df, add_df])
                ocean_df.drop(add_index, axis=0, inplace=True)
                ocean_df.reset_index(inplace=True, drop=True)
                port_df = ocean_df[['origin_port', 'destination_port', 'via']].copy()
            cis_df = self.cis_surcharge(port_df)
            afs_df = self.afs_surcharge(port_df)
            ams_df = self.ams_surcharge(port_df)
            isp_df = self.isp_surcharge(port_df)
            bl_df = self.bl_surcharge(port_df)
            faf_df = self.faf_surcharge(port_df)
            start_index = list(self.df[(self.df[0].str.contains("HCS", na=False))].index)
            end_index = list(self.df[(self.df[0].str.contains("302", na=False))].index)
            hcs_df = self.df[start_index[0]:end_index[0] + 1].copy(deep=True)
            hcs_df = self.hcs_surcharge(hcs_df, port_df)
            freight_df = pd.concat([ocean_df, cis_df, afs_df, ams_df, isp_df, bl_df, faf_df, hcs_df])
            date_dict = self.get_date()
            commodity = self.get_commodity()
            cust_name = self.customer_name()
            mode_of_trans = self.shipping_terms()
            inclusions = self.get_inclusions()
            sub_to = self.get_sub_to()
            remarks = self.get_remarks()
            freight_df['start_date'] = date_dict['start_date']
            freight_df['expiry_date'] = date_dict['expiry_date']
            freight_df['start_date'] = pd.to_datetime(freight_df['start_date'])
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date'])
            freight_df['commodity'] = commodity
            freight_df['customer_name'] = cust_name
            freight_df['mode_of_transportation_origin'] = mode_of_trans[0]
            freight_df['mode_of_transportation_destination'] = mode_of_trans[1]
            freight_df['inclusions'] = inclusions
            freight_df['subject_to'] = sub_to
            freight_df['remarks'] = remarks
            freight_df.drop(columns=[''], inplace=True)
            self.captured_output = {"Freight": freight_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    class Ceva_Cosco_Emea_2(Ceva_Cosco_Emea_1):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def region_surcharge(self, port_df, charge_index, regex):
            charge_dict = {}
            charge_values = []
            charge_df = port_df.copy()
            charge_keys = ['charge', 'region', 'currency', 'fee', 'basis']
            charge_str = self.df.iloc[charge_index[0]][0]
            matches = re.finditer(regex, charge_str, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group = match.group(groupNum)
                    charge_values.append(group)
            charge_dict = dict(zip(charge_keys, charge_values))
            if charge_dict['basis'] == 'teu' or charge_dict['basis'] == 'TEU':
                charge_dict['20GP'] = charge_dict.pop('fee')
                charge_dict['40GP'] = int(charge_dict['20GP']) * 2
                charge_dict['40HC'] = int(charge_dict['20GP']) * 2
                charge_dict['basis'] = 'Per container'
            elif charge_dict['basis'] == 'cont.' or charge_dict['basis'] == 'Cont.':
                charge_dict['20GP'] = charge_dict['fee']
                charge_dict['40GP'] = charge_dict['fee']
                charge_dict['40HC'] = charge_dict['fee']
                charge_dict['basis'] = 'Per container'
            charge_df['charge'] = charge_dict['charge']
            charge_df['charge_region'] = charge_dict['region']
            charge_df['20GP'] = charge_dict['20GP']
            charge_df['40GP'] = charge_dict['40GP']
            charge_df['40HC'] = charge_dict['40HC']
            charge_df['currency'] = charge_dict['currency']
            charge_df['basis'] = charge_dict['basis']
            return charge_df

        def wrs_gs(self, port_df):
            wrs_gs_index = self.df[(self.df[0].str.contains('WRS', na=False) &
                                    self.df[0].str.contains('Gulf States', na=False))].index
            wrs_gs_df = self.region_surcharge(port_df, wrs_gs_index,
                                              regex=r"([A-Z]{3})\s(.+?):\s+([A-Z]{3})\s(\d+)\/(.+?)$")
            return wrs_gs_df

        def eis_aqaba(self, port_df):
            eis_index = self.df[(self.df[0].str.contains('EIS', na=False) &
                                 self.df[0].str.contains('Aqaba', na=False))].index
            eis_df = self.region_surcharge(port_df, eis_index,
                                           regex=r"([A-Z]{3})\s(.+?)\s([A-Z]{3})\s(\d+)\/(.+?)\s")
            return eis_df

        def wrs_jeddah(self, port_df):
            wrs_jeddah_index = self.df[(self.df[0].str.contains('WRS', na=False) &
                                        self.df[0].str.contains('Jeddah', na=False))].index
            wrs_jeddah_df = self.region_surcharge(port_df, wrs_jeddah_index,
                                                  regex=r"([A-Z]{3})\s.+?\s(.+?)\s.+?:\s([A-Z]{3})\s(\d+?)\/(.+?)\s")
            return wrs_jeddah_df

        def capture(self):
            ocean_df = super().get_table()
            port_df = ocean_df[['origin_port', 'destination_port']].copy()
            if ocean_df['destination_port'].str.contains('Add-On').any():
                ocean_df['via'] = ''
                add_df = self.add_on(ocean_df)
                add_index = list(ocean_df[(ocean_df['destination_port'].str.contains("Add-On", na=False))].index)
                add_df = self.final_table(add_df, ocean_df)
                ocean_df = pd.concat([ocean_df, add_df])
                ocean_df.drop(add_index, axis=0, inplace=True)
                ocean_df.reset_index(inplace=True, drop=True)
                port_df = ocean_df[['origin_port', 'destination_port', 'via']].copy()
            port_df.reset_index(drop=True, inplace=True)
            wrs_df = self.wrs_gs(port_df)
            wrs_df1 = self.wrs_jeddah(port_df)
            isp_df = super().isp_surcharge(port_df)
            bl_df = super().bl_surcharge(port_df)
            faf_df = super().faf_surcharge(port_df)
            eis_df = self.eis_aqaba(port_df)
            start_index = list(self.df[(self.df[0].str.contains("HCS", na=False))].index)
            end_index = list(self.df[(self.df[0].str.contains("304", na=False))].index)
            hcs_df = self.df[start_index[0]:end_index[0] + 1].copy(deep=True)
            hcs_df = super().hcs_surcharge(hcs_df, port_df)
            freight_df = pd.concat([ocean_df, wrs_df, wrs_df1, isp_df, bl_df, faf_df, eis_df, hcs_df])
            date_dict = super().get_date()
            commodity = super().get_commodity()
            cust_name = super().customer_name()
            mode_of_trans = super().shipping_terms()
            inclusions = super().get_inclusions()
            sub_to = super().get_sub_to()
            remarks = super().get_remarks()
            freight_df['start_date'] = date_dict['start_date']
            freight_df['expiry_date'] = date_dict['expiry_date']
            freight_df['start_date'] = pd.to_datetime(freight_df['start_date'])
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date'])
            freight_df['commodity'] = commodity
            freight_df['customer_name'] = cust_name
            freight_df['mode_of_transportation_origin'] = mode_of_trans[0]
            freight_df['mode_of_transportation_destination'] = mode_of_trans[1]
            freight_df['inclusions'] = inclusions
            freight_df['subject_to'] = sub_to
            freight_df['remarks'] = remarks
            self.captured_output = {"Freight": freight_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    class Ceva_Cosco_Emea_3(Ceva_Cosco_Emea_1):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def faf_surcharge(self, port_df):
            if self.df[0].str.contains('FAF', na=False).any():
                faf_df = super().surcharge_index(port_df, search_text='FAF', regex=r"(.+?)\s(\d+)\/([A-Z]{3})")
                return faf_df

        def capture(self):
            ocean_df = super().get_table()
            port_df = ocean_df[['origin_port', 'destination_port']].copy()
            port_df.reset_index(drop=True, inplace=True)
            isp_df = super().isp_surcharge(port_df)
            bl_df = super().bl_surcharge(port_df)
            faf_df = self.faf_surcharge(port_df)
            freight_df = pd.concat([ocean_df, isp_df, bl_df, faf_df])
            date_dict = super().get_date()
            commodity = super().get_commodity()
            cust_name = super().customer_name()
            mode_of_trans = super().shipping_terms()
            inclusions = super().get_inclusions()
            sub_to = super().get_sub_to()
            remarks = super().get_remarks()
            freight_df['start_date'] = date_dict['start_date']
            freight_df['expiry_date'] = date_dict['expiry_date']
            freight_df['start_date'] = pd.to_datetime(freight_df['start_date'])
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date'])
            freight_df['commodity'] = commodity
            freight_df['customer_name'] = cust_name
            freight_df['mode_of_transportation_origin'] = mode_of_trans[0]
            freight_df['mode_of_transportation_destination'] = mode_of_trans[1]
            freight_df['inclusions'] = inclusions
            freight_df['subject_to'] = sub_to
            freight_df['remarks'] = remarks
            self.captured_output = {"Freight": freight_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    class Ceva_Cosco_Emea_4(Ceva_Cosco_Emea_1):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_table(self):
            start_index = list(self.df[(self.df[0].str.contains("ORIGIN", na=False))].index)
            end_index = list(self.df[(self.df[0].str.contains("SURCHARGES & CONDITIONS", na=False))].index)
            ocean_df = self.df[start_index[0]:end_index[0] - 1].copy(deep=True)
            ocean_df.columns = ocean_df.iloc[0]
            ocean_df = ocean_df[1:].copy()
            ocean_df.rename(columns={"20'DC": '20GP', 'ORIGIN': 'origin_port', \
                                     'DESTINATION': 'destination_port'}, inplace=True)
            ocean_df['40GP'] = ocean_df["40´GP/HQ"].copy()
            ocean_df['40HC'] = ocean_df["40´GP/HQ"].copy()
            ocean_df['20GP'] = ocean_df['20GP'].str.split(' ', expand=True)[1]
            ocean_df['40GP'] = ocean_df['40GP'].str.split(' ', expand=True)[1]
            ocean_df['40HC'] = ocean_df['40HC'].str.split(' ', expand=True)[1]
            ocean_df["currency"] = ocean_df["40´GP/HQ"].str.split(' ', expand=True)[0]
            ocean_df.reset_index(inplace=True)
            ocean_df['charge'] = 'Basic Ocean Freight'
            ocean_df['basis'] = 'Per container'
            ocean_df.drop(columns=['index', "40´GP/HQ", ''], inplace=True)
            return ocean_df

        def capture(self):
            ocean_df = self.get_table()
            port_df = ocean_df[['origin_port', 'destination_port']].copy()
            port_df.reset_index(drop=True, inplace=True)
            isp_df = super().isp_surcharge(port_df)
            bl_df = super().bl_surcharge(port_df)
            faf_df = super().faf_surcharge(port_df)
            freight_df = pd.concat([ocean_df, isp_df, bl_df, faf_df])
            date_dict = super().get_date()
            commodity = super().get_commodity()
            cust_name = super().customer_name()
            mode_of_trans = super().shipping_terms()
            inclusions = super().get_inclusions()
            sub_to = super().get_sub_to()
            remarks = super().get_remarks()
            freight_df['start_date'] = date_dict['start_date']
            freight_df['expiry_date'] = date_dict['expiry_date']
            freight_df['start_date'] = pd.to_datetime(freight_df['start_date'])
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date'])
            freight_df['commodity'] = commodity
            freight_df['customer_name'] = cust_name
            freight_df['mode_of_transportation_origin'] = mode_of_trans[0]
            freight_df['mode_of_transportation_destination'] = mode_of_trans[1]
            freight_df['inclusions'] = inclusions
            freight_df['subject_to'] = sub_to
            freight_df['remarks'] = remarks
            self.captured_output = {"Freight": freight_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    class Ceva_Cosco_Emea_5(Ceva_Cosco_Emea_2):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def ssl_surcharge(self, port_df):
            if self.df[0].str.contains('SSL', na=False).any():
                ssl_df = super().surcharge_index(port_df, search_text='SSL', regex=r"([A-Z]{3})\s(\d+?)\/(.+?)\.")
                return ssl_df

        def faf_ec(self, port_df):
            faf_ec_index = self.df[(self.df[0].str.contains('FAF', na=False) &
                                    self.df[0].str.contains('East', na=False))].index
            faf_ec_df = super().region_surcharge(port_df, faf_ec_index,
                                                 regex=r"([A-Z]{3})\s(.+?)\(.+?([A-Z]{3})\s(\d+?)\/(.+?)$")
            return faf_ec_df

        def faf_wc(self, port_df):
            faf_wc_index = self.df[(self.df[0].str.contains('FAF', na=False) &
                                    self.df[0].str.contains('West', na=False))].index
            faf_wc_df = super().region_surcharge(port_df, faf_wc_index,
                                                 regex=r"([A-Z]{3})\s(.+?)\(.+?([A-Z]{3})\s(\d+?)\/(.+?)$")
            return faf_wc_df

        def lws_charge(self, port_df):
            lws_index = self.df[(self.df[0].str.contains('LWS', na=False) &
                                 self.df[0].str.contains('Paraguay', na=False))].index
            lws_df = super().region_surcharge(port_df, lws_index,
                                              regex=r"([A-Z]{3})\s(.+?)\s([A-Z]{3})\s(\d+)\/(.+?)$")
            return lws_df

        def capture(self):
            ocean_df = self.get_table()
            port_df = ocean_df[['origin_port', 'destination_port']].copy()
            port_df.reset_index(drop=True, inplace=True)
            bl_df = super().bl_surcharge(port_df)
            ssl_df = self.ssl_surcharge(port_df)
            faf_ec_df = self.faf_ec(port_df)
            faf_wc_df = self.faf_wc(port_df)
            lws_df = self.lws_charge(port_df)
            freight_df = pd.concat([ocean_df, bl_df, ssl_df, faf_ec_df, faf_wc_df, lws_df])
            date_dict = super().get_date()
            commodity = super().get_commodity()
            cust_name = super().customer_name()
            mode_of_trans = super().shipping_terms()
            inclusions = super().get_inclusions()
            sub_to = super().get_sub_to()
            remarks = super().get_remarks()
            freight_df['start_date'] = date_dict['start_date']
            freight_df['expiry_date'] = date_dict['expiry_date']
            freight_df['start_date'] = pd.to_datetime(freight_df['start_date'])
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date'])
            freight_df['commodity'] = commodity
            freight_df['customer_name'] = cust_name
            freight_df['mode_of_transportation_origin'] = mode_of_trans[0]
            freight_df['mode_of_transportation_destination'] = mode_of_trans[1]
            freight_df['inclusions'] = inclusions
            freight_df['subject_to'] = sub_to
            freight_df['remarks'] = remarks
            self.captured_output = {"Freight": freight_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    class Ceva_Cosco_Emea_6(Ceva_Cosco_Emea_2):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def ssl_surcharge(self, port_df):
            if self.df[0].str.contains('SSL', na=False).any():
                ssl_df = super().surcharge_index(port_df, search_text='SSL', regex=r"([A-Z]{3})\s(\d+?)\/(.+?)\.")
                return ssl_df

        def wrs_israel(self, port_df):
            if self.df[0].str.contains('WRS', na=False).any() & self.df[0].str.contains('Israel', na=False).any():
                wrs_is_df = super().surcharge_index(port_df, search_text='WRS',
                                                    regex=r"([A-Z]{3})\s(\d+?)\/(.+?)\s.+?$")
                wrs_is_df['charge_region'] = 'Israel'
                return wrs_is_df

        def faf_surcharge(self, port_df):
            if self.df[0].str.contains('FAF', na=False).any():
                faf_df = super().surcharge_index(port_df, search_text='FAF', regex=r"\s([A-Z]{3})\s(\d.+?)\/(.+?)$")
                return faf_df

        def wsc_surcharge(self, port_df):
            if self.df[0].str.contains('WSC', na=False).any():
                wsc_df = super().surcharge_index(port_df, search_text='WSC',
                                                 regex=r"\s([A-Z]{3})\s(\d+?)\/([A-Z]{3})\s.+")
                return wsc_df

        def eis_israel(self, port_df):
            if self.df[0].str.contains('EIS', na=False).any() & self.df[0].str.contains('Israel', na=False).any():
                eis_is_df = super().surcharge_index(port_df, search_text='EIS',
                                                    regex=r"\s([A-Z]{3})\s(\d+?)\/(.+?)\s.+")
                eis_is_df['charge_region'] = 'Israel'
                return eis_is_df

        def ecc_israel(self, port_df):
            ecc_index = self.df[(self.df[0].str.contains('ECC', na=False) &
                                 self.df[0].str.contains('Israel', na=False))].index
            ecc_is_df = self.region_surcharge(port_df, ecc_index,
                                              regex=r"([A-Z]{3})\s(.+?)\s.+?([A-Z]{3})\s(\d+?)\/(.+?)\s.+")
            return ecc_is_df

        def capture(self):
            ocean_df = self.get_table()
            port_df = ocean_df[['origin_port', 'destination_port']].copy()
            if ocean_df['destination_port'].str.contains('Add-On').any():
                ocean_df['via'] = ''
                add_df = self.add_on(ocean_df)
                add_index = list(ocean_df[(ocean_df['destination_port'].str.contains("Add-On", na=False))].index)
                add_df = self.final_table(add_df, ocean_df)
                ocean_df = pd.concat([ocean_df, add_df])
                ocean_df.drop(add_index, axis=0, inplace=True)
                ocean_df.reset_index(inplace=True, drop=True)
                port_df = ocean_df[['origin_port', 'destination_port', 'via']].copy()
            port_df.reset_index(drop=True, inplace=True)
            port_df.reset_index(drop=True, inplace=True)
            ssl_df = self.ssl_surcharge(port_df)
            wrs_is_df = self.wrs_israel(port_df)
            faf_df = self.faf_surcharge(port_df)
            wsc_df = self.wsc_surcharge(port_df)
            eis_is_df = self.eis_israel(port_df)
            ecc_is_df = self.ecc_israel(port_df)
            freight_df = pd.concat([ocean_df, ssl_df, wrs_is_df, faf_df, wsc_df, eis_is_df, ecc_is_df])
            date_dict = super().get_date()
            commodity = super().get_commodity()
            cust_name = super().customer_name()
            mode_of_trans = super().shipping_terms()
            sub_to = super().get_sub_to()
            remarks = super().get_remarks()
            freight_df['start_date'] = date_dict['start_date']
            freight_df['expiry_date'] = date_dict['expiry_date']
            freight_df['start_date'] = pd.to_datetime(freight_df['start_date'])
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date'])
            freight_df['commodity'] = commodity
            freight_df['customer_name'] = cust_name
            freight_df['mode_of_transportation_origin'] = mode_of_trans[0]
            freight_df['mode_of_transportation_destination'] = mode_of_trans[1]
            freight_df['subject_to'] = sub_to
            freight_df['remarks'] = remarks
            self.captured_output = {"Freight": freight_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    class Ceva_Cosco_Emea_7(Ceva_Cosco_Emea_1):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def thc_surcharge(self, port_df):
            if self.df[0].str.contains('THC', na=False).any():
                thc_df = super().surcharge_index(port_df, search_text='THC', regex=r"\s([A-Z]{3})\s(\d.+?)\/(.+?)$")
                return thc_df

        def isp_surcharge(self, port_df):
            if self.df[0].str.contains('ISPS', na=False).any():
                isp_df = super().surcharge_index(port_df, search_text='ISPS', regex=r"\s([A-Z]{3})\s(\d+)\/(.+?)$")
                return isp_df

        def capture(self):
            ocean_df = super().get_table()
            ocean_df.drop(ocean_df.tail(1).index, inplace=True)
            port_df = ocean_df[['origin_port', 'destination_port']].copy()
            if ocean_df['destination_port'].str.contains('Add-On').any():
                ocean_df['via'] = ''
                add_df = self.add_on(ocean_df)
                add_df.drop(add_df.tail(1).index, inplace=True)
                add_index = list(ocean_df[(ocean_df['destination_port'].str.contains("Add-On", na=False))].index)
                add_df = self.final_table(add_df, ocean_df)
                ocean_df = pd.concat([ocean_df, add_df])
                ocean_df.drop(add_index, axis=0, inplace=True)
                ocean_df.reset_index(inplace=True, drop=True)
                port_df = ocean_df[['origin_port', 'destination_port', 'via']].copy()
            port_df.reset_index(drop=True, inplace=True)
            ocean_df.drop(columns=['', ''], inplace=True)
            thc_df = self.thc_surcharge(port_df).reset_index(drop=True)
            isp_df = self.isp_surcharge(port_df).reset_index(drop=True)
            afs_df = super().afs_surcharge(port_df).reset_index(drop=True)
            ams_df = super().ams_surcharge(port_df).reset_index(drop=True)
            cis_df = super().cis_surcharge(port_df).reset_index(drop=True)
            faf_df = super().faf_surcharge(port_df).reset_index(drop=True)
            start_index = list(self.df[(self.df[0].str.contains("HCS", na=False))].index)
            end_index = list(self.df[(self.df[0].str.contains("306", na=False))].index)
            hcs_df = self.df[start_index[0]:end_index[0] + 1].copy(deep=True)
            hcs_df = super().hcs_surcharge(hcs_df, port_df).reset_index(drop=True)
            freight_df = pd.concat([ocean_df, thc_df, isp_df, afs_df, ams_df, cis_df, faf_df, hcs_df])
            date_dict = super().get_date()
            commodity = super().get_commodity()
            cust_name = super().customer_name()
            mode_of_trans = super().shipping_terms()
            sub_to = super().get_sub_to()
            inclusions = super().get_inclusions()
            remarks = super().get_remarks()
            freight_df['start_date'] = date_dict['start_date']
            freight_df['expiry_date'] = date_dict['expiry_date']
            freight_df['start_date'] = pd.to_datetime(freight_df['start_date'])
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date'])
            freight_df['commodity'] = commodity
            freight_df['customer_name'] = cust_name
            freight_df['mode_of_transportation_origin'] = mode_of_trans[0]
            freight_df['mode_of_transportation_destination'] = mode_of_trans[1]
            freight_df['subject_to'] = sub_to
            freight_df['inclusions'] = inclusions
            freight_df['remarks'] = remarks
            self.captured_output = {"Freight": freight_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    class Ceva_Cosco_Emea_8(Ceva_Cosco_Emea_2):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def thc_surcharge(self, port_df):
            if self.df[0].str.contains('THC', na=False).any():
                thc_df = super().surcharge_index(port_df, search_text='THC', regex=r"\s([A-Z]{3})\s(\d.+?)\/(.+?)$")
                return thc_df

        def wrs_gs(self, port_df):
            wrs_gs_index = self.df[(self.df[0].str.contains('WRS', na=False) &
                                    self.df[0].str.contains('Gulf States', na=False))].index
            wrs_gs_df = self.region_surcharge(port_df, wrs_gs_index,
                                              regex=r"([A-Z]{3})\s(.+?)\s+([A-Z]{3})\s(\d+?)\/(.+?)$")
            return wrs_gs_df

        def capture(self):
            ocean_df = super().get_table()
            port_df = ocean_df[['origin_port', 'destination_port']].copy()
            port_df.reset_index(drop=True, inplace=True)
            bl_df = super().bl_surcharge(port_df)
            thc_df = self.thc_surcharge(port_df)
            isp_df = super().isp_surcharge(port_df)
            faf_df = super().faf_surcharge(port_df)
            wrs_df = self.wrs_gs(port_df)
            wrs_df1 = super().wrs_jeddah(port_df)
            eis_df = super().eis_aqaba(port_df)
            start_index = list(self.df[(self.df[0].str.contains("HCS", na=False))].index)
            end_index = list(self.df[(self.df[0].str.contains("306", na=False))].index)
            hcs_df = self.df[start_index[0]:end_index[0] + 1].copy(deep=True)
            hcs_df = super().hcs_surcharge(hcs_df, port_df)
            freight_df = pd.concat([ocean_df, bl_df, thc_df, isp_df, faf_df, wrs_df, wrs_df1, eis_df, hcs_df])
            date_dict = super().get_date()
            commodity = super().get_commodity()
            cust_name = super().customer_name()
            sub_to = super().get_sub_to()
            inclusions = super().get_inclusions()
            mode_of_trans = super().shipping_terms()
            remarks = super().get_remarks()
            freight_df['start_date'] = date_dict['start_date']
            freight_df['expiry_date'] = date_dict['expiry_date']
            freight_df['start_date'] = pd.to_datetime(freight_df['start_date'])
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date'])
            freight_df['commodity'] = commodity
            freight_df['customer_name'] = cust_name
            freight_df['mode_of_transportation_origin'] = mode_of_trans[0]
            freight_df['mode_of_transportation_destination'] = mode_of_trans[1]
            freight_df['subject_to'] = sub_to
            freight_df['inclusions'] = inclusions
            freight_df['remarks'] = remarks
            freight_df.drop(columns=[''], inplace=True)
            self.captured_output = {"Freight": freight_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    class Ceva_Cosco_Emea_9(Ceva_Cosco_Emea_7):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def faf_surcharge(self, port_df):
            if self.df[0].str.contains('FAF', na=False).any():
                faf_df = super().surcharge_index(port_df, search_text='FAF', regex=r"(.+?)\s(\d+)\/(.+?)\,")
                return faf_df

        def capture(self):
            ocean_df = super().get_table()
            port_df = ocean_df[['origin_port', 'destination_port']].copy()
            port_df.reset_index(drop=True, inplace=True)
            bl_df = super().bl_surcharge(port_df)
            thc_df = super().thc_surcharge(port_df)
            isp_df = super().isp_surcharge(port_df)
            faf_df = self.faf_surcharge(port_df)
            freight_df = pd.concat([ocean_df, bl_df, thc_df, isp_df, faf_df])
            date_dict = super().get_date()
            commodity = super().get_commodity()
            cust_name = super().customer_name()
            sub_to = super().get_sub_to()
            inclusions = super().get_inclusions()
            mode_of_trans = super().shipping_terms()
            remarks = super().get_remarks()
            freight_df['start_date'] = date_dict['start_date']
            freight_df['expiry_date'] = date_dict['expiry_date']
            freight_df['start_date'] = pd.to_datetime(freight_df['start_date'])
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date'])
            freight_df['commodity'] = commodity
            freight_df['customer_name'] = cust_name
            freight_df['mode_of_transportation_origin'] = mode_of_trans[0]
            freight_df['mode_of_transportation_destination'] = mode_of_trans[1]
            freight_df['subject_to'] = sub_to
            freight_df['inclusions'] = inclusions
            freight_df['remarks'] = remarks
            self.captured_output = {"Freight": freight_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    class Ceva_Cosco_Emea_10(Ceva_Cosco_Emea_4):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def thc_surcharge(self, port_df):
            if self.df[0].str.contains('THC', na=False).any():
                thc_df = super().surcharge_index(port_df, search_text='THC', regex=r"\s([A-Z]{3})\s(\d.+?)\/(.+?)$")
                return thc_df

        def capture(self):
            ocean_df = super().get_table()
            port_df = ocean_df[['origin_port', 'destination_port']].copy()
            port_df.reset_index(drop=True, inplace=True)
            bl_df = super().bl_surcharge(port_df)
            thc_df = self.thc_surcharge(port_df)
            isp_df = super().isp_surcharge(port_df)
            faf_df = super().faf_surcharge(port_df)
            freight_df = pd.concat([ocean_df, bl_df, thc_df, isp_df, faf_df])
            date_dict = super().get_date()
            commodity = super().get_commodity()
            cust_name = super().customer_name()
            sub_to = super().get_sub_to()
            inclusions = super().get_inclusions()
            mode_of_trans = super().shipping_terms()
            remarks = super().get_remarks()
            freight_df['start_date'] = date_dict['start_date']
            freight_df['expiry_date'] = date_dict['expiry_date']
            freight_df['start_date'] = pd.to_datetime(freight_df['start_date'])
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date'])
            freight_df['commodity'] = commodity
            freight_df['customer_name'] = cust_name
            freight_df['mode_of_transportation_origin'] = mode_of_trans[0]
            freight_df['mode_of_transportation_destination'] = mode_of_trans[1]
            freight_df['subject_to'] = sub_to
            freight_df['inclusions'] = inclusions
            freight_df['remarks'] = remarks
            self.captured_output = {"Freight": freight_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    class Ceva_Cosco_Emea_11(Ceva_Cosco_Emea_6):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):
            ocean_df = self.get_table()
            port_df = ocean_df[['origin_port', 'destination_port']].copy()
            if ocean_df['destination_port'].str.contains('Add-On').any():
                ocean_df['via'] = ''
                add_df = self.add_on(ocean_df)
                add_index = list(ocean_df[(ocean_df['destination_port'].str.contains("Add-On", na=False))].index)
                add_df = self.final_table(add_df, ocean_df)
                ocean_df = pd.concat([ocean_df, add_df])
                ocean_df.drop(add_index, axis=0, inplace=True)
                ocean_df.reset_index(inplace=True, drop=True)
                port_df = ocean_df[['origin_port', 'destination_port', 'via']].copy()
            port_df.reset_index(drop=True, inplace=True)
            port_df.reset_index(drop=True, inplace=True)
            ssl_df = self.ssl_surcharge(port_df)
            wrs_is_df = self.wrs_israel(port_df)
            faf_df = self.faf_surcharge(port_df)
            wsc_df = self.wsc_surcharge(port_df)
            eis_is_df = self.eis_israel(port_df)
            ecc_is_df = self.ecc_israel(port_df)
            freight_df = pd.concat([ocean_df, ssl_df, wrs_is_df, faf_df, wsc_df, eis_is_df, ecc_is_df])
            date_dict = super().get_date()
            commodity = super().get_commodity()
            cust_name = super().customer_name()
            mode_of_trans = super().shipping_terms()
            sub_to = super().get_sub_to()
            remarks = super().get_remarks()
            freight_df['start_date'] = date_dict['start_date']
            freight_df['expiry_date'] = date_dict['expiry_date']
            freight_df['start_date'] = pd.to_datetime(freight_df['start_date'])
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date'])
            freight_df['commodity'] = commodity
            freight_df['customer_name'] = cust_name
            freight_df['mode_of_transportation_origin'] = mode_of_trans[0]
            freight_df['mode_of_transportation_destination'] = mode_of_trans[1]
            freight_df['subject_to'] = sub_to
            freight_df['remarks'] = remarks
            self.captured_output = {"Freight": freight_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    def resolve_dependency(cls, fix_outputs):
        if "HAM-FE" in fix_outputs:
            sheet1 = fix_outputs.pop('HAM-FE')
            df1 = sheet1['Freight']
        if "HAM-IPBC" in fix_outputs:
            sheet2 = fix_outputs.pop('HAM-IPBC')
            df2 = sheet2['Freight']
        if "HAM-AU" in fix_outputs:
            sheet3 = fix_outputs.pop('HAM-AU')
            df3 = sheet3['Freight']
        if "HAM-NZ" in fix_outputs:
            sheet4 = fix_outputs.pop('HAM-NZ')
            df4 = sheet4['Freight']
        if "HAM-SAM" in fix_outputs:
            sheet5 = fix_outputs.pop('HAM-SAM')
            df5 = sheet5['Freight']
        if "HAM-IET" in fix_outputs:
            sheet6 = fix_outputs.pop('HAM-IET')
            df6 = sheet6['Freight']
        if "KOP-FE" in fix_outputs:
            sheet7 = fix_outputs.pop('KOP-FE')
            df7 = sheet7['Freight']
        if "KOP-IPBC" in fix_outputs:
            sheet8 = fix_outputs.pop('KOP-IPBC')
            df8 = sheet8['Freight']
        if "KOP-AU" in fix_outputs:
            sheet9 = fix_outputs.pop('KOP-AU')
            df9 = sheet9['Freight']
        if "KOP-NZ" in fix_outputs:
            sheet10 = fix_outputs.pop('KOP-NZ')
            df10 = sheet10['Freight']
        if "KOP-IET" in fix_outputs:
            sheet11 = fix_outputs.pop('KOP-IET')
            df11 = sheet11['Freight']
        final_df = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11])
        final_df.drop(columns=[''], inplace=True)
        final_df.reset_index(drop=True, inplace=True)
        fix_outputs = {"KOP-IET": {"Freight": final_df}}
        return fix_outputs
