from logging import getLogger
from base import BaseTemplate, BaseFix, BaseDocxFix
from custom_exceptions import InputValidationError
import pandas as pd
from collections import defaultdict
import re
from numpy import nan
import warnings
from bs4 import BeautifulSoup
from html2text import html2text
import io
from dateutil.parser import parse
from datetime import datetime
import dateparser

warnings.simplefilter(action='ignore', category=FutureWarning)

log = getLogger(__name__)


class OOCL_Far_east_No_arb(BaseTemplate):
    class _Rates(BaseFix):

        def check_input(self):
            check_errors = []
            if not self.df[0].str.contains('Origin', na=False).any():
                check_errors.append("Origin column not present")
            if check_errors:
                raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_freight_table(self):
            """
            Get the table from the rates sheet
            Returns:
                freight table
            """
            index = list(self.df[(self.df[0] == ("Origin"))].index)
            region_index = self.df[(self.df[0].str.contains("Rates - ", na=False))].index[0]

            region = self.df.iloc[region_index, 0]
            region = region.replace('Rates - ', '')

            freight_df = self.df[index[0]:self.df.tail(1).index.values[0] + 1].copy(deep=True)
            freight_df.columns = freight_df.iloc[0]
            freight_df = freight_df[1:].copy()
            col = freight_df.columns.tolist()
            counter = 0
            note_column = ['Notes']
            for col_ in range(len(col)):
                if col[col_] == '':
                    counter += 1
                    col[col_] = "Notes" + str(counter)
                    note_column.append(col[col_])

            freight_df.columns = col
            freight_df['remarks'] = freight_df[note_column].apply(lambda x: ', '.join(x[x.notnull()]), axis=1)
            freight_df['region'] = region
            return freight_df

        @staticmethod
        def format_output(df_freight):
            output = {'Freight': df_freight}
            return output

        def capture(self):
            freight_df = self.get_freight_table()
            self.captured_output = self.format_output(freight_df)

        def clean(self):
            freight_df = self.captured_output['Freight']

            column_names = {'Origin': 'origin_port', 'Destination': 'destination_icd', 'Mode': 'service_type',
                            20.0: '20GP', 40.0: '40GP', 'Expiry   MM/DD/YY': 'expiry_date',
                            '40H': '40HC', 'remarks': 'remarks', 'Cargo Nature': 'cargonature', 'region': 'region'
                            }
            freight_df.rename(columns=column_names, inplace=True)
            if 'cargonature' not in freight_df:
                column_names = {'Origin': 'origin_port', 'Destination': 'destination_icd', 'Mode': 'service_type',
                                20.0: '20GP', 40.0: '40GP', '40H': '40HC', 'remarks': 'remarks', 'region': 'region'
                                }
                freight_df = freight_df[list(column_names.values())].copy(deep=True)
            else:
                freight_df = freight_df[list(column_names.values())].copy(deep=True)
            if 'expiry_date' in freight_df:
                freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date']).dt.strftime('%Y-%m-%d')
            freight_df['charges_leg'] = 'L3'
            freight_df['charges'] = 'basic ocean freight'
            self.cleaned_output = {'Freight': freight_df}

    class _Notes(BaseFix):

        def check_input(self):
            # check_errors = []
            # if not self.df[0].str.contains('TRADE LANE', na=False).any():
            #     check_errors.append("Trade Line cannot be found on the first column, the input file "
            #                         "structure is different from sample template")
            # if check_errors:
            #     raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_freight_table(self):
            notes_df = self.df
            notes_df.replace('', nan, inplace=True)
            notes_df.dropna(axis=0, how='any', inplace=True)
            notes_df.reset_index(drop=True, inplace=True)
            index = list(notes_df[(notes_df[1].str.contains('Rates are inclusive'))].index)
            note_dict = {}
            for index_ in index:
                note_id = notes_df.iloc[index_, 0]
                note_value = notes_df.iloc[index_, 1]

                regex_inc_sub = r"Rates are inclusive of (.+?)Freight Rates are not inclusive of(.+?)$"
                matches_inc = re.finditer(regex_inc_sub, note_value, re.MULTILINE)
                for matchNum, match in enumerate(matches_inc, start=1):
                    inc_string = match.group(1)
                    subject_string = match.group(2)

                regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                included_list = []
                matches_inc = re.finditer(regex_incl, inc_string, re.MULTILINE)
                for matchNum, match in enumerate(matches_inc, start=1):
                    for groupNum in range(0, len(match.groups())):
                        groupNum = groupNum + 1
                        included_list.append(match.group(groupNum))
                included_list = ','.join(included_list)
                note_dict['inclusions'] = {note_id: included_list}

                subject_list = []
                matches_inc = re.finditer(regex_incl, subject_string, re.MULTILINE)
                for matchNum, match in enumerate(matches_inc, start=1):
                    for groupNum in range(0, len(match.groups())):
                        groupNum = groupNum + 1
                        subject_list.append(match.group(groupNum))
                subject_list = ','.join(subject_list)
                note_dict['subject to'] = {note_id: subject_list}

            inclusions_df = pd.DataFrame.from_dict([note_dict['inclusions']])
            inclusions_df.reset_index(inplace=True, drop=True)

            subject_to_df = pd.DataFrame.from_dict([note_dict['subject to']])
            subject_to_df.reset_index(inplace=True, drop=True)

            notes_df = notes_df.T
            notes_df.columns = notes_df.iloc[0]
            notes_df = notes_df[1:].copy()
            notes_df.reset_index(drop=True, inplace=True)
            return notes_df, inclusions_df, subject_to_df

        def capture(self):
            notes_df, inclusions_df, subject_to_df = self.get_freight_table()
            self.captured_output = {'notes': notes_df, 'inclusions': inclusions_df, 'subject to': subject_to_df}

        def clean(self):

            self.cleaned_output = self.captured_output

    class _Surcharge(BaseFix):

        def check_input(self):
            # check_errors = []
            # if not self.df[0].str.contains('TRADE LANE', na=False).any():
            #     check_errors.append("Trade Line cannot be found on the first column, the input file "
            #                         "structure is different from sample template")
            # if check_errors:
            #     raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_fixed_charges(self):
            fixed_charges = self.df

            return fixed_charges

        def capture(self):
            fixed_charges = self.get_fixed_charges()
            self.captured_output = {'fixed_charges': fixed_charges}

        def clean(self):
            self.cleaned_output = self.captured_output

    @classmethod
    def resolve_dependency(cls, fix_outputs):
        freight_df = fix_outputs.pop('Rates')

        notes_df = fix_outputs.pop('Notes')

        surcharge_df = fix_outputs.pop('Fixed Surcharges')

        inclusions = notes_df['inclusions']
        subject_to = notes_df['subject to']

        notes_df = notes_df['notes']

        notes_dict = notes_df.T.to_dict()[0]

        # pd.DataFrame.from_dict([note_dict['']])

        inclusion_id = inclusions.columns.tolist()
        for id in inclusion_id:
            freight_df.loc[(freight_df['remarks'].str.contains(id, regex=True)), 'inclusions'] = inclusions[id].iloc[0]

        subject_to_id = subject_to.columns.tolist()
        for id in subject_to_id:
            freight_df.loc[(freight_df['remarks'].str.contains(id, regex=True)), 'subject to'] = subject_to[id].iloc[0]

        for code in notes_dict:
            _code = (notes_dict[code])
            freight_df.replace(code, _code, inplace=True, regex=True)

        sur_charge = surcharge_df['fixed_charges']
        sur_index = list(sur_charge[(sur_charge[0].str.contains('For', na=False))].index)

        df = freight_df.copy(deep=True)
        for index in sur_index:
            ct_flag = 0
            tmp_df = pd.DataFrame()
            charge_dict = defaultdict(list)
            charge = sur_charge.iloc[index, 0]

            if 'any location except' in charge:
                regex = r"originating from any location except\s((.+?),)(the|(.+?)\()(.+?)\)\sis fixed at USD (.+?)\s"

                matches = re.finditer(regex, charge, re.MULTILINE)

                for matchNum, match in enumerate(matches, start=1):
                    except_country = match.group(2)
                    charge_name = match.group(4)
                    amt = match.group(6)
                    # if 'and' in except_country:
                    #     except_country = except_country.split(' and ')
                    # except_country = ','.join(except_country)
                    # charge_name = charge_name.strip()
                    # except_country = except_country.replace(',', '')
                    except_country = re.sub(",$", '', except_country, 0, re.MULTILINE)
                    charge_dict['except'] = except_country
                    charge_name = charge_name.replace('the ', '')
                    charge_dict['charge_name'] = charge_name
                    charge_dict['amount'] = amt

            elif 'originating from' in charge:
                regex = r"originating from\s((.+?),)(\s?the\s?|(.+?)\()(.+?)\((.+?)\)\sis fixed at USD\s(.+?)\s"

                matches = re.finditer(regex, charge, re.MULTILINE)

                for matchNum, match in enumerate(matches, start=1):
                    origin_country = match.group(1)
                    charge_name = match.group(5)
                    amt = match.group(7)
                    # if 'and' in origin_country:
                    #     origin_country.split(' and ')
                    #     origin_country = ','.join(origin_country)
                    # origin_country = origin_country.replace(',', '')
                    origin_country = re.sub(",$", '', origin_country, 0, re.MULTILINE)
                    charge_dict['origin'] = origin_country
                    charge_name = charge_name.replace('the ', '')
                    charge_dict['charge_name'] = charge_name
                    charge_dict['amount'] = amt

            elif 'cargo bound for' in charge:
                regex = r"cargo bound for(.+?)(.+?) the (.+?)\((.+?)\)\sis\sfixed at (.+?)\s(.+?)\s"

                matches = re.finditer(regex, charge, re.MULTILINE)

                for matchNum, match in enumerate(matches, start=1):
                    origin_country = match.group(2)
                    charge_name = match.group(3)
                    currency = match.group(5)
                    amt = match.group(6)
                    # if ';' in origin_country:
                    #     origin_country.split(';')
                    # origin_country = ','.join(origin_country)
                    # origin_country = origin_country.replace(',', '')
                    origin_country = re.sub(",$", '', origin_country, 0, re.MULTILINE)
                    charge_dict['destination'] = origin_country
                    charge_name = charge_name.replace('the ', '')
                    charge_dict['charge_name'] = charge_name
                    charge_dict['amount'] = amt

            elif 'dangerous cargo' in charge:
                regex = r"dangerous cargo,\s?(.+?)\s\((.+?)\)\sis\sfixed\sat\s(.+?)\s(.+?)\sper\sdry\s(.+?)\sft\scontainer\sor\s(.+?)\s(.+?)\sper\sdry\s(.+?)\s"

                matches = re.finditer(regex, charge, re.MULTILINE)

                for matchNum, match in enumerate(matches, start=1):
                    # origin_country = match.group(2)
                    charge_name = match.group(1)
                    currency = match.group(3)
                    amt_1 = match.group(4)
                    ct_1 = match.group(5)
                    amt_2 = match.group(7)
                    ct_2 = match.group(8)
                    # if ';' in origin_country:
                    #     origin_country.split(';')
                    # origin_country = ','.join(origin_country)
                    # origin_country = origin_country.replace(',', '')
                    # charge_dict['destination'] = origin_country
                    charge_name = charge_name.replace('the ', '')
                    charge_dict['charge_name'] = charge_name
                    charge_dict[ct_1 + '_GP'] = amt_1
                    charge_dict[ct_2 + '_GP'] = amt_2
                    ct_flag = 1

            elif 'general cargo' in charge:
                regex = r"general cargo,\s?(.+?)\s\((.+?)\)\sis\sfixed\sat\s(.+?)\s(.+?)\sper\sdry\s(.+?)\sft\scontainer\sor\s(.+?)\s(.+?)\sper\sdry\s(.+?)\sft,\sor\s(.+?)\s"

                matches = re.finditer(regex, charge, re.MULTILINE)

                for matchNum, match in enumerate(matches, start=1):
                    # origin_country = match.group(2)
                    charge_name = match.group(1)
                    currency = match.group(3)
                    amt_1 = match.group(4)
                    ct_1 = match.group(5)
                    amt_2 = match.group(7)
                    ct_2 = match.group(8)
                    ct_3 = match.group(9)
                    # if ';' in origin_country:
                    #     origin_country.split(';')
                    # origin_country = ','.join(origin_country)
                    # origin_country = origin_country.replace(',', '')
                    # charge_dict['destination'] = origin_country
                    charge_name = charge_name.replace('the ', '')
                    charge_dict['charge_name'] = charge_name
                    charge_dict[ct_1 + '_GP'] = amt_1
                    charge_dict[ct_2 + '_GP'] = amt_2
                    charge_dict[ct_3 + '_HC'] = amt_2
                    ct_flag = 1
            index = None
            if df['charges'].str.contains(charge_dict['charge_name']).any():
                # index = df[df['charges'].str.contains(charge_dict['charge_name'])].index.tolist()
                index_origin = []
                index_except = []
                index_destination = []
                if 'origin' in charge_dict:
                    if ';' in charge_dict['origin']:
                        origin_country = charge_dict['origin'].split(';')
                        for country in origin_country:
                            index_origin_tmp = df.loc[(df['charges'].str.contains(charge_dict['charge_name'])) &
                                                      (df['origin_port'].str.contains(country))].index.tolist()
                            index_origin = list(set(sorted(index_origin + index_origin_tmp)))
                        tmp_df = df.loc[index_origin].copy(deep=True)
                        df.drop(index_origin, axis=0, inplace=True)
                    else:
                        index_origin_tmp = df.loc[(df['charges'].str.contains(charge_dict['charge_name'])) &
                                                  (df['origin_port'].str.contains(
                                                      charge_dict['origin']))].index.tolist()
                        index_origin = list(set(sorted(index_origin + index_origin_tmp)))
                        tmp_df = df.loc[index_origin].copy(deep=True)
                        df.drop(index_origin, axis=0, inplace=True)

                elif 'except' in charge_dict:
                    if 'and' in charge_dict['except']:
                        origin_country = charge_dict['except'].replace(' and ', ';')
                        origin_country = origin_country.split(';')
                        for country in origin_country:
                            index_origin_tmp = df.loc[(df['charges'].str.contains(charge_dict['charge_name'])) &
                                                      ~(df['origin_port'].str.contains(country))].index.tolist()
                            index_except = list(set(sorted(index_except + index_origin_tmp)))
                        tmp_df = df.loc[index_except].copy(deep=True)
                        df.drop(index_except, axis=0, inplace=True)
                    else:
                        index_origin_tmp = df.loc[(df['charges'].str.contains(charge_dict['charge_name'])) &
                                                  ~(df['origin_port'].str.contains(
                                                      charge_dict['except']))].index.tolist()
                        index_except = list(set(sorted(index_except + index_origin_tmp)))
                        tmp_df = df.loc[index_except].copy(deep=True)
                        df.drop(index_except, axis=0, inplace=True)

                elif 'destination' in charge_dict:
                    if ';' in charge_dict['destination']:
                        origin_country = charge_dict['destination'].replace(' and ', ';')
                        origin_country = origin_country.split(';')
                        for country in origin_country:
                            index_origin_tmp = df.loc[(df['charges'].str.contains(charge_dict['charge_name'])) &
                                                      (df['destination_icd'].str.contains(country))].index.tolist()
                            index_destination = list(set(sorted(index_destination + index_origin_tmp)))
                        tmp_df = df.loc[index_destination].copy(deep=True)
                        df.drop(index_destination, axis=0, inplace=True)
                    else:
                        index_origin_tmp = df.loc[(df['charges'].str.contains(charge_dict['charge_name'])) &
                                                  (df['destination_icd'].str.contains(
                                                      charge_dict['destination']))].index.tolist()
                        index_destination = list(set(sorted(index_destination + index_origin_tmp)))
                        tmp_df = df.loc[index_destination].copy(deep=True)
                        df.drop(index_destination, axis=0, inplace=True)

            else:
                index_origin = []
                index_except = []
                index_destination = []
                if 'origin' in charge_dict:
                    if ';' in charge_dict['origin']:
                        origin_country = charge_dict['origin'].replace(' and ', ';')
                        origin_country = origin_country.split(';')
                        for country in origin_country:
                            index_origin_tmp = df.loc[(df['origin_port'].str.contains(country))].index.tolist()
                            index_except = list(set(sorted(index_except + index_origin_tmp)))
                        tmp_df = df.loc[index_origin].copy(deep=True)
                    else:
                        index_origin_tmp = df.loc[
                            (df['origin_port'].str.contains(charge_dict['origin']))].index.tolist()
                        index_origin = list(set(sorted(index_origin + index_origin_tmp)))
                        tmp_df = df.loc[index_origin].copy(deep=True)

                elif 'except' in charge_dict:
                    if ';' in charge_dict['except'] or 'and' in charge_dict['except']:
                        origin_country = charge_dict['except'].replace(' and ', ';')
                        origin_country = origin_country.split(';')
                        for country in origin_country:
                            index_origin_tmp = df.loc[~(df['origin_port'].str.contains(country))].index.tolist()
                            index_except = list(set(sorted(index_except + index_origin_tmp)))
                        tmp_df = df.loc[index_except].copy(deep=True)
                    else:
                        index_origin_tmp = df.loc[
                            ~(df['origin_port'].str.contains(charge_dict['except']))].index.tolist()
                        index_except = list(set(sorted(index_except + index_origin_tmp)))
                        tmp_df = df.loc[index_except].copy(deep=True)

                elif 'destination' in charge_dict:
                    if ';' in charge_dict['destination']:
                        origin_country = charge_dict['destination'].replace(' and ', ';')
                        origin_country = origin_country.split(';')
                        for country in origin_country:
                            index_origin_tmp = df.loc[(df['destination_icd'].str.contains(country))].index.tolist()
                            index_destination = list(set(sorted(index_destination + index_origin_tmp)))
                        tmp_df = df.loc[index_destination].copy(deep=True)
                    else:
                        index_origin_tmp = df.loc[
                            (df['destination_icd'].str.contains(charge_dict['destination']))].index.tolist()
                        index_destination = list(set(sorted(index_destination + index_origin_tmp)))
                        tmp_df = df.loc[index_destination].copy(deep=True)
                else:
                    ct_flag = 1
                    index_origin_tmp = df.loc[(df['charges'].str.contains('basic ocean freight'))].index.tolist()
                    tmp_df = df.loc[index_origin_tmp].copy(deep=True)

            if ct_flag == 1:
                tmp_df['charges'] = charge_dict['charge_name']
                tmp_df['20GP'] = charge_dict['20_GP']
                tmp_df['40GP'] = charge_dict['40_GP']
                if '45_HC' in charge_dict:
                    tmp_df['40HC'] = charge_dict['45_HC']
            else:
                tmp_df['charges'] = charge_dict['charge_name']
                tmp_df['20GP'] = charge_dict['amount']
                tmp_df['40GP'] = charge_dict['amount']
                tmp_df['40HC'] = charge_dict['amount']

            df.reset_index(drop=True, inplace=True)
            df = pd.concat([df, tmp_df], ignore_index=True)

        df['origin_port'] = df['origin_port'].str.split(';')
        # df['destination_port'] = df['destination_port'].str.split(';')
        df['destination_icd'] = df['destination_icd'].str.split(';')
        df = df.explode('origin_port')
        # df = df.explode('destination_port')
        df = df.explode('destination_icd')
        df.reset_index(drop=True, inplace=True)

        # df[['origin_port', 'origin_country', 'drop']] = df['origin_port'].str.split(',', expand=True)
        #
        # df[['destination_icd', 'destination_country']] = df['destination_icd'].str.split(',', expand=True)
        #
        # df.drop(columns=['drop'], inplace=True)

        df_out = {'Freight': df}

        fix_outputs = [df_out]

        # fix_outputs = {'App A - Base port USWC': df_USWC, 'App B - Base port USWC Bullet': df_USWC_bullet, 'App C - Base port USEC': df_USEC, 'App D - Base port USEC Bullet': df_USEC_bullet, 'App E - USWC IPI&MLB ': df_USEC_IPL}

        return fix_outputs


class OOCL_Far_east_v2(BaseTemplate):
    class _Rates(BaseFix):

        def check_input(self):
            check_errors = []
            if not self.df[0].str.contains('Origin', na=False).any():
                check_errors.append("Origin column not present")
            if check_errors:
                raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_freight_table(self):
            """
            Get the table from the rates sheet
            Returns:
                freight table
            """
            regions = []
            index = list(self.df[(self.df[0].str.startswith("Rates - ", na=False))].index)
            freight_df_concat = pd.DataFrame()
            index = index + list(self.df.tail(1).index.values)
            for index_iter in range(len(index) - 1):
                section_df = self.df[index[index_iter]: index[index_iter + 1]].copy(deep=True)
                section_df.reset_index(drop=True, inplace=True)
                region = self.df.iloc[index[index_iter], 0]
                index_region = list(section_df[(section_df[0].str.startswith("Origin", na=False))].index)
                index_region = index_region + list(section_df.tail(1).index.values + 2)
                region = region.replace('Rates - ', '')
                regions.append(region)
                for index_r in range(len(index_region) - 1):
                    commodity = section_df.iloc[section_df[(
                        section_df[0].str.startswith("Commodity", na=False))].index.values[0], 1]
                    freight_df = section_df[index_region[index_r]: index_region[index_r + 1] - 2].copy(deep=True)
                    freight_df.reset_index(drop=True, inplace=True)
                    freight_df.columns = freight_df.iloc[0]
                    freight_df = freight_df[1:].copy()
                    col = freight_df.columns.tolist()
                    counter = 0
                    note_column = ['Notes']
                    for col_ in range(len(col)):
                        if col[col_] == '':
                            counter += 1
                            col[col_] = "Notes" + str(counter)
                            note_column.append(col[col_])

                    freight_df.columns = col
                    freight_df['remarks'] = freight_df[note_column].apply(lambda x: ', '.join(x[x.notnull()]), axis=1)
                    freight_df['region'] = region
                    freight_df['commodity'] = commodity
                    freight_df_concat = pd.concat([freight_df_concat, freight_df], axis=0, ignore_index=True)
            return freight_df_concat, regions

        @staticmethod
        def format_output(df_freight, regions):
            output = {'Freight': df_freight, 'regions': regions}
            return output

        def capture(self):
            freight_df, regions = self.get_freight_table()
            self.captured_output = self.format_output(freight_df, regions)

        def clean(self):
            freight_df = self.captured_output['Freight']

            column_names = {'Origin': 'origin_port', 'Destination': 'destination_icd', 'Mode': 'service_type',
                            20.0: '20GP', 40.0: '40GP', 'Expiry   MM/DD/YY': 'expiry_date', 45.0: '45HC', '40H': '40HC',
                            '40H': '40HC', 'Notes': 'remarks', 'Cargo Nature': 'cargonature', 'region': 'region'
                            }
            freight_df.rename(columns=column_names, inplace=True)
            if 'cargonature' not in freight_df:
                column_names = {'Origin': 'origin_port', 'Destination': 'destination_icd', 'Mode': 'service_type',
                                20.0: '20GP', 40.0: '40GP', '40H': '40HC', 'Notes': 'remarks', 'region': 'region',
                                45.0: '45HC'
                                }
                freight_df = freight_df[list(column_names.values())].copy(deep=True)
            else:
                freight_df = freight_df[list(column_names.values())].copy(deep=True)
            if 'expiry_date' in freight_df:
                freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date']).dt.strftime('%Y-%m-%d')
            freight_df['charges_leg'] = 'L3'
            freight_df['charges'] = 'basic ocean freight'
            self.cleaned_output = {'Freight': freight_df, 'regions': self.captured_output['regions']}

    class _Notes(BaseFix):

        def check_input(self):
            # check_errors = []
            # if not self.df[0].str.contains('TRADE LANE', na=False).any():
            #     check_errors.append("Trade Line cannot be found on the first column, the input file "
            #                         "structure is different from sample template")
            # if check_errors:
            #     raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_freight_table(self):
            notes_df = self.df

            return notes_df

        def capture(self):
            notes_df = self.get_freight_table()
            self.captured_output = {'notes': notes_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    class _Arb(BaseFix):

        def check_input(self):
            # check_errors = []
            # if not self.df[0].str.contains('TRADE LANE', na=False).any():
            #     check_errors.append("Trade Line cannot be found on the first column, the input file "
            #                         "structure is different from sample template")
            # if check_errors:
            #     raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_arb_table(self):
            index = list(self.df[(self.df[0] == ("Location"))].index)

            """
            Hard coded 6 to find the region
            TODO : Implememnt a better solution
            """

            index_ = [x - 3 for x in index]
            arb_df_concat = pd.DataFrame()
            index = index + list(self.df.tail(1).index.values + 3)
            region_list = []
            for index_iter in range(len(index) - 1):
                region = self.df.iloc[index_[index_iter], 0]
                region_list.append(region)
                arb_df = self.df[index[index_iter]: index[index_iter + 1] - 3].copy(deep=True)

                arb_df.columns = arb_df.iloc[0]
                arb_df = arb_df[1:].copy()
                arb_df.rename(
                    columns={"20.0": "20GP", "40.0": '40GP', "40H": "40HC", 'Location': 'icd', 'Over': 'to',
                             'Via': 'via', 'Notes': 'remarks',
                             'Mode': 'service_type', 'TransportType': 'mode_of_transportation', 'Currency': 'currency',
                             'Effective MM/DD/YY': 'start_date', 'Expiry MM/DD/YY': 'expiry_date'},
                    inplace=True)

                arb_df['currency'] = 'USD'
                arb_df['charges_leg'] = 'L2'
                arb_df['charges'] = 'origin arbitrary charges'
                arb_df['at'] = 'origin'
                arb_df['region'] = region
                arb_df_concat = pd.concat([arb_df_concat, arb_df], axis=0, ignore_index=True)
            return arb_df_concat, list(set(region_list))

        def capture(self):
            arb_df, region_list = self.get_arb_table()
            self.captured_output = {'Arbitrary Charges': arb_df, 'regions': region_list}

        def clean(self):
            self.cleaned_output = self.captured_output

    @classmethod
    def resolve_dependency(cls, fix_outputs):
        freight_df = fix_outputs.pop('Rates')

        notes_df = fix_outputs.pop('Notes')

        arb_df = fix_outputs.pop('Inland')

        arb_df_regions = arb_df['regions']
        arb_df = arb_df['Arbitrary Charges']

        notes_df_1 = notes_df['notes']

        regions_list = freight_df.pop('regions')

        freight_df = freight_df.pop('Freight')

        regions_list = regions_list + arb_df_regions

        inc_group_index = []
        for region in regions_list:
            inc_group_index.append(notes_df_1.loc[(notes_df_1[0].str.startswith(region, na=False))].index.values[0])

        inc_group_index = inc_group_index + list(notes_df_1.tail(1).index.values + 1)

        for index_inc in range(len(inc_group_index) - 1):
            notes_df = notes_df_1[inc_group_index[index_inc] + 1:inc_group_index[index_inc + 1]].copy(deep=True)
            notes_df.reset_index(drop=True, inplace=True)
            index = list(notes_df[(notes_df[1].str.contains('Rates are inclusive'))].index)
            note_dict = {}
            for index_ in index:
                note_id = notes_df.iloc[index_, 0]
                note_value = notes_df.iloc[index_, 1]

                regex_inc_sub = r"Rates are inclusive of (.+?). Rates are not inclusive of (.+?)$"
                matches_inc = re.finditer(regex_inc_sub, note_value, re.MULTILINE)
                for matchNum, match in enumerate(matches_inc, start=1):
                    inc_string = match.group(1)
                    subject_string = match.group(2)

                regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                included_list = []
                matches_inc = re.finditer(regex_incl, inc_string, re.MULTILINE)
                for matchNum, match in enumerate(matches_inc, start=1):
                    for groupNum in range(0, len(match.groups())):
                        groupNum = groupNum + 1
                        included_list.append(match.group(groupNum))
                included_list = ','.join(included_list)
                note_dict['inclusions'] = {note_id: included_list}

                subject_list = []
                matches_inc = re.finditer(regex_incl, subject_string, re.MULTILINE)
                for matchNum, match in enumerate(matches_inc, start=1):
                    for groupNum in range(0, len(match.groups())):
                        groupNum = groupNum + 1
                        subject_list.append(match.group(groupNum))
                subject_list = ','.join(subject_list)
                note_dict['subject to'] = {note_id: subject_list}

            if 'inclusions' in note_dict:
                inclusions_df = pd.DataFrame.from_dict([note_dict['inclusions']])
                inclusions_df.reset_index(inplace=True, drop=True)

                inclusion_id = inclusions_df.columns.tolist()
                for id in inclusion_id:
                    freight_df.loc[(freight_df['remarks'].str.contains(id)) &
                                   (freight_df['region'] == (regions_list[index_inc])),
                                   'inclusions'] = inclusions_df[id].iloc[0]

            if 'subject to' in note_dict:
                subject_to_df = pd.DataFrame.from_dict([note_dict['subject to']])
                subject_to_df.reset_index(inplace=True, drop=True)

                subject_to_id = subject_to_df.columns.tolist()
                for id in subject_to_id:
                    freight_df.loc[(freight_df['remarks'].str.contains(id)) &
                                   (freight_df['region'] == (regions_list[index_inc])),
                                   'subject to'] = subject_to_df[id].iloc[0]

            notes_df = notes_df.T
            notes_df.columns = notes_df.iloc[0]
            notes_df = notes_df[1:].copy()
            notes_df.reset_index(drop=True, inplace=True)

            notes_dict = notes_df.T.to_dict()[0]
            for code in notes_dict:
                freight_df.loc[(freight_df['remarks'].str.contains(code)) &
                               (freight_df['region'] == (regions_list[index_inc])),
                               'remarks'] = notes_dict[code]
                arb_df.loc[(arb_df['remarks'].str.contains(code)) &
                           (arb_df['region'] == (regions_list[index_inc])),
                           'remarks'] = notes_dict[code]

        df_out = {'Freight': freight_df, 'Arbitrary Charges': arb_df}

        fix_outputs = [df_out]

        # fix_outputs = {'App A - Base port USWC': df_USWC, 'App B - Base port USWC Bullet': df_USWC_bullet, 'App C - Base port USEC': df_USEC, 'App D - Base port USEC Bullet': df_USEC_bullet, 'App E - USWC IPI&MLB ': df_USEC_IPL}
        return fix_outputs


class Expedoc_OOCL_Fix(BaseTemplate):
    class Word_OOCL_Fix(BaseDocxFix):

        def __init__(self, df: dict, file_model: dict, headers_config: dict):
            super().__init__(df, file_model, headers_config)
            self.dfs = self.df

        def check_input(self):

            special_list = ['&lt;', '&gt;', '&amp;', '&quot;']
            replace_list = ['<', '>', '&', '\"']
            replacer = 0
            for element in special_list:
                if element in self.raw_html:
                    self.raw_html = self.raw_html.replace(element, replace_list[replacer])
                replacer += 1

            regex = r"<s>(.+?)<\/s>"
            subst = ""
            if re.search(regex, self.raw_html):
                self.raw_html = re.sub(regex, subst, self.raw_html, 0, re.MULTILINE)
                self.dfs = pd.read_html(self.raw_html)
                for i in range(len(self.dfs)):
                    self.dfs[i].fillna(value=' ')

            return self.dfs

        def check_output(self):

            pass

        def get_contract_id(self):

            contract_details = self.dfs[0]
            contract_id = ''
            for element in contract_details.iloc[:, 0]:
                if type(element) != float and 'Service Contract number:' in element:
                    contract_id_index = \
                        list(contract_details[(contract_details[0].str.contains('Service Contract number:'))].index)[0]
                    contract_id = contract_details.iloc[contract_id_index][1]
            if contract_id == '':
                contract_id = 0

            return contract_id

        def get_contract_dates(self):

            contract_dates = self.dfs[5]
            contract_dates = contract_dates.fillna(' ')
            contract_start_date, contract_expiry_date = '', ''
            for element in contract_dates.iloc[:, 0]:
                if type(element) != float and 'COMMENCEMENT' in element:
                    contract_start_index = list(contract_dates[(contract_dates[0].str.contains('COMMENCEMENT'))].index)[
                        0]
                    contract_start_date = contract_start_date.replace('', contract_dates.iloc[contract_start_index][2])
                    contract_start_date = parse(contract_start_date)
                if type(element) != float and 'TERMINATION' in element:
                    contract_expiry_index = list(contract_dates[(contract_dates[0].str.contains('TERMINATION'))].index)[
                        0]
                    contract_expiry_date = contract_expiry_date.replace('',
                                                                        contract_dates.iloc[contract_expiry_index][2])
                    contract_expiry_date = parse(contract_expiry_date)
            if contract_start_date == '':
                contract_start_date = 0
            if contract_expiry_date == '':
                contract_expiry_date = 0

            return contract_start_date, contract_expiry_date

        def get_amd_no(self):
            contract_details = self.dfs[0]
            for element in contract_details.iloc[:, 0]:
                if type(element) != float and 'Amendment' in element:
                    amd_index = list(contract_details[(contract_details[0].str.contains('Amendment'))].index)[0]
                    amd_no = contract_details.iloc[amd_index][1]
                    regex = r"[@_!#$%^&*()<>?/|}{~:,]"
                    if re.search(pattern=regex, string=amd_no):
                        amd_no = list(amd_no.split())[0]
                else:
                    amd_no = 0

            return amd_no

        def get_group_name_desc(self):

            picker = re.findall(r'2\. COMMODITIES<\/p>(.+?)<p>3\. MINIMUM QUANTITY COMMITMENT\(S\)', self.raw_html)
            matches = re.finditer(r"<p>(.+?)<\/p>", str(picker), re.MULTILINE)
            raw_commodity_list = []
            for iter in matches:
                raw_commodity_list.append(iter.group())
            commodity_list = []
            for item in raw_commodity_list:
                item = item.replace('\\t', '')
                soup = BeautifulSoup(item, features="html.parser")
                item = soup.get_text().strip()
                if len(item) > 1:
                    commodity_list.append(item)
            commodity_dict = {}
            group_name = []
            for element in commodity_list:
                commodity_dict[element.split(":")[0]] = element.split(":")[-1]
                group_name.append(element.split(":")[0])

            return commodity_dict, group_name

        def get_group_name_2(self):

            group_name = []
            freight_line, arb_line = self.get_line_item()
            regex = r"<p>Commodity:\t(.+?)</p><table>"
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group_name.append(match.group(groupNum).strip())
            return group_name

        def get_main_leg_table(self, group_name, comm_dict={}):

            df_freight_tmp = []
            notes_dict = defaultdict(list)
            note_included = []
            note_not_included = []
            map_notes = {}

            for group in group_name:
                remarks = []
                if '(' in group or ')' in group:
                    temp_group = group.replace('(', '\\(')
                    temp_group = temp_group.replace(')', '\\)')
                else:
                    temp_group = group

                regex = 'Commodity:\t' + temp_group + '(.+?)<p>Note (.+?)Commodity:'
                if re.search(regex, self.raw_html) is None:
                    regex = 'Commodity: ' + temp_group + '(.+?)<p>Note (.+?)ASSESSORIALS'
                    if re.search(regex, self.raw_html) is None:
                        regex = 'Commodity:\t' + temp_group + '(.+?)<p>Note (.+?)<p><strong>GEOGRAPHIC TERMS'
                        if re.search(regex, self.raw_html) is None:
                            regex = r'Commodity:\tGP TWO(.+?)<p>Note (.+?)<p>\t  </p><p><strong>LEGEND'
                            if re.search(regex, self.raw_html) is None:
                                continue

                matches = re.finditer(regex, self.raw_html, re.MULTILINE)
                for matchNum, match in enumerate(matches, start=1):
                    notes_id_list = []
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
                                notes_id_list.append(notes_id)
                                notes_found = 0
                                remark_count = 0
                                # Notes_found = 1 is used to remove the notes that we are capturing in other columns.

                                if re.search(r'Rates are inclusive of the', notes_value) is not None:
                                    matches_inc = re.findall(
                                        r'Rates are inclusive of the(.+?)\s+?Rates are not inclusive of all',
                                        notes_value,
                                        re.MULTILINE)
                                    regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                                    matches_inc_br = re.finditer(regex_incl, matches_inc[0], re.MULTILINE)
                                    for matchNum_2, match_2 in enumerate(matches_inc_br, start=1):
                                        for groupNum_2 in range(0, len(match_2.groups())):
                                            groupNum_2 = groupNum_2 + 1
                                            if match_2.group(groupNum_2) not in note_included:
                                                note_included.append(match_2.group(groupNum_2))
                                    notes_dict[group].append({'Included': note_included})
                                    notes_found = 1
                                    remark_count = 0
                                note_not_included = []

                                if re.search(r'Rates are not inclusive of all', notes_value) is not None:
                                    matches_inc = re.findall(r'Rates are not inclusive of all(.+?)$', notes_value,
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
                                    remark_count = 0

                                if notes_found == 0:
                                    remark_count += 1

                                if remark_count == 1:
                                    remarks.append(notes_value)
                                    notes_dict[group].append({'un-captured_notes': remarks})

                                if remark_count == 0:
                                    notes_dict[group].append({notes_id: notes_value + ';'})
                                else:
                                    notes_dict[group].append({notes_id: notes_value + ';'})
                                if map_notes:
                                    if notes_id in map_notes.keys():
                                        pass
                                    else:
                                        map_notes[notes_id] = notes_value
                                else:
                                    map_notes[notes_id] = notes_value

                        for df_grp in df_freight_list:
                            df_grp.loc[(~df_grp['Notes'].isna()), 'bulletin'] = group
                            if comm_dict:
                                df_grp.loc[(~df_grp['Notes'].isna()), 'commodity'] = comm_dict[group].strip()

                            df_grp.loc[(~df_grp['Notes'].isna()), 'un-captured_notes'] = " ; ".join(remarks)

                            for element in notes_id_list:
                                try:
                                    df_grp.loc[(~df_grp['Notes'].isna()) & (df_grp['Notes'].str.contains(element))
                                    , 'inclusions'] = ','.join(note_included)
                                    df_grp.loc[(~df_grp['Notes'].isna()) & (df_grp['Notes'].str.contains(element))
                                    , 'subject_to'] = ','.join(note_not_included)
                                except AttributeError:
                                    continue
                            df_freight_tmp.append(df_grp)
                if df_freight_tmp:
                    df_freight = pd.concat(df_freight_tmp, axis=0, ignore_index=True)
            return df_freight, notes_dict, map_notes

        def map_notes(self, map_notes, df_freight):

            df_freight['Notes'] = df_freight['Notes'].str.replace(';', ' ', regex=True)
            notes_list = []
            for element in df_freight['Notes']:
                if type(element) != float:
                    notes_list.append(element.split())
                else:
                    notes_list.append(element)
            for i in range(len(notes_list)):
                if type(notes_list[i]) != float:
                    for j in range(len(notes_list[i])):
                        if notes_list[i][j] in map_notes.keys():
                            notes_list[i][j] = map_notes[notes_list[i][j]]
                else:
                    continue

            for i in range(len(df_freight['Notes'])):
                if type(df_freight['Notes'][i]) != float:
                    df_freight['Notes'][i] = " ".join(notes_list[i])

            return df_freight

        def get_lookup(self):
            regex = r'GEOGRAPHIC TERMS(.+?)LEGEND'
            if re.search(regex, self.raw_html) is not None:
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
                geo_code_df['value'] = geo_code_df['value'].str.strip()
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
                geo_code_df.fillna(value=' ')
                geo_code_df.reset_index(inplace=True, drop=True)
                geo_code_df['code'] = geo_code_df['code'].str.strip()
                regex = r"\s\([^()]*\)"
                geo_code_df['code'].replace(regex, '', regex=True, inplace=True)
                geo_code_dict = geo_code_df.set_index('code')['value'].to_dict()
                temp_dict = {}
                for key, value in geo_code_dict.items():
                    if type(key) != float and key.isupper():
                        temp_dict[key] = value
            else:
                temp_dict = {}
                return temp_dict
            return temp_dict

        def get_line_item(self, ):

            regex = r'GEOGRAPHIC TERMS(.+?)LEGEND'
            if re.search(regex, self.raw_html) is None:
                regex = r'Amend the following contract rates</p><p>(.+?)</p><p>Commodity:'
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    test_html = html2text(match.groups(1)[0])
                    line_item = test_html.strip()
                    line_list = line_item.split('\n\n')
                    freight_line_item, arb_line_item = [], []
                    for element in line_list:
                        if ':' not in element and '*' not in element:
                            if 'Arbitrary' not in element:
                                freight_line_item.append(element.strip())
                            else:
                                arb_line_item.append(element.strip()[element.index('F'):])

            return freight_line_item, arb_line_item

        def arb(self, lineitem, amd_no):
            df_arb = pd.DataFrame()

            if re.search(r"ASSESSORIALS(.+?)7. GOVERNING PUBLICATIONS AND TARIFFS OF GENERAL APPLICABILITY:"
                    , self.raw_html) is not None:
                regex_arb = r"ASSESSORIALS(.+?)7. GOVERNING PUBLICATIONS AND TARIFFS OF GENERAL APPLICABILITY:"
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
                for matchNum_note, match_note in enumerate(matches_notes, start=1):
                    for groupNum_note in range(0, len(match_note.groups()) - 1):
                        groupNum_note = groupNum_note + 1
                        notes_value = match_note.group(groupNum_note + 1)
                        notes_id = match_note.group(groupNum_note)
                        notes_dict_arb[notes_id] = notes_value

                df_arb.replace(notes_dict_arb, inplace=True, regex=True)
                df_arb.reset_index(drop=True, inplace=True)
                df_arb.fillna(value=' ')
                df_arb['currency'] = 'USD'
                df_arb['charges_leg'] = 'L3'
                df_arb['charges'] = 'origin arbitrary charges'
                # df_arb['start_date'] = start_date
                # df_arb['expiry_date'] = end_date
                df_arb['at'] = 'origin'
                df_arb.rename(
                    columns={"20": "20GP", "40": '40GP', "40H": "40HC", 'Location': 'icd', 'Over': 'to', 'Via': 'via',
                             'Mode': 'service_type', '45': '45HC', 'Notes': 'remarks'},
                    inplace=True)
                df_arb['trade'] = lineitem[0]
                df_arb['amendment_no'] = amd_no
                return df_arb
            else:
                return df_arb

        @staticmethod
        def format_output(df_freight, df_arb):
            if df_arb.empty:
                output = {'Freight': df_freight}
            else:
                output = {'Freight': df_freight, 'Arbitrary Charges': df_arb}

            return output

        def reefer_splitter(self, df):

            df['40RQ'] = ''
            if 'Nature' in df.columns:
                for i in range(len(df['Nature'])):
                    if df['Nature'][i] == 'RF':
                        df['40RQ'][i] = df['40H'][i]
                        df['40H'][i] = nan

            if 'Eqp.' in df.columns:
                for i in range(len(df['Eqp.'])):
                    if df['Eqp.'][i] == 'RQ':
                        df['40RQ'][i] = df['40H'][i]
                        df['40H'][i] = nan

            return df

        def capture(self):

            self.amd_no = self.get_amd_no()
            self.contract_id = self.get_contract_id()
            self.contract_start_date, self.contract_expiry_date = self.get_contract_dates()
            comm_dict, group_name = self.get_group_name_desc()
            if group_name == []:
                group_name = self.get_group_name_2()
            df_freight, notes_dict, map_notes = self.get_main_leg_table(group_name, comm_dict)
            df_freight = self.map_notes(map_notes, df_freight)
            self.geo_code_dict = self.get_lookup()
            self.freight_line_item, self.arb_line_item = self.get_line_item()
            df_arb = self.arb(self.arb_line_item, self.amd_no)
            df_freight = self.reefer_splitter(df_freight)
            df_arb = self.reefer_splitter(df_arb)
            self.captured_output = self.format_output(df_freight, df_arb)

            return self.captured_output

        def clean(self):

            freight_df = self.captured_output['Freight']
            # freight_df.replace(self.geo_code_dict, inplace=True)
            for code in self.geo_code_dict:
                _code = (self.geo_code_dict[code])
                freight_df.replace(code, _code, inplace=True, regex=True)
                for key, value in self.captured_output.items():
                    if key == 'Arbitrary Charges':
                        arb_df = self.captured_output['Arbitrary Charges']
                        arb_df.replace(code, _code, inplace=True, regex=True)
                        arb_df.applymap(lambda x: x.strip() if type(x) == str else x)
                        break
            if 'Nature' in freight_df.columns:
                freight_df.drop(['Nature'], axis=1, inplace=True)
            freight_df.rename(
                columns={'Origin': 'origin_port', 'Destination': 'destination_icd', 'Mode': 'service_type',
                         "Via(Origin)": "origin_icd", 'Via(Dest.)': 'destination_port',
                         '20': '20GP', '40': '40GP', '40H': '40HC', "45": "45HC", 'Notes': 'remarks',
                         "Eff Date": "start_date", "Exp Date": "expiry_date"
                         }, inplace=True)
            freight_df = freight_df.dropna(how='all')
            freight_df['currency'] = 'USD'
            freight_df['charges_leg'] = 'L3'
            freight_df['charges'] = 'Basic Ocean Freight'
            freight_df['amendment_no'] = self.amd_no
            freight_df['contract_id'] = self.contract_id
            freight_df['contract_start_date'] = self.contract_start_date
            freight_df['contract_expiry_date'] = self.contract_expiry_date
            freight_df['vendor'] = 'OOCL'
            freight_df['destination_port'] = freight_df['destination_port'].str.split(';')
            freight_df = freight_df.explode('destination_port')
            freight_df.reset_index(inplace=True, drop=True)
            freight_df = freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            freight_df.loc[~freight_df["destination_port"].isna() & freight_df["destination_port"].str.contains(
                'Canada'), "trade_name"] = self.freight_line_item[-1]
            freight_df.loc[
                ~freight_df["destination_port"].isna() & ~freight_df["destination_port"].str.contains('Canada',
                                                                                                      na=False), "trade_name"] = \
                self.freight_line_item[0]
            freight_df = freight_df.applymap(lambda x: x.strip() if type(x) == str else x)
            if 'Arbitrary Charges' in self.captured_output:
                self.cleaned_output = {'Freight': freight_df,
                                       'Arbitrary Charges': arb_df}
            else:
                self.cleaned_output = {'Freight': freight_df}

            return self.cleaned_output


class OOCL_Excel_Fix(BaseTemplate):
    class OceanRates(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_validality(self):
            def get_validity_date(date_input):
                return re.search(r"valid as from (.*)-(.*)", date_input)

            captured_validity = self.df.iloc[:, 0].apply(lambda x: get_validity_date(str(x)))
            start_date = ""
            expiry_date = ""
            for i in captured_validity:
                if i:
                    start_date_group = i.group(1).strip()
                    start_date = start_date_group
                    expiry_date_group = i.group(2).strip()
                    expiry_date = expiry_date_group
            return start_date, expiry_date

        def get_service_contract_no(self):
            def get_service_no(date_input):
                return re.search(r"(Service Contract Number.*)", date_input)

            service_contract_no_captured = self.df.iloc[:, 0].apply(lambda x: get_service_no(str(x)))
            service_contract_no = ""

            for i in service_contract_no_captured:
                if i:
                    service_contract_no = i.group(1).strip()
            return service_contract_no

        def get_surcharges(self):
            heavy_weight_df = pd.DataFrame()
            if self.df[0].str.startswith('heavy weight additional', na=False).any():
                heavy_index = self.df[self.df[0].str.startswith('heavy weight additional', na=False)].index.values[0]
                heavy_weight = self.df.iloc[heavy_index][0]
                tons_payload = self.df[self.df.iloc[:, 1].str.contains('tons payload', na=False)]

                heavy_weight_df[['remarks', 'remarks_']] = tons_payload.iloc[:, 1].str.split(":", expand=True, n=1)
                heavy_weight_df[['currency', 'remarks_']] = heavy_weight_df["remarks_"].str.strip().str.split(" ",
                                                                                                              expand=True,
                                                                                                              n=1)
                heavy_weight_df[['amount', 'load_type']] = heavy_weight_df["remarks_"].str.strip().str.split(r"/",
                                                                                                             expand=True,
                                                                                                             n=1)
                heavy_weight_df["charges"] = heavy_weight
                heavy_weight_df.drop(columns=["remarks_"], inplace=True)

            index = self.df[self.df[0].str.contains('plus following surcharges, valid until further notice', case=False,
                                                    na=False)].index.values[0]

            if self.df[0].str.contains('plus origin', case=False, na=False).any():
                end_index = self.df[self.df[0].str.contains('plus origin', case=False, na=False)].index.values[0]
            else:
                end_index = self.df[self.df[0].str.startswith('Effective', na=False)].index.values[0]

            surcharges_df = self.df.loc[index:end_index - 1, :]
            surcharges_df = surcharges_df.loc[
                ~surcharges_df.iloc[:, 0].str.contains("plus following surcharges", case=False, na=False)]
            surcharges_dict = surcharges_df.to_dict("records")
            surcharges_lst = []
            for row in surcharges_dict:
                get_charges = re.compile(r"(?P<charge>[A-Z]{3})")
                get_charges_code = re.compile(r"(?P<code>[0-9]{3})")
                captured_data_charge = re.search(get_charges, row[0])
                captured_data_chargecode = re.search(get_charges_code, row[0])
                charges = ""
                if captured_data_charge:
                    charges = captured_data_charge.group("charge")
                charges_code = ""
                if captured_data_chargecode:
                    charges_code = captured_data_chargecode.group("code")

                check_pattern = re.compile(r"(?P<cr>[A-Z]{3})\s(?P<amt>\d+),-(?P<type>[A-Za-z \/]+)(?P<rmks>(.*))")
                check_pattern_1 = re.compile(r"(?P<cr>[A-Z]{3})\s(?P<amt>\d+),-\s?\/?(?P<type>\/.*)-(?P<rmks>.*)")
                check_pattern_6 = re.compile(r"(?P<cr>[A-Z]{3})\s(?P<amt>\d+),-(?P<type>.*)(?P<rmks>\(.*)")
                check_pattern_2 = re.compile(r"(?P<cr>[A-Z]{3})\s(?P<amt>\d+)\s\/.(?P<type>[A-z]{3})(?P<rmks>.*)")
                check_pattern_7 = re.compile(r"(?P<cr>[A-Z]{3})\s(?P<amt>\d+)(?P<type>[A-Za-z \/]+)(?P<rmks>(.*))")
                check_pattern_3 = re.compile(r"(?P<cr>[A-Z]{3})\s(?P<amt>\d+)\/(?P<type>.*)")
                check_pattern_4 = re.compile(r"(?P<cr>[A-Z]{3})\s(?P<amt>\d+)(?P<type>.*)")
                check_pattern_5 = re.compile(
                    r"(?P<cr>[A-Z]{3})\s(?P<amt1>\d+),-\/(?P<amt2>\d+),-\/(?P<amt3>\d+),-.*(?P<rmks>\(.*)")
                if check_pattern_5.match(row[1]):

                    amt = 1
                    for load_type in ["20GP", "40GP", "40HC"]:
                        surcharge_dict = {}
                        surcharge_dict["charges"] = charges
                        surcharge_dict["code"] = charges_code
                        surcharge_dict["charges_"] = row[0]
                        captured_data = re.search(check_pattern_5, row[1])
                        surcharge_dict["currency"] = captured_data.group("cr")
                        amt_ = "amt" + str(amt)
                        surcharge_dict["amount"] = captured_data.group(amt_)
                        surcharge_dict["load_type"] = load_type
                        surcharge_dict["remarks"] = captured_data.group("rmks")
                        amt += 1
                        surcharges_lst.append(surcharge_dict)

                elif check_pattern.match(row[1]):
                    captured_data = re.search(check_pattern, row[1])
                    surcharge_dict = {}
                    surcharge_dict["charges"] = charges
                    surcharge_dict["code"] = charges_code
                    surcharge_dict["charges_"] = row[0]
                    surcharge_dict["currency"] = captured_data.group("cr")
                    surcharge_dict["amount"] = captured_data.group("amt")
                    surcharge_dict["load_type"] = captured_data.group("type")
                    surcharge_dict["remarks"] = captured_data.group("rmks")
                    surcharges_lst.append(surcharge_dict)

                elif check_pattern_1.match(row[1]):
                    captured_data = re.search(check_pattern_1, row[1])
                    surcharge_dict = {}
                    surcharge_dict["charges"] = charges
                    surcharge_dict["code"] = charges_code
                    surcharge_dict["charges_"] = row[0]
                    surcharge_dict["currency"] = captured_data.group("cr")
                    surcharge_dict["amount"] = captured_data.group("amt")
                    surcharge_dict["load_type"] = captured_data.group("type")
                    surcharge_dict["remarks"] = captured_data.group("rmks")
                    surcharges_lst.append(surcharge_dict)

                elif check_pattern_2.match(row[1]):
                    captured_data = re.search(check_pattern_2, row[1])
                    surcharge_dict = {}
                    surcharge_dict["charges"] = charges
                    surcharge_dict["code"] = charges_code
                    surcharge_dict["charges_"] = row[0]

                    surcharge_dict["currency"] = captured_data.group("cr")
                    surcharge_dict["amount"] = captured_data.group("amt")
                    surcharge_dict["load_type"] = captured_data.group("type")
                    surcharge_dict["remarks"] = captured_data.group("rmks")
                    surcharges_lst.append(surcharge_dict)
                #
                # elif check_pattern_7.match(row[1]):
                #     captured_data = re.search(check_pattern_7, row[1])
                #     surcharge_dict = {}
                #     surcharge_dict["charges"] = row[0]
                #     surcharge_dict["currency"] = captured_data.group("cr")
                #     surcharge_dict["amount"] = captured_data.group("amt")
                #     surcharge_dict["load_type"] = captured_data.group("type")
                #     surcharge_dict["remarks"] = captured_data.group("rmks")
                #     surcharges_lst.append(surcharge_dict)

                elif check_pattern_3.match(row[1]):
                    captured_data = re.search(check_pattern_3, row[1])
                    surcharge_dict = {}
                    surcharge_dict["charges"] = charges
                    surcharge_dict["code"] = charges_code
                    surcharge_dict["charges_"] = row[0]

                    surcharge_dict["currency"] = captured_data.group("cr")
                    surcharge_dict["amount"] = captured_data.group("amt")
                    surcharge_dict["load_type"] = captured_data.group("type")
                    # surcharge_dict["remarks"] = captured_data.group("rmks")
                    surcharges_lst.append(surcharge_dict)

                elif check_pattern_4.match(row[1]):
                    captured_data = re.search(check_pattern_4, row[1])
                    surcharge_dict = {}
                    surcharge_dict["charges"] = charges
                    surcharge_dict["code"] = charges_code
                    surcharge_dict["charges_"] = row[0]
                    surcharge_dict["currency"] = captured_data.group("cr")
                    surcharge_dict["amount"] = captured_data.group("amt")
                    surcharge_dict["load_type"] = captured_data.group("type")
                    # surcharge_dict["remarks"] = captured_data.group("rmks")
                    surcharges_lst.append(surcharge_dict)

                elif check_pattern_6.match(row[1]):
                    captured_data = re.search(check_pattern_6, row[1])
                    surcharge_dict = {}
                    surcharge_dict["charges"] = charges
                    surcharge_dict["code"] = charges_code
                    surcharge_dict["charges_"] = row[0]
                    surcharge_dict["currency"] = captured_data.group("cr")
                    surcharge_dict["amount"] = captured_data.group("amt")
                    surcharge_dict["load_type"] = captured_data.group("type")
                    surcharge_dict["remarks"] = captured_data.group("rmks")
                    surcharges_lst.append(surcharge_dict)

            surcharges_df = pd.concat([pd.DataFrame(surcharges_lst), heavy_weight_df], ignore_index=True)

            return surcharges_df

        def melt_load_type(self, df):
            if "load_type" not in df:
                df = df.melt(id_vars=[column for column in df.columns if not column[1].isdigit()],
                             var_name="load_type",
                             value_name="amount")

                return df

        def get_inclusions(self):
            def get_inclusions_(date_input):
                return re.search(r"included charges:(.*)", date_input, re.IGNORECASE)

            captured_inclusions = self.df.iloc[:, 0].apply(lambda x: get_inclusions_(str(x)))
            inclusions = ""
            for i in captured_inclusions:
                if i:
                    inclusions = i.group(1)
            return inclusions

        def first_rows_as_header(self, df):
            headers = df.iloc[0]
            df = df[1:]
            df.columns = headers
            return df

        def fix_origin_port(self, df, pod_df):
            origin_dict = df.to_dict("records")
            pod_df['destination_icd'] = pod_df['destination_icd'].str.split(r';')
            pod_df = pod_df.explode('destination_icd')
            dps = []
            for row in origin_dict:
                filtered_df = pod_df.loc[pod_df["destination_icd"].str.replace(" ", "", regex=True).str.contains(
                    row["destination_port"].replace(" ", "").replace("Pusan", "Busan"), case=False, na=False)]
                filtered_df["origin_port"] = row["origin_port"]
                dps.append(filtered_df)
            df = pd.concat(dps, ignore_index=True)
            return df

        def fix_over_blocks(self, block):
            if block[0].str.startswith('POL').any():
                index = block[block[0].str.startswith('POL')].index.values[0]
            else:
                index = block[block[0].str.startswith('POD')].index.values[0]
            block = block.applymap(lambda x: nan if x == ':' or x == '' else x)
            block = block.dropna(axis=1, how='all')
            block = block.T.reset_index(drop=True).T
            region = block[0].values[0]
            remarks = block[2].values[0]

            freight_df = block.loc[index:, :]
            freight_df = self.first_rows_as_header(freight_df)
            columns_rename = {"POL": "origin_port", "POD": "destination_port",
                              "20' Box": "20GP", "40' Box": "40GP", "40' HQ": "40HC",
                              "GC;DG": "cargo_type_", "GC; DG": "cargo_type_",
                              "Loop": "loop", "transshipment via": "destination_icd",
                              "via transshipment": "destination_icd", "Transshipment via": "destination_icd",
                              "20' GP": "20GP", "40' GP": "40GP"}
            freight_df.rename(columns=columns_rename, inplace=True)

            if "origin_port" in freight_df:
                freight_df.dropna(subset=["origin_port", "destination_port", "20GP"], inplace=True)
            else:
                freight_df.dropna(subset=["destination_port", "destination_icd", "40HC"], inplace=True)

            freight_df["region"] = region
            freight_df["remarks"] = remarks
            if block[1].str.contains('Rates incl.').any():
                inclusions_text = block[1].values[0]
                try:
                    freight_df["inclusions"] = inclusions_text.split("subject to")[0].split(".")[1]
                    freight_df["subject_to"] = inclusions_text.split("subject to")[1]
                except:
                    raise "Issue in getting inclusion from fix_over_blocks"

            spl_character_replace = {"-": "", "--": "", "$": ""}
            freight_df["20GP"].replace(spl_character_replace, inplace=True, regex=True)
            freight_df["40GP"].replace(spl_character_replace, inplace=True, regex=True)
            freight_df["40HC"].replace(spl_character_replace, inplace=True, regex=True)
            freight_df["20GP"] = freight_df["20GP"].apply(lambda x: str(x).strip('$'))
            freight_df["40GP"] = freight_df["40GP"].apply(lambda x: str(x).strip('$'))
            freight_df["40HC"] = freight_df["40HC"].apply(lambda x: str(x).strip('$'))

            freight_df.loc[(freight_df["destination_port"].str.contains("via")), "destination_icd"] = \
            freight_df["destination_port"].str.split("via").str[1].replace("\)", "", regex=True).replace(r"/rail", "",
                                                                                                         regex=True)

            # if "destination_icd" in freight_df:
            #     freight_df.loc[(freight_df["destination_icd"].str.contains("via")), "via_port"] = freight_df["destination_icd"]
            #     freight_df.loc[(freight_df["destination_icd"].str.contains("via")), "destination_icd"] = ""
            if "destination_icd" in freight_df:
                freight_df["destination_icd"] = freight_df["destination_icd"].replace("via", "", regex=True)

            freight_df = freight_df.loc[~freight_df["20GP"].str.contains("Khor Fakkan")]

            if not freight_df.empty:
                if "cargo_type_" in freight_df:
                    freight_df.loc[freight_df["cargo_type_"] == "DG", "cargo_type"] = "ONLY"
                    freight_df.loc[freight_df["cargo_type_"] == "GC", "cargo_type"] = "NO"

            freight_df["region"] = region

            return freight_df

            # freight_df = freight_df.fillna('')

        def get_regional_sections_pod(self):
            regional_sections = {}
            indexes = self.df[self.df[0].str.contains("POD", na=False)].index.tolist()
            end_index = self.df.index[-1]
            indexes.append(end_index)
            regional_sections = zip(indexes, indexes[1:])
            return regional_sections

        def get_regional_sections(self):
            regional_sections = {}
            indexes = self.df[self.df[0].str.contains("POL", na=False)].index.tolist()
            if self.df[0].str.contains("POD", na=False).any():
                end_index = self.df[self.df[0].str.contains("POD", na=False)].index.values[0]
            else:
                end_index = self.df.index[-1]

            indexes.append(end_index)
            regional_sections = zip(indexes, indexes[1:])
            return regional_sections

        def capture(self):
            start_date, expiry_date = self.get_validality()
            inclusions = self.get_inclusions()
            service_contract_no = self.get_service_contract_no()
            regional_sections = self.get_regional_sections()
            surcharges = self.get_surcharges()
            dps = []
            for regional_config in regional_sections:
                regional_df = self.df.loc[regional_config[0] - 1: regional_config[1] - 1, :]
                regional_df = regional_df.T.reset_index(drop=True).T
                block_df = self.fix_over_blocks(regional_df)
                dps.append(block_df)
            df = pd.concat(dps, ignore_index=True)

            origin_df = pd.DataFrame()
            if self.df[0].str.contains("POD").any():
                dps_pod = []
                regional_sections_pod = self.get_regional_sections_pod()
                for regional_config in regional_sections_pod:
                    regional_df = self.df.loc[regional_config[0] - 1: regional_config[1] - 1, :]
                    regional_df = regional_df.T.reset_index(drop=True).T
                    block_df = self.fix_over_blocks(regional_df)

                    dps_pod.append(block_df)
                pod_df = pd.concat(dps_pod, ignore_index=True)

                origin_df = self.fix_origin_port(df, pod_df)

            result_df = pd.concat([df, origin_df], ignore_index=True)

            if not "inclusions" in result_df:
                result_df["inclusions"] = inclusions
            result_df["start_date"] = start_date
            result_df["expiry_date"] = expiry_date

            result_df['origin_port_ref'] = result_df['origin_port']
            result_df['destination_port'] = result_df['destination_port'].str.split('/ ')
            result_df = result_df.explode('destination_port')

            result_df['origin_port'] = result_df['origin_port'].str.split('/')
            result_df = result_df.explode('origin_port')

            result_df['origin_port'] = result_df['origin_port'].str.split(r';')
            result_df = result_df.explode('origin_port')
            # result_df["currency"] = "USD"
            result_df["remarks"] = service_contract_no
            result_df["inclusions"].replace(",", ";", regex=True, inplace=True)
            if "destination_icd" in result_df:
                result_df['destination_icd'] = result_df['destination_icd'].str.replace("/", ";", regex=True)
                result_df['destination_icd'] = result_df['destination_icd'].str.split(r';')
                result_df = result_df.explode('destination_icd')

                result_df['destination_icd'] = result_df['destination_icd'].str.replace("-", ";", regex=True)

            port_pair_lookup = {
                "WVN": "Wilhelmshaven",
                "HAM": "Hamburg",
                "RTM": "Rotterdam",
                "ANR": "Antwerpen",
                "ZEE": "Zeebruegge",
                "BHV": "Bremerhaven"}

            result_df['origin_port'] = result_df['origin_port'].replace(port_pair_lookup, regex=True)
            result_df = result_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            surcharges["start_date"] = start_date
            surcharges["expiry_date"] = expiry_date

            result_df = self.melt_load_type(result_df)

            surcharges_df_with_rates = result_df.loc[result_df["amount"].str.contains("^\d+", na=False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"] = \
            surcharges_df_with_rates["amount"].str.split(" ").str[1], \
            surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = result_df.loc[~result_df["amount"].str.contains("^\d", na=False)]
            surcharges_df_without_rates["amount"] = surcharges_df_without_rates["amount"]
            replace_currency = {"EUR": "", "USD": ""}
            surcharges_df_without_rates["amount"].replace(replace_currency, regex=True, inplace=True)
            surcharges_df_without_rates["amount"].replace({"on request": "ON REQUEST", "no offer": "ON REQUEST"},
                                                          regex=True, inplace=True)

            freight_df = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index=True)
            freight_df = freight_df.loc[freight_df["amount"] != ""]
            freight_df["amount"].replace({"on request": "ON REQUEST", "no offer": "ON REQUEST"}, regex=True,
                                         inplace=True)

            freight_df["origin_port"].replace(replace_currency, regex=True, inplace=True)
            freight_df["destination_port"].replace(replace_currency, regex=True, inplace=True)
            freight_df["destination_icd"].replace(replace_currency, regex=True, inplace=True)

            self.captured_output = {'Freight': freight_df, "Surcharges": surcharges}

        def clean(self):

            self.cleaned_output = self.captured_output

    def resolve_dependency(cls, fix_outputs):
        if "Far East+Japan" in fix_outputs:
            df_freight = fix_outputs.pop("Far East+Japan")
            far_freight_df = df_freight["Freight"]
            far_freight_df["unique"] = "Far East+Japan"
            far_Surcharges_df = df_freight["Surcharges"]
            far_Surcharges_df["unique"] = "Far East+Japan"

        if "Koper+Trieste" in fix_outputs:
            df_freight = fix_outputs.pop("Koper+Trieste")
            KT_freight_df = df_freight["Freight"]
            KT_freight_df["unique"] = "Koper+Trieste"
            KT_Surcharges_df = df_freight["Surcharges"]
            KT_Surcharges_df["unique"] = "Koper+Trieste"

        if "Intra Europe" in fix_outputs:
            df_freight = fix_outputs.pop("Intra Europe")
            IE_freight_df = df_freight["Freight"]
            IE_freight_df["unique"] = "Intra Europe"
            IE_Surcharges_df = df_freight["Surcharges"]
            IE_Surcharges_df["unique"] = "Intra Europe"

        if "Middle East" in fix_outputs:
            df_freight = fix_outputs.pop("Middle East")
            ME_freight_df = df_freight["Freight"]
            ME_freight_df["unique"] = "Middle East"
            ME_Surcharges_df = df_freight["Surcharges"]
            ME_Surcharges_df["unique"] = "Middle East"

        if "India+Pakistan" in fix_outputs:
            df_freight = fix_outputs.pop("India+Pakistan")
            IP_freight_df = df_freight["Freight"]
            IP_freight_df["unique"] = "India+Pakistan"
            IP_Surcharges_df = df_freight["Surcharges"]
            IP_Surcharges_df["unique"] = "India+Pakistan"

        if "Australia & New Zealand" in fix_outputs:
            df_freight = fix_outputs.pop("Australia & New Zealand")
            AZ_freight_df = df_freight["Freight"]
            AZ_freight_df["unique"] = "Australia & New Zealand"
            AZ_Surcharges_df = df_freight["Surcharges"]
            AZ_Surcharges_df["unique"] = "Australia & New Zealand"

        freight_df = pd.concat(
            [far_freight_df, KT_freight_df, IE_freight_df, ME_freight_df, IP_freight_df, AZ_freight_df],
            ignore_index=False)
        freight_df["basis"] = "container"
        freight_df["sub_vendor"] = "ORIENTOVERSEASCONTAINERLINELTD.(HK-28195BREMEN)"
        freight_df["contract_no"] = "G2000907"
        freight_df = freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        freight_df["start_date"], freight_df["expiry_date"] = pd.to_datetime(freight_df["start_date"],
                                                                             format="%d.%m.%Y"), pd.to_datetime(
            freight_df["expiry_date"], format="%d.%m.%Y")

        Surcharges_df = pd.concat(
            [far_Surcharges_df, KT_Surcharges_df, IE_Surcharges_df, ME_Surcharges_df, IP_Surcharges_df,
             AZ_Surcharges_df], ignore_index=False)
        Surcharges_df["start_date"], Surcharges_df["expiry_date"] = pd.to_datetime(Surcharges_df["start_date"],
                                                                                   format="%d.%m.%Y"), pd.to_datetime(
            Surcharges_df["expiry_date"], format="%d.%m.%Y")

        Surcharges_df = Surcharges_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        fix_outputs = [{"Freight": freight_df, "Surcharges": Surcharges_df}]
        return fix_outputs


class OOCL_FarEast_Excel(BaseTemplate):
    class FarEast_Freight_Fix(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_validity(self):
            extra_details = {}
            client_index = self.df[(self.df.iloc[:, 0].str.contains('Client'))].index.values[0]
            extra_details["client_name"] = self.df.iloc[client_index, 1]

            carrier_index = self.df[(self.df.iloc[:, 0].str.contains('Carrier'))].index.values[0]
            extra_details["sub_vendor"] = self.df.iloc[carrier_index, 1]

            contract_index = self.df[(self.df.iloc[:, 0].str.contains('SC-Number:'))].index.values[0]
            # extra_details["contract_no"] = self.df.iloc[contract_index, 1]
            extra_details["contract_no"] = self.df.iloc[contract_index].to_string(index=False).replace("\n", " ")

            validity_index = self.df[(self.df.iloc[:, 0].str.contains('Validity:'))].index.values[0]
            validity = self.df.iloc[validity_index, 1]

            if validity:
                start_date = validity.split("-")[0].replace(" ", "")
                expiry_date = validity.split("-")[1]

                start_date += expiry_date.split(".")[2].replace(" ", "")

                start_date = dateparser.parse(start_date, date_formats=['%d.%m.%y', '%d.%m.%Y', '%d.%m%Y'])
                expiry_date = dateparser.parse(expiry_date)

                extra_details["start_date"] = start_date
                extra_details["expiry_date"] = expiry_date
            return extra_details

        def capture(self):
            validity = self.get_validity()
            freight_df = self.df
            freight_df.replace('', nan, inplace=True)
            freight_df = freight_df.dropna(subset=["origin_port", "destination_port", "20GP"])
            freight_df = freight_df[freight_df.origin_port != 'POL']
            freight_df["origin_port"] = freight_df["origin_port"].str.split(";")
            freight_df = freight_df.explode("origin_port")

            freight_df["destination_port"] = freight_df["destination_port"].str.split(";")
            freight_df = freight_df.explode("destination_port")

            freight_df["destination_port"] = freight_df["destination_port"].str.split("/")
            freight_df = freight_df.explode("destination_port")

            freight_df["client_name"] = validity["client_name"]
            freight_df["sub_vendor"] = validity["sub_vendor"]
            freight_df["contract_no"] = validity["contract_no"]
            freight_df["start_date"] = validity["start_date"]
            freight_df["expiry_date"] = validity["expiry_date"]
            freight_df['charges_leg'] = 'L3'
            freight_df['currency'] = 'USD'
            freight_df['charges'] = 'Basic Ocean Freight'
            freight_df["remarks"] = validity["contract_no"]
            freight_df["basis"] = "container"
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date']).dt.strftime('%d-%m-%Y')
            freight_df['start_date'] = pd.to_datetime(freight_df['start_date']).dt.strftime('%d-%m-%Y')

            freight_df = freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            self.captured_output = {"Freight": freight_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    def resolve_dependency(cls, fix_outputs):

        if "Far East Chinese ports" in fix_outputs:
            Far_East_Chinese_ports_freight = fix_outputs.pop('Far East Chinese ports')
            Far_East_Chinese_ports_freight_df = Far_East_Chinese_ports_freight['Freight']
            Far_East_Chinese_ports_freight_df["unique"] = "Far East Chinese ports"

        if "Far East non Chinese ports" in fix_outputs:
            FarEast_non_Chinese_ports = fix_outputs.pop('Far East non Chinese ports')
            FarEast_non_Chinese_ports_df = FarEast_non_Chinese_ports['Freight']
            FarEast_non_Chinese_ports_df["unique"] = "Far East non Chinese ports"

        if "India Dry" in fix_outputs:
            india_dry_ports = fix_outputs.pop('India Dry')
            india_dry_ports_df = india_dry_ports['Freight']
            india_dry_ports_df["unique"] = "India Dry"

        Freight_df = pd.concat([Far_East_Chinese_ports_freight_df, FarEast_non_Chinese_ports_df, india_dry_ports_df],
                               ignore_index=False)

        Freight_df = Freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        Freight_df["sub_vendor"] = "ORIENTOVERSEASCONTAINERLINELTD.(HK-28195BREMEN)"
        Freight_df["contract_no"] = "G2000907"

        fix_outputs = [{"Freight": Freight_df}]
        return fix_outputs


class OOCL_Far_east_v3(BaseTemplate):
    class _Rates(BaseFix):

        def check_input(self):
            check_errors = []
            if not self.df[0].str.contains('Origin', na=False).any():
                check_errors.append("Origin column not present")
            if check_errors:
                raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_freight_table(self):
            """
            Get the table from the rates sheet
            Returns:
                freight table
            """
            regions = []
            index = list(self.df[(self.df[0].str.startswith("Rates - ", na=False))].index)
            freight_df_concat = pd.DataFrame()
            index = index + list(self.df.tail(1).index.values + 1)
            for index_iter in range(len(index) - 1):
                section_df = self.df[index[index_iter]: index[index_iter + 1]].copy(deep=True)
                section_df.reset_index(drop=True, inplace=True)
                region = self.df.iloc[index[index_iter], 0]
                index_region = list(section_df[(section_df[0].str.startswith("Origin", na=False))].index)
                index_region = index_region + list(section_df.tail(1).index.values + 2)
                region = region.replace('Rates - ', '')
                regions.append(region)
                for index_r in range(len(index_region) - 1):
                    commodity = section_df.iloc[section_df[(
                        section_df[0].str.startswith("Commodity", na=False))].index.values[0], 1]
                    freight_df = section_df[index_region[index_r]: index_region[index_r + 1] - 1].copy(deep=True)
                    freight_df.reset_index(drop=True, inplace=True)
                    freight_df.columns = freight_df.iloc[0]
                    freight_df = freight_df[1:].copy()
                    col = freight_df.columns.tolist()
                    counter = 0
                    note_column = ['Notes']
                    for col_ in range(len(col)):
                        if col[col_] == '':
                            counter += 1
                            col[col_] = "Notes" + str(counter)
                            note_column.append(col[col_])

                    freight_df.columns = col
                    freight_df['remarks'] = freight_df[note_column].apply(lambda x: ', '.join(x[x.notnull()]), axis=1)
                    freight_df['region'] = region
                    freight_df['commodity'] = commodity
                    freight_df_concat = pd.concat([freight_df_concat, freight_df], axis=0, ignore_index=True)
            return freight_df_concat, regions

        @staticmethod
        def format_output(df_freight, regions):
            output = {'Freight': df_freight, 'regions': regions}
            return output

        def capture(self):
            freight_df, regions = self.get_freight_table()
            self.captured_output = self.format_output(freight_df, regions)

        def clean(self):
            freight_df = self.captured_output['Freight']

            column_names = {'Origin': 'origin_port', 'Destination': 'destination_icd', 'Mode': 'service_type',
                            20.0: '20GP', 40.0: '40GP', 'Expiry   MM/DD/YY': 'expiry_date', '40H': '40HC',
                            '40H': '40HC', 'remarks': 'remarks', 'Cargo Nature': 'cargonature', 'region': 'region',
                            'commodity': 'commodity'
                            }
            freight_df.rename(columns=column_names, inplace=True)
            if 'cargonature' not in freight_df:
                column_names = {'Origin': 'origin_port', 'Destination': 'destination_icd', 'Mode': 'service_type',
                                20.0: '20GP', 40.0: '40GP', '40H': '40HC', 'remarks': 'remarks', 'region': 'region',
                                'commodity': 'commodity'
                                }
                freight_df = freight_df[list(column_names.values())].copy(deep=True)
            else:
                freight_df = freight_df[list(column_names.values())].copy(deep=True)
            if 'expiry_date' in freight_df:
                freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date']).dt.strftime('%Y-%m-%d')
            freight_df['charges_leg'] = 'L3'
            freight_df['charges'] = 'basic ocean freight'
            self.cleaned_output = {'Freight': freight_df, 'regions': self.captured_output['regions']}

    class _Notes(BaseFix):

        def check_input(self):
            # check_errors = []
            # if not self.df[0].str.contains('TRADE LANE', na=False).any():
            #     check_errors.append("Trade Line cannot be found on the first column, the input file "
            #                         "structure is different from sample template")
            # if check_errors:
            #     raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_freight_table(self):
            notes_df = self.df

            return notes_df

        def capture(self):
            notes_df = self.get_freight_table()
            self.captured_output = {'notes': notes_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    class _Arb(BaseFix):

        def check_input(self):
            # check_errors = []
            # if not self.df[0].str.contains('TRADE LANE', na=False).any():
            #     check_errors.append("Trade Line cannot be found on the first column, the input file "
            #                         "structure is different from sample template")
            # if check_errors:
            #     raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_arb_table(self):
            index = list(self.df[(self.df[0] == ("Location"))].index)

            """
            Hard coded 6 to find the region
            TODO : Implememnt a better solution
            """

            index_ = [x - 3 for x in index]
            arb_df_concat = pd.DataFrame()
            index = index + list(self.df.tail(1).index.values + 4)
            region_list = []
            for index_iter in range(len(index) - 1):
                region = self.df.iloc[index_[index_iter], 0]
                region_list.append(region)
                arb_df = self.df[index[index_iter]: index[index_iter + 1] - 3].copy(deep=True)

                arb_df.columns = arb_df.iloc[0]
                arb_df = arb_df[1:].copy()
                arb_df.rename(
                    columns={20.0: "20GP", 40.0: '40GP', "40H": "40HC", 'Location': 'icd', 'Over': 'to',
                             'Via': 'via', 'Notes': 'remarks',
                             'Mode': 'service_type', 'TransportType': 'mode_of_transportation', 'Currency': 'currency',
                             'Effective MM/DD/YY': 'start_date', 'Expiry MM/DD/YY': 'expiry_date'},
                    inplace=True)
                arb_df['charges_leg'] = 'L2'
                arb_df['charges'] = 'origin arbitrary charges'
                arb_df['at'] = 'origin'
                arb_df['region'] = region
                arb_df_concat = pd.concat([arb_df_concat, arb_df], axis=0, ignore_index=True)
            return arb_df_concat, list(set(region_list))

        def capture(self):
            arb_df, region_list = self.get_arb_table()
            self.captured_output = {'Arbitrary Charges': arb_df, 'regions': region_list}

        def clean(self):
            self.cleaned_output = self.captured_output

    class _Surcharge(BaseFix):

        def check_input(self):
            # check_errors = []
            # if not self.df[0].str.contains('TRADE LANE', na=False).any():
            #     check_errors.append("Trade Line cannot be found on the first column, the input file "
            #                         "structure is different from sample template")
            # if check_errors:
            #     raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_fixed_charges(self):
            fixed_charges = self.df

            return fixed_charges

        def capture(self):
            fixed_charges = self.get_fixed_charges()
            self.captured_output = {'fixed_charges': fixed_charges}

        def clean(self):
            self.cleaned_output = self.captured_output

    class _General_Information(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):
            index = list(self.df[(self.df[0].str.startswith("VERSION EFFECTIVE:", na=False))].index)
            index_svc = list(self.df[(self.df[0].str.contains("SERVICE CONTRACT/ESSENTIAL TERMS NO:", na=False))].index)
            contract_no = \
                re.findall(r"SERVICE CONTRACT\/ESSENTIAL TERMS NO:\s(.+?)$", self.df.iloc[index_svc, 0].values[0])[0]
            start_date = self.df.iloc[index, 1].values[0]
            start_date = datetime.strptime(start_date, '%B %d, %Y')
            self.captured_output = {'start_date': start_date, 'contract_no': contract_no}

        def clean(self):
            self.cleaned_output = self.captured_output

    @classmethod
    def resolve_dependency(cls, fix_outputs):
        freight_df = fix_outputs.pop('Rates')

        notes_df = fix_outputs.pop('Notes')

        arb_df = fix_outputs.pop('Inland')

        surcharge_df = fix_outputs.pop('Fixed Surcharges')

        arb_df_regions = arb_df['regions']
        arb_df = arb_df['Arbitrary Charges']

        notes_df_1 = notes_df['notes']

        regions_list = freight_df.pop('regions')

        freight_df = freight_df.pop('Freight')

        regions_list = regions_list + arb_df_regions

        inc_group_index = []
        for region in regions_list:
            if notes_df_1.loc[(notes_df_1[0].str.startswith(region, na=False))].index.any():
                inc_group_index.append(notes_df_1.loc[(notes_df_1[0].str.startswith(region, na=False))].index.values[0])

        inc_group_index = inc_group_index + list(notes_df_1.tail(1).index.values + 1)

        for index_inc in range(len(inc_group_index) - 1):
            notes_df = notes_df_1[inc_group_index[index_inc] + 1:inc_group_index[index_inc + 1]].copy(deep=True)
            notes_df.reset_index(drop=True, inplace=True)
            index = list(notes_df[(notes_df[1].str.contains('Rates are inclusive'))].index)
            note_dict = {}
            note_dict['inclusions'] = {}
            note_dict['subject_to'] = {}
            for index_ in index:
                note_id = notes_df.iloc[index_, 0]
                note_value = notes_df.iloc[index_, 1]

                regex_inc_sub = r"Rates are inclusive of (.+?). Rates are not inclusive of (.+?)$"
                matches_inc = re.finditer(regex_inc_sub, note_value, re.MULTILINE)
                for matchNum, match in enumerate(matches_inc, start=1):
                    inc_string = match.group(1)
                    subject_string = match.group(2)

                regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                included_list = []
                matches_inc = re.finditer(regex_incl, inc_string, re.MULTILINE)
                for matchNum, match in enumerate(matches_inc, start=1):
                    for groupNum in range(0, len(match.groups())):
                        groupNum = groupNum + 1
                        included_list.append(match.group(groupNum))
                included_list = ','.join(included_list)

                note_dict['inclusions'][note_id] = included_list

                subject_list = []
                matches_inc = re.finditer(regex_incl, subject_string, re.MULTILINE)
                for matchNum, match in enumerate(matches_inc, start=1):
                    for groupNum in range(0, len(match.groups())):
                        groupNum = groupNum + 1
                        subject_list.append(match.group(groupNum))
                subject_list = ','.join(subject_list)
                note_dict['subject_to'][note_id] = subject_list

            if 'inclusions' in note_dict:
                inclusions_df = pd.DataFrame.from_dict([note_dict['inclusions']])
                inclusions_df.reset_index(inplace=True, drop=True)

                inclusion_id = inclusions_df.columns.tolist()
                for id in inclusion_id:
                    freight_df.loc[(freight_df['remarks'].str.contains(id)) &
                                   (freight_df['region'] == (regions_list[index_inc])),
                                   'inclusions'] = inclusions_df[id].iloc[0]

            if 'subject_to' in note_dict:
                subject_to_df = pd.DataFrame.from_dict([note_dict['subject_to']])
                subject_to_df.reset_index(inplace=True, drop=True)

                subject_to_id = subject_to_df.columns.tolist()
                for id in subject_to_id:
                    freight_df.loc[(freight_df['remarks'].str.contains(id)) &
                                   (freight_df['region'] == (regions_list[index_inc])),
                                   'subject_to'] = subject_to_df[id].iloc[0]

            notes_df = notes_df.T
            notes_df.columns = notes_df.iloc[0]
            notes_df = notes_df[1:].copy()
            notes_df.reset_index(drop=True, inplace=True)

            notes_dict = notes_df.T.to_dict()[0]
            for code in notes_dict:
                freight_df.loc[(freight_df['remarks'].str.contains(code)) &
                               (freight_df['region'] == (regions_list[index_inc])),
                               'remarks'] = notes_dict[code]
                if 'remarks' in arb_df:
                    arb_df.loc[(arb_df['remarks'].str.contains(code)) & (arb_df['region'] == (regions_list[index_inc])),
                               'remarks'] = notes_dict[code]

        sur_charge = surcharge_df['fixed_charges']
        sur_index = list(sur_charge[(sur_charge[0].str.contains('For', na=False))].index)

        df = freight_df.copy(deep=True)
        df['destination_icd'] = df['destination_icd'].str.split(';')
        df = df.explode('destination_icd')
        try:
            for index in sur_index:
                ct_flag = 0
                tmp_df = pd.DataFrame()
                charge_dict = defaultdict(list)
                charge = sur_charge.iloc[index, 0]

                if 'any location except' in charge:
                    regex = r"originating from any location except\s((.+?),)(the|(.+?)\()(.+?)\)\sis fixed at USD (.+?)\s"

                    matches = re.finditer(regex, charge, re.MULTILINE)

                    for matchNum, match in enumerate(matches, start=1):
                        except_country = match.group(2)
                        charge_name = match.group(4)
                        amt = match.group(6)
                        except_country = re.sub(",$", '', except_country, 0, re.MULTILINE)
                        charge_dict['except'] = except_country
                        charge_name = charge_name.replace('the ', '')
                        charge_dict['charge_name'] = charge_name
                        charge_dict['amount'] = amt
                        charge_dict['currency'] = ''

                elif 'originating from' in charge:
                    regex = r"originating from\s((.+?),)(\s?the\s?|(.+?)\()(.+?)\((.+?)\)\sis fixed at USD\s(.+?)\s"

                    matches = re.finditer(regex, charge, re.MULTILINE)

                    for matchNum, match in enumerate(matches, start=1):
                        origin_country = match.group(1)
                        charge_name = match.group(5)
                        amt = match.group(7)
                        origin_country = re.sub(",$", '', origin_country, 0, re.MULTILINE)
                        charge_dict['origin'] = origin_country
                        charge_name = charge_name.replace('the ', '')
                        charge_dict['charge_name'] = charge_name
                        charge_dict['amount'] = amt
                        charge_dict['currency'] = ''

                elif 'cargo bound for' in charge:
                    regex = r"cargo bound for(.+?)(.+?) the (.+?)\((.+?)\)\sis\sfixed at (.+?)\s(.+?)\s"

                    matches = re.finditer(regex, charge, re.MULTILINE)

                    for matchNum, match in enumerate(matches, start=1):
                        origin_country = match.group(2)
                        charge_name = match.group(3)
                        currency = match.group(5)
                        amt = match.group(6)
                        origin_country = re.sub(",$", '', origin_country, 0, re.MULTILINE)
                        charge_dict['destination'] = origin_country
                        charge_name = charge_name.replace('the ', '')
                        charge_dict['charge_name'] = charge_name
                        charge_dict['amount'] = amt
                        charge_dict['currency'] = currency

                elif 'dangerous cargo' in charge:
                    regex = r"dangerous cargo,\s?(.+?)\s\((.+?)\)\sis\sfixed\sat\s(.+?)\s(.+?)\sper\sdry\s(.+?)\sft\scontainer\sor\s(.+?)\s(.+?)\sper\sdry\s(.+?)\s"

                    matches = re.finditer(regex, charge, re.MULTILINE)

                    for matchNum, match in enumerate(matches, start=1):
                        # origin_country = match.group(2)
                        charge_name = match.group(1)
                        currency = match.group(3)
                        amt_1 = match.group(4)
                        ct_1 = match.group(5)
                        amt_2 = match.group(7)
                        ct_2 = match.group(8)
                        charge_name = charge_name.replace('the ', '')
                        charge_dict['charge_name'] = charge_name
                        charge_dict[ct_1 + '_GP'] = amt_1
                        charge_dict[ct_2 + '_GP'] = amt_2
                        charge_dict['currency'] = currency
                        ct_flag = 1

                index = None
                if df['charges'].str.contains(charge_dict['charge_name']).any():
                    # index = df[df['charges'].str.contains(charge_dict['charge_name'])].index.tolist()
                    index_origin = []
                    index_except = []
                    index_destination = []
                    if 'origin' in charge_dict:
                        if ';' in charge_dict['origin']:
                            origin_country = charge_dict['origin'].split(';')
                            for country in origin_country:
                                index_origin_tmp = df.loc[(df['charges'].str.contains('basic ocean')) &
                                                          (df['origin_port'].str.contains(country))].index.tolist()
                                index_origin = list(set(sorted(index_origin + index_origin_tmp)))
                            tmp_df = df.loc[index_origin].copy(deep=True)
                            df.drop(index_origin, axis=0, inplace=True)
                        else:
                            index_origin_tmp = df.loc[(df['charges'].str.contains('basic ocean')) &
                                                      (df['origin_port'].str.contains(
                                                          charge_dict['origin']))].index.tolist()
                            index_origin = index_origin_tmp
                            tmp_df = df.loc[index_origin].copy(deep=True)
                            # df.drop(index_origin, axis=0, inplace=True)

                    elif 'except' in charge_dict:
                        if 'and' in charge_dict['except']:
                            origin_country = charge_dict['except'].replace(' and ', ';')
                            origin_country = origin_country.split(';')
                            for country in origin_country:
                                index_origin_tmp = df.loc[(df['charges'].str.contains(charge_dict['charge_name'])) &
                                                          ~(df['origin_port'].str.contains(country))].index.tolist()
                                if index_except:
                                    index_except = list(set(index_except) & set(index_origin_tmp))
                                else:
                                    index_except = index_origin_tmp
                            tmp_df = df.loc[index_except].copy(deep=True)
                            df.drop(index_except, axis=0, inplace=True)
                        else:
                            index_origin_tmp = df.loc[(df['charges'].str.contains(charge_dict['charge_name'])) &
                                                      ~(df['origin_port'].str.contains(
                                                          charge_dict['except']))].index.tolist()
                            index_except = list(set(sorted(index_except + index_origin_tmp)))
                            tmp_df = df.loc[index_except].copy(deep=True)
                            # df.drop(index_except, axis=0, inplace=True)

                    elif 'destination' in charge_dict:
                        if ';' in charge_dict['destination']:
                            origin_country = charge_dict['destination'].replace(' and ', ';')
                            origin_country = origin_country.split(';')
                            for country in origin_country:
                                index_origin_tmp = df.loc[(df['charges'].str.contains('basic ocean')) &
                                                          (df['destination_icd'].str.contains(country))].index.tolist()
                                index_destination = list(set(sorted(index_destination + index_origin_tmp)))
                            tmp_df = df.loc[index_destination].copy(deep=True)
                            df.drop(index_destination, axis=0, inplace=True)
                        else:
                            index_origin_tmp = df.loc[(df['charges'].str.contains('basic ocean')) &
                                                      (df['destination_icd'].str.contains(
                                                          charge_dict['destination']))].index.tolist()
                            index_destination = index_origin_tmp
                            tmp_df = df.loc[index_destination].copy(deep=True)
                            # df.drop(index_destination, axis=0, inplace=True)

                else:
                    index_origin = []
                    index_except = []
                    index_destination = []
                    if 'origin' in charge_dict:
                        if ';' in charge_dict['origin']:
                            origin_country = charge_dict['origin'].replace(' and ', ';')
                            origin_country = origin_country.split(';')
                            for country in origin_country:
                                index_origin_tmp = df.loc[(df['origin_port'].str.contains(country))].index.tolist()
                                index_except = list(set(sorted(index_except + index_origin_tmp)))
                            tmp_df = df.loc[index_origin].copy(deep=True)
                        else:
                            index_origin_tmp = df.loc[(df['charges'].str.contains(charge_dict['charge_name'])) &
                                                      (df['origin_port'].str.contains(
                                                          charge_dict['origin']))].index.tolist()
                            index_origin = list(set(sorted(index_origin + index_origin_tmp)))
                            tmp_df = df.loc[index_origin].copy(deep=True)

                    elif 'except' in charge_dict:
                        if ';' in charge_dict['except'] or 'and' in charge_dict['except']:
                            origin_country = charge_dict['except'].replace(' and ', ';')
                            origin_country = origin_country.split(';')
                            for country in origin_country:
                                index_origin_tmp = df.loc[~(df['origin_port'].str.contains(country))].index.tolist()
                                if index_except:
                                    index_except = list(set(index_except) & set(index_origin_tmp))
                                else:
                                    index_except = index_origin_tmp
                                # index_except = list(sorted(index_except + index_origin_tmp))
                            tmp_df = df.loc[index_except].copy(deep=True)
                        else:
                            index_origin_tmp = df.loc[(df['charges'].str.contains(charge_dict['charge_name'])) &
                                                      ~(df['origin_port'].str.contains(
                                                          charge_dict['except']))].index.tolist()
                            index_except = list(set(sorted(index_except + index_origin_tmp)))
                            tmp_df = df.loc[index_except].copy(deep=True)

                    elif 'destination' in charge_dict:
                        if ';' in charge_dict['destination']:
                            origin_country = charge_dict['destination'].replace(' and ', ';')
                            origin_country = origin_country.split(';')
                            for country in origin_country:
                                index_origin_tmp = df.loc[(df['charges'].str.contains('basic ocean')) &
                                                          (df['destination_icd'].str.contains(country))].index.tolist()
                                index_destination = list(set(sorted(index_destination + index_origin_tmp)))
                            tmp_df = df.loc[index_destination].copy(deep=True)
                        else:
                            index_origin_tmp = df.loc[(df['charges'].str.contains(charge_dict['charge_name'])) &
                                                      (df['destination_icd'].str.contains(
                                                          charge_dict['destination']))].index.tolist()
                            index_destination = list(set(sorted(index_destination + index_origin_tmp)))
                            tmp_df = df.loc[index_destination].copy(deep=True)
                    else:
                        ct_flag = 1
                        index_origin_tmp = df.loc[(df['charges'].str.contains('basic ocean freight'))].index.tolist()
                        tmp_df = df.loc[index_origin_tmp].copy(deep=True)

                if ct_flag == 1:
                    tmp_df['charges'] = charge_dict['charge_name']
                    tmp_df['20GP'] = charge_dict['20_GP']
                    tmp_df['40GP'] = charge_dict['40_GP']
                    tmp_df['40HC'] = ''
                    tmp_df['currency'] = charge_dict['currency']

                else:
                    tmp_df['charges'] = charge_dict['charge_name']
                    tmp_df['20GP'] = charge_dict['amount']
                    tmp_df['40GP'] = charge_dict['amount']
                    tmp_df['40HC'] = charge_dict['amount']
                    tmp_df['currency'] = charge_dict['currency']

                df.reset_index(drop=True, inplace=True)
                df = pd.concat([df, tmp_df], ignore_index=True)
        except:
            pass

        """adding start Date"""
        data = fix_outputs.pop('General Information')
        start_date = data['start_date']
        contract_no = data['contract_no']

        df['contract_no'] = contract_no
        arb_df['contract_no'] = contract_no

        """Contract No"""
        if 'start_date' in df:
            df.loc[df['start_date'].isna() | (df['start_date'] == ''), 'start_date'] = start_date.date()

        if 'start_date' not in df:
            df['start_date'] = start_date.date()

        if 'start_date' in arb_df:
            arb_df.loc[arb_df['start_date'].isna() | (arb_df['start_date'] == ''), 'start_date'] = start_date.date()

        if 'start_date' not in df:
            arb_df['start_date'] = start_date.date()

        """AMS change"""
        index_ams = df.loc[
            df['charges'].str.contains(r'Advance Manifest Security Charge') & df['inclusions'].str.contains(
                r'AMS')].index.to_list()
        df.drop(axis=0, index=index_ams, inplace=True)

        """Currency USD"""
        df.rename(columns={"destination_icd": "destination_port"}, inplace=True)
        df['currency'] = df['currency'].apply(lambda x: 'USD' if x == '' or x is nan else x)
        arb_df['currency'] = arb_df['currency'].apply(lambda x: 'USD' if x == '' else x)
        arb_df['start_date'] = pd.to_datetime(arb_df['start_date']).dt.date
        df['start_date'] = pd.to_datetime(df['start_date']).dt.date
        arb_df['expiry_date'] = pd.to_datetime(arb_df['expiry_date']).dt.date
        df['expiry_date'] = pd.to_datetime(df['expiry_date']).dt.date
        df['basis'] = 'PER CONTAINER'
        # fix_outputs = {'Rates': {'Freight': df, 'Arbitrary Charges': arb_df}}
        fix_outputs = [{'Freight': df, 'Arbitrary Charges': arb_df}]
        return fix_outputs


class OOCL_Far_east(BaseTemplate):
    class _Rates(BaseFix):

        def check_input(self):
            check_errors = []
            if not self.df[0].str.contains('Origin', na=False).any():
                check_errors.append("Origin column not present")
            if check_errors:
                raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_freight_table(self):
            """
            Get the table from the rates sheet
            Returns:
                freight table
            """
            index = list(self.df[(self.df[0] == ("Origin"))].index)
            region_index = list(self.df[(self.df[0].str.contains("Rates - ", na=False))].index[0:])

            region_dict = {}
            for i in range(len(region_index)):
                region_name = self.df.iloc[region_index[i], 0].split('-')[-1]
                region_dict[region_name] = self.df.iloc[region_index[i]:region_index[i + 1], :]
                if i + 2 == len(region_index):
                    region_name = self.df.iloc[region_index[i + 1], 0].split('-')[-1]
                    region_dict[region_name] = self.df.iloc[region_index[i + 1]:, :]
                    break

            comm_dict = {}
            comm_list = []
            for key, value in region_dict.items():
                df = region_dict[key]
                df.reset_index(drop=True, inplace=True)
                comm_index = list(df[(df[0].str.contains('Commodity'))].index)
                if len(comm_index) == 1:
                    comm_name = df.iloc[comm_index[0], 1]
                    df1 = df.iloc[comm_index[0] + 2:, :]
                    df1['commodity'] = comm_name
                    df1.commodity.iloc[0] = "commodity"
                    df1['region'] = key
                    df1.region.iloc[0] = "region"
                    df1.columns = df1.iloc[0, :]

                    df1 = df1.iloc[1:, :]
                    comm_list.append(df1)

                else:
                    for i in range(len(comm_index)):
                        comm_name = df.loc[comm_index[i], 1]
                        df1 = df.loc[comm_index[i] + 2:comm_index[i + 1] - 1, :]
                        df1['commodity'] = comm_name
                        df1['region'] = key
                        df1.commodity.iloc[0] = "commodity"
                        df1.region.iloc[0] = "region"
                        df1.columns = df1.iloc[0, :]
                        df1 = df1.iloc[1:, :]
                        comm_list.append(df1)

                        if i + 2 == len(comm_index):
                            comm_name = df.loc[comm_index[i + 1], 1]
                            df1 = df.loc[comm_index[i + 1] + 2:, :].copy()
                            df1['commodity'] = comm_name
                            df1['region'] = key
                            df1.commodity.iloc[0] = "commodity"
                            df1.region.iloc[0] = "region"
                            df1.columns = df1.iloc[0, :]
                            df1 = df1.iloc[1:, :]
                            comm_list.append(df1)
                            break
            # freight_df = self.df[region_index[0]:self.df.tail(1).index.values[0] + 1].copy(deep=True)
            freight_df = pd.concat(comm_list, ignore_index=True)
            # freight_df = freight_df.reset_index(drop=True)
            # region = self.df.iloc[region_index, 0]
            # region = region.replace('Rates - ', '')
            # region_indices = list(df[(df[0].str.contains('Rates -'))].index)

            """region_list=[]
            for element in region:
                region_list.append(element.split('-')[-1].strip())


            #commodity_df=self.df.iloc[commodity,1]

            freight_df = self.df[region_index[0]:self.df.tail(1).index.values[0] + 1].copy(deep=True)
            freight_df = freight_df.reset_index(drop=True)
            freight_df["commodity"] = ""


            commodity = freight_df[(freight_df[0].str.contains("Commodity"))].index

            commodity_list=[]
            for i in range(len(commodity)):
                holder = freight_df.iloc[commodity[i]:commodity[i] - 1, 1:]
                commodity_list.append(holder)
                if i + 6 == len(commodity):
                    holder = freight_df.iloc[commodity[i]:, 1:]
                    #holder_ = freight_df.iloc[commodity[i + 1] - 1, 1]
                    commodity_list.append(holder)
                    break
            commodity_= freight_df.iloc[commodity, 0]
            commodity_ = commodity_.replace('Commodity : ', '')
            commodity_list = []
            for i in commodity_:
                commodity_list.append(i.split('-')[-1].strip())

            freight_df["region"]=""
            for i in range(len(region_list)):
                freight_df["region"][region_index[i]:region_index[i+1]-1]=region_list[i]
                if i+2==len(region_list):
                    freight_df["region"][region_index[i] + 5:region_index[i + 1] - 1] = region_list[i]
                    break"""

            col = freight_df.columns.tolist()
            counter = 0
            note_column = ['Notes']
            for col_ in range(len(col)):
                if col[col_] == '':
                    counter += 1
                    col[col_] = "Notes" + str(counter)
                    note_column.append(col[col_])

            freight_df.columns = col
            freight_df['remarks'] = freight_df[note_column].apply(lambda x: ', '.join(x[x.notnull()]), axis=1)
            # if freight_df["remarks"]
            # freight_df["Notes"] = freight_df[note_column]
            # freight_df["Notes1"] = freight_df['']
            # freight_df['Notes'] =
            # freight_df['region'] = region
            freight_df['currency'] = 'USD'
            return freight_df

        @staticmethod
        def format_output(df_freight):
            output = {'Freight': df_freight}
            return output

        def capture(self):
            freight_df = self.get_freight_table()
            self.captured_output = self.format_output(freight_df)

        def clean(self):
            freight_df = self.captured_output['Freight']
            # freight_df["remarks"]=""
            column_names = {'Origin': 'origin_port', 'Destination': 'destination_icd', 'Mode': 'service_type',
                            20.0: '20GP', 40.0: '40GP', 45.0: "45HC", "remarks": "remarks",
                            'Expiry   MM/DD/YY': 'expiry_date',
                            "currency": "currency", 'Svc Loop': 'loop',
                            '40H': '40HC', 'Cargo Nature': 'cargonature', 'region': 'region',
                            'Via (Dest.)': 'destination_port', "commodity": "bulletin"}

            freight_df.rename(columns=column_names, inplace=True)
            if 'cargonature' not in freight_df:
                column_names = {'Origin': 'origin_port', 'Destination': 'destination_icd', 'Mode': 'service_type',
                                20.0: '20GP', 40.0: '40GP', '40H': '40HC', 45.0: "45HC", 'remarks': 'remarks',
                                'region': 'region'
                                }
                freight_df = freight_df[list(column_names.values())].copy(deep=True)
            else:
                freight_df = freight_df[list(column_names.values())].copy(deep=True)
            # if 'expiry_date' in freight_df:
            # freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date']).dt.strftime('%Y-%m-%d')
            freight_df['charges_leg'] = 'L3'
            freight_df['charges'] = 'basic ocean freight'
            self.cleaned_output = {'Freight': freight_df}
            return self.cleaned_output

    class _Notes(BaseFix):

        def check_input(self):
            # check_errors = []
            # if not self.df[0].str.contains('TRADE LANE', na=False).any():
            #     check_errors.append("Trade Line cannot be found on the first column, the input file "
            #                         "structure is different from sample template")
            # if check_errors:
            #     raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_freight_table(self):
            notes_df = self.df
            notes_df.replace('', nan, inplace=True)
            notes_df.dropna(axis=0, how='any', inplace=True)
            notes_df.reset_index(drop=True, inplace=True)
            index = list(notes_df[(notes_df[1].str.contains('Rates are inclusive of'))].index)
            note_dict = {}
            inclusion_dict = {}
            subject_to_dict = {}
            for index_ in index:
                note_id = notes_df.iloc[index_, 0]
                note_value = notes_df.iloc[index_, 1]

                regex_inc_sub = r"Rates are inclusive of (.+?)Freight Rates are not inclusive of(.+?)$"
                if re.search(regex_inc_sub, note_value, re.MULTILINE) is None:
                    regex_inc_sub = r"Rates are inclusive of the (.+?)\. Rates are not inclusive of all(.*)$"

                # if re.search(regex_inc_sub,note_value,re.MULTILINE) is None:
                # regex_inc_sub = r""

                # if regex_inc_sub not in
                matches_inc = re.finditer(regex_inc_sub, note_value, re.MULTILINE)
                for matchNum, match in enumerate(matches_inc, start=1):
                    inc_string = match.group(1)
                    subject_string = match.group(2)

                    inc_string = match.group(1)
                    subject_string = ''

                regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                included_list = []

                matches_inc = re.finditer(regex_incl, inc_string, re.MULTILINE)
                for matchNum, match in enumerate(matches_inc, start=1):
                    for groupNum in range(0, len(match.groups())):
                        groupNum = groupNum + 1
                        included_list.append(match.group(groupNum))

                included_list = ','.join(included_list)
                note_dict['inclusions'] = {note_id: included_list}
                inclusion_dict[note_id] = included_list
                subject_list = []
                matches_inc = re.finditer(regex_incl, subject_string, re.MULTILINE)
                for matchNum, match in enumerate(matches_inc, start=1):
                    for groupNum in range(0, len(match.groups())):
                        groupNum = groupNum + 1
                        subject_list.append(match.group(groupNum))
                subject_list = ','.join(subject_list)
                note_dict['subject_to'] = {note_id: subject_list}
                subject_to_dict[note_id] = subject_list

            inclusion_dict = {'inclusions': inclusion_dict}
            inclusions_df = pd.DataFrame.from_dict([inclusion_dict['inclusions']])
            inclusions_df.reset_index(inplace=True, drop=True)

            subject_to_dict = {'subject_to': subject_to_dict}
            subject_to_df = pd.DataFrame.from_dict([subject_to_dict['subject_to']])
            subject_to_df.reset_index(inplace=True, drop=True)
            notes_df = notes_df.T
            notes_df.columns = notes_df.iloc[0]
            notes_df = notes_df[1:].copy()
            notes_df.reset_index(drop=True, inplace=True)
            self.captured_output = notes_df
            return notes_df, inclusions_df, subject_to_df

        def capture(self):
            notes_df, inclusions_df, subject_to_df = self.get_freight_table()
            self.captured_output = {'notes': notes_df, 'inclusions': inclusions_df, 'subject to': subject_to_df}

        def clean(self):

            self.cleaned_output = self.captured_output

    class _Arb(BaseFix):

        def check_input(self):
            # check_errors = []
            # if not self.df[0].str.contains('TRADE LANE', na=False).any():
            #     check_errors.append("Trade Line cannot be found on the first column, the input file "
            #                         "structure is different from sample template")
            # if check_errors:
            #     raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_arb_table(self):
            index = list(self.df[(self.df[0] == ("Location"))].index)

            """
            Hard coded 6 to find the region
            TODO : Implememnt a better solution
            """

            index_ = [x - 3 for x in index]
            arb_df_concat = pd.DataFrame()
            index = index + list(self.df.tail(1).index.values + 3)
            for index_iter in range(len(index) - 1):
                region = self.df.iloc[index_[index_iter], 0]
                arb_df = self.df[index[index_iter]: index[index_iter + 1] - 3].copy(deep=True)

                arb_df.columns = arb_df.iloc[0]
                arb_df = arb_df[1:].copy()
                arb_df.rename(
                    columns={20.0: "20GP", 40.0: '40GP', "40H": "40HC", 'Location': 'icd', 'Over': 'to',
                             'Via': 'via', 45.0: "45HC", 'Svc Loop': 'loop', 'Notes': 'Remarks',
                             'Mode': 'service_type', 'TransportType': 'mode_of_transportation', 'currency': "currency",
                             'Effective MM/DD/YY': 'start_date', 'Expiry MM/DD/YY': 'expiry_date'},
                    inplace=True)

                arb_df['currency'] = 'USD'
                arb_df['charges_leg'] = 'L2'
                arb_df['charges'] = 'origin arbitrary charges'
                arb_df['at'] = 'origin'
                arb_df['region'] = region
                arb_df_concat = pd.concat([arb_df_concat, arb_df], axis=0, ignore_index=True)
            return arb_df_concat

        def capture(self):
            arb_df = self.get_arb_table()
            self.captured_output = {'Arbitrary Charges': arb_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    class _Surcharge(BaseFix):

        def check_input(self):
            # check_errors = []
            # if not self.df[0].str.contains('TRADE LANE', na=False).any():
            #     check_errors.append("Trade Line cannot be found on the first column, the input file "
            #                         "structure is different from sample template")
            # if check_errors:
            #     raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_fixed_charges(self):
            fixed_charges = self.df

            return fixed_charges

        def capture(self):
            fixed_charges = self.get_fixed_charges()
            self.captured_output = {'fixed_charges': fixed_charges}

        def clean(self):
            self.cleaned_output = self.captured_output

    class _General_Information(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):
            index = list(self.df[(self.df[0].str.startswith("COMMENCEMENT:", na=False))].index)
            start_date = self.df.iloc[index, 1].values[0]
            start_date = datetime.strptime(start_date, '%B %d, %Y')
            index = list(self.df[(self.df[0].str.startswith("TERMINATION:", na=False))].index)
            expiry_date = self.df.iloc[index, 1].values[0]
            expiry_date = datetime.strptime(expiry_date, '%B %d, %Y')

            geo_code = {}
            if self.df[0].str.contains('GEOGRAPHIC TERMS').any():
                geo_code_index = self.df[(self.df[0].str.contains('GEOGRAPHIC TERMS'))].index.values[0]
                geo_code_df = self.df.loc[int(geo_code_index) + 1:, :]
                geo_code_df = geo_code_df.reset_index(drop=True)
                for i in range(len(geo_code_df[0])):
                    geo_code_df[0][i] = geo_code_df[0][i].split("-")[0].strip()
                geo_code = geo_code_df.set_index(geo_code_df[0]).to_dict()[1]
            com_index = self.df.iloc[10:18, 0]
            com_dict = {}
            for i in range(len(com_index)):
                if i % 2 == 0:
                    com_dict[com_index.iloc[i]] = com_index.iloc[i + 1]
                    if i + 1 == len(com_index):
                        break

            self.captured_output = {'geo_code': geo_code, 'comm_desc': com_dict, 'start_date': start_date,
                                    "expiry_date": expiry_date}

        def clean(self):
            self.cleaned_output = self.captured_output

    @classmethod
    def resolve_dependency(cls, fix_outputs):
        freight_df = fix_outputs.pop('Rates')['Freight']

        notes_df = fix_outputs.pop('Notes')

        arb_df = fix_outputs.pop('Inland')

        # surcharge_df = fix_outputs.pop('Fixed Surcharges')

        data = fix_outputs.pop('General Information')

        start_date = data['start_date']

        if 'start_date' in freight_df:
            freight_df.loc[
                freight_df['start_date'].isna() | (freight_df['start_date'] == ''), 'start_date'] = start_date.date()

        if 'start_date' not in freight_df:
            freight_df['start_date'] = start_date.date()

        """adding start Date"""
        expiry_date = data['expiry_date']
        if 'expiry_date' in freight_df:
            freight_df.loc[freight_df['expiry_date'].isna() | (
                    freight_df['expiry_date'] == ''), 'expiry_date'] = expiry_date.date()

        if 'expiry_date' not in freight_df:
            freight_df['expiry_date'] = expiry_date.date()

        freight_df['start_date'] = pd.to_datetime(freight_df['start_date'])
        geo_code_df_ = data['geo_code']
        comm_desc_dict = data['comm_desc']

        freight_df = freight_df.reset_index(drop=True)
        freight_df['commodity'] = ""
        freight_df['commodity'] = freight_df['bulletin']
        for commd_ in comm_desc_dict:
            _commd_descn = (comm_desc_dict[commd_])
            freight_df['commodity'].replace(commd_, _commd_descn, inplace=True)

        for code in geo_code_df_:
            _code = geo_code_df_[code]
            freight_df.replace(code, _code, inplace=True, regex=True)
        arb_df = arb_df["Arbitrary Charges"].reset_index(drop=True)

        if 'start_date' in arb_df:
            arb_df.loc[
                arb_df['start_date'].isna() | (arb_df['start_date'] == ''), 'start_date'] = start_date.date()

        if 'start_date' not in arb_df:
            arb_df['start_date'] = start_date.date()

        """adding start Date"""
        expiry_date = data['expiry_date']
        if 'expiry_date' in arb_df:
            arb_df.loc[arb_df['expiry_date'].isna() | (
                    arb_df['expiry_date'] == ''), 'expiry_date'] = expiry_date.date()

        if 'expiry_date' not in arb_df:
            arb_df['expiry_date'] = expiry_date.date()

        arb_df['start_date'] = pd.to_datetime(arb_df['start_date'])

        for geo in geo_code_df_:
            _geo = geo_code_df_[geo]
            arb_df.replace(geo, _geo, inplace=True, regex=True)

        inclusions = notes_df['inclusions']
        subject_to = notes_df['subject to']

        notes_df = notes_df['notes']

        notes_dict = notes_df.T.to_dict()[0]

        inclusion_id = inclusions.columns.tolist()
        for i in range(len(freight_df['remarks'])):
            freight_df['remarks'][i] = freight_df['remarks'][i].strip(',')

        for id in inclusion_id:
            freight_df.loc[(freight_df['remarks'].str.contains(id, regex=True, na=False)), 'inclusions'] = \
                inclusions[id].iloc[0]

        subject_to_id = subject_to.columns.tolist()
        for id in subject_to_id:
            freight_df.loc[(freight_df['remarks'].str.contains(id, regex=True, na=False)), 'subject_to'] = \
                subject_to[id].iloc[0]

        for code in notes_dict:
            _code = (notes_dict[code])
            freight_df['remarks'].replace(code, _code, inplace=True, regex=True)

        for org in notes_dict:
            _org = (notes_dict[org])
            arb_df.replace(org, _org, inplace=True, regex=True)

        df = freight_df.copy(deep=True)

        df['origin_port'] = df['origin_port'].str.split(';')
        df = df.explode('origin_port')
        df = df.explode('destination_icd')
        df.reset_index(drop=True, inplace=True)

        fix_outputs = {'Rates': {'Freight': df, 'Arbitrary Charges': arb_df}}

        return fix_outputs
