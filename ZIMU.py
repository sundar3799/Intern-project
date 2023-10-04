from logging import getLogger
from base import BaseTemplate, BaseFix, BaseDocxFix
from custom_exceptions import InputValidationError
import pandas as pd
import re
import warnings
from numpy import nan
from bs4 import BeautifulSoup
from dateutil.parser import parse
from datetime import datetime

warnings.simplefilter(action='ignore', category=FutureWarning)

log = getLogger(__name__)


class ZIM_AP(BaseTemplate):
    class _ZIM_v1(BaseFix):

        def check_input(self):
            check_errors = []
            if not self.df[0].str.contains('TRADE LANE', na=False).any():
                check_errors.append("Trade Line cannot be found on the first column, the input file "
                                    "structure is different from sample template")
            if check_errors:
                raise InputValidationError(check_errors)

        def check_output(self):
            pass

        def get_freight_table(self):
            index = list(self.df[(self.df[0].str.contains("TRADE LANE", na=False))].index)
            freight_df = self.df[index[0]:self.df.tail(1).index.values[0] + 1].copy(deep=True)
            freight_df.columns = freight_df.iloc[0]
            freight_df = freight_df[1:].copy()
            return freight_df

        def haz_imo_fix(self, freight_df):

            """Index capturing"""
            index_haz = freight_df[[freight_df['HAZ?'] == 'Y'] and freight_df['INCLUSIVES'].str.contains(
                "IMS", na=False)].index.to_list()
            index_haz_sur_ch = freight_df[[freight_df['HAZ?'] == 'Y'] and freight_df['FIXED'].str.contains(
                "IMS", na=False)].index.to_list()
            index_nhaz = freight_df.loc[freight_df['HAZ?'] == 'N'].index.to_list()

            """Haz"""
            freight_df.loc[index_haz, 'hazardous_mode'] = 'Hazardous only'
            freight_df.loc[index_haz, 'imo_class'] = \
                freight_df.loc[index_haz]["NOTES"].str.extract(r"IMO CLASS.:\s?(.+?)\s?")[0]

            """Haz with surcharge"""

            freight_df.loc[index_haz_sur_ch, 'hazardous_mode'] = 'Hazardous with surcharges only'
            freight_df.loc[index_haz_sur_ch, 'imo_class'] = 'All'

            """Non Haz"""

            freight_df.loc[index_nhaz, 'hazardous_mode'] = 'Nonhazardous only'

            return freight_df

        def expand_fixed_charges(self, freight_df):
            freight_df = freight_df.replace(nan, '')
            charges = "".join(freight_df['FIXED'].unique().tolist())
            regex = r"([A-Z]{3})|([A-Z]{1}\/[A-Z]{3})"
            matches = re.finditer(regex, charges, re.MULTILINE)
            charges_list = []
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    if match.group(groupNum):
                        charges_list.append(match.group(groupNum))
            charges_list = list(set(charges_list))

            def str_replace(regex, subst):
                freight_df['FIXED'] = freight_df['FIXED'].str.replace(regex, subst)
                return freight_df

            freight_df = str_replace(r"C\/", "/")

            freight_df = str_replace(r"C$", "")

            freight_df = str_replace(r";$", "")

            table = freight_df['FIXED'].str.split('/', expand=True)
            columns_ = table.columns.tolist()
            df = pd.DataFrame()
            for ch in charges_list:
                col_1 = 1
                for col in columns_:
                    ch_table = table.loc[(table[col].str.contains(ch, na=False))][col]
                    ch_table = ch_table.str.split(',', expand=True)
                    if ch_table.empty != 1:
                        regex = ch
                        ch_table[0] = ch_table[0].str.replace(regex, '', regex=True)
                        ch_table[0] = ch_table[0].str.replace('/', '', regex=True)
                        regex = r"[\([{})\]]"
                        ch_table[0] = ch_table[0].str.replace(regex, '', regex=True)
                        ch_table[3] = ch_table[3].str.replace(regex, '', regex=True)
                        ch_table.columns = ['20GP_' + ch + '__' + str(col_1), '40GP_' + ch + '__' + str(col_1),
                                            '40HC_' + ch + '__' + str(col_1), '45HC_' + ch + '__' + str(col_1)]
                        col_1 += 1
                        df = df.join(ch_table, how='outer', rsuffix='y', lsuffix='x')

            for col in df.columns.tolist():
                for i in range(1, len(df.columns.tolist())):
                    i += 1
                    if col.find('__' + str(i)) != -1:
                        col.replace('__\d', '')
                        col_up = re.sub(r"__\d", "", col)
                        df[col_up + '__1'].fillna(df[col], inplace=True)
                        df.drop(col, inplace=True, axis=1)
            col_name = df.columns.tolist()
            res = [sub.replace('__1', '') for sub in col_name]
            df.columns = res
            freight_df = pd.concat([freight_df, df], axis=1)

            return freight_df, res

        @staticmethod
        def fix_inclusions(freight_df):
            regex = r"\/"
            subst = ","
            freight_df['INCLUSIVES'] = freight_df['INCLUSIVES'].str.replace(regex, subst)
            return freight_df

        @staticmethod
        def reffer_charges(freight_df, charges):
            charge_name = [sub.replace('GP_', 'RE_') for sub in charges]
            charge_name = [sub.replace('40HC_', '40HR_') for sub in charge_name]
            charge_name = list(filter(lambda x: re.search('45HC', x) is None, charge_name))
            charges = list(filter(lambda x: re.search('45HC', x) is None, charges))
            res = dict(zip(charges, charge_name))
            refer_load_types = {'20GP': '20RE', '40GP': '40RE', '40HC': '40HR'}
            refer_load_types.update(res)

            for load_type in refer_load_types:
                freight_df.loc[(freight_df['CNTR TYPE'] == 'RF'), refer_load_types[load_type]] = freight_df[load_type]
                freight_df.loc[(freight_df['CNTR TYPE'] == 'RF'), load_type] = ''
            return freight_df

        @staticmethod
        def format_output(df_freight):
            output = {'Freight': df_freight}
            return output

        def capture(self):

            freight_df = self.get_freight_table()
            freight_df = self.haz_imo_fix(freight_df)
            freight_df, self.charges = self.expand_fixed_charges(freight_df)
            freight_df = self.fix_inclusions(freight_df)
            self.captured_output = self.format_output(freight_df)

        @staticmethod
        def commodity_fix(freight_df):
            freight_df.loc[freight_df['commodity'] == '', 'commodity'] = \
                freight_df.loc[freight_df['commodity'] == '']['COMMODITY SET']

            # comm_desc = {
            #     "9999.99.0000": "Freight all kind",
            #     "8500.00.0000": "OTHER NON-FAK GOODS",
            #     "GARMENTGROUP": "OTHER NON-FAK GOODS"
            # }
            # freight_df.replace(comm_desc, regex=True, inplace=True)

            index_haz_sur_ch = freight_df.loc[~(freight_df['hazardous_mode'] == 'Nonhazardous only')].index.to_list()

            freight_df.loc[index_haz_sur_ch, 'commodity'] = "DG-" + freight_df.loc[index_haz_sur_ch, 'commodity']

            return freight_df

        def clean(self):
            freight_df = self.captured_output['Freight']

            def str_replace(regex, subst, col):
                freight_df[col] = freight_df[col].str.replace(regex, subst)
                return freight_df

            col_rename = ['ORIGIN', 'DESTINATION', 'LIP', 'DIP']

            for col in col_rename:
                freight_df = str_replace(r"\/", ";", col)

            freight_df.rename(
                columns={"COMMODITY": 'commodity', "ORIGIN": 'origin_icd', "LIP": 'origin_port', "DESTINATION":
                    "destination_icd", "DIP": "destination_port",
                         'EXPORT/IMPORT SERVICE': 'mode_of_transportation', 20.0: "20GP",
                         40.0: '40GP', '40B': '40HC', 45.0: "45HC",
                         "CURRENCY": 'currency', "SVC": 'loop', "INCLUSIVES": 'inclusions',
                         "EFFECTIVE DATE": 'start_date',
                         'EXPIRATION DATE': 'expiry_date', 'NOTES': 'remarks', "AMENDMENT #": "Amendment no.",
                         "NAMED ACCOUNTS CUCC": "customer_name"
                         }, inplace=True)
            freight_df.loc[freight_df[
                               "customer_name"] == "USLAXZEXFA - ZEX FAST SERVICE", "premium_service"] = "ecommerce_xpress_-_zx2_uslaxzexfa_-_zex_fast_service"
            freight_df.loc[freight_df[
                               "customer_name"] == "USTACFAST - ZEX TACOMA FAST SERVICE", "premium_service"] = "ustacfast_-_zex_fast_service"

            freight_df.loc[freight_df["customer_name"].str.contains(
                "USLAXZEXFA - ZEX FAST SERVICE", na=False), "premium_service"] = \
                "ecommerce_xpress_-_zx2_uslaxzexfa_-_zex_fast_service"

            freight_df.loc[freight_df["customer_name"].str.contains(
                "USTACFAST - ZEX TACOMA FAST SERVICE", na=False), "premium_service"] = \
                r"ustacfast_-_zex_fast_service"

            freight_df['region'] = freight_df['customer_name']

            freight_df = self.commodity_fix(freight_df)

            """
                Changing service loop to ZEX
            """
            index_fast_service = freight_df.loc[freight_df['customer_name'].str.contains('FAST SERVICE',
                                                                                         na=False)].index.tolist()
            freight_df.loc[index_fast_service, 'customer_name'] = ''

            freight_df.loc[index_fast_service, 'loop'] = 'ZEX; ZX2'

            freight_df = self.reffer_charges(freight_df, self.charges)

            freight_df.drop(['TRADE LANE', 'COMMODITY SET', 'ORIGIN PORT CODE', 'ORIGIN PORT GROUP',
                             'DESTINATION PORT CODE', 'DESTINATION PORT GROUP', 'THRU_RATE', 'SHIPPER OWNED CNTR',
                             'FIXED', 'FUTURE_SURCHARGES', 'OUT OF GAUGE?', 'HAZ?', 'FLEXITANK',
                             'NAMED ACCOUNTS GROUP', 'REEFER-NON OPERATING',
                             'STATUS', 'CNTR TYPE', 'SPECIAL SERVICE'], axis=1, inplace=True)

            freight_df = freight_df.loc[(freight_df['origin_icd'] != '') | (freight_df['origin_port'] != '') |
                                        (freight_df['destination_port'] != '') | (
                                                freight_df['destination_icd'] != '')].copy(deep=True)

            freight_df['start_date'] = pd.to_datetime(freight_df['start_date']).dt.strftime('%Y-%m-%d')
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date']).dt.strftime('%Y-%m-%d')
            freight_df['charges'] = 'Basic Ocean Freight'

            freight_df['inclusions'].replace('BAF,', '', regex=True, inplace=True)

            """AMD no is from user"""
            freight_df['Amendment no.'] = ''

            """
                Changing the charges based on the origin and destination
            """
            destination_arb_index = freight_df.loc[
                (freight_df['origin_icd'] == '') & (freight_df['origin_port'] == '')].index.tolist()

            origin_arb_index = freight_df.loc[
                (freight_df['destination_icd'] == '') & (freight_df['destination_port'] == '')].index.tolist()

            destination_arb_df = pd.DataFrame()
            origin_arb_df = pd.DataFrame()
            if destination_arb_index:
                destination_arb_df = freight_df.loc[destination_arb_index].copy(deep=True)
                freight_df.drop(destination_arb_index, axis=0, inplace=True)
                destination_arb_df['charges'] = 'destination arbitrary charge'
                destination_arb_df.drop(columns=['origin_icd', 'origin_port'], inplace=True)
                destination_arb_df['at'] = 'destination'
                destination_arb_df = destination_arb_df.rename(
                    columns={'destination_icd': 'icd', 'destination_port': 'to',
                             'mode_of_transportation': 'mode_of_transportation_destination'})

            if origin_arb_index:
                origin_arb_df = freight_df.loc[origin_arb_index].copy(deep=True)
                freight_df.drop(origin_arb_index, axis=0, inplace=True)
                origin_arb_df['charges'] = 'origin arbitrary charge'
                origin_arb_df.drop(columns=['destination_icd', 'destination_port'], inplace=True)
                origin_arb_df['at'] = 'origin'
                origin_arb_df = origin_arb_df.rename(
                    columns={'origin_icd': 'to', 'origin_port': 'icd',
                             'mode_of_transportation': 'mode_of_transportation_origin'})

            arb_df = pd.concat([origin_arb_df, destination_arb_df], axis=0, ignore_index=True)

            if arb_df.empty:
                self.cleaned_output = {'Freight': freight_df}
            else:
                self.cleaned_output = {'Freight': freight_df, 'Arbitrary Charges': arb_df}


class Flexport_ZIM_v1(ZIM_AP):
    class _ZIM_v1(ZIM_AP._ZIM_v1):
        pass


class Expedoc_ZIM_v1(ZIM_AP):
    class _ZIM_v1(ZIM_AP._ZIM_v1):

        def get_freight_table(self):
            freight_df = super().get_freight_table()
            freight_df = freight_df.loc[~(freight_df['STATUS'].str.contains('DELETE'))]
            return freight_df

        def haz_imo_fix(self, freight_df):
            """ Overrriding this to return freight_df as it is as not sure if
            HAZ-IMO is needed for expedoc or not"""
            return freight_df

        def clean(self):
            freight_df = self.captured_output['Freight']

            def str_replace(regex, subst, col):
                freight_df[col] = freight_df[col].str.replace(regex, subst)
                return freight_df

            col_rename = ['ORIGIN', 'DESTINATION', 'LIP', 'DIP']

            for col in col_rename:
                freight_df = str_replace(r"\/", ";", col)

            freight_df.rename(
                columns={"COMMODITY SET": 'commodity', "ORIGIN": 'origin_icd', "LIP": 'origin_port', "DESTINATION":
                    "destination_icd", "DIP": "destination_port",
                         'EXPORT/IMPORT SERVICE': 'mode_of_transportation', 20.0: "20GP",
                         40.0: '40GP', '40B': '40HC', 45.0: "45HC",
                         "CURRENCY": 'currency', "SVC": 'loop', "INCLUSIVES": 'inclusions',
                         "EFFECTIVE DATE": 'start_date',
                         'EXPIRATION DATE': 'expiry_date', 'NOTES': 'remarks', "AMENDMENT #": "Amendment no.",
                         "NAMED ACCOUNTS CUCC": "customer_name"
                         }, inplace=True)

            freight_df = self.reffer_charges(freight_df, self.charges)

            freight_df.drop(['TRADE LANE', 'COMMODITY', 'ORIGIN PORT CODE', 'ORIGIN PORT GROUP',
                             'DESTINATION PORT CODE', 'DESTINATION PORT GROUP', 'THRU_RATE', 'SHIPPER OWNED CNTR',
                             'FIXED', 'FUTURE_SURCHARGES', 'OUT OF GAUGE?', 'HAZ?', 'FLEXITANK',
                             'NAMED ACCOUNTS GROUP', 'REEFER-NON OPERATING',
                             'STATUS', 'CNTR TYPE'], axis=1, inplace=True)

            freight_df['start_date'] = pd.to_datetime(freight_df['start_date']).dt.strftime('%Y-%m-%d')
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date']).dt.strftime('%Y-%m-%d')
            freight_df['charges_leg'] = 'L3'
            freight_df['charges'] = 'Basic Ocean Freight'
            self.cleaned_output = {'Freight': freight_df}


class Expedoc_ZIM_Word(BaseTemplate):
    class ZIM_Excel_fixed_am_fc(Expedoc_ZIM_v1._ZIM_v1):

        def melt_load_type(self, df):
            if "load_type" not in df:
                df = df.melt(id_vars=[column for column in df.columns if not column[1].isdigit()],
                             var_name="load_type",
                             value_name="amount")
                return df

        def freight_df_arb(self, df):

            df.reset_index(drop=True, inplace=True)
            df.loc[(df['origin_port'].isna()) & (df['origin_icd'].isna()), ['charges', 'icd', 'to', 'at']] \
                = ['destination arbitrary charges', df['destination_icd'], df['destination_port'], 'destination']
            df.loc[(df['destination_port'].isna()) & (df['destination_icd'].isna()), ['charges', 'icd', 'to', 'at']] \
                = ['origin arbitrary charges', df['origin_icd'], df['origin_port'], 'origin']
            df.drop(['commodity', 'ORIGIN PORT CODE', 'ORIGIN PORT GROUP', 'origin_port', 'origin_icd',
                     'DESTINATION PORT CODE', 'DESTINATION PORT GROUP', 'THRU_RATE', 'SHIPPER OWNED CNTR',
                     'FIXED', 'FUTURE_SURCHARGES', 'OUT OF GAUGE?', 'HAZ?', 'FLEXITANK ', 'destination_port',
                     'destination_icd', 'NAMED ACCOUNTS GROUP', 'REEFER-NON OPERATING', 'CNTR TYPE',
                     'mode_of_transportation'], axis=1, inplace=True)

            return df

        def common_routine(self, df):

            if 'start_date' in df:
                df['start_date'] = pd.to_datetime(df['start_date']).dt.strftime('%Y-%m-%d')
            df['expiry_date'] = pd.to_datetime(df['expiry_date']).dt.strftime('%Y-%m-%d')
            if 'load_type' not in df:
                df = self.melt_load_type(df)
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            df = df.replace('', nan)
            df = df.loc[df.amount.notna()]
            load = df['load_type'].str.split('_', expand=True)

            if 'to' not in df.columns:
                load[1].fillna('Basic Ocean Freight', inplace=True)
                df['load_type'], df['charges'] = load[0], load[1]
                cols = {'COMMODITY': 'commodity', 'SUBJECT_TO': 'subject_to', 'Amendment no.': 'amendment_no',
                        'customer_name': 'named_account'}
                df.rename(columns=cols, inplace=True)
            else:
                if 'mode_of_transportation_origin' in df.columns or 'mode_of_transportation_destination' in df.columns:
                    if 'mode_of_transportation' in df.columns:
                        df.drop(['mode_of_transportation'], axis=1, inplace=True)
                df['load_type'] = load[0]

            return df

        def capture(self, df):

            if df[0].str.contains('S/C:').any():
                sc_number_index = df[(df[0].str.contains('S/C:', na=False))].index.values[0]
                contract_id = df.loc[int(sc_number_index)][0].split(': ')[-1]
            freight_df = self.get_freight_table(df)
            freight_df, self.charges = self.expand_fixed_charges(freight_df)
            freight_df = self.fix_inclusions(freight_df)
            freight_df['contract_id'] = contract_id
            self.captured_output = self.format_output(freight_df)

        def clean(self):
            freight_df = self.captured_output['Freight']

            def str_replace(regex, subst, col):
                freight_df[col] = freight_df[col].str.replace(regex, subst)
                return freight_df

            col_rename = ['ORIGIN', 'DESTINATION', 'LIP', 'DIP']

            for col in col_rename:
                freight_df = str_replace(r"\/", ";", col)

            freight_df['origin_port'], freight_df['destination_port'], freight_df['origin_icd'] \
                , freight_df['destination_icd'] = '', '', '', ''

            freight_df = freight_df.reset_index(drop=True)

            # freight_df.replace('', nan, inplace=True)

            for i in range(freight_df.shape[0]):

                if freight_df['LIP'][i] != '':

                    freight_df['origin_port'][i] = freight_df['LIP'][i]
                    freight_df['origin_icd'][i] = freight_df['ORIGIN'][i]

                else:

                    freight_df['origin_port'][i] = freight_df['ORIGIN'][i]

                if freight_df['DIP'][i] != '':

                    freight_df['destination_port'][i] = freight_df['DIP'][i]
                    freight_df['destination_icd'][i] = freight_df['DESTINATION'][i]

                else:

                    freight_df['destination_port'][i] = freight_df['DESTINATION'][i]
                    pass

            freight_df.drop(['ORIGIN', 'LIP', 'DESTINATION', 'DIP'], axis=1, inplace=True)
            mode_of_transport = freight_df['EXPORT/IMPORT SERVICE'].str.split('/', expand=True).fillna('')
            freight_df['mode_of_transportation_origin'] = mode_of_transport[0]
            freight_df['mode_of_transportation_destination'] = mode_of_transport[1]
            freight_df.rename(
                columns={"COMMODITY SET": 'commodity',
                         'EXPORT/IMPORT SERVICE': 'mode_of_transportation', 20.0: "20GP",
                         40.0: '40GP', '40B': '40HC', 45.0: "45HC",
                         "CURRENCY": 'currency', "SVC": 'loop', "INCLUSIVES": 'inclusions',
                         "EFFECTIVE DATE": 'start_date',
                         'EXPIRATION DATE': 'expiry_date', 'NOTES': 'remarks', "AMENDMENT #": "Amendment no.",
                         "NAMED ACCOUNTS CUCC": "customer_name"
                         }, inplace=True)

            freight_df = self.reffer_charges(freight_df, self.charges)
            freight_df.replace('', nan, inplace=True)

            destination_arb_index = freight_df.loc[
                (freight_df['origin_icd'].isna()) & (freight_df['origin_port'].isna())].index.tolist()

            origin_arb_index = freight_df.loc[
                (freight_df['destination_icd'].isna()) & (freight_df['destination_port'].isna())].index.tolist()

            destination_arb_df = pd.DataFrame()
            origin_arb_df = pd.DataFrame()
            if destination_arb_index:
                destination_arb_df = freight_df.loc[destination_arb_index].copy(deep=True)
                freight_df.drop(destination_arb_index, axis=0, inplace=True)
                destination_arb_df['charges'] = 'destination arbitrary charge'
                destination_arb_df.drop(columns=['origin_icd', 'origin_port'], inplace=True)
                destination_arb_df['at'] = 'destination'
                destination_arb_df = destination_arb_df.rename(
                    columns={'destination_icd': 'icd', 'destination_port': 'to'})

            if origin_arb_index:
                origin_arb_df = freight_df.loc[origin_arb_index].copy(deep=True)
                freight_df.drop(origin_arb_index, axis=0, inplace=True)
                origin_arb_df['charges'] = 'origin arbitrary charge'
                origin_arb_df.drop(columns=['destination_icd', 'destination_port'], inplace=True)
                origin_arb_df['at'] = 'origin'
                origin_arb_df = origin_arb_df.rename(
                    columns={'origin_icd': 'to', 'origin_port': 'icd'})

            freight_arb = pd.concat([origin_arb_df, destination_arb_df], axis=0, ignore_index=True)
            freight_arb.dropna(how='all', axis=1, inplace=True)
            freight_arb = self.melt_load_type(freight_arb)

            freight_df.drop(['commodity', 'ORIGIN PORT CODE', 'ORIGIN PORT GROUP',
                             'DESTINATION PORT CODE', 'DESTINATION PORT GROUP', 'THRU_RATE', 'SHIPPER OWNED CNTR',
                             'FIXED', 'FUTURE_SURCHARGES', 'OUT OF GAUGE?', 'HAZ?', 'FLEXITANK ',
                             'NAMED ACCOUNTS GROUP', 'REEFER-NON OPERATING',
                             'CNTR TYPE', 'mode_of_transportation'], axis=1, inplace=True)

            freight_df['charges_leg'] = 'L3'
            freight_df['charges'] = 'Basic Ocean Freight'

            freight_df = self.common_routine(freight_df)

            if not freight_arb.empty:
                # freight_arb = self.freight_df_arb(freight_arb)
                freight_arb = self.common_routine(freight_arb)
                freight_arb.rename(columns={'COMMODITY': 'commodity'}, inplace=True)
                freight_arb.drop(['DESTINATION PORT CODE', 'SHIPPER OWNED CNTR',
                                  'CNTR TYPE', 'HAZ?'], axis=1, inplace=True)

                freight_arb['icd'] = freight_arb['icd'].str.replace('/', ';')
                freight_arb['to'] = freight_arb['to'].str.replace('/', ';')
                if 'via' in freight_arb.columns:
                    freight_arb['via'] = freight_arb['via'].str.replace('/', ';')
                self.cleaned_output = {'Freight': freight_df, 'Arbitrary': freight_arb}
            else:
                self.cleaned_output = {'Freight': freight_df}
            return self.cleaned_output

    class Origin_Arbitrary(BaseFix):

        def check_input(self):

            pass

        def check_output(self):

            pass

        def different_arb(self, df, origins_index):

            if df.iloc[:, -1].str.contains('effective from').any():
                effective_date_index = df[(df.iloc[:, -1].str.contains('effective from', na=False))].index.values[0]
                effective_date = df.iloc[effective_date_index, -1].split()[-1]
                year = self.get_headers(r'<p>Amendment(.+?)<\/p>').split('-')[-1]
                if '-' in effective_date:
                    effective_date = effective_date + '-' + year
                elif '/' in effective_date:
                    effective_date = effective_date + '/' + year
                effective_date = parse(effective_date)
            return df, effective_date

        def capture(self, df):

            if df[0].str.contains('Origins').any():
                origins_index = df[(df[0].str.contains('Origins', na=False))].index.values[0]

            if df.iloc[:, -1].str.contains('Effective from').any():
                effective_date_index = df[(df.iloc[:, -1].str.contains('Effective from', na=False))].index.values[0]
                effective_date = df.iloc[effective_date_index, -1].split()[-1]
                year = self.get_headers(r'Effective xx(.+?)<\/p>').split('-')[-1]
                if '-' in effective_date:
                    effective_date = effective_date + '-' + year
                elif '/' in effective_date:
                    effective_date = effective_date + '/' + year
                effective_date = parse(effective_date)

            if df[0].str.contains('Remark').any():
                end_index = df[(df[0].str.contains('Remark', na=False))].index.values[0]
            else:
                end_index = list(df[0].index)[-1] + 1
                df, effective_date = self.different_arb(df, origins_index)

            arbitrary_index = []
            services = pd.Series(df.iloc[origins_index, :])
            services.replace(nan, '', inplace=True)
            # services.reset_index(drop=True, inplace=True)
            for i in range(len(services)):
                if services[i] != nan and 'Arbitrary on top of POL' in services[i]:
                    arbitrary_index.append(i)

            main_arb = df.iloc[origins_index:end_index, 0:arbitrary_index[0] - 1]
            main_arb.columns = main_arb.iloc[0, :]
            main_arb = main_arb.iloc[2:, :]
            main_arb.dropna(how='all', inplace=True)

            services_df, arb_df = [], []
            if len(arbitrary_index) > 1:
                for i in range(len(arbitrary_index)):
                    services_df.append(df.iloc[origins_index:end_index, arbitrary_index[i]: arbitrary_index[i + 1]])
                    if i + 2 == len(arbitrary_index):
                        services_df.append(df.iloc[origins_index:end_index, arbitrary_index[i + 1]:])
                        break
            else:
                services_df.append(df)

            for df_ in services_df:
                services, flag = None, 0
                services_index = pd.Series(df_.iloc[0, :])
                if df.iloc[origins_index, :].str.contains('Services to').any():
                    for i in list(services_index.index):
                        try:
                            if 'Services to' in services_index[i]:
                                services = i
                        except KeyError:
                            continue
                    pol = df_.iloc[2:df_.shape[0] - 2, 0]
                else:
                    services = arbitrary_index[0] + 1
                    flag = 1
                    pol = df_.iloc[5:, 2]
                if flag == 0:
                    holder = df_.loc[:, services:services + 2]
                else:
                    holder = df_.loc[origins_index:, services:services + 2]
                holder['service_to'] = holder.iloc[0, 0]
                holder.columns = ['20GP', '40GP', '40HC', 'service_to']
                if flag == 0:
                    holder = holder.iloc[2:holder.shape[0] - 2, :]
                else:
                    holder = holder.iloc[2:, :]
                holder = pd.concat([main_arb, holder, pol], axis=1)
                holder['20GP'] = holder['20GP'].apply(lambda x: x if isinstance(x, int) else nan)
                holder.fillna('', inplace=True)
                holder = holder.loc[(holder['20GP'] != '') & (holder['40GP'] != '') & (holder['40HC'] != '')]
                holder = holder.loc[(holder['20GP'] != float(0)) & (holder['40GP'] != 0) & (holder['40HC'] != 0)]

                if holder['service_to'].str.contains('Services to \n').any():
                    holder['service_to'] = holder['service_to'].str.split('\n', expand=True)[1].str.strip()
                elif holder['service_to'].str.contains('Services to').any():
                    holder['service_to'] = holder['service_to'].str.split('Services to', expand=True)[1].str.strip()
                holder.set_axis([*holder.columns[:-1], 'POL'], axis=1, inplace=True)
                arb_df.append(holder)

            arb_df = pd.concat(arb_df, ignore_index=True)

            arb_df['start_date'] = effective_date
            self.captured_output = arb_df

            return self.captured_output

        def melt_load_type(self, df):
            if "load_type" not in df:
                df = df.melt(id_vars=[column for column in df.columns if not column[1].isdigit()],
                             var_name="load_type",
                             value_name="amount")
                return df

        def clean(self):

            arb_df = self.captured_output
            arb_df['service_to'] = arb_df['service_to'].str.split(',')
            arb_df = arb_df.explode('service_to')
            arb_df['service_to'] = arb_df['service_to'].apply(lambda x: x.strip())
            cols = {'Origins': 'icd', 'POL': 'via', 'service_to': 'to'}
            arb_df.rename(columns=cols, inplace=True)
            arb_df['charges'] = 'origin Arbitrary charges'
            arb_df['at'] = 'origin'
            if 'Port Code' in arb_df:
                arb_df.drop(['Port Code'], axis=1, inplace=True)
            arb_df = self.melt_load_type(arb_df)
            arb_df['currency'] = 'USD'

            self.cleaned_output = {'Arbitrary': arb_df}
            return self.cleaned_output

    class Zim_Word_Excel(BaseDocxFix, ZIM_Excel_fixed_am_fc, Origin_Arbitrary):

        def check_input(self):

            pass

        def check_output(self):

            pass

        def get_headers(self, regex):

            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            if not re.search(regex, self.raw_html):
                regex = r'Effective XX(.+?)<\/p>'
                matches = re.finditer(regex, self.raw_html, re.MULTILINE)
                if not re.search(regex, self.raw_html):
                    regex = r'<p>Amendment(.+?)<\/p>'
                    matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group = match.group(groupNum).strip()
            if '-' in group:
                return group
            elif len(group) > 2:
                return parse(group)
            else:
                return group

        def get_comm_desc(self):

            regex = r"– COMMODITIES:</strong></p>(.+?)<p><strong>APPENDIX"
            if re.search(regex, self.raw_html):
                matches = re.finditer(regex, self.raw_html, re.MULTILINE)
                for matchNum, match in enumerate(matches, start=1):
                    for groupNum in range(0, len(match.groups())):
                        groupNum = groupNum + 1
                        group = match.group(groupNum)
                if re.search(r'(</p><p>)(\s{2,})', group):
                    regex = r"(</p><p>)(\s{2,})"
                    subst = ','
                    group = re.sub(regex, subst, group, 0, re.MULTILINE)
                comm_list = group.split("</p><p>")
                comm_desc = {}
                for element in comm_list:
                    element = BeautifulSoup(element, "lxml").text
                    if '–' in element:
                        if element.count('–') > 1:
                            comm_desc[element.split('–')[0].strip()] = ''.join(element.split('–')[1:]).strip()
                        else:
                            comm_desc[element.split('–')[0].strip()] = element.split('–')[-1].strip()
                    else:
                        if element.count('-') > 1:
                            comm_desc[element.split('-')[0].strip()] = ''.join(element.split('-')[1:]).strip()
                        else:
                            comm_desc[element.split('-')[0].strip()] = element.split('-')[-1].strip()
                return comm_desc
            else:
                return 0

        def dest_arb(self, df):

            df.dropna(how='all', axis=0, inplace=True)
            df.dropna(how='all', axis=1, inplace=True)
            year = self.get_headers(r'<p>Amendment(.+?)<\/p>').split()[-1].split('-')[-1]
            effective_dates = [parse(df.iloc[0, 4].split()[-1] + year), parse(df.iloc[0, 5].split()[-1] + year)]
            main_arb = df.iloc[1:, 0:4]
            main_arb.columns = main_arb.iloc[0, :]
            main_arb.drop(['CUCC Code'], axis=1, inplace=True)
            main_arb = main_arb.iloc[1:, :].rename(columns={'RIPI Additional': 'icd'
                , 'Ramp/Door': 'mode_of_transportation_destination', 'Via': 'to'})

            def sub_arb(dfs):
                holder = dfs
                dfs = pd.DataFrame(dfs)
                dfs['20GP'], dfs['40GP'], dfs['40HC'] = holder, holder, holder
                dfs.drop(dfs.columns[0], axis=1, inplace=True)
                return dfs

            sub_arb_1 = df.iloc[2:, -2]
            sub_arb_1 = pd.concat([main_arb, sub_arb(sub_arb_1)], axis=1)
            sub_arb_1['start_date'] = effective_dates[0]
            sub_arb_1['expiry_date'] = effective_dates[-1]
            sub_arb_2 = df.iloc[2:, -1]
            sub_arb_2 = pd.concat([main_arb, sub_arb(sub_arb_2)], axis=1)
            sub_arb_2['start_date'] = effective_dates[-1]
            sub_arb_2['expiry_date'] = nan
            dest_arb = pd.concat([sub_arb_1, sub_arb_2])
            dest_arb['at'] = 'destination'
            dest_arb['charges'] = 'destination arbitrary charge'
            dest_arb['currency'] = 'USD'
            dest_arb = self.melt_load_type(dest_arb)

            return dest_arb

        def capture(self):

            for df in self.df:
                try:
                    if df[0].str.contains('TRADE LANE', na=False).any():
                        freight_df = df
                    elif df[0].str.contains('Origin Arbritaries', na=False).any():
                        origin_arbs = df
                    elif df[0].str.contains('Origin Arbitrary:', na=False).any():
                        origin_arb = df
                except AttributeError:
                    dest_arb = df
                    break

            contract_start_date = self.get_headers(r"effective on <strong>(.+?)<\/strong>")
            contract_expiry_date = self.get_headers(r"expire on <strong>(.+?)<\/strong>")
            amd_no = self.get_headers(r"Amendment No.(.+?)Effective")
            comm_desc = self.get_comm_desc()
            contract_id = self.df[0].iloc[0, 1]

            if not freight_df.empty:

                Expedoc_ZIM_Word.ZIM_Excel_fixed_am_fc.check_input(self, freight_df)
                Expedoc_ZIM_Word.ZIM_Excel_fixed_am_fc.capture(self, freight_df)
                freight = Expedoc_ZIM_Word.ZIM_Excel_fixed_am_fc.clean(self)
                Expedoc_ZIM_Word.ZIM_Excel_fixed_am_fc.check_output(self)

                for key, value in freight.items():
                    if comm_desc:
                        try:
                            value['commodity_description'] = value['commodity']
                        except KeyError:
                            value['commodity_description'] = value['COMMODITY']
                            value.rename(columns={'COMMODITY': 'commodity'}, inplace=True)
                        for code in comm_desc:
                            _code = (comm_desc[code])
                            value['commodity_description'].replace(code, _code, inplace=True, regex=True)

                    value['contract_start_date'] = contract_start_date
                    value['contract_expiry_date'] = contract_expiry_date
                    value['amendment_no'] = amd_no
                    value['vendor'] = 'ZIM'
                    value['contract_id'] = contract_id

            freight['Freight']['start_date'].loc[(freight['Freight']['start_date'].isna()) | (
                    freight['Freight']['start_date'] == '')] = contract_start_date
            freight['Freight']['expiry_date'].loc[(freight['Freight']['expiry_date'].isna()) | (
                    freight['Freight']['expiry_date'] == '')] = contract_expiry_date
            freight['Freight']['origin_port'] = freight['Freight']['origin_port'].astype(str).str.replace('/',
                                                                                                          ';').replace(
                'nan', nan)
            freight['Freight']['origin_icd'] = freight['Freight']['origin_icd'].astype(str).str.replace('/',
                                                                                                        ';').replace(
                'nan', nan)
            freight['Freight']['destination_port'] = freight['Freight']['destination_port'].astype(str).str.replace('/',
                                                                                                                    ';').replace(
                'nan', nan)
            freight['Freight']['destination_icd'] = freight['Freight']['destination_icd'].astype(str).str.replace('/',
                                                                                                                  ';').replace(
                'nan', nan)

            try:

                if not origin_arbs.empty:

                    Expedoc_ZIM_Word.Origin_Arbitrary.check_input(self)
                    Expedoc_ZIM_Word.Origin_Arbitrary.capture(self, origin_arbs)
                    origin_arbs = Expedoc_ZIM_Word.Origin_Arbitrary.clean(self)
                    Expedoc_ZIM_Word.Origin_Arbitrary.check_output(self)

                    origin_arbs['Arbitrary']['contract_start_date'] = contract_start_date
                    origin_arbs['Arbitrary']['contract_expiry_date'] = contract_expiry_date
                    origin_arbs['Arbitrary']['contract_id'] = contract_id
                    origin_arbs['Arbitrary']['expiry_date'] = contract_expiry_date
                    origin_arbs['Arbitrary']['amendment_no'] = amd_no
                    origin_arbs['Arbitrary']['vendor'] = 'ZIM'
                    origin_arbs['Arbitrary']['icd'] = origin_arbs['Arbitrary']['icd'].str.replace('/', ';')
                    origin_arbs['Arbitrary']['to'] = origin_arbs['Arbitrary']['to'].str.replace('/', ';')
                    if 'via' in origin_arbs['Arbitrary'].columns:
                        origin_arbs['Arbitrary']['via'] = origin_arbs['Arbitrary']['via'].str.replace('/', ';')
                        origin_arbs['Arbitrary']['via'] = origin_arbs['Arbitrary']['via'].str.replace('Direct', '')

            except NameError:

                origin_arbs = {}

            try:

                if not origin_arb.empty:

                    Expedoc_ZIM_Word.Origin_Arbitrary.check_input(self)
                    Expedoc_ZIM_Word.Origin_Arbitrary.capture(self, origin_arb)
                    origin_arb = Expedoc_ZIM_Word.Origin_Arbitrary.clean(self)
                    Expedoc_ZIM_Word.Origin_Arbitrary.check_output(self)

                    origin_arb['Arbitrary']['contract_start_date'] = contract_start_date
                    origin_arb['Arbitrary']['contract_expiry_date'] = contract_expiry_date
                    origin_arb['Arbitrary']['contract_id'] = contract_id
                    origin_arb['Arbitrary']['expiry_date'] = contract_expiry_date
                    origin_arb['Arbitrary']['amendment_no'] = amd_no
                    origin_arb['Arbitrary']['vendor'] = 'ZIM'
                    origin_arb['Arbitrary']['icd'] = origin_arb['Arbitrary']['icd'].str.replace('/', ';')
                    origin_arb['Arbitrary']['to'] = origin_arb['Arbitrary']['to'].str.replace('/', ';')
                    if 'via' in origin_arb['Arbitrary'].columns:
                        origin_arb['Arbitrary']['via'] = origin_arb['Arbitrary']['via'].str.replace('/', ';')
                        origin_arb['Arbitrary']['via'] = origin_arb['Arbitrary']['via'].str.replace('Direct', '')

            except NameError:

                origin_arb = {}

            try:

                if not dest_arb.empty:

                    dest_arb = self.dest_arb(dest_arb)
                    # dest_arb['expiry_date'] = contract_expiry_date
                    dest_arb['amendment_no'] = amd_no
                    dest_arb['vendor'] = 'ZIM'
                    dest_arb['contract_id'] = contract_id
                    dest_arb['contract_start_date'] = contract_start_date
                    dest_arb['contract_expiry_date'] = contract_expiry_date
                    dest_arb['expiry_date'].loc[dest_arb['expiry_date'].isna()] = contract_expiry_date
                    dest_arb['icd'] = dest_arb['icd'].str.replace('/', ';')
                    dest_arb['to'] = dest_arb['to'].str.replace('/', ';')
                    if 'via' in dest_arb.columns:
                        dest_arb['via'] = dest_arb['via'].str.replace('/', ';')
                        dest_arb['via'] = dest_arb['via'].str.replace('Direct', '')


            except NameError:

                dest_arb = {}

            if origin_arbs and origin_arb:
                if dest_arb.empty:
                    origin_arbs['Arbitrary'] = pd.concat([origin_arbs['Arbitrary'], origin_arb['Arbitrary']])
                else:
                    origin_arbs['Arbitrary'] = pd.concat([origin_arbs['Arbitrary'], origin_arb['Arbitrary'], dest_arb])

            if 'Arbitrary' in freight.keys():

                if 'start_date' not in freight['Arbitrary']:
                    freight['Arbitrary']['start_date'] = contract_start_date

                if 'Arbitrary' in origin_arbs.keys():
                    if 'Amendment no.' in freight['Arbitrary']:
                        freight['Arbitrary'].drop(columns=['Amendment no.'], inplace=True)
                    arb_df = pd.concat([freight['Arbitrary'], origin_arbs['Arbitrary']])
                    self.captured_output = {'Freight': freight['Freight'], 'Arbitrary Charges': arb_df}
                else:
                    self.captured_output = {'Freight': freight['Freight'], 'Arbitrary Charges': freight['Arbitrary']}

            else:

                if 'Arbitrary' in origin_arbs.keys():
                    self.captured_output = {'Freight': freight['Freight'],
                                            'Arbitrary Charges': origin_arbs['Arbitrary']}
                else:
                    self.captured_output = {'Freight': freight['Freight']}
            return self.captured_output

        def clean(self):

            self.cleaned_output = self.captured_output
            return self.cleaned_output


class ZIM_FECA(BaseTemplate):
    class Rate(BaseFix):

        def check_input(self):
            check_errors = []
            if not self.df[0].str.contains('Place of Receipt', na=False).any():
                check_errors.append("Place of Receipt cannot be found on the first column, the input file "
                                    "structure is different from sample template")
            if not self.df[0].str.contains('Origin Arbitrary', na=False).any():
                check_errors.append("Origin Arbitrary cannot be found on the first column, the input file "
                                    "structure is different from sample template")
            if check_errors:
                raise InputValidationError(check_errors)

        def check_output(self):
            pass

        def get_section(self):
            start_index = self.df[self.df[0].str.contains(r"Place of Receipt", na=False)].index.to_list()
            end_index = self.df[self.df[0].str.contains(r"Origin Arbitrary", na=False)].index.to_list()
            index = sorted(start_index + end_index)
            return index

        @staticmethod
        def get_subjects_to(freight_df, subjects):
            regex = r"(.+?)\s?:\s?(.+?)\s?for\s?(.+?)\s.*per\s?(.+?)\s?Effective\s?From\s?(.+?)\sTill\s?(.+?)\s"
            regex_gpo = r"GPO\s?Effective\s?From\s?(.+?)\s?Till\s?(.+?)\s"

            if re.search(regex, subjects):
                matches = re.findall(regex, subjects)
                for match in matches:
                    expiry_date = datetime.strptime(match[5], '%d-%b-%Y')
                    start_date = datetime.strptime(match[4], '%d-%b-%Y')
                    charge_name = match[0]
                    rates = match[1]
                    rates = rates.replace('$', '').split('/')
                    if match[2] == 'VAN':
                        index = freight_df.loc[freight_df['destination_icd'].str.contains('VANCOUVER')].index.to_list()

                    if match[2] == 'Halifax':
                        index = freight_df.loc[freight_df['destination_port'].str.contains('HALIFAX')].index.to_list()

                    if match[2] == 'VAN' and 'PRP' in subjects:
                        if freight_df.loc[freight_df['destination_port'].str.contains('VANCOUVER') |
                                          freight_df['destination_port'].str.contains('PRINCE RUPERT')].index.any():
                            index = freight_df.loc[freight_df['destination_port'].str.contains('VANCOUVER') |
                                                   freight_df['destination_port'].str.contains(
                                                       'PRINCE RUPERT')].index.to_list()

                    freight_df.loc[index, '20GP_' + charge_name] = rates[0]
                    freight_df.loc[index, '40GP_' + charge_name] = rates[1]
                    freight_df.loc[index, '40HC_' + charge_name] = rates[2]
                    freight_df.loc[index, '45HC_' + charge_name] = rates[3]
                    freight_df.loc[index, 'start_date'] = start_date
                    freight_df.loc[index, 'expiry_date'] = expiry_date

            if re.search(regex_gpo, subjects):
                subjects = subjects.replace('\n', '')
                match_date = re.findall(regex_gpo, subjects)
                start_date = match_date[0][0]
                expiry_date = match_date[0][1]
                regex_gpo = r"TO-(.+?)\s?&.*:\s?(.+?)\s?PER(.+?)TO(.+?)\s?&.*:(.+?)\s?PER\s?(.+?)$"
                charge_name = 'GPO'
                if re.search(regex_gpo, subjects):
                    match_van = re.findall(regex_gpo, subjects)[0]
                    rates = match_van[1].strip(' ').replace('$', '').split('/')
                    index = freight_df.loc[freight_df['destination_port'].str.contains('VANCOUVER') |
                                           freight_df['destination_port'].str.contains('PRINCE RUPERT')].index.to_list()
                    freight_df.loc[index, '20GP_' + charge_name] = rates[0]
                    freight_df.loc[index, '40GP_' + charge_name] = rates[1]
                    freight_df.loc[index, '40HC_' + charge_name] = rates[2]
                    freight_df.loc[index, '45HC_' + charge_name] = rates[3]
                    freight_df.loc[index, 'start_date'] = start_date
                    freight_df.loc[index, 'expiry_date'] = expiry_date
                    rates = match_van[4].strip(' ').replace('$', '').split('/')
                    index = freight_df.loc[freight_df['destination_port'].str.contains('HALIFAX')].index.to_list()
                    freight_df.loc[index, '20GP_' + charge_name] = rates[0]
                    freight_df.loc[index, '40GP_' + charge_name] = rates[1]
                    freight_df.loc[index, '40HC_' + charge_name] = rates[2]
                    freight_df.loc[index, '45HC_' + charge_name] = rates[3]
                    freight_df.loc[index, 'start_date'] = start_date
                    freight_df.loc[index, 'expiry_date'] = expiry_date
                return freight_df

        def charge_in_remarks(self, freight_df):
            if freight_df['remarks'].str.contains("Subject to fixed surcharge:").any():
                charge = freight_df['remarks'].str.extract(r"Subject to fixed surcharge:(.+?)\s?\((.+?)\)")
                charge_name = charge.iloc[0, 0]
                charge = charge[1].str.split(',', expand=True)
                charge.columns = ["20GP_" + charge_name, "40GP_" + charge_name, "40HC_" + charge_name, "45HC_" + charge_name]
                freight_df = pd.concat([freight_df, charge], axis=1)
                return freight_df
            else:
                return freight_df

        def get_freight_table(self, section_index):
            freight_df_final = pd.DataFrame()
            list_duration = self.df.iloc[section_index[0] - 1].to_list()
            duration = [duration for i, duration in enumerate(list_duration) if 'Valid' in duration][0]
            date = re.findall(r"Valid:\s?(.+?)\still\s?(.*)", duration)[0]
            start_date = date[0]
            expiry_date = date[1]
            for index in range(len(section_index) - 1):
                freight_df = self.df[section_index[index] + 1:section_index[index + 1]].copy(deep=True)
                group = self.df.iloc[section_index[index] - 1, 0]
                commodity_desc = re.findall(r"Group:\d{1,2}-(.+?)$", group)[0]
                freight_df.reset_index(inplace=True, drop=True)
                start_index = freight_df[freight_df[0].str.contains(r"Rate Include", na=False)].index.values[0]
                subject_index = freight_df[freight_df[0].str.contains(r"Rate Subject to", na=False)].index.values[0]
                inclusions = freight_df.iloc[start_index + 1, 0]
                subjects = freight_df.iloc[subject_index + 1, 0]
                inclusions = re.sub(r"(\/|)BAF(\/|)", "", inclusions, 0, re.MULTILINE)
                inclusions = inclusions.replace('/', ';')
                freight_df = freight_df[:start_index].copy(deep=True)
                freight_df.columns = [
                    'origin_port', 'origin_icd', 'destination_port', 'destination_icd', 'mode_of_transportation',
                    'currency', '20GP', '40GP', '40HC', '45HC', 'remarks'
                ]
                freight_df = self.charge_in_remarks(freight_df)
                freight_df = self.get_subjects_to(freight_df, subjects)
                freight_df['inclusions'] = inclusions
                freight_df['charges'] = 'Basic Ocean Freight'
                freight_df['bulletin'] = group
                freight_df['commodity'] = commodity_desc
                freight_df_final = pd.concat([freight_df_final, freight_df], axis=0, ignore_index=True)
                freight_df_final['start_date'] = start_date
                freight_df_final['expiry_date'] = expiry_date

            return freight_df_final, start_date, expiry_date

        def get_remarks(self, freight_df):
            start_index = self.df[self.df[0].str.contains(r"Remarks", na=False)].index.values[0]
            remarks_df = self.df[start_index:].copy()
            remarks_df.reset_index(drop=True, inplace=True)
            remarks = "\n".join(remarks_df[0].tolist())
            freight_df['remarks'] = freight_df['remarks'] + "\n" + remarks
            return freight_df

        def get_arb_table(self, index, start_date, expiry_date):
            end_index = self.df[self.df[0].str.contains(r"Remarks", na=False)].index.values[0]
            arb_table = self.df[index + 2:end_index].copy(deep=True)
            arb_table['via'] = arb_table[2].str.extract(r"via\s(.+?)\)")
            arb_table.drop(columns=[0, 2], inplace=True)
            arb_table.columns = ['icd', 'to', 'mode_of_transportation', 'currency', '20GP', '40GP',
                                 '40HC', '45HC', 'remarks', 'via']
            arb_table['other_geo_code'] = arb_table['remarks'].str.extract(r'For\s?(.+?);')[0]
            arb_table['at'] = 'origin'
            arb_table['charges'] = 'origin arbitrary charge'
            arb_table['start_date'] = start_date
            arb_table['expiry_date'] = expiry_date

            return arb_table

        @staticmethod
        def get_ows_and_map(x):
            regex_normal = r"OWS\s(\w{3})(\d{3})\/(\d{2}).*weight\sbetween\s(.+?)\s-\s?(.+?)\s(.*)"
            regex_normal_over = r"OWS\s(\w{3})(\d{3})\/(\d{2}).*weight\sover\s?(.+?)\s(.*)"
            regex_rail = r"via(.+?)(?:\((?:.*)|):(\w{3})(\d{1,5})\/(\d{2})(?:.*?)between\s(.+?)\s(\w{3})(?:.*?)of\s?(" \
                         r".+?)\s(\w{3})(?:.*)\)(\w{3})(\d{1,6})\/(.+?)\s(?:.*)between\s(.+?)\s(\w{3})(?:.*)of\s(" \
                         r".+?)\s(\w{3}) "
            ows_dict = {}

            if re.search(regex_normal, x):
                data = re.findall(regex_normal, x)[0]
                ows_dict['currency'] = data[0]
                ows_dict['20GP'] = data[1]
                ows_dict['weight_from'] = data[3]
                ows_dict['weight_to'] = data[4]
                ows_dict['weight_metric'] = data[5]

            elif re.search(regex_normal_over, x):
                data = re.findall(regex_normal_over, x)[0]
                ows_dict['currency'] = data[0]
                ows_dict['20GP'] = data[1]
                ows_dict['weight_from'] = data[3]
                ows_dict['weight_to'] = ''
                ows_dict['weight_metric'] = data[4]

            elif re.search(regex_rail, x):
                data = re.findall(regex_rail, x)[0]
                ows_dict['currency'] = data[1]
                ows_dict['20GP'] = data[2]
                ows_dict['weight_from'] = data[4]
                ows_dict['weight_to'] = data[6]
                ows_dict['weight_metric'] = data[7]
                ows_dict['country'] = data[0].strip(" ")
                ows_dict['40HC_dict'] = dict
                ows_dict['40HC_dict'] = {"currency": data[8]}
                ows_dict['40HC_dict']["40GP"] = data[9]
                ows_dict['40HC_dict']["weight_from"] = data[11]
                ows_dict['40HC_dict']["weight_to"] = data[13]
                ows_dict['40HC_dict']["weight_metric"] = data[14]

            return ows_dict

        def map_ows_charges(self, freight_df, ows_list):
            freight_df_final = freight_df.copy()
            freight_df_iter = freight_df.copy()
            # original_freight_df = self.melt_dimension(freight_df)
            for ows in ows_list:
                freight_df = freight_df_iter.copy()
                # freight_df['charges'] = 'OWS'
                if "country" not in ows:
                    if ows['weight_from'] == '':
                        freight_df['20GP_OWS_over'] = ows['20GP']
                        freight_df['40GP_OWS'] = ''
                        freight_df['45HC_OWS'] = ''
                        freight_df['40HC_OWS'] = ''
                    else:
                        freight_df['20GP_OWS'] = ows['20GP']
                        freight_df['20GP_OWS_over'] = ''
                    freight_df['weight_to'] = ows['weight_to']
                    freight_df['weight_from'] = ows['weight_from']
                    freight_df['weight_metric'] = ows['weight_metric']
                    if '40HC_dict' in ows:
                        freight_df_40 = freight_df_iter.copy()
                        country = ows['country']
                        ows = ows['40HC_dict']
                        ows['country'] = country
                        freight_df_40['40GP_OWS'] = ows['40GP']
                        freight_df_40['45HC_OWS'] = ows['40GP']
                        freight_df_40['40HC_OWS'] = ows['40GP']
                        freight_df_40['weight_to'] = ows['weight_to']
                        freight_df_40['weight_from'] = ows['weight_from']
                        freight_df_40['weight_metric'] = ows['weight_metric']
                        freight_df = pd.concat([freight_df, freight_df_40])
                else:
                    ows_country = ows['country'].split('/')
                    index_country = []
                    for country in ows_country:
                        index_country += freight_df.loc[freight_df['destination_port'].str.contains(country.upper(),
                                                                                                    regex=True) &
                                                        freight_df['mode_of_transportation'].str.contains('/R',
                                                                                                          regex=True)].index.to_list()
                    index_country = sorted(index_country)
                    freight_df.loc[index_country, '20GP_OWS_over'] = ''
                    freight_df.loc[index_country, '20GP_OWS'] = ows['20GP']
                    freight_df.loc[index_country, 'weight_to'] = ows['weight_to']
                    freight_df.loc[index_country, 'weight_from'] = ows['weight_from']
                    freight_df.loc[index_country, 'weight_metric'] = ows['weight_metric']
                    if '40HC_dict' in ows:
                        freight_df_40 = freight_df_iter.copy()
                        ows = ows['40HC_dict']
                        freight_df_40.loc[index_country, '40GP_OWS'] = ows['40GP']
                        freight_df_40.loc[index_country, '40HC_OWS'] = ows['40GP']
                        freight_df_40.loc[index_country, '45HC_OWS'] = ows['40GP']
                        freight_df_40.loc[index_country, 'weight_to'] = ows['weight_to']
                        freight_df_40.loc[index_country, 'weight_from'] = ows['weight_from']
                        freight_df_40.loc[index_country, 'weight_metric'] = ows['weight_metric']
                        freight_df = pd.concat([freight_df, freight_df_40])

                freight_df_final = pd.concat([freight_df_final, freight_df])

            return freight_df_final

        @staticmethod
        def melt_dimension(df):
            charge_profile = [column for column in df.columns if column[0].isdigit()]

            df = df.melt(
                id_vars=[column for column in df.columns if column not in charge_profile],
                value_vars=charge_profile, value_name='amount', var_name='load_type')
            df['amount'] = df['amount'].astype(str)
            return df

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

        def get_surcharge_from_remarks(self, freight_df):
            start_index = self.df[self.df[0].str.contains(r"Remarks", na=False)].index.values[0]
            remarks_df = self.df[start_index:].copy()
            remarks_df.reset_index(drop=True, inplace=True)
            ows_df = remarks_df.loc[remarks_df[0].str.contains("OWS") | remarks_df[0].str.contains("weight")][0].copy()
            if ows_df.empty:
                return freight_df
            index_ = ows_df.loc[ows_df.str.contains("via")].index.to_list()
            for index in index_:
                ows_df[index] = ows_df[index] + ows_df[index + 1] + ows_df[index + 2]
                ows_df[index + 1] = ''
                ows_df[index + 2] = ''
            ows_df.replace('', nan, inplace=True)
            ows_df = ows_df.dropna()
            ows_df.reset_index(drop=True, inplace=True)
            ows_rates = ows_df.apply(self.get_ows_and_map)
            ows_list = ows_rates.to_list()
            freight_df = self.map_ows_charges(freight_df, ows_list)
            freight_df = self.melt_dimension(freight_df)
            freight_df['amount'].replace('', nan, inplace=True)
            freight_df.dropna(subset=['amount'], inplace=True)
            freight_df = self.pivot_load_type(freight_df)
            freight_df = freight_df.replace("nan", '')
            freight_df = freight_df.replace(nan, '')
            freight_df['20GP_OWS'] = freight_df['20GP_OWS'].replace("", nan)
            freight_df['20GP_OWS'] = freight_df['20GP_OWS'].fillna(freight_df['20GP_OWS_over'])
            freight_df.drop(['20GP_OWS_over'], axis=1, inplace=True)
            freight_df = freight_df.loc[~((freight_df['weight_to'] == '') & (freight_df['weight_from'] == ''))]
            # freight_df = self.melt_dimension(freight_df)
            return freight_df

        @staticmethod
        def format_output(df_freight, arb_df):
            output = {'Freight': df_freight, 'Arbitrary Charges': arb_df}
            return output

        def capture(self):

            section_index = self.get_section()
            freight_df, start_date, expiry_date = self.get_freight_table(section_index)
            arb_df = self.get_arb_table(section_index[-1], start_date, expiry_date)
            freight_df = self.get_surcharge_from_remarks(freight_df)
            freight_df = self.get_remarks(freight_df)
            self.captured_output = self.format_output(freight_df, arb_df)

        def clean(self):
            freight_df = self.captured_output['Freight']

            def str_replace(regex, subst, col):
                freight_df[col] = freight_df[col].str.replace(regex, subst)
                return freight_df

            col_rename = ['origin_icd']

            for col in col_rename:
                freight_df = str_replace(r"\/", ";", col)

            self.cleaned_output = {'Freight': freight_df,
                                   'Arbitrary Charges': self.captured_output['Arbitrary Charges']}


class ZIM_Excel_FECA(ZIM_FECA):
    class Rate(ZIM_FECA.Rate):

        def capture(self):
            section_index = self.get_section()
            freight_df, start_date, expiry_date = self.get_freight_table(section_index)
            arb_df = self.get_arb_table(section_index[-1], start_date, expiry_date)
            self.captured_output = self.format_output(freight_df, arb_df)


class Ceva_Zim_Latam(ZIM_FECA):
    class _Rate(ZIM_FECA.Rate):
        def capture(self):
            section_index = self.get_section()
            freight_df, start_date, expiry_date = self.get_freight_table(section_index)
            arb_df = self.get_arb_table(section_index[-1], start_date, expiry_date)
            # freight_df = self.get_surcharge_from_remarks(freight_df)
            freight_df = self.get_remarks(freight_df)
            self.captured_output = self.format_output(freight_df, arb_df)


class Ceva_Zim_Usa(ZIM_FECA):
    class _Rate(ZIM_FECA.Rate):
        def capture(self):
            section_index = self.get_section()
            freight_df, start_date, expiry_date = self.get_freight_table(section_index)
            arb_df = self.get_arb_table(section_index[-1], start_date, expiry_date)
            # freight_df = self.get_surcharge_from_remarks(freight_df)
            freight_df = self.get_remarks(freight_df)
            self.captured_output = self.format_output(freight_df, arb_df)
