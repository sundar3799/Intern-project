from logging import getLogger
from dps_headers.headers import get_headers
import pandas as pd
from base import BaseTemplate, BaseFix, BaseMultipleTemplateResolver
from custom_exceptions import InputValidationError
import re
from numpy import nan
import numpy as np
from datetime import datetime, date
from dateutil.parser import parse

log = getLogger(__name__)


class Flexport_HAPAGLLYOD_TPEB_CANADA_v1(BaseTemplate):
    class BaseTier(BaseFix):

        def check_input(self):

            check_errors = []
            if not self.df.iloc[:, 0].str.contains('Rates are not subject to').any():
                check_errors.append("Rates are not subject to should be present in first Column")

            AmendmentNo = re.compile(r'.*Update # (\d+).*from (\w+ \d+, \d+).* (\w+ \d+, \d+).*')
            if not self.df.iloc[:, 0].apply(lambda x: bool(AmendmentNo.match(x))).any():
                check_errors.append("Amendment No , StartDate , Expiry Date  not found at First Column")

            if check_errors:
                raise InputValidationError(check_errors)

        def check_output(self):
            pass

        def capture(self):

            df = self.df

            def get_expiry_date(date_str):
                return re.search(r".*Update # (\d+).*from (\w+ \d+, \d+).* (\w+ \d+, \d+).*", date_str)

            def get_date_format(date_str):
                dobj = datetime.strptime(str(date_str), '%B %d, %Y')
                dobj_str = dobj.strftime('%Y-%m-%d')
                return datetime.strptime(dobj_str, '%Y-%m-%d')

            captured_data = df.iloc[:, 0].apply(lambda x: get_expiry_date(str(x)))
            for i in captured_data:
                if i:
                    AmendmentNo = i.group(1)
                    start_date = i.group(2)
                    expiry_date = i.group(3)

            indexes_start = df[(df.iloc[:, 0].str.lower() == 'rates are not subject to')].index[0]
            indexes_end = df[(df.iloc[:, 0].str.lower() == 'garments')].index.values[0]
            remarks_df = df.iloc[indexes_start:indexes_end, :2]
            remarks_str = remarks_df.replace(np.nan, '').to_string(header=False, index=False).replace(r'\n', '')

            indexes = df[(df.iloc[:, 0].str.lower() == 'rates are not subject to')].index[0]
            subject_to = df.iloc[indexes][1].split('(')[0]
            inclusions = subject_to.replace('/', ',')

            indexes = df[(df.iloc[:, 0].str.lower() == 'rates are subject to')].index[0]
            subject_to = df.iloc[indexes][1].split('. All')[0]
            cleaned_data = re.sub(r"\([\w\s]+\)", "", subject_to)
            rates_are_subject_to = cleaned_data.replace('.', ',')

            # df = df[self.headers_config['headers'].keys()]

            df.columns = ['origin_port', 'destination_port', 'destination_icd', '20GP', '20GP_MFR', '20GP_GRA',
                          '20GP_FAKRATE', '40HC', '40HC_MFR', '40HC_GRA', '40HC_FAKRATE', 'routing_notes']

            index = list(df[(df['origin_port'].str.contains("POL", na=False))].index)

            df = df[index[0] + 1:]

            df = df.reset_index(drop=True)

            nan_value = float("NaN")

            df.replace("", nan_value, inplace=True)

            df = df.dropna(subset=['destination_port', '20GP', '40HC', 'origin_port'])

            df.fillna('', inplace=True)

            df['start_date'] = get_date_format(start_date)

            df['expiry_date'] = get_date_format(expiry_date)

            df['amendment_no'] = AmendmentNo

            df['remarks'] = remarks_str

            df['remarks'] += '   ' + df['routing_notes']

            df['inclusions'] = inclusions

            df['subject_to'] = rates_are_subject_to
            self.captured_output = {'Freight': df}

        def check_output(self):
            pass

        def clean(self):

            df = self.captured_output['Freight']
            df["origin_port"].replace(['China Origin Exclusive'], "Shanghai;Ningbo;Qingdao;Xiamen;Yantian",
                                      inplace=True)
            df["origin_port"].replace(" Except China", '', regex=True, inplace=True)
            df["origin_port"].replace(['East Asian Base Ports'],
                                      'Pusan;Kwangyang (PS8);Hong Kong;Kaohsiung;Keelung (PS4 only);Cai Mep (Vung Tau);Haiphong (PS3);Laem Chabang;Port Kelang;Singapore;Tokyo;Kobe;Nagoya',
                                      inplace=True)

            df['currency'] = 'USD'
            df["commodity"] = 'FAK'
            df['charges'] = "Basic ocean freight"
            df["charges_leg"] = "L3"

            df.drop(columns=['20GP_FAKRATE', '40HC_FAKRATE'], inplace=True)

            df['40GP'] = df['40HC'].copy()
            if '40HC_MFR' in df:
                df['40GP_MFR'] = df['40HC_MFR'].copy()
            if '40HC_GRA' in df:
                df['40GP_GRA'] = df['40HC_GRA'].copy()

            df.drop(columns=["routing_notes"], inplace=True)

            df['destination_icd'].replace('', nan, inplace=True)

            df.loc[
                df[~df['destination_icd'].isna()].index.to_list(), 'mode_of_transportation_destination'] = 'RAMP'

            df['destination_port'].replace('/', ';', inplace=True, regex=True)
            df['destination_icd'].replace('/', ';', inplace=True, regex=True)
            self.cleaned_output = {'Freight': df}

    class RemarksBaseTier(BaseFix):

        def check_input(self):

            check_errors = []

            check_SGF = re.compile(r'-.(SGF):.*:.(USD)(\d+).(\w+)')
            if not self.df.iloc[:, 0].apply(lambda x: bool(check_SGF.match(x))).any():
                check_errors.append("SGF charges not found at First Column")

            # check_PSS = re.compile(r'(PSS):.*: (\w+) (\d+)\/(\w+)')
            # if not self.df.iloc[:, 0].apply(lambda x: bool(check_PSS.match(x))).any():
            #     check_errors.append("PSS charges not found at First column")

            if check_errors:
                raise InputValidationError(check_errors)

        def clean(self):

            cleaned_data = []
            for i in self.surcharges_list:
                charges = {}
                if i['load_type'].lower() == 'teu':
                    charges['20GP' + '_' + i['charge']] = int(i['amount'])
                    charges['40GP' + '_' + i['charge']] = int(i['amount']) * 2
                    charges['40HC' + '_' + i['charge']] = int(i['amount']) * 2
                    cleaned_data.append(charges)

            self.cleaned_output = {'Freight charges': cleaned_data}

        def capture(self):

            def get_surchagres(date_str):
                return re.search(r"(PSS):.*: (\w+) (\d+)/(\w+)", date_str)

            captured_data = self.df.iloc[:, 0].apply(lambda x: get_surchagres(str(x)))
            PSS_surcharges = {}
            for i in captured_data:
                if i:
                    PSS_surcharges['charge'] = i.group(1)
                    PSS_surcharges['currreny_type'] = i.group(2)
                    PSS_surcharges['amount'] = i.group(3)
                    PSS_surcharges['load_type'] = i.group(4)

            def get_surcharges_SGF(data_str):
                return re.search(r"-.(SGF):.*:.(USD)(\d+).(\w+)", data_str)

            captured_data = self.df.iloc[:, 0].apply(lambda x: get_surcharges_SGF(str(x)))

            SGF_surcharges = {}
            for i in captured_data:
                if i:
                    SGF_surcharges['charge'] = i.group(1)
                    SGF_surcharges['currreny_type'] = i.group(2)
                    SGF_surcharges['amount'] = i.group(3)
                    SGF_surcharges['load_type'] = i.group(4)

            self.surcharges_list = []

            self.surcharges_list.append(SGF_surcharges)
            self.surcharges_list.append(PSS_surcharges)

        def check_output(self):
            pass

    @classmethod
    def resolve_dependency(cls, fix_outputs):

        fix_outputs.pop(' Remarks Base Tier')

        return fix_outputs


class Flexport_Hapag_MN_FAK_v1(BaseTemplate):
    class TotalRate_(BaseFix):

        def check_input(self):

            check_errors = []
            if not self.df.iloc[:, 7].str.contains('No-Chargeable:').any():
                check_errors.append("No-Chargeable: should be present in seventh Column")

            if not self.df.iloc[:, 7].str.contains('Subject to:').any():
                check_errors.append("Subject to:  should be present in seventh Column")

            if check_errors:
                raise InputValidationError(check_errors)

        def capture(self):

            df = self.df

            def ZoneCapture(concated_df, origin_port):

                index_start = concated_df[(concated_df.iloc[:, 0].str.lower() == 'pod')].index.values[0]

                concated_df.columns = concated_df.iloc[index_start]

                concated_df.drop(concated_df.index[index_start])

                nan_value = float("NaN")

                concated_df.replace("", nan_value, inplace=True)

                concated_df = concated_df.reset_index(drop=True)

                concated_df = concated_df.dropna(subset=["POD", "20' STD \nSEA", "40' HC \nSEA", "40' RF \nSEA"])

                concated_df = concated_df[concated_df.RAMP != 'RAMP']

                concated_df = concated_df.loc[:, concated_df.columns.notnull()]

                concated_df.replace(np.nan, '', inplace=True)

                concated_df['origin_port'] = origin_port

                return concated_df

            def Get_IndiaZone2(df):

                start = df.columns.get_loc('INDIA_ZONE2')

                end = df.columns.get_loc('SRI_LANKA')

                columns_add = df[['destination_port', "destination_icd"]]

                Indiazone_2 = df.iloc[:, int(start):int(end)]

                columns_add = df[['destination_port', "destination_icd"]]

                concated_df = pd.concat([columns_add, Indiazone_2], ignore_index=True, axis=1)

                origin_ports = concated_df.iloc[2][7].replace('\n', ';')

                captured_df = ZoneCapture(concated_df, origin_ports)

                return captured_df

            def Get_PAKISTAN(df):

                start = df.columns.get_loc('PAKISTAN')

                end = df.columns.get_loc('ARABIAN_GULF')

                columns_add = df[['destination_port', "destination_icd"]]

                pakistan = df.iloc[:, int(start):int(end)]

                columns_add = df[['destination_port', "destination_icd"]]

                concated_df = pd.concat([columns_add, pakistan], ignore_index=True, axis=1)

                origin_ports = concated_df.iloc[2][7].replace('\n', ';')

                captured_df = ZoneCapture(concated_df, origin_ports)

                return captured_df

            def Get_SRILANKA(df):

                start = df.columns.get_loc('SRI_LANKA')

                end = df.columns.get_loc('BANGLADESH')

                columns_add = df[['destination_port', "destination_icd"]]

                sri_lanka = df.iloc[:, int(start):int(end)]

                columns_add = df[['destination_port', "destination_icd"]]

                concated_df = pd.concat([columns_add, sri_lanka], ignore_index=True, axis=1)

                origin_ports = concated_df.iloc[2][7].replace('\n', ';')

                captured_df = ZoneCapture(concated_df, origin_ports)

                return captured_df

            def Get_BANGLADESH(df):

                start = df.columns.get_loc('BANGLADESH')

                end = df.columns.get_loc('PAKISTAN')

                columns_add = df[['destination_port', "destination_icd"]]

                bangladesh = df.iloc[:, int(start):int(end)]

                columns_add = df[['destination_port', "destination_icd"]]

                combined_df = pd.concat([columns_add, bangladesh], ignore_index=True, axis=1)

                origin_port = combined_df.iloc[2][7].replace('\n', ';')

                captured_df = ZoneCapture(combined_df, origin_port)

                return captured_df

            def Get_ARABIANGULF(df):

                start = df.columns.get_loc('ARABIAN_GULF')

                end = df.columns.get_loc('RED_SEA')

                columns_add = df[['destination_port', "destination_icd"]]

                arabian_gulf = df.iloc[:, int(start):int(end)]

                columns_add = df[['destination_port', "destination_icd"]]

                combined_df = pd.concat([columns_add, arabian_gulf], ignore_index=True, axis=1)

                origin_ports = combined_df.iloc[2][7].replace('\n', ';')

                origin_port = re.sub(' +', ';', origin_ports)

                captured_df = ZoneCapture(combined_df, origin_port)

                return captured_df

            def Get_IndiaZone1(df):

                start = df.columns.get_loc('INDIA_ZONE1')

                end = df.columns.get_loc('INDIA_ZONE2')

                columns_add = df[['destination_port', "destination_icd"]]

                Indiazone_1 = df.iloc[:, int(start):int(end)]

                columns_add = df[['destination_port', "destination_icd"]]

                combined_df = pd.concat([columns_add, Indiazone_1], ignore_index=True, axis=1)

                origin_port = combined_df.iloc[2][7].replace('\n', ';')

                captured_df = ZoneCapture(combined_df, origin_port)

                return captured_df

            def Get_RedZone(df):

                start = df.columns.get_loc('RED_SEA')

                redzone_df = df.iloc[:, int(start):]

                columns_add = df[['destination_port', "destination_icd"]]

                combined_df = pd.concat([columns_add, redzone_df], ignore_index=True, axis=1)

                origin_port = combined_df.iloc[2][7].replace('\n', ';')

                captured_df = ZoneCapture(combined_df, origin_port)

                return captured_df

            def get_expiry_date(date_str):
                return re.search(r"\d+-\d+\-\d+", date_str)

            def get_date_format(date_str):
                dobj = datetime.strptime(str(date_str), '%Y-%m-%d')
                dobj_str = dobj.strftime('%Y-%m-%d')
                return datetime.strptime(dobj_str, '%Y-%m-%d')

            start_date = df.iloc[:, 7].apply(lambda x: get_expiry_date(str(x)))
            expiry_date = df.iloc[:, 9].apply(lambda x: get_expiry_date(str(x)))

            for i in start_date:
                if i:
                    start_date_str = i.group(0)

            for i in expiry_date:
                if i:
                    expiry_date_str = i.group(0)

            indexes_start = df[(df.iloc[:, 7].str.lower() == 'no-chargeable:')].index[0]
            indexes_end = df[(df.iloc[:, 7].str.lower() == 'shipper owned (sow) equipment')].index.values[0]
            remarks_df = df.iloc[indexes_start:indexes_end, :14]
            remarks_str = remarks_df.replace(np.nan, '').to_string(header=False, index=False).replace(r'\n', '')

            indexes = df[(df.iloc[:, 7].str.lower() == 'no-chargeable:')].index[0]
            subject_to = df.iloc[indexes][9]
            inclusive = subject_to.replace('/', ',')

            indexes = df[(df.iloc[:, 7].str.lower() == 'subject to:')].index[0]
            subject_to = df.iloc[indexes][9]
            data = re.findall(r'(?<![A-Z])[A-Z]{3}(?![A-Z])', subject_to)
            rates_are_subject_to = ','.join(data)

            IndiaZone1 = Get_IndiaZone1(df)
            IndiaZone2 = Get_IndiaZone2(df)
            SRILANKA = Get_SRILANKA(df)
            BANGLADESH = Get_BANGLADESH(df)
            ARABIANGULF = Get_ARABIANGULF(df)
            RedZone = Get_RedZone(df)
            PAKISTAN = Get_PAKISTAN(df)

            final_df = pd.concat([IndiaZone1, IndiaZone2, SRILANKA, PAKISTAN, BANGLADESH, ARABIANGULF, RedZone],
                                 ignore_index=True)

            final_df['start_date'] = get_date_format(str(start_date_str))
            final_df['expiry_date'] = get_date_format(str(expiry_date_str))
            # final_df['amendment_no'] = AmendmentNo
            final_df['currency'] = 'USD'
            final_df["commodity"] = 'FAK'
            final_df['charges'] = "Basic ocean freight"
            final_df["charges_leg"] = "L3"
            final_df['remarks'] = remarks_str
            final_df['inclusions'] = inclusive
            final_df['subject_to'] = rates_are_subject_to
            final_df['remarks'] = final_df['remarks'].str.strip()

            self.df = final_df

        def clean(self):

            cleaned_df = self.df

            cleaned_df.rename(columns={
                "RAMP": "destination_icd",
                "POD": "destination_port",
                "20 RF RAPP": '20RE_RAPP',
                "20 STD TOTAL": "20GP_TOTAL",
                "20' RAPP": '20GP_RAPP',
                "20' RF \nSEA": "20RE",
                "20' RF GRA": "20RE_GRA",
                "20' RF MFR": "20RE_MFR",
                "20' RF TOTAL": "20RE_TOTAL",

                "20' STD GRA": "20GP_GRA",
                "20' STD MFR": "20GP_MFR",
                "20' STD \nSEA": "20GP",

                "40' HC \nSEA": "40HC",
                "40' HC GRA": "40HC_GRA",
                "40' HC MFR": "40HC_MFR",
                "40' HC TOTAL": "40HC_TOTAL",
                "40' RAPP": "40HC_RAPP",

                "40' RF \nSEA": "40RE",
                "40' RF GRA": "40RE_GRA",
                "40' RF MFR": "40RE_MFR",
                "40' RF TOTAL": "40RE_TOTAL",
                "40RF RAPP": "40RE_RAPP"

            }, inplace=True)

            cols = cleaned_df.columns.to_series()

            # Two destination port columns there in sheet , Second destination to be removed
            if cols.iloc[1] == 'destination_port':
                cols.iloc[1] = 'second_destination_port'
                cleaned_df.columns = cols
                cleaned_df.drop(columns='second_destination_port', inplace=True)

            cleaned_df.replace(np.nan, '', inplace=True)

            cleaned_df.drop(
                columns=['20GP_TOTAL', '20RE_RAPP', '20RE_RAPP', '20RE_TOTAL', '40HC_TOTAL', '40HC_RAPP', '40RE_RAPP',
                         '40RE_TOTAL'], inplace=True)

            def assign_US(x):
                return list(map(lambda n: 'US' + n, x))

            def list_to_str(x):
                return ';'.join(x)

            index = cleaned_df[cleaned_df['destination_port'].str.contains('/')].index.tolist()

            cleaned_df.loc[index, 'destination_port'] = cleaned_df[cleaned_df['destination_port'].str.contains('/')][
                'destination_port'].str.split('/').apply(assign_US).apply(list_to_str)

            cleaned_df['destination_icd'].replace('/', ';', inplace=True, regex=True)

            cleaned_df['destination_port'].replace(',', ';', inplace=True, regex=True)

            cleaned_df['destination_port'] = cleaned_df['destination_port'].str.strip()

            cleaned_df['destination_icd'] = cleaned_df['destination_icd'].str.strip()

            self.cleaned_output = {'Freight': cleaned_df}

        def check_output(self):
            pass


class Flexport_HAPAGLLYOD_TPEB_USA_v1(BaseTemplate):
    class China_Fak_GL(BaseFix):

        def check_input(self):

            check_errors = []
            if not self.df.iloc[:, 0].str.contains('Rates are subject to').any():
                check_errors.append("Rates are subject to should be present in First Column")

            if not self.df.iloc[:, 0].str.contains('Garments').any():
                check_errors.append("Garments should be present in First Column")

            if not self.df.iloc[:, 0].str.contains('Non Chargeable or Inclusive').any():
                check_errors.append("'Non Chargeable or Inclusive' should be present in First Column")

            if check_errors:
                raise InputValidationError(check_errors)

        def clean(self):

            df = self.captured_output["freight"]

            df.replace(np.nan, '', inplace=True)


            df['remarks'] += '\n' + df['routing_notes']

            # df["origin_port"].replace(['China Origin Exclusive'], "Shanghai;Ningbo;Qingdao;Xiamen;Yantian",
            #                           inplace=True)
            # df["origin_port"].replace(" Except China", '', regex=True, inplace=True)
            # df["origin_port"].replace(['East Asian Base Ports'],
            #                           'Pusan;Kwangyang (PS8);Hong Kong;Kaohsiung;Keelung (PS4 only);Cai Mep (Vung Tau);Haiphong (PS3);Laem Chabang;Port Kelang;Singapore;Tokyo;Kobe;Nagoya',
            #                           inplace=True)
            df['currency'] = 'USD'
            df["commodity"] = 'FAK'
            df['charges'] = "Basic ocean freight"
            df["charges_leg"] = "L3"
            df['40GP'] = df['40HC'].copy()
            df['40GP_MFR'] = df['40HC_MFR'].copy()
            df['40GP_GRA'] = df['40HC_GRA'].copy()
            df["remarks"] = df['remarks'].str.lstrip().str.rstrip()


            df['destination_port'].replace('/', ';', inplace=True, regex=True)

            df['destination_icd'].replace('/', ';', inplace=True, regex=True)
            df['destination_port'] = df['destination_port'].str.replace('\s(?=\()', ';', regex=True)

            df.drop(columns=["routing_notes"], inplace=True)

            to_drop = df.loc[df['origin_port'] == 'POL'].index.to_list()
            df.drop(index=to_drop, inplace=True, axis=0)
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            arbitary_df = df.loc[df["destination_icd"] != ""]
            arbitary_df["charges_leg"], arbitary_df["charges"] = "L4", "Destination arbitrary Charges"
            freight_df = df.loc[df["destination_icd"] == ""]
            arbitary_df = arbitary_df.rename(columns={'destination_icd': 'icd', 'destination_port': 'via', 'origin_port': 'to'})
            arbitary_df["at"] = "destination"
            self.cleaned_output = {'Freight': freight_df, "Arbitary Charges": arbitary_df}

        def get_origin_lookup(self):
            origin_lookup ={}
            if self.df.iloc[:, 0].str.contains('East Asia Base Ports', na=False, case=False).any():
                indexes = self.df[(self.df.iloc[:, 0].str.contains('East Asia Base Ports', na=False, case=False))].index[0]
                origin_ = self.df.iloc[indexes][1]
                origin_lookup["East Asian Base Ports Except China"] = origin_.replace(",", ";")

            if self.df.iloc[:, 0].str.contains('China Origin Exclusive', na=False, case=False).any():
                indexes = self.df[(self.df.iloc[:, 0].str.contains('China Origin Exclusive', na=False, case=False))].index[0]
                origin_ = self.df.iloc[indexes][1]
                origin_lookup["China Base Ports"] = origin_.replace(",", ";")

            return origin_lookup

        def capture(self):

            df = self.df
            origin_lookup = self.get_origin_lookup()


            def get_expiry_date(date_str):
                return re.search(r".*Update #(\d+) .*From (\w+ \d+).* (\w+ \d+),( \d+).*", date_str)

            def get_date_format(date_str):
                dobj = parse(str(date_str))
                # dobj = datetime.strptime(str(date_str), '%B %d, %Y')
                dobj_str = dobj.strftime('%Y-%m-%d')
                return datetime.strptime(dobj_str, '%Y-%m-%d')

            captured_data = df.iloc[:, 0].apply(lambda x: get_expiry_date(str(x)))

            for i in captured_data:
                if i:
                    AmendmentNo = i.group(1)
                    start_date = i.group(2) + ', ' + i.group(4)
                    expiry_date = i.group(3) + ', ' + i.group(4)

            indexes_start = df[(df.iloc[:, 0].str.contains('Non-Direct ports', na=False, case=False))].index.values[0]
            indexes_end = df[(df.iloc[:, 0].str.lower() == 'garments')].index.values[0]
            remarks_df = df.iloc[indexes_start:indexes_end, :2]
            remarks_str = remarks_df.replace(np.nan, '').to_string(header=False, index=False).replace(r'\n', '')

            indexes = df[(df.iloc[:, 0].str.lower() == 'non chargeable or inclusive')].index[0]
            subject_to = df.iloc[indexes][1].split('(')[0]
            inclusive = subject_to.replace('/', ',')

            indexes = df[(df.iloc[:, 0].str.lower() == 'rates are subject to')].index[0]
            subject_to = df.iloc[indexes][1]
            data = re.findall(r'(?<![A-Z])[A-Z]{3}(?![A-Z])', subject_to)
            rates_are_subject_to = ','.join(data)

            # df = df[self.headers_config['headers'].keys()]
            # df = df[self.headers_config['actual_data_start_row']:]
            if len(df.columns) ==16:
                df.columns = ['origin_port', 'destination_port', 'destination_icd', '20GP', '20GP_MFR',
                              '20GP_GRA','20GP_FAKRATE', '40HC', '40HC_MFR', '40HC_GRA',
                              '40HC_FAKRATE', '45HC', '45HC_MFR', '45HC_GRA', '45HC_FAKRATE',
                              'routing_notes']

            else:
                df.columns = ['origin_port', 'destination_port', 'destination_icd', '20GP', '20GP_MFR',
                              '20GP_GRA','20GP_FAKRATE', '40HC', '40HC_MFR', '40HC_GRA',
                              '40HC_FAKRATE', 'routing_notes']

            df = df.reset_index(drop=True)
            nan_value = float("NaN")
            df.replace("", nan_value, inplace=True)

            # exclusions2 = df[df[['destination_port', '20GP', '40HC', 'origin_port']].isnull().any(axis=1)]
            df.drop(columns=['20GP_FAKRATE', '40HC_FAKRATE', '45HC_FAKRATE'], inplace=True)
            df = df.dropna(subset=['destination_port', '20GP', '40HC', 'origin_port'])

            df['start_date'] = get_date_format(start_date)

            df['expiry_date'] = get_date_format(expiry_date)

            df['amendment_no'] = AmendmentNo

            df['origin_port'] = df['origin_port'].replace(origin_lookup)

            df['remarks'] = remarks_str

            df['inclusions'] = inclusive

            df['subject_to'] = rates_are_subject_to

            df['destination_icd'].replace('', nan, inplace=True)

            df.loc[
                df[~df['destination_icd'].isna()].index.to_list(), 'mode_of_transportation_destination'] = 'RAMP'


            self.captured_output = {"freight" : df}

        def check_output(self):
            pass



    class Remark_(BaseFix):

        def check_input(self):
            check_errors = []

            # check_SGF = re.compile(r'(SGF):.*(USD).(\d+).(\w+)')
            # if not self.df.iloc[:, 0].apply(lambda x: bool(check_SGF.match(x))).any():
            #     check_errors.append("SGF charges not found at First Column")

            # check_PSS = re.compile(r'(PSS):.*: (\d,\d+).(USD).(\d+\w+).*(\d,\d+).(USD).(\d+\w+)')
            # if not self.df.iloc[:, 0].apply(lambda x: bool(check_PSS.match(x))).any():
            #     check_errors.append("PSS charges not found at First Column")

            if check_errors:
                raise InputValidationError(check_errors)

        def check_output(self):
            pass

        def clean(self):
            cleaned_data = []
            for i in self.surcharges_list:
                charges = {}
                if 'load_type' in i:
                    if i['load_type'].lower() == 'teu':
                        charges['20GP' + '_' + i['charge'].upper()] = int(i['amount'])
                        charges['40GP' + '_' + i['charge'].upper()] = int(i['amount']) * 2
                        charges['40HC' + '_' + i['charge'].upper()] = int(i['amount']) * 2
                        cleaned_data.append(charges)

                else:
                    charges['20GP' + '_' + i['charge'].upper()] = int(i['20GP_amount'])
                    charges['40GP' + '_' + i['charge'].upper()] = int(i['40GP_amount'])
                    charges['40HC' + '_' + i['charge'].upper()] = int(i['40GP_amount'])
                    cleaned_data.append(charges)

            self.cleaned_output = {'Charges': cleaned_data}

        def capture(self):

            df = self.df
            self.surcharges_list = []

            captured_data = df[0].str.extractall(r"(SGF):.*(USD).(\d+).(\w+)")

            if not captured_data.empty:
                captured_data = captured_data.values[0]
                SGF_surcharges = {}
                SGF_surcharges['charge'] = captured_data[0]
                SGF_surcharges['currreny_type'] = captured_data[1]
                SGF_surcharges['amount'] = captured_data[2]
                SGF_surcharges['load_type'] = captured_data[3]
                self.surcharges_list.append(SGF_surcharges)

            captured_data = df[0].str.extractall(r"(PSS):.*: (\d,\d+).(USD).(\d+\w+).*(\d,\d+).(USD).(\d+\w+)")

            if not captured_data.empty:
                captured_data = captured_data.values[0]
                PSS_surcharges = {}
                PSS_surcharges['charge'] = captured_data[0]
                PSS_surcharges['currreny_type'] = captured_data[2]
                PSS_surcharges['20GP_amount'] = captured_data[1].replace(',', '')
                PSS_surcharges['20GP_load_type'] = captured_data[3]
                PSS_surcharges['40GP_load_type'] = captured_data[6]
                PSS_surcharges['40GP_amount'] = captured_data[4].replace(',', '')
                self.surcharges_list.append(PSS_surcharges)

    @classmethod
    def resolve_dependency(cls, fix_outputs):

        china_fak = fix_outputs.pop("CHINA FAK GL")
        china_fak_freight = china_fak["Freight"]
        china_fak_arbitary = china_fak["Arbitary Charges"]

        non_china_fak = fix_outputs.pop("NON CHINA FAK GL")
        non_china_fak_freight = non_china_fak["Freight"]
        non_china_fak_arbitary = non_china_fak["Arbitary Charges"]


        freight_df = pd.concat([china_fak_freight, non_china_fak_freight], ignore_index=True)
        arbitary_df = pd.concat([china_fak_arbitary, non_china_fak_arbitary], ignore_index=True)

        remarks_surcharges = fix_outputs.pop("Remark")
        charges = remarks_surcharges["Charges"]



        for charge in charges:
            for row in charge.items():
                freight_df[row[0]] = row[1]
                arbitary_df[row[0]] = row[1]

        fix_outputs = {"Freight": freight_df, "Arbitrary Charges" : arbitary_df}

        return fix_outputs


class HLCU_TPEB_Excel(BaseTemplate):
    class _sea_freights(BaseFix):

        def check_input(self):
            check_errors = []
            if not self.df[1].str.contains('Area From', na=False).any():
                check_errors.append("Area From cannot be found on the first column, the input file "
                                    "structure is different from sample template")
            if check_errors:
                raise InputValidationError(check_errors)

        def check_output(self):
            pass

        def get_freight_table(self, columns):
            index = list(self.df[(self.df[1].str.contains("Area From", na=False))].index)
            freight_df = self.df[index[0] + 1:self.df.tail(1).index.values[0] + 1].copy(deep=True)
            freight_df.columns = columns
            freight_df['start_date'] = pd.to_datetime(freight_df['start_date']).dt.date
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date']).dt.date
            freight_df.drop(columns=['drop'], inplace=True)
            return freight_df

        def capture(self):
            columns = ['drop', 'drop', 'drop', 'origin_port', 'drop', 'drop', 'destination_port', 'drop', 'drop',
                       'mode_of_transportation_destination', 'destination_icd', 'drop', 'drop', 'drop', 'commodity',
                       'drop',
                       'drop', 'currency', '40RE', 'drop', 'drop', 'start_date', 'expiry_date', 'drop',
                       'inclusions', 'subject_to', 'drop', 'drop', 'drop', "amendment_no", "drop", "drop", "drop"]
            freight_df = self.get_freight_table(columns)
            self.captured_output = {'Freight': freight_df}

        def clean(self):
            self.cleaned_output = self.captured_output


class CEVA_Hapag_USA(BaseTemplate):
    class _sea_freights(HLCU_TPEB_Excel._sea_freights):

        def capture(self):
            columns = ['drop', 'drop', 'drop', 'origin_port', 'drop', 'drop', 'destination_port', 'drop', 'drop',
                       'commodity', 'drop', 'drop',
                       'customer_name', 'drop', 'drop', 'drop', 'drop', 'currency', '20GP', '40GP', 'drop', 'drop',
                       'start_date', 'expiry_date',
                       'drop', 'inclusions', 'subject_to', 'drop', 'drop', 'drop', "amendment_no", "drop", "drop",
                       "drop"]
            freight_df = self.get_freight_table(columns)
            freight_df['40HC'] = freight_df['40GP']
            freight_df['inclusions'] = freight_df['inclusions'].replace(',', ';', regex=True)
            freight_df['subject_to'] = freight_df['subject_to'].replace(',', ';', regex=True)
            self.captured_output = {'Freight': freight_df}

        def clean(self):
            self.cleaned_output = self.captured_output


class Ceva_Hapag_LatAm(BaseTemplate):
    class ceva_latam(BaseFix):

        def __init__(self, df, file_model: dict, headers_config: dict):
            super().__init__(df, file_model, headers_config)

            self.validity = {}
            self.load_type_map = {}

        def check_input(self):
            pass
        def check_output(self):
            pass

        def capture(self):
            freight_df=self.df[1:]
            freight_df= freight_df.reset_index(drop=True)
            freight_df = freight_df[freight_df["currency"].str.contains("Currency") == False]
            port_codes={"East Asia Base Ports" : "CNTAO,CNNGB,CNSHA,CNSHK,HKHKG,CNXMN,CNYTN,SGSIN",      #hardcoded,details obtained from image
                        "Mexico WC Base Ports" : "MXZLO,MXLZC",
                        "Mexico Gulf Base Ports" : "MXVER,MXATM",
                        "WCSA Base Ports 1 *" : "COBUN,ECGYE,PECLL,CLSAI,CLVAP,CLLQN,CLIQQ,CLCNL,CLANF,CLPAG",
                        "WCSA Base Ports 2*" : "COBUN,PECLL,CLSAI,CLVAP,CLLQN,CLIQQ,CLCNL,CLANF,CLPAG",
                        "WCSA Base Ports 3*" : "COBUN,ECGYE,PECLL,CLSAI,CLVAP,CLLQN,CLCNL,CLANF,CLPAG,CLIQQ",
                        "WCSA Base Ports 4 *": "ECGYE",
                        "WCCA Base Ports" : "SVAQJ,NICIO,CRCAL",
                        "Caribbean Base Ports" : "DOCAU,PRSJU,JMKIN,HNPCR,GTSTC,CRLIO",
                        "Caribbean BP India-ME 1" : "DOCAU/JMKJN",
                        "Caribbean BP India-ME 2" : "PRSJU/CRLIO/GTSTC/HNPCR",
                        "Panama Base Ports" : "PAROD,PAMIT,PABLB,PACFZ (via PAMIT)",
                        "Venezuela Base Ports" : "VELAG,VEPBL",
                        "Cuba Base Ports" : "CUMAR,CUSCU",
                        "TW Base Ports" : "TWKEL, TWKHH"
                        }
            lst=['20GP','40GP','40HC','40HC_NOR']
            cols = []
            count=0
            for column in freight_df.columns:
                if list(freight_df.columns).count(column) > 1:
                    column = lst[count] + '_PSS'
                    count += 1
                    cols.append(column)
                else:
                    cols.append(column)

            freight_df.columns = cols
            # freight_df.loc[freight_df['20 GP MFR'] != '', '20_MFR_inclusions'] = 'x'
            # freight_df.loc[freight_df['40 GP MFR'] != '', '40_MFR_inclusions'] = 'x'
            # freight_df.loc[freight_df['40 HC MFR'] != '', '40_HC_MFR_inclusions'] = 'x'
            # freight_df.loc[freight_df['40 HC NOR MFR'] != '', '40_HC_NOR_MFR_inclusions'] = 'x'
            # freight_df.loc[freight_df['20_GP_PSS'] != '', '20_GP_PSS_MFR_inclusions'] = 'x'
            # freight_df.loc[freight_df['40_GP_PSS'] != '', '40_GP_PSS_MFR_inclusions'] = 'x'
            # freight_df.loc[freight_df['40_HC_PSS'] != '', '40_HC_PSS_MFR_inclusions'] = 'x'
            # freight_df.loc[freight_df['40_HC_NOR_PSS'] != '', '40_HC_NOR_PSS_MFR_inclusions'] = 'x'

            freight_df['origin_icd']=freight_df['origin_icd'].replace(port_codes)
            freight_df['destination_icd']=freight_df['destination_icd'].replace(port_codes)
            freight_df['origin_icd'].replace(',',';')
            freight_df.reset_index(drop=True, inplace=True)
            # freight_df2=freight_df.drop('PSS',axis=1)
            freight_df2 = freight_df.rename(columns=lambda x: x.strip())
            freight_df1=freight_df2.copy()
            # freight_df2.loc[freight_df2['destination_icd'].str.contains('via'), 'via'] = freight_df2.loc[freight_df2['destination_icd'].str.contains('via')]['destination_icd']

            freight_df1['via'] = ''
            for i in range(freight_df1.shape[0]):
                if 'via' in freight_df1['destination_icd'][i]:
                    regex = r"\(via\s(.+?)\)"
                    matches = re.finditer(regex, freight_df1['destination_icd'][i], re.MULTILINE)
                    for matchNum, match in enumerate(matches, start=1):
                        for groupNum in range(0, len(match.groups())):
                            groupNum = groupNum + 1
                            self.group_name_ = match.group(groupNum)
                            freight_df1['via'][i] = self.group_name_
            freight_df1['destination_icd'] = freight_df1['destination_icd'].str.split(' \(',expand=True).iloc[:,0]
            freight_df1.rename(
                columns={'20_GP': '20GP', '20 GP MFR': '20GP_MFR', '20 GP_All_in': '20GP_Allin',
                         '40_GP_All_in': '40GP_Allin', '40_GP': '40GP', '40 GP MFR': '40GP_MFR', '40_HC': '40HC',
                         '40_HC_All_in': '40HC_Allin', '40 HC MFR': '40HC_MFR', '40_HC_NOR': '40HC_NOR','40HC_NOR_PSS':'40HCNOR_PSS',
                         '40 HC NOR MFR': '40HCNOR_MFR',
                         '40_HC_NOR_All_in': '40HCNOR_Allin'}, inplace=True)

            #freight_df2 = freight_df2.loc[~freight_df2['destination_icd'].str.contains('CL', na=False, case = False)]
            freight_tw_df = freight_df2.copy()
            freight_tw_df['origin_icd']='TW Base Ports'
            freight_tw_df=freight_tw_df.rename(columns=lambda x: x.strip())
            port_codes_1= {"East Asia Base Ports": "CNTAO,CNNGB,CNSHA,CNSHK,HKHKG,CNXMN,CNYTN,SGSIN",
                          # hardcoded,details obtained from image
                          "Mexico WC Base Ports": "MXZLO,MXLZC",
                          "Mexico Gulf Base Ports": "MXVER,MXATM",
                          "WCSA Base Ports 1 *" : "COBUN,ECGYE,PECLL",
                          "WCSA Base Ports 2*": "COBUN,PECLL",
                          "WCSA Base Ports 3*": "COBUN,ECGYE,PECLL",
                          "WCSA Base Ports 4 *": "ECGYE",
                          "WCCA Base Ports": "SVAQJ,NICIO,CRCAL",
                          "Caribbean Base Ports": "DOCAU,PRSJU,JMKIN,HNPCR,GTSTC,CRLIO",
                          "Caribbean BP India-ME 1": "DOCAU/JMKJN",
                          "Caribbean BP India-ME 2": "PRSJU/CRLIO/GTSTC/HNPCR",
                          "Panama Base Ports": "PAROD,PAMIT,PABLB,PACFZ (via PAMIT)",
                          "Venezuela Base Ports": "VELAG,VEPBL",
                          "Cuba Base Ports": "CUMAR,CUSCU",
                          "TW Base Ports": "TWKEL, TWKHH"
                          }
            freight_tw_df.rename(
                columns={'20_GP': '20GP', '20 GP MFR': '20GP_MFR', '20 GP_All_in': '20GP_Allin',
                         '40_GP_All_in': '40GP_Allin', '40_GP': '40GP', '40 GP MFR': '40GP_MFR', '40_HC': '40HC',
                         '40_HC_All_in': '40HC_Allin', '40 HC MFR': '40HC_MFR', '40_HC_NOR': '40HCNOR',
                         '40 HC NOR MFR': '40HCNOR_MFR','40HC_NOR_PSS':'40HCNOR_PSS',
                         '40_HC_NOR_All_in': '40HCNOR_Allin'},inplace=True)
            if freight_tw_df['origin_icd'].str.startswith('TW').any():     #hard coded,since the details were in image
                freight_tw_df['20GP_Allin']=freight_tw_df['20GP_Allin']+15
                freight_tw_df['40GP_Allin'] = freight_tw_df['40GP_Allin'] + 15
                freight_tw_df['40HC_Allin'] = freight_tw_df['40HC_Allin'] + 15
                freight_tw_df['40HCNOR_Allin'] = freight_tw_df['40HCNOR_Allin'] + 15
                freight_tw_df['origin_icd'] = freight_tw_df['origin_icd'].replace(port_codes)
                freight_tw_df['destination_icd'] = freight_tw_df['destination_icd'].replace(port_codes_1)
                freight_tw_df['via'] = ''
            for i in range(freight_tw_df.shape[0]):
                if 'via' in freight_tw_df['destination_icd'][i]:
                    regex = r"\(via\s(.+?)\)"
                    matches = re.finditer(regex, freight_tw_df['destination_icd'][i], re.MULTILINE)
                    for matchNum, match in enumerate(matches, start=1):
                        for groupNum in range(0, len(match.groups())):
                            groupNum = groupNum + 1
                            self.group_name_ = match.group(groupNum)
                            freight_tw_df['via'][i] = self.group_name_
            freight_tw_df['destination_icd'] = freight_tw_df['destination_icd'].str.split(' \(', expand=True).iloc[:,0]
            freight_tw_df['destination_icd'] = freight_tw_df['destination_icd'].replace('CL\w\w\w(,|$)', '', regex=True)
            # freight_tw_df['destination_icd']=freight_tw_df['destination_icd'].replace('',nan)
            # freight_tw_df['destination_icd']=freight_tw_df['destination_icd'].drop(nan,axis=1)
            freight_tw_df = freight_tw_df.loc[(freight_tw_df['destination_icd']!='')]
            freight_tw_df.reset_index(inplace=True, drop=True)
            df_freight=pd.concat([freight_df1, freight_tw_df], axis=0)
            df_freight.reset_index(inplace=True,drop=True)
            #df_freight=df_freight.drop(['20_GP_sea',"40_HC_SEA",'40GP_sea',"40HC_Nor_sea"],axis=1)
            #df_freight.drop('cols',axis=1)

            self.captured_output= df_freight

        def clean(self):
            self.cleaned_output=self.captured_output
            freight_df = self.cleaned_output
            self.cleaned_output = {'Freight': freight_df}
            if 'Arbitrary Charges' in self.captured_output:
                arbitrary_df = self.captured_output['Arbitrary Charges']
                self.cleaned_output['Arbitrary Charges'] = arbitrary_df


class Ceva_Hapag_AP(BaseTemplate):
    class Hapag_AP(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_freight_table(self):
            index_POL = list(self.df[(self.df[0].str.contains("Sourcing", na=False))].index)
            freight_df = self.df[index_POL[0]:self.df.tail(1).index.values[0] + 1].copy(deep=True)
            for i in range(freight_df.shape[-1]):
                if freight_df.iloc[1, i] == '':
                    freight_df.iloc[1, i] = freight_df.iloc[0, i]
            sell_index = [index for index in freight_df.iloc[0, :].index if 'Sell' in freight_df.iloc[0, index]][0]
            freight_df.columns = freight_df.iloc[1, :]
            freight_df = freight_df.iloc[2:, :]
            freight_df = pd.concat([freight_df.iloc[:, :sell_index], freight_df.iloc[:, sell_index+2:]], axis=1)
            return freight_df

        @staticmethod
        def rates_condition(freight_df):
            rates_index = list(freight_df[(freight_df["Rate Condition"].str.contains('USD', 'FGC'))].index)
            sub_to_lst = freight_df.loc[rates_index[0], 'Rate Condition']
            sub_to_dict = {}
            if freight_df["Rate Condition"].str.contains('USD', 'FGC', na=False).any():
                regex = r"[A-Z]{1,4}\s\(.+?\)"
                matches = re.finditer(regex, sub_to_lst, re.MULTILINE)
                for matchNum, match in enumerate(matches, start=1):
                    match = match.group()
                    if len(match.split()) <= 2:
                        sub_to_dict[match.split()[0]] = match.split()[-1].split('/')[0].replace('(', '')
                    else:
                        sub_to_dict[match.split()[0]] = match.split()[1].split('/')[0].replace('(', '')
            freight_df[1], freight_df[2] = freight_df['Rate Condition'].str.split('.', expand=True)[0] \
                , freight_df['Rate Condition'].str.split('.', expand=True)[1].str.split(" ", n=1, expand=True)[1]
            freight_df.loc[freight_df[1].str.contains('Include', na=False), 'inclusions'] = \
                freight_df.loc[freight_df[1].str.contains('Include', na=False)][1]
            freight_df.loc[freight_df[2].str.contains('Include', na=False), 'inclusions'] = \
                freight_df.loc[freight_df[2].str.contains('Include', na=False)][2]
            freight_df.loc[freight_df[1].str.contains('Subject', na=False), 'subject_to'] = \
                freight_df.loc[freight_df[1].str.contains('Subject', na=False)][1]
            freight_df.loc[freight_df[2].str.contains('Subject', na=False), 'subject_to'] = \
                freight_df.loc[freight_df[2].str.contains('Subject', na=False)][2]
            freight_df['inclusions'] = freight_df['inclusions'].str.split(" ", n=1, expand=True)[1].str.replace(' and ',';')
            freight_df['subject_to'] = freight_df['subject_to'].str.split(' to ',n=1, expand=True)[1].str.replace('Q1 ', '')
            freight_df['subject_to'] = freight_df['subject_to'].str.replace(', ',';')
            freight_df['subject_to'] = freight_df['subject_to'].str.replace(' and ', ';')
            index_fgc = list(freight_df.loc[(freight_df['Rate Condition'].str.contains("FGC", na=False))].index)
            freight_df.loc[index_fgc]['subject_to'] = freight_df.loc[index_fgc]['subject_to'].replace("USD","", regex=True,inplace=True)
            freight_df.loc[index_fgc,"20GP_FGC"] = sub_to_dict['FGC'].strip('USD')
            freight_df.loc[index_fgc,"40GP_FGC"] = int(sub_to_dict['FGC'].strip('USD'))
            freight_df.loc[index_fgc,"40GP_FGC"] *= 2
            freight_df.loc[index_fgc,"40HC_FGC"] = int(sub_to_dict['FGC'].strip('USD'))
            freight_df.loc[index_fgc,"40HC_FGC"] *= 2
            freight_df.loc[index_fgc,"20GP_ECC"] = sub_to_dict['ECC'].strip('USD')
            freight_df.loc[index_fgc,"40GP_ECC"] = int(sub_to_dict['ECC'].strip('USD'))
            freight_df.loc[index_fgc,"40GP_ECC"] *= 2
            freight_df.loc[index_fgc,"40HC_ECC"] = int(sub_to_dict['ECC'].strip('USD'))
            freight_df.loc[index_fgc,"40HC_ECC"] *= 2
            freight_df['subject_to'] = freight_df['subject_to'].replace('FGC \(USD50/TEU\);', '', regex=True)
            freight_df['subject_to'] = freight_df['subject_to'].replace(';ECC \(USD80/TEU Fixed\)', '', regex=True)
            return freight_df

        def capture(self):

            freight_df = self.get_freight_table()
            freight_df = self.rates_condition(freight_df)
            freight_df.drop(columns=['Sourcing','NAC/RFQ','Customer','Primary/\nSecondary/\nTertiary','DEM','DET','DEM',\
                                     'DET','Rate Reference (NAC / Bullet/ FAK)','Rate Condition',1,2,\
                                     'Expected Weekly Volume in TEU'], axis=1, inplace=True)
            freight_df.rename(columns={'BD Owner': 'customer_name', 'Start Date': 'effective_date', 'End Date':'expiry_date',\
                                        'POL':'origin_port','POD':'destination_port','Contract Reference':'contract_number',\
                                       'D20':'20GP','D40':'40GP','D40H':'40HC','20REF':'20RE','40REF':'40RE'}, inplace = True)
            self.captured_output = {"Freight": freight_df}


        def clean(self):
            self.cleaned_output = self.captured_output
