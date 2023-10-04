from logging import getLogger
from base import BaseTemplate, BaseFix
from custom_exceptions import InputValidationError
import pandas as pd
from pandas import concat
import re
from numpy import nan
import warnings
from dateutil.parser import parse
from datetime import date
from datetime import datetime

warnings.simplefilter(action='ignore', category=FutureWarning)
log = getLogger(__name__)
import locale


class Flexport_MSC_v1(BaseTemplate):
    class _USWC_v1(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_freight_table(self):
            line_item_index = list(self.df[(self.df[0] == ("Trade :"))].index)
            line_item = self.df[line_item_index[0]:line_item_index[0] + 1].iloc[0, 4]
            index = list(self.df[(self.df[0] == ("Contract Holder"))].index)
            freight_df = self.df[index[0]:self.df.tail(1).index.values[0] + 1].copy(deep=True)
            row_1 = freight_df.iloc[0]
            row_2 = freight_df.iloc[1]
            columns = []
            for i, j in zip(row_1, row_2):
                if j:
                    columns.append(j)
                else:
                    columns.append(i)
            freight_df.columns = columns
            freight_df = freight_df[2:].copy()
            if len(freight_df['PSS'].unique()[0]) > 5:
                freight_df['PSS'] = freight_df['PSS'].str.replace('USD', 'USD/')
                freight_df[['currency', '20GP_PSS', '40GP_PSS', '40HC_PSS', '45HC_PSS']] = \
                    freight_df['PSS'].str.split('/', expand=True)
                freight_df.drop(['PSS'], axis=1, inplace=True)
            freight_df['region'] = line_item
            return freight_df

        def add_inclusions_subject(self, freight_df):
            columns_ = freight_df.columns.tolist()
            for col in columns_:
                col = col.strip()
                if re.search('^[A-Z]{3}$', col) is not None:
                    if 'Yes' in freight_df[col].unique() or 'No' in freight_df[col].unique():
                        freight_df.loc[(freight_df[col] == 'Yes'), col + ' Included'] = ''
                        freight_df.loc[(freight_df[col] == 'No'), col + ' Included'] = 'X'
                        freight_df.loc[(freight_df[col] == 'NA'), col + ' Included'] = 'NA'
                        freight_df.drop(col, axis=1, inplace=True)
                    elif len(freight_df[col].unique()[0]) == 0:
                        freight_df.drop(col, axis=1, inplace=True)
            return freight_df

        @staticmethod
        def format_output(df_freight):
            output = {'Freight': df_freight}
            return output

        def capture(self):
            freight_df = self.get_freight_table()
            freight_df = self.add_inclusions_subject(freight_df)
            self.captured_output = self.format_output(freight_df)

        def clean(self):
            freight_df = self.captured_output['Freight']
            freight_df.rename(
                columns={"Commodity": 'commodity', "Port \nof Load": 'origin_port', "Place of Receipt": 'origin_icd',
                         "Port of \nDischarge": "destination_port", "Destination": "destination_icd",
                         'EXPORT/IMPORT SERVICE': 'mode_of_transportation', "20' GP": "20GP",
                         "40'GP": '40GP', "40'HC": '40HC', "45'HC": "45HC", "20'NOR": "20NOR", "20'RE": "20RE",
                         "Named Account": "customer_name", "40'RE": "40RE", "40'NOR": "40NOR", "40'HR": "40HR",
                         "ETD/Effective Date (mm/dd/yy)": 'start_date', "Reference No": "bulletin",
                         'Expiry Date (mm/dd/yy)': 'expiry_date', 'Special Notes and Comments': 'remarks',
                         "Amd #": "Amendment no.",
                         "Indicate (CY-CY, Ramp or Door - to include zip code)": 'mode_of_transportation',
                         "Service": "loop"
                         }, inplace=True)

            rename_port = {
                "USA/USEC BASE PORTS 2": "CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, "
                                         "LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USEC BASE PORTS 3": "NEW YORK/NORFOLK/CHARLESTON/SAVANNAH",
                "USA/USEC BASE PORTS 4": "BALTIMORE/HOUSTON/JACKSONVILLE/NEW ORLEANS/PHILADELPHIA",
                "USA/USEC BASE PORTS 5": "BALTIMORE, MD/CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USEC BASE PORTS 6": "CHARLESTON, SC/HOUSTON, TX/ JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, "
                                     "LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USWC BASE PORTS 1": "LONG BEACH, CA/LOS ANGELES, CA/OAKLAND, CA",
                "USA/USWC BASE PORTS 2": "LONG BEACH, CA/LOS ANGELES, CA",
                "USA/USEC BASE PORTS": "BALTIMORE, MD/CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USEC BASE PORTS 1": "BALTIMORE, MD/CHARLESTON, SC/JACKSONVILLE, FL/NEW YORK, NY/NORFOLK, VA/PHILADEPHIA, PA/SAVANNAH, GA",

                "USA/GULF BASE PORTS": "HOUSTON, TX/NEW ORLEANS, LA"

            }
            for code in rename_port:
                _code = ''.join(rename_port[code])
                freight_df.replace(code, _code, inplace=True)

            # freight_df['destination_port'].str.replace(rename_port)

            if "Vessel/Voyage" in freight_df:
                freight_df.drop(["Vessel/Voyage"], axis=1, inplace=True)
            freight_df.drop(['Contract Holder', 'A/C/D', 'Named Account Code',
                             "Surcharges as per MSC's Tariff", "Service Contract No."], axis=1, inplace=True)

            def str_replace(regex, subst, col):
                freight_df[col] = freight_df[col].str.replace(regex, subst)
                return freight_df

            col_rename = ['origin_port', 'destination_port']

            for col in col_rename:
                freight_df = str_replace(r"\/", ";", col)

            freight_df["mode_of_transportation"] = freight_df["mode_of_transportation"].str.replace("CY-CY", "")

            def fix_date(freight_df, col):
                if freight_df.loc[freight_df[col].astype(str).str.contains('222', na=False)].index.any():
                    index_date = freight_df.loc[freight_df[col].str.contains('222', na=False)].index.to_list()
                    today_date = date.today()
                    freight_df.loc[index_date, col] = \
                        freight_df.loc[index_date, col].str.extract(r'(\d{0,2}\.\d{0,2}\.)')[0] + str(today_date.year)

                return freight_df

            freight_df = fix_date(freight_df, 'start_date')
            freight_df = fix_date(freight_df, 'expiry_date')
            freight_df['start_date'] = pd.to_datetime(freight_df['start_date']).dt.strftime('%Y-%m-%d')
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date']).dt.strftime('%Y-%m-%d')
            freight_df['charges_leg'] = 'L3'
            freight_df['currency'] = 'USD'
            freight_df['charges'] = 'Basic Ocean Freight'
            self.cleaned_output = {'Freight': freight_df}

    class _USWC_arb_pre(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_freight_table(self):
            line_item_index = list(self.df[(self.df[0] == ("Trade :"))].index)
            line_item = self.df[line_item_index[0]:line_item_index[0] + 1].iloc[0, 4]
            indexes = list(self.df[(self.df[0] == ("Reference No"))].index)
            indexes.append(self.df.tail(1).index.values[0])
            arb_df_final = pd.DataFrame()
            pre_df_final = pd.DataFrame()
            for index in range(len(indexes) - 1):
                check_arb_or_pre = self.df[indexes[index] - 1:indexes[index]].copy(deep=True).iloc[0, 0]
                arb_df = self.df[indexes[index]:indexes[index + 1] - 1].copy(deep=True)
                arb_df.reset_index(drop=True, inplace=True)
                arb_df.drop(1, axis=0, inplace=True)
                arb_df.columns = arb_df.iloc[0]
                arb_df = arb_df[1:].copy()
                if not arb_df.loc[(arb_df['Special Notes and Comments'] == 'USWC')].empty:
                    arb_df = arb_df.loc[(arb_df['Special Notes and Comments'] == 'USWC')]

                arb_df = arb_df.loc[~(arb_df["20' GP"] == "Suspended until further notice")]
                arb_df['Reference No'] = arb_df['Reference No'].astype(str)
                notes = arb_df.loc[(arb_df['Origins'] == '')]
                indexes_drop = arb_df[(arb_df['Origins'] == '')].index.tolist()
                notes = r'\n'.join(notes['Reference No'])
                arb_df['remarks'] = notes
                arb_df['remarks'] = arb_df['Special Notes and Comments'].str.cat(arb_df['remarks'])
                arb_df['region'] = line_item
                arb_df.loc[arb_df['Origins'].str.contains(
                    'on top of Japan'), 'POL'] = "YOKOHAMA/NAGOYA/OSAKA/KOBE/HAKATA/TOKYO"
                arb_df.loc[arb_df['Origins'].str.contains(
                    'on top of Kao'), 'POL'] = "KAOHSIUNG"
                arb_df.drop(indexes_drop, inplace=True)
                if 'Effective Date' in arb_df:
                    arb_df.rename(
                        columns={"Effective Date": 'Valid from', "Expiry Date": 'Valid To',
                                 }, inplace=True)
                if re.search('Pre-Carriage', str(check_arb_or_pre)) is not None:
                    arb_df['charges_leg'] = 'L3'
                    arb_df['charges'] = 'basic ocean freight charge at pre-carriage'
                    pre_df_final = pd.concat([pre_df_final, arb_df], ignore_index=True)
                else:
                    arb_df['charges'] = 'origin arbitrary charges'
                    arb_df['charges_leg'] = 'L2'
                    arb_df['at'] = 'origin'
                    arb_df_final = pd.concat([arb_df_final, arb_df], ignore_index=True)

            replace_dict={r"\s?\(Add on top of .*\)":"",}
            pre_df_final['POL'].replace("on top of ", "", inplace=True,regex=True)
            pre_df_final['POL'].replace(replace_dict,inplace=True,regex=True)
            pre_df_final['Origins'].replace(replace_dict,inplace=True,regex=True)
            pre_df_final['Origins'].replace(" - Old Port","",inplace=True,regex=True)
            arb_df_final['Origins'].replace(" - Old Port","",inplace=True,regex=True)
            arb_df_final['Origins'].replace("Old Port", "", inplace=True, regex=True)
            pre_df_final['Origins'].replace("Add on top of", "", inplace=True, regex=True)
            pre_df_final['Origins'].replace("on top of","",inplace=True, regex=True)
            return pre_df_final, arb_df_final

        @staticmethod
        def format_output(arb_df_final, pre_df_final=None):
            if pre_df_final.empty:
                output = {'Freight': pre_df_final}
            else:
                output = {'Freight': pre_df_final, 'Arbitrary Charges': arb_df_final}
            return output

        def capture(self):
            pre_df_final, arb_df_final = self.get_freight_table()
            self.captured_output = self.format_output(arb_df_final, pre_df_final)

        def clean(self):
            freight_df = self.captured_output['Freight']
            freight_df.rename(
                columns={"POL": 'destination_port', "Origins": 'origin_port',
                         "20' GP": "20GP", "40'GP": '40GP', "40'HC": '40HC', "45'HC": "45HC",
                         "MOT": "mode_of_transportation_origin", "Valid from": 'start_date',
                         "Reference No": "bulletin", 'Valid To': 'expiry_date', "Service": "loop"
                         }, inplace=True)

            rename_port = {
                "USA/USEC BASE PORTS 2": "CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, "
                                         "LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USEC BASE PORTS 3": "NEW YORK/NORFOLK/CHARLESTON/SAVANNAH",
                "USA/USEC BASE PORTS 4": "BALTIMORE/HOUSTON/JACKSONVILLE/NEW ORLEANS/PHILADELPHIA",
                "USA/USEC BASE PORTS 5": "BALTIMORE, MD/CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USEC BASE PORTS 6": "CHARLESTON, SC/HOUSTON, TX/ JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, "
                                         "LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USWC BASE PORTS 1": "LONG BEACH, CA/LOS ANGELES, CA/OAKLAND, CA",
                "USA/USWC BASE PORTS 2": "LONG BEACH, CA/LOS ANGELES, CA",
                "USA/USEC BASE PORTS": "BALTIMORE, MD/CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USEC BASE PORTS 1": "BALTIMORE, MD/CHARLESTON, SC/JACKSONVILLE, FL/NEW YORK, NY/NORFOLK, VA/PHILADEPHIA, PA/SAVANNAH, GA",

                "USA/GULF BASE PORTS": "HOUSTON, TX/NEW ORLEANS, LA"

                }

            for code in rename_port:
                _code = ';'.join(rename_port[code])
                freight_df.replace(code, _code, inplace=True)

            # freight_df['destination_port'].str.replace(rename_port)

            freight_df.drop(["CY/Door within city limit"], axis=1, inplace=True)

            def str_replace(regex, subst, col):
                freight_df[col] = freight_df[col].str.replace(regex, subst)
                return freight_df

            col_rename = ['origin_port']

            for col in col_rename:
                freight_df = str_replace(r"\/", ";", col)

            freight_df['currency'] = 'USD'

            def fix_date(freight_df, col):
                if freight_df.loc[freight_df[col].astype(str).str.contains('222', na=False)].index.any():
                    index_date = freight_df.loc[freight_df[col].str.contains('222', na=False)].index.to_list()
                    today_date = date.today()
                    freight_df.loc[index_date, col] = \
                        freight_df.loc[index_date, col].str.extract(r'(\d{0,2}\.\d{0,2}\.)')[0] + str(today_date.year)

                return freight_df

            freight_df = fix_date(freight_df, 'start_date')

            freight_df = fix_date(freight_df, 'expiry_date')

            freight_df['start_date'] = pd.to_datetime(freight_df['start_date']).dt.strftime('%Y-%m-%d')
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date']).dt.strftime('%Y-%m-%d')

            arb_df = self.captured_output['Arbitrary Charges']
            arb_df.rename(
                columns={"POL": 'via', "Origins": 'icd',
                         "20' GP": "20GP", "40'GP": '40GP', "40'HC": '40HC', "45'HC": "45HC",
                         "MOT": "mode_of_transportation_origin", "Valid from": 'start_date',
                         "Reference No": "bulletin", 'Valid To': 'expiry_date', "Service": "loop"
                         }, inplace=True)
            arb_df.drop(["CY/Door within city limit"], axis=1, inplace=True)

            def str_replace(regex, subst, col):
                arb_df[col] = arb_df[col].str.replace(regex, subst)
                return arb_df

            col_rename = ['icd']

            for col in col_rename:
                arb_df = str_replace(r"\/", ";", col)

            def fix_date(freight_df, col):
                if freight_df.loc[freight_df[col].astype(str).str.contains('222', na=False)].index.any():
                    index_date = freight_df.loc[freight_df[col].str.contains('222', na=False)].index.to_list()
                    today_date = date.today()
                    freight_df.loc[index_date, col] = \
                        freight_df.loc[index_date, col].str.extract(r'(\d{0,2}\.\d{0,2}\.)')[0] + str(today_date.year)

                return freight_df

            arb_df = fix_date(arb_df, 'start_date')

            arb_df = fix_date(arb_df, 'expiry_date')

            arb_df['start_date'] = pd.to_datetime(arb_df['start_date']).dt.strftime('%Y-%m-%d')
            arb_df['expiry_date'] = pd.to_datetime(arb_df['expiry_date']).dt.strftime('%Y-%m-%d')

            arb_df['currency'] = 'USD'
            self.cleaned_output = {'Freight': freight_df, 'Arbitrary Charges': arb_df}

    class _DiamondTier(_USWC_v1):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):
            freight_df = self.get_freight_table()
            freight_df = self.add_inclusions_subject(freight_df)
            self.captured_output = self.format_output(freight_df)

        def clean(self):
            freight_df = self.captured_output['Freight']
            freight_df.loc[freight_df["Service Contract No."].str.contains("DT", na=False),
                           "premium_service"] = "MSC_diamond"

            freight_df.rename(
                columns={"Commodity": 'commodity', "Port \nof Load": 'origin_port', "Place of Receipt": 'origin_icd',
                         "Port of \nDischarge": "destination_port", "Destination": "destination_icd",
                         'EXPORT/IMPORT SERVICE': 'mode_of_transportation', "20' GP": "20GP",
                         "40'GP": '40GP', "40'HC": '40HC', "45'HC": "45HC", "20'NOR": "20NOR", "20'RE": "20RE",
                         "Named Account": "customer_name", "40'RE": "40RE", "40'NOR": "40NOR", "40'HR": "40HR",
                         "ETD/Effective Date (mm/dd/yy)": 'start_date', "Reference No": "bulletin",
                         'Expiry Date (mm/dd/yy)': 'expiry_date', 'Special Notes and Comments': 'remarks',
                         "Amd #": "Amendment no.",
                         "Indicate (CY-CY, Ramp or Door - to include zip code)": 'mode_of_transportation',
                         "Service": "loop"
                         }, inplace=True)

            rename_port = {
                "USA/USEC BASE PORTS 2": "CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, "
                                         "LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USEC BASE PORTS 3": "NEW YORK/NORFOLK/CHARLESTON/SAVANNAH",
                "USA/USEC BASE PORTS 4": "BALTIMORE/HOUSTON/JACKSONVILLE/NEW ORLEANS/PHILADELPHIA",
                "USA/USEC BASE PORTS 5": "BALTIMORE, MD/CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USEC BASE PORTS 6": "CHARLESTON, SC/HOUSTON, TX/ JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, "
                                     "LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USWC BASE PORTS 1": "LONG BEACH, CA/LOS ANGELES, CA/OAKLAND, CA",
                "USA/USWC BASE PORTS 2": "LONG BEACH, CA/LOS ANGELES, CA",
                "USA/USEC BASE PORTS": "BALTIMORE, MD/CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USEC BASE PORTS 1": "BALTIMORE, MD/CHARLESTON, SC/JACKSONVILLE, FL/NEW YORK, NY/NORFOLK, VA/PHILADEPHIA, PA/SAVANNAH, GA",

                "USA/GULF BASE PORTS": "HOUSTON, TX/NEW ORLEANS, LA"

            }
            for code in rename_port:
                _code = ''.join(rename_port[code])
                freight_df.replace(code, _code, inplace=True)

            # freight_df['destination_port'].str.replace(rename_port)

            if "Vessel/Voyage" in freight_df:
                freight_df.drop(["Vessel/Voyage"], axis=1, inplace=True)
            freight_df.drop(['Contract Holder', 'A/C/D', 'Named Account Code',
                             "Surcharges as per MSC's Tariff", "Service Contract No."], axis=1, inplace=True)

            def str_replace(regex, subst, col):
                freight_df[col] = freight_df[col].str.replace(regex, subst)
                return freight_df

            col_rename = ['origin_port', 'destination_port']

            for col in col_rename:
                freight_df = str_replace(r"\/", ";", col)

            freight_df["mode_of_transportation"] = freight_df["mode_of_transportation"].str.replace("CY-CY", "")

            def fix_date(freight_df, col):
                if freight_df.loc[freight_df[col].astype(str).str.contains('222', na=False)].index.any():
                    index_date = freight_df.loc[freight_df[col].str.contains('222', na=False)].index.to_list()
                    today_date = date.today()
                    freight_df.loc[index_date, col] = \
                        freight_df.loc[index_date, col].str.extract(r'(\d{0,2}\.\d{0,2}\.)')[0] + str(today_date.year)

                return freight_df

            freight_df = fix_date(freight_df, 'start_date')

            freight_df = fix_date(freight_df, 'expiry_date')

            freight_df['start_date'] = pd.to_datetime(freight_df['start_date']).dt.strftime('%Y-%m-%d')
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date']).dt.strftime('%Y-%m-%d')
            freight_df['charges_leg'] = 'L3'
            freight_df['currency'] = 'USD'
            freight_df['charges'] = 'Basic Ocean Freight'
            self.cleaned_output = {'Freight': freight_df}

    class _Reference(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_reference_dict(self):
            reference_df = self.df
            reference_df.columns = reference_df.iloc[0]
            reference_df = reference_df[1:].copy()
            reference_df.drop(columns=['Codes'], inplace=True)
            origin_df = reference_df.iloc[:, :2].copy()
            destination_df = reference_df.iloc[:, 2:].copy()
            destination_df.dropna(how='any', axis=0, inplace=True)
            origin_df.dropna(how='any', axis=0, inplace=True)
            destination_code_dict = destination_df.set_index('Port of Discharge').to_dict()['Location']
            origin_code_dict = origin_df.set_index('Port of Load').to_dict()['Location']

            return {'origin': origin_code_dict, 'destination': destination_code_dict}

        def capture(self):
            ref_dict = self.get_reference_dict()
            self.captured_output = ref_dict

        def clean(self):
            self.cleaned_output = self.captured_output

    @classmethod
    def resolve_dependency(cls, fix_outputs):
        ref = fix_outputs.pop('Reference Tables')
        origin_dict = ref['origin']
        destination_dict = ref['destination']

        dict_new = {"SEA BASE PORTS": "INDONESIA/MALAYSIA/SINGAPORE/THAILAND/VIETNAM",
                    "NPRC": "SHANGHAI/NINGBO/QINGDAO/XINGANG/DALIAN",
                    "SPRC": "HONG KONG/CHIWAN/YANTIAN/XIAMEN/FUZHOU/SHEKOU",
                    "USA;USWC IPI": "LONG BEACH, CA/LOS ANGELES, CA/OAKLAND, CA"
                    }

        origin_dict.update(dict_new)
        destination_dict.update(dict_new)

        for sheet, df_dict in fix_outputs.items():
            for leg, df in df_dict.items():
                if leg == 'Freight':
                    df['origin_port'].replace(origin_dict, regex=True, inplace=True)
                    df['destination_port'].replace(destination_dict, regex=True, inplace=True)
                    df['destination_port'].replace('/', ';', regex=True, inplace=True)
                    df['origin_port'].replace('/', ';', regex=True, inplace=True)
                    fix_outputs[sheet][leg] = df

                if leg == 'Arbitrary Charges':
                    df['icd'].replace(origin_dict, regex=True, inplace=True)
                    df['via'].replace(destination_dict, regex=True, inplace=True)
                    df['icd'].replace('/', ';', regex=True, inplace=True)
                    df['via'].replace('/', ';', regex=True, inplace=True)
                    fix_outputs[sheet][leg] = df
        return fix_outputs


class Expedoc_MSC_v1(BaseTemplate):
    class BAS_Fix(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_customer_name(self):
            customer_name_df = self.df[self.df.iloc[:, 0] == "NAC"]
            customer_name = ""
            if not customer_name_df.empty:
                customer_name = customer_name_df.iloc[:, :3].to_string(index=False, header=None).replace("NAC", "")
            return customer_name

        def capture(self):
            df = self.df
            customer_name = self.get_customer_name()
            nan_value = float("NaN")
            df.replace("", nan_value, inplace=True)
            df = df.dropna(subset=["origin_port", "destination_port"])
            df = df[df.origin_port.str.lower() != 'origin port']

            df["customer_name"] = customer_name
            df["destination_icd"] = ""
            df["origin_icd"] = ""

            self.df = df
            # df.pivot_table(values=["20GP", "40GP", "40HC"],
            #                columns=["region", "origin_port", "region", "commodity", "currency", "start_date", "expiry",
            #                         "inclusions", "subject_to", "20GP", "40GP", "40HC"])

        def clean(self):
            df = self.df
            df["charges"] = "Basic Ocean Freight"
            df["basis"] = "PER CONTAINER"
            df["charges_leg"] = "L3"
            # df = df.melt(
            #     id_vars=["destination_port", "origin_port", "region", "commodity", "currency",
            #              "basis", "charges", "inclusions", "subject_to" , "start_date", "expiry"],
            #     var_name="load_type",
            #     value_name="amount")
            #
            self.df = df
            self.cleaned_output = {'Freight': self.df}

    class China_Inlands_Fix(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        @classmethod
        def fix_over_block(cls, block):

            def get_remarks(data_input):
                return re.search(r"(Euro.*)", data_input)

            def get_validity_date(date_input):
                return re.search(r"(.*)\still\s(.*)", date_input)

            captured_validity = block.iloc[:, 2].apply(lambda x: get_validity_date(str(x)))
            start_date, expiry_date = "", ""
            for i in captured_validity:
                if i:
                    start_date_group = i.group(1)
                    st_dt = parse(start_date_group)
                    start_date = st_dt.strftime('%d-%m-%Y')
                    expiry_date_group = i.group(2)
                    exp_dt = parse(expiry_date_group)
                    expiry_date = exp_dt.strftime('%d-%m-%Y')

            if start_date == "" and expiry_date == "":
                captured_validity = block.iloc[:, 3].apply(lambda x: get_validity_date(str(x)))
                for i in captured_validity:
                    if i:
                        start_date_group = i.group(1)
                        st_dt = parse(start_date_group)
                        start_date = st_dt.strftime('%d-%m-%Y')
                        expiry_date_group = i.group(2)
                        exp_dt = parse(expiry_date_group)
                        expiry_date = exp_dt.strftime('%d-%m-%Y')

            remarks_captured = block.iloc[:, 9].apply(lambda x: get_remarks(str(x)))
            remarks_ = ''
            for i in remarks_captured:
                if i:
                    remarks_ = i.group(1)

            block_start_indexes = block[(block.iloc[:, 0].str.lower() == 'origins')].index[0]
            block_df = block.loc[int(block_start_indexes):, :]

            if len(block_df.columns) == 11:
                new_header = block_df.iloc[0]
                block_df = block_df[1:]
                block_df.columns = new_header
                block_df.rename(columns={"Origins": "icd",
                                         "Province": "provice",
                                         "via  (Rates to be added on top of)": "to",
                                         "20dv": "20GP", "40dv": "40GP", "40hc": "40HC"}, inplace=True)

                block_df = block_df.drop(columns=[column for column in block_df.columns if column.startswith('drop')])

                nan_value = float("NaN")
                block_df.replace("", nan_value, inplace=True)

                find_empty_column = block_df.columns.get_loc("")
                find_empty_column_loc = [i for i, x in enumerate(find_empty_column) if x]

                for rowIndex, row in block_df.iterrows():

                    BY_RAIL = row.str.contains("RAIL", case=False, flags=0, na=None, regex=True).any()
                    if BY_RAIL:
                        block_df.loc[int(rowIndex), 'mode_of_transportion'] = "RAIL"

                    By_Truck = row.str.contains("Truck", case=False, flags=0, na=None, regex=True).any()
                    if By_Truck:
                        block_df.loc[int(rowIndex), 'mode_of_transportion'] = "Truck"

                    By_Barge = row.str.contains("Barge", case=False, flags=0, na=None, regex=True).any()
                    if By_Barge:
                        block_df.loc[int(rowIndex), 'mode_of_transportion'] = "Barge"

                    Dg_cargo = row.str.contains("dg", case=False, flags=0, na=None, regex=True).any()
                    if Dg_cargo:
                        block_df.loc[int(rowIndex), 'CargoNature'] = "DG cargo"

                block_df.drop(block_df.columns[find_empty_column_loc], axis=1, inplace=True)

                remove_euro_rates = block_df.columns.get_loc("40HC")

                remove_euro_rates_loc = [i for i, x in enumerate(remove_euro_rates) if x]
                remove_euro_rates_start = remove_euro_rates_loc[0] + 1
                remove_euro_rates_end = remove_euro_rates_loc[1] + 1

                range_to_delete = [*range(remove_euro_rates_start, remove_euro_rates_end)]

                cols = block_df.columns.to_series()
                # Two  port columns there in sheet
                for index in range_to_delete:
                    if cols.iloc[index] in ["40HC", "20GP", "40GP"]:
                        cols.iloc[index] = 'drop_column'
                        block_df.columns = cols

                block_df.drop(columns='drop_column', inplace=True)

                block_df["currency"] = "USD"
                remarks_index = block_df[block_df[["icd", "to"]].isnull().values].index.to_list()
                block_df = block_df.reset_index(drop=True)

                block_df = block_df.dropna(subset=["icd", "to"])

            remarks_df = block.loc[remarks_index]
            remarks = remarks_df[0].to_string(header=None, index=False)

            if "rail" in remarks.lower():
                block_df.loc[block_df['mode_of_transportion'].isnull(), 'mode_of_transportion'] = 'rail'

            if "truck" in remarks.lower():
                block_df.loc[block_df['mode_of_transportion'].isnull(), 'mode_of_transportion'] = 'Truck'

            if "barge" in remarks.lower():
                block_df.loc[block_df['mode_of_transportion'].isnull(), 'mode_of_transportion'] = 'Barge'

            remarks = remarks_ + "\n" + remarks

            block_df["remarks"] = remarks
            block_df["start_date"] = start_date
            block_df["expiry_date"] = expiry_date
            reeger_list = block_df.loc[
                block_df["icd"].str.contains("Reefer", case=False, flags=0, na=None, regex=True)].index

            if reeger_list.any():
                for rowIndex in reeger_list:
                    row = block_df.iloc[rowIndex]
                    block_df.loc[int(rowIndex), ('20RE', "40RE")] = row["20GP"], row["40GP"]
            return block_df

        def get_pol_sections(self):
            regional_sections = {}
            indexes = self.df[self.df[0].str.startswith("POL")].index.tolist()
            indexes.append(len(self.df))
            indexes = zip(indexes, indexes[1:])

            for config in indexes:
                region = self.df[0][config[0]]
                if not region in regional_sections.keys():
                    regional_sections[region] = {'start': config[0], 'end': config[1]}
                else:
                    regional_sections[region + "*"] = {'start': config[0], 'end': config[1]}

            return regional_sections

        def get_inland(self):
            regional_sections = self.get_pol_sections()
            dfs = []
            for region, regional_config in regional_sections.items():
                region = region.replace("POL ", "")
                regional_df = self.df.loc[regional_config['start']:regional_config['end'] - 1, :]
                regional_df = regional_df.T.reset_index(drop=True).T
                commodity_df = self.fix_over_block(regional_df)
                if "provice" not in commodity_df:
                    commodity_df["provice"] = ""

                commodity_df['to'] = region
                dfs.append(commodity_df)

            df = concat(dfs, ignore_index=True, sort=False)
            df['charges'] = 'Origin Arbitrary Charges'
            df["unique"] = "China Inlands - North Central"
            df["at"] = "origin"
            df["charges_leg"] = "L2"

            return df

        def capture(self):
            arbitary = self.get_inland()
            self.df = arbitary

        def clean(self):
            self.df["remarks"] = self.df["remarks"].str.replace(r"Series\(\[\], \)", "", regex=True)

            self.cleaned_output = {'Arbitrary': self.df}

    class SouthChina_Inland_Fix(BaseFix):

        def check_input(self):
            # self.df.iloc[:, 8].str.contains("Euro for POD").any()
            pass

        def check_output(self):
            pass

        @classmethod
        def fix_over_block(cls, block):
            def get_remarks(data_input):
                return re.search(r"(Euro.*)", data_input)

            def get_validity_date(date_input):
                return re.search(r"(.*)\still\s(.*)", date_input)

            captured_validity = block.iloc[:, 2].apply(lambda x: get_validity_date(str(x)))
            start_date = ""
            expiry_date = ""
            for i in captured_validity:
                if i:
                    start_date_group = i.group(1)
                    st_dt = parse(start_date_group)
                    start_date = st_dt.strftime('%d-%m-%Y')
                    expiry_date_group = i.group(2)
                    exp_dt = parse(expiry_date_group)
                    expiry_date = exp_dt.strftime('%d-%m-%Y')

            remarks_captured = block.iloc[:, 8].apply(lambda x: get_remarks(str(x)))
            remarks_ = ''
            for i in remarks_captured:
                if i:
                    remarks_ = i.group(1)

            block_start_indexes = block[(block.iloc[:, 0].str.lower() == 'origins')].index[0]

            block_df = block.loc[int(block_start_indexes):, :]
            if len(block_df.columns) == 9:
                new_header = block_df.iloc[0]
                block_df = block_df[1:]
                block_df.columns = new_header
                block_df.rename(columns={"Origins": "icd",
                                         "Province": "provice",
                                         "via  (Rates to be added on top of)": "to",
                                         "20dv": "20GP", "40dv": "40GP", "40hc": "40HC", "": "drop1"}, inplace=True)

                block_df = block_df.loc[block_df.drop1.str.lower() != "port closed"]

                nan_value = float("NaN")
                block_df.replace("", nan_value, inplace=True)

                for rowIndex, row in block_df.iterrows():
                    BY_RAIL = row.str.contains("RAIL", case=False, flags=0, na=None, regex=True).any()
                    if BY_RAIL:
                        block_df.loc[int(rowIndex), 'mode_of_transportion'] = "RAIL"

                    By_Truck = row.str.contains("Truck", case=False, flags=0, na=None, regex=True).any()
                    if By_Truck:
                        block_df.loc[int(rowIndex), 'mode_of_transportion'] = "Truck"

                    By_Barge = row.str.contains("Barge", case=False, flags=0, na=None, regex=True).any()
                    if By_Barge:
                        block_df.loc[int(rowIndex), 'mode_of_transportion'] = "Barge"

                    Dg_cargo = row.str.contains("dg", case=False, flags=0, na=None, regex=True).any()
                    if Dg_cargo:
                        block_df.loc[int(rowIndex), 'CargoNature'] = "DG cargo"
                    # block_df.drop(block_df.columns[find_empty_column_loc], axis=1, inplace=True)

                remove_euro_rates = block_df.columns.get_loc("40HC")

                remove_euro_rates_loc = [i for i, x in enumerate(remove_euro_rates) if x]
                remove_euro_rates_start = remove_euro_rates_loc[0] + 1
                remove_euro_rates_end = remove_euro_rates_loc[1] + 1

                range_to_delete = [*range(remove_euro_rates_start, remove_euro_rates_end)]

                cols = block_df.columns.to_series()
                # Two  port columns there in sheet
                for index in range_to_delete:
                    if cols.iloc[index] in ["40HC", "20GP", "40GP"]:
                        cols.iloc[index] = 'drop_column'
                        block_df.columns = cols

                block_df.drop(columns='drop_column', inplace=True)
                block_df = block_df.drop(columns=[column for column in block_df.columns if column.startswith('drop')])

                remarks_index = block_df[block_df[["icd", "to"]].isnull().values].index.to_list()
                block_df = block_df.reset_index(drop=True)

                block_df = block_df.dropna(subset=["icd", "to"])

            remarks = ""
            if len(remarks_index) > 1:
                remarks_index = remarks_index[:-1]
                remarks_df = block.loc[remarks_index]
                remarks = remarks_df[0].to_string(header=None, index=False)

            remarks = remarks_ + "\n" + remarks
            block_df["remarks"] = remarks
            block_df["start_date"] = start_date
            block_df["expiry_date"] = expiry_date

            reeger_list = block_df.loc[
                block_df["icd"].str.contains("Reefer", case=False, flags=0, na=None, regex=True)].index

            if reeger_list.any():
                for rowIndex in reeger_list:
                    row = block_df.iloc[rowIndex]
                    block_df.loc[int(rowIndex), ('20RE', "40RE")] = row["20GP"], row["40GP"]

            return block_df

        def get_international_sectional(self, domestic_df):
            regional_sections = {}
            if len(domestic_df.columns) == 9:
                domestic_df.columns = [0, 1, 2, 3, 4, 5, 6, 7, 8]
            indexes = domestic_df[domestic_df[0].str.startswith("INTERNATIONAL FEEDER")].index.tolist()
            indexes.append(len(domestic_df))

            indexes = zip(indexes, indexes[1:])

            for config in indexes:
                region = domestic_df[0][config[0]]
                regional_sections[region] = {'start': config[0], 'end': config[1]}

            return regional_sections

        def get_domestic_sectional(self, domestic_df):
            regional_sections = {}
            indexes = domestic_df[domestic_df[0].str.startswith("Intermodal")].index.tolist()
            indexes.append(len(domestic_df))

            indexes = zip(indexes, indexes[1:])

            for config in indexes:
                region = domestic_df[0][config[0]]
                regional_sections[region] = {'start': config[0], 'end': config[1]}

            return regional_sections

        def get_pol_sections(self, pol_df):
            regional_sections = {}
            indexes = pol_df[pol_df[0].str.contains("POL")].index.tolist()
            # indexes.append(len(pol_df))
            indexes.append(pol_df.iloc[[-1]].index[0])
            indexes = zip(indexes, indexes[1:])

            for config in indexes:
                region = self.df[0][config[0]]
                regional_sections[region] = {'start': config[0], 'end': config[1]}

            return regional_sections

        def get_international(self, domestic_df, domestic_sectional):
            dfs = []
            for region, regional_config in domestic_sectional.items():
                region = region
                regional_df = domestic_df.loc[regional_config['start']:regional_config['end'] - 1, :]
                pol_sections = self.get_pol_sections(regional_df)
                for pol, pol_config in pol_sections.items():
                    if "POL".lower() in pol.lower():
                        charges = "Origin Arbitrary Charges"

                    pol = region.split("POL")[1]

                    pol_df = domestic_df.loc[pol_config['start']:pol_config['end'] - 1, :]
                    commodity_df = self.fix_over_block(pol_df)
                    mode_of_transportion = region
                    commodity_df['mode_of_transportion'] = mode_of_transportion.replace("FOR", "")

                    commodity_df['to'] = pol
                    commodity_df["currency"] = "USD"
                    dfs.append(commodity_df)

            df = concat(dfs, ignore_index=True, sort=False)
            nan_value = float("NaN")
            df.replace("", nan_value, inplace=True)

            df["start_date"] = df["start_date"].fillna(method='ffill')
            df["expiry_date"] = df["expiry_date"].fillna(method='ffill')
            df['charges'] = charges

            return df

        def get_domestic(self, domestic_df, domestic_sectional):
            dfs = []
            for region, regional_config in domestic_sectional.items():
                region = region.split(":")[1]
                regional_df = domestic_df.loc[regional_config['start']:regional_config['end'] - 1, :]
                pol_sections = self.get_pol_sections(regional_df)
                for pol, pol_config in pol_sections.items():
                    if "POL".lower() in pol.lower():
                        charges = "Origin Arbitrary Charges"

                    pol = pol.split("POL")[1]
                    pol_df = domestic_df.loc[pol_config['start']:pol_config['end'], :]
                    commodity_df = self.fix_over_block(pol_df)

                    commodity_df['mode_of_transportion'] = region
                    commodity_df['to'] = pol
                    commodity_df["currency"] = "USD"
                    dfs.append(commodity_df)

            df = concat(dfs, ignore_index=True, sort=False)

            nan_value = float("NaN")
            df.replace("", nan_value, inplace=True)
            df["start_date"] = df["start_date"].fillna(method='bfill')
            df["expiry_date"] = df["expiry_date"].fillna(method='bfill')

            df['charges'] = charges
            return df

        def get_split_sections(self):
            if self.df.iloc[:, 8].str.contains("Euro for POD").any():
                domestic_df = self.df.iloc[:, :9]
                international_df = self.df.iloc[:, 9:]
                domestic_sectional = self.get_domestic_sectional(domestic_df)
                domestic_result = self.get_domestic(domestic_df, domestic_sectional)
                international_sectional = self.get_international_sectional(international_df)
                international_result = self.get_international(international_df, international_sectional)
                df = concat([domestic_result, international_result], ignore_index=True, sort=False)
                df.loc[df['charges'].str.lower() == "origin arbitrary charges", ('at', 'charges_leg')] = 'origin', 'L2'
                df.loc[df['charges'].str.lower() == "destination arbitrary charges", (
                    'at', 'charges_leg')] = 'destination', 'L4'
                df["unique"] = "South China Inland"

                return df

        def capture(self):
            self.df = self.get_split_sections()

            pass

        def clean(self):
            self.df["remarks"] = self.df["remarks"].str.replace(r"Series\(\[\], \)", "", regex=True)

            self.df["mode_of_transportion"].replace({"service": ""}, regex=True, inplace=True)
            self.cleaned_output = {'Arbitrary': self.df}

    class Europe_Destination_TAD_Fix(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):
            df = self.df

            def get_start_validity_date(date_input):
                return re.search(r"Updated:(.*)", date_input)

            captured_start_validity = df.iloc[:, 0].apply(lambda x: get_start_validity_date(str(x)))
            start_date = ""
            expiry_date = ""
            for i in captured_start_validity:
                if i:
                    start_date_group = i.group(1)
                    st_dt = parse(start_date_group)
                    start_date = st_dt.strftime('%d-%m-%Y')

            def get_expiry_validity_date(date_input):
                return re.search(r"Validity:\sAs from(.*)", date_input)

            captured_expiry_validity = df.iloc[:, 0].apply(lambda x: get_expiry_validity_date(str(x)))
            for i in captured_expiry_validity:
                if i:
                    expiry_date_group = i.group(1)
                    et_dt = parse(expiry_date_group)
                    expiry_date = et_dt.strftime('%d-%m-%Y')

            indexes = df[df[0].str.startswith("TADs to be applied on top of these Base Port Rates")].index[0]

            arbitary_df = df[int(indexes):]
            arbitary_df.columns = ["Base Ports", "to", "base_port", "icd", "to_location", "region", "20GP", "40GP",
                                   "40HC", "base_ports", "remarks_with_date"]
            arbitary_df.dropna(subset=['Base Ports', "to"])
            arbitary_df = arbitary_df[arbitary_df.icd.str.lower() != 'pod']
            arbitary_df = arbitary_df[["icd", "to", "20GP", "40GP", "40HC"]]

            arbitary_df["currency"] = "USD"

            arbitary_df["start_date"] = start_date
            arbitary_df["expiry_date"] = expiry_date
            arbitary_df["unique"] = "Europe Destination TAD"
            arbitary_df["charges"] = "Destination Arbitrary Charges"
            arbitary_df["mode_of_transportion"] = ""
            arbitary_df["remarks"] = ""

            arbitary_df.loc[arbitary_df['charges'].str.lower() == "destination arbitrary charges", (
                'at', 'charges_leg')] = 'destination', 'L4'

            self.df = arbitary_df

        def clean(self):

            look_up = {
                "DE": "DEHAM;DEBRE",
                "GB": "GBLGP;GBLON;GBSOU",
                "FR": "FRLEH;FRMRS;FRDKK",
                "NL": "NLRTM",
                "IE": "IEDUB",
                "BE": "BEANR",
                "NO": "NOAES;NOBGO;NOOSL",
                "DK": "DKAAR",
                "SE": "SEGOT",
                "PL": "PLGDY",
                "FI": "FIHEL",
                "RU": "RULED",
                "LT": "LTKLJ",
                "LV": "LVRIX",
                "EE": "EETLL"
            }

            df = self.df.loc[self.df["20GP"] != 0]

            df["icd"] = df["to"].str[:2]
            df["icd"] = df["icd"].map(look_up)
            df["icd"] = df["icd"].str.split(";")

            self.df = df.explode("icd")

            self.cleaned_output = {'Arbitrary': self.df}

    class Surcharges_Fix(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):
            df = self.df
            df = df[df["surcharge_description"] != 'Surcharge Description']
            nan_value = float("NaN")
            df.replace("", nan_value, inplace=True)
            df = df.dropna(subset=["surcharge_description", "K+N_Abbreviation"])
            self.df = df

        def clean(self):
            self.cleaned_output = {'surcharge': self.df}

    class HAZ_surcharges_Fix(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):
            df = self.df
            cols = df.columns.to_series()
            cols.iloc[0] = "Hazardous_type"
            df.columns = cols
            nan_value = float("NaN")
            df.replace("", nan_value, inplace=True)
            df["Hazardous_type"] = df["Hazardous_type"].fillna(method='ffill')
            df = df[df["imo_surcharges"] != 'IMO SURCHARGES']
            df = df.dropna(subset=["imo_surcharges"])
            self.df = df

        def clean(self):
            # self.df =  self.df.melt(
            #     id_vars=["Hazardous_type", "imo_surcharges", "currency"],
            #     var_name="load_type",
            #     value_name="amount")
            self.cleaned_output = {'IMO_Surcharges': self.df}

    class Special_equipment_fix(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):
            df = self.df
            cols = df.columns.to_series()
            cols.iloc[0] = "equipment_surcharge"
            df.columns = cols
            nan_value = float("NaN")
            df.replace("", nan_value, inplace=True)
            df["equipment_surcharge"] = df["equipment_surcharge"].fillna(method='ffill')
            df = df[df["equipment_type"] != 'Equipment Type']
            df = df.dropna(subset=["equipment_type"])
            self.df = df

        def clean(self):
            self.cleaned_output = {'Special_Equipment': self.df}

    def resolve_dependency(cls, fix_outputs):

        if "800000-2111-LT" in fix_outputs:
            Freight_sheet = fix_outputs.pop('800000-2111-LT')
            Freight_df = Freight_sheet["Freight"]
            Freight_df["unique"] ="800000-2111-LT"

        if "Surcharges" in fix_outputs:
            surcharge_sheet = fix_outputs.pop('Surcharges')
            surcharge_df = surcharge_sheet['surcharge']
            inclusive_list = surcharge_df.loc[surcharge_df["subject_to"].str.lower() == "inclusive"][
                "carrier_abbreviation"].to_list()
            not_subject_to_list = surcharge_df.loc[surcharge_df["subject_to"].str.lower() == "not subject to"][
                "carrier_abbreviation"].to_list()

            inclusive = ",".join(inclusive_list)
            not_subject_to = ",".join(not_subject_to_list)

            inclusive = inclusive + "," + not_subject_to

            Freight_df["inclusions"] = inclusive
            Freight_df["subject_to"] = Freight_df["subject_to"].apply(lambda x: x.split("both")[0])

        if "haz surcharges" in fix_outputs:
            haz_sheet = fix_outputs.pop('haz surcharges')
            hazardous_df = haz_sheet['IMO_Surcharges']

        if "Special equipment" in fix_outputs:
            Special_equipment_sheet = fix_outputs.pop('Special equipment')
            Special_Equipment_df = Special_equipment_sheet['Special_Equipment']

        dfs = []
        for index, row in hazardous_df.iterrows():
            haz_df = Freight_df.copy()
            haz_df["charges"] = row["Hazardous_type"]
            haz_df["commodity"] = row["imo_surcharges"]
            haz_df["currency"] = row["currency"]
            haz_df["20GP"] = row["20GP"]
            haz_df["40GP"] = row["40GP"]
            haz_df["40HC"] = row["40HC"]
            dfs.append(haz_df)
        Freight_with_haz_df = concat(dfs, ignore_index=True, sort=False)

        dfs = []
        for index, row in Special_Equipment_df.iterrows():
            spl_equiment_df = Freight_df.copy()
            if "20GP" in spl_equiment_df and row["20GP"] is not None:
                # spl_equiment_df["charges"] = row["Hazardous_type"]
                # spl_equiment_df["commodity"] = row["imo_surcharges"]
                spl_equiment_df["currency"] = row["currency"]
                spl_equiment_df["20OT"] = row["20GP"]
                spl_equiment_df["40OT"] = row["40GP"]
                spl_equiment_df["20FR"] = row["20GP"]
                spl_equiment_df["40FR"] = row["40GP"]

                spl_equiment_df.drop(columns=["20GP", "40GP", "40HC"], inplace=True)
                spl_equiment_df.dropna(subset=["20OT", "40OT", "20FR", "40FR"], inplace=True)
                spl_equiment_df = spl_equiment_df.drop_duplicates()

                dfs.append(spl_equiment_df)

        Freight_spl_equiment_df = concat(dfs, ignore_index=True, sort=False)
        Freight_spl_equiment_df = Freight_spl_equiment_df.drop_duplicates()
        Freight_df = concat([Freight_df, Freight_with_haz_df, Freight_spl_equiment_df], ignore_index=True, sort=False)

        if "China Inlands - North Central" in fix_outputs:
            Arbitary_sheet = fix_outputs.pop('China Inlands - North Central')
            ChinaInlands_Arbitary_df_ = Arbitary_sheet['Arbitrary']

        if "South China Inland" in fix_outputs:
            Arbitary_sheet = fix_outputs.pop('South China Inland')
            SouthChina_Arbitary_df = Arbitary_sheet['Arbitrary']

        if "Europe Destination TAD" in fix_outputs:
            Arbitary_sheet = fix_outputs.pop('Europe Destination TAD')
            Europe_Arbitary_df = Arbitary_sheet['Arbitrary']

        Arbitary_df = concat([ChinaInlands_Arbitary_df_, SouthChina_Arbitary_df, Europe_Arbitary_df], ignore_index=True,
                             sort=False)

        Freight_df = Freight_df.melt(
            id_vars=["customer_name", "region", "destination_port", "destination_icd", "origin_port", "origin_icd",
                     "commodity", "currency",
                     "basis", "charges", "inclusions", "subject_to", "start_date", "expiry"],
            var_name="load_type",
            value_name="amount")

        nan_value = float("NaN")
        Freight_df.replace("", nan_value, inplace=True)
        Freight_df = Freight_df.dropna(subset=["amount"])
        Freight_df.rename(columns = {"expiry":"expiry_date"}, inplace = True)
        Freight_df["origin_port"] = Freight_df["origin_port"].str.replace(",",";", regex = True)

        if "origin_port" in Freight_df:
            Freight_df['origin_port'] = Freight_df['origin_port'].str.split(";")
            Freight_df = Freight_df.explode('origin_port')

        Arbitary_df = Arbitary_df.dropna(subset=["20GP", "40GP"])
        Arbitary_df["basis"] = "PER CONTAINER"
        Arbitary_df["to"].replace("by TRUCK", "", inplace=True, regex=True)
        Arbitary_df["to"].replace(r"\*", "", regex=True, inplace=True)

        fix_outputs = {"800000-2111-LT": {"Freight": Freight_df, "Arbitrary Charges": Arbitary_df}}

        return fix_outputs

class Expedock_MSC_v1(BaseTemplate):
    class FE_Fix(BaseFix):
        def check_input(self):
            pass
        def check_output(self):
            pass

        def remove_empty_columns(cls, df):
            df.reset_index(drop=True, inplace=True)
            df = df.applymap(lambda x: nan if x == ':' or x == '' else x)
            df = df.dropna(axis=1, how='all')
            df = df.fillna('')
            df = df.T.reset_index(drop=True).T
            return df

        def first_rows_as_header(self,df):
            headers = df.iloc[0]
            df = df[1:]
            df.columns = headers
            return df

        def fix_freight_assessional(self, block_df, section):

            if section["Freight assessorial charges"] is not None:
                freight_assessional = block_df.loc[section["Freight assessorial charges"]["start"]:section["Freight assessorial charges"]["end"],:]
                index_of_freight_assessional = freight_assessional[freight_assessional[0] == 'Charge'].index.values[0]
                freight_assessional_df = freight_assessional.loc[index_of_freight_assessional:, :]
                freight_assessional_df = self.remove_empty_columns(freight_assessional_df)
                freight_assessional_df = self.first_rows_as_header(freight_assessional_df)

                column_names = {'Charge': 'charge', 'Charge Formula': 'charge_formula', 'Currency': 'currency',
                                'Calculation Method': 'load_type', 'Valid from': 'start_date',
                                'Valid to': 'expiry_date', 'Included in FRT': 'Included in FRT',
                                'Amount': 'amount', 'Applicable at': 'applicable_at',
                                'Amount is subject to': 'subject_to',
                                'Commodity': 'commodity',
                                "Valid To": "expiry_date", "Valid From": "start_date"
                                }
                freight_assessional_df = freight_assessional_df.rename(columns=column_names)
                freight_assessional_df = freight_assessional_df.applymap(lambda x: nan if x == ':' or x == '' else x)
                freight_assessional_df = freight_assessional_df.dropna(subset=('charge','currency', 'amount'))
                load_type_look = {"per 20DV": "20GP",
                                  "per 40DV/HC": "40GP;40HC",
                                  "per 45HC": "45HC",
                                  "per Container": "20GP;40GP;40HC",
                                  "per 40HR": "40HR"}
                freight_assessional_df["load_type"] = freight_assessional_df["load_type"].replace(load_type_look)
                freight_assessional_df["load_type"] = freight_assessional_df["load_type"].str.replace(r";$", "")

                freight_assessional_df["load_type"] = freight_assessional_df["load_type"].str.split(r";")
                freight_assessional_df = freight_assessional_df.explode('load_type')
                return freight_assessional_df
            else:
                return pd.DataFrame()

        def fix_origin_subcharges(self, block_df, section):
            if section["Origin assessorial charges"] is not None:
                origin_subcharges = block_df.loc[section["Origin assessorial charges"]["start"]:section["Origin assessorial charges"]["end"], : ]
                index_of_origin_subcharges = origin_subcharges[origin_subcharges[0] == 'Charge'].index.values[0]
                origin_subcharges_df = origin_subcharges.loc[index_of_origin_subcharges:, :]
                origin_subcharges_df = self.remove_empty_columns(origin_subcharges_df)
                origin_subcharges_df = self.first_rows_as_header(origin_subcharges_df)

                column_names = {'Charge': 'charge', 'Charge Formula': 'charge_formula', 'Currency': 'currency',
                                'Calculation Method': 'load_type', 'Valid from': 'start_date',
                                'Valid to': 'expiry_date', 'Included in FRT': 'Included in FRT',
                                'Amount': 'amount', 'Applicable at': 'origin_port',
                                'Amount is subject to': 'subject_to',
                                'Commodity': 'commodity',
                                "Valid To": "expiry_date", "Valid From": "start_date"
                                }
                origin_subcharges_df = origin_subcharges_df.rename(columns=column_names)
                origin_subcharges_df = origin_subcharges_df.applymap(lambda x: nan if x == ':' or x == '' else x)
                origin_subcharges_df = origin_subcharges_df.dropna(subset=("charge", "load_type"))
                if len(origin_subcharges_df) > 1:
                    load_type_look = {"per 20DV": "20GP",
                                      "per 40DV/HC": "40GP;40HC",
                                      "per 45HC": "45HC",
                                      "per Container": "20GP;40GP;40HC",
                                      "per 40HR": "40HR"}
                    origin_subcharges_df["load_type"] = origin_subcharges_df["load_type"].replace(load_type_look)
                    origin_subcharges_df["load_type"] = origin_subcharges_df["load_type"].str.replace(r";$", "")

                    origin_subcharges_df["load_type"] = origin_subcharges_df["load_type"].str.split(r";")
                    origin_subcharges_df = origin_subcharges_df.explode('load_type')
                    origin_subcharges_df["origin_port"] = origin_subcharges_df["origin_port"].str.replace(r",(\s?[A-z]{3,})",r";\1")
                    origin_subcharges_df["origin_port"] = origin_subcharges_df["origin_port"].str.split(r";")
                    origin_subcharges_df = origin_subcharges_df.explode('origin_port')
                    origin_subcharges_df = origin_subcharges_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                    return origin_subcharges_df
                else:
                    return pd.DataFrame()


            else:
                return pd.DataFrame()

        def fix_destination_subcharges(self, block_df, section):
            if section["Destination assessorial charges"] is not None:
                destination_subcharges = block_df.loc[section["Destination assessorial charges"]["start"]:section["Destination assessorial charges"]["end"], : ]
                index_of_destination_subcharges = destination_subcharges[destination_subcharges[0] == 'Charge'].index.values[0]
                destination_subcharges_df = destination_subcharges.loc[index_of_destination_subcharges:, :]
                destination_subcharges_df = self.remove_empty_columns(destination_subcharges_df)
                destination_subcharges_df = self.first_rows_as_header(destination_subcharges_df)

                column_names = {'Charge': 'charge', 'Charge Formula': 'charge_formula', 'Currency': 'currency',
                                'Calculation Method': 'load_type', 'Valid from': 'start_date',
                                'Valid to': 'expiry_date', 'Included in FRT': 'Included in FRT',
                                'Amount': 'amount', 'Applicable at': 'destination_port',
                                'Amount is subject to': 'subject_to',
                                'Commodity': 'commodity',
                                "IMO Class" : "IMO Class",
                                "Valid To": "expiry_date", "Valid From": "start_date"
                                }
                destination_subcharges_df = destination_subcharges_df.rename(columns=column_names)
                destination_subcharges_df = destination_subcharges_df.applymap(lambda x: nan if x == ':' or x == '' else x)
                destination_subcharges_df = destination_subcharges_df.dropna(subset=("charge", "load_type"))

                load_type_look = {"per 20DV": "20GP",
                                  "per 40DV/HC": "40GP;40HC",
                                  "per 45HC": "45HC",
                                  "per Container": "20GP;40GP;40HC",
                                  "per 40HR": "40HR"}
                destination_subcharges_df["load_type"] = destination_subcharges_df["load_type"].replace(load_type_look)
                destination_subcharges_df["load_type"] = destination_subcharges_df["load_type"].str.replace(r";$", "")
                destination_subcharges_df["load_type"] = destination_subcharges_df["load_type"].str.split(r";")
                destination_subcharges_df = destination_subcharges_df.explode('load_type')

                destination_subcharges_df["destination_port"] = destination_subcharges_df["destination_port"].str.replace(
                    r",(\s?[A-z]{3,})", r";\1")
                destination_subcharges_df["destination_port"] = destination_subcharges_df["destination_port"].str.split(r";")
                destination_subcharges_df = destination_subcharges_df.explode('destination_port')
                destination_subcharges_df = destination_subcharges_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

                return destination_subcharges_df
            else:
                return pd.DataFrame()

        def fix_destination_arbitary_block(self, block_df, section):

            if section["Destination Inland charges"] is not None:
                arbitary_block = block_df.loc[section["Destination Inland charges"]["start"]:section["Destination Inland charges"]["end"], :]
                if arbitary_block[arbitary_block[0] == 'Commodity'].index.any():
                    index_of_arbitary = arbitary_block[arbitary_block[0] == 'Commodity'].index.values[0]
                else:
                    index_of_arbitary = arbitary_block[arbitary_block[0] == 'Transport'].index.values[0]

                arbitary_df = arbitary_block.loc[index_of_arbitary:, :]
                arbitary_df = self.remove_empty_columns(arbitary_df)
                arbitary_df = self.first_rows_as_header(arbitary_df)

                column_names = {'Port(s) of load': 'via',
                                'Port(s) of discharge' : 'icd',
                                'Transport': 'mode_of_transportation', 'Currency': 'currency',
                                'Ctr SzTp': 'load_type', 'Valid from': 'start_date',
                                'Valid to': 'expiry_date', 'Included in FRT': 'Included in FRT',
                                'Amount': 'amount', 'Amount is not subject to': 'inclusions',
                                'Amount is subject to': 'subject_to',
                                'Commodity': 'commodity',
                                'Destination' : "to", "Valid To" : "expiry_date" , "Valid From" : "start_date"
                                }
                arbitary_df = arbitary_df.rename(columns=column_names)
                arbitary_df = arbitary_df.applymap(lambda x: nan if x == ':' or x == '' else x)
                arbitary_df = arbitary_df.dropna(subset=("to","load_type"))
                arbitary_df["icd"] = arbitary_df["icd"].str.replace(r"(?<=[A-Z]{3}),(?=[A-Z]{3})", r";", regex=True)
                arbitary_df["icd"] = arbitary_df["icd"].str.split(r";")
                arbitary_df = arbitary_df.explode('icd')
                arbitary_df['charges'] = 'destination arbitrary charges'
                arbitary_df['at'] = 'destination'
                arbitary_df = arbitary_df.fillna('')

                return arbitary_df
            else:
                return pd.DataFrame()

        def fix_arbitary_block(self, block_df , section):

            if section["Origin Inland charges"] is not None:
                arbitary_block = block_df.loc[section["Origin Inland charges"]["start"]:section["Origin Inland charges"]["end"], : ]

                if arbitary_block[arbitary_block[0] == 'Commodity'].index.any():
                    index_of_arbitary = arbitary_block[arbitary_block[0] == 'Commodity'].index.values[0]
                else:
                    index_of_arbitary = arbitary_block[arbitary_block[0] == 'Transport'].index.values[0]

                arbitary_df = arbitary_block.loc[index_of_arbitary:, :]
                arbitary_df = self.remove_empty_columns(arbitary_df)
                arbitary_df = self.first_rows_as_header(arbitary_df)

                column_names = {'Origin': 'icd', 'Port(s) of load': 'to',
                                'Transport': 'mode_of_transportation', 'Currency': 'currency',
                                'Ctr SzTp': 'load_type', 'Valid from': 'start_date',
                                'Valid to': 'expiry_date', 'Included in FRT': 'Included in FRT',
                                'Amount': 'amount', 'Amount is not subject to': 'inclusions', 'Amount is subject to': 'subject_to',
                                'Commodity': 'commodity',
                                'Port(s) of discharge': 'via'
                                }

                arbitary_df = arbitary_df.rename(columns=column_names)
                arbitary_df = arbitary_df.applymap(lambda x: nan if x == ':' or x == '' else x)
                arbitary_df = arbitary_df.dropna(subset=("icd", "to"))
                arbitary_df["icd"] = arbitary_df["icd"].str.replace(r"(?<=[A-Z]{3}),(?=[A-Z]{3})", r";", regex=True)

                arbitary_df["icd"] = arbitary_df["icd"].str.split(r";")
                arbitary_df = arbitary_df.explode('icd')

                arbitary_df['charges'] = 'origin arbitrary charges'
                arbitary_df['at'] = 'origin'
                arbitary_df = arbitary_df.fillna('')

                return arbitary_df
            else:
                return pd.DataFrame()

        def fix_freight_block(self, block_df , section):
            remarks = '' # empty remarks are there
            if section["Freight charges"] is not None:
                if block_df[block_df[0].str.contains('Remarks:')].index.values:
                    index_of_remarks_start = block_df[block_df[0].str.contains('Remarks:')].index.values[0]
                    index_of_remarks_end = block_df[block_df[0] == 'Commodity'].index.values[0]
                    remarks_df = block_df.loc[index_of_remarks_start:index_of_remarks_end-1, :]
                    try:
                        remarks = remarks_df[1].to_string(index=False)
                    except:
                        raise "remarks out of index in Freight charges"

                freight_block = block_df.loc[section["Freight charges"]["start"]:section["Freight charges"]["end"], : ]
                index_of_freight = freight_block[freight_block[0] == 'Commodity'].index.values[0]
                freight_df = freight_block.loc[index_of_freight:, :]

                freight_df = self.remove_empty_columns(freight_df)
                freight_df = self.first_rows_as_header(freight_df)
                column_names = { 'Port(s) of load': 'origin_port',
                                'Port(s) of discharge' : 'destination_port',
                                'Transport': 'mode_of_transportation', 'Currency': 'currency',
                                'Ctr SzTp': 'load_type', 'Valid from': 'start_date',
                                'Valid to': 'expiry_date', 'Included in FRT': 'Included in FRT',
                                'FRT Amt.': 'amount', 'FRT is not subject to': 'inclusions',
                                'FRT is subject to': 'subject_to',
                                'Commodity': 'commodity' ,"Move" :"mode_of_transportation" ,"Remarks" : "remarks",
                                 "Destination" : "destination_icd",
                                 "Origin" : "origin_icd",
                                 "Valid To": "expiry_date", "Valid From": "start_date"
                                }
                freight_df = freight_df.rename(columns=column_names)
                freight_df = freight_df.applymap(lambda x: nan if x == ':' or x == '' else x)
                freight_df = freight_df.dropna(subset=("origin_port", "destination_port"))
                freight_df = freight_df.dropna(axis=1, how='all')
                if len(freight_df) > 1:
                    if "commodity" in freight_df:
                        freight_df = freight_df.loc[freight_df.commodity.str.lower() != "commodity"]
                    freight_df["load_type"] = freight_df["load_type"].str.replace(r",$","")
                    freight_df["load_type"] = freight_df["load_type"].str.split(r",")
                    freight_df = freight_df.explode('load_type')

                    freight_df["load_type"] = freight_df["load_type"].str.replace(r"/$", "")

                    freight_df["load_type"] = freight_df["load_type"].str.split(r"/")
                    freight_df = freight_df.explode('load_type')

                    load_type_freight = {"20DV": "20GP", "40DV": "40GP"}
                    freight_df["load_type"] = freight_df["load_type"].replace(load_type_freight)
                    freight_df["origin_port"] = freight_df["origin_port"].str.replace(r"(?<=\w),(?=\w)", ";", regex=True)

                    #freight_df["origin_port"] = freight_df["origin_port"].replace(",", ";", regex=True)
                    freight_df["destination_port"] = freight_df["destination_port"].str.replace("(?<=[A-Z]{3}),", ";", regex=True)

                    freight_df["origin_port"] = freight_df["origin_port"].str.split(r";")
                    freight_df = freight_df.explode('origin_port')
                    freight_df["origin_port"] = freight_df["origin_port"].str.replace(",", "", regex=True)

                    freight_df["destination_port"] = freight_df["destination_port"].str.split(r";")
                    freight_df = freight_df.explode('destination_port')
                    freight_df["remarks_"] = remarks
                    freight_df = freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

                    return freight_df
                else:
                    return pd.DataFrame()
            else:
                return pd.DataFrame()

        def apply_destination_subcharges(self, freight_df, destination_subcharges_df):

            if not destination_subcharges_df.empty:
                destination_subcharges_df = destination_subcharges_df.to_dict('records')
                apply_charges_df = []
                for row in destination_subcharges_df:
                    if row["destination_port"]:
                        try:
                            destination_port = row["destination_port"].split(",")[0]
                        except:
                            raise 'destination port  in apply_destination_subcharges lookup out of index'
                    filtered_freight = freight_df.loc[(freight_df["load_type"].str.lower() == row["load_type"].lower()) & (freight_df["destination_port"].str.contains(destination_port ,case =False , na =False))]
                    if "commodity" in row:
                        filtered_freight["commodity"] = row["commodity"]
                    else:
                        filtered_freight["commodity"] = ""
                    filtered_freight["amount"] = row["amount"]
                    filtered_freight["currency"] = row["currency"]
                    filtered_freight["charges"] = row["charge"]

                    if "start_date" in row:
                        filtered_freight["start_date"] = row["start_date"]
                    else:
                        filtered_freight["start_date"] = ""

                    if "expiry_date" in row:
                        filtered_freight["expiry_date"] = row["expiry_date"]
                    else:
                        filtered_freight["expiry_date"] = ""

                    apply_charges_df.append(filtered_freight)

                freight_with_charges_df = concat(apply_charges_df, ignore_index=True, sort=False)
                return freight_with_charges_df
            else:
                return pd.DataFrame()

        def apply_origin_subcharges(self, freight_df, origin_subcharges_df):

            if not origin_subcharges_df.empty:
                origin_subcharges = origin_subcharges_df.to_dict('records')
                apply_charges_df = []
                for row in origin_subcharges:
                    if row["origin_port"]:
                        try:
                            origin_port = row["origin_port"].split(",")[0]
                        except:
                            raise 'origin in apply_origin_subcharges lookup out of index'
                    filtered_freight = freight_df.loc[(freight_df["load_type"].astype(str).str.lower() == row["load_type"].lower()) & (freight_df["origin_port"].astype(str).str.contains(origin_port , case =False , na =False))]
                    if not filtered_freight.empty:
                        if "commodity" in row:
                            filtered_freight["commodity"] = row["commodity"]
                        else:
                            filtered_freight["commodity"] = ""
                        filtered_freight["amount"] = row["amount"]
                        filtered_freight["currency"] = row["currency"]
                        filtered_freight["charges"] = row["charge"]

                        if "start_date" in row:
                            filtered_freight["start_date"] = row["start_date"]
                        else:
                            filtered_freight["start_date"] = ""

                        if "expiry_date" in row:
                            filtered_freight["expiry_date"] = row["expiry_date"]
                        else:
                            filtered_freight["expiry_date"] = ""

                        apply_charges_df.append(filtered_freight)

                if apply_charges_df:
                    freight_with_charges_df = concat(apply_charges_df, ignore_index=True, sort=False)
                    return freight_with_charges_df
                else:
                    return pd.DataFrame()

            else:
                return pd.DataFrame()

        def apply_freight_subcharges(self, freight_df, freight_subcharges_df):

            if not freight_subcharges_df.empty:
                freight_subcharges_df = freight_subcharges_df.to_dict('records')
                apply_charges_df = []
                for row in freight_subcharges_df:
                    filtered_freight = freight_df.loc[freight_df["load_type"].str.lower() == row["load_type"].lower()]
                    if "commodity" in row:
                        filtered_freight["commodity"] = row["commodity"]
                    else:
                        filtered_freight["commodity"] = ""
                    filtered_freight["amount"] = row["amount"]
                    filtered_freight["currency"] = row["currency"]
                    filtered_freight["charges"] = row["charge"]

                    if "start_date" in row:
                        filtered_freight["start_date"] = row["start_date"]
                    else:
                        filtered_freight["start_date"] = ""

                    if "expiry_date" in row:
                        filtered_freight["expiry_date"] = row["expiry_date"]
                    else:
                        filtered_freight["expiry_date"] = ""

                    apply_charges_df.append(filtered_freight)
                freight_with_charges_df = concat(apply_charges_df, ignore_index=True, sort=False)
                return freight_with_charges_df
            else:
                return pd.DataFrame()

        def get_sections(self , block):
            df = block.T.reset_index(drop=True).T
            sections_to_check = ['Freight charges',
                                 'Origin Inland charges',
                                 'Destination Inland charges',
                                 'Origin assessorial charges',
                                 'Destination assessorial charges',
                                 'Freight assessorial charges']
            sections = {}
            previous_section = None
            for check in sections_to_check:
                if df[df[0].str.contains(check, na=False)].index.values.any():
                    index = df[df[0].str.contains(check, na=False)].index.values[0]
                    sections[check] = {'start': index, 'end': None}
                    if previous_section:
                        sections[previous_section]['end'] = index
                    previous_section = check
                else:
                    sections[check] = None
            return sections

        def get_regional_sections(self):
            regional_sections = {}
            indexes = self.df[self.df[0].str.contains("1.Freight charges")].index.tolist()
            indexes.append(len(self.df))
            regional_sections = zip(indexes, indexes[1:])
            return regional_sections

        def get_contracts_details(self):
            contracts_details = {}
            if self.df[self.df[0].str.contains("SVC No:")].index.values.any():
                index = self.df[self.df[0].str.contains("SVC No:")].index.values[0]
                contract_id = self.df.loc[index][2]

            if self.df[self.df[0].str.contains("Effective from:")].index.values.any():
                index = self.df[self.df[0].str.contains("Effective from:")].index.values[0]
                start_date = self.df.loc[index][2]

            if self.df[self.df[0].str.contains("Effective to:")].index.values.any():
                index = self.df[self.df[0].str.contains("Effective to:")].index.values[0]
                expiry_date = self.df.loc[index][2]

            contracts_details['contract_id'] = contract_id
            contracts_details['vendor'] = "MSC"
            contracts_details['start_date'] = parse(start_date)
            contracts_details['expiry_date'] = parse(expiry_date)
            return contracts_details

        def get_charges_legend(self):
            if self.df[self.df[0].str.contains("Charges legend:")].index.values.any():
                index = self.df[self.df[0].str.contains("Charges legend:")].index.values[0]
                charge_legend_df = self.df.loc[index:, :1]
                charge_legend_df.columns = ["charge_legend" , "abbreviation"]
                charge_legend_df = charge_legend_df.applymap(lambda x: nan if x == ':' or x == '' else x)
                charge_legend_df.dropna(subset = ("charge_legend","abbreviation"), inplace = True)
                charge_legend_df["abbreviation"] = charge_legend_df["abbreviation"].str.split("(").str[0]
                return charge_legend_df

        def get_blocks(self):
            self.contracts_details = self.get_contracts_details()
            self.charges_legend = self.get_charges_legend()
            regional_sections = self.get_regional_sections()
            freight_result = []
            origin_arbitary_result = []
            origin_subcharges_result = []
            freight_assessional_result = []
            destination_subcharges_result =[]
            destination_arbitary_result = []
            origin_subcharges_freight_result = []
            destination_subcharges_freight_result =[]
            freight_assess_subcharges_freight_result =[]
            for regional_config in regional_sections:
                regional_df = self.df.loc[regional_config[0]:regional_config[1]-1, :]
                region = ""
                try:
                    region = regional_df[2].values[0]
                    if not region:
                        region = regional_df[1].values[0]
                except:
                    raise "region name out of index"

                regional_df = regional_df.T.reset_index(drop=True).T

                sections = self.get_sections(regional_df)

                freight = self.fix_freight_block(regional_df, sections)

                if not freight.empty:
                    freight["region"] = region
                    freight_result.append(freight)

                origin_arbitary= self.fix_arbitary_block(regional_df, sections)
                if not origin_arbitary.empty:
                    origin_arbitary["region"] = region
                    origin_arbitary_result.append(origin_arbitary)

                destination_arbitary = self.fix_destination_arbitary_block(regional_df, sections)
                if not destination_arbitary.empty:
                    destination_arbitary["region"] = region
                    destination_arbitary_result.append(destination_arbitary)

                origin_subcharges = self.fix_origin_subcharges(regional_df, sections)
                if not origin_subcharges.empty:
                    origin_subcharges["region"] = region
                    origin_subcharges['leg'] = "L3"
                    origin_subcharges_result.append(origin_subcharges)

                freight_assessional = self.fix_freight_assessional(regional_df, sections)
                if not freight_assessional.empty:
                    freight_assessional["region"] = region
                    freight_assessional['leg'] = "L3"
                    freight_assessional_result.append(freight_assessional)

                destination_subcharges = self.fix_destination_subcharges(regional_df, sections)
                if not destination_subcharges.empty:
                    destination_subcharges["region"] = region
                    destination_subcharges['leg'] = "L4"
                    destination_subcharges_result.append(destination_subcharges)

                if not freight.empty and not origin_subcharges.empty:
                    origin_subcharges_freight = self.apply_origin_subcharges(freight, origin_subcharges)
                    origin_subcharges_freight_result.append(origin_subcharges_freight)

                if not freight.empty and not destination_subcharges.empty:
                    destination_subcharges_freight = self.apply_destination_subcharges(freight, destination_subcharges)
                    destination_subcharges_freight_result.append(destination_subcharges_freight)

                if not freight.empty and not freight_assessional.empty:
                    freight_assessional_subcharges_freight = self.apply_freight_subcharges(freight, freight_assessional)
                    freight_assess_subcharges_freight_result.append(freight_assessional_subcharges_freight)


            freight_df = pd.DataFrame()
            if freight_result:
                freight_df = concat(freight_result, ignore_index=True, sort=False)
                freight_df['charges_leg'] = 'L3'
                freight_df['charges'] = 'Basic Ocean Freight'

            origin_arbitary_df = pd.DataFrame()
            if origin_arbitary_result:
                origin_arbitary_df = concat(origin_arbitary_result, ignore_index=True, sort=False)


            destination_arbitary_df = pd.DataFrame()
            if destination_arbitary_result:
                destination_arbitary_df = concat(destination_arbitary_result, ignore_index=True, sort=False)

            df_origin_subcharges = pd.DataFrame()
            if origin_subcharges_result:
                df_origin_subcharges = concat(origin_subcharges_result, ignore_index=True, sort=False)

            origin_subcharges_freight_df = pd.DataFrame()
            if origin_subcharges_freight_result:
                origin_subcharges_freight_df = concat(origin_subcharges_freight_result, ignore_index=True, sort=False)

            destination_subcharges_freight_df = pd.DataFrame()
            if destination_subcharges_freight_result:
                destination_subcharges_freight_df = concat(destination_subcharges_freight_result, ignore_index=True, sort=False)

            freight_assessional_subcharges_freight_df = pd.DataFrame()
            if freight_assess_subcharges_freight_result:
                freight_assessional_subcharges_freight_df = concat(freight_assess_subcharges_freight_result, ignore_index=True, sort=False)


            df_freight_assessional = pd.DataFrame()
            if freight_assessional_result:
                df_freight_assessional = concat(freight_assessional_result, ignore_index=True, sort=False)

            df_destination_subcharges = pd.DataFrame()
            if destination_subcharges_result:
                df_destination_subcharges = concat(destination_subcharges_result, ignore_index=True, sort=False)

            df_surcharges = concat([df_origin_subcharges, df_freight_assessional, df_destination_subcharges], ignore_index=True, sort=False)

            df_arbitary = concat([origin_arbitary_df ,destination_arbitary_df],ignore_index=True ,sort= False)

            freight_df = concat([freight_df,origin_subcharges_freight_df, destination_subcharges_freight_df, freight_assessional_subcharges_freight_df],ignore_index=True ,sort= False)

            return freight_df, df_arbitary, df_surcharges

        def capture(self):
            df_freight, df_arbitary, df_surcharges = self.get_blocks()
            self.captured_output =  {'Freight': df_freight,
                                     "Arbitrary Charges": df_arbitary,
                                     "Surcharges": df_surcharges
                                     }

        def clean(self):
            freight_df = self.captured_output['Freight']
            arbitary_df = self.captured_output['Arbitrary Charges']
            surcharges_df = self.captured_output['Surcharges']
            load_type_freight = {"20DV": "20GP", "40DV": "40GP"}

            if "load_type" in arbitary_df:
                arbitary_df["load_type"] = arbitary_df["load_type"].str.replace(r",$", "")
                arbitary_df["load_type"] = arbitary_df["load_type"].str.split(r",")
                arbitary_df = arbitary_df.explode('load_type')
                arbitary_df["load_type"] = arbitary_df["load_type"].replace(load_type_freight, regex=True)

            if "via" in arbitary_df:
                arbitary_df["icd"] = arbitary_df["icd"].str.replace(r"(?<=[A-Z]{3}),(?=[A-Z]{3})", r";", regex=True)
                arbitary_df["via"] = arbitary_df["via"].str.replace(r"(?<=[A-Z]{3}),\s?(?=[A-Z]{3})", r";", regex=True)
                #arbitary_df = arbitary_df.explode('via')


            freight_df["contract_id"] = self.contracts_details["contract_id"]
            freight_df["contract_start_date"] = self.contracts_details["start_date"]
            freight_df["contract_expiry_date"] = self.contracts_details["expiry_date"]
            freight_df["vendor"] = self.contracts_details["vendor"]
            freight_df.loc[freight_df['start_date'].isna() | (freight_df['start_date'] == ''), 'start_date'] = self.contracts_details["start_date"]
            freight_df.loc[freight_df['expiry_date'].isna() | (freight_df['expiry_date'] == ''), 'expiry_date'] = self.contracts_details["expiry_date"]

            if "start_date" in arbitary_df:
                arbitary_df.loc[arbitary_df['start_date'].isna() | (arbitary_df['start_date'] == ''), 'start_date'] = self.contracts_details["start_date"]
            if "expiry_date" in arbitary_df:
                arbitary_df.loc[arbitary_df['expiry_date'].isna() | (arbitary_df['expiry_date'] == ''), 'expiry_date'] = self.contracts_details["expiry_date"]

            #SurCharges charge Lookup
            charges_legend_df = self.charges_legend
            # surcharges_result = pd.merge(surcharges_df, charges_legend_df, left_on="charge", right_on='charge_legend', how='left', sort=False)
            # surcharges_result.drop(columns=["charge", "charge_legend"], inplace=True)
            # surcharges_result.rename(columns={"abbreviation": "charge"}, inplace=True)
            charge_code_lookup = {
                "ACC": "ALAMEDA CORRIDOR SURCHARGE",
            "AGS": "ADEN GULF SURCHARGE",
            "BRC": "BUNKER RECOVERY CHARGE",
            "CCC": "CONTAINER COMPLIANCE CHARGE",
            "CUC": "CHASSIS USAGE CHARGE",
            "EIC": "EMERGENCY INTERMODAL CHARGE",
            "FEE": "FEEDER FREIGHT",
            "FES": "FUEL ESCALATION SURCHARGE",
            "GFS": "GLOBAL FUEL SURCHARGE",
            "HAZ": "HAZARDOUS",
            "LSC": "LOW SULPHUR FUEL CONTRIBUTION",
            "PCS": "PANAMA CANAL SURCHARGE",
            "SCS": "SUEZ CANAL SURCHARGE",
            "SPD": "ISPS - INTERN. SHIP AND PORT SECURITY CHARGE",
            "THC": "TERMINAL HANDLING CHARGE",
            "USC": "SECURITY MANIFEST DOCUMENTATION FEE",
            "WHA": "WHARFAGE" }

            freight_df["charges"] = freight_df["charges"].replace(charge_code_lookup)
            self.cleaned_output = {'Freight': freight_df,"Arbitrary Charges": arbitary_df, "Surcharges" : surcharges_df }

    class NAC_LIST_Fix(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def first_rows_as_header(self,df):
            headers = df.iloc[0]
            df = df[1:]
            df.columns = headers
            return df

        def capture(self):
            index = self.df[self.df[0].str.contains("NAMED ACCOUNT NAME")].index.values[0]
            df = self.df.loc[index:, :2]
            df = self.first_rows_as_header(df)
            self.captured_output = {"NAC" : df}

        def clean(self):
            self.cleaned_output = self.captured_output

    def resolve_dependency(cls, fix_outputs):

        df_NACLIST = fix_outputs.pop('NAC LIST')

        try:
            named_account = df_NACLIST["NAC"]["NAMED ACCOUNT NAME"].unique()[0]
        except:
            raise "named_account out of index in NAC LIST worksheet"

        df_FEUS = fix_outputs.pop('FE-US')
        df_FEUS["Freight"]["unique"] = "FE-US"
        df_FEUS["Freight"]["named_account"] = named_account
        df_FEUS["Arbitrary Charges"]["unique"] = "FE-US"
        df_FEUS["Arbitrary Charges"]["named_account"] = named_account

        if "Surcharges" in df_FEUS:
            df_FEUS["Surcharges"]["unique"] = "FE-US"

        df_FEUS_DT = fix_outputs.pop('FE-US DT')
        if "Freight" in df_FEUS_DT:
            df_FEUS_DT["Freight"]["named_account"] = named_account
            df_FEUS_DT["Freight"]["unique"] = "FE-US DT"
        if "Arbitrary Charges" in df_FEUS_DT:
            df_FEUS_DT["Arbitrary Charges"]["unique"] = "FE-US DT"
            df_FEUS_DT["Arbitrary Charges"]["named_account"] = named_account

        if "Surcharges" in df_FEUS_DT:
            df_FEUS_DT["Surcharges"]["unique"] = "FE-US DT"

        df_FEUS_NAC = fix_outputs.pop('FE-US (NAC)')
        df_FEUS_NAC["Freight"]["unique"] = "FE-US (NAC)"
        df_FEUS_NAC["Freight"]["named_account"] = named_account
        if "Arbitrary Charges" in df_FEUS_NAC:
            df_FEUS_NAC["Arbitrary Charges"]["unique"] = "FE-US (NAC)"
            df_FEUS_NAC["Arbitrary Charges"]["named_account"] = named_account

        if "Surcharges" in df_FEUS_NAC:
            df_FEUS_NAC["Surcharges"]["unique"] = "FE-US (NAC)"


        df_FEUS_999 = fix_outputs.pop('FE-US 999')
        df_FEUS_999["Freight"]["unique"] = "FE-US 999"
        df_FEUS_999["Freight"]["named_account"] = named_account

        if "Arbitrary Charges" in df_FEUS_999:
            df_FEUS_999["Arbitrary Charges"]["unique"] = "FE-US 999"
            df_FEUS_999["Arbitrary Charges"]["named_account"] = named_account

        if "Surcharges" in df_FEUS_999:
            df_FEUS_999["Surcharges"]["unique"] = "FE-US 999"

        df_FEUS_888 = fix_outputs.pop('FE-US (-888)')
        df_FEUS_888["Freight"]["unique"] = "FE-US (-888)"
        df_FEUS_888["Freight"]["named_account"] = named_account
        df_FEUS_888["Arbitrary Charges"]["named_account"] = named_account

        df_FEUS_888["Arbitrary Charges"]["unique"] = "FE-US (-888)"
        if "Surcharges" in df_FEUS_888:
            df_FEUS_888["Surcharges"]["unique"] = "FE-US (-888)"

        df_FEUS_BASERATES = fix_outputs.pop('FE-US BASE RATES')
        df_FEUS_BASERATES["Freight"]["named_account"] = named_account
        df_FEUS_BASERATES["Freight"]["unique"] = "FE-US BASE RATES"
        df_FEUS_BASERATES["Arbitrary Charges"]["unique"] = "FE-US BASE RATES"
        df_FEUS_BASERATES["Arbitrary Charges"]["named_account"] = named_account

        if "Surcharges" in df_FEUS_BASERATES:
            df_FEUS_BASERATES["Surcharges"]["unique"] = "FE-US BASE RATES"

        fix_outputs = [df_FEUS, df_FEUS_DT , df_FEUS_NAC , df_FEUS_888 , df_FEUS_999 ,df_FEUS_BASERATES]
        return fix_outputs


class All_Trades_Export_MSC(BaseTemplate):
    def __init__(self):

        global replace_load_type
        replace_load_type = {"per 40' Ctr": "40GP=;40HC", "per 40´": "40GP=;40HC", "40'": "40GP=;40HC", "per 40'DC/HC": "40GP=;40HC", "per 40'DV/HC": "40GP=;40HC", "per 40`" : "40GP=;40HC", "per 40'": "40GP=;40HC", "per 40'DV/HC ": "40GP=;40HC"}

        global replace_currency
        replace_currency = {"USD": "", "EUR": ""}

    class Fak_Rates_Fix(BaseFix):
        def check_input(self):
            pass

        def first_rows_as_header(self, df):
            headers = df.iloc[0]
            df = df[1:]
            df.columns = headers
            return df

        def check_output(self):
            pass

        def get_validity(self):
            def get_start_date(date_str):
                return re.search(r"Valid as from:(.*)", date_str)

            captured_data = self.df.iloc[:, 0].apply(lambda x: get_start_date(str(x)))
            validity = {}
            for i in captured_data:
                if i:
                    validity["start_date"] = i.group(1).strip()

            def get_end_date(date_str):
                return re.search(r"Valid until further notice but not beyond(.*)", date_str)

            captured_data = self.df.iloc[:, 0].apply(lambda x: get_end_date(str(x)))
            for i in captured_data:
                if i:
                    validity["end_date"] = i.group(1).strip()

            return validity

        def clean_surcharges_administration_with_cols(self, surcharges_administration_df, columns):

            surcharges_administration_df.columns = columns
            surcharges_administration_df["charges"].fillna(method='ffill', inplace =True)
            surcharges_administration_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_administration_df = surcharges_administration_df.drop(columns=[column for column in surcharges_administration_df.columns if column.startswith('drop')])
            surcharges_administration_df["load_type"].replace(replace_currency, regex=True, inplace=True)
            surcharges_administration_df["load_type"].replace(replace_load_type, inplace=True)
            surcharges_administration_df.loc[surcharges_administration_df["charges"].str.contains("Antwerp"), "origin_port"] = "Antwerp"
            surcharges_administration_df.loc[surcharges_administration_df["charges"].str.contains("IMO"), "cargo_type"] = "ONLY"
            surcharges_administration_df["currency"], surcharges_administration_df["amount"], surcharges_administration_df["charges_leg"] = surcharges_administration_df["amount"].str.split(" ").str[1], surcharges_administration_df["amount"].str.split(" ").str[0], "L4"

            return surcharges_administration_df

        def clean_surcharges_administration(self, surcharges_administration_df):

            if len(surcharges_administration_df.columns) == 4:
                columns = ["charges", "amount", "load_type", "remarks"]

            surcharges_administration_df.columns = columns
            surcharges_administration_df["charges"].fillna(method='ffill', inplace =True)
            surcharges_administration_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_administration_df = surcharges_administration_df.drop(columns=[column for column in surcharges_administration_df.columns if column.startswith('drop')])
            surcharges_administration_df["load_type"].replace(replace_currency,  regex =True, inplace =True)
            surcharges_administration_df["load_type"].replace(replace_load_type, inplace =True)

            surcharges_administration_df.loc[surcharges_administration_df["charges"].str.contains("Antwerp"), "origin_port"] = "Antwerp"
            surcharges_administration_df.loc[surcharges_administration_df["charges"].str.contains("IMO"), "cargo_type"] = "ONLY"
            surcharges_administration_df["currency"], surcharges_administration_df["amount"], surcharges_administration_df["charges_leg"] = surcharges_administration_df["amount"].str.split(" ").str[1], surcharges_administration_df["amount"].str.split(" ").str[0], "L4"

            return surcharges_administration_df


        def clean_surcharges_local_origin(self, surcharges_local_origin_df):

            if len(surcharges_local_origin_df.columns) == 6:
                columns = ["charges", "origin_port_1", "origin_port", "amount", "load_type", "drop1"]

            surcharges_local_origin_df.columns = columns
            surcharges_local_origin_df["charges"].fillna(method='ffill', inplace =True)
            surcharges_local_origin_thc_isps_df = surcharges_local_origin_df.loc[(surcharges_local_origin_df["charges"].str.contains("THC")) | (surcharges_local_origin_df["charges"].str.contains("ISPS"))]
            surcharges_other_local_origins = surcharges_local_origin_df.loc[~((surcharges_local_origin_df["charges"].str.contains("THC")) | (surcharges_local_origin_df["charges"].str.contains("ISPS")))]

            if len(surcharges_other_local_origins.columns) == 6:
                surcharges_other_local_origins.columns = ["charges", "origin_port", "amount", "load_type", "drop1", "drop2"]

            surcharges_local_origin_df = pd.concat([surcharges_local_origin_thc_isps_df, surcharges_other_local_origins], ignore_index= True)
            surcharges_local_origin_df = surcharges_local_origin_df.drop(columns=[column for column in surcharges_local_origin_df.columns if column.startswith('drop') or column.startswith('origin_port_1')])
            surcharges_local_origin_df["load_type"].replace(replace_currency,  regex =True, inplace =True)
            surcharges_local_origin_df["load_type"].replace(replace_load_type, inplace=True)

            surcharges_local_origin_df["origin_port"] = surcharges_local_origin_df["origin_port"].replace("/", ";", regex =True)
            surcharges_local_origin_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_local_origin_df.loc[surcharges_local_origin_df["amount"].str.contains("^[0-9]", na=False), "currency"] = surcharges_local_origin_df["amount"].str.split(" ").str[1]
            surcharges_local_origin_df.loc[surcharges_local_origin_df["amount"].str.contains("^[0-9]", na=False), "amount"] = surcharges_local_origin_df["amount"].str.split(" ").str[0]
            surcharges_local_origin_df["amount"].replace(replace_currency,  regex =True, inplace =True)
            surcharges_local_origin_df["charges_leg"] = "L2"

            return surcharges_local_origin_df


        def clean_surcharges(self, surcharges_df):

            if len(surcharges_df.columns) == 6:
                columns = ["charges",  "portnames", "amount", "load_type", "drop5", "drop2"]


            surcharges_df.columns = columns
            surcharges_df = surcharges_df.drop(columns=[column for column in surcharges_df.columns if column.startswith('drop')])
            surcharges_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_df_with_rates = surcharges_df.loc[surcharges_df["amount"].str.contains("^\d+",  na =False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"]= surcharges_df_with_rates["amount"].str.split(" ").str[1], surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = surcharges_df.loc[~surcharges_df["amount"].str.contains("^\d", na =False)]
            surcharges_df_without_rates.rename(columns = {"amount" : "remarks"}, inplace =True)

            surcharges_result = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index= True)
            self.replace_currency = {"USD" : "", "EUR" : ""}
            surcharges_result["load_type"].replace(self.replace_currency, regex =True , inplace =True)
            surcharges_result.loc[surcharges_result["charges"].str.contains("request", case=False,  na=False), ("remarks","amount")] = "ON REQUEST", "ON REQUEST"
            surcharges_result.loc[surcharges_result["charges"].str.contains("Australia", case=False,  na=False), "destination_country"] = "Australia"
            surcharges_result.loc[surcharges_result["charges"].str.contains("New Zealand", case=False,  na=False), "destination_country"] = "New Zealand"
            surcharges_result["charges_leg"] = "L3"
            surcharges_result.loc[surcharges_result["charges"].str.contains("REQUEST", case=False, na=False), ("amount", "load_type", "currency")] = "ON REQUEST", "per Container", "USD"
            surcharges_result.loc[surcharges_result["charges"].str.contains("IMO", case=False, na=False), "cargo_type"] = "ONLY"

            #surcharges_result["remarks"].replace(self.replace_currency, regex=True, inplace =True)
            return surcharges_result

        def get_surcharges(self):
            pass


        def melt_load_type(self, df):
            if "load_type" not in df:
                df = df.melt(id_vars=[column for column in df.columns if not column[1].isdigit()],
                             var_name="load_type",
                             value_name="amount")

                return df

        def get_contract_reference(self):
            df = self.df
            contract_reference_df = df.loc[df.iloc[:,0].str.contains("Contract / Filing Reference" , na =False)].iloc[:,1].str.split("-").str[0]
            contract_df = pd.DataFrame()
            contract_df["reference_no"] = contract_reference_df.str.split(" ").str[0]
            contract_df["region"] = contract_reference_df.str.split(" ", n=1).str[1]
            contract_df.loc[contract_df["region"].str.contains("POL"), "origin_port"] = contract_df["region"].str.replace("POL ", "",  regex = True).str.split("\(").str[0].str.strip()
            contract_df.loc[contract_df["region"].str.contains("POL"), "region"] = nan
            contract_df["region"] = contract_df["region"].str.replace("\(", "", regex = True).replace("\)", "", regex = True)
            return contract_df

        def get_freight_df(self):

            df = self.df

            start_index = df[df[0] == 'Port of Loading'].index.values[0]
            end_index = df[df[0] == "SURCHARGES - VALID AT TIME OF SHIPMENT (v.a.t.o.s)"].index.values[0]
            freight_df = df.loc[start_index: end_index - 1, :]
            freight_df = self.first_rows_as_header(freight_df)
            regions = freight_df.loc[(freight_df.iloc[:, 0] != "") & (freight_df.iloc[:, 1] == "")].iloc[:, 0].to_dict()
            freight_df_columns = freight_df.columns
            filtered_empty_columns = list(filter(lambda x: x != "" and x != 'Port of Loading' and x != "region", freight_df_columns))
            dps = []
            for pol in filtered_empty_columns:
                pol_index = freight_df.columns.get_loc(pol)
                sliced_df = freight_df.iloc[:, [0, pol_index, pol_index+1]]
                sliced_df = sliced_df.applymap(lambda x: nan if x == '' else x)
                if len(sliced_df.columns) == 3:
                    sliced_df.columns = ["destination_port", "20GP", "40GP"]
                    filter_via_port = sliced_df.loc[(sliced_df["destination_port"].str.contains("via", case=False)) & (sliced_df["20GP"].isna())]["destination_port"]
                    sliced_df["40HC"] = sliced_df["40GP"].copy()
                    sliced_df["origin_port"] = pol
                    sliced_df.dropna(subset = ["destination_port", "20GP", "40GP"], inplace = True)
                    for region in regions.items():
                        sliced_df.loc[int(region[0]) + 1, "region"] = region[1]
                    sliced_df["region"] = sliced_df["region"].fillna(method="ffill")
                    dps.append(sliced_df)

            freight_result_df = concat(dps, ignore_index=True)
            return freight_result_df

        def map_contract_reference(self, contract_reference_df, freight_df):

            regions_list = ["region", "origin_port", "destination_port"]
            for region_column in regions_list:
                if region_column in contract_reference_df:
                    region_dict = contract_reference_df[["reference_no", region_column]].to_dict("records")
                    for region in region_dict:
                        if region[region_column] != nan:
                            if region_column in freight_df:
                                freight_df.loc[freight_df[region_column].str.contains(str(region[region_column]).strip(), case=False, na=False), "contract_id"] = region["reference_no"]

            return freight_df

        def capture(self):
            validity = self.get_validity()
            freight_df = self.get_freight_df()
            #surcharges_df = self.get_surcharges()
            freight_df["start_date"], freight_df["expiry_date"] = validity["start_date"], validity["end_date"]
            freight_df['charges_leg'], freight_df['currency'], freight_df['charges'] = 'L3', 'USD', 'Basic Ocean Freight'


            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT TIME OF", "^LOCAL CHARGES AT ORIGIN - VALID")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            surcharges_result_df = self.clean_surcharges(surcharges_df)

            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN - VALID", "^ADMINISTRATION SURCHARGES - VALID AT")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            surcharges_local_origin = self.clean_surcharges_local_origin(surcharges_df)

            # surcharges_df = self.surcharges_( "^LOCAL CHARGES AT DESTINATION - VALID", "^ADMINISTRATION SURCHARGES - VALID AT")
            # surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(surcharges_df)
            # surcharges_local_destination = self.clean_surcharges_local_destination(surcharges_df)

            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^ADMINISTRATION SURCHARGES - VALID AT", None)
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            surcharges_administration = self.clean_surcharges_administration(surcharges_df)

            surcharges_df = pd.concat([surcharges_result_df, surcharges_local_origin, surcharges_administration], ignore_index=True)

            surcharges_df["start_date"], surcharges_df["expiry_date"] = validity["start_date"], validity["end_date"]

            self.captured_output = {"Freight": freight_df, "Charges": surcharges_df}

        def clean(self):
            contract_reference_lookup = self.get_contract_reference()
            freight_df = self.captured_output["Freight"]
            freight_df = self.map_contract_reference(contract_reference_lookup, freight_df)
            freight_df = self.melt_load_type(freight_df)
            freight_df["start_date"], freight_df["expiry_date"] = pd.to_datetime(freight_df["start_date"], format= "%d.%m.%y"), pd.to_datetime(freight_df["expiry_date"], format="%d.%m.%y")
            freight_df.loc[freight_df["destination_port"].str.contains("VIA"), "via"] = freight_df["destination_port"].str.split("VIA").str[1].replace("\)", "", regex=True)
            freight_df.loc[freight_df["region"].str.contains("VIA"), "via"] = freight_df["region"].str.split("VIA").str[1].replace("\)", "", regex=True)
            freight_df["via"].replace("/", ";", regex =True, inplace=True)
            freight_df["via"] = freight_df["via"].str.split(";")
            freight_df = freight_df.explode("via")
            charges_df = self.captured_output["Charges"]
            charges_df["contract_no"], charges_df["sub_vendor"] = "MSCMCDE", "MSC Mediterranean Shipping Company S.A. CORPORATION"
            freight_df["contract_no"], freight_df["sub_vendor"] = "MSCMCDE", "MSC Mediterranean Shipping Company S.A. CORPORATION"
            charges_df["start_date"], charges_df["expiry_date"] = pd.to_datetime(charges_df["start_date"], format="%d.%m.%y"), pd.to_datetime(charges_df["expiry_date"], format="%d.%m.%y")
            freight_df["currency"], freight_df["amount"],  freight_df["basis"] = freight_df["amount"].str.split(" ").str[1], freight_df["amount"].str.split(" ").str[0], "container"
            freight_df = freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            charges_df = charges_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            self.cleaned_output = {"Freight": freight_df, "Charges": charges_df}

    class Destination_Charges_Fix(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_surcharges_isps(self, surcharges_df):

            surcharges_df.columns = ["destination_port" ,  "20GP", "40GP", "charges", "drop1", "drop2"]
            surcharges_df = surcharges_df.drop(columns=[column for column in surcharges_df.columns if column.startswith('drop')])
            remove_currency = {"EUR" : "" , "AUS" : "" , "NZD" :"", "AUD": ""}
            surcharges_df["charges"].replace(remove_currency , regex = True , inplace =True)
            isps_charges_df = surcharges_df.loc[surcharges_df["charges"].str.contains("ISPS")]
            import_doc_charges_df = surcharges_df.loc[~surcharges_df["charges"].str.contains("ISPS")]
            import_doc_charges_df.rename(columns={"40GP": "basis"}, inplace=True)
            import_doc_charges_df["basis"].replace(remove_currency , regex = True , inplace =True)

            surcharges_result_df = pd.concat([isps_charges_df, import_doc_charges_df], ignore_index=True)

            if "40GP" in surcharges_result_df:
                surcharges_result_df["40HC"] = surcharges_result_df["40GP"].copy()

            surcharges_result_df = All_Trades_Export_MSC.Fak_Rates_Fix.melt_load_type(self, surcharges_result_df)
            surcharges_result_df = surcharges_result_df.loc[surcharges_result_df["amount"].notna()]

            surcharges_result_df["currency"], surcharges_result_df["amount"], surcharges_result_df["charges_leg"] = surcharges_result_df["amount"].str.split(" ").str[1], surcharges_result_df["amount"].str.split(" ").str[0], "L4"

            surcharges_result_df.loc[surcharges_result_df["currency"].str.contains("AUD", case =False, na =False), "destination_country" ] = "Australia"
            surcharges_result_df.loc[surcharges_result_df["currency"].str.contains("NZD", case =False, na =False), "destination_country" ] = "New Zealand"
            surcharges_result_df.drop(columns="destination_port", inplace=True)
            surcharges_result_df.loc[surcharges_result_df["charges"].str.contains("Doc"), "load_type"] = surcharges_result_df["basis"]
            return surcharges_result_df

        def capture(self):
            df = self.df
            df = df.applymap(lambda x: nan if x == '' else x)

            df.columns = ["drop1" , "destination_port", "20GP", "40GP", "20GP_HAZ", "40GP_HAZ", "charges"]
            df = df.drop(columns=[column for column in df.columns if column.startswith('drop')])
            df.dropna(subset=["destination_port", "20GP"], inplace =True)
            df = df[df["charges"].str.lower() != 'charge']
            surcharges_for_all_ports_df = df.loc[df["destination_port"].str.contains("All Ports", case=False)]
            surcharges_ispc_doc_fee = self.get_surcharges_isps(surcharges_for_all_ports_df)

            """ THC surcharges """
            THC_surcharges = df.loc[~df["destination_port"].str.contains("All Ports", case=False)]
            THC_surcharges["40HC"], THC_surcharges["40HC_HAZ"] = THC_surcharges["40GP"].copy(), THC_surcharges["40GP_HAZ"].copy()
            THC_surcharges_df = All_Trades_Export_MSC.Fak_Rates_Fix.melt_load_type(self, THC_surcharges)
            THC_surcharges_df.loc[THC_surcharges_df["load_type"].str.contains("_HAZ", case=False , na =False), "cargo_type"] = "ONLY"
            THC_surcharges_df.loc[THC_surcharges_df["load_type"].str.contains("_HAZ", case=False , na =False), "load_type"] = THC_surcharges_df["load_type"].str.split("_").str[0]
            THC_surcharges_df["currency"], THC_surcharges_df["amount"],  THC_surcharges_df["basis"], THC_surcharges_df["charges_leg"] = THC_surcharges_df["amount"].str.split(" ").str[1], THC_surcharges_df["amount"].str.split(" ").str[0], "container", "L4"
            surcharges_AUS = pd.concat([THC_surcharges_df, surcharges_ispc_doc_fee], ignore_index= True)
            self.captured_output = {"Charges": surcharges_AUS}

        def clean(self):
            self.cleaned_output = self.captured_output

    class India_fix(Fak_Rates_Fix):

        def remove_empty_rows_and_columns_(self, df):
            df = df.applymap(lambda x: nan if x == '' else x)
            df.dropna(how='all', inplace=True)
            df.dropna(axis=1, how="all", inplace=True)
            df.reset_index(drop=True, inplace=True)
            return df

        def clean_surcharges_local_origin(self, surcharges_local_origin_df):

            if len(surcharges_local_origin_df.columns) == 9:
                columns = ["charges", "drop1", "origin_port", "amount", "load_type", "drop5", "drop2", "drop3", "drop4"]

            elif len(surcharges_local_origin_df.columns) == 11:
                columns = ["charges", "drop1", "origin_port", "amount", "load_type", "remarks", "drop2", "drop3", "drop4", "drop7", "drop6"]

            elif len(surcharges_local_origin_df.columns) == 6:
                columns = ["charges",   "origin_port", "amount", "load_type", "drop5" , "drop6"]

            elif len(surcharges_local_origin_df.columns) == 7:
                columns = ["charges", "drop1",  "origin_port", "amount", "load_type", "drop5" , "drop6"]

            elif len(surcharges_local_origin_df.columns) == 4:
                columns = ["charges",  "origin_port", "amount", "load_type"]

            surcharges_local_origin_df.columns = columns
            surcharges_local_origin_df["charges"].fillna(method='ffill', inplace =True)
            surcharges_local_origin_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_local_origin_df = surcharges_local_origin_df.drop(columns=[column for column in surcharges_local_origin_df.columns if column.startswith('drop')])
            surcharges_local_origin_df["load_type"].replace(replace_currency,  regex =True, inplace =True)
            surcharges_local_origin_df["load_type"] = surcharges_local_origin_df["load_type"].str.strip()
            surcharges_local_origin_df["load_type"].replace(replace_load_type, inplace =True)

            surcharges_local_origin_df["currency"], surcharges_local_origin_df["amount"], surcharges_local_origin_df["charges_leg"]= surcharges_local_origin_df["amount"].str.split(" ").str[1], surcharges_local_origin_df["amount"].str.split(" ").str[0], "L2"
            surcharges_local_origin_df["origin_port"] = surcharges_local_origin_df["origin_port"].replace("/", ";", regex =True)
            return surcharges_local_origin_df

        def clean_surcharges_local_destination(self, surcharges_local_destination_df):

            if len(surcharges_local_destination_df.columns) == 9:
                columns = ["charges", "drop1", "portnames", "amount", "load_type", "drop5", "drop2", "drop3", "drop4"]

            elif len(surcharges_local_destination_df.columns) == 11:
                columns = ["charges", "drop1", "destination_port", "destination_port", "load_type", "remarks", "drop2", "drop3", "drop4", "drop7", "drop6"]

            elif len(surcharges_local_destination_df.columns) == 6:
                columns = ["charges", "destination_port", "amount", "load_type", "drop5", "drop6"]

            elif len(surcharges_local_destination_df.columns) == 5:
                columns = ["charges", "destination_port", "amount", "load_type", "drop5"]


            surcharges_local_destination_df.columns = columns
            surcharges_local_destination_df["charges"].fillna(method='ffill', inplace =True)
            surcharges_local_destination_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_local_destination_df = surcharges_local_destination_df.drop(columns=[column for column in surcharges_local_destination_df.columns if column.startswith('drop')])
            surcharges_local_destination_df["load_type"].replace(replace_currency,  regex =True, inplace =True)
            surcharges_local_destination_df["load_type"].replace(replace_load_type, inplace =True)

            #surcharges_local_destination_df["load_type"].replace(self.load_type_lookup,  regex =True, inplace =True)
            surcharges_local_destination_df["currency"], surcharges_local_destination_df["amount"], surcharges_local_destination_df["charges_leg"]= surcharges_local_destination_df["amount"].str.split(" ").str[1], surcharges_local_destination_df["amount"].str.split(" ").str[0], "L4"

            return surcharges_local_destination_df

        def clean_surcharges_administration(self, surcharges_administration_df):
            if len(surcharges_administration_df.columns) == 9:
                columns = ["charges", "drop1", "drop2", "amount", "load_type", "drop5", "drop2", "drop3", "drop4"]

            elif len(surcharges_administration_df.columns) == 11:
                columns = ["charges", "drop1", "drop2", "amount", "load_type", "remarks", "drop2", "drop3", "drop4", "drop7", "drop6"]

            elif len(surcharges_administration_df.columns) == 3:
                columns = ["charges",  "amount", "load_type"]

            elif len(surcharges_administration_df.columns) == 4:
                columns = ["charges",  "amount", "load_type", "drop1"]

            surcharges_administration_df.columns = columns
            surcharges_administration_df["charges"].fillna(method='ffill', inplace =True)
            surcharges_administration_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_administration_df = surcharges_administration_df.drop(columns=[column for column in surcharges_administration_df.columns if column.startswith('drop')])
            surcharges_administration_df["load_type"].replace(replace_currency,  regex =True, inplace =True)
            surcharges_administration_df["load_type"] =surcharges_administration_df["load_type"].str.strip()
            surcharges_administration_df["load_type"].replace(replace_load_type, inplace =True)

            #surcharges_administration_df["load_type"].replace(self.load_type_lookup,  regex =True, inplace =True)
            surcharges_administration_df["currency"], surcharges_administration_df["amount"], surcharges_administration_df["charges_leg"] = surcharges_administration_df["amount"].str.split(" ").str[1], surcharges_administration_df["amount"].str.split(" ").str[0], "L4"

            return surcharges_administration_df

        def clean_surcharges(self, surcharges_df):

            if len(surcharges_df.columns) == 9:
                columns = ["charges", "drop1", "portnames", "amount", "load_type", "drop5", "drop2", "drop3", "drop4"]

            elif len(surcharges_df.columns) == 11:
                columns = ["charges", "drop1", "portnames", "amount", "load_type", "drop5", "drop2", "drop3", "drop4", "drop7", "drop6"]

            elif len(surcharges_df.columns) == 6:
                columns = ["charges", "drop1",  "amount", "load_type", "drop5", "drop2"]

            elif len(surcharges_df.columns) == 5:
                columns = ["charges", "portnames", "amount", "load_type", "drop5"]

            surcharges_df.columns = columns
            surcharges_df = surcharges_df.drop(columns=[column for column in surcharges_df.columns if column.startswith('drop')])
            surcharges_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_df_with_rates = surcharges_df.loc[surcharges_df["amount"].str.contains("^\d+",  na=False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"]= surcharges_df_with_rates["amount"].str.split(" ").str[1], surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = surcharges_df.loc[~surcharges_df["amount"].str.contains("^\d", na=False)]
            surcharges_df_without_rates.rename(columns={"amount" : "remarks"}, inplace =True)

            surcharges_result = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index= True)
            # surcharges_result["load_type"].replace(replace_currency, regex =True , inplace =True)
            # replace_load_type = {"per 40' Ctr": "40GP=;40HC", "per 40´": "40GP=;40HC", "40'": "40GP=;40HC","per 40'DC/HC": "40GP=;40HC", "per 40'DV/HC":"40GP=;40HC"}
            surcharges_result["load_type"].replace(replace_currency, regex=True, inplace=True)
            surcharges_result["load_type"] = surcharges_result["load_type"].str.strip()
            surcharges_result["load_type"].replace(replace_load_type, inplace=True)
            #surcharges_result["load_type"].replace(self.load_type_lookup, regex =True , inplace =True)
            surcharges_result.loc[surcharges_result["charges"].str.contains("if requested" , case= False,   na =False), "remarks"] = "if requested"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("request" , case= False,  na =False), "amount"] = "ON REQUEST"

            surcharges_result["charges_leg"] = "L2"
            surcharges_result["remarks"].replace(replace_currency, regex=True, inplace =True)
            return surcharges_result

        def clean_surcharge_with_cols(self, surcharges_df, columns):

            surcharges_df.columns = columns
            surcharges_df = surcharges_df.drop(columns=[column for column in surcharges_df.columns if column.startswith('drop')])
            surcharges_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_df_with_rates = surcharges_df.loc[surcharges_df["amount"].str.contains("^\d+",  na =False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"]= surcharges_df_with_rates["amount"].str.split(" ").str[1], surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = surcharges_df.loc[~surcharges_df["amount"].str.contains("^\d", na =False)]
            surcharges_df_without_rates["remarks"], surcharges_df_without_rates["amount"] = surcharges_df_without_rates["amount"], ""
            surcharges_result = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index= True)
            # replace_currency = {"USD": "", "EUR": ""}
            # replace_load_type = {"per 40' Ctr": "40GP=;40HC", "per 40´": "40GP=;40HC", "40'": "40GP=;40HC", "per 40'DC/HC":"40GP=;40HC", "per 40'DV/HC":"40GP=;40HC"}
            surcharges_result["load_type"].replace(replace_currency, regex=True, inplace=True)
            surcharges_result["load_type"] = surcharges_result["load_type"].str.strip()
            surcharges_result["load_type"].replace(replace_load_type, inplace=True)
            surcharges_result.loc[surcharges_result["remarks"].str.contains("request" , case= False,  na =False), "amount"] = "ON REQUEST"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("not accepted" , case= False,  na =False), "amount"] = "ON REQUEST"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("incl" , case= False,  na=False), "amount"] = "incl"
            surcharges_result.loc[surcharges_result["charges"].str.contains("Antwerp"), "origin_port"] = "Antwerp"
            surcharges_result.loc[surcharges_result["charges"].str.contains("IMO"), "cargo_type"] = "ONLY"
            surcharges_result["charges_leg"] = "L3"

            surcharges_result.loc[surcharges_result["charges"].str.contains("origin", case= False,   na =False), "charges_leg"] = "L2"
            surcharges_result.loc[surcharges_result["charges"].str.contains("destination", case= False,   na =False), "charges_leg"] = "L4"
            surcharges_result["remarks"].fillna("", inplace=True)
            surcharges_result["remarks"].replace(replace_currency, regex=True, inplace =True)
            surcharges_result.loc[surcharges_result["charges"].str.contains("if requested", case=False, na=False), "remarks"] = "if requested"

            return surcharges_result

        def clean_surcharge_with_cols_origin_port(self, surcharges_df, columns, portname):

            surcharges_df.columns = columns
            surcharges_df["origin_port"] = portname
            surcharges_df = surcharges_df.drop(columns=[column for column in surcharges_df.columns if column.startswith('drop')])
            surcharges_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_df_with_rates = surcharges_df.loc[surcharges_df["amount"].str.contains("^\d+",  na =False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"]= surcharges_df_with_rates["amount"].str.split(" ").str[1], surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = surcharges_df.loc[~surcharges_df["amount"].str.contains("^\d", na =False)]
            surcharges_df_without_rates["remarks"], surcharges_df_without_rates["amount"] = surcharges_df_without_rates["amount"], ""
            surcharges_result = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index= True)
            surcharges_result["load_type"].replace(replace_currency, regex=True, inplace=True)
            surcharges_result["load_type"] = surcharges_result["load_type"].str.strip()
            surcharges_result["load_type"].replace(replace_load_type, inplace=True)
            #surcharges_result.loc[surcharges_result["charges"].str.contains("if requested" , case= False,   na =False), "remarks"] = "if requested"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("request" , case= False,  na =False), "amount"] = "ON REQUEST"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("incl" , case= False,  na =False), "amount"] = "incl"
            surcharges_result.loc[surcharges_result["charges"].str.contains("Antwerp"), "origin_port"] = "Antwerp"
            surcharges_result.loc[surcharges_result["charges"].str.contains("IMO"), "cargo_type"] = "ONLY"
            surcharges_result["charges_leg"] = "L2"

            surcharges_result.loc[surcharges_result["charges"].str.contains("origin" , case= False,   na =False), "charges_leg"] = "L2"
            surcharges_result.loc[surcharges_result["charges"].str.contains("destination" , case= False,   na =False), "charges_leg"] = "L4"
            surcharges_result["remarks"].replace(replace_currency, regex=True, inplace =True)
            return surcharges_result


        def clean_surcharge_with_cols_destination_country_portname(self, surcharges_df, columns, portname):

            surcharges_df.columns = columns
            surcharges_df["destination_country"] = portname
            surcharges_df["charges"].fillna(method='ffill', inplace =True)

            surcharges_df = surcharges_df.drop(columns=[column for column in surcharges_df.columns if column.startswith('drop')])
            surcharges_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_df_with_rates = surcharges_df.loc[surcharges_df["amount"].str.contains("^\d+",  na =False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"]= surcharges_df_with_rates["amount"].str.split(" ").str[1], surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = surcharges_df.loc[~surcharges_df["amount"].str.contains("^\d", na =False)]
            surcharges_df_without_rates["remarks"], surcharges_df_without_rates["amount"] = surcharges_df_without_rates["amount"], ""
            surcharges_result = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index= True)
            surcharges_result["load_type"].replace(replace_currency, regex=True, inplace=True)
            surcharges_result["load_type"] = surcharges_result["load_type"].str.strip()
            surcharges_result["load_type"].replace(replace_load_type, inplace=True)
            #surcharges_result.loc[surcharges_result["charges"].str.contains("if requested" , case= False,   na =False), "remarks"] = "if requested"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("request" , case= False,  na =False), "amount"] = "ON REQUEST"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("incl" , case= False,  na =False), "amount"] = "incl"
            surcharges_result.loc[surcharges_result["charges"].str.contains("Antwerp"), "origin_port"] = "Antwerp"
            surcharges_result.loc[surcharges_result["charges"].str.contains("IMO"), "cargo_type"] = "ONLY"
            surcharges_result["charges_leg"] = "L3"

            surcharges_result.loc[surcharges_result["charges"].str.contains("origin" , case= False,   na =False), "charges_leg"] = "L2"
            surcharges_result.loc[surcharges_result["charges"].str.contains("destination" , case= False,   na =False), "charges_leg"] = "L4"
            surcharges_result["remarks"].replace(replace_currency, regex=True, inplace =True)
            return surcharges_result


        def clean_surcharge_with_cols_destination_portname(self, surcharges_df, columns, portname):

            surcharges_df.columns = columns
            surcharges_df["destination_port"] = portname
            surcharges_df["charges"].fillna(method='ffill', inplace =True)

            surcharges_df = surcharges_df.drop(columns=[column for column in surcharges_df.columns if column.startswith('drop')])
            surcharges_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_df_with_rates = surcharges_df.loc[surcharges_df["amount"].str.contains("^\d+",  na =False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"]= surcharges_df_with_rates["amount"].str.split(" ").str[1], surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = surcharges_df.loc[~surcharges_df["amount"].str.contains("^\d", na =False)]
            surcharges_df_without_rates["remarks"], surcharges_df_without_rates["amount"] = surcharges_df_without_rates["amount"], ""
            surcharges_result = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index= True)
            surcharges_result["load_type"].replace(replace_currency, regex=True, inplace=True)
            surcharges_result["load_type"] = surcharges_result["load_type"].str.strip()
            surcharges_result["load_type"].replace(replace_load_type, inplace=True)
            #surcharges_result.loc[surcharges_result["charges"].str.contains("if requested" , case= False,   na =False), "remarks"] = "if requested"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("request" , case= False,  na =False), "amount"] = "ON REQUEST"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("incl" , case= False,  na =False), "amount"] = "incl"
            surcharges_result.loc[surcharges_result["charges"].str.contains("Antwerp"), "origin_port"] = "Antwerp"
            surcharges_result.loc[surcharges_result["charges"].str.contains("IMO"), "cargo_type"] = "ONLY"
            surcharges_result["charges_leg"] = "L3"

            surcharges_result.loc[surcharges_result["charges"].str.contains("origin" , case= False,   na =False), "charges_leg"] = "L2"
            surcharges_result.loc[surcharges_result["charges"].str.contains("destination" , case= False,   na =False), "charges_leg"] = "L4"
            surcharges_result["remarks"].replace(replace_currency, regex=True, inplace =True)
            return surcharges_result

        def explode_load_types(self, df):
            df['load_type'] = df['load_type'].str.split('=;')
            df = df.explode('load_type')
            df = df.drop_duplicates()
            df = df.reset_index(drop=True)
            return df

        def clean_surcharges_local_origin_with_cols(self, surcharges_local_origin_df, columns):

            surcharges_local_origin_df.columns = columns
            surcharges_local_origin_df["charges"].fillna(method='ffill', inplace =True)

            #surcharges_local_origin_df.dropna(subset=["charges", "amount"], inplace = True)

            surcharges_local_origin_df.dropna(subset=["charges", "origin_port"], inplace=True)
            surcharges_local_origin_df = surcharges_local_origin_df.drop(columns=[column for column in surcharges_local_origin_df.columns if column.startswith('drop')])
            surcharges_df_with_rates = surcharges_local_origin_df.loc[surcharges_local_origin_df["amount"].str.contains("^\d+", na=False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"] = surcharges_df_with_rates["amount"].str.split(" ").str[1], surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = surcharges_local_origin_df.loc[~surcharges_local_origin_df["amount"].str.contains("^\d", na=False)]
            surcharges_df_without_rates["remarks"], surcharges_df_without_rates["amount"] = surcharges_df_without_rates["amount"], ""
            surcharges_local_origin_df = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index=False)
            surcharges_local_origin_df["load_type"].replace(replace_currency,  regex =True, inplace=True)
            surcharges_local_origin_df["load_type"] = surcharges_local_origin_df["load_type"].str.strip()
            surcharges_local_origin_df["load_type"].replace(replace_load_type, inplace=True)
            surcharges_local_origin_df["charges_leg"] = "L2"
            surcharges_local_origin_df["origin_port"] = surcharges_local_origin_df["origin_port"].replace("/", ";", regex =True)
            surcharges_local_origin_df.loc[surcharges_local_origin_df["origin_port"].str.contains("on request", case=False, na=False), ("origin_port", "amount")] = "", "ON REQUEST"
            surcharges_local_origin_df.loc[surcharges_local_origin_df["charges"].str.contains("IMO", na=False, case=False), ("charges_leg", "cargo_type")] = "L3", "ONLY"
            surcharges_local_origin_df.loc[surcharges_local_origin_df["charges"].str.contains("origin", na=False, case=False), "charges_leg"] = "L2"
            surcharges_local_origin_df.loc[surcharges_local_origin_df["remarks"].str.contains("request" , case= False,  na =False), "amount"] = "ON REQUEST"

            return surcharges_local_origin_df



        def clean_surcharges_local_destination_with_cols(self, surcharges_local_destination_df, columns):

            surcharges_local_destination_df.columns = columns
            surcharges_local_destination_df["charges"].fillna(method='ffill', inplace =True)
            if "destination_port" in surcharges_local_destination_df:
                surcharges_local_destination_df["destination_port"].fillna(method='ffill', inplace =True)
            surcharges_local_destination_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_local_destination_df = surcharges_local_destination_df.drop(columns=[column for column in surcharges_local_destination_df.columns if column.startswith('drop')])
            # replace_currency = {"USD" : "", "EUR" : ""}
            # replace_load_type   = {"per 40' Ctr": "40GP=;40HC", "per 40´":"40GP=;40HC", "40'":"40GP=;40HC", "per 40'DC/HC":"40GP=;40HC", "per 40'DV/HC":"40GP=;40HC"}
            surcharges_local_destination_df["load_type"].replace(replace_currency,  regex =True, inplace=True)
            surcharges_local_destination_df["load_type"] = surcharges_local_destination_df["load_type"].str.strip()
            surcharges_local_destination_df["load_type"].replace(replace_load_type, inplace=True)
            surcharges_local_destination_df["currency"], surcharges_local_destination_df["amount"], surcharges_local_destination_df["charges_leg"]= surcharges_local_destination_df["amount"].str.split(" ").str[1], surcharges_local_destination_df["amount"].str.split(" ").str[0], "L4"
            if "destination_port" in surcharges_local_destination_df:
                surcharges_local_destination_df["destination_port"] = surcharges_local_destination_df["destination_port"].replace("/", ";", regex =True)
            surcharges_local_destination_df.loc[~surcharges_local_destination_df["charges"].str.contains("destination" ,case= False,   na =False), "charges_leg"] = "L3"

            return surcharges_local_destination_df

        def surcharges_(self, start_, end_=None):
            if start_ is not None and end_ is not None:
                start_index = self.df[self.df.iloc[:,0].str.contains(start_, na=False, case=False)].index.values[0]
                end_index = self.df[self.df.iloc[:,0].str.contains(end_, na=False, case=False)].index.values[0]
                surcharges_df = self.df.loc[start_index: end_index - 1, :]
                surcharges_df = surcharges_df.applymap(lambda x: nan if x == '' else x)
            else:
                start_index = self.df[self.df.iloc[:, 0].str.contains(start_, na=False, case=False)].index.values[0]
                surcharges_df = self.df.loc[start_index:, :]
                surcharges_df = surcharges_df.applymap(lambda x: nan if x == '' else x)

            return surcharges_df

        def surcharges_naf(self, start_, end_=None):
            if start_ is not None and end_ is not None:
                start_index = self.df[self.df.iloc[:,0].str.contains(start_, na=False)].index.values[0]
                end_index = self.df[self.df.iloc[:,0].str.contains(end_, na=False)].index.values[0]
                surcharges_df = self.df.loc[start_index: end_index - 1, :]
                surcharges_df = surcharges_df.applymap(lambda x: nan if x == '' else x)
            else:
                start_index = self.df[self.df.iloc[:, 0].str.contains(start_, na=False)].index.values[0]
                surcharges_df = self.df.loc[start_index:, :]
                surcharges_df = surcharges_df.applymap(lambda x: nan if x == '' else x)

            return surcharges_df

        def get_arbitary_rates(self):
            # yellow_surcharges_start_index = self.df[self.df[0] == 'Yellow Highlight'].index.values[0]
            df = self.df
            start_index = df[df[0].str.contains('Satellite Feeder Additional', na=False, case=False)].index.values[0]
            end_index = df[df[0].str.contains("SURCHARGES - VALID", na=False, case=False)].index.values[0]
            arbitary_rates = df.loc[start_index: end_index - 1, :]
            arbitary_rates.columns = ["to", "drop1", "drop2", "20GP", "40GP", "via", "drop3", "drop4", "drop5"]
            arbitary_rates = arbitary_rates.drop(columns=[column for column in arbitary_rates.columns if column.startswith('drop')])
            arbitary_rates = arbitary_rates.applymap(lambda x: nan if x == '' else x)
            arbitary_rates.loc[arbitary_rates["20GP"].str.contains("suspended until further notice", na =False, case =False), "40GP"] = "ON REQUEST"
            arbitary_rates.loc[arbitary_rates["20GP"].str.contains("suspended until further notice", na =False, case =False), "20GP"] = "ON REQUEST"
            arbitary_rates["via"] = arbitary_rates["via"].fillna(method = "ffill")
            arbitary_rates.dropna(subset=["to", "20GP", "40GP"], inplace=True)

            arbitary_rates["40HC"] = arbitary_rates["40GP"].copy()
            arbitary_rates = All_Trades_Export_MSC.Fak_Rates_Fix.melt_load_type(self, arbitary_rates)
            non_onrequest_rates = arbitary_rates.loc[~arbitary_rates["amount"].str.contains("ON REQUEST", case =False, na =False)]
            non_onrequest_rates["currency"], non_onrequest_rates["amount"] = non_onrequest_rates["amount"].str.split(" ").str[1], non_onrequest_rates["amount"].str.split(" ").str[0]
            on_request_rates = arbitary_rates.loc[arbitary_rates["amount"].str.contains("ON REQUEST")]
            arbitary_rates = pd.concat([non_onrequest_rates, on_request_rates], ignore_index= True)
            replace_via = {"via ": ""}
            arbitary_rates["via"] = arbitary_rates["via"].replace(replace_via,  regex =True)
            arbitary_rates["charges"], arbitary_rates["basis"], arbitary_rates["charges_leg"] = "Origin Arbitary charges", "container", "L2"

            return arbitary_rates

        def map_arbitary_rate(self, freight_df, arbitary_df):

            arbitary_df_dict = arbitary_df.to_dict("records")
            dps = []
            for arbitary_rate in arbitary_df_dict:
                freight_df_copy = freight_df.loc[(freight_df["destination_port"].str.contains(arbitary_rate["via"])) & (freight_df["load_type"].str.contains(arbitary_rate["load_type"]))]
                if arbitary_rate["amount"] != "ON REQUEST":
                    freight_df_copy["amount"] = freight_df_copy["amount"].astype(int) + int(arbitary_rate["amount"])
                else:
                    freight_df_copy["amount"] = arbitary_rate["amount"]

                freight_df_copy["charges"], freight_df_copy["basis"], freight_df_copy["charges_leg"] = "Origin Arbitary charges", "container", "L2"
                freight_df_copy["to"], freight_df_copy["remarks"] = arbitary_rate["to"], ""

                columns_rename = {"destination_port" : "via", "origin_port" : "icd"}
                freight_df_copy.rename(columns=columns_rename, inplace = True)

                dps.append(freight_df_copy)
            arbitary_rates = pd.concat(dps, ignore_index= True)

            return arbitary_rates

        def get_freight_df(self):
            # yellow_surcharges_start_index = self.df[self.df[0] == 'Yellow Highlight'].index.values[0]
            # df = self.df.loc[:yellow_surcharges_start_index - 1]
            df = self.df
            start_index = df[df[0] == 'Port of Loading'].index.values[0]
            end_index = df[df[0].str.contains("Satellite Feeder Additional", na=False, case=False)].index.values[0]
            freight_df = df.loc[start_index: end_index - 1, :]
            freight_df = self.first_rows_as_header(freight_df)
            regions = freight_df.loc[(freight_df.iloc[:, 0] != "") & (freight_df.iloc[:, 1] == "")].iloc[:, 0].to_dict()
            freight_df_columns = freight_df.columns
            filtered_empty_columns = list(filter(lambda x: x != "" and x != 'Port of Loading' and x != "region", freight_df_columns))
            dps = []
            for pol in filtered_empty_columns:
                pol_index = freight_df.columns.get_loc(pol)
                sliced_df = freight_df.iloc[:, [0, pol_index, pol_index+1]]
                sliced_df = sliced_df.applymap(lambda x: nan if x == '' else x)
                if len(sliced_df.columns) == 3:
                    sliced_df.columns = ["destination_port", "20GP", "40GP"]
                    filter_via_port = sliced_df.loc[(sliced_df["destination_port"].str.contains("via", case = False)) & (sliced_df["20GP"].isna())]["destination_port"]
                    sliced_df["40HC"] = sliced_df["40GP"].copy()
                    sliced_df["origin_port"] = pol
                    sliced_df.dropna(subset=["destination_port", "20GP", "40GP"], inplace = True)
                    dps.append(sliced_df)

            freight_result_df = concat(dps, ignore_index=True)
            return freight_result_df

        def capture(self):

            freight_df = self.get_freight_df()
            """TO DO """
            arbitary_df = self.get_arbitary_rates()

            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)
            contract_reference = All_Trades_Export_MSC.Fak_Rates_Fix.get_contract_reference(self)
            freight_df["start_date"], freight_df["expiry_date"], freight_df["contract_id"] = validity["start_date"], validity["end_date"], contract_reference["reference_no"].iloc[0]

            surcharges_df = self.surcharges_("^SURCHARGES - VALID AT TIME OF", "^LOCAL CHARGES AT ORIGIN - VALID")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)

            if len(surcharges_df.columns) == 5:
                columns = ["charges", "portnames", "amount", "load_type", "drop5"]
                surcharges_result_df = self.clean_surcharge_with_cols(surcharges_df, columns)

            surcharges_df = self.surcharges_( "^LOCAL CHARGES AT ORIGIN - VALID", "^LOCAL CHARGES AT DESTINATION - VALID")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 4:
                columns = ["charges", "origin_port", "amount", "load_type"]
                surcharges_local_origin = self.clean_surcharges_local_origin_with_cols(surcharges_df, columns)

            surcharges_df = self.surcharges_( "^LOCAL CHARGES AT DESTINATION - VALID", "^ADMINISTRATION SURCHARGES - VALID AT")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 5:
                columns = ["charges", "destination_port", "amount", "load_type","remarks"]
                surcharges_local_destination = self.clean_surcharges_local_destination_with_cols(surcharges_df, columns)

            surcharges_df = self.surcharges_("^ADMINISTRATION SURCHARGES - VALID AT", None)
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 3:
                columns = ["charges",  "amount", "load_type"]
                surcharges_administration = All_Trades_Export_MSC.Fak_Rates_Fix.clean_surcharges_administration_with_cols(self, surcharges_df, columns)

            combined_surcharges_df = pd.concat([surcharges_result_df, surcharges_local_origin, surcharges_local_destination, surcharges_administration], ignore_index= True)
            combined_surcharges_df["start_date"], combined_surcharges_df["expiry_date"] = validity["start_date"], validity["end_date"]

            arbitary_df["contract_id"] = contract_reference["reference_no"].iloc[0]

            self.captured_output = {"Freight" : freight_df , "Arbitary Charges" : arbitary_df, "Surcharges" : combined_surcharges_df}

        def clean(self):
            freight_df = self.captured_output["Freight"]
            arbitary_df = self.captured_output["Arbitary Charges"]
            surcharges_df = self.captured_output["Surcharges"]

            freight_df = All_Trades_Export_MSC.Fak_Rates_Fix.melt_load_type(self, freight_df)

            freight_df["currency"], freight_df["amount"],  freight_df["basis"] = freight_df["amount"].str.split(" ").str[1], freight_df["amount"].str.split(" ").str[0], "container"
            freight_df["start_date"], freight_df["expiry_date"] = pd.to_datetime(freight_df["start_date"], format="%d.%m.%y"), pd.to_datetime(freight_df["expiry_date"], format="%d.%m.%y")

            freight_df['charges_leg'], freight_df['charges'] = 'L3', 'Basic Ocean Freight'
            mapped_arbitary_df = self.map_arbitary_rate(freight_df, arbitary_df)
            mapped_arbitary_df["start_date"], mapped_arbitary_df["expiry_date"] = pd.to_datetime(mapped_arbitary_df["start_date"], format="%d.%m.%y"), pd.to_datetime(mapped_arbitary_df["expiry_date"], format="%d.%m.%y")

            freight_df = freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            mapped_arbitary_df = mapped_arbitary_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            surcharges_df["start_date"], surcharges_df["expiry_date"] = pd.to_datetime(surcharges_df["start_date"], format="%d.%m.%y"), pd.to_datetime(surcharges_df["expiry_date"], format="%d.%m.%y")

            if "load_type" in surcharges_df:
                surcharges_df = All_Trades_Export_MSC.India_fix.explode_load_types(self, surcharges_df)

            surcharges_df = surcharges_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            mapped_arbitary_df["at"] = "origin"
            self.cleaned_output = {"Freight": freight_df, "Arbitrary Charges": mapped_arbitary_df , "Charges" : surcharges_df}

    class Destination_charges_india_fix(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):
            df = self.df
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)

            end_index = df[df[0].str.contains("ON-CARRIAGE TO:", na=False, case=False)].index.values[0]
            surcharges_df = df.loc[: end_index - 1, :]
            surcharges_df.columns = ["charges", "destination_port", "20GP", "40GP", "drop1", "remarks", "drop2", "drop3"]

            surcharges_df = surcharges_df.drop(columns=[column for column in surcharges_df.columns if column.startswith('drop')])
            surcharges_df = surcharges_df.applymap(lambda x: nan if x == '' else x)
            additional_remarks_kolkata = surcharges_df.loc[surcharges_df.iloc[:, 0].str.contains("Kolkata", na=False, case=False)].to_string(index=False, header=False).replace("NaN", "").strip()
            surcharges_df.dropna(subset=["destination_port", "charges"], inplace =True)
            surcharges_df["charges_leg"], surcharges_df["40HC"] = "L4", surcharges_df["40GP"].copy()
            surcharges_df = All_Trades_Export_MSC.Fak_Rates_Fix.melt_load_type(self, surcharges_df)
            surcharges_df["currency"], surcharges_df["amount"], surcharges_df["basis"] = surcharges_df["amount"].str.split(" ").str[1], surcharges_df["amount"].str.split(" ").str[0], "container"
            #surcharges_df["start_date"], surcharges_df["expiry_date"] = validity["start_date"], validity["end_date"]
            #surcharges_df["start_date"], surcharges_df["expiry_date"] = pd.to_datetime(surcharges_df["start_date"], format="%d.%m.%y"), pd.to_datetime(surcharges_df["expiry_date"], format="%d.%m.%y")

            surcharges_df.loc[surcharges_df["destination_port"].str.contains("kolkata", case=False, na=False), "remarks" ] += "\n" + additional_remarks_kolkata
            surcharges_df = surcharges_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            self.captured_output = {"Charges": surcharges_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    class Indian_ocean_fix(BaseFix):

        def convert_currency_format(self, amount):

            if len(str(amount)) > 0:
                if str(amount[0]).isdigit():
                    locale.setlocale(locale.LC_ALL, "pt_BR")
                    formatted_amount = locale.atof(str(amount))
                    return formatted_amount
                else:
                    return amount
            else:
                return amount

        def convert_currency_format_with_currency(self, amount, currency):

            if len(str(amount)) > 0:
                if str(amount[0]).isdigit():
                    locales = {'USD': 'en_US', "GBP": "de_DE", "EUR": "de_DE"}
                    locale.setlocale(locale.LC_ALL, locales[currency])
                    formatted_amount = locale.atof(str(amount))
                    return formatted_amount
                else:
                    return amount
            else:
                return amount

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_freight_df(self):
            df = self.df
            start_index = df[df[0] == 'Port of Loading'].index.values[0]
            end_index = df[df[0].str.contains("SURCHARGES - VALID", na=False, case=False)].index.values[0]
            freight_df = df.loc[start_index: end_index - 1, :]
            freight_df = All_Trades_Export_MSC.Fak_Rates_Fix.first_rows_as_header(self, freight_df)
            regions = freight_df.loc[(freight_df.iloc[:, 0] != "") & (freight_df.iloc[:, 1] == "")].iloc[:, 0].to_dict()
            freight_df_columns = freight_df.columns
            filtered_empty_columns = list(filter(lambda x: x != "" and x != 'Port of Loading' and x != "region", freight_df_columns))
            dps = []
            for pol in filtered_empty_columns:
                pol_index = freight_df.columns.get_loc(pol)
                sliced_df = freight_df.iloc[:, [0, pol_index, pol_index+1]]
                sliced_df = sliced_df.applymap(lambda x: nan if x == '' else x)
                if len(sliced_df.columns) == 3:
                    sliced_df.columns = ["destination_port", "20GP", "40GP"]
                    filter_via_port = sliced_df.loc[(sliced_df["destination_port"].str.contains("via", case = False)) & (sliced_df["20GP"].isna())]["destination_port"]
                    sliced_df["40HC"] = sliced_df["40GP"].copy()
                    sliced_df["origin_port"] = pol
                    sliced_df.dropna(subset = ["destination_port", "20GP", "40GP"], inplace = True)
                    # for region in regions.items():
                    #     sliced_df.loc[int(region[0]) + 1, "region"] = region[1]
                    # sliced_df["region"] = sliced_df["region"].fillna(method="ffill")
                    dps.append(sliced_df)

            freight_result_df = concat(dps, ignore_index=True)
            return freight_result_df

        def get_contract_reference(self):

            df = self.df
            contract_reference_df = df.loc[df.iloc[:,0].str.contains("Contract / Filing Reference" , na =False)].iloc[:,1].str.split("-").str[0]
            contract_df = pd.DataFrame()
            contract_df["reference_no"] = contract_reference_df.str.split(" ").str[0]
            contract_df["destination_port"] = contract_reference_df.str.split(" ", n=1).str[1].replace("\(", "", regex  =True).replace("\)", "", regex  =True).replace("\+", ";", regex =True)
            contract_df["destination_port"] = contract_df["destination_port"].str.split(";")
            contract_df = contract_df.explode('destination_port')
            contract_df["destination_port"] = contract_df["destination_port"].str.strip()
            return contract_df

        def capture(self):
            freight_df = self.get_freight_df()
            contract_reference_lookup = self.get_contract_reference()
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)
            freight_df = All_Trades_Export_MSC.Fak_Rates_Fix.map_contract_reference(self, contract_reference_lookup, freight_df)
            freight_df["start_date"], freight_df["expiry_date"] = validity["start_date"],  validity["end_date"]

            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT TIME OF", "^LOCAL CHARGES AT ORIGIN - VALID")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 6:
                columns = ["charges", "destination_port", "amount", "load_type", "remarks", "drop1"]
                surcharges_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, surcharges_df, columns)
                surcharges_result_df.loc[surcharges_result_df["charges"].str.contains("Noumea", case= False,  na =False), "destination_port"] = "Noumea"
                surcharges_result_df.loc[surcharges_result_df["remarks"].str.contains("Mauritius", case= False,  na =False), "destination_port"] = "PORT LOUIS;POINTE DES GALETS;LONGONI"
                surcharges_result_country_df = surcharges_result_df.loc[surcharges_result_df["remarks"].str.contains("Madagascar", case= False,  na=False)]
                surcharges_result_country_df.loc[surcharges_result_country_df["remarks"].str.contains("Madagascar", case=False, na=False), ("destination_country", "destination_port")] = "Madagascar", ""
                surcharges_result_df = pd.concat([surcharges_result_df, surcharges_result_country_df], ignore_index=True)
                surcharges_result_df["destination_port"].replace("and", ";", regex=True, inplace=True)
                surcharges_result_df["destination_port"].replace(",", ";", regex=True, inplace=True)

            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN - VALID", "^ADMINISTRATION SURCHARGES - VALID AT")
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_origin_df)
            surcharges_local_origin = All_Trades_Export_MSC.India_fix.clean_surcharges_local_origin(self, surcharges_local_origin_df)

            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^ADMINISTRATION SURCHARGES - VALID AT", None)
            surcharges_adminstration_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            surcharges_administration_df = All_Trades_Export_MSC.India_fix.clean_surcharges_administration(self, surcharges_adminstration_df)

            combined_surcharges_df = pd.concat([surcharges_result_df, surcharges_local_origin, surcharges_administration_df], ignore_index= True)
            combined_surcharges_df["start_date"], combined_surcharges_df["expiry_date"] = validity["start_date"], validity["end_date"]
            combined_surcharges_df.loc[(combined_surcharges_df["amount"].str.contains("request", case= False,  na =False)) & (combined_surcharges_df["load_type"].isna()), "load_type"] = "per Ctr"

            if "load_type" in combined_surcharges_df:
                combined_surcharges_df = All_Trades_Export_MSC.India_fix.explode_load_types(self, combined_surcharges_df)

            self.captured_output = {"Freight" : freight_df, "Charges": combined_surcharges_df}

        def clean(self):
            freight_df = self.captured_output["Freight"]
            freight_df = All_Trades_Export_MSC.Fak_Rates_Fix.melt_load_type(self, freight_df)
            freight_df["start_date"], freight_df["expiry_date"] = pd.to_datetime(freight_df["start_date"], format="%d.%m.%y"), pd.to_datetime(freight_df["expiry_date"], format="%d.%m.%y")
            #freight_df["currency"], freight_df["amount"] = freight_df["amount"].str.split(" ").str[1], freight_df["amount"].str.split(" ").str[0]

            freight_df.loc[freight_df["amount"].str.contains("^[0-9]",  na =False), "currency"] = freight_df["amount"].str.split(" ").str[1]
            freight_df.loc[freight_df["amount"].str.contains("^[0-9]",  na =False), "amount"] = freight_df["amount"].str.split(" ").str[0]
            freight_df.loc[~freight_df["amount"].str.contains("^[0-9]",  na =False), "amount"] = "ON REQUEST"
            freight_df.loc[~freight_df["amount"].str.contains("^[0-9]",  na =False), "currency"] = "USD"

            Surcharges_df = self.captured_output["Charges"]
            Surcharges_df["start_date"], Surcharges_df["expiry_date"] = pd.to_datetime(Surcharges_df["start_date"], format="%d.%m.%y"), pd.to_datetime(Surcharges_df["expiry_date"], format="%d.%m.%y")
            surcharges_df = Surcharges_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            freight_df["amount"] = freight_df["amount"].apply(lambda x: self.convert_currency_format(x))


            if "load_type" in Surcharges_df:
                Surcharges_df = All_Trades_Export_MSC.India_fix.explode_load_types(self, Surcharges_df)

            freight_df["charges"], freight_df["charges_leg"], freight_df["basis"] = "Basic Ocean Freight", "L3", "container"
            freight_df = freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            Surcharges_df["contract_no"], Surcharges_df["sub_vendor"] = "MSCMCDE", "MSC Mediterranean Shipping Company S.A. CORPORATION"
            freight_df["contract_no"], freight_df["sub_vendor"] = "MSCMCDE", "MSC Mediterranean Shipping Company S.A. CORPORATION"

            Surcharges_df["amount"] = Surcharges_df["amount"].apply(lambda x: All_Trades_Export_MSC.Indian_ocean_fix.convert_currency_format(self, x))
            Surcharges_df = Surcharges_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            self.cleaned_output = {"Freight": freight_df, "Charges": Surcharges_df}

    class Middle_east_fix(Indian_ocean_fix):

        def clean_surcharges_local_origin(self, surcharges_local_origin_df):

            if len(surcharges_local_origin_df.columns) == 5:
                columns = ["charges", "origin_port", "amount", "load_type", "remarks"]

            surcharges_local_origin_df.columns = columns
            surcharges_local_origin_df["charges"].fillna(method='ffill', inplace=True)
            surcharges_local_origin_df.dropna(subset=["charges", "amount"], inplace=True)
            surcharges_local_origin_df = surcharges_local_origin_df.drop( columns=[column for column in surcharges_local_origin_df.columns if column.startswith('drop')])
            surcharges_local_origin_df["origin_port"] = surcharges_local_origin_df["origin_port"].replace("/", ";", regex=True)

            surcharges_df_with_rates = surcharges_local_origin_df.loc[surcharges_local_origin_df["amount"].str.contains("^\d+", na=False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"] = surcharges_df_with_rates["amount"].str.split(" ").str[1], surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = surcharges_local_origin_df.loc[~surcharges_local_origin_df["amount"].str.contains("^\d", na=False)]
            # surcharges_df_without_rates["remarks"] = surcharges_df_without_rates.amount

            surcharges_result = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index=True)
            replace_currency = {"USD": "", "EUR": ""}
            surcharges_result["load_type"].replace(replace_currency, regex =True , inplace =True)
            surcharges_result["load_type"] = surcharges_result["load_type"].str.strip()
            surcharges_result["load_type"].replace(replace_load_type, inplace=True)
            surcharges_result["charges_leg"] = "L2"
            surcharges_result.loc[surcharges_result["charges"].str.contains("if requested", case=False, na=False), "remarks"] = "if requested"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("request", case=False, na=False), "amount"] = "ON REQUEST"

            return surcharges_result


        def clean_surcharges(self, surcharges_df , columns):

            surcharges_df.columns = columns
            surcharges_df = surcharges_df.drop(columns=[column for column in surcharges_df.columns if column.startswith('drop')])
            surcharges_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_df_with_rates = surcharges_df.loc[surcharges_df["amount"].str.contains("^\d+",  na =False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"]= surcharges_df_with_rates["amount"].str.split(" ").str[1], surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = surcharges_df.loc[~surcharges_df["amount"].str.contains("^\d", na =False)]
            surcharges_df_without_rates.remarks = surcharges_df_without_rates.amount

            surcharges_result = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index= True)
            surcharges_result["load_type"].replace(replace_currency, regex =True , inplace =True)
            surcharges_result["load_type"] = surcharges_result["load_type"].str.strip()
            surcharges_result["load_type"].replace(replace_load_type, inplace=True)
            #surcharges_result["load_type"].replace(self.load_type_lookup, regex =True , inplace =True)
            surcharges_result.loc[surcharges_result["charges"].str.contains("if requested" , case= False,   na =False), "remarks"] = "if requested"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("request" , case= False,  na =False), "amount"] = "ON REQUEST"
            surcharges_result.loc[surcharges_result["charges"].str.contains("IMO" , case= False,   na =False), "cargo_type"] = "ONLY"

            surcharges_result["charges_leg"] = "L3"
            surcharges_result["remarks"].replace(replace_currency, regex=True, inplace =True)

            return surcharges_result


        def clean_surcharges_local_destination(self, surcharges_local_destination_df, columns):


            surcharges_local_destination_df.columns = columns
            surcharges_local_destination_df["charges"].fillna(method='ffill', inplace =True)
            surcharges_local_destination_df["destination_port"].fillna(method='ffill', inplace =True)

            surcharges_local_destination_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_local_destination_df = surcharges_local_destination_df.drop(columns=[column for column in surcharges_local_destination_df.columns if column.startswith('drop')])
            surcharges_local_destination_df["load_type"].replace(replace_currency,  regex =True, inplace =True)
            surcharges_local_destination_df["destination_port"].replace({"/" : ";"},  regex =True, inplace =True)
            surcharges_local_destination_df["load_type"] = surcharges_local_destination_df["load_type"].str.strip()
            surcharges_local_destination_df["load_type"].replace(replace_load_type, inplace =True)
            surcharges_local_destination_df["currency"], surcharges_local_destination_df["amount"], surcharges_local_destination_df["charges_leg"]= surcharges_local_destination_df["amount"].str.split(" ").str[1], surcharges_local_destination_df["amount"].str.split(" ").str[0], "L4"

            return surcharges_local_destination_df

        def get_freight_df(self):

            df = self.df
            start_index = df[df[0] == 'Port of Loading'].index.values[0]
            end_index = df[df[0].str.contains("SURCHARGES - VALID", na=False, case=False)].index.values[0]

            if df.iloc[:, 2].str.contains('ex').any():
                pol_index = df[(df.iloc[:, 2].str.contains('ex'))].index.values[0]
                pol = self.df.loc[int(pol_index)][2]

            freight_df = df.loc[start_index: end_index - 1, :]
            freight_df.columns = ["destination_port", "drop1", "20GP", "40GP", "drop3", "remarks"]
            freight_df = freight_df.applymap(lambda x: nan if x == '' else x)
            freight_df = freight_df.drop(columns=[column for column in freight_df.columns if column.startswith('drop')])
            freight_df.dropna(subset=["destination_port", "40GP"], inplace=True)
            freight_df = freight_df.loc[~freight_df["20GP"].str.contains("20' DV", case=False, na=False)]
            pol = pol.split("ex")[1]
            dps = []
            for origin_port in pol.split(","):
                freight_df_copy = freight_df.copy()
                freight_df_copy["origin_port"] = origin_port
                dps.append(freight_df_copy)
            freight_result_df = pd.concat(dps, ignore_index=True)
            freight_result_df["40HC"] = freight_result_df["40GP"].copy()

            return freight_result_df

        def capture(self):
            freight_df = self.get_freight_df()
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)
            contract_reference_lookup = All_Trades_Export_MSC.Fak_Rates_Fix.get_contract_reference(self)
            freight_df["start_date"], freight_df["expiry_date"], freight_df["contract_id"] = validity["start_date"], validity["end_date"],  str(contract_reference_lookup.iloc[0][0])
            freight_df["remarks"].fillna("", inplace=True)
            freight_df["contract_id"] = freight_df["contract_id"] + "\n"+  freight_df["remarks"]
            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT TIME OF", "^LOCAL CHARGES AT ORIGIN - VALID")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 5:
                columns = ["charges", "amount", "load_type", "remarks", "drop5"]
                surcharges_result_df = self.clean_surcharges(surcharges_df, columns)

            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN - VALID", "^LOCAL CHARGES AT DESTINATION - VALID AT TIME")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            surcharges_local_origin_df = self.clean_surcharges_local_origin(surcharges_df)
            surcharges_local_origin_df = surcharges_local_origin_df.loc[~surcharges_local_origin_df["remarks"].str.contains("not supplied",case =False, na=False)]

            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT DESTINATION - VALID AT TIME", "ONCARRIAGE")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 4:
                columns = ["charges", "destination_port", "amount", "load_type"]
                surcharges_local_destination_df = self.clean_surcharges_local_destination(surcharges_df, columns)

            combined_surcharges_df = pd.concat([surcharges_result_df, surcharges_local_origin_df, surcharges_local_destination_df], ignore_index= True)
            combined_surcharges_df["start_date"], combined_surcharges_df["expiry_date"] = validity["start_date"], validity["end_date"]

            self.captured_output = {'Freight': freight_df, "Charges": combined_surcharges_df}

    class Naf_fix(Indian_ocean_fix):

        def get_pol_sections(self, freight_df):
            regional_sections = {}
            indexes = freight_df[freight_df[0].str.contains("POL", na=False)].index.tolist()
            indexes.append(freight_df.index[-1])
            indexes = zip(indexes, indexes[1:])

            for config in indexes:
                region = freight_df[0][config[0]]
                regional_sections[region] = {'start': config[0], 'end': config[1]}

            return regional_sections


        def get_freight_df(self):
            df = self.df
            start_index = df[df[0] == 'SEAFREIGHT'].index.values[0]
            end_index = df[df[0].str.contains("SURCHARGES - VALID", na=False, case=False)].index.values[0]
            freight_df = df.loc[start_index: end_index - 1, :]
            freight_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_df)
            regional_sections = self.get_pol_sections(freight_df)
            dps = []
            for region, regional_config in regional_sections.items():
                region = region.replace("POL ", "")
                regional_df = freight_df.loc[regional_config['start']:regional_config['end'], :]
                regional_df = regional_df.T.reset_index(drop=True).T
                regional_df["origin_port"] = region
                dps.append(regional_df)
            freight_df = pd.concat(dps, ignore_index= True)
            if len(freight_df.columns) == 8:
                freight_df.columns = ["destination_port", "drop1", "drop2", "20GP", "40GP", "contract_id", "drop3", "origin_port"]
            elif len(freight_df.columns) == 5:
                freight_df.columns = ["destination_port", "20GP", "40GP", "contract_id", "origin_port"]

            freight_df = freight_df.drop(columns=[column for column in freight_df.columns if column.startswith('drop')])
            freight_df = freight_df.loc[freight_df["20GP"] != "20'DV"]
            freight_df["origin_port"], freight_df["40HC"]=freight_df["origin_port"].replace("/", ";", regex =True), freight_df["40GP"]
            freight_df.dropna(subset=["destination_port", "20GP"], inplace=True)
            return freight_df

        def capture(self):
            freight_df = self.get_freight_df()
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)
            contract_reference_df = All_Trades_Export_MSC.Fak_Rates_Fix.get_contract_reference(self)
            if "contract_id" in freight_df:
                freight_df.loc[freight_df["contract_id"].isna(), "contract_id"] = contract_reference_df.iloc[0][0]
            freight_df["start_date"], freight_df["expiry_date"] = validity["start_date"],  validity["end_date"]
            freight_df["contract_id"] = freight_df["contract_id"].str.split(":").str[1]
            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN - VALID", "^SPECIAL SURCHARGES")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            surcharges_local_origin = All_Trades_Export_MSC.India_fix.clean_surcharges_local_origin(self, surcharges_df)

            freight_surcharges = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT TIME OF SHIPMENT", "^ALGERIA")
            freight_surcharges = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges)
            if len(freight_surcharges.columns) == 5:
                columns = ["charges", "remarks", "amount", "load_type", "destination_country"]
                freight_surcharges_result = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, freight_surcharges, columns)

                freight_surcharges_result["destination_country"].replace(",", ";", regex =True, inplace =True)
                freight_surcharges_result["destination_country"].replace("&", ";", regex =True, inplace =True)
                freight_surcharges_result_with_port = freight_surcharges_result.loc[freight_surcharges_result["destination_country"].str.contains("Tunis", na=False, case=False)]
                freight_surcharges_result_with_port.loc[freight_surcharges_result_with_port["destination_country"].str.contains("Tunis", na=False, case=False), ("destination_port", "destination_country")] = "Tunis", ""

                freight_surcharges_result = pd.concat([freight_surcharges_result, freight_surcharges_result_with_port], ignore_index=True)
                freight_surcharges_result["destination_country"].replace("Tunis ;", "", regex=True, inplace =True)
                freight_surcharges_result["destination_country"].replace("reviewed monthly", "", regex=True, inplace =True)

                inclusion = freight_surcharges_result.loc[freight_surcharges_result["amount"].str.contains("incl", na=False, case=False)]
                if not inclusion.empty:
                    incl = inclusion["charges"].unique()
                    incl = ";".join(incl)
                    freight_df["inclusions"] = incl

            freight_surcharges_algeria = All_Trades_Export_MSC.India_fix.surcharges_naf(self, "ALGERIA", "CASABLANCA")
            freight_surcharges_algeria = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_algeria)
            if len(freight_surcharges_algeria.columns) == 6:
                columns = ["charges", "drop1", "amount", "load_type", "remarks", "drop1"]

                freight_surcharges_algeria_result = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols_destination_country_portname(self, freight_surcharges_algeria, columns, "ALGERIA")
                freight_surcharges_algeria_result.loc[freight_surcharges_algeria_result["charges"].str.contains("Hamburg/Bremerhaven", case=False, na=False), "origin_port"] = "Hamburg;Bremerhaven"



            freight_surcharges_casablanca = All_Trades_Export_MSC.India_fix.surcharges_naf(self, "^CASABLANCA$", "^TUNIS$")
            freight_surcharges_casablanca = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_casablanca)
            if len(freight_surcharges_casablanca.columns) == 5:
                columns = ["charges",  "amount", "load_type", "remarks", "drop1"]
                freight_surcharges_casablanca_result = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols_destination_portname(self, freight_surcharges_casablanca, columns, "CASABLANCA")
                freight_surcharges_casablanca_result.loc[freight_surcharges_casablanca_result["charges"].str.contains("Hamburg/Bremerhaven", case=False, na=False), "origin_port"] = "Hamburg;Bremerhaven"


            freight_surcharges_tunis = All_Trades_Export_MSC.India_fix.surcharges_naf(self, "^TUNIS$", "LIBYA$")
            freight_surcharges_tunis = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_tunis)
            if len(freight_surcharges_tunis.columns) == 6:
                columns = ["charges", "drop2", "amount", "load_type", "remarks", "drop1"]
                freight_surcharges_tunis_result = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols_destination_portname(self, freight_surcharges_tunis, columns, "TUNIS")
                freight_surcharges_tunis_result.loc[freight_surcharges_tunis_result["charges"].str.contains("Hamburg/Bremerhaven", case=False, na=False), "origin_port"] = "Hamburg;Bremerhaven"

            freight_surcharges_libya = All_Trades_Export_MSC.India_fix.surcharges_(self,  "^LIBYA$", "^LOCAL CHARGES AT ORIGIN")
            freight_surcharges_libya = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_libya)
            if len(freight_surcharges_libya.columns) == 5:
                columns = ["charges", "drop1", "amount", "load_type", "remarks"]
                freight_surcharges_libya_result = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols_destination_portname(self, freight_surcharges_libya, columns, "LIBYA")
                freight_surcharges_libya_result["destination_country"] = freight_surcharges_libya_result["destination_port"]
                freight_surcharges_libya_result.drop(columns = "destination_port", inplace =True)
            # surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SPECIAL SURCHARGES", "^REMARKS")
            # surcharges_administration_df = All_Trades_Export_MSC.India_fix.clean_surcharges_administration(self, surcharges_df)

            special_surcharges = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SPECIAL SURCHARGES", None)
            special_surcharges = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, special_surcharges)
            if len(special_surcharges.columns) == 4:
                columns = ["charges", "amount", "load_type", "remarks"]
                special_surcharges_result = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, special_surcharges, columns)

            combined_surcharges_df = pd.concat([surcharges_local_origin,
                                                freight_surcharges_result,
                                                freight_surcharges_algeria_result,
                                                freight_surcharges_casablanca_result,
                                                freight_surcharges_tunis_result,
                                                freight_surcharges_libya_result,
                                                special_surcharges_result

                                                ], ignore_index=True)
            combined_surcharges_df["start_date"], combined_surcharges_df["expiry_date"] = validity["start_date"],  validity["end_date"]
            # inclusion = combined_surcharges_df.loc[combined_surcharges_df["amount"].str.contains("incl", na=False, case=False)]
            # if not inclusion.empty:
            #     incl = inclusion["charges"].unique()
            #     incl = ";".join(incl)
            #     freight_df["inclusions"] = incl

            combined_surcharges_df = combined_surcharges_df.loc[~combined_surcharges_df["amount"].str.contains("incl", na=False, case=False)]

            self.captured_output = {"Freight": freight_df, "Charges": combined_surcharges_df}

    class Red_sea(Naf_fix):
        def clean_surcharge_with_cols(self, surcharges_df, columns):

            surcharges_df.columns = columns
            surcharges_df["charges"].fillna(method='ffill', inplace =True)
            if "origin_port" in surcharges_df:
                surcharges_df["origin_port"].fillna(method='ffill', inplace =True)
            surcharges_df = surcharges_df.drop(columns=[column for column in surcharges_df.columns if column.startswith('drop')])
            surcharges_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_df_with_rates = surcharges_df.loc[surcharges_df["amount"].str.contains("^\d+",  na =False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"]= surcharges_df_with_rates["amount"].str.split(" ").str[1], surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = surcharges_df.loc[~surcharges_df["amount"].str.contains("^\d", na =False)]
            surcharges_df_without_rates["remarks"], surcharges_df_without_rates["amount"] = surcharges_df_without_rates["amount"], ""
            surcharges_result = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index= True)
            # replace_currency = {"USD": "", "EUR": ""}
            # replace_load_type = {"per 40' Ctr": "40GP=;40HC", "per 40´": "40GP=;40HC", "40'": "40GP=;40HC", "per 40'DC/HC":"40GP=;40HC", "per 40'DV/HC":"40GP=;40HC"}
            surcharges_result["load_type"].replace(replace_currency, regex=True, inplace=True)
            surcharges_result["load_type"] = surcharges_result["load_type"].str.strip()
            surcharges_result["load_type"].replace(replace_load_type, inplace=True)
            #surcharges_result.loc[surcharges_result["charges"].str.contains("if requested" , case= False,   na =False), "remarks"] = "if requested"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("request" , case= False,  na =False), "amount"] = "ON REQUEST"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("incl" , case= False,  na =False), "amount"] = "incl"
            surcharges_result.loc[surcharges_result["charges"].str.contains("Antwerp"), "origin_port"] = "Antwerp"
            surcharges_result.loc[surcharges_result["charges"].str.contains("IMO"), "cargo_type"] = "ONLY"
            surcharges_result["charges_leg"] = "L3"

            surcharges_result.loc[surcharges_result["charges"].str.contains("origin" , case= False,   na =False), "charges_leg"] = "L2"
            surcharges_result.loc[surcharges_result["charges"].str.contains("destination" , case= False,   na =False), "charges_leg"] = "L4"
            surcharges_result["remarks"].fillna("", inplace=True)
            surcharges_result["remarks"].replace(replace_currency, regex=True, inplace =True)
            surcharges_result.loc[surcharges_result["charges"].str.contains("if requested" , case= False,  na =False), "remarks"] = "if requested"
            if "load_type" in surcharges_result:
                surcharges_result = All_Trades_Export_MSC.India_fix.explode_load_types(self, surcharges_result)

            return surcharges_result

        def apply_port_on_charges(self,  freight_df, freight_surcharges_df_result):
            dps = []
            for check_port in ["Trieste", "NWC"]:
                if check_port == "NWC":
                    pol_nwc = freight_df.loc[~freight_df["origin_port"].str.contains("Trieste", case=False, na=False)][["origin_port", "destination_port"]].drop_duplicates()
                else:
                    pol_nwc = freight_df.loc[freight_df["origin_port"].str.contains(check_port, case=False, na=False)][["origin_port", "destination_port"]].drop_duplicates()

                pol_nwc_dict = pol_nwc.to_dict("records")
                for port in pol_nwc_dict:

                    freight_surcharges_df_result_copy = freight_surcharges_df_result.loc[freight_surcharges_df_result["charges"].str.contains(check_port, case=False, na=False)]
                    freight_surcharges_df_result_copy.loc[freight_surcharges_df_result_copy["charges"].str.contains(check_port, case=False, na=False), ("origin_port", "destination_port")] = port["origin_port"], port["destination_port"]
                    dps.append(freight_surcharges_df_result_copy)

            applied_port_charges = pd.concat(dps, ignore_index=True)
            applied_port_charges = applied_port_charges.loc[~((applied_port_charges["origin_port"].str.contains("Trieste")) & (applied_port_charges["destination_port"].str.contains("Port Sudan")))]
            return applied_port_charges


        def apply_port_on_charges_origin_port(self,  freight_df, freight_surcharges_df_result):
            dps = []
            for check_port in ["Trieste", "NWC"]:
                if check_port == "NWC":
                    pol_nwc = freight_df.loc[~freight_df["origin_port"].str.contains("Trieste", case=False, na=False)]["origin_port"].drop_duplicates()
                else:
                    pol_nwc = freight_df.loc[freight_df["origin_port"].str.contains(check_port, case=False, na=False)]["origin_port"].drop_duplicates()

                # pol_nwc_dict = pol_nwc.to_dict("records")
                for port in pol_nwc:
                    if check_port == "NWC":
                        freight_surcharges_df_result_copy = freight_surcharges_df_result.loc[~freight_surcharges_df_result["charges"].str.contains("Trieste", case=False, na=False)]
                        freight_surcharges_df_result_copy.loc[~freight_surcharges_df_result_copy["charges"].str.contains("Trieste", case=False, na=False), "origin_port"] = port
                        dps.append(freight_surcharges_df_result_copy)
                    else:
                        freight_surcharges_df_result_copy = freight_surcharges_df_result.loc[freight_surcharges_df_result["charges"].str.contains(check_port, case=False, na=False)]
                        freight_surcharges_df_result_copy.loc[freight_surcharges_df_result_copy["charges"].str.contains(check_port, case=False, na=False), "origin_port"] = port
                        dps.append(freight_surcharges_df_result_copy)

            applied_port_charges = pd.concat(dps, ignore_index=True)

            return applied_port_charges

        def apply_port_on_imbalance_charges(self,  freight_df, freight_surcharges_df_result):
            dps = []
            for check_port in ["Trieste", "NWC"]:
                if check_port == "NWC":
                    pol_nwc = freight_df.loc[~freight_df["origin_port"].str.contains("Trieste", case=False, na=False)][["origin_port", "destination_port"]].drop_duplicates()
                else:
                    pol_nwc = freight_df.loc[freight_df["origin_port"].str.contains(check_port, case=False, na=False)][["origin_port", "destination_port"]].drop_duplicates()

                pol_nwc_dict = pol_nwc.to_dict("records")
                for port in pol_nwc_dict:
                    freight_surcharges_df_result_copy = freight_surcharges_df_result.loc[(freight_surcharges_df_result["origin_port"].str.contains(check_port, case=False, na=False)) & (freight_surcharges_df_result["charges"].str.contains( "EQUIPMENT IMBALANCE SURCHARGE", case=False, na=False))]
                    freight_surcharges_df_result_copy.loc[(freight_surcharges_df_result["origin_port"].str.contains(check_port, case=False, na=False)) & (freight_surcharges_df_result["charges"].str.contains( "EQUIPMENT IMBALANCE SURCHARGE", case=False, na=False)), ("origin_port", "destination_port")] = port["origin_port"], port["destination_port"]
                    dps.append(freight_surcharges_df_result_copy)

            applied_port_charges = pd.concat(dps, ignore_index=True)

            return applied_port_charges

        def apply_port_on_imbalance_charges_origin_port(self,  freight_df, freight_surcharges_df_result):
            dps = []
            for check_port in ["Trieste", "NWC"]:
                if check_port == "NWC":
                    pol_nwc = freight_df.loc[~freight_df["origin_port"].str.contains("Trieste", case=False, na=False)]["origin_port"].drop_duplicates()
                else:
                    pol_nwc = freight_df.loc[freight_df["origin_port"].str.contains(check_port, case=False, na=False)]["origin_port"].drop_duplicates()

                for port in pol_nwc:
                    freight_surcharges_df_result_copy = freight_surcharges_df_result.loc[(freight_surcharges_df_result["origin_port"].str.contains(check_port, case=False, na=False)) & (freight_surcharges_df_result["charges"].str.contains( "EQUIPMENT IMBALANCE SURCHARGE", case=False, na=False))]
                    freight_surcharges_df_result_copy.loc[(freight_surcharges_df_result["origin_port"].str.contains(check_port, case=False, na=False)) & (freight_surcharges_df_result["charges"].str.contains( "EQUIPMENT IMBALANCE SURCHARGE", case=False, na=False)), "origin_port"] = port
                    dps.append(freight_surcharges_df_result_copy)

            applied_port_charges = pd.concat(dps, ignore_index=True)

            return applied_port_charges



        def capture(self):
            freight_df = self.get_freight_df()
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)
            contract_reference_df = All_Trades_Export_MSC.Fak_Rates_Fix.get_contract_reference(self)
            # if "contract_id" in freight_df:
            #     freight_df.loc[freight_df["contract_id"].isna(), "contract_id"] = contract_reference_df.iloc[0][0]
            freight_df["start_date"], freight_df["expiry_date"] = validity["start_date"], validity["end_date"]


            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT TIME OF", "^Aqaba - Surcharges")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 5:
                columns = ["charges", "origin_port", "amount", "load_type", "remarks"]

                freight_surcharges_df_result = self.clean_surcharge_with_cols(surcharges_df, columns)
                freight_surcharges_df_result.loc[freight_surcharges_df_result["charges"].str.contains("ON REQUEST" , case= False,  na =False), "amount"] = "ON REQUEST"
                freight_surcharges_df_result_with_nwc = self.apply_port_on_charges(freight_df, freight_surcharges_df_result)
                freight_surcharges_imbalance_charges_df = self.apply_port_on_imbalance_charges(freight_df, freight_surcharges_df_result)
                freight_surcharges_df_result = freight_surcharges_df_result.loc[~(freight_surcharges_df_result["charges"].str.contains("Bunker Recovery Charge / BRC", case=False, na=False)) & ~(freight_surcharges_df_result["charges"].str.contains("EQUIPMENT IMBALANCE SURCHARGE", case=False, na=False))]



            freight_surcharges_agaba = All_Trades_Export_MSC.India_fix.surcharges_(self, "^Aqaba - Surcharges", "^DJIBOUTI - Surcharges")
            freight_surcharges_agaba = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_agaba)
            if len(freight_surcharges_agaba.columns) == 5:
                columns = ["charges", "drop1", "amount", "load_type", "remarks"]
                freight_surcharges_agaba_result = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols_destination_portname(self, freight_surcharges_agaba, columns , "Aqaba")

            djibouti_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^DJIBOUTI - Surcharges", "^YEMEN - Mukalla, Aden, Hodeidah - Surcharges")
            djibouti_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, djibouti_surcharges_df)
            if len(djibouti_surcharges_df.columns) == 4:
                columns = ["charges", "remarks", "amount", "load_type"]
                djibouti_surcharges_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols_destination_portname(self, djibouti_surcharges_df, columns, "DJIBOUTI")

            freight_surcharges_yemen = All_Trades_Export_MSC.India_fix.surcharges_(self, "^YEMEN - Mukalla, Aden, Hodeidah - Surcharges", "^PORT SUDAN - Surcharges")
            freight_surcharges_yemen = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self,freight_surcharges_yemen)
            if len(freight_surcharges_yemen.columns) == 5:
                columns = ["charges", "drop1", "amount","load_type", "remarks"]
                freight_surcharges_yemen_result = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols_destination_portname(self, freight_surcharges_yemen, columns, "YEMEN")
                freight_surcharges_yemen_result.rename(columns={"destination_port" : "destination_country"}, inplace =True)

            freight_surcharges_sudan = All_Trades_Export_MSC.India_fix.surcharges_(self, "PORT SUDAN - Surcharges", "^LOCAL CHARGES AT ORIGIN")
            freight_surcharges_sudan = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_sudan)
            if len(freight_surcharges_sudan.columns) == 5:
                columns = ["charges", "drop1", "amount", "load_type", "drop2"]
                freight_surcharges_port_sudan_result = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols_destination_portname(self, freight_surcharges_sudan, columns, "PORT SUDAN")
                # freight_surcharges_port_sudan_result.loc[freight_surcharges_port_sudan_result["charges"].str.contains("Trieste" , case=False,  na=False), "origin_port"] = "Trieste"

                freight_surcharges_port_sudan_result_bunker_charge = freight_surcharges_port_sudan_result.loc[freight_surcharges_port_sudan_result["charges"].str.contains("Bunker Recovery Charge / BRC", case=False, na=False)]
                freight_surcharges_port_sudan_result_bunker_charge_result = self.apply_port_on_charges_origin_port( freight_df, freight_surcharges_port_sudan_result_bunker_charge)


                freight_surcharges_port_sudan_imbalance_charge = freight_surcharges_port_sudan_result.loc[freight_surcharges_port_sudan_result["charges"].str.contains("EQUIPMENT IMBALANCE SURCHARGE", case=False, na=False)]
                freight_surcharges_port_sudan_imbalance_charge_result = self.apply_port_on_charges_origin_port( freight_df, freight_surcharges_port_sudan_imbalance_charge)


                freight_surcharges_port_sudan_result = freight_surcharges_port_sudan_result.loc[~(freight_surcharges_port_sudan_result["charges"].str.contains("Bunker Recovery Charge / BRC", case=False, na=False)) & ~(freight_surcharges_port_sudan_result["charges"].str.contains("EQUIPMENT IMBALANCE SURCHARGE", case=False, na=False))]

            surcharges_local_origin = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN", "^SPECIAL SURCHARGES$")
            surcharges_local_origin = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_origin)
            if len(surcharges_local_origin.columns) == 4:
                columns = ["charges", "origin_port", "amount", "load_type"]
                freight_surcharges_tunis_result = All_Trades_Export_MSC.India_fix.clean_surcharges_local_origin_with_cols( self, surcharges_local_origin, columns)

            surcharges_special_surcharges = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SPECIAL SURCHARGES", "^LOCAL CHARGES AT DESTINATION")
            surcharges_special_surcharges = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_special_surcharges)
            if len(surcharges_special_surcharges.columns) == 5:
                columns = ["charges", "origin_port", "amount", "load_type", "remarks"]
                surcharges_special_surcharges_result = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, surcharges_special_surcharges, columns)
                surcharges_special_surcharges_result["origin_port"] = surcharges_special_surcharges_result["origin_port"].replace("/", ";", regex=True)

            surcharges_local_destination = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT DESTINATION", None)
            surcharges_local_destination = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_destination)
            if len(surcharges_local_destination.columns) == 5:
                columns = ["charges", "remarks", "amount", "load_type", "drop1"]
                surcharges_local_destination_result = All_Trades_Export_MSC.India_fix.clean_surcharges_local_destination_with_cols(self, surcharges_local_destination, columns)
                surcharges_local_destination_result["charges_leg"] = "L4"
                surcharges_local_destination_result["destination_port"] = surcharges_local_destination_result["charges"].str.split("-").str[0]

            combined_surcharges_df = pd.concat([
                                                freight_surcharges_df_result,
                                                freight_surcharges_agaba_result ,
                                                djibouti_surcharges_result_df,
                                                freight_surcharges_yemen_result,
                                                freight_surcharges_port_sudan_result,
                                                freight_surcharges_tunis_result,
                                                surcharges_special_surcharges_result,
                                                surcharges_local_destination_result,
                                                freight_surcharges_df_result_with_nwc,
                                                freight_surcharges_imbalance_charges_df,
                                                freight_surcharges_port_sudan_result_bunker_charge_result,
                                                freight_surcharges_port_sudan_imbalance_charge_result
                                                ], ignore_index=True)
            combined_surcharges_df["start_date"], combined_surcharges_df["expiry_date"] = validity["start_date"], validity["end_date"]

            inclusion = combined_surcharges_df.loc[combined_surcharges_df["amount"].str.contains("incl", na=False, case=False)]
            if not inclusion.empty:
                incl = inclusion["charges"].unique()
                incl = ";".join(incl)
                freight_df["inclusions"] = incl

            combined_surcharges_df["remarks"].replace("reviewed monthly", "", regex=True, inplace=True)
            combined_surcharges_df = combined_surcharges_df.loc[~combined_surcharges_df["amount"].str.contains("incl", na=False, case=False)]

            self.captured_output = {"Freight": freight_df, "Charges": combined_surcharges_df}

    class West_africa_fix(Indian_ocean_fix):

        def clean_surcharges_local_origin_with_charges(self, charges, surcharges_local_origin_df, columns):

            surcharges_local_origin_df.columns = columns
            surcharges_local_origin_df["charges"] = charges
            surcharges_local_origin_df = surcharges_local_origin_df.drop(columns=[column for column in surcharges_local_origin_df.columns if column.startswith('drop')])

            if "destination_port" in surcharges_local_origin_df:
                surcharges_local_origin_df["destination_port"].fillna(method='ffill', inplace=True)

                surcharges_local_origin_df.dropna(subset=["destination_port", "amount"], inplace = True)
            surcharges_df_with_rates = surcharges_local_origin_df.loc[surcharges_local_origin_df["amount"].str.contains("^\d+",  na =False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"]= surcharges_df_with_rates["amount"].str.split(" ").str[1], surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = surcharges_local_origin_df.loc[~surcharges_local_origin_df["amount"].str.contains("^\d", na =False)]
            surcharges_df_without_rates["remarks"], surcharges_df_without_rates["amount"] = surcharges_df_without_rates["amount"], ""
            surcharges_result = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index= True)
            self.replace_currency = {"USD": "", "EUR": ""}

            if "load_type" in surcharges_result:
                surcharges_result["load_type"].replace(self.replace_currency, regex =True , inplace =True)
            surcharges_result.loc[surcharges_result["charges"].str.contains("if requested" , case= False,   na =False), "remarks"] = "if requested"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("request" , case= False,  na =False), "amount"] = "ON REQUEST"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("incl" , case= False,  na =False), "amount"] = "incl"

            surcharges_result["charges_leg"] = "L2"
            surcharges_result["remarks"].replace(self.replace_currency, regex=True, inplace =True)

            if "load_type" in surcharges_result:
                surcharges_result["load_type"].replace(replace_load_type, inplace =True)

            return surcharges_result

        def apply_inclusions(self, freight_df, inclusions_df):

            inclusions_df = inclusions_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            inclusions_df.fillna("", inplace=True)
            inclusions_dict = inclusions_df.to_dict("records")
            for row in inclusions_dict:
                if "destination_port" in row:
                    if row["destination_port"] == "":
                        if "inclusions" in freight_df:
                            freight_df["inclusions"] += ";" + row["charges"]
                        else:
                            freight_df["inclusions"] = row["charges"]
                    else:
                        freight_df.loc[freight_df["destination_port"].str.contains(row["destination_port"], case=False, na=False), "inclusions"] += ";" + row["charges"]

            return freight_df


        def get_freight_df(self):

            df = self.df
            start_index = df[df[0] == 'SEAFREIGHT'].index.values[0]
            end_index = df[df[0].str.contains("SURCHARGES - VALID", na=False, case=False)].index.values[0]

            if df.iloc[:, 0].str.contains('ex').any():
                pol_index = df[(df.iloc[:, 0].str.contains('ex', na =False))].index.values[0]
                pol = self.df.loc[int(pol_index)][0]

            freight_df = df.loc[start_index: end_index - 1, :]
            freight_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_df)
            freight_df.columns = ["destination_port", "20GP", "40GP", "40HC", "remarks"]
            freight_df = freight_df.applymap(lambda x: nan if x == '' else x)
            freight_df = freight_df.drop(columns=[column for column in freight_df.columns if column.startswith('drop')])
            freight_df.dropna(subset=["destination_port", "40GP"], inplace=True)
            freight_df = freight_df.loc[~freight_df["20GP"].str.contains("20' DV", case=False, na=False)]
            pol = pol.split("ex")[1]
            dps = []
            for origin_port in pol.split(","):
                freight_df_copy = freight_df.copy()
                freight_df_copy["origin_port"] = origin_port
                dps.append(freight_df_copy)
            freight_result_df = pd.concat(dps, ignore_index=True)
            freight_result_df["40HC"] = freight_result_df["40GP"].copy()
            freight_result_df = freight_result_df.loc[freight_result_df["20GP"] != "20'DV"]
            freight_result_df["destination_port"].replace({"/":";"}, regex =True, inplace = True)
            return freight_result_df

        def capture(self):
            freight_df = self.get_freight_df()
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)
            contract_reference_lookup = All_Trades_Export_MSC.Fak_Rates_Fix.get_contract_reference(self)
            freight_df["remarks"].fillna("", inplace=True)
            freight_df["start_date"], freight_df["expiry_date"], freight_df["contract_id"] = validity["start_date"], validity["end_date"], str(contract_reference_lookup.iloc[0][0]) + "\n" + freight_df["remarks"]

            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT TIME OF", "^LOCAL CHARGES - VALID AT TIME OF SHIPMENT")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 6:
                columns = ["charges", "drop1", "portnames", "amount", "load_type", "remarks"]
                surcharges_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, surcharges_df, columns)
                surcharges_result_df.loc[surcharges_result_df["charges"].str.contains("Agadir", case=False, na=False), "destination_port"] = "Agadir"

            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES - VALID AT TIME OF SHIPMENT", "Emergency Congestion")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 4:
                columns = ["charges", "origin_port", "amount", "load_type"]
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.clean_surcharges_local_origin_with_cols(self,surcharges_df, columns)

            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "Emergency Congestion", "Congestion Surcharge")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 5:
                columns = [ "destination_port", "remarks", "amount", "load_type", "drop1"]
                surcharges_local_origin_emergency_congestion_df = self.clean_surcharges_local_origin_with_charges( "Emergency Congestion", surcharges_df, columns)

            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "Congestion Surcharge", "Terminal Handling Charge")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 4:
                columns = [ "destination_port", "remarks", "amount", "load_type"]
                surcharges_local_origin_terminal_handling_charges_df = self.clean_surcharges_local_origin_with_charges("Terminal Handling Charge", surcharges_df, columns)


            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "Congestion Surcharge", "Terminal Handling Charge")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 4:
                columns = ["destination_port", "remarks", "amount", "load_type"]
                surcharges_local_origin_congestion_surcharge_charges_df = self.clean_surcharges_local_origin_with_charges("Congestion Surcharge", surcharges_df, columns)


            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "Terminal Handling Charge", "Logistic Fee")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 4:
                columns = ["destination_port", "amount", "load_type",  "remarks"]
                surcharges_local_origin_terminal_handling_charge_df = self.clean_surcharges_local_origin_with_charges("Terminal Handling Charge", surcharges_df, columns)


            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "Logistic Fee", "SPD at Destination")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 4:
                columns = ["destination_port", "amount", "load_type",  "remarks"]
                surcharges_local_origin_logistic_fee_charge_df = self.clean_surcharges_local_origin_with_charges("Logistic Fee", surcharges_df, columns)

            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "SPD at Destination", "Customs Duty Surcharge")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 4:
                columns = ["destination_port", "amount", "load_type", "remarks"]
                surcharges_local_origin_spd_charge_df = self.clean_surcharges_local_origin_with_charges("SPD", surcharges_df, columns)

            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "Customs Duty Surcharge", "Port Surcharge")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 4:
                columns = ["destination_port", "amount", "load_type", "remarks"]
                surcharges_local_origin_customs_duty_charge_df = self.clean_surcharges_local_origin_with_charges("Customs Duty Surcharge", surcharges_df, columns)

            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "Port Surcharge", "Documentation Handling Fee")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 4:
                columns = ["destination_port", "amount", "load_type", "remarks"]
                surcharges_local_origin_port_surcharges_charge_df = self.clean_surcharges_local_origin_with_charges("Port Surcharge", surcharges_df, columns)

            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "Documentation Handling Fee", "Angola Operations Additional")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 2:
                columns = ["destination_port", "amount"]
                surcharges_local_origin_doc_handling_charge_df = self.clean_surcharges_local_origin_with_charges("Documentation Handling Fee (DHF)", surcharges_df, columns)

            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "Angola Operations Additional", "SPECIAL SURCHARGES")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 2:
                columns = ["destination_port", "amount"]
                surcharges_local_origin_angola_operations_additional_charge_df = self.clean_surcharges_local_origin_with_charges("Angola Operations Additional", surcharges_df, columns)

            special_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "SPECIAL SURCHARGES", None)
            special_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, special_surcharges_df)
            if len(special_surcharges_df.columns) == 4:
                columns = ["charges", "amount", "load_type", "remarks"]
                speical_surcharges_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, special_surcharges_df, columns)


            # surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT DESTINATION - VALID AT TIME", "ONCARRIAGE")
            # surcharges_local_destination_df = All_Trades_Export_MSC.India_fix.clean_surcharges_local_destination(self, surcharges_df)

            combined_surcharges_df = pd.concat([surcharges_result_df,
                                                surcharges_local_origin_df,
                                                surcharges_local_origin_emergency_congestion_df,
                                                surcharges_local_origin_congestion_surcharge_charges_df,
                                                surcharges_local_origin_terminal_handling_charge_df,
                                                surcharges_local_origin_logistic_fee_charge_df,
                                                surcharges_local_origin_port_surcharges_charge_df,
                                                surcharges_local_origin_doc_handling_charge_df,
                                                surcharges_local_origin_angola_operations_additional_charge_df,
                                                speical_surcharges_result_df
                                                ], ignore_index=True)

            combined_surcharges_df["start_date"], combined_surcharges_df["expiry_date"] = validity["start_date"], validity["end_date"]
            inclusions = combined_surcharges_df.loc[combined_surcharges_df["amount"].str.contains("incl", na=False, case=False)]

            inclusions = inclusions[["charges", "destination_port"]]
            inclusions.rename(columns={"origin_port": "destination_port"}, inplace =True)
            inclusions["destination_port"] = inclusions["destination_port"].str.split("/")
            inclusions = inclusions.explode("destination_port")
            freight_df = self.apply_inclusions(freight_df, inclusions)
            combined_surcharges_df = combined_surcharges_df.loc[~combined_surcharges_df["amount"].str.contains("incl", na=False, case=False)]
            combined_surcharges_df.loc[(combined_surcharges_df["amount"].str.contains("request", case=False,  na=False)) & (combined_surcharges_df["load_type"].isna()), "load_type"] = "per Ctr"
            combined_surcharges_df.loc[(combined_surcharges_df["amount"].str.contains("request", case=False,  na=False)) & (combined_surcharges_df["currency"].isna()), "currency"] = "EUR"
            combined_surcharges_df.loc[combined_surcharges_df["remarks"].str.contains("monthly update", case=False,  na=False), "remarks"] = ""
            combined_surcharges_df.loc[combined_surcharges_df["remarks"].str.contains("per B/L", case=False,  na=False), "remarks"] = ""
            self.captured_output = {'Freight': freight_df, "Charges": combined_surcharges_df}

    class Bls(BaseFix):

        def check_input(self):

            pass

        def check_output(self):

            pass

        def agreement_number(self, test_str):

            remarks_index = list(self.df[self.df[0].str.contains(test_str, na=False)].index)[0]
            if ':' in self.df.iloc[remarks_index, 1]:
                remarks = {}
                remarks[self.df.iloc[remarks_index, 1].split()[0][:-1]] = self.df.iloc[remarks_index, 1].split()[1]
                remarks[self.df.iloc[remarks_index, 1].split()[2][:-1]] = self.df.iloc[remarks_index, 1].split()[3]
            else:
                remarks = self.df.iloc[remarks_index, 1].split()[0]
            return remarks

        def get_headers(self):

            if self.df[0].str.contains('Valid as from:').any():
                start_date_index = list(self.df[self.df[0].str.contains('Valid as from:', na=False)].index)[0]
                start_date = parse(self.df.iloc[start_date_index, 0].split(':')[-1].strip())

            if self.df[0].str.contains('Valid until').any():
                expiry_date_index = list(self.df[self.df[0].str.contains('Valid until', na=False)].index)[0]
                expiry_date = parse(self.df.iloc[expiry_date_index, 0].split()[-1].strip())

            if self.df[0].str.contains('Contract / Filing Reference').any():
                remarks = self.agreement_number('Contract / Filing Reference')

            elif self.df[0].str.contains('Contract Reference').any():
                remarks = self.agreement_number('Contract Reference')

            dates = {'start_date': start_date, 'expiry_date': expiry_date}

            return dates, remarks

        def get_main_df(self):

            if self.df[0].str.contains('Yellow Highlight', na=False).any():
                yellow_index = list(self.df[self.df[0].str.contains('Yellow Highlight', na=False)].index)[0]
                df, yellow_df = self.df.iloc[:yellow_index, :], self.df.iloc[yellow_index:, :]
            else:
                df, yellow_df = self.df, None
            return df, yellow_df

        def get_indexes(self, df):

            if df[0].str.contains('CHARGES', na=False).any():
                charges_index = list(df[df[0].str.contains('CHARGES', na=False)].index)
            else:
                charges_index = None
            if df[0].str.contains('POL', na=False).any():
                pol_index = list(df[df[0].str.contains('POL', na=False)].index)
            else:
                pol_index = None
            if df[0].str.contains('POD', na=False).any():
                pod_index = list(df[df[0].str.contains('POD', na=False)].index)
            else:
                pod_index = None

            return charges_index, pol_index, pod_index

        def apply_pols(self, main_df, pols=None):

            dfs = []
            cols = [column for column in main_df.columns if column[1].isdigit()]
            main_df['currency'] = main_df[cols[0]].str.split(expand=True).iloc[:, 1]
            main_df[cols[0]] = main_df[cols[0]].str.replace('[A-Z]', '', regex=True)
            if len(cols) > 3:
                main_df[cols[1]] = main_df[cols[1]].str.replace('[A-Z]', '', regex=True)
                main_df[cols[2]] = main_df[cols[2]].str.replace('[A-Z]', '', regex=True)
                main_df[cols[3]] = main_df[cols[3]].str.replace('[A-Z]', '', regex=True)
                main_df.rename(columns={cols[0]: '20GP', cols[1]: '40RE', cols[2]: '40GP', cols[3]: '40HC'}, inplace=True)
            else:
                main_df[cols[1]] = main_df[cols[1]].str.replace('[A-Z]', '', regex=True)
                main_df[cols[2]] = main_df[cols[2]].str.replace('[A-Z]', '', regex=True)
                main_df.rename(columns={cols[0]: '20GP', cols[1]: '40GP', cols[2]: '40HC'}, inplace=True)

            if pols is not None:
                for pol in pols:
                    holder = main_df.copy(deep=True)
                    holder['POL'] = pol.strip()
                    dfs.append(holder)
            if dfs:
                return pd.concat(dfs, ignore_index=True)
            else:
                return main_df

        def get_freight_df(self, df):

            charges_index, pol_index, pod_index = self.get_indexes(df)

            pols = df.iloc[pol_index, 0].iloc[0].split('POL')[-1].strip().split(',')

            if pol_index[0] < pod_index[0]:
                main_df = df.iloc[pod_index[0]:charges_index[0], :]
            else:
                main_df = df.iloc[pol_index[0]:charges_index[0], :]
                main_df.iloc[0, 0] = 'POD'
            main_df.columns = main_df.iloc[0, :]
            main_df = main_df.iloc[1:, :]
            main_df.drop([''], axis=1, inplace=True)
            if main_df['Remarks'].str.contains('via').any():
                via_series = main_df['Remarks'].str.split('via ', expand=True).iloc[:, 1].str.replace(' and ', ";", regex=True)\
                    .squeeze(axis=0)
                main_df["via"] = via_series

            return self.apply_pols(main_df, pols)

        def capture(self):

            dates, remarks = self.get_headers()
            df, yellow_df = self.get_main_df()
            freight_df = self.get_freight_df(df)
            freight_df['Remarks'] = remarks
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)

            freight_df.reset_index(drop=True, inplace=True)
            freight_df.rename(columns={'POD': 'destination_port', 'Remarks': 'remarks', 'POL': 'origin_port', "ROUTING_POINT" : "via"}, inplace=True)
            freight_df['start_date'], freight_df['expiry_date'] = validity['start_date'], validity['end_date']

            freight_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES  - RATES EUR", "^SURCHARGES  - RATES USD")
            freight_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_df)
            if len(freight_surcharges_df.columns) == 5:
                columns = ["charges", "drop1", "portname", "amount", "load_type"]
                freight_surcharges_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, freight_surcharges_df, columns)
                freight_surcharges_result_df.loc[freight_surcharges_result_df["charges"].str.contains("IMO", na=False, case=False), "destination_port"] = "Varna;Bourgas;Constanta"


            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT", "^SURCHARGES  - RATES EUR")
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_origin_df)
            if len(surcharges_local_origin_df.columns) == 4:
                columns = ["charges", "origin_port", "amount", "load_type"]
                local_charges = All_Trades_Export_MSC.India_fix.clean_surcharges_local_origin_with_cols(self, surcharges_local_origin_df, columns)

            freight_surcharges_2_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES  - RATES USD", None)
            freight_surcharges_2_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_2_df)
            if len(freight_surcharges_2_df.columns) == 6:
                columns = ["charges", "drop1", "portname", "amount", "load_type", "remarks"]
                freight_surcharges_2_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, freight_surcharges_2_df, columns)
                #freight_surcharges_2_result_df["currency"] = "USD"

            combined_surcharges_df = pd.concat([freight_surcharges_result_df, local_charges, freight_surcharges_2_result_df], ignore_index=True)

            combined_surcharges_df.loc[combined_surcharges_df["load_type"].str.contains("Class", na=False), "commodity"] = combined_surcharges_df["load_type"].str.split("IMO").str[1]
            combined_surcharges_df["commodity"] = combined_surcharges_df["commodity"].str.split("/")
            combined_surcharges_df = combined_surcharges_df.explode("commodity")

            combined_surcharges_df["commodity"].replace("Class", "", regex =True, inplace=True)
            combined_surcharges_df['start_date'], combined_surcharges_df['expiry_date'] = validity['start_date'], validity['end_date']
            combined_surcharges_df.loc[combined_surcharges_df["charges"].str.contains("no IMO cargo accepted for Giurgiulesti", na=False, case=False), "remarks"] = "(no IMO cargo accepted for Giurgiulesti)"
            combined_surcharges_df.loc[combined_surcharges_df["charges"].str.contains("incl. 5 positions", na=False, case=False), "remarks"] = "IMO (incl. 5 positions)"

            self.captured_output = {'Freight': freight_df, "Surcharges": combined_surcharges_df}

        def clean(self):

            if "Freight" in self.captured_output:
                freight_df = self.captured_output["Freight"]
                freight_df["start_date"], freight_df["expiry_date"] = pd.to_datetime(freight_df["start_date"], format="%d.%m.%y"), pd.to_datetime(freight_df["expiry_date"], format="%d.%m.%y")

                freight_df["charges"], freight_df["charges_leg"], freight_df["basis"] = "Basic Ocean Freight", "L3", "container"
                freight_df["contract_no"], freight_df["sub_vendor"] = "MSCMCDE", "MSC Mediterranean Shipping Company S.A. CORPORATION"

                if "load_type" in freight_df:
                    freight_df = All_Trades_Export_MSC.India_fix.explode_load_types(self, freight_df)


                freight_df = freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            if "Surcharges" in self.captured_output:
                surcharges_df = self.captured_output["Surcharges"]
                surcharges_df["start_date"], surcharges_df["expiry_date"] = pd.to_datetime(surcharges_df["start_date"], format="%d.%m.%y"), pd.to_datetime(surcharges_df["expiry_date"], format="%d.%m.%y")
                surcharges_df = surcharges_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                surcharges_df["contract_no"], surcharges_df["sub_vendor"] = "MSCMCDE",  "MSC Mediterranean Shipping Company S.A. CORPORATION"
                if "load_type" in surcharges_df:
                    surcharges_df = All_Trades_Export_MSC.India_fix.explode_load_types(self, surcharges_df)


            if "Arbitrary Charges" in self.captured_output:
                arbitary_df = self.captured_output["Arbitrary Charges"]
                arbitary_df["start_date"], arbitary_df["expiry_date"] = pd.to_datetime(arbitary_df["start_date"], format="%d.%m.%y"), pd.to_datetime(arbitary_df["expiry_date"], format="%d.%m.%y")
                arbitary_df = arbitary_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                arbitary_df["contract_no"], arbitary_df["sub_vendor"], arbitary_df["at"] = "MSCMCDE",  "MSC Mediterranean Shipping Company S.A. CORPORATION", "origin"
                if "load_type" in arbitary_df:
                    arbitary_df = All_Trades_Export_MSC.India_fix.explode_load_types(self, arbitary_df)

            if "Arbitrary Charges" in self.captured_output:
                self.cleaned_output = {"Freight": freight_df, "Charges" : surcharges_df, "Arbitrary Charges": arbitary_df}
            else:
                 self.cleaned_output = {"Freight": freight_df, "Charges": surcharges_df}

    class Canada_ec(Bls):

        def clean_surcharges_local_destination_with_cols(self, surcharges_local_destination_df, columns):
            surcharges_local_destination_df.columns = columns
            surcharges_local_destination_df["charges"].fillna(method='ffill', inplace =True)
            if "destination_country" in surcharges_local_destination_df:
                thc = surcharges_local_destination_df['charges'].str.contains("THC")
                surcharges_local_destination_df.loc[thc, 'destination_country'] = surcharges_local_destination_df.loc[thc, 'destination_country'].fillna(method='ffill')


            surcharges_local_destination_df.dropna(subset=["charges", "amount"], inplace=True)
            surcharges_local_destination_df = surcharges_local_destination_df.drop(columns=[column for column in surcharges_local_destination_df.columns if column.startswith('drop')])
            surcharges_local_destination_df["load_type"].replace(replace_currency,  regex =True, inplace=True)
            surcharges_local_destination_df["load_type"] = surcharges_local_destination_df["load_type"].str.strip()
            surcharges_local_destination_df["load_type"].replace(replace_load_type, inplace=True)
            surcharges_local_destination_df["currency"], surcharges_local_destination_df["amount"], surcharges_local_destination_df["charges_leg"] = surcharges_local_destination_df["amount"].str.split(" ").str[1], surcharges_local_destination_df["amount"].str.split(" ").str[0], "L4"
            if "destination_port" in surcharges_local_destination_df:
                surcharges_local_destination_df["destination_port"] = surcharges_local_destination_df["destination_port"].replace("/", ";", regex =True)
            surcharges_local_destination_df.loc[~surcharges_local_destination_df["charges"].str.contains("destination", case= False,  na=False), "charges_leg"] = "L3"

            return surcharges_local_destination_df

        def capture(self):
            dates, remarks = self.get_headers()
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)

            df, yellow_df = self.get_main_df()
            freight_df = self.get_freight_df(df)
            freight_df['Remarks'] = remarks
            freight_df.reset_index(drop=True, inplace=True)
            freight_df.rename(columns={'POD': 'destination_port', 'Remarks': 'remarks', 'POL': 'origin_port'}, inplace=True)
            freight_df["start_date"], freight_df["expiry_date"] = validity["start_date"],  validity["end_date"]
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN", "^SURCHARGES - VALID AT TIME")
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_origin_df)
            if len(surcharges_local_origin_df.columns) == 4:
                columns = ["charges", "origin_port", "amount", "load_type"]
                local_charges = All_Trades_Export_MSC.India_fix.clean_surcharges_local_origin_with_cols(self, surcharges_local_origin_df, columns)


            freight_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT TIME", "^LOCAL CHARGES AT DESTINATION")
            freight_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_df)
            if len(freight_surcharges_df.columns) == 5:
                columns = ["charges", "remarks", "amount", "load_type", "drop1"]
                freight_surcharges_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, freight_surcharges_df, columns)


            surcharges_local_destination_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT DESTINATION", "^ADMINISTRATIVE SURCHARGES - VALID AT")
            surcharges_local_destination_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_destination_df)
            if len(surcharges_local_destination_df.columns) == 5:
                columns = ["charges", "destination_country", "amount", "load_type", "remarks"]
                destination_charges = self.clean_surcharges_local_destination_with_cols(surcharges_local_destination_df, columns)


            surcharges_adminitration_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^ADMINISTRATIVE SURCHARGES - VALID AT", "^PRECARRIAGE")
            surcharges_adminitration_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_adminitration_df)
            if len(surcharges_adminitration_df.columns) == 4:
                columns = ["charges", "amount", "load_type", "remarks"]
                surcharges_adminitration_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, surcharges_adminitration_df, columns)

            surcharges_df = pd.concat([local_charges, freight_surcharges_result_df, destination_charges, surcharges_adminitration_result_df], ignore_index=True)
            surcharges_df["start_date"], surcharges_df["expiry_date"] = validity["start_date"],  validity["end_date"]
            self.captured_output = {'Freight': freight_df, "Surcharges": surcharges_df}

    class Caribic(Bls):

        def get_freight_df(self, df):

            charges_index, pol_index, pod_index = All_Trades_Export_MSC.Bls.get_indexes(self, df)
            freight_df = df.iloc[pol_index[0]:charges_index[0], :]
            pols = df.iloc[pol_index[0]:charges_index[0], :].iloc[0, 0].split()[-1].split('/')
            freight_df.iloc[0, 0] = 'POD'
            freight_df.columns = freight_df.iloc[0, :]
            freight_df = freight_df.iloc[1:, :]
            freight_df.reset_index(drop=True, inplace=True)

            if freight_df['POD'].str.contains('\\*', na=False).any():
                star_index = list(freight_df[freight_df['POD'].str.contains('\\*', na=False)].index)
                if freight_df["20\' DV"].tail(1).iloc[0] == '' and '*' in freight_df["POD"].tail(1).iloc[0]:
                    remark_add_on = freight_df.iloc[star_index[-1], 0]
                else:
                    remark_add_on = None

            if freight_df['POD'].str.contains('via ', na=False).any():
                pod_via = list(freight_df[freight_df['POD'].str.contains('via', na=False)].index)
                freight_df['ROUTING POINTS'] = ''
                for index in pod_via:
                    holder = freight_df['POD'][index]
                    pod = holder.split('via')[0].strip()
                    via = holder.split('via')[-1].strip().split()[0]
                    add_on = holder.split('X')[0].strip().split()[-1]
                    via_index = list(freight_df[freight_df['POD'] == via].index)[0]
                    freight_df.loc[index, 'POD'] = pod
                    freight_df.loc[index, 'ROUTING POINTS'] = via
                    freight_df.loc[index, '20\' DV'] = ' '.join(
                        [str(int(freight_df['20\' DV'][via_index].split()[0]) + int(add_on)),
                         freight_df['20\' DV'][via_index].split()[-1]])
                    freight_df.loc[index, '40\'DV/HC'] = ' '.join(
                        [str(int(freight_df['40\'DV/HC'][via_index].split()[0]) + int(add_on)),
                         freight_df['40\'DV/HC'][via_index].split()[-1]])

                    # freight_df.loc[index, '40\'RE'] = ' '.join(
                    #     [str(int(freight_df['40\'RE'][via_index].split()[0]) + int(add_on)),
                    #      freight_df['40\'RE'][via_index].split()[-1]])

            freight_df = freight_df.loc[freight_df['20\' DV'] != '']
            for column in freight_df.columns:
                if 'DV/HC' in column:
                    holder = column.split('\'')[-1].split('/')
                    freight_df[column.split('\'')[0].strip() + '\'' + holder[0].strip()] = freight_df[column]
                    freight_df[column.split('\'')[0].strip() + '\'' + holder[-1].strip()] = freight_df[column]
                    freight_df.drop(columns=[column, ''], inplace=True)
                    break
            freight_df = All_Trades_Export_MSC.Bls.apply_pols(self, freight_df, pols)

            return freight_df, remark_add_on

        def clean_surcharges_local_origin_with_cols_load_types(self, surcharges_local_origin_df, columns):
            surcharges_local_origin_df.columns = columns
            surcharges_local_origin_df["charges"].fillna(method='ffill', inplace=True)
            surcharges_local_origin_df.dropna(subset=["charges", "origin_port"], inplace=True)
            surcharges_local_origin_df["40HC"] = surcharges_local_origin_df["40GP"]
            surcharges_local_origin_df = surcharges_local_origin_df.drop(columns=[column for column in surcharges_local_origin_df.columns if column.startswith('drop')])
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.melt_load_type(self, surcharges_local_origin_df)
            surcharges_df_with_rates = surcharges_local_origin_df.loc[surcharges_local_origin_df["amount"].str.contains("^\d+", na=False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"] = surcharges_df_with_rates["amount"].str.split(" ").str[1], surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = surcharges_local_origin_df.loc[~surcharges_local_origin_df["amount"].str.contains("^\d", na=False)]
            surcharges_df_without_rates["remarks"], surcharges_df_without_rates["amount"] = surcharges_df_without_rates["amount"], ""
            surcharges_local_origin_df = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index=False)
            # surcharges_local_origin_df["currency"], surcharges_local_origin_df["amount"], surcharges_local_origin_df["charges_leg"] = surcharges_local_origin_df["amount"].str.split(" ").str[1], surcharges_local_origin_df["amount"].str.split(" ").str[0], "L2"
            surcharges_local_origin_df["origin_port"] = surcharges_local_origin_df["origin_port"].replace("/", ";", regex= True)
            return surcharges_local_origin_df


        def clean_surcharges_with_cols_load_types(self, surcharges_df, columns):
            surcharges_df.columns = columns
            surcharges_df["charges"].fillna(method='ffill', inplace=True)
            surcharges_df.dropna(subset=["charges", "20GP"], inplace=True)
            surcharges_df["40HC"] = surcharges_df["40GP"]
            surcharges_df = surcharges_df.drop(columns=[column for column in surcharges_df.columns if column.startswith('drop')])
            surcharges_df = All_Trades_Export_MSC.India_fix.melt_load_type(self, surcharges_df)
            surcharges_df_with_rates = surcharges_df.loc[surcharges_df["amount"].str.contains("^\d+", na=False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"] = surcharges_df_with_rates["amount"].str.split(" ").str[1], surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = surcharges_df.loc[~surcharges_df["amount"].str.contains("^\d", na=False)]
            surcharges_df_without_rates["remarks"], surcharges_df_without_rates["amount"] = surcharges_df_without_rates["amount"], ""
            surcharges_local_origin_df = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index=False)
            surcharges_df["currency"], surcharges_df["amount"], surcharges_df["charges_leg"] = surcharges_df["amount"].str.split(" ").str[1], surcharges_df["amount"].str.split(" ").str[0], "L3"
            surcharges_df.loc[surcharges_df["charges"].str.contains("Antwerp"), "origin_port"] = "Antwerp"

            return surcharges_df


        def capture(self):

            dates, remarks = All_Trades_Export_MSC.Bls.get_headers(self)
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)

            df, yellow_df = All_Trades_Export_MSC.Bls.get_main_df(self)
            freight_df, remark_add_on = self.get_freight_df(df)

            if remark_add_on is not None:
                freight_df['REMARKS'] = remarks
                freight_df.loc[freight_df['POD'].str.contains('\\*'), 'REMARKS'] \
                    = freight_df.loc[freight_df['POD'].str.contains('\\*'), 'REMARKS'] + '\n' + remark_add_on
                freight_df['REMARKS'] = freight_df['REMARKS'] + '\n' + freight_df['Comments']
            else:
                freight_df['REMARKS'] = remarks + '\n' + freight_df['Remark'] + '\n' + freight_df['Comments']
                freight_df.drop(columns=['Remark'], axis=1, inplace=True)
            freight_df['contract_id'] = freight_df['REMARKS'].apply(lambda x: x.strip('\n'))
            if 'Comments' in freight_df.columns:
                freight_df.drop(columns=['Comments'], axis=1, inplace=True)
            if 'DEM/DET free time at POD' in freight_df.columns:
                freight_df.drop(columns=['DEM/DET free time at POD'], axis=1, inplace=True)

            freight_df['POD'] = freight_df['POD'].apply(lambda x: x.strip(' *'))
            freight_df['start_date'], freight_df['expiry_date'] = validity['start_date'], validity['end_date']
            freight_df["start_date"], freight_df["expiry_date"] = pd.to_datetime(freight_df["start_date"], format="%d.%m.%y"), pd.to_datetime(freight_df["expiry_date"], format="%d.%m.%y")
            freight_df["charges"], freight_df["charges_leg"], freight_df["basis"] = "Basic Ocean Freight", "L3", "container"

            freight_df.replace('', nan, inplace=True)
            freight_df.dropna(how='all', axis=1, inplace=True)
            freight_df.rename(columns={'POD': 'destination_port', 'POL': 'origin_port', "REMARKS": "remarks", "ROUTING POINTS" : "via"}, inplace=True)

            #Surcharge capture

            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN", "^SURCHARGES - VALID AT TIME")
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_origin_df)
            if len(surcharges_local_origin_df.columns) == 5:
                columns = ["charges", "origin_port", "20GP", "40GP", "40RE"]
                local_charges_result_df = self.clean_surcharges_local_origin_with_cols_load_types(surcharges_local_origin_df, columns)

            freight_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT TIME","^ADMINISTRATIVE SURCHARGES - VALID")
            freight_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_df)
            if len(freight_surcharges_df.columns) == 5:
                columns = ["charges", "20GP", "40GP", "40RE", "remarks"]
                freight_surcharges_result_df = self.clean_surcharges_with_cols_load_types(freight_surcharges_df, columns)

                # for load_type in ["20GP", "40GP","40HC", "40RE"]:
                #     freight_surcharges_result_df.loc[freight_surcharges_result_df[load_type].str.contains("")]



            surcharges_administrative_df = All_Trades_Export_MSC.India_fix.surcharges_(self,"^ADMINISTRATIVE SURCHARGES - VALID", None)
            surcharges_administrative_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_administrative_df)
            if len(surcharges_administrative_df.columns) == 5:
                columns = ["charges", "20GP", "40GP", "40RE", "drop1"]
                surcharges_administrative_result_df = self.clean_surcharges_with_cols_load_types(surcharges_administrative_df, columns)


            surcharges_df = pd.concat([local_charges_result_df, freight_surcharges_result_df, surcharges_administrative_result_df], ignore_index=True)
            surcharges_df["start_date"], surcharges_df["expiry_date"] = validity["start_date"],  validity["end_date"]
            surcharges_df["amount"] = surcharges_df["amount"] + " " + surcharges_df["currency"]

            surcharges_df["amount"] = surcharges_df["amount"].apply(lambda x: All_Trades_Export_MSC.Indian_ocean_fix.convert_currency_format_with_currency(self, x.split()[0], x.split()[1]))
            surcharges_df["origin_port"] = surcharges_df["origin_port"].str.split(";")
            surcharges_df = surcharges_df.explode("origin_port")
            self.captured_output = {'Freight': freight_df, "Surcharges": surcharges_df}

    class Central_america(Bls):

        def get_freight_df(self, df):

            charges_index, pol_index, pod_index = All_Trades_Export_MSC.Bls.get_indexes(self, df)
            freight_df = df.iloc[pol_index[0]:charges_index[0], :]
            pols = df.iloc[pol_index[0]:charges_index[0], :].iloc[0, 0].split()[-1].split('/')
            freight_df.iloc[0, 0] = 'POD'
            freight_df.columns = freight_df.iloc[0, :]
            freight_df = freight_df.iloc[1:, :]
            freight_df.reset_index(drop=True, inplace=True)

            if freight_df['POD'].str.contains('\\*', na=False).any():
                star_index = list(freight_df[freight_df['POD'].str.contains('\\*', na=False)].index)
                if freight_df["20\' DV"].tail(1).iloc[0] == '' and '*' in freight_df["POD"].tail(1).iloc[0]:
                    remark_add_on = freight_df.iloc[star_index[-1], 0]
                else:
                    remark_add_on = None

            if freight_df['POD'].str.contains('via ', na=False).any():
                pod_via = list(freight_df[freight_df['POD'].str.contains('via', na=False)].index)
                freight_df['ROUTING POINTS'] = ''
                for index in pod_via:
                    holder = freight_df['POD'][index]
                    pod = holder.split('via')[0].strip()
                    via = holder.split('via')[-1].strip().split()[0]
                    add_on = holder.split('X')[0].strip().split()[-1]
                    via_index = list(freight_df[freight_df['POD'] == via].index)[0]
                    freight_df.loc[index, 'POD'] = pod
                    freight_df.loc[index, 'ROUTING POINTS'] = via
                    freight_df.loc[index, '20\' DV'] = ' '.join(
                        [str(int(freight_df['20\' DV'][via_index].split()[0]) + int(add_on)),
                         freight_df['20\' DV'][via_index].split()[-1]])
                    freight_df.loc[index, '40\'DV/HC'] = ' '.join(
                        [str(int(freight_df['40\'DV/HC'][via_index].split()[0]) + int(add_on)),
                         freight_df['40\'DV/HC'][via_index].split()[-1]])

                    freight_df.loc[index, '40\'RE'] = ' '.join(
                        [str(int(freight_df['40\'RE'][via_index].split()[0]) + int(add_on)),
                         freight_df['40\'RE'][via_index].split()[-1]])

            freight_df = freight_df.loc[freight_df['20\' DV'] != '']
            for column in freight_df.columns:
                if 'DV/HC' in column:
                    holder = column.split('\'')[-1].split('/')
                    freight_df[column.split('\'')[0].strip() + '\'' + holder[0].strip()] = freight_df[column]
                    freight_df[column.split('\'')[0].strip() + '\'' + holder[-1].strip()] = freight_df[column]
                    freight_df.drop(columns=[column, ''], inplace=True)
                    break
            freight_df = All_Trades_Export_MSC.Bls.apply_pols(self, freight_df, pols)

            return freight_df, remark_add_on


        def clean_surcharges_with_cols_load_types(self, surcharges_df, columns):
            surcharges_df.columns = columns
            surcharges_df["charges"].fillna(method='ffill', inplace=True)
            surcharges_df.dropna(subset=["charges", "20GP"], inplace=True)
            surcharges_df["40HC"] = surcharges_df["40GP"]
            surcharges_df = surcharges_df.drop(
                columns=[column for column in surcharges_df.columns if column.startswith('drop')])
            surcharges_df = All_Trades_Export_MSC.India_fix.melt_load_type(self, surcharges_df)
            surcharges_df["currency"], surcharges_df["amount"], surcharges_df["charges_leg"] = \
            surcharges_df["amount"].str.split(" ").str[1], surcharges_df["amount"].str.split(" ").str[0], "L3"
            surcharges_df.loc[surcharges_df["charges"].str.contains("Puerto Cabello", na =False, case=False), ("destination_port","charges_leg")] = "Puerto Cabello",  "L4"
            surcharges_df.loc[surcharges_df["charges"].str.contains("La Guaira",  na=False, case=False), ("destination_port","charges_leg")] = "La Guaira", "L4"
            return surcharges_df

        def capture(self):

            dates, remarks = All_Trades_Export_MSC.Bls.get_headers(self)
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)

            df, yellow_df = All_Trades_Export_MSC.Bls.get_main_df(self)
            freight_df, remark_add_on = self.get_freight_df(df)

            if remark_add_on is not None:
                freight_df['REMARKS'] = remarks
                freight_df.loc[freight_df['POD'].str.contains('\\*'), 'REMARKS'] \
                    = freight_df.loc[freight_df['POD'].str.contains('\\*'), 'REMARKS'] + '\n' + remark_add_on
                freight_df['REMARKS'] = freight_df['REMARKS'] + '\n' + freight_df['Comments']
            else:
                freight_df['REMARKS'] = remarks + '\n' + freight_df['Remark'] + '\n' + freight_df['Comments']
                freight_df.drop(columns=['Remark'], axis=1, inplace=True)
            freight_df['REMARKS'] = freight_df['REMARKS'].apply(lambda x: x.strip('\n'))
            if 'Comments' in freight_df.columns:
                freight_df.drop(columns=['Comments'], axis=1, inplace=True)
            if 'DEM/DET free time at POD' in freight_df.columns:
                freight_df.drop(columns=['DEM/DET free time at POD'], axis=1, inplace=True)

            freight_df['POD'] = freight_df['POD'].apply(lambda x: x.strip(' *'))
            freight_df['start_date'], freight_df['expiry_date'] = validity['start_date'], validity['end_date']

            freight_df.replace('', nan, inplace=True)
            freight_df.dropna(how='all', axis=1, inplace=True)
            freight_df.rename(columns={'POD': 'destination_port', 'POL': 'origin_port', "REMARKS": "contract_id",
                                       "ROUTING POINTS": "via"}, inplace=True)

            # Surcharge capture

            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN",
                                                                                     "^SURCHARGES - VALID AT TIME")
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self,
                                                                                                        surcharges_local_origin_df)
            if len(surcharges_local_origin_df.columns) == 4:
                columns = ["charges", "origin_port", "20GP", "40GP"]
                local_charges_result_df = All_Trades_Export_MSC.Caribic.clean_surcharges_local_origin_with_cols_load_types(self, surcharges_local_origin_df, columns)

            freight_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT TIME", "^ADMINISTRATIVE SURCHARGES - VALID")
            freight_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self,freight_surcharges_df)
            if len(freight_surcharges_df.columns) == 5:
                columns = ["charges", "port_names", "20GP", "40GP", "remarks"]
                freight_surcharges_result_df = self.clean_surcharges_with_cols_load_types(freight_surcharges_df, columns)

            surcharges_administrative_df = All_Trades_Export_MSC.India_fix.surcharges_(self,
                                                                                       "^ADMINISTRATIVE SURCHARGES - VALID",
                                                                                       "^SPECIAL REQUIREMENTS FOR SHIPMENTS TO CENTRAL AMERICA")
            surcharges_administrative_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self,surcharges_administrative_df)
            if len(surcharges_administrative_df.columns) == 5:
                columns = ["charges", "20GP", "40GP", "remarks", "drop1"]
                surcharges_administrative_result_df = self.clean_surcharges_with_cols_load_types(
                    surcharges_administrative_df, columns)

            surcharges_df = pd.concat(
                [local_charges_result_df, freight_surcharges_result_df, surcharges_administrative_result_df],
                ignore_index=True)

            surcharges_df["origin_port"] = surcharges_df["origin_port"].str.split(";")
            surcharges_df = surcharges_df.explode("origin_port")
            surcharges_df["amount"] = surcharges_df["amount"].apply(lambda x: All_Trades_Export_MSC.Indian_ocean_fix.convert_currency_format(self, x))
            surcharges_df["start_date"], surcharges_df["expiry_date"] = validity["start_date"], validity["end_date"]
            self.captured_output = {'Freight': freight_df, "Surcharges": surcharges_df}

    class Eaf(Bls):

        def common_freight(self, holder, element):

            holder.iloc[0, 0] = 'POD'
            holder.columns = holder.iloc[0, :]
            holder = holder.iloc[1:, :]
            holder['POL'] = element.strip()
            for column in holder.columns:
                if 'DV/HC' in column:
                    temp = column.split('\'')[-1].split('/')
                    holder[column.split('\'')[0].strip() + '\'' + temp[0].strip()] = holder[
                        column]
                    holder[column.split('\'')[0].strip() + '\'' + temp[-1].strip()] = holder[
                        column]
                    holder.drop(columns=[column, ''], inplace=True)
                    break
            return holder

        def get_freight_df(self, df):

            charges_index, pol_index, pod_index = self.get_indexes(df)
            temp = None
            if pol_index[0] + 1 == pol_index[1]:
                temp = [pol_index[i] for i in range(len(pol_index)) if i % 2 == 0]
            dfs = []
            if temp is None:
                for i in range(len(pol_index)):
                    if i + 2 <= len(pol_index):
                        pols = df.iloc[pol_index[i], 0].split('POL')[-1].strip().split('/')
                        for element in pols:
                            holder = df.iloc[pol_index[i]:pol_index[i + 1], :]
                            dfs.append(self.common_freight(holder, element))
                    else:
                        pols = df.iloc[pol_index[i], 0].split('POL')[-1].strip().split('/')
                        for element in pols:
                            holder = df.iloc[pol_index[i]:charges_index[0], :]
                            dfs.append(self.common_freight(holder, element))
                        break
            else:
                for i in range(len(temp)):
                    if i + 2 <= len(temp):
                        pols = df.iloc[temp[i], 0].split('POL')[-1].strip().split(',')
                        for element in pols:
                            holder = df.iloc[temp[i]:temp[i + 1], :]
                            holder = holder.iloc[1:, :]
                            dfs.append(self.common_freight(holder, element))
                    else:
                        pols = df.iloc[temp[i], 0].split('POL')[-1].strip().split(',')
                        for element in pols:
                            holder = df.iloc[temp[i]:charges_index[0], :]
                            holder = holder.iloc[1:, :]
                            dfs.append(self.common_freight(holder, element))
                        break

            return pd.concat(dfs, ignore_index=True)

        def capture(self):

            dates, remarks = All_Trades_Export_MSC.Bls.get_headers(self)
            df, yellow_df = All_Trades_Export_MSC.Bls.get_main_df(self)
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)
            freight_df = self.get_freight_df(df)
            freight_df = All_Trades_Export_MSC.Bls.apply_pols(self, freight_df, None)
            if 'Remarks' in freight_df.columns:
                # freight_df['REMARKS'] = remarks + '\n' + freight_df['Remarks']
                # freight_df['REMARKS'] = freight_df['REMARKS'].apply(lambda x: x.strip('\n'))
                freight_df['REMARKS'] = remarks
                freight_df.drop(columns=['Remarks'], axis=1, inplace=True)
            freight_df.rename(columns={'POD': 'destination_port', 'POL': 'origin_port', "REMARKS":"remarks"}, inplace=True)
            freight_df['start_date'], freight_df['expiry_date'] = validity['start_date'], validity['end_date']

            freight_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT TIME", "^LOCAL CHARGES - VALID AT")
            freight_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_df)
            if len(freight_surcharges_df.columns) == 5:
                columns = ["charges", "portname", "amount", "load_type", "remarks"]
                freight_surcharges_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, freight_surcharges_df, columns)

            inclusion = freight_surcharges_result_df.loc[freight_surcharges_result_df["amount"].str.contains("incl", na=False, case=False)]
            if not inclusion.empty:
                incl = inclusion["charges"].to_string(index=False, header=False).strip().replace("\n",";")
                freight_df["inclusions"] = incl

            freight_surcharges_result_df = freight_surcharges_result_df.loc[~freight_surcharges_result_df["amount"].str.contains("incl", na=False, case=False)]

            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES - VALID AT", "^SPECIAL SURCHARGES")
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_origin_df)
            if len(surcharges_local_origin_df.columns) == 4:
                columns = ["charges", "origin_port", "amount", "load_type"]
                local_charges = All_Trades_Export_MSC.India_fix.clean_surcharges_local_origin_with_cols(self, surcharges_local_origin_df, columns)

            special_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SPECIAL SURCHARGES", None)
            special_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, special_surcharges_df)
            if len(special_surcharges_df.columns) == 4:
                columns = ["charges", "amount", "load_type", "remarks"]
                special_surcharges_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, special_surcharges_df, columns)

            surcharges_df = pd.concat([freight_surcharges_result_df, local_charges, special_surcharges_result_df], ignore_index=True)
            surcharges_df['start_date'], surcharges_df['expiry_date'] = validity['start_date'], validity['end_date']

            self.captured_output = {'Freight': freight_df, "Surcharges": surcharges_df}

    class Eam(Eaf):


        def clean_surcharge_with_cols(self, surcharges_df, columns):

            surcharges_df.columns = columns
            surcharges_df = surcharges_df.drop(columns=[column for column in surcharges_df.columns if column.startswith('drop')])
            surcharges_df["charges"].fillna(method='ffill', inplace =True)
            surcharges_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_result_with_basis = surcharges_df.loc[surcharges_df["load_type"].str.contains("per", case=False,  na=False)]
            surcharges_result_with_basis.loc[surcharges_result_with_basis["portname"].notna(), "load_type"] = surcharges_result_with_basis["portname"]
            surcharges_result_with_load_types = surcharges_df.loc[~surcharges_df["load_type"].str.contains("per", case=False,  na=False)]
            surcharges_result_with_load_types.rename(columns={"amount":"20GP", "load_type":"40GP"}, inplace=True)
            surcharges_result_with_load_types["40HC"] = surcharges_result_with_load_types["40GP"]
            surcharges_result_with_load_types_result = All_Trades_Export_MSC.India_fix.melt_load_type(self, surcharges_result_with_load_types)

            surcharges_df = pd.concat([surcharges_result_with_basis, surcharges_result_with_load_types_result], ignore_index=True)

            surcharges_df_with_rates = surcharges_df.loc[surcharges_df["amount"].str.contains("^\d+",  na =False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"]= surcharges_df_with_rates["amount"].str.split(" ").str[1], surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = surcharges_df.loc[~surcharges_df["amount"].str.contains("^\d", na =False)]
            surcharges_df_without_rates["remarks"], surcharges_df_without_rates["amount"] = surcharges_df_without_rates["amount"], ""
            surcharges_result = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index= True)
            self.replace_currency = {"USD" : "", "EUR" : "", "per 40' Ctr": "40GP=;40HC", "per 40´" : "40GP=;40HC", "40'" : "40GP=;40HC", "per 40'DC/HC" :"40GP=;40HC"}
            surcharges_result["load_type"].replace(self.replace_currency, regex =True , inplace =True)
            surcharges_result.loc[surcharges_result["charges"].str.contains("if requested" , case= False,   na =False), "remarks"] = "if requested"
            #surcharges_result.loc[surcharges_result["remarks"].str.contains("request" , case= False,  na =False), "amount"] = "ON REQUEST"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("incl" , case= False,  na=False), "amount"] = "incl"
            surcharges_result.loc[surcharges_result["charges"].str.contains("Antwerp", na =False, case =False), "origin_port"] = "Antwerp"
            surcharges_result.loc[surcharges_result["charges"].str.contains("IMO", na=False, case =False), "cargo_type"] = "ONLY"
            surcharges_result.loc[surcharges_result["charges"].str.contains("Israel", na=False, case=False), "destination_country"] = "Israel"
            surcharges_result.loc[surcharges_result["charges"].str.contains("Beirut", na=False, case=False), "destination_port"] = "Beirut"
            surcharges_result.loc[surcharges_result["charges"].str.contains("Cyprus", na=False, case=False), "destination_country"] = "Cyprus"
            surcharges_result.loc[surcharges_result["charges"].str.contains("Lattakia, Tartous", na=False, case=False), "destination_country"] = "Syria"

            surcharges_result["charges_leg"] = "L3"

            surcharges_result.loc[surcharges_result["charges"].str.contains("origin" , case= False,   na =False), "charges_leg"] = "L2"
            surcharges_result.loc[surcharges_result["charges"].str.contains("destination" , case= False,   na =False), "charges_leg"] = "L4"
            surcharges_result["remarks"].replace(self.replace_currency, regex=True, inplace =True)
            return surcharges_result

        def capture(self):

            dates, remarks = All_Trades_Export_MSC.Bls.get_headers(self)
            df, yellow_df = All_Trades_Export_MSC.Bls.get_main_df(self)
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)
            freight_df = self.get_freight_df(df)
            freight_df = All_Trades_Export_MSC.Bls.apply_pols(self, freight_df, None)
            if 'Remarks' in freight_df.columns:
                freight_df['REMARKS'] = remarks + '\n' + freight_df['Remarks']
                freight_df['REMARKS'] = freight_df['REMARKS'].apply(lambda x: x.strip('\n'))
                freight_df.drop(columns=['Remarks'], axis=1, inplace=True)
            freight_df.rename(columns={'POD': 'destination_port', 'POL': 'origin_port', "REMARKS" : "contract_id"}, inplace=True)
            freight_df['start_date'], freight_df['expiry_date'] = validity['start_date'], validity['end_date']

            freight_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT", "^LOCAL CHARGES AT ORIGIN")
            freight_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_df)
            if len(freight_surcharges_df.columns) == 5:
                columns = ["charges", "portname", "amount", "load_type", "remarks"]
                freight_surcharges_result_df = self.clean_surcharge_with_cols(freight_surcharges_df, columns)

            inclusion = freight_surcharges_result_df.loc[freight_surcharges_result_df["amount"].str.contains("incl", na=False, case=False)]
            if not inclusion.empty:
                incl = inclusion["charges"].to_string(index=False, header=False).strip().replace("\n",";")
                freight_df["inclusions"] = incl

            freight_surcharges_result_df = freight_surcharges_result_df.loc[~freight_surcharges_result_df["amount"].str.contains("incl", na=False, case=False)]

            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN", "^ADMIN CHARGES - VALID")
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_origin_df)
            if len(surcharges_local_origin_df.columns) == 5:
                columns = ["charges", "origin_port", "amount", "load_type", "remarks"]
                local_charges = All_Trades_Export_MSC.India_fix.clean_surcharges_local_origin_with_cols(self, surcharges_local_origin_df, columns)

            admin_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^ADMIN CHARGES - VALID", None)
            admin_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, admin_surcharges_df)
            if len(admin_surcharges_df.columns) == 4:
                columns = ["charges", "amount", "load_type", "remarks"]
                admin_surcharges_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, admin_surcharges_df, columns)
                admin_surcharges_result_df.loc[(admin_surcharges_result_df["charges"].str.contains("B/L", na=False, case=False)) & (admin_surcharges_result_df["load_type"].isna()), "load_type"] = "per B/L"


            surcharges_df = pd.concat([freight_surcharges_result_df, local_charges, admin_surcharges_result_df],ignore_index= True)
            surcharges_df['start_date'], surcharges_df['expiry_date'] = validity['start_date'], validity['end_date']

            self.captured_output = {'Freight': freight_df, "Surcharges": surcharges_df}

    class Adria(Bls):

        def get_freight_df(self, df):

            charges_index, pol_index, pod_index = self.get_indexes(df)
            df = df.iloc[pol_index[0]:charges_index[0], :]
            pols = df.iloc[0, 0].split('POL')[-1].strip().split(',')
            df.iloc[0, 0] = 'POD'
            df.iloc[0, 1:] = df.iloc[1, 1:]
            df.columns = df.iloc[0, :]
            df = df.loc[~df['20\' DV'].str.contains('20\' DV', na=False)]
            if 'Routing // Transit Time' in df.columns:
                col = "Routing // Transit Time"
            elif 'Triest Routing' in df.columns:
                col = 'Triest Routing'
            df['via'] = df[col].str.split('//', expand=True)[0].str.replace('direct', '').str.replace('Tranship.', '').str.replace('via', '').apply(lambda x: x.strip())
            df['transit_time'] = df[col].str.split('//', expand=True)[1].str.split(expand=True)[0].apply(lambda x: x.strip())
            df['TIME QUALIFIER (TRANSITTIME)'] = df[col].str.split('//', expand=True)[1].str.split(expand=True)[1].apply(lambda x: x.strip())
            df.drop([col], axis=1, inplace=True)
            df = All_Trades_Export_MSC.Bls.apply_pols(self, df, pols)
            return df

        def capture(self):

            dates, remarks = All_Trades_Export_MSC.Bls.get_headers(self)
            df, yellow_df = All_Trades_Export_MSC.Bls.get_main_df(self)
            freight_df = self.get_freight_df(df)
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)
            freight_df['start_date'], freight_df['expiry_date'] = validity['start_date'], validity['end_date']

            if 'Remarks' in freight_df.columns:
                freight_df['REMARKS'] = remarks + '\n' + freight_df['Remarks']
                freight_df['REMARKS'] = freight_df['REMARKS'].apply(lambda x: x.strip('\n'))
                freight_df.drop(columns=['Remarks'], axis=1, inplace=True)
            freight_df.rename(columns={'POD': 'destination_port', 'POL': 'origin_port', "REMARKS" : "contract_id"}, inplace=True)


            freight_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT TIME", "^Cyprus, Malta")
            freight_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_df)
            if len(freight_surcharges_df.columns) == 4:
                columns = ["charges", "20GP", "40GP", "remarks"]
                freight_surcharges_egypt_lebanon_syria_df = All_Trades_Export_MSC.Caribic.clean_surcharges_with_cols_load_types(self, freight_surcharges_df, columns)
                # freight_surcharges_egypt_lebanon_syria_result_df.loc[freight_surcharges_egypt_lebanon_syria_result_df["charges"].str.contains("Beirut", na=False, case=False), "destination_port"] = "Beirut"
                # freight_surcharges_egypt_lebanon_syria_result_df.loc[freight_surcharges_egypt_lebanon_syria_result_df["charges"].str.contains("Syria", na=False, case=False), "destination_country"] = "Syria"

                freight_surcharges_egypt_lebanon_syria_df = freight_surcharges_egypt_lebanon_syria_df.loc[~freight_surcharges_egypt_lebanon_syria_df["charges"].str.contains("Egypt, Lebanon, Syria", case=False, na=False)]

                dps = []
                for country in ["Egypt", "Beirut", "Lattakia"]:
                    if country == "Egypt":
                        freight_surcharges_egypt_lebanon_syria_df_copy = freight_surcharges_egypt_lebanon_syria_df.loc[~freight_surcharges_egypt_lebanon_syria_df["charges"].str.contains("War Risk", case=False, na=False)].copy()
                        freight_surcharges_egypt_lebanon_syria_df_copy["destination_country"] = country
                        dps.append(freight_surcharges_egypt_lebanon_syria_df_copy)

                    else:
                        freight_surcharges_egypt_lebanon_syria_df_copy = freight_surcharges_egypt_lebanon_syria_df.loc[~freight_surcharges_egypt_lebanon_syria_df["charges"].str.contains("War Risk", case=False, na=False)].copy()
                        freight_surcharges_egypt_lebanon_syria_df_copy["destination_port"] = country
                        dps.append(freight_surcharges_egypt_lebanon_syria_df_copy)

                freight_surcharges_egypt_lebanon_syria_result_df = pd.concat(dps, ignore_index=True)
                freight_surcharges_egypt_lebanon_syria_df_war_risk = freight_surcharges_egypt_lebanon_syria_df.loc[freight_surcharges_egypt_lebanon_syria_df["charges"].str.contains("War Risk", case=False,na=False)].copy()
                freight_surcharges_egypt_lebanon_syria_df_war_risk.loc[freight_surcharges_egypt_lebanon_syria_df_war_risk["charges"].str.contains("Beirut", case=False, na=False), "destination_port"] = "Beirut"
                freight_surcharges_egypt_lebanon_syria_df_war_risk.loc[freight_surcharges_egypt_lebanon_syria_df_war_risk["charges"].str.contains("Syria", case=False, na=False), "destination_country"] = "Syria"

                freight_surcharges_egypt_lebanon_syria_result_df = pd.concat([freight_surcharges_egypt_lebanon_syria_result_df, freight_surcharges_egypt_lebanon_syria_df_war_risk], ignore_index=True)
                freight_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^Cyprus, Malta", "^ISRAEL$")
                freight_surcharges_cyprus_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self,freight_surcharges_df)
                if len(freight_surcharges_cyprus_df.columns) == 4:
                    columns = ["charges", "20GP", "40GP", "remarks"]
                    freight_surcharges_cyprus_result_df = All_Trades_Export_MSC.Caribic.clean_surcharges_with_cols_load_types(self, freight_surcharges_cyprus_df, columns)
                    freight_surcharges_cyprus_result_df = freight_surcharges_cyprus_result_df.loc[ ~freight_surcharges_cyprus_result_df["charges"].str.contains("Cyprus, Malta",case=False, na=False)]

                    dps = []
                    for port_name in ["Limassol", "Marsaxlokk"]:
                            freight_surcharges_cyprus_result_df_copy = freight_surcharges_cyprus_result_df.copy()
                            freight_surcharges_cyprus_result_df_copy["destination_port"] = port_name
                            dps.append(freight_surcharges_cyprus_result_df_copy)

                freight_surcharges_cyprus_result_df_resl = pd.concat(dps, ignore_index=True)



                freight_surcharges_israels_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^ISRAEL$", "^LOCAL CHARGES AT ORIGIN - VALID ")
                freight_surcharges_israels_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_israels_df)
                if len(freight_surcharges_israels_df.columns) == 4:
                    columns = ["charges", "20GP", "40GP", "remarks"]
                    freight_surcharges_israels_result_df = All_Trades_Export_MSC.Caribic.clean_surcharges_with_cols_load_types(self, freight_surcharges_israels_df, columns)
                    freight_surcharges_israels_result_df.loc[freight_surcharges_israels_result_df["charges"].str.contains("Israel", na=False, case=False), "destination_country"] = "Israel"

                    freight_surcharges_israels_result_df = freight_surcharges_israels_result_df.loc[~freight_surcharges_israels_result_df["charges"].str.contains("israels", case=False, na=False)]

                    dps = []
                    for port_name in ["Israels"]:
                        freight_surcharges_israels_result_df_copy = freight_surcharges_israels_result_df.copy()
                        freight_surcharges_israels_result_df_copy["destination_country"] = port_name
                        dps.append(freight_surcharges_israels_result_df_copy)

                freight_surcharges_israels_end_result = pd.concat(dps, ignore_index=True)

                surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN",
                                                                                         "^ADMINISTRATION SURCHARGES")
                surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_origin_df)
                if len(surcharges_local_origin_df.columns) == 5:
                    columns = ["charges", "origin_port", "amount", "load_type", "remarks"]
                    local_charges = All_Trades_Export_MSC.India_fix.clean_surcharges_local_origin_with_cols(self, surcharges_local_origin_df, columns)
                    local_charges["origin_port"] = local_charges["origin_port"].replace(",", ";", regex=True)

                surcharges_administrative_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^ADMINISTRATION SURCHARGES", "^REMARKS")
                surcharges_administrative_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_administrative_df)
                if len(surcharges_administrative_df.columns) == 4:
                    columns = ["charges", "amount", "load_type", "remarks"]
                    surcharges_administrative_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, surcharges_administrative_df, columns)

                surcharges_df = pd.concat([freight_surcharges_egypt_lebanon_syria_result_df, freight_surcharges_cyprus_result_df_resl,
                                           freight_surcharges_israels_end_result, local_charges, surcharges_administrative_result_df], ignore_index=True)

                surcharges_df["origin_port"] = surcharges_df["origin_port"].str.split(";")
                surcharges_df = surcharges_df.explode("origin_port")

                surcharges_df['start_date'], surcharges_df['expiry_date'] = validity['start_date'], validity['end_date']

            self.captured_output = {'Freight': freight_df, 'Surcharges': surcharges_df}

    class FarEast(Bls):

        def clean_surcharge_with_cols(self, surcharges_df, columns):

            surcharges_df.columns = columns
            surcharges_df["charges"].fillna(method='ffill', inplace=True)
            surcharges_df = surcharges_df.drop(columns=[column for column in surcharges_df.columns if column.startswith('drop')])
            surcharges_df.dropna(subset=["charges", "amount"], inplace = True)
            surcharges_df_with_rates = surcharges_df.loc[surcharges_df["amount"].str.contains("^\d+",  na =False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"]= surcharges_df_with_rates["amount"].str.split(" ").str[1], surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = surcharges_df.loc[~surcharges_df["amount"].str.contains("^\d", na =False)]
            surcharges_df_without_rates["remarks"], surcharges_df_without_rates["amount"] = surcharges_df_without_rates["amount"], ""
            surcharges_result = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index= True)
            self.replace_currency = {"USD" : "", "EUR" : "", "per 40' Ctr": "40GP=;40HC", "per 40´" : "40GP=;40HC", "40'" : "40GP=;40HC", "per 40'DC/HC" :"40GP=;40HC"}
            surcharges_result["load_type"].replace(self.replace_currency, regex =True , inplace=True)
            #surcharges_result.loc[surcharges_result["charges"].str.contains("if requested" , case= False,   na =False), "remarks"] = "if requested"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("request" , case= False,  na =False), "amount"] = "ON REQUEST"
            surcharges_result.loc[surcharges_result["remarks"].str.contains("incl" , case= False,  na =False), "amount"] = "incl"
            surcharges_result.loc[surcharges_result["charges"].str.contains("Antwerp"), "origin_port"] = "Antwerp"
            #surcharges_result.loc[surcharges_result["charges"].str.contains("IMO"), "cargo_type"] = "ONLY"
            surcharges_result["charges_leg"] = "L3"

            # if surcharges_result["remarks"].str.contains("class", case=False,  na=False).any():
            #     surcharges_result.loc[surcharges_result["remarks"].str.contains("class" , case= False,  na =False), "commodity"] = surcharges_result["remarks"].str.split("/").str[0]
            #     surcharges_result.loc[surcharges_result["remarks"].str.contains("class" , case= False,  na =False), "commodity"] = surcharges_result["commodity"].str.split("class").str[1]
            #     # surcharges_result["commodity"] = surcharges_result["commodity"].str.split(";")
            #     # surcharges_result = surcharges_result.explode("commodity")

            surcharges_result.loc[surcharges_result["charges"].str.contains("origin" , case= False,   na =False), "charges_leg"] = "L2"
            surcharges_result.loc[surcharges_result["charges"].str.contains("destination" , case= False,   na =False), "charges_leg"] = "L4"
            surcharges_result.loc[surcharges_result["currency"].str.contains("%", case=False,  na=False), "load_type"] = "BASIS"

            surcharges_result["remarks"].replace(self.replace_currency, regex=True, inplace =True)
            return surcharges_result

        def get_freight_df(self, df):

            charges_index, pol_index, pod_index = self.get_indexes(df)

            if pol_index is None or pol_index[-1] > 20:
                if df[0].str.contains('Port of Loading', na=False).any():
                    pol_index = list(df[df[0].str.contains('Port of Loading', na=False)].index)[0]
                    pols = df.iloc[pol_index, 1].replace(' ', '').split(',')
            if df[1].str.contains('20\'DC', na=False).any():
                start_index = list(df[df[1].str.contains('20\'DC', na=False)].index)[0]
            elif df[3].str.contains('20\'DV', na=False).any():
                start_index = list(df[df[3].str.contains('20\'DV', na=False)].index)[0]

            df = df.iloc[start_index - 1:charges_index[0], :]
            for i in range(len(list(df.columns))):
                if df.iloc[1, i] == '':
                    if df.iloc[0, i] != '':
                        df.iloc[1, i] = df.iloc[0, i]

            df.iloc[1, 0] = 'POD'
            df.columns = df.iloc[1, :]
            df = df.iloc[2:, :]
            if '20\'DC' not in df.columns:
                df.rename(columns={'20\'DV': '20\'DC'}, inplace=True)
            df = df.loc[df['20\'DC'] != '']
            df.loc[df.iloc[:, 3].str.contains("non HAZ", na=False, case=False), "cargo_type"] = "NO"
            df.loc[df.iloc[:, 5].str.contains("non HAZ", na=False, case=False), "cargo_type"] = "NO"

            df.drop([''], axis=1, inplace=True)
            for column in df.columns:
                if '!' in column:
                    df.drop([column], axis=1, inplace=True)
                if 'DC/HC' in column or 'DV/HC' in column:
                    types = column.split("'")[0]
                    df[types + "'" + column.split("'")[-1].split('/')[0]] = df[column].str.split(expand=True)[0]
                    df[types + "'" + column.split("'")[-1].split('/')[-1]] = df[column].str.split(expand=True)[0]
                    df.drop([column], axis=1, inplace=True)
            df.loc[df["POD"].str.contains("via", na=False, case=False), "via"] = df["POD"].str.split("via").str[1]
            return All_Trades_Export_MSC.Bls.apply_pols(self, df, pols)

        def capture(self):

            dates, remarks = All_Trades_Export_MSC.Bls.get_headers(self)
            df, yellow_df = All_Trades_Export_MSC.Bls.get_main_df(self)
            freight_df = self.get_freight_df(df)
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)

            freight_df['start_date'], freight_df['expiry_date'] = validity['start_date'], validity['end_date']
            """adding contract Id"""
            if 'Remarks' in freight_df.columns:
                freight_df['contract_id'] = remarks
                freight_df['REMARKS'] = freight_df['Remarks']
                freight_df['REMARKS'] = freight_df['REMARKS'].apply(lambda x: x.strip('\n'))
                freight_df.drop(columns=['Remarks'], axis=1, inplace=True)
            else:
                freight_df['contract_id'] = remarks
            freight_df.rename(columns={'POD': 'destination_port', 'POL': 'origin_port', "REMARKS" : "remarks"}, inplace=True)

            freight_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT", "^LOCAL CHARGES AT ORIGIN")
            freight_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_df)
            if len(freight_surcharges_df.columns) == 4:
                columns = ["charges",  "amount", "load_type", "remarks"]
                freight_surcharges_result_df = self.clean_surcharge_with_cols(freight_surcharges_df, columns)
                freight_surcharges_result_df.loc[freight_surcharges_result_df["remarks"].str.contains("applicable for class", na=False, case=False), "remarks"] = ""


            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN", "^LOCAL CHARGES AT DESTINATION")
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_origin_df)

            if len(surcharges_local_origin_df.columns) == 6:
                columns = ["charges", "origin_port", "amount", "load_type", "drop1", "drop2"]
                local_origins_df = All_Trades_Export_MSC.India_fix.clean_surcharges_local_origin_with_cols(self, surcharges_local_origin_df, columns)

            elif len(surcharges_local_origin_df.columns) == 5:
                columns = ["charges", "origin_port", "amount", "load_type", "remarks"]
                local_origins_df = All_Trades_Export_MSC.India_fix.clean_surcharges_local_origin_with_cols(self, surcharges_local_origin_df, columns)


            surcharges_local_destination_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT DESTINATION - VALID AT TIME", "ADMINISTRATION SURCHARGES")
            surcharges_local_destination_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_destination_df)
            if len(surcharges_local_destination_df.columns) == 6:
                columns = ["charges", "remarks", "destination_port", "amount", "load_type", "drop1"]
                surcharges_local_destination_result_df = All_Trades_Export_MSC.India_fix.clean_surcharges_local_destination_with_cols(self, surcharges_local_destination_df, columns)


            surcharges_administrative_df = All_Trades_Export_MSC.India_fix.surcharges_(self,"^ADMINISTRATION SURCHARGES",  None)
            surcharges_administrative_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_administrative_df)
            if len(surcharges_administrative_df.columns) == 5:
                columns = ["charges", "amount", "load_type", "remarks","drop1"]
                surcharges_administrative_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, surcharges_administrative_df, columns)

            elif len(surcharges_administrative_df.columns) == 6:
                columns = ["charges", "drop1", "drop2", "amount", "load_type", "remarks"]
                surcharges_administrative_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, surcharges_administrative_df, columns)

            surcharges_df = pd.concat([freight_surcharges_result_df, local_origins_df, surcharges_local_destination_result_df, surcharges_administrative_result_df], ignore_index=True)
            surcharges_df['start_date'], surcharges_df['expiry_date'] = validity['start_date'], validity['end_date']

            self.captured_output = {'Freight': freight_df, "Surcharges":surcharges_df}

    class Grt(Bls):

        def common_freight(self, holder, dfs):

            pols = holder.iloc[1, 0].replace(' ', '').split(',')
            holder.iloc[1, 0] = 'POD'
            holder.columns = holder.iloc[1, :]
            holder = holder.iloc[2:, :]
            for element in pols:
                temp = holder.copy(deep=True)
                temp['POL'] = element
                # temp = temp.loc[temp['20\' DV'] != '']
                dfs.append(temp)
            return dfs

        def get_freight_df(self, df):

            charges_index, pol_index, pod_index = self.get_indexes(df)
            df = df.iloc[pol_index[0] - 1:charges_index[-1], :]
            dfs = []
            seafreight = list(df[df[0].str.contains('SEAFREIGHT', na=False)].index)
            for i in range(len(seafreight)):
                if i + 2 < len(seafreight) + 1:
                    if df.loc[seafreight[i] - 1, 2] == '':
                        region = df.loc[seafreight[i] - 1, 0]
                        holder = df.loc[seafreight[i]: seafreight[i + 1] - 1, :]
                    else:
                        holder = df.loc[seafreight[i]: seafreight[i + 1] - 2, :]
                    holder['region'] = region
                    holder.iloc[1, -1] = 'region'
                    dfs = self.common_freight(holder, dfs)
                else:
                    if df.loc[seafreight[i] - 1, 2] == '':
                        region = df.loc[seafreight[i] - 1, 0]
                        holder = df.loc[seafreight[i]:, :]
                    else:
                        holder = df.loc[seafreight[i]:, :]
                    holder['region'] = region
                    holder.iloc[1, -1] = 'region'
                    dfs = self.common_freight(holder, dfs)
                    break

            return pd.concat(dfs, ignore_index=True)

        def capture(self):

            dates, remarks = All_Trades_Export_MSC.Bls.get_headers(self)
            df, yellow_df = All_Trades_Export_MSC.Bls.get_main_df(self)
            freight_df = self.get_freight_df(df)
            freight_df.drop([''], axis=1, inplace=True)
            freight_df = All_Trades_Export_MSC.Bls.apply_pols(self, freight_df, None)
            freight_df.rename(columns={'POD': 'destination_port', 'Remarks': 'remarks', 'POL': 'origin_port'}, inplace=True)
            for code in remarks:
                _code = (remarks[code])
                freight_df.replace(code.upper(), _code, inplace=True, regex=True)
            freight_df['remarks'] = freight_df['region']
            freight_df.drop(columns=['region'], axis=1, inplace=True)
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)
            freight_df['start_date'], freight_df['expiry_date'] = validity['start_date'], validity['end_date']

            freight_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT TIME OF", None)
            freight_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_df)
            if len(freight_surcharges_df.columns) == 5:
                columns = ["charges", "origin_port", "amount", "load_type", "remarks"]
                freight_surcharges_result_df = All_Trades_Export_MSC.FarEast.clean_surcharge_with_cols(self, freight_surcharges_df, columns)
                freight_surcharges_result_df.loc[freight_surcharges_result_df["load_type"].str.contains("Piraeus, Volos, Heraklion", case=False, na=False), "destination_port"] = "Piraeus;Volos;Heraklion "
                freight_surcharges_result_df["origin_port"] = freight_surcharges_result_df["origin_port"].replace("/",";",regex =True)
                freight_surcharges_result_df.loc[freight_surcharges_result_df["charges"].str.contains("Antwerp"), "origin_port"] = "Antwerp"
                freight_surcharges_result_df.loc[freight_surcharges_result_df["origin_port"].str.contains("2022", na=False, case=False), "origin_port"] = nan
                freight_surcharges_result_df.loc[freight_surcharges_result_df["origin_port"].str.contains("NWC", na=False, case=False), "origin_port"] = nan

                if freight_surcharges_result_df["load_type"].str.contains("Turkey, Thessaloniki", case=False, na=False).any():
                    freight_surcharges_turkey_thessaloniki = freight_surcharges_result_df.loc[freight_surcharges_result_df["load_type"].str.contains("Turkey, Thessaloniki", case=False, na=False)]
                    dps = []
                    for portname in ["Turkey", "Thessaloniki"]:
                        freight_surcharges_turkey_thessaloniki_copy = freight_surcharges_turkey_thessaloniki.copy()
                        if portname == "Turkey":
                            freight_surcharges_turkey_thessaloniki_copy["destination_country"] = portname
                            dps.append(freight_surcharges_turkey_thessaloniki_copy)
                        else:
                            freight_surcharges_turkey_thessaloniki_copy["destination_port"] = portname
                            dps.append(freight_surcharges_turkey_thessaloniki_copy)
                    freight_surcharges_non_turkey_thessaloniki = freight_surcharges_result_df.loc[~freight_surcharges_result_df["load_type"].str.contains("Turkey, Thessaloniki", case=False, na=False)]
                    dps.append(freight_surcharges_non_turkey_thessaloniki)
                    freight_surcharges_result_df = pd.concat(dps, ignore_index=True)

            freight_surcharges_result_df['start_date'], freight_surcharges_result_df['expiry_date'] = validity['start_date'], validity['end_date']
            freight_surcharges_result_df.loc[freight_surcharges_result_df["charges"].str.contains("if requested", case=False, na=False), "remarks"] = "if requested"
            self.captured_output = {'Freight': freight_df, "Surcharges" : freight_surcharges_result_df}

    class Grt_adria(Bls):

        def clean_surcharges_local_origin_with_columns(self, surcharges_local_origin_df, columns):

            surcharges_local_origin_df.columns = columns
            surcharges_local_origin_df = surcharges_local_origin_df.drop(columns=[column for column in surcharges_local_origin_df.columns if column.startswith('drop')])
            surcharges_local_origin_df["charges"].fillna(method='ffill', inplace=True)

            if "origin_port" in surcharges_local_origin_df:
                surcharges_local_origin_df.dropna(subset=["origin_port", "amount"], inplace = True)
            surcharges_df_with_rates = surcharges_local_origin_df.loc[surcharges_local_origin_df["amount"].str.contains("^\d+",  na =False)]
            surcharges_df_with_rates["currency"], surcharges_df_with_rates["amount"]= surcharges_df_with_rates["amount"].str.split(" ").str[1], surcharges_df_with_rates["amount"].str.split(" ").str[0]
            surcharges_df_without_rates = surcharges_local_origin_df.loc[~surcharges_local_origin_df["amount"].str.contains("^\d", na =False)]
            surcharges_df_without_rates["remarks"], surcharges_df_without_rates["amount"] = surcharges_df_without_rates["amount"], ""
            surcharges_result = pd.concat([surcharges_df_with_rates, surcharges_df_without_rates], ignore_index= True)
            self.replace_currency = {"USD" : "", "EUR" : ""}
            surcharges_result["load_type"].replace(self.replace_currency, regex =True , inplace =True)
            surcharges_result.loc[surcharges_result["charges"].str.contains("if requested" , case= False,   na =False), "remarks"] = "if requested"
            surcharges_result["charges_leg"] = "L2"
            surcharges_result["remarks"].replace(self.replace_currency, regex=True, inplace =True)
            return surcharges_result

        def get_freight_df(self, df):

            charges_index, pol_index, pod_index = self.get_indexes(df)
            df = df.iloc[pol_index[0]:charges_index[0], :]
            pols = df.iloc[0, 0].split('POL')[-1].strip().split(',')
            df.iloc[0, 0] = 'POD'
            df.iloc[0, 1:] = df.iloc[1, 1:]
            df.columns = df.iloc[0, :]
            df = df.loc[~df['20\' DV'].str.contains('20\' DV', na=False)]
            if 'Routing // Transit Time' in df.columns:
                col = "Routing // Transit Time"
            elif 'Triest Routing' in df.columns:
                col = 'Triest Routing'
            df['ROUTING POINTS'] = df[col].str.split('//', expand=True)[0].str.replace('direct',
                                                                                       '').str.replace(
                'Tranship.', '').str.replace('via', '').apply(lambda x: x.strip())
            df['TRANSIT TIME'] = df[col].str.split('//', expand=True)[1].str.split(expand=True)[0].apply(
                lambda x: x.strip())
            df['TIME QUALIFIER (TRANSITTIME)'] = df[col].str.split('//', expand=True)[1].str.split(expand=True)[
                1].apply(lambda x: x.strip())
            df.drop([col], axis=1, inplace=True)
            df = All_Trades_Export_MSC.Bls.apply_pols(self, df, pols)
            return df

        def capture(self):

            dates, remarks = All_Trades_Export_MSC.Bls.get_headers(self)
            df, yellow_df = All_Trades_Export_MSC.Bls.get_main_df(self)
            freight_df = self.get_freight_df(df)
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)
            freight_df['start_date'], freight_df['expiry_date'], freight_df['contract_id'] = validity['start_date'], validity['end_date'], remarks
            if 'Remarks' in freight_df.columns:
                freight_df['REMARKS'] = freight_df['Remarks']
                freight_df['REMARKS'] = freight_df['REMARKS'].apply(lambda x: x.strip('\n'))
                freight_df.drop(columns=['Remarks'], axis=1, inplace=True)
            freight_df.rename(columns={'POD': 'destination_port', 'POL': 'origin_port', "REMARKS": "remarks",
                                       "ROUTING POINTS": "via", "TRANSIT TIME": "transit_time"},
                              inplace=True)

            freight_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT TIME OF",
                                                                                "^Greece$")
            freight_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_df)
            if len(freight_surcharges_df.columns) == 4:
                columns = ["charges", "20GP", "40GP", "remarks"]
                freight_surcharges_result_df = All_Trades_Export_MSC.Caribic.clean_surcharges_with_cols_load_types(self, freight_surcharges_df,columns)
                freight_surcharges_result_df = freight_surcharges_result_df.loc[~freight_surcharges_result_df["charges"].str.contains("Turkey", case =False, na=False)]
                freight_surcharges_result_df["destination_country"] = "Turkey;GEORGIA;RUSSIA;ROMANIA"



            freight_surcharges_greece_df = All_Trades_Export_MSC.India_fix.surcharges_(self,   "^Greece$", "LOCAL CHARGES AT ORIGIN")
            freight_surcharges_greece_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self,  freight_surcharges_greece_df)
            if len(freight_surcharges_greece_df.columns) == 4:
                columns = ["charges", "20GP", "40GP", "remarks"]
                freight_surcharges_greece_result_df = All_Trades_Export_MSC.Caribic.clean_surcharges_with_cols_load_types(self, freight_surcharges_greece_df, columns)
                freight_surcharges_greece_result_df = freight_surcharges_greece_result_df.loc[ ~freight_surcharges_greece_result_df["charges"].str.contains("Greece", case=False, na=False)]
                freight_surcharges_greece_result_df["destination_country"] = "Greece"


            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN", "^ADMINISTRATION SURCHARGES")
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_origin_df)
            if len(surcharges_local_origin_df.columns) == 5:
                columns = ["charges", "origin_port", "amount", "load_type", "remarks"]
                local_charges = self.clean_surcharges_local_origin_with_columns(surcharges_local_origin_df, columns)
                local_charges = local_charges.loc[~((local_charges["charges"].str.contains("THC")) & (local_charges["remarks"].str.contains("only valid")))]

            admin_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^ADMINISTRATION SURCHARGES", None)
            admin_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, admin_surcharges_df)
            if len(admin_surcharges_df.columns) == 4:
                columns = ["charges", "amount", "load_type", "remarks"]
                admin_surcharges_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, admin_surcharges_df, columns)
                # admin_surcharges_result_df.loc[(admin_surcharges_result_df["charges"].str.contains("B/L", na=False, case=False)) & (admin_surcharges_result_df["load_type"].isna()), "load_type"] = "per B/L"

            surcharges_df = pd.concat([freight_surcharges_result_df, freight_surcharges_greece_result_df, local_charges, admin_surcharges_result_df], ignore_index= True)
            surcharges_df['start_date'], surcharges_df['expiry_date'] = validity['start_date'], validity['end_date']

            self.captured_output = {'Freight': freight_df, 'Surcharges': surcharges_df}

    class Canada_wc(Bls):
        def capture(self):
            dates, remarks = self.get_headers()
            df, yellow_df = self.get_main_df()
            freight_df = self.get_freight_df(df)
            freight_df['Remarks'] = remarks
            freight_df.reset_index(drop=True, inplace=True)
            freight_df.rename(
                columns={'POD': 'destination_port', 'Remarks': 'remarks', 'POL': 'origin_port', "ROUTING_POINT": "via"},
                inplace=True)
            freight_df['start_date'], freight_df['expiry_date'] = dates['start_date'], dates['expiry_date']

            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN", "^SURCHARGES - VALID")
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_origin_df)
            if len(surcharges_local_origin_df.columns) == 4:
                columns = ["charges", "origin_port", "amount", "load_type"]
                local_charges = All_Trades_Export_MSC.India_fix.clean_surcharges_local_origin_with_cols(self, surcharges_local_origin_df, columns)

            freight_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID", "^LOCAL CHARGES AT DESTINATION")
            freight_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_df)
            if len(freight_surcharges_df.columns) == 5:
                columns = ["charges", "remarks", "amount", "load_type", "drop1"]
                freight_surcharges_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, freight_surcharges_df, columns)

                if freight_surcharges_result_df["charges"].str.contains("class", case=False, na=False).any():
                    freight_surcharges_result_df.loc[freight_surcharges_result_df["charges"].str.contains("class", case=False, na=False), "commodity"] = freight_surcharges_result_df["charges"].str.split("class").str[1]
                    freight_surcharges_result_df["commodity"] = freight_surcharges_result_df["commodity"].str.split("/")
                    freight_surcharges_result_df = freight_surcharges_result_df.explode("commodity")

            surcharges_local_destination_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT DESTINATION", "^ADMINISTRATIVE SURCHARGES")
            surcharges_local_destination_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_destination_df)
            if len(surcharges_local_destination_df.columns) == 4:
                columns = ["charges", "amount", "load_type", "remarks"]
                local_destination_charges = All_Trades_Export_MSC.India_fix.clean_surcharges_local_destination_with_cols(self, surcharges_local_destination_df, columns)

            administrative_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^ADMINISTRATIVE SURCHARGES", None)
            administrative_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, administrative_surcharges_df)
            if len(administrative_surcharges_df.columns) == 4:
                columns = ["charges", "amount", "load_type", "remarks"]
                administrative_surcharges_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, administrative_surcharges_df, columns)

            surcharges_df = pd.concat([local_charges, freight_surcharges_result_df, local_destination_charges, administrative_surcharges_result_df], ignore_index=True)
            surcharges_df['start_date'], surcharges_df['expiry_date'] = dates['start_date'], dates['expiry_date']

            self.captured_output = {'Freight': freight_df, "Surcharges" : surcharges_df}

    class SouthAmerica(Bls):

        def common_freight(self, df, pols):

            dfs = []

            if df.iloc[:, 0].str.contains('via ', na=False).any():
                via_index = list(df[df.iloc[:, 0].str.contains('via ', na=False)].index)[0]
                via = df.loc[via_index, 'POD'].split('via ')[-1].split('/')[0].replace(',', ';')
                df['ROUTING POINTS'] = ''
                df.loc[via_index, 'ROUTING POINTS'] = via
                df.loc[via_index, 'POD'] = df.loc[via_index, 'POD'].split()[0]

            for element in pols:
                holder = df.copy(deep=True)
                holder['POL'] = element
                dfs.append(holder)

            return pd.concat(dfs, ignore_index=True)

        def get_freight_df(self, df):

            charges_index, pol_index, pod_index = self.get_indexes(df)
            df = df.iloc[pol_index[0]: charges_index[0], :]
            pols = df.iloc[0, 0].split()[-1].split('/')
            df.iloc[0, 0] = 'POD'
            df.columns = df.iloc[0, :]
            df = df.iloc[1:, :]
            if '20DV' in df.columns:
                df = df.loc[df['20DV'] != '']
            elif '20\' DV' in df.columns:
                df = df.loc[df['20\' DV'] != '']
            if '40DV/HC' in df.columns:
                df['40DV'] = df['40DV/HC'].copy(deep=True)
                df['40HC'] = df['40DV/HC'].copy(deep=True)
                df.drop(columns=['40DV/HC'], inplace=True)
            elif '40\'DV/HC' in df.columns:
                df['40DV'] = df['40\'DV/HC'].copy(deep=True)
                df['40HC'] = df['40\'DV/HC'].copy(deep=True)
                df.drop(columns=['40\'DV/HC'], inplace=True)
            df.drop([''], axis=1, inplace=True)
            df = self.common_freight(df, pols)
            if '20DV' in df.columns:
                df['CURRENCY'] = df['20DV'].str.split(expand=True)[1]
            elif '20\' DV' in df.columns:
                df['CURRENCY'] = df['20\' DV'].str.split(expand=True)[1]
            for column in df.columns:
                if column[0].isdigit():
                    df[column] = df[column].str.split(expand=True)[0]
            return df

        def capture(self):

            dates, remarks = All_Trades_Export_MSC.Bls.get_headers(self)
            df, yellow_df = All_Trades_Export_MSC.Bls.get_main_df(self)
            freight_df = self.get_freight_df(df)
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)
            freight_df['start_date'], freight_df['expiry_date'] = validity['start_date'], validity['end_date']

            if 'Comments' in freight_df.columns:
                freight_df['remarks'] = remarks + '\n' + freight_df['Comments']
                freight_df['remarks'] = freight_df['remarks'].apply(lambda x: x.strip('\n'))
            else:
                freight_df['remarks'] = remarks
            if '20\' DV' not in freight_df.columns:
                freight_df.rename(columns={'POD': 'destination_port', '20DV': '20GP', '40\'RE': '40RE', '40DV': '40GP', '40HC': '40HC'
                                           , 'POL': 'origin_port', "CURRENCY":"currency"}, inplace=True)
                freight_df.drop(columns=['Comments', 'DEM/DET free time at POD'], inplace=True)
            else:
                freight_df.rename(columns={'POD': 'destination_port', '20\' DV': '20GP', '40DV': '40GP', '40HC': '40HC'
                                           , 'POL': 'origin_port', "CURRENCY":"currency"}, inplace=True)
                freight_df.drop(columns=['DEM/DET free time at POD'], inplace=True)


            if "ROUTING POINTS" in freight_df:
                freight_df.rename(columns={'ROUTING POINTS': 'via'}, inplace=True)

            freight_df['destination_port'] = freight_df['destination_port'].str.replace('*', '').str.replace('/', ';')

            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN", "^SURCHARGES - VALID")
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_origin_df)
            if len(surcharges_local_origin_df.columns) == 5:
                columns = ["charges", "origin_port", "amount", "load_type", "remarks"]
                local_charges = All_Trades_Export_MSC.India_fix.clean_surcharges_local_origin_with_cols(self, surcharges_local_origin_df, columns)

            freight_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID", None)
            freight_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_df)
            if len(freight_surcharges_df.columns) == 5:
                columns = ["charges", "drop1", "amount", "load_type", "remarks"]
                freight_surcharges__result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, freight_surcharges_df, columns)


            surcharges_df = pd.concat([local_charges, freight_surcharges__result_df], ignore_index=True)
            surcharges_df['start_date'], surcharges_df['expiry_date'] = validity['start_date'], validity['end_date']

            self.captured_output = {'Freight': freight_df, "Surcharges": surcharges_df}

    class SouthAfrica(Bls):

        def get_freight_df(self, df):

            charges_index, pol_index, pod_index = self.get_indexes(df)
            df = df.loc[pol_index[0]: charges_index[0]-1, :]
            dfs = []
            for i in range(len(pol_index)):
                pols = df.loc[pol_index[i], 0].split()[-1].strip()
                holder = df.loc[pol_index[i]+1:pol_index[i+1]-1, :]
                holder.iloc[0, 0] = 'POD'
                holder.columns = holder.iloc[0, :]
                holder = holder.iloc[1:, :]
                if holder.iloc[:, 0].str.contains('via', na=False).any():
                    holder.loc[holder.iloc[:, 0].str.contains('via', na=False), ('POD', 'Remarks')] = holder[holder.iloc[:, 0].str.contains('via', na=False)]['POD'].iloc[0].split('/')[0].strip(), holder[holder.iloc[:, 0].str.contains('via', na=False)]['POD'].iloc[0].split('via')[-1].strip()
                    holder['POD'] = holder['POD'].str.replace(', ', ';').str.replace(' & ', ';')
                    holder.drop(columns=[''], inplace=True)
                    holder['POL'] = pols
                dfs.append(holder)
                if i+2 == len(pol_index):
                    pols = df.loc[pol_index[i+1], 0].split()[-1].strip()
                    holder = df.loc[pol_index[i+1]+1:, :]
                    holder.iloc[0, 0] = 'POD'
                    holder.columns = holder.iloc[0, :]
                    holder = holder.iloc[1:, :]
                    if holder.iloc[:, 0].str.contains('via', na=False).any():
                        holder.loc[holder.iloc[:, 0].str.contains('via', na=False), ('POD', 'Remarks')] = \
                        holder[holder.iloc[:, 0].str.contains('via', na=False)]['POD'].iloc[0].split('/')[0].strip(), \
                        holder[holder.iloc[:, 0].str.contains('via', na=False)]['POD'].iloc[0].split('via')[-1].strip()
                        holder['POD'] = holder['POD'].str.replace(', ', ';').str.replace(' & ', ';')
                        holder.drop(columns=[''], inplace=True)
                        holder['POL'] = pols
                    dfs.append(holder)
                    break
            return pd.concat(dfs, ignore_index=True)

        def capture(self):

            dates, remarks = All_Trades_Export_MSC.Bls.get_headers(self)
            df, yellow_df = All_Trades_Export_MSC.Bls.get_main_df(self)
            freight_df = self.get_freight_df(df)
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)
            freight_df["start_date"], freight_df["expiry_date"]= validity["start_date"], validity["end_date"]

            freight_df['CURRENCY'] = freight_df['20\' DV'].str.split(expand=True)[1]
            for column in freight_df.columns:
                if column[0].isdigit():
                    freight_df[column] = freight_df[column].str.split(expand=True)[0]
            freight_df['REMARKS'] = remarks + '\n' + freight_df['Remarks']
            freight_df['REMARKS'] = freight_df['REMARKS'].apply(lambda x: x.strip('\n'))
            freight_df.rename(columns={'POD': 'destination_port', '20\' DV': '20GP', '40\'DV': '40GP', '40\' HC': '40HC'
                                       , 'POL': 'origin_port',"REMARKS": "contract_id", "CURRENCY":"currency"}, inplace=True)
            freight_df.drop(columns=['Remarks'], inplace=True)


            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN", "^SURCHARGES - VALID")
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_origin_df)
            if len(surcharges_local_origin_df.columns) == 5:
                columns = ["charges", "origin_port", "amount", "load_type", "remarks"]
                local_charges = All_Trades_Export_MSC.India_fix.clean_surcharges_local_origin_with_cols(self, surcharges_local_origin_df, columns)

            freight_surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID", None)
            freight_surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, freight_surcharges_df)
            if len(freight_surcharges_df.columns) == 5:
                columns = ["charges", "drop1", "amount", "load_type", "remarks"]
                freight_surcharges_result_df = All_Trades_Export_MSC.India_fix.clean_surcharge_with_cols(self, freight_surcharges_df, columns)
                freight_surcharges_result_df.loc[freight_surcharges_result_df["charges"].str.contains("ON REQUEST", case=False, na=False), "amount"] = "ON REQUEST"
                freight_surcharges_result_df.loc[freight_surcharges_result_df["load_type"]=="", "load_type"] = "container"

            surcharges_df = pd.concat([local_charges, freight_surcharges_result_df], ignore_index=True)
            surcharges_df["start_date"], surcharges_df["expiry_date"] = validity["start_date"], validity["end_date"]

            self.captured_output = {'Freight': freight_df, "Surcharges": surcharges_df}

    class SouthAmerica(Bls):

        def common_freight(self, df, pols):

            dfs = []

            if df.iloc[:, 0].str.contains('via ', na=False).any():
                via_index = list(df[df.iloc[:, 0].str.contains('via ', na=False)].index)[0]
                via = df.loc[via_index, 'POD'].split('via ')[-1].split('/')[0].replace(',', ';')
                df['ROUTING POINTS'] = ''
                df.loc[via_index, 'ROUTING POINTS'] = via
                df.loc[via_index, 'POD'] = df.loc[via_index, 'POD'].split()[0]

            for element in pols:
                holder = df.copy(deep=True)
                holder['POL'] = element
                dfs.append(holder)

            return pd.concat(dfs, ignore_index=True)

        def get_freight_df(self, df):

            charges_index, pol_index, pod_index = self.get_indexes(df)
            df = df.iloc[pol_index[0]: charges_index[0], :]
            pols = df.iloc[0, 0].split()[-1].split('/')
            df.iloc[0, 0] = 'POD'
            df.columns = df.iloc[0, :]
            df = df.iloc[1:, :]
            if '20DV' in df.columns:
                df = df.loc[df['20DV'] != '']
            elif '20\' DV' in df.columns:
                df = df.loc[df['20\' DV'] != '']
            if '40DV/HC' in df.columns:
                df['40DV'] = df['40DV/HC'].copy(deep=True)
                df['40HC'] = df['40DV/HC'].copy(deep=True)
                df.drop(columns=['40DV/HC'], inplace=True)
            elif '40\'DV/HC' in df.columns:
                df['40DV'] = df['40\'DV/HC'].copy(deep=True)
                df['40HC'] = df['40\'DV/HC'].copy(deep=True)
                df.drop(columns=['40\'DV/HC'], inplace=True)
            df.drop([''], axis=1, inplace=True)
            df = self.common_freight(df, pols)
            if '20DV' in df.columns:
                df['CURRENCY'] = df['20DV'].str.split(expand=True)[1]
            elif '20\' DV' in df.columns:
                df['CURRENCY'] = df['20\' DV'].str.split(expand=True)[1]
            for column in df.columns:
                if column[0].isdigit():
                    df[column] = df[column].str.split(expand=True)[0]
            return df

        def get_additional_charges(self):
            def get_additional(addon_text):
                regex_pattern = r"POL(?P<origin>.*)to(?P<desti>.*)EUR.(?P<GP20>\d+)(.*)\/.EUR.(?P<GP40>\d+).(.*)"
                return re.search(regex_pattern, addon_text)

            captured_data = self.df.iloc[:, 0].apply(lambda x: get_additional(str(x)))
            additional_charges = {}
            for i in captured_data:
                if i:
                    additional_charges["origin_port"] = i.group("origin").strip()
                    additional_charges["destination_port"] = i.group("desti").replace("surcharge", "").strip()
                    additional_charges["20GP"], additional_charges["40GP"], additional_charges["40HC"] = int(i.group("GP20")),  int(i.group("GP40")),  int(i.group("GP40"))

            return additional_charges

        def apply_addon_to_freight(self, additional_charges, freight):

            freight = All_Trades_Export_MSC.Fak_Rates_Fix.melt_load_type(self, freight)
            for load_type in ["20GP", "40GP", "40HC"]:
                freight.loc[(freight["destination_port"].str.contains(additional_charges["destination_port"])) &
                (freight["origin_port"].str.contains(additional_charges["origin_port"]))
                & (freight["load_type"]==load_type), "add_on"] = additional_charges[load_type]

            freight["add_on"].fillna(0, inplace=True)
            freight["amount"] = freight["amount"].astype(int) + freight["add_on"].astype(int)

            return freight


        def capture(self):

            dates, remarks = All_Trades_Export_MSC.Bls.get_headers(self)
            df, yellow_df = All_Trades_Export_MSC.Bls.get_main_df(self)
            freight_df = self.get_freight_df(df)
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)

            add_on = self.get_additional_charges()

            freight_df['contract_id'] = remarks
            if 'Comments' in freight_df.columns:
                freight_df['REMARKS'] =  freight_df['Comments']
                freight_df['remarks'] = freight_df['REMARKS'].apply(lambda x: x.strip('\n'))

            if '20\' DV' not in freight_df.columns:
                freight_df.rename(columns={'POD': 'destination_port', '20DV': '20GP', '40\'RE': '40RE', '40DV': '40GP', '40HC': '40HC'
                                           , 'POL': 'origin_port', "CURRENCY":"currency"}, inplace=True)
                freight_df.drop(columns=['Comments', 'DEM/DET free time at POD'], inplace=True)
            else:
                freight_df.rename(columns={'POD': 'destination_port', '20\' DV': '20GP', '40DV': '40GP', '40HC': '40HC'
                                           , 'POL': 'origin_port', "CURRENCY":"currency"}, inplace=True)
                freight_df.drop(columns=['DEM/DET free time at POD'], inplace=True)

            freight_df = self.apply_addon_to_freight(add_on, freight_df)

            if "ROUTING POINTS" in freight_df:
                freight_df.drop(columns=['ROUTING POINTS', "add_on"], inplace=True)

            # freight_df.drop(columns=['DEM/DET free time at POD', "ROUTING POINTS"], inplace=True)

            freight_df["start_date"], freight_df["expiry_date"] = validity["start_date"], validity["end_date"]
            freight_df['destination_port'] = freight_df['destination_port'].str.replace('*', '').str.replace('/', ';')

            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN", "^SURCHARGES - VALID AT")
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_origin_df)
            if len(surcharges_local_origin_df.columns) == 5:
                columns = ["charges", "origin_port", "20GP", "40GP", "40RE"]
                local_charges = All_Trades_Export_MSC.Caribic.clean_surcharges_local_origin_with_cols_load_types(self, surcharges_local_origin_df, columns)

            elif len(surcharges_local_origin_df.columns) == 4:
                columns = ["charges", "origin_port", "20GP", "40GP"]
                local_charges = All_Trades_Export_MSC.Caribic.clean_surcharges_local_origin_with_cols_load_types(self, surcharges_local_origin_df, columns)


            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT", "^ADMINISTRATIVE SURCHARGES")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 6:

                columns = ["charges", "drop1", "20GP", "40GP", "40RE", "remarks"]
                surcharges_result_df = All_Trades_Export_MSC.Caribic.clean_surcharges_with_cols_load_types(self, surcharges_df, columns)
                surcharges_result_df.loc[surcharges_result_df["charges"].str.contains("Paraguay", case=False, na =False), "destination_country"] = "Paraguay"

            elif len(surcharges_df.columns) == 5:
                columns = ["charges", "drop1", "20GP", "40GP", "remarks"]
                surcharges_result_df = All_Trades_Export_MSC.Caribic.clean_surcharges_with_cols_load_types(self, surcharges_df, columns)

            surcharges_admin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^ADMINISTRATIVE SURCHARGES", None)
            surcharges_admin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_admin_df)
            if len(surcharges_admin_df.columns) == 5:
                columns = ["charges", "20GP", "40GP", "40RE", "remarks"]
                surcharges_admin_result_df = All_Trades_Export_MSC.Caribic.clean_surcharges_with_cols_load_types(self, surcharges_admin_df, columns)

            elif len(surcharges_admin_df.columns) == 4:
                columns = ["charges", "20GP", "40GP", "remarks"]
                surcharges_admin_result_df = All_Trades_Export_MSC.Caribic.clean_surcharges_with_cols_load_types(self, surcharges_admin_df, columns)


            combined_surcharges_df = pd.concat([local_charges, surcharges_result_df, surcharges_admin_result_df], ignore_index=True)
            combined_surcharges_df["start_date"], combined_surcharges_df["expiry_date"] = validity["start_date"], validity["end_date"]

            for load_type in ["20GP", "40GP", "40HC", "40RE"]:
                if load_type in freight_df:
                    freight_df.loc[~freight_df[load_type].str.contains("^[0-9]", na=False), load_type] ="ON REQUEST"

            combined_surcharges_df["amount"] = combined_surcharges_df["amount"].apply(lambda x: All_Trades_Export_MSC.Indian_ocean_fix.convert_currency_format(self, x))
            combined_surcharges_df['origin_port'] = combined_surcharges_df['origin_port'].str.split(';')
            combined_surcharges_df = combined_surcharges_df.explode('origin_port')
            self.captured_output = {'Freight': freight_df, "Surcharges": combined_surcharges_df}

    class South_america_east(SouthAmerica):


        def map_arbitary_rate(self, freight_df, arbitary_df):

            arbitary_df_dict = arbitary_df.to_dict("records")
            dps = []
            for arbitary_rate in arbitary_df_dict:
                freight_df_copy = freight_df.loc[(freight_df["destination_port"].str.contains(arbitary_rate["via"])) & (freight_df["load_type"].str.contains(arbitary_rate["load_type"]))]
                if arbitary_rate["amount"] != "ON REQUEST":
                    freight_df_copy["amount"] = freight_df_copy["amount"].astype(int) + int(arbitary_rate["amount"])
                else:
                    freight_df_copy["amount"] = arbitary_rate["amount"]

                freight_df_copy["via_port"] = arbitary_rate["destination_port"]

                freight_df_copy.drop(columns ="via", inplace=True)

                freight_df_copy["charges"], freight_df_copy["basis"], freight_df_copy["charges_leg"], freight_df_copy["contract_id"] = "Origin Arbitary charges", "container", "L2", freight_df_copy["contract_id"] + "\n" + "Feeder"
                dps.append(freight_df_copy)
            arbitary_rates = pd.concat(dps, ignore_index= True)

            columns_rename = {"destination_port": "via", "origin_port": "icd", "via_port": "to"}
            arbitary_rates.rename(columns=columns_rename, inplace=True)

            return arbitary_rates

        def get_feeder_additional(self, freight_df):

            def feeder_portname(portname):
                captured_validity = re.search(r"on top of (.*) rate", portname)
                if captured_validity:
                    return captured_validity.group(1).strip()
                return nan
            # on top of regex pattern for Feeder additional
            freight_df["via"] = freight_df["destination_port"].apply(lambda x: feeder_portname(str(x)))
            return freight_df


        def capture(self):

            dates, remarks = All_Trades_Export_MSC.Bls.get_headers(self)
            df, yellow_df = All_Trades_Export_MSC.Bls.get_main_df(self)
            freight_df = self.get_freight_df(df)
            validity = All_Trades_Export_MSC.Fak_Rates_Fix.get_validity(self)

            freight_df['contract_id'] = remarks

            if 'Comments' in freight_df.columns:
                freight_df['remarks'] = freight_df['Comments']

            if '20\' DV' not in freight_df.columns:
                freight_df.rename(columns={'POD': 'destination_port', '20DV': '20GP', '40\'RE': '40RE', '40DV': '40GP', '40HC': '40HC'
                                           , 'POL': 'origin_port', 'ROUTING POINTS': 'via', "CURRENCY": "currency"}, inplace=True)
                freight_df.drop(columns=['Comments', 'DEM/DET free time at POD'], inplace=True)
            else:
                freight_df.rename(columns={'POD': 'destination_port', '20\' DV': '20GP', '40DV': '40GP', '40HC': '40HC'
                                           , 'POL': 'origin_port', 'ROUTING POINTS': 'via', "CURRENCY":"currency"}, inplace=True)
                freight_df.drop(columns=['DEM/DET free time at POD'], inplace=True)

            freight_df.loc[freight_df["remarks"].str.contains("Felixstowe", case = False, na= False), "origin_port"] = "Felixstowe"

            freight_df = self.get_feeder_additional(freight_df)


            for load_type in ["20GP", "40GP", "40HC", "40RE"]:
                if load_type in freight_df:
                    freight_df.loc[~freight_df[load_type].str.contains("^[0-9]", na=False), load_type] ="ON REQUEST"

            freight_df["start_date"], freight_df["expiry_date"] = validity["start_date"], validity["end_date"]
            freight_df['destination_port'] = freight_df['destination_port'].str.replace('*', '').str.replace('/', ';')


            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^LOCAL CHARGES AT ORIGIN", "^SURCHARGES - VALID AT")
            surcharges_local_origin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_local_origin_df)
            if len(surcharges_local_origin_df.columns) == 5:
                columns = ["charges", "origin_port", "20GP", "40GP", "40RE"]
                local_charges = All_Trades_Export_MSC.Caribic.clean_surcharges_local_origin_with_cols_load_types(self, surcharges_local_origin_df, columns)

            elif len(surcharges_local_origin_df.columns) == 4:
                columns = ["charges", "origin_port", "20GP", "40GP"]
                local_charges = All_Trades_Export_MSC.Caribic.clean_surcharges_local_origin_with_cols_load_types(self, surcharges_local_origin_df, columns)


            surcharges_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^SURCHARGES - VALID AT", "^ADMINISTRATIVE SURCHARGES")
            surcharges_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_df)
            if len(surcharges_df.columns) == 6:

                columns = ["charges", "drop1", "20GP", "40GP", "40RE", "remarks"]
                surcharges_result_df = All_Trades_Export_MSC.Caribic.clean_surcharges_with_cols_load_types(self, surcharges_df, columns)
                surcharges_result_df.loc[surcharges_result_df["charges"].str.contains("Antwerp", case=False, na =False), "origin_port"] = "Antwerp"
                surcharges_result_df.loc[surcharges_result_df["charges"].str.contains("Paraguay", case=False, na =False), "destination_country"] = "Paraguay"
                surcharges_result_df.loc[surcharges_result_df["charges"].str.contains("if requested", case=False, na =False), "remarks"] = "if requested"

            elif len(surcharges_df.columns) == 5:
                columns = ["charges", "drop1", "20GP", "40GP", "remarks"]
                surcharges_result_df = All_Trades_Export_MSC.Caribic.clean_surcharges_with_cols_load_types(self, surcharges_df, columns)
                surcharges_result_df.loc[surcharges_result_df["charges"].str.contains("Antwerp", case=False, na =False), "origin_port"] = "Antwerp"
                surcharges_result_df.loc[surcharges_result_df["remarks"].str.contains("if requested", case=False, na =False), "remarks"] = "if requested"


            surcharges_admin_df = All_Trades_Export_MSC.India_fix.surcharges_(self, "^ADMINISTRATIVE SURCHARGES", None)
            surcharges_admin_df = All_Trades_Export_MSC.India_fix.remove_empty_rows_and_columns_(self, surcharges_admin_df)
            if len(surcharges_admin_df.columns) == 5:
                columns = ["charges", "20GP", "40GP", "40RE", "remarks"]
                surcharges_admin_result_df = All_Trades_Export_MSC.Caribic.clean_surcharges_with_cols_load_types(self, surcharges_admin_df, columns)
                surcharges_admin_result_df.loc[surcharges_admin_result_df["remarks"].str.contains("per B/L", case=False, na =False), "remarks"] = ""

            elif len(surcharges_admin_df.columns) == 4:
                columns = ["charges", "20GP", "40GP", "remarks"]
                surcharges_admin_result_df = All_Trades_Export_MSC.Caribic.clean_surcharges_with_cols_load_types(self, surcharges_admin_df, columns)


            surcharges_df = pd.concat([local_charges, surcharges_result_df, surcharges_admin_result_df], ignore_index=True)
            surcharges_df["start_date"], surcharges_df["expiry_date"] = validity["start_date"], validity["end_date"]
            arbitrary_df = freight_df.loc[freight_df["via"].notna()]
            freight_df = freight_df.loc[freight_df["via"].isna()]
            arbitrary_df = All_Trades_Export_MSC.Fak_Rates_Fix.melt_load_type(self, arbitrary_df)
            freight_df = All_Trades_Export_MSC.Fak_Rates_Fix.melt_load_type(self, freight_df)

            freight_df["amount"] = freight_df["amount"].apply(lambda x: All_Trades_Export_MSC.Indian_ocean_fix.convert_currency_format(self, x))
            arbitrary_df["amount"] = arbitrary_df["amount"].apply(lambda x: All_Trades_Export_MSC.Indian_ocean_fix.convert_currency_format(self, x))
            arbitrary_df = self.map_arbitary_rate(freight_df, arbitrary_df)
            surcharges_df["amount"].fillna("", inplace=True)
            surcharges_df.loc[surcharges_df["remarks"].str.contains("if requested", case=False, na=False), "remarks"] = "if requested"
            # surcharges_df["amount"] = surcharges_df["amount"] + " " + surcharges_df["currency"]
            #surcharges_df["amount"] = surcharges_df["amount"].apply(lambda x: All_Trades_Export_MSC.Indian_ocean_fix.convert_currency_format_with_currency(self, x.split()[0], x.split()[1]))
            surcharges_df["amount"] = surcharges_df["amount"].apply(lambda x: All_Trades_Export_MSC.Indian_ocean_fix.convert_currency_format(self, x))
            self.captured_output = {'Freight': freight_df, "Surcharges": surcharges_df, "Arbitrary Charges": arbitrary_df}


    def resolve_dependency(cls, fix_outputs):

        def apply_port_names_based_on_currency(freight_df, surcharges_df, charges_to_apply):
            grouped = freight_df.groupby("currency")["standard_destination_port_port_locode", "standard_destination_port_port_name"]
            lookup_currency_name = {}
            lookup_currency_code = {}

            for curr, destination_port_group in grouped:
                destination_country_name_ports = destination_port_group["standard_destination_port_port_name"].drop_duplicates().to_string(index=False).replace("\n", ";")
                destination_country_code_ports = destination_port_group["standard_destination_port_port_locode"].drop_duplicates().to_string(index=False).replace("\n", ";")
                lookup_currency_name[curr] = destination_country_name_ports
                lookup_currency_code[curr] = destination_country_code_ports
            surcharges_df["reference_currency"] = surcharges_df["currency"].str.strip().str.replace("%", "EUR")
            for charge in charges_to_apply:
                surcharges_df.loc[surcharges_df["standard_charges_surcharges_text"].str.contains(charge, na=False, case=False), "standard_destination_port_port_name"] = surcharges_df["reference_currency"].map(lookup_currency_name)
                surcharges_df.loc[surcharges_df["standard_charges_surcharges_text"].str.contains(charge, na=False, case=False), "standard_destination_port_port_locode"] = surcharges_df["reference_currency"].map(lookup_currency_code)

            return surcharges_df

        def remove_additional_surcharges(freight_df, charges_df):

            if "standard_origin_port_port_locode" in freight_df:
                origin_ports_to_check = list(freight_df["standard_origin_port_port_locode"].unique())
                origin_ports_to_check.append(nan)
                charges_df = charges_df.loc[charges_df["standard_origin_port_port_locode"].isin(origin_ports_to_check)]
            return charges_df

        def remove_additional_surcharges_for_destination_port(freight_df, charges_df):

            if "standard_destination_port_port_locode" in freight_df:
                ports_to_check = list(freight_df["standard_destination_port_port_locode"].unique())
                ports_to_check.append(nan)
                charges_df = charges_df.loc[charges_df["standard_destination_port_port_locode"].isin(ports_to_check)]
            return charges_df

        AUS_surcharges_df = pd.DataFrame()
        if "AUS incl. Trieste" in fix_outputs:
            AUS_freight = fix_outputs["AUS incl. Trieste"]
            AUS_freight_df = AUS_freight["Freight"]
            AUS_freight_df["unique"] = "AUS incl. Trieste"
            AUS_surcharges_df = AUS_freight["Charges"]
            AUS_surcharges_df["unique"] = "AUS incl. Trieste"

        if "Destination Charges AUS" in fix_outputs:
            AUS_destination_charges = fix_outputs["Destination Charges AUS"]
            AUS_destination_charges = AUS_destination_charges["Charges"]
            AUS_destination_charges["start_date"], AUS_destination_charges["expiry_date"], AUS_destination_charges["unique"] = AUS_surcharges_df["start_date"].iloc[0], AUS_surcharges_df["expiry_date"].iloc[0], "Destination Charges AUS"


            AUS_surcharges_df = pd.concat([AUS_surcharges_df, AUS_destination_charges],ignore_index= True)

            AUS_freight_df["contract_no"] = "MSCMCDE"
            AUS_freight_df["sub_vendor"] = "MSC Mediterranean Shipping Company S.A. CORPORATION"
            AUS_surcharges_df["contract_no"] = "MSCMCDE"
            AUS_surcharges_df["sub_vendor"] = "MSC Mediterranean Shipping Company S.A. CORPORATION"


            fix_outputs = {"Freight": AUS_freight_df, "Charges": AUS_surcharges_df}

            return fix_outputs

        if "India" in fix_outputs:
            India_Freight = fix_outputs["India"]
            India_Freight_df = India_Freight["Freight"]
            India_Arbitary_df = India_Freight["Arbitrary Charges"]
            India_Surcharges_df = India_Freight["Charges"]
            India_Surcharges_df["unique"] = "India"
            India_Arbitary_df["unique"] = "India"
            India_Freight_df["unique"] = "India"

            if "Destination Charges IPAK" in fix_outputs:
                India_destination_charges = fix_outputs["Destination Charges IPAK"]
                India_destination_charges_df = India_destination_charges["Charges"]
                India_destination_charges_df["unique"] = "Destination Charges IPAK"
                India_Surcharges_df = pd.concat([India_Surcharges_df, India_destination_charges_df], ignore_index= True)
                India_Surcharges_df["start_date"] = India_Freight_df["start_date"].iloc[0]
                India_Surcharges_df["expiry_date"] = India_Freight_df["expiry_date"].iloc[0]

            India_Freight_df["contract_no"] = "MSCMCDE"
            India_Freight_df["sub_vendor"] = "MSC Mediterranean Shipping Company S.A. CORPORATION"
            India_Surcharges_df["contract_no"] = "MSCMCDE"
            India_Surcharges_df["sub_vendor"] = "MSC Mediterranean Shipping Company S.A. CORPORATION"
            India_Arbitary_df["contract_no"] = "MSCMCDE"
            India_Arbitary_df["sub_vendor"] = "MSC Mediterranean Shipping Company S.A. CORPORATION"

            fix_outputs = {"Freight": India_Freight_df,  "Charges": India_Surcharges_df , "Arbitrary Charges" : India_Arbitary_df}
            return fix_outputs

        India_Arbitary_df = pd.DataFrame()
        if "Indian Ocean" in fix_outputs:
            India_Ocean_Freight = fix_outputs["Indian Ocean"]
            India_ocean_freight_df = India_Ocean_Freight["Freight"]
            India_ocean_surcharges_df = India_Ocean_Freight["Charges"]
            India_ocean_surcharges_df["unique"] = "Indian Ocean"
            India_ocean_freight_df["unique"] = "Indian Ocean"
            India_ocean_surcharges_df = remove_additional_surcharges(India_ocean_freight_df, India_ocean_surcharges_df)

            fix_outputs = {"Freight": India_ocean_freight_df,  "Charges": India_ocean_surcharges_df}
            return fix_outputs

        if "Middle East" in fix_outputs:
            middle_east = fix_outputs["Middle East"]
            middle_east_freight_df = middle_east["Freight"]
            middle_east_surcharges_df = middle_east["Charges"]
            middle_east_freight_df["unique"] = "Middle East"
            middle_east_surcharges_df["unique"] = "Middle East"

            fix_outputs = {"Freight": middle_east_freight_df,  "Charges": middle_east_surcharges_df }
            return fix_outputs

        if "BLS " in fix_outputs:
            bls_dict = fix_outputs["BLS "]
            bls_freight_df = bls_dict["Freight"]
            bls_surcharges_df = bls_dict["Charges"]
            bls_freight_df["unique"],  bls_surcharges_df["unique"] = "BLS", "BLS"
            charges_to_apply = ["Hauptlauf Gefahrgutzuschlag", "Bunker Adjustment Factor", "Low Sulfur Surcharge", "quipment Imbalance Surcharge", "Carrier Security Surcharge"]
            bls_surcharges_df = apply_port_names_based_on_currency(bls_freight_df, bls_surcharges_df, charges_to_apply)
            bls_surcharges_df = bls_surcharges_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            fix_outputs = {"Freight": bls_freight_df, "Charges": bls_surcharges_df}
            return fix_outputs


        if "Canada EC" in fix_outputs:
            canada_ec = fix_outputs["Canada EC"]
            canada_ec_freight_df = canada_ec["Freight"]
            canada_ec_surcharges_df = canada_ec["Charges"]
            canada_ec_freight_df["unique"],  canada_ec_surcharges_df["unique"] = "Canada EC", "Canada EC"
            fix_outputs = {"Freight": canada_ec_freight_df, "Charges": canada_ec_surcharges_df}
            return fix_outputs


        if "Canada WC" in fix_outputs:
            canada_wc = fix_outputs["Canada WC"]
            canada_wc_freight_df = canada_wc["Freight"]
            canada_wc_surcharges_df = canada_wc["Charges"]
            canada_wc_freight_df["unique"],  canada_wc_surcharges_df["unique"] = "Canada WC", "Canada WC"
            fix_outputs = {"Freight": canada_wc_freight_df, "Charges": canada_wc_surcharges_df}
            return fix_outputs


        if "Caribic" in fix_outputs:
            caribic = fix_outputs["Caribic"]
            caribic_freight_df = caribic["Freight"]
            caribic_surcharges_df = caribic["Charges"]
            caribic_freight_df["unique"], caribic_surcharges_df["unique"] = "Caribic", "Caribic"

            caribic_surcharges_df = remove_additional_surcharges(caribic_freight_df, caribic_surcharges_df)
            fix_outputs = {"Freight": caribic_freight_df, "Charges": caribic_surcharges_df}

            return fix_outputs

        if "Central America" in fix_outputs:
            central_america = fix_outputs["Central America"]
            central_america_freight_df = central_america["Freight"]
            central_america_surcharges_df = central_america["Charges"]
            central_america_surcharges_df = remove_additional_surcharges(central_america_freight_df, central_america_surcharges_df)
            central_america_freight_df["unique"], central_america_surcharges_df["unique"] = "Central America", "Central America"

            fix_outputs = {"Freight": central_america_freight_df, "Charges": central_america_surcharges_df}

            return fix_outputs

        if "EAF" in fix_outputs:
            eaf_dict = fix_outputs["EAF"]
            eaf_freight_df = eaf_dict["Freight"]
            eaf_surcharges_df = eaf_dict["Charges"]
            eaf_freight_df["unique"],  eaf_surcharges_df["unique"] = "EAF", "EAF"

            fix_outputs ={"Freight": eaf_freight_df, "Charges": eaf_surcharges_df}

            return fix_outputs

        def apply_ports_based_on_currency(eam_freight_df, eam_surcharges_df, charges_to_apply):
            grouped = eam_freight_df.groupby("currency")["standard_destination_port_port_country_code", "standard_destination_port_country_name"]
            lookup_currency_name = {}
            lookup_currency_code = {}

            for curr, destination_port_group in grouped:
                destination_country_name_ports = destination_port_group["standard_destination_port_country_name"].drop_duplicates().to_string(index=False).replace("\n", ";")
                destination_country_code_ports = destination_port_group["standard_destination_port_port_country_code"].drop_duplicates().to_string(index=False).replace("\n", ";")
                lookup_currency_name[curr] = destination_country_name_ports
                lookup_currency_code[curr] = destination_country_code_ports
            eam_surcharges_df["reference_currency"] = eam_surcharges_df["currency"].str.strip()
            for charge in charges_to_apply:
                eam_surcharges_df.loc[eam_surcharges_df["standard_charges_surcharges_text"].str.contains(charge, na=False, case=False), "standard_destination_country_country_name"] = eam_surcharges_df["reference_currency"].map(lookup_currency_name)
                eam_surcharges_df.loc[eam_surcharges_df["standard_charges_surcharges_text"].str.contains(charge, na=False, case=False), "standard_destination_country_country_code"] = eam_surcharges_df["reference_currency"].map(lookup_currency_code)

            return eam_surcharges_df


        if "EAM" in fix_outputs:
            eam_dict = fix_outputs["EAM"]
            eam_freight_df = eam_dict["Freight"]
            eam_surcharges_df = eam_dict["Charges"]
            eam_freight_df["unique"],  eam_surcharges_df["unique"] = "EAM", "EAM"
            charges_to_apply = ["(Low Sulfur Surcharge)", "(Carrier Security Surcharge)", "Equipment Imbalance Surcharge", "Hauptlauf"]
            eam_surcharges_df = apply_ports_based_on_currency(eam_freight_df, eam_surcharges_df, charges_to_apply)


            fix_outputs = {"Freight": eam_freight_df, "Charges": eam_surcharges_df}

            return fix_outputs

        if "EAM Adria" in fix_outputs:
            adria = fix_outputs["EAM Adria"]
            adria_freight_df = adria["Freight"]
            adria_surcharges_df = adria["Charges"]
            adria_freight_df["unique"],  adria_surcharges_df["unique"] = "EAM Adria", "EAM Adria"
            charges_to_apply = ["(Bunker Adjustment Factor)", "(Carrier Security Surcharge)", "Hauptlauf", "War Risk Beirut (WAR)", "Peak Season Surcharge (PSS)"]
            # adria_surcharges_df = apply_ports_based_on_currency(adria_freight_df, adria_surcharges_df, charges_to_apply)
            adria_surcharges_df = remove_additional_surcharges(adria_freight_df, adria_surcharges_df)
            fix_outputs ={"Freight": adria_freight_df, "Charges": adria_surcharges_df}
            return fix_outputs



        if "Far East" in fix_outputs:
            far_east = fix_outputs["Far East"]
            far_east_freight_df = far_east["Freight"]
            far_east_surcharges_df = far_east["Charges"]
            far_east_freight_df["unique"],  far_east_surcharges_df["unique"] = "Far East", "Far East"
            far_east_surcharges_df = remove_additional_surcharges(far_east_freight_df, far_east_surcharges_df)
            fix_outputs ={"Freight": far_east_freight_df, "Charges": far_east_surcharges_df}
            return fix_outputs

        if "Far East Adria" in fix_outputs:
            far_east_adria = fix_outputs["Far East Adria"]
            far_east_freight_df = far_east_adria["Freight"]
            far_east_surcharges_df = far_east_adria["Charges"]
            far_east_freight_df["unique"],  far_east_surcharges_df["unique"] = "Far East Adria", "Far East Adria"

            fix_outputs = {"Freight": far_east_freight_df, "Charges": far_east_surcharges_df }

            return fix_outputs


        if "GRT" in fix_outputs:
            grt = fix_outputs["GRT"]
            grt_freight_df = grt["Freight"]
            grt_surcharges_df = grt["Charges"]
            grt_freight_df["unique"],  grt_surcharges_df["unique"] = "GRT", "GRT"
            fix_outputs = {"Freight": grt_freight_df, "Charges": grt_surcharges_df}
            return fix_outputs



        if "GRT Adria" in fix_outputs:
            grt_adria = fix_outputs["GRT Adria"]
            grt_freight_df = grt_adria["Freight"]
            grt_surcharges_df = grt_adria["Charges"]
            grt_freight_df["unique"],  grt_surcharges_df["unique"] = "GRT Adria", "GRT Adria"

            fix_outputs = {"Freight": grt_freight_df, "Charges": grt_surcharges_df}

            return fix_outputs
        if "Middle East Trieste" in fix_outputs:
            middle_east_trieste = fix_outputs["Middle East Trieste"]
            middle_east_trieste_freight_df = middle_east_trieste["Freight"]
            middle_east_trieste_surcharges_df = middle_east_trieste["Charges"]
            middle_east_trieste_freight_df["unique"] = "Middle East Trieste"
            middle_east_trieste_surcharges_df["unique"] = "Middle East Trieste"
            fix_outputs = {"Freight": middle_east_trieste_freight_df,  "Charges": middle_east_trieste_surcharges_df }
            return fix_outputs

        if "NAF" in fix_outputs:
            naf= fix_outputs["NAF"]
            naf_freight_df = naf["Freight"]
            naf_surcharges_df = naf["Charges"]
            naf_freight_df["unique"] = "NAF"
            naf_surcharges_df["unique"] = "NAF"

            fix_outputs = {"Freight": naf_freight_df,  "Charges": naf_surcharges_df}
            return fix_outputs

        if "RED SEA incl. Trieste" in fix_outputs:
            red_sea = fix_outputs["RED SEA incl. Trieste"]
            red_sea_freight_df = red_sea["Freight"]
            red_sea_surcharges_df = red_sea["Charges"]
            red_sea_freight_df["unique"] = "RED SEA incl. Trieste"
            red_sea_surcharges_df["unique"] = "RED SEA incl. Trieste"
            charges_to_apply = ["(Carrier Security Surcharge)"]
            red_sea_surcharges_df = apply_port_names_based_on_currency(red_sea_freight_df, red_sea_surcharges_df, charges_to_apply)
            fix_outputs = {"Freight": red_sea_freight_df,  "Charges": red_sea_surcharges_df}
            return fix_outputs


        if "West Africa" in fix_outputs:
            west_africa = fix_outputs["West Africa"]
            west_africa_freight_df = west_africa["Freight"]
            west_africa_surcharges_df = west_africa["Charges"]
            west_africa_freight_df["unique"], west_africa_surcharges_df["unique"] = "West Africa", "West Africa"
            west_africa_surcharges_df = remove_additional_surcharges_for_destination_port(west_africa_freight_df, west_africa_surcharges_df)
            fix_outputs = {"Freight": west_africa_freight_df,  "Charges": west_africa_surcharges_df}
            return fix_outputs


        if "South Africa" in fix_outputs:
            south_africa = fix_outputs["South Africa"]
            south_africa_freight_df = south_africa["Freight"]
            south_africa_surcharges_df = south_africa["Charges"]
            south_africa_freight_df["unique"], south_africa_surcharges_df["unique"] = "South Africa", "South Africa"

            fix_outputs = {"Freight": south_africa_freight_df,  "Charges": south_africa_surcharges_df }
            return fix_outputs

        if "South America East" in fix_outputs:
            south_america_east = fix_outputs["South America East"]
            south_america_east_freight_df = south_america_east["Freight"]
            south_america_east_surcharge_df = south_america_east["Charges"]
            south_america_east_arbitary_df = south_america_east["Arbitrary Charges"]

            south_america_east_freight_df["unique"], south_america_east_surcharge_df["unique"], south_america_east_arbitary_df["unique"] = "South America East", "South America East", "South America East"

            fix_outputs = {"Freight": south_america_east_freight_df,  "Charges": south_america_east_surcharge_df, "Arbitrary Charges" : south_america_east_arbitary_df}
            return fix_outputs


        if "South America West" in fix_outputs:
            south_america_west = fix_outputs["South America West"]
            south_america_east_freight_df = south_america_west["Freight"]
            south_america_east_surcharge_df = south_america_west["Charges"]

            south_america_east_freight_df["unique"] = "South America West"
            south_america_east_surcharge_df["unique"] = "South America West"
            south_america_east_surcharge_df = remove_additional_surcharges(south_america_east_freight_df, south_america_east_surcharge_df)

            fix_outputs = {"Freight": south_america_east_freight_df,  "Charges": south_america_east_surcharge_df}
            return fix_outputs


class Flexport_MSC_v2(BaseTemplate):
    class _USWC_v1(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_freight_table(self):
            line_item_index = list(self.df[(self.df[0] == ("Trade :"))].index)
            line_item = self.df[line_item_index[0]:line_item_index[0] + 1].iloc[0, 4]
            index = list(self.df[(self.df[0] == ("Contract Holder"))].index)
            freight_df = self.df[index[0]:self.df.tail(1).index.values[0] + 1].copy(deep=True)
            row_1 = freight_df.iloc[0]
            row_2 = freight_df.iloc[1]
            columns = []
            for i, j in zip(row_1, row_2):
                if j:
                    columns.append(j)
                else:
                    columns.append(i)
            freight_df.columns = columns
            freight_df = freight_df[2:].copy()
            if len(freight_df['PSS'].unique()[0]) > 5:
                freight_df['PSS'] = freight_df['PSS'].str.replace('USD', 'USD/')
                freight_df[['currency', '20GP_PSS', '40GP_PSS', '40HC_PSS', '45HC_PSS']] = \
                    freight_df['PSS'].str.split('/', expand=True)
                freight_df.drop(['PSS'], axis=1, inplace=True)
            freight_df['region'] = line_item
            return freight_df

        def add_inclusions_subject(self, freight_df):
            columns_ = freight_df.columns.tolist()
            for col in columns_:
                col = col.strip()
                if re.search('^[A-Z]{3}$', col) is not None:
                    if 'Yes' in freight_df[col].unique() or 'No' in freight_df[col].unique():
                        freight_df.loc[(freight_df[col] == 'Yes'), col + ' Included'] = ''
                        freight_df.loc[(freight_df[col] == 'No'), col + ' Included'] = 'X'
                        freight_df.loc[(freight_df[col] == 'NA'), col + ' Included'] = 'NA'
                        freight_df.drop(col, axis=1, inplace=True)
                    elif len(freight_df[col].unique()[0]) == 0:
                        freight_df.drop(col, axis=1, inplace=True)
            return freight_df

        @staticmethod
        def format_output(df_freight):
            output = {'Freight': df_freight}
            return output

        def capture(self):
            freight_df = self.get_freight_table()
            freight_df = self.add_inclusions_subject(freight_df)
            self.captured_output = self.format_output(freight_df)

        def clean(self):
            freight_df = self.captured_output['Freight']
            freight_df.rename(
                columns={"Commodity": 'commodity', "Port \nof Load": 'origin_port', "Place of Receipt": 'origin_icd',
                         "Port of \nDischarge": "destination_port", "Destination": "destination_icd",
                         'EXPORT/IMPORT SERVICE': 'mode_of_transportation', "20' GP": "20GP",
                         "40'GP": '40GP', "40'HC": '40HC', "45'HC": "45HC", "20'NOR": "20NOR", "20'RE": "20RE",
                         "Named Account": "customer_name", "40'RE": "40RE", "40'NOR": "40NOR", "40'HR": "40HR",
                         "ETD/Effective Date (mm/dd/yy)": 'start_date', "Reference No": "bulletin",
                         'Expiry Date (mm/dd/yy)': 'expiry_date', 'Special Notes and Comments': 'remarks',
                         "Amd #": "Amendment no.",
                         "Indicate (CY-CY, Ramp or Door - to include zip code)": 'mode_of_transportation',
                         "Service": "loop"
                         }, inplace=True)

            rename_port = {
                "USA/USEC BASE PORTS 2": "CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, "
                                         "LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USEC BASE PORTS 3": "NEW YORK/NORFOLK/CHARLESTON/SAVANNAH",
                "USA/USEC BASE PORTS 4": "BALTIMORE/HOUSTON/JACKSONVILLE/NEW ORLEANS/PHILADELPHIA",
                "USA/USEC BASE PORTS 5": "BALTIMORE, MD/CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USEC BASE PORTS 6": "CHARLESTON, SC/HOUSTON, TX/ JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, "
                                     "LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USWC BASE PORTS 1": "LONG BEACH, CA/LOS ANGELES, CA/OAKLAND, CA",
                "USA/USWC BASE PORTS 2": "LONG BEACH, CA/LOS ANGELES, CA",
                "USA/USEC BASE PORTS": "BALTIMORE, MD/CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USEC BASE PORTS 1": "BALTIMORE, MD/CHARLESTON, SC/JACKSONVILLE, FL/NEW YORK, NY/NORFOLK, VA/PHILADEPHIA, PA/SAVANNAH, GA",

                "USA/GULF BASE PORTS": "HOUSTON, TX/NEW ORLEANS, LA"

            }
            for code in rename_port:
                _code = ''.join(rename_port[code])
                freight_df.replace(code, _code, inplace=True)

            # freight_df['destination_port'].str.replace(rename_port)

            if "Vessel/Voyage" in freight_df:
                freight_df.drop(["Vessel/Voyage"], axis=1, inplace=True)
            freight_df.drop(['Contract Holder', 'A/C/D', 'Named Account Code',
                             "Surcharges as per MSC's Tariff", "Service Contract No."], axis=1, inplace=True)

            def str_replace(regex, subst, col):
                freight_df[col] = freight_df[col].str.replace(regex, subst)
                return freight_df

            col_rename = ['origin_port', 'destination_port']

            for col in col_rename:
                freight_df = str_replace(r"\/", ";", col)

            freight_df["mode_of_transportation"] = freight_df["mode_of_transportation"].str.replace("CY-CY", "")

            def fix_date(freight_df, col):
                if freight_df.loc[freight_df[col].astype(str).str.contains('222', na=False)].index.any():
                    index_date = freight_df.loc[freight_df[col].str.contains('222', na=False)].index.to_list()
                    today_date = date.today()
                    freight_df.loc[index_date, col] = \
                        freight_df.loc[index_date, col].str.extract(r'(\d{0,2}\.\d{0,2}\.)')[0] + str(today_date.year)

                return freight_df

            freight_df = fix_date(freight_df, 'start_date')
            freight_df = fix_date(freight_df, 'expiry_date')
            freight_df['start_date'] = pd.to_datetime(freight_df['start_date']).dt.strftime('%Y-%m-%d')
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date']).dt.strftime('%Y-%m-%d')
            freight_df['charges_leg'] = 'L3'
            freight_df['currency'] = 'USD'
            freight_df['charges'] = 'Basic Ocean Freight'
            self.cleaned_output = {'Freight': freight_df}

    class _USWC_arb_pre(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_freight_table(self):
            line_item_index = list(self.df[(self.df[0] == ("Trade :"))].index)
            line_item = self.df[line_item_index[0]:line_item_index[0] + 1].iloc[0, 4]
            indexes = list(self.df[(self.df[0] == ("Reference No"))].index)
            indexes.append(self.df.tail(1).index.values[0])
            arb_df_final = pd.DataFrame()
            pre_df_final = pd.DataFrame()
            for index in range(len(indexes) - 1):
                check_arb_or_pre = self.df[indexes[index] - 1:indexes[index]].copy(deep=True).iloc[0, 0]
                arb_df = self.df[indexes[index]:indexes[index + 1] - 1].copy(deep=True)
                arb_df.reset_index(drop=True, inplace=True)
                arb_df.drop(1, axis=0, inplace=True)
                arb_df.columns = arb_df.iloc[0]
                arb_df = arb_df[1:].copy()
                if not arb_df.loc[(arb_df['Special Notes and Comments'] == 'USWC')].empty:
                    arb_df = arb_df.loc[(arb_df['Special Notes and Comments'] == 'USWC')]

                arb_df = arb_df.loc[~(arb_df["20' GP"] == "Suspended until further notice")]

                arb_df['Reference No'] = arb_df['Reference No'].astype(str)
                notes = arb_df.loc[(arb_df['Origins'] == '')]
                indexes_drop = arb_df[(arb_df['Origins'] == '')].index.tolist()
                notes = r'\n'.join(notes['Reference No'])
                arb_df['remarks'] = notes
                arb_df['remarks'] = arb_df['Special Notes and Comments'].str.cat(arb_df['remarks'])
                arb_df['region'] = line_item

                """Hard coded part"""

                arb_df.loc[arb_df['Origins'].str.contains(
                    'on top of Japan'), 'POL'] = "YOKOHAMA/NAGOYA/OSAKA/KOBE/HAKATA/TOKYO"
                arb_df.loc[arb_df['Origins'].str.contains(
                    'on top of Kao'), 'POL'] = "KAOHSIUNG"

                """******"""

                arb_df.drop(indexes_drop, inplace=True)
                if 'Effective Date' in arb_df:
                    arb_df.rename(
                        columns={"Effective Date": 'Valid from', "Expiry Date": 'Valid To',
                                 }, inplace=True)

                if re.search('Pre-Carriage', str(check_arb_or_pre)) is not None:
                    arb_df['charges_leg'] = 'L3'
                    arb_df['charges'] = 'basic ocean freight charge at pre-carriage'
                    pre_df_final = pd.concat([pre_df_final, arb_df], ignore_index=True)

                else:
                    arb_df['charges'] = 'origin arbitrary charges'
                    arb_df['charges_leg'] = 'L2'
                    arb_df['at'] = 'origin'
                    arb_df_final = pd.concat([arb_df_final, arb_df], ignore_index=True)

            replace_dict={r"\s?\(Add on top of .*\)":"",}
            pre_df_final['POL'].replace("on top of ", "", inplace=True,regex=True)
            pre_df_final['POL'].replace(replace_dict,inplace=True,regex=True)
            pre_df_final['Origins'].replace(replace_dict,inplace=True,regex=True)
            pre_df_final['Origins'].replace(" - Old Port","",inplace=True,regex=True)
            arb_df_final['Origins'].replace(" - Old Port","",inplace=True,regex=True)
            arb_df_final['Origins'].replace("Old Port", "", inplace=True, regex=True)
            pre_df_final['Origins'].replace("Add on top of", "", inplace=True, regex=True)
            pre_df_final['Origins'].replace("on top of","",inplace=True, regex=True)
            return pre_df_final, arb_df_final

        @staticmethod
        def format_output(arb_df_final, pre_df_final=None):
            if pre_df_final.empty:
                output = {'Freight': pre_df_final}
            else:
                output = {'Freight': pre_df_final, 'Arbitrary Charges': arb_df_final}
            return output

        def capture(self):
            pre_df_final, arb_df_final = self.get_freight_table()
            self.captured_output = self.format_output(arb_df_final, pre_df_final)

        def clean(self):
            freight_df = self.captured_output['Freight']
            freight_df.rename(
                columns={"POL": 'destination_port', "Origins": 'origin_port',
                         "20' GP": "20GP", "40'GP": '40GP', "40'HC": '40HC', "45'HC": "45HC",
                         "MOT": "mode_of_transportation_origin", "Valid from": 'start_date',
                         "Reference No": "bulletin", 'Valid To': 'expiry_date', "Service": "loop"
                         }, inplace=True)

            rename_port = {
                "USA/USEC BASE PORTS 2": "CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, "
                                         "LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USEC BASE PORTS 3": "NEW YORK/NORFOLK/CHARLESTON/SAVANNAH",
                "USA/USEC BASE PORTS 4": "BALTIMORE/HOUSTON/JACKSONVILLE/NEW ORLEANS/PHILADELPHIA",
                "USA/USEC BASE PORTS 5": "BALTIMORE, MD/CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USEC BASE PORTS 6": "CHARLESTON, SC/HOUSTON, TX/ JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, "
                                         "LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USWC BASE PORTS 1": "LONG BEACH, CA/LOS ANGELES, CA/OAKLAND, CA",
                "USA/USWC BASE PORTS 2": "LONG BEACH, CA/LOS ANGELES, CA",
                "USA/USEC BASE PORTS": "BALTIMORE, MD/CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USEC BASE PORTS 1": "BALTIMORE, MD/CHARLESTON, SC/JACKSONVILLE, FL/NEW YORK, NY/NORFOLK, VA/PHILADEPHIA, PA/SAVANNAH, GA",

                "USA/GULF BASE PORTS": "HOUSTON, TX/NEW ORLEANS, LA"

                }

            for code in rename_port:
                _code = ';'.join(rename_port[code])
                freight_df.replace(code, _code, inplace=True)

            # freight_df['destination_port'].str.replace(rename_port)

            freight_df.drop(["CY/Door within city limit"], axis=1, inplace=True)

            def str_replace(regex, subst, col):
                freight_df[col] = freight_df[col].str.replace(regex, subst)
                return freight_df

            col_rename = ['origin_port']

            for col in col_rename:
                freight_df = str_replace(r"\/", ";", col)

            freight_df['currency'] = 'USD'

            def fix_date(freight_df, col):
                if freight_df.loc[freight_df[col].astype(str).str.contains('222', na=False)].index.any():
                    index_date = freight_df.loc[freight_df[col].str.contains('222', na=False)].index.to_list()
                    today_date = date.today()
                    freight_df.loc[index_date, col] = \
                        freight_df.loc[index_date, col].str.extract(r'(\d{0,2}\.\d{0,2}\.)')[0] + str(today_date.year)

                return freight_df

            freight_df = fix_date(freight_df, 'start_date')

            freight_df = fix_date(freight_df, 'expiry_date')

            freight_df['start_date'] = pd.to_datetime(freight_df['start_date']).dt.strftime('%Y-%m-%d')
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date']).dt.strftime('%Y-%m-%d')

            arb_df = self.captured_output['Arbitrary Charges']
            arb_df.rename(
                columns={"POL": 'via', "Origins": 'icd',
                         "20' GP": "20GP", "40'GP": '40GP', "40'HC": '40HC', "45'HC": "45HC",
                         "MOT": "mode_of_transportation_origin", "Valid from": 'start_date',
                         "Reference No": "bulletin", 'Valid To': 'expiry_date', "Service": "loop"
                         }, inplace=True)
            arb_df.drop(["CY/Door within city limit"], axis=1, inplace=True)

            def str_replace(regex, subst, col):
                arb_df[col] = arb_df[col].str.replace(regex, subst)
                return arb_df

            col_rename = ['icd']

            for col in col_rename:
                arb_df = str_replace(r"\/", ";", col)

            def fix_date(freight_df, col):
                if freight_df.loc[freight_df[col].astype(str).str.contains('222', na=False)].index.any():
                    index_date = freight_df.loc[freight_df[col].str.contains('222', na=False)].index.to_list()
                    today_date = date.today()
                    freight_df.loc[index_date, col] = \
                        freight_df.loc[index_date, col].str.extract(r'(\d{0,2}\.\d{0,2}\.)')[0] + str(today_date.year)

                return freight_df

            arb_df = fix_date(arb_df, 'start_date')

            arb_df = fix_date(arb_df, 'expiry_date')

            arb_df['start_date'] = pd.to_datetime(arb_df['start_date']).dt.strftime('%Y-%m-%d')
            arb_df['expiry_date'] = pd.to_datetime(arb_df['expiry_date']).dt.strftime('%Y-%m-%d')

            arb_df['currency'] = 'USD'
            self.cleaned_output = {'Freight': freight_df, 'Arbitrary Charges': arb_df}

    class _DiamondTier(_USWC_v1):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):
            freight_df = self.get_freight_table()
            freight_df = self.add_inclusions_subject(freight_df)
            self.captured_output = self.format_output(freight_df)

        def clean(self):
            freight_df = self.captured_output['Freight']
            freight_df.loc[freight_df["Service Contract No."].str.contains("DT", na=False),
                           "premium_service"] = "MSC_diamond"

            freight_df.rename(
                columns={"Commodity": 'commodity', "Port \nof Load": 'origin_port', "Place of Receipt": 'origin_icd',
                         "Port of \nDischarge": "destination_port", "Destination": "destination_icd",
                         'EXPORT/IMPORT SERVICE': 'mode_of_transportation', "20' GP": "20GP",
                         "40'GP": '40GP', "40'HC": '40HC', "45'HC": "45HC", "20'NOR": "20NOR", "20'RE": "20RE",
                         "Named Account": "customer_name", "40'RE": "40RE", "40'NOR": "40NOR", "40'HR": "40HR",
                         "ETD/Effective Date (mm/dd/yy)": 'start_date', "Reference No": "bulletin",
                         'Expiry Date (mm/dd/yy)': 'expiry_date', 'Special Notes and Comments': 'remarks',
                         "Amd #": "Amendment no.",
                         "Indicate (CY-CY, Ramp or Door - to include zip code)": 'mode_of_transportation',
                         "Service": "loop"
                         }, inplace=True)

            rename_port = {
                "USA/USEC BASE PORTS 2": "CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, "
                                         "LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USEC BASE PORTS 3": "NEW YORK/NORFOLK/CHARLESTON/SAVANNAH",
                "USA/USEC BASE PORTS 4": "BALTIMORE/HOUSTON/JACKSONVILLE/NEW ORLEANS/PHILADELPHIA",
                "USA/USEC BASE PORTS 5": "BALTIMORE, MD/CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USEC BASE PORTS 6": "CHARLESTON, SC/HOUSTON, TX/ JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, "
                                     "LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USWC BASE PORTS 1": "LONG BEACH, CA/LOS ANGELES, CA/OAKLAND, CA",
                "USA/USWC BASE PORTS 2": "LONG BEACH, CA/LOS ANGELES, CA",
                "USA/USEC BASE PORTS": "BALTIMORE, MD/CHARLESTON, SC/HOUSTON, TX/JACKSONVILLE, FL/NEW YORK, NY/NEW ORLEANS, LA/NORFOLK, VA/PHILADELPHIA, PA/SAVANNAH, GA",
                "USA/USEC BASE PORTS 1": "BALTIMORE, MD/CHARLESTON, SC/JACKSONVILLE, FL/NEW YORK, NY/NORFOLK, VA/PHILADEPHIA, PA/SAVANNAH, GA",

                "USA/GULF BASE PORTS": "HOUSTON, TX/NEW ORLEANS, LA"

            }
            for code in rename_port:
                _code = ''.join(rename_port[code])
                freight_df.replace(code, _code, inplace=True)

            # freight_df['destination_port'].str.replace(rename_port)

            if "Vessel/Voyage" in freight_df:
                freight_df.drop(["Vessel/Voyage"], axis=1, inplace=True)
            freight_df.drop(['Contract Holder', 'A/C/D', 'Named Account Code',
                             "Surcharges as per MSC's Tariff", "Service Contract No."], axis=1, inplace=True)

            def str_replace(regex, subst, col):
                freight_df[col] = freight_df[col].str.replace(regex, subst)
                return freight_df

            col_rename = ['origin_port', 'destination_port']

            for col in col_rename:
                freight_df = str_replace(r"\/", ";", col)

            freight_df["mode_of_transportation"] = freight_df["mode_of_transportation"].str.replace("CY-CY", "")

            def fix_date(freight_df, col):
                if freight_df.loc[freight_df[col].astype(str).str.contains('222', na=False)].index.any():
                    index_date = freight_df.loc[freight_df[col].str.contains('222', na=False)].index.to_list()
                    today_date = date.today()
                    freight_df.loc[index_date, col] = \
                        freight_df.loc[index_date, col].str.extract(r'(\d{0,2}\.\d{0,2}\.)')[0] + str(today_date.year)

                return freight_df

            freight_df = fix_date(freight_df, 'start_date')

            freight_df = fix_date(freight_df, 'expiry_date')

            freight_df['start_date'] = pd.to_datetime(freight_df['start_date']).dt.strftime('%Y-%m-%d')
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date']).dt.strftime('%Y-%m-%d')
            freight_df['charges_leg'] = 'L3'
            freight_df['currency'] = 'USD'
            freight_df['charges'] = 'Basic Ocean Freight'
            self.cleaned_output = {'Freight': freight_df}

    class _Reference(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_reference_dict(self):
            reference_df = self.df
            reference_df.columns = reference_df.iloc[0]
            reference_df = reference_df[1:].copy()
            reference_df.drop(columns=['Codes'], inplace=True)
            origin_df = reference_df.iloc[:, :2].copy()
            destination_df = reference_df.iloc[:, 2:].copy()
            destination_df.dropna(how='any', axis=0, inplace=True)
            origin_df.dropna(how='any', axis=0, inplace=True)
            destination_code_dict = destination_df.set_index('Port of Discharge').to_dict()['Location']
            origin_code_dict = origin_df.set_index('Port of Load').to_dict()['Location']

            return {'origin': origin_code_dict, 'destination': destination_code_dict}

        def capture(self):
            ref_dict = self.get_reference_dict()
            self.captured_output = ref_dict

        def clean(self):
            self.cleaned_output = self.captured_output

    @classmethod
    def resolve_dependency(cls, fix_outputs):
        ref = fix_outputs.pop('Reference Tables')
        origin_dict = ref['origin']
        destination_dict = ref['destination']

        dict_new = {"SEA BASE PORTS": "INDONESIA/MALAYSIA/SINGAPORE/THAILAND/VIETNAM",
                    "NPRC": "SHANGHAI/NINGBO/QINGDAO/XINGANG/DALIAN",
                    "SPRC": "HONG KONG/CHIWAN/YANTIAN/XIAMEN/FUZHOU/SHEKOU",
                    "USA;USWC IPI": "LONG BEACH, CA/LOS ANGELES, CA/OAKLAND, CA"
                    }

        origin_dict.update(dict_new)
        destination_dict.update(dict_new)

        for sheet, df_dict in fix_outputs.items():
            for leg, df in df_dict.items():
                if leg == 'Freight':
                    df['origin_port'].replace(origin_dict, regex=True, inplace=True)
                    df['destination_port'].replace(destination_dict, regex=True, inplace=True)
                    df['destination_port'].replace('/', ';', regex=True, inplace=True)
                    df['origin_port'].replace('/', ';', regex=True, inplace=True)
                    fix_outputs[sheet][leg] = df

                if leg == 'Arbitrary Charges':
                    df['icd'].replace(origin_dict, regex=True, inplace=True)
                    df['via'].replace(destination_dict, regex=True, inplace=True)
                    df['icd'].replace('/', ';', regex=True, inplace=True)
                    df['via'].replace('/', ';', regex=True, inplace=True)
                    fix_outputs[sheet][leg] = df
        return fix_outputs
