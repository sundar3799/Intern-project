from logging import getLogger
from base import BaseTemplate, BaseFix
from custom_exceptions import InputValidationError
import pandas as pd
import re
from datetime import datetime
import warnings
from numpy import nan
# import numpy as np
# from datetime import datetime
from dateutil.parser import parse

warnings.simplefilter(action='ignore', category=FutureWarning)

log = getLogger(__name__)


class CV_GoldStar_v1(BaseTemplate):
    class SG(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_validity(self):
            def get_start_date(date_str):
                return re.search(r"Validity:(.*)", date_str)

            captured_data = self.df.iloc[:, 0].apply(lambda x: get_start_date(str(x)))
            validity = {}
            validity["start_date"] = ""
            for i in captured_data:
                if i:
                    validity["start_date"] = i.group(1)

            def get_end_date(date_str):
                return re.search(r"Validity:  till end of(.*)", date_str)

            captured_data = self.df.iloc[:, 7].apply(lambda x: get_end_date(str(x)))
            validity["expiry_date"] = ""
            for i in captured_data:
                if i:
                    validity["expiry_date"] = parse('28 ' + i.group(1))

            if validity["expiry_date"] == "":
                captured_data = self.df.iloc[:, 6].apply(lambda x: get_end_date(str(x)))
                for i in captured_data:
                    if i:
                        validity["expiry_date"] = parse('28 ' + i.group(1))

            return validity

        def get_freight_df(self, freight_df):

            flag = 0
            freight_df = freight_df.reset_index(drop=True)
            freight_df.columns = freight_df.iloc[0, :]
            freight_df = freight_df.iloc[1:, :]
            freight_df = freight_df.loc[freight_df['POL'] != '']
            freight_df.reset_index(drop=True, inplace=True)
            freight_df['subject_to'] = ''
            freight_df.rename(columns={'20\'DV': '20GP', '40\'GP': '40GP', '40\'HC': '40HC'}, inplace=True)

            if len(list(set(list(freight_df.columns)))) != len(list(freight_df.columns)):

                cols = list(freight_df.columns)

                for i in range(len(cols)):

                    if i+2 != len(cols):
                        if cols[i] == cols[i+1]:

                            cols[i] += '20'
                            cols[i+1] += '40'
                    else:
                        break

                freight_df.columns = cols

            for column in freight_df.columns:
                if 'DV/HC' in column:
                    name = ' '.join(column.split()[:-1])
                    freight_df[name + ' ' + column.split()[-1].split('/')[0]] = freight_df[column]
                    freight_df[name + ' ' + column.split()[-1].split('/')[-1]] = freight_df[column]
                    freight_df.drop(columns=column, axis=1, inplace=True)

            for column in freight_df.columns:
                for i in range(freight_df.shape[0]):
                    if isinstance(freight_df[column][i], str) and 'incl' in freight_df[column][i]:
                        if 'BAF' in column:
                            holder = 'BAF'
                        else:
                            holder = column.split()[0]
                        if holder not in freight_df['INCLUSIVE'][i]:
                            freight_df['INCLUSIVE'][i] += '/' + holder
                        freight_df[column][i] = 'INCLUSIVE'
                    '''
                    elif isinstance(freight_df[column][i], int) and freight_df[column].astype(str).str.contains('incl', na=False).any():
                        freight_df['20GP_'+column][i] = freight_df[column][i]
                        freight_df['40GP_' + column][i] = freight_df[column][i]
                        freight_df['40HC_' + column][i] = freight_df[column][i]
                    '''
                    if isinstance(freight_df[column][i], str) and 'Incl' in freight_df[column][i]:
                        holder = column.split()[0]
                        if holder not in freight_df['INCLUSIVE'][i]:
                            freight_df['INCLUSIVE'][i] += '/' + holder
                        freight_df[column][i] = 'INCLUSIVE'
                    '''
                    elif isinstance(freight_df[column][i], int) and freight_df[column].astype(str).str.contains('Incl', na=False).any():
                        freight_df['20GP_' + column][i] = freight_df[column][i]
                        freight_df['40GP_' + column][i] = freight_df[column][i]
                        freight_df['40HC_' + column][i] = freight_df[column][i]
                    '''
                    if isinstance(freight_df[column][i], str) and 'Tariff' in freight_df[column][i]:
                        holder = column.split()[0]
                        if holder not in freight_df['subject_to'][i]:
                            freight_df['subject_to'][i] += '/' + holder
                        freight_df[column][i] = 'SUBJECT'
                    '''
                    elif isinstance(freight_df[column][i], int) and freight_df[column].astype(str).str.contains('Tariff', na=False).any():
                        freight_df['20GP_' + column][i] = freight_df[column][i]
                        freight_df['40GP_' + column][i] = freight_df[column][i]
                        freight_df['40HC_' + column][i] = freight_df[column][i]
                    '''

            drops = []
            '''
            for column in freight_df.columns:
                if freight_df[column].astype(str).str.contains('INCLUSIVE').any():
                    if not freight_df[column].astype(str).str.contains('INCLUSIVE').all() and freight_df[column].astype(str).str.contains('^[0-9]').any():
                        freight_df['20GP_'+column] = freight_df[column]
                        freight_df['40GP_' + column] = freight_df[column]
                        freight_df['40HC_' + column] = freight_df[column]
                    else:
                        for i in range(freight_df.shape[0]):
                            if isinstance(freight_df[column][i], int):
                                freight_df['20GP_' + column] = freight_df[column]
                                freight_df['40GP_' + column] = freight_df[column]
                                freight_df['40HC_' + column] = freight_df[column]

                elif freight_df[column].astype(str).str.contains('SUBJECT').any() and freight_df[column].astype(str).str.contains('^[0-9]').any():
                    if not freight_df[column].astype(str).str.contains('SUBJECT').all():
                        freight_df['20GP_' + column] = freight_df[column]
                        freight_df['40GP_' + column] = freight_df[column]
                        freight_df['40HC_' + column] = freight_df[column]

                    else:
                        for i in range(freight_df.shape[0]):
                            if isinstance(freight_df[column][i], int):
                                freight_df['20GP_' + column] = freight_df[column]
                                freight_df['40GP_' + column] = freight_df[column]
                                freight_df['40HC_' + column] = freight_df[column]
            '''
            freight_df.replace('', nan, inplace=True)
            for column in freight_df.columns:
                if freight_df[column].astype(str).str.contains('INCLUSIVE|SUBJECT').all():
                    drops.append(column)
                elif freight_df[column].astype(str).str.contains('INCLUSIVE').all():
                    drops.append(column)
                elif freight_df[column].astype(str).str.contains('SUBJECT').all():
                    drops.append(column)
                elif freight_df[column].astype(str).str.contains('SUBJECT').any() and not freight_df[column].astype(
                        str).str.contains('^[0-9]').any():
                    drops.append(column)
                elif freight_df[column].astype(str).str.contains('INCLUSIVE').any() and not freight_df[column].astype(
                        str).str.contains('^[0-9]').any():
                    drops.append(column)

            '''
            for column in freight_df.columns:
                try:
                    if freight_df[column].str.contains('incl', na=False).any() or freight_df[column].str.contains('Incl', na=False).any():
                        holder = column.split()[0]
                        freight_df.loc[freight_df[column] == 'incl', 'INCLUSIVE'] = freight_df.loc[freight_df[column] == 'incl', 'INCLUSIVE'] + "/" + holder
                        freight_df.loc[freight_df[column] == 'Incl', 'INCLUSIVE'] = freight_df.loc[freight_df[column] == 'Incl', 'INCLUSIVE'] + "/" + holder
                        freight_df[column].replace('incl', 'X', inplace=True)
                        freight_df[column].replace('Incl', 'X', inplace=True)
                    elif freight_df[column].str.contains('Tariff', na=False).any():
                        holder = column.split()[0]
                        freight_df.loc[freight_df[column] == 'Tariff', 'subject_to'] = freight_df.loc[freight_df[column] == 'Tariff', 'subject_to'] + "/" + holder
                        freight_df[column].replace('Tariff', '', inplace=True)
                except AttributeError:
                    pass
            '''
            freight_df.replace(nan, '', inplace=True)
            freight_df['subject_to'] = freight_df['subject_to'].apply(lambda x: x.strip('/'))
            if drops:
                for column in drops:
                    try:
                        freight_df.drop(columns=column, axis=1, inplace=True)
                    except KeyError:
                        pass

            freight_df['INCLUSIVE'] = freight_df['INCLUSIVE'].str.split('/').apply(set).str.join('/')
            freight_df.replace('SUBJECT', '', inplace=True)
            freight_df.replace('INCLUSIVE', 'X', inplace=True)
            '''
            freight_df.replace('', nan, inplace=True)
            freight_df.dropna(how='all', axis=1, inplace=True)
            for column in freight_df.columns:
                try:
                    if freight_df[column].str.contains('X', na=False).all():
                        freight_df.drop(columns=column, inplace=True)
                except AttributeError:
                    pass
            '''


            columns_rename = {"POL": "origin_port", "POL PORT CODE": "origin_port_code", "POD PORT CODE":"destination_port_code", "POD": "destination_port", "Currency": "currency", "20'DV": "20GP",


                              "40'GP": "40GP", "40'HC": "40HC",
                              "T/S port": "Routing", "Service": "service_type",
                              "INCLUSIVE": "inclusions", "Validity:": "validity",
                              "Effective Date": "start_date", "Expiry Date": "expiry_date",
                              "EIS (Equipment Imbalance charge) 20": "eis_20",
                              "EIS (Equipment Imbalance charge) 40": "eis_40",
                              "EES (Emergency Equipment Surcharge) 20": "ees_20",
                              "EES (Emergency Equipment Surcharge) 40": "ees_40", "EBS 20": "ebs_20",
                              "EBS 40": "ebs_40", "COV (Emergency Cost Recovery Surcharge) 20": "cov_20",
                              "COV (Emergency Cost Recovery Surcharge) 40": "cov_40",
                              "FAF (Fuel Adjustment Factor) 20": "faf_20", "FAF (Fuel Adjustment Factor) 40": "faf_40",
                              "POL CODE": "origin_port_code", "POD CODE": "destination_port_code", "TT": "tt",
                              "ERP (Empty Return Surcharge) 20 DV": "erp_20",
                              "ERP (Empty Return Surcharge) 40 DV/HC": "erp_40",
                              "EIS (Equipment Imbalance charge) 20 DV": "eis_20dv",
                              "EIS (Equipment Imbalance charge) 40 DV/HC": "eis_40dv",
                              "EES (Emergency Equipment Surcharge) 20 DV": "ees_20dv",
                              "EES (Emergency Equipment Surcharge) 40 DV/HC": "ees_40dv", "NBF 20 DV": "nbf_20dv",
                              "NBF 40 DV/HC": "nbf_40dv", "CRS/COV (Cost Recovery Surcharge) 20 DV": "crs_20dv",
                              "CRS/COV (Cost Recovery Surcharge) 40 DV/HC": "crs_40dv",
                              "D. BAF 20": "d_baf_20", "D. BAF 40": "d_baf_40",
                              "COV (Emergency Cost Recovery Surcharge) 20 DV": "cov_20dv",
                              "COV (Emergency Cost Recovery Surcharge) 40 DV/HC": "cov_40dv",
                              "CNS (Congestion at Disc Port) 20 DV": "cns_20dv",
                              "CNS (Congestion at Disc Port) 40 DV/HC": "cns_40dv", "AMS": "ams",
                              "CRS/COV (Cost Recovery Surcharge) 20": "crs_20",
                              "CRS/COV (Cost Recovery Surcharge) 40": "crs_40",
                              "CRS/COV (Cost Recovery Surcharge)20": "20crs",
                              "CRS/COV (Cost Recovery Surcharge)40": "40crs",
                              "COV (Emergency Cost Recovery Surcharge) 20": "20cov",
                              "COV (Emergency Cost Recovery Surcharge)40": "40cov",
                              "FAF (Fuel Adjustment Factor)20": "20faf", "FAF (Fuel Adjustment Factor)40": "40faf",
                              "DRP (Drop Off Charge) 20": "drp_20", "DRP (Drop Off Charge  40": "drp_40",
                              "CNS (Congestion at Disc Port) 20": "cns_20",
                              "CNS (Congestion at Disc Port) 40": "cns_40",
                              "PIS 20": "pis_20", "PIS 40": "pis_40", "Place of receipt/delivery": "port_of_delivery",
                              "Via": "port_of_discharge", "20": "20GP", "40": "40GP", "40 HQ": "40HQ",
                              "YAC (Yen Appreciation Surcharge)20": "yac20",
                              "YAC (Yen Appreciation Surcharge)40": "yac40", "ICA 20": "ica_20", "ICA 40": "ica_40"}
            freight_df.rename(columns=columns_rename, inplace=True)
            """
            if ("CRS/COV (Cost Recovery Surcharge) 20 DV" in freight_df) | ("CRS/COV (Cost Recovery Surcharge) 20" in freight_df):
                freight_df.rename(columns=columns_rename, inplace=True)
            elif("D. BAF 20" in freight_df):
                freight_df.columns = ['origin_port','origin_port_code','destination_port','destination_port_code','Routing','service_type','tt','20GP','40GP','40HC','inclusions','d_baf_20','d_baf_40','cns_20dv','cns_40dv','yac20','yac40','ica_20','ica_40','subject_to']

            else:
                freight_df.columns = ['origin_port','origin_port_code','destination_port','destination_port_code','Routing','service_type','tt','20GP','40GP','40HC','inclusions','nbf_20dv','nbf_40dv','crs_20','crs_40','cns_20dv','cns_40dv','yac20','yac40','erp_20','erp_40','ica_20','ica_40','subject_to']
            """
            return freight_df

        def capture(self):

            if self.df.iloc[0, :].str.contains("Add on").any():
                if self.df[0].str.contains('#', na=False).any():
                    end_index = list(self.df[self.df[0].str.contains('#', na=False)].index)[0]
                    prd = self.df.loc[end_index, 0].split(':')[-1].strip()
                arb_df = self.df.iloc[2:end_index, 1:]
                arb_df.columns = list(self.df.iloc[0, 1:3]) + list(self.df.iloc[1, -3:])
                arb_df.replace('#', prd, inplace=True)
                arb_df.replace('/', ';', regex=True, inplace=True)
                arb_df.rename(columns={"Place of receipt/delivery": "icd", "Via": "to", 20: "20GP", "40\'": "40GP", "40 HQ": "40HC"}, inplace=True)
                arb_df.replace('', nan, inplace=True)
                arb_df.dropna(how='all', axis=1, inplace=True)
                arb_df.dropna(how='all', axis=0, inplace=True)
                arb_df.reset_index(drop=True, inplace=True)
                arb_df = arb_df.loc[arb_df['icd'] != '']

                self.captured_output = {"Arbitrary": arb_df}
                return self.captured_output

            else:

                validity = self.get_validity()
                if self.df[7].str.contains("Validity:", na=False).any():
                    index_1 = list(self.df[(self.df[7].str.contains("Validity:", na=False))].index)
                elif self.df[6].str.contains("Validity:", na=False).any():
                    index_1 = list(self.df[(self.df[6].str.contains("Validity:", na=False))].index)

                if self.df[16].str.contains("INCLUSIVE", na=False).any():
                    index = list(self.df[(self.df[16].str.contains("INCLUSIVE", na=False))].index)
                    freight_df = pd.concat([self.df.iloc[index_1[0]:, 0:7 + 3], self.df.iloc[index[0] - 1:, 16:]],
                                           axis=1, ignore_index=True)
                elif self.df[10].str.contains("INCLUSIVE", na=False).any():
                    index = list(self.df[(self.df[10].str.contains("INCLUSIVE", na=False))].index)
                    freight_df = pd.concat([self.df.iloc[index_1[0]:, 0:7 + 3], self.df.iloc[index[0] - 1:, 10:]],
                                           axis=1, ignore_index=True)
                elif self.df[9].str.contains("INCLUSIVE", na=False).any():
                    index = list(self.df[(self.df[9].str.contains("INCLUSIVE", na=False))].index)
                    freight_df = pd.concat([self.df.iloc[index_1[0]:, 0:6 + 3], self.df.iloc[index[0] - 1:, 9:]],
                                           axis=1, ignore_index=True)

                freight_df.columns = freight_df.iloc[0]
                freight_df = freight_df[1:].copy()
                freight_df = self.get_freight_df(freight_df)
                freight_df.replace('Tariff', '', inplace=True)
                freight_df.replace('', nan, inplace=True)
                freight_df.dropna(how='all', axis=1, inplace=True)
                freight_df.dropna(how='all', axis=0, inplace=True)
                # if 'inclusions' in freight_df.columns:
                freight_df['inclusions'].replace(r'/', r';', regex=True, inplace=True)
                # if 'subject_to' in freight_df.columns:
                freight_df['subject_to'].replace(r'/', r';', regex=True, inplace=True)
                freight_df['charges_leg'] = 'L3'
                freight_df['currency'] = 'USD'
                freight_df['charges'] = 'Basic Ocean Freight'
                freight_df['expiry_date'] = validity['expiry_date']
                freight_df.reset_index(drop=True, inplace=True)
                # if 'destination_port' in freight_df.columns:
                freight_df = freight_df.loc[freight_df['destination_port'] != '']

                self.captured_output = {"Freight": freight_df}
                return self.captured_output

        def clean(self):

            if 'Freight' in self.captured_output:
                freight_df = self.captured_output['Freight']
                if 'NBF 20\n(October)' in freight_df.columns:
                    freight_df.drop(columns=['NBF 20\n(October)'], axis=1, inplace=True)
                if 'NBF 40\n(October)' in freight_df.columns:
                    freight_df.drop(columns=['NBF 40\n(October)'], axis=1, inplace=True)
                if 'NBF 20\n(Oct)' in freight_df.columns:
                    freight_df.drop(columns=['NBF 20\n(Oct)'], axis=1, inplace=True)
                if 'NBF 40\n(Oct)' in freight_df.columns:
                    freight_df.drop(columns=['NBF 40\n(Oct)'], axis=1, inplace=True)
                if 'NBF 20 DV\n(December)' in freight_df.columns:
                    freight_df.drop(columns=['NBF 20 DV\n(December)'], axis=1, inplace=True)
                if 'NBF 40 DV/HC (December)' in freight_df.columns:
                    freight_df.drop(columns=['NBF 40 DV/HC (December)'], axis=1, inplace=True)
                self.cleaned_output = {"Freight": freight_df}
            elif 'Arbitrary' in self.captured_output:
                self.cleaned_output = self.captured_output
            return self.cleaned_output

        def resolve_dependency(cls, fix_outputs):

            SG_FREIGHT = fix_outputs['SG']['Freight']
            SG_FREIGHT["unique"] = "SG"
            MY_FREIGHT = fix_outputs['MY']['Freight']
            MY_FREIGHT["unique"] = "MY"
            ID_FREIGHT = fix_outputs['ID']['Freight']
            ID_FREIGHT["unique"] = "ID"
            PH_FREIGHT = fix_outputs['PH']['Freight']
            PH_FREIGHT["unique"] = "PH"
            TH_FREIGHT = fix_outputs['TH']['Freight']
            TH_FREIGHT["unique"] = "TH"
            VN_FREIGHT = fix_outputs['VN']['Freight']
            VN_FREIGHT["unique"] = "VN"
            QIN_DAL_XNG_FREIGHT = fix_outputs['QIN+DAL+XNG']['Freight']
            QIN_DAL_XNG_FREIGHT["unique"] = "QIN+DAL+XNG"
            SNH_NGB_FREIGHT = fix_outputs['SNH+NGB']['Freight']
            SNH_NGB_FREIGHT["unique"] = "SNH+NGB"
            HK_FREIGHT = fix_outputs['HK']['Freight']
            HK_FREIGHT["unique"] = "HK"
            XIA_FREIGHT = fix_outputs['XIA']['Freight']
            XIA_FREIGHT["unique"] = "XIA"
            S_China_FREIGHT = fix_outputs['S.China']['Freight']
            S_China_FREIGHT["unique"] = "S.China"
            freight_df = pd.concat(
                [SG_FREIGHT, MY_FREIGHT, ID_FREIGHT, PH_FREIGHT, TH_FREIGHT, VN_FREIGHT, QIN_DAL_XNG_FREIGHT,
                 SNH_NGB_FREIGHT, HK_FREIGHT, XIA_FREIGHT, S_China_FREIGHT], ignore_index=True)
            arbitrary_df = pd.DataFrame()
            arbitrary_df = fix_outputs['POR&DEL add on update']['Arbitrary']
            arbitrary_df['at'] = 'origin'

            if not arbitrary_df.empty:
                fix_outputs = {"SG": {"Freight": freight_df, "Arbitrary Charges": arbitrary_df}}
            else:
                fix_outputs = {"SG": {"Freight": freight_df}}
            return fix_outputs