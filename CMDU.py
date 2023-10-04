from logging import getLogger

import pandas as pd
from numpy import nan
import re

from base import BaseTemplate, BaseFix
from custom_exceptions import InputValidationError

log = getLogger(__name__)


class CMA_V1(BaseTemplate):
    """Base Template for CMA"""

    class Cover(BaseFix):
        def __init__(self, df, file_model: dict, headers_config: dict):
            super().__init__(df, file_model, headers_config)

            self.validity = {}
            self.load_type_map = {}

        def check_input(self):
            pass

        def check_output(self):
            pass

        def clean(self):
            self.cleaned_output = self.captured_output

    class USWC(BaseFix):
        def __init__(self, df, file_model: dict, headers_config: dict):
            super().__init__(df, file_model, headers_config)

            self.validity = {}
            self.load_type_map = {}

        def check_input(self):
            check_errors = []
            if not self.df[0].str.contains('COMMODITY  INDEX', na=False).any():
                check_errors.append("COMMODITY INDEX should be present")

            if not self.df[0].str.contains('Port Group Codes', na=False).index.any():
                check_errors.append("Port Group Codes table should be present.")

            if not self.df[0].str.contains('RATES CONDITIONS', na=False).any():
                check_errors.append("Rate table should be present.")

            if check_errors:
                raise InputValidationError(check_errors)

        def check_output(self):
            pass

        def get_sections(self):

            df = self.df.T.reset_index(drop=True).T

            sections_to_check = ['COMMODITY  INDEX', 'Port Group Codes', 'RATES CONDITIONS', 'REEFER',
                                 'SPECIAL EQUIPMENT', 'ORIGIN ARBITRARY', 'DESTINATION ARBITRARY', 'Note 1']
            sections = {}

            previous_section = None

            for check in sections_to_check:
                if df[df[0].str.startswith(check, na=False)].index.values.any():
                    index = df[df[0].str.startswith(check, na=False)].index.values[0]
                    sections[check] = {'start': index, 'end': None}

                    if previous_section:
                        sections[previous_section]['end'] = index

                    previous_section = check
                else:
                    sections[check] = None
            return sections

        @classmethod
        def remove_empty_columns(cls, df):
            df = df.applymap(lambda x: nan if x == '' else x)
            df = df.dropna(axis=1, how="all")
            df = df.reset_index(drop=True)
            df = df.fillna('')
            return df

        def commodity_types(self):
            sections = self.get_sections()
            commodity_df = self.df[sections['COMMODITY  INDEX']['start']:sections['COMMODITY  INDEX']['end']]
            commodity_df = commodity_df[2:]
            commodity_df = commodity_df.rename(columns=commodity_df.iloc[0])
            commodity_df = commodity_df.reset_index()
            commodity_df = commodity_df.drop(0)
            commodity_df = commodity_df.drop('index', axis=1)
            commodity_df = self.remove_empty_columns(commodity_df)
            return commodity_df

        def port_codes(self):
            sections = self.get_sections()
            port_df = self.df[sections['Port Group Codes']['start']:sections['Port Group Codes']['end']]
            port_df = self.remove_empty_columns(port_df)
            port_df = port_df[1:]
            port_df[1].replace(',', ';', inplace=True, regex=True)
            return port_df

        def rate_table(self):
            sections = self.get_sections()
            port_df = self.port_codes()
            rate_df = self.df[sections['RATES CONDITIONS']['start']:sections['RATES CONDITIONS']['end']]
            rate_df = self.remove_empty_columns(rate_df)

            rate_df = rate_df[4:]
            rate_df = rate_df.rename(columns=rate_df.iloc[0])
            rate_df = rate_df.reset_index()
            rate_df = rate_df.drop(0)
            rate_df = rate_df.drop('index', axis=1)

            rate_df = rate_df.merge(port_df, how='left', left_on='POL', right_on=0)
            rate_df[1].fillna(rate_df.POL, inplace=True)
            rate_df.drop(rate_df.columns[[2, -2]], axis=1, inplace=True)
            rate_df = rate_df.rename(columns={1: 'POL'})
            rate_df = rate_df.merge(port_df, how='left', left_on='POD', right_on=0)
            rate_df[1].fillna(rate_df.POD, inplace=True)
            rate_df.drop(rate_df.columns[[2, -2]], axis=1, inplace=True)
            rate_df = rate_df.rename(columns={1: 'POD'})
            rate_df['Shipper own'] = 'COC'
            rate_df = rate_df[[column for column in rate_df.columns if column != '']]
            return rate_df

        def reefer_table(self):
            sections = self.get_sections()
            port_df = self.port_codes()
            reefer_df = self.df[sections['REEFER']['start']:sections['REEFER']['end']]
            reefer_df = reefer_df[1:]
            reefer_df = reefer_df.rename(columns=reefer_df.iloc[0])
            reefer_df = reefer_df[2:]
            reefer_df = reefer_df.reset_index()
            reefer_df = reefer_df[[column for column in reefer_df.columns if column != '']]
            reefer_df = reefer_df.drop('index', axis=1)
            reefer_df = reefer_df.merge(port_df, how='left', left_on='POL', right_on=0)
            reefer_df[1].fillna(reefer_df.POL, inplace=True)
            reefer_df.drop(columns=['POL', 0], axis=1, inplace=True)
            reefer_df = reefer_df.rename(columns={1: 'POL'})
            reefer_df = reefer_df.merge(port_df, how='left', left_on='POD', right_on=0)
            reefer_df[1].fillna(reefer_df.POD, inplace=True)
            reefer_df.drop(columns=['POD', 0], axis=1, inplace=True)
            reefer_df = reefer_df.rename(columns={1: 'POD'})
            # reefer_df['Shipper own'] = 'COC'
            # indexes_soc = reefer_df[reefer_df['Shipper own'] == 'SOC'].index.tolist()
            # reefer_df.loc[indexes_soc, '20RE_SOC'] = reefer_df.loc[indexes_soc]['RF20']
            # reefer_df.loc[indexes_soc, '40RE_SOC'] = reefer_df.loc[indexes_soc]['RF40']
            # reefer_df.loc[indexes_soc, '40HR_SOC'] = reefer_df.loc[indexes_soc]['RH40']
            return reefer_df

        def equipment_table(self):
            sections = self.get_sections()
            port_df = self.port_codes()
            port_df = port_df.rename(columns=port_df.iloc[0])
            port_df = port_df[1:]
            equipment_df = self.df[sections['SPECIAL EQUIPMENT']['start']:sections['SPECIAL EQUIPMENT']['end']]
            equipment_df = equipment_df.dropna(axis=1, how='all')
            equipment_df = equipment_df.dropna(how='all')
            equipment_df = equipment_df.reset_index()
            equipment_df = equipment_df.drop(0)
            equipment_df = equipment_df.drop('index', axis=1)
            equipment_df = equipment_df.rename(columns=equipment_df.iloc[0])
            equipment_df = equipment_df[2:]
            equipment_df = equipment_df.merge(port_df, how='left', left_on='POL', right_on='Port Group Codes')
            index = equipment_df[equipment_df['Description'].isna()].index.tolist()
            equipment_df.loc[index, 'Description'] = equipment_df.loc[index, 'POL']
            equipment_df.drop(columns=['POL', 'Port Group Codes'], axis=1, inplace=True)
            equipment_df = equipment_df.rename(columns={'Description': 'POL'})
            equipment_df = equipment_df.merge(port_df, how='left', left_on='POD', right_on='Port Group Codes')
            index = equipment_df[equipment_df['Description'].isna()].index.tolist()
            equipment_df.loc[index, 'Description'] = equipment_df.loc[index, 'POD']
            equipment_df.drop(columns=['POD', 'Port Group Codes'], axis=1, inplace=True)
            equipment_df = equipment_df.rename(columns={'Description': 'POD', 20: 'D20', 40: 'D40'})
            equipment_df = equipment_df.drop(columns=[column for column in equipment_df if column == ''])
            return equipment_df


        @staticmethod
        def format_output(df_freight, org_arb_df):
            output = {'Freight': df_freight, 'Arbitrary Charges': org_arb_df}
            return output

        def capture(self):
            self.commodity_df = self.commodity_types()
            arb_df = self.origin_arb()
            destination_arb = self.destination_arb()
            # arb_df = origin_arb.merge(destination_arb, how='outer')
            reefer_df = self.reefer_table()
            rate_df = self.rate_table()
            equipment_df = self.equipment_table()
            combined_df = pd.concat([rate_df, reefer_df, equipment_df])
            # combined_df = combined_df.merge(equipment_df, how='outer')
            self.port_codes = self.port_codes()
            self.captured_output = self.format_output(combined_df, arb_df)

        def clean_uswc(self):
            freight_df = self.captured_output['Freight']
            arb_df = self.captured_output['Arbitrary Charges']
            freight_df['charges'] = 'basic ocean freight'
            column_names = {'Place of Receipt': 'origin_icd', 'Place of Delivery': 'destination_icd',
                            'MODE': 'mode_of_transportation', 'Curr': 'currency',
                            'D20': '20GP', 'D40': '40GP', 'H40': '40HC', 'H45': '45HC', 'Effective date': 'start_date',
                            'Expiration Date': 'expiry_date', 'POL': 'origin_port', 'POD': 'destination_port',
                            'RF20': '20RE', 'RF40': '40RE', 'RH40': '40HR',
                            'Shipper own': 'container_owned'
                            }
            freight_df = freight_df.rename(
                columns=column_names)
            freight_df = freight_df[
                list(column_names.values()) + ['FAK/ BULLETS', 'IPI Construction', 'SDD (Origin; Dest)']]
            freight_df = freight_df.merge(self.commodity_df, how='left', left_on='FAK/ BULLETS', right_on='FAK/Bullets')
            freight_df = freight_df.rename(columns={'ALL': 'bulletin'})
            freight_df = freight_df.rename(columns={'Description': 'commodity'})
            freight_df = freight_df.drop('FAK/Bullets', axis=1)
            freight_df = freight_df.drop('FAK/ BULLETS', axis=1)
            freight_df.drop(freight_df.loc[freight_df['IPI Construction'] == ''].index.to_list(), axis=0, inplace=True)
            arb_allowed = freight_df['IPI Construction']
            arbatorigin = []
            arbatdest = []
            for i in arb_allowed:
                if i == 'E':
                    arbatorigin.append('Yes')
                    arbatdest.append('No')
                elif i == 'I':
                    arbatorigin.append('NO')
                    arbatdest.append('Yes')
                elif i == 'B':
                    arbatorigin.append('Yes')
                    arbatdest.append('Yes')
                elif i == 'X':
                    arbatorigin.append('No')
                    arbatdest.append('No')
            freight_df['origin_arbitrary_allowed'] = arbatorigin
            freight_df['destination_arbitrary_allowed'] = arbatdest
            mot = freight_df['mode_of_transportation'].str.split('/')
            mot_origin = []
            mot_dest = []
            mot_lookup = {'CY': '', 'R': 'RAIL', 'M': 'Motor', 'RM': 'RAIL/MOTOR', 'B': 'BARGE', 'RB': 'RAIL/BARGE',
                          'BM': 'MOTOR/BARGE'}
            for i in mot:
                for key in mot_lookup:
                    if i[0] == key:
                        mot_origin.append(mot_lookup[key])
                    if i[1] == key:
                        mot_dest.append(mot_lookup[key])
            freight_df['mode_of_transportation_origin'] = mot_origin
            freight_df['mode_of_transportation_destination'] = mot_dest
            freight_df['mode_of_transportation'] = ''
            # sdd = freight_df['SDD (Origin; Dest)'].str.split('/')
            # charge_updated = []
            # for i in sdd:
            #     if i[0] == 'Y' and i[1] == 'Y':
            #         charge_updated.append('basic ocean freight at pre-carriage and on-carriage')
            #     elif i[0] == 'Y':
            #         charge_updated.append('basic ocean freight charge at pre-carriage')
            #     elif i[1] == 'Y':
            #         charge_updated.append('basic ocean freight charge at on-carriage')
            #     else:
            #         charge_updated.append('basic ocean freight')
            #
            # freight_df.dropna(axis='columns')
            freight_df['charges'] = 'basic ocean freight'
            freight_df = freight_df.drop(columns=['SDD (Origin; Dest)', 'IPI Construction'])
            self.cleaned_output = {'Freight': freight_df}
            if arb_df.empty:
                return pd.DataFrame()
            column_names = {'Place of Receipt': 'icd', 'MODE': 'mode_of_transportation', 'Curr': 'currency',
                            'D20': '20GP', 'D40': '40GP', 'H40': '40HC', 'H45': '45HC',
                            'POL': 'to', 'Bullet Exceptions': 'FAK/Bullets', 'at': 'at', 'charge': 'charges',
                            'SDD (Origin)': 'SDD'}
            arb_df = arb_df.rename(columns=column_names)
            arb_df = arb_df[list(column_names.values())]
            arb_df = arb_df.merge(self.commodity_df, how='left', on='FAK/Bullets')
            arb_df = arb_df.rename(columns={'ALL': 'bulletin'})
            arb_df = arb_df.rename(columns={'Description': 'commodity'})
            arb_df = arb_df.drop('FAK/Bullets', axis=1)
            mot = arb_df['mode_of_transportation']
            mot_item = []
            for i in mot:
                if i == 'CY':
                    mot_item.append('FEEDER')
                elif i == 'R':
                    mot_item.append('RAIL=R (RAMP)')
                elif i == 'M':
                    mot_item.append('MOTOR')
                elif i == 'RM':
                    mot_item.append('RAIL/MOTOR')
                elif i == 'B':
                    mot_item.append('BARGE')
                elif i == 'BM':
                    mot_item.append('BARGE/MOTOR')
                elif i == 'RB':
                    mot_item.append(' BARGE/MOTOR')
            if mot_item:
                arb_df['mode_of_transportation'] = mot_item
            else:
                arb_df['mode_of_transportation'] = ""
            arb_df.dropna(axis='columns')
            """	
                SDD changes	
            """
            arb_df.loc[(arb_df['SDD'] == 'Y') & (
                    arb_df['at'] == 'destination'), 'charges'] = 'basic ocean freight charge at on-carriage'
            arb_df.loc[(arb_df['SDD'] == 'Y') & (
                    arb_df['at'] == 'origin'), 'charges'] = 'basic ocean freight charge at pre-carriage'
            arb_df.dropna(axis='columns')
            arb_df.drop('SDD', axis=1, inplace=True)

            return arb_df


class Flexport_CMA_v1(CMA_V1):
    """Flexport_CMA_v1 inherits from CMA_V1"""

    class Cover(CMA_V1.Cover):
        def capture(self):
            cover = self.df.dropna(axis=1, how='all')
            cover = cover.dropna(how='all')
            amd_index = list(cover[(cover[0].str.contains('Amendment #:'))].index)[0]
            start_date_index = list(cover[(cover[0].str.contains('Effective Date of Amendment:'))].index)[0]
            amendment_no = cover.iloc[amd_index][1]
            start_date = cover.iloc[start_date_index][1]
            end_date_index = list(cover[(cover[0].str.contains('Contract Expiration Date:'))].index)[0]
            end_date = cover.iloc[end_date_index][1]
            coverlist = [amendment_no, start_date, end_date]
            self.captured_output = coverlist

    class USWC(CMA_V1.USWC):
        def origin_arb(self):
            sections = self.get_sections()
            # commodity_df = self.commodity()
            originarb_df = self.df[sections['ORIGIN ARBITRARY']['start']:sections['ORIGIN ARBITRARY']['end']]
            originarb_df = self.remove_empty_columns(originarb_df)
            arb_df = originarb_df.iloc[7:]
            arb_df = arb_df.rename(columns=arb_df.iloc[0])
            arb_df = arb_df[1:]
            arb_df = arb_df.reset_index()
            if arb_df.empty:
                return pd.DataFrame()
            arb_df = arb_df.drop(0)
            arb_df = arb_df.drop('index', axis=1)
            arb_df['charge'] = 'origin arbitrary charges'
            arb_df.drop_duplicates()
            arb_df = arb_df[[column for column in arb_df.columns if column != '']]
            arb_df['at'] = 'origin'
            return arb_df

        def destination_arb(self):
            sections = self.get_sections()
            # commodity_df = self.commodity()
            destination_arb = self.df[
                              sections['DESTINATION ARBITRARY']['start']:sections['DESTINATION ARBITRARY']['end']]
            destination_arb = destination_arb[[column for column in destination_arb.columns if column != '']]
            arb_df = destination_arb.iloc[7:]
            arb_df = arb_df.rename(columns=arb_df.iloc[0])
            arb_df = arb_df[1:]
            if arb_df.empty:
                return arb_df
            arb_df = arb_df.reset_index()
            arb_df = arb_df.drop(0)
            arb_df = arb_df.drop('index', axis=1)
            arb_df['charge'] = 'destination arbitrary charges'
            arb_df.drop_duplicates()
            arb_df['at'] = 'destination'
            return arb_df

        def clean(self):
            arb_df = self.clean_uswc()
            self.cleaned_output['Arbitrary Charges'] = arb_df

    def resolve_dependency(cls, fix_outputs):
        df_USWC = fix_outputs.pop('APPENDIX B-1  (FE - USWC)')
        df_USEC = fix_outputs.pop('APPENDIX B-2  (FE - USEC&GC)')
        coverlist = fix_outputs.pop('Cover')
        df_ISC_USWC = fix_outputs.pop('APPENDIX F-2 ISC-USWC')
        for key in df_USWC:
            if key == 'Freight':
                enumerate_list = ['amendment_no', 'start_date', 'expiry_date', 'contract_id', 'contract_effective_date',
                                  'contract_expiration_date']
            else:
                enumerate_list = ['amendment_no', 'start_date', 'expiry_date']
            for index, column in enumerate(enumerate_list):
                if column not in df_USWC[key]:
                    df_USWC[key][column] = coverlist[index]
                else:
                    df_USWC[key][column] = df_USWC[key][column].apply(lambda x: coverlist[index] if x == '' else x)
                if column not in df_USEC[key]:
                    df_USEC[key][column] = coverlist[index]
                else:
                    df_USEC[key][column] = df_USEC[key][column].apply(lambda x: coverlist[index] if x == '' else x)
                if column not in df_ISC_USWC[key]:
                    df_ISC_USWC[key][column] = coverlist[index]
                else:
                    df_ISC_USWC[key][column] = df_ISC_USWC[key][column].apply(lambda x: coverlist[index] if x == ''
                    else x)
        fix_outputs = {'APPENDIX B-1  (FE - USWC)': df_USWC, 'APPENDIX B-2  (FE - USEC&GC)': df_USEC,
                       'APPENDIX F-2 ISC-USWC': df_ISC_USWC}
        fix_outputs = [df_USWC, df_USEC, df_ISC_USWC]
        return fix_outputs


class Expedoc_CMA_v1(CMA_V1):
    """Flexport_CMA_v1 inherits from CMA_V1"""

    class Cover(CMA_V1.Cover):
        def capture(self):
            cover = self.df.dropna(axis=1, how='all')
            cover = cover.dropna(how='all')
            amd_index = list(cover[(cover[0].str.contains('Amendment #:'))].index)[0]
            start_date_index = list(cover[(cover[0].str.contains('Effective Date of Amendment:'))].index)[0]
            contract_id_index = list(cover[(cover[0].str.contains('Service Contract #:'))].index)[0]
            contract_effective_date_index = list(cover[(cover[0].str.contains('Contract Effective Date:'))].index)[0]
            contract_expiration_date_index = list(cover[(cover[0].str.contains('Contract Expiration Date:'))].index)[0]
            amendment_no = cover.iloc[amd_index][1]
            start_date = cover.iloc[start_date_index][1]
            contract_id = cover.iloc[contract_id_index][1]
            contract_effective_date = cover.iloc[contract_effective_date_index][1]
            contract_expiration_date = cover.iloc[contract_expiration_date_index][1]
            end_date_index = list(cover[(cover[0].str.contains('Contract Expiration Date:'))].index)[0]
            end_date = cover.iloc[end_date_index][1]
            coverlist = [amendment_no, start_date, end_date, contract_id, contract_effective_date,
                         contract_expiration_date]
            self.captured_output = coverlist

    class USWC(CMA_V1.USWC):
        def origin_arb(self):
            sections = self.get_sections()
            # commodity_df = self.commodity()
            originarb_df = self.df[sections['ORIGIN ARBITRARY']['start']:sections['ORIGIN ARBITRARY']['end']]
            originarb_df = self.remove_empty_columns(originarb_df)
            arb_df = originarb_df.iloc[7:]
            arb_df = arb_df.rename(columns=arb_df.iloc[0])
            if (arb_df.columns[1] == arb_df.iloc[0, 1]) and (arb_df.shape[0] > 1):
                arb_df = arb_df[1:]
                arb_df = arb_df.reset_index()
                # arb_df = arb_df.drop(0)
                arb_df = arb_df.drop('index', axis=1)
                arb_df['charge'] = 'origin arbitrary charges'
                arb_df.drop_duplicates()
                arb_df = arb_df[[column for column in arb_df.columns if column != '']]
                arb_df['at'] = 'origin'
                return arb_df
            else:
                arb_df = arb_df.rename(columns=arb_df.iloc[0])
                arb_df = arb_df.reset_index()
                arb_df = arb_df.drop('index', axis=1)
                arb_df['charge'] = 'origin arbitrary charges'
                arb_df.drop_duplicates()
                arb_df = arb_df[[column for column in arb_df.columns if column != '']]
                arb_df['at'] = 'origin'
                return arb_df

        def destination_arb(self):
            sections = self.get_sections()
            # commodity_df = self.commodity()
            destination_arb = self.df[
                              sections['DESTINATION ARBITRARY']['start']:sections['DESTINATION ARBITRARY']['end']]
            destination_arb = destination_arb[[column for column in destination_arb.columns if column != '']]
            arb_df = destination_arb.iloc[7:]
            arb_df = arb_df.rename(columns=arb_df.iloc[0])
            arb_df = arb_df[1:]
            if arb_df.empty:
                return arb_df
            arb_df = arb_df.reset_index()
            # arb_df = arb_df.drop(0)
            arb_df = arb_df.drop('index', axis=1)
            arb_df['charge'] = 'destination arbitrary charges'
            arb_df.drop_duplicates()
            arb_df['at'] = 'destination'
            return arb_df

        def clean(self):
            arb_df = self.clean_uswc()
            check_string = arb_df.iloc[0, 0]
            check_list = list(check_string.split())
            if check_list[0][1:].islower():
                arb_df = arb_df.drop(axis=0, index=0)
            self.cleaned_output['Arbitrary Charges'] = arb_df

        def resolve_dependency(cls, fix_outputs):
            df_USWC = fix_outputs.pop('APPENDIX B-1  (FE - USWC)')
            df_USEC = fix_outputs.pop('APPENDIX B-2  (FE - USEC&GC)')
            coverlist = fix_outputs.pop('Cover')
            for key in df_USWC:
                for index, column in enumerate(['amendment_no', 'start_date', 'expiry_date']):
                    if column not in df_USWC[key]:
                        df_USWC[key][column] = coverlist[index]
                    else:
                        df_USWC[key][column] = df_USWC[key][column].apply(lambda x: coverlist[index] if x == '' else x)
                    if column not in df_USEC[key]:
                        df_USEC[key][column] = coverlist[index]
                    else:
                        df_USEC[key][column] = df_USEC[key][column].apply(lambda x: coverlist[index] if x == '' else x)
            fix_outputs = {'APPENDIX B-1  (FE - USWC)': df_USWC, 'APPENDIX B-2  (FE - USEC&GC)': df_USEC}
            fix_outputs = [df_USWC, df_USEC]
            return fix_outputs

class CEVA_FAK_CNC (BaseTemplate):

    class Cnc_Fak(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):

            if self.df[1].str.contains('DG surcharge').any():
                dgsurcharge = list(self.df[(self.df[1].str.contains('DG surcharge'))].index)[0]
                dgs = self.df.iloc[dgsurcharge, 1]
                dgs_list = self.df.iloc[dgsurcharge, 1].split(':')[1:][0].split('.')[0].split(';')
                dgs_dict = {}
                for element in dgs_list:
                    dgs_dict[element.split('on')[-1]] = element.split('on')[0]

            def get_inclusions(date_str):
                return re.search(r"Rates include (.+?)$", date_str)

            captured_data = self.df.iloc[:, 1].apply(lambda x: get_inclusions(str(x)))

            for i in captured_data:
                if i:
                    inclusions = i.group(1)

            def get_subject_to(date_str):
                return re.search(r"Subject to (.+?)$", date_str)

            captured_data_1 = self.df.iloc[:, 1].apply(lambda x: get_subject_to(str(x)))

            for i in captured_data_1:
                if i:
                    subject_to = i.group(1)

            start_date = self.df.iloc[0, 7]
            end_date = self.df.iloc[0, 9]
            index = list(self.df[(self.df[3].str.contains("POO", na=False))].index)
            freight_df = self.df[index[0]:self.df.tail(1).index.values[0] + 1].copy(deep=True)
            freight_df.columns = freight_df.iloc[0]
            freight_df = freight_df[1:].copy()
            freight_df['inclusions'] = inclusions
            freight_df['subject_to'] = subject_to
            freight_df['start_date'] = start_date.date()
            freight_df['end_date'] = end_date.date()
            freight_df.loc[
                (freight_df['B/L'] == 'CNC') & (freight_df['Direct or \nTranship'] == 'Direct'), 'DG Surcharge'] = \
            dgs_dict[' CNC direct services']
            freight_df.loc[
                (freight_df['B/L'] == 'CNC') & (freight_df['Direct or \nTranship'] == 'Tranship'), 'DG Surcharge'] = \
            dgs_dict[' CNC indirect services']
            freight_df.loc[(freight_df['B/L'] == 'CMA'), 'DG Surcharge'] = dgs_dict[' CMA services']

            freight_df.loc[freight_df['Country of POD'].str.contains('Bangladesh', na = False, case= False ),'inclusions'] = inclusions + '/' + 'DTHC '
            freight_df['40GP'] = freight_df['40ST&HC']
            freight_df['40HC'] = freight_df['40ST&HC']
            freight_df.drop(['40ST&HC','Date of Revision','POO','POO CODE','FPD','FPD CODE','Mon','Tue','Wed','Thu','Fri','Sat','Sun'], axis = 1, inplace = True)
            freight_df.rename(columns = {'POL':'origin_port','Country of POL':'origin_country','POD':'destination_port','Country of POD':'destination_country','end_date':'expiry_date'}, inplace = True)


            self.captured_output = {"Freight": freight_df}

        def clean(self):

            self.cleaned_output = self.captured_output

    def resolve_dependency(cls, fix_outputs):

        if "CNC FAK" in fix_outputs:
            df_CNC_FAK_dict = fix_outputs.pop('CNC FAK')
            df_CNC_FAK  = df_CNC_FAK_dict["Freight"]

        if "Ex Japan Q1 FAK" in fix_outputs:

            df_Ex_Japan_dict = fix_outputs.pop('Ex Japan Q1 FAK')
            df_Ex_Japan = df_Ex_Japan_dict["Freight"]

        freight_df = pd.concat([df_CNC_FAK, df_Ex_Japan], ignore_index= True)
        fix_outputs =[{"Freight":freight_df}]
        return fix_outputs