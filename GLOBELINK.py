from logging import getLogger

import pandas as pd
from numpy import nan

from base import BaseTemplate, BaseFix
from custom_exceptions import InputValidationError

log = getLogger(__name__)
"""
file_path="https://docs.google.com/spreadsheets/d/1NusqorUWyoIeJyD-YPNMAu4O1iOP2_if/edit?usp=sharing&ouid=105952673786886454447&rtpof=true&sd=true"
"""

class Ceva_Globelink_Emea(BaseTemplate):
    class _submission(BaseFix):

        def check_input(self):
            check_errors = []
            if not self.df[2].str.contains('Origin Country', na=False).any():
                check_errors.append("Origin Country should be present")

            if not self.df[12].str.contains('Destination Country', na=False).index.any():
                check_errors.append("Destination Country should be present.")

            if not self.df[9].str.contains('Port of Loading', na=False).any():
                check_errors.append("Port of Loading should be present.")

            if not self.df[15].str.contains('Port of Discharge', na=False).any():
                check_errors.append("Port of Discharge should be present.")

            if check_errors:
                raise InputValidationError(check_errors)

        def fill_empty_columns_with_previous_column(self, column_header):
            for index, value in enumerate(column_header):
                if index == len(column_header):
                    break
                else:
                    if column_header[index] == '' and not index == 0:
                        column_header[index] = column_header[index-1]
            return column_header

        def append_top_header_with_column_header(self, column_header, column_header_1):
            for index,value in enumerate(column_header):
                if 'Origin Charges' in value:
                    column_header_1[index] = column_header_1[index]+'_origin_charges'
                elif 'Ocean Freight' in value:
                    column_header_1[index] = column_header_1[index]+'_ocean_freight'
                elif 'Destination Charges' in value:
                    column_header_1[index] = column_header_1[index]+'_destination_charges'
            return column_header_1

        def origin_surcharges_column_list(self, input_freight_df):
            origin_surcharges_column_list = [x for x in input_freight_df.columns if
                                             'origin_charges' in x and x not in ['Currency_origin_charges',
                                                                                 'W/M_origin_charges',
                                                                                 'Minimum_origin_charges']]
            return origin_surcharges_column_list

        def ocean_freight_surcharges_column_list(self, input_freight_df):
            ocean_freight_surcharges_column_list = [x for x in input_freight_df.columns if
                                                    'ocean_freight' in x and x not in ['Currency_ocean_freight',
                                                                                       'W/M_ocean_freight',
                                                                                       'Minimum_ocean_freight']]
            return ocean_freight_surcharges_column_list

        def destination_surcharges_column_list(self, input_freight_df):
            destination_surcharges_column_list = [x for x in input_freight_df.columns if
                                                  'destination_charges' in x and x not in [
                                                      'Currency_destination_charges',
                                                      'W/M_destination_charges',
                                                      'Minimum_destination_charges']]
            return destination_surcharges_column_list

        def origin_charges(self, origin_df, input_freight_df):
            origin_df['charges_leg'] = 'L2'
            origin_df['charges'] = 'cfs_charges_origin'
            origin_df['amount'] = input_freight_df['W/M_origin_charges']
            origin_df['basis'] = 'w/m'
            origin_df['amount_min'] = input_freight_df['Minimum_origin_charges']
            origin_df['currency'] = input_freight_df['Currency_origin_charges']
            return origin_df

        def origin_surcharges(self, origin_surcharges_column_list, origin_surcharges_temp_df, input_freight_df, origin_surcharges_df):
            for i in origin_surcharges_column_list:
                origin_surcharges_temp_df['charges_leg'] = 'L2'
                origin_surcharges_temp_df['charges'] = 'org_doc_charges'
                origin_surcharges_temp_df['amount'] = input_freight_df[i]
                origin_surcharges_temp_df['basis'] = 'Lumpsum'
                origin_surcharges_temp_df['currency'] = input_freight_df['Currency_origin_charges']
                origin_surcharges_df = pd.concat(
                    [origin_surcharges_df, origin_surcharges_temp_df], axis=0)
            return origin_surcharges_df

        def ocean_freight_charges(self, ocean_df, input_freight_df):
            ocean_df['charges_leg'] = 'L3'
            ocean_df['charges'] = 'basic_ocean_freight'
            ocean_df['amount'] = input_freight_df['W/M_ocean_freight']
            ocean_df['basis'] = 'w/m'
            ocean_df['amount_min'] = input_freight_df['Minimum_ocean_freight']
            ocean_df['currency'] = input_freight_df['Currency_ocean_freight']
            return ocean_df

        def ocean_freight_surcharges(self, ocean_freight_surcharges_column_list, ocean_freight_surcharges_temp_df, input_freight_df,
                              ocean_freight_surcharges_df):
            for i in ocean_freight_surcharges_column_list:
                ocean_freight_surcharges_temp_df['charges_leg'] = 'L3'
                ocean_freight_surcharges_temp_df['charges'] = str(i).replace("_ocean_freight", "").lower()
                ocean_freight_surcharges_temp_df['amount'] = input_freight_df[i]
                ocean_freight_surcharges_temp_df['basis'] = 'w/m'
                ocean_freight_surcharges_temp_df['currency'] = input_freight_df['Currency_ocean_freight']
                ocean_freight_surcharges_df = pd.concat(
                    [ocean_freight_surcharges_df, ocean_freight_surcharges_temp_df], axis=0)
            return ocean_freight_surcharges_df

        def destination_charges(self, destination_df, input_freight_df):
            destination_df['charges_leg'] = 'L4'
            destination_df['charges'] = 'cfs_charges_dest'
            destination_df['amount'] = input_freight_df['W/M_destination_charges']
            destination_df['basis'] = 'w/m'
            destination_df['amount_min'] = input_freight_df['Minimum_destination_charges']
            destination_df['currency'] = input_freight_df['Currency_destination_charges']
            return destination_df

        def destination_surcharges(self, destination_surcharges_column_list, destination_surcharges_temp_df, input_freight_df,
                              destination_surcharges_df):
            for i in destination_surcharges_column_list:
                destination_surcharges_temp_df['charges_leg'] = 'L4'
                destination_surcharges_temp_df['charges'] = 'dest_doc_charges'
                destination_surcharges_temp_df['amount'] = input_freight_df[i]
                destination_surcharges_temp_df['basis'] = 'Lumpsum'
                destination_surcharges_temp_df['currency'] = input_freight_df['Currency_destination_charges']
                destination_surcharges_df = pd.concat(
                    [destination_surcharges_df, destination_surcharges_temp_df], axis=0)
            return destination_surcharges_df

        def data_preprocessing(self, freight_df):
            freight_df.columns = list(freight_df.iloc[2, :])
            freight_df.drop(columns=['1 Oct 2021 PSS WEF Vsl SOB POL 1 Oct', '1 Nov 2021 PSS WEF Vsl SOB POL 1 Nov'],
                            axis=1, inplace=True)
            freight_df.rename(columns={'1 Dec 2021 PSS WEF Vsl SOB POL 1 Dec': 'PSS'}, inplace=True)
            input_freight_df = freight_df.copy()
            column_header = list(freight_df.iloc[1, :])
            column_header_1 = list(freight_df.columns)
            column_header = self.fill_empty_columns_with_previous_column(column_header)
            freight_df = freight_df.iloc[2:, 1:]
            freight_df.reset_index(drop=True, inplace=True)
            column_header = self.append_top_header_with_column_header(column_header, column_header_1)
            input_freight_df.columns = column_header
            input_freight_df = input_freight_df.iloc[3:, 1:]
            input_freight_df.reset_index(drop=True, inplace=True)
            freight_df = freight_df.iloc[1:, :]
            freight_df.rename(
                columns={'Origin CFS': 'origin_icd_name',
                         'Origin CFS Code': 'origin_icd_code',
                         'Port of Loading': 'origin_port_name',
                         'POL Code': 'origin_port_code',
                         'Origin Country': 'origin_country',
                         'Destination CFS': 'destination_icd_name',
                         'Destination CFS CODE': 'destination_icd_code',
                         'Port of Discharge': 'destination_port_name',
                         'POD Code': 'destination_port_code',
                         'Destination Country': 'destination_country',
                         'Valid From': 'start_date',
                         'Valid To': 'expiry_date',
                         'Port to Port Routing': 'via_port_1',
                         'Port to Port T/T': 'transit_time',
                         '1 Dec 2021 PSS WEF Vsl SOB POL 1 Dec': 'PSS'
                         }, inplace=True)
            freight_df = freight_df[['origin_icd_name','origin_icd_code','origin_port_name', 'origin_port_code', 'origin_country',
                                     'destination_icd_name','destination_icd_code','destination_port_name','destination_port_code', 'destination_country',
                                     'via_port_1', 'transit_time','start_date', 'expiry_date']]
            freight_df['start_date'] = pd.to_datetime(freight_df['start_date'], errors='coerce').dt.date
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date'], errors='coerce').dt.date
            freight_df[['Uniq', 'via_port_2', 'service_type', 'cargo_type', 'commodity', 'mode_of_transportation',
                        'inclusions', 'cfs_stuffing', 'remarks', 'charges_leg', 'charges', 'basis', 'amount',
                        'amount_min', 'amount_max',
                        'DIM', 'currency','mode_of_transportation_origin','mode_of_transportation_destination']] = ''
            freight_df['origin_country'] = freight_df['origin_port_code'].str[:2]
            freight_df['destination_country'] = freight_df['destination_port_code'].str[:2]
            freight_df['cargo_type'] = 'FAK'

            freight_df['mode_of_transportation_origin'] = [
                (x + ';' + y) if x != '' and y != '' else x if x != '' else y if y != '' else '' for x, y in
                zip(input_freight_df['CFS to origin Gateway mode (truck, rail, vessel, etc)'],
                    input_freight_df['Gateway to port of loading mode (truck, rail, vessel,  etc)'])]
            freight_df['mode_of_transportation_destination'] = [
                (x + ';' + y) if x != '' and y != '' else x if x != '' else y if y != '' else '' for x, y in
                zip(input_freight_df['Port of Discharge to Gateway mode (truck, rail, vessel,  etc)'],
                    input_freight_df['Destination Gateway to CFS mode (truck, rail, vessel, etc)'])]
            freight_df.reset_index(drop=True, inplace=True)

            return freight_df, input_freight_df

        def origin_charges_all(self, origin_df, input_freight_df, origin_surcharges_temp_df, origin_surcharges_df):
            origin_df = self.origin_charges(origin_df, input_freight_df)
            origin_surcharges_column_list = self.origin_surcharges_column_list(input_freight_df)
            origin_surcharges_df = self.origin_surcharges(origin_surcharges_column_list, origin_surcharges_temp_df,
                                                              input_freight_df, origin_surcharges_df)
            return origin_df, origin_surcharges_df

        def ocean_freight_charges_all(self, ocean_df, input_freight_df, ocean_freight_surcharges_temp_df, ocean_freight_surcharges_df):
            ocean_df = self.ocean_freight_charges(ocean_df, input_freight_df)
            ocean_freight_surcharges_column_list = self.ocean_freight_surcharges_column_list(input_freight_df)
            ocean_freight_surcharges_df = self.ocean_freight_surcharges(ocean_freight_surcharges_column_list,
                                                                        ocean_freight_surcharges_temp_df,
                                                                        input_freight_df, ocean_freight_surcharges_df)
            return ocean_df, ocean_freight_surcharges_df

        def destination_charges_all(self, destination_df, input_freight_df, destination_surcharges_temp_df, destination_surcharges_df):
            destination_df = self.destination_charges(destination_df, input_freight_df)
            destination_surcharges_column_list = self.destination_surcharges_column_list(input_freight_df)
            destination_surcharges_df = self.destination_surcharges(destination_surcharges_column_list,
                                                                            destination_surcharges_temp_df,
                                                                            input_freight_df,
                                                                            destination_surcharges_df)
            return destination_df, destination_surcharges_df


        def capture(self):
            freight_df = self.df.copy()
            freight_df, input_freight_df = self.data_preprocessing(freight_df)

            origin_df = freight_df.copy()
            origin_surcharges_temp_df = freight_df.copy()
            origin_surcharges_df = pd.DataFrame(columns=freight_df.columns)
            ocean_df = freight_df.copy()
            ocean_freight_surcharges_temp_df = freight_df.copy()
            ocean_freight_surcharges_df = pd.DataFrame(columns=freight_df.columns)
            destination_df = freight_df.copy()
            destination_surcharges_temp_df = freight_df.copy()
            destination_surcharges_df = pd.DataFrame(columns=freight_df.columns)

            origin_df, origin_surcharges_df = self.origin_charges_all(origin_df, input_freight_df, origin_surcharges_temp_df, origin_surcharges_df)
            ocean_df, ocean_freight_surcharges_df = self.ocean_freight_charges_all(ocean_df, input_freight_df, ocean_freight_surcharges_temp_df, ocean_freight_surcharges_df)
            destination_df, destination_surcharges_df = self.destination_charges_all(destination_df, input_freight_df, destination_surcharges_temp_df, destination_surcharges_df)
            final_df = pd.concat([origin_df, origin_surcharges_df, ocean_df, ocean_freight_surcharges_df, destination_df, destination_surcharges_df],axis=0)
            final_df = final_df[final_df.amount != 'NQ']

            final_df.reset_index(drop=True, inplace=True)
            self.captured_output = final_df

        def check_output(self):
            pass

        def clean(self):
            self.cleaned_output = {'Freight': self.captured_output}

