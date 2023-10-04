import datetime
import re
from itertools import product
from typing import List

import pandas as pd
from numpy import nan
from pandas import concat
from logging import getLogger
from collections import defaultdict
from base import BaseTemplate, BaseFix
from custom_exceptions import InputValidationError
from dateutil.parser import parse


log = getLogger(__name__)
log = getLogger(__name__)

"""  
template id = "626779b38d7dfbcbfb3644d4"
file details = https://drive.google.com/file/d/1j9O2duoQ1o5tP2022k2y02AMSze4maDt/view?usp=sharing
"""


class Ceva_Vanguard_Lcl(BaseTemplate):

    class ceva_vanguard_rates(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def fill_empty_columns_with_previous_column(self,col_header):  # col header is Data Frame
            for index, value in enumerate(col_header):
                if index == len(col_header):  # 0=50
                    break
                else:
                    if col_header[index] == '' and not index == 0: # 0 == "" and  0
                        col_header[index] = col_header[index - 1]
            return col_header

        def append_top_header_with_column_header(self,col_header,col_header_1):
            for index,value in enumerate(col_header):
                if 'Origin Charges' in value:
                    col_header_1[index] = col_header_1[index] + '_Origin_Charges'
                elif 'Oceanfreight' in value:
                    col_header_1[index] = col_header_1[index] + '_Ocean_Freight'
                elif "IPI (US Only)" in value:
                    col_header_1[index] = col_header_1[index]+"_IPI_(US_Only)"
                elif 'Destination charges' in value:
                    col_header_1[index] = col_header_1[index] + '_Destination_Charges'
            return col_header_1

        """Captureing the origin freight charges from input file"""

        def origin_charges(self, origin_df, input_freight_df):
            origin_df['charges_leg'] = 'L2'
            origin_df['charges'] = 'cfs_charges_origin'
            origin_df['amount'] = input_freight_df['CFS charges\nw/m_Origin_Charges']
            origin_df['basis'] = 'w/m'
            origin_df['amount_min'] = input_freight_df['CFS charges minimum_Origin_Charges']
            origin_df['currency'] = input_freight_df['Currency_Origin_Charges']
            origin_df["Uniq"] = "Origin Charges"

            return origin_df

        def origin_surcharges(self, origin_surcharges_column_list, origin_surcharges_temp_df, input_freight_df,
                              origin_surcharges_df):
            for i in origin_surcharges_column_list:
                origin_surcharges_temp_df['charges_leg'] = 'L2'
                if i == "BL Fee\nLumpsum_Origin_Charges":
                    origin_surcharges_temp_df['charges'] = 'org_doc_charges'
                    origin_surcharges_temp_df['basis'] = 'Lumpsum'
                elif i == 'ISPS Fee (Flat)_Origin_Charges':
                    origin_surcharges_temp_df['charges'] = 'ISPS charges'
                    origin_surcharges_temp_df['basis'] = 'lumpsum'
                elif i == 'Advance Filing Fee (AFR/ENS/Waive Fee)_Origin_Charges':
                    origin_surcharges_temp_df['charges'] = 'Advance Filing Fee'
                    origin_surcharges_temp_df['basis'] = 'w/m'
                elif i =='Solas fee_Origin_Charges':
                    origin_surcharges_temp_df['charges'] = 'solas Fee'
                    origin_surcharges_temp_df['basis'] = 'w/m'
                else:
                    pass
                origin_surcharges_temp_df['currency'] = input_freight_df['Currency_Origin_Charges']
                origin_surcharges_temp_df['amount'] = input_freight_df[i]
                origin_surcharges_temp_df["Uniq"] = "Origin Charges"

                origin_surcharges_df = pd.concat([origin_surcharges_df, origin_surcharges_temp_df], axis=0 )
            return origin_surcharges_df

        def origin_surcharges_column_list(self, input_freight_df):
            origin_surcharges_column_list = [x for x in input_freight_df.columns if
                                             'Origin_Charges' in x and x not in ['Currency_Origin_Charges',
                                                                                 'CFS charges\nw/m_Origin_Charges',
                                                                                 'CFS charges minimum_Origin_Charges']]
            return origin_surcharges_column_list

        def origin_charges_all(self, origin_df, input_freight_df, origin_surcharges_temp_df, origin_surcharges_df):
            origin_df = self.origin_charges(origin_df, input_freight_df)
            origin_surcharges_column_list = self.origin_surcharges_column_list(input_freight_df)
            origin_surcharges_df = self.origin_surcharges(origin_surcharges_column_list,origin_surcharges_temp_df,input_freight_df, origin_surcharges_df)

            return origin_df, origin_surcharges_df

        """Captureing the ocean freight charges from input file"""
        def ocean_surcharges_column_list(self, input_freight_df):
            ocean_surcharges_column_list = [x for x in input_freight_df.columns if
                                             'Ocean_Freight' in x and x not in ['Currency_Ocean_Freight',
                                                                                 'Ocean Freight (per CBM)_Ocean_Freight',
                                                                                 'Ocean Freight Minimum_Ocean_Freight',
                                                                                 'Ratio (CBM/KILOS)_Ocean_Freight']]
            return ocean_surcharges_column_list

        def ocean_charges(self,  ocean_df, input_freight_df):
            ocean_df['charges_leg'] = 'L3'
            ocean_df['charges'] = 'basic_ocean_freight'
            ocean_df['amount'] = input_freight_df['Ocean Freight (per CBM)_Ocean_Freight']
            ocean_df['basis'] = 'CBM'
            ocean_df['amount_min'] = input_freight_df['Ocean Freight Minimum_Ocean_Freight']
            ocean_df['currency'] = input_freight_df['Currency_Ocean_Freight']
            ocean_df['DIM'] = input_freight_df["Ratio (CBM/KILOS)_Ocean_Freight"]
            ocean_df["Uniq"] = "Ocean Charges"

            return ocean_df

        def ocean_surcharges(self, ocean_df,ocean_surcharges_column_list, ocean_surcharges_temp_df, input_freight_df,ocean_surcharges_df):
            for i in ocean_surcharges_column_list:
                ocean_surcharges_temp_df['charges_leg'] = 'L3'
                ocean_surcharges_temp_df['amount'] = input_freight_df[i]
                ocean_surcharges_temp_df["DIM"] = input_freight_df["Ratio (CBM/KILOS)_Ocean_Freight"]

                if i == 'Ocean Freight\n(per Ton)_Ocean_Freight':
                    ocean_surcharges_temp_df['charges'] = 'basic_ocean_freight'
                    ocean_surcharges_temp_df['basis'] = 'per ton'
                    ocean_surcharges_temp_df['amount_min'] = input_freight_df['Ocean Freight Minimum_Ocean_Freight']

                elif i == "PSS (USD)_Ocean_Freight":
                    ocean_surcharges_temp_df['charges'] = 'PSS'
                    ocean_surcharges_temp_df['basis'] = 'Lumpsum'

                elif i == "GRI (USD)_Ocean_Freight":
                    ocean_surcharges_temp_df['charges'] = 'GRI'
                    ocean_surcharges_temp_df['basis'] = 'Lumpsum'

                elif i =='IMO 2020 Surcharges   (USD)_Ocean_Freight':
                    ocean_surcharges_temp_df['charges'] = 'IMO '
                    ocean_surcharges_temp_df['basis'] = 'Lumpsum'
                else:
                    pass
                ocean_surcharges_temp_df['currency'] = input_freight_df['Currency_Ocean_Freight']
                ocean_surcharges_temp_df["Uniq"] = "Ocean Charges"

                ocean_surcharges_df = pd.concat([ocean_surcharges_df, ocean_surcharges_temp_df], axis=0 )
            return ocean_surcharges_df

        def ocean_charges_all(self, ocean_df, input_freight_df, ocean_surcharges_temp_df, ocean_surcharges_df):
            ocean_df = self.ocean_charges( ocean_df, input_freight_df)
            ocean_surcharges_column_list = self.ocean_surcharges_column_list( input_freight_df )
            ocean_surcharges_df = self.ocean_surcharges( ocean_df,ocean_surcharges_column_list, ocean_surcharges_temp_df,
                                                           input_freight_df,ocean_surcharges_df)
            return ocean_df, ocean_surcharges_df

        """Captureing the IPI_freight charges from input_file"""

        def ipi_surcharges_column_list(self, input_freight_df):
            ipi_surcharges_column_list = [x for x in input_freight_df.columns if
                                            'IPI_(US_Only)' in x and x not in ['Currency_IPI_(US_Only)',"Ratio_IPI_(US_Only)",'IPI Rate_IPI_(US_Only)',"IPI Minimum_IPI_(US_Only)"]]
            return ipi_surcharges_column_list

        def ipi_surcharges(self,ipi_df , ipi_surcharges_column_list, input_freight_df):
            for i in ipi_surcharges_column_list:
                ipi_df['charges_leg'] = 'L3'
                ipi_df['charges'] = 'IPI Rate'
                ipi_df['amount'] = input_freight_df['IPI Rate_IPI_(US_Only)']
                ipi_df['basis'] = 'w/m'
                ipi_df['amount_min'] = input_freight_df["IPI Minimum_IPI_(US_Only)"]
                ipi_df["Ratio"] = input_freight_df["Ratio_IPI_(US_Only)"]


                ipi_df['currency'] = input_freight_df['Currency_IPI_(US_Only)']

                return ipi_df

        def ipi_charges_all(self, ipi_df, ipi_surcharges_df, input_freight_df, ipi_surcharges_column_list):
            ipi_surcharges_column_list = self.ipi_surcharges_column_list( input_freight_df )
            ipi_df = self.ipi_surcharges(ipi_df , ipi_surcharges_column_list,input_freight_df )
            return ipi_df

        """Captureing the destination freight charges from  inputfile"""

        def destination_charges(self, destination_df, input_freight_df):
            destination_df['charges_leg'] = 'L4'
            destination_df['charges'] = 'cfs_charges_destination'
            destination_df['amount'] = input_freight_df['CFS charges w/m_Destination_Charges']
            destination_df['basis'] = 'w/m'
            destination_df['amount_min'] = input_freight_df['CFS charges minimum_Destination_Charges']
            destination_df['currency'] = input_freight_df['Currency_Destination_Charges']
            destination_df["Uniq"] = "Destination Charges"

            return destination_df

        def destination_surcharges(self, destination_surcharges_column_list, destination_surcharges_temp_df, input_freight_df,
                              destination_surcharges_df):
            for i in destination_surcharges_column_list:
                destination_surcharges_temp_df['charges_leg'] = 'L4'
                if i == "BL Fee_Destination_Charges":
                    destination_surcharges_temp_df['charges'] = "destination_doc_charges"
                    destination_surcharges_temp_df['basis'] = 'lumpsum'
                elif i == "ISPS Fee (Flat)_Destination_Charges":
                    destination_surcharges_temp_df['charges'] = 'destination_ISPS charges'
                    destination_surcharges_temp_df['basis'] = 'lumpsum'
                else:
                    pass
                destination_surcharges_temp_df['currency'] = input_freight_df['Currency_Destination_Charges']
                destination_surcharges_temp_df['amount'] = input_freight_df[i]
                destination_surcharges_temp_df["Uniq"] = "Destination Charges"


                destination_surcharges_df = pd.concat( [destination_surcharges_df, destination_surcharges_temp_df], axis=0 )
            return destination_surcharges_df

        def destination_surcharges_column_list(self, input_freight_df):
            destination_surcharges_column_list = [x for x in input_freight_df.columns if 'Destination_Charges' in x and x not in
                                                  ['CFS charges w/m_Destination_Charges','CFS charges minimum_Destination_Charges', 'Currency_Destination_Charges']]
            return destination_surcharges_column_list

        def destination_charges_all(self, destination_df, input_freight_df, destination_surcharges_temp_df, destination_surcharges_df):
            destination_df = self.destination_charges( destination_df, input_freight_df )
            destination_surcharges_column_list = self.destination_surcharges_column_list( input_freight_df )
            destination_surcharges_df = self.destination_surcharges( destination_surcharges_column_list, destination_surcharges_temp_df,
                                                           input_freight_df, destination_surcharges_df )

            return destination_df, destination_surcharges_df

        def data_preprocessing(self, freight_df):
            freight_df.columns = list(freight_df.iloc[1, :])
            #input_freight_df = freight_df.copy(deep=True)

            freight_df = freight_df.drop(columns=["Lane ID"], axis=1)
            input_freight_df = freight_df.copy()
            col_header = list(input_freight_df.iloc[0, :]) # data frame
            col_header_1 = list(input_freight_df.columns) # list of columns
            col_header = self.fill_empty_columns_with_previous_column(col_header)
            column_header = self.append_top_header_with_column_header(col_header, col_header_1)
            freight_df = freight_df.iloc[1:,:]
            freight_df.reset_index(drop=True, inplace=True)
            #freight_df.columns = freight_df.iloc[0]
            freight_df = freight_df.iloc[1:, :]
            freight_df.reset_index(drop=True, inplace=True)

            input_freight_df.columns = column_header
            input_freight_df = input_freight_df.iloc[2:, :]
            input_freight_df.reset_index(drop=True, inplace=True)

            freight_df.rename(columns = {"Port of Loading": "origin_port_name", "Port of Loading UNCODE": "origin_port_code", "Origin CFS": "origin_icd",
                                         "Origin CFS Code":"origin_icd_code"
                                               ,"Transhipment Port 1":"via_port_1" , "Transhipment Pot 2":"via_port_2","Port of Discharge":"destination_port_name"
                                         ,"Destination CFS":"destination_icd","Destination CFS Code":"destination_icd_code","Port of Discharge UNCODE":"destination_port_code"
                                                ,"Port to Port T/T":"transit_time","Valid\nFrom":"start_date","Valid\nUntil":"expiry_date","REMARKS":"Remarks"}, inplace = True)

            freight_df = freight_df[["origin_icd",'origin_icd_code','origin_port_name','origin_port_code'
                ,'destination_icd','destination_icd_code','destination_port_name',"destination_port_code",
                                      'via_port_1',"via_port_2" ,'transit_time',
                                     'start_date', 'expiry_date',"Remarks","Sailings per week",'CFS Cut-Off to ETD','Routing Type (Direct/Transit)']]

            freight_df['start_date'] = pd.to_datetime(freight_df['start_date'],errors='coerce').dt.date
            freight_df['expiry_date'] = pd.to_datetime(freight_df['expiry_date'],errors='coerce').dt.date

            freight_df [['Uniq', 'origin_country', 'service_type', 'commodity', 'mode_of_transportation',
                        'inclusions', 'cfs_stuffing', 'charges_leg', 'charges', 'basis', 'amount','destination_country',
                        'amount_min','cargo_type',"Ratio",
                        'DIM', 'currency']] = ''
            freight_df['origin_country'] = freight_df['origin_port_code'].str[:2]
            freight_df["destination_country"]  = freight_df["destination_port_code"].str[:2]
            freight_df['cargo_type'] = "FAK"
            return freight_df, input_freight_df


        def get_freight_table(self):
            pass

        def capture(self):
            freight_df = self.df.copy()
            freight_df, input_freight_df = self.data_preprocessing(freight_df)

            origin_df = freight_df.copy(deep = True)
            origin_surcharges_temp_df = freight_df.copy(deep = True)
            origin_surcharges_df = pd.DataFrame(columns=freight_df.columns)
            origin_df, origin_surcharges_df = self.origin_charges_all( origin_df, input_freight_df,
                                                                        origin_surcharges_temp_df,
                                                                        origin_surcharges_df )

            ocean_df = freight_df.copy(deep = True)
            ocean_surcharges_temp_df = freight_df.copy(deep = True)
            ocean_surcharges_df = pd.DataFrame(columns=freight_df.columns)
            ocean_df, ocean_surcharges_df = self.ocean_charges_all(ocean_df, input_freight_df,
                                                                       ocean_surcharges_temp_df,
                                                                       ocean_surcharges_df )
            ipi_df = freight_df.copy( deep=True)
            ipi_surcharges_column_list = freight_df.copy(deep = True)
            ipi_surcharges_df = pd.DataFrame( columns=freight_df.columns )
            ipi_df = self.ipi_charges_all(ipi_df,ipi_surcharges_df,input_freight_df,ipi_surcharges_column_list )

            destination_df = freight_df.copy( deep=True )
            destination_surcharges_temp_df = freight_df.copy( deep=True )
            destination_surcharges_df = pd.DataFrame( columns=freight_df.columns )
            destination_df,  destination_surcharges_df = self.destination_charges_all( destination_df, input_freight_df,
                                                                       destination_surcharges_temp_df,
                                                                       destination_surcharges_df )

            final_df = pd.concat([origin_df, origin_surcharges_df,ocean_df,ocean_surcharges_df,ipi_df,destination_df,destination_surcharges_df], axis=0)
            final_df.reset_index(drop=True, inplace=True)



            self.captured_output = final_df

        def clean(self):
            self.cleaned_output = {'Freight': self.captured_output}