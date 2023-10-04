from logging import getLogger
from base import BaseTemplate, BaseFix
import numpy as np
from .CMDU import Flexport_CMA_v1

log = getLogger(__name__)


class Ceva_Anl_Ap(BaseTemplate):
    class Ceva_Anl_Ap_1(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def commodity(self):
            commodity_index = self.df[(self.df[1].str.contains("Cdty", na=False))].index
            commodity = self.df.iloc[commodity_index, 1 + 1].values[0]
            return commodity

        def get_date(self):
            df1 = self.df
            date_index = list(self.df[(self.df[8].str.contains("Valid from", na=False))].index)
            date_df = self.df.iloc[date_index[0]:self.df.tail(1).index.values[0] + 1, 8:10].copy(deep=True)
            date_df.columns = date_df.iloc[0]
            date_df = date_df[1:].copy()
            start_date = str(date_df['Valid from']).split(' ', 1)[1].split('\n')[0]
            expiry_date = str(date_df['Valid To']).split(' ', 1)[1].split('\n')[0]
            date_list = []
            date_list.append(start_date)
            date_list.append(expiry_date)
            date_dict = {'start_date': '', 'expiry_date': ''}
            date_dict = dict(zip(date_dict, date_list))
            return date_dict

        def capture(self):
            commodity = self.commodity()
            date_dict = self.get_date()
            captured_dict = date_dict
            captured_dict['commodity'] = commodity
            self.captured_output = {'Freight': captured_dict}

        def clean(self):
            cleaned_df = self.captured_output
            self.cleaned_output = cleaned_df

    class Ceva_Anl_Ap_2(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_table(self):
            freight_df = self.df.copy()
            freight_df.columns = freight_df.iloc[1, :]
            freight_df = freight_df.iloc[2:, :]
            return freight_df

        @staticmethod
        def fill_na(freight_df):
            freight_df['Load Port'] = freight_df['Load Port'].replace('', np.nan, regex=True)
            freight_df['Discharge Port'] = freight_df['Discharge Port'].replace('', np.nan, regex=True)
            freight_df['Load Port'].fillna(method='ffill', inplace=True)
            freight_df['Discharge Port'].fillna(method='ffill', inplace=True)
            return freight_df

        @staticmethod
        def surcharge_remarks(freight_df):
            remarks_index = list(freight_df[(
                freight_df['Charge Description'].str.contains("CHARGES DEFINED AS COMMENTS", na=False))].index)
            remarks_df = freight_df.iloc[remarks_index[0]:freight_df.tail(1).index.values[0] + 1, :].copy(deep=True)
            remarks_df.drop(columns=['Load Port', 'Discharge Port'], axis=1, inplace=True)
            remarks_str = remarks_df.replace(np.nan, '').to_string(header=False, index=False).replace(r'\n', '') \
                .replace('20ST', '20ST:').replace('40ST', '40ST:').replace('40HC', '40HC:')
            return remarks_str

        def contract_no(self):
            contract_no = str(self.df.iloc[0, 0])
            contract_no = contract_no.split('\n')[1].split('-')[0]
            return contract_no

        def capture(self):
            freight_df = self.get_table()
            freight_df = self.fill_na(freight_df)
            remarks_str = self.surcharge_remarks(freight_df)
            contract_no = self.contract_no()
            freight_df.drop([2, 4, 12, 13, 14, 15, 16], inplace=True)
            freight_df['surcharge_remarks'] = remarks_str
            freight_df['contract_number'] = contract_no

            self.captured_output = {'Freight': freight_df}

        def clean(self):
            cleaned_df = self.captured_output['Freight']

            cleaned_df.drop(columns=['Quote\nLine N°', 'Place Of\nReceipt', 'Place Of\nDelivery', 'SOC', 'NOR', 'HAZ',
                                     'Comment to customer', 'Shipment\namount', 'Fixed', 'Payment\nMethod', 'Comments',
                                     'Additional Customer \ninformation', ], axis=1, inplace=True)
            cleaned_df.rename(
                columns={'Load Port': 'origin_port', 'Discharge Port': 'destination_port', 'Curr.': 'currency' \
                    , '20ST': '20GP', '40ST': '40GP', 'Charge Description': 'charge'}, inplace=True)
            cleaned_df.loc[3, 'charge'] = cleaned_df.loc[3, 'charge'].replace('RATE OFFER PER CONTAINER', "Ocean")
            cleaned_df['origin_port'] = cleaned_df['origin_port'].str.rsplit("\n").str[1]
            cleaned_df['destination_port'] = cleaned_df['destination_port'].str.rsplit("\n").str[1]
            self.cleaned_output = cleaned_df

    def resolve_dependency(cls, fix_outputs):

        if "Summary" in fix_outputs:
            summary_sheet = fix_outputs.pop('Summary')
            captured_dict = summary_sheet['Freight']
        if "Standard charges" in fix_outputs:
            standard_sheet = fix_outputs.pop('Standard charges')
            standard_sheet['commodity'] = captured_dict['commodity']
            standard_sheet['start_date'] = captured_dict['start_date']
            standard_sheet['expiry_date'] = captured_dict['expiry_date']
        fix_outputs = {"Summary": {"Freight": standard_sheet}}
        return fix_outputs


class Ceva_Anl_Usa(Flexport_CMA_v1):
    class Cover(Flexport_CMA_v1.Cover):
        pass

    class USWC(Flexport_CMA_v1.USWC):
        pass

    def resolve_dependency(cls, fix_outputs):
        df_USWC = fix_outputs.pop('APPENDIX UA-1 AUS2 (USWC-ANZ)')
        df_USEC = fix_outputs.pop('APPENDIX UA-5 NA TO PNG & SOPAC')
        coverlist = fix_outputs.pop('Cover')

        if 'Arbitrary Charges' in df_USWC.keys() and df_USWC['Arbitrary Charges'].empty:
            del(df_USWC['Arbitrary Charges'])
        if 'Arbitrary Charges' in df_USEC.keys() and df_USEC['Arbitrary Charges'].empty:
            del(df_USEC['Arbitrary Charges'])

        for key in df_USWC:
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
        fix_outputs = {'APPENDIX B-1  (FE - USWC)': df_USWC, 'APPENDIX B-2  (FE - USEC&GC)': df_USEC}
        return fix_outputs
