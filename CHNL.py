from logging import getLogger
import pandas as pd
from base import BaseTemplate, BaseFix
import re

log = getLogger(__name__)


class Ceva_Cnc_AP(BaseTemplate):
    class Cnc_Fak(BaseFix):
        def __init__(self, df, file_model: dict, headers_config: dict):
            super().__init__(df, file_model, headers_config)

            self.validity = {}
            self.load_type_map = {}

        def check_input(self):
            pass

        def check_output(self):
            pass

        @property
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

            freight_df.loc[freight_df['Country of POD'].str.contains('Bangladesh', na=False,
                                                                     case=False), 'inclusions'] = inclusions + '/' + 'DTHC '
            freight_df['40GP'] = freight_df['40ST&HC']
            freight_df['40HC'] = freight_df['40ST&HC']
            freight_df.drop(
                ['40ST&HC', 'Date of Revision', 'POO', 'POO CODE', 'FPD', 'FPD CODE', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri',
                 'Sat', 'Sun'], axis=1, inplace=True)
            freight_df.rename(
                columns={'POL': 'origin_port', 'Country of POL': 'origin_country', 'POD': 'destination_port',
                         'Country of POD': 'destination_country', 'end_date': 'expiry_date'}, inplace=True)

            self.captured_output = freight_df

            return self.captured_output

        def clean(self):

            self.cleaned_output = {"Freight": self.captured_output}
            return self.cleaned_output

    def resolve_dependency(cls, fix_outputs):

        if "CNC FAK" in fix_outputs:
            df_CNC_FAK_dict = fix_outputs.pop('CNC FAK')
            df_CNC_FAK = df_CNC_FAK_dict["Freight"]

        if "Ex Japan Q1 FAK" in fix_outputs:
            df_Ex_Japan_dict = fix_outputs.pop('Ex Japan Q1 FAK')
            df_Ex_Japan = df_Ex_Japan_dict["Freight"]

        freight_df = pd.concat([df_CNC_FAK, df_Ex_Japan], ignore_index=True)
        fix_outputs = {"CNC FAK": {"Freight": freight_df}}

        return fix_outputs
