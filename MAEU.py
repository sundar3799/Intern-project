from logging import getLogger

from base import BaseTemplate, BaseFix
from custom_exceptions import InputValidationError
import pandas as pd
import re
from numpy import nan
import warnings

# from dateutil.parser import parse


warnings.simplefilter(action='ignore', category=FutureWarning)

log = getLogger(__name__)


class KN_Mearsk_v1(BaseTemplate):
    class OceanRates(BaseFix):
        def check_input(self):
            pass
        def capture(self):
            df = self.df.reset_index(drop=True)

            df['origin_country'] = df['origin_port'].str.rsplit(',', 1).str[1]
            df['destination_country'] = df['destination_port'].str.rsplit(',', 1).str[1]

            df['origin_port'] = df['origin_port'].str.rsplit(',', 1).str[0]
            df['destination_port'] = df['destination_port'].str.rsplit(',', 1).str[0]
            nan_value = float("NaN")

            df.replace("", nan_value, inplace=True)
            df = df.dropna(subset=["origin_port", "destination_port"])
            df = df[df.origin_port != 'Receipt']
            inclusive_df  = df.loc[df['40HC'].str.lower() == 'inclusive'][["origin_port","destination_port","charge"]]
            df = pd.merge(df, inclusive_df, on=["origin_port", "destination_port"], how='left')
            df["inclusive"] = df["inclusive"] + df["charge_y"]
            df.drop(columns="charge_y", inplace=True)
            df.rename(columns={"charge_x": "charge"}, inplace=True)
            df.reset_index(drop=True, inplace=True)
            df['subject_to'] = ''
            for i in range(df.shape[0]):
                if '*' in df['40HC'][i]:
                    df['subject_to'][i] = df['charge'][i]
            df = df[df["40HC"].str.lower() != 'inclusive']

            if "20GP" in df:
                df['20GP_'] = df['20GP'].str.rsplit(' ', 1).str[0]
                df['currency'] = df['20GP'].str.rsplit(' ', 1).str[1]
                df['20GP'] = df['20GP_']

            if "40GP" in df:
                df['40GP'] = df['40GP'].str.rsplit(' ', 1).str[0]

            df['40HC_'] = df['40HC'].str.rsplit(' ', 1).str[0]
            df['currency'] = df['40HC'].str.rsplit(' ', 1).str[1]
            df['40HC'] = df['40HC_']
            df['amendment_no'] = 20

            df.drop(columns=["40HC_"], inplace=True)


            self.df = df

        def clean(self):
            df = self.df
            if "20GP" in df:
                df['20GP_'] = df['20GP'].str.rsplit(' ', 1).str[0]
                df['20GP'] = df['20GP_']
                df['40GP'] = df['40GP'].str.rsplit(' ', 1).str[0]
                df['40HC'] = df['40HC'].str.rsplit(' ', 1).str[0]
                df.drop(columns="20GP_",inplace= True)

            remap_basis = {"PER_CONTAINER" : "per container" , "PER_DOC" : "Per B/L","PER_DOCUMENT" : "Per B/L"}
            df["basis"] = df["basis"].replace(remap_basis)
            df['commodity'] = "FAK"
            df['service_type'] = 'yy'
            df["currency"] = df["currency"].str.replace('*','' , regex = True)
            df['liner_carrier_code'] = 'MAEU'
            df.rename(columns={'inclusive': 'inclusions', 'expiry': 'expiry_date', 'charge': 'charges'}, inplace=True)
            self.cleaned_output ={'Freight': df}



            pass
        def check_output(self):
            pass

    class Abbreviation(BaseFix):
        def check_input(self):
            pass
        def capture(self):
            df = self.df
            df = df[["charge_code", "charge_code_description"]]
            nan_value = float("NaN")
            df.replace("", nan_value, inplace=True)
            df = df.dropna(subset=["charge_code", "charge_code_description"])
            self.df = df

        def clean(self):
            self.cleaned_output ={'Abbreviation': self.df}
        def check_output(self):
            pass

    class InlandRates(BaseFix):
        def check_input(self):
            pass

        @classmethod
        def remove_empty_columns(cls, df):
            df = df.applymap(lambda x: nan if x == '' else x)
            df = df.dropna(axis=1, how="all")
            df = df.reset_index(drop=True)
            df = df.fillna('')
            return df

        def capture(self):
            df = self.df
            nan_value = float("NaN")

            df.replace("", nan_value, inplace=True)
            df = df.dropna(subset=["icd", "to" , "load_type"])
            df = df[df.icd.str.lower() != 'inland point']
            df.loc[df['direction'].str.lower() == 'import', 'charges'] = 'destination arbitrary charges'
            df.loc[df['direction'].str.lower() == 'import', 'at'] = 'destination'

            df.loc[df['direction'].str.lower() == 'export', 'charges'] = 'origin arbitrary charges'
            df.loc[df['direction'].str.lower() == 'export', 'at'] = 'origin'
            df['amendment_no'] = 20
            df = df[['icd', 'to', 'load_type', 'amount', 'currency', 'start_date', 'expiry', 'mode_of_transportation', 'commodity',"charges" , "at","amendment_no"]]

            self.df = df

        def clean(self):
            remap_load_type = {"20DRY": "20GP", "40DRY": "40GP", "40HDRY": "40HC"}
            self.df["load_type"] = self.df["load_type"].replace(remap_load_type)
            self.df['commodity'],  self.df['liner_carrier_code'], self.df['basis'] = 'FAK', 'MAEU', 'per container'
            self.cleaned_output ={'Arbitary': self.df}

        def check_output(self):
            pass

    class Title_Page(BaseFix):

        def check_input(self):

            pass

        def capture(self):

            if self.df[0].str.contains('Service Contract  Number').any():
                sc_number_index = self.df[(self.df[0].str.contains('Service Contract  Number'))].index.values[0]
                sc_number = self.df.iloc[sc_number_index, 1]
            self.captured_output = sc_number

        def clean(self):

            self.cleaned_output = {'contract_id': self.captured_output}

        def check_output(self):

            pass

    def resolve_dependency(cls, fix_outputs):

        Freight_sheet = fix_outputs.pop('Ocean Rates')
        Freight_df = Freight_sheet["Freight"]

        Abbreviation_sheet = fix_outputs.pop('Abbreviation ')
        Abbreviation_df = Abbreviation_sheet['Abbreviation']


        Abbreviation_df = Abbreviation_df[Abbreviation_df.charge_code != 'Charge_Code']
        subject_to_df = Freight_df.loc[Freight_df['subject_to'] != '']

        df = pd.merge(Freight_df ,Abbreviation_df,left_on="charges",right_on= 'charge_code', how='inner' ,sort=False)
        df.drop(columns=["charges","charge_code"], inplace=True)

        df.rename(columns={"charge_code_description": "charges"}, inplace=True)

        Arbitary_sheet = fix_outputs.pop('Inland Rates')
        Arbitary_df = Arbitary_sheet['Arbitary']

        title_page = fix_outputs.pop('Title Page')
        contract_id = title_page['contract_id']
        df.loc[(df['subject_to'] != ''), 'subject_to'] = ''
        subject_to_df = df.loc[(df['charges'] == 'Inland Haulage Import')]
        subject_to_df['subject_to'], subject_to_df['charges'] = 'IHI', 'Basic Ocean Freight'
        df = pd.concat([df, subject_to_df])
        df.drop(['origin_country', 'destination_country'], axis=1, inplace=True)
        df['contract_no'] = contract_id
        Arbitary_df['contract_no'] = contract_id

        fix_outputs =[{"Freight":df  ,"Arbitrary"  : Arbitary_df }]
        return fix_outputs


class CV_Maersk_v2(BaseTemplate):
    class OceanRates(BaseFix):
        def check_input(self):

            pass

        def first_rows_as_header(self, df):
            headers = df.iloc[0]
            df = df[1:]
            df.columns = headers
            return df

        def capture(self):
            df = self.df.reset_index(drop=True)

            if self.df.iloc[:, 0].str.contains("Receipt").any():
                start_index = self.df[(self.df.iloc[:, 0].str.contains('Receipt'))].index.values[0]
                df = self.df.loc[int(start_index):]
                df = self.first_rows_as_header(df)

                if "45HDRY" not in df:
                    columns_rename = {"20DRY": "20GP", "40DRY": "40GP", "40HDRY": "40HC", "40HREF": "40RF",
                                      "Charge": "charges", "Commodity Name": "commodity",
                                      "Receipt": "origin_port", "Delivery": "destination_port",
                                      "DIRECTION": "direction",
                                      "Effective Date": "start_date", "Expiry Date": "expiry",
                                      "INCLUSIVE SURCHARGES": "inclusive", "ORIGIN MOT": "mode_of_transportation",
                                      "POD": "via_pod", "POL": "via_pol",
                                      "Rate Basis": "basis", "Service Mode": "service_type",
                                      "TRANSIT TIME": "transit_time",
                                      "SOC": "SOC",
                                      "NOR": "NOR", "IMO": "IMO"}
                elif "45HDRY" in df:
                    columns_rename = {"20DRY": "20GP", "40DRY": "40GP", "40HDRY": "40HC", "45HDRY": "45HC",
                                      "Charge": "charges", "Commodity Name": "commodity", "Currency": "currency",
                                      "Delivery": "destination_port", "Effective Date": "start_date",
                                      "Expiry Date": "expiry", "Inclusive Surcharge": "inclusive",
                                      "Rate Basis": "basis",
                                      "Receipt": "origin_port", "Service Mode": "service_type",
                                      "Load Port": "Load_Port",
                                      "Discharge Port": "Discharge_Port"}
                df.rename(columns=columns_rename, inplace=True)

            df['origin_country'] = df['origin_port'].str.rsplit(',', 1).str[1]
            df['destination_country'] = df['destination_port'].str.rsplit(',', 1).str[1]
            df['origin_port'] = df['origin_port'].str.rsplit(',', 1).str[0]
            df['destination_port'] = df['destination_port'].str.rsplit(',', 1).str[0]
            nan_value = float("NaN")
            df.replace("", nan_value, inplace=True)
            df = df.dropna(subset=["origin_port", "destination_port"])
            df = df[df.origin_port != 'Receipt']
            if "40RF" in df:
                inclusive_df = df.loc[df['40RF'].str.lower() == 'inclusive'][["origin_port", "destination_port", "charges"]]
                df = pd.merge(df, inclusive_df, on=["origin_port", "destination_port"], how='left')
                df["inclusive"] = df["charges_y"]

                df.drop(columns="charges_y", inplace=True)
                df.rename(columns={"charges_x": "charges"}, inplace=True)
            if "40RF" in df:
                df = df[df["40RF"].str.lower() != 'inclusive']
                df.dropna(axis=1, how='all', inplace=True)
                df.dropna()  # drop all rows that have any NaN values
                # df.drop(how='all',inplace=True)

            if "20GP" in df:
                df['currency'] = df['20GP'].str.rsplit(' ', 1).str[0]
                df['20GP'] = df['20GP'].str.rsplit(' ', 1).str[1]

            if "40GP" in df:
                df['40GP'] = df['40GP'].str.rsplit(' ', 1).str[1]

            if "40HC" in df:
                # df['currency'] = df['40HC'].str.rsplit(' ', 1).str[0]
                df['40HC'] = df['40HC'].str.rsplit(' ', 1).str[1]

            # if "45HC" in df:
                # df['currency'] = df['40HC'].str.rsplit(' ', 1).str[0]
                # df['45HC'] = df['45HC'].str.rsplit(' ', 1).str[0]
            #df.drop(columns="Load_Port", inplace=True)
            #df.drop(columns="Discharge_Port", inplace=True)

            if "40RF" in df:
                df.loc[df['currency'].isnull(), 'currency'] = df['40RF']
                # df['currency'] = df['40RF'].str.rsplit(' ', 1).str[0]

                df['40RF'] = df['40RF'].str.rsplit(' ', 1).str[1]
                df['currency'] = df['currency'].str.rsplit(' ', 1).str[0]

            self.df = df

            return df

        def clean(self):
            df = self.df

            remap_basis = {"PER_CONTAINER": "per container", "PER_DOC": "Per B/L", "PER_DOCUMENT": "Per B/L"}
            df["basis"] = df["basis"].replace(remap_basis)
            df['commodity'] = "FAK"
            df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
            df['expiry'] = pd.to_datetime(df['expiry'], errors='coerce')



            df["currency"] = df["currency"].str.replace('*', '', regex=True)
            df = df.loc[~df["currency"].str.contains("inclusive", na = False, case = False)]

            self.cleaned_output = {'Freight': df}

            pass

        def check_output(self):
            pass

    class Abbreviation(BaseFix):
        def check_input(self):
            pass

        def capture(self):
            df = self.df
            df = df[["charge_code", "charge_code_description"]]
            nan_value = float("NaN")
            df.replace("", nan_value, inplace=True)
            # df = self.melt_load_type(df)
            df = df.dropna(subset=["charge_code", "charge_code_description"])
            self.df = df
            pass

        def clean(self):
            self.cleaned_output = {'Abbreviation': self.df}

        def check_output(self):
            pass

    def resolve_dependency(cls, fix_outputs):
        if "Ocean Rates" in fix_outputs:
            Freight_sheet = fix_outputs.pop('Ocean Rates')
            Freight_df = Freight_sheet["Freight"]

        if "Tender Quote" in fix_outputs:
            Freight_sheet = fix_outputs.pop('Tender Quote')
            Freight_df = Freight_sheet["Freight"]

        Abbreviation_sheet = fix_outputs.pop('Abbreviation')
        Abbreviation_df = Abbreviation_sheet['Abbreviation']

        Abbreviation_df = Abbreviation_df[Abbreviation_df.charge_code != 'Charge_Code']
        df = pd.merge(Freight_df, Abbreviation_df, left_on="charges", right_on='charge_code', how='inner', sort=False)
        df.drop(columns=["charges", "charge_code"], inplace=True)

        df.rename(columns={"charge_code_description": "charges"}, inplace=True)

        Arbitary_df = pd.DataFrame()
        if "Inland Rates" in fix_outputs:
            Arbitary_sheet = fix_outputs.pop('Inland Rates')
            Arbitary_df = Arbitary_sheet['Arbitary']
        if not Arbitary_df.empty:
            fix_outputs = [{"Freight": df, "Arbitrary": Arbitary_df}]
        else:
            fix_outputs = [{"Freight": df}]
        return fix_outputs


class CV_Maersk_v1(KN_Mearsk_v1):
    class OceanRates(KN_Mearsk_v1.OceanRates):
        def check_input(self):

            pass

        def first_rows_as_header(self, df):
            headers = df.iloc[0]
            df = df[1:]
            df.columns = headers
            return df

        def capture(self):
            df = self.df.reset_index(drop=True)

            if self.df.iloc[:, 0].str.contains("Receipt").any():
                start_index = self.df[(self.df.iloc[:, 0].str.contains('Receipt'))].index.values[0]
                df = self.df.loc[int(start_index):]
                df = self.first_rows_as_header(df)
                columns_rename = {"20DRY": "20GP", "40DRY": "40GP", "40HDRY": "40HC", "40HREF": "40RF",
                                  "Charge": "charges", "Commodity Name": "commodity",
                                  "Receipt": "origin_port", "Delivery": "destination_port", "DIRECTION": "direction",
                                  "Effective Date": "start_date", "Expiry Date": "expiry",
                                  "INCLUSIVE SURCHARGES": "inclusive", "ORIGIN MOT": "mode_of_transportation",
                                  "POD": "via_pod", "POL": "via_pol",
                                  "Rate Basis": "basis", "Service Mode": "service_type", "TRANSIT TIME": "transit_time",
                                  "SOC": "SOC",
                                  "NOR": "NOR", "IMO": "IMO"}
                df.rename(columns=columns_rename, inplace=True)

            df['origin_country'] = df['origin_port'].str.rsplit(',', 1).str[1]
            df['destination_country'] = df['destination_port'].str.rsplit(',', 1).str[1]
            df['origin_port'] = df['origin_port'].str.rsplit(',', 1).str[0]
            df['destination_port'] = df['destination_port'].str.rsplit(',', 1).str[0]
            nan_value = float("NaN")
            df.replace("", nan_value, inplace=True)
            df = df.dropna(subset=["origin_port", "destination_port"])
            df = df[df.origin_port != 'Receipt']
            inclusive_df = df.loc[df['40RF'].str.lower() == 'inclusive'][["origin_port", "destination_port", "charges"]]

            df = pd.merge(df, inclusive_df, on=["origin_port", "destination_port"], how='left')
            df["inclusive"] = df["charges_y"]

            df.drop(columns="charges_y", inplace=True)
            df.rename(columns={"charges_x": "charges"}, inplace=True)
            df = df[df["40RF"].str.lower() != 'inclusive']
            df.dropna(axis=1, how='all', inplace=True)
            df.dropna()

            if "20GP" in df:
                df['currency'] = df['20GP'].str.rsplit(' ', 1).str[0]
                df['20GP'] = df['20GP'].str.rsplit(' ', 1).str[1]

            if "40GP" in df:
                df['40GP'] = df['40GP'].str.rsplit(' ', 1).str[1]

            if "40HC" in df:

                df['40HC'] = df['40HC'].str.rsplit(' ', 1).str[1]

            if "40RF" in df:
                df.loc[df['currency'].isnull(), 'currency'] = df['40RF']

                df['40RF'] = df['40RF'].str.rsplit(' ', 1).str[1]
                df['currency'] = df['currency'].str.rsplit(' ', 1).str[0]

            self.df = df

            return df

        def clean(self):
            df = self.df

            remap_basis = {"PER_CONTAINER": "per container", "PER_DOC": "Per B/L", "PER_DOCUMENT": "Per B/L"}
            df["basis"] = df["basis"].replace(remap_basis)
            df['commodity'] = "FAK"

            df["currency"] = df["currency"].str.replace('*', '', regex=True)
            df = df.loc[~df["currency"].str.contains("inclusive", na = False, case = False)]
            df.drop(columns=["via_pod"], inplace=True)
            df.drop(columns=["via_pol"], inplace=True)


            self.cleaned_output = {'Freight': df}

            pass

        def check_output(self):
            pass

    class Abbreviation(BaseFix):
        def check_input(self):
            pass

        def capture(self):
            df = self.df
            df = df[["charge_code", "charge_code_description"]]
            nan_value = float("NaN")
            df.replace("", nan_value, inplace=True)
            df = df.dropna(subset=["charge_code", "charge_code_description"])
            df2 = [{"charge_code":"VP1","charge_code_description":"VP1"},{"charge_code":"CP1","charge_code_description":"CP1"}]
            df = df.append(df2, ignore_index=True)

            self.df = df

        def clean(self):
            self.cleaned_output = {'Abbreviation': self.df}

        def check_output(self):
            pass

    def resolve_dependency(cls, fix_outputs):
        if "Ocean Rates" in fix_outputs:
            Freight_sheet = fix_outputs.pop('Ocean Rates')
            Freight_df = Freight_sheet["Freight"]

        if "Tender Quote" in fix_outputs:
            Freight_sheet = fix_outputs.pop('Tender Quote')
            Freight_df = Freight_sheet["Freight"]

        Abbreviation_sheet = fix_outputs.pop('Abbreviation')
        Abbreviation_df = Abbreviation_sheet['Abbreviation']

        Abbreviation_df = Abbreviation_df[Abbreviation_df.charge_code != 'Charge_Code']
        df = pd.merge(Freight_df, Abbreviation_df, left_on="charges", right_on='charge_code', how='inner', sort=False)
        df.drop(columns=["charges", "charge_code"], inplace=True)

        df.rename(columns={"charge_code_description": "charges"}, inplace=True)

        Arbitary_df = pd.DataFrame()
        if "Inland Rates" in fix_outputs:
            Arbitary_sheet = fix_outputs.pop('Inland Rates')
            Arbitary_df = Arbitary_sheet['Arbitary']
        if not Arbitary_df.empty:
            fix_outputs = [{"Freight": df, "Arbitrary": Arbitary_df}]
        else:
            fix_outputs = [{"Freight": df}]
        return fix_outputs
