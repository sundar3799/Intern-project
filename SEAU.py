from logging import getLogger
from .MAEU import CV_Maersk_v1
from base import BaseTemplate, BaseFix
from dateutil.parser import parse
from custom_exceptions import InputValidationError
import pandas as pd
import re
from numpy import nan
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

log = getLogger(__name__)

class CV_Sealand_v1(BaseTemplate):
    class BAS_Surcharges(BaseFix, CV_Maersk_v1):
        def check_input(self):

            CV_Maersk_v1.OceanRates.capture(self)

        def first_rows_as_header(self, df):
            headers = df.iloc[0]
            df = df[1:]
            df.columns = headers
            return df

            df = self.df.reset_index(drop=True)

            if self.df.iloc[:, 0].str.contains("Receipt").any():
                start_index = self.df[(self.df.iloc[:, 0].str.contains('Receipt'))].index.values[0]
                df = self.df.loc[int(start_index):]
                df = self.first_rows_as_header(df)

                columns_rename = {"20DRY": "20GP", "40DRY": "40GP", "40HDRY": "40HC","45HDRY": "45HC","Charge": "charges", "Commodity Name": "commodity","Currency": "currency",
                                  "Delivery": "destination_port", "Effective Date": "start_date", "Expiry Date": "expiry", "Inclusive Surcharge": "inclusive", "Rate Basis": "basis",
                                  "Receipt": "origin_port", "Service Mode": "service_type", "Load Port": "Load_Port", "Discharge Port": "Discharge_Port"}


            df['origin_country'] = df['origin_port'].str.rsplit(',', 1).str[1]
            df['destination_country'] = df['destination_port'].str.rsplit(',', 1).str[1]
            df['origin_port'] = df['origin_port'].str.rsplit(',', 1).str[0]
            df['destination_port'] = df['destination_port'].str.rsplit(',', 1).str[0]
            nan_value = float("NaN")
            df.replace("", nan_value, inplace=True)
            df = df.dropna(subset=["origin_port", "destination_port"])
            df = df[df.origin_port != 'Receipt']
            df.drop(columns="charges_y", inplace=True)
            df.rename(columns={"charges_x": "charges"}, inplace=True)
            df.dropna(axis=1, how='all', inplace=True)
            df.dropna()
            if "20GP" in df:
                df['currency'] = df['20GP'].str.rsplit(' ', 1).str[0]
                df['20GP'] = df['20GP'].str.rsplit(' ', 1).str[1]

            if "40GP" in df:
                df['40GP'] = df['40GP'].str.rsplit(' ', 1).str[1]

            if "40HC" in df:
                df['40HC'] = df['40HC'].str.rsplit(' ', 1).str[1]

            if "45HC" in df:

                df['45HC'] = df['45HC'].str.rsplit(' ', 1).str[1]
            self.df = df
            return df

        def capture(self):
            pass
        def clean(self):
            df = self.df

            remap_basis = {"PER_CONTAINER": "per container", "PER_DOC": "Per B/L", "PER_DOCUMENT": "Per B/L"}
            df["basis"] = df["basis"].replace(remap_basis)
            df['commodity'] = "FAK"
            self.cleaned_output = {'Freight': df}

            pass

        def check_output(self):
            pass

        def resolve_dependency(cls, fix_outputs):
            if "BAS_Surcharges" in fix_outputs:
                Freight_sheet = fix_outputs.pop('BAS_Surcharges')
                Freight_df = Freight_sheet["Freight"]













