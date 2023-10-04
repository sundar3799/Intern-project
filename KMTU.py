from numpy import nan
import warnings
from logging import getLogger
from base import BaseTemplate, BaseFix
from custom_exceptions import InputValidationError
import pandas as pd
import re
from datetime import datetime
import warnings
'''
https://docs.google.com/spreadsheets/d/1W3Q74jP0aExDyR24cvZEH91EbvcACFZa/edit?usp=sharing&ouid=109468188401019395000&rtpof=true&sd=true
'''

class Ceva_Kmtc_Ap(BaseTemplate):
   class Ceva_Kmtc_Ap_Apr(BaseFix):

     def check_input(self):
        pass

     def check_output(self):
        pass



     def capture(self):

          self.df
          df = self.df.copy(deep=True)
          freight_df = df
          freight_df = freight_df.iloc[2:, :]
          freight_df = freight_df.loc[:, ~freight_df.columns.duplicated()]

          freight_df = freight_df[freight_df["contract_number"] != ""]
          freight_df['remarks'] = freight_df['remarks'].replace('', nan).fillna(method='ffill')


          freight_df['inclusions'] = freight_df['inclusions'].str.split('INCLUDING', expand=True)[1]
          freight_df["subject_to"] = freight_df["inclusions"].str.split("\(").str[1]


          freight_df["inclusions"] = freight_df["inclusions"].str.split("\(").str[0]
          freight_df['inclusions'] = freight_df['inclusions'].str.replace(r' ',';').str.strip(";")

          regex = r"(\w{3})  (\w{3})(\d{3})\/(.+?)(?:,|\(|)\s"

          for i in freight_df.to_dict("records"):

              matches = re.finditer(regex, str(i['subject_to']), re.MULTILINE)

              for matchNum, match in enumerate(matches, start=1):

                  charge_name = match.group(1)
                  curr = match.group(2)
                  value = match.group(3)


                  freight_df.loc[freight_df["subject_to"]==i['subject_to'], '20GP_' + charge_name] = value
                  freight_df.loc[freight_df["subject_to"]==i['subject_to'], '40HC_' + charge_name] = value
                  freight_df.loc[freight_df["subject_to"]==i['subject_to'], 'CURRENCY_' + charge_name] = curr


          freight_df.rename(columns={"40GP":"40HC"},inplace=True)
          freight_df.drop(columns=["subject_to"], inplace=True)
          freight_df["origin_port"] = "Shanghai"
          freight_df["destination_port"] = freight_df["destination_port"].str.replace('SIN', 'SINGAPORE')


          self.captured_output = {'Freight': freight_df}



     def clean(self):

         self.cleaned_output = self.captured_output
