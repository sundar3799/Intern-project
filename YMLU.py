from logging import getLogger
from base import BaseTemplate, BaseFix, BaseDocxFix
from custom_exceptions import InputValidationError
import pandas as pd
import re
import warnings
from numpy import nan
from dateutil.parser import parse
from datetime import datetime
import calendar
warnings.simplefilter(action='ignore', category=FutureWarning)
from dateutil.parser import parse
log = getLogger(__name__)

class Expedock_YML(BaseTemplate):

    def __init__(self):
        self.dependency = None

    class Header_Fix(BaseFix):

        def check_input(self):
            pass

        def capture(self):

            header_details = {}
            if self.df[1].str.contains('S.C. Number').any():
                sc_number_index = self.df[(self.df[1].str.contains('S.C. Number'))].index.values[0]
                header_details["sc_number"] = self.df.loc[int(sc_number_index)][2]
            elif self.df[0].str.contains('S.C. Number').any():
                sc_number_index = self.df[(self.df[0].str.contains('S.C. Number'))].index.values[0]
                header_details["sc_number"] = self.df.loc[int(sc_number_index)][1]

            if self.df[1].str.contains('AMD #.').any():
                amendment_no_index = self.df[(self.df[1].str.contains('AMD #.'))].index.values[0]
                header_details["amendment_no"] = self.df.loc[int(amendment_no_index)][2]
            elif self.df[0].str.contains('AMD #.').any():
                amendment_no_index = self.df[(self.df[0].str.contains('AMD #.'))].index.values[0]
                header_details["amendment_no"] = self.df.loc[int(amendment_no_index)][1]

            if self.df[3].str.contains('Trade').any():
                Trade_index = self.df[(self.df[3].str.contains('Trade'))].index.values[0]
                header_details["trade"] = self.df.loc[int(Trade_index)][4]
            elif self.df[2].str.contains('Trade').any():
                Trade_index = self.df[(self.df[2].str.contains('Trade'))].index.values[0]
                header_details["trade"] = self.df.loc[int(Trade_index)][3]

            header_details["expiry_date"] = ""
            if self.df[3].str.contains('~').any():
                expiry_date_index = self.df[(self.df[3].str.contains('~'))].index.values[0]
                header_details["expiry_date"] = self.df.loc[int(expiry_date_index)][4]
            elif self.df[2].str.contains('~').any():
                expiry_date_index = self.df[(self.df[2].str.contains('~'))].index.values[0]
                header_details["expiry_date"] = self.df.loc[int(expiry_date_index)][3]

            header_details["start_date"] = ""
            if self.df[1].str.contains("Duration").any():
                expiry_date_index = self.df[(self.df[1].str.contains('Duration'))].index.values[0]
                header_details["start_date"] = self.df.loc[int(expiry_date_index)][2]
            elif self.df[0].str.contains("Duration").any():
                expiry_date_index = self.df[(self.df[0].str.contains('Duration'))].index.values[0]
                header_details["start_date"] = self.df.loc[int(expiry_date_index)][1]

            self.captured_output = header_details

            return self.captured_output

        def clean(self):
            self.cleaned_output = {"Header": self.captured_output}

        def check_output(self):
            pass

    class OceanFreight_Fix(BaseFix):

        def check_input(self):
            pass

        def melt_load_type(self, df):
            if "load_type" not in df:
                df = df.melt(id_vars=[column for column in df.columns if not column[1].isdigit()],
                             var_name="load_type",
                             value_name="amount")

                return df


        def capture(self,capture=None):
            captured_df = self.df
            if capture is not None:
                captured_df = captured_df.loc[captured_df['Amd. Type'].str.lower() != "delete"]
            captured_df = captured_df.loc[captured_df.commodity.str.lower() != "commodity"]
            captured_df = captured_df.drop([''], axis=1)
            self.captured_output = captured_df
            return self.captured_output

        def clean(self):
            clean_df = self.captured_output
            if "origin_port" in clean_df:
                clean_df["origin_port"] = clean_df["origin_port"].replace("/", ";", regex=True)
            clean_df["destination_port"] = clean_df["destination_port"].replace("/", ";", regex=True)
            clean_df["destination_icd"] = clean_df["destination_icd"].replace("/", ";", regex=True)
            clean_df["origin_icd"] = clean_df["origin_icd"].replace("/", ";", regex=True)
            clean_df["inclusions"] = clean_df["inclusions"].replace("/", ",", regex=True)
            clean_df['charges_leg'] = 'L3'
            clean_df['charges'] = 'Basic Ocean Freight'
            clean_df = self.melt_load_type(clean_df)
            clean_df = clean_df.loc[clean_df["amount"] != '']
            clean_df["currency"] = "USD"
            clean_df.loc[clean_df['remarks'].str.contains("FOR AC:"), 'customer_name'] = clean_df['remarks']
            clean_df.fillna("", inplace=True)
            #clean_df["customer_name"] = clean_df["customer_name"].apply(lambda x: x.split("SHIPPER:")[1] if x else "")
            clean_df = clean_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            premium_rate_check = ["PM21", "PM21PM", "PMSOC"]
            if not clean_df.empty:
                clean_df.loc[clean_df['commodity'].isin(premium_rate_check), 'Premium Service'] = "YM_Premium"
                clean_df["remarks"] = clean_df["remarks"].apply(lambda x: "" if x.startswith("FOR AC:") else x)

            self.cleaned_output = {"Freight": clean_df}
            return self.cleaned_output

        def check_output(self):
            pass

    class Arbitrary_Charge_Fix(BaseFix):

        def check_input(self):
            pass

        def capture(self,capture=None):
            captured_df = self.df
            if capture is not None:
                captured_df = captured_df.loc[captured_df['Amd.Type'].str.lower() != "delete"]
            captured_df = captured_df.drop([''], axis=1)
            captured_df = captured_df.loc[captured_df.currency.str.lower() != "currency"]
            self.captured_output = captured_df
            return self.captured_output

        def clean(self):
            clean_df = self.captured_output
            clean_df["to"] = clean_df["to"].replace("/", ";", regex=True)
            clean_df["icd"] = clean_df["icd"].replace("/", ";", regex=True)
            clean_df["currency"] = clean_df["currency"].apply(lambda x: x.split(":")[0])
            if "option_reference" not in clean_df:
                clean_df["remarks"] = clean_df["remarks"]
            if "option_reference" in clean_df:
                clean_df["remarks"] = clean_df["remarks"] + " " + clean_df["option_reference"]
            #if "at" in clean_df.empty:
            if "at" in clean_df:
                clean_df.loc[(clean_df["at"].str.lower() == 'destination'), 'charges'] = "destination arbitrary charges"
                clean_df.loc[(clean_df["at"].str.lower() == 'origin'), 'charges'] = "origin arbitrary charges"
            self.cleaned_output = {"Arbitrary Charges": clean_df}
            return self.cleaned_output

        def check_output(self):
            pass

    class Accessorial_Surcharge_Fix(BaseFix):

        def check_input(self):
            pass

        def capture(self,capture=None):
            captured_df = self.df
            if capture is not None:
                captured_df = captured_df.loc[captured_df['Amd. Type'].str.lower() != "delete"]
            captured_df = captured_df.drop([''], axis=1)
            self.captured_output = captured_df
            return self.captured_output

        def melt_load_type(self, df):
            if "load_type" not in df:
                df = df.melt(id_vars=[column for column in df.columns if not column[1].isdigit()],
                             var_name="load_type",
                             value_name="amount")
                return df

        def subcharges_increment(self, df):
            dfs = []
            unique_dict = df[["charges"]].drop_duplicates().to_dict('records')
            for dict in unique_dict:
                filtered_df = df.loc[df.charges == dict["charges"]]
                filtered_dict = filtered_df[["charges", "option_reference"]].drop_duplicates().to_dict('records')
                incrmt = 0
                for i in filtered_dict:
                    filtered_with_options_df = filtered_df.loc[(filtered_df.charges == i["charges"]) & (
                                filtered_df["option_reference"] == i["option_reference"])]
                    if incrmt == 0:
                        filtered_with_options_df['charges'] = i["charges"]
                    else:
                        filtered_with_options_df['charges'] = i["charges"] + str(incrmt)
                    dfs.append(filtered_with_options_df)
                    incrmt += 1
            processed_df = pd.concat(dfs, ignore_index=True, sort=False)

            # ranking_charge = df[['charges', 'option_reference']].value_counts().to_dict()
            # for row, values in ranking_charge.items():
            #     print(row, values)
            #     filtered_df = df.loc[(df.charges == row[0]) & (df.option_reference == row[1])]
            #     for i in range(0, values):
            #         if i == 0:
            #             filtered_df.iloc[i]['charges'] = row[0]
            #         else:
            #             filtered_df.iloc[i]['charges'] = row[0] + "_" + str(i)
            processed_df["remarks"] = ""
            processed_df.loc[processed_df["option_reference"] != "", 'remarks'] = processed_df["charges"] + " : " + \
                                                                                  processed_df["option_reference"]
            return processed_df

        def clean(self,clean=None):

            clean_df = self.captured_output
            check_string = clean_df.iloc[0, 0]
            check_list = list(check_string.split())
            if check_list[0][1:].islower():
                clean_df = clean_df.drop(axis=0, index=0)
            clean_df["charges"] = clean_df["charges"].apply(lambda x: x.split(":")[1])

            # charge_code_lookup = {
            #     "CY DELIVERY CHARGE": "CDC",
            #     "CURRENCY ADJUSTMENT FACTOR": "CAF",
            #     "GENERAL RATES INCREASE/RESTORATION": "GRI",
            #     "PEAK SEASON SURCHARGE": "PSS",
            #     "LOW SULPHUR FUEL SURCHARGE": "LSF"
            # }
            #clean_df["charges"] = clean_df["charges"].replace(charge_code_lookup, regex=True)
            #clean_df = self.subcharges_increment(clean_df)
            if not clean_df.empty:
                clean_df.loc[(clean_df["option_reference"].str.contains("G4", case=False, na=False)) & (clean_df["at"].str.lower() == "all") , "destination_port"] = "Long Beach, CA; Los Angeles, CA; Oakland, CA; Seattle, WA; Tacoma, WA"
                clean_df.loc[(clean_df["option_reference"].str.contains("G4", case=False, na=False)) & (clean_df["at"].str.lower() == "all") , "destination_portcode"] = "USLGB;USLAX;USOAK;USSEA;USTIW"

                clean_df.loc[(clean_df["option_reference"].str.contains("USEC", case=False, na=False)) & (clean_df["at"].str.lower() == "all") , "destination_port"] = "Charleston, SC; New York, NY; Norfolk, VA; Savannah, GA; HOUSTON,TX; MOBILE,AL; NEW ORLEANS,LA"
                clean_df.loc[(clean_df["option_reference"].str.contains("USEC", case=False, na=False)) & (clean_df["at"].str.lower() == "all") , "destination_portcode"] = "USCHS;USNYC;USORF;USSAV;USHOU;USMOB;USMSY"

                clean_df.loc[(clean_df["option_reference"].str.contains("IPI", case=False, na=False)) & (clean_df["at"].str.lower() == "all") , "destination_port"] = "IPI"
                clean_df.loc[(clean_df["option_reference"].str.contains("IPI", case=False, na=False)) & (clean_df["at"].str.lower() == "all") , "destination_portcode"] = "IPI"

                clean_df["currency"] = clean_df["currency"].apply(lambda x: x.split(":")[0])
                clean_df["remarks"] = clean_df["remarks"] + " " + clean_df["option_reference"]
                #clean_df.loc[(clean_df["at"].str.lower() == 'all'), ('origin_port', 'destination_port')] = "", ""
                clean_df.loc[(clean_df["at"].str.lower() == 'destination'), 'destination_port'] = clean_df['location']
                clean_df.loc[(clean_df["at"].str.lower() == 'origin'), 'origin_port'] = clean_df['location']
                clean_df["destination_port"] = clean_df["destination_port"].str.split(r"/")
                clean_df.fillna("", inplace = True)
                if "destination_port" in clean_df:
                    clean_df = clean_df.explode('destination_port')
                    clean_df["destination_port"] = clean_df["destination_port"].str.split(r";")
                if "destination_portcode" in clean_df:
                    clean_df["destination_portcode"] = clean_df["destination_portcode"].str.split(r";")
                    clean_df = clean_df.explode(['destination_port','destination_portcode'])
                clean_df["origin_port"] = clean_df["origin_port"].str.split(r"/")
                if clean is None:
                    clean_df = clean_df.explode('origin_port')
                clean_df = self.melt_load_type(clean_df)
                clean_df = clean_df.loc[(clean_df["amount"] != '')]
                clean_df = clean_df.loc[(clean_df["amount"] != "0" )]
                clean_df = clean_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            self.cleaned_output = {"Surcharges": clean_df}
            return self.cleaned_output

        def check_output(self):
            pass

    class Commodity_Fix(BaseFix):

        def check_input(self):
            pass

        def capture(self):
            captured_df = self.df
            captured_df = captured_df.drop([''], axis=1)

            captured_df = captured_df.loc[captured_df.group.str.lower() != "group"]
            self.captured_output = captured_df
            return self.captured_output

        def clean(self):
            group_lookup = {
                "A": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "B": "SHIPS/ BOATS/ VEHICLES/ CARS",
                "EC2": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "EC3": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "EC4": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "EC5": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "PS3": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "PS4": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "PS5": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "PS6": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "PS8": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "FP2": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "CEN": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "PN1": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "PN2": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "PN3": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "PN4": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "PM21": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "SAMSUNG": "FOOTWEAR, SOCK, HEADBAND",
                "VALMONT": "ELECTRIC/LIGHT POLES/UTILITY POLE",
                "NICEPAK": "SPUNLACE NONWOVEN FABRIC",
                "PM21PM": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "TWPM": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "VNPM": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)",
                "PRCPM": "FAK (NON-HAZ, EXCLUDING REEFER/ SHIPS/ BOATS/ VEHICLES/ CARS)"
            }
            self.captured_output = pd.DataFrame(group_lookup.items(), columns=['group', 'group_description'])

            self.cleaned_output = {"Commodity": self.captured_output}

        def check_output(self):
            pass

    class Port_Group_Fix(BaseFix):

        def check_input(self):
            pass

        def capture(self):
            captured_df = self.df
            captured_df = captured_df.drop([''], axis=1)

            captured_df = captured_df.loc[captured_df.group.str.lower() != "group"]
            port_group_look = {"group":["USWC","USEC","ABPS"],
                "at":["Destination","Destination","Origin"],
                "locations":["LONG BEACH,CA/LOS ANGELES,CA/OAKLAND,CA/TACOMA,WA/SEATTLE,WA",
                    "NEW YORK,NY/NORFOLK,VA/WILMINGTON,NC/CHARLESTON,SC/SAVANNAH,GA/JACKSONVILLE,FL/HOUSTON,TX/MOBILE,AL/NEW ORLEANS,LA",
                    "PUSAN/SHANGHAI/QINGDAO/NINGBO/HONG KONG/YANTIAN/KAOHSIUNG/SINGAPORE/LAEM CHABANG/HO CHI MINH CITY"
                    ]}

            captured_df = pd.DataFrame.from_dict(port_group_look)
            captured_df["locations"] = captured_df["locations"].replace("/", ";", regex=True)
            self.captured_output = captured_df
            return self.captured_output

        def clean(self):
            self.cleaned_output = {"Port_Group": self.captured_output}

        def check_output(self):
            pass

    def resolve_dependency(cls, fix_outputs, dependency=None):
        if dependency is None:
            def apply_port_group(freight_df, port_group_df):
                port_group_dict = port_group_df.to_dict('records')
                for row in port_group_dict:
                    if (row["at"].lower() == "destination"):
                        freight_df.loc[freight_df["destination_port"].str.contains(row["group"]), 'destination_port'] = \
                        freight_df['destination_port'].replace({row["group"]: row["locations"]}, regex=True)
                        freight_df.loc[freight_df["destination_icd"].str.contains(row["group"]), 'destination_icd'] = \
                        freight_df['destination_icd'].replace({row["group"]: row["locations"]}, regex=True)
                    elif (row["at"].lower() == "origin"):
                        freight_df.loc[freight_df["origin_port"].str.contains(row["group"]), 'origin_port'] = freight_df[
                            'origin_port'].replace({row["group"]: row["locations"]}, regex=True)
                        freight_df.loc[freight_df["origin_icd"].str.contains(row["group"]), 'origin_icd'] = freight_df[
                            'origin_icd'].replace({row["group"]: row["locations"]}, regex=True)

                return freight_df

        def apply_subcharges(freight_df, subcharges_df):
            if not subcharges_df.empty:
                subcharges_non_ipi = subcharges_df.loc[subcharges_df["destination_port"] != "IPI"]

                subcharges_non_ipi = subcharges_non_ipi.to_dict('records')
                apply_charges_df = []
                for row in subcharges_non_ipi:
                    if row["at"].lower() == "origin":
                        if row["origin_port"]:
                            try:
                                origin_port = row["origin_port"].split(",")[0]
                            except:
                                raise 'origin  port  in apply_destination_subcharges lookup out of index'

                            filtered_freight = freight_df.loc[(freight_df["load_type"].str.lower() == row["load_type"].lower()) & (freight_df["origin_port"].str.contains(origin_port, case = False, na =False)) & (~freight_df["inclusions"].str.contains('GRI'))]

                    if str(row["destination_port"]) != "nan":
                        try:
                            destination_port = row["destination_port"].split(",")[0]
                        except:
                            raise 'destination port  in apply_destination subcharges lookup out of index'

                        filtered_freight = freight_df.loc[(freight_df["load_type"].str.lower()  == row["load_type"].lower()) & (freight_df["load_type"].str.lower() == row["load_type"].lower()) & (freight_df["destination_icd"].str.contains(destination_port, case = False, na =False)) & (~freight_df["inclusions"].str.contains('GRI'))]
                        freight_df.loc[(freight_df["load_type"].str.lower() == row["load_type"].lower()) & (freight_df["destination_icd"].str.contains(destination_port, case=False, na=False)), "flag"] = 1

                        if row["destination_portcode"]:
                            filtered_freight_portcode = freight_df.loc[ (freight_df["load_type"].str.lower() == row["load_type"].lower()) &  (freight_df["destination_icd"].str.contains(row["destination_portcode"], case=False, na=False)) & (~freight_df["inclusions"].str.contains('GRI'))]
                            freight_df.loc[(freight_df["load_type"].str.lower() == row["load_type"].lower()) & (freight_df["destination_icd"].str.contains(row["destination_portcode"], case=False, na=False)), "flag"] = 1
                            filtered_freight_portcode = freight_df.loc[
                                (freight_df["load_type"].str.lower() == row["load_type"].lower()) & (
                                    freight_df["destination_icd"].str.contains(row["destination_portcode"], case=False,
                                                                               na=False))]
                            freight_df.loc[(freight_df["load_type"].str.lower() == row["load_type"].lower()) & (
                                freight_df["destination_icd"].str.contains(row["destination_portcode"], case=False,
                                                                           na=False)), "flag"] = 1
                            filtered_freight = pd.concat([filtered_freight, filtered_freight_portcode], ignore_index= True)
                            if freight_df["inclusions"].str.contains('GRI'):
                                filtered_freight_portcode = freight_df.loc[ (freight_df["load_type"].str.lower() == row["load_type"].lower()) &  (freight_df["destination_icd"].str.contains(row["destination_portcode"], case=False, na=False)) & (~freight_df["inclusions"].str.contains('GRI'))]

                        filtered_freight["amount"] = row["amount"]
                        filtered_freight["flag"] = 1
                        filtered_freight["currency"] = row["currency"]
                        #filtered_freight["charges"] = "Basic Ocean Freight"
                        filtered_freight["load_type"] = row["load_type"]
                        filtered_freight["charges"] = row["charges"]

                        apply_charges_df.append(filtered_freight)

                        # load_types = list(filter(lambda x: x[0].isdigit(), row.keys()))
                        # for load_type in load_types:
                        #     filtered_freight[load_type + "_" + row["charges"]] = row[load_type]
                        #     filtered_freight["currency"] = row["currency"]
                        #     filtered_freight["charges"] = "Basic Ocean Freight"

                freight_with_charges_df = pd.concat(apply_charges_df, ignore_index=True, sort=False)
                inlands_subcharges_df = subcharges_df.loc[subcharges_df["destination_port"] == "IPI"]
                freight_with_ipi_charges = []
                subcharges_ipi = inlands_subcharges_df.to_dict('records')
                for row in subcharges_ipi:
                    filtered_freight_ipi = freight_df.loc[
                        (freight_df["flag"] != 1) & (freight_df["load_type"].str.lower() == row["load_type"].lower()) & (~freight_df["inclusions"].str.contains('GRI'))]

                    freight_df.loc[
                        (freight_df["flag"] != 1) & (
                                    freight_df["load_type"].str.lower() == row["load_type"].lower()) & (~freight_df["inclusions"].str.contains('GRI')) , "flag"] = 2

                    filtered_freight_ipi["amount"] = row["amount"]
                    filtered_freight_ipi["flag"] = 2
                    filtered_freight_ipi["currency"] = row["currency"]
                    filtered_freight_ipi["charges"] = row["charges"]
                    filtered_freight_ipi["load_type"] = row["load_type"]
                    freight_with_ipi_charges.append(filtered_freight_ipi)

                freight_with_ipi_charges_df = pd.concat(freight_with_ipi_charges, ignore_index=True, sort=False)

                freight_with_charges = pd.concat([freight_with_charges_df,freight_with_ipi_charges_df],  ignore_index=True, sort=False)

                return freight_with_charges
            else:
                return pd.DataFrame()

        if "(6-3)Bullet Rate" in fix_outputs:
            bullet_rate_df = fix_outputs.pop('(6-3)Bullet Rate')
            if "Freight" in bullet_rate_df:
                bullet_rate_df = bullet_rate_df["Freight"]
            elif "bulletin" in bullet_rate_df:
                bullet_rate_df["bulletin"] = "(6-3)Bullet Rate"

        if "(6-1)Ocean Freight" in fix_outputs:
            ocean_freight_df = fix_outputs.pop('(6-1)Ocean Freight')
            #ocean_freight_df = pd.DataFrame(ocean_freight_df)
            if "Freight" in ocean_freight_df:
                ocean_freight_df = ocean_freight_df["Freight"]
            elif "bulletin" in ocean_freight_df:
                ocean_freight_df["bulletin"] = "(6-1)Ocean Freight"

        if "(6-1)Ocean Freight" in fix_outputs:
            if bullet_rate_df in locals():
                freight_df = pd.concat([bullet_rate_df, ocean_freight_df], ignore_index=True, sort=False)
            else:
                freight_df=ocean_freight_df
        else:
            freight_df = pd.concat([bullet_rate_df], ignore_index=True, sort=False)

        if "(6-3)Bullet Rate" in fix_outputs:
            freight_df = pd.concat([bullet_rate_df, ocean_freight_df], ignore_index=True, sort=False)
        else:
            freight_df = pd.concat([ocean_freight_df], ignore_index=True, sort=False)
        if dependency is not None:
            if "Commodity" in fix_outputs:
                # commodity_df = fix_outputs.pop('Commodity')
                commodity_df = fix_outputs.pop('Commodity')
                commodity_dict = commodity_df.set_index('commodity').to_dict()['commodity_description']
                # freight_df["commodity"] = freight_df["commodity"].str.split(r"/")
                freight_df['commodity_description'] = freight_df['commodity']
                for code in commodity_dict:
                    _code = (commodity_dict[code])
                    freight_df['commodity_description'].replace(code, _code, inplace=True, regex=True)
                arbitrary_charge_df = pd.DataFrame()
        if dependency is None:
            if "Commodity" in fix_outputs:
                commodity_df = fix_outputs.pop('Commodity')
                commodity_df = commodity_df["Commodity"]
                freight_df = pd.merge(freight_df, commodity_df, left_on="commodity", right_on='group', how='left',
                                      sort=False)
                freight_df.drop(columns=["commodity"], inplace=True)
                freight_df.rename(columns={"group_description": "commodity"}, inplace=True)

        arbitrary_charge_df = pd.DataFrame()
        if "(6-4)Outport Arbitrary Charge" in fix_outputs:
            arbitrary_charge_df = fix_outputs.pop('(6-4)Outport Arbitrary Charge')
            arbitrary_df = arbitrary_charge_df["Arbitrary Charges"]
            arbitrary_df["bulletin"] = "(6-4)Outport Arbitrary Charge"

        if "Accessorial Surcharge" in fix_outputs:
            accessorial_surcharge_df = fix_outputs.pop('Accessorial Surcharge')
            accessorial_surcharge_df = accessorial_surcharge_df["Surcharges"]
            accessorial_surcharge_df = apply_subcharges(freight_df, accessorial_surcharge_df)
            freight_df = pd.concat([freight_df, accessorial_surcharge_df], ignore_index=True, sort=False)

        else:
            freight_df = pd.concat([freight_df], ignore_index=True, sort=False)
        # freight_df["remarks"] += " " + remarks

        if "Port_Group" in fix_outputs:
            port_group_dict = fix_outputs.pop('Port_Group')
            port_group_df = port_group_dict["Port_Group"]
            freight_df = apply_port_group(freight_df, port_group_df)

        if "Header" in fix_outputs:
            Header_dict = fix_outputs.pop('Header')
            Header_dict = Header_dict["Header"]
            freight_df["amendment_no"] = Header_dict["amendment_no"]
            freight_df["unique"] = Header_dict["trade"]

            if 'arbitrary_df' in locals():
                arbitrary_df["unique"] = Header_dict["trade"]

            freight_df["contract_id"] = Header_dict["sc_number"]
            freight_df["contract_expiry_date"] = Header_dict["expiry_date"]
            freight_df["contract_start_date"] = Header_dict["start_date"]
            freight_df["vendor"] = "YML"

            if 'arbitrary_df' in locals():
                arbitrary_df["amendment_no"] = Header_dict["amendment_no"]
                arbitrary_df.loc[arbitrary_df['expiry_date'].isna() | (arbitrary_df['expiry_date'] == ''), 'expiry_date'] = \
                    Header_dict["expiry_date"]
                arbitrary_df.loc[arbitrary_df['start_date'].isna() | (arbitrary_df['start_date'] == ''), 'start_date'] = \
                    Header_dict["start_date"]

            freight_df.loc[freight_df['expiry_date'].isna() | (freight_df['expiry_date'] == ''), 'expiry_date'] = \
                Header_dict["expiry_date"]
            freight_df.loc[freight_df['start_date'].isna() | (freight_df['start_date'] == ''), 'start_date'] = \
                Header_dict["start_date"]

        if 'flag' in freight_df:
            freight_df.drop(columns="flag" , inplace= True)
        freight_df.drop_duplicates(inplace=True)
        freight_df = freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        if dependency ==None:
            cols = {'destination_icd': 'destination_port', 'destination_port': 'destination_icd'}
            freight_df.rename(columns=cols, inplace=True)
            if 'arbitrary_df' in locals() and not arbitrary_df.empty:
                fix_outputs =[{"Arbitrary Charges": arbitrary_df, "Freight":freight_df}]
            else:
                fix_outputs = [{"Freight": freight_df}]

            if dependency!=None:
                fix_outputs = {'(6-4)Outport Arbitrary Charge': {"Arbitrary Charges": arbitrary_df, "Freight": pd.DataFrame()},
                                '(6-1)Ocean Freight': {"Freight": freight_df}}

        return fix_outputs


class CEVA_YML_Usa(Expedock_YML):

    class OceanFreight_Fix(Expedock_YML.OceanFreight_Fix):
        def capture(self):
            self.captured_output = super().capture(1)
            return self.captured_output
        def clean(self):
            self.cleaned_output = super().clean()
            return self.cleaned_output

    class Arbitrary_Charge_Fix(Expedock_YML.Arbitrary_Charge_Fix):
        def capture(self):
            self.captured_output = super().capture(1)
            return self.captured_output

        def clean(self):
            self.cleaned_output = super().clean()
            return self.cleaned_output

    class Accessorial_Surcharge_Fix(Expedock_YML.Accessorial_Surcharge_Fix):
        def capture(self):
            self.captured_output = super().capture(1)
            return self.captured_output
        def clean(self):
            self.cleaned_output = super().clean()
            return self.cleaned_output


    class Commodity_Fix(Expedock_YML.Commodity_Fix):
        def capture(self):
            self.captured_output=super().capture()
            return self.captured_output

        def clean(self):
            self.cleaned_output = self.captured_output
            cols={"group":"commodity","group_description":"commodity_description"}
            self.cleaned_output.rename(columns=cols,inplace =True)
            return self.cleaned_output

    def resolve_dependency(cls, fix_outputs, freight_df=None):
        def apply_subcharges(freight_df, subcharges_df):
            if not subcharges_df.empty:
                subcharges_non_ipi = subcharges_df.loc[subcharges_df["destination_port"] != "IPI"]

                subcharges_non_ipi = subcharges_non_ipi.to_dict('records')
                apply_charges_df = []
                for row in subcharges_non_ipi:
                    if row["at"].lower() == "origin":
                        if row["origin_port"]:
                            try:
                                origin_port = row["origin_port"].split(",")[0]
                            except:
                                raise 'origin  port  in apply_destination_subcharges lookup out of index'
                            filtered_freight = freight_df.loc[
                                (freight_df["load_type"].str.lower() == row["load_type"].
                                 lower()) & (freight_df["origin_port"].str.contains
                                             (origin_port, case=False, na=False))]

                    if str(row["destination_port"]) != "nan":
                        try:
                            destination_port = row["destination_port"].split(",")[0]
                        except:
                            raise 'destination port  in apply_destination subcharges lookup out of index'

                        filtered_freight = freight_df.loc[
                            (freight_df["load_type"].str.lower() == row["load_type"].lower())
                            & (freight_df["load_type"].str.lower() == row["load_type"].lower()) &
                            (freight_df["destination_icd"].str.contains(destination_port, case=False, na=False))]
                        freight_df.loc[(freight_df["load_type"].str.lower() == row["load_type"].lower()) &
                                       (freight_df["destination_icd"].str.contains(destination_port, case=False,
                                        na=False)), "flag"] = 1

                        if row["destination_portcode"]:
                            filtered_freight_portcode = freight_df.loc[(freight_df["load_type"].str.lower() ==
                            row["load_type"].lower()) & (freight_df["destination_icd"].str.contains(
                            row["destination_portcode"], case=False, na=False))]
                            freight_df.loc[(freight_df["load_type"].str.lower() == row["load_type"].
                                lower()) & (freight_df["destination_icd"].str.contains(
                                row["destination_portcode"], case=False, na=False)), "flag"] = 1
                            filtered_freight = pd.concat([filtered_freight, filtered_freight_portcode],
                                                         ignore_index=True)
                        filtered_freight["amount"] = row["amount"]
                        filtered_freight["flag"] = 1
                        filtered_freight["currency"] = row["currency"]
                        # filtered_freight["charges"] = "Basic Ocean Freight"
                        filtered_freight["load_type"] = row["load_type"]
                        filtered_freight["charges"] = row["charges"]
                        apply_charges_df.append(filtered_freight)

                freight_with_charges_df = pd.concat(apply_charges_df, ignore_index=True, sort=False)

                inlands_subcharges_df = subcharges_df.loc[subcharges_df["destination_port"] == "IPI"]
                freight_with_ipi_charges = []
                subcharges_ipi = inlands_subcharges_df.to_dict('records')
                for row in subcharges_ipi:
                    filtered_freight_ipi = freight_df.loc[(freight_df["flag"] != 1) & (freight_df["load_type"].
                                                                        str.lower() == row["load_type"].lower())]

                    freight_df.loc[(freight_df["flag"] != 1) & (freight_df["load_type"].str.lower() == row["load_type"]
                                                                .lower()), "flag"] = 2

                    filtered_freight_ipi["amount"] = row["amount"]
                    filtered_freight_ipi["flag"] = 2
                    filtered_freight_ipi["currency"] = row["currency"]
                    filtered_freight_ipi["charges"] = row["charges"]
                    filtered_freight_ipi["load_type"] = row["load_type"]
                    freight_with_ipi_charges.append(filtered_freight_ipi)

                freight_with_ipi_charges_df = pd.concat(freight_with_ipi_charges, ignore_index=True, sort=False)
                freight_with_charges = pd.concat([freight_with_charges_df, freight_with_ipi_charges_df],
                                                 ignore_index=True, sort=False)
                return freight_with_charges
            else:
                return pd.DataFrame()

        if "(6-3)Bullet Rate" in fix_outputs:
            bullet_rate_df = fix_outputs.pop('(6-3)Bullet Rate')
            # bullet_rate_df = bullet_rate_df["Freight"]
            if "bulletin" in bullet_rate_df:
                bullet_rate_df["bulletin"] = "(6-3)Bullet Rate"
            else:
                bullet_rate_df = bullet_rate_df["Freight"]

        if "(6-1)Ocean Freight" in fix_outputs:
            ocean_freight_df = fix_outputs.pop('(6-1)Ocean Freight')
            # ocean_freight_df = ocean_freight_df["Freight"]
            if "bulletin" not in ocean_freight_df:
                ocean_freight_df = ocean_freight_df["Freight"]
            else:
                ocean_freight_df["bulletin"] = "(6-1)Ocean Freight"

        if "(6-3)Bullet Rate" in locals():
            freight_df = pd.concat([bullet_rate_df, ocean_freight_df], ignore_index=True, sort=False)
        else:
            freight_df = pd.concat([ocean_freight_df], ignore_index=True, sort=False)

        if "Commodity" in fix_outputs:
            # commodity_df = fix_outputs.pop('Commodity')
            commodity_df = fix_outputs.pop('Commodity')
            commodity_dict = commodity_df.set_index('commodity').to_dict()['commodity_description']
            # freight_df["commodity"] = freight_df["commodity"].str.split(r"/")
            freight_df['commodity_description'] = freight_df['commodity']
            for code in commodity_dict:
                _code = (commodity_dict[code])
                freight_df['commodity_description'].replace(code, _code, inplace=True, regex=True)

            arbitrary_charge_df = pd.DataFrame()
        if "(6-4)Outport Arbitrary Charge" in fix_outputs:
            arbitrary_charge_df = fix_outputs.pop('(6-4)Outport Arbitrary Charge')
            arbitrary_df = arbitrary_charge_df["Arbitrary Charges"]
        if "bulletin" in arbitrary_df:
            arbitrary_df["bulletin"] = "(6-4)Outport Arbitrary Charge"
        else:
            arbitrary_df = arbitrary_charge_df["Arbitrary Charges"]

        if "Accessorial Surcharge" in fix_outputs:
            accessorial_surcharge_df = fix_outputs.pop('Accessorial Surcharge')
            accessorial_surcharge_df = accessorial_surcharge_df["Surcharges"]
            accessorial_surcharge_df = apply_subcharges(freight_df, accessorial_surcharge_df)
            freight_df = pd.concat([freight_df, accessorial_surcharge_df], ignore_index=True, sort=False)
        else:
            freight_df = pd.concat([freight_df], ignore_index=True, sort=False)
        if "Header" in fix_outputs:
            Header_dict = fix_outputs.pop('Header')
            Header_dict = Header_dict["Header"]
            freight_df["amendment_no"] = Header_dict["amendment_no"]
            freight_df["unique"] = Header_dict["trade"]
            if 'arbitrary_df' in locals():
                arbitrary_df["unique"] = Header_dict["trade"]
            freight_df["contract_id"] = Header_dict["sc_number"]
            freight_df["contract_expiry_date"] = Header_dict["expiry_date"]
            freight_df["contract_start_date"] = Header_dict["start_date"]
            freight_df["vendor"] = "YML"

            if 'arbitrary_df' in locals():
                arbitrary_df["amendment_no"] = Header_dict["amendment_no"]
                arbitrary_df.loc[
                    arbitrary_df['expiry_date'].isna() | (arbitrary_df['expiry_date'] == ''), 'expiry_date'] = \
                    Header_dict["expiry_date"]
                arbitrary_df.loc[
                    arbitrary_df['start_date'].isna() | (arbitrary_df['start_date'] == ''), 'start_date'] = \
                    Header_dict["start_date"]
            freight_df.loc[freight_df['expiry_date'].isna() | (freight_df['expiry_date'] == ''), 'expiry_date'] = \
                Header_dict["expiry_date"]
            freight_df.loc[freight_df['start_date'].isna() | (freight_df['start_date'] == ''), 'start_date'] = \
                Header_dict["start_date"]

        if 'flag' in freight_df:
            freight_df.drop(columns="flag", inplace=True)
        freight_df.drop_duplicates(inplace=True)
        freight_df = freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        fix_outputs = {
            '(6-4)Outport Arbitrary Charge': {"Arbitrary Charges": arbitrary_df, "Freight": pd.DataFrame()},
            '(6-1)Ocean Freight': {"Freight": freight_df}}
        return fix_outputs


class Ceva_YML_Latam(BaseTemplate):
    class Rate_Sheet(BaseFix):
        def check_input(self):
            pass

        def first_rows_as_header(self, df):
            headers = df.iloc[1]
            df = df[0:]
            # df = df.T[0]
            df.columns = headers
            return df
            # column_rename = {"Port Of Loading": "origin_port", "Port Of Discharge": "destination_port", "20GP": "20GP",
            #                  "40GP": "40GP", "40HQ": "40HQ"}
            #
            # freight_df.rename(columns=columns_rename, inplace=True)
            # return freight_df

        def capture(self):
            df = self.df.reset_index(drop=True)
            # st_dt = parse(start_date_group)

            if self.df.iloc[:, 1].str.contains("origin").any():
                start_index = self.df[(self.df.iloc[:, 1].str.contains('Origin'))].index.values[0]
            # df = self.df.loc[int(start_index):]
            df = self.first_rows_as_header(df)
            freight_df = self.df

            POL_1 = pd.DataFrame()
            POL_1['origin_port'] = freight_df[1]

            index = [index for index in freight_df.iloc[0, ].index if freight_df.iloc[0, ][index] != '']

            freight_df_nz = []
            for element in range(len(index)-1):
                holder = pd.concat([POL_1, freight_df.iloc[:, index[element]:index[element + 1]]], axis=1)
                # holder = pd.concat([POL_1,], ignore_index=True,axis=1)
            # for column in freight_df.columns:
            #     if 'DC/HQ' in column:
            #             name = ' '.join(column.split()[:-1])
            #             freight_df[name + ' ' + column.split()[-1].split('/')[0]] = freight_df[column]
            #             freight_df[name + ' ' + column.split()[-1].split('/')[-1]] = freight_df[column]
            #             freight_df.drop(columns=column, axis=1, inplace=True)
                holder['destination_port'] = holder.iloc[0, 1]
                holder['destination_port'].iloc[1] = 'destination_port'
                holder['20GP'] = holder.iloc[:, 1]
                holder['40GP'] = holder.iloc[:, 2]
                holder['40HC'] = holder.iloc[:, 2]
                # holder.drop([0],axis=0,inplace=True)
                # holder.drop([2,3],axis=1,inplace=True)
                # holder = df.loc[:, ~df.columns.duplicated()]

                holder = holder.iloc[3:, :]

                holder.drop([index[element], index[element]+1], axis=1, inplace=True)
                freight_df_nz.append(holder)
                freight_df.append(holder)
                if element + 2 == len(index):
                    holder = pd.concat([POL_1, freight_df.iloc[:, index[element + 1]:]], axis=1)
                    holder['destination_port'] = holder.iloc[0, 1]
                    holder['destination_port'].iloc[1] = 'destination_port'
                    holder['20GP'] = holder.iloc[:, 1]
                    holder['40GP'] = holder.iloc[:, 2]
                    holder['40HC'] = holder.iloc[:, 2]

                    # holder.columns = holder.iloc[1, :]
                    holder = holder.iloc[3:, :]
                    holder.drop([index[element]+2, index[element] + 3], axis=1, inplace=True)

                    freight_df_nz.append(holder)
                    # freight_df.append(holder)

            freight_df_nz = freight_df_nz.copy()
            freight_df_nz = pd.concat(freight_df_nz, axis=0)
            self.captured_output = {'Freight': freight_df_nz}

        def clean(self):
            self.cleaned_output = self.captured_output

        def check_output(self):
            pass

    class OAC(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_arbs(self):

            df = self.df.copy(deep=True)
            dfs = []
            if self.df[0].str.contains('T/S PORT', na=False).any():
                port_index = list(self.df[self.df[0].str.contains('T/S PORT', na=False)].index)
                for i in range(len(port_index)):
                    to = self.df.loc[port_index[i], 0].split(':')[-1].strip()
                    holder = df.loc[port_index[i]:port_index[i + 1] - 1, :1].copy(deep=True)
                    if holder[0].str.contains('Add-on', na=False).any():
                        add_ons = list(holder[holder[0].str.contains('Add-on', na=False)].index)
                        for j in range(len(add_ons)):
                            hold = holder.loc[add_ons[j]: add_ons[j + 1] - 2, :]
                            amount = hold.iloc[0, 0].split()[-1].split('/')[0]
                            hold.iloc[1, 1] = 'icd'
                            hold.columns = hold.iloc[1, :]
                            hold = hold.iloc[2:, :]
                            hold['to'] = to
                            hold['20GP'], hold['40GP'], hold['40HC'] = amount, amount, amount
                            dfs.append(hold)
                            if j + 2 == len(add_ons):
                                hold = holder.loc[add_ons[j + 1]:, :]
                                amount = hold.iloc[0, 0].split()[-1].split('/')[0]
                                hold.iloc[1, 1] = 'icd'
                                hold.columns = hold.iloc[1, :]
                                hold = hold.iloc[2:, :]
                                hold['to'] = to
                                hold['20GP'], hold['40GP'], hold['40HC'] = amount, amount, amount
                                dfs.append(hold)
                                break
                    if i + 2 == len(port_index):
                        to = self.df.loc[port_index[i + 1], 0].split(':')[-1].strip()
                        holder = df.loc[port_index[i + 1]:, :1].copy(deep=True)
                        if holder[0].str.contains('Add-on', na=False).any():
                            add_ons = list(holder[holder[0].str.contains('Add-on', na=False)].index)
                            for j in range(len(add_ons)):
                                hold = holder.loc[add_ons[j]: add_ons[j + 1] - 1, :]
                                amount = hold.iloc[0, 0].split()[-1].split('/')[0]
                                hold.iloc[1, 1] = 'icd'
                                hold.columns = hold.iloc[1, :]
                                hold = hold.iloc[2:, :]
                                hold['to'] = to
                                hold['20GP'], hold['40GP'], hold['40HC'] = amount, amount, amount
                                dfs.append(hold)
                                if j + 2 == len(add_ons):
                                    hold = holder.loc[add_ons[j + 1]:, :]
                                    amount = hold.iloc[0, 0].split()[-1].split('/')[0]
                                    hold.iloc[1, 1] = 'icd'
                                    hold.columns = hold.iloc[1, :]
                                    hold = hold.iloc[2:, :]
                                    hold['to'] = to
                                    hold['20GP'], hold['40GP'], hold['40HC'] = amount, amount, amount
                                    dfs.append(hold)
                                    break
                        break

            if self.df[2].str.contains('T/S PORT', na=False).any():
                port_index = list(self.df[self.df[2].str.contains('T/S PORT', na=False)].index)
                ports = self.df.loc[port_index[0], 2].split('\n')[1:][0].replace(' ', '').split('/')
                for port in ports:
                    for i in range(len(port_index)):
                        to = port
                        holder = df.loc[port_index[i]:, 2:].copy(deep=True)
                        holder.replace('', nan, inplace=True)
                        holder.dropna(how='all', inplace=True)
                        if holder.iloc[:, 0].str.contains('Add-on', na=False).any():
                            add_ons = list(holder[holder.iloc[:, 0].str.contains('Add-on', na=False)].index)
                            for j in range(len(add_ons)):
                                hold = holder.loc[add_ons[j]: add_ons[j + 1] - 2, :]
                                amount = hold.iloc[0, 0].split()[-1].split('/')[0]
                                hold.iloc[1, 1] = 'icd'
                                hold.columns = hold.iloc[1, :]
                                hold = hold.iloc[2:, :]
                                hold['to'] = to
                                hold['20GP'], hold['40GP'], hold['40HC'] = amount, amount, amount
                                dfs.append(hold)
                                if j + 2 == len(add_ons):
                                    hold = holder.loc[add_ons[j + 1]:, :]
                                    amount = hold.iloc[0, 0].split()[-1].split('/')[0]
                                    hold.iloc[1, 1] = 'icd'
                                    hold.columns = hold.iloc[1, :]
                                    hold = hold.iloc[2:, :]
                                    hold['to'] = to
                                    hold['20GP'], hold['40GP'], hold['40HC'] = amount, amount, amount
                                    dfs.append(hold)
                                    break
                            break

            arb_df = pd.concat(dfs, ignore_index=True)

            return arb_df

        def capture(self):

            arb_df = self.get_arbs()
            arb_df['currency'] = arb_df['20GP'].str.extract(r"(?:([a-zA-Z]+))?")
            arb_df['20GP'] = arb_df['20GP'].str.extract(r"(\d+)")
            arb_df['40GP'] = arb_df['40GP'].str.extract(r"(\d+)")
            arb_df['40HC'] = arb_df['40HC'].str.extract(r"(\d+)")
            arb_df['charges'] = 'Origin Arbitrary Charges'
            arb_df["charges_leg"] = "L2"
            arb_df.rename(columns={'Location': 'icd_reference'}, inplace=True)
            self.captured_output = {'Arbitrary Charges': arb_df}

        def clean(self):

            self.cleaned_output = self.captured_output

            return self.cleaned_output


class Ceva_Yml_Ap(BaseTemplate):
    class Ceva_Yml_Ap_1(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_table(self):

            index_POL = list(self.df[(self.df[0].str.contains("Sourcing", na=False))].index)
            freight_df = self.df.iloc[index_POL[0]:self.df.tail(1).index.values[0] + 1, :].copy(deep=True)
            for i in range(freight_df.shape[-1]):
                if freight_df.iloc[1, i] == '':
                    freight_df.iloc[1, i] = freight_df.iloc[0, i]
            sell_index = [index for index in freight_df.iloc[0, :].index if 'Sell' in freight_df.iloc[0, index]][0]
            freight_df.columns = freight_df.iloc[1, :]
            freight_df = freight_df.iloc[2:, :]
            freight_df = pd.concat([freight_df.iloc[:, :sell_index], freight_df.iloc[:, sell_index + 2:]], axis=1)\

            return freight_df

        @staticmethod
        def inclusions(freight_df):
            for element in ['Subject', 'Subj', 'subject']:
                freight_df['inclusions'] = freight_df['inclusions'].str.split(element, expand=True)[0]
            freight_df['inclusions'] = freight_df['inclusions'].str.split(' ', 1, expand=True)[1]
            freight_df['inclusions'] = freight_df['inclusions'].replace('\(including ', '', regex=True)
            freight_df['inclusions'] = freight_df['inclusions'].replace('\)', '', regex=True)
            freight_df['inclusions'] = freight_df['inclusions'].replace(', ', ';', regex=True)
            freight_df['inclusions'] = freight_df['inclusions'].replace('/', ';', regex=True)
            freight_df['inclusions'] = freight_df['inclusions'].str.split('.', 1, expand=True)[0]
            return freight_df

        @staticmethod
        def cis_surcharge(freight_df):
            cis_index = list(freight_df[(freight_df["Rate Condition"].str.contains('CIS'))].index)
            sub_to_lst = freight_df.loc[cis_index[0], 'Rate Condition']
            sub_to_dict = {}
            if freight_df["Rate Condition"].str.contains('CIS', na=False).any():
                regex = r"CIS \((.+?)\)"
                matches = re.finditer(regex, sub_to_lst, re.MULTILINE)
                for matchNum, match in enumerate(matches, start=1):
                    for groupNum in range(0, len(match.groups())):
                        groupNum = groupNum + 1
                group = match.group(groupNum)
                cis_list = group.split('for')
                cis_rate = cis_list[0].replace('$', '')
                cis_type = cis_list[1].replace("'"," type")
                keys = cis_type.split("/")
                values = cis_rate.split("/")
                cis_dict = {}
                cis_dict = dict(zip(keys, values))
                freight_df.loc[cis_index, 'CIS_20GP'] = cis_dict[' 20 type']
                freight_df.loc[cis_index, 'CIS_40GP'] = cis_dict['40 type']
                freight_df.loc[cis_index, 'CIS_40HC'] = cis_dict['40 type']
            return freight_df

        def subject_to(self,freight_df):
            freight_df['subject_to'] = freight_df['subject_to'].str.split('to ', expand=True)[1]
            for element in ['both ends ', ' \(IMO2020\)','\(if any\)', 'CIS', " \(\$20/30 for 20'/40'\) /  ",\
                            ' at ', "monthly floating IMO2020 BUNKER", '\(', '\)','Floating ', 'quarterly review',\
                            ' dest HK&TW EMC and other ']:
                freight_df['subject_to'] = freight_df['subject_to'].replace(element, '', regex=True)
            emc_index = []
            for element in ['TWKEL', 'TWKHH', 'HKHKG']:
                emc_index.extend(list(freight_df[(freight_df["POD"].str.contains(element))].index))
            for index in emc_index:
                freight_df.loc[index,'subject_to'] = freight_df.loc[index, 'subject_to'] + ";EMC and other local charges"
            freight_df['subject_to'] = freight_df['subject_to'].replace(',', ';', regex=True)
            freight_df['subject_to'] = freight_df['subject_to'].replace('/', ';', regex=True)
            return freight_df

        def rate_condition(self,freight_df):
            inclusion_index = freight_df[freight_df['Rate Condition'].str.contains('Incl|incl|including|Including', na=False)].index.to_list()
            freight_df.loc[freight_df['Rate Condition'].str.contains('Incl|incl|including|Including', na=False), 'inclusions'] = \
            freight_df.loc[freight_df['Rate Condition'].str.contains('Incl|incl|including|Including', na=False)]['Rate Condition']
            subject_index = freight_df[freight_df['Rate Condition'].str.contains('Subject|subject|Subj|subj', na=False)].index.to_list()
            freight_df.loc[freight_df['Rate Condition'].str.contains('Subject|subject|Subj|subj', na=False), 'subject_to'] = \
                freight_df.loc[freight_df['Rate Condition'].str.contains('Subject|subject|Subj|subj', na=False)][
                    'Rate Condition']
            freight_df = self.inclusions(freight_df)
            freight_df = self.subject_to(freight_df)
            return freight_df

        def capture(self):
            freight_df = self.get_table()
            freight_df = self.rate_condition(freight_df)
            freight_df = self.cis_surcharge(freight_df)
            self.df = freight_df

        def clean(self):
            cleaned_df = self.df
            cleaned_df.drop(columns=['Sourcing', 'NAC/RFQ', 'DEM', 'DET','DEM', 'DET','Rate Condition',\
                                     'Primary/\nSecondary/\nTertiary','CSA / SP (Space Protection TEU per week)',
                                     'BD Owner']\
                            ,axis=1, inplace=True)
            cleaned_df.rename(
                columns={'POL':'origin_port','POD':'destination_port','Start Date':'start_date','End Date':'expiry_date'\
                         ,'Equipment':'Equipment_type','D20':'20GP','D40':'40GP','D40H':'40HC','Contract Reference':\
                             'contract_number','Customer':'customer_name'}, inplace=True)

            self.cleaned_output = {'Freight': cleaned_df}



class Flexport_YML(BaseTemplate):
    class Header_Fix(BaseFix):

        def check_input(self):
            pass

        def capture(self):

            header_details = {}
            if self.df[0].str.contains('S.C. Number').any():
                sc_number_index = self.df[(self.df[0].str.contains('S.C. Number'))].index.values[0]
                header_details["sc_number"] = self.df.loc[int(sc_number_index)][1]

            if self.df[0].str.contains('AMD #.').any():
                amendment_no_index = self.df[(self.df[0].str.contains('AMD #.'))].index.values[0]
                header_details["amendment_no"] = self.df.loc[int(amendment_no_index)][1]

            if self.df[2].str.contains('Trade').any():
                Trade_index = self.df[(self.df[2].str.contains('Trade'))].index.values[0]
                header_details["trade"] = self.df.loc[int(Trade_index)][3]

            if self.df[2].str.contains('~').any():
                expiry_date_index = self.df[(self.df[2].str.contains('~'))].index.values[0]
                header_details["expiry_date"] = self.df.loc[int(expiry_date_index)][3]

            header_details["start_date"] = ""
            if self.df[0].str.contains("Duration").any():
                expiry_date_index = self.df[(self.df[0].str.contains('Duration'))].index.values[0]
                header_details["start_date"] = self.df.loc[int(expiry_date_index)][1]

            self.captured_output = header_details

            return self.captured_output

        def clean(self):
            self.cleaned_output = {"Header": self.captured_output}

        def check_output(self):
            pass

    class OceanFreight_Fix(BaseFix):

        def check_input(self):
            pass

        def melt_load_type(self, df):
            if "load_type" not in df:
                df = df.melt(id_vars=[column for column in df.columns if not column[1].isdigit()],
                             var_name="load_type",
                             value_name="amount")

                return df

        def capture(self):
            captured_df = self.df
            captured_df = captured_df.drop([''], axis=1)
            captured_df = captured_df.loc[captured_df.commodity.str.lower() != "commodity"]
            self.captured_output = captured_df
            return self.captured_output

        def clean(self):
            clean_df = self.captured_output
            clean_df["origin_port"] = clean_df["origin_port"].replace("/", ";", regex=True)
            clean_df["destination_port"] = clean_df["destination_port"].replace("/", ";", regex=True)
            clean_df["destination_icd"] = clean_df["destination_icd"].replace("/", ";", regex=True)
            clean_df["origin_icd"] = clean_df["origin_icd"].replace("/", ";", regex=True)
            clean_df["inclusions"] = clean_df["inclusions"].replace("/", ",", regex=True)
            clean_df['charges_leg'] = 'L3'
            clean_df['charges'] = 'Basic Ocean Freight'
            clean_df = self.melt_load_type(clean_df)
            clean_df = clean_df.loc[clean_df["amount"] != '']
            clean_df["currency"] = "USD"
            clean_df.loc[clean_df['remarks'].str.contains("FOR AC:"), 'customer_name'] = clean_df['remarks']
            clean_df.fillna("", inplace=True)
            # clean_df["customer_name"] = clean_df["customer_name"].apply(lambda x: x.split("SHIPPER:")[1] if x else "")
            clean_df = clean_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            premium_rate_check = ["PM21", "PM21PM", "PMSOC"]
            clean_df.loc[clean_df['commodity'].isin(premium_rate_check), 'Premium Service'] = "YM_Premium"
            clean_df["remarks"] = clean_df["remarks"].apply(lambda x: "" if x.startswith("FOR AC:") else x)

            self.cleaned_output = {"Freight": clean_df}
            return self.cleaned_output

        def check_output(self):
            pass

    class Arbitrary_Charge_Fix(BaseFix):

        def check_input(self):
            pass

        def capture(self):
            captured_df = self.df
            captured_df = captured_df.drop([''], axis=1)
            captured_df = captured_df.loc[captured_df.currency.str.lower() != "currency"]
            self.captured_output = captured_df
            return self.captured_output

        def clean(self):
            clean_df = self.captured_output
            clean_df["to"] = clean_df["to"].replace("/", ";", regex=True)
            clean_df["icd"] = clean_df["icd"].replace("/", ";", regex=True)
            clean_df["currency"] = clean_df["currency"].apply(lambda x: x.split(":")[0])

            clean_df["remarks"] = clean_df["remarks"] + " " + clean_df["option_reference"]
            clean_df.loc[(clean_df["at"].str.lower() == 'destination'), 'charges'] = "destination arbitrary charges"
            clean_df.loc[(clean_df["at"].str.lower() == 'origin'), 'charges'] = "origin arbitrary charges"
            self.cleaned_output = {"Arbitrary Charges": clean_df}
            return self.cleaned_output

        def check_output(self):
            pass

    class Accessorial_Surcharge_Fix(BaseFix):

        def check_input(self):
            pass

        def capture(self):
            captured_df = self.df
            captured_df = captured_df.drop([''], axis=1)
            self.captured_output = captured_df
            return self.captured_output

        def melt_load_type(self, df):
            if "load_type" not in df:
                df = df.melt(id_vars=[column for column in df.columns if not column[1].isdigit()],
                             var_name="load_type",
                             value_name="amount")
                return df

        def subcharges_increment(self, df):
            dfs = []
            unique_dict = df[["charges"]].drop_duplicates().to_dict('records')
            for dict in unique_dict:
                filtered_df = df.loc[df.charges == dict["charges"]]
                filtered_dict = filtered_df[["charges", "option_reference"]].drop_duplicates().to_dict('records')
                incrmt = 0
                for i in filtered_dict:
                    filtered_with_options_df = filtered_df.loc[(filtered_df.charges == i["charges"]) & (
                            filtered_df["option_reference"] == i["option_reference"])]
                    if incrmt == 0:
                        filtered_with_options_df['charges'] = i["charges"]
                    else:
                        filtered_with_options_df['charges'] = i["charges"] + str(incrmt)
                    dfs.append(filtered_with_options_df)
                    incrmt += 1
            processed_df = pd.concat(dfs, ignore_index=True, sort=False)

            # ranking_charge = df[['charges', 'option_reference']].value_counts().to_dict()
            # for row, values in ranking_charge.items():
            #     print(row, values)
            #     filtered_df = df.loc[(df.charges == row[0]) & (df.option_reference == row[1])]
            #     for i in range(0, values):
            #         if i == 0:
            #             filtered_df.iloc[i]['charges'] = row[0]
            #         else:
            #             filtered_df.iloc[i]['charges'] = row[0] + "_" + str(i)
            processed_df["remarks"] = ""
            processed_df.loc[processed_df["option_reference"] != "", 'remarks'] = processed_df["charges"] + " : " + \
                                                                                  processed_df["option_reference"]
            return processed_df

        def clean(self):

            clean_df = self.captured_output
            check_string = clean_df.iloc[0, 0]
            check_list = list(check_string.split())
            if check_list[0][1:].islower():
                clean_df = clean_df.drop(axis=0, index=0)
            clean_df["charges"] = clean_df["charges"].apply(lambda x: x.split(":")[1])

            charge_code_lookup = {
                "CY DELIVERY CHARGE": "CDC",
                "CURRENCY ADJUSTMENT FACTOR": "CAF",
                "GENERAL RATES INCREASE/RESTORATION": "GRI",
                "PEAK SEASON SURCHARGE": "PSS",
                "LOW SULPHUR FUEL SURCHARGE": "LSF"
            }
            clean_df["charges"] = clean_df["charges"].replace(charge_code_lookup, regex=True)
            # clean_df = self.subcharges_increment(clean_df)
            clean_df.loc[(clean_df["option_reference"].str.contains("G4", case=False, na=False)) & (clean_df[
                                                                                                        "at"].str.lower() == "all"), "destination_port"] = "Long Beach, CA; Los Angeles, CA; Oakland, CA; Seattle, WA; Tacoma, WA"
            clean_df.loc[(clean_df["option_reference"].str.contains("G4", case=False, na=False)) & (
                        clean_df["at"].str.lower() == "all"), "destination_portcode"] = "USLGB;USLAX;USOAK;USSEA;USTIW"

            clean_df.loc[(clean_df["option_reference"].str.contains("USEC", case=False, na=False)) & (clean_df[
                                                                                                          "at"].str.lower() == "all"), "destination_port"] = "Charleston, SC; New York, NY; Norfolk, VA; Savannah, GA"
            clean_df.loc[(clean_df["option_reference"].str.contains("USEC", case=False, na=False)) & (
                        clean_df["at"].str.lower() == "all"), "destination_portcode"] = "USCHS;USNYC;USORF;USSAV"

            clean_df.loc[(clean_df["option_reference"].str.contains("IPI", case=False, na=False)) & (
                        clean_df["at"].str.lower() == "all"), "destination_port"] = "IPI"
            clean_df.loc[(clean_df["option_reference"].str.contains("IPI", case=False, na=False)) & (
                        clean_df["at"].str.lower() == "all"), "destination_portcode"] = "IPI"

            clean_df["currency"] = clean_df["currency"].apply(lambda x: x.split(":")[0])
            clean_df["remarks"] = clean_df["remarks"] + " " + clean_df["option_reference"]
            # clean_df.loc[(clean_df["at"].str.lower() == 'all'), ('origin_port', 'destination_port')] = "", ""
            clean_df.loc[(clean_df["at"].str.lower() == 'destination'), 'destination_port'] = clean_df['location']
            clean_df.loc[(clean_df["at"].str.lower() == 'origin'), 'origin_port'] = clean_df['location']
            clean_df["destination_port"] = clean_df["destination_port"].str.split(r"/")
            clean_df.fillna("", inplace=True)
            clean_df = clean_df.explode('destination_port')
            clean_df["destination_port"] = clean_df["destination_port"].str.split(r";")
            clean_df["destination_portcode"] = clean_df["destination_portcode"].str.split(r";")
            clean_df = clean_df.explode(['destination_port', 'destination_portcode'])
            clean_df["origin_port"] = clean_df["origin_port"].str.split(r"/")
            clean_df = clean_df.explode('origin_port')
            clean_df = self.melt_load_type(clean_df)
            clean_df = clean_df.loc[(clean_df["amount"] != '')]
            clean_df = clean_df.loc[(clean_df["amount"] != "0")]
            clean_df = clean_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            self.cleaned_output = {"Surcharges": clean_df}
            return self.cleaned_output

        def check_output(self):
            pass

    class Commodity_Fix(BaseFix):

        def check_input(self):
            pass

        def capture(self):
            captured_df = self.df
            captured_df = captured_df.drop([''], axis=1)
            captured_df = captured_df.loc[captured_df.group.str.lower() != "group"]
            self.captured_output = captured_df
            return self.captured_output

        def clean(self):
            self.cleaned_output = {"Commodity": self.captured_output}

        def check_output(self):
            pass

    class Port_Group_Fix(BaseFix):

        def check_input(self):
            pass

        def capture(self):
            captured_df = self.df
            captured_df = captured_df.drop([''], axis=1)
            captured_df = captured_df.loc[captured_df.group.str.lower() != "group"]
            captured_df["locations"] = captured_df["locations"].replace("/", ";", regex=True)
            self.captured_output = captured_df
            return self.captured_output

        def clean(self):
            self.cleaned_output = {"Port_Group": self.captured_output}

        def check_output(self):
            pass

    def resolve_dependency(cls, fix_outputs):
        def apply_port_group(freight_df, port_group_df):
            port_group_dict = port_group_df.to_dict('records')
            for row in port_group_dict:
                if (row["at"].lower() == "destination"):
                    freight_df.loc[freight_df["destination_port"].str.contains(row["group"]), 'destination_port'] = \
                        freight_df['destination_port'].replace({row["group"]: row["locations"]}, regex=True)
                    freight_df.loc[freight_df["destination_icd"].str.contains(row["group"]), 'destination_icd'] = \
                        freight_df['destination_icd'].replace({row["group"]: row["locations"]}, regex=True)
                elif (row["at"].lower() == "origin"):
                    freight_df.loc[freight_df["origin_port"].str.contains(row["group"]), 'origin_port'] = freight_df[
                        'origin_port'].replace({row["group"]: row["locations"]}, regex=True)
                    freight_df.loc[freight_df["origin_icd"].str.contains(row["group"]), 'origin_icd'] = freight_df[
                        'origin_icd'].replace({row["group"]: row["locations"]}, regex=True)

            return freight_df

        def apply_subcharges(freight_df, subcharges_df):
            if not subcharges_df.empty:
                # remarks_list = subcharges_df["remarks"].unique()
                # remarks = "; ".join(remarks_list)
                # remarks = re.sub("^;", "", remarks)
                subcharges_non_ipi = subcharges_df.loc[subcharges_df["destination_port"] != "IPI"]

                subcharges_non_ipi = subcharges_non_ipi.to_dict('records')
                apply_charges_df = []
                for row in subcharges_non_ipi:
                    if row["at"].lower() == "origin":
                        if row["origin_port"]:
                            try:
                                origin_port = row["origin_port"].split(",")[0]
                            except:
                                raise 'origin  port  in apply_destination_subcharges lookup out of index'

                            filtered_freight = freight_df.loc[
                                (freight_df["load_type"].str.lower() == row["load_type"].lower()) & (
                                    freight_df["origin_port"].str.contains(origin_port, case=False, na=False))]

                    if str(row["destination_port"]) != "nan":
                        try:
                            destination_port = row["destination_port"].split(",")[0]
                        except:
                            raise 'destination port  in apply_destination subcharges lookup out of index'

                        filtered_freight = freight_df.loc[
                            (freight_df["load_type"].str.lower() == row["load_type"].lower()) & (
                                        freight_df["load_type"].str.lower() == row["load_type"].lower()) & (
                                freight_df["destination_icd"].str.contains(destination_port, case=False, na=False))]
                        freight_df.loc[(freight_df["load_type"].str.lower() == row["load_type"].lower()) & (
                            freight_df["destination_icd"].str.contains(destination_port, case=False,
                                                                       na=False)), "flag"] = 1

                        if row["destination_portcode"]:
                            filtered_freight_portcode = freight_df.loc[
                                (freight_df["load_type"].str.lower() == row["load_type"].lower()) & (
                                    freight_df["destination_icd"].str.contains(row["destination_portcode"], case=False,
                                                                               na=False))]
                            freight_df.loc[(freight_df["load_type"].str.lower() == row["load_type"].lower()) & (
                                freight_df["destination_icd"].str.contains(row["destination_portcode"], case=False,
                                                                           na=False)), "flag"] = 1

                            filtered_freight = pd.concat([filtered_freight, filtered_freight_portcode],
                                                         ignore_index=True)

                        filtered_freight["amount"] = row["amount"]
                        filtered_freight["flag"] = 1
                        filtered_freight["currency"] = row["currency"]
                        filtered_freight["charges"] = "Basic Ocean Freight"
                        filtered_freight["load_type"] = row["load_type"] + "_" + row["charges"]
                        apply_charges_df.append(filtered_freight)

                        # load_types = list(filter(lambda x: x[0].isdigit(), row.keys()))
                        # for load_type in load_types:
                        #     filtered_freight[load_type + "_" + row["charges"]] = row[load_type]
                        #     filtered_freight["currency"] = row["currency"]
                        #     filtered_freight["charges"] = "Basic Ocean Freight"

                freight_with_charges_df = pd.concat(apply_charges_df, ignore_index=True, sort=False)

                inlands_subcharges_df = subcharges_df.loc[subcharges_df["destination_port"] == "IPI"]
                freight_with_ipi_charges = []
                subcharges_ipi = inlands_subcharges_df.to_dict('records')
                for row in subcharges_ipi:
                    filtered_freight_ipi = freight_df.loc[
                        (freight_df["flag"] != 1) & (freight_df["load_type"].str.lower() == row["load_type"].lower())]

                    freight_df.loc[
                        (freight_df["flag"] != 1) & (
                                freight_df["load_type"].str.lower() == row["load_type"].lower()), "flag"] = 2

                    filtered_freight_ipi["amount"] = row["amount"]
                    filtered_freight_ipi["flag"] = 2
                    filtered_freight_ipi["currency"] = row["currency"]
                    filtered_freight_ipi["charges"] = "Basic Ocean Freight"
                    filtered_freight_ipi["load_type"] = row["load_type"] + "_" + row["charges"]
                    freight_with_ipi_charges.append(filtered_freight_ipi)

                freight_with_ipi_charges_df = pd.concat(freight_with_ipi_charges, ignore_index=True, sort=False)

                freight_with_charges = pd.concat([freight_with_charges_df, freight_with_ipi_charges_df],
                                                 ignore_index=True, sort=False)

                return freight_with_charges
            else:
                return pd.DataFrame()

        if "(6-3)Bullet Rate" in fix_outputs:
            bullet_rate_df = fix_outputs.pop('(6-3)Bullet Rate')
            bullet_rate_df = bullet_rate_df["Freight"]
            bullet_rate_df["bulletin"] = "(6-3)Bullet Rate"

        if "(6-1)Ocean Freight" in fix_outputs:
            ocean_freight_df = fix_outputs.pop('(6-1)Ocean Freight')
            ocean_freight_df = ocean_freight_df["Freight"]
            ocean_freight_df["bulletin"] = "(6-1)Ocean Freight"

        freight_df = pd.concat([bullet_rate_df, ocean_freight_df], ignore_index=True, sort=False)

        if "Commodity" in fix_outputs:
            commodity_df = fix_outputs.pop('Commodity')
            commodity_df = commodity_df["Commodity"]
            freight_df = pd.merge(freight_df, commodity_df, left_on="commodity", right_on='group', how='left',
                                  sort=False)
            freight_df.drop(columns=["group"], inplace=True)
            freight_df.rename(columns={"group_description": "commodity_description"}, inplace=True)

        arbitrary_charge_df = pd.DataFrame()
        if "(6-4)Outport Arbitrary Charge" in fix_outputs:
            arbitrary_charge_df = fix_outputs.pop('(6-4)Outport Arbitrary Charge')
            arbitrary_df = arbitrary_charge_df["Arbitrary Charges"]
            arbitrary_df["bulletin"] = "(6-4)Outport Arbitrary Charge"

        if "Accessorial Surcharge" in fix_outputs:
            accessorial_surcharge_df = fix_outputs.pop('Accessorial Surcharge')
            accessorial_surcharge_df = accessorial_surcharge_df["Surcharges"]
            accessorial_surcharge_df = apply_subcharges(freight_df, accessorial_surcharge_df)

        freight_df = pd.concat([freight_df, accessorial_surcharge_df], ignore_index=True, sort=False)
        # freight_df["remarks"] += " " + remarks

        if "Port_Group" in fix_outputs:
            port_group_dict = fix_outputs.pop('Port_Group')
            port_group_df = port_group_dict["Port_Group"]
            freight_df = apply_port_group(freight_df, port_group_df)

        if "Header" in fix_outputs:
            Header_dict = fix_outputs.pop('Header')
            Header_dict = Header_dict["Header"]
            freight_df["amendment_no"] = Header_dict["amendment_no"]
            freight_df["unique"] = Header_dict["trade"]
            arbitrary_df["unique"] = Header_dict["trade"]

            arbitrary_df["amendment_no"] = Header_dict["amendment_no"]
            arbitrary_df.loc[arbitrary_df['expiry_date'].isna() | (arbitrary_df['expiry_date'] == ''), 'expiry_date'] = \
                Header_dict["expiry_date"]
            arbitrary_df.loc[arbitrary_df['start_date'].isna() | (arbitrary_df['start_date'] == ''), 'start_date'] = \
                Header_dict["start_date"]
            freight_df.loc[freight_df['expiry_date'].isna() | (freight_df['expiry_date'] == ''), 'expiry_date'] = \
                Header_dict["expiry_date"]
            freight_df.loc[freight_df['start_date'].isna() | (freight_df['start_date'] == ''), 'start_date'] = \
                Header_dict["start_date"]

        freight_df.drop(columns="flag", inplace=True)
        freight_df = freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        fix_outputs = {'(6-4)Outport Arbitrary Charge': {"Arbitrary Charges": arbitrary_df, "Freight": pd.DataFrame()},
                       '(6-1)Ocean Freight': {"Freight": freight_df}}

        return fix_outputs


class YML_FAKRates_v1(BaseTemplate):
    class FAKRates_Fix(BaseFix):
        def check_input(self):
            pass
        def check_output(self):
            pass

        def get_remarks(self):
            index = self.df[self.df.iloc[:,0].str.contains("SUBJ. TO", case=False, na=False)].index.values[0]
            remark_df = self.df.loc[index:].to_string(header= False, columns=None , index =False)
            return  remark_df

        def get_inclusions(self):
            def get_inclusion(data_input):
                return re.search(r"INCL\sPOL-(.*)", data_input)

            captured_data = self.df.iloc[:, 0].apply(lambda x: get_inclusion(str(x)))
            inclusions = ""
            for i in captured_data:
                if i:
                    inclusions = i.group(1)

            return inclusions

        def melt_load_type(self, df):
            if "load_type" not in df:
                df = df.melt(id_vars=[column for column in df.columns if not column[1].isdigit()],
                             var_name="load_type",
                             value_name="amount")

                return df

        def get_surcharges_(self):
            index = self.df[self.df.iloc[:,0].str.contains('ABOVE RATES', case=False, na=False)].index.values[0]

            surcharges_df = self.df.loc[index:]

            surcharges_dict = surcharges_df.to_dict("records")

            surcharges_lst = []
            for row in surcharges_dict:
                # check_pattern_1 = re.compile(r"\+.*No.(?P<code>\d+).*\((?P<charges>.*)\).(?P<currency>[A-Z]{3}).(?P<amt>\d+)\/(?P<load_type>\w+).\(>(?P<min>\d+).*\)")
                # check_pattern_2 = re.compile(r"\+.*No.(?P<code>\d+).*\((?P<charges>.*)\).(?P<currency>[A-Z]{3}).(?P<amt>\d+)\/(?P<load_type>\w+)")


                check_pattern_1 = re.compile(r".*(?P<charges>.*LOW SULPHUR.*).(?P<currency>[A-Z]{3}).(?P<amt>\d+)\/(?P<load_type>\w+)")
                check_pattern_2 = re.compile(r".*(?P<charges>.*BUNKER.*).(?P<currency>[A-Z]{3}).(?P<amt>\d+)\/(?P<load_type>\w+)")
                check_pattern_3 = re.compile(r".*(?P<charges>.*ADVANCE MANIFEST FEE.*).(?P<currency>[A-Z]{3}).(?P<amt>\d+)\/(?P<load_type>\w+)")
                check_pattern_4 = re.compile(r".*(?P<charges>.*EXPORT SERVICE FEE.*).(?P<currency>[A-Z]{3}).(?P<amt>\d+)\/(?P<load_type>\w+)")
                check_pattern_5 = re.compile(r".*(?P<charges>.*OVERWEIGHT SURCHARGE.*).(?P<currency>[A-Z]{3}).(?P<amt>\d+)\/(?P<load_type>\w+).\((?P<comparsion_sep>.)(?P<weight_from>\d+).(?P<unit>\w+).*\)")

                check_list =[check_pattern_1, check_pattern_2, check_pattern_3, check_pattern_4, check_pattern_5 ]
                for each_line in row[0].split("\n"):
                    for checks in check_list:
                        if checks.match(each_line):
                            surcharge_dict = {}
                            captured_data = re.search(checks, each_line)
                            surcharge_dict["charges"] = captured_data.group("charges")
                            surcharge_dict["currency"] = captured_data.group("currency")
                            surcharge_dict["amount"] = captured_data.group("amt")
                            surcharge_dict["load_type"] = captured_data.group("load_type")
                            surcharge_dict["remarks"] = each_line
                            if "weight_from" in captured_data.groupdict().keys():
                                weight_from = captured_data.group("weight_from")
                                if captured_data.group("comparsion_sep") == ">":
                                    weight_from = captured_data.group("weight_from") + ".1"
                                surcharge_dict["weight_from"] = weight_from


                            if "unit" in captured_data.groupdict().keys():
                                surcharge_dict["unit"] = captured_data.group("unit")
                            surcharges_lst.append(surcharge_dict)

            surcharges_df = pd.concat([pd.DataFrame(surcharges_lst)], ignore_index=True)

            surcharges_df.loc[surcharges_df["remarks"].str.contains("CHINA"), "destination_country"] = "CHINA"
            surcharges_df.loc[surcharges_df["remarks"].str.contains("JAPAN"), "destination_country" ] = "JAPAN"
            surcharges_df.loc[surcharges_df["remarks"].str.contains("JAPAN"), "destination_country" ] = "JAPAN"

            load_type = {"20DC": "20GP", "TEU" : "teu", "BL" : "perBL", "Teu" : "teu"}

            surcharges_df["load_type"] = surcharges_df["load_type"].replace(load_type , regex = True)

            return surcharges_df

        def get_surcharges(self):

            index = self.df[self.df.iloc[:,0].str.contains('ABOVE RATES', case=False, na=False)].index.values[0]
            # if self.df[0].str.contains('plus origin', case=False, na=False).any():
            #     end_index = self.df[self.df[0].str.contains('plus origin', case=False, na=False)].index.values[0]
            # else:
            #     end_index = self.df[self.df[0].str.startswith('Effective')].index.values[0]
            surcharges_df = self.df.loc[index:]
            # surcharges_df = surcharges_df.loc[
            #     ~surcharges_df.iloc[:, 0].str.contains("plus following surcharges", case=False, na=False)]
            surcharges_dict = surcharges_df.to_dict("records")
            surcharges_lst = []
            for row in surcharges_dict:

                check_pattern_1 = re.compile(r"\+\s?(?P<code>[0-9]{3})(?P<sucharges>(.*)[A-Z]{2}\))\s(?P<cur>\w+)\s(?P<amt>\d+)\/(?P<loadtype>\w+)(?P<rmks>.*)")
                check_pattern_2 = re.compile(r"\+\s?(?P<code>[0-9]{3})(?P<sucharges>(.*):)\s(?P<cur>[A-Z]{3})(?P<amt>[0-9]+)\/(?P<loadtype>[A-Z]+)\s(?P<rmks>.*)")
                # check_pattern_5 = re.compile(
                #     r"(?P<cr>[A-Z]{3})\s(?P<amt1>\d+),-\/(?P<amt2>\d+),-\/(?P<amt3>\d+),-.*(?P<rmks>\(.*)")
                # check_pattern_6 = re.compile(r"(?P<cr>[A-Z]{3})\s(?P<amt>\d+),-(?P<type>.*)(?P<rmks>\(.*)")
                # check_pattern_2 = re.compile(r"(?P<cr>[A-Z]{3})\s(?P<amt>\d+)\s\/.(?P<type>[A-z]{3})(?P<rmks>.*)")
                # check_pattern_7 = re.compile(r"(?P<cr>[A-Z]{3})\s(?P<amt>\d+)(?P<type>[A-Za-z \/]+)(?P<rmks>(.*))")
                # check_pattern_3 = re.compile(r"(?P<cr>[A-Z]{3})\s(?P<amt>\d+)\/(?P<type>.*)")
                # check_pattern_4 = re.compile(r"(?P<cr>[A-Z]{3})\s(?P<amt>\d+)(?P<type>.*)")
                if check_pattern_1.match(row[0]):
                    surcharge_dict = {}
                    captured_data = re.search(check_pattern_1, row[0])
                    surcharge_dict["charges"] = captured_data.group("sucharges")
                    surcharge_dict["code"] = captured_data.group("code")
                    #surcharge_dict["charges_"] = captured_data.group("cur")
                    surcharge_dict["currency"] = captured_data.group("cur")
                    surcharge_dict["amount"] = captured_data.group("amt")
                    surcharge_dict["load_type"] = captured_data.group("loadtype")
                    surcharge_dict["remarks"] = captured_data.group("rmks")

                    surcharges_lst.append(surcharge_dict)

                if check_pattern_2.match(row[0]):
                    surcharge_dict = {}
                    captured_data = re.search(check_pattern_2, row[0])
                    surcharge_dict["charges"] = captured_data.group("sucharges")
                    surcharge_dict["code"] = captured_data.group("code")
                    # surcharge_dict["charges_"] = captured_data.group("cur")
                    surcharge_dict["currency"] = captured_data.group("cur")
                    surcharge_dict["amount"] = captured_data.group("amt")
                    surcharge_dict["load_type"] = captured_data.group("loadtype")
                    surcharge_dict["remarks"] = captured_data.group("rmks")

                    surcharges_lst.append(surcharge_dict)

            surcharges_df = pd.concat([pd.DataFrame(surcharges_lst)], ignore_index=True)

            return surcharges_df

        def get_validality(self):
            def get_validity_date(date_input):
                # return re.search(r"VALIDITY:(.*)\/(.*)\s([0-9]{4})", date_input)
                return re.search(r"VALIDITY:(.*)", date_input)

            captured_validity = self.df.iloc[:, 2].apply(lambda x: get_validity_date(str(x)))
            start_date = ""
            expiry_date = ""
            for i in captured_validity:
                if i:
                    start_date_group = i.group(1).strip()
                    start_date = "01 "+ start_date_group
                    start_date = parse(start_date)
                    expiry_date_ = start_date_group.split( )
                    d = pd.date_range('{}-{}'.format(expiry_date_[0], expiry_date_[1]), periods=1, freq='M')
                    expiry_date = d[0]
            return start_date, expiry_date

        def get_light_weights(self):
            def get_light_weight(data_input):
                return re.search(r"(?P<loadtype>\d+')\s:.*<\s\s?(?P<wgt>[0-9]+)(?P<unit>.*)\s(.*)", data_input)

            captured_data = self.df.iloc[:, 5].apply(lambda x: get_light_weight(str(x)))
            light_weight = []
            for i in captured_data:
                if i:
                    light_weight_dict = {}
                    light_weight_dict["loadtype"] = i.group("loadtype")
                    light_weight_dict["weight"] = i.group("wgt")
                    light_weight_dict["unit"] = i.group("unit")
                    light_weight.append(light_weight_dict)

            return light_weight

        def apply_light_weight(self, freight_df, light_weights):
            dps = []
            for row in light_weights:
                loadtype = row["loadtype"].replace("20'","20GP").replace("40'","40GP")
                if loadtype in freight_df:
                    df = freight_df.copy()
                    if loadtype == "20GP":
                        df["40GP"] = ""
                    if loadtype == "40GP":
                        df["20GP"] = ""

                    df["weight_to"] = row["weight"]
                    df["unit"] = row["unit"]
                    dps.append(df)
            df = pd.concat(dps, ignore_index=True)
            return df

        def get_freight(self):
            df = self.df
            if len(df.columns) == 8:
                df.columns = ['destination_country', 'destination_port', 'origin_port', 'via_port', '20GP', '40GP', '20GP_TAD', '40GP_TAD']
                df = df.applymap(lambda x: nan if x == ':' or x == '' else x)
                df = df.dropna(axis=1, how='all')
                df.dropna(subset = ["destination_port"], inplace =True)
                df.loc[(df["20GP_TAD"].str.contains("incl.", case= False)) | (df["40GP_TAD"].str.contains("incl.", case = False)), "inclusions"] = "TAD"
                freight_df = df[['destination_country', 'destination_port', 'origin_port', 'via_port', '20GP', '40GP','inclusions']]
                freight_df = freight_df[freight_df["destination_port"] != "DISCHARGE PORT"]
                freight_df["40HC"] = freight_df["40GP"].copy()
                freight_df["charges"] = "Basic Ocean Freight"
                freight_df["destination_country"].fillna(method="ffill", inplace=True)
                freight_df['origin_port_ref'] = freight_df['origin_port'].copy()
                freight_df['origin_port'] = freight_df['origin_port'].str.split('/')
                freight_df = freight_df.explode('origin_port')
                freight_df = freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

                surcharges_df = df[['destination_country', 'destination_port', 'origin_port', 'via_port', '20GP_TAD', '40GP_TAD']]
                surcharges_df = surcharges_df[surcharges_df["destination_port"] != "DISCHARGE PORT"]
                surcharges_df = surcharges_df.loc[~surcharges_df["20GP_TAD"].str.contains("---", na=False)]
                surcharges_df = surcharges_df.loc[~surcharges_df["20GP_TAD"].str.contains("incl.", na=False)]

                surcharges_df.rename(columns={"20GP_TAD" : "20GP"}, inplace= True)

                surcharges_df["charges"] = "TAD"
                surcharges_df["40GP_TAD"] = surcharges_df["40GP_TAD"].astype(str)
                surcharges_df[["40GP", "40HC"]] = surcharges_df["40GP_TAD"].str.split(r'/', expand = True)
                surcharges_df["code"] = 229
                surcharges_df["destination_country"].fillna(method="ffill", inplace=True)
                surcharges_df['origin_port_ref'] = surcharges_df['origin_port'].copy()
                surcharges_df['origin_port'] = surcharges_df['origin_port'].str.split('/')
                surcharges_df = surcharges_df.explode('origin_port')

                surcharges_df['origin_port'] = surcharges_df['origin_port'].replace(port_pair_lookup, regex=True)

                surcharges_df.drop(columns = ["40GP_TAD"], inplace= True)
                surcharges_df["20GP"] = surcharges_df["20GP"].apply(lambda x: str(x).strip('$')).replace("None", "", regex=True)
                surcharges_df["40GP"] = surcharges_df["40GP"].apply(lambda x: str(x).strip('$')).replace("None", "", regex=True)
                surcharges_df["40HC"] = surcharges_df["40HC"].apply(lambda x: str(x).strip('$')).replace("None", "",regex=True)

                return freight_df, surcharges_df


            if len(df.columns) == 6:
                df.columns = ['destination_country', 'destination_port', 'origin_port', 'via_port', '20GP','40GP']
                df = df.applymap(lambda x: nan if x == ':' or x == '' else x)
                df = df.dropna(axis=1, how='all')
                df.dropna(subset = ["destination_port"], inplace =True)
                #freight_df = df.loc[(df["20GP_TAD"].str.contains("incl.", case= False)) | (df["40GP_TAD"].str.contains("incl.", case = False)), "inclusions"] = "TAD"
                freight_df = df[['destination_country', 'destination_port', 'origin_port', 'via_port', '20GP','40GP' ]]
                freight_df = freight_df[freight_df["destination_port"] != "DISCHARGE PORT"]
                if light_weights:
                    #custom method for Light weight apply
                    freight_df = self.apply_light_weight(freight_df, light_weights)

                freight_df["40HC"] = freight_df["40GP"].copy()
                freight_df["charges"] = "Basic Ocean Freight"
                freight_df["destination_country"].fillna(method="ffill", inplace=True)
                freight_df["remarks" ] = remarks
                freight_df['origin_port_ref'] = freight_df['origin_port'].copy()
                freight_df['origin_port'] = freight_df['origin_port'].str.split('/')
                freight_df = freight_df.explode('origin_port')
                freight_df = freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                return freight_df, pd.DataFrame()


        def capture(self):
            remarks = self.get_remarks()

            surcharges = self.get_surcharges()
            inclusions = self.get_inclusions()
            light_weights = self.get_light_weights()
            start_date, expiry_date = self.get_validality()
            freight_df, surcharges_df = self.get_freight()
            freight_df["start_date"], freight_df["expiry_date"] = start_date, expiry_date
            surcharges_df = pd.concat([surcharges_df, surcharges], ignore_index=True)
            surcharges_df["start_date"], surcharges_df["expiry_date"] = start_date, expiry_date
            self.captured_output = {'Freight': freight_df, "Surcharges": surcharges_df}

        def clean(self):
            self.cleaned_output = self.captured_output

    def resolve_dependency(cls, fix_outputs):
        if "FAK Rates" in fix_outputs:
            fakrate_df = fix_outputs.pop("FAK Rates")
            freight_df = fakrate_df["Freight"]
            freight_df["contract_number"] = "YANGCDE"
            freight_df["sub_vendor"] = "Yang Ming Shipping CORPORATION"

            surcharges_df = fakrate_df["Surcharges"]
            surcharges_df["contract_number"] = "YANGCDE"
            surcharges_df["sub_vendor"] = "Yang Ming Shipping CORPORATION"

        if "Light weight FAK" in fix_outputs:
            lightweightfak_df = fix_outputs.pop("Light weight FAK")
            lightweightfreight_df = lightweightfak_df["Freight"]
            freight_df = pd.concat([freight_df, lightweightfreight_df], ignore_index=True)

        fix_outputs =[{"Freight": freight_df,  "Charges": surcharges_df}]

        return fix_outputs


class KarlGrossYmlIntraEurope(BaseTemplate):

    class IneuFak(BaseDocxFix):

        def check_input(self):

            pass

        def get_validity(self):

            dates = {}
            month_dict = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6, 'July': 7
                          , 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12}
            regex = r"Rate validity\s:(.+?)</strong>"
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group = match.group(groupNum)
                    dates['start_date'] = re.sub(r'(\d{4})-(\d{1,2})-(\d{1,2})', '\\3-\\2-\\1', str(parse("01" + group).date()))
                    dates['expiry_date'] = re.sub(r'(\d{4})-(\d{1,2})-(\d{1,2})', '\\3-\\2-\\1', str(parse(str(calendar.monthrange(int(group.strip().split()[-1]), month_dict[group.strip().split()[0].title()])[-1]) + group).date()))

            return dates

        def get_freight_df(self):

            freight_df = self.df[0].copy(deep=True)
            freight_df.columns = freight_df.iloc[1, :]
            freight_df = freight_df.iloc[2:, :]
            digit_cols = [column for column in freight_df.columns if column[0].isdigit()]
            for column in digit_cols:
                if '/' in column:
                    freight_df['40GP'] = freight_df[column]
                    freight_df['40HC'] = freight_df[column]
                    freight_df.drop(columns=[column], inplace=True)

            freight_df.rename(columns={'COUNTRY': 'destination_country', 'DISCHARGE PORT': 'destination_port'
                                       , 'POL': 'origin_port', 'T/S': 'via', 'SERVICE': 'service_type'
                                       , '20DC': '20GP'}, inplace=True)
            freight_df['origin_port'] = freight_df['origin_port'].str.split('/')
            freight_df = freight_df.explode('origin_port')

            def get_symbol(price):
                pattern = r'(\D*)[\d\,\.]+(\D*)'
                g = re.match(pattern, price.strip()).groups()
                return (g[0] or g[1]).strip()

            freight_df['currency'] = freight_df['20GP'].apply(lambda x: get_symbol(x)).replace('$', 'USD').replace('€', 'EUR')
            freight_df['20GP'] = freight_df['20GP'].apply(lambda x: x.replace(get_symbol(x), '').replace('.', '').strip())
            freight_df['40GP'] = freight_df['40GP'].apply(lambda x: x.replace(get_symbol(x), '').replace('.', '').strip())
            freight_df['40HC'] = freight_df['40HC'].apply(lambda x: x.replace(get_symbol(x), '').replace('.', '').strip())
            freight_df['via'] = freight_df['via'].replace('direct', '')
            return freight_df

        def no_125(self):

            pod, amount, currency, load_type, charges = [], [], [], [], []
            regex = r"The rates(.+?)No 125"
            if re.search(regex, self.raw_html) is None:
                regex = r"The rates(.+?)DF"
            match_list = []
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    match_list.append(match.group(groupNum))

            for pick in match_list:
                pod.append(re.search(r"<strong>(.+?)</strong>", pick).group(1).strip())
                regex = r"Export Service Fee(.+?)</p>"
                if re.search(regex, pick) is None:
                    regex = r"Export Service Fee(.+?)</li>"
                    if re.search(regex, pick) is None:
                        regex = r"Export Service Fee(.+?)$"

                charges.append("Export Service Fee")
                amount.append(re.search(regex, pick).group(1).split()[-1].split('/')[0].replace(',', '.'))
                currency.append(re.search(regex, pick).group(1).split()[0])
                load_type.append(re.search(regex, pick).group(1).split()[-1].split('/')[-1].strip(','))

            no_125 = pd.DataFrame({'destination_country': pod, 'charges': charges, 'amount': amount
                                   , 'currency': currency, 'load_type': load_type})
            no_125['destination_country'] = no_125['destination_country'].str.split(', ')
            no_125 = no_125.explode('destination_country')
            no_125.reset_index(drop=True, inplace=True)
            return no_125

        def no_216(self):

            pod, amount, currency, load_type, charges, remarks = [], [], [], [], [], []
            regex = r"No 125(.+?)No 216"
            if re.search(regex, self.raw_html) is None:
                regex = r"DF/DTHC/ISPS(.+?)\(subj\."
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group = match.group(groupNum)
            pod.append("".join(re.findall(r"<strong>(.+?)</strong>", group)).strip())
            charges.append("Peak Season Surcharge")
            amount.append(re.search(r"surcharge =(.+?)o/b date", group).group(1).split()[-1].split('/')[0].replace(',', '.'))
            currency.append(re.search(r"surcharge =(.+?)o/b date", group).group(1).split()[0])
            load_type.append(re.search(r"surcharge =(.+?)o/b date", group).group(1).split()[-1].split('/')[-1].strip(','))
            if re.search(r"o/b date -(.+?)\(", group) is None:
                remarks.append(group.split('-')[-1].strip())
            else:
                remarks.append(re.search(r"o/b date -(.+?)\(", group).group(1).strip())
            no_216 = pd.DataFrame({'destination_country': pod, 'charges': charges, 'amount': amount
                                      , 'currency': currency, 'load_type': load_type, 'remarks': remarks})
            no_216.reset_index(drop=True, inplace=True)
            return no_216

        def no_205(self):

            amount, currency, load_type, charges, cargo_type = [], [], [], [], []
            regex = r"IMO additional:\s(.+?)\("
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group = match.group(groupNum)
            amount.append(group.split()[-1].split('/')[0].replace(',', '.'))
            currency.append(group.split()[0])
            load_type.append(group.split()[-1].split('/')[-1].strip())
            charges.append('IMO additional')
            cargo_type.append('ONLY')
            no_205 = pd.DataFrame({'charges': charges, 'amount': amount
                                      , 'currency': currency, 'load_type': load_type, 'cargo_type': cargo_type})
            return no_205

        def no_206(self):

            amount, currency, load_type, charges = [], [], [], []
            regex = r"IMO2020 Bunker\s(.+?)No 206"
            if re.search (regex, self.raw_html) is None:
                regex = r"IMO[0-9]{1,4}\sBunker\s(.+?)Low"
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group = match.group(groupNum).split('</strong>')[0]
            amount.append(group.split(':')[-1].strip().split()[-1].split('/')[0].replace(',', '.'))
            currency.append(group.split(':')[-1].strip().split()[0])
            load_type.append(group.split(':')[-1].strip().split()[-1].split('/')[-1])
            charges.append("IMO2020 Bunker")
            no_206 = pd.DataFrame({'charges': charges, 'amount': amount, 'currency': currency, 'load_type': load_type})
            return no_206

        def no_212(self):

            amount, currency, load_type, charges = [], [], [], []
            regex = r":\sUSD(.+?)Low sulphur"
            if re.search(regex, self.raw_html) is None:
                regex = r"Low sulphur(.+?)</strong>"
            matches = re.finditer(regex, self.raw_html, re.MULTILINE)
            for matchNum, match in enumerate(matches, start=1):
                for groupNum in range(0, len(match.groups())):
                    groupNum = groupNum + 1
                    group = match.group(groupNum).split('</strong>')[0]
            charges.append('Low sulphur')
            currency.append('USD')
            if "USD" not in group:
                amount.append(group.split('/')[0].replace(',', '.').strip())
            else:
                amount.append(group.split('/')[0].split(":")[-1].split()[-1].replace(',','.'))
            load_type.append(group.split('/')[-1])
            no_212 = pd.DataFrame({'charges': charges, 'amount': amount, 'currency': currency, 'load_type': load_type})
            return no_212

        def capture(self):

            dates = self.get_validity()
            freight_df = self.get_freight_df()
            freight_df['basis'] = 'CTR'
            freight_df['start_date'], freight_df['expiry_date'] = dates['start_date'], dates['expiry_date']
            no_125 = self.no_125()
            no_216 = self.no_216()
            no_205 = self.no_205()
            no_206 = self.no_206()
            no_212 = self.no_212()
            surcharge_df = pd.concat([no_125, no_216, no_205, no_206, no_212], ignore_index=True)
            surcharge_df['load_type'] = surcharge_df['load_type'].replace('Teu', 'TEU').replace('Cntr', 'container').replace('CTR', 'container').replace('BL', 'perBL').replace('B/L', 'perBL')
            freight_df['basis'] = freight_df['basis'].replace('CTR', 'container').replace('BL', 'perBL').replace('B/L', 'perBL')
            surcharge_df['start_date'], surcharge_df['expiry_date'] = dates['start_date'], dates['expiry_date']
            freight_df['contract_no'], freight_df['sub_vendor'] = "YANGCDE", "Yang Ming Shipping CORPORATION"
            surcharge_df['contract_no'], surcharge_df['sub_vendor'] = "YANGCDE", "Yang Ming Shipping CORPORATION"
            self.captured_output = {'Freight': freight_df, 'Charges': surcharge_df}
            return self.captured_output

        def clean(self):

            self.cleaned_output = self.captured_output
            return self.cleaned_output

        def check_output(self):

            pass


class Karl_gross_yml_far_east_pdf(BaseTemplate):
    class Fak_rate_fix(YML_FAKRates_v1.FAKRates_Fix):


        def get_freight(self):
            df = self.df
            if len(df.columns) == 9:
                df.columns = ['destination_country', 'destination_port', 'origin_port', 'via', '20GP', '40GP', '20GP_TAD', '40GP_TAD', "remarks"]
                df = df.applymap(lambda x: nan if x == ':' or x == '' else x)
                df = df.dropna(axis=1, how='all')
                df.dropna(subset = ["destination_port"], inplace =True)
                df.loc[(df["20GP_TAD"].str.contains("incl.", case=False)) | (df["40GP_TAD"].str.contains("incl.", case = False)), "inclusions"] = "TAD"
                freight_df = df[['destination_country', 'destination_port', 'origin_port', 'via', '20GP', '40GP', 'inclusions']]
                freight_df = freight_df[freight_df["destination_port"] != "DISCHARGE PORT"]
                freight_df["40HC"] = freight_df["40GP"].copy()
                freight_df["charges"] = "Basic Ocean Freight"
                freight_df["destination_country"].fillna(method="ffill", inplace=True)
                freight_df['origin_port_ref'] = freight_df['origin_port'].copy()
                freight_df['origin_port'] = freight_df['origin_port'].str.split('/')
                freight_df = freight_df.explode('origin_port')
                freight_df = freight_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

                surcharges_df = df[['destination_country', 'destination_port', 'origin_port', 'via', '20GP_TAD', '40GP_TAD']]
                surcharges_df = surcharges_df[surcharges_df["destination_port"] != "DISCHARGE PORT"]
                surcharges_df = surcharges_df.loc[~surcharges_df["20GP_TAD"].str.contains("---", na=False)]
                surcharges_df = surcharges_df.loc[~surcharges_df["20GP_TAD"].str.contains("incl.", na=False)]

                surcharges_df.rename(columns={"20GP_TAD" : "20GP"}, inplace= True)

                surcharges_df["charges"] = "TAD"
                surcharges_df["40GP_TAD"] = surcharges_df["40GP_TAD"].astype(str)
                surcharges_df[["40GP", "40HC"]] = surcharges_df["40GP_TAD"].str.split(r'/', expand = True)
                surcharges_df["code"] = 229
                surcharges_df["destination_country"].fillna(method="ffill", inplace=True)
                surcharges_df['origin_port_ref'] = surcharges_df['origin_port'].copy()
                surcharges_df['origin_port'] = surcharges_df['origin_port'].str.split('/')
                surcharges_df = surcharges_df.explode('origin_port')
                surcharges_df.drop(columns = ["40GP_TAD"], inplace= True)
                surcharges_df["20GP"] = surcharges_df["20GP"].apply(lambda x: str(x).strip('$')).replace("None", "", regex=True)
                surcharges_df["40GP"] = surcharges_df["40GP"].apply(lambda x: str(x).strip('$')).replace("None", "", regex=True)
                surcharges_df["40HC"] = surcharges_df["40HC"].apply(lambda x: str(x).strip('$')).replace("None", "",regex=True)

                return freight_df, surcharges_df


        def capture(self):
            remarks = self.get_remarks()

            #surcharges = self.get_surcharges()
            #inclusions = self.get_inclusions()
            #light_weights = self.get_light_weights()
            #start_date, expiry_date = self.get_validality()
            freight_df, surcharges_df = self.get_freight()
            #freight_df["start_date"], freight_df["expiry_date"] = start_date, expiry_date
            #surcharges_df = pd.concat([surcharges_df, surcharges], ignore_index=True)
            #surcharges_df["start_date"], surcharges_df["expiry_date"] = start_date, expiry_date
            self.captured_output = [{'Freight': freight_df, "Surcharges": surcharges_df}]

        def clean(self):
            self.cleaned_output = self.captured_output
