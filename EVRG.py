from logging import getLogger
from base import BaseTemplate, BaseFix
from custom_exceptions import InputValidationError
import pandas as pd
import re
import warnings
from numpy import nan
from dateutil.parser import parse
from datetime import  datetime
warnings.simplefilter(action='ignore', category=FutureWarning)
log = getLogger(__name__)


class EVRG_Excel_Asia(BaseTemplate):
    class _AsiaToUSA(BaseFix):

        def check_input(self):
            check_errors = []
            if not self.df[1].str.contains('Origins', na=False).any():
                check_errors.append("Origin column can not be found")
            if not self.df[2].str.contains('POD', na=False).any():
                check_errors.append("POD column can not be found")
            if check_errors:
                raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_freight_table(self):
            index_bullet = list(self.df[(self.df[0].str.contains(r"Rate\s?|RATE", na=False))].index)
            region = self.df.iloc[0, 0]
            index_bullet += list(self.df.tail(1).index.values + 1)
            freight_df_concat = pd.DataFrame()
            for index_ in range(len(index_bullet) - 1):
                bullet = self.df.iloc[index_bullet[index_], 0]
                bullet_df = self.df[index_bullet[index_]:index_bullet[index_ + 1]].copy(deep=True)
                bullet_df.reset_index(drop=True, inplace=True)

                """Check Notes"""
                notes = bullet_df.loc[bullet_df[1].str.contains('Note', na=False)][1].tolist()
                index_origins = list(bullet_df[(bullet_df[1].str.contains("Origins", na=False)) &
                                               (bullet_df[2].str.contains("POD", na=False))].index)
                index_origins += list(bullet_df.tail(1).index.values + 2)
                for index in range(len(index_origins) - 1):
                    comm_desc = bullet_df.iloc[1, 0].strip()
                    freight_df = bullet_df[index_origins[index]:index_origins[index + 1] - 1].copy(deep=True)
                    freight_df.reset_index(drop=True, inplace=True)
                    freight_df = freight_df.loc[~freight_df[1].str.contains(r"[A-Z]\..*\:", na=False)]
                    freight_df.drop(0, axis=1, inplace=True)
                    column_original = freight_df.iloc[0].to_list()
                    freight_df = freight_df[1:].copy()
                    # freight_df = freight_df.loc[:, :12].copy(deep=True)

                    if '4RD' in column_original:
                        freight_df = freight_df.loc[:, :10].copy(deep=True)
                        columns = ['origin_icd', 'destination_port', 'destination_icd', 'drop1', '40RE', 'drop2',
                                   'drop3', 'mode_of_transportation', 'remarks', 'AMD']
                        freight_df.columns = columns
                        freight_df = freight_df.drop(columns=['drop1', 'drop2', 'drop3'])

                    elif "40'RH DRY" in column_original:
                        freight_df = freight_df.loc[:, :12].copy(deep=True)
                        columns = ['origin_icd', 'destination_port', 'destination_icd', '20GP', '40GP', '40HC', '45HC',
                                   '40HR', 'mode_of_transportation', 'vessel', 'remarks', 'AMD']
                        freight_df.columns = columns
                        regex = r"(.+?)(\d.+?)$"
                        extracted = freight_df['vessel'].str.extract(regex)
                        freight_df['vessel'] = extracted[0]
                        freight_df['voyage'] = extracted[1]

                    else:
                        freight_df = freight_df.loc[:, :10].copy(deep=True)
                        columns = ['origin_icd', 'destination_port', 'destination_icd', '20GP', '40GP', '40HC',
                                   '45HC', 'mode_of_transportation', 'remarks', 'AMD']

                        freight_df.columns = columns
                        # freight_df = freight_df.drop(columns=['drop1', 'drop2'])

                    freight_df['bulletin'] = bullet
                    freight_df['commodity'] = comm_desc
                    freight_df['remarks'] = freight_df['remarks'] + '; '.join(notes)
                    freight_df_concat = pd.concat([freight_df_concat, freight_df], axis=0)

            freight_df_concat = freight_df_concat.loc[~((freight_df_concat['destination_port'] == "") &
                                                        (freight_df_concat['destination_icd'] == ''))]
            freight_df_concat['origin_icd'].replace('/', ';', inplace=True)
            freight_df_concat['charges_leg'] = 'L3'
            freight_df_concat['charges'] = 'basic ocean freight'
            freight_df_concat['region'] = region

            return freight_df_concat

        def get_arb_table(self):
            index_bullet = list(self.df[(self.df[0].str.contains(r"Rate\s?|RATE", na=False))].index)
            region = self.df.iloc[0, 0]
            index_bullet += list(self.df.tail(1).index.values + 1)
            arb_df_concat = pd.DataFrame()
            for index_ in range(len(index_bullet) - 1):
                bullet = self.df.iloc[index_bullet[index_], 0]
                bullet_df = self.df[index_bullet[index_]:index_bullet[index_ + 1]].copy(deep=True)
                bullet_df.reset_index(drop=True, inplace=True)
                index_origins = list(bullet_df[(bullet_df[1].str.contains("Origins", na=False)) &
                                               ~(bullet_df[2].str.contains("POD", na=False))].index)
                index_origins_fr = list(bullet_df[(bullet_df[1].str.contains("Origins", na=False)) &
                                                  (bullet_df[2].str.contains("POD", na=False))].index)
                index_sorted = sorted(list(bullet_df.tail(1).index.values + 2) + index_origins_fr + index_origins)
                if index_origins:
                    for index in range(len(index_origins)):
                        index_end = index_sorted.index(index_origins[index])
                        arb_df = bullet_df[index_origins[index]:index_sorted[index_end + 1] - 1].copy(deep=True)
                        arb_df.reset_index(drop=True, inplace=True)
                        arb_df = arb_df.loc[~arb_df[1].str.contains(r"[A-Z]\..*\:", na=False)]
                        arb_df.drop(0, axis=1, inplace=True)
                        column_original = arb_df.iloc[0].to_list()
                        arb_df = arb_df[1:].copy()

                        if 'Remark' in column_original:
                            arb_df = arb_df.loc[:, :7].copy(deep=True)
                            columns = ['icd', '20GP', '40GP', '40HC', 'mode_of_transportation', 'to', 'AMD']
                            arb_df.columns = columns
                        else:
                            arb_df = arb_df.loc[:, :5].copy(deep=True)
                            columns = ['icd', '20GP', '40GP', '40HC', 'mode_of_transportation']
                            arb_df.columns = columns
                            destinations = bullet_df.iloc[index_origins[index] - 1, 1]
                            dest_re = re.compile(r"on\s?top\s?of\s?the\s?(.+?)$")
                            to = dest_re.findall(destinations)[0]
                            to = to.replace(',', ';')
                            arb_df['to'] = to

                        arb_df['bulletin'] = bullet

                        arb_df = arb_df.loc[~((arb_df['20GP'] == "") &
                                              (arb_df['20GP'] == ''))]
                        arb_df['charges_leg'] = 'L2'
                        arb_df['charges'] = 'origin arbitrary charges'
                        arb_df['at'] = 'origin'
                        arb_df['to'] = arb_df['to'].replace('On Top of', '', regex=True).replace(
                            'Rate', '', regex=True).replace('/', ';', regex=True)
                        arb_df['loop'] = arb_df['icd'].str.extract(r'For(.+?)(service|only)', re.IGNORECASE)[0]
                        index_via = arb_df.loc[arb_df['icd'].str.contains(r'\(via', na=False)].index.to_list()
                        arb_df.loc[index_via, 'to_drop'] = arb_df.loc[index_via, 'icd'].str.extract(
                            r'\(via(.+?)(only|\)|$)')[0]

                        for index_ in index_via:
                            port_name = arb_df.loc[index_, 'to_drop'].strip()
                            regex = r".*(\w{2}" + port_name + r")"
                            if len(port_name) == 3:
                                match = re.findall(regex, arb_df.loc[index_]['to'].strip())[0]
                            else:
                                match = port_name
                            arb_df.loc[index_, 'to'] = match
                        arb_df.drop('to_drop', axis=1, inplace=True)
                        arb_df['icd'] = arb_df['icd'].str.replace(r'\((.+?)\)', '')
                        arb_df['region'] = region
                        arb_df_concat = pd.concat([arb_df_concat, arb_df], axis=0)
                else:
                    continue

            return arb_df_concat

        @staticmethod
        def format_output(df_freight, arb_df):
            output = {'Freight': df_freight, 'Arbitrary Charges': arb_df}
            return output

        def capture(self):
            freight_df = self.get_freight_table()
            arb_df = self.get_arb_table()
            self.captured_output = self.format_output(freight_df, arb_df)

        def clean(self):
            # freight_df = self.captured_output['Freight']
            self.cleaned_output = self.captured_output

    class _Notes(BaseFix):

        def check_input(self):
            # check_errors = []
            # if not self.df[0].str.contains('TRADE LANE', na=False).any():
            #     check_errors.append("Trade Line cannot be found on the first column, the input file "
            #                         "structure is different from sample template")
            # if check_errors:
            #     raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_notes(self):
            notes = {}
            notes_df = self.df.loc[:, :1].copy(deep=True)
            notes_df.replace('', nan, inplace=True)
            notes_df.dropna(axis=0, how='any', inplace=True)
            inclusion_index = notes_df.loc[notes_df[1].str.contains('included|inclusive', na=False)].index.to_list()
            inclusions = list(set(notes_df.loc[inclusion_index][1].str.extractall(r'\((.+?)\)')[0].to_list()))
            notes['inclusions'] = inclusions
            return notes

        def capture(self):
            self.captured_output = self.get_notes()

        def clean(self):
            self.cleaned_output = self.captured_output

    class _LookUp(BaseFix):

        def check_input(self):
            # check_errors = []
            # if not self.df[0].str.contains('TRADE LANE', na=False).any():
            #     check_errors.append("Trade Line cannot be found on the first column, the input file "
            #                         "structure is different from sample template")
            # if check_errors:
            #     raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def get_lookup(self):
            origin_group_dict = {}
            destination_group_dict = {}
            origin = {}
            destination = {}
            origin_index = self.df[(self.df[0].str.contains(r"Origins", na=False))].index.values[0]
            destination_index = self.df[(self.df[0].str.contains(r"Destinations", na=False))].index.values[0]
            origin_df = self.df.loc[origin_index: destination_index - 1].copy(deep=True)
            destination_df = self.df.loc[destination_index: self.df.tail(1).index.values[0]].copy(deep=True)
            origin_df.replace('', nan, inplace=True)
            origin_df.dropna(how='all', axis=0, inplace=True)
            origin_df.columns = origin_df.iloc[0]
            origin_df = origin_df[1:].copy()
            origin_df.drop(columns=['Origins', 'Countries', 'AMD'], inplace=True, axis=1)
            origin_df['Group Code'].fillna(method='ffill', inplace=True)
            origin_df.dropna(how='any', axis=0, inplace=True)
            origin_df['Group Code'] = origin_df['Group Code'].str.strip()
            origin_group = origin_df.groupby('Group Code', dropna=True)['Points'].apply(';'.join).reset_index()
            reefer = origin_group.loc[origin_group['Group Code'].str.contains('Reefer')].copy()
            reefer['Group Code'].replace(r'\((.+?)\)', '', inplace=True, regex=True)
            reefer['Group Code'] = reefer['Group Code'].str.strip()
            dry = origin_group.loc[~origin_group['Group Code'].str.contains('Reefer')].copy()
            dry['Group Code'].replace(r'\((.+?)\)', '', inplace=True, regex=True)
            dry['Group Code'] = dry['Group Code'].str.strip()

            origin_group_dict['dry'] = dry
            origin_group_dict['reefer'] = reefer

            origin_df.drop('Group Code', axis=1, inplace=True)

            destination_df.replace('', nan, inplace=True)
            destination_df.dropna(how='all', axis=0, inplace=True)
            destination_df.columns = destination_df.iloc[0]
            destination_df = destination_df[1:].copy()
            destination_df.drop(columns=['Destinations', 'Countries', 'AMD'], inplace=True, axis=1)
            destination_df['Group Code'].fillna(method='ffill', inplace=True)
            destination_df.dropna(how='any', axis=0, inplace=True)
            destination_df['Group Code'] = destination_df['Group Code'].str.strip()
            destination_group = destination_df.groupby('Group Code', dropna=True)['Points'].apply(
                ';'.join).reset_index()
            reefer = destination_group.loc[destination_group['Group Code'].str.contains('Reefer')].copy()
            reefer['Group Code'].replace(r'\((.+?)\)', '', inplace=True, regex=True)
            reefer['Group Code'] = reefer['Group Code'].str.strip()
            dry = destination_group.loc[~destination_group['Group Code'].str.contains('Reefer')].copy()
            dry['Group Code'].replace(r'\((.+?)\)', '', inplace=True, regex=True)
            dry['Group Code'] = dry['Group Code'].str.strip()

            destination_group_dict['dry'] = dry
            destination_group_dict['reefer'] = reefer

            destination_df.drop('Group Code', axis=1, inplace=True)

            return {'origin': {'port': origin_df, 'group': origin_group_dict},
                    'destination': {'port': destination_df, 'group': destination_group_dict}}

        def capture(self):
            self.captured_output = self.get_lookup()

        def clean(self):
            self.cleaned_output = self.captured_output

    class _Duration(BaseFix):

        def check_input(self):
            check_errors = []
            if not self.df[0].str.contains('Duration of Service Contract', na=False).any():
                check_errors.append("Duration of Service Contract is missing in the Duration sheet. cannot take date")
            if check_errors:
                raise InputValidationError(check_errors)
            pass

        def check_output(self):
            pass

        def capture(self):
            date = self.df.iloc[self.df.tail(1).index, 1].values[0]
            amd_no = self.df.iloc[self.df.tail(1).index, 0].values[0]
            self.captured_output = {'date': date, 'amd_no': amd_no}

        def clean(self):
            self.cleaned_output = self.captured_output

    @classmethod
    def resolve_dependency(cls, fix_outputs):

        """Start date and amd no"""
        date_amd = fix_outputs.pop('Duration')
        date = date_amd['date']
        amd_no = date_amd['amd_no']
        date_re = re.compile(r"(.+?)\s?through\s?(.+?)$")
        start_date = date_re.findall(date)[0][0]
        expiry_date = date_re.findall(date)[0][1]

        lookup = fix_outputs.pop('Scope')
        origin_lookup = lookup.pop('origin')
        destination_lookup = lookup.pop('destination')

        origin_code_dict = origin_lookup['port'].set_index('Codes').to_dict()['Points']
        destination_code_dict = destination_lookup['port'].set_index('Codes').to_dict()['Points']

        origin_grp_dry = origin_lookup['group']['dry'].set_index('Group Code').to_dict()['Points']
        destination_grp_dry = destination_lookup['group']['dry'].set_index('Group Code').to_dict()['Points']

        origin_grp_reefer = origin_lookup['group']['reefer'].set_index('Group Code').to_dict()['Points']
        destination_grp_reefer = destination_lookup['group']['reefer'].set_index('Group Code').to_dict()['Points']

        """Notes"""

        notes_usa = fix_outputs.pop('Notes-Asia to USA Cargo')
        notes_canada = fix_outputs.pop('Notes-Canada Cargo')

        sheets = ["E1-Asia to USA(General Cargo)", "E2-Asia to USA(Bullet Cargo)", "Rate-Asia to Canada"]
        df = {}
        for sheet in sheets:
            dict = fix_outputs.pop(sheet)
            df_freight = dict['Freight']
            df_freight.reset_index(drop=True, inplace=True)
            df_freight['start_date'] = start_date
            df_freight['expiry_date'] = expiry_date

            df_freight['origin_icd'].replace(origin_code_dict, inplace=True, regex=True)
            df_freight['destination_port'].replace(destination_code_dict, inplace=True, regex=True)

            if '40RE' in df_freight:
                index_reefer = df_freight.loc[~df_freight['40RE'].isna()].index.to_list()
                df_freight.loc[index_reefer, 'destination_port'] = df_freight.loc[
                    index_reefer, 'destination_port'].replace(destination_grp_reefer, regex=True)
                df_freight.loc[index_reefer, 'destination_icd'] = df_freight.loc[
                    index_reefer, 'destination_icd'].replace(destination_grp_reefer, regex=True)
                df_freight.loc[index_reefer, 'origin_icd'] = df_freight.loc[index_reefer, 'origin_icd'].replace(
                    origin_grp_reefer, regex=True)

                index_dry = df_freight.loc[df_freight['40RE'].isna()].index.to_list()
                df_freight.loc[index_dry, 'destination_port'] = df_freight.loc[
                    index_dry, 'destination_port'].replace(destination_grp_dry, regex=True)
                df_freight.loc[index_dry, 'destination_icd'] = df_freight.loc[
                    index_dry, 'destination_icd'].replace(destination_grp_dry, regex=True)
                df_freight.loc[index_dry, 'origin_icd'] = df_freight.loc[index_dry, 'origin_icd'].replace(
                    origin_grp_dry, regex=True)

            else:
                df_freight['destination_port'].replace(destination_grp_dry,
                                                       regex=True, inplace=True)
                df_freight['origin_icd'].replace(origin_grp_dry,
                                                 regex=True, inplace=True)

            df_freight['amendment_no'] = amd_no

            if sheet != 'Rate-Asia to Canada':
                df_freight['inclusions'] = ','.join(notes_usa['inclusions'])
            else:
                df_freight['inclusions'] = ','.join(notes_canada['inclusions'])

            df_freight['origin_icd'] = df_freight['origin_icd'].replace('/', ';', regex=True)
            df_freight['destination_port'] = df_freight['destination_port'].replace('/', ';', regex=True)
            df_freight['destination_icd'] = df_freight['destination_icd'].replace('/', ';', regex=True)

            """removinf AMD column"""
            df_freight.drop('AMD', inplace=True, axis=1)

            fix_outputs[sheet] = {'Freight': df_freight}
            if 'Arbitrary Charges' in dict:
                df_arb = dict['Arbitrary Charges']
                if not df_arb.empty:
                    df_arb['start_date'] = start_date
                    df_arb['expiry_date'] = expiry_date
                    df_arb['amendment_no'] = amd_no
                    df_arb['to'].replace(origin_code_dict, inplace=True, regex=True)
                    df_arb['to'].replace(destination_code_dict, inplace=True, regex=True)
                    df_arb['icd'] = df_arb['icd'].replace('/', ';', regex=True)
                    df_arb['loop'] = df_arb['loop'].replace(',', ';', regex=True)

                    df_arb.drop('AMD', inplace=True, axis=1)
                    fix_outputs[sheet] = {'Freight': df_freight, 'Arbitrary Charges': df_arb}

        return fix_outputs

class Expedoc_Evergreen_Excel(BaseTemplate):

    class _Index(BaseFix):

        def check_input(self):

            pass

        def check_output(self):

            pass

        def capture(self):

            if self.df[0].str.contains('SC Number:').any():
                contract_id_index = list(self.df[(self.df[0].str.contains('SC Number:'))].index)[0]
                contract_id = self.df.iloc[contract_id_index, 1]

            if self.df[1].str.contains('Asia to USA').any():
                trade_name = 'Asia to USA'

            self.captured_output = {'contract_id': contract_id, 'trade_name': trade_name}

            return self.captured_output

        def clean(self):

            self.cleaned_output = self.captured_output

            return self.cleaned_output

    class _Duration(BaseFix):

        def check_input(self):

            check_errors = []
            if not self.df[0].str.contains('Duration of Service Contract', na=False).any():
                check_errors.append("Duration of Service Contract is missing in the Duration sheet. cannot take date")
            if check_errors:
                raise InputValidationError(check_errors)

        def check_output(self):

            pass

        def capture(self):

            date = self.df.iloc[self.df.tail(1).index, 1].values[0]
            amd_no = self.df.iloc[self.df.tail(1).index, 0].values[0]
            self.captured_output = {'date': date, 'amd_no': amd_no}
            return self.captured_output

        def clean(self):

            self.cleaned_output = self.captured_output
            return self.cleaned_output

    class _Scope(EVRG_Excel_Asia._LookUp):
        pass

    class _Bullet_Cargo(BaseFix):

        def check_input(self):

            pass

        def check_output(self):

            pass

        def capture(self):

            rates_index = list(self.df[(self.df[0].str.contains(r"Rate\s?|RATE", na=False))].index)
            container = []
            for i in range(len(rates_index)):
                if 'container' in self.df.iloc[rates_index[i], 0]:
                    holder = self.df.iloc[rates_index[i], 0]
                    holder = holder.split(':')[-1].split()[0]
                    container.append(holder)
                else:
                    container.append('')

            commodity_list = []
            for i in range(len(rates_index)):
                if self.df.iloc[rates_index[i]+1, 0] != '':
                    commodity_list.append(self.df.iloc[rates_index[i]+1, 0])
                else:
                    commodity_list.append(self.df.iloc[rates_index[i]+1, 1])

            origins_index = list(self.df[(self.df[1].str.contains('Origins'))].index)

            charges_list = []
            for i in range(len(origins_index)):
                if i == 0:
                    holder = self.df.iloc[origins_index[i]:rates_index[i+1], 1:]
                    holder['commodity'] = commodity_list[i]
                    holder['container_owned'] = container[i]
                    holder.commodity.iloc[0] = 'commodity'
                    holder.container_owned.iloc[0] = 'container_owned'
                    charges_list.append(holder)
                if i+2 == len(origins_index):
                    holder = self.df.iloc[origins_index[-1]:, 1:]
                    holder['commodity'] = commodity_list[i+1]
                    holder['container_owned'] = container[i+1]
                    holder.commodity.iloc[0] = 'commodity'
                    holder.container_owned.iloc[0] = 'container_owned'
                    charges_list.append(holder)
                    break

            self.captured_output = {'charges_list': charges_list}
            return self.captured_output

        def clean(self):

            self.cleaned_output = self.captured_output

            return self.cleaned_output

    class _Notes(BaseFix):

        def check_input(self):

            pass

        def check_output(self):

            pass

        def capture(self):

            df = self.df

            if self.df[1].str.contains('Charge Code').any():
                charge_index = list(self.df[self.df[1].str.contains('Charge Code')].index)[0]

            if self.df[0].str.contains('Note').any():
                notes_index = list(self.df[self.df[0].str.contains('Note')].index)

            for element in notes_index:
                if element > charge_index:
                    charge_df = self.df.iloc[charge_index:element, 1:]
                    break

            charge_df = charge_df.loc[charge_df.loc[:, 5] != 0]
            charge_df = charge_df.loc[charge_df.loc[:, 5].str.lower() != 'tariff']

            charge_codes = df.iloc[notes_index[1]+1:notes_index[2], 1:3]

            self.captured_output = {'charge_df': charge_df, 'charge_codes': charge_codes}

            return self.captured_output

        def clean(self):

            self.cleaned_output = self.captured_output

            return self.cleaned_output

    @staticmethod
    def melt_load_type(df):
        if "load_type" not in df:
            df = df.melt(id_vars=[column for column in df.columns if not column[1].isdigit()],
                         var_name="load_type",
                         value_name="amount")
            return df

    def resolve_dependency(cls, fix_outputs):

        index = fix_outputs.pop('Index')
        duration = fix_outputs.pop('Duration')
        scope = fix_outputs.pop('Scope')
        bullet = fix_outputs.pop('E2-Asia to USA (Bullet cargo)')
        notes = fix_outputs.pop('Notes-Asia to USA Cargo')

        origin_dry = scope['origin']['group']['dry']
        origin_reefer = scope['origin']['group']['reefer']
        destination_dry = scope['destination']['group']['dry']
        destination_reefer = scope['destination']['group']['reefer']

        final_list = []
        for element in bullet['charges_list']:
            df = element.iloc[1:, :]
            df.columns = element.iloc[0, :]
            for i in range(len(df.iloc[:, 0])):

                if 'Reefer' in df.iloc[i, 0]:
                    origin_group = origin_reefer
                else:
                    origin_group = origin_dry

                if 'Reefer' in df.iloc[i, 1]:
                    destination_group = destination_reefer
                else:
                    destination_group = destination_dry

                if df.iloc[i, 0] not in list(origin_group['Group Code']):
                    origin_group = pd.DataFrame()
                    origin_group = origin_group.append({'Group Code': df.iloc[i, 0], 'Points': df.iloc[i, 0]}
                                                       , ignore_index=True)

                if df.iloc[i, 1] not in list(destination_group['Group Code']):
                    destination_group = pd.DataFrame()
                    destination_group = destination_group.append({'Group Code': df.iloc[i, 1], 'Points': df.iloc[i, 1]}
                                                                 , ignore_index=True )

                for j in range(len(origin_group['Group Code'])):
                    if df.iloc[i, 0] == origin_group['Group Code'].iloc[j]:
                        origin_ports = origin_group['Points'].iloc[j].split(';')
                        break

                for k in range(len(destination_group['Group Code'])):
                    if df.iloc[i, 1] == destination_group['Group Code'].iloc[k]:
                        destination_ports = destination_group['Points'].iloc[k].split(';')
                        break
                holder = df
                holder.reset_index(drop=True, inplace=True)
                for l in range(len(origin_ports)):
                    for m in range(len(destination_ports)):
                        holder.Origins.loc[0] = origin_ports[l]
                        holder.POD.loc[0] = destination_ports[m]
                        master = pd.concat([holder], ignore_index=True)
                        final_list.append(master)

        freight_df = pd.concat(final_list, ignore_index=True)
        freight_df.drop_duplicates(keep='first', inplace=True)
        freight_df.replace(nan, '', inplace=True)
        freight_df['destination_icd'] = freight_df.Destinations + freight_df.Destination
        freight_df.drop(['Destination', 'Destinations'], axis=1, inplace=True)
        freight_df.replace('', nan, inplace=True)
        freight_df.dropna(how='all', axis=1, inplace=True)
        cols = {'Origins': 'origin_port', 'POD': 'destination_port', 'destination_icd': 'destination_icd', '4RH': '40RH'
                , '4TK': '40TK', 'Service Mode': 'service_type', 'commodity': 'commodity', 'AMD': 'amendment_no'
                , 'container_owned': 'container_owned'}
        freight_df = freight_df.rename(columns=cols)
        freight_df['charges'] = 'basic ocean freight'
        freight_df['currency'] = 'USD'
        freight_df = cls.melt_load_type(freight_df)
        freight_df = freight_df.loc[freight_df['amount'].notna()]

        subcharges_df = notes['charge_df']
        subcharges_df.columns = subcharges_df.iloc[0, :]
        subcharges_df = subcharges_df.iloc[1:, :]
        cols = {'Charge Code': 'charges', '20\'': '20GP', '40\'': '40GP', '40\'HQ': '40HC', '40\'RH': '40RH'
                , '45\'HQ': '45HC', 'Effective': 'start_date', 'Expire': 'expiry_date', 'Remark': 'remarks'
                , 'AMD': 'amendment_no'}
        subcharges_df.rename(columns=cols, inplace=True)
        subcharges_df = cls.melt_load_type(subcharges_df)
        subcharges_df['currency'] = 'USD'
        subcharges_df = subcharges_df.to_dict('records')
        apply_charges_df = []
        for row in subcharges_df:
            filtered_freight = freight_df.loc[freight_df["load_type"].str.lower() == row["load_type"].lower()]
            filtered_freight["amount"] = row["amount"]
            filtered_freight["currency"] = row["currency"]
            filtered_freight["charges"] = row["charges"]
            filtered_freight["load_type"] = row["load_type"]
            apply_charges_df.append(filtered_freight)
            freight_with_charges_df = pd.concat(apply_charges_df, ignore_index=True, sort=False)
        freight_df = pd.concat([freight_df, freight_with_charges_df], ignore_index=True)

        port_codes_df = pd.concat([scope['origin']['port'], scope['destination']['port']], ignore_index=True)\
            .drop_duplicates()

        port_codes = {}
        for i in range(len(port_codes_df['Codes'])):
            port_codes[port_codes_df['Codes'].iloc[i]] = port_codes_df['Points'].iloc[i]

        charge_codes = {}
        for i in range(len(notes['charge_codes'].iloc[:, 0])):
            charge_codes[notes['charge_codes'].iloc[i, 0]] = notes['charge_codes'].iloc[i, 1]

        freight_df['amendment_no'] = duration['amd_no']
        freight_df['contract_start_date'], freight_df['contract_expiry_date'] \
            = parse(duration['date'].split('through')[0]), parse(duration['date'].split('through')[-1])
        freight_df['contract_id'], freight_df['trade_name'] = index['contract_id'], index['trade_name']
        freight_df['vendor'] = 'Evergreen'

        for code in port_codes:
            _code = (port_codes[code])
            freight_df.replace(code, _code, inplace=True, regex=True)

        for code in charge_codes:
            _code = (charge_codes[code])
            freight_df.replace(code, _code, inplace=True, regex=True)

        df_freight = {'Freight': freight_df}
        fix_outputs = {'Freight': df_freight}
        fix_outputs = [df_freight]

        return fix_outputs


class CEVA_EVERGREEN_USA(BaseTemplate):
    class USA_rates(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def get_freight_table(self):
            #index_bullet = list(self.df[(self.df[0].str.contains(r"Rate\s?|RATE", na=False))].index)
            index_bullet= list(self.df[(self.df[0].str.contains(r"Commodity", na=False))].index)
            rates=self.df
            rates.columns = self.df.iloc[3, :]
            #region = self.df.iloc[0, 0]
            rates= self.df.iloc[4:,:]
            rates.reset_index(drop=True,inplace=True)
            rates.drop(columns=['Remark','AMD'], inplace=True,axis=1)
            self.captured_output=rates
            return self.captured_output
        def capture(self):
            self.captured_output = self.get_freight_table()
        def clean(self):
            self.cleaned_output = self.captured_output

    class USA_notes(BaseFix):

        def check_input(self):

            pass

        def check_output(self):
            pass

        def get_notes(self):
            notes = self.df
            notes.replace("", nan, inplace=True)
            notes.columns=['Notes','Description','AMD']
            notes.drop(columns=['AMD'],axis=1,inplace=True)
            notes.drop(notes.index[[0, 1]], inplace=True)
            notes_group=notes.ffill()
            notes_group = notes_group.groupby('Notes')['Description'].apply(list).to_dict()

            self.captured_output =notes_group
            return self.captured_output

        def capture(self):
            self.captured_output = self.get_notes()

        def clean(self):
            self.cleaned_output = self.captured_output

    class _commodities(BaseFix):
        def check_input(self):
            pass
        def check_output(self):
            pass

        def capture(self):
            commodities = self.df
            commodities.replace("",nan,inplace=True)
            commodities_group=commodities.ffill()
            commodities_group.columns=['Groups',"Description"]
            commodities_group.dropna(inplace=True)
            commodities_group=commodities_group.groupby('Groups')['Description'].apply(list).to_dict()
            for key in commodities_group.keys():
                commodities_group[key]=";".join(commodities_group[key])
            self.captured_output = commodities_group
            return self.captured_output
        def clean(self):
            self.cleaned_output = self.captured_output

    class contract_duration(BaseFix):

        def check_input(self):
            check_errors = []
            if not self.df[0].str.contains('Duration of Service Contract', na=False).any():
                check_errors.append("Duration of Service Contract is missing in the Duration sheet. cannot take date")
            if check_errors:
                raise InputValidationError(check_errors)


        def check_output(self):
            pass

        def capture(self):
            date=[]
            contract_duration=self.df
            contract_duration.columns=['category','description']
            duration_start= contract_duration['description'][1].split('through')[0]
            duration_expiry= contract_duration['description'][1].split('through')[1]
            amd_no= contract_duration['description'][2]
            self.captured_output = {'duration_start': duration_start,"duration_expiry":duration_expiry,'amd_no': amd_no}
            return self.captured_output
        def clean(self):
            self.cleaned_output = self.captured_output

    class _scope(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass
        def lookup(self):
            origin_group_dict = {}
            destination_group_dict = {}
            origin = {}
            destination = {}
            origin_index = self.df[(self.df[0].str.contains(r"Origins", na=False))].index.values[0]
            destination_index = self.df[(self.df[0].str.contains(r"Destinations", na=False))].index.values[0]
            origin_df = self.df.loc[origin_index: destination_index - 1].copy(deep=True)
            destination_df = self.df.loc[destination_index: self.df.tail(1).index.values[0]].copy(deep=True)
            origin_df.replace('', nan, inplace=True)
            origin_df.dropna(how='all', axis=0, inplace=True)
            origin_df.columns = origin_df.iloc[0]
            origin_df = origin_df[1:].copy()
            origin_df.drop(columns=['Origins','AMD'], inplace=True, axis=1)
            origin_df['Countries'].fillna(method='ffill', inplace=True)
            origin_df.dropna(how='any', axis=0, inplace=True)
            origin_df['Countries','Code'] = origin_df['Countries'].str.strip()
            origin_df.drop(columns=['Code'], axis=1, inplace=True)
            origin_dict={}
            for index in origin_df.groupby('Countries', dropna=True)['Ports/Points'].apply(';'.join).index.values:
                origin_dict[index]=origin_df.groupby('Countries', dropna=True)['Ports/Points'].apply(';'.join)[index]

            destination_df.replace('', nan, inplace=True)
            destination_df.dropna(how='all', axis=0, inplace=True)
            destination_df.columns = destination_df.iloc[0]
            destination_df = destination_df[1:].copy()
            destination_df.drop(columns=['Destinations','AMD'], inplace=True, axis=1)
            destination_df['Countries'].fillna(method='ffill', inplace=True)
            destination_df.dropna(how='any', axis=0, inplace=True)
            destination_df['Countries'] = destination_df['Countries'].str.strip()
            destination_df.drop('Countries', axis=1, inplace=True)
            destination_dict = {}
            for index in destination_df.groupby('Code', dropna=True)['Ports/Points'].apply(
                ';'.join).index.values:
                destination_dict [index] = destination_df.groupby('Code', dropna=True)['Ports/Points'].apply(';'.join)[index]
            return {'origin_group':origin_dict,"destination_group":destination_dict,'origin':origin_df,'destination':destination_df}

        def capture(self):
            self.captured_output = self.lookup()
            return self.captured_output
        def clean(self):
            self.cleaned_output = self.captured_output

    class _gri(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):
            gri_df=self.df
            gri_df= self.df.loc[1:,:]

            self.captured_output = gri_df
            return self.captured_output

        def clean(self):
            self.cleaned_output = self.captured_output

    class USA_inland(BaseFix):
        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):
            clause=self.df

            clause_df= self.df.loc[2:,:]
            clause_dict=clause_df.to_dict()
            clause_dict["clause_name"]=clause_dict.pop(0)
            self.captured_output = clause_dict
            return self.captured_output

        def clean(self):
            self.cleaned_output = self.captured_output

    @classmethod
    class _Notes(BaseFix):

        def check_input(self):

            pass

        def check_output(self):

            pass

        def capture(self):

            df = self.df

            if self.df[1].str.contains('Charge Code').any():
                charge_index = list(self.df[self.df[1].str.contains('Charge Code')].index)[0]

            if self.df[0].str.contains('Note').any():
                notes_index = list(self.df[self.df[0].str.contains('Note')].index)

            for element in notes_index:
                if element > charge_index:
                    charge_df = self.df.iloc[charge_index:element, 1:]
                    break

            charge_df = charge_df.loc[charge_df.loc[:, 5] != 0]
            charge_df = charge_df.loc[charge_df.loc[:, 5].str.lower() != 'tariff']

            charge_codes = df.iloc[notes_index[1] + 1:notes_index[2], 1:3]

            self.captured_output = {'charge_df': charge_df, 'charge_codes': charge_codes}

            return self.captured_output

        def clean(self):

            self.cleaned_output = self.captured_output

            return self.cleaned_output

    def resolve_dependency(cls, fix_outputs):
        duration = fix_outputs.pop("(C) Duration")
        commodities=fix_outputs.pop("(B) Commodities for CARI")
        scope = fix_outputs.pop('(D) Scope')
        rates= fix_outputs.pop('(E1) USA to CARI Rates')
        notes = fix_outputs.pop('(F1) USA to CARI Notes')
        gri= fix_outputs.pop('(G) GRI')
        inland_clause=fix_outputs.pop('(K) USA Inland Clause')
                             #Amd No and contract date
        start_date = duration['duration_start']
        expiry_date= duration["duration_expiry"]
        amd_no = duration['amd_no']
       #scope_dict=scope['origin']['Countries']_map.set_index('Ports/Points')

        rates['Origin'] = rates['Origin'].map(scope['origin_group'])
        rates['POL']='' #origin and POl are same so via origin is removed
        rates['POD']='' # destination and POD are same so via destination is removed
        rates['start_date']=start_date
        rates['start_date'] =pd.to_datetime(rates['start_date'])
        rates['expiry_date']=expiry_date
        rates['expiry_date']=pd.to_datetime(rates['expiry_date'])
        #rates['amd_no']=amd_no       """use when the amd no is available"""
        rates['amd_no'] =""
        origin_code_dict=scope['origin_group']
        destination_code_dict = scope['destination_group']
        commodity_list=list(rates['Commodity'].unique())
                                 ##Rates mapping
        for key in commodity_list:
            rates['Commodity']= rates['Commodity'].map(commodities)
        df_freight=rates
        df_freight['currency']='USD'
        df_freight['charges_leg']='L3'
        df_freight['remarks']=''
        cols = {"Origin":'origin','Destination':'destination','POL':'via_origin','POD':'via_destination',"20'": '20GP',
                "40'": '40GP','40HQ': '40HC'}
        df_freight=df_freight.rename(columns=cols)
        fix_outputs={"Freight": {'Freight':df_freight}}
        return fix_outputs


class Flexport_EVRG_asia_to_usa(BaseTemplate):
    class Consolidated_cargo_rate_fix(BaseFix):
        def check_input(self):
            pass

        def get_surcharges(self):
            note2_surcharges = self.df.loc[self.df.iloc[:,0].str.contains("Note 2", na=False, case=False)].copy(deep=True).iloc[0][0]
            note2_surcharges_captured = re.search("(?P<charge>TTOC)*(?P<cr>[A-Z]{3})\s(?P<amt>\d+)\/(?P<rmks>.*)", note2_surcharges)
            note2_surcharge = {}
            note2_surcharge_list = []
            if note2_surcharges_captured:
                note2_surcharge["charges"] = "TTOC"
                note2_surcharge["currency"] = note2_surcharges_captured.group("cr")
                note2_surcharge["amount"] = note2_surcharges_captured.group("amt")
                note2_surcharge["remarks"] = note2_surcharges_captured.group("rmks")

            load_types= ["20GP", "40GP", "40HC", "40RE"]
            for load_type in load_types:
                note2_surchrges = {}
                note2_surchrges["load_type"] = load_type
                note2_surchrges["charges"] = "TTOC"
                note2_surchrges["currency"] = note2_surcharges_captured.group("cr")
                note2_surchrges["amount"] = note2_surcharges_captured.group("amt")
                note2_surchrges["remarks"] = note2_surcharges_captured.group("rmks")
                note2_surcharge_list.append(note2_surchrges)

            return note2_surcharge_list

        # def get_commodity(self):
        #     'Note 3: Alameda Corridor Charge (ACC) is included.'
        #     commodity = self.df.loc[self.df.iloc[:, 0]]
        #     note3_inclusions_captured = re.search("Note 3:(.*)", commodity)
        #     inclusions = ""
        #     if note3_inclusions_captured:
        #         inclusions = note3_inclusions_captured.group(1).replace("is included.", "").strip()
        #
        #     return inclusions

        def get_inclusions(self):
            'Note 3: Alameda Corridor Charge (ACC) is included.'
            note3_inclusions = self.df.loc[self.df.iloc[:,0].str.contains("Note 3", na=False, case=False)].copy(deep=True).iloc[0][0]
            note3_inclusions_captured = re.search("Note 3:(.*)", note3_inclusions)
            inclusions = ""
            if note3_inclusions_captured:
                inclusions = note3_inclusions_captured.group(1).replace("is included.", "").strip().split("(")[0].strip()

            return inclusions






        def get_subject_to(self):
            'Note 4: Empty pick-up cost (E24M) is not applicable for below depots  " TWKLGR/TWKLGK/TWKLGL/TWKSGK/TWTCGF"'
            note4_subject_to = self.df.loc[self.df.iloc[:,0].str.contains("Note 4", na=False, case=False)].copy(deep=True).iloc[0][0]
            note4_subject_to_captured = re.search('(?P<charges>Empty pick-up).*"(.*)"', note4_subject_to)
            note4_subject_to_dict = {}
            note4_subject_to_list = []
            if note4_subject_to_captured:
                note4_subject_to_dict["subject_to"] = note4_subject_to_captured.group(1).strip()
                note4_subject_to_dict["origin_port"] = note4_subject_to_captured.group(2).strip().replace("/", ";")
                note4_subject_to_list= []
                for port in note4_subject_to_dict["origin_port"].split(";"):
                    note4_subject_to_ = {}
                    note4_subject_to_["subject_to"] = note4_subject_to_dict["subject_to"]
                    note4_subject_to_["origin_port"] = port.replace("TWTCGF", "TAICHUNG").replace("TWKSGK", "KAOHSIUNG")
                    note4_subject_to_list.append(note4_subject_to_)

            return note4_subject_to_list



        def capture(self):
            captured_freight_df = self.df
            surcharges = self.get_surcharges()
            inclusions = self.get_inclusions()
            subject_to = self.get_subject_to()
            nan_value = float("NaN")
            captured_freight_df.replace("", nan_value, inplace=True)
            captured_freight_df.dropna(subset=['destination_port', '20GP', '40HC'], inplace=True)
            captured_freight_df = captured_freight_df.loc[captured_freight_df["origin_icd"].str.lower()!= "origins"]
            captured_freight_df["amendment_no"].fillna(method='ffill', inplace=True)
            # captured_freight_df.loc[captured_freight_df["origin_port"] == captured_freight_df["origin_icd"], "origin_port"] = ""
            # captured_freight_df.loc[captured_freight_df["destination_port"] == captured_freight_df["destination_icd"], "destination_port"] = ""
            captured_freight_df["inclusions"] = inclusions

            try:
                for surcharge in surcharges:
                    captured_freight_df.loc[captured_freight_df[surcharge["load_type"]].notna(), surcharge["load_type"] + "_" + surcharge["charges"]] = surcharge["amount"]

                for row in subject_to:
                    captured_freight_df.loc[captured_freight_df["origin_port"].str.contains(row["origin_port"]), "inclusions"] +=", "  + row["subject_to"]
            except:
                pass

            if "service_type" in captured_freight_df:
                captured_freight_df.loc[captured_freight_df["service_type"].str.contains(r"/R", case=False, na=False), "mode_of_transportation_destination"] = "R"

            self.captured_output = {"Freight": captured_freight_df}


        def clean(self):

            clean_freight_df = self.captured_output["Freight"]
            clean_freight_df["charges"], clean_freight_df["charges_leg"] = "Basic Ocean Freight", "L3"

            self.cleaned_output = {"Freight": clean_freight_df}

        def check_output(self):
            pass


    class Duration_fix(BaseFix):
        def check_input(self):
            pass

        def capture_amendment(self):
            amendment_indexes = self.df[(self.df.iloc[:, 0].str.contains("Amendment", na=False, case=False))].index[0]
            amendment = self.df.iloc[amendment_indexes+1][0]
            return amendment

        def capture_validity(self):
            validity_indexes = self.df[(self.df.iloc[:, 1].str.contains("Effective Date", na=False, case=False))].index[0]
            validity = self.df.iloc[validity_indexes+1][1]
            validity_ = validity.split("through")
            start_date = datetime.strptime(validity_[0].strip(), "%Y%m%d")
            expiry_date = datetime.strptime(validity_[1].strip(), "%Y%m%d")
            return start_date, expiry_date

        def capture(self):

            amendement_no = self.capture_amendment()
            start_date, expiry_date = self.capture_validity()
            duration_ = {}
            duration_["amendement_no"], duration_["start_date"], duration_["expiry_date"] = amendement_no, start_date, expiry_date
            self.captured_output = {"duration": duration_}

        def clean(self):
            self.cleaned_output = self.captured_output

        def check_output(self):
            pass


    class Index_fix(BaseFix):
        def check_input(self):
            pass

        def get_sc_number(self):
            sc_number_index = self.df[(self.df.iloc[:, 0].str.contains("SC Number", na=False, case=False))].index[0]
            sc_number = self.df.iloc[sc_number_index][1]
            return sc_number

        def capture(self):

            sc_number = self.get_sc_number()
            self.captured_output = {"index" : sc_number}

        def clean(self):
            self.cleaned_output = self.captured_output

        def check_output(self):
            pass


    class General_note_fix(BaseFix):
        def check_input(self):
            pass

        def get_inclusions(self,general_note_df):

            inclusions = general_note_df.loc[general_note_df["include"].str.contains("V", na=False, case=False)]
            incl = ""
            if not inclusions.empty:
                incl = inclusions["full_name"].drop_duplicates().to_string(index=False, header=False).strip().replace("\n", ",")
            return incl

        def get_subject_to(self, general_note_df):

            subject_to = general_note_df.loc[general_note_df["exclude"].str.contains("V", na=False, case=False)]
            incl = ""
            if not subject_to.empty:
                incl = subject_to["full_name"].drop_duplicates().to_string(index=False, header=False).strip().replace("\n", ",")
            return incl

        def capture(self):

            general_note_df = self.df
            nan_value = float("NaN")
            general_note_df.replace("", nan_value, inplace=True)

            general_note_df.dropna(subset=['full_name', 'item'], inplace=True)

            inclusions = self.get_inclusions(general_note_df)
            subject_to = self.get_subject_to(general_note_df)

            general_notes = {"inclusions": inclusions, "subject_to": subject_to}

            self.captured_output = {"general_note": general_notes}


        def clean(self):
            self.cleaned_output = self.captured_output

        def check_output(self):
            pass

    class Commodity_fix(BaseFix):
        def check_input(self):
            pass

        def get_commodity(self):
            commodity = ""
            commodity_index = self.df[(self.df.iloc[:, 4].str.contains("Commodity", na=False, case=False))].index[0]
            commodity = self.df.iloc[commodity_index+1][4]
            return commodity

        def capture(self):
            commodity = self.get_commodity()
            self.captured_output = {"Commodity": commodity}

        def clean(self):
            self.cleaned_output = self.captured_output

        def check_output(self):
            pass

    def resolve_dependency(cls, fix_outputs):

        freight = fix_outputs.pop("Consolidated Cargo Rate")
        freight_df = freight["Freight"]
        duration_dict = fix_outputs.pop("Duration")
        duration_df = duration_dict["duration"]
        freight_df["amendement_no"], freight_df["start_date"], freight_df["expiry_date"] = duration_df["amendement_no"], duration_df["start_date"], duration_df["expiry_date"]

        index_dict = fix_outputs.pop("Index")
        freight_df["contract_no"] = index_dict["index"]


        commodity_dict = fix_outputs.pop("Commodity")
        freight_df["commodity"] = commodity_dict["Commodity"]



        general_dict = fix_outputs.pop("General Note")

        # if "subject_to" in freight_df:
        #     freight_df.loc[freight_df["subject_to"].isna(), "subject_to"] = general_dict["general_note"]["subject_to"]
        #     freight_df.loc[freight_df["subject_to"].notna(), "subject_to"] += ", "+general_dict["general_note"]["subject_to"]

        if "inclusions" in freight_df:
            freight_df.loc[freight_df["inclusions"].isna(), "inclusions"] = general_dict["general_note"]["inclusions"]
            freight_df.loc[freight_df["inclusions"].notna(), "inclusions"] += ", "+general_dict["general_note"]["inclusions"]

        # inclusions = freight_df["inclusions"].drop_duplicates().to_string(index=False, header =False).split(",")
        # subject_to = freight_df["subject_to"].drop_duplicates().to_string(index=False, header =False).split(",")
        #
        # for incl in inclusions:
        #     if incl.lower() in subject_to:
        #
        #
        #
        # for inclusion in inclusions:
        freight_df["currency"] = "USD"

        fix_outputs = {"Freight": freight_df}


        return  fix_outputs

