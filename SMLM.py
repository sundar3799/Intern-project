import datetime
import re
from collections import defaultdict
from itertools import product
from logging import getLogger

from numpy import nan
from pandas import concat, merge

from base import BaseTemplate, BaseFix
from custom_exceptions import InputValidationError

log = getLogger(__name__)

class SM_Excel(BaseTemplate):
    class _SM_v1(BaseFix):

        def __init__(self, df, file_model: dict, headers_config: dict):
            super().__init__(df, file_model, headers_config)

            self.validity = {}
            self.load_type_map = {}

        def check_input(self):
            pass
            check_errors = []
            if not self.df[0].str.contains('6-1').any():
                check_errors.append("Section definition '6-1' should be present in first Column")

            if not (self.df[0].str.startswith('[') & self.df[0].str.endswith(']')).any():
                check_errors.append("Region definition should be present in first column and should be enclosed in []")

            # if not self.df[0].str.contains('Code').any():
            #     check_errors.append("Load Type mapping table should be defined along first column under section "
            #                         "header 'Code'")

            if check_errors:
                raise InputValidationError(check_errors)

        def check_output(self):
            pass

        def get_sections(self):
            sections_to_check = ['6-1', '6-2', '12. PROVISIONS/NOTES/EXCEPTIONS:']
            sections = {}

            previous_section = None
            for check in sections_to_check:
                if self.df[self.df[0].str.startswith(check, na=False)].index.values:
                    index = self.df[self.df[0].str.contains(check, na=False)].index.values[0]
                    sections[check] = {'start': index, 'end': None}

                    if previous_section:
                        sections[previous_section]['end'] = index

                    previous_section = check
                else:
                    sections[check] = None

            for drop in ['6-3', '6-4', '12. PROVISIONS/NOTES/EXCEPTIONS:', '6-5', '7. LIQUIDATED']:
                if drop in sections:
                    sections.pop(drop)

            return sections

        def set_validity_from_section_8(self):

            if self.df[self.df[0] == '8. DURATION OF THE CONTRACT'].index.values:
                start_index = self.df[self.df[0] == '8. DURATION OF THE CONTRACT'].index.values[0]
                end_index = self.df[self.df[0].str.startswith("9. SIGNATURE", na=False)].index.values[0]

                validity_df = self.df.loc[start_index: end_index - 1, :]
                validity_df = validity_df.applymap(lambda x: nan if x == '' else x)
                validity_df = validity_df.dropna(axis=1, how='all')
                validity_df = validity_df.reset_index(drop=True)
                validity_df = validity_df.T.reset_index(drop=True).T

                self.validity['all'] = {
                    'start_date': datetime.datetime.strptime(validity_df[1][1], "%d %b, %Y").date().isoformat(),
                    'expiry_date': datetime.datetime.strptime(validity_df[3][1], "%d %b, %Y").date().isoformat()}

                region_indexes = validity_df[
                    validity_df[0].str.startswith('[', na=False) & validity_df[0].str.endswith(']',
                                                                                               na=False)].index.tolist()

                for index in region_indexes:
                    self.validity[validity_df[0][index][1:-1]] = {
                        'start_date': datetime.datetime.strptime(validity_df[1][index + 1],
                                                                 "%d %b, %Y").date().isoformat(),
                        'expiry_date': datetime.datetime.strptime(validity_df[3][index + 1],
                                                                  "%d %b, %Y").date().isoformat()}

        # def set_load_type_map(self):
        #     start_index = self.df[self.df[0] == 'Code'].index.values[0]
        #
        #     load_df = self.df.loc[start_index + 1:, :]
        #     load_df = load_df.applymap(lambda x: nan if x == '' else x)
        #     load_df = load_df.dropna(axis=1, how='all')
        #     load_df = load_df.reset_index(drop=True)
        #     load_df = load_df.T.reset_index(drop=True).T
        #     self.load_type_map = load_df.set_index(0).to_dict()[1]

        @classmethod
        def remove_empty_columns(cls, df):
            df = df.applymap(lambda x: nan if x == '' else x)
            df = df.dropna(axis=1, how="all")
            df = df.reset_index(drop=True)
            df = df.fillna('')
            return df

        @classmethod
        def get_regional_sections(cls, df, section_end):
            regional_sections = {}
            indexes = df[df[0].str.startswith('[', na=False) & df[0].str.endswith(']', na=False)].index.tolist()
            indexes.append(section_end + 1)
            indexes = zip(indexes, indexes[1:])

            for config in indexes:
                region = df[0][config[0]]
                regional_sections[region] = {'start': config[0], 'end': config[1]}

            return regional_sections

        def get_commodity_desc(self, freight_df):
            if self.df[self.df[0] == '3. COMMODITIES :'].index.values:
                end_index = \
                    self.df[self.df[0].str.startswith("4. MINIMUM VOLUME COMMITMENT('MVC')", na=False)].index.values[0]
                start_index = self.df[self.df[0].str.startswith("3. COMMODITIES :", na=False)].index.values[0]

                comm_desc_df = self.df.loc[start_index + 1: end_index - 1]
                comm_desc_df = comm_desc_df.applymap(lambda x: nan if x == '' else x)
                comm_desc_df = comm_desc_df.dropna(axis=1, how='all')
                comm_desc_df = comm_desc_df.reset_index(drop=True)

                indexes = comm_desc_df[comm_desc_df[0].str.startswith('[', na=False) &
                                       comm_desc_df[0].str.endswith(']', na=False)].index.tolist()
                indexes += [comm_desc_df.tail(1).index.values[0] + 1]

                comm_desc_dict = {}
                for index in range(len(indexes) - 1):
                    comm_group = comm_desc_df.loc[indexes[index] + 1:indexes[index + 1]].copy(deep=True)
                    comm_group_list = comm_group[0].to_list()
                    group = comm_desc_df.iloc[indexes[index], 0][1:-1]
                    comm_desc_dict[group] = \
                        dict(zip(comm_group_list[::2], comm_group_list[1::2]))
                for region in comm_desc_dict:
                    desc_dict = comm_desc_dict[region]
                    for desc in desc_dict:
                        # freight_df.loc[(freight_df['region'].str.contains(region)) &
                        #                (freight_df['commodity'].str.contains(desc)), 'commodity'] = desc_dict[desc]
                        index_region = freight_df.loc[(freight_df['region'].str.contains(region))].index.tolist()
                        freight_df.loc[index_region, 'commodity'] = \
                            freight_df.loc[index_region, 'commodity'].replace(desc, desc_dict[desc], regex=True)
                freight_df['commodity'] = freight_df['commodity'].replace("(SEE TERM 3. GROUP CODE DETAIL CODES)", '',
                                                                          regex=True)
                return freight_df

        @classmethod
        def get_validity_and_remarks(cls, block, notes_index):
            bl_dict = voyage_dict = {}
            notes_df = block.loc[notes_index].copy()
            notes_df.replace('', nan, inplace=True)
            notes_df.dropna(how='all', axis=1, inplace=True)
            notes_df['Notes No'] = notes_df[0].str.extract(r'(\d)')
            notes_df.drop(0, axis=1, inplace=True)
            notes_df = notes_df.groupby(['Notes No'], as_index=False)[2].apply(lambda x: '\n'.join(x)).reset_index(
                drop=True)
            if notes_df[2].str.contains('specified B/Ls only', na=False).any():
                notes_df[2] = notes_df[2].replace('\n', '; ', regex=True)
                notes_df['bl'] = notes_df[2].str.extract('specified B/Ls only :(.+?)$', re.MULTILINE)
                notes_df['bl'] = notes_df['bl'].str.strip()
                bl_dict = dict(zip(notes_df['Notes No'].tolist(), notes_df['bl'].tolist()))
            if notes_df[2].str.contains('applicable for the VVD', na=False).any():
                notes_df['voyage'] = notes_df[2].str.extract('applicable for the VVD "(.+?)"', re.MULTILINE)
                notes_df['voyage'] = notes_df['voyage'].str.strip()
                voyage_dict = dict(zip(notes_df['Notes No'].tolist(), notes_df['voyage'].tolist()))
            notes_dict = dict(zip(notes_df['Notes No'].tolist(), notes_df[2].tolist()))
            return notes_dict, bl_dict, voyage_dict

        # @classmethod
        # def get_validity_and_remarks_bullet(cls, block, notes_index):
        #     notes_df = block.loc[notes_index + 1:block.tail(1).index.values[0]].copy()
        #     notes_df.replace('', nan, inplace=True)
        #     notes_df.dropna(how='all', inplace=True, axis=1)
        #     notes_list = notes_df[0].to_list()
        #     pss_charge = defaultdict(list)
        #     loop_vessel = []
        #     for note in notes_list:
        #         note = note.replace('\n', '')
        #         if re.search('PSC will be applied as per tariff', note) or re.search(r"IPI\s?(\(CY\)\s:|:)", note):
        #             if re.search('IPI', note):
        #                 # regex = r'IPI\s?:\s?USD\s?(.+?)\s?per\s?(.+?)(\d\)|$)'
        #                 regex = r'IPI\s?(\(CY\)\s:|:)\s?USD\s?(.+?)\s?per\s?(.+?)(\d\)|$)'
        #                 matches = re.finditer(regex, note, re.MULTILINE)
        #                 for matchNum, match in enumerate(matches, start=1):
        #                     rate = match.group(2)
        #                     cnt_type = match.group(3)
        #                 pss_charge['IPI'] = {rate: cnt_type}
        #                 note = re.sub(regex, '', note)
        #             if re.search('CY', note):
        #                 regex = r'(\(CY\)\s:|CY\s?:)\s?USD\s?(.+?)\s?per\s?(.+?)(\d\)|$)'
        #                 matches = re.finditer(regex, note, re.MULTILINE)
        #                 for matchNum, match in enumerate(matches, start=1):
        #                     rate = match.group(2)
        #                     cnt_type = match.group(3)
        #                 pss_charge['CY'] = {rate: cnt_type}
        #             if re.search('Door delivery points', note):
        #                 regex = r'delivery points\s?:\s?USD\s?(.+?)\s?per\s?(.+?)(\d\)|$)'
        #                 matches = re.finditer(regex, note, re.MULTILINE)
        #                 for matchNum, match in enumerate(matches, start=1):
        #                     rate = match.group(1)
        #                     cnt_type = match.group(2)
        #                 pss_charge['SD'] = {rate: cnt_type}
        #         elif re.search('specified lane/VVD only', note):
        #             regex = r"specified lane/VVD only\s?:\s?(.+?)/(.+?)(\.|$)"
        #             matches = re.finditer(regex, note, re.MULTILINE)
        #             for matchNum, match in enumerate(matches, start=1):
        #                 loop_vessel.append(match.group(1))
        #                 loop_vessel.append(match.group(2))
        #     notes = "\n".join(notes_list)
        #     return notes, pss_charge, loop_vessel

        @classmethod
        def fix_commodity_block(cls, block):
            block.reset_index(drop=True, inplace=True)
            block = block.applymap(lambda x: nan if x == ':' or x == '' else x)
            block = block.dropna(axis=1, how='all')
            block = block.fillna('')
            block = block.T.reset_index(drop=True).T

            if len(block.columns) >= 15:
                block[2] = block[2] + block[3]
                block = block.drop(columns=[3])
                block = block.T.reset_index(drop=True).T

            if block[2].values[0] == '':
                commodity = block[3].values[0]
            else:
                commodity = block[2].values[0]

            if block[1].str.contains('ACTUAL CUSTOMER').any():
                index = block[(block[1].str.contains('ORIGIN', na=False))].index.values[0]
                start_index = block[(block[1].str.contains('ACTUAL CUSTOMER', na=False))].index.values[0]

                if (index - start_index) == 1:
                    customer_name = block[2].values[1]
                else:
                    customer_name = block[2][start_index:index].values.tolist()
            else:
                customer_name = ''

            # bulletin = block[0].values[0]

            start_date = expiry_date = note_included = note_not_included = service = notes = None
            notes_dict = pss_charge = {}
            loop_vessel = []

            if block[0].str.contains('NOTE', regex=True).any():
                index_of_notes = block[block[0].str.contains('NOTE', regex=True)].index.values
                notes_dict, bl, voyage_dict = cls.get_validity_and_remarks(block, index_of_notes)
                origin_indexes = block[block[1] == 'ORIGIN'].index.tolist()
                origin_indexes.append(block.index.values[-1] + 1)
            if block[0].str.contains('< Note for Bullet').any():
                index_of_notes = block[block[0].str.contains('< Note for Bullet')].index.values[0]
                # notes, pss_charge, loop_vessel = cls.get_validity_and_remarks_bullet(block, index_of_notes)
                origin_indexes = block[block[1] == 'ORIGIN'].index.tolist()
                origin_indexes.append(index_of_notes)
            else:
                notes = ''
                origin_indexes = block[block[1] == 'ORIGIN'].index.tolist()
                origin_indexes.append(block.index.values[-1] + 1)

            origin_config = zip(origin_indexes, origin_indexes[1:])

            dfs = []
            for config in origin_config:
                origin_block = block.loc[config[0]:config[1] - 1, :]
                origin_block = origin_block.applymap(lambda x: nan if x == '' else x)
                origin_block = origin_block.dropna(axis=1, how='all')
                origin_block = origin_block.fillna('')

                if origin_block.loc[origin_block[1] == 'ORIGIN', 2].values[0] != '':
                    origin = origin_block.loc[origin_block[1] == 'ORIGIN', 2].values[0]
                else:
                    origin = origin_block.loc[origin_block[1] == 'ORIGIN', 3].values[0]

                if origin_block.loc[origin_block[1] == 'ORIGIN VIA', 2].values:
                    origin_via = origin_block.loc[origin_block[1] == 'ORIGIN VIA', 2].values[0]
                else:
                    origin_via = ''

                index_of_destination = origin_block[origin_block[0] == 'Destination'].index.values[0]
                df = origin_block.loc[index_of_destination + 1:, :]

                if len(df.columns) == 14:
                    df.columns = ['destination_icd', 'drop1', 'destination_country',
                                  'destination_port', 'drop2', 'service_type', 'type', 'currency', '20GP', '40GP',
                                  '40HC', '45HC', 'direct', 'note']
                    df = df.drop(columns=['drop1', 'drop2'])
                elif len(df.columns) == 15:
                    df.columns = ['destination_icd', 'drop1', 'drop2', 'destination_country',
                                  'destination_port', 'drop3', 'service_type', 'type', 'currency', '20GP', '40GP',
                                  '40HC', '45HC', 'direct', 'note']
                    df = df.drop(columns=['drop1', 'drop2', 'drop3'])
                else:
                    raise Exception("Input file too different from reference template")

                df['destination_icd'] = df['destination_icd'].apply(
                    lambda
                        x: nan if x == 'BLANK' or 'NOTE' in x or x == 'Destination' or x == 'DO NOT USE - PHUOC LONG' else x)
                df = df.dropna(subset=['destination_icd'])
                df = df.reset_index(drop=True)
                for note in notes_dict:
                    if bl:
                        df.loc[df['note'] == int(note), 'bill_of_lading'] = bl[note]
                    if voyage_dict:
                        df.loc[df['note'] == int(note), 'voyage'] = voyage_dict[note]
                    df['note'].replace(int(note), notes_dict[note], inplace=True, regex=True)
                df['destination_arbitrary_allowed'] = 'Yes'
                if (df['note'] == "DAR (Destination Arbitrary) is not applicable.").any():
                    df.loc[df['note'] == "DAR (Destination Arbitrary) is not applicable.",
                           'destination_arbitrary_allowed'] = 'No'

                df['origin_icd'] = origin
                df['origin_port'] = origin_via
                df['remarks'] = df['note']
                dfs.append(df)

            df = concat(dfs, ignore_index=True, sort=False)
            df['commodity'] = commodity
            df['customer_name'] = customer_name
            df['start_date'] = start_date
            df['expiry_date'] = expiry_date
            bulletin = ' '.join(commodity.split('(')[:1]).strip()
            df['bulletin'] = bulletin
            if note_included:
                df['inclusions'] = ','.join(note_included)
            if note_not_included:
                df['subject_to'] = ','.join(note_not_included)
            if service:
                df['loop'] = ','.join(service)

            """PSS charge column"""

            if pss_charge:
                for type in pss_charge:
                    if type == 'IPI':
                        for rate, ct_type in pss_charge[type].items():
                            rates = rate.split('/')
                            ct_types = ct_type.split('/')
                        index = df.loc[~(df['destination_icd'] == df['destination_port'])].index.tolist()
                        # for index_ in range(len(rates)):
                        #     df.loc[index, ct_types[index_]+'_PSS'] = rates[index_]
                        df.loc[index, '20GP_PSS'] = rates[0]
                        df.loc[index, '40GP_PSS'] = rates[1]
                        df.loc[index, '40HC_PSS'] = rates[2]
                        df.loc[index, '45HC_PSS'] = rates[3]

                    if type == 'CY':
                        for rate, ct_type in pss_charge[type].items():
                            rates = rate.split('/')
                            ct_types = ct_type.split('/')
                        index = df.loc[(df['destination_icd'] == df['destination_port'])].index.tolist()
                        # for index_ in range(len(rates)):
                        #     df.loc[index, ct_types[index_]+'_PSS'] = rates[index_]
                        df.loc[index, '20GP_PSS'] = rates[0]
                        df.loc[index, '40GP_PSS'] = rates[1]
                        df.loc[index, '40HC_PSS'] = rates[2]
                        df.loc[index, '45HC_PSS'] = rates[3]

                    if type == 'SD':
                        for rate, ct_type in pss_charge[type].items():
                            rates = rate.split('/')
                            ct_types = ct_type.split('/')
                        index = df.loc[(df['service_type'] == 'SD')].index.tolist()
                        # for index_ in range(len(rates)):
                        #     df.loc[index, ct_types[index_]+'_PSS'] = rates[index_]
                        df.loc[index, '20GP_PSS'] = rates[0]
                        df.loc[index, '40GP_PSS'] = rates[1]
                        df.loc[index, '40HC_PSS'] = rates[2]
                        df.loc[index, '45HC_PSS'] = rates[3]

            if loop_vessel:
                df['loop'] = loop_vessel[0]
                df['voyage'] = loop_vessel[1]

            if notes:
                df['remarks'] = df['remarks'] + notes

            return df

        def _6_1(self, df, config):

            if config['end'] - config['start'] == 1:
                log.info(f"Section starting from {config['start']} has no data")
                return None

            sectional_df = df[config['start']:config['end']]
            bulletin = df.iloc[config['start'], 0]
            sectional_df = self.remove_empty_columns(sectional_df)
            regional_sections = self.get_regional_sections(sectional_df, sectional_df.shape[0] - 1)

            dfs = []
            for region, regional_config in regional_sections.items():
                region_tmp = region
                region = region[1:-1]
                regional_df = sectional_df.loc[regional_config['start'] + 1:regional_config['end'] - 1, :]
                regional_df = regional_df.T.reset_index(drop=True).T
                indexes = regional_df[regional_df[0].str.match('^\d+\)$')].index.tolist()
                indexes.append(regional_config['end'])
                indexes = zip(indexes, indexes[1:])

                for commodity_config in indexes:
                    commodity_df = self.fix_commodity_block(
                        regional_df.loc[commodity_config[0]: commodity_config[1] - 1, :])
                    commodity_df['region'] = bulletin + ' - ' + region_tmp
                    if self.validity:
                        if region in self.validity:
                            start_date, expiry_date = list(self.validity[region].values())
                        else:
                            start_date, expiry_date = list(self.validity['all'].values())
                    else:
                        start_date, expiry_date = '', ''
                    commodity_df.loc[commodity_df['start_date'].isna(), 'start_date'] = start_date
                    commodity_df.loc[commodity_df['expiry_date'].isna(), 'expiry_date'] = expiry_date
                    dfs.append(commodity_df)

            df = concat(dfs, ignore_index=True, sort=False)
            df['charges'] = 'Basic Ocean Freight'
            return df

        def _6_2(self, df, config):
            return self._6_1(df, config)

        @classmethod
        def get_notes_map(cls, notes):
            notes_map = {}
            for i, row in notes[[0, 1]].iterrows():
                notes_map[row[0].split()[1]] = row[1]

            return notes_map

        @classmethod
        def get_arb_validity(cls, remark):

            if not isinstance(remark, str):
                return '', ''

            remark = remark.split("\n")[0]
            validity_re = re.compile("Valid (.+?) to (.+?)( |$|;)")
            if not validity_re.match(remark):
                return '', ''
            else:
                return validity_re.findall(remark)[0]

        @classmethod
        def fix_over_block(cls, block, point):
            block = block.applymap(lambda x: nan if x == '' else x)
            block = block.dropna(axis=1, how='all')
            block = block.fillna('')
            over = block[5].values[0]
            block = block[2:]

            index_of_notes = block[block[0].str.startswith("NOTE", na=False)].index.tolist()
            if index_of_notes:
                notes = block.loc[index_of_notes]
                notes = cls.get_notes_map(notes)
                block = block.loc[:index_of_notes[0] - 1]
                block.columns = [f'{point}_icd', 'drop10', f'{point}_country', 'service_type', 'via',
                                 'drop2', 'drop3', 'drop4', 'drop5', 'mode_of_transportation', 'drop`12', 'currency',
                                 'drop6', '20GP', '40GP', '40HC',
                                 '45HC',
                                 'drop7', 'drop8', 'remarks']
                block['remarks'] = block['remarks'].astype(str).map(notes)
                block['start_date'] = block['remarks'].apply(cls.get_arb_validity)
                block['expiry_date'] = block['start_date'].str[1]
                block['start_date'] = block['start_date'].str[0]
            else:

                block.columns = [f'{point}_icd', f'{point}_country', 'service_type', 'via',
                                 'drop2', 'drop3', 'drop4', 'drop6', 'currency', 'drop6', '20GP', '40GP', '40HC',
                                 '45HC', 'drop8', 'drop9']
                block['expiry_date'] = ''
                block['start_date'] = ''

            block = block.drop(columns=[column for column in block.columns if column.startswith('drop')])
            block[f'{point}_port'] = over
            return block

        @classmethod
        def arbitary_fix(cls, df, config, point):
            if (config['end'] - config['start']) != 1:
                sectional_df = df[config['start']:config['end']]
                sectional_df = cls.remove_empty_columns(sectional_df)
                regional_sections = cls.get_regional_sections(sectional_df, sectional_df.shape[0] - 1)

                dfs = []
                for region, regional_config in regional_sections.items():
                    region = region[1:-1]
                    regional_df = sectional_df.loc[regional_config['start'] + 1:regional_config['end'] - 1, :]
                    regional_df = regional_df.T.reset_index(drop=True).T
                    regional_df.reset_index(drop=True, inplace=True)
                    indexes = regional_df[regional_df[0] == 'RATE APPLICABLE OVER  :'].index.tolist()
                    indexes.append(regional_config['end'])
                    indexes = zip(indexes, indexes[1:])

                    for over_config in indexes:
                        over_df = cls.fix_over_block(regional_df.loc[over_config[0]: over_config[1] - 1, :], point)
                        over_df['region'] = region
                        if regional_df.iloc[over_config[0], 4] != '':
                            over_df['origin_port'] = regional_df.iloc[over_config[0], 4]
                        dfs.append(over_df)

                df = concat(dfs, ignore_index=True, sort=False)
                df[f'{point}_icd'] = df[f'{point}_icd'].apply(
                    lambda x: nan if x == 'BLANK' or x == 'Point' or x == '' or len(x) == 2 else x)
                df = df.dropna(subset=[f'{point}_icd'])
                df['charges'] = f'{point.capitalize()} arbitrary charge'

                return df.reset_index(drop=True)

        @classmethod
        def _6_3(cls, df, config):
            if not config:
                return
            return cls.arbitary_fix(df, config, 'origin')

        @classmethod
        def _6_4(cls, df, config):
            if not config:
                return
            return cls.arbitary_fix(df, config, 'destination')

        @classmethod
        def split(cls, port):
            temp = port.split(", ")
            if len(temp) == 3:
                return [", ".join(temp[:2]), temp[2]]
            else:
                return [", ".join(temp), '']

        @classmethod
        def fix_port_names(cls, df):
            for point in ['origin', 'destination']:
                change = False
                if point + '_icd' in df:
                    df[point + '_icd'] = df[point + '_icd'].apply(lambda x: cls.split(x)[0])
                    change = True
                if point + '_port' in df:
                    df[point + '_port'] = df[point + '_port'].apply(lambda x: cls.split(x)[0])
                    change = True

                if change:
                    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                    df = df.reset_index(drop=True)

            return df

        def format_output(self, dfs, am_no, inclusions):
            output = {}

            freight_df = dfs[0]

            if dfs[1] is not None:
                freight_df = concat([freight_df, dfs[1]], ignore_index=True, sort=False)

            freight_df = freight_df.drop(columns=["type", "direct", "note"])
            freight_df = self.fix_port_names(freight_df)
            freight_df['amendment_no'] = am_no

            """Adding inclusions"""
            if inclusions is not None:
                for region in inclusions:
                    freight_df.loc[(freight_df['region'] == region), 'inclusions'] = inclusions[region][0]

            output['Freight'] = freight_df

            # origin_df = None
            # if dfs[2] is not None:
            #     origin_df = dfs[2]
            #     origin_df = self.fix_port_names(origin_df)
            #     origin_df['amendment_no'] = am_no
            #     origin_df = origin_df.rename(columns={'origin_icd': 'icd', 'origin_port': 'to'})
            #     origin_df['at'] = 'origin'
            #
            #     region_list = origin_df['region'].unique().tolist()
            #     origin_df['expiry_date'].replace('', nan, inplace=True)
            #     origin_df['start_date'].replace('', nan, inplace=True)
            #
            #     if self.validity:
            #         for region in region_list:
            #             if region in self.validity:
            #                 start_date, expiry_date = list(self.validity[region].values())
            #                 origin_df.loc[origin_df['start_date'].isna() &
            #                               origin_df['region'].str.contains(region), 'start_date'] = start_date
            #                 origin_df.loc[origin_df['expiry_date'].isna() &
            #                               origin_df['region'].str.contains(region), 'expiry_date'] = expiry_date
            #             else:
            #                 start_date, expiry_date = list(self.validity['all'].values())
            #                 origin_df.loc[origin_df['start_date'].isna(), 'start_date'] = start_date
            #                 origin_df.loc[origin_df['expiry_date'].isna(), 'expiry_date'] = expiry_date
            #     else:
            #         start_date, expiry_date = '', ''
            #         origin_df.loc[origin_df['start_date'].isna(), 'start_date'] = start_date
            #         origin_df.loc[origin_df['expiry_date'].isna(), 'expiry_date'] = expiry_date
            #
            # destination_df = None
            # if dfs[3] is not None:
            #     destination_df = dfs[3]
            #     destination_df = self.fix_port_names(destination_df)
            #     destination_df['amendment_no'] = am_no
            #     destination_df = destination_df.rename(columns={'destination_icd': 'icd', 'destination_port': 'to'})
            #     destination_df['at'] = 'destination'
            #
            #     region_list = destination_df['region'].unique().tolist()
            #     destination_df['expiry_date'].replace('', nan, inplace=True)
            #     destination_df['start_date'].replace('', nan, inplace=True)
            #
            #     if self.validity:
            #         for region in region_list:
            #             if region in self.validity:
            #                 start_date, expiry_date = list(self.validity[region].values())
            #                 destination_df.loc[destination_df['start_date'].isna() &
            #                                    destination_df['region'].str.contains(region), 'start_date'] = start_date
            #                 destination_df.loc[destination_df['expiry_date'].isna() &
            #                                    destination_df['region'].str.contains(
            #                                        region), 'expiry_date'] = expiry_date
            #             else:
            #                 start_date, expiry_date = list(self.validity['all'].values())
            #                 destination_df.loc[origin_df['start_date'].isna(), 'start_date'] = start_date
            #                 destination_df.loc[origin_df['expiry_date'].isna(), 'expiry_date'] = expiry_date
            #     else:
            #         start_date, expiry_date = '', ''
            #         destination_df.loc[destination_df['start_date'].isna(), 'start_date'] = start_date
            #         destination_df.loc[destination_df['expiry_date'].isna(), 'expiry_date'] = expiry_date
            #
            # if origin_df is not None or destination_df is not None:
            #     arbitrary_df = concat([origin_df, destination_df], ignore_index=True, sort=False)
            #
            #     output['Arbitrary Charges'] = arbitrary_df

            return output

        def get_amendment_no(self):
            index = self.df[self.df[0].str.startswith('SERVICE CONTRACT NO', na=False)].index.values[0]
            return self.df[0][index].split()[-1]

        def get_inclusions(self):
            inclusions = defaultdict(list)
            start_index = list(self.df[(self.df[0].str.contains('C. EXCEPTIONS', na=False))].index)
            end_index = self.df.tail(1).index.values[0] + 1
            inclusions_table_df = self.df[start_index[0]:end_index].copy(deep=True)
            inclusions_table_df.reset_index(drop=True, inplace=True)
            inc_start = list(inclusions_table_df[(inclusions_table_df[0].str.startswith('[', na=False))].index)
            inc_start.append(inclusions_table_df.tail(1).index.values[0])
            for _index in range(len(inc_start) - 1):
                inclusions_df = inclusions_table_df[inc_start[_index]:inc_start[_index + 1]].copy(deep=True)
                inclusions_df.reset_index(drop=True, inplace=True)
                if inclusions_df[3].str.contains('Rates are inclusive of', na=False).any():
                    start_inc = list(
                        inclusions_df[(inclusions_df[3].str.contains('Rates are inclusive of', na=False))].index)
                    inclusions_ch = inclusions_df.iloc[start_inc[0], 3]
                    group_name = inclusions_df.iloc[0, 0]
                    regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                    included_list = []
                    matches_inc = re.finditer(regex_incl, inclusions_ch, re.MULTILINE)
                    for matchNum, match in enumerate(matches_inc, start=1):
                        for groupNum in range(0, len(match.groups())):
                            groupNum = groupNum + 1
                            included_list.append(match.group(groupNum))
                    included_list = ','.join(included_list)
                    regex = r"\[|]"
                    group_name = re.sub(regex, '', group_name, 0, re.MULTILINE)
                    group_name = group_name.strip()
                    inclusions[group_name].append(included_list)

            return inclusions

        def capture(self):

            sections = self.get_sections()

            amendment_no = self.get_amendment_no()

            self.set_validity_from_section_8()

            # self.set_load_type_map()

            inclusions_dict = self.get_inclusions()

            dfs = []
            for section, config in sections.items():
                if config is not None:
                    section = '_' + section.replace('-', '_')
                    fix = getattr(self, section)
                    dfs.append(fix(self.df, config))
                else:
                    dfs.append(None)

            self.captured_output = self.format_output(dfs, amendment_no, inclusions_dict)

        def map_load_type(self, df):
            if '20GP_PSS' in df:
                charge_profile = ["20GP", "40GP", '40HC', "45HC", "20GP_PSS", "40GP_PSS", "40HC_PSS", "45HC_PSS"]
            else:
                charge_profile = ["20GP", "40GP", '40HC', "45HC"]
            df = df.melt(
                id_vars=[column for column in df.columns if column not in charge_profile],
                value_vars=charge_profile, value_name='amount', var_name='load_type')
            df['amount'] = df['amount'].astype(str)
            df.loc[df['amount'].str.contains("/", na=False), 'load_type'] = \
                df.loc[df['amount'].str.contains("/", na=False), 'amount'].str.split("/").str[0]

            df['load_type'] = df['load_type'].apply(
                lambda x: x if x not in self.load_type_map else self.load_type_map[x])
            df['amount'] = df['amount'].str.split("/").str[-1]
            df['load_type'] = df['load_type'].replace('20\' Flat Rack', '20FR')
            df['load_type'] = df['load_type'].replace('40\' Flat Rack', '40FR')
            df['load_type'] = df['load_type'].replace('20\' Open Top', '20OT')
            df['load_type'] = df['load_type'].replace('40\' Open Top', '40OT')
            df['load_type'] = df['load_type'].replace('20\' Reefer', '20RE')
            df['load_type'] = df['load_type'].replace('40\' Reefer High Cube', '40HR')
            df['amount'] = df['amount'].replace('', nan)
            df['amount'] = df['amount'].replace('nan', nan)
            df = df.dropna(subset=['amount'])
            df = df.reset_index(drop=True)

            return df

        def port_lookup(self, df):
            if self.df.loc[self.df[0].str.startswith('6. CONTRACT RATES', na=False)].index.any():
                start_index = self.df.loc[self.df[0].str.startswith('6. CONTRACT RATES', na=False)].index.values[0]
                end_index = self.df.loc[self.df[0].str.contains('6-1. General Rate', na=False)].index.values[0]
                port_df_all = self.df[start_index + 1:end_index - 1].copy(deep=True)
                port_df_all.reset_index(drop=True, inplace=True)
                indexes = port_df_all[port_df_all[0].str.startswith('[', na=False) &
                                      port_df_all[0].str.endswith(']', na=False)].index.tolist()
                indexes += [port_df_all.tail(1).index.values[0] + 1]

                lookup_dict = {}
                for index in range(len(indexes) - 1):
                    group = port_df_all.iloc[indexes[index], 0][1:-1]
                    port_df = port_df_all.loc[indexes[index] + 1:indexes[index + 1]].copy(deep=True)
                    port_df.replace('', nan, inplace=True)
                    port_df.dropna(axis=1, inplace=True, how='all')
                    port_df.columns = ['Name', 'Pairs']
                    port_df.dropna(subset=['Pairs'], axis=0, inplace=True)
                    port_df = port_df.fillna(method='ffill')
                    port_df_grouped = port_df.groupby(['Name'], as_index=False)[
                        'Pairs'].apply(lambda x: ';'.join(x)).reset_index(drop=True)
                    lookup = dict(zip(port_df_grouped['Name'].tolist(), port_df_grouped['Pairs'].tolist()))
                    lookup_dict[group] = lookup
                    index_region = df.loc[(df['region'].str.contains(group))].index.tolist()
                    df.loc[index_region] = df.loc[index_region].replace(lookup, regex=True)
                # freight_df.replace(lookup, inplace=True, regex=True)

            return df

        def clean(self):

            freight_df = self.captured_output['Freight']
            freight_df.drop(columns=['destination_country'], inplace=True)
            for c in product(['origin', 'destination'], ['icd', 'port']):
                _column = c[0] + '_' + c[1]
                if c[1] == 'icd':
                    freight_df[_column] = freight_df[_column].str.replace("\n", ";")
                freight_df[_column] = freight_df[_column].str.split('\n')
                freight_df = freight_df.explode(_column)
                freight_df = freight_df.reset_index(drop=True)
            freight_df['origin_icd'] = freight_df['origin_icd'].str.rstrip("(CY)")

            freight_df = self.map_load_type(freight_df)
            # freight_df = self.get_commodity_desc(freight_df)

            """
            Hard coded will replace once look up is ready
            """

            freight_df = self.port_lookup(freight_df)

            """ Premium column"""
            # freight_df.loc[freight_df['region'].str.contains('6-2. Special Rate') & freight_df['bulletin'].str.contains(
            #     'PB Product'), 'premium_service'] = 'premium_sml'

            self.cleaned_output = {'Freight': freight_df}

            if 'Arbitrary Charges' in self.captured_output:
                arbitrary_df = self.captured_output['Arbitrary Charges']

                """No Destination Arb"""
                if 'destination_country' in arbitrary_df:
                    arbitrary_df.drop(columns=['destination_country'], inplace=True)
                elif 'origin_country' in arbitrary_df:
                    arbitrary_df.drop(columns=['origin_country'], inplace=True)
                arbitrary_df['to'] = arbitrary_df['to'].str.rstrip("Rate")
                arbitrary_df['to'] = arbitrary_df['to'].str.strip()
                arbitrary_df['icd'] = arbitrary_df['icd'].str.replace("\n", ";")
                arbitrary_df = self.map_load_type(arbitrary_df)
                if 'remarks' in arbitrary_df:
                    arbitrary_df.drop(columns=['remarks'], inplace=True)

                arbitrary_df = self.port_lookup(arbitrary_df)

                self.cleaned_output['Arbitrary Charges'] = arbitrary_df

# class SM_Excel(BaseTemplate):
#     class _SM_v1(BaseFix):
#
#         def __init__(self, df, file_model: dict, headers_config: dict):
#             super().__init__(df, file_model, headers_config)
#
#             self.validity = {}
#             self.load_type_map = {}
#
#         def check_input(self):
#             check_errors = []
#             if not self.df[0].str.contains('6-1').any():
#                 check_errors.append("Section definition '6-1' should be present in first Column")
#
#             if not (self.df[0].str.startswith('[') & self.df[0].str.endswith(']')).any():
#                 check_errors.append("Region definition should be present in first column and should be enclosed in []")
#
#             if not self.df[0].str.contains('Code').any():
#                 check_errors.append("Load Type mapping table should be defined along first column under section "
#                                     "header 'Code'")
#
#             if check_errors:
#                 raise InputValidationError(check_errors)
#
#         def check_output(self):
#             pass
#
#         def get_sections(self):
#             sections_to_check = ['6-1', '6-2', '6-3', '6-4', '6-5']
#             sections = {}
#
#             previous_section = None
#             for check in sections_to_check:
#                 if self.df[self.df[0].str.startswith(check, na=False)].index.values:
#                     index = self.df[self.df[0].str.startswith(check, na=False)].index.values[0]
#                     sections[check] = {'start': index, 'end': None}
#
#                     if previous_section:
#                         sections[previous_section]['end'] = index
#
#                     previous_section = check
#                 else:
#                     sections[check] = None
#
#             for drop in ['6-5', '7. LIQUIDATED']:
#                 if drop in sections:
#                     sections.pop(drop)
#
#             return sections
#
#         def set_validity_from_section_8(self):
#
#             if self.df[self.df[0] == '8. DURATION OF THE CONTRACT'].index.values:
#                 start_index = self.df[self.df[0] == '8. DURATION OF THE CONTRACT'].index.values[0]
#                 end_index = self.df[self.df[0].str.startswith("9. SIGNATURE", na=False)].index.values[0]
#
#                 validity_df = self.df.loc[start_index: end_index - 1, :]
#                 validity_df = validity_df.applymap(lambda x: nan if x == '' else x)
#                 validity_df = validity_df.dropna(axis=1, how='all')
#                 validity_df = validity_df.reset_index(drop=True)
#                 validity_df = validity_df.T.reset_index(drop=True).T
#
#                 self.validity['all'] = {
#                     'start_date': datetime.datetime.strptime(validity_df[1][1], "%d %b, %Y").date().isoformat(),
#                     'expiry_date': datetime.datetime.strptime(validity_df[3][1], "%d %b, %Y").date().isoformat()}
#
#                 region_indexes = validity_df[
#                     validity_df[0].str.startswith('[', na=False) & validity_df[0].str.endswith(']',
#                                                                                                na=False)].index.tolist()
#
#                 for index in region_indexes:
#                     self.validity[validity_df[0][index][1:-1]] = {
#                         'start_date': datetime.datetime.strptime(validity_df[1][index + 1],
#                                                                  "%d %b, %Y").date().isoformat(),
#                         'expiry_date': datetime.datetime.strptime(validity_df[3][index + 1],
#                                                                   "%d %b, %Y").date().isoformat()}
#
#         def set_load_type_map(self):
#             start_index = self.df[self.df[0] == 'Code'].index.values[0]
#
#             load_df = self.df.loc[start_index + 1:, :]
#             load_df = load_df.applymap(lambda x: nan if x == '' else x)
#             load_df = load_df.dropna(axis=1, how='all')
#             load_df = load_df.reset_index(drop=True)
#             load_df = load_df.T.reset_index(drop=True).T
#             self.load_type_map = load_df.set_index(0).to_dict()[1]
#
#         @classmethod
#         def remove_empty_columns(cls, df):
#             df = df.applymap(lambda x: nan if x == '' else x)
#             df = df.dropna(axis=1, how="all")
#             df = df.reset_index(drop=True)
#             df = df.fillna('')
#             return df
#
#         @classmethod
#         def get_regional_sections(cls, df, section_end):
#             regional_sections = {}
#             indexes = df[df[0].str.startswith('[', na=False) & df[0].str.endswith(']', na=False)].index.tolist()
#             indexes.append(section_end + 1)
#             indexes = zip(indexes, indexes[1:])
#
#             for config in indexes:
#                 region = df[0][config[0]]
#                 regional_sections[region] = {'start': config[0], 'end': config[1]}
#
#             return regional_sections
#
#         def get_commodity_desc(self, freight_df):
#             if self.df[self.df[0] == '3. COMMODITIES :'].index.values:
#                 end_index = \
#                     self.df[self.df[0].str.startswith("4. MINIMUM VOLUME COMMITMENT('MVC')", na=False)].index.values[0]
#                 start_index = self.df[self.df[0].str.startswith("3. COMMODITIES :", na=False)].index.values[0]
#
#                 comm_desc_df = self.df.loc[start_index + 1: end_index - 1]
#                 comm_desc_df = comm_desc_df.applymap(lambda x: nan if x == '' else x)
#                 comm_desc_df = comm_desc_df.dropna(axis=1, how='all')
#                 comm_desc_df = comm_desc_df.reset_index(drop=True)
#
#                 indexes = comm_desc_df[comm_desc_df[0].str.startswith('[', na=False) &
#                                        comm_desc_df[0].str.endswith(']', na=False)].index.tolist()
#                 indexes += [comm_desc_df.tail(1).index.values[0] + 1]
#
#                 comm_desc_dict = {}
#                 for index in range(len(indexes) - 1):
#                     comm_group = comm_desc_df.loc[indexes[index] + 1:indexes[index + 1]].copy(deep=True)
#                     comm_group_list = comm_group[0].to_list()
#                     group = comm_desc_df.iloc[indexes[index], 0][1:-1]
#                     comm_desc_dict[group] = \
#                         dict(zip(comm_group_list[::2], comm_group_list[1::2]))
#                 for region in comm_desc_dict:
#                     desc_dict = comm_desc_dict[region]
#                     for desc in desc_dict:
#                         # freight_df.loc[(freight_df['region'].str.contains(region)) &
#                         #                (freight_df['commodity'].str.contains(desc)), 'commodity'] = desc_dict[desc]
#                         index_region = freight_df.loc[(freight_df['region'].str.contains(region))].index.tolist()
#                         freight_df.loc[index_region, 'commodity'] = \
#                             freight_df.loc[index_region, 'commodity'].replace(desc, desc_dict[desc], regex=True)
#                 freight_df['commodity'] = freight_df['commodity'].replace("(SEE TERM 3. GROUP CODE DETAIL CODES)", '',
#                                                                           regex=True)
#                 return freight_df
#
#         @classmethod
#         def get_validity_and_remarks(cls, block, notes_index):
#             bl_dict = voyage_dict = {}
#             notes_df = block.loc[notes_index].copy()
#             notes_df.replace('', nan, inplace=True)
#             notes_df.dropna(how='all', axis=1, inplace=True)
#             notes_df['Notes No'] = notes_df[0].str.extract(r'(\d)')
#             notes_df.drop(0, axis=1, inplace=True)
#             notes_df = notes_df.groupby(['Notes No'], as_index=False)[2].apply(lambda x: '\n'.join(x)).reset_index(
#                 drop=True)
#             if notes_df[2].str.contains('specified B/Ls only', na=False).any():
#                 notes_df[2] = notes_df[2].replace('\n', '; ', regex=True)
#                 notes_df['bl'] = notes_df[2].str.extract('specified B/Ls only :(.+?)$', re.MULTILINE)
#                 notes_df['bl'] = notes_df['bl'].str.strip()
#                 bl_dict = dict(zip(notes_df['Notes No'].tolist(), notes_df['bl'].tolist()))
#             if notes_df[2].str.contains('applicable for the VVD', na=False).any():
#                 notes_df['voyage'] = notes_df[2].str.extract('applicable for the VVD "(.+?)"', re.MULTILINE)
#                 notes_df['voyage'] = notes_df['voyage'].str.strip()
#                 voyage_dict = dict(zip(notes_df['Notes No'].tolist(), notes_df['voyage'].tolist()))
#             notes_dict = dict(zip(notes_df['Notes No'].tolist(), notes_df[2].tolist()))
#             return notes_dict, bl_dict, voyage_dict
#
#         @classmethod
#         def get_validity_and_remarks_bullet(cls, block, notes_index):
#             notes_df = block.loc[notes_index + 1:block.tail(1).index.values[0]].copy()
#             notes_df.replace('', nan, inplace=True)
#             notes_df.dropna(how='all', inplace=True, axis=1)
#             notes_list = notes_df[0].to_list()
#             pss_charge = defaultdict(list)
#             loop_vessel = []
#             for note in notes_list:
#                 note = note.replace('\n', '')
#                 if re.search('PSC will be applied as per tariff', note) or re.search(r"IPI\s?(\(CY\)\s:|:)", note):
#                     if re.search('IPI', note):
#                         # regex = r'IPI\s?:\s?USD\s?(.+?)\s?per\s?(.+?)(\d\)|$)'
#                         regex = r'IPI\s?(\(CY\)\s:|:)\s?USD\s?(.+?)\s?per\s?(.+?)(\d\)|$)'
#                         matches = re.finditer(regex, note, re.MULTILINE)
#                         for matchNum, match in enumerate(matches, start=1):
#                             rate = match.group(2)
#                             cnt_type = match.group(3)
#                         pss_charge['IPI'] = {rate: cnt_type}
#                         note = re.sub(regex, '', note)
#                     if re.search('CY', note):
#                         regex = r'(\(CY\)\s:|CY\s?:)\s?USD\s?(.+?)\s?per\s?(.+?)(\d\)|$)'
#                         matches = re.finditer(regex, note, re.MULTILINE)
#                         for matchNum, match in enumerate(matches, start=1):
#                             rate = match.group(2)
#                             cnt_type = match.group(3)
#                         pss_charge['CY'] = {rate: cnt_type}
#                     if re.search('Door delivery points', note):
#                         regex = r'delivery points\s?:\s?USD\s?(.+?)\s?per\s?(.+?)(\d\)|$)'
#                         matches = re.finditer(regex, note, re.MULTILINE)
#                         for matchNum, match in enumerate(matches, start=1):
#                             rate = match.group(1)
#                             cnt_type = match.group(2)
#                         pss_charge['SD'] = {rate: cnt_type}
#                 elif re.search('specified lane/VVD only', note):
#                     regex = r"specified lane/VVD only\s?:\s?(.+?)/(.+?)(\.|$)"
#                     matches = re.finditer(regex, note, re.MULTILINE)
#                     for matchNum, match in enumerate(matches, start=1):
#                         loop_vessel.append(match.group(1))
#                         loop_vessel.append(match.group(2))
#             notes = "\n".join(notes_list)
#             return notes, pss_charge, loop_vessel
#
#         @classmethod
#         def fix_commodity_block(cls, block):
#             block.reset_index(drop=True, inplace=True)
#             block = block.applymap(lambda x: nan if x == ':' or x == '' else x)
#             block = block.dropna(axis=1, how='all')
#             block = block.fillna('')
#             block = block.T.reset_index(drop=True).T
#
#             if len(block.columns) >= 15:
#                 block[2] = block[2] + block[3]
#                 block = block.drop(columns=[3])
#                 block = block.T.reset_index(drop=True).T
#
#             if block[2].values[0] == '':
#                 commodity = block[3].values[0]
#             else:
#                 commodity = block[2].values[0]
#
#             if block[1].str.contains('ACTUAL CUSTOMER').any():
#                 index = block[(block[1].str.contains('ORIGIN', na=False))].index.values[0]
#                 start_index = block[(block[1].str.contains('ACTUAL CUSTOMER', na=False))].index.values[0]
#
#                 if (index - start_index) == 1:
#                     customer_name = block[2].values[1]
#                 else:
#                     customer_name = block[2][start_index:index].values.tolist()
#             else:
#                 customer_name = ''
#
#             # bulletin = block[0].values[0]
#
#             start_date = expiry_date = note_included = note_not_included = service = notes = None
#             notes_dict = pss_charge = {}
#             loop_vessel = []
#
#             if block[0].str.contains('NOTE', regex=True).any():
#                 index_of_notes = block[block[0].str.contains('NOTE', regex=True)].index.values
#                 notes_dict, bl, voyage_dict = cls.get_validity_and_remarks(block, index_of_notes)
#                 origin_indexes = block[block[1] == 'ORIGIN'].index.tolist()
#                 origin_indexes.append(block.index.values[-1] + 1)
#             if block[0].str.contains('< Note for Bullet').any():
#                 index_of_notes = block[block[0].str.contains('< Note for Bullet')].index.values[0]
#                 notes, pss_charge, loop_vessel = cls.get_validity_and_remarks_bullet(block, index_of_notes)
#                 origin_indexes = block[block[1] == 'ORIGIN'].index.tolist()
#                 origin_indexes.append(index_of_notes)
#             else:
#                 notes = ''
#                 origin_indexes = block[block[1] == 'ORIGIN'].index.tolist()
#                 origin_indexes.append(block.index.values[-1] + 1)
#
#             origin_config = zip(origin_indexes, origin_indexes[1:])
#
#             dfs = []
#             for config in origin_config:
#                 origin_block = block.loc[config[0]:config[1] - 1, :]
#                 origin_block = origin_block.applymap(lambda x: nan if x == '' else x)
#                 origin_block = origin_block.dropna(axis=1, how='all')
#                 origin_block = origin_block.fillna('')
#
#                 if origin_block.loc[origin_block[1] == 'ORIGIN', 2].values[0] != '':
#                     origin = origin_block.loc[origin_block[1] == 'ORIGIN', 2].values[0]
#                 else:
#                     origin = origin_block.loc[origin_block[1] == 'ORIGIN', 3].values[0]
#
#                 if origin_block.loc[origin_block[1] == 'ORIGIN VIA', 2].values:
#                     origin_via = origin_block.loc[origin_block[1] == 'ORIGIN VIA', 2].values[0]
#                 else:
#                     origin_via = ''
#
#                 index_of_destination = origin_block[origin_block[0] == 'Destination'].index.values[0]
#                 df = origin_block.loc[index_of_destination + 1:, :]
#
#                 if len(df.columns) == 14:
#                     df.columns = ['destination_icd', 'drop1', 'destination_country',
#                                   'destination_port', 'drop2', 'service_type', 'type', 'currency', '20GP', '40GP',
#                                   '40HC', '45HC', 'direct', 'note']
#                     df = df.drop(columns=['drop1', 'drop2'])
#                 elif len(df.columns) == 15:
#                     df.columns = ['destination_icd', 'drop1', 'drop2', 'destination_country',
#                                   'destination_port', 'drop3', 'service_type', 'type', 'currency', '20GP', '40GP',
#                                   '40HC', '45HC', 'direct', 'note']
#                     df = df.drop(columns=['drop1', 'drop2', 'drop3'])
#                 else:
#                     raise Exception("Input file too different from reference template")
#
#                 df['destination_icd'] = df['destination_icd'].apply(
#                     lambda
#                         x: nan if x == 'BLANK' or 'NOTE' in x or x == 'Destination' or x == 'DO NOT USE - PHUOC LONG' else x)
#                 df = df.dropna(subset=['destination_icd'])
#                 df = df.reset_index(drop=True)
#                 for note in notes_dict:
#                     if bl:
#                         df.loc[df['note'] == int(note), 'bill_of_lading'] = bl[note]
#                     if voyage_dict:
#                         df.loc[df['note'] == int(note), 'voyage'] = voyage_dict[note]
#                     df['note'].replace(int(note), notes_dict[note], inplace=True, regex=True)
#                 df['destination_arbitrary_allowed'] = 'Yes'
#                 if (df['note'] == "DAR (Destination Arbitrary) is not applicable.").any():
#                     df.loc[df['note'] == "DAR (Destination Arbitrary) is not applicable.",
#                            'destination_arbitrary_allowed'] = 'No'
#
#                 df['origin_icd'] = origin
#                 df['origin_port'] = origin_via
#                 df['remarks'] = df['note']
#                 dfs.append(df)
#
#             df = concat(dfs, ignore_index=True, sort=False)
#             df['commodity'] = commodity
#             df['customer_name'] = customer_name
#             df['start_date'] = start_date
#             df['expiry_date'] = expiry_date
#             bulletin = ' '.join(commodity.split('(')[:1]).strip()
#             df['bulletin'] = bulletin
#             if note_included:
#                 df['inclusions'] = ','.join(note_included)
#             if note_not_included:
#                 df['subject_to'] = ','.join(note_not_included)
#             if service:
#                 df['loop'] = ','.join(service)
#
#             """PSS charge column"""
#
#             if pss_charge:
#                 for type in pss_charge:
#                     if type == 'IPI':
#                         for rate, ct_type in pss_charge[type].items():
#                             rates = rate.split('/')
#                             ct_types = ct_type.split('/')
#                         index = df.loc[~(df['destination_icd'] == df['destination_port'])].index.tolist()
#                         # for index_ in range(len(rates)):
#                         #     df.loc[index, ct_types[index_]+'_PSS'] = rates[index_]
#                         df.loc[index, '20GP_PSS'] = rates[0]
#                         df.loc[index, '40GP_PSS'] = rates[1]
#                         df.loc[index, '40HC_PSS'] = rates[2]
#                         df.loc[index, '45HC_PSS'] = rates[3]
#
#                     if type == 'CY':
#                         for rate, ct_type in pss_charge[type].items():
#                             rates = rate.split('/')
#                             ct_types = ct_type.split('/')
#                         index = df.loc[(df['destination_icd'] == df['destination_port'])].index.tolist()
#                         # for index_ in range(len(rates)):
#                         #     df.loc[index, ct_types[index_]+'_PSS'] = rates[index_]
#                         df.loc[index, '20GP_PSS'] = rates[0]
#                         df.loc[index, '40GP_PSS'] = rates[1]
#                         df.loc[index, '40HC_PSS'] = rates[2]
#                         df.loc[index, '45HC_PSS'] = rates[3]
#
#                     if type == 'SD':
#                         for rate, ct_type in pss_charge[type].items():
#                             rates = rate.split('/')
#                             ct_types = ct_type.split('/')
#                         index = df.loc[(df['service_type'] == 'SD')].index.tolist()
#                         # for index_ in range(len(rates)):
#                         #     df.loc[index, ct_types[index_]+'_PSS'] = rates[index_]
#                         df.loc[index, '20GP_PSS'] = rates[0]
#                         df.loc[index, '40GP_PSS'] = rates[1]
#                         df.loc[index, '40HC_PSS'] = rates[2]
#                         df.loc[index, '45HC_PSS'] = rates[3]
#
#             if loop_vessel:
#                 df['loop'] = loop_vessel[0]
#                 df['voyage'] = loop_vessel[1]
#
#             if notes:
#                 df['remarks'] = df['remarks'] + notes
#
#             return df
#
#         def _6_1(self, df, config):
#
#             if config['end'] - config['start'] == 1:
#                 log.info(f"Section starting from {config['start']} has no data")
#                 return None
#
#             sectional_df = df[config['start']:config['end']]
#             bulletin = df.iloc[config['start'], 0]
#             sectional_df = self.remove_empty_columns(sectional_df)
#             regional_sections = self.get_regional_sections(sectional_df, sectional_df.shape[0] - 1)
#
#             dfs = []
#             for region, regional_config in regional_sections.items():
#                 region_tmp = region
#                 region = region[1:-1]
#                 regional_df = sectional_df.loc[regional_config['start'] + 1:regional_config['end'] - 1, :]
#                 regional_df = regional_df.T.reset_index(drop=True).T
#                 indexes = regional_df[regional_df[0].str.match('^\d+\)$')].index.tolist()
#                 indexes.append(regional_config['end'])
#                 indexes = zip(indexes, indexes[1:])
#
#                 for commodity_config in indexes:
#                     commodity_df = self.fix_commodity_block(
#                         regional_df.loc[commodity_config[0]: commodity_config[1] - 1, :])
#                     commodity_df['region'] = bulletin + ' - ' + region_tmp
#                     if self.validity:
#                         if region in self.validity:
#                             start_date, expiry_date = list(self.validity[region].values())
#                         else:
#                             start_date, expiry_date = list(self.validity['all'].values())
#                     else:
#                         start_date, expiry_date = '', ''
#                     commodity_df.loc[commodity_df['start_date'].isna(), 'start_date'] = start_date
#                     commodity_df.loc[commodity_df['expiry_date'].isna(), 'expiry_date'] = expiry_date
#                     dfs.append(commodity_df)
#
#             df = concat(dfs, ignore_index=True, sort=False)
#             df['charges'] = 'Basic Ocean Freight'
#             return df
#
#         def _6_2(self, df, config):
#             return self._6_1(df, config)
#
#         @classmethod
#         def get_notes_map(cls, notes):
#             notes_map = {}
#             for i, row in notes[[0, 1]].iterrows():
#                 notes_map[row[0].split()[1]] = row[1]
#
#             return notes_map
#
#         @classmethod
#         def get_arb_validity(cls, remark):
#
#             if not isinstance(remark, str):
#                 return '', ''
#
#             remark = remark.split("\n")[0]
#             validity_re = re.compile("Valid (.+?) to (.+?)( |$|;)")
#             if not validity_re.match(remark):
#                 return '', ''
#             else:
#                 return validity_re.findall(remark)[0]
#
#         @classmethod
#         def fix_over_block(cls, block, point):
#             block = block.applymap(lambda x: nan if x == '' else x)
#             block = block.dropna(axis=1, how='all')
#             block = block.fillna('')
#             over = block[5].values[0]
#             block = block[2:]
#
#             index_of_notes = block[block[0].str.startswith("NOTE", na=False)].index.tolist()
#             if index_of_notes:
#                 notes = block.loc[index_of_notes]
#                 notes = cls.get_notes_map(notes)
#                 block = block.loc[:index_of_notes[0] - 1]
#                 block.columns = [f'{point}_icd', 'drop10', f'{point}_country', 'service_type', 'via',
#                                  'drop2', 'drop3', 'drop4', 'drop5', 'mode_of_transportation', 'drop`12', 'currency',
#                                  'drop6', '20GP', '40GP', '40HC',
#                                  '45HC',
#                                  'drop7', 'drop8', 'remarks']
#                 block['remarks'] = block['remarks'].astype(str).map(notes)
#                 block['start_date'] = block['remarks'].apply(cls.get_arb_validity)
#                 block['expiry_date'] = block['start_date'].str[1]
#                 block['start_date'] = block['start_date'].str[0]
#             else:
#
#                 block.columns = [f'{point}_icd', f'{point}_country', 'service_type', 'via',
#                                  'drop2', 'drop3', 'drop4', 'drop6', 'currency', 'drop6', '20GP', '40GP', '40HC',
#                                  '45HC', 'drop8', 'drop9']
#                 block['expiry_date'] = ''
#                 block['start_date'] = ''
#
#             block = block.drop(columns=[column for column in block.columns if column.startswith('drop')])
#             block[f'{point}_port'] = over
#             return block
#
#         @classmethod
#         def arbitary_fix(cls, df, config, point):
#             if (config['end'] - config['start']) != 1:
#                 sectional_df = df[config['start']:config['end']]
#                 sectional_df = cls.remove_empty_columns(sectional_df)
#                 regional_sections = cls.get_regional_sections(sectional_df, sectional_df.shape[0] - 1)
#
#                 dfs = []
#                 for region, regional_config in regional_sections.items():
#                     region = region[1:-1]
#                     regional_df = sectional_df.loc[regional_config['start'] + 1:regional_config['end'] - 1, :]
#                     regional_df = regional_df.T.reset_index(drop=True).T
#                     regional_df.reset_index(drop=True, inplace=True)
#                     indexes = regional_df[regional_df[0] == 'RATE APPLICABLE OVER  :'].index.tolist()
#                     indexes.append(regional_config['end'])
#                     indexes = zip(indexes, indexes[1:])
#
#                     for over_config in indexes:
#                         over_df = cls.fix_over_block(regional_df.loc[over_config[0]: over_config[1] - 1, :], point)
#                         over_df['region'] = region
#                         if regional_df.iloc[over_config[0], 4] != '':
#                             over_df['origin_port'] = regional_df.iloc[over_config[0], 4]
#                         dfs.append(over_df)
#
#                 df = concat(dfs, ignore_index=True, sort=False)
#                 df[f'{point}_icd'] = df[f'{point}_icd'].apply(
#                     lambda x: nan if x == 'BLANK' or x == 'Point' or x == '' or len(x) == 2 else x)
#                 df = df.dropna(subset=[f'{point}_icd'])
#                 df['charges'] = f'{point.capitalize()} arbitrary charge'
#
#                 return df.reset_index(drop=True)
#
#         @classmethod
#         def _6_3(cls, df, config):
#             if not config:
#                 return
#             return cls.arbitary_fix(df, config, 'origin')
#
#         @classmethod
#         def _6_4(cls, df, config):
#             if not config:
#                 return
#             return cls.arbitary_fix(df, config, 'destination')
#
#         @classmethod
#         def split(cls, port):
#             temp = port.split(", ")
#             if len(temp) == 3:
#                 return [", ".join(temp[:2]), temp[2]]
#             else:
#                 return [", ".join(temp), '']
#
#         @classmethod
#         def fix_port_names(cls, df):
#             for point in ['origin', 'destination']:
#                 change = False
#                 if point + '_icd' in df:
#                     df[point + '_icd'] = df[point + '_icd'].apply(lambda x: cls.split(x)[0])
#                     change = True
#                 if point + '_port' in df:
#                     df[point + '_port'] = df[point + '_port'].apply(lambda x: cls.split(x)[0])
#                     change = True
#
#                 if change:
#                     df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
#                     df = df.reset_index(drop=True)
#
#             return df
#
#         def format_output(self, dfs, am_no, inclusions):
#             output = {}
#
#             freight_df = dfs[0]
#
#             if dfs[1] is not None:
#                 freight_df = concat([freight_df, dfs[1]], ignore_index=True, sort=False)
#
#             freight_df = freight_df.drop(columns=["type", "direct", "note"])
#             freight_df = self.fix_port_names(freight_df)
#             freight_df['amendment_no'] = am_no
#
#             """Adding inclusions"""
#             if inclusions is not None:
#                 for region in inclusions:
#                     freight_df.loc[(freight_df['region'] == region), 'inclusions'] = inclusions[region][0]
#
#             output['Freight'] = freight_df
#
#             origin_df = None
#             if dfs[2] is not None:
#                 origin_df = dfs[2]
#                 origin_df = self.fix_port_names(origin_df)
#                 origin_df['amendment_no'] = am_no
#                 origin_df = origin_df.rename(columns={'origin_icd': 'icd', 'origin_port': 'to'})
#                 origin_df['at'] = 'origin'
#
#                 region_list = origin_df['region'].unique().tolist()
#                 origin_df['expiry_date'].replace('', nan, inplace=True)
#                 origin_df['start_date'].replace('', nan, inplace=True)
#
#                 if self.validity:
#                     for region in region_list:
#                         if region in self.validity:
#                             start_date, expiry_date = list(self.validity[region].values())
#                             origin_df.loc[origin_df['start_date'].isna() &
#                                           origin_df['region'].str.contains(region), 'start_date'] = start_date
#                             origin_df.loc[origin_df['expiry_date'].isna() &
#                                           origin_df['region'].str.contains(region), 'expiry_date'] = expiry_date
#                         else:
#                             start_date, expiry_date = list(self.validity['all'].values())
#                             origin_df.loc[origin_df['start_date'].isna(), 'start_date'] = start_date
#                             origin_df.loc[origin_df['expiry_date'].isna(), 'expiry_date'] = expiry_date
#                 else:
#                     start_date, expiry_date = '', ''
#                     origin_df.loc[origin_df['start_date'].isna(), 'start_date'] = start_date
#                     origin_df.loc[origin_df['expiry_date'].isna(), 'expiry_date'] = expiry_date
#
#             destination_df = None
#             if dfs[3] is not None:
#                 destination_df = dfs[3]
#                 destination_df = self.fix_port_names(destination_df)
#                 destination_df['amendment_no'] = am_no
#                 destination_df = destination_df.rename(columns={'destination_icd': 'icd', 'destination_port': 'to'})
#                 destination_df['at'] = 'destination'
#
#                 region_list = destination_df['region'].unique().tolist()
#                 destination_df['expiry_date'].replace('', nan, inplace=True)
#                 destination_df['start_date'].replace('', nan, inplace=True)
#
#                 if self.validity:
#                     for region in region_list:
#                         if region in self.validity:
#                             start_date, expiry_date = list(self.validity[region].values())
#                             destination_df.loc[destination_df['start_date'].isna() &
#                                                destination_df['region'].str.contains(region), 'start_date'] = start_date
#                             destination_df.loc[destination_df['expiry_date'].isna() &
#                                                destination_df['region'].str.contains(
#                                                    region), 'expiry_date'] = expiry_date
#                         else:
#                             start_date, expiry_date = list(self.validity['all'].values())
#                             destination_df.loc[origin_df['start_date'].isna(), 'start_date'] = start_date
#                             destination_df.loc[origin_df['expiry_date'].isna(), 'expiry_date'] = expiry_date
#                 else:
#                     start_date, expiry_date = '', ''
#                     destination_df.loc[destination_df['start_date'].isna(), 'start_date'] = start_date
#                     destination_df.loc[destination_df['expiry_date'].isna(), 'expiry_date'] = expiry_date
#
#             if origin_df is not None or destination_df is not None:
#                 arbitrary_df = concat([origin_df, destination_df], ignore_index=True, sort=False)
#
#                 output['Arbitrary Charges'] = arbitrary_df
#
#             return output
#
#         def get_amendment_no(self):
#             index = self.df[self.df[0].str.startswith('SERVICE CONTRACT NO', na=False)].index.values[0]
#             return self.df[0][index].split()[-1]
#
#         def get_inclusions(self):
#             inclusions = defaultdict(list)
#             start_index = list(self.df[(self.df[0].str.contains('C. EXCEPTIONS', na=False))].index)
#             end_index = self.df.tail(1).index.values[0] + 1
#             inclusions_table_df = self.df[start_index[0]:end_index].copy(deep=True)
#             inclusions_table_df.reset_index(drop=True, inplace=True)
#             inc_start = list(inclusions_table_df[(inclusions_table_df[0].str.startswith('[', na=False))].index)
#             inc_start.append(inclusions_table_df.tail(1).index.values[0])
#             for _index in range(len(inc_start) - 1):
#                 inclusions_df = inclusions_table_df[inc_start[_index]:inc_start[_index + 1]].copy(deep=True)
#                 inclusions_df.reset_index(drop=True, inplace=True)
#                 if inclusions_df[3].str.contains('Rates are inclusive of', na=False).any():
#                     start_inc = list(
#                         inclusions_df[(inclusions_df[3].str.contains('Rates are inclusive of', na=False))].index)
#                     inclusions_ch = inclusions_df.iloc[start_inc[0], 3]
#                     group_name = inclusions_df.iloc[0, 0]
#                     regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
#                     included_list = []
#                     matches_inc = re.finditer(regex_incl, inclusions_ch, re.MULTILINE)
#                     for matchNum, match in enumerate(matches_inc, start=1):
#                         for groupNum in range(0, len(match.groups())):
#                             groupNum = groupNum + 1
#                             included_list.append(match.group(groupNum))
#                     included_list = ','.join(included_list)
#                     regex = r"\[|]"
#                     group_name = re.sub(regex, '', group_name, 0, re.MULTILINE)
#                     group_name = group_name.strip()
#                     inclusions[group_name].append(included_list)
#
#             return inclusions
#
#         def capture(self):
#
#             sections = self.get_sections()
#
#             amendment_no = self.get_amendment_no()
#
#             self.set_validity_from_section_8()
#
#             # self.set_load_type_map()
#
#             inclusions_dict = self.get_inclusions()
#
#             dfs = []
#             for section, config in sections.items():
#                 if config is not None:
#                     section = '_' + section.replace('-', '_')
#                     fix = getattr(self, section)
#                     dfs.append(fix(self.df, config))
#                 else:
#                     dfs.append(None)
#
#             self.captured_output = self.format_output(dfs, amendment_no, inclusions_dict)
#
#         def map_load_type(self, df):
#             if '20GP_PSS' in df:
#                 charge_profile = ["20GP", "40GP", '40HC', "45HC", "20GP_PSS", "40GP_PSS", "40HC_PSS", "45HC_PSS"]
#             else:
#                 charge_profile = ["20GP", "40GP", '40HC', "45HC"]
#             df = df.melt(
#                 id_vars=[column for column in df.columns if column not in charge_profile],
#                 value_vars=charge_profile, value_name='amount', var_name='load_type')
#             df['amount'] = df['amount'].astype(str)
#             df.loc[df['amount'].str.contains("/", na=False), 'load_type'] = \
#                 df.loc[df['amount'].str.contains("/", na=False), 'amount'].str.split("/").str[0]
#
#             df['load_type'] = df['load_type'].apply(
#                 lambda x: x if x not in self.load_type_map else self.load_type_map[x])
#             df['amount'] = df['amount'].str.split("/").str[-1]
#             df['load_type'] = df['load_type'].replace('20\' Flat Rack', '20FR')
#             df['load_type'] = df['load_type'].replace('40\' Flat Rack', '40FR')
#             df['load_type'] = df['load_type'].replace('20\' Open Top', '20OT')
#             df['load_type'] = df['load_type'].replace('40\' Open Top', '40OT')
#             df['load_type'] = df['load_type'].replace('20\' Reefer', '20RE')
#             df['load_type'] = df['load_type'].replace('40\' Reefer High Cube', '40HR')
#             df['amount'] = df['amount'].replace('', nan)
#             df['amount'] = df['amount'].replace('nan', nan)
#             df = df.dropna(subset=['amount'])
#             df = df.reset_index(drop=True)
#
#             return df
#
#         def port_lookup(self, df):
#             if self.df.loc[self.df[0].str.startswith('6. CONTRACT RATES', na=False)].index.any():
#                 start_index = self.df.loc[self.df[0].str.startswith('6. CONTRACT RATES', na=False)].index.values[0]
#                 end_index = self.df.loc[self.df[0].str.contains('6-1. General Rate', na=False)].index.values[0]
#                 port_df_all = self.df[start_index + 1:end_index - 1].copy(deep=True)
#                 port_df_all.reset_index(drop=True, inplace=True)
#                 indexes = port_df_all[port_df_all[0].str.startswith('[', na=False) &
#                                       port_df_all[0].str.endswith(']', na=False)].index.tolist()
#                 indexes += [port_df_all.tail(1).index.values[0] + 1]
#
#                 lookup_dict = {}
#                 for index in range(len(indexes) - 1):
#                     group = port_df_all.iloc[indexes[index], 0][1:-1]
#                     port_df = port_df_all.loc[indexes[index] + 1:indexes[index + 1]].copy(deep=True)
#                     port_df.replace('', nan, inplace=True)
#                     port_df.dropna(axis=1, inplace=True, how='all')
#                     port_df.columns = ['Name', 'Pairs']
#                     port_df.dropna(subset=['Pairs'], axis=0, inplace=True)
#                     port_df = port_df.fillna(method='ffill')
#                     port_df_grouped = port_df.groupby(['Name'], as_index=False)[
#                         'Pairs'].apply(lambda x: ';'.join(x)).reset_index(drop=True)
#                     lookup = dict(zip(port_df_grouped['Name'].tolist(), port_df_grouped['Pairs'].tolist()))
#                     lookup_dict[group] = lookup
#                     index_region = df.loc[(df['region'].str.contains(group))].index.tolist()
#                     df.loc[index_region] = df.loc[index_region].replace(lookup, regex=True)
#                 # freight_df.replace(lookup, inplace=True, regex=True)
#
#             return df
#
#         def clean(self):
#
#             freight_df = self.captured_output['Freight']
#             freight_df.drop(columns=['destination_country'], inplace=True)
#             for c in product(['origin', 'destination'], ['icd', 'port']):
#                 _column = c[0] + '_' + c[1]
#                 if c[1] == 'icd':
#                     freight_df[_column] = freight_df[_column].str.replace("\n", ";")
#                 freight_df[_column] = freight_df[_column].str.split('\n')
#                 freight_df = freight_df.explode(_column)
#                 freight_df = freight_df.reset_index(drop=True)
#             freight_df['origin_icd'] = freight_df['origin_icd'].str.rstrip("(CY)")
#
#             freight_df = self.map_load_type(freight_df)
#             freight_df = self.get_commodity_desc(freight_df)
#
#             """
#             Hard coded will replace once look up is ready
#             """
#
#             freight_df = self.port_lookup(freight_df)
#
#             """ Premium column"""
#             freight_df.loc[freight_df['region'].str.contains('6-2. Special Rate') & freight_df['bulletin'].str.contains(
#                 'PB Product'), 'premium_service'] = 'premium_sml'
#
#             self.cleaned_output = {'Freight': freight_df}
#
#             if 'Arbitrary Charges' in self.captured_output:
#                 arbitrary_df = self.captured_output['Arbitrary Charges']
#
#                 """No Destination Arb"""
#                 if 'destination_country' in arbitrary_df:
#                     arbitrary_df.drop(columns=['destination_country'], inplace=True)
#                 elif 'origin_country' in arbitrary_df:
#                     arbitrary_df.drop(columns=['origin_country'], inplace=True)
#                 arbitrary_df['to'] = arbitrary_df['to'].str.rstrip("Rate")
#                 arbitrary_df['to'] = arbitrary_df['to'].str.strip()
#                 arbitrary_df['icd'] = arbitrary_df['icd'].str.replace("\n", ";")
#                 arbitrary_df = self.map_load_type(arbitrary_df)
#                 if 'remarks' in arbitrary_df:
#                     arbitrary_df.drop(columns=['remarks'], inplace=True)
#
#                 arbitrary_df = self.port_lookup(arbitrary_df)
#
#                 self.cleaned_output['Arbitrary Charges'] = arbitrary_df


class Ceva_Sm_Usa(BaseTemplate):
    class Ceva_Sm_Usa_1(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def pivot_load_type(self,df):
            df = df.rename(columns={'BOF': 'amount'})
            columns_list = list(df.columns)
            load_type_index = [columns_list.index(i) for i in columns_list if 'load_type' == i]
            expiry_date_index = [columns_list.index(i) for i in columns_list if 'expiry_date' == i]
            currency_column_list = columns_list[load_type_index[0]+1:expiry_date_index[0]]
            currency_column_list_with_load_type = currency_column_list.copy()
            currency_column_list_with_load_type.append('load_type')
            if "load_type" in df:
                df = df.fillna('')
                df = df.pivot_table(index=[column for column in df.columns if column not in currency_column_list_with_load_type],
                                    columns=['load_type'],
                                    values=currency_column_list,
                                    aggfunc='first')
                df = df.reset_index()
                new_columns = []
                for index in df.columns.to_flat_index():
                    if index[0] in currency_column_list:
                        new_columns.append(index[1]+'_'+index[0])
                    else:
                        new_columns.append(index[0])
                df.columns = new_columns
            return df

        def capture(self):
            index_POL = list(self.df[(self.df[0].str.contains("TLI Number", na=False))].index)
            freight_df = self.df.iloc[index_POL[0]:self.df.tail(1).index.values[0] + 1, :].copy(deep=True)
            freight_df.columns = freight_df.iloc[0]
            freight_df = freight_df.iloc[1:].copy()
            date_index = list(self.df[(self.df[0].str.contains("Expiration Date:"))].index)[0]
            expiry_date = self.df.iloc[date_index, 1]
            contract_index = list(self.df[(self.df[0].str.contains("Contract No.:"))].index)[0]
            contract_no = self.df.iloc[contract_index, 1]
            freight_df['expiry_date'] = expiry_date
            freight_df['contract_no'] = contract_no
            freight_df['Size'] = freight_df['Size'].str.replace('20', '20GP', regex=True)
            freight_df['Size'] = freight_df['Size'].str.replace('40X', '40GP', regex=True)
            freight_df['Size'] = freight_df['Size'].str.replace('45S', '45HC', regex=True)
            freight_df.rename(columns={'Origin': 'origin_port', 'Destination': 'destination_port','Origin': 'origin_icd','Port Of Loading': 'origin_port'\
                                       ,'Port Of Discharge':'destination_port','Destination':'destination_icd','Equipment Tyepe':'Equipment Type',\
                                       'Package Code':'Leg Type','Size':'load_type'}, inplace=True)
            freight_df.reset_index(drop=True, inplace=True)
            for i in range(freight_df.shape[0]):
                if freight_df['Origin Via'][i] != '':
                    freight_df['Origin Via'][i] = 'Origin Via' + '-' + freight_df['Origin Via'][i]
                if freight_df['Destination Via'][i] != '':
                    freight_df['Destination Via'][i] = 'Destination Via' + '-' + freight_df['Destination Via'][i]
            freight_df['remarks'] = freight_df['Origin Via'] + ',' + freight_df['Destination Via']
            freight_df['remarks'] = freight_df['remarks'].apply(lambda x: x.strip(','))
            freight_df['Mode_of_Transportation_Orgin'] = freight_df['Service Combo'].str.split('to', expand=True)[0]
            freight_df['Mode_of_Transportation_Destination'] = freight_df['Service Combo'].str.split('to', expand=True)[1]
            freight_df['unique'] = ''
            freight_df.drop(['Service Combo', 'Total (Estimated)','TLI Number','Origin Via','Destination Via','Notes'], axis=1, inplace=True)
            freight_df.reset_index(drop=True, inplace=True)
            freight_df = freight_df.loc[(freight_df['Equipment Type'] != 'Not Applicable') & (freight_df['load_type'] != '')]
            freight_df.reset_index(drop=True, inplace=True)
            for i in range(freight_df.shape[0]):
                if 'Reefer' in freight_df['Equipment Type'][i]:
                    freight_df['load_type'][i] = freight_df['load_type'][i][0:2] + 'RE'
                if 'Non Operating Reefer' in freight_df['Equipment Type'][i]:
                    freight_df['load_type'][i] = freight_df['load_type'][i][0:2] + 'NOR'
            freight_df = self.pivot_load_type(freight_df)
            self.captured_output = {"Freight": freight_df}

        def clean(self):
            self.cleaned_output = self.captured_output


