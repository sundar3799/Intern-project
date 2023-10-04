
import datetime
import re
from itertools import product
import pandas as pd
from dateutil.parser import parse
import pandas as pd
from numpy import nan
from pandas import concat
from logging import getLogger
from collections import defaultdict
import pandas as pd
from base import BaseTemplate, BaseFix
from custom_exceptions import InputValidationError
from dateutil.parser import parse
from dps_headers.headers import get_headers
from Utility import df_utils
log = getLogger(__name__)

from util import remarks_util
log = getLogger(__name__)


class Flexport_ONEY_TransAtlantic_v1(BaseTemplate):
    class _TransAtlantic(BaseFix):

        def __init__(self, df, file_model: dict, headers_config: dict):
            super().__init__(df, file_model, headers_config)

            self.validity = {}
            self.load_type_map = {}

        def check_input(self):
            check_errors = []
            if not self.df[0].str.contains('6-1').any():
                check_errors.append("Section definition '6-1' should be present in first Column")

            if not (self.df[0].str.startswith('[') & self.df[0].str.endswith(']')).any():
                check_errors.append("Region definition should be present in first column and should be enclosed in []")

            if not self.df[0].str.contains('Code').any():
                check_errors.append("Load Type mapping table should be defined along first column under section "
                                    "header 'Code'")

            if check_errors:
                raise InputValidationError(check_errors)

        def check_output(self):
            pass

        def get_sections(self):
            sections_to_check = ['6-1', '6-2', '6-3', '6-4', '6-5', '7. LIQUIDATED']
            sections = {}

            previous_section = None
            for check in sections_to_check:
                if self.df[self.df[0].str.startswith(check)].index.values:
                    index = self.df[self.df[0].str.startswith(check)].index.values[0]
                    sections[check] = {'start': index, 'end': None}

                    if previous_section:
                        sections[previous_section]['end'] = index

                    previous_section = check
                else:
                    sections[check] = None

            for drop in ['6-5', '7. LIQUIDATED']:
                if drop in sections:
                    sections.pop(drop)

            return sections

        def set_validity_from_section_8(self):

            if self.df[self.df[0] == '8. DURATION OF THE CONTRACT'].index.values:
                start_index = self.df[self.df[0] == '8. DURATION OF THE CONTRACT'].index.values[0]
                end_index = self.df[self.df[0].str.startswith("9. SIGNATURE")].index.values[0]

                validity_df = self.df.loc[start_index: end_index - 1, :]
                validity_df = validity_df.applymap(lambda x: nan if x == '' else x)
                validity_df = validity_df.dropna(axis=1, how='all')
                validity_df = validity_df.reset_index(drop=True)
                validity_df = validity_df.T.reset_index(drop=True).T

                self.validity['all'] = {
                    'start_date': datetime.datetime.strptime(validity_df[1][1], "%d %b, %Y").date().isoformat(),
                    'expiry_date': datetime.datetime.strptime(validity_df[3][1], "%d %b, %Y").date().isoformat()}

                region_indexes = validity_df[
                    validity_df[0].str.startswith('[') & validity_df[0].str.endswith(']')].index.tolist()

                for index in region_indexes:
                    self.validity[validity_df[0][index]] = {
                        'start_date': datetime.datetime.strptime(validity_df[1][index + 1],
                                                                 "%d %b, %Y").date().isoformat(),
                        'expiry_date': datetime.datetime.strptime(validity_df[1][index + 1],
                                                                  "%d %b, %Y").date().isoformat()}

        def set_load_type_map(self):
            start_index = self.df[self.df[0] == 'Code'].index.values[0]

            load_df = self.df.loc[start_index + 1:, :]
            load_df = load_df.applymap(lambda x: nan if x == '' else x)
            load_df = load_df.dropna(axis=1, how='all')
            load_df = load_df.reset_index(drop=True)
            load_df = load_df.T.reset_index(drop=True).T
            self.load_type_map = load_df.set_index(0).to_dict()[1]

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
            indexes = df[df[0].str.startswith('[') & df[0].str.endswith(']')].index.tolist()
            indexes.append(section_end + 1)
            indexes = zip(indexes, indexes[1:])

            for config in indexes:
                region = df[0][config[0]]
                regional_sections[region] = {'start': config[0], 'end': config[1]}

            return regional_sections

        @classmethod
        def get_validity_and_remarks(cls, notes):
            notes = notes.split('\n')
            date_flag = 0
            note_included = []
            note_not_included = []
            service = []
            start_date = ''
            expiry_date = ''
            for note in notes:
                if note.startswith("Rates are valid"):
                    if re.search("^Rates are valid (from \d+)? ?(to \d+)?\.?$", note):
                        groups = re.findall("^Rates are valid (from \d+)? ?(to \d+)?\.?$", note)
                        start_date, expiry_date = groups[0]
                        if start_date:
                            date_flag = 1
                            start_date = start_date.split()[1]
                            start_date = start_date[:4] + '-' + start_date[4:6] + '-' + start_date[6:]
                        if expiry_date:
                            date_flag = 1
                            expiry_date = expiry_date.split()[1]
                            expiry_date = expiry_date[:4] + '-' + expiry_date[4:6] + '-' + expiry_date[6:]
                elif note.startswith("Rates are inclusive"):
                    note_included = []
                    regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                    matches_inc_br = re.finditer(regex_incl, note, re.MULTILINE)
                    for matchNum_not_inc, match_not_inc in enumerate(matches_inc_br, start=1):
                        for groupNum_not_inc in range(0, len(match_not_inc.groups())):
                            groupNum_not_inc = groupNum_not_inc + 1
                            note_included.append(
                                match_not_inc.group(groupNum_not_inc))

                elif note.startswith("Rates are subject"):
                    note_not_included = []
                    regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                    matches_inc_br = re.finditer(regex_incl, note, re.MULTILINE)
                    for matchNum_not_inc, match_not_inc in enumerate(matches_inc_br, start=1):
                        for groupNum_not_inc in range(0, len(match_not_inc.groups())):
                            groupNum_not_inc = groupNum_not_inc + 1
                            note_not_included.append(
                                match_not_inc.group(groupNum_not_inc))

                elif note.startswith("Valid"):
                    note_not_included = []
                    regex_incl = r"Valid (.+?)\sto\s(.+?)$"
                    matches_valid = re.findall(regex_incl, note, re.MULTILINE)
                    start_date, expiry_date = matches_valid[0]

                elif note.startswith("For Service Loop:"):
                    regex_incl = r"For Service Loop: (.+?)$"
                    matches_serivce = re.findall(regex_incl, note, re.MULTILINE)
                    service.append(matches_serivce[0])

                elif date_flag == 0:
                    expiry_date = ''
                    start_date = ''

                # elif re.search("^Rates are valid from", notes[0]):
                #     groups = re.findall("^Rates are valid from (\d+)$", notes[0])
                #     start_date = groups[0]
                #     if start_date:
                #         start_date = start_date[:4] + '-' + start_date[4:6] + '-' + start_date[6:]
                #     expiry_date = ''
                #
                # elif re.search("^Rates are valid to ", notes[0]):
                #     groups = re.findall("^Rates are valid to (\d+)$", notes[0])
                #     expiry_date = groups[0]
                #     if expiry_date:
                #         expiry_date = expiry_date[:4] + '-' + expiry_date[4:6] + '-' + expiry_date[6:]
                #     start_date = ''

            notes = "\n".join(notes[1:])

            return start_date, expiry_date, notes, note_included, note_not_included, service

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

            """Customer name hard coded temp"""

            master_date = ['AEROGROW INTERNATIONAL INC',
                           'ALL BIRDS',
                           'AMERICAN METALCRAFT, INC.',
                           'ANATOMY SUPPLY PARTNERS (ASP GLOBAL) INC',
                           'APP GROUP INC',
                           'APPLE',
                           'ARCHITECTURAL MAILBOXES LLC',
                           'ARDEN',
                           'ARROWHEAD ELECTRICAL PRODUCTS INC',
                           'ARVCO CONTAINER',
                           'ASHLEY FURNITURE',
                           'B & W FIBER GLASS INC',
                           'BADCOCK',
                           'BALSAM BRANDS',
                           'BEST CHOICE PRODUCTS',
                           'BLU DOT DESIGN MANUFACTURING INC',
                           'BLUE SKY, THE COLOR OF IMAGINATION',
                           'BOB MILLS',
                           'BOLL & BRANCH',
                           'BOWERS & WILKINS (UK) GROUP LIMITED',
                           'BOYD SLEEP',
                           'BRONDELL, INC.',
                           'BROOKLYN LOLLIPOPS IMPORT CORP.',
                           'BURROW',
                           'CASPER SLEEP',
                           'CHIC HOME',
                           'CISCO MERAKI',
                           'CITY FURNITURE, INC.',
                           'CLEVA NORTH AMERICA',
                           'COMFORT PRODUCTS INC',
                           'CORKCICLE, LLC',
                           'CORONA CLIPPER',
                           'COSMIC PET (HYPER PET)',
                           'COSTCO',
                           'DANSONS',
                           'DEI SALES, INC',
                           'DESIGNER BRANDS',
                           'DHI CORP',
                           'DIONO LLC',
                           'DIRECTBUY HOME IMPROVEMENT INC',
                           'DMA',
                           'DOVE TAIL FURNITURE AND DESIGNS',
                           'ECHO ENGINEERING & PRODUCTION SUPPLIES INC.',
                           'ELLISON EDUCATIONAL',
                           'ELO TOUCH',
                           'ENGLEWOOD MARKETING LLC',
                           'FANACO FASTENER',
                           'FC BRANDS LLC',
                           'FIRST SOLAR',
                           'FITNESSCUBED',
                           'FLEET PRIDE',
                           'FLOOR AND DECOR - GU STRATEGIS FLOOR AND DECOR',
                           'FLORA CLASSIQUE',
                           'FOREST PRODUCTS DISTRIBUTORS, INC.',
                           'FORME LIFE',
                           'FORMOSA PLASTICS CORP. AMERICA',
                           'GATOR CASES INCORPORATED',
                           'GERBER PLUMBING FIXTURES LLC',
                           'GIMME HEALTH',
                           'GLOBAL FURNITURE USA',
                           'GLOBAL-PAK',
                           'GOLIATH',
                           'GRAND & BENEDICTS INC',
                           'GROVE COLLABORATIVE',
                           'GSM OUTDOORS',
                           'GUARDIAN BIKE COMPANY',
                           'KRAFT HEINZ',
                           'HAMILTON BEACH',
                           'HAPPIEST BABY INC.',
                           'HARLAND M BRAUN',
                           'HARTLAND CONTROLS',
                           'HARWARD MEDIA',
                           'HKC-US LLC',
                           'HOME FASHIONS DISTRIBUTOR',
                           'HOMEWERKS WORLDWIDE LLC',
                           "HUDSON'S BAY CO",
                           'HUNTER FAN COMPANY',
                           'IGLOO PRODUCTS CORP.',
                           'IMPRESSIONS VANITY COMPANY',
                           'INTERIOR DEFINE INC',
                           'ITW GLOBAL BRANDS',
                           'ITW BRANDS DIVISION ITW RESIDENTIAL AND RENOVATION',
                           'JAZWARES LLC',
                           'JR286',
                           'KEENEY MANUFACTURING',
                           'KENAS HOME',
                           'KID KRAFT',
                           'KINGSLEY-BATE LTD',
                           "KIRKLAND'S HOME STORES",
                           'KOLCRAFT ENTERPRISES INC',
                           'LANDING',
                           'LA-Z-BOY INC',
                           'LEATHER ITALIA USA',
                           'LOVEVERY INC',
                           'LULULEMON',
                           'MAGGY LONDON INTERNATIONAL LTD',
                           'MANNINGTON MILLS',
                           'MARTIN SVENSSON HOME LLC',
                           'M-D BUILDING PRODUCTS, INC',
                           'METHOD PRODUCTS',
                           'MIDEA AMERICA CORP',
                           'MILLION DOLLAR BABY',
                           'MITCHELL GOLD, BOB WILLIAMS',
                           'MODLOFT',
                           'MOOSE TOYS',
                           'NATIONAL PUBLIC SEATING',
                           'NATIONAL SPORTING GOODS',
                           'NEARLY NATURAL',
                           'NEWAGE PRODUCTS INC',
                           'NEW AIR',
                           'NINGBO ANNTO LOGISTICS TECHNOLOGY CO.',
                           'NOVILAND INTERNATIONAL LLC',
                           'NPC GLOBAL',
                           'OATEY',
                           'OLDE THOMPSON LLC',
                           'ON AG',
                           'OSTERMAN & COMPANY INC.',
                           'OUR PLACE',
                           'OUTDOOR CAP COMPANY, INC.',
                           'OUTER INC',
                           'PACKNWOOD',
                           'PAMPERED CHEF',
                           'PEAK ACHIEVEMENT ATHLETICS',
                           'PELOTON',
                           'PGP INTERNATIONAL INC',
                           'PKDC, LLC',
                           'PRIMO INTERNATIONAL',
                           'RADIANS INC.',
                           'RELIABLE OF MILWAUKEE',
                           'RG BARRY',
                           'RGI INC.',
                           'RICHARDS HOMEWARES INC.',
                           'RIVERSIDE FURNITURE',
                           'ROLLER DERBY SKATE CORPORATION',
                           'ROOCHI TRADERS, INC.',
                           'RURAL KING',
                           'SCHLEICH-GMBH',
                           'SERENA AND LILY',
                           'SEVES GROUP PPC INSULATORS',
                           'SF EXPRESS CHINA',
                           'SHENZHEN BING BINGPAPER',
                           'SHENZHEN HOSHINE SUPPLY CHAIN',
                           'SHIMANO INC.',
                           'SIGMA RECYCLING INC',
                           'SJ CREATIONS',
                           'SKULL CANDY',
                           'SKYCORP DISTRIBUTION LLC',
                           'SLUMBERLAND FURNITURE',
                           'SNOW JOE, LLC',
                           'SONOS INC',
                           'SONOS',
                           'SPECIALIZED',
                           'STAFAST PRODUCTS, INC',
                           'SUNBELT MARKETING INVESTMENT CORPORATION',
                           'TASKMASTER COMPONENTS',
                           'TAYLOR MADE GOLF CO LTD',
                           'TEAM INTERNATIONAL GROUP OF AMERICA',
                           'TEMPO',
                           'THE ALLEN COMPANY, INC',
                           'THE BOTTLE CREW',
                           'THE CLOROX INT. COMPANY',
                           'CLOROX',
                           'THE SCOTTS COMPANY',
                           'THRO',
                           'THULE, INC.',
                           'THUMA INC',
                           'TIDI',
                           'TINGLEY RUBBER',
                           'TITUS GROUP INC',
                           'TOUGHBUILT INDUSTRIES , INC.',
                           'TOY FACTORY LLC',
                           'TOYSMITH',
                           'TRADEMARK GLOBAL, LLC',
                           'TRAEGER PELLET GRILLS LLC',
                           'TRAEGER GRILLS',
                           'TRICON DRY CHEMICALS, LLC',
                           'TRUDEAU CORPORATION',
                           'TRUE BRANDS',
                           'TURN5',
                           'UNIEK INC.',
                           'UNIQUE USA',
                           'VARI',
                           'VINEYARD VINES',
                           'VIVO',
                           'VOLEX',
                           'VOLUME DISTRIBUTORS INC',
                           'VOYETRA TURTLE BEACH INC',
                           'WAC LIGHTING',
                           'WATER PIK, INC.',
                           'WATTS WATER',
                           'WAY INTERGLOBAL NETWORK, LLC',
                           'WHITMOR INC',
                           'WHO GIVES A CRAP',
                           'WHOLESALE WHEEL & TIRE LLC',
                           'YAHEETECH',
                           'YELLOW LUXURY',
                           'ZODIAC POOL SYSTEMS LLC']

            commodity = block[2].values[0]
            if block[1].str.contains('ACTUAL CUSTOMER').any():
                index = block[(block[1].str.contains('ORIGIN'))].index.values[0]
                start_index = block[(block[1].str.contains('ACTUAL CUSTOMER'))].index.values[0]

                if (index - start_index) == 1:
                    customer_name = block[2].values[1]
                    if customer_name not in master_date:
                        customer_name = ''
                else:
                    customer_name = block[2][start_index:index].values.tolist()
                    for name in customer_name:
                        if name not in master_date:
                            customer_name.remove(name)
                    customer_name = ','.join(customer_name)
            else:
                customer_name = ''

            bulletin = block[0].values[0]

            if block[block[0] == '< NOTE FOR COMMODITY >'].index.values:
                index_of_notes = block[block[0] == '< NOTE FOR COMMODITY >'].index.values[0]
                notes = block[0][index_of_notes + 1]
                origin_indexes = block[block[1] == 'ORIGIN'].index.tolist()
                origin_indexes.append(index_of_notes)
            else:
                notes = ''
                origin_indexes = block[block[1] == 'ORIGIN'].index.tolist()
                origin_indexes.append(block.index.values[-1] + 1)

            start_date, expiry_date, remarks, note_included, note_not_included, service = cls.get_validity_and_remarks(
                notes)

            origin_config = zip(origin_indexes, origin_indexes[1:])

            dfs = []
            for config in origin_config:
                origin_block = block.loc[config[0]:config[1] - 1, :]
                origin_block = origin_block.applymap(lambda x: nan if x == '' else x)
                origin_block = origin_block.dropna(axis=1, how='all')
                origin_block = origin_block.fillna('')

                origin = origin_block.loc[origin_block[1] == 'ORIGIN', 2].values[0]
                if origin_block.loc[origin_block[1] == 'ORIGIN VIA', 2].values:
                    origin_via = origin_block.loc[origin_block[1] == 'ORIGIN VIA', 2].values[0]
                else:
                    origin_via = ''

                # origin, origin_via = cls.fix_origin(origin, origin_via)
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
                        x: nan if x == 'BLANK' or x == 'NOTE 1 :' or x == 'Destination' or x == 'DO NOT USE - PHUOC LONG' else x)
                df = df.dropna(subset=['destination_icd'])
                df = df.reset_index(drop=True)
                df['origin_icd'] = origin
                df['origin_port'] = origin_via
                dfs.append(df)

            df = concat(dfs, ignore_index=True, sort=False)
            df['commodity'] = commodity
            df['customer_name'] = customer_name
            df['start_date'] = start_date
            df['expiry_date'] = expiry_date
            df['bulletin'] = bulletin
            if note_included:
                df['inclusions'] = ','.join(note_included)
            if note_not_included:
                df['subject_to'] = ','.join(note_not_included)
            if service:
                df['loop'] = ','.join(service)

            return df

        def _6_1(self, df, config):

            if config['end'] - config['start'] == 1:
                log.info(f"Section starting from {config['start']} has no data")
                return None

            sectional_df = df[config['start']:config['end']]
            sectional_df = self.remove_empty_columns(sectional_df)
            regional_sections = self.get_regional_sections(sectional_df, sectional_df.shape[0] - 1)

            dfs = []
            for region, regional_config in regional_sections.items():
                region = region[1:-1]
                regional_df = sectional_df.loc[regional_config['start'] + 1:regional_config['end'] - 1, :]
                regional_df = regional_df.T.reset_index(drop=True).T
                indexes = regional_df[regional_df[0].str.match('^\d+\)$')].index.tolist()
                indexes.append(regional_config['end'])
                indexes = zip(indexes, indexes[1:])

                for commodity_config in indexes:
                    commodity_df = self.fix_commodity_block(
                        regional_df.loc[commodity_config[0]: commodity_config[1] - 1, :])
                    commodity_df['region'] = region
                    if self.validity:
                        if region in self.validity:
                            start_date, expiry_date = list(self.validity[region].values())
                        else:
                            start_date, expiry_date = list(self.validity['all'].values())
                    else:
                        start_date, expiry_date = '', ''
                    commodity_df.loc[commodity_df['start_date'] == '', 'start_date'] = start_date
                    commodity_df.loc[commodity_df['expiry_date'] == '', 'expiry_date'] = expiry_date
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

            index_of_notes = block[block[0].str.startswith("NOTE")].index.tolist()
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
                                 'drop2', 'drop3', 'drop4', 'drop5', 'currency', 'drop6', '20GP', '40GP', '40HC',
                                 '45HC',
                                 'drop7', 'drop8', 'drop9']

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
                    indexes = regional_df[regional_df[0] == 'RATE APPLICABLE OVER  :'].index.tolist()
                    indexes.append(regional_config['end'])
                    indexes = zip(indexes, indexes[1:])

                    for over_config in indexes:
                        over_df = cls.fix_over_block(regional_df.loc[over_config[0]: over_config[1] - 1, :], point)
                        over_df['region'] = region
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

            origin_df = None
            if dfs[2] is not None:
                origin_df = dfs[2]
                origin_df = self.fix_port_names(origin_df)
                origin_df['amendment_no'] = am_no
                origin_df = origin_df.rename(columns={'origin_icd': 'icd', 'origin_port': 'to'})
                origin_df['at'] = 'origin'

            destination_df = None
            if dfs[3] is not None:
                destination_df = dfs[3]
                destination_df = self.fix_port_names(destination_df)
                destination_df['amendment_no'] = am_no
                destination_df = destination_df.rename(columns={'destination_icd': 'icd', 'destination_port': 'to'})
                destination_df['at'] = 'destination'

            if origin_df is not None or destination_df is not None:
                arbitrary_df = concat([origin_df, destination_df], ignore_index=True, sort=False)

                output['Arbitrary Charges'] = arbitrary_df

            return output

        def get_amendment_no(self):
            return self.df[0][0].split()[-1]

        def get_inclusions(self):
            inclusions = defaultdict(list)
            start_index = list(self.df[(self.df[0].str.contains('C. EXCEPTIONS', na=False))].index)
            end_index = list(self.df[(self.df[0].str.contains('GLOSSARY', na=False))].index)
            inclusions_table_df = self.df[start_index[0]:end_index[0]].copy(deep=True)
            inclusions_table_df.reset_index(drop=True, inplace=True)
            inc_start = list(inclusions_table_df[(inclusions_table_df[0].str.startswith('['))].index)
            inc_start.append(inclusions_table_df.tail(1).index.values[0])
            for _index in range(len(inc_start) - 1):
                inclusions_df = inclusions_table_df[inc_start[_index]:inc_start[_index + 1]].copy(deep=True)
                inclusions_df.reset_index(drop=True, inplace=True)
                if inclusions_df[2].str.contains('Rates are inclusive of', na=False).any():
                    start_inc = list(
                        inclusions_df[(inclusions_df[2].str.contains('Rates are inclusive of', na=False))].index)
                    inclusions_ch = inclusions_df.iloc[start_inc[0], 2]
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

            self.set_load_type_map()

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
            charge_profile = ["20GP", "40GP", '40HC', "45HC"]
            df = df.melt(
                id_vars=[column for column in df.columns if column not in charge_profile],
                value_vars=charge_profile, value_name='amount', var_name='load_type')
            df['amount'] = df['amount'].astype(str)
            df.loc[df['amount'].str.contains("/"), 'load_type'] = \
                df.loc[df['amount'].str.contains("/"), 'amount'].str.split("/").str[0]

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
            df = df.dropna(subset=['amount'])
            df = df.reset_index(drop=True)

            return df

        def client_name(self, df):
            """
            This function will take freight df and return the rates based on the date
            Args:
                df: Freight DF

            Returns:

            """
            client_name = {'AEROGROW INTERNATIONAL INC': 'AEROGROW INTERNATIONAL INC',
                           'ALL BIRDS': 'Allbirds',
                           'AMERICAN METALCRAFT, INC.': 'American Metalcraft, Inc.',
                           'ANATOMY SUPPLY PARTNERS (ASP GLOBAL) INC': 'Anatomy Supply Partners, LLC',
                           'APP GROUP INC': 'APP GROUP INC',
                           'APPLE': 'Apple',
                           'ARCHITECTURAL MAILBOXES LLC': 'Architectural Mailboxes LLC',
                           'ARDEN': 'ARDEN COMPANIES',
                           'ARROWHEAD ELECTRICAL PRODUCTS INC': 'Arrowhead Electrical Products Inc -',
                           'ARVCO CONTAINER': 'Arvco Container Corporation',
                           'ASHLEY FURNITURE': 'ASHLEY FURNITURE HOME STORE',
                           'B & W FIBER GLASS INC': 'B & W Fiberglass',
                           'BADCOCK': 'Badcock Home Furniture',
                           'BALSAM BRANDS': 'Balsam Brands Incorporated',
                           'BEST CHOICE PRODUCTS': 'BEST CHOICE PRODUCTS',
                           'BLU DOT DESIGN MANUFACTURING INC': 'Blu Dot Design Manufacturing Inc',
                           'BLUE SKY, THE COLOR OF IMAGINATION': 'Blue Sky The Color of Imagination, LLC',
                           'BOB MILLS': 'Bob Mills Furniture',
                           'BOLL & BRANCH': 'Boll & Branch',
                           'BOWERS & WILKINS (UK) GROUP LIMITED': 'Bowers & Wilkins (UK) Group Limited',
                           'BOYD SLEEP': 'Boyd Flotation, Inc.',
                           'BRONDELL, INC.': 'Brondell, Inc.',
                           'BROOKLYN LOLLIPOPS IMPORT CORP.': 'Brooklyn Lollipops Import',
                           'BURROW': 'Burrow',
                           'CASPER SLEEP': 'Casper Sleep',
                           'CHIC HOME': 'CHIC HOME',
                           'CISCO MERAKI': 'Cisco Meraki',
                           'CITY FURNITURE, INC.': 'City Furniture, Inc.',
                           'CLEVA NORTH AMERICA': 'Cleva North America',
                           'COMFORT PRODUCTS INC': 'Comfort Products, Inc.',
                           'CORKCICLE, LLC': 'CORKCICLE, LLC',
                           'CORONA CLIPPER': 'Corona Clipper Inc',
                           'COSMIC PET (HYPER PET)': 'Cosmic Pet',
                           'COSTCO': 'Costco',
                           'DANSONS': 'Dansons',
                           'DEI SALES, INC': 'DEI Sales, Inc',
                           'DESIGNER BRANDS': 'Designer Brands',
                           'DHI CORP': 'DHI Corp.',
                           'DIONO LLC': 'Diono',
                           'DIRECTBUY HOME IMPROVEMENT INC': 'DIRECTBUY HOME IMPROVEMENT INC',
                           'DMA': 'DMA',
                           'DOVE TAIL FURNITURE AND DESIGNS': 'Dovetail Furniture and Designs',
                           'ECHO ENGINEERING & PRODUCTION SUPPLIES INC.': 'Echo Engineering & Production Supplies',
                           'ELLISON EDUCATIONAL': 'Ellison Educational Equipment',
                           'ELO TOUCH': 'Elo Touch Solutions, Inc.',
                           'ENGLEWOOD MARKETING LLC': 'Englewood Marketing Group',
                           'FANACO FASTENER': 'Fanaco Fasteners',
                           'FC BRANDS LLC': 'FC Brands LLC',
                           'FIRST SOLAR': 'First Solar',
                           'FITNESSCUBED': 'FitnessCubed',
                           'FLEET PRIDE': 'FleetPride Inc',
                           'FLOOR AND DECOR - GU STRATEGIS FLOOR AND DECOR': 'Floor And Decor -',
                           'FLORA CLASSIQUE': 'Flora Classique',
                           'FOREST PRODUCTS DISTRIBUTORS, INC.': 'Forest Products Distributors',
                           'FORME LIFE': 'FormeLife',
                           'FORMOSA PLASTICS CORP. AMERICA': 'Formosa Plastics',
                           'GATOR CASES INCORPORATED': 'Gator Cases Incorporated',
                           'GERBER PLUMBING FIXTURES LLC': 'Gerber Plumbing Fixtures LLC',
                           'GIMME HEALTH': 'Gimme Health Foods',
                           'GLOBAL FURNITURE USA': 'Global Furniture USA',
                           'GLOBAL-PAK': 'Global-pak',
                           'GOLIATH': 'Goliath',
                           'GRAND & BENEDICTS INC': 'Grand + Benedicts, Inc.',
                           'GROVE COLLABORATIVE': 'Grove Collaborative',
                           'GSM OUTDOORS': 'GSM OUTDOORS',
                           'GUARDIAN BIKE COMPANY': 'Guardian Bike Company',
                           'KRAFT HEINZ': 'H.J. Heinz Holding B.V.',
                           'HAMILTON BEACH': 'Hamilton Beach Brands',
                           'HAPPIEST BABY INC.': 'Happiest Baby Inc.',
                           'HARLAND M BRAUN': 'Harland M. Braun & Co., Inc.',
                           'HARTLAND CONTROLS': 'Hartland Controls LLC',
                           'HARWARD MEDIA': 'Harward Media',
                           'HKC-US LLC': 'HKC-US (Palm Coast Imports) LLC',
                           'HOME FASHIONS DISTRIBUTOR': 'Home Fashions Distributor, Inc.',
                           'HOMEWERKS WORLDWIDE LLC': 'Homewerks Worldwide',
                           "HUDSON'S BAY CO": "Hudson's Bay Co",
                           'HUNTER FAN COMPANY': 'Hunter Fan Company Inc',
                           'IGLOO PRODUCTS CORP.': 'Igloo Products Corp.',
                           'IMPRESSIONS VANITY COMPANY': 'Impressions Vanity Company',
                           'INTERIOR DEFINE INC': 'Interior Define Inc',
                           'ITW GLOBAL BRANDS': 'ITW Global Brands',
                           'ITW BRANDS DIVISION ITW RESIDENTIAL AND RENOVATION': 'ITW Global Brands',
                           'JAZWARES LLC': 'Jazwares, LLC',
                           'JR286': 'JR286',
                           'KEENEY MANUFACTURING': 'KEENEY MANUFACTURING',
                           'KENAS HOME': 'Kenas Home -',
                           'KID KRAFT': 'KidKraft',
                           'KINGSLEY-BATE LTD': 'Kingsley-Bate',
                           "KIRKLAND'S HOME STORES": "Kirkland's Home Stores",
                           'KOLCRAFT ENTERPRISES INC': 'Kolcraft Enterprises Inc',
                           'LANDING': 'Landing',
                           'LA-Z-BOY INC': 'La-Z-Boy Inc.',
                           'LEATHER ITALIA USA': 'Leather Italia USA',
                           'LOVEVERY INC': 'Lovevery',
                           'LULULEMON': 'Lululemon',
                           'MAGGY LONDON INTERNATIONAL LTD': 'Maggy London International , ltd',
                           'MANNINGTON MILLS': 'Mannington Mills Inc.',
                           'MARTIN SVENSSON HOME LLC': 'Martin Svensson Home',
                           'M-D BUILDING PRODUCTS, INC': 'M-D BUILDING PRODUCTS, INC',
                           'METHOD PRODUCTS': 'Method Products',
                           'MIDEA AMERICA CORP': 'Midea Group',
                           'MILLION DOLLAR BABY': 'Million Dollar Baby (Bexco Enterprises)',
                           'MITCHELL GOLD, BOB WILLIAMS': 'MITCHELL GOLD + BOB WILLIAMS',
                           'MODLOFT': 'Modloft',
                           'MOOSE TOYS': 'MOOSE FAR EAST LIMITED',
                           'NATIONAL PUBLIC SEATING': 'NATIONAL PUBLIC SEATING',
                           'NATIONAL SPORTING GOODS': 'National Sporting Goods',
                           'NEARLY NATURAL': 'Nearly Natural LLC.',
                           'NEWAGE PRODUCTS INC': 'NewAge Products Inc',
                           'NEW AIR': 'Newair LLC',
                           'NINGBO ANNTO LOGISTICS TECHNOLOGY CO.': 'Ningbo Annto Logistics Technology Co',
                           'NOVILAND INTERNATIONAL LLC': 'Noviland International',
                           'NPC GLOBAL': 'NPC Global',
                           'OATEY': 'Oatey Supply Chain Services Inc.',
                           'OLDE THOMPSON LLC': 'Olde Thompson LLC',
                           'ON AG': 'On AG',
                           'OSTERMAN & COMPANY INC.': 'Osterman & Company',
                           'OUR PLACE': 'Our Place',
                           'OUTDOOR CAP COMPANY, INC.': 'Outdoor Research, LLC',
                           'OUTER INC': 'Outer',
                           'PACKNWOOD': 'Packnwood',
                           'PAMPERED CHEF': 'Pampered Chef',
                           'PEAK ACHIEVEMENT ATHLETICS': 'Peak Achievement Athletics',
                           'PELOTON': 'Peloton Interactive Inc',
                           'PGP INTERNATIONAL INC': 'PGP INTERNATIONAL INC.',
                           'PKDC, LLC': 'PKDC',
                           'PRIMO INTERNATIONAL': 'Primo International',
                           'RADIANS INC.': 'Radians Inc.',
                           'RELIABLE OF MILWAUKEE': 'Reliable of Milwaukee',
                           'RG BARRY': 'RG Barry',
                           'RGI INC.': 'RGI Inc.',
                           'RICHARDS HOMEWARES INC.': 'Richards Homewares',
                           'RIVERSIDE FURNITURE': 'Riverside Furniture',
                           'ROLLER DERBY SKATE CORPORATION': 'Roller Derby Skate Corporation',
                           'ROOCHI TRADERS, INC.': 'Roochi Traders, INC.',
                           'RURAL KING': 'Rural King',
                           'SCHLEICH-GMBH': 'Schleich-GmbH',
                           'SERENA AND LILY': 'Serena and Lily Inc.',
                           'SEVES GROUP PPC INSULATORS': 'Seves Group / PPC Insulators',
                           'SF EXPRESS CHINA': 'SF Express China',
                           'SHENZHEN BING BINGPAPER': 'Shenzhen Bingbing Paper Ltd.',
                           'SHENZHEN HOSHINE SUPPLY CHAIN': 'SHENZHEN HOSHINE SUPPLY CHAIN',
                           'SHIMANO INC.': 'Shimano',
                           'SIGMA RECYCLING INC': 'Sigma Recycling Inc',
                           'SJ CREATIONS': 'Sj Creations Incorporated',
                           'SKULL CANDY': 'Skullcandy',
                           'SKYCORP DISTRIBUTION LLC': 'SkyCorp Distribution LLC',
                           'SLUMBERLAND FURNITURE': 'Slumberland Furniture',
                           'SNOW JOE, LLC': 'Snow Joe, LLC',
                           'SONOS INC': 'Sonos Inc',
                           'SONOS': 'Sonos Inc',
                           'SPECIALIZED': 'Specialized',
                           'STAFAST PRODUCTS, INC': 'Stafast Products, Inc',
                           'SUNBELT MARKETING INVESTMENT CORPORATION': 'Sunbelt Marketing Investment Corporation',
                           'TASKMASTER COMPONENTS': 'Taskmaster Components',
                           'TAYLOR MADE GOLF CO LTD': 'TaylorMade Golf',
                           'TEAM INTERNATIONAL GROUP OF AMERICA': 'Team International Group of America',
                           'TEMPO': 'Tempo',
                           'THE ALLEN COMPANY, INC': 'The Allen Company',
                           'THE BOTTLE CREW': 'The Bottle Crew',
                           'THE CLOROX INT. COMPANY': 'The Clorox International Company',
                           'CLOROX': 'The Clorox International Company',
                           'THE SCOTTS COMPANY': 'THE SCOTTS COMPANY LLC',
                           'THRO': 'Thro',
                           'THULE, INC.': 'Thule, Inc.',
                           'THUMA INC': 'Thuma',
                           'TIDI': 'TIDI Products, LLC',
                           'TINGLEY RUBBER': 'Tingley Rubber Corporation',
                           'TITUS GROUP INC': 'Titus Group Inc.',
                           'TOUGHBUILT INDUSTRIES , INC.': 'TOUGHBUILT INDUSTRIES, INC.',
                           'TOY FACTORY LLC': 'Toy Factory LLC',
                           'TOYSMITH': 'Toysmith',
                           'TRADEMARK GLOBAL, LLC': 'Trademark Global',
                           'TRAEGER PELLET GRILLS LLC': 'Traeger Pellet Grills',
                           'TRAEGER GRILLS': 'Traeger Pellet Grills',
                           'TRICON DRY CHEMICALS, LLC': 'Tricon Dry Chemicals',
                           'TRUDEAU CORPORATION': 'Trudeau Corporation',
                           'TRUE BRANDS': 'True Brands',
                           'TURN5': 'Turn5 -',
                           'UNIEK INC.': 'Uniek Inc. -',
                           'UNIQUE USA': 'Unique USA Inc',
                           'VARI': 'VARIDESK LLC',
                           'VINEYARD VINES': 'Vineyard Vines Inc',
                           'VIVO': 'VIVO',
                           'VOLEX': 'Volex',
                           'VOLUME DISTRIBUTORS INC': 'Volume Distributors',
                           'VOYETRA TURTLE BEACH INC': 'Voyetra Turtle Beach Inc',
                           'WAC LIGHTING': 'WAC Lighting',
                           'WATER PIK, INC.': 'Water Pik, Inc.',
                           'WATTS WATER': 'Watts Water Technologies',
                           'WAY INTERGLOBAL NETWORK, LLC': 'Way Interglobal Network',
                           'WHITMOR INC': 'Whitmor Inc',
                           'WHO GIVES A CRAP': 'Who Gives A Crap',
                           'WHOLESALE WHEEL & TIRE LLC': 'Wholesale Wheel & Tire',
                           'YAHEETECH': 'Yaheetech',
                           'YELLOW LUXURY': 'Yellow Luxury',
                           'ZODIAC POOL SYSTEMS LLC': 'Zodiac Pool Systems LLC'}

            df['customer_name'].replace(client_name, regex=True, inplace=True)

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

            # freight_df.loc[freight_df['origin_icd'] == freight_df['origin_port'], 'origin_port'] = ''
            # freight_df.loc[freight_df['destination_icd'] == freight_df['destination_port'], 'destination_port'] = ''
            freight_df['origin_icd'] = freight_df['origin_icd'].str.rstrip("(CY)")

            freight_df = self.map_load_type(freight_df)

            freight_df = self.client_name(freight_df)

            self.cleaned_output = {'Freight': freight_df}

            if 'Arbitrary Charges' in self.captured_output:
                arbitrary_df = self.captured_output['Arbitrary Charges']

                """No Destination Arb"""
                if 'destination_country' in arbitrary_df:
                    arbitrary_df.drop(columns=['destination_country'], inplace=True)
                elif 'origin_country' in arbitrary_df:
                    arbitrary_df.drop(columns=['origin_country'], inplace=True)
                arbitrary_df['to'] = arbitrary_df['to'].str.rstrip(" Rate")
                arbitrary_df['icd'] = arbitrary_df['icd'].str.replace("\n", ";")
                arbitrary_df = self.map_load_type(arbitrary_df)
                if 'remarks' in arbitrary_df:
                    arbitrary_df.drop(columns=['remarks'], inplace=True)

                self.cleaned_output['Arbitrary Charges'] = arbitrary_df


class ONEY_TransAtlantic_PDF_v1(BaseTemplate):
    class _TransAtlantic(BaseFix):

        def __init__(self, df, file_model: dict, headers_config: dict):
            super().__init__(df, file_model, headers_config)

            self.validity = {}
            self.load_type_map = {}

        def check_input(self):

            check_errors = []
            if not self.df[0].str.contains('6-1').any():
                check_errors.append("Section definition '6-1' should be present in first Column")

            if not self.df[0].str.contains('< NOTE FOR COMMODITY >').any():
                check_errors.append("< NOTE FOR COMMODITY > should be present in first Column")

            if not self.df[0].str.contains('6-2').any():
                check_errors.append("Section definition '6-2' should be present in first Column")

            if check_errors:
                raise InputValidationError(check_errors)

        def check_output(self):
            pass

        def get_sections(self, dropList):
            sections_to_check = ['6-1', '6-2', '6-3', '6-4', '6-5', '7. LIQUIDATED']
            sections = {}

            previous_section = None
            for check in sections_to_check:
                if self.df[self.df[0].str.startswith(check)].index.values:
                    index = self.df[self.df[0].str.startswith(check)].index.values[0]
                    sections[check] = {'start': index, 'end': None}

                    if previous_section:
                        sections[previous_section]['end'] = index

                    previous_section = check
                else:
                    sections[check] = None

            for drop in dropList:
                if drop in sections:
                    sections.pop(drop)

            return sections

        def set_validity_from_section_8(self):

            if self.df[self.df[0].str.startswith('8. DURATION OF THE CONTRACT')].index.values:
                start_index = self.df[self.df[0].str.startswith('8. DURATION OF THE CONTRACT')].index.values[0]
                end_index = self.df[self.df[0].str.startswith("9. SIGNATURE")].index.values[0]

                validity_df = self.df.loc[start_index: end_index - 1, :]
                validity_df = validity_df.applymap(lambda x: nan if x == '' else x)
                validity_df = validity_df.dropna(axis=1, how='all')
                validity_df = validity_df.reset_index(drop=True)
                validity_df = validity_df.T.reset_index(drop=True).T

                date_start_end = re.search(r"Effective.*(\d\d \w+,\s\d+).*(\d\d \w+,\s\d+)", str(validity_df[0][0]))

                self.validity['all'] = {
                    'start_date': datetime.datetime.strptime(str(date_start_end.group(1)),
                                                             "%d %b, %Y").date().isoformat(),
                    'expiry_date': datetime.datetime.strptime(str(date_start_end.group(2)),
                                                              "%d %b, %Y").date().isoformat()
                }

                region_indexes = validity_df[
                    validity_df[0].str.startswith('[') & validity_df[0].str.endswith(']')].index.tolist()

                for index in region_indexes:
                    self.validity[validity_df[0][index]] = {
                        'start_date': datetime.datetime.strptime(validity_df[1][index + 1],
                                                                 "%d %b, %Y").date().isoformat(),
                        'expiry_date': datetime.datetime.strptime(validity_df[1][index + 1],
                                                                  "%d %b, %Y").date().isoformat()}

        def set_load_type_map(self):
            start_index = self.df[self.df[0] == 'Code'].index.values[0]

            load_df = self.df.loc[start_index + 1:, :]
            load_df = load_df.applymap(lambda x: nan if x == '' else x)
            load_df = load_df.dropna(axis=1, how='all')
            load_df = load_df.reset_index(drop=True)
            load_df = load_df.T.reset_index(drop=True).T
            self.load_type_map = load_df.set_index(0).to_dict()[1]

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
            indexes = df[df[0].str.startswith('[') & df[0].str.contains(']')].index.tolist()
            indexes.append(section_end + 1)
            indexes = zip(indexes, indexes[1:])

            for config in indexes:
                region = df[0][config[0]]
                regional_sections[region] = {'start': config[0], 'end': config[1]}

            return regional_sections

        @classmethod
        def get_validity_and_remarks(cls, block, index_of_notes):

            notes = block[0][index_of_notes]

            grp_service_loop = re.search(r"Service Loop:.(\w\w\w)", notes)
            service_loop = None
            if grp_service_loop:
                service_loop = grp_service_loop.group(1)

            if service_loop is None:
                try:
                    service_loop_cell = block[0][index_of_notes + 1]
                    grp_service_loop = re.search(r"Service Loop:.(\w\w\w)", service_loop_cell)
                    service_loop = None
                    if grp_service_loop:
                        service_loop = grp_service_loop.group(1)
                except:
                    pass
            notes_inclusive = notes.replace('\n', '')
            group_inclusion = re.search("inclusive.of.the(.*\.)", notes_inclusive)
            inclusion = None
            if group_inclusion:
                inclusion = group_inclusion.group(1)
                data = re.findall(r'\([A-Z]{3}\)', inclusion)
                inclusion = ','.join(data)
                inclusion = inclusion.replace("(", "").replace(")", "")

            if inclusion is None:
                inclusion_cell = block[0][index_of_notes + 1]
                group_inclusion = re.search("inclusive.of.the(.*\n?.*\.)", inclusion_cell)
                inclusion = None
                if group_inclusion:
                    inclusion = group_inclusion.group(1)
                    data = re.findall(r'\([A-Z]{3}\)', inclusion)
                    inclusion = ','.join(data)
                    inclusion = inclusion.replace("(", "").replace(")", "")

            group_subject = re.search("subject.to.(.*\.)", notes_inclusive)
            subject = None
            if group_subject:
                subject = group_subject.group(1)
                data = re.findall(r'\([A-Z]{3}\)', subject)
                subject = ','.join(data)
                subject = subject.replace("(", "").replace(")", "")

            if subject is None:
                inclusion_cell = block[0][index_of_notes + 1]
                group_subject = re.search("subject.to.(.*\.)", inclusion_cell)
                subject = None
                if group_subject:
                    subject = group_subject.group(1)
                    data = re.findall(r'\([A-Z]{3}\)', subject)
                    subject = ','.join(data)
                    subject = subject.replace("(", "").replace(")", "")

            notes = notes.split('\n')

            if notes[1].startswith("Rates are valid"):
                groups = re.findall("^Rates are valid (from \d+)? ?(to \d+)?\.?", notes[1])
                start_date, expiry_date = groups[0]
                if start_date:
                    start_date = start_date.split()[1]
                    start_date = start_date[:4] + '-' + start_date[4:6] + '-' + start_date[6:]
                if expiry_date:
                    expiry_date = expiry_date.split()[1]
                    expiry_date = expiry_date[:4] + '-' + expiry_date[4:6] + '-' + expiry_date[6:]
                notes = "\n".join(notes[1:])

            else:
                start_date = ''
                expiry_date = ''

            return start_date, expiry_date, notes, service_loop, inclusion, subject

        @classmethod
        def fix_commodity_bullent(cls, block, regexPattern):

            grp_commodity = re.search(regexPattern, block)
            commodity = None
            if grp_commodity:
                bullent = grp_commodity.group(1)
                commodity = grp_commodity.group(2)
            return bullent, commodity

        @classmethod
        def fix_commodity_block(cls, block):
            block = block.applymap(lambda x: nan if x == ':' or x == '' else x)
            block = block.dropna(axis=1, how='all')
            block = block.fillna('')
            block = block.T.reset_index(drop=True).T

            if len(block.columns) >= 15:
                block[2] = block[2] + block[3]
                block = block.drop(columns=[3])
                block = block.T.reset_index(drop=True).T

            bullent, commoddity = cls.fix_commodity_bullent(block[0].values[0])
            commodity = commoddity
            customer_name = block[2].values[1] if block[1].values[1].upper() == 'ACTUAL CUSTOMER' else ''
            bulletin = bullent

            if block[0].str.contains('< NOTE FOR COMMODITY >').any():
                try:
                    index_of_notes = block[block[0].str.startswith('< NOTE FOR COMMODITY >')].index.values[1]
                except:
                    index_of_notes = block[block[0].str.startswith('< NOTE FOR COMMODITY >')].index.values[0]

                origin_indexes = block[block[0].str.startswith('ORIGIN')].index.tolist()
                origin_indexes.append(index_of_notes)
            else:
                notes = ''
                origin_indexes = block[block[0].str.contains('ORIGIN')].index.tolist()
                origin_indexes.append(block.index.values[-1] + 1)

            start_date, expiry_date, remarks, service_loop, inclusions, subject = cls.get_validity_and_remarks(block,
                                                                                                               index_of_notes)

            origin_config = zip(origin_indexes, origin_indexes[1:])

            dfs = []
            for config in origin_config:
                origin_block = block.loc[config[0]:config[1] - 1, :]
                if not origin_block.empty:
                    origin_block = origin_block.applymap(lambda x: nan if x == '' else x)
                    origin_block = origin_block.dropna(axis=1, how='all')
                    origin_block = origin_block.fillna('')

                    origin = origin_block.loc[origin_block[0].str.startswith('ORIGIN')].values[0]

                    origin_join = " ".join(origin)
                    origin = origin_join.split(':')[1]

                    if origin_block[0].str.contains('ORIGIN VIA').any():
                        origin_via_join = ''.join(
                            origin_block.loc[origin_block[0].str.contains('ORIGIN VIA')].values[0])
                        origin_via = origin_via_join.split('ORIGIN VIA')[1].split(':')[1].replace('\n', '')
                        origin = origin_via_join.split('ORIGIN VIA')[0].split(':')[1].replace('\n', '')

                        # origin_via = origin_block.loc[origin_block[0].str.startswith('ORIGIN VIA'), 2].values[0]
                    else:
                        origin_via = ''

                    # origin, origin_via = cls.fix_origin(origin, origin_via)
                    index_of_destination = origin_block[origin_block[0] == 'Destination'].index.values[0]
                    df = origin_block.loc[index_of_destination + 1:, :]

                    if len(df.columns) == 13:
                        df.columns = ['destination_icd', 'destination_country', 'drop1', 'drop2',
                                      'service_type', 'type', 'currency', '20GP', '40GP',
                                      '40HC', '45HC', 'direct', 'note']
                        df = df.drop(columns=['drop1', 'drop2'])

                        df['destination_port'] = ''
                    elif len(df.columns) == 15:
                        df.columns = ['destination_icd', 'drop1', 'drop2', 'destination_country',
                                      'destination_port', 'drop3', 'service_type', 'type', 'currency', '20GP', '40GP',
                                      '40HC', '45HC', 'direct', 'note']
                        df = df.drop(columns=['drop1', 'drop2', 'drop3'])
                    else:
                        raise Exception("Input file too different from reference template")

                    df['destination_icd'] = df['destination_icd'].apply(
                        lambda x: nan if x == 'BLANK' or x == 'NOTE 1 :' or x == 'Destination' else x)
                    df = df.dropna(subset=['destination_icd'])
                    df = df.reset_index(drop=True)
                    df['origin_icd'] = origin
                    df['origin_port'] = origin_via
                    dfs.append(df)

            df = concat(dfs, ignore_index=True, sort=False)
            df['commodity'] = commodity
            df['customer_name'] = customer_name
            df['start_date'] = start_date
            df['expiry_date'] = expiry_date
            df['bulletin'] = bulletin
            df['loop'] = service_loop
            df['inclusions'] = inclusions
            df['subject_to'] = subject

            return df

        def _6_1(self, df, config):

            if config['end'] - config['start'] == 1:
                log.info(f"Section starting from {config['start']} has no data")
                return None

            sectional_df = df[config['start']:config['end']]
            sectional_df = self.remove_empty_columns(sectional_df)
            regional_sections = self.get_regional_sections(sectional_df, sectional_df.shape[0] - 1)

            dfs = []
            for region, regional_config in regional_sections.items():
                region = region[1:-1]
                regional_df = sectional_df.loc[regional_config['start']:regional_config['end'], :]
                regional_df = regional_df.T.reset_index(drop=True).T

                indexes = regional_df[regional_df[0].str.contains('\d+\).*COMMODITY')].index.tolist()
                indexes.append(regional_config['end'])
                indexes = zip(indexes, indexes[1:])

                for commodity_config in indexes:
                    commodity_df = self.fix_commodity_block(
                        regional_df.loc[commodity_config[0]: commodity_config[1], :])
                    if not commodity_df.empty:
                        commodity_df['region'] = region.split(']')[0]
                        if self.validity:
                            if region in self.validity:
                                start_date, expiry_date = list(self.validity[region].values())
                            else:
                                start_date, expiry_date = list(self.validity['all'].values())
                        else:
                            start_date, expiry_date = '', ''
                        commodity_df.loc[commodity_df['start_date'] == '', 'start_date'] = start_date
                        commodity_df.loc[commodity_df['expiry_date'] == '', 'expiry_date'] = expiry_date
                        dfs.append(commodity_df)

            df = concat(dfs, ignore_index=True, sort=False)
            df['charges'] = 'Basic Ocean Freight'

            return df

        def _6_2(self, df, config):
            return self._6_1(df, config)

        @classmethod
        def get_notes_map(cls, notes):
            notes_map = {}
            for i, rows in notes[0].str.split('\n').iteritems():
                for row in rows:
                    if row.startswith("NOTE"):
                        notes_map[row.split(':')[0].replace("NOTE ", "").replace(" ", "")] = row.split(':')[1]

            return notes_map

        @classmethod
        def get_arb_validity(cls, remark):

            if not isinstance(remark, str):
                return '', ''

            validity_re = re.compile(".*Valid (.*) to (.*)")
            if not validity_re.match(remark):
                return '', ''
            else:
                return validity_re.findall(remark)[0]

        @classmethod
        def fix_over_block(cls, block, point):
            block = block.applymap(lambda x: nan if x == '' else x)
            block = block.dropna(axis=1, how='all')
            block = block.fillna('')
            over = block[0].values[0].split(':')[1]
            block = block[2:]

            index_of_notes = block[block[0].str.startswith("NOTE")].index.tolist()
            if index_of_notes:
                notes = block.loc[index_of_notes]
                notes = cls.get_notes_map(notes)
                block = block.loc[:index_of_notes[0] - 1]

                if len(block.columns) == 18:
                    block.columns = [f'{point}_icd', f'{point}_country', 'service_type', 'via',
                                     'drop1', 'drop3', 'drop4', 'mode_of_transportation', 'type', 'currency', 'drop`12',
                                     '20GP', '40GP', '40HC', '45HC',
                                     'drop7', 'drop13', 'remarks']

                else:
                    block.iloc[:, 8] = block.iloc[:, 8].astype(str) + block.iloc[:, 9].astype(str)
                    block.drop(block.columns[9], axis=1, inplace=True)

                    block.columns = [f'{point}_icd', f'{point}_country', 'service_type', 'via',
                                     'drop1', 'drop3', 'drop4', 'mode_of_transportation', 'type', 'currency', 'drop`12',
                                     '20GP', '40GP', '40HC', '45HC',
                                     'drop7', 'drop13', 'remarks']

                block['remarks'] = block['remarks'].astype(str).map(notes)
                block['start_date'] = block['remarks'].apply(cls.get_arb_validity)
                block['expiry_date'] = block['start_date'].str[1]
                block['start_date'] = block['start_date'].str[0]
            else:
                block.columns = [f'{point}_icd', f'{point}_country', 'service_type', 'via',
                                 'drop2', 'drop3', 'drop4', 'drop5', 'currency', 'drop6', '20GP', '40GP', '40HC',
                                 '45HC',
                                 'drop7', 'drop8', 'drop9']

            block = block.drop(columns=[column for column in block.columns if column.startswith('drop')])
            block[f'{point}_port'] = over
            return block

        @classmethod
        def arbitary_fix(cls, df, config, point):
            sectional_df = df[config['start']:config['end']]
            sectional_df = cls.remove_empty_columns(sectional_df)
            regional_sections = cls.get_regional_sections(sectional_df, sectional_df.shape[0] - 1)

            dfs = []
            for region, regional_config in regional_sections.items():
                region = region[1:-1]
                regional_df = sectional_df.loc[regional_config['start']:regional_config['end'] - 1, :]
                regional_df = regional_df.T.reset_index(drop=True).T
                indexes = regional_df[regional_df[0].str.contains('RATE APPLICABLE OVER  :')].index.tolist()
                indexes.append(regional_config['end'])
                indexes = zip(indexes, indexes[1:])

                for over_config in indexes:
                    over_df = cls.fix_over_block(regional_df.loc[over_config[0]: over_config[1] - 1, :], point)
                    over_df['region'] = region.split(']')[0]
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
                    df[point + '_icd'] = df[point + '_icd']
                    # df[point + '_icd'] = df[point + '_icd'].apply(lambda x: cls.split(x)[0])

                    change = True
                if point + '_port' in df:
                    df[point + '_port'] = df[point + '_port']
                    # df[point + '_port'] = df[point + '_port'].apply(lambda x: cls.split(x)[0])

                    change = True

                if change:
                    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                    df = df.reset_index(drop=True)

            return df

        def format_output(self, dfs, am_no, inclusions):
            output = {}

            freight_df = dfs[0]
            freight_df = freight_df.loc[freight_df['type'].str.lower() != 'dg']

            if dfs[1] is not None:
                freight_df = concat([freight_df, dfs[1]], ignore_index=True, sort=False)

            freight_df = freight_df.drop(columns=["type", "direct", "note"])

            # freight_df = self.fix_port_names(freight_df)
            freight_df['amendment_no'] = am_no

            """Adding inclusions"""
            for region in inclusions:
                freight_df.loc[(freight_df['region'] == region), 'inclusions'] = inclusions[region][0]

            output['Freight'] = freight_df

            origin_df = None
            if dfs[2] is not None:
                origin_df = dfs[2]
                origin_df = origin_df.loc[origin_df['type'].str.lower() != 'dg']

                origin_df = self.fix_port_names(origin_df)
                origin_df['amendment_no'] = am_no
                origin_df = origin_df.rename(columns={'origin_icd': 'icd', 'origin_port': 'to'})
                origin_df['at'] = 'origin'

            destination_df = None
            # if dfs[3] is not None:
            #     destination_df = dfs[3]
            #     destination_df = self.fix_port_names(destination_df)
            #     destination_df['amendment_no'] = am_no
            #     destination_df = destination_df.rename(columns={'destination_icd': 'icd', 'destination_port': 'to'})
            #     destination_df['at'] = 'destination'

            if origin_df is not None or destination_df is not None:
                arbitrary_df = concat([origin_df, destination_df], ignore_index=True, sort=False)

                output['Arbitrary Charges'] = arbitrary_df

            return output

        def get_amendment_no(self):
            amt_no = re.search(r"AMENDMENT NO.*(\d+?)", self.df[0][0]).group(1)
            return amt_no

        def get_inclusions(self):
            inclusions = defaultdict(list)
            start_index = list(self.df[(self.df[0].str.contains('C. EXCEPTIONS', na=False))].index)
            end_index = list(self.df[(self.df[0].str.contains('GLOSSARY', na=False))].index)
            inclusions_table_df = self.df[start_index[0]:end_index[0]].copy(deep=True)
            inclusions_table_df.reset_index(drop=True, inplace=True)
            inc_start = list(inclusions_table_df[(inclusions_table_df[0].str.startswith('['))].index)
            inc_start.append(inclusions_table_df.tail(1).index.values[0])
            for _index in range(len(inc_start) - 1):
                inclusions_df = inclusions_table_df[inc_start[_index]:inc_start[_index + 1]].copy(deep=True)
                inclusions_df.reset_index(drop=True, inplace=True)
                start_inc = list(
                    inclusions_df[(inclusions_df[2].str.contains('Rates are inclusive of', na=False))].index)
                inclusions_ch = inclusions_df.iloc[start_inc[0], 2]
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

            self.set_load_type_map()

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
            charge_profile = ["20GP", "40GP", '40HC', "45HC"]
            for column in charge_profile:
                df[column].replace('', nan, inplace=True)
                df[column].fillna(0, inplace=True)

            df = df.melt(
                id_vars=[column for column in df.columns if column not in charge_profile],
                value_vars=charge_profile, value_name='amount', var_name='load_type')
            df['amount'] = df['amount'].astype(str)
            df.loc[df['amount'].str.contains("/"), 'load_type'] = \
                df.loc[df['amount'].str.contains("/"), 'amount'].str.split("/").str[0]

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
            df = df.dropna(subset=['amount'])
            df = df.reset_index(drop=True)

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

            # freight_df.loc[freight_df['origin_icd'] == freight_df['origin_port'], 'origin_port'] = ''
            # freight_df.loc[freight_df['destination_icd'] == freight_df['destination_port'], 'destination_port'] = ''
            freight_df['origin_icd'] = freight_df['origin_icd'].str.replace("\(CY\)", "", regex=True)
            freight_df['origin_icd'] = freight_df['origin_icd'].str.strip()
            freight_df['origin_port'] = freight_df['origin_port'].str.strip()

            freight_df = self.map_load_type(freight_df)

            self.cleaned_output = {'Freight': freight_df}

            if 'Arbitrary Charges' in self.captured_output:
                arbitrary_df = self.captured_output['Arbitrary Charges']
                # arbitrary_df.drop(columns=['origin_country', 'destination_country'], inplace=True)
                arbitrary_df.drop(columns=['origin_country'], inplace=True)
                arbitrary_df['to'] = arbitrary_df['to'].str.rstrip(" Rate")
                arbitrary_df['icd'] = arbitrary_df['icd'].str.replace("\n", ";")
                arbitrary_df = self.map_load_type(arbitrary_df)
                if 'remarks' in arbitrary_df:
                    arbitrary_df.drop(columns=['remarks'], inplace=True)

                self.cleaned_output['Arbitrary Charges'] = arbitrary_df


class Flexport_ONEY_TransAtlantic_PDF_v1(ONEY_TransAtlantic_PDF_v1):
    class _TransAtlantic(ONEY_TransAtlantic_PDF_v1._TransAtlantic):

        def get_sections(self):
            return super().get_sections(['6-4', '6-5', '7. LIQUIDATED'])

        @classmethod
        def fix_commodity_bullent(cls, block):

            return ONEY_TransAtlantic_PDF_v1._TransAtlantic.fix_commodity_bullent(block, r"(\d+\))\s+COMMODITY\s+:\s+(\w+\s?\w+\s?\w+)")


class Flexport_ONEY_TransAtlantic_PDF_AMD_v1(BaseTemplate):
    class _TransAtlantic(BaseFix):

        def __init__(self, df, file_model: dict, headers_config: dict):
            super().__init__(df, file_model, headers_config)

            self.validity = {}
            self.load_type_map = {}

        def check_input(self):

            check_errors = []
            if not self.df[0].str.contains('6-1').any():
                check_errors.append("Section definition '6-1' should be present in first Column")

            if check_errors:
                raise InputValidationError(check_errors)

        def check_output(self):
            pass

        def get_sections(self):
            sections_to_check = ['6-1', '7. LIQUIDATED']
            sections = {}

            previous_section = None
            for check in sections_to_check:
                if self.df[self.df[0].str.startswith(check)].index.values:
                    index = self.df[self.df[0].str.startswith(check)].index.values[0]
                    sections[check] = {'start': index, 'end': None}

                    if previous_section:
                        sections[previous_section]['end'] = index

                    previous_section = check
                else:
                    sections[check] = None

            for drop in ['6-4', '6-5', '7. LIQUIDATED']:
                if drop in sections:
                    sections.pop(drop)

            return sections

        def set_validity_from_section_8(self):

            if self.df[self.df[0].str.startswith('8. DURATION OF THE CONTRACT')].index.values:
                start_index = self.df[self.df[0].str.startswith('8. DURATION OF THE CONTRACT')].index.values[0]
                end_index = self.df[self.df[0].str.startswith("9. SIGNATURE")].index.values[0]

                validity_df = self.df.loc[start_index: end_index - 1, :]
                validity_df = validity_df.applymap(lambda x: nan if x == '' else x)
                validity_df = validity_df.dropna(axis=1, how='all')
                validity_df = validity_df.reset_index(drop=True)
                validity_df = validity_df.T.reset_index(drop=True).T

                date_start_end = re.search(r"Effective.*(\d\d \w+,\s\d+).*(\d\d \w+,\s\d+)", str(validity_df[0][0]))

                self.validity['all'] = {
                    'start_date': datetime.datetime.strptime(str(date_start_end.group(1)),
                                                             "%d %b, %Y").date().isoformat(),
                    'expiry_date': datetime.datetime.strptime(str(date_start_end.group(2)),
                                                              "%d %b, %Y").date().isoformat()
                }

                region_indexes = validity_df[
                    validity_df[0].str.startswith('[') & validity_df[0].str.endswith(']')].index.tolist()

                for index in region_indexes:
                    self.validity[validity_df[0][index]] = {
                        'start_date': datetime.datetime.strptime(validity_df[1][index + 1],
                                                                 "%d %b, %Y").date().isoformat(),
                        'expiry_date': datetime.datetime.strptime(validity_df[1][index + 1],
                                                                  "%d %b, %Y").date().isoformat()}

        def set_load_type_map(self):
            start_index = self.df[self.df[0] == 'Code'].index.values[0]

            load_df = self.df.loc[start_index + 1:, :]
            load_df = load_df.applymap(lambda x: nan if x == '' else x)
            load_df = load_df.dropna(axis=1, how='all')
            load_df = load_df.reset_index(drop=True)
            load_df = load_df.T.reset_index(drop=True).T
            self.load_type_map = load_df.set_index(0).to_dict()[1]

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
            indexes = df[df[0].str.startswith('[') & df[0].str.contains(']')].index.tolist()
            indexes.append(section_end + 1)
            indexes = zip(indexes, indexes[1:])

            for config in indexes:
                region = df[0][config[0]]
                regional_sections[region] = {'start': config[0], 'end': config[1]}

            return regional_sections

        @classmethod
        def get_validity_and_remarks(cls, block, index_of_notes):

            notes = block[0][index_of_notes]

            grp_service_loop = re.search(r"Service Loop:.(\w\w\w)", notes)
            service_loop = None
            if grp_service_loop:
                service_loop = grp_service_loop.group(1)

            if service_loop is None:
                try:
                    service_loop_cell = block[0][index_of_notes + 1]
                    grp_service_loop = re.search(r"Service Loop:.(\w\w\w)", service_loop_cell)
                    service_loop = None
                    if grp_service_loop:
                        service_loop = grp_service_loop.group(1)
                except:
                    pass

            group_inclusion = re.search("inclusive.of.the(.*\.)", notes)
            inclusion = None
            if group_inclusion:
                inclusion = group_inclusion.group(1)
                data = re.findall(r'\([A-Z]{3}\)', inclusion)
                inclusion = ','.join(data)
                inclusion = inclusion.replace("(", "").replace(")", "")

            if inclusion is None:
                inclusion_cell = block[0][index_of_notes + 1]
                group_inclusion = re.search("inclusive.of.the(.*\n?.*\.)", inclusion_cell)
                inclusion = None
                if group_inclusion:
                    inclusion = group_inclusion.group(1)
                    data = re.findall(r'\([A-Z]{3}\)', inclusion)
                    inclusion = ','.join(data)
                    inclusion = inclusion.replace("(", "").replace(")", "")

            group_subject = re.search("subject.to.(.*\.)", notes)
            subject = None
            if group_subject:
                subject = group_subject.group(1)
                data = re.findall(r'\([A-Z]{3}\)', subject)
                subject = ','.join(data)
                subject = subject.replace("(", "").replace(")", "")

            if subject is None:
                inclusion_cell = block[0][index_of_notes + 1]
                group_subject = re.search("subject.to.(.*\.)", inclusion_cell)
                subject = None
                if group_subject:
                    subject = group_subject.group(1)
                    data = re.findall(r'\([A-Z]{3}\)', subject)
                    subject = ','.join(data)
                    subject = subject.replace("(", "").replace(")", "")

            notes = notes.split('\n')

            if notes[1].startswith("Rates are valid"):
                groups = re.findall("^Rates are valid (from \d+)? ?(to \d+)?\.?", notes[1])
                start_date, expiry_date = groups[0]
                if start_date:
                    start_date = start_date.split()[1]
                    start_date = start_date[:4] + '-' + start_date[4:6] + '-' + start_date[6:]
                if expiry_date:
                    expiry_date = expiry_date.split()[1]
                    expiry_date = expiry_date[:4] + '-' + expiry_date[4:6] + '-' + expiry_date[6:]
                notes = "\n".join(notes[1:])

            else:
                start_date = ''
                expiry_date = ''

            return start_date, expiry_date, notes, service_loop, inclusion, subject

        @classmethod
        def fix_commodity_bullent(cls, block):

            grp_commodity = re.search(r"(\d+\))\s+COMMODITY\s+:\s+(\w+\s?\w+\s?\w+)", block)
            commodity = None
            if grp_commodity:
                bullent = grp_commodity.group(1)
                commodity = grp_commodity.group(2)
            return bullent, commodity

        @classmethod
        def fix_commodity_block(cls, block):
            block = block.replace('\xa0', ' ', regex=True)
            block = block.applymap(lambda x: nan if x == ':' or x == '' else x)
            block = block.dropna(axis=1, how='all')
            block = block.fillna('')
            block = block.T.reset_index(drop=True).T

            if len(block.columns) >= 15:
                block[2] = block[2] + block[3]
                block = block.drop(columns=[3])
                block = block.T.reset_index(drop=True).T

            bullent, commoddity = cls.fix_commodity_bullent(block[0].values[0])
            commodity = commoddity
            customer_name = block[2].values[1] if block[1].values[1].upper() == 'ACTUAL CUSTOMER' else ''
            bulletin = bullent
            if block[0].str.contains('< NOTE FOR COMMODITY >').any():
                try:
                    index_of_notes = block[block[0].str.startswith('< NOTE FOR COMMODITY >')].index.values[1]
                except:
                    index_of_notes = block[block[0].str.startswith('< NOTE FOR COMMODITY >')].index.values[0]

                origin_indexes = block[block[0].str.contains('ORIGIN')].index.tolist()
                origin_indexes.append(index_of_notes)
            else:
                notes = ''
                origin_indexes = block[block[0].str.contains('ORIGIN')].index.tolist()
                origin_indexes.append(block.index.values[-1] + 1)

            start_date, expiry_date, remarks, service_loop, inclusions, subject = cls.get_validity_and_remarks(block,
                                                                                                               index_of_notes)

            origin_config = zip(origin_indexes, origin_indexes[1:])

            dfs = []
            for config in origin_config:
                origin_block = block.loc[config[0]:config[1] - 1, :]
                if not origin_block.empty:
                    origin_block = origin_block.applymap(lambda x: nan if x == '' else x)
                    origin_block = origin_block.dropna(axis=1, how='all')
                    origin_block = origin_block.fillna('')

                    origin = origin_block.loc[origin_block[0].str.contains('ORIGIN')].values[0]

                    origin_join = " ".join(origin)
                    origin_capture = re.search("ORIGIN.*:(.*\w+\w+\)).*(ORIGIN.*VIA(.*))?", origin_join)

                    if origin_capture:
                        origin = origin_capture.group(1)
                        origin_via = origin_capture.group(2)


class ONE_Excel_TPEB(BaseTemplate):
    class _TransAtlantic(BaseFix):

        def __init__(self, df, file_model: dict, headers_config: dict):
            super().__init__(df, file_model, headers_config)

            self.validity = {}
            self.load_type_map = {}

        def check_input(self):
            check_errors = []
            if not self.df[0].str.contains('6-1').any():
                check_errors.append("Section definition '6-1' should be present in first Column")

            if not (self.df[0].str.startswith('[') & self.df[0].str.endswith(']')).any():
                check_errors.append("Region definition should be present in first column and should be enclosed in []")

            if not self.df[0].str.contains('Code').any():
                check_errors.append("Load Type mapping table should be defined along first column under section "
                                    "header 'Code'")

            if check_errors:
                raise InputValidationError(check_errors)

        def check_output(self):
            pass

        def get_sections(self):
            sections_to_check = ['6-1', '6-2', '6-3', '6-4', '6-5', '7. LIQUIDATED']
            sections = {}

            previous_section = None
            for check in sections_to_check:
                if self.df[self.df[0].str.startswith(check, na=False)].index.values:
                    index = self.df[self.df[0].str.startswith(check, na=False)].index.values[0]
                    sections[check] = {'start': index, 'end': None}

                    if previous_section:
                        sections[previous_section]['end'] = index

                    previous_section = check
                else:
                    sections[check] = None

            for drop in ['6-5', '7. LIQUIDATED']:
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
                    validity_df[0].str.startswith('[') & validity_df[0].str.endswith(']')].index.tolist()

                for index in region_indexes:
                    self.validity[validity_df[0][index]] = {
                        'start_date': datetime.datetime.strptime(validity_df[1][index + 1],
                                                                 "%d %b, %Y").date().isoformat(),
                        'expiry_date': datetime.datetime.strptime(validity_df[1][index + 1],
                                                                  "%d %b, %Y").date().isoformat()}

        def set_load_type_map(self):
            start_index = self.df[self.df[0] == 'Code'].index.values[0]

            load_df = self.df.loc[start_index + 1:, :]
            load_df = load_df.applymap(lambda x: nan if x == '' else x)
            load_df = load_df.dropna(axis=1, how='all')
            load_df = load_df.reset_index(drop=True)
            load_df = load_df.T.reset_index(drop=True).T
            self.load_type_map = load_df.set_index(0).to_dict()[1]

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
            indexes = df[df[0].str.startswith('[') & df[0].str.endswith(']')].index.tolist()
            indexes.append(section_end + 1)
            indexes = zip(indexes, indexes[1:])

            for config in indexes:
                region = df[0][config[0]]
                regional_sections[region] = {'start': config[0], 'end': config[1]}

            return regional_sections

        @classmethod
        def get_validity_and_remarks(cls, notes):
            notes = notes.split('\n')
            date_flag = 0
            note_included = []
            note_not_included = []
            service = []
            start_date = ''
            expiry_date = ''
            for note in notes:
                if note.startswith("Rates are valid"):
                    if re.search("^Rates are valid (from \d+)? ?(to \d+)?\.?$", note):
                        groups = re.findall("^Rates are valid (from \d+)? ?(to \d+)?\.?$", note)
                        start_date, expiry_date = groups[0]
                        if start_date:
                            date_flag = 1
                            start_date = start_date.split()[1]
                            start_date = start_date[:4] + '-' + start_date[4:6] + '-' + start_date[6:]
                        if expiry_date:
                            date_flag = 1
                            expiry_date = expiry_date.split()[1]
                            expiry_date = expiry_date[:4] + '-' + expiry_date[4:6] + '-' + expiry_date[6:]
                elif note.startswith("Rates are inclusive"):
                    note_included = []
                    regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                    matches_inc_br = re.finditer(regex_incl, note, re.MULTILINE)
                    for matchNum_not_inc, match_not_inc in enumerate(matches_inc_br, start=1):
                        for groupNum_not_inc in range(0, len(match_not_inc.groups())):
                            groupNum_not_inc = groupNum_not_inc + 1
                            note_included.append(
                                match_not_inc.group(groupNum_not_inc))

                elif note.startswith("Rates are subject"):
                    note_not_included = []
                    regex_incl = r"\(([a-zA-Z]{2,3}?)\)"
                    matches_inc_br = re.finditer(regex_incl, note, re.MULTILINE)
                    for matchNum_not_inc, match_not_inc in enumerate(matches_inc_br, start=1):
                        for groupNum_not_inc in range(0, len(match_not_inc.groups())):
                            groupNum_not_inc = groupNum_not_inc + 1
                            note_not_included.append(
                                match_not_inc.group(groupNum_not_inc))

                elif note.startswith("Valid"):
                    note_not_included = []
                    regex_incl = r"Valid (.+?)\sto\s(.+?)$"
                    matches_valid = re.findall(regex_incl, note, re.MULTILINE)
                    start_date, expiry_date = matches_valid[0]

                elif note.startswith("For Service Loop:"):
                    regex_incl = r"For Service Loop: (.+?)$"
                    matches_serivce = re.findall(regex_incl, note, re.MULTILINE)
                    service.append(matches_serivce[0])

                elif date_flag == 0:
                    expiry_date = ''
                    start_date = ''

                # elif re.search("^Rates are valid from", notes[0]):
                #     groups = re.findall("^Rates are valid from (\d+)$", notes[0])
                #     start_date = groups[0]
                #     if start_date:
                #         start_date = start_date[:4] + '-' + start_date[4:6] + '-' + start_date[6:]
                #     expiry_date = ''
                #
                # elif re.search("^Rates are valid to ", notes[0]):
                #     groups = re.findall("^Rates are valid to (\d+)$", notes[0])
                #     expiry_date = groups[0]
                #     if expiry_date:
                #         expiry_date = expiry_date[:4] + '-' + expiry_date[4:6] + '-' + expiry_date[6:]
                #     start_date = ''

            notes = "\n".join(notes[1:])

            return start_date, expiry_date, notes, note_included, note_not_included, service

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

            """Customer name hard coded temp"""

            master_date = ['AEROGROW INTERNATIONAL INC',
                           'ALL BIRDS',
                           'AMERICAN METALCRAFT, INC.',
                           'ANATOMY SUPPLY PARTNERS (ASP GLOBAL) INC',
                           'APP GROUP INC',
                           'APPLE',
                           'ARCHITECTURAL MAILBOXES LLC',
                           'ARDEN',
                           'ARROWHEAD ELECTRICAL PRODUCTS INC',
                           'ARVCO CONTAINER',
                           'ASHLEY FURNITURE',
                           'B & W FIBER GLASS INC',
                           'BADCOCK',
                           'BALSAM BRANDS',
                           'BEST CHOICE PRODUCTS',
                           'BLU DOT DESIGN MANUFACTURING INC',
                           'BLUE SKY, THE COLOR OF IMAGINATION',
                           'BOB MILLS',
                           'BOLL & BRANCH',
                           'BOWERS & WILKINS (UK) GROUP LIMITED',
                           'BOYD SLEEP',
                           'BRONDELL, INC.',
                           'BROOKLYN LOLLIPOPS IMPORT CORP.',
                           'BURROW',
                           'CASPER SLEEP',
                           'CHIC HOME',
                           'CISCO MERAKI',
                           'CITY FURNITURE, INC.',
                           'CLEVA NORTH AMERICA',
                           'COMFORT PRODUCTS INC',
                           'CORKCICLE, LLC',
                           'CORONA CLIPPER',
                           'COSMIC PET (HYPER PET)',
                           'COSTCO',
                           'DANSONS',
                           'DEI SALES, INC',
                           'DESIGNER BRANDS',
                           'DHI CORP',
                           'DIONO LLC',
                           'DIRECTBUY HOME IMPROVEMENT INC',
                           'DMA',
                           'DOVE TAIL FURNITURE AND DESIGNS',
                           'ECHO ENGINEERING & PRODUCTION SUPPLIES INC.',
                           'ELLISON EDUCATIONAL',
                           'ELO TOUCH',
                           'ENGLEWOOD MARKETING LLC',
                           'FANACO FASTENER',
                           'FC BRANDS LLC',
                           'FIRST SOLAR',
                           'FITNESSCUBED',
                           'FLEET PRIDE',
                           'FLOOR AND DECOR - GU STRATEGIS FLOOR AND DECOR',
                           'FLORA CLASSIQUE',
                           'FOREST PRODUCTS DISTRIBUTORS, INC.',
                           'FORME LIFE',
                           'FORMOSA PLASTICS CORP. AMERICA',
                           'GATOR CASES INCORPORATED',
                           'GERBER PLUMBING FIXTURES LLC',
                           'GIMME HEALTH',
                           'GLOBAL FURNITURE USA',
                           'GLOBAL-PAK',
                           'GOLIATH',
                           'GRAND & BENEDICTS INC',
                           'GROVE COLLABORATIVE',
                           'GSM OUTDOORS',
                           'GUARDIAN BIKE COMPANY',
                           'KRAFT HEINZ',
                           'HAMILTON BEACH',
                           'HAPPIEST BABY INC.',
                           'HARLAND M BRAUN',
                           'HARTLAND CONTROLS',
                           'HARWARD MEDIA',
                           'HKC-US LLC',
                           'HOME FASHIONS DISTRIBUTOR',
                           'HOMEWERKS WORLDWIDE LLC',
                           "HUDSON'S BAY CO",
                           'HUNTER FAN COMPANY',
                           'IGLOO PRODUCTS CORP.',
                           'IMPRESSIONS VANITY COMPANY',
                           'INTERIOR DEFINE INC',
                           'ITW GLOBAL BRANDS',
                           'ITW BRANDS DIVISION ITW RESIDENTIAL AND RENOVATION',
                           'JAZWARES LLC',
                           'JR286',
                           'KEENEY MANUFACTURING',
                           'KENAS HOME',
                           'KID KRAFT',
                           'KINGSLEY-BATE LTD',
                           "KIRKLAND'S HOME STORES",
                           'KOLCRAFT ENTERPRISES INC',
                           'LANDING',
                           'LA-Z-BOY INC',
                           'LEATHER ITALIA USA',
                           'LOVEVERY INC',
                           'LULULEMON',
                           'MAGGY LONDON INTERNATIONAL LTD',
                           'MANNINGTON MILLS',
                           'MARTIN SVENSSON HOME LLC',
                           'M-D BUILDING PRODUCTS, INC',
                           'METHOD PRODUCTS',
                           'MIDEA AMERICA CORP',
                           'MILLION DOLLAR BABY',
                           'MITCHELL GOLD, BOB WILLIAMS',
                           'MODLOFT',
                           'MOOSE TOYS',
                           'NATIONAL PUBLIC SEATING',
                           'NATIONAL SPORTING GOODS',
                           'NEARLY NATURAL',
                           'NEWAGE PRODUCTS INC',
                           'NEW AIR',
                           'NINGBO ANNTO LOGISTICS TECHNOLOGY CO.',
                           'NOVILAND INTERNATIONAL LLC',
                           'NPC GLOBAL',
                           'OATEY',
                           'OLDE THOMPSON LLC',
                           'ON AG',
                           'OSTERMAN & COMPANY INC.',
                           'OUR PLACE',
                           'OUTDOOR CAP COMPANY, INC.',
                           'OUTER INC',
                           'PACKNWOOD',
                           'PAMPERED CHEF',
                           'PEAK ACHIEVEMENT ATHLETICS',
                           'PELOTON',
                           'PGP INTERNATIONAL INC',
                           'PKDC, LLC',
                           'PRIMO INTERNATIONAL',
                           'RADIANS INC.',
                           'RELIABLE OF MILWAUKEE',
                           'RG BARRY',
                           'RGI INC.',
                           'RICHARDS HOMEWARES INC.',
                           'RIVERSIDE FURNITURE',
                           'ROLLER DERBY SKATE CORPORATION',
                           'ROOCHI TRADERS, INC.',
                           'RURAL KING',
                           'SCHLEICH-GMBH',
                           'SERENA AND LILY',
                           'SEVES GROUP PPC INSULATORS',
                           'SF EXPRESS CHINA',
                           'SHENZHEN BING BINGPAPER',
                           'SHENZHEN HOSHINE SUPPLY CHAIN',
                           'SHIMANO INC.',
                           'SIGMA RECYCLING INC',
                           'SJ CREATIONS',
                           'SKULL CANDY',
                           'SKYCORP DISTRIBUTION LLC',
                           'SLUMBERLAND FURNITURE',
                           'SNOW JOE, LLC',
                           'SONOS INC',
                           'SONOS',
                           'SPECIALIZED',
                           'STAFAST PRODUCTS, INC',
                           'SUNBELT MARKETING INVESTMENT CORPORATION',
                           'TASKMASTER COMPONENTS',
                           'TAYLOR MADE GOLF CO LTD',
                           'TEAM INTERNATIONAL GROUP OF AMERICA',
                           'TEMPO',
                           'THE ALLEN COMPANY, INC',
                           'THE BOTTLE CREW',
                           'THE CLOROX INT. COMPANY',
                           'CLOROX',
                           'THE SCOTTS COMPANY',
                           'THRO',
                           'THULE, INC.',
                           'THUMA INC',
                           'TIDI',
                           'TINGLEY RUBBER',
                           'TITUS GROUP INC',
                           'TOUGHBUILT INDUSTRIES , INC.',
                           'TOY FACTORY LLC',
                           'TOYSMITH',
                           'TRADEMARK GLOBAL, LLC',
                           'TRAEGER PELLET GRILLS LLC',
                           'TRAEGER GRILLS',
                           'TRICON DRY CHEMICALS, LLC',
                           'TRUDEAU CORPORATION',
                           'TRUE BRANDS',
                           'TURN5',
                           'UNIEK INC.',
                           'UNIQUE USA',
                           'VARI',
                           'VINEYARD VINES',
                           'VIVO',
                           'VOLEX',
                           'VOLUME DISTRIBUTORS INC',
                           'VOYETRA TURTLE BEACH INC',
                           'WAC LIGHTING',
                           'WATER PIK, INC.',
                           'WATTS WATER',
                           'WAY INTERGLOBAL NETWORK, LLC',
                           'WHITMOR INC',
                           'WHO GIVES A CRAP',
                           'WHOLESALE WHEEL & TIRE LLC',
                           'YAHEETECH',
                           'YELLOW LUXURY',
                           'ZODIAC POOL SYSTEMS LLC']

            commodity = block[2].values[0]
            if block[1].str.contains('ACTUAL CUSTOMER').any():
                index = block[(block[1].str.contains('ORIGIN'))].index.values[0]
                start_index = block[(block[1].str.contains('ACTUAL CUSTOMER'))].index.values[0]

                if (index - start_index) == 1:
                    customer_name = block[2].values[1]
                    if customer_name not in master_date:
                        customer_name = ''
                else:
                    customer_name = block[2][start_index:index].values.tolist()
                    for name in customer_name:
                        if name not in master_date:
                            customer_name.remove(name)
                    customer_name = ','.join(customer_name)
            else:
                customer_name = ''

            bulletin = block[0].values[0]

            if block[block[0] == '< NOTE FOR COMMODITY >'].index.values:
                index_of_notes = block[block[0] == '< NOTE FOR COMMODITY >'].index.values[0]
                notes = block[0][index_of_notes + 1]
                origin_indexes = block[block[1] == 'ORIGIN'].index.tolist()
                origin_indexes.append(index_of_notes)
            else:
                notes = ''
                origin_indexes = block[block[1] == 'ORIGIN'].index.tolist()
                origin_indexes.append(block.index.values[-1] + 1)

            start_date, expiry_date, remarks, note_included, note_not_included, service = cls.get_validity_and_remarks(
                notes)

            origin_config = zip(origin_indexes, origin_indexes[1:])

            dfs = []
            for config in origin_config:
                origin_block = block.loc[config[0]:config[1] - 1, :]
                origin_block = origin_block.applymap(lambda x: nan if x == '' else x)
                origin_block = origin_block.dropna(axis=1, how='all')
                origin_block = origin_block.fillna('')

                origin = origin_block.loc[origin_block[1] == 'ORIGIN', 2].values[0]
                if origin_block.loc[origin_block[1] == 'ORIGIN VIA', 2].values:
                    origin_via = origin_block.loc[origin_block[1] == 'ORIGIN VIA', 2].values[0]
                else:
                    origin_via = ''

                # origin, origin_via = cls.fix_origin(origin, origin_via)
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
                        x: nan if x == 'BLANK' or x == 'NOTE 1 :' or x == 'Destination' or x == 'DO NOT USE - PHUOC LONG' else x)
                df = df.dropna(subset=['destination_icd'])
                df = df.reset_index(drop=True)
                df['origin_icd'] = origin
                df['origin_port'] = origin_via
                dfs.append(df)

            df = concat(dfs, ignore_index=True, sort=False)
            df['commodity'] = commodity
            df['customer_name'] = customer_name
            df['start_date'] = start_date
            df['expiry_date'] = expiry_date
            df['bulletin'] = bulletin
            if note_included:
                df['inclusions'] = ','.join(note_included)
            if note_not_included:
                df['subject_to'] = ','.join(note_not_included)
            if service:
                df['loop'] = ','.join(service)

            return df

        def _6_1(self, df, config):

            if config['end'] - config['start'] == 1:
                log.info(f"Section starting from {config['start']} has no data")
                return None

            sectional_df = df[config['start']:config['end']]
            sectional_df = self.remove_empty_columns(sectional_df)
            regional_sections = self.get_regional_sections(sectional_df, sectional_df.shape[0] - 1)

            dfs = []
            for region, regional_config in regional_sections.items():
                region = region[1:-1]
                regional_df = sectional_df.loc[regional_config['start'] + 1:regional_config['end'] - 1, :]
                regional_df = regional_df.T.reset_index(drop=True).T
                indexes = regional_df[regional_df[0].str.match('^\d+\)$')].index.tolist()
                indexes.append(regional_config['end'])
                indexes = zip(indexes, indexes[1:])

                for commodity_config in indexes:
                    commodity_df = self.fix_commodity_block(
                        regional_df.loc[commodity_config[0]: commodity_config[1] - 1, :])
                    commodity_df['region'] = region
                    if self.validity:
                        if region in self.validity:
                            start_date, expiry_date = list(self.validity[region].values())
                        else:
                            start_date, expiry_date = list(self.validity['all'].values())
                    else:
                        start_date, expiry_date = '', ''
                    commodity_df.loc[commodity_df['start_date'] == '', 'start_date'] = start_date
                    commodity_df.loc[commodity_df['expiry_date'] == '', 'expiry_date'] = expiry_date
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

            index_of_notes = block[block[0].str.startswith("NOTE")].index.tolist()
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
                                 'drop2', 'drop3', 'drop4', 'drop5', 'currency', 'drop6', '20GP', '40GP', '40HC',
                                 '45HC',
                                 'drop7', 'drop8', 'drop9']

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
                    indexes = regional_df[regional_df[0] == 'RATE APPLICABLE OVER  :'].index.tolist()
                    indexes.append(regional_config['end'])
                    indexes = zip(indexes, indexes[1:])

                    for over_config in indexes:
                        over_df = cls.fix_over_block(regional_df.loc[over_config[0]: over_config[1] - 1, :], point)
                        over_df['region'] = region
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

            origin_df = None
            if dfs[2] is not None:
                origin_df = dfs[2]
                origin_df = self.fix_port_names(origin_df)
                origin_df['amendment_no'] = am_no
                origin_df = origin_df.rename(columns={'origin_icd': 'icd', 'origin_port': 'to'})
                origin_df['at'] = 'origin'

            destination_df = None
            if dfs[3] is not None:
                destination_df = dfs[3]
                destination_df = self.fix_port_names(destination_df)
                destination_df['amendment_no'] = am_no
                destination_df = destination_df.rename(columns={'destination_icd': 'icd', 'destination_port': 'to'})
                destination_df['at'] = 'destination'

            if origin_df is not None or destination_df is not None:
                arbitrary_df = concat([origin_df, destination_df], ignore_index=True, sort=False)

                output['Arbitrary Charges'] = arbitrary_df

            return output

        def get_amendment_no(self):
            return self.df[0][0].split()[-1]

        def get_inclusions(self):
            inclusions = defaultdict(list)
            start_index = list(self.df[(self.df[0].str.contains('C. EXCEPTIONS', na=False))].index)
            end_index = list(self.df[(self.df[0].str.contains('GLOSSARY', na=False))].index)
            inclusions_table_df = self.df[start_index[0]:end_index[0]].copy(deep=True)
            inclusions_table_df.reset_index(drop=True, inplace=True)
            inc_start = list(inclusions_table_df[(inclusions_table_df[0].str.startswith('['))].index)
            inc_start.append(inclusions_table_df.tail(1).index.values[0])
            for _index in range(len(inc_start) - 1):
                inclusions_df = inclusions_table_df[inc_start[_index]:inc_start[_index + 1]].copy(deep=True)
                inclusions_df.reset_index(drop=True, inplace=True)
                if inclusions_df[2].str.contains('Rates are inclusive of', na=False).any():
                    start_inc = list(
                        inclusions_df[(inclusions_df[2].str.contains('Rates are inclusive of', na=False))].index)
                    inclusions_ch = inclusions_df.iloc[start_inc[0], 2]
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

            self.set_load_type_map()

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
            charge_profile = ["20GP", "40GP", '40HC', "45HC"]

            df = df.melt(
                id_vars=[column for column in df.columns if column not in charge_profile],
                value_vars=charge_profile, value_name='amount', var_name='load_type')
            df['amount'] = df['amount'].astype(str)
            df.loc[df['amount'].str.contains("/"), 'load_type'] = \
                df.loc[df['amount'].str.contains("/"), 'amount'].str.split("/").str[0]

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
            df = df.dropna(subset=['amount'])
            df = df.reset_index(drop=True)

            return df

        def client_name(self, df):
            """
            This function will take freight df and return the rates based on the date
            Args:
                df: Freight DF

            Returns:

            """
            client_name = {'AEROGROW INTERNATIONAL INC': 'AEROGROW INTERNATIONAL INC',
                           'ALL BIRDS': 'Allbirds',
                           'AMERICAN METALCRAFT, INC.': 'American Metalcraft, Inc.',
                           'ANATOMY SUPPLY PARTNERS (ASP GLOBAL) INC': 'Anatomy Supply Partners, LLC',
                           'APP GROUP INC': 'APP GROUP INC',
                           'APPLE': 'Apple',
                           'ARCHITECTURAL MAILBOXES LLC': 'Architectural Mailboxes LLC',
                           'ARDEN': 'ARDEN COMPANIES',
                           'ARROWHEAD ELECTRICAL PRODUCTS INC': 'Arrowhead Electrical Products Inc -',
                           'ARVCO CONTAINER': 'Arvco Container Corporation',
                           'ASHLEY FURNITURE': 'ASHLEY FURNITURE HOME STORE',
                           'B & W FIBER GLASS INC': 'B & W Fiberglass',
                           'BADCOCK': 'Badcock Home Furniture',
                           'BALSAM BRANDS': 'Balsam Brands Incorporated',
                           'BEST CHOICE PRODUCTS': 'BEST CHOICE PRODUCTS',
                           'BLU DOT DESIGN MANUFACTURING INC': 'Blu Dot Design Manufacturing Inc',
                           'BLUE SKY, THE COLOR OF IMAGINATION': 'Blue Sky The Color of Imagination, LLC',
                           'BOB MILLS': 'Bob Mills Furniture',
                           'BOLL & BRANCH': 'Boll & Branch',
                           'BOWERS & WILKINS (UK) GROUP LIMITED': 'Bowers & Wilkins (UK) Group Limited',
                           'BOYD SLEEP': 'Boyd Flotation, Inc.',
                           'BRONDELL, INC.': 'Brondell, Inc.',
                           'BROOKLYN LOLLIPOPS IMPORT CORP.': 'Brooklyn Lollipops Import',
                           'BURROW': 'Burrow',
                           'CASPER SLEEP': 'Casper Sleep',
                           'CHIC HOME': 'CHIC HOME',
                           'CISCO MERAKI': 'Cisco Meraki',
                           'CITY FURNITURE, INC.': 'City Furniture, Inc.',
                           'CLEVA NORTH AMERICA': 'Cleva North America',
                           'COMFORT PRODUCTS INC': 'Comfort Products, Inc.',
                           'CORKCICLE, LLC': 'CORKCICLE, LLC',
                           'CORONA CLIPPER': 'Corona Clipper Inc',
                           'COSMIC PET (HYPER PET)': 'Cosmic Pet',
                           'COSTCO': 'Costco',
                           'DANSONS': 'Dansons',
                           'DEI SALES, INC': 'DEI Sales, Inc',
                           'DESIGNER BRANDS': 'Designer Brands',
                           'DHI CORP': 'DHI Corp.',
                           'DIONO LLC': 'Diono',
                           'DIRECTBUY HOME IMPROVEMENT INC': 'DIRECTBUY HOME IMPROVEMENT INC',
                           'DMA': 'DMA',
                           'DOVE TAIL FURNITURE AND DESIGNS': 'Dovetail Furniture and Designs',
                           'ECHO ENGINEERING & PRODUCTION SUPPLIES INC.': 'Echo Engineering & Production Supplies',
                           'ELLISON EDUCATIONAL': 'Ellison Educational Equipment',
                           'ELO TOUCH': 'Elo Touch Solutions, Inc.',
                           'ENGLEWOOD MARKETING LLC': 'Englewood Marketing Group',
                           'FANACO FASTENER': 'Fanaco Fasteners',
                           'FC BRANDS LLC': 'FC Brands LLC',
                           'FIRST SOLAR': 'First Solar',
                           'FITNESSCUBED': 'FitnessCubed',
                           'FLEET PRIDE': 'FleetPride Inc',
                           'FLOOR AND DECOR - GU STRATEGIS FLOOR AND DECOR': 'Floor And Decor -',
                           'FLORA CLASSIQUE': 'Flora Classique',
                           'FOREST PRODUCTS DISTRIBUTORS, INC.': 'Forest Products Distributors',
                           'FORME LIFE': 'FormeLife',
                           'FORMOSA PLASTICS CORP. AMERICA': 'Formosa Plastics',
                           'GATOR CASES INCORPORATED': 'Gator Cases Incorporated',
                           'GERBER PLUMBING FIXTURES LLC': 'Gerber Plumbing Fixtures LLC',
                           'GIMME HEALTH': 'Gimme Health Foods',
                           'GLOBAL FURNITURE USA': 'Global Furniture USA',
                           'GLOBAL-PAK': 'Global-pak',
                           'GOLIATH': 'Goliath',
                           'GRAND & BENEDICTS INC': 'Grand + Benedicts, Inc.',
                           'GROVE COLLABORATIVE': 'Grove Collaborative',
                           'GSM OUTDOORS': 'GSM OUTDOORS',
                           'GUARDIAN BIKE COMPANY': 'Guardian Bike Company',
                           'KRAFT HEINZ': 'H.J. Heinz Holding B.V.',
                           'HAMILTON BEACH': 'Hamilton Beach Brands',
                           'HAPPIEST BABY INC.': 'Happiest Baby Inc.',
                           'HARLAND M BRAUN': 'Harland M. Braun & Co., Inc.',
                           'HARTLAND CONTROLS': 'Hartland Controls LLC',
                           'HARWARD MEDIA': 'Harward Media',
                           'HKC-US LLC': 'HKC-US (Palm Coast Imports) LLC',
                           'HOME FASHIONS DISTRIBUTOR': 'Home Fashions Distributor, Inc.',
                           'HOMEWERKS WORLDWIDE LLC': 'Homewerks Worldwide',
                           "HUDSON'S BAY CO": "Hudson's Bay Co",
                           'HUNTER FAN COMPANY': 'Hunter Fan Company Inc',
                           'IGLOO PRODUCTS CORP.': 'Igloo Products Corp.',
                           'IMPRESSIONS VANITY COMPANY': 'Impressions Vanity Company',
                           'INTERIOR DEFINE INC': 'Interior Define Inc',
                           'ITW GLOBAL BRANDS': 'ITW Global Brands',
                           'ITW BRANDS DIVISION ITW RESIDENTIAL AND RENOVATION': 'ITW Global Brands',
                           'JAZWARES LLC': 'Jazwares, LLC',
                           'JR286': 'JR286',
                           'KEENEY MANUFACTURING': 'KEENEY MANUFACTURING',
                           'KENAS HOME': 'Kenas Home -',
                           'KID KRAFT': 'KidKraft',
                           'KINGSLEY-BATE LTD': 'Kingsley-Bate',
                           "KIRKLAND'S HOME STORES": "Kirkland's Home Stores",
                           'KOLCRAFT ENTERPRISES INC': 'Kolcraft Enterprises Inc',
                           'LANDING': 'Landing',
                           'LA-Z-BOY INC': 'La-Z-Boy Inc.',
                           'LEATHER ITALIA USA': 'Leather Italia USA',
                           'LOVEVERY INC': 'Lovevery',
                           'LULULEMON': 'Lululemon',
                           'MAGGY LONDON INTERNATIONAL LTD': 'Maggy London International , ltd',
                           'MANNINGTON MILLS': 'Mannington Mills Inc.',
                           'MARTIN SVENSSON HOME LLC': 'Martin Svensson Home',
                           'M-D BUILDING PRODUCTS, INC': 'M-D BUILDING PRODUCTS, INC',
                           'METHOD PRODUCTS': 'Method Products',
                           'MIDEA AMERICA CORP': 'Midea Group',
                           'MILLION DOLLAR BABY': 'Million Dollar Baby (Bexco Enterprises)',
                           'MITCHELL GOLD, BOB WILLIAMS': 'MITCHELL GOLD + BOB WILLIAMS',
                           'MODLOFT': 'Modloft',
                           'MOOSE TOYS': 'MOOSE FAR EAST LIMITED',
                           'NATIONAL PUBLIC SEATING': 'NATIONAL PUBLIC SEATING',
                           'NATIONAL SPORTING GOODS': 'National Sporting Goods',
                           'NEARLY NATURAL': 'Nearly Natural LLC.',
                           'NEWAGE PRODUCTS INC': 'NewAge Products Inc',
                           'NEW AIR': 'Newair LLC',
                           'NINGBO ANNTO LOGISTICS TECHNOLOGY CO.': 'Ningbo Annto Logistics Technology Co',
                           'NOVILAND INTERNATIONAL LLC': 'Noviland International',
                           'NPC GLOBAL': 'NPC Global',
                           'OATEY': 'Oatey Supply Chain Services Inc.',
                           'OLDE THOMPSON LLC': 'Olde Thompson LLC',
                           'ON AG': 'On AG',
                           'OSTERMAN & COMPANY INC.': 'Osterman & Company',
                           'OUR PLACE': 'Our Place',
                           'OUTDOOR CAP COMPANY, INC.': 'Outdoor Research, LLC',
                           'OUTER INC': 'Outer',
                           'PACKNWOOD': 'Packnwood',
                           'PAMPERED CHEF': 'Pampered Chef',
                           'PEAK ACHIEVEMENT ATHLETICS': 'Peak Achievement Athletics',
                           'PELOTON': 'Peloton Interactive Inc',
                           'PGP INTERNATIONAL INC': 'PGP INTERNATIONAL INC.',
                           'PKDC, LLC': 'PKDC',
                           'PRIMO INTERNATIONAL': 'Primo International',
                           'RADIANS INC.': 'Radians Inc.',
                           'RELIABLE OF MILWAUKEE': 'Reliable of Milwaukee',
                           'RG BARRY': 'RG Barry',
                           'RGI INC.': 'RGI Inc.',
                           'RICHARDS HOMEWARES INC.': 'Richards Homewares',
                           'RIVERSIDE FURNITURE': 'Riverside Furniture',
                           'ROLLER DERBY SKATE CORPORATION': 'Roller Derby Skate Corporation',
                           'ROOCHI TRADERS, INC.': 'Roochi Traders, INC.',
                           'RURAL KING': 'Rural King',
                           'SCHLEICH-GMBH': 'Schleich-GmbH',
                           'SERENA AND LILY': 'Serena and Lily Inc.',
                           'SEVES GROUP PPC INSULATORS': 'Seves Group / PPC Insulators',
                           'SF EXPRESS CHINA': 'SF Express China',
                           'SHENZHEN BING BINGPAPER': 'Shenzhen Bingbing Paper Ltd.',
                           'SHENZHEN HOSHINE SUPPLY CHAIN': 'SHENZHEN HOSHINE SUPPLY CHAIN',
                           'SHIMANO INC.': 'Shimano',
                           'SIGMA RECYCLING INC': 'Sigma Recycling Inc',
                           'SJ CREATIONS': 'Sj Creations Incorporated',
                           'SKULL CANDY': 'Skullcandy',
                           'SKYCORP DISTRIBUTION LLC': 'SkyCorp Distribution LLC',
                           'SLUMBERLAND FURNITURE': 'Slumberland Furniture',
                           'SNOW JOE, LLC': 'Snow Joe, LLC',
                           'SONOS INC': 'Sonos Inc',
                           'SONOS': 'Sonos Inc',
                           'SPECIALIZED': 'Specialized',
                           'STAFAST PRODUCTS, INC': 'Stafast Products, Inc',
                           'SUNBELT MARKETING INVESTMENT CORPORATION': 'Sunbelt Marketing Investment Corporation',
                           'TASKMASTER COMPONENTS': 'Taskmaster Components',
                           'TAYLOR MADE GOLF CO LTD': 'TaylorMade Golf',
                           'TEAM INTERNATIONAL GROUP OF AMERICA': 'Team International Group of America',
                           'TEMPO': 'Tempo',
                           'THE ALLEN COMPANY, INC': 'The Allen Company',
                           'THE BOTTLE CREW': 'The Bottle Crew',
                           'THE CLOROX INT. COMPANY': 'The Clorox International Company',
                           'CLOROX': 'The Clorox International Company',
                           'THE SCOTTS COMPANY': 'THE SCOTTS COMPANY LLC',
                           'THRO': 'Thro',
                           'THULE, INC.': 'Thule, Inc.',
                           'THUMA INC': 'Thuma',
                           'TIDI': 'TIDI Products, LLC',
                           'TINGLEY RUBBER': 'Tingley Rubber Corporation',
                           'TITUS GROUP INC': 'Titus Group Inc.',
                           'TOUGHBUILT INDUSTRIES , INC.': 'TOUGHBUILT INDUSTRIES, INC.',
                           'TOY FACTORY LLC': 'Toy Factory LLC',
                           'TOYSMITH': 'Toysmith',
                           'TRADEMARK GLOBAL, LLC': 'Trademark Global',
                           'TRAEGER PELLET GRILLS LLC': 'Traeger Pellet Grills',
                           'TRAEGER GRILLS': 'Traeger Pellet Grills',
                           'TRICON DRY CHEMICALS, LLC': 'Tricon Dry Chemicals',
                           'TRUDEAU CORPORATION': 'Trudeau Corporation',
                           'TRUE BRANDS': 'True Brands',
                           'TURN5': 'Turn5 -',
                           'UNIEK INC.': 'Uniek Inc. -',
                           'UNIQUE USA': 'Unique USA Inc',
                           'VARI': 'VARIDESK LLC',
                           'VINEYARD VINES': 'Vineyard Vines Inc',
                           'VIVO': 'VIVO',
                           'VOLEX': 'Volex',
                           'VOLUME DISTRIBUTORS INC': 'Volume Distributors',
                           'VOYETRA TURTLE BEACH INC': 'Voyetra Turtle Beach Inc',
                           'WAC LIGHTING': 'WAC Lighting',
                           'WATER PIK, INC.': 'Water Pik, Inc.',
                           'WATTS WATER': 'Watts Water Technologies',
                           'WAY INTERGLOBAL NETWORK, LLC': 'Way Interglobal Network',
                           'WHITMOR INC': 'Whitmor Inc',
                           'WHO GIVES A CRAP': 'Who Gives A Crap',
                           'WHOLESALE WHEEL & TIRE LLC': 'Wholesale Wheel & Tire',
                           'YAHEETECH': 'Yaheetech',
                           'YELLOW LUXURY': 'Yellow Luxury',
                           'ZODIAC POOL SYSTEMS LLC': 'Zodiac Pool Systems LLC'}

            df['customer_name'].replace(client_name, regex=True, inplace=True)

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

            # freight_df.loc[freight_df['origin_icd'] == freight_df['origin_port'], 'origin_port'] = ''
            # freight_df.loc[freight_df['destination_icd'] == freight_df['destination_port'], 'destination_port'] = ''
            freight_df['origin_icd'] = freight_df['origin_icd'].str.rstrip("(CY)")

            freight_df = self.map_load_type(freight_df)

            freight_df = self.client_name(freight_df)

            self.cleaned_output = {'Freight': freight_df}

            if 'Arbitrary Charges' in self.captured_output:
                arbitrary_df = self.captured_output['Arbitrary Charges']

                """No Destination Arb"""
                if 'destination_country' in arbitrary_df:
                    arbitrary_df.drop(columns=['destination_country'], inplace=True)
                elif 'origin_country' in arbitrary_df:
                    arbitrary_df.drop(columns=['origin_country'], inplace=True)
                arbitrary_df['to'] = arbitrary_df['to'].str.rstrip(" Rate")
                arbitrary_df['icd'] = arbitrary_df['icd'].str.replace("\n", ";")
                arbitrary_df = self.map_load_type(arbitrary_df)
                if 'remarks' in arbitrary_df:
                    arbitrary_df.drop(columns=['remarks'], inplace=True)

                self.cleaned_output['Arbitrary Charges'] = arbitrary_df


class EU_US_arbrates(BaseTemplate):
    class EUA_arbs(BaseFix):
        def check_input(self):
            check_errors = []
            if not self.df[0].str.contains('POL', na=False).any():
                check_errors.append("POL should be present in the first Column.")
            if check_errors:
                raise InputValidationError(check_errors)

        def check_output(self):
            pass

        def capture(self, regex_eu=None):

            if self.df[0].str.contains('POL').any():
                arb_index = self.df[(self.df[0].str.contains('POL'))].index.values[0]
                arb_df_eu = self.df.loc[int(arb_index):, :]

                arb_df_dry = arb_df_eu.iloc[0:, :5]
                arb_df_dry = arb_df_dry.replace("", nan, regex=True).dropna(how='all', axis=0)
                arb_df_rf = arb_df_eu.iloc[0:, 5:]

                arb_df_rf = arb_df_rf.replace("", nan, regex=True).dropna(how='all', axis=0)
                arb_df_rf = arb_df_rf.reset_index(drop=True)
                arb_df_dry.columns = arb_df_dry.iloc[0, :]
                arb_df_dry = arb_df_dry.iloc[1:, :]
                if not arb_df_rf.empty:
                    arb_df_rf.columns = arb_df_rf.iloc[0]
                    arb_df_rf = arb_df_rf.iloc[1:].copy()

                arb_list = []
                arb_dry_list = arb_df_dry['T/S Port'].tolist()
                arb_dry_list = pd.DataFrame(arb_dry_list)
                for origin in arb_df_dry['POL']:
                    arb_list.append(origin.split(',')[0].strip())
                arb_list = pd.DataFrame(arb_list)
                arb_df = pd.concat([arb_list, arb_dry_list, ], axis=1, ignore_index=True)
                pol = arb_df_dry['POL'].to_list()

                arb_df_dry.reset_index(drop=True, inplace=True)
                for originicd in range(len(pol)):
                    pol[originicd] = pol[originicd].split(',')[0]
                arb_df_dry['POL'] = pd.Series(pol)
                if not arb_df_rf.empty:
                    arb_final = pd.merge(arb_df_rf, arb_df_dry, on=['POL', 'T/S Port'], how='right')
                else:
                    arb_final = arb_df_dry
                arb_final['at'] = 'origin'
                arb_final['charges'] = "origin arbitrary charges"
                arb_final['charges_leg'] = 'L2'

                if self.df[0].str.contains('Validity:').any():
                    val_final = []
                    val_index = self.df[(self.df[0].str.contains('Validity'))].index.values[0]
                    val_df = self.df.loc[int(val_index):, :]
                    val_df_index = val_df.iloc[val_index, 0]
                regex_eua = r"Validity:\s?(.+)"
                regex_rail = r"Validity:\s(.+?)to \s?(.+)$"

                if re.search(regex_rail, str(val_df_index)) is not None:
                    matches_rail = re.finditer(regex_rail, val_df_index, re.MULTILINE)
                    for matchNum, match in enumerate(matches_rail, start=1):
                        arb_final["start_date"] = datetime.datetime.strptime(match.group(1).strip(' ').split(' ')[0][:1] +
                                                                    match.group(1).strip(' ').split(' ')[1] +
                                                                    '2022', "%d%b%Y").date()
                        date_time = match.group(2).strip(' ').split(' ')
                        arb_final["expiry_date"] = datetime.datetime.strptime(date_time[0][:-2] + date_time[1] +
                                                                     date_time[2], "%d%b%Y").date()

                elif re.search(regex_eua, str(val_df_index)) is not None:
                    matches_eu = re.finditer(regex_eua, val_df_index, re.MULTILINE)
                    for matchNum, match in enumerate(matches_eu, start=1):
                        arb_final["expiry_date"] = match.group(1)

            self.captured_output = {'Freight': arb_final}

        def clean(self):
            df_clean_out = self.captured_output['Freight']
            df_clean_out.rename(
                columns={"POL": "origin_icd", "T/S Port": "destination_icd", "20' Reefer": "20RE",
                         "40' Reefer": "40RE",
                         "T/T": "transit_time", "20'Dry": "20GP", "40'Dry": "40GP"}, inplace=True)
            df_clean_out['40HC'] = df_clean_out["40GP"]

            self.cleaned_output = {'Freight': df_clean_out}

    class US_rail(BaseFix):
        def check_input(self):
            check_errors = []
            if not self.df[0].str.contains('Port', na=False).any():
                check_errors.append("Port should be present in the first Column.")
            if check_errors:
                raise InputValidationError(check_errors)

        def check_output(self):
            pass

        def capture(self):
            arb_df_rail = {}
            if self.df[0].str.contains('Port').any():
                arb_rail_index = self.df[(self.df[0].str.contains('Port'))].index.values[0]
                arb_df_rail = self.df.loc[int(arb_rail_index):, :]
                arb_df_rail = arb_df_rail.reset_index(drop=True)
                arb_df_rail['mode'] = ''
                for elements in range(arb_df_rail.shape[0]):
                    if 'via' in arb_df_rail.iloc[elements, 5]:
                        arb_df_rail['mode'][elements] = arb_df_rail.iloc[elements, 5].split()[-1]
                arb_df_rail.iloc[0, 5] = "mode"
                arb_df_rail.columns = arb_df_rail.iloc[0]
                arb_df_rail = arb_df_rail.iloc[1:, :]

                arb_df_rail['at'] = 'Destination'
                arb_df_rail['charges'] = "Destination arbitrary charges"
                arb_df_rail['charges_leg'] = 'L4'
                if arb_df_rail["D2"].astype(str).str.contains('No 20fts').any():
                    arb_df_rail.loc[arb_df_rail["D2"].str.contains('No 20fts', na=False), "D2"] = ''
                if arb_df_rail["D4/D5"].astype(str).str.contains('No 40fts').any():
                    arb_df_rail.loc[arb_df_rail["D4/D5"].str.contains('No 40fts', na=False), "D4/D5"] = ''
                if self.df[0].str.contains('Validity:').any():
                    val_index = self.df[(self.df[0].str.contains('Validity'))].index.values[0]
                    val_df = self.df.loc[int(val_index):, :]
                    val_df_index = val_df.iloc[val_index, 0]
                    regex_eua = r"Validity:\s?(.+)"
                    regex_rail = r"Validity:\s(.+?)to \s?(.+)$"

                    if re.search(regex_rail, str(val_df_index)) is not None:
                        matches_rail = re.finditer(regex_rail, val_df_index, re.MULTILINE)
                        for matchNum, match in enumerate(matches_rail, start=1):
                            arb_df_rail["start_date"] = datetime.datetime.strptime(match.group(1).strip(' ').split(' ')[0][:1] +
                                                                    match.group(1).strip(' ').split(' ')[1] +
                                                                    '2022', "%d%B%Y").date()
                            date_time = match.group(2).strip(' ').split(' ')
                            arb_df_rail["expiry_date"] = datetime.datetime.strptime(date_time[0][:-2] + date_time[1] +
                                                                     date_time[2], "%d%B%Y").date()
                    elif re.search(regex_eua, str(val_df_index)) is not None:
                        matches_eu = re.finditer(regex_eua, val_df_index, re.MULTILINE)
                        for matchNum, match in enumerate(matches_eu, start=1):
                            arb_df_rail["expiry_date"] = match.group(1)
                self.captured_output = {'Freight': arb_df_rail}
                return self.captured_output

        def clean(self):
            df_clean = self.captured_output['Freight']
            df_clean.rename(
                columns={"Port of Discharge": "origin_icd", "CY Facility": "destination_icd",
                         "T/T": "transit_time", "D2": "20GP", "D4/D5": "40GP", "Remarks": "remarks",
                         "mode": "mode_of_transportation"}, inplace=True)

            df_clean['40HC'] = df_clean["40GP"]

            self.cleaned_output = {'Freight': df_clean}

            return self.cleaned_output


class Expedoc_ONEY_TransAtlantic_PDF_v1(ONEY_TransAtlantic_PDF_v1):
    class _TransAtlantic(ONEY_TransAtlantic_PDF_v1._TransAtlantic):

        def get_contracts_details(self):

            contracts_details = {}

            def get_contract_id_and_date(data_str):
                return re.search(r"CONTRACT NO.\s*(\w+).*\n?Effective Date\s(\d+.\w+,.\d+)", data_str)

            parsed_contract_id = self.df.iloc[:, 0].apply(lambda x: get_contract_id_and_date(str(x)))
            for row in parsed_contract_id:
                if row:
                    contracts_details['contract_id'] = row.group(1)
                    effective_date = row.group(2)

            def get_client_name(data_str):
                return re.search(r"Name of Shipper\s+:\s+(.*)", data_str)

            parsed_client_name = self.df.iloc[:, 0].apply(lambda x: get_client_name(str(x)))
            for row in parsed_client_name:
                if row:
                    contracts_details['client_name'] = row.group(1)

            contracts_details['vendor'] = "ONE"

            contracts_details['start_date'] = ""

            contracts_details['effective_date'] = parse(effective_date)

            return contracts_details

        def get_sections(self):
            return super().get_sections(['6-5', '7. LIQUIDATED'])

        @classmethod
        def fix_commodity_bullent(cls, block):
            return ONEY_TransAtlantic_PDF_v1.TransAtlantic.fix_commodity_bullent(block, r"(\d+\))\s+COMMODITY\s+:\s+(.*)")

        @classmethod
        def fix_commodity_block(cls, block):

            if len(block) > 1:
                block = block.applymap(lambda x: nan if x == ':' or x == '' else x)
                block = block.dropna(axis=1, how='all')
                block = block.fillna('')
                block = block.T.reset_index(drop=True).T

                if len(block.columns) >= 15:
                    block[2] = block[2] + block[3]
                    block = block.drop(columns=[3])
                    block = block.T.reset_index(drop=True).T

                bullent, commoddity = cls.fix_commodity_bullent(block[0].values[0])
                commodity = commoddity
                customer_name = block[2].values[1] if block[1].values[1].upper() == 'ACTUAL CUSTOMER' else ''
                bulletin = bullent

                if block[0].str.contains('< NOTE FOR COMMODITY >').any():
                    try:
                        index_of_notes = block[block[0].str.startswith('< NOTE FOR COMMODITY >')].index.values[1]
                    except:
                        index_of_notes = block[block[0].str.startswith('< NOTE FOR COMMODITY >')].index.values[0]

                    origin_indexes = block[block[0].str.startswith('ORIGIN')].index.tolist()
                    origin_indexes.append(index_of_notes)
                else:
                    notes = ''
                    origin_indexes = block[block[0].str.contains('ORIGIN')].index.tolist()
                    origin_indexes.append(block.index.values[-1] + 1)

                start_date, expiry_date, remarks, service_loop, inclusions, subject = cls.get_validity_and_remarks(block , index_of_notes )

                origin_config = zip(origin_indexes, origin_indexes[1:])


class ONE_KarlCross_V1(BaseTemplate):

    class Agreement(BaseFix):

        def check_input(self):

            pass

        def check_output(self):

            pass

        def get_dataframes(self):

            df = self.df
            origin_indices = list(df[(df[0].str.contains('Origin Country'))].index)
            surcharge_index = list(df[(df[0].str.contains('Surcharge Terms & Special Terms'))].index)[0]
            notes_index = list(df[(df[0].str.contains('Notes'))].index)[0]
            hidden_df_index = list(df[(df[1].str.contains('Date'))].index)[0]

            header_details = {}
            if self.df[0].str.contains('Rates are valid from:').any():
                start_date_index = self.df[(self.df[0].str.contains('Rates are valid from:'))].index.values[0]
                header_details["start_date"] = self.df.loc[int(start_date_index)][3]

            if self.df[0].str.contains('Rates are valid until:').any():
                expiry_date_index = self.df[(self.df[0].str.contains('Rates are valid until:'))].index.values[0]
                header_details["expiry_date"] = self.df.loc[int(expiry_date_index)][3]

            if self.df[8].str.contains('Commodity').any():
                commodity_index = self.df[(self.df[8].str.contains('Commodity'))].index.values[0]
                header_details["commodity"] = self.df.loc[int(commodity_index)][10]

            freight_df_list = []
            for i in range(len(origin_indices)):
                freight_df_list.append(df.loc[origin_indices[i]:origin_indices[i+1]-1, :])
                if i+2 == len(origin_indices):
                    freight_df_list.append(df.loc[origin_indices[i+1]+2:surcharge_index-1, :])
                    break
            if freight_df_list:
                freight_df = pd.concat(freight_df_list)
                freight_df = freight_df.reset_index(drop=True)
                freight_df.iloc[1, 0:11] = freight_df.iloc[0, 0:11]
                freight_df = freight_df.iloc[1:, :]

            surcharge_df = self.df.iloc[surcharge_index:notes_index, :]
            notes_df = self.df.iloc[notes_index:hidden_df_index, 0]
            hidden_df = self.df.iloc[hidden_df_index:, :]

            return header_details, freight_df, surcharge_df, hidden_df, notes_df

        def surcharge_df_splitter(self, surcharge_df):

            surcharge_df = surcharge_df.applymap(lambda x: nan if x == '' else x)
            surcharge_df = surcharge_df.dropna(axis=1, how='all')
            surcharge_df = surcharge_df.fillna('')
            surcharge_df = surcharge_df.reset_index(drop=True)
            surcharge_df.columns = range(surcharge_df.shape[1])
            if surcharge_df[1].str.contains('Rates are Inclusive of:').any():
                inclusions_index = surcharge_df[(surcharge_df[1].str.contains('Rates are Inclusive of:'))].index.values[0]
                inclusions_str = surcharge_df.iloc[int(inclusions_index), 1]
                inclusions = []
                regex = r"Rates are Inclusive of: (.+?)$"
                if re.search(regex, inclusions_str) is not None:
                    matches_inc = re.findall(regex, inclusions_str,re.MULTILINE)
                    regex_incl = r"\(([A-Z]{2,3}?)\)"
                    matches_inc_br = re.finditer(regex_incl, matches_inc[0], re.MULTILINE)
                    for matchNum_2, match_2 in enumerate(matches_inc_br, start=1):
                        for groupNum_2 in range(0, len(match_2.groups())):
                            groupNum_2 = groupNum_2 + 1
                            if match_2.group(groupNum_2) not in inclusions:
                                inclusions.append(match_2.group(groupNum_2))

            if surcharge_df[1].str.contains('Rates are subject to below mentioned surcharges:').any():
                subject_to_index = surcharge_df[(surcharge_df[1].str.contains('Rates are subject to below mentioned surcharges:'))].index.values[0]+2
                subject_to_dict = {}
                subject_to = surcharge_df.iloc[subject_to_index:, 1:4]
                subject_to = subject_to.reset_index(drop=True)
                for i in range(len(subject_to[1])):
                    if subject_to[3][i] == 'inclusive':
                        if subject_to[2][i] not in inclusions:
                            inclusions.append(subject_to[2][i].strip())
                            subject_to.drop([i], inplace=True)

                dfs = []
                for config in origin_config:
                    origin_block = block.loc[config[0]:config[1] - 1, :]
                    if not origin_block.empty:
                        origin_block = origin_block.applymap(lambda x: nan if x == '' else x)
                        origin_block = origin_block.dropna(axis=1, how='all')
                        origin_block = origin_block.fillna('')

                        origin = origin_block.loc[origin_block[0].str.startswith('ORIGIN')].values[0]

                        origin_join = " ".join(origin)
                        origin = origin_join.split(':')[1]


                        check_origin_exists_index = origin_block.loc[origin_block[0].str.startswith('ORIGIN')].index[0]

                        if origin_block.loc[check_origin_exists_index + 1][0] != "Destination":
                            origin +=  origin_block.loc[check_origin_exists_index + 1][0]


                        if origin_block[0].str.contains('ORIGIN VIA').any():
                            origin_via_join = ''.join(origin_block.loc[origin_block[0].str.contains('ORIGIN VIA')].values[0])
                            origin_via = origin_via_join.split('ORIGIN VIA')[1].split(':')[1].replace('\n','')
                            origin = origin_via_join.split('ORIGIN VIA')[0].split(':')[1].replace('\n','')

                            # origin_via = origin_block.loc[origin_block[0].str.startswith('ORIGIN VIA'), 2].values[0]
                        else:
                            origin_via = ''

                        # origin, origin_via = cls.fix_origin(origin, origin_via)
                        index_of_destination = origin_block[origin_block[0] == 'Destination'].index.values[0]
                        df = origin_block.loc[index_of_destination + 1:, :]

                        if len(df.columns) == 13:
                            df.columns = ['destination_icd', 'destination_country', 'destination_port','drop2',
                                           'service_type', 'type', 'currency', '20GP', '40GP',
                                          '40HC', '45HC', 'direct', 'note']
                            df = df.drop(columns=['drop2'])

                        elif len(df.columns) == 15:
                            df.columns = ['destination_icd', 'drop1', 'drop2', 'destination_country',
                                          'destination_port', 'drop3', 'service_type', 'type', 'currency', '20GP', '40GP',
                                          '40HC', '45HC', 'direct', 'note']
                            df = df.drop(columns=['drop1', 'drop2', 'drop3'])
                        else:
                            raise Exception("Input file too different from reference template")

                        df['destination_icd'] = df['destination_icd'].apply(
                            lambda x: nan if x == 'BLANK' or x == 'NOTE 1 :' or x == 'Destination' else x)
                        df = df.dropna(subset=['destination_icd'])
                        df = df.reset_index(drop=True)
                        df['origin_icd'] = origin
                        df['origin_port'] = origin_via
                        dfs.append(df)

                df = concat(dfs, ignore_index=True, sort=False)
                df['commodity'] = commodity
                df['customer_name'] = customer_name
                df['start_date'] = start_date
                df['expiry_date'] = expiry_date
                df['bulletin'] = bulletin
                df['loop'] = service_loop
                df['inclusions'] = inclusions
                df['subject_to'] = subject
                return df
            else:
                return pd.DataFrame()

        def format_output(self, dfs, am_no, inclusions , contracts_details ):
            output = {}

            freight_df = dfs[0]
            freight_df = freight_df.loc[freight_df['type'].str.lower() != 'dg']

            if dfs[1] is not None:
                freight_df = concat([freight_df, dfs[1]], ignore_index=True, sort=False)

            freight_df = freight_df.drop(columns=["type", "direct", "note"])

            #freight_df = self.fix_port_names(freight_df)
            freight_df['amendment_no'] = am_no

            freight_df['contract_id'] = contracts_details['contract_id']
            freight_df['contract_expiry_date'] = contracts_details['effective_date']
            freight_df['contract_start_date'] = contracts_details['start_date']
            freight_df['client_name'] = contracts_details['client_name']
            freight_df['vendor'] = contracts_details['vendor']


            """Adding inclusions"""
            for region in inclusions:
                freight_df.loc[(freight_df['region'].str.replace("\xa0", " ") == region), 'inclusions'] = inclusions[region][0]

            output['Freight'] = freight_df

            origin_df = None
            if dfs[2] is not None:
                origin_df = dfs[2]
                origin_df = origin_df.loc[origin_df['type'].str.lower() != 'dg']

                origin_df = self.fix_port_names(origin_df)
                origin_df['amendment_no'] = am_no
                origin_df = origin_df.rename(columns={'origin_icd': 'icd', 'origin_port': 'to'})
                origin_df['at'] = 'origin'

            destination_df = None
            if dfs[3] is not None:
                destination_df = dfs[3]
                destination_df = self.fix_port_names(destination_df)
                destination_df['amendment_no'] = am_no
                destination_df = destination_df.rename(columns={'destination_icd': 'icd', 'destination_port': 'to'})
                destination_df['at'] = 'destination'

            if origin_df is not None or destination_df is not None:
                arbitrary_df = concat([origin_df, destination_df], ignore_index=True, sort=False)

                output['Arbitrary Charges'] = arbitrary_df

            return output

        def get_amendment_no(self):
            amt_no = re.search("AMENDMENT NO.*(\d+\d+)", self.df[0][0]).group(1)
            return amt_no



        def capture(self):

            contracts_detail = self.get_contracts_details()

            sections = self.get_sections()

            amendment_no = self.get_amendment_no()

            self.set_validity_from_section_8()

            self.set_load_type_map()

            inclusions_dict = self.get_inclusions()

            dfs = []
            for section, config in sections.items():
                if config is not None:
                    section = '_' + section.replace('-', '_')
                    fix = getattr(self, section)
                    dfs.append(fix(self.df, config))
                else:
                    dfs.append(None)

            self.captured_output = self.format_output(dfs, amendment_no, inclusions_dict ,contracts_detail)

        def clean(self):

            freight_df = self.captured_output['Freight']
            freight_df.drop(columns=['destination_country'], inplace=True)
            # for c in product(['origin', 'destination'], ['icd', 'port']):
            #     _column = c[0] + '_' + c[1]
            #     if c[1] == 'icd':
            #         freight_df[_column] = freight_df[_column].str.replace("\n", ";")
            #     freight_df[_column] = freight_df[_column].str.split('\n')
            #     freight_df = freight_df.explode(_column)
            #     freight_df = freight_df.reset_index(drop=True)

            freight_df["destination_icd"] = freight_df["destination_icd"].str.replace(r"(?<=,\s[A-Z]{2})\s" , ";" , regex = True)
            freight_df["destination_port"] = freight_df["destination_port"].str.replace(r"(?<=,\s[A-Z]{2})\s" , ";" , regex = True)

            freight_df['origin_icd'] = freight_df['origin_icd'].str.replace("\(CY\)",";",regex=True)
            freight_df['origin_icd'] = freight_df['origin_icd'].str.strip()
            freight_df['origin_icd'] = freight_df['origin_icd'].str.rstrip(r";")

            freight_df['origin_port'] = freight_df['origin_port'].str.strip()

            freight_df = self.map_load_type(freight_df)

            self.cleaned_output = {'Freight': freight_df}

            if 'Arbitrary Charges' in self.captured_output:
                arbitrary_df = self.captured_output['Arbitrary Charges']
                #arbitrary_df.drop(columns=['origin_country', 'destination_country'], inplace=True)
                arbitrary_df.drop(columns=['origin_country'], inplace=True)
                arbitrary_df['to'] = arbitrary_df['to'].str.rstrip(" Rate")
                arbitrary_df['icd'] = arbitrary_df['icd'].str.replace("\n", ";")
                arbitrary_df = self.map_load_type(arbitrary_df)
                if 'remarks' in arbitrary_df:
                    arbitrary_df.drop(columns=['remarks'], inplace=True)

                self.cleaned_output['Arbitrary Charges'] = arbitrary_df


class Expedoc_ONE_Excel_TPEB(ONE_Excel_TPEB):
    class _TransAtlantic(ONE_Excel_TPEB._TransAtlantic):

        def clean(self):

            freight_df = self.captured_output['Freight']
            freight_df.drop(columns=['destination_country'], inplace=True)
            freight_df["destination_icd"] = freight_df["destination_icd"]\
                .str.replace(r"(?<=,\s[A-Z]{2})\s", ";",regex=True)
            freight_df["destination_port"] = freight_df["destination_port"]\
                .str.replace(r"(?<=,\s[A-Z]{2})\s", ";", regex=True)
            freight_df['origin_icd'] = freight_df['origin_icd'].str.replace("\(CY\)", ";", regex=True)
            freight_df['origin_icd'] = freight_df['origin_icd'].str.strip()
            freight_df['origin_icd'] = freight_df['origin_icd'].str.rstrip(r";")

            freight_df['origin_port'] = freight_df['origin_port'].str.strip()
            freight_df['origin_icd'] = freight_df['origin_icd'].str.rstrip("(CY)")
            freight_df = self.map_load_type(freight_df)
            freight_df = self.client_name(freight_df)
            self.cleaned_output = {'Freight': freight_df}
            if 'Arbitrary Charges' in self.captured_output:
                arbitrary_df = self.captured_output['Arbitrary Charges']

                """No Destination Arb"""
                if 'destination_country' in arbitrary_df:
                    arbitrary_df.drop(columns=['destination_country'], inplace=True)
                elif 'origin_country' in arbitrary_df:
                    arbitrary_df.drop(columns=['origin_country'], inplace=True)
                arbitrary_df['to'] = arbitrary_df['to'].str.rstrip(" Rate")
                arbitrary_df['icd'] = arbitrary_df['icd'].str.replace("\n", ";")
                arbitrary_df = self.map_load_type(arbitrary_df)
                if 'remarks' in arbitrary_df:
                    arbitrary_df.drop(columns=['remarks'], inplace=True)

                self.cleaned_output['Arbitrary Charges'] = arbitrary_df


class ONE_KarlCross_V1(BaseTemplate):

    class Agreement(BaseFix):

        def check_input(self):

            pass

        def check_output(self):

            pass

        def get_dataframes(self):

            df = self.df
            origin_indices = list(df[(df[0].str.contains('Origin Country'))].index)
            surcharge_index = list(df[(df[0].str.contains('Surcharge Terms & Special Terms'))].index)[0]
            notes_index = list(df[(df[0].str.contains('Notes'))].index)[0]
            hidden_df_index = list(df[(df[1].str.contains('Date'))].index)[0]

            header_details = {}
            if self.df[0].str.contains('Rates are valid from:').any():
                start_date_index = self.df[(self.df[0].str.contains('Rates are valid from:'))].index.values[0]
                header_details["start_date"] = self.df.loc[int(start_date_index)][3]

            if self.df[0].str.contains('Rates are valid until:').any():
                expiry_date_index = self.df[(self.df[0].str.contains('Rates are valid until:'))].index.values[0]
                header_details["expiry_date"] = self.df.loc[int(expiry_date_index)][3]

            if self.df[8].str.contains('Commodity').any():
                commodity_index = self.df[(self.df[8].str.contains('Commodity'))].index.values[0]
                header_details["commodity"] = self.df.loc[int(commodity_index)][10]

            if self.df[8].str.contains('ONE Pricing Reference').any():
                contract_id_index = self.df[(self.df[8].str.contains('ONE Pricing Reference'))].index.values[0]
                header_details["contract_id"] = self.df.loc[int(contract_id_index)][10]

            freight_df_list = []
            for i in range(len(origin_indices)):
                freight_df_list.append(df.loc[origin_indices[i]:origin_indices[i+1]-1, :])
                if i+2 == len(origin_indices):
                    freight_df_list.append(df.loc[origin_indices[i+1]+2:surcharge_index-1, :])
                    break
            if freight_df_list:
                freight_df = pd.concat(freight_df_list)
                freight_df = freight_df.reset_index(drop=True)
                freight_df.iloc[1, 0:11] = freight_df.iloc[0, 0:11]
                freight_df = freight_df.iloc[1:, :]

            surcharge_df = self.df.iloc[surcharge_index:notes_index, :]
            notes_df = self.df.iloc[notes_index:hidden_df_index, 0]
            hidden_df = self.df.iloc[hidden_df_index:, :]

            return header_details, freight_df, surcharge_df, hidden_df, notes_df

        def surcharge_df_splitter(self, surcharge_df):

            surcharge_df = surcharge_df.applymap(lambda x: nan if x == '' else x)
            surcharge_df = surcharge_df.dropna(axis=1, how='all')
            surcharge_df = surcharge_df.fillna('')
            surcharge_df = surcharge_df.reset_index(drop=True)
            surcharge_df.columns = range(surcharge_df.shape[1])
            if surcharge_df[1].str.contains('Rates are Inclusive of:').any():
                inclusions_index = surcharge_df[(surcharge_df[1].str.contains('Rates are Inclusive of:'))].index.values[0]
                inclusions_str = surcharge_df.iloc[int(inclusions_index), 1]
                inclusions = []
                regex = r"Rates are Inclusive of: (.+?)$"
                if re.search(regex, inclusions_str) is not None:
                    matches_inc = re.findall(regex, inclusions_str,re.MULTILINE)
                    inclusions.append(matches_inc[0])
                    '''
                    regex_incl = r"\(([A-Z]{2,3}?)\)"
                    matches_inc_br = re.finditer(regex_incl, matches_inc[0], re.MULTILINE)
                    for matchNum_2, match_2 in enumerate(matches_inc_br, start=1):
                        for groupNum_2 in range(0, len(match_2.groups())):
                            groupNum_2 = groupNum_2 + 1
                            if match_2.group(groupNum_2) not in inclusions:
                                inclusions.append(match_2.group(groupNum_2))
                    '''
            if surcharge_df[1].str.contains('Rates are subject to below mentioned surcharges:').any():
                subject_to_index = surcharge_df[(surcharge_df[1].str.contains('Rates are subject to below mentioned surcharges:'))].index.values[0]+2
                subject_to_dict = {}
                subject_to = surcharge_df.iloc[subject_to_index:, 1:4]
                subject_to = subject_to.reset_index(drop=True)
                for i in range(len(subject_to[1])):
                    if subject_to[3][i] == 'inclusive':
                        if subject_to[2][i] not in inclusions:
                            inclusions.append(subject_to[2][i].strip())
                            subject_to.drop([i], inplace=True)

            if surcharge_df[1].str.contains('Rates are also subject to').any():
                subject_to_index_2 = surcharge_df[(surcharge_df[1].str.contains('Rates are also subject to'))].index.values[0]
                subject_to_str = surcharge_df.iloc[int(subject_to_index_2), 1]
                subject_to_dict_1 = {}
                regex = r"Rates are also subject to (.+?), local surcharges at origin/destination and all other surcharges mentioned in the applicable ONE tariff"
                if re.search(regex, subject_to_str) is not None:
                    matches_inc_1 = re.findall(regex, subject_to_str, re.MULTILINE)
                    matches_inc_1 = matches_inc_1[0].split(',')
                    for element in matches_inc_1:
                        element = element.replace('(', ' ').replace(')', ' ')
                        subject_to_dict_1[element.split()[-1]] = ' '.join(element.split()[0:len(element.split())-1])

            subject_to_dict = subject_to.set_index(subject_to[2]).to_dict()[1]
            subject_to_dict = dict((k, v) for k, v in subject_to_dict.items() if k)
            subject_to_dict = {**subject_to_dict, **subject_to_dict_1}

            return inclusions, subject_to_dict

        def capture(self):

            header_details, freight_df, surcharge_df, hidden_df, notes_df = self.get_dataframes()
            inclusions, subject_to = self.surcharge_df_splitter(surcharge_df)
            self.captured_output = {'freight_df': freight_df, 'arb_df': hidden_df, 'inclusions': inclusions,
                                    'subject_to': subject_to, 'header_details': header_details, 'notes': notes_df}

            return self.captured_output

        def clean(self):

            freight_df = self.captured_output['freight_df']
            arb_df = self.captured_output['arb_df']
            inclusions = self.captured_output['inclusions']
            subject_to = self.captured_output['subject_to']
            header_details = self.captured_output['header_details']
            notes_df = self.captured_output['notes']

            notes = ''
            for element in notes_df.iloc[1:]:
                if '**' in element:
                    element = element.replace('**', '') + ';'
                    notes += element
            notes = notes[0:len(notes)-1]

            '''FREIGHT_DATA_FRAME'''

            freight_df.iloc[0, 1] = 'service_type'
            freight_df.iloc[1:, 1] = freight_df.iloc[1:, 2] + ':' + freight_df.iloc[1:, 7]
            freight_df.columns = freight_df.iloc[0]
            freight_df = freight_df.iloc[1:, ]
            freight_df['inclusions'] = ';'.join(inclusions)
            freight_df['inclusions'] = freight_df['inclusions'].str.replace(', ', ';')
            freight_df['subject_to'] = ';'.join(subject_to.keys())
            freight_df['start_date'] = header_details['start_date']
            freight_df['expiry_date'] = header_details['expiry_date']
            freight_df['commodity'] = header_details['commodity']
            freight_df['notes'] = notes
            if freight_df['remarks'].str.contains('included').any():
                temp = freight_df.loc[freight_df['remarks'].str.contains('included')]['inclusions'].copy(deep=True)
                adder = freight_df.loc[freight_df['remarks'].str.contains('included')]['remarks'].str.split("included|\n", expand=True)[0].str.extract(r"\((.+?)\)").copy(deep=True)
                freight_df.loc[freight_df['remarks'].str.contains('included'), 'inclusions'] = temp + ";" + adder.iloc[:, 0]
                freight_df.loc[freight_df['remarks'].str.contains('included|subject'), 'remarks'] = freight_df.loc[freight_df['remarks'].str.contains('subject')]['remarks'].str.split("\n", expand=True)[1]
                freight_df['remarks'] = freight_df['remarks'].fillna('')

            if freight_df['remarks'].str.contains('subject').any():
                temp = freight_df.loc[freight_df['remarks'].str.contains('subject')]['subject_to'].copy(deep=True)
                adder = freight_df.loc[freight_df['remarks'].str.contains('subject')]['remarks'].str.split("subject", expand=True)[1].str.extract(r"\((.+?)\)").copy(deep=True)
                freight_df.loc[freight_df['remarks'].str.contains('subject'), 'subject_to'] = temp + ";" + adder.iloc[:, 0]
            '''
            if freight_df['remarks'].str.contains('included|subject').any():
                freight_df.loc[freight_df['remarks'].str.contains('included|subject'), 'remarks'] = ''
            '''
            freight_df['contract_id'] = header_details['contract_id']
            freight_df['contract_id'] = freight_df['contract_id'] + '\n' + freight_df['remarks']
            freight_df.contract_id = freight_df.contract_id.apply(lambda x: x.strip("\n ") if isinstance(x, str) else '')
            freight_df.drop([''], axis=1, inplace=True)
            freight_cols = {'Origin Country': 'origin_country', 'Origin Port': 'origin_port'
                            , 'Destination Country': 'destination_country', 'Destination Port': 'destination_port'
                            , 'Routing Option': 'via', '20\' DC': '20GP', '40\' DC': '40GP', '40\' HC': '40HC'
                            }
            freight_df.rename(columns=freight_cols, inplace=True)

            freight_df['origin_port'] = freight_df['origin_port'].str.split(r'/')
            freight_df = freight_df.explode('origin_port')
            port_pair_lookup = {
                "WVN": "Wilhelmshaven",
                "HAM": "Hamburg",
                "RTM": "Rotterdam",
                "ANR": "Antwerpen",
                "ZEE": "Zeebruegge",
                "BHV": "Bremerhaven"}

            freight_df['origin_port'] = freight_df['origin_port'].replace(port_pair_lookup, regex=True)

            freight_df.loc[(freight_df["via"].str.contains("Direct")), "via"] = ""
            freight_df.reset_index(drop=True, inplace=True)
            freight_df.loc[freight_df["via"].str.contains("via"), "via"] = freight_df.loc[freight_df["via"].str.contains("via"), "via"].str.replace("via", "").apply(lambda x: x.strip())

            freight_df.loc[freight_df["20GP"].str.contains("no offer", na=False), "20GP"] = "ON REQUEST"

            freight_df.loc[freight_df["40GP"].str.contains("no offer", na=False), "40GP"] = "ON REQUEST"

            freight_df.loc[freight_df["40HC"].str.contains("no offer", na=False), "40HC"] = "ON REQUEST"

            freight_df.loc[freight_df["via"].str.contains("no offer", na=False), "via"] = ""

            freight_df.dropna(subset=["20GP", "40GP", "40HC"], inplace=True)

            freight_df['destination_port'] = freight_df['destination_port'].str.split('\(', expand=True)[0].apply(lambda x: x.strip())

            freight_df.origin_port = freight_df.origin_port.apply(lambda x: x.strip())

            freight_df['currency'] = 'USD'

            freight_df['contract_no'], freight_df['sub_vendor'] = "G2002860", "OCEANNETWORKEXPRESSPTELTD(SG-20537HAMBURG)"

            freight_df.loc[(freight_df['20GP'] == '') & (freight_df['40GP'] == '') & (freight_df['40HC'] == '')
                           , ('20GP', '40GP', '40HC')] = "ON REQUEST", "ON REQUEST", "ON REQUEST"

            '''ARBITRARY_DATA_FRAME'''

            start_date_arb_index = arb_df[(arb_df[1].str.contains('Date'))].index.values[0]
            start_date_arb = parse(arb_df.loc[int(start_date_arb_index)][2]).date().strftime('%d-%m-%Y')

            arb_df = arb_df.iloc[:, 7:15]
            arb_df.columns = arb_df.iloc[0]
            arb_df = arb_df.iloc[1:, ]

            arb_df['start_date'] = start_date_arb
            arb_cols = {'Country': 'country', 'Port Correct Wording': 'icd', 'Terminal': 'via', 'Term': 'service_type'
                        , 'T/S Port': 'to', '20\'DC': '20GP', '40\'DC': '40GP', '40\'HC': '40HC'}
            arb_df['contract_no'], arb_df['sub_vendor'] = "G2002860", "OCEANNETWORKEXPRESSPTELTD(SG-20537HAMBURG)"
            arb_df.rename(columns=arb_cols, inplace=True)

            self.cleaned_output = {'Freight': freight_df}
            return self.cleaned_output

    class Surcharge_(BaseFix):

        def check_input(self):

            pass

        def check_output(self):

            pass

        def capture(self):

            surcharge_df = self.df
            if surcharge_df[7].str.contains('Date').any():
                start_date_index = list(surcharge_df[(surcharge_df[7].str.contains('Date'))].index)[0]
                start_date = surcharge_df.iloc[start_date_index, 7].split(':')[-1].strip()
            if surcharge_df[1].str.contains('Description').any():
                description_indices = list(surcharge_df[(surcharge_df[1].str.contains('Description'))].index)
                end_index = surcharge_df[(surcharge_df[5].str.contains('Actual surcharges'))].index.values[0]
                surcharge_dfs = []
                for i in range(len(description_indices)):
                    holder = surcharge_df.iloc[description_indices[i]:description_indices[i+1]-1, 1:7]
                    holder['unique'] = surcharge_df.iloc[description_indices[i]-1, 1]

                    """If Bunker Surcharges has two date sections"""

                    date_check = holder.index[holder.iloc[:, 2] == ''].to_list()
                    if len(date_check) > 1:
                        holder = holder.loc[date_check[-1]:, :]

                    surcharge_dfs.append(holder)
                    if i+2 == len(description_indices):
                        holder = surcharge_df.iloc[description_indices[i+1]:end_index, 1:7]
                        holder['unique'] = surcharge_df.iloc[description_indices[i+1]-1, 1]
                        surcharge_dfs.append(holder)
                        break
                for i in range(1, len(surcharge_dfs)):
                    surcharge_dfs[i] = surcharge_dfs[i].iloc[1:, :]
            if surcharge_dfs:
                surcharge_df = pd.concat(surcharge_dfs)
            surcharge_df['unique'].iloc[0] = 'unique'
            surcharge_df.columns = surcharge_df.iloc[0]
            # surcharge_df.rename(columns=surcharge_df.iloc[0], inplace=True)
            surcharge_df = surcharge_df.iloc[1:, :]
            surcharge_df = remarks_util.Remarks.surcharge_remarks(surcharge_df)
            surcharge_df['country'] = surcharge_df['country'].str.replace(',', ';').str.replace(
                '/',
                ';')
            surcharge_df['country'] = surcharge_df['country'].str.split(';')
            surcharge_df = surcharge_df.explode('country')
            surcharge_df['country'] = surcharge_df['country'].apply(
                lambda x: x.strip() if isinstance(x, str) else x)
            cols = {'Description': 'charges', 'Charge Code': 'charge_code', 'Rate': 'amount', 'Currency': 'currency'
                    , 'Rate Base': 'load_type', 'Remarks': 'remarks', 'country': 'destination_country'
                    }
            surcharge_df.rename(columns=cols, inplace=True)

            surcharge_df["load_type"] = surcharge_df["load_type"].str.replace("per 40'DC and 40'HC", "40GP=;40HC")
            surcharge_df["load_type"] = surcharge_df["load_type"].str.split("=;")
            surcharge_df = surcharge_df.explode("load_type")

            self.captured_output = {'Charges': surcharge_df}

            return self.captured_output

        def clean(self):

            surcharge_df = self.captured_output['Charges']
            surcharge_df.charge_code = surcharge_df.charge_code.replace('', nan)
            surcharge_df.dropna(subset=["charge_code"], inplace=True)
            surcharge_df['contract_no'], surcharge_df['sub_vendor'] = "G2002860", "OCEANNETWORKEXPRESSPTELTD(SG-20537HAMBURG)"
            self.cleaned_output = {'Charges': surcharge_df}
            return self.cleaned_output

    def resolve_dependency(cls, fix_outputs):

        if "AEE (HAM&RTM&ANR)" in fix_outputs:
            df_freight = fix_outputs.pop("AEE (HAM&RTM&ANR)")
            freight_df_1 = df_freight["Freight"]
            # arb_df_1 = df_freight['Arbitrary']
            freight_df_1["unique"] = "AEE (HAM&RTM&ANR)"
            # arb_df_1['unique'] = "AEE (HAM&RTM&ANR)"

        if "AEE (SOU&LGP)" in fix_outputs:
            df_freight = fix_outputs.pop("AEE (SOU&LGP)")
            freight_df_2 = df_freight["Freight"]
            # arb_df_2 = df_freight['Arbitrary']
            freight_df_2["unique"] = "AEE (SOU&LGP)"
            # arb_df_2['unique'] = "AEE (SOU&LGP)"

        if "ME & INDIA" in fix_outputs:
            df_freight = fix_outputs.pop("ME & INDIA")
            freight_df_3 = df_freight["Freight"]
            # arb_df_3 = df_freight['Arbitrary']
            freight_df_3["unique"] = "ME & INDIA"
            # arb_df_3['unique'] = "ME & INDIA"

        if "ONE Tariff AEE" in fix_outputs:
            df_surcharge = fix_outputs.pop('ONE Tariff AEE')
            surcharge_df = df_surcharge['Charges'].copy(deep=True)
            surcharge_df.loc[surcharge_df['standard_destination_country_country_name'].str.contains(';', na=False), 'region'] \
                = True
            surcharge_df['region'] = surcharge_df['region'].fillna(False)
            surcharge_df['standard_destination_country_country_name'] = surcharge_df['standard_destination_country_country_name'].str.split(';')
            surcharge_df['standard_destination_country_country_code'] = surcharge_df['standard_destination_country_country_code'].str.split(';')
            surcharge_df = surcharge_df.explode(['standard_destination_country_country_name', 'standard_destination_country_country_code'], ignore_index=True)
            surcharge_df.sort_values(by=['region'], ascending=True, inplace=True, ignore_index=True)
            surcharge_df = surcharge_df.loc[~(surcharge_df['load_type'].str.contains('Reefer', na=False))]
            surcharge_df.drop_duplicates(subset=['standard_destination_country_country_name', 'standard_destination_country_country_code', 'standard_charges_surcharges_text'], keep='first', inplace=True, ignore_index=True)
            # surcharge_df['start_date'], surcharge_df['expiry_date'] = df_freight['header_details']['start_date']\
                # , df_freight['header_details']['expiry_date']
            surcharge_df.drop(['region'], axis=1, inplace=True)
        freight_df = pd.concat([freight_df_1, freight_df_2, freight_df_3], ignore_index=False)
        destination_country = list(map(lambda x: x.upper(), list(freight_df['destination_country'].unique())))
        destination_country.extend([nan, "UNITED ARAB EMIRATES", "RIYADH", "DAMMAM"])
        surcharge_df = surcharge_df.loc[surcharge_df["standard_destination_country_country_name"].isin(destination_country)]
        surcharge_df['start_date'], surcharge_df['expiry_date'] = freight_df['start_date'].iloc[0], freight_df['expiry_date'].iloc[0]
        # arb_df = pd.concat([arb_df_1, arb_df_2, arb_df_3], ignore_index=False)
        fix_outputs = {"Freight": freight_df, "Charges": surcharge_df}

        return fix_outputs



class NTG_ONE_AETWB(BaseTemplate):
    class Rates(BaseFix):

        def remove_empty_rows_and_columns(self):
            pass

        def check_input(self):
            pass

        def check_output(self):
            pass

        def capture(self):
            synonym_list = self.header_template['synonym_list']
            header_search_limit = self.header_template['header_search_limit']
            self.df = self.df[self.headers_config['headers']]
            df = self.df[self.headers_config['actual_data_start_row']:]
            df.reset_index(drop=True, inplace=True)
            try:
                end_index = df[(df.isnull().sum(axis=1) >= df.shape[1])].index[0]
                if end_index != len(df):
                    df_output = df[:end_index]
                    header_df = df.copy(deep=True)
                    while True:
                        header_df = header_df[end_index:].reset_index(drop=True)
                        headers_config = get_headers(synonym_list, header_df, header_search_limit)
                        if not headers_config['actual_data_start_row']:
                            break
                        header_df = header_df[self.headers_config['actual_data_start_row']:]
                        header_df.reset_index(drop=True, inplace=True)
                        end_index = \
                            header_df[(header_df.isnull().sum(axis=1) >= len(self.headers_config['headers'].keys()))].index[
                                0]
                        header_df = header_df[:end_index]
                        df_output = pd.concat([df_output, header_df])
                else:
                    df_output = df
            except IndexError:
                df_output = df

            df_output = df_utils.column_conversion(df_output,
                                                   ['Rotterdam', 'Antwerp', 'Hamburg', 'Le Havre', 'Southampton',
                                                    'London Gateway'], 'destination_port')

            self.captured_output = df_output

        def clean(self):
            df = self.captured_output
            df = df_utils.skipIncompleteRows(df, ['20GP', '40GP'])
            self.cleaned_output = {"Freight": df}

    class org_arb(Rates):

        def clean(self):
            self.cleaned_output = {"Arbitrary Charges": self.captured_output}
