from logging import getLogger

from base import BaseTemplate, BaseFix

log = getLogger(__name__)


class SITC_Pdf_Far_East(BaseTemplate):
    class Sitc_Ceva(BaseFix):

        def check_input(self):
            pass

        def check_output(self):
            pass

        def freight_table(self, freight_df):

            freight_df['inclusions'] = freight_df.loc[freight_df['Remarks'].str.contains('INC')]['Remarks']
            freight_df['subject_to'] = freight_df.loc[freight_df['Remarks'].str.contains('SUB')]['Remarks']
            freight_df['inclusions'] = freight_df['inclusions'].str.split(expand = True)[1]
            unique = list(freight_df['inclusions'].unique())
            subject_to = []
            for element in unique:
                if isinstance(element, str) and '/' in element:
                    holder = element.split('/')
                    for value in holder:
                        subject_to.append(value)
                elif isinstance(element, str):
                    subject_to.append(element)
            subject_to = "/".join(list(set(subject_to)))
            freight_df = freight_df.replace(to_replace='SUB TO ALL', value = subject_to, regex=True)
            contract_number = freight_df.loc[:,'SC NO.:']
            contract_number = str(contract_number[10])
            freight_df['contract_number'] = contract_number
            freight_df.drop(['Remarks','POD F/T','F/T NO.:','SC NO.:'], axis = 1, inplace = True)
            freight_df['20GP'] = freight_df['20GP'].str.split('$' , expand=True)[1]
            freight_df['40GP'] = freight_df['40GP'].str.split('$' , expand=True)[1]
            freight_df['40HC'] = freight_df['40HC'].str.split('$' , expand=True)[1]
            freight_df['currency'] = 'USD'

            return freight_df

        def capture(self):

            index_POL = list(self.df[(self.df[0].str.contains("POL", na=False))].index)
            freight_df = self.df[index_POL[0]:self.df.tail(1).index.values[0] + 1].copy(deep=True)
            freight_df.columns = freight_df.iloc[0]
            freight_df = freight_df[1:].copy()
            date_index = list(self.df[(self.df[0].str.contains("Period of Validity"))].index)[0]
            date = self.df.iloc[date_index, 2]
            date_list = date.split("-")
            start_date = date_list[0].replace('.','-')
            expiry_date = date_list[1].replace('.','-')
            index_remarks = list(self.df[(self.df[0].str.contains("Remarks", na=False))].index)[0]
            remarks = [self.df.iloc[index_remarks, 1], self.df.iloc[index_remarks + 1, 1]]
            remarks_dict = {}
            for element in remarks:
                remarks_dict[element.split('=')[0]] = element.split('=')[-1]
            freight_df = freight_df.loc[:index_remarks - 1, :]
            JPN7 = remarks_dict['JPN 7 '].replace(',', ';').replace(' ','')
            TWN3 = remarks_dict['TWN3 '].replace(',', ';').replace(' ','')
            freight_df.loc[freight_df['POD'].str.contains('JPN 7', na = False, case= False ),'POD'] = JPN7
            freight_df.loc[freight_df['POD'].str.contains('TWN3', na=False, case=False), 'POD'] = TWN3
            freight_df['start_date'] = start_date
            freight_df['expiry_date'] = expiry_date
            freight_df.rename(columns= {'POL':'origin_port','POD':'destination_port'}, inplace = True)
            freight_df = self.freight_table(freight_df)

            self.captured_output = {"Freight": freight_df}

        def clean(self):

            self.cleaned_output = self.captured_output

