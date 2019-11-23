import numpy as np
import os
import pandas as pd
import re
from sklearn.compose import ColumnTransformer, make_column_transformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from stemming.porter2 import stem


class ExcelIO:
    def __init__(self, raw_df=None, path=None):
        self.raw_df = raw_df
        self.path = path

    def import_csv(self):
        self.path = os.path.dirname(os.path.abspath(__file__)) + "/"
        self.raw_df = pd.read_csv(self.path + "gun-violence-data_01-2013_03-2018.csv")


# excel = ExcelIO()
# excel.import_csv()


class DFMiner:
    def __init__(self, df, incident_list=None, pivot_state_n_city=None):
        self.df = df
        self.incident_list = incident_list
        self.pivot_state_n_city = pivot_state_n_city

    # ============================ #
    def get_division(self, x):
        division_dict = {"Connecticut": 1, "Maine": 1, "Massachusetts": 1, "New Hampshire": 1,
                       "Rhode Island": 1, "Vermont": 1,"New Jersey": 2, "New York": 2, "Pennsylvania": 2,
                       "Illinois": 3, "Indiana": 3, "Michigan": 3, "Ohio": 3, "Wisconsin": 3,
                       "Iowa": 4, "Kansas": 4, "Minnesota": 4, "Missouri": 4,
                       "Nebraska": 4, "North Dakota": 4, "South Dakota": 4,
                       "Delaware": 5, "Florida": 5, "Georgia": 5, "Maryland": 5, "North Carolina": 5,
                       "South Carolina": 5, "Virginia": 5, "District of Columbia": 5, "West Virginia": 5,
                       "Alabama": 6, "Kentucky": 6, "Mississippi": 6, "Tennessee": 6,
                       "Arkansas": 7, "Louisiana": 7, "Oklahoma": 7, "Texas": 7,
                       "Arizona": 8, "Colorado": 8, "Idaho": 8, "Montana": 8,
                       "Nevada": 8, "New Mexico": 8, "Utah": 8, "Wyoming": 8,
                       "Alaska": 9, "California": 9, "Hawaii": 9, "Oregon": 9, "Washington": 9}

        for key in division_dict:
            if key in str(x):
                return division_dict[key]

    def get_region(self, x):
        region_dict = {1: 1, 2: 1, 3: 2, 4: 2, 5: 3, 6: 3, 7: 3, 8: 4, 9: 4}

        for key in region_dict:
            if str(key) in str(x):
                return region_dict[key]

    def get_state_n_city(self, x, y): return x + "_" + y

    def fill_na_4_state_n_city(self):
        self.pivot_state_n_city = pd.pivot_table(self.df, index='city_or_county',
                                                 columns='congressional_district', values='latitude',
                                                 aggfunc=lambda x: x.value_counts().count())
        # self.pivot_state_n_city = pd.DataFrame(data=self.pivot_state_n_city.max(axis=1), columns=['count'])
        self.pivot_state_n_city = self.pivot_state_n_city.idxmax(axis=1)

    def get_street_type(self, x):
        street_dict = {"STREET": "STREET", " ST.": "STREET", "AVENUE": "AVENUE", " AV": "AVENUE",
                      "ROAD": "ROAD", " RD": "ROAD", "DRIVE": "DRIVE",
                       "BOULEVARD": "BOULEVARD", "BLVD": "BOULEVARD", "COURT": "COURT"}

        for key in street_dict:
            if key in str(x).upper():
                return street_dict[key]

    # Get source index Page by long hyperlink
    def get_source(self, x):
        try:
            return str(x).split("http")[1].split("//")[1].split("/")[0]
        except:
            pass

    def get_no_of_gun(self, x):
        if pd.isnull(x):
            return 0
        else:
            return (len(str(x).split("|"))+1)/2

    # ============================ #
    def get_gun_type(self, x): return list(filter(lambda a: a not in ['', 'nan'],
                                                  re.sub(r'[^A-Za-z ]+', '_', str(x)).split('_')))

    def change_gun_type_list_to_col(self):
        gun_type_list = []
        for i in range(mined.df.shape[0]):
            tmp_gun_lst = mined.df.gun_type_lst[i]
            for item in tmp_gun_lst:
                if item not in gun_type_list:
                    gun_type_list.append(item)

        gun_type_list = filter(None, gun_type_list)

        for item in gun_type_list:
            col_with_gun_name = 'gun_type_' + item
            self.df[col_with_gun_name] = self.df['gun_type_lst'].apply(lambda x: str(x).count(item))

    # ============================= #
    def create_incident_lst(self):
        # Create Unique Incident List for
        self.Incident_list = []
        for index, rows in self.df.iterrows():
            try:
                case_inc_list = list(filter(None, rows.incident_characteristics.split('|')))
                for case in case_inc_list:
                    if case not in self.Incident_list:
                        self.Incident_list.append(case)
            except:
                continue
        # for item in sorted(self.Incident_list):
        #     print(item)

        # Create Column For Each Element in Incident List
        for idx, val in enumerate(self.Incident_list):
            if idx < 50:
                self.df[val] = self.df['incident_characteristics'].apply(lambda x: str(x).count(val))

    # ============================ #
    def get_venue_2(self, x):
        tmp_venue = str(x).upper().replace('(', '').replace(')', '').split(' ')[-1]
        tmp_venue = ''.join([i for i in tmp_venue if not i.isdigit()])
        return stem(tmp_venue) # stem = remove plural

    # ============================== #
    def get_participant_array(self, x):
        # lst = re.split(r'[|:]', str(x))
        # regular expression: lookahead and lookbehind,
        lst = re.split("(?<![|:])[|:]", str(x))  # error
        target_lst = []

        for i in range(len(lst)):
            if i % 2 == 1:
                target_lst.append(lst[i][1:])
        return list(map(int, filter(None, target_lst)))

    # ============================ #
    def main(self):
        self.df['year'] = self.df['date'].apply(lambda x: x[0:4])
        self.df['month'] = self.df['date'].apply(lambda x: x[5:7])
        self.df['day'] = self.df['date'].apply(lambda x: x[8:10])

        # state: specify to different section in america
        self.df['division'] = self.df['state'].apply(self.get_division)
        self.df['region'] = self.df['division'].apply(self.get_region)

        # ===== City or county: Although City or county are same, it may represent two places =====
        self.df['city_or_county'] = np.vectorize(self.get_state_n_city)(self.df['state'], self.df['city_or_county'])

        self.df['street_type'] = self.df['address'].apply(self.get_street_type)
        self.df['source_url'] = self.df['source_url'].apply(self.get_source)

        # ===== congressional_district: NAN can be guessed by city or county =====
        self.fill_na_4_state_n_city()  # Memory Error
        self.df['congressional_district'] = pd.to_numeric(self.df['congressional_district'], errors='coerce').fillna(-1)

        # Gun Stolen
        self.df['no_of_gun'] = self.df['gun_stolen'].apply(self.get_no_of_gun)
        self.df['no_of_gun_stolen'] = self.df['gun_stolen'].apply(lambda x: str(x).count("Stolen"))
        self.df['no_of_gun_not_stolen'] = self.df['gun_stolen'].apply(lambda x: str(x).count("Not-stolen"))
        self.df['no_of_gun_unknown'] = self.df['gun_stolen'].apply(lambda x: str(x).count("Unknown"))

        # Gun Type: List can have further study
        self.df['gun_type_lst'] = self.df['gun_type'].apply(self.get_gun_type)
        self.change_gun_type_list_to_col()

        # ===== Incident Characteristics ===== #
        # *. Split it to Array, then find the unique content (Memory Error)
        # self.create_incident_lst()

        # ===== Latitude and Longitude ===== #
        # *. For nan case, fill with relative city or county mean latitude and longitude
        self.df['latitude'] = self.df.groupby("city_or_county").transform(lambda x: x.fillna(x.mean()))['latitude']
        self.df['longitude'] = self.df.groupby("city_or_county").transform(lambda x: x.fillna(x.mean()))['longitude']

        # ===== Location Description ===== #
        # 1. Just try to show some venue example.
        # v. For better operation in the future, we should try to analysis the wording in depth
        # v. Scrap the Last Word, remove the s, () and integer
        # ~. only 20% row have data --> estimated the location by latitude and longitude
        # ~. For same latitude and longitude --> Use Longer Name (e.g. 19.695 Puainako Center)
        # *. Only Keep Hot Keyword
        self.df['venue'] = self.df['location_description'].apply(self.get_venue_2)

        # ===== n_guns_involved =====＃
        # 1. Cross Check with gun_stolen if necessary

        # ===== Notes ===== #
        # *. Contains Hot Venue --> Mark as Venue
        # 2. Contain Party, e.g. Police, Driver, Family(Mother, Sons) ...
        # 3. 80% Data
        # 4. Left Trim, Remove Special Character at start and end

        # ===== participant_age ===== #
        # *. single colon handling
        self.df['participant_age_array'] = self.df['participant_age'].apply(self.get_participant_array)

        self.df['participant_adults'] = self.df['participant_age_group'].apply(lambda x: str(x).count("Adult 18+"))
        self.df['participant_teens'] = self.df['participant_age_group'].apply(lambda x: str(x).count("Teen 12-17"))
        self.df['participant_children'] = self.df['participant_age_group'].apply(lambda x: str(x).count("Child 0-11"))

        self.df['participant_male'] = self.df['participant_gender'].apply(lambda x: str(x).count("Male"))
        self.df['participant_female'] = self.df['participant_gender'].apply(lambda x: str(x).count("Female"))

        # Participant Status are not independent, some participant have two or more status
        self.df['participant_arrested'] = self.df['participant_status'].apply(lambda x: str(x).count("Arrested"))
        self.df['participant_injured'] = self.df['participant_status'].apply(lambda x: str(x).count("Injured"))
        self.df['participant_killed'] = self.df['participant_status'].apply(lambda x: str(x).count("Killed"))
        self.df['participant_unharmed'] = self.df['participant_status'].apply(lambda x: str(x).count("Unharmed"))

        self.df['participant_victim'] = self.df['participant_type'].apply(lambda x: str(x).count("Victim"))
        self.df['participant_subject_suspect'] = self.df['participant_type'].\
            apply(lambda x: str(x).count("Subject-Suspect"))


mined = DFMiner(excel.raw_df.copy())  # Make sure excel.df are independent to mined.df
mined.main()


class DFReformer:
    def __init__(self, df):
        self.df = df

    def remove_column(self):
        self.df = self.df.drop(['city_or_county', 'address', 'incident_url', 'source_url',
                                'incident_url_fields_missing', 'gun_stolen', 'gun_type', 'incident_characteristics',
                                'location_description', 'notes',
                                'participant_age', 'participant_age_group', 'participant_gender',
                                'participant_name', 'participant_relationship',
                                'participant_status', 'participant_type', 'sources', 'gun_type_lst',
                                'participant_age_array'], 1)

    def main(self):
        self.remove_column()


reformed_df = DFReformer(mined.df.copy())
reformed_df.main()

# print(reformed_df.df['city_or_county'].value_counts())
# test = excel.raw_df.sort_values(by='congressional_district', ascending=True)
# test = mined.df.sort_values(by=['notes'], ascending=[True])


# class OneHotEncoderClass:
#     def __init__(self, df, df_123=None):
#         self.df = df
#         self.df_123 = df_123
#
#     def encode_by_one_hot(self):
#         # try new method, exist error: required positional argument
#         feature = self.df[['latitude', 'longitude', 'street_type']]
#         pre_process = make_column_transformer(
#             (StandardScaler(), ['latitude', 'longitude']),
#             (OneHotEncoder(), ['street_type'], )
#         )
#         data_str_ohe = pre_process.fit_transform(feature).toarray()[:3]
#         self.df_123 = pd.DataFrame(data_str_ohe)
#
#     def example(self):
#         feature = self.df[['latitude', 'longitude', 'street_type']]
#
#         numeric_features = ['latitude', 'longitude']
#         numeric_transformer = Pipeline(steps=[('imputer', SimpleImputer(strategy='median')),
#                                               ('scaler', StandardScaler())])
#
#         categorical_features = ['street_type']
#         categorical_transformer = Pipeline(steps=[('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
#                                                   ('onehot', OneHotEncoder(handle_unknown='ignore'))])
#
#         preprocessor = ColumnTransformer(
#             transformers=[('num', numeric_transformer, numeric_features),
#                           ('cat', categorical_transformer, categorical_features)])
#
#         data_str_ohe = preprocessor.fit_transform(feature).toarray()  # error exists here
#         self.df_123 = pd.DataFrame(data_str_ohe)
#
#     def main(self):
#         # self.encode_by_one_hot()
#         # self.example()
#         # self.df = pd.get_dummies(self.df)
#         pass

# encoded = OneHotEncoderClass(mined.df.copy())
# encoded.main()

# ================ #
# Notes: Time may be mined

