import os
from pathlib import Path
import pandas as pd
import numpy as np
from pydub import AudioSegment
from numpy.random import seed
from numpy.random import randint
from datetime import datetime as dt
import logging
import multiprocessing as mp
import traceback
logger = mp.log_to_stderr(logging.DEBUG)
from functools import partial

class getFromSWC:

    def __init__(self):
        self.path = ''#Path('/Users/adityabhatt/Downloads').parent.absolute()
        print(self.path)

    def _get_shortened_dict(self):
        i = 0
        dict_terms_count_shorten = {}
        for file in os.listdir('english3'):
            try:
                path2 = str(self.path) + 'english3/' + file
                if 'audio.ogg' in os.listdir(path2) and 'aligned.swc.df' in os.listdir(path2):
                    i += 1
                    df = pd.read_csv(path2 + '/aligned.swc.df', delimiter='\t')
                    df.drop(columns='Unnamed: 0', inplace=True)
                    # Get indexes where name column has value john
                    indexNames = df[(df['start_time'] == 0) | (df['end_time'] == 0)].index

                    # Delete these row indexes from dataFrame
                    df.drop(indexNames, inplace=True)

                    df = df.reset_index(drop=True)
                    for index, row in df.iterrows():
                        term_name = df.at[index, 'term'].lower().strip()
                        if term_name in dict_terms_count_shorten:
                            dict_terms_count_shorten[term_name] = dict_terms_count_shorten[term_name] + 1
                        else:
                            dict_terms_count_shorten[term_name] = 1
            except FileNotFoundError:
                print(f'something went wrong {traceback.format_exc()}')
            except NotADirectoryError:
                print(f'something went wrong {traceback.format_exc()}')
        for k, v in list(dict_terms_count_shorten.items()):
            if len(k) < 4:
                del dict_terms_count_shorten[k]
            elif v < 2:
                del dict_terms_count_shorten[k]
        return dict_terms_count_shorten

    @staticmethod
    def _get_sound_crop_to_output(dict_terms_count_shorten, path_output, path2, i, df):

        try:
            logger.debug(f'here {i}')
            for index, row in df.iterrows():
                term_name = df.at[index, 'term'].lower().strip()
                if term_name not in dict_terms_count_shorten:
                    continue
                sound = AudioSegment.from_file(path2 + '/audio.ogg')
                start_point = df.at[index, 'start_time']
                end_point = df.at[index, 'end_time']
                length_sound = end_point - start_point
                start_point = start_point - 200 if start_point - 200 > 0 else 0
                if start_point == 0:
                    print('found 0')
                if end_point == len(sound):
                    print('found len(sound)')
                end_point = end_point + 200 if end_point + 200 < len(sound) else len(sound)
                cropped_sound = sound[start_point:end_point]
                Path(path_output + term_name).mkdir(parents=True, exist_ok=True)
                seed(1)
                random_number = randint(0, 1000, 1)[0]
                cropped_sound.export(f"{path_output + term_name}/{i}_{length_sound}_{random_number}_{dt.now()}.ogg",
                                     format="ogg")
            return True
        except:
            logger.debug(traceback.format_exc())
            print(traceback.format_exc())
            return traceback.format_exc()

    def export_cropped_sounds_parallel(self):
        i = 0
        path_output = 'english2/'
        dict_terms_count_shorten = self._get_shortened_dict()
        print(f'got shorted dict.len = {len(dict_terms_count_shorten)}')
        for file in os.listdir('english3'):
            try:
                path2 = str(self.path) + 'english3/' + file

                if 'audio.ogg' in os.listdir(path2) and 'aligned.swc.df' in os.listdir(path2):
                    i += 1
                    df = pd.read_csv(path2 + '/aligned.swc.df', delimiter='\t')
                    print(path2)
                    df.drop(columns='Unnamed: 0', inplace=True)
                    # Get indexes where name column has value john
                    indexNames = df[(df['start_time'] == 0) | (df['end_time'] == 0)].index

                    # Delete these row indexes from dataFrame
                    df.drop(indexNames, inplace=True)

                    df = df.reset_index(drop=True)
                    num_cores = int(mp.cpu_count()/2.0)
                    df_splits = np.array_split(df, num_cores)
                    #getFromSWC._get_sound_crop_to_output(dict_terms_count_shorten,path_output, path2, i,df_splits[0])
                    with mp.Pool(num_cores) as p:
                        result = p.map(partial(getFromSWC._get_sound_crop_to_output,dict_terms_count_shorten, path_output, path2, i), df_splits)
                        print(result)

            except FileNotFoundError:
                print(f'something went wrong {traceback.format_exc()}')
            except NotADirectoryError:
                print(f'something went wrong {traceback.format_exc()}')
            except:
                print(f'something went wrong {traceback.format_exc()}')

if __name__ == '__main__':
    # y = getData()
    # y.get_mp3_youtube()
    # y = getDataIBM()
    # y.get_IBM_data()
    # y = convertToXML('post_IBM_request_2.csv')
    # y.convert_csv_XML()
    y = getFromSWC()
    y.export_cropped_sounds_parallel()
    print('PyCharm')