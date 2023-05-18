# -*- coding: utf-8 -*-
"""
@author: Hugo Franco, Roberto Arias
"""
try:
    import os, sys
    from pathlib import Path as p
    from pandas_profiling import ProfileReport

except Exception as exc:
            print('Module(s) {} are missing.:'.format(str(exc)))

dir_root = p(__file__).parents[1]
sys.path.append(str(p(dir_root) /'source' / 'clases'))


from cls_load_data_mf import load_data_mf as data_loader

#%%
''' Crear una instancia de la clase (objeto) '''
loader = data_loader()
loader.path = dir_root

#%%
'''Autenticación en el api de Kaggle'''
path_auth = str(p(loader.path) / 'kaggle')
loader.set_kaggle_api(path_auth)

#%%
''' Existe el directorio destino '''
path_data = str(p(loader.path) / 'Dataset' / 'nfl')
loader.check_path(path_data)
print(loader.dir_exist)

#%%
''' Espacio disponible en disco '''
path_data = str(p(loader.path) / 'Dataset' / 'Finance')
loader.check_free_space(path_data)
loader.check_used_space(path_data)

#%%
''' Listar competencias'''
loader.list_competition_kaggle(competition="nfl")

#%%
''' Listar archivos en una competencia. Ejemplo: 3'''
loader.competition = str(loader.lst_competition[3])
print("Competencia: ", loader.competition)
loader.list_files_competition_kaggle()

#%%
''' Descargar archivos de una competencia'''
path_data = str(p(loader.path) / 'Dataset' / 'nfl')
loader.dataset = str(loader.lst_files_c[0])
loader.get_data_from_kaggle_c(path_data)


#%%
''' Listar datasets'''
#loader.list_dataset_kaggle('cancer')
loader.list_dataset_kaggle('trending youtube')

loader.show_kaggle_datasets()

#%%
''' Descargar todos los archivos de un dataset alojado kaggle '''

path_data = str(p(loader.path) / 'Dataset' / 'Trending YouTube Video Statistics')
loader.get_data_from_kaggle_d(path_data,'datasnaek/youtube-new')


#%%
''' Listar archivos según el tipo'''
path_data = str(p(loader.path) / 'Dataset' / 'Trending YouTube Video Statistics')
loader.get_lst_files(path_data,'csv')
print('{}Instancia kaggle:'.format(os.linesep))
loader.muestra_archivos()

#%%
''' Cargar datos a memoria'''
print(loader.lst_files[0:3])


#%%
''' Mostrar datos'''
print(loader.data)

#%%
''' Crear perfil de los datos '''
df = loader.data
profile = ProfileReport(df, 
                        title="Pandas Profiling Report", 
                        explorative=True,
                        minimal=True)

profile.to_file("pandas_profiling_report.html")