# -*- coding: utf-8 -*-
__author__ = "Hugo Franco, Roberto Arias"
__maintainer__ = "Hugo Franco, Roberto Arias - ETL"
__copyright__ = "Copyright 2021"
__version__ = "0.0.4"

try:
    import pandas as pd # Análisis de datos en Python
    import pprint # Escritura recursiva de listas y diccionarios

    import os # funciones asociadas al sistema operativo
    import chardet # detección de codificaciones de archivos
    import shutil # copiar y guardar archivos y árboles de directorios
    import glob # Gestión de nombres de archivo
    
except Exception as exc:
            print('Module(s) {} are missing.:'.format(str(exc)))

#%%
class load_data_mf(object):
    def __init__(self, path=None,percent=None):
        '''
        Constructor de inicialización

        Parameters
        ----------
        path : TYPE, optional
            Ruta física hacia los datos. The default is None.
        percent : TYPE, optional
            porcentaje de datos cargados desde el origen. The default is None.

        Returns
        -------
        None.

        '''
        self.path = path
        self.percent = percent
        self.data = None
        self.lst_files = None
        self.competition = None
        self.dataset = None

#%%
    def set_kaggle_api(self,path_auth):
        '''
        Validar acceso a los datos.

        Returns
        -------
        None.

        '''
        try:
            os.environ['KAGGLE_CONFIG_DIR'] = path_auth
            
            from kaggle.api.kaggle_api_extended import KaggleApi
            self.api = KaggleApi()
            self.api.authenticate()
        
        except Exception as exc:
            self.show_error(exc)
   
#%%         
    def check_used_space(self,path):
        try:
            total_size = 0
            
            #use the walk() method to navigate through directory tree
            for dirpath, dirnames, filenames in os.walk(path):
                for i in filenames:
                    
                    #use join to concatenate all the components of path
                    f = os.path.join(dirpath, i)
                    
                    #use getsize to generate size in bytes and add it to the total size
                    total_size += os.path.getsize(f)
            
            self.bytes = total_size
            total_size = self.formatSize()
        
        except Exception as exc:
            self.show_error(exc)
        
        print('Espacio usado por el destino: {}'.format(total_size))
 
#%%           
    def check_free_space(self,path_data):
        try:
            self.bytes = shutil.disk_usage(str(path_data))[2]
            free_space = self.formatSize()
            print('Espacio libre en disco: {}'.format(free_space))
        
        except Exception as exc:
            self.show_error(exc)

#%%
    def formatSize(self):
        '''
        Convert a raw size into a readable representation (GMk)
        
        Returns
        -------
        Size as a human-readable string.
        
        '''
        
        try:
            bytes = float(self.bytes)
            kb = bytes / 1024
        except:
            return "Error"
        if kb >= 1024:
            M = kb / 1024
            if M >= 1024:
                G = M / 1024
                return "%.2fG" % (G)
            else:
                return "%.2fM" % (M)
        elif kb == 0:
            return 'Folder vacio'
        else:
            return "%.2fkb" % (kb)


#%%
    def check_path(self,path_check):
        '''
        Valida que exista el path

        Returns
        -------
        None.

        '''
        self.dir_exist = os.path.exists(path_check)
        
#%%
    def get_lst_files(self,path_data,tipo):
        '''
        Lista los archivo de un directorio segun el tipo de solicitado.

        Parameters
        ----------
        path_data : string
            Ruta del directorio que contiene los archivos.
        tipo : string
            Extensión o tipo de archivo.

        Returns
        -------
        None.

        '''
        try:
            self.lst_files = [f for f in glob.glob(str(path_data)+'/**/*.'+ tipo.lower(), recursive=True)]
            
        except Exception as exc:
            self.show_error(exc)
    
 
#%%       
    # Carga datos desde archivos tipo CSV
    def get_data_csv(self, the_path):
        '''
        Parameters
        ----------
        the_path : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        '''

        try:
            self.data = pd.read_csv(the_path)
            
        except Exception as exc:
            self.show_error(exc)

#%%            
    def get_data_csv_nozip(self, the_path):
        '''
        Obtiene un archivo de datos en CSV sin comprimir
        
        Parameters
        ----------
        the_path : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        '''

        try:
            with open(the_path, 'rb') as fx:
                result = chardet.detect(fx.read())
                
            child = os.path.splitext(os.path.basename(the_path))[0]
            print('File: {} - {}'.format(child,result))
            self.data = pd.read_csv(the_path
                             )
            
        except Exception as exc:
            self.show_error(exc)


########################################
## Métodos de Kaggle (fuente de datos) #
########################################

#%%
    
    def list_competition_kaggle(self,competition):
        '''
        Listar "competencias" de Kaggle que contengan una cadena
        
        '''
        try:
            self.lst_competition = self.api.competitions_list(search=competition)
            print('{}Competition: *{}*'.format(os.linesep,competition))
            [print('-->',c) for c in self.lst_competition]
        
        except Exception as exc:
            self.show_error(exc)

#%%        
    def list_files_competition_kaggle(self):
        try:
            self.lst_files_c = self.api.competition_list_files(self.competition)
            print('{}Datasets in: {}'.format(os.linesep,self.competition))
            [print('-->',c) for c in self.lst_files_c]
        
        except Exception as exc:
            self.show_error(exc)

#%%    
    def get_data_from_kaggle_c(self,path_kaggle_files):
        try:
            self.api.competition_download_file(self.competition, 
                                          self.dataset,
                                          path_kaggle_files,
                                          force=True,
                                          quiet=False)
        
        except Exception as exc:
            self.show_error(exc)



#%%

    def list_dataset_kaggle(self,dataset, show = False):
        try:
            self.ds = {}
            self.lst_datasets = self.api.datasets_list(search=dataset)
            print('{}Datasets about: *{}*'.format(os.linesep,dataset))
            
            for x in self.lst_datasets:
                self.ds[x['title']] = x['ref']
            
            if show:
                for x in self.lst_datasets:
                    print(x['title'])
                    pp = pprint.PrettyPrinter(depth=50)
                    pp.pprint(x)
                    break
        
        except Exception as exc:
            self.show_error(exc)

#%%        
    def show_kaggle_datasets(self):
        try:
            for x,y in self.ds.items():
                print('-- {} ---> {}'.format(x,y))
        
        except Exception as exc:
            self.show_error(exc)

#%%    
    def get_data_from_kaggle_d(self,path_kaggle_files,dataset_name):
        try:
            self.api.dataset_download_files(dataset_name, 
                                            path_kaggle_files,
                                            unzip=True
                                            )
        
        except Exception as exc:
            self.show_error(exc)

#%%
    # Control de excepciones
    def show_error(self,ex):
        '''
        Captura el tipo de error, su description y localización.

        Parameters
        ----------
        ex : Object
            Exception generada por el sistema.

        Returns
        -------
        None.

        '''
        trace = []
        tb = ex.__traceback__
        while tb is not None:
            trace.append({
                          "filename": tb.tb_frame.f_code.co_filename,
                          "name": tb.tb_frame.f_code.co_name,
                          "lineno": tb.tb_lineno
                          })
            
            tb = tb.tb_next
            
        print('{}Something went wrong:'.format(os.linesep))
        print('---type:{}'.format(str(type(ex).__name__)))
        print('---message:{}'.format(str(type(ex))))
        print('---trace:{}'.format(str(trace)))
        
#%%    
    def muestra_archivos(self):
        '''
        Imprime en pantalla cada uno de los elementos contenidos en lst_files

        Returns
        -------
        None.

        '''
        try:
            for f in self.lst_files:
                child = os.path.splitext(os.path.basename(f))[0]
                print(child)
                
        except Exception as exc:
            self.show_error(exc)
            
#%%            
    def save_df(self,df,filename = None):
        try:
            if filename is not None:
               df.to_excel(filename , sheet_name = 'sheet', index=False)
            
        except Exception as exc:
            self.show_error(exc)
    


