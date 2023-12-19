import json
import pickle
from core.settings import logger as log


def load_pkl(pkl_path):
    """ Given a pickle file, return his content """
    with open(pkl_path, 'rb') as fp:
        data = pickle.load(fp)
    return data


def get_json_from_pkl(pklpath, jsonfilepath=None):
    """ Given a pickle file, create a json file with his content """
    source = load_pkl(pklpath)

    if not jsonfilepath:
        jsonfilepath = f"{source.name}_exported.json"

    with open(jsonfilepath, 'w', encoding='utf-8-sig') as jsonf:
        log.info(f'Creando archivo {jsonfilepath}')
        jsonf.write(json.dumps(source.data, indent=4, ensure_ascii=False))
        log.info(f'Archivo {jsonfilepath} creado con éxito!')


if __name__ == '__main__':
    pathfile = '/Users/alfonso/Downloads/dispensacion_module-2.pkl'
    get_json_from_pkl(pathfile)
