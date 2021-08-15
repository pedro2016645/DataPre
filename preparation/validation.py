"""
Script with functions and models for validation on shad.
"""
import pandas as pd
import re
from shyness.scripts.processing.sources_configuration_files import read_yaml
from shyness.scripts.cli.process_sources import fix_yaml_columns, replace_data_types_tags
import logging

logger = logging.getLogger(__name__)


def shyness_data_types(df: pd.DataFrame, product: str, src_tag: str) -> pd.DataFrame:
    """
    Change data frame types according with shyness.
    :param df: the base data frame
    :param product: product associated with the source on shyness
    :param src_tag: name of the source in shyness
    :return:
    """
    try:
        logger.info('Start executing shyness_data_types')
        assert type(df) == pd.DataFrame, "The df is not a data frame."
        assert type(product) == str, "Product is not a string."
        assert type(src_tag) == str, "Source tag is not a string."

        package_name = '{}.{}'.format('shyness.params', product)
        source_conf = read_yaml(src_tag, package_name)

        if ('multilevel' in source_conf['raw_source']['load_parameters']) and \
                (source_conf['raw_source']['load_parameters']['multilevel']):
            multilevel = True
        else:
            multilevel = False

        source_conf = fix_yaml_columns(source_conf, multilevel)

        transform_params = source_conf['data_types']

        transform_params = replace_data_types_tags(transform_params, df)

        name_keys = ['n_client_cols', 'date_cols', 'str_cols', 'int_cols', 'nif_cols', 'float_cols']
        types_keys = ['int', 'date', 'str', 'int64', 'str', 'float']

        for k in range(len(name_keys)):
            logger.info('Start type: {} for {}'.format(types_keys[k], name_keys[k]))
            if k != 1:
                cols = transform_params[name_keys[k]]
            else:
                if transform_params[name_keys[k]] is not None:
                    cols = transform_params[name_keys[k]]['cols']
                else:
                    cols = None
            if cols is not None:
                cols = re.sub('[^a-zA-Z0-9, \n.]', ' ', cols.lower())
                cols = [c.strip() for c in cols
                        .replace("\n", "")
                        .replace('Ç', 'C')
                        .lower()
                        .replace(' ', '_')
                        .split(',')
                        ]

                cols = [c for c in cols
                        if c in list(df.columns)
                        ]

                if types_keys[k] == 'date':
                    for c in cols:
                        df.loc[:, c] = pd.to_datetime(df[c], format="%Y%m%d_%H%M%S", errors='coerce')
                else:
                    for c in cols:
                        # column valor___1 -> give warning:
                        # A value is trying to be set on a copy of a slice from a DataFrame.
                        df = df.astype({c: types_keys[k]})
        return df
    except AssertionError as e:
        logger.error(e)
