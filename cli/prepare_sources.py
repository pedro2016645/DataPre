import logging
import os
import pandas as pd
from datetime import datetime
from shyness.scripts.cli.process_sources import app
from shyness.scripts.preparation.time_handlers import compare_refresh_rate
from shyness.scripts.processing.sources_configuration_files import read_yaml
from shyness.scripts.processing.workspace import DirectoryOperations
from shyness.scripts.processing.upload import ReadFiles
from ..preparation.mapping import PREP_METHODS, ABT_METHODS, MODEL_DATA_METHODS, OBJECTS
from ..preparation.validation import shyness_data_types
from ..preparation.workspace import SaveExcelFiles
from ..alerts.notification import ClientDeliver

# load
logger = logging.getLogger(__name__)
# Create a custom logger
DATE_FORMAT_SOURCE = "%Y%m%d_%H%M%S"


def __load_aux_sources(source_setup: dict, src_time: str,
                       user: str = '', password: str = '', env: str = 'dev',
                       test: bool = False, data_folder_tests: str = r".\..\tests\sources") -> dict:
    """
    Load the auxiliary sources
    :param source_setup: dictionary of the auxiliary sources to load
    :param src_time: to loads the auxiliary sources
    :param user: User for connection to DataBases
    :param password: password correspondent to the user for connection to DataBases
    :param env: 'dev' or 'prod' connection to DataBases
    :param test: If it is running as test
    :param data_folder_tests: If test=True the file will be saved in data_folder_tests
    :return:
    """
    result = {}
    try:
        # Rearrange auxiliary_sources part in source setup dictionary to have the form
        aux_sources = source_setup['auxiliary_sources']
        rearranged_source_setup = {}
        if aux_sources is not None:
            logger.info("Start loading auxiliary sources")
            if 'processed_sources' in aux_sources.keys() and aux_sources['processed_sources'] is not None:
                rearranged_source_setup['processed_sources'] = {
                    'product': [],
                    'src_tag': []
                }
                for p in aux_sources['processed_sources'].keys():
                    src = aux_sources['processed_sources'][p]
                    # Add condition to deal with multiple sources from the same product
                    if type(src) != list:
                        src = [src]
                        aux_sources['processed_sources'][p] = src
                    rearranged_source_setup['processed_sources']['product'].extend([p] * len(src))
                    rearranged_source_setup['processed_sources']['src_tag'].extend(src)

            if 'prepared_sources' in aux_sources.keys() and aux_sources['prepared_sources'] is not None:
                rearranged_source_setup['prepared_sources'] = {
                    'use_case': [],
                    'src_type': [],
                    'src_tag': []
                }
                for uc in aux_sources['prepared_sources'].keys():
                    # if is not given a dictionary as value for usecase_key is assumed that the srctype_key is
                    # prep_data
                    if type(aux_sources['prepared_sources'][uc]) != dict:
                        aux_sources['prepared_sources'][uc] = {'prep_data': aux_sources['prepared_sources'][uc]}
                    for srctype in aux_sources['prepared_sources'][uc].keys():
                        src = aux_sources['prepared_sources'][uc][srctype]
                        # Add condition to deal with multiple sources from the same src_type
                        if type(src) != list:
                            src = [src]
                            aux_sources['prepared_sources'][uc][srctype] = src
                        rearranged_source_setup['prepared_sources']['use_case'].extend([uc] * len(src))
                        rearranged_source_setup['prepared_sources']['src_type'].extend([srctype] * len(src))
                        rearranged_source_setup['prepared_sources']['src_tag'].extend(src)

            # Start loading the auxiliary sources
            for src_origin in rearranged_source_setup.keys():
                # for each src_tag
                for i in range(0, len(rearranged_source_setup[src_origin]['src_tag'])):
                    src_process_complete_tag = rearranged_source_setup[src_origin]['src_tag'][i]
                    aux_file_name = src_process_complete_tag
                    if src_origin == 'processed_sources':
                        logger.info("Dealing with processed sources")
                        product = rearranged_source_setup[src_origin]['product'][i]
                    else:
                        logger.info("Dealing with prepared sources")
                        use_case = rearranged_source_setup[src_origin]['use_case'][i]
                        src_type = rearranged_source_setup[src_origin]['src_type'][i]
                        if src_type == 'model_data':
                            if len(aux_file_name.split('/')) == 1:
                                logger.warning('You are trying to load a file of model_data please provide a file_name.'
                                               'If you not provide the default will be the x_test file')
                                aux_file_name = 'x_test'
                            elif len(aux_file_name.split('/')) == 2:
                                src_process_complete_tag = aux_file_name.split('/')[0]
                                aux_file_name = aux_file_name.split('/')[1]
                    # Process each source: src_process_complete_tag
                    src_process_tag = aux_file_name.split(':')
                    if len(src_process_tag) == 1:
                        # if src_time_tag is not given assume the one give to the prep_app function
                        src_time_tag = src_time
                        src_process_tag = src_process_tag[0]
                        # Define aux_src_tag
                        if src_origin == 'processed_sources':
                            aux_src_tag = src_process_tag
                            logger.info("Processing the processed source {}".format(aux_src_tag))
                        else:
                            # Define aux_src_tag: src_type_src_tag
                            # Assumes that you will never need the same source prepared in different use_Cases
                            # for one preparation
                            aux_src_tag = '{}_{}'.format(src_type, src_process_tag)
                            logger.info("Processing the prepared source {}".format(aux_src_tag))

                    else:
                        src_time_tag = src_process_tag[1]
                        src_process_tag = src_process_tag[0]
                        # Check if the datetime have the correct format
                        try:
                            datetime.strptime(src_time_tag, DATE_FORMAT_SOURCE)

                        except ValueError as e:
                            logger.error(e)
                            raise ValueError(
                                "The given str_time for the {} auxiliary source must be on the format: {}"
                                .format(src_process_tag, DATE_FORMAT_SOURCE))

                        # Define aux_src_tag: src_tag_YYYYmmdd
                        src_date_tag = datetime.strptime(src_time_tag, DATE_FORMAT_SOURCE).strftime('%Y%m%d')
                        if src_origin == 'processed_sources':
                            aux_src_tag = '{}_{}'.format(src_process_tag, src_date_tag)
                            logger.info("Processing the processed source {} at src_time: {}"
                                        .format(src_process_tag, src_time_tag))
                        else:
                            aux_src_tag = '{}_{}_{}'.format(src_type, src_process_tag, src_date_tag)
                            logger.info("Processing prepared source {} at src_time: {}"
                                        .format(src_process_tag, src_time_tag))

                    # Load auxiliary sources
                    if test:
                        # if test save the auxiliary sources in the path given in data_folder_tests/src_base_tag
                        if src_origin == 'processed_sources':
                            app(src_tag=src_process_tag, product=product,
                                user=user, password=password, env=env,
                                src_time=src_time_tag, test=True, data_folder_tests=data_folder_tests)
                            source_folder = os.path.join(data_folder_tests, product, src_process_tag)

                        else:
                            prep_app(src_base_tag=src_process_tag, use_case=use_case, src_type=src_type,
                                     user=user, password=password, env=env, src_time=src_time_tag,
                                     test=True, data_folder_tests=data_folder_tests)
                            source_folder = os.path.join(data_folder_tests, use_case, src_type,
                                                         src_process_tag)
                    else:
                        if src_origin == 'processed_sources':
                            # Run shyness app
                            app(src_tag=src_process_tag, product=product, src_time=src_time_tag,
                                user=user, password=password, env=env)
                            # Find the save path
                            package = 'shyness.params.{}'.format(product)
                            processed_conf = read_yaml(src_process_tag, package)
                            source_folder = os.path.join(processed_conf['processed_source']['save_parameters']['path'],
                                                         product, src_process_tag)
                        else:
                            # Run shad prep_app
                            prep_app(src_base_tag=src_process_tag, use_case=use_case, src_type=src_type,
                                     user=user, password=password, env=env, src_time=src_time_tag)
                            # Find the save path
                            package = 'shad.params.{}.{}'.format(use_case, src_type)
                            prepared_conf = read_yaml(src_process_tag, package)
                            source_folder = os.path.join(prepared_conf['prepared_source']['save_parameters']['path'],
                                                         use_case, src_type, src_process_tag)
                    # Read file
                    if src_time_tag == '':
                        source_path = DirectoryOperations.select_recent_file(source_folder, src_process_tag)
                        logger.info("Loading source {}.".format(src_process_tag))
                    else:
                        source_path = os.path.join(source_folder,
                                                   '{}_{}.csv'.format(src_process_tag, src_time_tag))
                        logger.info("Loading source {} at src_time: {}".format(src_process_tag, src_time_tag))

                    df_aux = ReadFiles.data_file(source_path)

                    if src_origin == 'processed_sources':
                        df_aux = shyness_data_types(df_aux, product, src_process_tag)

                    result[aux_src_tag] = df_aux

    except AssertionError as e:
        logger.error(e)

    return result


def __prep_process(df_base: pd.DataFrame, prep_process_params: dict,
                   aux_sources: dict, conn_credentials: dict, date_tag_info: dict) -> pd.DataFrame:
    """
    Function to apply functions of prep_process
    :param df_base: Dataframe where the prep_process will be applied
    :param prep_process_params: dictionary with all the prep_process parameters
    :param aux_sources: dictionary with all the auxiliary sources
    :param conn_credentials: dictionary with information about user, password and env to connect to the database
    :return:
    """
    try:
        logger.info("Start Source Preparation")
        # Verify input
        assert type(df_base) == pd.DataFrame, "The df_base parameter must be a dataframe"
        assert type(prep_process_params) == dict, "The prep_process_params parameter must be a dictionary"
        assert type(aux_sources) == dict, "The aux_sources parameter must be a dictionary"
        assert type(conn_credentials) == dict, "The conn_credentials parameter must be a dictionary"

        # Cycle for the steps to take to prepare the source
        for step in prep_process_params.keys():
            # Cycle for step == filters
            if step == 'filters' and prep_process_params['filters'] is not None:
                filters = prep_process_params['filters']
                logger.info("Start applying Filters")
                # Run lower_step in filters: mixed, dates
                for lower_step in filters.keys():
                    if lower_step in ['mixed', 'dates'] and filters[lower_step] is not None:
                        # If lower_step is not None => run the filters f associated
                        for f in filters[lower_step]:
                            df_base = PREP_METHODS['filters'][lower_step](df_base, f)
                            logger.info('Applied the {} filter {}'.format(lower_step, f))
            # Cycle for step == transformations
            elif step == 'transformations' and prep_process_params['transformations'] is not None:
                transformations = prep_process_params['transformations']
                logger.info("Start applying Transformations")
                # Run lower_step in transformations: global, local
                for lower_step in transformations.keys():
                    # Run lower_step in transformations equal to global
                    if lower_step == 'global' and transformations['global'] is not None:
                        global_transformations = transformations['global']
                        if type(global_transformations) == list:
                            for f in global_transformations:
                                df_base = PREP_METHODS['transformations']['global'][f](
                                    df_base, **aux_sources, **conn_credentials, **date_tag_info)
                                logger.info('Applied the global transformation {}'.format(f))
                        else:
                            for f in global_transformations.keys():
                                variables = global_transformations[f]
                            # Added # char to the end of functions in yaml file in order
                            # to execute the same function multiple times: check example documents.yaml
                            # Because dictionary only allowed unique keys
                                if '#' in f:
                                    # if has a # on the key removes the extra content in order to
                                    # invoke a mapped function
                                    f = f[:f.find('#')]

                                df_base = PREP_METHODS['transformations']['global'][f](
                                    df_base, **variables, **aux_sources, **conn_credentials, **date_tag_info)
                                logger.info('Applied the global transformation {}'.format(f))
                    # Run lower_step in transformations equal to local
                    elif lower_step == 'local' and transformations['local'] is not None:
                        local_transformations = transformations['local']
                        if type(local_transformations) == list:
                            for f in local_transformations:
                                df_base = PREP_METHODS['transformations']['local'][f](
                                    df_base, **aux_sources, **conn_credentials, **date_tag_info)
                                logger.info('Applied the local transformation {}'.format(f))
                        else:
                            for f in local_transformations.keys():
                                variables = local_transformations[f]
                                df_base = PREP_METHODS['transformations']['local'][f](
                                    df_base, **variables, **aux_sources, **conn_credentials, **date_tag_info)
                                logger.info('Applied the local transformation {}'.format(f))

            # Cycle for step == metrics
            elif step == 'metrics' and prep_process_params['metrics'] is not None:
                metrics = prep_process_params['metrics']
                logger.info("Start applying Metrics")
                # Run lower_step in metrics: financial, eligibility
                for lower_step in metrics.keys():
                    if lower_step in PREP_METHODS['metrics'].keys() and metrics[lower_step] is not None:
                        for f in metrics[lower_step].keys():
                            variables = metrics[lower_step][f]
                            df_base = PREP_METHODS['metrics'][lower_step][f](df_base, **variables)
                            logger.info('Applied the {} metrics {}'.format(lower_step, f))

    except AssertionError as e:
        logger.error(e)

    return df_base


def __abt_process(df_base: pd.DataFrame, abt_process_params: dict, date_tag_info: dict,
                  use_case: str = None, load_parameters: dict = None) -> pd.DataFrame:
    """
    Function to apply functions of prep_process
    :param df_base: Dataframe where the prep_process will be applied
    :param abt_process_params: dictionary with all the abt_process parameters
    :return:
    """
    try:
        logger.info("Start ABT Preparation")
        # Verify input
        assert type(df_base) == pd.DataFrame, "The df_base parameter must be a dataframe"
        assert type(abt_process_params) == dict, "The abt_process_params parameter must be a dictionary"

        # Added cycle for in order to allowed to run by the order of conf file
        for step in abt_process_params.keys():
            # Cycle for step == feature_engineering
            if step in ['feature_engineering', 'feature_selection'] \
                    and abt_process_params[step] is not None:
                logger.info("Start applying {}".format(step))
                feature_step = abt_process_params[step]
                # Run lower_step in feature_engineering: categorical_features, specific_features, numerical_features
                for lower_step in feature_step.keys():
                    if lower_step in ABT_METHODS[step].keys() and\
                            feature_step[lower_step] is not None:
                        # if lower_step is not None run the respective functions
                        for cl in feature_step[lower_step].keys():
                            if cl in OBJECTS.keys():
                                if 'init' in feature_step[lower_step][cl].keys():
                                    OBJECTS[cl]['obj'] = OBJECTS[cl]['obj_name'](
                                        **feature_step[lower_step][cl]['init'])
                                else:
                                    OBJECTS[cl]['obj'] = OBJECTS[cl]['obj_name']()

                            for f in feature_step[lower_step][cl].keys():
                                variables = feature_step[lower_step][cl][f]
                                if '#' in f:
                                    # if has a # on the key removes the extra content in order to
                                    # invoke a mapped function
                                    f = f[:f.find('#')]
                                # If f.split('.')[0] is an object initialize it
                                if cl in OBJECTS.keys():
                                    exec_f = getattr(OBJECTS[cl]['obj'],
                                                     ABT_METHODS[step][lower_step][cl][f])

                                    df_base = exec_f(df_base, **variables, **date_tag_info)
                                else:
                                    df_base = ABT_METHODS[step][lower_step][cl][f](
                                        df_base, **variables, **date_tag_info)
                                logger.info('Applied the {}: {}.{}'.format(lower_step, cl, f))

            # Cycle for step == apply_model
            elif step in ['model_delivery', 'model_explainability'] \
                    and abt_process_params[step] is not None:
                logger.info("Start applying {}".format(step))

                # Verify if in that case use_case and load_parameters were given
                assert use_case is not None and type(use_case) == str, "The use_case given is not valid"
                assert load_parameters is not None and type(load_parameters) == dict, \
                    "The load_parameters given is not valid"
                # Check if models_path was given
                assert 'model_path' in load_parameters.keys(), \
                    "The model_path is missing from load_parameters keys"

                # Get lower step
                for f in abt_process_params[step].keys():
                    # Get variables to give to the functions
                    variables = abt_process_params[step][f]
                    # Add to variables the model_path
                    variables['model_path'] = os.path.join(load_parameters['model_path'], use_case)
                    df_base = ABT_METHODS[step][f](df_base, **variables)
                    logger.info('Applied {}'.format(f))

            # Cycle for step == columns_to_retain
            elif step == 'columns_to_retain' and abt_process_params['columns_to_retain'] is not None:
                logger.info('Start applying columns_to_retain')
                columns_to_retain = abt_process_params['columns_to_retain']
                # Run lower_step in columns_to_retain: cols, new_names
                for lower_step in columns_to_retain.keys():
                    if lower_step in ABT_METHODS['columns_to_retain'].keys() and\
                            columns_to_retain[lower_step] is not None:

                        # if lower step is not None run the respective functions
                        df_base = ABT_METHODS['columns_to_retain'][lower_step](df_base, **columns_to_retain)
                        logger.info('Applied the columns_to_retain: {}'.format(lower_step))

    except AssertionError as e:
        logger.error(e)

    return df_base


def __model_data_process(df_base: pd.DataFrame, model_data_process_params: dict,
                         aux_sources: dict, conn_credentials: dict, date_tag_info: dict) -> dict:
    """
    Function to apply functions of model_data_process
    :param df_base: Dataframe where the prep_process will be applied
    :param model_data_process_params: dictionary with all the model_data_process parameters
    :param aux_sources: dictionary with all the auxiliary sources
    :param conn_credentials: dictionary with information about user, password and env to connect to the database
    :return:
    """
    try:
        logger.info("Start ModelData Preparation")
        # Verify input
        assert type(df_base) == pd.DataFrame, "The df_base parameter must be a dataframe"
        assert type(model_data_process_params) == dict, "The model_data_process_params parameter must be a dictionary"
        assert type(aux_sources) == dict, "The aux_sources parameter must be a dictionary"
        assert type(conn_credentials) == dict, "The conn_credentials parameter must be a dictionary"

        # The model_data_process_params must have a data sampling method
        assert 'data_sampling' in model_data_process_params.keys(), \
            "The model_data_process must have a data sampling method"

        assert model_data_process_params['data_sampling'] is not None, \
            "The data_sampling must be not None"

        # Verify if there are present in model_data_process_params the prep_data
        assert 'prep_data' in model_data_process_params.keys() and model_data_process_params['prep_data'] is not None,\
            "The 'prep_data' param is missing from model_data_process_params keys"
        prep_data = model_data_process_params['prep_data']
        # Verify if all the parameters needed to separate the dataframes are present in prep_data
        missing_prep_data_params = [k for k in ['df_train', 'df_test', 'id_col', 'target_col']
                                    if k not in prep_data.keys()]
        assert len(missing_prep_data_params) == 0, "The {} keys are missing from model_data_process prep_test dict" \
            .format(', '.join(missing_prep_data_params))

        # Initialize data_split dictionary with x_train, y_train, x_validation, y_validation, x_test, x_id_test
        split_data = {k: None
                      for k in ['x_train', 'y_train', 'x_validation', 'y_validation', 'x_test', 'id_col_test']}

        logger.info('Start Train/Test dataframes preparation')
        # Get information for train and test
        data = {
            'df_test': None,
            'df_train': None
        }

        # if df_train and df_test are the same source
        # We only attribute that to df_train and maintain the df_test has None
        # else we attribute the respective dataframes to df_train and df_test
        # Reason: if the df_train and df_test bases are equal to not repeat preparation for both
        # we only do it for df_train and after preparation attribute the prepared to df_test
        logger.info('Preparing the dataframe of train and test.')
        if prep_data['df_train'] == prep_data['df_test']:
            if prep_data['df_train'] == 'base':
                data['df_train'] = df_base.copy()
            else:
                assert prep_data['df_train'] in aux_sources.keys(), \
                    "The dataframe {} in prep_test/base_info is missing from auxiliary_sources" \
                    .format(prep_data['df_train'])
                data['df_train'] = aux_sources[prep_data['df_train']].copy()

        # Attribute the dataframes in the case they aren't equal
        else:
            for k in data:
                if prep_data[k] == 'base':
                    data[k] = df_base.copy()
                else:
                    assert prep_data[k] in aux_sources.keys(), \
                        "The dataframe {} in prep_test/base_info is missing from auxiliary_sources" \
                        .format(prep_data[k])
                    data[k] = aux_sources[prep_data[k]].copy()

        # verify if there are preparations for both df_test and train
        # only apply if the dataframes are not None
        for k in data:
            if ('prep_process' in prep_data.keys()) and (prep_data['prep_process'] is not None) and \
                    (data[k] is not None):
                data[k] = __prep_process(data[k],
                                         prep_data['prep_process'],
                                         aux_sources,
                                         conn_credentials,
                                         date_tag_info)
            # Verify if abt_process is present in the train/test preparations
            if ('abt_process' in prep_data.keys()) and (prep_data['abt_process'] is not None) \
                    and (data[k] is not None):
                data[k] = __abt_process(data[k],
                                        prep_data['abt_process'],
                                        date_tag_info)

        if prep_data['df_train'] == prep_data['df_test']:
            # Attribute the df_train prepared in the case that the base sources for df_train and df_test are equal
            data['df_test'] = data['df_train'].copy()

        # start prep_train and prep_test
        for k in data:
            prep = 'prep_{}'.format(k.replace('df_', ''))
            if prep in prep_data.keys() and prep_data[prep] is not None:
                # start prep_train
                logger.info('Start {}.'.format(prep))
                # to run by the order given by the user
                for step in prep_data[prep].keys():
                    if '#' in step:
                        # if has a # on the key removes the extra content
                        clean_step = step[:step.find('#')]
                    else:
                        clean_step = step
                    if clean_step == 'prep_process' and prep_data[prep][step] is not None:
                        data[k] = __prep_process(data[k],
                                                 prep_data[prep][step],
                                                 aux_sources,
                                                 conn_credentials,
                                                 date_tag_info)

                    # Verify if abt_process is present in the train/test preparations
                    if clean_step == 'abt_process' and prep_data[prep][step] is not None:
                        data[k] = __abt_process(data[k],
                                                prep_data[prep][step],
                                                date_tag_info)

        # Verify if exists columns_to_retain
        if 'columns_to_retain' in model_data_process_params.keys() and\
                model_data_process_params['columns_to_retain'] is not None:
            logger.info('Start columns selection for the train and test dataframes')
            for step in model_data_process_params['columns_to_retain'].keys():
                params = model_data_process_params['columns_to_retain']
                data['df_train'] = MODEL_DATA_METHODS['columns_to_retain'][step](data['df_train'], **params)
                data['df_test'] = MODEL_DATA_METHODS['columns_to_retain'][step](data['df_test'], **params)
                logger.info('Applied the columns_to_retain to df_test and df_train: {}'.format(step))

        # Finalize dataframes
        df_test = data['df_test'].copy()
        df_train = data['df_train'].copy()

        # remove target_col from test dataframe if there
        target_cols = prep_data['target_col'].split(',')
        col_target_in_df_test = [col for col in target_cols if col in df_test.columns.tolist()]
        # confirmed that if the col_target_in_df_test the pop doesn't remove any column and works
        df_test = df_test.drop(columns=col_target_in_df_test)

        # Remove id_col from test dataframe and attribute that to id_col_test in split_data
        split_data['id_col_test'] = df_test[prep_data['id_col']]
        cols = df_test.columns.tolist()
        cols.remove(prep_data['id_col'])
        split_data['x_test'] = df_test[cols]

        # Run data_sampling method
        data_sampling_params = model_data_process_params['data_sampling']

        # Run the chosen data_sampling method
        logger.info('Starting DataSampling')
        # if the user gives more than one method for data_sampling only the first will be considered
        # Get the object and script of data sampling
        data_sampling = list(data_sampling_params.keys())[0]
        data_sampling_script = data_sampling.split('.')[0]
        data_sampling_obj = data_sampling.split('.')[1]

        # Start running the data sampling method
        # if init is given: give the value in init to the initialization of the object
        # other wise give nothing
        # if not an object jump directly to the functions
        if 'init' in data_sampling_params[data_sampling].keys():
            # Initialize data sampling object
            OBJECTS[data_sampling_obj]['obj'] = OBJECTS[data_sampling_obj]['obj_name'](
                **data_sampling_params[data_sampling]['init']
            )
            data_sampling_params[data_sampling].pop('init')
        elif data_sampling_obj in OBJECTS.keys():
            OBJECTS[data_sampling_obj]['obj'] = OBJECTS[data_sampling_obj]['obj_name']()

        for f in data_sampling_params[data_sampling].keys():
            # Execute the function/functions associated to the data sampling method
            logger.info('Start data sampling function: {}'.format(f))
            exec_f = getattr(OBJECTS[data_sampling_obj]['obj'],
                             MODEL_DATA_METHODS['data_sampling'][data_sampling_script][data_sampling_obj][f])
            exec_f(df_train, **data_sampling_params[data_sampling][f])
            logger.info('Finish data sampling function: {}'.format(f))

        # Attribute the results of data sampling to the respective split_data keys
        split_data['x_train'] = OBJECTS[data_sampling_obj]['obj'].x_train
        split_data['y_train'] = OBJECTS[data_sampling_obj]['obj'].y_train
        split_data['x_validation'] = OBJECTS[data_sampling_obj]['obj'].x_validation
        split_data['y_validation'] = OBJECTS[data_sampling_obj]['obj'].y_validation

        # Verify if x_train/x_validation/x_test have the same columns
        missing_cols = [col for col in split_data['x_test'].columns.tolist()
                        if ((col not in split_data['x_validation']) or (col not in split_data['x_train']))]
        assert len(missing_cols) == 0, \
            "The columns are not present in the three dataframes x_train, x_validation and x_test: {}"\
            .format(', '.join(missing_cols))

        # Added cycle for in order to allowed to run by the order of conf file
        # the rest of the functions applied to all the dataframes: x_train, x_test, x_validation
        model_data_process_params.pop('prep_data')
        model_data_process_params.pop('data_sampling')
        # Apply to all the desired preparation to the three files: 'x_train', 'x_validation', 'x_test'
        for data in ['x_train', 'x_validation', 'x_test']:
            # Cycle for the preparations desired
            for step in model_data_process_params.keys():
                params = model_data_process_params[step]
                if 'prep_process' in step and model_data_process_params[step] is not None:
                    __prep_process(split_data[data], params, aux_sources, conn_credentials, date_tag_info)
                elif 'abt_process' in step and model_data_process_params[step] is not None:
                    __abt_process(split_data[data], params, date_tag_info)

        return split_data
    except AssertionError as e:
        logger.error(e)


def prep_app(src_base_tag: str, use_case: str = '', src_type: str = 'prep_app', src_time: str = "",
             user: str = '', password: str = '', env: str = 'dev',
             test: bool = False, data_folder_tests: str = r".\..\tests\sources"):
    """
    Function to run the process for the preparation of a source
    :param src_base_tag: tag for the source that will be the base
    :param use_case: use_case where src_tag belongs
    :param src_type: type of the source: ['prep_data', 'abt', 'data_requests', 'model_data']
    :param src_time: Sources running timestamp, if '' it will be the current time
    :param user: User for connection to DataBases
    :param password: password correspondent to the user for connection to DataBases
    :param env: 'dev' or 'prod' connection to DataBases
    :param test: True is is a test false otherwise
    :param data_folder_tests: Folder where sources will be saved
    :return:
    """
    successful = False

    try:
        assert type(src_base_tag) == str, "The src_base_tag is not a string"
        assert type(use_case) == str, "The use_case parameter is not a string"
        assert type(src_type) == str, "The src_type parameter is not a string"
        possible_src_types = ['prep_data', 'abt', 'data_requests', 'model_data']
        assert src_type in possible_src_types, \
            "The src_type parameter must be {}".format('or '.join(possible_src_types))
        assert type(src_time) == str, "The given src_time parameter must be a string"
        assert type(user) == str, "The given user parameter must be a string"
        assert type(password) == str, "The given password parameter must be a string"
        assert env in ['dev', 'prod'], "The given env parameter must be 'dev' or 'prod'"
        assert type(test) == bool, "The given test parameter must be a boolean"

        # Check str
        if len(src_time) > 0:
            # Transform to date
            try:
                date_obj = datetime.strptime(src_time, DATE_FORMAT_SOURCE)
                date_tag = src_time

                # date_tag_shyness = src_time
            except ValueError as e:
                logger.error(e)
                raise ValueError("The given str_time must be on the format: {0}".format(DATE_FORMAT_SOURCE))

        else:
            date_obj = datetime.now()
            date_tag = date_obj.strftime(DATE_FORMAT_SOURCE)

        try:
            # Read prep conf file to load preparation settings
            if use_case != '':
                source_prep_setup = read_yaml(src_base_tag, '{}.{}.{}'.format('shad.params', use_case, src_type))
            else:
                source_prep_setup = read_yaml(src_base_tag, 'shad.params')

            # Check if there is an updated source
            # Check if there is a latency date
            if 'refresh_rate' in source_prep_setup['conf_file'].keys() and \
                    source_prep_setup['conf_file']['refresh_rate'] is not None:
                refresh_rate_rfr = source_prep_setup['conf_file']['refresh_rate'].split(',')
                time_reference = refresh_rate_rfr[0]
                rate = int(refresh_rate_rfr[1])
            else:
                time_reference = None
                rate = 0

            logger.info("Checking if there is an update version")
            # Get the file path
            if test:
                # Check the path
                DirectoryOperations.check_dir(data_folder_tests, use_case, src_type, src_base_tag)
                src_file_path = os.path.join(data_folder_tests,
                                             use_case,
                                             src_type,
                                             src_base_tag
                                             )
            else:
                DirectoryOperations.check_dir(source_prep_setup['prepared_source']['save_parameters']['path'],
                                              use_case, src_type, src_base_tag)
                src_file_path = os.path.join(source_prep_setup['prepared_source']['save_parameters']['path'],
                                             use_case,
                                             src_type,
                                             src_base_tag
                                             )

            # Check which type of source is updated
            if src_type == 'model_data':
                src_file_name = 'x_test'
            else:
                src_file_name = src_base_tag

            recent_file = DirectoryOperations.select_recent_file(src_file_path, src_file_name)
            not_processed = True
            file_date = recent_file.split('.')[0].replace(os.path.join(src_file_path,
                                                                       src_file_name) + '_', '')
            # Get information if the sad source is updated or not
            if len(recent_file) > 0 and len(src_time) == 0:
                logger.info("Last Update: {} ".format(file_date))
                file_date = datetime.strptime(file_date, DATE_FORMAT_SOURCE)
                not_processed = compare_refresh_rate(file_date, date_obj, time_reference, rate)
            elif len(src_time) != 0:
                not_processed = True
            # If the source is updated continue the process if not stop it
            if not_processed:
                load_parameters = source_prep_setup['processed_source']['load_parameters']

                # Check if the source_prep_setup parameters have, for each src_type the correct keys
                if src_type == 'model_data':
                    assert 'model_data_process' in source_prep_setup.keys(), \
                        "The src_type is model_data but there is no model_data_process key in the given yaml"

                elif src_type == 'data_request':
                    assert 'deliver_params' in source_prep_setup['save_parameters'].keys(), \
                        "The src_type is data_request but there is no deliver_params key in save_parameters dict" \
                        " in the given yaml"

                # Set path for data_folder_tests
                if test:
                    base_file_path = data_folder_tests
                else:
                    # Get paths
                    base_file_path = load_parameters['path']
                # Set default file_name as src_tag
                base_file_name = load_parameters['tag']
                if 'product' in load_parameters.keys():
                    base_package = 'shyness'
                    # run shyness for the source base
                    shyness_src_tag = load_parameters['tag']
                    shyness_src_product = load_parameters['product']
                    # Define file_name as src_tag
                    base_file_name = shyness_src_tag

                    # Add file path
                    DirectoryOperations.check_dir(base_file_path, load_parameters['product'], load_parameters['tag'])
                    base_file_path = os.path.join(base_file_path, load_parameters['product'], load_parameters['tag'])

                elif 'use_case' in load_parameters.keys():
                    base_package = 'shad'
                    # run shad for the source base
                    shad_src_tag = load_parameters['tag']
                    shad_use_case = load_parameters['use_case']
                    if 'src_type' in load_parameters.keys():
                        shad_src_type = load_parameters['src_type']
                    else:
                        load_parameters['src_type'] = 'prep_data'
                        shad_src_type = 'prep_data'

                    # Add file path
                    DirectoryOperations.check_dir(base_file_path, load_parameters['use_case'],
                                                  load_parameters['src_type'], load_parameters['tag'])

                    base_file_path = os.path.join(base_file_path, load_parameters['use_case'],
                                                  load_parameters['src_type'], load_parameters['tag'])

                    if shad_src_type == 'model_data':
                        if 'file_name' not in load_parameters.keys():
                            logger.warning('You are trying to load a file of model_data please provide a file_name.'
                                           'If you not provide the default will be the x_test file')
                            base_file_name = 'x_test'
                        else:
                            base_file_name = load_parameters['file_name']
                    else:
                        base_file_name = shad_src_tag

                else:
                    base_package = None
                    logger.warning("The load_parameters must have a product or use_case as key "
                                   "if it is from shyness or shad, respectively. "
                                   "In this case we will load the following file: {}".format(base_file_path))

                if base_package == 'shyness':
                    logger.info("Start processing for processed source base: {}".format(shyness_src_tag))

                    app(shyness_src_tag, product=shyness_src_product, src_time=src_time,
                        user=user, password=password, env=env,
                        test=test, data_folder_tests=data_folder_tests)
                elif base_package == 'shad':
                    logger.info("Start processing for prepares source base: {}".format(shad_src_tag))

                    prep_app(shad_src_tag, use_case=shad_use_case, src_type=shad_src_type, src_time=src_time,
                             user=user, password=password, env=env,
                             test=test, data_folder_tests=data_folder_tests)

                # Load base source
                if src_time != "":
                    logger.info("Selecting base source file at time: {}".format(src_time))
                    recent_file_path = os.path.join(base_file_path,
                                                    "{}_{}.csv".format(base_file_name, src_time))
                else:
                    logger.info("Selecting recent base source file")
                    recent_file_path = DirectoryOperations.select_recent_file(base_file_path,
                                                                              base_file_name)

                df_base = ReadFiles.data_file(recent_file_path, 0, load_parameters['delimiter'],
                                              load_parameters['encoding'])

                if base_package == 'shyness':
                    df_base = shyness_data_types(df_base, shyness_src_product, shyness_src_tag)

                # Start loading auxiliary_sources
                aux_sources = __load_aux_sources(
                    source_prep_setup, src_time=src_time,
                    user=user, password=password, env=env,
                    test=test,
                    data_folder_tests=data_folder_tests)

                # Prep process for auxiliary sources
                if 'aux_prep_process' in source_prep_setup.keys():
                    logger.info("Starting preparation for auxiliary sources")
                    aux_sources_filtered = source_prep_setup['aux_prep_process']
                    for source in aux_sources_filtered:
                        aux_filters = aux_sources_filtered[source]
                        if aux_filters is not None:
                            if aux_filters['filters']['mixed'] is not None:
                                for f in aux_filters['filters']['mixed']:
                                    aux_sources[source] = PREP_METHODS['filters']['mixed'](aux_sources[source], f)
                                    logger.info('Applied {}'.format(f))

                            if aux_filters['filters']['dates'] is not None:
                                for f in aux_filters['filters']['dates']:
                                    aux_sources[source] = PREP_METHODS['filters']['dates'](aux_sources[source], f)
                                    logger.info('Applied {}'.format(f))

                            if 'tag' in aux_filters.keys():
                                aux_sources[aux_filters['tag']] = aux_sources.pop(source)

                # Define conn_credentials: dict with information of user, password and env to connect to the database
                conn_credentials = {
                    'user': user,
                    'password': password,
                    'env': env
                }

                # To know which columns are from the original source
                original_columns = list(df_base.columns)

                # Set save parameters
                save_parameters = source_prep_setup['prepared_source']['save_parameters']

                # Prepare date_tag_info to give to the functions to apply
                date_tag_info = {'date_tag': date_tag}
                # Start the preparation of the source, that is, run the prep_process/abt_process in yaml
                for k in source_prep_setup.keys():
                    # the user may want to use the same process more than once or alternate
                    # for that he can had #number after k
                    if '#' in k:
                        step = k[:k.find('#')]
                    else:
                        step = k
                    # Check in which step you are
                    if step == 'prep_process' and source_prep_setup[k] is not None:
                        prep_parameters = source_prep_setup[k]
                        df_base = __prep_process(df_base, prep_parameters,
                                                 aux_sources=aux_sources, conn_credentials=conn_credentials,
                                                 date_tag_info=date_tag_info)

                    elif step == 'abt_process' and source_prep_setup[k] is not None:
                        abt_parameters = source_prep_setup[k]
                        df_base = __abt_process(df_base, abt_parameters, date_tag_info,
                                                use_case=use_case, load_parameters=load_parameters)

                    elif step == 'columns_to_retain' and source_prep_setup[k] is not None:
                        logger.info('Start applying columns_to_retain')
                        columns_to_retain = source_prep_setup[k]
                        columns_to_retain_conf = columns_to_retain.copy()
                        if 'add_automatically_new_cols' in columns_to_retain.keys():
                            columns_to_retain_conf['original_columns'] = original_columns
                            columns_to_retain.pop('add_automatically_new_cols')
                        for step in columns_to_retain.keys():
                            if columns_to_retain[step] is not None:
                                df_base = PREP_METHODS['columns_to_retain'][step](df_base, **columns_to_retain_conf)
                                logger.info('Applied the columns_to_retain: {}'.format(step))

                    # In the case to be a model_data you can't have more than one model_data_process =>
                    # k == 'model_data_process' instead of step == model_data_process
                    elif k == 'model_data_process' and source_prep_setup[k] is not None:
                        model_data_parameters = source_prep_setup[k]
                        split_data = __model_data_process(df_base, model_data_parameters,
                                                          aux_sources=aux_sources, conn_credentials=conn_credentials,
                                                          date_tag_info=date_tag_info)

                # Save file
                # Two types of save we can deliver or save on prep source or both
                if 'save_parameters' in source_prep_setup['prepared_source'].keys():
                    if test:
                        # DirectoryOperations.check_dir(data_folder_tests)
                        save_parameters['path'] = os.path.join(data_folder_tests)

                    DirectoryOperations.check_dir(save_parameters['path'], use_case, src_type, save_parameters['tag'])
                    save_path = os.path.join(save_parameters['path'], use_case, src_type, save_parameters['tag'])
                    if ((src_type != 'model_data') or \
                            (src_type == 'model_data' and source_prep_setup['model_data_process'] is None)):
                        logger.info('Saving file')
                        logger.info('File dimensions: {}'.format(df_base.shape))
                        df_base.to_csv(os.path.join(save_path,
                                                    '{}_{}.{}'.format(
                                                        save_parameters['tag'],
                                                        date_tag,
                                                        save_parameters['file_type']
                                                    )),
                                       save_parameters['delimiter'],
                                       index=False,
                                       encoding=save_parameters['encoding'])
                    else:
                        logger.info('Saving files')
                        for data in split_data.keys():
                            split_data[data].to_csv(os.path.join(save_path,
                                                                 '{}_{}.{}'.format(
                                                                     data,
                                                                     date_tag,
                                                                     save_parameters['file_type']
                                                                 )),
                                                    save_parameters['delimiter'],
                                                    index=False,
                                                    encoding=save_parameters['encoding'])

                if 'deliver_params' in source_prep_setup['prepared_source'].keys() and not test:
                    # Prepares Excel
                    delivers_params = source_prep_setup['prepared_source']['deliver_params']

                    # Check Folder
                    DirectoryOperations.check_dir(delivers_params['path'], delivers_params['folder'])
                    complete_save_path = os.path.join(delivers_params['path'], delivers_params['folder'])
                    file_name, _ = SaveExcelFiles.save(complete_save_path, delivers_params['file_name'],
                                                       df_base, delivers_params['schema_table'], date_tag)
                    if 'notification' in delivers_params:
                        ClientDeliver.receipt(file_name, delivers_params['folder'], date_obj,
                                              **delivers_params['notification'])

                logger.info("Source updated")
                successful = True

        except Exception as e:
            logger.error(e)
            raise e

    except AssertionError as e:
        logger.error(e)
        raise e

    return successful