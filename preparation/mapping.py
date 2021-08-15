"""
Script with Map of function to the keys allowed on yaml files for each source
"""

from ..preparation.sources_global import Filters
from ..preparation.sources_global import Columns
from ..preparation.sources_global import Transformations
from ..preparation.sources_global import Indicators
from ..preparation.sources_local import DmeRepository
from ..preparation.sources_local import Dun
from ..preparation.sources_local import Documents
from ..preparation.sources_local import InternationalTransfers
from ..preparation.metrics import Financial
from blackbass.scripts.feature_selection import supervised
from blackbass.scripts.feature_engineering import categorical_features, specific_features, numerical_features
from blackbass.scripts.data_sampling import binary_classification
from blackbass.scripts.model_explainability.features import BehaviorEval
from blackbass.scripts.model_delivery import modeling

# All yaml file are in folder params
# Objects Mapping
OBJECTS = {
    'balanced_binary_classification': {
        'obj_name': binary_classification.BalancedBinaryClassification,
        'obj': None
    },
    'selector': {
        'obj_name': supervised.Selector,
        'obj': None
    }
}
# All functions with filters, on yaml with key: filters inside pre_process key on yaml
FILTERS = {
    'mixed': Filters.by_query,
    'dates': Filters.by_query_with_date
}

# All functions with transformations on yaml with key: transformations inside pre_process
TRANSFORMATIONS = {
    'global': {
        'clients_to_drop': Transformations.clients_to_drop,
        'contains_content': Transformations.contains_content,
        'match_names': Transformations.match_names,
        'add_nif': Transformations.add_nif,
        'add_info': Transformations.add_info,
        'prepare_contacts_info': Transformations.prepare_contacts_info,
        'merge_into_one_column': Transformations.merge_into_one_column,
        'add_n_client': Transformations.add_n_client,
        'group_info': Transformations.group_info,
        'change_date_col': Transformations.change_date_col,
        'if_in_category': Transformations.if_in_category,
        'subtract_cols_by_date_tag': Transformations.subtract_cols_by_date_tag,
        'subtract_cols': Transformations.subtract_cols,
        'change_col_type': Transformations.change_col_type,
        'transpose_column': Transformations.transpose_column,
        'fill_na_values': Transformations.fill_na_values,
        'create_indicator': Transformations.create_indicator,
        'concatenate_cols': Transformations.concatenate_cols,
        'order_by_cols': Transformations.order_by_cols,
        'if_present': Indicators.if_present,
        'concatenate': Transformations.concatenate,
        'sort_column': Transformations.sort_column,
        'cells_rename': Transformations.cells_rename
    },
    'local': {
        'if_prod': DmeRepository.if_prod,
        'dun_mother_in_cgd': Dun.dun_mother_in_cgd,
        'add_anticipation_indicator': Documents.add_anticipation_indicator,
        'add_profit_by_intervention': Documents.add_profit_by_intervention,
        'agtypes_transfers': InternationalTransfers.agg_types_transfers
    }

}

# All functions that add new columns based on business concepts on yaml with key: metrics inside pre_process
METRICS = {
    'financial': {
        'financial_autonomy': Financial.financial_autonomy,
        'debt': Financial.debt,
        'netdebt': Financial.netdebt,
        'ebitda': Financial.ebidta
    }
}

# All functions related to selection or deleting columns on yaml with key: columns_to_retain
COLUMNS = {
    'cols': Columns.select,
    'new_names': Columns.rename
}

# Final dictionary that invokes all the prep_methods the above
PREP_METHODS = {
    'filters': FILTERS,
    'transformations': TRANSFORMATIONS,
    'metrics': METRICS,
    'columns_to_retain': COLUMNS
}

# All yaml file are in folder params use_case/abt
# All functions in abt_params on yaml with key: feature_engineering
FEATURE_ENGINEERING = {
    'categorical_features': {
        'encoding': {
            'one_hot': categorical_features.Encoding.one_hot
        },
    },
    'specific_features': {
        'cae': {
            'aggregate_samples_by_leveling_up': specific_features.CAE.aggregate_samples_by_leveling_up
        }
    },
    'numerical_features': {
        'transformations': {
            'logarithmic': numerical_features.Transformations.logarithmic,
            'box_cox': numerical_features.Transformations.box_cox
        },
        'scaling': {
            'min_max': numerical_features.Scaling.min_max_normalization
        },
        'timeseries': {
            'sma': numerical_features.TimeSeries.simple_moving_average_prep
        }
    }
}

# All functions in abt_params on yaml with key: feature_selection
FEATURE_SELECTION = {
    'supervised': {
        'selector': {
            'filter_based_univariate_chi': supervised.Selector.filter_based_univariate_chi,
            'embedded_random_forest': supervised.Selector.embedded_random_forest
        }
    }
}

MODEL_EXPLAINABILITY = {
    'shap_values': BehaviorEval.by_shap_values
}

MODEL_DELIVERY = {
    'apply_model': modeling.Model.apply_model
}

# Final dictionary that invokes all the abt_methods above
ABT_METHODS = {
    'feature_selection': FEATURE_SELECTION,
    'feature_engineering': FEATURE_ENGINEERING,
    'model_explainability': MODEL_EXPLAINABILITY,
    'model_delivery': MODEL_DELIVERY,
    'columns_to_retain': COLUMNS
}

# All yaml file are in folder params use_case/model_data
# All functions in model_data_params on yaml with key: data_sampling
DATA_SAMPLING = {
    'binary_classification': {
        'balanced_binary_classification': {
            'split_data': 'split_data'
        }
    }
}

# Final dictionary that invokes all the abt_methods above
MODEL_DATA_METHODS = {
    'data_sampling': DATA_SAMPLING,
    'feature_engineering': FEATURE_ENGINEERING,
    'columns_to_retain': COLUMNS
}
