import luther
import numpy as np
import pandas as pd
from loguru import logger

import statsmodels.api as sm
import statsmodels.tsa.stattools as ts

logger.level("RESULTS", no=40, color="<green>")
_log_file_name = __file__.split("/")[-1].split(".")[0]
logger.add(f"logs/{_log_file_name}.log", rotation="1 day")
logger.add(f"logs/success_{_log_file_name}.log", level='SUCCESS')
logger.add(f"logs/results_{_log_file_name}.log", level='RESULTS')
logger.add(f"logs/success.log", rotation="1 day", level="SUCCESS")



@logger.catch
def prepare_df_for_modeling(df, column_name='star_count_diff'):
    """

    'star_count_rel'
    """
    logger.info(f"Prepare DataFrame for modeling on column {column_name}.")
    group = df.groupby(['fake_date'])
    grouped_df = group.agg({
        column_name: ['mean', 'std', 'count', 'min', 'max', 'sum'],
    })

    logger.success(f"Finished prepare_df_for_modeling for {column_name}.")
    return grouped_df

@logger.catch
def ar_model_fitting(training, column_name, max_ar=5):
    ar_models = []
    for lag in range(1, max_ar+1):
        
        ar = sm.tsa.ARMA(training['mean'], dates=training.index, freq='D', order=(lag,0))
        ar_fit = ar.fit()

        betas = [ar_fit.params[k] for k in range(1,lag+1)]
        ar_models.append((lag, betas, column_name))

    logger.success(f"Finished ar_model_fitting for {column_name}.")
    return ar_models

@logger.catch
def predict_lin_reg(betas, xs):
    zipped = zip(betas, xs)
    return sum([ beta*x for beta, x in zipped ])

@logger.catch
def ar_model_validation(ar_model_results, validation):
    logger.info(f"Start AR Model Validation")

    overall_ar_results = []
    for ar_model in ar_model_results:
        lag, betas, column_name = ar_model
        logger.info(f"Validate AR({lag}) Model")
        mod_validation_data = validation[validation['days_since_mention'] > -lag ]
        grouped_star = prepare_df_for_modeling(mod_validation_data, column_name=column_name)['mean']
        grouped_mean_list = list(grouped_star)
        residuals = []
        y_predicted_list = []
        calculations = []
        for i in range(0, max(grouped_mean_list)-lag):
            xs = grouped_mean_list[i:lag+i]
            y_predict = predict_lin_reg(betas, xs)
            y_predicted_list.append(y_predicted)
            y = grouped_mean_list[lag+i+1]
            resid_square = (y - y_predict)**2
            residuals.append(resid_square)
            calculations.append(
                (lag, betas, xs, y_predict, y, resid_square, i)
            )

        rmse = sum(residuals)/len(residuals)
        overall_ar_results.append((lag, betas, rmse, calculations))

        logger.log("RESULTS", f"\n\n#############################################\n")
        logger.log("RESULTS", f" >>> AR({lag}) - VALIDATION - {column_name} <<<")
        logger.log("RESULTS", f"Results for AR({ar_model})")
        logger.log("RESULTS", f"Calculations: {calculations}")
        logger.log("RESULTS", f"lag: {lag}, betas: {betas}")
        logger.log("RESULTS", f"RSME: {rsme}")
        logger.log("RESULTS", f"\n#############################################\n\n")

    filename = "data/validation_result/ar_model_validation_" + column_name + "_" + luther.get_timestamp() + ".pk"

    # Report rmse results
    with open(filename, 'wb') as f:
        pickle.dump(overall_ar_results, f)

    logger.info(f"Pickled Overall AR Validation Results to {filename}")
    logger.success(f"Finished ar_model_validation and pickled to {filename}")
    return overall_ar_results
    

@logger.catch
def validate_all(training, validation, column_names=['star_count_diff', 'star_count_rel']):
    logger.info(f"Run Validation Pipeline.")

    for column_name in column_names:
        logger.info(f"Running the modeling pipeline for {column_name}.")
        prep_training = prepare_df_for_modeling(training, column_name=column_name)
        prep_validation = prepare_df_for_modeling(validation, column_name=column_name)
        ar_model_results = ar_model_fitting(prep_training, column_name)
        validation_results = ar_model_validation(ar_model_results, prep_validation)

    logger.info(f"Finished Validation Pipeline.")
    logger.success(f"Finished validate_all for {column_names}")


